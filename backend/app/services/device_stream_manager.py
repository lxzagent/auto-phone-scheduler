"""Per-device shared Scrcpy stream fan-out for preview and recording.

A single Scrcpy encoder/session is shared by all consumers of a device. Each
subscriber receives the same metadata and H.264 packets.
"""
from __future__ import annotations

import asyncio
import logging
import socket
import time
import zlib
from dataclasses import dataclass
from typing import AsyncIterator

from app.services.scrcpy_stream import ScrcpyStreamer
from app.services.scrcpy_protocol import ScrcpyMediaStreamPacket, ScrcpyVideoStreamMetadata

logger = logging.getLogger(__name__)
_END = object()


@dataclass
class StreamMessage:
    metadata: ScrcpyVideoStreamMetadata | None = None
    packet: ScrcpyMediaStreamPacket | None = None
    error: str | None = None


class DeviceStreamSubscription:
    def __init__(self, manager: "DeviceStreamManager", device_id: str):
        self.manager = manager
        self.device_id = device_id
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=256)
        self.lossless = False
        self.awaiting_keyframe = True
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[StreamMessage]:
        while True:
            item = await self.queue.get()
            if item is _END:
                break
            yield item

    async def close(self) -> None:
        if not self.closed:
            self.closed = True
            await self.manager.unsubscribe(self)


class _DeviceStream:
    def __init__(self, device_id: str, max_size: int, bit_rate: int):
        seed = zlib.crc32(device_id.encode("utf-8")) % 20000
        port = None
        for offset in range(20000):
            candidate = 28000 + ((seed + offset) % 20000)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
                try:
                    probe.bind(("127.0.0.1", candidate))
                    port = candidate
                    break
                except OSError:
                    continue
        if port is None:
            raise RuntimeError("没有可用的 Scrcpy 本地转发端口")
        self.streamer = ScrcpyStreamer(
            device_id=device_id,
            max_size=max_size,
            bit_rate=bit_rate,
            port=port,
        )
        self.subscribers: set[DeviceStreamSubscription] = set()
        self.metadata: ScrcpyVideoStreamMetadata | None = None
        self.pump_task: asyncio.Task | None = None
        self.start_lock = asyncio.Lock()
        self.stopped = False
        self.stopping = False
        self.packet_lock = asyncio.Lock()
        self.configuration_packet: ScrcpyMediaStreamPacket | None = None

    async def start(self) -> None:
        async with self.start_lock:
            if self.pump_task and not self.pump_task.done():
                return
            await self.streamer.start()
            self.metadata = await self.streamer.read_video_metadata()
            self.pump_task = asyncio.create_task(self._pump(), name=f"scrcpy-pump-{self.streamer.device_id}")

    async def _pump(self) -> None:
        started = time.monotonic()
        packet_count = 0
        data_packet_count = 0
        end_reason = "stream_exhausted"
        end_error: str | None = None
        logger.info("[StreamManager] Scrcpy 数据泵启动: device=%s", self.streamer.device_id)
        try:
            async for packet in self.streamer.iter_packets():
                packet_count += 1
                if packet.type == "data":
                    data_packet_count += 1
                async with self.packet_lock:
                    if packet.type == "configuration":
                        # Cache SPS/PPS so a browser that reconnects to an already
                        # running device stream can initialize a fresh WebCodecs decoder.
                        self.configuration_packet = packet
                    message = StreamMessage(packet=packet)
                    for subscriber in tuple(self.subscribers):
                        if subscriber.closed:
                            continue
                        if packet.type == "data":
                            # A newly attached decoder cannot decode inter frames until
                            # it receives an IDR/keyframe from the current stream.
                            if subscriber.awaiting_keyframe and not packet.keyframe:
                                continue
                            if packet.keyframe:
                                subscriber.awaiting_keyframe = False
                        if subscriber.queue.full():
                            if subscriber.lossless:
                                await subscriber.queue.put(message)
                            else:
                                try:
                                    subscriber.queue.get_nowait()
                                except asyncio.QueueEmpty:
                                    pass
                                subscriber.queue.put_nowait(message)
                        else:
                            subscriber.queue.put_nowait(message)
        except asyncio.CancelledError:
            end_reason = "cancelled"
            raise
        except Exception as exc:
            end_reason = "error"
            end_error = str(exc)
            logger.exception("[StreamManager] Scrcpy 数据泵异常: device=%s", self.streamer.device_id)
        finally:
            elapsed = time.monotonic() - started
            self.stopped = True
            self.streamer.stop()
            logger.warning(
                "[StreamManager] Scrcpy 数据泵结束: device=%s packets=%d data_packets=%d elapsed_s=%.3f reason=%s error=%s subscribers=%d",
                self.streamer.device_id, packet_count, data_packet_count, elapsed,
                end_reason, end_error, len(self.subscribers),
            )
            for subscriber in tuple(self.subscribers):
                # Never silently convert an unexpected source disconnect into a valid short recording.
                if end_error and not subscriber.closed:
                    if subscriber.queue.full():
                        try:
                            subscriber.queue.get_nowait()
                        except asyncio.QueueEmpty:
                            pass
                    subscriber.queue.put_nowait(StreamMessage(error=end_error))
                if subscriber.queue.full():
                    try:
                        subscriber.queue.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                subscriber.queue.put_nowait(_END)

    async def stop(self) -> None:
        if self.stopped:
            return
        self.stopping = True
        self.stopped = True
        if self.pump_task and not self.pump_task.done():
            self.pump_task.cancel()
            await asyncio.gather(self.pump_task, return_exceptions=True)
        self.streamer.stop()


class DeviceStreamManager:
    """Own exactly one Scrcpy video session per connected device."""

    def __init__(self):
        self._streams: dict[str, _DeviceStream] = {}
        self._lock = asyncio.Lock()

    async def subscribe(
        self, device_id: str, max_size: int = 1280, bit_rate: int = 4_000_000,
        lossless: bool = False,
    ) -> tuple[DeviceStreamSubscription, ScrcpyVideoStreamMetadata]:
        async with self._lock:
            stream = self._streams.get(device_id)
            if stream is None or stream.stopped:
                stream = _DeviceStream(device_id, max_size, bit_rate)
                self._streams[device_id] = stream
            # Register the subscriber before starting the packet pump. Scrcpy may
            # emit its codec configuration immediately after metadata; starting the
            # pump first can consume that packet before the first subscriber exists,
            # leaving WebCodecs unable to decode any subsequent frames (black screen).
            subscription = DeviceStreamSubscription(self, device_id)
            subscription.lossless = lossless
            stream.subscribers.add(subscription)
            try:
                await stream.start()
                async with stream.packet_lock:
                    subscription.queue.put_nowait(StreamMessage(metadata=stream.metadata))
                    if stream.configuration_packet is not None:
                        subscription.queue.put_nowait(
                            StreamMessage(packet=stream.configuration_packet)
                        )
            except Exception:
                stream.subscribers.discard(subscription)
                if not stream.subscribers:
                    self._streams.pop(device_id, None)
                raise
            return subscription, stream.metadata

    async def unsubscribe(self, subscription: DeviceStreamSubscription) -> None:
        async with self._lock:
            stream = self._streams.get(subscription.device_id)
            if stream is None or subscription not in stream.subscribers:
                return
            stream.subscribers.discard(subscription)
            if subscription.queue.full():
                try:
                    subscription.queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            subscription.queue.put_nowait(_END)
            if not stream.subscribers:
                self._streams.pop(subscription.device_id, None)
                await stream.stop()


device_stream_manager = DeviceStreamManager()
