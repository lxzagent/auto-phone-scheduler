"""Per-device shared Scrcpy stream fan-out for preview and recording.

A single Scrcpy encoder/session is shared by all consumers of a device. Each
subscriber receives the same metadata and H.264 packets.
"""
from __future__ import annotations

import asyncio
import logging
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
        self.queue: asyncio.Queue = asyncio.Queue()
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
        port = 28000 + (zlib.crc32(device_id.encode("utf-8")) % 20000)
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

    async def start(self) -> None:
        async with self.start_lock:
            if self.pump_task and not self.pump_task.done():
                return
            await self.streamer.start()
            self.metadata = await self.streamer.read_video_metadata()
            self.pump_task = asyncio.create_task(self._pump(), name=f"scrcpy-pump-{self.streamer.device_id}")

    async def _pump(self) -> None:
        try:
            async for packet in self.streamer.iter_packets():
                message = StreamMessage(packet=packet)
                # Keep the pump independent from slow consumers. Recording subscribers
                # are expected to drain continuously; queue metrics can be added later.
                for subscriber in tuple(self.subscribers):
                    if not subscriber.closed:
                        subscriber.queue.put_nowait(message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Scrcpy stream failed for %s", self.streamer.device_id)
            for subscriber in tuple(self.subscribers):
                subscriber.queue.put_nowait(StreamMessage(error=str(exc)))
        finally:
            for subscriber in tuple(self.subscribers):
                subscriber.queue.put_nowait(_END)

    async def stop(self) -> None:
        if self.stopped:
            return
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
        self, device_id: str, max_size: int = 1280, bit_rate: int = 4_000_000
    ) -> tuple[DeviceStreamSubscription, ScrcpyVideoStreamMetadata]:
        async with self._lock:
            stream = self._streams.get(device_id)
            if stream is None or stream.stopped:
                stream = _DeviceStream(device_id, max_size, bit_rate)
                self._streams[device_id] = stream
            await stream.start()
            subscription = DeviceStreamSubscription(self, device_id)
            stream.subscribers.add(subscription)
            subscription.queue.put_nowait(StreamMessage(metadata=stream.metadata))
            return subscription, stream.metadata

    async def unsubscribe(self, subscription: DeviceStreamSubscription) -> None:
        async with self._lock:
            stream = self._streams.get(subscription.device_id)
            if stream is None:
                return
            stream.subscribers.discard(subscription)
            subscription.queue.put_nowait(_END)
            if not stream.subscribers:
                self._streams.pop(subscription.device_id, None)
                await stream.stop()


device_stream_manager = DeviceStreamManager()
