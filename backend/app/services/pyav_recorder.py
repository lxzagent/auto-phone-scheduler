"""Lossless Scrcpy H.264 recorder using PyAV for timestamp-aware MP4 muxing.

Scrcpy supplies one encoded H.264 access unit per data packet plus a PTS in
microseconds. We spool the original Annex-B stream and PTS values to disk while
recording, then let FFmpeg's H.264 demuxer (through PyAV) discover codec
parameters and packet boundaries before muxing packets into MP4 with the
original Scrcpy timeline. This avoids decoding/re-encoding and avoids holding
the whole recording in RAM.

The finalizer deliberately verifies packet-count parity. If the demuxer does
not produce exactly one packet per Scrcpy data packet, it fails rather than
silently assigning incorrect timestamps.
"""
from __future__ import annotations

import asyncio
import logging
import os
import struct
from fractions import Fraction
from pathlib import Path
from statistics import median
from typing import BinaryIO

logger = logging.getLogger(__name__)
TIME_BASE = Fraction(1, 1_000_000)
DEFAULT_LAST_DURATION_US = 50_000


class PyAVRecorder:
    """Disk-spooled H.264 recorder; PyAV muxing runs when finalize() is called."""

    def __init__(self, output_path: str | Path):
        self.output_path = Path(output_path)
        self.raw_path = self.output_path.with_suffix(".recording.h264")
        self.pts_path = self.output_path.with_suffix(".recording.pts")
        self._raw: BinaryIO | None = None
        self._pts: BinaryIO | None = None
        self._packet_count = 0
        self._first_pts: int | None = None
        self._last_pts: int | None = None
        self._closed = False

    @property
    def packet_count(self) -> int:
        return self._packet_count

    def start(self) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self._raw = self.raw_path.open("wb")
        self._pts = self.pts_path.open("wb")

    def write_configuration(self, data: bytes) -> None:
        if self._raw is None or self._closed:
            raise RuntimeError("PyAV recorder is not active")
        if data:
            self._raw.write(data)

    def write_packet(self, data: bytes, pts_us: int) -> None:
        if self._raw is None or self._pts is None or self._closed:
            raise RuntimeError("PyAV recorder is not active")
        if not data:
            return
        if pts_us < 0:
            raise ValueError(f"Invalid negative Scrcpy PTS: {pts_us}")
        if self._last_pts is not None and pts_us <= self._last_pts:
            raise ValueError(
                f"Scrcpy PTS is not strictly increasing: previous={self._last_pts}, current={pts_us}"
            )
        self._raw.write(data)
        self._pts.write(struct.pack(">Q", pts_us))
        self._packet_count += 1
        if self._first_pts is None:
            self._first_pts = pts_us
        self._last_pts = pts_us

    def close_input(self) -> None:
        if self._closed:
            return
        self._closed = True
        for handle in (self._raw, self._pts):
            if handle:
                handle.flush()
                handle.close()
        self._raw = None
        self._pts = None

    @staticmethod
    def _read_pts(path: Path) -> list[int]:
        raw = path.read_bytes()
        if len(raw) % 8:
            raise RuntimeError("Scrcpy PTS sidecar is truncated")
        return [item[0] for item in struct.iter_unpack(">Q", raw)]

    def _finalize_sync(self) -> str:
        try:
            import av
        except ImportError as exc:
            raise RuntimeError(
                "PyAV 未安装。请在 backend 环境执行 pip install av==16.1.0"
            ) from exc

        self.close_input()
        if self._packet_count == 0:
            raise RuntimeError("没有收到任何 Scrcpy 视频数据包")

        pts_values = self._read_pts(self.pts_path)
        if len(pts_values) != self._packet_count:
            raise RuntimeError("PTS 记录数量与 Scrcpy 数据包数量不一致")

        temp_output = self.output_path.with_name(
            self.output_path.stem + ".pyav.tmp.mp4"
        )
        temp_output.unlink(missing_ok=True)

        input_container = None
        output_container = None
        demuxed_count = 0
        try:
            input_container = av.open(str(self.raw_path), mode="r", format="h264")
            if not input_container.streams.video:
                raise RuntimeError("PyAV 未能从 Scrcpy H.264 流识别视频流")
            input_stream = input_container.streams.video[0]

            output_container = av.open(str(temp_output), mode="w", format="mp4")
            output_stream = output_container.add_stream_from_template(input_stream)
            output_stream.time_base = TIME_BASE

            # One-packet lookahead lets us preserve real Scrcpy frame intervals.
            pending = None
            pending_pts = None
            pts_index = 0
            durations: list[int] = []
            for packet in input_container.demux(input_stream):
                if packet.size <= 0:
                    continue
                if pts_index >= len(pts_values):
                    raise RuntimeError(
                        "PyAV 解析出的 H.264 包多于 Scrcpy 原始包；拒绝生成时间戳错位的 MP4"
                    )
                source_pts = pts_values[pts_index]
                pts_index += 1
                normalized_pts = source_pts - pts_values[0]
                packet.stream = output_stream
                packet.time_base = TIME_BASE
                packet.pts = normalized_pts
                packet.dts = normalized_pts

                if pending is not None:
                    duration = normalized_pts - pending_pts
                    if duration <= 0:
                        raise RuntimeError("归一化后的 PTS 非单调递增")
                    pending.duration = duration
                    durations.append(duration)
                    output_container.mux(pending)

                pending = packet
                pending_pts = normalized_pts
                demuxed_count += 1

            if demuxed_count != self._packet_count:
                raise RuntimeError(
                    f"PyAV H.264 包数与 Scrcpy 包数不一致: "
                    f"scrcpy={self._packet_count}, demuxed={demuxed_count}。"
                    "为避免错误映射时间戳，本次 MP4 封装已中止。"
                )

            if pending is not None:
                pending.duration = int(median(durations)) if durations else DEFAULT_LAST_DURATION_US
                output_container.mux(pending)

            output_container.close()
            output_container = None
            input_container.close()
            input_container = None

            if not temp_output.exists() or temp_output.stat().st_size < 1024:
                raise RuntimeError("PyAV 输出 MP4 不存在或文件过小")
            os.replace(temp_output, self.output_path)
            return str(self.output_path)
        except Exception:
            if output_container is not None:
                output_container.close()
            if input_container is not None:
                input_container.close()
            temp_output.unlink(missing_ok=True)
            raise
        finally:
            # Keep raw input and PTS sidecar on failure for diagnosis/retry.
            if self.output_path.exists():
                self.raw_path.unlink(missing_ok=True)
                self.pts_path.unlink(missing_ok=True)

    async def finalize(self) -> str:
        """Finalize MP4 without blocking the asyncio event loop."""
        return await asyncio.to_thread(self._finalize_sync)

    def abort(self, *, keep_debug_files: bool = True) -> None:
        self.close_input()
        if not keep_debug_files:
            self.raw_path.unlink(missing_ok=True)
            self.pts_path.unlink(missing_ok=True)
