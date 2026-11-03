"""PC-side MP4 recorder backed by the shared Scrcpy H.264 stream."""
from __future__ import annotations

import asyncio
import logging
import os
import shutil
import time
from datetime import datetime
from pathlib import Path

from app.config import get_settings
from app.services.device_stream_manager import device_stream_manager

logger = logging.getLogger(__name__)
settings = get_settings()

# Keep synchronized with ScrcpyServerOptions.max_fps.
RECORDING_FPS = 20


class RecorderService:
    """Record a device's shared Scrcpy stream directly to a local MP4.

    No video file is created on the phone and no adb pull is required.
    The same per-device stream can be consumed by the live preview.
    """

    def __init__(self, device_serial: str | None = None):
        self.recordings_dir = Path(settings.recordings_dir).resolve()
        self.recordings_dir.mkdir(parents=True, exist_ok=True)
        self._device_serial = device_serial
        self._current_file: str | None = None
        self._subscription = None
        self._ffmpeg: asyncio.subprocess.Process | None = None
        self._writer_task: asyncio.Task | None = None
        self._stopping = False
        self._writer_error: str | None = None
        self._stderr_task: asyncio.Task | None = None
        self._started_monotonic: float | None = None
        self._pyav_recorder = None
        self._backend = os.getenv("RECORDING_BACKEND", "ffmpeg").strip().lower()

    @staticmethod
    def _ffmpeg_path() -> str:
        configured = os.getenv("FFMPEG_PATH")
        if configured:
            return configured
        found = shutil.which("ffmpeg")
        if not found:
            raise RuntimeError(
                "未找到 FFmpeg。请安装 FFmpeg 并将其加入 PATH，"
                "或设置环境变量 FFMPEG_PATH 指向 ffmpeg.exe"
            )
        return found

    async def start_recording(self, execution_id: int) -> str:
        if not self._device_serial:
            raise RuntimeError("启动 Scrcpy 录制必须指定 device_serial")
        if self._ffmpeg or self._subscription:
            raise RuntimeError("当前 RecorderService 已有正在进行的录制")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        output = self.recordings_dir / f"exec_{execution_id}_{timestamp}.mp4"
        ffmpeg = self._ffmpeg_path()

        if self._backend not in {"ffmpeg", "pyav"}:
            raise RuntimeError("RECORDING_BACKEND 仅支持 ffmpeg 或 pyav")

        subscription, metadata = await device_stream_manager.subscribe(
            self._device_serial, max_size=1280, bit_rate=6_000_000,
            lossless=True, queue_capacity=512,
        )
        if metadata.codec != 0x68323634:
            await subscription.close()
            raise RuntimeError(f"当前录制器只支持 H.264，设备返回 codec=0x{metadata.codec:x}")

        process = None
        pyav_recorder = None
        try:
            if self._backend == "ffmpeg":
                command = [
                    ffmpeg, "-hide_banner", "-loglevel", "warning", "-y",
                    "-f", "h264", "-framerate", str(RECORDING_FPS), "-i", "pipe:0",
                    "-an", "-c:v", "copy",
                    # Legacy fallback: fixed-CFR timestamp reconstruction.
                    "-bsf:v",
                    f"setts=pts=N/({RECORDING_FPS}*TB):dts=N/({RECORDING_FPS}*TB):duration=1/({RECORDING_FPS}*TB)",
                    "-movflags", "+faststart",
                    "-f", "mp4", str(output),
                ]
                process = await asyncio.create_subprocess_exec(
                    *command,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.PIPE,
                )
                if process.stdin is None:
                    process.kill()
                    await process.wait()
                    raise RuntimeError("FFmpeg stdin 初始化失败")
            else:
                from app.services.pyav_recorder import PyAVRecorder
                pyav_recorder = PyAVRecorder(output)
                pyav_recorder.start()
        except Exception:
            await subscription.close()
            raise

        self._current_file = str(output)
        self._subscription = subscription
        self._ffmpeg = process
        self._pyav_recorder = pyav_recorder
        self._stopping = False
        self._writer_error = None
        self._started_monotonic = time.monotonic()
        self._stderr_task = (
            asyncio.create_task(
                self._drain_ffmpeg_stderr(process), name=f"ffmpeg-stderr-{execution_id}-{self._device_serial}"
            ) if process is not None else None
        )
        self._writer_task = asyncio.create_task(
            self._write_stream(), name=f"record-{execution_id}-{self._device_serial}"
        )
        logger.info(
            "[Recorder] Scrcpy 录制已启动: device=%s resolution=%sx%s file=%s",
            self._device_serial, metadata.width, metadata.height, output,
        )
        return str(output)

    async def _drain_ffmpeg_stderr(self, process: asyncio.subprocess.Process) -> None:
        """Drain FFmpeg stderr continuously to prevent a full pipe from blocking FFmpeg."""
        if process.stderr is None:
            return
        tail = bytearray()
        try:
            while chunk := await process.stderr.read(4096):
                tail.extend(chunk)
                if len(tail) > 16_384:
                    del tail[:-16_384]
        except asyncio.CancelledError:
            raise
        finally:
            if tail:
                logger.warning("[Recorder] FFmpeg stderr: device=%s detail=%s", self._device_serial,
                               tail.decode("utf-8", errors="replace").strip())

    async def _write_stream(self) -> None:
        assert self._subscription is not None
        if self._backend == "ffmpeg":
            assert self._ffmpeg is not None and self._ffmpeg.stdin is not None
        else:
            assert self._pyav_recorder is not None
        packet_count = 0
        started = time.monotonic()
        last_packet_at = started
        max_inter_packet_gap = 0.0
        first_scrcpy_pts: int | None = None
        last_scrcpy_pts: int | None = None
        try:
            async for message in self._subscription:
                if message.error:
                    raise RuntimeError(f"Scrcpy 视频流异常: {message.error}")
                packet = message.packet
                if packet is None or packet.type not in ("configuration", "data"):
                    continue
                if packet.data:
                    if packet.type == "data":
                        now = time.monotonic()
                        max_inter_packet_gap = max(max_inter_packet_gap, now - last_packet_at)
                        last_packet_at = now
                        packet_count += 1
                        if packet.pts is not None:
                            if first_scrcpy_pts is None:
                                first_scrcpy_pts = packet.pts
                            last_scrcpy_pts = packet.pts
                    if self._backend == "ffmpeg":
                        self._ffmpeg.stdin.write(packet.data)
                        await self._ffmpeg.stdin.drain()
                    elif packet.type == "configuration":
                        self._pyav_recorder.write_configuration(packet.data)
                    else:
                        if packet.pts is None:
                            raise RuntimeError("Scrcpy 视频数据包缺少 PTS，无法进行 PyAV 时间戳封装")
                        self._pyav_recorder.write_packet(packet.data, packet.pts)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not self._stopping:
                self._writer_error = str(exc)
                logger.exception("[Recorder] 视频流写入失败")
        finally:
            if self._backend == "ffmpeg" and self._ffmpeg and self._ffmpeg.stdin and not self._ffmpeg.stdin.is_closing():
                self._ffmpeg.stdin.close()
            elif self._pyav_recorder is not None:
                self._pyav_recorder.close_input()
            pts_span = (
                (last_scrcpy_pts - first_scrcpy_pts) / 1_000_000
                if first_scrcpy_pts is not None and last_scrcpy_pts is not None
                else None
            )
            logger.info(
                "[Recorder] 输入流结束: device=%s packets=%d wall_elapsed_s=%.3f scrcpy_pts_span_s=%s max_packet_gap_s=%.3f stopping=%s",
                self._device_serial, packet_count, time.monotonic() - started, pts_span,
                max_inter_packet_gap, self._stopping,
            )

    async def stop_recording(self) -> str | None:
        if not self._ffmpeg or not self._current_file:
            logger.warning("[Recorder] 没有正在进行的录制")
            return None

        self._stopping = True
        output = self._current_file
        process = self._ffmpeg
        pyav_recorder = self._pyav_recorder
        subscription = self._subscription
        writer = self._writer_task
        stop_started = time.monotonic()

        if subscription:
            await subscription.close()
        if writer:
            try:
                await asyncio.wait_for(writer, timeout=15)
            except asyncio.TimeoutError:
                writer.cancel()
                await asyncio.gather(writer, return_exceptions=True)

        if self._stderr_task:
            await asyncio.gather(self._stderr_task, return_exceptions=True)

        pyav_error = None
        if pyav_recorder is not None and not self._writer_error:
            try:
                await pyav_recorder.finalize()
            except Exception as exc:
                pyav_error = str(exc)
                logger.exception("[Recorder] PyAV MP4 封装失败；原始 H.264/PTS 文件保留用于诊断")
        elif pyav_recorder is not None:
            pyav_recorder.abort(keep_debug_files=True)

        if process is not None:
            try:
                await asyncio.wait_for(process.wait(), timeout=20)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()

        self._ffmpeg = None
        self._pyav_recorder = None
        self._subscription = None
        self._writer_task = None
        self._stderr_task = None
        self._current_file = None

        path = Path(output)
        if (process is not None and process.returncode != 0) or self._writer_error or pyav_error:
            logger.error("[Recorder] 录制失败: backend=%s ffmpeg_exit=%s writer_error=%s pyav_error=%s",
                         self._backend, process.returncode if process else None, self._writer_error, pyav_error)
            path.unlink(missing_ok=True)
            self._writer_error = None
            return None
        self._writer_error = None
        if not path.exists() or path.stat().st_size < 1024:
            logger.error("[Recorder] MP4 文件不存在或过小: %s", output)
            path.unlink(missing_ok=True)
            return None

        elapsed = time.monotonic() - self._started_monotonic if self._started_monotonic else 0.0
        logger.info("[Recorder] MP4 录制完成: device=%s backend=%s elapsed_s=%.3f ffmpeg_exit=%s file=%s bytes=%s stop_wait_s=%.3f",
                    self._device_serial, self._backend, elapsed, process.returncode if process else None, output, path.stat().st_size,
                    time.monotonic() - stop_started)
        self._started_monotonic = None
        return output

    async def get_recording_path(self, execution_id: int) -> str | None:
        files = sorted(
            self.recordings_dir.glob(f"exec_{execution_id}_*.mp4"),
            key=lambda item: item.stat().st_mtime,
            reverse=True,
        )
        return str(files[0]) if files else None

    def delete_recording(self, filepath: str) -> bool:
        try:
            Path(filepath).unlink(missing_ok=True)
            return True
        except OSError:
            logger.exception("[Recorder] 删除录制文件失败: %s", filepath)
            return False
