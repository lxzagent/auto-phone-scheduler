"""Publish Scrcpy H.264 packets to a local MediaMTX RTSP path via FFmpeg."""
from __future__ import annotations

import asyncio
import logging
import re
import shutil
from urllib.parse import quote

from app.config import get_settings

logger = logging.getLogger(__name__)


class MediaMTXPublisher:
    """One FFmpeg RTSP publisher for one device stream.

    Scrcpy remains the capture source. FFmpeg only remuxes the H.264 elementary
    stream to RTSP; MediaMTX then serves it to browsers over WebRTC.
    """

    def __init__(self, device_id: str, fps: int = 20):
        settings = get_settings()
        self.device_id = device_id
        self.fps = fps
        self.rtsp_base = settings.mediamtx_rtsp_url.rstrip("/")
        self.process: asyncio.subprocess.Process | None = None
        self._stderr_task: asyncio.Task | None = None
        self._closed = False
        safe_id = re.sub(r"[^a-zA-Z0-9_-]", "_", device_id)
        self.path = f"devices/{safe_id}"
        self.url = f"{self.rtsp_base}/{quote(self.path, safe='/')}"

    @staticmethod
    def _ffmpeg_path() -> str:
        configured = get_settings().ffmpeg_path
        path = configured or shutil.which("ffmpeg")
        if not path:
            raise RuntimeError("MediaMTX 发布需要 FFmpeg；请安装 FFmpeg 或设置 FFMPEG_PATH")
        return path

    async def start(self) -> None:
        if self.process and self.process.returncode is None:
            return
        command = [
            self._ffmpeg_path(), "-hide_banner", "-loglevel", "warning",
            "-f", "h264", "-framerate", str(self.fps), "-i", "pipe:0",
            "-an", "-c:v", "copy", "-f", "rtsp", "-rtsp_transport", "tcp",
            self.url,
        ]
        self.process = await asyncio.create_subprocess_exec(
            *command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        self._stderr_task = asyncio.create_task(self._drain_stderr())
        logger.info("[MediaMTX] FFmpeg publisher started: device=%s path=%s pid=%s",
                    self.device_id, self.path, self.process.pid)

    async def _drain_stderr(self) -> None:
        process = self.process
        if not process or not process.stderr:
            return
        while True:
            line = await process.stderr.readline()
            if not line:
                break
            logger.warning("[MediaMTX][FFmpeg][%s] %s",
                           self.device_id, line.decode(errors="replace").rstrip())

    async def write(self, payload: bytes) -> None:
        if not payload:
            return
        process = self.process
        if not process or process.returncode is not None or process.stdin is None:
            raise RuntimeError(f"MediaMTX FFmpeg publisher is not running: {self.device_id}")
        process.stdin.write(payload)
        await process.stdin.drain()

    async def stop(self) -> None:
        if self._closed:
            return
        self._closed = True
        process = self.process
        if process:
            if process.stdin and not process.stdin.is_closing():
                process.stdin.close()
            try:
                await asyncio.wait_for(process.wait(), timeout=8)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
            if self._stderr_task:
                await asyncio.gather(self._stderr_task, return_exceptions=True)
            logger.info("[MediaMTX] publisher stopped: device=%s exit=%s",
                        self.device_id, process.returncode)
        self.process = None
