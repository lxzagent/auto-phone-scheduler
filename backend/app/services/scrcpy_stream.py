"""Scrcpy 视频流实现"""

import asyncio
import logging
import os
import socket
from pathlib import Path
from typing import Optional, AsyncIterator

from app.services.adb import get_adb_command, get_adb_host
from app.services.scrcpy_protocol import (
    PTS_CONFIG,
    PTS_KEYFRAME,
    SCRCPY_CODEC_NAME_TO_ID,
    SCRCPY_KNOWN_CODECS,
    ScrcpyMediaStreamPacket,
    ScrcpyVideoStreamMetadata,
    ScrcpyVideoStreamOptions,
    ScrcpyServerOptions,
)

logger = logging.getLogger(__name__)


class ScrcpyStreamer:
    """Scrcpy 服务器生命周期和视频流解析"""

    def __init__(
        self,
        device_id: str,
        max_size: int = 1280,
        bit_rate: int = 4_000_000,
        port: int = 27183,
        idr_interval_s: int = 1,
        stream_options: Optional[ScrcpyVideoStreamOptions] = None,
    ):
        self.device_id = device_id
        self.max_size = max_size
        self.bit_rate = bit_rate
        self.port = port
        self.idr_interval_s = idr_interval_s
        self.stream_options = stream_options or ScrcpyVideoStreamOptions()

        self.scrcpy_process: Optional[asyncio.subprocess.Process] = None
        self.tcp_socket: Optional[socket.socket] = None
        self.forward_cleanup_needed = False

        self._read_buffer = bytearray()
        self._metadata: Optional[ScrcpyVideoStreamMetadata] = None
        self._dummy_byte_skipped = False
        self._running = False

        self.scrcpy_server_path, self.scrcpy_version = self._find_scrcpy_server()

    def _find_scrcpy_server(self) -> tuple[str, str]:
        """查找 scrcpy-server 路径并返回 (路径, 版本号)"""
        import re

        def extract_version(path: str) -> str:
            """从文件名或路径中提取版本号"""
            # 尝试从文件名提取版本号，如 scrcpy-server-v3.3.4
            match = re.search(r'scrcpy-server-v?([\d.]+)', path)
            if match:
                return match.group(1)
            # 从 Homebrew 路径提取，如 /opt/homebrew/Cellar/scrcpy/3.3/...
            match = re.search(r'/scrcpy/([\d.]+)/', path)
            if match:
                return match.group(1)
            # 默认版本
            return "3.3.4"

        # 优先级 1: backend 目录下带版本号的文件
        backend_root = Path(__file__).parent.parent.parent
        for pattern in ["scrcpy-server-v*", "scrcpy-server"]:
            for server_file in backend_root.glob(pattern):
                if server_file.is_file():
                    version = extract_version(str(server_file))
                    logger.info(f"找到 scrcpy-server: {server_file}, 版本: {version}")
                    return str(server_file), version

        # 优先级 2: 项目根目录
        project_root = backend_root.parent
        for pattern in ["scrcpy-server-v*", "scrcpy-server"]:
            for server_file in project_root.glob(pattern):
                if server_file.is_file():
                    version = extract_version(str(server_file))
                    logger.info(f"找到 scrcpy-server: {server_file}, 版本: {version}")
                    return str(server_file), version

        # 优先级 3: 环境变量
        scrcpy_server = os.getenv("SCRCPY_SERVER_PATH")
        if scrcpy_server and os.path.exists(scrcpy_server):
            version = extract_version(scrcpy_server)
            return scrcpy_server, version

        raise FileNotFoundError(
            "scrcpy-server 未找到。请将 scrcpy-server-v3.3.4 放到 backend 目录或设置 SCRCPY_SERVER_PATH 环境变量"
        )

    def _adb_cmd(self, *args: str) -> list[str]:
        """构建 adb 命令"""
        cmd = get_adb_command()
        cmd.extend(["-s", self.device_id])
        cmd.extend(args)
        return cmd

    async def _run_cmd(self, cmd: list[str], timeout: float = 10) -> tuple[int, str, str]:
        """运行命令"""
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            return proc.returncode or 0, stdout.decode(), stderr.decode()
        except asyncio.TimeoutError:
            return -1, "", "timeout"
        except Exception as e:
            return -1, "", str(e)

    async def _cleanup_existing_server(self) -> None:
        """仅清理本实例端口转发，不杀死设备上的其他 app_process。

        旧实现 pkill -9 app_process 会误杀同一设备上的其他 Scrcpy 会话
        或依赖 app_process 的服务。设备服务的生命周期由共享流管理器负责。
        """
        logger.info("[scrcpy] 清理本实例端口转发: %s", self.port)
        cmd = self._adb_cmd("forward", "--remove", f"tcp:{self.port}")
        await self._run_cmd(cmd, timeout=2)

    async def _push_server(self) -> None:
        """推送 scrcpy-server 到设备"""
        print(f"[scrcpy] 推送 server: {self.scrcpy_server_path}...", flush=True)
        cmd = self._adb_cmd("push", self.scrcpy_server_path, "/data/local/tmp/scrcpy-server")
        returncode, _, stderr = await self._run_cmd(cmd, timeout=30)
        if returncode != 0:
            raise RuntimeError(f"推送 scrcpy-server 失败: {stderr}")
        print("[scrcpy] 推送 server: 完成", flush=True)

    async def _setup_port_forward(self) -> None:
        """设置端口转发"""
        print(f"[scrcpy] 设置端口转发: {self.port}...", flush=True)
        cmd = self._adb_cmd("forward", f"tcp:{self.port}", "localabstract:scrcpy")
        returncode, _, stderr = await self._run_cmd(cmd)
        if returncode != 0:
            raise RuntimeError(f"端口转发失败: {stderr}")
        self.forward_cleanup_needed = True
        print("[scrcpy] 设置端口转发: 完成", flush=True)

    def _build_server_options(self) -> ScrcpyServerOptions:
        """构建服务器选项"""
        codec_options = f"i-frame-interval={self.idr_interval_s}"
        return ScrcpyServerOptions(
            max_size=self.max_size,
            bit_rate=self.bit_rate,
            max_fps=20,
            video_codec=self.stream_options.video_codec,
            send_frame_meta=self.stream_options.send_frame_meta,
            send_device_meta=self.stream_options.send_device_meta,
            send_codec_meta=self.stream_options.send_codec_meta,
            send_dummy_byte=self.stream_options.send_dummy_byte,
            video_codec_options=codec_options,
        )

    async def _start_server(self) -> None:
        """启动 scrcpy 服务器（直接在手机上运行，不依赖客户端 scrcpy 二进制文件）"""
        options = self._build_server_options()

        logger.info(f"启动 scrcpy 服务器，版本: {self.scrcpy_version}")

        cmd = self._adb_cmd(
            "shell",
            "CLASSPATH=/data/local/tmp/scrcpy-server",
            "app_process", "/", "com.genymobile.scrcpy.Server", self.scrcpy_version,
            f"max_size={options.max_size}",
            f"video_bit_rate={options.bit_rate}",
            f"max_fps={options.max_fps}",
            f"tunnel_forward={str(options.tunnel_forward).lower()}",
            f"audio={str(options.audio).lower()}",
            f"control={str(options.control).lower()}",
            f"cleanup={str(options.cleanup).lower()}",
            f"video_codec={options.video_codec}",
            f"send_frame_meta={str(options.send_frame_meta).lower()}",
            f"send_device_meta={str(options.send_device_meta).lower()}",
            f"send_codec_meta={str(options.send_codec_meta).lower()}",
            f"send_dummy_byte={str(options.send_dummy_byte).lower()}",
        )
        if options.video_codec_options:
            cmd.append(f"video_codec_options={options.video_codec_options}")

        print(f"[scrcpy] 启动服务器...", flush=True)
        self.scrcpy_process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        print(f"[scrcpy] 服务器进程已创建, PID={self.scrcpy_process.pid}", flush=True)
        # 减少等待时间，socket连接会自动重试
        await asyncio.sleep(1)

        if self.scrcpy_process.returncode is not None:
            _, stderr = await self.scrcpy_process.communicate()
            raise RuntimeError(f"scrcpy 服务器启动失败: {stderr.decode()}")
        print("[scrcpy] 服务器启动成功", flush=True)

    async def _connect_socket(self) -> None:
        """连接 TCP socket"""
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.settimeout(5)
        self.tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2 * 1024 * 1024)

        # 端口转发在 ADB server 所在机器上，需要连接到那个地址
        adb_host = get_adb_host()
        logger.info(f"连接 scrcpy socket: {adb_host}:{self.port}")

        for _ in range(5):
            try:
                self.tcp_socket.connect((adb_host, self.port))
                self.tcp_socket.setblocking(False)
                return
            except (ConnectionRefusedError, OSError):
                await asyncio.sleep(0.5)

        raise ConnectionError(f"连接 scrcpy 服务器失败: {adb_host}:{self.port}")

    async def start(self) -> None:
        """启动 scrcpy 服务器并建立连接"""
        self._read_buffer.clear()
        self._metadata = None
        self._dummy_byte_skipped = False
        self._running = True

        try:
            logger.info(f"清理已有 scrcpy 进程: {self.device_id}")
            await self._cleanup_existing_server()

            logger.info("推送 scrcpy-server 到设备")
            await self._push_server()

            logger.info(f"设置端口转发: {self.port}")
            await self._setup_port_forward()

            logger.info("启动 scrcpy 服务器")
            await self._start_server()

            logger.info("连接 TCP socket")
            await self._connect_socket()
            logger.info("连接成功!")

        except Exception as e:
            logger.exception(f"启动失败: {e}")
            self.stop()
            raise

    async def _read_exactly(self, size: int) -> bytes:
        """精确读取指定字节数"""
        if not self.tcp_socket:
            raise ConnectionError("Socket 未连接")

        loop = asyncio.get_event_loop()
        while len(self._read_buffer) < size:
            try:
                chunk = await loop.sock_recv(self.tcp_socket, max(4096, size - len(self._read_buffer)))
                if not chunk:
                    raise ConnectionError("Socket 被远程关闭")
                self._read_buffer.extend(chunk)
            except BlockingIOError:
                await asyncio.sleep(0.01)

        data = bytes(self._read_buffer[:size])
        del self._read_buffer[:size]
        return data

    async def _read_u16(self) -> int:
        return int.from_bytes(await self._read_exactly(2), "big")

    async def _read_u32(self) -> int:
        return int.from_bytes(await self._read_exactly(4), "big")

    async def _read_u64(self) -> int:
        return int.from_bytes(await self._read_exactly(8), "big")

    async def read_video_metadata(self) -> ScrcpyVideoStreamMetadata:
        """读取视频流元数据"""
        if self._metadata is not None:
            return self._metadata

        if self.stream_options.send_dummy_byte and not self._dummy_byte_skipped:
            await self._read_exactly(1)
            self._dummy_byte_skipped = True

        device_name = None
        width = None
        height = None
        codec = SCRCPY_CODEC_NAME_TO_ID.get(
            self.stream_options.video_codec, SCRCPY_CODEC_NAME_TO_ID["h264"]
        )

        if self.stream_options.send_device_meta:
            raw_name = await self._read_exactly(64)
            device_name = raw_name.split(b"\x00", 1)[0].decode("utf-8", errors="replace")

        if self.stream_options.send_codec_meta:
            codec_value = await self._read_u32()
            if codec_value in SCRCPY_KNOWN_CODECS:
                codec = codec_value
                width = await self._read_u32()
                height = await self._read_u32()
            else:
                width = (codec_value >> 16) & 0xFFFF
                height = codec_value & 0xFFFF
        else:
            if self.stream_options.send_device_meta:
                width = await self._read_u16()
                height = await self._read_u16()

        self._metadata = ScrcpyVideoStreamMetadata(
            device_name=device_name,
            width=width,
            height=height,
            codec=codec,
        )
        return self._metadata

    async def read_media_packet(self) -> ScrcpyMediaStreamPacket:
        """读取一个 Scrcpy 媒体数据包"""
        if not self.stream_options.send_frame_meta:
            raise RuntimeError("send_frame_meta 已禁用，无法解析数据包")

        if self._metadata is None:
            await self.read_video_metadata()

        pts = await self._read_u64()
        data_length = await self._read_u32()
        payload = await self._read_exactly(data_length)

        if pts == PTS_CONFIG:
            return ScrcpyMediaStreamPacket(type="configuration", data=payload)

        if pts & PTS_KEYFRAME:
            return ScrcpyMediaStreamPacket(
                type="data",
                data=payload,
                keyframe=True,
                pts=pts & ~PTS_KEYFRAME,
            )

        return ScrcpyMediaStreamPacket(
            type="data",
            data=payload,
            keyframe=False,
            pts=pts,
        )

    async def iter_packets(self) -> AsyncIterator[ScrcpyMediaStreamPacket]:
        """持续生成数据包"""
        while self._running:
            try:
                yield await self.read_media_packet()
            except ConnectionError:
                break
            except Exception as e:
                logger.error(f"读取数据包失败: {e}")
                break

    def stop(self) -> None:
        """停止 scrcpy 服务器并清理资源"""
        self._running = False

        if self.tcp_socket:
            try:
                self.tcp_socket.close()
            except Exception:
                pass
            self.tcp_socket = None

        if self.scrcpy_process:
            try:
                self.scrcpy_process.terminate()
            except Exception:
                pass
            self.scrcpy_process = None

        if self.forward_cleanup_needed:
            try:
                import subprocess
                cmd = get_adb_command() + ["-s", self.device_id, "forward", "--remove", f"tcp:{self.port}"]
                subprocess.run(cmd, capture_output=True, timeout=2)
            except Exception:
                pass
            self.forward_cleanup_needed = False

    def __del__(self):
        self.stop()


# 全局流管理器
_active_streamers: dict[str, ScrcpyStreamer] = {}


def get_streamer(device_id: str) -> Optional[ScrcpyStreamer]:
    """获取设备的活跃流"""
    return _active_streamers.get(device_id)


def set_streamer(device_id: str, streamer: ScrcpyStreamer) -> None:
    """设置设备的流"""
    _active_streamers[device_id] = streamer


def remove_streamer(device_id: str) -> None:
    """移除设备的流"""
    if device_id in _active_streamers:
        _active_streamers[device_id].stop()
        del _active_streamers[device_id]

