"""Socket.IO video preview backed by the shared per-device Scrcpy stream."""
import asyncio
import logging
import re
import sys

import socketio

from app.services.adb import run_adb
from app.services.device_stream_manager import device_stream_manager

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins="*",
    cors_credentials=False,
    logger=False,
    engineio_logger=False,
)

_stream_tasks: dict[str, asyncio.Task] = {}


async def get_device_resolution(device_id: str) -> tuple[int, int] | None:
    """获取设备原始分辨率（触控坐标换算使用）。"""
    try:
        stdout, _ = await run_adb("shell", "wm", "size", serial=device_id)
        output = stdout.decode(errors="replace")
        match = re.search(r"Physical size:\s*(\d+)x(\d+)", output)
        if not match:
            match = re.search(r"(\d+)x(\d+)", output)
        if match:
            return int(match.group(1)), int(match.group(2))
    except Exception as exc:
        logger.warning("获取设备分辨率失败: %s", exc)
    return None


@sio.event
async def connect(sid, _environ):
    logger.info("Socket.IO 客户端连接: %s", sid)


@sio.event
async def disconnect(sid):
    task = _stream_tasks.pop(sid, None)
    if task:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@sio.on("connect-device")
async def connect_device(sid, data):
    device_id = (data or {}).get("device_id")
    max_size = int((data or {}).get("maxSize", 1280))
    bit_rate = int((data or {}).get("bitRate", 4_000_000))
    if not device_id:
        await sio.emit("error", {"message": "缺少 device_id"}, room=sid)
        return

    old_task = _stream_tasks.pop(sid, None)
    if old_task:
        old_task.cancel()
        await asyncio.gather(old_task, return_exceptions=True)

    async def stream_video():
        subscription = None
        try:
            subscription, metadata = await device_stream_manager.subscribe(
                device_id, max_size=max_size, bit_rate=bit_rate
            )
            resolution = await get_device_resolution(device_id)
            original_width, original_height = resolution or (
                metadata.width, metadata.height
            )
            await sio.emit("video-metadata", {
                "deviceName": metadata.device_name,
                "width": metadata.width,
                "height": metadata.height,
                "codec": metadata.codec,
                "originalWidth": original_width,
                "originalHeight": original_height,
            }, room=sid)

            async for message in subscription:
                if message.error:
                    await sio.emit("error", {"message": message.error}, room=sid)
                    break
                packet = message.packet
                if packet is None:
                    continue
                await sio.emit("video-data", {
                    "type": packet.type,
                    "data": bytes(packet.data),
                    "keyframe": packet.keyframe,
                    "pts": packet.pts,
                }, room=sid)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("设备视频流异常: %s", device_id)
            await sio.emit("error", {"message": str(exc)}, room=sid)
        finally:
            if subscription:
                await subscription.close()
            logger.info("视频预览订阅已释放: device=%s sid=%s", device_id, sid)

    task = asyncio.create_task(stream_video(), name=f"preview-{device_id}-{sid}")
    _stream_tasks[sid] = task


@sio.on("disconnect-device")
async def disconnect_device(sid, _data):
    task = _stream_tasks.pop(sid, None)
    if task:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def create_socket_app(app):
    return socketio.ASGIApp(sio, app)
