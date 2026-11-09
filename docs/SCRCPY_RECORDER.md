# Scrcpy 录制链路改造说明

## 目标

将原来的 `screenrecord -> 手机 MP4 -> adb pull` 改为：

```text
Android / HarmonyOS device
       |
       | Scrcpy H.264 stream over ADB forward
       v
DeviceStreamManager (one Scrcpy session per device)
       |                         |
       v                         v
Socket.IO preview          RecorderService
       |                         |
Browser player             FFmpeg -c:v copy
                                 |
                                 v
                           backend/recordings/*.mp4
```

预览和录制共享同一条设备视频流。预览订阅采用有界队列，过慢时丢弃较旧的预览包；录制订阅采用无损队列，避免主动丢帧。

## 变更文件

- `backend/app/services/device_stream_manager.py`: 新增按设备复用 Scrcpy 会话和数据分发。
- `backend/app/services/recorder.py`: 改为从共享 H.264 流读取数据，通过 FFmpeg 在 PC 端封装 MP4。
- `backend/app/services/socket_manager.py`: 实时预览改为订阅共享流，不再为每个 Socket.IO 客户端单独启动 Scrcpy。
- `backend/app/services/scrcpy_stream.py`: 清理逻辑不再执行 `pkill -9 app_process`，避免误杀同设备上的其他 app_process 服务。

Scheduler 的 `RecorderService(device_serial=...)`、`start_recording(execution_id)` 和 `stop_recording()` 调用方式保持不变。

## Windows 环境要求

1. 安装与当前系统架构匹配的 FFmpeg。
2. 将 FFmpeg 的 `bin` 目录加入 PATH，确保新开的 PowerShell 中 `ffmpeg -version` 可用。
3. 如不希望修改 PATH，可设置环境变量：

```powershell
$env:FFMPEG_PATH = "C:\\ffmpeg\\bin\\ffmpeg.exe"
```

4. 确保项目原有 Scrcpy Server 文件存在，且版本与 Python 代码识别出的版本一致。默认查找 backend 或项目根目录下的 `scrcpy-server-v*` / `scrcpy-server`，也可设置 `SCRCPY_SERVER_PATH`。
5. 确保设备已通过 ADB 授权并处于 `device` 状态。

## 验证步骤

先重启后端，连接一台设备，在设备实时预览页面确认视频正常。然后启动一个短测试任务：

- 确认日志出现 Scrcpy server 启动、视频元数据和录制启动日志。
- 任务结束后，检查 `backend/recordings` 下是否生成对应 `exec_<id>_*.mp4`。
- 使用 `ffprobe -v error -show_entries format=duration,size -of default=noprint_wrappers=1 <file>` 检查容器和时长。
- 播放视频，检查开头、结尾和关键画面。
- 同一设备同时打开两个预览客户端，并运行录制任务，确认只启动一条设备 Scrcpy 流。
- 先测试单设备 10 分钟，再逐步增加设备数量。

## 当前边界与注意事项

- 当前录制输入按 H.264 elementary stream 处理；H.265 / AV1 暂不支持。
- 当前 FFmpeg 使用 20 FPS 输入时间基准；需要通过真实设备验证录制时长与视频帧率。后续可基于 Scrcpy PTS 改为显式时间戳封装。
- FFmpeg 采用 `-c:v copy`，不做二次编码；但仍需要本机安装 FFmpeg。
- 本次改造尚未在真实 OPPO / HarmonyOS 设备上执行集成测试。不能仅凭代码提交认定设备兼容。
- 共享流管理器在最后一个订阅者退出时停止 Scrcpy 会话。若希望预览持续保持设备流，预览订阅必须保持连接。
- 不要再通过 `pkill app_process` 清理设备进程。

## 故障定位

- 找不到 FFmpeg：检查 `ffmpeg -version` 和 `FFMPEG_PATH`。
- 找不到 Scrcpy Server：检查 server 文件路径、版本和文件名。
- 连接 socket 失败：检查 ADB 授权、端口转发、设备端 server 日志及 Scrcpy Server 版本。
- MP4 不存在或小于 1 KB：检查 FFmpeg 启动日志、设备流是否发出 configuration / keyframe 数据包。
- 视频时长不准确：当前按 20 FPS 生成 H.264 输入时间基准，需根据设备实测校准或升级为保留 Scrcpy PTS 的封装方式。
