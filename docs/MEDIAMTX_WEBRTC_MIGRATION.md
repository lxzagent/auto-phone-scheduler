# Scrcpy + MediaMTX + WebRTC 改造实施方案

## 目标架构

Windows 单机运行 FastAPI、Scrcpy、FFmpeg、MediaMTX。Scrcpy 是唯一手机采集入口；DeviceStreamManager 每设备一个 Scrcpy session；FFmpeg 将 H.264 elementary stream 复用为 RTSP 发布到 MediaMTX；MediaMTX 通过 WebRTC/WHEP 分发给浏览器。Socket.IO 只传任务状态、日志、触控和设备控制，不再承载持续视频帧。

```text
Phone -> ADB/Scrcpy -> DeviceStreamManager
                         ├─ task/recorder lease -> FFmpeg -> MP4
                         └─ publisher lease -> FFmpeg RTSP -> MediaMTX -> WebRTC -> React <video>
Socket.IO: task events / logs / touch / control only
```

## 当前代码基线与必须修复的生命周期问题

本分支从 `feature/scrcpy-recorder-pipeline` 派生。已有 ScrcpyStreamer、DeviceStreamManager、RecorderService、Socket.IO 视频订阅和 WebCodecs 播放器。当前 DeviceStreamManager 在最后一个 subscriber 释放时停止 Scrcpy；Socket.IO disconnect 会关闭预览 subscription。因此刷新页面有机会释放最后一个订阅并停止底层流。**仅换成 MediaMTX 不会自动解决该问题。**

必须改成有 owner 的租约：
- `task` / `recorder` / `publisher` 租约由后端 Scheduler/服务持有。
- `preview` 租约只代表一个浏览器观看会话。
- 浏览器刷新只关闭自己的 WebRTC PeerConnection，不调用 publisher stop，也不影响 task/recorder lease。
- 只有明确的任务结束/停止流程才释放任务租约；无任务时才可按策略停止空闲流。
- 每个订阅者独立有界队列。不能在全局 packet pump 锁内等待慢 FFmpeg stdin drain；慢消费者应触发背压/丢帧策略，实时流丢帧后重新等待 IDR，录制流另行保证完整性。

## Publisher 服务设计

新增的 `backend/app/services/mediamtx_publisher.py` 提供 per-device FFmpeg RTSP publisher：
1. 订阅 DeviceStreamManager，先取得 metadata 和 SPS/PPS configuration。
2. FFmpeg 从 stdin 读取 H.264，使用 `-f h264 -framerate 20 -i pipe:0 -c:v copy -f rtsp -rtsp_transport tcp rtsp://127.0.0.1:8554/devices/<safe-device-id>`。
3. 持续 drain FFmpeg stderr 到 Python 日志，监测退出码。
4. publisher lease 释放时关闭 stdin、等待 FFmpeg 正常退出，超时再 kill。
5. FFmpeg/MediaMTX 崩溃时记录状态并由后端 supervisor 重启 publisher；不要由浏览器重连触发采集进程重启。

注意：Scrcpy packet 带 PTS，但 raw H.264 demuxer的 `-framerate` 会生成固定时间基，并不自动继承 packet.pts。第一版固定 20fps 仅用于 PoC；必须实测延迟/帧率/抖动。若需精确保留 Scrcpy PTS，应使用 PyAV/GStreamer 或自定义 muxer 注入时间戳。直通 `-c:v copy` 不重新编码；若浏览器不兼容 H.264 profile/B-frame，再明确启用无 B 帧低延迟转码。

## MediaMTX 配置和进程管理

MediaMTX 官方提供 Windows 单文件可执行程序，无需 Docker。建议放在 `backend/tools/mediamtx/mediamtx.exe`，配置放 `config/mediamtx.yml`，FastAPI lifespan 管理 MediaMTX 子进程及健康检查。初始只绑定 localhost：
- RTSP publish: TCP 8554
- WebRTC HTTP/WHEP: TCP 8889
- WebRTC media: UDP 8189
- 可选 HLS: TCP 8888（仅用于排障）

当浏览器在另一台局域网电脑时，必须将 `webrtcAdditionalHosts` 配置为后端电脑 LAN IP，并放通 8889/TCP、8189/UDP。不要把 localhost 播放 URL 返回给远端浏览器。生产环境需增加发布鉴权、访问控制和 Windows 防火墙规则。

环境变量建议：
- `MEDIAMTX_ENABLED=true`
- `MEDIAMTX_PATH=./tools/mediamtx/mediamtx.exe`
- `MEDIAMTX_CONFIG=./config/mediamtx.yml`
- `MEDIAMTX_RTSP_URL=rtsp://127.0.0.1:8554`
- `MEDIAMTX_WEBRTC_URL=http://127.0.0.1:8889`
- `FFMPEG_PATH=C:/tools/ffmpeg/bin/ffmpeg.exe`

## 前端改造

新增 `MediaMtxPlayer.tsx` 与独立 WHEP/WebRTC hook，替换 ScrcpyPlayer 的视频渲染职责：
- 使用原生 `<video autoPlay playsInline muted>`。
- 通过 MediaMTX WHEP endpoint `http://<host>:8889/devices/<id>/whep` 建立 RTCPeerConnection，处理 offer/answer、ICE、ontrack、连接状态和重试。
- 播放 URL 由后端 API 返回，不能硬编码 localhost。
- 组件卸载只关闭 PeerConnection。
- 触控覆盖层保留，坐标根据 video 元素显示矩形与设备分辨率映射；触控事件仍通过 Socket.IO/HTTP 控制通道发送。
- 保留旧 ScrcpyPlayer feature flag 回退，完成验收后再删除旧 video-data 事件。

## API 建议

- `GET /api/streams/{device_id}`: 返回 publisher 状态、WebRTC playback URL、分辨率、最近视频包时间。
- `POST /api/streams/{device_id}/start`: 受控启动 publisher/lease（或由任务生命周期自动 ensure）。
- `POST /api/streams/{device_id}/stop`: 仅管理员/任务管理流程可调用；必须确认没有 task/recorder lease。
- 状态字段：`state`、`last_packet_at`、`ffmpeg_pid`、`ffmpeg_exit_code`、`queue_depth`、`dropped_packets`、`lease_owners`。

## 分阶段实施和验收

### Phase 0：本机 PoC
1. 下载 Windows MediaMTX 和 FFmpeg。
2. 用 FFmpeg 测试文件发布 RTSP，确认 MediaMTX WebRTC 页面可播放。
3. 用 Scrcpy packet 经 FFmpeg 发布 RTSP，再用浏览器 WebRTC 播放。
4. 测量首帧时间、端到端延迟、帧率、CPU/RAM、断线恢复。

### Phase 1：后端
1. MediaMTXProcessManager：FastAPI lifespan 启停，健康检查和退出监控。
2. Publisher supervisor：每设备一个 publisher lease，缓存 config，等待 IDR，错误自动恢复。
3. 改造 DeviceStreamManager：owner leases，preview 与 task/recorder/publisher 解耦。
4. 修复队列背压和慢消费者隔离。
5. 新增状态 API 与结构化日志。
6. 录制器 MP4 lifecycle 与任务 lifecycle 绑定，不受网页生命周期影响。

### Phase 2：前端
1. WHEP hook + MediaMtxPlayer。
2. feature flag 双播放器切换。
3. 页面刷新 20 次，自动重新建立 WebRTC；后台任务/MP4 连续运行。
4. 删除 Socket.IO video-data 传输，仅保留控制与状态事件。

### Phase 3：多设备和稳定性
- 单设备持续运行 30 分钟无黑屏、卡死。
- 刷新页面 20 次，任务不停止，MP4 文件持续增长，WebRTC 自动恢复。
- 两个浏览器同时观看同一设备，底层 Scrcpy session 仍为 1。
- 5 台设备并发 30 分钟，记录 CPU、RAM、带宽、丢包、延迟。
- 设备拔线、ADB offline、MediaMTX 重启、FFmpeg 崩溃均有明确状态和恢复策略。
- 任务完成后释放 ADB forward、Scrcpy、FFmpeg、publisher/task leases；浏览器 PeerConnection 正常关闭。

## 官方参考

- MediaMTX Windows/概览：https://mediamtx.org/docs/kickoff/introduction
- FFmpeg RTSP 发布：https://mediamtx.org/docs/publish/ffmpeg
- WebRTC/WHEP 读取：https://mediamtx.org/docs/read/webrtc
- WebRTC 编码兼容和 ICE 排障：https://mediamtx.org/docs/features/webrtc-specific-features
- Scrcpy：https://github.com/Genymobile/scrcpy
