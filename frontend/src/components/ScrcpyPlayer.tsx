/**
 * Scrcpy 实时视频播放器组件
 * 使用 WebCodecs API 解码 H.264 视频流
 */
import { useCallback, useEffect, useRef, useState } from 'react'
import type { Socket } from 'socket.io-client'
import { io } from 'socket.io-client'
import { ScrcpyVideoCodecId } from '@yume-chan/scrcpy'
import {
  WebCodecsVideoDecoder,
  WebGLVideoFrameRenderer,
  BitmapVideoFrameRenderer,
} from '@yume-chan/scrcpy-decoder-webcodecs'

// Socket.IO 服务器地址
// 在 Docker 环境中通过 nginx 代理时使用相对路径（空字符串）
// 本地开发时使用 localhost:8000
const SOCKET_URL = import.meta.env.VITE_API_URL
  ? import.meta.env.VITE_API_URL.replace('/api', '') || window.location.origin
  : 'http://localhost:8000'

interface ScrcpyPlayerProps {
  deviceId: string
  className?: string
  onFallback?: () => void
  fallbackTimeout?: number
  enableTouch?: boolean  // 是否启用触控操作
  onSwipe?: (startX: number, startY: number, endX: number, endY: number) => void
  onTap?: (x: number, y: number) => void
}

interface VideoMetadata {
  deviceName?: string
  width?: number
  height?: number
  codec?: number
  // 设备原始分辨率（用于触控坐标转换）
  originalWidth?: number
  originalHeight?: number
}

interface VideoPacket {
  type: 'configuration' | 'data'
  data: ArrayBuffer | Uint8Array
  keyframe?: boolean
  pts?: number
}

export function ScrcpyPlayer({
  deviceId,
  className,
  onFallback,
  fallbackTimeout = 5000,
  enableTouch = false,
  onSwipe,
  onTap,
}: ScrcpyPlayerProps) {
  const socketRef = useRef<Socket | null>(null)
  const decoderRef = useRef<WebCodecsVideoDecoder | null>(null)
  const videoContainerRef = useRef<HTMLDivElement>(null)
  const canvasRef = useRef<HTMLCanvasElement | null>(null)
  const fallbackTimerRef = useRef<number | null>(null)
  const hasReceivedDataRef = useRef(false)

  const [status, setStatus] = useState<'connecting' | 'connected' | 'error' | 'disconnected'>('connecting')
  const [errorMessage, setErrorMessage] = useState<string | null>(null)
  const [screenInfo, setScreenInfo] = useState<{ width: number; height: number } | null>(null)
  // 设备原始分辨率（用于触控坐标转换，因为 scrcpy 视频是缩放过的）
  const [deviceResolution, setDeviceResolution] = useState<{ width: number; height: number } | null>(null)

  // 触控状态
  const touchStartRef = useRef<{ x: number; y: number; time: number } | null>(null)
  const isDraggingRef = useRef(false)

  // 创建视频帧渲染器
  const createVideoFrameRenderer = useCallback(async () => {
    if (WebGLVideoFrameRenderer.isSupported) {
      const renderer = new WebGLVideoFrameRenderer()
      return { renderer, element: renderer.canvas as HTMLCanvasElement }
    }
    const renderer = new BitmapVideoFrameRenderer()
    return { renderer, element: renderer.canvas as HTMLCanvasElement }
  }, [])

  // 创建解码器
  const createDecoder = useCallback(async (codecId: ScrcpyVideoCodecId) => {
    if (!WebCodecsVideoDecoder.isSupported) {
      // 检查是否是安全上下文问题
      if (!window.isSecureContext) {
        throw new Error(
          'WebCodecs API 需要安全上下文（HTTPS 或 localhost）。' +
          '当前通过非安全连接访问，请使用 https:// 或在本地访问 localhost'
        )
      }
      throw new Error('浏览器不支持 WebCodecs API，请使用最新版 Chrome/Edge')
    }

    // 清理旧的 canvas
    if (canvasRef.current && canvasRef.current.parentElement) {
      canvasRef.current.parentElement.removeChild(canvasRef.current)
    }

    const { renderer, element } = await createVideoFrameRenderer()
    canvasRef.current = element

    if (videoContainerRef.current && !element.parentElement) {
      videoContainerRef.current.appendChild(element)
    }

    return new WebCodecsVideoDecoder({ codec: codecId, renderer })
  }, [createVideoFrameRenderer])

  // 更新 Canvas 尺寸
  const updateCanvasSize = useCallback(() => {
    const canvas = canvasRef.current
    const container = videoContainerRef.current
    if (!canvas || !container || !screenInfo) return

    const containerWidth = container.clientWidth
    const containerHeight = container.clientHeight
    const { width: originalWidth, height: originalHeight } = screenInfo

    const aspectRatio = originalWidth / originalHeight
    let targetWidth = containerWidth
    let targetHeight = containerWidth / aspectRatio

    if (targetHeight > containerHeight) {
      targetHeight = containerHeight
      targetWidth = containerHeight * aspectRatio
    }

    canvas.width = originalWidth
    canvas.height = originalHeight
    canvas.style.width = `${targetWidth}px`
    canvas.style.height = `${targetHeight}px`
  }, [screenInfo])

  useEffect(() => {
    const handleResize = () => updateCanvasSize()
    window.addEventListener('resize', handleResize)
    return () => window.removeEventListener('resize', handleResize)
  }, [updateCanvasSize])

  useEffect(() => {
    updateCanvasSize()
  }, [screenInfo, updateCanvasSize])

  // 断开连接
  const disconnect = useCallback(() => {
    if (decoderRef.current) {
      try { decoderRef.current.dispose() } catch {}
      decoderRef.current = null
    }
    // 从 DOM 中移除 canvas
    if (canvasRef.current && canvasRef.current.parentElement) {
      canvasRef.current.parentElement.removeChild(canvasRef.current)
    }
    canvasRef.current = null
    if (socketRef.current) {
      socketRef.current.disconnect()
      socketRef.current = null
    }
    if (fallbackTimerRef.current) {
      clearTimeout(fallbackTimerRef.current)
      fallbackTimerRef.current = null
    }
    setStatus('disconnected')
  }, [])

  // 连接设备
  const connect = useCallback(() => {
    disconnect()
    hasReceivedDataRef.current = false
    setStatus('connecting')
    setErrorMessage(null)

    console.log('[ScrcpyPlayer] 正在连接 Socket.IO...')
    const socket = io(SOCKET_URL, { path: '/socket.io', transports: ['websocket', 'polling'], timeout: 10000 })
    socketRef.current = socket

    // The backend may emit the first H.264 packets immediately after metadata.
    // Buffer them while the asynchronous WebCodecs decoder is being initialized,
    // otherwise the SPS/PPS configuration or first keyframe can be lost.
    const earlyVideoPackets: VideoPacket[] = []
    let videoDataHandler: ((data: VideoPacket) => Promise<void>) | null = null
    socket.on('video-data', (data: VideoPacket) => {
      if (videoDataHandler) {
        void videoDataHandler(data)
      } else {
        earlyVideoPackets.push(data)
      }
    })

    socket.on('connect', () => {
      console.log('[ScrcpyPlayer] Socket 已连接, 请求设备:', deviceId)
      socket.emit('connect-device', { device_id: deviceId, maxSize: 1280, bitRate: 4_000_000 })

      fallbackTimerRef.current = window.setTimeout(() => {
        if (!hasReceivedDataRef.current) {
          setStatus('error')
          setErrorMessage('视频流超时')
          socket.close()
          onFallback?.()
        }
      }, fallbackTimeout)
    })

    socket.on('video-metadata', async (metadata: VideoMetadata) => {
      console.log('[ScrcpyPlayer] 收到 video-metadata:', metadata)
      try {
        // 保存设备原始分辨率（用于触控坐标转换）
        if (metadata.originalWidth && metadata.originalHeight) {
          setDeviceResolution({ width: metadata.originalWidth, height: metadata.originalHeight })
          console.log('[ScrcpyPlayer] 设备原始分辨率:', metadata.originalWidth, 'x', metadata.originalHeight)
        }

        if (decoderRef.current) {
          decoderRef.current.dispose()
          decoderRef.current = null
        }

        const codecId = metadata?.codec
          ? (metadata.codec as ScrcpyVideoCodecId)
          : ScrcpyVideoCodecId.H264

        console.log('[ScrcpyPlayer] 创建解码器, codecId:', codecId)
        decoderRef.current = await createDecoder(codecId)
        decoderRef.current.sizeChanged(({ width, height }) => {
          setScreenInfo({ width, height })
        })

        // 获取 WritableStream 的 writer
        const writer = decoderRef.current.writable.getWriter()

        // 设置视频流处理
        let configurationPacketSent = false
        const pendingDataPackets: VideoPacket[] = []

        let frameCount = 0
        const handleVideoData = async (data: VideoPacket) => {
          frameCount++
          if (frameCount <= 3 || frameCount % 100 === 0) {
            console.log(`[ScrcpyPlayer] 收到 video-data #${frameCount}:`, data.type, data.keyframe ? '(keyframe)' : '', 'size:', data.data instanceof ArrayBuffer ? data.data.byteLength : (data.data as Uint8Array).length)
          }
          hasReceivedDataRef.current = true
          if (fallbackTimerRef.current) {
            clearTimeout(fallbackTimerRef.current)
            fallbackTimerRef.current = null
          }

          const payload = data.data instanceof Uint8Array ? data.data : new Uint8Array(data.data)

          // 构造 ScrcpyMediaStreamPacket
          const packet = {
            type: data.type,
            data: payload,
            keyframe: data.keyframe,
            pts: data.pts ? BigInt(data.pts) : undefined,
          }

          if (data.type === 'configuration') {
            configurationPacketSent = true
            await writer.write(packet)
            // 处理待处理的数据包
            for (const p of pendingDataPackets) {
              const d = p.data instanceof Uint8Array ? p.data : new Uint8Array(p.data)
              await writer.write({
                type: p.type,
                data: d,
                keyframe: p.keyframe,
                pts: p.pts ? BigInt(p.pts) : undefined,
              })
            }
            pendingDataPackets.length = 0
          } else if (data.type === 'data') {
            if (!configurationPacketSent) {
              pendingDataPackets.push(data)
            } else {
              await writer.write(packet)
            }
          }
        }

        videoDataHandler = handleVideoData
        for (const packet of earlyVideoPackets.splice(0)) {
          await handleVideoData(packet)
        }
        setStatus('connected')

      } catch (error) {
        console.error('[ScrcpyPlayer] 解码器初始化失败:', error)
        setStatus('error')
        setErrorMessage('解码器初始化失败')
        socket.close()
        onFallback?.()
      }
    })

    socket.on('error', (error: { message?: string }) => {
      console.error('[ScrcpyPlayer] Socket 错误:', error)
      setStatus('error')
      setErrorMessage(error?.message || 'Socket 错误')
    })

    socket.onAny((eventName, ...args) => {
      if (eventName !== 'video-data') {
        console.log('[ScrcpyPlayer] Socket 事件:', eventName, args.length > 0 ? args[0] : '')
      }
    })

    socket.on('connect_error', (error: Error) => {
      console.error('[ScrcpyPlayer] Socket 连接错误:', error.message)
      setStatus('error')
      setErrorMessage(`连接失败: ${error.message}`)
      onFallback?.()
    })

    socket.on('disconnect', () => {
      setStatus('disconnected')
    })
  }, [deviceId, disconnect, createDecoder, fallbackTimeout, onFallback])

  // 只在 deviceId 变化时重新连接
  // 使用 ref 来跟踪组件是否真正卸载，避免 StrictMode 双重调用问题
  const mountedRef = useRef(false)
  const connectTimeoutRef = useRef<number | null>(null)

  useEffect(() => {
    // 清除任何待执行的连接定时器
    if (connectTimeoutRef.current) {
      clearTimeout(connectTimeoutRef.current)
      connectTimeoutRef.current = null
    }

    // 延迟连接，给 StrictMode 的卸载-重新挂载留出时间
    connectTimeoutRef.current = window.setTimeout(() => {
      mountedRef.current = true
      connect()
    }, 50)

    return () => {
      // 清除待执行的连接定时器
      if (connectTimeoutRef.current) {
        clearTimeout(connectTimeoutRef.current)
        connectTimeoutRef.current = null
      }
      // 只有在组件真正挂载后才断开连接
      if (mountedRef.current) {
        mountedRef.current = false
        disconnect()
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [deviceId])

  // 将屏幕坐标转换为设备坐标
  const screenToDevice = useCallback((clientX: number, clientY: number): { x: number; y: number } | null => {
    const canvas = canvasRef.current
    // 使用设备原始分辨率进行坐标转换（而不是 scrcpy 缩放后的分辨率）
    const targetResolution = deviceResolution || screenInfo
    if (!canvas || !targetResolution) {
      console.log('[ScrcpyPlayer] screenToDevice: canvas 或分辨率不存在', { canvas: !!canvas, deviceResolution, screenInfo })
      return null
    }

    const rect = canvas.getBoundingClientRect()
    // 使用 canvas 的显示尺寸（CSS 样式设置的尺寸）
    const displayWidth = rect.width
    const displayHeight = rect.height

    // 计算点击位置相对于 canvas 的比例
    const relativeX = clientX - rect.left
    const relativeY = clientY - rect.top

    // 检查点击是否在 canvas 范围内
    if (relativeX < 0 || relativeX > displayWidth || relativeY < 0 || relativeY > displayHeight) {
      console.log('[ScrcpyPlayer] screenToDevice: 点击在 canvas 范围外', { relativeX, relativeY, displayWidth, displayHeight })
      return null
    }

    // 转换为设备原始分辨率坐标（ADB input 命令需要原始分辨率）
    const x = Math.round((relativeX / displayWidth) * targetResolution.width)
    const y = Math.round((relativeY / displayHeight) * targetResolution.height)

    console.log('[ScrcpyPlayer] screenToDevice:', {
      clientX, clientY,
      rect: { left: rect.left, top: rect.top, width: rect.width, height: rect.height },
      deviceResolution,
      screenInfo,
      result: { x, y }
    })

    return { x, y }
  }, [deviceResolution, screenInfo])

  // 触控事件处理
  const handlePointerDown = useCallback((e: React.PointerEvent) => {
    if (!enableTouch || status !== 'connected') return

    const coords = screenToDevice(e.clientX, e.clientY)
    if (!coords) return

    touchStartRef.current = { x: coords.x, y: coords.y, time: Date.now() }
    isDraggingRef.current = false
  }, [enableTouch, status, screenToDevice])

  const handlePointerMove = useCallback((e: React.PointerEvent) => {
    if (!enableTouch || !touchStartRef.current) return

    const coords = screenToDevice(e.clientX, e.clientY)
    if (!coords) return

    const dx = Math.abs(coords.x - touchStartRef.current.x)
    const dy = Math.abs(coords.y - touchStartRef.current.y)

    // 移动超过 10 像素认为是拖动
    if (dx > 10 || dy > 10) {
      isDraggingRef.current = true
    }
  }, [enableTouch, screenToDevice])

  const handlePointerUp = useCallback((e: React.PointerEvent) => {
    if (!enableTouch || !touchStartRef.current) return

    const coords = screenToDevice(e.clientX, e.clientY)
    if (!coords) return

    const start = touchStartRef.current
    const duration = Date.now() - start.time

    if (isDraggingRef.current) {
      // 滑动操作
      onSwipe?.(start.x, start.y, coords.x, coords.y)
    } else if (duration < 300) {
      // 短按 = 点击
      onTap?.(start.x, start.y)
    }

    touchStartRef.current = null
    isDraggingRef.current = false
  }, [enableTouch, screenToDevice, onSwipe, onTap])

  const handlePointerLeave = useCallback(() => {
    touchStartRef.current = null
    isDraggingRef.current = false
  }, [])

  return (
    <div className={`relative w-full h-full flex items-center justify-center ${className || ''}`}>
      <div
        ref={videoContainerRef}
        className={`relative w-full h-full flex items-center justify-center bg-black/5 rounded-lg ${enableTouch ? 'cursor-pointer' : ''}`}
        onPointerDown={handlePointerDown}
        onPointerMove={handlePointerMove}
        onPointerUp={handlePointerUp}
        onPointerLeave={handlePointerLeave}
      >
        {status !== 'connected' && (
          <div className="absolute inset-0 flex items-center justify-center text-muted-foreground">
            {status === 'connecting' && '连接中...'}
            {status === 'error' && (errorMessage || '连接错误')}
            {status === 'disconnected' && '已断开连接'}
          </div>
        )}
      </div>
    </div>
  )
}

