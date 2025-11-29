"use client"

import { useState, useCallback, useEffect, useRef } from "react"
import type { DeviceConfig, DeviceState, LogEntry } from "@/lib/types"

interface WSCommand {
  action: string
  data?: any
}

interface WSMessage {
  type: string
  payload: any
}

const createDefaultConfig = (timestamp: string): DeviceConfig => ({
  device_id: "",
  measurement_unit: "",
  unit_price_msat: 0,
  reporting_strategy: "interval",
  reporting_interval: 0,
  heartbeat_interval: 0,
  authorize_request_msat: 0,
  timestamp,
})

const createDisconnectLog = (): LogEntry => ({
  id: typeof crypto !== "undefined" && typeof crypto.randomUUID === "function" ? crypto.randomUUID() : `log-${Date.now()}`,
  timestamp: new Date().toISOString(),
  message: "Device disconnected",
  type: "error",
})

const buildOfflineState = (prev: DeviceState | null): DeviceState => {
  const logEntry = createDisconnectLog()
  const baseConfig = prev?.config ?? createDefaultConfig(logEntry.timestamp)

  return {
    deviceId: prev?.deviceId ?? "",
    deviceStatus: "OFFLINE",
    appliances: prev?.appliances ?? [],
    balance: null,
    config: {
      ...baseConfig,
      timestamp: baseConfig.timestamp || logEntry.timestamp,
    },
    totalConsumption: prev?.totalConsumption ?? 0,
    instantPower: 0,
    invoice: null,
    currentAuthorization: prev?.currentAuthorization ?? null,
    logs: [logEntry],
    mqttStatus: "offline",
  }
}

export function useBackend() {
  const [state, setState] = useState<DeviceState | null>(null)
  const [connectionStatus, setConnectionStatus] = useState<"disconnected" | "connecting" | "connected" | "error">("disconnected")
  const wsRef = useRef<WebSocket | null>(null)
  const reconnectTimeoutRef = useRef<NodeJS.Timeout | undefined>(undefined)

  const connect = useCallback(() => {
    const backendUrl = process.env.NEXT_PUBLIC_BACKEND_WS_URL || "ws://localhost:8080/ws"
    
    setConnectionStatus("connecting")
    const ws = new WebSocket(backendUrl)
    wsRef.current = ws

    ws.onopen = () => {
      console.log("[Backend] Connected")
      setConnectionStatus("connected")
    }

    ws.onmessage = (event) => {
      try {
        const message: WSMessage = JSON.parse(event.data)
        console.log("[Backend] Message received:", message.type, message.payload)
        
        if (message.type === "state") {
          console.log("[Backend] State update - appliances:", message.payload.appliances?.length)
          console.log("[Backend] State update - deviceStatus:", message.payload.deviceStatus)
          setState(message.payload as DeviceState)
        }
      } catch (error) {
        console.error("[Backend] Error parsing message:", error)
      }
    }

    ws.onerror = (error) => {
      console.error("[Backend] WebSocket error:", error)
      setConnectionStatus("error")
    }

    ws.onclose = () => {
      console.log("[Backend] Disconnected")
      setConnectionStatus("disconnected")
      setState((prev) => buildOfflineState(prev))
      
      // Auto-reconnect after 3 seconds
      reconnectTimeoutRef.current = setTimeout(() => {
        connect()
      }, 3000)
    }
  }, [])

  const disconnect = useCallback(() => {
    if (reconnectTimeoutRef.current) {
      clearTimeout(reconnectTimeoutRef.current)
    }
    if (wsRef.current) {
      wsRef.current.close()
      wsRef.current = null
    }
  }, [])

  const sendCommand = useCallback((action: string, data?: any) => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      const command: WSCommand = { action, data }
      console.log("[Backend] Sending command:", command)
      wsRef.current.send(JSON.stringify(command))
    } else {
      console.warn("[Backend] WebSocket not ready. State:", wsRef.current?.readyState)
    }
  }, [])

  // Auto-connect on mount
  useEffect(() => {
    connect()
    return () => disconnect()
  }, [connect, disconnect])

  return {
    state,
    connectionStatus,
    sendCommand,
    connect,
    disconnect,
  }
}
