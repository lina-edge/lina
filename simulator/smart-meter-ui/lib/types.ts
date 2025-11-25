// MQTT Message Types for Smart Meter IoT Simulation

export interface DeviceConfig {
  device_id: string
  unit: string
  unit_price: string
  pricing_unit: string
  reporting_strategy: "interval" | "delta" | "total"
  reporting_interval: number
  heartbeat_interval: number
  authorize_request_msat: number
  timestamp: string
}

export interface HeartbeatMessage {
  device_id: string
  status: "ONLINE" | "OFFLINE"
  timestamp: string
}

export interface AuthorizeRequest {
  device_id: string
  request_id: string
  request_msat: number
  reason: "STARTUP" | "TOPUP" | "LOW_BALANCE"
  timestamp: string
}

export interface AuthorizeResponse {
  device_id: string
  request_id: string
  status: "GRANTED" | "REJECTED"
  authorization_id: string
  granted_msat: number
  remaining_msat: number
  issued_at: string
  expires_at: string
}

export interface BalanceMessage {
  device_id: string
  available_msat: number
  reserved_msat: number
  total_msat: number
  timestamp: string
}

export interface UsageReport {
  device_id: string
  report_id: string
  strategy: "interval" | "delta" | "total"
  measure: number
  unit: string
  timestamp: string
}

export interface InvoiceRequest {
  device_id: string
  request_id: string
  amount_msat: number
  reason: "USER_TOPUP" | "AUTO_TOPUP" | "OUT_OF_FUNDS"
  timestamp: string
}

export interface InvoiceResponse {
  device_id: string
  request_id: string
  status: "CREATED" | "PAID" | "EXPIRED" | "ERROR"
  invoice_id: string
  bolt11: string
  amount_msat: number
  expires_at: string
}

export interface ControlCommand {
  command: "STOP" | "PAUSE" | "RESUME" | "REBOOT" | "UPDATE_CONFIG" | "PING" | "AUTHORIZATION"
  reason?: string
  id?: string
  authorization_id?: string
}

export interface Appliance {
  id: string
  name: string
  icon: string
  minWatts: number
  maxWatts: number
  isOn: boolean
  currentWatts: number
}

export type DeviceStatus = "OFFLINE" | "STARTING" | "ONLINE" | "PAUSED" | "ERROR"

export type MQTTConnectionStatus = "disconnected" | "connecting" | "connected" | "error"

export interface MQTTEvent {
  id: string
  timestamp: string
  message: string
  type: "info" | "error" | "success"
}

export interface Authorization {
  authorization_id: string
  request_id: string
  granted_msat: number
  remaining_msat: number
  issued_at: string
  expires_at: string
  status: "ACTIVE" | "EXPIRED" | "CONSUMED"
}
