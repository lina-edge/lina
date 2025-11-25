"use client"

import { useState, useCallback, useRef, useEffect } from "react"
import type { Appliance, DeviceStatus, DeviceConfig, BalanceMessage, InvoiceResponse, Authorization } from "@/lib/types"
import { useMQTT } from "./use-mqtt"

const DEFAULT_APPLIANCES: Appliance[] = [
  { id: "fridge", name: "Refrigerator", icon: "fridge", minWatts: 100, maxWatts: 150, isOn: false, currentWatts: 0 },
  {
    id: "microwave",
    name: "Microwave",
    icon: "microwave",
    minWatts: 800,
    maxWatts: 1200,
    isOn: false,
    currentWatts: 0,
  },
  { id: "heater", name: "Space Heater", icon: "heater", minWatts: 1000, maxWatts: 1500, isOn: false, currentWatts: 0 },
  { id: "oven", name: "Electric Oven", icon: "oven", minWatts: 2000, maxWatts: 2500, isOn: false, currentWatts: 0 },
  { id: "computer", name: "Computer", icon: "computer", minWatts: 150, maxWatts: 300, isOn: false, currentWatts: 0 },
  { id: "washer", name: "Washing Machine", icon: "washer", minWatts: 300, maxWatts: 500, isOn: false, currentWatts: 0 },
]

const DEFAULT_CONFIG: DeviceConfig = {
  device_id: process.env.NEXT_PUBLIC_DEFAULT_DEVICE_ID || "smart-meter-001",
  unit: "kWh",
  unit_price: "10",
  pricing_unit: "msat",
  reporting_strategy: "interval",
  reporting_interval: 30,
  heartbeat_interval: 10,
  authorize_request_msat: 1000,
  timestamp: new Date().toISOString(),
}

interface SmartMeterState {
  deviceId: string
  deviceStatus: DeviceStatus
  appliances: Appliance[]
  balance: BalanceMessage | null
  config: DeviceConfig
  totalConsumption: number
  instantPower: number
  invoice: InvoiceResponse | null
  authorizations: Authorization[]
  logs: Array<{ id: string; timestamp: string; message: string; type: "info" | "error" | "success" }>
}

export function useSmartMeter() {
  const mqtt = useMQTT()
  const [state, setState] = useState<SmartMeterState>({
    deviceId: process.env.NEXT_PUBLIC_DEFAULT_DEVICE_ID || "smart-meter-001",
    deviceStatus: "OFFLINE",
    appliances: DEFAULT_APPLIANCES,
    balance: null,
    config: DEFAULT_CONFIG,
    totalConsumption: 0,
    instantPower: 0,
    invoice: null,
    authorizations: [],
    logs: [],
  })

  const intervalRefs = useRef<{ heartbeat?: NodeJS.Timeout; usage?: NodeJS.Timeout; power?: NodeJS.Timeout }>({})

  const addLog = useCallback((message: string, type: "info" | "error" | "success" = "info") => {
    setState((prev) => ({
      ...prev,
      logs: [{ id: `${Date.now()}-${Math.random()}`, timestamp: new Date().toISOString(), message, type }, ...prev.logs].slice(0, 50),
    }))
  }, [])

  const generateRequestId = () => Math.random().toString(36).substring(2, 10)

  // Simulate power variance for running appliances
  const updatePowerReadings = useCallback(() => {
    setState((prev) => {
      const updatedAppliances = prev.appliances.map((appliance) => {
        if (!appliance.isOn) return { ...appliance, currentWatts: 0 }
        const range = appliance.maxWatts - appliance.minWatts
        const variance = (Math.random() - 0.5) * range * 0.2
        const baseWatts = (appliance.minWatts + appliance.maxWatts) / 2
        const currentWatts = Math.max(appliance.minWatts, Math.min(appliance.maxWatts, baseWatts + variance))
        return { ...appliance, currentWatts: Math.round(currentWatts) }
      })

      const instantPower = updatedAppliances.reduce((sum, a) => sum + a.currentWatts, 0)

      return { ...prev, appliances: updatedAppliances, instantPower }
    })
  }, [])

  // Start the meter system
  const startMeter = useCallback(async () => {
    setState((prev) => ({ ...prev, deviceStatus: "STARTING" }))
    addLog("Starting meter system...", "info")

    // Connect to MQTT
    // Username should match device ID for proper ACL permissions
    // Password should match what was set during device provisioning
    const mqttUsername = process.env.NEXT_PUBLIC_MQTT_USERNAME || state.deviceId
    const mqttPassword = process.env.NEXT_PUBLIC_MQTT_PASSWORD || `${state.deviceId}_password`
    
    mqtt.connect({
      brokerUrl: process.env.NEXT_PUBLIC_MQTT_BROKER_URL || "wss://mqtt.example.com",
      deviceId: state.deviceId,
      username: mqttUsername,
      password: mqttPassword,
    })

    // Subscribe to authorization responses
    mqtt.subscribeToAuthorizeResponse((response) => {
      if (response.status === "GRANTED") {
        setState((prev) => {
          // Create authorization record - this is reserved balance
          const authorization: Authorization = {
            authorization_id: response.authorization_id,
            request_id: response.request_id,
            granted_msat: response.granted_msat,
            remaining_msat: response.remaining_msat,
            issued_at: response.issued_at,
            expires_at: response.expires_at,
            status: "ACTIVE",
          }

          // Don't calculate balance here - wait for backend to publish balance update
          return {
            ...prev,
            deviceStatus: "ONLINE",
            authorizations: [...prev.authorizations, authorization],
          }
        })
        addLog(`Authorization granted: ${response.granted_msat} msat (reserved)`, "success")
      } else if (response.status === "REJECTED") {
        addLog(`Authorization rejected: ${response.request_id}`, "error")
      }
    })

    // Subscribe to balance updates
    mqtt.subscribeToBalance((balance) => {
      setState((prev) => ({ ...prev, balance }))
      addLog(`Balance updated: ${balance.available_msat} msat available`, "info")
    })

    // Subscribe to invoice responses
    mqtt.subscribeToInvoiceResponse((response) => {
      if (response.status === "CREATED") {
        setState((prev) => ({ ...prev, invoice: response }))
        addLog("Invoice created - scan QR to pay", "success")
      } else if (response.status === "PAID") {
        setState((prev) => ({ ...prev, invoice: null }))
        addLog(`Payment received: ${response.amount_msat} msat`, "success")
      }
    })

    // Simulate startup sequence
    setTimeout(() => {
      // Publish heartbeat
      mqtt.publishHeartbeat({
        device_id: state.deviceId,
        status: "ONLINE",
        timestamp: new Date().toISOString(),
      })
      addLog("Heartbeat sent: ONLINE", "success")

      // Request authorization
      const requestId = generateRequestId()
      mqtt.publishAuthorizeRequest({
        device_id: state.deviceId,
        request_id: requestId,
        request_msat: state.config.authorize_request_msat,
        reason: "STARTUP",
        timestamp: new Date().toISOString(),
      })
      addLog(`Authorization requested: ${requestId}`, "info")

      // Fallback: If no authorization response after 5 seconds, set to ONLINE anyway
      setTimeout(() => {
        setState((prev) => {
          if (prev.deviceStatus === "STARTING") {
            addLog("No authorization response received, proceeding without balance", "error")
            return { ...prev, deviceStatus: "ONLINE" }
          }
          return prev
        })
      }, 5000)
    }, 1000)

    // Start power update interval
    intervalRefs.current.power = setInterval(updatePowerReadings, 1000)

    // Start heartbeat interval
    intervalRefs.current.heartbeat = setInterval(() => {
      mqtt.publishHeartbeat({
        device_id: state.deviceId,
        status: "ONLINE",
        timestamp: new Date().toISOString(),
      })
    }, state.config.heartbeat_interval * 1000)

    // Start usage reporting interval
    intervalRefs.current.usage = setInterval(() => {
      setState((prev) => {
        if (prev.deviceStatus !== "ONLINE" || prev.instantPower === 0) return prev

        // Calculate kWh consumed in this interval
        const kWhConsumed = (prev.instantPower / 1000) * (prev.config.reporting_interval / 3600)
        const costMsat = kWhConsumed * Number.parseFloat(prev.config.unit_price)

        // Publish usage report
        mqtt.publishUsageReport({
          device_id: prev.deviceId,
          report_id: generateRequestId(),
          strategy: prev.config.reporting_strategy,
          measure: kWhConsumed,
          unit: prev.config.unit,
          timestamp: new Date().toISOString(),
        })

        // Update balance
        const newBalance = prev.balance
          ? {
              ...prev.balance,
              available_msat: Math.max(0, prev.balance.available_msat - costMsat),
              total_msat: Math.max(0, prev.balance.total_msat - costMsat),
            }
          : null

        // Check if out of funds
        if (newBalance && newBalance.available_msat <= 0) {
          addLog("OUT OF FUNDS - All appliances stopped", "error")
          return {
            ...prev,
            balance: newBalance,
            totalConsumption: prev.totalConsumption + kWhConsumed,
            appliances: prev.appliances.map((a) => ({ ...a, isOn: false, currentWatts: 0 })),
            instantPower: 0,
          }
        }

        return {
          ...prev,
          balance: newBalance,
          totalConsumption: prev.totalConsumption + kWhConsumed,
        }
      })
    }, state.config.reporting_interval * 1000)
  }, [mqtt, state.deviceId, state.config, addLog, updatePowerReadings])

  // Stop the meter system
  const stopMeter = useCallback(() => {
    Object.values(intervalRefs.current).forEach((interval) => {
      if (interval) clearInterval(interval)
    })
    intervalRefs.current = {}

    mqtt.publishHeartbeat({
      device_id: state.deviceId,
      status: "OFFLINE",
      timestamp: new Date().toISOString(),
    })

    mqtt.disconnect()

    setState((prev) => ({
      ...prev,
      deviceStatus: "OFFLINE",
      appliances: prev.appliances.map((a) => ({ ...a, isOn: false, currentWatts: 0 })),
      instantPower: 0,
    }))
    addLog("Meter system stopped", "info")
  }, [mqtt, state.deviceId, addLog])

  // Toggle appliance
  const toggleAppliance = useCallback(
    (applianceId: string) => {
      setState((prev) => {
        // Can't turn on if out of funds
        const appliance = prev.appliances.find((a) => a.id === applianceId)
        if (appliance && !appliance.isOn && prev.balance && prev.balance.available_msat <= 0) {
          addLog(`Cannot turn on ${appliance.name} - out of funds`, "error")
          return prev
        }

        // Can't turn on if meter is offline
        if (prev.deviceStatus !== "ONLINE") {
          addLog("Cannot toggle appliance - meter is offline", "error")
          return prev
        }

        const updatedAppliances = prev.appliances.map((a) => {
          if (a.id === applianceId) {
            const newState = !a.isOn
            addLog(`${a.name} turned ${newState ? "ON" : "OFF"}`, "info")
            return { ...a, isOn: newState }
          }
          return a
        })

        return { ...prev, appliances: updatedAppliances }
      })
    },
    [addLog],
  )

  // Request invoice for top-up
  const requestTopUp = useCallback(
    (amountMsat: number) => {
      if (state.deviceStatus !== "ONLINE") {
        addLog("Cannot request top-up - meter is offline", "error")
        return
      }

      const requestId = generateRequestId()
      mqtt.publishInvoiceRequest({
        device_id: state.deviceId,
        request_id: requestId,
        amount_msat: amountMsat,
        reason: "USER_TOPUP",
        timestamp: new Date().toISOString(),
      })
      addLog(`Invoice requested: ${amountMsat} msat`, "info")

      // Simulate invoice response
      setTimeout(() => {
        const invoice: InvoiceResponse = {
          device_id: state.deviceId,
          request_id: requestId,
          status: "CREATED",
          invoice_id: `inv-${generateRequestId()}`,
          bolt11: `lnbc${amountMsat / 1000}u1pjq${generateRequestId()}${generateRequestId()}${generateRequestId()}`,
          amount_msat: amountMsat,
          expires_at: new Date(Date.now() + 10 * 60 * 1000).toISOString(),
        }
        setState((prev) => ({ ...prev, invoice }))
        addLog("Invoice created - scan QR to pay", "success")
      }, 500)
    },
    [mqtt, state.deviceId, state.deviceStatus, addLog],
  )

  // Simulate payment received
  const simulatePayment = useCallback(() => {
    if (!state.invoice) return

    setState((prev) => ({
      ...prev,
      balance: prev.balance
        ? {
            ...prev.balance,
            available_msat: prev.balance.available_msat + (prev.invoice?.amount_msat || 0),
            total_msat: prev.balance.total_msat + (prev.invoice?.amount_msat || 0),
          }
        : {
            device_id: prev.deviceId,
            available_msat: prev.invoice?.amount_msat || 0,
            reserved_msat: 0,
            total_msat: prev.invoice?.amount_msat || 0,
            timestamp: new Date().toISOString(),
          },
      invoice: null,
    }))
    addLog("Payment received!", "success")
  }, [state.invoice, addLog])

  const clearInvoice = useCallback(() => {
    setState((prev) => ({ ...prev, invoice: null }))
  }, [])

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      Object.values(intervalRefs.current).forEach((interval) => {
        if (interval) clearInterval(interval)
      })
    }
  }, [])

  return {
    ...state,
    mqttStatus: mqtt.connectionStatus,
    startMeter,
    stopMeter,
    toggleAppliance,
    requestTopUp,
    simulatePayment,
    clearInvoice,
  }
}
