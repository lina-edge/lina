"use client"

import { useBackend } from "./use-backend"

export function useSmartMeter() {
  const { state, connectionStatus, sendCommand } = useBackend()

  const startMeter = () => sendCommand("start")
  const stopMeter = () => sendCommand("stop")
  const toggleAppliance = (applianceId: string) => sendCommand("toggle_appliance", { applianceId })
  const requestTopUp = (amountMsat: number) => sendCommand("request_topup", { amountMsat })
  const simulatePayment = () => sendCommand("simulate_payment")
  const clearInvoice = () => sendCommand("clear_invoice")

  return {
    deviceId: state?.deviceId || "",
    deviceStatus: state?.deviceStatus || "OFFLINE",
    appliances: state?.appliances || [],
    balance: state?.balance || null,
    totalConsumption: state?.totalConsumption || 0,
    instantPower: state?.instantPower || 0,
    invoice: state?.invoice || null,
    logs: state?.logs || [],
    mqttStatus: state?.mqttStatus || "disconnected",
    backendStatus: connectionStatus,
    startMeter,
    stopMeter,
    toggleAppliance,
    requestTopUp,
    simulatePayment,
    clearInvoice,
  }
}
