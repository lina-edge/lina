"use client"

import { useSmartMeter } from "@/hooks/use-smart-meter"
import { MeterControl } from "@/components/meter-control"
import { MainMeter } from "@/components/main-meter"
import { ApplianceCard } from "@/components/appliance-card"
import { BalancePanel } from "@/components/balance-panel"
import { QRPayment } from "@/components/qr-payment"
import { ThemeToggle } from "@/components/theme-toggle"

export default function SmartMeterPage() {
  const {
    deviceId,
    deviceStatus,
    appliances,
    balance,
    totalConsumption,
    instantPower,
    invoice,
    logs,
    mqttStatus,
    startMeter,
    stopMeter,
    toggleAppliance,
    requestTopUp,
    simulatePayment,
    clearInvoice,
    backendStatus,
  } = useSmartMeter()

  const isOnline = deviceStatus === "ONLINE"
  const isBackendConnected = backendStatus === "connected"
  const canToggleAppliances = isOnline && balance && balance.available_msat > 0

  return (
    <main className="min-h-screen bg-background p-4 md:p-6 lg:p-8">
      <div className="mx-auto max-w-[1400px] space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold tracking-tight text-foreground">Smart Meter Simulator</h1>
            <p className="text-sm text-muted-foreground">IoT prepaid energy management with Lightning payments</p>
          </div>
          <ThemeToggle />
        </div>

        {/* Main Control */}
        <MeterControl
          deviceId={deviceId}
          deviceStatus={deviceStatus}
          mqttStatus={mqttStatus}
          onStart={startMeter}
          onStop={stopMeter}
          events={logs}
        />

        {isBackendConnected ? (
          /* Main Grid */
          <div className="grid gap-6 lg:grid-cols-3">
            {/* Meter Display */}
            <div className="lg:col-span-1">
              <MainMeter instantPower={instantPower} totalConsumption={totalConsumption} isOnline={isOnline} />
            </div>

            {/* Balance & Payment */}
            <div className="lg:col-span-1">
              <BalancePanel balance={balance} onRequestTopUp={requestTopUp} isOnline={isOnline} />
              {invoice && (
                <div className="mt-4">
                  <QRPayment invoice={invoice} onSimulatePayment={simulatePayment} onClose={clearInvoice} />
                </div>
              )}
            </div>

            {/* Appliances */}
            <div className="lg:col-span-1">
              <h2 className="mb-4 text-sm font-medium text-muted-foreground">Appliances</h2>
              <div className="grid gap-3">
                {appliances.map((appliance) => (
                  <ApplianceCard
                    key={appliance.id}
                    appliance={appliance}
                    onToggle={() => toggleAppliance(appliance.id)}
                    disabled={!canToggleAppliances && !appliance.isOn}
                  />
                ))}
              </div>
            </div>
          </div>
        ) : null}
      </div>
    </main>
  )
}
