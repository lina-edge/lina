"use client"

import { Card, CardContent } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Power, Wifi, WifiOff, RefreshCw } from "lucide-react"
import type { DeviceStatus, MQTTConnectionStatus, MQTTEvent } from "@/lib/types"
import { cn } from "@/lib/utils"
import { useMemo } from "react"

interface MeterControlProps {
  deviceId: string
  deviceStatus: DeviceStatus
  mqttStatus: MQTTConnectionStatus
  onStart: () => void
  onStop: () => void
  events?: MQTTEvent[] // added events prop
}

const statusColors: Record<DeviceStatus, string> = {
  OFFLINE: "bg-muted text-muted-foreground",
  STARTING: "bg-primary/20 text-primary",
  ONLINE: "bg-accent/20 text-accent",
  PAUSED: "bg-primary/20 text-primary",
  ERROR: "bg-destructive/20 text-destructive",
}

export function MeterControl({ deviceId, deviceStatus, mqttStatus, onStart, onStop, events = [] }: MeterControlProps) {
  const isOnline = deviceStatus === "ONLINE"
  const isStarting = deviceStatus === "STARTING"

  const recentEvents = useMemo(() => {
    return events.slice(0, 10).reverse()
  }, [events])

  return (
    <Card className="border-border bg-card">
      <CardContent className="p-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <Button
              size="lg" 
              variant={isOnline ? "destructive" : "default"}
              className={cn(
                "h-14 w-14 rounded-full p-0",
                !isOnline && "bg-accent hover:bg-accent/90 text-accent-foreground",
              )}
              onClick={isOnline ? onStop : onStart}
              disabled={isStarting}
            >
              {isStarting ? <RefreshCw className="h-6 w-6 animate-spin" /> : <Power className="h-6 w-6" />}
            </Button>

            <div>
              <h2 className="text-lg font-semibold text-foreground">Smart Meter Control</h2>
              <div className="mt-1 flex items-center gap-2">
                <Badge className={cn("font-mono text-xs", statusColors[deviceStatus])}>{deviceStatus}</Badge>
                <div className="flex items-center gap-1 text-xs text-muted-foreground">
                  {mqttStatus === "connected" ? (
                    <Wifi className="h-3 w-3 text-accent" />
                  ) : (
                    <WifiOff className="h-3 w-3" />
                  )}
                  MQTT: {mqttStatus}
                </div>
              </div>
            </div>
          </div>

          <div className="text-right">
            <p className="text-xs text-muted-foreground">Device ID</p>
            <p className="font-mono text-sm text-foreground">{deviceId || "unknown"}</p>
          </div>
        </div>

        {recentEvents.length > 0 && (
          <div className="mt-3 border-t border-border pt-3">
            <div className="space-y-1">
              {recentEvents.map((event) => (
                <div key={event.id} className="flex items-center gap-2 text-xs">
                  <span className="text-muted-foreground font-mono shrink-0">
                    {new Date(event.timestamp).toLocaleTimeString()}
                  </span>
                  <span className="text-foreground truncate">{event.message}</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  )
}
