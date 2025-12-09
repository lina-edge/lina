"use client"

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import type { InvoiceResponse } from "@/lib/types"
import { QrCode, Copy, Check, X, Zap } from "lucide-react"
import { useClipboard } from "@/hooks/use-clipboard"
import QRCode from "react-qr-code"

interface QRPaymentProps {
  invoice: InvoiceResponse | null
  onSimulatePayment: () => void
  onClose: () => void
}

export function QRPayment({ invoice, onSimulatePayment, onClose }: QRPaymentProps) {
  const { copied, copyToClipboard } = useClipboard()

  if (!invoice) return null

  const handleCopy = () => {
    copyToClipboard(invoice.bolt11)
  }

  const amountSats = Math.floor(invoice.amount_msat / 1000)

  return (
    <Card className="border-primary/50 bg-card">
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between">
          <CardTitle className="flex items-center gap-2 text-sm font-medium text-muted-foreground">
            <QrCode className="h-4 w-4" />
            Lightning Invoice
          </CardTitle>
          <Button variant="ghost" size="icon" className="h-6 w-6" onClick={onClose}>
            <X className="h-4 w-4" />
          </Button>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex flex-col items-center">
          <div className="rounded-lg bg-white p-3">
            <QRCode
              value={invoice.bolt11}
              size={160}
              bgColor="#ffffff"
              fgColor="#000000"
            />
          </div>
          <div className="mt-3 flex items-baseline gap-1">
            <Zap className="h-4 w-4 text-primary" />
            <span className="font-mono text-xl font-bold text-foreground">{amountSats.toLocaleString()}</span>
            <span className="text-sm text-muted-foreground">sats</span>
          </div>
        </div>

        <div className="space-y-2">
          <Button
            variant="outline"
            size="sm"
            className="w-full font-mono text-xs bg-transparent"
            onClick={handleCopy}
          >
            {copied ? (
              <>
                <Check className="mr-2 h-3 w-3" />
                Copied!
              </>
            ) : (
              <>
                <Copy className="mr-2 h-3 w-3" />
                Copy Invoice
              </>
            )}
          </Button>
        </div>

        <p className="text-center text-xs text-muted-foreground">
          Invoice expires: {new Date(invoice.expires_at).toLocaleTimeString()}
        </p>
      </CardContent>
    </Card>
  )
}
