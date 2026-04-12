package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	internalpkg "github.com/robertodantas/lina/internal"
)

var logger = internalpkg.NewLogger("autopay")

func main() {
	ctx := context.Background()
	internalpkg.InitLogLevel()

	// Load configuration
	cfg := LoadConfig()

	logger.InfoWithFields(ctx, "Starting autopay service", map[string]interface{}{
		"receiver_lnd_host": cfg.ReceiverLNDHost,
		"payer_lnd_host":    cfg.PayerLNDHost,
		"network":           cfg.Network,
		"autopay_enabled":   cfg.AutopayEnabled,
	})

	// Create receiver LND client (listens for invoice creation)
	receiverLNDCfg := LNDConfig{
		Host:          cfg.ReceiverLNDHost,
		TLSCertHex:    cfg.ReceiverLNDTLSCertHex,
		TLSServerName: cfg.ReceiverLNDTLSServerName,
		MacaroonHex:   cfg.ReceiverLNDMacaroonHex,
	}
	receiverLND, err := NewLNDClient(ctx, receiverLNDCfg, "receiver")
	if err != nil {
		logger.Fatal(ctx, "Failed to create receiver LND client", err)
	}
	defer receiverLND.Close()

	// Create payer LND client (pays invoices)
	payerLNDCfg := LNDConfig{
		Host:          cfg.PayerLNDHost,
		TLSCertHex:    cfg.PayerLNDTLSCertHex,
		TLSServerName: cfg.PayerLNDTLSServerName,
		MacaroonHex:   cfg.PayerLNDMacaroonHex,
	}
	payerLND, err := NewLNDClient(ctx, payerLNDCfg, "payer")
	if err != nil {
		logger.Fatal(ctx, "Failed to create payer LND client", err)
	}
	defer payerLND.Close()

	// Create invoice stream handler
	handler := NewInvoiceStreamHandler(receiverLND, payerLND, cfg.AutopayEnabled)

	// Start listening for invoice creation events
	if err := handler.Start(ctx); err != nil {
		logger.Fatal(ctx, "Failed to start invoice stream", err)
	}

	logger.Info(ctx, "Autopay service started and listening for invoice creation events")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	logger.Info(ctx, "Shutting down autopay service...")

	logger.Info(ctx, "Autopay service stopped")
}
