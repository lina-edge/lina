package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/robertodantas/lnpay/internal"
	ledgerpb "github.com/robertodantas/lnpay/proto/gen/interfaces/ledger"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

var logger = internal.NewLogger("ledger")

/*
   =========================================
   Models & helpers
   =========================================
*/

type CreditRequest struct {
	DeviceID       string `json:"device_id" binding:"required"`
	AmountMsat     int64  `json:"amount_msat" binding:"required"` // must be > 0
	Reason         string `json:"reason"`
	CorrelationID  string `json:"correlation_id,omitempty"`
	IdempotencyKey string `json:"idempotency_key" binding:"required"`
}

type DebitRequest struct {
	DeviceID       string `json:"device_id" binding:"required"`
	AmountMsat     int64  `json:"amount_msat" binding:"required"` // must be > 0
	Reason         string `json:"reason"`
	AllowNegative  bool   `json:"allow_negative,omitempty"`
	CorrelationID  string `json:"correlation_id,omitempty"`
	IdempotencyKey string `json:"idempotency_key" binding:"required"`
}

type EntryResponse struct {
	EntryID       string `json:"entry_id"`
	DeviceID      string `json:"device_id"`
	EntryType     string `json:"entry_type"`
	AmountMsat    int64  `json:"amount_msat"`
	BalanceAfter  int64  `json:"balance_after"`
	Reason        string `json:"reason,omitempty"`
	CreatedAt     int64  `json:"created_at"`
	CorrelationID string `json:"correlation_id,omitempty"`
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func hashReq(kind string, body any) string {
	// cheap & deterministic fingerprint (no crypto requirement here)
	b, _ := json.Marshal(body)
	return fmt.Sprintf("%s:%d:%x", kind, len(b), djb2(b))
}

func djb2(b []byte) uint64 {
	var h uint64 = 5381
	for _, c := range b {
		h = ((h << 5) + h) + uint64(c)
	}
	return h
}

/*
   =========================================
   main
   =========================================
*/

func main() {
	ctx := context.Background()

	logger.Info(ctx, "Starting ledger service")

	cfg := LoadConfig()

	// Initialize OpenTelemetry
	tracerShutdown, err := internal.InitTracer(internal.TracerConfig{
		ServiceName:          cfg.OTELServiceName,
		ExporterOTLPEndpoint: cfg.OTELExporterOTLPEndpoint,
	})
	if err != nil {
		logger.Warnf(ctx, "Failed to initialize OpenTelemetry: %v. Continuing without tracing.", err)
	} else {
		logger.Infof(ctx, "OpenTelemetry initialized with OTLP exporter at %s", cfg.OTELExporterOTLPEndpoint)
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := tracerShutdown(shutdownCtx); err != nil {
				logger.Errorf(shutdownCtx, "Error shutting down tracer: %v", err)
			}
		}()
	}

	// Initialize repository (creates DB connection and tables)
	repo, err := NewLedgerRepository(cfg.DBPath, cfg.BusyTimeoutMS)
	if err != nil {
		logger.Fatal(ctx, "Failed to initialize ledger repository", err)
	}
	defer repo.Close()

	// Connect to Redis stream
	logger.Info(ctx, "Connecting to Redis")
	streamClient, err := internal.NewStreamClientFromEnv(ctx)
	if err != nil {
		logger.Fatal(ctx, "Failed to create Redis stream client", err)
	}
	defer streamClient.Close()
	logger.Info(ctx, "Redis stream client connected successfully")

	// Create publisher
	publisher := NewEastWestStreamPublisher(streamClient)

	// Create handler
	handler := NewEastWestStreamHandler(repo, publisher)

	// Create stream interface
	streamInterface, err := NewEastWestStreamInterface(ctx, handler)
	if err != nil {
		logger.Fatal(ctx, "Failed to create stream interface", err)
	}
	defer streamInterface.Close()

	// Create context for graceful shutdown
	serviceCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start consumption consumer in a goroutine
	go func() {
		if err := streamInterface.StartConsumptionConsumer(serviceCtx); err != nil && err != context.Canceled {
			logger.WithStream("event.consumption", "consume").
				Error(serviceCtx, "Consumption consumer error", err)
		}
	}()

	// Start lightning consumer in a goroutine
	go func() {
		if err := streamInterface.StartLightningConsumer(serviceCtx); err != nil && err != context.Canceled {
			logger.WithStream("event.lightning", "consume").
				Error(serviceCtx, "Lightning consumer error", err)
		}
	}()

	// Start expiration checker in a goroutine
	go func() {
		if err := streamInterface.StartExpirationChecker(serviceCtx, repo, publisher); err != nil && err != context.Canceled {
			logger.Error(serviceCtx, "Expiration checker error", err)
		}
	}()

	// Start gRPC server in a goroutine
	go func() {
		lis, err := net.Listen("tcp", cfg.GRPCAddr)
		if err != nil {
			logger.Fatalf(ctx, "Failed to listen on %s via eastwest gRPC: %v", cfg.GRPCAddr, err)
		}

		grpcServer := grpc.NewServer(
			grpc.StatsHandler(otelgrpc.NewServerHandler()),
		)
		eastWestServer := NewEastWestServer(repo, publisher)
		ledgerpb.RegisterLedgerServiceServer(grpcServer, eastWestServer)

		logger.Infof(ctx, "gRPC server listening on %s via eastwest gRPC", cfg.GRPCAddr)
		if err := grpcServer.Serve(lis); err != nil {
			logger.Fatalf(ctx, "Failed to serve gRPC via eastwest: %v", err)
		}
	}()

	// Initialize and start northbound REST API
	logger.Info(ctx, "Initializing northbound REST API")
	northbound := NewNorthboundInterface(repo, cfg, publisher)

	// Start northbound server in a goroutine
	go func() {
		if err := northbound.Start(ctx, cfg.ListenAddr); err != nil && err != http.ErrServerClosed {
			logger.Fatalf(ctx, "Failed to start northbound API server: %v", err)
		}
	}()

	logger.InfoWithFields(ctx, "Ledger Service HTTP listening via northbound REST", map[string]interface{}{
		"listen_addr": cfg.ListenAddr,
		"db_path":     cfg.DBPath,
	})

	// Wait for interrupt signal to gracefully shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	logger.Info(ctx, "Shutting down ledger service")
	cancel() // Cancel context to stop consumers

	// Gracefully shutdown northbound server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := northbound.Stop(shutdownCtx); err != nil {
		logger.Errorf(shutdownCtx, "Error shutting down northbound server: %v", err)
	}

	logger.Info(ctx, "Ledger service stopped")
}
