package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/robertodantas/lnpay/internal"
	ledgerpb "github.com/robertodantas/lnpay/proto/gen/interfaces/ledger"
	"google.golang.org/grpc"
)


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
	cfg := LoadConfig()

	// Initialize repository (creates DB connection and tables)
	repo, err := NewLedgerRepository(cfg.DBPath, cfg.BusyTimeoutMS)
	if err != nil {
		log.Fatalf("Failed to initialize ledger repository: %v", err)
	}
	defer repo.Close()

	// Connect to Redis stream
	log.Println("Connecting to Redis...")
	streamClient, err := internal.NewStreamClientFromEnv()
	if err != nil {
		log.Fatalf("Failed to create Redis stream client: %v", err)
	}
	defer streamClient.Close()
	log.Println("Redis stream client connected successfully")

	// Create stream handler
	streamHandler := NewStreamHandler(streamClient, repo)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start consumption consumer in a goroutine
	go func() {
		if err := streamHandler.StartConsumptionConsumer(ctx); err != nil && err != context.Canceled {
			log.Printf("Consumption consumer error: %v", err)
		}
	}()

	// Start expiration checker in a goroutine
	go func() {
		if err := streamHandler.StartExpirationChecker(ctx); err != nil && err != context.Canceled {
			log.Printf("Expiration checker error: %v", err)
		}
	}()

	// Start gRPC server in a goroutine
	go func() {
		lis, err := net.Listen("tcp", cfg.GRPCAddr)
		if err != nil {
			log.Fatalf("failed to listen on %s: %v", cfg.GRPCAddr, err)
		}

		grpcServer := grpc.NewServer()
		eastWestServer := NewEastWestServer(repo, streamHandler)
		ledgerpb.RegisterLedgerServiceServer(grpcServer, eastWestServer)

		log.Printf("gRPC server listening on %s", cfg.GRPCAddr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve gRPC: %v", err)
		}
	}()

	// Initialize and start northbound REST API
	log.Println("Initializing northbound REST API...")
	northbound := NewNorthboundInterface(repo, cfg)

	// Start northbound server in a goroutine
	go func() {
		if err := northbound.Start(cfg.ListenAddr); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start northbound API server: %v", err)
		}
	}()

	log.Printf("Ledger Service HTTP listening on %s (DB=%s)", cfg.ListenAddr, cfg.DBPath)

	// Wait for interrupt signal to gracefully shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down ledger service...")
	cancel() // Cancel context to stop consumers

	// Gracefully shutdown northbound server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := northbound.Stop(shutdownCtx); err != nil {
		log.Printf("Error shutting down northbound server: %v", err)
	}

	log.Println("Ledger service stopped")
}
