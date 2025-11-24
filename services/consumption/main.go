package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/robertodantas/lnpay/internal"
)

func main() {
	cfg := LoadConfig()
	repository, err := NewConsumptionRepository(cfg.DBPath, cfg.BusyTimeoutMS)
	if err != nil {
		log.Fatalf("Failed to create consumption repository: %v", err)
	}
	defer repository.Close()

	// Connect to Redis stream
	log.Println("Connecting to Redis...")
	streamClient, err := internal.NewStreamClientFromEnv()
	if err != nil {
		log.Fatalf("Failed to create Redis stream client: %v", err)
	}
	defer streamClient.Close()
	log.Println("Redis stream client connected successfully")

	// Create stream handler
	streamHandler := NewStreamHandler(streamClient, cfg, repository)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start device event consumer (consumes from event.device stream)
	go func() {
		if err := streamHandler.StartDeviceConsumer(ctx); err != nil && err != context.Canceled {
			log.Printf("Device consumer error: %v", err)
		}
	}()

	// Start outbox publisher (publishes to event.consumption stream)
	go func() {
		if err := streamHandler.StartOutboxPublisher(ctx); err != nil && err != context.Canceled {
			log.Printf("Outbox publisher error: %v", err)
		}
	}()

	// Start outbox cleanup (removes old published records after retention period)
	go func() {
		if err := streamHandler.StartOutboxCleanup(ctx); err != nil && err != context.Canceled {
			log.Printf("Outbox cleanup error: %v", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down consumption service...")
	cancel() // Cancel context to stop consumers
	log.Println("Consumption service stopped")
}
