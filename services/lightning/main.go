package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/robertodantas/lnpay/internal"
	lightningpb "github.com/robertodantas/lnpay/proto/gen/interfaces/lightning"
	"google.golang.org/grpc"
)

var logger = internal.NewLogger("lightning")

func main() {
	// Load configuration
	cfg := LoadConfig()

	// Initialize OpenTelemetry
	tracerShutdown, err := internal.InitTracer(internal.TracerConfig{
		ServiceName:          cfg.OTELServiceName,
		ExporterOTLPEndpoint: cfg.OTELExporterOTLPEndpoint,
	})
	if err != nil {
		logger.Warnf("Failed to initialize OpenTelemetry: %v. Continuing without tracing.", err)
	} else {
		logger.Infof("OpenTelemetry initialized with OTLP exporter at %s", cfg.OTELExporterOTLPEndpoint)
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := tracerShutdown(ctx); err != nil {
				logger.Errorf("Error shutting down tracer: %v", err)
			}
		}()
	}

	// Connect to Redis stream
	logger.Info("Connecting to Redis")
	streamClient, err := internal.NewStreamClientFromEnv()
	if err != nil {
		logger.Fatal("Failed to create Redis stream client", err)
	}
	defer streamClient.Close()
	logger.Info("Redis stream client connected successfully")

	// Log configuration (masked for security)
	logger.InfoWithFields("Configuration loaded", map[string]interface{}{
		"lnd_host": cfg.LNDHost,
		"network":  cfg.Network,
		"tls_cert": cfg.LNDTLSCertHex[:min(20, len(cfg.LNDTLSCertHex))] + "...",
		"macaroon": cfg.LNDMacaroonHex[:min(20, len(cfg.LNDMacaroonHex))] + "...",
	})

	// Create LND client
	lndClient, err := NewLNDClient(*cfg)
	if err != nil {
		logger.Fatal("Failed to create LND client", err)
	}
	defer lndClient.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Get and display node info
	info, err := lndClient.GetInfo(ctx)
	if err != nil {
		logger.Fatal("Failed to get node info", err)
	}

	logger.InfoWithFields("Node info via cloud LND node", map[string]interface{}{
		"alias":           info.Alias,
		"identity":        info.IdentityPubkey,
		"version":         info.Version,
		"block_height":    info.BlockHeight,
		"synced":          info.SyncedToChain,
		"active_channels": info.NumActiveChannels,
	})

	// Create LND event stream
	lndEventStream := NewLNDEventStream(lndClient)
	if err := lndEventStream.Start(ctx); err != nil {
		logger.Fatal("Failed to start LND event stream", err)
	}

	// Create stream publisher (publishes to Redis)
	streamPublisher := NewStreamPublisher(streamClient, lndEventStream)
	if err := streamPublisher.Start(ctx); err != nil {
		logger.Fatal("Failed to start stream publisher", err)
	}

	// Create northbound REST interface
	logger.Info("Initializing northbound REST API")
	northbound := NewNorthboundInterface(lndClient, cfg)
	go func() {
		logger.Infof("Lightning northbound REST listening on %s", cfg.ListenAddr)
		if err := northbound.Start(cfg.ListenAddr); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("Failed to start northbound server: %v", err)
		}
	}()

	// Start gRPC server in a goroutine
	go func() {
		lis, err := net.Listen("tcp", cfg.GRPCAddr)
		if err != nil {
			logger.Fatalf("Failed to listen on %s via eastwest gRPC: %v", cfg.GRPCAddr, err)
		}

		grpcServer := grpc.NewServer()
		eastWestServer := NewEastWestServer(lndClient, streamPublisher)
		lightningpb.RegisterLightningServiceServer(grpcServer, eastWestServer)

		logger.Infof("gRPC server listening on %s via eastwest gRPC", cfg.GRPCAddr)
		if err := grpcServer.Serve(lis); err != nil {
			logger.Fatalf("Failed to serve gRPC via eastwest: %v", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	logger.Info("Shutting down lightning service")
	cancel() // Cancel context to stop consumers

	// Gracefully shutdown northbound server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := northbound.Stop(shutdownCtx); err != nil {
		logger.Errorf("Error shutting down northbound server: %v", err)
	}

	logger.Info("Lightning service stopped")
}
