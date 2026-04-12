package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/robertodantas/lina/internal"
	lightningpb "github.com/robertodantas/lina/proto/gen/interfaces/lightning"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var logger = internal.NewLogger("lightning")

func main() {
	ctx := context.Background()
	internal.InitLogLevel()

	// Load configuration
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

	// Connect to Redis stream
	logger.Info(ctx, "Connecting to Redis")
	streamClient, err := internal.NewStreamClientFromEnv(ctx)
	if err != nil {
		logger.Fatal(ctx, "Failed to create Redis stream client", err)
	}
	defer streamClient.Close()
	logger.Info(ctx, "Redis stream client connected successfully")

	// Log configuration (masked for security)
	logger.InfoWithFields(ctx, "Configuration loaded", map[string]interface{}{
		"lnd_host": cfg.LNDHost,
		"network":  cfg.Network,
		"tls_cert": cfg.LNDTLSCertHex[:min(20, len(cfg.LNDTLSCertHex))] + "...",
		"macaroon": cfg.LNDMacaroonHex[:min(20, len(cfg.LNDMacaroonHex))] + "...",
	})

	// Create LND client
	lndClient, err := NewLNDClient(ctx, *cfg)
	if err != nil {
		logger.Fatal(ctx, "Failed to create LND client", err)
	}
	defer lndClient.Close()

	serviceCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Get and display node info
	info, err := lndClient.GetInfo(serviceCtx)
	if err != nil {
		logger.Fatal(serviceCtx, "Failed to get node info", err)
	}

	logger.InfoWithFields(serviceCtx, "Node info via cloud LND node", map[string]interface{}{
		"alias":           info.Alias,
		"identity":        info.IdentityPubkey,
		"version":         info.Version,
		"block_height":    info.BlockHeight,
		"synced":          info.SyncedToChain,
		"active_channels": info.NumActiveChannels,
	})

	// Create stream publisher (publishes to Redis)
	streamPublisher := NewEastWestStreamPublisher(streamClient, cfg.LightningEphemeralRetention)

	// Create LND stream handler
	lndStreamHandler := NewLNDStreamHandler(streamPublisher)

	// Create LND event stream interface
	lndStreamInterface := NewLNDStreamInterface(lndClient, lndStreamHandler)
	if err := lndStreamInterface.Start(serviceCtx); err != nil {
		logger.Fatal(serviceCtx, "Failed to start LND event stream", err)
	}

	// Create northbound REST interface
	logger.Info(ctx, "Initializing northbound REST API")
	northbound := NewNorthboundInterface(lndClient, cfg)
	go func() {
		logger.Infof(ctx, "Lightning northbound REST listening on %s", cfg.ListenAddr)
		if err := northbound.Start(ctx, cfg.ListenAddr); err != nil && err != http.ErrServerClosed {
			logger.Fatalf(ctx, "Failed to start northbound server: %v", err)
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
			grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
				MinTime:             30 * time.Second,
				PermitWithoutStream: true,
			}),
		)
		eastWestServer := NewEastWestGRPCServer(lndClient, streamPublisher)
		lightningpb.RegisterLightningServiceServer(grpcServer, eastWestServer)

		logger.Infof(ctx, "gRPC server listening on %s via eastwest gRPC", cfg.GRPCAddr)
		if err := grpcServer.Serve(lis); err != nil {
			logger.Fatalf(ctx, "Failed to serve gRPC via eastwest: %v", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	logger.Info(ctx, "Shutting down lightning service")
	cancel() // Cancel context to stop consumers

	// Gracefully shutdown northbound server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := northbound.Stop(shutdownCtx); err != nil {
		logger.Errorf(shutdownCtx, "Error shutting down northbound server: %v", err)
	}

	logger.Info(ctx, "Lightning service stopped")
}
