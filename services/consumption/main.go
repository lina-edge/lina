package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/robertodantas/lina/internal"
)

var logger = internal.NewLogger("consumption")

func main() {
	ctx := context.Background()
	internal.InitLogLevel()

	logger.Info(ctx, "Starting consumption service")

	cfg := LoadConfig()

	// Initialize metrics (must be done before OpenTelemetry tracer to avoid conflicts)
	if err := initMetrics(); err != nil {
		logger.Warnf(ctx, "Failed to initialize metrics: %v. Continuing without metrics.", err)
	} else {
		logger.Info(ctx, "Metrics initialized successfully")
	}

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
			shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := tracerShutdown(shutdownCtx); err != nil {
				logger.Errorf(shutdownCtx, "Error shutting down tracer: %v", err)
			}
		}()
	}

	repository, repoImpl, repoPath, err := OpenConsumptionRepository(cfg)
	if err != nil {
		logger.Fatal(ctx, "Failed to create consumption repository", err)
	}
	logger.Infof(ctx, "Consumption repository implementation=%s resolved_path=%s", repoImpl, repoPath)
	defer repository.Close()

	// Connect to Redis stream
	logger.Info(ctx, "Connecting to Redis")
	streamInterface, err := NewEastWestStreamInterface(ctx, cfg, repository)
	if err != nil {
		logger.Fatal(ctx, "Failed to create Redis stream client", err)
	}
	defer streamInterface.Close()
	logger.Info(ctx, "Redis stream client connected successfully")

	// Create outbox trigger channel
	outboxTrigger := make(chan string, 100)

	// Create publisher
	publisher := NewEastWestStreamPublisher(streamInterface, repository, outboxTrigger)

	// Create handler
	handler := NewEastWestStreamHandler(repository, publisher)

	// Create northbound interface
	// Initialize and start northbound REST API
	logger.Info(ctx, "Initializing northbound REST API")
	northbound := NewNorthboundInterface(repository, cfg)

	// Start northbound server in a goroutine
	go func() {
		if err := northbound.Start(ctx, cfg.ListenAddr); err != nil && err != http.ErrServerClosed {
			logger.Fatalf(ctx, "Failed to start northbound API server: %v", err)
		}
	}()

	// Create context for graceful shutdown
	serviceCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start device event consumer (consumes from event.device stream)
	go func() {
		if err := streamInterface.StartDeviceConsumer(serviceCtx, handler); err != nil && err != context.Canceled {
			logger.WithStream("event.device", "consume").
				Error(serviceCtx, "Device consumer error", err)
		}
	}()

	// Start outbox publisher (publishes to event.consumption stream)
	go func() {
		if err := publisher.StartOutboxPublisher(serviceCtx); err != nil && err != context.Canceled {
			logger.WithStream("event.consumption", "produce").
				Error(serviceCtx, "Outbox publisher error", err)
		}
	}()

	// Start outbox cleanup (removes old published records after retention period)
	go func() {
		if err := publisher.StartOutboxCleanup(serviceCtx); err != nil && err != context.Canceled {
			logger.Error(serviceCtx, "Outbox cleanup error", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	metricsAddr := internal.GetEnv("METRICS_ADDR", ":9464")
	go func() {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", GetMetricsHandler())
		metricsServer := &http.Server{
			Addr:    metricsAddr,
			Handler: metricsMux,
		}
		logger.Infof(ctx, "Metrics server listening on %s/metrics", metricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Errorf(ctx, "Failed to start metrics server: %v", err)
		}
	}()

	<-sigChan

	logger.Info(ctx, "Shutting down consumption service")
	cancel() // Cancel context to stop consumers
	logger.Info(ctx, "Consumption service stopped")

	// Graceful shutdown
	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := northbound.Stop(shutdownCtx); err != nil {
		logger.Errorf(shutdownCtx, "Error stopping northbound API: %v", err)
	}
}
