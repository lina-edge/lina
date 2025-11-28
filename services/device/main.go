package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/robertodantas/lnpay/internal"
	autosdk "go.opentelemetry.io/auto/sdk"

	// Import the generated proto package
	// Note: The path matches the go_package in the proto file + the gen/ output directory
	devicepb "github.com/robertodantas/lnpay/proto/gen/model/device"
)

var logger = internal.NewLogger("device")

// EmitUsageRecord creates and serializes a DeviceUsageReportedEvent
// This demonstrates how to use the generated proto files to emit events
func EmitUsageRecord(deviceID, reportID, unit string, measure float64, strategy devicepb.UsageReportingStrategy) ([]byte, error) {
	// Create a UsageRecord
	usageRecord := &devicepb.UsageRecord{
		DeviceId: deviceID,
		ReportId: reportID,
		Strategy: strategy,
		Measure:  measure,
		Unit:     unit,
		// ISO-8601 timestamp
		Timestamp: time.Now().Format(time.RFC3339),
	}

	// Create the DeviceUsageReportedEvent
	event := &devicepb.DeviceUsageReportedEvent{
		Usage: usageRecord,
	}

	// Option 1: Serialize to protobuf binary format (recommended for gRPC/streaming)
	protoBytes, err := proto.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event: %w", err)
	}

	// Option 2: Serialize to JSON (useful for HTTP APIs, logging, etc.)
	opts := protojson.MarshalOptions{UseProtoNames: true}
	jsonBytes, err := opts.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event to JSON: %w", err)
	}

	// You can also create a DeviceEvent envelope if needed
	deviceEvent := &devicepb.DeviceEvent{
		Type: devicepb.DeviceEventType_DEVICE_EVENT_TYPE_USAGE_REPORTED,
		Payload: &devicepb.DeviceEvent_UsageReported{
			UsageReported: event,
		},
	}

	// Serialize the envelope
	envelopeBytes, err := proto.Marshal(deviceEvent)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal envelope: %w", err)
	}

	// For demonstration, return JSON (you can return protoBytes or envelopeBytes)
	_ = protoBytes
	_ = envelopeBytes

	return jsonBytes, nil
}

// Example usage function
func ExampleEmitUsageRecord() {
	// Example: Emit a usage record with DELTA strategy
	jsonData, err := EmitUsageRecord(
		"device-123",
		"report-456", // unique report ID for idempotency
		"kWh",        // unit
		2.5,          // measure (amount consumed)
		devicepb.UsageReportingStrategy_USAGE_STRATEGY_DELTA,
	)
	if err != nil {
		logger.Errorf("Error emitting usage record: %v", err)
		return
	}

	// Print the JSON for demonstration
	var prettyJSON map[string]interface{}
	if err := json.Unmarshal(jsonData, &prettyJSON); err == nil {
		prettyBytes, _ := json.MarshalIndent(prettyJSON, "", "  ")
		fmt.Println("Emitted usage record event:")
		fmt.Println(string(prettyBytes))
	}
}

// initTracer initializes OpenTelemetry with OTLP exporter
func initTracer(cfg Config) (func(context.Context) error, error) {
	ctx := context.Background()

	// Create OTLP exporter
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(cfg.OTELExporterOTLPEndpoint),
		otlptracegrpc.WithInsecure(), // Use insecure for local development
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
	}

	// Create resource with service name
	res, err := resource.New(ctx,
		resource.WithAttributes(
			attribute.String("service.name", cfg.OTELServiceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Create tracer provider with auto SDK and OTLP exporter
	autoTp := autosdk.TracerProvider()

	// Create a batch span processor with the OTLP exporter
	bsp := sdktrace.NewBatchSpanProcessor(exporter)

	// Create a new tracer provider that combines auto SDK with OTLP export
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
		// Use the auto SDK's sampler if available
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(1.0))),
	)

	// Set global tracer provider
	otel.SetTracerProvider(tp)

	// Set global propagator for trace context propagation
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// Shutdown function
	shutdown := func(ctx context.Context) error {
		// Shutdown the custom tracer provider
		if err := tp.Shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown tracer provider: %w", err)
		}
		// Note: auto SDK tracer provider shutdown is handled internally
		_ = autoTp
		return nil
	}

	logger.Infof("OpenTelemetry initialized with OTLP exporter at %s", cfg.OTELExporterOTLPEndpoint)
	return shutdown, nil
}

func main() {
	logger.Info("Starting device service")

	cfg := LoadConfig()

	// Initialize OpenTelemetry
	tracerShutdown, err := initTracer(cfg)
	if err != nil {
		logger.Warnf("Failed to initialize OpenTelemetry: %v. Continuing without tracing.", err)
	} else {
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := tracerShutdown(ctx); err != nil {
				logger.Errorf("Error shutting down tracer: %v", err)
			}
		}()
	}

	serviceCtx, serviceCancel := context.WithCancel(context.Background())
	defer serviceCancel()

	// Initialize device repository
	repo, err := NewDeviceRepository(cfg.DBPath)
	if err != nil {
		logger.Fatal("Failed to initialize device repository", err)
	}
	defer repo.Close()
	logger.Info("Device repository initialized")

	// Initialize dynamic security service
	logger.Info("Initializing dynamic security service")
	dynSecService, err := NewDynSecService(cfg)
	if err != nil {
		logger.Fatal("Failed to initialize dynamic security service", err)
	}
	defer dynSecService.Disconnect()

	// Provision device service user with ACLs to subscribe to device topics
	deviceServiceUsername := cfg.MQTTUsername
	if deviceServiceUsername == "" {
		deviceServiceUsername = "device-service"
	}
	deviceServicePassword := cfg.MQTTPassword
	if deviceServicePassword == "" {
		deviceServicePassword = "device-service-password" // Default password if not set
		logger.Warn("MQTT_PASSWORD not set, using default password")
	}

	logger.Infof("Provisioning device service user: %s", deviceServiceUsername)
	if err := dynSecService.ProvisionDeviceService(deviceServiceUsername, deviceServicePassword); err != nil {
		logger.Warnf("Failed to provision device service user: %v", err)
		// Continue even if provisioning fails (user might already be provisioned)
	} else {
		logger.Info("Device service user provisioned successfully")
	}

	// Connect to MQTT broker
	logger.Info("Connecting to MQTT broker")
	mqttClient, err := NewMQTTClient(cfg)
	if err != nil {
		logger.Fatal("Failed to create MQTT client", err)
	}
	defer mqttClient.Disconnect()
	logger.Info("MQTT client connected successfully")

	// Connect to Redis
	logger.Info("Connecting to Redis")
	streamClient, err := NewStreamClient()
	if err != nil {
		logger.Fatal("Failed to create Redis stream client", err)
	}
	defer streamClient.Close()
	logger.Info("Redis stream client connected successfully")

	// Connect to ledger service via gRPC
	logger.Info("Connecting to ledger service")
	ledgerClient, err := NewLedgerClient(cfg)
	if err != nil {
		logger.Fatal("Failed to create ledger gRPC client", err)
	}
	defer ledgerClient.Close()
	logger.Info("Ledger gRPC client connected successfully")

	// Connect to lightning service via gRPC
	logger.Info("Connecting to lightning service")
	lightningClient, err := NewLightningClient(cfg)
	if err != nil {
		logger.Fatal("Failed to create lightning gRPC client", err)
	}
	defer lightningClient.Close()
	logger.Info("Lightning gRPC client connected successfully")

	// Initialize and start northbound REST API
	logger.Info("Initializing northbound REST API")
	northbound := NewNorthboundInterface(repo, dynSecService, mqttClient)

	// Start northbound server in a goroutine
	go func() {
		if err := northbound.Start(cfg.APIAddr); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("Failed to start northbound API server: %v", err)
		}
	}()

	// Initialize and start southbound interface
	logger.Info("Initializing southbound interface")
	invoiceTimeout := time.Duration(cfg.LightningRPCTimeoutSeconds) * time.Second
	southbound := NewSouthboundInterface(mqttClient, streamClient, ledgerClient, lightningClient, repo, invoiceTimeout)
	if err := southbound.Start(); err != nil {
		logger.Fatal("Failed to start southbound interface", err)
	}

	// Start ledger balance subscriber to fan-out balance updates via MQTT
	streamClient.StartLedgerBalanceSubscriber(serviceCtx, mqttClient)

	logger.Info("Device service is running. Press Ctrl+C to stop")
	logger.Infof("Northbound REST API available at http://localhost%s", cfg.APIAddr)

	// Wait for interrupt signal to gracefully shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	logger.Info("Shutting down device service")
	serviceCancel()

	// Gracefully shutdown northbound server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := northbound.Stop(ctx); err != nil {
		logger.Errorf("Error shutting down northbound server: %v", err)
	}

	logger.Info("Device service stopped")
}
