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

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/robertodantas/lnpay/internal"

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
		logger.Errorf(nil, "Error emitting usage record: %v", err)
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

func main() {
	ctx := context.Background()

	logger.Info(ctx, "Starting device service")

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

	serviceCtx, serviceCancel := context.WithCancel(ctx)
	defer serviceCancel()

	// Initialize device repository
	repo, err := NewDeviceRepository(ctx, cfg.DBPath, cfg.BusyTimeoutMS)
	if err != nil {
		logger.Fatal(ctx, "Failed to initialize device repository", err)
	}
	defer repo.Close()
	logger.Info(ctx, "Device repository initialized")

	// Initialize dynamic security service
	logger.Info(ctx, "Initializing dynamic security service")
	dynSecService, err := NewDynSecService(ctx, cfg)
	if err != nil {
		logger.Fatal(ctx, "Failed to initialize dynamic security service", err)
	}
	defer dynSecService.Disconnect(ctx)

	// Provision device service user with ACLs to subscribe to device topics
	deviceServiceUsername := cfg.MQTTUsername
	if deviceServiceUsername == "" {
		deviceServiceUsername = "device-service"
	}
	deviceServicePassword := cfg.MQTTPassword
	if deviceServicePassword == "" {
		deviceServicePassword = "device-service-password" // Default password if not set
		logger.Warn(ctx, "MQTT_PASSWORD not set, using default password")
	}

	logger.Infof(ctx, "Provisioning device service user: %s", deviceServiceUsername)
	if err := dynSecService.ProvisionDeviceService(ctx, deviceServiceUsername, deviceServicePassword); err != nil {
		logger.Warnf(ctx, "Failed to provision device service user: %v", err)
		// Continue even if provisioning fails (user might already be provisioned)
	} else {
		logger.Info(ctx, "Device service user provisioned successfully")
	}

	// Connect to MQTT broker
	logger.Info(ctx, "Connecting to MQTT broker")
	mqttClient, err := NewMQTTClient(ctx, cfg)
	if err != nil {
		logger.Fatal(ctx, "Failed to create MQTT client", err)
	}
	defer mqttClient.Disconnect()
	logger.Info(ctx, "MQTT client connected successfully")

	// Connect to Redis
	logger.Info(ctx, "Connecting to Redis")
	streamClient, err := NewStreamClient(ctx)
	if err != nil {
		logger.Fatal(ctx, "Failed to create Redis stream client", err)
	}
	defer streamClient.Close()
	logger.Info(ctx, "Redis stream client connected successfully")

	// Connect to ledger service via gRPC
	logger.Info(ctx, "Connecting to ledger service")
	ledgerClient, err := NewLedgerClient(ctx, cfg)
	if err != nil {
		logger.Fatal(ctx, "Failed to create ledger gRPC client", err)
	}
	defer ledgerClient.Close()
	logger.Info(ctx, "Ledger gRPC client connected successfully")

	// Connect to lightning service via gRPC
	logger.Info(ctx, "Connecting to lightning service")
	lightningClient, err := NewLightningClient(ctx, cfg)
	if err != nil {
		logger.Fatal(ctx, "Failed to create lightning gRPC client", err)
	}
	defer lightningClient.Close()
	logger.Info(ctx, "Lightning gRPC client connected successfully")

	// Initialize and start northbound REST API
	logger.Info(ctx, "Initializing northbound REST API")
	northbound := NewNorthboundInterface(repo, dynSecService, mqttClient)

	// Start northbound server in a goroutine
	go func() {
		if err := northbound.Start(ctx, cfg.APIAddr); err != nil && err != http.ErrServerClosed {
			logger.Fatalf(ctx, "Failed to start northbound API server: %v", err)
		}
	}()

	// Initialize and start southbound interface
	logger.Info(ctx, "Initializing southbound interface")
	invoiceTimeout := time.Duration(cfg.LightningRPCTimeoutSeconds) * time.Second
	southbound := NewSouthboundInterface(mqttClient, streamClient, ledgerClient, lightningClient, repo, invoiceTimeout)
	if err := southbound.Start(ctx); err != nil {
		logger.Fatal(ctx, "Failed to start southbound interface", err)
	}

	// Start ledger balance subscriber to fan-out balance updates via MQTT
	streamClient.StartLedgerBalanceSubscriber(serviceCtx, mqttClient)

	// Start lightning invoice event subscriber to fan-out invoice updates via MQTT
	streamClient.StartLightningInvoiceSubscriber(serviceCtx, mqttClient)

	logger.Info(ctx, "Device service is running. Press Ctrl+C to stop")
	logger.Infof(ctx, "Northbound REST API available at http://localhost%s", cfg.APIAddr)

	// Wait for interrupt signal to gracefully shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	logger.Info(ctx, "Shutting down device service")
	serviceCancel()

	// Gracefully shutdown northbound server
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := northbound.Stop(shutdownCtx); err != nil {
		logger.Errorf(shutdownCtx, "Error shutting down northbound server: %v", err)
	}

	logger.Info(ctx, "Device service stopped")
}
