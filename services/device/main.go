package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	// Import the generated proto package
	// Note: The path matches the go_package in the proto file + the gen/ output directory
	devicepb "github.com/robertodantas/lnpay/proto/gen/model/device"
)

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
		log.Printf("Error emitting usage record: %v", err)
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
	log.Println("Starting device service...")

	cfg := LoadConfig()

	serviceCtx, serviceCancel := context.WithCancel(context.Background())
	defer serviceCancel()

	// Initialize device repository
	repo, err := NewDeviceRepository(cfg.DBPath)
	if err != nil {
		log.Fatalf("Failed to initialize device repository: %v", err)
	}
	defer repo.Close()
	log.Println("Device repository initialized")

	// Initialize dynamic security service
	log.Println("Initializing dynamic security service...")
	dynSecService, err := NewDynSecService(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize dynamic security service: %v", err)
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
		log.Printf("Warning: MQTT_PASSWORD not set, using default password")
	}

	log.Printf("Provisioning device service user: %s", deviceServiceUsername)
	if err := dynSecService.ProvisionDeviceService(deviceServiceUsername, deviceServicePassword); err != nil {
		log.Printf("Failed to provision device service user: %v", err)
		// Continue even if provisioning fails (user might already be provisioned)
	} else {
		log.Println("Device service user provisioned successfully")
	}

	// Connect to MQTT broker
	log.Println("Connecting to MQTT broker...")
	mqttClient, err := NewMQTTClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create MQTT client: %v", err)
	}
	defer mqttClient.Disconnect()
	log.Println("MQTT client connected successfully")

	// Connect to Redis
	log.Println("Connecting to Redis...")
	streamClient, err := NewStreamClient()
	if err != nil {
		log.Fatalf("Failed to create Redis stream client: %v", err)
	}
	defer streamClient.Close()
	log.Println("Redis stream client connected successfully")

	// Connect to ledger service via gRPC
	log.Println("Connecting to ledger service...")
	ledgerClient, err := NewLedgerClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create ledger gRPC client: %v", err)
	}
	defer ledgerClient.Close()
	log.Println("Ledger gRPC client connected successfully")

	// Initialize and start northbound REST API
	log.Println("Initializing northbound REST API...")
	northbound := NewNorthboundInterface(repo, dynSecService, mqttClient)

	// Start northbound server in a goroutine
	go func() {
		if err := northbound.Start(cfg.APIAddr); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start northbound API server: %v", err)
		}
	}()

	// Initialize and start southbound interface
	log.Println("Initializing southbound interface...")
	southbound := NewSouthboundInterface(mqttClient, streamClient, ledgerClient, repo)
	if err := southbound.Start(); err != nil {
		log.Fatalf("Failed to start southbound interface: %v", err)
	}

	// Start ledger balance subscriber to fan-out balance updates via MQTT
	streamClient.StartLedgerBalanceSubscriber(serviceCtx, mqttClient)

	log.Println("Device service is running. Press Ctrl+C to stop...")
	log.Printf("Northbound REST API available at http://localhost%s", cfg.APIAddr)

	// Wait for interrupt signal to gracefully shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down device service...")
	serviceCancel()

	// Gracefully shutdown northbound server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := northbound.Stop(ctx); err != nil {
		log.Printf("Error shutting down northbound server: %v", err)
	}

	log.Println("Device service stopped")
}
