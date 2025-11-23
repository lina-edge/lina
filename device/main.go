package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	_ "modernc.org/sqlite"

	// Import the generated proto package
	// Note: The path matches the go_package in the proto file + the gen/ output directory
	devicepb "github.com/robertodantas/lnpay/proto/gen/gen/iot/payperuse/edge/model/device"
)

// Device represents a registered IoT device
type Device struct {
	ID              string  `json:"id"`
	PublicKey       string  `json:"public_key"`
	Unit            string  `json:"unit"`           // e.g., "sheet", "m3"
	PricePerUnit    float64 `json:"price_per_unit"` // cost in sats per unit
	SecretKey       string  `json:"secret_key"`
	AggregationMode string  `json:"aggregation_mode"` // e.g., "per-unit", "time-window", "value-threshold"
}

// RegistryService manages the registered devices
type RegistryService struct {
	db *sql.DB
}

// NewRegistryService creates and initializes the SQLite database
func NewRegistryService(dbPath string) *RegistryService {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatalf("failed to connect to SQLite: %v", err)
	}

	// Create the devices table with aggregation_mode support
	createTable := `
	CREATE TABLE IF NOT EXISTS devices (
		id TEXT PRIMARY KEY,
		public_key TEXT,
		unit TEXT,
		price_per_unit REAL,
		secret_key TEXT,
		aggregation_mode TEXT DEFAULT 'per-unit'
	);`

	if _, err := db.Exec(createTable); err != nil {
		log.Fatalf("failed to create table: %v", err)
	}

	return &RegistryService{db: db}
}

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
	jsonBytes, err := protojson.Marshal(event)
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

	_ = NewRegistryService("devices.db")
	log.Println("Registry service initialized")

	// Initialize dynamic security service
	log.Println("Initializing dynamic security service...")
	dynSecService, err := NewDynSecService()
	if err != nil {
		log.Fatalf("Failed to initialize dynamic security service: %v", err)
	}
	defer dynSecService.Disconnect()

	// Example: Provision device123
	log.Println("Provisioning device123...")
	if err := dynSecService.ProvisionDevice("device123"); err != nil {
		log.Printf("Failed to provision device123: %v", err)
		// Continue even if provisioning fails (device might already be provisioned)
	} else {
		log.Println("Device123 provisioned successfully")
	}

	// Connect to MQTT broker
	log.Println("Connecting to MQTT broker...")
	mqttClient, err := NewMQTTClient()
	if err != nil {
		log.Fatalf("Failed to create MQTT client: %v", err)
	}
	defer mqttClient.Disconnect()
	log.Println("MQTT client connected successfully")

	// Example: Emit a usage record event
	jsonData, err := EmitUsageRecord(
		"device-123",
		"report-456",
		"kWh",
		2.5,
		devicepb.UsageReportingStrategy_USAGE_STRATEGY_DELTA,
	)
	if err != nil {
		log.Printf("Error emitting usage record: %v", err)
		return
	}

	// Publish usage record to MQTT
	deviceID := "device-123"
	topic := fmt.Sprintf("devices/%s/usage", deviceID)
	if err := mqttClient.Publish(topic, 1, false, jsonData); err != nil {
		log.Printf("Error publishing to MQTT: %v", err)
		return
	}

	log.Println("Usage record published successfully")
}
