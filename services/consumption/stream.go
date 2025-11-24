package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/robertodantas/lnpay/internal"
	consumptionpb "github.com/robertodantas/lnpay/proto/gen/model/consumption"
	devicepb "github.com/robertodantas/lnpay/proto/gen/model/device"
)

// StreamHandler handles Redis stream operations for the consumption service
type StreamHandler struct {
	streamClient *internal.StreamClient
	cfg          Config
	repository   *ConsumptionRepository
	consumerName string
	groupName    string
}

// NewStreamHandler creates a new stream handler
func NewStreamHandler(streamClient *internal.StreamClient, cfg Config, repository *ConsumptionRepository) *StreamHandler {
	return &StreamHandler{
		streamClient: streamClient,
		cfg:          cfg,
		repository:  repository,
		consumerName: "consumption-service",
		groupName:    "consumption-consumers",
	}
}

// StartDeviceConsumer starts consuming from the event.device stream
func (sh *StreamHandler) StartDeviceConsumer(ctx context.Context) error {
	streamName := "event.device"
	client := sh.streamClient.Client()
	streamCtx := sh.streamClient.Context()

	// Create consumer group if it doesn't exist
	err := client.XGroupCreateMkStream(streamCtx, streamName, sh.groupName, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		log.Printf("Warning: failed to create consumer group: %v", err)
		// Continue anyway, group might already exist
	}

	log.Printf("Starting device event consumer for stream: %s", streamName)

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping device event consumer...")
			return ctx.Err()
		default:
			// Read from stream with blocking read (wait up to 5 seconds)
			streams, err := client.XReadGroup(streamCtx, &redis.XReadGroupArgs{
				Group:    sh.groupName,
				Consumer: sh.consumerName,
				Streams:  []string{streamName, ">"},
				Count:    10, // Read up to 10 messages at a time
				Block:    5 * time.Second,
			}).Result()

			if err != nil {
				if err == redis.Nil {
					// No messages, continue
					continue
				}
				log.Printf("Error reading from stream %s: %v", streamName, err)
				time.Sleep(1 * time.Second)
				continue
			}

			// Process messages
			for _, stream := range streams {
				for _, msg := range stream.Messages {
					if err := sh.handleDeviceEvent(streamCtx, msg); err != nil {
						log.Printf("Error handling device event %s: %v", msg.ID, err)
						// Continue processing other messages
					} else {
						// Acknowledge the message
						client.XAck(streamCtx, streamName, sh.groupName, msg.ID)
					}
				}
			}
		}
	}
}

// handleDeviceEvent processes a DeviceUsageReported event from event.device stream
func (sh *StreamHandler) handleDeviceEvent(ctx context.Context, msg redis.XMessage) error {
	// Extract event JSON from message
	eventJSON, ok := msg.Values["event"].(string)
	if !ok {
		return fmt.Errorf("invalid event format: missing 'event' field")
	}

	// Unmarshal DeviceEvent
	var deviceEvent devicepb.DeviceEvent
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	if err := opts.Unmarshal([]byte(eventJSON), &deviceEvent); err != nil {
		return fmt.Errorf("failed to unmarshal device event: %w", err)
	}

	// Check event type
	if deviceEvent.GetType() != devicepb.DeviceEventType_DEVICE_EVENT_TYPE_USAGE_REPORTED {
		log.Printf("Skipping event type: %v", deviceEvent.GetType())
		return nil
	}

	usageReported := deviceEvent.GetUsageReported()
	if usageReported == nil || usageReported.GetUsage() == nil {
		return fmt.Errorf("missing usage_reported payload")
	}

	usage := usageReported.GetUsage()
	log.Printf("[DEVICE EVENT] Device: %s, ReportID: %s, Measure: %.2f %s, PricePerUnit: %d msat",
		usage.GetDeviceId(), usage.GetReportId(), usage.GetMeasure(), usage.GetUnit(), usage.GetPricePerUnitMsat())

	// Process the usage: calculate debit and store in outbox
	return sh.processUsageReport(ctx, usage)
}

// processUsageReport calculates debit amount and stores in database with outbox pattern
func (sh *StreamHandler) processUsageReport(ctx context.Context, usage *devicepb.UsageRecord) error {
	reportID := usage.GetReportId()
	if reportID == "" {
		return fmt.Errorf("missing report_id")
	}

	deviceID := usage.GetDeviceId()
	measure := usage.GetMeasure()
	pricePerUnitMsat := usage.GetPricePerUnitMsat()

	// Calculate debit amount: price_per_unit * measure
	// Convert measure (float64) to int64 msat
	debitMsat := int64(float64(pricePerUnitMsat) * measure)

	if debitMsat <= 0 {
		log.Printf("Warning: calculated debit amount is 0 or negative for report %s, skipping", reportID)
		return nil
	}

	tx, err := sh.repository.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Check idempotency: if report_id already exists, skip
	exists, err := sh.repository.CheckReportExists(ctx, tx, reportID)
	if err != nil {
		return err
	}
	if exists {
		// Report already processed, skip (idempotency)
		log.Printf("Report %s already processed, skipping (idempotency)", reportID)
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit: %w", err)
		}
		return nil
	}

	// Insert into consumption_records and outbox
	err = sh.repository.CreateConsumptionRecord(ctx, tx, reportID, deviceID, debitMsat, measure, pricePerUnitMsat, usage.GetUnit(), usage.GetTimestamp())
	if err != nil {
		return err
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("[CONSUMPTION RECORDED] Report: %s, Device: %s, Debit: %d msat",
		reportID, deviceID, debitMsat)

	return nil
}

// StartOutboxPublisher starts publishing events from outbox to event.consumption stream
func (sh *StreamHandler) StartOutboxPublisher(ctx context.Context) error {
	log.Println("Starting outbox publisher...")

	ticker := time.NewTicker(1 * time.Second) // Check every second
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping outbox publisher...")
			return ctx.Err()
		case <-ticker.C:
			if err := sh.publishOutboxEvents(ctx); err != nil {
				log.Printf("Error publishing outbox events: %v", err)
			}
		}
	}
}

// publishOutboxEvents publishes unpublished events from outbox to event.consumption stream
func (sh *StreamHandler) publishOutboxEvents(ctx context.Context) error {
	// Get unpublished events by joining outbox with consumption_records
	// This avoids duplication - outbox is minimal, records is the source of truth
	events, err := sh.repository.GetUnpublishedOutboxEvents(ctx, 10)
	if err != nil {
		return err
	}

	if len(events) == 0 {
		return nil // No events to publish
	}

	// Publish each event
	for _, e := range events {
		if err := sh.publishConsumptionEvent(ctx, e.ReportID, e.DeviceID, e.DebitMsat, e.Timestamp); err != nil {
			log.Printf("Failed to publish event for report %s: %v", e.ReportID, err)
			continue
		}

		// Mark as published
		if err := sh.repository.MarkOutboxAsPublished(ctx, e.ReportID); err != nil {
			log.Printf("Failed to mark report %s as published: %v", e.ReportID, err)
			// Continue anyway, we'll retry on next run
		}
	}

	if len(events) > 0 {
		log.Printf("Published %d events from outbox", len(events))
	}

	return nil
}

// publishConsumptionEvent publishes a DeviceConsumptionRecorded event to event.consumption stream
func (sh *StreamHandler) publishConsumptionEvent(ctx context.Context, reportID, deviceID string, debitMsat int64, timestamp string) error {
	// Create DeviceConsumptionRecordedEvent
	event := &consumptionpb.DeviceConsumptionRecordedEvent{
		DeviceId:  deviceID,
		DebitMsat: debitMsat,
		Timestamp: timestamp,
	}

	// Wrap in ConsumptionEvent envelope
	consumptionEvent := &consumptionpb.ConsumptionEvent{
		Type: consumptionpb.ConsumptionEventType_CONSUMPTION_EVENT_TYPE_DEVICE_CONSUMPTION_RECORDED,
		Payload: &consumptionpb.ConsumptionEvent_DeviceConsumptionRecorded{
			DeviceConsumptionRecorded: event,
		},
	}

	// Serialize to JSON
	opts := protojson.MarshalOptions{UseProtoNames: true}
	jsonBytes, err := opts.Marshal(consumptionEvent)
	if err != nil {
		return fmt.Errorf("failed to marshal consumption event to JSON: %w", err)
	}

	// Publish to Redis stream "event.consumption"
	streamName := "event.consumption"
	values := map[string]interface{}{
		"event":     string(jsonBytes),
		"timestamp": time.Now().UnixMilli(),
	}

	// Use XADD to add entry to stream
	result := sh.streamClient.Client().XAdd(ctx, &redis.XAddArgs{
		Stream: streamName,
		Values: values,
	})

	if result.Err() != nil {
		return fmt.Errorf("failed to publish to Redis stream %s: %w", streamName, result.Err())
	}

	log.Printf("Published DeviceConsumptionRecorded event (report: %s, device: %s, debit: %d msat) to stream %s (ID: %s)",
		reportID, deviceID, debitMsat, streamName, result.Val())
	return nil
}

// StartOutboxCleanup periodically removes old published records from outbox
// This keeps the outbox table small and only contains recent unpublished events
func (sh *StreamHandler) StartOutboxCleanup(ctx context.Context) error {
	log.Println("Starting outbox cleanup...")

	ticker := time.NewTicker(1 * time.Hour) // Run cleanup every hour
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping outbox cleanup...")
			return ctx.Err()
		case <-ticker.C:
			if err := sh.cleanupOutbox(ctx); err != nil {
				log.Printf("Error cleaning up outbox: %v", err)
			}
		}
	}
}

// cleanupOutbox removes published records older than retention period (default: 7 days)
// This is a common pattern: keep published records for debugging/audit, then clean up
func (sh *StreamHandler) cleanupOutbox(ctx context.Context) error {
	// Retention period: 7 days (configurable)
	retentionDays := 7
	rowsAffected, err := sh.repository.CleanupOutbox(ctx, retentionDays)
	if err != nil {
		return err
	}

	if rowsAffected > 0 {
		log.Printf("Cleaned up %d old published records from outbox (older than %d days)", rowsAffected, retentionDays)
	}

	return nil
}
