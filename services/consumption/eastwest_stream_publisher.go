package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/protobuf/encoding/protojson"

	consumptionpb "github.com/robertodantas/lnpay/proto/gen/model/consumption"
)

// EastWestStreamPublisher handles publishing messages to Redis streams for east-west communication
type EastWestStreamPublisher struct {
	streamInterface *EastWestStreamInterface
	repository      *ConsumptionRepository
	outboxTrigger   chan string
}

// NewEastWestStreamPublisher creates a new east-west stream publisher
func NewEastWestStreamPublisher(streamInterface *EastWestStreamInterface, repository *ConsumptionRepository, outboxTrigger chan string) *EastWestStreamPublisher {
	return &EastWestStreamPublisher{
		streamInterface: streamInterface,
		repository:      repository,
		outboxTrigger:   outboxTrigger,
	}
}

// GetOutboxTrigger returns the outbox trigger channel
func (esp *EastWestStreamPublisher) GetOutboxTrigger() chan string {
	return esp.outboxTrigger
}

// PublishConsumptionEvent publishes a DeviceConsumptionRecorded event to event.consumption stream
func (esp *EastWestStreamPublisher) PublishConsumptionEvent(ctx context.Context, reportID, deviceID string, debitMsat int64, timestamp string) error {
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
	// Clean event type: "CONSUMPTION_EVENT_TYPE_DEVICE_CONSUMPTION_RECORDED" -> "DEVICE_CONSUMPTION_RECORDED"
	eventTypeFull := consumptionEvent.Type.String()
	eventType := eventTypeFull
	if len(eventTypeFull) > len("CONSUMPTION_EVENT_TYPE_") && eventTypeFull[:len("CONSUMPTION_EVENT_TYPE_")] == "CONSUMPTION_EVENT_TYPE_" {
		eventType = eventTypeFull[len("CONSUMPTION_EVENT_TYPE_"):]
	}
	streamID, err := esp.streamInterface.XAddWithSpan(ctx, streamName, &redis.XAddArgs{
		Stream: streamName,
		Values: values,
	}, eventType)

	if err != nil {
		return fmt.Errorf("failed to publish to Redis stream %s: %w", streamName, err)
	}

	logger.WithDeviceID(deviceID).
		WithStream(streamName, "produce").
		InfoWithFields(ctx, "Published DeviceConsumptionRecorded event", map[string]interface{}{
			"report_id":  reportID,
			"debit_msat": debitMsat,
			"stream_id":  streamID,
		})
	return nil
}

// StartOutboxPublisher processes outbox on-demand + periodic safety check
// This runs less frequently as a safety net for failed immediate publishes
func (esp *EastWestStreamPublisher) StartOutboxPublisher(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Minute) // Safety check every 5 minutes
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-esp.outboxTrigger:
			// Triggered by failed publish - process immediately
			esp.publishOutboxEvents(ctx)
		case <-ticker.C:
			// Periodic safety check for any missed events
			esp.publishOutboxEvents(ctx)
		}
	}
}

// publishOutboxEvents publishes unpublished events from outbox to event.consumption stream
func (esp *EastWestStreamPublisher) publishOutboxEvents(ctx context.Context) error {
	// Get unpublished events by joining outbox with consumption_records
	// This avoids duplication - outbox is minimal, records is the source of truth
	events, err := esp.repository.GetUnpublishedOutboxEvents(ctx, 10)
	if err != nil {
		return err
	}

	if len(events) == 0 {
		return nil // No events to publish
	}

	// Publish each event
	for _, e := range events {
		// Extract parent context from stored trace context
		var publishCtx context.Context
		if len(e.TraceContext) > 0 {
			carrier := propagation.MapCarrier(e.TraceContext)
			publishCtx = consumptionPropagator.Extract(ctx, carrier)
		} else {
			publishCtx = ctx
		}

		if err := esp.PublishConsumptionEvent(publishCtx, e.ReportID, e.DeviceID, e.DebitMsat, e.Timestamp); err != nil {
			logger.WithDeviceID(e.DeviceID).
				WithStream("event.consumption", "produce").
				Errorf(ctx, "Failed to publish event for report %s: %v", e.ReportID, err)
			continue
		}

		// Mark as published
		if err := esp.repository.MarkOutboxAsPublished(ctx, e.ReportID); err != nil {
			logger.WithDeviceID(e.DeviceID).
				Errorf(ctx, "Failed to mark report %s as published: %v", e.ReportID, err)
			// Continue anyway, we'll retry on next run
		}
	}

	if len(events) > 0 {
		logger.WithStream("event.consumption", "produce").
			InfoWithFields(ctx, "Published events from outbox", map[string]interface{}{
				"count": len(events),
			})
	}

	return nil
}

// StartOutboxCleanup periodically removes old published records from outbox
// This keeps the outbox table small and only contains recent unpublished events
func (esp *EastWestStreamPublisher) StartOutboxCleanup(ctx context.Context) error {
	logger.Info(ctx, "Starting outbox cleanup")

	ticker := time.NewTicker(1 * time.Hour) // Run cleanup every hour
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info(ctx, "Stopping outbox cleanup")
			return ctx.Err()
		case <-ticker.C:
			if err := esp.cleanupOutbox(ctx); err != nil {
				logger.Error(ctx, "Error cleaning up outbox", err)
			}
		}
	}
}

// cleanupOutbox removes published records older than retention period (default: 1 days)
// This is a common pattern: keep published records for debugging/audit, then clean up
func (esp *EastWestStreamPublisher) cleanupOutbox(ctx context.Context) error {
	// Retention period: 1 day (configurable)
	retentionDays := 1
	rowsAffected, err := esp.repository.CleanupOutbox(ctx, retentionDays)
	if err != nil {
		return err
	}

	if rowsAffected > 0 {
		logger.InfoWithFields(ctx, "Cleaned up old published records from outbox", map[string]interface{}{
			"rows_affected":  rowsAffected,
			"retention_days": retentionDays,
		})
	}

	return nil
}
