package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	devicepb "github.com/robertodantas/lina/proto/gen/model/device"
)

var (
	consumptionPropagator = otel.GetTextMapPropagator()
)

const reportLockStripes = 256

// EastWestStreamHandler handles processing of Redis stream messages from east-west services
type EastWestStreamHandler struct {
	repository ConsumptionRepository
	publisher  *EastWestStreamPublisher
	// reportLocks serialize CreateConsumptionRecord per stripe so the same report_id cannot pass Get+Batch twice
	// concurrently; different report_ids (usually) use different stripes and run in parallel.
	reportLocks [reportLockStripes]sync.Mutex
}

func reportStripeIndex(reportID string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(reportID))
	return h.Sum32() % reportLockStripes
}

// NewEastWestStreamHandler creates a new east-west stream handler
func NewEastWestStreamHandler(repository ConsumptionRepository, publisher *EastWestStreamPublisher) *EastWestStreamHandler {
	return &EastWestStreamHandler{
		repository: repository,
		publisher:  publisher,
	}
}

// HandleUsageReported processes a DeviceUsageReported event from event.device stream
func (esh *EastWestStreamHandler) HandleUsageReported(ctx context.Context, usage *devicepb.UsageRecord) error {
	reportID := usage.GetReportId()
	if reportID == "" {
		return fmt.Errorf("missing report_id")
	}

	deviceID := usage.GetDeviceId()
	measure := usage.GetMeasure()
	pricePerUnitMsat := usage.GetPricePerUnitMsat()

	logger.WithDeviceID(deviceID).
		DebugWithFields(ctx, "Device event received", map[string]interface{}{
			"report_id":           usage.GetReportId(),
			"measure":             usage.GetMeasure(),
			"unit":                usage.GetUnit(),
			"price_per_unit_msat": usage.GetPricePerUnitMsat(),
		})

	// Calculate exact debit amount from this usage report
	usageDebitMsat := float64(pricePerUnitMsat) * measure

	// Calculate fractional part (for auditability)
	integerPart := int64(usageDebitMsat)
	fractionalMsat := usageDebitMsat - float64(integerPart)

	// Round up to next integer - fractional amounts are treated as 1 msat
	debitMsat := int64(math.Ceil(usageDebitMsat))
	if debitMsat < 1 {
		debitMsat = 1 // Minimum 1 msat
	}

	carrier := make(propagation.MapCarrier)
	consumptionPropagator.Inject(ctx, carrier)

	si := reportStripeIndex(reportID)
	esh.reportLocks[si].Lock()
	defer esh.reportLocks[si].Unlock()
	inserted, err := esh.repository.CreateConsumptionRecord(ctx, reportID, deviceID, debitMsat, fractionalMsat, measure, pricePerUnitMsat, usage.GetUnit(), usage.GetTimestamp(), carrier)
	if err != nil {
		return err
	}
	if !inserted {
		logger.WithDeviceID(deviceID).
			DebugWithFields(ctx, "Report already processed, skipping (idempotency)", map[string]interface{}{
				"report_id": reportID,
			})
		return nil
	}

	logger.WithDeviceID(deviceID).
		DebugWithFields(ctx, "Processing report", map[string]interface{}{
			"report_id":  reportID,
			"measure":    measure,
			"unit":       usage.GetUnit(),
			"price_msat": pricePerUnitMsat,
		})

	// Publish consumption event
	// Use explicit timestamp semantics:
	// - timestamp: original device/MQTT timestamp from UsageRecord
	// - report_timestamp: when the DeviceUsageReportedEvent was emitted (if available)
	// - record_timestamp: now, when the usage is priced/recorded
	// Extract parent context from stored trace context
	publishCtx := ctx
	if len(carrier) > 0 {
		publishCarrier := propagation.MapCarrier(carrier)
		publishCtx = consumptionPropagator.Extract(ctx, publishCarrier)
	}

	logger.WithDeviceID(deviceID).
		DebugWithFields(ctx, "Consumption recorded", map[string]interface{}{
			"report_id":     reportID,
			"usage_msat":    usageDebitMsat,
			"debit_msat":    debitMsat,
			"rounded_up_by": debitMsat - int64(usageDebitMsat),
		})

	// Use the original device/MQTT timestamp from the usage record for accurate end-to-end latency measurement
	// This measures from when the device originally reported usage to when it's debited in the ledger
	timestamp := usage.GetTimestamp()
	if timestamp == "" {
		// Fallback to current time if timestamp is missing (shouldn't happen in normal operation)
		timestamp = time.Now().UTC().Format(time.RFC3339Nano)
		logger.WithDeviceID(deviceID).
			Warn(ctx, "Missing timestamp in usage record, using current time as fallback")
	}

	if err := esh.publisher.PublishConsumptionEvent(publishCtx, reportID, deviceID, debitMsat, timestamp); err != nil {
		logger.WithDeviceID(deviceID).
			Warnf(ctx, "Failed to publish immediately, triggering outbox retry: %v", err)
		// Non-blocking send to trigger outbox processing
		outboxTrigger := esh.publisher.GetOutboxTrigger()
		select {
		case outboxTrigger <- reportID:
		default:
		}
	} else {
		// Successfully published, mark as published in outbox
		if err := esh.repository.MarkOutboxAsPublished(ctx, reportID); err != nil {
			logger.WithDeviceID(deviceID).
				Warnf(ctx, "Failed to mark as published: %v", err)
		}
	}

	return nil
}
