package main

import (
	"context"
	"fmt"
	"math"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	devicepb "github.com/robertodantas/lnpay/proto/gen/model/device"
)

var (
	consumptionPropagator = otel.GetTextMapPropagator()
)

// EastWestStreamHandler handles processing of Redis stream messages from east-west services
type EastWestStreamHandler struct {
	repository *ConsumptionRepository
	publisher  *EastWestStreamPublisher
}

// NewEastWestStreamHandler creates a new east-west stream handler
func NewEastWestStreamHandler(repository *ConsumptionRepository, publisher *EastWestStreamPublisher) *EastWestStreamHandler {
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
		InfoWithFields(ctx, "Device event received", map[string]interface{}{
			"report_id":           usage.GetReportId(),
			"measure":             usage.GetMeasure(),
			"unit":                usage.GetUnit(),
			"price_per_unit_msat": usage.GetPricePerUnitMsat(),
		})

	tx, err := esh.repository.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Check idempotency: if report_id already exists, skip
	exists, err := esh.repository.CheckReportExists(ctx, tx, reportID)
	if err != nil {
		return err
	}
	if exists {
		// Report already processed, skip (idempotency)
		logger.WithDeviceID(deviceID).
			DebugWithFields(ctx, "Report already processed, skipping (idempotency)", map[string]interface{}{
				"report_id": reportID,
			})
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit: %w", err)
		}
		return nil
	}

	logger.WithDeviceID(deviceID).
		InfoWithFields(ctx, "Processing report", map[string]interface{}{
			"report_id":  reportID,
			"measure":    measure,
			"unit":       usage.GetUnit(),
			"price_msat": pricePerUnitMsat,
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

	// Extract trace context to store in database
	carrier := make(propagation.MapCarrier)
	consumptionPropagator.Inject(ctx, carrier)

	// Create consumption record with rounded-up amount and fractional part for auditability
	err = esh.repository.CreateConsumptionRecord(ctx, tx, reportID, deviceID, debitMsat, fractionalMsat, measure, pricePerUnitMsat, usage.GetUnit(), usage.GetTimestamp(), carrier)
	if err != nil {
		return err
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Publish consumption event
	// Extract parent context from stored trace context
	publishCtx := ctx
	if len(carrier) > 0 {
		publishCarrier := propagation.MapCarrier(carrier)
		publishCtx = consumptionPropagator.Extract(ctx, publishCarrier)
	}

	logger.WithDeviceID(deviceID).
		InfoWithFields(ctx, "Consumption recorded", map[string]interface{}{
			"report_id":     reportID,
			"usage_msat":    usageDebitMsat,
			"debit_msat":    debitMsat,
			"rounded_up_by": debitMsat - int64(usageDebitMsat),
		})

	if err := esh.publisher.PublishConsumptionEvent(publishCtx, reportID, deviceID, debitMsat, usage.GetTimestamp()); err != nil {
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
