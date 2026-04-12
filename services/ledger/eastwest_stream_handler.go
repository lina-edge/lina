package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/robertodantas/lina/internal"
	consumptionpb "github.com/robertodantas/lina/proto/gen/model/consumption"
	lightningmodel "github.com/robertodantas/lina/proto/gen/model/lightning"
)

const (
	authorizationExpiredReason    = "AUTHORIZATION_EXPIRED"
	lightningInvoiceSettledReason = "LIGHTNING_INVOICE_SETTLED"
)

// EastWestStreamHandler handles processing of Redis stream messages from east-west services
type EastWestStreamHandler struct {
	repo      LedgerRepository
	publisher *EastWestStreamPublisher
}

// NewEastWestStreamHandler creates a new east-west stream handler
func NewEastWestStreamHandler(repo LedgerRepository, publisher *EastWestStreamPublisher) *EastWestStreamHandler {
	return &EastWestStreamHandler{
		repo:      repo,
		publisher: publisher,
	}
}

// HandleInvoiceSettled processes an invoice settled event
func (esh *EastWestStreamHandler) HandleInvoiceSettled(ctx context.Context, settled *lightningmodel.InvoiceSettledEvent) error {
	if settled == nil {
		return errors.New("invoice settled payload is nil")
	}

	invoiceID := settled.GetInvoiceId()
	deviceID := settled.GetDeviceId()
	amountMsat := settled.GetAmountReceivedMsat()

	if invoiceID == "" {
		return errors.New("missing invoice_id in lightning event")
	}
	if deviceID == "" {
		return errors.New("missing device_id in lightning event")
	}
	if amountMsat <= 0 {
		return fmt.Errorf("invalid amount for invoice %s: %d", invoiceID, amountMsat)
	}

	creditReq := CreditRequest{
		DeviceID:       deviceID,
		AmountMsat:     amountMsat,
		Reason:         lightningInvoiceSettledReason,
		CorrelationID:  invoiceID,
		IdempotencyKey: invoiceID,
	}

	// Fast path for duplicate events
	if kind, _, ok, err := esh.repo.GetCachedIdem(ctx, creditReq.IdempotencyKey); err != nil {
		return fmt.Errorf("failed to check idempotency for invoice %s: %w", invoiceID, err)
	} else if ok {
		if kind == "credit" {
			logger.WithDeviceID(deviceID).
				WithStream(internal.StreamLightning, "consume").
				DebugWithFields(ctx, "Invoice already credited, skipping", map[string]interface{}{
					"invoice_id": invoiceID,
				})
			return nil
		}
		return fmt.Errorf("idempotency key %s already used for kind %s", invoiceID, kind)
	}

	tx, err := esh.repo.BeginTx(ctx, &LedgerTxOptions{})
	if err != nil {
		return fmt.Errorf("failed to begin tx for invoice %s: %w", invoiceID, err)
	}
	defer func() { _ = tx.Rollback() }()

	entry, err := esh.repo.ApplyCredit(ctx, tx, creditReq)
	if err != nil {
		return fmt.Errorf("failed to apply credit for invoice %s: %w", invoiceID, err)
	}

	if err := esh.repo.SaveIdem(ctx, tx, creditReq.IdempotencyKey, "credit", hashReq("credit", creditReq), entry); err != nil {
		return fmt.Errorf("failed to store idempotency for invoice %s: %w", invoiceID, err)
	}

	commitStart := time.Now()
	err = tx.Commit()
	RecordTxCommitLatency(ctx, "stream.invoice_settled", time.Since(commitStart).Seconds(), err == nil)
	if err != nil {
		return fmt.Errorf("failed to commit credit for invoice %s: %w", invoiceID, err)
	}

	logger.WithDeviceID(deviceID).
		WithStream(internal.StreamLightning, "consume").
		DebugWithFields(ctx, "Credited device from invoice", map[string]interface{}{
			"invoice_id":    invoiceID,
			"amount_msat":   entry.AmountMsat,
			"balance_after": entry.BalanceAfter,
		})

	// Record metrics
	RecordEntry(ctx, "credit", "invoice")

	timestamp := time.Unix(entry.CreatedAt, 0).UTC().Format(time.RFC3339)
	if err := esh.publisher.PublishDeviceCredited(ctx, entry.DeviceID, entry.AmountMsat, entry.BalanceAfter, timestamp); err != nil {
		logger.WithDeviceID(deviceID).
			WithStream(internal.StreamLedger, "produce").
			Errorf(ctx, "Failed to publish DeviceCreditedEvent for invoice %s: %v", invoiceID, err)
	}

	return nil
}

// HandleDeviceConsumptionRecorded processes a device consumption recorded event
func (esh *EastWestStreamHandler) HandleDeviceConsumptionRecorded(ctx context.Context, recorded *consumptionpb.DeviceConsumptionRecordedEvent) error {
	if recorded == nil {
		return fmt.Errorf("missing device_consumption_recorded payload")
	}

	// Begin transaction for processing
	tx, err := esh.repo.BeginTx(ctx, &LedgerTxOptions{})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	logger.WithStream(internal.StreamConsumption, "consume").
		WithDeviceID(recorded.GetDeviceId()).
		DebugWithFields(ctx, "Consumption received", map[string]interface{}{
			"debit_msat": recorded.GetDebitMsat(),
		})

	// Process the consumption: debit from authorization (uses the same transaction)
	// Returns results needed for publishing events after commit
	result, err := esh.processConsumptionWithTx(ctx, tx, recorded)
	if err != nil {
		return err
	}

	// Commit transaction (processing)
	commitStart := time.Now()
	err = tx.Commit()
	RecordTxCommitLatency(ctx, "stream.consumption_recorded", time.Since(commitStart).Seconds(), err == nil)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Publish events after successful commit
	timestamp := time.Now().Format(time.RFC3339)

	// Publish AuthorizationDebited event
	if err := esh.publisher.PublishAuthorizationDebited(ctx, result.authorizationID, result.deviceID, result.actualDebit, result.newRemaining, timestamp); err != nil {
		logger.WithDeviceID(result.deviceID).
			WithStream(internal.StreamLedger, "produce").
			Error(ctx, "Failed to publish AuthorizationDebited event", err)
	}

	// Record metrics
	RecordAuthorizationDebited(ctx)

	if result.newStatus == "completed" {
		// Publish AuthorizationCompleted event
		if err := esh.publisher.PublishAuthorizationCompleted(ctx, result.authorizationID, result.deviceID, timestamp); err != nil {
			logger.WithDeviceID(result.deviceID).
				WithStream(internal.StreamLedger, "produce").
				Error(ctx, "Failed to publish AuthorizationCompleted event", err)
		}
	}

	// Publish DeviceDebited event for overflow if any
	if result.overflowEntry != nil {
		overflowTimestamp := time.Unix(result.overflowEntry.CreatedAt, 0).UTC().Format(time.RFC3339)
		if err := esh.publisher.PublishDeviceDebited(ctx, result.deviceID, result.authorizationID, result.overflowEntry.AmountMsat, result.overflowEntry.BalanceAfter, overflowTimestamp); err != nil {
			logger.WithDeviceID(result.deviceID).
				WithStream(internal.StreamLedger, "produce").
				Error(ctx, "Failed to publish DeviceDebited event for overflow", err)
		}
	}

	// Record latency only for successfully processed consumptions (no retries/failures).
	// The timestamp represents the original device/MQTT timestamp from when the usage was reported.
	// This measures end-to-end latency: device usage reported → ledger debit completed.
	if ts := recorded.GetTimestamp(); ts != "" {
		// Parse timestamp (support both RFC3339 and RFC3339Nano for safety)
		parseTimestamp := func(ts string) (time.Time, error) {
			if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
				return t, nil
			}
			return time.Parse(time.RFC3339, ts)
		}

		t, err := parseTimestamp(ts)
		if err != nil {
			logger.WithDeviceID(recorded.GetDeviceId()).
				WithStream(internal.StreamConsumption, "latency").
				Warnf(ctx, "Failed to parse timestamp for latency metric: %v", err)
			return nil
		}

		latencySeconds := time.Since(t).Seconds()
		RecordDebitLatency(ctx, latencySeconds)
	}

	return nil
}

// ExpectedFailureError indicates an expected failure that should be ACKed
// (e.g., no active authorization - we've already published a failed event)
type ExpectedFailureError struct {
	Err error
}

func (e *ExpectedFailureError) Error() string {
	return e.Err.Error()
}

func (e *ExpectedFailureError) Unwrap() error {
	return e.Err
}

// processConsumptionResult holds the results of processing a consumption for event publishing
type processConsumptionResult struct {
	authorizationID string
	deviceID        string
	actualDebit     int64
	newRemaining    int64
	newStatus       string
	overflowEntry   *EntryResponse
}

// processConsumptionWithTx debits from an authorization using the provided transaction
// Returns processConsumptionResult with information needed for event publishing
func (esh *EastWestStreamHandler) processConsumptionWithTx(ctx context.Context, tx LedgerTx, recorded *consumptionpb.DeviceConsumptionRecordedEvent) (*processConsumptionResult, error) {
	deviceID := recorded.GetDeviceId()
	if deviceID == "" {
		return nil, fmt.Errorf("missing device_id in consumption event")
	}

	// Find active authorization for the device
	// Order by created_at DESC to get the most recent active authorization
	now := time.Now().Format(time.RFC3339)
	authorizationID, remainingMsat, _, _, _, _, err := esh.repo.GetActiveAuthorization(ctx, tx, deviceID, now)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			// No active authorization found - publish failed event
			// This is an expected failure scenario (device may not have authorization yet)
			// We've handled it appropriately by publishing the failed event, so we should ACK the message
			logger.WithDeviceID(deviceID).
				Debug(ctx, "No active authorization found")
			timestamp := time.Now().Format(time.RFC3339)
			if err := esh.publisher.PublishAuthorizationDebitFailed(ctx, "", deviceID, recorded.GetDebitMsat(), 0, "NO_ACTIVE_AUTHORIZATION", timestamp); err != nil {
				logger.WithDeviceID(deviceID).
					WithStream(internal.StreamLedger, "produce").
					Error(ctx, "Failed to publish AuthorizationDebitFailed event", err)
			}
			RecordAuthorizationDebitFailed(ctx)
			// Return ExpectedFailureError so the consumer knows to ACK this message
			return nil, &ExpectedFailureError{Err: fmt.Errorf("no active authorization found for device %s", deviceID)}
		}
		return nil, fmt.Errorf("failed to get authorization: %w", err)
	}

	requestedDebit := recorded.GetDebitMsat()
	if requestedDebit <= 0 {
		return nil, fmt.Errorf("invalid debit amount: %d", requestedDebit)
	}

	// Use atomic update to consume from authorization
	// This reduces lock contention by doing calculation and update in database
	// The ConsumeAuthorization function handles insufficient funds by consuming what's available
	newRemaining, _, _, newStatus, err := esh.repo.ConsumeAuthorization(ctx, tx, authorizationID, requestedDebit)
	if err != nil {
		return nil, fmt.Errorf("failed to consume authorization: %w", err)
	}

	// Calculate actual debit amount (may be less if insufficient remaining)
	actualDebit := requestedDebit
	if remainingMsat < requestedDebit {
		actualDebit = remainingMsat
		logger.WithDeviceID(deviceID).
			DebugWithFields(ctx, "Insufficient remaining in authorization", map[string]interface{}{
				"authorization_id": authorizationID,
				"remaining_msat":   remainingMsat,
				"requested_msat":   requestedDebit,
				"actual_debit":     actualDebit,
			})
	}

	// Create debit entry for overflow if any
	// Calculate overflow delta (difference between requested and what was actually consumed from remaining)
	overflowDelta := requestedDebit - actualDebit
	var overflowEntry *EntryResponse
	if overflowDelta > 0 {
		entry, err := esh.repo.ApplyDebit(ctx, tx, DebitRequest{
			DeviceID:      deviceID,
			AmountMsat:    overflowDelta,
			Reason:        "AUTHORIZATION_OVERFLOW",
			CorrelationID: authorizationID,
			AllowNegative: true,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to apply overflow debit: %w", err)
		}
		overflowEntry = &entry
		// Record metrics for overflow entry
		RecordEntry(ctx, "debit", "overflow")
	}

	return &processConsumptionResult{
		authorizationID: authorizationID,
		deviceID:        deviceID,
		actualDebit:     actualDebit,
		newRemaining:    newRemaining,
		newStatus:       newStatus,
		overflowEntry:   overflowEntry,
	}, nil
}
