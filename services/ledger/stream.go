package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/robertodantas/lnpay/internal"
	consumptionpb "github.com/robertodantas/lnpay/proto/gen/model/consumption"
	ledgermodel "github.com/robertodantas/lnpay/proto/gen/model/ledger"
	lightningmodel "github.com/robertodantas/lnpay/proto/gen/model/lightning"
)

const (
	authorizationExpiredReason    = "AUTHORIZATION_EXPIRED"
	lightningInvoiceSettledReason = "LIGHTNING_INVOICE_SETTLED"
)

// StreamHandler handles Redis stream operations for the ledger service
type StreamHandler struct {
	streamClient *internal.StreamClient
	repo         *LedgerRepository
	consumerName string
	groupName    string
}

// NewStreamHandler creates a new stream handler
func NewStreamHandler(streamClient *internal.StreamClient, repo *LedgerRepository) *StreamHandler {
	return &StreamHandler{
		streamClient: streamClient,
		repo:         repo,
		consumerName: "ledger-service",
		groupName:    "ledger-consumers",
	}
}

// StartLightningConsumer starts consuming from the event.lightning stream
func (sh *StreamHandler) StartLightningConsumer(ctx context.Context) error {
	streamName := "event.lightning"

	// Create consumer group if it doesn't exist
	err := sh.streamClient.XGroupCreateMkStreamWithSpan(ctx, streamName, sh.groupName, "0")
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		logger.WithStream(streamName, "consume").
			Warnf(ctx, "Failed to create consumer group: %v", err)
	}

	logger.WithStream(streamName, "consume").
		Info(ctx, "Starting lightning consumer")

	for {
		select {
		case <-ctx.Done():
			logger.WithStream(streamName, "consume").
				Info(ctx, "Stopping lightning consumer")
			return ctx.Err()
		default:
			streams, err := sh.streamClient.XReadGroupWithSpan(ctx, streamName, sh.groupName, sh.consumerName, &redis.XReadGroupArgs{
				Group:    sh.groupName,
				Consumer: sh.consumerName,
				Streams:  []string{streamName, ">"},
				Count:    10,
				Block:    5 * time.Second,
			})

			if err != nil {
				if err == redis.Nil {
					continue
				}
				logger.WithStream(streamName, "consume").
					Error(ctx, "Error reading from stream", err)
				time.Sleep(1 * time.Second)
				continue
			}

			for _, stream := range streams {
				for _, msg := range stream.Messages {
					// Create ack function
					ackFn := func(ctx context.Context, msg redis.XMessage) error {
						return sh.streamClient.XAckWithSpan(ctx, streamName, sh.groupName, msg.ID, &msg)
					}

					if err := internal.TraceEventProcessing(ctx, streamName, msg, sh.handleLightningEvent, ackFn); err != nil {
						logger.WithStream(streamName, "consume").
							Errorf(ctx, "Error handling lightning event %s: %v", msg.ID, err)
					}
				}
			}
		}
	}
}

func (sh *StreamHandler) handleLightningEvent(ctx context.Context, msg redis.XMessage) error {
	eventJSON, ok := msg.Values["event"].(string)
	if !ok {
		return fmt.Errorf("invalid lightning event format: missing 'event' field")
	}

	var lightningEvent lightningmodel.LightningEvent
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	if err := opts.Unmarshal([]byte(eventJSON), &lightningEvent); err != nil {
		return fmt.Errorf("failed to unmarshal lightning event: %w", err)
	}

	if lightningEvent.GetType() != lightningmodel.LightningEventType_LIGHTNING_EVENT_TYPE_INVOICE_SETTLED {
		logger.WithStream("event.lightning", "consume").
			Debugf(ctx, "Skipping lightning event type: %v", lightningEvent.GetType())
		return nil
	}

	settled := lightningEvent.GetInvoiceSettled()
	if settled == nil {
		return fmt.Errorf("missing invoice_settled payload")
	}

	return sh.processInvoiceSettled(ctx, settled)
}

// StartConsumptionConsumer starts consuming from the event.consumption stream
func (sh *StreamHandler) StartConsumptionConsumer(ctx context.Context) error {
	streamName := "event.consumption"

	// Create consumer group if it doesn't exist
	err := sh.streamClient.XGroupCreateMkStreamWithSpan(ctx, streamName, sh.groupName, "0")
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		logger.WithStream(streamName, "consume").
			Warnf(ctx, "Failed to create consumer group: %v", err)
	}

	logger.WithStream(streamName, "consume").
		Info(ctx, "Starting consumption consumer")

	for {
		select {
		case <-ctx.Done():
			logger.WithStream(streamName, "consume").
				Info(ctx, "Stopping consumption consumer")
			return ctx.Err()
		default:
			// Read from stream - this creates a span and returns a context with that span
			streams, err := sh.streamClient.XReadGroupWithSpan(ctx, streamName, sh.groupName, sh.consumerName, &redis.XReadGroupArgs{
				Group:    sh.groupName,
				Consumer: sh.consumerName,
				Streams:  []string{streamName, ">"},
				Count:    10,
				Block:    5 * time.Second,
			})

			if err != nil {
				if err == redis.Nil {
					continue
				}
				logger.WithStream(streamName, "consume").
					Error(ctx, "Error reading from stream", err)
				time.Sleep(1 * time.Second)
				continue
			}

			// Process messages with the context that has the read span
			for _, stream := range streams {
				for _, msg := range stream.Messages {
					// Create ack function that will be called within the processing span
					ackFn := func(ctx context.Context, msg redis.XMessage) error {
						return sh.streamClient.XAckWithSpan(ctx, streamName, sh.groupName, msg.ID, &msg)
					}

					// TraceEventProcessing now handles both processing and ack within same span
					if err := internal.TraceEventProcessing(ctx, streamName, msg, sh.handleConsumptionEvent, ackFn); err != nil {
						logger.WithStream(streamName, "consume").
							Errorf(ctx, "Error handling consumption event %s: %v", msg.ID, err)
					}
				}
			}
		}
	}
}

// handleConsumptionEvent processes a DeviceConsumptionRecorded event
func (sh *StreamHandler) handleConsumptionEvent(ctx context.Context, msg redis.XMessage) error {
	// Extract event JSON from message
	eventJSON, ok := msg.Values["event"].(string)
	if !ok {
		return fmt.Errorf("invalid event format: missing 'event' field")
	}

	// Unmarshal ConsumptionEvent
	var consumptionEvent consumptionpb.ConsumptionEvent
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	if err := opts.Unmarshal([]byte(eventJSON), &consumptionEvent); err != nil {
		return fmt.Errorf("failed to unmarshal consumption event: %w", err)
	}

	// Check event type
	if consumptionEvent.GetType() != consumptionpb.ConsumptionEventType_CONSUMPTION_EVENT_TYPE_DEVICE_CONSUMPTION_RECORDED {
		logger.WithStream("event.consumption", "consume").
			Debugf(ctx, "Skipping event type: %v", consumptionEvent.GetType())
		return nil
	}

	recorded := consumptionEvent.GetDeviceConsumptionRecorded()
	if recorded == nil {
		return fmt.Errorf("missing device_consumption_recorded payload")
	}

	logger.WithStream("event.consumption", "consume").
		WithDeviceID(recorded.GetDeviceId()).
		InfoWithFields(ctx, "Consumption received", map[string]interface{}{
			"debit_msat": recorded.GetDebitMsat(),
		})

	// Process the consumption: debit from authorization
	return sh.processConsumption(ctx, recorded)
}

// processConsumption debits from an authorization and updates its status
func (sh *StreamHandler) processConsumption(ctx context.Context, recorded *consumptionpb.DeviceConsumptionRecordedEvent) error {
	deviceID := recorded.GetDeviceId()
	if deviceID == "" {
		return fmt.Errorf("missing device_id in consumption event")
	}

	tx, err := sh.repo.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Find active authorization for the device
	// Order by created_at DESC to get the most recent active authorization
	now := time.Now().Format(time.RFC3339)
	authorizationID, remainingMsat, grantedMsat, overflowMsat, _, _, err := sh.repo.GetActiveAuthorization(ctx, tx, deviceID, now)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// No active authorization found - publish failed event
			logger.WithDeviceID(deviceID).
				Warn(ctx, "No active authorization found")
			timestamp := time.Now().Format(time.RFC3339)
			if err := sh.PublishAuthorizationDebitFailed(ctx, "", deviceID, recorded.GetDebitMsat(), 0, "NO_ACTIVE_AUTHORIZATION", timestamp); err != nil {
				logger.WithDeviceID(deviceID).
					WithStream("event.ledger", "produce").
					Error(ctx, "Failed to publish AuthorizationDebitFailed event", err)
			}
			return fmt.Errorf("no active authorization found for device %s", deviceID)
		}
		return fmt.Errorf("failed to get authorization: %w", err)
	}

	debitAmount := recorded.GetDebitMsat()
	if debitAmount <= 0 {
		return fmt.Errorf("invalid debit amount: %d", debitAmount)
	}

	// Check if we have enough remaining
	if remainingMsat < debitAmount {
		logger.WithDeviceID(deviceID).
			WarnWithFields(ctx, "Insufficient remaining in authorization", map[string]interface{}{
				"authorization_id": authorizationID,
				"remaining_msat":   remainingMsat,
				"requested_msat":   debitAmount,
			})
		// Still debit what we can, but mark as completed
		debitAmount = remainingMsat
	}

	// Update authorization: subtract debit amount
	newRemaining := remainingMsat - debitAmount
	newStatus := "active"
	if newRemaining <= 0 {
		newStatus = "completed"
	}

	currentConsumed := grantedMsat - remainingMsat
	if currentConsumed < 0 {
		currentConsumed = 0
	}
	newConsumed := currentConsumed + debitAmount
	if newConsumed > grantedMsat {
		newConsumed = grantedMsat
	}

	overflowDelta := recorded.GetDebitMsat() - debitAmount
	if overflowDelta < 0 {
		overflowDelta = 0
	}
	newOverflow := overflowMsat + overflowDelta

	if err := sh.repo.UpdateAuthorization(ctx, tx, authorizationID, newRemaining, newConsumed, newOverflow, newStatus); err != nil {
		return fmt.Errorf("failed to update authorization: %w", err)
	}

	// Create debit entry for overflow if any
	var overflowEntry *EntryResponse
	if newOverflow > 0 {
		entry, err := sh.repo.ApplyDebit(ctx, tx, DebitRequest{
			DeviceID:      deviceID,
			AmountMsat:    newOverflow,
			Reason:        "AUTHORIZATION_OVERFLOW",
			CorrelationID: authorizationID,
			AllowNegative: true,
		})
		if err != nil {
			return fmt.Errorf("failed to apply overflow debit: %w", err)
		}
		overflowEntry = &entry
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Publish events based on new status
	timestamp := time.Now().Format(time.RFC3339)

	// Publish AuthorizationDebited event
	if err := sh.PublishAuthorizationDebited(ctx, authorizationID, deviceID, debitAmount, newRemaining, timestamp); err != nil {
		logger.WithDeviceID(deviceID).
			WithStream("event.ledger", "produce").
			Error(ctx, "Failed to publish AuthorizationDebited event", err)
	}

	if newStatus == "completed" {
		// Publish AuthorizationCompleted event
		if err := sh.PublishAuthorizationCompleted(ctx, authorizationID, deviceID, timestamp); err != nil {
			logger.WithDeviceID(deviceID).
				WithStream("event.ledger", "produce").
				Error(ctx, "Failed to publish AuthorizationCompleted event", err)
		}
	}

	// Publish DeviceDebited event for overflow if any
	if overflowEntry != nil {
		overflowTimestamp := time.Unix(overflowEntry.CreatedAt, 0).UTC().Format(time.RFC3339)
		if err := sh.PublishDeviceDebited(ctx, deviceID, authorizationID, overflowEntry.AmountMsat, overflowEntry.BalanceAfter, overflowTimestamp); err != nil {
			logger.WithDeviceID(deviceID).
				WithStream("event.ledger", "produce").
				Error(ctx, "Failed to publish DeviceDebited event for overflow", err)
		}
	}

	return nil
}

// processInvoiceSettled credits the device balance when an invoice settles
func (sh *StreamHandler) processInvoiceSettled(ctx context.Context, settled *lightningmodel.InvoiceSettledEvent) error {
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
	if kind, _, ok, err := sh.repo.GetCachedIdem(ctx, creditReq.IdempotencyKey); err != nil {
		return fmt.Errorf("failed to check idempotency for invoice %s: %w", invoiceID, err)
	} else if ok {
		if kind == "credit" {
			logger.WithDeviceID(deviceID).
				WithStream("event.lightning", "consume").
				InfoWithFields(ctx, "Invoice already credited, skipping", map[string]interface{}{
					"invoice_id": invoiceID,
				})
			return nil
		}
		return fmt.Errorf("idempotency key %s already used for kind %s", invoiceID, kind)
	}

	tx, err := sh.repo.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("failed to begin tx for invoice %s: %w", invoiceID, err)
	}
	defer func() { _ = tx.Rollback() }()

	entry, err := sh.repo.ApplyCredit(ctx, tx, creditReq)
	if err != nil {
		return fmt.Errorf("failed to apply credit for invoice %s: %w", invoiceID, err)
	}

	if err := sh.repo.SaveIdem(ctx, tx, creditReq.IdempotencyKey, "credit", hashReq("credit", creditReq), entry); err != nil {
		return fmt.Errorf("failed to store idempotency for invoice %s: %w", invoiceID, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit credit for invoice %s: %w", invoiceID, err)
	}

	logger.WithDeviceID(deviceID).
		WithStream("event.lightning", "consume").
		InfoWithFields(ctx, "Credited device from invoice", map[string]interface{}{
			"invoice_id":    invoiceID,
			"amount_msat":   entry.AmountMsat,
			"balance_after": entry.BalanceAfter,
		})

	timestamp := time.Unix(entry.CreatedAt, 0).UTC().Format(time.RFC3339)
	if err := sh.PublishDeviceCredited(ctx, entry.DeviceID, entry.AmountMsat, entry.BalanceAfter, timestamp); err != nil {
		logger.WithDeviceID(deviceID).
			WithStream("event.ledger", "produce").
			Errorf(ctx, "Failed to publish DeviceCreditedEvent for invoice %s: %v", invoiceID, err)
	}

	return nil
}

// PublishAuthorizationCreated publishes an AuthorizationCreated event to event.ledger
func (sh *StreamHandler) PublishAuthorizationCreated(ctx context.Context, auth *ledgermodel.Authorization) error {
	event := &ledgermodel.AuthorizationCreatedEvent{
		Authorization: auth,
	}

	ledgerEvent := &ledgermodel.LedgerEvent{
		Type: ledgermodel.LedgerEventType_LEDGER_EVENT_TYPE_AUTHORIZATION_CREATED,
		Payload: &ledgermodel.LedgerEvent_AuthorizationCreated{
			AuthorizationCreated: event,
		},
	}

	return sh.publishLedgerEvent(ctx, ledgerEvent)
}

// PublishAuthorizationCompleted publishes an AuthorizationCompleted event to event.ledger
func (sh *StreamHandler) PublishAuthorizationCompleted(ctx context.Context, authorizationID, deviceID, timestamp string) error {
	event := &ledgermodel.AuthorizationCompletedEvent{
		AuthorizationId: authorizationID,
		DeviceId:        deviceID,
		Timestamp:       timestamp,
	}

	ledgerEvent := &ledgermodel.LedgerEvent{
		Type: ledgermodel.LedgerEventType_LEDGER_EVENT_TYPE_AUTHORIZATION_COMPLETED,
		Payload: &ledgermodel.LedgerEvent_AuthorizationCompleted{
			AuthorizationCompleted: event,
		},
	}

	return sh.publishLedgerEvent(ctx, ledgerEvent)
}

// PublishAuthorizationExpired publishes an AuthorizationExpired event to event.ledger
func (sh *StreamHandler) PublishAuthorizationExpired(ctx context.Context, authorizationID, deviceID, timestamp string) error {
	event := &ledgermodel.AuthorizationExpiredEvent{
		AuthorizationId: authorizationID,
		DeviceId:        deviceID,
		Timestamp:       timestamp,
	}

	ledgerEvent := &ledgermodel.LedgerEvent{
		Type: ledgermodel.LedgerEventType_LEDGER_EVENT_TYPE_AUTHORIZATION_EXPIRED,
		Payload: &ledgermodel.LedgerEvent_AuthorizationExpired{
			AuthorizationExpired: event,
		},
	}

	return sh.publishLedgerEvent(ctx, ledgerEvent)
}

// PublishDeviceCredited publishes a DeviceCreditedEvent to event.ledger
func (sh *StreamHandler) PublishDeviceCredited(ctx context.Context, deviceID string, amountMsat, newBalanceMsat int64, timestamp string) error {
	event := &ledgermodel.DeviceCreditedEvent{
		DeviceId:       deviceID,
		AmountMsat:     amountMsat,
		NewBalanceMsat: newBalanceMsat,
		Timestamp:      timestamp,
	}

	ledgerEvent := &ledgermodel.LedgerEvent{
		Type: ledgermodel.LedgerEventType_LEDGER_EVENT_TYPE_DEVICE_CREDITED,
		Payload: &ledgermodel.LedgerEvent_DeviceCredited{
			DeviceCredited: event,
		},
	}

	return sh.publishLedgerEvent(ctx, ledgerEvent)
}

// PublishDeviceDebited publishes a DeviceDebitedEvent to event.ledger
func (sh *StreamHandler) PublishDeviceDebited(ctx context.Context, deviceID, authorizationID string, amountMsat, newBalanceMsat int64, timestamp string) error {
	event := &ledgermodel.DeviceDebitedEvent{
		DeviceId:        deviceID,
		AuthorizationId: authorizationID,
		AmountMsat:      amountMsat,
		NewBalanceMsat:  newBalanceMsat,
		Timestamp:       timestamp,
	}

	ledgerEvent := &ledgermodel.LedgerEvent{
		Type: ledgermodel.LedgerEventType_LEDGER_EVENT_TYPE_DEVICE_DEBITED,
		Payload: &ledgermodel.LedgerEvent_DeviceDebited{
			DeviceDebited: event,
		},
	}

	return sh.publishLedgerEvent(ctx, ledgerEvent)
}

// PublishAuthorizationDebited publishes an AuthorizationDebitedEvent to event.ledger
func (sh *StreamHandler) PublishAuthorizationDebited(ctx context.Context, authorizationID, deviceID string, amountMsat, remainingMsat int64, timestamp string) error {
	event := &ledgermodel.AuthorizationDebitedEvent{
		AuthorizationId: authorizationID,
		DeviceId:        deviceID,
		AmountMsat:      amountMsat,
		RemainingMsat:   remainingMsat,
		Timestamp:       timestamp,
	}

	ledgerEvent := &ledgermodel.LedgerEvent{
		Type: ledgermodel.LedgerEventType_LEDGER_EVENT_TYPE_AUTHORIZATION_DEBITED,
		Payload: &ledgermodel.LedgerEvent_AuthorizationDebited{
			AuthorizationDebited: event,
		},
	}

	return sh.publishLedgerEvent(ctx, ledgerEvent)
}

// PublishAuthorizationDebitFailed publishes an AuthorizationDebitFailedEvent to event.ledger
func (sh *StreamHandler) PublishAuthorizationDebitFailed(ctx context.Context, authorizationID, deviceID string, requestedMsat, remainingMsat int64, reason, timestamp string) error {
	event := &ledgermodel.AuthorizationDebitFailedEvent{
		AuthorizationId: authorizationID,
		DeviceId:        deviceID,
		RequestedMsat:   requestedMsat,
		RemainingMsat:   remainingMsat,
		Reason:          reason,
		Timestamp:       timestamp,
	}

	ledgerEvent := &ledgermodel.LedgerEvent{
		Type: ledgermodel.LedgerEventType_LEDGER_EVENT_TYPE_AUTHORIZATION_DEBIT_FAILED,
		Payload: &ledgermodel.LedgerEvent_AuthorizationDebitFailed{
			AuthorizationDebitFailed: event,
		},
	}

	return sh.publishLedgerEvent(ctx, ledgerEvent)
}

// publishLedgerEvent publishes a LedgerEvent to the event.ledger stream
func (sh *StreamHandler) publishLedgerEvent(ctx context.Context, ledgerEvent *ledgermodel.LedgerEvent) error {
	// Serialize to JSON
	opts := protojson.MarshalOptions{UseProtoNames: true}
	jsonBytes, err := opts.Marshal(ledgerEvent)
	if err != nil {
		return fmt.Errorf("failed to marshal ledger event to JSON: %w", err)
	}

	// Publish to Redis stream "event.ledger"
	streamName := "event.ledger"
	values := map[string]interface{}{
		"event":     string(jsonBytes),
		"timestamp": time.Now().UnixMilli(),
	}

	// Use XADD to add entry to stream
	// Clean event type: "LEDGER_EVENT_TYPE_AUTHORIZATION_DEBITED" -> "AUTHORIZATION_DEBITED"
	eventTypeFull := ledgerEvent.GetType().String()
	eventType := eventTypeFull
	if len(eventTypeFull) > len("LEDGER_EVENT_TYPE_") && eventTypeFull[:len("LEDGER_EVENT_TYPE_")] == "LEDGER_EVENT_TYPE_" {
		eventType = eventTypeFull[len("LEDGER_EVENT_TYPE_"):]
	}
	streamID, err := sh.streamClient.XAddWithSpan(ctx, streamName, &redis.XAddArgs{
		Stream: streamName,
		Values: values,
	}, eventType)

	if err != nil {
		return fmt.Errorf("failed to publish to Redis stream %s: %w", streamName, err)
	}

	// Extract device_id from event if available
	deviceID := extractDeviceIDFromLedgerEvent(ledgerEvent)
	logEntry := logger.WithStream(streamName, "produce")
	if deviceID != "" {
		logEntry = logEntry.WithDeviceID(deviceID)
	}
	logEntry.InfoWithFields(ctx, "Published LedgerEvent", map[string]interface{}{
		"event_type": ledgerEvent.GetType().String(),
		"stream_id":  streamID,
	})
	return nil
}

// extractDeviceIDFromLedgerEvent extracts device_id from various ledger event types
func extractDeviceIDFromLedgerEvent(event *ledgermodel.LedgerEvent) string {
	switch payload := event.GetPayload().(type) {
	case *ledgermodel.LedgerEvent_AuthorizationCreated:
		if payload.AuthorizationCreated != nil && payload.AuthorizationCreated.Authorization != nil {
			return payload.AuthorizationCreated.Authorization.DeviceId
		}
	case *ledgermodel.LedgerEvent_AuthorizationDebited:
		if payload.AuthorizationDebited != nil {
			return payload.AuthorizationDebited.DeviceId
		}
	case *ledgermodel.LedgerEvent_AuthorizationCompleted:
		if payload.AuthorizationCompleted != nil {
			return payload.AuthorizationCompleted.DeviceId
		}
	case *ledgermodel.LedgerEvent_AuthorizationExpired:
		if payload.AuthorizationExpired != nil {
			return payload.AuthorizationExpired.DeviceId
		}
	case *ledgermodel.LedgerEvent_AuthorizationDebitFailed:
		if payload.AuthorizationDebitFailed != nil {
			return payload.AuthorizationDebitFailed.DeviceId
		}
	case *ledgermodel.LedgerEvent_DeviceCredited:
		if payload.DeviceCredited != nil {
			return payload.DeviceCredited.DeviceId
		}
	case *ledgermodel.LedgerEvent_DeviceDebited:
		if payload.DeviceDebited != nil {
			return payload.DeviceDebited.DeviceId
		}
	}
	return ""
}

// StartExpirationChecker periodically checks for expired authorizations
func (sh *StreamHandler) StartExpirationChecker(ctx context.Context) error {
	logger.Info(ctx, "Starting authorization expiration checker")

	ticker := time.NewTicker(1 * time.Minute) // Check every minute
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info(ctx, "Stopping expiration checker")
			return ctx.Err()
		case <-ticker.C:
			if err := sh.checkExpiredAuthorizations(ctx); err != nil {
				logger.Error(ctx, "Error checking expired authorizations", err)
			}
		}
	}
}

// checkExpiredAuthorizations finds and marks expired authorizations
func (sh *StreamHandler) checkExpiredAuthorizations(ctx context.Context) error {
	now := time.Now().Format(time.RFC3339)

	// Find expired active authorizations
	expired, err := sh.repo.GetExpiredAuthorizations(ctx, now)
	if err != nil {
		return fmt.Errorf("failed to query expired authorizations: %w", err)
	}

	processed := 0

	// Update expired authorizations and publish events
	for _, auth := range expired {
		tx, err := sh.repo.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			logger.Error(ctx, "Failed to begin transaction for expiration", err)
			continue
		}

		deviceID, remainingMsat, err := sh.repo.GetActiveAuthorizationByID(ctx, tx, auth.AuthorizationID)
		if err != nil {
			_ = tx.Rollback()
			if errors.Is(err, sql.ErrNoRows) {
				logger.Debugf(ctx, "Authorization %s already processed, skipping", auth.AuthorizationID)
				continue
			}
			logger.Errorf(ctx, "Failed to load authorization %s: %v", auth.AuthorizationID, err)
			continue
		}

		var creditEntry *EntryResponse
		if remainingMsat > 0 {
			entry, err := sh.repo.ApplyCredit(ctx, tx, CreditRequest{
				DeviceID:      deviceID,
				AmountMsat:    remainingMsat,
				Reason:        authorizationExpiredReason,
				CorrelationID: auth.AuthorizationID,
			})
			if err != nil {
				_ = tx.Rollback()
				logger.WithDeviceID(deviceID).
					Errorf(ctx, "Failed to credit device for expired authorization %s: %v", auth.AuthorizationID, err)
				continue
			}
			creditEntry = &entry
		}

		if err := sh.repo.MarkAuthorizationExpired(ctx, tx, auth.AuthorizationID); err != nil {
			_ = tx.Rollback()
			logger.WithDeviceID(deviceID).
				Errorf(ctx, "Failed to update expired authorization %s: %v", auth.AuthorizationID, err)
			continue
		}

		if err := tx.Commit(); err != nil {
			logger.WithDeviceID(deviceID).
				Errorf(ctx, "Failed to commit expiration update for %s: %v", auth.AuthorizationID, err)
			continue
		}

		processed++

		// Publish expiration event
		timestamp := time.Now().Format(time.RFC3339)
		if err := sh.PublishAuthorizationExpired(ctx, auth.AuthorizationID, deviceID, timestamp); err != nil {
			logger.WithDeviceID(deviceID).
				WithStream("event.ledger", "produce").
				Error(ctx, "Failed to publish AuthorizationExpired event", err)
		}

		if creditEntry != nil {
			creditTimestamp := time.Unix(creditEntry.CreatedAt, 0).UTC().Format(time.RFC3339)
			if err := sh.PublishDeviceCredited(ctx, deviceID, creditEntry.AmountMsat, creditEntry.BalanceAfter, creditTimestamp); err != nil {
				logger.WithDeviceID(deviceID).
					WithStream("event.ledger", "produce").
					Errorf(ctx, "Failed to publish DeviceCreditedEvent for authorization %s: %v", auth.AuthorizationID, err)
			}
		}
	}

	if processed > 0 {
		logger.InfoWithFields(ctx, "Marked authorizations as expired", map[string]interface{}{
			"count": processed,
		})
	}

	return nil
}
