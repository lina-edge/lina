package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/robertodantas/lnpay/internal"
	consumptionpb "github.com/robertodantas/lnpay/proto/gen/model/consumption"
	ledgermodel "github.com/robertodantas/lnpay/proto/gen/model/ledger"
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

// StartConsumptionConsumer starts consuming from the event.consumption stream
func (sh *StreamHandler) StartConsumptionConsumer(ctx context.Context) error {
	streamName := "event.consumption"
	client := sh.streamClient.Client()
	streamCtx := sh.streamClient.Context()

	// Create consumer group if it doesn't exist
	err := client.XGroupCreateMkStream(streamCtx, streamName, sh.groupName, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		log.Printf("Warning: failed to create consumer group: %v", err)
		// Continue anyway, group might already exist
	}

	log.Printf("Starting consumption consumer for stream: %s", streamName)

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping consumption consumer...")
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
					if err := sh.handleConsumptionEvent(streamCtx, msg); err != nil {
						log.Printf("Error handling consumption event %s: %v", msg.ID, err)
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
		log.Printf("Skipping event type: %v", consumptionEvent.GetType())
		return nil
	}

	recorded := consumptionEvent.GetDeviceConsumptionRecorded()
	if recorded == nil {
		return fmt.Errorf("missing device_consumption_recorded payload")
	}

	log.Printf("[CONSUMPTION] Device: %s, Debit: %d msat",
		recorded.GetDeviceId(), recorded.GetDebitMsat())

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
	authorizationID, remainingMsat, _, _, _, err := sh.repo.GetActiveAuthorization(ctx, tx, deviceID, now)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// No active authorization found - return error to skip event (will be retried later)
			log.Printf("No active authorization found for device %s, skipping event (will retry)", deviceID)
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
		log.Printf("Insufficient remaining in authorization %s: have %d, need %d",
			authorizationID, remainingMsat, debitAmount)
		// Still debit what we can, but mark as completed
		debitAmount = remainingMsat
	}

	// Update authorization: subtract debit amount
	newRemaining := remainingMsat - debitAmount
	newStatus := "active"
	if newRemaining <= 0 {
		newStatus = "completed"
	}

	if err := sh.repo.UpdateAuthorization(ctx, tx, authorizationID, newRemaining, newStatus); err != nil {
		return fmt.Errorf("failed to update authorization: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Publish events based on new status
	timestamp := time.Now().Format(time.RFC3339)
	if newStatus == "completed" {
		// Publish AuthorizationCompleted event
		if err := sh.PublishAuthorizationCompleted(ctx, authorizationID, deviceID, timestamp); err != nil {
			log.Printf("Failed to publish AuthorizationCompleted event: %v", err)
		}
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
	result := sh.streamClient.Client().XAdd(ctx, &redis.XAddArgs{
		Stream: streamName,
		Values: values,
	})

	if result.Err() != nil {
		return fmt.Errorf("failed to publish to Redis stream %s: %w", streamName, result.Err())
	}

	log.Printf("Published LedgerEvent (type: %v) to stream %s (ID: %s)", ledgerEvent.GetType(), streamName, result.Val())
	return nil
}

// StartExpirationChecker periodically checks for expired authorizations
func (sh *StreamHandler) StartExpirationChecker(ctx context.Context) error {
	log.Println("Starting authorization expiration checker...")

	ticker := time.NewTicker(1 * time.Minute) // Check every minute
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping expiration checker...")
			return ctx.Err()
		case <-ticker.C:
			if err := sh.checkExpiredAuthorizations(ctx); err != nil {
				log.Printf("Error checking expired authorizations: %v", err)
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

	// Update expired authorizations and publish events
	for _, auth := range expired {
		tx, err := sh.repo.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			log.Printf("Failed to begin transaction for expiration: %v", err)
			continue
		}

		if err := sh.repo.MarkAuthorizationExpired(ctx, tx, auth.AuthorizationID); err != nil {
			tx.Rollback()
			log.Printf("Failed to update expired authorization %s: %v", auth.AuthorizationID, err)
			continue
		}

		if err := tx.Commit(); err != nil {
			log.Printf("Failed to commit expiration update: %v", err)
			continue
		}

		// Publish expiration event
		timestamp := time.Now().Format(time.RFC3339)
		if err := sh.PublishAuthorizationExpired(ctx, auth.AuthorizationID, auth.DeviceID, timestamp); err != nil {
			log.Printf("Failed to publish AuthorizationExpired event: %v", err)
		}
	}

	if len(expired) > 0 {
		log.Printf("Marked %d authorizations as expired", len(expired))
	}

	return nil
}
