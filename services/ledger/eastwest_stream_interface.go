package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/robertodantas/lnpay/internal"
	consumptionpb "github.com/robertodantas/lnpay/proto/gen/model/consumption"
	lightningmodel "github.com/robertodantas/lnpay/proto/gen/model/lightning"
)

const (
	// Redis key prefix for tracking processed messages
	// Format: ledger:processed:message:{stream_name}:{message_id}
	processedMessageKeyPrefix = "ledger:processed:message"
	// TTL for processed message keys
	processedMessageTTL = 30 * time.Second
)

// messageRetryInfo tracks retry information for a message
type messageRetryInfo struct {
	lastRetryAt time.Time
	firstSeenAt time.Time
}

// EastWestStreamInterface wraps the internal StreamClient with ledger-specific methods for east-west stream communication
type EastWestStreamInterface struct {
	*internal.StreamClient
	handler      *EastWestStreamHandler
	consumerName string
	groupName    string
	// retryTracker tracks retry counts and timestamps for messages
	retryTracker sync.Map // map[string]*messageRetryInfo
}

// NewEastWestStreamInterface creates a new Redis stream client using the internal package
func NewEastWestStreamInterface(ctx context.Context, handler *EastWestStreamHandler) (*EastWestStreamInterface, error) {
	libClient, err := internal.NewStreamClientFromEnv(ctx)
	if err != nil {
		return nil, err
	}

	return &EastWestStreamInterface{
		StreamClient: libClient,
		handler:      handler,
		consumerName: "ledger-service",
		groupName:    "ledger-consumers",
	}, nil
}

// StartLightningConsumer starts consuming from the event.lightning stream
func (ewsi *EastWestStreamInterface) StartLightningConsumer(ctx context.Context) error {
	streamName := "event.lightning"

	// Create consumer group if it doesn't exist
	err := ewsi.XGroupCreateMkStreamWithSpan(ctx, streamName, ewsi.groupName, "0")
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		logger.WithStream(streamName, "consume").
			Warnf(ctx, "Failed to create consumer group: %v", err)
	}

	logger.WithStream(streamName, "consume").
		Info(ctx, "Starting lightning consumer")

	// Start pending message retry mechanism in a separate goroutine
	go ewsi.startPendingMessageRetry(ctx, streamName, ewsi.handleLightningMessage)

	for {
		select {
		case <-ctx.Done():
			logger.WithStream(streamName, "consume").
				Info(ctx, "Stopping lightning consumer")
			return ctx.Err()
		default:
			streams, err := ewsi.XReadGroupWithSpan(ctx, streamName, ewsi.groupName, ewsi.consumerName, &redis.XReadGroupArgs{
				Group:    ewsi.groupName,
				Consumer: ewsi.consumerName,
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
						return ewsi.XAckWithSpan(ctx, streamName, ewsi.groupName, msg.ID, &msg)
					}

					if err := internal.TraceEventProcessing(ctx, streamName, msg, ewsi.handleLightningMessage, ackFn); err != nil {
						logger.WithStream(streamName, "consume").
							Errorf(ctx, "Error handling lightning event %s: %v", msg.ID, err)
					}
				}
			}
		}
	}
}

// handleLightningMessage decodes lightning event, checks event type, and delegates to appropriate handler method
func (ewsi *EastWestStreamInterface) handleLightningMessage(ctx context.Context, msg redis.XMessage) error {
	eventJSON, ok := msg.Values["event"].(string)
	if !ok {
		return fmt.Errorf("invalid lightning event format: missing 'event' field")
	}

	var lightningEvent lightningmodel.LightningEvent
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	if err := opts.Unmarshal([]byte(eventJSON), &lightningEvent); err != nil {
		return fmt.Errorf("failed to unmarshal lightning event: %w", err)
	}

	// Check event type and route to appropriate handler method
	switch lightningEvent.GetType() {
	case lightningmodel.LightningEventType_LIGHTNING_EVENT_TYPE_INVOICE_SETTLED:
		settled := lightningEvent.GetInvoiceSettled()
		if settled == nil {
			return fmt.Errorf("missing invoice_settled payload")
		}
		return ewsi.handler.HandleInvoiceSettled(ctx, settled)

	default:
		logger.WithStream("event.lightning", "consume").
			Debugf(ctx, "Skipping lightning event type: %v", lightningEvent.GetType())
		return nil
	}
}

// StartConsumptionConsumer starts consuming from the event.consumption stream
func (ewsi *EastWestStreamInterface) StartConsumptionConsumer(ctx context.Context) error {
	streamName := "event.consumption"

	// Create consumer group if it doesn't exist
	err := ewsi.XGroupCreateMkStreamWithSpan(ctx, streamName, ewsi.groupName, "0")
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		logger.WithStream(streamName, "consume").
			Warnf(ctx, "Failed to create consumer group: %v", err)
	}

	logger.WithStream(streamName, "consume").
		Info(ctx, "Starting consumption consumer")

	// Start pending message retry mechanism in a separate goroutine
	go ewsi.startPendingMessageRetry(ctx, streamName, ewsi.handleConsumptionMessage)

	for {
		select {
		case <-ctx.Done():
			logger.WithStream(streamName, "consume").
				Info(ctx, "Stopping consumption consumer")
			return ctx.Err()
		default:
			// Read from stream - this creates a span and returns a context with that span
			streams, err := ewsi.XReadGroupWithSpan(ctx, streamName, ewsi.groupName, ewsi.consumerName, &redis.XReadGroupArgs{
				Group:    ewsi.groupName,
				Consumer: ewsi.consumerName,
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
						return ewsi.XAckWithSpan(ctx, streamName, ewsi.groupName, msg.ID, &msg)
					}

					// TraceEventProcessing now handles both processing and ack within same span
					err := internal.TraceEventProcessing(ctx, streamName, msg, ewsi.handleConsumptionMessage, ackFn)
					if err != nil {
						// Check if this is an expected failure
						var expectedErr *ExpectedFailureError
						if errors.As(err, &expectedErr) {
							// Expected failure - don't ACK, let it go to pending for retry with backoff
							// The pending retry mechanism will handle backoff and max retries
							logger.WithStream(streamName, "consume").
								Debugf(ctx, "Expected failure, message will go to pending for retry: %v", expectedErr.Err)
						} else {
							// Unexpected failure - don't ACK, let it go to pending for retry
							logger.WithStream(streamName, "consume").
								Errorf(ctx, "Error handling consumption event %s: %v", msg.ID, err)
						}
					}
				}
			}
		}
	}
}

// handleConsumptionMessage decodes consumption event and delegates to handler
func (ewsi *EastWestStreamInterface) handleConsumptionMessage(ctx context.Context, msg redis.XMessage) error {
	streamName := "event.consumption"

	// Check idempotency FIRST using Redis: atomically check and mark message as being processed
	// This prevents duplicate processing when the same message is picked up by both
	// the main consumer (">") and pending retry ("0") before ACK completes
	// Uses SET NX (set if not exists) for atomic check-and-set operation
	alreadyProcessed, err := ewsi.isMessageProcessed(ctx, streamName, msg.ID)
	if err != nil {
		// Log error but continue processing (Redis check failure shouldn't block processing)
		logger.WithStream(streamName, "consume").
			Warnf(ctx, "Failed to check message idempotency in Redis: %v, continuing anyway", err)
	} else if alreadyProcessed {
		// Message already processed or being processed by another goroutine, skip (idempotency)
		logger.WithStream(streamName, "consume").
			DebugWithFields(ctx, "Message already processed, skipping (idempotency)", map[string]interface{}{
				"message_id": msg.ID,
			})
		return nil
	}

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

	// Check event type and route to appropriate handler method
	switch consumptionEvent.GetType() {
	case consumptionpb.ConsumptionEventType_CONSUMPTION_EVENT_TYPE_DEVICE_CONSUMPTION_RECORDED:
		recorded := consumptionEvent.GetDeviceConsumptionRecorded()
		if recorded == nil {
			return fmt.Errorf("missing device_consumption_recorded payload")
		}
		err = ewsi.handler.HandleDeviceConsumptionRecorded(ctx, recorded)
		if err != nil {
			return err
		}

	default:
		logger.WithStream(streamName, "consume").
			Debugf(ctx, "Skipping event type: %v", consumptionEvent.GetType())
		return nil
	}

	// Mark message as processed in Redis after successful processing
	// Use Redis SET with expiration to track processed messages
	if err := ewsi.markMessageProcessed(ctx, streamName, msg.ID); err != nil {
		// Log error but don't fail - Redis tracking is best-effort
		logger.WithStream(streamName, "consume").
			Warnf(ctx, "Failed to mark message as processed in Redis: %v", err)
	}

	return nil
}

// isMessageProcessed checks if a message has already been processed using Redis
// Uses atomic SET NX to check and mark in one operation
// Returns: (alreadyProcessed, error)
// If alreadyProcessed is true, the message was already being processed by another goroutine
func (ewsi *EastWestStreamInterface) isMessageProcessed(ctx context.Context, streamName, messageID string) (bool, error) {
	key := fmt.Sprintf("%s:%s:%s", processedMessageKeyPrefix, streamName, messageID)
	client := ewsi.Client()

	// Use SET NX (set if not exists) to atomically check and mark
	// This prevents race conditions where two goroutines both check and both proceed
	set, err := client.SetNX(ctx, key, "1", processedMessageTTL).Result()
	if err != nil {
		return false, err
	}

	// If set is false, the key already existed (message already processed or being processed)
	return !set, nil
}

// markMessageProcessed marks a message as processed in Redis with TTL
// Note: This is now called after successful processing, but the atomic check in
// isMessageProcessed already prevents duplicates. This just ensures the key persists.
func (ewsi *EastWestStreamInterface) markMessageProcessed(ctx context.Context, streamName, messageID string) error {
	key := fmt.Sprintf("%s:%s:%s", processedMessageKeyPrefix, streamName, messageID)
	client := ewsi.Client()

	// Use SET with expiration to automatically clean up old entries
	// This extends/refreshes the TTL after successful processing
	return client.Set(ctx, key, "1", processedMessageTTL).Err()
}

// startPendingMessageRetry continuously retries pending messages that failed to process
// This handles transient failures (e.g., temporary DB issues) that might resolve later
// Uses XPENDING + XCLAIM to claim messages from the main consumer that have been pending too long
// handlerFn is the function to call for processing each message
func (ewsi *EastWestStreamInterface) startPendingMessageRetry(ctx context.Context, streamName string, handlerFn func(context.Context, redis.XMessage) error) {
	retryConsumerName := ewsi.consumerName + "-retry"
	logger.WithStream(streamName, "consume").
		Info(ctx, "Starting pending message retry mechanism (continuous)")

	// Cleanup old retry tracking entries periodically
	go ewsi.cleanupRetryTracker(ctx)

	client := ewsi.Client()
	minIdleTime := 5 * time.Second // Only claim messages that have been pending for at least 5 seconds

	for {
		select {
		case <-ctx.Done():
			logger.WithStream(streamName, "consume").
				Info(ctx, "Stopping pending message retry")
			return
		default:
			// Use XPENDING to find messages pending for the main consumer
			// Then use XCLAIM to claim them to the retry consumer
			// This avoids the issue where reading from "0" with the same consumer name
			// would see messages currently being processed by the main consumer
			pending, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{
				Stream:   streamName,
				Group:    ewsi.groupName,
				Start:    "-",
				End:      "+",
				Count:    10,
				Consumer: ewsi.consumerName, // Only look at messages pending for the main consumer
			}).Result()

			if err != nil {
				if err == redis.Nil {
					// No pending messages, wait a bit before checking again
					time.Sleep(1 * time.Second)
					continue
				}
				logger.WithStream(streamName, "consume").
					Errorf(ctx, "Error checking pending messages: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			// Filter messages that have been idle long enough
			var messageIDs []string
			for _, p := range pending {
				if p.Idle >= minIdleTime {
					messageIDs = append(messageIDs, p.ID)
				}
			}

			if len(messageIDs) == 0 {
				// No messages to claim, wait a bit
				time.Sleep(1 * time.Second)
				continue
			}

			// Claim messages to the retry consumer
			claimed, err := client.XClaim(ctx, &redis.XClaimArgs{
				Stream:   streamName,
				Group:    ewsi.groupName,
				Consumer: retryConsumerName,
				MinIdle:  minIdleTime,
				Messages: messageIDs,
			}).Result()

			if err != nil {
				logger.WithStream(streamName, "consume").
					Errorf(ctx, "Error claiming messages: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			// Process claimed messages
			for _, msg := range claimed {
				ackFn := func(ctx context.Context, msg redis.XMessage) error {
					return ewsi.XAckWithSpan(ctx, streamName, ewsi.groupName, msg.ID, &msg)
				}

				err := internal.TraceEventProcessing(ctx, streamName, msg, handlerFn, ackFn)
				if err != nil {
					var expectedErr *ExpectedFailureError
					if errors.As(err, &expectedErr) {
						logger.WithStream(streamName, "consume").
							Debugf(ctx, "Expected failure on retry, message will go back to pending: %v", expectedErr.Err)
					} else {
						logger.WithStream(streamName, "consume").
							Errorf(ctx, "Error handling retry event %s: %v", msg.ID, err)
					}
				}
			}
		}
	}
}

// StartExpirationChecker periodically checks for expired authorizations
func (ewsi *EastWestStreamInterface) StartExpirationChecker(ctx context.Context, repo *LedgerRepository, publisher *EastWestStreamPublisher) error {
	logger.Info(ctx, "Starting authorization expiration checker")

	ticker := time.NewTicker(1 * time.Minute) // Check every minute
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info(ctx, "Stopping expiration checker")
			return ctx.Err()
		case <-ticker.C:
			if err := ewsi.checkExpiredAuthorizations(ctx, repo, publisher); err != nil {
				logger.Error(ctx, "Error checking expired authorizations", err)
			}
		}
	}
}

// checkExpiredAuthorizations finds and marks expired authorizations
func (ewsi *EastWestStreamInterface) checkExpiredAuthorizations(ctx context.Context, repo *LedgerRepository, publisher *EastWestStreamPublisher) error {
	now := time.Now().Format(time.RFC3339)

	// Find expired active authorizations
	expired, err := repo.GetExpiredAuthorizations(ctx, now)
	if err != nil {
		return fmt.Errorf("failed to query expired authorizations: %w", err)
	}

	processed := 0

	// Update expired authorizations and publish events
	for _, auth := range expired {
		tx, err := repo.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			logger.Error(ctx, "Failed to begin transaction for expiration", err)
			continue
		}

		deviceID, remainingMsat, err := repo.GetActiveAuthorizationByID(ctx, tx, auth.AuthorizationID)
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
			entry, err := repo.ApplyCredit(ctx, tx, CreditRequest{
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

		if err := repo.MarkAuthorizationExpired(ctx, tx, auth.AuthorizationID); err != nil {
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
		if err := publisher.PublishAuthorizationExpired(ctx, auth.AuthorizationID, deviceID, timestamp); err != nil {
			logger.WithDeviceID(deviceID).
				WithStream("event.ledger", "produce").
				Error(ctx, "Failed to publish AuthorizationExpired event", err)
		}

		if creditEntry != nil {
			creditTimestamp := time.Unix(creditEntry.CreatedAt, 0).UTC().Format(time.RFC3339)
			if err := publisher.PublishDeviceCredited(ctx, deviceID, creditEntry.AmountMsat, creditEntry.BalanceAfter, creditTimestamp); err != nil {
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

// cleanupRetryTracker periodically cleans up old retry tracking entries
func (ewsi *EastWestStreamInterface) cleanupRetryTracker(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			cleaned := 0
			ewsi.retryTracker.Range(func(key, value interface{}) bool {
				info := value.(*messageRetryInfo)
				// Remove entries older than 1 hour that haven't been retried recently
				if now.Sub(info.firstSeenAt) > 1*time.Hour && now.Sub(info.lastRetryAt) > 30*time.Minute {
					ewsi.retryTracker.Delete(key)
					cleaned++
				}
				return true
			})
			if cleaned > 0 {
				logger.Debugf(ctx, "Cleaned up %d old retry tracking entries", cleaned)
			}
		}
	}
}

// Close closes the Redis client connection (delegates to embedded internal client)
func (ewsi *EastWestStreamInterface) Close() error {
	return ewsi.StreamClient.Close()
}
