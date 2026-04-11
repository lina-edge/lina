package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/robertodantas/lina/internal"
	consumptionpb "github.com/robertodantas/lina/proto/gen/model/consumption"
	lightningmodel "github.com/robertodantas/lina/proto/gen/model/lightning"
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
	cfg          Config
	handler      *EastWestStreamHandler
	consumerName string
	groupName    string
	// retryTracker tracks retry counts and timestamps for messages
	retryTracker sync.Map // map[string]*messageRetryInfo
}

// NewEastWestStreamInterface creates a new Redis stream client using the internal package
func NewEastWestStreamInterface(ctx context.Context, cfg Config, handler *EastWestStreamHandler) (*EastWestStreamInterface, error) {
	libClient, err := internal.NewStreamClientFromEnv(ctx)
	if err != nil {
		return nil, err
	}

	cname := cfg.StreamConsumerName
	if cname == "" {
		cname = defaultLedgerStreamConsumerName()
	}
	return &EastWestStreamInterface{
		StreamClient: libClient,
		cfg:          cfg,
		handler:      handler,
		consumerName: cname,
		groupName:    "ledger-consumers",
	}, nil
}

func defaultLedgerStreamConsumerName() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("ledger-%s-%d", host, os.Getpid())
}

func clampParallelism(p int) int {
	if p < 1 {
		return 1
	}
	if p > 64 {
		return 64
	}
	return p
}

func runStreamMessagesParallel(p int, msgs []redis.XMessage, runOne func(redis.XMessage)) {
	p = clampParallelism(p)
	if len(msgs) == 0 {
		return
	}
	if p == 1 || len(msgs) == 1 {
		for _, msg := range msgs {
			runOne(msg)
		}
		return
	}
	sem := make(chan struct{}, p)
	var wg sync.WaitGroup
	for _, msg := range msgs {
		msg := msg
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			runOne(msg)
		}()
	}
	wg.Wait()
}

func messageAgeSecondsFromStreamID(messageID string, now time.Time) (float64, bool) {
	parts := strings.SplitN(messageID, "-", 2)
	if len(parts) == 0 || parts[0] == "" {
		return 0, false
	}
	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, false
	}
	age := now.Sub(time.UnixMilli(ms)).Seconds()
	if age < 0 {
		age = 0
	}
	return age, true
}

// processLightningMessagesParallel runs TraceEventProcessing for each message with bounded concurrency.
func (ewsi *EastWestStreamInterface) processLightningMessagesParallel(streamCtx context.Context, streamName string, msgs []redis.XMessage) {
	ewsi.processLightningMessagesParallelMode(streamCtx, streamName, msgs, false)
}

func (ewsi *EastWestStreamInterface) processLightningMessagesParallelMode(streamCtx context.Context, streamName string, msgs []redis.XMessage, pendingRetry bool) {
	runStreamMessagesParallel(ewsi.cfg.ConsumeParallelism, msgs, func(msg redis.XMessage) {
		if age, ok := messageAgeSecondsFromStreamID(msg.ID, time.Now()); ok {
			RecordStreamMessageAge(streamCtx, streamName, "handle_lightning", age, pendingRetry)
		}
		ackFn := func(ctx context.Context, msg redis.XMessage) error {
			ackStart := time.Now()
			err := ewsi.XAckWithSpan(streamCtx, streamName, ewsi.groupName, msg.ID, &msg)
			RecordStreamAckLatency(streamCtx, streamName, "handle_lightning", time.Since(ackStart).Seconds(), err == nil, pendingRetry)
			return err
		}
		handlerStart := time.Now()
		err := internal.TraceEventProcessing(streamCtx, streamName, msg, ewsi.handleLightningMessage, ackFn)
		RecordStreamHandlerLatency(streamCtx, streamName, "handle_lightning", time.Since(handlerStart).Seconds(), err == nil, pendingRetry)
		if err != nil {
			logger.WithStream(streamName, "consume").
				Errorf(streamCtx, "Error handling lightning event %s: %v", msg.ID, err)
		}
	})
}

// processConsumptionMessagesParallel runs TraceEventProcessing for each message with bounded concurrency.
func (ewsi *EastWestStreamInterface) processConsumptionMessagesParallel(streamCtx context.Context, streamName string, msgs []redis.XMessage) {
	ewsi.processConsumptionMessagesParallelMode(streamCtx, streamName, msgs, false)
}

func (ewsi *EastWestStreamInterface) processConsumptionMessagesParallelMode(streamCtx context.Context, streamName string, msgs []redis.XMessage, pendingRetry bool) {
	runStreamMessagesParallel(ewsi.cfg.ConsumeParallelism, msgs, func(msg redis.XMessage) {
		if age, ok := messageAgeSecondsFromStreamID(msg.ID, time.Now()); ok {
			RecordStreamMessageAge(streamCtx, streamName, "handle_consumption", age, pendingRetry)
		}
		ackFn := func(ctx context.Context, msg redis.XMessage) error {
			ackStart := time.Now()
			err := ewsi.XAckWithSpan(streamCtx, streamName, ewsi.groupName, msg.ID, &msg)
			RecordStreamAckLatency(streamCtx, streamName, "handle_consumption", time.Since(ackStart).Seconds(), err == nil, pendingRetry)
			return err
		}
		handlerStart := time.Now()
		err := internal.TraceEventProcessing(streamCtx, streamName, msg, ewsi.handleConsumptionMessage, ackFn)
		RecordStreamHandlerLatency(streamCtx, streamName, "handle_consumption", time.Since(handlerStart).Seconds(), err == nil, pendingRetry)
		if err != nil {
			var expectedErr *ExpectedFailureError
			if errors.As(err, &expectedErr) {
				if pendingRetry {
					logger.WithStream(streamName, "consume").
						Debugf(streamCtx, "Expected failure on retry, message will go back to pending: %v", expectedErr.Err)
				} else {
					logger.WithStream(streamName, "consume").
						Debugf(streamCtx, "Expected failure, message will go to pending for retry: %v", expectedErr.Err)
				}
			} else {
				if pendingRetry {
					logger.WithStream(streamName, "consume").
						Errorf(streamCtx, "Error handling retry event %s: %v", msg.ID, err)
				} else {
					logger.WithStream(streamName, "consume").
						Errorf(streamCtx, "Error handling consumption event %s: %v", msg.ID, err)
				}
			}
		}
	})
}

// StartLightningConsumer starts consuming from the event.lightning stream
func (ewsi *EastWestStreamInterface) StartLightningConsumer(ctx context.Context) error {
	streamName := "event.lightning"
	streamCtx := ewsi.Context()

	// Create consumer group if it doesn't exist
	err := ewsi.XGroupCreateMkStreamWithSpan(streamCtx, streamName, ewsi.groupName, "0")
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		logger.WithStream(streamName, "consume").
			Warnf(streamCtx, "Failed to create consumer group: %v", err)
	}

	par := clampParallelism(ewsi.cfg.ConsumeParallelism)
	logger.WithStream(streamName, "consume").
		Infof(streamCtx, "Starting lightning consumer (name=%s, batch_parallelism=%d, xreadgroup_count=%d)", ewsi.consumerName, par, ewsi.cfg.StreamReadCount)

	// Start pending message retry mechanism in a separate goroutine
	go ewsi.startPendingMessageRetry(ctx, streamName)

	for {
		select {
		case <-ctx.Done():
			logger.WithStream(streamName, "consume").
				Info(streamCtx, "Stopping lightning consumer")
			return ctx.Err()
		default:
			streams, err := ewsi.XReadGroupWithSpan(streamCtx, streamName, ewsi.groupName, ewsi.consumerName, &redis.XReadGroupArgs{
				Group:    ewsi.groupName,
				Consumer: ewsi.consumerName,
				Streams:  []string{streamName, ">"},
				Count:    int64(ewsi.cfg.StreamReadCount),
				Block:    5 * time.Second,
			})

			if err != nil {
				if err == redis.Nil {
					continue
				}
				logger.WithStream(streamName, "consume").
					Error(streamCtx, "Error reading from stream", err)
				time.Sleep(1 * time.Second)
				continue
			}

			for _, stream := range streams {
				ewsi.processLightningMessagesParallel(streamCtx, streamName, stream.Messages)
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
	streamCtx := ewsi.Context()

	// Create consumer group if it doesn't exist
	err := ewsi.XGroupCreateMkStreamWithSpan(streamCtx, streamName, ewsi.groupName, "0")
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		logger.WithStream(streamName, "consume").
			Warnf(streamCtx, "Failed to create consumer group: %v", err)
	}

	par := clampParallelism(ewsi.cfg.ConsumeParallelism)
	logger.WithStream(streamName, "consume").
		Infof(streamCtx, "Starting consumption consumer (name=%s, batch_parallelism=%d, xreadgroup_count=%d)", ewsi.consumerName, par, ewsi.cfg.StreamReadCount)

	// Start pending message retry mechanism in a separate goroutine
	go ewsi.startPendingMessageRetry(ctx, streamName)

	for {
		select {
		case <-ctx.Done():
			logger.WithStream(streamName, "consume").
				Info(streamCtx, "Stopping consumption consumer")
			return ctx.Err()
		default:
			streams, err := ewsi.XReadGroupWithSpan(streamCtx, streamName, ewsi.groupName, ewsi.consumerName, &redis.XReadGroupArgs{
				Group:    ewsi.groupName,
				Consumer: ewsi.consumerName,
				Streams:  []string{streamName, ">"},
				Count:    int64(ewsi.cfg.StreamReadCount),
				Block:    5 * time.Second,
			})

			if err != nil {
				if err == redis.Nil {
					continue
				}
				logger.WithStream(streamName, "consume").
					Error(streamCtx, "Error reading from stream", err)
				time.Sleep(1 * time.Second)
				continue
			}

			for _, stream := range streams {
				ewsi.processConsumptionMessagesParallel(streamCtx, streamName, stream.Messages)
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
			// isMessageProcessed sets a Redis key before the handler runs (duplicate-delivery guard).
			// ExpectedFailureError means "not applied yet" (e.g. no active authorization) — we must not
			// leave that key set or the next retry will treat the message as done and ACK without applying.
			var expectedErr *ExpectedFailureError
			if errors.As(err, &expectedErr) {
				if relErr := ewsi.releaseMessageIdempotencyMarker(ctx, streamName, msg.ID); relErr != nil {
					logger.WithStream(streamName, "consume").
						Warnf(ctx, "Failed to release idempotency marker for retryable consumption: %v", relErr)
				}
			}
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

// releaseMessageIdempotencyMarker deletes the per-message key set by isMessageProcessed so a later
// retry (same stream message ID, not ACKed) can run the handler again — required for retryable
// failures such as no active authorization.
func (ewsi *EastWestStreamInterface) releaseMessageIdempotencyMarker(ctx context.Context, streamName, messageID string) error {
	key := fmt.Sprintf("%s:%s:%s", processedMessageKeyPrefix, streamName, messageID)
	return ewsi.Client().Del(ctx, key).Err()
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
func (ewsi *EastWestStreamInterface) startPendingMessageRetry(ctx context.Context, streamName string) {
	streamCtx := ewsi.Context()
	retryConsumerName := ewsi.consumerName + "-retry"
	logger.WithStream(streamName, "consume").
		Info(streamCtx, "Starting pending message retry mechanism (continuous)")

	// Cleanup old retry tracking entries periodically
	go ewsi.cleanupRetryTracker(ctx)

	client := ewsi.Client()
	minIdleTime := 5 * time.Second // Only claim messages that have been pending for at least 5 seconds

	for {
		select {
		case <-ctx.Done():
			logger.WithStream(streamName, "consume").
				Info(streamCtx, "Stopping pending message retry")
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
					Errorf(streamCtx, "Error checking pending messages: %v", err)
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
					Errorf(streamCtx, "Error claiming messages: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			switch streamName {
			case "event.lightning":
				ewsi.processLightningMessagesParallelMode(streamCtx, streamName, claimed, true)
			case "event.consumption":
				ewsi.processConsumptionMessagesParallelMode(streamCtx, streamName, claimed, true)
			default:
				logger.WithStream(streamName, "consume").
					Warnf(streamCtx, "Unknown stream for pending retry: %s", streamName)
			}
		}
	}
}

// StartExpirationChecker periodically checks for expired authorizations
func (ewsi *EastWestStreamInterface) StartExpirationChecker(ctx context.Context, repo LedgerRepository, publisher *EastWestStreamPublisher) error {
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
func (ewsi *EastWestStreamInterface) checkExpiredAuthorizations(ctx context.Context, repo LedgerRepository, publisher *EastWestStreamPublisher) error {
	now := time.Now().Format(time.RFC3339)

	// Find expired active authorizations
	expired, err := repo.GetExpiredAuthorizations(ctx, now)
	if err != nil {
		return fmt.Errorf("failed to query expired authorizations: %w", err)
	}

	processed := 0

	// Update expired authorizations and publish events
	for _, auth := range expired {
		tx, err := repo.BeginTx(ctx, &LedgerTxOptions{})
		if err != nil {
			logger.Error(ctx, "Failed to begin transaction for expiration", err)
			continue
		}

		deviceID, remainingMsat, err := repo.GetActiveAuthorizationByID(ctx, tx, auth.AuthorizationID)
		if err != nil {
			_ = tx.Rollback()
			if errors.Is(err, ErrNotFound) {
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
			// Record metrics for expiration credit entry
			RecordEntry(ctx, "credit", "authorization")
		}

		if err := repo.MarkAuthorizationExpired(ctx, tx, auth.AuthorizationID); err != nil {
			_ = tx.Rollback()
			logger.WithDeviceID(deviceID).
				Errorf(ctx, "Failed to update expired authorization %s: %v", auth.AuthorizationID, err)
			continue
		}

		commitStart := time.Now()
		err = tx.Commit()
		RecordTxCommitLatency(ctx, "stream.expiration_checker", time.Since(commitStart).Seconds(), err == nil)
		if err != nil {
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

		// Record metrics
		RecordAuthorizationExpired(ctx)

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
