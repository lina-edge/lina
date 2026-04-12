package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/robertodantas/lina/internal"
	devicepb "github.com/robertodantas/lina/proto/gen/model/device"
	"google.golang.org/protobuf/encoding/protojson"
)

// messageRetryInfo tracks retry information for a message
type messageRetryInfo struct {
	retryCount  int
	lastRetryAt time.Time
	firstSeenAt time.Time
}

// isTransientPersistenceError reports errors that may resolve on retry (e.g. storage contention).
func isTransientPersistenceError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "database is locked") ||
		strings.Contains(errStr, "sqlite_busy") ||
		strings.Contains(errStr, "sqlite: database is locked") ||
		strings.Contains(errStr, "resource temporarily unavailable")
}

// EastWestStreamInterface wraps the internal StreamClient with consumption-specific methods for east-west stream communication
type EastWestStreamInterface struct {
	*internal.StreamClient
	cfg          Config
	repository   ConsumptionRepository
	consumerName string
	groupName    string
	// retryTracker tracks retry counts and timestamps for messages
	retryTracker sync.Map // map[string]*messageRetryInfo
}

// NewEastWestStreamInterface creates a new Redis stream client using the internal package
func NewEastWestStreamInterface(ctx context.Context, cfg Config, repository ConsumptionRepository) (*EastWestStreamInterface, error) {
	libClient, err := internal.NewStreamClientFromEnv(ctx)
	if err != nil {
		return nil, err
	}

	cname := cfg.StreamConsumerName
	if cname == "" {
		cname = defaultStreamConsumerName()
	}
	return &EastWestStreamInterface{
		StreamClient: libClient,
		cfg:          cfg,
		repository:   repository,
		consumerName: cname,
		groupName:    "consumption-consumers",
	}, nil
}

func defaultStreamConsumerName() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("consumption-%s-%d", host, os.Getpid())
}

// StartDeviceConsumer starts consuming from the event.device stream
func (ewsi *EastWestStreamInterface) StartDeviceConsumer(ctx context.Context, handler *EastWestStreamHandler) error {
	streamName := internal.StreamDevice
	streamCtx := ewsi.Context()

	// Create consumer group if it doesn't exist
	err := ewsi.XGroupCreateMkStreamWithSpan(streamCtx, streamName, ewsi.groupName, "0")
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		logger.WithStream(streamName, "consume").
			Warnf(streamCtx, "Failed to create consumer group: %v", err)
		// Continue anyway, group might already exist
	}

	par := clampParallelism(ewsi.cfg.ConsumeParallelism)
	logger.WithStream(streamName, "consume").
		Infof(streamCtx, "Starting device event consumer (name=%s, batch_parallelism=%d, xreadgroup_count=%d)", ewsi.consumerName, par, ewsi.cfg.StreamReadCount)

	// Start pending message retry mechanism in a separate goroutine
	go ewsi.startPendingMessageRetry(ctx, streamName, handler)

	// Start consumption loop
	return ewsi.consumeDeviceEvents(ctx, streamName, handler)
}

// consumeDeviceEvents handles the main consumption loop for device events
func (ewsi *EastWestStreamInterface) consumeDeviceEvents(ctx context.Context, streamName string, handler *EastWestStreamHandler) error {
	streamCtx := ewsi.Context()

	for {
		select {
		case <-ctx.Done():
			logger.WithStream(streamName, "consume").
				Info(streamCtx, "Stopping device event consumer")
			return ctx.Err()
		default:
			// Read from stream with blocking read (wait up to 5 seconds)
			streams, err := ewsi.XReadGroupWithSpan(streamCtx, streamName, ewsi.groupName, ewsi.consumerName, &redis.XReadGroupArgs{
				Group:    ewsi.groupName,
				Consumer: ewsi.consumerName,
				Streams:  []string{streamName, ">"},
				Count:    int64(ewsi.cfg.StreamReadCount),
				Block:    5 * time.Second,
			})

			if err != nil {
				if err == redis.Nil {
					// No messages, continue
					continue
				}
				logger.WithStream(streamName, "consume").
					Error(streamCtx, "Error reading from stream", err)
				time.Sleep(1 * time.Second)
				continue
			}

			for _, stream := range streams {
				ewsi.processMessagesParallel(streamCtx, streamName, stream.Messages, handler)
			}
		}
	}
}

// handleDeviceMessage decodes device event and routes to appropriate handler method
func (ewsi *EastWestStreamInterface) handleDeviceMessage(ctx context.Context, handler *EastWestStreamHandler, msg redis.XMessage) error {
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

	// Check event type and route to handler
	switch deviceEvent.GetType() {
	case devicepb.DeviceEventType_DEVICE_EVENT_TYPE_USAGE_REPORTED:
		usageReported := deviceEvent.GetUsageReported()
		if usageReported == nil || usageReported.GetUsage() == nil {
			return fmt.Errorf("missing usage_reported payload")
		}
		return handler.HandleUsageReported(ctx, usageReported.GetUsage())

	default:
		logger.WithStream(internal.StreamDevice, "consume").
			Debugf(ctx, "Skipping event type: %v", deviceEvent.GetType())
		return nil
	}
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

// processMessagesParallel runs TraceEventProcessing for each message with bounded concurrency.
func (ewsi *EastWestStreamInterface) processMessagesParallel(streamCtx context.Context, streamName string, msgs []redis.XMessage, handler *EastWestStreamHandler) {
	runStreamMessagesParallel(ewsi.cfg.ConsumeParallelism, msgs, func(msg redis.XMessage) {
		ackFn := func(ctx context.Context, msg redis.XMessage) error {
			if err := ewsi.XAckWithSpan(streamCtx, streamName, ewsi.groupName, msg.ID, &msg); err != nil {
				return err
			}
			if err := ewsi.XDelWithSpan(streamCtx, streamName, msg.ID); err != nil {
				logger.WithStream(streamName, "consume").
					Warnf(streamCtx, "XDEL after ACK failed for %s: %v", msg.ID, err)
			}
			return nil
		}
		if err := internal.TraceEventProcessing(streamCtx, streamName, msg, func(ctx context.Context, msg redis.XMessage) error {
			return ewsi.handleDeviceMessage(ctx, handler, msg)
		}, ackFn); err != nil {
			logger.WithStream(streamName, "consume").
				Errorf(streamCtx, "Error handling device event %s: %v", msg.ID, err)
		}
	})
}

func (ewsi *EastWestStreamInterface) processMessagesParallelRetry(streamCtx context.Context, streamName string, msgs []redis.XMessage, handler *EastWestStreamHandler) {
	runStreamMessagesParallel(ewsi.cfg.ConsumeParallelism, msgs, func(msg redis.XMessage) {
		ackFn := func(ctx context.Context, msg redis.XMessage) error {
			if err := ewsi.XAckWithSpan(streamCtx, streamName, ewsi.groupName, msg.ID, &msg); err != nil {
				return err
			}
			if err := ewsi.XDelWithSpan(streamCtx, streamName, msg.ID); err != nil {
				logger.WithStream(streamName, "consume").
					Warnf(streamCtx, "XDEL after ACK failed for %s: %v", msg.ID, err)
			}
			return nil
		}
		err := internal.TraceEventProcessing(streamCtx, streamName, msg, func(ctx context.Context, msg redis.XMessage) error {
			return ewsi.handleDeviceMessage(ctx, handler, msg)
		}, ackFn)
		if err != nil {
			if isTransientPersistenceError(err) {
				logger.WithStream(streamName, "consume").
					Warnf(streamCtx, "Transient persistence error on retry for message %s: %v (will retry later)", msg.ID, err)
			} else {
				logger.WithStream(streamName, "consume").
					Errorf(streamCtx, "Error handling retry event %s: %v", msg.ID, err)
			}
			return
		}
		logger.WithStream(streamName, "consume").
			Debugf(streamCtx, "Successfully retried pending message %s", msg.ID)
	})
}

// startPendingMessageRetry continuously retries pending messages that failed to process
// This handles transient failures (e.g., temporary DB issues) that might resolve later
// Uses blocking reads to process pending messages immediately when they become available
func (ewsi *EastWestStreamInterface) startPendingMessageRetry(ctx context.Context, streamName string, handler *EastWestStreamHandler) {
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

			ewsi.processMessagesParallelRetry(streamCtx, streamName, claimed, handler)
		}
	}
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
