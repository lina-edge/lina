package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"

	consumptionpb "github.com/robertodantas/lina/proto/gen/model/consumption"
)

// consumptionBatchItem is one decoded message prepared for batch transaction processing.
type consumptionBatchItem struct {
	msg      redis.XMessage
	recorded *consumptionpb.DeviceConsumptionRecordedEvent // nil when skip is true
	skip     bool                                           // already processed or invalid; ACK'd without DB work
}

// consumptionBatchResult holds the per-message outcome of a batch run.
type consumptionBatchResult struct {
	msg         redis.XMessage
	result      *processConsumptionResult // non-nil on success — caller must publish events after commit
	expectedErr *ExpectedFailureError     // non-nil when no active authorization found
	err         error                     // non-nil on unexpected failure — message stays in pending for retry
	ack         bool                      // true → XACK + XDEL
}

// processConsumptionMessagesBatch splits msgs into sub-batches of ConsumeBatchSize and processes
// each sub-batch in a single SQLite transaction, amortising the WAL commit cost across N debits.
// Only called when ConsumeBatchSize > 1 and RepositoryType == "sqlite".
func (ewsi *EastWestStreamInterface) processConsumptionMessagesBatch(
	streamCtx context.Context, streamName string, msgs []redis.XMessage, pendingRetry bool,
) {
	size := ewsi.cfg.ConsumeBatchSize
	for i := 0; i < len(msgs); i += size {
		end := i + size
		if end > len(msgs) {
			end = len(msgs)
		}
		ewsi.runConsumptionBatch(streamCtx, streamName, msgs[i:end], pendingRetry)
	}
}

// runConsumptionBatch handles one sub-batch: decode → single transaction → publish/ACK.
func (ewsi *EastWestStreamInterface) runConsumptionBatch(
	ctx context.Context, streamName string, msgs []redis.XMessage, pendingRetry bool,
) {
	// Phase 1: decode and idempotency-check outside the transaction.
	items := make([]consumptionBatchItem, len(msgs))
	for i, msg := range msgs {
		items[i] = ewsi.decodeBatchItem(ctx, streamName, msg)
	}

	// Phase 2: one transaction for all non-skipped items.
	results := ewsi.executeBatchTx(ctx, streamName, items)

	// Phase 3: publish events (successes only) and ACK processed messages.
	for _, r := range results {
		if r.result != nil {
			ewsi.handler.publishConsumptionResult(ctx, r.result)
		}
		if r.ack {
			ackStart := time.Now()
			err := ewsi.XAckWithSpan(ctx, streamName, ewsi.groupName, r.msg.ID, &r.msg)
			RecordStreamAckLatency(ctx, streamName, "handle_consumption", time.Since(ackStart).Seconds(), err == nil, pendingRetry)
			if err != nil {
				logger.WithStream(streamName, "consume").
					Warnf(ctx, "batch: XACK failed for %s: %v", r.msg.ID, err)
				continue
			}
			if err := ewsi.XDelWithSpan(ctx, streamName, r.msg.ID); err != nil {
				logger.WithStream(streamName, "consume").
					Warnf(ctx, "batch: XDEL failed for %s: %v", r.msg.ID, err)
			}
		} else if r.err != nil {
			logger.WithStream(streamName, "consume").
				Errorf(ctx, "batch: message %s left in pending: %v", r.msg.ID, r.err)
		}
	}
}

// decodeBatchItem checks Redis idempotency and unmarshals the protojson event for one message.
func (ewsi *EastWestStreamInterface) decodeBatchItem(ctx context.Context, streamName string, msg redis.XMessage) consumptionBatchItem {
	item := consumptionBatchItem{msg: msg}

	alreadyProcessed, err := ewsi.isMessageProcessed(ctx, streamName, msg.ID)
	if err != nil {
		logger.WithStream(streamName, "consume").
			Warnf(ctx, "batch: idempotency check failed for %s (will process): %v", msg.ID, err)
	} else if alreadyProcessed {
		item.skip = true
		return item
	}

	eventJSON, ok := msg.Values["event"].(string)
	if !ok {
		logger.WithStream(streamName, "consume").
			Errorf(ctx, "batch: missing 'event' field in message %s", msg.ID)
		item.skip = true // invalid message — ACK to unblock the stream
		return item
	}

	var consumptionEvent consumptionpb.ConsumptionEvent
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	if err := opts.Unmarshal([]byte(eventJSON), &consumptionEvent); err != nil {
		logger.WithStream(streamName, "consume").
			Errorf(ctx, "batch: failed to unmarshal event %s: %v", msg.ID, err)
		item.skip = true
		return item
	}

	if consumptionEvent.GetType() != consumptionpb.ConsumptionEventType_CONSUMPTION_EVENT_TYPE_DEVICE_CONSUMPTION_RECORDED {
		item.skip = true // unknown event type — ACK to unblock
		return item
	}

	recorded := consumptionEvent.GetDeviceConsumptionRecorded()
	if recorded == nil {
		logger.WithStream(streamName, "consume").
			Errorf(ctx, "batch: nil DeviceConsumptionRecorded payload in %s", msg.ID)
		item.skip = true
		return item
	}
	item.recorded = recorded
	return item
}

// executeBatchTx attempts to process all non-skipped items in one transaction via tryBatchTx.
// If the transaction cannot be opened or committed, falls back to individual per-message processing.
func (ewsi *EastWestStreamInterface) executeBatchTx(ctx context.Context, streamName string, items []consumptionBatchItem) []consumptionBatchResult {
	results := make([]consumptionBatchResult, len(items))
	for i, item := range items {
		results[i] = consumptionBatchResult{msg: item.msg, ack: item.skip}
	}

	work := make([]int, 0, len(items))
	for i, item := range items {
		if !item.skip {
			work = append(work, i)
		}
	}
	if len(work) == 0 {
		return results
	}

	batchResults, ok := ewsi.tryBatchTx(ctx, streamName, items, work)
	if !ok {
		// Transaction failed — process each message individually.
		for _, idx := range work {
			results[idx] = ewsi.processSingleBatchItem(ctx, streamName, items[idx])
		}
		return results
	}
	for idx, r := range batchResults {
		results[idx] = r
	}
	return results
}

// tryBatchTx opens one transaction, runs each work item under a savepoint for per-message isolation,
// then commits the whole batch. Returns (results, true) on success, (nil, false) to trigger fallback.
// On fallback all items will be re-processed individually, so it is safe for idempotency.
func (ewsi *EastWestStreamInterface) tryBatchTx(
	ctx context.Context, streamName string, items []consumptionBatchItem, work []int,
) (map[int]consumptionBatchResult, bool) {
	tx, err := ewsi.handler.repo.BeginTx(ctx, &LedgerTxOptions{})
	if err != nil {
		logger.WithStream(streamName, "consume").
			Errorf(ctx, "batch: BeginTx failed: %v", err)
		return nil, false
	}
	defer func() { _ = tx.Rollback() }()

	// Need the raw *sql.Tx to execute SAVEPOINT statements.
	sqlTx, err := expectSqliteTx(tx)
	if err != nil {
		logger.WithStream(streamName, "consume").
			Errorf(ctx, "batch: not a SQLite transaction: %v", err)
		return nil, false
	}

	perMsg := make(map[int]consumptionBatchResult, len(work))

	for i, idx := range work {
		sp := fmt.Sprintf("sp%d", i)

		if _, spErr := sqlTx.ExecContext(ctx, "SAVEPOINT "+sp); spErr != nil {
			// If the savepoint cannot be created the batch tx is broken; fall back for everything.
			// Prior items in this batch (work[0..i-1]) are uncommitted and will be redone individually.
			logger.WithStream(streamName, "consume").
				Errorf(ctx, "batch: SAVEPOINT %s failed: %v", sp, spErr)
			return nil, false
		}

		result, procErr := ewsi.handler.processConsumptionWithTx(ctx, tx, items[idx].recorded)

		var expectedErr *ExpectedFailureError
		switch {
		case procErr == nil:
			// Success: keep the writes in the batch tx and release the savepoint.
			_, _ = sqlTx.ExecContext(ctx, "RELEASE "+sp)
			perMsg[idx] = consumptionBatchResult{msg: items[idx].msg, result: result, ack: true}

		case errors.As(procErr, &expectedErr):
			// Expected failure (no active auth): processConsumptionWithTx made no DB writes,
			// so releasing the savepoint is a no-op for the transaction state.
			_, _ = sqlTx.ExecContext(ctx, "RELEASE "+sp)
			perMsg[idx] = consumptionBatchResult{msg: items[idx].msg, expectedErr: expectedErr, ack: true}
			// Release idempotency marker so the message can be retried once an authorization arrives.
			if relErr := ewsi.releaseMessageIdempotencyMarker(ctx, streamName, items[idx].msg.ID); relErr != nil {
				logger.WithStream(streamName, "consume").
					Warnf(ctx, "batch: failed to release idempotency marker for %s: %v", items[idx].msg.ID, relErr)
			}

		default:
			// Unexpected DB error: undo only this message's writes via savepoint, leave for retry.
			_, _ = sqlTx.ExecContext(ctx, "ROLLBACK TO "+sp)
			_, _ = sqlTx.ExecContext(ctx, "RELEASE "+sp)
			perMsg[idx] = consumptionBatchResult{msg: items[idx].msg, err: procErr, ack: false}
			if relErr := ewsi.releaseMessageIdempotencyMarker(ctx, streamName, items[idx].msg.ID); relErr != nil {
				logger.WithStream(streamName, "consume").
					Warnf(ctx, "batch: failed to release idempotency marker for %s: %v", items[idx].msg.ID, relErr)
			}
			logger.WithStream(streamName, "consume").
				Errorf(ctx, "batch: rolled back message %s (kept in pending): %v", items[idx].msg.ID, procErr)
		}
	}

	commitStart := time.Now()
	if err := tx.Commit(); err != nil {
		RecordTxCommitLatency(ctx, "stream.batch_consumption", time.Since(commitStart).Seconds(), false)
		logger.WithStream(streamName, "consume").
			Errorf(ctx, "batch: commit failed (%v); falling back to individual processing", err)
		// Release all idempotency markers so messages can be retried individually.
		for _, idx := range work {
			_ = ewsi.releaseMessageIdempotencyMarker(ctx, streamName, items[idx].msg.ID)
		}
		return nil, false
	}
	RecordTxCommitLatency(ctx, "stream.batch_consumption", time.Since(commitStart).Seconds(), true)
	return perMsg, true
}

// processSingleBatchItem handles one message with its own transaction (fallback from batch failure).
// Event publishing for successes is NOT done here; the caller (runConsumptionBatch) handles it via
// the returned result so that publishing always happens after the commit.
func (ewsi *EastWestStreamInterface) processSingleBatchItem(ctx context.Context, streamName string, item consumptionBatchItem) consumptionBatchResult {
	if item.skip {
		return consumptionBatchResult{msg: item.msg, ack: true}
	}

	tx, err := ewsi.handler.repo.BeginTx(ctx, &LedgerTxOptions{})
	if err != nil {
		_ = ewsi.releaseMessageIdempotencyMarker(ctx, streamName, item.msg.ID)
		return consumptionBatchResult{msg: item.msg, err: err}
	}
	defer func() { _ = tx.Rollback() }()

	result, procErr := ewsi.handler.processConsumptionWithTx(ctx, tx, item.recorded)

	var expectedErr *ExpectedFailureError
	if procErr != nil {
		if errors.As(procErr, &expectedErr) {
			_ = tx.Rollback()
			_ = ewsi.releaseMessageIdempotencyMarker(ctx, streamName, item.msg.ID)
			return consumptionBatchResult{msg: item.msg, expectedErr: expectedErr, ack: true}
		}
		_ = ewsi.releaseMessageIdempotencyMarker(ctx, streamName, item.msg.ID)
		return consumptionBatchResult{msg: item.msg, err: procErr}
	}

	commitStart := time.Now()
	if err := tx.Commit(); err != nil {
		RecordTxCommitLatency(ctx, "stream.single_consumption_fallback", time.Since(commitStart).Seconds(), false)
		_ = ewsi.releaseMessageIdempotencyMarker(ctx, streamName, item.msg.ID)
		return consumptionBatchResult{msg: item.msg, err: err}
	}
	RecordTxCommitLatency(ctx, "stream.single_consumption_fallback", time.Since(commitStart).Seconds(), true)
	return consumptionBatchResult{msg: item.msg, result: result, ack: true}
}
