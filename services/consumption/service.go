package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
)

type Service struct {
	cfg      Config
	db       *sql.DB
	registry *registryClient
	agg      *Aggregator
	workerID string
}

func NewService(cfg Config, db *sql.DB) *Service {
	return &Service{
		cfg:      cfg,
		db:       db,
		registry: newRegistryClient(cfg.RegistryBaseURL, cfg.ServiceToken, cfg.RegistryCacheTTL),
		agg:      NewAggregator(db),
		workerID: uuid.NewString(),
	}
}

func (s *Service) claimBatch(ctx context.Context, limit int) ([]int64, error) {
	now := time.Now().Unix()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.Exec(`
        UPDATE queue_events
           SET status='processing',
               worker_id=?,
               claimed_at=?,
               attempts=attempts+1
         WHERE id IN (
         	   SELECT id FROM queue_events
         	    WHERE status='pending'
         	    ORDER BY id
         	    LIMIT ?
         )
    `, s.workerID, now, limit)
	if err != nil {
		return nil, err
	}

	rows, err := tx.Query(`SELECT id FROM queue_events WHERE status='processing' AND worker_id=? AND claimed_at=?`, s.workerID, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return ids, nil
}

type RawQueueItem struct {
	ID        int64
	DeviceID  string
	Seq       int64
	TS        int64
	Quantity  sql.NullFloat64
	Counter   sql.NullFloat64
	Signature string
	CreatedAt int64
}

func (s *Service) loadQueueItem(ctx context.Context, qid int64) (RawQueueItem, error) {
	var ev RawQueueItem
	row := s.db.QueryRowContext(ctx, `
        SELECT re.id, re.device_id, re.seq, re.ts, re.quantity, re.counter, re.signature, re.created_at
          FROM queue_events q
          JOIN raw_events re ON re.id = q.raw_event_id
         WHERE q.id = ?`, qid)
	switch err := row.Scan(&ev.ID, &ev.DeviceID, &ev.Seq, &ev.TS, &ev.Quantity, &ev.Counter, &ev.Signature, &ev.CreatedAt); err {
	case nil:
		return ev, nil
	default:
		return RawQueueItem{}, err
	}
}

func (s *Service) setQueueDone(ctx context.Context, qid int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE queue_events SET status='done' WHERE id=?`, qid)
	return err
}
func (s *Service) setQueueError(ctx context.Context, qid int64, errMsg string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE queue_events SET status=CASE WHEN attempts<? THEN 'pending' ELSE 'error' END, last_error=? WHERE id=?`,
		s.cfg.MaxAttempts, truncate(errMsg, 500), qid)
	return err
}

func (s *Service) insertBatch(ctx context.Context, b Batch) error {
	_, err := s.db.ExecContext(ctx, `
        INSERT INTO consumption_batches(id, device_id, window_start, window_end, units, unit, unit_price_sats, total_sats, status, created_at)
        VALUES(?,?,?,?,?,?,?,?,?,?)`,
		b.ID, b.DeviceID, nullInt(b.WindowStart), nullInt(b.WindowEnd), b.Units, b.Unit, b.UnitPrice, b.TotalSats, "pending", b.CreatedAt)
	return err
}
func (s *Service) enqueueOutbox(ctx context.Context, kind, refID string, payload any) error {
	js, _ := json.Marshal(payload)
	_, err := s.db.ExecContext(ctx, `INSERT INTO outbox(kind, ref_id, payload, created_at) VALUES(?,?,?,?)`,
		kind, refID, string(js), time.Now().Unix())
	return err
}

func nullInt(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func (s *Service) workerLoop(ctx context.Context) {
	t := time.NewTicker(s.cfg.WorkerPollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			ids, err := s.claimBatch(ctx, s.cfg.WorkerBatchSize)
			if err != nil {
				log.Printf("[worker] claim error: %v", err)
				continue
			}
			for _, qid := range ids {
				if err := s.processQueueItem(ctx, qid); err != nil {
					log.Printf("[worker] process error: %v", err)
				}
			}
		}
	}
}

func (s *Service) processQueueItem(ctx context.Context, qid int64) error {
	ev, err := s.loadQueueItem(ctx, qid)
	if err != nil {
		_ = s.setQueueError(ctx, qid, "loadQueueItem: "+err.Error())
		return err
	}

	cfg, err := s.registry.GetConfig(ctx, ev.DeviceID)
	if err != nil {
		_ = s.setQueueError(ctx, qid, "registry: "+err.Error())
		return err
	}

	if cfg.SecretKey != "" {
		ok := verifySignature(cfg.SecretKey, ev.DeviceID, ev.TS, ev.Seq, ev.Quantity, ev.Counter, ev.Signature)
		if !ok {
			_ = s.setQueueError(ctx, qid, "invalid signature")
			return errors.New("invalid signature")
		}
	}

	var effectiveQty float64
	if cfg.ReportingMode == "counter" || ev.Counter.Valid {
		q, derr := s.deriveDeltaFromCounter(ctx, ev.DeviceID, ev.TS, ev.Counter, cfg)
		if derr != nil {
			_ = s.setQueueError(ctx, qid, "deriveDelta: "+derr.Error())
			return derr
		}
		effectiveQty = q
	} else {
		if !ev.Quantity.Valid {
			_ = s.setQueueError(ctx, qid, "missing quantity")
			return errors.New("missing quantity")
		}
		effectiveQty = ev.Quantity.Float64
	}

	if effectiveQty < 0 {
		effectiveQty = 0
	}

	batches, err := s.agg.OnEvent(ctx, cfg, AggEvent{
		DeviceID: ev.DeviceID,
		TS:       ev.TS,
		Quantity: effectiveQty,
	})
	if err != nil {
		_ = s.setQueueError(ctx, qid, "aggregate: "+err.Error())
		return err
	}

	for _, b := range batches {
		if err := s.insertBatch(ctx, b); err != nil {
			_ = s.setQueueError(ctx, qid, "insertBatch: "+err.Error())
			return err
		}
		if err := s.enqueueOutbox(ctx, "BatchReady", b.ID, b); err != nil {
			_ = s.setQueueError(ctx, qid, "enqueueOutbox: "+err.Error())
			return err
		}
	}

	if err := s.setQueueDone(ctx, qid); err != nil {
		return err
	}
	return nil
}

func (s *Service) deriveDeltaFromCounter(ctx context.Context, deviceID string, ts int64, counter sql.NullFloat64, cfg DeviceConfig) (float64, error) {
	if !counter.Valid {
		return 0, nil
	}
	var st deviceState
	row := s.db.QueryRowContext(ctx, `
        SELECT partial_remainder, current_bucket, sum_in_bucket, sum_for_threshold, last_counter, last_ts, updated_at
          FROM device_state WHERE device_id=?`, deviceID)
	switch err := row.Scan(&st.PartialRemainder, &st.CurrentBucket, &st.SumInBucket, &st.SumForThreshold, &st.LastCounter, &st.LastTS, &st.UpdatedAt); err {
	case sql.ErrNoRows:
		delta := 0.0
		if cfg.BillFromFirst {
			delta = counter.Float64
		}
		_, _ = s.db.ExecContext(ctx, `
          INSERT INTO device_state(device_id, partial_remainder, current_bucket, sum_in_bucket, sum_for_threshold, last_counter, last_ts, updated_at)
          VALUES(?,?,?,?,?,?,?,?)`,
			deviceID, st.PartialRemainder, st.CurrentBucket, st.SumInBucket, st.SumForThreshold, counter.Float64, ts, time.Now().Unix())
		return delta, nil
	case nil:
	default:
		return 0, err
	}

	if st.LastTS.Valid && ts <= st.LastTS.Int64 {
		return 0, nil
	}

	var delta float64
	if !st.LastCounter.Valid {
		delta = 0
		if cfg.BillFromFirst {
			delta = counter.Float64
		}
	} else {
		delta = counter.Float64 - st.LastCounter.Float64
		if delta < 0 {
			if cfg.MeterMax > 0 {
				delta = counter.Float64 + (cfg.MeterMax - st.LastCounter.Float64)
			} else {
				delta = counter.Float64
			}
		}
	}

	if cfg.MaxDeltaAbs > 0 && delta > cfg.MaxDeltaAbs {
		delta = cfg.MaxDeltaAbs
	}
	if delta < 0 {
		delta = 0
	}

	_, _ = s.db.ExecContext(ctx, `
      INSERT INTO device_state(device_id, partial_remainder, current_bucket, sum_in_bucket, sum_for_threshold, last_counter, last_ts, updated_at)
      VALUES(?,?,?,?,?,?,?,?)
      ON CONFLICT(device_id) DO UPDATE SET
        last_counter=excluded.last_counter,
        last_ts=excluded.last_ts,
        updated_at=excluded.updated_at
    `, deviceID, st.PartialRemainder, st.CurrentBucket, st.SumInBucket, st.SumForThreshold, counter.Float64, ts, time.Now().Unix())

	return delta, nil
}

func (s *Service) dispatcherLoop(ctx context.Context) {
	t := time.NewTicker(s.cfg.DispatcherEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.dispatchOnce(ctx, 50)
		}
	}
}

func (s *Service) dispatchOnce(ctx context.Context, limit int) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, kind, ref_id, payload FROM outbox WHERE sent_at IS NULL ORDER BY id LIMIT ?`, limit)
	if err != nil {
		log.Printf("[dispatch] query: %v", err)
		return
	}
	defer rows.Close()

	type item struct {
		ID      int64
		Kind    string
		RefID   string
		Payload string
	}
	var items []item
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.ID, &it.Kind, &it.RefID, &it.Payload); err == nil {
			items = append(items, it)
		}
	}
	for _, it := range items {
		if err := s.sendToLedger(ctx, it.Kind, it.RefID, it.Payload); err != nil {
			log.Printf("[dispatch] sendToLedger: %v", err)
			continue
		}
		_, _ = s.db.ExecContext(ctx, `UPDATE outbox SET sent_at=? WHERE id=?`, time.Now().Unix(), it.ID)
		_, _ = s.db.ExecContext(ctx, `UPDATE consumption_batches SET status='posted' WHERE id=?`, it.RefID)
	}
}

func (s *Service) sendToLedger(ctx context.Context, kind, refID, payload string) error {
	if kind != "BatchReady" {
		log.Printf("[sendToLedger] unknown kind: %s, refID: %s, payload: %s", kind, refID, payload)
		return fmt.Errorf("unknown kind: %s", kind)
	}

	// Unmarshal batch payload
	var batch Batch
	if err := json.Unmarshal([]byte(payload), &batch); err != nil {
		log.Printf("[sendToLedger] unmarshal batch: %v", err)
		return err
	}

	// Map to ledger DebitRequest
	ledgerReq := map[string]any{
		"device_id":       batch.DeviceID,
		"amount_sats":     int64(batch.TotalSats),
		"reason":          fmt.Sprintf("consumption batch %s", batch.ID),
		"idempotency_key": batch.ID,
		"allow_negative":  false, // Allow negative balance for consumption
	}

	js, _ := json.Marshal(ledgerReq)
	req, _ := http.NewRequestWithContext(ctx, "POST", s.cfg.LedgerURL+"/debit", bytes.NewBuffer(js))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", batch.ID)
	req.Header.Set("X-Service-Token", s.cfg.ServiceToken)

	log.Printf("[sendToLedger] POST %s/debit device_id=%s amount_sats=%d batch_id=%s", s.cfg.LedgerURL, batch.DeviceID, int64(batch.TotalSats), batch.ID)
	log.Printf("[sendToLedger] payload: %s", string(js))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("[sendToLedger] error: %v", err)
		return err
	}
	defer resp.Body.Close()
	log.Printf("[sendToLedger] response status: %s", resp.Status)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("ledger status: %s", resp.Status)
	}
	return nil
}
