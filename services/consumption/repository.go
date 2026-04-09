package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Key prefixes (Pebble lexicographic order). device_id and report_id must not contain '/'.
const (
	keyPrefixConsumptionRecord      = "consumption/record/"
	keyPrefixConsumptionByDevice    = "consumption/by_device/"
	keyPrefixOutboxRecord           = "outbox/record/"
	keyPrefixOutboxUnpublished      = "outbox/unpublished/"
)

// ConsumptionRepository manages Pebble storage for consumption records and the transactional outbox.
type ConsumptionRepository struct {
	db     *pebble.DB
	tracer trace.Tracer
}

// NewConsumptionRepository opens a Pebble store at storePath (directory).
func NewConsumptionRepository(storePath string) (*ConsumptionRepository, error) {
	db, err := pebble.Open(storePath, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("open pebble store: %w", err)
	}
	return &ConsumptionRepository{
		db:     db,
		tracer: otel.Tracer("repository.consumption"),
	}, nil
}

type storedConsumption struct {
	DeviceID         string  `json:"device_id"`
	DebitMsat        int64   `json:"debit_msat"`
	FractionalMsat   float64 `json:"fractional_msat"`
	Measure          float64 `json:"measure"`
	PricePerUnitMsat int64   `json:"price_per_unit_msat"`
	Unit             string  `json:"unit"`
	Timestamp        string  `json:"timestamp"`
	CreatedAt        int64   `json:"created_at"`
}

type storedOutbox struct {
	Published   bool   `json:"published"`
	PublishedAt int64  `json:"published_at,omitempty"`
	Traceparent string `json:"traceparent,omitempty"`
	CreatedAt   int64  `json:"created_at"`
}

func keyConsumptionRecord(reportID string) []byte {
	return []byte(keyPrefixConsumptionRecord + reportID)
}

func keyOutboxRecord(reportID string) []byte {
	return []byte(keyPrefixOutboxRecord + reportID)
}

// keyOutboxUnpublished orders unpublished rows by created_at (20-digit) then report_id for FIFO scans.
func keyOutboxUnpublished(createdAt int64, reportID string) []byte {
	return []byte(fmt.Sprintf("%s%020d/%s", keyPrefixOutboxUnpublished, createdAt, reportID))
}

// keyConsumptionByDevice orders by device, then inverted created time (hex), then report_id so iterators return newest first.
func keyConsumptionByDevice(deviceID string, createdAt int64, reportID string) []byte {
	inv := ^uint64(0) - uint64(createdAt)
	return []byte(fmt.Sprintf("%s%s/%016x/%s", keyPrefixConsumptionByDevice, deviceID, inv, reportID))
}

func prefixUpperBound(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		end[i]++
		if end[i] != 0 {
			return end
		}
	}
	return nil
}

// OutboxEvent represents an event stored in the outbox
type OutboxEvent struct {
	ReportID     string
	DeviceID     string
	DebitMsat    int64
	Timestamp    string
	CreatedAt    int64
	TraceContext map[string]string
}

// CreateConsumptionRecord inserts a consumption row and optional outbox entry in one atomic batch.
// Idempotency: duplicate report_id is a no-op (inserted=false). Outbox row is created only when
// inserted=true and debitMsat >= 1.
func (r *ConsumptionRepository) CreateConsumptionRecord(ctx context.Context, reportID, deviceID string, debitMsat int64, fractionalMsat float64, measure float64, pricePerUnitMsat int64, unit, timestamp string, traceContext map[string]string) (inserted bool, err error) {
	now := time.Now().Unix()

	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "BATCH"),
		attribute.String("db.table", "consumption_records"),
		attribute.String("report.id", reportID),
		attribute.String("device.id", deviceID),
		attribute.Int64("debit_msat", debitMsat),
		attribute.Float64("fractional_msat", fractionalMsat),
	}
	ctx, span := r.tracer.Start(ctx, "[repository] create consumption record", trace.WithAttributes(attrs...))
	defer span.End()

	crKey := keyConsumptionRecord(reportID)
	if _, closer, err := r.db.Get(crKey); err == nil {
		closer.Close()
		span.SetStatus(codes.Ok, "duplicate report_id")
		return false, nil
	} else if !errors.Is(err, pebble.ErrNotFound) {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return false, fmt.Errorf("get consumption key: %w", err)
	}

	rec := storedConsumption{
		DeviceID:         deviceID,
		DebitMsat:        debitMsat,
		FractionalMsat:   fractionalMsat,
		Measure:          measure,
		PricePerUnitMsat: pricePerUnitMsat,
		Unit:             unit,
		Timestamp:        timestamp,
		CreatedAt:        now,
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return false, fmt.Errorf("marshal consumption: %w", err)
	}

	batch := r.db.NewBatch()
	defer batch.Close()

	if err := batch.Set(crKey, payload, nil); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return false, fmt.Errorf("batch set consumption: %w", err)
	}

	devKey := keyConsumptionByDevice(deviceID, now, reportID)
	if err := batch.Set(devKey, nil, nil); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return false, fmt.Errorf("batch set device index: %w", err)
	}

	if debitMsat >= 1 {
		traceparent := ""
		if traceContext != nil {
			traceparent = traceContext["traceparent"]
		}
		ob := storedOutbox{
			Published:   false,
			Traceparent: traceparent,
			CreatedAt:   now,
		}
		obBytes, err := json.Marshal(ob)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return false, fmt.Errorf("marshal outbox: %w", err)
		}
		if err := batch.Set(keyOutboxRecord(reportID), obBytes, nil); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return false, fmt.Errorf("batch set outbox: %w", err)
		}
		if err := batch.Set(keyOutboxUnpublished(now, reportID), nil, nil); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return false, fmt.Errorf("batch set outbox unpublished index: %w", err)
		}
	}

	if err := batch.Commit(&pebble.WriteOptions{Sync: true}); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return false, fmt.Errorf("commit batch: %w", err)
	}
	span.SetStatus(codes.Ok, "success")
	return true, nil
}

// GetUnpublishedOutboxEvents retrieves unpublished events from the outbox (FIFO by created_at).
func (r *ConsumptionRepository) GetUnpublishedOutboxEvents(ctx context.Context, limit int) ([]OutboxEvent, error) {
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "ITER"),
		attribute.String("db.table", "consumption_outbox"),
		attribute.Int("limit", limit),
	}
	ctx, span := r.tracer.Start(ctx, "[repository] get unpublished outbox events", trace.WithAttributes(attrs...))
	defer span.End()

	iter, err := r.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(keyPrefixOutboxUnpublished),
		UpperBound: prefixUpperBound([]byte(keyPrefixOutboxUnpublished)),
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("iter obu: %w", err)
	}
	defer iter.Close()

	var events []OutboxEvent
	for iter.First(); iter.Valid() && len(events) < limit; iter.Next() {
		key := iter.Key()
		reportID, ok := parseOutboxUnpublishedKey(key)
		if !ok {
			continue
		}
		crVal, closer, err := r.db.Get(keyConsumptionRecord(reportID))
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, fmt.Errorf("get consumption for outbox: %w", err)
		}
		var sc storedConsumption
		if err := json.Unmarshal(crVal, &sc); err != nil {
			closer.Close()
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, fmt.Errorf("unmarshal consumption: %w", err)
		}
		closer.Close()

		obVal, closer, err := r.db.Get(keyOutboxRecord(reportID))
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, fmt.Errorf("get outbox: %w", err)
		}
		var so storedOutbox
		if err := json.Unmarshal(obVal, &so); err != nil {
			closer.Close()
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, fmt.Errorf("unmarshal outbox: %w", err)
		}
		closer.Close()

		e := OutboxEvent{
			ReportID:  reportID,
			DeviceID:  sc.DeviceID,
			DebitMsat: sc.DebitMsat,
			Timestamp: sc.Timestamp,
			CreatedAt: sc.CreatedAt,
		}
		if so.Traceparent != "" {
			e.TraceContext = map[string]string{"traceparent": so.Traceparent}
		}
		events = append(events, e)
	}
	if err := iter.Error(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("iter error: %w", err)
	}
	span.SetStatus(codes.Ok, "success")
	return events, nil
}

func parseOutboxUnpublishedKey(key []byte) (reportID string, ok bool) {
	s := string(key)
	if !strings.HasPrefix(s, keyPrefixOutboxUnpublished) {
		return "", false
	}
	s = s[len(keyPrefixOutboxUnpublished):]
	if len(s) < 20+1 || s[20] != '/' {
		return "", false
	}
	return s[21:], true
}

// MarkOutboxAsPublished marks an outbox entry as published.
func (r *ConsumptionRepository) MarkOutboxAsPublished(ctx context.Context, reportID string) error {
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "BATCH"),
		attribute.String("db.table", "consumption_outbox"),
		attribute.String("report.id", reportID),
	}
	ctx, span := r.tracer.Start(ctx, "[repository] mark outbox as published", trace.WithAttributes(attrs...))
	defer span.End()

	obKey := keyOutboxRecord(reportID)
	val, closer, err := r.db.Get(obKey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			span.SetStatus(codes.Ok, "no outbox row")
			return nil
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("get outbox: %w", err)
	}
	var so storedOutbox
	if err := json.Unmarshal(val, &so); err != nil {
		closer.Close()
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("unmarshal outbox: %w", err)
	}
	closer.Close()

	batch := r.db.NewBatch()
	defer batch.Close()

	if !so.Published {
		if err := batch.Delete(keyOutboxUnpublished(so.CreatedAt, reportID), nil); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return fmt.Errorf("delete obu index: %w", err)
		}
	}
	so.Published = true
	so.PublishedAt = time.Now().Unix()
	obBytes, err := json.Marshal(so)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("marshal outbox: %w", err)
	}
	if err := batch.Set(obKey, obBytes, nil); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("set outbox: %w", err)
	}
	if err := batch.Commit(&pebble.WriteOptions{Sync: true}); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("commit: %w", err)
	}
	span.SetStatus(codes.Ok, "success")
	return nil
}

// CleanupOutbox removes published outbox rows older than the retention period (consumption rows are kept).
func (r *ConsumptionRepository) CleanupOutbox(ctx context.Context, retentionDays int) (int64, error) {
	retentionSeconds := int64(retentionDays * 24 * 60 * 60)
	cutoffTime := time.Now().Unix() - retentionSeconds

	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "ITER_DELETE"),
		attribute.String("db.table", "consumption_outbox"),
		attribute.Int("retention_days", retentionDays),
	}
	ctx, span := r.tracer.Start(ctx, "[repository] cleanup outbox", trace.WithAttributes(attrs...))
	defer span.End()

	iter, err := r.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(keyPrefixOutboxRecord),
		UpperBound: prefixUpperBound([]byte(keyPrefixOutboxRecord)),
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, fmt.Errorf("iter outbox: %w", err)
	}
	defer iter.Close()

	var n int64
	batch := r.db.NewBatch()

	for iter.First(); iter.Valid(); iter.Next() {
		val := iter.Value()
		var so storedOutbox
		if err := json.Unmarshal(val, &so); err != nil {
			batch.Close()
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return n, fmt.Errorf("unmarshal outbox: %w", err)
		}
		if !so.Published || so.PublishedAt >= cutoffTime {
			continue
		}
		key := append([]byte(nil), iter.Key()...)
		if err := batch.Delete(key, nil); err != nil {
			batch.Close()
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return n, fmt.Errorf("batch delete: %w", err)
		}
		n++
	}
	if err := iter.Error(); err != nil {
		batch.Close()
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return n, fmt.Errorf("iter: %w", err)
	}
	if batch.Count() == 0 {
		batch.Close()
		span.SetStatus(codes.Ok, "success")
		return 0, nil
	}
	if err := batch.Commit(&pebble.WriteOptions{Sync: true}); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return n, fmt.Errorf("commit: %w", err)
	}
	span.SetStatus(codes.Ok, "success")
	return n, nil
}

// Close closes the Pebble store.
func (r *ConsumptionRepository) Close() error {
	return r.db.Close()
}

// ListDeviceConsumptions retrieves consumption records for a device with outbox status.
func (r *ConsumptionRepository) ListDeviceConsumptions(ctx context.Context, deviceID string, limit int) ([]ConsumptionResponse, error) {
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "ITER"),
		attribute.String("db.table", "consumption_records"),
		attribute.String("device.id", deviceID),
		attribute.Int("limit", limit),
	}
	ctx, span := r.tracer.Start(ctx, "[repository] list device consumptions", trace.WithAttributes(attrs...))
	defer span.End()

	prefix := []byte(fmt.Sprintf("%s%s/", keyPrefixConsumptionByDevice, deviceID))

	iter, err := r.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("iter device index: %w", err)
	}
	defer iter.Close()

	var results []ConsumptionResponse
	for iter.First(); iter.Valid() && len(results) < limit; iter.Next() {
		reportID, ok := parseConsumptionByDeviceKey(iter.Key())
		if !ok {
			continue
		}
		crVal, closer, err := r.db.Get(keyConsumptionRecord(reportID))
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, fmt.Errorf("get consumption: %w", err)
		}
		var sc storedConsumption
		if err := json.Unmarshal(crVal, &sc); err != nil {
			closer.Close()
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, fmt.Errorf("unmarshal consumption: %w", err)
		}
		closer.Close()

		resp := ConsumptionResponse{
			ReportID:         reportID,
			DeviceID:         sc.DeviceID,
			DebitMsat:        sc.DebitMsat,
			FractionalMsat:   sc.FractionalMsat,
			Measure:          sc.Measure,
			PricePerUnitMsat: sc.PricePerUnitMsat,
			Unit:             sc.Unit,
			Timestamp:        sc.Timestamp,
			CreatedAt:        sc.CreatedAt,
		}

		obVal, closer, err := r.db.Get(keyOutboxRecord(reportID))
		if err == nil {
			var so storedOutbox
			if err := json.Unmarshal(obVal, &so); err != nil {
				closer.Close()
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return nil, fmt.Errorf("unmarshal outbox: %w", err)
			}
			closer.Close()
			resp.Published = so.Published
			resp.Traceparent = so.Traceparent
		} else if errors.Is(err, pebble.ErrNotFound) {
			resp.Published = false
		} else {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, fmt.Errorf("get outbox: %w", err)
		}

		results = append(results, resp)
	}
	if err := iter.Error(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("iter: %w", err)
	}
	span.SetStatus(codes.Ok, "success")
	return results, nil
}

// parseConsumptionByDeviceKey expects keys shaped consumption/by_device/{deviceID}/{16 hex inverted created_at}/{reportID}.
func parseConsumptionByDeviceKey(key []byte) (reportID string, ok bool) {
	s := string(key)
	if !strings.HasPrefix(s, keyPrefixConsumptionByDevice) {
		return "", false
	}
	rest := s[len(keyPrefixConsumptionByDevice):]
	parts := strings.SplitN(rest, "/", 3)
	if len(parts) != 3 || len(parts[1]) != 16 {
		return "", false
	}
	return parts[2], true
}
