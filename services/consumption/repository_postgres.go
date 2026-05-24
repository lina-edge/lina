package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/robertodantas/lina/internal"
	"go.opentelemetry.io/otel/attribute"
)

// consumptionRepoPG is the Postgres implementation of ConsumptionRepository.
type consumptionRepoPG struct {
	db        *sql.DB
	sqlTracer *internal.SQLTracer
}

// openConsumptionRepoPostgres connects to Postgres and creates the schema if absent.
// maxOpenConns > 1 enables concurrent writers, unlike the SQLite single-connection constraint.
func openConsumptionRepoPostgres(dsn string, maxOpenConns int) (ConsumptionRepository, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Postgres: %w", err)
	}

	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxOpenConns / 2)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(2 * time.Minute)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("postgres ping failed: %w", err)
	}

	stmts := []string{
		`CREATE TABLE IF NOT EXISTS consumption_records (
			report_id          TEXT PRIMARY KEY,
			device_id          TEXT NOT NULL,
			debit_msat         BIGINT NOT NULL,
			fractional_msat    DOUBLE PRECISION NOT NULL,
			measure            DOUBLE PRECISION NOT NULL,
			price_per_unit_msat BIGINT NOT NULL,
			unit               TEXT NOT NULL,
			timestamp          TEXT NOT NULL,
			created_at         BIGINT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS consumption_outbox (
			report_id    TEXT PRIMARY KEY,
			published    INTEGER NOT NULL DEFAULT 0,
			published_at BIGINT,
			traceparent  TEXT,
			created_at   BIGINT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_published_created ON consumption_outbox (published, created_at)`,
	}

	repo := &consumptionRepoPG{
		db:        db,
		sqlTracer: internal.NewSQLTracer("repository.consumption"),
	}

	ctx := context.Background()
	attrs := []attribute.KeyValue{attribute.String("db.operation", "CREATE TABLE/INDEX")}
	for _, s := range stmts {
		if _, err := repo.sqlTracer.ExecWithSpan(ctx, "[repository] create schema", attrs, db, s); err != nil {
			return nil, fmt.Errorf("failed to create postgres schema: %w", err)
		}
	}

	return repo, nil
}

// CreateConsumptionRecord inserts a consumption row and optional outbox row in a single transaction.
// Idempotency: ON CONFLICT DO NOTHING on report_id (PRIMARY KEY). Returns inserted=false if duplicate.
// Only creates an outbox entry when inserted=true and debitMsat >= 1.
func (r *consumptionRepoPG) CreateConsumptionRecord(ctx context.Context, reportID, deviceID string, debitMsat int64, fractionalMsat float64, measure float64, pricePerUnitMsat int64, unit, timestamp string, traceContext map[string]string) (inserted bool, err error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		r.sqlTracer.LogSQLError(ctx, "[repository] begin create consumption record", []attribute.KeyValue{
			attribute.String("db.operation", "BEGIN"),
			attribute.String("db.table", "consumption_records"),
			attribute.String("report.id", reportID),
			attribute.String("device.id", deviceID),
		}, err)
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().Unix()

	insertAttrs := []attribute.KeyValue{
		attribute.String("db.operation", "INSERT"),
		attribute.String("db.table", "consumption_records"),
		attribute.String("report.id", reportID),
		attribute.String("device.id", deviceID),
		attribute.Int64("debit_msat", debitMsat),
		attribute.Float64("fractional_msat", fractionalMsat),
	}
	res, err := r.sqlTracer.ExecWithSpan(ctx, "[repository] create consumption record", insertAttrs, tx,
		`INSERT INTO consumption_records (
			report_id, device_id, debit_msat, fractional_msat,
			measure, price_per_unit_msat, unit, timestamp, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (report_id) DO NOTHING`,
		reportID, deviceID, debitMsat, fractionalMsat,
		measure, pricePerUnitMsat, unit, timestamp, now,
	)
	if err != nil {
		return false, fmt.Errorf("failed to insert consumption record: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		r.sqlTracer.LogSQLError(ctx, "[repository] create consumption rows affected", []attribute.KeyValue{
			attribute.String("db.operation", "ROWS AFFECTED"),
			attribute.String("db.table", "consumption_records"),
			attribute.String("report.id", reportID),
			attribute.String("device.id", deviceID),
		}, err)
		return false, fmt.Errorf("rows affected: %w", err)
	}
	if n == 0 {
		return false, nil
	}

	if debitMsat >= 1 {
		traceparent := ""
		if traceContext != nil {
			traceparent = traceContext["traceparent"]
		}
		outboxAttrs := []attribute.KeyValue{
			attribute.String("db.operation", "INSERT"),
			attribute.String("db.table", "consumption_outbox"),
			attribute.String("report.id", reportID),
		}
		_, err := r.sqlTracer.ExecWithSpan(ctx, "[repository] create outbox entry", outboxAttrs, tx,
			`INSERT INTO consumption_outbox (report_id, published, traceparent, created_at) VALUES ($1, 0, $2, $3)`,
			reportID, traceparent, now,
		)
		if err != nil {
			return false, fmt.Errorf("failed to insert into outbox: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		r.sqlTracer.LogSQLError(ctx, "[repository] commit create consumption record", []attribute.KeyValue{
			attribute.String("db.operation", "COMMIT"),
			attribute.String("db.table", "consumption_records"),
			attribute.String("report.id", reportID),
			attribute.String("device.id", deviceID),
		}, err)
		return false, fmt.Errorf("commit: %w", err)
	}
	return true, nil
}

func (r *consumptionRepoPG) GetUnpublishedOutboxEvents(ctx context.Context, limit int) ([]OutboxEvent, error) {
	query := `
		SELECT o.report_id, c.device_id, c.debit_msat, c.timestamp, c.created_at, o.traceparent
		FROM consumption_outbox o
		INNER JOIN consumption_records c ON o.report_id = c.report_id
		WHERE o.published = 0
		ORDER BY c.created_at ASC
		LIMIT $1`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "consumption_outbox"),
		attribute.Int("limit", limit),
	}
	rows, err := r.sqlTracer.QueryWithSpan(ctx, "[repository] get unpublished outbox events", attrs, r.db, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query unpublished outbox events: %w", err)
	}
	defer rows.Close()

	var events []OutboxEvent
	for rows.Next() {
		var e OutboxEvent
		var traceparent sql.NullString
		if err := rows.Scan(&e.ReportID, &e.DeviceID, &e.DebitMsat, &e.Timestamp, &e.CreatedAt, &traceparent); err != nil {
			return nil, fmt.Errorf("failed to scan outbox event: %w", err)
		}
		if traceparent.Valid && traceparent.String != "" {
			e.TraceContext = map[string]string{"traceparent": traceparent.String}
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		r.sqlTracer.LogSQLError(ctx, "[repository] get unpublished outbox events rows", attrs, err)
		return nil, fmt.Errorf("error iterating outbox events: %w", err)
	}
	return events, nil
}

func (r *consumptionRepoPG) MarkOutboxAsPublished(ctx context.Context, reportID string) error {
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "UPDATE"),
		attribute.String("db.table", "consumption_outbox"),
		attribute.String("report.id", reportID),
	}
	_, err := r.sqlTracer.ExecWithSpan(ctx, "[repository] mark outbox as published", attrs, r.db,
		`UPDATE consumption_outbox SET published = 1, published_at = $1 WHERE report_id = $2`,
		time.Now().Unix(), reportID,
	)
	if err != nil {
		return fmt.Errorf("failed to mark report %s as published: %w", reportID, err)
	}
	return nil
}

func (r *consumptionRepoPG) CleanupOutbox(ctx context.Context, retentionDays int) (int64, error) {
	cutoffTime := time.Now().Unix() - int64(retentionDays*24*60*60)
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "DELETE"),
		attribute.String("db.table", "consumption_outbox"),
		attribute.Int("retention_days", retentionDays),
	}
	result, err := r.sqlTracer.ExecWithSpan(ctx, "[repository] cleanup outbox", attrs, r.db,
		`DELETE FROM consumption_outbox WHERE published = 1 AND published_at < $1`,
		cutoffTime,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup outbox: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		r.sqlTracer.LogSQLError(ctx, "[repository] cleanup outbox rows affected", []attribute.KeyValue{
			attribute.String("db.operation", "ROWS AFFECTED"),
			attribute.String("db.table", "consumption_outbox"),
			attribute.Int("retention_days", retentionDays),
		}, err)
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}
	return rowsAffected, nil
}

func (r *consumptionRepoPG) ListDeviceConsumptions(ctx context.Context, deviceID string, limit int) ([]ConsumptionResponse, error) {
	query := `
		SELECT
			c.report_id,
			c.device_id,
			c.debit_msat,
			c.fractional_msat,
			c.measure,
			c.price_per_unit_msat,
			c.unit,
			c.timestamp,
			c.created_at,
			COALESCE(o.published, 0) AS published,
			o.traceparent
		FROM consumption_records c
		LEFT JOIN consumption_outbox o ON c.report_id = o.report_id
		WHERE c.device_id = $1
		ORDER BY c.created_at DESC
		LIMIT $2`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "consumption_records"),
		attribute.String("device.id", deviceID),
		attribute.Int("limit", limit),
	}
	rows, err := r.sqlTracer.QueryWithSpan(ctx, "[repository] list device consumptions", attrs, r.db, query, deviceID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query device consumptions: %w", err)
	}
	defer rows.Close()

	var results []ConsumptionResponse
	for rows.Next() {
		var resp ConsumptionResponse
		var published int
		var traceparent sql.NullString
		if err := rows.Scan(
			&resp.ReportID, &resp.DeviceID, &resp.DebitMsat, &resp.FractionalMsat,
			&resp.Measure, &resp.PricePerUnitMsat, &resp.Unit, &resp.Timestamp,
			&resp.CreatedAt, &published, &traceparent,
		); err != nil {
			return nil, fmt.Errorf("failed to scan consumption: %w", err)
		}
		resp.Published = published == 1
		if traceparent.Valid {
			resp.Traceparent = traceparent.String
		}
		results = append(results, resp)
	}
	if err := rows.Err(); err != nil {
		r.sqlTracer.LogSQLError(ctx, "[repository] list device consumptions rows", attrs, err)
		return nil, fmt.Errorf("error iterating consumptions: %w", err)
	}
	return results, nil
}

func (r *consumptionRepoPG) Close() error {
	return r.db.Close()
}
