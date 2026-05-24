package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/robertodantas/lina/internal"
	"go.opentelemetry.io/otel/attribute"
	_ "modernc.org/sqlite"
)

// consumptionRepoSQLite is the SQLite implementation of ConsumptionRepository.
type consumptionRepoSQLite struct {
	db        *sql.DB
	sqlTracer *internal.SQLTracer

	insertConsumptionStmt *sql.Stmt
	insertOutboxStmt      *sql.Stmt
	markPublishedStmt     *sql.Stmt
}

// openConsumptionRepoSQLite creates and initializes the SQLite database with schema.
func openConsumptionRepoSQLite(dbPath string, busyTimeoutMS int) (ConsumptionRepository, error) {
	// WAL + busy_timeout + performance optimizations for high load
	// - WAL mode: allows concurrent readers and one writer
	// - busy_timeout: how long to wait when database is locked (in ms)
	// - synchronous(NORMAL): good balance between safety and performance with WAL
	// - cache_size: increase cache to 8MB (negative = KB, so -8192 = 8MB, default is -2000 = 2MB)
	// - temp_store: use memory for temporary tables/indexes (2 = memory)
	// - mmap_size: use memory-mapped I/O for better performance (268435456 = 256MB)
	// - foreign_keys: enable foreign key constraints
	dsn := fmt.Sprintf(
		"%s?_pragma=busy_timeout(%d)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=cache_size(-8192)&_pragma=temp_store(2)&_pragma=mmap_size(268435456)&_pragma=foreign_keys(1)",
		dbPath, busyTimeoutMS,
	)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to SQLite: %w", err)
	}

	// Configure connection pool for SQLite
	// SQLite works best with limited connections due to its locking model
	// With WAL mode, we can have multiple readers but only one writer at a time
	// Set max open connections to a reasonable number (10-20 is good for WAL mode)
	db.SetMaxOpenConns(20)
	// Keep some connections idle for reuse
	db.SetMaxIdleConns(5)
	// Connection lifetime - close idle connections after 5 minutes
	db.SetConnMaxLifetime(5 * time.Minute)
	// Idle timeout - close idle connections after 10 minutes
	db.SetConnMaxIdleTime(10 * time.Minute)

	// Create tables and indexes
	stmts := []string{
		// Consumption records table - stores processed usage records per device_id with idempotency
		// This is the source of truth for business data
		`CREATE TABLE IF NOT EXISTS consumption_records (
			report_id TEXT PRIMARY KEY,
			device_id TEXT NOT NULL,
			debit_msat INTEGER NOT NULL,
			fractional_msat REAL NOT NULL,
			measure REAL NOT NULL,
			price_per_unit_msat INTEGER NOT NULL,
			unit TEXT NOT NULL,
			timestamp TEXT NOT NULL,
			created_at INTEGER NOT NULL
		)`,
		// Outbox table - minimal table for transactional outbox pattern
		// References consumption_records via report_id (acts as foreign key)
		// Only stores what's needed for publishing: report_id and published status
		`CREATE TABLE IF NOT EXISTS consumption_outbox (
			report_id TEXT PRIMARY KEY,
			published INTEGER NOT NULL DEFAULT 0,
			published_at INTEGER,
			traceparent TEXT,
			created_at INTEGER NOT NULL
		)`,
		// Index for consumption_outbox polling (published=0 ordered by created_at)
		`CREATE INDEX IF NOT EXISTS idx_published_created ON consumption_outbox (published, created_at)`,
	}

	repo := &consumptionRepoSQLite{
		db:        db,
		sqlTracer: internal.NewSQLTracer("repository.consumption"),
	}

	ctx := context.Background()
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "CREATE TABLE/INDEX"),
	}
	for _, s := range stmts {
		if _, err := repo.sqlTracer.ExecWithSpan(ctx, "[repository] create schema", attrs, db, s); err != nil {
			return nil, fmt.Errorf("failed to create schema: %w", err)
		}
	}

	insertConsumptionSQL := `
		INSERT OR IGNORE INTO consumption_records (
			report_id, device_id, debit_msat, fractional_msat,
			measure, price_per_unit_msat, unit, timestamp, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	repo.insertConsumptionStmt, err = db.PrepareContext(ctx, insertConsumptionSQL)
	if err != nil {
		repo.sqlTracer.LogSQLError(ctx, "[repository] prepare insert consumption", []attribute.KeyValue{
			attribute.String("db.operation", "PREPARE"),
			attribute.String("db.table", "consumption_records"),
		}, err)
		return nil, fmt.Errorf("prepare insert consumption: %w", err)
	}

	insertOutboxSQL := `
		INSERT INTO consumption_outbox (report_id, published, traceparent, created_at)
		VALUES (?, 0, ?, ?)`
	repo.insertOutboxStmt, err = db.PrepareContext(ctx, insertOutboxSQL)
	if err != nil {
		repo.sqlTracer.LogSQLError(ctx, "[repository] prepare insert outbox", []attribute.KeyValue{
			attribute.String("db.operation", "PREPARE"),
			attribute.String("db.table", "consumption_outbox"),
		}, err)
		_ = repo.insertConsumptionStmt.Close()
		return nil, fmt.Errorf("prepare insert outbox: %w", err)
	}

	markPublishedSQL := `
		UPDATE consumption_outbox
		SET published = 1, published_at = ?
		WHERE report_id = ?`
	repo.markPublishedStmt, err = db.PrepareContext(ctx, markPublishedSQL)
	if err != nil {
		repo.sqlTracer.LogSQLError(ctx, "[repository] prepare mark published", []attribute.KeyValue{
			attribute.String("db.operation", "PREPARE"),
			attribute.String("db.table", "consumption_outbox"),
		}, err)
		_ = repo.insertConsumptionStmt.Close()
		_ = repo.insertOutboxStmt.Close()
		return nil, fmt.Errorf("prepare mark published: %w", err)
	}

	return repo, nil
}

// CreateConsumptionRecord inserts a consumption row and optional outbox row in a single transaction.
// Idempotency: uses INSERT OR IGNORE on report_id (PRIMARY KEY). Returns inserted=false if duplicate.
// Only creates an outbox entry when inserted=true and debitMsat >= 1.
func (r *consumptionRepoSQLite) CreateConsumptionRecord(ctx context.Context, reportID, deviceID string, debitMsat int64, fractionalMsat float64, measure float64, pricePerUnitMsat int64, unit, timestamp string, traceContext map[string]string) (inserted bool, err error) {
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
	inserted, err = r.createConsumptionRecordWithTx(ctx, tx, reportID, deviceID, debitMsat, fractionalMsat, measure, pricePerUnitMsat, unit, timestamp, traceContext)
	if err != nil {
		return false, err
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
	return inserted, nil
}

func (r *consumptionRepoSQLite) createConsumptionRecordWithTx(ctx context.Context, tx *sql.Tx, reportID, deviceID string, debitMsat int64, fractionalMsat float64, measure float64, pricePerUnitMsat int64, unit, timestamp string, traceContext map[string]string) (inserted bool, err error) {
	now := time.Now().Unix()

	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "INSERT OR IGNORE"),
		attribute.String("db.table", "consumption_records"),
		attribute.String("report.id", reportID),
		attribute.String("device.id", deviceID),
		attribute.Int64("debit_msat", debitMsat),
		attribute.Float64("fractional_msat", fractionalMsat),
	}

	tcStmt := tx.StmtContext(ctx, r.insertConsumptionStmt)
	defer tcStmt.Close()

	res, err := r.sqlTracer.ExecStmtWithSpan(ctx, "[repository] create consumption record", attrs, tcStmt,
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
		toStmt := tx.StmtContext(ctx, r.insertOutboxStmt)
		defer toStmt.Close()
		_, err := r.sqlTracer.ExecStmtWithSpan(ctx, "[repository] create outbox entry", outboxAttrs, toStmt,
			reportID, traceparent, now,
		)
		if err != nil {
			return false, fmt.Errorf("failed to insert into outbox: %w", err)
		}
	}

	return true, nil
}

// GetUnpublishedOutboxEvents retrieves unpublished events from the outbox
func (r *consumptionRepoSQLite) GetUnpublishedOutboxEvents(ctx context.Context, limit int) ([]OutboxEvent, error) {
	query := `
		SELECT o.report_id, c.device_id, c.debit_msat, c.timestamp, c.created_at, o.traceparent
		FROM consumption_outbox o
		INNER JOIN consumption_records c ON o.report_id = c.report_id
		WHERE o.published = 0
		ORDER BY c.created_at ASC
		LIMIT ?
	`

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

		// Reconstruct trace context map from W3C traceparent
		if traceparent.Valid && traceparent.String != "" {
			e.TraceContext = map[string]string{
				"traceparent": traceparent.String,
			}
		}

		events = append(events, e)
	}

	if err := rows.Err(); err != nil {
		r.sqlTracer.LogSQLError(ctx, "[repository] get unpublished outbox events rows", attrs, err)
		return nil, fmt.Errorf("error iterating outbox events: %w", err)
	}

	return events, nil
}

// MarkOutboxAsPublished marks an outbox entry as published (separate write from the debit insert).
// Ordering is intentional: consumption row is committed before Redis publish; this UPDATE runs after a successful publish.
func (r *consumptionRepoSQLite) MarkOutboxAsPublished(ctx context.Context, reportID string) error {
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "UPDATE"),
		attribute.String("db.table", "consumption_outbox"),
		attribute.String("report.id", reportID),
	}
	if _, err := r.sqlTracer.ExecStmtWithSpan(ctx, "[repository] mark outbox as published", attrs, r.markPublishedStmt, time.Now().Unix(), reportID); err != nil {
		return fmt.Errorf("failed to mark report %s as published: %w", reportID, err)
	}
	return nil
}

// CleanupOutbox removes published records older than the retention period
func (r *consumptionRepoSQLite) CleanupOutbox(ctx context.Context, retentionDays int) (int64, error) {
	retentionSeconds := int64(retentionDays * 24 * 60 * 60)
	cutoffTime := time.Now().Unix() - retentionSeconds

	query := `
		DELETE FROM consumption_outbox
		WHERE published = 1 AND published_at < ?`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "DELETE"),
		attribute.String("db.table", "consumption_outbox"),
		attribute.Int("retention_days", retentionDays),
	}
	result, err := r.sqlTracer.ExecWithSpan(ctx, "[repository] cleanup outbox", attrs, r.db, query, cutoffTime)
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

// Close closes the database connection
func (r *consumptionRepoSQLite) Close() error {
	if r.insertConsumptionStmt != nil {
		_ = r.insertConsumptionStmt.Close()
	}
	if r.insertOutboxStmt != nil {
		_ = r.insertOutboxStmt.Close()
	}
	if r.markPublishedStmt != nil {
		_ = r.markPublishedStmt.Close()
	}
	return r.db.Close()
}

// ListDeviceConsumptions retrieves consumption records for a device with outbox status
func (r *consumptionRepoSQLite) ListDeviceConsumptions(ctx context.Context, deviceID string, limit int) ([]ConsumptionResponse, error) {
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
			COALESCE(o.published, 0) as published,
			o.traceparent
		FROM consumption_records c
		LEFT JOIN consumption_outbox o ON c.report_id = o.report_id
		WHERE c.device_id = ?
		ORDER BY c.created_at DESC
		LIMIT ?
	`

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
			&resp.ReportID,
			&resp.DeviceID,
			&resp.DebitMsat,
			&resp.FractionalMsat,
			&resp.Measure,
			&resp.PricePerUnitMsat,
			&resp.Unit,
			&resp.Timestamp,
			&resp.CreatedAt,
			&published,
			&traceparent,
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
