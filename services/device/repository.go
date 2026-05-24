package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/robertodantas/lina/internal"
	"go.opentelemetry.io/otel/attribute"
	_ "modernc.org/sqlite"
)

// Device represents a registered IoT device
type Device struct {
	DeviceID             string    `json:"device_id"`
	MeasurementUnit      string    `json:"measurement_unit"`       // e.g., "kWh"
	UnitPriceMsat        int64     `json:"unit_price_msat"`        // price per unit in millisatoshis
	ReportingStrategy    string    `json:"reporting_strategy"`     // "interval" | "delta" | "total"
	ReportingInterval    int       `json:"reporting_interval"`     // seconds between reports
	HeartbeatInterval    int       `json:"heartbeat_interval"`     // expected heartbeat frequency (s)
	AuthorizeRequestMsat int       `json:"authorize_request_msat"` // expected amount in each authorization request
	Timestamp            time.Time `json:"timestamp"`              // when device was created
}

// DeviceRepository manages database operations for devices
type DeviceRepository struct {
	db        *sql.DB
	sqlTracer *internal.SQLTracer
	cache     sync.Map // map[deviceID string]*Device — read-through cache for the hot MQTT path
}

// NewDeviceRepository creates and initializes the SQLite database
func NewDeviceRepository(ctx context.Context, dbPath string, busyTimeoutMS int) (*DeviceRepository, error) {
	// WAL + busy_timeout for concurrent writers on edge devices.
	dsn := fmt.Sprintf("%s?_pragma=busy_timeout(%d)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)", dbPath, busyTimeoutMS)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to SQLite: %w", err)
	}

	// Create the devices table with new schema
	createTable := `
	CREATE TABLE IF NOT EXISTS devices (
		device_id TEXT PRIMARY KEY,
		measurement_unit TEXT NOT NULL,
		unit_price_msat INTEGER NOT NULL,
		reporting_strategy TEXT NOT NULL,
		reporting_interval INTEGER NOT NULL,
		heartbeat_interval INTEGER NOT NULL,
		authorize_request_msat INTEGER NOT NULL,
		timestamp TEXT NOT NULL
	);`

	repo := &DeviceRepository{
		db:        db,
		sqlTracer: internal.NewSQLTracer("repository.device"),
	}
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "CREATE TABLE"),
		attribute.String("db.table", "devices"),
	}
	if _, err := repo.sqlTracer.ExecWithSpan(ctx, "[repository] create table", attrs, db, createTable); err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	// Add device_secret_hash column if it doesn't exist yet (idempotent migration).
	if _, err := db.ExecContext(ctx, `ALTER TABLE devices ADD COLUMN device_secret_hash TEXT NOT NULL DEFAULT ''`); err != nil {
		if !strings.Contains(err.Error(), "duplicate column name") {
			repo.sqlTracer.LogSQLError(ctx, "[repository] add device secret column", []attribute.KeyValue{
				attribute.String("db.operation", "ALTER TABLE"),
				attribute.String("db.table", "devices"),
			}, err)
			return nil, fmt.Errorf("failed to add device_secret_hash column: %w", err)
		}
	}

	return repo, nil
}

// CreateDevice inserts a new device into the database
func (r *DeviceRepository) CreateDevice(ctx context.Context, device *Device) error {
	query := `
	INSERT INTO devices (
		device_id, measurement_unit, unit_price_msat, reporting_strategy,
		reporting_interval, heartbeat_interval, authorize_request_msat, timestamp
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`

	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "INSERT"),
		attribute.String("db.table", "devices"),
		attribute.String("device.id", device.DeviceID),
	}
	if _, err := r.sqlTracer.ExecWithSpan(ctx, "[repository] create device", attrs, r.db, query,
		device.DeviceID,
		device.MeasurementUnit,
		device.UnitPriceMsat,
		device.ReportingStrategy,
		device.ReportingInterval,
		device.HeartbeatInterval,
		device.AuthorizeRequestMsat,
		device.Timestamp.Format(time.RFC3339),
	); err != nil {
		return fmt.Errorf("failed to insert device: %w", err)
	}

	r.cache.Store(device.DeviceID, device)
	logger.WithDeviceID(device.DeviceID).
		Info(ctx, "Device created in database")
	return nil
}

// GetDevice retrieves a device by ID.
// Checks an in-memory cache first to avoid a DB round-trip on the high-frequency MQTT usage path.
func (r *DeviceRepository) GetDevice(ctx context.Context, deviceID string) (*Device, error) {
	if v, ok := r.cache.Load(deviceID); ok {
		return v.(*Device), nil
	}

	query := `
	SELECT device_id, measurement_unit, unit_price_msat, reporting_strategy,
	       reporting_interval, heartbeat_interval, authorize_request_msat, timestamp
	FROM devices
	WHERE device_id = ?`

	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "devices"),
		attribute.String("device.id", deviceID),
	}
	row := r.sqlTracer.QueryRowWithSpan(ctx, "[repository] get device", attrs, r.db, query, deviceID)

	var device Device
	var timestampStr string
	err := row.Scan(
		&device.DeviceID,
		&device.MeasurementUnit,
		&device.UnitPriceMsat,
		&device.ReportingStrategy,
		&device.ReportingInterval,
		&device.HeartbeatInterval,
		&device.AuthorizeRequestMsat,
		&timestampStr,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("device not found: %s", deviceID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query device: %w", err)
	}

	device.Timestamp, err = time.Parse(time.RFC3339, timestampStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	r.cache.Store(deviceID, &device)
	return &device, nil
}

// UpdateDevice updates an existing device in the database
func (r *DeviceRepository) UpdateDevice(ctx context.Context, device *Device) error {
	query := `
	UPDATE devices SET
		measurement_unit = ?,
		unit_price_msat = ?,
		reporting_strategy = ?,
		reporting_interval = ?,
		heartbeat_interval = ?,
		authorize_request_msat = ?,
		timestamp = ?
	WHERE device_id = ?`

	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "UPDATE"),
		attribute.String("db.table", "devices"),
		attribute.String("device.id", device.DeviceID),
	}
	result, err := r.sqlTracer.ExecWithSpan(ctx, "[repository] update device", attrs, r.db, query,
		device.MeasurementUnit,
		device.UnitPriceMsat,
		device.ReportingStrategy,
		device.ReportingInterval,
		device.HeartbeatInterval,
		device.AuthorizeRequestMsat,
		device.Timestamp.Format(time.RFC3339),
		device.DeviceID,
	)
	if err != nil {
		return fmt.Errorf("failed to update device: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		r.sqlTracer.LogSQLError(ctx, "[repository] update device rows affected", []attribute.KeyValue{
			attribute.String("db.operation", "ROWS AFFECTED"),
			attribute.String("db.table", "devices"),
			attribute.String("device.id", device.DeviceID),
		}, err)
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("device not found: %s", device.DeviceID)
	}

	r.cache.Store(device.DeviceID, device)
	logger.WithDeviceID(device.DeviceID).
		Info(ctx, "Device updated in database")
	return nil
}

// ListDevices retrieves all devices
func (r *DeviceRepository) ListDevices(ctx context.Context) ([]*Device, error) {
	query := `
	SELECT device_id, measurement_unit, unit_price_msat, reporting_strategy,
	       reporting_interval, heartbeat_interval, authorize_request_msat, timestamp
	FROM devices
	ORDER BY timestamp DESC`

	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "devices"),
	}
	rows, err := r.sqlTracer.QueryWithSpan(ctx, "[repository] list devices", attrs, r.db, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query devices: %w", err)
	}
	defer rows.Close()

	var devices []*Device
	for rows.Next() {
		var device Device
		var timestampStr string

		err := rows.Scan(
			&device.DeviceID,
			&device.MeasurementUnit,
			&device.UnitPriceMsat,
			&device.ReportingStrategy,
			&device.ReportingInterval,
			&device.HeartbeatInterval,
			&device.AuthorizeRequestMsat,
			&timestampStr,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan device: %w", err)
		}

		device.Timestamp, err = time.Parse(time.RFC3339, timestampStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse timestamp: %w", err)
		}

		devices = append(devices, &device)
	}

	if err := rows.Err(); err != nil {
		r.sqlTracer.LogSQLError(ctx, "[repository] list devices rows", attrs, err)
		return nil, fmt.Errorf("error iterating devices: %w", err)
	}

	return devices, nil
}

// ListDevicesPage retrieves a page of devices with limit and offset for pagination
func (r *DeviceRepository) ListDevicesPage(ctx context.Context, limit, offset int) ([]*Device, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be > 0")
	}
	if offset < 0 {
		return nil, fmt.Errorf("offset must be >= 0")
	}

	query := `
	SELECT device_id, measurement_unit, unit_price_msat, reporting_strategy,
	       reporting_interval, heartbeat_interval, authorize_request_msat, timestamp
	FROM devices
	ORDER BY timestamp DESC
	LIMIT ? OFFSET ?`

	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "devices"),
		attribute.Int("db.limit", limit),
		attribute.Int("db.offset", offset),
	}
	rows, err := r.sqlTracer.QueryWithSpan(ctx, "[repository] list devices page", attrs, r.db, query, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to query devices page: %w", err)
	}
	defer rows.Close()

	var devices []*Device
	for rows.Next() {
		var device Device
		var timestampStr string

		err := rows.Scan(
			&device.DeviceID,
			&device.MeasurementUnit,
			&device.UnitPriceMsat,
			&device.ReportingStrategy,
			&device.ReportingInterval,
			&device.HeartbeatInterval,
			&device.AuthorizeRequestMsat,
			&timestampStr,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan device: %w", err)
		}

		device.Timestamp, err = time.Parse(time.RFC3339, timestampStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse timestamp: %w", err)
		}

		devices = append(devices, &device)
	}

	if err := rows.Err(); err != nil {
		r.sqlTracer.LogSQLError(ctx, "[repository] list devices page rows", attrs, err)
		return nil, fmt.Errorf("error iterating devices page: %w", err)
	}

	return devices, nil
}

// BatchExists checks if all devices in the specified range with the given pattern already exist
func (r *DeviceRepository) BatchExists(ctx context.Context, idStart, idEnd, idPadding int, deviceIDPattern string) (bool, error) {
	if idStart < 0 || idEnd < idStart {
		return false, fmt.Errorf("invalid range: idStart=%d, idEnd=%d", idStart, idEnd)
	}

	totalDevices := idEnd - idStart + 1
	if totalDevices == 0 {
		return true, nil // Empty range is considered to exist
	}

	// Generate all device IDs in the range
	deviceIDs := make([]string, 0, totalDevices)
	for id := idStart; id <= idEnd; id++ {
		idStr := fmt.Sprintf("%0*d", idPadding, id)
		deviceID := strings.ReplaceAll(deviceIDPattern, "{id}", idStr)
		deviceIDs = append(deviceIDs, deviceID)
	}

	// Check if all devices exist using a single query with IN clause
	// Build placeholders for the IN clause
	placeholders := make([]string, len(deviceIDs))
	args := make([]interface{}, len(deviceIDs))
	for i, deviceID := range deviceIDs {
		placeholders[i] = "?"
		args[i] = deviceID
	}

	query := fmt.Sprintf(`
		SELECT COUNT(*) 
		FROM devices 
		WHERE device_id IN (%s)`,
		strings.Join(placeholders, ","))

	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "devices"),
		attribute.Int("batch.size", totalDevices),
	}
	var count int
	err := r.sqlTracer.QueryRowWithSpan(ctx, "[repository] check batch exists", attrs, r.db, query, args...).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check batch existence: %w", err)
	}

	// Batch exists if all devices are found
	exists := count == totalDevices
	return exists, nil
}

// sqliteMaxQueryParams keeps IN (?) lists under SQLite's default bind variable limit (999).
const sqliteMaxQueryParams = 500

// ListDevicesByIDs returns devices for the given IDs. Missing IDs are omitted (no error).
func (r *DeviceRepository) ListDevicesByIDs(ctx context.Context, deviceIDs []string) ([]*Device, error) {
	if len(deviceIDs) == 0 {
		return nil, nil
	}

	var out []*Device
	for i := 0; i < len(deviceIDs); i += sqliteMaxQueryParams {
		end := i + sqliteMaxQueryParams
		if end > len(deviceIDs) {
			end = len(deviceIDs)
		}
		chunk := deviceIDs[i:end]
		devices, err := r.listDevicesByIDsChunk(ctx, chunk)
		if err != nil {
			return nil, err
		}
		out = append(out, devices...)
	}
	return out, nil
}

func (r *DeviceRepository) listDevicesByIDsChunk(ctx context.Context, deviceIDs []string) ([]*Device, error) {
	placeholders := make([]string, len(deviceIDs))
	args := make([]interface{}, len(deviceIDs))
	for i, id := range deviceIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`
	SELECT device_id, measurement_unit, unit_price_msat, reporting_strategy,
	       reporting_interval, heartbeat_interval, authorize_request_msat, timestamp
	FROM devices
	WHERE device_id IN (%s)`, strings.Join(placeholders, ","))

	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "devices"),
		attribute.Int("batch.size", len(deviceIDs)),
	}
	rows, err := r.sqlTracer.QueryWithSpan(ctx, "[repository] list devices by ids", attrs, r.db, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query devices by ids: %w", err)
	}
	defer rows.Close()

	var devices []*Device
	for rows.Next() {
		var device Device
		var timestampStr string

		err := rows.Scan(
			&device.DeviceID,
			&device.MeasurementUnit,
			&device.UnitPriceMsat,
			&device.ReportingStrategy,
			&device.ReportingInterval,
			&device.HeartbeatInterval,
			&device.AuthorizeRequestMsat,
			&timestampStr,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan device: %w", err)
		}

		device.Timestamp, err = time.Parse(time.RFC3339, timestampStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse timestamp: %w", err)
		}

		devices = append(devices, &device)
	}

	if err := rows.Err(); err != nil {
		r.sqlTracer.LogSQLError(ctx, "[repository] list devices by ids rows", attrs, err)
		return nil, fmt.Errorf("error iterating devices: %w", err)
	}

	return devices, nil
}

// CreateDevicesBatch inserts multiple devices in a single transaction
// Uses INSERT OR IGNORE to skip devices that already exist
func (r *DeviceRepository) CreateDevicesBatch(ctx context.Context, devices []*Device) error {
	if len(devices) == 0 {
		return nil
	}

	// Start transaction
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		r.sqlTracer.LogSQLError(ctx, "[repository] begin create devices batch", []attribute.KeyValue{
			attribute.String("db.operation", "BEGIN"),
			attribute.String("db.table", "devices"),
			attribute.Int("batch.size", len(devices)),
		}, err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Prepare statement for batch insert with OR IGNORE to skip existing devices
	query := `
	INSERT OR IGNORE INTO devices (
		device_id, measurement_unit, unit_price_msat, reporting_strategy,
		reporting_interval, heartbeat_interval, authorize_request_msat, timestamp
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		r.sqlTracer.LogSQLError(ctx, "[repository] prepare create devices batch", []attribute.KeyValue{
			attribute.String("db.operation", "PREPARE"),
			attribute.String("db.table", "devices"),
			attribute.Int("batch.size", len(devices)),
		}, err)
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// Insert all devices (existing ones will be ignored)
	insertedCount := 0
	for _, device := range devices {
		result, err := stmt.ExecContext(ctx,
			device.DeviceID,
			device.MeasurementUnit,
			device.UnitPriceMsat,
			device.ReportingStrategy,
			device.ReportingInterval,
			device.HeartbeatInterval,
			device.AuthorizeRequestMsat,
			device.Timestamp.Format(time.RFC3339),
		)
		if err != nil {
			// Log error but continue with other devices
			r.sqlTracer.LogSQLError(ctx, "[repository] create device in batch", []attribute.KeyValue{
				attribute.String("db.operation", "INSERT OR IGNORE"),
				attribute.String("db.table", "devices"),
				attribute.String("device.id", device.DeviceID),
			}, err)
			continue
		}

		// Check if row was actually inserted (rowsAffected > 0)
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			r.sqlTracer.LogSQLError(ctx, "[repository] batch create device rows affected", []attribute.KeyValue{
				attribute.String("db.operation", "ROWS AFFECTED"),
				attribute.String("db.table", "devices"),
				attribute.String("device.id", device.DeviceID),
			}, err)
		}
		if err == nil && rowsAffected > 0 {
			insertedCount++
		}
		r.cache.Store(device.DeviceID, device)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		r.sqlTracer.LogSQLError(ctx, "[repository] commit create devices batch", []attribute.KeyValue{
			attribute.String("db.operation", "COMMIT"),
			attribute.String("db.table", "devices"),
			attribute.Int("batch.size", len(devices)),
		}, err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	skippedCount := len(devices) - insertedCount
	if skippedCount > 0 {
		logger.Debugf(ctx, "Batch processed %d devices: %d inserted, %d skipped (already exist)", len(devices), insertedCount, skippedCount)
	} else {
		logger.Debugf(ctx, "Batch created %d devices in database", insertedCount)
	}
	return nil
}

// StoreDeviceSecret persists a bcrypt-hashed MQTT password for the given device.
// Called after CreateDevice / CreateDevicesBatch so the auth server can verify credentials.
func (r *DeviceRepository) StoreDeviceSecret(ctx context.Context, deviceID, passwordHash string) error {
	query := `UPDATE devices SET device_secret_hash = ? WHERE device_id = ?`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "UPDATE"),
		attribute.String("db.table", "devices"),
		attribute.String("device.id", deviceID),
	}
	_, err := r.sqlTracer.ExecWithSpan(ctx, "[repository] store device secret", attrs, r.db, query, passwordHash, deviceID)
	return err
}

// GetDeviceSecretHash returns the stored bcrypt hash for a device.
// Returns an error if the device is not found.
func (r *DeviceRepository) GetDeviceSecretHash(ctx context.Context, deviceID string) (string, error) {
	query := `SELECT device_secret_hash FROM devices WHERE device_id = ?`
	attrs := []attribute.KeyValue{
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "devices"),
		attribute.String("device.id", deviceID),
	}
	var hash string
	err := r.sqlTracer.QueryRowWithSpan(ctx, "[repository] get device secret hash", attrs, r.db, query, deviceID).Scan(&hash)
	if err == sql.ErrNoRows {
		return "", fmt.Errorf("device not found: %s", deviceID)
	}
	return hash, err
}

// Close closes the database connection
func (r *DeviceRepository) Close() error {
	return r.db.Close()
}
