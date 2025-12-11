package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/robertodantas/lnpay/internal"
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

	logger.WithDeviceID(device.DeviceID).
		Info(ctx, "Device created in database")
	return nil
}

// GetDevice retrieves a device by ID
func (r *DeviceRepository) GetDevice(ctx context.Context, deviceID string) (*Device, error) {
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
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("device not found: %s", device.DeviceID)
	}

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
		return nil, fmt.Errorf("error iterating devices: %w", err)
	}

	return devices, nil
}

// Close closes the database connection
func (r *DeviceRepository) Close() error {
	return r.db.Close()
}
