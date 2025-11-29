package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// Device represents a registered IoT device
type Device struct {
	DeviceID             string    `json:"device_id"`
	MeasurementUnit      string    `json:"measurement_unit"`       // e.g., "kWh"
	UnitPriceMsat        int64     `json:"unit_price_msat"`         // price per unit in millisatoshis
	ReportingStrategy    string    `json:"reporting_strategy"`     // "interval" | "delta" | "total"
	ReportingInterval    int       `json:"reporting_interval"`     // seconds between reports
	HeartbeatInterval    int       `json:"heartbeat_interval"`     // expected heartbeat frequency (s)
	AuthorizeRequestMsat int       `json:"authorize_request_msat"` // expected amount in each authorization request
	Timestamp            time.Time `json:"timestamp"`              // when device was created
}

// DeviceRepository manages database operations for devices
type DeviceRepository struct {
	db *sql.DB
}

// NewDeviceRepository creates and initializes the SQLite database
func NewDeviceRepository(ctx context.Context, dbPath string) (*DeviceRepository, error) {
	db, err := sql.Open("sqlite", dbPath)
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

	if _, err := db.Exec(createTable); err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	return &DeviceRepository{db: db}, nil
}

// CreateDevice inserts a new device into the database
func (r *DeviceRepository) CreateDevice(ctx context.Context, device *Device) error {
	query := `
	INSERT INTO devices (
		device_id, measurement_unit, unit_price_msat, reporting_strategy,
		reporting_interval, heartbeat_interval, authorize_request_msat, timestamp
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := r.db.Exec(
		query,
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

	var device Device
	var timestampStr string

	err := r.db.QueryRow(query, deviceID).Scan(
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

	result, err := r.db.Exec(
		query,
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

	rows, err := r.db.Query(query)
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
