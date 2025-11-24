package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "modernc.org/sqlite"
)

// Device represents a registered IoT device
type Device struct {
	DeviceID            string    `json:"device_id"`
	Unit                string    `json:"unit"`                 // e.g., "kWh"
	UnitPrice           string    `json:"unit_price"`           // price as string
	PricingUnit         string    `json:"pricing_unit"`         // e.g., "msat"
	ReportingStrategy   string    `json:"reporting_strategy"`    // "interval" | "delta" | "total"
	ReportingInterval   int       `json:"reporting_interval"`    // seconds between reports
	HeartbeatInterval   int       `json:"heartbeat_interval"`    // expected heartbeat frequency (s)
	AuthorizeRequestMsat int      `json:"authorize_request_msat"` // expected amount in each authorization request
	Timestamp           time.Time `json:"timestamp"`            // when device was created
}

// DeviceRepository manages database operations for devices
type DeviceRepository struct {
	db *sql.DB
}

// NewDeviceRepository creates and initializes the SQLite database
func NewDeviceRepository(dbPath string) (*DeviceRepository, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to SQLite: %w", err)
	}

	// Create the devices table with new schema
	createTable := `
	CREATE TABLE IF NOT EXISTS devices (
		device_id TEXT PRIMARY KEY,
		unit TEXT NOT NULL,
		unit_price TEXT NOT NULL,
		pricing_unit TEXT NOT NULL,
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
func (r *DeviceRepository) CreateDevice(device *Device) error {
	query := `
	INSERT INTO devices (
		device_id, unit, unit_price, pricing_unit, reporting_strategy,
		reporting_interval, heartbeat_interval, authorize_request_msat, timestamp
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := r.db.Exec(
		query,
		device.DeviceID,
		device.Unit,
		device.UnitPrice,
		device.PricingUnit,
		device.ReportingStrategy,
		device.ReportingInterval,
		device.HeartbeatInterval,
		device.AuthorizeRequestMsat,
		device.Timestamp.Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("failed to insert device: %w", err)
	}

	log.Printf("Device created in database: %s", device.DeviceID)
	return nil
}

// GetDevice retrieves a device by ID
func (r *DeviceRepository) GetDevice(deviceID string) (*Device, error) {
	query := `
	SELECT device_id, unit, unit_price, pricing_unit, reporting_strategy,
	       reporting_interval, heartbeat_interval, authorize_request_msat, timestamp
	FROM devices
	WHERE device_id = ?`

	var device Device
	var timestampStr string

	err := r.db.QueryRow(query, deviceID).Scan(
		&device.DeviceID,
		&device.Unit,
		&device.UnitPrice,
		&device.PricingUnit,
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
func (r *DeviceRepository) UpdateDevice(device *Device) error {
	query := `
	UPDATE devices SET
		unit = ?,
		unit_price = ?,
		pricing_unit = ?,
		reporting_strategy = ?,
		reporting_interval = ?,
		heartbeat_interval = ?,
		authorize_request_msat = ?,
		timestamp = ?
	WHERE device_id = ?`

	result, err := r.db.Exec(
		query,
		device.Unit,
		device.UnitPrice,
		device.PricingUnit,
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

	log.Printf("Device updated in database: %s", device.DeviceID)
	return nil
}

// ListDevices retrieves all devices
func (r *DeviceRepository) ListDevices() ([]*Device, error) {
	query := `
	SELECT device_id, unit, unit_price, pricing_unit, reporting_strategy,
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
			&device.Unit,
			&device.UnitPrice,
			&device.PricingUnit,
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

