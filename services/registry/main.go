package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	_ "modernc.org/sqlite"
)

// Device represents a registered IoT device
type Device struct {
	ID              string  `json:"id"`
	PublicKey       string  `json:"public_key"`
	Unit            string  `json:"unit"`           // e.g., "sheet", "m3"
	PricePerUnit    float64 `json:"price_per_unit"` // cost in sats per unit
	SecretKey       string  `json:"secret_key"`
	AggregationMode string  `json:"aggregation_mode"` // e.g., "per-unit", "time-window", "value-threshold"
}

// RegistryService manages the registered devices
type RegistryService struct {
	db *sql.DB
}

// NewRegistryService creates and initializes the SQLite database
func NewRegistryService(dbPath string) *RegistryService {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatalf("failed to connect to SQLite: %v", err)
	}

	// Create the devices table with aggregation_mode support
	createTable := `
	CREATE TABLE IF NOT EXISTS devices (
		id TEXT PRIMARY KEY,
		public_key TEXT,
		unit TEXT,
		price_per_unit REAL,
		secret_key TEXT,
		aggregation_mode TEXT DEFAULT 'per-unit'
	);`

	if _, err := db.Exec(createTable); err != nil {
		log.Fatalf("failed to create table: %v", err)
	}

	return &RegistryService{db: db}
}

// RegisterDevice adds or updates a device in the registry
func (s *RegistryService) RegisterDevice(c *gin.Context) {
	var d Device
	if err := c.BindJSON(&d); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid input"})
		return
	}

	if d.ID == "" || d.SecretKey == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id and secret_key required"})
		return
	}

	if d.AggregationMode == "" {
		d.AggregationMode = "per-unit" // default mode
	}

	query := `
		INSERT OR REPLACE INTO devices (id, public_key, unit, price_per_unit, secret_key, aggregation_mode)
		VALUES (?, ?, ?, ?, ?, ?)
	`
	_, err := s.db.Exec(query, d.ID, d.PublicKey, d.Unit, d.PricePerUnit, d.SecretKey, d.AggregationMode)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save device"})
		return
	}

	c.JSON(http.StatusCreated, d)
}

// GetDeviceConfig validates the device HMAC and returns its configuration
func (s *RegistryService) GetDeviceConfig(c *gin.Context) {
	deviceID := c.GetHeader("deviceId")
	timestamp := c.GetHeader("timestamp")
	signature := c.GetHeader("signature")

	if deviceID == "" || timestamp == "" || signature == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing headers"})
		return
	}

	var d Device
	query := `SELECT id, public_key, unit, price_per_unit, secret_key, aggregation_mode FROM devices WHERE id = ? LIMIT 1`
	err := s.db.QueryRow(query, deviceID).Scan(&d.ID, &d.PublicKey, &d.Unit, &d.PricePerUnit, &d.SecretKey, &d.AggregationMode)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "device not found"})
		return
	} else if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
		return
	}

	// Validate HMAC
	message := timestamp + deviceID
	expectedSig := generateHMAC(message, d.SecretKey)
	if signature != expectedSig {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid signature"})
		return
	}

	// Validate timestamp freshness
	tsInt, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || time.Since(time.Unix(tsInt, 0)) > 5*time.Minute {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "timestamp too old or invalid"})
		return
	}

	// Return device configuration (excluding the secret)
	c.JSON(http.StatusOK, gin.H{
		"id":               d.ID,
		"unit":             d.Unit,
		"price_per_unit":   d.PricePerUnit,
		"public_key":       d.PublicKey,
		"aggregation_mode": d.AggregationMode,
	})
}

// GetDeviceConfigInternal returns full device config for internal service calls
func (s *RegistryService) GetDeviceConfigInternal(c *gin.Context) {
	serviceToken := c.GetHeader("X-Service-Token")
	if serviceToken != "dev-token" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid service token"})
		return
	}
	deviceID := c.Query("deviceId")
	if deviceID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing deviceId"})
		return
	}
	var d Device
	query := `SELECT id, public_key, unit, price_per_unit, secret_key, aggregation_mode FROM devices WHERE id = ? LIMIT 1`
	err := s.db.QueryRow(query, deviceID).Scan(&d.ID, &d.PublicKey, &d.Unit, &d.PricePerUnit, &d.SecretKey, &d.AggregationMode)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "device not found"})
		return
	} else if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "db error"})
		return
	}
	c.JSON(http.StatusOK, d)
}

// generateHMAC returns the HMAC-SHA256 of the message using the secret
func generateHMAC(message, secret string) string {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(message))
	return hex.EncodeToString(h.Sum(nil))
}

func main() {
	service := NewRegistryService("devices.db")

	r := gin.Default()
	r.POST("/devices", service.RegisterDevice)
	r.GET("/devices/config", service.GetDeviceConfig)

	r.GET("/internal/devices/config", service.GetDeviceConfigInternal)

	fmt.Println("Registry Service running at http://localhost:8080")
	r.Run(":8080")
}
