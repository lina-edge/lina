package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	mqttpb "github.com/robertodantas/lnpay/proto/gen/model/mqtt"
)

// CreateDeviceRequest represents the request body for creating a device
type CreateDeviceRequest struct {
	DeviceID             string `json:"device_id" binding:"required"`
	DeviceSecret         string `json:"device_secret" binding:"required"`
	Unit                 string `json:"unit" binding:"required"`
	UnitPrice            string `json:"unit_price" binding:"required"`
	PricingUnit          string `json:"pricing_unit" binding:"required"`
	ReportingStrategy    string `json:"reporting_strategy" binding:"required"`
	ReportingInterval    int    `json:"reporting_interval" binding:"required"`
	HeartbeatInterval    int    `json:"heartbeat_interval" binding:"required"`
	AuthorizeRequestMsat int    `json:"authorize_request_msat" binding:"required"`
	Timestamp            string `json:"timestamp" binding:"required"`
}

// NorthboundInterface handles REST API endpoints
type NorthboundInterface struct {
	router     *gin.Engine
	repo       *DeviceRepository
	dynSec     *DynSecService
	mqttClient *MQTTClient
	server     *http.Server
}

// NewNorthboundInterface creates a new northbound interface
func NewNorthboundInterface(repo *DeviceRepository, dynSec *DynSecService, mqttClient *MQTTClient) *NorthboundInterface {
	router := gin.Default()

	// Add OpenTelemetry middleware for automatic route-based span naming
	// This will create spans named like "GET /api/v1/devices" or "POST /api/v1/devices/:id"
	router.Use(otelgin.Middleware("device-service"))

	nb := &NorthboundInterface{
		router:     router,
		repo:       repo,
		dynSec:     dynSec,
		mqttClient: mqttClient,
	}

	// Register routes
	nb.registerRoutes()

	return nb
}

// registerRoutes registers all REST API routes
func (nb *NorthboundInterface) registerRoutes() {
	api := nb.router.Group("/api/v1")
	{
		api.POST("/devices", nb.createDevice)
		api.GET("/devices", nb.listDevices)
		api.GET("/devices/:id", nb.getDevice)
	}
}

// createDevice handles POST /devices
func (nb *NorthboundInterface) createDevice(c *gin.Context) {
	var req CreateDeviceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Validate reporting_strategy
	validStrategies := map[string]bool{
		"interval": true,
		"delta":    true,
		"total":    true,
	}
	if !validStrategies[req.ReportingStrategy] {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "reporting_strategy must be one of: interval, delta, total",
		})
		return
	}

	// Parse timestamp
	timestamp, err := time.Parse(time.RFC3339, req.Timestamp)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid timestamp format, expected RFC3339 (e.g., 2025-11-07T17:40:00Z)",
		})
		return
	}

	// Create device struct (note: we don't store device_secret)
	device := &Device{
		DeviceID:             req.DeviceID,
		Unit:                 req.Unit,
		UnitPrice:            req.UnitPrice,
		PricingUnit:          req.PricingUnit,
		ReportingStrategy:    req.ReportingStrategy,
		ReportingInterval:    req.ReportingInterval,
		HeartbeatInterval:    req.HeartbeatInterval,
		AuthorizeRequestMsat: req.AuthorizeRequestMsat,
		Timestamp:            timestamp,
	}

	// Check if device already exists
	_, err = nb.repo.GetDevice(device.DeviceID)
	deviceExists := err == nil

	if deviceExists {
		// Update existing device in database
		logger.WithDeviceID(device.DeviceID).
			Info("Device already exists, updating via northbound REST")
		if err := nb.repo.UpdateDevice(device); err != nil {
			logger.WithDeviceID(device.DeviceID).
				Error("Failed to update device in database via northbound REST", err)
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "failed to update device",
			})
			return
		}
		logger.WithDeviceID(device.DeviceID).
			Info("Device updated in database via northbound REST")
	} else {
		// Create new device in database
		if err := nb.repo.CreateDevice(device); err != nil {
			logger.WithDeviceID(device.DeviceID).
				Error("Failed to create device in database via northbound REST", err)
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "failed to create device",
			})
			return
		}
		logger.WithDeviceID(device.DeviceID).
			Info("Device created in database via northbound REST")
	}

	// Trigger dynsec provisioning (using device_secret as password)
	logger.WithDeviceID(device.DeviceID).
		Info("Provisioning device in dynsec via northbound REST")
	if err := nb.dynSec.ProvisionDevice(device.DeviceID, req.DeviceSecret); err != nil {
		logger.WithDeviceID(device.DeviceID).
			Warnf("Failed to provision device in dynsec via northbound REST: %v", err)
		// Continue even if provisioning fails - device is already in database
	} else {
		logger.WithDeviceID(device.DeviceID).
			Info("Device provisioned successfully in dynsec via northbound REST")
	}

	// Publish device configuration to /devices/{device_id}/config
	if err := nb.publishDeviceConfig(device); err != nil {
		logger.WithDeviceID(device.DeviceID).
			Warnf("Failed to publish device config on southbound mqtt via northbound REST: %v", err)
		// Continue even if publishing fails - device is already in database and provisioned
	} else {
		logger.WithDeviceID(device.DeviceID).
			InfoWithFields("Device config published on southbound mqtt via northbound REST", map[string]interface{}{
				"topic": fmt.Sprintf("/devices/%s/config", device.DeviceID),
			})
	}

	// Return 200 OK for updates, 201 Created for new devices
	if deviceExists {
		c.JSON(http.StatusOK, device)
	} else {
		c.JSON(http.StatusCreated, device)
	}
}

// listDevices handles GET /devices
func (nb *NorthboundInterface) listDevices(c *gin.Context) {
	devices, err := nb.repo.ListDevices()
	if err != nil {
		logger.Error("Failed to list devices via northbound REST", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "failed to list devices",
		})
		return
	}

	c.JSON(http.StatusOK, devices)
}

// getDevice handles GET /devices/:id
func (nb *NorthboundInterface) getDevice(c *gin.Context) {
	deviceID := c.Param("id")
	device, err := nb.repo.GetDevice(deviceID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error": "device not found",
		})
		return
	}

	c.JSON(http.StatusOK, device)
}

// Start starts the HTTP server
func (nb *NorthboundInterface) Start(addr string) error {
	// OpenTelemetry middleware is already added in NewNorthboundInterface
	// It will automatically name spans as "{HTTP_METHOD} {route_template}"
	// e.g., "GET /api/v1/devices", "POST /api/v1/devices", "GET /api/v1/devices/:id"
	nb.server = &http.Server{
		Addr:    addr,
		Handler: nb.router,
	}

	logger.Infof("Starting northbound REST API server on %s", addr)
	return nb.server.ListenAndServe()
}

// publishDeviceConfig publishes the device configuration to MQTT
func (nb *NorthboundInterface) publishDeviceConfig(device *Device) error {
	// Map reporting_strategy string to enum
	var reportingStrategy mqttpb.ReportingStrategy
	switch device.ReportingStrategy {
	case "interval":
		reportingStrategy = mqttpb.ReportingStrategy_REPORTING_STRATEGY_INTERVAL
	case "delta":
		reportingStrategy = mqttpb.ReportingStrategy_REPORTING_STRATEGY_DELTA
	case "total":
		reportingStrategy = mqttpb.ReportingStrategy_REPORTING_STRATEGY_TOTAL
	default:
		return fmt.Errorf("invalid reporting strategy: %s", device.ReportingStrategy)
	}

	// Create config payload
	config := &mqttpb.ConfigPayload{
		DeviceId:             device.DeviceID,
		Unit:                 device.Unit,
		UnitPrice:            device.UnitPrice,
		PricingUnit:          device.PricingUnit,
		ReportingStrategy:    reportingStrategy,
		ReportingInterval:    int32(device.ReportingInterval),
		HeartbeatInterval:    int32(device.HeartbeatInterval),
		AuthorizeRequestMsat: int64(device.AuthorizeRequestMsat),
		Timestamp:            device.Timestamp.Format(time.RFC3339),
	}

	// Serialize to JSON
	configJSON, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal config payload: %w", err)
	}

	// Publish to /devices/{device_id}/config (retained message)
	configTopic := fmt.Sprintf("/devices/%s/config", device.DeviceID)
	if err := nb.mqttClient.Publish(configTopic, 1, true, configJSON); err != nil {
		return fmt.Errorf("failed to publish config: %w", err)
	}

	return nil
}

// Stop gracefully stops the HTTP server
func (nb *NorthboundInterface) Stop(ctx context.Context) error {
	if nb.server != nil {
		logger.Info("Stopping northbound REST API server")
		return nb.server.Shutdown(ctx)
	}
	return nil
}
