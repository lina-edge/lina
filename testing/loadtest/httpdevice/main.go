package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/robertodantas/lnpay/internal"
	devicepkg "github.com/robertodantas/lnpay/testing/device"
)

var logger = internal.NewLogger("httpdevice")

var config = LoadConfig()

// Store active connections
type DeviceSession struct {
	Device *HTTPDevice
}

var (
	sessions = make(map[string]*DeviceSession)
	sessMux  sync.RWMutex
)

func main() {
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()

	// Register batch routes BEFORE wildcard route (order matters in Gin)
	r.POST("/devices/batch/connect", handleBatchConnect)
	r.POST("/devices/batch/disconnect", handleBatchDisconnect)
	// Use a single wildcard route and dispatch based on the action
	r.POST("/devices/:deviceId/*action", handleDeviceRoute)

	listenAddr := ":" + config.HTTPPort
	logger.Info(context.Background(), fmt.Sprintf("HTTP Device service running on %s (broker: %s)", listenAddr, config.MQTTBroker))
	if err := r.Run(listenAddr); err != nil {
		logger.Fatal(context.Background(), "HTTP server failed", err)
	}
}

// handleDeviceRoute dispatches to the appropriate handler based on the action
func handleDeviceRoute(c *gin.Context) {
	action := c.Param("action")
	// Remove leading slash if present
	if len(action) > 0 && action[0] == '/' {
		action = action[1:]
	}

	switch action {
	case "connect":
		handleConnect(c)
	case "disconnect":
		handleDisconnect(c)
	default:
		handleDevicePublish(c)
	}
}

func handleConnect(c *gin.Context) {
	deviceID := c.Param("deviceId")
	if deviceID == "" {
		c.JSON(400, gin.H{"error": "deviceId is required"})
		return
	}

	var req struct {
		Secret string `json:"secret" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	// Check if device already has an active session
	sessMux.RLock()
	existingSession, exists := sessions[deviceID]
	sessMux.RUnlock()

	if exists && existingSession.Device.IsConnected() {
		// Device is already connected, skip reconnection
		logger.WithDeviceID(deviceID).Info(context.Background(), "Device already connected, skipping reconnection")
		c.Status(200)
		return
	}

	// If session exists but device is not connected, clean it up
	if exists {
		logger.WithDeviceID(deviceID).Info(context.Background(), "Existing session found but not connected, cleaning up")
		sessMux.Lock()
		if existingSession.Device.IsConnected() {
			existingSession.Device.Disconnect()
		}
		delete(sessions, deviceID)
		sessMux.Unlock()
		// Small delay to ensure the old connection is fully closed (reduced from 100ms to 50ms)
		time.Sleep(50 * time.Millisecond)
	}

	// Create HTTP device (DeviceInterface handles MQTT connection)
	device := NewHTTPDevice(deviceID, req.Secret, config)

	// Connect to MQTT broker
	if err := device.Connect(); err != nil {
		c.JSON(500, gin.H{"error": fmt.Sprintf("failed to connect: %v", err)})
		return
	}

	// Wait for connection to be established (optimized: faster polling, shorter timeout)
	timeout := time.After(5 * time.Second)          // Reduced from 10s to 5s
	ticker := time.NewTicker(50 * time.Millisecond) // Reduced from 100ms to 50ms for faster detection
	defer ticker.Stop()
	for {
		select {
		case <-timeout:
			device.Disconnect()
			c.JSON(500, gin.H{"error": "timeout waiting for MQTT connection"})
			return
		case <-ticker.C:
			if device.IsConnected() {
				goto connected
			}
		}
	}
connected:

	// DeviceInterface automatically requests authorization after connection
	// Invoice requests are handled through the normal flow (e.g., OnAuthorizationRejected callback)

	// Store session
	sessMux.Lock()
	sessions[deviceID] = &DeviceSession{
		Device: device,
	}
	sessMux.Unlock()

	c.Status(200)
}

func handleBatchConnect(c *gin.Context) {
	var req struct {
		Devices []struct {
			DeviceID string `json:"deviceId" binding:"required"`
			Secret   string `json:"secret" binding:"required"`
		} `json:"devices" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	if len(req.Devices) == 0 {
		c.JSON(400, gin.H{"error": "at least one device is required"})
		return
	}

	type deviceResult struct {
		DeviceID string `json:"deviceId"`
		Success  bool   `json:"success"`
		Error    string `json:"error,omitempty"`
	}

	results := make([]deviceResult, len(req.Devices))
	var wg sync.WaitGroup

	// Connect all devices in parallel
	for i, device := range req.Devices {
		wg.Add(1)
		go func(idx int, devID, secret string) {
			defer wg.Done()

			// Check if device already has an active session
			sessMux.RLock()
			existingSession, exists := sessions[devID]
			sessMux.RUnlock()

			if exists && existingSession.Device.IsConnected() {
				// Device is already connected, skip reconnection
				logger.WithDeviceID(devID).Info(context.Background(), "Device already connected, skipping reconnection")
				results[idx] = deviceResult{
					DeviceID: devID,
					Success:  true,
				}
				return
			}

			// If session exists but device is not connected, clean it up
			if exists {
				logger.WithDeviceID(devID).Info(context.Background(), "Existing session found but not connected, cleaning up")
				sessMux.Lock()
				if existingSession.Device.IsConnected() {
					existingSession.Device.Disconnect()
				}
				delete(sessions, devID)
				sessMux.Unlock()
				// Small delay to ensure the old connection is fully closed
				time.Sleep(100 * time.Millisecond)
			}

			// Create HTTP device (DeviceInterface handles MQTT connection)
			device := NewHTTPDevice(devID, secret, config)

			// Connect to MQTT broker
			if err := device.Connect(); err != nil {
				results[idx] = deviceResult{
					DeviceID: devID,
					Success:  false,
					Error:    fmt.Sprintf("failed to connect: %v", err),
				}
				return
			}

			// Wait for connection to be established
			timeout := time.After(2 * time.Second)
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
		connectionLoop:
			for {
				select {
				case <-timeout:
					device.Disconnect()
					results[idx] = deviceResult{
						DeviceID: devID,
						Success:  false,
						Error:    "timeout waiting for MQTT connection",
					}
					return
				case <-ticker.C:
					if device.IsConnected() {
						break connectionLoop
					}
				}
			}

			// DeviceInterface automatically requests authorization after connection
			// Invoice requests are handled through the normal flow (e.g., OnAuthorizationRejected callback)

			// Store session
			sessMux.Lock()
			sessions[devID] = &DeviceSession{
				Device: device,
			}
			sessMux.Unlock()

			results[idx] = deviceResult{
				DeviceID: devID,
				Success:  true,
			}
		}(i, device.DeviceID, device.Secret)
	}

	// Wait for all connections to complete
	wg.Wait()

	// Count successes and failures
	successCount := 0
	for _, result := range results {
		if result.Success {
			successCount++
		}
	}

	c.JSON(200, gin.H{
		"connected": successCount,
		"failed":    len(req.Devices) - successCount,
		"total":     len(req.Devices),
		"results":   results,
	})
}

func handleBatchDisconnect(c *gin.Context) {
	var req struct {
		DeviceIDs []string `json:"deviceIds" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	if len(req.DeviceIDs) == 0 {
		c.JSON(400, gin.H{"error": "at least one device ID is required"})
		return
	}

	type deviceResult struct {
		DeviceID string `json:"deviceId"`
		Success  bool   `json:"success"`
		Error    string `json:"error,omitempty"`
	}

	results := make([]deviceResult, len(req.DeviceIDs))
	var wg sync.WaitGroup

	// Disconnect all devices in parallel
	for i, deviceID := range req.DeviceIDs {
		wg.Add(1)
		go func(idx int, devID string) {
			defer wg.Done()

			sessMux.Lock()
			session, exists := sessions[devID]
			if exists {
				delete(sessions, devID)
			}
			sessMux.Unlock()

			if !exists {
				// Device wasn't connected, count as success (idempotent)
				results[idx] = deviceResult{
					DeviceID: devID,
					Success:  true,
				}
				return
			}

			// Disconnect device
			session.Device.Disconnect()
			logger.WithDeviceID(devID).Info(context.Background(), "Device disconnected")

			results[idx] = deviceResult{
				DeviceID: devID,
				Success:  true,
			}
		}(i, deviceID)
	}

	// Wait for all disconnections to complete
	wg.Wait()

	// Count successes and failures
	successCount := 0
	for _, result := range results {
		if result.Success {
			successCount++
		}
	}

	c.JSON(200, gin.H{
		"disconnected": successCount,
		"failed":       len(req.DeviceIDs) - successCount,
		"total":        len(req.DeviceIDs),
		"results":      results,
	})
}

func handleDisconnect(c *gin.Context) {
	deviceID := c.Param("deviceId")
	if deviceID == "" {
		c.JSON(400, gin.H{"error": "deviceId is required"})
		return
	}

	sessMux.Lock()
	session, exists := sessions[deviceID]
	if exists {
		delete(sessions, deviceID)
	}
	sessMux.Unlock()

	if !exists {
		c.JSON(404, gin.H{"error": "Device not connected"})
		return
	}

	// Disconnect device
	session.Device.Disconnect()
	logger.WithDeviceID(deviceID).Info(context.Background(), "Device disconnected")

	c.Status(200)
}

func handleDevicePublish(c *gin.Context) {
	deviceID := c.Param("deviceId")
	if deviceID == "" {
		c.JSON(400, gin.H{"error": "deviceId is required"})
		return
	}

	// Use the request path directly as the MQTT topic
	topic := c.Request.URL.Path

	sessMux.RLock()
	session, exists := sessions[deviceID]
	sessMux.RUnlock()

	if !exists {
		c.JSON(404, gin.H{"error": "Device not connected"})
		return
	}

	// Check if reporting is enabled (for usage reports)
	if session.Device != nil {
		// For usage reports, check if reporting is enabled
		if strings.Contains(topic, "/usage") {
			if !session.Device.IsReportingEnabled() {
				c.JSON(423, gin.H{"error": "reporting disabled (STOP/PAUSE command received)"})
				return
			}
		}
		// DeviceInterface handles authorization automatically
	}

	// Check if device is connected before publishing
	if !session.Device.IsConnected() {
		c.JSON(500, gin.H{"error": "Device not connected"})
		return
	}

	// Get the DeviceInterface
	deviceInterface := session.Device.GetDeviceInterface()
	if deviceInterface == nil || !deviceInterface.IsConnected() {
		c.JSON(500, gin.H{"error": "Device interface not available"})
		return
	}

	// For usage reports, parse JSON and use PublishUsageReport
	if strings.Contains(topic, "/usage") {
		// Check if there's an active authorization
		if !deviceInterface.HasActiveAuthorization() {
			logger.WithDeviceID(deviceID).Debug(context.Background(), "Usage report rejected: no active authorization")
			c.JSON(423, gin.H{"error": "no active authorization - cannot publish usage report"})
			return
		}

		// Check if balance is available and >= 0
		deviceState := deviceInterface.GetDeviceContext()
		if deviceState.Balance == nil {
			logger.WithDeviceID(deviceID).Debug(context.Background(), "Usage report rejected: balance not available")
			c.JSON(423, gin.H{"error": "balance not available - cannot publish usage report"})
			return
		}
		if deviceState.Balance.AvailableMsat < 0 {
			logger.WithDeviceID(deviceID).Debugf(context.Background(), "Usage report rejected: insufficient balance (%d msat)", deviceState.Balance.AvailableMsat)
			c.JSON(423, gin.H{"error": fmt.Sprintf("insufficient balance (%d msat) - cannot publish usage report", deviceState.Balance.AvailableMsat)})
			deviceInterface.PublishInvoiceRequest(devicepkg.GenerateID(), 0, "INSUFFICIENT_BALANCE")
			return
		}

		// Read request body as JSON
		var usagePayload struct {
			ReportID string  `json:"reportId"`
			Measure  float64 `json:"measure"`
		}
		if err := c.ShouldBindJSON(&usagePayload); err != nil {
			c.JSON(400, gin.H{"error": fmt.Sprintf("invalid JSON payload: %v", err)})
			return
		}

		// Use PublishUsageReport method
		deviceInterface.PublishUsageReport(usagePayload.ReportID, usagePayload.Measure)
		c.Status(200)
		return
	}

	// For other topics, use generic Publish method
	payload, err := c.GetRawData()
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	// Publish using DeviceInterface
	if err := deviceInterface.Publish(topic, 1, false, payload); err != nil {
		c.JSON(500, gin.H{"error": fmt.Sprintf("failed to publish: %v", err)})
		return
	}

	c.Status(200)
}
