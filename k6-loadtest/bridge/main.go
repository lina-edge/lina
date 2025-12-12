package main

import (
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/gin-gonic/gin"
)

var mqttBroker = getEnv("MQTT_BROKER", "ssl://localhost:8883")

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// Store active connections
type DeviceSession struct {
	Client    mqtt.Client
	DeviceCtx *DeviceContext
}

var (
	sessions = make(map[string]*DeviceSession)
	sessMux  sync.RWMutex
)

func main() {
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()

	// Use a single wildcard route and dispatch based on the action
	r.POST("/devices/:deviceId/*action", handleDeviceRoute)

	fmt.Printf("MQTT Proxy running on :3000 (broker: %s)\n", mqttBroker)
	log.Fatal(r.Run(":3000"))
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

	opts := mqtt.NewClientOptions()
	opts.AddBroker(mqttBroker)
	opts.SetClientID(deviceID)
	opts.SetUsername(deviceID)
	opts.SetPassword(req.Secret)

	// Configure TLS with certificate verification disabled
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	opts.SetTLSConfig(tlsConfig)

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		c.JSON(500, gin.H{"error": token.Error().Error()})
		return
	}

	// Create device context
	deviceCtx := NewDeviceContext(deviceID, req.Secret, client)

	// Subscribe to topics (this sets up message handlers)
	if err := deviceCtx.SubscribeToTopics(); err != nil {
		client.Disconnect(250)
		c.JSON(500, gin.H{"error": fmt.Sprintf("failed to subscribe: %v", err)})
		return
	}

	// Initialize device (request invoice, wait, request authorization, wait)
	if err := deviceCtx.Initialize(); err != nil {
		client.Disconnect(250)
		c.JSON(500, gin.H{"error": fmt.Sprintf("initialization failed: %v", err)})
		return
	}

	// Store session
	sessMux.Lock()
	sessions[deviceID] = &DeviceSession{
		Client:    client,
		DeviceCtx: deviceCtx,
	}
	sessMux.Unlock()

	// Start background goroutine to maintain authorization
	go maintainAuthorization(deviceCtx)

	c.Status(200)
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

	// Disconnect MQTT client
	session.Client.Disconnect(250)
	log.Printf("[%s] Device disconnected", deviceID)

	c.Status(200)
}

// maintainAuthorization periodically ensures authorization is active
func maintainAuthorization(ctx *DeviceContext) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx.EnsureAuthorizationActive()
		}
	}
}

func handleDevicePublish(c *gin.Context) {
	deviceID := c.Param("deviceId")
	if deviceID == "" {
		c.JSON(400, gin.H{"error": "deviceId is required"})
		return
	}

	// Use the request path directly as the MQTT topic
	topic := c.Request.URL.Path

	// Read request body as payload
	payload, err := c.GetRawData()
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	sessMux.RLock()
	session, exists := sessions[deviceID]
	sessMux.RUnlock()

	if !exists {
		c.JSON(404, gin.H{"error": "Device not connected"})
		return
	}

	// Check if reporting is enabled (for usage reports)
	if session.DeviceCtx != nil {
		// For usage reports, check if reporting is enabled
		if strings.Contains(topic, "/usage") {
			session.DeviceCtx.mu.RLock()
			reportingEnabled := session.DeviceCtx.ReportingEnabled
			session.DeviceCtx.mu.RUnlock()

			if !reportingEnabled {
				c.JSON(423, gin.H{"error": "reporting disabled (STOP/PAUSE command received)"})
				return
			}
		}
		// Ensure authorization is active before publishing usage reports
		session.DeviceCtx.EnsureAuthorizationActive()
	}

	token := session.Client.Publish(topic, 1, false, string(payload))
	token.Wait()

	if token.Error() != nil {
		c.JSON(500, gin.H{"error": token.Error().Error()})
		return
	}

	c.Status(200)
}
