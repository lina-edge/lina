package device

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/robertodantas/lnpay/internal"
	mqttmodel "github.com/robertodantas/lnpay/services/proto/gen/model/mqtt"
)

var (
	errConnectionTimeout = errors.New("connection timeout")
	logger               = internal.NewLogger("device")
)

// deviceContext contains common device state shared across device types
// It is private to DeviceInterface and should only be accessed through DeviceInterface methods
type deviceContext struct {
	mu                   sync.RWMutex
	DeviceID             string           `json:"deviceId"`
	DeviceStatus         string           `json:"deviceStatus"`
	Balance              *BalanceMessage  `json:"balance"`
	Config               *DeviceConfig    `json:"config"`
	Invoice              *InvoiceResponse `json:"invoice"`
	CurrentAuthorization *Authorization   `json:"currentAuthorization"`
	MQTTStatus           string           `json:"mqttStatus"`
}

// getDeviceID returns the device ID (thread-safe)
func (ctx *deviceContext) getDeviceID() string {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.DeviceID
}

// getDeviceStatus returns the device status (thread-safe)
func (ctx *deviceContext) getDeviceStatus() string {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.DeviceStatus
}

// getConfig returns the device config (thread-safe)
func (ctx *deviceContext) getConfig() *DeviceConfig {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.Config
}

// getBalance returns the balance (thread-safe)
func (ctx *deviceContext) getBalance() *BalanceMessage {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.Balance
}

// getInvoice returns the invoice (thread-safe)
func (ctx *deviceContext) getInvoice() *InvoiceResponse {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.Invoice
}

// getAuthorization returns the current authorization (thread-safe)
func (ctx *deviceContext) getAuthorization() *Authorization {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.CurrentAuthorization
}

// getMQTTStatus returns the MQTT status (thread-safe)
func (ctx *deviceContext) getMQTTStatus() string {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.MQTTStatus
}

// DeviceInterface handles MQTT communication for devices
// It uses callbacks to delegate device-specific behavior
// It manages the deviceContext internally
type DeviceInterface struct {
	callbacks            DeviceCallback
	mqttClient           mqtt.Client
	subscriptionsReady   chan bool
	cfg                  *Config
	ctx                  *deviceContext
	deviceID             string
	heartbeatTicker      *time.Ticker
	heartbeatEnabled     bool
	heartbeatMu          sync.Mutex
	pendingAuthorization bool
	pendingAuthMu        sync.Mutex
}

// NewDeviceInterface creates a new device interface
func NewDeviceInterface(callbacks DeviceCallback, cfg *Config, deviceID string) *DeviceInterface {
	return &DeviceInterface{
		callbacks:          callbacks,
		subscriptionsReady: make(chan bool, 1),
		cfg:                cfg,
		deviceID:           deviceID,
		heartbeatEnabled:   true, // Default to enabled
		ctx: &deviceContext{
			DeviceID:     deviceID,
			DeviceStatus: "OFFLINE",
			MQTTStatus:   "disconnected",
		},
	}
}

// SetHeartbeatEnabled enables or disables automatic heartbeat publishing
func (di *DeviceInterface) SetHeartbeatEnabled(enabled bool) {
	di.heartbeatMu.Lock()
	defer di.heartbeatMu.Unlock()
	di.heartbeatEnabled = enabled
	if !enabled {
		// Stop existing ticker if any
		if di.heartbeatTicker != nil {
			di.heartbeatTicker.Stop()
			di.heartbeatTicker = nil
		}
	} else if di.IsConnected() {
		// If connected and enabling, start heartbeat
		di.startHeartbeatLocked()
	}
}

// GetDeviceContext returns a copy of the device context for reading
func (di *DeviceInterface) GetDeviceContext() DeviceState {
	di.ctx.mu.RLock()
	defer di.ctx.mu.RUnlock()
	return DeviceState{
		DeviceID:             di.ctx.DeviceID,
		DeviceStatus:         di.ctx.DeviceStatus,
		Balance:              di.ctx.Balance,
		Config:               di.ctx.Config,
		Invoice:              di.ctx.Invoice,
		CurrentAuthorization: di.ctx.CurrentAuthorization,
		MQTTStatus:           di.ctx.MQTTStatus,
	}
}

// GetDeviceStatus returns the current device status
func (di *DeviceInterface) GetDeviceStatus() string {
	return di.ctx.getDeviceStatus()
}

// GetDeviceConfig returns the current device configuration
func (di *DeviceInterface) GetDeviceConfig() *DeviceConfig {
	return di.ctx.getConfig()
}

// GetBalance returns the current balance
func (di *DeviceInterface) GetBalance() *BalanceMessage {
	return di.ctx.getBalance()
}

// GetAuthorization returns the current authorization
func (di *DeviceInterface) GetAuthorization() *Authorization {
	return di.ctx.getAuthorization()
}

// createTLSConfig creates TLS configuration for MQTT connection
func (di *DeviceInterface) createTLSConfig() (*tls.Config, error) {
	caFile := di.cfg.MQTTTLSCACert
	skipVerify := di.cfg.MQTTTLSSkipVerify
	serverName := di.cfg.MQTTTLSServerName

	// Load CA cert
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, err
	}

	tlsConfig := &tls.Config{
		RootCAs:            caCertPool,
		InsecureSkipVerify: skipVerify,
		ServerName:         serverName,
	}

	return tlsConfig, nil
}

// Connect establishes MQTT connection
func (di *DeviceInterface) Connect(deviceID, deviceSecret string) {
	// Check if already connected
	if di.IsConnected() {
		di.callbacks.OnLog("Already connected to MQTT broker, updating status", "info")
		// Update status to reflect current connection state
		di.setMQTTStatus("connected")
		di.callbacks.OnMQTTStatus("connected")
		// Update device status if it's not already ONLINE
		currentStatus := di.ctx.getDeviceStatus()
		if currentStatus != "ONLINE" && currentStatus != "PAUSED" {
			di.setDeviceStatus("ONLINE")
			di.callbacks.OnDeviceStatus("ONLINE")
		}
		return
	}

	useTLS := di.cfg.MQTTUseTLS
	broker := di.cfg.MQTTBroker
	var brokerURL string

	if useTLS {
		brokerURL = fmt.Sprintf("ssl://%s:%d", broker, di.cfg.MQTTTLSPort)
	} else {
		brokerURL = fmt.Sprintf("tcp://%s:%d", broker, di.cfg.MQTTPort)
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetClientID(deviceID + "_device_" + GenerateID())
	opts.SetUsername(deviceID)
	opts.SetPassword(deviceSecret)
	opts.SetCleanSession(true)
	opts.SetAutoReconnect(true)

	// Configure TLS if enabled
	if useTLS {
		tlsConfig, err := di.createTLSConfig()
		if err != nil {
			di.callbacks.OnLog("Failed to create TLS config: "+err.Error(), "error")
			return
		}
		opts.SetTLSConfig(tlsConfig)
	}

	// Set connection status callback
	opts.SetOnConnectHandler(func(client mqtt.Client) {
		di.setMQTTStatus("connected")
		di.callbacks.OnMQTTStatus("connected")
		di.callbacks.OnLog("Connected to MQTT broker", "success")
		// Subscribe to topics and complete startup sequence
		di.subscribeToTopics()
		go di.completeConnectionSequence()
	})
	opts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
		di.setMQTTStatus("disconnected")
		di.callbacks.OnMQTTStatus("disconnected")
		di.callbacks.OnLog("MQTT connection lost: "+err.Error(), "error")
		// Set device status to OFFLINE when connection is lost
		currentStatus := di.ctx.getDeviceStatus()
		if currentStatus != "OFFLINE" {
			di.setDeviceStatus("OFFLINE")
			di.callbacks.OnDeviceStatus("OFFLINE")
		}
	})

	di.mqttClient = mqtt.NewClient(opts)

	// Set device status to STARTING and connecting status
	di.setDeviceStatus("STARTING")
	di.callbacks.OnDeviceStatus("STARTING")
	di.setMQTTStatus("connecting")
	di.callbacks.OnMQTTStatus("connecting")
	di.callbacks.OnLog("Connecting to MQTT broker...", "info")

	// Use WaitTimeout to avoid blocking indefinitely (15 second timeout for connection)
	token := di.mqttClient.Connect()
	if token.WaitTimeout(15 * time.Second) {
		if token.Error() != nil {
			err := token.Error()
			errMsg := err.Error()
			di.setMQTTStatus("error")
			di.callbacks.OnMQTTStatus("error")
			di.callbacks.OnLog("MQTT connection failed: "+errMsg, "error")
			// Set device status back to OFFLINE when connection fails
			di.setDeviceStatus("OFFLINE")
			di.callbacks.OnDeviceStatus("OFFLINE")
			if isMQTTAuthError(errMsg) {
				di.callbacks.OnLog("MQTT credentials rejected: shutting down", "error")
				// Call shutdown if callback supports it
				if shutdownCallback, ok := di.callbacks.(interface{ Shutdown() }); ok {
					shutdownCallback.Shutdown()
				}
			}
		}
		// If no error, connection succeeded - OnConnectHandler will be called
	} else {
		// Timeout occurred
		di.setMQTTStatus("error")
		di.callbacks.OnMQTTStatus("error")
		di.callbacks.OnLog("MQTT connection timeout after 15 seconds", "error")
		di.setDeviceStatus("OFFLINE")
		di.callbacks.OnDeviceStatus("OFFLINE")
	}
}

func isMQTTAuthError(errMsg string) bool {
	msg := strings.ToLower(errMsg)
	return strings.Contains(msg, "not authorized") || strings.Contains(msg, "not authorised")
}

func (di *DeviceInterface) subscribeToTopics() {
	ctx := context.Background()

	// Define topics in a specific order - critical response topics first
	criticalTopics := []struct {
		topic   string
		handler mqtt.MessageHandler
	}{
		{"/devices/" + di.deviceID + "/response/authorize", di.handleAuthorizeResponse},
		{"/devices/" + di.deviceID + "/response/invoice", di.handleInvoiceResponse},
		{"/devices/" + di.deviceID + "/events/invoice", di.handleInvoiceEvent},
		{"/devices/" + di.deviceID + "/balance", di.handleBalanceMessage},
		{"/devices/" + di.deviceID + "/config", di.handleConfigMessage},
		{"/devices/" + di.deviceID + "/control", di.handleControlMessage},
	}

	// Subscribe to each topic and wait for confirmation
	for _, t := range criticalTopics {
		token := di.mqttClient.Subscribe(t.topic, 1, t.handler)
		if token.WaitTimeout(5 * time.Second) {
			if token.Error() != nil {
				di.callbacks.OnLog("Failed to subscribe to "+t.topic+": "+token.Error().Error(), "error")
			} else {
				logger.InfoWithFields(ctx, "Subscribed to topic on device mqtt", map[string]interface{}{
					"topic": t.topic,
				})
				// Log subscription success for invoice events topic
				if strings.Contains(t.topic, "/events/invoice") {
					di.callbacks.OnLog("Subscribed to invoice events: "+t.topic, "info")
				}
			}
		} else {
			di.callbacks.OnLog("Timeout subscribing to "+t.topic, "error")
		}
	}

	// Additional delay to ensure broker has fully processed all subscriptions
	// This prevents race conditions where responses arrive before subscriptions are ready
	time.Sleep(500 * time.Millisecond)
	logger.Info(ctx, "All subscriptions established, ready to send messages on device mqtt")

	// Signal that subscriptions are ready
	select {
	case di.subscriptionsReady <- true:
	default:
		// Channel already has a value or is closed, ignore
	}
}

// completeConnectionSequence handles the startup sequence after MQTT connection
// It waits for subscriptions, sends heartbeat and authorization request, then calls OnConnected
func (di *DeviceInterface) completeConnectionSequence() {
	ctx := context.Background()
	const connectionTimeout = 5 * time.Second
	const subscriptionTimeout = 3 * time.Second

	// Wait for MQTT connection to be established (should be quick since we're already connecting)
	if err := di.waitForConnection(connectionTimeout); err != nil {
		if errors.Is(err, errConnectionTimeout) {
			di.callbacks.OnLog("MQTT connection timeout during startup - reverting to OFFLINE", "error")
			di.setDeviceStatus("OFFLINE")
			di.callbacks.OnDeviceStatus("OFFLINE")
			if shutdownCallback, ok := di.callbacks.(interface{ Shutdown() }); ok {
				shutdownCallback.Shutdown()
			}
		}
		return
	}

	// Wait for subscriptions to be ready (should be quick after connection)
	select {
	case <-di.subscriptionsReady:
		logger.WithDeviceID(di.deviceID).
			Info(ctx, "Subscriptions ready, proceeding with startup sequence on device mqtt")
	case <-time.After(subscriptionTimeout):
		di.callbacks.OnLog("Timeout waiting for subscriptions - reverting to OFFLINE", "error")
		di.setDeviceStatus("OFFLINE")
		di.callbacks.OnDeviceStatus("OFFLINE")
		if shutdownCallback, ok := di.callbacks.(interface{ Shutdown() }); ok {
			shutdownCallback.Shutdown()
		}
		return
	}

	// Send initial heartbeat and authorization request (framework handles this)
	di.PublishHeartbeat(mqttmodel.DeviceStatus_DEVICE_STATUS_ONLINE)
	di.PublishAuthorizeRequest("STARTUP")

	// Start heartbeat ticker if enabled
	di.startHeartbeat()

	// Notify callback that device is connected and ready
	di.callbacks.OnLog("Device connected and ready", "success")
	di.callbacks.OnConnected()
}

// waitForConnection waits for the MQTT connection to be established
func (di *DeviceInterface) waitForConnection(timeout time.Duration) error {
	// Check immediately first (connection might already be established)
	if di.IsConnected() {
		return nil
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	// Use a faster ticker for more responsive checking
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if di.IsConnected() {
			return nil
		}
		select {
		case <-timer.C:
			return errConnectionTimeout
		case <-ticker.C:
		}
	}
}

// MQTT Message Handlers
func (di *DeviceInterface) handleConfigMessage(client mqtt.Client, msg mqtt.Message) {
	// Deserialize using proto model first
	var config mqttmodel.ConfigPayload
	if err := ProtoUnmarshalOpts.Unmarshal(msg.Payload(), &config); err != nil {
		di.callbacks.OnLog("Failed to parse config message: "+err.Error(), "error")
		return
	}

	// Convert to domain type (type alias, so this is just a pointer conversion)
	domainConfig := (*DeviceConfig)(&config)

	// Update device context first
	oldConfig := di.ctx.getConfig()
	di.setConfig(domainConfig)

	// Restart heartbeat if interval changed
	if oldConfig != nil && oldConfig.HeartbeatInterval != domainConfig.HeartbeatInterval {
		di.restartHeartbeat()
	}

	// Then notify callback
	di.callbacks.OnLog("Configuration updated", "info")
	di.callbacks.OnConfigUpdated(domainConfig)
}

func (di *DeviceInterface) handleAuthorizeResponse(client mqtt.Client, msg mqtt.Message) {
	// Deserialize using proto model first
	var response mqttmodel.AuthorizationResponsePayload
	if err := ProtoUnmarshalOpts.Unmarshal(msg.Payload(), &response); err != nil {
		di.callbacks.OnLog("Failed to parse authorize response: "+err.Error(), "error")
		return
	}

	// Convert to domain type (type alias, so this is just a pointer conversion)
	domainResponse := (*AuthorizeResponse)(&response)

	switch response.Status {
	case mqttmodel.AuthorizationStatus_AUTHORIZATION_STATUS_GRANTED:
		auth := &Authorization{
			AuthorizationID: response.AuthorizationId,
			RequestID:       response.RequestId,
			GrantedMsat:     response.GrantedMsat,
			RemainingMsat:   response.RemainingMsat,
			IssuedAt:        response.IssuedAt,
			ExpiresAt:       response.ExpiresAt,
			Status:          "ACTIVE",
			Reason:          response.Reason,
		}
		di.setAuthorization(auth)
		di.setDeviceStatus("ONLINE")
		di.callbacks.OnDeviceStatus("ONLINE")
		// Clear pending authorization flag
		di.pendingAuthMu.Lock()
		di.pendingAuthorization = false
		di.pendingAuthMu.Unlock()
		di.callbacks.OnLog("Authorization granted: "+FormatMsat(response.GrantedMsat)+" msat (reserved)", "success")
		di.callbacks.OnAuthorizationGranted(domainResponse)

	case mqttmodel.AuthorizationStatus_AUTHORIZATION_STATUS_ACTIVE:
		// ACTIVE means an existing authorization was found and returned
		auth := &Authorization{
			AuthorizationID: response.AuthorizationId,
			RequestID:       response.RequestId,
			GrantedMsat:     response.GrantedMsat,
			RemainingMsat:   response.RemainingMsat,
			IssuedAt:        response.IssuedAt,
			ExpiresAt:       response.ExpiresAt,
			Status:          "ACTIVE",
			Reason:          response.Reason,
		}
		di.setAuthorization(auth)
		di.setDeviceStatus("ONLINE")
		di.callbacks.OnDeviceStatus("ONLINE")
		// Clear pending authorization flag
		di.pendingAuthMu.Lock()
		di.pendingAuthorization = false
		di.pendingAuthMu.Unlock()
		di.callbacks.OnLog("Authorization active (existing): "+FormatMsat(response.RemainingMsat)+" msat remaining (request_id: "+response.RequestId+")", "info")
		di.callbacks.OnAuthorizationActive(domainResponse)

	case mqttmodel.AuthorizationStatus_AUTHORIZATION_STATUS_REJECTED:
		rejected := &Authorization{
			AuthorizationID: response.AuthorizationId,
			RequestID:       response.RequestId,
			GrantedMsat:     0,
			RemainingMsat:   0,
			IssuedAt:        response.IssuedAt,
			ExpiresAt:       response.ExpiresAt,
			Status:          "REJECTED",
			Reason:          response.Reason,
		}
		di.setAuthorization(rejected)
		// Move to ONLINE even on rejection so device isn't stuck in STARTING
		if di.ctx.getDeviceStatus() == "STARTING" {
			di.setDeviceStatus("ONLINE")
			di.callbacks.OnDeviceStatus("ONLINE")
		}
		// Clear pending authorization flag
		di.pendingAuthMu.Lock()
		di.pendingAuthorization = false
		di.pendingAuthMu.Unlock()
		di.callbacks.OnLog("Authorization rejected: "+response.RequestId, "error")
		di.callbacks.OnAuthorizationRejected(domainResponse)
	}
}

func (di *DeviceInterface) handleBalanceMessage(client mqtt.Client, msg mqtt.Message) {
	// Deserialize using proto model first
	var balance mqttmodel.BalancePayload
	if err := ProtoUnmarshalOpts.Unmarshal(msg.Payload(), &balance); err != nil {
		di.callbacks.OnLog("Failed to parse balance message: "+err.Error(), "error")
		payloadStr := string(msg.Payload())
		if len(payloadStr) > 200 {
			payloadStr = payloadStr[:200] + "..."
		}
		di.callbacks.OnLog("Balance message payload: "+payloadStr, "error")
		return
	}

	// Convert to domain type (type alias, so this is just a pointer conversion)
	domainBalance := (*BalanceMessage)(&balance)

	// Update device context first
	di.setBalance(domainBalance)

	// Check if authorization should be requested (framework logic)
	di.checkAndRequestAuthorization(domainBalance)

	// Then notify callback
	di.callbacks.OnLog("Balance updated: "+FormatMsat(balance.AvailableMsat)+" msat available", "info")
	di.callbacks.OnBalanceUpdated(domainBalance)
}

// checkAndRequestAuthorization determines if authorization should be requested
// and publishes the request if needed. This is framework logic.
func (di *DeviceInterface) checkAndRequestAuthorization(balance *BalanceMessage) {
	available := balance.AvailableMsat
	if available <= 0 {
		return
	}

	di.pendingAuthMu.Lock()
	defer di.pendingAuthMu.Unlock()

	// Check if already pending
	if di.pendingAuthorization {
		return
	}

	// Check if there's an active authorization
	auth := di.ctx.getAuthorization()
	if auth != nil && auth.Status == "ACTIVE" {
		return
	}

	// Check if last authorization was rejected
	lastStatus := ""
	if auth != nil {
		lastStatus = auth.Status
	}
	if lastStatus != "REJECTED" {
		return
	}

	// All conditions met - request authorization
	di.pendingAuthorization = true
	di.PublishAuthorizeRequest("FUNDS_AVAILABLE")
}

func (di *DeviceInterface) handleInvoiceResponse(client mqtt.Client, msg mqtt.Message) {
	// Deserialize using proto model first
	var response mqttmodel.InvoiceResponsePayload
	if err := ProtoUnmarshalOpts.Unmarshal(msg.Payload(), &response); err != nil {
		di.callbacks.OnLog("Failed to parse invoice response: "+err.Error(), "error")
		return
	}

	// Convert to domain type (type alias, so this is just a pointer conversion)
	domainResponse := (*InvoiceResponse)(&response)

	switch response.Status {
	case mqttmodel.InvoiceStatus_INVOICE_STATUS_CREATED:
		di.setInvoice(domainResponse)
		di.callbacks.OnInvoiceCreated(domainResponse)
		di.callbacks.OnLog("Invoice created: "+response.InvoiceId, "success")
	case mqttmodel.InvoiceStatus_INVOICE_STATUS_EXPIRED:
		di.setInvoice(nil)
		di.callbacks.OnInvoiceExpired(response.InvoiceId)
		di.callbacks.OnLog("Invoice expired: "+response.InvoiceId, "error")
	case mqttmodel.InvoiceStatus_INVOICE_STATUS_FAILED:
		di.setInvoice(nil)
		di.callbacks.OnInvoiceFailed(response.InvoiceId)
		di.callbacks.OnLog("Invoice failed: "+response.InvoiceId, "error")
	case mqttmodel.InvoiceStatus_INVOICE_STATUS_SETTLED:
		di.handleInvoiceSettled(response.InvoiceId, response.AmountMsat)
	}
}

func (di *DeviceInterface) handleInvoiceEvent(client mqtt.Client, msg mqtt.Message) {
	// Deserialize using proto model first
	var event mqttmodel.InvoiceEventPayload
	if err := ProtoUnmarshalOpts.Unmarshal(msg.Payload(), &event); err != nil {
		di.callbacks.OnLog("Failed to parse invoice event: "+err.Error(), "error")
		return
	}

	// Get current invoice to check if this event is for the active invoice
	// This check is device-specific, so we'll let the callback handle it if needed
	// For now, we'll process all events

	switch event.Status {
	case mqttmodel.InvoiceStatus_INVOICE_STATUS_SETTLED:
		di.handleInvoiceSettled(event.InvoiceId, event.AmountReceivedMsat)
	case mqttmodel.InvoiceStatus_INVOICE_STATUS_EXPIRED:
		di.setInvoice(nil)
		di.callbacks.OnInvoiceExpired(event.InvoiceId)
		di.callbacks.OnLog("Invoice expired: "+event.InvoiceId, "error")
	default:
		di.callbacks.OnLog("Unhandled invoice event status: "+event.Status.String()+" for invoice "+event.InvoiceId, "error")
	}
}

// handleInvoiceSettled is a helper that handles invoice settlement
func (di *DeviceInterface) handleInvoiceSettled(invoiceID string, amountMsat int64) {
	di.setInvoice(nil)
	di.callbacks.OnInvoiceSettled(invoiceID, amountMsat)
	amountMsg := FormatMsat(amountMsat)
	di.callbacks.OnLog(fmt.Sprintf("Invoice settled: %s (%s msats received)", invoiceID, amountMsg), "success")
}

func (di *DeviceInterface) handleControlMessage(client mqtt.Client, msg mqtt.Message) {
	var control mqttmodel.ControlPayload
	if err := ProtoUnmarshalOpts.Unmarshal(msg.Payload(), &control); err != nil {
		di.callbacks.OnLog("Failed to parse control message: "+err.Error(), "error")
		return
	}

	switch control.Command {
	case mqttmodel.ControlCommand_CONTROL_COMMAND_STOP:
		di.handleStopCommand(control.Reason)

	case mqttmodel.ControlCommand_CONTROL_COMMAND_PAUSE:
		di.handlePauseCommand(control.Reason)

	case mqttmodel.ControlCommand_CONTROL_COMMAND_RESUME:
		di.handleResumeCommand()

	case mqttmodel.ControlCommand_CONTROL_COMMAND_REBOOT:
		di.callbacks.OnLog("Command REBOOT received - restarting device", "info")
		di.callbacks.OnControlReboot()

	case mqttmodel.ControlCommand_CONTROL_COMMAND_PING:
		di.handlePingCommand(control.Id)

	case mqttmodel.ControlCommand_CONTROL_COMMAND_UPDATE_CONFIG:
		// Call OnControlUpdateConfig if callback supports it (optional)
		if updateConfigCallback, ok := di.callbacks.(interface{ OnControlUpdateConfig() }); ok {
			updateConfigCallback.OnControlUpdateConfig()
		}
		di.callbacks.OnLog("Command UPDATE_CONFIG received", "info")
		// Configuration is automatically updated via the retained config topic subscription

	case mqttmodel.ControlCommand_CONTROL_COMMAND_AUTHORIZATION:
		di.handleAuthorizationCommand(control.Reason)

	default:
		di.callbacks.OnLog("Unknown control command received: "+control.Command.String(), "error")
	}
}

// handleStopCommand handles STOP control command
func (di *DeviceInterface) handleStopCommand(reason string) {
	// Set default reason if empty (framework logic)
	if reason == "" {
		reason = "REMOTE_COMMAND"
	}
	di.callbacks.OnLog("Command STOP received: "+reason, "warning")
	// Device implementation decides whether to halt consumption or shutdown
	di.callbacks.OnControlStop(reason)
}

// handlePauseCommand handles PAUSE control command
func (di *DeviceInterface) handlePauseCommand(reason string) {
	// Set default reason if empty (framework logic)
	if reason == "" {
		reason = "REMOTE_COMMAND"
	}
	// Update device status to PAUSED if currently ONLINE
	if di.ctx.getDeviceStatus() == "ONLINE" {
		di.setDeviceStatus("PAUSED")
		di.callbacks.OnDeviceStatus("PAUSED")
	}
	di.callbacks.OnLog("Command PAUSE received: "+reason, "info")
	di.callbacks.OnControlPause(reason)
	// Send heartbeat as ONLINE since device is still connected, just paused
	di.PublishHeartbeat(mqttmodel.DeviceStatus_DEVICE_STATUS_ONLINE)
}

// handleResumeCommand handles RESUME control command
func (di *DeviceInterface) handleResumeCommand() {
	// Update device status to ONLINE if currently PAUSED or OFFLINE
	status := di.ctx.getDeviceStatus()
	if status == "PAUSED" || status == "OFFLINE" {
		di.setDeviceStatus("ONLINE")
		di.callbacks.OnDeviceStatus("ONLINE")
	}
	di.callbacks.OnLog("Command RESUME received", "info")
	di.callbacks.OnControlResume()
	di.PublishHeartbeat(mqttmodel.DeviceStatus_DEVICE_STATUS_ONLINE)
}

// handlePingCommand handles PING control command
func (di *DeviceInterface) handlePingCommand(pingID string) {
	// Call OnControlPing if callback supports it (optional)
	if pingCallback, ok := di.callbacks.(interface{ OnControlPing(string) }); ok {
		pingCallback.OnControlPing(pingID)
	}
	if pingID != "" {
		di.callbacks.OnLog("Command PING received ("+pingID+")", "info")
	} else {
		di.callbacks.OnLog("Command PING received", "info")
	}
	di.PublishHeartbeat(mqttmodel.DeviceStatus_DEVICE_STATUS_ONLINE)
}

// handleAuthorizationCommand handles AUTHORIZATION control command
func (di *DeviceInterface) handleAuthorizationCommand(reason string) {
	if reason == "" {
		reason = "AUTHORIZATION_REQUIRED"
	}
	// Call OnControlAuthorization if callback supports it (optional)
	if authCallback, ok := di.callbacks.(interface{ OnControlAuthorization(string) }); ok {
		authCallback.OnControlAuthorization(reason)
	}
	di.callbacks.OnLog(fmt.Sprintf("Command AUTHORIZATION received (reason: %s)", reason), "info")
	// Request new authorization
	di.PublishAuthorizeRequest(reason)
}

// MQTT Publishers
func (di *DeviceInterface) PublishHeartbeat(status mqttmodel.DeviceStatus) {
	if di.mqttClient == nil || !di.mqttClient.IsConnected() {
		return
	}

	heartbeat := HeartbeatMessage{
		DeviceId:  di.deviceID,
		Status:    status,
		Timestamp: time.Now().Format(time.RFC3339),
	}

	payload, err := ProtoMarshalOpts.Marshal(&heartbeat)
	if err != nil {
		di.callbacks.OnLog("Failed to marshal heartbeat: "+err.Error(), "error")
		return
	}
	topic := "/devices/" + di.deviceID + "/heartbeat"

	// Use WaitTimeout to avoid blocking indefinitely (2 second timeout for heartbeat)
	token := di.mqttClient.Publish(topic, 1, false, payload)
	if token.WaitTimeout(2 * time.Second) {
		if token.Error() != nil {
			di.callbacks.OnLog("Failed to publish heartbeat: "+token.Error().Error(), "error")
		}
	} else {
		// Timeout occurred - log but don't fail (heartbeat is best-effort)
		di.callbacks.OnLog("Heartbeat publish timeout (non-critical)", "warning")
	}
}

func (di *DeviceInterface) PublishAuthorizeRequest(reason string) {
	if di.mqttClient == nil || !di.mqttClient.IsConnected() {
		return
	}

	devCfg := di.ctx.getConfig()
	if devCfg == nil {
		di.callbacks.OnLog("Cannot publish authorize request: device config not available", "error")
		return
	}

	// Set pending authorization flag (framework manages this)
	di.pendingAuthMu.Lock()
	di.pendingAuthorization = true
	di.pendingAuthMu.Unlock()

	request := AuthorizeRequest{
		DeviceId:    di.deviceID,
		RequestId:   GenerateID(),
		RequestMsat: devCfg.AuthorizeRequestMsat,
		Reason:      reason,
		Timestamp:   time.Now().Format(time.RFC3339),
	}

	payload, err := ProtoMarshalOpts.Marshal(&request)
	if err != nil {
		// Clear pending flag on error
		di.pendingAuthMu.Lock()
		di.pendingAuthorization = false
		di.pendingAuthMu.Unlock()
		di.callbacks.OnLog("Failed to marshal authorize request ("+request.RequestId+"): "+err.Error(), "error")
		return
	}
	topic := "/devices/" + di.deviceID + "/request/authorize"

	// Use WaitTimeout to avoid blocking indefinitely (5 second timeout)
	token := di.mqttClient.Publish(topic, 1, false, payload)
	if token.WaitTimeout(5 * time.Second) {
		if token.Error() != nil {
			// Clear pending flag on error
			di.pendingAuthMu.Lock()
			di.pendingAuthorization = false
			di.pendingAuthMu.Unlock()
			di.callbacks.OnLog("Failed to publish authorize request ("+request.RequestId+"): "+token.Error().Error(), "error")
		} else {
			msg := fmt.Sprintf(
				"Authorization requested (%s): %s msat for %s",
				request.RequestId,
				FormatMsat(request.RequestMsat),
				reason,
			)
			di.callbacks.OnLog(msg, "info")
		}
	} else {
		// Timeout occurred
		di.pendingAuthMu.Lock()
		di.pendingAuthorization = false
		di.pendingAuthMu.Unlock()
		di.callbacks.OnLog("Failed to publish authorize request ("+request.RequestId+"): timeout waiting for MQTT publish", "error")
	}
}

func (di *DeviceInterface) PublishUsageReport(reportID string, kWhConsumed float64) {
	if di.mqttClient == nil || !di.mqttClient.IsConnected() {
		di.callbacks.OnLog("Cannot publish usage report: MQTT client not connected", "error")
		return
	}

	devCfg := di.ctx.getConfig()
	if devCfg == nil {
		di.callbacks.OnLog("Cannot publish usage report: device config not available", "error")
		return
	}

	report := UsageReport{
		DeviceId:  di.deviceID,
		ReportId:  reportID,
		Strategy:  devCfg.ReportingStrategy,
		Measure:   kWhConsumed,
		Unit:      devCfg.MeasurementUnit,
		Timestamp: time.Now().Format(time.RFC3339),
	}

	payload, err := ProtoMarshalOpts.Marshal(&report)
	if err != nil {
		di.callbacks.OnLog("Failed to marshal usage report ("+reportID+"): "+err.Error(), "error")
		return
	}
	topic := "/devices/" + di.deviceID + "/usage"

	// Publish asynchronously (fire-and-forget) to avoid blocking
	// The MQTT client handles publishes in the background
	token := di.mqttClient.Publish(topic, 1, false, payload)

	// Check for immediate errors only (don't wait)
	if token.Error() != nil {
		di.callbacks.OnLog("Failed to publish usage report ("+reportID+"): "+token.Error().Error(), "error")
		return
	}

	// Log immediately - actual publish happens asynchronously
	msg := fmt.Sprintf(
		"Usage report sent (%s): %.4f %s",
		reportID,
		kWhConsumed,
		report.Unit,
	)
	di.callbacks.OnLog(msg, "info")
}

func (di *DeviceInterface) PublishInvoiceRequest(requestID string, amountMsat int64, reason string) {
	if di.mqttClient == nil || !di.mqttClient.IsConnected() {
		di.callbacks.OnLog("Cannot publish invoice request: MQTT client not connected", "error")
		return
	}

	// Check if device is online (framework logic)
	if di.ctx.getDeviceStatus() != "ONLINE" {
		di.callbacks.OnLog("Cannot request invoice - device is offline", "error")
		return
	}

	request := InvoiceRequest{
		DeviceId:   di.deviceID,
		RequestId:  requestID,
		AmountMsat: amountMsat,
		Reason:     reason,
		Timestamp:  time.Now().Format(time.RFC3339),
	}

	payload, err := ProtoMarshalOpts.Marshal(&request)
	if err != nil {
		di.callbacks.OnLog("Failed to marshal invoice request ("+requestID+"): "+err.Error(), "error")
		return
	}
	topic := "/devices/" + di.deviceID + "/request/invoice"

	// Use WaitTimeout to avoid blocking indefinitely (5 second timeout)
	token := di.mqttClient.Publish(topic, 1, false, payload)
	if token.WaitTimeout(5 * time.Second) {
		if token.Error() != nil {
			di.callbacks.OnLog("Failed to publish invoice request ("+requestID+"): "+token.Error().Error(), "error")
		} else {
			msg := fmt.Sprintf(
				"Invoice request sent (%s): %s msat for %s",
				requestID,
				FormatMsat(amountMsat),
				reason,
			)
			di.callbacks.OnLog(msg, "info")
		}
	} else {
		// Timeout occurred
		di.callbacks.OnLog("Failed to publish invoice request ("+requestID+"): timeout waiting for MQTT publish", "error")
	}
}

// ClearInvoice clears the current invoice from the device context
func (di *DeviceInterface) ClearInvoice() {
	currentInvoice := di.ctx.getInvoice()
	if currentInvoice != nil {
		di.setInvoice(nil)
		di.callbacks.OnLog("Invoice cleared", "info")
	}
}

// Publish publishes an arbitrary message to an MQTT topic
// For usage reports, this is fire-and-forget to avoid blocking HTTP handlers
func (di *DeviceInterface) Publish(topic string, qos byte, retained bool, payload []byte) error {
	if di.mqttClient == nil || !di.mqttClient.IsConnected() {
		return fmt.Errorf("MQTT client not connected")
	}

	token := di.mqttClient.Publish(topic, qos, retained, payload)

	// For usage reports, don't wait - fire and forget to avoid blocking HTTP handlers
	// The MQTT client will handle the publish asynchronously
	if strings.Contains(topic, "/usage") {
		// Return immediately - publish happens in background
		// Check for immediate errors only
		if token.Error() != nil {
			return fmt.Errorf("failed to publish to %s: %w", topic, token.Error())
		}
		return nil
	}

	// For other topics, wait with timeout
	if !token.WaitTimeout(5 * time.Second) {
		return fmt.Errorf("timeout publishing to %s", topic)
	}
	if token.Error() != nil {
		return fmt.Errorf("failed to publish to %s: %w", topic, token.Error())
	}
	return nil
}

// IsConnected returns whether the MQTT client is connected
func (di *DeviceInterface) IsConnected() bool {
	return di.mqttClient != nil && di.mqttClient.IsConnected()
}

// Disconnect disconnects from the MQTT broker
func (di *DeviceInterface) Disconnect() {
	// Stop heartbeat before disconnecting
	di.stopHeartbeat()

	// Update MQTT status and device status before disconnecting
	di.setMQTTStatus("disconnected")
	di.callbacks.OnMQTTStatus("disconnected")

	// Set device status to OFFLINE
	currentStatus := di.ctx.getDeviceStatus()
	if currentStatus != "OFFLINE" {
		di.setDeviceStatus("OFFLINE")
		di.callbacks.OnDeviceStatus("OFFLINE")
	}

	if di.mqttClient != nil && di.mqttClient.IsConnected() {
		di.mqttClient.Disconnect(250)
	}
}

// startHeartbeat starts the heartbeat ticker if enabled
func (di *DeviceInterface) startHeartbeat() {
	di.heartbeatMu.Lock()
	defer di.heartbeatMu.Unlock()

	// Stop existing ticker if any
	if di.heartbeatTicker != nil {
		di.heartbeatTicker.Stop()
		di.heartbeatTicker = nil
	}

	di.startHeartbeatLocked()
}

// stopHeartbeat stops the heartbeat ticker
// Must be called with heartbeatMu already locked
func (di *DeviceInterface) stopHeartbeat() {
	if di.heartbeatTicker != nil {
		di.heartbeatTicker.Stop()
		di.heartbeatTicker = nil
	}
}

// restartHeartbeat restarts the heartbeat ticker with the current config interval
func (di *DeviceInterface) restartHeartbeat() {
	di.heartbeatMu.Lock()
	defer di.heartbeatMu.Unlock()
	if !di.heartbeatEnabled {
		return
	}
	// Stop existing ticker if any
	if di.heartbeatTicker != nil {
		di.heartbeatTicker.Stop()
		di.heartbeatTicker = nil
	}
	// Start with new interval
	di.startHeartbeatLocked()
}

// startHeartbeatLocked starts the heartbeat ticker (assumes heartbeatMu is already locked)
func (di *DeviceInterface) startHeartbeatLocked() {
	if !di.heartbeatEnabled {
		return
	}

	// Get heartbeat interval from config
	config := di.ctx.getConfig()
	var interval int32 = 300 // Default to 5 minutes
	if config != nil && config.HeartbeatInterval > 0 {
		interval = config.HeartbeatInterval
	}

	di.heartbeatTicker = time.NewTicker(time.Duration(interval) * time.Second)
	go func() {
		for range di.heartbeatTicker.C {
			if di.IsConnected() {
				di.PublishHeartbeat(mqttmodel.DeviceStatus_DEVICE_STATUS_ONLINE)
			}
		}
	}()
}

// GetSubscriptionsReady returns the subscriptions ready channel
func (di *DeviceInterface) GetSubscriptionsReady() chan bool {
	return di.subscriptionsReady
}

// IsPendingAuthorization returns whether an authorization request is pending
func (di *DeviceInterface) IsPendingAuthorization() bool {
	di.pendingAuthMu.Lock()
	defer di.pendingAuthMu.Unlock()
	return di.pendingAuthorization
}

// HasActiveAuthorization returns whether there is an active authorization
func (di *DeviceInterface) HasActiveAuthorization() bool {
	auth := di.ctx.getAuthorization()
	return auth != nil && auth.Status == "ACTIVE"
}

// Private methods to update deviceContext (only DeviceInterface can modify it)

func (di *DeviceInterface) setDeviceStatus(status string) {
	di.ctx.mu.Lock()
	defer di.ctx.mu.Unlock()
	di.ctx.DeviceStatus = status
}

func (di *DeviceInterface) setConfig(config *DeviceConfig) {
	di.ctx.mu.Lock()
	defer di.ctx.mu.Unlock()
	di.ctx.Config = config
}

func (di *DeviceInterface) setBalance(balance *BalanceMessage) {
	di.ctx.mu.Lock()
	defer di.ctx.mu.Unlock()
	di.ctx.Balance = balance
}

func (di *DeviceInterface) setInvoice(invoice *InvoiceResponse) {
	di.ctx.mu.Lock()
	defer di.ctx.mu.Unlock()
	di.ctx.Invoice = invoice
}

func (di *DeviceInterface) setAuthorization(auth *Authorization) {
	di.ctx.mu.Lock()
	defer di.ctx.mu.Unlock()
	di.ctx.CurrentAuthorization = auth
}

func (di *DeviceInterface) setMQTTStatus(status string) {
	di.ctx.mu.Lock()
	defer di.ctx.mu.Unlock()
	di.ctx.MQTTStatus = status
}
