package device

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/robertodantas/lina/internal"
	mqttmodel "github.com/robertodantas/lina/services/proto/gen/model/mqtt"
)

var (
	logger = internal.NewLogger("device_interface")
)

// DeviceInterface defines the interface for device MQTT communication
// It provides methods for connecting, publishing messages, and querying device state
type DeviceInterface interface {

	// Connect establishes MQTT connection
	Connect(deviceID, deviceSecret string)

	// IsConnected returns whether the MQTT client is connected
	IsConnected() bool

	// Disconnect disconnects from the MQTT broker
	Disconnect()

	// IsPendingAuthorization returns whether an authorization request is pending
	IsPendingAuthorization() bool

	// HasActiveAuthorization returns whether there is an active authorization
	HasActiveAuthorization() bool

	// SetHeartbeatEnabled enables or disables automatic heartbeat publishing
	SetHeartbeatEnabled(enabled bool)

	// GetDeviceContext returns a copy of the device context for reading
	GetDeviceContext() DeviceState

	// GetDeviceStatus returns the current device status
	GetDeviceStatus() string

	// GetDeviceConfig returns the current device configuration
	GetDeviceConfig() *DeviceConfig

	// GetBalance returns the current balance
	GetBalance() *BalanceMessage

	// GetAuthorization returns the current authorization
	GetAuthorization() *Authorization

	// PublishHeartbeat publishes a heartbeat message
	PublishHeartbeat(status mqttmodel.DeviceStatus)

	// PublishAuthorizeRequest publishes an authorization request
	PublishAuthorizeRequest(reason string)

	// PublishUsageReport publishes a usage report
	PublishUsageReport(reportID string, kWhConsumed float64)

	// PublishInvoiceRequest publishes an invoice request
	PublishInvoiceRequest(requestID string, amountMsat int64, reason string)

	// ClearInvoice clears the current invoice from the device context
	ClearInvoice()
}

// deviceContext contains common device state shared across device types
// It is private to deviceInterfaceImpl and should only be accessed through DeviceInterface methods
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

// Getters - simple read locks for individual field access
func (ctx *deviceContext) getDeviceID() string {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.DeviceID
}

func (ctx *deviceContext) getDeviceStatus() string {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.DeviceStatus
}

func (ctx *deviceContext) getConfig() *DeviceConfig {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.Config
}

func (ctx *deviceContext) getBalance() *BalanceMessage {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.Balance
}

func (ctx *deviceContext) getInvoice() *InvoiceResponse {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.Invoice
}

func (ctx *deviceContext) getAuthorization() *Authorization {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.CurrentAuthorization
}

func (ctx *deviceContext) getMQTTStatus() string {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.MQTTStatus
}

// deviceInterfaceImpl handles MQTT communication for devices
// It uses callbacks to delegate device-specific behavior
// It manages the deviceContext internally
type deviceInterfaceImpl struct {
	callbacks        DeviceCallback
	mqttClient       mqtt.Client
	cfg              *Config
	ctx              *deviceContext
	deviceID         string
	heartbeatTicker  *time.Ticker
	heartbeatEnabled bool
}

// NewDeviceInterface creates a new device interface
func NewDeviceInterface(callbacks DeviceCallback, cfg *Config, deviceID string) DeviceInterface {
	return &deviceInterfaceImpl{
		callbacks:        callbacks,
		cfg:              cfg,
		deviceID:         deviceID,
		heartbeatEnabled: true, // Default to enabled
		ctx: &deviceContext{
			DeviceID:     deviceID,
			DeviceStatus: "OFFLINE",
			MQTTStatus:   "disconnected",
		},
	}
}

// SetHeartbeatEnabled enables or disables automatic heartbeat publishing
func (di *deviceInterfaceImpl) SetHeartbeatEnabled(enabled bool) {
	di.heartbeatEnabled = enabled
	if !enabled {
		// Stop existing ticker if any
		if di.heartbeatTicker != nil {
			di.heartbeatTicker.Stop()
			di.heartbeatTicker = nil
		}
	} else if di.IsConnected() {
		// If connected and enabling, start heartbeat
		di.startHeartbeat()
	}
}

// GetDeviceContext returns a copy of the device context for reading
func (di *deviceInterfaceImpl) GetDeviceContext() DeviceState {
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
func (di *deviceInterfaceImpl) GetDeviceStatus() string {
	return di.ctx.getDeviceStatus()
}

// GetDeviceConfig returns the current device configuration
func (di *deviceInterfaceImpl) GetDeviceConfig() *DeviceConfig {
	return di.ctx.getConfig()
}

// GetBalance returns the current balance
func (di *deviceInterfaceImpl) GetBalance() *BalanceMessage {
	return di.ctx.getBalance()
}

// GetAuthorization returns the current authorization
func (di *deviceInterfaceImpl) GetAuthorization() *Authorization {
	return di.ctx.getAuthorization()
}

// Connect establishes MQTT connection
func (di *deviceInterfaceImpl) Connect(deviceID, deviceSecret string) {
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
	var port int
	var protocol string
	if useTLS {
		port = di.cfg.MQTTTLSPort
		protocol = di.cfg.MQTTTLSProtocol
		if protocol == "" {
			protocol = "tls"
		}
	} else {
		port = di.cfg.MQTTPort
		protocol = "tcp"
	}

	clientID := deviceID + "_device_" + GenerateID()
	if di.cfg.MQTTClientID != "" {
		clientID = di.cfg.MQTTClientID + "-" + deviceID + "-" + GenerateID()
	}

	dial := internal.MQTTConnectConfig{
		Connection: internal.MQTTConnectionSpec{
			ClientID:       clientID,
			Username:       deviceID,
			Password:       deviceSecret,
			UseTLS:         useTLS,
			Broker:         broker,
			Port:           port,
			Protocol:       protocol,
			ConnectTimeout: 30 * time.Second,
			KeepAlive:      60 * time.Second,
		},
		Hooks: &internal.MQTTSessionHooks{
			OnConnect: func(client mqtt.Client) {
				// Paho invokes OnConnect before Connect()'s token completes and before
				// DialMQTT returns, so di.mqttClient is not assigned yet — set it here
				// before subscribeToTopics uses it.
				di.mqttClient = client
				di.setMQTTStatus("connected")
				di.callbacks.OnMQTTStatus("connected")
				di.callbacks.OnLog("Connected to MQTT broker", "success")
				di.subscribeToTopics()
				di.completeConnectionSequence()
			},
			OnConnectionLost: func(client mqtt.Client, err error) {
				di.setMQTTStatus("disconnected")
				di.callbacks.OnMQTTStatus("disconnected")
				di.callbacks.OnLog("MQTT connection lost: "+err.Error(), "error")
				currentStatus := di.ctx.getDeviceStatus()
				if currentStatus != "OFFLINE" {
					di.setDeviceStatus("OFFLINE")
					di.callbacks.OnDeviceStatus("OFFLINE")
				}
			},
		},
	}
	if useTLS {
		dial.TLS = &internal.MQTTTLSParams{
			BrokerHost:      broker,
			SkipVerify:      di.cfg.MQTTTLSSkipVerify,
			ServerName:      di.cfg.MQTTTLSServerName,
			CACertPath:      di.cfg.MQTTTLSCACert,
			RequireEdgeCert: di.cfg.MQTTTLSRequireEdgeCert,
			EdgeCertPath:    di.cfg.MQTTTLSEdgeCert,
			EdgeKeyPath:     di.cfg.MQTTTLSEdgeKey,
		}
	}

	di.setDeviceStatus("STARTING")
	di.callbacks.OnDeviceStatus("STARTING")
	di.setMQTTStatus("connecting")
	di.callbacks.OnMQTTStatus("connecting")
	di.callbacks.OnLog("Connecting to MQTT broker...", "info")

	client, err := internal.DialMQTT(dial)
	if err != nil {
		errMsg := err.Error()
		di.setMQTTStatus("error")
		di.callbacks.OnMQTTStatus("error")
		di.callbacks.OnLog("MQTT connection failed: "+errMsg, "error")
		di.setDeviceStatus("OFFLINE")
		di.callbacks.OnDeviceStatus("OFFLINE")
		if isMQTTAuthError(errMsg) {
			di.callbacks.OnLog("MQTT credentials rejected: shutting down", "error")
			if shutdownCallback, ok := di.callbacks.(interface{ Shutdown() }); ok {
				shutdownCallback.Shutdown()
			}
		}
		return
	}
	di.mqttClient = client
}

func isMQTTAuthError(errMsg string) bool {
	msg := strings.ToLower(errMsg)
	return strings.Contains(msg, "not authorized") || strings.Contains(msg, "not authorised")
}

func (di *deviceInterfaceImpl) subscribeToTopics() {
	ctx := context.Background()

	// Define topics in a specific order - critical response topics first
	// Wrap handlers in goroutines to prevent blocking the MQTT client's message processing loop
	criticalTopics := []struct {
		topic   string
		handler mqtt.MessageHandler
	}{
		{"/devices/" + di.deviceID + "/response/authorize", di.wrapHandler(di.handleAuthorizeResponse)},
		{"/devices/" + di.deviceID + "/response/invoice", di.wrapHandler(di.handleInvoiceResponse)},
		{"/devices/" + di.deviceID + "/events/invoice", di.wrapHandler(di.handleInvoiceEvent)},
		{"/devices/" + di.deviceID + "/balance", di.wrapHandler(di.handleBalanceMessage)},
		{"/devices/" + di.deviceID + "/config", di.wrapHandler(di.handleConfigMessage)},
		{"/devices/" + di.deviceID + "/control", di.wrapHandler(di.handleControlMessage)},
	}

	// Subscribe to all topics in parallel for faster initialization
	var wg sync.WaitGroup
	subscriptionTimeout := 2 * time.Second // Reduced from 5s to 2s
	successCount := 0
	var mu sync.Mutex

	for _, t := range criticalTopics {
		wg.Add(1)
		go func(topic string, handler mqtt.MessageHandler) {
			defer wg.Done()
			token := di.mqttClient.Subscribe(topic, 1, handler)
			if token.WaitTimeout(subscriptionTimeout) {
				if token.Error() != nil {
					di.callbacks.OnLog("Failed to subscribe to "+topic+": "+token.Error().Error(), "error")
				} else {
					mu.Lock()
					successCount++
					mu.Unlock()
					logger.InfoWithFields(ctx, "Subscribed to topic on device mqtt", map[string]interface{}{
						"topic": topic,
					})
					// Log subscription success for invoice events topic
					if strings.Contains(topic, "/events/invoice") {
						di.callbacks.OnLog("Subscribed to invoice events: "+topic, "info")
					}
				}
			} else {
				di.callbacks.OnLog("Timeout subscribing to "+topic, "error")
			}
		}(t.topic, t.handler)
	}

	// Wait for all subscriptions to complete (with overall timeout)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Wait for subscriptions with overall timeout
	select {
	case <-done:
		// All subscriptions completed
	case <-time.After(subscriptionTimeout + 500*time.Millisecond):
		di.callbacks.OnLog("Some subscriptions may have timed out", "warn")
	}

	// Small delay to ensure broker has processed subscriptions
	time.Sleep(50 * time.Millisecond)
	logger.InfoWithFields(ctx, "All subscriptions established, ready to send messages on device mqtt", map[string]interface{}{
		"subscribed": successCount,
		"total":      len(criticalTopics),
	})
}

// completeConnectionSequence handles the startup sequence after MQTT connection
// It sends heartbeat and authorization request, then calls OnConnected
// Note: This function is called after subscribeToTopics() completes, so subscriptions are already ready
func (di *deviceInterfaceImpl) completeConnectionSequence() {
	ctx := context.Background()
	logger.WithDeviceID(di.deviceID).
		Info(ctx, "Subscriptions ready, proceeding with startup sequence on device mqtt")

	// Start heartbeat ticker if enabled
	di.startHeartbeat()

	// Send authorization request
	di.PublishAuthorizeRequest("STARTUP")

	di.callbacks.OnLog("Device connected and ready", "success")
	di.callbacks.OnConnected()
}

// wrapHandler wraps a message handler in a goroutine to prevent blocking the MQTT client
func (di *deviceInterfaceImpl) wrapHandler(handler mqtt.MessageHandler) mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		topic := msg.Topic()
		payloadLen := len(msg.Payload())

		// Log that we received a message (for debugging)
		logger.DebugWithFields(context.Background(), "MQTT message received on device mqtt", map[string]interface{}{
			"topic":       topic,
			"payload_len": payloadLen,
		})

		// Copy message payload to avoid issues if the original message is reused
		payload := make([]byte, payloadLen)
		copy(payload, msg.Payload())

		// Create a message copy that can be safely used in a goroutine
		msgCopy := &messageCopy{
			topic:   topic,
			payload: payload,
		}

		// Process message in a goroutine to avoid blocking
		go func() {
			defer func() {
				if r := recover(); r != nil {
					errMsg := fmt.Sprintf("Panic in message handler for topic %s: %v", topic, r)
					logger.ErrorWithFields(context.Background(), "Panic in message handler on device mqtt", fmt.Errorf("%v", r), map[string]interface{}{
						"topic": topic,
					})
					di.callbacks.OnLog(errMsg, "error")
				}
			}()
			handler(client, msgCopy)
		}()
	}
}

// messageCopy is a simple message implementation for use in goroutines
type messageCopy struct {
	topic   string
	payload []byte
}

func (m *messageCopy) Duplicate() bool   { return false }
func (m *messageCopy) Qos() byte         { return 1 }
func (m *messageCopy) Retained() bool    { return false }
func (m *messageCopy) Topic() string     { return m.topic }
func (m *messageCopy) MessageID() uint16 { return 0 }
func (m *messageCopy) Payload() []byte   { return m.payload }
func (m *messageCopy) Ack()              {}

// MQTT Message Handlers
func (di *deviceInterfaceImpl) handleConfigMessage(client mqtt.Client, msg mqtt.Message) {
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

func (di *deviceInterfaceImpl) handleAuthorizeResponse(client mqtt.Client, msg mqtt.Message) {
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
		di.callbacks.OnLog("Authorization rejected: "+response.RequestId, "error")
		di.callbacks.OnAuthorizationRejected(domainResponse)
	}
}

func (di *deviceInterfaceImpl) handleBalanceMessage(client mqtt.Client, msg mqtt.Message) {
	ctx := context.Background()
	// Log that we received a balance message
	logger.DebugWithFields(ctx, "Received balance message on device mqtt", map[string]interface{}{
		"topic":       msg.Topic(),
		"payload_len": len(msg.Payload()),
	})

	// Deserialize using proto model first
	var balance mqttmodel.BalancePayload
	if err := ProtoUnmarshalOpts.Unmarshal(msg.Payload(), &balance); err != nil {
		logger.ErrorWithFields(ctx, "Failed to parse balance message on device mqtt", err, map[string]interface{}{
			"topic": msg.Topic(),
		})
		di.callbacks.OnLog("Failed to parse balance message: "+err.Error(), "error")
		payloadStr := string(msg.Payload())
		if len(payloadStr) > 200 {
			payloadStr = payloadStr[:200] + "..."
		}
		di.callbacks.OnLog("Balance message payload: "+payloadStr, "error")
		return
	}

	logger.DebugWithFields(ctx, "Successfully unmarshaled balance message on device mqtt", map[string]interface{}{
		"available_msat": balance.AvailableMsat,
	})

	// Convert to domain type (type alias, so this is just a pointer conversion)
	domainBalance := (*BalanceMessage)(&balance)

	// Update device context first
	logger.DebugWithFields(ctx, "About to set balance in device context on device mqtt", map[string]interface{}{
		"available_msat": balance.AvailableMsat,
	})
	di.setBalance(domainBalance)
	logger.DebugWithFields(ctx, "Balance set in device context on device mqtt", map[string]interface{}{
		"available_msat": balance.AvailableMsat,
	})

	// Check if authorization should be requested (framework logic)
	logger.DebugWithFields(ctx, "About to check authorization request on device mqtt", map[string]interface{}{
		"available_msat": balance.AvailableMsat,
	})
	di.checkAndRequestAuthorization(domainBalance)
	logger.DebugWithFields(ctx, "Returned from checkAndRequestAuthorization on device mqtt", map[string]interface{}{
		"available_msat": balance.AvailableMsat,
	})
	logger.DebugWithFields(ctx, "Checked authorization request on device mqtt", map[string]interface{}{
		"available_msat": balance.AvailableMsat,
	})

	// Then notify callback
	logger.DebugWithFields(ctx, "Calling OnBalanceUpdated callback on device mqtt", map[string]interface{}{
		"available_msat": balance.AvailableMsat,
	})
	di.callbacks.OnLog("Balance updated: "+FormatMsat(balance.AvailableMsat)+" msat available", "info")
	di.callbacks.OnBalanceUpdated(domainBalance)
	logger.DebugWithFields(ctx, "Completed balance message handling on device mqtt", map[string]interface{}{
		"available_msat": balance.AvailableMsat,
	})
}

// checkAndRequestAuthorization determines if authorization should be requested
// and publishes the request if needed. This is framework logic.
func (di *deviceInterfaceImpl) checkAndRequestAuthorization(balance *BalanceMessage) {
	ctx := context.Background()
	available := balance.AvailableMsat
	logger.DebugWithFields(ctx, "checkAndRequestAuthorization: starting", map[string]interface{}{
		"available_msat": available,
	})

	if available <= 0 {
		logger.DebugWithFields(ctx, "checkAndRequestAuthorization: available <= 0, returning", nil)
		return
	}

	// Check if there's an active authorization
	logger.DebugWithFields(ctx, "checkAndRequestAuthorization: checking authorization", nil)
	auth := di.ctx.getAuthorization()
	logger.DebugWithFields(ctx, "checkAndRequestAuthorization: got authorization", map[string]interface{}{
		"auth_is_nil": auth == nil,
		"auth_status": func() string {
			if auth == nil {
				return "nil"
			}
			return auth.Status
		}(),
	})

	if auth != nil && auth.Status == "ACTIVE" {
		logger.DebugWithFields(ctx, "checkAndRequestAuthorization: active authorization exists, returning", nil)
		return
	}

	// Check if last authorization was rejected
	lastStatus := ""
	if auth != nil {
		lastStatus = auth.Status
	}
	logger.DebugWithFields(ctx, "checkAndRequestAuthorization: checking last status", map[string]interface{}{
		"last_status": lastStatus,
	})

	if lastStatus != "REJECTED" {
		logger.DebugWithFields(ctx, "checkAndRequestAuthorization: last status not REJECTED, returning", nil)
		return
	}

	// All conditions met - request authorization
	// Backend handles duplicate request detection, so no need to track pending state
	logger.DebugWithFields(ctx, "checkAndRequestAuthorization: all conditions met, requesting authorization", nil)
	di.PublishAuthorizeRequest("FUNDS_AVAILABLE")
	logger.DebugWithFields(ctx, "checkAndRequestAuthorization: authorization request published", nil)
}

func (di *deviceInterfaceImpl) handleInvoiceResponse(client mqtt.Client, msg mqtt.Message) {
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

func (di *deviceInterfaceImpl) handleInvoiceEvent(client mqtt.Client, msg mqtt.Message) {
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
func (di *deviceInterfaceImpl) handleInvoiceSettled(invoiceID string, amountMsat int64) {
	di.setInvoice(nil)
	di.callbacks.OnInvoiceSettled(invoiceID, amountMsat)
	amountMsg := FormatMsat(amountMsat)
	di.callbacks.OnLog(fmt.Sprintf("Invoice settled: %s (%s msats received)", invoiceID, amountMsg), "success")
}

func (di *deviceInterfaceImpl) handleControlMessage(client mqtt.Client, msg mqtt.Message) {
	var control mqttmodel.ControlPayload
	if err := ProtoUnmarshalOpts.Unmarshal(msg.Payload(), &control); err != nil {
		di.callbacks.OnLog("Failed to parse control message: "+err.Error(), "error")
		return
	}

	logger.DebugWithFields(context.Background(), "Processing control message on device mqtt", map[string]interface{}{
		"command": control.Command.String(),
		"reason":  control.Reason,
	})

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
func (di *deviceInterfaceImpl) handleStopCommand(reason string) {
	// Set default reason if empty (framework logic)
	if reason == "" {
		reason = "REMOTE_COMMAND"
	}
	di.callbacks.OnLog("Command STOP received: "+reason, "warning")
	// Device implementation decides whether to halt consumption or shutdown
	di.callbacks.OnControlStop(reason)
}

// handlePauseCommand handles PAUSE control command
func (di *deviceInterfaceImpl) handlePauseCommand(reason string) {
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
func (di *deviceInterfaceImpl) handleResumeCommand() {
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
func (di *deviceInterfaceImpl) handlePingCommand(pingID string) {
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
func (di *deviceInterfaceImpl) handleAuthorizationCommand(reason string) {
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
func (di *deviceInterfaceImpl) PublishHeartbeat(status mqttmodel.DeviceStatus) {
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
	// Use QoS 0 for heartbeats as they are best-effort and sent frequently
	token := di.mqttClient.Publish(topic, 0, false, payload)
	if token.WaitTimeout(2 * time.Second) {
		if token.Error() != nil {
			di.callbacks.OnLog("Failed to publish heartbeat: "+token.Error().Error(), "error")
		}
	} else {
		// Timeout occurred - log but don't fail (heartbeat is best-effort)
		di.callbacks.OnLog("Heartbeat publish timeout (non-critical)", "warning")
	}
}

func (di *deviceInterfaceImpl) PublishAuthorizeRequest(reason string) {
	if di.mqttClient == nil || !di.mqttClient.IsConnected() {
		return
	}

	devCfg := di.ctx.getConfig()
	if devCfg == nil {
		di.callbacks.OnLog("Cannot publish authorize request: device config not available", "error")
		return
	}

	request := AuthorizeRequest{
		DeviceId:    di.deviceID,
		RequestId:   GenerateID(),
		RequestMsat: devCfg.AuthorizeRequestMsat,
		Reason:      reason,
		Timestamp:   time.Now().Format(time.RFC3339),
	}

	payload, err := ProtoMarshalOpts.Marshal(&request)
	if err != nil {
		di.callbacks.OnLog("Failed to marshal authorize request ("+request.RequestId+"): "+err.Error(), "error")
		return
	}
	topic := "/devices/" + di.deviceID + "/request/authorize"

	// Use WaitTimeout to avoid blocking indefinitely (reduced to 1 second for faster failure detection)
	token := di.mqttClient.Publish(topic, 1, false, payload)
	if token.WaitTimeout(1 * time.Second) {
		if token.Error() != nil {
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
		di.callbacks.OnLog("Failed to publish authorize request ("+request.RequestId+"): timeout waiting for MQTT publish", "error")
	}
}

func (di *deviceInterfaceImpl) PublishUsageReport(reportID string, kWhConsumed float64) {
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

func (di *deviceInterfaceImpl) PublishInvoiceRequest(requestID string, amountMsat int64, reason string) {
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

	// Use WaitTimeout to avoid blocking indefinitely (reduced to 1 second for faster failure detection)
	token := di.mqttClient.Publish(topic, 1, false, payload)
	if token.WaitTimeout(1 * time.Second) {
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
func (di *deviceInterfaceImpl) ClearInvoice() {
	currentInvoice := di.ctx.getInvoice()
	if currentInvoice != nil {
		di.setInvoice(nil)
		di.callbacks.OnLog("Invoice cleared", "info")
	}
}

// publish publishes an arbitrary message to an MQTT topic (internal use only)
// For usage reports, this is fire-and-forget to avoid blocking HTTP handlers
func (di *deviceInterfaceImpl) publish(topic string, qos byte, retained bool, payload []byte) error {
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

	// For other topics, wait with timeout (reduced to 2 seconds)
	if !token.WaitTimeout(2 * time.Second) {
		return fmt.Errorf("timeout publishing to %s", topic)
	}
	if token.Error() != nil {
		return fmt.Errorf("failed to publish to %s: %w", topic, token.Error())
	}
	return nil
}

// IsConnected returns whether the MQTT client is connected
func (di *deviceInterfaceImpl) IsConnected() bool {
	return di.mqttClient != nil && di.mqttClient.IsConnected()
}

// Disconnect disconnects from the MQTT broker
func (di *deviceInterfaceImpl) Disconnect() {
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
func (di *deviceInterfaceImpl) startHeartbeat() {
	if !di.heartbeatEnabled {
		return
	}

	// Stop existing ticker if any
	if di.heartbeatTicker != nil {
		di.heartbeatTicker.Stop()
		di.heartbeatTicker = nil
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

// stopHeartbeat stops the heartbeat ticker
func (di *deviceInterfaceImpl) stopHeartbeat() {
	if di.heartbeatTicker != nil {
		di.heartbeatTicker.Stop()
		di.heartbeatTicker = nil
	}
}

// restartHeartbeat restarts the heartbeat ticker with the current config interval
func (di *deviceInterfaceImpl) restartHeartbeat() {
	if !di.heartbeatEnabled {
		return
	}
	di.startHeartbeat()
}

// IsPendingAuthorization returns whether an authorization request is pending
// Note: This always returns false now - backend handles duplicate request detection
func (di *deviceInterfaceImpl) IsPendingAuthorization() bool {
	return false
}

// HasActiveAuthorization returns whether there is an active authorization
func (di *deviceInterfaceImpl) HasActiveAuthorization() bool {
	auth := di.ctx.getAuthorization()
	return auth != nil && auth.Status == "ACTIVE"
}

// Setters - simple write locks for individual field updates
// No need for transactional consistency - individual field updates are fine
func (di *deviceInterfaceImpl) setDeviceStatus(status string) {
	di.ctx.mu.Lock()
	defer di.ctx.mu.Unlock()
	di.ctx.DeviceStatus = status
}

func (di *deviceInterfaceImpl) setConfig(config *DeviceConfig) {
	di.ctx.mu.Lock()
	defer di.ctx.mu.Unlock()
	di.ctx.Config = config
}

func (di *deviceInterfaceImpl) setBalance(balance *BalanceMessage) {
	di.ctx.mu.Lock()
	defer di.ctx.mu.Unlock()
	di.ctx.Balance = balance
}

func (di *deviceInterfaceImpl) setInvoice(invoice *InvoiceResponse) {
	di.ctx.mu.Lock()
	defer di.ctx.mu.Unlock()
	di.ctx.Invoice = invoice
}

func (di *deviceInterfaceImpl) setAuthorization(auth *Authorization) {
	di.ctx.mu.Lock()
	defer di.ctx.mu.Unlock()
	di.ctx.CurrentAuthorization = auth
}

func (di *deviceInterfaceImpl) setMQTTStatus(status string) {
	di.ctx.mu.Lock()
	defer di.ctx.mu.Unlock()
	di.ctx.MQTTStatus = status
}
