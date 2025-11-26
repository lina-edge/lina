package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	mqttmodel "github.com/robertodantas/lnpay/services/proto/gen/model/mqtt"
)

// SouthboundInterface handles MQTT communication for the smart meter backend
type SouthboundInterface struct {
	backend *SmartMeterBackend
}

// NewSouthboundInterface creates a new southbound interface
func NewSouthboundInterface(backend *SmartMeterBackend) *SouthboundInterface {
	return &SouthboundInterface{
		backend: backend,
	}
}

// createTLSConfig creates TLS configuration for MQTT connection
func (sb *SouthboundInterface) createTLSConfig() (*tls.Config, error) {
	caFile := getEnv("MQTT_TLS_CA_CERT", "/certs/ca.crt")
	skipVerify := getEnv("MQTT_TLS_SKIP_VERIFY", "false") == "true"
	serverName := getEnv("MQTT_TLS_SERVER_NAME", "mosquitto")

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
func (sb *SouthboundInterface) Connect() {
	b := sb.backend
	// Determine broker URL based on TLS configuration
	useTLS := getEnv("MQTT_USE_TLS", "true") == "true"
	broker := getEnv("MQTT_BROKER", "mosquitto")
	var brokerURL string

	if useTLS {
		port := getEnv("MQTT_TLS_PORT", "8883")
		brokerURL = "ssl://" + broker + ":" + port
	} else {
		port := getEnv("MQTT_PORT", "1883")
		brokerURL = "tcp://" + broker + ":" + port
	}

	username := getEnv("MQTT_USERNAME", b.state.DeviceID)
	password := getEnv("MQTT_PASSWORD", b.state.DeviceID+"_password")

	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetClientID(b.state.DeviceID + "_backend_" + generateID())
	opts.SetUsername(username)
	opts.SetPassword(password)
	opts.SetCleanSession(true)
	opts.SetAutoReconnect(true)

	// Configure TLS if enabled
	if useTLS {
		tlsConfig, err := sb.createTLSConfig()
		if err != nil {
			b.addLog("Failed to create TLS config: "+err.Error(), "error")
			return
		}
		opts.SetTLSConfig(tlsConfig)
	}

	opts.SetOnConnectHandler(func(client mqtt.Client) {
		b.mu.Lock()
		b.state.MQTTStatus = "connected"
		b.mu.Unlock()

		b.addLog("Connected to MQTT broker", "success")
		sb.subscribeToTopics()
		b.broadcastState()
	})

	opts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
		b.mu.Lock()
		b.state.MQTTStatus = "disconnected"
		b.mu.Unlock()

		b.addLog("MQTT connection lost: "+err.Error(), "error")
		b.broadcastState()
	})

	b.mqttClient = mqtt.NewClient(opts)

	b.mu.Lock()
	b.state.MQTTStatus = "connecting"
	b.mu.Unlock()
	b.broadcastState()

	if token := b.mqttClient.Connect(); token.Wait() && token.Error() != nil {
		err := token.Error()
		errMsg := err.Error()
		b.mu.Lock()
		b.state.MQTTStatus = "error"
		b.mu.Unlock()
		b.addLog("MQTT connection failed: "+errMsg, "error")
		if isMQTTAuthError(errMsg) {
			b.addLog("MQTT credentials rejected - setting device OFFLINE", "error")
			b.shutdownMeter()
		}
		b.broadcastState()
	}
}

func isMQTTAuthError(errMsg string) bool {
	msg := strings.ToLower(errMsg)
	return strings.Contains(msg, "not authorized") || strings.Contains(msg, "not authorised")
}

func (sb *SouthboundInterface) subscribeToTopics() {
	b := sb.backend
	deviceID := b.state.DeviceID

	// Define topics in a specific order - critical response topics first
	criticalTopics := []struct {
		topic   string
		handler mqtt.MessageHandler
	}{
		{"/devices/" + deviceID + "/response/authorize", sb.handleAuthorizeResponse},
		{"/devices/" + deviceID + "/response/invoice", sb.handleInvoiceResponse},
		{"/devices/" + deviceID + "/balance", sb.handleBalanceMessage},
		{"/devices/" + deviceID + "/config", sb.handleConfigMessage},
		{"/devices/" + deviceID + "/control", sb.handleControlMessage},
	}

	// Subscribe to each topic and wait for confirmation
	for _, t := range criticalTopics {
		token := b.mqttClient.Subscribe(t.topic, 1, t.handler)
		if token.WaitTimeout(5 * time.Second) {
			if token.Error() != nil {
				b.addLog("Failed to subscribe to "+t.topic+": "+token.Error().Error(), "error")
			} else {
				log.Printf("Subscribed to %s", t.topic)
			}
		} else {
			b.addLog("Timeout subscribing to "+t.topic, "error")
		}
	}

	// Additional delay to ensure broker has fully processed all subscriptions
	// This prevents race conditions where responses arrive before subscriptions are ready
	time.Sleep(500 * time.Millisecond)
	log.Printf("All subscriptions established, ready to send messages")

	// Signal that subscriptions are ready
	select {
	case b.subscriptionsReady <- true:
	default:
		// Channel already has a value or is closed, ignore
	}
}

// MQTT Message Handlers
func (sb *SouthboundInterface) handleConfigMessage(client mqtt.Client, msg mqtt.Message) {
	log.Printf("DEBUG: Raw config payload: %s", string(msg.Payload()))
	b := sb.backend

	var config Config
	if err := protoUnmarshalOpts.Unmarshal(msg.Payload(), &config); err != nil {
		b.addLog("Failed to parse config message: "+err.Error(), "error")
		return
	}

	b.mu.Lock()
	b.state.Config = &config
	b.mu.Unlock()

	b.addLog("Configuration updated", "info")
	b.broadcastState()
}

func (sb *SouthboundInterface) handleAuthorizeResponse(client mqtt.Client, msg mqtt.Message) {
	b := sb.backend
	var response AuthorizeResponse
	if err := protoUnmarshalOpts.Unmarshal(msg.Payload(), &response); err != nil {
		b.addLog("Failed to parse authorize response: "+err.Error(), "error")
		return
	}

	switch response.Status {
	case mqttmodel.AuthorizationStatus_AUTHORIZATION_STATUS_GRANTED:
		auth := Authorization{
			AuthorizationID: response.AuthorizationId,
			RequestID:       response.RequestId,
			GrantedMsat:     response.GrantedMsat,
			RemainingMsat:   response.RemainingMsat,
			IssuedAt:        response.IssuedAt,
			ExpiresAt:       response.ExpiresAt,
			Status:          "ACTIVE",
			Reason:          response.Reason,
		}

		b.mu.Lock()
		b.state.Authorizations = append(b.state.Authorizations, auth)
		b.state.DeviceStatus = "ONLINE"
		b.pendingAuthorization = false
		b.mu.Unlock()

		b.addLog("Authorization granted: "+formatMsat(response.GrantedMsat)+" msat (reserved)", "success")
		b.broadcastState()
		// Restore previous appliance states if any were saved during halt
		b.restoreApplianceStates()
	case mqttmodel.AuthorizationStatus_AUTHORIZATION_STATUS_REJECTED:
		b.addLog("Authorization rejected: "+response.RequestId, "error")
		// Record rejected authorization for future retry logic
		b.mu.Lock()
		rejected := Authorization{
			AuthorizationID: response.AuthorizationId,
			RequestID:       response.RequestId,
			GrantedMsat:     0,
			RemainingMsat:   0,
			IssuedAt:        response.IssuedAt,
			ExpiresAt:       response.ExpiresAt,
			Status:          "REJECTED",
			Reason:          response.Reason,
		}
		b.state.Authorizations = append(b.state.Authorizations, rejected)
		b.pendingAuthorization = false
		// Move to ONLINE even on rejection so device isn't stuck in STARTING
		if b.state.DeviceStatus == "STARTING" {
			b.state.DeviceStatus = "ONLINE"
		}
		b.mu.Unlock()
		b.haltConsumption(response.Reason)
	}
}

func (sb *SouthboundInterface) handleBalanceMessage(client mqtt.Client, msg mqtt.Message) {
	b := sb.backend

	var balance BalanceMessage
	if err := protoUnmarshalOpts.Unmarshal(msg.Payload(), &balance); err != nil {
		b.addLog("Failed to parse balance message: "+err.Error(), "error")
		return
	}

	b.mu.Lock()
	b.state.Balance = &balance
	available := balance.AvailableMsat
	lastStatus := ""
	if len(b.state.Authorizations) > 0 {
		lastStatus = b.state.Authorizations[len(b.state.Authorizations)-1].Status
	}
	shouldRetry := available > 0 && !b.pendingAuthorization && !b.hasActiveAuthorization() && lastStatus == "REJECTED"
	if shouldRetry {
		b.pendingAuthorization = true
	}
	b.mu.Unlock()

	b.addLog("Balance updated: "+formatMsat(balance.AvailableMsat)+" msat available", "info")
	b.broadcastState()

	if shouldRetry {
		b.addLog("New funds detected requesting authorization", "info")
		sb.PublishAuthorizeRequest("FUNDS_AVAILABLE")
	}
}

func (sb *SouthboundInterface) handleInvoiceResponse(client mqtt.Client, msg mqtt.Message) {
	b := sb.backend
	var response InvoiceResponse
	if err := protoUnmarshalOpts.Unmarshal(msg.Payload(), &response); err != nil {
		b.addLog("Failed to parse invoice response: "+err.Error(), "error")
		return
	}

	switch response.Status {
	case mqttmodel.InvoiceStatus_INVOICE_STATUS_CREATED:
		b.mu.Lock()
		b.state.Invoice = &response
		b.mu.Unlock()

		b.addLog("Invoice created - scan QR to pay", "success")
		b.broadcastState()
	case mqttmodel.InvoiceStatus_INVOICE_STATUS_SETTLED:
		b.mu.Lock()
		b.state.Invoice = nil
		b.mu.Unlock()

		b.addLog("Payment received: "+formatMsat(response.AmountMsat)+" msat", "success")
		b.broadcastState()
	}
}

func (sb *SouthboundInterface) handleControlMessage(client mqtt.Client, msg mqtt.Message) {
	b := sb.backend

	var control mqttmodel.ControlPayload
	if err := protoUnmarshalOpts.Unmarshal(msg.Payload(), &control); err != nil {
		b.addLog("Failed to parse control message: "+err.Error(), "error")
		return
	}

	switch control.Command {
	case mqttmodel.ControlCommand_CONTROL_COMMAND_STOP:
		reason := control.Reason
		if reason == "" {
			reason = "REMOTE_COMMAND"
		}
		b.addLog("Command STOP received: "+reason, "warning")
		b.haltConsumption(reason)

	case mqttmodel.ControlCommand_CONTROL_COMMAND_PAUSE:
		reason := control.Reason
		if reason == "" {
			reason = "REMOTE_COMMAND"
		}
		b.addLog("Command PAUSE received: "+reason, "info")
		b.mu.Lock()
		if b.state.DeviceStatus == "ONLINE" {
			b.state.DeviceStatus = "PAUSED"
			// Turn off all appliances but keep connection
			for i := range b.state.Appliances {
				b.state.Appliances[i].IsOn = false
				b.state.Appliances[i].CurrentWatts = 0
			}
			b.state.InstantPower = 0
		}
		b.mu.Unlock()
		// Send heartbeat as ONLINE since device is still connected, just paused
		sb.PublishHeartbeat(mqttmodel.DeviceStatus_DEVICE_STATUS_ONLINE)
		b.broadcastState()

	case mqttmodel.ControlCommand_CONTROL_COMMAND_RESUME:
		b.addLog("Command RESUME received", "info")
		b.mu.Lock()
		if b.state.DeviceStatus == "PAUSED" || b.state.DeviceStatus == "OFFLINE" {
			b.state.DeviceStatus = "ONLINE"
		}
		b.mu.Unlock()
		sb.PublishHeartbeat(mqttmodel.DeviceStatus_DEVICE_STATUS_ONLINE)
		b.broadcastState()

	case mqttmodel.ControlCommand_CONTROL_COMMAND_REBOOT:
		b.addLog("Command REBOOT received - restarting device", "info")
		go func() {
			// Stop current operations
			b.shutdownMeter()
			// Wait a moment to simulate reboot
			time.Sleep(2 * time.Second)
			// Reconnect
			sb.Connect()
		}()

	case mqttmodel.ControlCommand_CONTROL_COMMAND_PING:
		pingID := control.Id
		if pingID != "" {
			b.addLog("Command PING received ("+pingID+")", "info")
		} else {
			b.addLog("Command PING received", "info")
		}
		sb.PublishHeartbeat(mqttmodel.DeviceStatus_DEVICE_STATUS_ONLINE)

	case mqttmodel.ControlCommand_CONTROL_COMMAND_UPDATE_CONFIG:
		b.addLog("Command UPDATE_CONFIG received", "info")
		// Configuration is automatically updated via the retained config topic subscription
		// No additional action needed here - the device will receive the updated config
		// through the handleConfigMessage handler

	case mqttmodel.ControlCommand_CONTROL_COMMAND_AUTHORIZATION:
		reason := control.Reason
		if reason == "" {
			reason = "AUTHORIZATION_REQUIRED"
		}
		b.addLog(fmt.Sprintf("Command AUTHORIZATION received (reason: %s)", reason), "info")
		// Request new authorization
		sb.PublishAuthorizeRequest(reason)

	default:
		b.addLog("Unknown control command received: "+control.Command.String(), "error")
	}
}

// MQTT Publishers
func (sb *SouthboundInterface) PublishHeartbeat(status mqttmodel.DeviceStatus) {
	b := sb.backend
	if b.mqttClient == nil || !b.mqttClient.IsConnected() {
		return
	}

	heartbeat := HeartbeatMessage{
		DeviceId:  b.state.DeviceID,
		Status:    status,
		Timestamp: time.Now().Format(time.RFC3339),
	}

	payload, err := protoMarshalOpts.Marshal(&heartbeat)
	if err != nil {
		b.addLog("Failed to marshal heartbeat: "+err.Error(), "error")
		return
	}
	topic := "/devices/" + b.state.DeviceID + "/heartbeat"

	if token := b.mqttClient.Publish(topic, 1, false, payload); token.Wait() && token.Error() != nil {
		b.addLog("Failed to publish heartbeat: "+token.Error().Error(), "error")
	}
	b.broadcastState()
}

func (sb *SouthboundInterface) PublishAuthorizeRequest(reason string) {
	b := sb.backend
	if b.mqttClient == nil || !b.mqttClient.IsConnected() {
		return
	}

	request := AuthorizeRequest{
		DeviceId:    b.state.DeviceID,
		RequestId:   generateID(),
		RequestMsat: b.state.Config.AuthorizeRequestMsat,
		Reason:      reason,
		Timestamp:   time.Now().Format(time.RFC3339),
	}

	payload, err := protoMarshalOpts.Marshal(&request)
	if err != nil {
		b.addLog("Failed to marshal authorize request ("+request.RequestId+"): "+err.Error(), "error")
		return
	}
	topic := "/devices/" + b.state.DeviceID + "/request/authorize"

	if token := b.mqttClient.Publish(topic, 1, false, payload); token.Wait() && token.Error() != nil {
		b.addLog("Failed to publish authorize request ("+request.RequestId+"): "+token.Error().Error(), "error")
	} else {
		msg := fmt.Sprintf(
			"Authorization requested (%s): %s msat for %s",
			request.RequestId,
			formatMsat(request.RequestMsat),
			reason,
		)
		b.addLog(msg, "info")
	}
}

func (sb *SouthboundInterface) PublishUsageReport(reportID string, kWhConsumed float64) {
	b := sb.backend
	if b.mqttClient == nil || !b.mqttClient.IsConnected() {
		return
	}
	if b.state.Config == nil {
		return
	}

	report := UsageReport{
		DeviceId:  b.state.DeviceID,
		ReportId:  reportID,
		Strategy:  b.state.Config.ReportingStrategy,
		Measure:   kWhConsumed,
		Unit:      b.state.Config.Unit,
		Timestamp: time.Now().Format(time.RFC3339),
	}

	payload, err := protoMarshalOpts.Marshal(&report)
	if err != nil {
		b.addLog("Failed to marshal usage report ("+reportID+"): "+err.Error(), "error")
		return
	}
	topic := "/devices/" + b.state.DeviceID + "/usage"

	if token := b.mqttClient.Publish(topic, 1, false, payload); token.Wait() && token.Error() != nil {
		b.addLog("Failed to publish usage report ("+reportID+"): "+token.Error().Error(), "error")
	} else {
		msg := fmt.Sprintf(
			"Usage report sent (%s): %.4f %s",
			reportID,
			kWhConsumed,
			report.Unit,
		)
		b.addLog(msg, "info")
	}
}

func (sb *SouthboundInterface) PublishInvoiceRequest(requestID string, amountMsat int64, reason string) {
	b := sb.backend
	if b.mqttClient == nil || !b.mqttClient.IsConnected() {
		return
	}

	request := InvoiceRequest{
		DeviceId:   b.state.DeviceID,
		RequestId:  requestID,
		AmountMsat: amountMsat,
		Reason:     reason,
		Timestamp:  time.Now().Format(time.RFC3339),
	}

	payload, err := protoMarshalOpts.Marshal(&request)
	if err != nil {
		b.addLog("Failed to marshal invoice request ("+requestID+"): "+err.Error(), "error")
		return
	}
	topic := "/devices/" + b.state.DeviceID + "/request/invoice"

	if token := b.mqttClient.Publish(topic, 1, false, payload); token.Wait() && token.Error() != nil {
		b.addLog("Failed to publish invoice request ("+requestID+"): "+token.Error().Error(), "error")
	} else {
		msg := fmt.Sprintf(
			"Invoice request sent (%s): %s msat for %s",
			requestID,
			formatMsat(amountMsat),
			reason,
		)
		b.addLog(msg, "info")
	}
}
