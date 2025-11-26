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
			b.stopMeter()
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

	topics := map[string]mqtt.MessageHandler{
		"/devices/" + deviceID + "/config":             sb.handleConfigMessage,
		"/devices/" + deviceID + "/response/authorize": sb.handleAuthorizeResponse,
		"/devices/" + deviceID + "/balance":            sb.handleBalanceMessage,
		"/devices/" + deviceID + "/response/invoice":   sb.handleInvoiceResponse,
		"/devices/" + deviceID + "/control":            sb.handleControlMessage,
	}

	for topic, handler := range topics {
		if token := b.mqttClient.Subscribe(topic, 1, handler); token.Wait() && token.Error() != nil {
			b.addLog("Failed to subscribe to "+topic+": "+token.Error().Error(), "error")
		} else {
			log.Printf("Subscribed to %s", topic)
		}
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
		}

		b.mu.Lock()
		b.state.Authorizations = append(b.state.Authorizations, auth)
		b.state.DeviceStatus = "ONLINE"
		b.mu.Unlock()

		b.addLog("Authorization granted: "+formatMsat(response.GrantedMsat)+" msat (reserved)", "success")
		b.broadcastState()
	case mqttmodel.AuthorizationStatus_AUTHORIZATION_STATUS_REJECTED:
		b.addLog("Authorization rejected: "+response.RequestId, "error")
		b.stopMeter()
	}
}

func (sb *SouthboundInterface) handleBalanceMessage(client mqtt.Client, msg mqtt.Message) {
	log.Printf("DEBUG: Raw balance payload: %s", string(msg.Payload()))
	b := sb.backend

	var balance BalanceMessage
	if err := protoUnmarshalOpts.Unmarshal(msg.Payload(), &balance); err != nil {
		b.addLog("Failed to parse balance message: "+err.Error(), "error")
		return
	}

	b.mu.Lock()
	b.state.Balance = &balance
	b.mu.Unlock()

	b.addLog("Balance updated: "+formatMsat(balance.AvailableMsat)+" msat available", "info")
	b.broadcastState()
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
	// Handle control commands from backend if needed
	log.Printf("Control message received: %s", string(msg.Payload()))
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
