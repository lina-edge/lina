package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	mqttmodel "github.com/robertodantas/lnpay/services/proto/gen/model/mqtt"
)

// SouthboundInterface handles MQTT communication for the smart meter backend
type SouthboundInterface struct {
	meter              *SmartMeter
	mqttClient         mqtt.Client
	subscriptionsReady chan bool
	cfg                *Config
}

// NewSouthboundInterface creates a new southbound interface
func NewSouthboundInterface(meter *SmartMeter, cfg *Config) *SouthboundInterface {
	return &SouthboundInterface{
		meter:              meter,
		subscriptionsReady: make(chan bool, 1),
		cfg:                cfg,
	}
}

// createTLSConfig creates TLS configuration for MQTT connection
func (sb *SouthboundInterface) createTLSConfig() (*tls.Config, error) {
	caFile := sb.cfg.MQTTTLSCACert
	skipVerify := sb.cfg.MQTTTLSSkipVerify
	serverName := sb.cfg.MQTTTLSServerName

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
	useTLS := sb.cfg.MQTTUseTLS
	broker := sb.cfg.MQTTBroker
	var brokerURL string

	if useTLS {
		brokerURL = fmt.Sprintf("ssl://%s:%d", broker, sb.cfg.MQTTTLSPort)
	} else {
		brokerURL = fmt.Sprintf("tcp://%s:%d", broker, sb.cfg.MQTTPort)
	}

	deviceID := sb.meter.GetDeviceID()
	username := deviceID
	password := sb.meter.deviceSecret

	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetClientID(deviceID + "_backend_" + generateID())
	opts.SetUsername(username)
	opts.SetPassword(password)
	opts.SetCleanSession(true)
	opts.SetAutoReconnect(true)

	// Configure TLS if enabled
	if useTLS {
		tlsConfig, err := sb.createTLSConfig()
		if err != nil {
			sb.meter.AddLog("Failed to create TLS config: "+err.Error(), "error")
			return
		}
		opts.SetTLSConfig(tlsConfig)
	}

	opts.SetOnConnectHandler(func(client mqtt.Client) {
		sb.meter.SetMQTTStatus("connected")
		sb.meter.AddLog("Connected to MQTT broker", "success")
		sb.subscribeToTopics()
	})

	opts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
		sb.meter.SetMQTTStatus("disconnected")
		sb.meter.AddLog("MQTT connection lost: "+err.Error(), "error")
	})

	sb.mqttClient = mqtt.NewClient(opts)

	sb.meter.SetMQTTStatus("connecting")

	if token := sb.mqttClient.Connect(); token.Wait() && token.Error() != nil {
		err := token.Error()
		errMsg := err.Error()
		sb.meter.SetMQTTStatus("error")
		sb.meter.AddLog("MQTT connection failed: "+errMsg, "error")
		if isMQTTAuthError(errMsg) {
			sb.meter.AddLog("MQTT credentials rejected: shutting down", "error")
			sb.meter.Shutdown()
		}
	}
}

func isMQTTAuthError(errMsg string) bool {
	msg := strings.ToLower(errMsg)
	return strings.Contains(msg, "not authorized") || strings.Contains(msg, "not authorised")
}

func (sb *SouthboundInterface) subscribeToTopics() {
	ctx := context.Background()
	deviceID := sb.meter.GetDeviceID()

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
		token := sb.mqttClient.Subscribe(t.topic, 1, t.handler)
		if token.WaitTimeout(5 * time.Second) {
			if token.Error() != nil {
				sb.meter.AddLog("Failed to subscribe to "+t.topic+": "+token.Error().Error(), "error")
			} else {
				logger.InfoWithFields(ctx, "Subscribed to topic on southbound mqtt", map[string]interface{}{
					"topic": t.topic,
				})
			}
		} else {
			sb.meter.AddLog("Timeout subscribing to "+t.topic, "error")
		}
	}

	// Additional delay to ensure broker has fully processed all subscriptions
	// This prevents race conditions where responses arrive before subscriptions are ready
	time.Sleep(500 * time.Millisecond)
	logger.Info(ctx, "All subscriptions established, ready to send messages on southbound mqtt")

	// Signal that subscriptions are ready
	select {
	case sb.subscriptionsReady <- true:
	default:
		// Channel already has a value or is closed, ignore
	}
}

// MQTT Message Handlers
func (sb *SouthboundInterface) handleConfigMessage(client mqtt.Client, msg mqtt.Message) {
	ctx := context.Background()
	logger.DebugWithFields(ctx, "Raw config payload received on southbound mqtt", map[string]interface{}{
		"payload": string(msg.Payload()),
	})

	var config DeviceConfig
	if err := protoUnmarshalOpts.Unmarshal(msg.Payload(), &config); err != nil {
		sb.meter.AddLog("Failed to parse config message: "+err.Error(), "error")
		return
	}

	sb.meter.UpdateDeviceConfig(&config)
}

func (sb *SouthboundInterface) handleAuthorizeResponse(client mqtt.Client, msg mqtt.Message) {
	var response AuthorizeResponse
	if err := protoUnmarshalOpts.Unmarshal(msg.Payload(), &response); err != nil {
		sb.meter.AddLog("Failed to parse authorize response: "+err.Error(), "error")
		return
	}

	switch response.Status {
	case mqttmodel.AuthorizationStatus_AUTHORIZATION_STATUS_GRANTED:
		sb.meter.HandleAuthorizationGranted(&response)

	case mqttmodel.AuthorizationStatus_AUTHORIZATION_STATUS_REJECTED:
		shouldHalt, haltReason := sb.meter.HandleAuthorizationRejected(&response)
		if shouldHalt {
			sb.meter.HaltConsumption(haltReason)
		}
	}
}

func (sb *SouthboundInterface) handleBalanceMessage(client mqtt.Client, msg mqtt.Message) {
	var balance BalanceMessage
	if err := protoUnmarshalOpts.Unmarshal(msg.Payload(), &balance); err != nil {
		sb.meter.AddLog("Failed to parse balance message: "+err.Error(), "error")
		return
	}

	shouldRequestAuth, reason := sb.meter.UpdateBalance(&balance)
	if shouldRequestAuth {
		sb.meter.AddLog("New funds detected requesting authorization", "info")
		sb.PublishAuthorizeRequest(reason)
	}
}

func (sb *SouthboundInterface) handleInvoiceResponse(client mqtt.Client, msg mqtt.Message) {
	var response InvoiceResponse
	if err := protoUnmarshalOpts.Unmarshal(msg.Payload(), &response); err != nil {
		sb.meter.AddLog("Failed to parse invoice response: "+err.Error(), "error")
		return
	}

	switch response.Status {
	case mqttmodel.InvoiceStatus_INVOICE_STATUS_CREATED:
		sb.meter.SetInvoice(&response)
		sb.meter.AddLog("Invoice created: "+response.InvoiceId, "success")
	case mqttmodel.InvoiceStatus_INVOICE_STATUS_EXPIRED:
		sb.meter.AddLog("Invoice expired: "+response.InvoiceId, "error")
		sb.meter.ClearInvoice()
	case mqttmodel.InvoiceStatus_INVOICE_STATUS_FAILED:
		sb.meter.AddLog("Invoice failed: "+response.InvoiceId, "error")
		sb.meter.ClearInvoice()
	case mqttmodel.InvoiceStatus_INVOICE_STATUS_SETTLED:
		sb.meter.ClearInvoice()
		sb.meter.AddLog("Payment received: "+formatMsat(response.AmountMsat)+" msa for invoice "+response.InvoiceId, "success")
	}
}

func (sb *SouthboundInterface) handleControlMessage(client mqtt.Client, msg mqtt.Message) {
	var control mqttmodel.ControlPayload
	if err := protoUnmarshalOpts.Unmarshal(msg.Payload(), &control); err != nil {
		sb.meter.AddLog("Failed to parse control message: "+err.Error(), "error")
		return
	}

	switch control.Command {
	case mqttmodel.ControlCommand_CONTROL_COMMAND_STOP:
		shouldHalt, haltReason := sb.meter.HandleControlStop(control.Reason)
		if shouldHalt {
			sb.meter.HaltConsumption(haltReason)
		}

	case mqttmodel.ControlCommand_CONTROL_COMMAND_PAUSE:
		sb.meter.HandleControlPause(control.Reason)
		// Send heartbeat as ONLINE since device is still connected, just paused
		sb.PublishHeartbeat(mqttmodel.DeviceStatus_DEVICE_STATUS_ONLINE)

	case mqttmodel.ControlCommand_CONTROL_COMMAND_RESUME:
		sb.meter.HandleControlResume()
		sb.PublishHeartbeat(mqttmodel.DeviceStatus_DEVICE_STATUS_ONLINE)

	case mqttmodel.ControlCommand_CONTROL_COMMAND_REBOOT:
		sb.meter.AddLog("Command REBOOT received - restarting device", "info")
		// Reboot will be handled by the orchestrator (main)

	case mqttmodel.ControlCommand_CONTROL_COMMAND_PING:
		pingID := control.Id
		if pingID != "" {
			sb.meter.AddLog("Command PING received ("+pingID+")", "info")
		} else {
			sb.meter.AddLog("Command PING received", "info")
		}
		sb.PublishHeartbeat(mqttmodel.DeviceStatus_DEVICE_STATUS_ONLINE)

	case mqttmodel.ControlCommand_CONTROL_COMMAND_UPDATE_CONFIG:
		sb.meter.AddLog("Command UPDATE_CONFIG received", "info")
		// Configuration is automatically updated via the retained config topic subscription
		// No additional action needed here - the device will receive the updated config
		// through the handleConfigMessage handler

	case mqttmodel.ControlCommand_CONTROL_COMMAND_AUTHORIZATION:
		reason := control.Reason
		if reason == "" {
			reason = "AUTHORIZATION_REQUIRED"
		}
		sb.meter.AddLog(fmt.Sprintf("Command AUTHORIZATION received (reason: %s)", reason), "info")
		// Request new authorization
		sb.PublishAuthorizeRequest(reason)

	default:
		sb.meter.AddLog("Unknown control command received: "+control.Command.String(), "error")
	}
}

// MQTT Publishers
func (sb *SouthboundInterface) PublishHeartbeat(status mqttmodel.DeviceStatus) {
	if sb.mqttClient == nil || !sb.mqttClient.IsConnected() {
		return
	}

	heartbeat := HeartbeatMessage{
		DeviceId:  sb.meter.GetDeviceID(),
		Status:    status,
		Timestamp: time.Now().Format(time.RFC3339),
	}

	payload, err := protoMarshalOpts.Marshal(&heartbeat)
	if err != nil {
		sb.meter.AddLog("Failed to marshal heartbeat: "+err.Error(), "error")
		return
	}
	topic := "/devices/" + sb.meter.GetDeviceID() + "/heartbeat"

	if token := sb.mqttClient.Publish(topic, 1, false, payload); token.Wait() && token.Error() != nil {
		sb.meter.AddLog("Failed to publish heartbeat: "+token.Error().Error(), "error")
	}
}

func (sb *SouthboundInterface) PublishAuthorizeRequest(reason string) {
	if sb.mqttClient == nil || !sb.mqttClient.IsConnected() {
		return
	}

	devCfg := sb.meter.GetDeviceConfig()
	request := AuthorizeRequest{
		DeviceId:    sb.meter.GetDeviceID(),
		RequestId:   generateID(),
		RequestMsat: devCfg.AuthorizeRequestMsat,
		Reason:      reason,
		Timestamp:   time.Now().Format(time.RFC3339),
	}

	payload, err := protoMarshalOpts.Marshal(&request)
	if err != nil {
		sb.meter.AddLog("Failed to marshal authorize request ("+request.RequestId+"): "+err.Error(), "error")
		return
	}
	topic := "/devices/" + sb.meter.GetDeviceID() + "/request/authorize"

	if token := sb.mqttClient.Publish(topic, 1, false, payload); token.Wait() && token.Error() != nil {
		sb.meter.AddLog("Failed to publish authorize request ("+request.RequestId+"): "+token.Error().Error(), "error")
	} else {
		msg := fmt.Sprintf(
			"Authorization requested (%s): %s msat for %s",
			request.RequestId,
			formatMsat(request.RequestMsat),
			reason,
		)
		sb.meter.AddLog(msg, "info")
	}
}

func (sb *SouthboundInterface) PublishUsageReport(reportID string, kWhConsumed float64) {
	if sb.mqttClient == nil || !sb.mqttClient.IsConnected() {
		return
	}

	devCfg := sb.meter.GetDeviceConfig()
	if devCfg == nil {
		return
	}

	report := UsageReport{
		DeviceId:  sb.meter.GetDeviceID(),
		ReportId:  reportID,
		Strategy:  devCfg.ReportingStrategy,
		Measure:   kWhConsumed,
		Unit:      devCfg.MeasurementUnit,
		Timestamp: time.Now().Format(time.RFC3339),
	}

	payload, err := protoMarshalOpts.Marshal(&report)
	if err != nil {
		sb.meter.AddLog("Failed to marshal usage report ("+reportID+"): "+err.Error(), "error")
		return
	}
	topic := "/devices/" + sb.meter.GetDeviceID() + "/usage"

	if token := sb.mqttClient.Publish(topic, 1, false, payload); token.Wait() && token.Error() != nil {
		sb.meter.AddLog("Failed to publish usage report ("+reportID+"): "+token.Error().Error(), "error")
	} else {
		msg := fmt.Sprintf(
			"Usage report sent (%s): %.4f %s",
			reportID,
			kWhConsumed,
			report.Unit,
		)
		sb.meter.AddLog(msg, "info")
	}
}

func (sb *SouthboundInterface) PublishInvoiceRequest(requestID string, amountMsat int64, reason string) {
	if sb.mqttClient == nil || !sb.mqttClient.IsConnected() {
		return
	}

	request := InvoiceRequest{
		DeviceId:   sb.meter.GetDeviceID(),
		RequestId:  requestID,
		AmountMsat: amountMsat,
		Reason:     reason,
		Timestamp:  time.Now().Format(time.RFC3339),
	}

	payload, err := protoMarshalOpts.Marshal(&request)
	if err != nil {
		sb.meter.AddLog("Failed to marshal invoice request ("+requestID+"): "+err.Error(), "error")
		return
	}
	topic := "/devices/" + sb.meter.GetDeviceID() + "/request/invoice"

	if token := sb.mqttClient.Publish(topic, 1, false, payload); token.Wait() && token.Error() != nil {
		sb.meter.AddLog("Failed to publish invoice request ("+requestID+"): "+token.Error().Error(), "error")
	} else {
		msg := fmt.Sprintf(
			"Invoice request sent (%s): %s msat for %s",
			requestID,
			formatMsat(amountMsat),
			reason,
		)
		sb.meter.AddLog(msg, "info")
	}
}

// IsConnected returns whether the MQTT client is connected
func (sb *SouthboundInterface) IsConnected() bool {
	return sb.mqttClient != nil && sb.mqttClient.IsConnected()
}

// Disconnect disconnects from the MQTT broker
func (sb *SouthboundInterface) Disconnect() {
	if sb.mqttClient != nil && sb.mqttClient.IsConnected() {
		sb.mqttClient.Disconnect(250)
	}
}

// GetSubscriptionsReady returns the subscriptions ready channel
func (sb *SouthboundInterface) GetSubscriptionsReady() chan bool {
	return sb.subscriptionsReady
}
