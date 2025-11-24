package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	mqttpb "github.com/robertodantas/lnpay/proto/gen/gen/iot/payperuse/edge/model/mqtt"
)

// SouthboundInterface handles MQTT subscriptions for device messages
type SouthboundInterface struct {
	mqttClient *MQTTClient
}

// NewSouthboundInterface creates a new southbound interface
func NewSouthboundInterface(mqttClient *MQTTClient) *SouthboundInterface {
	return &SouthboundInterface{
		mqttClient: mqttClient,
	}
}

// Start initializes all MQTT subscriptions for the southbound interface
func (sb *SouthboundInterface) Start() error {
	// Subscribe to heartbeat topic: /devices/#/heartbeat
	if err := sb.mqttClient.Subscribe("/devices/+/heartbeat", 1, sb.handleHeartbeat); err != nil {
		return fmt.Errorf("failed to subscribe to heartbeat topic: %w", err)
	}

	// Subscribe to usage topic: /devices/#/usage
	if err := sb.mqttClient.Subscribe("/devices/+/usage", 1, sb.handleUsage); err != nil {
		return fmt.Errorf("failed to subscribe to usage topic: %w", err)
	}

	// Subscribe to authorization request topic: /devices/#/request/authorize
	if err := sb.mqttClient.Subscribe("/devices/+/request/authorize", 1, sb.handleAuthorizationRequest); err != nil {
		return fmt.Errorf("failed to subscribe to authorization request topic: %w", err)
	}

	// Subscribe to invoice request topic: /devices/#/request/invoice
	if err := sb.mqttClient.Subscribe("/devices/+/request/invoice", 1, sb.handleInvoiceRequest); err != nil {
		return fmt.Errorf("failed to subscribe to invoice request topic: %w", err)
	}

	log.Println("Southbound interface started - all subscriptions active")
	return nil
}

// extractDeviceID extracts the device ID from an MQTT topic path
// Topics are in format: /devices/{deviceId}/...
func extractDeviceID(topic string) string {
	parts := strings.Split(strings.TrimPrefix(topic, "/"), "/")
	if len(parts) >= 2 && parts[0] == "devices" {
		return parts[1]
	}
	return ""
}

// handleHeartbeat processes heartbeat messages from devices
func (sb *SouthboundInterface) handleHeartbeat(client mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()
	deviceID := extractDeviceID(topic)

	var payload mqttpb.HeartbeatPayload
	if err := payload.UnmarshalJSON(msg.Payload()); err != nil {
		log.Printf("Error parsing heartbeat payload from device %s: %v", deviceID, err)
		return
	}

	log.Printf("[HEARTBEAT] Device: %s, Status: %s, Timestamp: %s",
		payload.GetDeviceId(),
		payload.GetStatus().String(),
		payload.GetTimestamp(),
	)
}

// handleUsage processes usage messages from devices
func (sb *SouthboundInterface) handleUsage(client mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()
	deviceID := extractDeviceID(topic)

	var payload mqttpb.UsagePayload
	if err := payload.UnmarshalJSON(msg.Payload()); err != nil {
		log.Printf("Error parsing usage payload from device %s: %v", deviceID, err)
		return
	}

	log.Printf("[USAGE] Device: %s, ReportID: %s, Strategy: %s, Measure: %.2f %s, Timestamp: %s",
		payload.GetDeviceId(),
		payload.GetReportId(),
		payload.GetStrategy().String(),
		payload.GetMeasure(),
		payload.GetUnit(),
		payload.GetTimestamp(),
	)
}

// handleAuthorizationRequest processes authorization requests from devices
func (sb *SouthboundInterface) handleAuthorizationRequest(client mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()
	deviceID := extractDeviceID(topic)

	var request mqttpb.AuthorizationRequestPayload
	// AuthorizationRequestPayload doesn't have enums, so we can use protojson.Unmarshal directly
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	if err := opts.Unmarshal(msg.Payload(), &request); err != nil {
		log.Printf("Error parsing authorization request payload from device %s: %v", deviceID, err)
		return
	}

	log.Printf("[AUTHORIZATION REQUEST] Device: %s, RequestID: %s, RequestMsat: %d, Reason: %s, Timestamp: %s",
		request.GetDeviceId(),
		request.GetRequestId(),
		request.GetRequestMsat(),
		request.GetReason(),
		request.GetTimestamp(),
	)

	// Create a placeholder response
	response := &mqttpb.AuthorizationResponsePayload{
		DeviceId:  request.GetDeviceId(),
		RequestId: request.GetRequestId(),
		Status:    mqttpb.AuthorizationStatus_AUTHORIZATION_STATUS_GRANTED,
		// TODO: Set actual values when processing is implemented
		AuthorizationId: "placeholder-auth-id",
		GrantedMsat:     request.GetRequestMsat(),
		RemainingMsat:   request.GetRequestMsat(),
		IssuedAt:        time.Now().Format(time.RFC3339),
		ExpiresAt:       time.Now().Add(1 * time.Hour).Format(time.RFC3339),
	}

	// Serialize response to JSON with short enum names
	responseJSON, err := json.Marshal(response)
	if err != nil {
		log.Printf("Error marshaling authorization response: %v", err)
		return
	}

	// Publish response to /devices/{deviceId}/response/authorize
	responseTopic := fmt.Sprintf("/devices/%s/response/authorize", deviceID)
	if err := sb.mqttClient.Publish(responseTopic, 1, false, responseJSON); err != nil {
		log.Printf("Error publishing authorization response to device %s: %v", deviceID, err)
		return
	}

	log.Printf("[AUTHORIZATION RESPONSE] Published to %s", responseTopic)
}

// handleInvoiceRequest processes invoice requests from devices
func (sb *SouthboundInterface) handleInvoiceRequest(client mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()
	deviceID := extractDeviceID(topic)

	var request mqttpb.InvoiceRequestPayload
	// InvoiceRequestPayload doesn't have enums, so we can use protojson.Unmarshal directly
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	if err := opts.Unmarshal(msg.Payload(), &request); err != nil {
		log.Printf("Error parsing invoice request payload from device %s: %v", deviceID, err)
		return
	}

	log.Printf("[INVOICE REQUEST] Device: %s, RequestID: %s, AmountMsat: %d, Reason: %s, Timestamp: %s",
		request.GetDeviceId(),
		request.GetRequestId(),
		request.GetAmountMsat(),
		request.GetReason(),
		request.GetTimestamp(),
	)

	// Create a placeholder response
	response := &mqttpb.InvoiceResponsePayload{
		DeviceId:   request.GetDeviceId(),
		RequestId:  request.GetRequestId(),
		Status:     mqttpb.InvoiceStatus_INVOICE_STATUS_CREATED,
		InvoiceId:  "placeholder-invoice-id",
		Bolt11:     "placeholder-bolt11-invoice",
		AmountMsat: request.GetAmountMsat(),
		ExpiresAt:  time.Now().Add(1 * time.Hour).Format(time.RFC3339),
	}

	// Serialize response to JSON with short enum names
	responseJSON, err := json.Marshal(response)
	if err != nil {
		log.Printf("Error marshaling invoice response: %v", err)
		return
	}

	// Publish response to /devices/{deviceId}/response/invoice
	responseTopic := fmt.Sprintf("/devices/%s/response/invoice", deviceID)
	if err := sb.mqttClient.Publish(responseTopic, 1, false, responseJSON); err != nil {
		log.Printf("Error publishing invoice response to device %s: %v", deviceID, err)
		return
	}

	log.Printf("[INVOICE RESPONSE] Published to %s", responseTopic)
}
