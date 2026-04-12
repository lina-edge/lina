package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	mqttpb "github.com/robertodantas/lina/proto/gen/model/mqtt"
)

// SouthboundInterface handles MQTT subscriptions and message decoding for device messages
type SouthboundInterface struct {
	mqttClient *MQTTClient
	handler    *SouthboundHandler
}

// NewSouthboundInterface creates a new southbound interface
func NewSouthboundInterface(mqttClient *MQTTClient, streamInterface *EastWestStreamInterface, ledgerClient *LedgerClient, lightningClient *LightningClient, repo *DeviceRepository, invoiceTimeout time.Duration) *SouthboundInterface {
	// Create publisher for sending messages to devices via MQTT
	publisher := NewSouthboundPublisher(mqttClient)

	// Create stream publisher for publishing to Redis streams
	streamPublisher := NewEastWestStreamPublisher(streamInterface)

	// Create handler for processing incoming messages
	handler := NewSouthboundHandler(publisher, streamPublisher, ledgerClient, lightningClient, repo, invoiceTimeout)

	return &SouthboundInterface{
		mqttClient: mqttClient,
		handler:    handler,
	}
}

// Start initializes all MQTT subscriptions for the southbound interface
func (sb *SouthboundInterface) Start(ctx context.Context) error {
	// Subscribe to heartbeat topic: /devices/#/heartbeat
	if err := sb.mqttClient.Subscribe(ctx, "/devices/+/heartbeat", 1, sb.handleHeartbeatMessage); err != nil {
		return fmt.Errorf("failed to subscribe to heartbeat topic: %w", err)
	}

	// Subscribe to usage topic: /devices/#/usage
	if err := sb.mqttClient.Subscribe(ctx, "/devices/+/usage", 1, sb.handleUsageMessage); err != nil {
		return fmt.Errorf("failed to subscribe to usage topic: %w", err)
	}

	// Subscribe to authorization request topic: /devices/#/request/authorize
	if err := sb.mqttClient.Subscribe(ctx, "/devices/+/request/authorize", 1, sb.handleAuthorizationRequestMessage); err != nil {
		return fmt.Errorf("failed to subscribe to authorization request topic: %w", err)
	}

	// Subscribe to invoice request topic: /devices/#/request/invoice
	if err := sb.mqttClient.Subscribe(ctx, "/devices/+/request/invoice", 1, sb.handleInvoiceRequestMessage); err != nil {
		return fmt.Errorf("failed to subscribe to invoice request topic: %w", err)
	}

	logger.Info(ctx, "Southbound interface started - all subscriptions active on southbound mqtt")
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

// handleHeartbeatMessage decodes MQTT message and calls callback with clean payload
func (sb *SouthboundInterface) handleHeartbeatMessage(ctx context.Context, client mqtt.Client, msg mqtt.Message) error {
	topic := msg.Topic()
	deviceID := extractDeviceID(topic)

	var payload mqttpb.HeartbeatPayload
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	if err := opts.Unmarshal(msg.Payload(), &payload); err != nil {
		logger.WithDeviceID(deviceID).
			Error(ctx, "Error parsing heartbeat payload on southbound mqtt", err)
		return err
	}

	sb.handler.HandleHeartbeat(ctx, deviceID, &payload)
	return nil
}

// handleUsageMessage decodes MQTT message and calls callback with clean payload
func (sb *SouthboundInterface) handleUsageMessage(ctx context.Context, client mqtt.Client, msg mqtt.Message) error {
	// Copy payload since we'll be processing in a goroutine and the original may be reused
	payloadBytes := make([]byte, len(msg.Payload()))
	copy(payloadBytes, msg.Payload())
	topic := msg.Topic()
	deviceID := extractDeviceID(topic)

	var payload mqttpb.UsagePayload
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	if err := opts.Unmarshal(payloadBytes, &payload); err != nil {
		logger.WithDeviceID(deviceID).
			Error(ctx, "Error parsing usage payload on southbound mqtt", err)
		return err
	}

	sb.handler.HandleUsage(ctx, deviceID, &payload)
	return nil
}

// handleAuthorizationRequestMessage decodes MQTT message and calls callback with clean payload
func (sb *SouthboundInterface) handleAuthorizationRequestMessage(ctx context.Context, client mqtt.Client, msg mqtt.Message) error {
	// Copy payload since we'll be processing in a goroutine and the original may be reused
	payloadBytes := make([]byte, len(msg.Payload()))
	copy(payloadBytes, msg.Payload())
	topic := msg.Topic()
	deviceID := extractDeviceID(topic)

	// Log that we received a message on this topic
	logger.WithDeviceID(deviceID).
		DebugWithFields(ctx, "Received message on authorization request topic on southbound mqtt", map[string]interface{}{
			"topic":        topic,
			"payload_size": len(payloadBytes),
			"payload":      string(payloadBytes),
		})

	var payload mqttpb.AuthorizationRequestPayload
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	if err := opts.Unmarshal(payloadBytes, &payload); err != nil {
		logger.WithDeviceID(deviceID).
			WithFields(map[string]interface{}{
				"payload": string(payloadBytes),
			}).
			Error(ctx, "Error parsing authorization request payload on southbound mqtt", err)
		return err
	}

	sb.handler.HandleAuthorizationRequest(ctx, deviceID, &payload)
	return nil
}

// handleInvoiceRequestMessage decodes MQTT message and calls callback with clean payload
func (sb *SouthboundInterface) handleInvoiceRequestMessage(ctx context.Context, client mqtt.Client, msg mqtt.Message) error {
	// Copy payload since we'll be processing in a goroutine and the original may be reused
	payloadBytes := make([]byte, len(msg.Payload()))
	copy(payloadBytes, msg.Payload())
	topic := msg.Topic()
	deviceID := extractDeviceID(topic)

	var payload mqttpb.InvoiceRequestPayload
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	if err := opts.Unmarshal(payloadBytes, &payload); err != nil {
		logger.WithDeviceID(deviceID).
			Error(ctx, "Error parsing invoice request payload on southbound mqtt", err)
		return err
	}

	sb.handler.HandleInvoiceRequest(ctx, deviceID, &payload)
	return nil
}
