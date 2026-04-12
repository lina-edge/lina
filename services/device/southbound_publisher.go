package main

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"

	mqttpb "github.com/robertodantas/lina/proto/gen/model/mqtt"
)

// SouthboundPublisher handles publishing messages to devices via MQTT
type SouthboundPublisher struct {
	mqttClient *MQTTClient
}

// NewSouthboundPublisher creates a new southbound publisher
func NewSouthboundPublisher(mqttClient *MQTTClient) *SouthboundPublisher {
	return &SouthboundPublisher{
		mqttClient: mqttClient,
	}
}

// PublishControlCommand publishes a control command to the device
func (sp *SouthboundPublisher) PublishControlCommand(ctx context.Context, deviceID string, command mqttpb.ControlCommand, reason string) error {
	if deviceID == "" {
		return fmt.Errorf("device ID is required")
	}

	payload := &mqttpb.ControlPayload{
		Command: command,
		Reason:  reason,
	}

	marshalOpts := protojson.MarshalOptions{UseProtoNames: true}
	msgBytes, err := marshalOpts.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal control payload: %w", err)
	}

	topic := fmt.Sprintf("/devices/%s/control", deviceID)
	if err := sp.mqttClient.Publish(ctx, topic, 1, false, msgBytes); err != nil {
		return fmt.Errorf("failed to publish control command to MQTT: %w", err)
	}

	logger.WithDeviceID(deviceID).
		DebugWithFields(ctx, "Published control command on southbound mqtt", map[string]interface{}{
			"command": command.String(),
			"reason":  reason,
		})
	return nil
}

// PublishAuthorizationResponse publishes an authorization response to the device
func (sp *SouthboundPublisher) PublishAuthorizationResponse(ctx context.Context, deviceID string, response *mqttpb.AuthorizationResponsePayload) error {
	marshalOpts := protojson.MarshalOptions{UseProtoNames: true}
	responseJSON, err := marshalOpts.Marshal(response)
	if err != nil {
		return fmt.Errorf("failed to marshal authorization response: %w", err)
	}

	responseTopic := fmt.Sprintf("/devices/%s/response/authorize", deviceID)
	if err := sp.mqttClient.Publish(ctx, responseTopic, 1, false, responseJSON); err != nil {
		return fmt.Errorf("failed to publish authorization response: %w", err)
	}

	logger.WithDeviceID(deviceID).
		DebugWithFields(ctx, "Authorization response published on southbound mqtt", map[string]interface{}{
			"topic":  responseTopic,
			"status": response.Status.String(),
		})
	return nil
}

// PublishInvoiceResponse publishes an invoice response to the device
func (sp *SouthboundPublisher) PublishInvoiceResponse(ctx context.Context, deviceID string, response *mqttpb.InvoiceResponsePayload) error {
	marshalOpts := protojson.MarshalOptions{UseProtoNames: true}
	responseJSON, err := marshalOpts.Marshal(response)
	if err != nil {
		return fmt.Errorf("failed to marshal invoice response: %w", err)
	}

	responseTopic := fmt.Sprintf("/devices/%s/response/invoice", deviceID)
	if err := sp.mqttClient.Publish(ctx, responseTopic, 1, false, responseJSON); err != nil {
		return fmt.Errorf("failed to publish invoice response: %w", err)
	}

	logger.WithDeviceID(deviceID).
		DebugWithFields(ctx, "Invoice response published on southbound mqtt", map[string]interface{}{
			"topic":      responseTopic,
			"invoice_id": response.InvoiceId,
			"status":     response.Status.String(),
		})
	return nil
}

// PublishControlCommandWithAuthID publishes a control command with authorization ID to the device
func (sp *SouthboundPublisher) PublishControlCommandWithAuthID(ctx context.Context, deviceID string, command mqttpb.ControlCommand, reason string, authorizationID string) error {
	if deviceID == "" {
		return fmt.Errorf("device ID is required")
	}

	payload := &mqttpb.ControlPayload{
		Command:         command,
		Reason:          reason,
		AuthorizationId: authorizationID,
	}

	marshalOpts := protojson.MarshalOptions{UseProtoNames: true}
	msgBytes, err := marshalOpts.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal control payload: %w", err)
	}

	topic := fmt.Sprintf("/devices/%s/control", deviceID)
	if err := sp.mqttClient.Publish(ctx, topic, 1, false, msgBytes); err != nil {
		return fmt.Errorf("failed to publish control command to MQTT: %w", err)
	}

	logger.WithDeviceID(deviceID).
		DebugWithFields(ctx, "Published control command on southbound mqtt", map[string]interface{}{
			"command":          command.String(),
			"reason":           reason,
			"authorization_id": authorizationID,
		})
	return nil
}

// PublishBalanceUpdate publishes a balance update to the device
func (sp *SouthboundPublisher) PublishBalanceUpdate(ctx context.Context, deviceID string, availableMsat int64, timestamp string) error {
	if deviceID == "" {
		return fmt.Errorf("device ID is required")
	}

	payload := &mqttpb.BalancePayload{
		DeviceId:      deviceID,
		AvailableMsat: availableMsat,
		ReservedMsat:  0,
		TotalMsat:     availableMsat,
		Timestamp:     timestamp,
	}

	marshalOpts := protojson.MarshalOptions{UseProtoNames: true}
	msgBytes, err := marshalOpts.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal balance payload: %w", err)
	}

	topic := fmt.Sprintf("/devices/%s/balance", deviceID)
	if err := sp.mqttClient.Publish(ctx, topic, 1, true, msgBytes); err != nil {
		return fmt.Errorf("failed to publish balance to MQTT: %w", err)
	}

	logger.WithDeviceID(deviceID).
		DebugWithFields(ctx, "Published updated balance on southbound mqtt", map[string]interface{}{
			"available_msat": availableMsat,
		})
	return nil
}

// PublishInvoiceEvent publishes an invoice event to the device
func (sp *SouthboundPublisher) PublishInvoiceEvent(ctx context.Context, deviceID string, invoiceID string, status mqttpb.InvoiceStatus, amountReceivedMsat int64, balanceMsat int64, timestamp string) error {
	if deviceID == "" {
		return fmt.Errorf("device ID is required")
	}

	payload := &mqttpb.InvoiceEventPayload{
		DeviceId:  deviceID,
		InvoiceId: invoiceID,
		Status:    status,
		Timestamp: timestamp,
	}

	// Only set amount and balance for settled invoices
	if status == mqttpb.InvoiceStatus_INVOICE_STATUS_SETTLED {
		payload.AmountReceivedMsat = amountReceivedMsat
		payload.BalanceMsat = balanceMsat
	}

	marshalOpts := protojson.MarshalOptions{UseProtoNames: true}
	msgBytes, err := marshalOpts.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal invoice event payload: %w", err)
	}

	topic := fmt.Sprintf("/devices/%s/events/invoice", deviceID)
	if err := sp.mqttClient.Publish(ctx, topic, 1, false, msgBytes); err != nil {
		return fmt.Errorf("failed to publish invoice event to MQTT: %w", err)
	}

	logger.WithDeviceID(deviceID).
		DebugWithFields(ctx, "Published invoice event on southbound mqtt", map[string]interface{}{
			"invoice_id": invoiceID,
			"status":     status.String(),
		})
	return nil
}
