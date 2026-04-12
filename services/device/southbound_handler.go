package main

import (
	"context"
	"fmt"
	"time"

	ledgermodel "github.com/robertodantas/lina/proto/gen/model/ledger"
	lightningmodel "github.com/robertodantas/lina/proto/gen/model/lightning"
	mqttpb "github.com/robertodantas/lina/proto/gen/model/mqtt"
)

// SouthboundHandler handles processing of incoming MQTT messages from devices
type SouthboundHandler struct {
	publisher       *SouthboundPublisher
	streamPublisher *EastWestStreamPublisher
	ledgerClient    *LedgerClient
	lightningClient *LightningClient
	repo            *DeviceRepository
	invoiceTimeout  time.Duration
}

// NewSouthboundHandler creates a new southbound message handler
func NewSouthboundHandler(publisher *SouthboundPublisher, streamPublisher *EastWestStreamPublisher, ledgerClient *LedgerClient, lightningClient *LightningClient, repo *DeviceRepository, invoiceTimeout time.Duration) *SouthboundHandler {
	if invoiceTimeout <= 0 {
		invoiceTimeout = 30 * time.Second
	}

	return &SouthboundHandler{
		publisher:       publisher,
		streamPublisher: streamPublisher,
		ledgerClient:    ledgerClient,
		lightningClient: lightningClient,
		repo:            repo,
		invoiceTimeout:  invoiceTimeout,
	}
}

// HandleHeartbeat processes heartbeat messages from devices
func (sh *SouthboundHandler) HandleHeartbeat(ctx context.Context, deviceID string, payload *mqttpb.HeartbeatPayload) {
	logger.WithDeviceID(payload.GetDeviceId()).
		DebugWithFields(ctx, "Heartbeat received on southbound mqtt", map[string]interface{}{
			"status":    payload.GetStatus().String(),
			"timestamp": payload.GetTimestamp(),
		})
}

// HandleUsage processes usage messages from devices
func (sh *SouthboundHandler) HandleUsage(ctx context.Context, deviceID string, payload *mqttpb.UsagePayload) {
	logger.WithDeviceID(payload.GetDeviceId()).
		DebugWithFields(ctx, "Usage received on southbound mqtt", map[string]interface{}{
			"report_id": payload.GetReportId(),
			"strategy":  payload.GetStrategy().String(),
			"measure":   payload.GetMeasure(),
			"unit":      payload.GetUnit(),
			"timestamp": payload.GetTimestamp(),
		})

	// Publish DeviceUsageReportedEvent to Redis stream (with price_per_unit from device config)
	if err := sh.streamPublisher.PublishDeviceUsageReportedEvent(ctx, payload, sh.repo); err != nil {
		logger.WithDeviceID(payload.GetDeviceId()).
			WithStream("event.device", "produce").
			Error(ctx, "Error publishing usage event to Redis stream on southbound mqtt", err)
		return
	}
}

// HandleAuthorizationRequest processes authorization requests from devices
func (sh *SouthboundHandler) HandleAuthorizationRequest(ctx context.Context, deviceID string, request *mqttpb.AuthorizationRequestPayload) {
	logger.WithDeviceID(request.GetDeviceId()).
		DebugWithFields(ctx, "Authorization request received on southbound mqtt", map[string]interface{}{
			"request_id":   request.GetRequestId(),
			"request_msat": request.GetRequestMsat(),
			"reason":       request.GetReason(),
			"timestamp":    request.GetTimestamp(),
		})

	// Call ledger service via gRPC
	grpcCtx, cancel := context.WithTimeout(ctx, sh.invoiceTimeout)
	defer cancel()

	ledgerResp, err := sh.ledgerClient.CreateOrGetAuthorization(
		grpcCtx,
		request.GetDeviceId(),
		request.GetRequestId(),
		request.GetRequestMsat(),
		request.GetReason(),
	)
	if err != nil {
		logger.WithDeviceID(deviceID).
			Error(ctx, "Error calling ledger service on southbound mqtt", err)

		// Send error response to device
		response := &mqttpb.AuthorizationResponsePayload{
			DeviceId:  request.GetDeviceId(),
			RequestId: request.GetRequestId(),
			Status:    mqttpb.AuthorizationStatus_AUTHORIZATION_STATUS_REJECTED,
			Reason:    fmt.Sprintf("Failed to process authorization: %v", err),
		}
		if err := sh.publisher.PublishAuthorizationResponse(ctx, deviceID, response); err != nil {
			logger.WithDeviceID(deviceID).
				Error(ctx, "Error publishing authorization error response on southbound mqtt", err)
		}
		// Publish STOP control command when authorization is rejected due to error
		if err := sh.publisher.PublishControlCommand(ctx, deviceID, mqttpb.ControlCommand_CONTROL_COMMAND_STOP, fmt.Sprintf("Failed to process authorization: %v", err)); err != nil {
			logger.WithDeviceID(deviceID).
				Error(ctx, "Error publishing STOP control command after authorization error on southbound mqtt", err)
		}
		return
	}

	// Map ledger response to MQTT response payload
	response := &mqttpb.AuthorizationResponsePayload{
		DeviceId:  request.GetDeviceId(),
		RequestId: request.GetRequestId(),
		Status:    mapLedgerStatusToMQTTStatus(ledgerResp.GetStatus()),
		Reason:    ledgerResp.GetReason(),
	}

	// If authorization was granted or is active, include authorization details
	if ledgerResp.GetAuthorization() != nil {
		auth := ledgerResp.GetAuthorization()
		response.AuthorizationId = auth.GetAuthorizationId()
		response.GrantedMsat = auth.GetGrantedMsat()
		response.RemainingMsat = auth.GetRemainingMsat()
		response.IssuedAt = auth.GetIssuedAt()
		response.ExpiresAt = auth.GetExpiresAt()
	}

	// Include available balance if provided
	if ledgerResp.GetAvailableMsat() > 0 {
		response.AvailableMsat = ledgerResp.GetAvailableMsat()
	}

	// Publish response
	if err := sh.publisher.PublishAuthorizationResponse(ctx, deviceID, response); err != nil {
		logger.WithDeviceID(deviceID).
			Error(ctx, "Error publishing authorization response on southbound mqtt", err)
		return
	}

	// If authorization was granted or is active, publish RESUME control command
	if response.Status == mqttpb.AuthorizationStatus_AUTHORIZATION_STATUS_GRANTED ||
		response.Status == mqttpb.AuthorizationStatus_AUTHORIZATION_STATUS_ACTIVE {
		reason := response.Reason
		if reason == "" {
			reason = "AUTHORIZATION_GRANTED"
		}
		if err := sh.publisher.PublishControlCommand(ctx, deviceID, mqttpb.ControlCommand_CONTROL_COMMAND_RESUME, reason); err != nil {
			logger.WithDeviceID(deviceID).
				Error(ctx, "Error publishing RESUME control command after authorization grant on southbound mqtt", err)
		}
	}

	// If authorization was rejected, publish STOP control command to halt the device
	if response.Status == mqttpb.AuthorizationStatus_AUTHORIZATION_STATUS_REJECTED {
		reason := response.Reason
		if reason == "" {
			reason = "AUTHORIZATION_REJECTED"
		}
		if err := sh.publisher.PublishControlCommand(ctx, deviceID, mqttpb.ControlCommand_CONTROL_COMMAND_STOP, reason); err != nil {
			logger.WithDeviceID(deviceID).
				Error(ctx, "Error publishing STOP control command after authorization rejection on southbound mqtt", err)
		}
	}
}

// HandleInvoiceRequest processes invoice requests from devices
func (sh *SouthboundHandler) HandleInvoiceRequest(ctx context.Context, deviceID string, request *mqttpb.InvoiceRequestPayload) {
	logger.WithDeviceID(request.GetDeviceId()).
		DebugWithFields(ctx, "Invoice request received on southbound mqtt", map[string]interface{}{
			"request_id":  request.GetRequestId(),
			"amount_msat": request.GetAmountMsat(),
			"reason":      request.GetReason(),
			"timestamp":   request.GetTimestamp(),
		})

	if sh.lightningClient == nil {
		logger.WithDeviceID(deviceID).
			Warn(ctx, "Lightning client not initialized on southbound mqtt; cannot process invoice request")
		return
	}

	grpcCtx, cancel := context.WithTimeout(ctx, sh.invoiceTimeout)
	defer cancel()

	lightningResp, err := sh.lightningClient.CreateInvoice(
		grpcCtx,
		request.GetDeviceId(),
		request.GetAmountMsat(),
		request.GetReason(),
	)
	if err != nil {
		logger.WithDeviceID(deviceID).
			Error(ctx, "Error calling lightning service on southbound mqtt", err)

		// Send error response
		response := &mqttpb.InvoiceResponsePayload{
			DeviceId:  request.GetDeviceId(),
			RequestId: request.GetRequestId(),
			Status:    mqttpb.InvoiceStatus_INVOICE_STATUS_FAILED,
		}
		if err := sh.publisher.PublishInvoiceResponse(ctx, deviceID, response); err != nil {
			logger.WithDeviceID(deviceID).
				Error(ctx, "Error publishing invoice error response on southbound mqtt", err)
		}
		return
	}

	invoice := lightningResp.GetInvoice()
	if invoice == nil {
		logger.WithDeviceID(deviceID).
			Warn(ctx, "Lightning service returned empty invoice on southbound mqtt")
		return
	}

	response := &mqttpb.InvoiceResponsePayload{
		DeviceId:   request.GetDeviceId(),
		RequestId:  request.GetRequestId(),
		Status:     mapLightningStatusToMQTTStatus(invoice.GetStatus()),
		InvoiceId:  invoice.GetInvoiceId(),
		Bolt11:     invoice.GetBolt11(),
		AmountMsat: invoice.GetAmountMsat(),
		ExpiresAt:  invoice.GetExpiresAt(),
	}

	// Publish response
	if err := sh.publisher.PublishInvoiceResponse(ctx, deviceID, response); err != nil {
		logger.WithDeviceID(deviceID).
			Error(ctx, "Error publishing invoice response on southbound mqtt", err)
		return
	}
}

// mapLedgerStatusToMQTTStatus maps ledger AuthorizationStatus to MQTT AuthorizationStatus
func mapLedgerStatusToMQTTStatus(status ledgermodel.AuthorizationStatus) mqttpb.AuthorizationStatus {
	// Both enums have the same values, so we can convert directly
	return mqttpb.AuthorizationStatus(status)
}

// mapLightningStatusToMQTTStatus maps lightning InvoiceStatus to MQTT InvoiceStatus
func mapLightningStatusToMQTTStatus(status lightningmodel.InvoiceStatus) mqttpb.InvoiceStatus {
	return mqttpb.InvoiceStatus(status)
}
