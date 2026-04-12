package main

import (
	"context"

	ledgermodel "github.com/robertodantas/lina/proto/gen/model/ledger"
	lightningmodel "github.com/robertodantas/lina/proto/gen/model/lightning"
	mqttpb "github.com/robertodantas/lina/proto/gen/model/mqtt"
)

// EastWestStreamHandler handles processing of Redis stream messages from east-west services
type EastWestStreamHandler struct {
	publisher *SouthboundPublisher
}

// NewEastWestStreamHandler creates a new east-west stream handler
func NewEastWestStreamHandler(publisher *SouthboundPublisher) *EastWestStreamHandler {
	return &EastWestStreamHandler{
		publisher: publisher,
	}
}

// HandleDeviceCredited processes device credited events
func (esh *EastWestStreamHandler) HandleDeviceCredited(ctx context.Context, payload *ledgermodel.DeviceCreditedEvent) error {
	return esh.publisher.PublishBalanceUpdate(ctx, payload.GetDeviceId(), payload.GetNewBalanceMsat(), payload.GetTimestamp())
}

// HandleDeviceDebited processes device debited events
func (esh *EastWestStreamHandler) HandleDeviceDebited(ctx context.Context, payload *ledgermodel.DeviceDebitedEvent) error {
	logger.WithDeviceID(payload.GetDeviceId()).
		DebugWithFields(ctx, "Device debited via eastwest gRPC", map[string]interface{}{
			"authorization_id": payload.GetAuthorizationId(),
			"amount_msat":      payload.GetAmountMsat(),
			"new_balance_msat": payload.GetNewBalanceMsat(),
		})
	return esh.publisher.PublishBalanceUpdate(ctx, payload.GetDeviceId(), payload.GetNewBalanceMsat(), payload.GetTimestamp())
}

// HandleAuthorizationCompleted processes authorization completed events
func (esh *EastWestStreamHandler) HandleAuthorizationCompleted(ctx context.Context, payload *ledgermodel.AuthorizationCompletedEvent) error {
	// "REPLENISH" distinguishes this path from authorization *status* strings and from debit-failure reasons.
	return esh.publisher.PublishControlCommandWithAuthID(ctx, payload.GetDeviceId(), mqttpb.ControlCommand_CONTROL_COMMAND_AUTHORIZATION, "REPLENISH", payload.GetAuthorizationId())
}

// HandleAuthorizationExpired processes authorization expired events
func (esh *EastWestStreamHandler) HandleAuthorizationExpired(ctx context.Context, payload *ledgermodel.AuthorizationExpiredEvent) error {
	return esh.publisher.PublishControlCommandWithAuthID(ctx, payload.GetDeviceId(), mqttpb.ControlCommand_CONTROL_COMMAND_AUTHORIZATION, "EXPIRED", payload.GetAuthorizationId())
}

// HandleAuthorizationDebitFailed processes authorization debit failed events
func (esh *EastWestStreamHandler) HandleAuthorizationDebitFailed(ctx context.Context, payload *ledgermodel.AuthorizationDebitFailedEvent) error {
	reason := payload.GetReason()
	if reason == "" {
		reason = "NO_ACTIVE_AUTHORIZATION"
	}
	logger.WithDeviceID(payload.GetDeviceId()).
		WarnWithFields(ctx, "Authorization debit failed via eastwest gRPC", map[string]interface{}{
			"authorization_id": payload.GetAuthorizationId(),
			"reason":           reason,
			"requested_msat":   payload.GetRequestedMsat(),
			"remaining_msat":   payload.GetRemainingMsat(),
		})
	// Same reason is sent on MQTT control and echoed in AuthorizeRequest so logs match ledger (e.g. NO_ACTIVE_AUTHORIZATION).
	return esh.publisher.PublishControlCommandWithAuthID(ctx, payload.GetDeviceId(), mqttpb.ControlCommand_CONTROL_COMMAND_AUTHORIZATION, reason, payload.GetAuthorizationId())
}

// HandleInvoiceSettled processes invoice settled events
func (esh *EastWestStreamHandler) HandleInvoiceSettled(ctx context.Context, payload *lightningmodel.InvoiceSettledEvent) error {
	logger.WithDeviceID(payload.GetDeviceId()).
		DebugWithFields(ctx, "Processing InvoiceSettled event from lightning stream", map[string]interface{}{
			"invoice_id":  payload.GetInvoiceId(),
			"amount_msat": payload.GetAmountReceivedMsat(),
		})
	// Publish invoice event
	if err := esh.publisher.PublishInvoiceEvent(ctx, payload.GetDeviceId(), payload.GetInvoiceId(), mqttpb.InvoiceStatus_INVOICE_STATUS_SETTLED, payload.GetAmountReceivedMsat(), payload.GetNewBalanceMsat(), payload.GetTimestamp()); err != nil {
		return err
	}
	// Note: Balance update will be published when DEVICE_CREDITED event is received from ledger service
	// Send RESUME command after invoice settlement to allow device to resume operation
	if err := esh.publisher.PublishControlCommand(ctx, payload.GetDeviceId(), mqttpb.ControlCommand_CONTROL_COMMAND_RESUME, "INVOICE_SETTLED"); err != nil {
		logger.WithDeviceID(payload.GetDeviceId()).
			Error(ctx, "Error publishing RESUME control command after invoice settlement", err)
		// Don't return error - invoice event was already published successfully
	}
	return nil
}

// HandleInvoiceExpired processes invoice expired events
func (esh *EastWestStreamHandler) HandleInvoiceExpired(ctx context.Context, payload *lightningmodel.InvoiceExpiredEvent) error {
	logger.WithDeviceID(payload.GetDeviceId()).
		DebugWithFields(ctx, "Processing InvoiceExpired event from lightning stream", map[string]interface{}{
			"invoice_id": payload.GetInvoiceId(),
		})
	return esh.publisher.PublishInvoiceEvent(ctx, payload.GetDeviceId(), payload.GetInvoiceId(), mqttpb.InvoiceStatus_INVOICE_STATUS_EXPIRED, 0, 0, payload.GetTimestamp())
}
