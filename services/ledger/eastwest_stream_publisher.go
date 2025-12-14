package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/robertodantas/lnpay/internal"
	ledgermodel "github.com/robertodantas/lnpay/proto/gen/model/ledger"
)

// EastWestStreamPublisher handles publishing messages to Redis streams for east-west communication
type EastWestStreamPublisher struct {
	streamClient *internal.StreamClient
}

// NewEastWestStreamPublisher creates a new east-west stream publisher
func NewEastWestStreamPublisher(streamClient *internal.StreamClient) *EastWestStreamPublisher {
	return &EastWestStreamPublisher{
		streamClient: streamClient,
	}
}

// PublishAuthorizationCreated publishes an AuthorizationCreated event to event.ledger
func (esp *EastWestStreamPublisher) PublishAuthorizationCreated(ctx context.Context, auth *ledgermodel.Authorization) error {
	event := &ledgermodel.AuthorizationCreatedEvent{
		Authorization: auth,
	}

	ledgerEvent := &ledgermodel.LedgerEvent{
		Type: ledgermodel.LedgerEventType_LEDGER_EVENT_TYPE_AUTHORIZATION_CREATED,
		Payload: &ledgermodel.LedgerEvent_AuthorizationCreated{
			AuthorizationCreated: event,
		},
	}

	deviceID := ""
	if auth != nil {
		deviceID = auth.DeviceId
	}

	return esp.publishLedgerEvent(ctx, ledgerEvent, deviceID)
}

// PublishAuthorizationCompleted publishes an AuthorizationCompleted event to event.ledger
func (esp *EastWestStreamPublisher) PublishAuthorizationCompleted(ctx context.Context, authorizationID, deviceID, timestamp string) error {
	event := &ledgermodel.AuthorizationCompletedEvent{
		AuthorizationId: authorizationID,
		DeviceId:        deviceID,
		Timestamp:       timestamp,
	}

	ledgerEvent := &ledgermodel.LedgerEvent{
		Type: ledgermodel.LedgerEventType_LEDGER_EVENT_TYPE_AUTHORIZATION_COMPLETED,
		Payload: &ledgermodel.LedgerEvent_AuthorizationCompleted{
			AuthorizationCompleted: event,
		},
	}

	return esp.publishLedgerEvent(ctx, ledgerEvent, deviceID)
}

// PublishAuthorizationExpired publishes an AuthorizationExpired event to event.ledger
func (esp *EastWestStreamPublisher) PublishAuthorizationExpired(ctx context.Context, authorizationID, deviceID, timestamp string) error {
	event := &ledgermodel.AuthorizationExpiredEvent{
		AuthorizationId: authorizationID,
		DeviceId:        deviceID,
		Timestamp:       timestamp,
	}

	ledgerEvent := &ledgermodel.LedgerEvent{
		Type: ledgermodel.LedgerEventType_LEDGER_EVENT_TYPE_AUTHORIZATION_EXPIRED,
		Payload: &ledgermodel.LedgerEvent_AuthorizationExpired{
			AuthorizationExpired: event,
		},
	}

	return esp.publishLedgerEvent(ctx, ledgerEvent, deviceID)
}

// PublishDeviceCredited publishes a DeviceCreditedEvent to event.ledger
func (esp *EastWestStreamPublisher) PublishDeviceCredited(ctx context.Context, deviceID string, amountMsat, newBalanceMsat int64, timestamp string) error {
	event := &ledgermodel.DeviceCreditedEvent{
		DeviceId:       deviceID,
		AmountMsat:     amountMsat,
		NewBalanceMsat: newBalanceMsat,
		Timestamp:      timestamp,
	}

	ledgerEvent := &ledgermodel.LedgerEvent{
		Type: ledgermodel.LedgerEventType_LEDGER_EVENT_TYPE_DEVICE_CREDITED,
		Payload: &ledgermodel.LedgerEvent_DeviceCredited{
			DeviceCredited: event,
		},
	}

	return esp.publishLedgerEvent(ctx, ledgerEvent, deviceID)
}

// PublishDeviceDebited publishes a DeviceDebitedEvent to event.ledger
func (esp *EastWestStreamPublisher) PublishDeviceDebited(ctx context.Context, deviceID, authorizationID string, amountMsat, newBalanceMsat int64, timestamp string) error {
	event := &ledgermodel.DeviceDebitedEvent{
		DeviceId:        deviceID,
		AuthorizationId: authorizationID,
		AmountMsat:      amountMsat,
		NewBalanceMsat:  newBalanceMsat,
		Timestamp:       timestamp,
	}

	ledgerEvent := &ledgermodel.LedgerEvent{
		Type: ledgermodel.LedgerEventType_LEDGER_EVENT_TYPE_DEVICE_DEBITED,
		Payload: &ledgermodel.LedgerEvent_DeviceDebited{
			DeviceDebited: event,
		},
	}

	return esp.publishLedgerEvent(ctx, ledgerEvent, deviceID)
}

// PublishAuthorizationDebited publishes an AuthorizationDebitedEvent to event.ledger
func (esp *EastWestStreamPublisher) PublishAuthorizationDebited(ctx context.Context, authorizationID, deviceID string, amountMsat, remainingMsat int64, timestamp string) error {
	event := &ledgermodel.AuthorizationDebitedEvent{
		AuthorizationId: authorizationID,
		DeviceId:        deviceID,
		AmountMsat:      amountMsat,
		RemainingMsat:   remainingMsat,
		Timestamp:       timestamp,
	}

	ledgerEvent := &ledgermodel.LedgerEvent{
		Type: ledgermodel.LedgerEventType_LEDGER_EVENT_TYPE_AUTHORIZATION_DEBITED,
		Payload: &ledgermodel.LedgerEvent_AuthorizationDebited{
			AuthorizationDebited: event,
		},
	}

	return esp.publishLedgerEvent(ctx, ledgerEvent, deviceID)
}

// PublishAuthorizationDebitFailed publishes an AuthorizationDebitFailedEvent to event.ledger
func (esp *EastWestStreamPublisher) PublishAuthorizationDebitFailed(ctx context.Context, authorizationID, deviceID string, requestedMsat, remainingMsat int64, reason, timestamp string) error {
	event := &ledgermodel.AuthorizationDebitFailedEvent{
		AuthorizationId: authorizationID,
		DeviceId:        deviceID,
		RequestedMsat:   requestedMsat,
		RemainingMsat:   remainingMsat,
		Reason:          reason,
		Timestamp:       timestamp,
	}

	ledgerEvent := &ledgermodel.LedgerEvent{
		Type: ledgermodel.LedgerEventType_LEDGER_EVENT_TYPE_AUTHORIZATION_DEBIT_FAILED,
		Payload: &ledgermodel.LedgerEvent_AuthorizationDebitFailed{
			AuthorizationDebitFailed: event,
		},
	}

	return esp.publishLedgerEvent(ctx, ledgerEvent, deviceID)
}

// publishLedgerEvent publishes a LedgerEvent to the event.ledger stream
func (esp *EastWestStreamPublisher) publishLedgerEvent(ctx context.Context, ledgerEvent *ledgermodel.LedgerEvent, deviceID string) error {
	// Serialize to JSON
	opts := protojson.MarshalOptions{UseProtoNames: true}
	jsonBytes, err := opts.Marshal(ledgerEvent)
	if err != nil {
		return fmt.Errorf("failed to marshal ledger event to JSON: %w", err)
	}

	// Publish to Redis stream "event.ledger"
	streamName := "event.ledger"
	values := map[string]interface{}{
		"event":     string(jsonBytes),
		"timestamp": time.Now().UnixMilli(),
	}

	// Use XADD to add entry to stream
	// Clean event type: "LEDGER_EVENT_TYPE_AUTHORIZATION_DEBITED" -> "AUTHORIZATION_DEBITED"
	eventTypeFull := ledgerEvent.GetType().String()
	eventType := eventTypeFull
	if len(eventTypeFull) > len("LEDGER_EVENT_TYPE_") && eventTypeFull[:len("LEDGER_EVENT_TYPE_")] == "LEDGER_EVENT_TYPE_" {
		eventType = eventTypeFull[len("LEDGER_EVENT_TYPE_"):]
	}
	streamID, err := esp.streamClient.XAddWithSpan(ctx, streamName, &redis.XAddArgs{
		Stream: streamName,
		Values: values,
	}, eventType)

	if err != nil {
		return fmt.Errorf("failed to publish to Redis stream %s: %w", streamName, err)
	}

	logEntry := logger.WithStream(streamName, "produce")
	if deviceID != "" {
		logEntry = logEntry.WithDeviceID(deviceID)
	}
	logEntry.InfoWithFields(ctx, "Published LedgerEvent", map[string]interface{}{
		"event_type": ledgerEvent.GetType().String(),
		"stream_id":  streamID,
	})
	return nil
}
