package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/robertodantas/lina/internal"
	ledgermodel "github.com/robertodantas/lina/proto/gen/model/ledger"
	lightningmodel "github.com/robertodantas/lina/proto/gen/model/lightning"
)

// EastWestStreamInterface wraps the internal StreamClient with device-specific methods for east-west stream communication
type EastWestStreamInterface struct {
	*internal.StreamClient
}

// NewEastWestStreamInterface creates a new Redis stream client using the internal package
func NewEastWestStreamInterface(ctx context.Context) (*EastWestStreamInterface, error) {
	libClient, err := internal.NewStreamClientFromEnv(ctx)
	if err != nil {
		return nil, err
	}

	return &EastWestStreamInterface{
		StreamClient: libClient,
	}, nil
}

// StartLedgerBalanceSubscriber listens for ledger balance events and forwards updates via MQTT
func (ewsi *EastWestStreamInterface) StartLedgerBalanceSubscriber(ctx context.Context, publisher *SouthboundPublisher) {
	// Create handler for processing ledger events
	handler := NewEastWestStreamHandler(publisher)
	go ewsi.consumeLedgerBalanceEvents(ctx, handler)
}

func (ewsi *EastWestStreamInterface) consumeLedgerBalanceEvents(ctx context.Context, handler *EastWestStreamHandler) {
	streamName := internal.StreamLedger
	lastID := "$"

	logger.WithStream(streamName, "consume").
		Info(ctx, "Starting ledger balance subscriber")

	for {
		select {
		case <-ctx.Done():
			logger.WithStream(streamName, "consume").
				Info(ctx, "Stopping ledger balance subscriber")
			return
		default:
		}

		streams, err := ewsi.XReadWithSpan(ctx, streamName, &redis.XReadArgs{
			Streams: []string{streamName, lastID},
			Count:   20,
			Block:   5 * time.Second,
		})
		if err != nil {
			if err == redis.Nil {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			logger.WithStream(streamName, "consume").
				Error(ctx, "Ledger balance subscriber read error", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		for _, stream := range streams {
			for _, msg := range stream.Messages {
				lastID = msg.ID

				// Wrap message handling with tracing; XDEL after success keeps event.ledger bounded (single consumer).
				if err := internal.TraceEventProcessing(ctx, streamName, msg, func(ctx context.Context, msg redis.XMessage) error {
					raw, ok := msg.Values["event"].(string)
					if !ok {
						return fmt.Errorf("ledger message missing event field")
					}
					return ewsi.handleLedgerMessage(ctx, handler, raw)
				}, nil); err != nil {
					logger.WithStream(streamName, "consume").
						Errorf(ctx, "Failed to handle ledger message %s: %v", msg.ID, err)
				} else {
					if err := ewsi.XDelWithSpan(ctx, streamName, msg.ID); err != nil {
						logger.WithStream(streamName, "consume").
							Warnf(ctx, "XDEL after successful ledger event processing failed for %s: %v", msg.ID, err)
					}
				}
			}
		}
	}
}

// StartLightningInvoiceSubscriber listens for lightning invoice events and forwards updates via MQTT
func (ewsi *EastWestStreamInterface) StartLightningInvoiceSubscriber(ctx context.Context, publisher *SouthboundPublisher) {
	// Create handler for processing lightning events
	handler := NewEastWestStreamHandler(publisher)
	go ewsi.consumeLightningInvoiceEvents(ctx, handler)
}

func (ewsi *EastWestStreamInterface) consumeLightningInvoiceEvents(ctx context.Context, handler *EastWestStreamHandler) {
	lastSettled := "$"
	lastEphemeral := "$"

	logger.WithStream(internal.StreamLightning, "consume").
		Info(ctx, "Starting lightning invoice subscriber (event.lightning + event.lightning.ephemeral)")

	for {
		select {
		case <-ctx.Done():
			logger.WithStream(internal.StreamLightning, "consume").
				Info(ctx, "Stopping lightning invoice subscriber")
			return
		default:
		}

		// XREAD STREAMS key [key ...] id [id ...] — go-redis Streams must be
		// [key1, key2, id1, id2], not [key1, id1, key2, id2].
		streams, err := ewsi.XReadWithSpan(ctx, "", &redis.XReadArgs{
			Streams: []string{
				internal.StreamLightning,
				internal.StreamLightningEphemeral,
				lastSettled,
				lastEphemeral,
			},
			Count: 20,
			Block: 5 * time.Second,
		})
		if err != nil {
			if err == redis.Nil {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			logger.WithStream(internal.StreamLightning, "consume").
				Error(ctx, "Lightning invoice subscriber read error", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		for _, stream := range streams {
			streamName := stream.Stream
			for _, msg := range stream.Messages {
				switch streamName {
				case internal.StreamLightning:
					lastSettled = msg.ID
				case internal.StreamLightningEphemeral:
					lastEphemeral = msg.ID
				}

				if err := internal.TraceEventProcessing(ctx, streamName, msg, func(ctx context.Context, msg redis.XMessage) error {
					raw, ok := msg.Values["event"].(string)
					if !ok {
						return fmt.Errorf("lightning message missing event field")
					}
					return ewsi.handleLightningMessage(ctx, handler, raw)
				}, nil); err != nil {
					logger.WithStream(streamName, "consume").
						Errorf(ctx, "Failed to handle lightning message %s: %v", msg.ID, err)
				}
			}
		}
	}
}

// handleLedgerMessage decodes ledger event and calls appropriate handler method
func (ewsi *EastWestStreamInterface) handleLedgerMessage(ctx context.Context, handler *EastWestStreamHandler, rawEvent string) error {
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}

	var ledgerEvent ledgermodel.LedgerEvent
	if err := opts.Unmarshal([]byte(rawEvent), &ledgerEvent); err != nil {
		return fmt.Errorf("failed to unmarshal ledger event: %w", err)
	}

	switch ledgerEvent.GetType() {
	case ledgermodel.LedgerEventType_LEDGER_EVENT_TYPE_DEVICE_CREDITED:
		payload := ledgerEvent.GetDeviceCredited()
		if payload == nil {
			return fmt.Errorf("ledger event missing DeviceCredited payload")
		}
		return handler.HandleDeviceCredited(ctx, payload)

	case ledgermodel.LedgerEventType_LEDGER_EVENT_TYPE_DEVICE_DEBITED:
		payload := ledgerEvent.GetDeviceDebited()
		if payload == nil {
			return fmt.Errorf("ledger event missing DeviceDebited payload")
		}
		return handler.HandleDeviceDebited(ctx, payload)

	case ledgermodel.LedgerEventType_LEDGER_EVENT_TYPE_AUTHORIZATION_COMPLETED:
		payload := ledgerEvent.GetAuthorizationCompleted()
		if payload == nil {
			return fmt.Errorf("ledger event missing AuthorizationCompleted payload")
		}
		return handler.HandleAuthorizationCompleted(ctx, payload)

	case ledgermodel.LedgerEventType_LEDGER_EVENT_TYPE_AUTHORIZATION_EXPIRED:
		payload := ledgerEvent.GetAuthorizationExpired()
		if payload == nil {
			return fmt.Errorf("ledger event missing AuthorizationExpired payload")
		}
		return handler.HandleAuthorizationExpired(ctx, payload)

	case ledgermodel.LedgerEventType_LEDGER_EVENT_TYPE_AUTHORIZATION_DEBIT_FAILED:
		payload := ledgerEvent.GetAuthorizationDebitFailed()
		if payload == nil {
			return fmt.Errorf("ledger event missing AuthorizationDebitFailed payload")
		}
		return handler.HandleAuthorizationDebitFailed(ctx, payload)

	default:
		return nil
	}
}

// handleLightningMessage decodes lightning event and calls appropriate handler method
func (ewsi *EastWestStreamInterface) handleLightningMessage(ctx context.Context, handler *EastWestStreamHandler, rawEvent string) error {
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}

	var lightningEvent lightningmodel.LightningEvent
	if err := opts.Unmarshal([]byte(rawEvent), &lightningEvent); err != nil {
		return fmt.Errorf("failed to unmarshal lightning event: %w", err)
	}

	switch lightningEvent.GetType() {
	case lightningmodel.LightningEventType_LIGHTNING_EVENT_TYPE_INVOICE_SETTLED:
		payload := lightningEvent.GetInvoiceSettled()
		if payload == nil {
			return fmt.Errorf("lightning event missing InvoiceSettled payload")
		}
		return handler.HandleInvoiceSettled(ctx, payload)

	case lightningmodel.LightningEventType_LIGHTNING_EVENT_TYPE_INVOICE_EXPIRED:
		payload := lightningEvent.GetInvoiceExpired()
		if payload == nil {
			return fmt.Errorf("lightning event missing InvoiceExpired payload")
		}
		return handler.HandleInvoiceExpired(ctx, payload)

	default:
		logger.DebugWithFields(ctx, "Ignoring lightning event type", map[string]interface{}{
			"type": lightningEvent.GetType().String(),
		})
		return nil
	}
}

// Close closes the Redis client connection (delegates to embedded internal client)
func (ewsi *EastWestStreamInterface) Close() error {
	return ewsi.StreamClient.Close()
}
