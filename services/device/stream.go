package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/robertodantas/lnpay/internal"
	devicepb "github.com/robertodantas/lnpay/proto/gen/model/device"
	ledgermodel "github.com/robertodantas/lnpay/proto/gen/model/ledger"
	mqttpb "github.com/robertodantas/lnpay/proto/gen/model/mqtt"
)

// StreamClient wraps the internal StreamClient with device-specific methods
type StreamClient struct {
	*internal.StreamClient
}

// NewStreamClient creates a new Redis stream client using the internal package
func NewStreamClient() (*StreamClient, error) {
	libClient, err := internal.NewStreamClientFromEnv()
	if err != nil {
		return nil, err
	}

	return &StreamClient{
		StreamClient: libClient,
	}, nil
}

// convertReportingStrategy converts MQTT ReportingStrategy to device UsageReportingStrategy
func convertReportingStrategy(strategy mqttpb.ReportingStrategy) devicepb.UsageReportingStrategy {
	switch strategy {
	case mqttpb.ReportingStrategy_REPORTING_STRATEGY_INTERVAL:
		return devicepb.UsageReportingStrategy_USAGE_STRATEGY_INTERVAL
	case mqttpb.ReportingStrategy_REPORTING_STRATEGY_DELTA:
		return devicepb.UsageReportingStrategy_USAGE_STRATEGY_DELTA
	case mqttpb.ReportingStrategy_REPORTING_STRATEGY_TOTAL:
		return devicepb.UsageReportingStrategy_USAGE_STRATEGY_TOTAL
	default:
		return devicepb.UsageReportingStrategy_USAGE_STRATEGY_UNSPECIFIED
	}
}

// PublishDeviceUsageReportedEvent publishes a DeviceEvent containing DeviceUsageReportedEvent to the Redis stream
// It fetches device config from the repository to append price_per_unit_msat
func (sc *StreamClient) PublishDeviceUsageReportedEvent(payload *mqttpb.UsagePayload, repo *DeviceRepository) error {
	// Fetch device config to get price_per_unit
	device, err := repo.GetDevice(payload.GetDeviceId())
	if err != nil {
		return fmt.Errorf("failed to get device config for %s: %w", payload.GetDeviceId(), err)
	}

	// Parse unit_price string to int64 (assuming it's already in msat if pricing_unit is "msat")
	var pricePerUnitMsat int64
	if device.PricingUnit == "msat" {
		// Parse the unit_price string to int64
		parsed, err := strconv.ParseInt(device.UnitPrice, 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse unit_price %s for device %s: %w", device.UnitPrice, payload.GetDeviceId(), err)
		}
		pricePerUnitMsat = parsed
	} else {
		// For other pricing units, we'd need conversion logic
		// For now, assume msat or log a warning
		log.Printf("Warning: device %s has pricing_unit %s, assuming msat", payload.GetDeviceId(), device.PricingUnit)
		parsed, err := strconv.ParseInt(device.UnitPrice, 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse unit_price %s for device %s: %w", device.UnitPrice, payload.GetDeviceId(), err)
		}
		pricePerUnitMsat = parsed
	}

	// Convert MQTT UsagePayload to device UsageRecord
	usageRecord := &devicepb.UsageRecord{
		DeviceId:         payload.GetDeviceId(),
		ReportId:         payload.GetReportId(),
		Strategy:         convertReportingStrategy(payload.GetStrategy()),
		Measure:          payload.GetMeasure(),
		Unit:             payload.GetUnit(),
		Timestamp:        payload.GetTimestamp(),
		PricePerUnitMsat: pricePerUnitMsat,
	}

	// Create the DeviceUsageReportedEvent
	usageReportedEvent := &devicepb.DeviceUsageReportedEvent{
		Usage: usageRecord,
	}

	// Wrap in DeviceEvent envelope
	deviceEvent := &devicepb.DeviceEvent{
		Type: devicepb.DeviceEventType_DEVICE_EVENT_TYPE_USAGE_REPORTED,
		Payload: &devicepb.DeviceEvent_UsageReported{
			UsageReported: usageReportedEvent,
		},
	}

	// Serialize to JSON for Redis stream
	opts := protojson.MarshalOptions{UseProtoNames: true}
	jsonBytes, err := opts.Marshal(deviceEvent)
	if err != nil {
		return fmt.Errorf("failed to marshal device event to JSON: %w", err)
	}

	// Publish to Redis stream "event.device"
	streamName := "event.device"
	values := map[string]interface{}{
		"event": string(jsonBytes),
		// Add timestamp for stream ordering
		"timestamp": time.Now().UnixMilli(),
	}

	// Use XADD to add entry to stream
	result := sc.Client().XAdd(sc.Context(), &redis.XAddArgs{
		Stream: streamName,
		Values: values,
	})

	if result.Err() != nil {
		return fmt.Errorf("failed to publish to Redis stream %s: %w", streamName, result.Err())
	}

	log.Printf("Published DeviceEvent (usage reported) to stream %s (ID: %s)", streamName, result.Val())
	return nil
}

// StartLedgerBalanceSubscriber listens for ledger balance events and forwards updates via MQTT
func (sc *StreamClient) StartLedgerBalanceSubscriber(ctx context.Context, mqttClient *MQTTClient) {
	go sc.consumeLedgerBalanceEvents(ctx, mqttClient)
}

func (sc *StreamClient) consumeLedgerBalanceEvents(ctx context.Context, mqttClient *MQTTClient) {
	client := sc.Client()
	streamName := "event.ledger"
	lastID := "$"
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}

	log.Printf("Starting ledger balance subscriber on stream %s", streamName)

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping ledger balance subscriber")
			return
		default:
		}

		streams, err := client.XRead(ctx, &redis.XReadArgs{
			Streams: []string{streamName, lastID},
			Count:   20,
			Block:   5 * time.Second,
		}).Result()
		if err != nil {
			if err == redis.Nil {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			log.Printf("ledger balance subscriber read error: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		for _, stream := range streams {
			for _, msg := range stream.Messages {
				lastID = msg.ID
				if err := sc.handleLedgerMessage(ctx, mqttClient, msg, opts); err != nil {
					log.Printf("failed to handle ledger message %s: %v", msg.ID, err)
				}
			}
		}
	}
}

func (sc *StreamClient) handleLedgerMessage(ctx context.Context, mqttClient *MQTTClient, msg redis.XMessage, opts protojson.UnmarshalOptions) error {
	raw, ok := msg.Values["event"].(string)
	if !ok {
		return fmt.Errorf("ledger message missing event field")
	}

	var ledgerEvent ledgermodel.LedgerEvent
	if err := opts.Unmarshal([]byte(raw), &ledgerEvent); err != nil {
		return fmt.Errorf("failed to unmarshal ledger event: %w", err)
	}

	switch ledgerEvent.GetType() {
	case ledgermodel.LedgerEventType_LEDGER_EVENT_TYPE_DEVICE_CREDITED:
		payload := ledgerEvent.GetDeviceCredited()
		if payload == nil {
			return fmt.Errorf("ledger event missing DeviceCredited payload")
		}
		return sc.publishBalanceUpdate(ctx, mqttClient, payload.GetDeviceId(), payload.GetNewBalanceMsat(), payload.GetTimestamp())
	case ledgermodel.LedgerEventType_LEDGER_EVENT_TYPE_DEVICE_DEBITED:
		payload := ledgerEvent.GetDeviceDebited()
		if payload == nil {
			return fmt.Errorf("ledger event missing DeviceDebited payload")
		}
		return sc.publishBalanceUpdate(ctx, mqttClient, payload.GetDeviceId(), payload.GetRemainingMsat(), payload.GetTimestamp())
	default:
		return nil
	}
}

func (sc *StreamClient) publishBalanceUpdate(ctx context.Context, mqttClient *MQTTClient, deviceID string, availableMsat int64, ts string) error {
	if deviceID == "" {
		return fmt.Errorf("ledger event missing device_id")
	}
	if ts == "" {
		ts = time.Now().UTC().Format(time.RFC3339)
	}

	payload := &mqttpb.BalancePayload{
		DeviceId:      deviceID,
		AvailableMsat: availableMsat,
		ReservedMsat:  0,
		TotalMsat:     availableMsat,
		Timestamp:     ts,
	}

	marshalOpts := protojson.MarshalOptions{UseProtoNames: true}
	msgBytes, err := marshalOpts.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal balance payload: %w", err)
	}

	topic := fmt.Sprintf("/devices/%s/balance", deviceID)
	if err := mqttClient.Publish(topic, 1, true, msgBytes); err != nil {
		return fmt.Errorf("failed to publish balance to MQTT: %w", err)
	}

	log.Printf("[BALANCE] Published updated balance for device %s (available=%d)", deviceID, availableMsat)
	return nil
}

// Close closes the Redis client connection (delegates to embedded internal client)
func (sc *StreamClient) Close() error {
	return sc.StreamClient.Close()
}
