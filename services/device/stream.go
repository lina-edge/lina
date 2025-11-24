package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/robertodantas/lnpay/internal"
	devicepb "github.com/robertodantas/lnpay/proto/gen/model/device"
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
		DeviceId:          payload.GetDeviceId(),
		ReportId:          payload.GetReportId(),
		Strategy:          convertReportingStrategy(payload.GetStrategy()),
		Measure:           payload.GetMeasure(),
		Unit:              payload.GetUnit(),
		Timestamp:         payload.GetTimestamp(),
		PricePerUnitMsat:  pricePerUnitMsat,
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

// Close closes the Redis client connection (delegates to embedded internal client)
func (sc *StreamClient) Close() error {
	return sc.StreamClient.Close()
}
