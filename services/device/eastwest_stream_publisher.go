package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/robertodantas/lina/internal"
	devicepb "github.com/robertodantas/lina/proto/gen/model/device"
	mqttpb "github.com/robertodantas/lina/proto/gen/model/mqtt"
)

// EastWestStreamPublisher handles publishing messages to Redis streams for east-west communication
type EastWestStreamPublisher struct {
	streamInterface *EastWestStreamInterface
}

// NewEastWestStreamPublisher creates a new east-west stream publisher
func NewEastWestStreamPublisher(streamInterface *EastWestStreamInterface) *EastWestStreamPublisher {
	return &EastWestStreamPublisher{
		streamInterface: streamInterface,
	}
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
func (esp *EastWestStreamPublisher) PublishDeviceUsageReportedEvent(ctx context.Context, payload *mqttpb.UsagePayload, repo *DeviceRepository) error {
	// Fetch device config to get price_per_unit
	device, err := repo.GetDevice(ctx, payload.GetDeviceId())
	if err != nil {
		return fmt.Errorf("failed to get device config for %s: %w", payload.GetDeviceId(), err)
	}

	// Use unit_price_msat directly (already in msat)
	pricePerUnitMsat := device.UnitPriceMsat

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

	streamName := internal.StreamDevice
	values := map[string]interface{}{
		"event":     string(jsonBytes),
		"timestamp": time.Now().UnixMilli(),
	}

	// Use XAddWithSpan to add entry to stream with tracing
	streamID, err := esp.streamInterface.XAddWithSpan(ctx, streamName, &redis.XAddArgs{
		Stream: streamName,
		Values: values,
	}, "USAGE_REPORTED")

	if err != nil {
		return fmt.Errorf("failed to publish to Redis stream %s: %w", streamName, err)
	}

	logger.WithDeviceID(payload.GetDeviceId()).
		WithStream(streamName, "produce").
		DebugWithFields(ctx, "Published DeviceEvent (usage reported) on southbound mqtt", map[string]interface{}{
			"stream_id": streamID,
			"report_id": payload.GetReportId(),
		})
	return nil
}
