package main

import (
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/robertodantas/lnpay/library"
	devicepb "github.com/robertodantas/lnpay/proto/gen/model/device"
	mqttpb "github.com/robertodantas/lnpay/proto/gen/model/mqtt"
)

// StreamClient wraps the library StreamClient with device-specific methods
type StreamClient struct {
	*library.StreamClient
}

// NewStreamClient creates a new Redis stream client using the library
func NewStreamClient() (*StreamClient, error) {
	libClient, err := library.NewStreamClientFromEnv()
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
func (sc *StreamClient) PublishDeviceUsageReportedEvent(payload *mqttpb.UsagePayload) error {
	// Convert MQTT UsagePayload to device UsageRecord
	usageRecord := &devicepb.UsageRecord{
		DeviceId:  payload.GetDeviceId(),
		ReportId:  payload.GetReportId(),
		Strategy:  convertReportingStrategy(payload.GetStrategy()),
		Measure:   payload.GetMeasure(),
		Unit:      payload.GetUnit(),
		Timestamp: payload.GetTimestamp(),
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

// Close closes the Redis client connection (delegates to embedded library client)
func (sc *StreamClient) Close() error {
	return sc.StreamClient.Close()
}
