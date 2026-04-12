package main

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/robertodantas/lina/internal"
	ledgermodel "github.com/robertodantas/lina/proto/gen/model/ledger"
)

func TestStreamHandlerPublishesLedgerEvents(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	container, host, port := startRedisContainer(t, ctx)
	defer func() {
		_ = container.Terminate(ctx)
	}()

	streamClient, err := internal.NewStreamClient(ctx, internal.StreamConfig{
		Host: host,
		Port: port,
		DB:   0,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = streamClient.Close() })

	require.NoError(t, streamClient.Client().FlushAll(ctx).Err())

	publisher := NewEastWestStreamPublisher(streamClient)

	ts := time.Now().UTC().Format(time.RFC3339)
	err = publisher.PublishDeviceCredited(ctx, "device-stream-1", 1_000, 1_000, ts)
	require.NoError(t, err)

	err = publisher.PublishDeviceDebited(ctx, "device-stream-1", "auth-123", 250, 750, ts)
	require.NoError(t, err)

	entries := readStreamEntries(t, streamClient.Client(), ctx, internal.StreamLedger)
	require.Len(t, entries, 2)

	assertLedgerEvent(t, entries[0], ledgermodel.LedgerEventType_LEDGER_EVENT_TYPE_DEVICE_CREDITED)
	assertLedgerEvent(t, entries[1], ledgermodel.LedgerEventType_LEDGER_EVENT_TYPE_DEVICE_DEBITED)
}

func startRedisContainer(t *testing.T, ctx context.Context) (testcontainers.Container, string, string) {
	t.Helper()

	req := testcontainers.ContainerRequest{
		Image:        "redis:7.2",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForListeningPort("6379/tcp"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("skipping integration test, redis container unavailable: %v", err)
	}

	host, err := container.Host(ctx)
	require.NoError(t, err)

	mappedPort, err := container.MappedPort(ctx, "6379/tcp")
	require.NoError(t, err)

	return container, host, mappedPort.Port()
}

func readStreamEntries(t *testing.T, client *redis.Client, ctx context.Context, stream string) []redis.XMessage {
	t.Helper()
	messages, err := client.XRange(ctx, stream, "-", "+").Result()
	require.NoError(t, err)
	return messages
}

func assertLedgerEvent(t *testing.T, msg redis.XMessage, expectedType ledgermodel.LedgerEventType) {
	t.Helper()
	raw, ok := msg.Values["event"].(string)
	require.True(t, ok, "expected string event payload")

	var event ledgermodel.LedgerEvent
	err := protojson.Unmarshal([]byte(raw), &event)
	require.NoError(t, err)
	require.Equal(t, expectedType, event.GetType())
}
