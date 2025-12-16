package main

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

var (
	// Metrics instruments
	authorizationsTotal metric.Int64Counter
	entriesTotal        metric.Int64Counter
	debitLatencySeconds metric.Float64Histogram
)

// initMetrics initializes Prometheus metrics using OpenTelemetry
func initMetrics() error {
	// Create Prometheus exporter
	exporter, err := prometheus.New()
	if err != nil {
		return err
	}

	// Create meter provider with Prometheus exporter
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exporter),
	)

	// Set as global meter provider
	otel.SetMeterProvider(provider)

	// Get meter
	meter := provider.Meter("ledger-service")

	// Create counter for authorizations
	authorizationsTotal, err = meter.Int64Counter(
		"ledger_authorizations_total",
		metric.WithDescription("Total number of authorization operations"),
	)
	if err != nil {
		return err
	}

	// Create counter for entries
	entriesTotal, err = meter.Int64Counter(
		"ledger_entries_total",
		metric.WithDescription("Total number of ledger entries"),
	)
	if err != nil {
		return err
	}

	// Create histogram for debit latency with explicit bucket boundaries
	// Optimized for sub-second latencies (typical range: 0.001s - 1s) with higher buckets for outliers
	// Buckets: 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, +Inf
	buckets := []float64{
		0.001, // 1ms
		0.002, // 2ms
		0.005, // 5ms
		0.01,  // 10ms
		0.02,  // 20ms
		0.05,  // 50ms
		0.1,   // 100ms
		0.2,   // 200ms
		0.5,   // 500ms
		1.0,   // 1s
		2.0,   // 2s
		5.0,   // 5s
		10.0,  // 10s
		30.0,  // 30s
		60.0,  // 1m
		120.0, // 2m
		300.0, // 5m
		600.0, // 10m
		// +Inf is automatically added
	}
	debitLatencySeconds, err = meter.Float64Histogram(
		"ledger_debit_latency_seconds",
		metric.WithDescription("Latency of debit operations from usage in seconds"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(buckets...),
	)
	if err != nil {
		return err
	}

	return nil
}

// GetMetricsHandler returns the HTTP handler for the /metrics endpoint
// The OTel prometheus exporter automatically collects metrics from the meter provider
// and exposes them via the default Prometheus registry
func GetMetricsHandler() http.Handler {
	// The exporter automatically collects metrics from the meter provider
	// We just need to expose them via HTTP using the default Prometheus handler
	// The exporter's internal mechanism will handle metric collection
	return promhttp.Handler()
}

// RecordAuthorizationCreated records an authorization created event
func RecordAuthorizationCreated(ctx context.Context) {
	authorizationsTotal.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("operation", "created"),
		),
	)
}

// RecordAuthorizationDebited records an authorization debited event
func RecordAuthorizationDebited(ctx context.Context) {
	authorizationsTotal.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("operation", "debited"),
		),
	)
}

// RecordAuthorizationExpired records an authorization expired event
func RecordAuthorizationExpired(ctx context.Context) {
	authorizationsTotal.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("operation", "expired"),
		),
	)
}

// RecordEntry records a ledger entry with type and source
func RecordEntry(ctx context.Context, entryType, source string) {
	entriesTotal.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("entry_type", entryType),
			attribute.String("source", source),
		),
	)
}

// RecordDebitLatency records the latency for a debit operation from usage
func RecordDebitLatency(ctx context.Context, latencySeconds float64) {
	debitLatencySeconds.Record(ctx, latencySeconds,
		metric.WithAttributes(
			attribute.String("source", "usage"),
		),
	)
}
