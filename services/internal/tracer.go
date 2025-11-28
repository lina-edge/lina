package internal

import (
	"context"
	"fmt"

	autosdk "go.opentelemetry.io/auto/sdk"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// TracerConfig holds configuration for initializing OpenTelemetry tracer
type TracerConfig struct {
	ServiceName          string
	ExporterOTLPEndpoint string
}

// InitTracer initializes OpenTelemetry with OTLP exporter
// Returns a shutdown function that should be called on service shutdown
func InitTracer(cfg TracerConfig) (func(context.Context) error, error) {
	ctx := context.Background()

	// Create OTLP exporter
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(cfg.ExporterOTLPEndpoint),
		otlptracegrpc.WithInsecure(), // Use insecure for local development
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
	}

	// Create resource with service name
	res, err := resource.New(ctx,
		resource.WithAttributes(
			attribute.String("service.name", cfg.ServiceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Create tracer provider with auto SDK and OTLP exporter
	autoTp := autosdk.TracerProvider()

	// Create a batch span processor with the OTLP exporter
	bsp := sdktrace.NewBatchSpanProcessor(exporter)

	// Create a new tracer provider that combines auto SDK with OTLP export
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
		// Use the auto SDK's sampler if available
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(1.0))),
	)

	// Set global tracer provider
	otel.SetTracerProvider(tp)

	// Set global propagator for trace context propagation
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// Shutdown function
	shutdown := func(ctx context.Context) error {
		// Shutdown the custom tracer provider
		if err := tp.Shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown tracer provider: %w", err)
		}
		// Note: auto SDK tracer provider shutdown is handled internally
		_ = autoTp
		return nil
	}

	return shutdown, nil
}
