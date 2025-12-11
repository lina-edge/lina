package internal

import (
	"context"
	"fmt"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// TracerConfig holds configuration for initializing OpenTelemetry tracer
type TracerConfig struct {
	ServiceName          string
	ExporterOTLPEndpoint string
}

// InitTracer initializes OpenTelemetry with OTLP exporter or noop (no span processor)
// Returns a shutdown function that should be called on service shutdown
// If ExporterOTLPEndpoint is empty, creates a TracerProvider without span processors (noop)
func InitTracer(cfg TracerConfig) (func(context.Context) error, error) {
	logger := NewLogger("internal")
	ctx := context.Background()

	// Check if tracing is explicitly disabled via OTEL_TRACES_EXPORTER=none
	otelTracesExporter := strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_TRACES_EXPORTER")))

	// Create resource with service name
	res, err := resource.New(ctx,
		resource.WithAttributes(
			attribute.String("service.name", cfg.ServiceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	var tp trace.TracerProvider

	// If endpoint is empty, create TracerProvider without span processors (noop)
	// This creates traces but doesn't export them, while keeping propagation active
	if cfg.ExporterOTLPEndpoint == "" || otelTracesExporter == "none" {
		logger.Info(ctx, "Tracing is explicitly disabled via OTEL_TRACES_EXPORTER=none")
		tp = sdktrace.NewTracerProvider(
			sdktrace.WithResource(res),
		)
	} else {
		logger.Info(ctx, "Tracing is enabled via OTEL_EXPORTER_OTLP_ENDPOINT="+cfg.ExporterOTLPEndpoint)
		// Create OTLP exporter
		exporter, err := otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(cfg.ExporterOTLPEndpoint),
			otlptracegrpc.WithInsecure(), // Use insecure for local development
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
		}

		// Create a batch span processor with the OTLP exporter
		bsp := sdktrace.NewBatchSpanProcessor(exporter)

		// Create a new tracer provider with OTLP exporter
		tp = sdktrace.NewTracerProvider(
			sdktrace.WithResource(res),
			sdktrace.WithSpanProcessor(bsp),
			sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(1.0))),
		)
	}

	// Set global tracer provider
	otel.SetTracerProvider(tp)

	// Set global propagator for trace context propagation
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// Shutdown function
	shutdown := func(ctx context.Context) error {
		// Shutdown the tracer provider if it's an SDK tracer provider
		if sdkTp, ok := tp.(*sdktrace.TracerProvider); ok {
			if err := sdkTp.Shutdown(ctx); err != nil {
				return fmt.Errorf("failed to shutdown tracer provider: %w", err)
			}
		}
		// noop.TracerProvider doesn't need shutdown
		return nil
	}

	return shutdown, nil
}
