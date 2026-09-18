// Package telemetry initialises the OpenTelemetry SDK.
// In development (OTEL_ENDPOINT=stdout) traces are printed as JSON to stdout.
// In production, set OTEL_ENDPOINT to an OTLP collector address.
package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Init sets up the global OTel TracerProvider and returns a shutdown function
// that must be called on graceful shutdown to flush pending spans.
func Init(serviceName string) (shutdown func(context.Context) error, err error) {
	exporter, err := stdouttrace.New(
		stdouttrace.WithPrettyPrint(),
	)
	if err != nil {
		return nil, fmt.Errorf("telemetry: create stdout exporter: %w", err)
	}

	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion("0.1.0"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("telemetry: create resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		// Batch exporter — spans are flushed in bulk, not per-span
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		// Sample 10% of traces to avoid log noise in dev
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(0.1)),
	)

	otel.SetTracerProvider(tp)

	return tp.Shutdown, nil
}
