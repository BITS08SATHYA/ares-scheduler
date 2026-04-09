// File: pkg/telemetry/tracer.go
// OpenTelemetry tracer initialization and management for Ares Scheduler.
// Supports OTLP HTTP export (for Jaeger/Grafana Tempo) and stdout export (for dev).
//
// Configuration via environment variables:
//   ARES_OTEL_ENABLED=true       — enable tracing (default: false)
//   ARES_OTEL_ENDPOINT=host:4318 — OTLP HTTP endpoint (default: localhost:4318)
//   ARES_OTEL_SERVICE_NAME       — service name (default: ares-scheduler)
//   ARES_OTEL_EXPORTER=stdout    — use stdout exporter for development

package telemetry

import (
	"context"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

var tp *sdktrace.TracerProvider

// IsEnabled returns true if ARES_OTEL_ENABLED=true
func IsEnabled() bool {
	return strings.EqualFold(os.Getenv("ARES_OTEL_ENABLED"), "true")
}

// Init initializes the OpenTelemetry tracer provider.
// Call Shutdown() on application exit to flush pending spans.
func Init(ctx context.Context) error {
	if !IsEnabled() {
		return nil
	}

	serviceName := os.Getenv("ARES_OTEL_SERVICE_NAME")
	if serviceName == "" {
		serviceName = "ares-scheduler"
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion("0.1.0"),
		),
	)
	if err != nil {
		return err
	}

	var exporter sdktrace.SpanExporter

	exporterType := os.Getenv("ARES_OTEL_EXPORTER")
	if strings.EqualFold(exporterType, "stdout") {
		exporter, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
	} else {
		endpoint := os.Getenv("ARES_OTEL_ENDPOINT")
		if endpoint == "" {
			endpoint = "localhost:4318"
		}
		opts := []otlptracehttp.Option{
			otlptracehttp.WithEndpoint(endpoint),
		}
		// Default to insecure for local development
		if !strings.EqualFold(os.Getenv("ARES_OTEL_TLS"), "true") {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		exporter, err = otlptracehttp.New(ctx, opts...)
	}
	if err != nil {
		return err
	}

	tp = sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	otel.SetTracerProvider(tp)
	return nil
}

// Shutdown flushes all pending spans and shuts down the tracer provider.
func Shutdown(ctx context.Context) error {
	if tp == nil {
		return nil
	}
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return tp.Shutdown(shutdownCtx)
}

// Tracer returns a named tracer for the given component.
// If tracing is disabled, returns a no-op tracer.
func Tracer(name string) trace.Tracer {
	return otel.Tracer(name)
}
