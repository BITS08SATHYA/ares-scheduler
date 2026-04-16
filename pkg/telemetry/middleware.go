// File: pkg/telemetry/middleware.go
// HTTP middleware for OpenTelemetry trace propagation.
// Extracts trace context from incoming requests and creates server spans.

package telemetry

import (
	"fmt"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// HTTPMiddleware wraps an http.Handler with OpenTelemetry tracing.
// It extracts trace context from incoming request headers and creates
// a server span for each request.
func HTTPMiddleware(next http.Handler) http.Handler {
	if !IsEnabled() {
		return next
	}

	tracer := Tracer("ares.http")
	propagator := otel.GetTextMapPropagator()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))

		spanName := fmt.Sprintf("%s %s", r.Method, r.URL.Path)
		ctx, span := tracer.Start(ctx, spanName,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				semconv.HTTPRequestMethodKey.String(r.Method),
				semconv.URLPath(r.URL.Path),
				attribute.String("http.client_ip", r.RemoteAddr),
			),
		)
		defer span.End()

		// Wrap response writer to capture status code
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r.WithContext(ctx))

		span.SetAttributes(attribute.Int("http.response.status_code", sw.status))
		if sw.status >= 400 {
			span.SetAttributes(attribute.Bool("error", true))
		}
	})
}

// statusWriter wraps http.ResponseWriter to capture the status code
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (sw *statusWriter) WriteHeader(code int) {
	sw.status = code
	sw.ResponseWriter.WriteHeader(code)
}
