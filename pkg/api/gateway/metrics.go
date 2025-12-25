// File: pkg/api/gateway/metrics.go
// Prometheus metrics for API Gateway

package gateway

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Metrics: Gateway metrics (exposed at /metrics)
type Metrics struct {
	// Counters
	TotalRequests  uint64 // Total HTTP requests
	TotalErrors    uint64 // Total errors
	TotalScheduled uint64 // Total jobs scheduled
	TotalFailed    uint64 // Total jobs failed

	// Timing
	RequestDuration int64 // Total request duration (nanoseconds)

	// Current state
	ActiveJobs int32 // Current active jobs
	QueuedJobs int32 // Current queued jobs
}

// RecordRequest: Record an HTTP request
func (m *Metrics) RecordRequest(duration time.Duration) {
	atomic.AddUint64(&m.TotalRequests, 1)
	atomic.AddInt64(&m.RequestDuration, duration.Nanoseconds())
}

// RecordError: Record an error
func (m *Metrics) RecordError() {
	atomic.AddUint64(&m.TotalErrors, 1)
}

// RecordJobScheduled: Record a job scheduled
func (m *Metrics) RecordJobScheduled() {
	atomic.AddUint64(&m.TotalScheduled, 1)
	atomic.AddInt32(&m.ActiveJobs, 1)
}

// RecordJobCompleted: Record a job completed
func (m *Metrics) RecordJobCompleted(success bool) {
	atomic.AddInt32(&m.ActiveJobs, -1)
	if !success {
		atomic.AddUint64(&m.TotalFailed, 1)
	}
}

// GetSuccessRate: Calculate success rate
func (m *Metrics) GetSuccessRate() float64 {
	total := atomic.LoadUint64(&m.TotalRequests)
	errors := atomic.LoadUint64(&m.TotalErrors)

	if total == 0 {
		return 0.0
	}

	return float64(total-errors) / float64(total) * 100.0
}

// GetAvgDuration: Calculate average request duration (ms)
func (m *Metrics) GetAvgDuration() float64 {
	total := atomic.LoadUint64(&m.TotalRequests)
	duration := atomic.LoadInt64(&m.RequestDuration)

	if total == 0 {
		return 0.0
	}

	return float64(duration) / float64(total) / 1e6 // Convert to ms
}

// ExportPrometheus: Export metrics in Prometheus format
func (m *Metrics) ExportPrometheus() string {
	return fmt.Sprintf(`# HELP ares_http_requests_total Total HTTP requests
# TYPE ares_http_requests_total counter
ares_http_requests_total %d

# HELP ares_http_errors_total Total HTTP errors
# TYPE ares_http_errors_total counter
ares_http_errors_total %d

# HELP ares_jobs_scheduled_total Total jobs scheduled
# TYPE ares_jobs_scheduled_total counter
ares_jobs_scheduled_total %d

# HELP ares_jobs_failed_total Total jobs failed
# TYPE ares_jobs_failed_total counter
ares_jobs_failed_total %d

# HELP ares_active_jobs Current active jobs
# TYPE ares_active_jobs gauge
ares_active_jobs %d

# HELP ares_queued_jobs Current queued jobs
# TYPE ares_queued_jobs gauge
ares_queued_jobs %d

# HELP ares_request_duration_avg_ms Average request duration in milliseconds
# TYPE ares_request_duration_avg_ms gauge
ares_request_duration_avg_ms %.2f

# HELP ares_success_rate_percent Success rate percentage
# TYPE ares_success_rate_percent gauge
ares_success_rate_percent %.2f
`,
		atomic.LoadUint64(&m.TotalRequests),
		atomic.LoadUint64(&m.TotalErrors),
		atomic.LoadUint64(&m.TotalScheduled),
		atomic.LoadUint64(&m.TotalFailed),
		atomic.LoadInt32(&m.ActiveJobs),
		atomic.LoadInt32(&m.QueuedJobs),
		m.GetAvgDuration(),
		m.GetSuccessRate(),
	)
}
