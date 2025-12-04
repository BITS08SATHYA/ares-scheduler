// File: pkg/api/gateway/gateway.go (LAYER 8 - API GATEWAY)
// HTTP REST API gateway for Ares scheduler
// Features: Feature 12 (API Gateway)
// Wraps GlobalScheduler.ScheduleJob() and exposes HTTP endpoints
// Production-ready: Zero errors, comprehensive validation, security, logging
// Depends on: types.go, logger.go, GlobalScheduler (Layer 7)

package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/global"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
)

// ============================================================================
// API GATEWAY SERVICE
// ============================================================================

// APIGateway: HTTP REST API for Ares scheduler
// Wraps GlobalScheduler and exposes HTTP endpoints
// Thread-safe: Uses atomic counters for metrics
type APIGateway struct {
	globalScheduler *global.GlobalScheduler
	log             *logger.Logger
	config          *GatewayConfig

	// Metrics (atomic for thread-safety)
	totalRequests   uint64
	totalErrors     uint64
	totalScheduled  uint64
	requestDuration int64 // nanoseconds, atomic

	// Server
	server *http.Server

	// Handler mutex
	handlerMu sync.RWMutex
}

// GatewayConfig: Configuration for API gateway
type GatewayConfig struct {
	Port             int           // HTTP port (e.g., 8080)
	RequestTimeout   time.Duration // Request timeout (default: 30s)
	MaxRequestSize   int64         // Max request body size (bytes)
	EnableCORS       bool          // Enable CORS headers
	EnablePrometheus bool          // Expose Prometheus metrics
	HealthCheckPath  string        // Health check endpoint
	MetricsPath      string        // Metrics endpoint
}

// APIRequest: Generic API request
type APIRequest struct {
	RequestID    string `json:"request_id"`
	Name         string `json:"name"`
	GPUCount     int    `json:"gpu_count"`
	GPUType      string `json:"gpu_type"`
	MemoryMB     int    `json:"memory_mb"`
	Priority     int    `json:"priority"`
	PreferRegion string `json:"prefer_region,omitempty"`
	Timeout      int    `json:"timeout,omitempty"`
	RetryCount   int    `json:"retry_count,omitempty"`
}

// APIResponse: Generic API response
type APIResponse struct {
	Success          bool     `json:"success"`
	RequestID        string   `json:"request_id"`
	ClusterID        string   `json:"cluster_id,omitempty"`
	Region           string   `json:"region,omitempty"`
	LocalScheduler   string   `json:"local_scheduler,omitempty"`
	ClusterScore     float64  `json:"cluster_score,omitempty"`
	PlacementReasons []string `json:"placement_reasons,omitempty"`
	Message          string   `json:"message,omitempty"`
	Timestamp        string   `json:"timestamp"`
	DurationMs       float64  `json:"duration_ms,omitempty"`
}

// HealthCheckResponse: Health check response
type HealthCheckResponse struct {
	Status        string  `json:"status"`
	ControlPlane  string  `json:"control_plane"`
	Timestamp     string  `json:"timestamp"`
	TotalRequests uint64  `json:"total_requests"`
	TotalErrors   uint64  `json:"total_errors"`
	SuccessRate   float64 `json:"success_rate"`
	AvgDurationMs float64 `json:"avg_duration_ms"`
}

// MetricsResponse: Scheduler metrics
type MetricsResponse struct {
	Timestamp        string                 `json:"timestamp"`
	TotalScheduled   uint64                 `json:"total_scheduled"`
	TotalFailed      uint64                 `json:"total_failed"`
	SuccessRate      float64                `json:"success_rate"`
	AvgDurationMs    float64                `json:"avg_duration_ms"`
	DatacenterLoad   map[string]interface{} `json:"datacenter_load"`
	FederationStatus map[string]interface{} `json:"federation_status"`
}

// ErrorResponse: Error response
type ErrorResponse struct {
	Success   bool   `json:"success"`
	ErrorCode string `json:"error_code"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
}

// Default config
var DefaultGatewayConfig = &GatewayConfig{
	Port:             8080,
	RequestTimeout:   30 * time.Second,
	MaxRequestSize:   1 << 20, // 1MB
	EnableCORS:       true,
	EnablePrometheus: true,
	HealthCheckPath:  "/health",
	MetricsPath:      "/metrics",
}

// NewAPIGateway: Create new API gateway
func NewAPIGateway(
	globalScheduler *global.GlobalScheduler,
	config *GatewayConfig,
) (*APIGateway, error) {

	if globalScheduler == nil {
		return nil, fmt.Errorf("global scheduler cannot be nil")
	}

	if config == nil {
		config = DefaultGatewayConfig
	}

	if config.Port <= 0 || config.Port > 65535 {
		return nil, fmt.Errorf("invalid port: %d (must be 1-65535)", config.Port)
	}

	if config.RequestTimeout <= 0 {
		config.RequestTimeout = 30 * time.Second
	}

	if config.MaxRequestSize <= 0 {
		config.MaxRequestSize = 1 << 20
	}

	gateway := &APIGateway{
		globalScheduler: globalScheduler,
		log:             logger.Get(),
		config:          config,
	}

	return gateway, nil
}

// ============================================================================
// HTTP HANDLERS
// ============================================================================

// RegisterRoutes: Register all HTTP routes
func (ag *APIGateway) RegisterRoutes() *http.ServeMux {
	mux := http.NewServeMux()

	// Schedule job endpoint (main)
	mux.HandleFunc("/schedule", ag.wrapHandler(ag.handleScheduleJob))

	// Health check
	mux.HandleFunc(ag.config.HealthCheckPath, ag.wrapHandler(ag.handleHealthCheck))

	// Metrics
	if ag.config.EnablePrometheus {
		mux.HandleFunc(ag.config.MetricsPath, ag.wrapHandler(ag.handleMetrics))
	}

	// Status endpoints
	mux.HandleFunc("/status/datacenter", ag.wrapHandler(ag.handleDatacenterStatus))
	mux.HandleFunc("/status/federation", ag.wrapHandler(ag.handleFederationStatus))
	mux.HandleFunc("/status/cluster", ag.wrapHandler(ag.handleClusterStatus))

	// Info endpoints
	mux.HandleFunc("/info/capacity", ag.wrapHandler(ag.handleCapacity))
	mux.HandleFunc("/info/clusters", ag.wrapHandler(ag.handleListClusters))

	return mux
}

// wrapHandler: Middleware wrapper for all handlers
func (ag *APIGateway) wrapHandler(handler func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// CORS headers
		if ag.config.EnableCORS {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		}

		// Default headers
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Ares-Version", "1.0")
		w.Header().Set("X-Request-ID", ag.generateRequestID())

		// Handle OPTIONS (CORS preflight)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		// Request logging
		startTime := time.Now()
		ag.log.Debug("API Request: %s %s from %s", r.Method, r.URL.Path, r.RemoteAddr)

		// Call handler
		handler(w, r)

		// Metrics
		duration := time.Since(startTime)
		atomic.AddUint64(&ag.totalRequests, 1)
		atomic.AddInt64(&ag.requestDuration, duration.Nanoseconds())

		ag.log.Debug("API Response completed in %.2fms", duration.Seconds()*1000)
	}
}

// handleScheduleJob: POST /schedule - Main scheduling endpoint
func (ag *APIGateway) handleScheduleJob(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()

	// Validate method
	if r.Method != http.MethodPost {
		ag.respondError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			fmt.Sprintf("expected POST, got %s", r.Method))
		atomic.AddUint64(&ag.totalErrors, 1)
		return
	}

	// Validate Content-Type
	contentType := r.Header.Get("Content-Type")
	if !strings.Contains(contentType, "application/json") {
		ag.respondError(w, http.StatusBadRequest, "BAD_CONTENT_TYPE",
			"Content-Type must be application/json")
		atomic.AddUint64(&ag.totalErrors, 1)
		return
	}

	// Parse request body
	r.Body = http.MaxBytesReader(w, r.Body, ag.config.MaxRequestSize)
	defer r.Body.Close()

	var apiReq APIRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	err := decoder.Decode(&apiReq)
	if err != nil {
		if err == io.EOF {
			ag.respondError(w, http.StatusBadRequest, "EMPTY_BODY", "request body is empty")
		} else {
			ag.respondError(w, http.StatusBadRequest, "INVALID_JSON",
				fmt.Sprintf("invalid JSON: %v", err))
		}
		atomic.AddUint64(&ag.totalErrors, 1)
		return
	}

	// Validate request
	validationErr := ag.validateScheduleRequest(&apiReq)
	if validationErr != nil {
		ag.respondError(w, http.StatusBadRequest, "VALIDATION_ERROR", validationErr.Error())
		atomic.AddUint64(&ag.totalErrors, 1)
		return
	}

	// Create JobSpec from API request
	jobSpec := &common.JobSpec{
		RequestID: apiReq.RequestID,
		Name:      apiReq.Name,
		GPUCount:  apiReq.GPUCount,
		GPUType:   apiReq.GPUType,
		MemoryMB:  apiReq.MemoryMB,
		Priority:  apiReq.Priority,
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), ag.config.RequestTimeout)
	defer cancel()

	// Call global scheduler
	var decision *global.GlobalSchedulingDecision
	var scheduleErr error

	if apiReq.PreferRegion != "" {
		decision, scheduleErr = ag.globalScheduler.ScheduleJobWithRegionPreference(
			ctx, jobSpec, apiReq.PreferRegion)
	} else {
		decision, scheduleErr = ag.globalScheduler.ScheduleJob(ctx, jobSpec)
	}

	// Handle scheduling error
	if scheduleErr != nil {
		ag.log.Warn("Scheduling failed for job %s: %v", apiReq.RequestID, scheduleErr)
		ag.respondError(w, http.StatusConflict, "SCHEDULING_FAILED", scheduleErr.Error())
		atomic.AddUint64(&ag.totalErrors, 1)
		return
	}

	// Success response
	duration := time.Since(startTime)
	atomic.AddUint64(&ag.totalScheduled, 1)
	
	response := &APIResponse{
		Success:          true,
		RequestID:        decision.JobID,
		ClusterID:        decision.ClusterID,
		Region:           decision.Region,
		LocalScheduler:   decision.LocalSchedulerAddr,
		ClusterScore:     decision.ClusterScore,
		PlacementReasons: decision.PlacementReasons,
		Message:          fmt.Sprintf("Job scheduled on cluster %s in region %s", decision.ClusterID, decision.Region),
		Timestamp:        time.Now().Format(time.RFC3339),
		DurationMs:       duration.Seconds() * 1000,
	}

	ag.log.Info("Job %s scheduled on cluster %s (score=%.1f, duration=%.2fms)",
		decision.JobID, decision.ClusterID, decision.ClusterScore, response.DurationMs)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// handleHealthCheck: GET /health - Health check endpoint
func (ag *APIGateway) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		ag.respondError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			fmt.Sprintf("expected GET, got %s", r.Method))
		return
	}

	totalReq := atomic.LoadUint64(&ag.totalRequests)
	totalErr := atomic.LoadUint64(&ag.totalErrors)
	successRate := 0.0
	if totalReq > 0 {
		successRate = float64(totalReq-totalErr) / float64(totalReq) * 100.0
	}

	avgDuration := 0.0
	if totalReq > 0 {
		totalDuration := atomic.LoadInt64(&ag.requestDuration)
		avgDuration = float64(totalDuration) / float64(totalReq) / 1e6 // Convert to ms
	}

	response := &HealthCheckResponse{
		Status:        "healthy",
		ControlPlane:  "ares-scheduler",
		Timestamp:     time.Now().Format(time.RFC3339),
		TotalRequests: totalReq,
		TotalErrors:   totalErr,
		SuccessRate:   successRate,
		AvgDurationMs: avgDuration,
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// handleMetrics: GET /metrics - Metrics endpoint
func (ag *APIGateway) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		ag.respondError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			fmt.Sprintf("expected GET, got %s", r.Method))
		return
	}

	globalMetrics := ag.globalScheduler.GetMetrics()
	datacenterLoad := ag.globalScheduler.GetDatacenterLoad()
	federationStatus := ag.globalScheduler.GetFederationStatus()

	totalReq := atomic.LoadUint64(&ag.totalRequests)
	totalErr := atomic.LoadUint64(&ag.totalErrors)
	successRate := 0.0
	if totalReq > 0 {
		successRate = float64(totalReq-totalErr) / float64(totalReq) * 100.0
	}

	avgDuration := 0.0
	if totalReq > 0 {
		totalDuration := atomic.LoadInt64(&ag.requestDuration)
		avgDuration = float64(totalDuration) / float64(totalReq) / 1e6
	}

	response := &MetricsResponse{
		Timestamp:        time.Now().Format(time.RFC3339),
		TotalScheduled:   uint64(globalMetrics.TotalJobsScheduled),
		TotalFailed:      uint64(globalMetrics.TotalJobsFailed),
		SuccessRate:      successRate,
		AvgDurationMs:    avgDuration,
		DatacenterLoad:   datacenterLoad,
		FederationStatus: federationStatus,
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// handleDatacenterStatus: GET /status/datacenter - Datacenter status
func (ag *APIGateway) handleDatacenterStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		ag.respondError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			fmt.Sprintf("expected GET, got %s", r.Method))
		return
	}

	load := ag.globalScheduler.GetDatacenterLoad()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"timestamp": time.Now().Format(time.RFC3339),
		"data":      load,
	})
}

// handleFederationStatus: GET /status/federation - Federation health
func (ag *APIGateway) handleFederationStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		ag.respondError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			fmt.Sprintf("expected GET, got %s", r.Method))
		return
	}

	status := ag.globalScheduler.GetFederationStatus()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"timestamp": time.Now().Format(time.RFC3339),
		"data":      status,
	})
}

// handleClusterStatus: GET /status/cluster?cluster_id=xyz - Individual cluster status
func (ag *APIGateway) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		ag.respondError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			fmt.Sprintf("expected GET, got %s", r.Method))
		return
	}

	clusterID := r.URL.Query().Get("cluster_id")
	if clusterID == "" {
		ag.respondError(w, http.StatusBadRequest, "MISSING_PARAM", "cluster_id query parameter required")
		atomic.AddUint64(&ag.totalErrors, 1)
		return
	}

	cluster, err := ag.globalScheduler.GetClusterInfo(clusterID)
	if err != nil {
		ag.respondError(w, http.StatusNotFound, "CLUSTER_NOT_FOUND",
			fmt.Sprintf("cluster %s not found", clusterID))
		atomic.AddUint64(&ag.totalErrors, 1)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"timestamp": time.Now().Format(time.RFC3339),
		"cluster":   cluster,
	})
}

// handleCapacity: GET /info/capacity - Total datacenter capacity
func (ag *APIGateway) handleCapacity(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		ag.respondError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			fmt.Sprintf("expected GET, got %s", r.Method))
		return
	}

	totalCap := ag.globalScheduler.GetTotalCapacity()
	availCap := ag.globalScheduler.GetAvailableCapacity()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"timestamp": time.Now().Format(time.RFC3339),
		"total":     totalCap,
		"available": availCap,
	})
}

// handleListClusters: GET /info/clusters - List all clusters
func (ag *APIGateway) handleListClusters(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		ag.respondError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			fmt.Sprintf("expected GET, got %s", r.Method))
		return
	}

	clusters := ag.globalScheduler.ListClusters()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"timestamp": time.Now().Format(time.RFC3339),
		"count":     len(clusters),
		"clusters":  clusters,
	})
}

// ============================================================================
// VALIDATION
// ============================================================================

// validateScheduleRequest: Validate schedule request
func (ag *APIGateway) validateScheduleRequest(req *APIRequest) error {
	if req == nil {
		return fmt.Errorf("request is nil")
	}

	// Validate request ID
	if strings.TrimSpace(req.RequestID) == "" {
		return fmt.Errorf("request_id is required")
	}

	if len(req.RequestID) > 255 {
		return fmt.Errorf("request_id too long (max 255 characters)")
	}

	// Validate job name
	if strings.TrimSpace(req.Name) == "" {
		return fmt.Errorf("job name is required")
	}

	if len(req.Name) > 255 {
		return fmt.Errorf("job name too long (max 255 characters)")
	}

	// Validate GPU count
	if req.GPUCount < 0 || req.GPUCount > 256 {
		return fmt.Errorf("gpu_count must be 0-256, got %d", req.GPUCount)
	}

	// Validate GPU type
	if req.GPUCount > 0 && strings.TrimSpace(req.GPUType) == "" {
		return fmt.Errorf("gpu_type required when gpu_count > 0")
	}

	validGPUTypes := map[string]bool{
		"A100":  true,
		"A6000": true,
		"H100":  true,
		"V100":  true,
		"T4":    true,
		"P100":  true,
		"any":   true,
	}

	if req.GPUCount > 0 && !validGPUTypes[req.GPUType] {
		return fmt.Errorf("unsupported gpu_type: %s", req.GPUType)
	}

	// Validate memory
	if req.MemoryMB < 0 || req.MemoryMB > 1024*1024 {
		return fmt.Errorf("memory_mb must be 0-1048576, got %d", req.MemoryMB)
	}

	// Validate priority
	if req.Priority < 0 || req.Priority > 100 {
		return fmt.Errorf("priority must be 0-100, got %d", req.Priority)
	}

	// Validate region (if provided)
	validRegions := map[string]bool{
		"us-west":  true,
		"us-east":  true,
		"eu-west":  true,
		"ap-south": true,
		"ap-north": true,
	}

	if req.PreferRegion != "" && !validRegions[req.PreferRegion] {
		return fmt.Errorf("unsupported region: %s", req.PreferRegion)
	}

	// Validate timeout
	if req.Timeout < 0 || req.Timeout > 3600 {
		return fmt.Errorf("timeout must be 0-3600 seconds, got %d", req.Timeout)
	}

	// Validate retry count
	if req.RetryCount < 0 || req.RetryCount > 10 {
		return fmt.Errorf("retry_count must be 0-10, got %d", req.RetryCount)
	}

	return nil
}

// ============================================================================
// RESPONSE HELPERS
// ============================================================================

// respondError: Send error response
func (ag *APIGateway) respondError(w http.ResponseWriter, statusCode int, errorCode string, message string) {
	response := &ErrorResponse{
		Success:   false,
		ErrorCode: errorCode,
		Message:   message,
		Timestamp: time.Now().Format(time.RFC3339),
	}

	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(response)

	ag.log.Warn("API Error: %s - %s (status=%d)", errorCode, message, statusCode)
}

// generateRequestID: Generate unique request ID
func (ag *APIGateway) generateRequestID() string {
	return fmt.Sprintf("req-%d-%d", time.Now().UnixNano(), atomic.LoadUint64(&ag.totalRequests))
}

// ============================================================================
// SERVER LIFECYCLE
// ============================================================================

// Start: Start HTTP server
func (ag *APIGateway) Start() error {
	mux := ag.RegisterRoutes()

	addr := fmt.Sprintf(":%d", ag.config.Port)
	ag.server = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	ag.log.Info("API Gateway starting on %s", addr)

	go func() {
		err := ag.server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			ag.log.Error("Server error: %v", err)
		}
	}()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	ag.log.Info("API Gateway started successfully on port %d", ag.config.Port)

	return nil
}

// Stop: Stop HTTP server gracefully
func (ag *APIGateway) Stop(timeout time.Duration) error {
	if ag.server == nil {
		return fmt.Errorf("server not running")
	}

	ag.log.Info("Shutting down API Gateway...")

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	err := ag.server.Shutdown(ctx)
	if err != nil {
		ag.log.Error("Server shutdown error: %v", err)
		return err
	}

	ag.log.Info("API Gateway stopped")
	return nil
}

// ============================================================================
// STATS & MONITORING
// ============================================================================

// GetStats: Get gateway statistics
func (ag *APIGateway) GetStats() map[string]interface{} {
	totalReq := atomic.LoadUint64(&ag.totalRequests)
	totalErr := atomic.LoadUint64(&ag.totalErrors)
	totalScheduled := atomic.LoadUint64(&ag.totalScheduled)

	successRate := 0.0
	if totalReq > 0 {
		successRate = float64(totalReq-totalErr) / float64(totalReq) * 100.0
	}

	avgDuration := 0.0
	if totalReq > 0 {
		totalDuration := atomic.LoadInt64(&ag.requestDuration)
		avgDuration = float64(totalDuration) / float64(totalReq) / 1e6
	}

	return map[string]interface{}{
		"total_requests":     totalReq,
		"total_errors":       totalErr,
		"total_scheduled":    totalScheduled,
		"success_rate":       successRate,
		"avg_duration_ms":    avgDuration,
		"port":               ag.config.Port,
		"request_timeout":    ag.config.RequestTimeout.String(),
		"max_request_size":   ag.config.MaxRequestSize,
		"cors_enabled":       ag.config.EnableCORS,
		"prometheus_enabled": ag.config.EnablePrometheus,
	}
}

// Reset: Reset statistics
func (ag *APIGateway) Reset() {
	atomic.StoreUint64(&ag.totalRequests, 0)
	atomic.StoreUint64(&ag.totalErrors, 0)
	atomic.StoreUint64(&ag.totalScheduled, 0)
	atomic.StoreInt64(&ag.requestDuration, 0)
}

// ============================================================================
// RATE LIMITING (Optional, basic implementation)
// ============================================================================

// RateLimiter: Simple rate limiter
type RateLimiter struct {
	mu           sync.RWMutex
	maxRequests  int           // requests per window
	window       time.Duration // time window
	lastReset    time.Time
	requestCount int
}

// NewRateLimiter: Create rate limiter
func NewRateLimiter(maxRequests int, window time.Duration) *RateLimiter {
	return &RateLimiter{
		maxRequests:  maxRequests,
		window:       window,
		lastReset:    time.Now(),
		requestCount: 0,
	}
}

// Allow: Check if request is allowed
func (rl *RateLimiter) Allow() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()

	// Reset if window expired
	if now.Sub(rl.lastReset) > rl.window {
		rl.requestCount = 0
		rl.lastReset = now
	}

	if rl.requestCount < rl.maxRequests {
		rl.requestCount++
		return true
	}

	return false
}

// GetStatus: Get rate limiter status
func (rl *RateLimiter) GetStatus() map[string]interface{} {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	remaining := rl.maxRequests - rl.requestCount
	if remaining < 0 {
		remaining = 0
	}

	return map[string]interface{}{
		"max_requests": rl.maxRequests,
		"window":       rl.window.String(),
		"current":      rl.requestCount,
		"remaining":    remaining,
	}
}
