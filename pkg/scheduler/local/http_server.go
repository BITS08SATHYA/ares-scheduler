package local

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/executor"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"net/http"
	"time"
)

// ============================================================================
// LOCAL SCHEDULER HTTP SERVER (New integration layer)
// ============================================================================
// Allows GlobalScheduler to call this cluster's LocalScheduler via HTTP

type LocalSchedulerServer struct {
	scheduler *LocalScheduler
	port      int
	log       *logger.Logger
	server    *http.Server
	executor  *executor.Executor
}

// NewLocalSchedulerServer creates HTTP server for LocalScheduler
func NewLocalSchedulerServer(scheduler *LocalScheduler, port int, myExecutor *executor.Executor) *LocalSchedulerServer {
	return &LocalSchedulerServer{
		scheduler: scheduler,
		port:      port,
		log:       logger.Get(),
		executor:  myExecutor,
	}
}

// Start starts the HTTP server
func (lss *LocalSchedulerServer) Start() error {
	mux := http.NewServeMux()

	// Register HTTP handlers
	mux.HandleFunc("/schedule", lss.handleSchedule)
	mux.HandleFunc("/health", lss.handleHealth)
	mux.HandleFunc("/status", lss.handleStatus)
	mux.HandleFunc("/metrics", lss.handleMetrics)

	addr := fmt.Sprintf(":%d", lss.port)
	lss.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	lss.log.Info("LocalScheduler HTTP server starting on %s", addr)

	go func() {
		if err := lss.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			lss.log.Error("Server error: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)
	lss.log.Info("LocalScheduler HTTP server ready")
	return nil
}

// Stop stops the HTTP server
func (lss *LocalSchedulerServer) Stop(timeout time.Duration) error {
	if lss.server == nil {
		return fmt.Errorf("server not running")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return lss.server.Shutdown(ctx)
}

// ========================================================================
// HTTP HANDLERS
// ========================================================================

// handleSchedule: POST /schedule - Schedule job on this cluster
// It should call the executor after scheduling decision
func (lss *LocalSchedulerServer) handleSchedule(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "expected POST",
		})
		return
	}

	// Parse request
	var req struct {
		JobID   string          `json:"job_id"`
		JobSpec *common.JobSpec `json:"job_spec"`
		Command []string        `json:"command,omitempty"` // ✅ ADDED
		Args    []string        `json:"args,omitempty"`    // ✅ ADDED
		Image   string          `json:"image,omitempty"`   // ✅ ADDED
	}

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error": fmt.Sprintf("invalid request: %v", err),
		})
		return
	}

	lss.log.Info("Received schedule request for job %s", req.JobID)

	// Step 1: LocalScheduler makes scheduling decision
	decision, err := lss.scheduler.ScheduleJob(r.Context(), req.JobSpec)
	if err != nil {
		lss.log.Warn("Scheduling failed: %v", err)
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	// Step 2: Execute Job (create Pod)
	if lss.executor != nil {
		lss.log.Info("Calling Executor to create Pod...")

		k8dec := &executor.K8Decision{
			JobID:            decision.JobID,
			NodeID:           decision.NodeID,
			GPUIndices:       decision.GPUIndices,
			NodeScore:        decision.NodeScore,
			GPUAffinityScore: decision.GPUAffinityScore,
			PlacementReasons: decision.PlacementReasons,
			ScheduledAt:      decision.ScheduledAt,
			Command:          req.Command,
			Args:             req.Args,
			Image:            req.Image,
		}

		// ✅ FIXED: Set defaults if not provided
		if len(k8dec.Command) == 0 && len(k8dec.Args) == 0 {
			k8dec.Command = []string{"sh", "-c"}
			k8dec.Args = []string{"nvidia-smi && echo 'Job started' && sleep 120"}
			lss.log.Info("Using default command/args for testing")
		}

		execCtx, err := lss.executor.ExecuteJob(r.Context(), k8dec)
		if err != nil {
			lss.log.Error("Pod creation failed: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"error":   fmt.Sprintf("executor failed: %v", err),
			})
			return
		}

		lss.log.Info("Pod created: %s for job %s", execCtx.PodName, req.JobID)
	} else {
		lss.log.Warn("Executor not configured - Pod not created (OK for testing)")
	}

	// Step 3: Return Decision
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":  true,
		"decision": decision,
	})
}

// handleHealth: GET /health - Get cluster health
func (lss *LocalSchedulerServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	health := lss.scheduler.GetHealthStatus()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"cluster_id": lss.scheduler.clusterID,
		"healthy":    len(health) > 0,
		"nodes":      health,
	})
}

// handleStatus: GET /status - Get cluster status
func (lss *LocalSchedulerServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	status := lss.scheduler.GetClusterLoad()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(status)
}

// handleMetrics: GET /metrics - Get scheduler metrics
func (lss *LocalSchedulerServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	metrics := lss.scheduler.GetMetrics()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"total_scheduled": metrics.TotalJobsScheduled,
		"total_failed":    metrics.TotalJobsFailed,
		"success_rate":    lss.scheduler.SuccessRate(),
		"last_updated":    metrics.LastUpdated,
	})
}
