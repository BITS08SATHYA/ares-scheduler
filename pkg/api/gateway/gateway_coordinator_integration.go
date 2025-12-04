// File: pkg/api/gateway/gateway_coordinator_integration.go
// Integration between API Gateway (Layer 8) and Job Coordinator (Layer 10)
// Shows complete wiring of all 10 layers in the scheduling pipeline

package gateway

import (
	"context"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"sync/atomic"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	//"github.com/BITS08SATHYA/ares-scheduler/pkg/executor"
	//"github.com/BITS08SATHYA/ares-scheduler/pkg/gpu"
	//"github.com/BITS08SATHYA/ares-scheduler/pkg/idempotency"
	//"github.com/BITS08SATHYA/ares-scheduler/pkg/job"
	//"github.com/BITS08SATHYA/ares-scheduler/pkg/lease"
	//"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	//"github.com/BITS08SATHYA/ares-scheduler/pkg/orchestrator"
	//"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/global"
	//"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/redis"
	//clientv3 "go.etcd.io/etcd/client/v3"
)

// ============================================================================
// API GATEWAY WITH JOB COORDINATOR
// ============================================================================
// Enhanced API Gateway that integrates all 10 layers via Job Coordinator
//
// Architecture:
//
//   API Request (Layer 8)
//        ↓
//   API Gateway (Layer 8) ← YOU ARE HERE
//        ↓
//   Job Coordinator (Layer 10) ← ORCHESTRATES ALL
//        ↓
//   ┌────────────────────────────────────────┐
//   ├─ Layer 3: Idempotency Check            │
//   ├─ Layer 3: Lease Acquisition            │
//   ├─ Layer 3: Job Persistence              │
//   ├─ Layer 7: Global Scheduler             │
//   ├─ Layer 6: Local Scheduler (per cluster)│
//   ├─ Layer 5: GPU Topology                 │
//   ├─ Layer 9: Executor (create Pod)        │
//   └────────────────────────────────────────┘
//        ↓
//   Kubernetes Pod (executes job)

// APIGatewayWithCoordinator: Enhanced gateway with full coordinator integration
//type APIGatewayWithCoordinator struct {
//	*APIGateway // Embed existing gateway
//
//	// Layer 10: Job Coordinator (orchestrates all layers)
//	jobCoordinator *orchestrator.JobCoordinator
//	log            *logger.Logger
//}
//
//// ============================================================================
//// INITIALIZATION: Wire all 10 layers
//// ============================================================================
//
//// NewAPIGatewayWithCoordinator: Initialize complete pipeline
////
//// # Returns fully wired API Gateway with Job Coordinator managing all layers
////
//// This function demonstrates the complete architecture:
//// - Connects to etcd for distributed coordination
//// - Connects to Redis for caching & job queuing
//// - Initializes all 10 layers in proper dependency order
//// - Returns ready-to-use gateway
//func NewAPIGatewayWithCoordinator(
//	controlPlaneAddr string,
//	etcdEndpoints []string,
//	redisAddr string,
//	config *GatewayConfig,
//) (*APIGatewayWithCoordinator, error) {
//
//	log := logger.Get()
//	log.Info("Initializing API Gateway with Job Coordinator...")
//
//	// ========================================================================
//	// LAYER 2: Connect to storage backends
//	// ========================================================================
//
//	log.Info("Layer 2: Connecting to etcd and Redis...")
//
//	// etcd: For distributed coordination, leases, state management
//	etcdClient, err := clientv3.New(clientv3.Config{
//		Endpoints:   etcdEndpoints,
//		DialTimeout: 10 * time.Second,
//	})
//	if err != nil {
//		log.Error("Failed to connect to etcd: %v", err)
//		return nil, fmt.Errorf("etcd connection failed: %w", err)
//	}
//
//	// Redis: For fast caching, queuing, deduplication
//	redisClient, err := redis.NewRedisClient(redisAddr, "", 0)
//	if err != nil {
//		log.Error("Failed to connect to Redis: %v", err)
//		etcdClient.Close()
//		return nil, fmt.Errorf("redis connection failed: %w", err)
//	}
//
//	log.Info("✓ Connected to etcd and Redis")
//
//	// ========================================================================
//	// LAYER 3: Persistence & Coordination
//	// ========================================================================
//
//	log.Info("Layer 3: Initializing persistence layer...")
//
//	// Job Store: Persists job state to etcd
//	jobStore := job.NewETCDJobStore(etcdClient)
//
//	// Lease Manager: Distributed leases for exactly-once semantics
//	leaseManager := lease.NewLeaseManager(etcdClient, controlPlaneAddr, &SimpleLogger{})
//
//	// Idempotency Manager: Request deduplication via Redis
//	idempotencyManager := idempotency.NewIdempotencyManager(redisClient)
//
//	log.Info("✓ Layer 3 initialized: JobStore, LeaseManager, IdempotencyManager")
//
//	// ========================================================================
//	// LAYER 5: GPU Discovery & Topology
//	// ========================================================================
//
//	log.Info("Layer 5: Initializing GPU discovery and topology...")
//
//	gpuDiscovery := gpu.NewGPUDiscovery(redisClient)
//	topologyManager := gpu.NewGPUTopologyManager(redisClient, gpuDiscovery)
//
//	log.Info("✓ Layer 5 initialized: GPUDiscovery, TopologyManager")
//
//	// ========================================================================
//	// LAYER 9: Executor (creates Kubernetes Pods)
//	// ========================================================================
//
//	log.Info("Layer 9: Initializing Kubernetes executor...")
//
//	mockK8sClient := executor.NewMockK8sClient()
//	executorConfig := &executor.ExecutorConfig{
//		ClusterID:                "global-control-plane",
//		Namespace:                "default",
//		DefaultTimeout:           1 * time.Hour,
//		DefaultMemoryMB:          1024,
//		DefaultCPUMillis:         500,
//		HealthCheckInterval:      5 * time.Second,
//		MaxConcurrentJobs:        1000,
//		ImageRegistry:            "docker.io",
//		DefaultJobImage:          "ares-job:latest",
//		RestartPolicy:            "OnFailure",
//		ImagePullPolicy:          "IfNotPresent",
//		EnableGPUSupport:         true,
//		LogCollectionEnabled:     true,
//		MetricsCollectionEnabled: true,
//	}
//
//	executorService, err := executor.NewExecutor(
//		"global-control-plane",
//		mockK8sClient,
//		executorConfig,
//	)
//	if err != nil {
//		log.Error("Failed to create executor: %v", err)
//		redisClient.Close()
//		etcdClient.Close()
//		return nil, fmt.Errorf("executor creation failed: %w", err)
//	}
//
//	log.Info("✓ Layer 9 initialized: Executor with Mock K8s client")
//
//	// ========================================================================
//	// LAYER 7: Global Scheduler (datacenter-level decisions)
//	// ========================================================================
//
//	log.Info("Layer 7: Initializing global scheduler...")
//
//	globalScheduler := global.NewGlobalScheduler(controlPlaneAddr, redisClient)
//
//	log.Info("✓ Layer 7 initialized: GlobalScheduler")
//
//	// ========================================================================
//	// LAYER 10: Job Coordinator (orchestrates all)
//	// ========================================================================
//
//	log.Info("Layer 10: Initializing job coordinator...")
//
//	jobCoordinator := orchestrator.NewJobCoordinator(
//		idempotencyManager,
//		leaseManager,
//		jobStore,
//		globalScheduler,
//		executorService,
//	)
//
//	log.Info("✓ Layer 10 initialized: JobCoordinator")
//
//	// ========================================================================
//	// LAYER 8: API Gateway
//	// ========================================================================
//
//	log.Info("Layer 8: Initializing API gateway...")
//
//	if config == nil {
//		config = DefaultGatewayConfig
//	}
//
//	baseGateway := &APIGateway{
//		globalScheduler: globalScheduler,
//		log:             log,
//		config:          config,
//		activeJobs:      make(map[string]*ExecutionContext),
//		completedJobs:   make(map[string]*ExecutionResult),
//		podRegistry:     make(map[string]*PodInfo),
//		k8sClient:       mockK8sClient,
//	}
//
//	gatewayWithCoordinator := &APIGatewayWithCoordinator{
//		APIGateway:     baseGateway,
//		jobCoordinator: jobCoordinator,
//		log:            log,
//	}
//
//	log.Info("✓ Layer 8 initialized: API Gateway")
//	log.Info("")
//	log.Info("✓✓✓ COMPLETE: All 10 layers initialized and wired ✓✓✓")
//	log.Info("")
//	log.Info("Architecture summary:")
//	log.Info("  Layer  1: Logger")
//	log.Info("  Layer  2: etcd + Redis")
//	log.Info("  Layer  3: JobStore + LeaseManager + IdempotencyManager")
//	log.Info("  Layer  5: GPUDiscovery + TopologyManager")
//	log.Info("  Layer  6: LocalScheduler (runs on each cluster)")
//	log.Info("  Layer  7: GlobalScheduler (datacenter-level)")
//	log.Info("  Layer  9: Executor (Kubernetes Pod creation)")
//	log.Info("  Layer 10: JobCoordinator (orchestrates all)")
//	log.Info("  Layer  8: API Gateway (entry point) ← YOU ARE HERE")
//	log.Info("")
//
//	return gatewayWithCoordinator, nil
//}

// ============================================================================
// MAIN INTEGRATION: handleScheduleJob with Job Coordinator
// ============================================================================

// HandleScheduleJobWithCoordinator: Schedule job using complete pipeline
//
// This is the main integration point where the API Gateway delegates to
// the Job Coordinator, which orchestrates all 10 layers.
//
// Pipeline:
//  1. Validate request
//  2. Create JobSpec from API request
//  3. Call Job Coordinator (manages layers 3-9)
//  4. Return result to client
//
// Returns: API response or error
func (ag *APIGatewayWithCoordinator) HandleScheduleJobWithCoordinator(
	ctx context.Context,
	apiReq *APIRequest,
) (*APIResponse, error) {

	startTime := time.Now()

	// ========================================================================
	// STEP 1: Validate request
	// ========================================================================

	if err := ag.validateScheduleRequest(apiReq); err != nil {
		ag.log.Warn("Request validation failed: %v", err)
		atomic.AddUint64(&ag.totalErrors, 1)
		return &APIResponse{
			Success:   false,
			RequestID: apiReq.RequestID,
			Message:   fmt.Sprintf("validation error: %v", err),
			Timestamp: time.Now().Format(time.RFC3339),
		}, nil
	}

	ag.log.Info("Processing job request: %s (gpus=%d, priority=%d, timeout=%ds)",
		apiReq.RequestID, apiReq.GPUCount, apiReq.Priority, apiReq.Timeout)

	// ========================================================================
	// STEP 2: Create JobSpec from API request
	// ========================================================================

	// Convert API request to internal JobSpec
	// This is the data structure that flows through all 10 layers
	jobSpec := &common.JobSpec{
		// Deduplication (Layer 3)
		RequestID: apiReq.RequestID,

		// Job description
		Name:    apiReq.Name,
		Image:   "ares-job:latest", // Default, can be made configurable
		Command: []string{},        // Can be extended from request

		// GPU requirements (Layer 5, 13)
		GPUCount:       apiReq.GPUCount,
		GPUType:        apiReq.GPUType,
		PreferNVLink:   true, // Feature 4: Topology-aware
		PreferSameNUMA: true, // Feature 4: NUMA aware

		// Resources
		MemoryMB:  apiReq.MemoryMB,
		CPUMillis: 500, // Default

		// Priority (Layer 5: Feature 5)
		Priority: apiReq.Priority,

		// Retry policy (Feature 21)
		TimeoutSecs: apiReq.Timeout,
		MaxRetries:  apiReq.RetryCount,

		// SLA (Feature 22)
		TargetLatencyMs: 5000, // 5 second SLA target
	}

	ag.log.Debug("Created JobSpec: name=%s, gpus=%d, priority=%d",
		jobSpec.Name, jobSpec.GPUCount, jobSpec.Priority)

	// ========================================================================
	// STEP 3: Call Job Coordinator (THE MAIN INTEGRATION POINT)
	// ========================================================================
	//
	// This is where the magic happens. Job Coordinator orchestrates:
	//
	// ┌─────────────────────────────────────────────────────────┐
	// │ Job Coordinator Pipeline:                               │
	// ├─────────────────────────────────────────────────────────┤
	// │ 1. Check for duplicate (Layer 3: Idempotency)           │
	// │    → If duplicate, return cached result immediately    │
	// │                                                          │
	// │ 2. Acquire distributed lease (Layer 3: Lease)          │
	// │    → Atomic operation: only ONE executor wins           │
	// │                                                          │
	// │ 3. Persist job to etcd (Layer 3: JobStore)             │
	// │    → Job survives coordinator crash                     │
	// │                                                          │
	// │ 4. Call GlobalScheduler (Layer 7)                       │
	// │    → Selects best cluster for job                       │
	// │    → Makes strategic datacenter-level decision          │
	// │                                                          │
	// │ 5. GlobalScheduler calls LocalScheduler (Layer 6)       │
	// │    → HTTP call to remote cluster                        │
	// │    → LocalScheduler selects node + GPU                  │
	// │    → Uses GPU topology (NVLink, NUMA) from Layer 5      │
	// │                                                          │
	// │ 6. LocalScheduler calls Executor (Layer 9)              │
	// │    → Creates Kubernetes Pod                             │
	// │    → Pod runs on selected node with GPUs                │
	// │                                                          │
	// │ 7. Monitor execution (Layer 10)                         │
	// │    → Track pod until completion                         │
	// │    → Implement retries on failure                       │
	// └─────────────────────────────────────────────────────────┘
	//
	// Result: SchedulingResult with:
	//   - JobID: unique identifier
	//   - ClusterID: which cluster accepted the job
	//   - NodeID: which node in cluster
	//   - GPUIndices: which GPUs allocated
	//   - LocalSchedulerAddr: how to contact local scheduler

	ag.log.Info("Calling Job Coordinator (orchestrates all 10 layers)...")

	schedulingResult, err := ag.jobCoordinator.ScheduleJob(ctx, jobSpec)
	if err != nil {
		ag.log.Error("Job Coordinator failed: %v", err)
		atomic.AddUint64(&ag.totalErrors, 1)

		return &APIResponse{
			Success:   false,
			RequestID: apiReq.RequestID,
			Message:   fmt.Sprintf("scheduling failed: %v", err),
			Timestamp: time.Now().Format(time.RFC3339),
		}, nil
	}

	// ========================================================================
	// STEP 4: Build successful response
	// ========================================================================

	duration := time.Since(startTime)
	atomic.AddUint64(&ag.totalScheduled, 1)
	atomic.AddInt64(&ag.requestDuration, duration.Nanoseconds())

	response := &APIResponse{
		Success:          true,
		RequestID:        schedulingResult.JobID,
		ClusterID:        schedulingResult.ClusterID,
		LocalScheduler:   schedulingResult.LocalSchedulerAddr,
		ClusterScore:     schedulingResult.ClusterScore,
		PlacementReasons: schedulingResult.PlacementReasons,
		Message:          "✓ Job scheduled successfully",
		Timestamp:        time.Now().Format(time.RFC3339),
		DurationMs:       duration.Seconds() * 1000,
	}

	ag.log.Info("✓ Job %s scheduled on cluster %s (duration=%.2fms)",
		schedulingResult.JobID, schedulingResult.ClusterID, response.DurationMs)

	return response, nil
}

// ============================================================================
// MONITORING & OBSERVABILITY ENDPOINTS
// ============================================================================

// GetCoordinatorStats: Get overall coordinator statistics
// Returns: Combined stats from gateway, scheduler, and coordinator
func (ag *APIGatewayWithCoordinator) GetCoordinatorStats() map[string]interface{} {
	gatewayStats := ag.GetStats()
	globalMetrics := ag.globalScheduler.GetMetrics()
	federationStatus := ag.globalScheduler.GetFederationStatus()
	datacenterLoad := ag.globalScheduler.GetDatacenterLoad()

	return map[string]interface{}{
		"gateway": gatewayStats,
		"global_scheduler": map[string]interface{}{
			"total_scheduled": globalMetrics.TotalJobsScheduled,
			"total_failed":    globalMetrics.TotalJobsFailed,
			"success_rate":    ag.globalScheduler.SuccessRate(),
			"last_updated":    globalMetrics.LastUpdated,
		},
		"federation_status": federationStatus,
		"datacenter_load":   datacenterLoad,
		"timestamp":         time.Now().Format(time.RFC3339),
	}
}

// GetJobStatus: Get status of a specific job
func (ag *APIGatewayWithCoordinator) GetJobStatus(
	ctx context.Context,
	jobID string,
) map[string]interface{} {

	jobStatus, err := ag.jobCoordinator.GetJobStatus(ctx, jobID)
	if err != nil {
		return map[string]interface{}{
			"error": fmt.Sprintf("job not found: %v", err),
		}
	}

	return map[string]interface{}{
		"job_id":       jobStatus.ID,
		"status":       jobStatus.Status,
		"cluster":      jobStatus.ClusterID,
		"node":         jobStatus.NodeID,
		"submitted_at": jobStatus.SubmitTime.Format(time.RFC3339),
		"started_at":   jobStatus.StartTime.Format(time.RFC3339),
		"gpu_count":    jobStatus.Spec.GPUCount,
		"priority":     jobStatus.Spec.Priority,
		"attempts":     jobStatus.Attempts,
	}
}

// CancelJob: Cancel a running job
func (ag *APIGatewayWithCoordinator) CancelJob(ctx context.Context, jobID string) error {
	ag.log.Info("Cancelling job %s", jobID)
	return ag.jobCoordinator.CancelJob(ctx, jobID)
}

// RetryJob: Manually retry a failed job
func (ag *APIGatewayWithCoordinator) RetryJob(ctx context.Context, jobID string) error {
	ag.log.Info("Retrying job %s", jobID)
	return ag.jobCoordinator.RetryJob(ctx, jobID)
}

// ============================================================================
// HELPER: Simple Logger implementation
// ============================================================================
// LeaseManager expects a Logger interface; this is a minimal implementation

type SimpleLogger struct{}

func (sl *SimpleLogger) Infof(format string, args ...interface{}) {
	logger.Get().Info(format, args...)
}

func (sl *SimpleLogger) Warnf(format string, args ...interface{}) {
	logger.Get().Warn(format, args...)
}

func (sl *SimpleLogger) Errorf(format string, args ...interface{}) {
	logger.Get().Error(format, args...)
}
