package executor

import (
	"context"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/job"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/lease"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	_ "k8s.io/client-go/kubernetes"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Scope: Single cluster (runs on each cluster's control plane)
type Executor struct {
	ClusterID    string
	ControlPlane string
	Log          *logger.Logger
	Config       *ExecutorConfig

	JobStore     job.JobStore
	LeaseManager *lease.LeaseManager

	// Job tracking
	JobsMu        sync.RWMutex
	ActiveJobs    map[string]*ExecutionContext // JobID -> ExecutionContext
	CompletedJobs map[string]*ExecutionResult  // JobID -> ExecutionResult

	// Metrics (atomic for thread-safety)
	TotalJobs       uint64
	TotalSuccessful uint64
	TotalFailed     uint64
	TotalCancelled  uint64
	TotalDuration   int64 // nanoseconds, atomic

	// Pod management
	PodMu       sync.RWMutex
	PodRegistry map[string]*PodInfo // PodName -> PodInfo

	K8sClient K8sClient

	OnJobRunning func(jobID string)
	// Callback when job completes (for resource release)
	OnJobComplete func(jobID string, nodeID string, gpuCount int, memoryMB int)
}

// ExecutorConfig: Configuration for executor
type ExecutorConfig struct {
	ClusterID                string        // e.g., "cluster-us-west-2a"
	Namespace                string        // e.g., "default" or "ares-jobs"
	DefaultTimeout           time.Duration // Pod timeout (default: 1 hour)
	DefaultMemoryMB          int           // Pod default memory
	DefaultCPUMillis         int           // Pod default CPU (millicores)
	HealthCheckInterval      time.Duration // Pod health check frequency
	MaxConcurrentJobs        int           // Max concurrent Pods
	ImageRegistry            string        // e.g., "docker.io", "ghcr.io"
	DefaultJobImage          string        // e.g., "ares-job:latest"
	RestartPolicy            string        // Always, OnFailure, Never
	ImagePullPolicy          string        // Always, IfNotPresent, Never
	EnableGPUSupport         bool          // Enable GPU requests
	LogCollectionEnabled     bool          // Collect Pod logs
	MetricsCollectionEnabled bool          // Collect Pod metrics
}

// ExecutionContext: Runtime context for a job being executed
type ExecutionContext struct {
	JobID         string
	LocalDecision K8Decision // From Layer 6 (Local Scheduler)
	PodName       string
	Namespace     string
	StartTime     time.Time
	Timeout       time.Duration
	Status        JobStatus
	LastUpdated   time.Time
	CurrentPhase  PodPhase
	NodeID        string                 // From LocalSchedulingDecision
	GPUIndices    []int                  // From LocalSchedulingDecision
	Logs          string                 // Job logs (if collected)
	Metrics       map[string]interface{} // Pod metrics
}

// ExecutionResult: Final result of job execution
type ExecutionResult struct {
	JobID        string
	PodName      string
	Status       JobStatus
	Phase        PodPhase
	StartTime    time.Time
	EndTime      time.Time
	Duration     time.Duration
	ExitCode     int
	ErrorMessage string
	Logs         string
	Metrics      map[string]interface{}
	CompletedAt  time.Time
}

// PodInfo: Information about created Kubernetes Pod
type PodInfo struct {
	PodName       string
	Namespace     string
	JobID         string
	NodeID        string
	Phase         PodPhase
	CreatedAt     time.Time
	StartedAt     time.Time
	FinishedAt    time.Time
	ContainerID   string
	GPUIndices    []int
	ResourceUsage map[string]interface{}
	Ready         bool
}

// JobStatus: High-level job status
type JobStatus string

const (
	StatusPending    JobStatus = "Pending"
	StatusRunning    JobStatus = "Running"
	StatusSuccessful JobStatus = "Successful"
	StatusFailed     JobStatus = "Failed"
	StatusCancelled  JobStatus = "Cancelled"
	StatusUnknown    JobStatus = "Unknown"
)

// PodPhase: Kubernetes Pod phase
type PodPhase string

const (
	PhasePending   PodPhase = "Pending"
	PhaseRunning   PodPhase = "Running"
	PhaseSucceeded PodPhase = "Succeeded"
	PhaseFailed    PodPhase = "Failed"
	PhaseUnknown   PodPhase = "Unknown"
)

// K8sClient: Interface for Kubernetes operations (mock in this file)
type K8sClient interface {
	CreatePod(ctx context.Context, pod *PodSpec) (podName string, err error)
	GetPod(ctx context.Context, podName string) (*PodInfo, error)
	DeletePod(ctx context.Context, podName string) error
	ListPods(ctx context.Context) ([]*PodInfo, error)
	GetPodLogs(ctx context.Context, podName string) (string, error)
	GetPodMetrics(ctx context.Context, podName string) (map[string]interface{}, error)
	WatchPod(ctx context.Context, podName string, callback func(*PodInfo)) error
}

// PodSpec: Specification for Kubernetes Pod
type PodSpec struct {
	PodName         string
	Namespace       string
	Image           string
	Command         []string
	Args            []string
	ImagePullPolicy string
	EnvVars         map[string]string
	MemoryMB        int
	CPUMillis       int
	GPUCount        int
	GPUIndices      []int
	Timeout         time.Duration
	RestartPolicy   string
	NodeID          string
	Labels          map[string]string
}

// Default config
var DefaultExecutorConfig = &ExecutorConfig{
	Namespace:           "ares-system",
	DefaultTimeout:      1 * time.Hour,
	DefaultMemoryMB:     1024,
	DefaultCPUMillis:    500,
	HealthCheckInterval: 5 * time.Second,
	MaxConcurrentJobs:   100,
	//ImageRegistry:            "docker.io",
	ImageRegistry:            "",
	DefaultJobImage:          "nvidia/cuda:13.0.2-runtime-ubuntu22.04",
	RestartPolicy:            "OnFailure",
	ImagePullPolicy:          "IfNotPresent",
	EnableGPUSupport:         true,
	LogCollectionEnabled:     true,
	MetricsCollectionEnabled: true,
}

type K8Decision struct {
	JobID             string
	NodeID            string
	GPUIndices        []int
	NodeScore         float64
	GPUAffinityScore  float64
	PlacementReasons  []string
	ScheduledAt       time.Time
	Command           []string
	Args              []string
	Image             string
	LeaseID           int64  // Fencing: Prevents split-brain at pod level
	FencingToken      string // Fencing: Human-readable fencing token for validation
	CheckpointEnabled bool
	CheckpointPath    string // Where to write checkpoints
	CheckpointRestore string // Last checkpoint to restore from (empty = fresh)
	CheckpointMeta    string // Metadata from last checkpoint
}

// ============================================================================
// EXECUTOR SERVICE (Layer 9)
// ============================================================================
// FIXES APPLIED:
// 1.  Fixed NewExecutor signature: k8sClient is common.K8sClient (interface), NOT *common.K8sClient (pointer)
// 2.  Removed 'log' parameter (we create our own logger inside)
// 3.  Fixed to match 3-parameter call from cmd/local/main.go
// 4.  Fixed k8sClient field assignment (removed dereference)
//
// EXPLANATION OF FIX:
//  WRONG: func NewExecutor(clusterID string, k8sClient *common.K8sClient, log logger.Logger, config...)
//  Why: Can't create *common.K8sClient (pointer to interface) - Go doesn't allow this
//  The concrete type is *kubernetes.K8sClientImpl which satisfies common.K8sClient interface
//
//  RIGHT: func NewExecutor(clusterID string, k8sClient common.K8sClient, config...)
//  Why: Interface types don't need to be pointers
//  *kubernetes.K8sClientImpl satisfies common.K8sClient interface
// ============================================================================

// NewExecutor: Create new executor
// ✅ FIXED SIGNATURE:
//   - k8sClient: common.K8sClient (INTERFACE, not pointer to interface)
//   - No log parameter (create own inside)
//   - 3 parameters total (matches cmd/local/main.go call)
func NewExecutor(
	clusterID string,
	k8sClient K8sClient, //  FIX: Interface type, NOT *common.K8sClient
	config *ExecutorConfig,
	jobStore job.JobStore,
	leaseManager *lease.LeaseManager,
) (*Executor, error) {

	if clusterID == "" {
		return nil, fmt.Errorf("cluster ID cannot be empty")
	}

	if k8sClient == nil {
		return nil, fmt.Errorf("kubernetes client cannot be nil")
	}

	if config == nil {
		config = DefaultExecutorConfig
	}

	if config.Namespace == "" {
		config.Namespace = "ares-system"
	}

	if config.DefaultTimeout <= 0 {
		config.DefaultTimeout = 1 * time.Hour
	}

	if config.HealthCheckInterval <= 0 {
		config.HealthCheckInterval = 5 * time.Second
	}

	if config.MaxConcurrentJobs <= 0 {
		config.MaxConcurrentJobs = 100
	}

	if jobStore == nil {
		return nil, fmt.Errorf("Job Store cannot be nil")
	}

	if leaseManager == nil {
		return nil, fmt.Errorf("Lease Manager cannot be nil")
	}

	executor := &Executor{
		ClusterID:     clusterID,
		ControlPlane:  fmt.Sprintf("ares-executor-%s", clusterID),
		Log:           logger.Get(), // Create our own logger
		Config:        config,
		ActiveJobs:    make(map[string]*ExecutionContext),
		CompletedJobs: make(map[string]*ExecutionResult),
		PodRegistry:   make(map[string]*PodInfo),
		K8sClient:     k8sClient, // FIX: Direct assignment (not dereferencing)
		JobStore:      jobStore,
		LeaseManager:  leaseManager,
	}

	executor.Log.Info("Executor Initialized for cluster %s", clusterID)
	executor.Log.Info("Namespace: %s", config.Namespace)
	executor.Log.Info("Max Concurrent jobs: %s", config.MaxConcurrentJobs)
	executor.Log.Info("Default timeout: %v", config.DefaultTimeout)
	executor.Log.Info("Pod Lifecycle monitoring enabled")

	return executor, nil
}

// ============================================================================
// JOB EXECUTION
// ============================================================================

// ExecuteJob: Execute job by creating Kubernetes Pod
// Input: LocalSchedulingDecision (from LocalScheduler - Layer 6)
// Output: ExecutionContext (tracking info)
func (ex *Executor) ExecuteJob(
	ctx context.Context,
	decision *K8Decision,
) (*ExecutionContext, error) {

	if decision == nil {
		return nil, fmt.Errorf("local scheduling decision cannot be nil")
	}

	if decision.JobID == "" {
		return nil, fmt.Errorf("job ID cannot be empty")
	}

	if decision.NodeID == "" {
		return nil, fmt.Errorf("node ID cannot be empty")
	}

	ex.Log.Info("Executing job %s on node %s with GPUs %v",
		decision.JobID, decision.NodeID, decision.GPUIndices)

	//Check concurrency limit
	ex.JobsMu.RLock()
	activeCount := len(ex.ActiveJobs)
	ex.JobsMu.RUnlock()

	if activeCount >= ex.Config.MaxConcurrentJobs {
		return nil, fmt.Errorf("max concurrent jobs (%d) reached", ex.Config.MaxConcurrentJobs)
	}

	// Create execution context
	execCtx := &ExecutionContext{
		JobID: decision.JobID,
		LocalDecision: K8Decision{
			JobID:            decision.JobID,
			NodeID:           decision.NodeID,
			GPUIndices:       decision.GPUIndices,
			NodeScore:        decision.NodeScore,
			GPUAffinityScore: decision.GPUAffinityScore,
			PlacementReasons: decision.PlacementReasons,
			ScheduledAt:      decision.ScheduledAt,
		},
		Namespace:    ex.Config.Namespace,
		StartTime:    time.Now(),
		Timeout:      ex.Config.DefaultTimeout,
		Status:       StatusPending,
		CurrentPhase: PhasePending,
		NodeID:       decision.NodeID,
		GPUIndices:   decision.GPUIndices,
		Metrics:      make(map[string]interface{}),
	}

	// Generate Pod name (max 63 chars in Kubernetes)
	podName := generatePodName(decision.JobID)
	execCtx.PodName = podName

	// Create Pod spec
	podSpec := createPodSpec(ex, decision, podName)

	// Create Pod in Kubernetes
	createdPodName, err := ex.K8sClient.CreatePod(ctx, podSpec)
	if err != nil {
		ex.Log.Error("Failed to create Pod for job %s: %v", decision.JobID, err)
		atomic.AddUint64(&ex.TotalFailed, 1)
		return nil, fmt.Errorf("failed to create Pod: %w", err)
	}

	ex.Log.Debug("Creating Pod %s for job %s", podName, decision.JobID)

	execCtx.PodName = createdPodName
	execCtx.Status = StatusRunning
	ex.Log.Info("Pod created for job %s: %s (node=%s, gpus=%v)",
		decision.JobID, createdPodName, decision.NodeID, decision.GPUIndices)

	// Store execution context
	ex.JobsMu.Lock()
	ex.ActiveJobs[decision.JobID] = execCtx
	ex.JobsMu.Unlock()

	// Increment metrics
	atomic.AddUint64(&ex.TotalJobs, 1)

	// Start background monitoring
	//go ex.monitorJobExecution(ctx, execCtx)

	go func() {
		err := ex.monitorAndUpdateJob(context.Background(), execCtx, decision.JobID)
		if err != nil && err != context.Canceled {
			// Log error but don't crash (monitoring is best-effort)
			ex.Log.Error("Monitoring failed for job %s: %v", decision.JobID, err)
		}
	}()

	ex.Log.Info("Started Pod Lifecycle monitoring for job %s", decision.JobID)

	return execCtx, nil
}

// createPodSpec: Create Kubernetes Pod specification
// CORRECTED: Uses NodeID and GPUIndices from LocalSchedulingDecision
func createPodSpec(
	ex *Executor,
	decision *K8Decision,
	podName string,
) *PodSpec {

	// Build image name
	//imageName := ex.Config.DefaultJobImage

	imageName := decision.Image // Use the image from the job spec
	//imageName := decision.Image
	ex.Log.Debug("Checking-1 Original Payload Passed Image Name: %s", imageName)

	if imageName == "" {
		imageName = ex.Config.DefaultJobImage // Fall back to default only if empty
	}

	if ex.Config.ImageRegistry != "" && !contains(imageName, "/") {
		imageName = fmt.Sprintf("%s/%s", ex.Config.ImageRegistry, imageName)
	}
	//imageName := decision.Image
	ex.Log.Debug("Passed Image Name: %s", imageName)

	// Convert GPU indices to strings for environment variable
	gpuDeviceStrs := make([]string, len(decision.GPUIndices))
	for i, idx := range decision.GPUIndices {
		gpuDeviceStrs[i] = fmt.Sprintf("%d", idx)
	}

	// Environment variables
	envVars := map[string]string{
		"ARES_JOB_ID":        decision.JobID,
		"ARES_CLUSTER_ID":    ex.ClusterID,
		"ARES_NODE_ID":       decision.NodeID,
		"ARES_ASSIGNED_GPUS": join(gpuDeviceStrs, ","),
		"ARES_GPU_COUNT":     fmt.Sprintf("%d", len(decision.GPUIndices)),
		// ★ Fencing token: Pod uses this to validate it's the rightful owner
		// before writing results. Prevents split-brain when lease changes hands.
		"ARES_LEASE_ID":      fmt.Sprintf("%d", decision.LeaseID),
		"ARES_FENCING_TOKEN": decision.FencingToken,
	}

	// ★ Checkpointing: inject restore path if job has a previous checkpoint
	if decision.CheckpointEnabled {
		envVars["ARES_CHECKPOINT_ENABLED"] = "true"
		envVars["ARES_CHECKPOINT_PATH"] = decision.CheckpointPath
		if decision.CheckpointRestore != "" {
			envVars["ARES_CHECKPOINT_RESTORE"] = decision.CheckpointRestore
			envVars["ARES_CHECKPOINT_META"] = decision.CheckpointMeta
		}
	}

	// GPU environment variables (if GPUs assigned)
	if len(decision.GPUIndices) > 0 {
		envVars["CUDA_VISIBLE_DEVICES"] = join(gpuDeviceStrs, ",")
		envVars["NVIDIA_VISIBLE_DEVICES"] = join(gpuDeviceStrs, ",")
	}

	safeClusterID := sanitizeLabelValue(ex.ClusterID)

	// Labels for tracking
	labels := map[string]string{
		"app":          "ares-job",
		"job-id":       decision.JobID,
		"cluster-id":   safeClusterID,
		"scheduled-by": "ares-executor",
	}

	// Pod spec
	spec := &PodSpec{
		PodName:         podName,
		Namespace:       ex.Config.Namespace,
		Image:           imageName,
		Command:         decision.Command, //  FIXED: Pass from decision
		Args:            decision.Args,    //  FIXED: Pass from decision
		ImagePullPolicy: ex.Config.ImagePullPolicy,
		EnvVars:         envVars,
		MemoryMB:        ex.Config.DefaultMemoryMB,
		CPUMillis:       ex.Config.DefaultCPUMillis,
		GPUCount:        len(decision.GPUIndices),
		GPUIndices:      decision.GPUIndices,
		Timeout:         ex.Config.DefaultTimeout,
		RestartPolicy:   ex.Config.RestartPolicy,
		NodeID:          decision.NodeID,
		Labels:          labels,
	}

	logger.Get().Info("Generated PodSpec: ", spec)

	return spec
}

// ============================================================================
// POD LIFECYCLE MONITORING (NEW)
// ============================================================================

// monitorAndUpdateJob: Monitor Pod status and update Job record in etcd
//
// This is the CRITICAL method that closes the loop:
// 1. Poll Pod status from Kubernetes API every 5 seconds
// 2. When Pod status changes, update Job record in etcd
// 3. When Pod completes (Succeeded/Failed), stop monitoring
//
// This runs in a background goroutine started by ExecuteJob()
func (e *Executor) monitorAndUpdateJob(
	ctx context.Context,
	execCtx *ExecutionContext,
	jobID string,
) error {

	// Panic recovery
	defer func() {
		if r := recover(); r != nil {
			e.Log.Error("🚨 PANIC in monitoring for job %s: %v", jobID, r)
			debug.PrintStack() // Print full stack trace
		}
	}()

	e.Log.Info("👁️ Starting Pod monitoring for job %s (pod=%s)", jobID, execCtx.PodName)

	// ✅ Check context
	e.Log.Info("Context type: %T", ctx)
	e.Log.Info("Context done channel: %v", ctx.Done())

	// Create ticker
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	e.Log.Info("✅ Ticker channel: %p", ticker.C)

	// Track status
	lastKnownStatus := common.StatusScheduled
	tickCount := 0

	e.Log.Info("🔄 Entering monitoring loop...")

	for {
		tickCount++
		e.Log.Info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		e.Log.Info("Loop iteration #%d for job %s", tickCount, jobID)
		e.Log.Info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

		select {
		case <-ctx.Done():
			e.Log.Info("❌ Context cancelled for job %s (reason: %v)", jobID, ctx.Err())
			return ctx.Err()

		case tickTime := <-ticker.C:
			e.Log.Info("⏰ TICK #%d received at %s for job %s", tickCount, tickTime.Format("15:04:05"), jobID)

			// ================================================================
			// STEP 1: Get Pod status from Kubernetes
			// ================================================================
			e.Log.Info("📡 Fetching Pod status for %s...", execCtx.PodName)
			podInfo, err := e.K8sClient.GetPod(ctx, execCtx.PodName)

			if err != nil {
				e.Log.Error("❌ Failed to get Pod %s: %v (will retry next tick)", execCtx.PodName, err)
				e.Log.Info("🔄 Continuing to next tick...")
				continue
			}

			podStatus := podInfo.Phase
			e.Log.Info("📊 Pod %s status: %s (last known: %s)", execCtx.PodName, podStatus, lastKnownStatus)

			// ================================================================
			// STEP 2: Get Job record from etcd
			// ================================================================
			e.Log.Info("📥 Fetching Job record from etcd for %s...", jobID)
			jobRecord, err := e.JobStore.GetJob(ctx, jobID)
			if err != nil {
				e.Log.Error("❌ Failed to get Job record: %v (will retry next tick)", err)
				e.Log.Info("🔄 Continuing to next tick...")
				continue
			}

			e.Log.Info("✅ Job record fetched, current status in etcd: %s", jobRecord.Status)

			// ================================================================
			// STEP 3: Map Pod status to Job status
			// ================================================================
			var newJobStatus common.JobStatus
			shouldUpdate := false
			shouldStop := false

			e.Log.Info("🔍 Checking Pod phase: %s", podStatus)

			switch podStatus {
			case PhasePending:
				newJobStatus = common.StatusPending
				if lastKnownStatus != common.StatusPending {
					shouldUpdate = true
					e.Log.Info("→ Status change detected: %s → PENDING", lastKnownStatus)
				}

			case PhaseRunning:
				newJobStatus = common.StatusRunning
				if lastKnownStatus != common.StatusRunning {
					shouldUpdate = true
					jobRecord.StartTime = time.Now()
					if e.OnJobRunning != nil {
						e.OnJobRunning(execCtx.JobID)
					}
					e.Log.Info("→ Status change detected: %s → RUNNING", lastKnownStatus)
				} else {
					e.Log.Info("→ Still RUNNING (no update needed)")
				}

			case PhaseSucceeded:
				newJobStatus = common.StatusSucceeded
				shouldUpdate = false
				shouldStop = true
				jobRecord.Status = newJobStatus
				jobRecord.EndTime = time.Now()
				jobRecord.ExitCode = 0
				e.Log.Info("→ Status change detected: %s → SUCCEEDED (final)", lastKnownStatus)
				// This job completed Successfully. So it needs to be persisted to the Etcd without leaseID.
				// As you know, the design goal is if the executor crashes in the middle of job execution,
				// the lease associated with the job expires, and thus auto deletes the job from etcd.
				// Thus avoiding, split-brain situation. Here, the job completed successfully, meaning that
				// executor give up the lease and job (auto deletes from etcd) and /getJobByJobID returns
				// no such job exists! To mitigate this, I did save the job in the Etcd store without leaseID.
				// Save without lease
				saveErr := e.JobStore.SaveJobFinal(ctx, jobRecord)
				if saveErr != nil {
					e.Log.Error("❌ Failed to persist SUCCEEDED job: %v", saveErr)
					continue
				}
				e.Log.Info("✅ Succeeded Job Persisted Permanently (no lease)")

				// Clean up: move from ActiveJobs -> CompletedJobs
				e.JobsMu.Lock()
				delete(e.ActiveJobs, execCtx.JobID)

				e.CompletedJobs[execCtx.JobID] = &ExecutionResult{
					JobID:       execCtx.JobID,
					PodName:     execCtx.PodName,
					Status:      StatusSuccessful,
					Phase:       PhaseSucceeded,
					StartTime:   execCtx.StartTime,
					EndTime:     time.Now(),
					Duration:    time.Since(execCtx.StartTime),
					CompletedAt: time.Now(),
				}
				e.JobsMu.Unlock()
				atomic.AddUint64(&e.TotalSuccessful, 1)

				// ★ FIX: Release GPU resources
				if e.OnJobComplete != nil {
					e.OnJobComplete(execCtx.JobID, execCtx.NodeID, len(execCtx.GPUIndices), 0)
				}

			case PhaseFailed:
				newJobStatus = common.StatusFailed
				shouldUpdate = false
				shouldStop = true
				jobRecord.Status = newJobStatus
				jobRecord.EndTime = time.Now()
				jobRecord.ExitCode = 1

				podLogs, logErr := e.K8sClient.GetPodLogs(ctx, execCtx.PodName)
				if logErr == nil && podLogs != "" {
					if len(podLogs) > 500 {
						podLogs = podLogs[:500] + "..."
					}
					jobRecord.ErrorMsg = fmt.Sprintf("Pod failed: %s", podLogs)
				} else {
					jobRecord.ErrorMsg = "Pod failed (logs unavailable)"
				}
				e.Log.Error("→ Status change detected: %s → FAILED (final)", lastKnownStatus)

				// Save without lease
				saveErr := e.JobStore.SaveJobFinal(ctx, jobRecord)
				if saveErr != nil {
					e.Log.Error("❌ Failed to persist FAILED job: %v", saveErr)
					continue
				}
				e.Log.Info("✅ Failed Job Persisted Permanently (no lease)")

				// Clean up: move from ActiveJobs → CompletedJobs
				e.JobsMu.Lock()
				delete(e.ActiveJobs, execCtx.JobID)
				e.CompletedJobs[execCtx.JobID] = &ExecutionResult{
					JobID:        execCtx.JobID,
					PodName:      execCtx.PodName,
					Status:       StatusFailed,
					Phase:        PhaseFailed,
					StartTime:    execCtx.StartTime,
					EndTime:      time.Now(),
					Duration:     time.Since(execCtx.StartTime),
					ErrorMessage: jobRecord.ErrorMsg,
					CompletedAt:  time.Now(),
				}
				e.JobsMu.Unlock()
				atomic.AddUint64(&e.TotalFailed, 1)

				// ★ FIX: Release GPU resources on failure too
				if e.OnJobComplete != nil {
					e.OnJobComplete(execCtx.JobID, execCtx.NodeID, len(execCtx.GPUIndices), 0)
				}

			case PhaseUnknown:
				e.Log.Warn("→ Pod status UNKNOWN, skipping update")
				e.Log.Info("🔄 Continuing to next tick...")
				continue

			default:
				e.Log.Warn("→ Unexpected pod status: %s, skipping update", podStatus)
				e.Log.Info("🔄 Continuing to next tick...")
				continue
			}

			// ================================================================
			// STEP 4: Update Job record if status changed
			// ================================================================
			if shouldUpdate {
				e.Log.Info("💾 UPDATE REQUIRED: %s → %s", lastKnownStatus, newJobStatus)

				jobRecord.Status = newJobStatus

				// Extract lease ID
				leaseID := int64(0)
				if jobRecord.ExecutionLease != nil && jobRecord.ExecutionLease.LeaseID != "" {
					parsed, parseErr := strconv.ParseInt(jobRecord.ExecutionLease.LeaseID, 10, 64)
					if parseErr != nil {
						e.Log.Error("❌ Lease ID parse failed: %v", parseErr)
						e.Log.Error("   Lease ID string: '%s'", jobRecord.ExecutionLease.LeaseID)
						e.Log.Info("🔄 Continuing to next tick...")
						continue
					}
					leaseID = parsed
					e.Log.Info("✅ Lease ID: %d", leaseID)
				} else {
					e.Log.Error("❌ No ExecutionLease found!")
					e.Log.Info("🔄 Continuing to next tick...")
					continue
				}

				if leaseID == 0 {
					e.Log.Error("❌ Lease ID is 0, cannot save!")
					e.Log.Info("🔄 Continuing to next tick...")
					continue
				}

				e.Log.Info("📤 Calling SaveJob(jobID=%s, leaseID=%d, status=%s)...", jobID, leaseID, newJobStatus)

				saveErr := e.JobStore.SaveJob(ctx, jobRecord, leaseID)

				if saveErr != nil {
					e.Log.Error("❌❌❌ SaveJob FAILED! ❌❌❌")
					e.Log.Error("   Error: %v", saveErr)
					e.Log.Error("   Error type: %T", saveErr)
					e.Log.Error("   JobID: %s", jobID)
					e.Log.Error("   LeaseID: %d", leaseID)
					e.Log.Error("   Status: %s", newJobStatus)
					e.Log.Info("🔄 Continuing to next tick...")
					continue
				}

				lastKnownStatus = newJobStatus
				e.Log.Info("✅✅✅ SaveJob SUCCESS! Job now: %s ✅✅✅", newJobStatus)
			} else {
				e.Log.Info("ℹ️ No update needed (status unchanged)")
			}

			// ================================================================
			// STEP 5: Stop monitoring if Pod completed
			// ================================================================
			if shouldStop {
				e.Log.Info("🏁 Pod completed, stopping monitoring (final status: %s)", newJobStatus)
				return nil
			}

			e.Log.Info("✅ Tick #%d complete, waiting for next tick...", tickCount)
		}
	}
}

// monitorJobExecution: Background job monitoring
func (ex *Executor) monitorJobExecution(ctx context.Context, execCtx *ExecutionContext) {

	ticker := time.NewTicker(ex.Config.HealthCheckInterval)
	defer ticker.Stop()

	timeoutTimer := time.NewTimer(execCtx.Timeout)
	defer timeoutTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			ex.handleJobCancellation(execCtx)
			return

		case <-timeoutTimer.C:
			ex.handleJobTimeout(execCtx)
			return

		case <-ticker.C:
			// Check Pod status
			podInfo, err := ex.K8sClient.GetPod(ctx, execCtx.PodName)
			if err != nil {
				ex.Log.Warn("Failed to get Pod status for job %s: %v", execCtx.JobID, err)
				continue
			}

			// Update execution context
			execCtx.CurrentPhase = podInfo.Phase
			execCtx.LastUpdated = time.Now()

			// Map Pod phase to job status
			switch podInfo.Phase {
			case PhaseRunning:
				execCtx.Status = StatusRunning
				ex.Log.Debug("Job %s running on Pod %s", execCtx.JobID, execCtx.PodName)

			case PhaseSucceeded:
				result := completeJob(ex, execCtx, StatusSuccessful)
				ex.Log.Info("Job %s completed successfully (duration=%.2fs)",
					execCtx.JobID, result.Duration.Seconds())
				return

			case PhaseFailed:
				result := completeJob(ex, execCtx, StatusFailed)
				ex.Log.Warn("Job %s failed (duration=%.2fs, error=%s)",
					execCtx.JobID, result.Duration.Seconds(), result.ErrorMessage)
				return

			case PhaseUnknown:
				ex.Log.Warn("Job %s in unknown phase", execCtx.JobID)
			}

			// Collect metrics periodically
			if ex.Config.MetricsCollectionEnabled {
				metrics, err := ex.K8sClient.GetPodMetrics(ctx, execCtx.PodName)
				if err == nil {
					execCtx.Metrics = metrics
				}
			}

			// Collect logs if job is completing
			if podInfo.Phase == PhaseSucceeded || podInfo.Phase == PhaseFailed {
				if ex.Config.LogCollectionEnabled {
					logs, err := ex.K8sClient.GetPodLogs(ctx, execCtx.PodName)
					if err == nil {
						execCtx.Logs = logs
					}
				}
			}
		}
	}
}

// handleJobTimeout: Handle job timeout
func (ex *Executor) handleJobTimeout(execCtx *ExecutionContext) {
	ex.Log.Warn("Job %s timeout after %.2fs", execCtx.JobID, execCtx.Timeout.Seconds())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := ex.K8sClient.DeletePod(ctx, execCtx.PodName)
	if err != nil {
		ex.Log.Error("Failed to delete timed-out Pod %s: %v", execCtx.PodName, err)
	}

	result := &ExecutionResult{
		JobID:        execCtx.JobID,
		PodName:      execCtx.PodName,
		Status:       StatusFailed,
		Phase:        execCtx.CurrentPhase,
		StartTime:    execCtx.StartTime,
		EndTime:      time.Now(),
		Duration:     time.Since(execCtx.StartTime),
		ErrorMessage: fmt.Sprintf("Job timeout after %.2f seconds", execCtx.Timeout.Seconds()),
		CompletedAt:  time.Now(),
	}

	ex.JobsMu.Lock()
	delete(ex.ActiveJobs, execCtx.JobID)
	ex.CompletedJobs[execCtx.JobID] = result
	ex.JobsMu.Unlock()

	atomic.AddUint64(&ex.TotalFailed, 1)
}

// handleJobCancellation: Handle job cancellation
func (ex *Executor) handleJobCancellation(execCtx *ExecutionContext) {
	ex.Log.Info("Cancelling job %s", execCtx.JobID)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := ex.K8sClient.DeletePod(ctx, execCtx.PodName)
	if err != nil {
		ex.Log.Error("Failed to delete cancelled Pod %s: %v", execCtx.PodName, err)
	}

	result := &ExecutionResult{
		JobID:       execCtx.JobID,
		PodName:     execCtx.PodName,
		Status:      StatusCancelled,
		Phase:       execCtx.CurrentPhase,
		StartTime:   execCtx.StartTime,
		EndTime:     time.Now(),
		Duration:    time.Since(execCtx.StartTime),
		CompletedAt: time.Now(),
	}

	ex.JobsMu.Lock()
	delete(ex.ActiveJobs, execCtx.JobID)
	ex.CompletedJobs[execCtx.JobID] = result
	ex.JobsMu.Unlock()

	atomic.AddUint64(&ex.TotalCancelled, 1)
}

// completeJob: Mark job as complete
func completeJob(ex *Executor, execCtx *ExecutionContext,
	status JobStatus) *ExecutionResult {
	endTime := time.Now()
	duration := endTime.Sub(execCtx.StartTime)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if ex.Config.LogCollectionEnabled {
		logs, err := ex.K8sClient.GetPodLogs(ctx, execCtx.PodName)
		if err == nil {
			execCtx.Logs = logs
		}
	}

	result := &ExecutionResult{
		JobID:       execCtx.JobID,
		PodName:     execCtx.PodName,
		Status:      status,
		Phase:       execCtx.CurrentPhase,
		StartTime:   execCtx.StartTime,
		EndTime:     endTime,
		Duration:    duration,
		Logs:        execCtx.Logs,
		Metrics:     execCtx.Metrics,
		CompletedAt: time.Now(),
	}

	ex.JobsMu.Lock()
	delete(ex.ActiveJobs, execCtx.JobID)
	ex.CompletedJobs[execCtx.JobID] = result
	ex.JobsMu.Unlock()

	atomic.AddInt64(&ex.TotalDuration, duration.Nanoseconds())
	if status == StatusSuccessful {
		atomic.AddUint64(&ex.TotalSuccessful, 1)
	} else if status == StatusFailed {
		atomic.AddUint64(&ex.TotalFailed, 1)
	}

	return result
}

// ============================================================================
// JOB QUERIES
// ============================================================================

// GetJobStatus: Get current job status
func (ex *Executor) GetJobStatus(jobID string) (*ExecutionContext, error) {
	if jobID == "" {
		return nil, fmt.Errorf("job ID cannot be empty")
	}

	ex.JobsMu.RLock()
	execCtx, exists := ex.ActiveJobs[jobID]
	ex.JobsMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("job %s not found in active jobs", jobID)
	}

	return execCtx, nil
}

// GetJobResult: Get completed job result
func (ex *Executor) GetJobResult(jobID string) (*ExecutionResult, error) {
	if jobID == "" {
		return nil, fmt.Errorf("job ID cannot be empty")
	}

	ex.JobsMu.RLock()
	result, exists := ex.CompletedJobs[jobID]
	ex.JobsMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("job %s not found in completed jobs", jobID)
	}

	return result, nil
}

// ListActiveJobs: List all active jobs
func (ex *Executor) ListActiveJobs() []*ExecutionContext {
	ex.JobsMu.RLock()
	defer ex.JobsMu.RUnlock()

	jobs := make([]*ExecutionContext, 0, len(ex.ActiveJobs))
	for _, job := range ex.ActiveJobs {
		jobs = append(jobs, job)
	}

	return jobs
}

// ListCompletedJobs: List all completed jobs
func (ex *Executor) ListCompletedJobs() []*ExecutionResult {
	ex.JobsMu.RLock()
	defer ex.JobsMu.RUnlock()

	results := make([]*ExecutionResult, 0, len(ex.CompletedJobs))
	for _, result := range ex.CompletedJobs {
		results = append(results, result)
	}

	return results
}

// CancelJob: Cancel a running job
func (ex *Executor) CancelJob(jobID string) error {
	if jobID == "" {
		return fmt.Errorf("job ID cannot be empty")
	}

	ex.JobsMu.RLock()
	execCtx, exists := ex.ActiveJobs[jobID]
	ex.JobsMu.RUnlock()

	if !exists {
		return fmt.Errorf("job %s not found", jobID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := ex.K8sClient.DeletePod(ctx, execCtx.PodName)
	if err != nil {
		ex.Log.Error("Failed to cancel job %s: %v", jobID, err)
		return fmt.Errorf("failed to cancel job: %w", err)
	}

	ex.Log.Info("Job %s cancelled", jobID)
	return nil
}

// ============================================================================
// STATISTICS & MONITORING
// ============================================================================

// GetStats: Get executor statistics
func (ex *Executor) GetStats() map[string]interface{} {
	ex.JobsMu.RLock()
	activeCount := len(ex.ActiveJobs)
	completedCount := len(ex.CompletedJobs)
	ex.JobsMu.RUnlock()

	totalJobs := atomic.LoadUint64(&ex.TotalJobs)
	totalSuccessful := atomic.LoadUint64(&ex.TotalSuccessful)
	totalFailed := atomic.LoadUint64(&ex.TotalFailed)
	totalCancelled := atomic.LoadUint64(&ex.TotalCancelled)

	successRate := 0.0
	if totalJobs > 0 {
		successRate = float64(totalSuccessful) / float64(totalJobs) * 100.0
	}

	avgDuration := 0.0
	if totalJobs > 0 {
		totalDuration := atomic.LoadInt64(&ex.TotalDuration)
		avgDuration = float64(totalDuration) / float64(totalJobs) / 1e9
	}

	return map[string]interface{}{
		"cluster_id":       ex.ClusterID,
		"active_jobs":      activeCount,
		"completed_jobs":   completedCount,
		"total_jobs":       totalJobs,
		"total_successful": totalSuccessful,
		"total_failed":     totalFailed,
		"total_cancelled":  totalCancelled,
		"success_rate":     successRate,
		"avg_duration_sec": avgDuration,
		"max_concurrent":   ex.Config.MaxConcurrentJobs,
		"namespace":        ex.Config.Namespace,
	}
}

// GetExecutorHealth: Get executor health status
func (ex *Executor) GetExecutorHealth() map[string]interface{} {
	ex.JobsMu.RLock()
	activeCount := len(ex.ActiveJobs)
	ex.JobsMu.RUnlock()

	isHealthy := activeCount < ex.Config.MaxConcurrentJobs
	utilization := float64(activeCount) / float64(ex.Config.MaxConcurrentJobs) * 100.0

	return map[string]interface{}{
		"cluster_id":      ex.ClusterID,
		"healthy":         isHealthy,
		"active_jobs":     activeCount,
		"max_concurrent":  ex.Config.MaxConcurrentJobs,
		"utilization_pct": utilization,
		"timestamp":       time.Now().Format(time.RFC3339),
	}
}

// Cleanup: Clean up old completed jobs
func (ex *Executor) Cleanup(olderThan time.Duration) error {
	cutoffTime := time.Now().Add(-olderThan)

	ex.JobsMu.Lock()
	defer ex.JobsMu.Unlock()

	deletedCount := 0
	for jobID, result := range ex.CompletedJobs {
		if result.CompletedAt.Before(cutoffTime) {
			delete(ex.CompletedJobs, jobID)
			deletedCount++
		}
	}

	ex.Log.Info("Cleanup: removed %d old completed jobs", deletedCount)
	return nil
}

// ============================================================================
// UTILITIES
// ============================================================================

// generatePodName: Generate valid Kubernetes Pod name
func generatePodName(jobID string) string {
	podName := ""
	for _, ch := range jobID {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '-' {
			podName += string(ch)
		} else if ch >= 'A' && ch <= 'Z' {
			podName += string(ch + 32)
		} else {
			podName += "-"
		}
	}

	for len(podName) > 0 && (podName[0] < 'a' || podName[0] > 'z') && (podName[0] < '0' || podName[0] > '9') {
		podName = podName[1:]
	}

	for len(podName) > 0 && (podName[len(podName)-1] < 'a' || podName[len(podName)-1] > 'z') && (podName[len(podName)-1] < '0' || podName[len(podName)-1] > '9') {
		podName = podName[:len(podName)-1]
	}

	if len(podName) > 63 {
		podName = podName[:57]
		timestamp := fmt.Sprintf("%d", time.Now().UnixNano()%1000000)
		podName = fmt.Sprintf("%s-%s", podName, timestamp)
		if len(podName) > 63 {
			podName = podName[:63]
		}
	}

	if podName == "" {
		podName = fmt.Sprintf("job-%d", time.Now().UnixNano())
	}

	logger.Get().Info("Generated Pod Name: %s", podName)

	return podName
}

// contains: Helper to check if string is in array
func contains(arr interface{}, val string) bool {
	switch v := arr.(type) {
	case []string:
		for _, item := range v {
			if item == val {
				return true
			}
		}
	}
	return false
}

// join: Helper to join strings with separator
func join(arr []string, sep string) string {
	if len(arr) == 0 {
		return ""
	}
	result := arr[0]
	for _, item := range arr[1:] {
		result += sep + item
	}
	return result
}

// ============================================================================
// MOCK KUBERNETES CLIENT
// ============================================================================

// MockK8sClient: Mock Kubernetes client for development
type MockK8sClient struct {
	mu         sync.RWMutex
	pods       map[string]*PodInfo
	podLogs    map[string]string
	podMetrics map[string]map[string]interface{}
}

// NewMockK8sClient: Create mock client
func NewMockK8sClient() *MockK8sClient {
	return &MockK8sClient{
		pods:       make(map[string]*PodInfo),
		podLogs:    make(map[string]string),
		podMetrics: make(map[string]map[string]interface{}),
	}
}

// CreatePod: Mock Pod creation
func (m *MockK8sClient) CreatePod(ctx context.Context, pod *PodSpec) (string, error) {
	if pod == nil || pod.PodName == "" {
		return "", fmt.Errorf("invalid pod spec")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	podInfo := &PodInfo{
		PodName:    pod.PodName,
		Namespace:  pod.Namespace,
		JobID:      pod.Labels["job-id"],
		NodeID:     pod.NodeID,
		Phase:      PhasePending,
		CreatedAt:  time.Now(),
		GPUIndices: pod.GPUIndices,
		Ready:      false,
	}

	m.pods[pod.PodName] = podInfo

	go func() {
		time.Sleep(100 * time.Millisecond)
		m.mu.Lock()
		if p, ok := m.pods[pod.PodName]; ok {
			p.Phase = PhaseRunning
			p.StartedAt = time.Now()
		}
		m.mu.Unlock()

		time.Sleep(1 * time.Second)
		m.mu.Lock()
		if p, ok := m.pods[pod.PodName]; ok {
			p.Phase = PhaseSucceeded
			p.FinishedAt = time.Now()
		}
		m.mu.Unlock()
	}()

	return pod.PodName, nil
}

// GetPod: Mock get Pod
func (m *MockK8sClient) GetPod(ctx context.Context, podName string) (*PodInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	pod, exists := m.pods[podName]
	if !exists {
		return nil, fmt.Errorf("pod not found")
	}

	return pod, nil
}

// DeletePod: Mock delete Pod
func (m *MockK8sClient) DeletePod(ctx context.Context, podName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.pods[podName]; !exists {
		return fmt.Errorf("pod not found")
	}

	delete(m.pods, podName)
	return nil
}

// ListPods: Mock list Pods
func (m *MockK8sClient) ListPods(ctx context.Context) ([]*PodInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	pods := make([]*PodInfo, 0, len(m.pods))
	for _, pod := range m.pods {
		pods = append(pods, pod)
	}

	return pods, nil
}

// GetPodLogs: Mock get Pod logs
func (m *MockK8sClient) GetPodLogs(ctx context.Context, podName string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	logs, exists := m.podLogs[podName]
	if !exists {
		return "No logs available", nil
	}

	return logs, nil
}

// GetPodMetrics: Mock get Pod metrics
func (m *MockK8sClient) GetPodMetrics(ctx context.Context, podName string) (map[string]interface{}, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	metrics, exists := m.podMetrics[podName]
	if !exists {
		return map[string]interface{}{
			"cpu_usage_millicores": 250,
			"memory_usage_mb":      512,
		}, nil
	}

	return metrics, nil
}

// WatchPod: Mock watch Pod
func (m *MockK8sClient) WatchPod(ctx context.Context, podName string, callback func(*PodInfo)) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			m.mu.RLock()
			pod, exists := m.pods[podName]
			m.mu.RUnlock()

			if !exists {
				return fmt.Errorf("pod not found")
			}

			callback(pod)
		}
	}
}

// SetPodLogs: Helper to set Pod logs (for testing)
func (m *MockK8sClient) SetPodLogs(podName string, logs string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.podLogs[podName] = logs
}

// SetPodMetrics: Helper to set Pod metrics (for testing)
func (m *MockK8sClient) SetPodMetrics(podName string, metrics map[string]interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.podMetrics[podName] = metrics
}

// sanitizeLabelValue: Replace invalid Kubernetes label characters with '-'
// K8s labels allow: [a-zA-Z0-9._-], max 63 chars, must start/end alphanumeric
func sanitizeLabelValue(s string) string {
	result := make([]byte, 0, len(s))
	for i, c := range s {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' {
			result = append(result, byte(c))
		} else {
			result = append(result, '-')
		}
		if i >= 62 {
			break // Max 63 chars
		}
	}
	// Trim leading/trailing non-alphanumeric
	s2 := string(result)
	s2 = strings.TrimLeft(s2, "-_.")
	s2 = strings.TrimRight(s2, "-_.")
	if s2 == "" {
		return "unknown"
	}
	return s2
}
