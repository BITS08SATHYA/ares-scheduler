package job

import (
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"math"
	"time"
)

// Layer 3: Job Model - Job creation and state machine
// Handles: Job creation, status transitions, backoff calculations

// JobFactory: Factory for creating new jobs
type JobFactory struct {
	log *logger.Logger
}

// NewJobFactory: Create a new job factory
func NewJobFactory() *JobFactory {
	return &JobFactory{
		log: logger.Get(),
	}
}

// ============================================================================
// JOB CREATION
// ============================================================================

// CreateJob: Create a new job from a JobSpec
// This is called when a user submits a job
//
// Validates the spec and creates a Job with:
// - Status: PENDING (waiting to be scheduled)
// - No cluster assigned yet
// - No GPU allocation yet
// - Attempt: 0 (first attempt)
func (jf *JobFactory) CreateJob(spec *common.JobSpec) (*common.Job, error) {

	// Validate the spec
	if err := ValidateJobSpec(spec); err != nil {
		jf.log.Error("Invalid job spec: %v", err)
		return nil, fmt.Errorf("invalid job spec: %w", err)
	}

	// Generate job ID (in production use UUID)
	jobID := generateJobID()

	job := &common.Job{
		ID:         jobID,
		Spec:       spec,
		Status:     common.StatusPending,
		Attempts:   0,
		Metrics:    make(map[string]interface{}),
		SubmitTime: time.Now(),
	}

	jf.log.Info("✓ Created job %s (spec: %s, GPU: %d)", jobID, spec.Name, spec.GPUCount)

	return job, nil
}

// ============================================================================
// JOB STATE TRANSITION HELPERS
// ============================================================================

// TransitionToPending: Move job to PENDING state
// Returns error if job is not in a valid state for this transition
func TransitionToPending(job *common.Job) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}

	if job.Status == common.StatusPending {
		return nil // Already pending
	}

	job.Status = common.StatusPending
	logger.Get().Debug("Job %s transitioned to PENDING", job.ID)
	return nil
}

// TransitionToScheduled: Move job to SCHEDULED state
// Called when global scheduler assigns job to a cluster
func TransitionToScheduled(job *common.Job, clusterID string, nodeID string) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}

	if job.Status != common.StatusPending {
		return fmt.Errorf("can only schedule from PENDING state, current: %s", job.Status)
	}

	job.Status = common.StatusScheduled
	job.ClusterID = clusterID
	job.NodeID = nodeID
	job.ScheduleTime = time.Now()

	logger.Get().Debug("Job %s scheduled to cluster %s, node %s",
		job.ID, clusterID, nodeID)

	return nil
}

// TransitionToRunning: Move job to RUNNING state
// Called when local scheduler creates pod and it starts
func TransitionToRunning(
	job *common.Job,
	podName string,
	allocatedGPUs []int,
	leaseID int64,
	token string,
) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}

	if job.Status != common.StatusScheduled {
		return fmt.Errorf("can only run from SCHEDULED state, current: %s", job.Status)
	}

	job.Status = common.StatusRunning
	job.PodName = podName
	job.AllocatedGPUIndices = allocatedGPUs
	job.ExecutionToken = token
	job.ExecutionLease = &common.LeaseInfo{
		LeaseID: fmt.Sprintf("%d", leaseID),
	}
	job.StartTime = time.Now()

	logger.Get().Info("Job %s is now RUNNING (pod: %s, GPUs: %v)",
		job.ID, podName, allocatedGPUs)

	return nil
}

// TransitionToSucceeded: Move job to SUCCEEDED state
// Called when pod exits with code 0
func TransitionToSucceeded(job *common.Job, exitCode int) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}

	if !IsJobRunning(job) {
		return fmt.Errorf("can only succeed from RUNNING state, current: %s", job.Status)
	}

	job.Status = common.StatusSucceeded
	job.ExitCode = exitCode
	job.EndTime = time.Now()

	duration := GetJobDuration(job)
	totalLatency := GetJobTotalLatency(job)

	logger.Get().Info("✓ Job %s SUCCEEDED (duration: %v, total latency: %v)",
		job.ID, duration, totalLatency)

	return nil
}

// TransitionToFailed: Move job to FAILED state
// Can retry later if Attempts < MaxRetries
//
// Sets NextRetryAt based on exponential backoff
// Returns nil and transitions to PENDING if retry will happen
// Returns error with status=FAILED if no more retries
func TransitionToFailed(job *common.Job, errorMsg string) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}

	if !IsJobRunning(job) {
		return fmt.Errorf("can only fail from RUNNING state, current: %s", job.Status)
	}

	job.Status = common.StatusFailed
	job.ErrorMsg = errorMsg
	job.EndTime = time.Now()
	job.Attempts++

	duration := GetJobDuration(job)

	// Calculate backoff for next retry
	if job.Attempts < job.Spec.MaxRetries {
		nextRetryAt := CalculateNextRetryTime(job.Attempts)
		job.NextRetryAt = time.Now().Add(nextRetryAt)

		logger.Get().Warn("Job %s FAILED (attempt %d/%d, will retry in %v): %s",
			job.ID, job.Attempts, job.Spec.MaxRetries, nextRetryAt, errorMsg)

		// Transition back to PENDING for retry
		job.Status = common.StatusPending
		job.ScheduleTime = time.Time{} // Reset for re-scheduling
		job.StartTime = time.Time{}    // Reset for re-execution

		return nil
	}

	// No more retries
	logger.Get().Error("✗ Job %s FAILED permanently (attempt %d/%d, duration: %v): %s",
		job.ID, job.Attempts, job.Spec.MaxRetries, duration, errorMsg)

	return nil
}

// ============================================================================
// BACKOFF CALCULATION (Feature 21 - Backoff & Retry)
// ============================================================================

// CalculateNextRetryTime: Calculate exponential backoff with jitter
//
// Formula: delay = min(baseDelay * 2^attempt + random jitter, maxDelay)
// Examples:
//
//	Attempt 1: ~1 second
//	Attempt 2: ~2 seconds
//	Attempt 3: ~4 seconds
//	Attempt 4: ~8 seconds
//	Attempt 5+: capped at maxDelay
func CalculateNextRetryTime(attempt int) time.Duration {
	const (
		baseDelay = 1 * time.Second
		maxDelay  = 5 * time.Minute
	)

	// Exponential backoff: 2^attempt seconds
	exponentialDelay := time.Duration(math.Pow(2, float64(attempt))) * time.Second

	// Cap at max delay
	if exponentialDelay > maxDelay {
		exponentialDelay = maxDelay
	}

	// Add random jitter (±10%)
	jitterPercent := 0.1
	jitter := time.Duration(float64(exponentialDelay) * jitterPercent)

	return exponentialDelay + jitter
}

// ============================================================================
// JOB VALIDATION
// ============================================================================

// ValidateJobSpec: Validate a JobSpec before creating a job
func ValidateJobSpec(spec *common.JobSpec) error {
	if spec == nil {
		return fmt.Errorf("spec is nil")
	}

	if spec.Name == "" {
		return fmt.Errorf("job name cannot be empty")
	}

	if spec.Image == "" {
		return fmt.Errorf("job image cannot be empty")
	}

	if spec.GPUCount < 0 {
		return fmt.Errorf("GPU count cannot be negative")
	}

	if spec.Priority < 0 || spec.Priority > 100 {
		return fmt.Errorf("priority must be between 0 and 100")
	}

	if spec.TimeoutSecs <= 0 {
		return fmt.Errorf("timeout must be positive")
	}

	if spec.MaxRetries < 0 {
		return fmt.Errorf("max retries cannot be negative")
	}

	if spec.TargetLatencyMs <= 0 {
		return fmt.Errorf("target latency must be positive")
	}

	return nil
}

// ============================================================================
// JOB QUERY HELPERS (NON-MUTATING)
// ============================================================================

// IsJobRunning: Check if job is in RUNNING state
func IsJobRunning(job *common.Job) bool {
	if job == nil {
		return false
	}
	return job.Status == common.StatusRunning
}

// IsJobCompleted: Check if job is in a completed state
func IsJobCompleted(job *common.Job) bool {
	if job == nil {
		return false
	}
	return job.Status == common.StatusSucceeded || job.Status == common.StatusFailed
}

// IsJobPending: Check if job is in PENDING state
func IsJobPending(job *common.Job) bool {
	if job == nil {
		return false
	}
	return job.Status == common.StatusPending
}

// GetJobDuration: Get how long job took from start to end
func GetJobDuration(job *common.Job) time.Duration {
	if job == nil || job.StartTime.IsZero() || job.EndTime.IsZero() {
		return 0
	}
	return job.EndTime.Sub(job.StartTime)
}

// GetJobTotalLatency: Get total latency from submit to end
func GetJobTotalLatency(job *common.Job) time.Duration {
	if job == nil || job.SubmitTime.IsZero() || job.EndTime.IsZero() {
		return 0
	}
	return job.EndTime.Sub(job.SubmitTime)
}

// GetJobElapsedTime: Get how long job has been running (if running)
func GetJobElapsedTime(job *common.Job) time.Duration {
	if job == nil || !IsJobRunning(job) || job.StartTime.IsZero() {
		return 0
	}
	return time.Since(job.StartTime)
}

// IsJobWithinTimeout: Check if job is still within timeout
func IsJobWithinTimeout(job *common.Job) bool {
	if job == nil || !IsJobRunning(job) {
		return false
	}

	elapsed := GetJobElapsedTime(job)
	maxDuration := time.Duration(job.Spec.TimeoutSecs) * time.Second

	return elapsed < maxDuration
}

// ShouldJobRetry: Check if job should retry
func ShouldJobRetry(job *common.Job) bool {
	if job == nil {
		return false
	}

	return job.Status == common.StatusFailed &&
		job.Attempts < job.Spec.MaxRetries &&
		time.Now().After(job.NextRetryAt)
}

// CanJobBeScheduled: Check if job can be scheduled
func CanJobBeScheduled(job *common.Job) bool {
	if job == nil {
		return false
	}
	return job.Status == common.StatusPending
}

// CanJobBeExecuted: Check if job can be executed
func CanJobBeExecuted(job *common.Job) bool {
	if job == nil {
		return false
	}
	return job.Status == common.StatusScheduled
}

// CanJobBeCancelled: Check if job can be cancelled
func CanJobBeCancelled(job *common.Job) bool {
	if job == nil {
		return false
	}
	return job.Status == common.StatusPending || job.Status == common.StatusScheduled
}

// ============================================================================
// JOB METRICS HELPERS
// ============================================================================

// UpdateJobMetric: Add or update a metric on a job
// Call this during execution to track performance
func UpdateJobMetric(job *common.Job, key string, value interface{}) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}

	if job.Metrics == nil {
		job.Metrics = make(map[string]interface{})
	}

	job.Metrics[key] = value
	return nil
}

// GetJobMetric: Get a metric value from a job
func GetJobMetric(job *common.Job, key string) (interface{}, bool) {
	if job == nil {
		return nil, false
	}

	if job.Metrics == nil {
		return nil, false
	}

	value, exists := job.Metrics[key]
	return value, exists
}

// SetJobMetrics: Replace all metrics on a job
func SetJobMetrics(job *common.Job, metrics map[string]interface{}) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}

	job.Metrics = metrics
	return nil
}

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

// generateJobID: Generate a unique job ID
// In production use UUID: uuid.NewString()
func generateJobID() string {
	// Simple implementation for now
	return fmt.Sprintf("job-%d", time.Now().UnixNano())
}

// GetJobStatusString: Get human-readable status
func GetJobStatusString(job *common.Job) string {
	if job == nil {
		return "UNKNOWN"
	}
	return string(job.Status)
}

// CompareJobStatus: Compare two job statuses
func CompareJobStatus(job *common.Job, status common.JobStatus) bool {
	if job == nil {
		return false
	}
	return job.Status == status
}
