package orchestrator

import (
	"context"
	"fmt"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/executor"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/idempotency"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/job"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/lease"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/global"
)

// ============================================================================
// JOB COORDINATOR (Layer 10)
// ============================================================================
// Orchestrates: Dedup → Lease → Persist → Schedule → Execute
// This is the "glue" that ties all layers together

type JobCoordinator struct {
	idempotencyMgr  *idempotency.IdempotencyManager
	leaseManager    *lease.LeaseManager
	jobStore        job.JobStore
	globalScheduler *global.GlobalScheduler
	executor        *executor.Executor
	log             *logger.Logger
}

type SchedulingResult struct {
	JobID              string
	ClusterID          string
	NodeID             string
	GPUIndices         []int
	ClusterScore       float64
	PlacementReasons   []string
	PodName            string
	LocalSchedulerAddr string
	CreatedAt          time.Time
}

// NewJobCoordinator creates a new job coordinator
func NewJobCoordinator(
	idempotencyMgr *idempotency.IdempotencyManager,
	leaseManager *lease.LeaseManager,
	jobStore job.JobStore,
	globalScheduler *global.GlobalScheduler,
	executor *executor.Executor,
) *JobCoordinator {
	return &JobCoordinator{
		idempotencyMgr:  idempotencyMgr,
		leaseManager:    leaseManager,
		jobStore:        jobStore,
		globalScheduler: globalScheduler,
		executor:        executor,
		log:             logger.Get(),
	}
}

// ScheduleJob orchestrates the complete job scheduling pipeline
//
// Pipeline:
// 1. Check for duplicate (idempotency)
// 2. Acquire distributed lease (exactly-once)
// 3. Create job record (persistence)
// 4. Call global scheduler (cluster selection)
// 5. [GlobalScheduler calls LocalScheduler via HTTP]
// 6. Monitor job until completion
//
// Returns: Complete scheduling result or error
func (jc *JobCoordinator) ScheduleJob(
	ctx context.Context,
	jobSpec *common.JobSpec,
) (*SchedulingResult, error) {

	if jobSpec == nil || jobSpec.RequestID == "" {
		return nil, fmt.Errorf("invalid job spec: nil or empty request ID")
	}

	// ========================================================================
	// STEP 1: Check for duplicate request (idempotency)
	// ========================================================================

	cachedResult, isDuplicate, err := jc.idempotencyMgr.CheckDuplicate(ctx, jobSpec.RequestID)
	if isDuplicate {
		jc.log.Info("Duplicate request detected: %s, returning cached result", jobSpec.RequestID)
		// Parse cached result and return
		result := &SchedulingResult{
			JobID:     cachedResult.JobID,
			CreatedAt: cachedResult.SubmitTime,
		}
		return result, nil
	}

	if err != nil {
		jc.log.Warn("Idempotency check failed (non-fatal): %v", err)
		// Continue with normal processing
	}

	// ========================================================================
	// STEP 2: Acquire distributed lease (exactly-once execution)
	// ========================================================================

	// Generate unique job ID
	jobID := fmt.Sprintf("job-%d-%s", time.Now().UnixNano(), jobSpec.RequestID)

	// Try to acquire lease (atomic operation in etcd)
	acquired, err := jc.leaseManager.AcquireLeaseForJob(ctx, jobID)
	if !acquired {
		jc.log.Error("Could not acquire lease for job %s (another executor already owns it)", jobID)
		return nil, fmt.Errorf("lease acquisition failed: another executor owns this job")
	}

	if err != nil {
		jc.log.Error("Lease acquisition error: %v", err)
		return nil, fmt.Errorf("lease error: %w", err)
	}

	jc.log.Info("Acquired lease for job %s", jobID)

	// Ensure we release lease on error
	defer func() {
		if err != nil {
			jc.leaseManager.ReleaseLeaseForJob(context.Background(), jobID)
		}
	}()

	// ========================================================================
	// STEP 3: Create job record (persistence)
	// ========================================================================

	jobRecord := &common.Job{
		ID:         jobID,
		Spec:       jobSpec,
		Status:     common.StatusPending,
		SubmitTime: time.Now(),
		Attempts:   0,
		Metrics:    make(map[string]interface{}),
	}

	// Store job in persistent storage (etcd)
	err = jc.jobStore.SaveJob(ctx, jobRecord)
	if err != nil {
		jc.log.Error("Failed to save job %s: %v", jobID, err)
		return nil, fmt.Errorf("job persistence failed: %w", err)
	}

	jc.log.Info("Job %s persisted to storage", jobID)

	// ========================================================================
	// STEP 4: Call global scheduler (cluster selection)
	// ========================================================================

	globalDecision, err := jc.globalScheduler.ScheduleJob(ctx, jobSpec)

	if err != nil {
		jc.log.Error("Global scheduling failed for job %s: %v", jobID, err)
		jobRecord.Status = common.StatusFailed
		jobRecord.ErrorMsg = fmt.Sprintf("scheduling failed: %v", err)
		jc.jobStore.SaveJob(context.Background(), jobRecord)
		return nil, fmt.Errorf("global scheduling failed: %w", err)
	}

	jc.log.Info("Job %s scheduled to cluster %s (score=%.1f)",
		jobID, globalDecision.ClusterID, globalDecision.ClusterScore)

	// ========================================================================
	// STEP 5: Update job record with scheduling decision
	// ========================================================================

	jobRecord.Status = common.StatusScheduled
	jobRecord.ClusterID = globalDecision.ClusterID
	jobRecord.NodeID = string(rune(len(globalDecision.ClusterID))) // Will be set by local scheduler
	jobRecord.ScheduleTime = time.Now()

	err = jc.jobStore.SaveJob(ctx, jobRecord)
	if err != nil {
		jc.log.Warn("Failed to update job status (non-fatal): %v", err)
	}

	// ========================================================================
	// STEP 6: Record in idempotency cache for deduplication
	// ========================================================================

	err = jc.idempotencyMgr.RecordSuccess(ctx, jobSpec.RequestID, jobID)
	if err != nil {
		jc.log.Warn("Failed to record idempotency (non-fatal): %v", err)
	}

	// ========================================================================
	// STEP 7: Return complete result
	// ========================================================================

	result := &SchedulingResult{
		JobID:              jobID,
		ClusterID:          globalDecision.ClusterID,
		NodeID:             globalDecision.NodeID,
		GPUIndices:         globalDecision.GPUIndices,
		ClusterScore:       globalDecision.ClusterScore,
		PlacementReasons:   globalDecision.PlacementReasons,
		PodName:            "", // Will be set after pod creation
		LocalSchedulerAddr: globalDecision.LocalSchedulerAddr,
		CreatedAt:          time.Now(),
	}

	jc.log.Info("✓ Job %s successfully scheduled (result=%+v)", jobID, result)
	return result, nil
}

// MonitorJob monitors job execution and handles completion
func (jc *JobCoordinator) MonitorJob(ctx context.Context, jobID string) error {
	jobRecord, err := jc.jobStore.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("get job failed: %w", err)
	}

	// Poll for completion
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	maxAge := time.Duration(jobRecord.Spec.TimeoutSecs) * time.Second
	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			jc.log.Info("Monitoring stopped for job %s", jobID)
			return ctx.Err()

		case <-ticker.C:
			// Check if job completed
			if jobRecord.Status == common.StatusSucceeded || jobRecord.Status == common.StatusFailed {
				jc.log.Info("Job %s completed (status=%s)", jobID, jobRecord.Status)
				jc.leaseManager.ReleaseLeaseForJob(context.Background(), jobID)
				return nil
			}

			// Check for timeout
			if time.Since(startTime) > maxAge {
				jc.log.Error("Job %s timeout after %.0fs", jobID, maxAge.Seconds())
				jc.leaseManager.ReleaseLeaseForJob(context.Background(), jobID)
				return fmt.Errorf("job timeout")
			}

			// Refresh job status from storage
			latest, err := jc.jobStore.GetJob(ctx, jobID)
			if err == nil && latest != nil {
				jobRecord = latest
			}
		}
	}
}

// RetryJob retries a failed job with exponential backoff
func (jc *JobCoordinator) RetryJob(ctx context.Context, jobID string) error {
	jobRecord, err := jc.jobStore.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("get job failed: %w", err)
	}

	if jobRecord.Attempts >= jobRecord.Spec.MaxRetries {
		return fmt.Errorf("max retries exceeded (%d)", jobRecord.Spec.MaxRetries)
	}

	// Calculate exponential backoff
	backoffSeconds := 1 << uint(jobRecord.Attempts) // 2^attempts
	if backoffSeconds > 300 {                       // Cap at 5 minutes
		backoffSeconds = 300
	}

	jc.log.Info("Retrying job %s in %d seconds (attempt %d/%d)",
		jobID, backoffSeconds, jobRecord.Attempts+1, jobRecord.Spec.MaxRetries)

	// Wait for backoff period
	select {
	case <-time.After(time.Duration(backoffSeconds) * time.Second):
	case <-ctx.Done():
		return ctx.Err()
	}

	// Retry by calling ScheduleJob again
	jobRecord.Attempts++
	jobRecord.Status = common.StatusPending
	jobRecord.NextRetryAt = time.Time{}

	err = jc.jobStore.SaveJob(ctx, jobRecord)
	if err != nil {
		return fmt.Errorf("save job failed: %w", err)
	}

	_, err = jc.ScheduleJob(ctx, jobRecord.Spec)
	return err
}

// CancelJob cancels a running job
func (jc *JobCoordinator) CancelJob(ctx context.Context, jobID string) error {
	jobRecord, err := jc.jobStore.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("get job failed: %w", err)
	}

	if jobRecord.Status != common.StatusPending && jobRecord.Status != common.StatusRunning {
		return fmt.Errorf("cannot cancel job in state: %s", jobRecord.Status)
	}

	// Release lease to allow other executors
	err = jc.leaseManager.ReleaseLeaseForJob(ctx, jobID)
	if err != nil {
		jc.log.Warn("Failed to release lease: %v", err)
	}

	// Delete pod if running
	if jobRecord.PodName != "" {
		err = jc.executor.CancelJob(jobID)
		if err != nil {
			jc.log.Warn("Failed to cancel executor: %v", err)
		}
	}

	// Update status
	jobRecord.Status = common.StatusFailed
	jobRecord.ErrorMsg = "cancelled by user"
	jobRecord.EndTime = time.Now()

	err = jc.jobStore.SaveJob(ctx, jobRecord)
	if err != nil {
		return fmt.Errorf("save job failed: %w", err)
	}

	jc.log.Info("Job %s cancelled", jobID)
	return nil
}

// GetJobStatus gets current job status
func (jc *JobCoordinator) GetJobStatus(ctx context.Context, jobID string) (*common.Job, error) {
	return jc.jobStore.GetJob(ctx, jobID)
}
