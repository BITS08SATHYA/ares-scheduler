package orchestrator

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/idempotency"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/job"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/lease"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/global"
)

// ============================================================================
// JOB COORDINATOR (Layer 10) - CORRECTED WITH FENCING TOKENS
// ============================================================================
// Orchestrates: Dedup → Lease → Persist → Schedule → Execute
// CRITICAL FIX: Fencing token checks prevent split-brain

type JobCoordinator struct {
	idempotencyMgr  *idempotency.IdempotencyManager
	leaseManager    *lease.LeaseManager
	jobStore        job.JobStore
	globalScheduler *global.GlobalScheduler
	log             *logger.Logger
	metricsRecorder *MetricsRecorder
}

// Metrics Recorder
type MetricsRecorder struct {
	OnDuplicateBlocked  func()
	OnLeaseAcquired     func()
	OnJobPriority       func(int)
	OnSchedulingLatency func(time.Duration)
	OnFencingTokenSet   func()
	OnJobCompleted      func(bool)
	OnJobQueued         func()
	OnJobDequeued       func()
	OnJobRescheduled    func()
	OnJobE2ELatency     func(time.Duration) // Record End-to-End Latency
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
	LeaseID            int64 // CRITICAL: Track lease ID for fencing
	CreatedAt          time.Time
}

func NewJobCoordinator(
	idempotencyMgr *idempotency.IdempotencyManager,
	leaseManager *lease.LeaseManager,
	jobStore job.JobStore,
	globalScheduler *global.GlobalScheduler,
	metricsRecorder *MetricsRecorder,
) *JobCoordinator {
	return &JobCoordinator{
		idempotencyMgr:  idempotencyMgr,
		leaseManager:    leaseManager,
		jobStore:        jobStore,
		globalScheduler: globalScheduler,
		log:             logger.Get(),
		metricsRecorder: metricsRecorder,
	}
}

// ============================================================================
// SCHEDULE JOB (CORRECTED WITH HEARTBEAT + FENCING)
// ============================================================================

// ScheduleJob orchestrates the complete job scheduling pipeline
//
// Pipeline (CORRECTED):
// 1. Check for duplicate (idempotency)
// 2. Acquire distributed lease + START HEARTBEAT (NEW)
// 3. Create job record (persistence)
// 4. Call global scheduler (cluster selection)
// 5. Call local scheduler via HTTP
// 6. Monitor job with FENCING TOKEN CHECKS (NEW)
//
// FIXED: Now returns proper SchedulingResult instead of nil, nil
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
		if jc.metricsRecorder != nil && jc.metricsRecorder.OnDuplicateBlocked != nil {
			jc.metricsRecorder.OnDuplicateBlocked()
		}
		jc.log.Debug("Duplicate request detected: %s, returning cached result", jobSpec.RequestID)
		result := &SchedulingResult{
			JobID:     cachedResult.JobID,
			CreatedAt: cachedResult.SubmitTime,
		}
		return result, nil
	}

	if err != nil {
		jc.log.Warn("Idempotency check failed (non-fatal): %v", err)
	}

	// ========================================================================
	// STEP 2: Acquire distributed lease (with heartbeat)
	// ========================================================================

	jobID := fmt.Sprintf("job-%d-%s", time.Now().UnixNano(), jobSpec.RequestID)

	// Acquire lease AND start heartbeat goroutine (CRITICAL FIX)
	acquired, leaseID, err := jc.leaseManager.AcquireLeaseForJob(ctx, jobID)
	if !acquired {
		jc.log.Error("Could not acquire lease for job %s (another executor owns it)", jobID)
		return nil, fmt.Errorf("lease acquisition failed: another executor owns this job")
	}

	if err != nil {
		jc.log.Error("Lease acquisition error: %v", err)
		return nil, fmt.Errorf("lease error: %w", err)
	}

	if jc.metricsRecorder != nil && jc.metricsRecorder.OnLeaseAcquired != nil {
		jc.metricsRecorder.OnLeaseAcquired()
	}

	jc.log.Info("Acquired lease for job %s (leaseID=%d, heartbeat started)", jobID, leaseID)

	// ========================================================================
	// STEP 3: Create job record with lease attachment
	// ========================================================================

	jobRecord := &common.Job{
		ID:         jobID,
		Spec:       jobSpec,
		Status:     common.StatusPending,
		SubmitTime: time.Now(),
		Attempts:   0,
		Metrics:    make(map[string]interface{}),
		ExecutionLease: &common.LeaseInfo{
			LeaseID:    fmt.Sprintf("%d", leaseID), // Convert int64 to string
			JobID:      jobID,
			ExecutorID: "ares-executor", // Or actual executor ID
			GrantedAt:  time.Now(),
			TTLSeconds: 30,
			ExpiresAt:  time.Now().Add(30 * time.Second),
		},
	}

	// Store job with lease for auto-cleanup (CRITICAL FIX)
	// When executor crashes, lease expires → job auto-deleted
	err = jc.jobStore.SaveJob(ctx, jobRecord, leaseID)
	if err != nil {
		jc.log.Error("Failed to save job %s: %v", jobID, err)
		return nil, fmt.Errorf("job persistence failed: %w", err)
	}

	// Job Priority value is checked and queued before the job gets scheduled
	if jc.metricsRecorder != nil && jc.metricsRecorder.OnJobPriority != nil {
		jc.metricsRecorder.OnJobPriority(jobSpec.Priority)
	}

	jc.log.Info("Job %s persisted (leaseID=%d for auto-cleanup)", jobID, leaseID)
	jc.log.Info("Executor will monitor Pod and update this Job record")
	// ========================================================================
	// STEP 4: Call global scheduler (cluster selection)
	// ========================================================================
	schedStart := time.Now()
	globalDecision, err := jc.globalScheduler.ScheduleJob(ctx, jobRecord)
	if jc.metricsRecorder != nil && jc.metricsRecorder.OnSchedulingLatency != nil {
		jc.metricsRecorder.OnSchedulingLatency(time.Since(schedStart))
	}
	if err != nil {
		jc.log.Warn("Global scheduling failed for job %s: %v (queueing for retry)", jobID, err)
		jobRecord.Status = common.StatusQueued
		jobRecord.ErrorMsg = fmt.Sprintf("queued: %v", err)
		if jc.metricsRecorder != nil && jc.metricsRecorder.OnJobQueued != nil {
			jc.metricsRecorder.OnJobQueued()
		}
		jc.jobStore.SaveJob(context.Background(), jobRecord, leaseID)

		// Return success to the user — job is accepted but queued
		return &SchedulingResult{
			JobID:            jobID,
			ClusterID:        "queued",
			CreatedAt:        time.Now(),
			PlacementReasons: []string{fmt.Sprintf("queued: waiting for resources (%v)", err)},
		}, nil
	}

	jc.log.Debug("GlobalDecision json (body): ", globalDecision)
	jc.log.Info("Job %s scheduled to cluster %s", jobID, globalDecision.ClusterID)

	// ========================================================================
	// STEP 5: Update job record with scheduling decision
	// ========================================================================

	jobRecord.Status = common.StatusScheduled
	jobRecord.ClusterID = globalDecision.ClusterID
	jobRecord.ScheduleTime = time.Now()

	if jc.metricsRecorder != nil && jc.metricsRecorder.OnJobDequeued != nil {
		jc.metricsRecorder.OnJobDequeued()
	}

	// Attached the executionToken (fencing Token) attached to the job record that goes to the pod (during execution)
	jobRecord.ExecutionToken = fmt.Sprintf("ares-fence-%s-%d", jobID, leaseID)
	if jc.metricsRecorder != nil && jc.metricsRecorder.OnFencingTokenSet != nil {
		jc.metricsRecorder.OnFencingTokenSet()
	}

	err = jc.jobStore.SaveJob(ctx, jobRecord, leaseID)
	if err != nil {
		jc.log.Warn("Failed to update job status (non-fatal): %v", err)
	}

	// Ensure we release lease on error
	defer func() {
		if err != nil {
			err := jc.leaseManager.ReleaseLeaseForJob(context.Background(), jobID)
			if err != nil {
				return
			}
		}
	}()

	// ========================================================================
	// STEP 6: Record in idempotency cache
	// ========================================================================

	err = jc.idempotencyMgr.RecordSuccess(ctx, jobSpec.RequestID, jobID)
	if err != nil {
		jc.log.Warn("Failed to record idempotency (non-fatal): %v", err)
	}

	// ========================================================================
	// STEP 7: Return scheduling result with lease ID
	// Critical step: LocalScheduler --> returns to Job Coordinator --> calls executor (real k8 client)
	// ========================================================================

	go func() {
		monitorCtx := context.Background()
		err := jc.MonitorJob(monitorCtx, jobID, leaseID)
		if err != nil {
			jc.log.Error("Monitoring failed for job %s: %v", jobID, err)
		}
	}()

	// ========================================================================
	// STEP 9: Return result
	// ========================================================================

	result := &SchedulingResult{
		JobID:            jobID,
		ClusterID:        globalDecision.ClusterID,
		NodeID:           globalDecision.NodeID,
		GPUIndices:       globalDecision.GPUIndices,
		ClusterScore:     globalDecision.ClusterScore,
		PlacementReasons: globalDecision.PlacementReasons,
		//PodName:            createdPodName,
		LocalSchedulerAddr: globalDecision.LocalSchedulerAddr,
		LeaseID:            leaseID,
		CreatedAt:          time.Now(),
	}

	jc.log.Debug("The final Result (json payload): ", result)

	//jc.log.Info("Job %s fully scheduled (Pod=%s, leaseID=%d)", jobID, createdPodName, leaseID)

	return result, nil

}

// ============================================================================
// MONITOR JOB (operates on JOBS, not pods)
// ============================================================================
// CRITICAL: This monitors the LOGICAL JOB, not the physical POD
// Job lifecycle: PENDING → SCHEDULED → RUNNING → SUCCEEDED/FAILED
// Pod lifecycle: Pending → Running → Succeeded/Failed (handled by executor)
//
// This method:
// ✓ Tracks job status in etcd
// ✓ Checks if lease still owned (fencing)
// ✓ Detects job completion
// ✗ Does NOT directly manipulate pods (executor does that)
func (jc *JobCoordinator) MonitorJob(ctx context.Context, jobID string, leaseID int64) error {
	jobRecord, err := jc.jobStore.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("get job failed: %w", err)
	}

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
			// CRITICAL: Check fencing token before reading job status
			fencingErr := jc.checkFencingToken(ctx, jobID, leaseID)
			if fencingErr != nil {
				jc.log.Error("FENCING: %v - aborting monitoring for job %s", fencingErr, jobID)
				// Don't write anything - prevent split-brain!
				return fencingErr
			}

			// Safe to read job status (we still own the lease)
			latest, err := jc.jobStore.GetJob(ctx, jobID)
			if err == nil && latest != nil {
				jobRecord = latest
			}

			//// Check if job completed
			//if jobRecord.Status == common.StatusSucceeded || jobRecord.Status == common.StatusFailed {
			//	jc.log.Info("Job %s completed (status=%s)", jobID, jobRecord.Status)
			//	jc.leaseManager.ReleaseLeaseForJob(context.Background(), jobID)
			//	return nil
			//}

			// Check if job completed
			if jobRecord.Status == common.StatusSucceeded {
				jc.log.Info("Job %s succeeded", jobID)

				// ★ Record end-to-end latency
				if jc.metricsRecorder != nil && jc.metricsRecorder.OnJobE2ELatency != nil {
					e2eLatency := time.Since(jobRecord.SubmitTime)
					jc.metricsRecorder.OnJobE2ELatency(e2eLatency)
				}

				if jc.metricsRecorder != nil && jc.metricsRecorder.OnJobCompleted != nil {
					jc.metricsRecorder.OnJobCompleted(true)
				}
				jc.leaseManager.ReleaseLeaseForJob(context.Background(), jobID)
				return nil
			}

			if jobRecord.Status == common.StatusFailed {
				if jc.metricsRecorder != nil && jc.metricsRecorder.OnJobCompleted != nil {
					jc.metricsRecorder.OnJobCompleted(false)
				}
				// Auto-retry if attempts remaining
				if jobRecord.Spec.MaxRetries > 0 && jobRecord.Attempts < jobRecord.Spec.MaxRetries {
					jc.log.Info("Job %s failed, auto-retrying (attempt %d/%d)",
						jobID, jobRecord.Attempts+1, jobRecord.Spec.MaxRetries)
					go func() {
						retryErr := jc.RetryJob(context.Background(), jobID, leaseID)
						if retryErr != nil {
							jc.log.Error("Auto-retry failed for job %s: %v", jobID, retryErr)
						}
					}()
					return nil // Stop monitoring this instance, retry creates new monitor
				}
				jc.log.Info("Job %s failed (no retries left)", jobID)
				jc.leaseManager.ReleaseLeaseForJob(context.Background(), jobID)
				return nil
			}

			// Check for timeout
			if time.Since(startTime) > maxAge {
				jc.log.Error("Job %s timeout after %.0fs", jobID, maxAge.Seconds())
				jc.leaseManager.ReleaseLeaseForJob(context.Background(), jobID)
				return fmt.Errorf("job timeout")
			}
		}
	}
}

// ============================================================================
// FENCING TOKEN CHECK (CRITICAL NEW FUNCTION)
// ============================================================================

// checkFencingToken verifies we still own the lease
//
// CRITICAL: Call before EVERY job status update
// If lease lost: Return error and ABORT write (prevent split-brain)
//
// Scenario that this prevents:
// 1. Executor A and B both start, A gets lease
// 2. Network partition between A and etcd
// 3. A's heartbeat stops but A doesn't know
// 4. B's heartbeat renews A's lease (wrong!)
// 5. B updates job status (duplicate execution!)
//
// With fencing:
// 5. A: Check lease before write → lease expired → ABORT ✓
// 5. B: Check lease before write → lease ours → proceed ✓
func (jc *JobCoordinator) checkFencingToken(ctx context.Context, jobID string, leaseID int64) error {
	// This is the CRITICAL check that prevents split-brain
	err := jc.leaseManager.CheckLeaseOwnership(ctx, jobID, leaseID)
	if err != nil {
		jc.log.Error("FENCING TRIGGERED: %v", err)
		// DO NOT UPDATE JOB - prevent split-brain!
		return err
	}

	jc.log.Debug("Fencing check passed for job %s", jobID)
	return nil
}

// ============================================================================
// SAFE JOB UPDATE (WITH FENCING)
// ============================================================================

// SafeUpdateJobStatus updates job status only if we still own the lease
//
// CRITICAL: All job updates should use this method (not SaveJob directly)
func (jc *JobCoordinator) SafeUpdateJobStatus(
	ctx context.Context,
	jobID string,
	leaseID int64,
	newStatus common.JobStatus,
) error {
	// FENCING CHECK: Verify we still own the lease
	if err := jc.checkFencingToken(ctx, jobID, leaseID); err != nil {
		return err // Abort - don't write
	}

	// Safe to update
	jobRecord, err := jc.jobStore.GetJob(ctx, jobID)
	if err != nil {
		return err
	}

	jobRecord.Status = newStatus
	err = jc.jobStore.SaveJob(ctx, jobRecord, leaseID)
	if err != nil {
		return err
	}

	jc.log.Info("Job %s updated to status %s (with fencing check)", jobID, newStatus)
	return nil
}

// ============================================================================
// JOB COMPLETION WITH FENCING
// ============================================================================

// CompleteJob marks job as complete (SUCCESS or FAILED)
//
// CRITICAL: Uses fencing to ensure only winner writes result
func (jc *JobCoordinator) CompleteJob(
	ctx context.Context,
	jobID string,
	leaseID int64,
	finalStatus common.JobStatus,
	exitCode int,
	errorMsg string,
) error {
	// FENCING: Check we still own the lease before writing result
	if err := jc.checkFencingToken(ctx, jobID, leaseID); err != nil {
		jc.log.Error("FENCING PREVENTED SPLIT-BRAIN: Job %s - %v", jobID, err)
		return err // Abort! Don't write result!
	}

	// Safe to write result
	jobRecord, err := jc.jobStore.GetJob(ctx, jobID)
	if err != nil {
		return err
	}

	jobRecord.Status = finalStatus
	jobRecord.ExitCode = exitCode
	jobRecord.ErrorMsg = errorMsg
	jobRecord.EndTime = time.Now()

	err = jc.jobStore.SaveJob(ctx, jobRecord, leaseID)
	if err != nil {
		return err
	}

	// Release lease when job completes
	jc.leaseManager.ReleaseLeaseForJob(ctx, jobID)

	jc.log.Info("✓ Job %s completed with status %s (fencing passed)", jobID, finalStatus)
	return nil
}

// ============================================================================
// RETRY JOB
// ============================================================================

// RetryJob retries a failed job with exponential backoff + jitter
func (jc *JobCoordinator) RetryJob(ctx context.Context, jobID string, leaseID int64) error {
	// Fencing check before retry
	if err := jc.checkFencingToken(ctx, jobID, leaseID); err != nil {
		return err
	}

	jobRecord, err := jc.jobStore.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("get job failed: %w", err)
	}

	if jobRecord.Attempts >= jobRecord.Spec.MaxRetries {
		return fmt.Errorf("max retries exceeded (%d)", jobRecord.Spec.MaxRetries)
	}

	// Calculate backoff with jitter (exponential: 1, 2, 4, 8, 16... max 300s)
	// Jitter prevents thundering herd when many jobs fail simultaneously
	baseBackoff := 1 << uint(jobRecord.Attempts)
	if baseBackoff > 300 {
		baseBackoff = 300
	}
	jitterRange := baseBackoff / 4
	if jitterRange < 1 {
		jitterRange = 1
	}
	backoffSeconds := baseBackoff + (int(time.Now().UnixNano()) % (2 * jitterRange)) - jitterRange
	if backoffSeconds < 1 {
		backoffSeconds = 1
	}

	// Record retry schedule (visible via GET /status/job)
	jobRecord.NextRetryAt = time.Now().Add(time.Duration(backoffSeconds) * time.Second)
	jobRecord.Status = common.StatusRetrying
	_ = jc.jobStore.SaveJobFinal(ctx, jobRecord)

	jc.log.Info("Retrying job %s in %ds (attempt %d/%d, backoff=%ds, jitter applied)",
		jobID, backoffSeconds, jobRecord.Attempts+1, jobRecord.Spec.MaxRetries, baseBackoff)

	select {
	case <-time.After(time.Duration(backoffSeconds) * time.Second):
	case <-ctx.Done():
		return ctx.Err()
	}

	// Retry: schedule again
	jobRecord.Attempts++
	jobRecord.Status = common.StatusPending
	jobRecord.NextRetryAt = time.Time{}

	err = jc.jobStore.SaveJobFinal(ctx, jobRecord)
	if err != nil {
		return fmt.Errorf("save job failed: %w", err)
	}

	_, err = jc.ScheduleJob(ctx, jobRecord.Spec)
	return err
}

// ============================================================================
// CANCEL JOB
// ============================================================================

// CancelJob cancels a job that's pending or running
func (jc *JobCoordinator) CancelJob(ctx context.Context, jobID string, leaseID int64) error {
	// Fencing check before cancellation
	if err := jc.checkFencingToken(ctx, jobID, leaseID); err != nil {
		return fmt.Errorf("cannot cancel: fencing check failed: %w", err)
	}

	jobRecord, err := jc.jobStore.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("get job failed: %w", err)
	}

	if jobRecord.Status != common.StatusPending && jobRecord.Status != common.StatusRunning {
		return fmt.Errorf("cannot cancel job in state: %s", jobRecord.Status)
	}

	// Release lease
	err = jc.leaseManager.ReleaseLeaseForJob(ctx, jobID)
	if err != nil {
		jc.log.Warn("Failed to release lease: %v", err)
	}

	// Delete pod if running
	if jobRecord.PodName != "" {
		// I have to implement this
		//err = jc.executor.CancelJob(jobID)
		//if err != nil {
		//	jc.log.Warn("Failed to cancel executor: %v", err)
		//}
	}

	// Update status
	jobRecord.Status = common.StatusFailed
	jobRecord.ErrorMsg = "cancelled by user"
	jobRecord.EndTime = time.Now()

	err = jc.jobStore.SaveJob(ctx, jobRecord, leaseID)
	if err != nil {
		return fmt.Errorf("save job failed: %w", err)
	}

	jc.log.Info("Job %s cancelled", jobID)
	return nil
}

// ============================================================================
// GET JOB STATUS
// ============================================================================

// GetJobStatus retrieves the current status of a job
func (jc *JobCoordinator) GetJobStatus(ctx context.Context, jobID string) (*common.Job, error) {
	return jc.jobStore.GetJob(ctx, jobID)
}

// ListJobs: List all jobs with optional filter
func (jc *JobCoordinator) ListJobs(ctx context.Context, filter *job.JobFilter) ([]*common.Job, error) {
	return jc.jobStore.ListJobs(ctx, filter)
}

// StartReconcileLoop: Background loop that retries QUEUED jobs when resources free up.
// This is the heartbeat of the scheduler — without it, queued jobs never run.
//
// How it works:
//  1. Every N seconds, scan etcd for jobs with status QUEUED
//  2. For each queued job, try to schedule it
//  3. If successful → status becomes SCHEDULED → RUNNING
//  4. If still no resources → stays QUEUED → retry next round
//
// This is identical to how Kubernetes scheduler works:
//
//	kube-scheduler has a scheduling queue + retry loop
func (jc *JobCoordinator) StartReconcileLoop(ctx context.Context, interval time.Duration) {
	jc.log.Info("RECONCILER: Starting reconcile loop (interval=%s)", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			jc.log.Info("RECONCILER: Stopped")
			return
		case <-ticker.C:
			jc.reconcileQueuedJobs(ctx)
		}
	}
}

// reconcileQueuedJobs: Try to schedule all QUEUED jobs
func (jc *JobCoordinator) reconcileQueuedJobs(ctx context.Context) {
	// Get all queued jobs
	queuedStatus := common.StatusQueued
	filter := &job.JobFilter{
		Status: &queuedStatus,
		Limit:  50, // Process up to 50 per round
	}

	jobs, err := jc.jobStore.ListJobs(ctx, filter)
	if err != nil {
		jc.log.Warn("RECONCILER: Failed to list queued jobs: %v", err)
		return
	}

	if len(jobs) == 0 {
		return
	}

	jc.log.Info("RECONCILER: Found %d queued jobs, attempting to schedule", len(jobs))

	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].Spec.Priority != jobs[j].Spec.Priority {
			return jobs[i].Spec.Priority > jobs[j].Spec.Priority // higher priority first
		}
		return jobs[i].SubmitTime.Before(jobs[j].SubmitTime) // FIFO within same priority
	})

	for _, jobRecord := range jobs {
		// Try to schedule
		globalDecision, err := jc.globalScheduler.ScheduleJob(ctx, jobRecord)
		if err != nil {
			jc.log.Debug("RECONCILER: Job %s still unschedulable: %v", jobRecord.ID, err)
			continue // Stay queued, try again next round
		}

		// Success! Update job record
		jobRecord.Status = common.StatusScheduled
		jobRecord.ClusterID = globalDecision.ClusterID
		jobRecord.NodeID = globalDecision.NodeID
		jobRecord.ScheduleTime = time.Now()
		jobRecord.ErrorMsg = ""

		var leaseID int64
		if jobRecord.ExecutionLease != nil {
			leaseID, _ = strconv.ParseInt(jobRecord.ExecutionLease.LeaseID, 10, 64)
		}

		err = jc.jobStore.SaveJob(ctx, jobRecord, leaseID)
		if err != nil {
			jc.log.Warn("RECONCILER: Failed to save scheduled job %s: %v", jobRecord.ID, err)
			continue
		}

		// ★ FIX: Update metrics — job moved from QUEUED → SCHEDULED
		if jc.metricsRecorder != nil {
			if jc.metricsRecorder.OnJobDequeued != nil {
				jc.metricsRecorder.OnJobDequeued() // QueuedJobs--
			}
			if jc.metricsRecorder.OnJobRescheduled != nil {
				jc.metricsRecorder.OnJobRescheduled() // ActiveJobs++
			}
			if jc.metricsRecorder.OnJobPriority != nil {
				jc.metricsRecorder.OnJobPriority(jobRecord.Spec.Priority) // Record priority
			}
		}

		// Start monitoring
		go func(jID string, lID int64) {
			monitorCtx := context.Background()
			jc.MonitorJob(monitorCtx, jID, lID)
		}(jobRecord.ID, leaseID)

		jc.log.Info("RECONCILER: ★ Job %s scheduled to cluster %s (was queued)",
			jobRecord.ID, globalDecision.ClusterID)
	}
}
