package orchestrator

import (
	"context"
	"fmt"
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
) *JobCoordinator {
	return &JobCoordinator{
		idempotencyMgr:  idempotencyMgr,
		leaseManager:    leaseManager,
		jobStore:        jobStore,
		globalScheduler: globalScheduler,
		log:             logger.Get(),
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
		jc.log.Info("Duplicate request detected: %s, returning cached result", jobSpec.RequestID)
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

	jc.log.Info("Acquired lease for job %s (leaseID=%d, heartbeat started)", jobID, leaseID)

	// Ensure we release lease on error
	defer func() {
		if err != nil {
			jc.leaseManager.ReleaseLeaseForJob(context.Background(), jobID)
		}
	}()

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
	}

	// Store job with lease for auto-cleanup (CRITICAL FIX)
	// When executor crashes, lease expires → job auto-deleted
	err = jc.jobStore.SaveJob(ctx, jobRecord, leaseID)
	if err != nil {
		jc.log.Error("Failed to save job %s: %v", jobID, err)
		return nil, fmt.Errorf("job persistence failed: %w", err)
	}

	jc.log.Info("Job %s persisted (leaseID=%d for auto-cleanup)", jobID, leaseID)

	// ========================================================================
	// STEP 4: Call global scheduler (cluster selection)
	// ========================================================================

	globalDecision, err := jc.globalScheduler.ScheduleJob(ctx, jobSpec)
	if err != nil {
		jc.log.Error("Global scheduling failed for job %s: %v", jobID, err)
		jobRecord.Status = common.StatusFailed
		jobRecord.ErrorMsg = fmt.Sprintf("scheduling failed: %v", err)
		jc.jobStore.SaveJob(context.Background(), jobRecord, leaseID)
		return nil, fmt.Errorf("global scheduling failed: %w", err)
	}

	jc.log.Debug("GlobalDecision json (body): ", globalDecision)
	jc.log.Info("Job %s scheduled to cluster %s", jobID, globalDecision.ClusterID)

	// ========================================================================
	// STEP 5: Update job record with scheduling decision
	// ========================================================================

	jobRecord.Status = common.StatusScheduled
	jobRecord.ClusterID = globalDecision.ClusterID
	jobRecord.ScheduleTime = time.Now()

	err = jc.jobStore.SaveJob(ctx, jobRecord, leaseID)
	if err != nil {
		jc.log.Warn("Failed to update job status (non-fatal): %v", err)
	}

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

	//jc.log.Info("Logging Executor (payload): ", jc.executor)
	//jc.log.Info("Logging Executor config (payload): ", jc.executor.Config)

	//jc.log.Debug("Namespace value: Issue")
	////jc.log.Debug("Accessing Namespace: ", &jc.executor.Config.Namespace)
	//// Build execution context from local decision
	//execCtx := &common2.ExecutionContext{
	//	JobID: jobID,
	//	LocalDecision: &local.LocalSchedulingDecision{ // Convert globalDecision to LocalSchedulingDecision
	//		JobID:            jobID,
	//		NodeID:           globalDecision.NodeID,
	//		GPUIndices:       globalDecision.GPUIndices,
	//		NodeScore:        globalDecision.ClusterScore,
	//		PlacementReasons: globalDecision.PlacementReasons,
	//		ScheduledAt:      time.Now(),
	//	},
	//	Namespace: jc.executor.Config.Namespace,
	//	//Namespace:  jc.namespace,
	//	StartTime:  time.Now(),
	//	Timeout:    time.Duration(jobSpec.TimeoutSecs) * time.Second,
	//	Status:     common2.StatusPending,
	//	NodeID:     globalDecision.NodeID,
	//	GPUIndices: globalDecision.GPUIndices,
	//	Metrics:    make(map[string]interface{}),
	//}
	//
	//jc.log.Debug("Executing Context json(payload): ", execCtx)
	//
	//// Generate Pod name
	//podName := fmt.Sprintf("job-%s", jobID[:8]) // Truncate for K8s name limits
	//execCtx.PodName = podName
	//
	//// Create PodSpec
	//podSpec := &common2.PodSpec{
	//	PodName:         podName,
	//	Namespace:       jc.executor.Config.Namespace,
	//	Image:           jobSpec.Image,
	//	ImagePullPolicy: jc.executor.Config.ImagePullPolicy,
	//	EnvVars: map[string]string{
	//		"ARES_JOB_ID":        jobID,
	//		"ARES_CLUSTER_ID":    globalDecision.ClusterID,
	//		"ARES_NODE_ID":       globalDecision.NodeID,
	//		"ARES_ASSIGNED_GPUS": fmt.Sprintf("%v", globalDecision.GPUIndices),
	//		"ARES_GPU_COUNT":     fmt.Sprintf("%d", len(globalDecision.GPUIndices)),
	//	},
	//	MemoryMB:      jobSpec.MemoryMB,
	//	CPUMillis:     jobSpec.CPUMillis,
	//	GPUCount:      len(globalDecision.GPUIndices),
	//	GPUIndices:    globalDecision.GPUIndices,
	//	Timeout:       time.Duration(jobSpec.TimeoutSecs) * time.Second,
	//	RestartPolicy: jc.executor.Config.RestartPolicy,
	//	NodeID:        globalDecision.NodeID,
	//	Labels: map[string]string{
	//		"app":          "ares-job",
	//		"job-id":       jobID,
	//		"cluster-id":   globalDecision.ClusterID,
	//		"scheduled-by": "ares",
	//	},
	//}
	//jc.log.Debug("PodSpec json(payload): ", execCtx)
	//
	//// ✅ THE CRITICAL CALL: Create Pod in Kubernetes
	//createdPodName, err := jc.executor.K8sClient.CreatePod(ctx, podSpec)
	//if err != nil {
	//	jc.log.Error("Failed to create Pod for job %s: %v", jobID, err)
	//	jobRecord.Status = common.StatusFailed
	//	jobRecord.ErrorMsg = fmt.Sprintf("pod creation failed: %v", err)
	//	jc.jobStore.SaveJob(context.Background(), jobRecord, leaseID)
	//	return nil, fmt.Errorf("pod creation failed: %w", err)
	//}
	//
	//execCtx.PodName = createdPodName
	//jc.log.Info("✅ Pod created: %s for job %s on node %s", createdPodName, jobID, globalDecision.NodeID)
	//
	//// Update job record with Pod info
	//jobRecord.Status = common.StatusRunning
	//jobRecord.PodName = createdPodName
	//jobRecord.NodeID = globalDecision.NodeID
	//jobRecord.AllocatedGPUIndices = globalDecision.GPUIndices
	//jobRecord.StartTime = time.Now()
	//err = jc.jobStore.SaveJob(ctx, jobRecord, leaseID)
	//if err != nil {
	//	jc.log.Warn("Failed to update job with Pod info: %v", err)
	//}

	// ========================================================================
	// STEP 8: Start monitoring in background
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
		//PodName:            createdPodName, // ✅ Include Pod name
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

// RetryJob retries a failed job with exponential backoff
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

	// Calculate backoff (exponential: 1, 2, 4, 8, 16... max 300s)
	backoffSeconds := 1 << uint(jobRecord.Attempts)
	if backoffSeconds > 300 {
		backoffSeconds = 300
	}

	jc.log.Info("Retrying job %s in %d seconds (attempt %d/%d)",
		jobID, backoffSeconds, jobRecord.Attempts+1, jobRecord.Spec.MaxRetries)

	select {
	case <-time.After(time.Duration(backoffSeconds) * time.Second):
	case <-ctx.Done():
		return ctx.Err()
	}

	// Retry: schedule again
	jobRecord.Attempts++
	jobRecord.Status = common.StatusPending
	jobRecord.NextRetryAt = time.Time{}

	err = jc.jobStore.SaveJob(ctx, jobRecord, leaseID)
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
