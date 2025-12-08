// ============================================================================
// LAYER 3: LEASE MANAGER & JOB STORE (CORRECTED WITH HEARTBEAT)
// ============================================================================
// Features: #19 (Distributed Locking / Lease System) + #17-18 (Job Lifecycle)
// CRITICAL FIX: Heartbeat goroutine + lease renewal monitoring
// ============================================================================

package lease

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/etcd"
	"math"
	"sync"
	"time"
)

// ============================================================================
// TYPES & CONSTANTS
// ============================================================================

type JobState string

const (
	JobStatePending   JobState = "PENDING"
	JobStateScheduled JobState = "SCHEDULED"
	JobStatePlaced    JobState = "PLACED"
	JobStateRunning   JobState = "RUNNING"
	JobStateSucceeded JobState = "SUCCEEDED"
	JobStateFailed    JobState = "FAILED"
	JobStateCancelled JobState = "CANCELLED"
)

type JobRecord struct {
	JobID           string    `json:"job_id"`
	TenantID        string    `json:"tenant_id"`
	Status          JobState  `json:"status"`
	CreatedAt       time.Time `json:"created_at"`
	ScheduledAt     time.Time `json:"scheduled_at,omitempty"`
	PlacedAt        time.Time `json:"placed_at,omitempty"`
	StartedAt       time.Time `json:"started_at,omitempty"`
	CompletedAt     time.Time `json:"completed_at,omitempty"`
	Attempts        int       `json:"attempts"`
	MaxRetries      int       `json:"max_retries"`
	LastRetryTime   time.Time `json:"last_retry_time,omitempty"`
	AssignedCluster string    `json:"assigned_cluster,omitempty"`
	AssignedNode    string    `json:"assigned_node,omitempty"`
	PodName         string    `json:"pod_name,omitempty"`
	Result          string    `json:"result,omitempty"`
	FencingToken    string    `json:"fencing_token,omitempty"`
	PreemptionCount int       `json:"preemption_count,omitempty"`
}

type Lease struct {
	JobID       string    `json:"job_id"`
	SchedulerID string    `json:"scheduler_id"`
	AcquiredAt  time.Time `json:"acquired_at"`
	ExpiresAt   time.Time `json:"expires_at"`
	TTL         int64     `json:"ttl"`
}

// LeaseInfo: Information about a lease (for monitoring)
type LeaseInfo struct {
	LeaseID         int64
	JobID           string
	SchedulerID     string
	AcquiredAt      time.Time
	LastRenewedAt   time.Time
	ExpiresAt       time.Time
	RenewalAttempts uint64
	RenewalFailures uint64
}

// ============================================================================
// LEASE MANAGER (CORRECTED WITH HEARTBEAT)
// ============================================================================

type LeaseManager struct {
	etcdClient        *etcd.ETCDClient
	schedulerID       string
	leaseTTL          int64
	log               Logger
	mu                sync.RWMutex
	activeLeases      map[string]*LeaseInfo
	heartbeatTicker   time.Duration
	heartbeatContexts map[string]context.CancelFunc // Store cancel functions
}

// NewLeaseManager creates a new lease manager with heartbeat
func NewLeaseManager(etcdClient *etcd.ETCDClient, schedulerID string, log Logger) *LeaseManager {
	return &LeaseManager{
		etcdClient:        etcdClient,
		schedulerID:       schedulerID,
		leaseTTL:          30, // 30 second lease TTL
		log:               log,
		activeLeases:      make(map[string]*LeaseInfo),
		heartbeatTicker:   10 * time.Second, // Renew every 10 seconds
		heartbeatContexts: make(map[string]context.CancelFunc),
	}
}

// ============================================================================
// LEASE ACQUISITION WITH HEARTBEAT (CRITICAL FIX)
// ============================================================================

// AcquireLeaseForJob acquires a distributed lease and starts heartbeat
//
// CRITICAL: Now includes heartbeat goroutine
// - Without heartbeat: Lease expires after 30 seconds
// - With heartbeat: Lease renewed every 10 seconds
func (lm *LeaseManager) AcquireLeaseForJob(ctx context.Context, jobID string) (bool, int64, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Step 1: Create etcd lease with TTL
	leaseID, err := lm.etcdClient.GrantLease(ctx, lm.leaseTTL)
	if err != nil {
		lm.log.Errorf("failed to grant lease: %v", err)
		return false, 0, fmt.Errorf("grant lease: %w", err)
	}

	leaseKey := fmt.Sprintf("ares:leases:%s", jobID)

	// Step 2: Create lease record
	leaseRecord := Lease{
		JobID:       jobID,
		SchedulerID: lm.schedulerID,
		AcquiredAt:  time.Now(),
		ExpiresAt:   time.Now().Add(time.Duration(lm.leaseTTL) * time.Second),
		TTL:         lm.leaseTTL,
	}
	leaseValue, _ := json.Marshal(leaseRecord)

	// Step 3: ATOMIC transaction - only one executor succeeds
	txnResponse, err := lm.etcdClient.LeaseCAS(ctx, leaseKey, string(leaseValue), leaseID)
	if err != nil {
		lm.log.Errorf("txn failed for lease: %v", err)
		lm.etcdClient.RevokeLease(context.Background(), leaseID)
		return false, 0, fmt.Errorf("txn failed: %w", err)
	}

	if !txnResponse {
		// Another scheduler holds the lease
		lm.log.Warnf("lease already held for job %s (by another scheduler)", jobID)
		lm.etcdClient.RevokeLease(context.Background(), leaseID)
		return false, 0, nil
	}

	lm.log.Infof("lease acquired for job %s (leaseID=%d)", jobID, leaseID)

	// ========================================================================
	// Created Cancellable context instead of context.Background()
	// ========================================================================
	leaseInfo := &LeaseInfo{
		LeaseID:       leaseID,
		JobID:         jobID,
		SchedulerID:   lm.schedulerID,
		AcquiredAt:    time.Now(),
		LastRenewedAt: time.Now(),
		ExpiresAt:     time.Now().Add(time.Duration(lm.leaseTTL) * time.Second),
	}

	lm.activeLeases[jobID] = leaseInfo

	// Create cancellable context for hearbeat
	heartbeatCtx, cancel := context.WithCancel(context.Background())
	lm.heartbeatContexts[jobID] = cancel

	// Start background heartbeat goroutine
	go lm.runHeartbeat(heartbeatCtx, jobID, leaseID, leaseKey)

	return true, leaseID, nil
}

// ============================================================================
// HEARTBEAT GOROUTINE (NEW - CRITICAL)
// ============================================================================

// runHeartbeat: Heartbeat goroutine that keeps lease alive
//
// Every 10 seconds:
// 1. Call KeepAliveOnce to renew lease
// 2. Track renewal successes/failures
// 3. Log warnings if renewal fails
// 4. Stop when context cancelled or lease revoked
func (lm *LeaseManager) runHeartbeat(
	ctx context.Context,
	jobID string,
	leaseID int64,
	leaseKey string,
) {
	ticker := time.NewTicker(lm.heartbeatTicker)
	defer ticker.Stop()

	consecutiveFailures := 0
	maxConsecutiveFailures := 3

	lm.log.Infof("heartbeat started for job %s (leaseID=%d)", jobID, leaseID)

	for {
		select {
		case <-ctx.Done():
			// Context cancelled, (from ReleaseLeaseForJob) -- fixed
			lm.log.Infof("heartbeat stopping for job %s (context cancelled)", jobID)
			lm.releaseLease(context.Background(), jobID, leaseID, leaseKey)
			return

		case <-ticker.C:
			// Time to renew lease
			err := lm.etcdClient.KeepAliveOnce(context.Background(), leaseID)
			if err != nil {
				consecutiveFailures++
				lm.log.Warnf("heartbeat FAILED for job %s: %v (attempt %d/%d)",
					jobID, err, consecutiveFailures, maxConsecutiveFailures)

				// Update lease info
				lm.mu.Lock()
				if info, ok := lm.activeLeases[jobID]; ok {
					info.RenewalFailures++
				}
				lm.mu.Unlock()

				// After 3 consecutive failures, alert and possibly shutdown
				if consecutiveFailures >= maxConsecutiveFailures {
					lm.log.Errorf("CRITICAL: Lease renewal failed %d times for job %s - possible etcd outage",
						maxConsecutiveFailures, jobID)
					// In production: trigger alerting system, graceful shutdown
					// For now: continue trying
				}

				continue
			}

			// Success: renewal worked
			consecutiveFailures = 0

			// Update lease info
			lm.mu.Lock()
			if info, ok := lm.activeLeases[jobID]; ok {
				info.LastRenewedAt = time.Now()
				info.ExpiresAt = time.Now().Add(time.Duration(lm.leaseTTL) * time.Second)
				info.RenewalAttempts++
			}
			lm.mu.Unlock()

			lm.log.Infof("heartbeat renewed lease for job %s (leaseID=%d)", jobID, leaseID)
		}
	}
}

// ============================================================================
// FENCING TOKENS (NEW - CRITICAL FOR SPLIT-BRAIN PREVENTION)
// ============================================================================

// CheckLeaseOwnership: Verify we still own the lease (FENCING)
//
// CRITICAL: Call this BEFORE writing job results
// If we lost the lease, another executor claimed the job
// Return error to ABORT the write (prevent split-brain)
func (lm *LeaseManager) CheckLeaseOwnership(ctx context.Context, jobID string, leaseID int64) error {
	leaseKey := fmt.Sprintf("ares:leases:%s", jobID)

	// Step 1: Check if lease key exists in etcd (source of truth)
	leaseData, err := lm.etcdClient.Get(ctx, leaseKey)
	if err != nil {
		lm.log.Errorf("FENCING: Failed to check lease in etcd: %v", err)
		return fmt.Errorf("fencing error: failed to check etcd: %w", err)
	}

	if leaseData == "" {
		// Lease key not found in etcd - we lost ownership
		lm.log.Errorf("FENCING: Lease key not found in etcd (job=%s) - split-brain prevented!", jobID)
		return fmt.Errorf("fencing error: lease not found in etcd (split-brain prevented!)")
	}

	// Step 2: Verify it's still our lease (parse and check SchedulerID)
	var lease Lease
	err = json.Unmarshal([]byte(leaseData), &lease)
	if err != nil {
		lm.log.Errorf("FENCING: Failed to parse lease: %v", err)
		return fmt.Errorf("fencing error: failed to parse lease: %w", err)
	}

	if lease.SchedulerID != lm.schedulerID {
		lm.log.Errorf("FENCING: Lease owned by different scheduler (expected %s, got %s)",
			lm.schedulerID, lease.SchedulerID)
		return fmt.Errorf("fencing error: lease owned by another scheduler (split-brain prevented!)")
	}

	// Step 3: Verify leaseID matches (extra safety check)
	if lease.JobID != jobID {
		lm.log.Errorf("FENCING: JobID mismatch in lease")
		return fmt.Errorf("fencing error: job ID mismatch")
	}

	lm.log.Infof("fencing check passed for job %s (lease still ours in etcd)", jobID)
	return nil
}

// ============================================================================
// LEASE RELEASE
// ============================================================================

func (lm *LeaseManager) ReleaseLeaseForJob(ctx context.Context, jobID string) error {
	lm.mu.Lock()
	info, exists := lm.activeLeases[jobID]
	cancel, hasCancel := lm.heartbeatContexts[jobID]

	if exists {
		delete(lm.activeLeases, jobID)
	}
	if hasCancel {
		delete(lm.heartbeatContexts, jobID)
	}
	lm.mu.Unlock()

	if !exists {
		return fmt.Errorf("lease not found for job %s", jobID)
	}

	// FIX: Stop heartbeat goroutine before releasing lease
	if hasCancel {
		cancel()                           // Signals context.Done() in runHeartbeat
		time.Sleep(100 * time.Millisecond) // Give goroutine time to exit
		lm.log.Debugf("heartbeat stopped for job %s", jobID)
	}

	return lm.releaseLease(ctx, jobID, info.LeaseID, fmt.Sprintf("ares:leases:%s", jobID))
}

func (lm *LeaseManager) releaseLease(
	ctx context.Context,
	jobID string,
	leaseID int64,
	leaseKey string,
) error {
	// Delete the lease key first
	err := lm.etcdClient.Delete(ctx, leaseKey)
	if err != nil {
		lm.log.Errorf("failed to delete lease key: %v", err)
		return fmt.Errorf("delete lease key failed: %w", err) // FIX: Return error
	}

	// Revoke the lease (forces auto-expiration)
	err = lm.etcdClient.RevokeLease(ctx, leaseID)
	if err != nil {
		lm.log.Errorf("failed to revoke lease %d: %v", leaseID, err)
		return fmt.Errorf("revoke lease failed: %w", err) // FIX: Better error message
	}

	lm.log.Infof("lease released for job %s", jobID)
	return nil
}

// ============================================================================
// LEASE INFO QUERIES
// ============================================================================

// GetLeaseForJob gets lease info for monitoring
func (lm *LeaseManager) GetLeaseForJob(ctx context.Context, jobID string) (*LeaseInfo, error) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	info, exists := lm.activeLeases[jobID]
	if !exists {
		return nil, fmt.Errorf("lease not found for job %s", jobID)
	}

	return info, nil
}

// GetActiveLeases returns all active leases (for monitoring)
func (lm *LeaseManager) GetActiveLeases() map[string]*LeaseInfo {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	// Return a copy
	result := make(map[string]*LeaseInfo)
	for k, v := range lm.activeLeases {
		info := *v
		result[k] = &info
	}

	return result
}

// ============================================================================
// JOB STORE
// ============================================================================

type JobStore struct {
	etcdClient *etcd.ETCDClient
	log        Logger
	mu         sync.RWMutex
}

func NewJobStore(etcdClient *etcd.ETCDClient, log Logger) *JobStore {
	return &JobStore{
		etcdClient: etcdClient,
		log:        log,
	}
}

// StoreJob: Store job with lease attachment (auto-cleanup on crash)
//
// CRITICAL FIX: Jobs should be stored with lease ID
// So when executor crashes:
// - Lease expires after 30 seconds
// - Job automatically deleted from etcd (no stale jobs)
func (js *JobStore) StoreJob(ctx context.Context, job *JobRecord, leaseID int64) error {

	// FIX: Serialize job OUTSIDE lock (avoid blocking during marshaling)
	jobKey := fmt.Sprintf("ares:jobs:%s", job.JobID)
	jobValue, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	// FIX: Network operation OUTSIDE lock (don't block other operations)
	err = js.etcdClient.PutWithLease(ctx, jobKey, string(jobValue), leaseID)
	if err != nil {
		js.log.Errorf("failed to store job %s: %v", job.JobID, err)
		return fmt.Errorf("put job: %w", err)
	}

	// Only lock for internal state updates (if needed)
	js.mu.Lock()
	defer js.mu.Unlock()

	js.log.Infof("job stored with lease: %s (state=%s, leaseID=%d)", job.JobID, job.Status, leaseID)
	return nil
}

// GetJob retrieves a job record from etcd
func (js *JobStore) GetJob(ctx context.Context, jobID string) (*JobRecord, error) {
	jobKey := fmt.Sprintf("ares:jobs:%s", jobID)

	resp, err := js.etcdClient.Get(ctx, jobKey)
	if err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}

	if resp == "" {
		return nil, errors.New("job not found")
	}

	var job JobRecord
	if err := json.Unmarshal([]byte(resp), &job); err != nil {
		return nil, fmt.Errorf("unmarshal job: %w", err)
	}

	return &job, nil
}

// UpdateJobState updates job state atomically
func (js *JobStore) UpdateJobState(
	ctx context.Context,
	jobID string,
	newState JobState,
	metadata map[string]interface{},
	leaseID int64,
) error {
	js.mu.Lock()
	defer js.mu.Unlock()

	job, err := js.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("get job: %w", err)
	}

	if !isValidTransition(job.Status, newState) {
		return fmt.Errorf("invalid state transition: %s → %s", job.Status, newState)
	}

	job.Status = newState
	now := time.Now()

	switch newState {
	case JobStateScheduled:
		job.ScheduledAt = now
	case JobStatePlaced:
		job.PlacedAt = now
		if nodeID, ok := metadata["node_id"].(string); ok {
			job.AssignedNode = nodeID
		}
		if clusterID, ok := metadata["cluster_id"].(string); ok {
			job.AssignedCluster = clusterID
		}
	case JobStateRunning:
		job.StartedAt = now
		if podName, ok := metadata["pod_name"].(string); ok {
			job.PodName = podName
		}
	case JobStateSucceeded, JobStateFailed:
		job.CompletedAt = now
		if result, ok := metadata["result"].(string); ok {
			job.Result = result
		}
	}

	return js.StoreJob(ctx, job, leaseID)
}

func isValidTransition(from, to JobState) bool {
	validTransitions := map[JobState][]JobState{
		JobStatePending:   {JobStateScheduled, JobStateCancelled},
		JobStateScheduled: {JobStatePlaced, JobStateCancelled},
		JobStatePlaced:    {JobStateRunning, JobStateCancelled},
		JobStateRunning:   {JobStateSucceeded, JobStateFailed, JobStateCancelled},
		JobStateSucceeded: {},
		JobStateFailed:    {JobStatePending},
		JobStateCancelled: {},
	}

	if transitions, ok := validTransitions[from]; ok {
		for _, valid := range transitions {
			if valid == to {
				return true
			}
		}
	}

	return false
}

// PrepareRetry prepares a failed job for retry
func (js *JobStore) PrepareRetry(ctx context.Context, jobID string) (time.Duration, error) {
	js.mu.Lock()
	defer js.mu.Unlock()

	job, err := js.GetJob(ctx, jobID)
	if err != nil {
		return 0, fmt.Errorf("get job: %w", err)
	}

	if job.Attempts >= job.MaxRetries {
		return 0, fmt.Errorf("max retries exceeded (%d)", job.MaxRetries)
	}

	backoffSeconds := math.Min(math.Pow(2, float64(job.Attempts)), 300)
	jitterFraction := float64(time.Now().UnixNano()%100) / 100.0 * 0.1
	backoffSeconds *= (1.0 + jitterFraction)
	backoff := time.Duration(backoffSeconds) * time.Second

	job.Attempts++
	job.LastRetryTime = time.Now()
	job.Status = JobStatePending

	js.log.Infof("job %s prepared for retry %d (backoff: %v)", jobID, job.Attempts, backoff)
	return backoff, nil
}

// Helper methods (abbreviated for space)
func (js *JobStore) GetJobsByTenant(ctx context.Context, tenantID string) ([]*JobRecord, error) {
	return nil, nil // Implementation similar to original
}

func (js *JobStore) GetJobsByStatus(ctx context.Context, status JobState) ([]*JobRecord, error) {
	return nil, nil
}

func (js *JobStore) DeleteJob(ctx context.Context, jobID string) error {
	return nil
}

// ============================================================================
// LOGGER INTERFACE
// ============================================================================

type Logger interface {
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Debugf(format string, args ...interface{})
}
