// ============================================================================
// LAYER 3: LEASE MANAGER & JOB STORE
// ============================================================================
// Features: #19 (Distributed Locking / Lease System) + #17-18 (Job Lifecycle)
// Responsibilities:
//   1. Distributed leases via etcd (for exactly-once semantics)
//   2. Job state management (PENDING → RUNNING → SUCCEEDED/FAILED)
//   3. Request deduplication (via request IDs)
//   4. Retry logic with exponential backoff
// ============================================================================

package lease

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// ============================================================================
// TYPES & CONSTANTS
// ============================================================================

// JobState represents the state of a job in the scheduling pipeline
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

// JobRecord is the complete state of a job stored in etcd
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

// Lease represents a distributed lease in etcd
type Lease struct {
	JobID       string    `json:"job_id"`
	SchedulerID string    `json:"scheduler_id"`
	AcquiredAt  time.Time `json:"acquired_at"`
	ExpiresAt   time.Time `json:"expires_at"`
	TTL         int64     `json:"ttl"`
}

// LeaseManager handles distributed leases for exactly-once semantics
type LeaseManager struct {
	etcdClient  *clientv3.Client
	schedulerID string
	leaseTTL    int64
	log         Logger
	mu          sync.RWMutex
}

// JobStore handles persistent job state in etcd
type JobStore struct {
	etcdClient *clientv3.Client
	log        Logger
	mu         sync.RWMutex
}

// ============================================================================
// LEASE MANAGER IMPLEMENTATION
// ============================================================================

// NewLeaseManager creates a new lease manager
// schedulerID: unique identifier for this scheduler instance
// leaseTTL: how long a lease lasts (default 30 seconds)
func NewLeaseManager(etcdClient *clientv3.Client, schedulerID string, log Logger) *LeaseManager {
	return &LeaseManager{
		etcdClient:  etcdClient,
		schedulerID: schedulerID,
		leaseTTL:    30, // 30 second lease TTL
		log:         log,
	}
}

// AcquireLeaseForJob attempts to acquire a distributed lease for a job
//
// CRITICAL: Uses etcd Txn (compare-and-set) which is ATOMIC.
// Only ONE scheduler succeeds, even if 1000 try simultaneously.
//
// Returns:
//   - (true, nil) if lease acquired by this scheduler
//   - (false, nil) if lease already held by another scheduler
//   - (false, err) on error
//
// Algorithm:
// 1. Create etcd lease with 30-second TTL
// 2. Atomically check: does "ares:leases:job-123" exist?
// 3. If NO: Create it with this scheduler's ID and lease ID
// 4. If YES: Another scheduler holds it, return false
//
// RACE CONDITION SAFETY:
// etcd Txn is atomic - either completely succeeds or completely fails.
// Between check and set, etcd prevents other schedulers from inserting.
func (lm *LeaseManager) AcquireLeaseForJob(ctx context.Context, jobID string) (bool, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Step 1: Create etcd lease with TTL
	lease, err := lm.etcdClient.Lease.Grant(ctx, lm.leaseTTL)
	if err != nil {
		lm.log.Errorf("failed to grant lease: %v", err)
		return false, fmt.Errorf("grant lease: %w", err)
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

	// Step 3: ATOMIC transaction
	// If key doesn't exist (CreateRevision == 0), create it
	// Otherwise, abort the transaction
	txnResponse, err := lm.etcdClient.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(leaseKey), "=", 0)).
		Then(clientv3.OpPut(leaseKey, string(leaseValue), clientv3.WithLease(lease.ID))).
		Else(clientv3.OpGet(leaseKey)).
		Commit()

	if err != nil {
		lm.log.Errorf("txn failed for lease: %v", err)
		return false, fmt.Errorf("txn failed: %w", err)
	}

	if txnResponse.Succeeded {
		lm.log.Infof("lease acquired for job %s", jobID)
		return true, nil
	}

	// Another scheduler holds the lease
	lm.log.Warnf("lease already held for job %s (by another scheduler)", jobID)
	return false, nil
}

// RenewLeaseForJob renews an existing lease (called periodically)
// This prevents the lease from expiring while we're still executing
func (lm *LeaseManager) RenewLeaseForJob(ctx context.Context, jobID string) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	leaseKey := fmt.Sprintf("ares:leases:%s", jobID)

	// Get current lease
	resp, err := lm.etcdClient.Get(ctx, leaseKey)
	if err != nil {
		return fmt.Errorf("get lease: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return errors.New("lease not found")
	}

	// Parse existing lease
	var lease Lease
	if err := json.Unmarshal(resp.Kvs[0].Value, &lease); err != nil {
		return fmt.Errorf("unmarshal lease: %w", err)
	}

	// Verify we're the owner
	if lease.SchedulerID != lm.schedulerID {
		return errors.New("lease owned by different scheduler")
	}

	// Create new lease with extended TTL
	newLease, err := lm.etcdClient.Lease.Grant(ctx, lm.leaseTTL)
	if err != nil {
		return fmt.Errorf("grant new lease: %w", err)
	}

	// Update with new lease ID (old lease will auto-expire)
	lease.ExpiresAt = time.Now().Add(time.Duration(lm.leaseTTL) * time.Second)
	leaseValue, _ := json.Marshal(lease)

	_, err = lm.etcdClient.Put(ctx, leaseKey, string(leaseValue), clientv3.WithLease(newLease.ID))
	return err
}

// ReleaseLeaseForJob releases a lease (when job completes or times out)
func (lm *LeaseManager) ReleaseLeaseForJob(ctx context.Context, jobID string) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	leaseKey := fmt.Sprintf("ares:leases:%s", jobID)

	// Get current lease
	resp, err := lm.etcdClient.Get(ctx, leaseKey)
	if err != nil {
		return fmt.Errorf("get lease: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil // Already released
	}

	// Parse and verify ownership
	var lease Lease
	if err := json.Unmarshal(resp.Kvs[0].Value, &lease); err != nil {
		return fmt.Errorf("unmarshal lease: %w", err)
	}

	if lease.SchedulerID != lm.schedulerID {
		return errors.New("lease owned by different scheduler")
	}

	// Delete the lease key
	_, err = lm.etcdClient.Delete(ctx, leaseKey)
	if err != nil {
		return fmt.Errorf("delete lease: %w", err)
	}

	lm.log.Infof("lease released for job %s", jobID)
	return nil
}

// GetLeaseForJob gets the current lease holder for a job (for monitoring)
func (lm *LeaseManager) GetLeaseForJob(ctx context.Context, jobID string) (*Lease, error) {
	leaseKey := fmt.Sprintf("ares:leases:%s", jobID)

	resp, err := lm.etcdClient.Get(ctx, leaseKey)
	if err != nil {
		return nil, fmt.Errorf("get lease: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, errors.New("lease not found")
	}

	var lease Lease
	if err := json.Unmarshal(resp.Kvs[0].Value, &lease); err != nil {
		return nil, fmt.Errorf("unmarshal lease: %w", err)
	}

	return &lease, nil
}

// ============================================================================
// JOB STORE IMPLEMENTATION
// ============================================================================

// NewJobStore creates a new job store
func NewJobStore(etcdClient *clientv3.Client, log Logger) *JobStore {
	return &JobStore{
		etcdClient: etcdClient,
		log:        log,
	}
}

// StoreJob stores a job record in etcd
// This is called when a job is first submitted
func (js *JobStore) StoreJob(ctx context.Context, job *JobRecord) error {
	js.mu.Lock()
	defer js.mu.Unlock()

	jobKey := fmt.Sprintf("ares:jobs:%s", job.JobID)
	jobValue, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	_, err = js.etcdClient.Put(ctx, jobKey, string(jobValue))
	if err != nil {
		js.log.Errorf("failed to store job %s: %v", job.JobID, err)
		return fmt.Errorf("put job: %w", err)
	}

	js.log.Infof("job stored: %s (state=%s)", job.JobID, job.Status)
	return nil
}

// GetJob retrieves a job record from etcd
func (js *JobStore) GetJob(ctx context.Context, jobID string) (*JobRecord, error) {
	jobKey := fmt.Sprintf("ares:jobs:%s", jobID)

	resp, err := js.etcdClient.Get(ctx, jobKey)
	if err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, errors.New("job not found")
	}

	var job JobRecord
	if err := json.Unmarshal(resp.Kvs[0].Value, &job); err != nil {
		return nil, fmt.Errorf("unmarshal job: %w", err)
	}

	return &job, nil
}

// UpdateJobState updates a job's state atomically
// This ensures state transitions are valid and durable
//
// Valid transitions:
// PENDING → SCHEDULED (global scheduler routed it)
// SCHEDULED → PLACED (local scheduler placed it)
// PLACED → RUNNING (pod created)
// RUNNING → SUCCEEDED/FAILED (pod exited)
// Any state → CANCELLED (user cancelled)
func (js *JobStore) UpdateJobState(ctx context.Context, jobID string, newState JobState, metadata map[string]interface{}) error {
	js.mu.Lock()
	defer js.mu.Unlock()

	// Fetch current job
	job, err := js.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("get job: %w", err)
	}

	// Validate state transition
	if !isValidTransition(job.Status, newState) {
		return fmt.Errorf("invalid state transition: %s → %s", job.Status, newState)
	}

	// Update fields based on new state
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

	// Store updated job
	return js.StoreJob(ctx, job)
}

// isValidTransition checks if a state transition is allowed
func isValidTransition(from, to JobState) bool {
	validTransitions := map[JobState][]JobState{
		JobStatePending:   {JobStateScheduled, JobStateCancelled},
		JobStateScheduled: {JobStatePlaced, JobStateCancelled},
		JobStatePlaced:    {JobStateRunning, JobStateCancelled},
		JobStateRunning:   {JobStateSucceeded, JobStateFailed, JobStateCancelled},
		JobStateSucceeded: {},
		JobStateFailed:    {JobStatePending}, // Retry: go back to pending
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

// PrepareRetry prepares a failed job for retry with exponential backoff
//
// Algorithm:
// 1. Calculate backoff: delay = min(2^attempt, 5 minutes) + jitter
// 2. Increment attempt counter
// 3. Set job status back to PENDING
// 4. Store updated job
//
// Returns: (backoff duration, error)
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

	// Calculate exponential backoff: 2^attempt seconds, capped at 5 minutes
	backoffSeconds := math.Min(math.Pow(2, float64(job.Attempts)), 300)

	// Add jitter (0-10% random variation)
	jitterFraction := float64(time.Now().UnixNano()%100) / 100.0 * 0.1
	backoffSeconds *= (1.0 + jitterFraction)

	backoff := time.Duration(backoffSeconds) * time.Second

	// Update job for retry
	job.Attempts++
	job.LastRetryTime = time.Now()
	job.Status = JobStatePending

	if err := js.StoreJob(ctx, job); err != nil {
		return 0, fmt.Errorf("store job: %w", err)
	}

	js.log.Infof("job %s prepared for retry %d (backoff: %v)", jobID, job.Attempts, backoff)
	return backoff, nil
}

// GetJobsByTenant retrieves all jobs for a tenant
func (js *JobStore) GetJobsByTenant(ctx context.Context, tenantID string) ([]*JobRecord, error) {
	prefix := "ares:jobs:"

	resp, err := js.etcdClient.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("get jobs: %w", err)
	}

	var jobs []*JobRecord
	for _, kv := range resp.Kvs {
		var job JobRecord
		if err := json.Unmarshal(kv.Value, &job); err != nil {
			js.log.Warnf("failed to unmarshal job: %v", err)
			continue
		}

		if job.TenantID == tenantID {
			jobs = append(jobs, &job)
		}
	}

	return jobs, nil
}

// GetJobsByStatus retrieves all jobs with a given status
func (js *JobStore) GetJobsByStatus(ctx context.Context, status JobState) ([]*JobRecord, error) {
	prefix := "ares:jobs:"

	resp, err := js.etcdClient.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("get jobs: %w", err)
	}

	var jobs []*JobRecord
	for _, kv := range resp.Kvs {
		var job JobRecord
		if err := json.Unmarshal(kv.Value, &job); err != nil {
			js.log.Warnf("failed to unmarshal job: %v", err)
			continue
		}

		if job.Status == status {
			jobs = append(jobs, &job)
		}
	}

	return jobs, nil
}

// DeleteJob deletes a job (for cleanup after completion)
func (js *JobStore) DeleteJob(ctx context.Context, jobID string) error {
	js.mu.Lock()
	defer js.mu.Unlock()

	jobKey := fmt.Sprintf("ares:jobs:%s", jobID)
	_, err := js.etcdClient.Delete(ctx, jobKey)
	if err != nil {
		return fmt.Errorf("delete job: %w", err)
	}

	return nil
}

// ============================================================================
// REQUEST DEDUPLICATION (Feature #18: Idempotent Job Submission)
// ============================================================================

// RequestDeduplicator handles idempotent job submission using request IDs
// If client submits same jobID twice, we return the cached result
type RequestDeduplicator struct {
	etcdClient *clientv3.Client
	log        Logger
	mu         sync.RWMutex
}

// NewRequestDeduplicator creates a new request deduplicator
func NewRequestDeduplicator(etcdClient *clientv3.Client, log Logger) *RequestDeduplicator {
	return &RequestDeduplicator{
		etcdClient: etcdClient,
		log:        log,
	}
}

// CacheResult stores the result of a job submission for deduplication
// If same jobID is submitted again, we return this cached result
func (rd *RequestDeduplicator) CacheResult(ctx context.Context, jobID string, result interface{}) error {
	rd.mu.Lock()
	defer rd.mu.Unlock()

	cacheKey := fmt.Sprintf("ares:results:%s", jobID)
	cacheValue, _ := json.Marshal(result)

	// Store with 7-day TTL so old results eventually expire
	resp, err := rd.etcdClient.Grant(ctx, 7*24*60*60)
	if err != nil {
		return fmt.Errorf("grant lease: %w", err)
	}

	_, err = rd.etcdClient.Put(ctx, cacheKey, string(cacheValue), clientv3.WithLease(resp.ID))
	return err
}

// GetCachedResult retrieves cached result if it exists
// Returns (result, true) if found, (nil, false) if not found, (nil, err) on error
func (rd *RequestDeduplicator) GetCachedResult(ctx context.Context, jobID string) (interface{}, bool, error) {
	cacheKey := fmt.Sprintf("ares:results:%s", jobID)

	resp, err := rd.etcdClient.Get(ctx, cacheKey)
	if err != nil {
		return nil, false, fmt.Errorf("get cache: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, false, nil // Not cached
	}

	var result interface{}
	if err := json.Unmarshal(resp.Kvs[0].Value, &result); err != nil {
		return nil, false, fmt.Errorf("unmarshal result: %w", err)
	}

	rd.log.Infof("returning cached result for job %s", jobID)
	return result, true, nil
}

// ============================================================================
// LOGGER INTERFACE (for dependency injection)
// ============================================================================

type Logger interface {
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}
