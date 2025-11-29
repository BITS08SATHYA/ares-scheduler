package scheduler

import (
	"context"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage"
	"time"
)

// Feature #17: Job LifeCycle Management
// Pattern: etcd for state transistions + Redis for job Queues

// Job State Machine

// Job State represents a job state
type JobState string

const (
	StateSubmitted JobState = "submitted" // Just Submitted
	StatePending   JobState = "pending"   // Waiting for resources
	StateRouted    JobState = "routed"    // Routed to a cluster
	StateScheduled JobState = "scheduled" // Scheduled on node
	StateRunning   JobState = "running"   // Currently executing
	StateSucceeded JobState = "succeeded" // Completed Successfully
	StateFailed    JobState = "failed"    // Job Failed
	StateCancelled JobState = "cancelled" // Cancelled by user
)

// Job represents a job in the system
type Job struct {
	ID          string                 `json:"id"` // Unique Job id
	RequestID   string                 `json:"request_id"`
	SubmittedAt time.Time              `json:"submitted_at"`
	SubmittedBy string                 `json:"submitted_by"`
	State       JobState               `json:"state"`
	RoutedTo    string                 `json:"routed_to"`
	ScheduledOn string                 `json:"scheduled_on"`
	StartedAt   time.Time              `json:"started_at"`
	CompletedAt time.Time              `json:"completed_at"`
	Result      JobResult              `json:"result"`
	Metadata    map[string]interface{} `json:"metadata"` // custom data
	RetryCount  int                    `json:"retry_count"`
	LastError   string                 `json:"last_error"`
}

// JobResult represents job execution result
type JobResult struct {
	Status   string `json:"status"` // "success" or "error"
	ExitCode int    `json:"exit_code"`
	Output   string `json:"output"`
	Error    string `json:"error"`
}

// Job Lifecycle Manager
type LifecycleManager struct {
	storage *storage.Client
}

// NewLifecycleManager creates a new lifecycle manager
func NewLifecycleManager(st *storage.Client) *LifecycleManager {
	return &LifecycleManager{
		storage: st,
	}
}

// State Transitions
// SubmitJob creates a new Job (submitted + pending)
func (lm *LifecycleManager) SubmitJob(ctx context.Context, jobID string, requestID string,
	submittedBy string, metadata map[string]interface{}) (*Job, error) {
	job := &Job{
		ID:          jobID,
		RequestID:   requestID,
		SubmittedAt: time.Now(),
		SubmittedBy: submittedBy,
		State:       StateSubmitted,
		Metadata:    metadata,
		RetryCount:  0,
	}

	// Write to etcd (source of truth for job state)
	if err := lm.storeJobState(ctx, job); err != nil {
		return nil, fmt.Errorf("failed to store job: %w", err)
	}

	return job, nil
}

// TransitionToPending moves job  from submitted --> pending
func (lm *LifecycleManager) TransitionToPending(ctx context.Context, jobID string) error {
	return lm.transitionState(ctx, jobID, StateSubmitted, StatePending)
}

// TransitionToRouted moves job to routed state with cluster assignment
func (lm *LifecycleManager) TransitionToRouted(ctx context.Context, jobID string, clusterName string) error {
	job, err := lm.getJob(ctx, jobID)
	if err != nil {
		return err
	}

	if job.State != StatePending {
		return fmt.Errorf("cannot transition from %s to routed", job.State)
	}

	job.State = StateRouted
	job.RoutedTo = clusterName

	//	Write to etcd
	if err := lm.storeJobState(ctx, job); err != nil {
		return err
	}

	//	 Push to cluster's queue in redis
	queueName := fmt.Sprintf("queue:%s", clusterName)
	return lm.storage.PushQueue(ctx, queueName, jobID)
}

// TransitionToScheduled moves job from routed --> scheduled
func (lm *LifecycleManager) TransitionToScheduled(ctx context.Context, jobID string, nodeName string) error {
	job, err := lm.getJob(ctx, jobID)
	if err != nil {
		return err
	}

	job.State = StateScheduled
	job.ScheduledOn = nodeName

	return lm.storeJobState(ctx, job)
}

// TransitionToRunning moves job from scheduled to running
func (lm *LifecycleManager) TransitionToRunning(ctx context.Context, jobID string) error {
	job, err := lm.getJob(ctx, jobID)
	if err != nil {
		return err
	}

	job.State = StateRunning
	job.StartedAt = time.Now()

	return lm.storeJobState(ctx, job)
}

// TransitionToSucceeded moves job -> succeeded with result
func (lm *LifecycleManager) TransitionToSucceeded(ctx context.Context, jobID string, result JobResult) error {
	job, err := lm.getJob(ctx, jobID)
	if err != nil {
		return err
	}
	job.State = StateSucceeded
	job.CompletedAt = time.Now()
	job.Result = result

	//	Write to etcd (permanent record)
	if err := lm.storeJobState(ctx, job); err != nil {
		return err
	}

	//	Clean up redis queue entry
	err1 := lm.removeFromQueue(ctx, job.RoutedTo, jobID)
	if err1 != nil {
		return err1
	}

	return nil
}

// TransitionFailed moves job -> failed with error
func (lm *LifecycleManager) TransitionToFailed(ctx context.Context, jobID string, errorMsg string) error {
	job, err := lm.getJob(ctx, jobID)
	if err != nil {
		return err
	}

	job.State = StateFailed
	job.CompletedAt = time.Now()
	job.LastError = errorMsg
	job.Result = JobResult{
		Status: "error",
		Error:  errorMsg,
	}

	//	Write to etcd
	if err := lm.storeJobState(ctx, job); err != nil {
		return err
	}

	//	Clean up redis queue
	lm.removeFromQueue(ctx, job.RoutedTo, jobID)

	return nil

}

// TransitionToCancelled cancels a job
func (lm *LifecycleManager) TransitionToCancelled(
	ctx context.Context,
	jobID string,
) error {
	job, err := lm.getJob(ctx, jobID)
	if err != nil {
		return err
	}

	// Only cancel if not already finished
	if job.State == StateSucceeded || job.State == StateFailed {
		return fmt.Errorf("cannot cancel finished job")
	}

	job.State = StateCancelled
	job.CompletedAt = time.Now()

	// Write to etcd
	if err := lm.storeJobState(ctx, job); err != nil {
		return err
	}

	// Clean up Redis queue
	lm.removeFromQueue(ctx, job.RoutedTo, jobID)

	return nil
}

/*
	Query Operations
// GetJob Retrieves current job state
// Fast Path: Check Redis Cache first
// Slow Path: Query Etcd if cache miss
*/

func (lm *LifecycleManager) GetJob(ctx context.Context, jobID string) (*Job, error) {
	/*
		Strategy:
		1. Try Redis Cache (hot data)
		2. If miss: Query etcd
		3. update cache
	*/
	cacheKey := fmt.Sprintf("job:%s", jobID)

	//	Fast path: Redis cache
	var job Job
	if err := lm.storage.GetRedis(ctx, cacheKey, &job); err != nil {
		return &job, nil
	}

	// slow path: etcd (source of truth)
	return lm.getJob(ctx, jobID)
}

// getjob is internal method that queries etcd
func (lm *LifecycleManager) getJob(ctx context.Context, jobID string) (*Job, error) {
	etcdKey := fmt.Sprintf("/ares/jobs/%s", jobID)
	var job Job
	err := lm.storage.GetEtcd(ctx, etcdKey, &job)
	return &job, err
}

// ListJobsByState returns all jobs in a given state
func (lm *LifecycleManager) ListJobsByState(ctx context.Context, state JobState) ([]*Job, error) {
	// Query etcd for all jobs matching prefix
	prefix := "/ares/jobs/"
	jobsMap, err := lm.storage.GetEtcdWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}

	var result []*Job
	for _, jobJSON := range jobsMap {
		var job Job
		if err := unmarshialJSON(jobJSON, &job); err != nil {
			continue
		}
		if job.State == state {
			result = append(result, &job)
		}
	}
	return result, nil
}

/*
	Storage Operations
*/
// StoreJobState writes job to both etcd and redis
func (lm *LifecycleManager) storeJobState(ctx context.Context, job *Job) error {
	//	Write to etcd (source of truth, permanent)
	etcdKey := fmt.Sprintf("/ares/jobs/%s", job.ID)
	if err := lm.storage.PutEtcd(ctx, etcdKey, job); err != nil {
		return fmt.Errorf("failed to write to etcd: %w", err)
	}

	//	Write to redis cache (for fast lookups)
	// jobs in running/failed state: cache for 1 hour
	// jobs in completed state: cache for 24 hours
	var ttl time.Duration
	if job.State == StateSucceeded || job.State == StateFailed {
		ttl = 24 * time.Hour
	} else {
		ttl = 1 * time.Hour
	}

	cacheKey := fmt.Sprintf("job:%s", job.ID)
	if err := lm.storage.setRedis(ctx, cacheKey, ttl); err != nil {
		//	Non-Critical: we wrote to etcd, just missing cache
		fmt.Printf("WARNING: Failed to cache job %s in Redis: %v\n", job.ID, err)
	}

	return nil

}

// Queue Operations
// removeFromQueue removes job from cluster Queue
func (lm *LifecycleManager) removeFromQueue(ctx context.Context, clusterName string, jobID string) error {
	if clusterName == "" {
		return nil // Not routed to any cluster
	}

	//	In redis, we store job ids in queue:{cluster}
	// To remove, we'd need to reimplemented the queue storage
	// For now, we just mark it in a "processed" set

	processedKey := fmt.Sprintf("processed:%s", clusterName)
	return lm.storage.redis.SAdd(ctx, processedKey, jobID).Err()
}

// Monitoring & Metrics

// GetJobState returns statistics about jobs
func (lm *LifecycleManager) GetJobState(ctx context.Context) (map[string]int64, error) {
	stats := make(map[string]int64)

	//	Count jobs by state
	for _, state := range []JobState{
		StateSubmitted, StatePending, StateRouted, StateScheduled, StateRunning, StateSucceeded, StateFailed,
	} {
		jobs, err := lm.ListJobsByState(ctx, state)
		if err != nil {
			continue
		}
		stats[string(state)] = int64(len(jobs))
	}
	return stats, nil
}

// GetRunningJobsCount returns count of running jobs
func (lm *LifecycleManager) GetRunningJobsCount(ctx context.Context) (int64, error) {
	jobs, err := lm.ListJobsByState(ctx, StateRunning)
	if err != nil {
		return 0, err
	}
	return int64(len(jobs)), nil
}

// HELPER: JSON Marshalling
func unmarshialJSON(jsonStr string, v interface{}) error {
	//	In real code, use encoding/json
	//	This is a placeholder
	return fmt.Errorf("implement json unmarshalling")
}
