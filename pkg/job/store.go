package job

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/etcd"
	"sync"
	"time"
)

// Layer 3: Job Store - Persistence layer for jobs
// Stores jobs in etcd for durability and accessibility

// JobStore: Interface for job persistence
// Abstraction so we can swap implementations later
type JobStore interface {
	// SaveJob: Save or update a job
	SaveJob(ctx context.Context, job *common.Job, leaseId int64) error

	// GetJob: Retrieve a job by ID
	GetJob(ctx context.Context, jobID string) (*common.Job, error)

	// UpdateJobStatus: Update only the status of a job
	UpdateJobStatus(ctx context.Context, jobID string, status common.JobStatus,
		leaseID int64) error

	// DeleteJob: Delete a job
	DeleteJob(ctx context.Context, jobID string) error

	// ListJobs: List all jobs with optional filter
	ListJobs(ctx context.Context, filter *JobFilter) ([]*common.Job, error)

	// WatchJobs: Watch for job changes
	WatchJobs(ctx context.Context, jobID string) <-chan *common.Job

	// Close: Close the store
	Close() error
}

// JobFilter: Filter for ListJobs
type JobFilter struct {
	Status  *common.JobStatus // Filter by status
	Tenant  string            // Filter by tenant
	Cluster string            // Filter by cluster
	Limit   int               // Max results
}

// ============================================================================
// ETCD JOB STORE IMPLEMENTATION
// ============================================================================

// ETCDJobStore: Job store backed by etcd
type ETCDJobStore struct {
	etcd      *etcd.ETCDClient
	log       *logger.Logger
	keyPrefix string // "/ares/jobs"
	mu        sync.Mutex
}

// NewETCDJobStore: Create a new etcd-backed job store
func NewETCDJobStore(etcd *etcd.ETCDClient) *ETCDJobStore {
	return &ETCDJobStore{
		etcd:      etcd,
		log:       logger.Get(),
		keyPrefix: "/ares/jobs",
	}
}

// SaveJob: Save a job to etcd
//
// Stores as JSON:
// Key: /ares/jobs/{jobID}
// Value: {entire job as JSON}
func (store *ETCDJobStore) SaveJob(ctx context.Context, job *common.Job, leaseID int64) error {
	if job == nil {
		return fmt.Errorf("cannot save nil job")
	}

	if job.ID == "" {
		return fmt.Errorf("job ID cannot be empty")
	}

	store.log.Info("Job Data received: ", job)
	// Marshal job to JSON
	jobData, err := json.Marshal(job)

	if err != nil {
		store.log.Error("Failed to marshal job %s: %v", job.ID, err)
		return fmt.Errorf("marshal failed: %w", err)
	}

	// Store the job with lease for auto-cleanup
	key := fmt.Sprintf("%s/%s", store.keyPrefix, job.ID)

	// Use transaction with ModRevision check (optimistic locking)
	// Add mutex to  ETCDJobStore
	store.mu.Lock()
	defer store.mu.Unlock()

	err = store.etcd.PutWithLease(ctx, key, string(jobData), leaseID)
	if err != nil {
		store.log.Error("Failed to save job %s to etcd: %v", job.ID, err)
		return fmt.Errorf("save to etcd failed: %w", err)
	}

	store.log.Info("Saved job %s (status: %s)", job.ID, job.Status)
	return nil
}

// GetJob: Retrieve a job from etcd by ID
func (store *ETCDJobStore) GetJob(ctx context.Context, jobID string) (*common.Job, error) {

	store.log.Debug("Entered Get Job Method()")
	store.log.Debug("Job ID received: ", jobID)
	if jobID == "" {
		return nil, fmt.Errorf("job ID cannot be empty")
	}

	key := fmt.Sprintf("%s/%s", store.keyPrefix, jobID)
	store.log.Debug("This is the key: %v with store prefix ", key, store.keyPrefix)
	jobData, err := store.etcd.Get(ctx, key)
	store.log.Debug("Job Found: %v", jobData)
	if err != nil {
		store.log.Error("Failed to get job %s from etcd: %v", jobID, err)
		return nil, fmt.Errorf("get from etcd failed: %w", err)
	}

	if jobData == "" {
		// Job not found
		store.log.Debug("Job not found: %s", jobID)
		return nil, nil
	}

	// Unmarshal JSON to job
	job := &common.Job{}
	err = json.Unmarshal([]byte(jobData), job)
	if err != nil {
		store.log.Error("Failed to unmarshal job %s: %v", jobID, err)
		return nil, fmt.Errorf("unmarshal failed: %w", err)
	}

	store.log.Debug("Retrieved job %s (status: %s)", jobID, job.Status)
	return job, nil
}

// UpdateJobStatus: Update only the status of a job
//
// Reads current job, updates status, writes back
// This is NOT atomic, but sufficient for most use cases
func (store *ETCDJobStore) UpdateJobStatus(
	ctx context.Context,
	jobID string,
	status common.JobStatus,
	leaseID int64,
) error {

	// Get current job
	job, err := store.GetJob(ctx, jobID)
	if err != nil {
		return err
	}

	if job == nil {
		return fmt.Errorf("job not found: %s", jobID)
	}

	// Update status
	job.Status = status

	// Save back
	return store.SaveJob(ctx, job, leaseID)
}

// DeleteJob: Delete a job from etcd
func (store *ETCDJobStore) DeleteJob(ctx context.Context, jobID string) error {
	if jobID == "" {
		return fmt.Errorf("job ID cannot be empty")
	}

	key := fmt.Sprintf("%s/%s", store.keyPrefix, jobID)
	err := store.etcd.Delete(ctx, key)
	if err != nil {
		store.log.Error("Failed to delete job %s from etcd: %v", jobID, err)
		return fmt.Errorf("delete from etcd failed: %w", err)
	}

	store.log.Debug("Deleted job %s", jobID)
	return nil
}

// ListJobs: List all jobs with optional filtering
//
// Gets all jobs with prefix, applies filter
func (store *ETCDJobStore) ListJobs(
	ctx context.Context,
	filter *JobFilter,
) ([]*common.Job, error) {

	// Get all jobs with prefix
	jobMap, err := store.etcd.GetAll(ctx, store.keyPrefix)
	if err != nil {
		store.log.Error("Failed to list jobs: %v", err)
		return nil, fmt.Errorf("list from etcd failed: %w", err)
	}

	jobs := make([]*common.Job, 0, len(jobMap))

	// Parse each job and apply filter
	for _, jobData := range jobMap {
		job := &common.Job{}
		err := json.Unmarshal([]byte(jobData), job)
		if err != nil {
			store.log.Warn("Failed to parse job: %v", err)
			continue // Skip malformed jobs
		}

		// Apply filter
		if filter != nil {
			// Filter by status
			if filter.Status != nil && job.Status != *filter.Status {
				continue
			}

			// Filter by tenant
			if filter.Tenant != "" && job.Spec.TenantID != filter.Tenant {
				continue
			}

			// Filter by cluster
			if filter.Cluster != "" && job.ClusterID != filter.Cluster {
				continue
			}
		}

		jobs = append(jobs, job)

		// Check limit
		if filter != nil && filter.Limit > 0 && len(jobs) >= filter.Limit {
			break
		}
	}

	store.log.Debug("Listed %d jobs", len(jobs))
	return jobs, nil
}

// WatchJobs: Watch for changes to a specific job
//
// Returns a channel that receives job updates
// Uses etcd watch internally
func (store *ETCDJobStore) WatchJobs(ctx context.Context, jobID string) <-chan *common.Job {
	jobChan := make(chan *common.Job)

	go func() {
		defer close(jobChan)

		key := fmt.Sprintf("%s/%s", store.keyPrefix, jobID)
		watchChan := store.etcd.Watch(ctx, key)

		for {
			select {
			case <-ctx.Done():
				store.log.Debug("Stopped watching job %s", jobID)
				return

			case resp, ok := <-watchChan:
				if !ok {
					store.log.Debug("Watch channel closed for job %s", jobID)
					return
				}

				// Process watch response
				for _, event := range resp.Events {
					jobData := string(event.Kv.Value)

					job := &common.Job{}
					err := json.Unmarshal([]byte(jobData), job)
					if err != nil {
						store.log.Warn("Failed to parse watched job: %v", err)
						continue
					}

					store.log.Debug("Watched job %s changed (status: %s)", job.ID, job.Status)
					jobChan <- job
				}
			}
		}
	}()

	return jobChan
}

// Close: Close the store (no-op for etcd)
func (store *ETCDJobStore) Close() error {
	return nil
}

// ============================================================================
// JOB QUERY HELPERS
// ============================================================================

// GetJobsByStatus: Convenience method to get all jobs with a specific status
func (store *ETCDJobStore) GetJobsByStatus(
	ctx context.Context,
	status common.JobStatus,
) ([]*common.Job, error) {

	return store.ListJobs(ctx, &JobFilter{
		Status: &status,
	})
}

// GetJobsByCluster: Convenience method to get all jobs on a cluster
func (store *ETCDJobStore) GetJobsByCluster(
	ctx context.Context,
	clusterID string,
) ([]*common.Job, error) {

	return store.ListJobs(ctx, &JobFilter{
		Cluster: clusterID,
	})
}

// GetJobsByTenant: Convenience method to get all jobs for a tenant
func (store *ETCDJobStore) GetJobsByTenant(
	ctx context.Context,
	tenantID string,
) ([]*common.Job, error) {

	return store.ListJobs(ctx, &JobFilter{
		Tenant: tenantID,
	})
}

// CountJobsByStatus: Count jobs with a specific status
func (store *ETCDJobStore) CountJobsByStatus(
	ctx context.Context,
	status common.JobStatus,
) (int, error) {

	jobs, err := store.GetJobsByStatus(ctx, status)
	if err != nil {
		return 0, err
	}

	return len(jobs), nil
}

func (store *ETCDJobStore) CleanupOldJobs(ctx context.Context, olderThan time.Duration) error {
	filter := &JobFilter{} // Get all
	jobs, err := store.ListJobs(ctx, filter)
	if err != nil {
		return err
	}

	cutoff := time.Now().Add(-olderThan)
	for _, job := range jobs {
		if job.Status == common.StatusSucceeded || job.Status == common.StatusFailed {
			if job.EndTime.Before(cutoff) {
				store.DeleteJob(ctx, job.ID)
			}
		}
	}
	return nil
}

// ============================================================================
// BULK OPERATIONS
// ============================================================================

// need to think about it!
// SaveJobs: Save multiple jobs at once
//func (store *ETCDJobStore) SaveJobs(ctx context.Context, jobs []*common.Job) error {
//	for _, job := range jobs {
//		if err := store.SaveJob(ctx, job); err != nil {
//			return err
//		}
//	}
//	store.log.Debug("Saved %d jobs", len(jobs))
//	return nil
//}

// DeleteJobs: Delete multiple jobs at once
func (store *ETCDJobStore) DeleteJobs(ctx context.Context, jobIDs []string) error {
	for _, jobID := range jobIDs {
		if err := store.DeleteJob(ctx, jobID); err != nil {
			return err
		}
	}
	store.log.Debug("Deleted %d jobs", len(jobIDs))
	return nil
}

// ============================================================================
// STATS AND MONITORING
// ============================================================================

// GetJobStats: Get statistics about jobs
type JobStats struct {
	TotalJobs   int
	PendingJobs int
	RunningJobs int
	SuccessJobs int
	FailedJobs  int
	AvgDuration time.Duration
}

// GetStats: Calculate statistics about jobs in the system
func (store *ETCDJobStore) GetStats(ctx context.Context) (*JobStats, error) {
	// Get all jobs
	allJobs, err := store.ListJobs(ctx, nil)
	if err != nil {
		return nil, err
	}

	stats := &JobStats{
		TotalJobs: len(allJobs),
	}

	totalDuration := time.Duration(0)
	completedCount := 0

	for _, job := range allJobs {
		switch job.Status {
		case common.StatusPending:
			stats.PendingJobs++
		case common.StatusRunning:
			stats.RunningJobs++
		case common.StatusSucceeded:
			stats.SuccessJobs++
			if !job.EndTime.IsZero() && !job.StartTime.IsZero() {
				totalDuration += job.EndTime.Sub(job.StartTime)
				completedCount++
			}
		case common.StatusFailed:
			stats.FailedJobs++
		}
	}

	if completedCount > 0 {
		stats.AvgDuration = totalDuration / time.Duration(completedCount)
	}

	return stats, nil
}
