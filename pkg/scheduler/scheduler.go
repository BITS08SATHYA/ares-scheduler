package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/gpu"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/job/to_delete"
	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"time"
)

// Scheduler orchestrates job submission and execution
type Scheduler struct {
	etcd *clientv3.Client
}

// NewScheduler creates a new scheduler instance
func NewScheduler(etcdEndpoints []string) (*Scheduler, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		return nil, fmt.Errorf("Failed to connect to etcd: %w", err)
	}

	return &Scheduler{
		etcd: cli,
	}, nil
}

// Close the etcd client connection
func (s *Scheduler) Close() error {
	return s.etcd.Close()
}

// SubmitJob submits a new Job with idempotency guarantee and GPU-aware scheduling
func (s *Scheduler) SubmitJob(ctx context.Context, req to_delete.JobSubmission) (string, error) {
	log.Printf("Submitting Job: %s (RequestID: %s)", req.Name, req.RequestID)

	// Step 1: Check if this request was already processed (idempotency)
	// checking deduplication
	requestKey := fmt.Sprintf("/requests/%s", req.RequestID)
	getResp, err := s.etcd.Get(ctx, requestKey)

	if err != nil {
		return "", fmt.Errorf("Failed to check request: %w", err)
	}

	//	If request already exists, return existing job ID
	if len(getResp.Kvs) > 0 {
		existingJobID := string(getResp.Kvs[0].Value)
		log.Printf("Request %s already processed, returning existing JobID: %s", req.RequestID, existingJobID)
		return existingJobID, nil
	}

	//	If Job not exists, generate a new Job ID
	jobID := uuid.New().String()
	log.Printf("Creating new job with ID: %s", jobID)

	// Set retry Configuration
	maxRetries := req.MaxRetries
	if maxRetries == 0 {
		maxRetries = 3 // Default retries
	}

	//	Step 3: Create job Object with retry policy
	newJob := &to_delete.Job{
		JobID:          jobID,
		Name:           req.Name,
		RequestID:      req.RequestID,
		Image:          req.Image,
		Command:        req.Command,
		CPUs:           req.CPUs,
		GPUs:           req.GPUs,
		State:          to_delete.StatePending,
		CreatedAt:      time.Now(),
		ExecutionCount: 0,
		RetryCount:     0,
		MaxRetries:     maxRetries,
		RetryPolicy:    to_delete.DefaultRetryPolicy(),
	}

	jobData, err := newJob.Serialize()

	if err != nil {
		return "", fmt.Errorf("Failed to serialize job: %w", err)
	}

	// If job needs GPUs, find best placement
	var assignedGPUs []int
	if req.GPUs > 0 {
		placement, err := s.findBestGPUPlacement(ctx, req.GPUs)
		if err != nil {
			log.Printf("Warning: Could not find any Optimal GPU Placement: %v", err)
			//	Job Still queued, worker will try to place it
		} else {
			assignedGPUs = placement.GPUIndices
			log.Printf("GPU placement for job %s: %v (score: %.1f, %s)",
				jobID, assignedGPUs, placement.Score, placement.Reasoning)
		}
	}

	// Store assignment if found
	if len(assignedGPUs) > 0 {
		assignmentData, _ := json.Marshal(map[string]interface{}{
			"gpu_indices": assignedGPUs,
		})
		s.etcd.Put(ctx, fmt.Sprintf("/assignments/%s", jobID), string(assignmentData))
	}

	//	Step 4: Atomic Transaction
	txn := s.etcd.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(requestKey), "=", 0)).
		Then(
			clientv3.OpPut(requestKey, jobID),
			clientv3.OpPut(fmt.Sprintf("/jobs/%s", jobID), jobData),
			clientv3.OpPut(fmt.Sprintf("/queue/pending/%s", jobID), jobID),
			clientv3.OpPut(fmt.Sprintf("/state/%s", jobID), string(to_delete.StatePending)),
		)

	txnResp, err := txn.Commit()
	if err != nil {
		return "", fmt.Errorf("Failed to commit transaction: %w", err)
	}

	if !txnResp.Succeeded {
		log.Printf("Race Condition detected for RequestID: %s, retrying...", req.RequestID)
		return s.SubmitJob(ctx, req)
	}

	log.Printf("Job %s queued successfully", jobID)
	return jobID, nil

}

// findBestGPUPlacement finds optimal GPU placement across workers
func (s *Scheduler) findBestGPUPlacement(ctx context.Context, requiredGPUs int) (*gpu.PlacementCandidate, error) {
	//	Get all worker topologies
	resp, err := s.etcd.Get(ctx, "/topology/", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var bestPlacement *gpu.PlacementCandidate
	var bestScore float64 = -1

	//	Try each worker
	for _, kv := range resp.Kvs {
		var topology gpu.Topology
		if err := json.Unmarshal(kv.Value, &topology); err != nil {
			continue
		}

		scorer := gpu.NewPlacementScorer(&topology)
		placement, err := scorer.FindBestPlacement(requiredGPUs)
		if err != nil {
			continue
		}

		if placement.Score > bestScore {
			bestScore = placement.Score
			bestPlacement = placement
			bestPlacement.Reasoning += fmt.Sprintf(" on worker %s", topology.WorkerID)
		}
	}

	if bestPlacement == nil {
		return nil, fmt.Errorf("no suitable GPU placement found")
	}

	return bestPlacement, nil
}

// GetJob retrieves job information by ID
func (s *Scheduler) GetJob(ctx context.Context, jobID string) (*to_delete.Job, error) {
	key := fmt.Sprintf("/jobs/%s", jobID)
	resp, err := s.etcd.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("Failed to get job %s: %w", jobID, err)
	}
	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}
	return to_delete.DeserializeJob(string(resp.Kvs[0].Value))
}

// ListPendingJobs -- lists all jobs in the pending queue
func (s *Scheduler) ListPendingJobs(ctx context.Context) ([]*to_delete.Job, error) {
	resp, err := s.etcd.Get(ctx, "/queue/pending/", clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("Failed to list pending jobs: %w", err)
	}
	var jobs []*to_delete.Job
	for _, kv := range resp.Kvs {
		jobID := string(kv.Value)
		j, err := s.GetJob(ctx, jobID)
		if err != nil {
			log.Printf("Warning: Failed to get job %s: %v", jobID, err)
			continue
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}
