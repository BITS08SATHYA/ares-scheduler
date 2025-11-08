package scheduler

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"time"

	// Import your job package
	"github.com/BITS08SATHYA/ares-scheduler/pkg/job"
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

// Closes the etcd client connection
func (s *Scheduler) Close() error {
	return s.etcd.Close()
}

// SubmitJob submits a new Job with idempotency guarantee
func (s *Scheduler) SubmitJob(ctx context.Context, req job.JobSubmission) (string, error) {
	log.Printf("Submitting Job: %s (RequestID: %s)", req.Name, req.RequestID)

	// Step 1: Check if this request was already processed (idempotency)
	requestKey := fmt.Sprintf("/requests/%s", req.RequestID)
	getResp, err := s.etcd.Get(ctx, requestKey)

	if err != nil {
		return "", fmt.Errorf("Failed to check request: %w", err)
	}

	//	If request already exists, return exisiting job ID
	if len(getResp.Kvs) > 0 {
		exisitngJobID := string(getResp.Kvs[0].Value)
		log.Printf("Request %s already processed, returning existing JobID: %s", req.RequestID, exisitngJobID)
		return exisitngJobID, nil
	}

	//	If Job not exists, generate a new Job ID
	jobID := uuid.New().String()
	log.Printf("Creating new job with ID: %s", jobID)

	//	Step 3: Create job Object
	newJob := &job.Job{
		JobID:          jobID,
		Name:           req.Name,
		RequestID:      req.RequestID,
		Image:          req.Image,
		Command:        req.Command,
		CPUs:           req.CPUs,
		GPUs:           req.GPUs,
		State:          job.StatePending,
		CreatedAt:      time.Now(),
		ExecutionCount: 0,
	}

	jobData, err := newJob.Serialize()

	if err != nil {
		return "", fmt.Errorf("Failed to serialize job: %w", err)
	}

	//	Step 4: Atomic Transaction
	txn := s.etcd.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(requestKey), "=", 0)).
		Then(
			clientv3.OpPut(requestKey, jobID),
			clientv3.OpPut(fmt.Sprintf("/jobs/%s", jobID), jobData),
			clientv3.OpPut(fmt.Sprintf("/queue/pending/%s", jobID), jobID),
			clientv3.OpPut(fmt.Sprintf("/state/%s", jobID), string(job.StatePending)),
		)

	txnResp, err := txn.Commit()
	if err != nil {
		return "", fmt.Errorf("Failed to commit transaction: %w", err)
	}

	if !txnResp.Succeeded {
		log.Printf("Race Condition detected for RequestID: %s, retrying...", req.RequestID)
		return s.SubmitJob(ctx, req)
	}

	log.Printf("Job %s successfully created and queued", jobID)
	return jobID, nil

}

// GetJob retrieves job information by ID
func (s *Scheduler) GetJob(ctx context.Context, jobID string) (*job.Job, error) {
	key := fmt.Sprintf("/jobs/%s", jobID)
	resp, err := s.etcd.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("Failed to get job %s: %w", jobID, err)
	}
	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}
	return job.DeserializeJob(string(resp.Kvs[0].Value))
}

// ListPendingJobs -- lists all jobs in the pending queue
func (s *Scheduler) ListPendingJobs(ctx context.Context) ([]*job.Job, error) {
	resp, err := s.etcd.Get(ctx, "/queue/pending/", clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("Failed to list pending jobs: %w", err)
	}
	var jobs []*job.Job
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
