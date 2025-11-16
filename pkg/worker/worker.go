package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/gpu"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/job"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/lease"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"time"
)

// Worker Executes jobs with exactly-once guarantees
type Worker struct {
	id          string
	etcd        *clientv3.Client
	leaseMgr    *lease.Manager
	gpuDetector *gpu.Detector
	topology    *gpu.Topology
}

// JobResult stores job execution outcome
type JobResult struct {
	Output   string
	ExitCode int
}

// NewWorker creates a new Worker
func NewWorker(workerID string, etcdEndpoints []string) (*Worker, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd: %w", err)
	}

	// Detect GPU topology
	detector := gpu.NewDetector()
	topology, err := detector.DetectTopology(context.Background())
	if err != nil {
		log.Printf("Warning: GPU detection failed : %v", err)
		topology = &gpu.Topology{GPUs: []*gpu.GPU{}}
	} else {
		topology.WorkerID = workerID
		log.Printf("Detected %d GPUs", len(topology.GPUs))
	}

	return &Worker{
		id:          workerID,
		etcd:        cli,
		leaseMgr:    lease.NewManager(cli, workerID),
		gpuDetector: detector,
		topology:    topology,
	}, nil
}

func (w *Worker) Close() error {
	return w.etcd.Close()
}

func (w *Worker) Run(ctx context.Context) error {
	log.Printf("Worker %s starting...", w.id)

	// Register topology
	if err := w.RegisterTopology(ctx); err != nil {
		log.Printf("Warning: Failed to register topology: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %s stopping...", w.id)
			return ctx.Err()
		default:
			if err := w.processNextJob(ctx); err != nil {
				log.Printf("Worker %s: error: %v", w.id, err)
			}
			time.Sleep(1 * time.Second)
		}
	}
}

// RegisterTopology Added method
func (w *Worker) RegisterTopology(ctx context.Context) error {
	topologyKey := fmt.Sprintf("/topology/%s", w.id)

	topologyData, err := json.Marshal(w.topology)
	if err != nil {
		return err
	}

	_, err = w.etcd.Put(ctx, topologyKey, string(topologyData))
	if err != nil {
		return err
	}

	log.Printf("Worker %s registered with %d GPUs", w.id, len(w.topology.GPUs))
	for _, g := range w.topology.GPUs {
		log.Printf(" - %s", g.String())
	}

	return nil
}

func (w *Worker) processNextJob(ctx context.Context) error {
	// Get next job
	j, err := w.getNextPendingJob(ctx)
	if err != nil || j == nil {
		return err
	}

	log.Printf("Worker %s: Found job %s (attempt %d/%d)", w.id, j.JobID, j.RetryCount+1, j.MaxRetries)

	// Check if we should retry this job
	if j.State == job.StateRetrying {
		if time.Now().Before(j.NextRetryAt) {
			//	Not yet time to retry
			return nil
		}
		log.Printf("Worker %s: Retry delay elapsed, attempting job %s", w.id, j.JobID)
	}

	//	Acquire lease with fencing token
	lease, err := w.leaseMgr.AcquireJobLease(ctx, j.JobID, 30)
	if err != nil {
		log.Printf("Worker %s: Failed to acquire lease: %v", w.id, err)
		return nil
	}
	defer w.leaseMgr.ReleaseLease(ctx, lease)

	log.Printf("Worker %s: ACQUIRED LEASE (token=%d) for %s", w.id, lease.FencingToken, j.JobID)

	j.RetryCount++
	w.updateJobMetadata(ctx, j)

	//	Transition from PENDING to RUNNING
	if err := w.transitionState(ctx, j.JobID, j.State, job.StateRunning); err != nil {
		log.Printf("Worker %s: Failed to transition state: %v", w.id, err)
		//w.leaseMgr.ReleaseLease(ctx, lease)
		return err
	}

	log.Printf("Worker %s: Job %s -> RUNNING", w.id, j.JobID)

	//	Start KeepAlive
	leaseCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	keepAliveErrCh, err := w.leaseMgr.KeepAlive(leaseCtx, lease)
	if err != nil {
		log.Printf("Worker %s: Failed to start keepalive: %v", w.id, err)
		//w.leaseMgr.ReleaseLease(ctx, lease)
		//w.transitionState(ctx, j.JobID, job.StateRunning, job.StateFailed)

		// handleJobFailure
		w.handleJobFailure(ctx, j, lease, err)
		return err
	}

	//	Monitor keepAlive in background
	go func() {
		select {
		case err := <-keepAliveErrCh:
			if err != nil {
				log.Printf("Worker %s: Lease Lost: %v", w.id, err)
				cancel()
			}
		case <-leaseCtx.Done():
		}
	}()

	// Execute: Do the actual work
	result, execErr := w.executeJob(leaseCtx, j, lease)

	// State transition: Running --> Succeeded/Failed
	if execErr != nil {
		//log.Printf("Worker %s: Job Execution failed: %v", w.id, execErr)
		//
		////	Transition to Failed
		//w.transitionState(ctx, j.JobID, job.StateRunning, job.StateFailed)
		//log.Printf("Worker %s: Job %s -> FAILED", w.id, j.JobID)
		//
		//return w.commitFailure(ctx, j.JobID, lease, execErr)
		//	 JOB Failed -- Check Retry Policy
		return w.handleJobFailure(ctx, j, lease, execErr)
	}

	// Transition to SUCCEEDED
	if err := w.transitionState(ctx, j.JobID, job.StateRunning, job.StateSucceeded); err != nil {
		log.Printf("Worker %s: Failed to transition to Succeeded: %v", w.id, err)
		return err
	}

	log.Printf("Worker %s: Job %s -> SUCCEEDED", w.id, j.JobID)

	// Commit - write results with fencing validation
	if err := w.commitSuccess(ctx, j.JobID, lease, result); err != nil {
		log.Printf("Worker %s: Zombie Detected during commit: %v", w.id, err)
		return err
	}

	log.Printf("Worker %s: Result Committed (token validated)", w.id)
	return nil

	//	Execute Job
	//var execErr error
	//result := &JobResult{}
	//
	//func() {
	//	defer func() {
	//		if r := recover(); r != nil {
	//			execErr = fmt.Errorf("panic during execution: %v", r)
	//		}
	//	}()
	//	execErr = w.executeJob(leaseCtx, j, result)
	//}()
	//
	//// Commit result with fencing token validation
	//if execErr != nil {
	//	log.Printf("Worker %s: Job Failed: %v", w.id, execErr)
	//	w.commitFailure(ctx, j.JobID, lease, execErr)
	//} else {
	//	log.Printf("Worker %s: Job Succeeded", w.id)
	//	if err := w.commitSuccess(ctx, j.JobID, lease, result); err != nil {
	//		log.Printf("Worker %s: ZOMBIE Detected: %v", w.id, err)
	//		return err
	//	}
	//	log.Printf("Worker %s: Result Committed (token validated)", w.id)
	//}
	//
	//return nil
}

// handleJobFailure implements retry logic with exponential backoff
func (w *Worker) handleJobFailure(ctx context.Context, j *job.Job, lease *lease.Lease, execErr error) error {
	log.Printf("Worker %s: Job %s failed (attempt %d/%d): %v",
		w.id, j.JobID, j.RetryCount, j.MaxRetries, execErr)

	// Get retry policy
	policy := j.RetryPolicy
	if policy == nil {
		policy = job.DefaultRetryPolicy()
	}

	// Should we retry?
	if policy.ShouldRetry(j.RetryCount) {
		//	YES - RETRY with BACKOFF
		backoff := policy.CalculateBackoff(j.RetryCount)
		nextRetryAt := time.Now().Add(backoff)

		log.Printf("Worker %s: Scheduling retry for job %s in %v (attempt %d/%d)",
			w.id, j.JobID, backoff.Round(time.Millisecond), j.RetryCount+1, j.MaxRetries)

		//	Update Job Metadata
		j.State = job.StateRetrying
		j.NextRetryAt = nextRetryAt
		j.LastFailure = execErr.Error()
		w.updateJobMetadata(ctx, j)

		//	Transition to RETRYING state
		w.transitionState(ctx, j.JobID, job.StateRunning, job.StateRetrying)

		//	Re-queue job for retry
		queueKey := fmt.Sprintf("/queue/pending/%s", j.JobID)
		w.etcd.Put(ctx, queueKey, j.JobID)

		return nil

	}

	//	NO more retires -- PERMANENT FAILURE
	log.Printf("Worker %s: Job %s exhausted retires (%d/%d) - marking as FAILED", w.id, j.JobID, j.RetryCount, j.MaxRetries)

	w.transitionState(ctx, j.JobID, job.StateRunning, job.StateFailed)
	return w.commitFailure(ctx, j.JobID, lease, execErr)

}

// executeJob runs the actual Job
func (w *Worker) executeJob(ctx context.Context, j *job.Job, lease *lease.Lease) (*JobResult, error) {
	log.Printf("Worker %s: Executing job %s", w.id, j.JobID)
	log.Printf("Image: %s", j.Image)
	log.Printf("Command: %v", j.Command)

	result := &JobResult{}

	//	Simulate work with periodic fencing checks
	for i := 0; i < 5; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		//	check fencing token every second
		if err := w.leaseMgr.ValidateFencingToken(ctx, lease); err != nil {
			return nil, fmt.Errorf("fencing validation failed (Zombie detected): %w", err)
		}

		log.Printf("Worker %s: Progress %d/5 (token valid)", w.id, i+1)
		time.Sleep(1 * time.Second)
	}

	result.Output = "Job Completed Successfully"
	result.ExitCode = 0
	return result, nil
}

// updateJobMetadata
func (w *Worker) updateJobMetadata(ctx context.Context, j *job.Job) error {
	jobKey := fmt.Sprintf("/jobs/%s", j.JobID)
	jobData, err := j.Serialize()
	if err != nil {
		return err
	}
	_, err = w.etcd.Put(ctx, jobKey, jobData)
	return err
}

// commitSuccess write results with fencing token validation
func (w *Worker) commitSuccess(ctx context.Context, jobID string, lease *lease.Lease, result *JobResult) error {
	fencingKey := fmt.Sprintf("/fencing/%s", jobID)
	resultKey := fmt.Sprintf("/results/%s", jobID)
	//stateKey := fmt.Sprintf("/state/%s", jobID)
	lockKey := fmt.Sprintf("/locks/%s", jobID)
	queueKey := fmt.Sprintf("/queue/pending/%s", jobID)

	//	Atomic commit with fencing token check
	txn := w.etcd.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(fencingKey), "=", fmt.Sprintf("%d", lease.FencingToken))).
		Then(
			clientv3.OpPut(resultKey, result.Output),
			//clientv3.OpPut(stateKey, string(job.StateSucceeded)),
			clientv3.OpDelete(lockKey),
			clientv3.OpDelete(fencingKey),
			clientv3.OpDelete(queueKey),
		)
	txnResp, err := txn.Commit()
	if err != nil {
		return fmt.Errorf("Commit transaction failed: %w", err)
	}

	if !txnResp.Succeeded {
		return fmt.Errorf("ZOMBIE WORKER: fencing token mismatch, another worker has lease")
	}

	//	Revoke lease
	//w.leaseMgr.ReleaseLease(ctx, lease)

	return nil
}

// CommitFailure marks job as failed
func (w *Worker) commitFailure(ctx context.Context, jobID string, lease *lease.Lease, execErr error) error {

	errorKey := fmt.Sprintf("/errors/%s", jobID)
	lockKey := fmt.Sprintf("/locks/%s", jobID)
	fencingKey := fmt.Sprintf("/fencing/%s", jobID)
	queueKey := fmt.Sprintf("/queue/pending/%s", jobID)

	// Store error details
	w.etcd.Put(ctx, errorKey, execErr.Error())

	// Clean Up
	w.etcd.Delete(ctx, lockKey)
	w.etcd.Delete(ctx, fencingKey)
	w.etcd.Delete(ctx, queueKey)

	return nil
}

// transitionState atomically changes job state
func (w *Worker) transitionState(ctx context.Context, jobID string, from, to job.JobState) error {
	key := fmt.Sprintf("/state/%s", jobID)

	// Allow transition from Retrying to Running (for retries)
	if from == job.StateRetrying && to == job.StateRunning {
		_, err := w.etcd.Put(ctx, key, string(to))
		return err
	}

	txn := w.etcd.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(key), "=", string(from))).
		Then(clientv3.OpPut(key, string(to)))

	resp, err := txn.Commit()
	if err != nil {
		return fmt.Errorf("transaction failed: %w", err)
	}

	if !resp.Succeeded {
		return fmt.Errorf("invalid state transition %s->%s", from, to)
	}
	return nil
}

func (w *Worker) getNextPendingJob(ctx context.Context) (*job.Job, error) {
	resp, err := w.etcd.Get(ctx, "/queue/pending/",
		clientv3.WithPrefix(),
		clientv3.WithLimit(1),
		clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortAscend),
	)
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, nil
	}

	jobID := string(resp.Kvs[0].Value)
	jobResp, err := w.etcd.Get(ctx, fmt.Sprintf("/jobs/%s", jobID))

	if err != nil {
		return nil, err
	}

	if len(jobResp.Kvs) == 0 {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}

	return job.DeserializeJob(string(jobResp.Kvs[0].Value))
}

//
//func (w *Worker) processNextJob(ctx context.Context) error {
//	currentLease = lease
//}
