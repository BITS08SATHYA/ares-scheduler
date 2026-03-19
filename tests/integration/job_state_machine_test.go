package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/lease"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// JOB STATE MACHINE INTEGRATION TESTS
//
// These tests use etcd leases directly (via ETCDClient.GrantLease) instead of
// LeaseManager.AcquireLeaseForJob, because AcquireLeaseForJob starts a heartbeat
// goroutine that calls KeepAliveOnce(context.Background()) and cannot be
// interrupted, causing test hangs. The JobStore/state machine logic is the same.
// ============================================================================

// TestJobSM_HappyPath walks a job through PENDING → SCHEDULED → PLACED → RUNNING → SUCCEEDED,
// all persisted to etcd.
func TestJobSM_HappyPath(t *testing.T) {
	skipIfNoInfra(t)

	ec := newETCDClient(t)
	ctx := context.Background()

	js := lease.NewJobStore(ec, &testLogger{})

	// Create an etcd lease directly (no heartbeat goroutine)
	leaseID, err := ec.GrantLease(ctx, 60)
	require.NoError(t, err)

	jobID := fmt.Sprintf("job-sm-happy-%d", time.Now().UnixNano())

	t.Cleanup(func() {
		js.DeleteJob(ctx, jobID)
		ec.RevokeLease(ctx, leaseID)
		cleanupETCDPrefix(t, ec, "ares:jobs:"+jobID)
	})

	// Create job in PENDING state
	jobRecord := &lease.JobRecord{
		JobID:      jobID,
		TenantID:   "tenant-sm",
		Status:     lease.JobStatePending,
		MaxRetries: 3,
		CreatedAt:  time.Now(),
	}
	require.NoError(t, js.StoreJob(ctx, jobRecord, leaseID))

	// Transition: PENDING → SCHEDULED
	err = js.UpdateJobState(ctx, jobID, lease.JobStateScheduled, nil, leaseID)
	require.NoError(t, err)
	job, _ := js.GetJob(ctx, jobID)
	assert.Equal(t, lease.JobStateScheduled, job.Status)

	// Transition: SCHEDULED → PLACED
	err = js.UpdateJobState(ctx, jobID, lease.JobStatePlaced, map[string]interface{}{
		"cluster_id": "cluster-us-west",
		"node_id":    "node-001",
	}, leaseID)
	require.NoError(t, err)
	job, _ = js.GetJob(ctx, jobID)
	assert.Equal(t, lease.JobStatePlaced, job.Status)
	assert.Equal(t, "cluster-us-west", job.AssignedCluster)
	assert.Equal(t, "node-001", job.AssignedNode)

	// Transition: PLACED → RUNNING
	err = js.UpdateJobState(ctx, jobID, lease.JobStateRunning, map[string]interface{}{
		"pod_name": "ares-pod-123",
	}, leaseID)
	require.NoError(t, err)
	job, _ = js.GetJob(ctx, jobID)
	assert.Equal(t, lease.JobStateRunning, job.Status)
	assert.Equal(t, "ares-pod-123", job.PodName)

	// Transition: RUNNING → SUCCEEDED
	err = js.UpdateJobState(ctx, jobID, lease.JobStateSucceeded, map[string]interface{}{
		"result": "training complete",
	}, leaseID)
	require.NoError(t, err)
	job, _ = js.GetJob(ctx, jobID)
	assert.Equal(t, lease.JobStateSucceeded, job.Status)
	assert.Equal(t, "training complete", job.Result)
}

// TestJobSM_RetryLifecycle tests RUNNING → FAILED → PENDING (retry) → SCHEDULED → PLACED → RUNNING → SUCCEEDED.
func TestJobSM_RetryLifecycle(t *testing.T) {
	skipIfNoInfra(t)

	ec := newETCDClient(t)
	ctx := context.Background()

	js := lease.NewJobStore(ec, &testLogger{})
	leaseID, err := ec.GrantLease(ctx, 60)
	require.NoError(t, err)

	jobID := fmt.Sprintf("job-sm-retry-%d", time.Now().UnixNano())

	t.Cleanup(func() {
		js.DeleteJob(ctx, jobID)
		ec.RevokeLease(ctx, leaseID)
		cleanupETCDPrefix(t, ec, "ares:jobs:"+jobID)
	})

	// Start in PENDING → SCHEDULED → PLACED → RUNNING
	jobRecord := &lease.JobRecord{
		JobID:      jobID,
		TenantID:   "tenant-retry",
		Status:     lease.JobStatePending,
		MaxRetries: 3,
		Attempts:   0,
		CreatedAt:  time.Now(),
	}
	require.NoError(t, js.StoreJob(ctx, jobRecord, leaseID))
	require.NoError(t, js.UpdateJobState(ctx, jobID, lease.JobStateScheduled, nil, leaseID))
	require.NoError(t, js.UpdateJobState(ctx, jobID, lease.JobStatePlaced, nil, leaseID))
	require.NoError(t, js.UpdateJobState(ctx, jobID, lease.JobStateRunning, nil, leaseID))

	// RUNNING → FAILED
	err = js.UpdateJobState(ctx, jobID, lease.JobStateFailed, map[string]interface{}{
		"result": "OOM killed",
	}, leaseID)
	require.NoError(t, err)
	job, _ := js.GetJob(ctx, jobID)
	assert.Equal(t, lease.JobStateFailed, job.Status)

	// FAILED → PENDING (retry)
	err = js.UpdateJobState(ctx, jobID, lease.JobStatePending, nil, leaseID)
	require.NoError(t, err)
	job, _ = js.GetJob(ctx, jobID)
	assert.Equal(t, lease.JobStatePending, job.Status)

	// Second attempt: PENDING → SCHEDULED → PLACED → RUNNING → SUCCEEDED
	require.NoError(t, js.UpdateJobState(ctx, jobID, lease.JobStateScheduled, nil, leaseID))
	require.NoError(t, js.UpdateJobState(ctx, jobID, lease.JobStatePlaced, nil, leaseID))
	require.NoError(t, js.UpdateJobState(ctx, jobID, lease.JobStateRunning, nil, leaseID))
	require.NoError(t, js.UpdateJobState(ctx, jobID, lease.JobStateSucceeded, map[string]interface{}{
		"result": "training complete on retry",
	}, leaseID))

	job, _ = js.GetJob(ctx, jobID)
	assert.Equal(t, lease.JobStateSucceeded, job.Status)
	assert.Equal(t, "training complete on retry", job.Result)
}

// TestJobSM_LeaseOwnershipCheck uses LeaseManager's CheckLeaseOwnership.
// We use LeaseCAS directly to avoid starting heartbeat goroutines.
func TestJobSM_LeaseOwnershipCheck(t *testing.T) {
	skipIfNoInfra(t)

	ec := newETCDClient(t)
	ctx := context.Background()

	lm := lease.NewLeaseManager(ec, "scheduler-sm-fence", &testLogger{})

	jobID := fmt.Sprintf("job-sm-fence-%d", time.Now().UnixNano())

	// Grant a lease and write a lease key manually (no heartbeat goroutine)
	leaseID, err := ec.GrantLease(ctx, 60)
	require.NoError(t, err)

	leaseKey := fmt.Sprintf("ares:leases:%s", jobID)
	_, err = ec.LeaseCAS(ctx, leaseKey, fmt.Sprintf(`{"job_id":"%s","scheduler_id":"scheduler-sm-fence"}`, jobID), leaseID)
	require.NoError(t, err)

	t.Cleanup(func() {
		ec.Delete(ctx, leaseKey)
		ec.RevokeLease(ctx, leaseID)
	})

	// Check ownership should succeed
	modRev, err := lm.CheckLeaseOwnership(ctx, jobID, leaseID)
	require.NoError(t, err)
	assert.Greater(t, modRev, int64(0), "modRevision should be positive")

	// Delete the lease key (simulating lease release)
	require.NoError(t, ec.Delete(ctx, leaseKey))

	// Ownership check should fail now (lease key deleted)
	_, err = lm.CheckLeaseOwnership(ctx, jobID, leaseID)
	assert.Error(t, err, "ownership check should fail after lease key deleted")
}

// TestJobSM_FencedSaveRejectedAfterLeaseChange verifies that a fenced save
// is rejected when a different scheduler acquires the lease (split-brain prevention).
func TestJobSM_FencedSaveRejectedAfterLeaseChange(t *testing.T) {
	skipIfNoInfra(t)

	ec := newETCDClient(t)
	ctx := context.Background()

	jobID := fmt.Sprintf("job-sm-fenced-%d", time.Now().UnixNano())
	leaseKey := fmt.Sprintf("ares:leases:%s", jobID)
	jobKey := fmt.Sprintf("ares:jobs:%s", jobID)

	// Scheduler A: grant lease and write lease key
	leaseIDA, err := ec.GrantLease(ctx, 60)
	require.NoError(t, err)

	_, err = ec.LeaseCAS(ctx, leaseKey, `{"scheduler_id":"A"}`, leaseIDA)
	require.NoError(t, err)

	// Read the modRevision for scheduler A's fencing token
	_, modRev, err := ec.GetWithRevision(ctx, leaseKey)
	require.NoError(t, err)

	// Scheduler A "loses" the lease: delete + revoke
	require.NoError(t, ec.Delete(ctx, leaseKey))
	require.NoError(t, ec.RevokeLease(ctx, leaseIDA))

	// Scheduler B: grant new lease and write lease key
	leaseIDB, err := ec.GrantLease(ctx, 60)
	require.NoError(t, err)

	_, err = ec.LeaseCAS(ctx, leaseKey, `{"scheduler_id":"B"}`, leaseIDB)
	require.NoError(t, err)

	t.Cleanup(func() {
		ec.Delete(ctx, leaseKey)
		ec.Delete(ctx, jobKey)
		ec.RevokeLease(ctx, leaseIDB)
	})

	// Scheduler A tries to do a fenced write with stale modRevision → should fail
	success, err := ec.PutIfModRevision(ctx, leaseKey, modRev, jobKey,
		`{"job_id":"`+jobID+`","status":"SUCCEEDED"}`, 0)
	require.NoError(t, err)
	assert.False(t, success, "fenced write should be rejected after lease ownership changed")
}

// TestJobSM_ListJobsByStatus verifies that GetJobsByStatus correctly filters.
func TestJobSM_ListJobsByStatus(t *testing.T) {
	skipIfNoInfra(t)

	ec := newETCDClient(t)
	ctx := context.Background()

	js := lease.NewJobStore(ec, &testLogger{})
	leaseID, err := ec.GrantLease(ctx, 60)
	require.NoError(t, err)

	prefix := fmt.Sprintf("list-%d", time.Now().UnixNano())
	jobIDs := []string{
		prefix + "-pending",
		prefix + "-running",
		prefix + "-succeeded",
	}

	t.Cleanup(func() {
		for _, id := range jobIDs {
			js.DeleteJob(ctx, id)
		}
		ec.RevokeLease(ctx, leaseID)
	})

	// Create jobs with different statuses
	for _, id := range jobIDs {
		status := lease.JobStatePending
		if id == jobIDs[1] {
			status = lease.JobStateRunning
		} else if id == jobIDs[2] {
			status = lease.JobStateSucceeded
		}

		rec := &lease.JobRecord{
			JobID:     id,
			TenantID:  "tenant-list",
			Status:    status,
			CreatedAt: time.Now(),
		}
		require.NoError(t, js.StoreJob(ctx, rec, leaseID))
	}

	// Filter by RUNNING
	running, err := js.GetJobsByStatus(ctx, lease.JobStateRunning)
	require.NoError(t, err)

	var found bool
	for _, j := range running {
		if j.JobID == jobIDs[1] {
			found = true
		}
		assert.Equal(t, lease.JobStateRunning, j.Status)
	}
	assert.True(t, found, "RUNNING job should appear in filtered results")
}

// TestJobSM_CheckpointRecord verifies that checkpoint metadata can be stored
// in a job record and retrieved (for pod restart env var injection).
func TestJobSM_CheckpointRecord(t *testing.T) {
	skipIfNoInfra(t)

	ec := newETCDClient(t)
	ctx := context.Background()

	js := lease.NewJobStore(ec, &testLogger{})
	leaseID, err := ec.GrantLease(ctx, 60)
	require.NoError(t, err)

	jobID := fmt.Sprintf("job-sm-ckpt-%d", time.Now().UnixNano())

	t.Cleanup(func() {
		js.DeleteJob(ctx, jobID)
		ec.RevokeLease(ctx, leaseID)
	})

	// Store job with checkpoint metadata
	jobRecord := &lease.JobRecord{
		JobID:     jobID,
		TenantID:  "tenant-ckpt",
		Status:    lease.JobStateRunning,
		CreatedAt: time.Now(),
		Result:    "checkpoint:s3://bucket/checkpoints/job123/epoch-47,meta:epoch=47,loss=0.023",
	}
	require.NoError(t, js.StoreJob(ctx, jobRecord, leaseID))

	// Read back and verify checkpoint metadata survives serialization
	retrieved, err := js.GetJob(ctx, jobID)
	require.NoError(t, err)
	assert.Contains(t, retrieved.Result, "checkpoint:s3://bucket/checkpoints/job123/epoch-47")
	assert.Contains(t, retrieved.Result, "epoch=47,loss=0.023")
}
