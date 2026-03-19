package integration_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/idempotency"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/lease"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// IDEMPOTENCY → LEASE → JOB STORE PIPELINE
// ============================================================================

// TestPipeline_SubmitReserveLeaseSave exercises the full pipeline:
// idempotency reserve (Redis) → lease acquire (etcd) → job save (etcd) → read back.
// Uses direct etcd lease grants instead of LeaseManager to avoid heartbeat goroutine hangs.
func TestPipeline_SubmitReserveLeaseSave(t *testing.T) {
	skipIfNoInfra(t)

	rc := newRedisClient(t)
	ec := newETCDClient(t)
	ctx := context.Background()

	im := idempotency.NewIdempotencyManager(rc)
	js := lease.NewJobStore(ec, &testLogger{})

	requestID := fmt.Sprintf("inttest-pipeline-%d", time.Now().UnixNano())
	jobID := fmt.Sprintf("job-pipeline-%d", time.Now().UnixNano())

	// Grant lease directly (no heartbeat goroutine)
	leaseID, err := ec.GrantLease(ctx, 60)
	require.NoError(t, err)

	t.Cleanup(func() {
		im.DeleteEntry(ctx, requestID)
		js.DeleteJob(ctx, jobID)
		ec.RevokeLease(ctx, leaseID)
		cleanupETCDPrefix(t, ec, "ares:jobs:"+jobID)
	})

	// Step 1: Idempotency reserve
	_, isDup, err := im.CheckAndReserve(ctx, requestID, jobID)
	require.NoError(t, err)
	assert.False(t, isDup, "first submission should not be a duplicate")

	// Step 2: Record success in idempotency cache
	err = im.RecordSuccess(ctx, requestID, jobID)
	require.NoError(t, err)

	// Step 3: Lease already acquired above

	// Step 4: Store job in etcd with lease
	jobRecord := &lease.JobRecord{
		JobID:     jobID,
		TenantID:  "tenant-inttest",
		Status:    lease.JobStatePending,
		CreatedAt: time.Now(),
	}
	err = js.StoreJob(ctx, jobRecord, leaseID)
	require.NoError(t, err)

	// Step 5: Read back
	retrieved, err := js.GetJob(ctx, jobID)
	require.NoError(t, err)
	assert.Equal(t, jobID, retrieved.JobID)
	assert.Equal(t, lease.JobStatePending, retrieved.Status)
}

// TestPipeline_DuplicateSubmissionReturnsCached verifies that a duplicate
// submission returns the cached result without creating a new lease.
func TestPipeline_DuplicateSubmissionReturnsCached(t *testing.T) {
	skipIfNoInfra(t)

	rc := newRedisClient(t)
	ctx := context.Background()
	im := idempotency.NewIdempotencyManager(rc)

	requestID := fmt.Sprintf("inttest-dup-%d", time.Now().UnixNano())
	jobID := "job-dup-original"

	t.Cleanup(func() { im.DeleteEntry(ctx, requestID) })

	// First submission
	_, isDup, err := im.CheckAndReserve(ctx, requestID, jobID)
	require.NoError(t, err)
	assert.False(t, isDup)

	// Record success
	require.NoError(t, im.RecordSuccess(ctx, requestID, jobID))

	// Second submission (duplicate)
	result, isDup, err := im.CheckAndReserve(ctx, requestID, "job-dup-should-not-use")
	require.NoError(t, err)
	assert.True(t, isDup, "second submission should be flagged as duplicate")
	require.NotNil(t, result)
	assert.Equal(t, jobID, result.JobID, "duplicate should return original job ID")
}

// TestPipeline_TwoSchedulersCompeteForLease verifies that exactly one scheduler
// wins the lease when two compete for the same job using etcd LeaseCAS.
func TestPipeline_TwoSchedulersCompeteForLease(t *testing.T) {
	skipIfNoInfra(t)

	ec := newETCDClient(t)
	ctx := context.Background()

	jobID := fmt.Sprintf("job-compete-%d", time.Now().UnixNano())
	leaseKey := fmt.Sprintf("ares:leases:%s", jobID)

	// Scheduler A: grant lease, try CAS
	leaseIDA, err := ec.GrantLease(ctx, 60)
	require.NoError(t, err)

	winA, err := ec.LeaseCAS(ctx, leaseKey, `{"scheduler":"A"}`, leaseIDA)
	require.NoError(t, err)

	// Scheduler B: grant lease, try CAS
	leaseIDB, err := ec.GrantLease(ctx, 60)
	require.NoError(t, err)

	winB, err := ec.LeaseCAS(ctx, leaseKey, `{"scheduler":"B"}`, leaseIDB)
	require.NoError(t, err)

	t.Cleanup(func() {
		ec.Delete(ctx, leaseKey)
		ec.RevokeLease(ctx, leaseIDA)
		ec.RevokeLease(ctx, leaseIDB)
	})

	// Exactly one should win
	assert.NotEqual(t, winA, winB,
		"exactly one scheduler should win the lease CAS (got: A=%v, B=%v)", winA, winB)
	assert.True(t, winA || winB, "at least one should win")
}

// TestPipeline_JobAutoDeletedOnLeaseRevoke verifies that a job stored with a
// lease is auto-deleted when the lease is revoked.
func TestPipeline_JobAutoDeletedOnLeaseRevoke(t *testing.T) {
	skipIfNoInfra(t)

	ec := newETCDClient(t)
	ctx := context.Background()

	js := lease.NewJobStore(ec, &testLogger{})

	jobID := fmt.Sprintf("job-revoke-%d", time.Now().UnixNano())

	leaseID, err := ec.GrantLease(ctx, 60)
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupETCDPrefix(t, ec, "ares:jobs:"+jobID)
	})

	// Store job with lease
	jobRecord := &lease.JobRecord{
		JobID:     jobID,
		TenantID:  "tenant-revoke",
		Status:    lease.JobStateRunning,
		CreatedAt: time.Now(),
	}
	require.NoError(t, js.StoreJob(ctx, jobRecord, leaseID))

	// Verify job exists
	retrieved, err := js.GetJob(ctx, jobID)
	require.NoError(t, err)
	require.NotNil(t, retrieved)

	// Revoke the lease (simulating executor crash)
	err = ec.RevokeLease(ctx, leaseID)
	require.NoError(t, err)

	// Wait for etcd to clean up the lease-attached key
	time.Sleep(500 * time.Millisecond)

	// Job should be auto-deleted
	retrieved, err = js.GetJob(ctx, jobID)
	assert.Error(t, err, "job should be deleted after lease revoke")
}

// TestPipeline_ConcurrentSetNXAtomicity races 10 goroutines on the same request ID.
// Only one should win the SetNX; all others should see it as a duplicate.
func TestPipeline_ConcurrentSetNXAtomicity(t *testing.T) {
	skipIfNoInfra(t)

	rc := newRedisClient(t)
	ctx := context.Background()
	im := idempotency.NewIdempotencyManager(rc)

	requestID := fmt.Sprintf("inttest-race-%d", time.Now().UnixNano())
	t.Cleanup(func() { im.DeleteEntry(ctx, requestID) })

	const goroutines = 10
	var wins int32
	var wg sync.WaitGroup

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			jobID := fmt.Sprintf("job-race-%d", idx)
			_, isDup, err := im.CheckAndReserve(ctx, requestID, jobID)
			if err != nil {
				t.Logf("goroutine %d error: %v", idx, err)
				return
			}
			if !isDup {
				atomic.AddInt32(&wins, 1)
			}
		}(i)
	}

	wg.Wait()
	assert.Equal(t, int32(1), wins, "exactly one goroutine should win the SetNX race")
}
