package lease_test

import (
	"context"
	"testing"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/lease"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/etcd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// MOCK LOGGER
// ============================================================================

type mockLogger struct{}

func (m *mockLogger) Infof(format string, args ...interface{})  {}
func (m *mockLogger) Warnf(format string, args ...interface{})  {}
func (m *mockLogger) Errorf(format string, args ...interface{}) {}
func (m *mockLogger) Debugf(format string, args ...interface{}) {}

// ============================================================================
// SECTION 1: LEASE MANAGER CREATION TESTS
// ============================================================================

// TestLeaseManagerCreation tests lease manager initialization
func TestLeaseManagerCreation(t *testing.T) {
	t.Run("Create lease manager with valid etcd", func(t *testing.T) {
		etcdClient, err := etcd.NewETCDClient([]string{"localhost:2379"}, 10*time.Second)
		require.NoError(t, err)
		defer etcdClient.Close()

		logger := &mockLogger{}
		manager := lease.NewLeaseManager(etcdClient, "scheduler-1", logger)

		assert.NotNil(t, manager, "Manager should be created")
	})

	t.Run("Multiple managers with different scheduler IDs", func(t *testing.T) {
		etcdClient, err := etcd.NewETCDClient([]string{"localhost:2379"}, 10*time.Second)
		require.NoError(t, err)
		defer etcdClient.Close()

		logger := &mockLogger{}
		manager1 := lease.NewLeaseManager(etcdClient, "scheduler-1", logger)
		manager2 := lease.NewLeaseManager(etcdClient, "scheduler-2", logger)

		assert.NotNil(t, manager1)
		assert.NotNil(t, manager2)
	})
}

// ============================================================================
// SECTION 2: LEASE ACQUISITION TESTS (CORE FEATURE)
// ============================================================================

// TestAcquireLeaseForJob tests lease acquisition
func TestAcquireLeaseForJob(t *testing.T) {
	ctx := context.Background()
	etcdClient, err := etcd.NewETCDClient([]string{"localhost:2379"}, 10*time.Second)
	require.NoError(t, err)
	defer etcdClient.Close()

	logger := &mockLogger{}
	manager := lease.NewLeaseManager(etcdClient, "scheduler-test", logger)

	t.Run("Acquire lease for new job succeeds", func(t *testing.T) {
		jobID := "test-job-acquire-success-111"

		// Clean up any existing lease
		manager.ReleaseLeaseForJob(ctx, jobID)
		time.Sleep(200 * time.Millisecond)

		success, leaseID, err := manager.AcquireLeaseForJob(ctx, jobID)

		assert.NoError(t, err, "Should not error acquiring new lease")
		assert.True(t, success, "Should succeed acquiring new lease")
		assert.Greater(t, leaseID, int64(0), "Lease ID should be positive")

		// Verify lease info is tracked
		info, err := manager.GetLeaseForJob(ctx, jobID)
		assert.NoError(t, err)
		assert.Equal(t, jobID, info.JobID)
		assert.Equal(t, "scheduler-test", info.SchedulerID)
		assert.False(t, info.AcquiredAt.IsZero())

		// Clean up
		manager.ReleaseLeaseForJob(ctx, jobID)
		time.Sleep(200 * time.Millisecond)
	})

	t.Run("Acquire lease for same job twice fails (mutual exclusion)", func(t *testing.T) {
		jobID := "test-job-acquire-duplicate-222"

		// Clean up
		manager.ReleaseLeaseForJob(ctx, jobID)
		time.Sleep(200 * time.Millisecond)

		// First acquisition succeeds
		success1, leaseID1, err1 := manager.AcquireLeaseForJob(ctx, jobID)
		require.NoError(t, err1)
		require.True(t, success1)
		require.Greater(t, leaseID1, int64(0))

		// Second acquisition fails (lease already held)
		success2, leaseID2, err2 := manager.AcquireLeaseForJob(ctx, jobID)

		// Should not error, but should fail to acquire
		assert.NoError(t, err2, "Should not error, just fail to acquire")
		assert.False(t, success2, "Should fail to acquire (already held)")
		assert.Equal(t, int64(0), leaseID2, "Lease ID should be 0 when failed")

		t.Logf("✅ Mutual exclusion test passed: first acquired=%v (leaseID=%d), second acquired=%v (leaseID=%d)",
			success1, leaseID1, success2, leaseID2)

		// Clean up
		manager.ReleaseLeaseForJob(ctx, jobID)
		time.Sleep(200 * time.Millisecond)
	})

	t.Run("Different schedulers compete for same job (only one wins)", func(t *testing.T) {
		jobID := "test-job-compete-333"

		// Create two managers (different scheduler IDs)
		manager1 := lease.NewLeaseManager(etcdClient, "scheduler-1", logger)
		manager2 := lease.NewLeaseManager(etcdClient, "scheduler-2", logger)

		// Clean up
		manager1.ReleaseLeaseForJob(ctx, jobID)
		time.Sleep(200 * time.Millisecond)

		// Manager 1 acquires lease
		success1, leaseID1, err1 := manager1.AcquireLeaseForJob(ctx, jobID)
		require.NoError(t, err1)
		require.True(t, success1)
		require.Greater(t, leaseID1, int64(0))

		// Manager 2 tries to acquire same lease (should fail)
		success2, leaseID2, err2 := manager2.AcquireLeaseForJob(ctx, jobID)

		assert.NoError(t, err2)
		assert.False(t, success2, "Manager 2 should fail to acquire (manager 1 holds it)")
		assert.Equal(t, int64(0), leaseID2)

		t.Logf("✅ Competition test passed: manager1 acquired=%v, manager2 acquired=%v", success1, success2)

		// Clean up
		manager1.ReleaseLeaseForJob(ctx, jobID)
		time.Sleep(200 * time.Millisecond)
	})

	t.Run("Acquire lease starts heartbeat goroutine", func(t *testing.T) {
		jobID := "test-job-heartbeat-444"

		// Clean up
		manager.ReleaseLeaseForJob(ctx, jobID)
		time.Sleep(200 * time.Millisecond)

		success, _, err := manager.AcquireLeaseForJob(ctx, jobID)
		require.NoError(t, err)
		require.True(t, success)

		// Get initial lease info
		info1, err := manager.GetLeaseForJob(ctx, jobID)
		require.NoError(t, err)
		initialRenewTime := info1.LastRenewedAt
		initialAttempts := info1.RenewalAttempts

		t.Logf("Initial state: LastRenewedAt=%v, RenewalAttempts=%d", initialRenewTime, initialAttempts)

		// Wait for heartbeat to renew (heartbeat every 10s, wait 12s)
		t.Log("Waiting 12 seconds for heartbeat renewal...")
		time.Sleep(12 * time.Second)

		// Get updated lease info
		info2, err := manager.GetLeaseForJob(ctx, jobID)
		require.NoError(t, err)

		t.Logf("After 12s: LastRenewedAt=%v, RenewalAttempts=%d", info2.LastRenewedAt, info2.RenewalAttempts)

		// Renewal attempts should have increased
		assert.Greater(t, info2.RenewalAttempts, initialAttempts,
			"Heartbeat should have renewed lease at least once")
		assert.True(t, info2.LastRenewedAt.After(initialRenewTime),
			"Last renewed time should have updated")

		t.Logf("✅ Heartbeat test passed: renewal attempts increased from %d to %d",
			initialAttempts, info2.RenewalAttempts)

		// Clean up
		manager.ReleaseLeaseForJob(ctx, jobID)
		time.Sleep(200 * time.Millisecond)
	})
}

// ============================================================================
// SECTION 3: LEASE RELEASE TESTS
// ============================================================================

// TestReleaseLeaseForJob tests lease release
func TestReleaseLeaseForJob(t *testing.T) {
	ctx := context.Background()
	etcdClient, err := etcd.NewETCDClient([]string{"localhost:2379"}, 10*time.Second)
	require.NoError(t, err)
	defer etcdClient.Close()

	logger := &mockLogger{}
	manager := lease.NewLeaseManager(etcdClient, "scheduler-release", logger)

	t.Run("Release acquired lease succeeds", func(t *testing.T) {
		jobID := "test-job-release-success-555"

		// Acquire lease
		success, _, err := manager.AcquireLeaseForJob(ctx, jobID)
		require.NoError(t, err)
		require.True(t, success)

		// Verify lease exists
		info, err := manager.GetLeaseForJob(ctx, jobID)
		require.NoError(t, err)
		require.Equal(t, jobID, info.JobID)

		// Release lease
		err = manager.ReleaseLeaseForJob(ctx, jobID)
		assert.NoError(t, err, "Should release lease without error")

		// Verify lease no longer exists
		_, err = manager.GetLeaseForJob(ctx, jobID)
		assert.Error(t, err, "Lease should not exist after release")
		assert.Contains(t, err.Error(), "not found")

		t.Log("✅ Release test passed: lease successfully removed")
	})

	t.Run("Release non-existent lease fails", func(t *testing.T) {
		jobID := "test-job-release-nonexistent-666"

		err := manager.ReleaseLeaseForJob(ctx, jobID)
		assert.Error(t, err, "Should error releasing non-existent lease")
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("After release, another scheduler can acquire", func(t *testing.T) {
		jobID := "test-job-release-reacquire-777"

		manager1 := lease.NewLeaseManager(etcdClient, "scheduler-1", logger)
		manager2 := lease.NewLeaseManager(etcdClient, "scheduler-2", logger)

		// Manager 1 acquires
		success1, _, err := manager1.AcquireLeaseForJob(ctx, jobID)
		require.NoError(t, err)
		require.True(t, success1)

		// Manager 1 releases
		err = manager1.ReleaseLeaseForJob(ctx, jobID)
		require.NoError(t, err)
		time.Sleep(200 * time.Millisecond)

		// Manager 2 should now be able to acquire
		success2, _, err := manager2.AcquireLeaseForJob(ctx, jobID)
		assert.NoError(t, err)
		assert.True(t, success2, "Manager 2 should acquire after manager 1 released")

		t.Log("✅ Reacquisition test passed: lease released and reacquired by different scheduler")

		// Clean up
		manager2.ReleaseLeaseForJob(ctx, jobID)
		time.Sleep(200 * time.Millisecond)
	})
}

// ============================================================================
// SECTION 4: FENCING TOKEN TESTS (SPLIT-BRAIN PREVENTION)
// ============================================================================

// TestCheckLeaseOwnership tests fencing tokens
func TestCheckLeaseOwnership(t *testing.T) {
	ctx := context.Background()
	etcdClient, err := etcd.NewETCDClient([]string{"localhost:2379"}, 10*time.Second)
	require.NoError(t, err)
	defer etcdClient.Close()

	logger := &mockLogger{}
	manager := lease.NewLeaseManager(etcdClient, "scheduler-fencing", logger)

	t.Run("Check ownership of valid lease succeeds", func(t *testing.T) {
		jobID := "test-job-fencing-valid-888"

		// Clean up
		manager.ReleaseLeaseForJob(ctx, jobID)
		time.Sleep(200 * time.Millisecond)

		// Acquire lease
		success, leaseID, err := manager.AcquireLeaseForJob(ctx, jobID)
		require.NoError(t, err)
		require.True(t, success)

		// Check ownership (should succeed)
		err = manager.CheckLeaseOwnership(ctx, jobID, leaseID)
		assert.NoError(t, err, "Should own the lease")

		t.Log("✅ Fencing test passed: ownership verified")

		// Clean up
		manager.ReleaseLeaseForJob(ctx, jobID)
		time.Sleep(200 * time.Millisecond)
	})

	t.Run("Check ownership after release fails (lease gone)", func(t *testing.T) {
		jobID := "test-job-fencing-released-999"

		// Clean up
		manager.ReleaseLeaseForJob(ctx, jobID)
		time.Sleep(200 * time.Millisecond)

		// Acquire lease
		success, leaseID, err := manager.AcquireLeaseForJob(ctx, jobID)
		require.NoError(t, err)
		require.True(t, success)

		// Release lease
		err = manager.ReleaseLeaseForJob(ctx, jobID)
		require.NoError(t, err)
		time.Sleep(200 * time.Millisecond)

		// Check ownership (should fail - lease no longer in etcd)
		err = manager.CheckLeaseOwnership(ctx, jobID, leaseID)
		assert.Error(t, err, "Should fail ownership check (lease released)")
		assert.Contains(t, err.Error(), "not found")

		t.Log("✅ Fencing test passed: detected lease was released (split-brain prevented)")
	})

	t.Run("Check ownership by different scheduler fails", func(t *testing.T) {
		jobID := "test-job-fencing-different-000"

		// Manager 1 acquires lease
		manager1 := lease.NewLeaseManager(etcdClient, "scheduler-A", logger)
		success, leaseID, err := manager1.AcquireLeaseForJob(ctx, jobID)
		require.NoError(t, err)
		require.True(t, success)

		// Manager 2 tries to check ownership (should fail - different scheduler ID)
		manager2 := lease.NewLeaseManager(etcdClient, "scheduler-B", logger)
		err = manager2.CheckLeaseOwnership(ctx, jobID, leaseID)

		assert.Error(t, err, "Should fail ownership check (different scheduler)")
		assert.Contains(t, err.Error(), "another scheduler")

		t.Log("✅ Fencing test passed: detected lease owned by different scheduler (split-brain prevented)")

		// Clean up
		manager1.ReleaseLeaseForJob(ctx, jobID)
		time.Sleep(200 * time.Millisecond)
	})
}

// ============================================================================
// SECTION 5: LEASE INFO QUERIES TESTS
// ============================================================================

// TestGetLeaseForJob tests lease info retrieval
func TestGetLeaseForJob(t *testing.T) {
	ctx := context.Background()
	etcdClient, err := etcd.NewETCDClient([]string{"localhost:2379"}, 10*time.Second)
	require.NoError(t, err)
	defer etcdClient.Close()

	logger := &mockLogger{}
	manager := lease.NewLeaseManager(etcdClient, "scheduler-info", logger)

	t.Run("Get lease info for existing lease", func(t *testing.T) {
		jobID := "test-job-getinfo-111"

		// Clean up
		manager.ReleaseLeaseForJob(ctx, jobID)
		time.Sleep(200 * time.Millisecond)

		// Acquire lease
		success, leaseID, err := manager.AcquireLeaseForJob(ctx, jobID)
		require.NoError(t, err)
		require.True(t, success)

		// Get lease info
		info, err := manager.GetLeaseForJob(ctx, jobID)

		assert.NoError(t, err)
		assert.NotNil(t, info)
		assert.Equal(t, jobID, info.JobID)
		assert.Equal(t, leaseID, info.LeaseID)
		assert.Equal(t, "scheduler-info", info.SchedulerID)
		assert.False(t, info.AcquiredAt.IsZero())
		assert.False(t, info.LastRenewedAt.IsZero())
		assert.False(t, info.ExpiresAt.IsZero())

		// Clean up
		manager.ReleaseLeaseForJob(ctx, jobID)
		time.Sleep(200 * time.Millisecond)
	})

	t.Run("Get lease info for non-existent lease fails", func(t *testing.T) {
		jobID := "test-job-getinfo-nonexistent-222"

		info, err := manager.GetLeaseForJob(ctx, jobID)

		assert.Error(t, err)
		assert.Nil(t, info)
		assert.Contains(t, err.Error(), "not found")
	})
}

// TestGetActiveLeases tests retrieval of all active leases
func TestGetActiveLeases(t *testing.T) {
	ctx := context.Background()
	etcdClient, err := etcd.NewETCDClient([]string{"localhost:2379"}, 10*time.Second)
	require.NoError(t, err)
	defer etcdClient.Close()

	logger := &mockLogger{}
	manager := lease.NewLeaseManager(etcdClient, "scheduler-active", logger)

	t.Run("Get active leases with multiple leases", func(t *testing.T) {
		// Clean up any existing
		activeLeases := manager.GetActiveLeases()
		for jobID := range activeLeases {
			manager.ReleaseLeaseForJob(ctx, jobID)
		}
		time.Sleep(200 * time.Millisecond)

		// Acquire multiple leases
		jobIDs := []string{
			"test-job-active-1",
			"test-job-active-2",
			"test-job-active-3",
		}

		for _, jobID := range jobIDs {
			success, _, err := manager.AcquireLeaseForJob(ctx, jobID)
			require.NoError(t, err)
			require.True(t, success)
		}

		// Get active leases
		leases := manager.GetActiveLeases()
		assert.GreaterOrEqual(t, len(leases), 3, "Should have at least 3 active leases")

		// Verify each lease
		for _, jobID := range jobIDs {
			_, found := leases[jobID]
			assert.True(t, found, "Lease for %s should be found", jobID)
		}

		// Clean up
		for _, jobID := range jobIDs {
			manager.ReleaseLeaseForJob(ctx, jobID)
		}
		time.Sleep(200 * time.Millisecond)
	})
}

// ============================================================================
// SECTION 6: JOB STORE TESTS
// ============================================================================

// TestJobStore tests job storage operations
func TestJobStore(t *testing.T) {
	ctx := context.Background()
	etcdClient, err := etcd.NewETCDClient([]string{"localhost:2379"}, 10*time.Second)
	require.NoError(t, err)
	defer etcdClient.Close()

	logger := &mockLogger{}
	jobStore := lease.NewJobStore(etcdClient, logger)
	leaseMgr := lease.NewLeaseManager(etcdClient, "scheduler-jobstore", logger)

	t.Run("Store and retrieve job", func(t *testing.T) {
		jobID := "test-jobstore-store-333"

		// Acquire lease for job
		success, leaseID, err := leaseMgr.AcquireLeaseForJob(ctx, jobID)
		require.NoError(t, err)
		require.True(t, success)

		// Create job record
		job := &lease.JobRecord{
			JobID:      jobID,
			TenantID:   "tenant-1",
			Status:     lease.JobStatePending,
			CreatedAt:  time.Now(),
			MaxRetries: 3,
			Attempts:   0,
		}

		// Store job
		err = jobStore.StoreJob(ctx, job, leaseID)
		assert.NoError(t, err, "Should store job without error")

		// Retrieve job
		retrieved, err := jobStore.GetJob(ctx, jobID)
		assert.NoError(t, err)
		assert.NotNil(t, retrieved)
		assert.Equal(t, jobID, retrieved.JobID)
		assert.Equal(t, "tenant-1", retrieved.TenantID)
		assert.Equal(t, lease.JobStatePending, retrieved.Status)

		// Clean up
		leaseMgr.ReleaseLeaseForJob(ctx, jobID)
		time.Sleep(200 * time.Millisecond)
	})

	t.Run("Get non-existent job fails", func(t *testing.T) {
		jobID := "test-jobstore-nonexistent-444"

		retrieved, err := jobStore.GetJob(ctx, jobID)
		assert.Error(t, err)
		assert.Nil(t, retrieved)
		assert.Contains(t, err.Error(), "not found")
	})
}

// TestUpdateJobState tests job state transitions
func TestUpdateJobState(t *testing.T) {
	ctx := context.Background()
	etcdClient, err := etcd.NewETCDClient([]string{"localhost:2379"}, 10*time.Second)
	require.NoError(t, err)
	defer etcdClient.Close()

	logger := &mockLogger{}
	jobStore := lease.NewJobStore(etcdClient, logger)
	leaseMgr := lease.NewLeaseManager(etcdClient, "scheduler-update", logger)

	t.Run("Valid state transition succeeds", func(t *testing.T) {
		jobID := "test-jobstore-transition-555"

		// Acquire lease
		success, leaseID, err := leaseMgr.AcquireLeaseForJob(ctx, jobID)
		require.NoError(t, err)
		require.True(t, success)

		// Create pending job
		job := &lease.JobRecord{
			JobID:      jobID,
			TenantID:   "tenant-1",
			Status:     lease.JobStatePending,
			CreatedAt:  time.Now(),
			MaxRetries: 3,
		}
		err = jobStore.StoreJob(ctx, job, leaseID)
		require.NoError(t, err)

		// Transition to SCHEDULED
		err = jobStore.UpdateJobState(ctx, jobID, lease.JobStateScheduled, nil, leaseID)
		assert.NoError(t, err, "Should transition PENDING → SCHEDULED")

		// Verify state updated
		updated, err := jobStore.GetJob(ctx, jobID)
		require.NoError(t, err)
		assert.Equal(t, lease.JobStateScheduled, updated.Status)
		assert.False(t, updated.ScheduledAt.IsZero(), "ScheduledAt should be set")

		// Clean up
		leaseMgr.ReleaseLeaseForJob(ctx, jobID)
		time.Sleep(200 * time.Millisecond)
	})

	t.Run("Invalid state transition fails", func(t *testing.T) {
		jobID := "test-jobstore-invalid-transition-666"

		// Acquire lease
		success, leaseID, err := leaseMgr.AcquireLeaseForJob(ctx, jobID)
		require.NoError(t, err)
		require.True(t, success)

		// Create pending job
		job := &lease.JobRecord{
			JobID:      jobID,
			Status:     lease.JobStatePending,
			CreatedAt:  time.Now(),
			MaxRetries: 3,
		}
		err = jobStore.StoreJob(ctx, job, leaseID)
		require.NoError(t, err)

		// Try invalid transition: PENDING → RUNNING (should fail)
		err = jobStore.UpdateJobState(ctx, jobID, lease.JobStateRunning, nil, leaseID)
		assert.Error(t, err, "Should fail invalid transition")
		assert.Contains(t, err.Error(), "invalid state transition")

		// Clean up
		leaseMgr.ReleaseLeaseForJob(ctx, jobID)
		time.Sleep(200 * time.Millisecond)
	})
}

// ============================================================================
// SECTION 7: EDGE CASES TESTS
// ============================================================================

// TestEdgeCases tests edge cases in lease manager
func TestEdgeCases(t *testing.T) {
	ctx := context.Background()
	etcdClient, err := etcd.NewETCDClient([]string{"localhost:2379"}, 10*time.Second)
	require.NoError(t, err)
	defer etcdClient.Close()

	logger := &mockLogger{}
	manager := lease.NewLeaseManager(etcdClient, "scheduler-edge", logger)

	t.Run("Rapid acquire/release cycles", func(t *testing.T) {
		jobID := "test-edge-rapid-888"

		for i := 0; i < 5; i++ {
			// Acquire
			success, _, err := manager.AcquireLeaseForJob(ctx, jobID)
			assert.NoError(t, err)
			assert.True(t, success, "Iteration %d should succeed", i)

			// Immediate release
			err = manager.ReleaseLeaseForJob(ctx, jobID)
			assert.NoError(t, err, "Release iteration %d should succeed", i)

			time.Sleep(100 * time.Millisecond)
		}

		t.Log("✅ Rapid cycles test passed: 5 acquire/release cycles completed")
	})

	t.Run("Three schedulers compete (exactly one wins)", func(t *testing.T) {
		jobID := "test-edge-competition-999"

		manager1 := lease.NewLeaseManager(etcdClient, "scheduler-1", logger)
		manager2 := lease.NewLeaseManager(etcdClient, "scheduler-2", logger)
		manager3 := lease.NewLeaseManager(etcdClient, "scheduler-3", logger)

		// Clean up
		manager1.ReleaseLeaseForJob(ctx, jobID)
		time.Sleep(200 * time.Millisecond)

		// All try to acquire simultaneously
		success1, _, _ := manager1.AcquireLeaseForJob(ctx, jobID)
		success2, _, _ := manager2.AcquireLeaseForJob(ctx, jobID)
		success3, _, _ := manager3.AcquireLeaseForJob(ctx, jobID)

		// Count winners
		winners := 0
		if success1 {
			winners++
		}
		if success2 {
			winners++
		}
		if success3 {
			winners++
		}

		// Exactly one should win
		assert.Equal(t, 1, winners, "Exactly one scheduler should acquire lease")

		t.Logf("✅ Competition test passed: exactly 1 of 3 schedulers won (s1=%v, s2=%v, s3=%v)",
			success1, success2, success3)

		// Clean up
		if success1 {
			manager1.ReleaseLeaseForJob(ctx, jobID)
		}
		if success2 {
			manager2.ReleaseLeaseForJob(ctx, jobID)
		}
		if success3 {
			manager3.ReleaseLeaseForJob(ctx, jobID)
		}
		time.Sleep(200 * time.Millisecond)
	})
}
