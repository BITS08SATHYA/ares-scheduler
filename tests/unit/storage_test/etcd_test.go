package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/etcd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestETCDClientBasicOperations tests Put, Get, Delete
func TestETCDClientBasicOperations(t *testing.T) {
	// This test requires a running etcd instance
	// For pure unit tests, we'd mock clientv3.Client
	// For now, this is a "unit test" that tests the wrapper logic

	t.Run("Put and Get", func(t *testing.T) {
		// Setup
		client, err := etcd.NewETCDClient([]string{"localhost:2379"}, 5*time.Second)
		require.NoError(t, err, "Failed to create ETCD client")
		defer client.Close()

		ctx := context.Background()
		key := "test/basic/key1"
		value := "test-value-123"

		// Test Put
		err = client.Put(ctx, key, value)
		assert.NoError(t, err, "Put should succeed")

		// Test Get
		retrieved, err := client.Get(ctx, key)
		assert.NoError(t, err, "Get should succeed")
		assert.Equal(t, value, retrieved, "Retrieved value should match")

		// Cleanup
		client.Delete(ctx, key)
	})

	t.Run("Get non-existent key", func(t *testing.T) {
		client, err := etcd.NewETCDClient([]string{"localhost:2379"}, 5*time.Second)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		key := "test/nonexistent/key999"

		retrieved, err := client.Get(ctx, key)
		assert.NoError(t, err, "Get should not error on missing key")
		assert.Empty(t, retrieved, "Retrieved value should be empty")
	})

	t.Run("Delete key", func(t *testing.T) {
		client, err := etcd.NewETCDClient([]string{"localhost:2379"}, 5*time.Second)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		key := "test/delete/key1"
		value := "to-be-deleted"

		// Put then delete
		client.Put(ctx, key, value)
		err = client.Delete(ctx, key)
		assert.NoError(t, err, "Delete should succeed")

		// Verify deleted
		retrieved, err := client.Get(ctx, key)
		assert.NoError(t, err)
		assert.Empty(t, retrieved, "Key should be gone")
	})
}

// TestETCDClientWithTTL tests time-to-live operations
func TestETCDClientWithTTL(t *testing.T) {
	t.Run("PutWithTTL expires", func(t *testing.T) {
		client, err := etcd.NewETCDClient([]string{"localhost:2379"}, 5*time.Second)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		key := "test/ttl/key1"
		value := "expires-soon"
		ttl := int64(2) // 2 seconds

		// Put with TTL
		err = client.PutWithTTL(ctx, key, value, ttl)
		assert.NoError(t, err, "PutWithTTL should succeed")

		// Verify exists immediately
		retrieved, err := client.Get(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, value, retrieved)

		// Wait for expiration
		time.Sleep(3 * time.Second)

		// Verify expired
		retrieved, err = client.Get(ctx, key)
		assert.NoError(t, err)
		assert.Empty(t, retrieved, "Key should have expired")
	})
}

// TestETCDClientLeaseOperations tests lease management (critical for exactly-once)
func TestETCDClientLeaseOperations(t *testing.T) {
	t.Run("GrantLease and KeepAliveOnce", func(t *testing.T) {
		client, err := etcd.NewETCDClient([]string{"localhost:2379"}, 5*time.Second)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()

		// Grant lease
		leaseID, err := client.GrantLease(ctx, 10) // 10 second lease
		assert.NoError(t, err, "GrantLease should succeed")
		assert.NotZero(t, leaseID, "LeaseID should be non-zero")

		// Renew lease
		err = client.KeepAliveOnce(ctx, leaseID)
		assert.NoError(t, err, "KeepAliveOnce should succeed")

		// Revoke lease
		err = client.RevokeLease(ctx, leaseID)
		assert.NoError(t, err, "RevokeLease should succeed")
	})

	t.Run("PutWithLease auto-deletes on lease expiration", func(t *testing.T) {
		client, err := etcd.NewETCDClient([]string{"localhost:2379"}, 5*time.Second)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		key := "test/lease/key1"
		value := "auto-delete-me"

		// Grant short lease
		leaseID, err := client.GrantLease(ctx, 2) // 2 seconds
		require.NoError(t, err)

		// Put with lease
		err = client.PutWithLease(ctx, key, value, leaseID)
		assert.NoError(t, err, "PutWithLease should succeed")

		// Verify exists
		retrieved, err := client.Get(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, value, retrieved)

		// Wait for lease to expire
		time.Sleep(3 * time.Second)

		// Verify auto-deleted
		retrieved, err = client.Get(ctx, key)
		assert.NoError(t, err)
		assert.Empty(t, retrieved, "Key should be auto-deleted after lease expiry")
	})

	t.Run("PutWithoutLease persists after lease expiry", func(t *testing.T) {
		client, err := etcd.NewETCDClient([]string{"localhost:2379"}, 5*time.Second)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		key := "test/persist/key1"
		value := "permanent-job"

		// Grant short lease
		leaseID, err := client.GrantLease(ctx, 2)
		require.NoError(t, err)

		// Put with lease initially
		err = client.PutWithLease(ctx, key, "temporary", leaseID)
		require.NoError(t, err)

		// Overwrite WITHOUT lease (persist completed job)
		err = client.PutWithoutLease(ctx, key, value)
		assert.NoError(t, err, "PutWithoutLease should succeed")

		// Wait for original lease to expire
		time.Sleep(3 * time.Second)

		// Verify STILL exists (not deleted)
		retrieved, err := client.Get(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, value, retrieved, "Key should persist after lease expiry")

		// Cleanup
		client.Delete(ctx, key)
	})
}

// TestETCDClientCAS tests Compare-And-Swap (exactly-once guarantees)
func TestETCDClientCAS(t *testing.T) {
	t.Run("CAS succeeds when version matches", func(t *testing.T) {
		client, err := etcd.NewETCDClient([]string{"localhost:2379"}, 5*time.Second)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		key := "test/cas/key1"

		// CAS on non-existent key (version 0)
		success, err := client.CAS(ctx, key, 0, "first-value")
		assert.NoError(t, err)
		assert.True(t, success, "CAS should succeed on non-existent key")

		// Verify value set
		retrieved, err := client.Get(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, "first-value", retrieved)

		// Cleanup
		client.Delete(ctx, key)
	})

	t.Run("CAS fails when version doesn't match", func(t *testing.T) {
		client, err := etcd.NewETCDClient([]string{"localhost:2379"}, 5*time.Second)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		key := "test/cas/key2"

		// Set initial value
		client.Put(ctx, key, "existing-value")

		// Try CAS with wrong version (expecting version 0, but key exists)
		success, err := client.CAS(ctx, key, 0, "should-fail")
		assert.NoError(t, err)
		assert.False(t, success, "CAS should fail when version doesn't match")

		// Verify original value unchanged
		retrieved, err := client.Get(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, "existing-value", retrieved)

		// Cleanup
		client.Delete(ctx, key)
	})
}

// TestETCDClientLeaseCAS tests lease-based CAS (exactly-once job execution)
func TestETCDClientLeaseCAS(t *testing.T) {
	t.Run("LeaseCAS succeeds on first attempt", func(t *testing.T) {
		client, err := etcd.NewETCDClient([]string{"localhost:2379"}, 5*time.Second)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		key := "test/leasecas/job1"
		value := "executor-123"

		// Grant lease
		leaseID, err := client.GrantLease(ctx, 10)
		require.NoError(t, err)

		// First executor tries to claim job
		success, err := client.LeaseCAS(ctx, key, value, leaseID)
		assert.NoError(t, err)
		assert.True(t, success, "First LeaseCAS should succeed")

		// Verify claimed
		retrieved, err := client.Get(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, value, retrieved)

		// Cleanup
		client.RevokeLease(ctx, leaseID)
	})

	t.Run("LeaseCAS fails on second attempt (exactly-once)", func(t *testing.T) {
		client, err := etcd.NewETCDClient([]string{"localhost:2379"}, 5*time.Second)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		key := "test/leasecas/job2"

		// Executor 1 claims job
		lease1, err := client.GrantLease(ctx, 10)
		require.NoError(t, err)
		success1, err := client.LeaseCAS(ctx, key, "executor-1", lease1)
		require.NoError(t, err)
		require.True(t, success1, "Executor 1 should claim job")

		// Executor 2 tries to claim SAME job
		lease2, err := client.GrantLease(ctx, 10)
		require.NoError(t, err)
		success2, err := client.LeaseCAS(ctx, key, "executor-2", lease2)
		assert.NoError(t, err)
		assert.False(t, success2, "Executor 2 should FAIL to claim job (exactly-once)")

		// Verify Executor 1 still owns job
		retrieved, err := client.Get(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, "executor-1", retrieved)

		// Cleanup
		client.RevokeLease(ctx, lease1)
		client.RevokeLease(ctx, lease2)
	})
}

// TestETCDClientPrefixOperations tests GetAll and DeleteWithPrefix
func TestETCDClientPrefixOperations(t *testing.T) {
	t.Run("GetAll with prefix", func(t *testing.T) {
		client, err := etcd.NewETCDClient([]string{"localhost:2379"}, 5*time.Second)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		prefix := "test/prefix/"

		// Put multiple keys with same prefix
		client.Put(ctx, prefix+"key1", "value1")
		client.Put(ctx, prefix+"key2", "value2")
		client.Put(ctx, prefix+"key3", "value3")

		// Get all with prefix
		results, err := client.GetAll(ctx, prefix)
		assert.NoError(t, err)
		assert.Len(t, results, 3, "Should retrieve 3 keys")
		assert.Equal(t, "value1", results[prefix+"key1"])
		assert.Equal(t, "value2", results[prefix+"key2"])
		assert.Equal(t, "value3", results[prefix+"key3"])

		// Cleanup
		client.DeleteWithPrefix(ctx, prefix)
	})

	t.Run("DeleteWithPrefix removes all keys", func(t *testing.T) {
		client, err := etcd.NewETCDClient([]string{"localhost:2379"}, 5*time.Second)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		prefix := "test/deleteprefix/"

		// Put multiple keys
		client.Put(ctx, prefix+"key1", "value1")
		client.Put(ctx, prefix+"key2", "value2")

		// Delete all
		err = client.DeleteWithPrefix(ctx, prefix)
		assert.NoError(t, err)

		// Verify all deleted
		results, err := client.GetAll(ctx, prefix)
		assert.NoError(t, err)
		assert.Empty(t, results, "All keys should be deleted")
	})
}

// TestETCDClientExists tests existence checking
func TestETCDClientExists(t *testing.T) {
	client, err := etcd.NewETCDClient([]string{"localhost:2379"}, 5*time.Second)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	key := "test/exists/key1"

	// Key doesn't exist initially
	exists, err := client.Exists(ctx, key)
	assert.NoError(t, err)
	assert.False(t, exists)

	// Create key
	client.Put(ctx, key, "value")

	// Key now exists
	exists, err = client.Exists(ctx, key)
	assert.NoError(t, err)
	assert.True(t, exists)

	// Cleanup
	client.Delete(ctx, key)
}

// TestETCDClientWatch tests watch functionality (for heartbeat monitoring)
func TestETCDClientWatch(t *testing.T) {
	t.Run("Watch detects Put events", func(t *testing.T) {
		client, err := etcd.NewETCDClient([]string{"localhost:2379"}, 5*time.Second)
		require.NoError(t, err)
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		key := "test/watch/key1"

		// Start watching
		watchChan := client.Watch(ctx, key)

		// Put value in background
		go func() {
			time.Sleep(500 * time.Millisecond)
			client.Put(context.Background(), key, "watched-value")
		}()

		// Wait for watch event
		select {
		case watchResp := <-watchChan:
			assert.NotNil(t, watchResp, "Should receive watch event")
			assert.Len(t, watchResp.Events, 1, "Should have 1 event")
			event := watchResp.Events[0]
			assert.Equal(t, clientv3.EventTypePut, event.Type)
			assert.Equal(t, "watched-value", string(event.Kv.Value))
		case <-ctx.Done():
			t.Fatal("Watch timeout - no event received")
		}

		// Cleanup
		client.Delete(context.Background(), key)
	})
}

// TestETCDClientConnectionFailure tests error handling
func TestETCDClientConnectionFailure(t *testing.T) {
	t.Run("Fails to connect to invalid endpoint", func(t *testing.T) {
		// Use a very short timeout to fail fast
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		client, err := etcd.NewETCDClient([]string{"invalid:9999"}, 1*time.Second)

		// Either immediate error on creation, or error on first operation
		if err == nil {
			// Try an operation - should timeout
			_, opErr := client.Get(ctx, "test")
			assert.Error(t, opErr, "Should fail when using invalid endpoint")
			client.Close()
		} else {
			// Connection failed immediately (expected)
			assert.Error(t, err, "Should fail to connect to invalid endpoint")
		}
	})
}
