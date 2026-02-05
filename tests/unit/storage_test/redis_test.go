package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRedisClientBasicOperations tests Set, Get, Del
func TestRedisClientBasicOperations(t *testing.T) {
	t.Run("Set and Get", func(t *testing.T) {
		client, err := redis.NewRedisClient("localhost:6379", "", 0)
		require.NoError(t, err, "Failed to create Redis client")
		defer client.Close()

		ctx := context.Background()
		key := "test:basic:key1"
		value := "test-value-123"

		// Test Set
		err = client.Set(ctx, key, value, 0) // No TTL
		assert.NoError(t, err, "Set should succeed")

		// Test Get
		retrieved, err := client.Get(ctx, key)
		assert.NoError(t, err, "Get should succeed")
		assert.Equal(t, value, retrieved, "Retrieved value should match")

		// Cleanup
		client.Del(ctx, key)
	})

	t.Run("Get non-existent key", func(t *testing.T) {
		client, err := redis.NewRedisClient("localhost:6379", "", 0)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		key := "test:nonexistent:key999"

		retrieved, err := client.Get(ctx, key)
		assert.NoError(t, err, "Get should not error on missing key")
		assert.Empty(t, retrieved, "Retrieved value should be empty")
	})

	t.Run("Set with TTL expires", func(t *testing.T) {
		client, err := redis.NewRedisClient("localhost:6379", "", 0)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		key := "test:ttl:key1"
		value := "expires-soon"
		ttl := 2 * time.Second

		// Set with TTL
		err = client.Set(ctx, key, value, ttl)
		assert.NoError(t, err, "Set with TTL should succeed")

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

	t.Run("Del removes key", func(t *testing.T) {
		client, err := redis.NewRedisClient("localhost:6379", "", 0)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		key := "test:del:key1"
		value := "to-be-deleted"

		// Set then delete
		client.Set(ctx, key, value, 0)
		err = client.Del(ctx, key)
		assert.NoError(t, err, "Del should succeed")

		// Verify deleted
		retrieved, err := client.Get(ctx, key)
		assert.NoError(t, err)
		assert.Empty(t, retrieved, "Key should be gone")
	})
}

// TestRedisClientExists tests existence checking
func TestRedisClientExists(t *testing.T) {
	client, err := redis.NewRedisClient("localhost:6379", "", 0)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	key := "test:exists:key1"

	// Key doesn't exist initially
	exists, err := client.Exists(ctx, key)
	assert.NoError(t, err)
	assert.False(t, exists)

	// Create key
	client.Set(ctx, key, "value", 0)

	// Key now exists
	exists, err = client.Exists(ctx, key)
	assert.NoError(t, err)
	assert.True(t, exists)

	// Cleanup
	client.Del(ctx, key)
}

// TestRedisClientSortedSetOperations tests ZAdd, ZPopMin (priority queue)
func TestRedisClientSortedSetOperations(t *testing.T) {
	t.Run("ZAdd and ZPopMin (priority queue)", func(t *testing.T) {
		client, err := redis.NewRedisClient("localhost:6379", "", 0)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		queueKey := "test:queue:priority1"

		// Add jobs with different priorities
		// Lower score = higher priority
		err = client.ZAdd(ctx, queueKey, "job-high-priority", -100.0)
		assert.NoError(t, err)
		err = client.ZAdd(ctx, queueKey, "job-medium-priority", -50.0)
		assert.NoError(t, err)
		err = client.ZAdd(ctx, queueKey, "job-low-priority", -10.0)
		assert.NoError(t, err)

		// Pop highest priority job (minimum score)
		results, err := client.ZPopMin(ctx, queueKey, 1)
		assert.NoError(t, err)
		assert.Len(t, results, 1)

		// Should get high priority job first
		for member, score := range results {
			assert.Equal(t, "job-high-priority", member)
			assert.Equal(t, -100.0, score)
		}

		// Pop next job
		results, err = client.ZPopMin(ctx, queueKey, 1)
		assert.NoError(t, err)
		for member := range results {
			assert.Equal(t, "job-medium-priority", member)
		}

		// Cleanup
		client.Del(ctx, queueKey)
	})

	t.Run("ZCard counts members", func(t *testing.T) {
		client, err := redis.NewRedisClient("localhost:6379", "", 0)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		queueKey := "test:queue:count1"

		// Add 3 jobs
		client.ZAdd(ctx, queueKey, "job1", 1.0)
		client.ZAdd(ctx, queueKey, "job2", 2.0)
		client.ZAdd(ctx, queueKey, "job3", 3.0)

		// Count
		count, err := client.ZCard(ctx, queueKey)
		assert.NoError(t, err)
		assert.Equal(t, 3, count)

		// Cleanup
		client.Del(ctx, queueKey)
	})

	t.Run("ZRange retrieves jobs in order", func(t *testing.T) {
		client, err := redis.NewRedisClient("localhost:6379", "", 0)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		queueKey := "test:queue:range1"

		// Add jobs
		client.ZAdd(ctx, queueKey, "job-z", 30.0)
		client.ZAdd(ctx, queueKey, "job-a", 10.0)
		client.ZAdd(ctx, queueKey, "job-m", 20.0)

		// Get all jobs (sorted by score)
		results, err := client.ZRange(ctx, queueKey, 0, -1)
		assert.NoError(t, err)
		assert.Len(t, results, 3)

		// Should be sorted by score
		assert.Equal(t, "job-a", results[0])
		assert.Equal(t, "job-m", results[1])
		assert.Equal(t, "job-z", results[2])

		// Cleanup
		client.Del(ctx, queueKey)
	})

	t.Run("ZRem removes member", func(t *testing.T) {
		client, err := redis.NewRedisClient("localhost:6379", "", 0)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		queueKey := "test:queue:rem1"

		// Add jobs
		client.ZAdd(ctx, queueKey, "job1", 1.0)
		client.ZAdd(ctx, queueKey, "job2", 2.0)

		// Remove job1
		err = client.ZRem(ctx, queueKey, "job1")
		assert.NoError(t, err)

		// Verify only job2 remains
		count, _ := client.ZCard(ctx, queueKey)
		assert.Equal(t, 1, count)

		// Cleanup
		client.Del(ctx, queueKey)
	})
}

// TestRedisClientHashOperations tests HSet, HGet, HGetAll (job details)
func TestRedisClientHashOperations(t *testing.T) {
	t.Run("HSet and HGet", func(t *testing.T) {
		client, err := redis.NewRedisClient("localhost:6379", "", 0)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		hashKey := "test:job:123"

		// Set hash fields
		err = client.HSet(ctx, hashKey, "status", "RUNNING")
		assert.NoError(t, err)
		err = client.HSet(ctx, hashKey, "executor", "executor-abc")
		assert.NoError(t, err)

		// Get individual field
		status, err := client.HGet(ctx, hashKey, "status")
		assert.NoError(t, err)
		assert.Equal(t, "RUNNING", status)

		executor, err := client.HGet(ctx, hashKey, "executor")
		assert.NoError(t, err)
		assert.Equal(t, "executor-abc", executor)

		// Cleanup
		client.Del(ctx, hashKey)
	})

	t.Run("HGetAll retrieves all fields", func(t *testing.T) {
		client, err := redis.NewRedisClient("localhost:6379", "", 0)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		hashKey := "test:job:456"

		// Set multiple fields
		client.HSet(ctx, hashKey, "id", "job-456")
		client.HSet(ctx, hashKey, "status", "SUCCEEDED")
		client.HSet(ctx, hashKey, "result", "success-data")

		// Get all fields
		allFields, err := client.HGetAll(ctx, hashKey)
		assert.NoError(t, err)
		assert.Len(t, allFields, 3)
		assert.Equal(t, "job-456", allFields["id"])
		assert.Equal(t, "SUCCEEDED", allFields["status"])
		assert.Equal(t, "success-data", allFields["result"])

		// Cleanup
		client.Del(ctx, hashKey)
	})

	t.Run("HGet non-existent field", func(t *testing.T) {
		client, err := redis.NewRedisClient("localhost:6379", "", 0)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		hashKey := "test:job:789"

		// Set one field
		client.HSet(ctx, hashKey, "status", "PENDING")

		// Get non-existent field
		value, err := client.HGet(ctx, hashKey, "nonexistent")
		assert.NoError(t, err)
		assert.Empty(t, value)

		// Cleanup
		client.Del(ctx, hashKey)
	})
}

// TestRedisClientAtomicOperations tests SetNX, GetSet, Incr (idempotency)
func TestRedisClientAtomicOperations(t *testing.T) {
	t.Run("SetNX only sets if not exists (idempotency)", func(t *testing.T) {
		client, err := redis.NewRedisClient("localhost:6379", "", 0)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		key := "test:setnx:request1"
		value := "processed"

		// First SetNX should succeed
		ok, err := client.SetNX(ctx, key, value, 10*time.Second)
		assert.NoError(t, err)
		assert.True(t, ok, "First SetNX should succeed")

		// Second SetNX should fail (key exists)
		ok, err = client.SetNX(ctx, key, "duplicate", 10*time.Second)
		assert.NoError(t, err)
		assert.False(t, ok, "Second SetNX should fail (idempotency)")

		// Original value should be unchanged
		retrieved, _ := client.Get(ctx, key)
		assert.Equal(t, value, retrieved)

		// Cleanup
		client.Del(ctx, key)
	})

	t.Run("GetSet atomically gets and sets", func(t *testing.T) {
		client, err := redis.NewRedisClient("localhost:6379", "", 0)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		key := "test:getset:counter"

		// Set initial value
		client.Set(ctx, key, "old-value", 0)

		// GetSet returns old value and sets new
		oldValue, err := client.GetSet(ctx, key, "new-value")
		assert.NoError(t, err)
		assert.Equal(t, "old-value", oldValue)

		// Verify new value set
		currentValue, _ := client.Get(ctx, key)
		assert.Equal(t, "new-value", currentValue)

		// Cleanup
		client.Del(ctx, key)
	})

	t.Run("Incr atomically increments", func(t *testing.T) {
		client, err := redis.NewRedisClient("localhost:6379", "", 0)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		key := "test:incr:counter"

		// First increment (key doesn't exist, starts at 0)
		val1, err := client.Incr(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), val1)

		// Second increment
		val2, err := client.Incr(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, int64(2), val2)

		// Third increment
		val3, err := client.Incr(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, int64(3), val3)

		// Cleanup
		client.Del(ctx, key)
	})
}

// TestRedisClientKeyPatternOperations tests Keys, DeleteWithPattern
func TestRedisClientKeyPatternOperations(t *testing.T) {
	t.Run("Keys finds matching patterns", func(t *testing.T) {
		client, err := redis.NewRedisClient("localhost:6379", "", 0)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		prefix := "test:pattern:"

		// Create keys with pattern
		client.Set(ctx, prefix+"key1", "v1", 0)
		client.Set(ctx, prefix+"key2", "v2", 0)
		client.Set(ctx, prefix+"key3", "v3", 0)

		// Find all matching keys
		keys, err := client.Keys(ctx, prefix+"*")
		assert.NoError(t, err)
		assert.Len(t, keys, 3)

		// Cleanup
		client.Del(ctx, keys...)
	})

	t.Run("DeleteWithPattern removes all matching keys", func(t *testing.T) {
		client, err := redis.NewRedisClient("localhost:6379", "", 0)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		prefix := "test:deletepattern:"

		// Create keys
		client.Set(ctx, prefix+"key1", "v1", 0)
		client.Set(ctx, prefix+"key2", "v2", 0)

		// Delete all with pattern
		err = client.DeleteWithPattern(ctx, prefix+"*")
		assert.NoError(t, err)

		// Verify all deleted
		keys, _ := client.Keys(ctx, prefix+"*")
		assert.Empty(t, keys, "All keys should be deleted")
	})
}

// TestRedisClientExpirationOperations tests Expire, TTL
func TestRedisClientExpirationOperations(t *testing.T) {
	t.Run("Expire sets key expiration", func(t *testing.T) {
		client, err := redis.NewRedisClient("localhost:6379", "", 0)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		key := "test:expire:key1"

		// Set key without expiration
		client.Set(ctx, key, "value", 0)

		// Set expiration
		err = client.Expire(ctx, key, 3*time.Second)
		assert.NoError(t, err)

		// Key should exist now
		exists, _ := client.Exists(ctx, key)
		assert.True(t, exists)

		// Wait for expiration
		time.Sleep(4 * time.Second)

		// Key should be gone
		exists, _ = client.Exists(ctx, key)
		assert.False(t, exists)
	})

	t.Run("TTL returns remaining time", func(t *testing.T) {
		client, err := redis.NewRedisClient("localhost:6379", "", 0)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		key := "test:ttl:key1"

		// Set key with 10 second TTL
		client.Set(ctx, key, "value", 10*time.Second)

		// Check TTL
		ttl, err := client.TTL(ctx, key)
		assert.NoError(t, err)
		assert.Greater(t, ttl.Seconds(), 8.0, "TTL should be ~10 seconds")
		assert.LessOrEqual(t, ttl.Seconds(), 10.0)

		// Cleanup
		client.Del(ctx, key)
	})
}

// TestRedisClientConnectionFailure tests error handling
func TestRedisClientConnectionFailure(t *testing.T) {
	t.Run("Fails to connect to invalid endpoint", func(t *testing.T) {
		// Redis will connect lazily, so we need to test actual operation
		client, err := redis.NewRedisClient("invalid:9999", "", 0)

		// Either immediate error on creation, or error on first operation
		if err == nil {
			// Try an operation - should fail
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			_, opErr := client.Get(ctx, "test")
			assert.Error(t, opErr, "Should fail when using invalid endpoint")
			client.Close()
		} else {
			// Connection failed immediately (expected)
			assert.Error(t, err, "Should fail to connect to invalid endpoint")
		}
	})
}

// TestRedisClientConcurrentOperations tests thread-safety
func TestRedisClientConcurrentOperations(t *testing.T) {
	t.Run("Concurrent Incr is atomic", func(t *testing.T) {
		client, err := redis.NewRedisClient("localhost:6379", "", 0)
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()
		key := "test:concurrent:counter"

		// 10 goroutines increment 100 times each
		numGoroutines := 10
		incrementsPerGoroutine := 100
		//expectedTotal := int64(numGoroutines * incrementsPerGoroutine)

		done := make(chan bool, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				for j := 0; j < incrementsPerGoroutine; j++ {
					client.Incr(ctx, key)
				}
				done <- true
			}()
		}

		// Wait for all goroutines
		for i := 0; i < numGoroutines; i++ {
			<-done
		}

		// Verify final count
		finalValue, err := client.Get(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, "1000", finalValue) // 10 * 100 = 1000

		// Cleanup
		client.Del(ctx, key)
	})
}
