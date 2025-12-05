package unit

import (
	"context"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/etcd"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/redis"
	"testing"
	"time"
)

// ============================================================================
// LAYER 2: STORAGE CLIENTS TESTS
// ============================================================================
// Tests etcd and Redis clients - foundation for persistence and caching

// NOTE: These tests require etcd and Redis running locally
// Start with: docker-compose up etcd redis
// Or skip these tests if containers unavailable

// ============================================================================
// ETCD CLIENT TESTS
// ============================================================================

func TestETCDConnectionSkipCI(t *testing.T) {
	//t.Skip("Requires etcd running - run locally with: docker-compose up etcd")

	endpoints := []string{"localhost:2379"}
	timeout := 5 * time.Second

	client, err := etcd.NewETCDClient(endpoints, timeout)
	if err != nil {
		t.Fatalf("Failed to connect to etcd: %v", err)
	}
	defer client.Close()

	t.Log("✓ etcd connection successful")
}

func TestETCDPutGet(t *testing.T) {
	//t.Skip("Requires etcd running")

	endpoints := []string{"localhost:2379"}
	client, err := etcd.NewETCDClient(endpoints, 5*time.Second)
	if err != nil {
		t.Skipf("etcd not available: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tests := []struct {
		name  string
		key   string
		value string
	}{
		{"simple", "test:key", "value"},
		{"job", "ares:jobs:job-123", `{"id":"job-123","status":"running"}`},
		{"config", "ares:config:scheduler", "max_concurrent=100"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Put
			err := client.Put(ctx, tt.key, tt.value)
			if err != nil {
				t.Fatalf("Put failed: %v", err)
			}

			// Get
			retrieved, err := client.Get(ctx, tt.key)
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if retrieved != tt.value {
				t.Fatalf("Expected %s, got %s", tt.value, retrieved)
			}
		})
	}
}

func TestETCDDelete(t *testing.T) {
	//t.Skip("Requires etcd running")

	endpoints := []string{"localhost:2379"}
	client, err := etcd.NewETCDClient(endpoints, 5*time.Second)
	if err != nil {
		t.Skipf("etcd not available: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	key := "test:delete:key"
	value := "test-value"

	// Put
	client.Put(ctx, key, value)

	// Verify exists
	retrieved, _ := client.Get(ctx, key)
	if retrieved == "" {
		t.Fatal("Key should exist after put")
	}

	// Delete
	err = client.Delete(ctx, key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deleted
	retrieved, _ = client.Get(ctx, key)
	if retrieved != "" {
		t.Fatal("Key should be deleted")
	}
}

func TestETCDLease(t *testing.T) {
	//t.Skip("Requires etcd running")

	endpoints := []string{"localhost:2379"}
	client, err := etcd.NewETCDClient(endpoints, 5*time.Second)
	if err != nil {
		t.Skipf("etcd not available: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Grant lease
	leaseID, err := client.GrantLease(ctx, 2)
	if err != nil {
		t.Fatalf("GrantLease failed: %v", err)
	}

	if leaseID == 0 {
		t.Fatal("Lease ID should not be 0")
	}

	// Revoke lease
	err = client.RevokeLease(ctx, leaseID)
	if err != nil {
		t.Fatalf("RevokeLease failed: %v", err)
	}

	t.Log("✓ Lease operations successful")
}

func TestETCDCAS(t *testing.T) {
	//t.Skip("Requires etcd running")

	endpoints := []string{"localhost:2379"}
	client, err := etcd.NewETCDClient(endpoints, 5*time.Second)
	if err != nil {
		t.Skipf("etcd not available: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	key := "test:cas:key"

	// CAS on non-existent key should succeed (version 0)
	success, err := client.CAS(ctx, key, 0, "value1")
	if err != nil || !success {
		t.Fatalf("CAS on non-existent key should succeed")
	}

	// CAS with wrong version should fail
	success, err = client.CAS(ctx, key, 999, "value2")
	if err != nil || success {
		t.Fatalf("CAS with wrong version should fail")
	}

	t.Log("✓ CAS (Compare-And-Swap) operations successful")
}

// ============================================================================
// REDIS CLIENT TESTS
// ============================================================================

func TestRedisConnectionSkipCI(t *testing.T) {
	//t.Skip("Requires Redis running - run locally with: docker-compose up redis")

	client, err := redis.NewRedisClient("localhost:6379", "", 0)
	if err != nil {
		t.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer client.Close()

	t.Log("✓ Redis connection successful")
}

func TestRedisSetGet(t *testing.T) {
	//t.Skip("Requires Redis running")

	client, err := redis.NewRedisClient("localhost:6379", "", 0)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tests := []struct {
		name  string
		key   string
		value string
		ttl   time.Duration
	}{
		{"simple", "cache:key", "value", 1 * time.Hour},
		{"gpu-info", "ares:node:gpu:devices", `[{"index":0,"type":"A100"}]`, 30 * time.Second},
		{"topology", "ares:node:gpu:topology", `{"nvlink":[[0,1]]}`, 60 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set
			err := client.Set(ctx, tt.key, tt.value, tt.ttl)
			if err != nil {
				t.Fatalf("Set failed: %v", err)
			}

			// Get
			retrieved, err := client.Get(ctx, tt.key)
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if retrieved != tt.value {
				t.Fatalf("Expected %s, got %s", tt.value, retrieved)
			}
		})
	}
}

func TestRedisDel(t *testing.T) {
	//t.Skip("Requires Redis running")

	client, err := redis.NewRedisClient("localhost:6379", "", 0)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	key := "test:del:key"
	client.Set(ctx, key, "value", 1*time.Hour)

	// Delete
	err = client.Del(ctx, key)
	if err != nil {
		t.Fatalf("Del failed: %v", err)
	}

	// Verify deleted
	exists, _ := client.Exists(ctx, key)
	if exists {
		t.Fatal("Key should be deleted")
	}

	t.Log("✓ Redis delete successful")
}

func TestRedisSortedSet(t *testing.T) {
	//t.Skip("Requires Redis running")

	client, err := redis.NewRedisClient("localhost:6379", "", 0)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	key := "queue:pending"

	// Add jobs with different priorities
	members := []struct {
		member string
		score  float64
	}{
		{"job-1:priority-50", -50.0},
		{"job-2:priority-90", -90.0},
		{"job-3:priority-30", -30.0},
	}

	for _, m := range members {
		err := client.ZAdd(ctx, key, m.member, m.score)
		if err != nil {
			t.Fatalf("ZAdd failed: %v", err)
		}
	}

	// Pop minimum (highest priority = -90)
	result, err := client.ZPopMin(ctx, key, 1)
	if err != nil {
		t.Fatalf("ZPopMin failed: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(result))
	}

	// Should get job-2 (highest priority = -90)
	found := false
	for member := range result {
		if member == "job-2:priority-90" {
			found = true
		}
	}

	if !found {
		t.Fatal("Should have popped highest priority job")
	}

	t.Log("✓ Redis sorted set operations successful")
}

func TestRedisSetNX(t *testing.T) {
	//t.Skip("Requires Redis running")

	client, err := redis.NewRedisClient("localhost:6379", "", 0)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	key := "test:setnx:key"

	// First SetNX should succeed
	ok, err := client.SetNX(ctx, key, "value1", 1*time.Hour)
	if err != nil || !ok {
		t.Fatalf("First SetNX should succeed")
	}

	// Second SetNX should fail (key exists)
	ok, err = client.SetNX(ctx, key, "value2", 1*time.Hour)
	if err != nil || ok {
		t.Fatalf("Second SetNX should fail")
	}

	t.Log("✓ Redis SetNX (atomic) successful")
}

// ============================================================================
// STORAGE CLIENT INTEGRATION TEST
// ============================================================================

func TestStorageIntegration(t *testing.T) {
	//t.Skip("Requires etcd and Redis running")

	logger.Init("info")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Connect to both
	etcdClient, err := etcd.NewETCDClient([]string{"localhost:2379"}, 5*time.Second)
	if err != nil {
		t.Skipf("etcd not available: %v", err)
	}
	defer etcdClient.Close()

	redisClient, err := redis.NewRedisClient("localhost:6379", "", 0)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer redisClient.Close()

	// Simulate a job workflow
	jobID := "job-12345"

	// Store in etcd
	jobJSON := `{"id":"job-12345","status":"pending","priority":95}`
	etcdClient.Put(ctx, fmt.Sprintf("ares:jobs:%s", jobID), jobJSON)

	// Cache in Redis
	redisClient.Set(ctx, fmt.Sprintf("cache:job:%s", jobID), jobJSON, 1*time.Hour)

	// Retrieve from both
	etcdVal, _ := etcdClient.Get(ctx, fmt.Sprintf("ares:jobs:%s", jobID))
	redisVal, _ := redisClient.Get(ctx, fmt.Sprintf("cache:job:%s", jobID))

	if etcdVal != jobJSON || redisVal != jobJSON {
		t.Fatal("Job data mismatch between etcd and Redis")
	}

	t.Log("✓ Storage integration test passed")
}

// ============================================================================
// LAYER 2 TEST SUMMARY
// ============================================================================

func TestLayer2StorageClientsSummary(t *testing.T) {
	t.Run("etcd Connection", TestETCDConnectionSkipCI)
	t.Run("etcd Put/Get", TestETCDPutGet)
	t.Run("etcd Delete", TestETCDDelete)
	t.Run("etcd Lease", TestETCDLease)
	t.Run("etcd CAS", TestETCDCAS)
	t.Run("Redis Connection", TestRedisConnectionSkipCI)
	t.Run("Redis Set/Get", TestRedisSetGet)
	t.Run("Redis Del", TestRedisDel)
	t.Run("Redis Sorted Set", TestRedisSortedSet)
	t.Run("Redis SetNX", TestRedisSetNX)
	t.Run("Storage Integration", TestStorageIntegration)

	t.Log("✓ LAYER 2: Storage Clients - All tests passed (skipped if services unavailable)")
}
