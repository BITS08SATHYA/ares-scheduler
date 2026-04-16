//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/cluster"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	etcdStore "github.com/BITS08SATHYA/ares-scheduler/pkg/storage/etcd"
	redisStore "github.com/BITS08SATHYA/ares-scheduler/pkg/storage/redis"
)

// ============================================================================
// INFRASTRUCTURE HELPERS
// ============================================================================

// skipIfNoInfra skips the test if Redis or etcd is not reachable.
func skipIfNoInfra(t *testing.T) {
	t.Helper()

	// Check Redis
	rc, err := redisStore.NewRedisClient("localhost:6379", "", 0)
	if err != nil {
		t.Skipf("Skipping: Redis not available at localhost:6379: %v", err)
	}
	rc.Close()

	// Check etcd
	ec, err := etcdStore.NewETCDClient([]string{"localhost:2379"}, 3*time.Second)
	if err != nil {
		t.Skipf("Skipping: etcd not available at localhost:2379: %v", err)
	}
	ec.Close()
}

// newRedisClient creates a Redis client for integration tests.
// Registers t.Cleanup to close the client automatically.
func newRedisClient(t *testing.T) *redisStore.RedisClient {
	t.Helper()
	rc, err := redisStore.NewRedisClient("localhost:6379", "", 0)
	if err != nil {
		t.Fatalf("Failed to connect to Redis: %v", err)
	}
	t.Cleanup(func() { rc.Close() })
	return rc
}

// newETCDClient creates an etcd client for integration tests.
// Registers t.Cleanup to close the client automatically.
func newETCDClient(t *testing.T) *etcdStore.ETCDClient {
	t.Helper()
	ec, err := etcdStore.NewETCDClient([]string{"localhost:2379"}, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to etcd: %v", err)
	}
	// NOTE: We intentionally skip ec.Close() in cleanup because etcd v3 client's
	// Close() blocks waiting for outstanding gRPC streams to finish, which can
	// cause test hangs. TestMain's os.Exit ensures the process terminates.
	return ec
}

// ============================================================================
// CLEANUP HELPERS
// ============================================================================

// testPrefix returns a unique key prefix for test isolation.
func testPrefix(t *testing.T, component string) string {
	return fmt.Sprintf("inttest-%s-%s-%d", t.Name(), component, time.Now().UnixNano())
}

// cleanupRedisKeys deletes all Redis keys matching the given pattern.
func cleanupRedisKeys(t *testing.T, client *redisStore.RedisClient, pattern string) {
	t.Helper()
	ctx := context.Background()
	if err := client.DeleteWithPattern(ctx, pattern); err != nil {
		t.Logf("Warning: Redis cleanup for %s failed: %v", pattern, err)
	}
}

// cleanupETCDPrefix deletes all etcd keys with the given prefix.
func cleanupETCDPrefix(t *testing.T, client *etcdStore.ETCDClient, prefix string) {
	t.Helper()
	ctx := context.Background()
	if err := client.DeleteWithPrefix(ctx, prefix); err != nil {
		t.Logf("Warning: etcd cleanup for %s failed: %v", prefix, err)
	}
}

// ============================================================================
// TEST FIXTURES
// ============================================================================

// testJobSpec creates a common.JobSpec fixture for integration tests.
func testJobSpec(requestID string) *common.JobSpec {
	return &common.JobSpec{
		RequestID:       requestID,
		Name:            "inttest-job-" + requestID,
		Image:           "nvidia/cuda:12.0",
		Command:         []string{"python", "train.py"},
		GPUCount:        2,
		GPUType:         "A100",
		MemoryMB:        8192,
		CPUMillis:       4000,
		Priority:        50,
		TimeoutSecs:     300,
		MaxRetries:      3,
		TenantID:        "tenant-inttest",
		QuotaGB:         100.0,
		TargetLatencyMs: 5000,
	}
}

// testClusterConfig creates a ClusterConfig fixture for integration tests.
func testClusterConfig(id, region string) *cluster.ClusterConfig {
	return &cluster.ClusterConfig{
		ClusterID:              id,
		Name:                   "inttest-cluster-" + id,
		Region:                 region,
		Zone:                   region + "-1a",
		ControlAddr:            fmt.Sprintf("http://localhost:808%s", id[len(id)-1:]),
		AutoHeartbeatInterval:  10 * time.Second,
		HealthCheckInterval:    30 * time.Second,
		HeartbeatTimeout:       60 * time.Second,
		AutonomyEnabled:        false,
		MaxConsecutiveFailures: 3,
		Labels:                 map[string]string{"env": "inttest"},
	}
}

// ============================================================================
// MOCK LOGGER (implements lease.Logger)
// ============================================================================

type testLogger struct{}

func (l *testLogger) Infof(format string, args ...interface{})  {}
func (l *testLogger) Warnf(format string, args ...interface{})  {}
func (l *testLogger) Errorf(format string, args ...interface{}) {}
func (l *testLogger) Debugf(format string, args ...interface{}) {}

// ============================================================================
// POLL HELPER
// ============================================================================

// pollUntil retries fn every interval until it returns true or timeout elapses.
func pollUntil(t *testing.T, timeout, interval time.Duration, fn func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(interval)
	}
	return false
}
