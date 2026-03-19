package integration_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/cluster"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// CLUSTER LIFECYCLE INTEGRATION TESTS
// ============================================================================

// TestCluster_JoinAndQuery verifies a cluster can join and be queried in JOINING state.
func TestCluster_JoinAndQuery(t *testing.T) {
	skipIfNoInfra(t)

	rc := newRedisClient(t)
	cm := cluster.NewClusterManager(rc, nil)

	ctx := context.Background()
	cfg := testClusterConfig("cl-join-1", "us-west")

	c, err := cm.JoinCluster(ctx, cfg)
	require.NoError(t, err)
	require.NotNil(t, c)

	assert.Equal(t, cluster.StateJoining, c.State, "new cluster should be in JOINING state")
	assert.True(t, c.IsHealthy)

	// Query it back
	queried, err := cm.GetCluster(cfg.ClusterID)
	require.NoError(t, err)
	assert.Equal(t, cfg.ClusterID, queried.ClusterID)
	assert.Equal(t, "us-west", queried.Region)

	t.Cleanup(func() {
		cm.LeaveCluster(ctx, cfg.ClusterID)
		cleanupRedisKeys(t, rc, "ares:cluster:"+cfg.ClusterID)
	})
}

// TestCluster_HeartbeatTransitionsToReady verifies that a capacity heartbeat
// transitions a JOINING cluster to READY.
func TestCluster_HeartbeatTransitionsToReady(t *testing.T) {
	skipIfNoInfra(t)

	rc := newRedisClient(t)
	cm := cluster.NewClusterManager(rc, nil)
	ctx := context.Background()

	cfg := testClusterConfig("cl-hb-1", "eu-west")
	c, err := cm.JoinCluster(ctx, cfg)
	require.NoError(t, err)
	assert.Equal(t, cluster.StateJoining, c.State)

	// Send first capacity heartbeat with GPUs
	err = cm.UpdateClusterCapacity(ctx, cfg.ClusterID, 8, 64, 512.0)
	require.NoError(t, err)

	// Cluster should now be READY
	c, err = cm.GetCluster(cfg.ClusterID)
	require.NoError(t, err)
	assert.Equal(t, cluster.StateReady, c.State, "cluster should transition to READY after capacity heartbeat with GPUs")
	assert.Equal(t, 8, c.TotalGPUs)

	t.Cleanup(func() {
		cm.LeaveCluster(ctx, cfg.ClusterID)
		cleanupRedisKeys(t, rc, "ares:cluster:"+cfg.ClusterID)
	})
}

// TestCluster_HeartbeatTimeoutAndRecovery verifies that stale heartbeats mark
// a cluster unhealthy and that a fresh heartbeat recovers it.
func TestCluster_HeartbeatTimeoutAndRecovery(t *testing.T) {
	skipIfNoInfra(t)

	rc := newRedisClient(t)
	// Use a very short heartbeat timeout for test speed.
	// The watchdog ticks every 10s by default, so we start it and wait for the first tick.
	shortTimeout := 200 * time.Millisecond
	shortCfg := &cluster.ClusterConfig{
		ClusterID:        "cl-timeout-1",
		Name:             "timeout-test",
		Region:           "us-east",
		Zone:             "us-east-1a",
		HeartbeatTimeout: shortTimeout,
	}
	cm := cluster.NewClusterManager(rc, shortCfg)
	ctx := context.Background()

	_, err := cm.JoinCluster(ctx, shortCfg)
	require.NoError(t, err)

	// Make cluster READY
	require.NoError(t, cm.UpdateClusterCapacity(ctx, shortCfg.ClusterID, 4, 32, 256.0))

	// Wait for heartbeat to go stale (must exceed shortTimeout)
	time.Sleep(400 * time.Millisecond)

	// StartHealthWatchdog ticks every 10s which is too slow.
	// Instead, just start the watchdog and wait for one full tick cycle.
	watchCtx, watchCancel := context.WithCancel(ctx)
	go cm.StartHealthWatchdog(watchCtx, shortTimeout)

	// The watchdog ticker is 10s, so we poll the cluster health state.
	ok := pollUntil(t, 15*time.Second, 500*time.Millisecond, func() bool {
		c, err := cm.GetCluster(shortCfg.ClusterID)
		if err != nil {
			return false
		}
		return !c.IsHealthy
	})
	watchCancel()
	assert.True(t, ok, "cluster should be unhealthy after heartbeat timeout")

	// Recovery: fresh heartbeat
	cm.UpdateHeartbeatTimestamp(shortCfg.ClusterID)

	c, err := cm.GetCluster(shortCfg.ClusterID)
	require.NoError(t, err)
	assert.True(t, c.IsHealthy, "cluster should recover after fresh heartbeat")

	t.Cleanup(func() {
		cm.LeaveCluster(ctx, shortCfg.ClusterID)
		cleanupRedisKeys(t, rc, "ares:cluster:"+shortCfg.ClusterID)
	})
}

// TestCluster_GlobalSchedulerReceivesJoinEvent verifies that registering a
// ClusterEventListener receives the join event when a cluster joins.
func TestCluster_GlobalSchedulerReceivesJoinEvent(t *testing.T) {
	skipIfNoInfra(t)

	rc := newRedisClient(t)
	cm := cluster.NewClusterManager(rc, nil)
	ctx := context.Background()

	var joinReceived int32
	listener := &mockClusterEventListener{
		onJoin: func(_ context.Context, c *cluster.Cluster) error {
			atomic.AddInt32(&joinReceived, 1)
			return nil
		},
	}
	cm.RegisterEventListener(listener)

	cfg := testClusterConfig("cl-event-1", "ap-south")
	_, err := cm.JoinCluster(ctx, cfg)
	require.NoError(t, err)

	// Events fire in goroutines — poll briefly
	ok := pollUntil(t, 2*time.Second, 50*time.Millisecond, func() bool {
		return atomic.LoadInt32(&joinReceived) > 0
	})
	assert.True(t, ok, "listener should have received join event")

	t.Cleanup(func() {
		cm.LeaveCluster(ctx, cfg.ClusterID)
		cleanupRedisKeys(t, rc, "ares:cluster:"+cfg.ClusterID)
	})
}

// TestCluster_LeaveRemovesFromManager verifies that LeaveCluster removes
// the cluster from the manager's registry.
func TestCluster_LeaveRemovesFromManager(t *testing.T) {
	skipIfNoInfra(t)

	rc := newRedisClient(t)
	cm := cluster.NewClusterManager(rc, nil)
	ctx := context.Background()

	cfg := testClusterConfig("cl-leave-1", "us-west")
	_, err := cm.JoinCluster(ctx, cfg)
	require.NoError(t, err)

	assert.Equal(t, 1, cm.CountClusters())

	err = cm.LeaveCluster(ctx, cfg.ClusterID)
	require.NoError(t, err)

	assert.Equal(t, 0, cm.CountClusters())

	_, err = cm.GetCluster(cfg.ClusterID)
	assert.Error(t, err, "cluster should not be found after leave")

	t.Cleanup(func() { cleanupRedisKeys(t, rc, "ares:cluster:"+cfg.ClusterID) })
}

// TestCluster_MultiFederationStats verifies stats aggregation across multiple clusters.
func TestCluster_MultiFederationStats(t *testing.T) {
	skipIfNoInfra(t)

	rc := newRedisClient(t)
	cm := cluster.NewClusterManager(rc, nil)
	ctx := context.Background()

	// Join 3 clusters
	configs := []*cluster.ClusterConfig{
		testClusterConfig("cl-fed-1", "us-west"),
		testClusterConfig("cl-fed-2", "eu-west"),
		testClusterConfig("cl-fed-3", "ap-south"),
	}

	for _, cfg := range configs {
		_, err := cm.JoinCluster(ctx, cfg)
		require.NoError(t, err)
	}

	// Set capacity
	require.NoError(t, cm.UpdateClusterCapacity(ctx, "cl-fed-1", 8, 64, 512.0))
	require.NoError(t, cm.UpdateClusterCapacity(ctx, "cl-fed-2", 4, 32, 256.0))
	require.NoError(t, cm.UpdateClusterCapacity(ctx, "cl-fed-3", 16, 128, 1024.0))

	// Set some load
	require.NoError(t, cm.UpdateClusterLoad(ctx, "cl-fed-1", 2, 8, 64.0, 2, 0))
	require.NoError(t, cm.UpdateClusterLoad(ctx, "cl-fed-3", 8, 32, 256.0, 4, 1))

	stats := cm.GetClusterStats()
	assert.Equal(t, 3, stats["total_clusters"])
	assert.Equal(t, 28, stats["total_gpus"])       // 8+4+16
	assert.Equal(t, 10, stats["gpus_in_use"])       // 2+0+8
	assert.Equal(t, 3, stats["healthy_clusters"])

	t.Cleanup(func() {
		for _, cfg := range configs {
			cm.LeaveCluster(ctx, cfg.ClusterID)
			cleanupRedisKeys(t, rc, "ares:cluster:"+cfg.ClusterID)
		}
	})
}

// ============================================================================
// MOCK EVENT LISTENER
// ============================================================================

type mockClusterEventListener struct {
	onJoin        func(ctx context.Context, c *cluster.Cluster) error
	onLeave       func(ctx context.Context, clusterID string) error
	onHealthChange func(ctx context.Context, clusterID string, health *cluster.ClusterHealth) error
	onStateChange func(ctx context.Context, clusterID string, oldState, newState cluster.ClusterState) error
}

func (m *mockClusterEventListener) OnClusterJoin(ctx context.Context, c *cluster.Cluster) error {
	if m.onJoin != nil {
		return m.onJoin(ctx, c)
	}
	return nil
}

func (m *mockClusterEventListener) OnClusterLeave(ctx context.Context, clusterID string) error {
	if m.onLeave != nil {
		return m.onLeave(ctx, clusterID)
	}
	return nil
}

func (m *mockClusterEventListener) OnClusterHealthChange(ctx context.Context, clusterID string, health *cluster.ClusterHealth) error {
	if m.onHealthChange != nil {
		return m.onHealthChange(ctx, clusterID, health)
	}
	return nil
}

func (m *mockClusterEventListener) OnClusterStateChange(ctx context.Context, clusterID string, oldState, newState cluster.ClusterState) error {
	if m.onStateChange != nil {
		return m.onStateChange(ctx, clusterID, oldState, newState)
	}
	return nil
}
