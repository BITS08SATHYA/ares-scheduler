//go:build integration

package integration_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/cluster"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/drf"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/global"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/local"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/preemption"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// SCHEDULING PIPELINE INTEGRATION TESTS
//
// NOTE: NewGlobalScheduler starts a gang scheduling goroutine that cannot be
// stopped (no Close method). We use os.Exit in TestMain to ensure the test
// process exits cleanly even with background goroutines.
// ============================================================================

func TestMain(m *testing.M) {
	code := m.Run()
	// Force exit to terminate any leaked goroutines (e.g., gang scheduling loop
	// from NewGlobalScheduler, gRPC keepalive goroutines from etcd client).
	os.Exit(code)
}

// setupGlobalScheduler creates a ClusterManager + GlobalScheduler with the given
// clusters already joined and made READY.
func setupGlobalScheduler(t *testing.T, clusters []clusterSetup) (*global.GlobalScheduler, *cluster.ClusterManager) {
	t.Helper()

	rc := newRedisClient(t)
	cm := cluster.NewClusterManager(rc, nil)
	ctx := context.Background()

	// Join clusters and set capacity BEFORE creating GlobalScheduler.
	// This way, gs.clusters cache is empty and SelectBestCluster uses
	// the fallback path through cm.GetClusterInfo (which has correct data).
	for _, cs := range clusters {
		cfg := &cluster.ClusterConfig{
			ClusterID:   cs.ID,
			Name:        cs.ID,
			Region:      cs.Region,
			Zone:        cs.Region + "-1a",
			ControlAddr: "http://localhost:8080",
		}
		_, err := cm.JoinCluster(ctx, cfg)
		require.NoError(t, err)

		// Set capacity and transition to READY
		require.NoError(t, cm.UpdateClusterCapacity(ctx, cs.ID, cs.GPUs, 64, float64(cs.GPUs)*80.0))
	}

	// Create GlobalScheduler after clusters are fully set up.
	// The existing clusters won't fire join events, so gs.clusters stays empty.
	// SelectBestCluster falls back to cm.GetClusterInfo for scoring.
	gs := global.NewGlobalScheduler("control-plane-inttest", rc, cm)

	t.Cleanup(func() {
		for _, cs := range clusters {
			cm.LeaveCluster(ctx, cs.ID)
			cleanupRedisKeys(t, rc, "ares:cluster:"+cs.ID)
		}
	})

	return gs, cm
}

type clusterSetup struct {
	ID      string
	Region  string
	GPUs    int
	GPUType string
}

// TestScheduling_SelectBestClusterPicksHighestScored verifies that SelectBestCluster
// picks the cluster with the most GPUs (which scores highest).
func TestScheduling_SelectBestClusterPicksHighestScored(t *testing.T) {
	skipIfNoInfra(t)

	gs, _ := setupGlobalScheduler(t, []clusterSetup{
		{ID: "cl-sched-a", Region: "us-west", GPUs: 4},
		{ID: "cl-sched-b", Region: "us-west", GPUs: 16},
		{ID: "cl-sched-c", Region: "us-west", GPUs: 8},
	})

	spec := testJobSpec("sched-best-cluster")
	spec.GPUCount = 2

	best, score, err := gs.SelectBestCluster(context.Background(), spec, "")
	require.NoError(t, err)
	require.NotNil(t, best)

	// Cluster B has 16 GPUs, should score highest
	assert.Equal(t, "cl-sched-b", best.ClusterID, "cluster with most GPUs should be selected")
	assert.Greater(t, score.Score, 0.0)
}

// TestScheduling_RegionPreferenceRouting verifies that a region preference
// biases cluster selection toward the preferred region.
func TestScheduling_RegionPreferenceRouting(t *testing.T) {
	skipIfNoInfra(t)

	// Both clusters have the same GPU count, so region preference (+40) determines the winner.
	// Score caps at 100, but the -5 penalty for non-preferred region makes us-west score lower.
	// With only 2 GPUs total: base(20) + GPU(2) + health(30) + freeCapacity(~30) = ~82
	// eu-west: +40 region → 100 (capped). us-west: -5 region → ~77.
	gs, _ := setupGlobalScheduler(t, []clusterSetup{
		{ID: "cl-region-us", Region: "us-west", GPUs: 2},
		{ID: "cl-region-eu", Region: "eu-west", GPUs: 2},
	})

	spec := testJobSpec("sched-region")
	spec.GPUCount = 2
	spec.MemoryMB = 1024 // Low memory to avoid score differences

	// Prefer eu-west — region bonus (+40) should tip it over us-west (-5)
	best, _, err := gs.SelectBestCluster(context.Background(), spec, "eu-west")
	require.NoError(t, err)
	require.NotNil(t, best)

	assert.Equal(t, "eu-west", best.Region, "region preference should route to eu-west")
}

// TestScheduling_LocalSelectBestNodePicksRichest verifies that LocalScheduler
// picks the node with more available GPUs.
func TestScheduling_LocalSelectBestNodePicksRichest(t *testing.T) {
	skipIfNoInfra(t)

	rc := newRedisClient(t)
	ls := local.NewLocalScheduler("cluster-local-test", rc, nil, nil)
	ctx := context.Background()

	// Register two nodes with different GPU counts
	require.NoError(t, ls.RegisterNode(ctx, &local.NodeState{
		NodeID:            "node-small",
		IsHealthy:         true,
		GPUCount:          4,
		AvailableGPUs:     2,
		MemoryGB:          320.0,
		AvailableMemoryGB: 160.0,
		LastHealthCheck:   time.Now(),
	}))
	require.NoError(t, ls.RegisterNode(ctx, &local.NodeState{
		NodeID:            "node-large",
		IsHealthy:         true,
		GPUCount:          8,
		AvailableGPUs:     6,
		MemoryGB:          640.0,
		AvailableMemoryGB: 480.0,
		LastHealthCheck:   time.Now(),
	}))

	spec := testJobSpec("local-best-node")
	spec.GPUCount = 2

	bestNode, score, err := ls.SelectBestNode(ctx, spec)
	require.NoError(t, err)
	require.NotNil(t, bestNode)

	assert.Equal(t, "node-large", bestNode.NodeID, "node with more available GPUs should score higher")
	assert.Greater(t, score.Score, 0.0)

	t.Cleanup(func() { cleanupRedisKeys(t, rc, "ares:cluster:cluster-local-test:*") })
}

// TestScheduling_DRFBlocksOverQuotaTenant verifies that a tenant exceeding their
// dominant resource share is blocked from scheduling.
func TestScheduling_DRFBlocksOverQuotaTenant(t *testing.T) {
	skipIfNoInfra(t)

	dm := drf.NewDRFManager(&drf.DRFConfig{
		GPUWeight:          3.0,
		CPUWeight:          1.0,
		MemoryWeight:       0.5,
		FairnessThreshold:  0.5, // 50% threshold
		DefaultTenantQuota: 0.5,
	})
	ctx := context.Background()

	// Set total capacity
	dm.UpdateCapacity(100, 1000, 1000.0)

	// Register tenant-B with a small allocation so tenant-A is NOT the lowest-share tenant.
	// Without this, DRF's "lowest share exception" always allows the only tenant.
	dm.OnJobScheduled("tenant-B", 5, 50, 25.0)

	// First allocation: 40 GPUs (40% of 100) → should be allowed
	decision := dm.CheckFairness(ctx, "tenant-A", 40, 100, 50.0)
	assert.True(t, decision.Allowed, "40% GPU share should be allowed")

	// Record the allocation
	dm.OnJobScheduled("tenant-A", 40, 100, 50.0)

	// Second allocation: 20 more GPUs (total 60%) → exceeds 50% threshold
	// tenant-B has 5% GPU share, so tenant-A is NOT the lowest → should be denied
	decision = dm.CheckFairness(ctx, "tenant-A", 20, 50, 25.0)
	assert.False(t, decision.Allowed, "60% GPU share should exceed 50% threshold")
}

// TestScheduling_PreemptionFindsLowestPriorityVictim verifies that the preemption
// manager selects the lowest-priority running job as the victim.
func TestScheduling_PreemptionFindsLowestPriorityVictim(t *testing.T) {
	skipIfNoInfra(t)

	pm := preemption.NewPreemptionManager(&preemption.PreemptionConfig{
		Enabled:                true,
		MinPriorityGap:         10,
		MaxPreemptionsPerHour:  100,
		MaxPreemptionsPerDay:   500,
		GracePeriodSec:         5,
		PreemptiblePriorityMax: 40,
		MinimumAgeSeconds:      0, // Disable age check for test
		CooldownPeriod:         0,
	})
	ctx := context.Background()

	// Incoming high-priority job
	incoming := &common.JobSpec{
		RequestID:       "incoming-job",
		Name:            "high-pri-job",
		Image:           "nvidia/cuda:12.0",
		GPUCount:        4,
		Priority:        80,
		TimeoutSecs:     300,
		MaxRetries:      0,
		TargetLatencyMs: 5000,
	}

	// Running jobs with different priorities
	running := []*common.Job{
		{
			ID:        "victim-low",
			Spec:      &common.JobSpec{RequestID: "v1", Name: "low-pri", Image: "img", GPUCount: 4, Priority: 20, TimeoutSecs: 300, MaxRetries: 0, TargetLatencyMs: 5000},
			Status:    common.StatusRunning,
			StartTime: time.Now().Add(-5 * time.Minute),
		},
		{
			ID:        "victim-mid",
			Spec:      &common.JobSpec{RequestID: "v2", Name: "mid-pri", Image: "img", GPUCount: 4, Priority: 50, TimeoutSecs: 300, MaxRetries: 0, TargetLatencyMs: 5000},
			Status:    common.StatusRunning,
			StartTime: time.Now().Add(-3 * time.Minute),
		},
	}

	decision := pm.FindPreemptionVictim(ctx, incoming, running)
	require.True(t, decision.ShouldPreempt)
	require.NotNil(t, decision.Victim)
	assert.Equal(t, "victim-low", decision.Victim.JobID, "lowest-priority job should be preempted")
	assert.Equal(t, 20, decision.Victim.Priority)
}

// TestScheduling_NoCapacityError verifies that scheduling fails gracefully when
// all clusters are at capacity.
func TestScheduling_NoCapacityError(t *testing.T) {
	skipIfNoInfra(t)

	gs, cm := setupGlobalScheduler(t, []clusterSetup{
		{ID: "cl-full-1", Region: "us-west", GPUs: 2},
		{ID: "cl-full-2", Region: "eu-west", GPUs: 2},
	})

	ctx := context.Background()

	// Set both clusters to full capacity
	require.NoError(t, cm.UpdateClusterLoad(ctx, "cl-full-1", 2, 64, 160.0, 2, 0))
	require.NoError(t, cm.UpdateClusterLoad(ctx, "cl-full-2", 2, 64, 160.0, 2, 0))

	// Wait for GlobalScheduler to pick up load
	time.Sleep(200 * time.Millisecond)

	spec := testJobSpec("sched-no-capacity")
	spec.GPUCount = 4 // Requesting more than any cluster has

	_, _, err := gs.SelectBestCluster(ctx, spec, "")
	assert.Error(t, err, "should fail when no cluster has sufficient GPUs")
}
