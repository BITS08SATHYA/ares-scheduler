package global

import (
	"context"
	"testing"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/cluster"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Helpers
// ============================================================================

func testRedis() *redis.RedisClient {
	rc, err := redis.NewRedisClient("localhost:6379", "", 0)
	if err != nil {
		panic("test redis not available — run: docker compose up -d")
	}
	return rc
}

func newTestGlobalScheduler() *GlobalScheduler {
	redisClient := testRedis()
	cm := cluster.NewClusterManager(redisClient, cluster.DefaultClusterConfig)
	return NewGlobalScheduler("test-cp", redisClient, cm)
}

func addTestCluster(gs *GlobalScheduler, id, region string, totalGPUs, availGPUs int, healthy bool, gpuTypes []string) {
	gs.clustersMu.Lock()
	gs.clusters[id] = &cluster.ClusterInfo{
		ClusterID:         id,
		Region:            region,
		IsHealthy:         healthy,
		TotalGPUs:         totalGPUs,
		AvailableGPUs:     availGPUs,
		TotalMemoryGB:     256.0,
		AvailableMemoryGB: 200.0,
		RunningJobsCount:  totalGPUs - availGPUs,
		LastHeartbeat:     time.Now(),
		GPUTypes:          gpuTypes,
	}
	gs.clustersMu.Unlock()
}

func testJobSpec(gpuCount int) *common.JobSpec {
	return &common.JobSpec{
		RequestID:       "req-test",
		Name:            "test-job",
		Image:           "nvidia/cuda:12.0",
		GPUCount:        gpuCount,
		GPUType:         "any",
		Priority:        50,
		TimeoutSecs:     600,
		MaxRetries:      3,
		TargetLatencyMs: 1000,
	}
}

// ============================================================================
// SECTION 1: Constructor
// ============================================================================

func TestNewGlobalScheduler(t *testing.T) {
	gs := newTestGlobalScheduler()
	require.NotNil(t, gs)
	assert.NotNil(t, gs.drfManager)
	assert.NotNil(t, gs.preemptionManager)
	assert.NotNil(t, gs.gangManager)
	assert.Equal(t, "test-cp", gs.controlPlaneName)
}

// ============================================================================
// SECTION 2: Cluster Scoring
// ============================================================================

func TestScoreCluster_GPUAvailability(t *testing.T) {
	gs := newTestGlobalScheduler()
	jobSpec := testJobSpec(4)

	// Cluster with enough GPUs
	rich := &cluster.ClusterInfo{
		ClusterID: "rich", IsHealthy: true, TotalGPUs: 16,
		AvailableGPUs: 12, TotalMemoryGB: 256, AvailableMemoryGB: 200,
	}
	// Cluster with insufficient GPUs
	poor := &cluster.ClusterInfo{
		ClusterID: "poor", IsHealthy: true, TotalGPUs: 8,
		AvailableGPUs: 2, TotalMemoryGB: 256, AvailableMemoryGB: 200,
	}

	richScore := gs.scoreCluster(rich, jobSpec, "")
	poorScore := gs.scoreCluster(poor, jobSpec, "")

	assert.Greater(t, richScore.Score, poorScore.Score)
	assert.Contains(t, richScore.Reasons, "has-12-gpus")
	assert.Contains(t, poorScore.Reasons, "insufficient-gpus")
}

func TestScoreCluster_HealthBonus(t *testing.T) {
	gs := newTestGlobalScheduler()
	jobSpec := testJobSpec(2)

	healthy := &cluster.ClusterInfo{
		ClusterID: "h", IsHealthy: true, TotalGPUs: 8,
		AvailableGPUs: 8, TotalMemoryGB: 256, AvailableMemoryGB: 200,
	}
	unhealthy := &cluster.ClusterInfo{
		ClusterID: "u", IsHealthy: false, TotalGPUs: 8,
		AvailableGPUs: 8, TotalMemoryGB: 256, AvailableMemoryGB: 200,
	}

	hScore := gs.scoreCluster(healthy, jobSpec, "")
	uScore := gs.scoreCluster(unhealthy, jobSpec, "")

	assert.Greater(t, hScore.Score, uScore.Score)
	assert.Equal(t, 100.0, hScore.HealthScore)
	assert.Equal(t, 0.0, uScore.HealthScore)
}

func TestScoreCluster_RegionPreference(t *testing.T) {
	gs := newTestGlobalScheduler()
	jobSpec := testJobSpec(2)

	c := &cluster.ClusterInfo{
		ClusterID: "c1", Region: "us-west", IsHealthy: true, TotalGPUs: 8,
		AvailableGPUs: 8, TotalMemoryGB: 256, AvailableMemoryGB: 200,
	}

	matchScore := gs.scoreCluster(c, jobSpec, "us-west")
	noMatchScore := gs.scoreCluster(c, jobSpec, "eu-west")
	noPreference := gs.scoreCluster(c, jobSpec, "")

	assert.Greater(t, matchScore.Score, noMatchScore.Score)
	assert.Greater(t, matchScore.Score, noPreference.Score)
	assert.Contains(t, matchScore.Reasons, "preferred-region=us-west")
}

func TestScoreCluster_GPUTypeMatch(t *testing.T) {
	gs := newTestGlobalScheduler()
	jobSpec := testJobSpec(4)
	jobSpec.GPUType = "H100"

	hasH100 := &cluster.ClusterInfo{
		ClusterID: "h100", IsHealthy: true, TotalGPUs: 8,
		AvailableGPUs: 8, TotalMemoryGB: 256, AvailableMemoryGB: 200,
		GPUTypes: []string{"H100"},
	}
	hasA100 := &cluster.ClusterInfo{
		ClusterID: "a100", IsHealthy: true, TotalGPUs: 8,
		AvailableGPUs: 8, TotalMemoryGB: 256, AvailableMemoryGB: 200,
		GPUTypes: []string{"A100"},
	}

	h100Score := gs.scoreCluster(hasH100, jobSpec, "")
	a100Score := gs.scoreCluster(hasA100, jobSpec, "")

	assert.Greater(t, h100Score.Score, a100Score.Score)
	assert.Contains(t, h100Score.Reasons, "has-H100")
	assert.Contains(t, a100Score.Reasons, "no-H100")
}

func TestScoreCluster_ScoreCapped(t *testing.T) {
	gs := newTestGlobalScheduler()
	jobSpec := testJobSpec(1)

	huge := &cluster.ClusterInfo{
		ClusterID: "huge", IsHealthy: true, TotalGPUs: 1000,
		AvailableGPUs: 1000, TotalMemoryGB: 10000, AvailableMemoryGB: 10000,
	}

	score := gs.scoreCluster(huge, jobSpec, "")
	assert.LessOrEqual(t, score.Score, 100.0)
}

func TestScoreCluster_ScoreFloor(t *testing.T) {
	gs := newTestGlobalScheduler()
	jobSpec := testJobSpec(100)

	tiny := &cluster.ClusterInfo{
		ClusterID: "tiny", IsHealthy: false, TotalGPUs: 2,
		AvailableGPUs: 0, TotalMemoryGB: 1, AvailableMemoryGB: 0,
	}

	score := gs.scoreCluster(tiny, jobSpec, "eu-central")
	assert.GreaterOrEqual(t, score.Score, 0.0)
}

func TestScoreCluster_QueueDepthPenalty(t *testing.T) {
	gs := newTestGlobalScheduler()
	jobSpec := testJobSpec(2)

	idle := &cluster.ClusterInfo{
		ClusterID: "idle", IsHealthy: true, TotalGPUs: 8,
		AvailableGPUs: 8, TotalMemoryGB: 256, AvailableMemoryGB: 200,
		RunningJobsCount: 0,
	}
	saturated := &cluster.ClusterInfo{
		ClusterID: "saturated", IsHealthy: true, TotalGPUs: 8,
		AvailableGPUs: 2, TotalMemoryGB: 256, AvailableMemoryGB: 200,
		RunningJobsCount: 10, // more running than GPUs
	}

	idleScore := gs.scoreCluster(idle, jobSpec, "")
	satScore := gs.scoreCluster(saturated, jobSpec, "")

	assert.Greater(t, idleScore.Score, satScore.Score)
}

// ============================================================================
// SECTION 3: SelectBestCluster
// ============================================================================

func TestSelectBestCluster_PicksHighestScore(t *testing.T) {
	gs := newTestGlobalScheduler()

	// Register clusters with ClusterManager (needed because SelectBestCluster calls ListClusters)
	cfg1 := &cluster.ClusterConfig{ClusterID: "rich", Region: "us-west", Zone: "a", ControlAddr: "http://localhost:1"}
	cfg2 := &cluster.ClusterConfig{ClusterID: "poor", Region: "us-west", Zone: "a", ControlAddr: "http://localhost:2"}
	gs.clusterManager.JoinCluster(context.Background(), cfg1)
	gs.clusterManager.JoinCluster(context.Background(), cfg2)
	gs.clusterManager.UpdateClusterCapacity(context.Background(), "rich", 16, 64, 256.0)
	gs.clusterManager.UpdateClusterCapacity(context.Background(), "poor", 4, 16, 64.0)

	// Also populate gs.clusters cache
	addTestCluster(gs, "rich", "us-west", 16, 12, true, nil)
	addTestCluster(gs, "poor", "us-west", 4, 2, true, nil)

	best, score, err := gs.SelectBestCluster(context.Background(), testJobSpec(4), "")
	require.NoError(t, err)
	assert.Equal(t, "rich", best.ClusterID)
	assert.Greater(t, score.Score, 0.0)
}

func TestSelectBestCluster_NoClusters(t *testing.T) {
	gs := newTestGlobalScheduler()

	_, _, err := gs.SelectBestCluster(context.Background(), testJobSpec(4), "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no clusters")
}

func TestSelectBestCluster_InsufficientGPUs(t *testing.T) {
	gs := newTestGlobalScheduler()

	cfg := &cluster.ClusterConfig{ClusterID: "small", Region: "us-west", Zone: "a", ControlAddr: "http://localhost:1"}
	gs.clusterManager.JoinCluster(context.Background(), cfg)
	gs.clusterManager.UpdateClusterCapacity(context.Background(), "small", 4, 16, 64.0)
	addTestCluster(gs, "small", "us-west", 4, 2, true, nil)

	_, _, err := gs.SelectBestCluster(context.Background(), testJobSpec(8), "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "sufficient GPUs")
}

func TestSelectBestCluster_OptimisticDecrement(t *testing.T) {
	gs := newTestGlobalScheduler()

	cfg := &cluster.ClusterConfig{ClusterID: "c1", Region: "us-west", Zone: "a", ControlAddr: "http://localhost:1"}
	gs.clusterManager.JoinCluster(context.Background(), cfg)
	gs.clusterManager.UpdateClusterCapacity(context.Background(), "c1", 16, 64, 256.0)
	addTestCluster(gs, "c1", "us-west", 16, 16, true, nil)

	gs.SelectBestCluster(context.Background(), testJobSpec(4), "")

	// After selection, available GPUs should be decremented optimistically
	gs.clustersMu.RLock()
	assert.Equal(t, 12, gs.clusters["c1"].AvailableGPUs)
	gs.clustersMu.RUnlock()
}

// ============================================================================
// SECTION 4: Cluster Health
// ============================================================================

func TestCheckClusterHealth_MarkStale(t *testing.T) {
	gs := newTestGlobalScheduler()
	addTestCluster(gs, "stale", "us-west", 8, 8, true, nil)

	// Set old heartbeat
	gs.clustersMu.Lock()
	gs.clusters["stale"].LastHeartbeat = time.Now().Add(-5 * time.Minute)
	gs.clustersMu.Unlock()

	gs.CheckClusterHealth()

	gs.clustersMu.RLock()
	assert.False(t, gs.clusters["stale"].IsHealthy)
	gs.clustersMu.RUnlock()
}

func TestGetHealthStatus(t *testing.T) {
	gs := newTestGlobalScheduler()
	addTestCluster(gs, "h", "us-west", 8, 8, true, nil)
	addTestCluster(gs, "u", "us-east", 8, 8, false, nil)

	status := gs.GetHealthStatus()
	assert.True(t, status["h"])
	assert.False(t, status["u"])
}

func TestHealthyClusterCount(t *testing.T) {
	gs := newTestGlobalScheduler()
	addTestCluster(gs, "h1", "us-west", 8, 8, true, nil)
	addTestCluster(gs, "h2", "us-east", 8, 8, true, nil)
	addTestCluster(gs, "u1", "eu-west", 8, 8, false, nil)

	assert.Equal(t, 2, gs.HealthyClusterCount())
}

func TestGetTotalAvailableGPUs(t *testing.T) {
	gs := newTestGlobalScheduler()
	addTestCluster(gs, "c1", "us-west", 16, 10, true, nil)
	addTestCluster(gs, "c2", "us-east", 8, 6, true, nil)
	addTestCluster(gs, "c3", "eu-west", 8, 4, false, nil) // unhealthy, excluded

	assert.Equal(t, 16, gs.GetTotalAvailableGPUs())
}

func TestGetClusterState(t *testing.T) {
	gs := newTestGlobalScheduler()
	addTestCluster(gs, "c1", "us-west", 8, 8, true, nil)

	assert.NotNil(t, gs.GetClusterState("c1"))
	assert.Nil(t, gs.GetClusterState("nonexistent"))
}

// ============================================================================
// SECTION 5: Cluster Event Listener
// ============================================================================

func TestOnClusterJoin(t *testing.T) {
	gs := newTestGlobalScheduler()

	c := &cluster.Cluster{
		ClusterID: "new-cluster", Region: "us-west", Zone: "a",
		TotalGPUs: 16, TotalMemGB: 256,
		IsHealthy: true, ControlAddr: "http://localhost:9090",
	}
	err := gs.OnClusterJoin(context.Background(), c)
	assert.NoError(t, err)

	assert.NotNil(t, gs.GetClusterState("new-cluster"))
}

func TestOnClusterJoin_NilCluster(t *testing.T) {
	gs := newTestGlobalScheduler()
	err := gs.OnClusterJoin(context.Background(), nil)
	assert.Error(t, err)
}

func TestOnClusterLeave(t *testing.T) {
	gs := newTestGlobalScheduler()
	addTestCluster(gs, "leaving", "us-west", 8, 8, true, nil)

	err := gs.OnClusterLeave(context.Background(), "leaving")
	assert.NoError(t, err)
	assert.Nil(t, gs.GetClusterState("leaving"))
}

func TestOnClusterLeave_EmptyID(t *testing.T) {
	gs := newTestGlobalScheduler()
	err := gs.OnClusterLeave(context.Background(), "")
	assert.Error(t, err)
}

// ============================================================================
// SECTION 6: UpdateClusterLoad
// ============================================================================

func TestUpdateClusterLoad(t *testing.T) {
	gs := newTestGlobalScheduler()
	addTestCluster(gs, "c1", "us-west", 16, 16, true, nil)

	gs.UpdateClusterLoad("c1", 6, 100.0, 3)

	gs.clustersMu.RLock()
	c := gs.clusters["c1"]
	assert.Equal(t, 10, c.AvailableGPUs) // 16-6
	assert.Equal(t, 3, c.RunningJobsCount)
	assert.True(t, c.IsHealthy)
	gs.clustersMu.RUnlock()
}

func TestUpdateClusterGPUTypes(t *testing.T) {
	gs := newTestGlobalScheduler()
	addTestCluster(gs, "c1", "us-west", 8, 8, true, nil)

	gs.UpdateClusterGPUTypes("c1", []string{"A100", "H100"})

	gs.clustersMu.RLock()
	assert.Equal(t, []string{"A100", "H100"}, gs.clusters["c1"].GPUTypes)
	gs.clustersMu.RUnlock()
}

// ============================================================================
// SECTION 7: Metrics
// ============================================================================

func TestMetrics(t *testing.T) {
	gs := newTestGlobalScheduler()

	gs.recordSchedulingSuccess("c1")
	gs.recordSchedulingSuccess("c1")
	gs.recordSchedulingFailure()

	m := gs.GetMetrics()
	assert.Equal(t, int64(2), m.TotalJobsScheduled)
	assert.Equal(t, int64(1), m.TotalJobsFailed)
	assert.Equal(t, int64(2), m.TotalJobsRouted["c1"])

	assert.InDelta(t, 66.6, gs.SuccessRate(), 0.5)
}

func TestSuccessRate_NoJobs(t *testing.T) {
	gs := newTestGlobalScheduler()
	assert.Equal(t, 0.0, gs.SuccessRate())
}

// ============================================================================
// SECTION 8: Capacity Queries
// ============================================================================

func TestCanScheduleJob(t *testing.T) {
	gs := newTestGlobalScheduler()
	addTestCluster(gs, "c1", "us-west", 16, 12, true, nil)

	assert.True(t, gs.CanScheduleJob(testJobSpec(4)))
	assert.False(t, gs.CanScheduleJob(testJobSpec(20)))
}

func TestGetDatacenterLoad(t *testing.T) {
	gs := newTestGlobalScheduler()
	addTestCluster(gs, "c1", "us-west", 16, 10, true, nil)
	addTestCluster(gs, "c2", "us-east", 8, 6, true, nil)

	load := gs.GetDatacenterLoad()
	assert.Equal(t, 2, load["total_clusters"])
	assert.Equal(t, 24, load["total_gpus"])
	assert.Equal(t, 8, load["gpus_in_use"]) // (16-10) + (8-6)
}

// ============================================================================
// SECTION 9: ScheduleJob Validation
// ============================================================================

func TestScheduleJob_NilRecord(t *testing.T) {
	gs := newTestGlobalScheduler()
	_, err := gs.ScheduleJob(context.Background(), nil)
	assert.Error(t, err)
}

func TestScheduleJob_InvalidSpec(t *testing.T) {
	gs := newTestGlobalScheduler()
	job := &common.Job{ID: "j1", Spec: &common.JobSpec{}}
	_, err := gs.ScheduleJob(context.Background(), job)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing name")
}

func TestScheduleJob_InvalidGPUCount(t *testing.T) {
	gs := newTestGlobalScheduler()
	job := &common.Job{ID: "j1", Spec: &common.JobSpec{Name: "test", GPUCount: 300}}
	_, err := gs.ScheduleJob(context.Background(), job)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid GPU count")
}

// ============================================================================
// SECTION 10: ListClusters
// ============================================================================

func TestListClusters(t *testing.T) {
	gs := newTestGlobalScheduler()
	addTestCluster(gs, "c1", "us-west", 8, 8, true, nil)
	addTestCluster(gs, "c2", "us-east", 8, 8, true, nil)

	clusters := gs.ListClusters()
	assert.Len(t, clusters, 2)
}
