package local

import (
	"context"
	"testing"

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

func newTestLocalScheduler() *LocalScheduler {
	rc := testRedis()
	// Clean up stale GPU allocation keys from previous test runs
	ctx := context.Background()
	keys, _ := rc.Keys(ctx, "ares:gpu-alloc:test-cluster:*")
	for _, k := range keys {
		rc.Del(ctx, k)
	}
	return NewLocalScheduler("test-cluster", rc, nil, nil)
}

func healthyNode(id string, gpuCount, availGPUs int, memGB, availMem float64) *NodeState {
	return &NodeState{
		NodeID:            id,
		IsHealthy:         true,
		GPUCount:          gpuCount,
		AvailableGPUs:     availGPUs,
		MemoryGB:          memGB,
		AvailableMemoryGB: availMem,
		RunningJobsCount:  gpuCount - availGPUs,
	}
}

func testJobSpec(gpuCount int) *common.JobSpec {
	return &common.JobSpec{
		RequestID:       "req-test",
		Name:            "test-job",
		Image:           "nvidia/cuda:12.0",
		GPUCount:        gpuCount,
		GPUType:         "any",
		Priority:        50,
		MemoryMB:        4096,
		TimeoutSecs:     600,
		MaxRetries:      3,
		TargetLatencyMs: 1000,
	}
}

// ============================================================================
// SECTION 1: Constructor
// ============================================================================

func TestNewLocalScheduler(t *testing.T) {
	ls := newTestLocalScheduler()
	require.NotNil(t, ls)
	assert.Equal(t, "test-cluster", ls.clusterID)
	assert.NotNil(t, ls.metrics)
}

// ============================================================================
// SECTION 2: Node Management
// ============================================================================

func TestRegisterNode(t *testing.T) {
	ls := newTestLocalScheduler()
	node := healthyNode("node-1", 8, 8, 256, 256)

	err := ls.RegisterNode(context.Background(), node)
	assert.NoError(t, err)

	nodes := ls.ListNodes()
	assert.Len(t, nodes, 1)
	assert.Equal(t, "node-1", nodes[0].NodeID)
}

func TestRegisterNode_Invalid(t *testing.T) {
	ls := newTestLocalScheduler()

	assert.Error(t, ls.RegisterNode(context.Background(), nil))
	assert.Error(t, ls.RegisterNode(context.Background(), &NodeState{}))
}

func TestGetNodeState(t *testing.T) {
	ls := newTestLocalScheduler()
	ls.RegisterNode(context.Background(), healthyNode("n1", 8, 8, 256, 256))

	n, err := ls.GetNodeState("n1")
	assert.NoError(t, err)
	assert.Equal(t, "n1", n.NodeID)
}

func TestGetNodeState_NotFound(t *testing.T) {
	ls := newTestLocalScheduler()
	_, err := ls.GetNodeState("nonexistent")
	assert.Error(t, err)
}

func TestUpdateNodeState(t *testing.T) {
	ls := newTestLocalScheduler()
	ls.RegisterNode(context.Background(), healthyNode("n1", 8, 8, 256, 256))

	updated := healthyNode("n1", 8, 4, 256, 128)
	err := ls.UpdateNodeState(context.Background(), updated)
	assert.NoError(t, err)

	n, _ := ls.GetNodeState("n1")
	assert.Equal(t, 4, n.AvailableGPUs)
}

func TestUpdateNodeState_NotRegistered(t *testing.T) {
	ls := newTestLocalScheduler()
	err := ls.UpdateNodeState(context.Background(), healthyNode("unregistered", 8, 8, 256, 256))
	assert.Error(t, err)
}

func TestListNodes_ReturnsCopy(t *testing.T) {
	ls := newTestLocalScheduler()
	ls.RegisterNode(context.Background(), healthyNode("n1", 8, 8, 256, 256))

	nodes := ls.ListNodes()
	nodes[0].AvailableGPUs = 0 // mutate copy

	n, _ := ls.GetNodeState("n1")
	assert.Equal(t, 8, n.AvailableGPUs) // original unchanged
}

// ============================================================================
// SECTION 3: Node Scoring
// ============================================================================

func TestScoreNode_GPUAvailability(t *testing.T) {
	ls := newTestLocalScheduler()
	jobSpec := testJobSpec(4)

	rich := healthyNode("rich", 16, 12, 512, 400)
	poor := healthyNode("poor", 8, 2, 512, 400)

	richScore := ls.scoreNode(rich, jobSpec)
	poorScore := ls.scoreNode(poor, jobSpec)

	assert.Greater(t, richScore.Score, poorScore.Score)
	assert.Contains(t, richScore.Reasons, "has-12-gpus")
	assert.Contains(t, poorScore.Reasons, "insufficient-gpus")
}

func TestScoreNode_HealthBonus(t *testing.T) {
	ls := newTestLocalScheduler()
	jobSpec := testJobSpec(2)

	healthy := healthyNode("h", 8, 8, 256, 256)
	unhealthy := &NodeState{NodeID: "u", IsHealthy: false, GPUCount: 8, AvailableGPUs: 8, MemoryGB: 256, AvailableMemoryGB: 256}

	hScore := ls.scoreNode(healthy, jobSpec)
	uScore := ls.scoreNode(unhealthy, jobSpec)

	assert.Greater(t, hScore.Score, uScore.Score)
}

func TestScoreNode_MemoryPenalty(t *testing.T) {
	ls := newTestLocalScheduler()
	jobSpec := testJobSpec(2)
	jobSpec.MemoryMB = 200 * 1024 // 200 GB

	lowMem := healthyNode("low", 8, 8, 256, 50)
	highMem := healthyNode("high", 8, 8, 512, 400)

	lowScore := ls.scoreNode(lowMem, jobSpec)
	highScore := ls.scoreNode(highMem, jobSpec)

	assert.Greater(t, highScore.Score, lowScore.Score)
	assert.Contains(t, lowScore.Reasons, "insufficient-memory")
}

func TestScoreNode_Capped(t *testing.T) {
	ls := newTestLocalScheduler()
	jobSpec := testJobSpec(1)

	huge := healthyNode("huge", 1000, 1000, 10000, 10000)
	score := ls.scoreNode(huge, jobSpec)
	assert.LessOrEqual(t, score.Score, 100.0)
}

func TestScoreNode_Floor(t *testing.T) {
	ls := newTestLocalScheduler()
	jobSpec := testJobSpec(100)

	tiny := &NodeState{NodeID: "tiny", IsHealthy: false, GPUCount: 2, AvailableGPUs: 0, MemoryGB: 1, AvailableMemoryGB: 0}
	score := ls.scoreNode(tiny, jobSpec)
	assert.GreaterOrEqual(t, score.Score, 0.0)
}

// ============================================================================
// SECTION 4: SelectBestNode
// ============================================================================

func TestSelectBestNode(t *testing.T) {
	ls := newTestLocalScheduler()
	ls.RegisterNode(context.Background(), healthyNode("big", 16, 12, 512, 400))
	ls.RegisterNode(context.Background(), healthyNode("small", 4, 2, 128, 64))

	node, score, err := ls.SelectBestNode(context.Background(), testJobSpec(4))
	require.NoError(t, err)
	assert.Equal(t, "big", node.NodeID)
	assert.Greater(t, score.Score, 0.0)
}

func TestSelectBestNode_NoNodes(t *testing.T) {
	ls := newTestLocalScheduler()
	_, _, err := ls.SelectBestNode(context.Background(), testJobSpec(4))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no nodes")
}

func TestSelectBestNode_AllUnhealthy(t *testing.T) {
	ls := newTestLocalScheduler()
	ls.RegisterNode(context.Background(), &NodeState{
		NodeID: "u1", IsHealthy: false, GPUCount: 8, AvailableGPUs: 8,
		MemoryGB: 256, AvailableMemoryGB: 256,
	})

	_, _, err := ls.SelectBestNode(context.Background(), testJobSpec(4))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no suitable nodes")
}

// ============================================================================
// SECTION 5: Resource Reservation
// ============================================================================

func TestReserveResources(t *testing.T) {
	ls := newTestLocalScheduler()
	ls.RegisterNode(context.Background(), healthyNode("n1", 8, 8, 256, 256))

	err := ls.ReserveResources(context.Background(), "job-1", "n1", 4, 64*1024, []int{0, 1, 2, 3})
	assert.NoError(t, err)

	n, _ := ls.GetNodeState("n1")
	assert.Equal(t, 4, n.AvailableGPUs)
	assert.Equal(t, 1, n.RunningJobsCount)

	// Check GPU allocation tracking
	ls.nodesMu.RLock()
	assert.Equal(t, "job-1", ls.allocatedGPUs["n1"][0])
	assert.Equal(t, "job-1", ls.allocatedGPUs["n1"][3])
	ls.nodesMu.RUnlock()
}

func TestReserveResources_DoubleAssignment(t *testing.T) {
	ls := newTestLocalScheduler()
	ls.RegisterNode(context.Background(), healthyNode("n1", 8, 8, 256, 256))

	ls.ReserveResources(context.Background(), "job-1", "n1", 2, 1024, []int{0, 1})
	err := ls.ReserveResources(context.Background(), "job-2", "n1", 2, 1024, []int{1, 2}) // overlap at GPU 1
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already allocated")
}

func TestReserveResources_InsufficientGPUs(t *testing.T) {
	ls := newTestLocalScheduler()
	ls.RegisterNode(context.Background(), healthyNode("n1", 4, 4, 256, 256))

	err := ls.ReserveResources(context.Background(), "job-1", "n1", 8, 1024, []int{0, 1, 2, 3, 4, 5, 6, 7})
	assert.Error(t, err)
}

// ============================================================================
// SECTION 6: Resource Release
// ============================================================================

func TestReleaseResources(t *testing.T) {
	ls := newTestLocalScheduler()
	ls.RegisterNode(context.Background(), healthyNode("n1", 8, 8, 256, 256))
	ls.ReserveResources(context.Background(), "job-1", "n1", 4, 64*1024, []int{0, 1, 2, 3})

	// Verify reservation worked
	n1, _ := ls.GetNodeState("n1")
	assert.Equal(t, 4, n1.AvailableGPUs)

	err := ls.ReleaseResources(context.Background(), "job-1", "n1", 4, 64*1024)
	assert.NoError(t, err)

	n2, _ := ls.GetNodeState("n1")
	assert.Equal(t, 8, n2.AvailableGPUs)

	// GPU tracking should be cleared
	ls.nodesMu.RLock()
	for _, owner := range ls.allocatedGPUs["n1"] {
		assert.NotEqual(t, "job-1", owner)
	}
	ls.nodesMu.RUnlock()
}

func TestReleaseJobResources(t *testing.T) {
	ls := newTestLocalScheduler()
	ls.RegisterNode(context.Background(), healthyNode("n1", 8, 8, 256, 256))
	ls.ReserveResources(context.Background(), "job-1", "n1", 2, 1024, []int{4, 5})

	ls.ReleaseJobResources("job-1")

	ls.nodesMu.RLock()
	for _, jobID := range ls.allocatedGPUs["n1"] {
		assert.NotEqual(t, "job-1", jobID)
	}
	ls.nodesMu.RUnlock()
}

func TestClearGPUAllocations(t *testing.T) {
	ls := newTestLocalScheduler()
	ls.RegisterNode(context.Background(), healthyNode("n1", 8, 8, 256, 256))
	ls.ReserveResources(context.Background(), "j1", "n1", 4, 1024, []int{0, 1, 2, 3})

	ls.ClearGPUAllocations()

	n, _ := ls.GetNodeState("n1")
	assert.Equal(t, 8, n.AvailableGPUs) // restored
	assert.Equal(t, 0, n.RunningJobsCount)

	ls.nodesMu.RLock()
	assert.Empty(t, ls.allocatedGPUs["n1"])
	ls.nodesMu.RUnlock()
}

// ============================================================================
// SECTION 7: Health Monitoring
// ============================================================================

func TestCheckNodeHealth(t *testing.T) {
	ls := newTestLocalScheduler()
	ls.RegisterNode(context.Background(), healthyNode("n1", 8, 8, 256, 256))

	// Healthy node should stay healthy after check
	ls.CheckNodeHealth()
	status := ls.GetHealthStatus()
	assert.True(t, status["n1"])
}

func TestGetHealthStatus(t *testing.T) {
	ls := newTestLocalScheduler()
	ls.RegisterNode(context.Background(), healthyNode("h", 8, 8, 256, 256))
	ls.RegisterNode(context.Background(), &NodeState{
		NodeID: "u", IsHealthy: false, GPUCount: 4, AvailableGPUs: 4,
		MemoryGB: 128, AvailableMemoryGB: 128,
	})

	status := ls.GetHealthStatus()
	assert.True(t, status["h"])
	assert.False(t, status["u"])
}

// ============================================================================
// SECTION 8: Metrics
// ============================================================================

func TestMetrics(t *testing.T) {
	ls := newTestLocalScheduler()

	ls.recordSchedulingSuccess()
	ls.recordSchedulingSuccess()
	ls.recordSchedulingFailure()

	m := ls.GetMetrics()
	assert.Equal(t, int64(2), m.TotalJobsScheduled)
	assert.Equal(t, int64(1), m.TotalJobsFailed)

	assert.InDelta(t, 66.6, ls.SuccessRate(), 0.5)
}

func TestSuccessRate_NoJobs(t *testing.T) {
	ls := newTestLocalScheduler()
	assert.Equal(t, 0.0, ls.SuccessRate())
}

// ============================================================================
// SECTION 9: Cluster Load
// ============================================================================

func TestGetClusterLoad(t *testing.T) {
	ls := newTestLocalScheduler()
	ls.RegisterNode(context.Background(), healthyNode("n1", 8, 4, 256, 128))
	ls.RegisterNode(context.Background(), healthyNode("n2", 8, 8, 256, 256))

	load := ls.GetClusterLoad()
	assert.Equal(t, "test-cluster", load["cluster_id"])
	assert.Equal(t, 16, load["total_gpus"])
	assert.Equal(t, 4, load["gpus_in_use"]) // 8-4
	assert.Equal(t, 2, load["nodes_count"])
	assert.Equal(t, 2, load["healthy_nodes"])
}

// ============================================================================
// SECTION 10: ScheduleJob Validation
// ============================================================================

func TestScheduleJob_NilSpec(t *testing.T) {
	ls := newTestLocalScheduler()
	_, err := ls.ScheduleJob(context.Background(), nil)
	assert.Error(t, err)
}

func TestScheduleJob_InvalidGPUCount(t *testing.T) {
	ls := newTestLocalScheduler()
	spec := testJobSpec(0)
	spec.GPUCount = -1
	_, err := ls.ScheduleJob(context.Background(), spec)
	assert.Error(t, err)
}

func TestScheduleJob_TooManyGPUs(t *testing.T) {
	ls := newTestLocalScheduler()
	spec := testJobSpec(33) // max is 32
	_, err := ls.ScheduleJob(context.Background(), spec)
	assert.Error(t, err)
}
