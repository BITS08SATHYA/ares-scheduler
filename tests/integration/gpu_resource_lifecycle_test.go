package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/local"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newLocalSchedulerWithNode creates a LocalScheduler with a single registered node.
// Returns the scheduler and node ID. GPU discovery/topology are nil (not needed for resource tracking tests).
func newLocalSchedulerWithNode(t *testing.T, clusterID, nodeID string, gpuCount int, memGB float64) *local.LocalScheduler {
	t.Helper()
	rc := newRedisClient(t)

	ls := local.NewLocalScheduler(clusterID, rc, nil, nil)

	node := &local.NodeState{
		NodeID:            nodeID,
		IsHealthy:         true,
		GPUCount:          gpuCount,
		AvailableGPUs:     gpuCount,
		MemoryGB:          memGB,
		AvailableMemoryGB: memGB,
		RunningJobsCount:  0,
		LastHealthCheck:   time.Now(),
	}
	require.NoError(t, ls.RegisterNode(context.Background(), node))

	// Cleanup GPU alloc keys from Redis
	prefix := "ares:gpu-alloc:" + clusterID + ":*"
	t.Cleanup(func() { cleanupRedisKeys(t, rc, prefix) })

	return ls
}

// TestGPU_ReserveAndRelease verifies reserve → count drop → release → count restore.
func TestGPU_ReserveAndRelease(t *testing.T) {
	skipIfNoInfra(t)

	clusterID := testPrefix(t, "cluster")
	nodeID := "node-gpu-1"
	ls := newLocalSchedulerWithNode(t, clusterID, nodeID, 8, 640.0)
	ctx := context.Background()

	// Reserve 2 GPUs for job-A
	err := ls.ReserveResources(ctx, "job-A", nodeID, 2, 16384, []int{0, 1})
	require.NoError(t, err)

	// Verify available GPUs dropped from 8 to 6
	node, err := ls.GetNodeState(nodeID)
	require.NoError(t, err)
	assert.Equal(t, 6, node.AvailableGPUs, "expected 6 available GPUs after reserving 2")

	// Release resources
	err = ls.ReleaseResources(ctx, "job-A", nodeID, 2, 16384)
	require.NoError(t, err)

	// Verify available GPUs restored to 8
	node, err = ls.GetNodeState(nodeID)
	require.NoError(t, err)
	assert.Equal(t, 8, node.AvailableGPUs, "expected 8 available GPUs after release")
}

// TestGPU_DoubleAssignmentBlocked verifies a GPU allocated to job-A cannot be given to job-B.
func TestGPU_DoubleAssignmentBlocked(t *testing.T) {
	skipIfNoInfra(t)

	clusterID := testPrefix(t, "cluster")
	nodeID := "node-gpu-2"
	ls := newLocalSchedulerWithNode(t, clusterID, nodeID, 4, 320.0)
	ctx := context.Background()

	// Reserve GPU 1 for job-A
	err := ls.ReserveResources(ctx, "job-A", nodeID, 1, 8192, []int{1})
	require.NoError(t, err)

	// Attempt to reserve GPU 1 for job-B → must fail
	err = ls.ReserveResources(ctx, "job-B", nodeID, 1, 8192, []int{1})
	assert.Error(t, err, "expected error when double-assigning GPU 1")
	assert.Contains(t, err.Error(), "already allocated")
}

// TestGPU_AllocationPersistsToRedis verifies GPU allocations survive a scheduler restart.
func TestGPU_AllocationPersistsToRedis(t *testing.T) {
	skipIfNoInfra(t)

	rc := newRedisClient(t)
	clusterID := testPrefix(t, "cluster")
	nodeID := "node-gpu-3"
	ctx := context.Background()

	// Create first scheduler, reserve GPUs
	ls1 := local.NewLocalScheduler(clusterID, rc, nil, nil)
	node := &local.NodeState{
		NodeID:            nodeID,
		IsHealthy:         true,
		GPUCount:          4,
		AvailableGPUs:     4,
		MemoryGB:          320.0,
		AvailableMemoryGB: 320.0,
	}
	require.NoError(t, ls1.RegisterNode(ctx, node))

	err := ls1.ReserveResources(ctx, "job-persist", nodeID, 2, 16384, []int{0, 1})
	require.NoError(t, err)

	// Create second scheduler (simulating restart) — it should load allocations from Redis
	ls2 := local.NewLocalScheduler(clusterID, rc, nil, nil)
	require.NoError(t, ls2.RegisterNode(ctx, &local.NodeState{
		NodeID:            nodeID,
		IsHealthy:         true,
		GPUCount:          4,
		AvailableGPUs:     4,
		MemoryGB:          320.0,
		AvailableMemoryGB: 320.0,
	}))

	// GPU 0 should be blocked because ls2 loaded the allocation from Redis
	err = ls2.ReserveResources(ctx, "job-new", nodeID, 1, 8192, []int{0})
	assert.Error(t, err, "expected GPU 0 to be blocked after restart (loaded from Redis)")

	// GPU 2 should be available
	err = ls2.ReserveResources(ctx, "job-new", nodeID, 1, 8192, []int{2})
	assert.NoError(t, err, "GPU 2 should be free")

	// Cleanup
	prefix := "ares:gpu-alloc:" + clusterID + ":*"
	t.Cleanup(func() { cleanupRedisKeys(t, rc, prefix) })
}

// TestGPU_ReleaseJobResources frees only the specified job's GPUs in a multi-job scenario.
func TestGPU_ReleaseJobResources(t *testing.T) {
	skipIfNoInfra(t)

	clusterID := testPrefix(t, "cluster")
	nodeID := "node-gpu-4"
	ls := newLocalSchedulerWithNode(t, clusterID, nodeID, 8, 640.0)
	ctx := context.Background()

	// Reserve GPUs for two jobs
	require.NoError(t, ls.ReserveResources(ctx, "job-X", nodeID, 2, 16384, []int{0, 1}))
	require.NoError(t, ls.ReserveResources(ctx, "job-Y", nodeID, 2, 16384, []int{2, 3}))

	node, _ := ls.GetNodeState(nodeID)
	assert.Equal(t, 4, node.AvailableGPUs)

	// Release only job-X
	ls.ReleaseJobResources("job-X")

	node, _ = ls.GetNodeState(nodeID)
	assert.Equal(t, 6, node.AvailableGPUs, "releasing job-X should free 2 GPUs")

	// job-Y's GPUs should still be allocated
	err := ls.ReserveResources(ctx, "job-Z", nodeID, 1, 8192, []int{2})
	assert.Error(t, err, "GPU 2 should still be held by job-Y")
}

// TestGPU_ClearAllocationsResetsAll verifies ClearGPUAllocations resets all nodes.
func TestGPU_ClearAllocationsResetsAll(t *testing.T) {
	skipIfNoInfra(t)

	clusterID := testPrefix(t, "cluster")
	nodeID := "node-gpu-5"
	ls := newLocalSchedulerWithNode(t, clusterID, nodeID, 4, 320.0)
	ctx := context.Background()

	require.NoError(t, ls.ReserveResources(ctx, "job-clear", nodeID, 3, 24576, []int{0, 1, 2}))

	node, _ := ls.GetNodeState(nodeID)
	assert.Equal(t, 1, node.AvailableGPUs)

	ls.ClearGPUAllocations()

	node, _ = ls.GetNodeState(nodeID)
	assert.Equal(t, 4, node.AvailableGPUs, "ClearGPUAllocations should restore all GPUs")
	assert.Equal(t, 0, node.RunningJobsCount, "ClearGPUAllocations should reset running jobs count")

	// Previously-allocated GPU should now be available
	err := ls.ReserveResources(ctx, "job-after-clear", nodeID, 1, 8192, []int{0})
	assert.NoError(t, err, "GPU 0 should be free after ClearGPUAllocations")
}

// ============================================================================
// HELPERS (used only within this file)
// ============================================================================

// gpuJobSpec creates a minimal JobSpec for GPU tests.
func gpuJobSpec(gpuCount int) *common.JobSpec {
	return &common.JobSpec{
		RequestID:       "gpu-test-req",
		Name:            "gpu-test",
		Image:           "nvidia/cuda:12.0",
		GPUCount:        gpuCount,
		GPUType:         "A100",
		MemoryMB:        8192,
		Priority:        50,
		TimeoutSecs:     60,
		MaxRetries:      0,
		TenantID:        "tenant-gpu",
		TargetLatencyMs: 5000,
	}
}
