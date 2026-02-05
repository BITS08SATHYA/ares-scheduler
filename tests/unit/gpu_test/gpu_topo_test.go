package gpu_test

import (
	"context"
	"testing"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/gpu"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// SECTION 1: GPU AFFINITY SCORING TESTS (Pure Logic - No Dependencies)
// ============================================================================

// TestGPUAffinityScoring tests the scoring logic for GPU placement
func TestGPUAffinityScoring(t *testing.T) {
	// Create topology manager
	redisClient, err := redis.NewRedisClient("localhost:6379", "", 0)
	require.NoError(t, err)
	defer redisClient.Close()

	discovery := gpu.NewGPUDiscovery(redisClient)
	topoMgr := gpu.NewGPUTopologyManager(redisClient, discovery)

	// Create mock topology
	topology := &common.GPUTopology{
		NVLinkPairs: [][]int{
			{0, 1}, // GPU 0 and 1 connected via NVLink
			{2, 3}, // GPU 2 and 3 connected via NVLink
		},
		GPUToNUMA: map[int]int{
			0: 0, // GPU 0 on NUMA node 0
			1: 0, // GPU 1 on NUMA node 0
			2: 1, // GPU 2 on NUMA node 1
			3: 1, // GPU 3 on NUMA node 1
		},
		PCIeGen: map[int]int{
			0: 4, // GPU 0 uses PCIe Gen4
			1: 4,
			2: 5, // GPU 2 uses PCIe Gen5
			3: 5,
		},
	}

	t.Run("Score NVLink connected GPUs (highest score)", func(t *testing.T) {
		// GPU 0 and 1 are NVLink connected AND same NUMA
		score := topoMgr.ScoreGPUPlacement(0, 1, topology)

		assert.NotNil(t, score)
		assert.True(t, score.HasNVLink, "Should detect NVLink")
		assert.True(t, score.SameNUMA, "Should detect same NUMA")
		assert.Greater(t, score.Score, 70.0, "NVLink + NUMA should score >70")
		assert.Contains(t, score.Reason, "NVLink", "Reason should mention NVLink")
	})

	t.Run("Score same NUMA but no NVLink", func(t *testing.T) {
		// Manually test scoring without NVLink
		// Create topology with no NVLink but same NUMA
		topoNoNVLink := &common.GPUTopology{
			NVLinkPairs: [][]int{}, // No NVLink!
			GPUToNUMA: map[int]int{
				0: 0,
				1: 0, // Same NUMA as GPU 0
			},
			PCIeGen: map[int]int{
				0: 4,
				1: 4,
			},
		}

		score := topoMgr.ScoreGPUPlacement(0, 1, topoNoNVLink)

		assert.False(t, score.HasNVLink, "Should not detect NVLink")
		assert.True(t, score.SameNUMA, "Should detect same NUMA")
		assert.Less(t, score.Score, 50.0, "No NVLink should score <50")
		assert.Contains(t, score.Reason, "same-NUMA", "Reason should mention NUMA")
	})

	t.Run("Score different NUMA, no NVLink (lowest score)", func(t *testing.T) {
		// GPU 0 (NUMA 0) and GPU 2 (NUMA 1) - different NUMA, no NVLink
		score := topoMgr.ScoreGPUPlacement(0, 2, topology)

		assert.False(t, score.HasNVLink, "Should not detect NVLink")
		assert.False(t, score.SameNUMA, "Should not detect same NUMA")
		assert.Less(t, score.Score, 40.0, "Different NUMA + no NVLink = low score")
	})

	t.Run("Score PCIe Gen5 bonus", func(t *testing.T) {
		// GPU 2 and 3 have PCIe Gen5
		score := topoMgr.ScoreGPUPlacement(2, 3, topology)

		assert.Equal(t, 5, score.PCIeGeneration, "Should detect Gen5")
		assert.Contains(t, score.Reason, "PCIe-Gen5", "Should mention Gen5")
	})

	t.Run("Score is capped at 100", func(t *testing.T) {
		// Even with NVLink + NUMA + Gen5, score shouldn't exceed 100
		score := topoMgr.ScoreGPUPlacement(0, 1, topology)
		assert.LessOrEqual(t, score.Score, 100.0, "Score should be capped at 100")
	})
}

// TestGPUSetScoring tests scoring multiple GPUs together
func TestGPUSetScoring(t *testing.T) {
	redisClient, err := redis.NewRedisClient("localhost:6379", "", 0)
	require.NoError(t, err)
	defer redisClient.Close()

	discovery := gpu.NewGPUDiscovery(redisClient)
	topoMgr := gpu.NewGPUTopologyManager(redisClient, discovery)

	topology := &common.GPUTopology{
		NVLinkPairs: [][]int{
			{0, 1},
			{1, 2},
			{2, 3},
		},
		GPUToNUMA: map[int]int{
			0: 0, 1: 0, 2: 0, 3: 0, // All same NUMA
		},
		PCIeGen: map[int]int{
			0: 4, 1: 4, 2: 4, 3: 4,
		},
	}

	t.Run("Score single GPU (no pairs)", func(t *testing.T) {
		gpuSet := []int{0}
		score := topoMgr.ScoreGPUSet(gpuSet, topology)

		assert.Equal(t, 100.0, score, "Single GPU should score 100 (no affinity needed)")
	})

	t.Run("Score GPU pair with NVLink", func(t *testing.T) {
		gpuSet := []int{0, 1} // NVLink connected
		score := topoMgr.ScoreGPUSet(gpuSet, topology)

		assert.Greater(t, score, 70.0, "NVLink pair should score high")
	})

	t.Run("Score GPU quad (4 GPUs)", func(t *testing.T) {
		gpuSet := []int{0, 1, 2, 3}
		score := topoMgr.ScoreGPUSet(gpuSet, topology)

		// 6 pairs total: (0,1), (0,2), (0,3), (1,2), (1,3), (2,3)
		// Some have NVLink, some don't
		assert.Greater(t, score, 40.0, "Quad should have reasonable score")
		assert.Less(t, score, 100.0, "Not all pairs have NVLink")
	})

	t.Run("Score empty GPU set", func(t *testing.T) {
		gpuSet := []int{}
		score := topoMgr.ScoreGPUSet(gpuSet, topology)

		assert.Equal(t, 100.0, score, "Empty set should return 100 (default)")
	})
}

// ============================================================================
// SECTION 2: GPU SELECTION TESTS (Critical Path)
// ============================================================================

// TestSelectBestGPUSet tests the GPU selection algorithm
func TestSelectBestGPUSet(t *testing.T) {
	ctx := context.Background()
	redisClient, err := redis.NewRedisClient("localhost:6379", "", 0)
	require.NoError(t, err)
	defer redisClient.Close()

	discovery := gpu.NewGPUDiscovery(redisClient)
	topoMgr := gpu.NewGPUTopologyManager(redisClient, discovery)

	t.Run("Select single GPU (no panic)", func(t *testing.T) {
		// This was the bug - requesting 1 GPU caused panic
		availableGPUs := []*common.GPUDevice{
			{Index: 0, UUID: "GPU-0", MemoryGB: 40.0, AvailableMemGB: 40.0, IsHealthy: true},
			{Index: 1, UUID: "GPU-1", MemoryGB: 40.0, AvailableMemGB: 40.0, IsHealthy: true},
		}

		selected, score, err := topoMgr.SelectBestGPUSet(ctx, availableGPUs, 1, false, false)

		assert.NoError(t, err, "Should not error for single GPU")
		assert.Len(t, selected, 1, "Should select exactly 1 GPU")
		assert.NotNil(t, score, "Score should not be nil")
		assert.Contains(t, score.Reason, "single-gpu", "Should indicate single GPU selection")
	})

	t.Run("Select GPU pair", func(t *testing.T) {
		availableGPUs := []*common.GPUDevice{
			{Index: 0, UUID: "GPU-0", MemoryGB: 40.0, AvailableMemGB: 40.0, IsHealthy: true},
			{Index: 1, UUID: "GPU-1", MemoryGB: 40.0, AvailableMemGB: 40.0, IsHealthy: true},
			{Index: 2, UUID: "GPU-2", MemoryGB: 40.0, AvailableMemGB: 40.0, IsHealthy: true},
		}

		selected, score, err := topoMgr.SelectBestGPUSet(ctx, availableGPUs, 2, true, true)

		assert.NoError(t, err, "Should not error for GPU pair")
		assert.Len(t, selected, 2, "Should select exactly 2 GPUs")
		assert.NotNil(t, score)
	})

	t.Run("Select more GPUs than available (error)", func(t *testing.T) {
		availableGPUs := []*common.GPUDevice{
			{Index: 0, UUID: "GPU-0", MemoryGB: 40.0, AvailableMemGB: 40.0, IsHealthy: true},
		}

		selected, score, err := topoMgr.SelectBestGPUSet(ctx, availableGPUs, 4, false, false)

		assert.Error(t, err, "Should error when requesting more GPUs than available")
		assert.Nil(t, selected)
		assert.Nil(t, score)
		assert.Contains(t, err.Error(), "insufficient", "Error should mention insufficient GPUs")
	})

	t.Run("Select from empty GPU list (error)", func(t *testing.T) {
		availableGPUs := []*common.GPUDevice{}

		selected, score, err := topoMgr.SelectBestGPUSet(ctx, availableGPUs, 1, false, false)

		assert.Error(t, err, "Should error with no available GPUs")
		assert.Nil(t, selected)
		assert.Nil(t, score)
		assert.Contains(t, err.Error(), "no GPUs available")
	})

	t.Run("Select with invalid count (error)", func(t *testing.T) {
		availableGPUs := []*common.GPUDevice{
			{Index: 0, UUID: "GPU-0", MemoryGB: 40.0, AvailableMemGB: 40.0, IsHealthy: true},
		}

		selected, score, err := topoMgr.SelectBestGPUSet(ctx, availableGPUs, 0, false, false)

		assert.Error(t, err, "Should error with invalid GPU count")
		assert.Nil(t, selected)
		assert.Nil(t, score)
	})
}

// ============================================================================
// SECTION 3: EDGE CASE TESTS (Critical for Stability)
// ============================================================================

// TestEdgeCases tests edge cases that could cause panics
func TestEdgeCases(t *testing.T) {
	ctx := context.Background()
	redisClient, err := redis.NewRedisClient("localhost:6379", "", 0)
	require.NoError(t, err)
	defer redisClient.Close()

	discovery := gpu.NewGPUDiscovery(redisClient)
	topoMgr := gpu.NewGPUTopologyManager(redisClient, discovery)

	t.Run("Nil topology should not panic", func(t *testing.T) {
		// This should not panic even with nil topology
		assert.NotPanics(t, func() {
			topology := &common.GPUTopology{
				NVLinkPairs: nil,
				GPUToNUMA:   nil,
				PCIeGen:     nil,
			}
			topoMgr.ScoreGPUPlacement(0, 1, topology)
		})
	})

	t.Run("Single GPU selection should not access index 1", func(t *testing.T) {
		// This was THE BUG - requesting 1 GPU tried to access indices[1]
		availableGPUs := []*common.GPUDevice{
			{Index: 0, UUID: "GPU-0", MemoryGB: 40.0, AvailableMemGB: 40.0, IsHealthy: true},
		}

		// Should NOT panic
		assert.NotPanics(t, func() {
			topoMgr.SelectBestGPUSet(ctx, availableGPUs, 1, true, true)
		})
	})
}

// ============================================================================
// SECTION 4: TOPOLOGY CACHING TESTS
// ============================================================================

// TestTopologyCache tests Redis caching of topology
func TestTopologyCache(t *testing.T) {
	ctx := context.Background()
	redisClient, err := redis.NewRedisClient("localhost:6379", "", 0)
	require.NoError(t, err)
	defer redisClient.Close()

	discovery := gpu.NewGPUDiscovery(redisClient)
	topoMgr := gpu.NewGPUTopologyManager(redisClient, discovery)

	t.Run("Clear topology cache", func(t *testing.T) {
		// Clear any existing cache
		err := topoMgr.ClearTopologyCache(ctx)
		assert.NoError(t, err, "Should clear cache without error")
	})

	t.Run("Detect topology creates cache", func(t *testing.T) {
		// This will attempt to detect topology
		// Even if detection fails, it should create an empty topology in cache
		topology, err := topoMgr.DetectTopology(ctx)

		// Should not error (may return empty topology)
		assert.NoError(t, err, "DetectTopology should not error")
		assert.NotNil(t, topology, "Topology should not be nil")
		assert.NotNil(t, topology.NVLinkPairs, "NVLinkPairs should be initialized")
		assert.NotNil(t, topology.GPUToNUMA, "GPUToNUMA should be initialized")
		assert.NotNil(t, topology.PCIeGen, "PCIeGen should be initialized")
	})

	t.Run("Second detect uses cache", func(t *testing.T) {
		// Second call should be faster (cache hit)
		start := time.Now()
		topology, err := topoMgr.DetectTopology(ctx)
		duration := time.Since(start)

		assert.NoError(t, err)
		assert.NotNil(t, topology)
		assert.Less(t, duration, 100*time.Millisecond, "Cache hit should be fast")
	})
}
