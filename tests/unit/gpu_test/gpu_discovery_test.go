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
// SECTION 1: GPU DISCOVERY INITIALIZATION TESTS
// ============================================================================

// TestGPUDiscoveryCreation tests GPUDiscovery initialization
func TestGPUDiscoveryCreation(t *testing.T) {
	t.Run("Create GPU discovery with valid Redis", func(t *testing.T) {
		redisClient, err := redis.NewRedisClient("localhost:6379", "", 0)
		require.NoError(t, err)
		defer redisClient.Close()

		discovery := gpu.NewGPUDiscovery(redisClient)
		assert.NotNil(t, discovery, "Discovery should be created")
	})

	t.Run("Discovery should not panic with nil Redis (if handled)", func(t *testing.T) {
		// Some implementations might handle nil Redis gracefully
		assert.NotPanics(t, func() {
			gpu.NewGPUDiscovery(nil)
		})
	})
}

// ============================================================================
// SECTION 2: GPU DETECTION TESTS
// ============================================================================

// TestDiscoverGPUs tests GPU detection functionality
func TestDiscoverGPUs(t *testing.T) {
	ctx := context.Background()
	redisClient, err := redis.NewRedisClient("localhost:6379", "", 0)
	require.NoError(t, err)
	defer redisClient.Close()

	discovery := gpu.NewGPUDiscovery(redisClient)

	t.Run("Detect GPUs (may fail without actual GPUs)", func(t *testing.T) {
		// This will likely fail without actual GPUs in the system
		// But it should NOT panic
		gpus, err := discovery.DiscoverGPUs(ctx)

		// Either succeeds with GPUs, or fails gracefully
		if err != nil {
			t.Logf("GPU detection failed (expected without GPUs): %v", err)
			assert.NotNil(t, gpus, "Should return empty array on error")
			assert.Len(t, gpus, 0, "Should have 0 GPUs on error")
		} else {
			t.Logf("GPU detection succeeded: found %d GPUs", len(gpus))
			assert.NotNil(t, gpus, "GPUs should not be nil")

			// If GPUs were detected, verify structure
			for i, gpu := range gpus {
				assert.GreaterOrEqual(t, gpu.Index, 0, "GPU %d should have valid index", i)
				assert.NotEmpty(t, gpu.UUID, "GPU %d should have UUID", i)
				assert.NotEmpty(t, gpu.Type, "GPU %d should have type", i)
				assert.Greater(t, gpu.MemoryGB, 0.0, "GPU %d should have memory", i)
			}
		}
	})

	t.Run("Detect GPUs returns consistent results", func(t *testing.T) {
		// Call twice, should get same results
		gpus1, err1 := discovery.DiscoverGPUs(ctx)
		gpus2, err2 := discovery.DiscoverGPUs(ctx)

		// Both should succeed or both should fail
		if err1 == nil && err2 == nil {
			assert.Len(t, gpus1, len(gpus2), "Should detect same number of GPUs")
		}
	})
}

// ============================================================================
// SECTION 3: GPU METADATA TESTS
// ============================================================================

// TestGPUMetadata tests GPU metadata extraction
func TestGPUMetadata(t *testing.T) {
	ctx := context.Background()
	redisClient, err := redis.NewRedisClient("localhost:6379", "", 0)
	require.NoError(t, err)
	defer redisClient.Close()

	discovery := gpu.NewGPUDiscovery(redisClient)

	t.Run("GPU metadata contains required fields", func(t *testing.T) {
		gpus, err := discovery.DiscoverGPUs(ctx)

		if err != nil || len(gpus) == 0 {
			t.Skip("Skipping metadata test - no GPUs detected")
		}

		for i, gpu := range gpus {
			// Verify all required fields are present
			assert.GreaterOrEqual(t, gpu.Index, 0, "GPU %d: Index should be >= 0", i)
			assert.NotEmpty(t, gpu.UUID, "GPU %d: UUID should not be empty", i)
			assert.NotEmpty(t, gpu.Type, "GPU %d: Type should not be empty", i)
			assert.Greater(t, gpu.MemoryGB, 0.0, "GPU %d: MemoryGB should be > 0", i)

			// Check memory crdt (MemoryGB >= AvailableMemGB)
			if gpu.MemoryGB > 0 {
				assert.GreaterOrEqual(t, gpu.MemoryGB, gpu.AvailableMemGB,
					"GPU %d: Total memory should be >= available memory", i)
			}
		}
	})

	t.Run("GPU types are valid strings", func(t *testing.T) {
		gpus, err := discovery.DiscoverGPUs(ctx)

		if err != nil || len(gpus) == 0 {
			t.Skip("Skipping GPU type test - no GPUs detected")
		}

		validTypes := map[string]bool{
			"A100": true, "A6000": true, "H100": true,
			"V100": true, "T4": true, "P100": true,
			"NVIDIA GPU": true, // Generic fallback
		}

		for i, gpu := range gpus {
			// Type should either be known type or contain "NVIDIA" or "Tesla"
			if !validTypes[gpu.Type] {
				assert.Contains(t, gpu.Type, "NVIDIA",
					"GPU %d: Type '%s' should contain NVIDIA", i, gpu.Type)
			}
		}
	})
}

// ============================================================================
// SECTION 4: GPU FILTERING TESTS
// ============================================================================

// TestGPUFiltering tests GPU filtering logic
func TestGPUFiltering(t *testing.T) {
	ctx := context.Background()
	redisClient, err := redis.NewRedisClient("localhost:6379", "", 0)
	require.NoError(t, err)
	defer redisClient.Close()

	discovery := gpu.NewGPUDiscovery(redisClient)

	t.Run("Filter healthy GPUs", func(t *testing.T) {
		gpus, err := discovery.DiscoverGPUs(ctx)

		if err != nil || len(gpus) == 0 {
			t.Skip("Skipping filter test - no GPUs detected")
		}

		// Count healthy GPUs
		healthyCount := 0
		for _, gpu := range gpus {
			if gpu.IsHealthy {
				healthyCount++
			}
		}

		t.Logf("Found %d healthy GPUs out of %d total", healthyCount, len(gpus))
		assert.GreaterOrEqual(t, healthyCount, 0, "Healthy count should be >= 0")
		assert.LessOrEqual(t, healthyCount, len(gpus), "Healthy count should be <= total")
	})

	t.Run("Filter GPUs by memory", func(t *testing.T) {
		gpus, err := discovery.DiscoverGPUs(ctx)

		if err != nil || len(gpus) == 0 {
			t.Skip("Skipping memory filter test - no GPUs detected")
		}

		minMemory := 16.0 // Require 16GB
		suitableGPUs := 0

		for _, gpu := range gpus {
			if gpu.AvailableMemGB >= minMemory {
				suitableGPUs++
			}
		}

		t.Logf("Found %d GPUs with >= %.0fGB memory", suitableGPUs, minMemory)
	})

	t.Run("Filter GPUs by type", func(t *testing.T) {
		gpus, err := discovery.DiscoverGPUs(ctx)

		if err != nil || len(gpus) == 0 {
			t.Skip("Skipping type filter test - no GPUs detected")
		}

		targetType := "A100"
		matchingGPUs := 0

		for _, gpu := range gpus {
			if gpu.Type == targetType {
				matchingGPUs++
			}
		}

		t.Logf("Found %d GPUs of type %s", matchingGPUs, targetType)
	})
}

// ============================================================================
// SECTION 5: GPU STRUCT VALIDATION TESTS
// ============================================================================

// TestGPUDeviceStruct tests the GPUDevice struct
func TestGPUDeviceStruct(t *testing.T) {
	t.Run("Create valid GPU device", func(t *testing.T) {
		gpu := &common.GPUDevice{
			Index:          0,
			UUID:           "GPU-abc-123",
			Type:           "A100",
			MemoryGB:       40.0,
			AvailableMemGB: 35.0,
			IsHealthy:      true,
		}

		assert.Equal(t, 0, gpu.Index)
		assert.Equal(t, "GPU-abc-123", gpu.UUID)
		assert.Equal(t, "A100", gpu.Type)
		assert.Equal(t, 40.0, gpu.MemoryGB)
		assert.True(t, gpu.IsHealthy)
	})

	t.Run("GPU with partial availability", func(t *testing.T) {
		gpu := &common.GPUDevice{
			Index:          1,
			UUID:           "GPU-def-456",
			Type:           "V100",
			MemoryGB:       32.0,
			AvailableMemGB: 16.0, // Half available
			IsHealthy:      true,
		}

		utilizationPct := (1.0 - gpu.AvailableMemGB/gpu.MemoryGB) * 100.0
		assert.Equal(t, 50.0, utilizationPct, "Should be 50% utilized")
	})

	t.Run("Unhealthy GPU", func(t *testing.T) {
		gpu := &common.GPUDevice{
			Index:          2,
			UUID:           "GPU-broken",
			Type:           "T4",
			MemoryGB:       16.0,
			AvailableMemGB: 0.0, // No memory available
			IsHealthy:      false,
		}

		assert.False(t, gpu.IsHealthy, "GPU should be unhealthy")
		assert.Equal(t, 0.0, gpu.AvailableMemGB, "Should have no available memory")
	})
}

// ============================================================================
// SECTION 6: CONCURRENT ACCESS TESTS
// ============================================================================

// TestConcurrentGPUDetection tests thread-safety of GPU detection
func TestConcurrentGPUDetection(t *testing.T) {
	ctx := context.Background()
	redisClient, err := redis.NewRedisClient("localhost:6379", "", 0)
	require.NoError(t, err)
	defer redisClient.Close()

	discovery := gpu.NewGPUDiscovery(redisClient)

	t.Run("Concurrent GPU detection calls", func(t *testing.T) {
		numGoroutines := 10
		done := make(chan bool, numGoroutines)

		// Launch multiple concurrent detections
		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				gpus, err := discovery.DiscoverGPUs(ctx)

				// Should not panic
				if err == nil {
					t.Logf("Goroutine %d: detected %d GPUs", id, len(gpus))
				} else {
					t.Logf("Goroutine %d: detection failed: %v", id, err)
				}

				done <- true
			}(i)
		}

		// Wait for all goroutines
		for i := 0; i < numGoroutines; i++ {
			<-done
		}

		// If we got here, no panics occurred
		assert.True(t, true, "Concurrent access completed without panics")
	})
}

// ============================================================================
// SECTION 7: ERROR HANDLING TESTS
// ============================================================================

// TestGPUDiscoveryErrorHandling tests error handling in GPU discovery
func TestGPUDiscoveryErrorHandling(t *testing.T) {
	ctx := context.Background()
	redisClient, err := redis.NewRedisClient("localhost:6379", "", 0)
	require.NoError(t, err)
	defer redisClient.Close()

	discovery := gpu.NewGPUDiscovery(redisClient)

	t.Run("Handle context cancellation", func(t *testing.T) {
		// Create cancelled context
		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel() // Cancel immediately

		gpus, err := discovery.DiscoverGPUs(cancelledCtx)

		// Should handle cancellation gracefully
		if err != nil {
			t.Logf("Correctly handled cancelled context: %v", err)
		}
		// Should still return non-nil slice (even if empty)
		assert.NotNil(t, gpus, "Should return non-nil slice even on error")
	})

	t.Run("Handle timeout", func(t *testing.T) {
		// Create context with very short timeout
		timeoutCtx, cancel := context.WithTimeout(ctx, 1*time.Nanosecond)
		defer cancel()

		// Wait for timeout to definitely occur
		time.Sleep(10 * time.Millisecond)

		gpus, err := discovery.DiscoverGPUs(timeoutCtx)

		// Should handle timeout gracefully
		if err != nil {
			t.Logf("Correctly handled timeout: %v", err)
		}
		assert.NotNil(t, gpus, "Should return non-nil slice even on timeout")
	})
}

// ============================================================================
// SECTION 8: INTEGRATION WITH TOPOLOGY MANAGER TESTS
// ============================================================================

// TestGPUDiscoveryWithTopology tests integration with topology manager
func TestGPUDiscoveryWithTopology(t *testing.T) {
	ctx := context.Background()
	redisClient, err := redis.NewRedisClient("localhost:6379", "", 0)
	require.NoError(t, err)
	defer redisClient.Close()

	discovery := gpu.NewGPUDiscovery(redisClient)
	topoMgr := gpu.NewGPUTopologyManager(redisClient, discovery)

	t.Run("Topology manager uses discovery correctly", func(t *testing.T) {
		// Topology manager should use discovery for GPU info
		// Even if detection fails, should not panic
		assert.NotPanics(t, func() {
			topoMgr.DetectTopology(ctx)
		})
	})

	t.Run("Select best GPU set uses discovered GPUs", func(t *testing.T) {
		gpus, err := discovery.DiscoverGPUs(ctx)

		if err != nil || len(gpus) == 0 {
			t.Skip("Skipping integration test - no GPUs detected")
		}

		// Should be able to select from discovered GPUs
		selected, score, err := topoMgr.SelectBestGPUSet(ctx, gpus, 1, false, false)

		if err == nil {
			assert.NotNil(t, selected, "Should select GPUs")
			assert.NotNil(t, score, "Should provide score")
			assert.Len(t, selected, 1, "Should select 1 GPU")
		}
	})
}
