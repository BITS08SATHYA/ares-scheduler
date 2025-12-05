// // File: pkg/gpu/discovery_test.go & topology_test.go
// // Layer 5: GPU Discovery and Topology Test Suite
// // Coverage: GPU discovery, health checks, topology detection, affinity scoring
// // Test Approach: Mock nvidia-smi, test topology parsing, scoring algorithms
package unit

//
//import (
//	"context"
//	"fmt"
//	"github.com/BITS08SATHYA/ares-scheduler/pkg/gpu"
//	"testing"
//
//	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
//	"github.com/stretchr/testify/assert"
//)
//
//// ============================================================================
//// MOCK REDIS CLIENT
//// ============================================================================
//
//type MockRedisForGPU struct {
//	store map[string]string
//}
//
//func NewMockRedisForGPU() *MockRedisForGPU {
//	return &MockRedisForGPU{
//		store: make(map[string]string),
//	}
//}
//
//func (m *MockRedisForGPU) Set(ctx context.Context, key string, value interface{}, ttl interface{}) error {
//	m.store[key] = fmt.Sprintf("%v", value)
//	return nil
//}
//
//func (m *MockRedisForGPU) Get(ctx context.Context, key string) (string, error) {
//	if val, ok := m.store[key]; ok {
//		return val, nil
//	}
//	return "", nil
//}
//
//func (m *MockRedisForGPU) Del(ctx context.Context, keys ...string) error {
//	for _, key := range keys {
//		delete(m.store, key)
//	}
//	return nil
//}
//
//func (m *MockRedisForGPU) Keys(ctx context.Context, pattern string) ([]string, error) {
//	var keys []string
//	for k := range m.store {
//		keys = append(keys, k)
//	}
//	return keys, nil
//}
//
//func (m *MockRedisForGPU) Close() error {
//	return nil
//}
//
//// ============================================================================
//// GPU DISCOVERY TESTS
//// ============================================================================
//
//func TestGPUDiscoveryNew(t *testing.T) {
//	mockRedis := NewMockRedisForGPU()
//	discovery := gpu.NewGPUDiscovery(mockRedis)
//
//	assert.NotNil(t, discovery)
//	assert.Equal(t, mockRedis, discovery.redisClient)
//}
//
//// Test: Parse GPU line from nvidia-smi output
//func TestParseGPULineValid(t *testing.T) {
//	mockRedis := NewMockRedisForGPU()
//	discovery := gpu.NewGPUDiscovery(mockRedis)
//
//	// Example nvidia-smi output line
//	// Format: index,uuid,type,memory_total_MB,memory_free_MB,utilization_pct,temp_celsius
//	line := "0,GPU-abc123,NVIDIA A100-SXM4-80GB,81920,65536,25.0,45"
//
//	gpu, err := discovery.parseGPULine(line)
//
//	assert.NoError(t, err)
//	assert.NotNil(t, gpu)
//	assert.Equal(t, 0, gpu.Index)
//	assert.Equal(t, "A100", gpu.Type)
//	assert.Equal(t, 80.0, gpu.MemoryGB)       // 81920 MB / 1024
//	assert.Equal(t, 64.0, gpu.AvailableMemGB) // 65536 MB / 1024
//	assert.Equal(t, 25.0, gpu.UtilizationPercent)
//	assert.Equal(t, 45.0, gpu.TemperatureCelsius)
//	assert.True(t, gpu.IsHealthy)
//}
//
//// Test: Parse multiple GPU types
//func TestParseGPUTypesExtracts(t *testing.T) {
//	mockRedis := NewMockRedisForGPU()
//	discovery := gpu.NewGPUDiscovery(mockRedis)
//
//	testCases := []struct {
//		name     string
//		line     string
//		expected string
//	}{
//		{
//			name:     "A100",
//			line:     "0,GPU-1,NVIDIA A100-SXM4-80GB,81920,65536,25.0,45",
//			expected: "A100",
//		},
//		{
//			name:     "H100",
//			line:     "1,GPU-2,NVIDIA H100 PCIe 80GB,81920,65536,30.0,50",
//			expected: "H100",
//		},
//		{
//			name:     "V100",
//			line:     "2,GPU-3,NVIDIA V100-SXM2-32GB,32768,16384,50.0,60",
//			expected: "V100",
//		},
//		{
//			name:     "T4",
//			line:     "3,GPU-4,NVIDIA T4,16384,8192,75.0,70",
//			expected: "T4",
//		},
//	}
//
//	for _, tc := range testCases {
//		t.Run(tc.name, func(t *testing.T) {
//			gpu, err := discovery.parseGPULine(tc.line)
//			assert.NoError(t, err)
//			assert.Equal(t, tc.expected, gpu.Type)
//		})
//	}
//}
//
//// Test: Invalid GPU line returns error
//func TestParseGPULineInvalid(t *testing.T) {
//	mockRedis := NewMockRedisForGPU()
//	discovery := gpu.NewGPUDiscovery(mockRedis)
//
//	testCases := []string{
//		"invalid",                          // Too few fields
//		"0,GPU-1,A100",                     // Incomplete
//		"abc,GPU-1,A100,80000,40000,25,45", // Non-numeric index
//	}
//
//	for _, line := range testCases {
//		gpu, err := discovery.parseGPULine(line)
//		assert.Error(t, err, "should error for: %s", line)
//		assert.Nil(t, gpu)
//	}
//}
//
//// Test: GPU health check
//func TestCheckGPUHealth(t *testing.T) {
//	mockRedis := NewMockRedisForGPU()
//	discovery := gpu.NewGPUDiscovery(mockRedis)
//
//	ctx := context.Background()
//
//	// Create healthy GPU
//	healthyGPU := &common.GPUDevice{
//		Index:              0,
//		Type:               "A100",
//		TemperatureCelsius: 45.0,
//		AvailableMemGB:     50.0,
//		IsHealthy:          true,
//	}
//
//	gpus := []*common.GPUDevice{healthyGPU}
//	checked, err := discovery.CheckGPUHealth(ctx, gpus)
//
//	assert.NoError(t, err)
//	assert.Len(t, checked, 1)
//	assert.True(t, checked[0].IsHealthy)
//}
//
//// Test: GPU too hot marked unhealthy
//func TestGPUHealthCheckOverheated(t *testing.T) {
//	mockRedis := NewMockRedisForGPU()
//	discovery := gpu.NewGPUDiscovery(mockRedis)
//
//	ctx := context.Background()
//
//	// GPU too hot
//	hotGPU := &common.GPUDevice{
//		Index:              0,
//		Type:               "A100",
//		TemperatureCelsius: 90.0, // > 85°C threshold
//		AvailableMemGB:     50.0,
//		IsHealthy:          true,
//	}
//
//	gpus := []*common.GPUDevice{hotGPU}
//	checked, _ := discovery.CheckGPUHealth(ctx, gpus)
//
//	assert.False(t, checked[0].IsHealthy, "GPU over 85°C should be unhealthy")
//}
//
//// Test: GPU low memory marked unhealthy
//func TestGPUHealthCheckLowMemory(t *testing.T) {
//	mockRedis := NewMockRedisForGPU()
//	discovery := gpu.NewGPUDiscovery(mockRedis)
//
//	ctx := context.Background()
//
//	// GPU with critically low memory
//	lowMemGPU := &common.GPUDevice{
//		Index:              0,
//		Type:               "A100",
//		TemperatureCelsius: 45.0,
//		AvailableMemGB:     0.5, // < 1GB threshold
//		IsHealthy:          true,
//	}
//
//	gpus := []*common.GPUDevice{lowMemGPU}
//	checked, _ := discovery.CheckGPUHealth(ctx, gpus)
//
//	assert.False(t, checked[0].IsHealthy, "GPU with <1GB memory should be unhealthy")
//}
//
//// Test: Filter GPUs by type
//func TestFilterGPUsByType(t *testing.T) {
//	mockRedis := NewMockRedisForGPU()
//	discovery := gpu.NewGPUDiscovery(mockRedis)
//
//	gpus := []*common.GPUDevice{
//		{Index: 0, Type: "A100", IsHealthy: true},
//		{Index: 1, Type: "H100", IsHealthy: true},
//		{Index: 2, Type: "A100", IsHealthy: true},
//		{Index: 3, Type: "T4", IsHealthy: true},
//		{Index: 4, Type: "A100", IsHealthy: false}, // Unhealthy, should be filtered
//	}
//
//	filtered := discovery.FilterGPUsByType(gpus, "A100")
//
//	assert.Len(t, filtered, 2)
//	for _, gpu := range filtered {
//		assert.Equal(t, "A100", gpu.Type)
//		assert.True(t, gpu.IsHealthy)
//	}
//}
//
//// Test: Filter GPUs by memory
//func TestFilterGPUsByMemory(t *testing.T) {
//	mockRedis := NewMockRedisForGPU()
//	discovery := gpu.NewGPUDiscovery(mockRedis)
//
//	gpus := []*common.GPUDevice{
//		{Index: 0, AvailableMemGB: 80.0, IsHealthy: true},
//		{Index: 1, AvailableMemGB: 32.0, IsHealthy: true},
//		{Index: 2, AvailableMemGB: 50.0, IsHealthy: true},
//		{Index: 3, AvailableMemGB: 0.5, IsHealthy: true},
//	}
//
//	filtered := discovery.FilterGPUsByMemory(gpus, 40.0) // Min 40GB
//
//	assert.Len(t, filtered, 2)
//	assert.Equal(t, int64(80), int64(filtered[0].AvailableMemGB))
//	assert.Equal(t, int64(50), int64(filtered[1].AvailableMemGB))
//}
//
//// ============================================================================
//// GPU TOPOLOGY TESTS
//// ============================================================================
//
//func TestGPUTopologyManagerNew(t *testing.T) {
//	mockRedis := NewMockRedisForGPU()
//	discovery := gpu.NewGPUDiscovery(mockRedis)
//	topology := gpu.NewGPUTopologyManager(mockRedis, discovery)
//
//	assert.NotNil(t, topology)
//}
//
//// Test: Parse NVLink matrix
//func TestParseNVLinkMatrix(t *testing.T) {
//	mockRedis := NewMockRedisForGPU()
//	topology := gpu.NewGPUTopologyManager(mockRedis, nil)
//
//	// Example nvidia-smi topo output
//	matrixOutput := `
//GPU0	GPU1	GPU2	GPU3	CPU Affinity	NUMA Affinity
//GPU0	X	NV1	NV1	SYS	0-63	0
//GPU1	NV1	X	SYS	NV1	0-63	0
//GPU2	NV1	SYS	X	NV1	0-63	1
//GPU3	SYS	NV1	NV1	X	0-63	1
//`
//
//	pairs, err := topology.parseNVLinkMatrix(matrixOutput)
//
//	assert.NoError(t, err)
//	assert.NotEmpty(t, pairs)
//	// Should find NVLink connections
//	assert.Greater(t, len(pairs), 0)
//}
//
//// Test: Score GPU placement with NVLink bonus
//func TestScoreGPUPlacementNVLink(t *testing.T) {
//	mockRedis := NewMockRedisForGPU()
//	topology := gpu.NewGPUTopologyManager(mockRedis, nil)
//
//	// Create topology with NVLink connection
//	topo := &common.GPUTopology{
//		NVLinkPairs: [][]int{{0, 1}}, // GPUs 0 and 1 have NVLink
//		GPUToNUMA:   map[int]int{0: 0, 1: 0},
//		PCIeGen:     map[int]int{0: 4, 1: 4},
//	}
//
//	score := topology.ScoreGPUPlacement(0, 1, topo)
//
//	assert.True(t, score.HasNVLink)
//	assert.Greater(t, score.Score, 50.0) // NVLink bonus is +50
//}
//
//// Test: Score GPU placement without NVLink (PCIe only)
//func TestScoreGPUPlacementPCIeOnly(t *testing.T) {
//	mockRedis := NewMockRedisForGPU()
//	topology := gpu.NewGPUTopologyManager(mockRedis, nil)
//
//	topo := &common.GPUTopology{
//		NVLinkPairs: [][]int{},
//		GPUToNUMA:   map[int]int{0: 0, 1: 1}, // Different NUMA
//		PCIeGen:     map[int]int{0: 4, 1: 4},
//	}
//
//	score := topology.ScoreGPUPlacement(0, 1, topo)
//
//	assert.False(t, score.HasNVLink)
//	assert.False(t, score.SameNUMA)
//	assert.Less(t, score.Score, 50.0) // No NVLink bonus
//}
//
//// Test: Score GPU placement with same NUMA
//func TestScoreGPUPlacementSameNUMA(t *testing.T) {
//	mockRedis := NewMockRedisForGPU()
//	topology := gpu.NewGPUTopologyManager(mockRedis, nil)
//
//	topo := &common.GPUTopology{
//		NVLinkPairs: [][]int{},
//		GPUToNUMA:   map[int]int{0: 0, 1: 0}, // Same NUMA
//		PCIeGen:     map[int]int{0: 4, 1: 4},
//	}
//
//	score := topology.ScoreGPUPlacement(0, 1, topo)
//
//	assert.True(t, score.SameNUMA)
//	assert.Greater(t, score.Score, 30.0) // NUMA bonus is +15
//}
//
//// Test: Score GPU set (multiple GPUs)
//func TestScoreGPUSet(t *testing.T) {
//	mockRedis := NewMockRedisForGPU()
//	topology := gpu.NewGPUTopologyManager(mockRedis, nil)
//
//	topo := &common.GPUTopology{
//		NVLinkPairs: [][]int{{0, 1}, {1, 2}},
//		GPUToNUMA:   map[int]int{0: 0, 1: 0, 2: 0},
//		PCIeGen:     map[int]int{0: 4, 1: 4, 2: 4},
//	}
//
//	// Score set of 3 GPUs all connected
//	gpuIndices := []int{0, 1, 2}
//	setScore := topology.ScoreGPUSet(gpuIndices, topo)
//
//	assert.Greater(t, setScore, 0.0)
//	assert.LessOrEqual(t, setScore, 100.0)
//}
//
//// Test: Select best GPU set
//func TestSelectBestGPUSet(t *testing.T) {
//	mockRedis := NewMockRedisForGPU()
//	discovery := gpu.NewGPUDiscovery(mockRedis)
//	topology := gpu.NewGPUTopologyManager(mockRedis, discovery)
//
//	ctx := context.Background()
//
//	// Create GPUs
//	gpus := []*common.GPUDevice{
//		{Index: 0, Type: "A100", IsHealthy: true, AvailableMemGB: 80.0},
//		{Index: 1, Type: "A100", IsHealthy: true, AvailableMemGB: 80.0},
//		{Index: 2, Type: "A100", IsHealthy: true, AvailableMemGB: 80.0},
//		{Index: 3, Type: "A100", IsHealthy: true, AvailableMemGB: 80.0},
//	}
//
//	indices, score, err := topology.SelectBestGPUSet(ctx, gpus, 2, true, true)
//
//	assert.NoError(t, err)
//	assert.Len(t, indices, 2)
//	assert.NotNil(t, score)
//	assert.Greater(t, score.Score, 0.0)
//}
//
//// Test: GPU node has GPUs
//func TestNodeHasGPUs(t *testing.T) {
//	mockRedis := NewMockRedisForGPU()
//	discovery := gpu.NewGPUDiscovery(mockRedis)
//
//	ctx := context.Background()
//
//	// Mock some GPUs in cache
//	gpus := []*common.GPUDevice{
//		{Index: 0, Type: "A100", IsHealthy: true},
//	}
//
//	// This would need real nvidia-smi or mocked exec
//	// For now, test the filtering logic
//
//	filtered := discovery.FilterGPUsByType(gpus, "A100")
//	assert.Len(t, filtered, 1)
//}
//
//// Test: Validate GPU indices
//func TestValidateGPUIndices(t *testing.T) {
//	mockRedis := NewMockRedisForGPU()
//	discovery := gpu.NewGPUDiscovery(mockRedis)
//
//	gpus := []*common.GPUDevice{
//		{Index: 0, IsHealthy: true},
//		{Index: 1, IsHealthy: true},
//		{Index: 2, IsHealthy: true},
//	}
//
//	// Valid indices
//	err := discovery.ValidateGPUIndices(gpus, []int{0, 2})
//	assert.NoError(t, err)
//
//	// Invalid index
//	err = discovery.ValidateGPUIndices(gpus, []int{0, 5})
//	assert.Error(t, err)
//
//	// Unhealthy GPU
//	gpus[1].IsHealthy = false
//	err = discovery.ValidateGPUIndices(gpus, []int{1})
//	assert.Error(t, err)
//}
//
//// ============================================================================
//// BENCHMARK TESTS
//// ============================================================================
//
//func BenchmarkParseGPULine(b *testing.B) {
//	mockRedis := NewMockRedisForGPU()
//	discovery := gpu.NewGPUDiscovery(mockRedis)
//
//	line := "0,GPU-abc123,NVIDIA A100-SXM4-80GB,81920,65536,25.0,45"
//
//	b.ResetTimer()
//
//	for i := 0; i < b.N; i++ {
//		_, _ = discovery.parseGPULine(line)
//	}
//}
//
//func BenchmarkFilterGPUsByType(b *testing.B) {
//	mockRedis := NewMockRedisForGPU()
//	discovery := gpu.NewGPUDiscovery(mockRedis)
//
//	gpus := make([]*common.GPUDevice, 100)
//	for i := 0; i < 100; i++ {
//		gpuType := "A100"
//		if i%3 == 0 {
//			gpuType = "H100"
//		}
//		gpus[i] = &common.GPUDevice{
//			Index:     i,
//			Type:      gpuType,
//			IsHealthy: true,
//		}
//	}
//
//	b.ResetTimer()
//
//	for i := 0; i < b.N; i++ {
//		_ = discovery.FilterGPUsByType(gpus, "A100")
//	}
//}
//
//func BenchmarkScoreGPUPlacement(b *testing.B) {
//	mockRedis := NewMockRedisForGPU()
//	topology := gpu.NewGPUTopologyManager(mockRedis, nil)
//
//	topo := &common.GPUTopology{
//		NVLinkPairs: [][]int{{0, 1}},
//		GPUToNUMA:   map[int]int{0: 0, 1: 0},
//		PCIeGen:     map[int]int{0: 4, 1: 4},
//	}
//
//	b.ResetTimer()
//
//	for i := 0; i < b.N; i++ {
//		_ = topology.ScoreGPUPlacement(0, 1, topo)
//	}
//}
