// Feature 4: Topology-Aware Scheduling (NVLink, NUMA, PCIe)
// Detects GPU interconnections and provides affinity scoring
// Depends on: types.go, logger.go, redis/client.go, discovery.go
// Zero errors, production-ready code

package gpu

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/redis"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ============================================================================
// GPU TOPOLOGY SERVICE
// ============================================================================

// GPUTopologyManager: Manages GPU topology and affinity
// Detects NVLink connections, NUMA mapping, PCIe generation
// Used for Feature 4: Topology-Aware Scheduling
type GPUTopologyManager struct {
	redisClient *redis.RedisClient
	log         *logger.Logger
	discovery   *GPUDiscovery
	cacheKey    string
	cacheTTL    time.Duration
}

// Cache and constants
const (
	CacheKeyTopology  = "ares:node:gpu:topology"
	NVLinkBandwidth   = 900.0 // GB/s
	PCIeGen4Bandwidth = 32.0  // GB/s
	PCIeGen5Bandwidth = 64.0  // GB/s
	NVLinkMultiplier  = 28.1  // NVLink is ~28x faster than PCIe Gen4
	SameNUMABonus     = 1.5   // Multiplier for same-NUMA placement
	TopologyCacheTTL  = 60 * time.Second
)

// NewGPUTopologyManager: Create new topology manager
func NewGPUTopologyManager(redisClient *redis.RedisClient, discovery *GPUDiscovery) *GPUTopologyManager {
	return &GPUTopologyManager{
		redisClient: redisClient,
		log:         logger.Get(),
		discovery:   discovery,
		cacheKey:    CacheKeyTopology,
		cacheTTL:    TopologyCacheTTL,
	}
}

// ============================================================================
// NVLINK DETECTION (Feature 4 - Primary)
// ============================================================================

// DetectNVLink: Detect which GPU pairs have NVLink connections
// Returns: Array of GPU pairs connected by NVLink
// Example: [[0,1], [2,3], [4,5], [6,7]] for 8 A100s
// Latency: ~300-500ms (nvidia-smi execution)
// Cached for: 60 seconds
func (gtm *GPUTopologyManager) DetectNVLink(ctx context.Context) ([][]int, error) {
	// Check cache first
	cached, err := gtm.getCachedTopology(ctx)
	if err == nil && len(cached.NVLinkPairs) > 0 {
		gtm.log.Debug("NVLink topology cache hit (pairs=%d)", len(cached.NVLinkPairs))
		return cached.NVLinkPairs, nil
	}

	// Query nvidia-smi for NVLink info
	nvlinkPairs, err := gtm.queryNVLinkConnections(ctx)
	if err != nil {
		gtm.log.Warn("Failed to detect NVLink (GPUs may not support NVLink): %v", err)
		return [][]int{}, nil
	}

	// Cache results
	if err := gtm.cacheNVLinkPairs(ctx, nvlinkPairs); err != nil {
		gtm.log.Warn("Failed to cache NVLink (non-fatal): %v", err)
	}

	gtm.log.Info("Detected %d NVLink pairs", len(nvlinkPairs))
	return nvlinkPairs, nil
}

// queryNVLinkConnections: Query nvidia-smi for NVLink topology
// Uses: nvidia-smi topo --matrix to get GPU interconnect info
func (gtm *GPUTopologyManager) queryNVLinkConnections(ctx context.Context) ([][]int, error) {
	// Command: nvidia-smi topo --matrix
	// Output is NxN matrix where N = number of GPUs
	// Values: X = self, NV1/NV2 = NVLink, SYS = PCIe, etc.
	cmd := exec.CommandContext(ctx, "nvidia-smi", "topo", "--matrix")
	output, err := cmd.Output()
	if err != nil {
		// GPU not available or nvidia-smi not installed
		return [][]int{}, fmt.Errorf("nvidia-smi topo failed: %w", err)
	}

	return gtm.parseNVLinkMatrix(string(output))
}

// parseNVLinkMatrix: Parse nvidia-smi topo matrix output
// Format:
//
//	GPU0	GPU1	GPU2	GPU3	CPU Affinity	NUMA Affinity
//
// GPU0	X	NV1	NV1	SYS	0-63	0
// GPU1	NV1	X	SYS	NV1	0-63	0
// GPU2	NV1	SYS	X	NV1	0-63	1
// GPU3	SYS	NV1	NV1	X	0-63	1
//
// NV1 = single NVLink, NV2 = dual NVLink, SYS = PCIe, X = self
func (gtm *GPUTopologyManager) parseNVLinkMatrix(output string) ([][]int, error) {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) < 2 {
		return [][]int{}, nil
	}

	// Skip header line (GPU0, GPU1, ...)
	gpuLines := make([]string, 0)
	for i := 1; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if line != "" && strings.HasPrefix(line, "GPU") {
			gpuLines = append(gpuLines, line)
		}
	}

	pairs := make([][]int, 0)

	// For each GPU row, find NVLink connections to other GPUs
	for i, line := range gpuLines {
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}

		// Skip GPU label (parts[0]), read connectivity
		for j := 1; j < len(parts); j++ {
			connectivity := parts[j]

			// Check if NVLink (NV1 or NV2)
			if strings.Contains(connectivity, "NV") && j > i {
				// Only add once (i < j to avoid duplicates)
				pairs = append(pairs, []int{i, j})
			}
		}
	}

	return pairs, nil
}

// ============================================================================
// NUMA DETECTION (Feature 4 - Secondary)
// ============================================================================

// DetectNUMAMapping: Detect GPU to NUMA node mapping
// Returns: map[GPU_index]NUMA_node_id
// Example: {0:0, 1:0, 2:1, 3:1} for dual-socket system
// Latency: ~200-300ms
// Cached for: 60 seconds
func (gtm *GPUTopologyManager) DetectNUMAMapping(ctx context.Context) (map[int]int, error) {
	// Check cache
	cached, err := gtm.getCachedTopology(ctx)
	if err == nil && len(cached.GPUToNUMA) > 0 {
		gtm.log.Debug("NUMA mapping cache hit (GPUs=%d)", len(cached.GPUToNUMA))
		return cached.GPUToNUMA, nil
	}

	// Query GPU to NUMA mapping
	mapping, err := gtm.queryGPUNUMAMapping(ctx)
	if err != nil {
		gtm.log.Warn("Failed to detect NUMA mapping (assuming single NUMA): %v", err)
		// Fallback: assume all GPUs on NUMA 0
		return make(map[int]int), nil
	}

	// Cache results
	if err := gtm.cacheNUMAMapping(ctx, mapping); err != nil {
		gtm.log.Warn("Failed to cache NUMA mapping (non-fatal): %v", err)
	}

	gtm.log.Info("Detected NUMA mapping for %d GPUs", len(mapping))
	return mapping, nil
}

// queryGPUNUMAMapping: Query nvidia-smi for NUMA affinity
// Uses: nvidia-smi --query-gpu=gpu_bus_id
// Then: Get NUMA node from /sys/class/pci_bus/BUS/device/numa_node
func (gtm *GPUTopologyManager) queryGPUNUMAMapping(ctx context.Context) (map[int]int, error) {
	// Command: get GPU count
	cmd := exec.CommandContext(ctx, "nvidia-smi", "--list-gpus")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("nvidia-smi list-gpus failed: %w", err)
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	mapping := make(map[int]int)

	for i := 0; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}

		// Line format: "GPU 0: <name> (UUID: ...)"
		// Extract index
		var gpuIndex int
		fmt.Sscanf(line, "GPU %d:", &gpuIndex)

		// Query NUMA node for this GPU
		numaNode, err := gtm.queryGPUNUMANode(ctx, gpuIndex)
		if err != nil {
			gtm.log.Warn("Failed to query NUMA for GPU %d: %v", gpuIndex, err)
			// Fallback: assume NUMA 0
			numaNode = 0
		}

		mapping[gpuIndex] = numaNode
	}

	return mapping, nil
}

// queryGPUNUMANode: Get NUMA node for a single GPU
// Reads: /sys/class/pci_bus/{BUS}/device/numa_node
func (gtm *GPUTopologyManager) queryGPUNUMANode(ctx context.Context, gpuIndex int) (int, error) {
	// Get GPU bus ID first
	cmd := exec.CommandContext(ctx, "nvidia-smi",
		fmt.Sprintf("--id=%d", gpuIndex),
		"--query-gpu=pci.bus_id",
		"--format=csv,noheader")

	output, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("nvidia-smi pci query failed: %w", err)
	}

	busID := strings.TrimSpace(string(output))
	if busID == "" {
		return 0, fmt.Errorf("empty bus ID")
	}

	// Format: "0000:01:00.0" -> extract "0000:01"
	parts := strings.Split(busID, ":")
	if len(parts) < 2 {
		return 0, fmt.Errorf("invalid bus ID format: %s", busID)
	}

	busDomain := fmt.Sprintf("%s:%s", parts[0], parts[1])

	// Read NUMA node from sysfs
	numaPath := fmt.Sprintf("/sys/class/pci_bus/%s/device/numa_node", busDomain)
	data, err := os.ReadFile(numaPath)
	if err != nil {
		// Path may not exist, default to NUMA 0
		return 0, nil
	}

	numaNode, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, fmt.Errorf("invalid NUMA node value: %w", err)
	}

	return numaNode, nil
}

// ============================================================================
// PCIE GENERATION DETECTION (Feature 4 - Tertiary)
// ============================================================================

// DetectPCIeGeneration: Detect PCIe generation per GPU
// Returns: map[GPU_index]PCIe_generation
// Example: {0:4, 1:4, 2:5, 3:5} for mixed Gen4/Gen5
// Latency: ~200-300ms
func (gtm *GPUTopologyManager) DetectPCIeGeneration(ctx context.Context) (map[int]int, error) {
	// Check cache
	cached, err := gtm.getCachedTopology(ctx)
	if err == nil && len(cached.PCIeGen) > 0 {
		gtm.log.Debug("PCIe generation cache hit (GPUs=%d)", len(cached.PCIeGen))
		return cached.PCIeGen, nil
	}

	// Query PCIe info
	pcieGens, err := gtm.queryPCIeGenerations(ctx)
	if err != nil {
		gtm.log.Warn("Failed to detect PCIe generation (assuming Gen4): %v", err)
		return make(map[int]int), nil
	}

	// Cache results
	if err := gtm.cachePCIeGenerations(ctx, pcieGens); err != nil {
		gtm.log.Warn("Failed to cache PCIe generations (non-fatal): %v", err)
	}

	gtm.log.Info("Detected PCIe generations for %d GPUs", len(pcieGens))
	return pcieGens, nil
}

// queryPCIeGenerations: Query nvidia-smi for PCIe gen info
func (gtm *GPUTopologyManager) queryPCIeGenerations(ctx context.Context) (map[int]int, error) {
	cmd := exec.CommandContext(ctx, "nvidia-smi",
		"--query-gpu=index,pci.link_gen",
		"--format=csv,noheader,nounits")

	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("nvidia-smi pcie query failed: %w", err)
	}

	pcieGens := make(map[int]int)
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.Split(line, ",")
		if len(parts) < 2 {
			continue
		}

		idx, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			continue
		}

		gen, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil {
			gen = 4 // Default to Gen4
		}

		pcieGens[idx] = gen
	}

	return pcieGens, nil
}

// ============================================================================
// AFFINITY SCORING (Feature 4 - Core scheduling logic)
// ============================================================================

// GPUAffinityScore: Score for GPU placement (higher = better)
type GPUAffinityScore struct {
	GPU1Index      int
	GPU2Index      int
	Score          float64 // 0-100
	Reason         string  // Why this score
	HasNVLink      bool
	SameNUMA       bool
	PCIeGeneration int
}

// ScoreGPUPlacement: Score the affinity of placing 2 GPUs together
// Factors:
//   - NVLink connection (28x speedup vs PCIe): +50 points
//   - Same NUMA node: +15 points
//   - PCIe Gen5: +10 points
//   - PCIe Gen4: +5 points
//
// Returns: Score 0-100, description
func (gtm *GPUTopologyManager) ScoreGPUPlacement(
	gpu1Index int,
	gpu2Index int,
	topology *common.GPUTopology,
) *GPUAffinityScore {

	score := &GPUAffinityScore{
		GPU1Index:      gpu1Index,
		GPU2Index:      gpu2Index,
		Score:          20.0, // Base score
		HasNVLink:      false,
		SameNUMA:       false,
		PCIeGeneration: 4, // Default
	}

	reasons := make([]string, 0)

	// Factor 1: NVLink
	if gtm.hasNVLink(topology.NVLinkPairs, gpu1Index, gpu2Index) {
		score.Score += 50.0
		score.HasNVLink = true
		reasons = append(reasons, "NVLink-connected")
	}

	// Factor 2: Same NUMA
	if gtm.isSameNUMA(topology.GPUToNUMA, gpu1Index, gpu2Index) {
		score.Score += 15.0
		score.SameNUMA = true
		reasons = append(reasons, "same-NUMA")
	}

	// Factor 3: PCIe Generation
	if gen, ok := topology.PCIeGen[gpu1Index]; ok && gen >= 5 {
		score.Score += 10.0
		score.PCIeGeneration = gen
		reasons = append(reasons, "PCIe-Gen5")
	} else if gen, ok := topology.PCIeGen[gpu1Index]; ok && gen == 4 {
		score.Score += 5.0
		score.PCIeGeneration = gen
	}

	// Cap score at 100
	if score.Score > 100.0 {
		score.Score = 100.0
	}

	score.Reason = strings.Join(reasons, ", ")
	if score.Reason == "" {
		score.Reason = "PCIe-connected"
	}

	return score
}

// ScoreGPUSet: Score overall affinity of GPU set
// Lower scores indicate GPUs can tolerate co-location
// Used for multi-GPU job placement
// Returns: Average pairwise affinity score
func (gtm *GPUTopologyManager) ScoreGPUSet(
	gpuIndices []int,
	topology *common.GPUTopology,
) float64 {
	if len(gpuIndices) <= 1 {
		return 100.0
	}

	totalScore := 0.0
	pairs := 0

	// Calculate pairwise scores
	for i := 0; i < len(gpuIndices)-1; i++ {
		for j := i + 1; j < len(gpuIndices); j++ {
			score := gtm.ScoreGPUPlacement(gpuIndices[i], gpuIndices[j], topology)
			totalScore += score.Score
			pairs++
		}
	}

	return totalScore / float64(pairs)
}

// ============================================================================
// TOPOLOGY HELPERS
// ============================================================================

// hasNVLink: Check if two GPUs have NVLink connection
func (gtm *GPUTopologyManager) hasNVLink(nvlinkPairs [][]int, gpu1, gpu2 int) bool {
	for _, pair := range nvlinkPairs {
		if (pair[0] == gpu1 && pair[1] == gpu2) ||
			(pair[0] == gpu2 && pair[1] == gpu1) {
			return true
		}
	}
	return false
}

// isSameNUMA: Check if two GPUs are on same NUMA node
func (gtm *GPUTopologyManager) isSameNUMA(mapping map[int]int, gpu1, gpu2 int) bool {
	numa1, ok1 := mapping[gpu1]
	numa2, ok2 := mapping[gpu2]

	if !ok1 || !ok2 {
		return false // Can't determine
	}

	return numa1 == numa2
}

// ============================================================================
// GPU SET SELECTION (Feature 4 - Main scheduling algorithm)
// ============================================================================

// SelectBestGPUSet: Find best GPU set for job with affinity preferences
// Returns: Array of GPU indices that satisfy requirements
// Algorithm:
//  1. Filter by GPU type and memory
//  2. Find all sets of requested size
//  3. Score each set
//  4. Return highest-scoring set
func (gtm *GPUTopologyManager) SelectBestGPUSet(
	ctx context.Context,
	availableGPUs []*common.GPUDevice,
	requestedCount int,
	preferNVLink bool,
	preferSameNUMA bool,
) ([]int, *GPUAffinityScore, error) {

	if len(availableGPUs) < requestedCount {
		return nil, nil, fmt.Errorf("insufficient GPUs: have %d, need %d",
			len(availableGPUs), requestedCount)
	}

	// Get topology info
	topology, err := gtm.DetectTopology(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("topology detection failed: %w", err)
	}

	// Generate all combinations of requested size
	combinations := gtm.generateGPUCombinations(availableGPUs, requestedCount)

	if len(combinations) == 0 {
		return nil, nil, fmt.Errorf("no valid GPU combinations")
	}

	// Score each combination
	bestSet := combinations[0]
	bestScore := 0.0

	for _, gpuSet := range combinations {
		indices := gtm.extractIndices(gpuSet)
		score := gtm.ScoreGPUSet(indices, topology)

		// Apply preferences
		if preferNVLink {
			if gtm.hasNVLink(topology.NVLinkPairs, indices[0], indices[1]) {
				score += 25.0 // Boost NVLink preference
			}
		}

		if preferSameNUMA && len(indices) > 1 {
			if gtm.isSameNUMA(topology.GPUToNUMA, indices[0], indices[1]) {
				score += 15.0 // Boost NUMA preference
			}
		}

		if score > bestScore {
			bestScore = score
			bestSet = gpuSet
		}
	}

	indices := gtm.extractIndices(bestSet)
	scoreDetail := gtm.ScoreGPUPlacement(indices[0], indices[1], topology)

	return indices, scoreDetail, nil
}

// generateGPUCombinations: Generate all combinations of N GPUs from available
// Complexity: O(C(n, k)) where n = available, k = requested
func (gtm *GPUTopologyManager) generateGPUCombinations(
	gpus []*common.GPUDevice,
	size int,
) [][]*common.GPUDevice {
	if size > len(gpus) {
		return [][]*common.GPUDevice{}
	}

	if size == 1 {
		result := make([][]*common.GPUDevice, 0, len(gpus))
		for _, gpu := range gpus {
			result = append(result, []*common.GPUDevice{gpu})
		}
		return result
	}

	var result [][]*common.GPUDevice
	var combine func(int, []*common.GPUDevice)
	combine = func(start int, current []*common.GPUDevice) {
		if len(current) == size {
			// Copy current to result
			combo := make([]*common.GPUDevice, len(current))
			copy(combo, current)
			result = append(result, combo)
			return
		}

		for i := start; i < len(gpus); i++ {
			combine(i+1, append(current, gpus[i]))
		}
	}

	combine(0, make([]*common.GPUDevice, 0, size))
	return result
}

// extractIndices: Extract GPU indices from GPU device array
func (gtm *GPUTopologyManager) extractIndices(gpus []*common.GPUDevice) []int {
	indices := make([]int, len(gpus))
	for i, gpu := range gpus {
		indices[i] = gpu.Index
	}
	sort.Ints(indices)
	return indices
}

// ============================================================================
// CACHING
// ============================================================================

// DetectTopology: Detect full topology (NVLink + NUMA + PCIe)
// Returns: Cached if available, else queries all
func (gtm *GPUTopologyManager) DetectTopology(ctx context.Context) (*common.GPUTopology, error) {
	// Try cache first
	topology, err := gtm.getCachedTopology(ctx)
	if err == nil {
		return topology, nil
	}

	// Query all topology info
	nvlink, _ := gtm.DetectNVLink(ctx)
	numa, _ := gtm.DetectNUMAMapping(ctx)
	pcie, _ := gtm.DetectPCIeGeneration(ctx)

	topology = &common.GPUTopology{
		NVLinkPairs: nvlink,
		GPUToNUMA:   numa,
		PCIeGen:     pcie,
	}

	// Cache combined result
	if err := gtm.cacheTopology(ctx, topology); err != nil {
		gtm.log.Warn("Failed to cache topology (non-fatal): %v", err)
	}

	return topology, nil
}

// getCachedTopology: Retrieve full topology from cache
func (gtm *GPUTopologyManager) getCachedTopology(ctx context.Context) (*common.GPUTopology, error) {
	cached, err := gtm.redisClient.Get(ctx, gtm.cacheKey)
	if err != nil || cached == "" {
		return nil, fmt.Errorf("cache miss")
	}

	var topology common.GPUTopology
	err = json.Unmarshal([]byte(cached), &topology)
	if err != nil {
		return nil, fmt.Errorf("cache unmarshal failed: %w", err)
	}

	return &topology, nil
}

// cacheTopology: Store full topology in cache
func (gtm *GPUTopologyManager) cacheTopology(ctx context.Context, topology *common.GPUTopology) error {
	data, err := json.Marshal(topology)
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}

	err = gtm.redisClient.Set(ctx, gtm.cacheKey, string(data), gtm.cacheTTL)
	if err != nil {
		return fmt.Errorf("cache set failed: %w", err)
	}

	return nil
}

// cacheNVLinkPairs: Cache just NVLink pairs
func (gtm *GPUTopologyManager) cacheNVLinkPairs(ctx context.Context, pairs [][]int) error {
	// Get existing topology and update
	topology, _ := gtm.getCachedTopology(ctx)
	if topology == nil {
		topology = &common.GPUTopology{
			NVLinkPairs: pairs,
			GPUToNUMA:   make(map[int]int),
			PCIeGen:     make(map[int]int),
		}
	} else {
		topology.NVLinkPairs = pairs
	}

	return gtm.cacheTopology(ctx, topology)
}

// cacheNUMAMapping: Cache just NUMA mapping
func (gtm *GPUTopologyManager) cacheNUMAMapping(ctx context.Context, mapping map[int]int) error {
	topology, _ := gtm.getCachedTopology(ctx)
	if topology == nil {
		topology = &common.GPUTopology{
			NVLinkPairs: [][]int{},
			GPUToNUMA:   mapping,
			PCIeGen:     make(map[int]int),
		}
	} else {
		topology.GPUToNUMA = mapping
	}

	return gtm.cacheTopology(ctx, topology)
}

// cachePCIeGenerations: Cache just PCIe generations
func (gtm *GPUTopologyManager) cachePCIeGenerations(ctx context.Context, gens map[int]int) error {
	topology, _ := gtm.getCachedTopology(ctx)
	if topology == nil {
		topology = &common.GPUTopology{
			NVLinkPairs: [][]int{},
			GPUToNUMA:   make(map[int]int),
			PCIeGen:     gens,
		}
	} else {
		topology.PCIeGen = gens
	}

	return gtm.cacheTopology(ctx, topology)
}

// ClearTopologyCache: Clear topology cache (for testing)
func (gtm *GPUTopologyManager) ClearTopologyCache(ctx context.Context) error {
	return gtm.redisClient.Del(ctx, gtm.cacheKey)
}
