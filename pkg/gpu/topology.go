// Feature 4: Topology-Aware Scheduling (NVLink, NUMA, PCIe)
// FIXED VERSION - Compatible with types.go and main.go
// Detects GPU interconnections and provides affinity scoring

package gpu

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/redis"
)

// ============================================================================
// GPU TOPOLOGY SERVICE
// ============================================================================

// GPUTopologyManager: Manages GPU topology and affinity
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
	NVLinkBandwidth   = 900.0
	PCIeGen4Bandwidth = 32.0
	PCIeGen5Bandwidth = 64.0
	NVLinkMultiplier  = 28.1
	SameNUMABonus     = 1.5
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
// NVLINK DETECTION
// ============================================================================

func (gtm *GPUTopologyManager) DetectNVLink(ctx context.Context) ([][]int, error) {
	cached, err := gtm.getCachedTopology(ctx)
	if err == nil && len(cached.NVLinkPairs) > 0 {
		gtm.log.Debug("NVLink topology cache hit (pairs=%d)", len(cached.NVLinkPairs))
		return cached.NVLinkPairs, nil
	}

	nvlinkPairs, err := gtm.queryNVLinkConnections(ctx)
	if err != nil {
		gtm.log.Warn("Failed to detect NVLink (GPUs may not support NVLink): %v", err)
		return [][]int{}, nil
	}

	if err := gtm.cacheNVLinkPairs(ctx, nvlinkPairs); err != nil {
		gtm.log.Warn("Failed to cache NVLink (non-fatal): %v", err)
	}

	gtm.log.Info("Detected %d NVLink pairs", len(nvlinkPairs))
	return nvlinkPairs, nil
}

func (gtm *GPUTopologyManager) queryNVLinkConnections(ctx context.Context) ([][]int, error) {
	// ✅ FIX: Try multiple nvidia-smi paths
	nvidiaSMI := findNvidiaSMI()
	if nvidiaSMI == "" {
		return [][]int{}, fmt.Errorf("nvidia-smi not found")
	}

	cmd := exec.CommandContext(ctx, nvidiaSMI, "topo", "--matrix")
	output, err := cmd.Output()
	if err != nil {
		return [][]int{}, fmt.Errorf("nvidia-smi topo failed: %w", err)
	}

	return gtm.parseNVLinkMatrix(string(output))
}

func (gtm *GPUTopologyManager) parseNVLinkMatrix(output string) ([][]int, error) {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) < 2 {
		return [][]int{}, nil
	}

	gpuLines := make([]string, 0)
	for i := 1; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if line != "" && strings.HasPrefix(line, "GPU") {
			gpuLines = append(gpuLines, line)
		}
	}

	pairs := make([][]int, 0)

	for i, line := range gpuLines {
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}

		for j := 1; j < len(parts); j++ {
			connectivity := parts[j]
			if strings.Contains(connectivity, "NV") && j > i {
				pairs = append(pairs, []int{i, j})
			}
		}
	}

	return pairs, nil
}

// ============================================================================
// NUMA DETECTION
// ============================================================================

func (gtm *GPUTopologyManager) DetectNUMAMapping(ctx context.Context) (map[int]int, error) {
	cached, err := gtm.getCachedTopology(ctx)
	if err == nil && len(cached.GPUToNUMA) > 0 {
		gtm.log.Debug("NUMA mapping cache hit (GPUs=%d)", len(cached.GPUToNUMA))
		return cached.GPUToNUMA, nil
	}

	mapping, err := gtm.queryGPUNUMAMapping(ctx)
	if err != nil {
		gtm.log.Warn("Failed to detect NUMA mapping (assuming single NUMA): %v", err)
		return make(map[int]int), nil
	}

	if err := gtm.cacheNUMAMapping(ctx, mapping); err != nil {
		gtm.log.Warn("Failed to cache NUMA mapping (non-fatal): %v", err)
	}

	gtm.log.Info("Detected NUMA mapping for %d GPUs", len(mapping))
	return mapping, nil
}

func (gtm *GPUTopologyManager) queryGPUNUMAMapping(ctx context.Context) (map[int]int, error) {
	nvidiaSMI := findNvidiaSMI()
	if nvidiaSMI == "" {
		return nil, fmt.Errorf("nvidia-smi not found")
	}

	cmd := exec.CommandContext(ctx, nvidiaSMI, "--list-gpus")
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

		var gpuIndex int
		fmt.Sscanf(line, "GPU %d:", &gpuIndex)

		numaNode, err := gtm.queryGPUNUMANode(ctx, gpuIndex)
		if err != nil {
			gtm.log.Warn("Failed to query NUMA for GPU %d: %v", gpuIndex, err)
			numaNode = 0
		}

		mapping[gpuIndex] = numaNode
	}

	return mapping, nil
}

func (gtm *GPUTopologyManager) queryGPUNUMANode(ctx context.Context, gpuIndex int) (int, error) {
	nvidiaSMI := findNvidiaSMI()
	if nvidiaSMI == "" {
		return 0, fmt.Errorf("nvidia-smi not found")
	}

	cmd := exec.CommandContext(ctx, nvidiaSMI,
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

	parts := strings.Split(busID, ":")
	if len(parts) < 2 {
		return 0, fmt.Errorf("invalid bus ID format: %s", busID)
	}

	busDomain := fmt.Sprintf("%s:%s", parts[0], parts[1])

	numaPath := fmt.Sprintf("/sys/class/pci_bus/%s/device/numa_node", busDomain)
	data, err := os.ReadFile(numaPath)
	if err != nil {
		return 0, nil
	}

	numaNode, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, fmt.Errorf("invalid NUMA node value: %w", err)
	}

	return numaNode, nil
}

// ============================================================================
// PCIE GENERATION DETECTION
// ============================================================================

func (gtm *GPUTopologyManager) DetectPCIeGeneration(ctx context.Context) (map[int]int, error) {
	cached, err := gtm.getCachedTopology(ctx)
	if err == nil && len(cached.PCIeGen) > 0 {
		gtm.log.Debug("PCIe generation cache hit (GPUs=%d)", len(cached.PCIeGen))
		return cached.PCIeGen, nil
	}

	pcieGens, err := gtm.queryPCIeGenerations(ctx)
	if err != nil {
		gtm.log.Warn("Failed to detect PCIe generation (assuming Gen4): %v", err)
		return make(map[int]int), nil
	}

	if err := gtm.cachePCIeGenerations(ctx, pcieGens); err != nil {
		gtm.log.Warn("Failed to cache PCIe generations (non-fatal): %v", err)
	}

	gtm.log.Info("Detected PCIe generations for %d GPUs", len(pcieGens))
	return pcieGens, nil
}

func (gtm *GPUTopologyManager) queryPCIeGenerations(ctx context.Context) (map[int]int, error) {
	nvidiaSMI := findNvidiaSMI()
	if nvidiaSMI == "" {
		return nil, fmt.Errorf("nvidia-smi not found")
	}

	cmd := exec.CommandContext(ctx, nvidiaSMI,
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
			gen = 4
		}

		pcieGens[idx] = gen
	}

	return pcieGens, nil
}

// ============================================================================
// AFFINITY SCORING
// ============================================================================

type GPUAffinityScore struct {
	GPU1Index      int
	GPU2Index      int
	Score          float64
	Reason         string
	HasNVLink      bool
	SameNUMA       bool
	PCIeGeneration int
}

func (gtm *GPUTopologyManager) ScoreGPUPlacement(
	gpu1Index int,
	gpu2Index int,
	topology *common.GPUTopology,
) *GPUAffinityScore {

	score := &GPUAffinityScore{
		GPU1Index:      gpu1Index,
		GPU2Index:      gpu2Index,
		Score:          20.0,
		HasNVLink:      false,
		SameNUMA:       false,
		PCIeGeneration: 4,
	}

	reasons := make([]string, 0)

	if gtm.hasNVLink(topology.NVLinkPairs, gpu1Index, gpu2Index) {
		score.Score += 50.0
		score.HasNVLink = true
		reasons = append(reasons, "NVLink-connected")
	}

	if gtm.isSameNUMA(topology.GPUToNUMA, gpu1Index, gpu2Index) {
		score.Score += 15.0
		score.SameNUMA = true
		reasons = append(reasons, "same-NUMA")
	}

	if gen, ok := topology.PCIeGen[gpu1Index]; ok && gen >= 5 {
		score.Score += 10.0
		score.PCIeGeneration = gen
		reasons = append(reasons, "PCIe-Gen5")
	} else if gen, ok := topology.PCIeGen[gpu1Index]; ok && gen == 4 {
		score.Score += 5.0
		score.PCIeGeneration = gen
	}

	if score.Score > 100.0 {
		score.Score = 100.0
	}

	score.Reason = strings.Join(reasons, ", ")
	if score.Reason == "" {
		score.Reason = "PCIe-connected"
	}

	return score
}

func (gtm *GPUTopologyManager) ScoreGPUSet(
	gpuIndices []int,
	topology *common.GPUTopology,
) float64 {
	if len(gpuIndices) <= 1 {
		return 100.0
	}

	totalScore := 0.0
	pairs := 0

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

func (gtm *GPUTopologyManager) hasNVLink(nvlinkPairs [][]int, gpu1, gpu2 int) bool {
	for _, pair := range nvlinkPairs {
		if (pair[0] == gpu1 && pair[1] == gpu2) ||
			(pair[0] == gpu2 && pair[1] == gpu1) {
			return true
		}
	}
	return false
}

func (gtm *GPUTopologyManager) isSameNUMA(mapping map[int]int, gpu1, gpu2 int) bool {
	numa1, ok1 := mapping[gpu1]
	numa2, ok2 := mapping[gpu2]

	if !ok1 || !ok2 {
		return false
	}

	return numa1 == numa2
}

// ============================================================================
// GPU SET SELECTION
// ============================================================================

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

	topology, err := gtm.DetectTopology(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("topology detection failed: %w", err)
	}

	combinations := gtm.generateGPUCombinations(availableGPUs, requestedCount)

	if len(combinations) == 0 {
		return nil, nil, fmt.Errorf("no valid GPU combinations")
	}

	bestSet := combinations[0]
	bestScore := 0.0

	for _, gpuSet := range combinations {
		indices := gtm.extractIndices(gpuSet)
		score := gtm.ScoreGPUSet(indices, topology)

		if preferNVLink {
			if gtm.hasNVLink(topology.NVLinkPairs, indices[0], indices[1]) {
				score += 25.0
			}
		}

		if preferSameNUMA && len(indices) > 1 {
			if gtm.isSameNUMA(topology.GPUToNUMA, indices[0], indices[1]) {
				score += 15.0
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

func (gtm *GPUTopologyManager) DetectTopology(ctx context.Context) (*common.GPUTopology, error) {
	topology, err := gtm.getCachedTopology(ctx)
	if err == nil {
		return topology, nil
	}

	nvlink, _ := gtm.DetectNVLink(ctx)
	numa, _ := gtm.DetectNUMAMapping(ctx)
	pcie, _ := gtm.DetectPCIeGeneration(ctx)

	topology = &common.GPUTopology{
		NVLinkPairs: nvlink,
		GPUToNUMA:   numa,
		PCIeGen:     pcie,
	}

	if err := gtm.cacheTopology(ctx, topology); err != nil {
		gtm.log.Warn("Failed to cache topology (non-fatal): %v", err)
	}

	return topology, nil
}

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

func (gtm *GPUTopologyManager) cacheNVLinkPairs(ctx context.Context, pairs [][]int) error {
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

func (gtm *GPUTopologyManager) ClearTopologyCache(ctx context.Context) error {
	return gtm.redisClient.Del(ctx, gtm.cacheKey)
}

// ============================================================================
// HELPER: Find nvidia-smi
// ============================================================================

func findNvidiaSMI() string {
	log := logger.Get()

	paths := []string{
		"/host-usr/bin/nvidia-smi",
		"/usr/bin/nvidia-smi",
		"/usr/local/bin/nvidia-smi",
		"/usr/local/nvidia/bin/nvidia-smi",
		"/opt/nvidia/bin/nvidia-smi",
		"nvidia-smi",
	}

	for _, path := range paths {
		if _, err := exec.LookPath(path); err == nil {
			log.Info("✓ Found nvidia-smi at: %s", path)
			return path
		}
	}

	log.Warn("nvidia-smi not found in any standard location")
	return ""
}

// ============================================================================
// GPU DISCOVERY HELPERS (For Fallback)
// ============================================================================

// ✅ FIX: These are standalone helper functions, not recursive calls
func detectGPUsFromKubernetesNode(nodeName string) ([]*common.GPUDevice, error) {
	log := logger.Get()

	cmd := exec.Command("kubectl", "get", "node", nodeName, "-o", "json")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("kubectl failed: %w", err)
	}

	outputStr := string(output)
	gpus := make([]*common.GPUDevice, 0)

	if strings.Contains(outputStr, "cloud.google.com/gke-accelerator") {
		gpuType := "UNKNOWN"
		if strings.Contains(outputStr, "nvidia-tesla-t4") {
			gpuType = "Tesla T4"
		} else if strings.Contains(outputStr, "nvidia-tesla-v100") {
			gpuType = "Tesla V100"
		} else if strings.Contains(outputStr, "nvidia-tesla-a100") {
			gpuType = "Tesla A100"
		}

		gpuCount := 1

		for i := 0; i < gpuCount; i++ {
			gpus = append(gpus, &common.GPUDevice{
				Index:    i,
				UUID:     fmt.Sprintf("GPU-%d", i),
				Type:     gpuType,
				MemoryGB: 16.0,
			})
		}

		log.Info("✓ Detected %d GPU(s) via Kubernetes labels: %s", gpuCount, gpuType)
	}

	return gpus, nil
}

func detectGPUsFromProc() ([]*common.GPUDevice, error) {
	log := logger.Get()

	versionPath := "/proc/driver/nvidia/version"
	if _, err := exec.Command("test", "-f", versionPath).CombinedOutput(); err != nil {
		return nil, fmt.Errorf("NVIDIA driver not loaded (no %s)", versionPath)
	}

	gpuDevices, err := filepath.Glob("/proc/driver/nvidia/gpus/*/information")
	if err != nil {
		return nil, err
	}

	gpuCount := len(gpuDevices)
	if gpuCount == 0 {
		return nil, fmt.Errorf("no GPU devices found in /proc/driver/nvidia/gpus")
	}

	log.Info("✓ Detected %d GPU(s) via /proc filesystem", gpuCount)

	gpus := make([]*common.GPUDevice, gpuCount)
	for i := 0; i < gpuCount; i++ {
		gpus[i] = &common.GPUDevice{
			Index:    i,
			UUID:     fmt.Sprintf("GPU-%d", i),
			Type:     "NVIDIA GPU",
			MemoryGB: 16.0,
		}
	}

	return gpus, nil
}
