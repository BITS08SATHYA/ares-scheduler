// Feature 4: Topology-Aware Scheduling (NVLink, NUMA, PCIe)
// FIXED VERSION - With bounds checking to prevent panics
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
	redisClient  *redis.RedisClient
	log          *logger.Logger
	discovery    *GPUDiscovery
	cacheKey     string
	cacheTTL     time.Duration
	nvlinkCounts map[string]int // "gpu0-gpu1" -> NVLink link count (populated during parsing)
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

	// Also populate NVLink link counts for bandwidth-aware scoring
	if gtm.nvlinkCounts == nil {
		gtm.nvlinkCounts = make(map[string]int)
	}

	for i, line := range gpuLines {
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}

		for j := 1; j < len(parts); j++ {
			connectivity := parts[j]
			if strings.Contains(connectivity, "NV") && j > i {
				pairs = append(pairs, []int{i, j})

				// Extract NVLink count: "NV12" -> 12, "NV6" -> 6, "NV2" -> 2
				linkCount := gtm.extractNVLinkCount(connectivity)
				pairKey := fmt.Sprintf("%d-%d", i, j)
				gtm.nvlinkCounts[pairKey] = linkCount

				gtm.log.Debug("NVLink pair GPU %d ↔ GPU %d: %s (%d links)",
					i, j, connectivity, linkCount)
			}
		}
	}

	return pairs, nil
}

// extractNVLinkCount: Parse NVLink connection string to get link count
// nvidia-smi topo --matrix outputs: NV1, NV2, NV4, NV6, NV12, etc.
// The number indicates how many NVLink connections between the GPU pair
// More links = higher bandwidth:
//
//	NV12 = 12 links (~600 GB/s on A100, same NVSwitch)
//	NV6  = 6 links  (~300 GB/s on A100, cross-NVSwitch)
//	NV2  = 2 links  (~100 GB/s)
//	NV1  = 1 link   (~50 GB/s)
func (gtm *GPUTopologyManager) extractNVLinkCount(connectivity string) int {
	// Remove "NV" prefix, parse remaining number
	nvStr := strings.TrimPrefix(connectivity, "NV")
	// Handle cases like "NV12" -> "12", "NV6" -> "6"
	// Also handle "NVB" (NVLink Bridge on older architectures) -> treat as 2
	if nvStr == "B" {
		return 2
	}
	count, err := strconv.Atoi(nvStr)
	if err != nil {
		// If we can't parse, at least we know it's NVLink — default to 1
		gtm.log.Warn("Could not parse NVLink count from '%s', defaulting to 1", connectivity)
		return 1
	}
	return count
}

// DetectNVSwitchDomains: Identify groups of GPUs on the same NVSwitch
// On p4d.24xlarge: GPUs 0-3 share NVSwitch-0, GPUs 4-7 share NVSwitch-1
// GPUs within the same domain have maximum NVLink bandwidth (NV12)
// GPUs across domains have reduced NVLink bandwidth (NV6)
//
// Algorithm: GPUs connected with the highest link count form a domain.
// Uses union-find on pairs with link count >= threshold (max_links * 0.8)
func (gtm *GPUTopologyManager) DetectNVSwitchDomains(
	gpuCount int,
	nvlinkPairs [][]int,
	nvlinkCounts map[string]int,
) map[int][]int {

	if len(nvlinkPairs) == 0 || len(nvlinkCounts) == 0 {
		return map[int][]int{}
	}

	// Find max link count (this is the intra-NVSwitch bandwidth)
	maxLinks := 0
	for _, count := range nvlinkCounts {
		if count > maxLinks {
			maxLinks = count
		}
	}

	// Threshold: pairs with >= 80% of max links are in the same NVSwitch domain
	// On p4d: max=12, threshold=9.6 -> NV12 pairs are same-domain, NV6 are cross-domain
	threshold := int(float64(maxLinks) * 0.8)

	// Union-Find to group GPUs into domains
	parent := make([]int, gpuCount)
	for i := range parent {
		parent[i] = i
	}

	var find func(int) int
	find = func(x int) int {
		if parent[x] != x {
			parent[x] = find(parent[x])
		}
		return parent[x]
	}

	union := func(x, y int) {
		px, py := find(x), find(y)
		if px != py {
			parent[px] = py
		}
	}

	// Union GPUs that share high-bandwidth NVLink
	for _, pair := range nvlinkPairs {
		if len(pair) < 2 {
			continue
		}
		pairKey := fmt.Sprintf("%d-%d", pair[0], pair[1])
		if count, ok := nvlinkCounts[pairKey]; ok && count >= threshold {
			union(pair[0], pair[1])
		}
	}

	// Build domain map: root GPU index -> list of GPUs in domain
	domains := make(map[int][]int)
	for i := 0; i < gpuCount; i++ {
		root := find(i)
		domains[root] = append(domains[root], i)
	}

	// Re-key domains to 0, 1, 2, ... for cleanliness
	result := make(map[int][]int)
	domainID := 0
	for _, gpus := range domains {
		if len(gpus) > 1 { // Only count groups with multiple GPUs as domains
			result[domainID] = gpus
			domainID++
		}
	}

	gtm.log.Info("Detected %d NVSwitch domains (max NVLink count=%d, threshold=%d)",
		len(result), maxLinks, threshold)
	for id, gpus := range result {
		gtm.log.Info("  NVSwitch domain %d: GPUs %v", id, gpus)
	}

	return result
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

	// NVLink scoring — now bandwidth-aware using link counts
	// NV12 (same NVSwitch, ~600 GB/s) scores much higher than NV2 (~100 GB/s)
	if gtm.hasNVLink(topology.NVLinkPairs, gpu1Index, gpu2Index) {
		score.HasNVLink = true
		linkCount := gtm.getNVLinkCount(topology, gpu1Index, gpu2Index)

		switch {
		case linkCount >= 12:
			// NV12: Same NVSwitch domain — maximum bandwidth (~600 GB/s on A100)
			score.Score += 60.0
			reasons = append(reasons, fmt.Sprintf("NV%d-same-NVSwitch", linkCount))
		case linkCount >= 6:
			// NV6: Cross-NVSwitch — good bandwidth (~300 GB/s on A100)
			score.Score += 40.0
			reasons = append(reasons, fmt.Sprintf("NV%d-cross-NVSwitch", linkCount))
		case linkCount >= 2:
			// NV2-NV4: Fewer links — moderate bandwidth
			score.Score += 25.0
			reasons = append(reasons, fmt.Sprintf("NV%d-limited", linkCount))
		default:
			// NV1 or unknown NVLink — still better than PCIe
			score.Score += 15.0
			reasons = append(reasons, fmt.Sprintf("NV%d-minimal", linkCount))
		}
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
		score.Reason = "PCIe-only"
	}

	return score
}

// getNVLinkCount: Get the NVLink link count between two GPUs
// Returns link count from topology data or from cached parse results
// Falls back to 1 if link count is unknown (we know NVLink exists but not how many)
func (gtm *GPUTopologyManager) getNVLinkCount(topology *common.GPUTopology, gpu1, gpu2 int) int {
	// Try topology struct first (populated from cache)
	if topology.NVLinkCount != nil {
		key1 := fmt.Sprintf("%d-%d", gpu1, gpu2)
		if count, ok := topology.NVLinkCount[key1]; ok {
			return count
		}
		key2 := fmt.Sprintf("%d-%d", gpu2, gpu1)
		if count, ok := topology.NVLinkCount[key2]; ok {
			return count
		}
	}

	// Try in-memory parse results (populated during parseNVLinkMatrix)
	if gtm.nvlinkCounts != nil {
		key1 := fmt.Sprintf("%d-%d", gpu1, gpu2)
		if count, ok := gtm.nvlinkCounts[key1]; ok {
			return count
		}
		key2 := fmt.Sprintf("%d-%d", gpu2, gpu1)
		if count, ok := gtm.nvlinkCounts[key2]; ok {
			return count
		}
	}

	return 1 // Default: we know NVLink exists but not how many links
}

func (gtm *GPUTopologyManager) ScoreGPUSet(
	gpuIndices []int,
	topology *common.GPUTopology,
) float64 {
	if len(gpuIndices) <= 1 {
		return 100.0
	}

	// Base score: average of all pairwise placement scores
	totalScore := 0.0
	pairs := 0

	for i := 0; i < len(gpuIndices)-1; i++ {
		for j := i + 1; j < len(gpuIndices); j++ {
			score := gtm.ScoreGPUPlacement(gpuIndices[i], gpuIndices[j], topology)
			totalScore += score.Score
			pairs++
		}
	}

	avgScore := totalScore / float64(pairs)

	// NVSwitch domain bonus: reward keeping GPUs within the same NVSwitch domain
	// For all-reduce, the bottleneck is the WORST link in the communication ring.
	// If all GPUs are in one NVSwitch domain, the worst link is NV12 (~600 GB/s).
	// If GPUs span domains, the worst link drops to NV6 (~300 GB/s) — 2x slower.
	if topology.NVSwitchDomains != nil && len(topology.NVSwitchDomains) > 0 {
		domainScore := gtm.scoreNVSwitchDomainLocality(gpuIndices, topology.NVSwitchDomains)
		avgScore += domainScore
	}

	// Worst-link penalty: for collective operations (all-reduce), performance
	// is limited by the slowest link in the ring, not the average.
	// Penalize sets that have one bad pair among otherwise good pairs.
	if pairs > 1 {
		worstPairScore := 100.0
		for i := 0; i < len(gpuIndices)-1; i++ {
			for j := i + 1; j < len(gpuIndices); j++ {
				pairScore := gtm.ScoreGPUPlacement(gpuIndices[i], gpuIndices[j], topology)
				if pairScore.Score < worstPairScore {
					worstPairScore = pairScore.Score
				}
			}
		}
		// Blend: 70% average score + 30% worst-link score
		// This ensures we don't pick a set with 7 great links and 1 terrible one
		avgScore = avgScore*0.7 + worstPairScore*0.3
	}

	return avgScore
}

// scoreNVSwitchDomainLocality: Bonus for keeping GPUs in the same NVSwitch domain
// Returns 0-25 bonus points based on how well the GPU set fits within NVSwitch domains
//
// All GPUs in one domain:  +25 (ideal — all communication at max NVLink bandwidth)
// Most GPUs in one domain: +15 (good — most communication is fast)
// Evenly split:            +0  (no bonus — cross-domain traffic is unavoidable)
func (gtm *GPUTopologyManager) scoreNVSwitchDomainLocality(
	gpuIndices []int,
	domains map[int][]int,
) float64 {

	if len(domains) == 0 {
		return 0.0
	}

	// Count how many of our GPUs fall in each domain
	domainHits := make(map[int]int) // domain_id -> count of our GPUs in it
	for _, gpuIdx := range gpuIndices {
		for domainID, domainGPUs := range domains {
			for _, dGPU := range domainGPUs {
				if dGPU == gpuIdx {
					domainHits[domainID]++
					break
				}
			}
		}
	}

	// Find the domain with the most of our GPUs
	maxInOneDomain := 0
	for _, count := range domainHits {
		if count > maxInOneDomain {
			maxInOneDomain = count
		}
	}

	// Calculate locality ratio: what fraction of our GPUs are in the best domain?
	localityRatio := float64(maxInOneDomain) / float64(len(gpuIndices))

	// Scale: 100% locality = +25 bonus, 50% locality = +0 bonus
	bonus := (localityRatio - 0.5) * 50.0 // Maps 0.5->0, 1.0->25
	if bonus < 0.0 {
		bonus = 0.0
	}

	return bonus
}

// ============================================================================
// TOPOLOGY HELPERS
// ============================================================================

func (gtm *GPUTopologyManager) hasNVLink(nvlinkPairs [][]int, gpu1, gpu2 int) bool {
	for _, pair := range nvlinkPairs {
		// ★★★ FIX: Check pair has at least 2 elements ★★★
		if len(pair) < 2 {
			continue
		}
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
// GPU SET SELECTION - ★★★ FIXED VERSION ★★★
// ============================================================================

func (gtm *GPUTopologyManager) SelectBestGPUSet(
	ctx context.Context,
	availableGPUs []*common.GPUDevice,
	requestedCount int,
	preferNVLink bool,
	preferSameNUMA bool,
) ([]int, *GPUAffinityScore, error) {

	// ★★★ FIX 1: Validate inputs ★★★
	if len(availableGPUs) == 0 {
		return nil, nil, fmt.Errorf("no GPUs available")
	}

	if requestedCount <= 0 {
		return nil, nil, fmt.Errorf("invalid GPU count: %d", requestedCount)
	}

	if len(availableGPUs) < requestedCount {
		return nil, nil, fmt.Errorf("insufficient GPUs: have %d, need %d",
			len(availableGPUs), requestedCount)
	}

	// ★★★ FIX 2: Special case for single GPU - skip all pair logic ★★★
	if requestedCount == 1 {
		// Just return the best single GPU (most memory, healthiest)
		bestGPU := gtm.selectSingleBestGPU(availableGPUs)
		return []int{bestGPU.Index}, &GPUAffinityScore{
			GPU1Index:      bestGPU.Index,
			GPU2Index:      -1, // No second GPU
			Score:          50.0,
			Reason:         "single-gpu-no-affinity-needed",
			HasNVLink:      false,
			SameNUMA:       false,
			PCIeGeneration: 4,
		}, nil
	}

	// Multi-GPU case: detect topology and find best combination
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

		// ★★★ FIX 3: Check length before accessing indices[1] ★★★
		if preferNVLink && len(indices) >= 2 {
			if gtm.hasNVLink(topology.NVLinkPairs, indices[0], indices[1]) {
				score += 25.0
			}
		}

		if preferSameNUMA && len(indices) >= 2 {
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

	// ★★★ FIX 4: Handle single vs multi-GPU scoring safely ★★★
	var scoreDetail *GPUAffinityScore
	if len(indices) >= 2 {
		scoreDetail = gtm.ScoreGPUPlacement(indices[0], indices[1], topology)
	} else if len(indices) == 1 {
		scoreDetail = &GPUAffinityScore{
			GPU1Index:      indices[0],
			GPU2Index:      -1,
			Score:          50.0,
			Reason:         "single-gpu",
			HasNVLink:      false,
			SameNUMA:       false,
			PCIeGeneration: 4,
		}
	} else {
		return nil, nil, fmt.Errorf("no GPUs selected")
	}

	return indices, scoreDetail, nil
}

// ★★★ NEW HELPER: Select single best GPU ★★★
func (gtm *GPUTopologyManager) selectSingleBestGPU(gpus []*common.GPUDevice) *common.GPUDevice {
	if len(gpus) == 0 {
		return nil
	}
	if len(gpus) == 1 {
		return gpus[0]
	}

	best := gpus[0]
	for _, gpu := range gpus[1:] {
		// Prefer healthy GPUs
		if gpu.IsHealthy && !best.IsHealthy {
			best = gpu
			continue
		}
		// Prefer more available memory
		if gpu.AvailableMemGB > best.AvailableMemGB {
			best = gpu
		}
	}
	return best
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

	// Detect GPU count for NVSwitch domain detection
	gpuCount := 0
	if gtm.discovery != nil {
		gpus, err := gtm.discovery.DiscoverGPUs(ctx)
		if err == nil {
			gpuCount = len(gpus)
		}
	}
	if gpuCount == 0 {
		// Infer from NVLink pairs
		for _, pair := range nvlink {
			for _, idx := range pair {
				if idx+1 > gpuCount {
					gpuCount = idx + 1
				}
			}
		}
	}

	// Build NVLink count map from parsed data
	nvlinkCountMap := make(map[string]int)
	if gtm.nvlinkCounts != nil {
		for k, v := range gtm.nvlinkCounts {
			nvlinkCountMap[k] = v
		}
	}

	// Detect NVSwitch domains
	nvswitchDomains := gtm.DetectNVSwitchDomains(gpuCount, nvlink, nvlinkCountMap)

	topology = &common.GPUTopology{
		NVLinkPairs:     nvlink,
		NVLinkCount:     nvlinkCountMap,
		NVSwitchDomains: nvswitchDomains,
		GPUToNUMA:       numa,
		PCIeGen:         pcie,
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

func (gtm *GPUTopologyManager) FindNvidiaSMI_test_path() string {
	log := logger.Get()

	paths := []string{
		"/opt/nvidia/bin/nvidia-smi",
		"/host-usr/bin/nvidia-smi",
		"/usr/bin/nvidia-smi",
		"/usr/local/bin/nvidia-smi",
		"/usr/local/nvidia/bin/nvidia-smi",
		"nvidia-smi",
	}

	//path := "/opt/nvidia/bin/nvidia-smi"

	for _, path := range paths {
		if _, err := exec.LookPath(path); err == nil {
			log.Info("✓ opt location used: Found nvidia-smi at: %s", path)
			return path
		}
	}

	log.Warn("opt location was used .. nvidia-smi not found in any standard location")
	return ""
}

// ============================================================================
// HELPER: Find nvidia-smi
// ============================================================================

func findNvidiaSMI() string {
	log := logger.Get()

	paths := []string{
		"/opt/nvidia/bin/nvidia-smi",
		"/host-usr/bin/nvidia-smi",
		"/usr/bin/nvidia-smi",
		"/usr/local/bin/nvidia-smi",
		"/usr/local/nvidia/bin/nvidia-smi",
		"nvidia-smi",
	}

	//path := "/opt/nvidia/bin/nvidia-smi"

	for _, path := range paths {
		if _, err := exec.LookPath(path); err == nil {
			log.Info("✓ opt helper location used: Found nvidia-smi at: %s", path)
			return path
		}
	}

	log.Warn("opt helper location was used .. nvidia-smi not found in any standard location")
	return ""
}

// ============================================================================
// GPU DISCOVERY HELPERS (For Fallback)
// ============================================================================

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
