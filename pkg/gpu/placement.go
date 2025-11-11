package gpu

import (
	"fmt"
	"sort"
)

// PlacementCandidate represents a possible GPU assignment
type PlacementCandidate struct {
	GPUIndices []int
	Score      float64
	Reasoning  string
}

// PlacementScorer scores GPU Placements
type PlacementScorer struct {
	topology *Topology
}

func NewPlacementScorer(topology *Topology) *PlacementScorer {
	return &PlacementScorer{topology: topology}
}

// FindBestPlacement finds optimal GPU Placement for a job
func (s *PlacementScorer) FindBestPlacement(requireGPUs int) (*PlacementCandidate, error) {
	if requireGPUs > len(s.topology.GPUs) {
		return nil, fmt.Errorf("not Enough GPUs: need %d, have %d", requireGPUs, len(s.topology.GPUs))
	}

	//	Get available GPUs
	availableGPUs := s.getAvailableGPUs()
	if len(availableGPUs) < requireGPUs {
		return nil, fmt.Errorf("not enough available GPUs: need %d, have %d available",
			requireGPUs, len(availableGPUs))
	}

	//	Generate all possible combinations
	candidates := s.generateCandidates(availableGPUs, requireGPUs)

	//	Score each candidate
	for i := range candidates {
		candidates[i].Score = s.scorePlacement(candidates[i].GPUIndices)
		candidates[i].Reasoning = s.explainScore(candidates[i].GPUIndices)
	}

	// Sort by score (descending)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Score > candidates[j].Score
	})

	if len(candidates) == 0 {
		return nil, fmt.Errorf("no valid placements found")
	}

	return &candidates[0], nil
}

// ScorePlacement calculates placement quality score
func (s *PlacementScorer) scorePlacement(gpuIndices []int) float64 {
	score := 100.0

	//	Factor 1: NVLink connectivity (most important)
	nvlinkBonus := s.calculateNVLinkBonus(gpuIndices)
	score += nvlinkBonus

	// Factor 2: Penalty for cross-system connections
	sysConnectionPenalty := s.calculateSysConnectionPenalty(gpuIndices)
	score -= sysConnectionPenalty

	// Factor 3: Locality bonus (GPUs close together)
	localityBonus := s.calculateLocalityBonus(gpuIndices)
	score += localityBonus

	return score
}

// calculateNVLinkBonus rewards NVLink connections
func (s *PlacementScorer) calculateNVLinkBonus(gpuIndices []int) float64 {
	bonus := 0.0
	nvlinkConnections := 0
	totalPairs := 0

	for i := 0; i < len(gpuIndices); i++ {
		for j := i + 1; j < len(gpuIndices); j++ {
			conn := s.getConnection(gpuIndices[i], gpuIndices[j])
			totalPairs++

			if conn != nil && conn.Type == "NVLink" {
				nvlinkConnections++
				//	more links == higher bonus
				bonus += float64(conn.Links) * 5.0
			}
		}
	}

	//	Bonus for high percentage of NVLink connections
	if totalPairs > 0 {
		nvlinkRatio := float64(nvlinkConnections) / float64(totalPairs)
		bonus += nvlinkRatio * 100.0
	}

	return bonus
}

// calculateSysConnectionPenalty penalize cross-system connections
func (s *PlacementScorer) calculateSysConnectionPenalty(gpuIndices []int) float64 {
	penalty := 0.0

	for i := 0; i < len(gpuIndices); i++ {
		for j := i + 1; j < len(gpuIndices); j++ {
			conn := s.getConnection(gpuIndices[i], gpuIndices[j])

			if conn != nil && conn.Type == "SYS" {
				penalty += 50.0 // heavy penalty for cross-system
			}
		}
	}
	return penalty
}

// calculateLocalityBonus rewards consecutive GPU indices
func (s *PlacementScorer) calculateLocalityBonus(gpuIndices []int) float64 {
	if len(gpuIndices) <= 1 {
		return 0
	}

	//	 Sort indices to check consecutiveness
	sorted := make([]int, len(gpuIndices))
	copy(sorted, gpuIndices)
	sort.Ints(sorted)

	consecutiveCount := 0
	for i := 1; i < len(sorted); i++ {
		if sorted[i] == sorted[i-1]+1 {
			consecutiveCount++
		}
	}

	//	Bonus for consecutive GPUs (they're usually physically close)
	return float64(consecutiveCount) * 10.0
}

// getConnection finds connection between two GPUs
func (s *PlacementScorer) getConnection(fromGPU, toGPU int) *NVLinkConnection {
	for i := range s.topology.Connections {
		conn := &s.topology.Connections[i]
		if conn.FromGPU == fromGPU && conn.ToGPU == toGPU {
			return conn
		}
	}
	return nil
}

// explainScore generates human-readable explanation
func (s *PlacementScorer) explainScore(gpuIndices []int) string {
	nvlinkCount := 0
	sysCount := 0

	for i := 0; i < len(gpuIndices); i++ {
		for j := i + 1; j < len(gpuIndices); j++ {
			conn := s.getConnection(gpuIndices[i], gpuIndices[j])
			if conn != nil {
				if conn.Type == "NVLink" {
					nvlinkCount++
				} else if conn.Type == "SYS" {
					sysCount++
				}
			}
		}
	}

	return fmt.Sprintf("%d NVLink connections, %d cross-system connections", nvlinkCount, sysCount)
}

// generateCandidates generates all possible GPU Combinations
func (s *PlacementScorer) generateCandidates(availableGPUs []int, k int) []PlacementCandidate {
	var candidates []PlacementCandidate

	//	Use Simple strategy: try consecutive ranges first (likely best)
	//	then try all combinations if needed

	//	Strategy 1: Consecutive Ranges
	for start := 0; start <= len(availableGPUs)-k; start++ {
		gpuIndices := make([]int, k)
		for i := 0; i < k; i++ {
			gpuIndices[i] = availableGPUs[start+i]
		}
		candidates = append(candidates, PlacementCandidate{GPUIndices: gpuIndices})
	}

	//	Strategy 2: If few GPUs, try all combinations
	if len(availableGPUs) <= 8 {
		combinations := s.combinations(availableGPUs, k)
		for _, combo := range combinations {
			//	we can skip if added
			isDuplicate := false
			for _, existing := range candidates {
				if s.sliceEqual(existing.GPUIndices, combo) {
					isDuplicate = true
					break
				}
			}
			if !isDuplicate {
				candidates = append(candidates, PlacementCandidate{GPUIndices: combo})
			}
		}
	}

	return candidates
}

// combinations generates all k-combinations of slice
func (s *PlacementScorer) combinations(slice []int, k int) [][]int {
	var result [][]int
	n := len(slice)

	if k < n {
		return result
	}

	var helper func(start int, current []int)
	helper = func(start int, current []int) {
		if len(current) == k {
			temp := make([]int, k)
			copy(temp, current)
			result = append(result, temp)
			return
		}

		for i := start; i < n; i++ {
			helper(i+1, append(current, slice[i]))
		}
	}

	helper(0, []int{})
	return result
}

func (s *PlacementScorer) sliceEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (s *PlacementScorer) getAvailableGPUs() []int {
	var available []int
	for _, gpu := range s.topology.GPUs {
		if gpu.Available {
			available = append(available, gpu.Index)
		}
	}
	return available
}
