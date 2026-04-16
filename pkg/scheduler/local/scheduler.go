// File: pkg/scheduler/local/scheduler.go (LAYER 6 - LOCAL SCHEDULER)
// Cluster-level scheduler running on each worker cluster
// Features: Feature 1 (foundation), Feature 4, Feature 5, Feature 13
// Makes tactical decisions: which node, which GPUs
// Depends on: types.go, logger.go, redis/client.go, gpu/discovery.go, gpu/topology.go
package local

import (
	"context"
	"encoding/json"
	"fmt"

	//"github.com/BITS08SATHYA/ares-scheduler/pkg/executor"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/gpu"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/redis"
)

// ============================================================================
// LOCAL SCHEDULER SERVICE
// ============================================================================

// LocalScheduler: Cluster-level job scheduler
// Runs on each worker cluster (one instance per cluster)
// Makes tactical scheduling decisions:
//   - Which node to place job on
//   - Which GPUs to allocate
//   - Load balancing within cluster
//
// Thread-safe: Uses sync.RWMutex for node state
type LocalScheduler struct {
	clusterID       string
	redisClient     *redis.RedisClient
	log             *logger.Logger
	gpuDiscovery    *gpu.GPUDiscovery
	topologyManager *gpu.GPUTopologyManager

	// Node state
	nodesMu sync.RWMutex
	nodes   map[string]*NodeState

	// GPU allocation tracking: nodeID -> (gpuIndex -> jobID)
	// Prevents two jobs from being assigned the same physical GPU.
	// Without this, SelectBestGPUSet queries all GPUs via discovery
	// and has no way to know which are already taken.
	allocatedGPUs map[string]map[int]string

	// Metrics
	metricsMu sync.RWMutex
	metrics   *LocalMetrics

	// Metrics callbacks (wired by gateway)
	onGPUDoubleAssignBlocked func()

	// Executor
	//executor *executor.Executor
}

// NodeState: Track state of single node in cluster
type NodeState struct {
	NodeID            string
	IsHealthy         bool
	GPUCount          int
	AvailableGPUs     int
	MemoryGB          float64
	AvailableMemoryGB float64
	RunningJobsCount  int
	LastHealthCheck   time.Time
	GPUsInfo          []*common.GPUDevice // Cached GPU info
}

// LocalMetrics: Scheduling metrics for this cluster
type LocalMetrics struct {
	TotalJobsScheduled    int64
	TotalJobsFailed       int64
	GPUDoubleAssignBlocks int64 // GPU double-assignment attempts blocked
	AvgSchedulingTime     time.Duration
	LastUpdated           time.Time
}

// SchedulingScore: Score for node placement
type SchedulingScore struct {
	NodeID             string
	Score              float64 // 0-100
	AvailableGPUs      int
	AvailableMemoryGB  float64
	CurrentLoad        int
	UtilizationPercent float64
	Reasons            []string
}

// LocalSchedulingDecision: Complete local scheduling decision
type LocalSchedulingDecision struct {
	JobID            string
	NodeID           string
	GPUIndices       []int
	NodeScore        float64
	GPUAffinityScore float64
	PlacementReasons []string
	ScheduledAt      time.Time
}

// Cache keys
const (
	CacheKeyNodeState    = "ares:cluster:%s:node:%s"
	CacheKeyLocalMetrics = "ares:cluster:%s:metrics"
	CacheKeyGPUAlloc     = "ares:gpu-alloc:%s:%s" // clusterID, nodeID
	NodeStateCacheTTL    = 30 * time.Second
	MetricsCacheTTL      = 60 * time.Second
	GPUAllocCacheTTL     = 24 * time.Hour
	HealthCheckInterval  = 30 * time.Second
)

// NewLocalScheduler: Create new local scheduler for a cluster
func NewLocalScheduler(
	clusterID string,
	redisClient *redis.RedisClient,
	gpuDiscovery *gpu.GPUDiscovery,
	topologyManager *gpu.GPUTopologyManager,
) *LocalScheduler {
	ls := &LocalScheduler{
		clusterID:       clusterID,
		redisClient:     redisClient,
		log:             logger.Get(),
		gpuDiscovery:    gpuDiscovery,
		topologyManager: topologyManager,
		nodes:           make(map[string]*NodeState),
		allocatedGPUs:   make(map[string]map[int]string),
		metrics: &LocalMetrics{
			TotalJobsScheduled: 0,
			TotalJobsFailed:    0,
			LastUpdated:        time.Now(),
		},
	}
	// Default: self-wire to increment local metrics counter
	ls.onGPUDoubleAssignBlocked = func() {
		atomic.AddInt64(&ls.metrics.GPUDoubleAssignBlocks, 1)
	}

	// Restore GPU allocations from Redis on startup
	initCtx, initCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer initCancel()
	if err := ls.loadAllGPUAllocations(initCtx); err != nil {
		ls.log.Warn("Failed to load GPU allocations from Redis (non-fatal): %v", err)
	}

	return ls
}

// SetMetricsCallbacks: Inject metrics callbacks from gateway layer
func (ls *LocalScheduler) SetMetricsCallbacks(onGPUDoubleAssignBlocked func()) {
	ls.onGPUDoubleAssignBlocked = onGPUDoubleAssignBlocked
	ls.log.Info("Metrics callbacks injected into LocalScheduler")
}

// ============================================================================
// NODE MANAGEMENT
// ============================================================================

// RegisterNode: Register node in cluster
// Called when node joins cluster
func (ls *LocalScheduler) RegisterNode(ctx context.Context, node *NodeState) error {
	if node == nil || node.NodeID == "" {
		return fmt.Errorf("invalid node: nil or empty ID")
	}

	ls.nodesMu.Lock()
	defer ls.nodesMu.Unlock()

	ls.nodes[node.NodeID] = node
	node.LastHealthCheck = time.Now()

	// Cache node state in Redis
	nodeData, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("marshal node failed: %w", err)
	}

	cacheKey := fmt.Sprintf(CacheKeyNodeState, ls.clusterID, node.NodeID)
	err = ls.redisClient.Set(ctx, cacheKey, string(nodeData), NodeStateCacheTTL)
	if err != nil {
		ls.log.Warn("Failed to cache node state (non-fatal): %v", err)
	}

	ls.log.Info("Registered node %s in cluster %s (GPUs=%d, memory=%.0fGB)",
		node.NodeID, ls.clusterID, node.GPUCount, node.MemoryGB)

	return nil
}

// UpdateNodeState: Update node resource state
// Called periodically with latest GPU/CPU/memory info
func (ls *LocalScheduler) UpdateNodeState(ctx context.Context, node *NodeState) error {
	if node == nil || node.NodeID == "" {
		return fmt.Errorf("invalid node")
	}

	ls.nodesMu.Lock()
	defer ls.nodesMu.Unlock()

	_, exists := ls.nodes[node.NodeID]
	if !exists {
		return fmt.Errorf("node not registered: %s", node.NodeID)
	}

	node.LastHealthCheck = time.Now()
	ls.nodes[node.NodeID] = node

	// Cache updated state
	nodeData, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}

	cacheKey := fmt.Sprintf(CacheKeyNodeState, ls.clusterID, node.NodeID)
	err = ls.redisClient.Set(ctx, cacheKey, string(nodeData), NodeStateCacheTTL)
	if err != nil {
		ls.log.Warn("Failed to update node cache: %v", err)
	}

	return nil
}

// GetNodeState: Get state of specific node
func (ls *LocalScheduler) GetNodeState(nodeID string) (*NodeState, error) {
	ls.nodesMu.RLock()
	defer ls.nodesMu.RUnlock()

	node, exists := ls.nodes[nodeID]
	if !exists {
		return nil, fmt.Errorf("node not found: %s", nodeID)
	}

	return node, nil
}

// ListNodes: Get all registered nodes
func (ls *LocalScheduler) ListNodes() []*NodeState {
	ls.nodesMu.RLock()
	defer ls.nodesMu.RUnlock()

	nodes := make([]*NodeState, 0, len(ls.nodes))
	for _, node := range ls.nodes {
		// Return a COPY, not a pointer into the map
		nodeCopy := *node
		nodes = append(nodes, &nodeCopy)
	}

	return nodes
}

// ============================================================================
// NODE SELECTION (Strategic cluster-level decision)
// ============================================================================

// SelectBestNode: Find best node for job placement
// Scoring factors:
//   - Available GPUs: +2 per GPU
//   - Available memory: +0.1 per GB
//   - Low load: +0.5 per % free
//   - Health: +20 if healthy
//
// Returns: Best node, score, error
func (ls *LocalScheduler) SelectBestNode(
	ctx context.Context,
	jobSpec *common.JobSpec,
) (*NodeState, *SchedulingScore, error) {

	// get all the nodes
	nodes := ls.ListNodes()
	if len(nodes) == 0 {
		return nil, nil, fmt.Errorf("no nodes available in cluster")
	}

	var bestNode *NodeState
	var bestScore *SchedulingScore

	// A Worker cluster contains n nodes.  list all nodes whether it is a cpu or gpu node then
	// iterate each node and to use the gpu flag
	// to filter out the gpu node and score it
	for _, node := range nodes {
		if !node.IsHealthy {
			ls.log.Debug("Skipping unhealthy node %s", node.NodeID)
			continue
		}

		score := ls.scoreNode(node, jobSpec)

		if bestScore == nil || score.Score > bestScore.Score {
			bestNode = node
			bestScore = score
		}
	}

	if bestNode == nil {
		return nil, nil, fmt.Errorf("no suitable nodes in cluster")
	}

	ls.log.Info("Selected node %s for job (score=%.1f, reasons=%v)",
		bestNode.NodeID, bestScore.Score, bestScore.Reasons)

	return bestNode, bestScore, nil
}

// scoreNode: Calculate fitness score for a node
func (ls *LocalScheduler) scoreNode(
	node *NodeState,
	jobSpec *common.JobSpec,
) *SchedulingScore {

	score := &SchedulingScore{
		NodeID:  node.NodeID,
		Score:   20.0, // Base score
		Reasons: make([]string, 0),
	}

	// Calculate available resources
	score.AvailableGPUs = node.AvailableGPUs
	score.AvailableMemoryGB = node.AvailableMemoryGB
	score.CurrentLoad = node.RunningJobsCount

	// Utilization
	if node.GPUCount > 0 {
		score.UtilizationPercent = float64(node.GPUCount-node.AvailableGPUs) / float64(node.GPUCount) * 100.0
	}

	// Factor 1: GPU availability
	if node.AvailableGPUs >= jobSpec.GPUCount {
		score.Score += float64(node.AvailableGPUs) * 2.0
		score.Reasons = append(score.Reasons, fmt.Sprintf("has-%d-gpus", node.AvailableGPUs))
	} else {
		// Not enough GPUs - heavily penalize
		score.Score -= float64(jobSpec.GPUCount-node.AvailableGPUs) * 10.0
		score.Reasons = append(score.Reasons, "insufficient-gpus")
	}

	// Factor 2: Memory availability
	requiredMemoryGB := float64(jobSpec.MemoryMB) / 1024.0
	if node.AvailableMemoryGB >= requiredMemoryGB {
		score.Score += (node.AvailableMemoryGB - requiredMemoryGB) * 0.1
		score.Reasons = append(score.Reasons, fmt.Sprintf("%.0fgb-mem", node.AvailableMemoryGB))
	} else {
		score.Score -= 25.0
		score.Reasons = append(score.Reasons, "insufficient-memory")
	}

	// Factor 3: Node load (prefer less loaded)
	freeCapacity := 100.0 - score.UtilizationPercent
	score.Score += freeCapacity * 0.5

	if score.UtilizationPercent > 80.0 {
		score.Reasons = append(score.Reasons, "high-load")
	} else if score.UtilizationPercent < 20.0 {
		score.Reasons = append(score.Reasons, "low-load")
	}

	// Factor 4: Node health
	if node.IsHealthy {
		score.Score += 20.0
	} else {
		score.Score -= 50.0
		score.Reasons = append(score.Reasons, "unhealthy")
	}

	// Cap score
	if score.Score > 100.0 {
		score.Score = 100.0
	}
	if score.Score < 0.0 {
		score.Score = 0.0
	}

	return score
}

// ============================================================================
// GPU SELECTION (Tactical node-level decision)
// ============================================================================

// SelectBestGPUsInNode: Find best GPUs on selected node
// Uses Layer 5 GPU topology (NVLink, NUMA, PCIe)
// Returns: GPU indices, affinity score, error
func (ls *LocalScheduler) SelectBestGPUsInNode(
	ctx context.Context,
	jobSpec *common.JobSpec,
	nodeID string,
) ([]int, *gpu.GPUAffinityScore, error) {

	// Get node
	node, err := ls.GetNodeState(nodeID)
	if err != nil {
		return nil, nil, fmt.Errorf("node not found: %w", err)
	}

	if !node.IsHealthy {
		return nil, nil, fmt.Errorf("node unhealthy: %s", nodeID)
	}

	if node.AvailableGPUs < jobSpec.GPUCount {
		return nil, nil, fmt.Errorf("insufficient GPUs: need %d, have %d",
			jobSpec.GPUCount, node.AvailableGPUs)
	}

	// Discover GPUs on node
	allGPUs, err := ls.gpuDiscovery.DiscoverGPUs(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("GPU discovery failed: %w", err)
	}

	if len(allGPUs) == 0 {
		return nil, nil, fmt.Errorf("no GPUs found on node %s", nodeID)
	}

	// Filter out already-allocated GPUs so two jobs never get the same physical GPU.
	// Copy the allocation map under the lock to avoid racing with concurrent
	// ReserveResources/ReleaseResources calls that modify the map.
	ls.nodesMu.RLock()
	origAllocations := ls.allocatedGPUs[nodeID]
	nodeAllocations := make(map[int]string, len(origAllocations))
	for idx, jobID := range origAllocations {
		nodeAllocations[idx] = jobID
	}
	ls.nodesMu.RUnlock()

	if len(nodeAllocations) > 0 {
		freeGPUs := make([]*common.GPUDevice, 0, len(allGPUs))
		for _, g := range allGPUs {
			if _, taken := nodeAllocations[g.Index]; !taken {
				freeGPUs = append(freeGPUs, g)
			}
		}
		ls.log.Debug("Node %s: %d total GPUs, %d allocated, %d free",
			nodeID, len(allGPUs), len(nodeAllocations), len(freeGPUs))
		allGPUs = freeGPUs
	}

	if len(allGPUs) == 0 {
		return nil, nil, fmt.Errorf("no free GPUs on node %s (all allocated)", nodeID)
	}

	// Filter by type
	suitableGPUs := ls.gpuDiscovery.FilterGPUsByType(allGPUs, jobSpec.GPUType)
	if len(suitableGPUs) == 0 {
		return nil, nil, fmt.Errorf("no GPUs of type %s on node", jobSpec.GPUType)
	}

	// Filter by memory
	requiredMemoryGB := float64(jobSpec.MemoryMB) / 1024.0
	suitableGPUs = ls.gpuDiscovery.FilterGPUsByMemory(suitableGPUs, requiredMemoryGB)
	if len(suitableGPUs) < jobSpec.GPUCount {
		return nil, nil, fmt.Errorf("insufficient GPU memory: need %.0fGB", requiredMemoryGB)
	}

	// Check health
	suitableGPUs, err = ls.gpuDiscovery.CheckGPUHealth(ctx, suitableGPUs)
	if err != nil {
		ls.log.Warn("GPU health check failed (non-fatal): %v", err)
	}

	// Select best GPU set using topology
	gpuIndices, affinityScore, err := ls.topologyManager.SelectBestGPUSet(
		ctx,
		suitableGPUs,
		jobSpec.GPUCount,
		jobSpec.PreferNVLink,
		jobSpec.PreferSameNUMA,
	)

	if err != nil {
		return nil, nil, fmt.Errorf("GPU selection failed: %w", err)
	}

	ls.log.Info("Selected %d GPUs on node %s (affinity=%.1f, reason=%s)",
		len(gpuIndices), nodeID, affinityScore.Score, affinityScore.Reason)

	return gpuIndices, affinityScore, nil
}

// ============================================================================
// MAIN SCHEDULING METHOD
// ============================================================================

// ScheduleJob: Schedule job on this cluster
// Main entry point: called by global scheduler after cluster selection
// Returns: Complete local scheduling decision
func (ls *LocalScheduler) ScheduleJob(
	ctx context.Context,
	jobSpec *common.JobSpec,
) (*LocalSchedulingDecision, error) {

	ls.log.Debug("LocalScheduler schedule method entered()")

	if jobSpec == nil || jobSpec.Name == "" {
		return nil, fmt.Errorf("invalid job spec")
	}

	if jobSpec.GPUCount < 0 || jobSpec.GPUCount > 32 {
		return nil, fmt.Errorf("invalid GPU count: %d", jobSpec.GPUCount)
	}

	// Step 1: Select best node
	bestNode, nodeScore, err := ls.SelectBestNode(ctx, jobSpec)
	if err != nil {
		ls.recordSchedulingFailure()
		return nil, fmt.Errorf("node selection failed: %w", err)
	}

	// Step 2: Select best GPUs on node
	var gpuIndices []int
	var affinityScore *gpu.GPUAffinityScore
	var gpuErr error

	if jobSpec.GPUCount > 0 {
		gpuIndices, affinityScore, gpuErr = ls.SelectBestGPUsInNode(ctx, jobSpec, bestNode.NodeID)
		if gpuErr != nil {
			ls.recordSchedulingFailure()
			return nil, fmt.Errorf("GPU selection failed: %w", gpuErr)
		}
	}

	// Step 3: Create decision
	decision := &LocalSchedulingDecision{
		JobID:            jobSpec.RequestID,
		NodeID:           bestNode.NodeID,
		GPUIndices:       gpuIndices,
		NodeScore:        nodeScore.Score,
		PlacementReasons: nodeScore.Reasons,
		ScheduledAt:      time.Now(),
	}

	if affinityScore != nil {
		decision.GPUAffinityScore = affinityScore.Score
		decision.PlacementReasons = append(decision.PlacementReasons,
			fmt.Sprintf("gpu-affinity=%s", affinityScore.Reason))
	}

	// This either moved to coordinator.go or to the global.go (depends)
	// Call Executor to create Pod
	//execCtx, err := ls.executor.ExecuteJob(ctx, decision)
	//if err != nil {
	//	ls.log.Error("Pod creation failed: %v", err)
	//	ls.recordSchedulingFailure()
	//	return nil, fmt.Errorf("executor failed: %w", err)
	//}
	//
	//ls.log.Info("Pod created: %s for job %s", execCtx.PodName, jobSpec.RequestID)
	//
	//// Update the metrics (global metrics)
	//ls.recordSchedulingSuccess()
	//
	//ls.log.Info("Scheduled job %s on node %s GPUs %v (node_score=%.1f, gpu_affinity=%.1f)",
	//	jobSpec.RequestID, bestNode.NodeID, gpuIndices, decision.NodeScore, decision.GPUAffinityScore)

	return decision, nil
}

// ============================================================================
// SCHEDULING WITH CONSTRAINTS
// ============================================================================

// NodeConstraints: Additional node selection constraints
type NodeConstraints struct {
	RequiredNodeIDs  []string // Only these nodes
	ForbiddenNodeIDs []string // Never these nodes
	MaxUtilization   float64  // Node util must be < this %
	MinAvailableGPUs int      // Node must have at least this many
	PreferredNodeIDs []string // Prefer these nodes
}

// ScheduleJobWithConstraints: Schedule with additional node constraints
func (ls *LocalScheduler) ScheduleJobWithConstraints(
	ctx context.Context,
	jobSpec *common.JobSpec,
	constraints *NodeConstraints,
) (*LocalSchedulingDecision, error) {

	nodes := ls.ListNodes()
	if len(nodes) == 0 {
		ls.recordSchedulingFailure()
		return nil, fmt.Errorf("no nodes available")
	}

	// Filter nodes by constraints
	validNodes := make([]*NodeState, 0)

	for _, node := range nodes {
		if !node.IsHealthy {
			continue
		}

		// Required nodes constraint
		if len(constraints.RequiredNodeIDs) > 0 {
			found := false
			for _, reqID := range constraints.RequiredNodeIDs {
				if node.NodeID == reqID {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		// Forbidden nodes constraint
		inForbidden := false
		for _, forbidID := range constraints.ForbiddenNodeIDs {
			if node.NodeID == forbidID {
				inForbidden = true
				break
			}
		}
		if inForbidden {
			continue
		}

		// Utilization constraint
		util := float64(node.GPUCount-node.AvailableGPUs) / float64(node.GPUCount) * 100.0
		if util > constraints.MaxUtilization {
			continue
		}

		// GPU availability constraint
		if node.AvailableGPUs < constraints.MinAvailableGPUs {
			continue
		}

		validNodes = append(validNodes, node)
	}

	if len(validNodes) == 0 {
		ls.recordSchedulingFailure()
		return nil, fmt.Errorf("no nodes satisfy constraints")
	}

	// Score valid nodes with preference boost
	var bestNode *NodeState
	var bestScore *SchedulingScore

	for _, node := range validNodes {
		score := ls.scoreNode(node, jobSpec)

		// Boost score for preferred nodes
		for _, prefID := range constraints.PreferredNodeIDs {
			if node.NodeID == prefID {
				score.Score += 30.0
				break
			}
		}

		if bestScore == nil || score.Score > bestScore.Score {
			bestNode = node
			bestScore = score
		}
	}

	// Continue with GPU selection
	var gpuIndices []int
	var affinityScore *gpu.GPUAffinityScore
	var gpuErr error

	if jobSpec.GPUCount > 0 {
		gpuIndices, affinityScore, gpuErr = ls.SelectBestGPUsInNode(ctx, jobSpec, bestNode.NodeID)
		if gpuErr != nil {
			ls.recordSchedulingFailure()
			return nil, fmt.Errorf("GPU selection failed: %w", gpuErr)
		}
	}

	// Create decision
	decision := &LocalSchedulingDecision{
		JobID:            jobSpec.RequestID,
		NodeID:           bestNode.NodeID,
		GPUIndices:       gpuIndices,
		NodeScore:        bestScore.Score,
		PlacementReasons: bestScore.Reasons,
		ScheduledAt:      time.Now(),
	}

	if affinityScore != nil {
		decision.GPUAffinityScore = affinityScore.Score
	}

	ls.recordSchedulingSuccess()

	return decision, nil
}

// ============================================================================
// PRIORITY-AWARE SCHEDULING (Feature 5 - Priority foundation)
// ============================================================================

// ScheduleJobWithPriority: Schedule job considering priority
// Higher priority jobs get better (less loaded) nodes
func (ls *LocalScheduler) ScheduleJobWithPriority(
	ctx context.Context,
	jobSpec *common.JobSpec,
) (*LocalSchedulingDecision, error) {

	if jobSpec.Priority < 0 || jobSpec.Priority > 100 {
		return nil, fmt.Errorf("invalid priority: %d (must be 0-100)", jobSpec.Priority)
	}

	// Get all healthy nodes
	nodes := ls.ListNodes()
	validNodes := make([]*NodeState, 0)

	for _, node := range nodes {
		if node.IsHealthy {
			validNodes = append(validNodes, node)
		}
	}

	if len(validNodes) == 0 {
		ls.recordSchedulingFailure()
		return nil, fmt.Errorf("no healthy nodes available")
	}

	// Score nodes with priority boost
	scores := make([]*SchedulingScore, 0, len(validNodes))
	for _, node := range validNodes {
		score := ls.scoreNode(node, jobSpec)

		// Higher priority → better score
		score.Score += float64(jobSpec.Priority) * 0.5

		scores = append(scores, score)
	}

	// Sort by score (descending)
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].Score > scores[j].Score
	})

	// Get best node — retry with next-best if the top node was unregistered
	// between scoring and lookup (race window)
	var bestNode *NodeState
	var bestIdx int
	for i, s := range scores {
		node, err := ls.GetNodeState(s.NodeID)
		if err == nil && node != nil {
			bestNode = node
			bestIdx = i
			break
		}
	}
	if bestNode == nil {
		ls.recordSchedulingFailure()
		return nil, fmt.Errorf("all scored nodes became unavailable")
	}
	// Select GPUs
	var gpuIndices []int
	var affinityScore *gpu.GPUAffinityScore
	var gpuErr error

	if jobSpec.GPUCount > 0 {
		gpuIndices, affinityScore, gpuErr = ls.SelectBestGPUsInNode(ctx, jobSpec, bestNode.NodeID)
		if gpuErr != nil {
			ls.recordSchedulingFailure()
			return nil, fmt.Errorf("GPU selection failed: %w", gpuErr)
		}
	}

	decision := &LocalSchedulingDecision{
		JobID:            jobSpec.RequestID,
		NodeID:           bestNode.NodeID,
		GPUIndices:       gpuIndices,
		NodeScore:        scores[bestIdx].Score,
		PlacementReasons: append(scores[bestIdx].Reasons, fmt.Sprintf("priority=%d", jobSpec.Priority)),
		ScheduledAt:      time.Now(),
	}

	if affinityScore != nil {
		decision.GPUAffinityScore = affinityScore.Score
	}

	ls.recordSchedulingSuccess()

	return decision, nil
}

// ============================================================================
// METRICS & OBSERVABILITY
// ============================================================================

// recordSchedulingSuccess: Record successful scheduling
func (ls *LocalScheduler) recordSchedulingSuccess() {
	ls.metricsMu.Lock()
	defer ls.metricsMu.Unlock()

	ls.metrics.TotalJobsScheduled++
	ls.metrics.LastUpdated = time.Now()
}

// recordSchedulingFailure: Record failed scheduling
func (ls *LocalScheduler) recordSchedulingFailure() {
	ls.metricsMu.Lock()
	defer ls.metricsMu.Unlock()

	ls.metrics.TotalJobsFailed++
	ls.metrics.LastUpdated = time.Now()
}

// GetMetrics: Get current scheduler metrics
func (ls *LocalScheduler) GetMetrics() *LocalMetrics {
	ls.metricsMu.RLock()
	defer ls.metricsMu.RUnlock()

	// Return copy
	metrics := &LocalMetrics{
		TotalJobsScheduled: ls.metrics.TotalJobsScheduled,
		TotalJobsFailed:    ls.metrics.TotalJobsFailed,
		AvgSchedulingTime:  ls.metrics.AvgSchedulingTime,
		LastUpdated:        ls.metrics.LastUpdated,
	}

	return metrics
}

// SuccessRate: Calculate scheduling success rate (%)
func (ls *LocalScheduler) SuccessRate() float64 {
	ls.metricsMu.RLock()
	defer ls.metricsMu.RUnlock()

	total := ls.metrics.TotalJobsScheduled + ls.metrics.TotalJobsFailed
	if total == 0 {
		return 0.0
	}

	return float64(ls.metrics.TotalJobsScheduled) / float64(total) * 100.0
}

// GetClusterLoad: Get cluster resource utilization
func (ls *LocalScheduler) GetClusterLoad() map[string]interface{} {
	ls.nodesMu.RLock()
	defer ls.nodesMu.RUnlock()

	totalGPUs := 0
	totalMemory := 0.0
	gpusInUse := 0
	memoryInUse := 0.0
	jobsRunning := 0
	healthyNodes := 0

	for _, node := range ls.nodes {
		totalGPUs += node.GPUCount
		totalMemory += node.MemoryGB
		gpusInUse += node.GPUCount - node.AvailableGPUs
		memoryInUse += node.MemoryGB - node.AvailableMemoryGB
		jobsRunning += node.RunningJobsCount

		if node.IsHealthy {
			healthyNodes++
		}
	}

	gpuUtil := 0.0
	if totalGPUs > 0 {
		gpuUtil = float64(gpusInUse) / float64(totalGPUs) * 100.0
	}

	memUtil := 0.0
	if totalMemory > 0 {
		memUtil = memoryInUse / totalMemory * 100.0
	}

	return map[string]interface{}{
		"cluster_id":              ls.clusterID,
		"total_gpus":              totalGPUs,
		"gpus_in_use":             gpusInUse,
		"gpu_utilization_pct":     gpuUtil,
		"total_memory_gb":         totalMemory,
		"memory_in_use_gb":        memoryInUse,
		"memory_utilization_pct":  memUtil,
		"running_jobs":            jobsRunning,
		"nodes_count":             len(ls.nodes),
		"healthy_nodes":           healthyNodes,
		"scheduling_success_rate": ls.SuccessRate(),
	}
}

// GetDiscoveredGPUTypes: Returns the unique GPU types discovered on this cluster's nodes
// Used by heartbeat to report actual hardware to the global scheduler
// e.g., ["T4"] for a T4 cluster, ["A100"] for a p4d, ["A100", "H100"] for mixed
func (ls *LocalScheduler) GetDiscoveredGPUTypes() []string {
	if ls.gpuDiscovery == nil {
		return nil
	}

	discoverCtx, discoverCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer discoverCancel()
	gpus, err := ls.gpuDiscovery.DiscoverGPUs(discoverCtx)
	if err != nil || len(gpus) == 0 {
		return nil
	}

	// Deduplicate GPU types
	typeSet := make(map[string]bool)
	for _, g := range gpus {
		if g.Type != "" {
			typeSet[g.Type] = true
		}
	}

	types := make([]string, 0, len(typeSet))
	for t := range typeSet {
		types = append(types, t)
	}

	return types
}

// ============================================================================
// RESOURCE ALLOCATION & RESERVATION
// ============================================================================

// ReserveResources: Reserve specific GPUs and memory on node
// Called after job placement decision
func (ls *LocalScheduler) ReserveResources(
	ctx context.Context,
	jobID string,
	nodeID string,
	gpuCount int,
	memoryMB int,
	gpuIndices []int,
) error {

	node, err := ls.GetNodeState(nodeID)
	if err != nil {
		return err
	}

	if node.AvailableGPUs < gpuCount {
		return fmt.Errorf("insufficient GPUs to reserve")
	}

	memoryGB := float64(memoryMB) / 1024.0
	if node.AvailableMemoryGB < memoryGB {
		return fmt.Errorf("insufficient memory to reserve")
	}

	// Track GPU indices and reserve resources atomically under a single lock
	ls.nodesMu.Lock()
	if ls.allocatedGPUs[nodeID] == nil {
		ls.allocatedGPUs[nodeID] = make(map[int]string)
	}
	for _, idx := range gpuIndices {
		if existingJob, taken := ls.allocatedGPUs[nodeID][idx]; taken {
			ls.nodesMu.Unlock()
			if ls.onGPUDoubleAssignBlocked != nil {
				ls.onGPUDoubleAssignBlocked()
			}
			return fmt.Errorf("GPU %d on node %s already allocated to job %s", idx, nodeID, existingJob)
		}
		ls.allocatedGPUs[nodeID][idx] = jobID
	}
	node.AvailableGPUs -= gpuCount
	node.AvailableMemoryGB -= memoryGB
	node.RunningJobsCount++
	ls.nodesMu.Unlock()

	// Persist GPU allocations to Redis for crash recovery
	ls.saveGPUAllocations(ctx, nodeID)

	ls.log.Info("Reserved %d GPUs %v on node %s for job %s", gpuCount, gpuIndices, nodeID, jobID)
	return ls.UpdateNodeState(ctx, node)
}

// ReleaseResources: Free GPUs and memory on node
// Called when job completes
func (ls *LocalScheduler) ReleaseResources(
	ctx context.Context,
	jobID string,
	nodeID string,
	gpuCount int,
	memoryMB int,
) error {

	node, err := ls.GetNodeState(nodeID)
	if err != nil {
		return err
	}

	// Release tracked GPU indices and free resources atomically
	ls.nodesMu.Lock()
	if nodeGPUs, exists := ls.allocatedGPUs[nodeID]; exists {
		var toDelete []int
		for idx, owner := range nodeGPUs {
			if owner == jobID {
				toDelete = append(toDelete, idx)
			}
		}
		for _, idx := range toDelete {
			delete(nodeGPUs, idx)
		}
	}
	node.AvailableGPUs += gpuCount
	node.AvailableMemoryGB += float64(memoryMB) / 1024.0
	node.RunningJobsCount--
	if node.RunningJobsCount < 0 {
		node.RunningJobsCount = 0
	}
	ls.nodesMu.Unlock()

	// Persist updated GPU allocations to Redis
	ls.saveGPUAllocations(ctx, nodeID)

	ls.log.Info("Released %d GPUs on node %s from job %s", gpuCount, nodeID, jobID)
	return ls.UpdateNodeState(ctx, node)
}

// ClearGPUAllocations: Clear all in-memory GPU allocations.
// Used by benchmark cleanup to reset GPU state without restarting.
func (ls *LocalScheduler) ClearGPUAllocations() {
	ls.nodesMu.Lock()
	for nodeID := range ls.allocatedGPUs {
		delete(ls.allocatedGPUs, nodeID)
	}
	// Also reset node available GPUs
	for _, node := range ls.nodes {
		node.AvailableGPUs = node.GPUCount
		node.RunningJobsCount = 0
	}
	ls.nodesMu.Unlock()
	ls.log.Info("Cleared all GPU allocations (benchmark reset)")
}

// ReleaseJobResources: Release all resources held by a job (looked up by jobID)
// Used by the cancel/preemption path where we only know the jobID
func (ls *LocalScheduler) ReleaseJobResources(jobID string) {
	ls.nodesMu.Lock()
	var foundNodeID string
	gpuCount := 0
	for nodeID, gpuMap := range ls.allocatedGPUs {
		for idx, owner := range gpuMap {
			if owner == jobID {
				delete(gpuMap, idx)
				gpuCount++
				foundNodeID = nodeID
			}
		}
	}
	ls.nodesMu.Unlock()

	if foundNodeID == "" {
		ls.log.Warn("ReleaseJobResources: no GPU allocations found for job %s", jobID)
		return
	}

	// Update node state
	node, err := ls.GetNodeState(foundNodeID)
	if err != nil {
		ls.log.Error("ReleaseJobResources: node %s not found: %v", foundNodeID, err)
		return
	}

	ls.nodesMu.Lock()
	node.AvailableGPUs += gpuCount
	node.RunningJobsCount--
	if node.RunningJobsCount < 0 {
		node.RunningJobsCount = 0
	}
	ls.nodesMu.Unlock()

	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cleanupCancel()
	ls.saveGPUAllocations(cleanupCtx, foundNodeID)
	ls.UpdateNodeState(cleanupCtx, node)

	ls.log.Info("ReleaseJobResources: freed %d GPUs on node %s for job %s", gpuCount, foundNodeID, jobID)
}

// ============================================================================
// GPU ALLOCATION PERSISTENCE
// ============================================================================

// saveGPUAllocations: Persist GPU allocations for a node to Redis
// Called after ReserveResources and ReleaseResources to survive crashes
func (ls *LocalScheduler) saveGPUAllocations(ctx context.Context, nodeID string) {
	ls.nodesMu.RLock()
	allocs := ls.allocatedGPUs[nodeID]
	// Copy under lock to avoid races
	allocCopy := make(map[string]string, len(allocs))
	for idx, jobID := range allocs {
		allocCopy[fmt.Sprintf("%d", idx)] = jobID
	}
	ls.nodesMu.RUnlock()

	key := fmt.Sprintf(CacheKeyGPUAlloc, ls.clusterID, nodeID)

	if len(allocCopy) == 0 {
		// No allocations — delete the key
		if err := ls.redisClient.Del(ctx, key); err != nil {
			ls.log.Warn("Failed to delete GPU alloc key %s: %v", key, err)
		}
		return
	}

	data, err := json.Marshal(allocCopy)
	if err != nil {
		ls.log.Warn("Failed to marshal GPU allocations for node %s: %v", nodeID, err)
		return
	}

	if err := ls.redisClient.Set(ctx, key, string(data), GPUAllocCacheTTL); err != nil {
		ls.log.Warn("Failed to persist GPU allocations for node %s: %v", nodeID, err)
	}
}

// loadAllGPUAllocations: Restore GPU allocations from Redis on startup
func (ls *LocalScheduler) loadAllGPUAllocations(ctx context.Context) error {
	pattern := fmt.Sprintf(CacheKeyGPUAlloc, ls.clusterID, "*")
	keys, err := ls.redisClient.Keys(ctx, pattern)
	if err != nil {
		return fmt.Errorf("failed to scan GPU alloc keys: %w", err)
	}

	if len(keys) == 0 {
		return nil
	}

	// Read all allocations from Redis without holding the lock
	prefix := fmt.Sprintf("ares:gpu-alloc:%s:", ls.clusterID)
	allNodeAllocs := make(map[string]map[int]string)
	restored := 0

	for _, key := range keys {
		// Extract nodeID from key: ares:gpu-alloc:{clusterID}:{nodeID}
		if len(key) <= len(prefix) {
			continue
		}
		nodeID := key[len(prefix):]

		val, err := ls.redisClient.Get(ctx, key)
		if err != nil || val == "" {
			continue
		}

		var raw map[string]string
		if err := json.Unmarshal([]byte(val), &raw); err != nil {
			ls.log.Warn("Failed to unmarshal GPU alloc for node %s: %v", nodeID, err)
			continue
		}

		nodeAllocs := make(map[int]string, len(raw))
		for idxStr, jobID := range raw {
			var idx int
			if _, err := fmt.Sscanf(idxStr, "%d", &idx); err == nil {
				nodeAllocs[idx] = jobID
			}
		}

		if len(nodeAllocs) > 0 {
			allNodeAllocs[nodeID] = nodeAllocs
			restored += len(nodeAllocs)
		}
	}

	// Lock only for the in-memory update
	if len(allNodeAllocs) > 0 {
		ls.nodesMu.Lock()
		for nodeID, allocs := range allNodeAllocs {
			ls.allocatedGPUs[nodeID] = allocs
		}
		ls.nodesMu.Unlock()
	}

	if restored > 0 {
		ls.log.Info("Restored %d GPU allocations from Redis across %d nodes", restored, len(allNodeAllocs))
	}
	return nil
}

// ============================================================================
// HEALTH MONITORING
// ============================================================================

// CheckNodeHealth: Check if node is responsive
// Mark unhealthy if no recent update
func (ls *LocalScheduler) CheckNodeHealth() {
	ls.nodesMu.Lock()
	defer ls.nodesMu.Unlock()

	now := time.Now()
	maxAge := HealthCheckInterval

	for _, node := range ls.nodes {
		if now.Sub(node.LastHealthCheck) > maxAge {
			node.IsHealthy = false
			ls.log.Warn("Node %s marked unhealthy: no update for %v",
				node.NodeID, now.Sub(node.LastHealthCheck))
		}
	}
}

// GetHealthStatus: Get health status of all nodes
func (ls *LocalScheduler) GetHealthStatus() map[string]bool {
	ls.nodesMu.RLock()
	defer ls.nodesMu.RUnlock()

	status := make(map[string]bool)
	for nodeID, node := range ls.nodes {
		status[nodeID] = node.IsHealthy
	}

	return status
}
