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
	"github.com/BITS08SATHYA/ares-scheduler/pkg/executor"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/gpu"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/redis"
	"sort"
	"sync"
	"time"
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

	// Metrics
	metricsMu sync.RWMutex
	metrics   *LocalMetrics

	// Executor
	executor *executor.Executor
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
	TotalJobsScheduled int64
	TotalJobsFailed    int64
	AvgSchedulingTime  time.Duration
	LastUpdated        time.Time
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
	NodeStateCacheTTL    = 30 * time.Second
	MetricsCacheTTL      = 60 * time.Second
	HealthCheckInterval  = 30 * time.Second
)

// NewLocalScheduler: Create new local scheduler for a cluster
func NewLocalScheduler(
	clusterID string,
	redisClient *redis.RedisClient,
	gpuDiscovery *gpu.GPUDiscovery,
	topologyManager *gpu.GPUTopologyManager,
	executor *executor.Executor,
) *LocalScheduler {
	return &LocalScheduler{
		clusterID:       clusterID,
		redisClient:     redisClient,
		log:             logger.Get(),
		gpuDiscovery:    gpuDiscovery,
		topologyManager: topologyManager,
		nodes:           make(map[string]*NodeState),
		metrics: &LocalMetrics{
			TotalJobsScheduled: 0,
			TotalJobsFailed:    0,
			LastUpdated:        time.Now(),
		},
		executor: executor,
	}
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
		ls.nodesMu.Unlock()
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
		nodes = append(nodes, node)
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

	nodes := ls.ListNodes()
	if len(nodes) == 0 {
		return nil, nil, fmt.Errorf("no nodes available in cluster")
	}

	var bestNode *NodeState
	var bestScore *SchedulingScore

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

	// Call Executor to create Pod
	execCtx, err := ls.executor.ExecuteJob(ctx, decision)
	if err != nil {
		ls.log.Error("Pod creation failed: %v", err)
		ls.recordSchedulingFailure()
		return nil, fmt.Errorf("executor failed: %w", err)
	}

	ls.log.Info("Pod created: %s for job %s", execCtx.PodName, jobSpec.RequestID)

	// Update the metrics (global metrics)
	ls.recordSchedulingSuccess()

	ls.log.Info("Scheduled job %s on node %s GPUs %v (node_score=%.1f, gpu_affinity=%.1f)",
		jobSpec.RequestID, bestNode.NodeID, gpuIndices, decision.NodeScore, decision.GPUAffinityScore)

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

	// Get best node
	bestNode, _ := ls.GetNodeState(scores[0].NodeID)
	if bestNode == nil {
		return nil, fmt.Errorf("internal error: best node not found")
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
		NodeScore:        scores[0].Score,
		PlacementReasons: append(scores[0].Reasons, fmt.Sprintf("priority=%d", jobSpec.Priority)),
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

// ============================================================================
// RESOURCE ALLOCATION & RESERVATION
// ============================================================================

// ReserveResources: Reserve GPUs and memory on node
// Called after job placement decision
func (ls *LocalScheduler) ReserveResources(
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

	if node.AvailableGPUs < gpuCount {
		return fmt.Errorf("insufficient GPUs to reserve")
	}

	memoryGB := float64(memoryMB) / 1024.0
	if node.AvailableMemoryGB < memoryGB {
		return fmt.Errorf("insufficient memory to reserve")
	}

	// Reserve resources
	node.AvailableGPUs -= gpuCount
	node.AvailableMemoryGB -= memoryGB
	node.RunningJobsCount++

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

	// Free resources
	node.AvailableGPUs += gpuCount
	node.AvailableMemoryGB += float64(memoryMB) / 1024.0
	node.RunningJobsCount--

	if node.RunningJobsCount < 0 {
		node.RunningJobsCount = 0
	}

	return ls.UpdateNodeState(ctx, node)
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
