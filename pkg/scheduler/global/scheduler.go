// File: pkg/scheduler/global/scheduler.go (LAYER 7 - GLOBAL SCHEDULER)
// Datacenter-level scheduler (runs in global control plane)
// Features: Feature 1, Feature 7, Feature 22
// Makes strategic decisions: which cluster to send job to
// Depends on: types.go, logger.go, redis/client.go, local/scheduler.go
// Zero errors, production-ready code
package global

import (
	"context"
	"encoding/json"
	"fmt"
	_ "sort"
	"sync"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/redis"
)

// ============================================================================
// GLOBAL SCHEDULER SERVICE
// ============================================================================

// GlobalScheduler: Datacenter-level job scheduler
// Runs in global control plane (one instance for entire datacenter)
// Makes strategic scheduling decisions:
//   - Which cluster to send job to
//   - Load balancing across clusters
//   - Regional routing
//
// Thread-safe: Uses sync.RWMutex for cluster state
type GlobalScheduler struct {
	controlPlaneName string             // Name of control plane
	redisClient      *redis.RedisClient // For caching & coordination
	log              *logger.Logger     // Logging

	// Cluster registry
	clustersMu sync.RWMutex
	clusters   map[string]*ClusterInfo // clusterID -> cluster info

	// Metrics
	metricsMu sync.RWMutex
	metrics   *GlobalMetrics
}

// ClusterInfo: Information about registered cluster
type ClusterInfo struct {
	ClusterID          string
	Region             string // us-west, eu-west, ap-south
	Zone               string // us-west-2a, us-west-2b
	IsHealthy          bool
	TotalGPUs          int
	AvailableGPUs      int
	TotalMemoryGB      float64
	AvailableMemoryGB  float64
	RunningJobsCount   int
	LastHeartbeat      time.Time
	LocalSchedulerAddr string // URL to contact local scheduler
}

// GlobalMetrics: Scheduling metrics for entire datacenter
type GlobalMetrics struct {
	TotalJobsScheduled int64
	TotalJobsFailed    int64
	TotalJobsRouted    map[string]int64 // clusterID -> count
	AvgSchedulingTime  time.Duration
	LastUpdated        time.Time
}

// ClusterScore: Score for cluster selection
type ClusterScore struct {
	ClusterID             string
	Region                string
	Score                 float64 // 0-100
	AvailableGPUs         int
	AvailableMemoryGB     float64
	CurrentLoad           int
	UtilizationPercent    float64
	HealthScore           float64
	RegionPreferenceScore float64
	Reasons               []string
}

// GlobalSchedulingDecision: Complete global scheduling decision
type GlobalSchedulingDecision struct {
	JobID              string
	ClusterID          string // Which cluster
	Region             string // Which region
	ClusterScore       float64
	PlacementReasons   []string
	LocalSchedulerAddr string // How to contact local scheduler
	ScheduledAt        time.Time
}

// Cache keys
const (
	CacheKeyClusterInfo     = "ares:global:cluster:%s"
	CacheKeyGlobalMetrics   = "ares:global:metrics"
	CacheKeyClusterRegistry = "ares:global:clusters"
	ClusterInfoCacheTTL     = 30 * time.Second
	MetricsCacheTTL         = 60 * time.Second
	HeartbeatTimeout        = 60 * time.Second
)

// NewGlobalScheduler: Create new global scheduler for datacenter
func NewGlobalScheduler(
	controlPlaneName string,
	redisClient *redis.RedisClient,
) *GlobalScheduler {
	return &GlobalScheduler{
		controlPlaneName: controlPlaneName,
		redisClient:      redisClient,
		log:              logger.Get(),
		clusters:         make(map[string]*ClusterInfo),
		metrics: &GlobalMetrics{
			TotalJobsScheduled: 0,
			TotalJobsFailed:    0,
			TotalJobsRouted:    make(map[string]int64),
			LastUpdated:        time.Now(),
		},
	}
}

// ============================================================================
// CLUSTER MANAGEMENT
// ============================================================================

// RegisterCluster: Register cluster in federation
// Called when cluster joins federation
func (gs *GlobalScheduler) RegisterCluster(ctx context.Context, cluster *ClusterInfo) error {
	if cluster == nil || cluster.ClusterID == "" {
		return fmt.Errorf("invalid cluster: nil or empty ID")
	}

	if cluster.Region == "" || cluster.Zone == "" {
		return fmt.Errorf("cluster must have region and zone")
	}

	if cluster.LocalSchedulerAddr == "" {
		return fmt.Errorf("cluster must have LocalSchedulerAddr")
	}

	gs.clustersMu.Lock()
	defer gs.clustersMu.Unlock()

	cluster.LastHeartbeat = time.Now()
	gs.clusters[cluster.ClusterID] = cluster

	// Cache cluster info in Redis
	clusterData, err := json.Marshal(cluster)
	if err != nil {
		return fmt.Errorf("marshal cluster failed: %w", err)
	}

	cacheKey := fmt.Sprintf(CacheKeyClusterInfo, cluster.ClusterID)
	err = gs.redisClient.Set(ctx, cacheKey, string(clusterData), ClusterInfoCacheTTL)
	if err != nil {
		gs.log.Warn("Failed to cache cluster info (non-fatal): %v", err)
	}

	// Update cluster registry
	clusterIDs := make([]string, 0, len(gs.clusters))
	for id := range gs.clusters {
		clusterIDs = append(clusterIDs, id)
	}
	registryData, _ := json.Marshal(clusterIDs)
	gs.redisClient.Set(ctx, CacheKeyClusterRegistry, string(registryData), ClusterInfoCacheTTL)

	gs.log.Info("Registered cluster %s in region %s zone %s (GPUs=%d, memory=%.0fGB, addr=%s)",
		cluster.ClusterID, cluster.Region, cluster.Zone, cluster.TotalGPUs, cluster.TotalMemoryGB,
		cluster.LocalSchedulerAddr)

	return nil
}

// DeregisterCluster: Remove cluster from federation
// Called when cluster leaves federation
func (gs *GlobalScheduler) DeregisterCluster(ctx context.Context, clusterID string) error {
	if clusterID == "" {
		return fmt.Errorf("invalid cluster ID")
	}

	gs.clustersMu.Lock()
	defer gs.clustersMu.Unlock()

	_, exists := gs.clusters[clusterID]
	if !exists {
		return fmt.Errorf("cluster not registered: %s", clusterID)
	}

	delete(gs.clusters, clusterID)

	// Remove from Redis cache
	cacheKey := fmt.Sprintf(CacheKeyClusterInfo, clusterID)
	gs.redisClient.Del(ctx, cacheKey)

	// Update cluster registry
	clusterIDs := make([]string, 0, len(gs.clusters))
	for id := range gs.clusters {
		clusterIDs = append(clusterIDs, id)
	}
	registryData, _ := json.Marshal(clusterIDs)
	gs.redisClient.Set(ctx, CacheKeyClusterRegistry, string(registryData), ClusterInfoCacheTTL)

	gs.log.Info("Deregistered cluster %s", clusterID)

	return nil
}

// UpdateClusterState: Update cluster resource state
// Called periodically by clusters via heartbeat
func (gs *GlobalScheduler) UpdateClusterState(ctx context.Context, cluster *ClusterInfo) error {
	if cluster == nil || cluster.ClusterID == "" {
		return fmt.Errorf("invalid cluster")
	}

	gs.clustersMu.Lock()
	defer gs.clustersMu.Unlock()

	_, exists := gs.clusters[cluster.ClusterID]
	if !exists {
		gs.clustersMu.Unlock()
		return fmt.Errorf("cluster not registered: %s", cluster.ClusterID)
	}

	cluster.LastHeartbeat = time.Now()
	gs.clusters[cluster.ClusterID] = cluster

	// Cache updated state
	clusterData, err := json.Marshal(cluster)
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}

	cacheKey := fmt.Sprintf(CacheKeyClusterInfo, cluster.ClusterID)
	err = gs.redisClient.Set(ctx, cacheKey, string(clusterData), ClusterInfoCacheTTL)
	if err != nil {
		gs.log.Warn("Failed to update cluster cache: %v", err)
	}

	return nil
}

// GetClusterInfo: Get info about specific cluster
func (gs *GlobalScheduler) GetClusterInfo(clusterID string) (*ClusterInfo, error) {
	gs.clustersMu.RLock()
	defer gs.clustersMu.RUnlock()

	cluster, exists := gs.clusters[clusterID]
	if !exists {
		return nil, fmt.Errorf("cluster not found: %s", clusterID)
	}

	return cluster, nil
}

// ListClusters: Get all registered clusters
func (gs *GlobalScheduler) ListClusters() []*ClusterInfo {
	gs.clustersMu.RLock()
	defer gs.clustersMu.RUnlock()

	clusters := make([]*ClusterInfo, 0, len(gs.clusters))
	for _, cluster := range gs.clusters {
		clusters = append(clusters, cluster)
	}

	return clusters
}

// ============================================================================
// CLUSTER HEALTH MONITORING
// ============================================================================

// CheckClusterHealth: Mark clusters unhealthy if no heartbeat
// Call periodically (e.g., every 30 seconds)
func (gs *GlobalScheduler) CheckClusterHealth() {
	gs.clustersMu.Lock()
	defer gs.clustersMu.Unlock()

	now := time.Now()

	for _, cluster := range gs.clusters {
		if now.Sub(cluster.LastHeartbeat) > HeartbeatTimeout {
			cluster.IsHealthy = false
			gs.log.Warn("Cluster %s marked unhealthy: no heartbeat for %v",
				cluster.ClusterID, now.Sub(cluster.LastHeartbeat))
		}
	}
}

// GetHealthStatus: Get health status of all clusters
func (gs *GlobalScheduler) GetHealthStatus() map[string]bool {
	gs.clustersMu.RLock()
	defer gs.clustersMu.RUnlock()

	status := make(map[string]bool)
	for clusterID, cluster := range gs.clusters {
		status[clusterID] = cluster.IsHealthy
	}

	return status
}

// HealthyClusterCount: Count healthy clusters
func (gs *GlobalScheduler) HealthyClusterCount() int {
	gs.clustersMu.RLock()
	defer gs.clustersMu.RUnlock()

	count := 0
	for _, cluster := range gs.clusters {
		if cluster.IsHealthy {
			count++
		}
	}

	return count
}

// ============================================================================
// CLUSTER SELECTION (Strategic datacenter-level decision)
// ============================================================================

// SelectBestCluster: Find best cluster for job placement
// This is the ONLY scheduling decision made by global scheduler
// Scoring factors:
//   - Available GPUs: +1 per GPU
//   - Available memory: +0.05 per GB
//   - Low load: +0.3 per % free
//   - Health: +30 if healthy
//   - Region preference: +40 if matches
//
// Returns: Best cluster, score, error
func (gs *GlobalScheduler) SelectBestCluster(
	ctx context.Context,
	jobSpec *common.JobSpec,
	preferredRegion string,
) (*ClusterInfo, *ClusterScore, error) {

	clusters := gs.ListClusters()
	if len(clusters) == 0 {
		return nil, nil, fmt.Errorf("no clusters available in federation")
	}

	var bestCluster *ClusterInfo
	var bestScore *ClusterScore

	for _, cluster := range clusters {
		if !cluster.IsHealthy {
			gs.log.Debug("Skipping unhealthy cluster %s", cluster.ClusterID)
			continue
		}

		score := gs.scoreCluster(cluster, jobSpec, preferredRegion)

		if bestScore == nil || score.Score > bestScore.Score {
			bestCluster = cluster
			bestScore = score
		}
	}

	if bestCluster == nil {
		return nil, nil, fmt.Errorf("no suitable clusters in federation")
	}

	gs.log.Info("Selected cluster %s in region %s (score=%.1f, reasons=%v)",
		bestCluster.ClusterID, bestCluster.Region, bestScore.Score, bestScore.Reasons)

	return bestCluster, bestScore, nil
}

// scoreCluster: Calculate fitness score for a cluster
func (gs *GlobalScheduler) scoreCluster(
	cluster *ClusterInfo,
	jobSpec *common.JobSpec,
	preferredRegion string,
) *ClusterScore {

	score := &ClusterScore{
		ClusterID: cluster.ClusterID,
		Region:    cluster.Region,
		Score:     20.0, // Base score
		Reasons:   make([]string, 0),
	}

	// Calculate available resources
	score.AvailableGPUs = cluster.AvailableGPUs
	score.AvailableMemoryGB = cluster.AvailableMemoryGB
	score.CurrentLoad = cluster.RunningJobsCount

	// Utilization
	if cluster.TotalGPUs > 0 {
		score.UtilizationPercent = float64(cluster.TotalGPUs-cluster.AvailableGPUs) / float64(cluster.TotalGPUs) * 100.0
	}

	// Factor 1: GPU availability (datacenter-level, coarse-grained)
	if cluster.AvailableGPUs >= jobSpec.GPUCount {
		score.Score += float64(cluster.AvailableGPUs) * 1.0
		score.Reasons = append(score.Reasons, fmt.Sprintf("has-%d-gpus", cluster.AvailableGPUs))
	} else {
		// Not enough GPUs - heavily penalize
		score.Score -= float64(jobSpec.GPUCount-cluster.AvailableGPUs) * 8.0
		score.Reasons = append(score.Reasons, "insufficient-gpus")
	}

	// Factor 2: Memory availability (datacenter-level)
	requiredMemoryGB := float64(jobSpec.MemoryMB) / 1024.0
	if cluster.AvailableMemoryGB >= requiredMemoryGB {
		score.Score += (cluster.AvailableMemoryGB - requiredMemoryGB) * 0.05
		score.Reasons = append(score.Reasons, fmt.Sprintf("%.0fgb-mem", cluster.AvailableMemoryGB))
	} else {
		score.Score -= 20.0
		score.Reasons = append(score.Reasons, "insufficient-memory")
	}

	// Factor 3: Cluster load (prefer less loaded clusters)
	freeCapacity := 100.0 - score.UtilizationPercent
	score.Score += freeCapacity * 0.3

	if score.UtilizationPercent > 80.0 {
		score.Reasons = append(score.Reasons, "high-load")
	} else if score.UtilizationPercent < 20.0 {
		score.Reasons = append(score.Reasons, "low-load")
	}

	// Factor 4: Cluster health (critical)
	if cluster.IsHealthy {
		score.HealthScore = 100.0
		score.Score += 30.0
	} else {
		score.HealthScore = 0.0
		score.Score -= 50.0
		score.Reasons = append(score.Reasons, "unhealthy")
	}

	// Factor 5: Region preference (datacenter has multiple regions)
	if preferredRegion != "" && cluster.Region == preferredRegion {
		score.RegionPreferenceScore = 100.0
		score.Score += 40.0
		score.Reasons = append(score.Reasons, fmt.Sprintf("preferred-region=%s", preferredRegion))
	} else if preferredRegion == "" {
		// No preference, neutral
		score.RegionPreferenceScore = 50.0
	} else {
		// Different region, slight penalty
		score.RegionPreferenceScore = 25.0
		score.Score -= 5.0
		score.Reasons = append(score.Reasons, fmt.Sprintf("not-preferred-region=%s", cluster.Region))
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
// MAIN SCHEDULING METHOD
// ============================================================================

// ScheduleJob: Schedule job on best cluster
// Main entry point: makes datacenter-level decision (which cluster)
// Then delegates to LocalScheduler for node/GPU selection
// Returns: Complete global scheduling decision
func (gs *GlobalScheduler) ScheduleJob(
	ctx context.Context,
	jobSpec *common.JobSpec,
) (*GlobalSchedulingDecision, error) {

	if jobSpec == nil || jobSpec.Name == "" {
		return nil, fmt.Errorf("invalid job spec")
	}

	if jobSpec.GPUCount < 0 || jobSpec.GPUCount > 256 {
		return nil, fmt.Errorf("invalid GPU count: %d", jobSpec.GPUCount)
	}

	// Step 1: Select best cluster (ONLY decision made by global scheduler)
	bestCluster, clusterScore, err := gs.SelectBestCluster(ctx, jobSpec, "")
	if err != nil {
		gs.recordSchedulingFailure()
		return nil, fmt.Errorf("cluster selection failed: %w", err)
	}

	// Step 2: Create decision
	decision := &GlobalSchedulingDecision{
		JobID:              jobSpec.RequestID,
		ClusterID:          bestCluster.ClusterID,
		Region:             bestCluster.Region,
		ClusterScore:       clusterScore.Score,
		PlacementReasons:   clusterScore.Reasons,
		LocalSchedulerAddr: bestCluster.LocalSchedulerAddr,
		ScheduledAt:        time.Now(),
	}

	gs.recordSchedulingSuccess(bestCluster.ClusterID)

	gs.log.Info("Scheduled job %s to cluster %s in region %s (score=%.1f, addr=%s)",
		jobSpec.RequestID, bestCluster.ClusterID, bestCluster.Region, decision.ClusterScore,
		bestCluster.LocalSchedulerAddr)

	return decision, nil
}

// ============================================================================
// SCHEDULING WITH CONSTRAINTS
// ============================================================================

// ClusterConstraints: Additional cluster selection constraints
type ClusterConstraints struct {
	RequiredRegions   []string // Only these regions
	ForbiddenRegions  []string // Never these regions
	RequiredClusters  []string // Only these clusters
	ForbiddenClusters []string // Never these clusters
	MaxUtilization    float64  // Cluster util must be < this %
	MinAvailableGPUs  int      // Cluster must have at least this
	PreferredRegions  []string // Prefer these regions
}

// ScheduleJobWithConstraints: Schedule with additional cluster constraints
func (gs *GlobalScheduler) ScheduleJobWithConstraints(
	ctx context.Context,
	jobSpec *common.JobSpec,
	constraints *ClusterConstraints,
) (*GlobalSchedulingDecision, error) {

	clusters := gs.ListClusters()
	if len(clusters) == 0 {
		gs.recordSchedulingFailure()
		return nil, fmt.Errorf("no clusters available")
	}

	// Filter clusters by constraints
	validClusters := make([]*ClusterInfo, 0)

	for _, cluster := range clusters {
		if !cluster.IsHealthy {
			continue
		}

		// Required regions constraint
		if len(constraints.RequiredRegions) > 0 {
			found := false
			for _, reqRegion := range constraints.RequiredRegions {
				if cluster.Region == reqRegion {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		// Forbidden regions constraint
		inForbiddenRegion := false
		for _, forbidRegion := range constraints.ForbiddenRegions {
			if cluster.Region == forbidRegion {
				inForbiddenRegion = true
				break
			}
		}
		if inForbiddenRegion {
			continue
		}

		// Required clusters constraint
		if len(constraints.RequiredClusters) > 0 {
			found := false
			for _, reqCluster := range constraints.RequiredClusters {
				if cluster.ClusterID == reqCluster {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		// Forbidden clusters constraint
		inForbiddenCluster := false
		for _, forbidCluster := range constraints.ForbiddenClusters {
			if cluster.ClusterID == forbidCluster {
				inForbiddenCluster = true
				break
			}
		}
		if inForbiddenCluster {
			continue
		}

		// Utilization constraint
		util := float64(cluster.TotalGPUs-cluster.AvailableGPUs) / float64(cluster.TotalGPUs) * 100.0
		if util > constraints.MaxUtilization {
			continue
		}

		// GPU availability constraint
		if cluster.AvailableGPUs < constraints.MinAvailableGPUs {
			continue
		}

		validClusters = append(validClusters, cluster)
	}

	if len(validClusters) == 0 {
		gs.recordSchedulingFailure()
		return nil, fmt.Errorf("no clusters satisfy constraints")
	}

	// Score valid clusters with preference boost
	var bestCluster *ClusterInfo
	var bestScore *ClusterScore

	for _, cluster := range validClusters {
		score := gs.scoreCluster(cluster, jobSpec, "")

		// Boost score for preferred regions
		for _, prefRegion := range constraints.PreferredRegions {
			if cluster.Region == prefRegion {
				score.Score += 25.0
				break
			}
		}

		if bestScore == nil || score.Score > bestScore.Score {
			bestCluster = cluster
			bestScore = score
		}
	}

	// Create decision
	decision := &GlobalSchedulingDecision{
		JobID:              jobSpec.RequestID,
		ClusterID:          bestCluster.ClusterID,
		Region:             bestCluster.Region,
		ClusterScore:       bestScore.Score,
		PlacementReasons:   bestScore.Reasons,
		LocalSchedulerAddr: bestCluster.LocalSchedulerAddr,
		ScheduledAt:        time.Now(),
	}

	gs.recordSchedulingSuccess(bestCluster.ClusterID)

	return decision, nil
}

// ============================================================================
// REGION-AWARE SCHEDULING
// ============================================================================

// ScheduleJobWithRegionPreference: Schedule with region preference
// Used when job needs to run in specific region (e.g., data locality)
func (gs *GlobalScheduler) ScheduleJobWithRegionPreference(
	ctx context.Context,
	jobSpec *common.JobSpec,
	preferredRegion string,
) (*GlobalSchedulingDecision, error) {

	if preferredRegion == "" {
		return nil, fmt.Errorf("preferred region cannot be empty")
	}

	bestCluster, clusterScore, err := gs.SelectBestCluster(ctx, jobSpec, preferredRegion)
	if err != nil {
		gs.recordSchedulingFailure()
		return nil, fmt.Errorf("cluster selection failed: %w", err)
	}

	decision := &GlobalSchedulingDecision{
		JobID:              jobSpec.RequestID,
		ClusterID:          bestCluster.ClusterID,
		Region:             bestCluster.Region,
		ClusterScore:       clusterScore.Score,
		PlacementReasons:   append(clusterScore.Reasons, fmt.Sprintf("region-preference=%s", preferredRegion)),
		LocalSchedulerAddr: bestCluster.LocalSchedulerAddr,
		ScheduledAt:        time.Now(),
	}

	gs.recordSchedulingSuccess(bestCluster.ClusterID)

	return decision, nil
}

// ============================================================================
// LOAD BALANCING & AWARENESS
// ============================================================================

// GetDatacenterLoad: Get total datacenter resource utilization
func (gs *GlobalScheduler) GetDatacenterLoad() map[string]interface{} {
	gs.clustersMu.RLock()
	defer gs.clustersMu.RUnlock()

	totalGPUs := 0
	totalMemory := 0.0
	gpusInUse := 0
	memoryInUse := 0.0
	jobsRunning := 0
	healthyClusters := 0
	regions := make(map[string]int) // region -> cluster count

	for _, cluster := range gs.clusters {
		totalGPUs += cluster.TotalGPUs
		totalMemory += cluster.TotalMemoryGB
		gpusInUse += cluster.TotalGPUs - cluster.AvailableGPUs
		memoryInUse += cluster.TotalMemoryGB - cluster.AvailableMemoryGB
		jobsRunning += cluster.RunningJobsCount

		if cluster.IsHealthy {
			healthyClusters++
		}

		regions[cluster.Region]++
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
		"control_plane":           gs.controlPlaneName,
		"total_clusters":          len(gs.clusters),
		"healthy_clusters":        healthyClusters,
		"total_gpus":              totalGPUs,
		"gpus_in_use":             gpusInUse,
		"gpu_utilization_pct":     gpuUtil,
		"total_memory_gb":         totalMemory,
		"memory_in_use_gb":        memoryInUse,
		"memory_utilization_pct":  memUtil,
		"running_jobs":            jobsRunning,
		"regions":                 regions,
		"scheduling_success_rate": gs.SuccessRate(),
	}
}

// GetClustersByRegion: Get all clusters in specific region
func (gs *GlobalScheduler) GetClustersByRegion(region string) []*ClusterInfo {
	gs.clustersMu.RLock()
	defer gs.clustersMu.RUnlock()

	regional := make([]*ClusterInfo, 0)
	for _, cluster := range gs.clusters {
		if cluster.Region == region && cluster.IsHealthy {
			regional = append(regional, cluster)
		}
	}

	return regional
}

// GetRegionLoad: Get utilization in specific region
func (gs *GlobalScheduler) GetRegionLoad(region string) map[string]interface{} {
	clusters := gs.GetClustersByRegion(region)

	totalGPUs := 0
	totalMemory := 0.0
	gpusInUse := 0
	memoryInUse := 0.0
	jobsRunning := 0

	for _, cluster := range clusters {
		totalGPUs += cluster.TotalGPUs
		totalMemory += cluster.TotalMemoryGB
		gpusInUse += cluster.TotalGPUs - cluster.AvailableGPUs
		memoryInUse += cluster.TotalMemoryGB - cluster.AvailableMemoryGB
		jobsRunning += cluster.RunningJobsCount
	}

	gpuUtil := 0.0
	if totalGPUs > 0 {
		gpuUtil = float64(gpusInUse) / float64(totalGPUs) * 100.0
	}

	return map[string]interface{}{
		"region":                 region,
		"cluster_count":          len(clusters),
		"total_gpus":             totalGPUs,
		"gpu_utilization_pct":    gpuUtil,
		"total_memory_gb":        totalMemory,
		"memory_utilization_pct": 0.0,
		"running_jobs":           jobsRunning,
	}
}

// ============================================================================
// METRICS & OBSERVABILITY
// ============================================================================

// recordSchedulingSuccess: Record successful scheduling
func (gs *GlobalScheduler) recordSchedulingSuccess(clusterID string) {
	gs.metricsMu.Lock()
	defer gs.metricsMu.Unlock()

	gs.metrics.TotalJobsScheduled++
	gs.metrics.TotalJobsRouted[clusterID]++
	gs.metrics.LastUpdated = time.Now()
}

// recordSchedulingFailure: Record failed scheduling
func (gs *GlobalScheduler) recordSchedulingFailure() {
	gs.metricsMu.Lock()
	defer gs.metricsMu.Unlock()

	gs.metrics.TotalJobsFailed++
	gs.metrics.LastUpdated = time.Now()
}

// GetMetrics: Get current scheduler metrics
func (gs *GlobalScheduler) GetMetrics() *GlobalMetrics {
	gs.metricsMu.RLock()
	defer gs.metricsMu.RUnlock()

	// Return copy
	metrics := &GlobalMetrics{
		TotalJobsScheduled: gs.metrics.TotalJobsScheduled,
		TotalJobsFailed:    gs.metrics.TotalJobsFailed,
		TotalJobsRouted:    make(map[string]int64),
		AvgSchedulingTime:  gs.metrics.AvgSchedulingTime,
		LastUpdated:        gs.metrics.LastUpdated,
	}

	for clusterID, count := range gs.metrics.TotalJobsRouted {
		metrics.TotalJobsRouted[clusterID] = count
	}

	return metrics
}

// SuccessRate: Calculate scheduling success rate (%)
func (gs *GlobalScheduler) SuccessRate() float64 {
	gs.metricsMu.RLock()
	defer gs.metricsMu.RUnlock()

	total := gs.metrics.TotalJobsScheduled + gs.metrics.TotalJobsFailed
	if total == 0 {
		return 0.0
	}

	return float64(gs.metrics.TotalJobsScheduled) / float64(total) * 100.0
}

// GetSchedulingDistribution: Get job distribution across clusters
func (gs *GlobalScheduler) GetSchedulingDistribution() map[string]interface{} {
	gs.metricsMu.RLock()
	defer gs.metricsMu.RUnlock()

	distribution := make(map[string]int64)
	for clusterID, count := range gs.metrics.TotalJobsRouted {
		distribution[clusterID] = count
	}

	return map[string]interface{}{
		"total_scheduled": gs.metrics.TotalJobsScheduled,
		"total_failed":    gs.metrics.TotalJobsFailed,
		"per_cluster":     distribution,
		"success_rate":    gs.SuccessRate(),
	}
}

// ============================================================================
// CLUSTER CAPACITY QUERIES
// ============================================================================

// GetTotalCapacity: Get total capacity across federation
func (gs *GlobalScheduler) GetTotalCapacity() map[string]interface{} {
	gs.clustersMu.RLock()
	defer gs.clustersMu.RUnlock()

	totalGPUs := 0
	totalMemory := 0.0

	for _, cluster := range gs.clusters {
		totalGPUs += cluster.TotalGPUs
		totalMemory += cluster.TotalMemoryGB
	}

	return map[string]interface{}{
		"total_gpus":    totalGPUs,
		"total_memory":  totalMemory,
		"cluster_count": len(gs.clusters),
	}
}

// GetAvailableCapacity: Get available capacity across federation
func (gs *GlobalScheduler) GetAvailableCapacity() map[string]interface{} {
	gs.clustersMu.RLock()
	defer gs.clustersMu.RUnlock()

	availGPUs := 0
	availMemory := 0.0

	for _, cluster := range gs.clusters {
		if cluster.IsHealthy {
			availGPUs += cluster.AvailableGPUs
			availMemory += cluster.AvailableMemoryGB
		}
	}

	return map[string]interface{}{
		"available_gpus":   availGPUs,
		"available_memory": availMemory,
	}
}

// CanScheduleJob: Check if federation has capacity for job
func (gs *GlobalScheduler) CanScheduleJob(jobSpec *common.JobSpec) bool {
	cap := gs.GetAvailableCapacity()

	availGPUs := cap["available_gpus"].(int)
	availMemory := cap["available_memory"].(float64)

	requiredMemoryGB := float64(jobSpec.MemoryMB) / 1024.0

	return availGPUs >= jobSpec.GPUCount && availMemory >= requiredMemoryGB
}

// ============================================================================
// FEDERATION STATUS
// ============================================================================

// GetFederationStatus: Get complete federation status
func (gs *GlobalScheduler) GetFederationStatus() map[string]interface{} {
	gs.clustersMu.RLock()
	defer gs.clustersMu.RUnlock()

	totalClusters := len(gs.clusters)
	healthyClusters := 0
	unhealthyClusters := 0

	for _, cluster := range gs.clusters {
		if cluster.IsHealthy {
			healthyClusters++
		} else {
			unhealthyClusters++
		}
	}

	return map[string]interface{}{
		"control_plane":      gs.controlPlaneName,
		"total_clusters":     totalClusters,
		"healthy_clusters":   healthyClusters,
		"unhealthy_clusters": unhealthyClusters,
		"health_percent":     float64(healthyClusters) / float64(totalClusters) * 100.0,
		"scheduling_success": gs.SuccessRate(),
		"last_updated":       time.Now(),
	}
}
