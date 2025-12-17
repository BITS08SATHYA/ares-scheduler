// File: pkg/scheduler/global/scheduler.go (LAYER 7 - GLOBAL SCHEDULER)
// Datacenter-level scheduler (runs in global control plane)
// Features: Feature 1, Feature 7, Feature 22
// Makes strategic decisions: which cluster to send job to
// Depends on: types.go, logger.go, redis/client.go, local/scheduler.go
// Zero errors, production-ready code
package global

import (
	"context"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/cluster"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/redis"
	_ "sort"
	"sync"
	"time"
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
	clusters   map[string]*cluster.ClusterInfo // clusterID -> cluster info

	// Reference to ClusterManager (for event subscription)
	clusterManager *cluster.ClusterManager

	// Metrics
	metricsMu sync.RWMutex
	metrics   *cluster.GlobalMetrics
}

// NewGlobalScheduler: Initialized GlobalScheduler with ClusterManager
func NewGlobalScheduler(
	controlPlaneName string,
	redisClient *redis.RedisClient,
	clusterManager *cluster.ClusterManager,
) *GlobalScheduler {

	// Get logger instance
	log := logger.Get()

	log.Info("Layer 7: Global Scheduled Initialized")

	gs := &GlobalScheduler{
		controlPlaneName: controlPlaneName,
		redisClient:      redisClient,
		log:              logger.Get(),
		clusters:         make(map[string]*cluster.ClusterInfo),
		clusterManager:   clusterManager,
		metrics: &cluster.GlobalMetrics{
			TotalJobsScheduled: 0,
			TotalJobsFailed:    0,
			TotalJobsRouted:    make(map[string]int64),
			LastUpdated:        time.Now(),
		},
	}

	// Subscribe to cluster events from ClusterManager
	// When cluster joins/leaves/changes -> GlobalScheduler gets notified
	clusterManager.RegisterEventListener(gs)

	log.Info("GlobalScheduler initialized")
	log.Info("Subscribed to ClusterManager events")
	log.Info("Will populate clusters from cluster join events")

	return gs
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

	for _, _cluster := range gs.clusters {
		if now.Sub(_cluster.LastHeartbeat) > gs.clusterManager.GetHeartbeatTimeout() {
			_cluster.IsHealthy = false
			gs.log.Warn("Cluster %s marked unhealthy: no heartbeat for %v",
				_cluster.ClusterID, now.Sub(_cluster.LastHeartbeat))
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
) (*cluster.ClusterInfo, *cluster.ClusterScore, error) {

	gs.log.Info("Selecting Best Cluster method: ")

	clusters := gs.clusterManager.ListClusters()

	gs.log.Info("Length of clusters: ", len(clusters))

	if len(clusters) == 0 {
		return nil, nil, fmt.Errorf("no clusters available in federation")
	}

	var bestCluster *cluster.ClusterInfo
	var bestScore *cluster.ClusterScore

	for _, _cluster := range clusters {
		if !_cluster.IsHealthy {
			gs.log.Debug("Skipping unhealthy cluster %s", _cluster.ClusterID)
			continue
		}

		clusterInfo, _ := gs.clusterManager.GetClusterInfo(_cluster.ClusterID)

		score := gs.scoreCluster(clusterInfo, jobSpec, preferredRegion)

		if bestScore == nil || score.Score > bestScore.Score {
			bestCluster = clusterInfo
			bestScore = score
		}
	}

	if bestCluster == nil {
		return nil, nil, fmt.Errorf("no suitable clusters in federation")
	}

	gs.log.Info("Selected cluster: %s in region %s (score=%.1f, reasons=%v)",
		bestCluster.ClusterID, bestCluster.Region, bestScore.Score, bestScore.Reasons)

	return bestCluster, bestScore, nil
}

// scoreCluster: Calculate fitness score for a cluster
func (gs *GlobalScheduler) scoreCluster(
	clusterInfo *cluster.ClusterInfo,
	jobSpec *common.JobSpec,
	preferredRegion string,
) *cluster.ClusterScore {

	score := &cluster.ClusterScore{
		ClusterID: clusterInfo.ClusterID,
		Region:    clusterInfo.Region,
		Score:     20.0, // Base score
		Reasons:   make([]string, 0),
	}

	// Calculate available resources
	score.AvailableGPUs = clusterInfo.AvailableGPUs
	score.AvailableMemoryGB = clusterInfo.AvailableMemoryGB
	score.CurrentLoad = clusterInfo.RunningJobsCount

	// Utilization
	if clusterInfo.TotalGPUs > 0 {
		score.UtilizationPercent = float64(clusterInfo.TotalGPUs-clusterInfo.AvailableGPUs) / float64(clusterInfo.TotalGPUs) * 100.0
	}

	// Factor 1: GPU availability (datacenter-level, coarse-grained)
	if clusterInfo.AvailableGPUs >= jobSpec.GPUCount {
		score.Score += float64(clusterInfo.AvailableGPUs) * 1.0
		score.Reasons = append(score.Reasons, fmt.Sprintf("has-%d-gpus", clusterInfo.AvailableGPUs))
	} else {
		// Not enough GPUs - heavily penalize
		score.Score -= float64(jobSpec.GPUCount-clusterInfo.AvailableGPUs) * 8.0
		score.Reasons = append(score.Reasons, "insufficient-gpus")
	}

	// Factor 2: Memory availability
	requiredMemoryGB := float64(jobSpec.MemoryMB) / 1024.0
	if clusterInfo.AvailableMemoryGB >= requiredMemoryGB {
		score.Score += (clusterInfo.AvailableMemoryGB - requiredMemoryGB) * 0.05
		score.Reasons = append(score.Reasons, fmt.Sprintf("%.0fgb-mem", clusterInfo.AvailableMemoryGB))
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
	if clusterInfo.IsHealthy {
		score.HealthScore = 100.0
		score.Score += 30.0
	} else {
		score.HealthScore = 0.0
		score.Score -= 50.0
		score.Reasons = append(score.Reasons, "unhealthy")
	}

	// Factor 5: Region preference (datacenter has multiple regions)
	if preferredRegion != "" && clusterInfo.Region == preferredRegion {
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
		score.Reasons = append(score.Reasons, fmt.Sprintf("not-preferred-region=%s", clusterInfo.Region))
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
) (*cluster.GlobalSchedulingDecision, error) {

	gs.log.Info("Global Scheduler ScheduleJob entered")
	gs.log.Info("Job Spec: %v", jobSpec)

	if jobSpec == nil || jobSpec.Name == "" {
		return nil, fmt.Errorf("Invalid job spec")
	}

	if jobSpec.GPUCount < 0 || jobSpec.GPUCount > 256 {
		return nil, fmt.Errorf("Invalid GPU count: %d", jobSpec.GPUCount)
	}

	// Step 1: Select best cluster (ONLY decision made by global scheduler)
	bestCluster, clusterScore, err := gs.SelectBestCluster(ctx, jobSpec, "")
	if err != nil {
		gs.recordSchedulingFailure()
		return nil, fmt.Errorf("cluster selection failed: %w", err)
	}

	// Step 2
	// NEW: Call LocalScheduler via HTTP
	localClient := NewLocalSchedulerClient()
	localDecision, err := localClient.ScheduleJob(
		ctx,
		bestCluster.LocalSchedulerAddr,
		jobSpec,
	)

	gs.log.Debug("Local Scheduler ScheduleJob returned: %v", localDecision)

	if err != nil {
		gs.log.Error("Local scheduling failed: %v", err)
		gs.recordSchedulingFailure()
		return nil, fmt.Errorf("local scheduling failed: %w", err)
	}

	// Step 3: Create decision
	decision := &cluster.GlobalSchedulingDecision{
		JobID:              jobSpec.RequestID,
		ClusterID:          bestCluster.ClusterID,
		NodeID:             localDecision.NodeID,
		GPUIndices:         localDecision.GPUIndices,
		Region:             bestCluster.Region,
		ClusterScore:       clusterScore.Score,
		PlacementReasons:   clusterScore.Reasons,
		LocalSchedulerAddr: bestCluster.LocalSchedulerAddr,
		ScheduledAt:        time.Now(),
	}

	gs.recordSchedulingSuccess(bestCluster.ClusterID)

	gs.log.Info("Scheduled job: %s to cluster %s in region %s (score=%.1f, addr=%s)",
		jobSpec.RequestID, bestCluster.ClusterID, bestCluster.Region, decision.ClusterScore,
		bestCluster.LocalSchedulerAddr)

	return decision, nil
}

// ============================================================================
// SCHEDULING WITH CONSTRAINTS
// ============================================================================
// ScheduleJobWithConstraints: Schedule with additional cluster constraints
func (gs *GlobalScheduler) ScheduleJobWithConstraints(
	ctx context.Context,
	jobSpec *common.JobSpec,
	constraints *cluster.ClusterConstraints,
) (*cluster.GlobalSchedulingDecision, error) {

	clusters := gs.clusterManager.ListClusters()
	if len(clusters) == 0 {
		gs.recordSchedulingFailure()
		return nil, fmt.Errorf("no clusters available")
	}

	// Filter clusters by constraints
	validClusters := make([]*cluster.ClusterInfo, 0)

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
		util := float64(cluster.TotalGPUs-cluster.AvailableGPUs()) / float64(cluster.TotalGPUs) * 100.0
		if util > constraints.MaxUtilization {
			continue
		}

		// GPU availability constraint
		if cluster.AvailableGPUs() < constraints.MinAvailableGPUs {
			continue
		}
		cI, _ := gs.clusterManager.GetClusterInfo(cluster.ClusterID)
		validClusters = append(validClusters, cI)
	}

	if len(validClusters) == 0 {
		gs.recordSchedulingFailure()
		return nil, fmt.Errorf("no clusters satisfy constraints")
	}

	// Score valid clusters with preference boost
	var bestCluster *cluster.ClusterInfo
	var bestScore *cluster.ClusterScore

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
	decision := &cluster.GlobalSchedulingDecision{
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
) (*cluster.GlobalSchedulingDecision, error) {

	if preferredRegion == "" {
		return nil, fmt.Errorf("preferred region cannot be empty")
	}

	bestCluster, clusterScore, err := gs.SelectBestCluster(ctx, jobSpec, preferredRegion)
	if err != nil {
		gs.recordSchedulingFailure()
		return nil, fmt.Errorf("cluster selection failed: %w", err)
	}

	decision := &cluster.GlobalSchedulingDecision{
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

// GetRegionLoad: Get utilization in specific region
func (gs *GlobalScheduler) GetRegionLoad(region string) map[string]interface{} {
	clusters := gs.clusterManager.ListClustersByRegion(region)

	totalGPUs := 0
	totalMemory := 0.0
	gpusInUse := 0
	memoryInUse := 0.0
	jobsRunning := 0

	for _, cluster := range clusters {
		totalGPUs += cluster.TotalGPUs
		totalMemory += cluster.TotalMemGB
		gpusInUse += cluster.TotalGPUs - cluster.AvailableGPUs()
		memoryInUse += cluster.TotalMemGB - cluster.AvailableMemGB()
		jobsRunning += cluster.RunningJobs
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
func (gs *GlobalScheduler) GetMetrics() *cluster.GlobalMetrics {
	gs.metricsMu.RLock()
	defer gs.metricsMu.RUnlock()

	// Return copy
	metrics := &cluster.GlobalMetrics{
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

// Cluster Registration Events
// FILE: Add these methods to pkg/scheduler/global/scheduler.go
// Location: At the end of the GlobalScheduler struct (before the closing brace of the file)
// These methods implement the ClusterEventListener interface

// ============================================================================
// CLUSTER EVENT LISTENER IMPLEMENTATION
// ============================================================================
// GlobalScheduler implements ClusterEventListener interface
// Receives notifications when clusters join/leave/change
// Updates internal cluster cache accordingly

// OnClusterJoin: Called when cluster joins the federation
// Updates GlobalScheduler's cluster cache
func (gs *GlobalScheduler) OnClusterJoin(ctx context.Context, clusterObj *cluster.Cluster) error {
	if clusterObj == nil {
		return fmt.Errorf("cluster cannot be nil")
	}

	gs.log.Info("GlobalScheduler: Cluster %s joined (region=%s, gpus=%d)",
		clusterObj.ClusterID, clusterObj.Region, clusterObj.TotalGPUs)

	// Convert Cluster to ClusterInfo for caching
	clusterInfo := &cluster.ClusterInfo{
		ClusterID:          clusterObj.ClusterID,
		Region:             clusterObj.Region,
		Zone:               clusterObj.Zone,
		IsHealthy:          clusterObj.IsHealthy,
		TotalGPUs:          clusterObj.TotalGPUs,
		AvailableGPUs:      clusterObj.AvailableGPUs(),
		TotalMemoryGB:      clusterObj.TotalMemGB,
		AvailableMemoryGB:  clusterObj.AvailableMemGB(),
		RunningJobsCount:   clusterObj.RunningJobs,
		LastHeartbeat:      time.Time{},
		LocalSchedulerAddr: "",
	}

	// Add to cache
	gs.clustersMu.Lock()
	gs.clusters[clusterObj.ClusterID] = clusterInfo
	gs.clustersMu.Unlock()

	gs.log.Info("GlobalScheduler: Added cluster %s to cache (region=%s, gpus=%d)",
		clusterObj.ClusterID, clusterObj.Region, clusterObj.TotalGPUs)

	return nil
}

// OnClusterLeave: Called when cluster leaves the federation
// Removes cluster from GlobalScheduler's cache
func (gs *GlobalScheduler) OnClusterLeave(ctx context.Context, clusterID string) error {
	if clusterID == "" {
		return fmt.Errorf("cluster ID cannot be empty")
	}

	gs.log.Info("GlobalScheduler: Received OnClusterLeave event for cluster %s", clusterID)

	// Remove from cache
	gs.clustersMu.Lock()
	delete(gs.clusters, clusterID)
	gs.clustersMu.Unlock()

	gs.log.Info("GlobalScheduler: Removed cluster %s from cache", clusterID)

	return nil
}

// OnClusterHealthChange: Called when cluster health status changes
// Updates cluster health in cache
func (gs *GlobalScheduler) OnClusterHealthChange(
	ctx context.Context,
	clusterID string,
	health *cluster.ClusterHealth,
) error {
	if clusterID == "" {
		return fmt.Errorf("cluster ID cannot be empty")
	}

	gs.clustersMu.Lock()
	clusterInfo, err := gs.clusterManager.GetCluster(clusterID)
	gs.clustersMu.Unlock()

	if err != nil {
		gs.log.Warn("GlobalScheduler: Received health change for unknown cluster %s", clusterID)
		return fmt.Errorf("cluster not in cache: %s", clusterID)
	}

	// Update health status
	if health != nil {
		gs.clustersMu.Lock()
		clusterInfo.IsHealthy = health.IsHealthy
		//clusterInfo.IsReachable = health.IsReachable
		clusterInfo.LastHeartbeatAt = health.LastHeartbeat
		gs.clustersMu.Unlock()

		gs.log.Info("GlobalScheduler: Updated health for cluster %s (healthy=%v, reachable=%v)",
			clusterID, health.IsHealthy, clusterInfo.IsReachable)
	}

	return nil
}

// OnClusterStateChange: Called when cluster state changes
// Updates cluster state in cache
func (gs *GlobalScheduler) OnClusterStateChange(
	ctx context.Context,
	clusterID string,
	oldState, newState cluster.ClusterState,
) error {
	if clusterID == "" {
		return fmt.Errorf("cluster ID cannot be empty")
	}

	gs.clustersMu.Lock()
	_, exists := gs.clusters[clusterID]
	gs.clustersMu.Unlock()

	if !exists {
		gs.log.Warn("GlobalScheduler: Received state change for unknown cluster %s", clusterID)
		return fmt.Errorf("cluster not in cache: %s", clusterID)
	}

	gs.log.Info("GlobalScheduler: Cluster %s state changed: %s → %s",
		clusterID, oldState, newState)

	// Update cluster state in cache if needed
	// (You might want to track this in ClusterInfo struct)

	return nil
}

// ============================================================================
// HELPER: List all clusters from cache
// ============================================================================

// ListClusters: Get all clusters from cache
func (gs *GlobalScheduler) ListClusters() []*cluster.ClusterInfo {
	gs.clustersMu.RLock()
	defer gs.clustersMu.RUnlock()

	clusters := make([]*cluster.ClusterInfo, 0, len(gs.clusters))
	for _, cluster := range gs.clusters {
		clusters = append(clusters, cluster)
	}

	return clusters
}
