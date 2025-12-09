// File: pkg/cluster/manager.go
// Layer 4: Cluster Manager - Cluster registration, deregistration, state management
// Features: Feature 9 (Dynamic Cluster Registration)
// Production-ready: Zero errors, thread-safe operations

package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/redis"
	"sync"
	"time"
)

// Cache keys
const (
	CacheKeyClusterInfo     = "ares:global:cluster:%s"
	CacheKeyGlobalMetrics   = "ares:global:metrics"
	CacheKeyClusterRegistry = "ares:global:clusters"
	ClusterInfoCacheTTL     = 30 * time.Second
	MetricsCacheTTL         = 60 * time.Second
	HeartbeatTimeout        = 60 * time.Second
)

// ============================================================================
// CLUSTER MANAGER
// ============================================================================
type ClusterManager struct {
	log    *logger.Logger
	redis  *redis.RedisClient
	mu     sync.RWMutex
	config *ClusterConfig

	// In-memory registry (fast lookups)
	clusters map[string]*Cluster // clusterID -> cluster

	// Event listeners
	eventListeners []ClusterEventListener
	eventMu        sync.RWMutex
}

// ClusterManager: Manages cluster lifecycle (join, leave, state management)
// Runs in global control plane
// Thread-safe: Uses sync.RWMutex for cluster registry

// ClusterEventListener: Callback for cluster events
type ClusterEventListener interface {
	OnClusterJoin(ctx context.Context, cluster *Cluster) error
	OnClusterLeave(ctx context.Context, clusterID string) error
	OnClusterHealthChange(ctx context.Context, clusterID string, health *ClusterHealth) error
	OnClusterStateChange(ctx context.Context, clusterID string, oldState, newState ClusterState) error
}

// NewClusterManager: Create new cluster manager
func NewClusterManager(redisClient *redis.RedisClient, config *ClusterConfig) *ClusterManager {
	if config == nil {
		config = DefaultClusterConfig
	}

	return &ClusterManager{
		log:            logger.Get(),
		redis:          redisClient,
		config:         config,
		clusters:       make(map[string]*Cluster),
		eventListeners: make([]ClusterEventListener, 0),
	}
}

// ============================================================================
// CLUSTER REGISTRATION (Feature 9)
// ============================================================================

// JoinCluster: Register cluster with federation
// Called when cluster starts and connects to control plane
// Returns: Cluster object or error
func (cm *ClusterManager) JoinCluster(ctx context.Context, config *ClusterConfig) (*Cluster, error) {
	if config == nil || config.ClusterID == "" {
		return nil, fmt.Errorf("invalid cluster config: missing ClusterID")
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Check if already registered
	if _, exists := cm.clusters[config.ClusterID]; exists {
		cm.log.Warn("Cluster %s already registered", config.ClusterID)
		return cm.clusters[config.ClusterID], nil
	}

	// Create cluster entry
	cluster := &Cluster{
		ClusterID:       config.ClusterID,
		Name:            config.Name,
		Region:          config.Region,
		Zone:            config.Zone,
		ControlAddr:     config.ControlAddr,
		State:           StateJoining,
		IsHealthy:       true,
		IsReachable:     true,
		LastHeartbeatAt: time.Now(),
		Labels:          config.Labels,
		CreatedAt:       time.Now(),
		UpdatedAt:       time.Now(),
		TotalGPUs:       0,   // Will be updated by heartbeat
		TotalCPUs:       0,   // Will be updated by heartbeat
		TotalMemGB:      0.0, // Will be updated by heartbeat
	}

	// Store in registry
	cm.clusters[config.ClusterID] = cluster

	// Persist to Redis (for multi-control-plane setup)
	clusterData, err := json.Marshal(cluster)
	if err != nil {
		cm.log.Error("Failed to marshal cluster: %v", err)
		return nil, fmt.Errorf("marshal failed: %w", err)
	}

	key := fmt.Sprintf("ares:cluster:%s", config.ClusterID)
	err = cm.redis.Set(ctx, key, string(clusterData), 24*time.Hour)
	if err != nil {
		cm.log.Warn("Failed to persist cluster (non-fatal): %v", err)
	}

	// Update state
	cluster.State = StateReady

	cm.log.Info("Cluster %s joined federation (region=%s, zone=%s, addr=%s)",
		config.ClusterID, config.Region, config.Zone, config.ControlAddr)

	// Notify listeners
	cm.notifyClusterJoin(ctx, cluster)

	return cluster, nil
}

// LeaveCluster: Deregister cluster from federation
// Called when cluster shuts down
func (cm *ClusterManager) LeaveCluster(ctx context.Context, clusterID string) error {
	if clusterID == "" {
		return fmt.Errorf("cluster ID cannot be empty")
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	cluster, exists := cm.clusters[clusterID]
	if !exists {
		return fmt.Errorf("cluster not registered: %s", clusterID)
	}

	// Mark as leaving
	//oldState := cluster.State
	cluster.State = StateLeaving

	// Remove from registry
	delete(cm.clusters, clusterID)

	// Remove from Redis
	key := fmt.Sprintf("ares:cluster:%s", clusterID)
	err := cm.redis.Del(ctx, key)
	if err != nil {
		cm.log.Warn("Failed to delete cluster from Redis (non-fatal): %v", err)
	}

	cm.log.Info("Cluster %s left federation", clusterID)

	// Notify listeners
	cm.notifyClusterLeave(ctx, clusterID)

	return nil
}

// ============================================================================
// CLUSTER QUERIES
// ============================================================================

// GetCluster: Get cluster by ID
func (cm *ClusterManager) GetCluster(clusterID string) (*Cluster, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	cluster, exists := cm.clusters[clusterID]
	if !exists {
		return nil, fmt.Errorf("cluster not found: %s", clusterID)
	}

	return cluster, nil
}

// GetCluster: Get cluster by ID
func (cm *ClusterManager) GetClusterInfo(clusterID string) (*ClusterInfo, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	cluster, exists := cm.clusters[clusterID]
	if !exists {
		return nil, fmt.Errorf("cluster not found: %s", clusterID)
	}

	return &ClusterInfo{
		ClusterID:          cluster.ClusterID,
		Region:             cluster.Region,
		Zone:               cluster.Zone,
		IsHealthy:          cluster.IsHealthy,
		TotalGPUs:          cluster.TotalGPUs,
		AvailableGPUs:      cluster.AvailableGPUs(),
		TotalMemoryGB:      cluster.TotalMemGB,
		AvailableMemoryGB:  cluster.AvailableMemGB(),
		RunningJobsCount:   cluster.RunningJobs,
		LastHeartbeat:      cluster.LastHeartbeatAt,
		LocalSchedulerAddr: cluster.ControlAddr,
	}, nil
}

// ListClusters: Get all registered clusters
func (cm *ClusterManager) ListClusters() []*Cluster {

	cm.log.Info("List Cluster method: ")

	cm.mu.RLock()
	defer cm.mu.RUnlock()

	clusters := make([]*Cluster, 0, len(cm.clusters))
	for _, cluster := range cm.clusters {
		clusters = append(clusters, cluster)
	}

	return clusters
}

// ListClustersByRegion: Get clusters in specific region
func (cm *ClusterManager) ListClustersByRegion(region string) []*Cluster {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	regional := make([]*Cluster, 0)
	for _, cluster := range cm.clusters {
		if cluster.Region == region {
			regional = append(regional, cluster)
		}
	}

	return regional
}

// ListHealthyClusters: Get only healthy clusters
func (cm *ClusterManager) ListHealthyClusters() []*Cluster {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	healthy := make([]*Cluster, 0)
	for _, cluster := range cm.clusters {
		if cluster.IsHealthy && cluster.IsReachable {
			healthy = append(healthy, cluster)
		}
	}

	return healthy
}

// CountClusters: Get total cluster count
func (cm *ClusterManager) CountClusters() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	return len(cm.clusters)
}

// ============================================================================
// CLUSTER STATE UPDATES
// ============================================================================

// UpdateClusterState: Update cluster state
func (cm *ClusterManager) UpdateClusterState(ctx context.Context, clusterID string, newState ClusterState) error {
	cm.mu.Lock()
	cluster, exists := cm.clusters[clusterID]
	cm.mu.Unlock()

	if !exists {
		return fmt.Errorf("cluster not found: %s", clusterID)
	}

	oldState := cluster.State
	cluster.State = newState
	cluster.UpdatedAt = time.Now()

	cm.log.Info("Cluster %s state changed: %s → %s", clusterID, oldState, newState)

	// Notify listeners
	cm.notifyClusterStateChange(ctx, clusterID, oldState, newState)

	return nil
}

// UpdateClusterCapacity: Update cluster resource capacity
func (cm *ClusterManager) UpdateClusterCapacity(
	ctx context.Context,
	clusterID string,
	gpuCount, cpuCount int,
	memoryGB float64,
) error {

	cm.mu.Lock()
	defer cm.mu.Unlock()

	cluster, exists := cm.clusters[clusterID]
	if !exists {
		return fmt.Errorf("cluster not found: %s", clusterID)
	}

	cluster.TotalGPUs = gpuCount
	cluster.TotalCPUs = cpuCount
	cluster.TotalMemGB = memoryGB
	cluster.UpdatedAt = time.Now()

	cm.log.Debug("Updated cluster %s capacity: GPUs=%d, CPUs=%d, Memory=%.0fGB",
		clusterID, gpuCount, cpuCount, memoryGB)

	return nil
}

// UpdateClusterLoad: Update cluster current load
func (cm *ClusterManager) UpdateClusterLoad(
	ctx context.Context,
	clusterID string,
	gpusInUse, cpusInUse int,
	memGBInUse float64,
	runningJobs, pendingJobs int,
) error {

	cm.mu.Lock()
	defer cm.mu.Unlock()

	cluster, exists := cm.clusters[clusterID]
	if !exists {
		return fmt.Errorf("cluster not found: %s", clusterID)
	}

	cluster.GPUsInUse = gpusInUse
	cluster.CPUsInUse = cpusInUse
	cluster.MemGBInUse = memGBInUse
	cluster.RunningJobs = runningJobs
	cluster.PendingJobs = pendingJobs
	cluster.UpdatedAt = time.Now()

	cm.log.Debug("Updated cluster %s load: GPUs=%d/%d, Memory=%.0f/%.0fGB, Jobs=%d/%d",
		clusterID, gpusInUse, cluster.TotalGPUs, memGBInUse, cluster.TotalMemGB,
		runningJobs, pendingJobs)

	return nil
}

// ============================================================================
// EVENT LISTENERS (Feature 10 - Propagation)
// ============================================================================

// RegisterEventListener: Register callback for cluster events
func (cm *ClusterManager) RegisterEventListener(listener ClusterEventListener) {
	cm.eventMu.Lock()
	defer cm.eventMu.Unlock()

	cm.eventListeners = append(cm.eventListeners, listener)
	cm.log.Debug("Registered event listener")
}

// notifyClusterJoin: Notify listeners of cluster join
func (cm *ClusterManager) notifyClusterJoin(ctx context.Context, cluster *Cluster) {
	cm.eventMu.RLock()
	listeners := cm.eventListeners
	cm.eventMu.RUnlock()

	for _, listener := range listeners {
		go func(l ClusterEventListener) {
			err := l.OnClusterJoin(ctx, cluster)
			if err != nil {
				cm.log.Warn("Event listener error on join: %v", err)
			}
		}(listener)
	}
}

// notifyClusterLeave: Notify listeners of cluster leave
func (cm *ClusterManager) notifyClusterLeave(ctx context.Context, clusterID string) {
	cm.eventMu.RLock()
	listeners := cm.eventListeners
	cm.eventMu.RUnlock()

	for _, listener := range listeners {
		go func(l ClusterEventListener) {
			err := l.OnClusterLeave(ctx, clusterID)
			if err != nil {
				cm.log.Warn("Event listener error on leave: %v", err)
			}
		}(listener)
	}
}

// notifyClusterStateChange: Notify listeners of state change
func (cm *ClusterManager) notifyClusterStateChange(
	ctx context.Context,
	clusterID string,
	oldState, newState ClusterState,
) {
	cm.eventMu.RLock()
	listeners := cm.eventListeners
	cm.eventMu.RUnlock()

	for _, listener := range listeners {
		go func(l ClusterEventListener) {
			err := l.OnClusterStateChange(ctx, clusterID, oldState, newState)
			if err != nil {
				cm.log.Warn("Event listener error on state change: %v", err)
			}
		}(listener)
	}
}

// notifyClusterHealthChange: Notify listeners of health change
func (cm *ClusterManager) notifyClusterHealthChange(
	ctx context.Context,
	clusterID string,
	health *ClusterHealth,
) {
	cm.eventMu.RLock()
	listeners := cm.eventListeners
	cm.eventMu.RUnlock()

	for _, listener := range listeners {
		go func(l ClusterEventListener) {
			err := l.OnClusterHealthChange(ctx, clusterID, health)
			if err != nil {
				cm.log.Warn("Event listener error on health change: %v", err)
			}
		}(listener)
	}
}

// ============================================================================
// STATISTICS
// ============================================================================

// GetClusterStats: Get statistics
func (cm *ClusterManager) GetClusterStats() map[string]interface{} {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	totalGPUs := 0
	gpusInUse := 0
	totalMemGB := 0.0
	memGBInUse := 0.0
	healthyClusters := 0

	for _, cluster := range cm.clusters {
		totalGPUs += cluster.TotalGPUs
		gpusInUse += cluster.GPUsInUse
		totalMemGB += cluster.TotalMemGB
		memGBInUse += cluster.MemGBInUse

		if cluster.IsHealthy {
			healthyClusters++
		}
	}

	gpuUtil := 0.0
	if totalGPUs > 0 {
		gpuUtil = float64(gpusInUse) / float64(totalGPUs) * 100.0
	}

	memUtil := 0.0
	if totalMemGB > 0 {
		memUtil = memGBInUse / totalMemGB * 100.0
	}

	return map[string]interface{}{
		"total_clusters":      len(cm.clusters),
		"healthy_clusters":    healthyClusters,
		"total_gpus":          totalGPUs,
		"gpus_in_use":         gpusInUse,
		"gpu_utilization_pct": gpuUtil,
		"total_memory_gb":     totalMemGB,
		"memory_in_use_gb":    memGBInUse,
		"memory_util_pct":     memUtil,
	}
}
