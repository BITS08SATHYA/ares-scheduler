// File: pkg/cluster/registry.go
// Layer 4: Cluster Registry - Central registry and federation management
// Features: Feature 9 (Dynamic Cluster Registration), Feature 11 (Eventual Consistency)
// Production-ready: Zero errors, federation-aware

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

// ============================================================================
// CLUSTER REGISTRY
// ============================================================================

// ClusterRegistry: Central registry of all clusters in federation
// Maintains list of clusters, their status, and federation metadata
// Supports eventual consistency for multi-control-plane deployments
type ClusterRegistry struct {
	log   *logger.Logger
	redis *redis.RedisClient
	mu    sync.RWMutex

	// Registry data
	clusters    map[string]*Cluster       // clusterID -> cluster
	regionIndex map[string][]string       // region -> [clusterIDs]
	stateIndex  map[ClusterState][]string // state -> [clusterIDs]

	// Metadata
	lastSync     time.Time
	syncInterval time.Duration
}

// NewClusterRegistry: Create new cluster registry
func NewClusterRegistry(redisClient *redis.RedisClient) *ClusterRegistry {
	return &ClusterRegistry{
		log:          logger.Get(),
		redis:        redisClient,
		clusters:     make(map[string]*Cluster),
		regionIndex:  make(map[string][]string),
		stateIndex:   make(map[ClusterState][]string),
		syncInterval: 30 * time.Second,
	}
}

// ============================================================================
// REGISTRY OPERATIONS
// ============================================================================

// RegisterCluster: Register cluster in registry (from ClusterManager)
func (cr *ClusterRegistry) RegisterCluster(ctx context.Context, cluster *Cluster) error {
	if cluster == nil || cluster.ClusterID == "" {
		return fmt.Errorf("invalid cluster")
	}

	cr.mu.Lock()
	defer cr.mu.Unlock()

	// Add to main registry
	cr.clusters[cluster.ClusterID] = cluster

	// Update indexes
	cr.updateIndexes()

	// Persist to Redis for federation
	clusterData, err := json.Marshal(cluster)
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}

	key := fmt.Sprintf("ares:federation:cluster:%s", cluster.ClusterID)
	err = cr.redis.Set(ctx, key, string(clusterData), 24*time.Hour)
	if err != nil {
		cr.log.Warn("Failed to persist cluster to Redis (non-fatal): %v", err)
	}

	// Update federation registry timestamp
	cr.redis.Set(ctx, "ares:federation:last_sync", time.Now().Format(time.RFC3339), 24*time.Hour)

	cr.log.Info("Registered cluster %s in registry", cluster.ClusterID)
	return nil
}

// UnregisterCluster: Unregister cluster from registry
func (cr *ClusterRegistry) UnregisterCluster(ctx context.Context, clusterID string) error {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	if _, exists := cr.clusters[clusterID]; !exists {
		return fmt.Errorf("cluster not found: %s", clusterID)
	}

	// Remove from registry
	delete(cr.clusters, clusterID)

	// Update indexes
	cr.updateIndexes()

	// Remove from Redis
	key := fmt.Sprintf("ares:federation:cluster:%s", clusterID)
	err := cr.redis.Del(ctx, key)
	if err != nil {
		cr.log.Warn("Failed to delete cluster from Redis (non-fatal): %v", err)
	}

	cr.log.Info("Unregistered cluster %s from registry", clusterID)
	return nil
}

// ============================================================================
// REGISTRY QUERIES
// ============================================================================

// GetAllClusters: Get all clusters
func (cr *ClusterRegistry) GetAllClusters() []*Cluster {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	clusters := make([]*Cluster, 0, len(cr.clusters))
	for _, cluster := range cr.clusters {
		clusters = append(clusters, cluster)
	}

	return clusters
}

// GetClustersByRegion: Get clusters in specific region
func (cr *ClusterRegistry) GetClustersByRegion(region string) []*Cluster {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	clusterIDs, exists := cr.regionIndex[region]
	if !exists {
		return []*Cluster{}
	}

	clusters := make([]*Cluster, 0, len(clusterIDs))
	for _, clusterID := range clusterIDs {
		if cluster, ok := cr.clusters[clusterID]; ok {
			clusters = append(clusters, cluster)
		}
	}

	return clusters
}

// GetClustersByState: Get clusters in specific state
func (cr *ClusterRegistry) GetClustersByState(state ClusterState) []*Cluster {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	clusterIDs, exists := cr.stateIndex[state]
	if !exists {
		return []*Cluster{}
	}

	clusters := make([]*Cluster, 0, len(clusterIDs))
	for _, clusterID := range clusterIDs {
		if cluster, ok := cr.clusters[clusterID]; ok {
			clusters = append(clusters, cluster)
		}
	}

	return clusters
}

// ============================================================================
// FEDERATION SYNC (Feature 11 - Eventual Consistency)
// ============================================================================

// SyncWithFederation: Sync registry with Redis (for multi-control-plane setup)
// Call periodically to ensure consistency across multiple control planes
func (cr *ClusterRegistry) SyncWithFederation(ctx context.Context) error {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	// Get all clusters from Redis
	keys, err := cr.redis.Keys(ctx, "ares:federation:cluster:*")
	if err != nil {
		return fmt.Errorf("keys fetch failed: %w", err)
	}

	// Load clusters from Redis
	for _, key := range keys {
		clusterData, err := cr.redis.Get(ctx, key)
		if err != nil {
			cr.log.Warn("Failed to get cluster from Redis: %v", err)
			continue
		}

		var cluster Cluster
		err = json.Unmarshal([]byte(clusterData), &cluster)
		if err != nil {
			cr.log.Warn("Failed to unmarshal cluster: %v", err)
			continue
		}

		// Update local registry
		cr.clusters[cluster.ClusterID] = &cluster
	}

	// Update indexes
	cr.updateIndexes()

	cr.lastSync = time.Now()

	cr.log.Debug("Synced registry with federation (clusters=%d)", len(cr.clusters))
	return nil
}

// ============================================================================
// INDEXES
// ============================================================================

// updateIndexes: Rebuild region and state indexes (lock required)
func (cr *ClusterRegistry) updateIndexes() {
	// Clear indexes
	cr.regionIndex = make(map[string][]string)
	cr.stateIndex = make(map[ClusterState][]string)

	// Rebuild from clusters
	for clusterID, cluster := range cr.clusters {
		// Region index
		cr.regionIndex[cluster.Region] = append(cr.regionIndex[cluster.Region], clusterID)

		// State index
		cr.stateIndex[cluster.State] = append(cr.stateIndex[cluster.State], clusterID)
	}
}

// ============================================================================
// STATISTICS
// ============================================================================

// GetRegistryStats: Get registry statistics
func (cr *ClusterRegistry) GetRegistryStats() map[string]interface{} {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	// Count by region
	regionCounts := make(map[string]int)
	for region, clusterIDs := range cr.regionIndex {
		regionCounts[region] = len(clusterIDs)
	}

	// Count by state
	stateCounts := make(map[string]int)
	for state, clusterIDs := range cr.stateIndex {
		stateCounts[string(state)] = len(clusterIDs)
	}

	return map[string]interface{}{
		"total_clusters": len(cr.clusters),
		"by_region":      regionCounts,
		"by_state":       stateCounts,
		"last_sync":      cr.lastSync,
		"sync_interval":  cr.syncInterval,
	}
}

// GetFederationStatus: Get overall federation status
func (cr *ClusterRegistry) GetFederationStatus() map[string]interface{} {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	totalClusters := len(cr.clusters)
	healthyClusters := 0
	reachableClusters := 0
	regions := make(map[string]int)

	for _, cluster := range cr.clusters {
		if cluster.IsHealthy {
			healthyClusters++
		}
		if cluster.IsReachable {
			reachableClusters++
		}
		regions[cluster.Region]++
	}

	healthPercent := 0.0
	if totalClusters > 0 {
		healthPercent = float64(healthyClusters) / float64(totalClusters) * 100.0
	}

	return map[string]interface{}{
		"total_clusters":      totalClusters,
		"healthy_clusters":    healthyClusters,
		"reachable_clusters":  reachableClusters,
		"health_percentage":   healthPercent,
		"regions":             regions,
		"last_sync":           cr.lastSync,
		"time_since_sync_sec": time.Since(cr.lastSync).Seconds(),
	}
}

// ============================================================================
// FEDERATION VERIFICATION
// ============================================================================

// VerifyFederationConsistency: Check for consistency issues
// Returns: List of inconsistencies found
func (cr *ClusterRegistry) VerifyFederationConsistency(ctx context.Context) []string {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	issues := make([]string, 0)

	// Check 1: No duplicate cluster IDs
	seenIDs := make(map[string]bool)
	for _, cluster := range cr.clusters {
		if seenIDs[cluster.ClusterID] {
			issues = append(issues, fmt.Sprintf("Duplicate cluster ID: %s", cluster.ClusterID))
		}
		seenIDs[cluster.ClusterID] = true
	}

	// Check 2: All clusters have required fields
	for clusterID, cluster := range cr.clusters {
		if cluster.Region == "" {
			issues = append(issues, fmt.Sprintf("Cluster %s missing region", clusterID))
		}
		if cluster.Zone == "" {
			issues = append(issues, fmt.Sprintf("Cluster %s missing zone", clusterID))
		}
		if cluster.ControlAddr == "" {
			issues = append(issues, fmt.Sprintf("Cluster %s missing ControlAddr", clusterID))
		}
	}

	// Check 3: Capacity makes sense
	for clusterID, cluster := range cr.clusters {
		if cluster.GPUsInUse > cluster.TotalGPUs {
			issues = append(issues, fmt.Sprintf("Cluster %s: GPUs in use (%d) > total (%d)",
				clusterID, cluster.GPUsInUse, cluster.TotalGPUs))
		}
		if cluster.MemGBInUse > cluster.TotalMemGB {
			issues = append(issues, fmt.Sprintf("Cluster %s: Memory in use > total", clusterID))
		}
	}

	return issues
}

// RepairFederationConsistency: Attempt to fix consistency issues
func (cr *ClusterRegistry) RepairFederationConsistency(ctx context.Context) error {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	// Fix: Ensure GPUs in use doesn't exceed total
	for _, cluster := range cr.clusters {
		if cluster.GPUsInUse > cluster.TotalGPUs {
			cluster.GPUsInUse = cluster.TotalGPUs
		}
		if cluster.MemGBInUse > cluster.TotalMemGB {
			cluster.MemGBInUse = cluster.TotalMemGB
		}
	}

	cr.log.Info("Repaired federation consistency")
	return nil
}
