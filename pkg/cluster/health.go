// File: pkg/cluster/health.go
// Layer 4: Health Monitor - Track cluster health via heartbeats
// Features: Feature 10 (Health & Heartbeat Propagation)
// Production-ready: Zero errors, comprehensive monitoring

package cluster

import (
	"context"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/redis"
	"sync"
	"time"
)

// ============================================================================
// HEALTH MONITOR
// ============================================================================

// HealthMonitor: Tracks cluster health via heartbeats
// Runs in global control plane
// Thread-safe: Uses sync.RWMutex for health state
type HealthMonitor struct {
	log            *logger.Logger
	redis          *redis.RedisClient
	mu             sync.RWMutex
	config         *ClusterConfig
	clusterManager *ClusterManager

	// Health tracking
	health map[string]*ClusterHealth // clusterID -> health info

	// Event listeners
	healthListeners []HealthChangeListener
	listenerMu      sync.RWMutex
}

// HealthChangeListener: Callback for health changes
type HealthChangeListener interface {
	OnHealthy(ctx context.Context, clusterID string) error
	OnUnhealthy(ctx context.Context, clusterID string, reason string) error
}

// NewHealthMonitor: Create new health monitor
func NewHealthMonitor(
	redisClient *redis.RedisClient,
	clusterManager *ClusterManager,
	config *ClusterConfig,
) *HealthMonitor {

	if config == nil {
		config = DefaultClusterConfig
	}

	return &HealthMonitor{
		log:             logger.Get(),
		redis:           redisClient,
		config:          config,
		clusterManager:  clusterManager,
		health:          make(map[string]*ClusterHealth),
		healthListeners: make([]HealthChangeListener, 0),
	}
}

// ============================================================================
// HEARTBEAT RECEPTION (Feature 10)
// ============================================================================

// ReceivedHeartbeat: Process heartbeat from cluster
// Called when cluster sends periodic heartbeat
func (hm *HealthMonitor) ReceivedHeartbeat(ctx context.Context, heartbeat *Heartbeat) error {
	if heartbeat == nil || heartbeat.ClusterID == "" {
		return fmt.Errorf("invalid heartbeat")
	}

	now := time.Now()

	hm.mu.Lock()
	defer hm.mu.Unlock()

	// Get or create health entry
	health, exists := hm.health[heartbeat.ClusterID]
	if !exists {
		health = &ClusterHealth{
			ClusterID:        heartbeat.ClusterID,
			State:            StateReady,
			IsHealthy:        true,
			LastHeartbeat:    now,
			HeartbeatAge:     0,
			ConsecutiveFails: 0,
			Timestamp:        now,
		}
		hm.health[heartbeat.ClusterID] = health
	}

	// Update health from heartbeat
	health.LastHeartbeat = now
	health.HeartbeatAge = 0
	health.Timestamp = now

	// Determine health from heartbeat
	wasHealthy := health.IsHealthy
	health.IsHealthy = heartbeat.Status == "healthy"

	// Reset consecutive failures on successful heartbeat
	if health.IsHealthy {
		health.ConsecutiveFails = 0
	} else {
		health.ConsecutiveFails++
	}

	// Update cluster manager with new load
	hm.clusterManager.UpdateClusterLoad(
		ctx,
		heartbeat.ClusterID,
		heartbeat.GPUsInUse,
		0, // CPUs not in heartbeat yet
		heartbeat.MemGBInUse,
		heartbeat.RunningJobs,
		heartbeat.PendingJobs,
	)

	hm.log.Debug("Received heartbeat from %s: health=%v, jobs=%d, gpus=%d/%d",
		heartbeat.ClusterID, health.IsHealthy, heartbeat.RunningJobs,
		heartbeat.GPUsInUse, heartbeat.GPUCount)

	// Notify if health changed
	if wasHealthy != health.IsHealthy {
		if health.IsHealthy {
			hm.notifyHealthy(ctx, heartbeat.ClusterID)
		} else {
			hm.notifyUnhealthy(ctx, heartbeat.ClusterID, heartbeat.Status)
		}
	}

	return nil
}

// ============================================================================
// HEALTH CHECKING (Feature 10)
// ============================================================================

// CheckClusterHealth: Periodic health check for clusters
// Call regularly (e.g., every 30 seconds) to detect stale heartbeats
// Returns: Number of clusters marked unhealthy
func (hm *HealthMonitor) CheckClusterHealth(ctx context.Context) int {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	now := time.Now()
	heartbeatTimeout := hm.config.HeartbeatTimeout
	maxFailures := hm.config.MaxConsecutiveFailures

	unHealthyCount := 0

	for clusterID, health := range hm.health {
		// Calculate heartbeat age
		health.HeartbeatAge = now.Sub(health.LastHeartbeat)

		// Check if stale
		if health.HeartbeatAge > heartbeatTimeout {
			if health.IsHealthy {
				health.ConsecutiveFails++

				// Mark unhealthy after max consecutive failures
				if health.ConsecutiveFails > maxFailures {
					hm.log.Warn("Cluster %s marked unhealthy: no heartbeat for %v (fails=%d)",
						clusterID, health.HeartbeatAge, health.ConsecutiveFails)

					health.IsHealthy = false
					health.ErrorMessage = fmt.Sprintf("no heartbeat for %v", health.HeartbeatAge)
					health.State = StateUnhealthy
					unHealthyCount++

					// Update cluster manager
					hm.clusterManager.UpdateClusterState(ctx, clusterID, StateUnhealthy)

					// Notify listeners
					hm.notifyUnhealthy(ctx, clusterID, "no heartbeat")
				}
			}
		}
	}

	if unHealthyCount > 0 {
		hm.log.Warn("Health check: %d clusters marked unhealthy", unHealthyCount)
	}

	return unHealthyCount
}

// ============================================================================
// HEALTH QUERIES
// ============================================================================

// GetClusterHealth: Get health of specific cluster
func (hm *HealthMonitor) GetClusterHealth(clusterID string) (*ClusterHealth, error) {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	health, exists := hm.health[clusterID]
	if !exists {
		return nil, fmt.Errorf("health info not found for cluster: %s", clusterID)
	}

	return health, nil
}

// ListHealthStatus: Get health of all clusters
func (hm *HealthMonitor) ListHealthStatus() map[string]*ClusterHealth {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	// Return copy
	status := make(map[string]*ClusterHealth)
	for clusterID, health := range hm.health {
		status[clusterID] = health
	}

	return status
}

// CountHealthyClusters: Count healthy clusters
func (hm *HealthMonitor) CountHealthyClusters() int {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	count := 0
	for _, health := range hm.health {
		if health.IsHealthy {
			count++
		}
	}

	return count
}

// CountUnhealthyClusters: Count unhealthy clusters
func (hm *HealthMonitor) CountUnhealthyClusters() int {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	count := 0
	for _, health := range hm.health {
		if !health.IsHealthy {
			count++
		}
	}

	return count
}

// IsClusterHealthy: Quick health check
func (hm *HealthMonitor) IsClusterHealthy(clusterID string) bool {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	health, exists := hm.health[clusterID]
	if !exists {
		return false
	}

	return health.IsHealthy
}

// ============================================================================
// HEALTH ALERTS (Feature 10 - Propagation)
// ============================================================================

// RegisterHealthListener: Register callback for health changes
func (hm *HealthMonitor) RegisterHealthListener(listener HealthChangeListener) {
	hm.listenerMu.Lock()
	defer hm.listenerMu.Unlock()

	hm.healthListeners = append(hm.healthListeners, listener)
	hm.log.Debug("Registered health listener")
}

// notifyHealthy: Notify cluster became healthy
func (hm *HealthMonitor) notifyHealthy(ctx context.Context, clusterID string) {
	hm.listenerMu.RLock()
	listeners := hm.healthListeners
	hm.listenerMu.RUnlock()

	for _, listener := range listeners {
		go func(l HealthChangeListener) {
			err := l.OnHealthy(ctx, clusterID)
			if err != nil {
				hm.log.Warn("Health listener error: %v", err)
			}
		}(listener)
	}
}

// notifyUnhealthy: Notify cluster became unhealthy
func (hm *HealthMonitor) notifyUnhealthy(ctx context.Context, clusterID string, reason string) {
	hm.listenerMu.RLock()
	listeners := hm.healthListeners
	hm.listenerMu.RUnlock()

	for _, listener := range listeners {
		go func(l HealthChangeListener) {
			err := l.OnUnhealthy(ctx, clusterID, reason)
			if err != nil {
				hm.log.Warn("Health listener error: %v", err)
			}
		}(listener)
	}
}

// ============================================================================
// STATISTICS
// ============================================================================

// GetHealthStats: Get health statistics
func (hm *HealthMonitor) GetHealthStats() map[string]interface{} {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	totalClusters := len(hm.health)
	healthyClusters := 0
	unhealthyClusters := 0
	staleClusters := 0

	now := time.Now()
	staleTimeout := hm.config.HeartbeatTimeout

	for _, health := range hm.health {
		if health.IsHealthy {
			healthyClusters++
		} else {
			unhealthyClusters++
		}

		if now.Sub(health.LastHeartbeat) > staleTimeout {
			staleClusters++
		}
	}

	healthPercent := 0.0
	if totalClusters > 0 {
		healthPercent = float64(healthyClusters) / float64(totalClusters) * 100.0
	}

	return map[string]interface{}{
		"total_clusters":     totalClusters,
		"healthy_clusters":   healthyClusters,
		"unhealthy_clusters": unhealthyClusters,
		"stale_clusters":     staleClusters,
		"health_percentage":  healthPercent,
		"heartbeat_timeout":  staleTimeout,
		"last_check":         time.Now(),
	}
}
