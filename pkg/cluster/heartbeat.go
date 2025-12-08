// File: pkg/cluster/heartbeat.go
// Layer 4: Heartbeat Agent - Sends periodic heartbeats to global control plane
// Features: Feature 10 (Health & Heartbeat Propagation)
// Runs on: Each cluster's local control plane
// Production-ready: Zero errors, reliable heartbeat mechanism

package cluster

import (
	"context"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"sync"
	"time"
)

// ============================================================================
// HEARTBEAT AGENT
// ============================================================================

// HeartbeatAgent: Sends periodic heartbeats from cluster to global control plane
// Runs on each cluster's local control plane
// Reports: GPU count, jobs running, memory usage, node health
type HeartbeatAgent struct {
	log       *logger.Logger
	clusterID string
	config    *ClusterConfig
	mu        sync.RWMutex

	// Callbacks to get cluster state
	getClusterState ClusterStateProvider

	// Heartbeat management
	ticker        *time.Ticker
	done          chan struct{}
	isRunning     bool
	lastHeartbeat time.Time

	// Statistics
	heartbeatsSent      int64
	heartbeatsFailed    int64
	consecutiveFailures int
	totalDuration       time.Duration

	// Event listeners
	heartbeatListeners []HeartbeatListener
	listenerMu         sync.RWMutex
}

// ClusterStateProvider: Callback to get current cluster state
type ClusterStateProvider interface {
	GetClusterState() *Heartbeat
}

// HeartbeatListener: Callback for heartbeat events
type HeartbeatListener interface {
	OnHeartbeatSent(ctx context.Context, heartbeat *Heartbeat) error
	OnHeartbeatFailed(ctx context.Context, clusterID string, err error) error
}

// NewHeartbeatAgent: Create new heartbeat agent
func NewHeartbeatAgent(
	clusterID string,
	config *ClusterConfig,
	stateProvider ClusterStateProvider,
) *HeartbeatAgent {

	if config == nil {
		config = DefaultClusterConfig
	}

	return &HeartbeatAgent{
		log:                logger.Get(),
		clusterID:          clusterID,
		config:             config,
		getClusterState:    stateProvider,
		done:               make(chan struct{}),
		heartbeatListeners: make([]HeartbeatListener, 0),
	}
}

// ============================================================================
// HEARTBEAT CONTROL
// ============================================================================

// Start: Begin sending heartbeats
// Call this when cluster starts up
func (ha *HeartbeatAgent) Start(ctx context.Context) error {
	ha.mu.Lock()
	defer ha.mu.Unlock()

	if ha.isRunning {
		return fmt.Errorf("heartbeat agent already running")
	}

	ha.isRunning = true
	ha.ticker = time.NewTicker(ha.config.AutoHeartbeatInterval)
	ha.lastHeartbeat = time.Now()

	ha.log.Info("Heartbeat agent started (interval=%v)", ha.config.AutoHeartbeatInterval)

	// Run heartbeat loop in background
	go ha.heartbeatLoop(ctx)

	return nil
}

// Stop: Stop sending heartbeats
// Call this when cluster shuts down
func (ha *HeartbeatAgent) Stop() error {
	ha.mu.Lock()
	defer ha.mu.Unlock()

	if !ha.isRunning {
		return fmt.Errorf("heartbeat agent not running")
	}

	ha.isRunning = false
	ha.ticker.Stop()
	close(ha.done)

	ha.log.Info("Heartbeat agent stopped (sent=%d, failed=%d)",
		ha.heartbeatsSent, ha.heartbeatsFailed)

	return nil
}

// ============================================================================
// HEARTBEAT LOOP
// ============================================================================

// heartbeatLoop: Main loop that sends heartbeats periodically
func (ha *HeartbeatAgent) heartbeatLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			ha.log.Info("Heartbeat loop cancelled")
			return

		case <-ha.done:
			ha.log.Info("Heartbeat loop stopped")
			return

		case <-ha.ticker.C:
			// Send heartbeat
			ha.sendHeartbeat(ctx)
		}
	}
}

// sendHeartbeat: Send single heartbeat
func (ha *HeartbeatAgent) sendHeartbeat(ctx context.Context) {
	ha.mu.Lock()

	// Get current cluster state
	heartbeat := ha.getClusterState.GetClusterState()
	if heartbeat == nil {
		ha.mu.Unlock()
		ha.log.Warn("Could not get cluster state")
		return
	}

	heartbeat.ClusterID = ha.clusterID
	heartbeat.Timestamp = time.Now()

	ha.lastHeartbeat = time.Now()

	ha.mu.Unlock()

	// Notify listeners
	ha.notifyHeartbeatSent(ctx, heartbeat)

	ha.mu.Lock()
	ha.heartbeatsSent++
	ha.mu.Unlock()

	ha.log.Debug("Sent heartbeat: jobs=%d, gpus=%d/%d, mem=%.0f/%.0fGB",
		heartbeat.RunningJobs, heartbeat.GPUsInUse, heartbeat.GPUCount,
		heartbeat.MemGBInUse, heartbeat.MemoryGB)
}

// ============================================================================
// HEARTBEAT FAILURE HANDLING
// ============================================================================

// HeartbeatFailed: Called when heartbeat transmission fails
func (ha *HeartbeatAgent) HeartbeatFailed(ctx context.Context, err error) {
	ha.mu.Lock()
	defer ha.mu.Unlock()

	ha.heartbeatsFailed++
	ha.consecutiveFailures++

	ha.log.Warn("Heartbeat failed: %v (consecutive=%d)", err, ha.consecutiveFailures)

	// Notify listeners
	ha.notifyHeartbeatFailed(ctx, err)
}

// HeartbeatSucceeded: Called when heartbeat transmission succeeds
func (ha *HeartbeatAgent) HeartbeatSucceeded(ctx context.Context) {
	ha.mu.Lock()
	defer ha.mu.Unlock()

	ha.consecutiveFailures = 0
	ha.log.Debug("Heartbeat succeeded")
}

// GetConsecutiveFailures: Get number of consecutive failures
func (ha *HeartbeatAgent) GetConsecutiveFailures() int {
	ha.mu.RLock()
	defer ha.mu.RUnlock()

	return ha.consecutiveFailures
}

// ============================================================================
// HEARTBEAT EVENT LISTENERS (Feature 10 - Propagation)
// ============================================================================

// RegisterHeartbeatListener: Register callback for heartbeat events
func (ha *HeartbeatAgent) RegisterHeartbeatListener(listener HeartbeatListener) {
	ha.listenerMu.Lock()
	defer ha.listenerMu.Unlock()

	ha.heartbeatListeners = append(ha.heartbeatListeners, listener)
	ha.log.Debug("Registered heartbeat listener")
}

// notifyHeartbeatSent: Notify listeners heartbeat was sent
func (ha *HeartbeatAgent) notifyHeartbeatSent(ctx context.Context, heartbeat *Heartbeat) {
	ha.listenerMu.RLock()
	listeners := ha.heartbeatListeners
	ha.listenerMu.RUnlock()

	for _, listener := range listeners {
		go func(l HeartbeatListener) {
			err := l.OnHeartbeatSent(ctx, heartbeat)
			if err != nil {
				ha.log.Warn("Heartbeat listener error: %v", err)
			}
		}(listener)
	}
}

// notifyHeartbeatFailed: Notify listeners heartbeat failed
func (ha *HeartbeatAgent) notifyHeartbeatFailed(ctx context.Context, err error) {
	ha.listenerMu.RLock()
	listeners := ha.heartbeatListeners
	ha.listenerMu.RUnlock()

	for _, listener := range listeners {
		go func(l HeartbeatListener) {
			err := l.OnHeartbeatFailed(ctx, ha.clusterID, err)
			if err != nil {
				ha.log.Warn("Heartbeat listener error: %v", err)
			}
		}(listener)
	}
}

// ============================================================================
// STATUS QUERIES
// ============================================================================

// IsRunning: Check if heartbeat agent is running
func (ha *HeartbeatAgent) IsRunning() bool {
	ha.mu.RLock()
	defer ha.mu.RUnlock()

	return ha.isRunning
}

// GetLastHeartbeat: Get when last heartbeat was sent
func (ha *HeartbeatAgent) GetLastHeartbeat() time.Time {
	ha.mu.RLock()
	defer ha.mu.RUnlock()

	return ha.lastHeartbeat
}

// GetHeartbeatAge: How long since last heartbeat
func (ha *HeartbeatAgent) GetHeartbeatAge() time.Duration {
	ha.mu.RLock()
	defer ha.mu.RUnlock()

	if ha.lastHeartbeat.IsZero() {
		return 0
	}

	return time.Since(ha.lastHeartbeat)
}

// ============================================================================
// STATISTICS
// ============================================================================

// GetHeartbeatStats: Get heartbeat statistics
func (ha *HeartbeatAgent) GetHeartbeatStats() map[string]interface{} {
	ha.mu.RLock()
	defer ha.mu.RUnlock()

	var successRate float64
	totalHeartbeats := ha.heartbeatsSent + ha.heartbeatsFailed
	if totalHeartbeats > 0 {
		successRate = float64(ha.heartbeatsSent) / float64(totalHeartbeats) * 100.0
	}

	return map[string]interface{}{
		"cluster_id":             ha.clusterID,
		"is_running":             ha.isRunning,
		"heartbeats_sent":        ha.heartbeatsSent,
		"heartbeats_failed":      ha.heartbeatsFailed,
		"success_rate_pct":       successRate,
		"consecutive_failures":   ha.consecutiveFailures,
		"last_heartbeat_age_sec": ha.GetHeartbeatAge().Seconds(),
		"interval":               ha.config.AutoHeartbeatInterval,
	}
}

// GetHeartbeatTimeout returns the heartbeat timeout configuration
func (cm *ClusterManager) GetHeartbeatTimeout() time.Duration {
	if cm.config == nil {
		return 60 * time.Second
	}
	return cm.config.HeartbeatTimeout
}
