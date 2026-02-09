// File: pkg/crdt/sync.go
// CRDT Sync Protocol
//
// Handles periodic state exchange between control plane peers.
// Uses anti-entropy gossip: each node periodically sends its full state
// to a random peer. Peers merge and converge.
//
// Sync modes:
//   1. Push: Send my state to a random peer (default)
//   2. Pull: Request state from a random peer
//   3. Push-Pull: Exchange state bidirectionally (most efficient)
//
// Convergence: With N nodes doing push-pull every T seconds,
// all nodes converge in O(log N) rounds, or O(T * log N) seconds.
//
// For 3 control planes syncing every 5 seconds:
//   Convergence time ≈ 5 * log2(3) ≈ 8 seconds

package crdt

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
)

// ============================================================================
// SYNC MANAGER
// ============================================================================

// SyncManager: Coordinates CRDT state synchronization between control planes
type SyncManager struct {
	mu     sync.RWMutex
	log    *logger.Logger
	config *SyncConfig

	// Local state store
	store *CRDTStateStore

	// Peer control planes
	peers []PeerInfo

	// Sync metrics
	syncRoundCount uint64
	lastSyncRound  time.Time
	failedSyncs    uint64
	totalBytesSent uint64
	totalBytesRecv uint64

	// HTTP client for peer communication
	httpClient *http.Client
}

// SyncConfig: Configuration for state sync
type SyncConfig struct {
	SyncInterval   time.Duration // How often to sync (default: 5s)
	SyncMode       string        // "push", "pull", "push-pull" (default: "push-pull")
	MaxSyncPayload int           // Max bytes per sync message (default: 10MB)
	SyncTimeout    time.Duration // HTTP timeout for sync requests (default: 10s)
	FanOut         int           // Number of peers to sync with per round (default: 1)
}

// DefaultSyncConfig: Sensible defaults
var DefaultSyncConfig = &SyncConfig{
	SyncInterval:   5 * time.Second,
	SyncMode:       "push-pull",
	MaxSyncPayload: 10 * 1024 * 1024, // 10MB
	SyncTimeout:    10 * time.Second,
	FanOut:         1,
}

// PeerInfo: Information about a peer control plane
type PeerInfo struct {
	NodeID   string    `json:"node_id"`
	Address  string    `json:"address"` // HTTP address: "http://cp-us-west:8080"
	Region   string    `json:"region"`
	Healthy  bool      `json:"healthy"`
	LastSeen time.Time `json:"last_seen"`
}

// ============================================================================
// CONSTRUCTOR
// ============================================================================

func NewSyncManager(store *CRDTStateStore, config *SyncConfig) *SyncManager {
	if config == nil {
		config = DefaultSyncConfig
	}

	return &SyncManager{
		log:    logger.Get(),
		config: config,
		store:  store,
		peers:  make([]PeerInfo, 0),
		httpClient: &http.Client{
			Timeout: config.SyncTimeout,
		},
	}
}

// ============================================================================
// PEER MANAGEMENT
// ============================================================================

// AddPeer: Register a peer control plane
func (sm *SyncManager) AddPeer(nodeID, address, region string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Don't add self
	if nodeID == sm.store.nodeID {
		return
	}

	// Check if peer already exists
	for i, p := range sm.peers {
		if p.NodeID == nodeID {
			sm.peers[i].Address = address
			sm.peers[i].Region = region
			sm.peers[i].Healthy = true
			sm.peers[i].LastSeen = time.Now()
			return
		}
	}

	sm.peers = append(sm.peers, PeerInfo{
		NodeID:   nodeID,
		Address:  address,
		Region:   region,
		Healthy:  true,
		LastSeen: time.Now(),
	})

	sm.log.Info("CRDT SYNC: Added peer %s at %s (region=%s)", nodeID, address, region)
}

// RemovePeer: Remove a peer control plane
func (sm *SyncManager) RemovePeer(nodeID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for i, p := range sm.peers {
		if p.NodeID == nodeID {
			sm.peers = append(sm.peers[:i], sm.peers[i+1:]...)
			sm.log.Info("CRDT SYNC: Removed peer %s", nodeID)
			return
		}
	}
}

// GetPeers: List all known peers
func (sm *SyncManager) GetPeers() []PeerInfo {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	result := make([]PeerInfo, len(sm.peers))
	copy(result, sm.peers)
	return result
}

// ============================================================================
// SYNC LOOP
// ============================================================================

// StartSyncLoop: Begin periodic synchronization with peers
// Runs in background goroutine until context is cancelled
func (sm *SyncManager) StartSyncLoop(ctx context.Context) {
	sm.log.Info("CRDT SYNC: Starting sync loop (interval=%s, mode=%s, fanout=%d)",
		sm.config.SyncInterval, sm.config.SyncMode, sm.config.FanOut)

	ticker := time.NewTicker(sm.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			sm.log.Info("CRDT SYNC: Sync loop stopped")
			return
		case <-ticker.C:
			sm.doSyncRound(ctx)
		}
	}
}

// doSyncRound: Execute one round of synchronization
func (sm *SyncManager) doSyncRound(ctx context.Context) {
	sm.mu.RLock()
	if len(sm.peers) == 0 {
		sm.mu.RUnlock()
		return
	}

	// Select random peers (fan-out)
	targets := sm.selectRandomPeers(sm.config.FanOut)
	sm.mu.RUnlock()

	for _, peer := range targets {
		switch sm.config.SyncMode {
		case "push":
			sm.pushState(ctx, peer)
		case "pull":
			sm.pullState(ctx, peer)
		case "push-pull":
			sm.pushPullState(ctx, peer)
		}
	}

	sm.syncRoundCount++
	sm.lastSyncRound = time.Now()
}

// selectRandomPeers: Pick N random healthy peers
func (sm *SyncManager) selectRandomPeers(n int) []PeerInfo {
	healthy := make([]PeerInfo, 0)
	for _, p := range sm.peers {
		if p.Healthy {
			healthy = append(healthy, p)
		}
	}

	if len(healthy) == 0 {
		return nil
	}

	if n >= len(healthy) {
		return healthy
	}

	// Fisher-Yates shuffle, take first N
	shuffled := make([]PeerInfo, len(healthy))
	copy(shuffled, healthy)
	for i := len(shuffled) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}

	return shuffled[:n]
}

// ============================================================================
// SYNC OPERATIONS
// ============================================================================

// pushState: Send local state to a peer
func (sm *SyncManager) pushState(ctx context.Context, peer PeerInfo) {
	data, err := sm.store.Serialize()
	if err != nil {
		sm.log.Error("CRDT SYNC: Failed to serialize state: %v", err)
		return
	}

	url := fmt.Sprintf("%s/crdt/sync", peer.Address)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(data))
	if err != nil {
		sm.log.Error("CRDT SYNC: Failed to create request to %s: %v", peer.NodeID, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-CRDT-Node-ID", sm.store.nodeID)

	resp, err := sm.httpClient.Do(req)
	if err != nil {
		sm.log.Warn("CRDT SYNC: Push to %s failed: %v", peer.NodeID, err)
		sm.markPeerUnhealthy(peer.NodeID)
		sm.failedSyncs++
		return
	}
	defer resp.Body.Close()

	sm.totalBytesSent += uint64(len(data))
	sm.markPeerHealthy(peer.NodeID)

	sm.log.Debug("CRDT SYNC: Pushed %d bytes to %s (status=%d)",
		len(data), peer.NodeID, resp.StatusCode)
}

// pullState: Request state from a peer and merge
func (sm *SyncManager) pullState(ctx context.Context, peer PeerInfo) {
	url := fmt.Sprintf("%s/crdt/state", peer.Address)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		sm.log.Error("CRDT SYNC: Failed to create pull request to %s: %v", peer.NodeID, err)
		return
	}
	req.Header.Set("X-CRDT-Node-ID", sm.store.nodeID)

	resp, err := sm.httpClient.Do(req)
	if err != nil {
		sm.log.Warn("CRDT SYNC: Pull from %s failed: %v", peer.NodeID, err)
		sm.markPeerUnhealthy(peer.NodeID)
		sm.failedSyncs++
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		sm.log.Error("CRDT SYNC: Failed to read response from %s: %v", peer.NodeID, err)
		return
	}

	sm.totalBytesRecv += uint64(len(body))

	remoteStore, err := DeserializeCRDTStateStore(body)
	if err != nil {
		sm.log.Error("CRDT SYNC: Failed to deserialize state from %s: %v", peer.NodeID, err)
		return
	}

	result := sm.store.MergeRemoteState(remoteStore)
	sm.markPeerHealthy(peer.NodeID)

	sm.log.Debug("CRDT SYNC: Pulled from %s: %s", peer.NodeID, result.Reason)
}

// pushPullState: Exchange state bidirectionally (most efficient)
// 1. Push local state to peer
// 2. Peer merges and responds with its (now-updated) state
// 3. We merge the response
// Result: Both nodes converge in one round-trip
func (sm *SyncManager) pushPullState(ctx context.Context, peer PeerInfo) {
	data, err := sm.store.Serialize()
	if err != nil {
		sm.log.Error("CRDT SYNC: Failed to serialize state: %v", err)
		return
	}

	url := fmt.Sprintf("%s/crdt/sync-exchange", peer.Address)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(data))
	if err != nil {
		sm.log.Error("CRDT SYNC: Failed to create exchange request to %s: %v", peer.NodeID, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-CRDT-Node-ID", sm.store.nodeID)

	resp, err := sm.httpClient.Do(req)
	if err != nil {
		sm.log.Warn("CRDT SYNC: Exchange with %s failed: %v", peer.NodeID, err)
		sm.markPeerUnhealthy(peer.NodeID)
		sm.failedSyncs++
		return
	}
	defer resp.Body.Close()

	sm.totalBytesSent += uint64(len(data))

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		sm.log.Error("CRDT SYNC: Failed to read exchange response from %s: %v", peer.NodeID, err)
		return
	}

	sm.totalBytesRecv += uint64(len(body))

	remoteStore, err := DeserializeCRDTStateStore(body)
	if err != nil {
		sm.log.Error("CRDT SYNC: Failed to deserialize exchange response from %s: %v", peer.NodeID, err)
		return
	}

	result := sm.store.MergeRemoteState(remoteStore)
	sm.markPeerHealthy(peer.NodeID)

	sm.log.Debug("CRDT SYNC: Exchange with %s: %s", peer.NodeID, result.Reason)
}

// ============================================================================
// HTTP HANDLERS (for receiving sync from peers)
// ============================================================================

// HandleSyncPush: Handle incoming state push from a peer
// Endpoint: POST /crdt/sync
func (sm *SyncManager) HandleSyncPush(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read failed", http.StatusBadRequest)
		return
	}

	remoteStore, err := DeserializeCRDTStateStore(body)
	if err != nil {
		http.Error(w, "deserialize failed", http.StatusBadRequest)
		return
	}

	result := sm.store.MergeRemoteState(remoteStore)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"merged":             result.Success,
		"clusters_updated":   result.ClustersUpdated,
		"jobs_updated":       result.JobsUpdated,
		"conflicts_resolved": result.ConflictsResolved,
	})
}

// HandleStateRequest: Handle state pull request from a peer
// Endpoint: GET /crdt/state
func (sm *SyncManager) HandleStateRequest(w http.ResponseWriter, r *http.Request) {
	data, err := sm.store.Serialize()
	if err != nil {
		http.Error(w, "serialize failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// HandleSyncExchange: Handle push-pull exchange from a peer
// Endpoint: POST /crdt/sync-exchange
// 1. Receive peer's state
// 2. Merge into our state
// 3. Respond with our (now-updated) state
func (sm *SyncManager) HandleSyncExchange(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read failed", http.StatusBadRequest)
		return
	}

	remoteStore, err := DeserializeCRDTStateStore(body)
	if err != nil {
		http.Error(w, "deserialize failed", http.StatusBadRequest)
		return
	}

	// Merge incoming state
	sm.store.MergeRemoteState(remoteStore)

	// Respond with our updated state
	data, err := sm.store.Serialize()
	if err != nil {
		http.Error(w, "serialize failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// ============================================================================
// PEER HEALTH TRACKING
// ============================================================================

func (sm *SyncManager) markPeerHealthy(nodeID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for i, p := range sm.peers {
		if p.NodeID == nodeID {
			sm.peers[i].Healthy = true
			sm.peers[i].LastSeen = time.Now()
			return
		}
	}
}

func (sm *SyncManager) markPeerUnhealthy(nodeID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for i, p := range sm.peers {
		if p.NodeID == nodeID {
			sm.peers[i].Healthy = false
			return
		}
	}
}

// ============================================================================
// SYNC STATISTICS
// ============================================================================

// GetSyncStats: Sync protocol statistics
func (sm *SyncManager) GetSyncStats() map[string]interface{} {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	healthyPeers := 0
	for _, p := range sm.peers {
		if p.Healthy {
			healthyPeers++
		}
	}

	return map[string]interface{}{
		"node_id":          sm.store.nodeID,
		"sync_mode":        sm.config.SyncMode,
		"sync_interval":    sm.config.SyncInterval.String(),
		"peer_count":       len(sm.peers),
		"healthy_peers":    healthyPeers,
		"sync_rounds":      sm.syncRoundCount,
		"last_sync_round":  sm.lastSyncRound,
		"failed_syncs":     sm.failedSyncs,
		"total_bytes_sent": sm.totalBytesSent,
		"total_bytes_recv": sm.totalBytesRecv,
		"store_stats":      sm.store.GetSyncStats(),
	}
}
