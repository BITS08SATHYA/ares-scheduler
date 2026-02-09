// File: pkg/crdt/state.go
// Composed CRDT state types for Ares
//
// These wrap cluster and job state in CRDT primitives so that
// multiple control planes can update independently and merge
// without conflicts.

package crdt

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// ============================================================================
// CRDT CLUSTER STATE
// ============================================================================

// CRDTClusterState: Cluster info wrapped in CRDTs for conflict-free replication.
// Each mutable field is an LWW register; cluster membership uses OR-Set.
//
// Immutable fields (ClusterID, Region, Zone) don't need CRDTs.
// Mutable fields (IsHealthy, AvailableGPUs, etc.) use LWW registers.
type CRDTClusterState struct {
	mu sync.RWMutex

	// Immutable identity
	ClusterID string `json:"cluster_id"`
	Region    string `json:"region"`
	Zone      string `json:"zone"`

	// Mutable state (LWW registers)
	IsHealthy       *LWWRegister `json:"is_healthy"`
	AvailableGPUs   *LWWRegister `json:"available_gpus"`
	TotalGPUs       *LWWRegister `json:"total_gpus"`
	AvailableMemGB  *LWWRegister `json:"available_mem_gb"`
	TotalMemGB      *LWWRegister `json:"total_mem_gb"`
	RunningJobCount *LWWRegister `json:"running_job_count"`
	LastHeartbeat   *LWWRegister `json:"last_heartbeat"`
	SchedulerAddr   *LWWRegister `json:"scheduler_addr"`

	// Causal context
	Clock     *VectorClock `json:"clock"`
	UpdatedAt time.Time    `json:"updated_at"`
}

// NewCRDTClusterState: Create a new CRDT-wrapped cluster state
func NewCRDTClusterState(nodeID string, clusterID string, region string, zone string) *CRDTClusterState {
	return &CRDTClusterState{
		ClusterID:       clusterID,
		Region:          region,
		Zone:            zone,
		IsHealthy:       NewLWWRegister(nodeID, true),
		AvailableGPUs:   NewLWWRegister(nodeID, 0),
		TotalGPUs:       NewLWWRegister(nodeID, 0),
		AvailableMemGB:  NewLWWRegister(nodeID, 0.0),
		TotalMemGB:      NewLWWRegister(nodeID, 0.0),
		RunningJobCount: NewLWWRegister(nodeID, 0),
		LastHeartbeat:   NewLWWRegister(nodeID, time.Now()),
		SchedulerAddr:   NewLWWRegister(nodeID, ""),
		Clock:           NewVectorClock(),
		UpdatedAt:       time.Now(),
	}
}

// UpdateLoad: Update cluster load metrics (called on heartbeat)
func (cs *CRDTClusterState) UpdateLoad(nodeID string, gpusInUse int, memGBInUse float64, runningJobs int) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.AvailableGPUs.Set(nodeID, gpusInUse)
	cs.AvailableMemGB.Set(nodeID, memGBInUse)
	cs.RunningJobCount.Set(nodeID, runningJobs)
	cs.LastHeartbeat.Set(nodeID, time.Now())
	cs.Clock.Increment(nodeID)
	cs.UpdatedAt = time.Now()
}

// SetHealth: Update cluster health status
func (cs *CRDTClusterState) SetHealth(nodeID string, healthy bool) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.IsHealthy.Set(nodeID, healthy)
	cs.Clock.Increment(nodeID)
	cs.UpdatedAt = time.Now()
}

// Merge: Merge with a remote cluster state (conflict-free)
// Each LWW register resolves independently by timestamp
func (cs *CRDTClusterState) Merge(remote *CRDTClusterState) *MergeResult {
	if remote == nil || remote.ClusterID != cs.ClusterID {
		return &MergeResult{Merged: false, Reason: "nil or mismatched cluster ID"}
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	result := &MergeResult{
		Merged:    true,
		Conflicts: make([]string, 0),
	}

	// Merge each LWW register independently
	if cs.IsHealthy.Merge(remote.IsHealthy) {
		result.Conflicts = append(result.Conflicts, "is_healthy")
	}
	if cs.AvailableGPUs.Merge(remote.AvailableGPUs) {
		result.Conflicts = append(result.Conflicts, "available_gpus")
	}
	if cs.TotalGPUs.Merge(remote.TotalGPUs) {
		result.Conflicts = append(result.Conflicts, "total_gpus")
	}
	if cs.AvailableMemGB.Merge(remote.AvailableMemGB) {
		result.Conflicts = append(result.Conflicts, "available_mem_gb")
	}
	if cs.TotalMemGB.Merge(remote.TotalMemGB) {
		result.Conflicts = append(result.Conflicts, "total_mem_gb")
	}
	if cs.RunningJobCount.Merge(remote.RunningJobCount) {
		result.Conflicts = append(result.Conflicts, "running_job_count")
	}
	if cs.LastHeartbeat.Merge(remote.LastHeartbeat) {
		result.Conflicts = append(result.Conflicts, "last_heartbeat")
	}
	if cs.SchedulerAddr.Merge(remote.SchedulerAddr) {
		result.Conflicts = append(result.Conflicts, "scheduler_addr")
	}

	// Merge vector clocks
	cs.Clock.Merge(remote.Clock)

	if remote.UpdatedAt.After(cs.UpdatedAt) {
		cs.UpdatedAt = remote.UpdatedAt
	}

	result.FieldsUpdated = len(result.Conflicts)
	result.Reason = fmt.Sprintf("merged %d fields", result.FieldsUpdated)

	return result
}

// Serialize: JSON-encode for network transmission
func (cs *CRDTClusterState) Serialize() ([]byte, error) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return json.Marshal(cs)
}

// ============================================================================
// CRDT JOB STATE
// ============================================================================

// CRDTJobState: Job record wrapped in CRDTs for conflict-free replication.
// Critical for ensuring job status doesn't get lost when two control planes
// process the same job concurrently.
type CRDTJobState struct {
	mu sync.RWMutex

	// Immutable identity
	JobID string `json:"job_id"`

	// Mutable state (LWW registers)
	Status    *LWWRegister `json:"status"`
	ClusterID *LWWRegister `json:"cluster_id"`
	NodeID    *LWWRegister `json:"node_id"`
	PodName   *LWWRegister `json:"pod_name"`
	Attempts  *LWWRegister `json:"attempts"`
	ErrorMsg  *LWWRegister `json:"error_msg"`
	ExitCode  *LWWRegister `json:"exit_code"`

	// Checkpoint state
	LastCheckpointPath *LWWRegister `json:"last_checkpoint_path"`
	LastCheckpointMeta *LWWRegister `json:"last_checkpoint_meta"`

	// Causal context
	Clock     *VectorClock `json:"clock"`
	UpdatedAt time.Time    `json:"updated_at"`
}

// NewCRDTJobState: Create a new CRDT-wrapped job state
func NewCRDTJobState(nodeID string, jobID string) *CRDTJobState {
	return &CRDTJobState{
		JobID:              jobID,
		Status:             NewLWWRegister(nodeID, "PENDING"),
		ClusterID:          NewLWWRegister(nodeID, ""),
		NodeID:             NewLWWRegister(nodeID, ""),
		PodName:            NewLWWRegister(nodeID, ""),
		Attempts:           NewLWWRegister(nodeID, 0),
		ErrorMsg:           NewLWWRegister(nodeID, ""),
		ExitCode:           NewLWWRegister(nodeID, 0),
		LastCheckpointPath: NewLWWRegister(nodeID, ""),
		LastCheckpointMeta: NewLWWRegister(nodeID, ""),
		Clock:              NewVectorClock(),
		UpdatedAt:          time.Now(),
	}
}

// UpdateStatus: Update job status with causal tracking
func (js *CRDTJobState) UpdateStatus(nodeID string, status string) {
	js.mu.Lock()
	defer js.mu.Unlock()

	js.Status.Set(nodeID, status)
	js.Clock.Increment(nodeID)
	js.UpdatedAt = time.Now()
}

// UpdatePlacement: Update job placement info
func (js *CRDTJobState) UpdatePlacement(nodeID string, clusterID string, node string, podName string) {
	js.mu.Lock()
	defer js.mu.Unlock()

	js.ClusterID.Set(nodeID, clusterID)
	js.NodeID.Set(nodeID, node)
	js.PodName.Set(nodeID, podName)
	js.Clock.Increment(nodeID)
	js.UpdatedAt = time.Now()
}

// UpdateCheckpoint: Update checkpoint info
func (js *CRDTJobState) UpdateCheckpoint(nodeID string, path string, meta string) {
	js.mu.Lock()
	defer js.mu.Unlock()

	js.LastCheckpointPath.Set(nodeID, path)
	js.LastCheckpointMeta.Set(nodeID, meta)
	js.Clock.Increment(nodeID)
	js.UpdatedAt = time.Now()
}

// Merge: Merge with remote job state
func (js *CRDTJobState) Merge(remote *CRDTJobState) *MergeResult {
	if remote == nil || remote.JobID != js.JobID {
		return &MergeResult{Merged: false, Reason: "nil or mismatched job ID"}
	}

	js.mu.Lock()
	defer js.mu.Unlock()

	result := &MergeResult{
		Merged:    true,
		Conflicts: make([]string, 0),
	}

	if js.Status.Merge(remote.Status) {
		result.Conflicts = append(result.Conflicts, "status")
	}
	if js.ClusterID.Merge(remote.ClusterID) {
		result.Conflicts = append(result.Conflicts, "cluster_id")
	}
	if js.NodeID.Merge(remote.NodeID) {
		result.Conflicts = append(result.Conflicts, "node_id")
	}
	if js.PodName.Merge(remote.PodName) {
		result.Conflicts = append(result.Conflicts, "pod_name")
	}
	if js.Attempts.Merge(remote.Attempts) {
		result.Conflicts = append(result.Conflicts, "attempts")
	}
	if js.ErrorMsg.Merge(remote.ErrorMsg) {
		result.Conflicts = append(result.Conflicts, "error_msg")
	}
	if js.ExitCode.Merge(remote.ExitCode) {
		result.Conflicts = append(result.Conflicts, "exit_code")
	}
	if js.LastCheckpointPath.Merge(remote.LastCheckpointPath) {
		result.Conflicts = append(result.Conflicts, "checkpoint_path")
	}
	if js.LastCheckpointMeta.Merge(remote.LastCheckpointMeta) {
		result.Conflicts = append(result.Conflicts, "checkpoint_meta")
	}

	js.Clock.Merge(remote.Clock)

	if remote.UpdatedAt.After(js.UpdatedAt) {
		js.UpdatedAt = remote.UpdatedAt
	}

	result.FieldsUpdated = len(result.Conflicts)
	result.Reason = fmt.Sprintf("merged %d fields", result.FieldsUpdated)

	return result
}

// Serialize: JSON-encode for network transmission
func (js *CRDTJobState) Serialize() ([]byte, error) {
	js.mu.RLock()
	defer js.mu.RUnlock()
	return json.Marshal(js)
}

// ============================================================================
// MERGE RESULT
// ============================================================================

// MergeResult: Outcome of a CRDT merge operation
type MergeResult struct {
	Merged        bool     // Did the merge succeed?
	FieldsUpdated int      // How many fields changed
	Conflicts     []string // Which fields had conflicting values (resolved by LWW)
	Reason        string   // Human-readable description
}

// ============================================================================
// CRDT STATE STORE
// ============================================================================

// CRDTStateStore: Manages all CRDT state for a control plane node.
// Each control plane has one CRDTStateStore that tracks:
//   - All known cluster states (OR-Set of cluster IDs + LWW state per cluster)
//   - All known job states (OR-Set of job IDs + LWW state per job)
//
// Sync protocol:
//  1. Periodically serialize local state
//  2. Send to peer control planes via gossip/push
//  3. Peers merge incoming state with their local state
//  4. All replicas converge to the same state
type CRDTStateStore struct {
	mu     sync.RWMutex
	nodeID string // This control plane's ID

	// Cluster state
	ActiveClusters *ORSet                       `json:"active_clusters"`
	ClusterStates  map[string]*CRDTClusterState `json:"cluster_states"`

	// Job state
	ActiveJobs *ORSet                   `json:"active_jobs"`
	JobStates  map[string]*CRDTJobState `json:"job_states"`

	// Completed jobs (grow-only, never removed)
	CompletedJobs *GSet `json:"completed_jobs"`

	// Sync metadata
	Clock        *VectorClock `json:"clock"`
	LastSyncTime time.Time    `json:"last_sync_time"`
	SyncCount    uint64       `json:"sync_count"`

	// Metrics
	TotalMerges    uint64 `json:"total_merges"`
	ConflictsFound uint64 `json:"conflicts_found"`
}

// NewCRDTStateStore: Create a new CRDT state store for this control plane
func NewCRDTStateStore(nodeID string) *CRDTStateStore {
	return &CRDTStateStore{
		nodeID:         nodeID,
		ActiveClusters: NewORSet(nodeID),
		ClusterStates:  make(map[string]*CRDTClusterState),
		ActiveJobs:     NewORSet(nodeID),
		JobStates:      make(map[string]*CRDTJobState),
		CompletedJobs:  NewGSet(),
		Clock:          NewVectorClock(),
		LastSyncTime:   time.Now(),
	}
}

// ============================================================================
// CLUSTER OPERATIONS
// ============================================================================

// RegisterCluster: Add a cluster to the CRDT state
func (store *CRDTStateStore) RegisterCluster(clusterID, region, zone string) *CRDTClusterState {
	store.mu.Lock()
	defer store.mu.Unlock()

	store.ActiveClusters.Add(clusterID)

	state, exists := store.ClusterStates[clusterID]
	if !exists {
		state = NewCRDTClusterState(store.nodeID, clusterID, region, zone)
		store.ClusterStates[clusterID] = state
	}

	store.Clock.Increment(store.nodeID)
	return state
}

// DeregisterCluster: Remove a cluster from active set
func (store *CRDTStateStore) DeregisterCluster(clusterID string) {
	store.mu.Lock()
	defer store.mu.Unlock()

	store.ActiveClusters.Remove(clusterID)
	store.Clock.Increment(store.nodeID)
}

// UpdateClusterLoad: Update cluster metrics from heartbeat
func (store *CRDTStateStore) UpdateClusterLoad(clusterID string, gpusInUse int, memGBInUse float64, runningJobs int) {
	store.mu.RLock()
	state, exists := store.ClusterStates[clusterID]
	store.mu.RUnlock()

	if !exists {
		return
	}

	state.UpdateLoad(store.nodeID, gpusInUse, memGBInUse, runningJobs)
}

// GetClusterState: Get CRDT state for a cluster
func (store *CRDTStateStore) GetClusterState(clusterID string) *CRDTClusterState {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.ClusterStates[clusterID]
}

// GetActiveClusters: List all active cluster IDs
func (store *CRDTStateStore) GetActiveClusters() []string {
	return store.ActiveClusters.Members()
}

// ============================================================================
// JOB OPERATIONS
// ============================================================================

// RegisterJob: Add a job to CRDT state tracking
func (store *CRDTStateStore) RegisterJob(jobID string) *CRDTJobState {
	store.mu.Lock()
	defer store.mu.Unlock()

	store.ActiveJobs.Add(jobID)

	state, exists := store.JobStates[jobID]
	if !exists {
		state = NewCRDTJobState(store.nodeID, jobID)
		store.JobStates[jobID] = state
	}

	store.Clock.Increment(store.nodeID)
	return state
}

// CompleteJob: Move job from active to completed (permanent)
func (store *CRDTStateStore) CompleteJob(jobID string) {
	store.mu.Lock()
	defer store.mu.Unlock()

	store.ActiveJobs.Remove(jobID)
	store.CompletedJobs.Add(jobID)
	store.Clock.Increment(store.nodeID)
}

// GetJobState: Get CRDT state for a job
func (store *CRDTStateStore) GetJobState(jobID string) *CRDTJobState {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.JobStates[jobID]
}

// ============================================================================
// FULL STATE SYNC (MERGE)
// ============================================================================

// MergeRemoteState: Merge entire state from a peer control plane.
// This is the core replication operation.
// Called when this node receives a state snapshot from another node.
//
// Convergence guarantee: After all nodes exchange state, they all
// have identical data, regardless of message order or duplicates.
func (store *CRDTStateStore) MergeRemoteState(remote *CRDTStateStore) *StateMergeResult {
	if remote == nil {
		return &StateMergeResult{Success: false, Reason: "nil remote state"}
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	result := &StateMergeResult{
		Success:           true,
		ClustersUpdated:   0,
		JobsUpdated:       0,
		ConflictsResolved: 0,
	}

	// 1. Merge cluster membership (OR-Set)
	store.ActiveClusters.Merge(remote.ActiveClusters)

	// 2. Merge cluster states (LWW registers per cluster)
	for clusterID, remoteState := range remote.ClusterStates {
		localState, exists := store.ClusterStates[clusterID]
		if !exists {
			// New cluster we didn't know about
			store.ClusterStates[clusterID] = remoteState
			result.ClustersUpdated++
		} else {
			mergeResult := localState.Merge(remoteState)
			if mergeResult.FieldsUpdated > 0 {
				result.ClustersUpdated++
				result.ConflictsResolved += mergeResult.FieldsUpdated
			}
		}
	}

	// 3. Merge job membership (OR-Set)
	store.ActiveJobs.Merge(remote.ActiveJobs)

	// 4. Merge job states (LWW registers per job)
	for jobID, remoteState := range remote.JobStates {
		localState, exists := store.JobStates[jobID]
		if !exists {
			store.JobStates[jobID] = remoteState
			result.JobsUpdated++
		} else {
			mergeResult := localState.Merge(remoteState)
			if mergeResult.FieldsUpdated > 0 {
				result.JobsUpdated++
				result.ConflictsResolved += mergeResult.FieldsUpdated
			}
		}
	}

	// 5. Merge completed jobs (G-Set: union only)
	store.CompletedJobs.Merge(remote.CompletedJobs)

	// 6. Merge vector clocks
	store.Clock.Merge(remote.Clock)

	// Update sync metadata
	store.SyncCount++
	store.LastSyncTime = time.Now()
	store.TotalMerges++
	store.ConflictsFound += uint64(result.ConflictsResolved)

	result.Reason = fmt.Sprintf("synced %d clusters, %d jobs, %d conflicts resolved",
		result.ClustersUpdated, result.JobsUpdated, result.ConflictsResolved)

	return result
}

// Serialize: Encode entire state for transmission to peers
func (store *CRDTStateStore) Serialize() ([]byte, error) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return json.Marshal(store)
}

// Deserialize: Decode state received from a peer
func DeserializeCRDTStateStore(data []byte) (*CRDTStateStore, error) {
	var store CRDTStateStore
	err := json.Unmarshal(data, &store)
	if err != nil {
		return nil, fmt.Errorf("CRDT deserialize failed: %w", err)
	}
	return &store, nil
}

// ============================================================================
// SYNC STATISTICS
// ============================================================================

// StateMergeResult: Outcome of a full state merge
type StateMergeResult struct {
	Success           bool
	ClustersUpdated   int
	JobsUpdated       int
	ConflictsResolved int
	Reason            string
}

// GetSyncStats: Statistics about CRDT replication
func (store *CRDTStateStore) GetSyncStats() map[string]interface{} {
	store.mu.RLock()
	defer store.mu.RUnlock()

	return map[string]interface{}{
		"node_id":            store.nodeID,
		"active_clusters":    store.ActiveClusters.Size(),
		"active_jobs":        store.ActiveJobs.Size(),
		"completed_jobs":     store.CompletedJobs.Size(),
		"cluster_states":     len(store.ClusterStates),
		"job_states":         len(store.JobStates),
		"sync_count":         store.SyncCount,
		"last_sync_time":     store.LastSyncTime,
		"total_merges":       store.TotalMerges,
		"conflicts_resolved": store.ConflictsFound,
		"vector_clock":       store.Clock.String(),
	}
}
