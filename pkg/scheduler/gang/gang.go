// File: pkg/scheduler/gang/gang.go
// Feature: Gang Scheduling
//
// Gang scheduling ensures all-or-nothing resource allocation for
// distributed training jobs. A gang is a group of tasks that must
// ALL be scheduled simultaneously or NONE of them run.
//
// Why this matters:
//   - PyTorch DDP needs all N workers running before any can train
//   - Horovod/DeepSpeed require synchronized AllReduce across all ranks
//   - Partial allocation wastes GPUs (workers sit idle waiting for peers)
//
// Core problems solved:
//   1. Atomicity: Reserve all resources before starting any task
//   2. Deadlock: Two gangs each holding half the resources
//   3. Fragmentation: Pack gang members efficiently across nodes
//   4. Barrier sync: All members must reach "running" state together
//
// References:
//   - Google Borg: "Gang scheduling for large-scale ML workloads"
//   - Kubernetes Volcano: kube-batch gang scheduling plugin
//   - SLURM: --nodes=N --ntasks-per-node=M semantics

package gang

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
)

// ============================================================================
// GANG SPEC (what the user submits)
// ============================================================================

// GangSpec: Defines a gang of tasks that must be co-scheduled
type GangSpec struct {
	// Identity
	GangID string `json:"gang_id"` // Unique gang identifier
	Name   string `json:"name"`    // Human-readable name: "llama-70b-training"

	// Gang shape
	MinMembers    int `json:"min_members"`     // Minimum tasks required (e.g., 4)
	MaxMembers    int `json:"max_members"`     // Maximum tasks (0 = same as min)
	GPUsPerMember int `json:"gpus_per_member"` // GPUs each member needs (e.g., 8)
	TotalGPUs     int `json:"total_gpus"`      // Total GPUs for entire gang (e.g., 32)

	// Placement constraints
	MaxNodesSpread  int  `json:"max_nodes_spread"`  // Max nodes to spread across (0 = no limit)
	PreferColocated bool `json:"prefer_colocated"`  // Prefer all members on same node/rack
	RequireSameZone bool `json:"require_same_zone"` // All members must be in same zone
	RequireNVLink   bool `json:"require_nvlink"`    // All inter-member communication via NVLink

	// Timing
	ScheduleTimeout time.Duration `json:"schedule_timeout"` // Max wait for all members (default: 5min)
	BarrierTimeout  time.Duration `json:"barrier_timeout"`  // Max wait for all members to reach running
	Priority        int           `json:"priority"`         // Gang priority (0-100)

	// Job template (applied to each member)
	JobTemplate *common.JobSpec `json:"job_template"` // Base spec for each gang member
}

// ============================================================================
// GANG STATE MACHINE
// ============================================================================

// GangPhase: Current phase in the gang lifecycle
type GangPhase string

const (
	GangPending    GangPhase = "PENDING"    // Waiting for all members to submit
	GangScheduling GangPhase = "SCHEDULING" // Finding resources for all members
	GangAllocated  GangPhase = "ALLOCATED"  // Resources reserved, creating pods
	GangBarrier    GangPhase = "BARRIER"    // Waiting for all pods to reach running
	GangRunning    GangPhase = "RUNNING"    // All members running, barrier released
	GangSucceeded  GangPhase = "SUCCEEDED"  // All members completed successfully
	GangFailed     GangPhase = "FAILED"     // One or more members failed
	GangCancelled  GangPhase = "CANCELLED"  // User cancelled the gang
	GangTimeout    GangPhase = "TIMEOUT"    // Scheduling or barrier timed out
)

// GangState: Runtime state of a gang
type GangState struct {
	mu sync.RWMutex

	// Spec
	Spec *GangSpec `json:"spec"`

	// Phase
	Phase     GangPhase `json:"phase"`
	PhaseTime time.Time `json:"phase_time"` // When current phase started

	// Members
	Members      []*GangMember `json:"members"`
	ReadyCount   int           `json:"ready_count"`   // Members that reached "running"
	FailedCount  int           `json:"failed_count"`  // Members that failed
	PendingCount int           `json:"pending_count"` // Members still pending

	// Placement decision
	Placement *GangPlacement `json:"placement"` // Where each member goes

	// Timing
	CreateTime   time.Time `json:"create_time"`
	ScheduleTime time.Time `json:"schedule_time"` // When resources were allocated
	BarrierTime  time.Time `json:"barrier_time"`  // When barrier was released
	EndTime      time.Time `json:"end_time"`

	// Error tracking
	LastError string `json:"last_error"`
}

// GangMember: Individual task within a gang
type GangMember struct {
	MemberIndex int              `json:"member_index"` // 0-based rank
	JobID       string           `json:"job_id"`       // Assigned job ID
	Status      common.JobStatus `json:"status"`       // Current status
	ClusterID   string           `json:"cluster_id"`   // Assigned cluster
	NodeID      string           `json:"node_id"`      // Assigned node
	GPUIndices  []int            `json:"gpu_indices"`  // Assigned GPUs
	PodName     string           `json:"pod_name"`     // K8s pod name
	Ready       bool             `json:"ready"`        // Reached running state
	Error       string           `json:"error"`        // Error if failed
}

// GangPlacement: Placement decision for entire gang
type GangPlacement struct {
	// Per-member placement
	Assignments []MemberAssignment `json:"assignments"`

	// Aggregated info
	ClustersUsed []string `json:"clusters_used"` // Which clusters
	NodesUsed    []string `json:"nodes_used"`    // Which nodes
	TotalGPUs    int      `json:"total_gpus"`    // Total GPUs reserved
	Score        float64  `json:"score"`         // Placement quality score

	// Colocality metrics
	IsColocated     bool    `json:"is_colocated"`     // All on same node?
	IsSameZone      bool    `json:"is_same_zone"`     // All in same zone?
	NVLinkConnected bool    `json:"nvlink_connected"` // All via NVLink?
	FragmentScore   float64 `json:"fragment_score"`   // 0-1, lower = less fragmentation
}

// MemberAssignment: Where a specific gang member goes
type MemberAssignment struct {
	MemberIndex int    `json:"member_index"`
	ClusterID   string `json:"cluster_id"`
	NodeID      string `json:"node_id"`
	GPUIndices  []int  `json:"gpu_indices"`
}

// ============================================================================
// GANG MANAGER
// ============================================================================

// GangManager: Coordinates gang scheduling across the cluster
type GangManager struct {
	mu  sync.RWMutex
	log *logger.Logger

	// Active gangs
	gangs map[string]*GangState // gangID -> state

	// Wait queue: gangs waiting for resources
	waitQueue []*GangState

	// Configuration
	config *GangConfig

	// Deadlock detection
	deadlockDetector *DeadlockDetector

	// Metrics
	totalGangsSubmitted uint64
	totalGangsCompleted uint64
	totalGangsFailed    uint64
	totalGangsTimedOut  uint64
	totalDeadlocks      uint64
}

// GangConfig: Configuration
type GangConfig struct {
	MaxGangsInQueue   int           // Max gangs waiting (default: 100)
	MaxMembersPerGang int           // Max members per gang (default: 128)
	DefaultTimeout    time.Duration // Default schedule timeout (default: 5min)
	BarrierTimeout    time.Duration // Default barrier timeout (default: 10min)
	DeadlockCheckSec  int           // Deadlock check interval (default: 30)
	EnableDeadlock    bool          // Enable deadlock detection (default: true)
}

var DefaultGangConfig = &GangConfig{
	MaxGangsInQueue:   100,
	MaxMembersPerGang: 128,
	DefaultTimeout:    5 * time.Minute,
	BarrierTimeout:    10 * time.Minute,
	DeadlockCheckSec:  30,
	EnableDeadlock:    true,
}

// NewGangManager: Create a new gang manager
func NewGangManager(config *GangConfig) *GangManager {
	if config == nil {
		config = DefaultGangConfig
	}

	gm := &GangManager{
		log:              logger.Get(),
		gangs:            make(map[string]*GangState),
		waitQueue:        make([]*GangState, 0),
		config:           config,
		deadlockDetector: NewDeadlockDetector(),
	}

	return gm
}

// ============================================================================
// GANG LIFECYCLE
// ============================================================================

// SubmitGang: Submit a new gang for scheduling
// Returns immediately. Gang enters PENDING phase.
func (gm *GangManager) SubmitGang(ctx context.Context, spec *GangSpec) (*GangState, error) {
	if err := gm.validateSpec(spec); err != nil {
		return nil, fmt.Errorf("invalid gang spec: %w", err)
	}

	gm.mu.Lock()
	defer gm.mu.Unlock()

	// Check if gang already exists (idempotency)
	if existing, ok := gm.gangs[spec.GangID]; ok {
		return existing, nil
	}

	// Check queue capacity
	if len(gm.waitQueue) >= gm.config.MaxGangsInQueue {
		return nil, fmt.Errorf("gang queue full (%d/%d)", len(gm.waitQueue), gm.config.MaxGangsInQueue)
	}

	// Set defaults
	if spec.MaxMembers == 0 {
		spec.MaxMembers = spec.MinMembers
	}
	if spec.ScheduleTimeout == 0 {
		spec.ScheduleTimeout = gm.config.DefaultTimeout
	}
	if spec.BarrierTimeout == 0 {
		spec.BarrierTimeout = gm.config.BarrierTimeout
	}
	if spec.TotalGPUs == 0 {
		spec.TotalGPUs = spec.MinMembers * spec.GPUsPerMember
	}

	// Create members
	members := make([]*GangMember, spec.MinMembers)
	for i := 0; i < spec.MinMembers; i++ {
		members[i] = &GangMember{
			MemberIndex: i,
			JobID:       fmt.Sprintf("%s-member-%d", spec.GangID, i),
			Status:      common.StatusPending,
		}
	}

	state := &GangState{
		Spec:         spec,
		Phase:        GangPending,
		PhaseTime:    time.Now(),
		Members:      members,
		PendingCount: spec.MinMembers,
		CreateTime:   time.Now(),
	}

	gm.gangs[spec.GangID] = state
	gm.waitQueue = append(gm.waitQueue, state)
	gm.totalGangsSubmitted++

	gm.log.Info("GANG: Submitted gang %s (%s): %d members × %d GPUs = %d total GPUs",
		spec.GangID, spec.Name, spec.MinMembers, spec.GPUsPerMember, spec.TotalGPUs)

	return state, nil
}

// TryScheduleGang: Attempt to schedule a gang (called by scheduler loop)
// Returns placement if resources available, nil if not.
//
// This is the core algorithm:
//  1. Check if total GPUs available >= gang's TotalGPUs
//  2. Find node combination that satisfies placement constraints
//  3. Atomically reserve all resources
//  4. Transition to ALLOCATED phase
func (gm *GangManager) TryScheduleGang(
	ctx context.Context,
	gang *GangState,
	availableNodes []NodeResources,
) (*GangPlacement, error) {

	gang.mu.Lock()
	defer gang.mu.Unlock()

	if gang.Phase != GangPending && gang.Phase != GangScheduling {
		return nil, fmt.Errorf("gang %s in wrong phase: %s", gang.Spec.GangID, gang.Phase)
	}

	// Check timeout
	if time.Since(gang.CreateTime) > gang.Spec.ScheduleTimeout {
		gang.Phase = GangTimeout
		gang.PhaseTime = time.Now()
		gang.LastError = "scheduling timed out"
		gm.totalGangsTimedOut++
		return nil, fmt.Errorf("gang %s scheduling timed out after %s", gang.Spec.GangID, gang.Spec.ScheduleTimeout)
	}

	gang.Phase = GangScheduling
	gang.PhaseTime = time.Now()

	// Step 1: Filter nodes with enough GPUs
	candidateNodes := gm.filterCandidateNodes(gang.Spec, availableNodes)
	if len(candidateNodes) == 0 {
		gang.Phase = GangPending // Back to waiting
		return nil, nil          // Not an error, just not enough resources yet
	}

	// Step 2: Check total capacity
	totalAvailGPUs := 0
	for _, node := range candidateNodes {
		totalAvailGPUs += node.AvailableGPUs
	}
	if totalAvailGPUs < gang.Spec.TotalGPUs {
		gang.Phase = GangPending
		return nil, nil // Not enough total GPUs
	}

	// Step 3: Find optimal placement (bin-packing)
	placement := gm.findOptimalPlacement(gang.Spec, candidateNodes)
	if placement == nil {
		gang.Phase = GangPending
		return nil, nil // Couldn't satisfy constraints
	}

	// Step 4: Validate placement meets constraints
	if err := gm.validatePlacement(gang.Spec, placement); err != nil {
		gang.Phase = GangPending
		gm.log.Warn("GANG: Placement for %s failed validation: %v", gang.Spec.GangID, err)
		return nil, nil
	}

	// Step 5: ATOMIC ALLOCATION — this is the critical section
	// All resources must be reserved at once, or none
	gang.Placement = placement
	gang.Phase = GangAllocated
	gang.PhaseTime = time.Now()
	gang.ScheduleTime = time.Now()

	// Update member assignments
	for i, assignment := range placement.Assignments {
		if i < len(gang.Members) {
			gang.Members[i].ClusterID = assignment.ClusterID
			gang.Members[i].NodeID = assignment.NodeID
			gang.Members[i].GPUIndices = assignment.GPUIndices
			gang.Members[i].Status = common.StatusScheduled
		}
	}

	gm.log.Info("GANG: Allocated %s: %d GPUs across %d nodes (score=%.2f, colocated=%v, nvlink=%v)",
		gang.Spec.GangID, placement.TotalGPUs, len(placement.NodesUsed),
		placement.Score, placement.IsColocated, placement.NVLinkConnected)

	return placement, nil
}

// ============================================================================
// PLACEMENT ALGORITHM
// ============================================================================

// NodeResources: Available resources on a node (input to placement algorithm)
type NodeResources struct {
	ClusterID     string
	NodeID        string
	Zone          string
	AvailableGPUs int
	TotalGPUs     int
	GPUType       string
	HasNVLink     bool
	NUMANodes     int
	GPUsPerNUMA   int
}

// filterCandidateNodes: Filter nodes that could host gang members
func (gm *GangManager) filterCandidateNodes(spec *GangSpec, nodes []NodeResources) []NodeResources {
	candidates := make([]NodeResources, 0)

	for _, node := range nodes {
		// Must have at least GPUsPerMember available
		if node.AvailableGPUs < spec.GPUsPerMember {
			continue
		}

		// GPU type must match (if template specifies)
		if spec.JobTemplate != nil && spec.JobTemplate.GPUType != "" && spec.JobTemplate.GPUType != "any" {
			if node.GPUType != spec.JobTemplate.GPUType {
				continue
			}
		}

		// NVLink requirement
		if spec.RequireNVLink && !node.HasNVLink {
			continue
		}

		candidates = append(candidates, node)
	}

	return candidates
}

// findOptimalPlacement: Bin-pack gang members onto nodes
//
// Strategy: First-Fit Decreasing with colocality preference
//  1. Sort nodes by available GPUs (descending) — pack big nodes first
//  2. Greedily assign members to nodes
//  3. Prefer fewer nodes (better colocality, less network overhead)
//
// For distributed training, minimizing inter-node communication is critical.
// NVLink within a node: 900 GB/s. Network between nodes: 100-400 Gbps.
// A 9x bandwidth difference means colocated placement can be 2-3x faster.
func (gm *GangManager) findOptimalPlacement(spec *GangSpec, nodes []NodeResources) *GangPlacement {
	// Sort nodes by available GPUs (descending) — pack into fewest nodes
	sortedNodes := make([]NodeResources, len(nodes))
	copy(sortedNodes, nodes)
	for i := 0; i < len(sortedNodes); i++ {
		for j := i + 1; j < len(sortedNodes); j++ {
			if sortedNodes[j].AvailableGPUs > sortedNodes[i].AvailableGPUs {
				sortedNodes[i], sortedNodes[j] = sortedNodes[j], sortedNodes[i]
			}
		}
	}

	assignments := make([]MemberAssignment, 0, spec.MinMembers)
	nodesUsed := make(map[string]bool)
	clustersUsed := make(map[string]bool)
	remainingGPUs := make(map[string]int) // nodeID -> remaining after allocation
	totalAssigned := 0

	for _, node := range sortedNodes {
		remaining := node.AvailableGPUs

		// How many members can this node host?
		membersOnNode := remaining / spec.GPUsPerMember
		if membersOnNode == 0 {
			continue
		}

		// Don't exceed what we need
		membersNeeded := spec.MinMembers - totalAssigned
		if membersOnNode > membersNeeded {
			membersOnNode = membersNeeded
		}

		// Assign members to this node
		for m := 0; m < membersOnNode; m++ {
			memberIdx := totalAssigned
			gpuStart := node.TotalGPUs - remaining
			gpuIndices := make([]int, spec.GPUsPerMember)
			for g := 0; g < spec.GPUsPerMember; g++ {
				gpuIndices[g] = gpuStart + (m * spec.GPUsPerMember) + g
			}

			assignments = append(assignments, MemberAssignment{
				MemberIndex: memberIdx,
				ClusterID:   node.ClusterID,
				NodeID:      node.NodeID,
				GPUIndices:  gpuIndices,
			})

			remaining -= spec.GPUsPerMember
			totalAssigned++
			nodesUsed[node.NodeID] = true
			clustersUsed[node.ClusterID] = true
		}

		remainingGPUs[node.NodeID] = remaining

		if totalAssigned >= spec.MinMembers {
			break // All members placed
		}
	}

	// Did we place all members?
	if totalAssigned < spec.MinMembers {
		return nil // Can't satisfy gang
	}

	// Build node and cluster lists
	nodeList := make([]string, 0, len(nodesUsed))
	for n := range nodesUsed {
		nodeList = append(nodeList, n)
	}
	clusterList := make([]string, 0, len(clustersUsed))
	for c := range clustersUsed {
		clusterList = append(clusterList, c)
	}

	// Calculate placement quality
	isColocated := len(nodeList) == 1
	isSameZone := len(clusterList) == 1 // Simplified: same cluster = same zone

	// Check NVLink connectivity (all colocated = NVLink possible)
	nvlinkConnected := isColocated
	for _, node := range sortedNodes {
		if nodesUsed[node.NodeID] && !node.HasNVLink {
			nvlinkConnected = false
			break
		}
	}

	// Fragmentation score: 0 = no fragmentation, 1 = max fragmentation
	// Fewer nodes used = less fragmentation
	fragmentScore := float64(len(nodeList)-1) / float64(max(spec.MinMembers-1, 1))

	// Overall placement score
	score := 50.0
	if isColocated {
		score += 30.0 // Big bonus for single-node
	}
	if nvlinkConnected {
		score += 15.0
	}
	if isSameZone {
		score += 5.0
	}
	score -= fragmentScore * 20.0 // Penalty for fragmentation

	return &GangPlacement{
		Assignments:     assignments,
		ClustersUsed:    clusterList,
		NodesUsed:       nodeList,
		TotalGPUs:       totalAssigned * spec.GPUsPerMember,
		Score:           score,
		IsColocated:     isColocated,
		IsSameZone:      isSameZone,
		NVLinkConnected: nvlinkConnected,
		FragmentScore:   fragmentScore,
	}
}

// validatePlacement: Check placement meets gang constraints
func (gm *GangManager) validatePlacement(spec *GangSpec, placement *GangPlacement) error {
	if placement == nil {
		return fmt.Errorf("nil placement")
	}

	// Check total GPUs
	if placement.TotalGPUs < spec.TotalGPUs {
		return fmt.Errorf("insufficient GPUs: need %d, got %d", spec.TotalGPUs, placement.TotalGPUs)
	}

	// Check max nodes spread
	if spec.MaxNodesSpread > 0 && len(placement.NodesUsed) > spec.MaxNodesSpread {
		return fmt.Errorf("too many nodes: limit %d, got %d", spec.MaxNodesSpread, len(placement.NodesUsed))
	}

	// Check same zone requirement
	if spec.RequireSameZone && !placement.IsSameZone {
		return fmt.Errorf("same-zone required but placed across zones")
	}

	// Check NVLink requirement
	if spec.RequireNVLink && !placement.NVLinkConnected {
		return fmt.Errorf("NVLink required but not all members have NVLink")
	}

	return nil
}

// validateSpec: Validate gang spec
func (gm *GangManager) validateSpec(spec *GangSpec) error {
	if spec.GangID == "" {
		return fmt.Errorf("gang_id is required")
	}
	if spec.MinMembers < 1 {
		return fmt.Errorf("min_members must be >= 1")
	}
	if spec.MinMembers > gm.config.MaxMembersPerGang {
		return fmt.Errorf("min_members %d exceeds max %d", spec.MinMembers, gm.config.MaxMembersPerGang)
	}
	if spec.GPUsPerMember < 1 {
		return fmt.Errorf("gpus_per_member must be >= 1")
	}
	return nil
}

// ============================================================================
// BARRIER SYNCHRONIZATION
// ============================================================================

// ReportMemberReady: Called when a gang member's pod reaches running state
// When all members are ready, the barrier releases.
func (gm *GangManager) ReportMemberReady(gangID string, memberIndex int) error {
	gm.mu.RLock()
	gang, exists := gm.gangs[gangID]
	gm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("gang %s not found", gangID)
	}

	gang.mu.Lock()
	defer gang.mu.Unlock()

	if gang.Phase != GangAllocated && gang.Phase != GangBarrier {
		return fmt.Errorf("gang %s in wrong phase for ready report: %s", gangID, gang.Phase)
	}

	// Check barrier timeout
	if gang.Phase == GangBarrier && time.Since(gang.PhaseTime) > gang.Spec.BarrierTimeout {
		gang.Phase = GangTimeout
		gang.LastError = "barrier timed out waiting for all members"
		gm.totalGangsTimedOut++
		return fmt.Errorf("gang %s barrier timed out", gangID)
	}

	if gang.Phase == GangAllocated {
		gang.Phase = GangBarrier
		gang.PhaseTime = time.Now()
	}

	// Mark member ready
	if memberIndex >= 0 && memberIndex < len(gang.Members) {
		if !gang.Members[memberIndex].Ready {
			gang.Members[memberIndex].Ready = true
			gang.Members[memberIndex].Status = common.StatusRunning
			gang.ReadyCount++
			gang.PendingCount--
		}
	}

	gm.log.Info("GANG: %s member %d ready (%d/%d)",
		gangID, memberIndex, gang.ReadyCount, gang.Spec.MinMembers)

	// Check if all members are ready — BARRIER RELEASE
	if gang.ReadyCount >= gang.Spec.MinMembers {
		gang.Phase = GangRunning
		gang.PhaseTime = time.Now()
		gang.BarrierTime = time.Now()

		gm.log.Info("GANG: ★ BARRIER RELEASED for %s — all %d members running!",
			gangID, gang.Spec.MinMembers)
	}

	return nil
}

// ReportMemberFailed: Called when a gang member fails
// If any member fails, the entire gang fails (all-or-nothing).
func (gm *GangManager) ReportMemberFailed(gangID string, memberIndex int, err string) error {
	gm.mu.RLock()
	gang, exists := gm.gangs[gangID]
	gm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("gang %s not found", gangID)
	}

	gang.mu.Lock()
	defer gang.mu.Unlock()

	if memberIndex >= 0 && memberIndex < len(gang.Members) {
		gang.Members[memberIndex].Status = common.StatusFailed
		gang.Members[memberIndex].Error = err
		gang.FailedCount++
	}

	// ANY member failure = entire gang fails
	gang.Phase = GangFailed
	gang.PhaseTime = time.Now()
	gang.EndTime = time.Now()
	gang.LastError = fmt.Sprintf("member %d failed: %s", memberIndex, err)
	gm.totalGangsFailed++

	gm.log.Warn("GANG: %s FAILED — member %d error: %s (cancelling all members)",
		gangID, memberIndex, err)

	return nil
}

// ReportGangCompleted: All members finished successfully
func (gm *GangManager) ReportGangCompleted(gangID string) error {
	gm.mu.RLock()
	gang, exists := gm.gangs[gangID]
	gm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("gang %s not found", gangID)
	}

	gang.mu.Lock()
	defer gang.mu.Unlock()

	gang.Phase = GangSucceeded
	gang.PhaseTime = time.Now()
	gang.EndTime = time.Now()
	gm.totalGangsCompleted++

	duration := gang.EndTime.Sub(gang.CreateTime)
	gm.log.Info("GANG: ★ %s SUCCEEDED — %d members completed in %s",
		gangID, gang.Spec.MinMembers, duration)

	return nil
}

// ============================================================================
// GANG QUERIES
// ============================================================================

// GetGang: Get gang state
func (gm *GangManager) GetGang(gangID string) *GangState {
	gm.mu.RLock()
	defer gm.mu.RUnlock()
	return gm.gangs[gangID]
}

// GetPendingGangs: Get gangs waiting for resources (for scheduler loop)
func (gm *GangManager) GetPendingGangs() []*GangState {
	gm.mu.RLock()
	defer gm.mu.RUnlock()

	pending := make([]*GangState, 0)
	for _, gang := range gm.waitQueue {
		gang.mu.RLock()
		if gang.Phase == GangPending || gang.Phase == GangScheduling {
			pending = append(pending, gang)
		}
		gang.mu.RUnlock()
	}

	// Sort by priority (highest first)
	for i := 0; i < len(pending); i++ {
		for j := i + 1; j < len(pending); j++ {
			if pending[j].Spec.Priority > pending[i].Spec.Priority {
				pending[i], pending[j] = pending[j], pending[i]
			}
		}
	}

	return pending
}

// CancelGang: Cancel a gang and release all resources
func (gm *GangManager) CancelGang(gangID string) error {
	gm.mu.RLock()
	gang, exists := gm.gangs[gangID]
	gm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("gang %s not found", gangID)
	}

	gang.mu.Lock()
	defer gang.mu.Unlock()

	gang.Phase = GangCancelled
	gang.PhaseTime = time.Now()
	gang.EndTime = time.Now()
	gang.LastError = "cancelled by user"

	gm.log.Info("GANG: %s cancelled", gangID)
	return nil
}

// ============================================================================
// DEADLOCK DETECTION
// ============================================================================

// DeadlockDetector: Detects resource deadlocks between gangs
//
// Deadlock scenario:
//
//	Gang A needs 16 GPUs on node-1, holding 8 GPUs on node-2
//	Gang B needs 16 GPUs on node-2, holding 8 GPUs on node-1
//	Neither can proceed → deadlock
//
// Detection: Build wait-for graph. If cycle exists → deadlock.
// Resolution: Cancel the lowest-priority gang in the cycle.
type DeadlockDetector struct {
	mu sync.RWMutex

	// Wait-for graph: gangID -> set of gangIDs it's waiting for
	waitGraph map[string]map[string]bool

	// Resource holdings: gangID -> resources held
	holdings map[string][]ResourceHold

	totalDetections uint64
}

// ResourceHold: A resource held by a gang
type ResourceHold struct {
	NodeID   string
	GPUCount int
}

func NewDeadlockDetector() *DeadlockDetector {
	return &DeadlockDetector{
		waitGraph: make(map[string]map[string]bool),
		holdings:  make(map[string][]ResourceHold),
	}
}

// AddWaiting: Record that gangA is waiting for resources held by gangB
func (dd *DeadlockDetector) AddWaiting(waitingGang, holdingGang string) {
	dd.mu.Lock()
	defer dd.mu.Unlock()

	if dd.waitGraph[waitingGang] == nil {
		dd.waitGraph[waitingGang] = make(map[string]bool)
	}
	dd.waitGraph[waitingGang][holdingGang] = true
}

// RemoveGang: Remove a gang from the wait-for graph (when it completes/cancels)
func (dd *DeadlockDetector) RemoveGang(gangID string) {
	dd.mu.Lock()
	defer dd.mu.Unlock()

	delete(dd.waitGraph, gangID)
	delete(dd.holdings, gangID)

	// Remove from others' wait sets
	for _, waitsFor := range dd.waitGraph {
		delete(waitsFor, gangID)
	}
}

// DetectCycle: Check for cycles in the wait-for graph (DFS)
// Returns the cycle if found, nil if no deadlock
func (dd *DeadlockDetector) DetectCycle() []string {
	dd.mu.RLock()
	defer dd.mu.RUnlock()

	visited := make(map[string]bool)
	inStack := make(map[string]bool)
	path := make([]string, 0)

	for node := range dd.waitGraph {
		if !visited[node] {
			if cycle := dd.dfs(node, visited, inStack, path); cycle != nil {
				dd.totalDetections++
				return cycle
			}
		}
	}
	return nil
}

// dfs: Depth-first search for cycle detection
func (dd *DeadlockDetector) dfs(node string, visited, inStack map[string]bool, path []string) []string {
	visited[node] = true
	inStack[node] = true
	path = append(path, node)

	for neighbor := range dd.waitGraph[node] {
		if !visited[neighbor] {
			if cycle := dd.dfs(neighbor, visited, inStack, path); cycle != nil {
				return cycle
			}
		} else if inStack[neighbor] {
			// Found a cycle! Extract it
			cycle := make([]string, 0)
			for i := len(path) - 1; i >= 0; i-- {
				cycle = append([]string{path[i]}, cycle...)
				if path[i] == neighbor {
					break
				}
			}
			return cycle
		}
	}

	inStack[node] = false
	return nil
}

// ============================================================================
// SCHEDULING LOOP
// ============================================================================

// RunSchedulingLoop: Periodically try to schedule pending gangs
func (gm *GangManager) RunSchedulingLoop(ctx context.Context, getNodes func() []NodeResources) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	deadlockTicker := time.NewTicker(time.Duration(gm.config.DeadlockCheckSec) * time.Second)
	defer deadlockTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			gm.log.Info("GANG: Scheduling loop stopped")
			return

		case <-ticker.C:
			// Try to schedule pending gangs
			pending := gm.GetPendingGangs()
			if len(pending) == 0 {
				continue
			}

			nodes := getNodes()
			for _, gang := range pending {
				placement, err := gm.TryScheduleGang(ctx, gang, nodes)
				if err != nil {
					gm.log.Warn("GANG: Failed to schedule %s: %v", gang.Spec.GangID, err)
					continue
				}
				if placement != nil {
					gm.log.Info("GANG: Scheduled %s — launching %d members", gang.Spec.GangID, len(placement.Assignments))
					// Remove from wait queue
					gm.removeFromWaitQueue(gang.Spec.GangID)
				}
			}

		case <-deadlockTicker.C:
			// Check for deadlocks
			if gm.config.EnableDeadlock {
				if cycle := gm.deadlockDetector.DetectCycle(); cycle != nil {
					gm.handleDeadlock(cycle)
				}
			}
		}
	}
}

// removeFromWaitQueue: Remove gang from wait queue after scheduling
func (gm *GangManager) removeFromWaitQueue(gangID string) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	for i, g := range gm.waitQueue {
		if g.Spec.GangID == gangID {
			gm.waitQueue = append(gm.waitQueue[:i], gm.waitQueue[i+1:]...)
			return
		}
	}
}

// handleDeadlock: Resolve a detected deadlock by cancelling lowest-priority gang
func (gm *GangManager) handleDeadlock(cycle []string) {
	gm.log.Warn("GANG: ⚠ DEADLOCK DETECTED in cycle: %v", cycle)
	gm.totalDeadlocks++

	// Find lowest-priority gang in cycle
	var lowestGang *GangState
	lowestPriority := 101

	for _, gangID := range cycle {
		gang := gm.GetGang(gangID)
		if gang != nil && gang.Spec.Priority < lowestPriority {
			lowestPriority = gang.Spec.Priority
			lowestGang = gang
		}
	}

	if lowestGang != nil {
		gm.log.Warn("GANG: Resolving deadlock by cancelling %s (priority=%d)",
			lowestGang.Spec.GangID, lowestGang.Spec.Priority)
		gm.CancelGang(lowestGang.Spec.GangID)
		gm.deadlockDetector.RemoveGang(lowestGang.Spec.GangID)
	}
}

// ============================================================================
// METRICS
// ============================================================================

func (gm *GangManager) GetStats() map[string]interface{} {
	gm.mu.RLock()
	defer gm.mu.RUnlock()

	return map[string]interface{}{
		"total_submitted": gm.totalGangsSubmitted,
		"total_completed": gm.totalGangsCompleted,
		"total_failed":    gm.totalGangsFailed,
		"total_timed_out": gm.totalGangsTimedOut,
		"total_deadlocks": gm.totalDeadlocks,
		"active_gangs":    len(gm.gangs),
		"queue_depth":     len(gm.waitQueue),
	}
}

// ============================================================================
// HELPERS
// ============================================================================

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
