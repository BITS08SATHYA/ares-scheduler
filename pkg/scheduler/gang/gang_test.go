package gang

import (
	"context"
	"testing"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Helpers
// ============================================================================

func testGangConfig() *GangConfig {
	return &GangConfig{
		MaxGangsInQueue:   10,
		MaxMembersPerGang: 128,
		DefaultTimeout:    5 * time.Minute,
		BarrierTimeout:    10 * time.Minute,
		DeadlockCheckSec:  30,
		EnableDeadlock:    true,
	}
}

func testGangSpec(gangID string, members, gpusPerMember int) *GangSpec {
	return &GangSpec{
		GangID:          gangID,
		Name:            "test-gang",
		MinMembers:      members,
		GPUsPerMember:   gpusPerMember,
		ScheduleTimeout: 5 * time.Minute,
		BarrierTimeout:  10 * time.Minute,
		Priority:        50,
	}
}

func testNodes(count, gpusPerNode int) []NodeResources {
	nodes := make([]NodeResources, count)
	for i := 0; i < count; i++ {
		nodes[i] = NodeResources{
			ClusterID:     "cluster-1",
			NodeID:        "node-" + string(rune('a'+i)),
			Zone:          "us-west-2a",
			AvailableGPUs: gpusPerNode,
			TotalGPUs:     gpusPerNode,
			GPUType:       "A100",
			HasNVLink:     true,
			NUMANodes:     2,
			GPUsPerNUMA:   gpusPerNode / 2,
		}
	}
	return nodes
}

// ============================================================================
// SECTION 1: Constructor
// ============================================================================

func TestNewGangManager_NilConfig(t *testing.T) {
	gm := NewGangManager(nil)
	require.NotNil(t, gm)
	assert.Equal(t, DefaultGangConfig.MaxGangsInQueue, gm.config.MaxGangsInQueue)
}

func TestNewGangManager_CustomConfig(t *testing.T) {
	cfg := testGangConfig()
	cfg.MaxGangsInQueue = 50
	gm := NewGangManager(cfg)
	assert.Equal(t, 50, gm.config.MaxGangsInQueue)
}

// ============================================================================
// SECTION 2: SubmitGang
// ============================================================================

func TestSubmitGang_Success(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	spec := testGangSpec("gang-1", 4, 8)

	state, err := gm.SubmitGang(context.Background(), spec)

	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, GangPending, state.Phase)
	assert.Len(t, state.Members, 4)
	assert.Equal(t, 4, state.PendingCount)
	assert.Equal(t, 32, spec.TotalGPUs) // 4 * 8
	assert.Equal(t, 4, spec.MaxMembers) // defaults to MinMembers
}

func TestSubmitGang_Idempotent(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	spec := testGangSpec("gang-1", 4, 8)

	s1, _ := gm.SubmitGang(context.Background(), spec)
	s2, _ := gm.SubmitGang(context.Background(), spec)

	assert.Same(t, s1, s2) // exact same pointer
}

func TestSubmitGang_MemberIDs(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	spec := testGangSpec("gang-1", 3, 4)

	state, _ := gm.SubmitGang(context.Background(), spec)

	for i, m := range state.Members {
		assert.Equal(t, i, m.MemberIndex)
		assert.Equal(t, "gang-1-member-"+string(rune('0'+i)), m.JobID)
		assert.Equal(t, common.StatusPending, m.Status)
	}
}

func TestSubmitGang_QueueFull(t *testing.T) {
	cfg := testGangConfig()
	cfg.MaxGangsInQueue = 2
	gm := NewGangManager(cfg)

	gm.SubmitGang(context.Background(), testGangSpec("g1", 2, 4))
	gm.SubmitGang(context.Background(), testGangSpec("g2", 2, 4))
	_, err := gm.SubmitGang(context.Background(), testGangSpec("g3", 2, 4))

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "queue full")
}

func TestSubmitGang_InvalidSpec_NoID(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	spec := testGangSpec("", 4, 8)

	_, err := gm.SubmitGang(context.Background(), spec)
	assert.Error(t, err)
}

func TestSubmitGang_InvalidSpec_ZeroMembers(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	spec := testGangSpec("g1", 0, 8)

	_, err := gm.SubmitGang(context.Background(), spec)
	assert.Error(t, err)
}

func TestSubmitGang_InvalidSpec_ZeroGPUs(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	spec := testGangSpec("g1", 4, 0)

	_, err := gm.SubmitGang(context.Background(), spec)
	assert.Error(t, err)
}

func TestSubmitGang_InvalidSpec_TooManyMembers(t *testing.T) {
	cfg := testGangConfig()
	cfg.MaxMembersPerGang = 8
	gm := NewGangManager(cfg)

	_, err := gm.SubmitGang(context.Background(), testGangSpec("g1", 16, 4))
	assert.Error(t, err)
}

// ============================================================================
// SECTION 3: TryScheduleGang
// ============================================================================

func TestTryScheduleGang_Success(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	spec := testGangSpec("gang-1", 2, 4) // 2 members × 4 GPUs = 8 total
	state, _ := gm.SubmitGang(context.Background(), spec)

	// 2 nodes with 8 GPUs each — plenty of room
	nodes := testNodes(2, 8)

	placement, err := gm.TryScheduleGang(context.Background(), state, nodes)
	require.NoError(t, err)
	require.NotNil(t, placement)

	assert.Equal(t, 8, placement.TotalGPUs)
	assert.Len(t, placement.Assignments, 2)
	assert.Equal(t, GangAllocated, state.Phase)
}

func TestTryScheduleGang_ColocatedOnSingleNode(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	spec := testGangSpec("gang-1", 2, 4) // 8 GPUs total
	state, _ := gm.SubmitGang(context.Background(), spec)

	// 1 node with 16 GPUs — should colocate
	nodes := testNodes(1, 16)

	placement, _ := gm.TryScheduleGang(context.Background(), state, nodes)
	require.NotNil(t, placement)

	assert.True(t, placement.IsColocated)
	assert.Len(t, placement.NodesUsed, 1)
}

func TestTryScheduleGang_InsufficientResources(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	spec := testGangSpec("gang-1", 4, 8) // 32 GPUs total
	state, _ := gm.SubmitGang(context.Background(), spec)

	// Only 16 GPUs available
	nodes := testNodes(2, 8)

	placement, err := gm.TryScheduleGang(context.Background(), state, nodes)
	assert.NoError(t, err)
	assert.Nil(t, placement)
	assert.Equal(t, GangPending, state.Phase) // back to waiting
}

func TestTryScheduleGang_NoNodes(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	spec := testGangSpec("gang-1", 2, 4)
	state, _ := gm.SubmitGang(context.Background(), spec)

	placement, err := gm.TryScheduleGang(context.Background(), state, nil)
	assert.NoError(t, err)
	assert.Nil(t, placement)
}

func TestTryScheduleGang_Timeout(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	spec := testGangSpec("gang-1", 2, 4)
	spec.ScheduleTimeout = 1 * time.Millisecond
	state, _ := gm.SubmitGang(context.Background(), spec)

	time.Sleep(5 * time.Millisecond)

	_, err := gm.TryScheduleGang(context.Background(), state, testNodes(2, 8))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")
	assert.Equal(t, GangTimeout, state.Phase)
}

func TestTryScheduleGang_WrongPhase(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	spec := testGangSpec("gang-1", 2, 4)
	state, _ := gm.SubmitGang(context.Background(), spec)

	state.mu.Lock()
	state.Phase = GangRunning
	state.mu.Unlock()

	_, err := gm.TryScheduleGang(context.Background(), state, testNodes(2, 8))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "wrong phase")
}

// ============================================================================
// SECTION 4: Barrier Synchronization
// ============================================================================

func TestReportMemberReady_BarrierRelease(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	spec := testGangSpec("gang-1", 3, 4)
	state, _ := gm.SubmitGang(context.Background(), spec)

	// Schedule the gang first
	gm.TryScheduleGang(context.Background(), state, testNodes(1, 16))
	require.Equal(t, GangAllocated, state.Phase)

	// Report members ready one by one
	gm.ReportMemberReady("gang-1", 0)
	assert.Equal(t, GangBarrier, state.Phase)
	assert.Equal(t, 1, state.ReadyCount)

	gm.ReportMemberReady("gang-1", 1)
	assert.Equal(t, GangBarrier, state.Phase)
	assert.Equal(t, 2, state.ReadyCount)

	gm.ReportMemberReady("gang-1", 2)
	assert.Equal(t, GangRunning, state.Phase) // BARRIER RELEASED!
	assert.Equal(t, 3, state.ReadyCount)
}

func TestReportMemberReady_NotFound(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	err := gm.ReportMemberReady("nonexistent", 0)
	assert.Error(t, err)
}

func TestReportMemberReady_Idempotent(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	spec := testGangSpec("gang-1", 2, 4)
	state, _ := gm.SubmitGang(context.Background(), spec)
	gm.TryScheduleGang(context.Background(), state, testNodes(1, 16))

	gm.ReportMemberReady("gang-1", 0)
	gm.ReportMemberReady("gang-1", 0)    // duplicate
	assert.Equal(t, 1, state.ReadyCount) // should still be 1
}

// ============================================================================
// SECTION 4b: OnMemberStatus ingestion (barrier wiring)
// ============================================================================

func allocatedGang(t *testing.T, gm *GangManager, id string, members int) *GangState {
	t.Helper()
	state, _ := gm.SubmitGang(context.Background(), testGangSpec(id, members, 4))
	gm.TryScheduleGang(context.Background(), state, testNodes(1, 16))
	require.Equal(t, GangAllocated, state.Phase)
	return state
}

func TestOnMemberStatus_DrivesBarrierAndSuccess(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	state := allocatedGang(t, gm, "gang-1", 3)

	// All members reach RUNNING -> barrier releases.
	require.NoError(t, gm.OnMemberStatus("gang-1", 0, common.StatusRunning, ""))
	require.NoError(t, gm.OnMemberStatus("gang-1", 1, common.StatusRunning, ""))
	require.NoError(t, gm.OnMemberStatus("gang-1", 2, common.StatusRunning, ""))
	assert.Equal(t, GangRunning, state.Phase)

	// All members succeed -> gang completes.
	gm.OnMemberStatus("gang-1", 0, common.StatusSucceeded, "")
	gm.OnMemberStatus("gang-1", 1, common.StatusSucceeded, "")
	gm.OnMemberStatus("gang-1", 2, common.StatusSucceeded, "")
	assert.Equal(t, GangSucceeded, state.Phase)
}

func TestOnMemberStatus_FailureCascades(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	state := allocatedGang(t, gm, "gang-1", 3)

	gm.OnMemberStatus("gang-1", 0, common.StatusRunning, "")
	require.NoError(t, gm.OnMemberStatus("gang-1", 1, common.StatusFailed, "OOM killed"))
	assert.Equal(t, GangFailed, state.Phase)
}

func TestOnMemberStatus_PartialSuccessDoesNotComplete(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	state := allocatedGang(t, gm, "gang-1", 3)

	for i := 0; i < 3; i++ {
		gm.OnMemberStatus("gang-1", i, common.StatusRunning, "")
	}
	gm.OnMemberStatus("gang-1", 0, common.StatusSucceeded, "")
	gm.OnMemberStatus("gang-1", 1, common.StatusSucceeded, "")
	assert.Equal(t, GangRunning, state.Phase) // not all done yet
}

func TestOnMemberStatus_IgnoresNonTerminalStatuses(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	state := allocatedGang(t, gm, "gang-1", 2)

	require.NoError(t, gm.OnMemberStatus("gang-1", 0, common.StatusScheduled, ""))
	assert.Equal(t, GangAllocated, state.Phase) // unchanged
}

// ============================================================================
// SECTION 4c: Regression — GPU index allocation, deadlock wiring, barrier sweep
// ============================================================================

// TestFindOptimalPlacement_GPUIndicesContiguousAndInRange guards the bug where
// packing >1 member onto a node double-counted the per-member offset, producing
// out-of-range GPU indices (e.g. [8,9,10,11] on an 8-GPU node).
func TestFindOptimalPlacement_GPUIndicesContiguousAndInRange(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	spec := testGangSpec("gang-1", 2, 4) // 2 members × 4 GPUs = 8, fits one node
	spec.TotalGPUs = 8

	placement := gm.findOptimalPlacement(spec, testNodes(1, 8))
	require.NotNil(t, placement)
	require.Len(t, placement.Assignments, 2)
	assert.True(t, placement.IsColocated, "both members should pack onto one node")

	seen := map[int]bool{}
	for _, a := range placement.Assignments {
		require.Len(t, a.GPUIndices, 4)
		for _, idx := range a.GPUIndices {
			assert.GreaterOrEqual(t, idx, 0)
			assert.Less(t, idx, 8, "GPU index must stay within the node's 8 GPUs")
			assert.False(t, seen[idx], "GPU index %d assigned twice", idx)
			seen[idx] = true
		}
	}
	// All 8 distinct GPUs 0..7 must be covered exactly once.
	assert.Len(t, seen, 8)
}

// TestDeadlockDetector_FedFromLifecycle verifies the wait-for graph is populated
// from real allocation/contention rather than being permanently empty.
func TestDeadlockDetector_FedFromLifecycle(t *testing.T) {
	gm := NewGangManager(testGangConfig())

	// Gang A grabs the only node's GPUs (allocated -> becomes a holder).
	a, _ := gm.SubmitGang(context.Background(), testGangSpec("gang-a", 2, 4))
	nodes := testNodes(1, 8)
	_, err := gm.TryScheduleGang(context.Background(), a, nodes)
	require.NoError(t, err)
	require.Equal(t, GangAllocated, a.Phase)
	assert.Contains(t, gm.deadlockDetector.Holders(), "gang-a")

	// Gang B can't fit (node now logically full) -> records a wait edge on A.
	b, _ := gm.SubmitGang(context.Background(), testGangSpec("gang-b", 2, 4))
	emptyNode := testNodes(1, 8)
	emptyNode[0].AvailableGPUs = 0 // A holds them all
	_, err = gm.TryScheduleGang(context.Background(), b, emptyNode)
	require.NoError(t, err)
	require.Equal(t, GangPending, b.Phase)
	assert.True(t, gm.deadlockDetector.waitGraph["gang-b"]["gang-a"],
		"B should be recorded as waiting on holder A")

	// When A completes, its holdings are released from the detector.
	for i := 0; i < 2; i++ {
		gm.OnMemberStatus("gang-a", i, common.StatusRunning, "")
	}
	for i := 0; i < 2; i++ {
		gm.OnMemberStatus("gang-a", i, common.StatusSucceeded, "")
	}
	assert.NotContains(t, gm.deadlockDetector.Holders(), "gang-a")
}

// TestSweepBarrierTimeouts_FailsStuckGang guards the gap where a gang whose
// members never reach RUNNING would hang in ALLOCATED forever, because the only
// barrier-deadline check lived inside ReportMemberReady.
func TestSweepBarrierTimeouts_FailsStuckGang(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	spec := testGangSpec("gang-1", 3, 4)
	spec.BarrierTimeout = 10 * time.Millisecond
	state, _ := gm.SubmitGang(context.Background(), spec)
	_, err := gm.TryScheduleGang(context.Background(), state, testNodes(1, 16))
	require.NoError(t, err)
	require.Equal(t, GangAllocated, state.Phase)

	// No member ever reports RUNNING. Before the deadline: still allocated.
	gm.sweepBarrierTimeouts()
	assert.Equal(t, GangAllocated, state.Phase)

	time.Sleep(15 * time.Millisecond)
	gm.sweepBarrierTimeouts()
	assert.Equal(t, GangTimeout, state.Phase)
	assert.Equal(t, uint64(1), gm.GetStats()["total_timed_out"])
}

// ============================================================================
// SECTION 5: Member Failure
// ============================================================================

func TestReportMemberFailed_FailsEntireGang(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	spec := testGangSpec("gang-1", 3, 4)
	state, _ := gm.SubmitGang(context.Background(), spec)

	gm.TryScheduleGang(context.Background(), state, testNodes(1, 16))
	gm.ReportMemberReady("gang-1", 0)

	err := gm.ReportMemberFailed("gang-1", 1, "OOM killed")
	assert.NoError(t, err)
	assert.Equal(t, GangFailed, state.Phase)
	assert.Equal(t, "OOM killed", state.Members[1].Error)
	assert.Contains(t, state.LastError, "member 1 failed")

	// Gang should be removed from active gangs
	assert.Nil(t, gm.GetGang("gang-1"))
}

func TestReportMemberFailed_NotFound(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	err := gm.ReportMemberFailed("nonexistent", 0, "error")
	assert.Error(t, err)
}

// ============================================================================
// SECTION 6: Gang Completion
// ============================================================================

func TestReportGangCompleted(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	spec := testGangSpec("gang-1", 2, 4)
	gm.SubmitGang(context.Background(), spec)

	err := gm.ReportGangCompleted("gang-1")
	assert.NoError(t, err)

	// Gang removed from map
	assert.Nil(t, gm.GetGang("gang-1"))

	stats := gm.GetStats()
	assert.Equal(t, uint64(1), stats["total_completed"])
}

func TestReportGangCompleted_NotFound(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	err := gm.ReportGangCompleted("nonexistent")
	assert.Error(t, err)
}

// ============================================================================
// SECTION 7: CancelGang
// ============================================================================

func TestCancelGang(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	spec := testGangSpec("gang-1", 2, 4)
	state, _ := gm.SubmitGang(context.Background(), spec)

	err := gm.CancelGang("gang-1")
	assert.NoError(t, err)
	assert.Equal(t, GangCancelled, state.Phase)
	assert.Contains(t, state.LastError, "canceled")
}

func TestCancelGang_NotFound(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	err := gm.CancelGang("nonexistent")
	assert.Error(t, err)
}

// ============================================================================
// SECTION 8: Placement Algorithm
// ============================================================================

func TestFilterCandidateNodes_GPURequirement(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	spec := testGangSpec("g1", 2, 4) // needs 4 GPUs per member

	nodes := []NodeResources{
		{NodeID: "big", AvailableGPUs: 8, HasNVLink: true},
		{NodeID: "small", AvailableGPUs: 2, HasNVLink: true}, // too small
	}

	candidates := gm.filterCandidateNodes(spec, nodes)
	assert.Len(t, candidates, 1)
	assert.Equal(t, "big", candidates[0].NodeID)
}

func TestFilterCandidateNodes_NVLinkRequired(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	spec := testGangSpec("g1", 2, 4)
	spec.RequireNVLink = true

	nodes := []NodeResources{
		{NodeID: "nvlink", AvailableGPUs: 8, HasNVLink: true},
		{NodeID: "pcie", AvailableGPUs: 8, HasNVLink: false},
	}

	candidates := gm.filterCandidateNodes(spec, nodes)
	assert.Len(t, candidates, 1)
	assert.Equal(t, "nvlink", candidates[0].NodeID)
}

func TestFilterCandidateNodes_GPUTypeMatch(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	spec := testGangSpec("g1", 2, 4)
	spec.JobTemplate = &common.JobSpec{GPUType: "H100"}

	nodes := []NodeResources{
		{NodeID: "h100", AvailableGPUs: 8, GPUType: "H100", HasNVLink: true},
		{NodeID: "a100", AvailableGPUs: 8, GPUType: "A100", HasNVLink: true},
	}

	candidates := gm.filterCandidateNodes(spec, nodes)
	assert.Len(t, candidates, 1)
	assert.Equal(t, "h100", candidates[0].NodeID)
}

func TestValidatePlacement_MaxNodesSpread(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	spec := testGangSpec("g1", 4, 4)
	spec.MaxNodesSpread = 2

	placement := &GangPlacement{
		NodesUsed: []string{"n1", "n2", "n3"}, // 3 > max 2
		TotalGPUs: 16,
	}

	err := gm.validatePlacement(spec, placement)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "too many nodes")
}

func TestValidatePlacement_SameZoneRequired(t *testing.T) {
	gm := NewGangManager(testGangConfig())
	spec := testGangSpec("g1", 2, 4)
	spec.RequireSameZone = true

	placement := &GangPlacement{
		TotalGPUs:  8,
		IsSameZone: false,
	}

	err := gm.validatePlacement(spec, placement)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "same-zone")
}

// ============================================================================
// SECTION 9: Deadlock Detection
// ============================================================================

func TestDeadlockDetector_NoCycle(t *testing.T) {
	dd := NewDeadlockDetector()
	dd.AddWaiting("A", "B")
	dd.AddWaiting("B", "C")

	cycle := dd.DetectCycle()
	assert.Nil(t, cycle)
}

func TestDeadlockDetector_SimpleCycle(t *testing.T) {
	dd := NewDeadlockDetector()
	dd.AddWaiting("A", "B")
	dd.AddWaiting("B", "A") // A→B→A cycle

	cycle := dd.DetectCycle()
	require.NotNil(t, cycle)
	assert.Len(t, cycle, 2)
}

func TestDeadlockDetector_ThreeNodeCycle(t *testing.T) {
	dd := NewDeadlockDetector()
	dd.AddWaiting("A", "B")
	dd.AddWaiting("B", "C")
	dd.AddWaiting("C", "A") // A→B→C→A

	cycle := dd.DetectCycle()
	require.NotNil(t, cycle)
}

func TestDeadlockDetector_RemoveGang(t *testing.T) {
	dd := NewDeadlockDetector()
	dd.AddWaiting("A", "B")
	dd.AddWaiting("B", "A")

	dd.RemoveGang("A")

	cycle := dd.DetectCycle()
	assert.Nil(t, cycle)
}

// ============================================================================
// SECTION 10: GetStats
// ============================================================================

func TestGetStats(t *testing.T) {
	gm := NewGangManager(testGangConfig())

	gm.SubmitGang(context.Background(), testGangSpec("g1", 2, 4))
	gm.SubmitGang(context.Background(), testGangSpec("g2", 2, 4))

	stats := gm.GetStats()
	assert.Equal(t, uint64(2), stats["total_submitted"])
	assert.Equal(t, 2, stats["active_gangs"])
	assert.Equal(t, 2, stats["queue_depth"])
}

// ============================================================================
// SECTION 11: GetPendingGangs
// ============================================================================

func TestGetPendingGangs_PriorityOrder(t *testing.T) {
	gm := NewGangManager(testGangConfig())

	s1 := testGangSpec("g-low", 2, 4)
	s1.Priority = 10
	s2 := testGangSpec("g-high", 2, 4)
	s2.Priority = 90

	gm.SubmitGang(context.Background(), s1)
	gm.SubmitGang(context.Background(), s2)

	pending := gm.GetPendingGangs()
	require.Len(t, pending, 2)
	assert.Equal(t, "g-high", pending[0].Spec.GangID) // highest priority first
	assert.Equal(t, "g-low", pending[1].Spec.GangID)
}
