package orchestrator

import (
	"context"
	"testing"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/cluster"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/stretchr/testify/assert"
)

// ============================================================================
// SECTION 1: resolveActualGPUType (pure logic, no mocks needed)
// ============================================================================

func TestResolveActualGPUType_SpecificType(t *testing.T) {
	jc := &JobCoordinator{}
	spec := &common.JobSpec{GPUType: "H100"}
	decision := &cluster.GlobalSchedulingDecision{ClusterID: "c1"}

	result := jc.resolveActualGPUType(spec, decision)
	assert.Equal(t, "H100", result)
}

func TestResolveActualGPUType_AnyFallback(t *testing.T) {
	jc := &JobCoordinator{}
	spec := &common.JobSpec{GPUType: "any"}
	decision := &cluster.GlobalSchedulingDecision{ClusterID: "c1"}

	result := jc.resolveActualGPUType(spec, decision)
	assert.Equal(t, "any", result) // no globalScheduler set, fallback
}

func TestResolveActualGPUType_EmptyFallback(t *testing.T) {
	jc := &JobCoordinator{}
	spec := &common.JobSpec{GPUType: ""}
	decision := &cluster.GlobalSchedulingDecision{ClusterID: "c1"}

	result := jc.resolveActualGPUType(spec, decision)
	assert.Equal(t, "", result)
}

// ============================================================================
// SECTION 2: SchedulingResult Structure
// ============================================================================

func TestSchedulingResult_Fields(t *testing.T) {
	r := &SchedulingResult{
		JobID:      "job-1",
		ClusterID:  "cluster-a",
		NodeID:     "node-1",
		GPUIndices: []int{0, 1, 2, 3},
		LeaseID:    12345,
	}
	assert.Equal(t, "job-1", r.JobID)
	assert.Equal(t, "cluster-a", r.ClusterID)
	assert.Equal(t, "node-1", r.NodeID)
	assert.Equal(t, []int{0, 1, 2, 3}, r.GPUIndices)
	assert.Equal(t, int64(12345), r.LeaseID)
}

// ============================================================================
// SECTION 3: MetricsRecorder Structure
// ============================================================================

func TestMetricsRecorder_Callbacks(t *testing.T) {
	duplicateCount := 0
	leaseCount := 0
	completedSuccess := 0

	mr := &MetricsRecorder{
		OnDuplicateBlocked: func() { duplicateCount++ },
		OnLeaseAcquired:    func() { leaseCount++ },
		OnJobCompleted: func(success bool) {
			if success {
				completedSuccess++
			}
		},
	}

	mr.OnDuplicateBlocked()
	mr.OnDuplicateBlocked()
	mr.OnLeaseAcquired()
	mr.OnJobCompleted(true)

	assert.Equal(t, 2, duplicateCount)
	assert.Equal(t, 1, leaseCount)
	assert.Equal(t, 1, completedSuccess)
}

// ============================================================================
// SECTION 4: NewJobCoordinator
// ============================================================================

func TestNewJobCoordinator(t *testing.T) {
	jc := NewJobCoordinator(nil, nil, nil, nil, nil)
	assert.NotNil(t, jc)
	assert.NotNil(t, jc.log)
}

// ============================================================================
// SECTION 5: ScheduleJob Validation
// ============================================================================

func TestScheduleJob_NilSpec(t *testing.T) {
	jc := NewJobCoordinator(nil, nil, nil, nil, nil)
	_, err := jc.ScheduleJob(context.TODO(), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid job spec")
}

func TestScheduleJob_EmptyRequestID(t *testing.T) {
	jc := NewJobCoordinator(nil, nil, nil, nil, nil)
	_, err := jc.ScheduleJob(context.TODO(), &common.JobSpec{RequestID: ""})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "empty request ID")
}

// ============================================================================
// SECTION 6: contextWithCleanupTimeout
// ============================================================================

func TestContextWithCleanupTimeout(t *testing.T) {
	ctx, cancel := contextWithCleanupTimeout()
	defer cancel()

	assert.NotNil(t, ctx)
	deadline, ok := ctx.Deadline()
	assert.True(t, ok)
	assert.False(t, deadline.IsZero())
}
