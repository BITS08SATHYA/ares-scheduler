package job

import (
	"testing"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Helpers
// ============================================================================

func validSpec() *common.JobSpec {
	return &common.JobSpec{
		RequestID:      "req-123",
		Name:           "test-job",
		Image:          "nvidia/cuda:12.0",
		GPUCount:       4,
		Priority:       50,
		TimeoutSecs:    600,
		MaxRetries:     3,
		TargetLatencyMs: 1000,
		TenantID:       "tenant-a",
	}
}

func pendingJob() *common.Job {
	return &common.Job{
		ID:         "job-1",
		Spec:       validSpec(),
		Status:     common.StatusPending,
		SubmitTime: time.Now(),
	}
}

func scheduledJob() *common.Job {
	j := pendingJob()
	j.Status = common.StatusScheduled
	j.ClusterID = "cluster-1"
	j.NodeID = "node-1"
	j.ScheduleTime = time.Now()
	return j
}

func runningJob() *common.Job {
	j := scheduledJob()
	j.Status = common.StatusRunning
	j.PodName = "ares-job-1-0"
	j.AllocatedGPUIndices = []int{0, 1, 2, 3}
	j.StartTime = time.Now()
	return j
}

// ============================================================================
// SECTION 1: JobFactory & CreateJob
// ============================================================================

func TestNewJobFactory(t *testing.T) {
	jf := NewJobFactory()
	require.NotNil(t, jf)
}

func TestCreateJob_Success(t *testing.T) {
	jf := NewJobFactory()
	job, err := jf.CreateJob(validSpec())

	require.NoError(t, err)
	require.NotNil(t, job)
	assert.NotEmpty(t, job.ID)
	assert.Equal(t, common.StatusPending, job.Status)
	assert.Equal(t, 0, job.Attempts)
	assert.NotNil(t, job.Metrics)
	assert.False(t, job.SubmitTime.IsZero())
	assert.Equal(t, "test-job", job.Spec.Name)
}

func TestCreateJob_NilSpec(t *testing.T) {
	jf := NewJobFactory()
	_, err := jf.CreateJob(nil)
	assert.Error(t, err)
}

func TestCreateJob_EmptyName(t *testing.T) {
	jf := NewJobFactory()
	spec := validSpec()
	spec.Name = ""
	_, err := jf.CreateJob(spec)
	assert.Error(t, err)
}

func TestCreateJob_EmptyImage(t *testing.T) {
	jf := NewJobFactory()
	spec := validSpec()
	spec.Image = ""
	_, err := jf.CreateJob(spec)
	assert.Error(t, err)
}

func TestCreateJob_NegativeGPU(t *testing.T) {
	jf := NewJobFactory()
	spec := validSpec()
	spec.GPUCount = -1
	_, err := jf.CreateJob(spec)
	assert.Error(t, err)
}

func TestCreateJob_InvalidPriority(t *testing.T) {
	jf := NewJobFactory()

	spec := validSpec()
	spec.Priority = -1
	_, err := jf.CreateJob(spec)
	assert.Error(t, err)

	spec.Priority = 101
	_, err = jf.CreateJob(spec)
	assert.Error(t, err)
}

func TestCreateJob_ZeroTimeout(t *testing.T) {
	jf := NewJobFactory()
	spec := validSpec()
	spec.TimeoutSecs = 0
	_, err := jf.CreateJob(spec)
	assert.Error(t, err)
}

func TestCreateJob_NegativeRetries(t *testing.T) {
	jf := NewJobFactory()
	spec := validSpec()
	spec.MaxRetries = -1
	_, err := jf.CreateJob(spec)
	assert.Error(t, err)
}

func TestCreateJob_ZeroTargetLatency(t *testing.T) {
	jf := NewJobFactory()
	spec := validSpec()
	spec.TargetLatencyMs = 0
	_, err := jf.CreateJob(spec)
	assert.Error(t, err)
}

// ============================================================================
// SECTION 2: State Transitions
// ============================================================================

func TestTransitionToPending(t *testing.T) {
	job := scheduledJob()
	err := TransitionToPending(job)
	assert.NoError(t, err)
	assert.Equal(t, common.StatusPending, job.Status)
}

func TestTransitionToPending_AlreadyPending(t *testing.T) {
	job := pendingJob()
	err := TransitionToPending(job)
	assert.NoError(t, err)
}

func TestTransitionToPending_NilJob(t *testing.T) {
	err := TransitionToPending(nil)
	assert.Error(t, err)
}

func TestTransitionToScheduled(t *testing.T) {
	job := pendingJob()
	err := TransitionToScheduled(job, "cluster-us-west", "node-001")

	assert.NoError(t, err)
	assert.Equal(t, common.StatusScheduled, job.Status)
	assert.Equal(t, "cluster-us-west", job.ClusterID)
	assert.Equal(t, "node-001", job.NodeID)
	assert.False(t, job.ScheduleTime.IsZero())
}

func TestTransitionToScheduled_NotPending(t *testing.T) {
	job := runningJob()
	err := TransitionToScheduled(job, "c", "n")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "PENDING")
}

func TestTransitionToScheduled_NilJob(t *testing.T) {
	err := TransitionToScheduled(nil, "c", "n")
	assert.Error(t, err)
}

func TestTransitionToRunning(t *testing.T) {
	job := scheduledJob()
	err := TransitionToRunning(job, "pod-1", []int{0, 1}, 12345, "token-abc")

	assert.NoError(t, err)
	assert.Equal(t, common.StatusRunning, job.Status)
	assert.Equal(t, "pod-1", job.PodName)
	assert.Equal(t, []int{0, 1}, job.AllocatedGPUIndices)
	assert.Equal(t, "token-abc", job.ExecutionToken)
	assert.NotNil(t, job.ExecutionLease)
	assert.False(t, job.StartTime.IsZero())
}

func TestTransitionToRunning_NotScheduled(t *testing.T) {
	job := pendingJob()
	err := TransitionToRunning(job, "p", nil, 0, "t")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SCHEDULED")
}

func TestTransitionToRunning_NilJob(t *testing.T) {
	err := TransitionToRunning(nil, "p", nil, 0, "t")
	assert.Error(t, err)
}

func TestTransitionToSucceeded(t *testing.T) {
	job := runningJob()
	err := TransitionToSucceeded(job, 0)

	assert.NoError(t, err)
	assert.Equal(t, common.StatusSucceeded, job.Status)
	assert.Equal(t, 0, job.ExitCode)
	assert.False(t, job.EndTime.IsZero())
}

func TestTransitionToSucceeded_NotRunning(t *testing.T) {
	job := pendingJob()
	err := TransitionToSucceeded(job, 0)
	assert.Error(t, err)
}

func TestTransitionToSucceeded_NilJob(t *testing.T) {
	err := TransitionToSucceeded(nil, 0)
	assert.Error(t, err)
}

func TestTransitionToFailed_WithRetry(t *testing.T) {
	job := runningJob()
	job.Spec.MaxRetries = 3

	err := TransitionToFailed(job, "OOM killed")
	assert.NoError(t, err)
	// Should transition back to PENDING for retry
	assert.Equal(t, common.StatusPending, job.Status)
	assert.Equal(t, 1, job.Attempts)
	assert.False(t, job.NextRetryAt.IsZero())
	assert.True(t, job.ScheduleTime.IsZero()) // reset
	assert.True(t, job.StartTime.IsZero())    // reset
}

func TestTransitionToFailed_NoMoreRetries(t *testing.T) {
	job := runningJob()
	job.Spec.MaxRetries = 1
	job.Attempts = 1 // already at max

	err := TransitionToFailed(job, "GPU error")
	assert.NoError(t, err)
	assert.Equal(t, common.StatusFailed, job.Status)
	assert.Equal(t, "GPU error", job.ErrorMsg)
}

func TestTransitionToFailed_NotRunning(t *testing.T) {
	job := pendingJob()
	err := TransitionToFailed(job, "error")
	assert.Error(t, err)
}

func TestTransitionToFailed_NilJob(t *testing.T) {
	err := TransitionToFailed(nil, "error")
	assert.Error(t, err)
}

// ============================================================================
// SECTION 3: Backoff Calculation
// ============================================================================

func TestCalculateNextRetryTime(t *testing.T) {
	d0 := CalculateNextRetryTime(0) // ~1s
	d1 := CalculateNextRetryTime(1) // ~2s
	d2 := CalculateNextRetryTime(2) // ~4s
	d3 := CalculateNextRetryTime(3) // ~8s

	// With ±10% jitter, check ranges
	assert.InDelta(t, 1.0, d0.Seconds(), 0.2)
	assert.InDelta(t, 2.0, d1.Seconds(), 0.3)
	assert.InDelta(t, 4.0, d2.Seconds(), 0.5)
	assert.InDelta(t, 8.0, d3.Seconds(), 1.0)
}

func TestCalculateNextRetryTime_Capped(t *testing.T) {
	d := CalculateNextRetryTime(100) // should be capped at 5 minutes
	assert.LessOrEqual(t, d, 5*time.Minute+30*time.Second)
}

// ============================================================================
// SECTION 4: Query Helpers
// ============================================================================

func TestIsJobRunning(t *testing.T) {
	assert.True(t, IsJobRunning(runningJob()))
	assert.False(t, IsJobRunning(pendingJob()))
	assert.False(t, IsJobRunning(nil))
}

func TestIsJobCompleted(t *testing.T) {
	succeeded := runningJob()
	succeeded.Status = common.StatusSucceeded
	assert.True(t, IsJobCompleted(succeeded))

	failed := runningJob()
	failed.Status = common.StatusFailed
	assert.True(t, IsJobCompleted(failed))

	assert.False(t, IsJobCompleted(runningJob()))
	assert.False(t, IsJobCompleted(nil))
}

func TestIsJobPending(t *testing.T) {
	assert.True(t, IsJobPending(pendingJob()))
	assert.False(t, IsJobPending(runningJob()))
	assert.False(t, IsJobPending(nil))
}

func TestGetJobDuration(t *testing.T) {
	job := runningJob()
	job.StartTime = time.Now().Add(-10 * time.Second)
	job.EndTime = time.Now()

	d := GetJobDuration(job)
	assert.InDelta(t, 10.0, d.Seconds(), 0.1)
}

func TestGetJobDuration_ZeroEndTime(t *testing.T) {
	job := runningJob()
	assert.Equal(t, time.Duration(0), GetJobDuration(job))
}

func TestGetJobDuration_NilJob(t *testing.T) {
	assert.Equal(t, time.Duration(0), GetJobDuration(nil))
}

func TestGetJobTotalLatency(t *testing.T) {
	job := runningJob()
	job.SubmitTime = time.Now().Add(-30 * time.Second)
	job.EndTime = time.Now()

	d := GetJobTotalLatency(job)
	assert.InDelta(t, 30.0, d.Seconds(), 0.1)
}

func TestGetJobElapsedTime(t *testing.T) {
	job := runningJob()
	job.StartTime = time.Now().Add(-5 * time.Second)

	elapsed := GetJobElapsedTime(job)
	assert.InDelta(t, 5.0, elapsed.Seconds(), 0.2)
}

func TestGetJobElapsedTime_NotRunning(t *testing.T) {
	assert.Equal(t, time.Duration(0), GetJobElapsedTime(pendingJob()))
}

func TestIsJobWithinTimeout(t *testing.T) {
	job := runningJob()
	job.Spec.TimeoutSecs = 600
	job.StartTime = time.Now().Add(-5 * time.Second)
	assert.True(t, IsJobWithinTimeout(job))
}

func TestIsJobWithinTimeout_Expired(t *testing.T) {
	job := runningJob()
	job.Spec.TimeoutSecs = 10
	job.StartTime = time.Now().Add(-20 * time.Second)
	assert.False(t, IsJobWithinTimeout(job))
}

func TestShouldJobRetry(t *testing.T) {
	job := runningJob()
	job.Status = common.StatusFailed
	job.Spec.MaxRetries = 3
	job.Attempts = 1
	job.NextRetryAt = time.Now().Add(-1 * time.Second) // past retry time

	assert.True(t, ShouldJobRetry(job))
}

func TestShouldJobRetry_NoRetriesLeft(t *testing.T) {
	job := runningJob()
	job.Status = common.StatusFailed
	job.Spec.MaxRetries = 3
	job.Attempts = 3

	assert.False(t, ShouldJobRetry(job))
}

func TestShouldJobRetry_FutureRetryTime(t *testing.T) {
	job := runningJob()
	job.Status = common.StatusFailed
	job.Spec.MaxRetries = 3
	job.Attempts = 1
	job.NextRetryAt = time.Now().Add(1 * time.Hour)

	assert.False(t, ShouldJobRetry(job))
}

func TestCanJobBeScheduled(t *testing.T) {
	assert.True(t, CanJobBeScheduled(pendingJob()))
	assert.False(t, CanJobBeScheduled(runningJob()))
	assert.False(t, CanJobBeScheduled(nil))
}

func TestCanJobBeExecuted(t *testing.T) {
	assert.True(t, CanJobBeExecuted(scheduledJob()))
	assert.False(t, CanJobBeExecuted(pendingJob()))
	assert.False(t, CanJobBeExecuted(nil))
}

func TestCanJobBeCancelled(t *testing.T) {
	assert.True(t, CanJobBeCancelled(pendingJob()))
	assert.True(t, CanJobBeCancelled(scheduledJob()))
	assert.False(t, CanJobBeCancelled(runningJob()))
	assert.False(t, CanJobBeCancelled(nil))
}

// ============================================================================
// SECTION 5: Metrics Helpers
// ============================================================================

func TestUpdateJobMetric(t *testing.T) {
	job := pendingJob()
	err := UpdateJobMetric(job, "gpu_util", 85.5)
	assert.NoError(t, err)

	val, exists := GetJobMetric(job, "gpu_util")
	assert.True(t, exists)
	assert.Equal(t, 85.5, val)
}

func TestUpdateJobMetric_NilJob(t *testing.T) {
	assert.Error(t, UpdateJobMetric(nil, "k", "v"))
}

func TestUpdateJobMetric_NilMetrics(t *testing.T) {
	job := &common.Job{ID: "j1"}
	job.Metrics = nil
	err := UpdateJobMetric(job, "k", "v")
	assert.NoError(t, err)
	assert.NotNil(t, job.Metrics)
}

func TestGetJobMetric_NotFound(t *testing.T) {
	job := pendingJob()
	_, exists := GetJobMetric(job, "nonexistent")
	assert.False(t, exists)
}

func TestGetJobMetric_NilJob(t *testing.T) {
	_, exists := GetJobMetric(nil, "k")
	assert.False(t, exists)
}

func TestSetJobMetrics(t *testing.T) {
	job := pendingJob()
	m := map[string]interface{}{"a": 1, "b": 2}
	err := SetJobMetrics(job, m)
	assert.NoError(t, err)
	assert.Equal(t, m, job.Metrics)
}

func TestSetJobMetrics_NilJob(t *testing.T) {
	assert.Error(t, SetJobMetrics(nil, nil))
}

// ============================================================================
// SECTION 6: Utility Functions
// ============================================================================

func TestGetJobStatusString(t *testing.T) {
	assert.Equal(t, "PENDING", GetJobStatusString(pendingJob()))
	assert.Equal(t, "RUNNING", GetJobStatusString(runningJob()))
	assert.Equal(t, "UNKNOWN", GetJobStatusString(nil))
}

func TestCompareJobStatus(t *testing.T) {
	assert.True(t, CompareJobStatus(pendingJob(), common.StatusPending))
	assert.False(t, CompareJobStatus(pendingJob(), common.StatusRunning))
	assert.False(t, CompareJobStatus(nil, common.StatusPending))
}

func TestGenerateJobID(t *testing.T) {
	id1 := generateJobID()
	assert.NotEmpty(t, id1)
	assert.Contains(t, id1, "job-")
}
