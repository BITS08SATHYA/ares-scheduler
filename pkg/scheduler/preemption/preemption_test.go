package preemption

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

func testConfig() *PreemptionConfig {
	return &PreemptionConfig{
		Enabled:                true,
		MinPriorityGap:         20,
		MaxPreemptionsPerHour:  10,
		MaxPreemptionsPerDay:   50,
		GracePeriodSec:         30,
		PreemptiblePriorityMax: 30,
		MinimumAgeSeconds:      60,
		CooldownPeriod:         5 * time.Minute,
	}
}

func runningJob(id string, priority, gpuCount int, age time.Duration) *common.Job {
	return &common.Job{
		ID:     id,
		Status: common.StatusRunning,
		Spec: &common.JobSpec{
			Priority: priority,
			GPUCount: gpuCount,
		},
		StartTime: time.Now().Add(-age),
	}
}

func incomingSpec(priority, gpuCount int) *common.JobSpec {
	return &common.JobSpec{
		RequestID: "req-incoming",
		Priority:  priority,
		GPUCount:  gpuCount,
	}
}

// ============================================================================
// SECTION 1: Constructor
// ============================================================================

func TestNewPreemptionManager_NilConfig(t *testing.T) {
	pm := NewPreemptionManager(nil)
	require.NotNil(t, pm)
	assert.Equal(t, DefaultPreemptionConfig.MinPriorityGap, pm.config.MinPriorityGap)
}

func TestNewPreemptionManager_CustomConfig(t *testing.T) {
	cfg := testConfig()
	cfg.MinPriorityGap = 10
	pm := NewPreemptionManager(cfg)
	assert.Equal(t, 10, pm.config.MinPriorityGap)
}

// ============================================================================
// SECTION 2: Nil & Disabled Cases
// ============================================================================

func TestFindPreemptionVictim_NilIncomingJob(t *testing.T) {
	pm := NewPreemptionManager(testConfig())
	decision := pm.FindPreemptionVictim(context.Background(), nil, nil)
	assert.False(t, decision.ShouldPreempt)
	assert.Contains(t, decision.Reason, "nil")
}

func TestFindPreemptionVictim_Disabled(t *testing.T) {
	cfg := testConfig()
	cfg.Enabled = false
	pm := NewPreemptionManager(cfg)

	decision := pm.FindPreemptionVictim(context.Background(), incomingSpec(90, 4), nil)
	assert.False(t, decision.ShouldPreempt)
	assert.Contains(t, decision.Reason, "disabled")
}

// ============================================================================
// SECTION 3: Candidate Filtering
// ============================================================================

func TestFindPreemptionVictim_NoCandidates(t *testing.T) {
	pm := NewPreemptionManager(testConfig())

	incoming := incomingSpec(90, 4)
	running := []*common.Job{
		// Same priority — not enough gap
		runningJob("j1", 80, 4, 2*time.Hour),
	}

	decision := pm.FindPreemptionVictim(context.Background(), incoming, running)
	assert.False(t, decision.ShouldPreempt)
	assert.Contains(t, decision.Reason, "no preemptible candidates")
}

func TestFindPreemptionVictim_SkipsTooYoung(t *testing.T) {
	pm := NewPreemptionManager(testConfig())

	incoming := incomingSpec(90, 4)
	running := []*common.Job{
		// Priority gap OK (90-10=80 > 20), but only 30s old (< 60s min)
		runningJob("j1", 10, 4, 30*time.Second),
	}

	decision := pm.FindPreemptionVictim(context.Background(), incoming, running)
	assert.False(t, decision.ShouldPreempt)
}

func TestFindPreemptionVictim_SkipsAlreadyPreempted(t *testing.T) {
	pm := NewPreemptionManager(testConfig())

	incoming := incomingSpec(90, 4)
	job := runningJob("j1", 10, 4, 2*time.Hour)
	job.ErrorMsg = "previously preempted by higher priority job"

	decision := pm.FindPreemptionVictim(context.Background(), incoming, []*common.Job{job})
	assert.False(t, decision.ShouldPreempt)
}

func TestFindPreemptionVictim_SkipsNonRunning(t *testing.T) {
	pm := NewPreemptionManager(testConfig())

	incoming := incomingSpec(90, 4)
	job := runningJob("j1", 10, 4, 2*time.Hour)
	job.Status = common.StatusPending

	decision := pm.FindPreemptionVictim(context.Background(), incoming, []*common.Job{job})
	assert.False(t, decision.ShouldPreempt)
}

func TestFindPreemptionVictim_SkipsNilJobs(t *testing.T) {
	pm := NewPreemptionManager(testConfig())

	incoming := incomingSpec(90, 4)
	decision := pm.FindPreemptionVictim(context.Background(), incoming, []*common.Job{nil})
	assert.False(t, decision.ShouldPreempt)
}

// ============================================================================
// SECTION 4: Successful Preemption
// ============================================================================

func TestFindPreemptionVictim_SelectsLowestPriority(t *testing.T) {
	pm := NewPreemptionManager(testConfig())

	incoming := incomingSpec(90, 4)
	running := []*common.Job{
		runningJob("j-high", 60, 4, 2*time.Hour), // gap=30, high priority victim
		runningJob("j-low", 20, 4, 2*time.Hour),  // gap=70, low priority victim
		runningJob("j-mid", 40, 4, 2*time.Hour),  // gap=50, mid priority victim
	}

	decision := pm.FindPreemptionVictim(context.Background(), incoming, running)
	require.True(t, decision.ShouldPreempt)
	assert.Equal(t, "j-low", decision.Victim.JobID)
	assert.Equal(t, 20, decision.VictimPri)
	assert.Equal(t, 90, decision.IncomingPri)
	assert.Equal(t, 70, decision.PriorityGap)
	assert.Equal(t, 30*time.Second, decision.GracePeriod)
}

func TestFindPreemptionVictim_NeedsSufficientGPUs(t *testing.T) {
	pm := NewPreemptionManager(testConfig())

	incoming := incomingSpec(90, 8) // needs 8 GPUs
	running := []*common.Job{
		runningJob("j1", 10, 4, 2*time.Hour), // only 4 GPUs — not enough
	}

	decision := pm.FindPreemptionVictim(context.Background(), incoming, running)
	assert.False(t, decision.ShouldPreempt)
	assert.Contains(t, decision.Reason, "no single victim has enough resources")
}

func TestFindPreemptionVictim_PrefersPreemptiblePriority(t *testing.T) {
	pm := NewPreemptionManager(testConfig())

	incoming := incomingSpec(90, 4)
	running := []*common.Job{
		runningJob("j-normal", 50, 4, 2*time.Hour),      // gap=40, above preemptible max (30)
		runningJob("j-preemptible", 20, 4, 2*time.Hour), // gap=70, below preemptible max → bonus -50
	}

	decision := pm.FindPreemptionVictim(context.Background(), incoming, running)
	require.True(t, decision.ShouldPreempt)
	// j-preemptible should be chosen (lower score due to -50 bonus)
	assert.Equal(t, "j-preemptible", decision.Victim.JobID)
}

// ============================================================================
// SECTION 5: Rate Limiting
// ============================================================================

func TestFindPreemptionVictim_HourlyRateLimit(t *testing.T) {
	cfg := testConfig()
	cfg.MaxPreemptionsPerHour = 2
	pm := NewPreemptionManager(cfg)

	// Record 2 preemptions
	pm.RecordPreemption("in1", "v1", 90, 10, "cluster-1")
	pm.RecordPreemption("in2", "v2", 90, 10, "cluster-1")

	incoming := incomingSpec(90, 4)
	running := []*common.Job{runningJob("j1", 10, 4, 2*time.Hour)}

	decision := pm.FindPreemptionVictim(context.Background(), incoming, running)
	assert.False(t, decision.ShouldPreempt)
	assert.Contains(t, decision.Reason, "rate limit reached")
}

func TestFindPreemptionVictim_DailyRateLimit(t *testing.T) {
	cfg := testConfig()
	cfg.MaxPreemptionsPerHour = 100
	cfg.MaxPreemptionsPerDay = 3
	pm := NewPreemptionManager(cfg)

	for i := 0; i < 3; i++ {
		pm.RecordPreemption("in", "v", 90, 10, "c")
	}

	incoming := incomingSpec(90, 4)
	running := []*common.Job{runningJob("j1", 10, 4, 2*time.Hour)}

	decision := pm.FindPreemptionVictim(context.Background(), incoming, running)
	assert.False(t, decision.ShouldPreempt)
	assert.Contains(t, decision.Reason, "rate limit reached")
}

// ============================================================================
// SECTION 6: Record & History
// ============================================================================

func TestRecordPreemption(t *testing.T) {
	pm := NewPreemptionManager(testConfig())

	pm.RecordPreemption("incoming-1", "victim-1", 90, 10, "cluster-a")

	stats := pm.GetStats()
	assert.Equal(t, uint64(1), stats["preemptions_this_hour"])
	assert.Equal(t, uint64(1), stats["preemptions_today"])
	assert.Equal(t, 1, stats["total_preemptions"])
}

func TestGetRecentPreemptions(t *testing.T) {
	pm := NewPreemptionManager(testConfig())

	pm.RecordPreemption("in1", "v1", 90, 10, "c1")
	pm.RecordPreemption("in2", "v2", 80, 20, "c2")
	pm.RecordPreemption("in3", "v3", 70, 30, "c3")

	recent := pm.GetRecentPreemptions(2)
	assert.Len(t, recent, 2)
	assert.Equal(t, "in2", recent[0].IncomingJobID)
	assert.Equal(t, "in3", recent[1].IncomingJobID)
}

func TestGetRecentPreemptions_LargerThanHistory(t *testing.T) {
	pm := NewPreemptionManager(testConfig())
	pm.RecordPreemption("in1", "v1", 90, 10, "c1")

	recent := pm.GetRecentPreemptions(100)
	assert.Len(t, recent, 1)
}

func TestGetRecentPreemptions_ZeroLimit(t *testing.T) {
	pm := NewPreemptionManager(testConfig())
	pm.RecordPreemption("in1", "v1", 90, 10, "c1")

	recent := pm.GetRecentPreemptions(0)
	assert.Len(t, recent, 1) // returns all
}

// ============================================================================
// SECTION 7: Scoring
// ============================================================================

func TestScoreCandidate_LowerPriorityBetterVictim(t *testing.T) {
	pm := NewPreemptionManager(testConfig())
	incoming := incomingSpec(90, 4)

	lowPriJob := &common.Job{
		Spec: &common.JobSpec{Priority: 10, GPUCount: 4},
	}
	highPriJob := &common.Job{
		Spec: &common.JobSpec{Priority: 60, GPUCount: 4},
	}

	lowScore := pm.scoreCandidate(lowPriJob, incoming, 30*time.Minute)
	highScore := pm.scoreCandidate(highPriJob, incoming, 30*time.Minute)

	assert.Less(t, lowScore, highScore, "lower priority job should have lower (better) score")
}

func TestScoreCandidate_PreemptibleBonus(t *testing.T) {
	pm := NewPreemptionManager(testConfig())
	incoming := incomingSpec(90, 4)

	preemptible := &common.Job{
		Spec: &common.JobSpec{Priority: 25, GPUCount: 4}, // below PreemptiblePriorityMax=30
	}
	normal := &common.Job{
		Spec: &common.JobSpec{Priority: 35, GPUCount: 4}, // above
	}

	preemptibleScore := pm.scoreCandidate(preemptible, incoming, 30*time.Minute)
	normalScore := pm.scoreCandidate(normal, incoming, 30*time.Minute)

	assert.Less(t, preemptibleScore, normalScore, "preemptible job should score lower")
}

func TestScoreCandidate_AgePenalty(t *testing.T) {
	pm := NewPreemptionManager(testConfig())
	incoming := incomingSpec(90, 4)
	job := &common.Job{
		Spec: &common.JobSpec{Priority: 20, GPUCount: 4},
	}

	youngScore := pm.scoreCandidate(job, incoming, 30*time.Minute)
	oldScore := pm.scoreCandidate(job, incoming, 2*time.Hour)
	veryOldScore := pm.scoreCandidate(job, incoming, 4*time.Hour)

	assert.Less(t, youngScore, oldScore, "old jobs should have higher score (less desirable to evict)")
	assert.Less(t, oldScore, veryOldScore, "very old jobs should have even higher score")
}

func TestScoreCandidate_ExactGPUMatch(t *testing.T) {
	pm := NewPreemptionManager(testConfig())
	incoming := incomingSpec(90, 4)

	exactMatch := &common.Job{
		Spec: &common.JobSpec{Priority: 20, GPUCount: 4}, // exact match
	}
	overMatch := &common.Job{
		Spec: &common.JobSpec{Priority: 20, GPUCount: 8}, // frees more than needed
	}

	exactScore := pm.scoreCandidate(exactMatch, incoming, 30*time.Minute)
	overScore := pm.scoreCandidate(overMatch, incoming, 30*time.Minute)

	assert.Less(t, exactScore, overScore, "exact GPU match should score lower (better)")
}

// ============================================================================
// SECTION 8: GetStats
// ============================================================================

func TestGetStats(t *testing.T) {
	pm := NewPreemptionManager(testConfig())

	stats := pm.GetStats()
	assert.Equal(t, true, stats["enabled"])
	assert.Equal(t, uint64(0), stats["preemptions_this_hour"])
	assert.Equal(t, uint64(0), stats["preemptions_today"])
	assert.Equal(t, 10, stats["max_per_hour"])
	assert.Equal(t, 50, stats["max_per_day"])
	assert.Equal(t, 20, stats["min_priority_gap"])
	assert.Equal(t, 30, stats["grace_period_sec"])
	assert.Equal(t, 30, stats["preemptible_max_pri"])
	assert.Equal(t, 0, stats["total_preemptions"])
}
