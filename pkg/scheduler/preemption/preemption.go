// File: pkg/scheduler/preemption/preemption.go
// Feature: Priority & Preemption
//
// When a high-priority job can't be scheduled (no resources), it can evict
// a lower-priority running job to claim its resources.
//
// Rules:
// 1. Only preempt if incoming priority > victim priority (by at least MinPriorityGap)
// 2. Prefer evicting the LOWEST priority job
// 3. Respect rate limits (max preemptions per hour/day)
// 4. Grace period: victim gets time to checkpoint before kill
// 5. Jobs below PreemptiblePriorityMax are always preemptible
// 6. Never preempt a job that's already been preempted (avoid cascade)

package preemption

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
)

// ============================================================================
// PREEMPTION MANAGER
// ============================================================================

type PreemptionManager struct {
	mu     sync.RWMutex
	log    *logger.Logger
	config *PreemptionConfig

	// Rate limiting
	preemptionsThisHour uint64
	preemptionsToday    uint64
	lastHourReset       time.Time
	lastDayReset        time.Time

	// History (for audit trail)
	history []PreemptionEvent
}

// PreemptionConfig: Configuration
type PreemptionConfig struct {
	Enabled                bool
	MinPriorityGap         int           // Minimum priority difference to preempt (default: 20)
	MaxPreemptionsPerHour  int           // Rate limit per hour (default: 10)
	MaxPreemptionsPerDay   int           // Rate limit per day (default: 50)
	GracePeriodSec         int           // Seconds before kill (default: 30)
	PreemptiblePriorityMax int           // Jobs at or below this priority are always preemptible (default: 30)
	MinimumAgeSeconds      int           // Don't preempt jobs younger than this (default: 60)
	CooldownPeriod         time.Duration // Time between preemptions for same tenant (default: 5m)
}

// DefaultPreemptionConfig: Sensible defaults
var DefaultPreemptionConfig = &PreemptionConfig{
	Enabled:                true,
	MinPriorityGap:         20,
	MaxPreemptionsPerHour:  10,
	MaxPreemptionsPerDay:   50,
	GracePeriodSec:         30,
	PreemptiblePriorityMax: 30,
	MinimumAgeSeconds:      60,
	CooldownPeriod:         5 * time.Minute,
}

// PreemptionCandidate: A running job that could be evicted
type PreemptionCandidate struct {
	JobID     string
	TenantID  string
	Priority  int
	GPUCount  int
	MemoryMB  int
	StartTime time.Time
	Age       time.Duration
	Score     float64 // Lower = better victim (more likely to be evicted)
}

// PreemptionDecision: Result of preemption analysis
type PreemptionDecision struct {
	ShouldPreempt bool
	Victim        *PreemptionCandidate // Job to evict (nil if no preemption)
	Reason        string
	IncomingJobID string
	IncomingPri   int
	VictimPri     int
	PriorityGap   int
	GracePeriod   time.Duration
}

// PreemptionEvent: Audit record of a preemption
type PreemptionEvent struct {
	Timestamp     time.Time
	IncomingJobID string
	VictimJobID   string
	IncomingPri   int
	VictimPri     int
	PriorityGap   int
	Reason        string
	ClusterID     string
}

// ============================================================================
// CONSTRUCTOR
// ============================================================================

func NewPreemptionManager(config *PreemptionConfig) *PreemptionManager {
	if config == nil {
		config = DefaultPreemptionConfig
	}

	now := time.Now()
	return &PreemptionManager{
		log:           logger.Get(),
		config:        config,
		lastHourReset: now,
		lastDayReset:  now,
		history:       make([]PreemptionEvent, 0),
	}
}

// ============================================================================
// CORE PREEMPTION LOGIC
// ============================================================================

// FindPreemptionVictim: Given a high-priority job that can't be scheduled,
// find the best running job to evict.
//
// Algorithm:
// 1. Filter running jobs to only preemptible candidates
// 2. Score each candidate (lower score = better victim)
// 3. Pick the candidate that frees enough resources with lowest impact
//
// Parameters:
//   - incomingJob: the job that needs resources
//   - runningJobs: all currently running jobs on the target cluster
//
// Returns PreemptionDecision with victim (or nil if no preemption possible)
func (pm *PreemptionManager) FindPreemptionVictim(
	ctx context.Context,
	incomingJob *common.JobSpec,
	runningJobs []*common.Job,
) *PreemptionDecision {

	if !pm.config.Enabled {
		return &PreemptionDecision{
			ShouldPreempt: false,
			Reason:        "preemption disabled",
		}
	}

	// Rate limit check
	if !pm.checkRateLimits() {
		return &PreemptionDecision{
			ShouldPreempt: false,
			Reason: fmt.Sprintf("rate limit reached (hour=%d/%d, day=%d/%d)",
				atomic.LoadUint64(&pm.preemptionsThisHour), pm.config.MaxPreemptionsPerHour,
				atomic.LoadUint64(&pm.preemptionsToday), pm.config.MaxPreemptionsPerDay),
		}
	}

	// Build candidate list
	candidates := pm.buildCandidates(incomingJob, runningJobs)

	if len(candidates) == 0 {
		return &PreemptionDecision{
			ShouldPreempt: false,
			IncomingJobID: "",
			IncomingPri:   incomingJob.Priority,
			Reason:        "no preemptible candidates found",
		}
	}

	// Sort by score (lowest = best victim)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Score < candidates[j].Score
	})

	// Pick the best victim that frees enough resources
	for _, candidate := range candidates {
		if candidate.GPUCount >= incomingJob.GPUCount {
			gap := incomingJob.Priority - candidate.Priority
			return &PreemptionDecision{
				ShouldPreempt: true,
				Victim:        &candidate,
				IncomingPri:   incomingJob.Priority,
				VictimPri:     candidate.Priority,
				PriorityGap:   gap,
				GracePeriod:   time.Duration(pm.config.GracePeriodSec) * time.Second,
				Reason: fmt.Sprintf("preempting job %s (pri=%d) for incoming (pri=%d), gap=%d",
					candidate.JobID, candidate.Priority, incomingJob.Priority, gap),
			}
		}
	}

	// No single victim has enough GPUs — try combining victims
	// (simplified: just report no single victim found)
	return &PreemptionDecision{
		ShouldPreempt: false,
		IncomingPri:   incomingJob.Priority,
		Reason:        "no single victim has enough resources",
	}
}

// buildCandidates: Filter running jobs into preemptible candidates
func (pm *PreemptionManager) buildCandidates(
	incomingJob *common.JobSpec,
	runningJobs []*common.Job,
) []PreemptionCandidate {

	candidates := make([]PreemptionCandidate, 0)
	now := time.Now()

	for _, job := range runningJobs {
		// Skip non-running jobs
		if job.Status != common.StatusRunning {
			continue
		}

		// Rule 1: Must have lower priority than incoming
		priorityGap := incomingJob.Priority - job.Spec.Priority
		if priorityGap < pm.config.MinPriorityGap {
			continue
		}

		// Rule 2: Must be old enough (don't preempt jobs that just started)
		age := now.Sub(job.StartTime)
		if age.Seconds() < float64(pm.config.MinimumAgeSeconds) {
			continue
		}

		// Rule 3: Don't preempt jobs that were already preempted
		if job.ErrorMsg == "preempted by higher priority job" {
			continue
		}

		// Score the candidate (lower = better victim to evict)
		score := pm.scoreCandidate(job, incomingJob, age)

		candidates = append(candidates, PreemptionCandidate{
			JobID:     job.ID,
			TenantID:  job.Spec.TenantID,
			Priority:  job.Spec.Priority,
			GPUCount:  job.Spec.GPUCount,
			MemoryMB:  job.Spec.MemoryMB,
			StartTime: job.StartTime,
			Age:       age,
			Score:     score,
		})
	}

	pm.log.Debug("Preemption candidates: %d out of %d running jobs", len(candidates), len(runningJobs))
	return candidates
}

// scoreCandidate: Score a potential victim (lower = better to evict)
//
// Scoring factors:
// - Lower priority → lower score (evict low-priority first)
// - Always-preemptible jobs → lower score
// - More GPUs freed → lower score (prefer freeing more resources)
// - Older jobs → slightly higher score (prefer evicting newer jobs)
func (pm *PreemptionManager) scoreCandidate(
	job *common.Job,
	incomingJob *common.JobSpec,
	age time.Duration,
) float64 {

	score := 0.0

	// Priority component (0-100, lower priority = lower score = better victim)
	score += float64(job.Spec.Priority)

	// Always-preemptible bonus (jobs below threshold are ideal victims)
	if job.Spec.Priority <= pm.config.PreemptiblePriorityMax {
		score -= 50.0
	}

	// Resource match bonus (victim frees exactly what we need = ideal)
	gpuDiff := job.Spec.GPUCount - incomingJob.GPUCount
	if gpuDiff == 0 {
		score -= 20.0 // Perfect match
	} else if gpuDiff > 0 {
		score -= 10.0 // Frees more than needed (slight waste)
	}

	// Age penalty (prefer evicting newer jobs over long-running ones)
	ageMinutes := age.Minutes()
	if ageMinutes > 60 {
		score += 15.0 // Penalize evicting jobs that ran > 1 hour
	}
	if ageMinutes > 180 {
		score += 25.0 // Strongly penalize evicting jobs > 3 hours
	}

	return score
}

// ============================================================================
// RATE LIMITING
// ============================================================================

func (pm *PreemptionManager) checkRateLimits() bool {
	now := time.Now()

	// Reset hourly counter
	if now.Sub(pm.lastHourReset) > time.Hour {
		atomic.StoreUint64(&pm.preemptionsThisHour, 0)
		pm.lastHourReset = now
	}

	// Reset daily counter
	if now.Sub(pm.lastDayReset) > 24*time.Hour {
		atomic.StoreUint64(&pm.preemptionsToday, 0)
		pm.lastDayReset = now
	}

	hourly := atomic.LoadUint64(&pm.preemptionsThisHour)
	daily := atomic.LoadUint64(&pm.preemptionsToday)

	if int(hourly) >= pm.config.MaxPreemptionsPerHour {
		pm.log.Warn("Preemption hourly rate limit reached (%d/%d)",
			hourly, pm.config.MaxPreemptionsPerHour)
		return false
	}

	if int(daily) >= pm.config.MaxPreemptionsPerDay {
		pm.log.Warn("Preemption daily rate limit reached (%d/%d)",
			daily, pm.config.MaxPreemptionsPerDay)
		return false
	}

	return true
}

// ============================================================================
// PREEMPTION EXECUTION
// ============================================================================

// RecordPreemption: Record that a preemption happened (for rate limiting + audit)
func (pm *PreemptionManager) RecordPreemption(
	incomingJobID string,
	victimJobID string,
	incomingPri int,
	victimPri int,
	clusterID string,
) {
	atomic.AddUint64(&pm.preemptionsThisHour, 1)
	atomic.AddUint64(&pm.preemptionsToday, 1)

	event := PreemptionEvent{
		Timestamp:     time.Now(),
		IncomingJobID: incomingJobID,
		VictimJobID:   victimJobID,
		IncomingPri:   incomingPri,
		VictimPri:     victimPri,
		PriorityGap:   incomingPri - victimPri,
		Reason:        "priority preemption",
		ClusterID:     clusterID,
	}

	pm.mu.Lock()
	pm.history = append(pm.history, event)
	// Keep last 1000 events
	if len(pm.history) > 1000 {
		pm.history = pm.history[len(pm.history)-1000:]
	}
	pm.mu.Unlock()

	pm.log.Info("PREEMPTION RECORDED: job %s (pri=%d) evicted job %s (pri=%d) on cluster %s",
		incomingJobID, incomingPri, victimJobID, victimPri, clusterID)
}

// ============================================================================
// STATS & MONITORING
// ============================================================================

// GetStats: Get preemption statistics
func (pm *PreemptionManager) GetStats() map[string]interface{} {
	pm.mu.RLock()
	historyLen := len(pm.history)
	pm.mu.RUnlock()

	return map[string]interface{}{
		"enabled":               pm.config.Enabled,
		"preemptions_this_hour": atomic.LoadUint64(&pm.preemptionsThisHour),
		"preemptions_today":     atomic.LoadUint64(&pm.preemptionsToday),
		"max_per_hour":          pm.config.MaxPreemptionsPerHour,
		"max_per_day":           pm.config.MaxPreemptionsPerDay,
		"min_priority_gap":      pm.config.MinPriorityGap,
		"grace_period_sec":      pm.config.GracePeriodSec,
		"preemptible_max_pri":   pm.config.PreemptiblePriorityMax,
		"total_preemptions":     historyLen,
	}
}

// GetRecentPreemptions: Get recent preemption events (for dashboard)
func (pm *PreemptionManager) GetRecentPreemptions(limit int) []PreemptionEvent {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if limit <= 0 || limit > len(pm.history) {
		limit = len(pm.history)
	}

	// Return most recent
	start := len(pm.history) - limit
	if start < 0 {
		start = 0
	}

	result := make([]PreemptionEvent, limit)
	copy(result, pm.history[start:])
	return result
}
