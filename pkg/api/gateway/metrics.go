// File: pkg/api/gateway/metrics.go
// Prometheus metrics for Ares Scheduler
// Covers all 26 features across 7 subsystems:
//   1. HTTP API       - request rate, errors, latency
//   2. Scheduling     - jobs scheduled, placement decisions, latency
//   3. Clusters       - health, capacity, heartbeats
//   4. GPU Topology   - NVLink placements, NUMA hits, topology scores
//   5. Reliability    - retries, preemptions, checkpoints, lease activity
//   6. DRF Fairness   - tenant shares, fairness index, quota denials
//   7. CRDT Sync      - merge count, conflicts, replication lag

package gateway

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ============================================================================
// METRICS STRUCT
// ============================================================================

type Metrics struct {
	mu sync.RWMutex

	// ====================================================================
	// 1. HTTP API METRICS
	// ====================================================================
	TotalRequests   uint64 // Total HTTP requests
	TotalErrors     uint64 // Total HTTP errors
	RequestDuration int64  // Total request duration (nanoseconds)

	// Per-endpoint counters
	ScheduleRequests uint64 // POST /schedule
	StatusRequests   uint64 // GET /status/job
	HealthRequests   uint64 // GET /health
	CancelRequests   uint64 // POST /job/cancel
	RetryRequests    uint64 // POST /job/retry
	ListRequests     uint64 // GET /jobs

	// ====================================================================
	// 2. SCHEDULING METRICS
	// ====================================================================
	TotalScheduled       uint64 // Jobs successfully scheduled
	TotalFailed          uint64 // Jobs that failed to schedule
	ActiveJobs           int32  // Currently running jobs
	QueuedJobs           int32  // Jobs waiting in queue
	CompletedJobs        uint64 // Total completed (succeeded + failed)
	SucceededJobs        uint64 // Jobs that succeeded
	SchedulingLatencySum int64  // Sum of scheduling latency (nanoseconds)
	SchedulingLatencyMax int64  // Max scheduling latency (nanoseconds)

	// Per-priority counters
	HighPriorityJobs   uint64 // Priority >= 80
	MediumPriorityJobs uint64 // Priority 40-79
	LowPriorityJobs    uint64 // Priority < 40

	// Per-region counters
	RegionScheduled sync.Map // region -> uint64

	// ====================================================================
	// 3. CLUSTER METRICS
	// ====================================================================
	TotalClusters     int32  // Total registered clusters
	HealthyClusters   int32  // Currently healthy clusters
	UnhealthyClusters int32  // Currently unhealthy clusters
	HeartbeatsRecv    uint64 // Total heartbeats received
	HeartbeatsMissed  uint64 // Missed heartbeats (stale clusters detected)
	ClusterJoins      uint64 // Cluster join events
	ClusterLeaves     uint64 // Cluster leave events
	ClusterFailovers  uint64 // Cluster failover events (watchdog triggered)

	// ====================================================================
	// 4. GPU TOPOLOGY METRICS
	// ====================================================================
	TotalGPUsManaged    int32  // Total GPUs across all clusters
	GPUsInUse           int32  // GPUs currently allocated
	GPUsAvailable       int32  // GPUs currently free
	NVLinkPlacements    uint64 // Jobs placed on NVLink-connected GPUs
	PCIePlacements      uint64 // Jobs placed on PCIe-connected GPUs
	SameNUMAPlacements  uint64 // Jobs placed on same-NUMA GPUs
	CrossNUMAPlacements uint64 // Jobs placed across NUMA nodes
	TopologyScoreSum    int64  // Sum of topology scores (for averaging)
	TopologyScoreCount  uint64 // Count of topology scores

	// Per-GPU-type counters
	A100Scheduled uint64
	H100Scheduled uint64
	V100Scheduled uint64
	T4Scheduled   uint64
	OtherGPU      uint64

	// ====================================================================
	// 5. RELIABILITY METRICS
	// ====================================================================

	// Exactly-once (Feature 6)
	DuplicateRequestsBlocked uint64 // Idempotency dedup hits
	FencingTokensIssued      uint64 // Fencing tokens generated
	FencingTokensValidated   uint64 // Fencing token checks passed
	FencingTokensRejected    uint64 // Fencing token checks failed (stale)

	// Leases (Feature 19)
	LeasesAcquired uint64 // Leases granted
	LeasesRenewed  uint64 // Lease renewals (heartbeats)
	LeasesExpired  uint64 // Leases that expired (no renewal)
	LeasesReleased uint64 // Leases explicitly released

	// Retry (Feature 7)
	TotalRetries   uint64 // Total retry attempts
	RetrySuccesses uint64 // Retries that succeeded
	RetryExhausted uint64 // Jobs that exhausted all retries
	BackoffWaitSum int64  // Sum of backoff wait time (seconds)

	// Preemption (Feature 12)
	PreemptionsTotal    uint64 // Total preemptions executed
	PreemptionsThisHour uint64 // Preemptions in current hour
	PreemptionsDenied   uint64 // Preemptions denied (rate limit, no victim)
	PreemptionAvgGap    int64  // Sum of priority gaps (for averaging)

	// Checkpointing (Feature 8)
	CheckpointsRecorded uint64 // Checkpoints saved
	CheckpointRestores  uint64 // Jobs restored from checkpoint
	CheckpointsFailed   uint64 // Failed checkpoint operations

	// ====================================================================
	// 6. DRF FAIRNESS METRICS (Feature 9)
	// ====================================================================
	DRFChecksTotal  uint64  // Total DRF fairness checks
	DRFChecksPassed uint64  // Jobs allowed by DRF
	DRFChecksDenied uint64  // Jobs denied by DRF (quota exceeded)
	FairnessIndex   float64 // Jain's Fairness Index (0-1)
	TotalTenants    int32   // Number of active tenants
	MaxTenantShare  float64 // Highest tenant dominant share
	MinTenantShare  float64 // Lowest tenant dominant share

	// ====================================================================
	// 7. CRDT SYNC METRICS (Feature 16)
	// ====================================================================
	CRDTSyncRounds      uint64 // Total sync rounds completed
	CRDTMergesTotal     uint64 // Total state merges
	CRDTConflictsFound  uint64 // Conflicts resolved via LWW
	CRDTBytesSent       uint64 // Total bytes sent to peers
	CRDTBytesRecv       uint64 // Total bytes received from peers
	CRDTSyncFailures    uint64 // Failed sync attempts
	CRDTPeersHealthy    int32  // Number of healthy peers
	CRDTVectorClockSize int32  // Size of vector clock
}

// ============================================================================
// RECORDING METHODS
// ============================================================================

// --- HTTP ---

func (m *Metrics) RecordRequest(duration time.Duration) {
	atomic.AddUint64(&m.TotalRequests, 1)
	atomic.AddInt64(&m.RequestDuration, duration.Nanoseconds())
}

func (m *Metrics) RecordError() {
	atomic.AddUint64(&m.TotalErrors, 1)
}

func (m *Metrics) RecordEndpoint(endpoint string) {
	switch endpoint {
	case "schedule":
		atomic.AddUint64(&m.ScheduleRequests, 1)
	case "status":
		atomic.AddUint64(&m.StatusRequests, 1)
	case "health":
		atomic.AddUint64(&m.HealthRequests, 1)
	case "cancel":
		atomic.AddUint64(&m.CancelRequests, 1)
	case "retry":
		atomic.AddUint64(&m.RetryRequests, 1)
	case "list":
		atomic.AddUint64(&m.ListRequests, 1)
	}
}

// --- Scheduling ---

func (m *Metrics) RecordJobScheduled() {
	atomic.AddUint64(&m.TotalScheduled, 1)
	atomic.AddInt32(&m.ActiveJobs, 1)
}

func (m *Metrics) RecordJobCompleted(success bool) {
	atomic.AddInt32(&m.ActiveJobs, -1)
	atomic.AddUint64(&m.CompletedJobs, 1)
	if success {
		atomic.AddUint64(&m.SucceededJobs, 1)
	} else {
		atomic.AddUint64(&m.TotalFailed, 1)
	}
}

func (m *Metrics) RecordSchedulingLatency(duration time.Duration) {
	ns := duration.Nanoseconds()
	atomic.AddInt64(&m.SchedulingLatencySum, ns)

	// Update max (CAS loop)
	for {
		current := atomic.LoadInt64(&m.SchedulingLatencyMax)
		if ns <= current {
			break
		}
		if atomic.CompareAndSwapInt64(&m.SchedulingLatencyMax, current, ns) {
			break
		}
	}
}

func (m *Metrics) RecordJobPriority(priority int) {
	switch {
	case priority >= 80:
		atomic.AddUint64(&m.HighPriorityJobs, 1)
	case priority >= 40:
		atomic.AddUint64(&m.MediumPriorityJobs, 1)
	default:
		atomic.AddUint64(&m.LowPriorityJobs, 1)
	}
}

func (m *Metrics) RecordRegionScheduled(region string) {
	if region == "" {
		return
	}
	val, _ := m.RegionScheduled.LoadOrStore(region, new(uint64))
	atomic.AddUint64(val.(*uint64), 1)
}

func (m *Metrics) RecordGPUType(gpuType string) {
	switch gpuType {
	case "A100":
		atomic.AddUint64(&m.A100Scheduled, 1)
	case "H100":
		atomic.AddUint64(&m.H100Scheduled, 1)
	case "V100":
		atomic.AddUint64(&m.V100Scheduled, 1)
	case "T4":
		atomic.AddUint64(&m.T4Scheduled, 1)
	default:
		atomic.AddUint64(&m.OtherGPU, 1)
	}
}

// --- Clusters ---

func (m *Metrics) RecordHeartbeat() {
	atomic.AddUint64(&m.HeartbeatsRecv, 1)
}

func (m *Metrics) RecordClusterJoin() {
	atomic.AddUint64(&m.ClusterJoins, 1)
	atomic.AddInt32(&m.TotalClusters, 1)
	atomic.AddInt32(&m.HealthyClusters, 1)
}

func (m *Metrics) RecordClusterLeave() {
	atomic.AddUint64(&m.ClusterLeaves, 1)
	atomic.AddInt32(&m.TotalClusters, -1)
	atomic.AddInt32(&m.HealthyClusters, -1)
}

func (m *Metrics) RecordClusterUnhealthy() {
	atomic.AddUint64(&m.ClusterFailovers, 1)
	atomic.AddInt32(&m.HealthyClusters, -1)
	atomic.AddInt32(&m.UnhealthyClusters, 1)
}

func (m *Metrics) RecordClusterRecovered() {
	atomic.AddInt32(&m.HealthyClusters, 1)
	atomic.AddInt32(&m.UnhealthyClusters, -1)
}

// --- GPU Topology ---

func (m *Metrics) RecordTopologyPlacement(hasNVLink bool, sameNUMA bool, score float64) {
	if hasNVLink {
		atomic.AddUint64(&m.NVLinkPlacements, 1)
	} else {
		atomic.AddUint64(&m.PCIePlacements, 1)
	}
	if sameNUMA {
		atomic.AddUint64(&m.SameNUMAPlacements, 1)
	} else {
		atomic.AddUint64(&m.CrossNUMAPlacements, 1)
	}
	atomic.AddInt64(&m.TopologyScoreSum, int64(score*100))
	atomic.AddUint64(&m.TopologyScoreCount, 1)
}

func (m *Metrics) SetGPUCounts(total, inUse, available int32) {
	atomic.StoreInt32(&m.TotalGPUsManaged, total)
	atomic.StoreInt32(&m.GPUsInUse, inUse)
	atomic.StoreInt32(&m.GPUsAvailable, available)
}

// --- Reliability ---

func (m *Metrics) RecordDuplicateBlocked() {
	atomic.AddUint64(&m.DuplicateRequestsBlocked, 1)
}

func (m *Metrics) RecordFencingToken(validated bool) {
	atomic.AddUint64(&m.FencingTokensIssued, 1)
	if validated {
		atomic.AddUint64(&m.FencingTokensValidated, 1)
	} else {
		atomic.AddUint64(&m.FencingTokensRejected, 1)
	}
}

func (m *Metrics) RecordLease(event string) {
	switch event {
	case "acquired":
		atomic.AddUint64(&m.LeasesAcquired, 1)
	case "renewed":
		atomic.AddUint64(&m.LeasesRenewed, 1)
	case "expired":
		atomic.AddUint64(&m.LeasesExpired, 1)
	case "released":
		atomic.AddUint64(&m.LeasesReleased, 1)
	}
}

func (m *Metrics) RecordRetry(success bool, backoffSecs int) {
	atomic.AddUint64(&m.TotalRetries, 1)
	atomic.AddInt64(&m.BackoffWaitSum, int64(backoffSecs))
	if success {
		atomic.AddUint64(&m.RetrySuccesses, 1)
	}
}

func (m *Metrics) RecordRetryExhausted() {
	atomic.AddUint64(&m.RetryExhausted, 1)
}

func (m *Metrics) RecordPreemption(priorityGap int) {
	atomic.AddUint64(&m.PreemptionsTotal, 1)
	atomic.AddUint64(&m.PreemptionsThisHour, 1)
	atomic.AddInt64(&m.PreemptionAvgGap, int64(priorityGap))
}

func (m *Metrics) RecordPreemptionDenied() {
	atomic.AddUint64(&m.PreemptionsDenied, 1)
}

func (m *Metrics) RecordCheckpoint(event string) {
	switch event {
	case "recorded":
		atomic.AddUint64(&m.CheckpointsRecorded, 1)
	case "restored":
		atomic.AddUint64(&m.CheckpointRestores, 1)
	case "failed":
		atomic.AddUint64(&m.CheckpointsFailed, 1)
	}
}

// --- DRF ---

func (m *Metrics) RecordDRFCheck(allowed bool) {
	atomic.AddUint64(&m.DRFChecksTotal, 1)
	if allowed {
		atomic.AddUint64(&m.DRFChecksPassed, 1)
	} else {
		atomic.AddUint64(&m.DRFChecksDenied, 1)
	}
}

func (m *Metrics) SetDRFStats(fairnessIndex float64, tenants int32, maxShare, minShare float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.FairnessIndex = fairnessIndex
	m.MaxTenantShare = maxShare
	m.MinTenantShare = minShare
	atomic.StoreInt32(&m.TotalTenants, tenants)
}

// --- CRDT ---

func (m *Metrics) RecordCRDTSync(bytesSent, bytesRecv uint64, conflicts int) {
	atomic.AddUint64(&m.CRDTSyncRounds, 1)
	atomic.AddUint64(&m.CRDTBytesSent, bytesSent)
	atomic.AddUint64(&m.CRDTBytesRecv, bytesRecv)
	atomic.AddUint64(&m.CRDTConflictsFound, uint64(conflicts))
	atomic.AddUint64(&m.CRDTMergesTotal, 1)
}

func (m *Metrics) RecordCRDTSyncFailure() {
	atomic.AddUint64(&m.CRDTSyncFailures, 1)
}

func (m *Metrics) SetCRDTPeers(healthy int32) {
	atomic.StoreInt32(&m.CRDTPeersHealthy, healthy)
}

// ============================================================================
// COMPUTED METRICS
// ============================================================================

func (m *Metrics) GetSuccessRate() float64 {
	total := atomic.LoadUint64(&m.TotalRequests)
	errors := atomic.LoadUint64(&m.TotalErrors)
	if total == 0 {
		return 0.0
	}
	return float64(total-errors) / float64(total) * 100.0
}

func (m *Metrics) GetAvgDuration() float64 {
	total := atomic.LoadUint64(&m.TotalRequests)
	duration := atomic.LoadInt64(&m.RequestDuration)
	if total == 0 {
		return 0.0
	}
	return float64(duration) / float64(total) / 1e6
}

func (m *Metrics) GetAvgSchedulingLatency() float64 {
	count := atomic.LoadUint64(&m.TotalScheduled)
	sum := atomic.LoadInt64(&m.SchedulingLatencySum)
	if count == 0 {
		return 0.0
	}
	return float64(sum) / float64(count) / 1e6
}

func (m *Metrics) GetAvgTopologyScore() float64 {
	count := atomic.LoadUint64(&m.TopologyScoreCount)
	sum := atomic.LoadInt64(&m.TopologyScoreSum)
	if count == 0 {
		return 0.0
	}
	return float64(sum) / float64(count) / 100.0
}

// ============================================================================
// PROMETHEUS EXPORT
// ============================================================================

func (m *Metrics) ExportPrometheus() string {
	m.mu.RLock()
	fairnessIndex := m.FairnessIndex
	maxShare := m.MaxTenantShare
	minShare := m.MinTenantShare
	m.mu.RUnlock()

	output := ""

	// ── 1. HTTP API ──────────────────────────────────────────────────────
	output += promCounter("ares_http_requests_total", "Total HTTP requests", atomic.LoadUint64(&m.TotalRequests))
	output += promCounter("ares_http_errors_total", "Total HTTP errors", atomic.LoadUint64(&m.TotalErrors))
	output += promGaugeF("ares_http_request_duration_avg_ms", "Average request duration in milliseconds", m.GetAvgDuration())
	output += promGaugeF("ares_http_success_rate_percent", "HTTP success rate percentage", m.GetSuccessRate())

	output += promCounter("ares_http_schedule_requests_total", "Total /schedule requests", atomic.LoadUint64(&m.ScheduleRequests))
	output += promCounter("ares_http_status_requests_total", "Total /status requests", atomic.LoadUint64(&m.StatusRequests))
	output += promCounter("ares_http_cancel_requests_total", "Total /job/cancel requests", atomic.LoadUint64(&m.CancelRequests))
	output += promCounter("ares_http_retry_requests_total", "Total /job/retry requests", atomic.LoadUint64(&m.RetryRequests))

	// ── 2. SCHEDULING ────────────────────────────────────────────────────
	output += promCounter("ares_jobs_scheduled_total", "Total jobs successfully scheduled", atomic.LoadUint64(&m.TotalScheduled))
	output += promCounter("ares_jobs_failed_total", "Total jobs that failed to schedule", atomic.LoadUint64(&m.TotalFailed))
	output += promCounter("ares_jobs_completed_total", "Total jobs completed", atomic.LoadUint64(&m.CompletedJobs))
	output += promCounter("ares_jobs_succeeded_total", "Total jobs succeeded", atomic.LoadUint64(&m.SucceededJobs))
	output += promGauge("ares_jobs_active", "Currently running jobs", int64(atomic.LoadInt32(&m.ActiveJobs)))
	output += promGauge("ares_jobs_queued", "Jobs waiting in queue", int64(atomic.LoadInt32(&m.QueuedJobs)))
	output += promGaugeF("ares_scheduling_latency_avg_ms", "Average scheduling latency in milliseconds", m.GetAvgSchedulingLatency())
	output += promGaugeF("ares_scheduling_latency_max_ms", "Maximum scheduling latency in milliseconds", float64(atomic.LoadInt64(&m.SchedulingLatencyMax))/1e6)

	output += promCounter("ares_jobs_priority_high_total", "Jobs with priority >= 80", atomic.LoadUint64(&m.HighPriorityJobs))
	output += promCounter("ares_jobs_priority_medium_total", "Jobs with priority 40-79", atomic.LoadUint64(&m.MediumPriorityJobs))
	output += promCounter("ares_jobs_priority_low_total", "Jobs with priority < 40", atomic.LoadUint64(&m.LowPriorityJobs))

	// Per-region (dynamic)
	m.RegionScheduled.Range(func(key, value interface{}) bool {
		region := key.(string)
		count := atomic.LoadUint64(value.(*uint64))
		output += fmt.Sprintf("# HELP ares_jobs_scheduled_by_region Jobs scheduled per region\n# TYPE ares_jobs_scheduled_by_region counter\nares_jobs_scheduled_by_region{region=\"%s\"} %d\n\n", region, count)
		return true
	})

	// ── 3. CLUSTERS ──────────────────────────────────────────────────────
	output += promGauge("ares_clusters_total", "Total registered clusters", int64(atomic.LoadInt32(&m.TotalClusters)))
	output += promGauge("ares_clusters_healthy", "Currently healthy clusters", int64(atomic.LoadInt32(&m.HealthyClusters)))
	output += promGauge("ares_clusters_unhealthy", "Currently unhealthy clusters", int64(atomic.LoadInt32(&m.UnhealthyClusters)))
	output += promCounter("ares_cluster_heartbeats_total", "Total heartbeats received", atomic.LoadUint64(&m.HeartbeatsRecv))
	output += promCounter("ares_cluster_joins_total", "Cluster join events", atomic.LoadUint64(&m.ClusterJoins))
	output += promCounter("ares_cluster_leaves_total", "Cluster leave events", atomic.LoadUint64(&m.ClusterLeaves))
	output += promCounter("ares_cluster_failovers_total", "Cluster failover events", atomic.LoadUint64(&m.ClusterFailovers))

	// ── 4. GPU TOPOLOGY ──────────────────────────────────────────────────
	output += promGauge("ares_gpus_total", "Total GPUs across all clusters", int64(atomic.LoadInt32(&m.TotalGPUsManaged)))
	output += promGauge("ares_gpus_in_use", "GPUs currently allocated", int64(atomic.LoadInt32(&m.GPUsInUse)))
	output += promGauge("ares_gpus_available", "GPUs currently available", int64(atomic.LoadInt32(&m.GPUsAvailable)))
	output += promCounter("ares_gpu_nvlink_placements_total", "Jobs placed on NVLink-connected GPUs", atomic.LoadUint64(&m.NVLinkPlacements))
	output += promCounter("ares_gpu_pcie_placements_total", "Jobs placed on PCIe-connected GPUs", atomic.LoadUint64(&m.PCIePlacements))
	output += promCounter("ares_gpu_same_numa_placements_total", "Jobs placed on same-NUMA GPUs", atomic.LoadUint64(&m.SameNUMAPlacements))
	output += promCounter("ares_gpu_cross_numa_placements_total", "Jobs placed across NUMA nodes", atomic.LoadUint64(&m.CrossNUMAPlacements))
	output += promGaugeF("ares_gpu_topology_score_avg", "Average GPU topology placement score", m.GetAvgTopologyScore())

	output += promCounter("ares_gpu_a100_scheduled_total", "Jobs scheduled on A100 GPUs", atomic.LoadUint64(&m.A100Scheduled))
	output += promCounter("ares_gpu_h100_scheduled_total", "Jobs scheduled on H100 GPUs", atomic.LoadUint64(&m.H100Scheduled))
	output += promCounter("ares_gpu_v100_scheduled_total", "Jobs scheduled on V100 GPUs", atomic.LoadUint64(&m.V100Scheduled))
	output += promCounter("ares_gpu_t4_scheduled_total", "Jobs scheduled on T4 GPUs", atomic.LoadUint64(&m.T4Scheduled))

	// ── 5. RELIABILITY ───────────────────────────────────────────────────

	// Exactly-once
	output += promCounter("ares_dedup_blocked_total", "Duplicate requests blocked by idempotency", atomic.LoadUint64(&m.DuplicateRequestsBlocked))
	output += promCounter("ares_fencing_tokens_issued_total", "Fencing tokens generated", atomic.LoadUint64(&m.FencingTokensIssued))
	output += promCounter("ares_fencing_tokens_validated_total", "Fencing token validations passed", atomic.LoadUint64(&m.FencingTokensValidated))
	output += promCounter("ares_fencing_tokens_rejected_total", "Fencing token validations failed (stale)", atomic.LoadUint64(&m.FencingTokensRejected))

	// Leases
	output += promCounter("ares_leases_acquired_total", "Leases acquired", atomic.LoadUint64(&m.LeasesAcquired))
	output += promCounter("ares_leases_renewed_total", "Lease renewals", atomic.LoadUint64(&m.LeasesRenewed))
	output += promCounter("ares_leases_expired_total", "Leases expired", atomic.LoadUint64(&m.LeasesExpired))
	output += promCounter("ares_leases_released_total", "Leases released", atomic.LoadUint64(&m.LeasesReleased))

	// Retry
	output += promCounter("ares_retries_total", "Total retry attempts", atomic.LoadUint64(&m.TotalRetries))
	output += promCounter("ares_retries_succeeded_total", "Retries that succeeded", atomic.LoadUint64(&m.RetrySuccesses))
	output += promCounter("ares_retries_exhausted_total", "Jobs that exhausted all retries", atomic.LoadUint64(&m.RetryExhausted))
	output += promGaugeF("ares_retry_backoff_avg_seconds", "Average backoff wait time", func() float64 {
		retries := atomic.LoadUint64(&m.TotalRetries)
		if retries == 0 {
			return 0
		}
		return float64(atomic.LoadInt64(&m.BackoffWaitSum)) / float64(retries)
	}())

	// Preemption
	output += promCounter("ares_preemptions_total", "Total preemptions executed", atomic.LoadUint64(&m.PreemptionsTotal))
	output += promCounter("ares_preemptions_this_hour", "Preemptions in current hour", atomic.LoadUint64(&m.PreemptionsThisHour))
	output += promCounter("ares_preemptions_denied_total", "Preemptions denied", atomic.LoadUint64(&m.PreemptionsDenied))

	// Checkpointing
	output += promCounter("ares_checkpoints_recorded_total", "Checkpoints saved", atomic.LoadUint64(&m.CheckpointsRecorded))
	output += promCounter("ares_checkpoints_restored_total", "Jobs restored from checkpoint", atomic.LoadUint64(&m.CheckpointRestores))
	output += promCounter("ares_checkpoints_failed_total", "Failed checkpoint operations", atomic.LoadUint64(&m.CheckpointsFailed))

	// ── 6. DRF FAIRNESS ──────────────────────────────────────────────────
	output += promCounter("ares_drf_checks_total", "Total DRF fairness checks", atomic.LoadUint64(&m.DRFChecksTotal))
	output += promCounter("ares_drf_checks_passed_total", "Jobs allowed by DRF", atomic.LoadUint64(&m.DRFChecksPassed))
	output += promCounter("ares_drf_checks_denied_total", "Jobs denied by DRF quota", atomic.LoadUint64(&m.DRFChecksDenied))
	output += promGaugeF("ares_drf_fairness_index", "Jain's Fairness Index (0-1, 1=perfectly fair)", fairnessIndex)
	output += promGauge("ares_drf_tenants_active", "Number of active tenants", int64(atomic.LoadInt32(&m.TotalTenants)))
	output += promGaugeF("ares_drf_max_tenant_share", "Highest tenant dominant share", maxShare)
	output += promGaugeF("ares_drf_min_tenant_share", "Lowest tenant dominant share", minShare)

	// ── 7. CRDT SYNC ─────────────────────────────────────────────────────
	output += promCounter("ares_crdt_sync_rounds_total", "Total CRDT sync rounds", atomic.LoadUint64(&m.CRDTSyncRounds))
	output += promCounter("ares_crdt_merges_total", "Total CRDT state merges", atomic.LoadUint64(&m.CRDTMergesTotal))
	output += promCounter("ares_crdt_conflicts_total", "CRDT conflicts resolved via LWW", atomic.LoadUint64(&m.CRDTConflictsFound))
	output += promCounter("ares_crdt_bytes_sent_total", "Total bytes sent to CRDT peers", atomic.LoadUint64(&m.CRDTBytesSent))
	output += promCounter("ares_crdt_bytes_recv_total", "Total bytes received from CRDT peers", atomic.LoadUint64(&m.CRDTBytesRecv))
	output += promCounter("ares_crdt_sync_failures_total", "Failed CRDT sync attempts", atomic.LoadUint64(&m.CRDTSyncFailures))
	output += promGauge("ares_crdt_peers_healthy", "Number of healthy CRDT peers", int64(atomic.LoadInt32(&m.CRDTPeersHealthy)))

	return output
}

// ============================================================================
// PROMETHEUS FORMAT HELPERS
// ============================================================================

func promCounter(name string, help string, value uint64) string {
	return fmt.Sprintf("# HELP %s %s\n# TYPE %s counter\n%s %d\n\n", name, help, name, name, value)
}

func promGauge(name string, help string, value int64) string {
	return fmt.Sprintf("# HELP %s %s\n# TYPE %s gauge\n%s %d\n\n", name, help, name, name, value)
}

func promGaugeF(name string, help string, value float64) string {
	return fmt.Sprintf("# HELP %s %s\n# TYPE %s gauge\n%s %.4f\n\n", name, help, name, name, value)
}
