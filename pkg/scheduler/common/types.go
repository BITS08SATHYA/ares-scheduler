package common

import "time"

// CRITICAL: This is the foundation. Everything else imports from here.
// Copy this EXACTLY into pkg/scheduler/common/types.go

// ============================================================================
// SECTION 1: JOB TYPES (Feature 17 - Job Lifecycle Management)
// ============================================================================
// JobSpec: Immutable specification of what user wants (the "request")

// JobSpec: Immutable specification of what user wants (the "request")
type JobSpec struct {
	// Deduplication (Feature 6 - Layer 1)
	RequestID string // Unique request ID for dedup

	// Basic info
	Name    string
	Image   string   // Docker image: "nvidia/cuda:12.0"
	Command []string // ["python", "train.py"]
	Args    []string // ["--epochs", "100"]

	// GPU Requirements (Feature 13 - GPU-Aware Scheduling)
	GPUCount int    // How many GPUs needed
	GPUType  string // "A100", "H100", "V100", "T4"

	// Topology Preferences (Feature 4 - Topology-Aware)
	PreferNVLink   bool // Want NVLink-connected GPUs (900 GB/s)
	PreferSameNUMA bool // Want same-NUMA placement

	// Checkpointing
	CheckpointEnabled  bool   // Whether job supports checkpointing
	CheckpointPath     string // Base path for checkpoints (e.g., s3://bucket/checkpoints/)
	CheckpointInterval int    // Seconds between checkpoints (default: 300 = 5 min)

	// Resources
	MemoryMB  int // Memory in megabytes
	CPUMillis int // CPU in millicores (1000 = 1 core)

	// Job Control (Feature 5 - Priority & Preemption)
	Priority int // 0-100, higher = more urgent

	// Retry (Feature 21 - Backoff & Retry)
	TimeoutSecs int // Job timeout in seconds
	MaxRetries  int // Max retry attempts

	// Multi-tenancy (Feature 25 - RBAC & Tenant Isolation)
	TenantID string  // Which tenant owns this
	QuotaGB  float64 // Quota in GB

	// SLA (Feature 22 - Global Metrics)
	TargetLatencyMs int // Target latency in ms

	// Gang Scheduling (Feature 10)
	GangID        string `json:"gang_id,omitempty"`
	GangSize      int    `json:"gang_size,omitempty"`
	GangMemberIdx int    `json:"gang_member_idx,omitempty"`
}

// JobStatus: Current state of a job
type JobStatus string

const (
	StatusPending   JobStatus = "PENDING"   // Queued, waiting
	StatusScheduled JobStatus = "SCHEDULED" // Assigned to cluster
	StatusRunning   JobStatus = "RUNNING"   // Pod is executing
	StatusSucceeded JobStatus = "SUCCEEDED" // Completed OK
	StatusFailed    JobStatus = "FAILED"    // Failed, may retry
	StatusRetrying  JobStatus = "RETRYING"
	StatusQueued    JobStatus = "QUEUED" // Accepted but waiting for resources
)

// Job: Mutable runtime state (the "execution instance")
type Job struct {
	ID   string   // UUID: "550e8400-e29b-41d4-a716-446655440000"
	Spec *JobSpec // Reference to the spec

	// State Machine (Feature 17)
	Status JobStatus // Current status

	// Placement (Feature 1 - Multi-Cluster)
	ClusterID string // Which cluster: "cluster-us-west-2a"
	NodeID    string // Which node: "node-001"
	PodName   string // K8s pod: "ares-{jobID}-{attempt}"

	// GPU Allocation (Feature 4, 13)
	AllocatedGPUIndices []int // e.g., [0, 1]

	// Leasing (Feature 6, 19)
	ExecutionLease *LeaseInfo // Layer 2: etcd lease
	ExecutionToken string     // Layer 3: fencing token

	// Retry (Feature 21)
	Attempts    int       // How many execution attempts
	NextRetryAt time.Time // When to retry

	// Timing
	SubmitTime   time.Time // User submitted
	ScheduleTime time.Time // Assigned to cluster
	StartTime    time.Time // Pod started
	EndTime      time.Time // Pod finished

	// Results
	ExitCode int    // 0 = success
	ErrorMsg string // Why it failed

	// Checkpointing
	LastCheckpointPath string    // Path to last checkpoint (e.g., s3://bucket/job123/epoch-47)
	LastCheckpointTime time.Time // When last checkpoint was saved
	LastCheckpointMeta string    // Optional metadata (e.g., "epoch=47,loss=0.023")

	// Metrics (Feature 22)
	Metrics map[string]interface{}
}

// Helper methods
func (j *Job) IsRunning() bool {
	return j.Status == StatusRunning
}

func (j *Job) IsCompleted() bool {
	return j.Status == StatusSucceeded || j.Status == StatusFailed
}

func (j *Job) IsPending() bool {
	return j.Status == StatusPending
}

func (j *Job) CalculateDuration() time.Duration {
	if j.EndTime.IsZero() {
		return 0
	}
	return j.EndTime.Sub(j.StartTime)
}

func (j *Job) CalculateTotalLatency() time.Duration {
	if j.EndTime.IsZero() {
		return 0
	}
	return j.EndTime.Sub(j.SubmitTime)
}

// ============================================================================
// SECTION 2: GPU TYPES (Feature 4, 13 - GPU-Aware Scheduling)
// ============================================================================

// GPUDevice: Single physical GPU
type GPUDevice struct {
	Index              int     // 0, 1, 2, ...
	UUID               string  // GPU UUID
	Type               string  // "A100", "H100", "V100"
	MemoryGB           float64 // Total: 80GB
	AvailableMemGB     float64 // Currently free
	UtilizationPercent float64 // 0-100
	TemperatureCelsius float64 // Current temp
	PowerDrawWatts     float64 // Current power
	IsHealthy          bool    // Health status
}

// GPUTopology: GPU interconnection details
// Key for Feature 4 (Topology-Aware): NVLink 900 GB/s vs PCIe 32 GB/s
type GPUTopology struct {
	// Which GPUs have NVLink (fast interconnect)
	// Example: [[0,1], [2,3], [4,5], [6,7]] for 8 A100s
	NVLinkPairs [][]int

	// NVLink link count per GPU pair: "gpu0-gpu1" -> link count
	// On p4d.24xlarge (8×A100): NV12 = same NVSwitch (600 GB/s), NV6 = cross-NVSwitch (300 GB/s)
	// Higher link count = higher bandwidth = better placement score
	// Example: {"0-1": 12, "0-4": 6} means GPU 0↔1 has 12 links, GPU 0↔4 has 6 links
	NVLinkCount map[string]int `json:"nvlink_count,omitempty"`

	// NVSwitch domain grouping: domain_id -> []gpu_indices
	// GPUs within the same NVSwitch domain have maximum NVLink bandwidth
	// On p4d.24xlarge: domain 0 = [0,1,2,3], domain 1 = [4,5,6,7]
	NVSwitchDomains map[int][]int `json:"nvswitch_domains,omitempty"`

	// GPU to NUMA node mapping (for memory locality)
	// Example: {0:0, 1:0, 2:1, 3:1, ...}
	GPUToNUMA map[int]int

	// PCIe generation per GPU
	// Gen4: 32 GB/s, Gen5: 64 GB/s
	PCIeGen map[int]int
}

// ============================================================================
// SECTION 3: CLUSTER TYPES (Feature 1, 7 - Multi-Cluster Scheduling)
// ============================================================================

// Cluster: A Kubernetes cluster
type Cluster struct {
	ID     string // "cluster-us-west-2a"
	Name   string
	Region string // For Feature 2 (Multi-Region)
	Zone   string

	Capacity    *Capacity // Total resources
	CurrentLoad *Load     // Current usage

	// Health (Feature 10 - Health & Heartbeat)
	IsHealthy       bool
	LastHeartbeat   time.Time
	HealthCheckTime time.Time
}

// Capacity: Total cluster resources
type Capacity struct {
	TotalGPUs     int            // Total GPUs
	TotalCPUCores int            // Total cores
	TotalMemoryGB float64        // Total memory
	NumGPUsByType map[string]int // {"A100": 8, "H100": 16}
}

// Load: Current resource usage
type Load struct {
	GPUsInUse        int     // In-use GPUs
	CPUCoresInUse    int     // In-use cores
	MemoryGBInUse    float64 // In-use memory
	RunningJobsCount int     // Running jobs
}

// Available returns remaining capacity
func (c *Cluster) AvailableCapacity() *Capacity {
	return &Capacity{
		TotalGPUs:     c.Capacity.TotalGPUs - c.CurrentLoad.GPUsInUse,
		TotalCPUCores: c.Capacity.TotalCPUCores - c.CurrentLoad.CPUCoresInUse,
		TotalMemoryGB: c.Capacity.TotalMemoryGB - c.CurrentLoad.MemoryGBInUse,
	}
}

// Utilization returns GPU utilization %
func (c *Cluster) UtilizationPercent() float64 {
	if c.Capacity.TotalGPUs == 0 {
		return 0
	}
	return float64(c.CurrentLoad.GPUsInUse) / float64(c.Capacity.TotalGPUs) * 100
}

// ============================================================================
// SECTION 4: LEASING TYPES (Feature 19, 6 - Distributed Locking)
// ============================================================================

// LeaseInfo: etcd distributed lease (Feature 6 Layer 2, Feature 19)
type LeaseInfo struct {
	LeaseID    string    // etcd lease ID
	GrantedAt  time.Time // When granted
	TTLSeconds int       // Usually 30 seconds
	ExpiresAt  time.Time // Auto-expiration time
	JobID      string    // Which job
	ExecutorID string    // Which executor holds it
}

// IsExpired: Check if lease timed out
func (li *LeaseInfo) IsExpired() bool {
	return time.Now().After(li.ExpiresAt)
}

// TimeUntilExpiration: How long until timeout
func (li *LeaseInfo) TimeUntilExpiration() time.Duration {
	return time.Until(li.ExpiresAt)
}

// ExecutionToken: Fencing token (Feature 6 Layer 3)
// Proves THIS executor owns THIS execution attempt
// Prevents stale executor from re-running after crash
type ExecutionToken struct {
	Token      string    // UUID: unique per attempt
	JobID      string    // Which job
	ExecutorID string    // Which executor
	IssuedAt   time.Time // When created
	ExpiresAt  time.Time // When expires
}

// IsValid: Check if token still valid
func (et *ExecutionToken) IsValid() bool {
	return time.Now().Before(et.ExpiresAt)
}

// ============================================================================
// SECTION 5: SCHEDULING TYPES (Feature 4 - Topology-Aware)
// ============================================================================

// PlacementDecision: GPU placement choice
// Scoring: NVLink +50, NUMA ±30, memory +2/GB, util -0.5/%
type PlacementDecision struct {
	GPUIndices []int   // e.g., [0, 1]
	Score      float64 // Higher is better

	// Breakdown (for debugging)
	NVLinkBonus        float64 // +50 if NVLink
	NUMABonus          float64 // ±30 if same/diff NUMA
	MemoryBonus        float64 // +2 per GB available
	UtilizationPenalty float64 // -0.5 per % busy

	Reasons []string // Why this placement
}

// ============================================================================
// SECTION 6: METRICS TYPES (Feature 22 - Global Metrics)
// ============================================================================

// SLARecord: SLA compliance tracking
type SLARecord struct {
	JobID           string    // Which job
	TenantID        string    // Which tenant
	TargetLatencyMs int       // SLA target
	ActualLatencyMs int       // Actual time
	Compliant       bool      // Met SLA?
	Timestamp       time.Time // When recorded
}

// IsCompliant: Check if met SLA
func (sr *SLARecord) IsCompliant() bool {
	return sr.ActualLatencyMs <= sr.TargetLatencyMs
}

// ============================================================================
// SECTION 7: ERROR TYPES
// ============================================================================

// NotFoundError: Resource doesn't exist
type NotFoundError struct {
	ResourceType string // "Job", "Cluster"
	ResourceID   string // Specific ID
}

func (e *NotFoundError) Error() string {
	return "NotFound: " + e.ResourceType + " " + e.ResourceID
}

// AlreadyExistsError: Resource already exists
type AlreadyExistsError struct {
	ResourceType string
	ResourceID   string
}

func (e *AlreadyExistsError) Error() string {
	return "AlreadyExists: " + e.ResourceType + " " + e.ResourceID
}

// InsufficientResourcesError: Not enough capacity
type InsufficientResourcesError struct {
	Requested map[string]interface{}
	Available map[string]interface{}
}

func (e *InsufficientResourcesError) Error() string {
	return "InsufficientResources"
}

// QuotaExceededError: Tenant quota exceeded
type QuotaExceededError struct {
	TenantID string
	Quota    map[string]interface{}
}

func (e *QuotaExceededError) Error() string {
	return "QuotaExceeded: " + e.TenantID
}

// LeaseAcquisitionFailedError: Couldn't get lease
type LeaseAcquisitionFailedError struct {
	JobID  string
	Reason string
}

func (e *LeaseAcquisitionFailedError) Error() string {
	return "LeaseAcquisitionFailed: " + e.JobID + " - " + e.Reason
}

// ============================================================================
// SECTION 8: CONFIG TYPE
// ============================================================================

// Config: System-wide configuration
type Config struct {
	// etcd
	EtcdEndpoints   []string      // ["localhost:2379"]
	EtcdDialTimeout time.Duration // Connection timeout

	// Redis
	RedisAddr     string // "localhost:6379"
	RedisPassword string
	RedisDB       int

	// Kubernetes
	KubeConfigPath string // Path to kubeconfig
	Namespace      string // K8s namespace

	// Scheduler
	GlobalSchedulerPort int
	LocalSchedulerPort  int

	// Logging
	LogLevel string // "debug", "info", "warn", "error"

	// Features
	EnableMetrics bool
	EnableTracing bool

	// Timeouts
	JobTimeout           time.Duration
	LeaseRenewalInterval time.Duration
	HealthCheckInterval  time.Duration
}
