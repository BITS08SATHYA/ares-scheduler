// File: pkg/cluster/model.go
// CONSOLIDATED: All cluster-related data models
// Layer 4: Cluster data structures and models
// Features: Feature 9 (Dynamic Cluster Registration), Feature 10 (Health & Heartbeat)
// Production-ready: Zero errors, comprehensive type definitions

package cluster

import (
	"time"
)

// ============================================================================
// CLUSTER STATES
// ============================================================================

// ClusterState: Operational state of cluster
type ClusterState string

const (
	StateJoining    ClusterState = "JOINING"    // Registering with federation
	StateReady      ClusterState = "READY"      // Operational
	StateHealthy    ClusterState = "HEALTHY"    // All systems nominal
	StateDegraded   ClusterState = "DEGRADED"   // Partial outage
	StateUnhealthy  ClusterState = "UNHEALTHY"  // Major problems
	StateLeaving    ClusterState = "LEAVING"    // Deregistering
	StateAutonomous ClusterState = "AUTONOMOUS" // Control plane unreachable (Feature 8)
	StateRecovering ClusterState = "RECOVERING" // Reconnecting to control plane
)

// ClusterEventType: Type of cluster lifecycle event
type ClusterEventType string

const (
	EventJoin            ClusterEventType = "join"
	EventLeave           ClusterEventType = "leave"
	EventHealthChanged   ClusterEventType = "health_changed"
	EventStateChanged    ClusterEventType = "state_changed"
	EventCapacityChanged ClusterEventType = "capacity_changed"
)

// ============================================================================
// SECTION 2: CORE CLUSTER MODELS
// Source of truth for cluster information (used by ClusterManager)
// ============================================================================

// Cluster: Complete cluster information
// Registered with global control plane, tracked for scheduling decisions
type Cluster struct {
	// Identity
	ClusterID   string // "cluster-us-west-2a"
	Name        string // Human-readable name
	Region      string // "us-west"
	Zone        string // "us-west-2a"
	ControlAddr string // Address of cluster's local control plane

	// State (Feature 10 - Health & Heartbeat Propagation)
	State            ClusterState
	IsHealthy        bool
	IsReachable      bool
	LastHeartbeatAt  time.Time
	LastHeartbeatAge time.Duration // Calculated: time.Since(LastHeartbeatAt)

	// Capacity (what cluster has)
	TotalGPUs  int
	TotalCPUs  int
	TotalMemGB float64

	// Current Load (what's in use)
	GPUsInUse   int
	CPUsInUse   int
	MemGBInUse  float64
	RunningJobs int
	PendingJobs int

	// Score
	//score *ClusterScore

	// Metadata
	Labels      map[string]string // Cluster tags
	Annotations map[string]string // Additional info
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// IsAvailable: Check if cluster can accept jobs
func (c *Cluster) IsAvailable() bool {
	return c.IsHealthy && c.IsReachable && c.State == StateReady
}

// AvailableGPUs: Get available GPU count
func (c *Cluster) AvailableGPUs() int {
	return c.TotalGPUs - c.GPUsInUse
}

// AvailableMemGB: Get available memory
func (c *Cluster) AvailableMemGB() float64 {
	return c.TotalMemGB - c.MemGBInUse
}

// GPUUtilization: GPU utilization percentage
func (c *Cluster) GPUUtilization() float64 {
	if c.TotalGPUs == 0 {
		return 0.0
	}
	return float64(c.GPUsInUse) / float64(c.TotalGPUs) * 100.0
}

// MemoryUtilization: Memory utilization percentage
func (c *Cluster) MemoryUtilization() float64 {
	if c.TotalMemGB == 0 {
		return 0.0
	}
	return c.MemGBInUse / c.TotalMemGB * 100.0
}

// ============================================================================
//  SECTION 2B: CLUSTER HEALTH MODELS - HEALTH INFORMATION (Feature 10)
// ============================================================================

// ClusterHealth: Health metrics for cluster (Feature-10)
type ClusterHealth struct {
	ClusterID        string
	State            ClusterState
	IsHealthy        bool
	LastHeartbeat    time.Time
	HeartbeatAge     time.Duration
	ConsecutiveFails int
	ErrorMessage     string
	Timestamp        time.Time
}

// IsStale: Check if heartbeat is stale (> 60 seconds)
func (ch *ClusterHealth) IsStale() bool {
	return ch.HeartbeatAge > 60*time.Second
}

// IsCritical: Check if cluster critical (> 3 consecutive failures)
func (ch *ClusterHealth) IsCritical() bool {
	return ch.ConsecutiveFails > 3
}

// ============================================================================
// HEARTBEAT INFORMATION (Feature 10)
// ============================================================================

// Heartbeat: Periodic health signal from cluster
type Heartbeat struct {
	ClusterID    string
	Timestamp    time.Time
	GPUCount     int     // Current total GPUs
	MemoryGB     float64 // Current total memory
	GPUsInUse    int
	MemGBInUse   float64
	RunningJobs  int
	PendingJobs  int
	NodeCount    int
	HealthyNodes int
	Version      string // Cluster version
	Status       string // "healthy", "degraded", "unhealthy"
}

// ============================================================================
// SECTION 2C: CLUSTER CONFIG & REGISTRATION
// ============================================================================

// ============================================================================
// CLUSTER CONFIGURATION
// ============================================================================
// ClusterConfig: Configuration for cluster joining federation (Feature 9)
type ClusterConfig struct {
	ClusterID              string
	Name                   string
	Region                 string
	Zone                   string
	ControlAddr            string
	AutoHeartbeatInterval  time.Duration
	HealthCheckInterval    time.Duration
	HeartbeatTimeout       time.Duration
	AutonomyEnabled        bool // Feature 8
	MaxConsecutiveFailures int
	Labels                 map[string]string
}

// Default config
var DefaultClusterConfig = &ClusterConfig{
	AutoHeartbeatInterval:  10 * time.Second,
	HealthCheckInterval:    30 * time.Second,
	HeartbeatTimeout:       60 * time.Second,
	AutonomyEnabled:        true,
	MaxConsecutiveFailures: 3,
}

// ============================================================================
// SECTION 2D: CLUSTER EVENTS
// ============================================================================

// ClusterEvent: Event in cluster lifecycle
type ClusterEvent struct {
	Type      ClusterEventType
	ClusterID string
	OldState  ClusterState
	NewState  ClusterState
	Reason    string
	Timestamp time.Time
}

// ============================================================================
// SECTION 3: SCHEDULER MODELS
// Models used by GlobalScheduler for cluster selection and scoring
// Different from core Cluster model - scheduler's specific needs
// ============================================================================

// ClusterInfo: Scheduler's view of cluster for scoring decisions
// IMPORTANT: Different from Cluster struct
// - Cluster: Source of truth (managed by ClusterManager)
// - ClusterInfo: Scheduler's cached view (for faster scoring)
type ClusterInfo struct {
	ClusterID          string
	Region             string
	Zone               string
	IsHealthy          bool
	TotalGPUs          int
	AvailableGPUs      int
	TotalMemoryGB      float64
	AvailableMemoryGB  float64
	RunningJobsCount   int
	LastHeartbeat      time.Time
	LocalSchedulerAddr string // URL to contact local scheduler

	// Heterogeneous Hardware
	DeviceTypes   []string // ["GPU"], ["TPU"], ["GPU", "FPGA"]
	TPUChipsTotal int      // Total TPU chips (0 if no TPUs)
	TPUChipsAvail int      // Available TPU chips
	FPGACount     int      // Total FPGAs
	FPGAAvail     int      // Available FPGAs
}

// ClusterScore: Score for cluster selection
// Result of scoring algorithm used in cluster selection
type ClusterScore struct {
	ClusterID             string
	Region                string
	Score                 float64 // 0-100
	AvailableGPUs         int
	AvailableMemoryGB     float64
	CurrentLoad           int
	UtilizationPercent    float64
	HealthScore           float64
	RegionPreferenceScore float64
	Reasons               []string // Why this score?
}

// GlobalSchedulingDecision: Complete global scheduling decision
// Output of GlobalScheduler.ScheduleJob()
type GlobalSchedulingDecision struct {
	JobID              string
	ClusterID          string // Which cluster
	NodeID             string
	GPUIndices         []int
	Region             string
	ClusterScore       float64
	PlacementReasons   []string
	LocalSchedulerAddr string // How to contact local scheduler
	ScheduledAt        time.Time
}

// ============================================================================
// SECTION 4: METRICS MODELS
// Observability and monitoring
// ============================================================================

// GlobalMetrics: Scheduling metrics for entire datacenter
type GlobalMetrics struct {
	TotalJobsScheduled int64
	TotalJobsFailed    int64
	TotalJobsRouted    map[string]int64 // clusterID -> count
	AvgSchedulingTime  time.Duration
	LastUpdated        time.Time
}

// ClusterStats: Statistics about cluster
type ClusterStats struct {
	ClusterID         string
	TotalJobsExecuted int64
	SuccessfulJobs    int64
	FailedJobs        int64
	TotalUptime       time.Duration
	LastUpdated       time.Time
}

// DatacenterLoad: Aggregated load across all clusters
type DatacenterLoad struct {
	TotalClusters        int
	HealthyClusters      int
	TotalGPUs            int
	GPUsInUse            int
	GPUUtilizationPct    float64
	TotalMemoryGB        float64
	MemoryInUseGB        float64
	MemoryUtilizationPct float64
	RunningJobs          int
	Regions              map[string]int // region -> cluster count
}

// ============================================================================
// SECTION 5: API MODELS
// HTTP request/response types for cluster management APIs
// ============================================================================

// ---- REGISTRATION ----

// ClusterRegistrationRequest: HTTP request for cluster registration
// Sent by worker cluster when it starts
// Maps to: POST /clusters/register
type ClusterRegistrationRequest struct {
	// Required
	ClusterID          string `json:"cluster_id"`
	Region             string `json:"region"`
	Zone               string `json:"zone"`
	LocalSchedulerAddr string `json:"local_scheduler_addr"`

	// Capacity
	TotalGPUs     int     `json:"total_gpus"`
	TotalCPUs     int     `json:"total_cpus"`
	TotalMemoryGB float64 `json:"total_memory_gb"`

	// GPU Topology (Feature 4)
	GPUTopology map[string]interface{} `json:"gpu_topology,omitempty"`

	// Metadata
	Labels map[string]string `json:"labels,omitempty"`
}

// ClusterRegistrationResponse: HTTP response to registration
type ClusterRegistrationResponse struct {
	Success   bool   `json:"success"`
	ClusterID string `json:"cluster_id"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
}

// ---- DEREGISTRATION ----

// ClusterDeregistrationRequest: HTTP request to leave federation
type ClusterDeregistrationRequest struct {
	ClusterID string `json:"cluster_id"`
}

// ClusterDeregistrationResponse: HTTP response to deregistration
type ClusterDeregistrationResponse struct {
	Success   bool   `json:"success"`
	ClusterID string `json:"cluster_id"`
	Message   string `json:"message"`
}

// ---- HEARTBEAT ----

// ClusterHeartbeatRequest: HTTP request for health update
// Sent periodically (every 10 seconds) by worker cluster
// Maps to: POST /cluster-heartbeat
type ClusterHeartbeatRequest struct {
	ClusterID   string  `json:"cluster_id"`
	GPUsInUse   int     `json:"gpus_in_use"`
	MemGBInUse  float64 `json:"mem_gb_in_use"`
	CPUsInUse   int     `json:"cpus_in_use"`
	RunningJobs int     `json:"running_jobs"`
	PendingJobs int     `json:"pending_jobs"`
	Status      string  `json:"status"` // "healthy", "degraded", "unhealthy"
}

// ClusterHeartbeatResponse: HTTP response to heartbeat
type ClusterHeartbeatResponse struct {
	Success   bool   `json:"success"`
	ClusterID string `json:"cluster_id"`
	Message   string `json:"message"`
}

// ---- LIST CLUSTERS ----

// ClusterListItem: Single cluster in list response
// Maps to: GET /clusters/list response body
type ClusterListItem struct {
	ClusterID       string  `json:"cluster_id"`
	Region          string  `json:"region"`
	Zone            string  `json:"zone"`
	IsHealthy       bool    `json:"is_healthy"`
	TotalGPUs       int     `json:"total_gpus"`
	GPUsInUse       int     `json:"gpus_in_use"`
	AvailableGPUs   int     `json:"available_gpus"`
	TotalMemoryGB   float64 `json:"total_memory_gb"`
	MemGBInUse      float64 `json:"mem_gb_in_use"`
	RunningJobs     int     `json:"running_jobs"`
	PendingJobs     int     `json:"pending_jobs"`
	LastHeartbeatAt string  `json:"last_heartbeat_at"`
}

// ListClustersResponse: HTTP response for listing clusters
// Maps to: GET /clusters/list response
type ListClustersResponse struct {
	Success   bool               `json:"success"`
	Clusters  []*ClusterListItem `json:"clusters"`
	Count     int                `json:"count"`
	Timestamp string             `json:"timestamp"`
}

// ClusterConstraints: Additional cluster selection constraints
type ClusterConstraints struct {
	RequiredRegions   []string // Only these regions
	ForbiddenRegions  []string // Never these regions
	RequiredClusters  []string // Only these clusters
	ForbiddenClusters []string // Never these clusters
	MaxUtilization    float64  // Cluster util must be < this %
	MinAvailableGPUs  int      // Cluster must have at least this
	PreferredRegions  []string // Prefer these regions
}
