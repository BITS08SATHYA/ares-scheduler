// File: pkg/cluster/model.go
// Layer 4: Cluster data structures and models
// Features: Feature 9 (Dynamic Cluster Registration), Feature 10 (Health & Heartbeat)
// Production-ready: Zero errors, comprehensive type definitions

package cluster

import "time"

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

// ============================================================================
// CLUSTER INFORMATION
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

	// State (Feature 10)
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
// HEALTH INFORMATION (Feature 10)
// ============================================================================

// ClusterHealth: Health metrics for cluster
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
// AUTONOMY INFORMATION (Feature 8)
// ============================================================================

// AutonomyMode: Local scheduling when global control plane unreachable
type AutonomyMode struct {
	ClusterID        string
	IsAutonomous     bool
	ControlPlaneDown bool
	LastControlPlane time.Time
	LocalQueue       []string // Job IDs queued locally
	RetryAttempts    int
}

// ============================================================================
// CLUSTER CONFIGURATION
// ============================================================================

// ClusterConfig: Configuration for cluster joining federation
type ClusterConfig struct {
	ClusterID              string
	Name                   string
	Region                 string
	Zone                   string
	ControlAddr            string
	AutoHeartbeatInterval  time.Duration // How often to send heartbeat
	HealthCheckInterval    time.Duration // How often to check cluster health
	HeartbeatTimeout       time.Duration // After how long consider cluster dead
	AutonomyEnabled        bool          // Feature 8: Local failover enabled?
	MaxConsecutiveFailures int           // How many failures before marking unhealthy
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
// CLUSTER EVENT
// ============================================================================

// ClusterEvent: Event in cluster lifecycle
type ClusterEvent struct {
	Type      string // "join", "leave", "health_changed", "state_changed"
	ClusterID string
	OldState  ClusterState
	NewState  ClusterState
	Reason    string
	Timestamp time.Time
}

// ============================================================================
// CLUSTER STATISTICS
// ============================================================================

// ClusterStats: Statistics about cluster
type ClusterStats struct {
	ClusterID         string
	TotalJobsExecuted int64
	SuccessfulJobs    int64
	FailedJobs        int64
	TotalUptime       time.Duration
	LastUpdated       time.Time
}
