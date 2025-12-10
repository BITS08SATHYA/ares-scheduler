package common

import (
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/local"
	"golang.org/x/net/context"
	"sync"
	"time"
)

// Scope: Single cluster (runs on each cluster's control plane)
type Executor struct {
	ClusterID    string
	ControlPlane string
	Log          *logger.Logger
	Config       *ExecutorConfig

	// Job tracking
	jobsMu        sync.RWMutex
	ActiveJobs    map[string]*ExecutionContext // JobID -> ExecutionContext
	CompletedJobs map[string]*ExecutionResult  // JobID -> ExecutionResult

	// Metrics (atomic for thread-safety)
	totalJobs       uint64
	totalSuccessful uint64
	totalFailed     uint64
	totalCancelled  uint64
	totalDuration   int64 // nanoseconds, atomic

	// Pod management
	podMu       sync.RWMutex
	PodRegistry map[string]*PodInfo // PodName -> PodInfo

	// Kubernetes client (mock for now, will be real K8s client in production)
	K8sClient K8sClient
}

func (e Executor) CancelJob(id string) error {
	e.Log.Info("Canceling job %v", id)
	return nil
}

// ExecutorConfig: Configuration for executor
type ExecutorConfig struct {
	ClusterID                string        // e.g., "cluster-us-west-2a"
	Namespace                string        // e.g., "default" or "ares-jobs"
	DefaultTimeout           time.Duration // Pod timeout (default: 1 hour)
	DefaultMemoryMB          int           // Pod default memory
	DefaultCPUMillis         int           // Pod default CPU (millicores)
	HealthCheckInterval      time.Duration // Pod health check frequency
	MaxConcurrentJobs        int           // Max concurrent Pods
	ImageRegistry            string        // e.g., "docker.io", "ghcr.io"
	DefaultJobImage          string        // e.g., "ares-job:latest"
	RestartPolicy            string        // Always, OnFailure, Never
	ImagePullPolicy          string        // Always, IfNotPresent, Never
	EnableGPUSupport         bool          // Enable GPU requests
	LogCollectionEnabled     bool          // Collect Pod logs
	MetricsCollectionEnabled bool          // Collect Pod metrics
}

// ExecutionContext: Runtime context for a job being executed
type ExecutionContext struct {
	JobID         string
	LocalDecision *local.LocalSchedulingDecision // From Layer 6 (Local Scheduler)
	PodName       string
	Namespace     string
	StartTime     time.Time
	Timeout       time.Duration
	Status        JobStatus
	LastUpdated   time.Time
	CurrentPhase  PodPhase
	NodeID        string                 // From LocalSchedulingDecision
	GPUIndices    []int                  // From LocalSchedulingDecision
	Logs          string                 // Job logs (if collected)
	Metrics       map[string]interface{} // Pod metrics
}

// ExecutionResult: Final result of job execution
type ExecutionResult struct {
	JobID        string
	PodName      string
	Status       JobStatus
	Phase        PodPhase
	StartTime    time.Time
	EndTime      time.Time
	Duration     time.Duration
	ExitCode     int
	ErrorMessage string
	Logs         string
	Metrics      map[string]interface{}
	CompletedAt  time.Time
}

// PodInfo: Information about created Kubernetes Pod
type PodInfo struct {
	PodName       string
	Namespace     string
	JobID         string
	NodeID        string
	Phase         PodPhase
	CreatedAt     time.Time
	StartedAt     time.Time
	FinishedAt    time.Time
	ContainerID   string
	GPUIndices    []int
	ResourceUsage map[string]interface{}
	Ready         bool
}

// JobStatus: High-level job status
type JobStatus string

const (
	StatusPending    JobStatus = "Pending"
	StatusRunning    JobStatus = "Running"
	StatusSuccessful JobStatus = "Successful"
	StatusFailed     JobStatus = "Failed"
	StatusCancelled  JobStatus = "Cancelled"
	StatusUnknown    JobStatus = "Unknown"
)

// PodPhase: Kubernetes Pod phase
type PodPhase string

const (
	PhasePending   PodPhase = "Pending"
	PhaseRunning   PodPhase = "Running"
	PhaseSucceeded PodPhase = "Succeeded"
	PhaseFailed    PodPhase = "Failed"
	PhaseUnknown   PodPhase = "Unknown"
)

// K8sClient: Interface for Kubernetes operations (mock in this file)
type K8sClient interface {
	CreatePod(ctx context.Context, pod *PodSpec) (podName string, err error)
	GetPod(ctx context.Context, podName string) (*PodInfo, error)
	DeletePod(ctx context.Context, podName string) error
	ListPods(ctx context.Context) ([]*PodInfo, error)
	GetPodLogs(ctx context.Context, podName string) (string, error)
	GetPodMetrics(ctx context.Context, podName string) (map[string]interface{}, error)
	WatchPod(ctx context.Context, podName string, callback func(*PodInfo)) error
}

// PodSpec: Specification for Kubernetes Pod
type PodSpec struct {
	PodName         string
	Namespace       string
	Image           string
	ImagePullPolicy string
	EnvVars         map[string]string
	MemoryMB        int
	CPUMillis       int
	GPUCount        int
	GPUIndices      []int
	Timeout         time.Duration
	RestartPolicy   string
	NodeID          string
	Labels          map[string]string
}

// Default config
var DefaultExecutorConfig = &ExecutorConfig{
	Namespace:                "default",
	DefaultTimeout:           1 * time.Hour,
	DefaultMemoryMB:          1024,
	DefaultCPUMillis:         500,
	HealthCheckInterval:      5 * time.Second,
	MaxConcurrentJobs:        100,
	ImageRegistry:            "docker.io",
	DefaultJobImage:          "ares-job:latest",
	RestartPolicy:            "OnFailure",
	ImagePullPolicy:          "IfNotPresent",
	EnableGPUSupport:         true,
	LogCollectionEnabled:     true,
	MetricsCollectionEnabled: true,
}
