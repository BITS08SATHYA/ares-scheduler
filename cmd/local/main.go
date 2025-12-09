// File: cmd/local/main.go (UPDATED WITH AUTO-REGISTRATION + REAL K8S CLIENT)
// Local scheduler with automatic cluster registration and heartbeat
// CRITICAL CHANGES:
// 1. Auto-registers cluster on startup (no manual /clusters/register needed)
// 2. Sends periodic heartbeats to control plane
// 3. Uses REAL Kubernetes client (not mock)
// 4. Implements local failover when control plane unreachable

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/cluster"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/executor"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/gpu"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/kubernetes"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/local"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/redis"
)

const (
	DefaultLocalPort    = 9090
	DefaultRedisAddr    = "localhost:6379"
	DefaultControlPlane = "http://localhost:8080"
	DefaultNamespace    = "ares-jobs"
)

var (
	clusterID    = flag.String("cluster-id", "", "Cluster ID (required)")
	localPort    = flag.Int("port", DefaultLocalPort, "Local scheduler port")
	redisAddr    = flag.String("redis", DefaultRedisAddr, "Redis address")
	controlPlane = flag.String("control-plane", DefaultControlPlane, "Global control plane URL")
	region       = flag.String("region", "us-west", "Region")
	zone         = flag.String("zone", "us-west-1a", "Zone")
	logLevel     = flag.String("log-level", "info", "Log level")
	namespace    = flag.String("k8s-namespace", DefaultNamespace, "Kubernetes namespace")
)

func main() {
	flag.Parse()

	// Validate required flags
	if *clusterID == "" {
		fmt.Fprintf(os.Stderr, "Error: --cluster-id is required\n")
		flag.Usage()
		os.Exit(1)
	}

	// Initialize logger
	log := initializeLogger(*logLevel)
	defer log.Sync()

	log.Info("╔═══════════════════════════════════════════════════════╗")
	log.Info("║   Ares Local Scheduler - Worker Cluster              ║")
	log.Info("║   Auto-Registration + Real K8s Client                ║")
	log.Info("╚═══════════════════════════════════════════════════════╝")
	log.Info("")
	log.Info("Starting Local Scheduler...")
	log.Info("  Cluster ID: %s", *clusterID)
	log.Info("  Region: %s", *region)
	log.Info("  Zone: %s", *zone)
	log.Info("  Port: %d", *localPort)
	log.Info("  Redis: %s", *redisAddr)
	log.Info("  Control Plane: %s", *controlPlane)
	log.Info("  Kubernetes Namespace: %s", *namespace)
	log.Info("")

	ctx := context.Background()

	// ========================================================================
	// STEP 1: Connect to Redis
	// ========================================================================

	log.Info("Connecting to Redis...")
	redisClient, err := redis.NewRedisClient(*redisAddr, "", 0)
	if err != nil {
		log.Error("Failed to connect to Redis: %v", err)
		os.Exit(1)
	}
	defer redisClient.Close()
	log.Info("✓ Connected to Redis")

	// ========================================================================
	// STEP 2: Initialize GPU discovery and topology
	// ========================================================================

	log.Info("Initializing GPU discovery...")
	gpuDiscovery := gpu.NewGPUDiscovery(redisClient)

	gpus, err := gpuDiscovery.DiscoverGPUs(ctx)
	if err != nil {
		log.Warn("GPU discovery failed (non-fatal): %v", err)
	} else {
		log.Info("✓ Discovered %d GPUs", len(gpus))
	}

	topologyManager := gpu.NewGPUTopologyManager(redisClient, gpuDiscovery)
	log.Info("✓ Topology manager initialized")

	// ========================================================================
	// STEP 3: Create REAL Kubernetes client (not mock!)
	// ========================================================================

	log.Info("Initializing REAL Kubernetes client...")
	k8sClient, err := kubernetes.NewK8sClient(*namespace)
	if err != nil {
		log.Error("Failed to create K8s client: %v", err)
		log.Warn("Continuing with mock client (Pods won't actually be created)")
		k8sClient = executor.NewMockK8sClient() // Fallback to mock
	} else {
		log.Info("✓ Real K8s client initialized (using kubeconfig or in-cluster config)")
	}

	// ========================================================================
	// STEP 4: Initialize executor (for pod creation)
	// ========================================================================

	log.Info("Initializing executor...")
	executorConfig := &executor.ExecutorConfig{
		ClusterID:                *clusterID,
		Namespace:                *namespace,
		DefaultTimeout:           1 * time.Hour,
		DefaultMemoryMB:          1024,
		DefaultCPUMillis:         500,
		HealthCheckInterval:      5 * time.Second,
		MaxConcurrentJobs:        1000,
		ImageRegistry:            "docker.io",
		DefaultJobImage:          "ares-job:latest",
		RestartPolicy:            "OnFailure",
		ImagePullPolicy:          "IfNotPresent",
		EnableGPUSupport:         true,
		LogCollectionEnabled:     true,
		MetricsCollectionEnabled: true,
	}

	_, err = executor.NewExecutor(*clusterID, k8sClient, executorConfig)
	if err != nil {
		log.Error("Failed to create executor: %v", err)
		os.Exit(1)
	}
	log.Info("✓ Executor initialized")

	// ========================================================================
	// STEP 5: Initialize local scheduler
	// ========================================================================

	log.Info("Initializing local scheduler...")
	localScheduler := local.NewLocalScheduler(
		*clusterID,
		redisClient,
		gpuDiscovery,
		topologyManager,
	)
	log.Info("✓ Local scheduler initialized")

	// ========================================================================
	// STEP 6: AUTO-REGISTER CLUSTER WITH CONTROL PLANE
	// ========================================================================
	// CRITICAL: This enables automatic cluster discovery!
	// NO MANUAL /clusters/register endpoint needed!

	log.Info("")
	log.Info("╔─────────────────────────────────────────────────────╗")
	log.Info("║    AUTO-REGISTERING CLUSTER WITH CONTROL PLANE     ║")
	log.Info("╚─────────────────────────────────────────────────────╝")

	autoRegConfig := &cluster.AutoRegistrationConfig{
		ClusterID:          *clusterID,
		Region:             *region,
		Zone:               *zone,
		LocalSchedulerAddr: fmt.Sprintf("http://localhost:%d", *localPort),
		ControlPlaneURL:    *controlPlane,
		TotalGPUs:          len(gpus),
		TotalCPUs:          256,   // TODO: Get from kubelet or environment
		TotalMemoryGB:      512.0, // TODO: Get from kubelet or environment
		GPUTopology:        topologyManager.GetTopologyMap(),
	}

	err = cluster.AutoRegisterCluster(ctx, autoRegConfig)
	if err != nil {
		log.Error("Auto-registration failed: %v", err)
		log.Warn("Cluster will continue as AUTONOMOUS (control plane disconnected)")
		// Don't exit - cluster can work autonomously
	} else {
		log.Info("✓ Cluster auto-registered successfully")
	}

	log.Info("")

	// ========================================================================
	// STEP 7: START HTTP SERVER for local scheduler
	// ========================================================================

	log.Info("Starting HTTP server on port %d...", *localPort)
	server := local.NewLocalSchedulerServer(localScheduler, *localPort)
	if err := server.Start(); err != nil {
		log.Error("Failed to start server: %v", err)
		os.Exit(1)
	}

	log.Info("")
	log.Info("╔─────────────────────────────────────────────────────╗")
	log.Info("║        Local Scheduler Ready                        ║")
	log.Info("╚─────────────────────────────────────────────────────╝")
	log.Info("")
	log.Info("Endpoints:")
	log.Info("  POST   /schedule       - Schedule job on this cluster")
	log.Info("  GET    /health         - Health check")
	log.Info("  GET    /status         - Cluster status")
	log.Info("  GET    /metrics        - Metrics")
	log.Info("")

	// ========================================================================
	// STEP 8: START AUTOMATIC HEARTBEAT
	// ========================================================================
	// CRITICAL: Enables health detection and autonomous failover
	// Heartbeat will fail gracefully if control plane unreachable

	log.Info("Starting automatic heartbeat (every 10 seconds)...")

	heartbeatConfig := &cluster.HeartbeatConfig{
		ClusterID:       *clusterID,
		ControlPlaneURL: *controlPlane,
		Interval:        10 * time.Second,
		GetLoadFunc: func() map[string]interface{} {
			return localScheduler.GetClusterLoad()
		},
	}

	go cluster.StartHeartbeat(ctx, heartbeatConfig)

	log.Info("✓ Heartbeat started")
	log.Info("")

	// ========================================================================
	// STEP 9: Graceful shutdown
	// ========================================================================

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan

	log.Info("")
	log.Info("Received signal: %v", sig)
	log.Info("Shutting down gracefully...")

	if err := server.Stop(10 * time.Second); err != nil {
		log.Error("Shutdown error: %v", err)
		os.Exit(1)
	}

	log.Info("Shutdown complete")
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

// initializeLogger: Initialize the logger
func initializeLogger(logLevel string) *logger.Logger {
	log := logger.Get()
	return log
}
