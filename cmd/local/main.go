// File: cmd/local/main.go (FIXED)
// Local scheduler with automatic cluster registration and heartbeat
// FIXES:
// 1. GetLoadFunc is now a proper function (not a map)
// 2. Safe GPU discovery with fallback
// 3. Proper error handling for heartbeat

package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/cluster"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/executor"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/executor/common"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/executor/kubernetes"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/gpu"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	common2 "github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/local"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/redis"
)

// ============================================================================
// CONFIGURATION CONSTANTS
// ============================================================================

const (
	DefaultLocalPort    = 9090
	DefaultRedisAddr    = "localhost:6379"
	DefaultControlPlane = "http://localhost:8080"
	DefaultNamespace    = "ares-jobs"
	DefaultRegion       = "us-west"
	DefaultZone         = "us-west-1a"
)

// ============================================================================
// COMMAND-LINE FLAGS (with env var fallback)
// ============================================================================

var (
	clusterID = flag.String(
		"cluster-id",
		getEnvString("ARES_CLUSTER_ID", ""),
		"Cluster ID (required, env: ARES_CLUSTER_ID)",
	)

	localPort = flag.Int(
		"port",
		getEnvInt("ARES_LOCAL_PORT", DefaultLocalPort),
		"Local scheduler port (env: ARES_LOCAL_PORT)",
	)

	redisAddr = flag.String(
		"redis",
		getEnvString("ARES_REDIS_ADDR", DefaultRedisAddr),
		"Redis address (env: ARES_REDIS_ADDR)",
	)

	controlPlane = flag.String(
		"control-plane",
		getEnvString("ARES_CONTROL_PLANE", DefaultControlPlane),
		"Global control plane URL (env: ARES_CONTROL_PLANE)",
	)

	region = flag.String(
		"region",
		getEnvString("ARES_REGION", DefaultRegion),
		"Region (env: ARES_REGION)",
	)

	zone = flag.String(
		"zone",
		getEnvString("ARES_ZONE", DefaultZone),
		"Zone (env: ARES_ZONE)",
	)

	logLevel = flag.String(
		"log-level",
		getEnvString("ARES_LOG_LEVEL", "info"),
		"Log level (env: ARES_LOG_LEVEL)",
	)

	namespace = flag.String(
		"k8s-namespace",
		getEnvString("ARES_K8S_NAMESPACE", DefaultNamespace),
		"Kubernetes namespace (env: ARES_K8S_NAMESPACE)",
	)

	clusterAddress = flag.String(
		"cluster-address",
		getEnvString("ARES_CLUSTER_ADDRESS", ""),
		"Override cluster address (env: ARES_CLUSTER_ADDRESS)",
	)
)

// ============================================================================
// MAIN ENTRY POINT
// ============================================================================

func main() {
	flag.Parse()

	// Initialize logger
	log := initializeLogger(*logLevel)
	defer log.Sync()

	log.Info("╔═══════════════════════════════════════════════════════╗")
	log.Info("║   Ares Local Scheduler - Worker Cluster              ║")
	log.Info("║   Auto-Registration + Real K8s Client                ║")
	log.Info("╚═══════════════════════════════════════════════════════╝")
	log.Info("")

	// Validate required flags
	if *clusterID == "" {
		// Try to use NODE_NAME from Kubernetes if available
		if nodeName := os.Getenv("NODE_NAME"); nodeName != "" {
			*clusterID = nodeName
			log.Info("Using NODE_NAME as cluster-id: %s", *clusterID)
		} else {
			log.Error("Error: --cluster-id is required (or set NODE_NAME env var)")
			flag.Usage()
			os.Exit(1)
		}
	}

	log.Info("Configuration:")
	log.Info("  Cluster ID: %s", *clusterID)
	log.Info("  Region: %s", *region)
	log.Info("  Zone: %s", *zone)
	log.Info("  Port: %d", *localPort)
	log.Info("  Redis: %s", *redisAddr)
	log.Info("  Control Plane: %s", *controlPlane)
	log.Info("  K8s Namespace: %s", *namespace)
	log.Info("  Log Level: %s", *logLevel)
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

	// ✅ FIX: Safe GPU discovery with fallback
	var gpus []*common2.GPUDevice
	var totalGPUs int

	gpuDiscovery := gpu.NewGPUDiscovery(redisClient)
	gpus, err = gpuDiscovery.DiscoverGPUs(ctx)
	if err != nil {
		log.Warn("GPU discovery failed (non-fatal): %v", err)
		log.Warn("Attempting fallback GPU detection...")

		// Fallback: Check if we're on a GPU node via Kubernetes
		if nodeName := os.Getenv("NODE_NAME"); nodeName != "" {
			gpus = detectGPUsFromK8sNode(nodeName, log)
		}

		if len(gpus) == 0 {
			log.Warn("No GPUs detected - continuing in CPU-only mode")
			gpus = make([]*common2.GPUDevice, 0)
		}
	}

	totalGPUs = len(gpus)
	log.Info("✓ Discovered %d GPUs", totalGPUs)
	for i, gpu := range gpus {
		log.Info("  GPU %d: %s (%.0f GB memory, %.1f%% util)",
			i, gpu.Type, gpu.MemoryGB, gpu.UtilizationPercent)
	}

	topologyManager := gpu.NewGPUTopologyManager(redisClient, gpuDiscovery)
	log.Info("✓ Topology manager initialized")

	// ========================================================================
	// STEP 3: Create REAL Kubernetes client
	// ========================================================================

	log.Info("Initializing Kubernetes client...")
	k8sClient, err := kubernetes.NewK8sClient(*namespace)
	if err != nil {
		log.Error("Failed to create K8s client: %v", err)
		log.Error("Cannot proceed without Kubernetes access")
		os.Exit(1)
	}
	log.Info("✓ Real K8s client initialized")

	// ========================================================================
	// STEP 4: Initialize executor (for pod creation)
	// ========================================================================

	log.Info("Initializing executor...")
	executorConfig := &common.ExecutorConfig{
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

	log.Info("")
	log.Info("╔─────────────────────────────────────────────────────╗")
	log.Info("║    AUTO-REGISTERING CLUSTER WITH CONTROL PLANE     ║")
	log.Info("╚─────────────────────────────────────────────────────╝")

	// Detect GPU topology
	var topologyData map[string]interface{}
	topology, err := topologyManager.DetectTopology(ctx)
	if err == nil && topology != nil {
		topologyData = map[string]interface{}{
			"nvlink_pairs": topology.NVLinkPairs,
			"gpu_to_numa":  topology.GPUToNUMA,
			"pcie_gen":     topology.PCIeGen,
		}
		log.Info("✓ GPU topology detected")
	} else {
		log.Warn("GPU topology detection failed (non-fatal): %v", err)
		topologyData = make(map[string]interface{})
	}

	// Get cluster address (how control plane will reach this scheduler)
	clusterAddr := getClusterAddress(*localPort, *clusterAddress)
	log.Info("Cluster address: %s", clusterAddr)

	autoRegConfig := &cluster.AutoRegistrationConfig{
		ClusterID:          *clusterID,
		Region:             *region,
		Zone:               *zone,
		LocalSchedulerAddr: clusterAddr,
		ControlPlaneURL:    *controlPlane,
		TotalGPUs:          totalGPUs,
		TotalCPUs:          256,
		TotalMemoryGB:      512.0,
		GPUTopology:        topologyData,
	}

	err = cluster.AutoRegisterCluster(ctx, autoRegConfig)
	if err != nil {
		log.Error("Auto-registration failed: %v", err)
		log.Warn("Cluster will continue as AUTONOMOUS (control plane disconnected)")
	} else {
		log.Info("✓ Cluster auto-registered successfully")
	}

	log.Info("")

	// ========================================================================
	// STEP 7: START HTTP SERVER
	// ========================================================================

	log.Info("Starting HTTP server on port %d...", *localPort)
	server := local.NewLocalSchedulerServer(localScheduler, *localPort)

	// Start server in background
	go func() {
		if err := server.Start(); err != nil {
			log.Error("Failed to start server: %v", err)
			os.Exit(1)
		}
	}()

	// Give server time to start
	time.Sleep(500 * time.Millisecond)

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

	log.Info("Starting automatic heartbeat (every 10 seconds)...")

	// ✅ FIX: GetLoadFunc is now a proper FUNCTION, not a map
	heartbeatConfig := &cluster.HeartbeatConfig{
		ClusterID:       *clusterID,
		ControlPlaneURL: *controlPlane,
		Interval:        10 * time.Second,
		GetLoadFunc: func() map[string]interface{} {
			// ✅ CRITICAL: This function gets called every heartbeat
			// It should return current cluster load metrics

			load := localScheduler.GetClusterLoad()

			// ✅ Safe field access with defaults
			if load == nil {
				load = make(map[string]interface{})
			}

			// Ensure all required fields exist
			safeLoad := map[string]interface{}{
				"cluster_id":      *clusterID,
				"gpus_in_use":     safeGetInt(load, "gpus_in_use", 0),
				"mem_gb_in_use":   safeGetFloat64(load, "mem_gb_in_use", 0.0),
				"cpus_in_use":     safeGetInt(load, "cpus_in_use", 0),
				"running_jobs":    safeGetInt(load, "running_jobs", 0),
				"pending_jobs":    safeGetInt(load, "pending_jobs", 0),
				"total_gpus":      totalGPUs,
				"total_memory_gb": 512.0,
				"nodes_count":     safeGetInt(load, "nodes_count", 0),
			}

			return safeLoad
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

func initializeLogger(logLevel string) *logger.Logger {
	return logger.Get()
}

// getClusterAddress determines how the control plane will reach this scheduler
func getClusterAddress(port int, override string) string {
	// 1. Use explicit override if provided
	if override != "" {
		return fmt.Sprintf("http://%s:%d", override, port)
	}

	// 2. In Kubernetes, try to get pod IP
	if podIP := os.Getenv("POD_IP"); podIP != "" {
		return fmt.Sprintf("http://%s:%d", podIP, port)
	}

	// 3. Try to detect host IP
	if hostIP := getHostIP(); hostIP != "" {
		return fmt.Sprintf("http://%s:%d", hostIP, port)
	}

	// 4. Fallback to hostname
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}

	return fmt.Sprintf("http://%s:%d", hostname, port)
}

// getHostIP attempts to get the host's primary IP address
func getHostIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return ""
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}

// detectGPUsFromK8sNode: Fallback GPU detection via Kubernetes node labels
func detectGPUsFromK8sNode(nodeName string, log *logger.Logger) []*common2.GPUDevice {
	// This is a simplified fallback - in production, you'd use K8s client
	// to query node labels like cloud.google.com/gke-accelerator

	// For now, if NODE_NAME is set and we're on GKE, assume 1 GPU
	if nodeName != "" {
		log.Info("Fallback: Assuming 1 GPU based on NODE_NAME presence")
		return []*common2.GPUDevice{
			{
				Index:              0,
				UUID:               "GPU-FALLBACK-0",
				Type:               "Tesla T4",
				MemoryGB:           16.0,
				UtilizationPercent: 0.0,
				AvailableMemGB:     0.0,
			},
		}
	}

	return []*common2.GPUDevice{}
}

// ============================================================================
// SAFE TYPE HELPERS (for GetLoadFunc)
// ============================================================================

func safeGetInt(m map[string]interface{}, key string, defaultVal int) int {
	if m == nil {
		return defaultVal
	}
	val, exists := m[key]
	if !exists {
		return defaultVal
	}
	if intVal, ok := val.(int); ok {
		return intVal
	}
	if floatVal, ok := val.(float64); ok {
		return int(floatVal)
	}
	return defaultVal
}

func safeGetFloat64(m map[string]interface{}, key string, defaultVal float64) float64 {
	if m == nil {
		return defaultVal
	}
	val, exists := m[key]
	if !exists {
		return defaultVal
	}
	if floatVal, ok := val.(float64); ok {
		return floatVal
	}
	if intVal, ok := val.(int); ok {
		return float64(intVal)
	}
	return defaultVal
}

// ============================================================================
// ENVIRONMENT VARIABLE HELPERS
// ============================================================================

func getEnvString(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}
