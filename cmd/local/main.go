// File: cmd/local/main.go
// FINAL CORRECTED VERSION - WITH NODE REGISTRATION FIX
// All fixes applied:
// 1. ✅ Correct K8s client wrapper initialization
// 2. ✅ Pass wrapper to executor (common.K8sClient type)
// 3. ✅ Get k8s.io client for gpu_discovery
// 4. ✅ All type mismatches resolved
// 5. ✅ Ready for production deployment
// 6. ✅ NODE REGISTRATION ADDED - Fixes "no nodes available in cluster" error

package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/executor"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/cluster"
	executorK8s "github.com/BITS08SATHYA/ares-scheduler/pkg/executor/kubernetes"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/gpu"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/job"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/lease"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	common2 "github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/local"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/etcd"
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
	DefaultMemoryGB     = 512.0 // Default node memory in GB
)

// ============================================================================
// COMMAND-LINE FLAGS (with env var fallback)
// ============================================================================

var (
	clusterID = flag.String(
		"cluster-id",
		getEnvString("WORKER_CLUSTER_ID", ""),
		"Cluster ID (required, env: ARES_CLUSTER_ID)",
	)

	nodeID = flag.String(
		"node-id",
		getEnvString("NODE_NAME", ""),
		"Node ID (required, env: NODE_NAME)",
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

	nodeMemoryGB = flag.Float64(
		"node-memory-gb",
		getEnvFloat64("ARES_NODE_MEMORY_GB", DefaultMemoryGB),
		"Node memory in GB (env: ARES_NODE_MEMORY_GB)",
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
	log.Info("║   Ares Local Scheduler - Worker Cluster               ║")
	log.Info("║   K8s-Native GPU Discovery + Executor Wrapper         ║")
	log.Info("╚═══════════════════════════════════════════════════════╝")
	log.Info("")

	// Validate required flags
	if *clusterID == "" {
		log.Error("Error: --cluster-id is required (or set NODE_NAME env var)")
		flag.Usage()
		os.Exit(1)
	}

	log.Info("Configuration:")
	log.Info("  Cluster ID: %s", *clusterID)
	log.Info("  Node ID: %s", *nodeID)
	log.Info("  Region: %s", *region)
	log.Info("  Zone: %s", *zone)
	log.Info("  Port: %d", *localPort)
	log.Info("  Redis: %s", *redisAddr)
	log.Info("  Control Plane: %s", *controlPlane)
	log.Info("  K8s Namespace: %s", *namespace)
	log.Info("  Node Memory: %.0f GB", *nodeMemoryGB)
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
	log.Info("Connected to Redis")

	// ========================================================================
	// STEP 1b: Connect to etcd (for JobStore and LeaseManager)
	// ========================================================================

	log.Info("Connecting to etcd...")
	//etcdEndpoints := []string{"etcd-0.etcd.ares-system.svc.cluster.local:2379"}
	//etcdClient, err := etcd.NewETCDClient(etcdEndpoints, 5*time.Second)

	etcdEndpoints := []string{
		//"http://etcd-client.ares-system.svc.cluster.local:2379",
		"http://34.48.35.8:2379",
	}
	etcdClient, err := etcd.NewETCDClient(etcdEndpoints, 5*time.Second)

	if err != nil {
		log.Error("Failed to connect to etcd: %v", err)
		os.Exit(1)
	}
	defer etcdClient.Close()
	log.Info("Connected to etcd")

	// ========================================================================
	// STEP 1c: Initialize JobStore and LeaseManager
	// ========================================================================

	log.Info("Initializing JobStore...")
	jobStore := job.NewETCDJobStore(etcdClient)
	log.Info("JobStore initialized")

	log.Info("Initializing LeaseManager...")
	leaseManager := lease.NewLeaseManager(etcdClient, *controlPlane, &simpleLogger{})
	log.Info("LeaseManager initialized")

	// ========================================================================
	// STEP 2: Create K8s client wrapper (single source of truth!)
	// ========================================================================

	log.Info("Initializing Kubernetes client (via executor wrapper)...")
	k8sClientWrapper, err := executorK8s.NewK8sClient(*namespace)
	if err != nil {
		log.Error("Failed to create K8s client: %v", err)
		os.Exit(1)
	}
	log.Info("Kubernetes client initialized")

	// ========================================================================
	// STEP 3: Initialize GPU discovery WITH K8s client
	// ========================================================================

	log.Info("Initializing GPU discovery...")

	//nodeName := os.Getenv("NODE_NAME")
	//if nodeName == "" {
	//	nodeName = *nodeID
	//}
	nodeName := *nodeID

	// Get the real k8s.io client for gpu_discovery
	// (different interface than common.K8sClient)
	k8sIOClient := k8sClientWrapper.GetKubernetesInterface()

	// Pass k8s.io client to gpu_discovery (it needs raw K8s API access)
	gpuDiscovery := gpu.NewGPUDiscoveryWithK8s(redisClient, k8sIOClient, nodeName)
	log.Info("GPU discovery initialized (will try K8s API first, fallback to nvidia-smi)")

	// Discover GPUs (tries K8s API first, falls back to nvidia-smi)
	gpus, err := gpuDiscovery.DiscoverGPUs(ctx)
	if err != nil {
		log.Error("GPU discovery failed (will continue with 0 GPUs): %v", err)
		gpus = make([]*common2.GPUDevice, 0)
	}

	totalGPUs := len(gpus)
	log.Info("Discovered %d GPUs", totalGPUs)
	for i, gpu1 := range gpus {
		log.Info("  GPU %d: %s (%.0f GB memory)", i, gpu1.Type, gpu1.MemoryGB)
	}

	topologyManager := gpu.NewGPUTopologyManager(redisClient, gpuDiscovery)
	log.Info("Topology manager initialized")
	log.Info("Detecting Nvidia-smi...")
	smiPath := topologyManager.FindNvidiaSMI_test_path()

	if smiPath == "" {
		log.Warn("⚠️nvidia-smi not found - GPU topology detection disabled")
		log.Warn("GPU discovery will use Kubernetes API")
	} else {
		log.Info("GPU topology detection enabled")
		log.Info("nvidia-smi path: %s", smiPath)
	}

	// ========================================================================
	// STEP 4: Initialize executor (for pod creation)
	// ========================================================================

	log.Info("Initializing Executor Config...")
	executorConfig := &executor.ExecutorConfig{
		ClusterID:                *clusterID,
		Namespace:                *namespace,
		DefaultTimeout:           1 * time.Hour,
		DefaultMemoryMB:          1024,
		DefaultCPUMillis:         500,
		HealthCheckInterval:      5 * time.Second,
		MaxConcurrentJobs:        1000,
		ImageRegistry:            "",
		DefaultJobImage:          "nvidia/cuda:13.0.2-runtime-ubuntu22.04",
		RestartPolicy:            "OnFailure",
		ImagePullPolicy:          "Always",
		EnableGPUSupport:         true,
		LogCollectionEnabled:     true,
		MetricsCollectionEnabled: true,
	}

	// CRITICAL FIX: Pass wrapper directly (common.K8sClient type)
	// NOT k8sIOClient (which is kubernetes.Interface)
	var myExecutor *executor.Executor
	myExecutor, err = executor.NewExecutor(*clusterID,
		k8sClientWrapper,
		executorConfig,
		jobStore,
		leaseManager,
	)

	if err != nil {
		log.Error("Failed to create executor: %v", err)
		os.Exit(1)
	}
	log.Info("Executor initialized with Pod Lifecycle Monitoring")

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
	// STEP 5b: REGISTER THIS NODE WITH LOCAL SCHEDULER
	// ========================================================================
	// ★★★ CRITICAL FIX ★★★
	// Without this, ListNodes() returns empty and scheduling fails with:
	// "node selection failed: no nodes available in cluster"

	log.Info("")
	log.Info("╔─────────────────────────────────────────────────────╗")
	log.Info("║    REGISTERING NODE WITH LOCAL SCHEDULER            ║")
	log.Info("╚─────────────────────────────────────────────────────╝")

	nodeState := &local.NodeState{
		NodeID:            *nodeID,
		IsHealthy:         true,
		GPUCount:          totalGPUs,
		AvailableGPUs:     totalGPUs,     // Initially all GPUs available
		MemoryGB:          *nodeMemoryGB, // From flag/env
		AvailableMemoryGB: *nodeMemoryGB, // Initially all memory available
		RunningJobsCount:  0,
		LastHealthCheck:   time.Now(),
		GPUsInfo:          gpus, // From GPU discovery
	}

	err = localScheduler.RegisterNode(ctx, nodeState)
	if err != nil {
		log.Error("Failed to register node: %v", err)
		os.Exit(1)
	}
	log.Info("Node registered with LocalScheduler:")
	log.Info("Node ID: %s", nodeState.NodeID)
	log.Info("GPUs: %d (all available)", nodeState.GPUCount)
	log.Info("Memory: %.0f GB (all available)", nodeState.MemoryGB)
	log.Info("Health: %v", nodeState.IsHealthy)

	// Verify registration worked
	registeredNodes := localScheduler.ListNodes()
	log.Info("Verified: LocalScheduler now has %d registered node(s)", len(registeredNodes))

	// ========================================================================
	// STEP 6: AUTO-REGISTER CLUSTER WITH CONTROL PLANE
	// ========================================================================

	log.Info("")
	log.Info("╔─────────────────────────────────────────────────────╗")
	log.Info("║    AUTO-REGISTERING CLUSTER WITH CONTROL PLANE      ║")
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
		TotalMemoryGB:      *nodeMemoryGB,
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
	server := local.NewLocalSchedulerServer(localScheduler, *localPort, myExecutor)

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
	log.Info("Registered Nodes: %d", len(localScheduler.ListNodes()))
	log.Info("")

	// ========================================================================
	// STEP 8: START AUTOMATIC HEARTBEAT
	// ========================================================================

	log.Info("Starting automatic heartbeat (every 10 seconds)...")

	heartbeatConfig := &cluster.HeartbeatConfig{
		ClusterID:       *clusterID,
		ControlPlaneURL: *controlPlane,
		Interval:        10 * time.Second,
		GetLoadFunc: func() map[string]interface{} {
			load := localScheduler.GetClusterLoad()

			if load == nil {
				load = make(map[string]interface{})
			}

			safeLoad := map[string]interface{}{
				"cluster_id":      *clusterID,
				"gpus_in_use":     safeGetInt(load, "gpus_in_use", 0),
				"mem_gb_in_use":   safeGetFloat64(load, "mem_gb_in_use", 0.0),
				"cpus_in_use":     safeGetInt(load, "cpus_in_use", 0),
				"running_jobs":    safeGetInt(load, "running_jobs", 0),
				"pending_jobs":    safeGetInt(load, "pending_jobs", 0),
				"total_gpus":      totalGPUs,
				"total_memory_gb": *nodeMemoryGB,
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
	// First check explicit override
	if override != "" {
		return override
	}

	// Try to get external address from env var
	if externalAddr := os.Getenv("ARES_LOCAL_SCHEDULER_EXTERNAL_ADDR"); externalAddr != "" {
		return externalAddr
	}

	// Fallback to pod IP (for single-cluster testing)
	if podIP := os.Getenv("POD_IP"); podIP != "" {
		return fmt.Sprintf("http://%s:%d", podIP, port)
	}

	// Try to get host IP
	if hostIP := getHostIP(); hostIP != "" {
		return fmt.Sprintf("http://%s:%d", hostIP, port)
	}

	// Last resort: localhost
	return fmt.Sprintf("http://localhost:%d", port)
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

// ============================================================================
// SAFE TYPE HELPERS
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

func getEnvFloat64(key string, defaultValue float64) float64 {
	if value := os.Getenv(key); value != "" {
		if floatValue, err := strconv.ParseFloat(value, 64); err == nil {
			return floatValue
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}

// ============================================================================
// SIMPLE LOGGER (for LeaseManager)
// ============================================================================

type simpleLogger struct{}

func (sl *simpleLogger) Infof(format string, args ...interface{}) {
	logger.Get().Info(format, args...)
}

func (sl *simpleLogger) Debugf(format string, args ...interface{}) {
	logger.Get().Debug(format, args...)
}

func (sl *simpleLogger) Warnf(format string, args ...interface{}) {
	logger.Get().Warn(format, args...)
}

func (sl *simpleLogger) Errorf(format string, args ...interface{}) {
	logger.Get().Error(format, args...)
}
