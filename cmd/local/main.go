// cmd/local/main.go
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/executor"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/gpu"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/local"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/redis"
)

const (
	DefaultLocalPort    = 9090
	DefaultRedisAddr    = "localhost:6379"
	DefaultControlPlane = "http://localhost:8080"
)

var (
	clusterID    = flag.String("cluster-id", "", "Cluster ID (required)")
	localPort    = flag.Int("port", DefaultLocalPort, "Local scheduler port")
	redisAddr    = flag.String("redis", DefaultRedisAddr, "Redis address")
	controlPlane = flag.String("control-plane", DefaultControlPlane, "Global control plane URL")
	region       = flag.String("region", "us-west", "Region")
	zone         = flag.String("zone", "us-west-2a", "Zone")
	logLevel     = flag.String("log-level", "info", "Log level")
)

func main() {

	// Parse command line flags
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

	log.Info("═══════════════════════════════════════════════════════")
	log.Info("   Ares Local Scheduler - Worker Cluster")
	log.Info("═══════════════════════════════════════════════════════")
	log.Info("")
	log.Info("Starting Local Scheduler...")
	log.Info("  Cluster ID: %s", *clusterID)
	log.Info("  Region: %s", *region)
	log.Info("  Zone: %s", *zone)
	log.Info("  Port: %d", *localPort)
	log.Info("  Redis: %s", *redisAddr)
	log.Info("  Control Plane: %s", *controlPlane)
	log.Info("")

	ctx := context.Background()

	// Connect to Redis
	log.Info("Connecting to Redis...")
	redisClient, err := redis.NewRedisClient(*redisAddr, "", 0)
	if err != nil {
		log.Error("Failed to connect to Redis: %v", err)
		os.Exit(1)
	}
	defer redisClient.Close()
	log.Info("Connected to Redis")

	// Initialize GPU discovery
	log.Info("Initializing GPU discovery...")
	gpuDiscovery := gpu.NewGPUDiscovery(redisClient)

	// Discover GPUs
	gpus, err := gpuDiscovery.DiscoverGPUs(ctx)
	if err != nil {
		log.Warn("GPU discovery failed (non-fatal): %v", err)
	} else {
		log.Info("Discovered %d GPUs", len(gpus))
	}

	// Initialize topology manager
	topologyManager := gpu.NewGPUTopologyManager(redisClient, gpuDiscovery)
	log.Info("Topology manager initialized")

	// Initialize executor
	log.Info("Initializing executor...")
	mockK8sClient := executor.NewMockK8sClient()
	executorConfig := executor.DefaultExecutorConfig
	executorConfig.ClusterID = *clusterID

	_, err = executor.NewExecutor(*clusterID, mockK8sClient, executorConfig)
	if err != nil {
		log.Error("Failed to create executor: %v", err)
		os.Exit(1)
	}
	log.Info("✓ Executor initialized")

	// Initialize local scheduler
	log.Info("Initializing local scheduler...")
	localScheduler := local.NewLocalScheduler(
		*clusterID,
		redisClient,
		gpuDiscovery,
		topologyManager,
	)
	log.Info("Local scheduler initialized")

	// Register this cluster with global control plane
	log.Info("Registering with global control plane...")
	err = registerWithControlPlane(ctx, *clusterID, *region, *zone, *localPort, *controlPlane, len(gpus))
	if err != nil {
		log.Error("Failed to register with control plane: %v", err)
		log.Warn("Continuing anyway - cluster can be registered manually")
	} else {
		log.Info("Registered with control plane")
	}

	// Start HTTP server
	log.Info("Starting HTTP server on port %d...", *localPort)
	server := local.NewLocalSchedulerServer(localScheduler, *localPort)
	if err := server.Start(); err != nil {
		log.Error("Failed to start server: %v", err)
		os.Exit(1)
	}

	log.Info("")
	log.Info("Local Scheduler ready")
	log.Info("")
	log.Info("Endpoints:")
	log.Info("  POST   /schedule  - Schedule job on this cluster")
	log.Info("  GET    /health    - Health check")
	log.Info("  GET    /status    - Cluster status")
	log.Info("  GET    /metrics   - Metrics")
	log.Info("")

	// Start heartbeat to global control plane (every 10 seconds)
	go startHeartbeat(ctx, *clusterID, *controlPlane, localScheduler)

	// Graceful shutdown
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

// registerWithControlPlane: Register cluster with global control plane
func registerWithControlPlane(ctx context.Context, clusterID, region, zone string, port int, controlPlaneURL string, gpuCount int) error {
	// Implementation: POST to {controlPlaneURL}/clusters/register
	// For now, just log
	log := logger.Get()
	log.Info("Would register: POST %s/clusters/register", controlPlaneURL)
	log.Info("  cluster_id: %s", clusterID)
	log.Info("  region: %s", region)
	log.Info("  zone: %s", zone)
	log.Info("  local_scheduler_addr: http://localhost:%d", port)
	log.Info("  total_gpus: %d", gpuCount)
	return nil
}

// startHeartbeat: Send periodic heartbeats to control plane
func startHeartbeat(ctx context.Context, clusterID, controlPlaneURL string, scheduler *local.LocalScheduler) {
	log := logger.Get()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Get cluster load
			load := scheduler.GetClusterLoad()

			log.Debug("Sending heartbeat to control plane...")
			log.Debug("Cluster: %s", clusterID)
			log.Debug("GPUs in use: %v", load["gpus_in_use"])
			log.Debug("Running jobs: %v", load["running_jobs"])

			// TODO: Actually POST to {controlPlaneURL}/cluster-heartbeat
		}
	}
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

// initializeLogger: Initialize the logger based on log level
func initializeLogger(logLevel string) *logger.Logger {
	// This creates a global logger instance
	// Adjust based on your logger.Logger implementation
	log := logger.Get()

	// Set log level if your logger supports it
	// (This depends on your logger implementation)
	// log.SetLevel(logLevel)

	return log
}
