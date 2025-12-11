// File: cmd/global/main.go (FINAL CORRECTED)
// Entry point for Ares Scheduler API Gateway
// FIXES:
// 1. Single etcd.endpoint (not plural)
// 2. No comma-separated parsing needed
// 3. Simplified configuration

package main

import (
	"flag"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/api/gateway"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/cluster"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/redis"
)

// ============================================================================
// CONFIGURATION CONSTANTS
// ============================================================================

const (
	// Server
	DefaultGatewayPort    = 8080
	DefaultRequestTimeout = 30 * time.Second
	DefaultMaxRequestSize = 1 << 20 // 1MB

	// Storage - ✅ FIX: Single endpoint, not plural
	DefaultETCDEndpoint = "localhost:2379"
	DefaultRedisAddr    = "localhost:6379"
	DefaultControlPlane = "localhost:8080"

	// Graceful shutdown
	ShutdownTimeout = 10 * time.Second
)

// ============================================================================
// COMMAND-LINE FLAGS (with env var fallback)
// ============================================================================

var (
	// Gateway flags
	gatewayPort = flag.Int(
		"gateway.port",
		getEnvInt("ARES_GATEWAY_PORT", DefaultGatewayPort),
		"HTTP port for API gateway (env: ARES_GATEWAY_PORT)",
	)
	requestTimeout = flag.Duration(
		"gateway.timeout",
		DefaultRequestTimeout,
		"Request timeout",
	)

	// Storage flags - ✅ FIX: Single endpoint
	etcdEndpoint = flag.String(
		"etcd.endpoint",
		getEnvString("ARES_ETCD_ENDPOINT", DefaultETCDEndpoint),
		"etcd endpoint (env: ARES_ETCD_ENDPOINT)",
	)

	redisAddr = flag.String(
		"redis.addr",
		getEnvString("ARES_REDIS_ADDR", DefaultRedisAddr),
		"Redis address (env: ARES_REDIS_ADDR)",
	)

	controlPlane = flag.String(
		"control-plane.addr",
		getEnvString("ARES_CONTROL_PLANE", DefaultControlPlane),
		"Control plane address for lease metadata (env: ARES_CONTROL_PLANE)",
	)

	// Logging
	logLevel = flag.String(
		"log.level",
		getEnvString("ARES_LOG_LEVEL", "info"),
		"Log level (debug, info, warn, error, env: ARES_LOG_LEVEL)",
	)

	// Feature flags
	enableCoordinator = flag.Bool(
		"enable-coordinator",
		getEnvBool("ARES_ENABLE_COORDINATOR", true),
		"Enable job coordinator (env: ARES_ENABLE_COORDINATOR)",
	)

	enableMetrics = flag.Bool(
		"enable-metrics",
		getEnvBool("ARES_ENABLE_METRICS", true),
		"Enable /metrics endpoint (env: ARES_ENABLE_METRICS)",
	)
)

// ============================================================================
// MAIN ENTRY POINT
// ============================================================================

func main() {
	// Parse command-line flags
	flag.Parse()

	// Initialize logger
	log := initializeLogger(*logLevel)
	defer func() {
		if log != nil {
			_ = log.Sync()
		}
	}()

	log.Info("")
	log.Info("╔════════════════════════════════════════════════════════╗")
	log.Info("║     Ares Scheduler - API Gateway                       ║")
	log.Info("║     Multi-Cluster GPU Scheduler with Exactly-Once      ║")
	log.Info("╚════════════════════════════════════════════════════════╝")
	log.Info("")
	log.Info("Starting Ares Gateway (all 10 layers)...")
	log.Info("")

	// Create gateway configuration
	gatewayConfig := &gateway.GatewayConfig{
		Port:             *gatewayPort,
		RequestTimeout:   *requestTimeout,
		MaxRequestSize:   DefaultMaxRequestSize,
		EnableCORS:       true,
		EnablePrometheus: *enableMetrics,
		HealthCheckPath:  "/health",
		MetricsPath:      "/metrics",
	}

	log.Info("Configuration:")
	log.Info("  Gateway Port: %d", gatewayConfig.Port)
	log.Info("  etcd Endpoint: %s", *etcdEndpoint) // ✅ FIX: Singular
	log.Info("  Redis: %s", *redisAddr)
	log.Info("  Control Plane: %s", *controlPlane)
	log.Info("  Log Level: %s", *logLevel)
	log.Info("  Coordinator: %v", *enableCoordinator)
	log.Info("  Metrics: %v", *enableMetrics)
	log.Info("")

	// Initialize API gateway
	var apiGateway *gateway.APIGateway
	var err error

	if *enableCoordinator {
		log.Info("Initializing API Gateway WITH Job Coordinator (10-layer pipeline)...")

		// Initialize Redis for cluster manager
		redisClient, err := redis.NewRedisClient(*redisAddr, "", 0)
		if err != nil {
			log.Error("Failed to connect to Redis: %v", err)
			os.Exit(1)
		}

		// Initialize cluster manager
		clusterManager := cluster.NewClusterManager(redisClient, &cluster.ClusterConfig{
			AutoHeartbeatInterval:  10 * time.Second,
			HealthCheckInterval:    30 * time.Second,
			HeartbeatTimeout:       60 * time.Second,
			AutonomyEnabled:        true,
			MaxConsecutiveFailures: 3,
		})

		// ✅ FIX: Wrap single endpoint in array (gateway expects []string)
		etcdEndpoints := []string{*etcdEndpoint}

		// Initialize gateway with coordinator
		gatewayWithCoordinator, err := gateway.NewAPIGatewayWithCoordinator(
			*controlPlane,
			etcdEndpoints, // Single endpoint wrapped in array
			*redisAddr,
			gatewayConfig,
			clusterManager,
		)
		if err != nil {
			log.Error("Failed to initialize API Gateway with Coordinator: %v", err)
			os.Exit(1)
		}

		if gatewayWithCoordinator == nil || gatewayWithCoordinator.APIGateway == nil {
			log.Error("API Gateway initialization returned nil")
			os.Exit(1)
		}

		apiGateway = gatewayWithCoordinator.APIGateway
		log.Info("✓ API Gateway initialized with Job Coordinator")

	} else {
		log.Warn("Running in coordinator-disabled mode (not recommended for production)")

		apiGateway, err = gateway.NewAPIGateway(
			nil,
			gatewayConfig,
		)
		if err != nil {
			log.Error("Failed to initialize API Gateway: %v", err)
			os.Exit(1)
		}

		log.Info("✓ API Gateway initialized (limited features)")
	}

	// Start HTTP server
	log.Info("")
	log.Info("Starting HTTP server...")

	if err := apiGateway.Start(); err != nil {
		log.Error("Failed to start API Gateway: %v", err)
		os.Exit(1)
	}

	log.Info("")
	log.Info("╔════════════════════════════════════════════════════════╗")
	log.Info("║              Ares Gateway Ready                        ║")
	log.Info("╚════════════════════════════════════════════════════════╝")
	log.Info("")
	log.Info("Server listening on port %d", *gatewayPort)
	log.Info("")
	log.Info("Available Endpoints:")
	log.Info("  POST   /schedule                - Schedule a job")
	log.Info("  GET    /health                  - Health check")
	log.Info("  GET    /metrics                 - Metrics")
	log.Info("  GET    /status/job              - Get job status")
	log.Info("  GET    /status/datacenter       - Datacenter status")
	log.Info("  GET    /status/federation       - Federation status")
	log.Info("  GET    /status/cluster          - Cluster status")
	log.Info("  GET    /info/capacity           - Capacity info")
	log.Info("  GET    /info/clusters           - List clusters")
	log.Info("  POST   /job/cancel              - Cancel job")
	log.Info("  POST   /job/retry               - Retry job")
	log.Info("  POST   /clusters/register       - Register cluster")
	log.Info("  POST   /cluster-heartbeat       - Cluster heartbeat")
	log.Info("")
	log.Info("Test it:")
	log.Info("  curl -X POST http://localhost:%d/schedule \\", *gatewayPort)
	log.Info("    -H 'Content-Type: application/json' \\")
	log.Info("    -d '{")
	log.Info("      \"request_id\": \"test-1\",")
	log.Info("      \"name\": \"test-job\",")
	log.Info("      \"gpu_count\": 2,")
	log.Info("      \"gpu_type\": \"A100\",")
	log.Info("      \"memory_mb\": 8192,")
	log.Info("      \"priority\": 50")
	log.Info("    }'")
	log.Info("")

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan

	log.Info("")
	log.Info("Received signal: %v", sig)
	log.Info("Initiating graceful shutdown...")

	if err := apiGateway.Stop(ShutdownTimeout); err != nil {
		log.Error("Error during shutdown: %v", err)
		os.Exit(1)
	}

	log.Info("Graceful shutdown complete")
	log.Info("Goodbye!")
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

func initializeLogger(logLevel string) *logger.Logger {
	return logger.Get()
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

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}
