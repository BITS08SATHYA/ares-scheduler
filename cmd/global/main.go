// File: cmd/gateway/main.go
// Entry point for Ares Scheduler API Gateway
// Initializes all 10 layers and starts HTTP server
//
// Architecture:
//   main.go
//      ↓
//   (Initialize Logger)
//      ↓
//   (Initialize Storage: etcd + Redis)
//      ↓
//   (Initialize all 10 layers)
//      ↓
//   (Create API Gateway)
//      ↓
//   (Start HTTP server)
//      ↓
//   (Listen for HTTP requests on /schedule, /health, /metrics, etc.)

package main

import (
	//"context"
	"flag"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/cluster"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/redis"

	//"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/api/gateway"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
)

// ============================================================================
// CONFIGURATION CONSTANTS
// ============================================================================

const (
	// Server
	DefaultGatewayPort    = 8080
	DefaultRequestTimeout = 30 * time.Second
	DefaultMaxRequestSize = 1 << 20 // 1MB

	// Storage
	//DefaultETCDEndpoint = "http://etcd-0.etcd.ares-system.svc.cluster.local:2379"
	//DefaultRedisAddr    = "redis-0.redis.ares-system.svc.cluster.local:6379"
	DefaultETCDEndpoint = "localhost:2379"
	DefaultRedisAddr    = "localhost:6379"
	DefaultControlPlane = "localhost:8080"

	// Graceful shutdown
	ShutdownTimeout = 10 * time.Second
)

// ============================================================================
// COMMAND-LINE FLAGS
// ============================================================================

var (
	// Gateway flags
	gatewayPort = flag.Int(
		"gateway.port",
		DefaultGatewayPort,
		"HTTP port for API gateway (default: 8080)",
	)
	requestTimeout = flag.Duration(
		"gateway.timeout",
		DefaultRequestTimeout,
		"Request timeout (default: 30s)",
	)

	// Storage flags
	etcdEndpoint = flag.String(
		"etcd.endpoint",
		DefaultETCDEndpoint,
		"etcd endpoint (default: localhost:2379)",
	)

	redisAddr = flag.String(
		"redis.addr",
		DefaultRedisAddr,
		"Redis address (default: localhost:6379)",
	)
	controlPlane = flag.String(
		"control-plane.addr",
		DefaultControlPlane,
		"Control plane address (default: localhost)",
	)

	// Logging
	logLevel = flag.String(
		"log.level",
		"info",
		"Log level (debug, info, warn, error)",
	)

	// Feature flags
	enableCoordinator = flag.Bool(
		"enable-coordinator",
		true,
		"Enable job coordinator (exactly-once, leases, etc)",
	)
	enableMetrics = flag.Bool(
		"enable-metrics",
		true,
		"Enable /metrics endpoint",
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
			_ = log.Sync() // Sync is called but error is ignored (best effort)
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

	// ========================================================================
	// INITIALIZATION PHASE
	// ========================================================================

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

	log.Info("Gateway Config:")
	log.Info("  Port: %d", gatewayConfig.Port)
	log.Info("  Request Timeout: %v", gatewayConfig.RequestTimeout)
	log.Info("  Max Request Size: %d bytes", gatewayConfig.MaxRequestSize)
	log.Info("  CORS Enabled: %v", gatewayConfig.EnableCORS)
	log.Info("  Metrics Enabled: %v", gatewayConfig.EnablePrometheus)
	log.Info("")

	// Create API gateway (initializes all 10 layers)
	// Note: NewAPIGatewayWithCoordinator returns (*APIGatewayWithCoordinator, error)
	// which embeds *APIGateway, so it can be used as *APIGateway
	var apiGateway *gateway.APIGateway
	var err error

	if *enableCoordinator {
		log.Debug("Debug Mode Activated ....")
		log.Info("Initializing API Gateway WITH Job Coordinator (10-layer pipeline)...")
		log.Info("Storage backends:")
		log.Info("  etcd: %s", *etcdEndpoint)
		log.Info("  Redis: %s", *redisAddr)
		log.Info("")

		// Get the coordinator wrapper which embeds APIGateway
		redisClient, err := redis.NewRedisClient("localhost:6379", "", 0)

		clusterManager := cluster.NewClusterManager(redisClient, &cluster.ClusterConfig{
			AutoHeartbeatInterval:  10 * time.Second,
			HealthCheckInterval:    30 * time.Second,
			HeartbeatTimeout:       60 * time.Second,
			AutonomyEnabled:        true,
			MaxConsecutiveFailures: 3,
		})
		gatewayWithCoordinator, err := gateway.NewAPIGatewayWithCoordinator(
			*controlPlane,
			[]string{*etcdEndpoint},
			*redisAddr,
			gatewayConfig,
			clusterManager,
		)
		if err != nil {
			log.Error("Failed to initialize API Gateway with Coordinator: %v", err)
			os.Exit(1)
		}

		if gatewayWithCoordinator == nil {
			log.Error("API Gateway with Coordinator is nil after initialization")
			os.Exit(1)
		}

		// Extract the embedded APIGateway
		apiGateway = gatewayWithCoordinator.APIGateway
		if apiGateway == nil {
			log.Error("Embedded API Gateway is nil")
			os.Exit(1)
		}

		log.Info("")
		log.Info("API Gateway initialized with Job Coordinator")
		log.Info("All 10 layers ready")

	} else {
		log.Info("Initializing API Gateway WITHOUT Job Coordinator (testing mode)...")
		log.Info("Note: Coordinator features (exactly-once, leases) will be disabled")
		log.Info("")

		// For testing without coordinator, we'd create a basic global scheduler
		// This is less common in production, but useful for development
		log.Warn("Running in coordinator-disabled mode (not recommended for production)")

		apiGateway, err = gateway.NewAPIGateway(
			nil, // No global scheduler for now (can be added if needed)
			gatewayConfig,
		)
		if err != nil {
			log.Error("Failed to initialize API Gateway: %v", err)
			os.Exit(1)
		}

		log.Info("API Gateway initialized (limited features)")
	}

	log.Info("")

	// ========================================================================
	// SERVER STARTUP
	// ========================================================================

	log.Info("Starting HTTP server...")
	log.Info("")

	if err := apiGateway.Start(); err != nil {
		log.Error("Failed to start API Gateway: %v", err)
		os.Exit(1)
	}

	log.Info("")
	log.Info("╔════════════════════════════════════════════════════════╗")
	log.Info("║              Ares Gateway Ready                        ║")
	log.Info("╚════════════════════════════════════════════════════════╝")
	log.Info("")
	log.Info(" Server listening on port %d", *gatewayPort)
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
	log.Info("")
	log.Info("Test it:")
	log.Info("  curl -X POST http://localhost:%d/schedule \\", *gatewayPort)
	log.Info("    -H 'Content-Type: application/json' \\")
	log.Info("    -d '{\"request_id\": \"test-1\", \"name\": \"job\", \"gpu_count\": 2, \"gpu_type\": \"A100\", \"memory_mb\": 8192, \"priority\": 50}'")
	log.Info("")

	// ========================================================================
	// GRACEFUL SHUTDOWN
	// ========================================================================

	// Create signal channel for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	sig := <-sigChan
	log.Info("")
	log.Info("Received signal: %v", sig)
	log.Info("Initiating graceful shutdown...")
	log.Info("")

	// Attempt graceful shutdown
	// Note: apiGateway.Stop() takes a duration directly, not a context
	// The context would be used if we were doing multiple operations
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

// ============================================================================
// ENVIRONMENT VARIABLE SUPPORT (Optional Enhancement)
// ============================================================================

// You can enhance this by reading from environment variables:
//
// Example usage:
//   export ARES_GATEWAY_PORT=9090
//   export ARES_ETCD_ENDPOINT=etcd.example.com:2379
//   export ARES_REDIS_ADDR=redis.example.com:6379
//   go run ./cmd/gateway/main.go
//
// Uncomment and use the function below if you want env var support:

/*
func initializeFromEnv() {
	if port := os.Getenv("ARES_GATEWAY_PORT"); port != "" {
		gatewayPort = flag.Int("gateway.port", parseInt(port), "")
	}

	if etcd := os.Getenv("ARES_ETCD_ENDPOINT"); etcd != "" {
		*etcdEndpoint = etcd
	}

	if redis := os.Getenv("ARES_REDIS_ADDR"); redis != "" {
		*redisAddr = redis
	}

	if cp := os.Getenv("ARES_CONTROL_PLANE"); cp != "" {
		*controlPlane = cp
	}
}

func parseInt(s string) int {
	val, err := strconv.Atoi(s)
	if err != nil {
		return DefaultGatewayPort
	}
	return val
}
*/
