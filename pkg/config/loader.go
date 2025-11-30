// Layer 1: Configuration loading (depends only on types.go)
package config

import (
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"os"
	"strconv"
	"time"
)

// LoadConfig loads configuration from environment variables and defaults
func LoadConfig() *common.Config {
	cfg := &common.Config{
		// etcd configuration
		EtcdEndpoints:   getStringSlice("ARES_ETCD_ENDPOINTS", []string{"localhost:2379"}),
		EtcdDialTimeout: getDuration("ARES_ETCD_TIMEOUT", 10*time.Second),

		// Redis configuration
		RedisAddr:     getString("ARES_REDIS_ADDR", "localhost:6379"),
		RedisPassword: getString("ARES_REDIS_PASSWORD", ""),
		RedisDB:       getInt("ARES_REDIS_DB", 0),

		// Kubernetes configuration
		KubeConfigPath: getString("ARES_KUBECONFIG", "/root/.kube/config"),
		Namespace:      getString("ARES_K8S_NAMESPACE", "default"),

		// Scheduler ports
		GlobalSchedulerPort: getInt("ARES_GLOBAL_SCHEDULER_PORT", 8080),
		LocalSchedulerPort:  getInt("ARES_LOCAL_SCHEDULER_PORT", 8081),

		// Logging
		LogLevel: getString("ARES_LOG_LEVEL", "info"),

		// Features
		EnableMetrics: getBool("ARES_ENABLE_METRICS", true),
		EnableTracing: getBool("ARES_ENABLE_TRACING", false),

		// Timeouts
		JobTimeout:           getDuration("ARES_JOB_TIMEOUT", 30*time.Minute),
		LeaseRenewalInterval: getDuration("ARES_LEASE_RENEWAL_INTERVAL", 10*time.Second),
		HealthCheckInterval:  getDuration("ARES_HEALTH_CHECK_INTERVAL", 30*time.Second),
	}

	return cfg
}

// Helper functions to read environment variables with type conversion

func getString(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func getInt(key string, defaultValue int) int {
	if value, exists := os.LookupEnv(key); exists {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}

func getBool(key string, defaultValue bool) bool {
	if value, exists := os.LookupEnv(key); exists {
		return value == "true" || value == "1" || value == "yes"
	}
	return defaultValue
}

func getDuration(key string, defaultValue time.Duration) time.Duration {
	if value, exists := os.LookupEnv(key); exists {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}

func getStringSlice(key string, defaultValue []string) []string {
	if value, exists := os.LookupEnv(key); exists && value != "" {
		// Simple comma-separated parsing
		//endpoints := make([]string, 0)
		// In real code, use strings.Split(value, ",")
		return []string{value}
	}
	return defaultValue
}
