// Layer 1: Configuration loading (depends only on types.go)
package config

import (
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"os"
	"strconv"
	"strings"
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

// getStringSlice: Read comma-separated strings from environment variable
// Example: "localhost:2379,etcd-2:2379,etcd-3:2379" → []string{"localhost:2379", "etcd-2:2379", "etcd-3:2379"}
func getStringSlice(key string, defaultValue []string) []string {
	if value, exists := os.LookupEnv(key); exists && value != "" {
		// Split by comma and trim whitespace from each part
		parts := strings.Split(value, ",")
		result := make([]string, 0, len(parts))
		for _, part := range parts {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				result = append(result, trimmed)
			}
		}
		if len(result) > 0 {
			return result
		}
	}
	return defaultValue
}

// ============================================================================
// ADDITIONAL UTILITY FUNCTIONS
// ============================================================================

// ValidateConfig: Validate configuration values
// Returns error if any required config is invalid
func ValidateConfig(cfg *common.Config) error {
	// Check etcd endpoints
	if len(cfg.EtcdEndpoints) == 0 {
		return &configError{field: "EtcdEndpoints", reason: "cannot be empty"}
	}

	// Check redis address
	if cfg.RedisAddr == "" {
		return &configError{field: "RedisAddr", reason: "cannot be empty"}
	}

	// Check ports
	if cfg.GlobalSchedulerPort < 1 || cfg.GlobalSchedulerPort > 65535 {
		return &configError{field: "GlobalSchedulerPort", reason: "must be between 1 and 65535"}
	}

	if cfg.LocalSchedulerPort < 1 || cfg.LocalSchedulerPort > 65535 {
		return &configError{field: "LocalSchedulerPort", reason: "must be between 1 and 65535"}
	}

	// Check timeouts
	if cfg.JobTimeout <= 0 {
		return &configError{field: "JobTimeout", reason: "must be positive"}
	}

	if cfg.LeaseRenewalInterval <= 0 {
		return &configError{field: "LeaseRenewalInterval", reason: "must be positive"}
	}

	return nil
}

// configError: Custom error type for config validation
type configError struct {
	field  string
	reason string
}

func (e *configError) Error() string {
	return "Config validation error: " + e.field + " " + e.reason
}

// ============================================================================
// CONFIGURATION PRINTING (FOR DEBUG)
// ============================================================================

// PrintConfig: Print config values for debugging
func PrintConfig(cfg *common.Config) {
	println("=== Ares Configuration ===")
	println("etcd Endpoints:", cfg.EtcdEndpoints)
	println("etcd Timeout:", cfg.EtcdDialTimeout.String())
	println("Redis Address:", cfg.RedisAddr)
	println("Redis DB:", cfg.RedisDB)
	println("Kube Config Path:", cfg.KubeConfigPath)
	println("Namespace:", cfg.Namespace)
	println("Global Scheduler Port:", cfg.GlobalSchedulerPort)
	println("Local Scheduler Port:", cfg.LocalSchedulerPort)
	println("Log Level:", cfg.LogLevel)
	println("Enable Metrics:", cfg.EnableMetrics)
	println("Enable Tracing:", cfg.EnableTracing)
	println("Job Timeout:", cfg.JobTimeout.String())
	println("Lease Renewal Interval:", cfg.LeaseRenewalInterval.String())
	println("Health Check Interval:", cfg.HealthCheckInterval.String())
	println("===========================")
}

// DRFConfig configuration for fair resource allocation
type DRFConfig struct {
	// Resource weights
	CPUWeight    float64
	GPUWeight    float64
	MemoryWeight float64

	// Fairness thresholds
	FairnessThreshold float64
	UpdateInterval    time.Duration

	// Preemption settings
	PreemptionEnabled      bool
	MaxPreemptionsPerHour  int
	GracePeriodSec         int
	PreemptiblePriorityMax int
}

// PreemptionConfig configuration for job preemption
type PreemptionConfig struct {
	Enabled               bool
	GracePeriodSec        int
	CheckGracePeriodEvery int
	MaxPreemptionsPerHour int
	MaxPreemptionsPerDay  int
	HighPriorityWaitSec   int
	FairnessViolation     bool
	PriorityPreemption    bool
	SLAPreemption         bool
	PreferCheckpointable  bool
	AvoidDuplicates       bool
	MinimumAgeSeconds     int
}

// LoadDRFConfig loads DRF configuration from environment variables
func LoadDRFConfig() *DRFConfig {
	return &DRFConfig{
		CPUWeight:              getEnvFloat("DRF_CPU_WEIGHT", 1.0),
		GPUWeight:              getEnvFloat("DRF_GPU_WEIGHT", 3.0),
		MemoryWeight:           getEnvFloat("DRF_MEMORY_WEIGHT", 0.5),
		FairnessThreshold:      getEnvFloat("DRF_FAIRNESS_THRESHOLD", 0.2),
		UpdateInterval:         time.Duration(getEnvInt("DRF_UPDATE_INTERVAL_SEC", 30)) * time.Second,
		PreemptionEnabled:      getEnvBool("PREEMPTION_ENABLED", true),
		MaxPreemptionsPerHour:  getEnvInt("MAX_PREEMPTIONS_PER_HOUR", 10),
		GracePeriodSec:         getEnvInt("GRACE_PERIOD_SEC", 30),
		PreemptiblePriorityMax: getEnvInt("PREEMPTIBLE_PRIORITY_MAX", 50),
	}
}

// LoadPreemptionConfig loads preemption configuration from environment variables
func LoadPreemptionConfig() *PreemptionConfig {
	return &PreemptionConfig{
		Enabled:               getEnvBool("PREEMPTION_ENABLED", true),
		GracePeriodSec:        getEnvInt("GRACE_PERIOD_SEC", 30),
		CheckGracePeriodEvery: getEnvInt("CHECK_GRACE_PERIOD_MS", 100),
		MaxPreemptionsPerHour: getEnvInt("MAX_PREEMPTIONS_PER_HOUR", 10),
		MaxPreemptionsPerDay:  getEnvInt("MAX_PREEMPTIONS_PER_DAY", 100),
		HighPriorityWaitSec:   getEnvInt("HIGH_PRIORITY_WAIT_SEC", 300),
		FairnessViolation:     getEnvBool("PREEMPT_ON_FAIRNESS", true),
		PriorityPreemption:    getEnvBool("PREEMPT_ON_PRIORITY", true),
		SLAPreemption:         getEnvBool("PREEMPT_ON_SLA", true),
		PreferCheckpointable:  getEnvBool("PREFER_CHECKPOINT_JOBS", true),
		AvoidDuplicates:       getEnvBool("AVOID_DUPLICATE_PREEMPTION", true),
		MinimumAgeSeconds:     getEnvInt("MINIMUM_JOB_AGE_SEC", 10),
	}
}

// Helper functions (add to existing config.go if not present)
func getEnvBool(key string, defaultVal bool) bool {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	b, _ := strconv.ParseBool(val)
	return b
}

func getEnvInt(key string, defaultVal int) int {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	i, _ := strconv.Atoi(val)
	return i
}

func getEnvFloat(key string, defaultVal float64) float64 {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	f, _ := strconv.ParseFloat(val, 64)
	return f
}
