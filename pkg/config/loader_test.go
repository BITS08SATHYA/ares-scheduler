package config

import (
	"os"
	"testing"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// SECTION 1: LoadConfig Defaults
// ============================================================================

func TestLoadConfig_Defaults(t *testing.T) {
	// Unset all ARES env vars to ensure defaults
	envVars := []string{
		"ARES_ETCD_ENDPOINTS", "ARES_ETCD_TIMEOUT", "ARES_REDIS_ADDR",
		"ARES_REDIS_PASSWORD", "ARES_REDIS_DB", "ARES_KUBECONFIG",
		"ARES_K8S_NAMESPACE", "ARES_GLOBAL_SCHEDULER_PORT", "ARES_LOCAL_SCHEDULER_PORT",
		"ARES_LOG_LEVEL", "ARES_ENABLE_METRICS", "ARES_ENABLE_TRACING",
		"ARES_JOB_TIMEOUT", "ARES_LEASE_RENEWAL_INTERVAL", "ARES_HEALTH_CHECK_INTERVAL",
	}
	for _, v := range envVars {
		os.Unsetenv(v)
	}

	cfg := LoadConfig()

	assert.Equal(t, []string{"localhost:2379"}, cfg.EtcdEndpoints)
	assert.Equal(t, 10*time.Second, cfg.EtcdDialTimeout)
	assert.Equal(t, "localhost:6379", cfg.RedisAddr)
	assert.Equal(t, "", cfg.RedisPassword)
	assert.Equal(t, 0, cfg.RedisDB)
	assert.Equal(t, "/root/.kube/config", cfg.KubeConfigPath)
	assert.Equal(t, "default", cfg.Namespace)
	assert.Equal(t, 8080, cfg.GlobalSchedulerPort)
	assert.Equal(t, 8081, cfg.LocalSchedulerPort)
	assert.Equal(t, "info", cfg.LogLevel)
	assert.True(t, cfg.EnableMetrics)
	assert.False(t, cfg.EnableTracing)
	assert.Equal(t, 30*time.Minute, cfg.JobTimeout)
	assert.Equal(t, 10*time.Second, cfg.LeaseRenewalInterval)
	assert.Equal(t, 30*time.Second, cfg.HealthCheckInterval)
}

// ============================================================================
// SECTION 2: LoadConfig with env overrides
// ============================================================================

func TestLoadConfig_EnvOverrides(t *testing.T) {
	t.Setenv("ARES_REDIS_ADDR", "redis.prod:6380")
	t.Setenv("ARES_REDIS_DB", "3")
	t.Setenv("ARES_K8S_NAMESPACE", "ares-system")
	t.Setenv("ARES_GLOBAL_SCHEDULER_PORT", "9090")
	t.Setenv("ARES_LOG_LEVEL", "debug")
	t.Setenv("ARES_ENABLE_TRACING", "true")
	t.Setenv("ARES_JOB_TIMEOUT", "1h")

	cfg := LoadConfig()

	assert.Equal(t, "redis.prod:6380", cfg.RedisAddr)
	assert.Equal(t, 3, cfg.RedisDB)
	assert.Equal(t, "ares-system", cfg.Namespace)
	assert.Equal(t, 9090, cfg.GlobalSchedulerPort)
	assert.Equal(t, "debug", cfg.LogLevel)
	assert.True(t, cfg.EnableTracing)
	assert.Equal(t, time.Hour, cfg.JobTimeout)
}

func TestLoadConfig_EtcdEndpoints(t *testing.T) {
	t.Setenv("ARES_ETCD_ENDPOINTS", "etcd-1:2379,etcd-2:2379,etcd-3:2379")

	cfg := LoadConfig()
	assert.Equal(t, []string{"etcd-1:2379", "etcd-2:2379", "etcd-3:2379"}, cfg.EtcdEndpoints)
}

// ============================================================================
// SECTION 3: ValidateConfig
// ============================================================================

func TestValidateConfig_Valid(t *testing.T) {
	cfg := LoadConfig()
	err := ValidateConfig(cfg)
	assert.NoError(t, err)
}

func TestValidateConfig_EmptyEtcdEndpoints(t *testing.T) {
	cfg := &common.Config{
		EtcdEndpoints:       []string{},
		RedisAddr:           "localhost:6379",
		GlobalSchedulerPort: 8080,
		LocalSchedulerPort:  8081,
		JobTimeout:          time.Minute,
		LeaseRenewalInterval: time.Second,
	}
	err := ValidateConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "EtcdEndpoints")
}

func TestValidateConfig_EmptyRedisAddr(t *testing.T) {
	cfg := &common.Config{
		EtcdEndpoints:       []string{"localhost:2379"},
		RedisAddr:           "",
		GlobalSchedulerPort: 8080,
		LocalSchedulerPort:  8081,
		JobTimeout:          time.Minute,
		LeaseRenewalInterval: time.Second,
	}
	err := ValidateConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RedisAddr")
}

func TestValidateConfig_InvalidPort_Zero(t *testing.T) {
	cfg := &common.Config{
		EtcdEndpoints:       []string{"localhost:2379"},
		RedisAddr:           "localhost:6379",
		GlobalSchedulerPort: 0,
		LocalSchedulerPort:  8081,
		JobTimeout:          time.Minute,
		LeaseRenewalInterval: time.Second,
	}
	err := ValidateConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "GlobalSchedulerPort")
}

func TestValidateConfig_InvalidPort_TooHigh(t *testing.T) {
	cfg := &common.Config{
		EtcdEndpoints:       []string{"localhost:2379"},
		RedisAddr:           "localhost:6379",
		GlobalSchedulerPort: 8080,
		LocalSchedulerPort:  99999,
		JobTimeout:          time.Minute,
		LeaseRenewalInterval: time.Second,
	}
	err := ValidateConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "LocalSchedulerPort")
}

func TestValidateConfig_NegativeJobTimeout(t *testing.T) {
	cfg := &common.Config{
		EtcdEndpoints:       []string{"localhost:2379"},
		RedisAddr:           "localhost:6379",
		GlobalSchedulerPort: 8080,
		LocalSchedulerPort:  8081,
		JobTimeout:          -1 * time.Second,
		LeaseRenewalInterval: time.Second,
	}
	err := ValidateConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "JobTimeout")
}

func TestValidateConfig_NegativeLeaseRenewal(t *testing.T) {
	cfg := &common.Config{
		EtcdEndpoints:       []string{"localhost:2379"},
		RedisAddr:           "localhost:6379",
		GlobalSchedulerPort: 8080,
		LocalSchedulerPort:  8081,
		JobTimeout:          time.Minute,
		LeaseRenewalInterval: -1 * time.Second,
	}
	err := ValidateConfig(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "LeaseRenewalInterval")
}

// ============================================================================
// SECTION 4: Helper Functions
// ============================================================================

func TestGetString(t *testing.T) {
	t.Setenv("TEST_KEY", "hello")
	assert.Equal(t, "hello", getString("TEST_KEY", "default"))
}

func TestGetString_Default(t *testing.T) {
	os.Unsetenv("TEST_KEY_MISSING")
	assert.Equal(t, "default", getString("TEST_KEY_MISSING", "default"))
}

func TestGetInt(t *testing.T) {
	t.Setenv("TEST_INT", "42")
	assert.Equal(t, 42, getInt("TEST_INT", 0))
}

func TestGetInt_InvalidFallsToDefault(t *testing.T) {
	t.Setenv("TEST_INT_BAD", "notanumber")
	assert.Equal(t, 10, getInt("TEST_INT_BAD", 10))
}

func TestGetInt_Missing(t *testing.T) {
	os.Unsetenv("TEST_INT_MISSING")
	assert.Equal(t, 99, getInt("TEST_INT_MISSING", 99))
}

func TestGetBool(t *testing.T) {
	tests := []struct {
		value    string
		expected bool
	}{
		{"true", true},
		{"1", true},
		{"yes", true},
		{"false", false},
		{"0", false},
		{"no", false},
		{"anything", false},
	}
	for _, tt := range tests {
		t.Setenv("TEST_BOOL", tt.value)
		assert.Equal(t, tt.expected, getBool("TEST_BOOL", false), "getBool(%q)", tt.value)
	}
}

func TestGetBool_Default(t *testing.T) {
	os.Unsetenv("TEST_BOOL_MISS")
	assert.True(t, getBool("TEST_BOOL_MISS", true))
	assert.False(t, getBool("TEST_BOOL_MISS", false))
}

func TestGetDuration(t *testing.T) {
	t.Setenv("TEST_DUR", "5m")
	assert.Equal(t, 5*time.Minute, getDuration("TEST_DUR", time.Second))
}

func TestGetDuration_Invalid(t *testing.T) {
	t.Setenv("TEST_DUR_BAD", "notaduration")
	assert.Equal(t, 10*time.Second, getDuration("TEST_DUR_BAD", 10*time.Second))
}

func TestGetStringSlice(t *testing.T) {
	t.Setenv("TEST_SLICE", "a,b,c")
	assert.Equal(t, []string{"a", "b", "c"}, getStringSlice("TEST_SLICE", nil))
}

func TestGetStringSlice_WithWhitespace(t *testing.T) {
	t.Setenv("TEST_SLICE_WS", " a , b , c ")
	assert.Equal(t, []string{"a", "b", "c"}, getStringSlice("TEST_SLICE_WS", nil))
}

func TestGetStringSlice_EmptyValue(t *testing.T) {
	t.Setenv("TEST_SLICE_EMPTY", "")
	assert.Equal(t, []string{"default"}, getStringSlice("TEST_SLICE_EMPTY", []string{"default"}))
}

func TestGetStringSlice_Missing(t *testing.T) {
	os.Unsetenv("TEST_SLICE_MISS")
	assert.Equal(t, []string{"x"}, getStringSlice("TEST_SLICE_MISS", []string{"x"}))
}

func TestGetStringSlice_CommaSeparatedWithEmpty(t *testing.T) {
	t.Setenv("TEST_SLICE_GAP", "a,,b,  ,c")
	result := getStringSlice("TEST_SLICE_GAP", nil)
	assert.Equal(t, []string{"a", "b", "c"}, result)
}

// ============================================================================
// SECTION 5: DRF & Preemption Config
// ============================================================================

func TestLoadDRFConfig_Defaults(t *testing.T) {
	// Unset DRF env vars
	for _, v := range []string{"DRF_CPU_WEIGHT", "DRF_GPU_WEIGHT", "DRF_MEMORY_WEIGHT", "DRF_FAIRNESS_THRESHOLD"} {
		os.Unsetenv(v)
	}

	cfg := LoadDRFConfig()
	assert.Equal(t, 1.0, cfg.CPUWeight)
	assert.Equal(t, 3.0, cfg.GPUWeight)
	assert.Equal(t, 0.5, cfg.MemoryWeight)
	assert.Equal(t, 0.2, cfg.FairnessThreshold)
}

func TestLoadPreemptionConfig_Defaults(t *testing.T) {
	for _, v := range []string{"PREEMPTION_ENABLED", "GRACE_PERIOD_SEC", "MAX_PREEMPTIONS_PER_HOUR"} {
		os.Unsetenv(v)
	}

	cfg := LoadPreemptionConfig()
	assert.True(t, cfg.Enabled)
	assert.Equal(t, 30, cfg.GracePeriodSec)
	assert.Equal(t, 10, cfg.MaxPreemptionsPerHour)
}

// ============================================================================
// SECTION 6: getEnv helpers
// ============================================================================

func TestGetEnvBool(t *testing.T) {
	t.Setenv("TEST_ENVBOOL", "true")
	assert.True(t, getEnvBool("TEST_ENVBOOL", false))

	os.Unsetenv("TEST_ENVBOOL_MISS")
	assert.True(t, getEnvBool("TEST_ENVBOOL_MISS", true))
}

func TestGetEnvInt(t *testing.T) {
	t.Setenv("TEST_ENVINT", "42")
	assert.Equal(t, 42, getEnvInt("TEST_ENVINT", 0))

	os.Unsetenv("TEST_ENVINT_MISS")
	assert.Equal(t, 99, getEnvInt("TEST_ENVINT_MISS", 99))
}

func TestGetEnvFloat(t *testing.T) {
	t.Setenv("TEST_ENVFLOAT", "3.14")
	assert.InDelta(t, 3.14, getEnvFloat("TEST_ENVFLOAT", 0), 0.001)

	os.Unsetenv("TEST_ENVFLOAT_MISS")
	assert.Equal(t, 1.5, getEnvFloat("TEST_ENVFLOAT_MISS", 1.5))
}
