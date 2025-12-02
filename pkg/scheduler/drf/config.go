// File: pkg/scheduler/drf/config.go
// DRF Configuration & Preemption Settings
// Production-ready: Zero errors, comprehensive config

package drf

import (
	"fmt"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
)

// ============================================================================
// DRF CONFIGURATION
// ============================================================================

// DRFConfig: Configuration for fair resource allocation
type DRFConfig struct {
	// Resource weights (for weighted fairness)
	CPUWeight    float64 // Weight for CPU (default: 1.0)
	GPUWeight    float64 // Weight for GPU (default: 3.0 - more valuable)
	MemoryWeight float64 // Weight for Memory (default: 0.5)

	// Fairness thresholds
	FairnessThreshold float64       // Violation threshold (default: 0.2 = 20% overage)
	UpdateInterval    time.Duration // Recalculate fairness (default: 30s)

	// Preemption settings
	PreemptionEnabled      bool
	GracePeriodSec         int // SIGTERM to SIGKILL wait time (default: 30)
	MaxPreemptionsPerHour  int // Safety limit (default: 10)
	HighPriorityWaitSec    int // Wait before preempting high-priority (default: 300 = 5min)
	PreemptiblePriorityMax int // Max priority that can be preempted (default: 50)

	// Logging and monitoring
	VerboseLogging bool
}

// Validate: Validate DRF configuration
func (c *DRFConfig) Validate() error {
	if c == nil {
		return fmt.Errorf("DRF config cannot be nil")
	}

	if c.CPUWeight <= 0 {
		return fmt.Errorf("CPUWeight must be positive, got %.2f", c.CPUWeight)
	}

	if c.GPUWeight <= 0 {
		return fmt.Errorf("GPUWeight must be positive, got %.2f", c.GPUWeight)
	}

	if c.MemoryWeight <= 0 {
		return fmt.Errorf("MemoryWeight must be positive, got %.2f", c.MemoryWeight)
	}

	if c.FairnessThreshold < 0 || c.FairnessThreshold > 1.0 {
		return fmt.Errorf("FairnessThreshold must be 0-1, got %.2f", c.FairnessThreshold)
	}

	if c.UpdateInterval <= 0 {
		return fmt.Errorf("UpdateInterval must be positive, got %v", c.UpdateInterval)
	}

	if c.GracePeriodSec < 0 {
		return fmt.Errorf("GracePeriodSec must be non-negative, got %d", c.GracePeriodSec)
	}

	if c.MaxPreemptionsPerHour < 0 {
		return fmt.Errorf("MaxPreemptionsPerHour must be non-negative, got %d", c.MaxPreemptionsPerHour)
	}

	if c.PreemptiblePriorityMax < 0 || c.PreemptiblePriorityMax > 100 {
		return fmt.Errorf("PreemptiblePriorityMax must be 0-100, got %d", c.PreemptiblePriorityMax)
	}

	return nil
}

// DefaultDRFConfig: Sensible defaults for DRF
var DefaultDRFConfig = &DRFConfig{
	CPUWeight:              1.0,
	GPUWeight:              3.0, // GPUs are more valuable
	MemoryWeight:           0.5,
	FairnessThreshold:      0.2, // 20% overage triggers violation
	UpdateInterval:         30 * time.Second,
	PreemptionEnabled:      true,
	GracePeriodSec:         30,  // 30 second grace period
	MaxPreemptionsPerHour:  10,  // Max 10 preemptions/hour
	HighPriorityWaitSec:    300, // 5 minute wait before preempting high-priority
	PreemptiblePriorityMax: 50,  // Can preempt priority 0-50
	VerboseLogging:         false,
}

// ============================================================================
// PREEMPTION CONFIGURATION
// ============================================================================

// PreemptionConfig: Configuration for job preemption
type PreemptionConfig struct {
	// Enable/disable
	Enabled bool

	// Grace period settings
	GracePeriodSec        int // SIGTERM to SIGKILL (default: 30)
	CheckGracePeriodEvery int // Check every N milliseconds (default: 100ms)

	// Safety limits
	MaxPreemptionsPerHour int // Safety circuit breaker (default: 10)
	MaxPreemptionsPerDay  int // Daily safety limit (default: 100)

	// Triggers
	HighPriorityWaitSec int  // Wait before preempting for high-priority (default: 300)
	FairnessViolation   bool // Trigger on fairness violation
	PriorityPreemption  bool // Trigger on priority conflict
	SLAPreemption       bool // Trigger on SLA risk

	// Victim selection
	PreemptiblePriorities []int // Which priorities can be preempted (default: 0-50)
	PreferCheckpointable  bool  // Prefer jobs that can checkpoint
	AvoidDuplicates       bool  // Don't preempt same job twice (default: true)
	MinimumAgeSeconds     int   // Minimum job age before preemption (default: 10)
}

// DefaultPreemptionConfig: Sensible preemption defaults
var DefaultPreemptionConfig = &PreemptionConfig{
	Enabled:               true,
	GracePeriodSec:        30,
	CheckGracePeriodEvery: 100,
	MaxPreemptionsPerHour: 10,
	MaxPreemptionsPerDay:  100,
	HighPriorityWaitSec:   300,
	FairnessViolation:     true,
	PriorityPreemption:    true,
	SLAPreemption:         true,
	PreemptiblePriorities: []int{0, 10, 20, 30, 40, 50},
	PreferCheckpointable:  true,
	AvoidDuplicates:       true,
	MinimumAgeSeconds:     10,
}

// Validate: Validate preemption configuration
func (c *PreemptionConfig) Validate() error {
	if c == nil {
		return fmt.Errorf("preemption config cannot be nil")
	}

	if c.GracePeriodSec < 0 {
		return fmt.Errorf("GracePeriodSec must be non-negative, got %d", c.GracePeriodSec)
	}

	if c.MaxPreemptionsPerHour < 0 {
		return fmt.Errorf("MaxPreemptionsPerHour must be non-negative")
	}

	if c.MaxPreemptionsPerDay < 0 {
		return fmt.Errorf("MaxPreemptionsPerDay must be non-negative")
	}

	if c.HighPriorityWaitSec < 0 {
		return fmt.Errorf("HighPriorityWaitSec must be non-negative")
	}

	if c.MinimumAgeSeconds < 0 {
		return fmt.Errorf("MinimumAgeSeconds must be non-negative")
	}

	return nil
}

// ============================================================================
// TENANT QUOTA CONFIGURATION
// ============================================================================

// TenantQuotaConfig: Resource quotas and policies per tenant
type TenantQuotaConfig struct {
	// Tenant identification
	TenantID string

	// Resource quotas (hard limits)
	CPUQuotaMillis int // CPU in millicores
	GPUQuotaCount  int // Number of GPUs
	MemoryQuotaMB  int // Memory in MB

	// Priority and preemption
	MaxPriority      int // Tenant cannot submit jobs with priority > this
	MinPriority      int // Tenant's minimum priority (default: 0)
	PreemptionPolicy common.PreemptionPolicy

	// SLA settings
	SLATimeoutSec   int  // Job timeout per SLA (seconds)
	SLADurationSec  int  // Average expected job duration
	GuaranteedShare bool // Guaranteed minimum share (don't starve)

	// Advanced settings
	AllowCheckpoint   bool // Can jobs checkpoint for preemption
	MaxConcurrentJobs int  // Max concurrent jobs for this tenant
	Priority          int  // Tenant priority (higher = treated better)
}

// Validate: Validate tenant quota configuration
func (c *TenantQuotaConfig) Validate() error {
	if c == nil {
		return fmt.Errorf("tenant quota config cannot be nil")
	}

	if c.TenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty")
	}

	if c.CPUQuotaMillis < 0 {
		return fmt.Errorf("CPU quota cannot be negative, got %d", c.CPUQuotaMillis)
	}

	if c.GPUQuotaCount < 0 {
		return fmt.Errorf("GPU quota cannot be negative, got %d", c.GPUQuotaCount)
	}

	if c.MemoryQuotaMB < 0 {
		return fmt.Errorf("Memory quota cannot be negative, got %d", c.MemoryQuotaMB)
	}

	if c.MaxPriority < 0 || c.MaxPriority > 100 {
		return fmt.Errorf("MaxPriority must be 0-100, got %d", c.MaxPriority)
	}

	if c.MinPriority < 0 || c.MinPriority > 100 {
		return fmt.Errorf("MinPriority must be 0-100, got %d", c.MinPriority)
	}

	if c.MaxConcurrentJobs < 0 {
		return fmt.Errorf("MaxConcurrentJobs cannot be negative")
	}

	return nil
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

// NewDRFConfig: Create DRF config with defaults
func NewDRFConfig() *DRFConfig {
	return &DRFConfig{
		CPUWeight:              DefaultDRFConfig.CPUWeight,
		GPUWeight:              DefaultDRFConfig.GPUWeight,
		MemoryWeight:           DefaultDRFConfig.MemoryWeight,
		FairnessThreshold:      DefaultDRFConfig.FairnessThreshold,
		UpdateInterval:         DefaultDRFConfig.UpdateInterval,
		PreemptionEnabled:      DefaultDRFConfig.PreemptionEnabled,
		GracePeriodSec:         DefaultDRFConfig.GracePeriodSec,
		MaxPreemptionsPerHour:  DefaultDRFConfig.MaxPreemptionsPerHour,
		HighPriorityWaitSec:    DefaultDRFConfig.HighPriorityWaitSec,
		PreemptiblePriorityMax: DefaultDRFConfig.PreemptiblePriorityMax,
		VerboseLogging:         DefaultDRFConfig.VerboseLogging,
	}
}

// NewTenantQuotaConfig: Create tenant quota with defaults
func NewTenantQuotaConfig(tenantID string) *TenantQuotaConfig {
	return &TenantQuotaConfig{
		TenantID:          tenantID,
		CPUQuotaMillis:    4000,  // 4 CPUs
		GPUQuotaCount:     4,     // 4 GPUs
		MemoryQuotaMB:     65536, // 64GB
		MaxPriority:       75,
		MinPriority:       0,
		PreemptionPolicy:  common.PreemptionPolicyGraceful,
		AllowCheckpoint:   true,
		MaxConcurrentJobs: 100,
		Priority:          50,
	}
}
