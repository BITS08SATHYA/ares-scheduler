// File: pkg/scheduler/drf/drf.go
// SchedulingFeature: Fair Resource Allocation using Dominant Resource Fairness (DRF)
//
// DRF Algorithm (Ghodsi et al., NSDI 2011):
// Each tenant's "dominant share" = max(cpu_share, gpu_share, mem_share)
// The tenant with the SMALLEST dominant share gets priority.
// This prevents any single tenant from hogging all of one resource.
//
// Example:
//   Cluster has 100 GPUs, 1000 CPUs, 1000 GB memory
//   Tenant A uses 50 GPUs, 100 CPUs, 200 GB → shares: 50%, 10%, 20% → dominant = 50% (GPU)
//   Tenant B uses 10 GPUs, 500 CPUs, 100 GB → shares: 10%, 50%, 10% → dominant = 50% (CPU)
//   Both have 50% dominant share → fair. If A requests more GPUs, denied until B catches up.

package drf

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
)

// ============================================================================
// DRF MANAGER
// ============================================================================

// DRFManager: Tracks per-tenant resource usage and enforces fairness
type DRFManager struct {
	mu     sync.RWMutex
	log    *logger.Logger
	config *DRFConfig

	// Per-tenant usage tracking
	tenantUsage map[string]*TenantUsage // tenantID -> usage

	// Total cluster capacity (updated by heartbeats)
	totalGPUs  int
	totalCPUs  int
	totalMemGB float64
}

// DRFConfig: Configuration for DRF
type DRFConfig struct {
	GPUWeight          float64       // Weight for GPU in dominant resource calc (default: 3.0)
	CPUWeight          float64       // Weight for CPU (default: 1.0)
	MemoryWeight       float64       // Weight for memory (default: 0.5)
	FairnessThreshold  float64       // Max allowed dominant share before throttling (default: 0.8 = 80%)
	UpdateInterval     time.Duration // How often to recalculate shares
	DefaultTenantQuota float64       // Default max share per tenant if not specified (default: 0.5 = 50%)
}

// DefaultDRFConfig: Sensible defaults
var DefaultDRFConfig = &DRFConfig{
	GPUWeight:          3.0,
	CPUWeight:          1.0,
	MemoryWeight:       0.5,
	FairnessThreshold:  0.8,
	UpdateInterval:     30 * time.Second,
	DefaultTenantQuota: 0.5,
}

// TenantUsage: Current resource usage for a tenant
type TenantUsage struct {
	TenantID     string
	GPUsInUse    int
	CPUsInUse    int
	MemGBInUse   float64
	RunningJobs  int
	TotalJobsRun int

	// Computed shares (0.0 - 1.0)
	GPUShare      float64
	CPUShare      float64
	MemoryShare   float64
	DominantShare float64 // max(gpu_share, cpu_share, mem_share)
	DominantType  string  // "GPU", "CPU", or "Memory"

	// Quota (0.0 - 1.0, max share this tenant can use)
	MaxShare float64

	LastUpdated time.Time
}

// DRFDecision: Result of a DRF check
type DRFDecision struct {
	Allowed       bool // Can this tenant schedule?
	TenantID      string
	DominantShare float64 // Current dominant share
	DominantType  string  // Which resource is dominant
	Reason        string  // Why allowed/denied
	GPUShare      float64
	CPUShare      float64
	MemoryShare   float64
}

// ============================================================================
// CONSTRUCTOR
// ============================================================================

func NewDRFManager(config *DRFConfig) *DRFManager {
	if config == nil {
		config = DefaultDRFConfig
	}

	return &DRFManager{
		log:         logger.Get(),
		config:      config,
		tenantUsage: make(map[string]*TenantUsage),
	}
}

// ============================================================================
// CAPACITY UPDATES
// ============================================================================

// UpdateCapacity: Called when cluster capacity changes (from heartbeats)
func (dm *DRFManager) UpdateCapacity(totalGPUs, totalCPUs int, totalMemGB float64) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	dm.totalGPUs = totalGPUs
	dm.totalCPUs = totalCPUs
	dm.totalMemGB = totalMemGB

	dm.log.Debug("DRF capacity updated: GPUs=%d, CPUs=%d, Memory=%.0fGB",
		totalGPUs, totalCPUs, totalMemGB)
}

// ============================================================================
// CORE DRF CHECK
// ============================================================================

// CheckFairness: Can this tenant schedule a job with these resources?
//
// Algorithm:
// 1. Calculate what tenant's shares WOULD BE if job is admitted
// 2. Compute dominant share (max of all resource shares)
// 3. If dominant share > threshold AND other tenants have lower shares → deny
// 4. If dominant share <= threshold OR tenant has lowest share → allow
func (dm *DRFManager) CheckFairness(
	ctx context.Context,
	tenantID string,
	requestedGPUs int,
	requestedCPUs int,
	requestedMemGB float64,
) *DRFDecision {

	dm.mu.Lock()
	defer dm.mu.Unlock()

	// No tenant specified = always allow (backward compatible)
	if tenantID == "" {
		return &DRFDecision{
			Allowed:  true,
			TenantID: "",
			Reason:   "no tenant specified, DRF bypassed",
		}
	}

	// Get or create tenant usage
	usage, exists := dm.tenantUsage[tenantID]
	if !exists {
		usage = &TenantUsage{
			TenantID: tenantID,
			MaxShare: dm.config.DefaultTenantQuota,
		}
		dm.tenantUsage[tenantID] = usage
	}

	// Calculate projected shares (after this job would be admitted)
	projectedGPUs := usage.GPUsInUse + requestedGPUs
	projectedCPUs := usage.CPUsInUse + requestedCPUs
	projectedMem := usage.MemGBInUse + requestedMemGB

	gpuShare := 0.0
	cpuShare := 0.0
	memShare := 0.0

	if dm.totalGPUs > 0 {
		gpuShare = float64(projectedGPUs) / float64(dm.totalGPUs)
	}
	if dm.totalCPUs > 0 {
		cpuShare = float64(projectedCPUs) / float64(dm.totalCPUs)
	}
	if dm.totalMemGB > 0 {
		memShare = projectedMem / dm.totalMemGB
	}

	// Dominant share = max of all resource shares
	dominantShare := gpuShare
	dominantType := "GPU"
	if cpuShare > dominantShare {
		dominantShare = cpuShare
		dominantType = "CPU"
	}
	if memShare > dominantShare {
		dominantShare = memShare
		dominantType = "Memory"
	}

	decision := &DRFDecision{
		TenantID:      tenantID,
		DominantShare: dominantShare,
		DominantType:  dominantType,
		GPUShare:      gpuShare,
		CPUShare:      cpuShare,
		MemoryShare:   memShare,
	}

	// Check 1: Would this exceed tenant's max quota?
	if dominantShare > usage.MaxShare {
		// Exception: if this tenant has the LOWEST dominant share, still allow
		if !dm.hasLowestDominantShare(tenantID) {
			decision.Allowed = false
			decision.Reason = fmt.Sprintf(
				"tenant %s dominant share would be %.1f%% (%s), exceeds quota %.1f%%",
				tenantID, dominantShare*100, dominantType, usage.MaxShare*100)
			dm.log.Warn("DRF DENIED: %s", decision.Reason)
			return decision
		}
	}

	// Check 2: Would this exceed the global fairness threshold?
	if dominantShare > dm.config.FairnessThreshold {
		if !dm.hasLowestDominantShare(tenantID) {
			decision.Allowed = false
			decision.Reason = fmt.Sprintf(
				"tenant %s dominant share would be %.1f%% (%s), exceeds fairness threshold %.1f%%",
				tenantID, dominantShare*100, dominantType, dm.config.FairnessThreshold*100)
			dm.log.Warn("DRF DENIED: %s", decision.Reason)
			return decision
		}
	}

	// Allowed
	decision.Allowed = true
	decision.Reason = fmt.Sprintf(
		"tenant %s dominant share %.1f%% (%s) within limits",
		tenantID, dominantShare*100, dominantType)

	dm.log.Debug("DRF ALLOWED: %s (GPU=%.1f%%, CPU=%.1f%%, Mem=%.1f%%)",
		tenantID, gpuShare*100, cpuShare*100, memShare*100)

	return decision
}

// hasLowestDominantShare: Does this tenant have the lowest dominant share?
// If yes, DRF says they should get the next resource (even if above threshold)
func (dm *DRFManager) hasLowestDominantShare(tenantID string) bool {
	target, exists := dm.tenantUsage[tenantID]
	if !exists {
		return true // New tenant always has lowest share
	}

	for id, usage := range dm.tenantUsage {
		if id == tenantID {
			continue
		}
		if usage.DominantShare < target.DominantShare {
			return false // Someone else has a lower share
		}
	}

	return true
}

// ============================================================================
// RESOURCE ACCOUNTING
// ============================================================================

// OnJobScheduled: Update tenant usage when a job is scheduled
func (dm *DRFManager) OnJobScheduled(tenantID string, gpus, cpus int, memGB float64) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if tenantID == "" {
		return
	}

	usage, exists := dm.tenantUsage[tenantID]
	if !exists {
		usage = &TenantUsage{
			TenantID: tenantID,
			MaxShare: dm.config.DefaultTenantQuota,
		}
		dm.tenantUsage[tenantID] = usage
	}

	usage.GPUsInUse += gpus
	usage.CPUsInUse += cpus
	usage.MemGBInUse += memGB
	usage.RunningJobs++
	usage.TotalJobsRun++
	usage.LastUpdated = time.Now()

	dm.recalculateShares(usage)

	dm.log.Info("DRF: Tenant %s job scheduled (GPUs=%d/%d, dominant=%.1f%% %s)",
		tenantID, usage.GPUsInUse, dm.totalGPUs, usage.DominantShare*100, usage.DominantType)
}

// OnJobCompleted: Update tenant usage when a job finishes
func (dm *DRFManager) OnJobCompleted(tenantID string, gpus, cpus int, memGB float64) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if tenantID == "" {
		return
	}

	usage, exists := dm.tenantUsage[tenantID]
	if !exists {
		return
	}

	usage.GPUsInUse -= gpus
	if usage.GPUsInUse < 0 {
		usage.GPUsInUse = 0
	}
	usage.CPUsInUse -= cpus
	if usage.CPUsInUse < 0 {
		usage.CPUsInUse = 0
	}
	usage.MemGBInUse -= memGB
	if usage.MemGBInUse < 0 {
		usage.MemGBInUse = 0
	}
	usage.RunningJobs--
	if usage.RunningJobs < 0 {
		usage.RunningJobs = 0
	}
	usage.LastUpdated = time.Now()

	dm.recalculateShares(usage)

	dm.log.Info("DRF: Tenant %s job completed (GPUs=%d/%d, dominant=%.1f%% %s)",
		tenantID, usage.GPUsInUse, dm.totalGPUs, usage.DominantShare*100, usage.DominantType)
}

// recalculateShares: Recalculate a tenant's resource shares
func (dm *DRFManager) recalculateShares(usage *TenantUsage) {
	usage.GPUShare = 0
	usage.CPUShare = 0
	usage.MemoryShare = 0

	if dm.totalGPUs > 0 {
		usage.GPUShare = float64(usage.GPUsInUse) / float64(dm.totalGPUs)
	}
	if dm.totalCPUs > 0 {
		usage.CPUShare = float64(usage.CPUsInUse) / float64(dm.totalCPUs)
	}
	if dm.totalMemGB > 0 {
		usage.MemoryShare = usage.MemGBInUse / dm.totalMemGB
	}

	usage.DominantShare = usage.GPUShare
	usage.DominantType = "GPU"
	if usage.CPUShare > usage.DominantShare {
		usage.DominantShare = usage.CPUShare
		usage.DominantType = "CPU"
	}
	if usage.MemoryShare > usage.DominantShare {
		usage.DominantShare = usage.MemoryShare
		usage.DominantType = "Memory"
	}
}

// ============================================================================
// TENANT MANAGEMENT
// ============================================================================

// SetTenantQuota: Set max share for a specific tenant
func (dm *DRFManager) SetTenantQuota(tenantID string, maxShare float64) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if maxShare <= 0 || maxShare > 1.0 {
		maxShare = dm.config.DefaultTenantQuota
	}

	usage, exists := dm.tenantUsage[tenantID]
	if !exists {
		usage = &TenantUsage{
			TenantID: tenantID,
		}
		dm.tenantUsage[tenantID] = usage
	}
	usage.MaxShare = maxShare

	dm.log.Info("DRF: Set tenant %s quota to %.0f%%", tenantID, maxShare*100)
}

// ============================================================================
// STATS & MONITORING
// ============================================================================

// GetTenantStats: Get all tenant usage stats (for dashboard/API)
func (dm *DRFManager) GetTenantStats() map[string]*TenantUsage {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	stats := make(map[string]*TenantUsage, len(dm.tenantUsage))
	for id, usage := range dm.tenantUsage {
		copy := *usage
		stats[id] = &copy
	}
	return stats
}

// GetFairnessReport: Summary of fairness across all tenants
func (dm *DRFManager) GetFairnessReport() map[string]interface{} {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	tenantCount := len(dm.tenantUsage)
	if tenantCount == 0 {
		return map[string]interface{}{
			"tenant_count":   0,
			"is_fair":        true,
			"fairness_index": 1.0,
		}
	}

	// Jain's Fairness Index: (sum(x))^2 / (n * sum(x^2))
	// 1.0 = perfectly fair, 1/n = maximally unfair
	sumShares := 0.0
	sumSquares := 0.0
	maxShare := 0.0
	minShare := 1.0

	tenants := make([]map[string]interface{}, 0, tenantCount)

	for _, usage := range dm.tenantUsage {
		sumShares += usage.DominantShare
		sumSquares += usage.DominantShare * usage.DominantShare

		if usage.DominantShare > maxShare {
			maxShare = usage.DominantShare
		}
		if usage.DominantShare < minShare {
			minShare = usage.DominantShare
		}

		tenants = append(tenants, map[string]interface{}{
			"tenant_id":      usage.TenantID,
			"dominant_share": usage.DominantShare,
			"dominant_type":  usage.DominantType,
			"gpu_share":      usage.GPUShare,
			"cpu_share":      usage.CPUShare,
			"memory_share":   usage.MemoryShare,
			"running_jobs":   usage.RunningJobs,
			"gpus_in_use":    usage.GPUsInUse,
		})
	}

	fairnessIndex := 1.0
	if sumSquares > 0 && tenantCount > 0 {
		fairnessIndex = (sumShares * sumShares) / (float64(tenantCount) * sumSquares)
	}

	return map[string]interface{}{
		"tenant_count":    tenantCount,
		"fairness_index":  fairnessIndex,
		"is_fair":         fairnessIndex > 0.9,
		"max_share":       maxShare,
		"min_share":       minShare,
		"share_spread":    maxShare - minShare,
		"threshold":       dm.config.FairnessThreshold,
		"total_gpus":      dm.totalGPUs,
		"total_cpus":      dm.totalCPUs,
		"total_memory_gb": dm.totalMemGB,
		"tenants":         tenants,
	}
}
