package drf

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// SECTION 1: Constructor & Defaults
// ============================================================================

func TestNewDRFManager_NilConfig(t *testing.T) {
	dm := NewDRFManager(nil)
	require.NotNil(t, dm)
	assert.Equal(t, DefaultDRFConfig.FairnessThreshold, dm.config.FairnessThreshold)
	assert.Equal(t, DefaultDRFConfig.GPUWeight, dm.config.GPUWeight)
	assert.Equal(t, DefaultDRFConfig.DefaultTenantQuota, dm.config.DefaultTenantQuota)
}

func TestNewDRFManager_CustomConfig(t *testing.T) {
	cfg := &DRFConfig{
		FairnessThreshold:  0.6,
		DefaultTenantQuota: 0.3,
		GPUWeight:          5.0,
	}
	dm := NewDRFManager(cfg)
	assert.Equal(t, 0.6, dm.config.FairnessThreshold)
	assert.Equal(t, 0.3, dm.config.DefaultTenantQuota)
}

// ============================================================================
// SECTION 2: Capacity Updates
// ============================================================================

func TestUpdateCapacity(t *testing.T) {
	dm := NewDRFManager(nil)
	dm.UpdateCapacity(100, 1000, 500.0)

	assert.Equal(t, 100, dm.totalGPUs)
	assert.Equal(t, 1000, dm.totalCPUs)
	assert.Equal(t, 500.0, dm.totalMemGB)
}

// ============================================================================
// SECTION 3: Core Fairness Check
// ============================================================================

func TestCheckFairness_EmptyTenant(t *testing.T) {
	dm := NewDRFManager(nil)
	dm.UpdateCapacity(100, 1000, 1000.0)

	decision := dm.CheckFairness(context.Background(), "", 10, 100, 100.0)
	assert.True(t, decision.Allowed)
	assert.Contains(t, decision.Reason, "no tenant specified")
}

func TestCheckFairness_NewTenantAllowed(t *testing.T) {
	dm := NewDRFManager(nil)
	dm.UpdateCapacity(100, 1000, 1000.0)

	decision := dm.CheckFairness(context.Background(), "tenant-a", 10, 100, 100.0)
	assert.True(t, decision.Allowed)
	assert.Equal(t, 0.1, decision.GPUShare)  // 10/100
	assert.Equal(t, 0.1, decision.CPUShare)  // 100/1000
	assert.Equal(t, 0.1, decision.MemoryShare) // 100/1000
	assert.Equal(t, "GPU", decision.DominantType)
}

func TestCheckFairness_ZeroCapacity(t *testing.T) {
	dm := NewDRFManager(nil)
	// Don't call UpdateCapacity — shares will all be 0

	decision := dm.CheckFairness(context.Background(), "tenant-a", 10, 100, 100.0)
	assert.True(t, decision.Allowed, "should allow when capacity not set")
	assert.Equal(t, 0.0, decision.GPUShare)
}

func TestCheckFairness_DominantShareCalculation(t *testing.T) {
	dm := NewDRFManager(nil)
	dm.UpdateCapacity(100, 1000, 1000.0)

	// Tenant A: GPU-heavy (50 GPUs = 50%, 100 CPUs = 10%, 200 mem = 20%)
	dm.OnJobScheduled("tenant-a", 50, 100, 200.0)

	stats := dm.GetTenantStats()
	usage := stats["tenant-a"]
	assert.Equal(t, 0.5, usage.GPUShare)
	assert.Equal(t, 0.1, usage.CPUShare)
	assert.Equal(t, 0.2, usage.MemoryShare)
	assert.Equal(t, 0.5, usage.DominantShare)
	assert.Equal(t, "GPU", usage.DominantType)
}

func TestCheckFairness_CPUDominant(t *testing.T) {
	dm := NewDRFManager(nil)
	dm.UpdateCapacity(100, 1000, 1000.0)

	// Tenant B: CPU-heavy (10 GPUs = 10%, 500 CPUs = 50%, 100 mem = 10%)
	dm.OnJobScheduled("tenant-b", 10, 500, 100.0)

	stats := dm.GetTenantStats()
	usage := stats["tenant-b"]
	assert.Equal(t, 0.5, usage.DominantShare)
	assert.Equal(t, "CPU", usage.DominantType)
}

func TestCheckFairness_MemoryDominant(t *testing.T) {
	dm := NewDRFManager(nil)
	dm.UpdateCapacity(100, 1000, 1000.0)

	dm.OnJobScheduled("tenant-c", 5, 50, 800.0)

	stats := dm.GetTenantStats()
	usage := stats["tenant-c"]
	assert.Equal(t, 0.8, usage.DominantShare)
	assert.Equal(t, "Memory", usage.DominantType)
}

func TestCheckFairness_DeniedExceedsQuota(t *testing.T) {
	cfg := &DRFConfig{
		FairnessThreshold:  0.8,
		DefaultTenantQuota: 0.5,
		GPUWeight:          3.0,
		CPUWeight:          1.0,
		MemoryWeight:       0.5,
	}
	dm := NewDRFManager(cfg)
	dm.UpdateCapacity(100, 1000, 1000.0)

	// Give tenant-a 40 GPUs (40% share)
	dm.OnJobScheduled("tenant-a", 40, 0, 0)
	// Give tenant-b 10 GPUs (10% share) — they have lower share
	dm.OnJobScheduled("tenant-b", 10, 0, 0)

	// Tenant-a requesting 20 more → projected 60% > quota 50%
	// tenant-b has lower share, so tenant-a is NOT lowest → denied
	decision := dm.CheckFairness(context.Background(), "tenant-a", 20, 0, 0)
	assert.False(t, decision.Allowed)
	assert.Contains(t, decision.Reason, "exceeds quota")
}

func TestCheckFairness_AllowedLowestShare(t *testing.T) {
	cfg := &DRFConfig{
		FairnessThreshold:  0.8,
		DefaultTenantQuota: 0.5,
		GPUWeight:          3.0,
		CPUWeight:          1.0,
		MemoryWeight:       0.5,
	}
	dm := NewDRFManager(cfg)
	dm.UpdateCapacity(100, 1000, 1000.0)

	// Give tenant-a 60% GPU share
	dm.OnJobScheduled("tenant-a", 60, 0, 0)
	// Give tenant-b 10% GPU share
	dm.OnJobScheduled("tenant-b", 10, 0, 0)

	// Tenant-b requesting 50 more → projected 60% > quota 50%
	// But tenant-b has LOWEST dominant share → allowed (DRF says lowest gets priority)
	decision := dm.CheckFairness(context.Background(), "tenant-b", 50, 0, 0)
	assert.True(t, decision.Allowed)
}

func TestCheckFairness_DeniedExceedsFairnessThreshold(t *testing.T) {
	cfg := &DRFConfig{
		FairnessThreshold:  0.5,
		DefaultTenantQuota: 1.0, // no quota limit
	}
	dm := NewDRFManager(cfg)
	dm.UpdateCapacity(100, 1000, 1000.0)

	// Tenant-a at 40% GPU share
	dm.OnJobScheduled("tenant-a", 40, 0, 0)
	// Tenant-b at 10% GPU share
	dm.OnJobScheduled("tenant-b", 10, 0, 0)

	// Tenant-a requesting 20 more → 60% > threshold 50%, and not lowest → denied
	decision := dm.CheckFairness(context.Background(), "tenant-a", 20, 0, 0)
	assert.False(t, decision.Allowed)
	assert.Contains(t, decision.Reason, "exceeds fairness threshold")
}

// ============================================================================
// SECTION 4: Resource Accounting
// ============================================================================

func TestOnJobScheduled_TracksUsage(t *testing.T) {
	dm := NewDRFManager(nil)
	dm.UpdateCapacity(100, 1000, 1000.0)

	dm.OnJobScheduled("tenant-a", 8, 16, 64.0)

	stats := dm.GetTenantStats()
	usage := stats["tenant-a"]
	assert.Equal(t, 8, usage.GPUsInUse)
	assert.Equal(t, 16, usage.CPUsInUse)
	assert.Equal(t, 64.0, usage.MemGBInUse)
	assert.Equal(t, 1, usage.RunningJobs)
	assert.Equal(t, 1, usage.TotalJobsRun)
}

func TestOnJobCompleted_ReleasesResources(t *testing.T) {
	dm := NewDRFManager(nil)
	dm.UpdateCapacity(100, 1000, 1000.0)

	dm.OnJobScheduled("tenant-a", 8, 16, 64.0)
	dm.OnJobCompleted("tenant-a", 8, 16, 64.0)

	stats := dm.GetTenantStats()
	usage := stats["tenant-a"]
	assert.Equal(t, 0, usage.GPUsInUse)
	assert.Equal(t, 0, usage.CPUsInUse)
	assert.Equal(t, 0.0, usage.MemGBInUse)
	assert.Equal(t, 0, usage.RunningJobs)
	assert.Equal(t, 1, usage.TotalJobsRun) // total doesn't decrease
}

func TestOnJobCompleted_NeverGoesNegative(t *testing.T) {
	dm := NewDRFManager(nil)
	dm.UpdateCapacity(100, 1000, 1000.0)

	// Complete without scheduling (shouldn't panic or go negative)
	dm.OnJobCompleted("tenant-a", 8, 16, 64.0)
	// No entry should have been created since tenant didn't exist
}

func TestOnJobScheduled_EmptyTenant(t *testing.T) {
	dm := NewDRFManager(nil)
	dm.OnJobScheduled("", 8, 16, 64.0)
	assert.Empty(t, dm.GetTenantStats())
}

func TestOnJobCompleted_EmptyTenant(t *testing.T) {
	dm := NewDRFManager(nil)
	dm.OnJobCompleted("", 8, 16, 64.0)
	assert.Empty(t, dm.GetTenantStats())
}

// ============================================================================
// SECTION 5: Tenant Quota Management
// ============================================================================

func TestSetTenantQuota(t *testing.T) {
	dm := NewDRFManager(nil)
	dm.SetTenantQuota("tenant-a", 0.7)

	stats := dm.GetTenantStats()
	assert.Equal(t, 0.7, stats["tenant-a"].MaxShare)
}

func TestSetTenantQuota_InvalidValue(t *testing.T) {
	dm := NewDRFManager(nil)
	dm.SetTenantQuota("tenant-a", -0.5)

	stats := dm.GetTenantStats()
	assert.Equal(t, DefaultDRFConfig.DefaultTenantQuota, stats["tenant-a"].MaxShare)
}

func TestSetTenantQuota_OverOne(t *testing.T) {
	dm := NewDRFManager(nil)
	dm.SetTenantQuota("tenant-a", 1.5)

	stats := dm.GetTenantStats()
	assert.Equal(t, DefaultDRFConfig.DefaultTenantQuota, stats["tenant-a"].MaxShare)
}

func TestSetTenantQuota_ExactlyOne(t *testing.T) {
	dm := NewDRFManager(nil)
	dm.SetTenantQuota("tenant-a", 1.0)

	stats := dm.GetTenantStats()
	assert.Equal(t, 1.0, stats["tenant-a"].MaxShare)
}

// ============================================================================
// SECTION 6: Fairness Report & Stats
// ============================================================================

func TestGetFairnessReport_NoTenants(t *testing.T) {
	dm := NewDRFManager(nil)
	report := dm.GetFairnessReport()

	assert.Equal(t, 0, report["tenant_count"])
	assert.Equal(t, true, report["is_fair"])
	assert.Equal(t, 1.0, report["fairness_index"])
}

func TestGetFairnessReport_PerfectFairness(t *testing.T) {
	dm := NewDRFManager(nil)
	dm.UpdateCapacity(100, 1000, 1000.0)

	// Both tenants at exactly 50% dominant share
	dm.OnJobScheduled("tenant-a", 50, 100, 200.0) // GPU dominant: 50%
	dm.OnJobScheduled("tenant-b", 10, 500, 100.0)  // CPU dominant: 50%

	report := dm.GetFairnessReport()
	assert.Equal(t, 2, report["tenant_count"])
	assert.Equal(t, 1.0, report["fairness_index"])
	assert.Equal(t, true, report["is_fair"])
	assert.Equal(t, 0.0, report["share_spread"])
}

func TestGetFairnessReport_UnfairDistribution(t *testing.T) {
	dm := NewDRFManager(nil)
	dm.UpdateCapacity(100, 1000, 1000.0)

	dm.OnJobScheduled("tenant-a", 90, 0, 0) // 90% GPU dominant
	dm.OnJobScheduled("tenant-b", 5, 0, 0)  // 5% GPU dominant

	report := dm.GetFairnessReport()
	fairnessIndex := report["fairness_index"].(float64)
	assert.Less(t, fairnessIndex, 0.9, "unfair distribution should have low Jain's index")
}

func TestGetTenantStats_ReturnsCopy(t *testing.T) {
	dm := NewDRFManager(nil)
	dm.UpdateCapacity(100, 1000, 1000.0)
	dm.OnJobScheduled("tenant-a", 10, 0, 0)

	stats := dm.GetTenantStats()
	stats["tenant-a"].GPUsInUse = 999 // mutate the copy

	// Original should be unchanged
	stats2 := dm.GetTenantStats()
	assert.Equal(t, 10, stats2["tenant-a"].GPUsInUse)
}

// ============================================================================
// SECTION 7: Concurrency
// ============================================================================

func TestConcurrentFairnessChecks(t *testing.T) {
	dm := NewDRFManager(nil)
	dm.UpdateCapacity(1000, 10000, 10000.0)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			tenantID := "tenant"
			if n%2 == 0 {
				tenantID = "tenant-a"
			} else {
				tenantID = "tenant-b"
			}
			dm.CheckFairness(context.Background(), tenantID, 1, 1, 1.0)
			dm.OnJobScheduled(tenantID, 1, 1, 1.0)
		}(i)
	}
	wg.Wait()

	// Should not panic or race
	stats := dm.GetTenantStats()
	totalGPUs := 0
	for _, u := range stats {
		totalGPUs += u.GPUsInUse
	}
	assert.Equal(t, 50, totalGPUs)
}
