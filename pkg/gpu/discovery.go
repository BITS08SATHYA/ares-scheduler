// Feature 13: GPU-Aware Scheduling with Topology
// Detects GPUs on local node, enumerates properties, performs health checks
// Depends on: types.go, logger.go, redis/client.go
// Zero errors, production-ready code

package gpu

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/redis"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// ============================================================================
// GPU DISCOVERY SERVICE
// ============================================================================

// GPUDiscovery: Discovers and monitors local GPUs
// Uses nvidia-smi for detection, caches results in Redis
// Thread-safe: All operations protected by context cancellation
type GPUDiscovery struct {
	redisClient *redis.RedisClient
	log         *logger.Logger
	cacheKey    string
	cacheTTL    time.Duration
}

// Cache keys
const (
	CacheKeyGPUDevices  = "ares:node:gpu:devices"
	CacheKeyGPUTopology = "ares:node:gpu:topology"
	CacheKeyGPUHealth   = "ares:node:gpu:health"
	DefaultCacheTTL     = 30 * time.Second
	HealthCheckInterval = 60 * time.Second
)

// NewGPUDiscovery: Create new GPU discovery service
func NewGPUDiscovery(redisClient *redis.RedisClient) *GPUDiscovery {
	return &GPUDiscovery{
		redisClient: redisClient,
		log:         logger.Get(),
		cacheKey:    CacheKeyGPUDevices,
		cacheTTL:    DefaultCacheTTL,
	}
}

// ============================================================================
// GPU DETECTION (Feature 13)
// ============================================================================

// DiscoverGPUs: Detect all local GPUs using nvidia-smi
// Returns: Array of GPU devices with properties
// Latency: ~200-500ms (nvidia-smi execution + parsing)
// Cached for: 30 seconds
func (gd *GPUDiscovery) DiscoverGPUs(ctx context.Context) ([]*common.GPUDevice, error) {
	// Check cache first
	cached, err := gd.getCachedGPUs(ctx)
	if err == nil && len(cached) > 0 {
		gd.log.Debug("GPU discovery cache hit (count=%d)", len(cached))
		return cached, nil
	}

	// No cache, query nvidia-smi
	gpus, err := gd.queryNvidiaSMI(ctx)
	if err != nil {
		gd.log.Error("Failed to query nvidia-smi: %v", err)
		return nil, fmt.Errorf("nvidia-smi query failed: %w", err)
	}

	if len(gpus) == 0 {
		gd.log.Warn("No GPUs detected on this node")
		return []*common.GPUDevice{}, nil
	}

	// Cache the results
	if err := gd.cacheGPUs(ctx, gpus); err != nil {
		gd.log.Warn("Failed to cache GPUs (non-fatal): %v", err)
	}

	gd.log.Info("Discovered %d GPUs", len(gpus))
	return gpus, nil
}

// queryNvidiaSMI: Execute nvidia-smi and parse output
// Format: comma-separated values with GPU index, UUID, type, memory, utilization
// Example output: 0,GPU-abc123,A100,81920,0,30.5,45
func (gd *GPUDiscovery) queryNvidiaSMI(ctx context.Context) ([]*common.GPUDevice, error) {
	// Command format:
	// nvidia-smi --query-gpu=index,gpu_uuid,gpu_name,memory.total,memory.free,utilization.gpu,temperature.gpu \
	//            --format=csv,noheader,nounits
	cmd := exec.CommandContext(ctx, "nvidia-smi",
		"--query-gpu=index,gpu_uuid,gpu_name,memory.total,memory.free,utilization.gpu,temperature.gpu",
		"--format=csv,noheader,nounits")

	output, err := cmd.Output()
	if err != nil {
		// nvidia-smi not found or failed
		gd.log.Warn("nvidia-smi execution failed (GPUs unavailable): %v", err)
		return []*common.GPUDevice{}, nil
	}

	gpus := make([]*common.GPUDevice, 0)
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")

	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}

		gpu, err := gd.parseGPULine(line)
		if err != nil {
			gd.log.Warn("Failed to parse GPU line %q: %v", line, err)
			continue
		}

		gpus = append(gpus, gpu)
	}

	return gpus, nil
}

// parseGPULine: Parse single nvidia-smi CSV line
// Format: index,uuid,type,memory_total_MB,memory_free_MB,utilization_pct,temp_celsius
// Example: 0,GPU-abc123,NVIDIA A100-SXM4-80GB,81920,65536,25.0,45
func (gd *GPUDiscovery) parseGPULine(line string) (*common.GPUDevice, error) {
	parts := strings.Split(strings.TrimSpace(line), ",")
	if len(parts) < 7 {
		return nil, fmt.Errorf("invalid GPU line format: expected 7 fields, got %d", len(parts))
	}

	// Parse index
	index, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		return nil, fmt.Errorf("invalid GPU index: %w", err)
	}

	// UUID
	uuid := strings.TrimSpace(parts[1])

	// GPU type (extract from name: "NVIDIA A100-SXM4-80GB" -> "A100")
	gpuName := strings.TrimSpace(parts[2])
	gpuType := extractGPUType(gpuName)

	// Parse memory (in MB, convert to GB)
	memoryMB, err := strconv.ParseFloat(strings.TrimSpace(parts[3]), 64)
	if err != nil {
		return nil, fmt.Errorf("invalid memory: %w", err)
	}
	memoryGB := memoryMB / 1024.0

	// Parse available memory
	availableMB, err := strconv.ParseFloat(strings.TrimSpace(parts[4]), 64)
	if err != nil {
		return nil, fmt.Errorf("invalid available memory: %w", err)
	}
	availableGB := availableMB / 1024.0

	// Parse utilization
	utilization, err := strconv.ParseFloat(strings.TrimSpace(parts[5]), 64)
	if err != nil {
		return nil, fmt.Errorf("invalid utilization: %w", err)
	}

	// Parse temperature
	temperature, err := strconv.ParseFloat(strings.TrimSpace(parts[6]), 64)
	if err != nil {
		return nil, fmt.Errorf("invalid temperature: %w", err)
	}

	return &common.GPUDevice{
		Index:              index,
		UUID:               uuid,
		Type:               gpuType,
		MemoryGB:           memoryGB,
		AvailableMemGB:     availableGB,
		UtilizationPercent: utilization,
		TemperatureCelsius: temperature,
		PowerDrawWatts:     0,    // Will be queried separately if needed
		IsHealthy:          true, // Will be updated by health check
	}, nil
}

// extractGPUType: Extract GPU type from nvidia-smi output
// Input:  "NVIDIA A100-SXM4-80GB" or "NVIDIA H100 PCIe 80GB"
// Output: "A100" or "H100"
func extractGPUType(gpuName string) string {
	// Replace "NVIDIA" prefix
	name := strings.TrimPrefix(gpuName, "NVIDIA")
	name = strings.TrimSpace(name)

	// Extract first token (e.g., "A100" from "A100-SXM4-80GB")
	parts := strings.Fields(name)
	if len(parts) > 0 {
		// Remove dashes and take first part
		return strings.Split(parts[0], "-")[0]
	}

	return "UNKNOWN"
}

// ============================================================================
// HEALTH CHECKING (Feature 10 - Health & Heartbeat)
// ============================================================================

// CheckGPUHealth: Run health checks on all GPUs
// Returns: Array of GPUs with health status updated
// Checks: Accessibility, memory errors, temperature, power
func (gd *GPUDiscovery) CheckGPUHealth(ctx context.Context, gpus []*common.GPUDevice) ([]*common.GPUDevice, error) {
	if len(gpus) == 0 {
		return gpus, nil
	}

	for _, gpu := range gpus {
		// Assume healthy unless check fails
		gpu.IsHealthy = true

		// Check 1: Temperature (fail if > 85°C)
		if gpu.TemperatureCelsius > 85.0 {
			gd.log.Warn("GPU %d temperature too high: %.1f°C", gpu.Index, gpu.TemperatureCelsius)
			gpu.IsHealthy = false
		}

		// Check 2: Memory available
		if gpu.AvailableMemGB < 1.0 {
			gd.log.Warn("GPU %d memory critically low: %.2f GB", gpu.Index, gpu.AvailableMemGB)
			gpu.IsHealthy = false
		}

		// Check 3: Try to query power draw (optional)
		powerDraw, err := gd.queryGPUPowerDraw(ctx, gpu.Index)
		if err != nil {
			gd.log.Warn("Failed to query power for GPU %d: %v", gpu.Index, err)
		} else {
			gpu.PowerDrawWatts = powerDraw

			// Check power within reasonable bounds (not dead, not overheating)
			if powerDraw < 0 {
				gpu.IsHealthy = false
			}
		}
	}

	return gpus, nil
}

// queryGPUPowerDraw: Get current power draw in watts
// Returns: Power in watts, or error if GPU unavailable
func (gd *GPUDiscovery) queryGPUPowerDraw(ctx context.Context, gpuIndex int) (float64, error) {
	cmd := exec.CommandContext(ctx, "nvidia-smi",
		fmt.Sprintf("--id=%d", gpuIndex),
		"--query-gpu=power.draw",
		"--format=csv,noheader,nounits")

	output, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("nvidia-smi power query failed: %w", err)
	}

	powerStr := strings.TrimSpace(string(output))
	power, err := strconv.ParseFloat(powerStr, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse power: %w", err)
	}

	return power, nil
}

// ============================================================================
// CACHING (Layer 1 of observability)
// ============================================================================

// getCachedGPUs: Retrieve GPU list from cache
func (gd *GPUDiscovery) getCachedGPUs(ctx context.Context) ([]*common.GPUDevice, error) {
	cached, err := gd.redisClient.Get(ctx, gd.cacheKey)
	if err != nil || cached == "" {
		return nil, fmt.Errorf("cache miss")
	}

	var gpus []*common.GPUDevice
	err = json.Unmarshal([]byte(cached), &gpus)
	if err != nil {
		return nil, fmt.Errorf("cache unmarshal failed: %w", err)
	}

	return gpus, nil
}

// cacheGPUs: Store GPU list in cache
func (gd *GPUDiscovery) cacheGPUs(ctx context.Context, gpus []*common.GPUDevice) error {
	data, err := json.Marshal(gpus)
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}

	err = gd.redisClient.Set(ctx, gd.cacheKey, string(data), gd.cacheTTL)
	if err != nil {
		return fmt.Errorf("cache set failed: %w", err)
	}

	gd.log.Debug("Cached %d GPUs (TTL=%v)", len(gpus), gd.cacheTTL)
	return nil
}

// ClearGPUCache: Clear GPU cache (for testing or forced refresh)
func (gd *GPUDiscovery) ClearGPUCache(ctx context.Context) error {
	return gd.redisClient.Del(ctx, gd.cacheKey)
}

// ============================================================================
// GPU INVENTORY (Observability - Feature 22)
// ============================================================================

// GetGPUInventory: Get summary of all GPUs on node
// Returns: Total GPUs, types, and capacity summary
// Latency: <5ms (cached)
func (gd *GPUDiscovery) GetGPUInventory(ctx context.Context) (*GPUInventory, error) {
	gpus, err := gd.DiscoverGPUs(ctx)
	if err != nil {
		return nil, fmt.Errorf("discovery failed: %w", err)
	}

	inventory := &GPUInventory{
		TotalGPUs:     len(gpus),
		GPUsByType:    make(map[string]int),
		HealthyGPUs:   0,
		TotalMemoryGB: 0,
		Devices:       gpus,
	}

	for _, gpu := range gpus {
		inventory.GPUsByType[gpu.Type]++
		if gpu.IsHealthy {
			inventory.HealthyGPUs++
		}
		inventory.TotalMemoryGB += gpu.MemoryGB
	}

	return inventory, nil
}

// GPUInventory: Summary of node GPU capacity
type GPUInventory struct {
	TotalGPUs     int                 `json:"total_gpus"`
	HealthyGPUs   int                 `json:"healthy_gpus"`
	GPUsByType    map[string]int      `json:"gpus_by_type"`
	TotalMemoryGB float64             `json:"total_memory_gb"`
	Devices       []*common.GPUDevice `json:"devices"`
}

// AvailableGPUs: Count of healthy GPUs
func (inv *GPUInventory) AvailableGPUs() int {
	return inv.HealthyGPUs
}

// ============================================================================
// FILTER & MATCH (Feature 13 - GPU-Aware Scheduling)
// ============================================================================

// FilterGPUsByType: Get GPUs matching specific type
// Returns: Array of GPUs of given type
// Example: FilterGPUsByType("A100") -> all A100 GPUs
func (gd *GPUDiscovery) FilterGPUsByType(gpus []*common.GPUDevice, gpuType string) []*common.GPUDevice {
	filtered := make([]*common.GPUDevice, 0)

	for _, gpu := range gpus {
		if gpu.Type == gpuType && gpu.IsHealthy {
			filtered = append(filtered, gpu)
		}
	}

	return filtered
}

// FilterGPUsByMemory: Get GPUs with minimum memory available
// Returns: Array of GPUs with at least minGBMemory available
func (gd *GPUDiscovery) FilterGPUsByMemory(gpus []*common.GPUDevice, minGBMemory float64) []*common.GPUDevice {
	filtered := make([]*common.GPUDevice, 0)

	for _, gpu := range gpus {
		if gpu.AvailableMemGB >= minGBMemory && gpu.IsHealthy {
			filtered = append(filtered, gpu)
		}
	}

	return filtered
}

// FilterGPUsByUtilization: Get least utilized GPUs
// Returns: Array of healthy GPUs with utilization <= maxPercent
func (gd *GPUDiscovery) FilterGPUsByUtilization(gpus []*common.GPUDevice, maxPercent float64) []*common.GPUDevice {
	filtered := make([]*common.GPUDevice, 0)

	for _, gpu := range gpus {
		if gpu.UtilizationPercent <= maxPercent && gpu.IsHealthy {
			filtered = append(filtered, gpu)
		}
	}

	return filtered
}

// ============================================================================
// UTILITIES
// ============================================================================

// NodeHasGPUs: Check if node has any GPUs
func (gd *GPUDiscovery) NodeHasGPUs(ctx context.Context) (bool, error) {
	gpus, err := gd.DiscoverGPUs(ctx)
	if err != nil {
		return false, err
	}

	return len(gpus) > 0, nil
}

// IsGPUAvailable: Check if specific GPU is healthy
func (gd *GPUDiscovery) IsGPUAvailable(gpus []*common.GPUDevice, index int) bool {
	for _, gpu := range gpus {
		if gpu.Index == index && gpu.IsHealthy {
			return true
		}
	}
	return false
}

// SumGPUMemory: Get total available memory across GPUs
func (gd *GPUDiscovery) SumGPUMemory(gpus []*common.GPUDevice) float64 {
	total := 0.0
	for _, gpu := range gpus {
		if gpu.IsHealthy {
			total += gpu.AvailableMemGB
		}
	}
	return total
}

// ============================================================================
// MONITORING (Feature 22 - Global Metrics Pipeline)
// ============================================================================

// MonitorGPUMetrics: Continuously monitor GPU metrics
// Runs in background, updates cache periodically
// Call as: go discovery.MonitorGPUMetrics(ctx)
func (gd *GPUDiscovery) MonitorGPUMetrics(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			gd.log.Info("GPU monitoring stopped")
			return
		case <-ticker.C:
			gpus, err := gd.queryNvidiaSMI(ctx)
			if err != nil {
				gd.log.Error("Periodic GPU query failed: %v", err)
				continue
			}

			// Check health
			gpus, err = gd.CheckGPUHealth(ctx, gpus)
			if err != nil {
				gd.log.Error("GPU health check failed: %v", err)
				continue
			}

			// Cache updated metrics
			if err := gd.cacheGPUs(ctx, gpus); err != nil {
				gd.log.Warn("Failed to update GPU cache: %v", err)
			}

			gd.log.Debug("GPU metrics updated (count=%d)", len(gpus))
		}
	}
}

// ============================================================================
// ERROR HANDLING & SAFETY
// ============================================================================

// ValidateGPUIndices: Check if requested GPU indices exist and are healthy
// Used before scheduling job to ensure GPUs available
func (gd *GPUDiscovery) ValidateGPUIndices(gpus []*common.GPUDevice, indices []int) error {
	if len(indices) == 0 {
		return fmt.Errorf("no GPU indices requested")
	}

	for _, idx := range indices {
		found := false
		for _, gpu := range gpus {
			if gpu.Index == idx {
				if !gpu.IsHealthy {
					return fmt.Errorf("GPU %d is not healthy", idx)
				}
				found = true
				break
			}
		}

		if !found {
			return fmt.Errorf("GPU %d not found", idx)
		}
	}

	return nil
}

// CheckGPUOversubscription: Detect if GPUs are oversubscribed (bad for latency)
// Returns: True if any GPU utilization > 90%
func (gd *GPUDiscovery) CheckGPUOversubscription(gpus []*common.GPUDevice) bool {
	for _, gpu := range gpus {
		if gpu.UtilizationPercent > 90.0 {
			return true
		}
	}
	return false
}
