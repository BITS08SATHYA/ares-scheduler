package gpu

import (
	"context"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage"
	"github.com/NVIDIA/go-nvml/pkg/nvml"
	"time"
)

// GPU Toplogy DATA Structures

// GPU Topology represents GPU layout in a cluster
type GPUTopology struct {
	ClusterName string            `json:"cluster_name"`
	UpdateAt    time.Time         `json:"update_at"`
	GPUs        []GPUInfo         `json:"gpus"`
	Connections map[string]string `json:"connections"` // GPU0-GPU1: "NVLink", "PCIe", etc
}

// GPUInfo represents a single GPU
type GPUInfo struct {
	Index             int    `json:"index"`              // 0 , 1, 2
	Model             string `json:"model"`              // "A100, H100, T4"
	Memory            int64  `json:"memory_bytes"`       // Total Memory in bytes
	NUMANode          int    `json:"numa_node"`          // which NUMA node
	PCIBusID          string `json:"pci_bus_id"`         // For Identification
	ComputeCapability string `json:"compute_capability"` // e.g., "8.0"
}

// Topology Detector
// TopologyDetector queries GPU hardware for topology
type TopologyDetector struct {
	storage *storage.Client
}

// NewTopologyDetector creates detector
func NewTopologyDetector(st *storage.Client) *TopologyDetector {
	return &TopologyDetector{
		storage: st,
	}
}

// Detect Topology From Hardware
// DetectClusterTopology queries nvidia-smi and builds topology
func (td *TopologyDetector) DetectClusterTopology(ctx context.Context, clusterName string) (*GPUTopology, error) {
	/*
		Pattern:
		1. Initialize NVIDIA driver
		2. Query all GPUs
		3. Detect interconnects (NVLink vs PCIe)
		4. Build topology
		5. Store in etcd (permanent) + Redis cache (hot)
	*/

	//	Step 1: Initialize NVIDIA Management Library
	if err := nvml.Init(); err != nil {
		return nil, fmt.Errorf("failed to init NVML: %w", err)
	}
	defer nvml.Shutdown()

	//	Step 2: Get GPU Count
	count, err := nvml.DeviceGetCount()
	if err != nil {
		return nil, fmt.Errorf("failed to get device count: %w", err)
	}

	//	Step 3: Query each GPU
	topology := &GPUTopology{
		ClusterName: clusterName,
		UpdateAt:    time.Now(),
		GPUs:        make([]GPUInfo, count),
		Connections: make(map[string]string),
	}

	for i := 0; i < count; i++ {
		device, err := nvml.DeviceGetHandleByIndex(i)
		if err != nil {
			continue
		}

		info, err := td.queryGPU(ctx, device, i)
		if err != nil {
			continue
		}

		topology.GPUs[i] = info
	}

	//	Step 4: Detect interconnects
	if err := td.detectInterconnects(ctx, topology); err != nil {
		fmt.Printf("WARNING: Failed to detect interconnects: %v\n", err)
	}

	//	Step 5: Store topology
	if err := td.storeTopology(ctx, clusterName, topology); err != nil {
		return nil, err
	}

	return topology, nil

}

// queryGPU gets information about a single GPU
func (td *TopologyDetector) queryGPU(ctx context.Context, device nvml.Device, index int) (GPUInfo, error) {

	info := GPUInfo{
		Index: index,
	}

	//	Get GPU name
	name, _ := device.GetName()
	info.Model = name

	//	Get total Memory
	memInfo, _ := device.GetMemoryInfo()
	info.Memory = int64(memInfo.Total)

	//	Get PCI bus ID
	busID, _ := device.GetPciInfo()
	info.PCIBusID = busID.String()

	//	Get NUMA node (requires special query)
	// In real implementations, use nvidia-smi -i {index} --query-gpu=gpu_bus_id

	return info, nil
}

// detectInterconnects detects GPU-to-GPU connections
func (td *TopologyDetector) detectInterconnects(ctx context.Context, topology *GPUTopology) error {
	/*
		Detect:
		- NVLink (100+ GB/s, only on A100/H100)
		- PCIe Gen4 (64 GB/s)
		- PCIe Gen3 (16 GB/s)

		Key insight: NVLink connections can be 10x faster for data transfer
		This affects job co-location strategy
	*/

	for i := 0; i < len(topology.GPUs); i++ {
		for j := i + 1; j < len(topology.GPUs); j++ {
			connectionType := td.detectConnection(i, j, topology)
			key := fmt.Sprintf("GPU%d-GPU%d", i, j)
			topology.Connections[key] = connectionType
		}
	}

	return nil
}

// detectConnection determines connection type between two GPUs
func (td *TopologyDetector) detectConnection(i, j int, topology *GPUTopology) string {
	//	In the production, Use Nvidia-smi topo -m
	//	For now: Heuristic based on PCI topology

	//	If same NUMA node: likely NVLink or close PCIe
	if topology.GPUs[i].NUMANode == topology.GPUs[j].NUMANode {
		//	Check if GPU supports NVLink (A100, H100)
		if topology.GPUs[i].Model == "A100" || topology.GPUs[i].Model == "H100" {
			return "NVLink"
		}
		return "PCIe-Gen4"
	}
	//	Different NUMA nodes: cross-socket connection (slower)
	return "PCIe-Gen3"
}

// Store Topology
// storeTopology writes topology to etcd + Redis cache
func (td *TopologyDetector) storeTopology(ctx context.Context,
	clusterName string, topology *GPUTopology) error {
	/*
		Pattern:
		- etcd: Permanent storage (survives restarts)
		- Redis: Hot cache (TTL = 5 minutes)

		Configuration doesn't change often, so 5min cache is safe.
	*/

//	Write to etcd (source of truth)
	etcdKey := fmt.Sprintf("/ares/clusters/%s/gpu-topology", clusterName)
	if err := td.storage.PutEtcd(ctx, etcdKey, topology); err != nil {
		return fmt.Errorf("failed to store topology in etcd: %w", err)
	}

//	Cache in redis for 5 minutes
	cacheKey := fmt.Sprintf("topology:%s", clusterName)
	if err := td.storage.setRedis(ctx, cacheKey, topology, 5 * time.Minute); err != nil {
		fmt.Printf("WARNING: Failed to cache topology in Redis: %v\n", err)
	}

	return nil
}

// Retrieve Topology
// GetClusterTopology retrieves topology from cache/etcd
// slow path: etcd query
func (td *TopologyDetector) GetClusterTopology(ctx context.Context, clusterName string ) (*GPUTopology, error) {
	cacheKey := fmt.Sprintf("topology:%s", clusterName)

//	FastPath: Check Redis cache
	var topology GPUTopology
	if err := td.storage.GetEtcd(ctx, cacheKey, &topology); err != nil {
		return &topology, nil
	}

//	Slow path: Query Etcd
	etcdKey := fmt.Sprintf("/ares/clusters/%s/gpu-topology", clusterName)
	if err := td.storage.GetEtcd(ctx, etcdKey, &topology); err != nil {
		return nil, fmt.Errorf("topology not found for cluster %s", clusterName)
	}

	// Refresh Cache
	td.storage.setRedis(ctx, cacheKey, &topology, 5 * time.Minute)


	return &topology, nil
}

// Topology-AWare scheduling
// ScorePlacementByTopology scores a GPU pair for co-location
// Higher score = better for placing together
func (td *TopologyDetector) ScorePlacementByTopology(ctx context.Context, clusterName string, gpu1 int , gpu2 int) (float64, error) {
	/*
		Scoring:
		- NVLink connection: 1.0 (ideal for distributed traning)
		- PCIe Gen4: 0.5
		- PCIe Gen3: 0.2
		- No connection: 0.0
	*/

	topology, err := td.GetClusterTopology(ctx, clusterName)
	if err != nil {
		return 0, err
	}

//	Check if connection exists
	key := fmt.Sprintf("GPU%d-GPU%d", gpu1, gpu2)
	if reverse := fmt.Sprintf("GPU%d-GPU%d", gpu2, gpu1); _, exists := topology.Connections[key]; !exists {
		if _, exists := topology.Connections[rerverse]; !exists {
			return 0, nil // No direct connection
		}
		key = reverse
	}

//	Score based on Connection type
	connectionType := topology.Connections[key]
	switch connectionType {
	case "NVLink":
		return 1.0, nil
	case "PCIe-Gen4":
		return 0.5, nil
	case "PCIe-Gen3":
		return 0.2, nil
	default:
		return 0, nil
	}
}


// FindBestGPUPlacement finds optimal GPUs for a distributed job
func (td *TopologyDetector) FindBestGPUPlacement(ctx context.Context, clusterName string, requiredGPUCount int)([]int, error) {
	/*
		Algorithm:
		1. Get Cluster Topology
		2. Find GPUs with best interconencts
		3. Return GPU indicces

	Example: If requiredGPUCount=4 and we have 8 GPUs:
		- Find 4 GPUs with NVLink connections (best for distributed training)
		- Return [0, 1, 2, 3] (assuming they're in an NVLink ring)

	*/

	topology, err := td.GetClusterTopology(ctx, clusterName)
	if err != nil {
		return nil, err
	}

	if requiredGPUCount > len(topology.GPUs) {
		return nil, fmt.Errorf("cluster has %d GPUs, required %d", len(topology.GPUs), requiredGPUCount)
	}

//	Simple algorithm: Find first N GPUs with best interconnects
// In Production: Use more sophisticated algorithm (e.g., graph partitioning)
	selectedGPUs := make([]int, requiredGPUCount)
	for i := 0; i < requiredGPUCount; i++ {
		selectedGPUs[i] = i
	}

	return selectedGPUs, nil
}

// Monitoring
// RecordTopologyQuery increments query counter

func (td *TopologyDetector) RecordTopologyQuery(ctx context.Context) error {
	_, err := td.storage.IncrCounter(ctx, "metrics:topology-queries")
	return err
}