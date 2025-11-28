package gpu

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// GPU represents a physical GPU device
type GPU struct {
	Index     int    `json:"index"`
	UUID      string `json:"uuid"`
	Name      string `json:"name"`
	MemoryMB  int    `json:"memory_mb"`
	PCIBusID  string `json:"pci_bus_id"`
	Available bool   `json:"available"`
}

// NVLinkConnection represents connection between GPUs
type NVLinkConnection struct {
	FromGPU int    `json:"from_gpu"`
	ToGPU   int    `json:"to_gpu"`
	Type    string `json:"type"`  // "NVLink", "PCIe", "SYS"
	Links   int    `json:"links"` // Number of NVLinks (e.g., 12)
}

// Topology represents GPU interconnect topology
type Topology struct {
	GPUs        []*GPU             `json:"gpus"`
	Connections []NVLinkConnection `json:"connections"`
	WorkerID    string             `json:"worker_id"`
}

// Detector handles GPU discovery
type Detector struct{}

func NewDetector() *Detector {
	return &Detector{}
}

// DetectGPUs discovers all GPUs on the system
func (d *Detector) DetectGPUs(ctx context.Context) ([]*GPU, error) {
	//	Check for mock mode (for testing without GPUs)
	if os.Getenv("ARES_MOCK_GPU") == "true" {
		return d.getMockGPUs(), nil
	}

	cmd := exec.CommandContext(ctx, "nvidia-smi",
		"--query-gpu=index,uuid,name,memory.total,pci_bus_id,available",
		"--format=csv,noheader,nounits",
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("nvidia-smi failed: %w", err)
	}

	return d.parseGPUInfo(string(output))

}

func (d *Detector) parseGPUInfo(output string) ([]*GPU, error) {
	reader := csv.NewReader(strings.NewReader(output))
	reader.TrimLeadingSpace = true

	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %w", err)
	}

	var gpus []*GPU
	for _, record := range records {
		if len(record) < 5 {
			continue
		}

		index, _ := strconv.Atoi(strings.TrimSpace(record[0]))
		uuid := strings.TrimSpace(record[1])
		name := strings.TrimSpace(record[2])
		memoryMB, _ := strconv.Atoi(strings.TrimSpace(record[3]))
		pciBusID := strings.TrimSpace(record[4])

		gpus = append(gpus, &GPU{
			Index:     index,
			UUID:      uuid,
			Name:      name,
			MemoryMB:  memoryMB,
			PCIBusID:  pciBusID,
			Available: true,
		})
	}

	return gpus, nil
}

// DetectTopology detects GPU interconnect topology
func (d *Detector) DetectTopology(ctx context.Context) (*Topology, error) {
	//	Get GPUs
	gpus, err := d.DetectGPUs(ctx)
	if err != nil {
		return nil, err
	}

	//	Get topology matrix
	connections, err := d.detectNVLinkConnections(ctx, len(gpus))
	if err != nil {
		return nil, err
	}

	return &Topology{
		GPUs:        gpus,
		Connections: connections,
	}, nil
}

// detectNVLinkConnections parse nvidia-smi topo --matrix
func (d *Detector) detectNVLinkConnections(ctx context.Context, numGPUs int) ([]NVLinkConnection, error) {
	//	Check for mock mode
	if os.Getenv("ARES_MOCK_GPU") == "true" {
		return d.getMockTopology(numGPUs), nil
	}

	cmd := exec.CommandContext(ctx, "nvidia-smi", "topo", "--matrix")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("nvidia-smi topo failed: %w", err)
	}

	return d.parseTopologyMatrix(string(output), numGPUs)
}

func (d *Detector) parseTopologyMatrix(output string, numGPUs int) ([]NVLinkConnection, error) {
	lines := strings.Split(output, "\n")
	var connections []NVLinkConnection

	//	Find the matrix section (after "GPU" header row)
	matrixStart := -1
	for i, line := range lines {
		if strings.Contains(line, "GPU0") {
			matrixStart = i + 1
			break
		}
	}

	if matrixStart == -1 {
		return nil, fmt.Errorf("could not find topology matrix")
	}

	//	Parse matrix rows
	for i := 0; i < numGPUs && matrixStart+i < len(lines); i++ {
		line := lines[matrixStart+i]
		fields := strings.Fields(line)

		if len(fields) < numGPUs+1 {
			continue
		}

		//	Skip first field (GPU Label)
		for j := 0; j < numGPUs; j++ {
			if i == j {
				continue // skip self
			}

			connectionType := fields[j+1]
			conn := NVLinkConnection{
				FromGPU: i,
				ToGPU:   j,
				Type:    d.parseConnectionType(connectionType),
				Links:   d.parseNVLinkCount(connectionType),
			}
			connections = append(connections, conn)
		}
	}

	return connections, nil
}

func (d *Detector) parseConnectionType(s string) string {
	if strings.HasPrefix(s, "NV") {
		return "NVLink"
	} else if s == "SYS" {
		return "SYS"
	} else if s == "PHB" {
		return "PCIe"
	}
	return "Unknown"
}

func (d *Detector) parseNVLinkCount(s string) int {
	//	Extract number from "NV12" -> 12
	if strings.HasPrefix(s, "NV") {
		count, _ := strconv.Atoi(strings.TrimPrefix(s, "NV"))
		return count
	}
	return 0
}

// Mock data for testing without GPUs
func (d *Detector) getMockGPUs() []*GPU {
	return []*GPU{
		{Index: 0, UUID: "GPU-mock-0", Name: "Tesla V100", MemoryMB: 32510, PCIBusID: "0000:00:1e.0", Available: true},
		{Index: 1, UUID: "GPU-mock-1", Name: "Tesla V100", MemoryMB: 32510, PCIBusID: "0000:00:1f.0", Available: true},
		{Index: 2, UUID: "GPU-mock-2", Name: "Tesla V100", MemoryMB: 32510, PCIBusID: "0000:00:20.0", Available: true},
		{Index: 3, UUID: "GPU-mock-3", Name: "Tesla V100", MemoryMB: 32510, PCIBusID: "0000:00:21.0", Available: true},
	}
}

func (d *Detector) getMockTopology(numGPUs int) []NVLinkConnection {
	//	Mock : GPUs 0-1 and 2-3 are NVLink Connected
	return []NVLinkConnection{
		{FromGPU: 0, ToGPU: 1, Type: "NVLink", Links: 12},
		{FromGPU: 1, ToGPU: 0, Type: "NVLink", Links: 12},
		{FromGPU: 2, ToGPU: 3, Type: "NVLink", Links: 12},
		{FromGPU: 3, ToGPU: 2, Type: "NVLink", Links: 12},
		{FromGPU: 0, ToGPU: 2, Type: "SYS", Links: 0},
		{FromGPU: 0, ToGPU: 3, Type: "SYS", Links: 0},
		{FromGPU: 1, ToGPU: 2, Type: "SYS", Links: 0},
		{FromGPU: 1, ToGPU: 3, Type: "SYS", Links: 0},
	}
}

func (g *GPU) String() string {
	return fmt.Sprintf("GPU-%d: %s (%d MB)", g.Index, g.Name, g.MemoryMB)
}
