package main

import (
	"context"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/gpu"
	"log"
	"strings"
)

func main() {

	log.Println("Ares GPU Topology Test")
	log.Println(strings.Repeat("=", 60))

	detector := gpu.NewDetector()
	ctx := context.Background()

	//	Test 1: Detect GPUs
	log.Println("\n Test 1: GPU Detection")
	gpus, err := detector.DetectGPUs(ctx)
	if err != nil {
		log.Fatalf("Failed: %v", err)
	}
	log.Println("Found %d GPUs", len(gpus))
	for _, g := range gpus {
		fmt.Printf(" - %s\n", g.String())
	}

	//	Test 2: Detect Topology
	log.Println("\n Test 2: Topology Detection")
	topology, err := detector.DetectTopology(ctx)
	if err != nil {
		log.Fatalf("Failed: %v", err)
	}
	log.Printf("Detected %d connections ", len(topology.Connections))

	nvlinkCount := 0
	for _, conn := range topology.Connections {
		if conn.Type == "NVLink" {
			fmt.Printf(" - GPU-%d <-> GPU-%d: NVLink (%d links)\n", conn.FromGPU, conn.ToGPU, conn.Links)
			nvlinkCount++
		}
	}
	log.Printf("Total NVLink Connections: %d", nvlinkCount)

	//	Test 3: Placement scoring
	log.Println("\n Test 3: Placement Scoring")
	scorer := gpu.NewPlacementScorer(topology)

	testCases := []int{2, 4}
	for _, numGPUs := range testCases {
		placement, err := scorer.FindBestPlacement(numGPUs)
		if err != nil {
			log.Printf("Failed to find placement for %d GPUs: %v", numGPUs, err)
			continue
		}

		log.Printf("\n Best Placement for %d GPUs: %v", numGPUs)
		log.Printf(" GPUs: %v ", placement.GPUIndices)
		log.Printf(" Score: %.1f", placement.Score)
		log.Printf(" Reasoning: %s", placement.Reasoning)
	}

	log.Println("\n" + strings.Repeat("=", 60))
	log.Println("All tests passed!")
}
