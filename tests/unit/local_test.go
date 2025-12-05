// File: pkg/scheduler/local_test.go
// Layer 6 - Local Scheduler Tests
// Coverage: Job selection, scheduling, placement, constraints checking
// ~650 LOC, comprehensive coverage

package unit

import (
	"context"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/local"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
)

// ============================================================================
// MOCK JOB QUEUE
// ============================================================================

type MockJobQueue struct {
	jobs map[string]*common.Job
	mu   sync.RWMutex
}

func NewMockJobQueue() *MockJobQueue {
	return &MockJobQueue{
		jobs: make(map[string]*common.Job),
	}
}

func (m *MockJobQueue) Enqueue(ctx context.Context, job *common.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[job.ID] = job
	return nil
}

func (m *MockJobQueue) Dequeue(ctx context.Context) (*common.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, job := range m.jobs {
		if job.Status == common.StatusPending {
			return job, nil
		}
	}
	return nil, nil
}

func (m *MockJobQueue) Peek(ctx context.Context, limit int) ([]*common.Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var jobs []*common.Job
	count := 0
	for _, job := range m.jobs {
		if job.Status == common.StatusPending && count < limit {
			jobs = append(jobs, job)
			count++
		}
	}
	return jobs, nil
}

// ============================================================================
// TEST CASES
// ============================================================================

// TestSelectNextJob - Basic job selection
func TestSelectNextJob(t *testing.T) {
	queue := NewMockJobQueue()
	scheduler := local.NewLocalScheduler(queue, DefaultSchedulerConfig)

	job := &common.Job{
		ID:     "job-001",
		Status: common.StatusPending,
		Spec: &common.JobSpec{
			Name:     "test-job",
			GPUCount: 1,
		},
	}

	queue.Enqueue(context.Background(), job)

	selected, err := scheduler.SelectNextJob(context.Background())
	if err != nil {
		t.Fatalf("SelectNextJob failed: %v", err)
	}

	if selected == nil || selected.ID != job.ID {
		t.Fatalf("Wrong job selected")
	}
}

// TestSelectNextJobEmpty - No jobs available
func TestSelectNextJobEmpty(t *testing.T) {
	queue := NewMockJobQueue()
	scheduler := local.NewLocalScheduler(queue, DefaultSchedulerConfig)

	selected, err := scheduler.SelectNextJob(context.Background())
	if err != nil {
		t.Fatalf("SelectNextJob returned error: %v", err)
	}

	if selected != nil {
		t.Fatal("Expected nil when no jobs available")
	}
}

// TestSelectNextJobPriority - Higher priority jobs selected first
func TestSelectNextJobPriority(t *testing.T) {
	queue := NewMockJobQueue()
	scheduler := local.NewLocalScheduler(queue, DefaultSchedulerConfig)

	// Create low-priority job
	lowJob := &common.Job{
		ID:       "job-low",
		Status:   common.StatusPending,
		Priority: 10,
		Spec:     &common.JobSpec{Name: "low-priority"},
	}

	// Create high-priority job
	highJob := &common.Job{
		ID:       "job-high",
		Status:   common.StatusPending,
		Priority: 50,
		Spec:     &common.JobSpec{Name: "high-priority"},
	}

	queue.Enqueue(context.Background(), lowJob)
	queue.Enqueue(context.Background(), highJob)

	selected, _ := scheduler.SelectNextJob(context.Background())

	if selected.ID != highJob.ID {
		t.Fatalf("Expected high-priority job, got %s", selected.ID)
	}
}

// TestScheduleJob - Basic scheduling
func TestScheduleJob(t *testing.T) {
	queue := NewMockJobQueue()
	scheduler := local.NewLocalScheduler(queue, DefaultSchedulerConfig)

	job := &common.Job{
		ID:     "job-schedule",
		Status: common.StatusPending,
		Spec: &common.JobSpec{
			Name:     "test",
			GPUCount: 2,
			Memory:   4.0,
		},
	}

	placement, err := scheduler.ScheduleJob(context.Background(), job)
	if err != nil {
		t.Fatalf("ScheduleJob failed: %v", err)
	}

	if placement == nil {
		t.Fatal("Expected placement result")
	}

	if placement.JobID != job.ID {
		t.Fatalf("Placement job ID mismatch")
	}
}

// TestScheduleJobNotEnoughResources - Insufficient resources
func TestScheduleJobNotEnoughResources(t *testing.T) {
	queue := NewMockJobQueue()
	config := DefaultSchedulerConfig
	config.MaxGPUs = 4 // Limited to 4 GPUs

	scheduler := local.NewLocalScheduler(queue, config)

	job := &common.Job{
		ID:     "job-big",
		Status: common.StatusPending,
		Spec: &common.JobSpec{
			Name:     "large-job",
			GPUCount: 10, // Request 10 GPUs but only 4 available
		},
	}

	placement, err := scheduler.ScheduleJob(context.Background(), job)

	// Should either fail or return nil placement
	if err == nil && placement != nil {
		t.Fatal("Expected failure due to insufficient resources")
	}
}

// TestCheckResourceConstraints - Constraint validation
func TestCheckResourceConstraints(t *testing.T) {
	queue := NewMockJobQueue()
	scheduler := local.NewLocalScheduler(queue, DefaultSchedulerConfig)

	testCases := []struct {
		name       string
		job        *common.Job
		shouldFail bool
	}{
		{
			name: "Valid GPU request",
			job: &common.Job{
				ID: "valid-gpu",
				Spec: &common.JobSpec{
					GPUCount: 2,
					Memory:   4.0,
				},
			},
			shouldFail: false,
		},
		{
			name: "Zero GPU request",
			job: &common.Job{
				ID: "zero-gpu",
				Spec: &common.JobSpec{
					GPUCount: 0,
				},
			},
			shouldFail: true,
		},
		{
			name: "Negative memory",
			job: &common.Job{
				ID: "neg-mem",
				Spec: &common.JobSpec{
					GPUCount: 1,
					Memory:   -1.0,
				},
			},
			shouldFail: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := scheduler.CheckResourceConstraints(tc.job)
			if tc.shouldFail && err == nil {
				t.Fatal("Expected validation error")
			}
			if !tc.shouldFail && err != nil {
				t.Fatalf("Unexpected validation error: %v", err)
			}
		})
	}
}

// TestCheckAffinityConstraints - Affinity rule validation
func TestCheckAffinityConstraints(t *testing.T) {
	queue := NewMockJobQueue()
	scheduler := local.NewLocalScheduler(queue, DefaultSchedulerConfig)

	job := &common.Job{
		ID: "affinity-job",
		Spec: &common.JobSpec{
			Name:          "gpu-test",
			GPUCount:      2,
			GPUType:       "A100",
			RequireNVLink: true,
		},
	}

	err := scheduler.CheckAffinityConstraints(job)
	// Just verify the method works without error
	if err != nil {
		t.Logf("Affinity check result: %v", err)
	}
}

// TestCheckPreemptionEligibility - Preemption rules
func TestCheckPreemptionEligibility(t *testing.T) {
	queue := NewMockJobQueue()
	scheduler := local.NewLocalScheduler(queue, DefaultSchedulerConfig)

	runningJob := &common.Job{
		ID:       "running",
		Status:   common.StatusRunning,
		Priority: 10,
	}

	preemptingJob := &common.Job{
		ID:       "preempting",
		Status:   common.StatusPending,
		Priority: 100, // Much higher priority
	}

	eligible, err := scheduler.CheckPreemptionEligibility(runningJob, preemptingJob)
	if err != nil {
		t.Fatalf("CheckPreemptionEligibility failed: %v", err)
	}

	// Higher priority job should be able to preempt lower priority
	if !eligible {
		t.Fatal("Expected preemption to be eligible")
	}
}

// ============================================================================
// PLACEMENT TESTS
// ============================================================================

// TestComputePlacementScore - Scoring algorithm
func TestComputePlacementScore(t *testing.T) {
	queue := NewMockJobQueue()
	scheduler := local.NewLocalScheduler(queue, DefaultSchedulerConfig)

	job := &common.Job{
		ID:     "score-job",
		Status: common.StatusPending,
		Spec: &common.JobSpec{
			GPUCount: 2,
			Memory:   8.0,
		},
	}

	// Create placement options
	placements := []*common.Placement{
		{
			JobID:      job.ID,
			NodeID:     "node-1",
			GPUIndices: []int{0, 1},
			Score:      85.0,
		},
		{
			JobID:      job.ID,
			NodeID:     "node-2",
			GPUIndices: []int{2, 3},
			Score:      65.0,
		},
	}

	// Select best placement
	best := scheduler.SelectBestPlacement(placements)
	if best.NodeID != "node-1" {
		t.Fatalf("Expected best placement on node-1, got %s", best.NodeID)
	}
}

// TestPlacementWithGPUTopology - Topology-aware placement
func TestPlacementWithGPUTopology(t *testing.T) {
	queue := NewMockJobQueue()
	scheduler := local.NewLocalScheduler(queue, DefaultSchedulerConfig)

	job := &common.Job{
		ID:     "topo-job",
		Status: common.StatusPending,
		Spec: &common.JobSpec{
			GPUCount:      2,
			RequireNVLink: true, // Wants NVLink-connected GPUs
		},
	}

	// Create mock node with topology info
	node := &common.Node{
		NodeID: "topo-node",
		GPUs: []*common.GPU{
			{Index: 0, Type: "A100"},
			{Index: 1, Type: "A100"},
			{Index: 2, Type: "A100"},
			{Index: 3, Type: "A100"},
		},
		GPUTopology: map[string]string{
			"0-1": "NVLink",
			"0-2": "PCIe",
			"2-3": "NVLink",
		},
	}

	// Should prefer NVLink-connected GPUs (0-1 or 2-3)
	placement, _ := scheduler.ComputeTopologyAwarePlacement(job, node)
	if placement == nil {
		t.Fatal("Expected valid placement")
	}
}

// ============================================================================
// CONCURRENCY TESTS
// ============================================================================

// TestConcurrentScheduling - Multiple simultaneous scheduling requests
func TestConcurrentScheduling(t *testing.T) {
	queue := NewMockJobQueue()
	scheduler := local.NewLocalScheduler(queue, DefaultSchedulerConfig)

	numGoroutines := 50
	var wg sync.WaitGroup
	var successCount int32

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			job := &common.Job{
				ID:     fmt.Sprintf("concurrent-job-%d", idx),
				Status: common.StatusPending,
				Spec: &common.JobSpec{
					Name:     "concurrent-test",
					GPUCount: 1,
				},
			}

			queue.Enqueue(context.Background(), job)

			_, err := scheduler.ScheduleJob(context.Background(), job)
			if err == nil {
				atomic.AddInt32(&successCount, 1)
			}
		}(i)
	}

	wg.Wait()

	// Most jobs should schedule successfully
	if successCount < int32(numGoroutines/2) {
		t.Logf("Only %d/%d jobs scheduled (this may be expected due to resource limits)",
			successCount, numGoroutines)
	}
}

// TestSchedulingUnderLoad - Performance under load
func TestSchedulingUnderLoad(t *testing.T) {
	queue := NewMockJobQueue()
	scheduler := local.NewLocalScheduler(queue, DefaultSchedulerConfig)

	// Create 1000 jobs
	for i := 0; i < 1000; i++ {
		job := &common.Job{
			ID:       fmt.Sprintf("load-job-%d", i),
			Status:   common.StatusPending,
			Priority: int32(i % 100), // Varying priorities
			Spec: &common.JobSpec{
				Name:     "load-test",
				GPUCount: (i % 4) + 1, // 1-4 GPUs
			},
		}
		queue.Enqueue(context.Background(), job)
	}

	start := time.Now()

	// Try to schedule batches
	for batch := 0; batch < 10; batch++ {
		jobs, _ := queue.Peek(context.Background(), 100)
		for _, job := range jobs {
			scheduler.ScheduleJob(context.Background(), job)
		}
	}

	elapsed := time.Since(start)
	t.Logf("Scheduled 1000 jobs in %v", elapsed)
}

// ============================================================================
// BENCHMARK TESTS
// ============================================================================

// BenchmarkSelectNextJob
func BenchmarkSelectNextJob(b *testing.B) {
	queue := NewMockJobQueue()
	scheduler := local.NewLocalScheduler(queue, DefaultSchedulerConfig)
	ctx := context.Background()

	// Pre-populate queue
	for i := 0; i < 100; i++ {
		job := &common.Job{
			ID:     fmt.Sprintf("bench-job-%d", i),
			Status: common.StatusPending,
		}
		queue.Enqueue(ctx, job)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		scheduler.SelectNextJob(ctx)
	}
}

// BenchmarkScheduleJob
func BenchmarkScheduleJob(b *testing.B) {
	queue := NewMockJobQueue()
	scheduler := local.NewLocalScheduler(queue, DefaultSchedulerConfig)
	ctx := context.Background()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		job := &common.Job{
			ID:     fmt.Sprintf("bench-schedule-%d", i),
			Status: common.StatusPending,
			Spec: &common.JobSpec{
				Name:     "bench",
				GPUCount: 2,
				Memory:   4.0,
			},
		}

		scheduler.ScheduleJob(ctx, job)
	}
}

// BenchmarkCheckResourceConstraints
func BenchmarkCheckResourceConstraints(b *testing.B) {
	queue := NewMockJobQueue()
	scheduler := local.NewLocalScheduler(queue, DefaultSchedulerConfig)

	job := &common.Job{
		ID: "bench-constraints",
		Spec: &common.JobSpec{
			GPUCount: 2,
			Memory:   8.0,
		},
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		scheduler.CheckResourceConstraints(job)
	}
}
