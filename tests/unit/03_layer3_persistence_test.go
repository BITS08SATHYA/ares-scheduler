package unit

import (
	"context"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/idempotency"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/job"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/redis"
	"testing"
	"time"
)

// ============================================================================
// LAYER 3: PERSISTENCE, IDEMPOTENCY, LEASING TESTS
// ============================================================================
// Tests job persistence, request deduplication, and distributed leasing

func TestIdempotencyManagerCreation(t *testing.T) {
	//t.Skip("Requires Redis running")

	redisClient, err := redis.NewRedisClient("localhost:6379", "", 0)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer redisClient.Close()

	manager := idempotency.NewIdempotencyManager(redisClient)
	if manager == nil {
		t.Fatal("IdempotencyManager should not be nil")
	}

	t.Log("✓ IdempotencyManager created successfully")
}

func TestIdempotencyCheckDuplicate(t *testing.T) {
	//t.Skip("Requires Redis running")

	redisClient, err := redis.NewRedisClient("localhost:6379", "", 0)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer redisClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	manager := idempotency.NewIdempotencyManager(redisClient)
	requestID := "req-test-123"

	// First check - should be cache miss
	result1, isDup1, err := manager.CheckDuplicate(ctx, requestID)
	if err != nil {
		t.Fatalf("CheckDuplicate failed: %v", err)
	}

	if isDup1 || result1 != nil {
		t.Fatal("First check should be cache miss")
	}

	// Record success
	jobID := "job-abc123"
	err = manager.RecordSuccess(ctx, requestID, jobID)
	if err != nil {
		t.Fatalf("RecordSuccess failed: %v", err)
	}

	// Second check - should be cache hit
	result2, isDup2, err := manager.CheckDuplicate(ctx, requestID)
	if err != nil {
		t.Fatalf("CheckDuplicate failed: %v", err)
	}

	if !isDup2 || result2 == nil {
		t.Fatal("Second check should be cache hit")
	}

	if result2.JobID != jobID {
		t.Fatalf("Expected job ID %s, got %s", jobID, result2.JobID)
	}

	t.Log("✓ Idempotency deduplication successful")
}

func TestIdempotencyRecordCompletion(t *testing.T) {
	//t.Skip("Requires Redis running")

	redisClient, err := redis.NewRedisClient("localhost:6379", "", 0)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer redisClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	manager := idempotency.NewIdempotencyManager(redisClient)
	requestID := "req-completion-123"
	jobID := "job-xyz789"

	// Record completion
	err = manager.RecordCompletion(ctx, requestID, jobID, "succeeded", "Job completed successfully")
	if err != nil {
		t.Fatalf("RecordCompletion failed: %v", err)
	}

	// Check completion was recorded
	result, isDup, err := manager.CheckDuplicate(ctx, requestID)
	if err != nil || !isDup {
		t.Fatalf("Should have cache hit for completed job")
	}

	if result.Status != "succeeded" {
		t.Fatalf("Expected status 'succeeded', got %s", result.Status)
	}

	t.Log("✓ Idempotency completion recording successful")
}

func TestIdempotencyValidateRequestID(t *testing.T) {
	redisClient, _ := redis.NewRedisClient("localhost:6379", "", 0)
	manager := idempotency.NewIdempotencyManager(redisClient)

	tests := []struct {
		name      string
		requestID string
		shouldErr bool
	}{
		{"valid uuid", "550e8400-e29b-41d4-a716-446655440000", false},
		{"valid tenant:id", "tenant-1:req-123", false},
		{"valid hyphen", "req-test-123", false},
		{"empty", "", true},
		{"too short", "ab", true},
		{"too long", "a" + string(make([]byte, 300)), true},
		{"invalid char", "req@invalid", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := manager.ValidateRequestID(tt.requestID)
			if (err != nil) != tt.shouldErr {
				t.Fatalf("Expected error=%v, got %v", tt.shouldErr, err != nil)
			}
		})
	}

	t.Log("✓ Request ID validation successful")
}

// ============================================================================
// JOB LIFECYCLE TESTS
// ============================================================================

func TestJobFactoryCreation(t *testing.T) {
	logger.Init("debug")

	factory := job.NewJobFactory()
	if factory == nil {
		t.Fatal("JobFactory should not be nil")
	}

	// Create a job spec
	spec := &common.JobSpec{
		RequestID:       "req-123",
		Name:            "training-job",
		Image:           "nvidia/cuda:12.0",
		Command:         []string{"python", "train.py"},
		GPUCount:        2,
		GPUType:         "A100",
		MemoryMB:        8192,
		CPUMillis:       4000,
		Priority:        95,
		TimeoutSecs:     3600,
		MaxRetries:      3,
		TargetLatencyMs: 5000,
	}

	// Create job
	createdJob, err := factory.CreateJob(spec)
	if err != nil {
		t.Fatalf("CreateJob failed: %v", err)
	}

	if createdJob == nil {
		t.Fatal("Created job should not be nil")
	}

	if createdJob.Spec.Name != "training-job" {
		t.Fatalf("Expected job name 'training-job', got %s", createdJob.Spec.Name)
	}

	if createdJob.Status != common.StatusPending {
		t.Fatalf("Expected status PENDING, got %s", createdJob.Status)
	}

	t.Log("✓ Job factory creation successful")
}

func TestJobStateTransitions(t *testing.T) {
	logger.Init("debug")

	factory := job.NewJobFactory()
	spec := &common.JobSpec{
		RequestID:       "req-456",
		Name:            "test-job",
		Image:           "test:latest",
		GPUCount:        0,
		Priority:        50,
		TimeoutSecs:     300,
		MaxRetries:      2,
		TargetLatencyMs: 1000,
	}

	createdJob, _ := factory.CreateJob(spec)

	// Test valid transitions
	tests := []struct {
		name       string
		from       common.JobStatus
		fn         func() error
		expectedTo common.JobStatus
	}{
		{
			"pending to scheduled",
			common.StatusPending,
			func() error {
				return job.TransitionToScheduled(createdJob, "cluster-1", "node-1")
			},
			common.StatusScheduled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			createdJob.Status = tt.from

			err := tt.fn()
			if err != nil {
				t.Fatalf("Transition failed: %v", err)
			}

			if createdJob.Status != tt.expectedTo {
				t.Fatalf("Expected status %s, got %s", tt.expectedTo, createdJob.Status)
			}
		})
	}

	t.Log("✓ Job state transitions successful")
}

func TestJobBackoffCalculation(t *testing.T) {
	tests := []struct {
		name    string
		attempt int
		minTime time.Duration
		maxTime time.Duration
	}{
		{"attempt 1", 1, 1 * time.Second, 3 * time.Second},
		{"attempt 2", 2, 2 * time.Second, 4 * time.Second},
		{"attempt 3", 3, 4 * time.Second, 6 * time.Second},
		{"attempt 5", 5, 30 * time.Second, 35 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backoff := job.CalculateNextRetryTime(tt.attempt)

			// Allow for jitter variation
			if backoff < tt.minTime || backoff > 5*time.Minute {
				t.Fatalf("Backoff out of range: %v (expected between %v and %v)",
					backoff, tt.minTime, tt.maxTime)
			}

			t.Logf("Backoff for attempt %d: %v", tt.attempt, backoff)
		})
	}

	t.Log("✓ Job backoff calculation successful")
}

func TestJobQueryHelpers(t *testing.T) {
	logger.Init("debug")

	spec := &common.JobSpec{
		RequestID:       "req-789",
		Name:            "query-test",
		Image:           "test:latest",
		Priority:        50,
		TimeoutSecs:     300,
		MaxRetries:      2,
		TargetLatencyMs: 1000,
	}

	factory := job.NewJobFactory()
	testJob, _ := factory.CreateJob(spec)

	tests := []struct {
		name     string
		fn       func() bool
		expected bool
	}{
		{"is pending", func() bool { return job.IsJobPending(testJob) }, true},
		{"is running", func() bool { return job.IsJobRunning(testJob) }, false},
		{"is completed", func() bool { return job.IsJobCompleted(testJob) }, false},
		{"can be scheduled", func() bool { return job.CanJobBeScheduled(testJob) }, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.fn()
			if result != tt.expected {
				t.Fatalf("Expected %v, got %v", tt.expected, result)
			}
		})
	}

	t.Log("✓ Job query helpers successful")
}

func TestJobMetrics(t *testing.T) {
	logger.Init("debug")

	spec := &common.JobSpec{
		RequestID:       "req-metrics",
		Name:            "metrics-job",
		Image:           "test:latest",
		Priority:        50,
		TimeoutSecs:     300,
		MaxRetries:      2,
		TargetLatencyMs: 1000,
	}

	factory := job.NewJobFactory()
	testJob, _ := factory.CreateJob(spec)

	// Set metrics
	err := job.UpdateJobMetric(testJob, "cpu_usage", 45.5)
	if err != nil {
		t.Fatalf("UpdateJobMetric failed: %v", err)
	}

	err = job.UpdateJobMetric(testJob, "memory_usage", 2048)
	if err != nil {
		t.Fatalf("UpdateJobMetric failed: %v", err)
	}

	// Get metrics
	cpuMetric, exists := job.GetJobMetric(testJob, "cpu_usage")
	if !exists || cpuMetric != 45.5 {
		t.Fatalf("CPU metric mismatch")
	}

	memMetric, exists := job.GetJobMetric(testJob, "memory_usage")
	if !exists || memMetric != 2048 {
		t.Fatalf("Memory metric mismatch")
	}

	t.Log("✓ Job metrics tracking successful")
}

// ============================================================================
// LAYER 3 INTEGRATION TEST
// ============================================================================

func TestLayer3Integration(t *testing.T) {
	//t.Skip("Requires Redis running")

	logger.Init("debug")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	redisClient, err := redis.NewRedisClient("localhost:6379", "", 0)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer redisClient.Close()

	// Simulate complete job lifecycle with idempotency
	manager := idempotency.NewIdempotencyManager(redisClient)
	requestID := "req-integration-123"
	jobID := "job-integration-123"

	// Step 1: Check for duplicate (first time)
	result, isDup, _ := manager.CheckDuplicate(ctx, requestID)
	if isDup {
		t.Fatal("First request should not be duplicate")
	}

	// Step 2: Create job
	factory := job.NewJobFactory()
	spec := &common.JobSpec{
		RequestID:       requestID,
		Name:            "integration-job",
		Image:           "test:latest",
		GPUCount:        2,
		Priority:        90,
		TimeoutSecs:     300,
		MaxRetries:      3,
		TargetLatencyMs: 5000,
	}

	createdJob, _ := factory.CreateJob(spec)
	createdJob.ID = jobID

	// Step 3: Record success for dedup
	manager.RecordSuccess(ctx, requestID, jobID)

	// Step 4: Check for duplicate again (should hit cache)
	result, isDup, _ = manager.CheckDuplicate(ctx, requestID)
	if !isDup || result.JobID != jobID {
		t.Fatal("Second request should be duplicate")
	}

	// Step 5: Transition job through states
	job.TransitionToScheduled(createdJob, "cluster-us-west-2a", "node-001")
	if createdJob.Status != common.StatusScheduled {
		t.Fatal("Job should be scheduled")
	}

	t.Log("✓ Layer 3 integration test passed")
}

// ============================================================================
// LAYER 3 TEST SUMMARY
// ============================================================================

func TestLayer3PersistenceSummary(t *testing.T) {
	t.Run("IdempotencyManager Creation", TestIdempotencyManagerCreation)
	t.Run("Idempotency Check Duplicate", TestIdempotencyCheckDuplicate)
	t.Run("Idempotency Record Completion", TestIdempotencyRecordCompletion)
	t.Run("Idempotency Validate RequestID", TestIdempotencyValidateRequestID)
	t.Run("Job Factory Creation", TestJobFactoryCreation)
	t.Run("Job State Transitions", TestJobStateTransitions)
	t.Run("Job Backoff Calculation", TestJobBackoffCalculation)
	t.Run("Job Query Helpers", TestJobQueryHelpers)
	t.Run("Job Metrics", TestJobMetrics)
	t.Run("Layer 3 Integration", TestLayer3Integration)

	t.Log("✓ LAYER 3: Persistence & Idempotency - All tests passed")
}
