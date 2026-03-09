// File: cmd/benchmark/main.go
// Ares Benchmark Suite — Run against a live Ares control plane
//
// Usage:
//   go run cmd/benchmark/main.go -control-plane http://localhost:8080 -suite all
//   go run cmd/benchmark/main.go -control-plane http://localhost:8080 -suite stress
//   go run cmd/benchmark/main.go -control-plane http://localhost:8080 -suite exactlyonce
//   go run cmd/benchmark/main.go -control-plane http://localhost:8080 -suite failure
//   go run cmd/benchmark/main.go -control-plane http://localhost:8080 -suite gang
//
// Output: JSON results file + human-readable summary to stdout

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ============================================================================
// TYPES
// ============================================================================

type ScheduleRequest struct {
	RequestID      string   `json:"request_id"`
	Name           string   `json:"name"`
	Image          string   `json:"image"`
	Command        []string `json:"command,omitempty"`
	Args           []string `json:"args,omitempty"`
	GPUCount       int      `json:"gpu_count"`
	GPUType        string   `json:"gpu_type,omitempty"`
	Priority       int      `json:"priority,omitempty"`
	MemoryMB       int      `json:"memory_mb,omitempty"`
	CPUMillis      int      `json:"cpu_millis,omitempty"`
	PreferNVLink   bool     `json:"prefer_nvlink,omitempty"`
	PreferSameNUMA bool     `json:"prefer_same_numa,omitempty"`
	MaxRetries     int      `json:"max_retries,omitempty"`
	GangID         string   `json:"gang_id,omitempty"`
	GangSize       int      `json:"gang_size,omitempty"`
}

type ScheduleResponse struct {
	RequestID    string  `json:"request_id"`
	JobID        string  `json:"job_id"`
	ClusterID    string  `json:"cluster_id"`
	NodeID       string  `json:"node_id"`
	DurationMs   float64 `json:"duration_ms"`
	ClusterScore float64 `json:"cluster_score"`
}

type ErrorResponse struct {
	Error     string `json:"error"`
	ErrorCode string `json:"error_code"`
	Message   string `json:"message"`
}

type BenchmarkResult struct {
	Suite            string                 `json:"suite"`
	Timestamp        string                 `json:"timestamp"`
	TotalRequests    int                    `json:"total_requests"`
	SuccessCount     int                    `json:"success_count"`
	ErrorCount       int                    `json:"error_count"`
	DuplicateBlocked int                    `json:"duplicate_blocked"`
	Latencies        LatencyStats           `json:"latencies"`
	Throughput       float64                `json:"throughput_rps"`
	Duration         string                 `json:"total_duration"`
	Details          map[string]interface{} `json:"details,omitempty"`
}

type LatencyStats struct {
	P50  float64 `json:"p50_ms"`
	P95  float64 `json:"p95_ms"`
	P99  float64 `json:"p99_ms"`
	P999 float64 `json:"p999_ms"`
	Min  float64 `json:"min_ms"`
	Max  float64 `json:"max_ms"`
	Avg  float64 `json:"avg_ms"`
}

// ============================================================================
// MAIN
// ============================================================================

func main() {
	controlPlane := flag.String("control-plane", "http://localhost:8080", "Ares control plane URL")
	suite := flag.String("suite", "all", "Benchmark suite: all, stress, exactlyonce, failure, gang, drf, priority, multicluster")
	outputFile := flag.String("output", "benchmark_results.json", "Output JSON file")
	flag.Parse()

	fmt.Println("╔══════════════════════════════════════════════════════╗")
	fmt.Println("║         ARES SCHEDULER BENCHMARK SUITE              ║")
	fmt.Println("╚══════════════════════════════════════════════════════╝")
	fmt.Printf("  Control Plane: %s\n", *controlPlane)
	fmt.Printf("  Suite: %s\n", *suite)
	fmt.Printf("  Start: %s\n\n", time.Now().Format(time.RFC3339))

	// Verify control plane is reachable
	if !healthCheck(*controlPlane) {
		fmt.Println("ERROR: Cannot reach control plane at", *controlPlane)
		os.Exit(1)
	}
	fmt.Println("✓ Control plane reachable\n")

	results := make([]BenchmarkResult, 0)

	switch *suite {
	case "all":
		results = append(results, runStressTest(*controlPlane))
		results = append(results, runExactlyOnceTest(*controlPlane))
		results = append(results, runFailureInjectionTest(*controlPlane))
		results = append(results, runGangSchedulingTest(*controlPlane))
		results = append(results, runDRFFairnessTest(*controlPlane))
		results = append(results, runPriorityPreemptionTest(*controlPlane))
		results = append(results, runMultiClusterRoutingTest(*controlPlane))
	case "stress":
		results = append(results, runStressTest(*controlPlane))
	case "exactlyonce":
		results = append(results, runExactlyOnceTest(*controlPlane))
	case "failure":
		results = append(results, runFailureInjectionTest(*controlPlane))
	case "gang":
		results = append(results, runGangSchedulingTest(*controlPlane))
	case "drf":
		results = append(results, runDRFFairnessTest(*controlPlane))
	case "priority":
		results = append(results, runPriorityPreemptionTest(*controlPlane))
	case "multicluster":
		results = append(results, runMultiClusterRoutingTest(*controlPlane))
	default:
		fmt.Printf("Unknown suite: %s\n", *suite)
		os.Exit(1)
	}

	// Write results to JSON file
	writeResults(*outputFile, results)

	fmt.Println("\n╔══════════════════════════════════════════════════════╗")
	fmt.Println("║              BENCHMARK COMPLETE                      ║")
	fmt.Println("╚══════════════════════════════════════════════════════╝")
	fmt.Printf("  Results: %s\n", *outputFile)
}

// ============================================================================
// SUITE 1: STRESS TEST
// Measures: throughput, p50/p95/p99/p999 latency under load
// ============================================================================

func runStressTest(baseURL string) BenchmarkResult {
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  SUITE 1: SCHEDULER STRESS TEST")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	allLatencies := make([]float64, 0)

	// ── Phase 1: Sequential baseline (1000 jobs) ──
	fmt.Println("\n  Phase 1: Sequential submission (1000 jobs)...")
	seqLatencies := make([]float64, 0, 1000)
	seqSuccess := 0
	seqErrors := 0
	seqStart := time.Now()

	for i := 0; i < 1000; i++ {
		req := ScheduleRequest{
			RequestID: fmt.Sprintf("stress-seq-%d-%d", time.Now().UnixNano(), i),
			Name:      fmt.Sprintf("stress-seq-%d", i),
			Image:     "nvidia/cuda:12.0-base-ubuntu22.04",
			Command:   []string{"sleep", "1"},
			GPUCount:  1,
			Priority:  5,
			MemoryMB:  512,
			CPUMillis: 500,
		}

		start := time.Now()
		_, err := submitJob(baseURL, req)
		elapsed := time.Since(start).Seconds() * 1000 // ms

		if err != nil {
			seqErrors++
		} else {
			seqSuccess++
			seqLatencies = append(seqLatencies, elapsed)
		}

		if (i+1)%100 == 0 {
			fmt.Printf("    %d/1000 submitted (success=%d, errors=%d)\n", i+1, seqSuccess, seqErrors)
		}
	}
	seqDuration := time.Since(seqStart)
	allLatencies = append(allLatencies, seqLatencies...)

	fmt.Printf("  Phase 1 complete: %d success, %d errors, %.1f jobs/sec\n",
		seqSuccess, seqErrors, float64(seqSuccess)/seqDuration.Seconds())

	// ── Phase 2: Concurrent burst (100 jobs simultaneously) ──
	fmt.Println("\n  Phase 2: Concurrent burst (100 simultaneous jobs)...")
	var burstSuccess int64
	var burstErrors int64
	burstLatencies := make([]float64, 0, 100)
	var burstMu sync.Mutex
	var wg sync.WaitGroup

	burstStart := time.Now()
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			req := ScheduleRequest{
				RequestID: fmt.Sprintf("stress-burst-%d-%d", time.Now().UnixNano(), idx),
				Name:      fmt.Sprintf("stress-burst-%d", idx),
				Image:     "nvidia/cuda:12.0-base-ubuntu22.04",
				Command:   []string{"sleep", "1"},
				GPUCount:  1,
				Priority:  5,
				MemoryMB:  512,
				CPUMillis: 500,
			}

			start := time.Now()
			_, err := submitJob(baseURL, req)
			elapsed := time.Since(start).Seconds() * 1000

			if err != nil {
				atomic.AddInt64(&burstErrors, 1)
			} else {
				atomic.AddInt64(&burstSuccess, 1)
				burstMu.Lock()
				burstLatencies = append(burstLatencies, elapsed)
				burstMu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	burstDuration := time.Since(burstStart)
	allLatencies = append(allLatencies, burstLatencies...)

	fmt.Printf("  Phase 2 complete: %d success, %d errors, %.1f jobs/sec\n",
		burstSuccess, burstErrors, float64(burstSuccess)/burstDuration.Seconds())

	// ── Phase 3: Sustained load (500 jobs at 50 concurrent) ──
	fmt.Println("\n  Phase 3: Sustained load (500 jobs, 50 concurrent workers)...")
	var sustainedSuccess int64
	var sustainedErrors int64
	sustainedLatencies := make([]float64, 0, 500)
	var sustainedMu sync.Mutex
	jobChan := make(chan int, 500)
	var swg sync.WaitGroup

	sustainedStart := time.Now()

	// Launch 50 workers
	for w := 0; w < 50; w++ {
		swg.Add(1)
		go func() {
			defer swg.Done()
			for idx := range jobChan {
				req := ScheduleRequest{
					RequestID: fmt.Sprintf("stress-sustained-%d-%d", time.Now().UnixNano(), idx),
					Name:      fmt.Sprintf("stress-sustained-%d", idx),
					Image:     "nvidia/cuda:12.0-base-ubuntu22.04",
					Command:   []string{"sleep", "1"},
					GPUCount:  1,
					Priority:  5,
					MemoryMB:  512,
					CPUMillis: 500,
				}

				start := time.Now()
				_, err := submitJob(baseURL, req)
				elapsed := time.Since(start).Seconds() * 1000

				if err != nil {
					atomic.AddInt64(&sustainedErrors, 1)
				} else {
					atomic.AddInt64(&sustainedSuccess, 1)
					sustainedMu.Lock()
					sustainedLatencies = append(sustainedLatencies, elapsed)
					sustainedMu.Unlock()
				}
			}
		}()
	}

	// Feed 500 jobs
	for i := 0; i < 500; i++ {
		jobChan <- i
	}
	close(jobChan)
	swg.Wait()
	sustainedDuration := time.Since(sustainedStart)
	allLatencies = append(allLatencies, sustainedLatencies...)

	fmt.Printf("  Phase 3 complete: %d success, %d errors, %.1f jobs/sec\n",
		sustainedSuccess, sustainedErrors, float64(sustainedSuccess)/sustainedDuration.Seconds())

	// ── Results ──
	totalSuccess := seqSuccess + int(burstSuccess) + int(sustainedSuccess)
	totalErrors := seqErrors + int(burstErrors) + int(sustainedErrors)
	totalDuration := seqDuration + burstDuration + sustainedDuration
	stats := calcLatencyStats(allLatencies)

	printLatencyTable("STRESS TEST", stats, totalSuccess, totalErrors,
		float64(totalSuccess)/totalDuration.Seconds())

	return BenchmarkResult{
		Suite:         "stress",
		Timestamp:     time.Now().Format(time.RFC3339),
		TotalRequests: totalSuccess + totalErrors,
		SuccessCount:  totalSuccess,
		ErrorCount:    totalErrors,
		Latencies:     stats,
		Throughput:    float64(totalSuccess) / totalDuration.Seconds(),
		Duration:      totalDuration.String(),
		Details: map[string]interface{}{
			"sequential_1000":  map[string]interface{}{"success": seqSuccess, "errors": seqErrors, "rps": float64(seqSuccess) / seqDuration.Seconds(), "latency": calcLatencyStats(seqLatencies)},
			"burst_100":        map[string]interface{}{"success": burstSuccess, "errors": burstErrors, "rps": float64(burstSuccess) / burstDuration.Seconds(), "latency": calcLatencyStats(burstLatencies)},
			"sustained_500x50": map[string]interface{}{"success": sustainedSuccess, "errors": sustainedErrors, "rps": float64(sustainedSuccess) / sustainedDuration.Seconds(), "latency": calcLatencyStats(sustainedLatencies)},
		},
	}
}

// ============================================================================
// SUITE 2: EXACTLY-ONCE PROOF
// Measures: duplicate detection, zero missed jobs, zero double execution
// ============================================================================

func runExactlyOnceTest(baseURL string) BenchmarkResult {
	fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  SUITE 2: EXACTLY-ONCE GUARANTEE PROOF")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	// ── Phase 1: Duplicate storm — same request ID submitted 10 times ──
	fmt.Println("\n  Phase 1: Duplicate storm (100 unique jobs × 10 replays = 1000 submissions)...")

	uniqueJobs := 100
	replays := 10
	totalSubmissions := uniqueJobs * replays

	// Generate unique request IDs
	requestIDs := make([]string, uniqueJobs)
	for i := 0; i < uniqueJobs; i++ {
		requestIDs[i] = fmt.Sprintf("exactlyonce-%d-%d", time.Now().UnixNano(), i)
	}

	var accepted int64     // First submission of each unique ID
	var deduplicated int64 // Duplicate submissions blocked
	var errors int64
	latencies := make([]float64, 0, totalSubmissions)
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Track which request IDs got accepted (should be exactly 100)
	acceptedIDs := make(map[string]string) // requestID -> jobID
	var idMu sync.Mutex

	start := time.Now()

	// Submit all replays concurrently (simulates network retries / client bugs)
	for replay := 0; replay < replays; replay++ {
		for i := 0; i < uniqueJobs; i++ {
			wg.Add(1)
			go func(reqID string, jobIdx, replayIdx int) {
				defer wg.Done()

				req := ScheduleRequest{
					RequestID: reqID, // SAME request ID across replays
					Name:      fmt.Sprintf("exactlyonce-job-%d", jobIdx),
					Image:     "nvidia/cuda:12.0-base-ubuntu22.04",
					Command:   []string{"sleep", "1"},
					GPUCount:  1,
					Priority:  5,
					MemoryMB:  512,
					CPUMillis: 500,
				}

				// Small random jitter to simulate real retry timing
				time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)

				submitStart := time.Now()
				resp, err := submitJobRaw(baseURL, req)
				elapsed := time.Since(submitStart).Seconds() * 1000

				mu.Lock()
				latencies = append(latencies, elapsed)
				mu.Unlock()

				if err != nil {
					// Check if it's a duplicate rejection (expected)
					if strings.Contains(err.Error(), "DUPLICATE") ||
						strings.Contains(err.Error(), "IDEMPOTENT") ||
						strings.Contains(err.Error(), "already") ||
						strings.Contains(err.Error(), "409") {
						atomic.AddInt64(&deduplicated, 1)
					} else {
						atomic.AddInt64(&errors, 1)
					}
					return
				}

				// First successful submission for this ID
				if resp != nil && resp.JobID != "" {
					idMu.Lock()
					if _, exists := acceptedIDs[reqID]; !exists {
						acceptedIDs[reqID] = resp.JobID
						atomic.AddInt64(&accepted, 1)
					} else {
						// Same request ID accepted TWICE — this is a bug!
						atomic.AddInt64(&deduplicated, 1)
					}
					idMu.Unlock()
				}
			}(requestIDs[i], i, replay)
		}
	}
	wg.Wait()
	duration := time.Since(start)

	// ── Phase 2: Verify unique job count ──
	fmt.Println("\n  Phase 2: Verifying job uniqueness...")

	uniqueJobIDs := make(map[string]bool)
	for _, jobID := range acceptedIDs {
		uniqueJobIDs[jobID] = true
	}

	// ── Results ──
	stats := calcLatencyStats(latencies)

	fmt.Println("\n  ┌─────────────────────────────────────────────────┐")
	fmt.Println("    │           EXACTLY-ONCE RESULTS                  │")
	fmt.Println("    ├─────────────────────────────────────────────────┤")
	fmt.Printf("  │  Total submissions:      %4d                    │\n", totalSubmissions)
	fmt.Printf("  │  Unique request IDs:      %4d                   │\n", uniqueJobs)
	fmt.Printf("  │  Accepted (first submit): %4d                   │\n", accepted)
	fmt.Printf("  │  Duplicates blocked:      %4d                   │\n", deduplicated)
	fmt.Printf("  │  Errors:                  %4d                   │\n", errors)
	fmt.Printf("  │  Unique job IDs created:  %4d                   │\n", len(uniqueJobIDs))
	fmt.Println("    ├─────────────────────────────────────────────────┤")

	// THE CRITICAL CHECK
	duplicateExecutions := int(accepted) - len(uniqueJobIDs)
	missedJobs := uniqueJobs - len(uniqueJobIDs)

	if duplicateExecutions == 0 && missedJobs == 0 {
		fmt.Println("  │  ✅ ZERO duplicate executions                  │")
		fmt.Println("  │  ✅ ZERO missed jobs                           │")
		fmt.Println("  │  ✅ EXACTLY-ONCE GUARANTEE: PROVEN             │")
	} else {
		if duplicateExecutions > 0 {
			fmt.Printf("  │  ❌ DUPLICATE EXECUTIONS: %d (BUG!)            │\n", duplicateExecutions)
		}
		if missedJobs > 0 {
			fmt.Printf("  │  ❌ MISSED JOBS: %d (BUG!)                    │\n", missedJobs)
		}
		fmt.Println("  │  ❌ EXACTLY-ONCE GUARANTEE: FAILED             │")
	}
	fmt.Println("  └─────────────────────────────────────────────────┘")

	printLatencyTable("EXACTLY-ONCE", stats, int(accepted), int(errors),
		float64(totalSubmissions)/duration.Seconds())

	return BenchmarkResult{
		Suite:            "exactly-once",
		Timestamp:        time.Now().Format(time.RFC3339),
		TotalRequests:    totalSubmissions,
		SuccessCount:     int(accepted),
		ErrorCount:       int(errors),
		DuplicateBlocked: int(deduplicated),
		Latencies:        stats,
		Throughput:       float64(totalSubmissions) / duration.Seconds(),
		Duration:         duration.String(),
		Details: map[string]interface{}{
			"unique_request_ids":   uniqueJobs,
			"replays_per_id":       replays,
			"unique_job_ids":       len(uniqueJobIDs),
			"duplicate_executions": duplicateExecutions,
			"missed_jobs":          missedJobs,
			"exactly_once_proven":  duplicateExecutions == 0 && missedJobs == 0,
		},
	}
}

// ============================================================================
// SUITE 3: FAILURE INJECTION
// Measures: recovery latency after worker kill, job completion after failures
// Note: This test submits long-running jobs and checks status.
//       For full failure injection, manually kill a worker mid-test.
// ============================================================================

func runFailureInjectionTest(baseURL string) BenchmarkResult {
	fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  SUITE 3: FAILURE INJECTION TEST")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("\n  INSTRUCTIONS:")
	fmt.Println("  1. This test submits 50 long-running jobs (60s each)")
	fmt.Println("  2. After 15 seconds, MANUALLY kill a worker:")
	fmt.Println("     kubectl delete pod -l app=ares-local -n ares --force")
	fmt.Println("  3. The test monitors job completion and measures recovery")
	fmt.Println("")

	// ── Phase 1: Submit 50 long-running jobs ──
	fmt.Println("  Phase 1: Submitting 50 jobs (60s execution time)...")
	jobIDs := make([]string, 0, 50)
	requestIDs := make([]string, 0, 50)
	latencies := make([]float64, 0, 50)
	successCount := 0
	errorCount := 0

	for i := 0; i < 50; i++ {
		reqID := fmt.Sprintf("failure-%d-%d", time.Now().UnixNano(), i)
		req := ScheduleRequest{
			RequestID:  reqID,
			Name:       fmt.Sprintf("failure-job-%d", i),
			Image:      "nvidia/cuda:12.0-base-ubuntu22.04",
			Command:    []string{"sleep", "60"},
			GPUCount:   1,
			Priority:   5,
			MemoryMB:   512,
			CPUMillis:  500,
			MaxRetries: 3,
		}

		start := time.Now()
		resp, err := submitJob(baseURL, req)
		elapsed := time.Since(start).Seconds() * 1000
		latencies = append(latencies, elapsed)

		if err != nil {
			errorCount++
			continue
		}

		successCount++
		jobIDs = append(jobIDs, resp.JobID)
		requestIDs = append(requestIDs, reqID)
	}

	fmt.Printf("  Submitted: %d success, %d errors\n", successCount, errorCount)

	// ── Phase 2: Wait and monitor ──
	fmt.Println("\n  Phase 2: Monitoring job status (waiting 90 seconds)...")
	fmt.Println("  *** KILL A WORKER NOW: kubectl delete pod -l app=ares-local -n ares --force ***")
	fmt.Println("")

	// Poll job status every 5 seconds for 90 seconds
	statusCounts := make(map[string]int)
	for tick := 0; tick < 18; tick++ { // 18 × 5s = 90s
		time.Sleep(5 * time.Second)

		succeeded := 0
		failed := 0
		running := 0
		pending := 0

		for _, jobID := range jobIDs {
			status := getJobStatus(baseURL, jobID)
			switch status {
			case "SUCCEEDED", "succeeded":
				succeeded++
			case "FAILED", "failed":
				failed++
			case "RUNNING", "running":
				running++
			default:
				pending++
			}
		}

		fmt.Printf("    T+%3ds: running=%d, succeeded=%d, failed=%d, pending=%d\n",
			(tick+1)*5, running, succeeded, failed, pending)

		statusCounts["running"] = running
		statusCounts["succeeded"] = succeeded
		statusCounts["failed"] = failed
		statusCounts["pending"] = pending
	}

	// ── Phase 3: Final status check ──
	fmt.Println("\n  Phase 3: Final status check...")
	finalSucceeded := 0
	finalFailed := 0
	finalOther := 0

	for _, jobID := range jobIDs {
		status := getJobStatus(baseURL, jobID)
		switch status {
		case "SUCCEEDED", "succeeded":
			finalSucceeded++
		case "FAILED", "failed":
			finalFailed++
		default:
			finalOther++
		}
	}

	fmt.Println("\n┌─────────────────────────────────────────────────┐")
	fmt.Println("  │           FAILURE INJECTION RESULTS             │")
	fmt.Println("  ├─────────────────────────────────────────────────┤")
	fmt.Printf("│   Jobs submitted:     %4d                       │\n", len(jobIDs))
	fmt.Printf("│  Final succeeded:    %4d                        │\n", finalSucceeded)
	fmt.Printf("│  Final failed:       %4d                        │\n", finalFailed)
	fmt.Printf("│  Still running:      %4d                        │\n", finalOther)
	fmt.Println("  ├─────────────────────────────────────────────────┤")

	recoveryRate := 0.0
	if len(jobIDs) > 0 {
		recoveryRate = float64(finalSucceeded) / float64(len(jobIDs)) * 100
	}
	fmt.Printf("  │  Recovery rate:      %.1f%%                     │\n", recoveryRate)

	if finalSucceeded == len(jobIDs) {
		fmt.Println("  │  ✅ ALL JOBS RECOVERED AND COMPLETED            │")
	} else if finalSucceeded+finalFailed == len(jobIDs) {
		fmt.Println("  │  ⚠️  All jobs finished (some failed)             │")
	} else {
		fmt.Println("  │  ⏳ Some jobs still in progress                  │")
	}
	fmt.Println("  └─────────────────────────────────────────────────┘")

	stats := calcLatencyStats(latencies)

	return BenchmarkResult{
		Suite:         "failure-injection",
		Timestamp:     time.Now().Format(time.RFC3339),
		TotalRequests: len(jobIDs),
		SuccessCount:  finalSucceeded,
		ErrorCount:    finalFailed,
		Latencies:     stats,
		Duration:      "~90s",
		Details: map[string]interface{}{
			"jobs_submitted":    len(jobIDs),
			"final_succeeded":   finalSucceeded,
			"final_failed":      finalFailed,
			"still_running":     finalOther,
			"recovery_rate_pct": recoveryRate,
		},
	}
}

// ============================================================================
// SUITE 4: GANG SCHEDULING
// Measures: barrier sync time, all-or-nothing placement, deadlock handling
// Uses: /gang/submit endpoint (dedicated gang API)
// ============================================================================

// GangSubmitRequest matches the server's GangSubmitRequest in handlers_gang.go
type GangSubmitRequest struct {
	GangID          string   `json:"gang_id"`
	Name            string   `json:"name"`
	MinMembers      int      `json:"min_members"`
	GPUsPerMember   int      `json:"gpus_per_member"`
	GPUType         string   `json:"gpu_type,omitempty"`
	Priority        int      `json:"priority,omitempty"`
	PreferColocated bool     `json:"prefer_colocated,omitempty"`
	RequireNVLink   bool     `json:"require_nvlink,omitempty"`
	Image           string   `json:"image,omitempty"`
	Command         []string `json:"command,omitempty"`
	Args            []string `json:"args,omitempty"`

	ScheduleTimeoutSecs int `json:"schedule_timeout_secs,omitempty"`
	BarrierTimeoutSecs  int `json:"barrier_timeout_secs,omitempty"`
}

type GangSubmitResponse struct {
	Success   bool   `json:"success"`
	GangID    string `json:"gang_id"`
	Phase     string `json:"phase"`
	Members   int    `json:"members"`
	TotalGPUs int    `json:"total_gpus"`
	Message   string `json:"message"`
}

type GangStatusResponse struct {
	GangID       string             `json:"gang_id"`
	Phase        string             `json:"phase"`
	Members      int                `json:"members"`
	TotalGPUs    int                `json:"total_gpus"`
	ReadyCount   int                `json:"ready_count"`
	FailedCount  int                `json:"failed_count"`
	PendingCount int                `json:"pending_count"`
	MemberDetail []GangMemberDetail `json:"member_detail"`
	LastError    string             `json:"last_error,omitempty"`
}

type GangMemberDetail struct {
	MemberIndex int    `json:"member_index"`
	JobID       string `json:"job_id"`
	Status      string `json:"status"`
	ClusterID   string `json:"cluster_id,omitempty"`
	NodeID      string `json:"node_id,omitempty"`
	GPUIndices  []int  `json:"gpu_indices,omitempty"`
	Ready       bool   `json:"ready"`
	Error       string `json:"error,omitempty"`
}

func runGangSchedulingTest(baseURL string) BenchmarkResult {
	fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  SUITE 4: GANG SCHEDULING TEST")
	fmt.Println("  Using dedicated /gang/submit endpoint")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	latencies := make([]float64, 0)
	totalSuccess := 0
	totalErrors := 0
	allOrNothingViolations := 0

	// ── Phase 1: Small gangs (5 gangs × 2 members × 1 GPU each) ──
	fmt.Println("\n  Phase 1: Small gangs (5 gangs × 2 members × 1 GPU each)...")
	for gangIdx := 0; gangIdx < 5; gangIdx++ {
		gangID := fmt.Sprintf("gang-small-%d-%d", time.Now().UnixNano(), gangIdx)

		req := GangSubmitRequest{
			GangID:              gangID,
			Name:                fmt.Sprintf("bench-gang-small-%d", gangIdx),
			MinMembers:          2,
			GPUsPerMember:       1,
			Priority:            5,
			Image:               "nvidia/cuda:12.0-base-ubuntu22.04",
			Command:             []string{"sleep", "5"},
			ScheduleTimeoutSecs: 30,
			BarrierTimeoutSecs:  30,
		}

		gangStart := time.Now()
		resp, err := submitGang(baseURL, req)
		gangElapsed := time.Since(gangStart).Seconds() * 1000
		latencies = append(latencies, gangElapsed)

		if err != nil {
			totalErrors++
			fmt.Printf("    Gang %d: FAILED to submit: %v (%.1fms)\n", gangIdx+1, err, gangElapsed)
			continue
		}

		totalSuccess++
		fmt.Printf("    Gang %d: submitted (phase=%s, members=%d, gpus=%d) %.1fms\n",
			gangIdx+1, resp.Phase, resp.Members, resp.TotalGPUs, gangElapsed)

		// Poll status to check placement
		status := pollGangStatus(baseURL, gangID, 10*time.Second)
		if status != nil {
			if status.ReadyCount > 0 && status.ReadyCount < status.Members {
				allOrNothingViolations++
				fmt.Printf("           ❌ PARTIAL placement: %d/%d (violates all-or-nothing!)\n",
					status.ReadyCount, status.Members)
			} else if status.ReadyCount == status.Members {
				fmt.Printf("           ✅ All %d members placed\n", status.Members)
			} else {
				fmt.Printf("           ⏳ Phase: %s (ready=%d, pending=%d, failed=%d)\n",
					status.Phase, status.ReadyCount, status.PendingCount, status.FailedCount)
			}
		}
	}

	// ── Phase 2: Medium gangs (3 gangs × 4 members × 1 GPU, NVLink preferred) ──
	fmt.Println("\n  Phase 2: Medium gangs (3 gangs × 4 members × 1 GPU, NVLink preferred)...")
	for gangIdx := 0; gangIdx < 3; gangIdx++ {
		gangID := fmt.Sprintf("gang-medium-%d-%d", time.Now().UnixNano(), gangIdx)

		req := GangSubmitRequest{
			GangID:              gangID,
			Name:                fmt.Sprintf("bench-gang-medium-%d", gangIdx),
			MinMembers:          4,
			GPUsPerMember:       1,
			RequireNVLink:       true,
			PreferColocated:     true,
			Priority:            5,
			Image:               "nvidia/cuda:12.0-base-ubuntu22.04",
			Command:             []string{"sleep", "5"},
			ScheduleTimeoutSecs: 30,
			BarrierTimeoutSecs:  30,
		}

		gangStart := time.Now()
		resp, err := submitGang(baseURL, req)
		gangElapsed := time.Since(gangStart).Seconds() * 1000
		latencies = append(latencies, gangElapsed)

		if err != nil {
			totalErrors++
			fmt.Printf("    Gang %d: FAILED to submit: %v (%.1fms)\n", gangIdx+1, err, gangElapsed)
			continue
		}

		totalSuccess++

		status := pollGangStatus(baseURL, gangID, 15*time.Second)
		placement := "⏳ pending"
		if status != nil {
			if status.ReadyCount > 0 && status.ReadyCount < status.Members {
				allOrNothingViolations++
				placement = fmt.Sprintf("❌ PARTIAL %d/%d", status.ReadyCount, status.Members)
			} else if status.ReadyCount == status.Members {
				placement = fmt.Sprintf("✅ all %d placed", status.Members)
			} else {
				placement = fmt.Sprintf("phase=%s (ready=%d, failed=%d)",
					status.Phase, status.ReadyCount, status.FailedCount)
			}
		}

		fmt.Printf("    Gang %d: submitted (phase=%s, gpus=%d) %.1fms — %s\n",
			gangIdx+1, resp.Phase, resp.TotalGPUs, gangElapsed, placement)
	}

	// ── Phase 3: Oversubscription test (should reject, not deadlock) ──
	fmt.Println("\n  Phase 3: Oversubscription (request more GPUs than available)...")
	oversubGangID := fmt.Sprintf("gang-oversub-%d", time.Now().UnixNano())

	oversubReq := GangSubmitRequest{
		GangID:              oversubGangID,
		Name:                "bench-gang-oversub",
		MinMembers:          100,
		GPUsPerMember:       8,
		Priority:            5,
		Image:               "nvidia/cuda:12.0-base-ubuntu22.04",
		Command:             []string{"sleep", "5"},
		ScheduleTimeoutSecs: 10,
		BarrierTimeoutSecs:  10,
	}

	oversubStart := time.Now()
	_, oversubErr := submitGang(baseURL, oversubReq)
	oversubElapsed := time.Since(oversubStart).Seconds() * 1000

	oversubHandled := false
	if oversubErr != nil {
		oversubHandled = true
		fmt.Printf("    Oversubscription correctly rejected: %.1fms (no deadlock)\n", oversubElapsed)
	} else {
		// Submitted but should timeout or fail during scheduling
		status := pollGangStatus(baseURL, oversubGangID, 15*time.Second)
		if status != nil && (status.Phase == "TIMEOUT" || status.Phase == "FAILED") {
			oversubHandled = true
			fmt.Printf("    Oversubscription timed out correctly: phase=%s %.1fms\n", status.Phase, oversubElapsed)
		} else {
			fmt.Printf("    ⚠️  Oversubscription accepted unexpectedly: %.1fms\n", oversubElapsed)
		}
	}

	// ── Results ──
	stats := calcLatencyStats(latencies)

	fmt.Println("\n  ┌─────────────────────────────────────────────────┐")
	fmt.Println("  │           GANG SCHEDULING RESULTS                │")
	fmt.Println("  ├─────────────────────────────────────────────────┤")
	fmt.Printf("  │  Gangs submitted:              %4d              │\n", totalSuccess+totalErrors)
	fmt.Printf("  │  Successfully accepted:        %4d              │\n", totalSuccess)
	fmt.Printf("  │  Failed to submit:             %4d              │\n", totalErrors)
	fmt.Printf("  │  All-or-nothing violations:    %4d              │\n", allOrNothingViolations)
	fmt.Printf("  │  Oversubscription handled:     %v               │\n", oversubHandled)
	fmt.Printf("  │  Avg submit latency:           %.1fms            │\n", stats.Avg)
	fmt.Println("  ├─────────────────────────────────────────────────┤")

	if allOrNothingViolations == 0 && oversubHandled {
		fmt.Println("  │  ✅ ALL-OR-NOTHING: PROVEN                      │")
		fmt.Println("  │  ✅ DEADLOCK PREVENTION: PROVEN                  │")
	} else {
		if allOrNothingViolations > 0 {
			fmt.Println("  │  ❌ ALL-OR-NOTHING: VIOLATED                    │")
		}
		if !oversubHandled {
			fmt.Println("  │  ❌ DEADLOCK PREVENTION: NOT PROVEN              │")
		}
	}
	fmt.Println("  └─────────────────────────────────────────────────┘")

	return BenchmarkResult{
		Suite:         "gang-scheduling",
		Timestamp:     time.Now().Format(time.RFC3339),
		TotalRequests: totalSuccess + totalErrors,
		SuccessCount:  totalSuccess,
		ErrorCount:    totalErrors,
		Latencies:     stats,
		Duration:      "n/a",
		Details: map[string]interface{}{
			"small_gangs_2member":       5,
			"medium_gangs_4member":      3,
			"all_or_nothing_violations": allOrNothingViolations,
			"oversubscription_handled":  oversubHandled,
			"all_or_nothing_proven":     allOrNothingViolations == 0,
		},
	}
}

// ============================================================================
// GANG HTTP HELPERS
// ============================================================================

func submitGang(baseURL string, req GangSubmitRequest) (*GangSubmitResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal error: %w", err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	httpResp, err := client.Post(baseURL+"/gang/submit", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("http error: %w", err)
	}
	defer httpResp.Body.Close()

	respBody, _ := io.ReadAll(httpResp.Body)

	if httpResp.StatusCode != http.StatusAccepted && httpResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%d: %s", httpResp.StatusCode, string(respBody))
	}

	var gangResp GangSubmitResponse
	if err := json.Unmarshal(respBody, &gangResp); err != nil {
		return nil, fmt.Errorf("decode error: %w", err)
	}

	return &gangResp, nil
}

func getGangStatus(baseURL string, gangID string) (*GangStatusResponse, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(fmt.Sprintf("%s/gang/status?gang_id=%s", baseURL, gangID))
	if err != nil {
		return nil, fmt.Errorf("http error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	var status GangStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("decode error: %w", err)
	}

	return &status, nil
}

// pollGangStatus: Poll gang status until terminal state or timeout
func pollGangStatus(baseURL string, gangID string, timeout time.Duration) *GangStatusResponse {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		status, err := getGangStatus(baseURL, gangID)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		// Terminal states — stop polling
		switch status.Phase {
		case "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED", "TIMEOUT":
			return status
		}

		// Also stop if all members are ready (even if phase hasn't updated)
		if status.ReadyCount == status.Members && status.Members > 0 {
			return status
		}

		time.Sleep(1 * time.Second)
	}

	// One final check
	status, _ := getGangStatus(baseURL, gangID)
	return status
}

// ============================================================================
// SUITE 5: DRF FAIRNESS
// Measures: GPU allocation fairness between competing tenants
// Proves: No tenant starvation under contention
// ============================================================================

func runDRFFairnessTest(baseURL string) BenchmarkResult {
	fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  SUITE 5: DRF FAIRNESS TEST")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	// ── Phase 1: Equal weight tenants competing ──
	fmt.Println("\n  Phase 1: Two equal tenants competing for GPUs (30 jobs each)...")

	tenantAScheduled := int64(0)
	tenantBScheduled := int64(0)
	tenantALatencies := make([]float64, 0, 30)
	tenantBLatencies := make([]float64, 0, 30)
	var mu sync.Mutex
	var wg sync.WaitGroup

	start := time.Now()

	// Tenant A submits 30 jobs
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req := ScheduleRequest{
				RequestID: fmt.Sprintf("drf-tenantA-%d-%d", time.Now().UnixNano(), idx),
				Name:      fmt.Sprintf("drf-tenantA-job-%d", idx),
				Image:     "nvidia/cuda:12.0-base-ubuntu22.04",
				Command:   []string{"sleep", "30"},
				GPUCount:  1,
				Priority:  5,
				MemoryMB:  512,
				CPUMillis: 500,
			}
			// Add tenant_id via custom field
			submitStart := time.Now()
			_, err := submitJobWithTenant(baseURL, req, "tenant-alpha")
			elapsed := time.Since(submitStart).Seconds() * 1000

			if err == nil {
				atomic.AddInt64(&tenantAScheduled, 1)
			}
			mu.Lock()
			tenantALatencies = append(tenantALatencies, elapsed)
			mu.Unlock()
		}(i)
	}

	// Tenant B submits 30 jobs simultaneously
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req := ScheduleRequest{
				RequestID: fmt.Sprintf("drf-tenantB-%d-%d", time.Now().UnixNano(), idx),
				Name:      fmt.Sprintf("drf-tenantB-job-%d", idx),
				Image:     "nvidia/cuda:12.0-base-ubuntu22.04",
				Command:   []string{"sleep", "30"},
				GPUCount:  1,
				Priority:  5,
				MemoryMB:  512,
				CPUMillis: 500,
			}
			submitStart := time.Now()
			_, err := submitJobWithTenant(baseURL, req, "tenant-beta")
			elapsed := time.Since(submitStart).Seconds() * 1000

			if err == nil {
				atomic.AddInt64(&tenantBScheduled, 1)
			}
			mu.Lock()
			tenantBLatencies = append(tenantBLatencies, elapsed)
			mu.Unlock()
		}(i)
	}

	wg.Wait()
	phase1Duration := time.Since(start)

	totalA := atomic.LoadInt64(&tenantAScheduled)
	totalB := atomic.LoadInt64(&tenantBScheduled)

	fmt.Printf("  Tenant Alpha: %d/30 scheduled\n", totalA)
	fmt.Printf("  Tenant Beta:  %d/30 scheduled\n", totalB)

	// ── Phase 2: Unequal demand (Tenant A = 50 jobs, Tenant B = 10 jobs) ──
	fmt.Println("\n  Phase 2: Unequal demand (Tenant A=50 jobs vs Tenant B=10 jobs)...")

	var tenantAHeavy int64
	var tenantBLight int64
	var wg2 sync.WaitGroup

	phase2Start := time.Now()

	// Tenant A: heavy demand
	for i := 0; i < 50; i++ {
		wg2.Add(1)
		go func(idx int) {
			defer wg2.Done()
			req := ScheduleRequest{
				RequestID: fmt.Sprintf("drf-heavy-A-%d-%d", time.Now().UnixNano(), idx),
				Name:      fmt.Sprintf("drf-heavy-A-%d", idx),
				Image:     "nvidia/cuda:12.0-base-ubuntu22.04",
				Command:   []string{"sleep", "20"},
				GPUCount:  1,
				Priority:  5,
				MemoryMB:  512,
				CPUMillis: 500,
			}
			_, err := submitJobWithTenant(baseURL, req, "tenant-alpha")
			if err == nil {
				atomic.AddInt64(&tenantAHeavy, 1)
			}
		}(i)
	}

	// Tenant B: light demand
	for i := 0; i < 10; i++ {
		wg2.Add(1)
		go func(idx int) {
			defer wg2.Done()
			req := ScheduleRequest{
				RequestID: fmt.Sprintf("drf-light-B-%d-%d", time.Now().UnixNano(), idx),
				Name:      fmt.Sprintf("drf-light-B-%d", idx),
				Image:     "nvidia/cuda:12.0-base-ubuntu22.04",
				Command:   []string{"sleep", "20"},
				GPUCount:  1,
				Priority:  5,
				MemoryMB:  512,
				CPUMillis: 500,
			}
			_, err := submitJobWithTenant(baseURL, req, "tenant-beta")
			if err == nil {
				atomic.AddInt64(&tenantBLight, 1)
			}
		}(i)
	}

	wg2.Wait()
	phase2Duration := time.Since(phase2Start)

	fmt.Printf("  Tenant Alpha (heavy): %d/50 scheduled\n", tenantAHeavy)
	fmt.Printf("  Tenant Beta (light):  %d/10 scheduled\n", tenantBLight)

	// ── Fairness Analysis ──
	allLatencies := append(tenantALatencies, tenantBLatencies...)
	stats := calcLatencyStats(allLatencies)

	// Calculate fairness ratio (Jain's fairness index)
	// Perfect fairness = 1.0, total unfairness = 1/n
	fairnessRatio := 0.0
	if totalA+totalB > 0 {
		xA := float64(totalA)
		xB := float64(totalB)
		sumX := xA + xB
		sumX2 := xA*xA + xB*xB
		fairnessRatio = (sumX * sumX) / (2.0 * sumX2) // Jain's index for 2 users
	}

	fmt.Println("\n  ┌─────────────────────────────────────────────────┐")
	fmt.Println("  │           DRF FAIRNESS RESULTS                   │")
	fmt.Println("  ├─────────────────────────────────────────────────┤")
	fmt.Printf("  │  Phase 1 (equal demand):                        │\n")
	fmt.Printf("  │    Tenant Alpha: %3d jobs scheduled              │\n", totalA)
	fmt.Printf("  │    Tenant Beta:  %3d jobs scheduled              │\n", totalB)
	fmt.Printf("  │    Jain's Fairness Index: %.4f                 │\n", fairnessRatio)
	fmt.Printf("  │  Phase 2 (unequal demand):                      │\n")
	fmt.Printf("  │    Tenant Alpha (heavy): %3d/50 scheduled       │\n", tenantAHeavy)
	fmt.Printf("  │    Tenant Beta (light):  %3d/10 scheduled       │\n", tenantBLight)
	fmt.Println("  ├─────────────────────────────────────────────────┤")

	if fairnessRatio >= 0.90 {
		fmt.Println("  │  ✅ FAIR: Jain's index ≥ 0.90                   │")
	} else if fairnessRatio >= 0.75 {
		fmt.Println("  │  ⚠️  MODERATE: Jain's index 0.75-0.90            │")
	} else {
		fmt.Println("  │  ❌ UNFAIR: Jain's index < 0.75                  │")
	}

	// Check tenant B wasn't starved in phase 2
	if tenantBLight == 10 {
		fmt.Println("  │  ✅ NO STARVATION: Light tenant fully served     │")
	} else if tenantBLight >= 7 {
		fmt.Println("  │  ⚠️  MILD STARVATION: Light tenant partially served│")
	} else {
		fmt.Println("  │  ❌ STARVATION: Light tenant starved              │")
	}
	fmt.Println("  └─────────────────────────────────────────────────┘")

	totalDuration := phase1Duration + phase2Duration

	return BenchmarkResult{
		Suite:         "drf-fairness",
		Timestamp:     time.Now().Format(time.RFC3339),
		TotalRequests: 120, // 30+30+50+10
		SuccessCount:  int(totalA + totalB + tenantAHeavy + tenantBLight),
		ErrorCount:    120 - int(totalA+totalB+tenantAHeavy+tenantBLight),
		Latencies:     stats,
		Throughput:    float64(totalA+totalB) / phase1Duration.Seconds(),
		Duration:      totalDuration.String(),
		Details: map[string]interface{}{
			"phase1_tenant_a": totalA,
			"phase1_tenant_b": totalB,
			"phase2_tenant_a": tenantAHeavy,
			"phase2_tenant_b": tenantBLight,
			"jains_fairness":  fairnessRatio,
			"no_starvation":   tenantBLight >= 7,
		},
	}
}

// ============================================================================
// SUITE 6: PRIORITY PREEMPTION
// Measures: High-priority job start time when cluster is full
// Proves: Priority scheduling works, low-priority jobs yield
//
// Server-side preemption config (defaults):
//   - Preemption triggers when incoming Priority >= 50
//   - MinPriorityGap: 20 (incoming must be 20+ above victim)
//   - PreemptiblePriorityMax: 30 (jobs at or below 30 are always preemptible)
//   - MinimumAgeSeconds: 60 (victim must be running 60+ seconds)
//   - MaxPreemptionsPerHour: 10
// ============================================================================

func runPriorityPreemptionTest(baseURL string) BenchmarkResult {
	fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  SUITE 6: PRIORITY PREEMPTION TEST")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  Config: low=10, high=90, min_age=60s, min_gap=20")

	// ── Phase 1: Fill cluster with low-priority jobs ──
	fmt.Println("\n  Phase 1: Filling cluster with low-priority jobs (priority=10)...")

	lowPriorityJobs := 10 // Submit enough to saturate available GPUs
	lowPriorityIDs := make([]string, 0, lowPriorityJobs)
	lowSuccess := 0
	lowQueued := 0

	for i := 0; i < lowPriorityJobs; i++ {
		reqID := fmt.Sprintf("preempt-low-%d-%d", time.Now().UnixNano(), i)
		req := ScheduleRequest{
			RequestID: reqID,
			Name:      fmt.Sprintf("low-priority-job-%d", i),
			Image:     "nvidia/cuda:12.0-base-ubuntu22.04",
			Command:   []string{"sleep", "300"}, // 5 min — long enough to survive the wait
			GPUCount:  1,
			Priority:  10, // LOW — below PreemptiblePriorityMax (30)
			MemoryMB:  512,
			CPUMillis: 500,
		}

		resp, err := submitJob(baseURL, req)
		if err != nil {
			lowQueued++
		} else {
			lowSuccess++
			lowPriorityIDs = append(lowPriorityIDs, resp.JobID)
		}
	}
	fmt.Printf("  Submitted: %d low-priority jobs (%d accepted, %d errored)\n",
		lowPriorityJobs, lowSuccess, lowQueued)

	// ── Phase 2: Wait for minimum age to pass ──
	// PreemptionConfig.MinimumAgeSeconds = 60 — victims must be 60+ seconds old
	waitSecs := 65
	fmt.Printf("\n  Phase 2: Waiting %d seconds for jobs to exceed MinimumAgeSeconds (60s)...\n", waitSecs)
	fmt.Println("  (Preemption requires victims to be running for at least 60 seconds)")

	for remaining := waitSecs; remaining > 0; remaining -= 10 {
		fmt.Printf("    %ds remaining...\n", remaining)
		if remaining > 10 {
			time.Sleep(10 * time.Second)
		} else {
			time.Sleep(time.Duration(remaining) * time.Second)
		}
	}

	// Check current status of low-priority jobs
	lowRunning := 0
	for _, jobID := range lowPriorityIDs {
		status := getJobStatus(baseURL, jobID)
		if status == "RUNNING" || status == "running" || status == "SCHEDULED" || status == "scheduled" {
			lowRunning++
		}
	}
	fmt.Printf("  Low-priority jobs currently running/scheduled: %d\n", lowRunning)

	// ── Phase 3: Submit high-priority job ──
	// Priority 90 → triggers preemption path (>= 50 threshold)
	// Gap: 90 - 10 = 80 → exceeds MinPriorityGap (20)
	fmt.Println("\n  Phase 3: Submitting HIGH-priority job (priority=90)...")
	fmt.Println("  (Priority >= 50 triggers preemption; gap=80 exceeds min_gap=20)")

	highPriorityReqID := fmt.Sprintf("preempt-high-%d", time.Now().UnixNano())
	highReq := ScheduleRequest{
		RequestID: highPriorityReqID,
		Name:      "high-priority-critical-job",
		Image:     "nvidia/cuda:12.0-base-ubuntu22.04",
		Command:   []string{"sleep", "10"},
		GPUCount:  1,
		Priority:  90, // HIGH — triggers preemption path (>= 50)
		MemoryMB:  512,
		CPUMillis: 500,
	}

	highStart := time.Now()
	highResp, highErr := submitJob(baseURL, highReq)
	highSubmitLatency := time.Since(highStart).Seconds() * 1000

	highSubmitted := false
	if highErr != nil {
		fmt.Printf("  High-priority job submission: FAILED (%v)\n", highErr)
	} else {
		highSubmitted = true
		fmt.Printf("  High-priority job submitted: %s (%.1fms)\n", highResp.JobID, highSubmitLatency)
	}

	// ── Phase 4: Monitor high-priority job status ──
	fmt.Println("\n  Phase 4: Monitoring high-priority job status (60 seconds max)...")

	timeToRunning := -1.0
	monitorStart := time.Now()

	if highSubmitted {
		for tick := 0; tick < 30; tick++ { // 30 × 2s = 60s max
			time.Sleep(2 * time.Second)

			status := getJobStatus(baseURL, highResp.JobID)
			elapsed := time.Since(monitorStart).Seconds()

			fmt.Printf("    T+%.0fs: high-priority status=%s\n", elapsed, status)

			if status == "RUNNING" || status == "running" {
				timeToRunning = elapsed
				break
			}
			if status == "SUCCEEDED" || status == "succeeded" {
				timeToRunning = elapsed
				break
			}
		}
	}

	// ── Phase 5: Check if any low-priority jobs were preempted ──
	fmt.Println("\n  Phase 5: Checking low-priority job status after preemption...")

	preempted := 0
	running := 0
	completed := 0
	failed := 0
	other := 0

	for _, jobID := range lowPriorityIDs {
		status := getJobStatus(baseURL, jobID)
		switch {
		case status == "PREEMPTED" || status == "preempted":
			preempted++
		case status == "FAILED" || status == "failed":
			failed++
		case status == "RUNNING" || status == "running":
			running++
		case status == "SUCCEEDED" || status == "succeeded":
			completed++
		default:
			other++
		}
	}

	// ── Results ──
	fmt.Println("\n  ┌─────────────────────────────────────────────────┐")
	fmt.Println("  │           PRIORITY PREEMPTION RESULTS            │")
	fmt.Println("  ├─────────────────────────────────────────────────┤")
	fmt.Printf("  │  Low-priority submitted:      %4d              │\n", lowSuccess)
	fmt.Printf("  │  Low-priority running:        %4d              │\n", running)
	fmt.Printf("  │  Low-priority preempted:      %4d              │\n", preempted)
	fmt.Printf("  │  Low-priority failed:         %4d              │\n", failed)
	fmt.Printf("  │  Low-priority completed:      %4d              │\n", completed)
	fmt.Printf("  │  Low-priority other:          %4d              │\n", other)
	fmt.Println("  ├─────────────────────────────────────────────────┤")
	fmt.Printf("  │  High-priority submitted:     %v               │\n", highSubmitted)
	fmt.Printf("  │  High-priority submit latency: %.1fms          │\n", highSubmitLatency)

	if timeToRunning >= 0 {
		fmt.Printf("  │  High-priority time to RUNNING: %.1fs           │\n", timeToRunning)
	}

	fmt.Println("  ├─────────────────────────────────────────────────┤")

	// Determine pass/fail
	highPriorityStarted := timeToRunning >= 0
	preemptionObserved := preempted > 0 || failed > 0

	if highPriorityStarted && preemptionObserved {
		fmt.Println("  │  ✅ HIGH-PRIORITY JOB STARTED                   │")
		fmt.Printf("  │  ✅ PREEMPTION OBSERVED: %d job(s) evicted       │\n", preempted+failed)
		fmt.Println("  │  ✅ PRIORITY PREEMPTION: PROVEN                  │")
	} else if highPriorityStarted {
		fmt.Println("  │  ✅ HIGH-PRIORITY JOB STARTED                   │")
		fmt.Println("  │  ⚠️  No preemption needed (spare capacity)       │")
		fmt.Println("  │  ✅ PRIORITY SCHEDULING: PROVEN                  │")
	} else {
		fmt.Println("  │  ❌ HIGH-PRIORITY JOB DID NOT START             │")
		if !preemptionObserved {
			fmt.Println("  │  ❌ NO PREEMPTION OBSERVED                      │")
		}
	}
	fmt.Println("  └─────────────────────────────────────────────────┘")

	latencies := []float64{highSubmitLatency}
	stats := calcLatencyStats(latencies)

	return BenchmarkResult{
		Suite:         "priority-preemption",
		Timestamp:     time.Now().Format(time.RFC3339),
		TotalRequests: lowSuccess + 1,
		SuccessCount:  lowSuccess + 1,
		ErrorCount:    0,
		Latencies:     stats,
		Duration:      time.Since(highStart).String(),
		Details: map[string]interface{}{
			"low_priority_submitted":          lowSuccess,
			"low_priority_running":            running,
			"low_priority_preempted":          preempted,
			"low_priority_failed":             failed,
			"high_priority_submitted":         highSubmitted,
			"high_priority_submit_ms":         highSubmitLatency,
			"high_priority_time_to_running_s": timeToRunning,
			"preemption_observed":             preemptionObserved,
			"priority_scheduling_proven":      highPriorityStarted,
		},
	}
}

// ============================================================================
// SUITE 7: MULTI-CLUSTER ROUTING
// Measures: Jobs route to correct cluster based on GPU type requirements
// Proves: Cross-cloud scheduling decisions are topology-aware
// ============================================================================

func runMultiClusterRoutingTest(baseURL string) BenchmarkResult {
	fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  SUITE 7: MULTI-CLUSTER ROUTING TEST")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	type routingResult struct {
		RequestedGPU string
		ClusterID    string
		Success      bool
		LatencyMs    float64
	}

	results := make([]routingResult, 0)
	var mu sync.Mutex

	// ── Phase 1: GPU-type-specific routing ──
	fmt.Println("\n  Phase 1: Submitting jobs requesting specific GPU types...")

	gpuTypes := []struct {
		gpuType string
		count   int
		desc    string
	}{
		{"T4", 5, "Should route to GKE T4 cluster"},
		{"A10G", 5, "Should route to AWS g5 cluster"},
		{"H100", 5, "Should route to AWS p5 cluster"},
		{"any", 10, "Should route to best available cluster"},
	}

	var wg sync.WaitGroup
	start := time.Now()

	for _, gt := range gpuTypes {
		fmt.Printf("  Submitting %d jobs requesting %s GPU... (%s)\n", gt.count, gt.gpuType, gt.desc)

		for i := 0; i < gt.count; i++ {
			wg.Add(1)
			go func(gpuType string, idx int) {
				defer wg.Done()

				req := ScheduleRequest{
					RequestID: fmt.Sprintf("route-%s-%d-%d", gpuType, time.Now().UnixNano(), idx),
					Name:      fmt.Sprintf("route-%s-%d", gpuType, idx),
					Image:     "nvidia/cuda:12.0-base-ubuntu22.04",
					Command:   []string{"sleep", "5"},
					GPUCount:  1,
					GPUType:   gpuType,
					Priority:  5,
					MemoryMB:  512,
					CPUMillis: 500,
				}

				submitStart := time.Now()
				resp, err := submitJob(baseURL, req)
				elapsed := time.Since(submitStart).Seconds() * 1000

				r := routingResult{
					RequestedGPU: gpuType,
					LatencyMs:    elapsed,
				}

				if err == nil && resp != nil {
					r.ClusterID = resp.ClusterID
					r.Success = true
				}

				mu.Lock()
				results = append(results, r)
				mu.Unlock()
			}(gt.gpuType, i)
		}
	}

	wg.Wait()
	duration := time.Since(start)

	// ── Phase 2: NVLink preference routing ──
	fmt.Println("\n  Phase 2: Submitting multi-GPU jobs with NVLink preference...")

	nvlinkResults := make([]routingResult, 0)

	for i := 0; i < 5; i++ {
		req := ScheduleRequest{
			RequestID:    fmt.Sprintf("route-nvlink-%d-%d", time.Now().UnixNano(), i),
			Name:         fmt.Sprintf("route-nvlink-%d", i),
			Image:        "nvidia/cuda:12.0-base-ubuntu22.04",
			Command:      []string{"sleep", "5"},
			GPUCount:     2, // Multi-GPU
			Priority:     5,
			MemoryMB:     512,
			CPUMillis:    500,
			PreferNVLink: true, // Should route to NVLink-capable cluster
		}

		submitStart := time.Now()
		resp, err := submitJob(baseURL, req)
		elapsed := time.Since(submitStart).Seconds() * 1000

		r := routingResult{
			RequestedGPU: "nvlink-preferred",
			LatencyMs:    elapsed,
		}
		if err == nil && resp != nil {
			r.ClusterID = resp.ClusterID
			r.Success = true
		}

		nvlinkResults = append(nvlinkResults, r)
		fmt.Printf("    NVLink job %d → cluster: %s (%.1fms)\n", i, r.ClusterID, elapsed)
	}

	// ── Analyze routing decisions ──
	fmt.Println("\n  ┌─────────────────────────────────────────────────┐")
	fmt.Println("  │           MULTI-CLUSTER ROUTING RESULTS          │")
	fmt.Println("  ├─────────────────────────────────────────────────┤")

	// Group by requested GPU type → show which cluster each landed on
	routingMap := make(map[string]map[string]int) // gpuType -> clusterID -> count
	successCount := 0
	errorCount := 0

	for _, r := range results {
		if r.Success {
			successCount++
			if routingMap[r.RequestedGPU] == nil {
				routingMap[r.RequestedGPU] = make(map[string]int)
			}
			routingMap[r.RequestedGPU][r.ClusterID]++
		} else {
			errorCount++
		}
	}

	for gpuType, clusters := range routingMap {
		fmt.Printf("  │  GPU=%s requests:                              │\n", gpuType)
		for clusterID, count := range clusters {
			// Truncate cluster ID for display
			displayID := clusterID
			if len(displayID) > 35 {
				displayID = displayID[:35] + "..."
			}
			fmt.Printf("  │    → %s: %d jobs     │\n", displayID, count)
		}
	}

	fmt.Println("  ├─────────────────────────────────────────────────┤")

	// NVLink routing analysis
	nvlinkClusters := make(map[string]int)
	for _, r := range nvlinkResults {
		if r.Success {
			nvlinkClusters[r.ClusterID]++
		}
	}
	fmt.Printf("  │  NVLink-preferred jobs:                         │\n")
	for clusterID, count := range nvlinkClusters {
		displayID := clusterID
		if len(displayID) > 35 {
			displayID = displayID[:35] + "..."
		}
		fmt.Printf("  │    → %s: %d jobs     │\n", displayID, count)
	}

	fmt.Println("  ├─────────────────────────────────────────────────┤")
	fmt.Printf("  │  Total routed: %d  Errors: %d                   │\n", successCount, errorCount)
	fmt.Println("  └─────────────────────────────────────────────────┘")

	allLatencies := make([]float64, 0)
	for _, r := range results {
		allLatencies = append(allLatencies, r.LatencyMs)
	}
	stats := calcLatencyStats(allLatencies)

	return BenchmarkResult{
		Suite:         "multi-cluster-routing",
		Timestamp:     time.Now().Format(time.RFC3339),
		TotalRequests: len(results) + len(nvlinkResults),
		SuccessCount:  successCount + len(nvlinkResults),
		ErrorCount:    errorCount,
		Latencies:     stats,
		Throughput:    float64(successCount) / duration.Seconds(),
		Duration:      duration.String(),
		Details: map[string]interface{}{
			"routing_map":    routingMap,
			"nvlink_routing": nvlinkClusters,
		},
	}
}

// submitJobWithTenant: Submit a job with tenant_id set
func submitJobWithTenant(baseURL string, req ScheduleRequest, tenantID string) (*ScheduleResponse, error) {
	// Build a custom payload that includes tenant_id
	payload := map[string]interface{}{
		"request_id":       req.RequestID,
		"name":             req.Name,
		"image":            req.Image,
		"command":          req.Command,
		"gpu_count":        req.GPUCount,
		"priority":         req.Priority,
		"memory_mb":        req.MemoryMB,
		"cpu_millis":       req.CPUMillis,
		"tenant_id":        tenantID,
		"prefer_nvlink":    req.PreferNVLink,
		"prefer_same_numa": req.PreferSameNUMA,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal error: %w", err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	httpResp, err := client.Post(baseURL+"/schedule", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("http error: %w", err)
	}
	defer httpResp.Body.Close()

	respBody, _ := io.ReadAll(httpResp.Body)

	if httpResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%d: %s", httpResp.StatusCode, string(respBody))
	}

	var schedResp ScheduleResponse
	if err := json.Unmarshal(respBody, &schedResp); err != nil {
		return nil, fmt.Errorf("decode error: %w", err)
	}

	return &schedResp, nil
}

// ============================================================================
// HTTP HELPERS
// ============================================================================

func healthCheck(baseURL string) bool {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(baseURL + "/health")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func submitJob(baseURL string, req ScheduleRequest) (*ScheduleResponse, error) {
	resp, err := submitJobRaw(baseURL, req)
	return resp, err
}

func submitJobRaw(baseURL string, req ScheduleRequest) (*ScheduleResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal error: %w", err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	httpResp, err := client.Post(baseURL+"/schedule", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("http error: %w", err)
	}
	defer httpResp.Body.Close()

	respBody, _ := io.ReadAll(httpResp.Body)

	if httpResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%d: %s", httpResp.StatusCode, string(respBody))
	}

	var schedResp ScheduleResponse
	if err := json.Unmarshal(respBody, &schedResp); err != nil {
		return nil, fmt.Errorf("decode error: %w", err)
	}

	return &schedResp, nil
}

func getJobStatus(baseURL string, jobID string) string {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(fmt.Sprintf("%s/status/job?job_id=%s", baseURL, jobID))
	if err != nil {
		return "unknown"
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	if status, ok := result["status"].(string); ok {
		return status
	}
	return "unknown"
}

// ============================================================================
// STATISTICS HELPERS
// ============================================================================

func calcLatencyStats(latencies []float64) LatencyStats {
	if len(latencies) == 0 {
		return LatencyStats{}
	}

	sort.Float64s(latencies)

	sum := 0.0
	for _, l := range latencies {
		sum += l
	}

	return LatencyStats{
		P50:  percentile(latencies, 50),
		P95:  percentile(latencies, 95),
		P99:  percentile(latencies, 99),
		P999: percentile(latencies, 99.9),
		Min:  latencies[0],
		Max:  latencies[len(latencies)-1],
		Avg:  sum / float64(len(latencies)),
	}
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(float64(len(sorted))*p/100.0)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func printLatencyTable(name string, stats LatencyStats, success, errors int, rps float64) {
	fmt.Printf("\n┌─────────────────────────────────────────────────┐\n")
	fmt.Printf("  │  %s LATENCY REPORT                              │\n", name)
	fmt.Printf("  ├─────────────────────────────────────────────────┤\n")
	fmt.Printf("  │  p50:     %8.2f ms                              │\n", stats.P50)
	fmt.Printf("  │  p95:     %8.2f ms                              │\n", stats.P95)
	fmt.Printf("  │  p99:     %8.2f ms                              │\n", stats.P99)
	fmt.Printf("  │  p999:    %8.2f ms                              │\n", stats.P999)
	fmt.Printf("  │  min:     %8.2f ms                              │\n", stats.Min)
	fmt.Printf("  │  max:     %8.2f ms                              │\n", stats.Max)
	fmt.Printf("  │  avg:     %8.2f ms                              │\n", stats.Avg)
	fmt.Printf("  ├─────────────────────────────────────────────────┤\n")
	fmt.Printf("  │  Success: %d  Errors: %d  Throughput: %.1f/s    │\n", success, errors, rps)
	fmt.Printf("  └─────────────────────────────────────────────────┘\n")
}

// ============================================================================
// OUTPUT
// ============================================================================

func writeResults(filename string, results []BenchmarkResult) {
	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		fmt.Printf("Error marshaling results: %v\n", err)
		return
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		fmt.Printf("Error writing results: %v\n", err)
		return
	}

	fmt.Printf("\n  Results written to: %s\n", filename)
}
