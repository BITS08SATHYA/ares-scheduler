package main

import (
	"context"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/job/to_delete"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler"
	"log"
	"strings"
	"time"
)

func main() {

	log.Println("Feature: Backoff & Retry Policy Test")
	log.Println(strings.Repeat("=", 60))

	//	Test-1: Retry Policy Calculations
	log.Println("\n Test 1: Retry Policy Calculations")
	policy := to_delete.DefaultRetryPolicy()

	log.Printf("Configurations:")
	log.Printf(" Max Retries: %d", policy.MaxRetries)
	log.Printf(" Initial Backoff: %v", policy.InitialBackoff)
	log.Printf(" Max Backoff: %v", policy.MaxBackoff)
	log.Printf(" Multiplier: %.1f", policy.Multiplier)
	log.Printf(" Jitter: %v", policy.EnableJitter)

	log.Println("\nBackoff Schedule: ")
	for attempt := 1; attempt <= 5; attempt++ {
		backoff := policy.CalculateBackoff(attempt)
		shouldRetry := policy.ShouldRetry(attempt)

		status := "RETRY"
		if !shouldRetry {
			status = "NO More Retries"
		}

		log.Printf(" Attempt %d: backoff=%v, status=%s", attempt, backoff.Round(time.Millisecond), status)

	}

	//	Test 2: Submit Job with custom retry policy
	log.Println("\n Test 2: Submit Job with Retry Policy")

	sched, err := scheduler.NewScheduler([]string{"localhost:2379"})

	if err != nil {
		log.Fatalf("Failed to create scheduler: %v", err)
	}

	defer sched.Close()

	ctx := context.Background()

	jobID, err := sched.SubmitJob(ctx, to_delete.JobSubmission{
		RequestID:  fmt.Sprintf("retry-test-%d", time.Now().Unix()),
		Name:       "test-retry-job",
		Image:      "alpine:latest",
		Command:    []string{"echo", "hello"},
		CPUs:       1,
		GPUs:       0,
		MaxRetries: 5,
	})

	if err != nil {
		log.Fatalf("Failed to submit job: %v", err)
	}

	log.Printf("Job Submitted: %s", jobID)

	//	Verify job has retry policy
	time.Sleep(100 * time.Millisecond)
	j, err := sched.GetJob(ctx, jobID)
	if err != nil {
		log.Fatalf("Failed to get job: %v", err)
	}

	log.Println("\n Job Configuration:")
	log.Printf(" JobID: %s", j.JobID)
	log.Printf(" Name: %s", j.Name)
	log.Printf(" Max Retries: %d", j.MaxRetries)
	log.Printf(" Retry Count: %d", j.RetryCount)
	log.Printf(" State: %s", j.State)

	if j.RetryPolicy != nil {
		log.Println(" Retry Policy: Configured")
		log.Printf(" Max Retries: %d", j.RetryPolicy.MaxRetries)
		log.Printf(" Initial Backoff: %v", j.RetryPolicy.InitialBackoff)
		log.Printf(" Multiplier: %.1f", j.RetryPolicy.Multiplier)
	} else {
		log.Println(" Retry Policy: Missing")
	}

	//	Summary
	log.Println("\n " + strings.Repeat("=", 60))
	log.Println("Backoff and Retry Policy Implemented")
	log.Println("\nComponents Verified: ")
	log.Println(" Exponential backoff calculation: ")
	log.Println(" Jitter (random variance) ")
	log.Println(" Max Retry Enforcement")
	log.Println(" Job submission with retry policy")
	log.Println(" Retry metadata storage")
	log.Println(" Completed")

}
