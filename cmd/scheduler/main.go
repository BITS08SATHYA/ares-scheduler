package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/job"
	s "github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler"
)

func main() {

	log.Println("Starting Ares Scheduler....")

	//	Connect to etcd
	scheduler, err := s.NewScheduler([]string{"http://localhost:2379"})

	if err != nil {
		log.Fatalf("Failed to create a scheduler: %v", err)
	}

	defer scheduler.Close()

	log.Println("Connected to etcd")

	ctx := context.Background()

	//	Test 1: Submit first job
	log.Println("\n--- Test 1: Submit First Job ---")
	job1ID, err := scheduler.SubmitJob(ctx, job.JobSubmission{
		RequestID: "req-001",
		Name:      "ml-traning-v1",
		Image:     "pytorch:2.0",
		Command:   []string{"python", "train.py"},
		CPUs:      4,
		GPUs:      2,
	})

	if err != nil {
		log.Fatalf("Failed to submit job: %v", err)
	}
	log.Printf("Job Submitted: %s", job1ID)

	//	Test 2: Submit duplicated request (testing idempotency)
	job1IDRetry, err := scheduler.SubmitJob(ctx, job.JobSubmission{
		RequestID: "req-001", // Same RequestID!
		Name:      "ml-training-v1-retry",
		Image:     "pytorch:2.0",
		Command:   []string{"python", "train.py"},
		CPUs:      8,
		GPUs:      4,
	})
	if err != nil {
		log.Fatalf("Failed to submit duplicate: %v", err)
	}
	log.Printf("Duplicate request returned existing JobID: %s", job1IDRetry)

	if job1ID == job1IDRetry {
		log.Println("IDEMPOTENCY TEST PASSED! Same JobID returned")
	} else {
		log.Println("IDEMPOTENCY TEST FAILED! Different JobIDs")
	}

	//	Test 3: Submit Second unique job
	log.Println("\n--- Test 3: Submit Second Unique Job ---")
	job2ID, err := scheduler.SubmitJob(ctx, job.JobSubmission{
		RequestID: "req-002",
		Name:      "data-pipeline-v1",
		Image:     "python:3.11",
		Command:   []string{"python", "process.py"},
		CPUs:      2,
		GPUs:      0,
	})

	if err != nil {
		log.Fatalf("Failed to submit job 2: %v", err)
	}
	log.Printf("Job submitted: %s", job2ID)

	//	Test 4: Retrieve Job Details
	log.Println("\n--- Test 4: Retrieve Job Details ---")
	job, err := scheduler.GetJob(ctx, job1ID)
	if err != nil {
		log.Fatalf("Failed to get job: %v", err)
	}

	fmt.Printf("Job Details: \n")
	fmt.Printf(" ID: %s\n", job.JobID)
	fmt.Printf(" Name: %s\n", job.Name)
	fmt.Printf(" State: %s\n", job.State)
	fmt.Printf(" RequestID: %s\n", job.RequestID)
	fmt.Printf("  Image: %s\n", job.Image)
	fmt.Printf("  Command: %v\n", job.Command)
	fmt.Printf("  Created: %s\n", job.CreatedAt.Format(time.RFC3339))

	//	Test 5: List all pending jobs
	log.Println("\n--- Test 5: List All Pending Jobs ---")
	pendingJobs, err := scheduler.ListPendingJobs(ctx)
	if err != nil {
		log.Fatalf("Failed to list jobs: %v", err)
	}
	log.Printf("Found %d pending jobs", len(pendingJobs))
	for i, lj := range pendingJobs {
		log.Printf(" %d. %s (%s) - State: %s", i+1, lj.Name, lj.JobID, lj.State)
	}

	log.Println("\n--- All tests completed successfully ---")
	log.Println("Ares MVP Day 1 Completed")
}
