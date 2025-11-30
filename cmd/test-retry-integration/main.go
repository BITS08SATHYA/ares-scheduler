package main

import (
	"context"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/job/to_delete"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"strings"
	"time"
)

func main() {

	log.Println(" Full Retry Integration Test")
	log.Println(strings.Repeat("=", 6))

	//	Connect to etcd
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		log.Fatal(err)
	}

	defer cli.Close()

	//	Create Scheduler
	sched, err := scheduler.NewScheduler([]string{"localhost:2379"})
	if err != nil {
		log.Fatal(err)
	}

	defer sched.Close()

	ctx := context.Background()

	//	Submit a job
	log.Println("\n Submitting job with retry policy")
	jobID, err := sched.SubmitJob(ctx, to_delete.JobSubmission{
		RequestID:  fmt.Sprintf("retry-integration=%d", time.Now().Unix()),
		Name:       "retry-test-job",
		Image:      "alpine:latest",
		Command:    []string{"sh", "-c", "exit 1"},
		CPUs:       1,
		GPUs:       0,
		MaxRetries: 3,
	})

	if err != nil {
		log.Fatal("Failed: %v ", err)
	}

	log.Printf("Job Submitted: %s", jobID)

	//	Watch job state transition
	log.Printf("\n Watching job state transitions...")
	log.Printf(" (Start worker in another terminal to see retries)")
	log.Println(" Expected: PENDING -> RUNNING -> RETRYING -> RUNNING -> RETRYING -> RUNNING -> FAILED ")

	stateKey := fmt.Sprintf("/state/%s", jobID)
	watchChan := cli.Watch(ctx, stateKey)

	timeout := time.After(60 * time.Second)
	stateChanges := []string{}

	for {
		select {
		case watchResp := <-watchChan:
			for _, event := range watchResp.Events {
				state := string(event.Kv.Value)
				stateChanges = append(stateChanges, state)
				log.Printf(" State: %s", state)

				if state == string(to_delete.StateFailed) {
					log.Println("\n Final State: Failed (expected after retries)")
					goto done
				}
			}
		case <-timeout:
			log.Println("\n Timeout Reached")
			goto done
		}
	}

done:
	//		Get final Job details
	log.Println("\n Final Job Details: ")
	j, _ := sched.GetJob(ctx, jobID)
	if j != nil {
		log.Printf("  JobID: %s", j.JobID)
		log.Printf("  State: %s", j.State)
		log.Printf("  Retry Count: %d/%d", j.RetryCount, j.MaxRetries)
		log.Printf("  Last Failure: %s", j.LastFailure)
	}

	// Summary
	log.Println("\n" + strings.Repeat("=", 60))
	log.Println("State Transitions Observed:")
	for i, state := range stateChanges {
		log.Printf("  %d. %s", i+1, state)
	}

	log.Println("\n Feature #21 Integration Test Complete!")
	log.Println("To see full retry flow, run worker in another terminal:")
	log.Println("  $ go run cmd/worker/main.go")

}
