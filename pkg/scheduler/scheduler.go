package main

import (
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

// Scheduler orchestrates job submission and execution
type Scheduler struct {
	etcd *clientv3.Client
}

// NewScheduler creates a new scheduler instance
func NewScheduler(etcdEndpoints []string) (*Scheduler, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		return nil, fmt.Errorf("Failed to connect to etcd: %w", err)
	}

	return &Scheduler{
		etcd: cli,
	}, nil
}

// Closes the etcd client connection
func (s *Scheduler) Close() error {
	return s.etcd.Close()
}

// SubmitJob submits a new Job with idempotency guarantee
func (s *Scheduler) SubmitJob(ctx context.Context, req JobSubmission) (string, error) {

}
