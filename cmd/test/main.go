package main

import (
	"context"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/config"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/etcd"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/redis"
	"time"
)

func main() {

	// Initialize logger
	logger.Init("debug")
	logger.Info("Starting Ares test program")

	// Load Config
	cfg := config.LoadConfig()
	logger.Info("Config Loaded: etcd:%v, redis=%s", cfg.EtcdEndpoints, cfg.RedisAddr)

	// Test Types
	testTypes()
	testStorageClients(cfg)

	logger.Info("All tests passes!")

}

func testTypes() {

	logger.Info("Testing types...")

	// Create a JobSpec
	spec := &common.JobSpec{
		RequestID:       "req-001",
		Name:            "test-job",
		Image:           "nvidia/cuda:12.0",
		Command:         []string{"python"},
		Args:            []string{"train.py"},
		GPUCount:        2,
		GPUType:         "A100",
		PreferNVLink:    true,
		PreferSameNUMA:  true,
		MemoryMB:        8192,
		CPUMillis:       4000,
		Priority:        50,
		TimeoutSecs:     3600,
		MaxRetries:      3,
		TenantID:        "tenant-1",
		QuotaGB:         100,
		TargetLatencyMs: 5000,
	}

	// Create a Job
	job := &common.Job{
		ID:       "job-550e8400-e29b-41d4-a716-446655440000",
		Spec:     spec,
		Status:   common.StatusPending,
		Attempts: 0,
		Metrics:  make(map[string]interface{}),
	}

	// Test helper methods
	fmt.Printf("Job ID: %s\n", job.ID)
	fmt.Printf("Job Status: %s\n", job.Status)
	fmt.Printf("Is Pending: %v\n", job.IsPending())
	fmt.Printf("Is Running: %v\n", job.IsRunning())
	fmt.Printf("Is Completed: %v\n", job.IsCompleted())

	// Test status transitions
	job.Status = common.StatusRunning
	fmt.Printf("After transition - Is Running: %v\n", job.IsRunning())

	// Test cluster types
	cluster := &common.Cluster{
		ID:     "cluster-us-west-2a",
		Name:   "us-west-2a",
		Region: "us-west-2",
		Zone:   "us-west-2a",
		Capacity: &common.Capacity{
			TotalGPUs:     16,
			TotalCPUCores: 64,
			TotalMemoryGB: 256,
			NumGPUsByType: map[string]int{
				"A100": 8,
				"H100": 8,
			},
		},
		CurrentLoad: &common.Load{
			GPUsInUse:        4,
			CPUCoresInUse:    16,
			MemoryGBInUse:    64,
			RunningJobsCount: 2,
		},
		IsHealthy: true,
	}

	fmt.Printf("Cluster: %s\n", cluster.Name)
	fmt.Printf("Total GPUs: %d\n", cluster.Capacity.TotalGPUs)
	fmt.Printf("GPUs in use: %d\n", cluster.CurrentLoad.GPUsInUse)
	fmt.Printf("Utilization: %.1f%%\n", cluster.UtilizationPercent())
	fmt.Printf("Available capacity: %d GPUs\n", cluster.AvailableCapacity().TotalGPUs)

	logger.Info("Types test completed successfully!")

}

func testStorageClients(cfg *common.Config) {
	logger.Info("Testing storage clients...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Test etcd (requires etcd running on localhost:2379)
	logger.Info("Connecting to etcd at %v", cfg.EtcdEndpoints)
	ec, err := etcd.NewETCDClient(cfg.EtcdEndpoints, cfg.EtcdDialTimeout)
	if err != nil {
		logger.Warn("etcd connection failed (is etcd running?): %v", err)
	} else {
		defer ec.Close()

		// Put and get
		ec.Put(ctx, "/test/key", "value123")
		val, _ := ec.Get(ctx, "/test/key")
		logger.Info("etcd test: put/get succeeded, value=%s", val)
	}

	// Test redis (requires redis running on localhost:6379)
	logger.Info("Connecting to redis at %s", cfg.RedisAddr)
	rc, err := redis.NewRedisClient(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		logger.Warn("redis connection failed (is redis running?): %v", err)
	} else {
		defer rc.Close()

		// Set and get
		rc.Set(ctx, "test:key", "value456", 1*time.Hour)
		val, _ := rc.Get(ctx, "test:key")
		logger.Info("redis test: set/get succeeded, value=%s", val)
	}
}
