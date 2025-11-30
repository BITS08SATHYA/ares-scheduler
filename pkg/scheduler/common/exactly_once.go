package common

import (
	"context"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage"
	"time"
)

// Feature #6: Exactly-Once Job Execution
// Pattern: etcd leases for coordination + Redis for deduplication cache

// Exactly-Once Semantics
// ExactlyOnceEngine ensures job run exactly once despite failures/retries

type ExactlyOnceEngine struct {
	storage *storage.Client

	//	Configuration
	leaseTTL              time.Duration // How long a job can hold execution lease
	deduplicationCacheTTL time.Duration // How long to remember request IDs
	maxRetries            int           // Maximum retry attempts
}

// NewExactlyOnceEngine creates engine
func NewExactlyOnceEngine(st *storage.Client) *ExactlyOnceEngine {
	return &ExactlyOnceEngine{
		storage:               st,
		leaseTTL:              30 * time.Second, // Job Execution Lease
		deduplicationCacheTTL: 24 * time.Hour,   // Remember for 1 day
		maxRetries:            3,
	}
}

// CORE: Check for Duplicate Submission
// CheckIfAlreadySubmitted checks if a request_id was already processed
// This is the FAST path (redis deduplication cache)
func (e *ExactlyOnceEngine) CheckIfAlreadySubmitted(ctx context.Context, requestID string) (bool, string, error) {

	/*
		Strategy:
		1. Check Redis Cache (fast path) - should be hot for recent updates
		2. If miss: Check etcd for historical records (slow path)
		3. Cache Result in Redis
	*/

	cacheKey := fmt.Sprintf("request:%s", requestID)

	// Fast Path: Check Redis Cache
	existingJobID, exists := e.c

}

// checkRedisCache tries to get from redis
func (e *ExactlyOnceEngine) checkRedisCache(ctx context.Context, cacheKey string) (string, bool) {
	val, err := e.storage.redis.Get(ctx, cacheKey).Result()
	if err != nil {
		return "", false // Not in cache
	}
	return val, true
}

// CheckEtcdHistory checks etcd for job record
func (e *ExactlyOnceEngine) checkEtcdHistory(ctx context.Context, requestID string) (string, error) {
	key := fmt.Sprintf("/ares/request_ids/%s", requestID)
	var jobID string
}
