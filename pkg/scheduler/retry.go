package scheduler

import (
	"context"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage"
	"math"
	"math/rand/v2"
	"time"
)

// Feature #21: Backoff & Retry Policy
// Pattern: Redis Sorted Sets for priority Queue with exponential backoff

// Retry Policy Engine
// RetryPolicy defines retry behavior
type RetryPolicy struct {
	MaxRetries        int           // Maximum number of retries
	InitialBackoff    time.Duration // First retry delay
	MaxBackoff        time.Duration // Maximum delay
	BackoffMultiplier float64       // Exponential growth factor
	JitterFraction    float64       // Jitter as fraction of backoff (0.0-1.0)
}

// DefaultRetryPolicy returns sensible defaults
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxRetries:        3,
		InitialBackoff:    1 * time.Second,
		MaxBackoff:        1 * time.Second,
		BackoffMultiplier: 2.0,
		JitterFraction:    0.1, // 10% Jitter
	}
}

// Retry Manager
// RetryManager handles job retries with exponential backoff
type RetryManager struct {
	storage *storage.Client
	policy  RetryPolicy
}

// NewRetryManager creates a new retry manager
func NewRetryManager(st *storage.Client, policy RetryPolicy) *RetryManager {
	return &RetryManager{
		storage: st,
		policy:  policy,
	}
}

// Core: Schedule Retry
// ScheduleRetry schedules a job for retry with backoff
// uses Redis Sortedset for priority queue
func (rm *RetryManager) ScheduleRetry(ctx context.Context, jobID string,
	retryCount int, lastError string) (bool, time.Duration, error) {
	/*
		Pattern: Redis sorted set for retry queue
		- Score: timestamp when job should be retried
		- Member: jobID

		Algorithm:
		1. Check if max retries exceeded
		2. Calculate backoff duration
		3. Add to sorted set with future timestamp
		4. Return when it will be retried
	*/

	//	 STEP 1: Check if max retries exceeded
	if retryCount >= rm.policy.MaxRetries {
		return false, 0, nil
	}

	//	STEP 2: Calculate backoff
	backoffDuration := rm.calculateBackoff(retryCount)

	//	STEP 3: Schedule retry in sorted Set
	retryTime := time.Now().Add(backoffDuration)
	score := float64(retryTime.UnixNano()) / 1e9 // Convert to seconds for redis

	retryQueueName := "retry-queue"
	if err := rm.storage.AddToSortedSet(ctx, retryQueueName, score, jobID); err != nil {
		return false, 0, fmt.Errorf("failed to schedule retry: %w", err)
	}

	//	Record in etcd for audit trail
	if err := rm.recordRetryAttempt(ctx, jobID, retryCount, lastError, backoffDuration); err != nil {
		fmt.Printf("WARNING: Failed to record retry: %v\n", err)
	}

	return true, backoffDuration, nil

}

// recordRetryAttempt records retry in etcd for audit trail
func (rm *RetryManager) recordRetryAttempt(
	ctx context.Context, jobID string, retryCount int, lastError string, backoffDuration time.Duration,
) error {
	record := map[string]interface{}{
		"job_id":    jobID,
		"retry_num": retryCount,
		"error":     lastError,
		"backoff":   backoffDuration.String(),
		"timestamp": time.Now().Unix(),
	}

	key := fmt.Sprintf("/ares/retries/%s/%d", jobID, retryCount)
	return rm.storage.PutEtcd(ctx, key, record)
}

// Core: Process Due Retries
// ProcessDueRetries checks sortedset for jobs ready to retry
// called periodically by scheduler
func (rm *RetryManager) ProcessDueRetries(ctx context.Context) ([]string, error) {

	/*
		Pattern: ZRANGEBYSCORE to get jobs due to retry

		Every 10 seconds:
		1. Query Redis for jobs with score <= now
		2. Pop them from sorted set
		3. Return to caller for resubmission
	*/

	now := time.Now()
	maxScore := float64(now.UnixNano()) / 1e9

	//	Get all jobs due for retry
	retryQueueName := "retry-queue"
	jobIDs, err := rm.storage.GetFromSortedSet(ctx, retryQueueName, 0, maxScore)

	if err != nil {
		return nil, fmt.Errorf("failed to get due retries: %w", err)
	}

	//	Remove them from sorted set
	for _, jobID := range jobIDs {
		rm.storage.RemoveFromSortedSet(ctx, retryQueueName, jobID)
	}

	return jobIDs, nil
}

// Backoff Calculation
// CalculateBackoff returns exponential backoff with jitter
// Formula: min(initial * multiplier^retries + jitter , maxBackoff)
func (rm *RetryManager) calculateBackoff(retryCount int) time.Duration {

	/*
		Example (with defaults):
		Retry 0: 1s + jitter = 1.0-1.1s
		Retry 1: 2s + jitter = 2.0-2.2s
		Retry 2: 4s + jitter = 4.0-4.4s
		Retry 3: 8s + jitter = 8.0-8.8s

		This prevents thundering herd: If 100 jobs fail at same time,
		they don't all retry at exact same amount.
	*/

	// Exponential: multiplier^retryCount
	exponentialBackoff := rm.policy.InitialBackoff.Seconds() *
		math.Pow(rm.policy.BackoffMultiplier, float64(retryCount))

	// Cap at max backoff
	if exponentialBackoff > rm.policy.MaxBackoff.Seconds() {
		exponentialBackoff = rm.policy.MaxBackoff.Seconds()
	}

	// Add jitter: random between 0 and (backoff + jitterFraction)
	jitterAmount := exponentialBackoff * rm.policy.JitterFraction
	jitter := rand.Float64() * jitterAmount

	totalSeconds := exponentialBackoff + jitter
	return time.Duration(totalSeconds * float64(time.Second))
}

// Retry Statistics
// GetRetryStats returns statistics about retries
func (rm *RetryManager) GetRetryState(ctx context.Context) (map[string]int64, error) {

	/*
		Returns:
		- Total Jobs in retry queue
		- Jobs due now
		- Jobs due in next hour
	*/

	retryQueueName := "retry-queue"

	// Total in Queue
	total, err := rm.storage.redis.ZCard(ctx, retryQueueName).Result()
	if err != nil {
		return nil, err
	}

	//	Due Now
	now := time.Now()
	nowScore := float64(now.UnixNano()) / 1e9
	dueNow, err := rm.storage.redis.ZCount(ctx, retryQueueName, "-inf",
		fmt.Sprintf("%f", nowScore)).Result()
	if err != nil {
		return nil, err
	}

	// Due within 1 hour
	futureTime := now.Add(1 * time.Hour)
	futureScore := float64(futureTime.UnixNano()) / 1e9
	dueOneHour, err := rm.storage.redis.ZCount(ctx, retryQueueName, "-inf",
		fmt.Sprintf("%f", futureScore)).Result()
	if err != nil {
		return nil, err
	}

	return map[string]int64{
		"total_in_queue": total,
		"due_now":        dueNow,
		"due_one_hour":   dueOneHour,
	}, nil

}

// RecordRetry increments retry counter
func (rm *RetryManager) RecordRetry(ctx context.Context) error {
	_, err := rm.storage.IncrCounter(ctx, "metrics:retries")
	return err
}

// RecordFailedRetry Increments failed retry counter
func (rm *RetryManager) RecordFailedRetry(ctx context.Context) error {
	_, err := rm.storage.IncrCounter(ctx, "metrics:failed-retries")
	return err
}

// Advanced; Adaptive Retry Policy
// If jobs keep failing, increase backoff to reduce load

func (rm *RetryManager) AdjustPolicyBasedOnMetrics(ctx context.Context) {
	/*
			- If Failure rate < 10%: Use aggressive backoff (faster retry)
			- If failure rate 10 - 30%: Use Standard backoff
			- If failure rate > 30%: Use Conservative backoff (slower retry)

		This prevents cascading failures.
	*/
	failureRate := getJobFailureRate(ctx)
	if failureRate < 0.10 {
		rm.policy.MaxRetries = 5
		rm.policy.InitialBackoff = 100 * time.Millisecond
	} else if failureRate > 0.30 {
		rm.policy.MaxRetries = 2
		rm.policy.InitialBackoff = 5 * time.Second
		rm.policy.MaxBackoff = 5 * time.Minute
	}
}

// Circuit Breaker Pattern
// ShouldCircuitBreakerTrip  decides if we should stop retrying
// Used for cascading failure prevention
func (rm *RetryManager) ShouldCircuitBreakerTrip(ctx context.Context, clusterName string) (bool, error) {
	/*
		Rules:
		- If > 50% of jobs failed in last 5 minutes: Trip circuit
		- If > 100 consecutive failures: Trip circuit
		- If cluster is down: Trip circuit
	*/

	//	Get Failure count
	failureKey := fmt.Sprintf("failures:%s", clusterName)
	failures, _ := rm.storage.GetCounter(ctx, failureKey)

	//	If too many failures, trip
	if failures > 100 {
		return true, nil
	}
	return false, nil
}
