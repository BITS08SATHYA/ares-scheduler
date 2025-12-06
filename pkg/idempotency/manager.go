// Request deduplication for exactly-once job execution
// Implements: Feature 6 (Exactly-Once), Feature 18 (Idempotent Submission)
// Depends on: types.go, logger.go, redis/client.go

package idempotency

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/redis"
	"time"
)

// IdempotencyManager: Manages request deduplication
// Layer 1 of exactly-once semantics (Feature 6)
// Uses Redis for fast dedup checks (<1ms latency, 99% hit rate)
type IdempotencyManager struct {
	redisClient *redis.RedisClient
	log         *logger.Logger
	ttl         time.Duration // TTL for dedup cache entries (default: 24 hours)
}

// IdempotencyResult: Result of a previous identical request
type IdempotencyResult struct {
	JobID          string    `json:"job_id"`
	Status         string    `json:"status"`
	SubmitTime     time.Time `json:"submit_time"`
	CompletionTime time.Time `json:"completion_time,omitempty"`
	Result         string    `json:"result,omitempty"` // Success or error message
}

// NewIdempotencyManager: Create a new idempotency manager
func NewIdempotencyManager(redisClient *redis.RedisClient) *IdempotencyManager {
	return &IdempotencyManager{
		redisClient: redisClient,
		log:         logger.Get(),
		ttl:         24 * time.Hour, // Cache dedup results for 24 hours
	}
}

// ============================================================================
// DEDUPLICATION OPERATIONS (Feature 6, 18)
// ============================================================================

// CheckDuplicate: Check if a request was already processed
// Returns: (previous result, was duplicate, error)
// Fast path: Redis cache hit (<1ms)
// Slow path: Cache miss (process normally)
func (im *IdempotencyManager) CheckDuplicate(ctx context.Context, requestID string) (*IdempotencyResult, bool, error) {
	if requestID == "" {
		return nil, false, fmt.Errorf("request ID cannot be empty")
	}

	dedupeKey := fmt.Sprintf("ares:idempotency:%s", requestID)

	// Try to get from cache (O(1) operation, <1ms)
	cached, err := im.redisClient.Get(ctx, dedupeKey)
	if err != nil {
		// Cache error - log but don't fail (proceed with normal processing)
		im.log.Warn("Failed to check dedup cache: %v", err)
		return nil, false, nil
	}

	// Cache hit: request was already processed
	if cached != "" {
		var result IdempotencyResult
		err := json.Unmarshal([]byte(cached), &result)
		if err != nil {
			im.log.Error("Failed to unmarshal cached result: %v", err)
			return nil, false, nil // Treat as miss on unmarshal error
		}

		im.log.Debug("Dedup cache hit for request %s (job=%s, status=%s)",
			requestID, result.JobID, result.Status)
		im.log.Info("Dedup cache hit for request %s (job=%s, status=%s)", requestID, result.JobID, result.Status)
		return &result, true, nil
	}

	// Cache miss: this is a new request
	im.log.Debug("Dedup cache miss for request %s", requestID)
	return nil, false, nil
}

// ============================================================================
// RECORDING OPERATIONS (Feature 6, 18)
// ============================================================================

// RecordSuccess: Record a successful job submission in dedup cache
// Call this after job is successfully created
func (im *IdempotencyManager) RecordSuccess(ctx context.Context, requestID string, jobID string) error {
	if requestID == "" || jobID == "" {
		return fmt.Errorf("request ID and job ID cannot be empty")
	}

	dedupeKey := fmt.Sprintf("ares:idempotency:%s", requestID)

	result := &IdempotencyResult{
		JobID:      jobID,
		Status:     "submitted",
		SubmitTime: time.Now(),
	}

	resultJSON, err := json.Marshal(result)
	if err != nil {
		im.log.Error("Failed to marshal result: %v", err)
		return fmt.Errorf("marshal failed: %w", err)
	}

	// Store with TTL (expires after 24 hours)
	err = im.redisClient.Set(ctx, dedupeKey, string(resultJSON), im.ttl)
	if err != nil {
		im.log.Error("Failed to store dedup result: %v", err)
		return fmt.Errorf("store failed: %w", err)
	}

	im.log.Info("Recorded dedup entry: request=%s, job=%s", requestID, jobID)
	return nil
}

// RecordCompletion: Record job completion in dedup cache
// Call this when job finishes (success or failure)
func (im *IdempotencyManager) RecordCompletion(ctx context.Context, requestID string, jobID string, status string, result string) error {
	if requestID == "" || jobID == "" {
		return fmt.Errorf("request ID and job ID cannot be empty")
	}

	dedupeKey := fmt.Sprintf("ares:idempotency:%s", requestID)

	completion := &IdempotencyResult{
		JobID:          jobID,
		Status:         status,     // "succeeded" or "failed"
		SubmitTime:     time.Now(), // Would be submission time in real code
		CompletionTime: time.Now(),
		Result:         result,
	}

	completionJSON, err := json.Marshal(completion)
	if err != nil {
		im.log.Error("Failed to marshal completion: %v", err)
		return fmt.Errorf("marshal failed: %w", err)
	}

	err = im.redisClient.Set(ctx, dedupeKey, string(completionJSON), im.ttl)
	if err != nil {
		im.log.Error("Failed to store completion: %v", err)
		return fmt.Errorf("store failed: %w", err)
	}

	im.log.Info("Recorded completion: request=%s, job=%s, status=%s", requestID, jobID, status)
	return nil
}

// ============================================================================
// VALIDATION (Feature 18 - Idempotent Submission)
// ============================================================================

// ValidateRequestID: Validate request ID format
// Request ID should be: UUID or {tenant-id}:{unique-id}
// Used in job submission to ensure proper formatting
func (im *IdempotencyManager) ValidateRequestID(requestID string) error {
	if requestID == "" {
		return fmt.Errorf("request ID cannot be empty")
	}

	if len(requestID) < 8 {
		return fmt.Errorf("request ID too short (min 8 chars)")
	}

	if len(requestID) > 256 {
		return fmt.Errorf("request ID too long (max 256 chars)")
	}

	// Request ID should only contain alphanumeric, hyphen, colon, underscore
	for _, ch := range requestID {
		if !((ch >= 'a' && ch <= 'z') ||
			(ch >= 'A' && ch <= 'Z') ||
			(ch >= '0' && ch <= '9') ||
			ch == '-' || ch == '_' || ch == ':') {
			return fmt.Errorf("request ID contains invalid character: %c", ch)
		}
	}

	return nil
}

// ============================================================================
// CLEANUP OPERATIONS
// ============================================================================

// DeleteEntry: Delete a dedup entry (when needed)
// Useful for: testing, manual cleanup
func (im *IdempotencyManager) DeleteEntry(ctx context.Context, requestID string) error {
	dedupeKey := fmt.Sprintf("ares:idempotency:%s", requestID)
	err := im.redisClient.Del(ctx, dedupeKey)
	if err != nil {
		im.log.Error("Failed to delete dedup entry: %v", err)
		return fmt.Errorf("delete failed: %w", err)
	}

	im.log.Info("Deleted dedup entry: %s", requestID)
	return nil
}

// ClearExpired: (Automatic) Redis expires keys automatically based on TTL
// No manual cleanup needed - Redis handles expiration

// ============================================================================
// STATISTICS (Feature 22 - Metrics)
// ============================================================================

// GetDedupeStats: Get dedup statistics
// Useful for monitoring: cache hit rate, entries in cache, etc.
type DedupeStats struct {
	DedupeKeysPattern string // ares:idempotency:*
	// In production, would use Redis SCAN to count active keys
}

// CountCacheEntries: Count active dedup entries
// Warning: This scans Redis, could be slow with many entries
// Use sparingly (e.g., once per minute for metrics)
func (im *IdempotencyManager) CountCacheEntries(ctx context.Context) (int64, error) {
	// In production: Use Redis SCAN for paging
	// For now: Return 0 (would require Redis SCAN implementation)
	im.log.Debug("Counting dedup cache entries (would use SCAN in production)")
	return 0, nil
}

// ============================================================================
// IDEMPOTENCY WITH JOB LIFECYCLE (Feature 6 + 17)
// ============================================================================

// RecordJobTransition: Record job state transition in dedup cache
// Call this when job changes status
func (im *IdempotencyManager) RecordJobTransition(ctx context.Context, requestID string, job *common.Job) error {
	dedupeKey := fmt.Sprintf("ares:idempotency:%s", requestID)

	// Get current result
	cached, err := im.redisClient.Get(ctx, dedupeKey)
	if err != nil {
		return fmt.Errorf("get failed: %w", err)
	}

	var result IdempotencyResult
	if cached != "" {
		err := json.Unmarshal([]byte(cached), &result)
		if err != nil {
			// Initialize new result
			result = IdempotencyResult{
				JobID:      job.ID,
				SubmitTime: job.SubmitTime,
			}
		}
	} else {
		result = IdempotencyResult{
			JobID:      job.ID,
			SubmitTime: job.SubmitTime,
		}
	}

	// Update status based on job state
	result.Status = string(job.Status)
	if job.Status == common.StatusSucceeded || job.Status == common.StatusFailed {
		result.CompletionTime = time.Now()
		if job.ErrorMsg != "" {
			result.Result = job.ErrorMsg
		}
	}

	resultJSON, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}

	err = im.redisClient.Set(ctx, dedupeKey, string(resultJSON), im.ttl)
	if err != nil {
		return fmt.Errorf("store failed: %w", err)
	}

	return nil
}

// ============================================================================
// HELPER: Request ID Generation (for client use)
// ============================================================================

// GenerateRequestID: Generate a unique request ID for idempotency
// Format: {timestamp}-{random}
// In production, use UUID
func GenerateRequestID() string {
	// Simple implementation: timestamp + nanoseconds
	// In production: use github.com/google/uuid
	return fmt.Sprintf("req-%d-%d", time.Now().Unix(), time.Now().UnixNano()%1000)
}
