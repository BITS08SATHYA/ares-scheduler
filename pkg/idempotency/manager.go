// Request deduplication for exactly-once job execution
// Implements: Feature 6 (Exactly-Once), Feature 18 (Idempotent Submission)
// Depends on: types.go, logger.go, redis/client.go

package idempotency

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/redis"
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
		ttl:         defaultTTL,
	}
}

// defaultTTL is used when no WithTTL option is supplied. It must exceed the
// longest typical job runtime so retry-after-completion still hits the cache.
const defaultTTL = 24 * time.Hour

// ComputeTTL returns the idempotency cache TTL for a job with the given
// timeoutSecs. TTLs shorter than defaultTTL are clamped up, and a one-hour
// safety margin is added so a retry that arrives just after a job finishes
// still sees the cached result.
//
// For example, a 30-hour training job gets TTL = 31h, not the 24h default —
// without this, its 25h-retry would re-execute the job.
func ComputeTTL(timeoutSecs int) time.Duration {
	if timeoutSecs <= 0 {
		return defaultTTL
	}
	ttl := time.Duration(timeoutSecs)*time.Second + time.Hour
	if ttl < defaultTTL {
		return defaultTTL
	}
	return ttl
}

// RecordOption customises a single idempotency operation. Use WithTTL to
// override the per-call cache duration (e.g. for long-running jobs).
type RecordOption func(*recordConfig)

type recordConfig struct {
	ttl time.Duration
}

// WithTTL overrides the dedup cache TTL for this call only. Pass
// ComputeTTL(job.Spec.TimeoutSecs) from the orchestrator.
func WithTTL(ttl time.Duration) RecordOption {
	return func(c *recordConfig) {
		if ttl > 0 {
			c.ttl = ttl
		}
	}
}

func (im *IdempotencyManager) applyOptions(opts []RecordOption) recordConfig {
	cfg := recordConfig{ttl: im.ttl}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
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
		// CRITICAL: Redis failure breaks idempotency guarantees.
		// If we return "not a duplicate", every retry creates a new job.
		// Better to fail the request and let the client retry with
		// the same request ID once Redis recovers.
		im.log.Error("Idempotency check failed (Redis unreachable): %v", err)
		return nil, false, fmt.Errorf("idempotency check unavailable (Redis error): %w", err)
	}

	// Cache hit: request was already processed
	if cached != "" {
		var result IdempotencyResult
		err := json.Unmarshal([]byte(cached), &result)
		if err != nil {
			im.log.Error("Failed to unmarshal cached result: %v", err)
			return nil, false, nil // Treat as miss on unmarshal error
		}

		im.log.Info("Dedup cache hit for request %s (job=%s, status=%s)", requestID, result.JobID, result.Status)
		return &result, true, nil
	}

	// Cache miss: this is a new request
	im.log.Info("Dedup cache miss for request %s", requestID)
	return nil, false, nil
}

// CheckAndReserve: Atomically check for duplicate AND reserve the request ID.
// Uses Redis SetNX (SET if Not eXists) — a single atomic operation that
// eliminates the race window between CheckDuplicate and RecordSuccess.
//
// The jobID parameter is stored in the placeholder so that concurrent
// duplicates immediately see the real job ID (not a generic "pending").
//
// Returns: (previous result if duplicate, was duplicate, error)
// - If new request: reserves it atomically, returns (nil, false, nil)
// - If duplicate: returns (cached result, true, nil)
// - If Redis error: returns (nil, false, error) — caller should fail the request
func (im *IdempotencyManager) CheckAndReserve(ctx context.Context, requestID string, jobID string, opts ...RecordOption) (*IdempotencyResult, bool, error) {
	if requestID == "" {
		return nil, false, fmt.Errorf("request ID cannot be empty")
	}

	cfg := im.applyOptions(opts)
	dedupeKey := fmt.Sprintf("ares:idempotency:%s", requestID)

	// Store the real job ID in the placeholder so duplicates get it immediately
	placeholder := &IdempotencyResult{
		JobID:      jobID,
		Status:     "reserving",
		SubmitTime: time.Now(),
	}
	placeholderJSON, err := json.Marshal(placeholder)
	if err != nil {
		return nil, false, fmt.Errorf("marshal failed: %w", err)
	}

	// Atomic SetNX: only ONE concurrent request wins this race
	wasSet, err := im.redisClient.SetNX(ctx, dedupeKey, string(placeholderJSON), cfg.ttl)
	if err != nil {
		im.log.Error("Idempotency atomic reserve failed (Redis unreachable): %v", err)
		return nil, false, fmt.Errorf("idempotency check unavailable (Redis error): %w", err)
	}

	if wasSet {
		// We won the race — this is a new, unique request
		im.log.Info("Dedup atomic reserve: request %s reserved (new request)", requestID)
		return nil, false, nil
	}

	// SetNX returned false — key already exists, this is a duplicate
	// Read the existing entry to return cached result
	cached, err := im.redisClient.Get(ctx, dedupeKey)
	if err != nil {
		im.log.Error("Failed to read existing dedup entry: %v", err)
		return nil, true, nil // We know it's a duplicate even if we can't read the cached result
	}

	if cached != "" {
		var result IdempotencyResult
		if err := json.Unmarshal([]byte(cached), &result); err == nil {
			im.log.Info("Dedup atomic reserve: request %s is duplicate (job=%s)", requestID, result.JobID)
			return &result, true, nil
		}
	}

	// Key exists but couldn't parse — still a duplicate
	im.log.Info("Dedup atomic reserve: request %s is duplicate (cached result unreadable)", requestID)
	return nil, true, nil
}

// ============================================================================
// RECORDING OPERATIONS (Feature 6, 18)
// ============================================================================

// RecordSuccess: Record a successful job submission in dedup cache
// Call this after job is successfully created
func (im *IdempotencyManager) RecordSuccess(ctx context.Context, requestID string, jobID string, opts ...RecordOption) error {
	if requestID == "" || jobID == "" {
		return fmt.Errorf("request ID and job ID cannot be empty")
	}

	cfg := im.applyOptions(opts)
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

	err = im.redisClient.Set(ctx, dedupeKey, string(resultJSON), cfg.ttl)
	if err != nil {
		im.log.Error("Failed to store dedup result: %v", err)
		return fmt.Errorf("store failed: %w", err)
	}

	im.log.Info("Recorded dedup entry: request=%s, job=%s", requestID, jobID)
	return nil
}

// RecordCompletion: Record job completion in dedup cache
// Call this when job finishes (success or failure)
func (im *IdempotencyManager) RecordCompletion(ctx context.Context, requestID string, jobID string, status string, result string, opts ...RecordOption) error {
	if requestID == "" || jobID == "" {
		return fmt.Errorf("request ID and job ID cannot be empty")
	}

	cfg := im.applyOptions(opts)
	dedupeKey := fmt.Sprintf("ares:idempotency:%s", requestID)

	// Preserve original submit time from existing cache entry if available
	submitTime := time.Now()
	if cached, err := im.redisClient.Get(ctx, dedupeKey); err == nil && cached != "" {
		var existing IdempotencyResult
		if err := json.Unmarshal([]byte(cached), &existing); err == nil && !existing.SubmitTime.IsZero() {
			submitTime = existing.SubmitTime
		}
	}

	completion := &IdempotencyResult{
		JobID:          jobID,
		Status:         status, // "succeeded" or "failed"
		SubmitTime:     submitTime,
		CompletionTime: time.Now(),
		Result:         result,
	}

	completionJSON, err := json.Marshal(completion)
	if err != nil {
		im.log.Error("Failed to marshal completion: %v", err)
		return fmt.Errorf("marshal failed: %w", err)
	}

	err = im.redisClient.Set(ctx, dedupeKey, string(completionJSON), cfg.ttl)
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
		if (ch < 'a' || ch > 'z') &&
			(ch < 'A' || ch > 'Z') &&
			(ch < '0' || ch > '9') &&
			ch != '-' && ch != '_' && ch != ':' {
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
	im.log.Info("Counting dedup cache entries (would use SCAN in production)")
	return 0, nil
}

// ============================================================================
// IDEMPOTENCY WITH JOB LIFECYCLE (Feature 6 + 17)
// ============================================================================

// RecordJobTransition: Record job state transition in dedup cache
// Call this when job changes status. TTL defaults to the manager default;
// callers with long-running jobs should pass WithTTL(ComputeTTL(job.Spec.TimeoutSecs)).
func (im *IdempotencyManager) RecordJobTransition(ctx context.Context, requestID string, job *common.Job, opts ...RecordOption) error {
	cfg := im.applyOptions(opts)
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

	err = im.redisClient.Set(ctx, dedupeKey, string(resultJSON), cfg.ttl)
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
