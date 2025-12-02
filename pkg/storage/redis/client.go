// Layer 2: Redis client wrapper (depends on types, logger)
package redis

import (
	"context"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/redis/go-redis/v9"
	"time"
)

// RedisClient: Wrapper around redis client
type RedisClient struct {
	cli *redis.Client
	log *logger.Logger
}

// NewRedisClient: Create a new Redis client
func NewRedisClient(addr, password string, db int) (*RedisClient, error) {
	cli := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := cli.Ping(ctx).Err()
	if err != nil {
		logger.Error("Failed to connect to Redis at %s: %v", addr, err)
		return nil, err
	}

	logger.Info("Connected to Redis at %s", addr)

	return &RedisClient{
		cli: cli,
		log: logger.Get(),
	}, nil
}

// Close: Close Redis connection
func (rc *RedisClient) Close() error {
	return rc.cli.Close()
}

// Set: Store a string value
func (rc *RedisClient) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	err := rc.cli.Set(ctx, key, value, ttl).Err()
	if err != nil {
		rc.log.Error("Failed to set key %s: %v", key, err)
		return err
	}
	rc.log.Debug("Set key: %s (TTL: %v)", key, ttl)
	return nil
}

// Get: Retrieve a string value
func (rc *RedisClient) Get(ctx context.Context, key string) (string, error) {
	val, err := rc.cli.Get(ctx, key).Result()
	if err == redis.Nil {
		rc.log.Debug("Key not found: %s", key)
		return "", nil
	}
	if err != nil {
		rc.log.Error("Failed to get key %s: %v", key, err)
		return "", err
	}
	rc.log.Debug("Got key: %s", key)
	return val, nil
}

// Del: Delete one or more keys
func (rc *RedisClient) Del(ctx context.Context, keys ...string) error {
	err := rc.cli.Del(ctx, keys...).Err()
	if err != nil {
		rc.log.Error("Failed to delete keys: %v", err)
		return err
	}
	rc.log.Debug("Deleted %d keys", len(keys))
	return nil
}

// Exists: Check if key exists
func (rc *RedisClient) Exists(ctx context.Context, key string) (bool, error) {
	count, err := rc.cli.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// ============================================================================
// SORTED SET OPERATIONS (For priority queue - Feature 21)
// ============================================================================

// ZAdd: Add member to sorted set with score
// Lower score = higher priority in queue
// Score format: -priority + (timestamp/1e10)
// Example: Job with priority 10 at time 1000 → score = -10 + 1e-7 = -9.99999999
func (rc *RedisClient) ZAdd(ctx context.Context, key string, member string, score float64) error {
	err := rc.cli.ZAdd(ctx, key, redis.Z{
		Score:  score,
		Member: member,
	}).Err()
	if err != nil {
		rc.log.Error("Failed to zadd %s: %v", key, err)
		return err
	}
	rc.log.Debug("Added to sorted set %s: %s (score: %f)", key, member, score)
	return nil
}

// ZPopMin: Pop member with minimum score (atomic dequeue)
// Returns: member, score, error
// Used for: Dequeue next job from priority queue
func (rc *RedisClient) ZPopMin(ctx context.Context, key string, count int) (map[string]float64, error) {
	results, err := rc.cli.ZPopMin(ctx, key, int64(count)).Result()
	if err != nil {
		rc.log.Error("Failed to zpopmin %s: %v", key, err)
		return nil, err
	}

	result := make(map[string]float64)
	for _, z := range results {
		result[z.Member.(string)] = z.Score
	}
	rc.log.Debug("Popped %d members from sorted set %s", len(result), key)
	return result, nil
}

// ZRange: Get members by index range
func (rc *RedisClient) ZRange(ctx context.Context, key string, start, stop int) ([]string, error) {
	results, err := rc.cli.ZRange(ctx, key, int64(start), int64(stop)).Result()
	if err != nil {
		rc.log.Error("Failed to zrange %s: %v", key, err)
		return nil, err
	}
	rc.log.Debug("Got %d members from sorted set %s", len(results), key)
	return results, nil
}

// ZCard: Get number of members in sorted set
func (rc *RedisClient) ZCard(ctx context.Context, key string) (int, error) {
	count, err := rc.cli.ZCard(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	return int(count), nil
}

// ZRem: Remove member from sorted set
func (rc *RedisClient) ZRem(ctx context.Context, key string, members ...string) error {
	err := rc.cli.ZRem(ctx, key, members).Err()
	if err != nil {
		return err
	}
	rc.log.Debug("Removed %d members from sorted set %s", len(members), key)
	return nil
}

// ============================================================================
// HASH OPERATIONS (For job details storage)
// ============================================================================

// HSet: Set hash field value
// Example: HSet("job:123", "status", "RUNNING")
func (rc *RedisClient) HSet(ctx context.Context, key string, field string, value interface{}) error {
	err := rc.cli.HSet(ctx, key, field, value).Err()
	if err != nil {
		rc.log.Error("Failed to hset %s:%s: %v", key, field, err)
		return err
	}
	rc.log.Debug("Set hash field: %s:%s", key, field)
	return nil
}

// HGet: Get hash field value
func (rc *RedisClient) HGet(ctx context.Context, key string, field string) (string, error) {
	val, err := rc.cli.HGet(ctx, key, field).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		rc.log.Error("Failed to hget %s:%s: %v", key, field, err)
		return "", err
	}
	return val, nil
}

// HGetAll: Get all fields and values from hash
func (rc *RedisClient) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	vals, err := rc.cli.HGetAll(ctx, key).Result()
	if err != nil {
		rc.log.Error("Failed to hgetall %s: %v", key, err)
		return nil, err
	}
	return vals, nil
}

// ============================================================================
// ATOMIC OPERATIONS (Feature 6 - Layer 1: Idempotency)
// ============================================================================

// SetNX: Set if not exists (atomic)
// Used for: Request ID deduplication
// Returns: true if set, false if already existed
func (rc *RedisClient) SetNX(ctx context.Context, key string, value interface{}, ttl time.Duration) (bool, error) {
	ok, err := rc.cli.SetNX(ctx, key, value, ttl).Result()
	if err != nil {
		rc.log.Error("Failed to setnx %s: %v", key, err)
		return false, err
	}
	rc.log.Debug("SetNX on key %s: success=%v", key, ok)
	return ok, nil
}

// GetSet: Atomic get-and-set
// Returns previous value
func (rc *RedisClient) GetSet(ctx context.Context, key string, value interface{}) (string, error) {
	val, err := rc.cli.GetSet(ctx, key, value).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return val, nil
}

// Incr: Increment integer value (atomic)
func (rc *RedisClient) Incr(ctx context.Context, key string) (int64, error) {
	val, err := rc.cli.Incr(ctx, key).Result()
	if err != nil {
		rc.log.Error("Failed to incr %s: %v", key, err)
		return 0, err
	}
	return val, nil
}

// ============================================================================
// KEY PATTERN OPERATIONS
// ============================================================================

// Keys: Get all keys matching pattern
func (rc *RedisClient) Keys(ctx context.Context, pattern string) ([]string, error) {
	keys, err := rc.cli.Keys(ctx, pattern).Result()
	if err != nil {
		rc.log.Error("Failed to get keys with pattern %s: %v", pattern, err)
		return nil, err
	}
	rc.log.Debug("Found %d keys matching pattern %s", len(keys), pattern)
	return keys, nil
}

// DeleteWithPattern: Delete all keys matching pattern
func (rc *RedisClient) DeleteWithPattern(ctx context.Context, pattern string) error {
	keys, err := rc.Keys(ctx, pattern)
	if err != nil {
		return err
	}

	if len(keys) == 0 {
		return nil
	}

	return rc.Del(ctx, keys...)
}

// ============================================================================
// EXPIRATION OPERATIONS
// ============================================================================

// Expire: Set key expiration time
func (rc *RedisClient) Expire(ctx context.Context, key string, ttl time.Duration) error {
	err := rc.cli.Expire(ctx, key, ttl).Err()
	if err != nil {
		return err
	}
	rc.log.Debug("Set expiration on key %s: %v", key, ttl)
	return nil
}

// TTL: Get remaining time to live
func (rc *RedisClient) TTL(ctx context.Context, key string) (time.Duration, error) {
	ttl, err := rc.cli.TTL(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	return ttl, nil
}
