package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

// Client provides unified access to etcd and Redis
type Client struct {
	etcd  *clientv3.Client
	redis *redis.Client
}

// NewClient creates a new storage client
func NewClient(etcdEndpoints []string, redisAddr string) (*Client, error) {
	//	Connect to etcd
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd: %w", err)
	}

	//	Connect to Redis
	redisClient := redis.NewClient(&redis.Options{
		Addr:        redisAddr,
		MaxRetries:  3,
		PoolSize:    10,
		ReadTimeout: 3 * time.Second,
	})

	//	Test Redis Connection
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("Failed to connect to redis: %w", err)
	}

	return &Client{
		etcd:  etcdClient,
		redis: redisClient,
	}, nil

}

// Close closes all connections
func (c *Client) Close() error {

	redisErr := c.redis.Close()
	etcdErr := c.etcd.Close()
	if redisErr != nil {
		return redisErr
	}
	return etcdErr
}

// ETCD Operations: Strong Consistency
// PutEtcd writes a value to etcd
// Use for: Job state, configuration, permanent records
func (c *Client) PutEtcd(ctx context.Context, key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	_, err = c.etcd.Put(ctx, key, string(data))
	return err
}

// GetEtcd reads a value from etcd
func (c *Client) GetEtcd(ctx context.Context, key string, result interface{}) error {
	resp, err := c.etcd.Get(ctx, key)
	if err != nil {
		return err
	}

	if len(resp.Kvs) == 0 {
		return fmt.Errorf("Key not found: %s", key)
	}

	return json.Unmarshal(resp.Kvs[0].Value, result)
}

// DeleteEtcd deletes a key from etcd
func (c *Client) DeleteEtcd(ctx context.Context, key string) error {
	_, err := c.etcd.Delete(ctx, key)
	return err
}

// GetEtcdWithPrefix gets all keys matching prefix
func (c *Client) GetEtcdWithPrefix(ctx context.Context, prefix string) (map[string]string, error) {
	resp, err := c.etcd.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	result := make(map[string]string)
	for _, kv := range resp.Kvs {
		result[string(kv.Key)] = string(kv.Value)
	}

	return result, nil
}

// REDIS OPERATIONS: Fast Cache & Queues
// SetRedis sets a key with optional TTL
// Use for: Caches, temporary state, hot data
func (c *Client) setRedis(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.redis.Set(ctx, key, data, ttl).Err()
}

// GetRedis gets a value from Redis
func (c *Client) GetRedis(ctx context.Context, key string, result interface{}) error {
	val, err := c.redis.Get(ctx, key).Result()
	if err == redis.Nil {
		return fmt.Errorf("Key not found: %s", key)
	}
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(val), result)
}

// DeleteRedis deletes a key from Redis
func (c *Client) DeleteRedis(ctx context.Context, key string) error {
	return c.redis.Del(ctx, key).Err()
}

// ExistsRedis checks if a key exists in Redis
func (c *Client) ExistsRedis(ctx context.Context, key string) (bool, error) {
	n, err := c.redis.Exists(ctx, key).Result()
	return n > 0, err
}

// Queue Operations: Redis Streams/Lists
// PushQueue adds items to a queue (list)
// Use for: Job Queues, taks lists
func (c *Client) PushQueue(ctx context.Context, queueName string, items ...string) error {
	if len(items) == 0 {
		return nil
	}
	//	LPUSH = push to left (head)
	return c.redis.LPush(ctx, queueName, items).Err()
}

// PopQueue removes item from queue
// Use for: Worker pops job to execute
func (c *Client) PopQueue(ctx context.Context, queueName string, timeout time.Duration) (string, error) {
	//	BRPOP = blocking right pop (tail)
	result, err := c.redis.BRPop(ctx, timeout, queueName).Result()
	if err == redis.Nil {
		return "", fmt.Errorf("quque empty: %s", queueName)
	}
	if err != nil {
		return "", err
	}

	//	Result is [queueName, value]
	if len(result) < 2 {
		return "", fmt.Errorf("invalid pop result")
	}
	return result[1], nil
}

// QueueLen returns the length of a queue
func (c *Client) QueueLen(ctx context.Context, queueName string) (int64, error) {
	return c.redis.LLen(ctx, queueName).Result()
}

// SORTED SET Operations: Retry/BackOff Queues
// AddToSortedSet adds item to sorted set with score
// Use for: Retry queues with backoff timing
func (c *Client) AddToSortedSet(ctx context.Context, key string, score float64, member string) error {
	return c.redis.ZAdd(ctx, key, &redis.Z{Score: score, Member: member}).Err()
}

// GetFromSortedSet gets items from sorted set by score range
func (c *Client) GetFromSortedSet(ctx context.Context, key string, minScore, maxScore float64) ([]string, error) {
	return c.redis.ZRangeByScore(ctx, key, &redis.ZRangeBy{
		Min: fmt.Sprintf("%f", minScore),
		Max: fmt.Sprintf("%f", maxScore),
	}).Result()
}

// RemoveFromSortedSet removes item from sorted set
func (c *Client) RemoveFromSortedSet(ctx context.Context, key string, member string) error {
	return c.redis.ZRem(ctx, key, member).Err()
}

// Distributed Locking: Leases
// AcquireLease acquires a distributed lock
// Use for: Ensuring only one scheduler runs
func (c *Client) AcquireLease(ctx context.Context, name string, ttl time.Duration) (clientv3.LeaseID, error) {
	//	Create Lease
	grant, err := c.etcd.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return 0, err
	}

	//	Create key with lease
	_, err = c.etcd.Put(ctx, fmt.Sprintf("/ares/leases/%s", name), "held", clientv3.WithLease(grant.ID))
	if err != nil {
		return 0, err
	}

	return grant.ID, nil
}

// KeepAliveLease renews a lease
func (c *Client) KeepAliveLease(ctx context.Context, leaseID clientv3.LeaseID) error {
	_, err := c.etcd.KeepAliveOnce(ctx, leaseID)
	return err
}

// ReleaseLease releases a lease
func (c *Client) ReleaseLease(ctx context.Context, leaseID clientv3.LeaseID) error {
	_, err := c.etcd.Revoke(ctx, leaseID)
	return err
}

// DeDuplication: Request Tracking
// SetIfNotExists sets a key only if it doesn't exist (atomic)
// Use for: Request Deduplication
func (c *Client) setIfNotExists(ctx context.Context, key string, value string, ttl time.Duration) (bool, error) {
	//	SET With NX Option = set if not exists
	result, err := c.redis.SetNX(ctx, key, value, ttl).Result()
	return result, err
}

// GetOrSet get existing value or sets new one (atomic)
func (c *Client) GetOrSet(ctx context.Context, key string, value string, ttl time.Duration) (string, bool, error) {
	//	Try or Set
	exists, err := c.setIfNotExists(ctx, key, value, ttl)
	if err != nil {
		return "", false, err
	}

	if exists {
		//	We set it (was new)
		return value, true, nil
	}

	// It already existed, get the value
	retrieved, err := c.redis.Get(ctx, key).Result()
	if err != nil {
		return "", false, err
	}

	//	Return existing value
	return retrieved, false, nil

}

// Metrics & Counters
// IncrCounter increments a counter
// Use for: Metrics (jobs submitted, jobs completed, etc)
func (c *Client) IncrCounter(ctx context.Context, key string) (int64, error) {
	return c.redis.Incr(ctx, key).Result()
}

// GetCounter gets a counter value
func (c *Client) GetCounter(ctx context.Context, key string) (int64, error) {
	n, err := c.redis.Get(ctx, key).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return n, err
}

// HELPER: Choose storage by key pattern
// StoreState stores state with appropriate persistence
// Automatically chooses etcd for critical state, Redis for cache
func (c *Client) StoreState(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	//	 Determine which storage to use based on key prefix
	switch {
	case len(key) > 0 && key[0] == '/': // etcd-style key (e.g., /ares/jobs/123)
		return c.PutEtcd(ctx, key, value)
	default:
		return c.setRedis(ctx, key, value, ttl)
	}
}

// Health Check
// HealthCheck checks both storage systems
func (c *Client) HealthCheck(ctx context.Context) error {
	//	Check etcd
	if _, err := c.etcd.Get(ctx, "health"); err != nil {
		return fmt.Errorf("etcd health check failed: %w", err)
	}

	//	Check Redis
	if err := c.redis.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis health check failed: %w", err)
	}

	return nil
}
