package etcd

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// Layer 2: etcd client wrapper (depends on types, config, logger)
// ETCDClient: Wrapper around etcd client for all etcd operations
type ETCDClient struct {
	cli        *clientv3.Client
	timeout    time.Duration
	log        *logger.Logger
	leaseRenew time.Duration

	// Circuit breaker: prevents cascading failures during etcd outages.
	// After consecutiveFailures hits threshold, the breaker opens and
	// all operations fail fast for the cooldown period instead of
	// waiting for timeout on every call.
	cbMu                sync.Mutex
	consecutiveFailures int
	lastFailure         time.Time
	circuitOpen         bool

	// Circuit breaker metrics (exposed via GetCircuitBreakerStats)
	cbOpens   uint64
	cbRejects uint64
	cbResets  uint64
}

const (
	cbFailureThreshold = 5                // Open circuit after 5 consecutive failures
	cbCooldownPeriod   = 10 * time.Second // Stay open for 10 seconds before retrying
)

// NewETCDClient: Create a new etcd client
func NewETCDClient(endpoints []string, timeout time.Duration) (*ETCDClient, error) {

	// Get logger instance
	log := logger.Get()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: timeout,
	})
	if err != nil {
		log.Error("Failed to connect to etcd: %v", err)
		return nil, err
	}

	log.Info("Connected to etcd at %v", endpoints)

	return &ETCDClient{
		cli:        cli,
		timeout:    timeout,
		log:        logger.Get(),
		leaseRenew: 10 * time.Second,
	}, nil
}

// Close: Close etcd connection
func (ec *ETCDClient) Close() error {
	return ec.cli.Close()
}

// checkCircuitBreaker: Fail fast if etcd is known to be down.
// Returns error if circuit is open (etcd recently failed repeatedly).
// After cooldown period, allows one request through (half-open state).
func (ec *ETCDClient) checkCircuitBreaker() error {
	ec.cbMu.Lock()
	defer ec.cbMu.Unlock()

	if !ec.circuitOpen {
		return nil
	}

	// Check if cooldown period has elapsed (half-open: allow one retry)
	if time.Since(ec.lastFailure) > cbCooldownPeriod {
		ec.log.Info("Circuit breaker half-open: allowing retry after %.0fs cooldown",
			cbCooldownPeriod.Seconds())
		ec.circuitOpen = false
		ec.consecutiveFailures = 0
		atomic.AddUint64(&ec.cbResets, 1)
		return nil
	}

	atomic.AddUint64(&ec.cbRejects, 1)

	return fmt.Errorf("circuit breaker OPEN: etcd unreachable (%d consecutive failures, retry in %.0fs)",
		ec.consecutiveFailures, (cbCooldownPeriod - time.Since(ec.lastFailure)).Seconds())
}

// GetCircuitBreakerStats: Expose circuit breaker metrics for Prometheus scraping
func (ec *ETCDClient) GetCircuitBreakerStats() (opens, rejects, resets uint64) {
	return atomic.LoadUint64(&ec.cbOpens), atomic.LoadUint64(&ec.cbRejects), atomic.LoadUint64(&ec.cbResets)
}

// recordSuccess: Reset circuit breaker on successful operation
func (ec *ETCDClient) recordSuccess() {
	ec.cbMu.Lock()
	defer ec.cbMu.Unlock()
	if ec.consecutiveFailures > 0 {
		ec.log.Info("Circuit breaker: etcd recovered (resetting after %d failures)", ec.consecutiveFailures)
	}
	ec.consecutiveFailures = 0
	ec.circuitOpen = false
}

// recordFailure: Track failure and potentially open circuit breaker
func (ec *ETCDClient) recordFailure() {
	ec.cbMu.Lock()
	defer ec.cbMu.Unlock()
	ec.consecutiveFailures++
	ec.lastFailure = time.Now()
	if ec.consecutiveFailures >= cbFailureThreshold && !ec.circuitOpen {
		ec.circuitOpen = true
		atomic.AddUint64(&ec.cbOpens, 1)
		ec.log.Error("Circuit breaker OPENED: etcd unreachable after %d consecutive failures (cooldown %.0fs)",
			ec.consecutiveFailures, cbCooldownPeriod.Seconds())
	}
}

// ============================================================================
// BASIC OPERATIONS
// ============================================================================

// Put: Store a key-value pair
func (ec *ETCDClient) Put(ctx context.Context, key, value string) error {
	if err := ec.checkCircuitBreaker(); err != nil {
		return err
	}
	_, err := ec.cli.Put(ctx, key, value)
	if err != nil {
		ec.recordFailure()
		ec.log.Error("Failed to put key %s: %v", key, err)
		return err
	}
	ec.recordSuccess()
	ec.log.Debug("Put key: %s", key)
	return nil
}

// PutWithTTL: Store a key-value pair with expiration (TTL)
func (ec *ETCDClient) PutWithTTL(ctx context.Context, key, value string, ttlSeconds int64) error {
	grant, err := ec.cli.Grant(ctx, ttlSeconds)
	if err != nil {
		return err
	}

	_, err = ec.cli.Put(ctx, key, value, clientv3.WithLease(grant.ID))
	if err != nil {
		ec.log.Error("Failed to put key with TTL %s: %v", key, err)
		return err
	}

	ec.log.Debug("Put key with TTL: %s (expires in %d seconds)", key, ttlSeconds)
	return nil
}

// PutWithLease: Store a key-value pair with a lease (CRITICAL FOR AUTO-CLEANUP)
//
// When executor crashes, lease expires → key is automatically deleted
// This is how jobs auto-cleanup from etcd when executor fails
// Used by: JobStore to attach jobs to executor leases
func (ec *ETCDClient) PutWithLease(ctx context.Context, key, value string, leaseID int64) error {
	_, err := ec.cli.Put(ctx, key, value, clientv3.WithLease(clientv3.LeaseID(leaseID)))
	if err != nil {
		ec.log.Error("Failed to put key with lease %s: %v", key, err)
		return err
	}

	ec.log.Info("Put key with lease: %s (leaseID: %d)", key, leaseID)
	return nil
}

// Persist the completed job to the store even after the lease expiry
func (ec *ETCDClient) PutWithoutLease(ctx context.Context, key, value string) error {

	putctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	_, err := ec.cli.Put(putctx, key, value)
	if err != nil {
		return fmt.Errorf("failed to put key %s: %w", key, err)
	}

	ec.log.Info("[etcd] Saved (Job) key permanently (no lease): %s", key)

	return nil

}

// PutIfVersion: Atomic write that only succeeds if the key's ModRevision
// matches the expected value. This is the proper fencing pattern — instead of
// "read lease, check owner, then write job" (TOCTOU vulnerable), we do
// "write job only if lease revision hasn't changed" (atomic).
func (ec *ETCDClient) PutIfVersion(ctx context.Context, fenceKey string, expectedVersion int64, writeKey string, writeValue string) (bool, error) {
	txn := ec.cli.Txn(ctx)

	// Condition: the fence key's ModRevision equals what we last saw
	cond := clientv3.Compare(clientv3.ModRevision(fenceKey), "=", expectedVersion)

	// If true: write the job data
	thenOp := clientv3.OpPut(writeKey, writeValue)

	// If false: get current value (for debugging)
	elseOp := clientv3.OpGet(fenceKey)

	resp, err := txn.If(cond).Then(thenOp).Else(elseOp).Commit()
	if err != nil {
		return false, fmt.Errorf("fenced write failed: %w", err)
	}

	return resp.Succeeded, nil
}

// Get: Retrieve a value by key
func (ec *ETCDClient) Get(ctx context.Context, key string) (string, error) {

	ec.log.Debug("Entered Get Etcd Method()")

	if err := ec.checkCircuitBreaker(); err != nil {
		return "", err
	}

	// Use caller's deadline if shorter, otherwise default to 5s
	getCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := ec.cli.Get(getCtx, key)

	ec.log.Debug("Get Response: %v", resp)

	if err != nil {
		ec.recordFailure()
		ec.log.Error("Failed to get key %s: %v", key, err)
		return "", err
	}

	ec.recordSuccess()

	if len(resp.Kvs) == 0 {
		ec.log.Debug("Key not found: %s", key)
		return "", nil
	}

	ec.log.Debug("Got key: %s", key)
	return string(resp.Kvs[0].Value), nil

}

// GetAll: Get all keys with prefix
func (ec *ETCDClient) GetAll(ctx context.Context, prefix string) (map[string]string, error) {
	resp, err := ec.cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	result := make(map[string]string)
	for _, kv := range resp.Kvs {
		result[string(kv.Key)] = string(kv.Value)
	}

	ec.log.Debug("Got %d keys with prefix: %s", len(result), prefix)
	return result, nil
}

func (ec *ETCDClient) GetWithRevision(ctx context.Context, key string) (string, int64, error) {
	resp, err := ec.cli.Get(ctx, key)
	if err != nil {
		return "", 0, err
	}
	if len(resp.Kvs) == 0 {
		return "", 0, nil
	}
	return string(resp.Kvs[0].Value), resp.Kvs[0].ModRevision, nil
}

// GetWithRevisionAndLease returns (value, modRevision, etcdLeaseID, error).
// The etcdLeaseID is the lease attached to the key, useful for ownership verification.
func (ec *ETCDClient) GetWithRevisionAndLease(ctx context.Context, key string) (string, int64, int64, error) {
	resp, err := ec.cli.Get(ctx, key)
	if err != nil {
		return "", 0, 0, err
	}
	if len(resp.Kvs) == 0 {
		return "", 0, 0, nil
	}
	return string(resp.Kvs[0].Value), resp.Kvs[0].ModRevision, resp.Kvs[0].Lease, nil
}

// PutIfModRevision: Atomic fenced write
// Writes writeKey=writeValue ONLY IF fenceKey's ModRevision still equals expectedModRevision.
// This is the proper fencing pattern: instead of "read lease, check owner, then write job"
// (TOCTOU vulnerable), we do "write job only if lease revision hasn't changed" (atomic).
//
// Returns true if the write succeeded, false if the fence key changed (another scheduler
// acquired the lease), and error for network/etcd failures.
func (ec *ETCDClient) PutIfModRevision(ctx context.Context, fenceKey string, expectedModRevision int64, writeKey string, writeValue string, leaseID int64) (bool, error) {
	// Use caller's deadline if shorter, otherwise default to 5s
	putCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	txn := ec.cli.Txn(putCtx)

	// Condition: the fence key's ModRevision equals what we last saw
	cond := clientv3.Compare(clientv3.ModRevision(fenceKey), "=", expectedModRevision)

	// If true: write the job data with lease
	var thenOp clientv3.Op
	if leaseID > 0 {
		thenOp = clientv3.OpPut(writeKey, writeValue, clientv3.WithLease(clientv3.LeaseID(leaseID)))
	} else {
		thenOp = clientv3.OpPut(writeKey, writeValue)
	}

	// If false: get current fence key (for debugging what changed)
	elseOp := clientv3.OpGet(fenceKey)

	resp, err := txn.If(cond).Then(thenOp).Else(elseOp).Commit()
	if err != nil {
		ec.log.Error("Fenced write failed on %s (fence=%s): %v", writeKey, fenceKey, err)
		return false, fmt.Errorf("fenced write failed: %w", err)
	}

	if !resp.Succeeded {
		ec.log.Error("Fenced write REJECTED: fence key %s ModRevision changed (expected %d)", fenceKey, expectedModRevision)
	} else {
		ec.log.Debug("Fenced write succeeded: %s (fence=%s, rev=%d)", writeKey, fenceKey, expectedModRevision)
	}

	return resp.Succeeded, nil
}

// Delete: Delete a key
func (ec *ETCDClient) Delete(ctx context.Context, key string) error {
	_, err := ec.cli.Delete(ctx, key)
	if err != nil {
		ec.log.Error("Failed to delete key %s: %v", key, err)
		return err
	}

	ec.log.Debug("Deleted key: %s", key)
	return nil
}

// ============================================================================
// WATCH OPERATIONS (Feature 10 - Heartbeat)
// ============================================================================

// Watch: Watch for changes to a key
func (ec *ETCDClient) Watch(ctx context.Context, key string) clientv3.WatchChan {
	ec.log.Debug("Watching key: %s", key)
	return ec.cli.Watch(ctx, key)
}

// WatchPrefix: Watch for changes to all keys with prefix
func (ec *ETCDClient) WatchPrefix(ctx context.Context, prefix string) clientv3.WatchChan {
	ec.log.Debug("Watching prefix: %s", prefix)
	return ec.cli.Watch(ctx, prefix, clientv3.WithPrefix())
}

// ============================================================================
// LEASE OPERATIONS (Feature 6 Layer 2, Feature 19 - Distributed Locking)
// ============================================================================

// GrantLease: Create a new lease for distributed locking
// Returns LeaseID that can be renewed
func (ec *ETCDClient) GrantLease(ctx context.Context, ttlSeconds int64) (int64, error) {
	grant, err := ec.cli.Grant(ctx, ttlSeconds)
	if err != nil {
		ec.log.Error("Failed to grant lease: %v", err)
		return 0, err
	}

	ec.log.Info("Granted lease: %d (TTL: %d seconds)", grant.ID, ttlSeconds)
	return int64(grant.ID), nil
}

// RevokeLease: Cancel a lease (release the lock)
func (ec *ETCDClient) RevokeLease(ctx context.Context, leaseID int64) error {
	_, err := ec.cli.Revoke(ctx, clientv3.LeaseID(leaseID))
	if err != nil {
		ec.log.Error("Failed to revoke lease %d: %v", leaseID, err)
		return err
	}

	ec.log.Debug("Revoked lease: %d", leaseID)
	return nil
}

// KeepAliveOnce: Renew a lease once (keeps it alive for another TTL period)
// CRITICAL: Called by heartbeat goroutine every 10 seconds
func (ec *ETCDClient) KeepAliveOnce(ctx context.Context, leaseID int64) error {
	_, err := ec.cli.KeepAliveOnce(ctx, clientv3.LeaseID(leaseID))
	if err != nil {
		ec.log.Error("Failed to renew lease %d: %v", leaseID, err)
		return err
	}

	ec.log.Debug("Renewed lease: %d", leaseID)
	return nil
}

// CAS: Compare-And-Swap (atomic operation)
// Only puts value if current version matches expectedVersion
// Used for exactly-once semantics: "Only put if key doesn't exist"
func (ec *ETCDClient) CAS(ctx context.Context, key string, expectedVersion int64, value string) (bool, error) {
	txn := ec.cli.Txn(ctx)

	// Condition: "Version of key equals expectedVersion"
	cond := clientv3.Compare(clientv3.Version(key), "=", expectedVersion)

	// If condition true: put the value
	thenOp := clientv3.OpPut(key, value)

	// If condition false: get current value
	elseOp := clientv3.OpGet(key)

	resp, err := txn.If(cond).Then(thenOp).Else(elseOp).Commit()
	if err != nil {
		ec.log.Error("CAS failed on key %s: %v", key, err)
		return false, err
	}

	success := resp.Succeeded
	ec.log.Debug("CAS on key %s: succeeded=%v", key, success)
	return success, nil
}

// LeaseCAS: Atomic "set with lease if not exists"
// Used for exactly-once: Only acquire lease if no one else has it
// CRITICAL: This ensures only ONE executor claims each job
func (ec *ETCDClient) LeaseCAS(ctx context.Context, key string, value string, leaseID int64) (bool, error) {
	txn := ec.cli.Txn(ctx)

	// Condition: "Version of key equals 0" (doesn't exist)
	cond := clientv3.Compare(clientv3.Version(key), "=", 0)

	// If condition true: put with lease
	thenOp := clientv3.OpPut(key, value, clientv3.WithLease(clientv3.LeaseID(leaseID)))

	// If condition false: get current
	elseOp := clientv3.OpGet(key)

	resp, err := txn.If(cond).Then(thenOp).Else(elseOp).Commit()
	if err != nil {
		return false, err
	}

	success := resp.Succeeded
	ec.log.Debug("LeaseCAS on key %s: succeeded=%v", key, success)
	return success, nil
}

// Session: Create a session for distributed coordination
// Used for more complex locking scenarios
func (ec *ETCDClient) NewSession(ctx context.Context, ttl int) (*concurrency.Session, error) {
	sess, err := concurrency.NewSession(ec.cli, concurrency.WithTTL(ttl))
	if err != nil {
		return nil, err
	}
	ec.log.Debug("Created session: %d", sess.Lease())
	return sess, nil
}

// ============================================================================
// HELPER METHODS
// ============================================================================

// Exists: Check if a key exists
func (ec *ETCDClient) Exists(ctx context.Context, key string) (bool, error) {
	value, err := ec.Get(ctx, key)
	if err != nil {
		return false, err
	}
	return value != "", nil
}

// DeleteWithPrefix: Delete all keys with given prefix
func (ec *ETCDClient) DeleteWithPrefix(ctx context.Context, prefix string) error {
	_, err := ec.cli.Delete(ctx, prefix, clientv3.WithPrefix())
	return err
}

// GetWithPrefix returns all values whose keys start with prefix
func (ec *ETCDClient) GetWithPrefix(ctx context.Context, prefix string) ([]string, error) {
	resp, err := ec.cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		ec.log.Error("Failed to get prefix %s: %v", prefix, err)
		return nil, fmt.Errorf("prefix get failed: %w", err)
	}

	values := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		values = append(values, string(kv.Value))
	}
	return values, nil
}
