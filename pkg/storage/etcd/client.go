package etcd

import (
	"context"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"time"
)

// Layer 2: etcd client wrapper (depends on types, config, logger)
// ETCDClient: Wrapper around etcd client for all etcd operations
type ETCDClient struct {
	cli        *clientv3.Client
	timeout    time.Duration
	log        *logger.Logger
	leaseRenew time.Duration
}

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

// ============================================================================
// BASIC OPERATIONS
// ============================================================================

// Put: Store a key-value pair
func (ec *ETCDClient) Put(ctx context.Context, key, value string) error {
	_, err := ec.cli.Put(ctx, key, value)
	if err != nil {
		ec.log.Error("Failed to put key %s: %v", key, err)
		return err
	}
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

// Get: Retrieve a value by key
func (ec *ETCDClient) Get(ctx context.Context, key string) (string, error) {
	resp, err := ec.cli.Get(ctx, key)
	if err != nil {
		ec.log.Error("Failed to get key %s: %v", key, err)
		return "", err
	}

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

	ec.log.Debug("Granted lease: %d (TTL: %d seconds)", grant.ID, ttlSeconds)
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
