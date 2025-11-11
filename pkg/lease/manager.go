package lease

import (
	"context"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"strconv"
	"sync"
	"time"
)

// Lease represents an exclusive lock on a job (Prevent Duplicates)
type Lease struct {
	JobID        string
	LeaseID      clientv3.LeaseID
	WorkerID     string
	FencingToken int64 // Monotonic token for zombie protection
	ExpiresAt    time.Time
}

// Manager handles lease acquisition and maintenance
type Manager struct {
	etcd     *clientv3.Client
	workerID string
	mu       sync.Mutex
}

// NewManager func creates a new lease manager
func NewManager(etcd *clientv3.Client, workerID string) *Manager {
	return &Manager{
		etcd:     etcd,
		workerID: workerID,
	}
}

// AcquireJobLease attempts to acquire exclusive lease with fencing token
func (m *Manager) AcquireJobLease(ctx context.Context, jobID string, ttlSeconds int64) (*Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	//	Step-1: Generate fencing Token (monotonically increasing)
	fencingToken, err := m.generateFencingToken(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to generate fencing token: %w", err)
	}

	// step-2: Create etcd lease with TTL
	leaseResp, err := m.etcd.Grant(ctx, ttlSeconds)
	if err != nil {
		return nil, fmt.Errorf("Failed to create lease: %w", err)
	}

	leaseID := leaseResp.ID
	lockKey := fmt.Sprintf("locks/%s", jobID)
	lockValue := fmt.Sprintf("%s:%d:%d", m.workerID, fencingToken, time.Now().Unix())

	// Step-3: Try to acquire lock atomically with fencing tokens
	txn := m.etcd.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
		Then(
			clientv3.OpPut(lockKey, lockValue, clientv3.WithLease(leaseID)),
			clientv3.OpPut(fmt.Sprintf("/fencing/%s", jobID), fmt.Sprintf("%d", fencingToken)),
		).
		Else(clientv3.OpGet(lockKey))

	txnResp, err := txn.Commit()
	if err != nil {
		m.etcd.Revoke(ctx, leaseID)
		return nil, fmt.Errorf("failed to commit lease transaction: %w", err)
	}

	if !txnResp.Succeeded {
		m.etcd.Revoke(ctx, leaseID)

		existingHolder := "unknown"
		if len(txnResp.Responses) > 0 {
			getResp := txnResp.Responses[0].GetResponseRange()
			if len(getResp.Kvs) > 0 {
				existingHolder = string(getResp.Kvs[0].Value)
			}
		}
		return nil, fmt.Errorf("lease already held by %s", existingHolder)
	}

	return &Lease{
		JobID:        jobID,
		LeaseID:      leaseID,
		WorkerID:     m.workerID,
		FencingToken: fencingToken,
		ExpiresAt:    time.Now().Add(time.Duration(ttlSeconds) * time.Second),
	}, nil
}

// generateFencingToken creates monotonically increasing token
func (m *Manager) generateFencingToken(ctx context.Context, jobID string) (int64, error) {
	counterKey := fmt.Sprintf("/token-counter/%s", jobID)

	for attempt := 0; attempt < 10; attempt++ {
		//	Get current counter value
		resp, err := m.etcd.Get(ctx, counterKey)
		if err != nil {
			return 0, err
		}

		var currentValue int64 = 0
		if len(resp.Kvs) > 0 {
			currentValue, _ = strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)
		}

		newValue := currentValue + 1

		//	Atomic Compare-and-swap
		var txn clientv3.Txn
		if len(resp.Kvs) == 0 {
			//	Create key if doesn't exist
			txn = m.etcd.Txn(ctx).
				If(clientv3.Compare(clientv3.CreateRevision(counterKey), "=", 0)).
				Then(clientv3.OpPut(counterKey, fmt.Sprintf("%d", newValue)))
		} else {
			//	Assuming key exists in the etcd.. increase it
			txn = m.etcd.Txn(ctx).
				If(clientv3.Compare(clientv3.Value(counterKey), "=", fmt.Sprintf("%d", currentValue))).
				Then(clientv3.OpPut(counterKey, fmt.Sprintf("%d", newValue)))
		}

		txnResp, err := txn.Commit()
		if err != nil {
			return 0, err
		}

		if txnResp.Succeeded {
			return newValue, nil
		}

		//	CAS Failed, retry
		time.Sleep(10 * time.Millisecond)
	}

	return 0, fmt.Errorf("failed to generate token after 10 attempts")
}

// ValidateFencingToken checks if worker still holds valid lease
func (m *Manager) ValidateFencingToken(ctx context.Context, lease *Lease) error {
	fencingKey := fmt.Sprintf("/fencing/%s", lease.JobID)

	resp, err := m.etcd.Get(ctx, fencingKey)
	if err != nil {
		return fmt.Errorf("Failed to get fencing token: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return fmt.Errorf("fencing token not found (lease expired)")
	}

	currentToken, _ := strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)

	if currentToken != lease.FencingToken {
		return fmt.Errorf("fencing token mismatch: have %d, current is %d (ZOMBIE WORKER)",
			lease.FencingToken, currentToken)
	}
	return nil
}

// ReleaseLease releases the lease with fencing token validation
func (m *Manager) ReleaseLease(ctx context.Context, lease *Lease) error {
	lockKey := fmt.Sprintf("/locks/%s", lease.JobID)
	fencingKey := fmt.Sprintf("/fencing/%s", lease.JobID)

	//	Atomic delete with fencing token check
	txn := m.etcd.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(fencingKey), "=", fmt.Sprintf("%d", lease.FencingToken))).
		Then(
			clientv3.OpDelete(lockKey),
			clientv3.OpDelete(fencingKey),
		)

	txnResp, err := txn.Commit()
	if err != nil {
		return fmt.Errorf("failed to release lease: %w", err)
	}

	if !txnResp.Succeeded {
		return fmt.Errorf("lease release failed: fencing token mismatch (zombie worker)")
	}

	//	Revoke etcd lease
	_, err = m.etcd.Revoke(ctx, lease.LeaseID)

	if err != nil {
		return fmt.Errorf("failed to revoke lease: %w", err)
	}

	return nil
}

// KeepAlive maintains the lease with periodic heartbeats
func (m *Manager) KeepAlive(ctx context.Context, lease *Lease) (<-chan error, error) {
	keepAliveCh, err := m.etcd.KeepAlive(ctx, lease.LeaseID)
	if err != nil {
		return nil, fmt.Errorf("failed to start keepAlive: %w", err)
	}
	errCh := make(chan error, 1)

	go func() {
		defer close(errCh)

		for {
			select {
			case ka := <-keepAliveCh:
				if ka == nil {
					errCh <- fmt.Errorf("lease lost (keepAlive failed)")
					return
				}
			//	Heartbeat successful
			case <-ctx.Done():
				return
			}
		}
	}()
	return errCh, nil
}
