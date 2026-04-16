// File: pkg/scheduler/global/election.go
// Leader election for the global scheduler using etcd concurrency.
// Only the leader instance processes scheduling requests; standby replicas
// wait until they win the election.
//
// Usage:
//   le := NewLeaderElector(etcdClient, "/ares/leader", "instance-1")
//   le.RunAsLeader(ctx, func(ctx context.Context) {
//       // This runs only while we are the leader
//       startScheduler(ctx)
//   })

package global

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
)

// LeaderElector manages leader election via etcd
type LeaderElector struct {
	client     *clientv3.Client
	prefix     string // etcd key prefix for election (e.g., "/ares/leader")
	instanceID string // unique identifier for this instance
	log        *logger.Logger
	isLeader   atomic.Bool
	sessionTTL int // seconds
}

// NewLeaderElector creates a new leader elector
func NewLeaderElector(client *clientv3.Client, prefix string, instanceID string) *LeaderElector {
	return &LeaderElector{
		client:     client,
		prefix:     prefix,
		instanceID: instanceID,
		log:        logger.Get(),
		sessionTTL: 15, // 15-second session TTL
	}
}

// IsLeader returns true if this instance is currently the leader
func (le *LeaderElector) IsLeader() bool {
	return le.isLeader.Load()
}

// RunAsLeader blocks until this instance becomes leader, then calls leaderFunc.
// If leadership is lost, leaderFunc's context is canceled and the election restarts.
// This method only returns when ctx is canceled.
func (le *LeaderElector) RunAsLeader(ctx context.Context, leaderFunc func(ctx context.Context)) error {
	for {
		select {
		case <-ctx.Done():
			le.isLeader.Store(false)
			return ctx.Err()
		default:
		}

		err := le.campaignAndLead(ctx, leaderFunc)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			le.log.Warn("Leader election error: %v (retrying in 5s)", err)
			select {
			case <-time.After(5 * time.Second):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// campaignAndLead runs one election cycle: create session, campaign, lead, resign
func (le *LeaderElector) campaignAndLead(ctx context.Context, leaderFunc func(ctx context.Context)) error {
	le.log.Info("ELECTION: Instance %s entering campaign for %s", le.instanceID, le.prefix)

	// Create a session with TTL — if this instance crashes, the session expires
	// and another instance can win the election
	session, err := concurrency.NewSession(le.client,
		concurrency.WithTTL(le.sessionTTL),
		concurrency.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("create session failed: %w", err)
	}
	defer func() { _ = session.Close() }()

	election := concurrency.NewElection(session, le.prefix)

	// Campaign blocks until we win or ctx is canceled
	le.log.Info("ELECTION: Instance %s campaigning...", le.instanceID)
	if err := election.Campaign(ctx, le.instanceID); err != nil {
		return fmt.Errorf("campaign failed: %w", err)
	}

	le.isLeader.Store(true)
	le.log.Info("ELECTION: Instance %s is now LEADER", le.instanceID)

	// Run the leader function with a cancellable context.
	// Cancel when either: parent ctx canceled, or session expires (lost leadership).
	leaderCtx, leaderCancel := context.WithCancel(ctx)
	defer leaderCancel()

	// Watch for session expiry (lost leadership)
	go func() {
		select {
		case <-session.Done():
			le.log.Warn("ELECTION: Session expired — lost leadership")
			le.isLeader.Store(false)
			leaderCancel()
		case <-leaderCtx.Done():
		}
	}()

	// Run leader workload
	leaderFunc(leaderCtx)

	// Resign leadership gracefully
	le.isLeader.Store(false)
	le.log.Info("ELECTION: Instance %s resigning leadership", le.instanceID)

	resignCtx, resignCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer resignCancel()
	if err := election.Resign(resignCtx); err != nil {
		le.log.Warn("ELECTION: Resign failed: %v", err)
	}

	return nil
}
