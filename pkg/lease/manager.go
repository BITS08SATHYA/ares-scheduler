// File: pkg/lease/manager.go
// Layer 3: Distributed Lease Manager (Feature 6 Layer 2, Feature 19)
// CRITICAL: This implements distributed locking for exactly-once semantics
//
// How it works:
// 1. Job wants to execute
// 2. Request lease from etcd (with TTL, auto-expiry)
// 3. If we get the lease, we own the execution
// 4. Keep-alive lease while executing (heartbeat every 10s)
// 5. If executor crashes, lease expires and someone else can take over
// 6. Using fencing tokens to prevent stale executor from re-running

package lease

import (
	"context"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/storage/etcd"
	"time"
)

// LeaseManager: Manages distributed leases for job execution
type LeaseManager struct {
	etcd            *etcd.ETCDClient
	log             *logger.Logger
	leaseTTL        int              // TTL in seconds (usually 30)
	renewalInterval time.Duration    // How often to renew (usually 10s)
	leaseKeyPrefix  string           // "/ares/leases/job"
	activeLeases    map[string]int64 // Track active leases in memory: jobID → leaseID
}

// NewLeaseManager: Create a new lease manager
func NewLeaseManager(
	etcd *etcd.ETCDClient,
	leaseTTL int,
	renewalInterval time.Duration,
) *LeaseManager {
	return &LeaseManager{
		etcd:            etcd,
		log:             logger.Get(),
		leaseTTL:        leaseTTL,
		renewalInterval: renewalInterval,
		leaseKeyPrefix:  "/ares/leases/job",
		activeLeases:    make(map[string]int64),
	}
}

// ============================================================================
// CORE LEASE OPERATIONS
// ============================================================================

// AcquireJobLease: Try to acquire a lease for a job
//
// Returns:
//   - leaseID: ID of the acquired lease (used to renew/revoke)
//   - token: Fencing token unique to this execution attempt
//   - acquired: true if we got the lease, false if someone else has it
//   - error: Any error during operation
//
// KEY POINT: This is atomic. Either we get it or we don't. No race conditions.
func (lm *LeaseManager) AcquireJobLease(
	ctx context.Context,
	jobID string,
	executorID string,
) (leaseID int64, token string, acquired bool, err error) {

	lm.log.Debug("Attempting to acquire lease for job: %s", jobID)

	// Step 1: Create a new lease in etcd
	// This lease will auto-expire after leaseTTL seconds
	leaseID, err = lm.etcd.GrantLease(ctx, lm.leaseTTL)
	if err != nil {
		lm.log.Error("Failed to grant lease for job %s: %v", jobID, err)
		return 0, "", false, fmt.Errorf("grant lease failed: %w", err)
	}

	lm.log.Debug("Granted lease %d for job %s", leaseID, jobID)

	// Step 2: Generate fencing token (unique to this execution attempt)
	// This proves that WE are the ones executing, not a stale executor
	token = generateFencingToken()

	// Step 3: Build lease value (what to store in etcd)
	// Format: "jobID|executorID|token|leaseID|timestamp"
	leaseValue := fmt.Sprintf("%s|%s|%s|%d|%d",
		jobID,
		executorID,
		token,
		leaseID,
		time.Now().Unix(),
	)

	// Step 4: Atomic "set with lease if not exists"
	// If the key already exists, someone else has the lease
	// If the key doesn't exist, we get it
	leaseKey := fmt.Sprintf("%s/%s", lm.leaseKeyPrefix, jobID)
	success, err := lm.etcd.LeaseCAS(ctx, leaseKey, leaseValue, leaseID)
	if err != nil {
		// Lease creation failed, revoke what we created
		lm.etcd.RevokeLease(ctx, leaseID)
		lm.log.Error("Failed to acquire lease for job %s: %v", jobID, err)
		return 0, "", false, fmt.Errorf("lease acquisition failed: %w", err)
	}

	if !success {
		// Someone else has the lease, revoke ours
		lm.etcd.RevokeLease(ctx, leaseID)
		lm.log.Debug("Lease already held for job %s, backing off", jobID)
		return 0, "", false, nil // Not an error, just already leased
	}

	// Success! We have the lease
	lm.activeLeases[jobID] = leaseID
	lm.log.Info("✓ Acquired lease for job %s (lease: %d, token: %s)", jobID, leaseID, token[:8])

	return leaseID, token, true, nil
}

// RenewLease: Keep a lease alive
//
// Call this periodically (every 10 seconds) while job is running
// If we don't renew, lease expires and another executor can take over
func (lm *LeaseManager) RenewLease(
	ctx context.Context,
	leaseID int64,
	jobID string,
) error {

	if leaseID == 0 {
		return fmt.Errorf("invalid lease ID")
	}

	err := lm.etcd.KeepAliveOnce(ctx, leaseID)
	if err != nil {
		lm.log.Error("Failed to renew lease %d for job %s: %v", leaseID, jobID, err)
		return fmt.Errorf("lease renewal failed: %w", err)
	}

	lm.log.Debug("Renewed lease %d for job %s", leaseID, jobID)
	return nil
}

// ReleaseLease: Explicitly release a lease
//
// Call this when:
// - Job completed successfully
// - Job failed and won't retry
// - We're shutting down
//
// This allows another executor to take over immediately
// (instead of waiting for lease to expire)
func (lm *LeaseManager) ReleaseLease(
	ctx context.Context,
	leaseID int64,
	jobID string,
) error {

	if leaseID == 0 {
		return fmt.Errorf("invalid lease ID")
	}

	err := lm.etcd.RevokeLease(ctx, leaseID)
	if err != nil {
		lm.log.Error("Failed to revoke lease %d for job %s: %v", leaseID, jobID, err)
		return fmt.Errorf("lease revocation failed: %w", err)
	}

	delete(lm.activeLeases, jobID)
	lm.log.Info("✓ Released lease for job %s (lease: %d)", jobID, leaseID)

	return nil
}

// ============================================================================
// LEASE VERIFICATION
// ============================================================================

// GetLeaseInfo: Get information about a job's lease
// Returns the lease value and how much time is left
func (lm *LeaseManager) GetLeaseInfo(
	ctx context.Context,
	jobID string,
) (string, time.Duration, error) {

	leaseKey := fmt.Sprintf("%s/%s", lm.leaseKeyPrefix, jobID)
	leaseValue, err := lm.etcd.Get(ctx, leaseKey)
	if err != nil {
		return "", 0, fmt.Errorf("failed to get lease: %w", err)
	}

	if leaseValue == "" {
		return "", 0, fmt.Errorf("lease not found for job %s", jobID)
	}

	// Try to get TTL from etcd (simplified - just return leaseTTL)
	timeLeft := time.Duration(lm.leaseTTL) * time.Second

	return leaseValue, timeLeft, nil
}

// ValidateToken: Check if a fencing token is valid
//
// Use this to make sure a stale executor doesn't re-run
// If token doesn't match the one we have, reject the execution
func (lm *LeaseManager) ValidateToken(
	ctx context.Context,
	jobID string,
	providedToken string,
) (bool, error) {

	leaseValue, _, err := lm.GetLeaseInfo(ctx, jobID)
	if err != nil {
		return false, fmt.Errorf("failed to validate token: %w", err)
	}

	// Parse: "jobID|executorID|token|leaseID|timestamp"
	parts := parseLeaseValue(leaseValue)
	if len(parts) < 3 {
		return false, fmt.Errorf("invalid lease format")
	}

	storedToken := parts[2]
	isValid := storedToken == providedToken

	if !isValid {
		lm.log.Warn("Token mismatch for job %s: got %s, expected %s",
			jobID, providedToken[:8], storedToken[:8])
	}

	return isValid, nil
}

// ============================================================================
// BACKGROUND MAINTENANCE
// ============================================================================

// StartLeaseRenewalLoop: Start background goroutine to renew active leases
//
// This runs in the background and:
// 1. Keeps all active leases alive
// 2. Removes completed jobs from tracking
// 3. Logs any renewal failures
//
// Call this once at startup: go lm.StartLeaseRenewalLoop(ctx, activeJobs)
// Where activeJobs is a channel that receives jobs to track
func (lm *LeaseManager) StartLeaseRenewalLoop(
	ctx context.Context,
	jobUpdates <-chan *common.Job,
) {

	ticker := time.NewTicker(lm.renewalInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Context cancelled, stop renewal loop
			lm.log.Info("Lease renewal loop stopped")
			return

		case <-ticker.C:
			// Time to renew all active leases
			lm.renewAllLeases(ctx)

		case job := <-jobUpdates:
			// Track new job or clean up completed job
			if job == nil {
				continue
			}

			if job.IsCompleted() {
				// Job is done, remove from active leases
				delete(lm.activeLeases, job.ID)
				lm.log.Debug("Removed completed job from lease tracking: %s", job.ID)
			}
		}
	}
}

// renewAllLeases: Renew all active leases
// Called periodically by renewal loop
func (lm *LeaseManager) renewAllLeases(ctx context.Context) {
	for jobID, leaseID := range lm.activeLeases {
		err := lm.RenewLease(ctx, leaseID, jobID)
		if err != nil {
			lm.log.Warn("Failed to renew lease for job %s: %v", jobID, err)
			// Don't delete from activeLeases, will retry next interval
		}
	}
}

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

// generateFencingToken: Generate a unique token for this execution attempt
// This proves that WE are the current executor, not a stale one
func generateFencingToken() string {
	// In production, use uuid.NewString() from github.com/google/uuid
	// For now, simple implementation using timestamp + random
	return fmt.Sprintf("token-%d-%x", time.Now().UnixNano(), time.Now().UnixNano()%65536)
}

// parseLeaseValue: Parse lease value string
// Format: "jobID|executorID|token|leaseID|timestamp"
func parseLeaseValue(leaseValue string) []string {
	// Simple parsing - in production use strings.Split with error handling
	parts := make([]string, 0)
	if leaseValue == "" {
		return parts
	}
	return stringToSlice(leaseValue, '|')
}

// stringToSlice: Split string by delimiter (helper)
func stringToSlice(s string, delimiter rune) []string {
	result := make([]string, 0)
	current := ""
	for _, r := range s {
		if r == delimiter {
			result = append(result, current)
			current = ""
		} else {
			current += string(r)
		}
	}
	if current != "" {
		result = append(result, current)
	}
	return result
}

// GetActiveLeaseCount: Return number of active leases being tracked
func (lm *LeaseManager) GetActiveLeaseCount() int {
	return len(lm.activeLeases)
}

// GetActiveLease: Get the lease ID for a specific job
func (lm *LeaseManager) GetActiveLease(jobID string) (int64, bool) {
	leaseID, exists := lm.activeLeases[jobID]
	return leaseID, exists
}
