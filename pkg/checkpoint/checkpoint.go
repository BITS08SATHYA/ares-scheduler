// File: pkg/checkpoint/checkpoint.go
// Feature: Checkpointing & Recovery
//
// Enables jobs to save progress to shared storage and resume after
// crashes, preemptions, or retries. Completes the reliability story:
//   exactly-once → retry with backoff → preemption → checkpoint recovery
//
// How it works:
//   1. Job submits with CheckpointEnabled=true, CheckpointPath="s3://bucket/checkpoints/"
//   2. Executor injects ARES_CHECKPOINT_PATH and ARES_CHECKPOINT_RESTORE into pod env
//   3. Job application saves checkpoints periodically to ARES_CHECKPOINT_PATH
//   4. Job notifies Ares via POST /checkpoint (or Ares detects checkpoint files)
//   5. On restart/preemption recovery, Ares sets ARES_CHECKPOINT_RESTORE to last checkpoint
//   6. Job reads ARES_CHECKPOINT_RESTORE and resumes from that state
//
// The job application is responsible for:
//   - Writing checkpoint files to ARES_CHECKPOINT_PATH/{job_id}/
//   - Reading ARES_CHECKPOINT_RESTORE on startup to resume
//   - Ares manages the metadata (which checkpoint, when, what state)

package checkpoint

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
)

// ============================================================================
// CHECKPOINT MANAGER
// ============================================================================

type CheckpointManager struct {
	mu  sync.RWMutex
	log *logger.Logger

	// In-memory index of checkpoints per job
	checkpoints map[string]*CheckpointRecord // jobID -> latest checkpoint
}

// CheckpointRecord: Metadata about a job's checkpoint
type CheckpointRecord struct {
	JobID          string
	CheckpointPath string    // Full path: s3://bucket/checkpoints/job123/epoch-47
	BasePath       string    // Base path from JobSpec: s3://bucket/checkpoints/
	Epoch          int       // Training epoch (if applicable)
	Step           int       // Training step (if applicable)
	Metadata       string    // Arbitrary metadata from the job (e.g., "epoch=47,loss=0.023")
	CreatedAt      time.Time // When checkpoint was saved
	SizeBytes      int64     // Size of checkpoint data
	IsValid        bool      // Whether checkpoint was validated
}

// CheckpointEnvVars: Environment variables injected into the pod
type CheckpointEnvVars struct {
	CheckpointPath    string // Where to WRITE new checkpoints
	CheckpointRestore string // Where to READ checkpoint on restart (empty = fresh start)
	CheckpointMeta    string // Metadata from last checkpoint (e.g., "epoch=47")
	CheckpointJobDir  string // Job-specific directory: {base_path}/{job_id}/
}

// ============================================================================
// CONSTRUCTOR
// ============================================================================

func NewCheckpointManager() *CheckpointManager {
	return &CheckpointManager{
		log:         logger.Get(),
		checkpoints: make(map[string]*CheckpointRecord),
	}
}

// ============================================================================
// CHECKPOINT REGISTRATION
// ============================================================================

// RecordCheckpoint: Called when a job reports a new checkpoint
// This can be triggered by:
//   - POST /checkpoint API from the pod
//   - Periodic polling of checkpoint storage
//   - Job completion with final checkpoint
func (cm *CheckpointManager) RecordCheckpoint(
	ctx context.Context,
	jobID string,
	checkpointPath string,
	metadata string,
) (*CheckpointRecord, error) {

	if jobID == "" {
		return nil, fmt.Errorf("job ID required")
	}
	if checkpointPath == "" {
		return nil, fmt.Errorf("checkpoint path required")
	}

	record := &CheckpointRecord{
		JobID:          jobID,
		CheckpointPath: checkpointPath,
		Metadata:       metadata,
		CreatedAt:      time.Now(),
		IsValid:        true,
	}

	// Parse epoch/step from metadata if present
	record.Epoch = parseMetaInt(metadata, "epoch")
	record.Step = parseMetaInt(metadata, "step")

	cm.mu.Lock()
	cm.checkpoints[jobID] = record
	cm.mu.Unlock()

	cm.log.Info("CHECKPOINT RECORDED: job=%s path=%s meta=%s",
		jobID, checkpointPath, metadata)

	return record, nil
}

// UpdateJobCheckpoint: Update the Job record with checkpoint info
// Called after RecordCheckpoint to persist into the job store
func (cm *CheckpointManager) UpdateJobCheckpoint(job *common.Job, record *CheckpointRecord) {
	if job == nil || record == nil {
		return
	}

	job.LastCheckpointPath = record.CheckpointPath
	job.LastCheckpointTime = record.CreatedAt
	job.LastCheckpointMeta = record.Metadata
}

// ============================================================================
// CHECKPOINT RECOVERY (for restarts/preemption)
// ============================================================================

// GetRestoreEnvVars: Build environment variables for a restarting job
// This is called by the executor when creating a pod for a job that
// has been retried or recovered from preemption.
//
// Returns env vars that tell the pod where to find its last checkpoint.
func (cm *CheckpointManager) GetRestoreEnvVars(job *common.Job) *CheckpointEnvVars {
	if job == nil || job.Spec == nil || !job.Spec.CheckpointEnabled {
		return nil
	}

	basePath := job.Spec.CheckpointPath
	if basePath == "" {
		return nil
	}

	// Job-specific checkpoint directory
	jobDir := fmt.Sprintf("%s/%s", trimTrailingSlash(basePath), job.ID)

	envVars := &CheckpointEnvVars{
		CheckpointPath:   jobDir,
		CheckpointJobDir: jobDir,
	}

	// If job has a previous checkpoint, set restore path
	if job.LastCheckpointPath != "" {
		envVars.CheckpointRestore = job.LastCheckpointPath
		envVars.CheckpointMeta = job.LastCheckpointMeta
		cm.log.Info("CHECKPOINT RESTORE: job=%s restoring from %s (meta=%s)",
			job.ID, job.LastCheckpointPath, job.LastCheckpointMeta)
	} else {
		// Check in-memory index
		cm.mu.RLock()
		record, exists := cm.checkpoints[job.ID]
		cm.mu.RUnlock()

		if exists && record.IsValid {
			envVars.CheckpointRestore = record.CheckpointPath
			envVars.CheckpointMeta = record.Metadata
			cm.log.Info("CHECKPOINT RESTORE (from index): job=%s restoring from %s",
				job.ID, record.CheckpointPath)
		} else {
			cm.log.Info("CHECKPOINT: job=%s no previous checkpoint, starting fresh", job.ID)
		}
	}

	return envVars
}

// HasCheckpoint: Does this job have a checkpoint to restore from?
func (cm *CheckpointManager) HasCheckpoint(jobID string) bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	record, exists := cm.checkpoints[jobID]
	return exists && record.IsValid
}

// GetLatestCheckpoint: Get the latest checkpoint for a job
func (cm *CheckpointManager) GetLatestCheckpoint(jobID string) *CheckpointRecord {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	record, exists := cm.checkpoints[jobID]
	if !exists {
		return nil
	}

	copy := *record
	return &copy
}

// ============================================================================
// CHECKPOINT CLEANUP
// ============================================================================

// ClearCheckpoint: Remove checkpoint record when job completes successfully
func (cm *CheckpointManager) ClearCheckpoint(jobID string) {
	cm.mu.Lock()
	delete(cm.checkpoints, jobID)
	cm.mu.Unlock()

	cm.log.Debug("CHECKPOINT CLEARED: job=%s", jobID)
}

// ClearOldCheckpoints: Remove checkpoint records older than maxAge
func (cm *CheckpointManager) ClearOldCheckpoints(maxAge time.Duration) int {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cutoff := time.Now().Add(-maxAge)
	cleared := 0

	for jobID, record := range cm.checkpoints {
		if record.CreatedAt.Before(cutoff) {
			delete(cm.checkpoints, jobID)
			cleared++
		}
	}

	if cleared > 0 {
		cm.log.Info("CHECKPOINT CLEANUP: cleared %d old checkpoints", cleared)
	}

	return cleared
}

// ============================================================================
// STATS
// ============================================================================

// GetStats: Checkpoint system statistics
func (cm *CheckpointManager) GetStats() map[string]interface{} {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	totalCheckpoints := len(cm.checkpoints)
	validCount := 0

	for _, record := range cm.checkpoints {
		if record.IsValid {
			validCount++
		}
	}

	return map[string]interface{}{
		"total_checkpoints": totalCheckpoints,
		"valid_checkpoints": validCount,
	}
}

// ============================================================================
// HELPERS
// ============================================================================

// parseMetaInt: Extract integer value from metadata string
// Metadata format: "epoch=47,loss=0.023,step=1500"
func parseMetaInt(metadata string, key string) int {
	if metadata == "" {
		return 0
	}

	target := key + "="
	idx := 0
	for i := 0; i <= len(metadata)-len(target); i++ {
		if metadata[i:i+len(target)] == target {
			idx = i + len(target)
			break
		}
	}

	if idx == 0 {
		return 0
	}

	val := 0
	for idx < len(metadata) && metadata[idx] >= '0' && metadata[idx] <= '9' {
		val = val*10 + int(metadata[idx]-'0')
		idx++
	}

	return val
}

// trimTrailingSlash: Remove trailing slash from path
func trimTrailingSlash(path string) string {
	if len(path) > 0 && path[len(path)-1] == '/' {
		return path[:len(path)-1]
	}
	return path
}
