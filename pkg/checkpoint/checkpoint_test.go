package checkpoint

import (
	"context"
	"testing"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// SECTION 1: Constructor
// ============================================================================

func TestNewCheckpointManager(t *testing.T) {
	cm := NewCheckpointManager()
	require.NotNil(t, cm)
	assert.Empty(t, cm.checkpoints)
}

// ============================================================================
// SECTION 2: RecordCheckpoint
// ============================================================================

func TestRecordCheckpoint_Success(t *testing.T) {
	cm := NewCheckpointManager()

	record, err := cm.RecordCheckpoint(
		context.Background(),
		"job-123",
		"s3://bucket/checkpoints/job-123/epoch-47",
		"epoch=47,loss=0.023",
	)

	require.NoError(t, err)
	assert.Equal(t, "job-123", record.JobID)
	assert.Equal(t, "s3://bucket/checkpoints/job-123/epoch-47", record.CheckpointPath)
	assert.Equal(t, "epoch=47,loss=0.023", record.Metadata)
	assert.Equal(t, 47, record.Epoch)
	assert.True(t, record.IsValid)
	assert.False(t, record.CreatedAt.IsZero())
}

func TestRecordCheckpoint_ParsesStep(t *testing.T) {
	cm := NewCheckpointManager()

	record, err := cm.RecordCheckpoint(
		context.Background(), "j1", "s3://path", "step=1500,epoch=10",
	)

	require.NoError(t, err)
	assert.Equal(t, 1500, record.Step)
	assert.Equal(t, 10, record.Epoch)
}

func TestRecordCheckpoint_EmptyMetadata(t *testing.T) {
	cm := NewCheckpointManager()

	record, err := cm.RecordCheckpoint(context.Background(), "j1", "s3://path", "")
	require.NoError(t, err)
	assert.Equal(t, 0, record.Epoch)
	assert.Equal(t, 0, record.Step)
}

func TestRecordCheckpoint_EmptyJobID(t *testing.T) {
	cm := NewCheckpointManager()
	_, err := cm.RecordCheckpoint(context.Background(), "", "s3://path", "")
	assert.Error(t, err)
}

func TestRecordCheckpoint_EmptyPath(t *testing.T) {
	cm := NewCheckpointManager()
	_, err := cm.RecordCheckpoint(context.Background(), "j1", "", "")
	assert.Error(t, err)
}

func TestRecordCheckpoint_OverwritesPrevious(t *testing.T) {
	cm := NewCheckpointManager()

	cm.RecordCheckpoint(context.Background(), "j1", "s3://old", "epoch=1")
	cm.RecordCheckpoint(context.Background(), "j1", "s3://new", "epoch=2")

	record := cm.GetLatestCheckpoint("j1")
	assert.Equal(t, "s3://new", record.CheckpointPath)
	assert.Equal(t, 2, record.Epoch)
}

// ============================================================================
// SECTION 3: HasCheckpoint / GetLatestCheckpoint
// ============================================================================

func TestHasCheckpoint(t *testing.T) {
	cm := NewCheckpointManager()
	assert.False(t, cm.HasCheckpoint("nonexistent"))

	cm.RecordCheckpoint(context.Background(), "j1", "s3://path", "")
	assert.True(t, cm.HasCheckpoint("j1"))
}

func TestGetLatestCheckpoint_NotFound(t *testing.T) {
	cm := NewCheckpointManager()
	assert.Nil(t, cm.GetLatestCheckpoint("nonexistent"))
}

func TestGetLatestCheckpoint_ReturnsCopy(t *testing.T) {
	cm := NewCheckpointManager()
	cm.RecordCheckpoint(context.Background(), "j1", "s3://path", "epoch=5")

	record := cm.GetLatestCheckpoint("j1")
	record.Epoch = 999 // mutate copy

	original := cm.GetLatestCheckpoint("j1")
	assert.Equal(t, 5, original.Epoch) // original unchanged
}

// ============================================================================
// SECTION 4: UpdateJobCheckpoint
// ============================================================================

func TestUpdateJobCheckpoint(t *testing.T) {
	cm := NewCheckpointManager()
	job := &common.Job{ID: "j1", Spec: &common.JobSpec{}}
	record := &CheckpointRecord{
		JobID:          "j1",
		CheckpointPath: "s3://bucket/j1/epoch-10",
		Metadata:       "epoch=10",
		CreatedAt:      time.Now(),
	}

	cm.UpdateJobCheckpoint(job, record)

	assert.Equal(t, "s3://bucket/j1/epoch-10", job.LastCheckpointPath)
	assert.Equal(t, "epoch=10", job.LastCheckpointMeta)
	assert.Equal(t, record.CreatedAt, job.LastCheckpointTime)
}

func TestUpdateJobCheckpoint_NilJob(t *testing.T) {
	cm := NewCheckpointManager()
	cm.UpdateJobCheckpoint(nil, &CheckpointRecord{}) // should not panic
}

func TestUpdateJobCheckpoint_NilRecord(t *testing.T) {
	cm := NewCheckpointManager()
	cm.UpdateJobCheckpoint(&common.Job{}, nil) // should not panic
}

// ============================================================================
// SECTION 5: GetRestoreEnvVars
// ============================================================================

func TestGetRestoreEnvVars_CheckpointDisabled(t *testing.T) {
	cm := NewCheckpointManager()
	job := &common.Job{
		ID:   "j1",
		Spec: &common.JobSpec{CheckpointEnabled: false},
	}
	assert.Nil(t, cm.GetRestoreEnvVars(job))
}

func TestGetRestoreEnvVars_NilJob(t *testing.T) {
	cm := NewCheckpointManager()
	assert.Nil(t, cm.GetRestoreEnvVars(nil))
}

func TestGetRestoreEnvVars_EmptyBasePath(t *testing.T) {
	cm := NewCheckpointManager()
	job := &common.Job{
		ID:   "j1",
		Spec: &common.JobSpec{CheckpointEnabled: true, CheckpointPath: ""},
	}
	assert.Nil(t, cm.GetRestoreEnvVars(job))
}

func TestGetRestoreEnvVars_FreshStart(t *testing.T) {
	cm := NewCheckpointManager()
	job := &common.Job{
		ID: "j1",
		Spec: &common.JobSpec{
			CheckpointEnabled: true,
			CheckpointPath:    "s3://bucket/checkpoints",
		},
	}

	envVars := cm.GetRestoreEnvVars(job)
	require.NotNil(t, envVars)
	assert.Equal(t, "s3://bucket/checkpoints/j1", envVars.CheckpointPath)
	assert.Equal(t, "s3://bucket/checkpoints/j1", envVars.CheckpointJobDir)
	assert.Empty(t, envVars.CheckpointRestore) // no prior checkpoint
}

func TestGetRestoreEnvVars_WithPreviousCheckpoint(t *testing.T) {
	cm := NewCheckpointManager()
	job := &common.Job{
		ID: "j1",
		Spec: &common.JobSpec{
			CheckpointEnabled: true,
			CheckpointPath:    "s3://bucket/checkpoints/",
		},
		LastCheckpointPath: "s3://bucket/checkpoints/j1/epoch-47",
		LastCheckpointMeta: "epoch=47",
	}

	envVars := cm.GetRestoreEnvVars(job)
	require.NotNil(t, envVars)
	assert.Equal(t, "s3://bucket/checkpoints/j1", envVars.CheckpointPath)
	assert.Equal(t, "s3://bucket/checkpoints/j1/epoch-47", envVars.CheckpointRestore)
	assert.Equal(t, "epoch=47", envVars.CheckpointMeta)
}

func TestGetRestoreEnvVars_FromInMemoryIndex(t *testing.T) {
	cm := NewCheckpointManager()
	cm.RecordCheckpoint(context.Background(), "j1", "s3://bucket/j1/epoch-5", "epoch=5")

	job := &common.Job{
		ID: "j1",
		Spec: &common.JobSpec{
			CheckpointEnabled: true,
			CheckpointPath:    "s3://bucket",
		},
	}

	envVars := cm.GetRestoreEnvVars(job)
	require.NotNil(t, envVars)
	assert.Equal(t, "s3://bucket/j1/epoch-5", envVars.CheckpointRestore)
	assert.Equal(t, "epoch=5", envVars.CheckpointMeta)
}

func TestGetRestoreEnvVars_TrailingSlashTrimmed(t *testing.T) {
	cm := NewCheckpointManager()
	job := &common.Job{
		ID: "j1",
		Spec: &common.JobSpec{
			CheckpointEnabled: true,
			CheckpointPath:    "s3://bucket/checkpoints/",
		},
	}

	envVars := cm.GetRestoreEnvVars(job)
	assert.Equal(t, "s3://bucket/checkpoints/j1", envVars.CheckpointPath)
}

// ============================================================================
// SECTION 6: Cleanup
// ============================================================================

func TestClearCheckpoint(t *testing.T) {
	cm := NewCheckpointManager()
	cm.RecordCheckpoint(context.Background(), "j1", "s3://path", "")

	cm.ClearCheckpoint("j1")
	assert.False(t, cm.HasCheckpoint("j1"))
}

func TestClearCheckpoint_NonExistent(t *testing.T) {
	cm := NewCheckpointManager()
	cm.ClearCheckpoint("doesnt-exist") // should not panic
}

func TestClearOldCheckpoints(t *testing.T) {
	cm := NewCheckpointManager()

	// Add an "old" checkpoint by directly manipulating
	cm.checkpoints["old-job"] = &CheckpointRecord{
		JobID:     "old-job",
		CreatedAt: time.Now().Add(-48 * time.Hour),
		IsValid:   true,
	}
	cm.RecordCheckpoint(context.Background(), "new-job", "s3://path", "")

	cleared := cm.ClearOldCheckpoints(24 * time.Hour)
	assert.Equal(t, 1, cleared)
	assert.False(t, cm.HasCheckpoint("old-job"))
	assert.True(t, cm.HasCheckpoint("new-job"))
}

func TestClearOldCheckpoints_NothingToClean(t *testing.T) {
	cm := NewCheckpointManager()
	cm.RecordCheckpoint(context.Background(), "j1", "s3://path", "")

	cleared := cm.ClearOldCheckpoints(24 * time.Hour)
	assert.Equal(t, 0, cleared)
}

// ============================================================================
// SECTION 7: Stats
// ============================================================================

func TestGetStats(t *testing.T) {
	cm := NewCheckpointManager()
	cm.RecordCheckpoint(context.Background(), "j1", "s3://path1", "")
	cm.RecordCheckpoint(context.Background(), "j2", "s3://path2", "")

	stats := cm.GetStats()
	assert.Equal(t, 2, stats["total_checkpoints"])
	assert.Equal(t, 2, stats["valid_checkpoints"])
}

func TestGetStats_Empty(t *testing.T) {
	cm := NewCheckpointManager()
	stats := cm.GetStats()
	assert.Equal(t, 0, stats["total_checkpoints"])
}

// ============================================================================
// SECTION 8: Helpers
// ============================================================================

func TestParseMetaInt(t *testing.T) {
	tests := []struct {
		metadata string
		key      string
		expected int
	}{
		{"epoch=47,loss=0.023", "epoch", 47},
		{"epoch=47,step=1500", "step", 1500},
		{"epoch=0", "epoch", 0},
		{"", "epoch", 0},
		{"loss=0.023", "epoch", 0},
		{"epoch=100", "epoch", 100},
		{"step=99,epoch=5", "epoch", 5},
	}

	for _, tt := range tests {
		result := parseMetaInt(tt.metadata, tt.key)
		assert.Equal(t, tt.expected, result, "parseMetaInt(%q, %q)", tt.metadata, tt.key)
	}
}

func TestTrimTrailingSlash(t *testing.T) {
	assert.Equal(t, "s3://bucket/path", trimTrailingSlash("s3://bucket/path/"))
	assert.Equal(t, "s3://bucket/path", trimTrailingSlash("s3://bucket/path"))
	assert.Equal(t, "", trimTrailingSlash(""))
}
