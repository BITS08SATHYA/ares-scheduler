package job

import (
	"encoding/json"
	"fmt"
	"time"
)

// JobState represents the lifecycle state of a job
type JobState string

const (
	StatePending   JobState = "PENDING"
	StateRunning   JobState = "RUNNING"
	StateSucceeded JobState = "SUCCEEDED"
	StateFailed    JobState = "FAILED"
	StateRetrying  JobState = "RETRYING" // Retry Scenario
)

// Job Represents a unit of work to be executed
type Job struct {
	//	Identity
	JobID     string `json:"job_id"`
	Name      string `json:"name"`
	RequestID string `json:"request_id"` // For idempotency

	// Execution Details
	Image   string   `json:"image"`
	Command []string `json:"command"`

	//	Resources
	CPUs int `json:"cpus"`
	GPUs int `json:"gpus"`

	//	Lifecycle
	State       JobState  `json:"state"`
	CreatedAt   time.Time `json:"created_at"`
	StartedAt   time.Time `json:"started_at,omitempty"`
	CompletedAt time.Time `json:"completed_at,omitempty"`

	//	Exactly-Once tracking
	ExecutionCount int `json:"execution_count"`

	// Retry tracking
	RetryCount  int          `json:"retry_count"`
	MaxRetries  int          `json:"max_retries"`
	LastFailure string       `json:"last_failure,omitempty"`
	NextRetryAt time.Time    `json:"next_retry_at,omitempty"`
	RetryPolicy *RetryPolicy `json:"retry_policy,omitempty"`
}

// JobSubmission represents a clients' job submission request
type JobSubmission struct {
	RequestID  string   `json:"request_id"`
	Name       string   `json:"name"`
	Image      string   `json:"image"`
	Command    []string `json:"command"`
	CPUs       int      `json:"cpus"`
	GPUs       int      `json:"gpus"`
	MaxRetries int      `json:"max_retries,omitempty"`
}

// Serialize converts Job to JSON string
func (j *Job) Serialize() (string, error) {
	data, err := json.Marshal(j)
	if err != nil {
		return "Marshalling Failed", err
	}
	return string(data), nil
}

// DeserializeJob converts JSON string back to Job
func DeserializeJob(data string) (*Job, error) {
	var j Job
	if err := json.Unmarshal([]byte(data), &j); err != nil {
		return nil, fmt.Errorf("failed to deserialize job: %w", err)
	}
	return &j, nil
}
