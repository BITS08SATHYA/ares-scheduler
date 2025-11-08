package job

import (
	"encoding/json"
	"time"
)

// JobState represents the lifecycle state of a job
type JobState string

const (
	StatePending   JobState = "PENDING"
	StateRunning   JobState = "RUNNING"
	StateSucceeded JobState = "SUCCEEDED"
	StateFailed    JobState = "FAILED"
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
}

// Serialize converts Job to JSON string
func (j *Job) Serialize() (string, error) {
	data, err := json.Marshal(j)
	if err != nil {
		return "Marshalling Failed", err
	}
	return string(data), nil
}

// Deserialization converts JSON string back to Job
func DeserializeJob(data string) (*Job, error) {
	var job Job
	err := json.Unmarshal([]byte(data), &job)
	if err != nil {
		return nil, err
	}
	return &job, nil
}

// JobSubmission represents a clients' job submission request
type JobSubmission struct {
	RequestID string   `json:"request_id"`
	Name      string   `json:"name"`
	Image     string   `json:"image"`
	Command   []string `json:"command"`
	CPUs      int      `json:"cpus"`
	GPUs      int      `json:"gpus"`
}
