package global

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/local"
)

// ============================================================================
// LOCAL SCHEDULER HTTP CLIENT (New integration layer)
// ============================================================================
// GlobalScheduler uses this to call LocalScheduler on remote cluster

type LocalSchedulerClient struct {
	httpClient *http.Client
	log        *logger.Logger
}

// LocalScheduleRequest: Request to schedule job on local cluster
type LocalScheduleRequest struct {
	JobID             string          `json:"job_id"`
	JobSpec           *common.JobSpec `json:"job_spec"`
	LeaseID           int64           `json:"lease_id,omitempty"`
	FencingToken      string          `json:"fencing_token,omitempty"`
	CheckpointEnabled bool            `json:"checkpoint_enabled,omitempty"`
	CheckpointPath    string          `json:"checkpoint_path,omitempty"`
	CheckpointRestore string          `json:"checkpoint_restore,omitempty"`
	CheckpointMeta    string          `json:"checkpoint_meta,omitempty"`
}

// LocalScheduleResponse: Response from local scheduler
type LocalScheduleResponse struct {
	Success  bool                           `json:"success"`
	Decision *local.LocalSchedulingDecision `json:"decision,omitempty"`
	Error    string                         `json:"error,omitempty"`
}

// NewLocalSchedulerClient creates new client
// Skips TLS verification for self-signed K8s certificates (testing only)
func NewLocalSchedulerClient() *LocalSchedulerClient {

	// Create transport with TLS config that skips certificate verification
	// This is necessary for K8s self-signed certificates
	// FOR TESTING ONLY - Never use in production!
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // Skip cert verification for testing
		},
	}

	return &LocalSchedulerClient{
		httpClient: &http.Client{
			Transport: tr,
			Timeout:   60 * time.Second,
		},
		log: logger.Get(),
	}
}

// ScheduleJob calls remote LocalScheduler to schedule job
//
// RPC: HTTP POST to LocalScheduler
// URL: {localSchedulerAddr}/schedule
// Request: JobSpec
// Response: LocalSchedulingDecision
//
// This is the critical integration between Layer 7 (GlobalScheduler) and Layer 6 (LocalScheduler)
func (c *LocalSchedulerClient) ScheduleJob(
	ctx context.Context,
	localSchedulerAddr string,
	jobRecord *common.Job,
) (*local.LocalSchedulingDecision, error) {

	c.log.Debug("Local Scheduler method Entered()")

	if localSchedulerAddr == "" {
		return nil, fmt.Errorf("local scheduler address cannot be empty")
	}

	if jobRecord.Spec == nil {
		return nil, fmt.Errorf("job spec cannot be nil")
	}

	// Prepare request with fencing token
	var leaseID int64
	var fencingToken string
	if jobRecord.ExecutionLease != nil && jobRecord.ExecutionLease.LeaseID != "" {
		parsed, parseErr := strconv.ParseInt(jobRecord.ExecutionLease.LeaseID, 10, 64)
		if parseErr == nil {
			leaseID = parsed
			fencingToken = fmt.Sprintf("ares-fence-%s-%d", jobRecord.ID, leaseID)
		}
	}

	req := &LocalScheduleRequest{
		JobID:             jobRecord.ID,
		JobSpec:           jobRecord.Spec,
		LeaseID:           leaseID,
		FencingToken:      fencingToken,
		CheckpointEnabled: jobRecord.Spec.CheckpointEnabled,
		CheckpointPath:    fmt.Sprintf("%s/%s", trimTrailingSlash(jobRecord.Spec.CheckpointPath), jobRecord.ID),
		CheckpointRestore: jobRecord.LastCheckpointPath,
		CheckpointMeta:    jobRecord.LastCheckpointMeta,
	}

	reqBody, err := json.Marshal(req)
	if err != nil {
		c.log.Error("Failed to marshal request: %v", err)
		return nil, fmt.Errorf("marshal failed: %w", err)
	}

	// Make HTTP POST request
	url := fmt.Sprintf("%s/schedule", localSchedulerAddr)
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(reqBody))
	if err != nil {
		c.log.Error("Failed to create HTTP request: %v", err)
		return nil, fmt.Errorf("request creation failed: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Request-ID", jobRecord.ID)

	c.log.Debug("Calling LocalScheduler: POST %s", url)

	c.log.Debug("The RequestBody is: ", reqBody)

	// Send request
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		c.log.Error("HTTP request failed: %v", err)
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	// Parse response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		c.log.Error("Failed to read response body: %v", err)
		return nil, fmt.Errorf("read response failed: %w", err)
	}

	var respData LocalScheduleResponse
	err = json.Unmarshal(body, &respData)
	if err != nil {
		c.log.Error("Failed to unmarshal response: %v", err)
		return nil, fmt.Errorf("unmarshal response failed: %w", err)
	}

	// Check response status
	if !respData.Success {
		c.log.Warn("LocalScheduler returned error: %s", respData.Error)
		return nil, fmt.Errorf("local scheduling failed: %s", respData.Error)
	}

	if respData.Decision == nil {
		c.log.Error("LocalScheduler returned no decision")
		return nil, fmt.Errorf("local scheduler returned empty decision")
	}

	c.log.Info("Local scheduling succeeded: node=%s, gpus=%v",
		respData.Decision.NodeID, respData.Decision.GPUIndices)

	return respData.Decision, nil
}

// GetClusterHealth gets cluster health status via HTTP
func (c *LocalSchedulerClient) GetClusterHealth(
	ctx context.Context,
	localSchedulerAddr string,
) (map[string]interface{}, error) {

	url := fmt.Sprintf("%s/health", localSchedulerAddr)
	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("request creation failed: %w", err)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	var health map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&health)
	if err != nil {
		return nil, fmt.Errorf("unmarshal response failed: %w", err)
	}

	return health, nil
}

func trimTrailingSlash(path string) string {
	if len(path) > 0 && path[len(path)-1] == '/' {
		return path[:len(path)-1]
	}
	return path
}
