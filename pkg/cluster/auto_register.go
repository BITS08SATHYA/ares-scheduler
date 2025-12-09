// File: pkg/cluster/auto_register.go (NEW FILE)
// Auto-registration and heartbeat for worker clusters
// Automatically registers cluster on startup and sends periodic heartbeats
// CRITICAL: Enables automatic cluster discovery (no manual registration needed)

package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
)

// ============================================================================
// CLUSTER AUTO-REGISTRATION
// ============================================================================

// AutoRegistrationConfig: Configuration for auto-registration
type AutoRegistrationConfig struct {
	ClusterID          string
	Region             string
	Zone               string
	LocalSchedulerAddr string
	ControlPlaneURL    string
	TotalGPUs          int
	TotalCPUs          int
	TotalMemoryGB      float64
	GPUTopology        map[string]interface{}
}

// AutoRegisterCluster: Register cluster with control plane (blocking, with retries)
// Called at local scheduler startup
// Returns: error if registration ultimately fails
func AutoRegisterCluster(ctx context.Context, config *AutoRegistrationConfig) error {
	log := logger.Get()

	if config == nil || config.ClusterID == "" {
		return fmt.Errorf("invalid auto-registration config")
	}

	log.Info("========================================")
	log.Info("Auto-registering cluster: %s", config.ClusterID)
	log.Info("  Region: %s", config.Region)
	log.Info("  Zone: %s", config.Zone)
	log.Info("  Scheduler: %s", config.LocalSchedulerAddr)
	log.Info("  Control Plane: %s", config.ControlPlaneURL)
	log.Info("  GPUs: %d, CPUs: %d, Memory: %.0fGB", config.TotalGPUs, config.TotalCPUs, config.TotalMemoryGB)
	log.Info("========================================")

	// Build registration request
	regReq := &ClusterRegistrationRequest{
		ClusterID:          config.ClusterID,
		Region:             config.Region,
		Zone:               config.Zone,
		LocalSchedulerAddr: config.LocalSchedulerAddr,
		TotalGPUs:          config.TotalGPUs,
		TotalCPUs:          config.TotalCPUs,
		TotalMemoryGB:      config.TotalMemoryGB,
		GPUTopology:        config.GPUTopology,
	}

	// Try registration with exponential backoff
	maxRetries := 5
	backoff := 1 * time.Second

	for attempt := 0; attempt < maxRetries; attempt++ {
		log.Info("Registration attempt %d/%d...", attempt+1, maxRetries)

		err := sendRegistrationRequest(ctx, config.ControlPlaneURL, regReq)
		if err == nil {
			log.Info("✓ Cluster %s successfully registered with control plane", config.ClusterID)
			return nil
		}

		log.Warn("Registration attempt %d failed: %v", attempt+1, err)

		if attempt < maxRetries-1 {
			log.Info("Retrying in %.0f seconds...", backoff.Seconds())
			select {
			case <-time.After(backoff):
				backoff *= 2 // Exponential backoff
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return fmt.Errorf("failed to register cluster after %d attempts", maxRetries)
}

// sendRegistrationRequest: Send HTTP POST request to control plane
func sendRegistrationRequest(ctx context.Context, controlPlaneURL string, req *ClusterRegistrationRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request failed: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST",
		fmt.Sprintf("%s/clusters/register", controlPlaneURL),
		bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("create request failed: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("registration failed: status=%d, body=%s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

// ============================================================================
// CLUSTER HEARTBEAT (automatic health signaling)
// ============================================================================

// HeartbeatConfig: Configuration for periodic heartbeats
type HeartbeatConfig struct {
	ClusterID       string
	ControlPlaneURL string
	Interval        time.Duration
	GetLoadFunc     func() map[string]interface{} // Callback to get current load
}

// StartHeartbeat: Start sending periodic heartbeats
// Runs in background goroutine
// CRITICAL: Enables automatic health detection and autonomous mode failover
func StartHeartbeat(ctx context.Context, config *HeartbeatConfig) {
	if config == nil || config.ClusterID == "" {
		logger.Get().Error("Invalid heartbeat config")
		return
	}

	if config.Interval == 0 {
		config.Interval = 10 * time.Second
	}

	log := logger.Get()

	log.Info("Starting heartbeat for cluster %s (interval=%.0fs)", config.ClusterID, config.Interval.Seconds())

	ticker := time.NewTicker(config.Interval)
	defer ticker.Stop()

	successCount := 0
	failureCount := 0

	for {
		select {
		case <-ctx.Done():
			log.Info("Heartbeat stopped for cluster %s (sent=%d, failed=%d)",
				config.ClusterID, successCount, failureCount)
			return

		case <-ticker.C:
			// Get current cluster load
			load := config.GetLoadFunc()

			// Build heartbeat request
			hbReq := &ClusterHeartbeatRequest{
				ClusterID:   config.ClusterID,
				GPUsInUse:   load["gpus_in_use"].(int),
				MemGBInUse:  load["mem_gb_in_use"].(float64),
				CPUsInUse:   load["cpus_in_use"].(int),
				RunningJobs: load["running_jobs"].(int),
				PendingJobs: load["pending_jobs"].(int),
				Status:      "healthy", // TODO: Check actual health
			}

			// Send heartbeat
			err := sendHeartbeatRequest(ctx, config.ControlPlaneURL, hbReq)
			if err != nil {
				failureCount++
				log.Debug("Heartbeat failed: %v (failures=%d)", err, failureCount)

				// After 3 consecutive failures, log warning
				if failureCount%3 == 0 {
					log.Warn("Heartbeat failing for %.0f seconds (cluster may enter autonomous mode)",
						config.Interval.Seconds()*float64(failureCount))
				}
			} else {
				successCount++
				failureCount = 0 // Reset failure counter

				log.Debug("Heartbeat sent: jobs=%d, gpus=%d, status=%s",
					hbReq.RunningJobs, hbReq.GPUsInUse, hbReq.Status)
			}
		}
	}
}

// sendHeartbeatRequest: Send HTTP POST heartbeat to control plane
func sendHeartbeatRequest(ctx context.Context, controlPlaneURL string, req *ClusterHeartbeatRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request failed: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST",
		fmt.Sprintf("%s/cluster-heartbeat", controlPlaneURL),
		bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("create request failed: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("heartbeat failed: status=%d", resp.StatusCode)
	}

	return nil
}

// ============================================================================
// CLUSTER REQUEST TYPES (from handler, but useful here)
// ============================================================================

//// ClusterRegistrationRequest: Auto-registration request
//type ClusterRegistrationRequest struct {
//	ClusterID          string                 `json:"cluster_id"`
//	Region             string                 `json:"region"`
//	Zone               string                 `json:"zone"`
//	LocalSchedulerAddr string                 `json:"local_scheduler_addr"`
//	TotalGPUs          int                    `json:"total_gpus"`
//	TotalCPUs          int                    `json:"total_cpus"`
//	TotalMemoryGB      float64                `json:"total_memory_gb"`
//	GPUTopology        map[string]interface{} `json:"gpu_topology,omitempty"`
//	Labels             map[string]string      `json:"labels,omitempty"`
//}
//
//// ClusterHeartbeatRequest: Periodic heartbeat request
//type ClusterHeartbeatRequest struct {
//	ClusterID   string  `json:"cluster_id"`
//	GPUsInUse   int     `json:"gpus_in_use"`
//	MemGBInUse  float64 `json:"mem_gb_in_use"`
//	CPUsInUse   int     `json:"cpus_in_use"`
//	RunningJobs int     `json:"running_jobs"`
//	PendingJobs int     `json:"pending_jobs"`
//	Status      string  `json:"status"`
//}
