// File: pkg/api/gateway/handlers_cluster.go (NEW FILE)
// Cluster registration and management endpoints
// ★ CRITICAL: Routes cluster registration to ClusterManager (source of truth)

package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/cluster"
	"net/http"
	"time"
)

// ============================================================================
// CLUSTER REGISTRATION REQUEST/RESPONSE TYPES
// ============================================================================

// ClusterRegistrationRequest: HTTP request for cluster registration
// Sent by worker cluster when it starts
type ClusterRegistrationRequest struct {
	// Required
	ClusterID          string `json:"cluster_id"`           // "cluster-us-west-2a"
	Region             string `json:"region"`               // "us-west"
	Zone               string `json:"zone"`                 // "us-west-2a"
	LocalSchedulerAddr string `json:"local_scheduler_addr"` // "http://localhost:9090"

	// Capacity
	TotalGPUs     int     `json:"total_gpus"`      // 8
	TotalCPUs     int     `json:"total_cpus"`      // 256
	TotalMemoryGB float64 `json:"total_memory_gb"` // 512.0

	// GPU Topology (Feature 4 - Topology-Aware Scheduling)
	// ★ CRITICAL: Includes NVLink connections and NUMA info
	GPUTopology map[string]interface{} `json:"gpu_topology"`

	// Metadata
	Labels map[string]string `json:"labels,omitempty"`
}

type ClusterRegistrationResponse struct {
	Success   bool   `json:"success"`
	ClusterID string `json:"cluster_id"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
}

// ============================================================================
// CLUSTER DEREGISTRATION
// ============================================================================

type ClusterDeregistrationRequest struct {
	ClusterID string `json:"cluster_id"`
}

type ClusterDeregistrationResponse struct {
	Success   bool   `json:"success"`
	ClusterID string `json:"cluster_id"`
	Message   string `json:"message"`
}

// ============================================================================
// CLUSTER HEARTBEAT (Health Update)
// ============================================================================

type ClusterHeartbeatRequest struct {
	ClusterID   string  `json:"cluster_id"`
	GPUsInUse   int     `json:"gpus_in_use"`
	MemGBInUse  float64 `json:"mem_gb_in_use"`
	CPUsInUse   int     `json:"cpus_in_use"`
	RunningJobs int     `json:"running_jobs"`
	PendingJobs int     `json:"pending_jobs"`
	Status      string  `json:"status"` // "healthy", "degraded", "unhealthy"
}

type ClusterHeartbeatResponse struct {
	Success   bool   `json:"success"`
	ClusterID string `json:"cluster_id"`
	Message   string `json:"message"`
}

// ============================================================================
// HANDLER: POST /clusters/register
// ============================================================================

// handleRegisterCluster: Process cluster registration request
// ★ CRITICAL: This routes to ClusterManager (source of truth)
// NOT to GlobalScheduler
func (ag *APIGateway) handleRegisterCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		ag.respondError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			fmt.Sprintf("expected POST, got %s", r.Method))
		return
	}

	// Parse request
	var regReq ClusterRegistrationRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&regReq); err != nil {
		ag.respondError(w, http.StatusBadRequest, "INVALID_JSON",
			fmt.Sprintf("invalid JSON: %v", err))
		return
	}
	defer r.Body.Close()

	// Validate
	if err := ag.validateClusterRegistration(&regReq); err != nil {
		ag.respondError(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}

	ag.log.Info("Cluster registration request: cluster_id=%s, region=%s, gpus=%d, topology=%v",
		regReq.ClusterID, regReq.Region, regReq.TotalGPUs,
		regReq.GPUTopology != nil)

	// ========================================================================
	// STEP 1: Register cluster with ClusterManager (SOURCE OF TRUTH)
	// ========================================================================

	clusterConfig := &cluster.ClusterConfig{
		ClusterID:   regReq.ClusterID,
		Name:        regReq.ClusterID,
		Region:      regReq.Region,
		Zone:        regReq.Zone,
		ControlAddr: regReq.LocalSchedulerAddr,
		Labels:      regReq.Labels,
	}

	clusterObj, err := ag.clusterManager.JoinCluster(r.Context(), clusterConfig)

	if err != nil {
		ag.log.Error("ClusterManager.JoinCluster failed: %v", err)
		ag.respondError(w, http.StatusConflict, "REGISTRATION_FAILED", err.Error())
		return
	}

	if ag.crdtStore != nil {
		ag.crdtStore.RegisterCluster(clusterConfig.ClusterID, clusterConfig.Region, clusterConfig.Zone)
	}

	ag.log.Info("ClusterManager: Cluster %s joined", clusterObj.ClusterID)

	// ========================================================================
	// STEP 2: Update capacity in ClusterManager
	// ========================================================================

	err = ag.clusterManager.UpdateClusterCapacity(r.Context(),
		regReq.ClusterID,
		regReq.TotalGPUs,
		regReq.TotalCPUs,
		regReq.TotalMemoryGB,
	)
	if err != nil {
		ag.log.Warn("Failed to update cluster capacity (non-fatal): %v", err)
	}

	ag.log.Info("Updated cluster %s capacity: GPUs=%d, CPUs=%d, Memory=%.0fGB",
		regReq.ClusterID, regReq.TotalGPUs, regReq.TotalCPUs, regReq.TotalMemoryGB)

	// ========================================================================
	// STEP 3: Store GPU Topology (Feature 4 - Topology-Aware Scheduling)
	// ========================================================================

	if regReq.GPUTopology != nil {
		err := ag.storeGPUTopology(r.Context(), regReq.ClusterID, regReq.GPUTopology)
		if err != nil {
			ag.log.Warn("Failed to store GPU topology (non-fatal): %v", err)
		} else {
			ag.log.Info("Stored GPU topology for cluster %s", regReq.ClusterID)
		}
	}

	// ========================================================================
	// STEP 4: GlobalScheduler automatically gets cluster via ClusterManager event
	// (OnClusterJoin is called by ClusterManager.JoinCluster)
	// ========================================================================

	// Verify GlobalScheduler knows about the cluster
	_, err = ag.clusterManager.GetClusterInfo(regReq.ClusterID)
	if err != nil {
		ag.log.Warn("GlobalScheduler not yet aware of cluster %s (will be after event)", regReq.ClusterID)
	} else {
		ag.log.Info("GlobalScheduler is aware of cluster %s", regReq.ClusterID)
	}

	// ========================================================================
	// RESPONSE
	// ========================================================================

	response := &ClusterRegistrationResponse{
		Success:   true,
		ClusterID: regReq.ClusterID,
		Message:   fmt.Sprintf("Cluster %s registered successfully in federation", regReq.ClusterID),
		Timestamp: time.Now().Format(time.RFC3339),
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)

	ag.metrics.RecordClusterJoin()

	ag.log.Info("CLUSTER REGISTRATION COMPLETE: %s", clusterObj.ClusterID)
}

// ============================================================================
// HANDLER: POST /clusters/deregister
// ============================================================================

func (ag *APIGateway) handleDeregisterCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		ag.respondError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			fmt.Sprintf("expected POST, got %s", r.Method))
		return
	}

	var deregReq ClusterDeregistrationRequest
	json.NewDecoder(r.Body).Decode(&deregReq)
	defer r.Body.Close()

	if deregReq.ClusterID == "" {
		ag.respondError(w, http.StatusBadRequest, "MISSING_FIELD", "cluster_id required")
		return
	}

	// Call ClusterManager (source of truth)
	err := ag.clusterManager.LeaveCluster(r.Context(), deregReq.ClusterID)
	if err != nil {
		ag.respondError(w, http.StatusNotFound, "CLUSTER_NOT_FOUND", err.Error())
		return
	}

	if ag.crdtStore != nil {
		ag.crdtStore.DeregisterCluster(deregReq.ClusterID)
	}

	ag.log.Info("Cluster %s deregistered", deregReq.ClusterID)

	response := &ClusterDeregistrationResponse{
		Success:   true,
		ClusterID: deregReq.ClusterID,
		Message:   fmt.Sprintf("Cluster %s deregistered", deregReq.ClusterID),
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)

	ag.metrics.RecordClusterLeave()

	ag.log.Info("CLUSTER REGISTRATION COMPLETE: %s", response.ClusterID)
}

// ============================================================================
// HANDLER: POST /clusters/:id/heartbeat
// ============================================================================

// handleClusterHeartbeat: Process cluster health/load update
// Called by worker cluster periodically (every 10 seconds)
func (ag *APIGateway) handleClusterHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		ag.respondError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			fmt.Sprintf("expected POST, got %s", r.Method))
		return
	}

	var hbReq ClusterHeartbeatRequest
	json.NewDecoder(r.Body).Decode(&hbReq)
	defer r.Body.Close()

	if hbReq.ClusterID == "" {
		ag.respondError(w, http.StatusBadRequest, "MISSING_FIELD", "cluster_id required")
		return
	}

	// ========================================================================
	// Update ClusterManager with current load
	// ========================================================================

	err := ag.clusterManager.UpdateClusterLoad(r.Context(),
		hbReq.ClusterID,
		hbReq.GPUsInUse,
		hbReq.CPUsInUse,
		hbReq.MemGBInUse,
		hbReq.RunningJobs,
		hbReq.PendingJobs,
	)
	if err != nil {
		ag.log.Warn("Failed to update cluster load: %v", err)
		ag.respondError(w, http.StatusNotFound, "CLUSTER_NOT_FOUND", err.Error())
		return
	}
	// updating the timestamp of the Clusters' LastHeartBeat to the Global control plane
	ag.clusterManager.UpdateHeartbeatTimestamp(hbReq.ClusterID)

	// ★ CRDT: Update cluster load in CRDT state store
	if ag.crdtStore != nil {
		ag.crdtStore.UpdateClusterLoad(
			hbReq.ClusterID,
			hbReq.GPUsInUse,
			hbReq.MemGBInUse,
			hbReq.RunningJobs,
		)
	}

	// ========================================================================
	// Update GlobalScheduler's copy with same load info
	// ========================================================================

	clusterInfo, err := ag.clusterManager.GetClusterInfo(hbReq.ClusterID)
	if err == nil && clusterInfo != nil {
		clusterInfo.AvailableGPUs = clusterInfo.TotalGPUs - hbReq.GPUsInUse
		clusterInfo.AvailableMemoryGB = clusterInfo.TotalMemoryGB - hbReq.MemGBInUse
		clusterInfo.RunningJobsCount = hbReq.RunningJobs
		clusterInfo.LastHeartbeat = time.Now()

		// Note: GlobalScheduler doesn't have UpdateClusterState anymore
		// But we update the in-memory copy directly above
	}

	response := &ClusterHeartbeatResponse{
		Success:   true,
		ClusterID: hbReq.ClusterID,
		Message:   "Heartbeat received",
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)

	ag.metrics.RecordHeartbeat()

	// ★ FIX: Aggregate GPU counts across ALL clusters, not just this one
	clusters := ag.clusterManager.ListClusters()
	var totalGPUs, gpusInUse int32
	for _, c := range clusters {
		totalGPUs += int32(c.TotalGPUs)
		gpusInUse += int32(c.GPUsInUse)
	}
	ag.metrics.SetGPUCounts(totalGPUs, gpusInUse, totalGPUs-gpusInUse)

	ag.log.Debug("Heartbeat from %s: gpus=%d/%d, jobs=%d running",
		hbReq.ClusterID, hbReq.GPUsInUse, clusterInfo.TotalGPUs, hbReq.RunningJobs)
}

// ============================================================================
// HANDLER: GET /clusters (NEW - List all clusters)
// ============================================================================

func (ag *APIGateway) handleListClusters(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		ag.respondError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			fmt.Sprintf("expected GET, got %s", r.Method))
		return
	}

	// Get clusters from ClusterManager (source of truth)
	clusters := ag.clusterManager.ListClusters()

	items := make([]*cluster.ClusterListItem, 0, len(clusters))
	for _, clusterObj := range clusters {
		item := &cluster.ClusterListItem{
			ClusterID:       clusterObj.ClusterID,
			Region:          clusterObj.Region,
			Zone:            clusterObj.Zone,
			IsHealthy:       clusterObj.IsHealthy,
			TotalGPUs:       clusterObj.TotalGPUs,
			GPUsInUse:       clusterObj.GPUsInUse,
			AvailableGPUs:   clusterObj.AvailableGPUs(),
			TotalMemoryGB:   clusterObj.TotalMemGB,
			MemGBInUse:      clusterObj.MemGBInUse,
			RunningJobs:     clusterObj.RunningJobs,
			PendingJobs:     clusterObj.PendingJobs,
			LastHeartbeatAt: clusterObj.LastHeartbeatAt.Format(time.RFC3339),
		}
		items = append(items, item)
	}

	response := &cluster.ListClustersResponse{
		Success:   true,
		Clusters:  items,
		Count:     len(items),
		Timestamp: time.Now().Format(time.RFC3339),
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// ============================================================================
// VALIDATION & HELPERS
// ============================================================================

func (ag *APIGateway) validateClusterRegistration(req *ClusterRegistrationRequest) error {
	if req.ClusterID == "" {
		return fmt.Errorf("cluster_id required")
	}
	if len(req.ClusterID) > 255 {
		return fmt.Errorf("cluster_id too long (max 255)")
	}

	if req.Region == "" {
		return fmt.Errorf("region required")
	}

	if req.LocalSchedulerAddr == "" {
		return fmt.Errorf("local_scheduler_addr required")
	}

	if req.TotalGPUs < 0 || req.TotalGPUs > 1024 {
		return fmt.Errorf("invalid total_gpus: %d (must be 0-1024)", req.TotalGPUs)
	}

	if req.TotalMemoryGB < 0 || req.TotalMemoryGB > 100000 {
		return fmt.Errorf("invalid total_memory_gb: %.0f", req.TotalMemoryGB)
	}

	return nil
}

// storeGPUTopology: Store GPU topology in Redis
// Used by Feature 4 (Topology-Aware Scheduling)
func (ag *APIGateway) storeGPUTopology(
	ctx context.Context,
	clusterID string,
	topology map[string]interface{},
) error {

	if ag.redisClient == nil {
		return fmt.Errorf("redis client not available")
	}

	// Serialize topology
	topologyData, err := json.Marshal(topology)
	if err != nil {
		return fmt.Errorf("marshal topology failed: %w", err)
	}

	// Store in Redis with 24 hour TTL
	// Key format: ares:topology:cluster:{clusterID}
	key := fmt.Sprintf("ares:topology:cluster:%s", clusterID)
	err = ag.redisClient.Set(ctx, key, string(topologyData), 24*time.Hour)
	if err != nil {
		return fmt.Errorf("redis set failed: %w", err)
	}

	ag.log.Debug("Stored GPU topology for cluster %s: %d bytes",
		clusterID, len(topologyData))

	return nil
}

// ============================================================================
// REGISTER CLUSTER ROUTES
// ============================================================================

// These should be called from RegisterRoutes() in gateway.go
func (ag *APIGateway) registerClusterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/clusters/register", ag.wrapHandler(ag.handleRegisterCluster))
	mux.HandleFunc("/clusters/deregister", ag.wrapHandler(ag.handleDeregisterCluster))
	mux.HandleFunc("/clusters/list", ag.wrapHandler(ag.handleListClusters))
	// For heartbeat: POST /clusters/{id}/heartbeat
	// Note: Standard Go http.ServeMux doesn't support path parameters
	// You'd need a router like gorilla/mux or just use: /cluster-heartbeat
	mux.HandleFunc("/cluster-heartbeat", ag.wrapHandler(ag.handleClusterHeartbeat))

	ag.log.Info("Registered cluster management routes:")
	ag.log.Info("  POST   /clusters/register")
	ag.log.Info("  POST   /clusters/deregister")
	ag.log.Info("  POST   /cluster-heartbeat")
	ag.log.Info("  GET    /clusters/list")
}
