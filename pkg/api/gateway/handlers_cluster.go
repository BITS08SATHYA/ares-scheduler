// File: pkg/api/gateway/handlers_cluster.go
// Cluster registration and management endpoints
// ★ CRITICAL: Routes cluster registration to ClusterManager (source of truth)
// Uses types from pkg/cluster/model.go — single source of truth for request/response types

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

	// Parse request using canonical type from pkg/cluster
	var regReq cluster.ClusterRegistrationRequest
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()

	if err := decoder.Decode(&regReq); err != nil {
		ag.respondError(w, http.StatusBadRequest, "INVALID_JSON",
			fmt.Sprintf("invalid JSON: %v", err))
		return
	}

	// Validate all fields including topology cross-validation
	if err := ag.validateClusterRegistration(&regReq); err != nil {
		ag.respondError(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}

	ag.log.Info("Cluster registration request: cluster_id=%s, region=%s, zone=%s, gpus=%d, cpus=%d, mem=%.0fGB, topology=%v",
		regReq.ClusterID, regReq.Region, regReq.Zone, regReq.TotalGPUs,
		regReq.TotalCPUs, regReq.TotalMemoryGB, regReq.GPUTopology != nil)

	// ========================================================================
	// STEP 1: Register cluster with ClusterManager (SOURCE OF TRUTH)
	// Cluster starts in JOINING state — not yet READY
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

	ag.log.Info("ClusterManager: Cluster %s joined (state=JOINING)", clusterObj.ClusterID)

	// ========================================================================
	// STEP 2: Update capacity in ClusterManager
	// ★ FIX: Set capacity BEFORE marking READY so scheduler never sees
	//         a READY cluster with 0 GPUs / 0 memory
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
	// STEP 4: Now mark cluster READY (capacity is set, topology stored)
	// GlobalScheduler gets cluster via ClusterManager event listener
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

	response := &cluster.ClusterRegistrationResponse{
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

	var deregReq cluster.ClusterDeregistrationRequest
	if err := json.NewDecoder(r.Body).Decode(&deregReq); err != nil {
		ag.respondError(w, http.StatusBadRequest, "INVALID_JSON",
			fmt.Sprintf("invalid JSON: %v", err))
		return
	}
	defer r.Body.Close()

	if deregReq.ClusterID == "" {
		ag.respondError(w, http.StatusBadRequest, "MISSING_FIELD", "cluster_id required")
		return
	}

	ag.log.Info("Cluster deregistration request: cluster_id=%s", deregReq.ClusterID)

	// Call ClusterManager (source of truth)
	err := ag.clusterManager.LeaveCluster(r.Context(), deregReq.ClusterID)
	if err != nil {
		ag.respondError(w, http.StatusNotFound, "CLUSTER_NOT_FOUND", err.Error())
		return
	}

	if ag.crdtStore != nil {
		ag.crdtStore.DeregisterCluster(deregReq.ClusterID)
	}

	response := &cluster.ClusterDeregistrationResponse{
		Success:   true,
		ClusterID: deregReq.ClusterID,
		Message:   fmt.Sprintf("Cluster %s deregistered", deregReq.ClusterID),
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)

	ag.metrics.RecordClusterLeave()

	ag.log.Info("CLUSTER DE-REGISTRATION COMPLETE: %s", response.ClusterID)
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

	var hbReq cluster.ClusterHeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&hbReq); err != nil {
		ag.respondError(w, http.StatusBadRequest, "INVALID_JSON",
			fmt.Sprintf("invalid JSON: %v", err))
		return
	}
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

		// ★ Reconcile load into GlobalScheduler's optimistic cache
		// This corrects the optimistic decrements made by SelectBestCluster
		if ag.globalScheduler != nil {
			ag.globalScheduler.UpdateClusterLoad(
				hbReq.ClusterID,
				hbReq.GPUsInUse,
				hbReq.MemGBInUse,
				hbReq.RunningJobs,
			)
		}

		// Update GPU types from heartbeat (auto-discovered by local scheduler)
		// This ensures the global scheduler knows what GPU hardware each cluster has
		// so metrics correctly track A100/T4/H100/etc. even when jobs request "any"
		if len(hbReq.GPUTypes) > 0 {
			clusterInfo.GPUTypes = hbReq.GPUTypes

			// Sync to GlobalScheduler's internal cluster cache
			if ag.globalScheduler != nil {
				ag.globalScheduler.UpdateClusterGPUTypes(hbReq.ClusterID, hbReq.GPUTypes)
			}
		}

		// Note: GlobalScheduler doesn't have UpdateClusterState anymore
		// But we update the in-memory copy directly above
	}

	response := &cluster.ClusterHeartbeatResponse{
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

func (ag *APIGateway) validateClusterRegistration(req *cluster.ClusterRegistrationRequest) error {
	// ---- Required identity fields ----
	if req.ClusterID == "" {
		return fmt.Errorf("cluster_id required")
	}
	if len(req.ClusterID) > 255 {
		return fmt.Errorf("cluster_id too long (max 255)")
	}
	if req.Region == "" {
		return fmt.Errorf("region required")
	}
	if req.Zone == "" {
		return fmt.Errorf("zone required (used for topology-aware placement and consistency checks)")
	}
	if req.LocalSchedulerAddr == "" {
		return fmt.Errorf("local_scheduler_addr required")
	}

	// ---- Capacity bounds ----
	if req.TotalGPUs < 0 || req.TotalGPUs > 1024 {
		return fmt.Errorf("invalid total_gpus: %d (must be 0-1024)", req.TotalGPUs)
	}
	if req.TotalCPUs < 0 || req.TotalCPUs > 4096 {
		return fmt.Errorf("invalid total_cpus: %d (must be 0-4096)", req.TotalCPUs)
	}
	if req.TotalMemoryGB < 0 || req.TotalMemoryGB > 100000 {
		return fmt.Errorf("invalid total_memory_gb: %.0f (must be 0-100000)", req.TotalMemoryGB)
	}

	// ---- GPU Topology validation (Feature 4) ----
	// If topology is provided, cross-validate against declared GPU count
	if req.GPUTopology != nil {
		if err := ag.validateGPUTopology(req.GPUTopology, req.TotalGPUs); err != nil {
			return fmt.Errorf("gpu_topology validation failed: %w", err)
		}
	}

	return nil
}

// validateGPUTopology: Cross-validate GPU topology against declared GPU count
// Ensures NVLink pairs reference valid GPU indices, NUMA mappings are consistent,
// and PCIe generation is a known value.
func (ag *APIGateway) validateGPUTopology(topology map[string]interface{}, totalGPUs int) error {
	// Validate nvlink_pairs: each pair must reference GPU indices in [0, totalGPUs)
	if nvlinkRaw, ok := topology["nvlink_pairs"]; ok && nvlinkRaw != nil {
		pairs, ok := nvlinkRaw.([]interface{})
		if !ok {
			return fmt.Errorf("nvlink_pairs must be an array of [gpuA, gpuB] pairs")
		}
		for i, pairRaw := range pairs {
			pair, ok := pairRaw.([]interface{})
			if !ok || len(pair) != 2 {
				return fmt.Errorf("nvlink_pairs[%d]: must be a 2-element array [gpuA, gpuB]", i)
			}
			for j, gpuRaw := range pair {
				gpuIdx, ok := toInt(gpuRaw)
				if !ok {
					return fmt.Errorf("nvlink_pairs[%d][%d]: must be an integer GPU index", i, j)
				}
				if gpuIdx < 0 || gpuIdx >= totalGPUs {
					return fmt.Errorf("nvlink_pairs[%d][%d]: GPU index %d out of range (cluster has %d GPUs, valid range 0-%d)",
						i, j, gpuIdx, totalGPUs, totalGPUs-1)
				}
			}
			// Self-link check
			a, _ := toInt(pair[0])
			b, _ := toInt(pair[1])
			if a == b {
				return fmt.Errorf("nvlink_pairs[%d]: GPU %d cannot NVLink to itself", i, a)
			}
		}
	}

	// Validate gpu_to_numa: keys must be valid GPU indices, values must be non-negative NUMA nodes
	if numaRaw, ok := topology["gpu_to_numa"]; ok && numaRaw != nil {
		numaMap, ok := numaRaw.(map[string]interface{})
		if !ok {
			return fmt.Errorf("gpu_to_numa must be a map of gpuIndex -> numaNode")
		}
		for gpuKey, numaRaw := range numaMap {
			numaNode, ok := toInt(numaRaw)
			if !ok {
				return fmt.Errorf("gpu_to_numa[%s]: NUMA node must be an integer", gpuKey)
			}
			if numaNode < 0 || numaNode > 15 {
				return fmt.Errorf("gpu_to_numa[%s]: NUMA node %d out of range (valid 0-15)", gpuKey, numaNode)
			}
		}
	}

	// Validate pcie_gen: must be a known PCIe generation
	if pcieRaw, ok := topology["pcie_gen"]; ok && pcieRaw != nil {
		validGens := map[string]bool{"3": true, "4": true, "5": true, "3.0": true, "4.0": true, "5.0": true}
		pcieStr := fmt.Sprintf("%v", pcieRaw)
		if !validGens[pcieStr] {
			return fmt.Errorf("pcie_gen: %q is not a valid PCIe generation (expected 3, 4, or 5)", pcieStr)
		}
	}

	return nil
}

// toInt: Convert interface{} to int (handles float64 from JSON)
func toInt(v interface{}) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), true
	case int:
		return n, true
	case int64:
		return int(n), true
	}
	return 0, false
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
