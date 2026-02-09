// File: pkg/api/gateway/handlers_gang.go
// HTTP handlers for gang scheduling endpoints

package gateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/common"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/scheduler/gang"
)

// ============================================================================
// REQUEST / RESPONSE TYPES
// ============================================================================

// GangSubmitRequest: POST /gang/submit request body
type GangSubmitRequest struct {
	GangID        string `json:"gang_id"`
	Name          string `json:"name"`
	MinMembers    int    `json:"min_members"`
	GPUsPerMember int    `json:"gpus_per_member"`
	GPUType       string `json:"gpu_type"`
	Priority      int    `json:"priority"`

	// Placement constraints
	MaxNodesSpread  int  `json:"max_nodes_spread,omitempty"`
	PreferColocated bool `json:"prefer_colocated,omitempty"`
	RequireSameZone bool `json:"require_same_zone,omitempty"`
	RequireNVLink   bool `json:"require_nvlink,omitempty"`

	// Timing
	ScheduleTimeoutSecs int `json:"schedule_timeout_secs,omitempty"`
	BarrierTimeoutSecs  int `json:"barrier_timeout_secs,omitempty"`

	// Job template (applied to each member)
	Image   string   `json:"image,omitempty"`
	Command []string `json:"command,omitempty"`
	Args    []string `json:"args,omitempty"`

	// Multi-tenancy
	TenantID string `json:"tenant_id,omitempty"`
}

// GangSubmitResponse: POST /gang/submit response
type GangSubmitResponse struct {
	Success   bool   `json:"success"`
	GangID    string `json:"gang_id"`
	Phase     string `json:"phase"`
	Members   int    `json:"members"`
	TotalGPUs int    `json:"total_gpus"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
}

// GangStatusResponse: GET /gang/status response
type GangStatusResponse struct {
	GangID       string              `json:"gang_id"`
	Name         string              `json:"name"`
	Phase        string              `json:"phase"`
	Members      int                 `json:"members"`
	TotalGPUs    int                 `json:"total_gpus"`
	ReadyCount   int                 `json:"ready_count"`
	FailedCount  int                 `json:"failed_count"`
	PendingCount int                 `json:"pending_count"`
	MemberDetail []GangMemberDetail  `json:"member_detail"`
	Placement    *gang.GangPlacement `json:"placement,omitempty"`
	CreateTime   string              `json:"create_time"`
	ScheduleTime string              `json:"schedule_time,omitempty"`
	BarrierTime  string              `json:"barrier_time,omitempty"`
	EndTime      string              `json:"end_time,omitempty"`
	LastError    string              `json:"last_error,omitempty"`
	Timestamp    string              `json:"timestamp"`
}

// GangMemberDetail: Per-member status in response
type GangMemberDetail struct {
	MemberIndex int    `json:"member_index"`
	JobID       string `json:"job_id"`
	Status      string `json:"status"`
	ClusterID   string `json:"cluster_id,omitempty"`
	NodeID      string `json:"node_id,omitempty"`
	GPUIndices  []int  `json:"gpu_indices,omitempty"`
	Ready       bool   `json:"ready"`
	Error       string `json:"error,omitempty"`
}

// ============================================================================
// HANDLERS
// ============================================================================

// handleSubmitGang: POST /gang/submit
func (ag *APIGateway) handleSubmitGang(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		ag.respondError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			fmt.Sprintf("expected POST, got %s", r.Method))
		return
	}

	// Parse request
	var req GangSubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		ag.respondError(w, http.StatusBadRequest, "INVALID_JSON",
			fmt.Sprintf("invalid JSON: %v", err))
		atomic.AddUint64(&ag.totalErrors, 1)
		return
	}

	// Validate
	if req.GangID == "" {
		ag.respondError(w, http.StatusBadRequest, "MISSING_FIELD", "gang_id is required")
		return
	}
	if req.MinMembers < 1 {
		ag.respondError(w, http.StatusBadRequest, "INVALID_FIELD", "min_members must be >= 1")
		return
	}
	if req.GPUsPerMember < 1 {
		ag.respondError(w, http.StatusBadRequest, "INVALID_FIELD", "gpus_per_member must be >= 1")
		return
	}

	// Get gang manager from global scheduler
	gangMgr := ag.globalScheduler.GetGangManager()
	if gangMgr == nil {
		ag.respondError(w, http.StatusInternalServerError, "GANG_NOT_AVAILABLE",
			"gang scheduling not initialized")
		return
	}

	// Set defaults
	scheduleTimeout := 5 * time.Minute
	if req.ScheduleTimeoutSecs > 0 {
		scheduleTimeout = time.Duration(req.ScheduleTimeoutSecs) * time.Second
	}
	barrierTimeout := 10 * time.Minute
	if req.BarrierTimeoutSecs > 0 {
		barrierTimeout = time.Duration(req.BarrierTimeoutSecs) * time.Second
	}
	image := req.Image
	if image == "" {
		image = "ares-job:latest"
	}
	gpuType := req.GPUType
	if gpuType == "" {
		gpuType = "any"
	}

	// Build gang spec
	spec := &gang.GangSpec{
		GangID:          req.GangID,
		Name:            req.Name,
		MinMembers:      req.MinMembers,
		GPUsPerMember:   req.GPUsPerMember,
		TotalGPUs:       req.MinMembers * req.GPUsPerMember,
		MaxNodesSpread:  req.MaxNodesSpread,
		PreferColocated: req.PreferColocated,
		RequireSameZone: req.RequireSameZone,
		RequireNVLink:   req.RequireNVLink,
		ScheduleTimeout: scheduleTimeout,
		BarrierTimeout:  barrierTimeout,
		Priority:        req.Priority,
		JobTemplate: &common.JobSpec{
			Name:         req.Name,
			Image:        image,
			Command:      req.Command,
			Args:         req.Args,
			GPUCount:     req.GPUsPerMember,
			GPUType:      gpuType,
			Priority:     req.Priority,
			PreferNVLink: req.RequireNVLink,
			TenantID:     req.TenantID,
		},
	}

	// Submit gang
	ctx := r.Context()
	gangState, err := gangMgr.SubmitGang(ctx, spec)
	if err != nil {
		ag.respondError(w, http.StatusConflict, "GANG_SUBMIT_FAILED", err.Error())
		atomic.AddUint64(&ag.totalErrors, 1)
		return
	}

	// Response
	resp := &GangSubmitResponse{
		Success:   true,
		GangID:    gangState.Spec.GangID,
		Phase:     string(gangState.Phase),
		Members:   gangState.Spec.MinMembers,
		TotalGPUs: gangState.Spec.TotalGPUs,
		Message: fmt.Sprintf("Gang %s submitted: %d members × %d GPUs = %d total GPUs",
			req.GangID, req.MinMembers, req.GPUsPerMember, req.MinMembers*req.GPUsPerMember),
		Timestamp: time.Now().Format(time.RFC3339),
	}

	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(resp)

	ag.log.Info("GANG API: Submitted %s (%d members × %d GPUs)", req.GangID, req.MinMembers, req.GPUsPerMember)
}

// handleGangStatus: GET /gang/status?gang_id=X
func (ag *APIGateway) handleGangStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		ag.respondError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			fmt.Sprintf("expected GET, got %s", r.Method))
		return
	}

	gangID := r.URL.Query().Get("gang_id")
	if gangID == "" {
		ag.respondError(w, http.StatusBadRequest, "MISSING_PARAM", "gang_id query parameter required")
		return
	}

	gangMgr := ag.globalScheduler.GetGangManager()
	if gangMgr == nil {
		ag.respondError(w, http.StatusInternalServerError, "GANG_NOT_AVAILABLE",
			"gang scheduling not initialized")
		return
	}

	gangState := gangMgr.GetGang(gangID)
	if gangState == nil {
		ag.respondError(w, http.StatusNotFound, "GANG_NOT_FOUND",
			fmt.Sprintf("gang %s not found", gangID))
		return
	}

	// Build member detail
	members := make([]GangMemberDetail, len(gangState.Members))
	for i, m := range gangState.Members {
		members[i] = GangMemberDetail{
			MemberIndex: m.MemberIndex,
			JobID:       m.JobID,
			Status:      string(m.Status),
			ClusterID:   m.ClusterID,
			NodeID:      m.NodeID,
			GPUIndices:  m.GPUIndices,
			Ready:       m.Ready,
			Error:       m.Error,
		}
	}

	resp := &GangStatusResponse{
		GangID:       gangState.Spec.GangID,
		Name:         gangState.Spec.Name,
		Phase:        string(gangState.Phase),
		Members:      gangState.Spec.MinMembers,
		TotalGPUs:    gangState.Spec.TotalGPUs,
		ReadyCount:   gangState.ReadyCount,
		FailedCount:  gangState.FailedCount,
		PendingCount: gangState.PendingCount,
		MemberDetail: members,
		Placement:    gangState.Placement,
		CreateTime:   formatTime(gangState.CreateTime),
		LastError:    gangState.LastError,
		Timestamp:    time.Now().Format(time.RFC3339),
	}

	if !gangState.ScheduleTime.IsZero() {
		resp.ScheduleTime = formatTime(gangState.ScheduleTime)
	}
	if !gangState.BarrierTime.IsZero() {
		resp.BarrierTime = formatTime(gangState.BarrierTime)
	}
	if !gangState.EndTime.IsZero() {
		resp.EndTime = formatTime(gangState.EndTime)
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

// handleCancelGang: POST /gang/cancel?gang_id=X
func (ag *APIGateway) handleCancelGang(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		ag.respondError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			fmt.Sprintf("expected POST, got %s", r.Method))
		return
	}

	gangID := r.URL.Query().Get("gang_id")
	if gangID == "" {
		ag.respondError(w, http.StatusBadRequest, "MISSING_PARAM", "gang_id query parameter required")
		return
	}

	gangMgr := ag.globalScheduler.GetGangManager()
	if gangMgr == nil {
		ag.respondError(w, http.StatusInternalServerError, "GANG_NOT_AVAILABLE",
			"gang scheduling not initialized")
		return
	}

	err := gangMgr.CancelGang(gangID)
	if err != nil {
		ag.respondError(w, http.StatusNotFound, "GANG_CANCEL_FAILED", err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":   true,
		"gang_id":   gangID,
		"message":   fmt.Sprintf("Gang %s cancelled", gangID),
		"timestamp": time.Now().Format(time.RFC3339),
	})
}
