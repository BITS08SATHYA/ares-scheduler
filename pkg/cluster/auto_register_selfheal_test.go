package cluster

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// TestSendHeartbeatRequest_NotFoundReturnsSentinel verifies a 404 from the
// control plane surfaces as ErrClusterNotRegistered so the loop can self-heal.
func TestSendHeartbeatRequest_NotFoundReturnsSentinel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	err := sendHeartbeatRequest(context.Background(), srv.URL, &ClusterHeartbeatRequest{ClusterID: "c1"})
	if !errors.Is(err, ErrClusterNotRegistered) {
		t.Fatalf("expected ErrClusterNotRegistered, got %v", err)
	}
}

// TestStartHeartbeat_SelfHealsAfterControlPlaneForget simulates a control plane
// that has forgotten the cluster (heartbeats 404) until the cluster re-registers.
// The heartbeat loop must detect the 404, re-register, and then succeed.
func TestStartHeartbeat_SelfHealsAfterControlPlaneForget(t *testing.T) {
	var registered atomic.Bool
	var reRegisterCalls atomic.Int32
	var okHeartbeats atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/clusters/register":
			if reRegisterCalls.Add(1); true {
				registered.Store(true)
			}
			w.WriteHeader(http.StatusOK)
		case "/cluster-heartbeat":
			if !registered.Load() {
				// Control plane lost its registry — reject until re-registered.
				w.WriteHeader(http.StatusNotFound)
				return
			}
			okHeartbeats.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cfg := &HeartbeatConfig{
		ClusterID:       "c1",
		ControlPlaneURL: srv.URL,
		Interval:        30 * time.Millisecond,
		GetLoadFunc:     func() map[string]interface{} { return map[string]interface{}{} },
		RegistrationConfig: &AutoRegistrationConfig{
			ClusterID:       "c1",
			ControlPlaneURL: srv.URL,
			TotalGPUs:       4,
		},
	}

	go StartHeartbeat(ctx, cfg)

	// Wait until at least one heartbeat succeeds after self-healing.
	deadline := time.After(1500 * time.Millisecond)
	for okHeartbeats.Load() == 0 {
		select {
		case <-deadline:
			t.Fatalf("heartbeat never recovered: reRegisterCalls=%d okHeartbeats=%d",
				reRegisterCalls.Load(), okHeartbeats.Load())
		case <-time.After(10 * time.Millisecond):
		}
	}

	if reRegisterCalls.Load() == 0 {
		t.Fatalf("expected at least one re-registration, got 0")
	}
}

// TestStartHeartbeat_NoRegistrationConfigDoesNotPanic ensures the loop tolerates
// a nil RegistrationConfig (logs but cannot self-heal) without crashing.
func TestStartHeartbeat_NoRegistrationConfigDoesNotPanic(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	cfg := &HeartbeatConfig{
		ClusterID:       "c1",
		ControlPlaneURL: srv.URL,
		Interval:        30 * time.Millisecond,
		GetLoadFunc:     func() map[string]interface{} { return map[string]interface{}{} },
		// RegistrationConfig intentionally nil.
	}

	StartHeartbeat(ctx, cfg) // returns when ctx times out; must not panic
}
