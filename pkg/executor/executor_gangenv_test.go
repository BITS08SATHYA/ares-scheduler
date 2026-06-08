package executor

import (
	"testing"

	"github.com/BITS08SATHYA/ares-scheduler/pkg/logger"
)

// newTestExecutor builds a minimal Executor sufficient for createPodSpec, which
// only reads ex.ClusterID, ex.Log and ex.Config — no Kubernetes client needed.
func newTestExecutor() *Executor {
	return &Executor{
		ClusterID: "cluster-test",
		Log:       logger.Get(),
		Config: &ExecutorConfig{
			Namespace:        "ares-system",
			DefaultJobImage:  "ares-job:latest",
			ImagePullPolicy:  "IfNotPresent",
			RestartPolicy:    "Never",
			DefaultMemoryMB:  1024,
			DefaultCPUMillis: 500,
		},
	}
}

// TestCreatePodSpec_InjectsGangEnv verifies a gang member's rank/world-size are
// surfaced into the pod env so distributed-training entrypoints can self-config.
func TestCreatePodSpec_InjectsGangEnv(t *testing.T) {
	ex := newTestExecutor()
	decision := &K8Decision{
		JobID:         "llama-member-2",
		NodeID:        "node-a",
		GPUIndices:    []int{0, 1},
		Image:         "nvidia/cuda:12.0",
		GangID:        "llama-70b",
		GangSize:      4,
		GangMemberIdx: 2,
	}

	spec := createPodSpec(ex, decision, "pod-llama-2")

	want := map[string]string{
		"ARES_GANG_ID":          "llama-70b",
		"ARES_GANG_RANK":        "2",
		"ARES_GANG_WORLD_SIZE":  "4",
		"ARES_GANG_MASTER_PORT": "29500",
	}
	for k, v := range want {
		if got := spec.EnvVars[k]; got != v {
			t.Errorf("env %s = %q, want %q", k, got, v)
		}
	}
}

// TestCreatePodSpec_NonGangHasNoGangEnv verifies non-gang jobs get none of the
// gang env vars — the injection must be gated on a non-empty GangID.
func TestCreatePodSpec_NonGangHasNoGangEnv(t *testing.T) {
	ex := newTestExecutor()
	decision := &K8Decision{
		JobID:      "solo-job",
		NodeID:     "node-a",
		GPUIndices: []int{0},
		Image:      "nvidia/cuda:12.0",
		// no GangID
	}

	spec := createPodSpec(ex, decision, "pod-solo")

	for _, k := range []string{"ARES_GANG_ID", "ARES_GANG_RANK", "ARES_GANG_WORLD_SIZE", "ARES_GANG_MASTER_PORT"} {
		if _, ok := spec.EnvVars[k]; ok {
			t.Errorf("non-gang pod should not have env %s, but it was set to %q", k, spec.EnvVars[k])
		}
	}
}
