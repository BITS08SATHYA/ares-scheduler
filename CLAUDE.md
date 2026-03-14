# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is Ares

Ares is a distributed multi-cluster GPU scheduler for Kubernetes, written in Go. It routes ML training jobs across clusters with exactly-once execution guarantees, GPU topology-aware placement (NVLink/NUMA optimization), gang scheduling, and DRF fair allocation.

## Build & Run

```bash
# Dependencies
go mod download

# Build
go build -o ares-global ./cmd/global
go build -o ares-local ./cmd/local
go build -o ares-benchmark ./cmd/benchmark

# Infrastructure (etcd + Redis required)
docker-compose up -d

# Run global scheduler
go run cmd/global/main.go \
  -gateway.port 8080 -etcd.endpoint localhost:2379 \
  -redis.addr localhost:6379 -enable-coordinator true -enable-metrics true

# Run local scheduler (one per cluster)
ARES_CLUSTER_ID=cluster-a go run cmd/local/main.go \
  -cluster-id cluster-a -port 9090 \
  -redis localhost:6379 -control-plane http://localhost:8080
```

## Testing

```bash
# Unit tests
go test ./tests/unit/...

# Single test file
go test ./tests/unit/gpu_test/

# Single test
go test ./tests/unit/gpu_test/ -run TestScorePlacement

# Benchmark suites (requires running control plane)
go run cmd/benchmark/main.go -control-plane http://localhost:8080 -suite all
# Individual suites: stress, exactlyonce, failure, gang, drf, priority, multicluster
```

Tests use `github.com/stretchr/testify` (assert/require). Unit tests that test storage require Docker-based etcd/Redis to be running.

## Architecture

**8-layer stack, two scheduler tiers:**

1. **API Gateway** (`pkg/api/gateway/`) — REST endpoints: `/schedule`, `/gang/submit`, `/health`, `/metrics`, `/status/{id}`, `/info/{cluster}`
2. **Global Scheduler** (`pkg/scheduler/global/`) — Scores clusters by GPU availability, model match, load, and health. Routes jobs to best cluster.
3. **Local Scheduler** (`pkg/scheduler/local/`) — Per-cluster node selection and GPU allocation with NVLink topology scoring
4. **DRF / Gang / Preemption** (`pkg/scheduler/drf/`, `gang/`, `preemption/`) — Fairness, all-or-nothing gang allocation, priority eviction
5. **Executor** (`pkg/executor/`) — Creates K8s pods with GPU resource requests, monitors pod lifecycle
6. **Job Model** (`pkg/job/`) — State machine: PENDING → SCHEDULED → RUNNING → SUCCEEDED/FAILED. etcd-backed store.
7. **Reliability** — Three-layer exactly-once: Redis request dedup (`pkg/idempotency/`), etcd distributed leases (`pkg/lease/`), fencing tokens
8. **Storage** — etcd (`pkg/storage/etcd/`) for consensus/leases, Redis (`pkg/storage/redis/`) for caching/idempotency

**Other key packages:**
- `pkg/scheduler/common/types.go` — All shared types (JobSpec, Job, ClusterInfo, GPUDevice, LeaseInfo)
- `pkg/gpu/` — GPU discovery (nvidia-smi parsing, K8s API) and NVLink topology scoring
- `pkg/cluster/` — Cluster health, heartbeat, auto-registration with global plane
- `pkg/crdt/` — Vector clocks, LWW-registers, OR-Sets for split-brain resilience
- `pkg/orchestrator/` — Multi-cluster coordination

**Entry points:** `cmd/global/main.go` (global scheduler), `cmd/local/main.go` (local scheduler), `cmd/benchmark/main.go` (benchmark suite)

**Job flow:** Client → API Gateway → idempotency check (Redis) → global scheduler (cluster selection) → DRF/gang checks → local scheduler (node + GPU selection with topology scoring) → lease acquisition (etcd) → executor (K8s pod creation) → heartbeat loop → completion with fencing token validation

## Module Path

`github.com/BITS08SATHYA/ares-scheduler` — use this for all import paths.

## Current State

- 5 of 7 benchmark suites passing (priority preemption trigger and failure recovery are in progress)
- K8s manifests in `k8s/global/` and `k8s/local/` for GKE + EKS deployment
- `run.sh` builds Docker images and deploys to GKE/EKS
