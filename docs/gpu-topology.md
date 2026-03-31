# GPU Topology Scoring: Design, Validation, and Learnings

## Overview

Ares includes a GPU topology scorer (`pkg/gpu/topology.go`, 1,147 LOC) that parses real hardware interconnect data and scores GPU placement candidates. The scorer detects NVLink connections, NVSwitch domains, NUMA locality, and PCIe generation — then produces a 0-100 affinity score for every GPU pair.

**Key finding:** After benchmarking on p4d.24xlarge (8x A100-SXM4-40GB), we discovered that NVSwitch makes intra-node topology scoring irrelevant on modern hardware. This document explains what we built, what we learned, and where the scorer still matters.

---

## GPU Interconnect Topologies

### NVSwitch: Full Crossbar (Modern Hardware)

On NVSwitch-based servers (p4d.24xlarge, p5.48xlarge), all GPUs connect through a set of NVSwitch ASICs that form a **full crossbar** — every GPU can communicate with every other GPU at maximum bandwidth.

![NVSwitch Full Crossbar](diagrams/gpu-nvswitch-crossbar.png)

```
p4d.24xlarge topology (nvidia-smi topo --matrix):

      GPU0  GPU1  GPU2  GPU3  GPU4  GPU5  GPU6  GPU7
GPU0   X    NV12  NV12  NV12  NV12  NV12  NV12  NV12
GPU1  NV12   X    NV12  NV12  NV12  NV12  NV12  NV12
GPU2  NV12  NV12   X    NV12  NV12  NV12  NV12  NV12
GPU3  NV12  NV12  NV12   X    NV12  NV12  NV12  NV12
GPU4  NV12  NV12  NV12  NV12   X    NV12  NV12  NV12
GPU5  NV12  NV12  NV12  NV12  NV12   X    NV12  NV12
GPU6  NV12  NV12  NV12  NV12  NV12  NV12   X    NV12
GPU7  NV12  NV12  NV12  NV12  NV12  NV12  NV12   X

NV12 = 12 NVLink bridges per pair
28 GPU pairs, all with identical bandwidth
```

**Result: Every GPU placement scores equally.** There is no "wrong" pair to select — the crossbar makes all GPUs equidistant. Same-NUMA vs cross-NUMA placement shows < 1% bandwidth difference (validated below).

Hardware with NVSwitch: A100 SXM, H100 SXM, B200, p4d.24xlarge, p5.48xlarge, DGX A100/H100.

### PCIe: Tree Topology (Legacy Hardware)

On PCIe-based servers (p3.8xlarge, V100 PCIe), GPUs connect through a hierarchy of PCIe switches. Only select GPU pairs have direct NVLink bridges — the rest communicate via PCIe, which is 5-37x slower.

![PCIe-Based Topology](diagrams/gpu-pcie-topology.png)

```
p3.8xlarge topology (nvidia-smi topo --matrix):

      GPU0  GPU1  GPU2  GPU3
GPU0   X    NV1   PHB   SYS
GPU1  NV1    X    SYS   PHB
GPU2  PHB   SYS    X    NV1
GPU3  SYS   PHB   NV1    X

NV1 = NVLink bridge (~200 GB/s)
PHB = PCIe Host Bridge (~32 GB/s)
SYS = Cross-NUMA via system bus (~16 GB/s)
```

**Result: GPU placement matters significantly.** Selecting GPU 0 + GPU 1 (NVLink) vs GPU 0 + GPU 2 (PCIe) produces a 6.25x bandwidth difference.

Hardware without NVSwitch: V100 PCIe, T4, A10G, p3.8xlarge, g4dn instances.

---

## Scoring Algorithm

The scorer (`ScoreGPUPlacement` in `pkg/gpu/topology.go`) evaluates each GPU pair on a 0-100 scale:

| Factor | Score | Condition |
|--------|-------|-----------|
| NV12+ (same NVSwitch) | +60 | 12+ NVLink bridges between pair |
| NV6 (cross NVSwitch) | +40 | 6-11 NVLink bridges |
| NV2-NV4 (limited) | +25 | 2-5 NVLink bridges |
| NV1 (minimal) | +15 | 1 NVLink bridge |
| Same NUMA domain | +15 | Both GPUs on same NUMA node |
| PCIe Gen5 | +10 | PCIe generation 5 detected |
| PCIe Gen4 | +5 | PCIe generation 4 detected |
| Base (PCIe only) | 20 | No NVLink, no special factors |

### NVSwitch Domain Detection

The scorer uses a **union-find algorithm** to group GPUs into NVSwitch domains:

1. Parse all NVLink pairs and their link counts from `nvidia-smi topo --matrix`
2. Find the maximum link count across all pairs (e.g., NV12 = 12)
3. Set threshold at 80% of max (e.g., 12 * 0.8 = 9.6)
4. Union GPU pairs with link count >= threshold into the same domain
5. Result: domain map (e.g., domain 0 = GPU 0-3, domain 1 = GPU 4-7)

On p4d.24xlarge, all 28 pairs have NV12 — so all 8 GPUs land in a single domain (confirming NVSwitch full crossbar).

### NUMA Detection

Reads GPU-to-NUMA mapping via:
1. `nvidia-smi --query-gpu=pci.bus_id` to get each GPU's PCIe bus address
2. `/sys/class/pci_bus/{bus}/device/numa_node` to map PCIe bus to NUMA node

On p4d.24xlarge: GPU 0-3 on NUMA 0, GPU 4-7 on NUMA 1.

### PCIe Generation Detection

Queries `nvidia-smi --query-gpu=index,pci.link_gen` to determine PCIe generation per GPU. Gen5 gets a +10 bonus, Gen4 gets +5.

---

## NCCL Hardware Validation

We ran NCCL `all_reduce_perf` benchmarks on p4d.24xlarge (8x A100-SXM4-40GB) to validate whether the topology scorer's placement decisions produce measurable bandwidth differences.

![NCCL Benchmark Results](diagrams/gpu-nccl-benchmark.png)

### Results

| Test | GPUs | Avg Bus BW | Peak BW (256MB) | Delta vs Baseline |
|------|------|-----------|-----------------|-------------------|
| Same NUMA (GPU 0,1) | 2 | 39.9 GB/s | 179 GB/s | baseline |
| Cross NUMA (GPU 0,4) | 2 | 40.2 GB/s | 180 GB/s | +0.8% |
| Same NUMA (0,1,2,3) | 4 | 46.2 GB/s | 206 GB/s | baseline |
| Cross NUMA (0,1,4,5) | 4 | 46.7 GB/s | 206 GB/s | +1.1% |
| 8-GPU sequential | 8 | 45.1 GB/s | 215 GB/s | baseline |
| 8-GPU interleaved | 8 | 44.9 GB/s | 214 GB/s | -0.4% |

### Analysis

**2-GPU tests:** Same NUMA (39.9 GB/s) vs Cross NUMA (40.2 GB/s) = 0.8% delta. Statistically insignificant. The NVSwitch crossbar makes GPU 0-GPU 1 and GPU 0-GPU 4 equivalent.

**4-GPU tests:** Same pattern. Same NUMA (46.2 GB/s) vs Cross NUMA (46.7 GB/s) = 1.1% delta. The cross-NUMA configuration actually measured slightly *faster* (within noise).

**8-GPU tests:** Sequential order (45.1 GB/s) vs interleaved order (44.9 GB/s) = 0.4% delta. All 8 GPUs through a single all-reduce — order doesn't matter.

### Conclusion

**On NVSwitch hardware, the topology scorer adds no value for intra-node placement.** The full crossbar makes every GPU equidistant. The scorer correctly assigns NV12+60 to all pairs, producing uniform scores — which is the right answer, but it means the scoring computation is wasted.

---

## Where Topology Scoring Matters

### 1. PCIe-Based Hardware

On systems without NVSwitch (p3.8xlarge, V100 PCIe, T4, A10G), the scorer produces meaningful differentiation:

- NVLink pair: score 80+ (200 GB/s)
- Same-NUMA PCIe pair: score 40 (32 GB/s)
- Cross-NUMA PCIe pair: score 20 (16 GB/s)

Selecting the NVLink pair over the PCIe pair avoids a 6-12x bandwidth penalty. On these systems, topology scoring is critical.

### 2. Mixed Fleets

Organizations running both NVSwitch (A100 SXM for training) and PCIe (T4/A10G for inference) benefit from the scorer — it adapts per-node based on detected topology, applying scoring only where it matters.

### 3. Inter-Node Boundary

The biggest bandwidth gap isn't within a node — it's between nodes:

| Path | Bandwidth |
|------|-----------|
| NVLink (intra-node) | ~200 GB/s |
| EFA (inter-node, p4d) | ~25 GB/s |
| Standard Ethernet | ~3 GB/s |

Keeping all workers on the same node avoids the 8-60x penalty at the network boundary. This is ultimately a **bin-packing** decision (fit all GPUs on one node), not a topology decision (which GPUs on that node).

---

## Design Decision

After validating against real hardware, we made a deliberate choice: **keep the topology scorer but shift focus to distributed coordination.**

The scorer is correct, well-tested, and adds value on PCIe hardware and mixed fleets. But on modern NVSwitch hardware — which is what most teams deploying A100/H100 clusters use — the real scheduling challenges are:

1. **Exactly-once execution** — preventing duplicate job runs across worker failures
2. **Multi-cluster routing** — placing jobs on the right cluster across a federated fleet
3. **Gang scheduling** — atomic all-or-nothing allocation for distributed training
4. **DRF fairness** — preventing tenant starvation across resource dimensions
5. **Partition tolerance** — continuing to schedule when the control plane is unreachable

These problems are where Ares's complexity and value live. The topology scorer is one component of a larger system — and the honest finding that NVSwitch makes it less impactful on modern hardware is itself a valuable engineering insight.

---

## Code References

| Component | File | Lines |
|-----------|------|-------|
| NVLink detection | `pkg/gpu/topology.go` | `DetectNVLink()`, `parseNVLinkMatrix()` |
| NVSwitch domain detection | `pkg/gpu/topology.go` | `DetectNVSwitchDomains()` (union-find) |
| NUMA mapping | `pkg/gpu/topology.go` | `DetectNUMAMapping()`, `queryGPUNUMANode()` |
| PCIe generation | `pkg/gpu/topology.go` | `DetectPCIeGeneration()` |
| Affinity scoring | `pkg/gpu/topology.go` | `ScoreGPUPlacement()` |
| GPU discovery | `pkg/gpu/discovery.go` | `nvidia-smi` parsing, GPU enumeration |
| Local scheduler integration | `pkg/scheduler/local/scheduler.go` | `SelectBestGPUsInNode()` |
| Topology tests | `tests/unit/gpu_topology_test/` | NVLink parsing, scoring validation |
| Diagram generator | `scripts/generate_topology_diagrams.py` | Generates all diagrams in this doc |
