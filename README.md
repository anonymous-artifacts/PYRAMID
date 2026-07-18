# PYRAMID: Progressive DPU-Offloaded Combinatorial Optimization on Dynamic Graphs

This repository contains the **prototype implementation of PYRAMID**, a staged offloading pipeline that incrementally moves the phases of dynamic graph algorithms; from the CPU onto a network-attached DPU accelerator, and measures the performance impact at each stage.

The implementation is **native C/C++ over TCP**.



## 1. Overview

Both MIS and GC are implemented as **3-phase batch-update pipelines**:

* **Phase 1** — local candidate/conflict detection over the incoming update batch (`ProcessCE` for GC, `BFS` for MIS)
* **Phase 2** — conflict resolution (`CheckConflict` for GC, `Clustering` for MIS)
* **Phase 3** — topology repair and final state update (`UpdateNeighbors` for GC, `MIS Computation` for MIS)

PYRAMID studies what happens as each phase, in order, is **offloaded from the CPU to a DPU** (an ARM Cortex-A72 accelerator node, reached over TCP via a thin relay process). The offload configurations form a pyramid of increasing DPU responsibility:

| Configuration  | Phase 1 | Phase 2 | Phase 3 |
| -------------- | ------- | ------- | ------- |
| **PartialRDS**  | DPU     | CPU     | CPU     |
| **FullRDS**     | DPU     | DPU     | CPU     |
| **CompleteRDS** | DPU     | DPU     | DPU     |

Alongside the offload study, the repository includes a **pure CPU baseline**, a **CPU-thread-utilization profile**, a **multi-node distributed variant** (CPU-only and DPU-assisted, with and without boundary synchronization), and a **thread-scaling study**.

Each algorithm/variant operates on **batched dynamic updates** (edge insertions/deletions) applied on top of a static base graph, and reports per-phase timing breakdowns.



## 2. System Design (Code Perspective)

### Core Concepts

* **Host program**
  Reads the base graph (CSR/MTX), applies update batches, and — depending on the variant — executes phases locally or ships them to the DPU.

* **Relay (`relay.cpp`)**
  A thin bidirectional TCP forwarder that runs on a compute node (`cn04`/`cn05`) and bridges the login-node host process to the DPU device, which sits on a private network (`192.168.100.2`).

* **DPU kernel (`*_dpu.c`)**
  A standalone TCP server compiled and run directly on the DPU's ARM core. It receives the graph (and, per configuration, batch edges / sync payloads) and returns the phases it was assigned.

* **Distributed master/slave (`Distributed`, `DST_MST_SLV*`)**
  A multi-node extension where a login-node **master** partitions the graph (METIS + 2-hop ghost halo) across **slave** processes on `cn04`/`cn05`, each of which may additionally offload phases to its own DPU. The `NOSYNC` variant skips boundary color-conflict resolution and ghost refresh, trading correctness at partition boundaries for lower synchronization cost.

### DPU Wire Protocol (summary)

```text
Init:      [4B nv][4B ne][(nv+1)*4B offsets][ne*4B nbrs][nv*4B state]
Host→DPU:  [4B num_edges][edges...][sync payloads for phases done on CPU]
DPU→Host:  [phase results for whichever phases were offloaded]
```

Configurations that keep a phase on the CPU carry the corresponding sync payload back and forth each batch except **CompleteRDS**.



## 3. Repository Structure

```text
pyramid/
├── GC/                          # Graph Coloring
│   ├── CPU-Only/                # 3-phase CPU-only baseline
│   ├── CPUUtilization/          # CPU-only baseline, profiled for per-phase thread utilization
│   ├── PartialRDS/              # Phase 1 offloaded to DPU
│   ├── FullRDS/                 # Phase 1 + 2 offloaded to DPU
│   ├── CompleteRDS/             # Phase 1 + 2 + 3 offloaded to DPU
│   ├── Distributed/             # Multi-node CPU-only (master + slaves, METIS partitioning)
│   ├── DST_MST_SLV/             # Multi-node, slaves offload P1+P2 to their own DPU
│   ├── DST_MST_SLV_NOSYNC/      # Same as above, without boundary sync/ghost refresh
│   └── ThreadScaling/           # Thread-count sweep (1..32) on FullRDS
├── MIS/                         # Maximal Independent Set — same layout as GC/
│   ├── CPU-Only/
│   ├── CPUUtilization/
│   ├── PartialRDS/
│   ├── FullRDS/
│   ├── CompleteRDS/
│   ├── Distributed/
│   ├── DST_MST_SLV/
│   ├── DST_MST_SLV_NOSYNC/
│   └── ThreadScaling/
└── README.md
```

Each leaf folder holds its own source (`.cpp`/`.c`), a `master.sh` experiment driver, a `results.csv` it appends to, and — for DPU variants — a `start_infra.sh` to manage the relay/DPU lifecycle and a prebuilt `relay`/host/DPU binary.



## 4. Build Requirements

### Software

* GCC/G++ with C++17 and OpenMP support (`g++ -std=c++17 -fopenmp`)
* SLURM (`sbatch`/`srun`/`scancel`) for distributed and DPU-backed variants
* Passwordless SSH from the compute node to the DPU device, and `gcc` available on the DPU itself
* METIS (for the `Distributed` / `DST_MST_SLV*` partitioning step)

### Hardware

* Multi-core CPU node (experiments use up to 32 threads)
* A network-reachable DPU device (ARM Cortex-A72) for all `PartialRDS`/`FullRDS`/`CompleteRDS`/`DST_MST_SLV*` variants
* SLURM cluster with at least one compute node (`cn04`/`cn05` in this setup) able to reach the DPU's private IP



## 5. Building

Each variant builds independently; there is no repository-wide build step. From inside a variant's folder:

```bash
# Host program (CPU-only baseline, or the CPU-side of a DPU offload variant)
g++ -O3 -fopenmp -std=c++17 <Name>_host.cpp -o <Name>_host

# DPU kernel (compiled on the DPU itself, see start_infra.sh deploy)
gcc -O2 -o <Name>_dpu <Name>_dpu.c

# Relay (only needed for DPU variants)
g++ -std=c++17 -O2 -Wall -o relay relay.cpp
```

The exact compile command for each file is also recorded in a header comment at the top of its source.



## 6. Running

### 6.1 Input Format

Every variant expects a **preprocessed dataset directory** (not included in this repository) of the form:

```text
<dataset>/
├── converted/<dataset>.mtx        # base graph, Matrix Market format
├── Coloring/<dataset>.txt         # initial per-vertex color (GC only)
├── MIS/<dataset>.txt              # initial MIS membership (MIS only)
└── Batches/<ratio>/*.mtx          # pre-generated update batches, one file per batch
```

`<ratio>` selects a batch-size tier (`3`–`7` in the provided experiments; a higher ratio yields smaller per-batch update sets). Batch generation itself is handled upstream of this repository.

### 6.2 Starting DPU Infrastructure (DPU variants only)

From inside a `PartialRDS`/`FullRDS`/`CompleteRDS`/`DST_MST_SLV*` folder:

```bash
./start_infra.sh deploy   # one-time: cross-copy + compile the DPU kernel on the DPU device
./start_infra.sh start    # submit relay+DPU as a SLURM job, wait for "Listening on port ..."
./start_infra.sh stop     # tear down when done
```

### 6.3 Running an Experiment

Each variant's `master.sh` runs the full dataset × batch-ratio sweep and appends results to `results.csv`:

```bash
./master.sh
```

To run a single dataset/ratio directly against the host binary instead of the full sweep:

```bash
./PartialRDS_MIS_host <MTX> <MIS_File> <BatchesFolder> <NumBatches> <Threads> <DPU_Host> [DPU_Port]
./PartialRDS_GC_host  <MTX> <Colors>   <BatchesFolder> <NumBatches> <Threads> <DPU_Host> [DPU_Port]
```

`CPU-Only`/`CPUUtilization` binaries drop the trailing `<DPU_Host> [DPU_Port]` arguments since no DPU is involved. `Distributed`/`DST_MST_SLV*` variants use `master`/`slave` binaries instead — see each folder's `master.sh`/`start_slaves.sh` for the exact invocation.



## 7. Datasets

Experiments are evaluated on the following real-world graphs (loaded via their Matrix Market representation):

| Dataset            | Vertices    | Edges (mtx nnz) | Avg. Degree |
| ------------------- | ----------: | ---------------: | ----------: |
| roadNet-CA          | 1,971,281   | 9,461,452         | 4.80        |
| rgg_n_2_20_s0       | 1,048,576   | 23,570,114        | 22.48       |
| great-britain_osm   | 7,733,822   | 27,896,166        | 3.61        |
| germany_osm         | 11,548,845  | 42,304,240        | 3.66        |
| soc-LiveJournal1    | 4,847,571   | 235,954,502       | 48.68       |
| com-Orkut           | 3,072,441   | 373,853,788       | 121.68      |
| uk-2002             | 18,520,486  | 536,587,038       | 28.97       |
| kmer_A2a            | 170,728,175 | 616,587,916       | 3.61        |
| com-Friendster      | 65,608,366  | 855,498,836       | 13.04       |
| arabic-2005         | 22,744,080  | 1,151,994,356     | 50.66       |



## 8. Results and Logging

* Every run appends a row per processed batch to that folder's `results.csv`, e.g. for GC's `PartialRDS`:

  ```text
  Dataset,BatchRatio,BatchID,BatchFile,PrePhase_CPU,P1_DPU,P1_Transfer,P1_Total,
  CheckConflict_CPU_P2,UpdateNeighbors_CPU_P3,Verify_CPU_post,TotalBatch_ms,
  Iterations,ChromaticNum,SessionTime_ms,AvgLatency_ms
  ```

  Column sets vary slightly by variant (e.g. MIS reports `MIS_Cardinality`/`NumClusters` instead of `ChromaticNum`), matching whichever phases that variant offloads.

* `master.sh` and `start_infra.sh` write process logs under a `logs/` folder created alongside them (relay/DPU startup logs, per-dataset/ratio run logs). These are run artifacts and are not checked in.



## 9. Running the Full Suite

To reproduce the full sweep for one algorithm/variant end-to-end:

```bash
cd GC/FullRDS                 # or any other variant folder
./start_infra.sh deploy       # DPU variants only, one-time per DPU image
./start_infra.sh start        # DPU variants only
./master.sh                   # runs all datasets x all batch ratios, appends results.csv
./start_infra.sh stop         # DPU variants only
```

For the multi-node variants (`Distributed`, `DST_MST_SLV*`), use `run_all.sh` instead, which wraps slave startup, `master.sh`, and slave teardown into a single call.
