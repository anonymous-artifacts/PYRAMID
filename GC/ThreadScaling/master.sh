#!/bin/bash
# pyramid/GC/ThreadScaling/master.sh — Thread scaling study for FullRDS_GC
# Runs with thread counts: 1, 2, 4, 8, 16, 32
# Dataset: com-Friendster only, Ratio: 3 only

set -e

DATASETS=(
    "com-Friendster"
)

BATCH_RATIOS=("3")
THREAD_COUNTS=("1" "2" "4" "8" "16" "32")
NUM_BATCHES=10
PREPROCESSED="/scratch/m24cse014/dynamic-mis-pipeline/preprocessed"

# DPU connection settings
DPU_HOST="${DPU_HOST:-cn05}"
DPU_PORT="${DPU_PORT:-5003}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BINARY="${SCRIPT_DIR}/../FullRDS/FullRDS_GC_host"
LOG_DIR="${SCRIPT_DIR}/logs"
RESULTS_CSV="${SCRIPT_DIR}/results.csv"

mkdir -p "$LOG_DIR"

# Fresh results CSV with NumThreads column
rm -f "$RESULTS_CSV"
echo "Dataset,BatchRatio,NumThreads,BatchID,BatchFile,PrePhase_CPU,P12_DPU_P1,P12_DPU_P2,P12_Transfer,P12_Total,UpdateNeighbors_CPU_P3,Verify_CPU_post,TotalBatch_ms,Iterations,ChromaticNum,SessionTime_ms,AvgLatency_ms" \
    > "$RESULTS_CSV"

echo "============================================="
echo "  GC Thread Scaling Study (FullRDS)"
echo "  $(date)"
echo "  Thread counts: ${THREAD_COUNTS[*]}"
echo "  DPU: ${DPU_HOST}:${DPU_PORT}"
echo "  Batches per run: ${NUM_BATCHES}"
echo "============================================="

EXPERIMENT=0
TOTAL=$((${#DATASETS[@]} * ${#BATCH_RATIOS[@]} * ${#THREAD_COUNTS[@]}))

for dataset in "${DATASETS[@]}"; do
    for ratio in "${BATCH_RATIOS[@]}"; do
        for threads in "${THREAD_COUNTS[@]}"; do
            EXPERIMENT=$((EXPERIMENT + 1))

            MTX_FILE="${PREPROCESSED}/${dataset}/converted/${dataset}.mtx"
            COLOR_FILE="${PREPROCESSED}/${dataset}/Coloring/${dataset}.txt"
            BATCH_DIR="${PREPROCESSED}/${dataset}/Batches/${ratio}"
            LOG_FILE="${LOG_DIR}/${dataset}_ratio${ratio}_t${threads}.log"

            echo ""
            echo "[$EXPERIMENT/$TOTAL] Dataset=${dataset}, Ratio=${ratio}, Threads=${threads}"

            if [[ ! -f "$MTX_FILE" ]]; then echo "  SKIP: $MTX_FILE not found"; continue; fi
            if [[ ! -f "$COLOR_FILE" ]]; then echo "  SKIP: $COLOR_FILE not found"; continue; fi
            if [[ ! -d "$BATCH_DIR" ]]; then echo "  SKIP: $BATCH_DIR not found"; continue; fi

            cd "$SCRIPT_DIR"

            # Temporarily move master CSV so binary creates a fresh one
            mv "$RESULTS_CSV" "${RESULTS_CSV}.bak"

            "$BINARY" "$MTX_FILE" "$COLOR_FILE" "$BATCH_DIR" "$NUM_BATCHES" "$threads" \
                "$DPU_HOST" "$DPU_PORT" \
                2>&1 | tee "$LOG_FILE"

            # Merge binary's results.csv into master CSV with NumThreads column
            if [[ -f "${SCRIPT_DIR}/results.csv" ]]; then
                tail -n +2 "${SCRIPT_DIR}/results.csv" | \
                    awk -F',' -v t="$threads" 'NF>0 {print $1","$2","t","$3","$4","$5","$6","$7","$8","$9","$10","$11","$12","$13","$14","$15","$16}' \
                    >> "${RESULTS_CSV}.bak"
            fi

            mv "${RESULTS_CSV}.bak" "$RESULTS_CSV"

        done
    done
done

echo ""
echo "============================================="
echo "  GC Thread scaling study completed."
echo "  CSV: ${RESULTS_CSV}"
echo "  Logs: ${LOG_DIR}/"
echo "============================================="
