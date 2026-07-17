#!/bin/bash
# pyramid/MIS/PartialRDS/master.sh — Run PartialRDS_MIS across all datasets and batch ratios
# Phase 1 (BFS) runs on DPU, Phases 2+3 on CPU

set -e

DATASETS=(
    "roadNet-CA"
    "rgg_n_2_20_s0"
    "great-britain_osm"
    "germany_osm"
    "soc-LiveJournal1"
    "com-Orkut"
    "uk-2002"
    "kmer_A2a"
    "com-Friendster"
    "arabic-2005"
)

BATCH_RATIOS=("3" "4" "5" "6" "7")
NUM_BATCHES=10
NUM_THREADS=32
PREPROCESSED="/scratch/m24cse014/dynamic-mis-pipeline/preprocessed"

# DPU connection settings
DPU_HOST="${DPU_HOST:-cn05}"
DPU_PORT="${DPU_PORT:-5000}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BINARY="${SCRIPT_DIR}/PartialRDS_MIS_host"
LOG_DIR="${SCRIPT_DIR}/logs"

mkdir -p "$LOG_DIR"

# Remove old CSV for fresh run
rm -f "${SCRIPT_DIR}/results.csv"

echo "============================================="
echo "  MIS PartialRDS Experiment Runner"
echo "  $(date)"
echo "  Threads: ${NUM_THREADS}"
echo "  DPU: ${DPU_HOST}:${DPU_PORT}"
echo "  Batches per run: ${NUM_BATCHES}"
echo "============================================="

EXPERIMENT=0
TOTAL=$((${#DATASETS[@]} * ${#BATCH_RATIOS[@]}))

for dataset in "${DATASETS[@]}"; do
    for ratio in "${BATCH_RATIOS[@]}"; do
        EXPERIMENT=$((EXPERIMENT + 1))

        MTX_FILE="${PREPROCESSED}/${dataset}/converted/${dataset}.mtx"
        MIS_FILE="${PREPROCESSED}/${dataset}/MIS/${dataset}.txt"
        BATCH_DIR="${PREPROCESSED}/${dataset}/Batches/${ratio}"
        LOG_FILE="${LOG_DIR}/${dataset}_ratio${ratio}.log"

        echo ""
        echo "[$EXPERIMENT/$TOTAL] Dataset=${dataset}, Ratio=${ratio}"

        if [[ ! -f "$MTX_FILE" ]]; then echo "  SKIP: $MTX_FILE not found"; continue; fi
        if [[ ! -f "$MIS_FILE" ]]; then echo "  SKIP: $MIS_FILE not found"; continue; fi
        if [[ ! -d "$BATCH_DIR" ]]; then echo "  SKIP: $BATCH_DIR not found"; continue; fi

        cd "$SCRIPT_DIR"
        "$BINARY" "$MTX_FILE" "$MIS_FILE" "$BATCH_DIR" "$NUM_BATCHES" "$NUM_THREADS" \
            "$DPU_HOST" "$DPU_PORT" \
            2>&1 | tee "$LOG_FILE"

    done
done

echo ""
echo "============================================="
echo "  All experiments completed."
echo "  CSV: ${SCRIPT_DIR}/results.csv"
echo "  Logs: ${LOG_DIR}/"
echo "============================================="
