#!/bin/bash
# pyramid/GC/Distributed/master.sh — Run Distributed CPU-Only GC experiments

set -e

export LD_LIBRARY_PATH="/opt/ohpc/pub/libs/gnu12/metis/5.1.0/lib:${LD_LIBRARY_PATH:-}"

DATASETS=(
    "kmer_A2a"
    "com-Friendster"
    "arabic-2005"
)

BATCH_RATIOS=("3")
NUM_BATCHES=10
NUM_THREADS=16
NUM_PARTITIONS=2
SLAVE_PORT=6001
PREPROCESSED="/scratch/m24cse014/dynamic-mis-pipeline/preprocessed"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BINARY="${SCRIPT_DIR}/master"
LOG_DIR="${SCRIPT_DIR}/logs"

mkdir -p "$LOG_DIR"
rm -f "${SCRIPT_DIR}/results.csv"

echo "============================================="
echo "  GC Distributed CPU-Only Experiment Runner"
echo "  $(date)"
echo "  Threads: ${NUM_THREADS}, Partitions: ${NUM_PARTITIONS}"
echo "============================================="

EXPERIMENT=0
TOTAL=$((${#DATASETS[@]} * ${#BATCH_RATIOS[@]}))

for dataset in "${DATASETS[@]}"; do
    for ratio in "${BATCH_RATIOS[@]}"; do
        EXPERIMENT=$((EXPERIMENT + 1))

        MTX_FILE="${PREPROCESSED}/${dataset}/converted/${dataset}.mtx"
        COLOR_FILE="${PREPROCESSED}/${dataset}/Coloring/${dataset}.txt"
        BATCH_DIR="${PREPROCESSED}/${dataset}/Batches/${ratio}"
        LOG_FILE="${LOG_DIR}/${dataset}_ratio${ratio}.log"

        echo ""
        echo "[$EXPERIMENT/$TOTAL] Dataset=${dataset}, Ratio=${ratio}"

        if [[ ! -f "$MTX_FILE" ]]; then echo "  SKIP: $MTX_FILE not found"; continue; fi
        if [[ ! -f "$COLOR_FILE" ]]; then echo "  SKIP: $COLOR_FILE not found"; continue; fi
        if [[ ! -d "$BATCH_DIR" ]]; then echo "  SKIP: $BATCH_DIR not found"; continue; fi

        cd "$SCRIPT_DIR"
        "$BINARY" "$MTX_FILE" "$COLOR_FILE" "$BATCH_DIR" "$NUM_BATCHES" "$NUM_THREADS" \
            "$NUM_PARTITIONS" "cn04:${SLAVE_PORT}" "cn05:${SLAVE_PORT}" \
            2>&1 | tee "$LOG_FILE"

    done
done

echo ""
echo "============================================="
echo "  All experiments completed."
echo "  CSV: ${SCRIPT_DIR}/results.csv"
echo "  Logs: ${LOG_DIR}/"
echo "============================================="
