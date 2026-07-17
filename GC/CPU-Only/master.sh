#!/bin/bash
# pyramid/GC/master.sh — Run CPU_Only_GC across all datasets and batch ratios
# All results go into a single results.csv (appended by the C++ program)

set -e

# DATASETS=(
#     "roadNet-CA"
#     "rgg_n_2_20_s0"
#     "great-britain_osm"
#     "germany_osm"
#     "soc-LiveJournal1"
#     "com-Orkut"
#     "uk-2002"
#     "kmer_A2a"
#     "com-Friendster"
#     "arabic-2005"
# )

# BATCH_RATIOS=("3" "4" "5" "6" "7")

DATASETS=(

    "com-Friendster"
)

BATCH_RATIOS=("3")



NUM_BATCHES=10
NUM_THREADS=32
PREPROCESSED="/scratch/m24cse014/dynamic-mis-pipeline/preprocessed"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BINARY="${SCRIPT_DIR}/CPU_Only_GC"
LOG_DIR="${SCRIPT_DIR}/logs"

mkdir -p "$LOG_DIR"

# Remove old CSV for fresh run
rm -f "${SCRIPT_DIR}/results.csv"

echo "============================================="
echo "  GC Experiment Runner"
echo "  $(date)"
echo "  Threads: ${NUM_THREADS}"
echo "  Batches per run: ${NUM_BATCHES}"
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
            2>&1 | tee "$LOG_FILE"

    done
done

echo ""
echo "============================================="
echo "  All experiments completed."
echo "  CSV: ${SCRIPT_DIR}/results.csv"
echo "  Logs: ${LOG_DIR}/"
echo "============================================="
