#!/bin/bash
# pyramid/GC/DST_MST_SLV/master.sh — Run DST_MST_SLV GC experiments

set +e  # Don't exit on failure — allow recovery between datasets

export LD_LIBRARY_PATH="/opt/ohpc/pub/libs/gnu12/metis/5.1.0/lib:${LD_LIBRARY_PATH:-}"

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

BATCH_RATIOS=("3")
NUM_BATCHES=10
NUM_THREADS=16
NUM_PARTITIONS=2
SLAVE_PORT=6003
PREPROCESSED="/scratch/m24cse014/dynamic-mis-pipeline/preprocessed"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BINARY="${SCRIPT_DIR}/master"
LOG_DIR="${SCRIPT_DIR}/logs"

mkdir -p "$LOG_DIR"
rm -f "${SCRIPT_DIR}/results.csv"

echo "============================================="
echo "  GC DST_MST_SLV Experiment Runner"
echo "  $(date)"
echo "  Threads: ${NUM_THREADS}, Partitions: ${NUM_PARTITIONS}"
echo "  DPU offload: ProcessCE + CheckConflict"
echo "============================================="

restart_slaves() {
    echo ""
    echo "--- Restarting slaves + DPU ---"
    "$SCRIPT_DIR/start_infra.sh" stop
    sleep 3
    "$SCRIPT_DIR/start_infra.sh"

    # Wait for both slaves to be ready
    for node in cn04 cn05; do
        logfile="${LOG_DIR}/slave_${node}.log"
        for i in $(seq 1 120); do
            if grep -q "READY" "$logfile" 2>/dev/null; then
                echo "  ${node}: READY"
                break
            fi
            sleep 1
        done
        if ! grep -q "READY" "$logfile" 2>/dev/null; then
            echo "  WARNING: ${node} not ready after 120s"
        fi
    done
    echo "--- Slaves restarted ---"
    echo ""
}

EXPERIMENT=0
TOTAL=$((${#DATASETS[@]} * ${#BATCH_RATIOS[@]}))
FIRST=1

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

        # Restart slaves between datasets (slaves exit after each run)
        if [[ "$FIRST" -ne 1 ]]; then
            restart_slaves
        fi
        FIRST=0

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
