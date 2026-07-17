#!/bin/bash
# start_slaves.sh — Launch GC slave processes on cn04 and cn05 via SLURM
# Usage: ./start_slaves.sh [stop]

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SLAVE_BIN="${SCRIPT_DIR}/slave"
LOG_DIR="${SCRIPT_DIR}/logs"
SLAVE_PORT=6001
NUM_THREADS=16

mkdir -p "$LOG_DIR"

if [[ "$1" == "stop" ]]; then
    echo "Stopping slave jobs..."
    scancel --name=gc_dist_slave_cn04 2>/dev/null
    scancel --name=gc_dist_slave_cn05 2>/dev/null
    echo "Done."
    exit 0
fi

if [[ ! -f "$SLAVE_BIN" ]]; then
    echo "ERROR: $SLAVE_BIN not found. Run 'make' first."
    exit 1
fi

# Clear old logs so stale READY doesn't fool the wait loop
rm -f "${LOG_DIR}/slave_cn04.log" "${LOG_DIR}/slave_cn05.log"

echo "Starting GC Distributed slaves on cn04 and cn05 (port ${SLAVE_PORT})..."

# Kill any existing slave jobs
scancel --name=gc_dist_slave_cn04 2>/dev/null
scancel --name=gc_dist_slave_cn05 2>/dev/null
sleep 1

# Launch on cn04
sbatch --partition=mtech \
       --job-name=gc_dist_slave_cn04 \
       --nodelist=cn04 \
       --ntasks=1 \
       --cpus-per-task=16 \
       --time=08:00:00 \
       --output="${LOG_DIR}/slave_cn04.log" \
       --error="${LOG_DIR}/slave_cn04.log" \
       --wrap="${SLAVE_BIN} ${SLAVE_PORT} ${NUM_THREADS}"

# Launch on cn05
sbatch --partition=mtech \
       --job-name=gc_dist_slave_cn05 \
       --nodelist=cn05 \
       --ntasks=1 \
       --cpus-per-task=16 \
       --time=08:00:00 \
       --output="${LOG_DIR}/slave_cn05.log" \
       --error="${LOG_DIR}/slave_cn05.log" \
       --wrap="${SLAVE_BIN} ${SLAVE_PORT} ${NUM_THREADS}"

echo "Slaves submitted. Waiting for READY..."

# Wait for both slaves to be ready
for node in cn04 cn05; do
    logfile="${LOG_DIR}/slave_${node}.log"
    for i in $(seq 1 60); do
        if grep -q "READY" "$logfile" 2>/dev/null; then
            echo "  ${node}: READY"
            break
        fi
        sleep 1
    done
    if ! grep -q "READY" "$logfile" 2>/dev/null; then
        echo "  WARNING: ${node} not ready after 60s"
    fi
done

echo "All slaves ready."
