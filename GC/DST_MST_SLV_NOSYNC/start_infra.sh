#!/bin/bash
# start_infra.sh — Launch GC NOSYNC slave + DPU on cn04 and cn05 via SLURM
# Usage: ./start_infra.sh [stop]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SLAVE_BIN="${SCRIPT_DIR}/slave"
SLAVE_PORT=6005
DPU_USER="ubuntu"
DPU_IP="192.168.100.2"
DPU_PORT=5007
DPU_ADDR="${DPU_IP}:${DPU_PORT}"
DPU_BIN_NAME="gc_dst_dpu"
NODES=("cn04" "cn05")
LOG_DIR="${SCRIPT_DIR}/logs"

if [[ "${1:-}" == "stop" ]]; then
    echo "Stopping slave+DPU processes..."
    for node in "${NODES[@]}"; do
        scancel --nodelist="${node}" --name="gc_nosync_slave" 2>/dev/null || true
    done
    echo "Done."
    exit 0
fi

if [[ ! -x "${SLAVE_BIN}" ]]; then
    echo "ERROR: slave binary not found at ${SLAVE_BIN}"
    echo "Build first: make slave"
    exit 1
fi

mkdir -p "${LOG_DIR}"

echo "Starting GC DST_MST_SLV_NOSYNC slave + DPU (slave port ${SLAVE_PORT}, DPU ${DPU_ADDR})..."
echo ""

for node in "${NODES[@]}"; do
    LOG_FILE="${LOG_DIR}/slave_${node}.log"
    echo "  Launching slave + DPU on ${node}..."

    sbatch --job-name="gc_nosync_slave" \
           --partition=mtech \
           --nodelist="${node}" \
           --ntasks=1 \
           --cpus-per-task=16 \
           --time=04:00:00 \
           --output="${LOG_FILE}" \
           --error="${LOG_FILE}" \
           --export="ALL,LD_LIBRARY_PATH=/opt/ohpc/pub/compiler/gcc/12.3.0/lib64:${LD_LIBRARY_PATH:-}" \
           --wrap="
# Cleanup: kill any leftover DPU process
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${DPU_USER}@${DPU_IP} 'pkill -x ${DPU_BIN_NAME} 2>/dev/null' || true
sleep 1

# Start fresh DPU server on ARM
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${DPU_USER}@${DPU_IP} 'nohup ~/${DPU_BIN_NAME} ${DPU_PORT} > /tmp/${DPU_BIN_NAME}.log 2>&1 &'
sleep 5

# Verify DPU is actually running
for attempt in 1 2 3; do
    if ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${DPU_USER}@${DPU_IP} 'ss -tlnp 2>/dev/null | grep -q ${DPU_PORT}'; then
        echo '[DPU] Verified: listening on ${DPU_IP}:${DPU_PORT}'
        break
    fi
    echo \"[DPU] Attempt \${attempt}: not listening yet, retrying...\"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${DPU_USER}@${DPU_IP} 'nohup ~/${DPU_BIN_NAME} ${DPU_PORT} > /tmp/${DPU_BIN_NAME}.log 2>&1 &'
    sleep 5
done

# Trap: kill DPU on exit
cleanup() {
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${DPU_USER}@${DPU_IP} 'pkill -x ${DPU_BIN_NAME} 2>/dev/null' || true
    echo '[DPU] Cleaned up on exit'
}
trap cleanup EXIT

# Run slave
${SLAVE_BIN} ${SLAVE_PORT} ${DPU_ADDR}
"

    echo "    -> Submitted (log: ${LOG_FILE})"
done

echo ""
echo "Wait for slaves to print 'READY on port ${SLAVE_PORT}' in their logs."
echo "Then run: ./master.sh"
