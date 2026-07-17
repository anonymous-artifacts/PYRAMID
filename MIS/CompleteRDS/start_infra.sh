#!/bin/bash
###############################################################################
# start_infra.sh - Start/stop relay+DPU for CompleteRDS_MIS on real DPU ARM
#
# Usage:
#   ./start_infra.sh              # start relay+DPU on cn05
#   ./start_infra.sh stop         # stop
#   ./start_infra.sh deploy       # deploy DPU binary to ARM (one-time)
###############################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RELAY_BIN="${SCRIPT_DIR}/relay"
DPU_SRC="${SCRIPT_DIR}/CompleteRDS_MIS_dpu.c"
DPU_USER="ubuntu"
DPU_IP="192.168.100.2"
DPU_PORT=5004
RELAY_PORT=5004
NODE="cn05"
LOG_DIR="${SCRIPT_DIR}/logs"

case "${1:-start}" in
    deploy)
        echo "Deploying CompleteRDS_MIS_dpu.c to DPU ARM on ${NODE}..."
        srun -p mtech --nodelist="${NODE}" --ntasks=1 --time=00:05:00 \
            scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
            "${DPU_SRC}" ${DPU_USER}@${DPU_IP}:~/CompleteRDS_MIS_dpu.c
        echo "Compiling on DPU ARM..."
        srun -p mtech --nodelist="${NODE}" --ntasks=1 --time=00:05:00 \
            ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
            ${DPU_USER}@${DPU_IP} "gcc -O2 -o ~/CompleteRDS_MIS_dpu ~/CompleteRDS_MIS_dpu.c"
        echo "Deploy done."
        ;;

    stop)
        echo "Stopping relay..."
        scancel --nodelist="${NODE}" --name="mis_p123_relay" 2>/dev/null || true
        echo "Done."
        ;;

    start)
        if [[ ! -x "${RELAY_BIN}" ]]; then
            echo "Building relay..."
            cd "${SCRIPT_DIR}" && g++ -std=c++17 -O2 -Wall -o relay relay.cpp
        fi

        mkdir -p "${LOG_DIR}"
        LOG_FILE="${LOG_DIR}/relay_${NODE}.log"

        echo "Starting relay+DPU on ${NODE} (relay port ${RELAY_PORT} -> DPU ${DPU_IP}:${DPU_PORT})..."

        sbatch --job-name="mis_p123_relay" \
               --partition=mtech \
               --nodelist="${NODE}" \
               --ntasks=1 \
               --cpus-per-task=2 \
               --time=04:00:00 \
               --output="${LOG_FILE}" \
               --error="${LOG_FILE}" \
               --export="ALL,LD_LIBRARY_PATH=/opt/ohpc/pub/compiler/gcc/12.3.0/lib64:${LD_LIBRARY_PATH:-}" \
               --wrap="
# Start DPU server in background
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${DPU_USER}@${DPU_IP} 'pkill -x CompleteRDS_MIS_dpu 2>/dev/null; sleep 1; nohup ~/CompleteRDS_MIS_dpu ${DPU_PORT} > /tmp/mis_p123_dpu.log 2>&1 &'
sleep 2
echo '[DPU] MIS P123 server started on ${DPU_IP}:${DPU_PORT}'
# Run relay
${RELAY_BIN} ${RELAY_PORT} ${DPU_IP} ${DPU_PORT}
# Cleanup
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${DPU_USER}@${DPU_IP} 'pkill -x CompleteRDS_MIS_dpu 2>/dev/null' || true
"
        echo "Submitted. Check log: ${LOG_FILE}"
        echo "Wait for relay to print 'Listening on port ${RELAY_PORT}', then run master.sh"
        ;;

    *)
        echo "Usage: $0 [start|stop|deploy]"
        exit 1
        ;;
esac
