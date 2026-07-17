#!/bin/bash
# deploy_dpu.sh — Deploy and compile MIS DPU kernel on ARM cores
# (Same DPU kernel as DST_MST_SLV — DPU logic is unchanged)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DPU_SRC="${SCRIPT_DIR}/dpu_kernel.c"
DPU_USER="ubuntu"
DPU_IP="192.168.100.2"
DPU_BIN_NAME="mis_dst_dpu"
NODES=("cn04" "cn05")

if [[ ! -f "${DPU_SRC}" ]]; then
    echo "ERROR: dpu_kernel.c not found at ${DPU_SRC}"
    exit 1
fi

echo "=== Deploying MIS DST_MST_SLV_NOSYNC DPU kernel to ARM cores ==="
echo ""

for node in "${NODES[@]}"; do
    echo "--- ${node} ---"

    echo "  [1/3] Copying dpu_kernel.c to ${node}'s DPU..."
    srun -p mtech --nodelist="${node}" --ntasks=1 --time=00:02:00 \
        scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        "${DPU_SRC}" ${DPU_USER}@${DPU_IP}:~/${DPU_BIN_NAME}.c

    echo "  [2/3] Compiling on DPU ARM..."
    srun -p mtech --nodelist="${node}" --ntasks=1 --time=00:02:00 \
        ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        ${DPU_USER}@${DPU_IP} "gcc -O2 -o ~/${DPU_BIN_NAME} ~/${DPU_BIN_NAME}.c && echo BUILD_OK"

    echo "  [3/3] Verifying binary..."
    srun -p mtech --nodelist="${node}" --ntasks=1 --time=00:01:00 \
        ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        ${DPU_USER}@${DPU_IP} "file ~/${DPU_BIN_NAME} && uname -m"

    echo "  -> ${node} DPU: DONE"
    echo ""
done

echo "=== Deployment complete ==="
