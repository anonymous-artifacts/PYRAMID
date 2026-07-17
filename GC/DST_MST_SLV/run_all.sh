#!/bin/bash
# run_all.sh — Full orchestration: start infra, run experiments, stop infra

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Step 1: Start slaves + DPU ==="
"$SCRIPT_DIR/start_infra.sh"

echo ""
echo "=== Waiting for slaves to be ready ==="
LOG_DIR="${SCRIPT_DIR}/logs"
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

echo ""
echo "=== Step 2: Run experiments ==="
"$SCRIPT_DIR/master.sh"

echo ""
echo "=== Step 3: Stop infra ==="
"$SCRIPT_DIR/start_infra.sh" stop

echo ""
echo "=== Done ==="
