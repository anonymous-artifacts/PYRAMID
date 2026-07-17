#!/bin/bash
# run_all.sh — Full orchestration: start slaves, run experiments, stop slaves

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Step 1: Start slaves ==="
"$SCRIPT_DIR/start_slaves.sh"

echo ""
echo "=== Step 2: Run experiments ==="
"$SCRIPT_DIR/master.sh"

echo ""
echo "=== Step 3: Stop slaves ==="
"$SCRIPT_DIR/start_slaves.sh" stop

echo ""
echo "=== Done ==="
