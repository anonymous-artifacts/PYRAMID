#!/bin/bash
###############################################################################
# start_infra.sh - Start/stop relay+DPU for FullRDS_GC thread scaling
#
# Delegates to the FullRDS infrastructure scripts.
#
# Usage:
#   ./start_infra.sh              # start relay+DPU on cn05
#   ./start_infra.sh stop         # stop
#   ./start_infra.sh deploy       # deploy DPU binary to ARM (one-time)
###############################################################################

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
P1P2_DIR="${SCRIPT_DIR}/../FullRDS"

exec "${P1P2_DIR}/start_infra.sh" "${1:-start}"
