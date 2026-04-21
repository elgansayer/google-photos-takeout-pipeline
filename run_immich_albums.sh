#!/usr/bin/env bash
# run_immich_albums.sh — Create/update Immich albums from event folders.
#
# Usage:
#   bash run_immich_albums.sh           # create albums from FINAL_DIR
#   bash run_immich_albums.sh --dry-run # preview without changes

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ -f "$SCRIPT_DIR/.env" ]; then set -a; source "$SCRIPT_DIR/.env"; set +a; fi

PIPELINE_DIR="${PIPELINE_DIR:-$SCRIPT_DIR}"
FINAL_DIR="${FINAL_DIR:-}"
LOG="$PIPELINE_DIR/immich_albums.log"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG"; }

if [ -z "$FINAL_DIR" ] || [ ! -d "$FINAL_DIR" ]; then
    echo "ERROR: FINAL_DIR not set or not found: ${FINAL_DIR:-unset}"
    exit 1
fi

log "=== Immich Albums: creating from $FINAL_DIR ==="
python3 "$SCRIPT_DIR/immich_albums.py" "$@" 2>&1 | tee -a "$LOG"
log "=== Done ==="
