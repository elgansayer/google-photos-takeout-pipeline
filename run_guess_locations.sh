#!/usr/bin/env bash
# run_guess_locations.sh — Infer GPS coordinates for photos missing location data.
#
# Usage:
#   bash run_guess_locations.sh                   # run all strategies
#   bash run_guess_locations.sh --step report     # just show stats
#   bash run_guess_locations.sh --step temporal   # one strategy
#   bash run_guess_locations.sh --apply-all       # accept medium/low confidence too
#   bash run_guess_locations.sh --dry-run         # preview
#
# Resumable: already-guessed photos are skipped.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ -f "$SCRIPT_DIR/.env" ]; then
    set -a; source "$SCRIPT_DIR/.env"; set +a
fi

EVO_MOUNT="${EVO_MOUNT:-/run/media/elgan/evo}"
DB="${PIPELINE_DIR:-$SCRIPT_DIR}/photos.db"
LOG="${PIPELINE_DIR:-$SCRIPT_DIR}/guess_locations.log"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG"; }
err() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" | tee -a "$LOG" >&2; }

if ! mountpoint -q "$EVO_MOUNT" 2>/dev/null; then
    err "Drive not mounted at $EVO_MOUNT"
    if [ ! -e "$DB" ]; then
        err "Database $DB not found. Is the drive connected?"
        exit 1
    fi
fi

log "=== GUESS LOCATIONS PIPELINE ==="
exec python3 "$SCRIPT_DIR/guess_locations.py" "$@"
