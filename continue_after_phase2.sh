#!/usr/bin/env bash
# continue_after_phase2.sh — Wait for sidecar-merge (phase 2) to finish, then continue.
#
# Run this if you started phase 2 separately and want phases 2.5–9 to auto-follow.
# Usage:  bash continue_after_phase2.sh

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ -f "$SCRIPT_DIR/.env" ]; then
    set -a; source "$SCRIPT_DIR/.env"; set +a
fi

PIPELINE_DIR="${PIPELINE_DIR:-$SCRIPT_DIR}"
DB="$PIPELINE_DIR/photos.db"
LOGFILE="$PIPELINE_DIR/orchestrator.log"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOGFILE"; }

# Poll for a running merge-sidecars process
PHASE2_PID=$(pgrep -f "pipeline.py.*merge-sidecars\|pipeline.py.*--step 2" 2>/dev/null | head -1 || echo "")

if [[ -n "$PHASE2_PID" ]]; then
    log "Polling for merge-sidecars (PID $PHASE2_PID) to complete..."
    while kill -0 "$PHASE2_PID" 2>/dev/null; do
        sleep 30
    done
    log "Phase 2 process ended."
fi

# Verify merge looks complete
MERGED=$(sqlite3 "$DB" "SELECT COUNT(*) FROM photos WHERE json_merged=1;" 2>/dev/null || echo 0)
TOTAL=$(sqlite3  "$DB" "SELECT COUNT(*) FROM photos WHERE has_json_sidecar=1;" 2>/dev/null || echo 0)
log "Phase 2 result: $MERGED/$TOTAL sidecars merged"

if [[ "$MERGED" -lt 1000 ]]; then
    log "WARNING: only $MERGED sidecars merged — phase 2 may be incomplete"
    log "Re-run with: bash run_pipeline.sh --from 2"
fi

log "Continuing from phase 2.5 (date fixing)..."
exec bash "$SCRIPT_DIR/run_pipeline.sh" --from 2.5
