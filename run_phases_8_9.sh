#!/usr/bin/env bash
# run_phases_8_9.sh — Re-run export + upload-prep after AI naming completes.
#
# Usage:
#   bash run_phases_8_9.sh             # wait for name_events if running, then export
#   bash run_phases_8_9.sh --dry-run   # preview
#
# Resumable: safe to re-run at any time.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ -f "$SCRIPT_DIR/.env" ]; then
    set -a; source "$SCRIPT_DIR/.env"; set +a
fi

PIPELINE_DIR="${PIPELINE_DIR:-$SCRIPT_DIR}"
FINAL_DIR="${FINAL_DIR:-}"
LOG="$PIPELINE_DIR/orchestrator.log"
DRY_RUN=""
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN="--dry-run"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG"; }

log "=== Phases 8 + 9 ==="

# Wait for any running name_events.py to finish (use PID file, not hardcoded PID)
NAME_EVENTS_PID_FILE="$PIPELINE_DIR/name_events.pid"
if [ -f "$NAME_EVENTS_PID_FILE" ]; then
    NE_PID=$(cat "$NAME_EVENTS_PID_FILE" 2>/dev/null || echo 0)
    if [ "$NE_PID" -gt 0 ] && kill -0 "$NE_PID" 2>/dev/null; then
        log "Waiting for name_events.py (PID $NE_PID) to finish..."
        while kill -0 "$NE_PID" 2>/dev/null; do
            RENAMED=$(grep -c "→" "$PIPELINE_DIR/name_events.log" 2>/dev/null || echo 0)
            log "  name_events in progress: $RENAMED albums renamed..."
            sleep 30
        done
        log "name_events.py complete."
    fi
fi

# Phase 8: export to final directory
log "=== Phase 8: Export to $FINAL_DIR ==="
python3 "$SCRIPT_DIR/pipeline.py" --step export $DRY_RUN >> "$PIPELINE_DIR/phase8.log" 2>&1
FILE_COUNT=$(find "$FINAL_DIR" \( -type f -o -type l \) 2>/dev/null | wc -l)
log "Phase 8 complete: $FILE_COUNT files in $FINAL_DIR"

# Phase 9: upload prep
log "=== Phase 9: Upload Prep ==="
python3 "$SCRIPT_DIR/pipeline.py" --step prep-upload $DRY_RUN >> "$PIPELINE_DIR/phase9.log" 2>&1 || true
python3 "$SCRIPT_DIR/google_photos_upload.py" >> "$PIPELINE_DIR/upload.log" 2>&1 || true
log "Phase 9 complete."

log ""
log "═══════════════════════════════════════════"
log "DONE: $FILE_COUNT files in $FINAL_DIR"
log "Upload: bash upload_to_gphotos.sh"
log "Instagram: bash run_instagram.sh"
log "═══════════════════════════════════════════"
