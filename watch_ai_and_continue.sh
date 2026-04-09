#!/usr/bin/env bash
# Watches AI classify (phase 6) and auto-runs rerun_after_ai.sh when done.
# Run this in background: nohup bash watch_ai_and_continue.sh &


# Load configuration from .env if present
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/.env" ]; then
    # shellcheck disable=SC1091
    set -a; source "$SCRIPT_DIR/.env"; set +a
fi

# Defaults
PIPELINE_DIR="${PIPELINE_DIR:-$SCRIPT_DIR}"
FINAL_DIR="${FINAL_DIR:-}"
EVO_MOUNT="${EVO_MOUNT:-}"
IMMICH_MOUNT="${IMMICH_MOUNT:-}"
OLD_FINAL_DIR="${OLD_FINAL_DIR:-}"

PIPELINE_DIR="${PIPELINE_DIR}"
DB="$PIPELINE_DIR/pipeline_v2.db"
LOG="$PIPELINE_DIR/orchestrator.log"
PID_FILE="$PIPELINE_DIR/ai_classify.pid"
CHECK_INTERVAL=300  # check every 5 minutes

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [watcher] $*" | tee -a "$LOG"; }

log "Watcher started. Monitoring AI classify until complete..."

while true; do
    # Check if classify process is alive
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            DONE=$(sqlite3 "$DB" "SELECT COUNT(*) FROM photos WHERE ai_processed=1 AND is_duplicate=0 AND media_type='image';" 2>/dev/null)
            TOTAL=$(sqlite3 "$DB" "SELECT COUNT(*) FROM photos WHERE is_duplicate=0 AND media_type='image';" 2>/dev/null)
            REMAINING=$(sqlite3 "$DB" "SELECT COUNT(*) FROM photos WHERE ai_processed=0 AND is_duplicate=0 AND media_type='image';" 2>/dev/null)
            log "AI classify running: $DONE/$TOTAL classified, $REMAINING remaining (PID $PID)"
            sleep $CHECK_INTERVAL
            continue
        fi
    fi

    # Process not running — check if work remains
    REMAINING=$(sqlite3 "$DB" "SELECT COUNT(*) FROM photos WHERE ai_processed=0 AND is_duplicate=0 AND media_type='image';" 2>/dev/null)
    FAILED=$(sqlite3 "$DB" "SELECT COUNT(*) FROM photos WHERE ai_processed=-1 AND is_duplicate=0 AND media_type='image';" 2>/dev/null)

    if [ "$REMAINING" -gt 0 ] 2>/dev/null; then
        log "AI classify stopped with $REMAINING remaining — restarting..."
        nohup python3 "$PIPELINE_DIR/pipeline_v2.py" --phase 6 >> "$PIPELINE_DIR/ai_classify.log" 2>&1 &
        echo $! > "$PID_FILE"
        log "Restarted AI classify PID $(cat $PID_FILE)"
        sleep 30
        continue
    fi

    # All done!
    DONE=$(sqlite3 "$DB" "SELECT COUNT(*) FROM photos WHERE ai_processed=1 AND is_duplicate=0 AND media_type='image';" 2>/dev/null)
    log "AI classify COMPLETE: $DONE classified, $FAILED failed."
    log "Auto-running rerun_after_ai.sh..."
    bash "$PIPELINE_DIR/rerun_after_ai.sh"
    log "Post-AI rerun complete. Watcher exiting."
    exit 0
done
