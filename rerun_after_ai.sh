#!/usr/bin/env bash
# Run this after AI classify finishes to improve event albums with full AI data.
# The AI classify (phase 6) takes ~5 days. Once done:
#   bash ${PIPELINE_DIR}/rerun_after_ai.sh


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
FINAL_DIR="${FINAL_DIR}"
DB="$PIPELINE_DIR/pipeline_v2.db"
LOG="$PIPELINE_DIR/orchestrator.log"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG"; }

log "=== POST-AI RERUN: Phases 7 → 7.5 → 8 → 9 ==="

AI_DONE=$(sqlite3 "$DB" "SELECT COUNT(*) FROM photos WHERE ai_processed=1 AND is_duplicate=0 AND media_type='image';" 2>/dev/null)
TOTAL=$(sqlite3 "$DB" "SELECT COUNT(*) FROM photos WHERE is_duplicate=0 AND media_type='image';" 2>/dev/null)
log "AI classify: $AI_DONE/$TOTAL classified"

# Clear auto albums (re-cluster with better AI data)
log "Clearing auto albums for re-clustering..."
python3 - <<'PYEOF'
import sqlite3, time
db = "${PIPELINE_DIR}/pipeline_v2.db"
for attempt in range(30):
    try:
        conn = sqlite3.connect(db, timeout=120)
        conn.execute("PRAGMA busy_timeout=120000")
        conn.execute("UPDATE photos SET album_id=NULL, album_name=NULL WHERE album_id IN (SELECT id FROM albums WHERE source='auto')")
        conn.execute("DELETE FROM albums WHERE source='auto'")
        conn.commit()
        conn.close()
        print("Done")
        break
    except sqlite3.OperationalError as e:
        print(f"Retry {attempt+1}: {e}")
        time.sleep(10)
PYEOF

log "Re-running Phase 7 (album grouping with full AI data)..."
python3 "$PIPELINE_DIR/pipeline_v2.py" --phase 7 >> "$PIPELINE_DIR/phase7_post_ai.log" 2>&1

ALBUM_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM albums;" 2>/dev/null)
log "Phase 7 complete: $ALBUM_COUNT albums"

log "Re-running Phase 7.5 (AI event naming with full descriptions)..."
python3 "$PIPELINE_DIR/name_events.py" >> "$PIPELINE_DIR/name_events_post_ai.log" 2>&1

log "Re-running Phase 8 (organize to $FINAL_DIR with event-based folders)..."
python3 "$PIPELINE_DIR/pipeline_v2.py" --phase 8 >> "$PIPELINE_DIR/phase8_post_ai.log" 2>&1

FILE_COUNT=$(find "$FINAL_DIR" \( -type f -o -type l \) 2>/dev/null | wc -l)
log "Phase 8 complete: $FILE_COUNT files"

log "Running Phase 9 (upload prep)..."
python3 "$PIPELINE_DIR/pipeline_v2.py" --phase 9 >> "$PIPELINE_DIR/phase9_post_ai.log" 2>&1
python3 "$PIPELINE_DIR/google_photos_upload.py" >> "$PIPELINE_DIR/upload.log" 2>&1 || true

log ""
log "====================================="
log "POST-AI RERUN COMPLETE"
log "  Files: $FILE_COUNT in $FINAL_DIR"
log "  Upload: bash $PIPELINE_DIR/upload_to_gphotos.sh"
log "====================================="
