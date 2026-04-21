#!/usr/bin/env bash
# rerun_after_ai.sh — Re-run album grouping + naming + export after AI classify finishes.
#
# Usage:  bash rerun_after_ai.sh
# Safe to re-run; phases 7-9 are idempotent.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ -f "$SCRIPT_DIR/.env" ]; then
    set -a; source "$SCRIPT_DIR/.env"; set +a
fi

PIPELINE_DIR="${PIPELINE_DIR:-$SCRIPT_DIR}"
FINAL_DIR="${FINAL_DIR:-}"
DB="$PIPELINE_DIR/photos.db"
LOG="$PIPELINE_DIR/orchestrator.log"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG"; }

log "=== POST-AI RERUN: Phases 7 → 7.5 → 8 → 9 ==="

AI_DONE=$(sqlite3 "$DB" "SELECT COUNT(*) FROM photos WHERE ai_processed=1 AND is_duplicate=0 AND media_type='image';" 2>/dev/null || echo 0)
TOTAL=$(sqlite3   "$DB" "SELECT COUNT(*) FROM photos WHERE is_duplicate=0 AND media_type='image';"     2>/dev/null || echo 0)
log "AI classify: $AI_DONE/$TOTAL classified"

# Clear auto albums so phase 7 re-clusters with richer AI data
log "Clearing auto albums for re-clustering..."
python3 - "$DB" <<'EOF'
import sqlite3, sys, time
db = sys.argv[1]
for attempt in range(30):
    try:
        conn = sqlite3.connect(db, timeout=120)
        conn.execute("PRAGMA busy_timeout=120000")
        conn.execute("UPDATE photos SET album_id=NULL, album_name=NULL WHERE album_id IN (SELECT id FROM albums WHERE source='auto')")
        conn.execute("DELETE FROM albums WHERE source='auto'")
        conn.commit()
        conn.close()
        print("Auto albums cleared.")
        break
    except sqlite3.OperationalError as e:
        print(f"Retry {attempt+1}/30: {e}")
        time.sleep(10)
EOF

log "Phase 7: re-clustering with full AI data..."
python3 "$SCRIPT_DIR/pipeline.py" --step group-albums >> "$PIPELINE_DIR/phase7_post_ai.log" 2>&1
ALBUM_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM albums;" 2>/dev/null || echo "?")
log "Phase 7 complete: $ALBUM_COUNT albums"

log "Phase 7.5: AI event naming..."
python3 "$SCRIPT_DIR/name_events.py" >> "$PIPELINE_DIR/name_events_post_ai.log" 2>&1

log "Phase 8: organizing to $FINAL_DIR..."
python3 "$SCRIPT_DIR/pipeline.py" --step export >> "$PIPELINE_DIR/phase8_post_ai.log" 2>&1
FILE_COUNT=$(find "$FINAL_DIR" \( -type f -o -type l \) 2>/dev/null | wc -l)
log "Phase 8 complete: $FILE_COUNT files"

log "Phase 9: upload prep..."
python3 "$SCRIPT_DIR/pipeline.py" --step prep-upload >> "$PIPELINE_DIR/phase9_post_ai.log" 2>&1 || true
python3 "$SCRIPT_DIR/google_photos_upload.py" >> "$PIPELINE_DIR/upload.log" 2>&1 || true

log ""
log "═══════════════════════════════════════════════"
log "POST-AI RERUN COMPLETE"
log "  Files:    $FILE_COUNT in $FINAL_DIR"
log "  Upload:   bash upload_to_gphotos.sh"
log "  Instagram: bash run_instagram.sh"
log "═══════════════════════════════════════════════"
