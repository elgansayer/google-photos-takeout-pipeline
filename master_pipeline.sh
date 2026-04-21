#!/usr/bin/env bash
# =============================================================================
# master_pipeline.sh — Photo Pipeline Master Orchestrator
# =============================================================================
# Continues pipeline after crash. Handles:
#   - AI classify (background, resumes from checkpoint)
#   - Date fixing (neighbor inference)
#   - Event album grouping & AI naming
#   - Output to ${FINAL_DIR}/ (symlinks)
#   - Upload prep for Google Photos
#
# Usage:
#   bash master_pipeline.sh              # run everything
#   bash master_pipeline.sh --from 7    # start from phase N
#   bash master_pipeline.sh --ai-only   # just restart AI classify
#   bash master_pipeline.sh --status    # show status and exit
#   bash master_pipeline.sh --after-ai  # run phases 7-9 after AI finishes
#
# Progress:  tail -f orchestrator.log
# Status:    bash progress.sh
# =============================================================================


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

set -uo pipefail

PIPELINE_DIR="${PIPELINE_DIR}"
FINAL_DIR="${FINAL_DIR}"
OLD_FINAL_DIR="${OLD_FINAL_DIR}"
DB="$PIPELINE_DIR/photos.db"
LOG="$PIPELINE_DIR/orchestrator.log"
PID_FILE="$PIPELINE_DIR/ai_classify.pid"
PROGRESS_FILE="$PIPELINE_DIR/progress.json"

cd "$PIPELINE_DIR"

# ── Logging ────────────────────────────────────────────────────────────────
log() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $*"
    echo "$msg" | tee -a "$LOG"
}

err() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*"
    echo "$msg" | tee -a "$LOG" >&2
}

# ── Progress JSON ──────────────────────────────────────────────────────────
update_progress() {
    local phase="$1" status="$2" msg="${3:-}"
    python3 -c "
import json, time
from pathlib import Path
f = Path('$PROGRESS_FILE')
try:
    d = json.loads(f.read_text()) if f.exists() else {'phases': {}}
except:
    d = {'phases': {}}
d['phases']['$phase'] = {'status': '$status', 'msg': '''$msg''', 'ts': time.time()}
d['last_update'] = time.time()
f.write_text(json.dumps(d, indent=2))
" 2>/dev/null || true
}

# ── Parse args ─────────────────────────────────────────────────────────────
FROM_PHASE=0
AI_ONLY=false
STATUS_ONLY=false
AFTER_AI=false
DRY_RUN=false

for arg in "$@"; do
    case "$arg" in
        --from)    shift; FROM_PHASE="${1:-0}" ;;
        --from=*)  FROM_PHASE="${arg#--from=}" ;;
        --ai-only) AI_ONLY=true ;;
        --status)  STATUS_ONLY=true ;;
        --after-ai) AFTER_AI=true ;;
        --dry-run) DRY_RUN=true ;;
    esac
done

# ── Status only ────────────────────────────────────────────────────────────
if $STATUS_ONLY; then
    bash "$PIPELINE_DIR/progress.sh"
    exit 0
fi

# ── Check prereqs ──────────────────────────────────────────────────────────
check_prereqs() {
    log "Checking prerequisites..."
    python3 -c "import sqlite3, requests" 2>/dev/null || { err "Missing Python deps"; exit 1; }

    if ! mountpoint -q ${EVO_MOUNT} 2>/dev/null; then
        err "EVO drive not mounted at ${EVO_MOUNT}"
        exit 1
    fi
    if ! mountpoint -q ${IMMICH_MOUNT} 2>/dev/null; then
        err "Immich drive not mounted at ${IMMICH_MOUNT}"
        exit 1
    fi

    local FREE_IMMICH
    FREE_IMMICH=$(df -BG ${IMMICH_MOUNT} 2>/dev/null | awk 'NR==2{gsub(/G/,"",$4); print $4}')
    log "Immich free space: ${FREE_IMMICH}GB"

    if [[ "${FREE_IMMICH:-0}" -lt 5 ]]; then
        err "Less than 5GB free on immich — aborting"
        exit 1
    fi

    # Verify ollama is up
    if curl -sf http://localhost:11434/api/version >/dev/null 2>&1; then
        log "Ollama: running"
    else
        log "WARNING: ollama not responding — AI steps will be skipped"
    fi
}

# ── AI Classify ────────────────────────────────────────────────────────────
restart_ai_classify() {
    log "=== Restarting AI Classify (Phase 6) ==="

    # Kill existing if running
    if [[ -f "$PID_FILE" ]]; then
        local OLD_PID
        OLD_PID=$(cat "$PID_FILE" 2>/dev/null || echo 0)
        if [[ "$OLD_PID" -gt 0 ]] && kill -0 "$OLD_PID" 2>/dev/null; then
            log "AI classify already running (PID $OLD_PID), keeping it"
            return 0
        fi
    fi

    # Check how many left
    local AI_DONE TOTAL_IMGS
    AI_DONE=$(sqlite3 "$DB" "SELECT COUNT(*) FROM photos WHERE ai_processed=1 AND is_duplicate=0 AND media_type='image';" 2>/dev/null || echo 0)
    TOTAL_IMGS=$(sqlite3 "$DB" "SELECT COUNT(*) FROM photos WHERE is_duplicate=0 AND media_type='image';" 2>/dev/null || echo 0)
    local AI_LEFT=$((TOTAL_IMGS - AI_DONE))

    log "AI classify: $AI_DONE/$TOTAL_IMGS done, $AI_LEFT remaining"

    if [[ "$AI_LEFT" -le 0 ]]; then
        log "AI classify already complete!"
        update_progress "6_ai" "complete" "$AI_DONE/$TOTAL_IMGS images classified"
        return 0
    fi

    # Launch in background with nohup
    log "Launching AI classify in background..."
    nohup python3 "$PIPELINE_DIR/pipeline.py" --step 6 \
        >> "$PIPELINE_DIR/ai_classify.log" 2>&1 &
    local NEW_PID=$!
    echo "$NEW_PID" > "$PID_FILE"
    log "AI classify started: PID $NEW_PID (~$(( AI_LEFT * 46 / 3600 ))h remaining)"
    update_progress "6_ai" "running" "PID=$NEW_PID, $AI_DONE/$TOTAL_IMGS done"
}

# ── Date Fixing ────────────────────────────────────────────────────────────
run_date_fix() {
    log "=== Phase 2.5: Date Fixing ==="
    update_progress "2.5_dates" "running" "Started"

    local DRY=""
    $DRY_RUN && DRY="--dry-run"

    # First run the original fix_dates.py (JSON sidecars, EXIF)
    log "Running fix_dates.py (JSON sidecar + EXIF method)..."
    python3 "$PIPELINE_DIR/fix_dates.py" $DRY >> "$PIPELINE_DIR/fix_dates.log" 2>&1 || true

    # Then run the neighbor inference
    log "Running neighbor_date_fix.py (directory consensus + filename sequences)..."
    python3 "$PIPELINE_DIR/neighbor_date_fix.py" $DRY \
        >> "$PIPELINE_DIR/neighbor_date_fix.log" 2>&1 || true

    update_progress "2.5_dates" "complete" "Date fixing done"
    log "Phase 2.5 complete"
}

# ── Album Regrouping ───────────────────────────────────────────────────────
regroup_albums() {
    log "=== Phase 7: Re-group Event Albums ==="
    update_progress "7_albums" "running" "Clearing auto albums and re-grouping"

    local DRY=""
    $DRY_RUN && DRY="--dry-run"

    # Clear auto-generated albums to re-cluster with improved dates + AI data
    log "Clearing auto-generated albums (keeping existing Google albums)..."
    if ! $DRY_RUN; then
        python3 - "$DB" <<'EOF'
import sqlite3, sys, time
db = sys.argv[1]
for attempt in range(30):
    try:
        conn = sqlite3.connect(db, timeout=120)
        conn.execute("PRAGMA busy_timeout=120000")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("UPDATE photos SET album_id=NULL, album_name=NULL WHERE album_id IN (SELECT id FROM albums WHERE source='auto')")
        conn.execute("DELETE FROM albums WHERE source='auto'")
        conn.commit()
        conn.close()
        print("Auto albums cleared")
        break
    except sqlite3.OperationalError as e:
        print(f"DB busy, retry {attempt+1}/30: {e}")
        time.sleep(10)
EOF
        log "Cleared auto albums"
    fi

    log "Re-running Phase 7 (event clustering)..."
    python3 "$PIPELINE_DIR/pipeline.py" --step 7 $DRY \
        >> "$PIPELINE_DIR/phase7.log" 2>&1 || { err "Phase 7 failed"; return 1; }

    local ALBUM_COUNT
    ALBUM_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM albums;" 2>/dev/null || echo "?")
    update_progress "7_albums" "complete" "$ALBUM_COUNT total albums created"
    log "Phase 7 complete: $ALBUM_COUNT albums"
}

# ── AI Event Naming ────────────────────────────────────────────────────────
rename_events() {
    log "=== Phase 7.5: AI Event Naming ==="
    update_progress "7.5_names" "running" "Renaming auto albums with AI"

    local DRY=""
    $DRY_RUN && DRY="--dry-run"

    if ! curl -sf http://localhost:11434/api/version >/dev/null 2>&1; then
        log "WARNING: ollama not available, skipping AI naming"
        update_progress "7.5_names" "skipped" "ollama not available"
        return 0
    fi

    python3 "$PIPELINE_DIR/name_events.py" $DRY \
        >> "$PIPELINE_DIR/name_events.log" 2>&1 || true

    local RENAMED
    RENAMED=$(grep -c "→" "$PIPELINE_DIR/name_events.log" 2>/dev/null || echo "?")
    update_progress "7.5_names" "complete" "$RENAMED albums renamed"
    log "Phase 7.5 complete"
}

# ── Organize to Immich ─────────────────────────────────────────────────────
organize_to_immich() {
    log "=== Phase 8: Organize to $FINAL_DIR ==="
    update_progress "8_organize" "running" "Organizing photos into event dirs"

    local DRY=""
    $DRY_RUN && DRY="--dry-run"

    # Clear existing output
    if [[ -d "$FINAL_DIR" ]] && ! $DRY_RUN; then
        log "Clearing existing $FINAL_DIR..."
        rm -rf "$FINAL_DIR"
    fi
    mkdir -p "$FINAL_DIR" 2>/dev/null || true

    python3 "$PIPELINE_DIR/pipeline.py" --step 8 $DRY \
        >> "$PIPELINE_DIR/phase8.log" 2>&1 || { err "Phase 8 failed"; return 1; }

    local FILE_COUNT
    FILE_COUNT=$(find "$FINAL_DIR" -type f -o -type l 2>/dev/null | wc -l)
    update_progress "8_organize" "complete" "$FILE_COUNT files organized"
    log "Phase 8 complete: $FILE_COUNT files in $FINAL_DIR"
}

# ── Upload Prep ────────────────────────────────────────────────────────────
upload_prep() {
    log "=== Phase 9: Upload Prep ==="
    update_progress "9_upload" "running" "Generating upload scripts"

    local DRY=""
    $DRY_RUN && DRY="--dry-run"

    python3 "$PIPELINE_DIR/pipeline.py" --step 9 $DRY \
        >> "$PIPELINE_DIR/phase9.log" 2>&1 || true

    # Regenerate upload script pointing to new FINAL_DIR
    python3 "$PIPELINE_DIR/google_photos_upload.py" $DRY \
        >> "$PIPELINE_DIR/upload.log" 2>&1 || true

    update_progress "9_upload" "complete" "Upload scripts ready"
    log "Phase 9 complete"
}

# ── Post-AI re-run ─────────────────────────────────────────────────────────
wait_for_ai_then_rerun() {
    log "Waiting for AI classify to finish before re-running phases 7-9..."
    while true; do
        if [[ -f "$PID_FILE" ]]; then
            local PID
            PID=$(cat "$PID_FILE" 2>/dev/null || echo 0)
            if [[ "$PID" -gt 0 ]] && kill -0 "$PID" 2>/dev/null; then
                local AI_DONE TOTAL
                AI_DONE=$(sqlite3 "$DB" "SELECT COUNT(*) FROM photos WHERE ai_processed=1 AND is_duplicate=0 AND media_type='image';" 2>/dev/null || echo 0)
                TOTAL=$(sqlite3 "$DB" "SELECT COUNT(*) FROM photos WHERE is_duplicate=0 AND media_type='image';" 2>/dev/null || echo 1)
                local PCT=$(( AI_DONE * 100 / TOTAL ))
                log "AI classify: $AI_DONE/$TOTAL ($PCT%) — still running, checking again in 30min"
                sleep 1800
                continue
            fi
        fi
        break
    done
    log "AI classify finished! Re-running phases 7-9 with full AI data..."
    regroup_albums
    rename_events
    organize_to_immich
    upload_prep
}

# ── Main ───────────────────────────────────────────────────────────────────
main() {
    log ""
    log "============================================================"
    log "   PHOTO PIPELINE — MASTER ORCHESTRATOR"
    log "   $(date)"
    log "   FROM_PHASE=$FROM_PHASE  AI_ONLY=$AI_ONLY  DRY=$DRY_RUN"
    log "============================================================"

    check_prereqs

    # Always restart AI classify (it runs in background)
    restart_ai_classify

    if $AI_ONLY; then
        log "AI-only mode: AI classify restarted. Done."
        exit 0
    fi

    if $AFTER_AI; then
        wait_for_ai_then_rerun
        exit 0
    fi

    # Main pipeline — run phases in order, respecting --from N
    run_phase() {
        local PHASE_NUM=$1
        shift
        if [[ "$PHASE_NUM" -ge "$FROM_PHASE" ]]; then
            "$@"
        else
            log "Skipping phase $PHASE_NUM (< --from $FROM_PHASE)"
        fi
    }

    run_phase 2 run_date_fix
    run_phase 7 regroup_albums
    run_phase 7 rename_events
    run_phase 8 organize_to_immich
    run_phase 9 upload_prep

    log ""
    log "============================================================"
    log "   PIPELINE COMPLETE"
    log "   Output: $FINAL_DIR"
    log ""
    log "   AI classify still running in background."
    log "   When it finishes, run:"
    log "     bash $PIPELINE_DIR/master_pipeline.sh --after-ai"
    log "   to re-run album grouping + naming with full AI data."
    log ""
    log "   To upload to Google Photos:"
    log "     bash $PIPELINE_DIR/upload_to_gphotos.sh"
    log "============================================================"
    log ""

    bash "$PIPELINE_DIR/progress.sh"
}

main "$@"
