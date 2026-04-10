#!/usr/bin/env bash
# =============================================================================
# resume.sh — Resume photo pipeline after crash or reboot
# =============================================================================
# Usage:  bash resume.sh
# Safe to run anytime — detects state and resumes what's needed.
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

PIPELINE_DIR="${PIPELINE_DIR}"
DB="$PIPELINE_DIR/photos.db"
AI_LOG="$PIPELINE_DIR/ai_classify.log"

log() { echo "[$(date '+%H:%M:%S')] $*"; }
ok()  { echo "  [OK]  $*"; }
run() { echo "  [RUN] $*"; }
skip(){ echo "  [--]  $*"; }

echo ""
echo "=============================================="
echo "  Photo Pipeline — Resume"
echo "  $(date '+%Y-%m-%d %H:%M:%S')"
echo "=============================================="
echo ""

# ── 1. AI Classify (Phase 6) ──────────────────────────────────────────────
AI_REMAINING=$(sqlite3 "$DB" "SELECT COUNT(*) FROM photos WHERE ai_processed=0 AND is_duplicate=0 AND media_type='image';" 2>/dev/null || echo 0)
AI_DONE=$(sqlite3 "$DB" "SELECT COUNT(*) FROM photos WHERE ai_processed=1 AND is_duplicate=0 AND media_type='image';" 2>/dev/null || echo 0)
AI_TOTAL=$(sqlite3 "$DB" "SELECT COUNT(*) FROM photos WHERE is_duplicate=0 AND media_type='image';" 2>/dev/null || echo 0)
AI_PID=$(cat "$PIPELINE_DIR/ai_classify.pid" 2>/dev/null || echo 0)

if [[ "$AI_REMAINING" -eq 0 ]]; then
    ok "AI classify complete ($AI_DONE/$AI_TOTAL)"
elif kill -0 "$AI_PID" 2>/dev/null; then
    ok "AI classify already running (PID $AI_PID) — $AI_DONE/$AI_TOTAL done, $AI_REMAINING left"
else
    run "Starting AI classify... ($AI_REMAINING images remaining)"
    cd "$PIPELINE_DIR"
    nohup python3 pipeline.py --step 6 >> "$AI_LOG" 2>&1 &
    echo $! > "$PIPELINE_DIR/ai_classify.pid"
    ok "AI classify started (PID $!)"
fi

# ── 2. Auto-watcher ───────────────────────────────────────────────────────
WATCHER_PID=$(pgrep -f "watch_ai_and_continue.sh" 2>/dev/null | head -1 || echo 0)

if kill -0 "$WATCHER_PID" 2>/dev/null; then
    ok "Auto-watcher already running (PID $WATCHER_PID)"
else
    run "Starting auto-watcher (will continue pipeline when AI finishes)..."
    cd "$PIPELINE_DIR"
    nohup bash watch_ai_and_continue.sh >> "$PIPELINE_DIR/orchestrator.log" 2>&1 &
    ok "Auto-watcher started (PID $!)"
fi

# ── 3. Final output ───────────────────────────────────────────────────────
FINAL_DIR="${FINAL_DIR}"
FINAL_COUNT=$(find "$FINAL_DIR" \( -type f -o -type l \) 2>/dev/null | wc -l)
FOLDER_COUNT=$(find "$FINAL_DIR" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | wc -l)

if [[ "$FINAL_COUNT" -gt 0 ]]; then
    ok "Output exists: $FINAL_COUNT files in $FOLDER_COUNT event folders"
else
    run "Rebuilding output (Phase 8)..."
    cd "$PIPELINE_DIR"
    python3 pipeline.py --step 8 >> "$PIPELINE_DIR/phase8.log" 2>&1
    ok "Phase 8 done"
fi

echo ""
echo "──────────────────────────────────────────────"
echo "  Status summary:"
printf "  AI classify:   %s/%s done (%s remaining)\n" "$AI_DONE" "$AI_TOTAL" "$AI_REMAINING"
printf "  Output:        %s files in %s event folders\n" "$FINAL_COUNT" "$FOLDER_COUNT"
echo ""
echo "  Check progress:"
echo "    bash $PIPELINE_DIR/progress.sh"
echo "    watch -n 60 bash $PIPELINE_DIR/progress.sh"
echo ""
echo "  After AI finishes, watcher auto-runs phases 7→8→9"
echo "  then upload with:  bash $PIPELINE_DIR/upload_to_gphotos.sh"
echo "=============================================="
echo ""
