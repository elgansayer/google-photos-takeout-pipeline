#!/usr/bin/env bash
# Quick status check for all running pipelines
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/.env" ]; then set -a; source "$SCRIPT_DIR/.env"; set +a; fi

PIPELINE_DIR="${PIPELINE_DIR:-$SCRIPT_DIR}"
FINAL_DIR="${FINAL_DIR:-}"

sep() { printf '%s\n' "────────────────────────────────────────────"; }

sep
echo "  INSTAGRAM PIPELINE"
sep
if [ -f "$SCRIPT_DIR/instagram_pipeline.pid" ] && kill -0 "$(cat "$SCRIPT_DIR/instagram_pipeline.pid")" 2>/dev/null; then
    scored=$(sqlite3 "$SCRIPT_DIR/instagram.db" "SELECT COUNT(*) FROM scores;" 2>/dev/null)
    curated=$(sqlite3 "$SCRIPT_DIR/instagram.db" "SELECT COUNT(DISTINCT album_name) FROM curated;" 2>/dev/null)
    ready=$(find "$SCRIPT_DIR/instagram_ready" -name "caption.txt" 2>/dev/null | wc -l)
    last=$(grep "Scored" "$SCRIPT_DIR/instagram_run.log" 2>/dev/null | tail -1)
    echo "  Status:  RUNNING (PID $(cat "$SCRIPT_DIR/instagram_pipeline.pid"))"
    echo "  Scored:  $scored photos | $curated albums curated | $ready ready to upload"
    echo "  Last:    $last"
else
    echo "  Status:  NOT RUNNING"
    if [ -f "$SCRIPT_DIR/instagram_run.log" ]; then
        tail -3 "$SCRIPT_DIR/instagram_run.log" | sed 's/^/  /'
    fi
fi

sep
echo "  GUESS LOCATIONS"
sep
if [ -f "$SCRIPT_DIR/guess_locations.pid" ] && kill -0 "$(cat "$SCRIPT_DIR/guess_locations.pid")" 2>/dev/null; then
    echo "  Status:  RUNNING (PID $(cat "$SCRIPT_DIR/guess_locations.pid"))"
    tail -3 "$SCRIPT_DIR/guess_locations_run.log" 2>/dev/null | sed 's/^/  /'
else
    echo "  Status:  $([ -f "$SCRIPT_DIR/guess_locations_run.log" ] && echo 'COMPLETED' || echo 'NOT STARTED')"
    tail -3 "$SCRIPT_DIR/guess_locations_run.log" 2>/dev/null | sed 's/^/  /'
fi

sep
echo "  AI CLASSIFY"
sep
AI_PID=$(cat "$PIPELINE_DIR/ai_classify.pid" 2>/dev/null || echo 0)
if [ "${AI_PID:-0}" -gt 0 ] && kill -0 "$AI_PID" 2>/dev/null; then
    done=$(sqlite3 "$PIPELINE_DIR/photos.db" "SELECT COUNT(*) FROM photos WHERE ai_processed=1 AND is_duplicate=0 AND media_type='image';" 2>/dev/null || echo "?")
    total=$(sqlite3 "$PIPELINE_DIR/photos.db" "SELECT COUNT(*) FROM photos WHERE is_duplicate=0 AND media_type='image';" 2>/dev/null || echo "?")
    echo "  Status:  RUNNING (PID $AI_PID)"
    echo "  Progress: $done / $total"
else
    echo "  Status:  NOT RUNNING"
fi

sep
echo "  OUTPUT"
sep
if [ -n "$FINAL_DIR" ] && [ -d "$FINAL_DIR" ]; then
    fcount=$(find "$FINAL_DIR" \( -type f -o -type l \) 2>/dev/null | wc -l)
    echo "  $fcount files in $FINAL_DIR"
else
    echo "  FINAL_DIR not set or not found: ${FINAL_DIR:-unset}"
fi
sep
