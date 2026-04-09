#!/usr/bin/env bash
# =============================================================================
# progress.sh — Photo Pipeline Progress Dashboard (Enhanced)
# =============================================================================
# Shows current status with ETAs and progress bars.
# Run anytime: bash progress.sh
# Watch live:  watch -n 30 bash progress.sh
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
DB="$PIPELINE_DIR/pipeline_v2.db"
FINAL_DIR="${FINAL_DIR}"
OLD_FINAL="${OLD_FINAL_DIR}"
AI_LOG="$PIPELINE_DIR/ai_classify.log"
PID_FILE="$PIPELINE_DIR/ai_classify.pid"

# Colors
G='\033[0;32m'  # green
Y='\033[0;33m'  # yellow
R='\033[0;31m'  # red
B='\033[0;34m'  # blue
C='\033[0;36m'  # cyan
W='\033[1;37m'  # white bold
N='\033[0m'     # reset

bar() {
    local val=$1 max=$2 width=${3:-30}
    local filled=$(( val * width / (max > 0 ? max : 1) ))
    local empty=$(( width - filled ))
    local result=""
    local i
    for (( i=0; i<filled; i++ )); do result+="█"; done
    for (( i=0; i<empty; i++ )); do result+="░"; done
    printf '%b%s%b' "${G}" "${result}" "${N}"
}

pct() {
    local val=$1 max=$2
    [[ "${max:-0}" -gt 0 ]] && echo "$(( val * 100 / max ))%" || echo "0%"
}

# Read DB stats
if [[ ! -f "$DB" ]]; then
    echo "ERROR: DB not found at $DB"
    exit 1
fi

read TOTAL DUPES AI_DONE IN_ALBUMS GEOCODED JSON_MERGED < <(sqlite3 "$DB" "
SELECT
    COUNT(*),
    SUM(CASE WHEN is_duplicate=1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN ai_processed=1 AND is_duplicate=0 THEN 1 ELSE 0 END),
    SUM(CASE WHEN album_id IS NOT NULL AND is_duplicate=0 THEN 1 ELSE 0 END),
    SUM(CASE WHEN country IS NOT NULL AND is_duplicate=0 THEN 1 ELSE 0 END),
    SUM(CASE WHEN json_merged=1 THEN 1 ELSE 0 END)
FROM photos;" 2>/dev/null | tr '|' ' ')

UNIQUE=$(( TOTAL - DUPES ))
ALBUM_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM albums;" 2>/dev/null || echo 0)
AUTO_ALBUMS=$(sqlite3 "$DB" "SELECT COUNT(*) FROM albums WHERE source='auto';" 2>/dev/null || echo 0)
GOOGLE_ALBUMS=$(sqlite3 "$DB" "SELECT COUNT(*) FROM albums WHERE source IN ('google','existing');" 2>/dev/null || echo 0)
NAMING_PID=$(cat "$PIPELINE_DIR/name_events.pid" 2>/dev/null | head -1 || echo 0)
WATCHER_PID=$(pgrep -f "watch_ai_and_continue.sh" 2>/dev/null | head -1 || echo 0)

# Total images for AI (images only, non-duplicates)
TOTAL_IMGS=$(sqlite3 "$DB" "SELECT COUNT(*) FROM photos WHERE is_duplicate=0 AND media_type='image';" 2>/dev/null || echo 0)
AI_LEFT=$(( TOTAL_IMGS - AI_DONE ))

# Output dir counts
FINAL_FILES=$(find "$FINAL_DIR" -type f -o -type l 2>/dev/null | wc -l)
OLD_FILES=$(find "$OLD_FINAL" \( -type f -o -type l \) 2>/dev/null | wc -l)

# AI process status
AI_PID=$(cat "$PID_FILE" 2>/dev/null || echo 0)
AI_STATUS="${R}stopped${N}"
if [[ "${AI_PID:-0}" -gt 0 ]] && kill -0 "$AI_PID" 2>/dev/null; then
    AI_STATUS="${G}running (PID $AI_PID)${N}"
fi

# AI ETA (from log - format: "AI: 170/93729")
AI_ETA="unknown"
if [[ -f "$AI_LOG" ]]; then
    # Get first and last counts from recent section of log
    RECENT_NUMS=$(grep -oP 'AI: \K\d+(?=/\d)' "$AI_LOG" 2>/dev/null | tail -20)
    FIRST_NUM=$(echo "$RECENT_NUMS" | head -1)
    LAST_NUM=$(echo "$RECENT_NUMS" | tail -1)
    # Get total from log (denominator in AI: X/Y)
    TOTAL_IN_LOG=$(grep -oP 'AI: \d+/\K\d+' "$AI_LOG" 2>/dev/null | tail -1)
    if [[ -n "$LAST_NUM" && -n "$FIRST_NUM" && "$LAST_NUM" != "$FIRST_NUM" && "${TOTAL_IN_LOG:-0}" -gt 0 ]]; then
        # Approx: 10 photos per log line, each ~46s
        RATE_PER_HOUR=$(( (LAST_NUM - FIRST_NUM) * 3600 / (20 * 46) ))
        if [[ "${RATE_PER_HOUR:-0}" -gt 0 ]]; then
            REMAINING_IN_LOG=$(( TOTAL_IN_LOG - LAST_NUM ))
            ETA_HOURS=$(( REMAINING_IN_LOG / RATE_PER_HOUR ))
            ETA_DAYS=$(( ETA_HOURS / 24 ))
            ETA_REM=$(( ETA_HOURS % 24 ))
            AI_ETA="${ETA_DAYS}d ${ETA_REM}h (~${RATE_PER_HOUR}/hr)"
        fi
    fi
    LAST_AI_LOG=$(tail -1 "$AI_LOG" 2>/dev/null | cut -c1-38)
fi

# Disk space
EVO_FREE=$(df -BG ${EVO_MOUNT} 2>/dev/null | awk 'NR==2{gsub(/G/,"",$4); print $4"GB free, "int($5)"%"}')
IMMICH_FREE=$(df -BG ${IMMICH_MOUNT} 2>/dev/null | awk 'NR==2{gsub(/G/,"",$4); print $4"GB free, "int($5)"%"}')
ROOT_FREE=$(df -BG / 2>/dev/null | awk 'NR==2{gsub(/G/,"",$4); print $4"GB free, "int($5)"%"}')

clear 2>/dev/null || true

echo ""
echo -e "${W}╔══════════════════════════════════════════════════════════════╗${N}"
echo -e "${W}║           PHOTO PIPELINE — PROGRESS DASHBOARD               ║${N}"
echo -e "${W}║           $(date '+%Y-%m-%d %H:%M:%S')                        ║${N}"
echo -e "${W}╚══════════════════════════════════════════════════════════════╝${N}"
echo ""

echo -e "${C}── DISK SPACE ─────────────────────────────────────────────────${N}"
printf "  %-12s %s\n" "evo:"   "${EVO_FREE:-unknown}"
printf "  %-12s %s\n" "immich:" "${IMMICH_FREE:-unknown}"
printf "  %-12s %s\n" "root:" "${ROOT_FREE:-unknown}"
echo ""

echo -e "${C}── DATABASE ───────────────────────────────────────────────────${N}"
printf "  Total catalogued: %s\n" "${TOTAL:?}"
printf "  Unique photos:    %-8s  (%s duplicates removed)\n" "${UNIQUE}" "${DUPES}"
printf "  JSON merged:      %-8s  " "${JSON_MERGED}"; bar ${JSON_MERGED:-0} ${TOTAL} 25; echo " $(pct ${JSON_MERGED:-0} ${TOTAL})"
printf "  Geocoded:         %-8s  " "${GEOCODED}"; bar ${GEOCODED:-0} ${UNIQUE} 25; echo " $(pct ${GEOCODED:-0} ${UNIQUE})"
printf "  In albums:        %-8s  " "${IN_ALBUMS}"; bar ${IN_ALBUMS:-0} ${UNIQUE} 25; echo " $(pct ${IN_ALBUMS:-0} ${UNIQUE})"
echo ""

echo -e "${C}── AI CLASSIFY (Phase 6) ──────────────────────────────────────${N}"
printf "  Status:     %b\n" "${AI_STATUS}"
printf "  Progress:   %-8s / %-8s  " "${AI_DONE}" "${TOTAL_IMGS}"; bar ${AI_DONE:-0} ${TOTAL_IMGS} 25; echo " $(pct ${AI_DONE:-0} ${TOTAL_IMGS})"
printf "  Remaining:  %-8s images\n" "${AI_LEFT}"
printf "  ETA:        %s\n" "${AI_ETA}"
[[ -n "${LAST_AI_LOG:-}" ]] && printf "  Last log:   %s\n" "$LAST_AI_LOG"
echo ""

echo -e "${C}── ALBUMS ─────────────────────────────────────────────────────${N}"
printf "  Total albums:       %s\n" "${ALBUM_COUNT}"
printf "  Auto-generated:     %s\n" "${AUTO_ALBUMS}"
printf "  From Google Photos: %s\n" "${GOOGLE_ALBUMS}"
echo ""

echo -e "${C}── OUTPUT DIRECTORIES ─────────────────────────────────────────${N}"
printf "  FINAL (immich):     %-8s files  %s\n" "${FINAL_FILES}" "${FINAL_DIR}"
printf "  OLD (evo):          %-8s files  %s\n" "${OLD_FILES}" "${OLD_FINAL}"
echo ""

echo -e "${C}── PHASE STATUS ───────────────────────────────────────────────${N}"

check_phase() {
    local name="$1" log_file="$2" done_str="$3" run_str="${4:-running}"
    if grep -q "$done_str" "$log_file" 2>/dev/null; then
        echo -e "  ${G}✓${N} $name"
    elif [[ -s "$log_file" ]]; then
        echo -e "  ${Y}~${N} $name (${run_str})"
    else
        echo -e "  ${R}·${N} $name"
    fi
}

check_phase "Phase 1    Audit both dirs"       "$PIPELINE_DIR/pipeline_v2.log" "Phase 1\|Catalogued"
check_phase "Phase 2    Merge JSON sidecars"   "$PIPELINE_DIR/pipeline_v2.log" "Phase 2 complete"
check_phase "Phase 2.5  Fix dates"             "$PIPELINE_DIR/orchestrator.log" "Phase 2.5 complete"
check_phase "Phase 2.5b Neighbor date fix"     "$PIPELINE_DIR/neighbor_date_fix.log" "Total dates fixed"
check_phase "Phase 3    Deduplicate"           "$PIPELINE_DIR/pipeline_v2.log" "Phase 3 complete"
check_phase "Phase 4    Fix dir names"         "$PIPELINE_DIR/pipeline_v2.log" "Phase 4 complete"
check_phase "Phase 5    Geocode"               "$PIPELINE_DIR/pipeline_v2.log" "Phase 5 complete"
if [[ "${AI_DONE:-0}" -ge "${TOTAL_IMGS:-1}" ]]; then
    echo -e "  ${G}✓${N} Phase 6    AI classify (${AI_DONE}/${TOTAL_IMGS})"
else
    echo -e "  ${Y}~${N} Phase 6    AI classify (${AI_DONE}/${TOTAL_IMGS} = $(pct ${AI_DONE:-0} ${TOTAL_IMGS}))"
fi
check_phase "Phase 7    Group event albums"    "$PIPELINE_DIR/phase7.log"      "Phase 7 complete"
if kill -0 "${NAMING_PID:-0}" 2>/dev/null; then
    RENAMED_SO_FAR=$(grep -c "→" "$PIPELINE_DIR/name_events.log" 2>/dev/null || echo "?")
    echo -e "  ${Y}~${N} Phase 7.5  AI event naming (${RENAMED_SO_FAR} renamed so far...)"
else
    check_phase "Phase 7.5  AI event naming"   "$PIPELINE_DIR/name_events.log" "Renamed.*albums"
fi
[[ "${FINAL_FILES:-0}" -gt 0 ]] && \
    echo -e "  ${G}✓${N} Phase 8    Organize to immich (${FINAL_FILES} files)" || \
    check_phase "Phase 8    Organize to immich" "$PIPELINE_DIR/phase8.log"    "Phase 8 complete"
check_phase "Phase 9    Upload prep"           "$PIPELINE_DIR/phase9.log"      "Phase 9\|Upload"
echo ""

echo -e "${C}── BACKGROUND PROCESSES ───────────────────────────────────────${N}"
if [[ "${AI_PID:-0}" -gt 0 ]] && kill -0 "$AI_PID" 2>/dev/null; then
    echo -e "  ${G}●${N} AI classify running  (PID $AI_PID)"
else
    echo -e "  ${R}○${N} AI classify stopped"
fi
if [[ "${WATCHER_PID:-0}" -gt 0 ]] && kill -0 "$WATCHER_PID" 2>/dev/null; then
    echo -e "  ${G}●${N} Auto-watcher running (PID $WATCHER_PID) — will continue after AI finishes"
else
    echo -e "  ${Y}○${N} Auto-watcher not running"
    echo -e "      Start: nohup bash $PIPELINE_DIR/watch_ai_and_continue.sh &"
fi
echo ""

echo -e "${C}── NEXT STEPS ─────────────────────────────────────────────────${N}"
if [[ "${FINAL_FILES:-0}" -eq 0 ]]; then
    echo -e "  ${Y}►${N} Run pipeline:  bash master_pipeline.sh"
elif [[ "${AI_LEFT:-0}" -gt 0 ]]; then
    echo -e "  ${G}►${N} AI still running — watcher will auto-continue when done"
    echo -e "  ${G}►${N} Upload to Google Photos (after AI finishes):"
    echo -e "    bash $PIPELINE_DIR/upload_to_gphotos.sh"
else
    echo -e "  ${G}►${N} All done! Upload:"
    echo -e "    bash $PIPELINE_DIR/upload_to_gphotos.sh"
fi
echo ""
