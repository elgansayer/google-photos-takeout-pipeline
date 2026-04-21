#!/usr/bin/env bash
# dashboard.sh — Full pipeline status dashboard.
#
# Usage:
#   bash dashboard.sh            # one-shot
#   watch -n 30 bash dashboard.sh  # live (refreshes every 30s)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/.env" ]; then set -a; source "$SCRIPT_DIR/.env"; set +a; fi

PIPELINE_DIR="${PIPELINE_DIR:-$SCRIPT_DIR}"
FINAL_DIR="${FINAL_DIR:-}"
EVO_MOUNT="${EVO_MOUNT:-/run/media/elgan/evo}"
IMMICH_MOUNT="${IMMICH_MOUNT:-/run/media/elgan/immich}"
DB="$PIPELINE_DIR/photos.db"
UPLOAD_DIR="${EVO_MOUNT}/Pictures/photo_pipeline"

# ── Colours ────────────────────────────────────────────────────────────────
G='\033[0;32m'; Y='\033[0;33m'; R='\033[0;31m'; C='\033[0;36m'
W='\033[1;37m'; D='\033[2m'; N='\033[0m'

ok()   { printf "  ${G}✓${N}  %s\n" "$*"; }
run()  { printf "  ${Y}⟳${N}  %s\n" "$*"; }
err()  { printf "  ${R}✗${N}  %s\n" "$*"; }
idle() { printf "  ${D}·${N}  %s\n" "$*"; }
hdr()  { printf "\n${C}── %s %s${N}\n" "$1" "$(printf '%.0s─' {1..50})" | cut -c1-70; }

pid_running() { [ "${1:-0}" -gt 0 ] && kill -0 "$1" 2>/dev/null; }
service_running() { systemctl --user is-active --quiet "$1" 2>/dev/null; }

bar() {
    local val=$1 max=${2:-1} width=${3:-20}
    [ "$max" -le 0 ] && max=1
    local filled=$(( val * width / max ))
    [ "$filled" -gt "$width" ] && filled=$width
    local empty=$(( width - filled ))
    printf "${G}"
    printf '█%.0s' $(seq 1 $filled 2>/dev/null) 2>/dev/null || printf '%*s' "$filled" | tr ' ' '█'
    printf "${D}"
    printf '░%.0s' $(seq 1 $empty 2>/dev/null) 2>/dev/null || printf '%*s' "$empty" | tr ' ' '░'
    printf "${N}"
}

pct() { local v=$1 m=${2:-1}; [ "$m" -le 0 ] && m=1; echo "$(( v * 100 / m ))%"; }

clear 2>/dev/null || true

# ── Header ─────────────────────────────────────────────────────────────────
printf "\n${W}╔══════════════════════════════════════════════════════════════╗${N}\n"
printf   "${W}║        PHOTO PIPELINE DASHBOARD  %-26s║${N}\n" "$(date '+%Y-%m-%d %H:%M:%S')"
printf   "${W}╚══════════════════════════════════════════════════════════════╝${N}\n"

# ── Disk space ─────────────────────────────────────────────────────────────
hdr "DISK SPACE"
df -h "$EVO_MOUNT" "$IMMICH_MOUNT" / 2>/dev/null \
    | awk 'NR>1 {printf "  %-38s %5s used  %5s avail  %s\n", $6, $3, $4, $5}'

# ── Database ───────────────────────────────────────────────────────────────
hdr "DATABASE"
if [ -f "$DB" ]; then
    eval "$(sqlite3 "$DB" "
        SELECT
            COUNT(*),
            SUM(CASE WHEN is_duplicate=1 THEN 1 ELSE 0 END),
            SUM(CASE WHEN ai_processed=1 AND is_duplicate=0 THEN 1 ELSE 0 END),
            SUM(CASE WHEN ai_processed=-1 AND is_duplicate=0 THEN 1 ELSE 0 END),
            SUM(CASE WHEN album_id IS NOT NULL AND is_duplicate=0 THEN 1 ELSE 0 END),
            SUM(CASE WHEN country IS NOT NULL AND is_duplicate=0 THEN 1 ELSE 0 END),
            SUM(CASE WHEN latitude IS NOT NULL AND is_duplicate=0 THEN 1 ELSE 0 END),
            (SELECT COUNT(*) FROM albums WHERE source='auto')
        FROM photos;" 2>/dev/null \
    | awk -F'|' '{
        printf "TOTAL=%s DUPES=%s AI_DONE=%s AI_SKIP=%s IN_ALB=%s GEO=%s GPS=%s AUTO_ALB=%s",
               $1,$2,$3,$4,$5,$6,$7,$8}')"
    UNIQUE=$(( ${TOTAL:-0} - ${DUPES:-0} ))
    TOTAL_IMG=$(sqlite3 "$DB" "SELECT COUNT(*) FROM photos WHERE is_duplicate=0 AND media_type='image';" 2>/dev/null || echo 0)
    printf "  Total: %'.0f files, %'.0f unique (%'.0f duplicates removed)\n" "${TOTAL:-0}" "$UNIQUE" "${DUPES:-0}"
    printf "  AI classified:  %'.0f / %'.0f  " "${AI_DONE:-0}" "$TOTAL_IMG"
    bar "${AI_DONE:-0}" "$TOTAL_IMG"; echo "  $(pct "${AI_DONE:-0}" "$TOTAL_IMG")"
    printf "  Geocoded:       %'.0f / %'.0f  " "${GEO:-0}" "$UNIQUE"
    bar "${GEO:-0}" "$UNIQUE"; echo "  $(pct "${GEO:-0}" "$UNIQUE")"
    printf "  Has GPS:        %'.0f / %'.0f  " "${GPS:-0}" "$UNIQUE"
    bar "${GPS:-0}" "$UNIQUE"; echo "  $(pct "${GPS:-0}" "$UNIQUE")"
    printf "  In albums:      %'.0f / %'.0f  " "${IN_ALB:-0}" "$UNIQUE"
    bar "${IN_ALB:-0}" "$UNIQUE"; echo "  $(pct "${IN_ALB:-0}" "$UNIQUE")"
    printf "  Auto albums:    %'.0f\n" "${AUTO_ALB:-0}"
else
    err "DB not found: $DB"
fi

# ── Google Photos Upload ────────────────────────────────────────────────────
hdr "GOOGLE PHOTOS UPLOAD"
if service_running "photo-upload"; then
    SVC_LINE=$(systemctl --user status photo-upload.service 2>/dev/null \
        | grep "Active:" | sed 's/^[[:space:]]*//')
    run "photo-upload.service  —  $SVC_LINE"
else
    err "photo-upload.service  NOT RUNNING"
    echo "      Start:  systemctl --user start photo-upload"
fi

STATE_FILE="$UPLOAD_DIR/upload_state.json"
if [ -f "$STATE_FILE" ]; then
    python3 - "$STATE_FILE" <<'PYEOF'
import json, sys
from pathlib import Path
d = json.loads(Path(sys.argv[1]).read_text())
albums = d.get('albums', {})
done    = sum(1 for v in albums.values() if v.get('status') == 'done')
failed  = sum(1 for v in albums.values() if v.get('status') == 'failed')
total   = len(albums)
pending = total - done - failed
pct = done * 100 // total if total else 0
bar_w = 30
filled = done * bar_w // total if total else 0
bar = '\033[0;32m' + '█' * filled + '\033[2m' + '░' * (bar_w - filled) + '\033[0m'
print(f"  Progress:  {done}/{total} albums  {bar}  {pct}%")
if failed:
    print(f"  \033[0;31mFailed:\033[0m  {failed} albums")
if pending:
    print(f"  Pending:   {pending} albums remaining")
# Current album from rclone log
PYEOF
    # Show current rclone activity
    RCLONE_PID=$(pgrep -f "rclone.*google-photos" 2>/dev/null | head -1)
    if pid_running "$RCLONE_PID"; then
        CURRENT=$(ps -p "$RCLONE_PID" -o args= 2>/dev/null \
            | grep -oP 'final-google-photos/\K[^"]+(?=")' | head -1 || echo "")
        [ -n "$CURRENT" ] && echo "  Uploading: $CURRENT"
    fi
    QUOTA_ERR=$(tail -50 "$UPLOAD_DIR/rclone_upload.log" 2>/dev/null \
        | grep -c "Quota exceeded" || echo 0)
    [ "$QUOTA_ERR" -gt 0 ] && printf "  ${Y}⚠${N}  Daily quota exceeded — service sleeping until 08:30 UK time\n"
else
    idle "No upload state file yet"
fi

# ── Instagram pipeline ─────────────────────────────────────────────────────
hdr "INSTAGRAM PIPELINE"
IG_PID=$(cat "$SCRIPT_DIR/instagram_pipeline.pid" 2>/dev/null || echo 0)
if pid_running "$IG_PID"; then
    SCORED=$(sqlite3 "$SCRIPT_DIR/instagram.db" "SELECT COUNT(*) FROM scores;" 2>/dev/null || echo "?")
    TOTAL_IG=$(sqlite3 "$SCRIPT_DIR/instagram.db" "SELECT COUNT(DISTINCT photo_path) FROM scores UNION SELECT COUNT(*) FROM (SELECT DISTINCT album_name FROM scores) LIMIT 1;" 2>/dev/null || echo "?")
    CURATED=$(sqlite3 "$SCRIPT_DIR/instagram.db" "SELECT COUNT(DISTINCT album_name) FROM curated;" 2>/dev/null || echo 0)
    READY=$(find "$SCRIPT_DIR/instagram_ready" -name "caption.txt" 2>/dev/null | wc -l)
    LAST=$(tail -3 "$SCRIPT_DIR/instagram_run.log" 2>/dev/null | grep -v "^$" | tail -1)
    run "Running (PID $IG_PID)"
    echo "  Scored: $SCORED photos  |  $CURATED albums curated  |  $READY ready to upload"
    [ -n "$LAST" ] && echo "  Last:   $LAST"
elif service_running "photo-instagram"; then
    run "photo-instagram.service running (restarting?)"
else
    READY=$(find "$SCRIPT_DIR/instagram_ready" -name "caption.txt" 2>/dev/null | wc -l)
    CURATED=$(sqlite3 "$SCRIPT_DIR/instagram.db" "SELECT COUNT(DISTINCT album_name) FROM curated;" 2>/dev/null || echo 0)
    if [ "${READY:-0}" -gt 0 ]; then
        ok "Done — $READY albums ready, $CURATED curated"
        echo "  Upload: bash $SCRIPT_DIR/run_instagram.sh --step export"
    else
        idle "Not running — start: bash run_instagram.sh --step all"
    fi
fi

# ── Guess locations ────────────────────────────────────────────────────────
hdr "GUESS LOCATIONS (GPS INFERENCE)"
GL_PID=$(cat "$SCRIPT_DIR/guess_locations.pid" 2>/dev/null || echo 0)
if pid_running "$GL_PID"; then
    PROGRESS=$(tail -5 "$SCRIPT_DIR/guess_locations_run.log" 2>/dev/null | grep -oP 'Indexed \d+/\d+' | tail -1)
    run "Running (PID $GL_PID)  $PROGRESS"
elif pgrep -f "guess_locations.py" >/dev/null 2>&1; then
    run "Running (no PID file)"
else
    if [ -f "$SCRIPT_DIR/guess_locations_run.log" ]; then
        DONE_LINE=$(grep -c "Updated\|Inferred" "$SCRIPT_DIR/guess_locations_run.log" 2>/dev/null || echo 0)
        ok "Completed  ($DONE_LINE locations inferred)"
    else
        idle "Not run yet — start: bash run_guess_locations.sh"
    fi
fi

# ── AI Classify ────────────────────────────────────────────────────────────
hdr "AI CLASSIFY (PHASE 6)"
AI_PID=$(cat "$PIPELINE_DIR/ai_classify.pid" 2>/dev/null || echo 0)
if pid_running "$AI_PID"; then
    AI_DONE=$(sqlite3 "$DB" "SELECT COUNT(*) FROM photos WHERE ai_processed=1 AND is_duplicate=0 AND media_type='image';" 2>/dev/null || echo 0)
    AI_TOTAL=$(sqlite3 "$DB" "SELECT COUNT(*) FROM photos WHERE is_duplicate=0 AND media_type='image';" 2>/dev/null || echo 1)
    LAST_LOG=$(tail -1 "$PIPELINE_DIR/ai_classify.log" 2>/dev/null | cut -c1-60)
    run "Running (PID $AI_PID)  —  $AI_DONE/$AI_TOTAL  $(pct "$AI_DONE" "$AI_TOTAL")"
    [ -n "$LAST_LOG" ] && echo "  Last:  $LAST_LOG"
elif [ -f "$DB" ]; then
    AI_DONE=$(sqlite3 "$DB" "SELECT COUNT(*) FROM photos WHERE ai_processed=1 AND is_duplicate=0 AND media_type='image';" 2>/dev/null || echo 0)
    AI_SKIP=$(sqlite3 "$DB" "SELECT COUNT(*) FROM photos WHERE ai_processed=-1 AND is_duplicate=0 AND media_type='image';" 2>/dev/null || echo 0)
    AI_TOTAL=$(sqlite3 "$DB" "SELECT COUNT(*) FROM photos WHERE is_duplicate=0 AND media_type='image';" 2>/dev/null || echo 1)
    if [ "$(( AI_DONE + AI_SKIP ))" -ge "$AI_TOTAL" ]; then
        ok "Complete — $AI_DONE classified, $AI_SKIP unreachable"
    else
        REMAINING=$(( AI_TOTAL - AI_DONE - AI_SKIP ))
        idle "Stopped — $AI_DONE/$AI_TOTAL done, $REMAINING remaining"
        echo "  Resume: nohup python3 $PIPELINE_DIR/pipeline.py --step classify >> $PIPELINE_DIR/ai_classify.log 2>&1 &"
    fi
fi

# ── Name events ────────────────────────────────────────────────────────────
hdr "AI EVENT NAMING (PHASE 7.5)"
NE_PID=$(cat "$PIPELINE_DIR/name_events.pid" 2>/dev/null || echo 0)
if pid_running "$NE_PID"; then
    RENAMED=$(grep -c "→" "$PIPELINE_DIR/name_events.log" 2>/dev/null || echo 0)
    run "Running (PID $NE_PID)  —  $RENAMED albums renamed so far"
elif pgrep -f "name_events.py" >/dev/null 2>&1; then
    run "Running (no PID file)"
else
    if [ -f "$PIPELINE_DIR/name_events.log" ]; then
        RENAMED=$(grep -c "→" "$PIPELINE_DIR/name_events.log" 2>/dev/null || echo 0)
        ok "Completed — $RENAMED albums renamed"
    else
        idle "Not run yet"
    fi
fi

# ── Watch-AI watcher ───────────────────────────────────────────────────────
hdr "BACKGROUND WATCHER"
WATCHER_PID=$(pgrep -f "watch_ai_and_continue.sh" 2>/dev/null | head -1 || echo 0)
if pid_running "$WATCHER_PID"; then
    ok "watch_ai_and_continue.sh running (PID $WATCHER_PID)"
elif service_running "photo-watcher"; then
    ok "photo-watcher.service running"
else
    idle "Watcher not running"
    echo "  Start: nohup bash $SCRIPT_DIR/watch_ai_and_continue.sh >> $PIPELINE_DIR/orchestrator.log 2>&1 &"
fi

# ── Systemd services ───────────────────────────────────────────────────────
hdr "SYSTEMD SERVICES (auto-resume on reboot)"
for svc in photo-upload photo-instagram photo-watcher; do
    if systemctl --user list-unit-files "$svc.service" 2>/dev/null | grep -q "$svc"; then
        STATE=$(systemctl --user is-active "$svc.service" 2>/dev/null)
        ENABLED=$(systemctl --user is-enabled "$svc.service" 2>/dev/null)
        case "$STATE" in
            active)   ok "$svc.service  (active, $ENABLED)" ;;
            inactive) idle "$svc.service  (inactive, $ENABLED)" ;;
            failed)   err "$svc.service  (FAILED, $ENABLED)" ;;
            *)        idle "$svc.service  ($STATE, $ENABLED)" ;;
        esac
    else
        err "$svc.service  NOT INSTALLED"
        echo "      Install: bash $SCRIPT_DIR/install_services.sh"
    fi
done

# ── Output ─────────────────────────────────────────────────────────────────
hdr "OUTPUT"
if [ -n "$FINAL_DIR" ] && [ -d "$FINAL_DIR" ]; then
    FCOUNT=$(find "$FINAL_DIR" \( -type f -o -type l \) 2>/dev/null | wc -l)
    DCOUNT=$(find "$FINAL_DIR" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | wc -l)
    ok "$FCOUNT files in $DCOUNT album folders"
    echo "  Path: $FINAL_DIR"
else
    err "FINAL_DIR not found: ${FINAL_DIR:-unset}"
fi
INSTA_READY=$(find "$SCRIPT_DIR/instagram_ready" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | wc -l)
[ "$INSTA_READY" -gt 0 ] && ok "$INSTA_READY Instagram albums ready in instagram_ready/"

# ── Recent log ─────────────────────────────────────────────────────────────
hdr "RECENT ACTIVITY"
tail -4 "$PIPELINE_DIR/orchestrator.log" 2>/dev/null | grep -v "^$" | sed 's/^/  /'

printf "\n${D}  Live:     tail -f $PIPELINE_DIR/orchestrator.log"
printf "\n  Upload:   journalctl --user -u photo-upload -f"
printf "\n  Resume:   bash $SCRIPT_DIR/run_pipeline.sh --from N"
printf "${N}\n\n"
