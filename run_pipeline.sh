#!/usr/bin/env bash
# run_pipeline.sh — Main photo pipeline orchestrator (phases 1–9).
#
# Usage:
#   bash run_pipeline.sh                # run all phases from the beginning
#   bash run_pipeline.sh --from 3       # resume from phase 3
#   bash run_pipeline.sh --from 7       # re-cluster albums (post-AI)
#   bash run_pipeline.sh --dry-run      # preview without changes
#
# Resumable: pass --from N to skip already-completed phases.
#
# Step map:  1=scan  2=merge-sidecars  3=deduplicate  4=fix-dates
#            5=geocode  6=classify(AI)  7=group-albums
#            8=export   9=prep-upload

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ -f "$SCRIPT_DIR/.env" ]; then
    set -a; source "$SCRIPT_DIR/.env"; set +a
fi

PIPELINE_DIR="${PIPELINE_DIR:-$SCRIPT_DIR}"
FINAL_DIR="${FINAL_DIR:-}"
EVO_MOUNT="${EVO_MOUNT:-/run/media/elgan/evo}"
IMMICH_MOUNT="${IMMICH_MOUNT:-/run/media/elgan/immich}"

LOGFILE="$PIPELINE_DIR/orchestrator.log"
DRY_RUN=""
FROM_PHASE="1"

while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)  DRY_RUN="--dry-run"; shift ;;
        --from)     FROM_PHASE="$2";     shift 2 ;;
        --from=*)   FROM_PHASE="${1#--from=}"; shift ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

mkdir -p "$PIPELINE_DIR"

log()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOGFILE"; }
die()  { log "FATAL: $*"; exit 1; }

should_run() {
    python3 -c "exit(0 if float('$1') >= float('$FROM_PHASE') else 1)" 2>/dev/null
}

run_phase() {
    local num="$1" name="$2"; shift 2
    should_run "$num" || { log "Skipping phase $num ($name)"; return 0; }
    log ""; log "══════════════════════════════════════════════"
    log "PHASE $num: $name"; log "══════════════════════════════════════════════"
    local t0; t0=$(date +%s)
    if "$@" 2>&1 | tee -a "$LOGFILE"; then
        log "✓ Phase $num done in $(( $(date +%s) - t0 ))s"
    else
        die "Phase $num failed. Fix the issue and re-run: bash run_pipeline.sh --from $num"
    fi
}

log "Pipeline started — FROM=${FROM_PHASE} DRY_RUN=${DRY_RUN:-none}"

# Check prereqs
if ! mountpoint -q "$EVO_MOUNT" 2>/dev/null; then
    die "Evo drive not mounted at $EVO_MOUNT"
fi

run_phase 1   "Scan source directories"         python3 pipeline.py --step scan              $DRY_RUN
run_phase 2   "Merge Google JSON sidecars"       python3 pipeline.py --step merge-sidecars    $DRY_RUN
run_phase 2.5 "Fix wrong/missing dates"          bash fix_all_dates.sh                        $DRY_RUN
run_phase 3   "Deduplicate"                      python3 pipeline.py --step deduplicate        $DRY_RUN
run_phase 4   "Reverse geocode"                  python3 pipeline.py --step geocode           $DRY_RUN
run_phase 4.5 "Guess locations (GPS inference)"  bash run_guess_locations.sh                  $DRY_RUN

# Phase 6: AI classify runs in the background — don't block pipeline
if should_run 6 && [[ -z "$DRY_RUN" ]]; then
    log ""; log "══════════════════════════════════════════════"
    log "PHASE 6: AI Classification (background, hours)"
    log "══════════════════════════════════════════════"
    nohup python3 pipeline.py --step classify >> "$PIPELINE_DIR/ai_classify.log" 2>&1 &
    AI_PID=$!
    echo "$AI_PID" > "$PIPELINE_DIR/ai_classify.pid"
    log "AI classify started in background (PID $AI_PID)"
    log "Monitor: tail -f $PIPELINE_DIR/ai_classify.log"
fi

run_phase 7   "Group photos into event albums"   python3 pipeline.py --step group-albums      $DRY_RUN
run_phase 7.5 "AI event naming"                  python3 name_events.py                       $DRY_RUN
run_phase 8   "Export to final directory"        python3 pipeline.py --step export            $DRY_RUN
run_phase 9   "Generate upload scripts"          python3 pipeline.py --step prep-upload       $DRY_RUN

log ""
log "══════════════════════════════════════════════"
log "ALL PHASES COMPLETE"
log "══════════════════════════════════════════════"
log "Output:  ${FINAL_DIR}/"
log "Upload:  bash upload_to_gphotos.sh"
log "Instagram: bash run_instagram.sh"
