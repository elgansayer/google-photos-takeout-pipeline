#!/bin/bash
# ============================================================
# Master Photo Pipeline Orchestrator v2
# ============================================================
# Fully sequential, error-safe. Each phase must complete
# successfully before the next starts.
#
# Usage:
#   bash run_pipeline.sh              # Run all phases
#   bash run_pipeline.sh --from 3     # Resume from phase 3
#   bash run_pipeline.sh --dry-run    # Preview without changes
# ============================================================

set -uo pipefail

PIPELINE_DIR="${PIPELINE_DIR}"
LOGFILE="$PIPELINE_DIR/orchestrator.log"
PYTHON3="python3"
DRY_RUN=""
FROM_PHASE="1"

while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run) DRY_RUN="--dry-run"; shift;;
        --from)    FROM_PHASE="$2"; shift 2;;
        *) echo "Unknown: $1"; exit 1;;
    esac
done

mkdir -p "$PIPELINE_DIR"
cd "$PIPELINE_DIR"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOGFILE"; }
die() { log "FATAL: $*"; exit 1; }

should_run() {
    python3 -c "exit(0 if float('$1') >= float('$FROM_PHASE') else 1)" 2>/dev/null
}

run_phase() {
    local num="$1" name="$2"; shift 2
    should_run "$num" || { log "Skipping phase $num ($name)"; return 0; }
    log ""; log "══════════════════════════════════════════════"
    log "PHASE $num: $name"; log "══════════════════════════════════════════════"
    local t0=$(date +%s)
    # Run with pipefail-safe logging
    if "$@" 2>&1 | tee -a "$LOGFILE"; ret="${PIPESTATUS[0]}"; [ "${ret}" -eq 0 ]; then
        log "✓ Phase $num done in $(( $(date +%s) - t0 ))s"
    else
        die "Phase $num failed (exit $ret). Fix the issue and re-run with --from $num"
    fi
}

log "Pipeline started. FROM=${FROM_PHASE} DRY_RUN=${DRY_RUN:-none}"

# Phase 1
run_phase 1 "Unified audit of both directories" \
    $PYTHON3 pipeline.py --step 1 $DRY_RUN

# Phase 2 - merge JSON sidecars
run_phase 2 "Merge JSON sidecars into EXIF" \
    $PYTHON3 pipeline.py --step 2 $DRY_RUN

# Phase 2.5 - fix bad dates
run_phase 2.5 "Fix wrong/missing dates" \
    $PYTHON3 fix_dates.py $DRY_RUN

# Phase 3 - deduplicate
run_phase 3 "Deduplicate" \
    $PYTHON3 pipeline.py --step 3 $DRY_RUN

# Phase 4 - fix broken dir names
run_phase 4 "Fix broken directory names" \
    $PYTHON3 pipeline.py --step 4 $DRY_RUN

# Phase 5 - geocode
run_phase 5 "Reverse geocode (offline GeoNames)" \
    $PYTHON3 pipeline.py --step 5 $DRY_RUN

# Phase 6 - AI classification in background, don't block
if should_run 6 && [[ -z "$DRY_RUN" ]]; then
    log ""; log "══════════════════════════════════════════════"
    log "PHASE 6: AI Classification (background, ~80h)"
    log "══════════════════════════════════════════════"
    $PYTHON3 pipeline.py --step 6 >> "$PIPELINE_DIR/ai_classify.log" 2>&1 &
    AI_PID=$!
    log "AI started PID=$AI_PID. Monitor: tail -f $PIPELINE_DIR/ai_classify.log"
    log "Waiting 5 min before clustering..."
    sleep 300
fi

# Phase 7 - cluster into albums
run_phase 7 "Auto-group into holiday/event albums" \
    $PYTHON3 pipeline.py --step 7 $DRY_RUN

# Phase 7.5 - AI event naming
run_phase 7.5 "Rename albums to event names (AI)" \
    $PYTHON3 name_events.py $DRY_RUN

# Phase 8 - organize
run_phase 8 "Organize into final directory" \
    $PYTHON3 pipeline.py --step 8 $DRY_RUN

# Phase 9 - upload prep
run_phase 9 "Upload prep + Google Photos scripts" \
    $PYTHON3 pipeline.py --step 9 $DRY_RUN

run_phase 9 "Generate Google Photos rclone scripts" \
    $PYTHON3 google_photos_upload.py --generate-scripts

log ""
log "══════════════════════════════════════════════"
log "ALL PHASES COMPLETE"
log "══════════════════════════════════════════════"
log "Output:         ${OLD_FINAL_DIR}/"
log "Upload script:  $PIPELINE_DIR/upload_to_gphotos.sh"
log "Summary:        $PIPELINE_DIR/UPLOAD_SUMMARY.md"
log ""
log "To upload to Google Photos:"
log "  1. curl https://rclone.org/install.sh | sudo bash"
log "  2. rclone config   # select 'Google Photos', name it 'gphotos'"
log "  3. bash $PIPELINE_DIR/upload_to_gphotos.sh"
log ""
log "Re-run albums after AI finishes:"
log "  bash run_pipeline.sh --from 7"
