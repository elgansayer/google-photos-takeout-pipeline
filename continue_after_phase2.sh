#!/bin/bash
# Wait for Phase 2 to finish, then continue pipeline from fix_dates + phase 3

PIPELINE_DIR="${PIPELINE_DIR}"
LOGFILE="$PIPELINE_DIR/orchestrator.log"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOGFILE"; }

# Poll until Phase 2 process is gone
PHASE2_PID=$(pgrep -f "pipeline.py --step 2" | head -1)
if [[ -n "$PHASE2_PID" ]]; then
    log "Polling for Phase 2 (PID $PHASE2_PID) to complete..."
    while kill -0 "$PHASE2_PID" 2>/dev/null; do
        sleep 30
    done
    log "Phase 2 process ended, checking result..."
    MERGED=$(python3 -c "
import sqlite3
conn = sqlite3.connect('$PIPELINE_DIR/photos.db', timeout=30)
n = conn.execute('SELECT COUNT(*) FROM photos WHERE json_merged=1').fetchone()[0]
print(n)
conn.close()
" 2>/dev/null)
    log "Phase 2 result: $MERGED rows merged"
    if [[ "${MERGED:-0}" -lt 5000 ]]; then
        log "FATAL: Phase 2 looks incomplete (only $MERGED merged)"
        exit 1
    fi
fi

# Run fix_dates (Phase 2.5)
log ""; log "══════════════════════════════════════════════"
log "PHASE 2.5: Fix wrong/missing dates"
log "══════════════════════════════════════════════"
cd "$PIPELINE_DIR"
t0=$(date +%s)
python3 fix_dates.py 2>&1 | tee -a "$LOGFILE"
if [[ ${PIPESTATUS[0]} -ne 0 ]]; then
    log "FATAL: fix_dates.py failed"
    exit 1
fi
log "Phase 2.5 complete in $(( $(date +%s) - t0 ))s"

# Continue from Phase 3
log ""; log "══════════════════════════════════════════════"
log "Continuing pipeline from Phase 3..."
log "══════════════════════════════════════════════"
bash "$PIPELINE_DIR/run_pipeline.sh" --from 3
