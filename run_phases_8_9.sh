#!/usr/bin/env bash
# Runs after phase 7.5 finishes: organize to immich, upload prep
PIPELINE_DIR="${PIPELINE_DIR}"
FINAL_DIR="${FINAL_DIR}"
LOG="$PIPELINE_DIR/orchestrator.log"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG"; }

# Wait for phase 7.5 to finish
log "Waiting for phase 7.5 (AI naming) to complete..."
while kill -0 47596 2>/dev/null; do
    RENAMED=$(grep -c "→" "$PIPELINE_DIR/name_events.log" 2>/dev/null || echo 0)
    log "  Phase 7.5 in progress: $RENAMED albums renamed so far..."
    sleep 60
done
log "Phase 7.5 complete!"

# Clear old output dir
log "=== Phase 8: Organize to $FINAL_DIR ==="
if [[ -d "$FINAL_DIR" ]]; then
    log "Clearing existing $FINAL_DIR..."
    rm -rf "$FINAL_DIR"
fi
mkdir -p "$FINAL_DIR"

python3 "$PIPELINE_DIR/pipeline_v2.py" --phase 8 >> "$PIPELINE_DIR/phase8.log" 2>&1
log "Phase 8 complete!"

# Upload prep
log "=== Phase 9: Upload Prep ==="
python3 "$PIPELINE_DIR/pipeline_v2.py" --phase 9 >> "$PIPELINE_DIR/phase9.log" 2>&1 || true
python3 "$PIPELINE_DIR/google_photos_upload.py" >> "$PIPELINE_DIR/upload.log" 2>&1 || true
log "Phase 9 complete!"

FILE_COUNT=$(find "$FINAL_DIR" -type f -o -type l 2>/dev/null | wc -l)
log ""
log "============================================================"
log "PIPELINE COMPLETE: $FILE_COUNT files in $FINAL_DIR"
log "To upload: bash $PIPELINE_DIR/upload_to_gphotos.sh"
log "============================================================"
