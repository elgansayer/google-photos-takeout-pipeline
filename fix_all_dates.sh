#!/usr/bin/env bash
# fix_all_dates.sh — Fix wrong/missing dates using sidecars, EXIF, and neighbour inference.
#
# Usage:
#   bash fix_all_dates.sh              # run both date-fixing passes
#   bash fix_all_dates.sh --dry-run    # preview, no changes
#   bash fix_all_dates.sh --use-ai     # also ask Ollama to estimate dates (slow)
#
# Resumable: already-fixed photos are skipped.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ -f "$SCRIPT_DIR/.env" ]; then
    set -a; source "$SCRIPT_DIR/.env"; set +a
fi

EVO_MOUNT="${EVO_MOUNT:-/run/media/elgan/evo}"
PIPELINE_DIR="${PIPELINE_DIR:-$SCRIPT_DIR}"
DB="$PIPELINE_DIR/photos.db"
LOG="$PIPELINE_DIR/fix_all_dates.log"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG"; }
err() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" | tee -a "$LOG" >&2; }

DRY_RUN="" USE_AI=""
for arg in "$@"; do
    case $arg in
        --dry-run) DRY_RUN="--dry-run" ;;
        --use-ai)  USE_AI="--use-ai"   ;;
    esac
done

if ! mountpoint -q "$EVO_MOUNT" 2>/dev/null && [ ! -e "$DB" ]; then
    err "Drive not mounted at $EVO_MOUNT and DB not found at $DB"
    exit 1
fi

bad_count() {
    sqlite3 "$DB" "SELECT COUNT(*) FROM photos WHERE
        strftime('%Y', best_date) IN ('1970','1904','1900','0001','9999')
        OR best_date IS NULL OR best_date = '';" 2>/dev/null || echo "?"
}

log "=== DATE FIXING PIPELINE ==="
[ -n "$DRY_RUN" ] && log "DRY RUN — no changes will be saved"
log "Bad/missing dates BEFORE: $(bad_count)"

log "Step 1: fix_dates.py (JSON sidecars + EXIF)"
python3 "$SCRIPT_DIR/fix_dates.py" $DRY_RUN $USE_AI 2>&1 | tee -a "$PIPELINE_DIR/fix_dates.log"

log "Step 2: neighbor_date_fix.py (sequence/burst inference)"
python3 "$SCRIPT_DIR/neighbor_date_fix.py" $DRY_RUN 2>&1 | tee -a "$PIPELINE_DIR/neighbor_date_fix.log"

log "Bad/missing dates AFTER: $(bad_count)"
log "Date fixing complete."
