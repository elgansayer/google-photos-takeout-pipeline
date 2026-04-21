#!/usr/bin/env bash
# run_instagram.sh — Score, grade and export Instagram-ready albums.
#
# Usage:
#   bash run_instagram.sh                         # run all steps
#   bash run_instagram.sh --step discover         # see ranked album list
#   bash run_instagram.sh --step score            # just score photos
#   bash run_instagram.sh --step summary          # show readiness table
#   bash run_instagram.sh --album "Barcelona"     # one album only
#   bash run_instagram.sh --dry-run               # preview, no changes
#   bash run_instagram.sh --step all --dry-run    # preview full run
#
# Fully resumable: already-scored photos and existing exports are skipped.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ -f "$SCRIPT_DIR/.env" ]; then
    set -a; source "$SCRIPT_DIR/.env"; set +a
fi

# Prereq checks
if ! mountpoint -q "${IMMICH_MOUNT:-/run/media/elgan/immich}" 2>/dev/null; then
    echo "ERROR: Immich drive not mounted at ${IMMICH_MOUNT:-/run/media/elgan/immich}"
    exit 1
fi
if ! mountpoint -q "${EVO_MOUNT:-/run/media/elgan/evo}" 2>/dev/null; then
    echo "ERROR: Evo drive not mounted at ${EVO_MOUNT:-/run/media/elgan/evo}"
    exit 1
fi

python3 "$SCRIPT_DIR/instagram_pipeline.py" "$@" &
INST_PID=$!
echo "$INST_PID" > "$SCRIPT_DIR/instagram_pipeline.pid"
wait "$INST_PID"
EXIT=$?
rm -f "$SCRIPT_DIR/instagram_pipeline.pid"
exit $EXIT
