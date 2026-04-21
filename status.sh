#!/usr/bin/env bash
# status.sh — Photo pipeline status dashboard.
#
# Usage:  bash status.sh
#         watch -n 30 bash status.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/.env" ]; then set -a; source "$SCRIPT_DIR/.env"; set +a; fi

PIPELINE_DIR="${PIPELINE_DIR:-$SCRIPT_DIR}"
FINAL_DIR="${FINAL_DIR:-}"
EVO_MOUNT="${EVO_MOUNT:-/run/media/elgan/evo}"
IMMICH_MOUNT="${IMMICH_MOUNT:-/run/media/elgan/immich}"
DB="$PIPELINE_DIR/photos.db"

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║             PHOTO PIPELINE STATUS DASHBOARD                  ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# Disk space
echo "── DISK SPACE ────────────────────────────────────────────────"
df -h "$EVO_MOUNT/" "$IMMICH_MOUNT/" / 2>/dev/null | awk 'NR>1 {printf "  %-40s %5s used  %5s free  %s\n", $6, $3, $4, $5}'
echo ""

# DB stats — pass FINAL_DIR as argument so it's visible inside the heredoc
echo "── PHOTO DATABASE ────────────────────────────────────────────"
python3 - "$DB" "$FINAL_DIR" "$PIPELINE_DIR" << 'PYEOF'
import sqlite3, sys, os
db, final_dir, pipeline_dir = sys.argv[1], sys.argv[2], sys.argv[3]
try:
    c = sqlite3.connect(db, timeout=10).cursor()
    r = c.execute("""
        SELECT
          COUNT(*) total,
          SUM(is_duplicate) dupes,
          COUNT(*)-SUM(is_duplicate) unique_photos,
          SUM(CASE WHEN json_merged=1 THEN 1 ELSE 0 END) json_merged,
          SUM(CASE WHEN best_date IS NOT NULL AND CAST(SUBSTR(best_date,1,4) AS INT) BETWEEN 1990 AND 2030 THEN 1 ELSE 0 END) good_dates,
          SUM(CASE WHEN latitude IS NOT NULL THEN 1 ELSE 0 END) has_gps,
          SUM(CASE WHEN country IS NOT NULL THEN 1 ELSE 0 END) geocoded,
          SUM(CASE WHEN ai_processed=1 THEN 1 ELSE 0 END) ai_done,
          SUM(CASE WHEN album_id IS NOT NULL AND is_duplicate=0 THEN 1 ELSE 0 END) in_album
        FROM photos
    """).fetchone()
    total, dupes, uniq, json_m, gdates, gps, geo, ai, in_alb = r
    total = total or 0; dupes = dupes or 0; uniq = uniq or 0
    sidecars = c.execute("SELECT COUNT(*) FROM photos WHERE has_json_sidecar=1").fetchone()[0]
    print(f"  Total files:      {total:>7,}")
    print(f"  Unique photos:    {uniq:>7,}  ({dupes:,} duplicates removed)")
    print(f"  JSON merged:      {json_m:>7,} / {sidecars:,} sidecars")
    print(f"  Good dates:       {gdates:>7,} / {total:,}")
    print(f"  Has GPS:          {gps:>7,} / {total:,}")
    print(f"  Geocoded:         {geo:>7,} / {uniq:,}")
    print(f"  AI classified:    {ai:>7,} / {uniq:,}")
    print(f"  In albums:        {in_alb:>7,} / {uniq:,}")
    org_count = sum(len(files) for _, _, files in os.walk(final_dir)) if os.path.exists(final_dir) else 0
    print(f"  Organized files:  {org_count:>7,}  (in final-google-photos)")
    albums_count = c.execute("SELECT COUNT(*) FROM albums WHERE source='auto'").fetchone()[0]
    print(f"  Auto albums:      {albums_count:>7,}")
except Exception as e:
    print(f"  DB error: {e}")
PYEOF
echo ""

# Phase status
echo "── PHASE STATUS ──────────────────────────────────────────────"
python3 - "$DB" "$PIPELINE_DIR/orchestrator.log" "$FINAL_DIR" "$PIPELINE_DIR" << 'PYEOF'
import sqlite3, sys, re, os, subprocess
db, logfile, final_dir, pipeline_dir = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]

phases = {
    "1":   "Scan source directories",
    "2":   "Merge JSON sidecars",
    "2.5": "Fix bad/missing dates",
    "3":   "Deduplicate",
    "4":   "Reverse geocode",
    "4.5": "Guess locations (GPS inference)",
    "6":   "AI classify (background)",
    "7":   "Group into event albums",
    "7.5": "AI event naming",
    "8":   "Export to final directory",
    "9":   "Upload prep",
}

try:
    c = sqlite3.connect(db, timeout=10).cursor()
    total     = c.execute("SELECT COUNT(*) FROM photos").fetchone()[0]
    json_done = c.execute("SELECT COUNT(*) FROM photos WHERE json_merged=1").fetchone()[0]
    dupes     = c.execute("SELECT COUNT(*) FROM photos WHERE is_duplicate=1").fetchone()[0]
    geocoded  = c.execute("SELECT COUNT(*) FROM photos WHERE country IS NOT NULL").fetchone()[0]
    ai_done   = c.execute("SELECT COUNT(*) FROM photos WHERE ai_processed=1").fetchone()[0]
    in_album  = c.execute("SELECT COUNT(*) FROM photos WHERE album_id IS NOT NULL AND is_duplicate=0").fetchone()[0]
except:
    total = json_done = dupes = geocoded = ai_done = in_album = 0

org_count = sum(len(files) for _, _, files in os.walk(final_dir)) if os.path.exists(final_dir) else 0
manifest  = os.path.exists(os.path.join(pipeline_dir, "upload_manifest.json"))

completed, failed = set(), set()
if os.path.exists(logfile):
    with open(logfile) as f:
        for line in f:
            m = re.search(r'✓ Phase ([\d.]+) done', line)
            if m: completed.add(m.group(1))
            m = re.search(r'FATAL: Phase ([\d.]+) failed', line)
            if m: failed.add(m.group(1))
    for k in list(failed):
        if k in completed: failed.discard(k)

if total     > 0:   completed.add("1")
if json_done > 100: completed.add("2")
if dupes     > 0:   completed.add("3")
if geocoded  > 0:   completed.add("4")
if ai_done   > 0:   completed.add("6")
if in_album  > 0:   completed.add("7")
if org_count > 0:   completed.add("8")
if manifest:        completed.add("9")

running = set()
try:
    out = subprocess.check_output(['pgrep', '-fa', 'pipeline.py|fix_dates|name_events|guess_locations'], text=True)
    for line in out.splitlines():
        if 'fix_dates'     in line: running.add("2.5")
        elif 'name_events' in line: running.add("7.5")
        elif 'guess_loc'   in line: running.add("4.5")
        else:
            m = re.search(r'--step (\S+)', line)
            if m: running.add(m.group(1))
except: pass

for num, name in phases.items():
    if   num in running:   icon, suffix = "⟳", " [RUNNING]"
    elif num in completed: icon, suffix = "✓", ""
    elif num in failed:    icon, suffix = "✗", " [FAILED]"
    else:                  icon, suffix = "·", ""
    print(f"  {icon} Phase {num:<4} {name}{suffix}")
PYEOF
echo ""

# Running processes
echo "── RUNNING PROCESSES ─────────────────────────────────────────"
PROCS=$(pgrep -fa "pipeline.py|fix_dates|name_events|instagram_pipeline|guess_locations" 2>/dev/null || true)
if [[ -z "$PROCS" ]]; then
    echo "  (no pipeline processes running)"
else
    echo "$PROCS" | while IFS= read -r line; do echo "  $line"; done
fi
echo ""

# Recent log
echo "── RECENT LOG ────────────────────────────────────────────────"
if [[ -f "$PIPELINE_DIR/orchestrator.log" ]]; then
    tail -5 "$PIPELINE_DIR/orchestrator.log" | grep -v "^$" | sed 's/^/  /'
fi
echo ""
echo "  Live log:   tail -f $PIPELINE_DIR/orchestrator.log"
echo "  Resume:     bash run_pipeline.sh --from N"
echo "  Instagram:  bash run_instagram.sh --step summary"
