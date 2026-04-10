#!/usr/bin/env python3
"""
Enhanced Neighbor Date Fixer
=============================
Fixes wrong/missing dates by inferring from neighboring photos.

Strategies (in order of confidence):
1. Same directory + sequential filename (IMG_1234 → IMG_1235 = same date)
2. Same directory majority vote (if 80%+ of dir has same date, apply to rest)
3. Camera sequence inference: sort by filename, find nearest dated neighbor
4. GPS-based: photos with same GPS cluster → same event → same date range

What counts as a "bad" date:
  - Exactly midnight on Jan 1 (year N): likely a default/fallback
  - Before 1990 or after 2026
  - Unix epoch 1970-01-01
  - Known bogus years: 1904, 1900, 1, 9999

Run: python3 neighbor_date_fix.py [--dry-run] [--verbose]
     python3 neighbor_date_fix.py --stats   # just show stats, no changes
"""

import argparse
import logging
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

PIPELINE_DIR = Path(os.environ.get("PIPELINE_DIR", str(Path(__file__).parent)))
PIPELINE_DB  = PIPELINE_DIR / 'photos.db'
LOG_FILE     = PIPELINE_DIR / 'neighbor_date_fix.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(str(LOG_FILE)),
        logging.StreamHandler()
    ]
)
log = logging.getLogger('neighbor_date_fix')

MIN_YEAR = 1990
MAX_YEAR = 2026
BOGUS_YEARS = {1970, 1904, 1900, 1, 9999, 2895, 6444, 7694, 2492}


def get_db():
    conn = sqlite3.connect(str(PIPELINE_DB), timeout=300)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA busy_timeout=300000')  # 5 min
    conn.execute('PRAGMA cache_size=-100000')
    return conn


def is_bad_date(date_str):
    """Return True if the date is likely wrong."""
    if not date_str:
        return True
    try:
        dt = datetime.fromisoformat(date_str[:19])
        y = dt.year
        if y in BOGUS_YEARS:
            return True
        if y < MIN_YEAR or y > MAX_YEAR:
            return True
        if y == 1970 and dt.month == 1 and dt.day == 1:
            return True
        # midnight Jan 1st of any year = likely fallback
        if dt.month == 1 and dt.day == 1 and dt.hour == 0 and dt.minute == 0 and dt.second == 0:
            return True
        return False
    except (ValueError, TypeError):
        return True


def is_low_confidence_date(date_str):
    """Midnight exactly on any date = low confidence (fallback from filename/dir)."""
    if not date_str:
        return True
    if is_bad_date(date_str):
        return True
    try:
        dt = datetime.fromisoformat(date_str[:19])
        # Exactly midnight = likely set from filename date only, no time component
        if dt.hour == 0 and dt.minute == 0 and dt.second == 0:
            return True
        return False
    except (ValueError, TypeError):
        return True


def extract_img_number(filename):
    """Extract numeric sequence from filename like IMG_1234.jpg, DSC00123.JPG, etc."""
    name = Path(filename).stem.upper()
    # Common patterns
    m = re.search(r'(?:IMG|DSC|DSCN|DSCF|P|PIC|PHOTO|MVI|VID|MOV)[-_]?(\d{4,8})', name)
    if m:
        return int(m.group(1))
    # Just a number
    m = re.fullmatch(r'(\d{4,10})', name)
    if m:
        return int(m.group(1))
    return None


def fix_by_directory_consensus(conn, dry_run=False):
    """
    For each directory, if 70%+ of photos with good dates agree on the same
    year+month, apply that year+month to photos with bad dates in the same dir.
    """
    cursor = conn.cursor()

    log.info("Strategy 1: Directory consensus date fixing...")

    # Get all photos grouped by parent_dir
    rows = cursor.execute("""
        SELECT id, file_path, filename, parent_dir, best_date, exif_date, google_date
        FROM photos
        WHERE is_duplicate = 0
        ORDER BY parent_dir, filename
    """).fetchall()

    # Group by directory
    by_dir = defaultdict(list)
    for row in rows:
        by_dir[row['parent_dir']].append(row)

    fixed = 0
    dirs_processed = 0

    for parent_dir, photos in by_dir.items():
        if len(photos) < 3:
            continue

        # Collect good dates
        good_dates = []
        bad_ids = []
        for p in photos:
            if not is_bad_date(p['best_date']) and not is_low_confidence_date(p['best_date']):
                good_dates.append(p['best_date'][:7])  # YYYY-MM
            else:
                bad_ids.append(p['id'])

        if not good_dates or not bad_ids:
            continue

        # Check consensus: 70%+ agree on year-month
        total_good = len(good_dates)
        counter = Counter(good_dates)
        top_ym, top_count = counter.most_common(1)[0]

        consensus_ratio = top_count / total_good
        if consensus_ratio < 0.70:
            continue

        # Use consensus year-month for bad dates
        # Keep original day if it was set, otherwise use day 01
        try:
            consensus_year, consensus_month = int(top_ym[:4]), int(top_ym[5:7])
        except ValueError:
            continue

        for pid in bad_ids:
            # Get the photo details
            p_row = next((p for p in photos if p['id'] == pid), None)
            if not p_row:
                continue

            # Try to salvage a day from the original date
            old_date = p_row['best_date']
            try:
                old_dt = datetime.fromisoformat(old_date[:19])
                # If the day seems valid and reasonable (not Jan 1), keep it
                if old_dt.day > 1 and old_dt.day <= 28:
                    new_date = f"{consensus_year:04d}-{consensus_month:02d}-{old_dt.day:02d}T12:00:00"
                else:
                    new_date = f"{consensus_year:04d}-{consensus_month:02d}-15T12:00:00"
            except (ValueError, TypeError):
                new_date = f"{consensus_year:04d}-{consensus_month:02d}-15T12:00:00"

            if not dry_run:
                cursor.execute(
                    "UPDATE photos SET best_date=? WHERE id=?",
                    (new_date, pid)
                )
            fixed += 1

        dirs_processed += 1
        if dirs_processed % 500 == 0:
            if not dry_run:
                conn.commit()
            log.info(f"  Processed {dirs_processed} directories, fixed {fixed} dates...")

    if not dry_run:
        conn.commit()
    log.info(f"Strategy 1 complete: fixed {fixed} dates across {dirs_processed} directories")
    return fixed


def fix_by_filename_sequence(conn, dry_run=False):
    """
    For photos with sequential filenames (IMG_1234, IMG_1235, ...),
    if neighbors have good dates, interpolate for the bad-dated photo.
    """
    cursor = conn.cursor()

    log.info("Strategy 2: Filename sequence date fixing...")

    rows = cursor.execute("""
        SELECT id, file_path, filename, parent_dir, best_date
        FROM photos
        WHERE is_duplicate = 0
        ORDER BY parent_dir, filename
    """).fetchall()

    # Group by directory
    by_dir = defaultdict(list)
    for row in rows:
        num = extract_img_number(row['filename'])
        if num is not None:
            by_dir[row['parent_dir']].append((num, row))

    fixed = 0

    for parent_dir, numbered_photos in by_dir.items():
        if len(numbered_photos) < 2:
            continue

        # Sort by sequence number
        numbered_photos.sort(key=lambda x: x[0])
        nums = [x[0] for x in numbered_photos]
        photos = [x[1] for x in numbered_photos]

        # Build index of good dates by position
        good_at = {}  # position → date
        for i, p in enumerate(photos):
            if not is_bad_date(p['best_date']) and not is_low_confidence_date(p['best_date']):
                good_at[i] = p['best_date']

        if len(good_at) < 2:
            continue

        # For each bad-dated photo, find nearest good neighbors
        for i, p in enumerate(photos):
            if i in good_at:
                continue  # already good
            if not is_bad_date(p['best_date']) and not is_low_confidence_date(p['best_date']):
                continue

            # Find nearest good date before and after
            before_idx = max((k for k in good_at if k < i), default=None)
            after_idx = min((k for k in good_at if k > i), default=None)

            # Use the nearest one, or interpolate if both exist
            if before_idx is not None and after_idx is not None:
                # Prefer the one closest by sequence number
                dist_before = nums[i] - nums[before_idx]
                dist_after = nums[after_idx] - nums[i]
                if dist_before <= dist_after and dist_before <= 50:
                    candidate = good_at[before_idx]
                elif dist_after <= 50:
                    candidate = good_at[after_idx]
                else:
                    continue  # too far

                # Verify before and after are within same week
                try:
                    dt_before = datetime.fromisoformat(good_at[before_idx][:19])
                    dt_after = datetime.fromisoformat(good_at[after_idx][:19])
                    if abs((dt_after - dt_before).days) > 7:
                        continue  # different events
                    candidate = good_at[before_idx]
                except (ValueError, TypeError):
                    continue

            elif before_idx is not None and (nums[i] - nums[before_idx]) <= 50:
                candidate = good_at[before_idx]
            elif after_idx is not None and (nums[after_idx] - nums[i]) <= 50:
                candidate = good_at[after_idx]
            else:
                continue

            if not dry_run:
                cursor.execute(
                    "UPDATE photos SET best_date=? WHERE id=?",
                    (candidate, p['id'])
                )
            fixed += 1

        if fixed % 1000 == 0 and fixed > 0:
            if not dry_run:
                conn.commit()

    if not dry_run:
        conn.commit()
    log.info(f"Strategy 2 complete: fixed {fixed} dates by filename sequence")
    return fixed


def fix_by_gps_cluster(conn, dry_run=False):
    """
    Photos with same GPS location cluster (within 0.1 degrees) taken close
    in time: if some have good dates, fix the others.
    """
    cursor = conn.cursor()

    log.info("Strategy 3: GPS cluster date fixing...")

    rows = cursor.execute("""
        SELECT id, best_date, latitude, longitude
        FROM photos
        WHERE is_duplicate = 0 AND latitude IS NOT NULL AND longitude IS NOT NULL
        ORDER BY latitude, longitude
    """).fetchall()

    if not rows:
        log.info("No GPS photos to process")
        return 0

    fixed = 0
    # Simple approach: for bad-dated photos with GPS, find nearby photos with good dates
    bad_gps = [(r['id'], r['latitude'], r['longitude']) for r in rows
               if is_bad_date(r['best_date']) or is_low_confidence_date(r['best_date'])]

    if not bad_gps:
        log.info("No bad-dated GPS photos")
        return 0

    good_gps = [(r['best_date'], r['latitude'], r['longitude']) for r in rows
                if not is_bad_date(r['best_date']) and not is_low_confidence_date(r['best_date'])]

    log.info(f"  {len(bad_gps)} bad-dated GPS photos, {len(good_gps)} good-dated GPS photos")

    for pid, lat, lon in bad_gps:
        # Find nearest good-dated photo within 0.05 degrees (~5km)
        best_dist = 9999
        best_date = None
        for gd, glat, glon in good_gps:
            dist = ((lat - glat) ** 2 + (lon - glon) ** 2) ** 0.5
            if dist < 0.05 and dist < best_dist:
                best_dist = dist
                best_date = gd

        if best_date:
            if not dry_run:
                cursor.execute(
                    "UPDATE photos SET best_date=? WHERE id=?",
                    (best_date, pid)
                )
            fixed += 1

    if fixed % 1000 == 0 and fixed > 0:
        if not dry_run:
            conn.commit()

    if not dry_run:
        conn.commit()
    log.info(f"Strategy 3 complete: fixed {fixed} dates by GPS clustering")
    return fixed


def print_stats(conn):
    """Print date quality statistics."""
    cursor = conn.cursor()

    total = cursor.execute("SELECT COUNT(*) FROM photos WHERE is_duplicate=0").fetchone()[0]

    # Count bad dates
    rows = cursor.execute(
        "SELECT id, best_date FROM photos WHERE is_duplicate=0"
    ).fetchall()

    bad = sum(1 for r in rows if is_bad_date(r['best_date']))
    low_conf = sum(1 for r in rows if not is_bad_date(r['best_date']) and
                   is_low_confidence_date(r['best_date']))
    good = total - bad - low_conf

    print(f"\n=== Date Quality Stats ===")
    print(f"Total unique photos: {total:,}")
    print(f"  Good dates (with time): {good:,} ({100*good//total}%)")
    print(f"  Low confidence (midnight only): {low_conf:,} ({100*low_conf//total}%)")
    print(f"  Bad/missing dates: {bad:,} ({100*bad//total}%)")

    # Year distribution
    year_counts = Counter()
    for r in rows:
        if r['best_date']:
            try:
                year_counts[r['best_date'][:4]] += 1
            except:
                pass
    print(f"\n  Photos per year:")
    for year in sorted(year_counts.keys()):
        bar = '█' * (year_counts[year] // 1000)
        print(f"    {year}: {year_counts[year]:6,} {bar}")


def main():
    parser = argparse.ArgumentParser(description='Fix photo dates using neighbor inference')
    parser.add_argument('--dry-run', action='store_true', help='Show what would change, no writes')
    parser.add_argument('--stats', action='store_true', help='Show date stats and exit')
    parser.add_argument('--strategy', choices=['1', '2', '3', 'all'], default='all',
                        help='Which strategy to run (default: all)')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    conn = get_db()

    print_stats(conn)

    if args.stats:
        conn.close()
        return

    if args.dry_run:
        log.info("DRY RUN - no changes will be written")

    total_fixed = 0

    if args.strategy in ('1', 'all'):
        total_fixed += fix_by_directory_consensus(conn, args.dry_run)

    if args.strategy in ('2', 'all'):
        total_fixed += fix_by_filename_sequence(conn, args.dry_run)

    if args.strategy in ('3', 'all'):
        total_fixed += fix_by_gps_cluster(conn, args.dry_run)

    log.info(f"\nTotal dates fixed: {total_fixed}")
    print_stats(conn)
    conn.close()


if __name__ == '__main__':
    main()
