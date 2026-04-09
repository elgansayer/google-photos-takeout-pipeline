#!/usr/bin/env python3
"""
Date Fixer - Phase 2.5
========================
Finds photos with wrong/missing dates and attempts to correct them by:

1. Checking Google JSON sidecar (most authoritative)
2. Checking file modification time (if within 2001-2026 range)
3. Checking neighboring files in same directory (same sequence/burst)
4. Checking similar filenames in same dir (e.g. IMG_1234 → IMG_1235 dates)
5. Using EXIF from the file itself with validation
6. Asking ollama to estimate date from image content (last resort)

Heuristics for bad dates:
  - Before 2000-01-01 (camera clocks not set)
  - After 2026-01-01 (future = wrong)
  - Exactly 1970-01-01 (Unix epoch = no date set)
  - 1904-01-01 (old Mac/camera default)
  - Year matches known bogus values: 0001, 9999, 1900

Run: python3 fix_dates.py [--dry-run] [--use-ai]
"""

import json
import logging
import math
import os
import re
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(str(Path(os.environ.get("PIPELINE_DIR", str(Path(__file__).parent))) / 'fix_dates.log')),
        logging.StreamHandler()
    ]
)
log = logging.getLogger('fix_dates')

PIPELINE_DIR = Path(os.environ.get("PIPELINE_DIR", str(Path(__file__).parent)))
PIPELINE_DB  = PIPELINE_DIR / 'pipeline_v2.db'
OLLAMA_HOST = 'http://localhost:11434'
VISION_MODEL = 'llama3.2-vision:latest'

# Valid date range for photos
MIN_YEAR = 1990
MAX_YEAR = 2026

# Known bogus dates (year only checks)
BOGUS_YEARS = {1970, 1904, 1900, 1, 9999, 2895, 6444, 7694, 2492}


def get_db():
    conn = sqlite3.connect(str(PIPELINE_DB), timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA busy_timeout=60000')
    return conn


def is_bad_date(date_str):
    """Return True if the date is likely wrong."""
    if not date_str:
        return True
    try:
        dt = datetime.fromisoformat(date_str[:19])
        year = dt.year
        if year in BOGUS_YEARS:
            return True
        if year < MIN_YEAR or year > MAX_YEAR:
            return True
        # 1970-01-01 is Unix epoch = no date
        if year == 1970 and dt.month == 1 and dt.day == 1:
            return True
        return False
    except (ValueError, TypeError):
        return True


def try_json_sidecar(file_path, json_path):
    """Extract date from Google JSON sidecar."""
    if not json_path or not os.path.exists(json_path):
        return None
    try:
        with open(json_path) as f:
            meta = json.load(f)
        for field in ('photoTakenTime', 'creationTime'):
            ts_data = meta.get(field, {})
            if ts_data.get('timestamp'):
                ts = int(ts_data['timestamp'])
                if ts > 0:
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                    if MIN_YEAR <= dt.year <= MAX_YEAR:
                        return dt.isoformat()
    except (json.JSONDecodeError, OSError, ValueError, OverflowError):
        pass
    return None


def try_file_mtime(file_path):
    """Use file modification time if it's in a reasonable range."""
    try:
        mtime = os.path.getmtime(file_path)
        dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
        if MIN_YEAR <= dt.year <= MAX_YEAR:
            return dt.isoformat()
    except OSError:
        pass
    return None


def try_filename_date(filename):
    """Extract date from filename patterns like IMG_20190615_... or 2019-06-15_..."""
    patterns = [
        r'(\d{4})[_-](\d{2})[_-](\d{2})',   # 2019-06-15 or 20190615
        r'(\d{8})',                              # 20190615
        r'BURST(\d{4})(\d{2})(\d{2})',           # BURST20190615
        r'PXL_(\d{4})(\d{2})(\d{2})',           # Pixel naming
        r'(\d{4})(\d{2})(\d{2})_\d{6}',        # timestamp format
    ]
    for pat in patterns:
        m = re.search(pat, filename)
        if m:
            try:
                groups = m.groups()
                if len(groups) == 1:
                    s = groups[0]
                    year, month, day = int(s[:4]), int(s[4:6]), int(s[6:8])
                elif len(groups) == 3:
                    year, month, day = int(groups[0]), int(groups[1]), int(groups[2])
                else:
                    continue
                if MIN_YEAR <= year <= MAX_YEAR and 1 <= month <= 12 and 1 <= day <= 31:
                    dt = datetime(year, month, day)
                    return dt.isoformat()
            except (ValueError, IndexError):
                continue
    return None


def try_exiftool(file_path):
    """Read EXIF date tags using exiftool."""
    try:
        result = subprocess.run(
            ['exiftool', '-DateTimeOriginal', '-CreateDate', '-MediaCreateDate',
             '-TrackCreateDate', '-json', file_path],
            capture_output=True, text=True, timeout=15
        )
        if result.returncode == 0:
            data = json.loads(result.stdout)
            if data:
                for field in ('DateTimeOriginal', 'CreateDate', 'MediaCreateDate', 'TrackCreateDate'):
                    val = data[0].get(field, '')
                    if val and val not in ('0000:00:00 00:00:00', ''):
                        try:
                            # Parse "2019:06:15 10:30:00" format
                            dt = datetime.strptime(val[:19], '%Y:%m:%d %H:%M:%S')
                            if MIN_YEAR <= dt.year <= MAX_YEAR:
                                return dt.isoformat()
                        except ValueError:
                            continue
    except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError, OSError):
        pass
    return None


def try_neighbor_dates(file_path, cursor):
    """Find dates from neighboring files in the same directory."""
    parent = os.path.dirname(file_path)
    fname = os.path.basename(file_path)

    # Get files in same directory sorted by name (nearby in sequence)
    neighbors = cursor.execute("""
        SELECT best_date FROM photos
        WHERE parent_dir = (SELECT parent_dir FROM photos WHERE file_path = ?)
          AND best_date IS NOT NULL
          AND is_duplicate = 0
          AND file_path != ?
        ORDER BY ABS(
            CAST(SUBSTR(filename, 1, INSTR(filename || '.', '.') - 1) AS INTEGER) -
            CAST(SUBSTR(?, 1, INSTR(? || '.', '.') - 1) AS INTEGER)
        )
        LIMIT 20
    """, (file_path, file_path, fname, fname)).fetchall()

    valid_dates = []
    for n in neighbors:
        d = n['best_date']
        if d and not is_bad_date(d):
            try:
                dt = datetime.fromisoformat(d[:19])
                valid_dates.append(dt)
            except ValueError:
                continue

    if valid_dates:
        # Use median date of neighbors
        valid_dates.sort()
        median = valid_dates[len(valid_dates) // 2]
        return median.isoformat()
    return None


def try_directory_date(parent_dir):
    """Try to extract a date from the directory name."""
    patterns = [
        # "15 Jun 2019" or "15 Jun 2019 - 20 Jun 2019"
        (r'(\d{1,2})\s+(\w{3,9})\s+(\d{4})', '%d %b %Y'),
        (r'(\d{4})-(\d{2})-(\d{2})', None),  # "2019-06-15"
        (r'(\d{4})_(\d{2})_(\d{2})', None),  # "2019_06_15"
        (r'(\w+)\s+(\d{4})', None),           # "Japan 2019"
    ]
    dirname = os.path.basename(parent_dir)

    # Try "DD Mon YYYY"
    m = re.search(r'(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+(\d{4})', dirname, re.IGNORECASE)
    if m:
        try:
            dt = datetime.strptime(f"{m.group(1)} {m.group(2)[:3].capitalize()} {m.group(3)}", '%d %b %Y')
            if MIN_YEAR <= dt.year <= MAX_YEAR:
                return dt.isoformat()
        except ValueError:
            pass

    # Try "YYYY-MM-DD"
    m = re.search(r'(\d{4})[_-](\d{2})[_-](\d{2})', dirname)
    if m:
        try:
            dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            if MIN_YEAR <= dt.year <= MAX_YEAR:
                return dt.isoformat()
        except ValueError:
            pass

    # Try year at end: "Japan 2019"
    m = re.search(r'\b(199\d|20[012]\d)\b', dirname)
    if m:
        year = int(m.group(1))
        if MIN_YEAR <= year <= MAX_YEAR:
            return datetime(year, 6, 15).isoformat()  # Mid-year estimate

    return None


def try_ai_date(file_path):
    """Use ollama vision to estimate date from image content. Last resort."""
    import base64
    import requests

    ext = Path(file_path).suffix.lower()
    if ext not in {'.jpg', '.jpeg', '.png', '.heic', '.webp'}:
        return None

    try:
        with open(file_path, 'rb') as f:
            data = f.read()
        if len(data) > 10 * 1024 * 1024:  # Skip >10MB
            return None

        img_b64 = base64.b64encode(data).decode()

        prompt = """Look at this photo and estimate when it was taken.
Look for clues: technology visible, clothing styles, car models, seasonal indicators, location hints.
Respond ONLY with JSON: {"year": 2019, "confidence": "high|medium|low", "reason": "brief reason"}
If you cannot estimate, respond: {"year": null, "confidence": "none", "reason": "cannot determine"}"""

        resp = requests.post(f'{OLLAMA_HOST}/api/generate', json={
            'model': VISION_MODEL,
            'prompt': prompt,
            'images': [img_b64],
            'stream': False,
            'options': {'temperature': 0.1, 'num_predict': 100}
        }, timeout=60)

        text = resp.json().get('response', '')
        # Parse JSON
        m = re.search(r'\{.*?\}', text, re.DOTALL)
        if m:
            ai_data = json.loads(m.group())
            year = ai_data.get('year')
            confidence = ai_data.get('confidence', 'low')
            if year and MIN_YEAR <= int(year) <= MAX_YEAR and confidence in ('high', 'medium'):
                return datetime(int(year), 6, 15).isoformat()  # Mid-year estimate
    except Exception as e:
        log.debug(f"AI date failed for {file_path}: {e}")
    return None


def fix_dates(dry_run=False, use_ai=False):
    """Main date fixing logic."""
    conn = get_db()
    cursor = conn.cursor()

    # Find all photos with bad dates
    bad_photos = cursor.execute("""
        SELECT id, file_path, filename, parent_dir, best_date, exif_date,
               google_date, json_sidecar_path, has_json_sidecar
        FROM photos
        WHERE is_duplicate = 0
    """).fetchall()

    bad = [p for p in bad_photos if is_bad_date(p['best_date'])]
    log.info(f"Found {len(bad)} photos with bad/missing dates out of {len(bad_photos)}")

    fixed = 0
    methods = {}

    for photo in bad:
        fpath = photo['file_path']
        if not os.path.exists(fpath):
            continue

        new_date = None
        method = None

        # Priority 1: Google JSON sidecar
        if photo['has_json_sidecar'] and photo['json_sidecar_path']:
            new_date = try_json_sidecar(fpath, photo['json_sidecar_path'])
            if new_date:
                method = 'json_sidecar'

        # Priority 2: Filename pattern
        if not new_date:
            new_date = try_filename_date(photo['filename'])
            if new_date:
                method = 'filename_pattern'

        # Priority 3: exiftool (other EXIF tags)
        if not new_date:
            new_date = try_exiftool(fpath)
            if new_date:
                method = 'exiftool'

        # Priority 4: Directory name date
        if not new_date:
            new_date = try_directory_date(photo['parent_dir'] or '')
            if new_date:
                method = 'directory_name'

        # Priority 5: Neighboring files
        if not new_date:
            new_date = try_neighbor_dates(fpath, cursor)
            if new_date:
                method = 'neighbor_inference'

        # Priority 6: File modification time
        if not new_date:
            new_date = try_file_mtime(fpath)
            if new_date:
                method = 'file_mtime'

        # Priority 7: AI estimation (slow, only if requested)
        if not new_date and use_ai:
            new_date = try_ai_date(fpath)
            if new_date:
                method = 'ai_estimation'

        if new_date and not is_bad_date(new_date):
            methods[method] = methods.get(method, 0) + 1
            if not dry_run:
                cursor.execute("""
                    UPDATE photos SET best_date = ?, exif_date = COALESCE(exif_date, ?)
                    WHERE id = ?
                """, (new_date, new_date, photo['id']))

                # Also write to EXIF if exiftool-supported format
                ext = Path(fpath).suffix.lower()
                if ext in {'.jpg', '.jpeg', '.png', '.tiff', '.tif', '.dng', '.heic'}:
                    try:
                        dt = datetime.fromisoformat(new_date[:19])
                        date_str = dt.strftime('%Y:%m:%d %H:%M:%S')
                        subprocess.run(
                            ['exiftool', '-overwrite_original', '-ignoreMinorErrors',
                             f'-DateTimeOriginal={date_str}', fpath],
                            capture_output=True, timeout=15
                        )
                    except (subprocess.TimeoutExpired, ValueError, OSError):
                        pass

            fixed += 1
            if fixed % 100 == 0:
                if not dry_run:
                    conn.commit()
                if fixed % 1000 == 0:
                    log.info(f"  Fixed {fixed}/{len(bad)}... Methods: {methods}")

    if not dry_run:
        conn.commit()

    still_bad = sum(1 for p in bad if is_bad_date(p['best_date']))
    log.info(f"""
    Date fixing complete:
    Bad dates found:     {len(bad)}
    Fixed:               {fixed}
    Methods used:        {methods}
    Still unfixable:     {len(bad) - fixed}
    """)
    conn.close()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--use-ai', action='store_true', help='Use ollama to estimate dates (slow)')
    args = parser.parse_args()
    fix_dates(args.dry_run, args.use_ai)
