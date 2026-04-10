#!/usr/bin/env python3
"""
Unified Photo Organization Pipeline v2
========================================
Processes Google Takeout photos from one or two source directories into a
single organized library of event-based albums.

Configure via environment variables (see .env.example):
  PRIMARY_DIR   — primary Google Takeout export directory
  SECONDARY_DIR — optional second export (e.g. enriched EXIF copy)
  FINAL_DIR     — output directory for organized photos
  PIPELINE_DIR  — working directory for DB and logs (defaults to script dir)

Steps (run in order):
  scan            Catalogue all photos from source directories
  merge-sidecars  Merge Google JSON metadata into photo records
  deduplicate     Remove duplicate photos (hash-based)
  fix-dates       Fix broken/missing timestamps
  geocode         Reverse-geocode photos using GeoNames (offline)
  classify        AI-tag every photo using a vision model (slow — runs in background)
  group-albums    Cluster photos into event albums by date/location
  export          Organise albums into flat event folders in output dir
  prep-upload     Generate rclone upload scripts for Google Photos

Usage:
    python3 pipeline.py --step scan
    python3 pipeline.py --step classify
    python3 pipeline.py --step all
    python3 pipeline.py --step export --dry-run
"""

import argparse
import csv
import hashlib
import json
import logging
import math
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time
from collections import defaultdict, Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

# === Configuration (set via environment variables or edit directly) ===
# See .env.example for all available options
PRIMARY_DIR   = Path(os.environ.get("PRIMARY_DIR",   ""))   # Required: primary photo source dir
SECONDARY_DIR = Path(os.environ.get("SECONDARY_DIR", ""))   # Optional: secondary source (leave empty if unused)
PIPELINE_DIR  = Path(os.environ.get("PIPELINE_DIR",  str(Path(__file__).parent)))
PIPELINE_DB   = PIPELINE_DIR / "photos.db"
GEONAMES_FILE = Path(os.environ.get("GEONAMES_FILE", str(PIPELINE_DIR / "allCountries.txt")))
FINAL_DIR     = Path(os.environ.get("FINAL_DIR",     ""))   # Required: output directory

# DigiKam DBs (optional, only used when SECONDARY_DIR is set)
DIGIKAM_DB    = SECONDARY_DIR / "digikam4.db"    if SECONDARY_DIR.name else None
SIMILARITY_DB = SECONDARY_DIR / "similarity.db"  if SECONDARY_DIR.name else None

OLLAMA_HOST  = os.environ.get("OLLAMA_HOST",  "http://localhost:11434")
VISION_MODEL = os.environ.get("VISION_MODEL", "llama3.2-vision:latest")
TEXT_MODEL   = os.environ.get("TEXT_MODEL",   "gemma3:4b")

IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.heic', '.webp', '.bmp',
                    '.tiff', '.tif', '.arw', '.dng', '.cr2', '.nef', '.orf', '.rw2'}
VIDEO_EXTENSIONS = {'.mp4', '.mov', '.avi', '.mkv', '.3gp', '.wmv', '.flv', '.webm'}
MEDIA_EXTENSIONS = IMAGE_EXTENSIONS | VIDEO_EXTENSIONS
SKIP_EXTENSIONS = {'.json', '.db', '.py', '.sh', '.txt', '.log', '.md', '.csv',
                   '.html', '.js', '.css', '.bak', '.venv'}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(PIPELINE_DIR / "pipeline.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("pipeline")


# ============================================================
# Database
# ============================================================

def init_db():
    conn = sqlite3.connect(str(PIPELINE_DB), timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("PRAGMA cache_size=-200000")  # 200MB cache

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS photos (
            id INTEGER PRIMARY KEY,
            file_path TEXT UNIQUE,
            source TEXT,  -- 'primary' or 'secondary'
            filename TEXT,
            parent_dir TEXT,
            file_size INTEGER,
            file_hash TEXT,
            -- Dates
            exif_date TEXT,
            google_date TEXT,
            best_date TEXT,
            -- GPS
            latitude REAL,
            longitude REAL,
            altitude REAL,
            -- Location
            country TEXT,
            country_code TEXT,
            city TEXT,
            state TEXT,
            location_name TEXT,
            -- Metadata
            camera_make TEXT,
            camera_model TEXT,
            width INTEGER,
            height INTEGER,
            media_type TEXT,
            -- Google JSON sidecar
            has_json_sidecar INTEGER DEFAULT 0,
            json_sidecar_path TEXT,
            google_description TEXT,
            google_people TEXT,
            google_url TEXT,
            -- AI
            ai_description TEXT,
            ai_tags TEXT,
            ai_scene_type TEXT,
            ai_activity TEXT,
            ai_indoor_outdoor TEXT,
            ai_is_holiday INTEGER DEFAULT 0,
            ai_holiday_type TEXT,
            ai_processed INTEGER DEFAULT 0,
            -- Album
            album_name TEXT,
            album_id INTEGER,
            -- State
            json_merged INTEGER DEFAULT 0,
            is_duplicate INTEGER DEFAULT 0,
            duplicate_of INTEGER,
            phase_completed INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS albums (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE,
            start_date TEXT,
            end_date TEXT,
            country TEXT,
            city TEXT,
            photo_count INTEGER DEFAULT 0,
            album_type TEXT,
            source TEXT
        );

        CREATE TABLE IF NOT EXISTS duplicates (
            id INTEGER PRIMARY KEY,
            canonical_id INTEGER,
            duplicate_id INTEGER,
            canonical_path TEXT,
            duplicate_path TEXT,
            match_type TEXT,
            similarity_score REAL,
            UNIQUE(canonical_id, duplicate_id)
        );

        CREATE INDEX IF NOT EXISTS idx_p_hash ON photos(file_hash);
        CREATE INDEX IF NOT EXISTS idx_p_date ON photos(best_date);
        CREATE INDEX IF NOT EXISTS idx_p_latlon ON photos(latitude, longitude);
        CREATE INDEX IF NOT EXISTS idx_p_album ON photos(album_id);
        CREATE INDEX IF NOT EXISTS idx_p_dup ON photos(is_duplicate);
        CREATE INDEX IF NOT EXISTS idx_p_parent ON photos(parent_dir);
        CREATE INDEX IF NOT EXISTS idx_p_source ON photos(source);
        CREATE INDEX IF NOT EXISTS idx_p_size ON photos(file_size);
        CREATE INDEX IF NOT EXISTS idx_p_fn ON photos(filename);
    """)
    conn.commit()
    return conn


def get_db():
    conn = sqlite3.connect(str(PIPELINE_DB), timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    return conn


# ============================================================
# Phase 1: Unified Audit
# ============================================================

def scan(dry_run=False):
    """Catalog ALL files from both directories."""
    log.info("=== PHASE 1: Unified Audit of Both Directories ===")
    conn = init_db()
    cursor = conn.cursor()

    # Step 1: Import DigiKam data (covers the internal 162K files with rich metadata)
    log.info("Step 1: Importing DigiKam catalog (162K files with hashes, GPS, metadata)...")
    dk = sqlite3.connect(str(DIGIKAM_DB))
    dk.row_factory = sqlite3.Row

    dk_query = """
        SELECT i.name as filename, a.relativePath as rel_path,
               i.fileSize, i.uniqueHash,
               ii.creationDate, ii.width, ii.height,
               ip.latitudeNumber as lat, ip.longitudeNumber as lon, ip.altitude,
               im.make, im.model
        FROM Images i
        JOIN Albums a ON i.album = a.id
        LEFT JOIN ImageInformation ii ON i.id = ii.imageid
        LEFT JOIN ImagePositions ip ON i.id = ip.imageid
        LEFT JOIN ImageMetadata im ON i.id = im.imageid
        WHERE i.status = 1
    """

    batch = []
    imported = 0
    for row in dk.execute(dk_query):
        rel = (row['rel_path'] or '/').lstrip('/')
        # Build paths for BOTH locations (file may exist in either/both)
        fname = row['filename']
        ext = Path(fname).suffix.lower()
        mtype = 'image' if ext in IMAGE_EXTENSIONS else ('video' if ext in VIDEO_EXTENSIONS else 'other')
        lat = row['lat'] if row['lat'] and abs(row['lat']) > 0.001 else None
        lon = row['lon'] if row['lon'] and abs(row['lon']) > 0.001 else None

        # Try primary path first, then secondary
        if rel:
            primary_path = str(PRIMARY_DIR / rel / fname)
            secondary_path = str(SECONDARY_DIR / rel / fname)
        else:
            primary_path = str(PRIMARY_DIR / fname)
            secondary_path = str(SECONDARY_DIR / fname)

        # Determine which path actually exists
        if os.path.exists(primary_path):
            fpath = primary_path
            source = 'primary'
        elif os.path.exists(secondary_path):
            fpath = secondary_path
            source = 'secondary'
        else:
            # File might have been moved/quarantined - skip
            continue

        batch.append((
            fpath, source, fname, rel,
            row['fileSize'], row['uniqueHash'],
            row['creationDate'], lat, lon, row['altitude'],
            row['make'], row['model'], row['width'], row['height'], mtype
        ))

        if len(batch) >= 5000:
            cursor.executemany("""
                INSERT OR IGNORE INTO photos
                (file_path, source, filename, parent_dir, file_size, file_hash,
                 exif_date, latitude, longitude, altitude,
                 camera_make, camera_model, width, height, media_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch)
            imported += len(batch)
            batch = []
            log.info(f"  DigiKam import: {imported}...")

    if batch:
        cursor.executemany("""
            INSERT OR IGNORE INTO photos
            (file_path, source, filename, parent_dir, file_size, file_hash,
             exif_date, latitude, longitude, altitude,
             camera_make, camera_model, width, height, media_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, batch)
        imported += len(batch)
    conn.commit()
    dk.close()
    log.info(f"DigiKam: imported {imported} records")

    # Step 2: Walk PRIMARY directory for everything DigiKam missed
    log.info("Step 2: Scanning primary directory (external drive, 422K files)...")
    new_primary = _index_directory(PRIMARY_DIR, 'primary', cursor, conn)
    log.info(f"Primary scan: {new_primary} new files")

    # Step 3: Walk SECONDARY for any unique files
    log.info("Step 3: Scanning secondary directory (internal drive)...")
    new_secondary = _index_directory(SECONDARY_DIR, 'secondary', cursor, conn)
    log.info(f"Secondary scan: {new_secondary} new files")

    # Step 4: Match JSON sidecars (the big win — 89K on external)
    log.info("Step 4: Matching JSON sidecars...")
    json_matched = 0
    sources = [(PRIMARY_DIR, 'primary')]
    if SECONDARY_DIR.name and SECONDARY_DIR.is_dir():
        sources.append((SECONDARY_DIR, 'secondary'))
    for source_dir, source_label in sources:
        for root, dirs, files in os.walk(str(source_dir)):
            json_files = [f for f in files if f.endswith('.json') and f != 'metadata.json'
                         and 'print-subscriptions' not in f and 'shared_album_comments' not in f
                         and 'user-generated-memory' not in f]
            for jf in json_files:
                json_path = os.path.join(root, jf)
                # Strip sidecar suffixes to find the base image name
                base = jf
                for suffix in ['.supplemental-metadata.json', '.json']:
                    if base.endswith(suffix):
                        base = base[:-len(suffix)]
                        break

                image_path = os.path.join(root, base)
                # Also try without (N) suffix variations
                row = cursor.execute(
                    "SELECT id FROM photos WHERE file_path = ?", (image_path,)
                ).fetchone()
                if row:
                    cursor.execute("""
                        UPDATE photos SET has_json_sidecar = 1, json_sidecar_path = ?
                        WHERE id = ? AND has_json_sidecar = 0
                    """, (json_path, row['id']))
                    json_matched += 1

            if json_matched % 10000 == 0 and json_matched > 0:
                conn.commit()

    conn.commit()
    log.info(f"Matched {json_matched} JSON sidecars")

    # Step 5: Set best_date
    cursor.execute("""
        UPDATE photos SET best_date = COALESCE(exif_date, google_date)
        WHERE best_date IS NULL
    """)
    conn.commit()

    # Report
    stats = cursor.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN source = 'primary' THEN 1 ELSE 0 END) as from_primary,
            SUM(CASE WHEN source = 'secondary' THEN 1 ELSE 0 END) as from_secondary,
            SUM(CASE WHEN media_type = 'image' THEN 1 ELSE 0 END) as images,
            SUM(CASE WHEN media_type = 'video' THEN 1 ELSE 0 END) as videos,
            SUM(CASE WHEN latitude IS NOT NULL THEN 1 ELSE 0 END) as with_gps,
            SUM(CASE WHEN has_json_sidecar = 1 THEN 1 ELSE 0 END) as with_json,
            SUM(CASE WHEN file_hash IS NOT NULL THEN 1 ELSE 0 END) as with_hash,
            SUM(CASE WHEN best_date IS NOT NULL THEN 1 ELSE 0 END) as with_date,
            SUM(file_size) as total_bytes
        FROM photos
    """).fetchone()

    log.info(f"""
    === Phase 1 Complete ===
    Total files:     {stats['total']}
      From primary:  {stats['from_primary']} (external evo)
      From secondary:{stats['from_secondary']} (internal)
    Images:          {stats['images']}
    Videos:          {stats['videos']}
    With GPS:        {stats['with_gps']}
    With JSON:       {stats['with_json']}
    With hash:       {stats['with_hash']}
    With date:       {stats['with_date']}
    Total size:      {(stats['total_bytes'] or 0) / (1024**3):.1f} GB
    """)
    conn.close()


def _index_directory(base_dir, source_label, cursor, conn):
    """Walk a directory and add uncatalogued media files to DB."""
    new_count = 0
    batch = []
    for root, dirs, files in os.walk(str(base_dir)):
        # Skip known non-media directories
        dirs[:] = [d for d in dirs if d not in ('_processed', '__pycache__', '.venv', '.genkit', 'old')]
        for fname in files:
            ext = Path(fname).suffix.lower()
            if ext not in MEDIA_EXTENSIONS:
                continue
            fpath = os.path.join(root, fname)
            # Check not already in DB
            exists = cursor.execute("SELECT 1 FROM photos WHERE file_path = ?", (fpath,)).fetchone()
            if exists:
                continue
            rel = os.path.relpath(os.path.dirname(fpath), str(base_dir))
            try:
                fsize = os.path.getsize(fpath)
            except OSError:
                continue
            mtype = 'image' if ext in IMAGE_EXTENSIONS else 'video'
            batch.append((fpath, source_label, fname, rel, fsize, mtype))
            if len(batch) >= 5000:
                cursor.executemany("""
                    INSERT OR IGNORE INTO photos
                    (file_path, source, filename, parent_dir, file_size, media_type)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, batch)
                new_count += len(batch)
                batch = []
                conn.commit()
                log.info(f"    {source_label}: scanned {new_count}...")
    if batch:
        cursor.executemany("""
            INSERT OR IGNORE INTO photos
            (file_path, source, filename, parent_dir, file_size, media_type)
            VALUES (?, ?, ?, ?, ?, ?)
        """, batch)
        new_count += len(batch)
        conn.commit()
    return new_count


# ============================================================
# Phase 2: Merge JSON Metadata into EXIF
# ============================================================

def merge_sidecars(dry_run=False):
    """Read Google JSON sidecars and write metadata into EXIF."""
    log.info("=== PHASE 2: Merge 89K JSON Sidecars into EXIF ===")
    conn = get_db()
    cursor = conn.cursor()

    rows = cursor.execute("""
        SELECT id, file_path, json_sidecar_path
        FROM photos
        WHERE has_json_sidecar = 1 AND json_merged = 0
    """).fetchall()

    log.info(f"Found {len(rows)} files with unmerged JSON sidecars")
    merged = 0
    errors = 0

    for row in rows:
        try:
            with open(row['json_sidecar_path'], 'r') as f:
                meta = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError, PermissionError):
            errors += 1
            continue

        updates = {}
        exiftool_args = ['-overwrite_original', '-ignoreMinorErrors']

        # Photo taken time
        taken = meta.get('photoTakenTime', {})
        if taken.get('timestamp'):
            try:
                ts = int(taken['timestamp'])
                dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                date_str = dt.strftime('%Y:%m:%d %H:%M:%S')
                exiftool_args.append(f'-DateTimeOriginal={date_str}')
                updates['google_date'] = dt.isoformat()
            except (ValueError, OverflowError, OSError):
                pass

        # GPS data
        geo = meta.get('geoData', {})
        lat = geo.get('latitude', 0)
        lon = geo.get('longitude', 0)
        alt = geo.get('altitude', 0)

        if abs(lat) > 0.001 or abs(lon) > 0.001:
            lat_ref = 'N' if lat >= 0 else 'S'
            lon_ref = 'E' if lon >= 0 else 'W'
            exiftool_args.extend([
                f'-GPSLatitude={abs(lat)}', f'-GPSLatitudeRef={lat_ref}',
                f'-GPSLongitude={abs(lon)}', f'-GPSLongitudeRef={lon_ref}',
            ])
            if alt:
                exiftool_args.append(f'-GPSAltitude={abs(alt)}')
            updates['latitude'] = lat
            updates['longitude'] = lon
            updates['altitude'] = alt

        # Description
        desc = meta.get('description', '')
        if desc:
            exiftool_args.append(f'-ImageDescription={desc}')
            updates['google_description'] = desc

        # People
        people = meta.get('people', [])
        if people:
            names = [p.get('name', '') for p in people if p.get('name')]
            if names:
                for name in names:
                    exiftool_args.append(f'-XMP-iptcExt:PersonInImage={name}')
                updates['google_people'] = json.dumps(names)

        # URL
        url = meta.get('url', '')
        if url:
            updates['google_url'] = url

        # Write EXIF if meaningful data and not dry run
        if len(exiftool_args) > 2 and not dry_run:
            fpath = row['file_path']
            if os.path.exists(fpath):
                ext = Path(fpath).suffix.lower()
                if ext in {'.jpg', '.jpeg', '.png', '.tiff', '.tif', '.dng', '.heic'}:
                    try:
                        result = subprocess.run(
                            ['exiftool'] + exiftool_args + [fpath],
                            capture_output=True, text=True, timeout=30
                        )
                        if result.returncode != 0:
                            errors += 1
                            continue
                    except (subprocess.TimeoutExpired, FileNotFoundError):
                        errors += 1
                        continue

        # Update DB
        updates['json_merged'] = 1
        set_clause = ', '.join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [row['id']]
        cursor.execute(f"UPDATE photos SET {set_clause} WHERE id = ?", values)
        merged += 1

        if merged % 1000 == 0:
            conn.commit()
            log.info(f"  Merged {merged}/{len(rows)}... (errors: {errors})")

    # Update best_date from google_date where missing
    cursor.execute("""
        UPDATE photos SET best_date = google_date
        WHERE best_date IS NULL AND google_date IS NOT NULL
    """)
    # Also update lat/lon where we got it from JSON but didn't have it from DigiKam
    cursor.execute("""
        UPDATE photos SET best_date = COALESCE(best_date, exif_date, google_date)
    """)
    conn.commit()
    log.info(f"Phase 2 complete: merged {merged}, errors {errors}")
    conn.close()


# ============================================================
# Phase 3: Deduplicate
# ============================================================

def deduplicate(dry_run=False):
    """Hash-based dedup across both sources. Also compute hashes for unhashed files."""
    log.info("=== PHASE 3: Deduplicate ===")
    conn = get_db()
    cursor = conn.cursor()

    # Step 1: Compute hashes for files that don't have one (from DigiKam)
    unhashed = cursor.execute("""
        SELECT id, file_path, file_size FROM photos
        WHERE file_hash IS NULL OR file_hash = ''
    """).fetchall()

    if unhashed:
        log.info(f"Computing hashes for {len(unhashed)} unhashed files...")
        hashed = 0
        for row in unhashed:
            fpath = row['file_path']
            if not os.path.exists(fpath):
                continue
            try:
                h = _file_hash(fpath)
                cursor.execute("UPDATE photos SET file_hash = ? WHERE id = ?", (h, row['id']))
                hashed += 1
                if hashed % 5000 == 0:
                    conn.commit()
                    log.info(f"  Hashed {hashed}/{len(unhashed)}...")
            except (OSError, PermissionError):
                continue
        conn.commit()
        log.info(f"Hashed {hashed} files")

    # Step 2: Find hash-based duplicates
    log.info("Finding hash-based duplicates...")
    hash_groups = cursor.execute("""
        SELECT file_hash, GROUP_CONCAT(id) as ids, COUNT(*) as cnt
        FROM photos
        WHERE file_hash IS NOT NULL AND file_hash != ''
        GROUP BY file_hash
        HAVING cnt > 1
    """).fetchall()

    log.info(f"Found {len(hash_groups)} duplicate hash groups")
    dupe_count = 0

    for group in hash_groups:
        ids = [int(x) for x in group['ids'].split(',')]
        photos = cursor.execute(
            f"SELECT * FROM photos WHERE id IN ({','.join('?' * len(ids))})", ids
        ).fetchall()

        canonical = _select_best_copy(photos)
        for photo in photos:
            if photo['id'] != canonical['id']:
                cursor.execute("""
                    INSERT OR IGNORE INTO duplicates
                    (canonical_id, duplicate_id, canonical_path, duplicate_path, match_type, similarity_score)
                    VALUES (?, ?, ?, ?, 'hash', 1.0)
                """, (canonical['id'], photo['id'], canonical['file_path'], photo['file_path']))
                cursor.execute(
                    "UPDATE photos SET is_duplicate = 1, duplicate_of = ? WHERE id = ?",
                    (canonical['id'], photo['id'])
                )
                dupe_count += 1

        if dupe_count % 10000 == 0 and dupe_count > 0:
            conn.commit()

    # Step 3: Also match by filename + size for files without hashes
    log.info("Finding filename+size duplicates...")
    size_groups = cursor.execute("""
        SELECT filename, file_size, GROUP_CONCAT(id) as ids, COUNT(*) as cnt
        FROM photos
        WHERE is_duplicate = 0 AND file_size > 0
          AND (file_hash IS NULL OR file_hash = '')
        GROUP BY filename, file_size
        HAVING cnt > 1
    """).fetchall()

    for group in size_groups:
        ids = [int(x) for x in group['ids'].split(',')]
        photos = cursor.execute(
            f"SELECT * FROM photos WHERE id IN ({','.join('?' * len(ids))})", ids
        ).fetchall()
        canonical = _select_best_copy(photos)
        for photo in photos:
            if photo['id'] != canonical['id']:
                cursor.execute("""
                    INSERT OR IGNORE INTO duplicates
                    (canonical_id, duplicate_id, canonical_path, duplicate_path, match_type, similarity_score)
                    VALUES (?, ?, ?, ?, 'name_size', 0.95)
                """, (canonical['id'], photo['id'], canonical['file_path'], photo['file_path']))
                cursor.execute(
                    "UPDATE photos SET is_duplicate = 1, duplicate_of = ? WHERE id = ?",
                    (canonical['id'], photo['id'])
                )
                dupe_count += 1

    conn.commit()

    stats = cursor.execute("""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN is_duplicate = 0 THEN 1 ELSE 0 END) as unique_count,
               SUM(CASE WHEN is_duplicate = 1 THEN 1 ELSE 0 END) as dupe_count
        FROM photos
    """).fetchone()

    log.info(f"""
    Phase 3 complete:
    Total files:     {stats['total']}
    Unique:          {stats['unique_count']}
    Duplicates:      {stats['dupe_count']}
    """)
    conn.close()


def _file_hash(path, chunk_size=65536):
    """Compute MD5 hash of first 64KB + last 64KB + file size for speed."""
    fsize = os.path.getsize(path)
    h = hashlib.md5()
    h.update(str(fsize).encode())
    with open(path, 'rb') as f:
        h.update(f.read(chunk_size))
        if fsize > chunk_size * 2:
            f.seek(-chunk_size, 2)
            h.update(f.read(chunk_size))
    return h.hexdigest()


def _select_best_copy(photos):
    """Pick best copy: prefer primary with JSON sidecar > named album > most metadata."""
    def score(p):
        s = 0
        d = p['parent_dir'] or ''
        # Prefer files with JSON sidecar
        if p['has_json_sidecar']:
            s -= 100
        # Prefer primary (external - more complete)
        if p['source'] == 'primary':
            s -= 50
        # Prefer named albums over date dirs
        if d and not re.match(r'^\d', d) and ':' not in d:
            s -= 20
        elif d.startswith('Photos from'):
            s -= 10
        elif ':' in d:
            s += 50  # Broken dir name
        # Prefer files with GPS
        if p['latitude']:
            s -= 5
        return s

    return min(photos, key=score)


# ============================================================
# Phase 4: Fix Broken Directory Names
# ============================================================

def fix_dates(dry_run=False):
    """Fix broken timestamp directory names on PRIMARY (external) drive."""
    log.info("=== PHASE 4: Fix Broken Directory Names ===")
    conn = get_db()
    cursor = conn.cursor()

    broken_dirs = set()
    for entry in os.listdir(str(PRIMARY_DIR)):
        if (PRIMARY_DIR / entry).is_dir() and re.match(r'\d{4}:', entry):
            broken_dirs.add(entry)

    log.info(f"Found {len(broken_dirs)} broken timestamp directories on external")

    fixed = 0
    for dirname in sorted(broken_dirs):
        src = PRIMARY_DIR / dirname
        try:
            dt = datetime.strptime(dirname[:10], '%Y:%m:%d')
            new_name = dt.strftime('%d %b %Y')
        except ValueError:
            new_name = dirname.replace(':', '-')

        dst = PRIMARY_DIR / new_name

        if not dry_run:
            if dst.exists():
                # Merge
                for item in os.listdir(str(src)):
                    item_src = src / item
                    item_dst = dst / item
                    if item_dst.exists():
                        base, ext = os.path.splitext(item)
                        counter = 1
                        while item_dst.exists():
                            item_dst = dst / f"{base}_m{counter}{ext}"
                            counter += 1
                    try:
                        shutil.move(str(item_src), str(item_dst))
                    except Exception as e:
                        log.warning(f"  Could not move {item_src}: {e}")
                try:
                    src.rmdir()
                except OSError:
                    pass
            else:
                try:
                    src.rename(dst)
                except OSError as e:
                    log.warning(f"  Could not rename {dirname}: {e}")
                    continue

            # Update DB
            old_prefix = str(src)
            new_prefix = str(dst)
            cursor.execute("""
                UPDATE OR IGNORE photos SET
                    file_path = REPLACE(file_path, ?, ?),
                    parent_dir = REPLACE(parent_dir, ?, ?)
                WHERE file_path LIKE ?
            """, (old_prefix, new_prefix, dirname, new_name, f"{old_prefix}%"))

        fixed += 1
        if fixed % 1000 == 0:
            conn.commit()
            log.info(f"  Fixed {fixed}/{len(broken_dirs)}...")

    conn.commit()
    log.info(f"Phase 4 complete: fixed {fixed} directories")
    conn.close()


# ============================================================
# Phase 5: Reverse Geocoding
# ============================================================

class GeoNamesLookup:
    """Offline reverse geocoder using GeoNames allCountries.txt."""
    def __init__(self, path):
        self.places = []
        self._load(path)

    def _load(self, path):
        log.info(f"Loading GeoNames from {path}...")
        count = 0
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.split('\t')
                if len(parts) < 15:
                    continue
                if parts[6] not in ('P', 'A'):
                    continue
                try:
                    pop = int(parts[14]) if parts[14] else 0
                    if pop < 500:
                        continue
                    self.places.append({
                        'name': parts[1], 'lat': float(parts[4]), 'lon': float(parts[5]),
                        'cc': parts[8], 'pop': pop
                    })
                    count += 1
                except (ValueError, IndexError):
                    continue
        self.places.sort(key=lambda x: -x['pop'])
        log.info(f"Loaded {count} places")

    def find_nearest(self, lat, lon, max_km=50):
        best = None
        best_d = max_km
        for p in self.places:
            d = self._haversine(lat, lon, p['lat'], p['lon'])
            if d < best_d:
                best_d = d
                best = p
                if d < 1:
                    break
        return best

    @staticmethod
    def _haversine(lat1, lon1, lat2, lon2):
        R = 6371
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
        return R * 2 * math.asin(min(1, math.sqrt(a)))


COUNTRY_CODES = {
    'US': 'USA', 'GB': 'United Kingdom', 'FR': 'France', 'DE': 'Germany',
    'ES': 'Spain', 'IT': 'Italy', 'JP': 'Japan', 'AU': 'Australia',
    'NZ': 'New Zealand', 'CA': 'Canada', 'NL': 'Netherlands', 'BE': 'Belgium',
    'AT': 'Austria', 'CH': 'Switzerland', 'SE': 'Sweden', 'NO': 'Norway',
    'DK': 'Denmark', 'FI': 'Finland', 'PT': 'Portugal', 'IE': 'Ireland',
    'GR': 'Greece', 'TR': 'Turkey', 'TH': 'Thailand', 'VN': 'Vietnam',
    'ID': 'Indonesia', 'MY': 'Malaysia', 'SG': 'Singapore', 'PH': 'Philippines',
    'IN': 'India', 'CN': 'China', 'KR': 'South Korea', 'TW': 'Taiwan',
    'HK': 'Hong Kong', 'MX': 'Mexico', 'BR': 'Brazil', 'AR': 'Argentina',
    'CL': 'Chile', 'CO': 'Colombia', 'PE': 'Peru', 'ZA': 'South Africa',
    'EG': 'Egypt', 'MA': 'Morocco', 'KE': 'Kenya', 'TZ': 'Tanzania',
    'IS': 'Iceland', 'HR': 'Croatia', 'CZ': 'Czech Republic', 'PL': 'Poland',
    'HU': 'Hungary', 'RO': 'Romania', 'RS': 'Serbia', 'ME': 'Montenegro',
    'BA': 'Bosnia', 'SK': 'Slovakia', 'SI': 'Slovenia', 'LT': 'Lithuania',
    'LV': 'Latvia', 'EE': 'Estonia', 'MT': 'Malta', 'CY': 'Cyprus',
    'LU': 'Luxembourg', 'AD': 'Andorra', 'BG': 'Bulgaria',
}


def geocode(dry_run=False):
    """Reverse geocode photos with GPS but no location names."""
    log.info("=== PHASE 5: Reverse Geocoding ===")
    conn = get_db()
    cursor = conn.cursor()

    rows = cursor.execute("""
        SELECT id, latitude, longitude FROM photos
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
          AND (country IS NULL OR country = '')
          AND is_duplicate = 0
    """).fetchall()

    log.info(f"Found {len(rows)} photos needing geocoding")
    if not rows:
        conn.close()
        return

    geo = GeoNamesLookup(str(GEONAMES_FILE))
    cache = {}
    geocoded = 0

    for row in rows:
        key = (round(row['latitude'], 2), round(row['longitude'], 2))
        if key in cache:
            result = cache[key]
        else:
            result = geo.find_nearest(row['latitude'], row['longitude'])
            cache[key] = result

        if result and not dry_run:
            country = COUNTRY_CODES.get(result['cc'], result['cc'])
            cursor.execute("""
                UPDATE photos SET country = ?, country_code = ?, city = ?,
                    location_name = ? WHERE id = ?
            """, (country, result['cc'], result['name'],
                  f"{result['name']}, {country}", row['id']))
            geocoded += 1

        if geocoded % 10000 == 0 and geocoded > 0:
            conn.commit()
            log.info(f"  Geocoded {geocoded}/{len(rows)}...")

    conn.commit()
    log.info(f"Phase 5 complete: geocoded {geocoded}")
    conn.close()


# ============================================================
# Phase 6: AI Classification
# ============================================================

def _prepare_image(fpath):
    """Load and resize image to max 1024px, return base64 JPEG. Returns None on failure."""
    import base64, io
    from PIL import Image as PILImage

    MAX_DIM = 1024
    MAX_BYTES = 20 * 1024 * 1024

    try:
        file_size = os.path.getsize(fpath)
        # For small files, encode directly without opening via Pillow (faster)
        if file_size < 512 * 1024:
            with open(fpath, 'rb') as f:
                return base64.b64encode(f.read()).decode()

        with PILImage.open(fpath) as img:
            # Convert to RGB (handles RGBA, palette, etc.)
            if img.mode not in ('RGB', 'L'):
                img = img.convert('RGB')
            # Resize if needed
            w, h = img.size
            if w > MAX_DIM or h > MAX_DIM:
                img.thumbnail((MAX_DIM, MAX_DIM), PILImage.LANCZOS)
            buf = io.BytesIO()
            img.save(buf, format='JPEG', quality=85, optimize=True)
            return base64.b64encode(buf.getvalue()).decode()
    except Exception:
        # Fall back to raw read if Pillow fails (e.g. raw/DNG)
        try:
            if os.path.getsize(fpath) <= MAX_BYTES:
                with open(fpath, 'rb') as f:
                    return base64.b64encode(f.read()).decode()
        except Exception:
            pass
        return None


def classify(dry_run=False, max_items=None):
    """Classify photos using ollama llama3.2-vision with concurrent workers."""
    log.info("=== PHASE 6: AI Classification ===")
    import requests
    import threading
    from concurrent.futures import ThreadPoolExecutor, as_completed

    WORKERS = 5  # concurrent ollama requests

    conn = get_db()
    cursor = conn.cursor()

    limit = f"LIMIT {max_items}" if max_items else ""
    rows = cursor.execute(f"""
        SELECT id, file_path, best_date, latitude, longitude, city, country
        FROM photos
        WHERE ai_processed = 0 AND is_duplicate = 0 AND media_type = 'image'
        ORDER BY best_date {limit}
    """).fetchall()

    log.info(f"Found {len(rows)} images to classify")
    if dry_run:
        conn.close()
        return

    try:
        resp = requests.get(f"{OLLAMA_HOST}/api/version", timeout=5)
        log.info(f"Ollama version: {resp.json().get('version')}")
    except Exception as e:
        log.error(f"Ollama not available: {e}")
        conn.close()
        return

    db_lock = threading.Lock()
    processed = errors = 0
    counter_lock = threading.Lock()

    def classify_one(row):
        """Run in thread: resize image, call ollama, return (id, ai_data or None, missing)."""
        fpath = row['file_path']
        if not os.path.exists(fpath):
            return (row['id'], None, True)

        img_b64 = _prepare_image(fpath)
        if img_b64 is None:
            return (row['id'], None, False)

        ctx = []
        if row['best_date']:
            ctx.append(f"Date: {row['best_date']}")
        if row['city']:
            ctx.append(f"Location: {row['city']}, {row['country']}")

        prompt = f"""Analyze this photo. Respond ONLY with JSON:
{{"scene": "brief description (15 words max)", "type": "holiday|event|daily_life|landscape|portrait|food|architecture|nature|sport|celebration", "activity": "what people are doing (5 words max, or none)", "indoor_outdoor": "indoor|outdoor", "tags": ["tag1","tag2","tag3","tag4","tag5"], "is_holiday": true/false, "holiday_type": "beach|city_break|hiking|skiing|camping|road_trip|cultural|none"}}
{('Context: ' + '. '.join(ctx)) if ctx else ''}"""

        session = requests.Session()
        try:
            resp = session.post(f"{OLLAMA_HOST}/api/generate", json={
                "model": VISION_MODEL, "prompt": prompt, "images": [img_b64],
                "stream": False, "options": {"temperature": 0.1, "num_predict": 300}
            }, timeout=120)
            resp.raise_for_status()
            ai_data = _extract_json(resp.json().get('response', ''))
            return (row['id'], ai_data, False)
        except Exception as e:
            log.debug(f"classify_one failed for {fpath}: {e}")
            return (row['id'], None, False, True)  # 4th element = transient error

    total = len(rows)
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(classify_one, row): row for row in rows}
        for future in as_completed(futures):
            try:
                result = future.result()
                photo_id, ai_data, missing = result[0], result[1], result[2]
                transient_error = result[3] if len(result) > 3 else False
            except Exception as e:
                log.warning(f"Unexpected future error: {e}")
                with counter_lock:
                    errors += 1
                continue

            with db_lock:
                if missing:
                    # File not found — mark permanently skipped
                    cursor.execute("UPDATE photos SET ai_processed = -1 WHERE id = ?", (photo_id,))
                elif transient_error:
                    # Network/ollama error — leave ai_processed=0 so it can be retried
                    with counter_lock:
                        errors += 1
                    continue
                elif ai_data:
                    cursor.execute("""
                        UPDATE photos SET ai_description=?, ai_tags=?, ai_scene_type=?,
                            ai_activity=?, ai_indoor_outdoor=?,
                            ai_is_holiday=?, ai_holiday_type=?,
                            ai_processed=1
                        WHERE id=?
                    """, (ai_data.get('scene',''), json.dumps(ai_data.get('tags',[])),
                          ai_data.get('type',''), ai_data.get('activity',''),
                          ai_data.get('indoor_outdoor',''),
                          1 if ai_data.get('is_holiday') else 0,
                          ai_data.get('holiday_type',''),
                          photo_id))
                else:
                    # Parsed but empty response — mark done to avoid infinite retry
                    cursor.execute("UPDATE photos SET ai_processed=1 WHERE id=?", (photo_id,))
                    with counter_lock:
                        errors += 1

                with counter_lock:
                    processed += 1
                    if processed % 10 == 0:
                        conn.commit()
                        log.info(f"  AI: {processed}/{total}...")

    conn.commit()
    log.info(f"Phase 6 complete: {processed} classified, {errors} errors")
    conn.close()


def _extract_json(text):
    text = text.strip()
    if text.startswith('```'):
        text = re.sub(r'^```\w*\n?', '', text)
        text = re.sub(r'```\s*$', '', text)
    start = text.find('{')
    if start == -1:
        return None
    depth = 0
    for i, c in enumerate(text[start:], start):
        if c == '{': depth += 1
        elif c == '}':
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[start:i+1])
                except json.JSONDecodeError:
                    return None
    return None


# ============================================================
# Phase 7: Auto-group into Albums
# ============================================================

def group_albums(dry_run=False):
    """Group photos into holiday/event albums: 'Austria 2020', 'Japan 2024', etc."""
    log.info("=== PHASE 7: Auto-group into Albums ===")
    conn = get_db()
    cursor = conn.cursor()

    # Step 1: Import existing Google albums (from metadata.json in dirs)
    log.info("Importing existing Google albums...")
    existing = 0
    scan_dirs = [d for d in [PRIMARY_DIR, SECONDARY_DIR] if d.name and d.is_dir()]
    for source_dir in scan_dirs:
        for entry in os.listdir(str(source_dir)):
            meta_file = source_dir / entry / 'metadata.json'
            if not meta_file.exists():
                continue
            try:
                with open(meta_file) as f:
                    meta = json.load(f)
                album_name = meta.get('title', entry)
                if not album_name:
                    continue
                cursor.execute("""
                    INSERT OR IGNORE INTO albums (name, album_type, source) VALUES (?, 'existing', 'google')
                """, (album_name,))
                aid = cursor.execute("SELECT id FROM albums WHERE name=?", (album_name,)).fetchone()
                if aid:
                    cursor.execute("""
                        UPDATE photos SET album_name=?, album_id=?
                        WHERE parent_dir=? AND is_duplicate=0 AND album_id IS NULL
                    """, (album_name, aid['id'], entry))
                    existing += 1
            except (json.JSONDecodeError, KeyError):
                continue
    conn.commit()
    log.info(f"Imported {existing} existing Google albums")

    # Step 2: Cluster unassigned photos by date + location
    log.info("Clustering unassigned photos into trips/events...")
    unassigned = cursor.execute("""
        SELECT id, best_date, latitude, longitude, country, country_code, city,
               ai_scene_type, ai_tags
        FROM photos
        WHERE album_id IS NULL AND is_duplicate = 0 AND best_date IS NOT NULL
        ORDER BY best_date
    """).fetchall()
    log.info(f"{len(unassigned)} unassigned photos with dates")

    clusters = []
    current = []

    for photo in unassigned:
        if not current:
            current.append(photo)
            continue

        last = current[-1]
        same = False
        try:
            last_dt = datetime.fromisoformat(last['best_date'][:19])
            this_dt = datetime.fromisoformat(photo['best_date'][:19])
            gap_days = abs((this_dt - last_dt).days)

            if gap_days <= 2:
                # Within 2 days — check if same country
                if photo['country'] and last['country']:
                    same = (photo['country'] == last['country'])
                elif photo['latitude'] and last['latitude']:
                    d = GeoNamesLookup._haversine(
                        last['latitude'], last['longitude'],
                        photo['latitude'], photo['longitude'])
                    same = d < 150
                else:
                    same = gap_days <= 1
            elif gap_days <= 5 and photo['country'] and last['country']:
                # 2-5 day gap: only group if same foreign country (not home country UK)
                same = (photo['country'] == last['country'] and
                        photo['country_code'] not in ('GB', None))
        except (ValueError, TypeError):
            pass

        if same:
            current.append(photo)
        else:
            if len(current) >= 3:
                clusters.append(current)
            current = [photo]

    if len(current) >= 3:
        clusters.append(current)

    log.info(f"Found {len(clusters)} trip/event clusters")

    # Step 3: Name and create albums
    created = 0
    for cluster in clusters:
        name = _build_album_name(cluster)
        if not name:
            continue

        dates = [p['best_date'] for p in cluster if p['best_date']]
        start_date = min(dates) if dates else None
        end_date = max(dates) if dates else None
        countries = [p['country'] for p in cluster if p['country']]
        cities = [p['city'] for p in cluster if p['city']]
        primary_country = Counter(countries).most_common(1)[0][0] if countries else None
        primary_city = Counter(cities).most_common(1)[0][0] if cities else None

        # Determine type
        holiday_tags = {'holiday', 'landscape', 'nature', 'hiking', 'beach'}
        ai_types = [p['ai_scene_type'] for p in cluster if p['ai_scene_type']]
        is_holiday = any(t in holiday_tags for t in ai_types) or (
            primary_country and primary_country != 'United Kingdom')
        album_type = 'holiday' if is_holiday else 'event'

        # Ensure unique
        existing_check = cursor.execute("SELECT 1 FROM albums WHERE name=?", (name,)).fetchone()
        if existing_check:
            name = f"{name} ({start_date[:10]})" if start_date else f"{name} ({created})"

        if not dry_run:
            cursor.execute("""
                INSERT OR IGNORE INTO albums (name, start_date, end_date, country, city,
                    photo_count, album_type, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'auto')
            """, (name, start_date, end_date, primary_country, primary_city,
                  len(cluster), album_type))
            aid = cursor.execute("SELECT id FROM albums WHERE name=?", (name,)).fetchone()
            if aid:
                for p in cluster:
                    cursor.execute("UPDATE photos SET album_name=?, album_id=? WHERE id=?",
                                   (name, aid['id'], p['id']))

        created += 1
        if created % 50 == 0:
            conn.commit()
        if len(cluster) >= 10:
            log.info(f"  Album: '{name}' ({len(cluster)} photos, {album_type})")

    conn.commit()

    stats = cursor.execute("""
        SELECT COUNT(DISTINCT album_id) as albums,
               SUM(CASE WHEN album_id IS NOT NULL THEN 1 ELSE 0 END) as assigned,
               SUM(CASE WHEN album_id IS NULL AND is_duplicate=0 THEN 1 ELSE 0 END) as unassigned
        FROM photos WHERE is_duplicate=0
    """).fetchone()

    log.info(f"""
    Phase 7 complete:
    Albums:      {stats['albums']}
    Assigned:    {stats['assigned']}
    Unassigned:  {stats['unassigned']}
    New albums:  {created}
    """)
    conn.close()


def _build_album_name(cluster):
    """Generate album name like 'Austria 2020' or 'Japan 2024 (14 days)'."""
    countries = [p['country'] for p in cluster if p['country']]
    cities = [p['city'] for p in cluster if p['city']]
    dates = [p['best_date'] for p in cluster if p['best_date']]
    if not dates:
        return None

    try:
        start = datetime.fromisoformat(dates[0][:19])
        end = datetime.fromisoformat(dates[-1][:19])
        year = start.strftime('%Y')
        month = start.strftime('%B')
        duration = (end - start).days
    except ValueError:
        return None

    primary_city = Counter(cities).most_common(1)[0][0] if cities else None
    primary_country = Counter(countries).most_common(1)[0][0] if countries else None
    unique_countries = list(set(countries))

    # Build name
    if len(unique_countries) > 1 and len(unique_countries) <= 3:
        location = ' & '.join(sorted(unique_countries))
    elif primary_country and primary_country != 'United Kingdom':
        # Foreign trip: use country (or city if notable)
        if primary_city and len(set(cities)) <= 3:
            location = primary_city
        else:
            location = primary_country
    elif primary_city:
        location = primary_city
    else:
        location = month

    name = f"{location} {year}"

    if duration > 7 and primary_country != 'United Kingdom':
        name += f" ({duration} days)"

    return name


# ============================================================
# Phase 8: Organize into Final Directory
# ============================================================

def export_albums(dry_run=False):
    """Organize unique photos into flat Event Album Name/ dirs at the final output dir."""
    log.info(f"=== PHASE 8: Organize into {FINAL_DIR} ===")
    conn = get_db()
    cursor = conn.cursor()

    # Clear existing output (symlinks only — safe, no data loss)
    if not dry_run and FINAL_DIR.exists():
        log.info("Clearing previous output (symlinks)...")
        cleared = 0
        for item in FINAL_DIR.rglob('*'):
            if item.is_symlink():
                item.unlink()
                cleared += 1
        # Remove empty dirs
        for item in sorted(FINAL_DIR.rglob('*'), reverse=True):
            if item.is_dir():
                try:
                    item.rmdir()
                except OSError:
                    pass
        log.info(f"Cleared {cleared} symlinks from previous run")

    FINAL_DIR.mkdir(parents=True, exist_ok=True)

    # Photos with albums — flat structure: Album Name/filename
    photos = cursor.execute("""
        SELECT id, file_path, filename, album_name, best_date
        FROM photos WHERE is_duplicate=0 AND album_name IS NOT NULL
        ORDER BY album_name, best_date
    """).fetchall()
    log.info(f"Organizing {len(photos)} album photos + unassigned into event folders")

    moved = 0
    for photo in photos:
        if not os.path.exists(photo['file_path']):
            continue
        album = re.sub(r'[<>:"/\\|?*]', '_', photo['album_name']).strip('. ')
        dest_dir = FINAL_DIR / album
        if not dry_run:
            dest_dir.mkdir(parents=True, exist_ok=True)
        dest = _unique_path(dest_dir, photo['filename'])
        if not dry_run:
            _create_link(photo['file_path'], str(dest))
        moved += 1
        if moved % 5000 == 0:
            log.info(f"  Organized {moved}...")

    # Unassigned — group by year/month as fallback
    unassigned = cursor.execute("""
        SELECT id, file_path, filename, best_date
        FROM photos WHERE is_duplicate=0 AND album_name IS NULL
    """).fetchall()

    for photo in unassigned:
        if not os.path.exists(photo['file_path']):
            continue
        year = _extract_year(photo['best_date'])
        dest_dir = FINAL_DIR / f'Unsorted {year}'
        if not dry_run:
            dest_dir.mkdir(parents=True, exist_ok=True)
        dest = _unique_path(dest_dir, photo['filename'])
        if not dry_run:
            _create_link(photo['file_path'], str(dest))
        moved += 1

    log.info(f"Phase 8 complete: organized {moved} files into {FINAL_DIR}")
    conn.close()


def _extract_year(date_str):
    if date_str:
        try:
            return datetime.fromisoformat(date_str[:19]).strftime('%Y')
        except ValueError:
            pass
    return 'Undated'


def _create_link(src, dst):
    """Hardlink if same filesystem, symlink if cross-filesystem, copy as last resort."""
    try:
        os.link(src, dst)
    except OSError:
        try:
            os.symlink(src, dst)
        except OSError:
            shutil.copy2(src, dst)


def _unique_path(dest_dir, filename):
    dest = dest_dir / filename
    if not dest.exists() and not dest.is_symlink():
        return dest
    base, ext = os.path.splitext(filename)
    c = 1
    while dest.exists() or dest.is_symlink():
        dest = dest_dir / f"{base}_{c}{ext}"
        c += 1
    return dest


# ============================================================
# Phase 9: Upload Prep
# ============================================================

def prep_upload(dry_run=False):
    log.info("=== PHASE 9: Upload Preparation ===")
    conn = get_db()
    cursor = conn.cursor()

    albums = cursor.execute("""
        SELECT a.name, a.start_date, a.end_date, a.country, a.city,
               a.album_type, a.source, COUNT(p.id) as cnt
        FROM albums a LEFT JOIN photos p ON p.album_id=a.id AND p.is_duplicate=0
        GROUP BY a.id ORDER BY a.start_date
    """).fetchall()

    manifest = {'generated': datetime.now().isoformat(), 'albums': []}
    for a in albums:
        manifest['albums'].append({
            'name': a['name'], 'start': a['start_date'], 'end': a['end_date'],
            'country': a['country'], 'city': a['city'],
            'count': a['cnt'], 'type': a['album_type'], 'source': a['source']
        })

    with open(PIPELINE_DIR / 'manifest.json', 'w') as f:
        json.dump(manifest, f, indent=2)

    stats = cursor.execute("""
        SELECT COUNT(*) as total,
            SUM(CASE WHEN is_duplicate=0 THEN 1 ELSE 0 END) as uniq,
            SUM(CASE WHEN is_duplicate=1 THEN 1 ELSE 0 END) as dupes,
            SUM(CASE WHEN album_id IS NOT NULL AND is_duplicate=0 THEN 1 ELSE 0 END) as in_albums,
            SUM(CASE WHEN ai_processed=1 THEN 1 ELSE 0 END) as ai,
            SUM(CASE WHEN country IS NOT NULL AND is_duplicate=0 THEN 1 ELSE 0 END) as geo,
            SUM(CASE WHEN source='primary' THEN 1 ELSE 0 END) as from_ext,
            SUM(CASE WHEN source='secondary' THEN 1 ELSE 0 END) as from_int
        FROM photos
    """).fetchone()

    log.info(f"""
    =========================================
    FINAL SUMMARY
    =========================================
    Total catalogued:   {stats['total']}
      From external:    {stats['from_ext']}
      From internal:    {stats['from_int']}
    Unique files:       {stats['uniq']}
    Duplicates:         {stats['dupes']}
    In albums:          {stats['in_albums']}
    AI classified:      {stats['ai']}
    Geocoded:           {stats['geo']}
    Total albums:       {len(albums)}
    Output dir:         {FINAL_DIR}
    =========================================
    """)
    conn.close()


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description='Google Photos Takeout Organiser',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Steps (run in order):
  scan            Catalogue all photos from source directories
  merge-sidecars  Merge Google JSON metadata into photo records
  deduplicate     Remove duplicate photos (hash-based)
  fix-dates       Fix broken/missing timestamps
  geocode         Reverse-geocode photos using GeoNames
  classify        AI-tag every photo using a vision model (slow)
  group-albums    Cluster photos into event albums
  export          Organise albums into the output directory
  prep-upload     Generate rclone upload scripts
  all             Run all steps in order

Examples:
  python3 pipeline.py --step scan
  python3 pipeline.py --step classify --max-items 100
  python3 pipeline.py --step all --dry-run
        """
    )
    parser.add_argument(
        '--step', required=True,
        metavar='STEP',
        help='Step name (e.g. scan, classify, export) or "all"'
    )
    parser.add_argument('--dry-run', action='store_true', help='Preview changes without writing anything')
    parser.add_argument('--max-items', type=int, default=None, help='Limit items processed (useful for testing)')
    args = parser.parse_args()

    # Validate required configuration
    errors = []
    if not PRIMARY_DIR.name:
        errors.append("PRIMARY_DIR is not set (set via environment variable or .env file)")
    elif not PRIMARY_DIR.is_dir():
        errors.append(f"PRIMARY_DIR does not exist: {PRIMARY_DIR}")
    if not FINAL_DIR.name:
        errors.append("FINAL_DIR is not set (set via environment variable or .env file)")
    if errors:
        for e in errors:
            log.error(f"Configuration error: {e}")
        log.error("See .env.example for setup instructions.")
        sys.exit(1)

    PIPELINE_DIR.mkdir(parents=True, exist_ok=True)

    # Named steps — also accept legacy numeric aliases (1-9)
    steps = {
        'scan':           scan,
        'merge-sidecars': merge_sidecars,
        'deduplicate':    deduplicate,
        'fix-dates':      fix_dates,
        'geocode':        geocode,
        'classify':       lambda dr: classify(dr, max_items=args.max_items),
        'group-albums':   group_albums,
        'export':         export_albums,
        'prep-upload':    prep_upload,
    }
    numeric_aliases = {
        '1': 'scan', '2': 'merge-sidecars', '3': 'deduplicate',
        '4': 'fix-dates', '5': 'geocode', '6': 'classify',
        '7': 'group-albums', '8': 'export', '9': 'prep-upload',
    }

    step = numeric_aliases.get(args.step, args.step)

    if step == 'all':
        for name, fn in steps.items():
            log.info(f"\n{'='*60}\n{name}\n{'='*60}")
            fn(args.dry_run)
    elif step in steps:
        steps[step](args.dry_run)
    else:
        print(f"Unknown step: {args.step!r}\nRun with --help to see available steps.")
        sys.exit(1)


if __name__ == '__main__':
    main()
