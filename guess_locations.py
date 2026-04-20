#!/usr/bin/env python3
"""
guess_locations.py — Infer GPS location for photos that have no EXIF coordinates.

Four strategies, applied in confidence order:

  temporal   Interpolate between GPS-tagged photos close in time.
             Sub-30 min same album → high. Sub-6 hr same album → medium.

  album      Majority-vote: if ≥50% of an album's photos have GPS,
             assign the album's median lat/lon to the rest.
             ≥70% coverage → high, 50-70% → medium.

  similar    Perceptual-hash index of every GPS-tagged photo.
             Photos with Hamming distance ≤10 bits inherit the location.
             Covers burst shots, edited copies, rescans of the same scene.

  vision     Last resort: Ollama llama3.2-vision looks at the photo and
             guesses country + city from architecture, signs, vegetation,
             vehicles, and any visible text.

  apply      Write accepted guesses back to the photos table (and optionally
             GPS EXIF). High-confidence guesses apply automatically;
             medium/low require --apply-all.

  report     Print a summary table of every photo and its guess status.

  all        Run temporal → album → similar → vision → apply → report.

Usage:
    python3 guess_locations.py                        # run everything
    python3 guess_locations.py --step report          # just show status
    python3 guess_locations.py --step all --apply-all # apply every guess
    python3 guess_locations.py --step vision --dry-run

Dependencies:
    pip install imagehash pillow numpy requests
    pip install piexif          # optional: write GPS back into EXIF files

Environment (all have sensible defaults, no setup needed):
    PIPELINE_DB    photos.db from pipeline.py
    GEONAMES_FILE  allCountries.txt for reverse geocoding
    VISION_MODEL   Ollama model  (default: llama3.2-vision:latest)
    OLLAMA_URL     Ollama server (default: http://localhost:11434)
    MAX_WORKERS    Parallel Ollama requests (default: 3)
"""

import argparse
import base64
import io
import json
import logging
import math
import os
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
from PIL import Image

try:
    import imagehash
    HAS_IMAGEHASH = True
except ImportError:
    HAS_IMAGEHASH = False
    print("WARNING: imagehash not installed. Run: pip install imagehash")
    print("         The 'similar' strategy will be skipped.")

try:
    import piexif
    HAS_PIEXIF = True
except ImportError:
    HAS_PIEXIF = False

try:
    import requests as _requests
    _USE_REQUESTS = True
except ImportError:
    import urllib.request as _urllib
    _USE_REQUESTS = False

# ── Config ─────────────────────────────────────────────────────────────────────
PIPELINE_DB   = Path(os.environ.get("PIPELINE_DB",   str(Path(__file__).parent / "photos.db")))
GEONAMES_FILE = Path(os.environ.get("GEONAMES_FILE", "/run/media/elgan/evo/Pictures/photo_pipeline/allCountries.txt"))
VISION_MODEL  = os.environ.get("VISION_MODEL", "llama3.2-vision:latest")
OLLAMA_URL    = os.environ.get("OLLAMA_URL",   "http://localhost:11434")
MAX_WORKERS   = int(os.environ.get("MAX_WORKERS", "3"))

# Confidence thresholds for auto-apply
AUTO_APPLY    = {"high"}          # always applied
REVIEW_APPLY  = {"medium", "low"} # need --apply-all

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── DB ─────────────────────────────────────────────────────────────────────────
def open_db() -> sqlite3.Connection:
    if not PIPELINE_DB.exists():
        raise FileNotFoundError(f"Pipeline DB not found: {PIPELINE_DB}")
    conn = sqlite3.connect(PIPELINE_DB)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS location_guesses (
            id           INTEGER PRIMARY KEY,
            photo_id     INTEGER,
            file_path    TEXT UNIQUE,
            strategy     TEXT,
            confidence   TEXT,
            latitude     REAL,
            longitude    REAL,
            country      TEXT,
            country_code TEXT,
            city         TEXT,
            notes        TEXT,
            applied      INTEGER DEFAULT 0,
            guessed_at   TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_lg_photo ON location_guesses(photo_id);
        CREATE INDEX IF NOT EXISTS idx_lg_conf  ON location_guesses(confidence);
    """)
    conn.commit()
    return conn


def get_ungeotagged(conn) -> list:
    """Photos with no GPS coordinates and no existing high-confidence guess."""
    return conn.execute("""
        SELECT p.id, p.file_path, p.filename, p.best_date, p.album_name, p.album_id
        FROM photos p
        LEFT JOIN location_guesses g ON g.photo_id = p.id
        WHERE p.latitude IS NULL
          AND p.is_duplicate = 0
          AND (g.id IS NULL OR g.confidence = 'low')
        ORDER BY p.best_date
    """).fetchall()


def get_geotagged(conn) -> list:
    """All photos that have GPS."""
    return conn.execute("""
        SELECT id, file_path, filename, best_date, album_id, album_name,
               latitude, longitude, country, country_code, city
        FROM photos
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
          AND is_duplicate = 0
        ORDER BY best_date
    """).fetchall()


# ── GeoNames reverse geocoder (self-contained, no import from pipeline.py) ────
class GeoNames:
    def __init__(self, path: Path):
        self.places = []
        self._name_index = {}  # lowercased name → place
        if path.exists():
            self._load(path)
        else:
            log.warning(f"GeoNames file not found: {path}  (reverse geocoding disabled)")

    def _load(self, path: Path):
        log.info(f"Loading GeoNames from {path}...")
        count = 0
        with open(path, encoding="utf-8") as f:
            for line in f:
                parts = line.split("\t")
                if len(parts) < 15 or parts[6] not in ("P", "A"):
                    continue
                try:
                    pop = int(parts[14]) if parts[14] else 0
                    if pop < 500:
                        continue
                    place = {
                        "name": parts[1], "lat": float(parts[4]), "lon": float(parts[5]),
                        "cc": parts[8], "pop": pop,
                    }
                    self.places.append(place)
                    key = parts[1].lower()
                    if key not in self._name_index or self._name_index[key]["pop"] < pop:
                        self._name_index[key] = place
                    count += 1
                except (ValueError, IndexError):
                    continue
        self.places.sort(key=lambda x: -x["pop"])
        log.info(f"  Loaded {count:,} places")

    def reverse(self, lat: float, lon: float, max_km: float = 80) -> dict | None:
        best, best_d = None, max_km
        for p in self.places:
            d = _haversine(lat, lon, p["lat"], p["lon"])
            if d < best_d:
                best_d, best = d, p
                if d < 1:
                    break
        return best

    def lookup(self, city: str, country_code: str = None) -> dict | None:
        """Find a place by city name (case-insensitive, fuzzy)."""
        key = city.lower().strip()
        # Exact match
        if key in self._name_index:
            p = self._name_index[key]
            if country_code is None or p["cc"].upper() == country_code.upper():
                return p
        # Partial match against top places
        for p in self.places[:50000]:
            if key in p["name"].lower() or p["name"].lower() in key:
                if country_code is None or p["cc"].upper() == country_code.upper():
                    return p
        return None


def _haversine(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(min(1.0, math.sqrt(a)))


_COUNTRY_CODES = {
    "USA": "US", "UNITED STATES": "US", "UK": "GB", "UNITED KINGDOM": "GB",
    "FRANCE": "FR", "GERMANY": "DE", "SPAIN": "ES", "ITALY": "IT",
    "JAPAN": "JP", "AUSTRALIA": "AU", "NEW ZEALAND": "NZ", "CANADA": "CA",
    "NETHERLANDS": "NL", "BELGIUM": "BE", "AUSTRIA": "AT", "SWITZERLAND": "CH",
    "SWEDEN": "SE", "NORWAY": "NO", "DENMARK": "DK", "FINLAND": "FI",
    "PORTUGAL": "PT", "IRELAND": "IE", "GREECE": "GR", "TURKEY": "TR",
    "THAILAND": "TH", "VIETNAM": "VN", "INDONESIA": "ID", "MALAYSIA": "MY",
    "SINGAPORE": "SG", "PHILIPPINES": "PH", "INDIA": "IN", "CHINA": "CN",
    "SOUTH KOREA": "KR", "TAIWAN": "TW", "HONG KONG": "HK", "MEXICO": "MX",
    "BRAZIL": "BR", "ARGENTINA": "AR", "CHILE": "CL", "COLOMBIA": "CO",
    "PERU": "PE", "SOUTH AFRICA": "ZA", "EGYPT": "EG", "MOROCCO": "MA",
    "KENYA": "KE", "TANZANIA": "TZ", "ICELAND": "IS", "CROATIA": "HR",
    "CZECH REPUBLIC": "CZ", "CZECHIA": "CZ", "POLAND": "PL", "HUNGARY": "HU",
    "ROMANIA": "RO", "SERBIA": "RS", "MONTENEGRO": "ME", "SLOVAKIA": "SK",
    "SLOVENIA": "SI", "ESTONIA": "EE", "LATVIA": "LV", "LITHUANIA": "LT",
    "MALTA": "MT", "CYPRUS": "CY", "BULGARIA": "BG", "ALBANIA": "AL",
    "NORTH MACEDONIA": "MK", "MOLDOVA": "MD", "UKRAINE": "UA",
}


def country_to_code(name: str) -> str | None:
    return _COUNTRY_CODES.get(name.upper().strip())


# ── Helpers ────────────────────────────────────────────────────────────────────
def parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:19], fmt)
        except ValueError:
            continue
    return None


def img_to_b64(path: Path, max_dim: int = 512) -> str:
    img = Image.open(path).convert("RGB")
    img.thumbnail((max_dim, max_dim), Image.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=82)
    return base64.b64encode(buf.getvalue()).decode()


def ollama(prompt: str, image_b64: str) -> str:
    payload = json.dumps({
        "model": VISION_MODEL, "prompt": prompt,
        "images": [image_b64], "stream": False,
        "options": {"temperature": 0.1},
    }).encode()
    url = f"{OLLAMA_URL}/api/generate"
    if _USE_REQUESTS:
        r = _requests.post(url, data=payload,
                           headers={"Content-Type": "application/json"}, timeout=90)
        r.raise_for_status()
        return r.json()["response"]
    req = _urllib.Request(url, data=payload, headers={"Content-Type": "application/json"})
    with _urllib.urlopen(req, timeout=90) as resp:
        return json.loads(resp.read())["response"]


def parse_json(text: str) -> dict:
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group())
        except json.JSONDecodeError:
            pass
    return {}


def save_guess(conn, photo_id: int, file_path: str, strategy: str, confidence: str,
               lat: float, lon: float, country: str, cc: str, city: str, notes: str):
    conn.execute("""
        INSERT OR REPLACE INTO location_guesses
          (photo_id, file_path, strategy, confidence, latitude, longitude,
           country, country_code, city, notes, applied, guessed_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,0,datetime('now'))
    """, (photo_id, file_path, strategy, confidence, lat, lon, country, cc, city, notes))
    conn.commit()


# ── Strategy 1: Temporal interpolation ────────────────────────────────────────
def strategy_temporal(conn, ungeotagged: list, geotagged: list, dry_run: bool):
    log.info(f"Temporal: checking {len(ungeotagged)} photos against {len(geotagged)} GPS refs...")

    # Index geotagged by datetime
    tagged = []
    for r in geotagged:
        dt = parse_dt(r["best_date"])
        if dt:
            tagged.append((dt, r))
    tagged.sort(key=lambda x: x[0])
    if not tagged:
        log.info("  No GPS-tagged photos with parseable dates")
        return

    tagged_dts = [t[0] for t in tagged]
    found = 0

    for row in ungeotagged:
        dt = parse_dt(row["best_date"])
        if not dt:
            continue

        # Find nearest GPS photos before and after
        import bisect
        idx = bisect.bisect_left(tagged_dts, dt)

        candidates = []
        if 0 < idx <= len(tagged):
            candidates.append(tagged[idx - 1])
        if idx < len(tagged):
            candidates.append(tagged[idx])

        best = None
        best_delta = None
        for cdt, cref in candidates:
            delta = abs((dt - cdt).total_seconds()) / 3600  # hours
            if best_delta is None or delta < best_delta:
                best_delta = delta
                best = (cdt, cref, delta)

        if not best:
            continue

        _, ref, delta_h = best
        same_album = (row["album_id"] and row["album_id"] == ref["album_id"])

        # Confidence by time gap
        if delta_h <= 0.5:
            confidence = "high"
        elif delta_h <= 6 and same_album:
            confidence = "high"
        elif delta_h <= 24 and same_album:
            confidence = "medium"
        elif delta_h <= 6:
            confidence = "medium"
        else:
            continue  # too far apart

        notes = f"nearest GPS photo {delta_h:.1f}h away ({ref['filename']}), same_album={same_album}"
        log.debug(f"  {row['filename']}: {confidence} ({notes})")

        if not dry_run:
            save_guess(conn, row["id"], row["file_path"], "temporal", confidence,
                       ref["latitude"], ref["longitude"],
                       ref["country"] or "", ref["country_code"] or "", ref["city"] or "",
                       notes)
        found += 1

    log.info(f"  Temporal: {found} guesses")


# ── Strategy 2: Album majority vote ───────────────────────────────────────────
def strategy_album(conn, ungeotagged: list, dry_run: bool):
    log.info("Album: computing per-album GPS coverage...")

    # Build album GPS clusters
    rows = conn.execute("""
        SELECT album_id, album_name, latitude, longitude, country, country_code, city
        FROM photos
        WHERE latitude IS NOT NULL AND is_duplicate=0 AND album_id IS NOT NULL
    """).fetchall()

    albums: dict = {}
    for r in rows:
        aid = r["album_id"]
        if aid not in albums:
            albums[aid] = {"lats": [], "lons": [], "country": r["country"],
                           "cc": r["country_code"], "city": r["city"], "name": r["album_name"]}
        albums[aid]["lats"].append(r["latitude"])
        albums[aid]["lons"].append(r["longitude"])
        if r["city"] and not albums[aid]["city"]:
            albums[aid]["city"] = r["city"]
        if r["country"] and not albums[aid]["country"]:
            albums[aid]["country"] = r["country"]

    # Total photos per album (for coverage %)
    totals = {}
    for r in conn.execute("SELECT album_id, COUNT(*) as n FROM photos WHERE is_duplicate=0 AND album_id IS NOT NULL GROUP BY album_id"):
        totals[r["album_id"]] = r["n"]

    found = 0
    skipped_low_cov = 0

    for row in ungeotagged:
        aid = row["album_id"]
        if not aid or aid not in albums:
            continue

        a       = albums[aid]
        total   = totals.get(aid, 1)
        gps_n   = len(a["lats"])
        coverage = gps_n / total

        if coverage < 0.50:
            skipped_low_cov += 1
            continue

        # Median cluster to avoid outliers
        lat = float(np.median(a["lats"]))
        lon = float(np.median(a["lons"]))
        confidence = "high" if coverage >= 0.70 else "medium"

        notes = f"album '{a['name']}' {coverage:.0%} GPS coverage ({gps_n}/{total} photos)"
        if not dry_run:
            save_guess(conn, row["id"], row["file_path"], "album", confidence,
                       lat, lon, a["country"] or "", a["cc"] or "", a["city"] or "", notes)
        found += 1

    log.info(f"  Album: {found} guesses  ({skipped_low_cov} skipped, album coverage <50%)")


# ── Strategy 3: Perceptual hash similarity ────────────────────────────────────
def strategy_similar(conn, ungeotagged: list, geotagged: list, dry_run: bool):
    if not HAS_IMAGEHASH:
        log.info("Similar: skipped (imagehash not installed)")
        return

    log.info(f"Similar: building pHash index from {len(geotagged)} GPS-tagged photos...")

    # Build hash → GPS index (skip already-guessed photos to keep memory down)
    index = []  # list of (hash, lat, lon, country, cc, city, filename)
    built = 0
    for ref in geotagged:
        path = Path(ref["file_path"])
        if not path.exists():
            continue
        try:
            h = imagehash.phash(Image.open(path), hash_size=8)
            index.append((h, ref["latitude"], ref["longitude"],
                          ref["country"] or "", ref["country_code"] or "",
                          ref["city"] or "", ref["filename"]))
            built += 1
        except Exception:
            continue
        if built % 1000 == 0:
            log.info(f"  Indexed {built}/{len(geotagged)}...")

    log.info(f"  Index built: {built} photos")
    if not index:
        return

    found = 0
    for row in ungeotagged:
        # Skip if already has a high-confidence guess
        existing = conn.execute(
            "SELECT confidence FROM location_guesses WHERE photo_id=?", (row["id"],)
        ).fetchone()
        if existing and existing["confidence"] == "high":
            continue

        path = Path(row["file_path"])
        if not path.exists():
            continue
        try:
            h = imagehash.phash(Image.open(path), hash_size=8)
        except Exception:
            continue

        best_dist = 999
        best_ref  = None
        for ref_h, *rest in index:
            dist = h - ref_h
            if dist < best_dist:
                best_dist = dist
                best_ref  = rest

        if best_dist > 12:  # Hamming distance threshold (out of 64 bits)
            continue

        lat, lon, country, cc, city, ref_name = best_ref
        confidence = "high" if best_dist <= 4 else "medium" if best_dist <= 10 else "low"
        notes = f"pHash Hamming={best_dist} to '{ref_name}'"

        if not dry_run:
            save_guess(conn, row["id"], row["file_path"], "similar", confidence,
                       lat, lon, country, cc, city, notes)
        found += 1

    log.info(f"  Similar: {found} guesses")


# ── Strategy 4: Vision AI ──────────────────────────────────────────────────────
_VISION_PROMPT = """\
Look at this photo carefully. Based on architecture, landscape, vegetation, \
signage text, vehicle types, clothing, and any other visual clues, \
where was this photo most likely taken?

Reply ONLY with valid JSON — no extra text:
{"country": "<country name>", "city": "<city or region, empty string if unsure>", \
"confidence": "<high|medium|low>", \
"clues": "<one sentence: what visual evidence led you here>"}

If you genuinely cannot tell, set confidence to "low" and your best guess for country."""


def _vision_one(row: sqlite3.Row, geo: GeoNames) -> dict | None:
    path = Path(row["file_path"])
    if not path.exists():
        return None
    try:
        b64      = img_to_b64(path)
        response = ollama(_VISION_PROMPT, b64)
        data     = parse_json(response)
        if not data.get("country"):
            return None

        country    = data["country"].strip()
        city       = data.get("city", "").strip()
        confidence = data.get("confidence", "low")
        clues      = data.get("clues", "")
        cc         = country_to_code(country) or ""

        # Try to resolve to coordinates via GeoNames
        lat, lon = None, None
        if geo.places:
            place = geo.lookup(city, cc) if city else None
            if place:
                lat, lon = place["lat"], place["lon"]
                if not cc:
                    cc = place["cc"]
            else:
                # Fall back to country centroid
                place = geo.lookup(country)
                if place:
                    lat, lon = place["lat"], place["lon"]
                    if not cc:
                        cc = place["cc"]

        if lat is None:
            confidence = "low"  # can't geocode → downgrade

        return {
            "photo_id":  row["id"],
            "file_path": row["file_path"],
            "lat": lat, "lon": lon,
            "country": country, "cc": cc, "city": city,
            "confidence": confidence,
            "notes": f"vision: {clues}",
        }
    except Exception as e:
        log.debug(f"Vision failed {row['filename']}: {e}")
        return None


def strategy_vision(conn, ungeotagged: list, geo: GeoNames, dry_run: bool):
    # Only run on photos still without any guess
    needs_vision = []
    for row in ungeotagged:
        existing = conn.execute(
            "SELECT id FROM location_guesses WHERE photo_id=?", (row["id"],)
        ).fetchone()
        if not existing:
            needs_vision.append(row)

    log.info(f"Vision: {len(needs_vision)} photos have no guess yet, querying Ollama...")
    if not needs_vision:
        return

    geo_loaded = bool(geo.places)
    if not geo_loaded:
        log.warning("  GeoNames not loaded — coordinates will be NULL for vision guesses")

    found = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {pool.submit(_vision_one, row, geo): row for row in needs_vision}
        done = 0
        for fut in as_completed(futs):
            done += 1
            result = fut.result()
            if done % 50 == 0:
                log.info(f"  Vision: {done}/{len(needs_vision)}...")
            if not result:
                continue
            if not dry_run:
                save_guess(conn, result["photo_id"], result["file_path"],
                           "vision", result["confidence"],
                           result["lat"], result["lon"],
                           result["country"], result["cc"], result["city"],
                           result["notes"])
            found += 1

    log.info(f"  Vision: {found} guesses")


# ── Apply step ─────────────────────────────────────────────────────────────────
def _write_exif_gps(path: Path, lat: float, lon: float):
    """Write GPS coordinates into EXIF (requires piexif)."""
    if not HAS_PIEXIF or lat is None or lon is None:
        return
    try:
        def to_dms(deg):
            d = int(abs(deg))
            m = int((abs(deg) - d) * 60)
            s = round(((abs(deg) - d) * 60 - m) * 60 * 10000)
            return ((d, 1), (m, 1), (s, 10000))

        exif_dict = piexif.load(str(path))
        gps_ifd = {
            piexif.GPSIFD.GPSVersionID:      (2, 2, 0, 0),
            piexif.GPSIFD.GPSLatitudeRef:    b"N" if lat >= 0 else b"S",
            piexif.GPSIFD.GPSLatitude:       to_dms(lat),
            piexif.GPSIFD.GPSLongitudeRef:   b"E" if lon >= 0 else b"W",
            piexif.GPSIFD.GPSLongitude:      to_dms(lon),
        }
        exif_dict["GPS"] = gps_ifd
        piexif.insert(piexif.dump(exif_dict), str(path))
    except Exception as e:
        log.debug(f"EXIF write failed {path.name}: {e}")


def apply_guesses(conn, apply_all: bool = False, dry_run: bool = False, write_exif: bool = False):
    levels = AUTO_APPLY | REVIEW_APPLY if apply_all else AUTO_APPLY
    placeholders = ",".join("?" * len(levels))

    pending = conn.execute(f"""
        SELECT g.*, p.file_path as ppath
        FROM location_guesses g
        JOIN photos p ON p.id = g.photo_id
        WHERE g.applied = 0 AND g.confidence IN ({placeholders})
        ORDER BY g.confidence DESC, g.photo_id
    """, list(levels)).fetchall()

    log.info(f"Apply: {len(pending)} guesses to apply (levels: {', '.join(sorted(levels))})")

    applied = 0
    for g in pending:
        if dry_run:
            log.info(f"  Would apply [{g['confidence']:6s}] {g['strategy']:8s}  "
                     f"{Path(g['file_path']).name}  → {g['city'] or ''} {g['country'] or ''}")
            applied += 1
            continue

        conn.execute("""
            UPDATE photos SET
              latitude=?, longitude=?, country=?, country_code=?, city=?
            WHERE id=? AND latitude IS NULL
        """, (g["latitude"], g["longitude"], g["country"], g["country_code"],
              g["city"], g["photo_id"]))
        conn.execute("UPDATE location_guesses SET applied=1 WHERE id=?", (g["id"],))
        conn.commit()

        if write_exif and g["latitude"] is not None:
            _write_exif_gps(Path(g["file_path"]), g["latitude"], g["longitude"])

        applied += 1

    if not dry_run:
        log.info(f"  Applied: {applied} guesses written to photos table")
        if write_exif:
            log.info(f"  GPS also written to EXIF" + ("" if HAS_PIEXIF else " (piexif missing — skipped)"))
    return applied


# ── Report ─────────────────────────────────────────────────────────────────────
def report(conn):
    total = conn.execute("SELECT COUNT(*) FROM photos WHERE is_duplicate=0").fetchone()[0]
    with_gps = conn.execute(
        "SELECT COUNT(*) FROM photos WHERE is_duplicate=0 AND latitude IS NOT NULL"
    ).fetchone()[0]
    no_gps = total - with_gps

    guesses = conn.execute("""
        SELECT strategy, confidence, COUNT(*) as n, SUM(applied) as done
        FROM location_guesses GROUP BY strategy, confidence ORDER BY strategy, confidence
    """).fetchall()

    still_blank = conn.execute("""
        SELECT COUNT(*) FROM photos p
        LEFT JOIN location_guesses g ON g.photo_id = p.id
        WHERE p.latitude IS NULL AND p.is_duplicate=0 AND g.id IS NULL
    """).fetchone()[0]

    print(f"\n{'─'*62}")
    print(f"  LOCATION COVERAGE")
    print(f"{'─'*62}")
    print(f"  Total photos        {total:>7,}")
    print(f"  With GPS            {with_gps:>7,}  ({with_gps/total*100:.0f}%)")
    print(f"  Missing GPS         {no_gps:>7,}")
    print()

    if guesses:
        print(f"  {'Strategy':<12} {'Confidence':<10} {'Guessed':>8}  {'Applied':>8}")
        print(f"  {'─'*12} {'─'*10} {'─'*8}  {'─'*8}")
        for g in guesses:
            print(f"  {g['strategy']:<12} {g['confidence']:<10} {g['n']:>8,}  {g['done'] or 0:>8,}")
        print()

    print(f"  Still no guess      {still_blank:>7,}")

    if still_blank and still_blank < no_gps:
        pct = (no_gps - still_blank) / no_gps * 100
        print(f"  Coverage improved   {pct:.0f}% of missing photos now have a guess")
    print(f"{'─'*62}\n")


# ── Orchestration ──────────────────────────────────────────────────────────────
def run_temporal(dry_run=False, **_):
    conn = open_db()
    strategy_temporal(conn, get_ungeotagged(conn), get_geotagged(conn), dry_run)
    conn.close()

def run_album(dry_run=False, **_):
    conn = open_db()
    strategy_album(conn, get_ungeotagged(conn), dry_run)
    conn.close()

def run_similar(dry_run=False, **_):
    conn = open_db()
    strategy_similar(conn, get_ungeotagged(conn), get_geotagged(conn), dry_run)
    conn.close()

def run_vision(dry_run=False, **_):
    geo  = GeoNames(GEONAMES_FILE)
    conn = open_db()
    strategy_vision(conn, get_ungeotagged(conn), geo, dry_run)
    conn.close()

def run_apply(dry_run=False, apply_all=False, write_exif=False, **_):
    conn = open_db()
    apply_guesses(conn, apply_all=apply_all, dry_run=dry_run, write_exif=write_exif)
    conn.close()

def run_report(**_):
    conn = open_db()
    report(conn)
    conn.close()


STEPS = {
    "temporal": run_temporal,
    "album":    run_album,
    "similar":  run_similar,
    "vision":   run_vision,
    "apply":    run_apply,
    "report":   run_report,
}


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Guess locations for photos without GPS"
    )
    parser.add_argument("--step", default="all",
                        choices=[*STEPS.keys(), "all"],
                        help="Step to run (default: all)")
    parser.add_argument("--apply-all", action="store_true",
                        help="Apply medium + low confidence guesses too (default: high only)")
    parser.add_argument("--write-exif", action="store_true",
                        help="Write GPS coordinates back into EXIF (requires piexif)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview without writing anything")
    args = parser.parse_args()

    kwargs = {"dry_run": args.dry_run, "apply_all": args.apply_all, "write_exif": args.write_exif}

    if args.step == "all":
        conn = open_db()
        ungeotagged = get_ungeotagged(conn)
        geotagged   = get_geotagged(conn)
        conn.close()

        log.info(f"{len(ungeotagged)} photos without GPS  |  {len(geotagged)} GPS-tagged reference photos")

        geo = GeoNames(GEONAMES_FILE)

        log.info("\n── temporal ──")
        conn = open_db()
        strategy_temporal(conn, ungeotagged, geotagged, args.dry_run)
        conn.close()

        log.info("\n── album ──")
        conn = open_db()
        # Refresh ungeotagged after each step — previous guesses may have been saved
        strategy_album(conn, get_ungeotagged(conn), args.dry_run)
        conn.close()

        log.info("\n── similar ──")
        conn = open_db()
        strategy_similar(conn, get_ungeotagged(conn), geotagged, args.dry_run)
        conn.close()

        log.info("\n── vision ──")
        conn = open_db()
        strategy_vision(conn, get_ungeotagged(conn), geo, args.dry_run)
        conn.close()

        log.info("\n── apply ──")
        conn = open_db()
        apply_guesses(conn, apply_all=args.apply_all, dry_run=args.dry_run, write_exif=args.write_exif)
        conn.close()

        log.info("\n── report ──")
        conn = open_db()
        report(conn)
        conn.close()
    else:
        STEPS[args.step](**kwargs)


if __name__ == "__main__":
    main()
