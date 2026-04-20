#!/usr/bin/env python3
"""
instagram_pipeline.py — Score, grade, and package holiday photos for Instagram.

Reads event folders from pipeline.py, scores every photo for Instagram appeal,
applies a cinematic colour grade (warm highlights, teal shadows, lifted blacks,
vignette, film grain), curates the best shots per album ensuring narrative variety,
builds 9:16 story images with location overlays, writes AI captions + hashtags,
and packages everything into instagram_ready/ for manual review and upload.

Steps:
  score      Score every photo (blur/exposure + Ollama aesthetic AI)
  edit       Apply cinematic grading to top candidates → _graded/
  curate     Pick best N photos per album, spread across trip timeline
  stories    Build 9:16 story images with blurred bg + location/date text
  caption    Write Instagram caption + hashtags via Ollama
  export     Package carousel + stories + caption.txt → instagram_ready/
  all        Run all steps in order (default)
  summary    Just print the album readiness list

Usage:
    python3 instagram_pipeline.py                        # run everything
    python3 instagram_pipeline.py --album "Barcelona"    # just one album
    python3 instagram_pipeline.py --step score           # one step only
    python3 instagram_pipeline.py --step all --dry-run   # preview
"""

import argparse
import base64
import io
import json
import logging
import math
import os
import re
import shutil
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
from PIL import Image, ImageFilter, ImageEnhance, ImageDraw, ImageFont

try:
    import pillow_heif
    pillow_heif.register_heif_opener()
except ImportError:
    pass

try:
    import requests as _requests
    _USE_REQUESTS = True
except ImportError:
    import urllib.request as _urllib
    _USE_REQUESTS = False

# ── Config ─────────────────────────────────────────────────────────────────────
FINAL_DIR     = Path(os.environ.get("FINAL_DIR",     "/run/media/elgan/immich/final-google-photos"))
INSTAGRAM_DIR = Path(os.environ.get("INSTAGRAM_DIR", str(Path(__file__).parent / "instagram_ready")))
PIPELINE_DB   = Path(os.environ.get("PIPELINE_DB",   str(Path(__file__).parent / "photos.db")))
INSTAGRAM_DB  = Path(os.environ.get("INSTAGRAM_DB",  str(Path(__file__).parent / "instagram.db")))
VISION_MODEL  = os.environ.get("VISION_MODEL",  "llama3.2-vision:latest")
OLLAMA_URL    = os.environ.get("OLLAMA_URL",    "http://localhost:11434")
IMMICH_URL    = os.environ.get("IMMICH_URL",    "http://localhost:2283")
IMMICH_KEY    = os.environ.get("IMMICH_API_KEY", "oT4gxVnyYPiiIJtHnSkVrSvmyvWmodCd3yoLY5WSxQ")
MAX_CAROUSEL  = int(os.environ.get("MAX_CAROUSEL", "9"))
MAX_WORKERS   = int(os.environ.get("MAX_WORKERS",  "3"))

SUPPORTED_EXT = {'.jpg', '.jpeg', '.png', '.heic', '.heif', '.webp'}
SKIP_PREFIX   = "Photos from "

# Instagram standard dimensions
CAROUSEL_W, CAROUSEL_H = 1080, 1350  # 4:5 portrait
STORY_W,    STORY_H    = 1080, 1920  # 9:16

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ── DB ─────────────────────────────────────────────────────────────────────────
def open_db() -> sqlite3.Connection:
    INSTAGRAM_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(INSTAGRAM_DB)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS scores (
            id              INTEGER PRIMARY KEY,
            file_path       TEXT UNIQUE,
            album_name      TEXT,
            blur_score      REAL,
            exposure_ok     INTEGER,
            resolution_ok   INTEGER,
            ai_score        INTEGER,
            ai_mood         TEXT,
            ai_best_feature TEXT,
            ai_raw          TEXT,
            technical_score REAL,
            final_score     REAL,
            scored_at       TEXT
        );
        CREATE TABLE IF NOT EXISTS curated (
            id          INTEGER PRIMARY KEY,
            album_name  TEXT,
            file_path   TEXT,
            rank        INTEGER,
            edited_path TEXT,
            UNIQUE(album_name, file_path)
        );
        CREATE TABLE IF NOT EXISTS captions (
            id           INTEGER PRIMARY KEY,
            album_name   TEXT UNIQUE,
            caption      TEXT,
            hashtags     TEXT,
            alt_text     TEXT,
            generated_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_scores_album ON scores(album_name);
        CREATE INDEX IF NOT EXISTS idx_curated_album ON curated(album_name);
    """)
    conn.commit()
    return conn


# ── Helpers ────────────────────────────────────────────────────────────────────
def is_photo(p: Path) -> bool:
    return p.suffix.lower() in SUPPORTED_EXT and not p.name.startswith('.')


def load_image(path: Path) -> Image.Image:
    img = Image.open(path)
    try:
        from PIL import ExifTags
        exif = img._getexif()
        if exif:
            for tag, val in exif.items():
                if ExifTags.TAGS.get(tag) == 'Orientation':
                    ops = {3: 180, 6: 270, 8: 90}
                    if val in ops:
                        img = img.rotate(ops[val], expand=True)
                    break
    except Exception:
        pass
    return img.convert('RGB')


def img_to_b64(img: Image.Image) -> str:
    buf = io.BytesIO()
    t = img.copy()
    t.thumbnail((512, 512), Image.LANCZOS)
    t.save(buf, format='JPEG', quality=85)
    return base64.b64encode(buf.getvalue()).decode()


def ollama_vision(prompt: str, img: Image.Image) -> str:
    payload = json.dumps({
        "model":   VISION_MODEL,
        "prompt":  prompt,
        "images":  [img_to_b64(img)],
        "stream":  False,
        "options": {"temperature": 0.2},
    }).encode()
    url = f"{OLLAMA_URL}/api/generate"
    if _USE_REQUESTS:
        r = _requests.post(url, data=payload,
                           headers={"Content-Type": "application/json"}, timeout=60)
        r.raise_for_status()
        return r.json()["response"]
    req = _urllib.Request(url, data=payload, headers={"Content-Type": "application/json"})
    with _urllib.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())["response"]


def parse_json(text: str) -> dict:
    m = re.search(r'\{.*\}', text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group())
        except json.JSONDecodeError:
            pass
    return {}


def safe_name(name: str) -> str:
    return re.sub(r'[<>:"/\\|?*]', '_', name).strip('. ')


def find_albums(album_filter=None) -> list:
    if not FINAL_DIR or not FINAL_DIR.exists():
        log.error("FINAL_DIR not set or doesn't exist")
        return []
    albums = sorted([
        d for d in FINAL_DIR.iterdir()
        if d.is_dir()
        and not d.name.startswith(SKIP_PREFIX)
        and not d.name.startswith('.')
    ])
    if album_filter:
        albums = [a for a in albums if album_filter.lower() in a.name.lower()]
    return albums


def pipeline_meta(file_path: str) -> dict:
    """Fetch AI tags + location from the main pipeline DB (optional enrichment)."""
    if not PIPELINE_DB.exists():
        return {}
    try:
        conn = sqlite3.connect(PIPELINE_DB)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT ai_tags, ai_scene_type, ai_description, country, city, best_date "
            "FROM photos WHERE file_path=?", (file_path,)
        ).fetchone()
        conn.close()
        return dict(row) if row else {}
    except Exception:
        return {}


def album_meta(album_name: str) -> dict:
    """Fetch album-level location from pipeline DB, enriched by Immich if available."""
    result = {}
    if PIPELINE_DB.exists():
        try:
            conn = sqlite3.connect(PIPELINE_DB)
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT country, city, start_date FROM albums WHERE name=?", (album_name,)
            ).fetchone()
            conn.close()
            if row:
                result = dict(row)
        except Exception:
            pass

    # If pipeline DB has no location, try Immich
    if not result.get('city') and not result.get('country'):
        result.update(_immich_album_meta(album_name))
    return result


def _immich_album_meta(album_name: str) -> dict:
    """Pull location/date from an Immich album matching album_name."""
    try:
        if _USE_REQUESTS:
            r = _requests.get(
                f"{IMMICH_URL}/api/albums",
                headers={"x-api-key": IMMICH_KEY},
                timeout=10,
            )
            if not r.ok:
                return {}
            albums = r.json()
        else:
            req = _urllib.Request(
                f"{IMMICH_URL}/api/albums",
                headers={"x-api-key": IMMICH_KEY, "Accept": "application/json"},
            )
            with _urllib.urlopen(req, timeout=10) as resp:
                albums = json.loads(resp.read())

        # Find the closest-matching album name
        match = next(
            (a for a in albums if album_name.lower() in a.get('albumName', '').lower()
             or a.get('albumName', '').lower() in album_name.lower()),
            None,
        )
        if not match:
            return {}

        # Fetch first asset's exif for location
        aid = match['id']
        if _USE_REQUESTS:
            r = _requests.get(
                f"{IMMICH_URL}/api/albums/{aid}",
                headers={"x-api-key": IMMICH_KEY},
                timeout=10,
            )
            if not r.ok:
                return {}
            detail = r.json()
        else:
            req = _urllib.Request(
                f"{IMMICH_URL}/api/albums/{aid}",
                headers={"x-api-key": IMMICH_KEY, "Accept": "application/json"},
            )
            with _urllib.urlopen(req, timeout=10) as resp:
                detail = json.loads(resp.read())

        assets = detail.get('assets', [])
        if not assets:
            return {}
        exif = assets[0].get('exifInfo', {})
        return {
            'city':       exif.get('city', ''),
            'country':    exif.get('country', ''),
            'start_date': assets[0].get('fileCreatedAt', '')[:10],
        }
    except Exception:
        return {}


# ── Technical scoring ──────────────────────────────────────────────────────────
def score_blur(img: Image.Image) -> float:
    """Laplacian variance — higher = sharper. Returns 0-100."""
    lap = img.convert('L').filter(
        ImageFilter.Kernel(size=3, kernel=[0, 1, 0, 1, -4, 1, 0, 1, 0], scale=1)
    )
    variance = float(np.var(np.array(lap, dtype=np.float32)))
    return min(100.0, variance / 10.0)


def score_exposure(img: Image.Image) -> tuple:
    arr = np.array(img.convert('L'), dtype=np.float32)
    hist, _ = np.histogram(arr, bins=256, range=(0, 256))
    total     = float(arr.size)
    pct_dark  = hist[:26].sum()  / total
    pct_blown = hist[230:].sum() / total
    pct_mid   = hist[64:192].sum() / total
    score     = float(np.clip(pct_mid * 100 - pct_dark * 50 - pct_blown * 30, 0, 100))
    ok        = pct_dark < 0.35 and pct_blown < 0.15
    return score, ok


def score_technical(img: Image.Image) -> dict:
    blur          = score_blur(img)
    exp, exp_ok   = score_exposure(img)
    w, h          = img.size
    res_ok        = w >= 800 and h >= 800
    tech          = blur * 0.6 + exp * 0.3 + (20 if res_ok else 0)
    return {
        'blur_score':     round(blur, 1),
        'exposure_ok':    int(exp_ok),
        'resolution_ok':  int(res_ok),
        'technical_score': round(min(100.0, tech), 1),
    }


# ── AI scoring ─────────────────────────────────────────────────────────────────
_SCORE_PROMPT = (
    "Rate this photo for Instagram. Reply ONLY with valid JSON, nothing else:\n"
    '{"instagram_score": <1-10>, "composition": <1-10>, "lighting": <1-10>, '
    '"mood": "<golden_hour|moody|vibrant|dramatic|soft|flat>", '
    '"best_feature": "<10 words max>", "is_holiday": <true|false>}\n'
    "Score 9-10 only for genuinely stunning shots. Blurry or boring = 1-4."
)


def score_aesthetic(img: Image.Image) -> dict:
    try:
        raw  = ollama_vision(_SCORE_PROMPT, img)
        data = parse_json(raw)
        return {
            'ai_score':        int(data.get('instagram_score', 5)),
            'ai_mood':         str(data.get('mood', 'flat')),
            'ai_best_feature': str(data.get('best_feature', '')),
            'ai_raw':          raw[:500],
        }
    except Exception as e:
        log.debug(f"AI score failed: {e}")
        return {'ai_score': 5, 'ai_mood': 'flat', 'ai_best_feature': '', 'ai_raw': ''}


# ── Score step ─────────────────────────────────────────────────────────────────
def score(dry_run=False, album_filter=None):
    albums = find_albums(album_filter)
    conn   = open_db()

    todo = []
    for album_dir in albums:
        for p in sorted(album_dir.iterdir()):
            if not is_photo(p):
                continue
            if not conn.execute("SELECT id FROM scores WHERE file_path=?", (str(p),)).fetchone():
                todo.append((album_dir.name, p))

    log.info(f"Scoring {len(todo)} photos across {len(albums)} albums...")
    if dry_run or not todo:
        return

    def score_one(album_name, path):
        try:
            img  = load_image(path)
            tech = score_technical(img)
            ai   = score_aesthetic(img)
            # Weight: 70% AI appeal, 30% technical quality
            final = tech['technical_score'] * 0.3 + ai['ai_score'] * 10 * 0.7
            return album_name, path, tech, ai, round(final, 1)
        except Exception as e:
            log.warning(f"Score failed {path.name}: {e}")
            return None

    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {pool.submit(score_one, a, p): (a, p) for a, p in todo}
        for fut in as_completed(futs):
            result = fut.result()
            if not result:
                continue
            album_name, path, tech, ai, final = result
            conn.execute("""
                INSERT OR REPLACE INTO scores
                  (file_path, album_name, blur_score, exposure_ok, resolution_ok,
                   ai_score, ai_mood, ai_best_feature, ai_raw,
                   technical_score, final_score, scored_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
            """, (str(path), album_name,
                  tech['blur_score'], tech['exposure_ok'], tech['resolution_ok'],
                  ai['ai_score'], ai['ai_mood'], ai['ai_best_feature'], ai['ai_raw'],
                  tech['technical_score'], final))
            conn.commit()
            done += 1
            if done % 20 == 0:
                log.info(f"  Scored {done}/{len(todo)}...")

    log.info(f"Score complete: {done} photos scored")
    conn.close()


# ── Cinematic grading ──────────────────────────────────────────────────────────
def _build_lut(r_pts, g_pts, b_pts) -> list:
    """Build a 768-value PIL LUT (256 R, 256 G, 256 B — sequential by channel)."""
    def interp(pts, x):
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        return int(np.clip(np.interp(x, xs, ys), 0, 255))

    r = [interp(r_pts, i) for i in range(256)]
    g = [interp(g_pts, i) for i in range(256)]
    b = [interp(b_pts, i) for i in range(256)]
    return r + g + b  # PIL expects sequential: all-R then all-G then all-B


# Cinematic "orange and teal" travel preset:
#   - Lifted blacks (matte/faded film base)
#   - Warm golden highlights (R+, G+, B-)
#   - Cool teal shadows (R-, B+)
#   - Slightly pulled whites (no blown highlights)
_CINEMA_LUT = _build_lut(
    r_pts=[(0, 15), (64,  68), (128, 133), (192, 200), (255, 255)],  # warm highlights
    g_pts=[(0, 22), (64,  74), (128, 133), (192, 195), (255, 250)],  # golden midtones
    b_pts=[(0, 35), (64,  78), (128, 128), (192, 180), (255, 225)],  # teal shadows, warm highs
)


def apply_vibrance(img: Image.Image, amount: float = 0.32) -> Image.Image:
    """Boost dull colours more than vivid ones — preserves skin tones."""
    arr = np.array(img, dtype=np.float32) / 255.0
    sat  = np.max(arr, axis=2) - np.min(arr, axis=2)              # 0=grey, 1=vivid
    boost = (1.0 + amount * (1.0 - sat))[..., np.newaxis]         # more boost where dull
    lum   = (0.299 * arr[..., 0] + 0.587 * arr[..., 1] + 0.114 * arr[..., 2])[..., np.newaxis]
    result = lum + (arr - lum) * boost
    return Image.fromarray(np.clip(result * 255, 0, 255).astype(np.uint8))


def apply_vignette(img: Image.Image, strength: float = 0.22) -> Image.Image:
    """Elliptical vignette: darkens edges ~22%, drawing the eye to centre."""
    arr = np.array(img, dtype=np.float32)
    h, w = arr.shape[:2]
    Y = np.linspace(-1, 1, h)[:, np.newaxis]
    X = np.linspace(-1, 1, w)[np.newaxis, :]
    dist = np.sqrt(X**2 + Y**2) / math.sqrt(2)  # 0 = centre, 1 = corner
    mask = 1.0 - strength * np.clip((dist - 0.45) / 0.55, 0, 1) ** 1.6
    return Image.fromarray(np.clip(arr * mask[..., np.newaxis], 0, 255).astype(np.uint8))


def apply_grain(img: Image.Image, amount: float = 3.5) -> Image.Image:
    """Uniform luminance noise — analogue film texture without colour blotches."""
    arr   = np.array(img, dtype=np.float32)
    noise = np.random.normal(0, amount, arr.shape[:2])
    for c in range(3):
        arr[..., c] += noise
    return Image.fromarray(np.clip(arr, 0, 255).astype(np.uint8))


def grade_photo(img: Image.Image) -> Image.Image:
    """Full cinematic grade: tone curve → split tone → vibrance → sharpen → vignette → grain."""
    graded = img.point(_CINEMA_LUT)                                          # tone + split tone
    graded = ImageEnhance.Color(graded).enhance(1.06)                        # slight saturation lift
    graded = apply_vibrance(graded, amount=0.32)                             # selective sat boost
    graded = graded.filter(ImageFilter.UnsharpMask(radius=0.9, percent=65, threshold=3))
    graded = apply_vignette(graded, strength=0.22)
    graded = apply_grain(graded, amount=3.5)
    return graded


def smart_crop(img: Image.Image, target_w: int, target_h: int) -> Image.Image:
    """Centre-crop to target ratio, bias slightly upward (preserves sky / headroom)."""
    src_w, src_h = img.size
    target_ratio = target_w / target_h
    src_ratio    = src_w   / src_h

    if src_ratio > target_ratio:
        new_w = int(src_h * target_ratio)
        left  = (src_w - new_w) // 2
        img   = img.crop((left, 0, left + new_w, src_h))
    else:
        new_h = int(src_w / target_ratio)
        top   = int((src_h - new_h) * 0.38)   # slight upward bias for sky/headroom
        img   = img.crop((0, top, src_w, top + new_h))

    return img.resize((target_w, target_h), Image.LANCZOS)


# ── Edit step ──────────────────────────────────────────────────────────────────
def edit(dry_run=False, album_filter=None):
    conn     = open_db()
    work_dir = INSTAGRAM_DIR / "_graded"
    work_dir.mkdir(parents=True, exist_ok=True)

    albums = conn.execute("SELECT DISTINCT album_name FROM scores").fetchall()
    if album_filter:
        albums = [a for a in albums if album_filter.lower() in a['album_name'].lower()]

    total = 0
    for row in albums:
        album_name = row['album_name']
        photos = conn.execute("""
            SELECT file_path, final_score FROM scores
            WHERE album_name=? AND resolution_ok=1
            ORDER BY final_score DESC
        """, (album_name,)).fetchall()

        # Grade top 30% or at least 2× MAX_CAROUSEL candidates
        cutoff     = max(MAX_CAROUSEL * 2, int(len(photos) * 0.30))
        candidates = photos[:cutoff]

        for p in candidates:
            path     = Path(p['file_path'])
            out_path = work_dir / f"{path.stem}_graded.jpg"
            if not path.exists() or out_path.exists():
                continue
            if dry_run:
                log.info(f"  Would grade: {path.name}")
                total += 1
                continue
            try:
                img     = load_image(path)
                graded  = grade_photo(img)
                cropped = smart_crop(graded, CAROUSEL_W, CAROUSEL_H)
                cropped.save(out_path, 'JPEG', quality=95, optimize=True)
                total += 1
            except Exception as e:
                log.warning(f"  Edit failed {path.name}: {e}")

    log.info(f"Edit complete: {total} photos graded → {work_dir}")
    conn.close()


# ── Curate step ────────────────────────────────────────────────────────────────
def curate(dry_run=False, album_filter=None):
    conn     = open_db()
    work_dir = INSTAGRAM_DIR / "_graded"

    albums = conn.execute("SELECT DISTINCT album_name FROM scores").fetchall()
    if album_filter:
        albums = [a for a in albums if album_filter.lower() in a['album_name'].lower()]

    total = 0
    for row in albums:
        album_name = row['album_name']

        photos = conn.execute("""
            SELECT file_path, final_score, ai_score, ai_mood
            FROM scores
            WHERE album_name=? AND resolution_ok=1 AND exposure_ok=1
            ORDER BY final_score DESC
        """, (album_name,)).fetchall()

        if len(photos) < 3:
            log.info(f"  {album_name}: only {len(photos)} usable photos, skipping")
            continue

        # Sort by filename (encodes date/time) to preserve trip narrative
        by_time = sorted(photos, key=lambda p: p['file_path'])
        n       = min(MAX_CAROUSEL, len(by_time))

        # Divide trip into N time segments — pick highest-scoring shot from each
        selected = []
        seg_size = len(by_time) / n
        for i in range(n):
            start   = int(i * seg_size)
            end     = int((i + 1) * seg_size)
            segment = by_time[start:end]
            if segment:
                selected.append(max(segment, key=lambda p: p['final_score']))

        # Restore chronological order for narrative flow
        selected.sort(key=lambda p: p['file_path'])

        if dry_run:
            log.info(f"  {album_name}: would select {len(selected)} photos")
            for i, p in enumerate(selected):
                log.info(f"    {i+1}. {Path(p['file_path']).name}  score={p['final_score']}  mood={p['ai_mood']}")
            continue

        conn.execute("DELETE FROM curated WHERE album_name=?", (album_name,))
        for rank, p in enumerate(selected, 1):
            path       = Path(p['file_path'])
            graded     = work_dir / f"{path.stem}_graded.jpg"
            edited_path = str(graded) if graded.exists() else p['file_path']
            conn.execute("""
                INSERT OR REPLACE INTO curated (album_name, file_path, rank, edited_path)
                VALUES (?,?,?,?)
            """, (album_name, p['file_path'], rank, edited_path))
        conn.commit()
        total += 1
        log.info(f"  Curated: '{album_name}' → {len(selected)} photos")

    log.info(f"Curate complete: {total} albums ready")
    conn.close()


# ── Stories step ───────────────────────────────────────────────────────────────
_FONT_CANDIDATES = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    "/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf",
    "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
    "/System/Library/Fonts/Helvetica.ttc",
]


def _font(size: int):
    for path in _FONT_CANDIDATES:
        if Path(path).exists():
            return ImageFont.truetype(path, size)
    return ImageFont.load_default()


def make_story(img: Image.Image, location: str = '', date: str = '') -> Image.Image:
    """
    9:16 canvas: blurred + darkened photo fills background,
    the graded photo sits centred with a thin white border,
    location name at top, month/year at bottom.
    """
    # Background: blurred + darkened to ~40% brightness
    bg_arr = np.array(smart_crop(img.copy(), STORY_W, STORY_H), dtype=np.float32)
    bg_arr = np.clip(bg_arr * 0.40, 0, 255).astype(np.uint8)
    bg     = Image.fromarray(bg_arr).filter(ImageFilter.GaussianBlur(radius=28))

    # Foreground: 92% story width, keep 4:5 ratio, white border
    fw      = int(STORY_W * 0.92)
    fh      = int(fw * CAROUSEL_H / CAROUSEL_W)
    photo   = smart_crop(img, fw, fh)
    bordered = Image.new('RGB', (fw + 6, fh + 6), (255, 255, 255))
    bordered.paste(photo, (3, 3))

    # Paste centred with slight upward bias
    x = (STORY_W - bordered.width) // 2
    y = int((STORY_H - bordered.height) * 0.44)
    bg.paste(bordered, (x, y))

    draw = ImageDraw.Draw(bg)

    # Location text (top)
    if location:
        font_loc = _font(54)
        text     = location.upper()
        bbox     = draw.textbbox((0, 0), text, font=font_loc)
        tw       = bbox[2] - bbox[0]
        tx       = (STORY_W - tw) // 2
        draw.text((tx + 2, 82), text, font=font_loc, fill=(0, 0, 0))      # shadow
        draw.text((tx,     80), text, font=font_loc, fill=(255, 255, 255))

    # Date text (bottom)
    if date:
        font_date = _font(38)
        try:
            from datetime import datetime as _dt
            d        = _dt.fromisoformat(date[:10])
            date_str = d.strftime('%B %Y').upper()
        except Exception:
            date_str = date[:10].upper()
        bbox = draw.textbbox((0, 0), date_str, font=font_date)
        tw   = bbox[2] - bbox[0]
        tx   = (STORY_W - tw) // 2
        draw.text((tx + 1, STORY_H - 111), date_str, font=font_date, fill=(0, 0, 0))
        draw.text((tx,     STORY_H - 112), date_str, font=font_date, fill=(200, 200, 200))

    return bg


def stories(dry_run=False, album_filter=None):
    conn = open_db()

    albums = conn.execute("SELECT DISTINCT album_name FROM curated").fetchall()
    if album_filter:
        albums = [a for a in albums if album_filter.lower() in a['album_name'].lower()]

    total = 0
    for row in albums:
        album_name = row['album_name']
        out_dir    = INSTAGRAM_DIR / safe_name(album_name) / "stories"

        picks = conn.execute("""
            SELECT file_path, edited_path, rank FROM curated
            WHERE album_name=? ORDER BY rank
        """, (album_name,)).fetchall()
        if not picks:
            continue

        # Location from pipeline DB or fall back to album name
        meta      = album_meta(album_name)
        loc_parts = [p for p in [meta.get('city'), meta.get('country')] if p]
        location  = ', '.join(loc_parts) if loc_parts else album_name
        date      = meta.get('start_date', '')

        if dry_run:
            log.info(f"  Would create {len(picks)} stories for '{album_name}'")
            continue

        out_dir.mkdir(parents=True, exist_ok=True)
        for pick in picks:
            src      = Path(pick['edited_path'] or pick['file_path'])
            out_path = out_dir / f"{pick['rank']:02d}_story.jpg"
            if not src.exists() or out_path.exists():
                continue
            try:
                img   = load_image(src)
                story = make_story(img, location=location, date=date)
                story.save(out_path, 'JPEG', quality=92)
                total += 1
            except Exception as e:
                log.warning(f"  Story failed {src.name}: {e}")

    log.info(f"Stories complete: {total} story images created")
    conn.close()


# ── Caption step ───────────────────────────────────────────────────────────────
_CAPTION_PROMPT = """\
You are a travel photographer writing Instagram captions.

Album:       {album_name}
Location:    {location}
Date:        {date}
Photo mood:  {mood}
Description: {description}

Write an Instagram caption. Requirements:
- 2-3 sentences: evocative, first-person, not generic travel clichés
- 20-25 hashtags mixing location, mood, travel, and photography tags
- 2-3 emojis woven naturally into the text (not just at the end)

Reply ONLY with valid JSON, nothing else:
{{"caption": "...", "hashtags": ["tag1", "tag2", ...], "alt_text": "one sentence description for accessibility"}}"""


def caption(dry_run=False, album_filter=None):
    conn = open_db()

    albums = conn.execute("SELECT DISTINCT album_name FROM curated").fetchall()
    if album_filter:
        albums = [a for a in albums if album_filter.lower() in a['album_name'].lower()]

    total = 0
    for row in albums:
        album_name = row['album_name']
        if conn.execute("SELECT id FROM captions WHERE album_name=?", (album_name,)).fetchone():
            continue

        hero = conn.execute("""
            SELECT file_path, edited_path FROM curated
            WHERE album_name=? ORDER BY rank LIMIT 1
        """, (album_name,)).fetchone()
        if not hero:
            continue

        meta       = album_meta(album_name)
        score_row  = conn.execute(
            "SELECT ai_mood, ai_best_feature FROM scores WHERE file_path=?",
            (hero['file_path'],)
        ).fetchone()
        photo_meta = pipeline_meta(hero['file_path'])

        loc_parts = [p for p in [meta.get('city'), meta.get('country')] if p]
        location  = ', '.join(loc_parts) if loc_parts else album_name
        date      = (meta.get('start_date') or photo_meta.get('best_date', ''))[:10]
        mood      = (score_row['ai_mood']         if score_row else 'travel')
        desc      = (photo_meta.get('ai_description') or
                     (score_row['ai_best_feature'] if score_row else ''))

        prompt = _CAPTION_PROMPT.format(
            album_name=album_name, location=location,
            date=date, mood=mood, description=desc
        )

        if dry_run:
            log.info(f"  Would generate caption for '{album_name}'")
            continue

        try:
            hero_img = load_image(Path(hero['edited_path'] or hero['file_path']))
            raw      = ollama_vision(prompt, hero_img)
            data     = parse_json(raw)
            conn.execute("""
                INSERT OR REPLACE INTO captions
                  (album_name, caption, hashtags, alt_text, generated_at)
                VALUES (?,?,?,?,datetime('now'))
            """, (album_name, data.get('caption', ''),
                  json.dumps(data.get('hashtags', [])),
                  data.get('alt_text', '')))
            conn.commit()
            total += 1
            log.info(f"  Caption written: '{album_name}'")
        except Exception as e:
            log.warning(f"  Caption failed '{album_name}': {e}")

    log.info(f"Caption complete: {total} captions generated")
    conn.close()


# ── Export step ────────────────────────────────────────────────────────────────
def export(dry_run=False, album_filter=None):
    conn = open_db()

    albums = conn.execute("SELECT DISTINCT album_name FROM curated").fetchall()
    if album_filter:
        albums = [a for a in albums if album_filter.lower() in a['album_name'].lower()]

    total = 0
    for row in albums:
        album_name   = row['album_name']
        out_dir      = INSTAGRAM_DIR / safe_name(album_name)
        carousel_dir = out_dir / "carousel"

        picks   = conn.execute("""
            SELECT file_path, edited_path, rank FROM curated
            WHERE album_name=? ORDER BY rank
        """, (album_name,)).fetchall()
        cap_row = conn.execute(
            "SELECT caption, hashtags, alt_text FROM captions WHERE album_name=?",
            (album_name,)
        ).fetchone()

        if dry_run:
            log.info(f"  Would export '{album_name}': {len(picks)} carousel photos")
            continue

        carousel_dir.mkdir(parents=True, exist_ok=True)

        for pick in picks:
            src = Path(pick['edited_path'] or pick['file_path'])
            dst = carousel_dir / f"{pick['rank']:02d}_{src.stem}.jpg"
            if src.exists() and not dst.exists():
                shutil.copy2(src, dst)

        caption_file = out_dir / "caption.txt"
        if cap_row and not caption_file.exists():
            hashtags    = json.loads(cap_row['hashtags'] or '[]')
            hashtag_str = ' '.join(f'#{h.lstrip("#")}' for h in hashtags)
            with open(caption_file, 'w') as f:
                f.write(f"{cap_row['caption']}\n\n{hashtag_str}\n")
                if cap_row['alt_text']:
                    f.write(f"\n---\nAlt text: {cap_row['alt_text']}\n")

        total += 1
        log.info(f"  Exported: {out_dir}")

    log.info(f"Export complete: {total} albums in {INSTAGRAM_DIR}")
    conn.close()


# ── Summary ────────────────────────────────────────────────────────────────────
def summary(dry_run=False, album_filter=None):
    """Print a table of every album and its Instagram readiness."""
    conn = open_db()

    albums = conn.execute("SELECT DISTINCT album_name FROM scores ORDER BY album_name").fetchall()
    if album_filter:
        albums = [a for a in albums if album_filter.lower() in a['album_name'].lower()]

    if not albums:
        print("\nNo albums scored yet — run:  python3 instagram_pipeline.py --step score")
        conn.close()
        return

    rows = []
    for row in albums:
        name   = row['album_name']
        scored = conn.execute("SELECT COUNT(*) FROM scores WHERE album_name=?",       (name,)).fetchone()[0]
        curated_n = conn.execute("SELECT COUNT(*) FROM curated WHERE album_name=?",   (name,)).fetchone()[0]
        stories_dir = INSTAGRAM_DIR / safe_name(name) / "stories"
        story_n = len(list(stories_dir.glob("*.jpg"))) if stories_dir.exists() else 0
        has_cap = bool(conn.execute("SELECT id FROM captions WHERE album_name=?",     (name,)).fetchone())
        exported = (INSTAGRAM_DIR / safe_name(name) / "caption.txt").exists()

        steps_done = []
        if scored:     steps_done.append(f"scored({scored})")
        if curated_n:  steps_done.append(f"curated({curated_n})")
        if story_n:    steps_done.append(f"stories({story_n})")
        if has_cap:    steps_done.append("caption")
        if exported:   steps_done.append("EXPORTED")

        status = "  READY  " if exported else " partial " if steps_done else "  pending"
        rows.append((status, name, ", ".join(steps_done) or "—"))

    conn.close()

    ready   = sum(1 for r in rows if "READY" in r[0])
    partial = sum(1 for r in rows if "partial" in r[0])

    print(f"\n{'─'*72}")
    print(f"  INSTAGRAM ALBUMS  ({ready} ready, {partial} partial, {len(rows)} total)")
    print(f"{'─'*72}")
    col = max(len(r[1]) for r in rows) + 2
    for status, name, progress in rows:
        print(f"  [{status}]  {name:<{col}}  {progress}")
    print(f"{'─'*72}")
    if ready:
        print(f"\n  Output folder: {INSTAGRAM_DIR}")
    print()


# ── Main ───────────────────────────────────────────────────────────────────────
STEPS = {
    'score':   score,
    'edit':    edit,
    'curate':  curate,
    'stories': stories,
    'caption': caption,
    'export':  export,
    'summary': summary,
}


def main():
    parser = argparse.ArgumentParser(
        description="Instagram pipeline: score → edit → curate → stories → caption → export"
    )
    parser.add_argument('--step', default='all',
                        choices=[*STEPS.keys(), 'all'],
                        help='Step to run (default: all)')
    parser.add_argument('--album',   default=None,
                        help='Filter to albums containing this string (case-insensitive)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview without writing anything')
    args = parser.parse_args()

    if not FINAL_DIR.exists():
        parser.error(f"Photo library not found: {FINAL_DIR}\n"
                     "Mount the drive or set FINAL_DIR env var.")

    INSTAGRAM_DIR.mkdir(parents=True, exist_ok=True)
    kwargs = {'dry_run': args.dry_run, 'album_filter': args.album}

    pipeline_steps = ['score', 'edit', 'curate', 'stories', 'caption', 'export']

    if args.step == 'all':
        for name in pipeline_steps:
            log.info(f"\n── {name} ──────────────────────────────────────")
            STEPS[name](**kwargs)
        summary(**kwargs)
    else:
        STEPS[args.step](**kwargs)


if __name__ == '__main__':
    main()
