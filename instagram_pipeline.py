#!/usr/bin/env python3
"""
instagram_pipeline.py — Score, grade, and package holiday photos for Instagram.

Reads event folders from pipeline.py's final-google-photos output, scores every
photo for Instagram appeal, applies a mood-matched cinematic colour grade, curates
the best shots per album ensuring narrative variety, builds 9:16 story images with
gradient overlays and location text, writes AI captions + hashtags, and packages
everything into instagram_ready/ for manual review and upload.

Steps:
  discover   Rank all event albums by Instagram potential — start here
  purge      Remove stale/noise albums from instagram.db
  score      Score every photo (blur/exposure + Ollama aesthetic AI)
  edit       Apply mood-matched cinematic grade to top candidates → _graded/
  curate     Pick best N photos per album, spread across trip timeline
  stories    Build 9:16 story images with gradient overlays + location text
  caption    Write Instagram caption + hashtags via Ollama
  export     Package carousel + collage + stories + caption.txt → instagram_ready/
  all        Run purge → score → edit → curate → stories → caption → export
  summary    Print the album readiness table

Usage:
    python3 instagram_pipeline.py                        # run everything
    python3 instagram_pipeline.py --album "Barcelona"    # just one album
    python3 instagram_pipeline.py --step score           # one step only
    python3 instagram_pipeline.py --step discover        # see ranked album list
    python3 instagram_pipeline.py --step purge --dry-run # preview noise cleanup
    python3 instagram_pipeline.py --step all --dry-run   # preview full run
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
FINAL_DIR     = Path(os.environ.get("FINAL_DIR",     "/run/media/elgan/immich1/final-google-photos"))
INSTAGRAM_DIR = Path(os.environ.get("INSTAGRAM_DIR", str(Path(__file__).parent / "instagram_ready")))
PIPELINE_DB   = Path(os.environ.get("PIPELINE_DB",   "/run/media/elgan/extra/Pictures/photo_pipeline/pipeline_v2.db"))
INSTAGRAM_DB  = Path(os.environ.get("INSTAGRAM_DB",  str(Path(__file__).parent / "instagram.db")))
VISION_MODEL  = os.environ.get("VISION_MODEL",  "llama3.2-vision:latest")
OLLAMA_URL    = os.environ.get("OLLAMA_URL",    "http://localhost:11434")
IMMICH_URL    = os.environ.get("IMMICH_URL",    "http://localhost:2283")
IMMICH_KEY    = os.environ.get("IMMICH_API_KEY", "oT4gxVnyYPiiIJtHnSkVrSvmyvWmodCd3yoLY5WSxQ")
MAX_CAROUSEL  = int(os.environ.get("MAX_CAROUSEL",   "9"))
MAX_WORKERS   = int(os.environ.get("MAX_WORKERS",    "4"))
MIN_PHOTOS    = int(os.environ.get("MIN_PHOTOS",     "5"))
# Max photos to AI-score per album. Albums spread score samples evenly across
# the timeline so every part of the trip has representation.
MAX_PER_ALBUM = int(os.environ.get("MAX_PER_ALBUM", "150"))

SUPPORTED_EXT = {'.jpg', '.jpeg', '.png', '.heic', '.heif', '.webp'}
SKIP_PREFIX   = "Photos from "

# Instagram standard dimensions
CAROUSEL_W, CAROUSEL_H = 1080, 1350   # 4:5 portrait
STORY_W,    STORY_H    = 1080, 1920   # 9:16
COLLAGE_W,  COLLAGE_H  = 1080, 1080   # 1:1 grid

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
    return (p.suffix.lower() in SUPPORTED_EXT
            and not p.name.startswith('.')
            and p.exists())  # resolves symlinks — skips broken ones


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


def ollama_text(prompt: str) -> str:
    """Call Ollama without an image (text-only generation)."""
    text_model = os.environ.get("TEXT_MODEL", "gemma3:4b")
    payload = json.dumps({
        "model":   text_model,
        "prompt":  prompt,
        "stream":  False,
        "options": {"temperature": 0.7},
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
        log.error(f"FINAL_DIR not set or doesn't exist: {FINAL_DIR}")
        return []
    albums = []
    for d in FINAL_DIR.iterdir():
        if not d.is_dir():
            continue
        name = d.name
        if name.startswith('.') or name.startswith(SKIP_PREFIX):
            continue
        # Count actual photos (not just any files)
        photo_count = sum(1 for f in d.iterdir()
                          if f.is_file() and f.suffix.lower() in SUPPORTED_EXT)
        if photo_count < MIN_PHOTOS:
            continue
        albums.append(d)
    albums.sort(key=lambda d: d.name)
    if album_filter:
        albums = [a for a in albums if album_filter.lower() in a.name.lower()]
    return albums


def _valid_album_names(album_filter=None) -> set:
    return {a.name for a in find_albums(album_filter)}


def pipeline_meta(file_path: str) -> dict:
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
    result = {}
    if PIPELINE_DB.exists():
        try:
            conn = sqlite3.connect(PIPELINE_DB)
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT country, city, start_date, end_date FROM albums WHERE name=?",
                (album_name,)
            ).fetchone()
            conn.close()
            if row:
                result = dict(row)
        except Exception:
            pass

    if not result.get('city') and not result.get('country'):
        result.update(_immich_album_meta(album_name))
    return result


def _immich_album_meta(album_name: str) -> dict:
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

        match = next(
            (a for a in albums if album_name.lower() in a.get('albumName', '').lower()
             or a.get('albumName', '').lower() in album_name.lower()),
            None,
        )
        if not match:
            return {}

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
    lap = img.convert('L').filter(
        ImageFilter.Kernel(size=(3, 3), kernel=[0, 1, 0, 1, -4, 1, 0, 1, 0], scale=1)
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
        'blur_score':      round(blur, 1),
        'exposure_ok':     int(exp_ok),
        'resolution_ok':   int(res_ok),
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
        all_photos = sorted([p for p in album_dir.iterdir() if is_photo(p)])
        already    = {r[0] for r in conn.execute(
            "SELECT file_path FROM scores WHERE album_name=?", (album_dir.name,)
        ).fetchall()}
        unscored = [p for p in all_photos if str(p) not in already]

        # Spread sample evenly across the album timeline (preserves trip narrative)
        if len(unscored) > MAX_PER_ALBUM:
            step     = len(unscored) / MAX_PER_ALBUM
            unscored = [unscored[int(i * step)] for i in range(MAX_PER_ALBUM)]

        todo.extend((album_dir.name, p) for p in unscored)

    log.info(f"Scoring {len(todo)} photos across {len(albums)} albums...")
    if dry_run or not todo:
        conn.close()
        return

    def score_one(album_name, path):
        try:
            img  = load_image(path)
            tech = score_technical(img)
            ai   = score_aesthetic(img)
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
    def interp(pts, x):
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        return int(np.clip(np.interp(x, xs, ys), 0, 255))
    r = [interp(r_pts, i) for i in range(256)]
    g = [interp(g_pts, i) for i in range(256)]
    b = [interp(b_pts, i) for i in range(256)]
    return r + g + b


# Six mood-matched cinematic LUTs ───────────────────────────────────────────────

# golden_hour: warm orange-gold highlights, lifted mattes, cool-teal shadows
_LUT_GOLDEN_HOUR = _build_lut(
    r_pts=[(0, 18), (64, 74), (128, 140), (192, 212), (255, 255)],
    g_pts=[(0, 20), (64, 72), (128, 136), (192, 200), (255, 248)],
    b_pts=[(0, 32), (64, 68), (128, 118), (192, 162), (255, 210)],
)

# moody: deep blue-green shadows, slight desaturation, filmic contrast
_LUT_MOODY = _build_lut(
    r_pts=[(0, 8),  (64, 56), (128, 120), (192, 186), (255, 248)],
    g_pts=[(0, 16), (64, 64), (128, 124), (192, 184), (255, 244)],
    b_pts=[(0, 45), (64, 90), (128, 142), (192, 194), (255, 238)],
)

# vibrant: punchy, saturated, warm tones for outdoor/travel/nature
_LUT_VIBRANT = _build_lut(
    r_pts=[(0, 12), (64, 68), (128, 136), (192, 208), (255, 255)],
    g_pts=[(0, 18), (64, 78), (128, 142), (192, 202), (255, 252)],
    b_pts=[(0, 28), (64, 68), (128, 124), (192, 172), (255, 218)],
)

# dramatic: crushed blacks, strong contrast, slight warm cast — epic landscapes
_LUT_DRAMATIC = _build_lut(
    r_pts=[(0, 4),  (64, 52), (128, 128), (192, 208), (255, 255)],
    g_pts=[(0, 6),  (64, 55), (128, 126), (192, 196), (255, 250)],
    b_pts=[(0, 10), (64, 56), (128, 120), (192, 178), (255, 230)],
)

# soft/dreamy: pastel, lifted shadows, slight pink-lavender cast — intimate/portrait
_LUT_SOFT = _build_lut(
    r_pts=[(0, 32), (64, 82), (128, 140), (192, 198), (255, 248)],
    g_pts=[(0, 28), (64, 78), (128, 136), (192, 194), (255, 244)],
    b_pts=[(0, 42), (64, 86), (128, 136), (192, 192), (255, 238)],
)

# flat/cinema (default): orange-teal travel preset
_LUT_FLAT = _build_lut(
    r_pts=[(0, 15), (64,  68), (128, 133), (192, 200), (255, 255)],
    g_pts=[(0, 22), (64,  74), (128, 133), (192, 195), (255, 250)],
    b_pts=[(0, 35), (64,  78), (128, 128), (192, 180), (255, 225)],
)

_MOOD_LUTS = {
    'golden_hour': _LUT_GOLDEN_HOUR,
    'moody':       _LUT_MOODY,
    'vibrant':     _LUT_VIBRANT,
    'dramatic':    _LUT_DRAMATIC,
    'soft':        _LUT_SOFT,
    'flat':        _LUT_FLAT,
}


def apply_vibrance(img: Image.Image, amount: float = 0.32) -> Image.Image:
    arr = np.array(img, dtype=np.float32) / 255.0
    sat  = np.max(arr, axis=2) - np.min(arr, axis=2)
    boost = (1.0 + amount * (1.0 - sat))[..., np.newaxis]
    lum   = (0.299 * arr[..., 0] + 0.587 * arr[..., 1] + 0.114 * arr[..., 2])[..., np.newaxis]
    result = lum + (arr - lum) * boost
    return Image.fromarray(np.clip(result * 255, 0, 255).astype(np.uint8))


def apply_vignette(img: Image.Image, strength: float = 0.22) -> Image.Image:
    arr = np.array(img, dtype=np.float32)
    h, w = arr.shape[:2]
    Y = np.linspace(-1, 1, h)[:, np.newaxis]
    X = np.linspace(-1, 1, w)[np.newaxis, :]
    dist = np.sqrt(X**2 + Y**2) / math.sqrt(2)
    mask = 1.0 - strength * np.clip((dist - 0.45) / 0.55, 0, 1) ** 1.6
    return Image.fromarray(np.clip(arr * mask[..., np.newaxis], 0, 255).astype(np.uint8))


def apply_grain(img: Image.Image, amount: float = 3.5) -> Image.Image:
    arr   = np.array(img, dtype=np.float32)
    noise = np.random.normal(0, amount, arr.shape[:2])
    for c in range(3):
        arr[..., c] += noise
    return Image.fromarray(np.clip(arr, 0, 255).astype(np.uint8))


def grade_photo(img: Image.Image, mood: str = 'flat') -> Image.Image:
    """Full cinematic grade with mood-matched LUT + per-mood colour adjustments."""
    lut    = _MOOD_LUTS.get(mood, _LUT_FLAT)
    graded = img.point(lut)

    if mood == 'golden_hour':
        graded = ImageEnhance.Color(graded).enhance(1.10)
        graded = apply_vibrance(graded, amount=0.38)
    elif mood == 'moody':
        graded = ImageEnhance.Color(graded).enhance(0.88)
        graded = ImageEnhance.Contrast(graded).enhance(1.08)
        graded = apply_vibrance(graded, amount=0.20)
    elif mood == 'vibrant':
        graded = ImageEnhance.Color(graded).enhance(1.18)
        graded = apply_vibrance(graded, amount=0.45)
    elif mood == 'dramatic':
        graded = ImageEnhance.Color(graded).enhance(0.95)
        graded = ImageEnhance.Contrast(graded).enhance(1.15)
        graded = apply_vibrance(graded, amount=0.22)
    elif mood == 'soft':
        graded = ImageEnhance.Color(graded).enhance(0.92)
        graded = apply_vibrance(graded, amount=0.25)
    else:
        graded = ImageEnhance.Color(graded).enhance(1.06)
        graded = apply_vibrance(graded, amount=0.32)

    graded = graded.filter(ImageFilter.UnsharpMask(radius=0.9, percent=65, threshold=3))
    graded = apply_vignette(graded, strength=0.22)
    graded = apply_grain(graded, amount=3.5)
    return graded


def smart_crop(img: Image.Image, target_w: int, target_h: int) -> Image.Image:
    src_w, src_h = img.size
    target_ratio = target_w / target_h
    src_ratio    = src_w   / src_h

    if src_ratio > target_ratio:
        new_w = int(src_h * target_ratio)
        left  = (src_w - new_w) // 2
        img   = img.crop((left, 0, left + new_w, src_h))
    else:
        new_h = int(src_w / target_ratio)
        top   = int((src_h - new_h) * 0.38)  # slight upward bias for sky/headroom
        img   = img.crop((0, top, src_w, top + new_h))

    return img.resize((target_w, target_h), Image.LANCZOS)


# ── Collage maker ──────────────────────────────────────────────────────────────
def make_collage(image_paths: list, mood: str = 'flat') -> Image.Image:
    """
    3×2 grid collage from the 6 highest-scoring shots — gives an at-a-glance
    overview of the whole trip as the first carousel image.
    """
    cols, rows = 3, 2
    gap      = 8
    cell_w   = (COLLAGE_W - gap * (cols + 1)) // cols
    cell_h   = (COLLAGE_H - gap * (rows + 1)) // rows
    canvas   = Image.new('RGB', (COLLAGE_W, COLLAGE_H), (12, 12, 12))

    for i, path in enumerate(image_paths[:cols * rows]):
        try:
            img    = load_image(path)
            graded = grade_photo(img, mood)
            cell   = smart_crop(graded, cell_w, cell_h)
            col_i  = i % cols
            row_i  = i // cols
            x      = gap + col_i * (cell_w + gap)
            y      = gap + row_i * (cell_h + gap)
            canvas.paste(cell, (x, y))
        except Exception as e:
            log.debug(f"Collage cell failed {path}: {e}")

    return canvas


# ── Edit step ──────────────────────────────────────────────────────────────────
def edit(dry_run=False, album_filter=None):
    conn     = open_db()
    work_dir = INSTAGRAM_DIR / "_graded"
    work_dir.mkdir(parents=True, exist_ok=True)

    valid    = _valid_album_names(album_filter)
    all_rows = conn.execute("SELECT DISTINCT album_name FROM scores").fetchall()
    albums   = [r for r in all_rows if r['album_name'] in valid]

    total = 0
    for row in albums:
        album_name = row['album_name']

        # Determine dominant mood for this album → choose LUT preset
        mood_row = conn.execute("""
            SELECT ai_mood, COUNT(*) AS cnt FROM scores
            WHERE album_name=? GROUP BY ai_mood ORDER BY cnt DESC LIMIT 1
        """, (album_name,)).fetchone()
        mood = mood_row['ai_mood'] if mood_row and mood_row['ai_mood'] else 'flat'
        mood = mood if mood in _MOOD_LUTS else 'flat'

        photos = conn.execute("""
            SELECT file_path, final_score FROM scores
            WHERE album_name=? AND resolution_ok=1
            ORDER BY final_score DESC
        """, (album_name,)).fetchall()

        cutoff     = max(MAX_CAROUSEL * 2, int(len(photos) * 0.30))
        candidates = photos[:cutoff]

        if dry_run:
            log.info(f"  {album_name}: would grade {len(candidates)} photos (mood={mood})")
            continue

        graded_paths = []
        for p in candidates:
            path     = Path(p['file_path'])
            out_path = work_dir / f"{path.stem}_graded.jpg"
            graded_paths.append((path, out_path))
            if not path.exists() or out_path.exists():
                continue
            try:
                img     = load_image(path)
                graded  = grade_photo(img, mood)
                cropped = smart_crop(graded, CAROUSEL_W, CAROUSEL_H)
                cropped.save(out_path, 'JPEG', quality=95, optimize=True)
                total += 1
            except Exception as e:
                log.warning(f"  Edit failed {path.name}: {e}")

        # Build collage from top-6 graded images for large albums
        if len(candidates) >= 6:
            collage_path = work_dir / f"__collage_{safe_name(album_name)}.jpg"
            if not collage_path.exists():
                top6 = [Path(p['file_path']) for p in candidates[:6] if Path(p['file_path']).exists()]
                if len(top6) >= 4:
                    try:
                        collage = make_collage(top6, mood=mood)
                        collage.save(collage_path, 'JPEG', quality=95, optimize=True)
                        log.info(f"  Collage saved: {collage_path.name}")
                    except Exception as e:
                        log.warning(f"  Collage failed '{album_name}': {e}")

    log.info(f"Edit complete: {total} photos graded → {work_dir}")
    conn.close()


# ── Curate step ────────────────────────────────────────────────────────────────
def curate(dry_run=False, album_filter=None):
    conn     = open_db()
    work_dir = INSTAGRAM_DIR / "_graded"

    valid    = _valid_album_names(album_filter)
    all_rows = conn.execute("SELECT DISTINCT album_name FROM scores").fetchall()
    albums   = [r for r in all_rows if r['album_name'] in valid]

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

        # Sort chronologically (filename encodes time) for trip narrative
        by_time = sorted(photos, key=lambda p: p['file_path'])
        n       = min(MAX_CAROUSEL, len(by_time))

        # Divide trip into N segments — pick highest-scoring shot from each
        selected = []
        seg_size = len(by_time) / n
        for i in range(n):
            start   = int(i * seg_size)
            end     = int((i + 1) * seg_size)
            segment = by_time[start:end]
            if segment:
                selected.append(max(segment, key=lambda p: p['final_score']))

        selected.sort(key=lambda p: p['file_path'])

        if dry_run:
            log.info(f"  {album_name}: would select {len(selected)} photos")
            for i, p in enumerate(selected):
                log.info(f"    {i+1}. {Path(p['file_path']).name}  score={p['final_score']}  mood={p['ai_mood']}")
            continue

        conn.execute("DELETE FROM curated WHERE album_name=?", (album_name,))
        for rank, p in enumerate(selected, 1):
            path        = Path(p['file_path'])
            graded      = work_dir / f"{path.stem}_graded.jpg"
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

_FONT_LIGHT_CANDIDATES = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    "/usr/share/fonts/truetype/ubuntu/Ubuntu-L.ttf",
    "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
]


def _font(size: int, bold: bool = True):
    candidates = _FONT_CANDIDATES if bold else _FONT_LIGHT_CANDIDATES
    for path in candidates:
        if Path(path).exists():
            return ImageFont.truetype(path, size)
    return ImageFont.load_default()


def _apply_gradient_bar(img: Image.Image, position: str,
                         height: int = 240, strength: float = 0.78) -> Image.Image:
    """Fade-to-black gradient at top or bottom for text legibility."""
    arr = np.array(img, dtype=np.float32)
    h   = arr.shape[0]
    if position == 'top':
        fade = np.linspace(strength, 0.0, height)[:, np.newaxis, np.newaxis]
        arr[:height] *= (1.0 - fade)
    elif position == 'bottom':
        fade = np.linspace(0.0, strength, height)[:, np.newaxis, np.newaxis]
        arr[h - height:] *= (1.0 - fade)
    return Image.fromarray(np.clip(arr, 0, 255).astype(np.uint8))


def make_story(img: Image.Image, location: str = '', date: str = '') -> Image.Image:
    """
    9:16 canvas: blurred + darkened photo fills the background,
    the graded photo sits centred with a thin white border,
    gradient bars at top/bottom frame the location and date text.
    """
    # Background: blurred + darkened
    bg_arr = np.array(smart_crop(img.copy(), STORY_W, STORY_H), dtype=np.float32)
    bg_arr = np.clip(bg_arr * 0.38, 0, 255).astype(np.uint8)
    bg     = Image.fromarray(bg_arr).filter(ImageFilter.GaussianBlur(radius=30))

    # Add gradient bars for text zones
    bg = _apply_gradient_bar(bg, 'top',    height=260, strength=0.72)
    bg = _apply_gradient_bar(bg, 'bottom', height=200, strength=0.65)

    # Foreground: 91% story width, white border
    fw       = int(STORY_W * 0.91)
    fh       = int(fw * CAROUSEL_H / CAROUSEL_W)
    photo    = smart_crop(img, fw, fh)
    bordered = Image.new('RGB', (fw + 8, fh + 8), (255, 255, 255))
    bordered.paste(photo, (4, 4))

    x = (STORY_W - bordered.width) // 2
    y = int((STORY_H - bordered.height) * 0.44)
    bg.paste(bordered, (x, y))

    draw = ImageDraw.Draw(bg)

    # Location name — bold, large, all-caps with letter-spacing effect
    if location:
        font_loc  = _font(52, bold=True)
        text      = location.upper()
        # Manual letter-spacing by drawing char by char
        bbox_full = draw.textbbox((0, 0), text, font=font_loc)
        tw        = bbox_full[2] - bbox_full[0]
        tx        = (STORY_W - tw) // 2
        # Shadow + white
        draw.text((tx + 2, 88), text, font=font_loc, fill=(0, 0, 0, 180))
        draw.text((tx,     86), text, font=font_loc, fill=(255, 255, 255))

    # Thin separator line under location
    if location:
        line_y = 152
        line_w = 120
        lx     = (STORY_W - line_w) // 2
        draw.rectangle([lx, line_y, lx + line_w, line_y + 2], fill=(255, 255, 255, 160))

    # Date — lighter weight, bottom
    if date:
        font_date = _font(36, bold=False)
        try:
            from datetime import datetime as _dt
            d        = _dt.fromisoformat(date[:10])
            date_str = d.strftime('%B %Y').upper()
        except Exception:
            date_str = date[:10].upper()
        bbox  = draw.textbbox((0, 0), date_str, font=font_date)
        tw    = bbox[2] - bbox[0]
        tx    = (STORY_W - tw) // 2
        draw.text((tx + 1, STORY_H - 108), date_str, font=font_date, fill=(0, 0, 0))
        draw.text((tx,     STORY_H - 110), date_str, font=font_date, fill=(210, 210, 210))

    return bg


def stories(dry_run=False, album_filter=None):
    conn = open_db()

    valid    = _valid_album_names(album_filter)
    all_rows = conn.execute("SELECT DISTINCT album_name FROM curated").fetchall()
    albums   = [r for r in all_rows if r['album_name'] in valid]

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
def _season_from_date(date_str: str) -> str:
    try:
        from datetime import datetime as _dt
        m = _dt.fromisoformat(date_str[:10]).month
        if m in (12, 1, 2):  return 'winter'
        if m in (3, 4, 5):   return 'spring'
        if m in (6, 7, 8):   return 'summer'
        return 'autumn'
    except Exception:
        return ''


_CAPTION_PROMPT = """\
You are a travel photographer with a distinctive voice — poetic, personal, never clichéd.

Album:      {album_name}
Location:   {location}
Date:       {date}
Season:     {season}
Photo mood: {mood}
Scene:      {description}
Trip span:  {duration}

Write an Instagram carousel caption. Rules:
- 2-3 sentences: evocative first-person, specific sensory detail, NOT generic travel phrases
  ("wanderlust", "adventure awaits", "living my best life" are banned)
- Mention one specific detail from the scene description if possible
- 20-25 hashtags: mix of specific location, mood, photography style, and seasonal tags
- 2-3 emojis woven naturally into the text
- alt_text: one plain-English sentence describing the images for accessibility

Reply ONLY with valid JSON, nothing else:
{{"caption": "...", "hashtags": ["tag1", "tag2", ...], "alt_text": "..."}}"""


def caption(dry_run=False, album_filter=None):
    conn = open_db()

    valid    = _valid_album_names(album_filter)
    all_rows = conn.execute("SELECT DISTINCT album_name FROM curated").fetchall()
    albums   = [r for r in all_rows if r['album_name'] in valid]

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

        meta      = album_meta(album_name)
        score_row = conn.execute(
            "SELECT ai_mood, ai_best_feature FROM scores WHERE file_path=?",
            (hero['file_path'],)
        ).fetchone()
        photo_meta = pipeline_meta(hero['file_path'])

        loc_parts = [p for p in [meta.get('city'), meta.get('country')] if p]
        location  = ', '.join(loc_parts) if loc_parts else album_name
        date      = (meta.get('start_date') or photo_meta.get('best_date', ''))[:10]
        season    = _season_from_date(date)
        mood      = (score_row['ai_mood'] if score_row else 'travel')
        desc      = (photo_meta.get('ai_description') or
                     (score_row['ai_best_feature'] if score_row else ''))

        # Trip duration
        end_date   = meta.get('end_date', '')
        duration   = ''
        if date and end_date and date != end_date:
            try:
                from datetime import datetime as _dt
                days = (_dt.fromisoformat(end_date[:10]) - _dt.fromisoformat(date)).days
                if days == 1:
                    duration = '2-day trip'
                elif days < 7:
                    duration = f'{days+1}-day trip'
                elif days < 14:
                    duration = f'{(days+6)//7}-week trip'
                else:
                    duration = f'{days} days'
            except Exception:
                pass

        prompt = _CAPTION_PROMPT.format(
            album_name=album_name, location=location,
            date=date, season=season, mood=mood,
            description=desc, duration=duration or 'unknown',
        )

        if dry_run:
            log.info(f"  Would generate caption for '{album_name}' ({location}, {season})")
            continue

        try:
            hero_img = load_image(Path(hero['edited_path'] or hero['file_path']))
            raw      = ollama_vision(prompt, hero_img)
            data     = parse_json(raw)
            if not data.get('caption'):
                # Fall back to text-only if vision parse failed
                raw  = ollama_text(prompt)
                data = parse_json(raw)
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
    conn     = open_db()
    work_dir = INSTAGRAM_DIR / "_graded"

    valid    = _valid_album_names(album_filter)
    all_rows = conn.execute("SELECT DISTINCT album_name FROM curated").fetchall()
    albums   = [r for r in all_rows if r['album_name'] in valid]

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

        # Prepend collage as first carousel image if available
        collage_src = work_dir / f"__collage_{safe_name(album_name)}.jpg"
        if collage_src.exists():
            dst = carousel_dir / f"00_collage.jpg"
            if not dst.exists():
                shutil.copy2(collage_src, dst)

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


# ── Discover step ──────────────────────────────────────────────────────────────
_TRAVEL_RE = re.compile(
    r'\b(holiday|trip|visit|tour|explore|walk|hike|summer|winter|spring|autumn|'
    r'adventure|break|weekend|journey|japan|spain|france|italy|germany|thailand|'
    r'bali|greece|amsterdam|barcelona|paris|london|edinburgh|iceland|norway|'
    r'sweden|austria|portugal|zealand|australia|canada|scotland|ireland|wales|'
    r'croatia|turkey|morocco|peru|mexico|usa|texas|colorado|california|florida|'
    r'tokyo|kyoto|osaka|berlin|rome|venice|florence|prague|budapest|lisbon|'
    r'athens|dubai|singapore|hongkong|maldives|safari|beach|mountain|ski|snow)\b',
    re.IGNORECASE
)


def discover(dry_run=False, album_filter=None):
    """Rank all event albums by Instagram potential and print a priority list."""
    albums = find_albums(album_filter)
    conn   = open_db()

    rows = []
    for album_dir in albums:
        photos = [p for p in album_dir.iterdir() if is_photo(p)]
        n      = len(photos)

        scored_row = conn.execute(
            "SELECT AVG(final_score), AVG(ai_score), COUNT(*) FROM scores WHERE album_name=?",
            (album_dir.name,)
        ).fetchone()
        avg_score = scored_row[0] or 0.0
        avg_ai    = scored_row[1] or 0.0
        scored_n  = scored_row[2] or 0

        curated_n = conn.execute(
            "SELECT COUNT(*) FROM curated WHERE album_name=?", (album_dir.name,)
        ).fetchone()[0]
        has_caption = bool(conn.execute(
            "SELECT id FROM captions WHERE album_name=?", (album_dir.name,)
        ).fetchone())
        exported = (INSTAGRAM_DIR / safe_name(album_dir.name) / "caption.txt").exists()

        is_travel = bool(_TRAVEL_RE.search(album_dir.name))
        potential = (
            min(n / 8.0, 4.0) +
            (avg_score / 20.0 if scored_n else 0) +
            (2.5 if is_travel else 0) +
            (1.0 if scored_n  else 0) +
            (0.5 if curated_n else 0)
        )

        if exported:
            status = "READY   "
        elif curated_n:
            status = "curated "
        elif scored_n:
            status = "scored  "
        else:
            status = "pending "

        rows.append((potential, album_dir.name, n, avg_score, status))

    conn.close()
    rows.sort(reverse=True)

    ready   = sum(1 for r in rows if 'READY' in r[4])
    scored  = sum(1 for r in rows if 'scored' in r[4])
    pending = sum(1 for r in rows if 'pending' in r[4])

    print(f"\n{'─'*76}")
    print(f"  ALBUM DISCOVERY  "
          f"({len(rows)} albums · {ready} ready · {scored} scored · {pending} pending)")
    print(f"{'─'*76}")
    print(f"  {'ALBUM':<44} {'PHOTOS':>6}  {'SCORE':>5}  STATUS")
    print(f"{'─'*76}")
    for potential, name, n, score, status in rows:
        score_str = f"{score:5.1f}" if score else "   — "
        print(f"  {name:<44} {n:>6}  {score_str}  {status}")
    print(f"{'─'*76}")
    print(f"\n  Output folder: {INSTAGRAM_DIR}\n")


# ── Purge step ─────────────────────────────────────────────────────────────────
def purge(dry_run=False, album_filter=None):
    """Remove stale/noise albums from instagram.db that aren't in find_albums()."""
    valid = _valid_album_names()
    conn  = open_db()

    all_scored = [r[0] for r in conn.execute("SELECT DISTINCT album_name FROM scores").fetchall()]
    stale      = [n for n in all_scored if n not in valid]

    if not stale:
        log.info("Purge: nothing to clean up.")
        conn.close()
        return

    log.info(f"Purge: {len(stale)} noise albums → {len(valid)} valid albums kept")
    if dry_run:
        for name in stale[:30]:
            log.info(f"  Would remove: {name!r}")
        if len(stale) > 30:
            log.info(f"  ... and {len(stale)-30} more")
        conn.close()
        return

    for name in stale:
        conn.execute("DELETE FROM scores  WHERE album_name=?", (name,))
        conn.execute("DELETE FROM curated WHERE album_name=?", (name,))
        conn.execute("DELETE FROM captions WHERE album_name=?", (name,))
    conn.commit()
    log.info(f"Purge complete: removed {len(stale)} noise albums.")
    conn.close()


# ── Summary ────────────────────────────────────────────────────────────────────
def summary(dry_run=False, album_filter=None):
    conn   = open_db()
    valid  = _valid_album_names(album_filter)

    if not valid:
        print("\nNo valid event albums found. Check FINAL_DIR in your .env")
        conn.close()
        return

    rows = []
    for name in sorted(valid):
        scored    = conn.execute("SELECT COUNT(*) FROM scores WHERE album_name=?",  (name,)).fetchone()[0]
        curated_n = conn.execute("SELECT COUNT(*) FROM curated WHERE album_name=?", (name,)).fetchone()[0]
        stories_d = INSTAGRAM_DIR / safe_name(name) / "stories"
        story_n   = len(list(stories_d.glob("*.jpg"))) if stories_d.exists() else 0
        has_cap   = bool(conn.execute("SELECT id FROM captions WHERE album_name=?", (name,)).fetchone())
        exported  = (INSTAGRAM_DIR / safe_name(name) / "caption.txt").exists()

        steps = []
        if scored:    steps.append(f"scored({scored})")
        if curated_n: steps.append(f"curated({curated_n})")
        if story_n:   steps.append(f"stories({story_n})")
        if has_cap:   steps.append("caption")
        if exported:  steps.append("EXPORTED")

        status = "  READY  " if exported else " partial " if steps else " pending "
        rows.append((status, name, ", ".join(steps) or "—"))

    conn.close()

    ready   = sum(1 for r in rows if "READY"   in r[0])
    partial = sum(1 for r in rows if "partial" in r[0])

    print(f"\n{'─'*76}")
    print(f"  INSTAGRAM ALBUMS  ({ready} ready, {partial} partial, {len(rows)} total)")
    print(f"{'─'*76}")
    col = min(max(len(r[1]) for r in rows) + 2, 50)
    for status, name, progress in rows:
        print(f"  [{status}]  {name:<{col}}  {progress}")
    print(f"{'─'*76}")
    if ready:
        print(f"\n  Output folder: {INSTAGRAM_DIR}")
    print()


# ── Main ───────────────────────────────────────────────────────────────────────
STEPS = {
    'discover': discover,
    'purge':    purge,
    'score':    score,
    'edit':     edit,
    'curate':   curate,
    'stories':  stories,
    'caption':  caption,
    'export':   export,
    'summary':  summary,
}


def main():
    parser = argparse.ArgumentParser(
        description="Instagram pipeline: discover → purge → score → edit → curate → stories → caption → export"
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
    kwargs         = {'dry_run': args.dry_run, 'album_filter': args.album}
    pipeline_steps = ['purge', 'score', 'edit', 'curate', 'stories', 'caption', 'export']

    if args.step == 'all':
        for name in pipeline_steps:
            log.info(f"\n── {name} ──────────────────────────────────────")
            STEPS[name](**kwargs)
        summary(**kwargs)
    else:
        STEPS[args.step](**kwargs)


if __name__ == '__main__':
    main()
