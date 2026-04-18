#!/usr/bin/env python3
"""
immich_albums.py — Create Immich albums from event-folder structure.

Scans the GooglePhotos directory for event folders (anything that isn't a
"Photos from YYYY" folder), matches their files against assets already in
Immich, then creates albums and populates them.

Idempotent: skips albums that already exist and assets already in an album.

Usage:
    python3 immich_albums.py [--photos-dir DIR] [--dry-run]

Environment variables (or edit defaults below):
    IMMICH_URL      Base URL of Immich instance (default: http://localhost:2283)
    IMMICH_API_KEY  API key
    PHOTOS_DIR      Root GooglePhotos directory to scan
"""

import argparse
import os
import sys
import time
from pathlib import Path

import requests

# ── Defaults ──────────────────────────────────────────────────────────────────
IMMICH_URL    = os.environ.get("IMMICH_URL",    "http://localhost:2283")
IMMICH_API_KEY = os.environ.get("IMMICH_API_KEY", "oT4gxVnyYPiiIJtHnSkVrSvmyvWmodCd3yoLY5WSxQ")
PHOTOS_DIR    = Path(os.environ.get("PHOTOS_DIR",  "/home/elgan/Pictures/GooglePhotos"))

# Folders matching this prefix are raw Takeout dumps, not event albums
SKIP_PREFIX = "Photos from "

# ── HTTP helpers ───────────────────────────────────────────────────────────────

def session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"x-api-key": IMMICH_API_KEY, "Content-Type": "application/json"})
    return s


def get_all_assets(s: requests.Session) -> dict[str, str]:
    """Return {originalPath: assetId} for every asset in Immich."""
    print("Fetching all assets from Immich (this may take a while)…")
    path_to_id: dict[str, str] = {}
    page = 1
    page_size = 1000
    while True:
        resp = s.post(
            f"{IMMICH_URL}/api/search/metadata",
            json={"size": page_size, "page": page},
        )
        resp.raise_for_status()
        data = resp.json()
        assets_data = data.get("assets", {})
        items = assets_data.get("items", [])
        if not items:
            break
        for asset in items:
            path_to_id[asset["originalPath"]] = asset["id"]
        print(f"  {len(path_to_id)} assets indexed…", end="\r")
        # Use nextPage field — the 'total' field is capped at page_size in this API
        if not assets_data.get("nextPage"):
            break
        page += 1
    print(f"\n  Done — {len(path_to_id)} assets indexed.")
    return path_to_id


def get_existing_albums(s: requests.Session) -> dict[str, str]:
    """Return {albumName: albumId} for all existing albums."""
    resp = s.get(f"{IMMICH_URL}/api/albums")
    resp.raise_for_status()
    return {a["albumName"]: a["id"] for a in resp.json()}


def create_album(s: requests.Session, name: str, asset_ids: list[str], dry_run: bool) -> str | None:
    if dry_run:
        print(f"  [dry-run] Would create album '{name}' with {len(asset_ids)} assets")
        return None
    resp = s.post(f"{IMMICH_URL}/api/albums", json={"albumName": name, "assetIds": asset_ids})
    resp.raise_for_status()
    return resp.json()["id"]


def add_assets_to_album(s: requests.Session, album_id: str, asset_ids: list[str], dry_run: bool) -> int:
    if dry_run:
        print(f"  [dry-run] Would add {len(asset_ids)} assets to existing album")
        return 0
    # Immich accepts max 1000 assets per request
    added = 0
    for i in range(0, len(asset_ids), 1000):
        chunk = asset_ids[i : i + 1000]
        resp = s.put(f"{IMMICH_URL}/api/albums/{album_id}/assets", json={"ids": chunk})
        resp.raise_for_status()
        results = resp.json()
        added += sum(1 for r in results if r.get("success"))
    return added


# ── Main ──────────────────────────────────────────────────────────────────────

def scan_event_folders(photos_dir: Path) -> list[Path]:
    """Return subdirectories that represent event albums (not raw year folders or hidden dirs)."""
    return sorted(
        d for d in photos_dir.iterdir()
        if d.is_dir()
        and not d.name.startswith(SKIP_PREFIX)
        and not d.name.startswith(".")
    )


def collect_files(folder: Path) -> list[Path]:
    """Recursively collect all files under an event folder."""
    return [f for f in folder.rglob("*") if f.is_file()]


def main():
    parser = argparse.ArgumentParser(description="Sync event folders to Immich albums")
    parser.add_argument("--photos-dir", default=str(PHOTOS_DIR),
                        help="Root GooglePhotos directory (default: %(default)s)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would happen without making changes")
    args = parser.parse_args()

    photos_dir = Path(args.photos_dir)
    dry_run = args.dry_run

    if not photos_dir.exists():
        print(f"ERROR: photos dir not found: {photos_dir}", file=sys.stderr)
        sys.exit(1)

    s = session()

    # Verify connectivity
    try:
        s.get(f"{IMMICH_URL}/api/server/about").raise_for_status()
    except Exception as e:
        print(f"ERROR: Cannot reach Immich at {IMMICH_URL}: {e}", file=sys.stderr)
        sys.exit(1)

    path_to_id   = get_all_assets(s)
    existing     = get_existing_albums(s)
    event_folders = scan_event_folders(photos_dir)

    print(f"\nFound {len(event_folders)} event folders to process.")
    if dry_run:
        print("DRY RUN — no changes will be made.\n")

    stats = {"created": 0, "updated": 0, "skipped": 0, "total_assets": 0}

    for folder in event_folders:
        album_name = folder.name
        files = collect_files(folder)
        if not files:
            continue

        # Match files to Immich asset IDs
        asset_ids = [path_to_id[str(f)] for f in files if str(f) in path_to_id]
        if not asset_ids:
            print(f"  SKIP '{album_name}' — no matching Immich assets for {len(files)} files")
            stats["skipped"] += 1
            continue

        if album_name in existing:
            album_id = existing[album_name]
            added = add_assets_to_album(s, album_id, asset_ids, dry_run)
            if added:
                print(f"  UPDATE '{album_name}' — added {added} assets (album already existed)")
                stats["updated"] += 1
                stats["total_assets"] += added
            else:
                stats["skipped"] += 1
        else:
            album_id = create_album(s, album_name, asset_ids, dry_run)
            if album_id:
                print(f"  CREATE '{album_name}' — {len(asset_ids)} assets")
                stats["created"] += 1
                stats["total_assets"] += len(asset_ids)
            elif dry_run:
                stats["created"] += 1
                stats["total_assets"] += len(asset_ids)

    print(f"\nDone. Created: {stats['created']}  Updated: {stats['updated']}  "
          f"Skipped: {stats['skipped']}  Total assets assigned: {stats['total_assets']}")


if __name__ == "__main__":
    main()
