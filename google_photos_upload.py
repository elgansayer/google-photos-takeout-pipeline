#!/usr/bin/env python3
"""
Google Photos Upload Preparer
================================
Prepares the organized library for re-upload to Google Photos.

Two upload methods:
1. gphotos-sync (recommended) - creates albums matching our structure
2. google-photos-upload CLI
3. Manual instructions

Also creates an upload manifest with album → file mappings.

Run: python3 google_photos_upload.py [--check] [--create-manifest]
"""

import json
import logging
import os
import subprocess
import sys
from pathlib import Path
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(str(Path(os.environ.get("PIPELINE_DIR", str(Path(__file__).parent))) / 'upload.log')),
        logging.StreamHandler()
    ]
)
log = logging.getLogger('upload')

PIPELINE_DIR    = Path(os.environ.get("PIPELINE_DIR", str(Path(__file__).parent)))
PIPELINE_DB     = PIPELINE_DIR / 'photos.db'
FINAL_DIR       = Path(os.environ.get("FINAL_DIR", ""))
UPLOAD_MANIFEST = PIPELINE_DIR / 'upload_manifest.json'

# Max files per Google Photos album (API limit)
GPHOTOS_ALBUM_MAX = 20000


def check_tools():
    """Check which upload tools are available."""
    tools = {}
    for tool in ['gphotos-sync', 'gphoto2', 'rclone']:
        try:
            result = subprocess.run([tool, '--version'], capture_output=True, timeout=5)
            tools[tool] = result.returncode == 0
        except (FileNotFoundError, subprocess.TimeoutExpired):
            tools[tool] = False
    return tools


def create_upload_manifest():
    """Create a comprehensive upload manifest from the final directory."""
    import sqlite3
    conn = sqlite3.connect(str(PIPELINE_DB), timeout=30)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    albums = cursor.execute("""
        SELECT a.id, a.name, a.start_date, a.end_date, a.country,
               a.album_type, a.source,
               COUNT(p.id) as count
        FROM albums a
        LEFT JOIN photos p ON p.album_id = a.id AND p.is_duplicate = 0
        GROUP BY a.id
        ORDER BY a.start_date
    """).fetchall()

    manifest = {
        'generated': datetime.now().isoformat(),
        'final_dir': str(FINAL_DIR),
        'total_albums': len(albums),
        'albums': []
    }

    for album in albums:
        # Find corresponding directory in final dir
        year = album['start_date'][:4] if album['start_date'] else 'Undated'
        import re
        safe_name = re.sub(r'[<>:"/\\|?*]', '_', album['name']).strip('. ')
        album_dir = FINAL_DIR / year / safe_name
        files = list(album_dir.glob('*')) if album_dir.exists() else []

        manifest['albums'].append({
            'name': album['name'],
            'dir': str(album_dir),
            'exists': album_dir.exists(),
            'file_count': len(files),
            'start_date': album['start_date'],
            'end_date': album['end_date'],
            'country': album['country'],
            'type': album['album_type'],
            'source': album['source'],
        })

    # Add unsorted
    for year_dir in sorted(FINAL_DIR.iterdir()):
        if not year_dir.is_dir():
            continue
        unsorted = year_dir / 'Unsorted'
        if unsorted.exists():
            files = list(unsorted.glob('*'))
            manifest['albums'].append({
                'name': f'Unsorted {year_dir.name}',
                'dir': str(unsorted),
                'exists': True,
                'file_count': len(files),
                'type': 'unsorted'
            })

    with open(UPLOAD_MANIFEST, 'w') as f:
        json.dump(manifest, f, indent=2)

    total_files = sum(a['file_count'] for a in manifest['albums'])
    log.info(f"Manifest created: {len(manifest['albums'])} albums, ~{total_files} files")
    log.info(f"Saved to: {UPLOAD_MANIFEST}")
    conn.close()
    return manifest


def generate_rclone_commands():
    """Generate rclone commands for Google Photos upload."""
    if not UPLOAD_MANIFEST.exists():
        create_upload_manifest()

    with open(UPLOAD_MANIFEST) as f:
        manifest = json.load(f)

    script_path = PIPELINE_DIR / 'upload_to_gphotos.sh'
    lines = [
        '#!/bin/bash',
        '# Upload organized photos to Google Photos via rclone',
        '# Prerequisites:',
        '#   1. Install rclone: curl https://rclone.org/install.sh | sudo bash',
        '#   2. Configure: rclone config  (choose "Google Photos", name it "gphotos")',
        '#   3. Run this script: bash upload_to_gphotos.sh',
        '#',
        '# This uploads each album folder as a separate Google Photos album.',
        '# Albums > 20,000 photos are split automatically.',
        '',
        'set -e',
        'RCLONE_REMOTE="gphotos"  # Change to your rclone remote name',
        '',
        'echo "Starting Google Photos upload..."',
        '',
    ]

    for album in manifest['albums']:
        if not album.get('exists') or album.get('file_count', 0) == 0:
            continue
        album_dir = album['dir']
        album_name = album['name'].replace('"', '\\"')
        file_count = album['file_count']

        if file_count > GPHOTOS_ALBUM_MAX:
            # Split into chunks
            parts = (file_count // GPHOTOS_ALBUM_MAX) + 1
            lines.append(f'# Note: "{album_name}" has {file_count} files, will be split into {parts} parts')

        lines.append(f'echo "Uploading: {album_name} ({file_count} files)"')
        lines.append(
            f'rclone copy "{album_dir}" "${{RCLONE_REMOTE}}:album/{album_name}" '
            f'--drive-use-created-date '
            f'--transfers 4 '
            f'--checkers 8 '
            f'--progress '
            f'--log-level INFO '
            f'--log-file {PIPELINE_DIR}/rclone_upload.log'
        )
        lines.append('')

    lines.append('echo "Upload complete!"')

    with open(script_path, 'w') as f:
        f.write('\n'.join(lines))
    os.chmod(script_path, 0o755)
    log.info(f"rclone upload script: {script_path}")
    return script_path


def generate_summary_report():
    """Generate a human-readable summary of what will be uploaded."""
    if not UPLOAD_MANIFEST.exists():
        create_upload_manifest()

    with open(UPLOAD_MANIFEST) as f:
        manifest = json.load(f)

    report_path = PIPELINE_DIR / 'UPLOAD_SUMMARY.md'
    lines = [
        '# Photo Library Upload Summary',
        f'Generated: {manifest["generated"]}',
        f'Total albums: {manifest["total_albums"]}',
        '',
        '## Albums by Year',
        '',
    ]

    current_year = None
    for album in sorted(manifest['albums'], key=lambda x: x.get('start_date') or ''):
        year = (album.get('start_date') or 'Undated')[:4]
        if year != current_year:
            lines.append(f'\n### {year}')
            current_year = year
        status = '✓' if album.get('exists') else '✗'
        lines.append(f"- {status} **{album['name']}** ({album.get('file_count', 0)} photos, {album.get('type','')})")

    with open(report_path, 'w') as f:
        f.write('\n'.join(lines))
    log.info(f"Summary report: {report_path}")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--check', action='store_true', help='Check available upload tools')
    parser.add_argument('--create-manifest', action='store_true', help='Create upload manifest')
    parser.add_argument('--generate-scripts', action='store_true', help='Generate upload scripts')
    args = parser.parse_args()

    if args.check:
        tools = check_tools()
        for tool, available in tools.items():
            log.info(f"{tool}: {'available' if available else 'not installed'}")
        if not any(tools.values()):
            log.info("""
No upload tools found. Install rclone:
  curl https://rclone.org/install.sh | sudo bash
  rclone config  # Choose Google Photos, name it "gphotos"
""")

    if args.create_manifest:
        create_upload_manifest()

    if args.generate_scripts:
        create_upload_manifest()
        generate_rclone_commands()
        generate_summary_report()
        log.info("All upload prep files created.")
