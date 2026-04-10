#!/usr/bin/env python3
"""
Event Namer - Phase 7 helper
==============================
Takes photo clusters and asks ollama to generate descriptive event names.
"Skiing in Austria 2020" not "Austria 2020"
"Beach Holiday in Bali" not "Bali 2019"
"Christmas in Scotland" not "December 2020"

This runs AFTER basic clustering to improve album names.
Uses gemma3:4b (text model) for fast, cheap naming.

Run: python3 name_events.py [--dry-run]
"""

import json
import logging
import os
import re
import sqlite3
from collections import Counter
from datetime import datetime
from pathlib import Path

import requests

PIPELINE_DIR = Path(os.environ.get("PIPELINE_DIR", str(Path(__file__).parent)))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(PIPELINE_DIR / 'name_events.log'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger('name_events')

PIPELINE_DB = PIPELINE_DIR / 'photos.db'
OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
TEXT_MODEL  = os.environ.get("TEXT_MODEL", "gemma3:4b")

# Holidays and known event places in UK context
UK_PLACES = {'London', 'Edinburgh', 'Bristol', 'Bath', 'York', 'Cambridge', 'Oxford',
             'Brighton', 'Manchester', 'Liverpool', 'Glasgow', 'Cardiff', 'Belfast',
             'Bury St Edmunds', 'Keswick', 'Lake District', 'Peak District', 'Snowdonia',
             'Dartmoor', 'Pembroke', 'Pembrokeshire', 'Hove', 'Arundel', 'Friston',
             'Banham', 'Woodbridge', 'Duxford', 'Bedlinog', 'Bethesda', 'Debenham'}

HOLIDAY_COUNTRIES = {'Japan', 'France', 'Spain', 'Italy', 'Greece', 'Austria', 'Germany',
                     'Switzerland', 'Portugal', 'Croatia', 'Iceland', 'Norway', 'Sweden',
                     'Thailand', 'Indonesia', 'Vietnam', 'Australia', 'New Zealand',
                     'USA', 'Canada', 'Mexico', 'Brazil', 'Argentina', 'Peru',
                     'Morocco', 'South Africa', 'Kenya', 'Tanzania', 'Egypt',
                     'Bali', 'Singapore', 'Malaysia', 'Philippines', 'India', 'China',
                     'South Korea', 'Hong Kong', 'Taiwan', 'Turkey', 'Czech Republic',
                     'Poland', 'Hungary', 'Romania', 'Serbia', 'Montenegro', 'Bosnia',
                     'Belgium', 'Netherlands', 'Ireland', 'Malta', 'Cyprus', 'Luxembourg'}


def get_db():
    conn = sqlite3.connect(str(PIPELINE_DB), timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA busy_timeout=60000')
    return conn


def ask_ollama_for_name(cluster_context):
    """Ask gemma3:4b to generate an event name from cluster metadata."""
    prompt = f"""You are naming a photo album. Given these details about a group of photos, generate ONE concise, human-readable event album name.

Rules:
- Maximum 5 words
- Be descriptive of the EVENT, not just the location
- Include the YEAR at the end
- Examples: "Skiing in Austria 2020", "Beach Holiday Bali 2019", "Christmas in Scotland 2021", "Tokyo Highlights 2024", "Lake District Hiking 2023", "Family Reunion Yorkshire 2018", "New Year Amsterdam 2020"
- If it's clearly a holiday abroad, say what kind: beach, skiing, city break, hiking, etc.
- If it's a local UK event: say what the event was (wedding, birthday, festival, climbing, walking)
- Do NOT use words like "trip", "visit", "photos", "album", "collection"
- Respond with ONLY the album name, nothing else

Photo cluster details:
{cluster_context}

Album name:"""

    try:
        resp = requests.post(f'{OLLAMA_HOST}/api/generate', json={
            'model': TEXT_MODEL,
            'prompt': prompt,
            'stream': False,
            'options': {'temperature': 0.3, 'num_predict': 20}
        }, timeout=30)
        name = resp.json().get('response', '').strip()
        # Clean up
        name = name.strip('"\'').strip()
        name = re.sub(r'\s+', ' ', name)
        # Remove any markdown
        name = re.sub(r'\*+', '', name)
        if name and len(name) > 3 and len(name) < 80:
            return name
    except Exception as e:
        log.debug(f"Ollama naming failed: {e}")
    return None


def build_context(photos, album_name, album_type):
    """Build a text context for the naming model."""
    countries = [p['country'] for p in photos if p['country']]
    cities = [p['city'] for p in photos if p['city']]
    ai_tags = []
    ai_scenes = []
    for p in photos:
        if p['ai_tags']:
            try:
                ai_tags.extend(json.loads(p['ai_tags']))
            except:
                pass
        if p['ai_scene_type']:
            ai_scenes.append(p['ai_scene_type'])
        if p['ai_activity'] and p['ai_activity'] != 'none':
            ai_scenes.append(p['ai_activity'])

    dates = [p['best_date'] for p in photos if p['best_date']]
    start = end = year = month = ''
    if dates:
        try:
            start_dt = datetime.fromisoformat(min(dates)[:19])
            end_dt = datetime.fromisoformat(max(dates)[:19])
            year = start_dt.strftime('%Y')
            month = start_dt.strftime('%B')
            duration = (end_dt - start_dt).days
        except:
            duration = 0

    primary_country = Counter(countries).most_common(1)[0][0] if countries else None
    primary_city = Counter(cities).most_common(1)[0][0] if cities else None
    top_tags = [t for t, _ in Counter(ai_tags).most_common(5)]
    top_scenes = [s for s, _ in Counter(ai_scenes).most_common(3)]
    unique_countries = list(set(countries))

    is_abroad = primary_country and primary_country != 'United Kingdom'
    is_holiday = album_type == 'holiday' or primary_country in HOLIDAY_COUNTRIES

    lines = [f"Current auto-name: {album_name}"]
    if primary_country:
        if len(unique_countries) > 1:
            lines.append(f"Countries: {', '.join(sorted(set(unique_countries)))}")
        else:
            lines.append(f"Country: {primary_country}")
    if primary_city:
        lines.append(f"Main location: {primary_city}")
    if year:
        lines.append(f"Year: {year}, Month: {month}")
    if duration:
        lines.append(f"Duration: {duration} days")
    if top_tags:
        lines.append(f"Visual tags: {', '.join(top_tags)}")
    if top_scenes:
        lines.append(f"Scene types: {', '.join(top_scenes)}")
    if is_abroad:
        lines.append("Type: Holiday abroad")
    elif is_holiday:
        lines.append("Type: Domestic holiday/break")
    else:
        lines.append("Type: Local event or day out")
    lines.append(f"Photo count: {len(photos)}")

    return '\n'.join(lines)


def rename_albums(dry_run=False):
    """Rename auto-generated albums to event names using AI."""
    conn = get_db()
    cursor = conn.cursor()

    # Get albums to rename - only auto-generated ones, not existing Google albums
    albums = cursor.execute("""
        SELECT a.id, a.name, a.album_type, a.start_date, a.end_date, a.country, a.source
        FROM albums a
        WHERE a.source = 'auto'
        ORDER BY a.start_date
    """).fetchall()

    log.info(f"Found {len(albums)} auto-generated albums to rename")

    # Check ollama is up
    try:
        requests.get(f'{OLLAMA_HOST}/api/version', timeout=5)
    except Exception as e:
        log.error(f"Ollama not available: {e}")
        conn.close()
        return

    renamed = 0
    for album in albums:
        # Get photos in this album
        photos = cursor.execute("""
            SELECT best_date, country, city, ai_tags, ai_scene_type, ai_activity
            FROM photos WHERE album_id = ? AND is_duplicate = 0
        """, (album['id'],)).fetchall()

        if not photos:
            continue

        context = build_context(photos, album['name'], album['album_type'])
        new_name = ask_ollama_for_name(context)

        if not new_name or new_name == album['name']:
            continue

        # Ensure unique — loop with incrementing suffix
        base_name = new_name
        year = album['start_date'][:4] if album['start_date'] else str(album['id'])
        counter = 0
        while cursor.execute("SELECT id FROM albums WHERE name = ? AND id != ?",
                             (new_name, album['id'])).fetchone():
            counter += 1
            new_name = f"{base_name} ({year})" if counter == 1 else f"{base_name} ({year}-{counter})"

        log.info(f"  '{album['name']}' → '{new_name}'")

        if not dry_run:
            cursor.execute("UPDATE OR IGNORE albums SET name = ? WHERE id = ?", (new_name, album['id']))
            cursor.execute("UPDATE photos SET album_name = ? WHERE album_id = ?",
                           (new_name, album['id']))

        renamed += 1
        if renamed % 20 == 0:
            if not dry_run:
                conn.commit()

    if not dry_run:
        conn.commit()

    log.info(f"Renamed {renamed}/{len(albums)} albums with event names")
    conn.close()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    rename_albums(args.dry_run)
