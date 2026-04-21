# Google Photos Takeout Pipeline

A 9-phase pipeline to process Google Takeout photo exports into a clean, deduplicated library of AI-named event albums — ready to re-upload to Google Photos or import into Immich.

**What it does:**
- Catalogs and deduplicates photos across one or two Takeout exports
- Merges Google JSON sidecars back into photo metadata
- Fixes broken/missing timestamps using neighbour inference
- Reverse geocodes photos offline using GeoNames data
- Infers GPS coordinates for un-tagged photos using cluster inference
- AI-classifies every photo using a local vision model (via [Ollama](https://ollama.com))
- Auto-groups photos into event albums ("Cornwall Beach Holiday 2015", "Peak District Hiking 2023")
- AI-names each album using a text model
- Organises output into flat event folders (symlinks, no data duplication)
- Generates rclone upload scripts for Google Photos
- Curates the best photos into an Instagram-ready set with cinematic grading

**Typical results:** ~170K unique photos from ~350K total, organised into ~600 named event albums.

---

## Prerequisites

| Tool | Purpose |
|------|---------|
| Python 3.10+ | Pipeline scripts |
| [Ollama](https://ollama.com) | Local AI inference |
| SQLite 3 | Pipeline database (pre-installed on most systems) |
| exiftool | EXIF metadata reading/writing (Phase 2) |
| [rclone](https://rclone.org) | Google Photos upload (Phase 9 only) |

```bash
pip install -r requirements.txt

# Install exiftool
sudo apt install exiftool        # Ubuntu/Debian
brew install exiftool            # macOS
```

### Ollama models

```bash
ollama pull llama3.2-vision   # vision model — Phase 6 (AI classify)
ollama pull gemma3:4b         # text model   — Phase 7.5 (event naming) + Instagram captions
```

### GeoNames data (for reverse geocoding)

```bash
wget https://download.geonames.org/export/dump/allCountries.zip
unzip allCountries.zip
# Set GEONAMES_FILE=/path/to/allCountries.txt in .env
```

### GPU acceleration (AMD ROCm / NVIDIA CUDA)

GPU makes Phase 6 significantly faster. Ollama auto-detects NVIDIA CUDA.  
For AMD:
```bash
sudo apt install rocminfo libhsa-runtime64-1
curl -fsSL https://ollama.com/install.sh | sudo sh
rocminfo | grep -E "Name|gfx"   # verify GPU detected
```

---

## Configuration

```bash
cp .env.example .env
# Edit .env with your paths
```

Minimum required settings:

```env
PRIMARY_DIR=/path/to/GooglePhotos   # your Takeout export (the folder with year subdirs)
FINAL_DIR=/path/to/output           # where event albums are created (needs space)
EVO_MOUNT=/run/media/youruser/evo   # source drive mount point
IMMICH_MOUNT=/run/media/youruser/immich  # output drive mount point
```

Optional settings:
```env
SECONDARY_DIR=/path/to/second/export  # second Takeout export (enriched metadata)
GEONAMES_FILE=/path/to/allCountries.txt
PIPELINE_DIR=/home/you/photo-pipeline-repo
OLLAMA_HOST=http://localhost:11434
VISION_MODEL=llama3.2-vision:latest
TEXT_MODEL=gemma3:4b
MAX_PER_ALBUM=150   # max photos scored per album for Instagram (default: 150)
```

---

## Usage

### Quick start

```bash
# Run all phases (1-9)
bash run_pipeline.sh

# Resume after a crash or reboot
bash resume.sh

# Check progress anytime
bash status.sh
watch -n 60 bash progress.sh
```

### Run individual phases

```bash
bash run_pipeline.sh --from 3    # resume from phase 3
bash run_pipeline.sh --from 7    # re-cluster albums (e.g. post-AI)
bash run_pipeline.sh --dry-run   # preview without changes
```

Or run individual steps directly:

```bash
python3 pipeline.py --step scan           # Phase 1: catalog
python3 pipeline.py --step merge-sidecars # Phase 2: merge JSON sidecars
bash fix_all_dates.sh                     # Phase 2.5: fix bad/missing dates
python3 pipeline.py --step deduplicate    # Phase 3: deduplicate
python3 pipeline.py --step geocode        # Phase 4: reverse geocode
bash run_guess_locations.sh               # Phase 4.5: GPS inference
python3 pipeline.py --step classify       # Phase 6: AI classify (slow)
python3 pipeline.py --step group-albums   # Phase 7: cluster into albums
python3 name_events.py                    # Phase 7.5: AI event naming
python3 pipeline.py --step export         # Phase 8: export to FINAL_DIR
python3 pipeline.py --step prep-upload    # Phase 9: generate upload scripts
```

Numeric shortcuts also work: `--step 1` through `--step 9`.

---

## Phase details

| Phase | Name | Script | Description | Duration |
|-------|------|--------|-------------|----------|
| 1 | `scan` | `pipeline.py` | Catalog source dirs into SQLite | Minutes |
| 2 | `merge-sidecars` | `pipeline.py` | Merge Google JSON sidecars | Minutes |
| 2.5 | — | `fix_all_dates.sh` | Fix bad/missing timestamps | Minutes |
| 3 | `deduplicate` | `pipeline.py` | Hash-based deduplication | Minutes |
| 4 | `geocode` | `pipeline.py` | Offline reverse geocoding via GeoNames | Minutes–hours |
| 4.5 | — | `run_guess_locations.sh` | GPS inference from album clusters | Minutes |
| 6 | `classify` | `pipeline.py` | AI vision classification (background) | **Days** |
| 7 | `group-albums` | `pipeline.py` | Cluster photos into event albums | Minutes |
| 7.5 | — | `name_events.py` | AI-generate descriptive event names | Minutes |
| 8 | `export` | `pipeline.py` | Organise into event-named folders (symlinks) | Minutes |
| 9 | `prep-upload` | `pipeline.py` | Generate rclone upload scripts | Seconds |

> **Phase 6 note:** AI classification is the longest phase (days for large libraries). It runs in the background and checkpoints every 10 photos. `watch_ai_and_continue.sh` monitors it, auto-restarts on crash, and triggers Phases 7–9 when complete.

---

## Helper scripts

| Script | Purpose |
|--------|---------|
| `run_pipeline.sh` | Main orchestrator — runs all phases with `--from N` resumability |
| `status.sh` | Dashboard: disk space, DB stats, phase completion status |
| `progress.sh` | Detailed progress with ETAs and progress bars |
| `status_all.sh` | Quick overview of all running processes |
| `resume.sh` | Resume after crash — restarts AI classify and watcher |
| `fix_all_dates.sh` | Run date-fixing scripts (JSON sidecar + neighbour inference) |
| `run_guess_locations.sh` | Run GPS inference (phase 4.5) |
| `continue_after_phase2.sh` | Wait for phase 2, then auto-continue from 2.5 |
| `rerun_after_ai.sh` | Re-run phases 7–9 after AI classify finishes |
| `run_phases_8_9.sh` | Re-run export + upload prep only |
| `master_pipeline.sh` | Advanced orchestrator with `--after-ai`, `--ai-only` modes |
| `watch_ai_and_continue.sh` | Background watcher: monitors AI and triggers post-AI phases |
| `run_instagram.sh` | Run Instagram curation pipeline |
| `run_immich_albums.sh` | Create/update Immich albums from event folders |
| `upload_to_gphotos.sh` | Generated rclone upload script (created by phase 9) |

---

## Background watcher

For large libraries, start the watcher and walk away:

```bash
nohup bash watch_ai_and_continue.sh &
```

It will:
1. Monitor Phase 6 (AI classify) and restart it if it crashes
2. Automatically run Phases 7 → 7.5 → 8 → 9 when Phase 6 completes

---

## Output structure

Phase 8 creates a flat directory of event album folders (symlinks to originals):

```
final-google-photos/
├── Cornwall Beach Holiday 2015/
│   ├── IMG_1234.jpg -> /path/to/source/IMG_1234.jpg
│   └── ...
├── Peak District Hiking 2023/
│   └── ...
└── Christmas in Yorkshire 2019/
    └── ...
```

---

## Instagram pipeline

Curates the best photos from your event albums into an Instagram-ready set with cinematic colour grading.

```bash
# Run full pipeline (discover → score → edit → curate → stories → caption → export)
bash run_instagram.sh --step all

# Run individual steps
bash run_instagram.sh --step discover    # find album candidates
bash run_instagram.sh --step score       # AI-score photos (requires Ollama)
bash run_instagram.sh --step edit        # apply cinematic LUT grading + collages
bash run_instagram.sh --step curate      # select best 9 photos per album
bash run_instagram.sh --step stories     # generate story images
bash run_instagram.sh --step caption     # generate captions with Ollama
bash run_instagram.sh --step export      # copy ready set to instagram_ready/
bash run_instagram.sh --step summary     # show current status

# Resume from a specific step
bash run_instagram.sh --step score       # scoring is resumable
```

Output lands in `instagram_ready/` with one sub-folder per album:
- `01.jpg` … `09.jpg` — colour-graded best photos
- `story.jpg` — vertical story image with gradient text overlay
- `collage.jpg` — 3×2 grid of top picks (albums with ≥6 photos)
- `caption.txt` — AI-generated caption with hashtags

**Scoring throughput:** ~22 photos/min with local Ollama. With `MAX_PER_ALBUM=150` (default), scoring takes ~13 hours for 500 albums.

---

## Upload to Google Photos

After Phase 9:

```bash
# 1. Install rclone
curl https://rclone.org/install.sh | sudo bash

# 2. Configure Google Photos remote
rclone config
# Choose: New remote → Google Photos → follow prompts → name it "gphotos"

# 3. Upload
bash upload_to_gphotos.sh
```

---

## Two-source setup

If you have two Takeout exports:

```env
PRIMARY_DIR=/path/to/main/GooglePhotos       # larger, more complete
SECONDARY_DIR=/path/to/secondary/GooglePhotos # smaller, enriched metadata
```

The pipeline uses PRIMARY as the source of truth for files, and merges enriched metadata from SECONDARY.

---

## Speeding up Phase 6

Phase 6 (AI classify) is the bottleneck. Options:

**1. GPU acceleration** — the biggest win. See [GPU section](#gpu-acceleration-amd-rocm--nvidia-cuda) above.

**2. Tune Ollama parallelism** — edit `/etc/systemd/system/ollama.service`:
```ini
Environment="OLLAMA_NUM_PARALLEL=2"
Environment="OLLAMA_FLASH_ATTENTION=1"
```

**3. The pipeline already:**
- Resizes images to max 1024px before encoding
- Runs 5 concurrent workers
- Skips videos and already-processed photos

---

## Troubleshooting

**Pipeline DB locked:** Another process is using it. Wait or kill stray Python processes: `pkill -f pipeline.py`

**Ollama not available:** Check `curl http://localhost:11434/api/version`. Start with `ollama serve`.

**Drive not mounted:** The pipeline checks mounts at startup. Ensure drives are mounted before running. Check mount points in `.env` match `lsblk` output.

**Phase 6 crashed:** Run `bash resume.sh` — it picks up from the last checkpoint.

**Broken symlinks in output:** If drives were remounted at different paths, fix symlinks:
```bash
find "$FINAL_DIR" -type l -xtype l | while read link; do
    target=$(readlink "$link")
    newtarget="${target/old_path/new_path}"
    ln -sf "$newtarget" "$link"
done
```

---

## License

MIT
