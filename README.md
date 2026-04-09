# Google Photos Takeout Pipeline

A 9-phase pipeline to process Google Takeout photo exports into a clean, deduplicated library of AI-named event albums — ready to re-upload to Google Photos or import into Immich.

**What it does:**
- Catalogs and deduplicates photos across one or two Takeout exports
- Merges Google JSON sidecars back into photo metadata
- Fixes broken/missing timestamps using neighbour inference
- Reverse geocodes photos offline using GeoNames
- AI-classifies every photo using a local vision model (via [Ollama](https://ollama.com))
- Auto-groups photos into event albums ("Cornwall Beach Holiday 2015", "Peak District Hiking 2023")
- AI-names each album using a text model
- Organises output into flat event folders (symlinks, so no data is duplicated)
- Generates rclone upload scripts for Google Photos

**Typical results:** ~170K unique photos from ~350K total, organised into ~600 named event albums.

---

## Prerequisites

| Tool | Purpose |
|------|---------|
| Python 3.10+ | Pipeline scripts |
| [Ollama](https://ollama.com) | Local AI inference |
| SQLite 3 | Pipeline database (usually pre-installed) |
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
ollama pull gemma3:4b         # text model   — Phase 7.5 (event naming)
```

### GeoNames data (optional, for reverse geocoding)

Download and extract to your pipeline directory:

```bash
wget https://download.geonames.org/export/dump/allCountries.zip
unzip allCountries.zip
```

### GPU acceleration (AMD ROCm)

If you have an AMD GPU, install ROCm so Ollama uses it instead of CPU — gives a significant speedup for Phase 6:

```bash
# Ubuntu/Debian
sudo apt install rocminfo libhsa-runtime64-1
curl -fsSL https://ollama.com/install.sh | sudo sh
```

Verify GPU is detected:
```bash
rocminfo | grep -E "Name|gfx"
```

---

## Configuration

```bash
cp .env.example .env
# Edit .env with your paths
```

Minimum required settings:

```env
PRIMARY_DIR=/path/to/GooglePhotos   # your Takeout export
FINAL_DIR=/path/to/output           # where event albums are created
```

---

## Usage

### Quick start

```bash
# Run all phases (1-9)
bash master_pipeline.sh

# Resume after a crash or reboot
bash resume.sh

# Check progress anytime
bash progress.sh
watch -n 60 bash progress.sh
```

### Run individual phases

```bash
python3 pipeline_v2.py --phase 1        # Audit & catalog
python3 pipeline_v2.py --phase 2        # Merge JSON sidecars
python3 pipeline_v2.py --phase 3        # Deduplicate
python3 pipeline_v2.py --phase 4        # Fix directory names
python3 pipeline_v2.py --phase 5        # Reverse geocode
python3 pipeline_v2.py --phase 6        # AI classify (slow — runs in background)
python3 pipeline_v2.py --phase 7        # Group into event albums
python3 name_events.py                  # AI event naming (Phase 7.5)
python3 pipeline_v2.py --phase 8        # Organise to output dir
python3 pipeline_v2.py --phase 9        # Upload prep
```

### Start from a specific phase

```bash
bash master_pipeline.sh --from 7
```

### Dry run (no changes)

```bash
python3 pipeline_v2.py --phase 1 --dry-run
bash master_pipeline.sh --dry-run
```

---

## Phase details

| Phase | Script | Description | Duration |
|-------|--------|-------------|----------|
| 1 | `pipeline_v2.py` | Audit both source dirs, build SQLite catalogue | Minutes |
| 2 | `pipeline_v2.py` | Merge Google JSON sidecars into photo metadata | Minutes |
| 2.5 | `fix_dates.py` + `neighbor_date_fix.py` | Fix broken timestamps | Minutes |
| 3 | `pipeline_v2.py` | Hash-based deduplication | Minutes |
| 4 | `pipeline_v2.py` | Rename malformed timestamp directories | Minutes |
| 5 | `pipeline_v2.py` | Offline reverse geocoding via GeoNames | Minutes–hours |
| 6 | `pipeline_v2.py` | AI vision classification (runs in background) | **Days** |
| 7 | `pipeline_v2.py` | Cluster photos into event albums by date/location | Minutes |
| 7.5 | `name_events.py` | AI-generate descriptive event names | Minutes |
| 8 | `pipeline_v2.py` | Organise into flat event-named folders (symlinks) | Minutes |
| 9 | `pipeline_v2.py` + `google_photos_upload.py` | Generate rclone upload scripts | Seconds |

> **Phase 6 note:** AI classification is the longest phase (days for large libraries). It runs in the background and checkpoints after every 10 photos. The watcher script (`watch_ai_and_continue.sh`) monitors it, auto-restarts on crash, and triggers Phases 7–9 when complete.

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

Phase 8 creates a flat directory of event album folders:

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

Each entry is a **symlink** back to the original file — no data is duplicated. Both source and output directories must remain accessible during upload.

---

## Upload to Google Photos

After Phase 9, upload with rclone:

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

If you have two Takeout exports (e.g. one with original quality and one with enriched EXIF from DigiKam):

```env
PRIMARY_DIR=/path/to/main/GooglePhotos       # larger, more complete
SECONDARY_DIR=/path/to/secondary/GooglePhotos # smaller, enriched metadata
```

The pipeline uses PRIMARY as the source of truth for files, and merges any enriched metadata from SECONDARY.

---

## Speeding up Phase 6

Phase 6 (AI classify) is the bottleneck. Options to speed it up:

**1. GPU acceleration** — the biggest win. See [AMD ROCm](#gpu-acceleration-amd-rocm) above. Also works with NVIDIA (CUDA support built into Ollama).

**2. Tune Ollama parallelism** — edit `/etc/systemd/system/ollama.service`:
```ini
Environment="OLLAMA_NUM_PARALLEL=2"
Environment="OLLAMA_FLASH_ATTENTION=1"
```

**3. The pipeline already:**
- Resizes images to max 1024px before encoding (faster network + less VRAM)
- Runs 5 concurrent workers to keep the GPU saturated
- Skips videos and already-processed photos

---

## Troubleshooting

**Pipeline DB locked:** Another process is using it. Wait or kill stray Python processes.

**Ollama not available:** Check `curl http://localhost:11434/api/version`. Start with `ollama serve`.

**Drive not mounted:** The pipeline checks for source/output directories at startup. Ensure drives are mounted before running.

**Phase 6 crashed:** Just run `bash resume.sh` — it picks up from the last checkpoint.

---

## License

MIT
