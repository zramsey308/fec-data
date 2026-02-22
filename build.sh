#!/usr/bin/env bash
# build.sh — Render static-site build command.
#
# Render runs this script on every deploy. It:
#   1. Installs Python dependencies.
#   2. Runs the FEC ingestion pipeline (fetches from Google Drive, parses,
#      filters to monitored districts, writes CSVs to public/data/).
#   3. Render then serves everything in public/ as a static site.
set -euo pipefail

echo "=== Installing Python dependencies ==="
pip install --quiet -r requirements.txt

echo "=== Running FEC data ingestion ==="
python -m tasks.fec_ingest

echo "=== Build complete — output files: ==="
ls -lh public/data/
