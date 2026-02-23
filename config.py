"""
Clawbot configuration — all secrets loaded from environment variables.
"""
import os

# --- Google Drive ---
# Service-account JSON key path — never committed.
GOOGLE_SERVICE_ACCOUNT_FILE: str = os.environ.get(
    "GOOGLE_SERVICE_ACCOUNT_FILE", ""
)
# Service-account JSON as a raw string (preferred on Render).
# Set this env var with the full contents of your service-account.json file.
GOOGLE_SERVICE_ACCOUNT_JSON: str = os.environ.get(
    "GOOGLE_SERVICE_ACCOUNT_JSON", ""
)
# Folder ID that contains the FEC bulk files
GOOGLE_DRIVE_FOLDER_ID: str = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "")

# --- FEC processing ---
CHUNK_SIZE: int = int(os.environ.get("FEC_CHUNK_SIZE", "50000"))

# Election cycles to process — comma-separated years.
# Accepts FEC_CYCLES or FEC_CYCLE (either works with a comma-separated list).
# Each cycle corresponds to 2-digit file suffixes on Drive (e.g. indiv20.zip).
_cycles_raw = os.environ.get("FEC_CYCLES") or os.environ.get("FEC_CYCLE") or "2020,2022,2024,2026"
FEC_CYCLES: list[int] = [int(y.strip()) for y in _cycles_raw.split(",")]

# --- Output ---
# Build output directory for the static site
OUTPUT_DIR: str = os.environ.get("OUTPUT_DIR", "public/data")
RAW_DATA_DIR: str = os.environ.get("RAW_DATA_DIR", "raw_data")

# --- Monitored districts ---
MONITORED_DISTRICTS: list[str] = [
    "TX-02", "TX-08", "TX-09", "TX-10", "TX-11", "TX-19",
    "TX-21", "TX-32", "TX-35", "TX-38",
    "AL-01",
    "AZ-01", "AZ-05",
    "FL-19",
    "GA-01", "GA-10",
    "IA-02", "IA-04",
    "KY-06",
    "NE-02",
    "SC-01", "SC-05",
    "SD-AL",
    "TN-06",
    "WI-07",
]
