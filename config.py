"""
Clawbot configuration — all secrets loaded from environment variables.
"""
import os


# --- Database (Supabase PostgreSQL) ---
DATABASE_URL: str = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://user:pass@localhost:5432/clawbot",
)

# --- Google Drive ---
# Service-account JSON key path *or* OAuth token — never committed.
GOOGLE_SERVICE_ACCOUNT_FILE: str = os.environ.get(
    "GOOGLE_SERVICE_ACCOUNT_FILE", ""
)
# Folder ID that contains the indivYY.zip bulk files
GOOGLE_DRIVE_FOLDER_ID: str = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "")

# --- FEC processing ---
# Chunk size for streaming CSV reads (number of lines per batch)
CHUNK_SIZE: int = int(os.environ.get("FEC_CHUNK_SIZE", "50000"))

# Election cycle to process (e.g. 2024)
DEFAULT_CYCLE: int = int(os.environ.get("FEC_CYCLE", "2024"))

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
