"""
Google Drive download service.

Supports two credential methods:
  1. GOOGLE_SERVICE_ACCOUNT_JSON env var (paste the full JSON — preferred on Render)
  2. GOOGLE_SERVICE_ACCOUNT_FILE env var (path to a .json key file — local dev)
"""
from __future__ import annotations

import io
import json
import logging
import os

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

import config

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
_DOWNLOAD_CHUNK = 64 * 1024


def _get_credentials():
    """Build credentials from env var JSON string or key file."""
    if config.GOOGLE_SERVICE_ACCOUNT_JSON:
        info = json.loads(config.GOOGLE_SERVICE_ACCOUNT_JSON)
        return service_account.Credentials.from_service_account_info(
            info, scopes=SCOPES
        )
    if config.GOOGLE_SERVICE_ACCOUNT_FILE:
        return service_account.Credentials.from_service_account_file(
            config.GOOGLE_SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
    raise RuntimeError(
        "Set GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SERVICE_ACCOUNT_FILE"
    )


def _get_drive_service():
    """Authenticate and return a Drive API service object."""
    creds = _get_credentials()
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def list_files(folder_id: str | None = None) -> list[dict]:
    """Return metadata for every file in the target Drive folder."""
    folder_id = folder_id or config.GOOGLE_DRIVE_FOLDER_ID
    service = _get_drive_service()
    query = f"'{folder_id}' in parents and trashed=false"

    all_files = []
    page_token = None

    while True:
        results = (
            service.files()
            .list(
                q=query,
                fields="nextPageToken, files(id, name, size, mimeType)",
                pageSize=100,
                pageToken=page_token,
            )
            .execute()
        )
        all_files.extend(results.get("files", []))
        page_token = results.get("nextPageToken")
        if not page_token:
            break

    logger.info("Found %d files in Drive folder %s", len(all_files), folder_id)
    return all_files


def list_zip_files(folder_id: str | None = None) -> list[dict]:
    """Return metadata for every .zip file in the target Drive folder."""
    all_files = list_files(folder_id)
    return [f for f in all_files if f["name"].lower().endswith(".zip")]


def list_data_files(folder_id: str | None = None) -> list[dict]:
    """Return metadata for every FEC data file (.zip or .txt) in the folder."""
    all_files = list_files(folder_id)
    return [
        f for f in all_files
        if f["name"].lower().endswith((".zip", ".txt"))
    ]


def download_file(file_id: str, dest_path: str) -> str:
    """Download a Drive file to a local path."""
    os.makedirs(os.path.dirname(dest_path) or ".", exist_ok=True)
    service = _get_drive_service()
    request = service.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request, chunksize=_DOWNLOAD_CHUNK)

    done = False
    while not done:
        status, done = downloader.next_chunk()
        if status:
            logger.debug("Download %s: %d%%", file_id, int(status.progress() * 100))

    buf.seek(0)
    with open(dest_path, "wb") as fh:
        fh.write(buf.read())

    logger.info("Downloaded %s → %s", file_id, dest_path)
    return dest_path
