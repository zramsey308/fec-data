"""
Google Drive streaming service.

Downloads files as byte-streams so we never hold an entire ZIP in RAM.
Supports both service-account and pre-shared public links.
"""
from __future__ import annotations

import io
import logging
from typing import Iterator

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

import config

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

# 64 KB download chunks — keeps memory flat.
_DOWNLOAD_CHUNK = 64 * 1024


def _get_drive_service():
    """Authenticate and return a Drive API service object."""
    creds = service_account.Credentials.from_service_account_file(
        config.GOOGLE_SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def list_zip_files(folder_id: str | None = None) -> list[dict]:
    """Return metadata for every .zip file in the target Drive folder."""
    folder_id = folder_id or config.GOOGLE_DRIVE_FOLDER_ID
    service = _get_drive_service()
    query = (
        f"'{folder_id}' in parents "
        "and mimeType='application/zip' "
        "and trashed=false"
    )
    results = (
        service.files()
        .list(q=query, fields="files(id, name, size)", pageSize=100)
        .execute()
    )
    return results.get("files", [])


def stream_file_bytes(file_id: str) -> Iterator[bytes]:
    """
    Yield raw bytes of a Drive file in fixed-size chunks.

    This is the core memory-safe primitive: the caller never holds
    more than _DOWNLOAD_CHUNK bytes of the remote file at a time.
    """
    service = _get_drive_service()
    request = service.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request, chunksize=_DOWNLOAD_CHUNK)

    done = False
    while not done:
        status, done = downloader.next_chunk()
        buf.seek(0)
        data = buf.read()
        if data:
            yield data
        buf.seek(0)
        buf.truncate()

    logger.info("Finished streaming file %s", file_id)


def download_file_to_tempfile(file_id: str, dest_path: str) -> str:
    """
    Stream a Drive file to a local temp path.

    Used when we need seekable access (ZIP central directory requires it)
    but still avoids loading the full file into a Python object.
    """
    with open(dest_path, "wb") as fh:
        for chunk in stream_file_bytes(file_id):
            fh.write(chunk)
    logger.info("Downloaded %s → %s", file_id, dest_path)
    return dest_path
