"""
Download FEC bulk data files directly from fec.gov.

No credentials needed — all files are public.
URL pattern: https://www.fec.gov/files/bulk-downloads/{YYYY}/{file}{YY}.zip
"""
from __future__ import annotations

import logging
import os

import requests

logger = logging.getLogger(__name__)

FEC_BASE_URL = "https://www.fec.gov/files/bulk-downloads"

# Canonical prefix → actual filename prefix on fec.gov
# Most match 1:1, but PAC contributions use "pas2" not "pas".
_FILENAME_PREFIX = {
    "cn": "cn",
    "cm": "cm",
    "ccl": "ccl",
    "indiv": "indiv",
    "pas": "pas2",
    "oppexp": "oppexp",
}


def download_fec_file(
    prefix: str, cycle: int, dest_dir: str, timeout: int = 600,
) -> str | None:
    """Download an FEC bulk ZIP for *prefix* and *cycle*.

    Returns the local path on success, or ``None`` if the file
    doesn't exist on fec.gov (e.g. future cycle not yet published).
    """
    yy = str(cycle)[-2:]
    fec_prefix = _FILENAME_PREFIX.get(prefix, prefix)
    filename = f"{fec_prefix}{yy}.zip"
    url = f"{FEC_BASE_URL}/{cycle}/{filename}"
    dest_path = os.path.join(dest_dir, filename)

    logger.info("Downloading %s …", url)

    try:
        resp = requests.get(url, stream=True, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("Could not download %s: %s", url, exc)
        return None

    os.makedirs(dest_dir, exist_ok=True)
    size = 0
    with open(dest_path, "wb") as fh:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            fh.write(chunk)
            size += len(chunk)

    size_mb = size / (1024 * 1024)
    logger.info("Downloaded %s (%.1f MB)", filename, size_mb)
    return dest_path
