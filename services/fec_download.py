"""
Download FEC bulk data files from the FEC's public S3 bucket.

No credentials needed — all files are public.
URL pattern:
  https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com
  /bulk-downloads/{YYYY}/{file}{YY}.zip
"""
from __future__ import annotations

import logging
import os
import time

import requests

logger = logging.getLogger(__name__)

# The FEC hosts bulk data on a public S3 bucket (gov-cloud).
FEC_BASE_URL = (
    "https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74"
    ".s3-us-gov-west-1.amazonaws.com/bulk-downloads"
)

# Canonical prefix → actual filename prefix in the FEC bulk data.
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
    doesn't exist (e.g. future cycle not yet published).
    Retries up to 3 times on transient network errors.
    """
    yy = str(cycle)[-2:]
    fec_prefix = _FILENAME_PREFIX.get(prefix, prefix)
    filename = f"{fec_prefix}{yy}.zip"
    url = f"{FEC_BASE_URL}/{cycle}/{filename}"
    dest_path = os.path.join(dest_dir, filename)

    os.makedirs(dest_dir, exist_ok=True)

    for attempt in range(4):
        if attempt > 0:
            wait = 2 ** attempt
            print(f"  Retry {attempt}/3 in {wait}s …")
            time.sleep(wait)

        try:
            print(f"  Downloading {url} …")
            resp = requests.get(url, stream=True, timeout=timeout)
            resp.raise_for_status()
        except requests.RequestException as exc:
            if attempt == 3:
                print(f"  FAILED: {exc}")
                return None
            continue

        size = 0
        with open(dest_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                fh.write(chunk)
                size += len(chunk)

        size_mb = size / (1024 * 1024)
        print(f"  Downloaded {filename} ({size_mb:.1f} MB)")
        return dest_path

    return None
