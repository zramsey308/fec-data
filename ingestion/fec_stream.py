"""
Memory-safe FEC bulk-file streaming.

The indivYY.zip files from the FEC contain a single pipe-delimited text file
(e.g. itcont.txt) with *no header row*.  Column positions are fixed per the
FEC bulk-data spec.

This module:
  1. Opens a ZIP from a local path (already downloaded via services/drive).
  2. Iterates the inner text file line-by-line — never loads the full file.
  3. Extracts only the three columns we need.
  4. Yields typed dicts in configurable chunk sizes.
"""
from __future__ import annotations

import csv
import io
import logging
import zipfile
from decimal import Decimal, InvalidOperation
from typing import Iterator, TypedDict

import config

logger = logging.getLogger(__name__)

# ── FEC individual-contributions column positions (0-indexed) ──
# Full spec: https://www.fec.gov/campaign-finance-data/contributions-individuals-file-description/
COL_CMTE_ID = 0
COL_ENTITY_TP = 6
COL_TRANSACTION_AMT = 14


class ContributionRow(TypedDict):
    cmte_id: str
    transaction_amt: Decimal


def _parse_line(line: str) -> ContributionRow | None:
    """
    Parse a single pipe-delimited line and return the fields we need,
    or None if the line is malformed / amount is unparseable.
    """
    parts = line.split("|")
    if len(parts) <= COL_TRANSACTION_AMT:
        return None

    cmte_id = parts[COL_CMTE_ID].strip()
    raw_amt = parts[COL_TRANSACTION_AMT].strip()

    if not cmte_id or not raw_amt:
        return None

    try:
        amt = Decimal(raw_amt)
    except (InvalidOperation, ValueError):
        return None

    return ContributionRow(cmte_id=cmte_id, transaction_amt=amt)


def stream_contributions(
    zip_path: str,
    chunk_size: int | None = None,
) -> Iterator[list[ContributionRow]]:
    """
    Yield chunks of parsed contribution rows from a ZIP file.

    Each chunk is a list of at most *chunk_size* ContributionRow dicts.
    Memory stays bounded regardless of file size because we read line-by-line
    from the compressed stream and flush each chunk before accumulating the next.
    """
    chunk_size = chunk_size or config.CHUNK_SIZE

    with zipfile.ZipFile(zip_path, "r") as zf:
        # FEC ZIPs typically contain a single .txt file
        names = zf.namelist()
        txt_name = next((n for n in names if n.endswith(".txt")), names[0])
        logger.info("Streaming %s from %s", txt_name, zip_path)

        with zf.open(txt_name) as raw:
            text_stream = io.TextIOWrapper(raw, encoding="utf-8", errors="replace")
            chunk: list[ContributionRow] = []

            for lineno, line in enumerate(text_stream, start=1):
                row = _parse_line(line)
                if row is not None:
                    chunk.append(row)

                if len(chunk) >= chunk_size:
                    logger.debug("Yielding chunk at line %d (%d rows)", lineno, len(chunk))
                    yield chunk
                    chunk = []

            # Flush remaining rows
            if chunk:
                yield chunk

    logger.info("Finished streaming %s", zip_path)


def stream_candidate_master(zip_path: str) -> Iterator[dict]:
    """
    Stream the candidate master file (cnYY.txt) line-by-line.

    Yields dicts with keys: cand_id, cand_office_st, cand_office_district,
    cand_pcc (principal campaign committee ID).

    Column positions per FEC spec:
      0  CAND_ID
      4  CAND_OFFICE_ST
      5  CAND_OFFICE_DISTRICT
      9  CAND_PCC
    """
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()
        txt_name = next((n for n in names if n.endswith(".txt")), names[0])

        with zf.open(txt_name) as raw:
            text_stream = io.TextIOWrapper(raw, encoding="utf-8", errors="replace")
            for line in text_stream:
                parts = line.split("|")
                if len(parts) < 10:
                    continue
                yield {
                    "cand_id": parts[0].strip(),
                    "cand_office_st": parts[4].strip(),
                    "cand_office_district": parts[5].strip(),
                    "cand_pcc": parts[9].strip(),
                }
