"""
Memory-safe FEC bulk-file streaming.

Parses the FEC bulk download ZIP files line-by-line so memory stays flat
regardless of file size.  Each parser yields dicts with named fields.

Supported file types:
  - indivYY.zip  → individual contributions  (itcont.txt)
  - pasYY.zip    → PAC / committee-to-committee contributions (itpas2.txt)
  - oppexpYY.zip → operating expenditures / disbursements (oppexp.txt)
  - cnYY.zip     → candidate master (cn.txt)
  - cmYY.zip     → committee master (cm.txt)
  - ccnYY.zip    → candidate-committee linkage (ccl.txt)

Column positions follow the FEC bulk-data spec:
https://www.fec.gov/campaign-finance-data/contributions-individuals-file-description/
"""
from __future__ import annotations

import io
import logging
import zipfile
from decimal import Decimal, InvalidOperation
from typing import Iterator

import config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _open_txt_in_zip(zip_path: str):
    """Open the first .txt inside a ZIP, or a raw .txt file directly.

    Returns (closeable, text_stream) where closeable.close() cleans up.
    Works with both .zip archives and plain .txt files.
    """
    if zip_path.lower().endswith(".zip"):
        zf = zipfile.ZipFile(zip_path, "r")
        names = zf.namelist()
        txt_name = next((n for n in names if n.lower().endswith(".txt")), names[0])
        logger.info("Streaming %s from %s", txt_name, zip_path)
        raw = zf.open(txt_name)
        text_stream = io.TextIOWrapper(raw, encoding="utf-8", errors="replace")
        return zf, text_stream
    else:
        # Raw .txt file — wrap in an object with a .close() method
        logger.info("Streaming raw file %s", zip_path)
        fh = open(zip_path, "r", encoding="utf-8", errors="replace")
        return fh, fh


def _safe_decimal(raw: str) -> Decimal | None:
    raw = raw.strip()
    if not raw:
        return None
    try:
        return Decimal(raw)
    except (InvalidOperation, ValueError):
        return None


def _clean(val: str) -> str:
    return val.strip()


# ---------------------------------------------------------------------------
# Individual contributions  (indivYY.zip → itcont.txt)
# ---------------------------------------------------------------------------
# Columns: CMTE_ID|AMNDT_IND|RPT_TP|TRANSACTION_PGI|IMAGE_NUM|TRANSACTION_TP|
#           ENTITY_TP|NAME|CITY|STATE|ZIP_CODE|EMPLOYER|OCCUPATION|
#           TRANSACTION_DT|TRANSACTION_AMT|OTHER_ID|TRAN_ID|FILE_NUM|
#           MEMO_CD|MEMO_TEXT|SUB_ID

def stream_individual_contributions(
    zip_path: str,
    chunk_size: int | None = None,
) -> Iterator[list[dict]]:
    """Yield chunks of individual contribution rows."""
    chunk_size = chunk_size or config.CHUNK_SIZE
    zf, text_stream = _open_txt_in_zip(zip_path)

    try:
        chunk: list[dict] = []
        for line in text_stream:
            parts = line.split("|")
            if len(parts) < 15:
                continue
            amt = _safe_decimal(parts[14])
            if amt is None:
                continue

            chunk.append({
                "cmte_id": _clean(parts[0]),
                "entity_type": _clean(parts[6]),
                "contributor_name": _clean(parts[7]),
                "city": _clean(parts[8]),
                "state": _clean(parts[9]),
                "zip_code": _clean(parts[10]),
                "employer": _clean(parts[11]),
                "occupation": _clean(parts[12]),
                "transaction_date": _clean(parts[13]),
                "transaction_amt": amt,
                "memo_text": _clean(parts[19]) if len(parts) > 19 else "",
            })

            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk
    finally:
        zf.close()

    logger.info("Finished streaming individual contributions from %s", zip_path)


# Keep backward-compatible alias used by mapping.py
stream_contributions = stream_individual_contributions


# ---------------------------------------------------------------------------
# PAC / committee-to-committee  (pasYY.zip → itpas2.txt)
# ---------------------------------------------------------------------------
# Same column layout as individual contributions.

def stream_pac_contributions(
    zip_path: str,
    chunk_size: int | None = None,
) -> Iterator[list[dict]]:
    """Yield chunks of PAC / committee-to-committee contribution rows."""
    chunk_size = chunk_size or config.CHUNK_SIZE
    zf, text_stream = _open_txt_in_zip(zip_path)

    try:
        chunk: list[dict] = []
        for line in text_stream:
            parts = line.split("|")
            if len(parts) < 15:
                continue
            amt = _safe_decimal(parts[14])
            if amt is None:
                continue

            chunk.append({
                "cmte_id": _clean(parts[0]),
                "entity_type": _clean(parts[6]),
                "contributor_name": _clean(parts[7]),
                "city": _clean(parts[8]),
                "state": _clean(parts[9]),
                "zip_code": _clean(parts[10]),
                "transaction_date": _clean(parts[13]),
                "transaction_amt": amt,
                "other_id": _clean(parts[15]) if len(parts) > 15 else "",
                "memo_text": _clean(parts[19]) if len(parts) > 19 else "",
            })

            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk
    finally:
        zf.close()

    logger.info("Finished streaming PAC contributions from %s", zip_path)


# ---------------------------------------------------------------------------
# Operating expenditures  (oppexpYY.zip → oppexp.txt)
# ---------------------------------------------------------------------------
# Columns: CMTE_ID|AMNDT_IND|RPT_YR|RPT_TP|IMAGE_NUM|LINE_NUM|FORM_TP_CD|
#           SCHED_TP_CD|NAME|CITY|STATE|ZIP_CODE|TRANSACTION_DT|
#           TRANSACTION_AMT|PURPOSE|MEMO_CD|MEMO_TEXT|ENTITY_TP|...

def stream_expenditures(
    zip_path: str,
    chunk_size: int | None = None,
) -> Iterator[list[dict]]:
    """Yield chunks of operating expenditure rows."""
    chunk_size = chunk_size or config.CHUNK_SIZE
    zf, text_stream = _open_txt_in_zip(zip_path)

    try:
        chunk: list[dict] = []
        for line in text_stream:
            parts = line.split("|")
            if len(parts) < 14:
                continue
            amt = _safe_decimal(parts[13])
            if amt is None:
                continue

            chunk.append({
                "cmte_id": _clean(parts[0]),
                "recipient_name": _clean(parts[8]),
                "city": _clean(parts[9]),
                "state": _clean(parts[10]),
                "zip_code": _clean(parts[11]),
                "transaction_date": _clean(parts[12]),
                "transaction_amt": amt,
                "purpose": _clean(parts[14]) if len(parts) > 14 else "",
                "memo_text": _clean(parts[16]) if len(parts) > 16 else "",
            })

            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk
    finally:
        zf.close()

    logger.info("Finished streaming expenditures from %s", zip_path)


# ---------------------------------------------------------------------------
# Candidate master  (cnYY.zip → cn.txt)
# ---------------------------------------------------------------------------
# Columns: CAND_ID|CAND_NAME|CAND_PTY_AFFILIATION|CAND_ELECTION_YR|
#           CAND_OFFICE_ST|CAND_OFFICE_DISTRICT|CAND_OFFICE|CAND_ICI|
#           CAND_STATUS|CAND_PCC|CAND_ST1|CAND_ST2|CAND_CITY|CAND_ST|CAND_ZIP

def stream_candidate_master(zip_path: str) -> Iterator[dict]:
    """Yield one dict per candidate row."""
    zf, text_stream = _open_txt_in_zip(zip_path)
    try:
        for line in text_stream:
            parts = line.split("|")
            if len(parts) < 10:
                continue
            yield {
                "cand_id": _clean(parts[0]),
                "cand_name": _clean(parts[1]),
                "cand_party": _clean(parts[2]),
                "cand_election_yr": _clean(parts[3]),
                "cand_office_st": _clean(parts[4]),
                "cand_office_district": _clean(parts[5]),
                "cand_office": _clean(parts[6]),
                "cand_ici": _clean(parts[7]),
                "cand_status": _clean(parts[8]),
                "cand_pcc": _clean(parts[9]),
            }
    finally:
        zf.close()


# ---------------------------------------------------------------------------
# Committee master  (cmYY.zip → cm.txt)
# ---------------------------------------------------------------------------
# Columns: CMTE_ID|CMTE_NM|TRES_NM|CMTE_ST1|CMTE_ST2|CMTE_CITY|CMTE_ST|
#           CMTE_ZIP|CMTE_DSGN|CMTE_TP|CMTE_PTY_AFFILIATION|
#           CMTE_FILING_FREQ|ORG_TP|CONNECTED_ORG_NM|CAND_ID

def stream_committee_master(zip_path: str) -> Iterator[dict]:
    """Yield one dict per committee row."""
    zf, text_stream = _open_txt_in_zip(zip_path)
    try:
        for line in text_stream:
            parts = line.split("|")
            if len(parts) < 11:
                continue
            yield {
                "cmte_id": _clean(parts[0]),
                "cmte_name": _clean(parts[1]),
                "treasurer_name": _clean(parts[2]),
                "city": _clean(parts[5]) if len(parts) > 5 else "",
                "state": _clean(parts[6]) if len(parts) > 6 else "",
                "zip_code": _clean(parts[7]) if len(parts) > 7 else "",
                "designation": _clean(parts[8]) if len(parts) > 8 else "",
                "cmte_type": _clean(parts[9]) if len(parts) > 9 else "",
                "party": _clean(parts[10]) if len(parts) > 10 else "",
                "connected_org": _clean(parts[13]) if len(parts) > 13 else "",
                "cand_id": _clean(parts[14]) if len(parts) > 14 else "",
            }
    finally:
        zf.close()


# ---------------------------------------------------------------------------
# Candidate-committee linkage  (ccnYY.zip → ccl.txt)
# ---------------------------------------------------------------------------
# Columns: CAND_ID|CAND_ELECTION_YR|FEC_ELECTION_YR|CMTE_ID|CMTE_TP|
#           CMTE_DSGN|LINKAGE_ID

def stream_candidate_committee_linkage(zip_path: str) -> Iterator[dict]:
    """Yield one dict per candidate-committee linkage row."""
    zf, text_stream = _open_txt_in_zip(zip_path)
    try:
        for line in text_stream:
            parts = line.split("|")
            if len(parts) < 6:
                continue
            yield {
                "cand_id": _clean(parts[0]),
                "cand_election_yr": _clean(parts[1]),
                "cmte_id": _clean(parts[3]),
                "cmte_type": _clean(parts[4]),
                "cmte_designation": _clean(parts[5]),
            }
    finally:
        zf.close()
