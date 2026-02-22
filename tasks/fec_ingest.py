"""
Background ingestion task.

Orchestrates the full pipeline:
  1. List ZIP files on Google Drive.
  2. Download candidate master → build district map.
  3. For each indivYY.zip:
     a. Stream-download to a temp file.
     b. Stream-parse line-by-line in chunks.
     c. Filter to monitored districts.
     d. Aggregate per-district totals in memory.
     e. Flush aggregates to Postgres via upsert.
  4. Clean up temp files.

Designed to run as a BackgroundTask in FastAPI or a standalone CLI job.
Memory stays flat because we never hold more than one chunk + aggregates.
"""
from __future__ import annotations

import logging
import os
import tempfile
from collections import defaultdict
from decimal import Decimal

import config
from ingestion.fec_stream import stream_contributions
from ingestion.mapping import build_district_map, filter_contributions
from models.district_metrics import SessionLocal, init_db, upsert_metrics
from services.drive import download_file_to_tempfile, list_zip_files

logger = logging.getLogger(__name__)


def _flush_aggregates(
    aggregates: dict[str, dict],
    cycle: int,
) -> None:
    """Write accumulated per-district totals to Postgres and reset."""
    session = SessionLocal()
    try:
        for district, agg in aggregates.items():
            upsert_metrics(
                session,
                district=district,
                cycle=cycle,
                count_delta=agg["count"],
                sum_delta=agg["total"],
            )
        session.commit()
        logger.info("Flushed %d district aggregates to DB", len(aggregates))
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def run_ingest(
    cycle: int | None = None,
    folder_id: str | None = None,
) -> dict:
    """
    Main entry point.  Returns a summary dict.

    Parameters
    ----------
    cycle : election cycle year (default from config).
    folder_id : Google Drive folder containing the ZIPs.
    """
    cycle = cycle or config.DEFAULT_CYCLE
    folder_id = folder_id or config.GOOGLE_DRIVE_FOLDER_ID

    init_db()

    # ── Step 1: discover files ──
    all_files = list_zip_files(folder_id)
    logger.info("Found %d ZIP files on Drive", len(all_files))

    # Separate candidate master from individual-contribution files
    candidate_zips = [f for f in all_files if f["name"].lower().startswith("cn")]
    indiv_zips = [f for f in all_files if f["name"].lower().startswith("indiv")]

    if not candidate_zips:
        raise RuntimeError("No candidate master ZIP (cnYY.zip) found in Drive folder")
    if not indiv_zips:
        raise RuntimeError("No individual contribution ZIP (indivYY.zip) found in Drive folder")

    tmpdir = tempfile.mkdtemp(prefix="clawbot_")

    try:
        # ── Step 2: build district map ──
        cn_file = candidate_zips[0]
        cn_path = os.path.join(tmpdir, cn_file["name"])
        download_file_to_tempfile(cn_file["id"], cn_path)
        district_map = build_district_map(cn_path)
        os.unlink(cn_path)

        if not district_map:
            raise RuntimeError(
                "District map is empty — no monitored-district committees found "
                "in candidate master.  Check config.MONITORED_DISTRICTS."
            )

        # ── Step 3: process each contributions file ──
        total_rows = 0
        total_matched = 0

        for fmeta in indiv_zips:
            indiv_path = os.path.join(tmpdir, fmeta["name"])
            download_file_to_tempfile(fmeta["id"], indiv_path)

            # Per-file aggregates — flushed after the entire file is done
            aggregates: dict[str, dict] = defaultdict(
                lambda: {"count": 0, "total": Decimal("0")}
            )

            for chunk in stream_contributions(indiv_path):
                total_rows += len(chunk)
                matched = filter_contributions(chunk, district_map)
                total_matched += len(matched)

                for district, row in matched:
                    aggregates[district]["count"] += 1
                    aggregates[district]["total"] += row["transaction_amt"]

            # Flush after each file
            if aggregates:
                _flush_aggregates(dict(aggregates), cycle)

            os.unlink(indiv_path)
            logger.info(
                "Processed %s — %d rows, %d matched",
                fmeta["name"],
                total_rows,
                total_matched,
            )

    finally:
        # Clean up temp directory
        try:
            os.rmdir(tmpdir)
        except OSError:
            pass

    summary = {
        "cycle": cycle,
        "files_processed": len(indiv_zips),
        "total_rows": total_rows,
        "matched_rows": total_matched,
        "districts": len(district_map),
    }
    logger.info("Ingest complete: %s", summary)
    return summary


# Allow standalone execution: python -m tasks.fec_ingest
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run_ingest()
    print(result)
