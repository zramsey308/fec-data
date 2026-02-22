"""
FEC ingestion → filtered CSV output for dashboard (multi-cycle).

Orchestrates the full pipeline across multiple election cycles:
  1. List all ZIP files on Google Drive.
  2. For each cycle (e.g. 2020, 2022, 2024, 2026):
     a. Match files by 2-digit year suffix (indiv20.zip, cn24.zip, etc.)
     b. Download reference files → build district map.
     c. Stream each bulk file, filter to monitored districts, write CSV rows.
  3. Write combined CSVs with a "cycle" column on every row.
  4. Write a summary CSV with per-district per-cycle totals.

Output CSVs (written to config.OUTPUT_DIR):
  - candidates.csv              candidate details for monitored districts
  - committees.csv              committee details for monitored districts
  - individual_contributions.csv   itemised individual donations
  - pac_contributions.csv       PAC / committee-to-committee donations
  - expenditures.csv            operating expenditures / disbursements
  - district_summary.csv        aggregated totals per district per cycle
"""
from __future__ import annotations

import csv
import logging
import os
import tempfile
from collections import defaultdict
from decimal import Decimal

import config
from ingestion.fec_stream import (
    stream_candidate_master,
    stream_committee_master,
    stream_expenditures,
    stream_individual_contributions,
    stream_pac_contributions,
)
from ingestion.mapping import build_district_map, filter_contributions
from services.drive import download_file, list_zip_files

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CSV writers
# ---------------------------------------------------------------------------

def _open_csv(filename: str, fieldnames: list[str]):
    """Create a CSV writer in the output directory. Returns (file, writer)."""
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    path = os.path.join(config.OUTPUT_DIR, filename)
    fh = open(path, "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    return fh, writer


# ---------------------------------------------------------------------------
# File matching helpers
# ---------------------------------------------------------------------------

def _suffix(cycle: int) -> str:
    """Return 2-digit year suffix for a cycle, e.g. 2024 → '24'."""
    return str(cycle)[-2:]


def _find_zip_for_cycle(files: list[dict], prefix: str, cycle: int) -> dict | None:
    """Find a ZIP matching prefix + 2-digit cycle suffix (e.g. 'cn' + '24')."""
    suffix = _suffix(cycle)
    for f in files:
        name = f["name"].lower()
        if name.startswith(prefix) and suffix in name and name.endswith(".zip"):
            return f
    return None


def _find_all_zips_for_cycle(files: list[dict], prefix: str, cycle: int) -> list[dict]:
    """Find all ZIPs matching prefix + 2-digit cycle suffix."""
    suffix = _suffix(cycle)
    return [
        f for f in files
        if f["name"].lower().startswith(prefix)
        and suffix in f["name"].lower()
        and f["name"].lower().endswith(".zip")
    ]


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run_ingest(
    cycles: list[int] | None = None,
    folder_id: str | None = None,
) -> dict:
    """
    Main entry point. Fetches FEC data from Drive for each cycle,
    filters to monitored districts, and writes combined CSV files.
    """
    cycles = cycles or config.FEC_CYCLES
    folder_id = folder_id or config.GOOGLE_DRIVE_FOLDER_ID

    logger.info("Processing cycles: %s", cycles)

    # ── Discover all files on Drive once ──
    all_zips = list_zip_files(folder_id)
    logger.info("Found %d ZIP files on Drive", len(all_zips))

    for f in all_zips:
        logger.info("  Drive file: %s", f["name"])

    tmpdir = tempfile.mkdtemp(prefix="clawbot_")

    stats: dict[str, int] = defaultdict(int)

    # ── Open output CSV files (shared across all cycles) ──
    cand_fh, cand_writer = _open_csv("candidates.csv", [
        "cycle", "district", "cand_id", "cand_name", "cand_party",
        "cand_election_yr", "cand_office", "cand_ici", "cand_status",
    ])
    cmte_fh, cmte_writer = _open_csv("committees.csv", [
        "cycle", "district", "cmte_id", "cmte_name", "cmte_type",
        "designation", "party", "connected_org", "treasurer_name",
    ])
    indiv_fh, indiv_writer = _open_csv("individual_contributions.csv", [
        "cycle", "district", "cmte_id", "contributor_name", "city", "state",
        "zip_code", "employer", "occupation", "transaction_date",
        "transaction_amt", "entity_type",
    ])
    pac_fh, pac_writer = _open_csv("pac_contributions.csv", [
        "cycle", "district", "cmte_id", "contributor_name", "city", "state",
        "zip_code", "transaction_date", "transaction_amt",
        "other_id", "entity_type",
    ])
    exp_fh, exp_writer = _open_csv("expenditures.csv", [
        "cycle", "district", "cmte_id", "recipient_name", "city", "state",
        "zip_code", "transaction_date", "transaction_amt", "purpose",
    ])

    # Aggregation accumulators: (district, cycle) → counts
    agg_indiv: dict[tuple[str, int], dict] = defaultdict(
        lambda: {"count": 0, "total": Decimal("0")}
    )
    agg_pac: dict[tuple[str, int], dict] = defaultdict(
        lambda: {"count": 0, "total": Decimal("0")}
    )
    agg_exp: dict[tuple[str, int], dict] = defaultdict(
        lambda: {"count": 0, "total": Decimal("0")}
    )

    try:
        for cycle in cycles:
            logger.info("═══ Processing cycle %d ═══", cycle)

            # ── 1. Build district map for this cycle ──
            cn_zip = _find_zip_for_cycle(all_zips, "cn", cycle)
            ccl_zip = (
                _find_zip_for_cycle(all_zips, "ccl", cycle)
                or _find_zip_for_cycle(all_zips, "ccn", cycle)
            )

            if not cn_zip:
                logger.warning(
                    "No candidate master (cn%s.zip) found — skipping cycle %d",
                    _suffix(cycle), cycle,
                )
                continue

            cn_path = os.path.join(tmpdir, cn_zip["name"])
            download_file(cn_zip["id"], cn_path)

            ccl_path = None
            if ccl_zip:
                ccl_path = os.path.join(tmpdir, ccl_zip["name"])
                download_file(ccl_zip["id"], ccl_path)

            district_map = build_district_map(cn_path, ccl_path)

            if not district_map:
                logger.warning(
                    "District map empty for cycle %d — no monitored districts found",
                    cycle,
                )
                os.unlink(cn_path)
                if ccl_path:
                    os.unlink(ccl_path)
                continue

            monitored_districts = set(config.MONITORED_DISTRICTS)

            # ── 2. Write candidates for this cycle ──
            for rec in stream_candidate_master(cn_path):
                st = rec["cand_office_st"]
                raw_d = rec["cand_office_district"]
                if not st or not raw_d:
                    continue
                dist = f"{st}-AL" if raw_d == "00" else f"{st}-{raw_d.zfill(2)}"
                if dist in monitored_districts:
                    cand_writer.writerow({
                        "cycle": cycle,
                        "district": dist,
                        "cand_id": rec["cand_id"],
                        "cand_name": rec["cand_name"],
                        "cand_party": rec["cand_party"],
                        "cand_election_yr": rec["cand_election_yr"],
                        "cand_office": rec["cand_office"],
                        "cand_ici": rec["cand_ici"],
                        "cand_status": rec["cand_status"],
                    })
                    stats["candidates"] += 1

            os.unlink(cn_path)
            if ccl_path:
                os.unlink(ccl_path)

            # ── 3. Write committees for this cycle ──
            cm_zip = _find_zip_for_cycle(all_zips, "cm", cycle)
            if cm_zip:
                cm_path = os.path.join(tmpdir, cm_zip["name"])
                download_file(cm_zip["id"], cm_path)
                for rec in stream_committee_master(cm_path):
                    dist = district_map.get(rec["cmte_id"])
                    if dist:
                        cmte_writer.writerow({
                            "cycle": cycle,
                            "district": dist,
                            "cmte_id": rec["cmte_id"],
                            "cmte_name": rec["cmte_name"],
                            "cmte_type": rec["cmte_type"],
                            "designation": rec["designation"],
                            "party": rec["party"],
                            "connected_org": rec["connected_org"],
                            "treasurer_name": rec["treasurer_name"],
                        })
                        stats["committees"] += 1
                os.unlink(cm_path)

            # ── 4. Individual contributions ──
            indiv_zips = _find_all_zips_for_cycle(all_zips, "indiv", cycle)
            for fmeta in indiv_zips:
                path = os.path.join(tmpdir, fmeta["name"])
                download_file(fmeta["id"], path)
                for chunk in stream_individual_contributions(path):
                    stats["individual_rows"] += len(chunk)
                    matched = filter_contributions(chunk, district_map)
                    stats["individual_matched"] += len(matched)
                    for district, row in matched:
                        row_out = dict(row)
                        row_out["cycle"] = cycle
                        row_out["district"] = district
                        row_out["transaction_amt"] = float(row_out["transaction_amt"])
                        indiv_writer.writerow(row_out)
                        agg_indiv[(district, cycle)]["count"] += 1
                        agg_indiv[(district, cycle)]["total"] += row["transaction_amt"]
                os.unlink(path)

            # ── 5. PAC contributions ──
            pas_zips = _find_all_zips_for_cycle(all_zips, "pas", cycle)
            for fmeta in pas_zips:
                path = os.path.join(tmpdir, fmeta["name"])
                download_file(fmeta["id"], path)
                for chunk in stream_pac_contributions(path):
                    stats["pac_rows"] += len(chunk)
                    matched = filter_contributions(chunk, district_map)
                    stats["pac_matched"] += len(matched)
                    for district, row in matched:
                        row_out = dict(row)
                        row_out["cycle"] = cycle
                        row_out["district"] = district
                        row_out["transaction_amt"] = float(row_out["transaction_amt"])
                        pac_writer.writerow(row_out)
                        agg_pac[(district, cycle)]["count"] += 1
                        agg_pac[(district, cycle)]["total"] += row["transaction_amt"]
                os.unlink(path)

            # ── 6. Expenditures ──
            oppexp_zips = _find_all_zips_for_cycle(all_zips, "oppexp", cycle)
            for fmeta in oppexp_zips:
                path = os.path.join(tmpdir, fmeta["name"])
                download_file(fmeta["id"], path)
                for chunk in stream_expenditures(path):
                    stats["expenditure_rows"] += len(chunk)
                    matched = filter_contributions(chunk, district_map)
                    stats["expenditure_matched"] += len(matched)
                    for district, row in matched:
                        row_out = dict(row)
                        row_out["cycle"] = cycle
                        row_out["district"] = district
                        row_out["transaction_amt"] = float(row_out["transaction_amt"])
                        exp_writer.writerow(row_out)
                        agg_exp[(district, cycle)]["count"] += 1
                        agg_exp[(district, cycle)]["total"] += row["transaction_amt"]
                os.unlink(path)

            logger.info("Finished cycle %d", cycle)

        # Close detail CSVs
        cand_fh.close()
        cmte_fh.close()
        indiv_fh.close()
        pac_fh.close()
        exp_fh.close()

        # ── 7. Write district_summary.csv (one row per district per cycle) ──
        all_keys = sorted(
            set(list(agg_indiv) + list(agg_pac) + list(agg_exp))
        )
        sum_fh, sum_writer = _open_csv("district_summary.csv", [
            "district", "cycle",
            "individual_donation_count", "individual_donation_total",
            "pac_donation_count", "pac_donation_total",
            "expenditure_count", "expenditure_total",
            "total_raised", "net_cash_flow",
        ])

        for key in all_keys:
            district, cycle = key
            indiv = agg_indiv.get(key, {"count": 0, "total": Decimal("0")})
            pac = agg_pac.get(key, {"count": 0, "total": Decimal("0")})
            exp = agg_exp.get(key, {"count": 0, "total": Decimal("0")})
            total_raised = indiv["total"] + pac["total"]
            net = total_raised - exp["total"]

            sum_writer.writerow({
                "district": district,
                "cycle": cycle,
                "individual_donation_count": indiv["count"],
                "individual_donation_total": float(indiv["total"]),
                "pac_donation_count": pac["count"],
                "pac_donation_total": float(pac["total"]),
                "expenditure_count": exp["count"],
                "expenditure_total": float(exp["total"]),
                "total_raised": float(total_raised),
                "net_cash_flow": float(net),
            })

        sum_fh.close()
        logger.info(
            "Wrote district_summary.csv (%d rows across %d cycles)",
            len(all_keys), len(cycles),
        )

    finally:
        try:
            os.rmdir(tmpdir)
        except OSError:
            pass

    logger.info("Ingest complete: %s", dict(stats))
    return dict(stats)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run_ingest()
    print(result)
