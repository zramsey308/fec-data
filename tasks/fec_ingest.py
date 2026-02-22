"""
FEC ingestion → filtered CSV output for dashboard.

Orchestrates the full pipeline:
  1. List files on Google Drive.
  2. Download reference files → build district map.
  3. Stream each bulk file, filter to monitored districts, write CSV.
  4. Write a summary CSV with per-district totals.

Output CSVs (written to config.OUTPUT_DIR):
  - candidates.csv          candidate details for monitored districts
  - committees.csv          committee details for monitored districts
  - individual_contributions.csv   itemised individual donations
  - pac_contributions.csv   PAC / committee-to-committee donations
  - expenditures.csv        operating expenditures / disbursements
  - district_summary.csv    aggregated totals per district
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
# Pipeline
# ---------------------------------------------------------------------------

def _find_zip(files: list[dict], prefix: str) -> dict | None:
    """Find first ZIP whose name starts with the given prefix (case-insensitive)."""
    for f in files:
        if f["name"].lower().startswith(prefix):
            return f
    return None


def _find_all_zips(files: list[dict], prefix: str) -> list[dict]:
    """Find all ZIPs whose name starts with the given prefix."""
    return [f for f in files if f["name"].lower().startswith(prefix)]


def run_ingest(
    cycle: int | None = None,
    folder_id: str | None = None,
) -> dict:
    """
    Main entry point. Fetches FEC data from Drive, filters to monitored
    districts, and writes CSV files.
    """
    cycle = cycle or config.DEFAULT_CYCLE
    folder_id = folder_id or config.GOOGLE_DRIVE_FOLDER_ID

    all_zips = list_zip_files(folder_id)
    logger.info("Found %d ZIP files on Drive", len(all_zips))

    # Identify each file type
    cn_zip = _find_zip(all_zips, "cn")
    ccl_zip = _find_zip(all_zips, "ccl") or _find_zip(all_zips, "ccn")
    cm_zip = _find_zip(all_zips, "cm")
    indiv_zips = _find_all_zips(all_zips, "indiv")
    pas_zips = _find_all_zips(all_zips, "pas")
    oppexp_zips = _find_all_zips(all_zips, "oppexp")

    if not cn_zip:
        raise RuntimeError("No candidate master ZIP (cnYY.zip) found on Drive")

    tmpdir = tempfile.mkdtemp(prefix="clawbot_")
    stats = {
        "individual_rows": 0,
        "individual_matched": 0,
        "pac_rows": 0,
        "pac_matched": 0,
        "expenditure_rows": 0,
        "expenditure_matched": 0,
    }

    try:
        # ── 1. Download reference files and build district map ──
        cn_path = os.path.join(tmpdir, cn_zip["name"])
        download_file(cn_zip["id"], cn_path)

        ccl_path = None
        if ccl_zip:
            ccl_path = os.path.join(tmpdir, ccl_zip["name"])
            download_file(ccl_zip["id"], ccl_path)

        district_map = build_district_map(cn_path, ccl_path)

        if not district_map:
            raise RuntimeError(
                "District map is empty — no monitored-district committees found. "
                "Check config.MONITORED_DISTRICTS."
            )

        # Reverse map: district → set of cmte_ids (for committee/candidate output)
        district_cmtes: dict[str, set[str]] = defaultdict(set)
        for cmte_id, dist in district_map.items():
            district_cmtes[dist].add(cmte_id)

        # ── 2. Write candidates.csv ──
        cand_fh, cand_writer = _open_csv("candidates.csv", [
            "district", "cand_id", "cand_name", "cand_party",
            "cand_election_yr", "cand_office", "cand_ici", "cand_status",
        ])
        # Build set of candidate IDs in monitored districts
        monitored_cand_ids: set[str] = set()
        monitored_districts = set(config.MONITORED_DISTRICTS)

        for rec in stream_candidate_master(cn_path):
            st = rec["cand_office_st"]
            raw_d = rec["cand_office_district"]
            if not st or not raw_d:
                continue
            dist = f"{st}-AL" if raw_d == "00" else f"{st}-{raw_d.zfill(2)}"
            if dist in monitored_districts:
                monitored_cand_ids.add(rec["cand_id"])
                cand_writer.writerow({
                    "district": dist,
                    "cand_id": rec["cand_id"],
                    "cand_name": rec["cand_name"],
                    "cand_party": rec["cand_party"],
                    "cand_election_yr": rec["cand_election_yr"],
                    "cand_office": rec["cand_office"],
                    "cand_ici": rec["cand_ici"],
                    "cand_status": rec["cand_status"],
                })
        cand_fh.close()
        os.unlink(cn_path)
        if ccl_path:
            os.unlink(ccl_path)
        logger.info("Wrote candidates.csv (%d candidates)", len(monitored_cand_ids))

        # ── 3. Write committees.csv ──
        all_monitored_cmtes = set(district_map.keys())
        if cm_zip:
            cm_path = os.path.join(tmpdir, cm_zip["name"])
            download_file(cm_zip["id"], cm_path)

            cmte_fh, cmte_writer = _open_csv("committees.csv", [
                "district", "cmte_id", "cmte_name", "cmte_type",
                "designation", "party", "connected_org", "treasurer_name",
            ])
            cmte_count = 0
            for rec in stream_committee_master(cm_path):
                dist = district_map.get(rec["cmte_id"])
                if dist:
                    cmte_writer.writerow({
                        "district": dist,
                        "cmte_id": rec["cmte_id"],
                        "cmte_name": rec["cmte_name"],
                        "cmte_type": rec["cmte_type"],
                        "designation": rec["designation"],
                        "party": rec["party"],
                        "connected_org": rec["connected_org"],
                        "treasurer_name": rec["treasurer_name"],
                    })
                    cmte_count += 1
            cmte_fh.close()
            os.unlink(cm_path)
            logger.info("Wrote committees.csv (%d committees)", cmte_count)

        # ── 4. Write individual_contributions.csv ──
        indiv_fh, indiv_writer = _open_csv("individual_contributions.csv", [
            "district", "cmte_id", "contributor_name", "city", "state",
            "zip_code", "employer", "occupation", "transaction_date",
            "transaction_amt", "entity_type",
        ])

        # Aggregation accumulators
        agg_indiv: dict[str, dict] = defaultdict(
            lambda: {"count": 0, "total": Decimal("0")}
        )

        for fmeta in indiv_zips:
            path = os.path.join(tmpdir, fmeta["name"])
            download_file(fmeta["id"], path)
            for chunk in stream_individual_contributions(path):
                stats["individual_rows"] += len(chunk)
                matched = filter_contributions(chunk, district_map)
                stats["individual_matched"] += len(matched)
                for district, row in matched:
                    row_out = dict(row)
                    row_out["district"] = district
                    row_out["transaction_amt"] = float(row_out["transaction_amt"])
                    indiv_writer.writerow(row_out)
                    agg_indiv[district]["count"] += 1
                    agg_indiv[district]["total"] += row["transaction_amt"]
            os.unlink(path)

        indiv_fh.close()
        logger.info(
            "Wrote individual_contributions.csv (%d of %d rows)",
            stats["individual_matched"], stats["individual_rows"],
        )

        # ── 5. Write pac_contributions.csv ──
        pac_fh, pac_writer = _open_csv("pac_contributions.csv", [
            "district", "cmte_id", "contributor_name", "city", "state",
            "zip_code", "transaction_date", "transaction_amt",
            "other_id", "entity_type",
        ])

        agg_pac: dict[str, dict] = defaultdict(
            lambda: {"count": 0, "total": Decimal("0")}
        )

        for fmeta in pas_zips:
            path = os.path.join(tmpdir, fmeta["name"])
            download_file(fmeta["id"], path)
            for chunk in stream_pac_contributions(path):
                stats["pac_rows"] += len(chunk)
                matched = filter_contributions(chunk, district_map)
                stats["pac_matched"] += len(matched)
                for district, row in matched:
                    row_out = dict(row)
                    row_out["district"] = district
                    row_out["transaction_amt"] = float(row_out["transaction_amt"])
                    pac_writer.writerow(row_out)
                    agg_pac[district]["count"] += 1
                    agg_pac[district]["total"] += row["transaction_amt"]
            os.unlink(path)

        pac_fh.close()
        logger.info(
            "Wrote pac_contributions.csv (%d of %d rows)",
            stats["pac_matched"], stats["pac_rows"],
        )

        # ── 6. Write expenditures.csv ──
        exp_fh, exp_writer = _open_csv("expenditures.csv", [
            "district", "cmte_id", "recipient_name", "city", "state",
            "zip_code", "transaction_date", "transaction_amt", "purpose",
        ])

        agg_exp: dict[str, dict] = defaultdict(
            lambda: {"count": 0, "total": Decimal("0")}
        )

        for fmeta in oppexp_zips:
            path = os.path.join(tmpdir, fmeta["name"])
            download_file(fmeta["id"], path)
            for chunk in stream_expenditures(path):
                stats["expenditure_rows"] += len(chunk)
                matched = filter_contributions(chunk, district_map)
                stats["expenditure_matched"] += len(matched)
                for district, row in matched:
                    row_out = dict(row)
                    row_out["district"] = district
                    row_out["transaction_amt"] = float(row_out["transaction_amt"])
                    exp_writer.writerow(row_out)
                    agg_exp[district]["count"] += 1
                    agg_exp[district]["total"] += row["transaction_amt"]
            os.unlink(path)

        exp_fh.close()
        logger.info(
            "Wrote expenditures.csv (%d of %d rows)",
            stats["expenditure_matched"], stats["expenditure_rows"],
        )

        # ── 7. Write district_summary.csv ──
        sum_fh, sum_writer = _open_csv("district_summary.csv", [
            "district", "cycle",
            "individual_donation_count", "individual_donation_total",
            "pac_donation_count", "pac_donation_total",
            "expenditure_count", "expenditure_total",
            "total_raised", "net_cash_flow",
        ])

        all_districts = sorted(
            set(list(agg_indiv) + list(agg_pac) + list(agg_exp))
        )
        for dist in all_districts:
            indiv = agg_indiv.get(dist, {"count": 0, "total": Decimal("0")})
            pac = agg_pac.get(dist, {"count": 0, "total": Decimal("0")})
            exp = agg_exp.get(dist, {"count": 0, "total": Decimal("0")})
            total_raised = indiv["total"] + pac["total"]
            net = total_raised - exp["total"]

            sum_writer.writerow({
                "district": dist,
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
        logger.info("Wrote district_summary.csv (%d districts)", len(all_districts))

    finally:
        try:
            os.rmdir(tmpdir)
        except OSError:
            pass

    logger.info("Ingest complete: %s", stats)
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run_ingest()
    print(result)
