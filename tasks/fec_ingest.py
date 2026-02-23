"""
FEC ingestion → filtered CSV output for dashboard (multi-cycle).

Downloads FEC bulk data directly from fec.gov (no credentials needed),
filters to monitored districts, and writes combined CSV files.

Pipeline per cycle:
  1. Download candidate master + linkage → build district map.
  2. Download committee master, individual/PAC contributions, expenditures.
  3. Stream each file, keep only monitored-district records, write CSVs.
  4. Delete the downloaded file before moving to the next.

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
from services.fec_download import download_fec_file

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


def _download_and_clean(prefix: str, cycle: int, tmpdir: str) -> str | None:
    """Download an FEC file, return its path, or None if unavailable."""
    path = download_fec_file(prefix, cycle, tmpdir)
    if path and not os.path.exists(path):
        return None
    return path


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run_ingest(cycles: list[int] | None = None) -> dict:
    """
    Main entry point.  Downloads FEC bulk data from fec.gov for each cycle,
    filters to monitored districts, and writes combined CSV files.
    """
    cycles = cycles or config.FEC_CYCLES

    print(f"=== Processing cycles: {cycles} ===")
    print(f"=== Monitored districts: {config.MONITORED_DISTRICTS} ===")

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
            print(f"\n{'='*60}")
            print(f"  Processing cycle {cycle}")
            print(f"{'='*60}")

            # ── 1. Build district map for this cycle ──
            cn_path = _download_and_clean("cn", cycle, tmpdir)
            if not cn_path:
                print(f"  [SKIP] No candidate master (cn) for {cycle}")
                continue

            ccl_path = _download_and_clean("ccl", cycle, tmpdir)

            district_map = build_district_map(cn_path, ccl_path)
            print(f"  District map: {len(district_map)} committees across "
                  f"{len(set(district_map.values()))} districts")

            if not district_map:
                print(f"  [SKIP] No monitored districts found for cycle {cycle}")
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
            print(f"  Candidates: {stats['candidates']} total so far")

            # ── 3. Write committees for this cycle ──
            cm_path = _download_and_clean("cm", cycle, tmpdir)
            if cm_path:
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
            print(f"  Committees: {stats['committees']} total so far")

            # ── 4. Individual contributions ──
            indiv_path = _download_and_clean("indiv", cycle, tmpdir)
            if indiv_path:
                for chunk in stream_individual_contributions(indiv_path):
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
                os.unlink(indiv_path)
                print(f"  Individual contributions: {stats['individual_matched']} "
                      f"matched / {stats['individual_rows']} scanned")

            # ── 5. PAC contributions ──
            pas_path = _download_and_clean("pas", cycle, tmpdir)
            if pas_path:
                for chunk in stream_pac_contributions(pas_path):
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
                os.unlink(pas_path)
                print(f"  PAC contributions: {stats['pac_matched']} "
                      f"matched / {stats['pac_rows']} scanned")

            # ── 6. Expenditures ──
            oppexp_path = _download_and_clean("oppexp", cycle, tmpdir)
            if oppexp_path:
                for chunk in stream_expenditures(oppexp_path):
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
                os.unlink(oppexp_path)
                print(f"  Expenditures: {stats['expenditure_matched']} "
                      f"matched / {stats['expenditure_rows']} scanned")

            print(f"  Cycle {cycle} complete.")

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
        print(f"\n=== Wrote district_summary.csv "
              f"({len(all_keys)} rows across {len(cycles)} cycles) ===")

    finally:
        try:
            os.rmdir(tmpdir)
        except OSError:
            pass

    print(f"\n=== Ingest complete: {dict(stats)} ===")
    return dict(stats)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run_ingest()
