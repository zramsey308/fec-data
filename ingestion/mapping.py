"""
Candidate → District mapping layer.

Builds lookups from committee IDs to normalised district strings like "TX-32"
using both the candidate master (cnYY.zip) and candidate-committee linkage
(ccnYY.zip) so we capture *all* committees associated with a candidate —
not just the principal campaign committee.
"""
from __future__ import annotations

import logging
from typing import Iterable

import config
from ingestion.fec_stream import (
    stream_candidate_committee_linkage,
    stream_candidate_master,
)

logger = logging.getLogger(__name__)


def build_district_map(
    candidate_master_zip: str,
    linkage_zip: str | None = None,
    monitored: set[str] | None = None,
) -> dict[str, str]:
    """
    Return {cmte_id: "ST-DD"} for every committee whose candidate
    runs in one of the *monitored* districts.

    Uses PCC from candidate master, plus all committees from the
    candidate-committee linkage file if provided.
    """
    monitored = monitored or set(config.MONITORED_DISTRICTS)
    cmte_to_district: dict[str, str] = {}

    # Map candidate IDs to districts
    cand_to_district: dict[str, str] = {}

    for rec in stream_candidate_master(candidate_master_zip):
        state = rec["cand_office_st"]
        raw_dist = rec["cand_office_district"]
        pcc = rec["cand_pcc"]
        cand_id = rec["cand_id"]

        if not state or not raw_dist:
            continue

        if raw_dist == "00":
            district = f"{state}-AL"
        else:
            district = f"{state}-{raw_dist.zfill(2)}"

        if district not in monitored:
            continue

        cand_to_district[cand_id] = district

        # Always include the PCC
        if pcc:
            cmte_to_district[pcc] = district

    # If we have the linkage file, add all linked committees
    if linkage_zip:
        for rec in stream_candidate_committee_linkage(linkage_zip):
            cand_id = rec["cand_id"]
            cmte_id = rec["cmte_id"]
            district = cand_to_district.get(cand_id)
            if district and cmte_id:
                cmte_to_district[cmte_id] = district

    logger.info(
        "Built district map: %d committees across %d districts",
        len(cmte_to_district),
        len(set(cmte_to_district.values())),
    )
    return cmte_to_district


def filter_contributions(
    chunk: Iterable[dict],
    district_map: dict[str, str],
) -> list[tuple[str, dict]]:
    """
    Given a chunk of contribution/expenditure dicts, return only those whose
    cmte_id appears in the district map, tagged with the district.
    """
    results = []
    for row in chunk:
        district = district_map.get(row["cmte_id"])
        if district is not None:
            results.append((district, row))
    return results
