"""
Candidate → District mapping layer.

Builds a lookup from *committee ID* (CMTE_ID) to a normalised district
string like "TX-32" by joining:

  committee  →  candidate (via CAND_PCC in the candidate master)
  candidate  →  state + district

The map is built once from the FEC candidate master file (cnYY.zip)
streamed line-by-line so memory stays flat, then held as a plain dict
for O(1) lookups during contribution ingestion.
"""
from __future__ import annotations

import logging
from typing import Iterable

import config
from ingestion.fec_stream import stream_candidate_master

logger = logging.getLogger(__name__)


def build_district_map(
    candidate_master_zip: str,
    monitored: set[str] | None = None,
) -> dict[str, str]:
    """
    Return {cmte_id: "ST-DD"} for every principal campaign committee whose
    candidate runs in one of the *monitored* districts.

    Parameters
    ----------
    candidate_master_zip:
        Path to the downloaded cnYY.zip file.
    monitored:
        Set of district strings to include (e.g. {"TX-32", "AL-01"}).
        Defaults to config.MONITORED_DISTRICTS.

    Returns
    -------
    dict mapping committee IDs to district labels.
    """
    monitored = monitored or set(config.MONITORED_DISTRICTS)
    cmte_to_district: dict[str, str] = {}

    for rec in stream_candidate_master(candidate_master_zip):
        state = rec["cand_office_st"]
        raw_dist = rec["cand_office_district"]
        pcc = rec["cand_pcc"]

        if not state or not raw_dist or not pcc:
            continue

        # Normalise: FEC stores district as "02", we want "TX-02".
        # At-large seats are stored as "00" — map to "ST-AL".
        if raw_dist == "00":
            district = f"{state}-AL"
        else:
            district = f"{state}-{raw_dist.zfill(2)}"

        if district not in monitored:
            continue

        cmte_to_district[pcc] = district

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
    Given a chunk of ContributionRow dicts, return only those whose
    CMTE_ID appears in the district map, tagged with the district.

    Returns a list of (district, row) pairs.
    """
    results = []
    for row in chunk:
        district = district_map.get(row["cmte_id"])
        if district is not None:
            results.append((district, row))
    return results
