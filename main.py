"""
Clawbot — FastAPI application.

Exposes:
  POST /ingest          → kick off FEC ingestion as a background task
  GET  /metrics         → return dashboard-ready district metrics
  GET  /metrics/{dist}  → return metrics for a single district
  GET  /health          → liveness probe for Render
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from sqlalchemy import text

import config
from models.district_metrics import SessionLocal, init_db
from tasks.fec_ingest import run_ingest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Clawbot", version="0.1.0")

# Single-thread pool so we never run two ingests concurrently
_executor = ThreadPoolExecutor(max_workers=1)
_ingest_running = False


# ── Startup ──

@app.on_event("startup")
def on_startup():
    init_db()
    logger.info("Database tables verified.")


# ── Health ──

@app.get("/health")
def health():
    return {"status": "ok"}


# ── Trigger ingestion ──

def _background_ingest(cycle: int):
    """Wrapper that sets/clears the running flag."""
    global _ingest_running
    _ingest_running = True
    try:
        result = run_ingest(cycle=cycle)
        logger.info("Background ingest finished: %s", result)
    except Exception:
        logger.exception("Background ingest failed")
    finally:
        _ingest_running = False


@app.post("/ingest")
def trigger_ingest(
    background_tasks: BackgroundTasks,
    cycle: int = Query(default=config.DEFAULT_CYCLE),
):
    """
    Kick off FEC data ingestion in a background thread.

    The actual work runs off the main ASGI event loop so it never
    blocks HTTP request handling.
    """
    if _ingest_running:
        raise HTTPException(status_code=409, detail="Ingestion already in progress")

    background_tasks.add_task(_background_ingest, cycle)
    return {"status": "started", "cycle": cycle}


# ── Dashboard-ready endpoints ──

@app.get("/metrics")
def get_all_metrics(cycle: Optional[int] = Query(default=None)):
    """Return aggregated metrics for all monitored districts."""
    session = SessionLocal()
    try:
        query = "SELECT district, cycle, donation_count, donation_sum, last_updated FROM district_fundraising_metrics"
        params = {}
        if cycle:
            query += " WHERE cycle = :cycle"
            params["cycle"] = cycle
        query += " ORDER BY district, cycle"

        rows = session.execute(text(query), params).fetchall()
        return [
            {
                "district": r[0],
                "cycle": r[1],
                "donation_count": r[2],
                "donation_sum": float(r[3]),
                "last_updated": r[4].isoformat() if r[4] else None,
            }
            for r in rows
        ]
    finally:
        session.close()


@app.get("/metrics/{district}")
def get_district_metrics(district: str, cycle: Optional[int] = Query(default=None)):
    """Return metrics for a single district."""
    district = district.upper()
    if district not in config.MONITORED_DISTRICTS:
        raise HTTPException(status_code=404, detail=f"District {district} is not monitored")

    session = SessionLocal()
    try:
        query = "SELECT district, cycle, donation_count, donation_sum, last_updated FROM district_fundraising_metrics WHERE district = :district"
        params: dict = {"district": district}
        if cycle:
            query += " AND cycle = :cycle"
            params["cycle"] = cycle

        rows = session.execute(text(query), params).fetchall()
        if not rows:
            raise HTTPException(status_code=404, detail="No data for this district yet")

        return [
            {
                "district": r[0],
                "cycle": r[1],
                "donation_count": r[2],
                "donation_sum": float(r[3]),
                "last_updated": r[4].isoformat() if r[4] else None,
            }
            for r in rows
        ]
    finally:
        session.close()
