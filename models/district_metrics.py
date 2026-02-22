"""
SQLAlchemy model for the district_fundraising_metrics table.

This is the single dashboard-ready table.  The ingestion pipeline
UPSERTs into it so every run is idempotent.
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    create_engine,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

import config


class Base(DeclarativeBase):
    pass


class DistrictFundraisingMetrics(Base):
    __tablename__ = "district_fundraising_metrics"
    __table_args__ = (
        UniqueConstraint("district", "cycle", name="uq_district_cycle"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    district = Column(String, nullable=False, index=True)
    cycle = Column(Integer, nullable=False)
    donation_count = Column(Integer, nullable=False, default=0)
    donation_sum = Column(Numeric, nullable=False, default=0)
    last_updated = Column(DateTime, nullable=False, default=datetime.utcnow)

    def __repr__(self) -> str:
        return (
            f"<DistrictMetrics {self.district} cycle={self.cycle} "
            f"count={self.donation_count} sum={self.donation_sum}>"
        )


# ── Engine / session factory ──

engine = create_engine(config.DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)


def init_db() -> None:
    """Create tables if they don't exist."""
    Base.metadata.create_all(engine)


def upsert_metrics(
    session: Session,
    district: str,
    cycle: int,
    count_delta: int,
    sum_delta,
) -> None:
    """
    Atomically increment donation_count and donation_sum for a
    (district, cycle) row.  Creates the row if it doesn't exist.

    Uses raw SQL with ON CONFLICT for a true upsert — avoids
    race conditions when multiple workers run concurrently.
    """
    session.execute(
        text("""
            INSERT INTO district_fundraising_metrics
                   (district, cycle, donation_count, donation_sum, last_updated)
            VALUES (:district, :cycle, :count, :total, NOW())
            ON CONFLICT ON CONSTRAINT uq_district_cycle
            DO UPDATE SET
                donation_count = district_fundraising_metrics.donation_count + :count,
                donation_sum   = district_fundraising_metrics.donation_sum   + :total,
                last_updated   = NOW()
        """),
        {
            "district": district,
            "cycle": cycle,
            "count": count_delta,
            "total": sum_delta,
        },
    )
