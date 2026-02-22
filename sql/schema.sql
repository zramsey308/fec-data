-- Clawbot: district_fundraising_metrics
-- Dashboard-ready aggregated FEC data by congressional district.

CREATE TABLE IF NOT EXISTS district_fundraising_metrics (
    id              SERIAL PRIMARY KEY,
    district        TEXT    NOT NULL,
    cycle           INTEGER NOT NULL,
    donation_count  INTEGER NOT NULL DEFAULT 0,
    donation_sum    NUMERIC NOT NULL DEFAULT 0,
    last_updated    TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_district_cycle UNIQUE (district, cycle)
);

-- Indexes for dashboard queries
CREATE INDEX IF NOT EXISTS idx_dfm_district ON district_fundraising_metrics (district);
CREATE INDEX IF NOT EXISTS idx_dfm_cycle    ON district_fundraising_metrics (cycle);
