-- create_tables.sql
-- Star schema for the Load Shedding ETL pipeline

-- Dimension: Area
CREATE TABLE IF NOT EXISTS dim_area (
    area_id     TEXT PRIMARY KEY,
    area_name   TEXT NOT NULL,
    region      TEXT,
    municipality TEXT
);

-- Dimension: Stage
CREATE TABLE IF NOT EXISTS dim_stage (
    stage_id    INTEGER PRIMARY KEY,
    stage_name  TEXT NOT NULL,   -- e.g. "Stage 4"
    hours_per_day REAL           -- approximate daily outage hours
);

-- Dimension: Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_id     TEXT PRIMARY KEY,  -- ISO date string YYYY-MM-DD
    year        INTEGER,
    month       INTEGER,
    day         INTEGER,
    weekday     TEXT,
    is_weekend  INTEGER            -- 1 = weekend, 0 = weekday
);

-- Fact: Scheduled Outage Slots
CREATE TABLE IF NOT EXISTS fact_outages (
    outage_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    area_id         TEXT REFERENCES dim_area(area_id),
    stage_id        INTEGER REFERENCES dim_stage(stage_id),
    date_id         TEXT REFERENCES dim_date(date_id),
    start_time      TEXT,          -- HH:MM
    end_time        TEXT,          -- HH:MM
    duration_hours  REAL,
    extracted_at    TEXT           -- ISO timestamp of when this was pulled
);

-- Snapshot: National status per run
CREATE TABLE IF NOT EXISTS fact_status_snapshot (
    snapshot_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    region          TEXT,
    stage           INTEGER,
    stage_updated   TEXT,
    next_stages     TEXT,
    extracted_at    TEXT
);

-- Seed dim_stage
INSERT OR IGNORE INTO dim_stage (stage_id, stage_name, hours_per_day) VALUES
    (0, 'No Load Shedding', 0),
    (1, 'Stage 1', 2),
    (2, 'Stage 2', 4),
    (3, 'Stage 3', 6),
    (4, 'Stage 4', 8),
    (5, 'Stage 5', 10),
    (6, 'Stage 6', 12),
    (7, 'Stage 7', 14),
    (8, 'Stage 8', 16)
