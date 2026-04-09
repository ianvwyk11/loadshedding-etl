-- analysis_queries.sql
-- Analytical queries for the Load Shedding pipeline

-- 1. Most recent national stage per region
SELECT
    region,
    stage,
    stage_updated
FROM fact_status_snapshot
WHERE extracted_at = (SELECT MAX(extracted_at) FROM fact_status_snapshot)
ORDER BY stage DESC;

-- 2. Average stage over time per region
SELECT
    region,
    DATE(extracted_at) AS snapshot_date,
    ROUND(AVG(stage), 2) AS avg_stage
FROM fact_status_snapshot
GROUP BY region, snapshot_date
ORDER BY snapshot_date DESC;

-- 3. Total outage hours per area (top 20 most affected)
SELECT
    a.area_name,
    a.municipality,
    ROUND(SUM(f.duration_hours), 1) AS total_outage_hours
FROM fact_outages f
JOIN dim_area a ON f.area_id = a.area_id
GROUP BY a.area_name, a.municipality
ORDER BY total_outage_hours DESC
LIMIT 20;

-- 4. Outage hours by stage
SELECT
    s.stage_name,
    COUNT(*) AS outage_slots,
    ROUND(SUM(f.duration_hours), 1) AS total_hours
FROM fact_outages f
JOIN dim_stage s ON f.stage_id = s.stage_id
GROUP BY s.stage_name
ORDER BY s.stage_id;

-- 5. Outages by day of week
SELECT
    d.weekday,
    COUNT(*) AS outage_count,
    ROUND(AVG(f.duration_hours), 2) AS avg_duration_hours
FROM fact_outages f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.weekday
ORDER BY outage_count DESC;
