CREATE TABLE IF NOT EXISTS sampleC (
    event_type STRING,
    event_count BIGINT,
    report_date DATE,
    created_ts TIMESTAMP
)
USING DELTA;
