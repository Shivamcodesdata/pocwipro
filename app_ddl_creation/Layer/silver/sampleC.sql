CREATE TABLE IF NOT EXISTS silver.sampleC (
    event_id STRING,
    event_type STRING,
    event_date DATE,
    processed_ts TIMESTAMP
)
USING DELTA;
