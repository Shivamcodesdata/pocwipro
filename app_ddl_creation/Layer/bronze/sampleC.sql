CREATE TABLE IF NOT EXISTS sampleC (
    event_id STRING,
    event_type STRING,
    event_time TIMESTAMP,
    ingestion_ts TIMESTAMP
)
USING DELTA;
