CREATE TABLE IF NOT EXISTS sampleA (
    id BIGINT,
    name STRING,
    source_system STRING,
    ingestion_ts TIMESTAMP
)
USING DELTA;
