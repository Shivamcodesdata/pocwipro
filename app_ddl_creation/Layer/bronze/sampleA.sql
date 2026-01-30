CREATE TABLE IF NOT EXISTS bronze.sampleA (
    id BIGINT,
    name STRING,
    source_system STRING,
    ingestion_ts TIMESTAMP
)
USING DELTA;
