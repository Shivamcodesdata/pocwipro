CREATE TABLE IF NOT EXISTS silver.sampleA (
    id BIGINT,
    name STRING,
    source_system STRING,
    processed_ts TIMESTAMP
)
USING DELTA;
