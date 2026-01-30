CREATE TABLE IF NOT EXISTS bronze.sampleB (
    order_id BIGINT,
    customer_id BIGINT,
    order_amount DOUBLE,
    ingestion_ts TIMESTAMP
)
USING DELTA;
