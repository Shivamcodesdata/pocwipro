CREATE TABLE IF NOT EXISTS silver.sampleB (
    order_id BIGINT,
    customer_id BIGINT,
    order_amount DOUBLE,
    order_status STRING,
    processed_ts TIMESTAMP
)
USING DELTA;
