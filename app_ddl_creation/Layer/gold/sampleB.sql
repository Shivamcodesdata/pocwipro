CREATE TABLE IF NOT EXISTS gold.sampleB (
    total_orders BIGINT,
    total_revenue DOUBLE,
    report_date DATE,
    created_ts TIMESTAMP
)
USING DELTA;
