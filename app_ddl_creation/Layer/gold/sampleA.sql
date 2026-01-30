CREATE TABLE IF NOT EXISTS gold.sampleA (
    total_customers BIGINT,
    report_date DATE,
    created_ts TIMESTAMP
)
USING DELTA;
