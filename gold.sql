CREATE OR REPLACE TABLE gold.summary AS
SELECT
    vendorid,
    COUNT(*) AS total_records,
    AVG(trip_distance) AS avg_trip_distance
FROM silver.data
GROUP BY vendorid;

SELECT * FROM gold.summary;

CREATE OR REPLACE TABLE gold.daily_metrics AS
SELECT
    DATE(tpep_pickup_datetime) AS trip_date,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(trip_distance) AS avg_distance
FROM silver.data
GROUP BY trip_date
ORDER BY trip_date;

SELECT * FROM gold.daily_metrics;

CREATE OR REPLACE TABLE gold.payment_analysis AS
SELECT
    payment_type,
    COUNT(*) AS trips,
    SUM(total_amount) AS revenue
FROM silver.data
GROUP BY payment_type
ORDER BY revenue DESC;

SELECT * FROM gold.payment_analysis;

CREATE OR REPLACE TABLE gold.tip_analysis AS
SELECT
    AVG(tip_amount) AS avg_tip,
    MAX(tip_amount) AS max_tip,
    MIN(tip_amount) AS min_tip
FROM silver.data;

SELECT * FROM gold.tip_analysis;