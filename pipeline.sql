CREATE WAREHOUSE my_warehouse
WITH WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE;

CREATE DATABASE data_project;

CREATE SCHEMA data_project.bronze;
CREATE SCHEMA data_project.silver;
CREATE SCHEMA data_project.gold;


CREATE OR REPLACE TABLE silver.data AS
SELECT * FROM bronze.czesc0
UNION ALL
SELECT * FROM bronze.czesc1
UNION ALL
SELECT * FROM bronze.czesc2
UNION ALL
SELECT * FROM bronze.czesc3
UNION ALL
SELECT * FROM bronze.czesc4
UNION ALL
SELECT * FROM bronze.czesc5
UNION ALL
SELECT * FROM bronze.czesc6
UNION ALL
SELECT * FROM bronze.czesc7
UNION ALL
SELECT * FROM bronze.czesc8;

SELECT COUNT(*) FROM silver.data;

CREATE OR REPLACE TABLE silver.data AS
SELECT DISTINCT *
FROM (
SELECT * FROM bronze.czesc1
UNION ALL
SELECT * FROM bronze.czesc2
UNION ALL
SELECT * FROM bronze.czesc3
UNION ALL
SELECT * FROM bronze.czesc4
UNION ALL
SELECT * FROM bronze.czesc5
UNION ALL
SELECT * FROM bronze.czesc6
UNION ALL
SELECT * FROM bronze.czesc7
UNION ALL
SELECT * FROM bronze.czesc8
);

SELECT COUNT(*) FROM silver.data;

SELECT * FROM silver.data LIMIT 10;

DELETE FROM silver.data
WHERE vendorid IS NULL;

DELETE FROM silver.data
WHERE fare_amount < 0;

DELETE FROM silver.data
WHERE total_amount < 0;

SELECT MIN(total_amount) FROM silver.data;
SELECT MAX(total_amount) FROM silver.data;

DELETE FROM silver.data
WHERE total_amount > 10000;

DELETE FROM silver.data
WHERE trip_distance=0;

DELETE FROM silver.data
WHERE passenger_count=0;

DELETE FROM silver.data
WHERE pickup_latitude = 0
   OR pickup_longitude = 0
   OR dropoff_latitude = 0
   OR dropoff_longitude = 0;

DELETE FROM silver.data
WHERE tpep_dropoff_datetime < tpep_pickup_datetime;

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




