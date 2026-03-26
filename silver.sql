CREATE OR REPLACE TABLE silver.data AS
SELECT DISTINCT *
FROM (
SELECT * FROM bronze.PART0
UNION ALL
SELECT * FROM bronze.PART1
UNION ALL
SELECT * FROM bronze.PART2
UNION ALL
SELECT * FROM bronze.PART3
UNION ALL
SELECT * FROM bronze.PART4
UNION ALL
SELECT * FROM bronze.PART5
UNION ALL
SELECT * FROM bronze.PART6
UNION ALL
SELECT * FROM bronze.PART7
UNION ALL
SELECT * FROM bronze.PART8
);

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