
-- WEEK 3 HOMEWORK - DATA WARHOUSING & BIG QUERY

-- SETUP: 

-- CREATE AN EXTERNAL TABLE REFERRING TO GREEN TAXI FILES IN GOOGLE CLOUD STORAGE 

CREATE OR REPLACE EXTERNAL TABLE `ny-taxi-rides-data.ny_taxi.external_green_taxi_2022`
OPTIONS(
  format = 'PARQUET', 
  URIS = ['gs://mage-zoomcamp-967682f4482f/green_taxi_2022/green_tripdata_2022-*.parquet']
);

-- CREATE A TABLE IN BIGQUERY USING THE GREEN TAXI TRIP RECORDS FOR 2022 (DO NOT PARTITION OR CLUSTER)

CREATE TABLE `ny-taxi-rides-data.ny_taxi.green_taxi_2022`
AS
SELECT *
FROM `ny-taxi-rides-data.ny_taxi.external_green_taxi_2022`;

-----------------------------------------------------------------------------------------------------------

 -- QUESTION 1:
 -- What is count of records for the 2022 Green Taxi Data??

SELECT COUNT(*) AS record_count
FROM `ny-taxi-rides-data.ny_taxi.green_taxi_2022`;

-----------------------------------------------------------------------------------------------------------

-- QUESTION 2:
-- Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.

-- External Table

SELECT COUNT(DISTINCT PULocationID) AS distinct_PULocationIDs
FROM `ny-taxi-rides-data.ny_taxi.external_green_taxi_2022`;

-- Materalised Table 

SELECT COUNT(DISTINCT PULocationID) AS distinct_PULocationIDs
FROM `ny-taxi-rides-data.ny_taxi.green_taxi_2022`;

-----------------------------------------------------------------------------------------------------------

-- QUESTION 3:

-- How many records have a fare_amount of 0?

SELECT COUNT(*) AS zero_fare_amount_records
FROM `ny-taxi-rides-data.ny_taxi.green_taxi_2022`
WHERE fare_amount = 0;

-----------------------------------------------------------------------------------------------------------

-- QUESTION 4:

-- What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)


CREATE TABLE `ny-taxi-rides-data.ny_taxi.green_taxi_2022_partition_lpep_cluster_pulocationid`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID
AS
SELECT *
FROM `ny-taxi-rides-data.ny_taxi.green_taxi_2022`;

-----------------------------------------------------------------------------------------------------------

-- QUESTION 5:

-- Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
-- Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed.

-- Materialised Non-partitioned & Non-clustered table - 12.85 MB

SELECT COUNT(DISTINCT PULocationID) AS distinct_PULocationIDs
FROM `ny-taxi-rides-data.ny_taxi.green_taxi_2022`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

-- Materialsied Partitioned on 'lpep_pickup_datetime' & Clustered on 'PULocationID' - 1.12 MB

SELECT COUNT(DISTINCT PULocationID) AS distinct_PULocationIDs
FROM `ny-taxi-rides-data.ny_taxi.green_taxi_2022_partition_lpep_cluster_pulocationid`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';




