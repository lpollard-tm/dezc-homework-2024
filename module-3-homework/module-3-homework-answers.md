## Week 3 Homework

<b>SETUP:</b></br>

* Create an external table using the Green Taxi Trip Records Data for 2022.
* Create a table in BigQuery using the Green Taxi Trip Records for 2022 (do not partition or cluster this table).

```sql
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
```

## Question 1:
Question 1: What is count of records for the 2022 Green Taxi Data??
- 65,623,481
- __*840,402*__
- 1,936,423
- 253,647

**ANSWER: 840,402**

```sql
SELECT COUNT(*) AS record_count
FROM `ny-taxi-rides-data.ny_taxi.green_taxi_2022`;
```

## Question 2:

Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.

What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 0 MB for the External Table and 6.41MB for the Materialized Table
- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- __*0 MB for the External Table and 0MB for the Materialized Table*__
- 2.14 MB for the External Table and 0MB for the Materialized Table

**ANSWER: 0 MB for the External Table and 0MB for the Materialized Table**

```sql
-- External Table

SELECT COUNT(DISTINCT PULocationID) AS distinct_PULocationIDs
FROM `ny-taxi-rides-data.ny_taxi.external_green_taxi_2022`;

-- Materalised Table 

SELECT COUNT(DISTINCT PULocationID) AS distinct_PULocationIDs
FROM `ny-taxi-rides-data.ny_taxi.green_taxi_2022`;
```


## Question 3:

How many records have a fare_amount of 0?

- 12,488
- 128,219
- 112
- __*1,622*__

**ANSWER: 1,622**

```sql
SELECT COUNT(*) AS zero_fare_amount_records
FROM `ny-taxi-rides-data.ny_taxi.green_taxi_2022`
WHERE fare_amount = 0;
```

## Question 4:

What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

- Cluster on lpep_pickup_datetime Partition by PUlocationID
- __*Partition by lpep_pickup_datetime  Cluster on PUlocationID*__
- Partition by lpep_pickup_datetime and Partition by PUlocationID
- Cluster on by lpep_pickup_datetime and Cluster on PUlocationID

**ANSWER: Partition by lpep_pickup_datetime  Cluster on PUlocationID**

```sql
CREATE TABLE `ny-taxi-rides-data.ny_taxi.green_taxi_2022_partition_lpep_cluster_pulocationid`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID
AS
SELECT *
FROM `ny-taxi-rides-data.ny_taxi.green_taxi_2022`;
```

## Question 5:

Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?

Choose the answer which most closely matches.

- 22.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- __*12.82 MB for non-partitioned table and 1.12 MB for the partitioned table*__
- 5.63 MB for non-partitioned table and 0 MB for the partitioned table
- 10.31 MB for non-partitioned table and 10.31 MB for the partitioned table

**ANSWER: 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table**

```sql
-- Materialised Non-partitioned & Non-clustered table - 12.82 MB

SELECT COUNT(DISTINCT PULocationID) AS distinct_PULocationIDs
FROM `ny-taxi-rides-data.ny_taxi.green_taxi_2022`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

-- Materialsied Partitioned on 'lpep_pickup_datetime' & Clustered on 'PULocationID' - 1.12 MB

SELECT COUNT(DISTINCT PULocationID) AS distinct_PULocationIDs
FROM `ny-taxi-rides-data.ny_taxi.green_taxi_2022_partition_lpep_cluster_pulocationid`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
```

## Question 6: 

Where is the data stored in the External Table you created?

- Big Query
- __*GCP Bucket*__
- Big Table
- Container Registry

**ANSWER: GCP Bucket**

## Question 7:

It is best practice in Big Query to always cluster your data:

- True
- __*False*__

**ANSWER: False - It's not necessarily best practice to always cluster your data, as the decision to cluster your data depends on several factors including your specific use case, query patterns, and performance requirements. Clustering your data can improve query performance by physically organising the data within each partition based on the clustering columns. This can lead to reduced data scan and processing times, especially for queries that involve filtering, joining, or grouping by the clustering columns. However, clustering also incurs additional costs for data storage and maintenance, as BigQuery automatically reorganises the data to maintain the clustering order when new data is inserted or existing data is updated**

## (Bonus: Not worth points) Question 8:

Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

**ANSWER - 0 bytes - the reason for this is because BigQuery determines that it can use it's in-built optimiser to satisfy the query entirely from the metadata or cached results (from the queries we previously ran) without the need to access the underlying data storage**

---------------------------
 
