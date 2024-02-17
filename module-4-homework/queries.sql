
-- run web_to_gcs.py to download files & upload to Google Cloud Bucket first 
-- manually added taxi_zone_lookup.csv to Google Cloud Bucket 
-- manually create dataset 'trips_data_all' in region 'europe-west9' 

CREATE OR REPLACE EXTERNAL TABLE `ny-taxi-rides-data.trips_data_all.external_green_tripdata`
OPTIONS(
  format = 'PARQUET', 
  URIS = ['gs://mage-zoomcamp-967682f4482f/green/green_tripdata_202-*.parquet', 'gs://mage-zoomcamp-967682f4482f/green/green_tripdata_2019-*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE `ny-taxi-rides-data.trips_data_all.external_green_tripdata`
OPTIONS(
  format = 'PARQUET', 
  URIS = ['gs://mage-zoomcamp-967682f4482f/green/green_tripdata_202-*.parquet', 'gs://mage-zoomcamp-967682f4482f/green/green_tripdata_2019-*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE `ny-taxi-rides-data.trips_data_all.taxi_zone_lookup`
OPTIONS(
  format = 'CSV', 
  URIS = ['gs://mage-zoomcamp-967682f4482f/taxi_zone_lookup.csv']
);

  -- Fixes yellow table schema
ALTER TABLE `ny-taxi-rides-data.trips_data_all.external_yellow_tripdata`
  RENAME COLUMN vendor_id TO VendorID;
ALTER TABLE `ny-taxi-rides-data.trips_data_all.external_yellow_tripdata`
  RENAME COLUMN pickup_datetime TO tpep_pickup_datetime;
ALTER TABLE `ny-taxi-rides-data.trips_data_all.external_yellow_tripdata`
  RENAME COLUMN dropoff_datetime TO tpep_dropoff_datetime;
ALTER TABLE `ny-taxi-rides-data.trips_data_all.external_yellow_tripdata`
  RENAME COLUMN rate_code TO RatecodeID;
ALTER TABLE `ny-taxi-rides-data.trips_data_all.external_yellow_tripdata`
  RENAME COLUMN imp_surcharge TO improvement_surcharge;
ALTER TABLE `ny-taxi-rides-data.trips_data_all.external_yellow_tripdata`
  RENAME COLUMN pickup_location_id TO PULocationID;
ALTER TABLE `ny-taxi-rides-data.trips_data_all.external_yellow_tripdata`
  RENAME COLUMN dropoff_location_id TO DOLocationID;

  -- Fixes green table schema
ALTER TABLE `ny-taxi-rides-data.trips_data_all.external_green_tripdata`
  RENAME COLUMN vendor_id TO VendorID;
ALTER TABLE `ny-taxi-rides-data.trips_data_all.external_green_tripdata`
  RENAME COLUMN pickup_datetime TO lpep_pickup_datetime;
ALTER TABLE `ny-taxi-rides-data.trips_data_all.external_green_tripdata`
  RENAME COLUMN dropoff_datetime TO lpep_dropoff_datetime;
ALTER TABLE `ny-taxi-rides-data.trips_data_all.external_green_tripdata`
  RENAME COLUMN rate_code TO RatecodeID;
ALTER TABLE `ny-taxi-rides-data.trips_data_all.external_green_tripdata`
  RENAME COLUMN imp_surcharge TO improvement_surcharge;
ALTER TABLE `ny-taxi-rides-data.trips_data_all.external_green_tripdata`
  RENAME COLUMN pickup_location_id TO PULocationID;
ALTER TABLE `ny-taxi-rides-data.trips_data_all.external_green_tripdata`
  RENAME COLUMN dropoff_location_id TO DOLocationID;