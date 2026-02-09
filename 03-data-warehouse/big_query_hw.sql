-- In Google's documentation, it's:
--   gs://<BUCKET_NAME>/[<FOLDER_NAME>/]<FILE_NAME>
-- 
/* So, this creates an external table using the Yellow Taxi Trip Records
   loaded by the load_yellow_tax_data.py Python program;
   it loaded all of the records for months 01 through 06 of 2024.
*/
--                                taxi-rides-ny.nytaxi.fhv_tripdata
CREATE OR REPLACE EXTERNAL TABLE `datatalks-dezoomcamp2026.datatalks_dezoomcamp2026_dataset.fhv_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://datatalks-dezoomcamp2026-puddle-bucket/yellow_tripdata_2024-*.parquet']
);


/* Count how many rows got loaded for the year 2024, assuming that
   what we want to see is the count of records for the first 6 months of 2024.
*/
SELECT count(*) FROM `datatalks-dezoomcamp2026.datatalks_dezoomcamp2026_dataset.fhv_tripdata`;


SELECT COUNT(DISTINCT(dispatching_base_num)) FROM `datatalks-dezoomcamp2026.datatalks_dezoomcamp2026_dataset.fhv_tripdata`;


/* Creates a "regular, nonpartitioned bigquery table" from the storage-bucket-hosted, "external" data.
*/
CREATE OR REPLACE TABLE `datatalks-dezoomcamp2026.datatalks_dezoomcamp2026_dataset.fhv_nonpartitioned_tripdata`
AS SELECT * FROM `datatalks-dezoomcamp2026.datatalks_dezoomcamp2026_dataset.fhv_tripdata`;

/* Creates a "partitioned bigquery table" from the storage-bucket-hosted, "external" data.
   This one will be partitioned by dropoff_datetime.
   (hmmm, partitioning by a date-and-time-stamp?  Does not make a ton of sense to me, up front...)
*/
CREATE OR REPLACE TABLE `datatalks-dezoomcamp2026.datatalks_dezoomcamp2026_dataset.fhv_partitioned_tripdata`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS (
  SELECT * FROM `datatalks-dezoomcamp2026.datatalks_dezoomcamp2026_dataset.fhv_tripdata`
);

SELECT count(*) FROM  `datatalks-dezoomcamp2026.datatalks_dezoomcamp2026_dataset.fhv_nonpartitioned_tripdata`
WHERE DATE(dropoff_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
  AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');


SELECT count(*) FROM `datatalks-dezoomcamp2026.datatalks_dezoomcamp2026_dataset.fhv_partitioned_tripdata`
WHERE DATE(dropoff_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
  AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');
