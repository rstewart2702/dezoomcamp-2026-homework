-- These two statements create "external tables" which are merely
-- wrappers around a mechanism to turn those csv files into 
-- tables.  These "external tables" are used to populate the
-- data warehouse tables built by the dbt project.

create or replace external table `datatalks-dezoomcamp2026.nytaxi_04.yellow_tripdata`
options (
  format = 'CSV',
  uris = [ 'gs://datatalks-dezoomcamp2026-puddle-bucket/yellow_tripdata_2019-*.csv.gz',
           'gs://datatalks-dezoomcamp2026-puddle-bucket/yellow_tripdata_2020-*.csv.gz' ]
);

create or replace external table `datatalks-dezoomcamp2026.nytaxi_04.green_tripdata`
options (
  format = 'CSV',
  uris = [ 'gs://datatalks-dezoomcamp2026-puddle-bucket/green_tripdata_2019-*.csv.gz',
           'gs://datatalks-dezoomcamp2026-puddle-bucket/green_tripdata_2020-*.csv.gz' ]
);



-- =======================================================================

-- Question 3:
-- after running your dbt project ,query the fct_monthly_zone_revenue model.
-- what is the count of records in the fct_monthly_zone_revenue model?

-- I DO NOT UNDERSTAND WHY I AM NOT SEEING ANY OF THE POSSIBLE TALLIES MENTIONED FOR QUESTION 3:
select count(*) from `datatalks-dezoomcamp2026.nytaxi_04.yellow_tripdata`;

-- TRYING TO UNDERSTAND WHY I DON'T GET ANYTHING CLOSE TO THE NUMBER OF ROWS 
-- OUT OF THE POSSIBILITIES OF 12998, 14120, 12184, 15421
select count(*) orig_row_tally, count(*) * 2 doubled_row_tally, sum(total_monthly_trips) trip_tally from `dbt_rstewart.fct_monthly_zone_revenue`;
-- rerunning the dbt with:  "dbt build --target prod --full-refresh" was what I needed to do after fixing the uris for the "external tables"
-- which are the source for this thing!
-- this was the originally incorrect answer:
-- 6684, 13368, 87551133
--
-- Here are the corrected tallies after the "dbt built --target prod --full-refresh":
-- 12184, 24368, 112086662
-- ANSWER:  12184

select count(*) from `dbt_rstewart.stg_yellow_tripdata`;
-- 107991349
-- after refresh, still the same:
--   107791349

select distinct revenue_month, service_type from `dbt_rstewart.fct_monthly_zone_revenue` order by service_type, revenue_month;

--==============================================================================

-- trying to see if I get anything close to one of the possible answers for questions 4 or 5:
-- Question 4: best performing zone for green taxis 2020
--             find pickup zone with highest total revenue (revenue_monthly_total_amount) for Green taxi trips in 2020.
-- choices are 'East Harlem North' 20.6,'Morningside Heights','East Harlem South','Washington Heights South'
select t.pickup_zone, sum(t.revenue_monthly_total_amount) tot_revenue, t.service_type  
from `dbt_rstewart.fct_monthly_zone_revenue` t 
where 
    t.service_type = 'Green' 
and 
    '2020-01-01' <= t.revenue_month and t.revenue_month <= '2020-12-01'
group by t.pickup_zone, t.service_type
order by sum(t.revenue_monthly_total_amount) desc;
-- after refresh:  'East Harlem North' 1817773.65, 'Morningside Heights' 764404.44, 'East Harlem South' 1653337.61, 'Washington Heights South' 879938.2
-- ANSWER:  'East Harlem North' 1817773.65

--==============================================================================

-- Question 5:  what is the total number of trips (total_monthly_trips) for Green taxis in October 2019?
-- the choices were:  500234, 350891, 384624, 421509
select sum(t.total_monthly_trips) sum_tot_monthly_trips
from `dbt_rstewart.fct_monthly_zone_revenue` t where t.service_type='Green' and t.revenue_month = '2019-10-01';
-- well, I got 384624, and that is one of the possible answers...
-- refresh didn't change this answer.
-- ANSWER:  384624

--=====================================================================

/* Some more research for question 2: */
select payment_type, count(*) row_tally
from `dbt_rstewart.fct_trips` 
group by payment_type
order by payment_type;

--=========================================================================
-- work for question 6:

create or replace external table `datatalks-dezoomcamp2026.nytaxi_04.fhv_tripdata`
options (
  format = 'CSV',
  uris = [ 'gs://datatalks-dezoomcamp2026-puddle-bucket/fhv_tripdata_2019-*.csv.gz' ]
);

select * from `nytaxi_04.fhv_tripdata` where "SR_Flag" is not null;

select t.SR_Flag, count(*) sr_flag_tally from `nytaxi_04.fhv_tripdata` t group by t.SR_Flag order by t.SR_Flag;


-- debugging my attempt at "model SQL":
with source as (
    select * from `datatalks-dezoomcamp2026`.`nytaxi_04`.`fhv_tripdata`
),

renamed as (
    select 
      -- identifiers
      cast(dispatching_base_num as string) as dispatching_base_num,

      -- timestamps
      cast(pickup_datetime as timestamp) as pickup_datetime,
      cast(dropOff_datetime as timestamp) as dropoff_datetime,

      -- location keys:
      cast(PUlocationID as integer) as pickup_location_id,
      cast(DOlocationID as integer) as dropoff_location_id,

      -- "SR Flag?"
      cast(SR_Flag as integer) as sr_flag,

      cast(Affiliated_base_number as string) as affiliated_base_number
    from source
    -- some filtering as required in homework 04:
    where 
        dispatching_base_num is not null
)

select * from renamed;


-- Got the view created, and now we must query it:
select count(*) from `dbt_rstewart.stg_fhv_tripdata`;
-- derived a count of 43244693
