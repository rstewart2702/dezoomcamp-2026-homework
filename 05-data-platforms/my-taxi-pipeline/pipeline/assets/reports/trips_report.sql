/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# TODO: Set the asset name (recommended: reports.trips_report).
name: reports.trips_report

# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependency on the staging asset(s) this report reads from.
depends:
  - staging.trips

# TODO: Choose materialization strategy.
# For reports, `time_interval` is a good choice to rebuild only the relevant time window.
# Important: Use the same `incremental_key` as staging (e.g., pickup_datetime) for consistency.
materialization:
  type: table
  # suggested strategy: time_interval
  strategy: time_interval
  # TODO: set to your report's date column
  incremental_key: trip_date
  # TODO: set to `date` or `timestamp`
  time_granularity: date

# TODO: Define report columns + primary key(s) at your chosen level of aggregation.
columns:
  - name: trip_date
    type: date
    description: "Date of the trip."
    primary_key: true
  - name: taxi_type
    type: string
    description: "Taxi type (i.e., yellow, green etc...)"
    primary_key: true
  - name: payment_type
    type: string
    description: "Type of payment (i.e., cash, credit-card, etc?)"
    primary_key: true
  - name: trip_count
    type: bigint
    description: "Not sure where this one came from, or where it actually is."
    checks:
      - name: non_negative

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

select
  cast(pickup_datetime as date) as trip_date
, taxi_type
, payment_type_name as payment_type
, count(*) as trip_count
, sum(fare_amount) as total_fare
, avg(fare_amount) as avg_fare
from staging.trips
where 
    pickup_datetime >= '{{ start_datetime }}'
and pickup_datetime <  '{{ end_datetime }}'
group by 1, 2, 3
;