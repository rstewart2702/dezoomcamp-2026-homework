"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# TODO: Set the connection.
connection: duckdb-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"

@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python

import os
import json
import pandas as pd
import pyarrow

def list_of_months(start_month, end_month):
    '''
    Uses "ordinary" while loop interation instead of range-objects.
    Range objects could probably be used, but I struggled to come up
    with an elegant, reasonable way to use them which was not more 
    complicated than what I have done here.
    '''
    # We assume that the start_month, end_month have format "yyyy-mm".
    initial_year = int(start_month[0:4])  # should be the "yyyy" part at the start of the string.
    initial_month = int(start_month[5:7])  # should be the "mm" part at the end of the string.
    # And then the same reasoning applies to the end_month:
    end_year = int(end_month[0:4])
    end_month = int(end_month[5:7])
    #
    yr = initial_year
    mth = initial_month
    while yr != end_year:
        yield(yr,mth)
        mth += 1
        while mth != 13:
            yield(yr,mth)
            mth += 1
        mth = 1
        yr += 1
    # and now we decide what to do because yr == end_year
    while mth != end_month+1 :
        yield (yr,mth)
        mth += 1
    #

column_mappings = {
    'green' :  {
      'VendorID': 'vendor_id',
      'lpep_pickup_datetime': 'pickup_datetime',
      'lpep_dropoff_datetime': 'dropoff_datetime',
      'store_and_fwd_flag': '',
      'RateCodeID': 'rate_code_id',
      'PULocationID': 'pickup_location_id',
      'DOLocationID': 'dropoff_location_id',
      'passenger_count': 'passenger_count'
      'trip_distance': 'trip_distance'
      'fare_amount': 'fare_amount'
      'extra': 'extra'
      'mta_tax': 'mta_tax'
      'tip_amount': 'tip_amount'
      'tolls_amount': 'tolls_amount'
      'ehail_fee': 'ehail_fee'
      'improvement_surcharge': 'improvement_surcharge'
      'total_amount': 'total_amount'
      'payment_type': 'payment_type_name'
      'trip_type': 'trip_type',
      'congestion_surcharge': 'congestion_surcharge'
    },
    'yellow' : {
      'VendorID': 'vendor_id',
      'tpep_pickup_datetime': 'pickup_datetime',
      'tpep_dropoff_datetime': 'dropoff_datetime',
      'passenger_count': 'passenger_count',
      'trip_distance': 'trip_distance',
      'RatecodeID': 'ratecode_id',
      'store_and_fwd_flag': 'store_and_fwd_flag',
      'PULocationID': 'pickup_location_id',
      'DOLocationID': 'dropoff_location_id',
      'payment_type': 'payment_type_name',
      'fare_amount': 'fare_amount',
      'extra': 'extra',
      'mta_tax': 'mta_tax',
      'tip_amount': 'tip_amount',
      'tolls_amount': 'tolls_amount',
      'improvement_surcharge': 'improvement_surcharge',
      'total_amount': 'total_amount',
      'congestion_surcharge': 'congestion_surcharge',
      'airport_fee': 'airport_fee'
    }
}


# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
# NOTE: This is intended to return a dataframe (pandas?) so I need to understand how to do that.
def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])

    # Generate list of months between start and end dates
    # Fetch parquet files from:
    # https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month}.parquet
    # To make this as simple as possible, I shall simply assume that the starting and ending dates 
    # are going to be in "yyyy-mm" format to make parsing-out of url parts as simpler.  Generalized
    # date-string parsing will have to come later.
    # Also, this attempts to read all of the yellow taxi data into gigantic
    # data frame, which may become a problem, but I will try this dirt-simple 
    # approach first.
    dfs = []
    big_df = None
    # taxi_type = 'yellow'
    for taxi_type in taxi_types :
        for (yr, mth) in list_of_months(start_date, end_date):
            big_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{yr}-{mth:02}.parquet'
            print(f'Now trying:  {big_url}')
            df = pd.read_parquet(big_url, engine='pyarrow') 
                                 # https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
            # THIS IS PART OF THE MAPPING SOLUTION:
            df.rename(columns = column_mappings[taxi_type], inplace=True)
            # WE DO NOT, YET HAVE A MAPPING FOR COLUMN NAMED taxi_type!!!
            # I do not yet understand how to map columns which aren't 
            # present in the original DataFrame into the target table.
            dfs.append(df)
    big_df = pd.concat(dfs, ignore_index=True)

    return big_df