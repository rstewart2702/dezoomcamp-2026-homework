# Repository for Data Engineering Zoomcamp

## Homework 5

### Question 1. Bruin Pipeline Structure

In a Bruin project, what are the required files/directories?

- `bruin.yml` and `assets/`
- `.bruin.yml` and `pipeline.yml` (assets can be anywhere)
- `.bruin.yml` and `pipeline/` with `pipeline.yml` and `assets/`
- `pipeline.yml` and `assets/` only

ANSWER:

- `.bruin.yml` and `pipeline.yml` (assets can be anywhere)

NOTES
I tried to create an empty bruin project with

```
bruin init default .
```

and from that I saw that there was a `pipeline.yml` in the folder
where the command was run.  Also, the `.bruin.yml` in the root
folder/directory of the git repository was touched.  This file
was first created by work done earlier with other bruin exercises.

---

### Question 2. Materialization Strategies

You're building a pipeline that processes NYC taxi data organized by month based on `pickup_datetime`. Which incremental strategy is best for processing a specific interval period by deleting and inserting data for that time period?

- `append` - always add new rows
- `replace` - truncate and rebuild entirely
- `time_interval` - incremental based on a time column
- `view` - create a virtual table only

ANSWER:

- `time_interval` - incremental based on a time column

NOTES

Based this upon reading through the Bruin documentation, for example, in:
["Materialization Strategy"](https://getbruin.com/docs/bruin/assets/materialization.html#materialization-strategy)

---

### Question 3. Pipeline Variables

You have the following variable defined in `pipeline.yml`:

```yaml
variables:
  taxi_types:
    type: array
    items:
      type: string
    default: ["yellow", "green"]
```

How do you override this when running the pipeline to only process yellow taxis?

- `bruin run --taxi-types yellow`
- `bruin run --var taxi_types=yellow`
- `bruin run --var 'taxi_types=["yellow"]'`
- `bruin run --set taxi_types=["yellow"]`

ANSWER:

- `bruin run --var 'taxi_types=["yellow"]'`

NOTES:
Typically, one must quote such commmand-line arguments to prevent
the command-shell from trying to "interpret" or otherwise change
strings like:

```
taxi_types=["yellow"]
```

and that command-line argument is enclosed in "single quote marks,"
i.e., the `'` character.  So, we go from `taxi_types = ["yellow", "green"]` to
`taxi_types = ["yellow"]`.

---

### Question 4. Running with Dependencies

You've modified the `ingestion/trips.py` asset and want to run it plus all downstream assets. Which command should you use?

- `bruin run ingestion.trips --all`
- `bruin run ingestion/trips.py --downstream`
- `bruin run pipeline/trips.py --recursive`
- `bruin run --select ingestion.trips+`

ANSWER:

- `bruin run ingestion/trips.py --downstream`

NOTES:
One way to see this is simply to run it...

---

### Question 5. Quality Checks

You want to ensure the `pickup_datetime` column in your trips table never has NULL values. Which quality check should you add to your asset definition?

- `name: unique`
- `name: not_null`
- `name: positive`
- `name: accepted_values, value: [not_null]`

ANSWER:

- `name: not_null`

NOTES:
See [](https://getbruin.com/docs/bruin/quality/available_checks.html#not-null)


---

### Question 6. Lineage and Dependencies

After building your pipeline, you want to visualize the dependency graph between assets. Which Bruin command should you use?

- `bruin graph`
- `bruin dependencies`
- `bruin lineage`
- `bruin show`

---

### Question 7. First-Time Run

You're running a Bruin pipeline for the first time on a new DuckDB database. What flag should you use to ensure tables are created from scratch?

- `--create`
- `--init`
- `--full-refresh`
- `--truncate`

---

## Submitting the solutions

- Form for submitting: <https://courses.datatalks.club/de-zoomcamp-2026/homework/hw5>

=======

## Learning in Public

We encourage everyone to share what they learned. This is called "learning in public".

Read more about the benefits [here](https://alexeyondata.substack.com/p/benefits-of-learning-in-public-and).

### Example post for LinkedIn

```
🚀 Week 5 of Data Engineering Zoomcamp by @DataTalksClub complete!

Just finished Module 5 - Data Platforms with Bruin. Learned how to:

✅ Build end-to-end ELT pipelines with Bruin
✅ Configure environments and connections
✅ Use materialization strategies for incremental processing
✅ Add data quality checks to ensure data integrity
✅ Deploy pipelines from local to cloud (BigQuery)

Modern data platforms in a single CLI tool - no vendor lock-in!

Here's my homework solution: <LINK>

Following along with this amazing free course - who else is learning data engineering?

You can sign up here: https://github.com/DataTalksClub/data-engineering-zoomcamp/
```

### Example post for Twitter/X

```
📊 Module 5 of Data Engineering Zoomcamp done!

- Data Platforms with Bruin
- End-to-end ELT pipelines
- Data quality & lineage
- Deployment to BigQuery

My solution: <LINK>

Free course by @DataTalksClub: https://github.com/DataTalksClub/data-engineering-zoomcamp/
```

===================================================================================================

## Homework 4
### Question 1. dbt Lineage and Execution

Given a dbt project with the following structure:

```
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```

if you run `dbt run --select int_trips_unioned`, what models will be built?

* `stg_green_tripdata`, `stg_yellow_tripdata`, and `int_trips_unioned` (upstream dependencies)
* Any model with upstream and downstream dependencies to `int_trips_unioned`
* `int_trips_unioned` only
* `int_trips_unioned`, `int_trips`, `fct_trips` (downstream dependencies)

ANSWER:
* `int_trips_unioned` only

Elaboration:

There are other notations that can be used with the `dbt run --select`:

There are lots of variations and "graph operators" which allow one to
ask `dbt` to build all of the "downstream descendants," for example.

In order to ask for the construction/derivation of `int_trips_unioned` and its
upstream dependencies, `stg_green_tripdata` and `stg_yellow_tripdata`, one would use the command:

```
dbt run --select +int_trips_unioned
```

Also, the `dbt ls --select +int_trips_unioned` would show you what would be "run" without
running it; `dbt ls` lets you see what the "graph selection syntax" allows you to
specify.

References:

https://docs.getdbt.com/reference/node-selection/syntax

https://docs.getdbt.com/reference/commands/run

https://docs.getdbt.com/reference/commands/list


=============================================================

### Question 2. dbt Tests

You've configured a generic test like this in your `schema.yml` :

```
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```

Your model `fct_trips` has been running successfully for months.  A new value `6` now appears in the source data.

What happens when you run `dbt --test --select fct_trips`?


* dbt will skip the test because the model didn't change
* dbt will fail the test, returning a non-zero exit code
* dbt will pass the test with a warning about the new value
* dbt will update the configuration to include the new value

ANSWER;
* dbt will fail the test, returning a non-zero exit code

Elaboration:

I tried to explore this behavior by tweaking the `taxi_rides-ny/models/marts/schema.yml`
by adding `data_tests` to the column definition for `payment_type` and adding
the `accepted_values` tests in the manner described above:

```
      - name: payment_type
        description: Payment method code
        data_type: integer
        data_tests:
        - accepted_values:
            arguments:
              values: [1, 2, 3, 4]
              quote: false
```

and then ran a `dbt build --select fct_trips --full-refresh` to get `dbt` to
show me what an error condition and test failure looks like when it finds
values of 5 in column `payment_type`.


=============================================================

### Question 3. Counting Records in `fct_monthly_zone_revenue`

After running your dbt project, query the `fct_monthly_zone_revenue` model.

What is the count of records in the `fct_monthly_zone_revenue` model?

```
    12,998
    14,120
    12,184
    15,421
```

ANSWER:

```
    12,184
```

Elaboration:

```
select count(*) row_tally from `dbt_rstewart.fct_monthly_zone_revenue`;
```

yielded the correct result, *after* executing `dbt build --target -prod --full-refresh`
to derived correct population of data, after I discovered that I had typos in the
uri's used to query the "data lake csv.gz files" that had been loaded into cloud
storage!

=============================================================

### Question 4. Best Performing Zone for Green Taxis (2020)

Using the `fct_monthly_zone_revenue` table, find the pickup zone with the highest total revenue (`revenue_monthly_total_amount`) for *Green* taxi trips in 2020.

Which zone had the highest revenue?

    * East Harlem North
    * Morningside Heights
    * East Harlem South
    * Washington Heights South

ANSWER:
* East Harlem North


Elaboration:

The following query yielded the answer:

```
select t.pickup_zone, sum(t.revenue_monthly_total_amount) tot_revenue, t.service_type  
from `dbt_rstewart.fct_monthly_zone_revenue` t 
where 
    t.service_type = 'Green' 
and 
    '2020-01-01' <= t.revenue_month and t.revenue_month <= '2020-12-01'
group by t.pickup_zone, t.service_type
order by sum(t.revenue_monthly_total_amount) desc;
```

=============================================================

### Question 5. Green Taxi Trip Counts (October 2019)

Using the fct_monthly_zone_revenue table, what is the total number of trips (total_monthly_trips) for Green taxis in October 2019?

```
    500,234
    350,891
    384,624
    421,509
```

ANSWER:

```
    384,624
```

Elaboration:

```
select sum(t.total_monthly_trips) sum_tot_monthly_trips
from `dbt_rstewart.fct_monthly_zone_revenue` t where t.service_type='Green' and t.revenue_month = '2019-10-01';
```

=============================================================

### Question 6. Build a Staging Model for FHV Data

Create a staging model for the *For-Hire Vehicle (FHV)* trip data for 2019.

  1.  Load the [FHV trip data](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) for 2019 into your data warehouse
  2.  Create a staging model `stg_fhv_tripdata` with these requirements:
      * Filter out records where `dispatching_base_num` IS NULL
      * Rename fields to match your project's naming conventions (e.g., `PUlocationID` → `pickup_location_id`)

What is the count of records in stg_fhv_tripdata?

  * 42,084,899
  * 43,244,693
  * 22,998,722
  * 44,112,187

ANSWER:
  * 43,244,693



Elaboration:
I shall have to gin up a python loading program just to set up the GCP
data for that "fhv" stuff; DONE 2026-02-16 Mon 10:27

I will need to define an "external table" for the fhv data; DONE 2026-02-16 Mon 10:29

What actually worked out:
(after asking Gemini for a little help!)
The SQL comes first, then the yml files which document/elucidate the "schema" and
"sources" can be automatically generated, given the right "dbt plugin," and
It looks like those are automatically added when using dbt cloud, it seems.

And:  it seems to be more complicated than that, for dbt stopped to complain
with:

```
Compilation Error
  Model 'model.taxi_rides_ny.stg_fhv_tripdata' (models/staging/stg_fhv_tripdata.sql) depends on a source named 'raw.fhv_tripdata' which was not found
```

So, I had to add some text to the `taxi_rides_ny/models/staging/sources.yml` file.
That seems to be required, at least in the case of this project's definition.

So, I created a taxi_rides_ny/models/staging/stg_fhv_tripdata.sql file
and ran

```
dbt run --select stg_fhv_tripdata
```

And after correcting a typo in my SQL, the command above created a
view which queries the "data lake o'csv files" which are reached
via the name `dbt_rstewart.fhv_tripdata`.

#### Further Notes

It turns out that some parts can be generated automatically, with commands
like the following?

```
dbt run-operation generate_model_yaml --args '{ "model_names":  ["stg_fhv_tripdata"] }'
```

So I tried it, and it spat out the following to the logged output in the `dbt platform`:

```
models:
  - name: stg_fhv_tripdata
    description: ""
    columns:
      - name: dispatching_base_num
        data_type: string
        description: ""

      - name: pickup_datetime
        data_type: timestamp
        description: ""

      - name: dropoff_datetime
        data_type: timestamp
        description: ""

      - name: pickup_location_id
        data_type: int64
        description: ""

      - name: dropoff_location_id
        data_type: int64
        description: ""

      - name: sr_flag
        data_type: int64
        description: ""

      - name: affiliated_base_number
        data_type: string
        description: ""
```

So, this is a start on what could be pasted into the `taxi_rides_ny/models/staging/schema.yml`.

And, the "studio" in `dbt cloud` or `dbt platform` leaves a lot to be desired as far as
interactions with Git are concerned:  I guess you'd better "restart" studio after making
changes to files in your project which have *not* been committed yet?  But it's really
hard to tell, and I hope their documentation talks about it.  But they've clearly tried
to emulate the VSCode experience, and this has caused me lots of confusion because
it looked like I had lost work when I really had not, all because the editor in the "studio"
got out of sync with the "git repository and local working-copy" reality.  This has not
been a good developer experience.



## Homework 3
### Loading Data
Python to load into my GCP Big Query tables is [here](03-data-warehouse/load_yellow_taxi_data.py).


### Homework and the Queries Used
Question 1. Counting records

What is count of records for the 2024 Yellow Taxi Data?

    65,623
    840,402
    20,332,093
    85,431,289

ANSWER:

    20,332,093

WORK:
```
select count(*) from `datatalks-dezoomcamp2026.datatalks_dezoomcamp2026_dataset.fhv_tripdata`;
```

returned a count of:

20332093

=========================================================================

Question 2. Data read estimation

Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.

What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

    18.82 MB for the External Table and 47.60 MB for the Materialized Table
    0 MB for the External Table and 155.12 MB for the Materialized Table
    2.14 GB for the External Table and 0MB for the Materialized Table
    0 MB for the External Table and 0MB for the Materialized Table

ANSWER:

    0 MB for the External Table and 155.12 MB for the Materialized Table

WORK:

Concluded this by highlighting the SQL statement in the web-page editor
and looking for a "green, circular checkmark icon" and message to appear
below the query-editor pane; the message for the query of the non-partitioned
"regular bigquery table" named "fhv_nonpartitioned_tripdata" was:

  `This query will process 155.12 MB when run.`

=========================================================================

Question 3. Understanding columnar storage

Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table.

Why are the estimated number of Bytes different?

    BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.
    BigQuery duplicates data across multiple storage partitions, so selecting two columns instead of one requires scanning the table twice, doubling the estimated bytes processed.
    BigQuery automatically caches the first queried column, so adding a second column increases processing time but does not affect the estimated bytes scanned.
    When selecting multiple columns, BigQuery performs an implicit join operation between them, increasing the estimated bytes processed

ANSWER:

    BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

WORK:

I shall verify by looking at the estimates published for both of the requested queries, but
the answer is that the query which mentions two separate columns scans more data, because
the columns are stored separately; this implies that the more columns you access in a columnar
database, the more data you can end up scanning, ceteris paribus.

Here are the two queries with the "data read estimates" Google's service provided:

```
select t.PULocationID from `datatalks-dezoomcamp2026.datatalks_dezoomcamp2026_dataset.fhv_nonpartitioned_tripdata` t;
```

Provided estimate was:

`This query will process 155.12 MB when run.`

```
select t.PULocationID, t.DOLocationID from `datatalks-dezoomcamp2026.datatalks_dezoomcamp2026_dataset.fhv_nonpartitioned_tripdata` t;
```

Provided estimate was:

`This query will process 310.24 MB when run.`

=========================================================================

Question 4. Counting zero fare trips

How many records have a fare_amount of 0?

    128,210
    546,578
    20,188,016
    8,333

ANSWER:

    8,333

WORK:
```
select count(*) from `datatalks-dezoomcamp2026.datatalks_dezoomcamp2026_dataset.fhv_nonpartitioned_tripdata` t where t.fare_amount = 0;
```

returned the count of 8333.

=========================================================================

Question 5. Partitioning and clustering

What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

    Partition by tpep_dropoff_datetime and Cluster on VendorID
    Cluster on by tpep_dropoff_datetime and Cluster on VendorID
    Cluster on tpep_dropoff_datetime Partition by VendorID
    Partition by tpep_dropoff_datetime and Partition by VendorID

ANSWER:

    Partition by tpep_dropoff_datetime and Cluster on VendorID

WORK:

My first inclination was to answer:
    Cluster on by tpep_dropoff_datetime and Cluster on VendorID

But then the exercise showed that one literlly may not partition on a
timestamp valued column; on must compute a function of it, such as
extracting the date-portion of it.  When doing that, then the table
becomes, in this case, 192 "mini-tables," each of which is pre-sorted
on the VendorID (probably because the rows literally are ordered on that
column, or there is a b-tree index for the mini-table on that value,
or else the entire mini-table structure IS organized in a btree structure!)

So, the better, possible-in-Big-Query answer is:

    Partition by tpep_dropoff_datetime and Cluster on VendorID

TABLE-CREATION STATEMENTS:
```
create table `datatalks-dezoomcamp2026.datatalks_dezoomcamp2026_dataset.fhv_clustered_tripdata`
cluster by tpep_dropoff_datetime, VendorID
as select * from `datatalks-dezoomcamp2026.datatalks_dezoomcamp2026_dataset.fhv_tripdata` ;
```

```
create table `datatalks-dezoomcamp2026.datatalks_dezoomcamp2026_dataset.fhv_partitioned_tripdata`
partition by date(tpep_dropoff_datetime)
cluster by tpep_dropoff_datetime, VendorID
as select * from `datatalks-dezoomcamp2026.datatalks_dezoomcamp2026_dataset.fhv_tripdata` ;
```

```
select t.*
from `datatalks-dezoomcamp2026.datatalks_dezoomcamp2026_dataset.fhv_partitioned_tripdata` t
where timestamp("2024-06-01")  <= t.tpep_dropoff_datetime and t.tpep_dropoff_datetime < timestamp("2024-06-02")
order by t.VendorID;
-- estimated 17.66 MB traversed 
```

```
select t.*
from `datatalks-dezoomcamp2026.datatalks_dezoomcamp2026_dataset.fhv_partitioned_tripdata` t
where timestamp('2024-06-01') = date_trunc(t.tpep_dropoff_datetime,DAY)
order by t.VendorID;
-- estimated 17.66 MB traversed 
```

```
select t.*
from `datatalks-dezoomcamp2026.datatalks_dezoomcamp2026_dataset.fhv_clustered_tripdata` t
where timestamp("2024-06-01")  <= t.tpep_dropoff_datetime and t.tpep_dropoff_datetime < timestamp("2024-06-02")
order by t.VendorID;
-- estimated 103.51 MB traversed

/* this means that partitioning can SIGNIFICANTLY reduce the amount of data traversed,
   when the partitioning is possible. */
```


I queried for the cardinality of the tpsp_dropoff_datetime column, and discovered
that it has almost 1e+11 (that is 10 MILLION) distinct values.  This number is far
higher than the maximum number of permitted partitions allowed for a Big Query
partitioned table.  Therefore, the best one could do would be to cluster first on
tpep_dropoff_datetime, ensuring that filtering operations will be cheap, and also
to cluster on VendorID, which will make it cheap to impose ordering by VendorID on
the result set of rows retrieved.

Any other choice in the list of choices will either lead to a failure to partition
(because there are far more possible values than the number of allowed partitions)
or will lead to more i/o than the choice chosen for the answer, either because
the partitioning will not be useful (due to the stated filtering and ordering
contraint...)

What they did NOT make clear is whether or not we want to partition on the
date portion of the tpep_dropoff_datetime.  If we do that, then there are only 192
distinct days in question, so this makes it much less clear what to do.  That said,

If we are trying to filter on a particular DAY (ignoring the timestamp portion) then
it makes sense to partition by the DATE(tpep_dropoff_datetime) and then cluster
based on VendorID, because the filtering can be done by the paritioning VERY CHEAPLY,
and the clustering ensures that the data will already be sorted.

On the other hand, if we are filtering on the entirety of the tpep_dropoff_datetime,
then partitioning on tpep_dropoff_datetime is impossible, and the best we could
do would be to cluster based on tpep_dropoff_datetime, VendorID.

=========================================================================

Question 6. Partition benefits

Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?

Choose the answer which most closely matches.

    12.47 MB for non-partitioned table and 326.42 MB for the partitioned table
    310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
    5.87 MB for non-partitioned table and 0 MB for the partitioned table
    310.31 MB for non-partitioned table and 285.64 MB for the partitioned table

ANSWER:

    310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

WORK:
```
select distinct(t.VendorId) distinct_vendorid
from `datatalks-dezoomcamp2026.datatalks_dezoomcamp2026_dataset.fhv_nonpartitioned_tripdata` t
where timestamp('2024-03-01') <= t.tpep_dropoff_datetime and t.tpep_dropoff_datetime < timestamp('2024-03-16')
-- esimtated 310.24 MB to be processed/traversed.
```

```
select distinct(t.VendorId) distinct_vendorid
from `datatalks-dezoomcamp2026.datatalks_dezoomcamp2026_dataset.fhv_partitioned_tripdata` t
where timestamp('2024-03-01') <= t.tpep_dropoff_datetime and t.tpep_dropoff_datetime < timestamp('2024-03-16')
-- estimated 26.84 MB to be processed/traversed.
```

=========================================================================

Question 7. External table storage

Where is the data stored in the External Table you created?

    Big Query
    Container Registry
    GCP Bucket
    Big Table

ANSWER:

    GCP Bucket

=========================================================================

Question 8. Clustering best practices

It is best practice in Big Query to always cluster your data:

    True
    False

ANSWER:

    False

WORK:

Clustering can impose overhead, extra processing work-and-cost; it depends on the
kind of querying that will be done, i.e., the "workload" that will be needed.

=========================================================================

Question 9. Understanding table scans

No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

ANSWER:

    Big Query's estimate is "0 B when run."

My initial guess was that was likely because some of this information is already cached.
    
When I looked at the execution-plan-graph, it was pretty clear that, due to the way Big Query
works, record tallies of that kind are stored in readily-available locations like system-catalogs
or information-schemata, because something like Big Query is a "read-mostly" or "read-only"
environment, and in such an environment, the total number of rows in a table is something that is
statically known ahead of all queries.

=========================================================================

## Homework #2
This solution shows how to use a ForEach task, with a Subflow, to ingest the first
seven months of data for each of the green and yellow taxi lines.
This uses the already-provided 04_postgres_taxi flow.

```
id: hw02_drive_taxi_ingestion
namespace: zoomcamp

concurrency:
  limit: 1

tasks:
  - id: hello
    type: io.kestra.plugin.core.log.Log
    message: This is a driver flow to ingest multiple Taxi data files for 2021, for both green and yellow taxi services.
  
  - id: iterative_ingestion_green
    type: io.kestra.plugin.core.flow.ForEach
    values: [ "01", "02", "03", "04", "05", "06", "07" ]
    concurrencyLimit: 1
    tasks:
      - id: call_subflow_green
        type: io.kestra.plugin.core.flow.Subflow
        flowId: 04_postgres_taxi
        namespace: zoomcamp
        inputs: 
          month: "{{ taskrun.value }}"
          year: "2021"
          taxi: "green"


  - id: iterative_ingestion_yellow
    type: io.kestra.plugin.core.flow.ForEach
    values: [ "01", "02", "03", "04", "05", "06", "07" ]
    concurrencyLimit: 1
    tasks:
      - id: call_subflow_yellow
        type: io.kestra.plugin.core.flow.Subflow
        flowId: 04_postgres_taxi
        namespace: zoomcamp
        inputs: 
          month: "{{ taskrun.value }}"
          year: "2021"
          taxi: "yellow"
```



## Homework #1
Notes about this are in file homework-answers.txt.

## Other Notes
As of 2026-02-02 Mon, I have switched to WSL2.


Not a terribly neat repository yet, but should get better 
as the course progresses.

N.B. Used Windows for homework #1, and that has its own challenges.
May switch to WSL2 for everything else going forward, but that 
means copying keys and such into the WSL setup, and re-working
some setup inside WSL.

