# Repository for Data Engineering Zoomcamp

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

