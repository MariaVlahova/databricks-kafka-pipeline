# Databricks Kafka-like Streaming Pipeline

This repo contains a modular streaming pipeline for Databricks (Enterprise or CE):

- Dynamic schema inference (no hard-coded schemas)
- Schema registry in Unity Catalog
- Bronze (Delta streaming)
- Silver (typed tables)
- Gold (aggregated metrics)

## Structure

- `pipelines/` — Python package with config, schema utils, bronze/silver/gold, and run orchestrator.
- `notebooks/run_pipeline.py` — Databricks entrypoint.
- `tests/` — basic PySpark tests for schema logic.

## Usage in Databricks

1. Add this repo under **Repos** in Databricks.
2. Open a notebook in `notebooks/`.
3. Install package:

   ```python
   %pip install -e /Workspace/Repos/<your-user>/<databricks-kafka-pipeline>
   
4. Run 
- one batch:

from pipelines.run import run_once
run_once(spark, dbutils)

- continuous loop:

from pipelines.run import run_loop
run_loop(spark, dbutils, interval_sec=10)

5. Create a Databricks Workflow Job that calls this notebook on a schedule.

## Features
✔ Dynamic Schema Inference

The pipeline automatically detects data types using safe try_cast evaluation: Timestamp
,Date ,Double ,Integer ,String. Invalid cast attempts do not break the pipeline.

✔ Schema Evolution - New fields and type conflicts are handled gracefully via:
1.Promotion to string for conflicts
2.Merging of new fields
2.Registry table in Unity Catalog

✔ Streaming Bronze Ingestion - include processing raw JSONs using:

1.Structured Streaming with trigger(once=True)
2.CE-compatible ingestion
3.Delta format with transaction logs ensured

✔ Silver Transformation - transforming raw data to good for analytics vis

1.Strong typing from the schema registry
2.Column alignment
3.Audit fields (e.g. ingest_ts)

✔ Gold Metrics Transformation - Provides aggregated business logic over silver transformation:
We have the listed metroics included:
1.Total order amounts
2.Cancellation rates
3.Revenue & gross margin
4.Daily/weekly KPIs
5.Cusomer profitability

## Deployment on Databricks

1.Add repo under Repos in Databricks.
2.Open notebooks/run_pipeline.py.
3.Install package:

%pip install -e /Workspace/Repos/<user>/<databricks-kafka-pipeline>

4.Run manually or create a Databricks Workflow Job:
Task: notebook or Python script
Cluster: job cluster or all-purpose
Trigger: scheduled or manual
Parameters: --loop, --interval
The same project structure should deploy seamlessly to AWS, Azure, and GCP Databricks.

## Next steps
- add proper logging (structured logs to a Delta table)  
- add data quality checks (fail job on bad data)  
- add schema drift alerts  