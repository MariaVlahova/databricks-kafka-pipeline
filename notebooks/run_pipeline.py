# notebooks/run_pipeline.py

# In Databricks notebook

# Install the package from repo root (only needed first time per cluster)
# Adjust the path to your repo location in Databricks Repos.
/*
%pip install -e /Workspace/Repos/<your-user>/<databricks-kafka-pipeline>
*/

from pipelines.run import run_once, run_loop

# For one batch:
run_once(spark, dbutils)

# Or for continuous loop:
# run_loop(spark, dbutils, interval_sec=10)
