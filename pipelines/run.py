# pipelines/run.py

import time
import argparse

from . import config
from .schema_utils import ensure_schema_for_topic
from .bronze import ensure_clean_delta_path, build_stream_readers, run_bronze_ingest
from .silver import run_silver_batch
from .gold import run_gold_batch


def run_once(spark, dbutils):
    """
    Run a single end-to-end batch:
      schema → bronze → silver → gold
      RUn once per batch
    """
    print("=== PIPELINE: START===")


    # 1) Ensure schemas
    customer_schema = ensure_schema_for_topic(
        spark, dbutils,
        config.CUSTOMER_TOPIC_PATH,
        config.CUSTOMER_SCHEMA_TABLE
    )
    order_schema = ensure_schema_for_topic(
        spark, dbutils,
        config.ORDER_TOPIC_PATH,
        config.ORDER_SCHEMA_TABLE
    )
    sales_schema = ensure_schema_for_topic(
        spark, dbutils,
        config.SALES_TOPIC_PATH,
        config.SALES_SCHEMA_TABLE
    )

    # 2) Stream readers
    customer_stream, order_stream, sales_stream = build_stream_readers(
        spark,
        customer_schema, order_schema, sales_schema,
        config.CUSTOMER_TOPIC_PATH,
        config.ORDER_TOPIC_PATH,
        config.SALES_TOPIC_PATH
    )

    # 3) Ensure bronze paths are valid Delta
    ensure_clean_delta_path(spark, dbutils, config.CUSTOMER_BRONZE_PATH)
    ensure_clean_delta_path(spark, dbutils, config.ORDER_BRONZE_PATH)
    ensure_clean_delta_path(spark, dbutils, config.SALES_BRONZE_PATH)

    # 4) Run bronze batch
    run_bronze_ingest(customer_stream, config.CUSTOMER_CHECKPOINT, config.CUSTOMER_BRONZE_PATH)
    run_bronze_ingest(order_stream,    config.ORDER_CHECKPOINT,    config.ORDER_BRONZE_PATH)
    run_bronze_ingest(sales_stream,    config.SALES_CHECKPOINT,    config.SALES_BRONZE_PATH)

    # 5) Run Silver batch
    run_silver_batch(
        spark,
        config.CUSTOMER_BRONZE_PATH,
        config.ORDER_BRONZE_PATH,
        config.SALES_BRONZE_PATH,
        config.CUSTOMER_SCHEMA_TABLE,
        config.ORDER_SCHEMA_TABLE,
        config.SALES_SCHEMA_TABLE,
        config.CUSTOMER_SILVER_TABLE,
        config.ORDER_SILVER_TABLE,
        config.SALES_SILVER_TABLE,
    )

    # 6) Run Gold metrics
    run_gold_batch(
        spark,
        config.CUSTOMER_SILVER_TABLE,
        config.ORDER_SILVER_TABLE,
        config.SALES_SILVER_TABLE,
        config.ORDER_METRICS_GOLD_TABLE,
        config.SALES_METRICS_GOLD_TABLE,
        config.COMBINED_METRICS_GOLD_TABLE,
    )

    print("=== PIPELINE: DONE ===")


def run_loop(spark, dbutils, interval_sec: int = config.DEFAULT_LOOP_INTERVAL_SEC):
    """
    Run the pipeline in a loop:
      - each iteration processes one batch
      - sleep interval controls frequency
    """
    print(f"=== PIPELINE: RUN LOOP (interval={interval_sec}s) ===")
    while True:
        run_once(spark, dbutils)
        print(f"[LOOP] Sleeping for {interval_sec} seconds...")
        time.sleep(interval_sec)


def main():
    """
    Entry point for python -m pipelines.run
    NOTE: In Databricks Python jobs, spark & dbutils are pre-injected.
    """
    from pyspark.sql import SparkSession

    # Databricks injects a global `spark`, but we also ensure one is available
    spark = SparkSession.builder.getOrCreate()

    # dbutils is injected in Databricks; here we access it via global
    try:
        dbutils  # type: ignore
    except NameError:
        # In non-Databricks environments, this will fail. That’s fine.
        raise RuntimeError("dbutils is not available. Run this inside Databricks.")

    parser = argparse.ArgumentParser(description="Run Databricks streaming pipeline")
    parser.add_argument("--loop", action="store_true", help="Run in infinite loop mode")
    parser.add_argument(
        "--interval",
        type=int,
        default=config.DEFAULT_LOOP_INTERVAL_SEC,
        help="Loop interval in seconds (only used with --loop)",
    )
    args, _ = parser.parse_known_args()

    if args.loop:
        run_loop(spark, dbutils, args.interval)
    else:
        run_once(spark, dbutils)


if __name__ == "__main__":
    main()
