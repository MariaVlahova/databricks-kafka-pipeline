# pipelines/bronze.py

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp


def ensure_clean_delta_path(spark, dbutils, path: str):
    """
    Ensure the given path is either:
      - a valid Delta table, or
      - cleaned up so Delta can initialize it cleanly.
    """
    try:
        spark.read.format("delta").load(path)
        print(f"[BRONZE] Delta table already exists at: {path}")
        return
    except Exception as e:
        msg = str(e)
        if "DELTA_MISSING_TRANSACTION_LOG" in msg or "is not a Delta table" in msg:
            print(f"[BRONZE] Path exists but is NOT Delta → resetting: {path}")
            dbutils.fs.rm(path, recurse=True)
        else:
            print(f"[BRONZE] Path not readable or missing → will initialize: {path}")

    parent = "/".join(path.split("/")[:-1])
    dbutils.fs.mkdirs(parent)
    print(f"[BRONZE] Clean path prepared for Delta: {path}")


def build_stream_readers(
    spark,
    customer_schema,
    order_schema,
    sales_schema,
    customer_topic,
    order_topic,
    sales_topic
):
    """
    Create streaming DataFrames for each topic with an ingest timestamp.
    """
    customer_stream = (
        spark.readStream
            .format("json")
            .schema(customer_schema)
            .load(customer_topic)
            .withColumn("ingest_ts", current_timestamp())
    )

    order_stream = (
        spark.readStream
            .format("json")
            .schema(order_schema)
            .load(order_topic)
            .withColumn("ingest_ts", current_timestamp())
    )

    sales_stream = (
        spark.readStream
            .format("json")
            .schema(sales_schema)
            .load(sales_topic)
            .withColumn("ingest_ts", current_timestamp())
    )

    return customer_stream, order_stream, sales_stream


def run_bronze_ingest(
    df_stream: DataFrame,
    checkpoint: str,
    target_path: str
):
    """
    Process all currently available data in a streaming micro-batch and stop.
    CE and Enterprise both support trigger(once=True) micro-batch mode.
    """
    q = (
        df_stream.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .trigger(once=True)
            .start(target_path)
    )
    q.awaitTermination()
    print(f"[BRONZE] Batch written to {target_path}")
