# pipelines/silver.py

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, lit


def apply_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """
    Cast columns of df to the provided schema.
      - If a column exists in schema but not in df → add as NULL
      - Extra columns in df are dropped
    """
    cols = []
    for f in schema.fields:
        if f.name in df.columns:
            cols.append(col(f.name).cast(f.dataType).alias(f.name))
        else:
            cols.append(lit(None).cast(f.dataType).alias(f.name))
    return df.select(*cols)


def run_silver_batch(
    spark,
    customer_bronze_path: str,
    order_bronze_path: str,
    sales_bronze_path: str,
    customer_schema_table: str,
    order_schema_table: str,
    sales_schema_table: str,
    customer_silver_table: str,
    order_silver_table: str,
    sales_silver_table: str,
):
    """
    Read Bronze Delta → apply schema from registry → write Silver tables.
    """
    # Load schemas from registry
    customer_schema_hms = spark.table(customer_schema_table).schema
    order_schema_hms    = spark.table(order_schema_table).schema
    sales_schema_hms    = spark.table(sales_schema_table).schema

    # Read bronze
    bronze_customers = spark.read.format("delta").load(customer_bronze_path)
    bronze_orders    = spark.read.format("delta").load(order_bronze_path)
    bronze_sales     = spark.read.format("delta").load(sales_bronze_path)

    customers_silver = apply_schema(bronze_customers, customer_schema_hms)
    orders_silver    = apply_schema(bronze_orders,    order_schema_hms)
    sales_silver     = apply_schema(bronze_sales,     sales_schema_hms)

    customers_silver.write.format("delta").mode("overwrite").saveAsTable(customer_silver_table)
    orders_silver.write.format("delta").mode("overwrite").saveAsTable(order_silver_table)
    sales_silver.write.format("delta").mode("overwrite").saveAsTable(sales_silver_table)

    print("[SILVER] Tables created:")
    print("  ", customer_silver_table)
    print("  ", order_silver_table)
    print("  ", sales_silver_table)
