# pipelines/gold.py

from pyspark.sql.functions import (
    sum as _sum, count, when, to_date, weekofyear,
    current_timestamp, lag, col
)
from pyspark.sql import Window


def run_gold_batch(
    spark,
    customer_silver_table: str,
    order_silver_table: str,
    sales_silver_table: str,
    order_metrics_gold_table: str,
    sales_metrics_gold_table: str,
    combined_metrics_gold_table: str,
):
    """
    Build Gold metrics from one batch of Silver data and write to Delta tables.
    """

    print("[GOLD] Building GOLD metrics from this ONE batch...")

    customers_silver = spark.table(customer_silver_table)
    orders_silver    = spark.table(order_silver_table)
    sales_silver     = spark.table(sales_silver_table)

    # -----------------------------
    # ORDER METRICS
    # -----------------------------
    order_metrics_batch = (
        orders_silver
            .groupBy("order_id", "customer_id")
            .agg(
                _sum("amount").alias("total_amount_per_order"),
                count("*").alias("order_row_count"),
                _sum(when(col("status") == "cancelled", 1).otherwise(0))
                    .alias("cancelled_rows")
            )
            .withColumn(
                "cancellation_rate",
                col("cancelled_rows") / col("order_row_count")
            )
    )

    order_metrics_batch.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(order_metrics_gold_table)

    print(f"[GOLD] Created: {order_metrics_gold_table}")

    # -----------------------------
    # SALES METRICS
    # -----------------------------
    sales_silver_batch = sales_silver.withColumn(
        "sale_date", to_date("sale_timestamp")
    )

    sales_metrics_batch = (
        sales_silver_batch
            .groupBy("sale_date", "product_id", "customer_id")
            .agg(
                _sum("revenue").alias("total_revenue"),
                _sum(
                    when(col("cost").isNotNull(), col("revenue") - col("cost"))
                    .otherwise(None)
                ).alias("gross_margin")
            )
    )

    sales_metrics_batch.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(sales_metrics_gold_table)

    print(f"[GOLD] Created: {sales_metrics_gold_table}")

    # -----------------------------
    # COMBINED METRICS
    # -----------------------------

    customers_silver.createOrReplaceTempView("customers_batch_view")
    orders_silver.createOrReplaceTempView("orders_batch_view")
    sales_silver_batch.createOrReplaceTempView("sales_batch_view")

    combined_batch = spark.sql("""
        SELECT
            c.customer_id,
            o.order_id,
            to_date(s.sale_timestamp) AS sale_date,
            weekofyear(s.sale_timestamp) AS week_number,
            s.revenue,
            s.cost
        FROM customers_batch_view c
        LEFT JOIN orders_batch_view o USING (customer_id)
        LEFT JOIN sales_batch_view s USING (order_id)
    """)

    # daily revenue
    daily_revenue = (
        combined_batch
            .groupBy("sale_date")
            .agg(_sum("revenue").alias("daily_revenue"))
    )

    # weekly revenue + growth
    weekly_revenue = (
        combined_batch
            .groupBy("week_number")
            .agg(_sum("revenue").alias("weekly_revenue"))
    )

    w = Window.orderBy("week_number")
    weekly_growth = (
        weekly_revenue
            .withColumn("previous_week", lag("weekly_revenue").over(w))
            .withColumn(
                "weekly_growth",
                (col("weekly_revenue") - col("previous_week"))
                / col("previous_week")
            )
    )

    # customer profitability
    customer_profitability = (
        combined_batch
            .groupBy("customer_id")
            .agg(
                (_sum("revenue") - _sum("cost")).alias("customer_profitability")
            )
    )

    combined_metrics_batch = (
        daily_revenue
            .join(weekly_growth, "week_number", "left")
            .join(customer_profitability, "customer_id", "left")
            .withColumn("batch_ts", current_timestamp())
    )

    combined_metrics_batch.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(combined_metrics_gold_table)

    print(f"[GOLD] Created: {combined_metrics_gold_table}")
    print("[GOLD] Done.")
