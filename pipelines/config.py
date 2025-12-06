# pipelines/config.py

CATALOG = "workspace"
SCHEMA = "default"

# "Kafka-like" topic folders (written by your fake / real producer)
CUSTOMER_TOPIC_PATH = "/Volumes/workspace/default/kafka_sim/customers"
ORDER_TOPIC_PATH    = "/Volumes/workspace/default/kafka_sim/orders"
SALES_TOPIC_PATH    = "/Volumes/workspace/default/kafka_sim/sales"

# Checkpoints for streaming
CUSTOMER_CHECKPOINT = "/Volumes/workspace/default/kafka_sim/checkpoints/customers"
ORDER_CHECKPOINT    = "/Volumes/workspace/default/kafka_sim/checkpoints/orders"
SALES_CHECKPOINT    = "/Volumes/workspace/default/kafka_sim/checkpoints/sales"

# Bronze Delta locations (in your datalake volume)
CUSTOMER_BRONZE_PATH = "/Volumes/workspace/default/datalake/customers_bronze"
ORDER_BRONZE_PATH    = "/Volumes/workspace/default/datalake/orders_bronze"
SALES_BRONZE_PATH    = "/Volumes/workspace/default/datalake/sales_bronze"

# Schema registry tables (Unity Catalog, no hive_metastore)
CUSTOMER_SCHEMA_TABLE = f"{CATALOG}.{SCHEMA}.customers_schema"
ORDER_SCHEMA_TABLE    = f"{CATALOG}.{SCHEMA}.orders_schema"
SALES_SCHEMA_TABLE    = f"{CATALOG}.{SCHEMA}.sales_schema"

# Silver tables (typed, queryable)
CUSTOMER_SILVER_TABLE = f"{CATALOG}.{SCHEMA}.customers_silver"
ORDER_SILVER_TABLE    = f"{CATALOG}.{SCHEMA}.orders_silver"
SALES_SILVER_TABLE    = f"{CATALOG}.{SCHEMA}.sales_silver"

# Gold tables
ORDER_METRICS_GOLD_TABLE    = f"{CATALOG}.{SCHEMA}.order_metrics_gold"
SALES_METRICS_GOLD_TABLE    = f"{CATALOG}.{SCHEMA}.sales_metrics_gold"
COMBINED_METRICS_GOLD_TABLE = f"{CATALOG}.{SCHEMA}.combined_metrics_gold"

# Default loop settings for main()
DEFAULT_LOOP_INTERVAL_SEC = 10
