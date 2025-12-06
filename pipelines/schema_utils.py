# pipelines/schema_utils.py

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, regexp_replace
from pyspark.sql.types import (
    DataType, StringType, TimestampType, DateType,
    DoubleType, IntegerType, StructField, StructType
)


def safe_detect_type(sample_df: DataFrame, col_name: str) -> DataType:
    """
    Decide a single Spark DataType for an entire column based on sample data.
    Priority: TIMESTAMP > DATE > DOUBLE > INT > STRING.
    Uses try_cast so invalid values do NOT throw errors.
    """
    non_null = sample_df.filter(col(col_name).isNotNull())

    if non_null.limit(1).count() == 0:
        return StringType()

    # TIMESTAMP
    ts_test = non_null.select(expr(f"try_cast({col_name} AS timestamp) AS v"))
    if ts_test.filter(col("v").isNull()).count() == 0:
        return TimestampType()

    # DATE
    date_test = non_null.select(expr(f"try_cast({col_name} AS date) AS v"))
    if date_test.filter(col("v").isNull()).count() == 0:
        return DateType()

    # DOUBLE
    dbl_test = non_null.select(
        expr(f"try_cast(regexp_replace({col_name}, ',', '.') AS double) AS v")
    )
    if dbl_test.filter(col("v").isNull()).count() == 0:
        return DoubleType()

    # INT
    int_test = non_null.select(expr(f"try_cast({col_name} AS int) AS v"))
    if int_test.filter(col("v").isNull()).count() == 0:
        return IntegerType()

    return StringType()


def build_dynamic_schema_from_sample(df: DataFrame) -> StructType:
    """
    Build a StructType by inspecting all columns of a sample DF.
    """
    fields = []
    for c in df.columns:
        dtype = safe_detect_type(df, c)
        fields.append(StructField(c, dtype, True))
    return StructType(fields)


def merge_schemas(existing: StructType, new: StructType) -> StructType:
    """
    Merge two schemas:
      - add new columns
      - if type conflicts, promote to STRING
    """
    field_map = {f.name: f for f in existing.fields}

    for nf in new.fields:
        if nf.name in field_map:
            ef = field_map[nf.name]
            if ef.dataType != nf.dataType:
                field_map[nf.name] = StructField(nf.name, StringType(), True)
        else:
            field_map[nf.name] = nf

    return StructType(list(field_map.values()))


def ensure_schema_for_topic(
    spark,
    dbutils,
    topic_path: str,
    schema_table: str
) -> StructType:
    """
    1) Read a sample batch from topic path
    2) Infer dynamic schema
    3) Merge with existing if schema table exists
    4) Save as empty table to Unity Catalog
    """
    sample_batch = dbutils.fs.ls(topic_path)[0].path
    sample_df = spark.read.json(sample_batch)

    new_schema = build_dynamic_schema_from_sample(sample_df)

    if spark.catalog.tableExists(schema_table):
        existing_schema = spark.table(schema_table).schema
        final_schema = merge_schemas(existing_schema, new_schema)
    else:
        final_schema = new_schema

    spark.createDataFrame([], final_schema) \
        .write.mode("overwrite") \
        .saveAsTable(schema_table)

    print(f"[SCHEMA] Updated schema table: {schema_table}")
    return final_schema
