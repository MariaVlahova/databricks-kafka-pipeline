# tests/test_schema_utils.py

from pyspark.sql import SparkSession
from pipelines.schema_utils import safe_detect_type
from pyspark.sql.types import TimestampType, IntegerType, DoubleType


def test_safe_detect_type_timestamp():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    df = spark.createDataFrame([("2024-01-01 12:13:14",)], ["ts"])
    dtype = safe_detect_type(df, "ts")
    assert isinstance(dtype, TimestampType)


def test_safe_detect_type_int():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    df = spark.createDataFrame([("123",), ("456",)], ["num"])
    dtype = safe_detect_type(df, "num")
    assert isinstance(dtype, DoubleType)

