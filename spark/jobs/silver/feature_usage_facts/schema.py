"""
Silver feature usage facts output columns:
    user_id (INT)
    feature_id (INT)
    first_used_date (DATE)
    last_used_date (DATE)
    total_usage_count (INT)
    avg_daily_usage (DOUBLE)
    as_of_date (DATE) - Snapshot date
"""

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StructField,
    StructType,
)

FEATURE_USAGE_FACTS_COLUMNS = [
    "user_id",
    "feature_id",
    "first_used_date",
    "last_used_date",
    "total_usage_count",
    "avg_daily_usage",
    "as_of_date",
]

FEATURE_USAGE_FACTS_SCHEMA = StructType(
    [
        StructField("user_id", IntegerType(), True),
        StructField("feature_id", IntegerType(), True),
        StructField("first_used_date", DateType(), True),
        StructField("last_used_date", DateType(), True),
        StructField("total_usage_count", LongType(), True),
        StructField("avg_daily_usage", DoubleType(), True),
        StructField("as_of_date", DateType(), True),
    ]
)
