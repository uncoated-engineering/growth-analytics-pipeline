from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

FEATURE_USAGE_EVENTS_SCHEMA = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("feature_id", IntegerType(), True),
        StructField("feature_name", StringType(), True),
        StructField("event_type", StringType(), True),
    ]
)

# Schema after ingestion (timestamp converted, event_id/event_date added)
FEATURE_USAGE_EVENTS_OUTPUT_SCHEMA = StructType(
    [
        StructField("event_id", LongType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("feature_id", IntegerType(), True),
        StructField("feature_name", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_timestamp", TimestampType(), True),
        StructField("event_date", DateType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ]
)
