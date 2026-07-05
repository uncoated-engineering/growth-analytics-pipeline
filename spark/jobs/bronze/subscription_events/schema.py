from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SUBSCRIPTION_EVENTS_SCHEMA = StructType(
    [
        StructField("event_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("event_date", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("plan", StringType(), True),
        StructField("mrr", IntegerType(), True),
        StructField("previous_plan", StringType(), True),
        StructField("previous_mrr", IntegerType(), True),
    ]
)

# Schema after ingestion (ingestion_timestamp added)
SUBSCRIPTION_EVENTS_OUTPUT_SCHEMA = StructType(
    [
        StructField("event_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("event_date", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("plan", StringType(), True),
        StructField("mrr", IntegerType(), True),
        StructField("previous_plan", StringType(), True),
        StructField("previous_mrr", IntegerType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ]
)
