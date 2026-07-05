from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

MARKETING_ATTRIBUTION_SCHEMA = StructType(
    [
        StructField("user_id", IntegerType(), True),
        StructField("channel", StringType(), True),
        StructField("campaign", StringType(), True),
        StructField("first_touch_date", StringType(), True),
    ]
)

# Schema after ingestion (ingestion_timestamp added)
MARKETING_ATTRIBUTION_OUTPUT_SCHEMA = StructType(
    [
        StructField("user_id", IntegerType(), True),
        StructField("channel", StringType(), True),
        StructField("campaign", StringType(), True),
        StructField("first_touch_date", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ]
)
