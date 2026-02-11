from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CONVERSIONS_SCHEMA = StructType(
    [
        StructField("user_id", IntegerType(), True),
        StructField("conversion_date", StringType(), True),
        StructField("plan", StringType(), True),
        StructField("mrr", IntegerType(), True),
        StructField("signup_date", StringType(), True),
        StructField("days_to_convert", IntegerType(), True),
        StructField("used_real_time_collab", BooleanType(), True),
    ]
)

# Schema after ingestion (ingestion_timestamp added)
CONVERSIONS_OUTPUT_SCHEMA = StructType(
    [
        StructField("user_id", IntegerType(), True),
        StructField("conversion_date", StringType(), True),
        StructField("plan", StringType(), True),
        StructField("mrr", IntegerType(), True),
        StructField("signup_date", StringType(), True),
        StructField("days_to_convert", IntegerType(), True),
        StructField("used_real_time_collab", BooleanType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ]
)
