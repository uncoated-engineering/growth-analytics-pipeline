from pyspark.sql.types import IntegerType, StringType, StructField, StructType

FEATURE_USAGE_EVENTS_SCHEMA = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("feature_id", IntegerType(), True),
        StructField("feature_name", StringType(), True),
        StructField("event_type", StringType(), True),
    ]
)
