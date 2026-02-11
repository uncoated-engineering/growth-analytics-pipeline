from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

FEATURE_RELEASES_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("release_date", StringType(), True),
        StructField("version", StringType(), True),
    ]
)

# Schema after ingestion (columns renamed, ingestion_timestamp added)
FEATURE_RELEASES_OUTPUT_SCHEMA = StructType(
    [
        StructField("feature_id", IntegerType(), True),
        StructField("feature_name", StringType(), True),
        StructField("release_date", StringType(), True),
        StructField("version", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ]
)
