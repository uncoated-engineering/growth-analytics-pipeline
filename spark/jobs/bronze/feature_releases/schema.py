from pyspark.sql.types import IntegerType, StringType, StructField, StructType

FEATURE_RELEASES_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("release_date", StringType(), True),
        StructField("version", StringType(), True),
    ]
)
