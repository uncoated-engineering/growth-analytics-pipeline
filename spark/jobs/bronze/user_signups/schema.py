from pyspark.sql.types import IntegerType, StringType, StructField, StructType

USER_SIGNUPS_SCHEMA = StructType(
    [
        StructField("user_id", IntegerType(), True),
        StructField("email", StringType(), True),
        StructField("signup_date", StringType(), True),
        StructField("company_size", StringType(), True),
        StructField("industry", StringType(), True),
    ]
)
