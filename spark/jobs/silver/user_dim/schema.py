"""
Silver user dimension output columns:
    user_id (INT)
    signup_date (DATE)
    company_size (STRING)
    industry (STRING)
    current_plan (STRING) - Joined from conversions, defaults to 'free'
"""

from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

USER_DIM_COLUMNS = [
    "user_id",
    "signup_date",
    "company_size",
    "industry",
    "current_plan",
]

USER_DIM_SCHEMA = StructType(
    [
        StructField("user_id", IntegerType(), True),
        StructField("signup_date", DateType(), True),
        StructField("company_size", StringType(), True),
        StructField("industry", StringType(), True),
        StructField("current_plan", StringType(), True),
    ]
)
