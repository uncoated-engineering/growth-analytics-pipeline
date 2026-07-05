"""
Gold channel performance output columns (one row per signup month x channel):
    signup_month (DATE)          - First day of the signup month
    acquisition_channel (STRING) - First-touch channel ('unattributed' if untracked)
    signups (LONG)               - Users who signed up
    conversions (LONG)           - Of those, users who later converted to paid
    conversion_rate (DOUBLE)     - conversions / signups
    avg_days_to_convert (DOUBLE) - Average signup->conversion delay (converted only)
    total_new_mrr (LONG)         - MRR acquired from this cohort at conversion
    avg_new_mrr (DOUBLE)         - Average MRR per conversion
"""

from pyspark.sql.types import DateType, DoubleType, LongType, StringType, StructField, StructType

CHANNEL_PERFORMANCE_COLUMNS = [
    "signup_month",
    "acquisition_channel",
    "signups",
    "conversions",
    "conversion_rate",
    "avg_days_to_convert",
    "total_new_mrr",
    "avg_new_mrr",
]

CHANNEL_PERFORMANCE_SCHEMA = StructType(
    [
        StructField("signup_month", DateType(), True),
        StructField("acquisition_channel", StringType(), True),
        StructField("signups", LongType(), True),
        StructField("conversions", LongType(), True),
        StructField("conversion_rate", DoubleType(), True),
        StructField("avg_days_to_convert", DoubleType(), True),
        StructField("total_new_mrr", LongType(), True),
        StructField("avg_new_mrr", DoubleType(), True),
    ]
)
