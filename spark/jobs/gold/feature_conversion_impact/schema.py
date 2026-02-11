"""
Gold feature conversion impact output columns:
    feature_name (STRING)
    cohort (STRING)             - 'used_feature', 'available_not_used',
                                  or 'not_available'
    total_users (LONG)
    converted_users (LONG)
    conversion_rate (DOUBLE)
    avg_days_to_convert (DOUBLE)
    avg_mrr (DOUBLE)
"""

from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

FEATURE_CONVERSION_IMPACT_COLUMNS = [
    "feature_name",
    "cohort",
    "total_users",
    "converted_users",
    "conversion_rate",
    "avg_days_to_convert",
    "avg_mrr",
]

FEATURE_CONVERSION_IMPACT_SCHEMA = StructType(
    [
        StructField("feature_name", StringType(), True),
        StructField("cohort", StringType(), True),
        StructField("total_users", LongType(), True),
        StructField("converted_users", LongType(), True),
        StructField("conversion_rate", DoubleType(), True),
        StructField("avg_days_to_convert", DoubleType(), True),
        StructField("avg_mrr", DoubleType(), True),
    ]
)
