"""
Silver user dimension output columns:
    user_id (INT)
    signup_date (DATE)
    company_size (STRING)
    industry (STRING)
    acquisition_channel (STRING)  - From marketing attribution, 'unattributed' if untracked
    acquisition_campaign (STRING) - From marketing attribution, 'unattributed' if untracked
    current_plan (STRING)         - From latest subscription event:
                                    'free' (never converted), 'pro', 'enterprise',
                                    or 'churned' (cancelled)
    current_mrr (INT)             - MRR of the active subscription, 0 for free/churned
"""

from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

USER_DIM_COLUMNS = [
    "user_id",
    "signup_date",
    "company_size",
    "industry",
    "acquisition_channel",
    "acquisition_campaign",
    "current_plan",
    "current_mrr",
]

USER_DIM_SCHEMA = StructType(
    [
        StructField("user_id", IntegerType(), True),
        StructField("signup_date", DateType(), True),
        StructField("company_size", StringType(), True),
        StructField("industry", StringType(), True),
        StructField("acquisition_channel", StringType(), True),
        StructField("acquisition_campaign", StringType(), True),
        StructField("current_plan", StringType(), True),
        StructField("current_mrr", IntegerType(), True),
    ]
)
