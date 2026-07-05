"""
Silver subscription periods output columns:
    user_id (INT)
    plan (STRING)            - Plan in force during the period
    mrr (INT)                - MRR in force during the period
    period_start (DATE)      - Date the state took effect
    period_end (DATE)        - Date the next event superseded it (9999-12-31 if none)
    is_active (BOOLEAN)      - True for the customer's current, non-cancelled state
    change_type (STRING)     - Subscription event that opened the period
"""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

END_OF_TIME = "9999-12-31"

SUBSCRIPTION_PERIODS_COLUMNS = [
    "user_id",
    "plan",
    "mrr",
    "period_start",
    "period_end",
    "is_active",
    "change_type",
]

SUBSCRIPTION_PERIODS_SCHEMA = StructType(
    [
        StructField("user_id", IntegerType(), True),
        StructField("plan", StringType(), True),
        StructField("mrr", IntegerType(), True),
        StructField("period_start", DateType(), True),
        StructField("period_end", DateType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("change_type", StringType(), True),
    ]
)
