"""
Gold weekly engagement output columns (one row per week x feature):
    week_start (DATE)              - Monday of the ISO week
    feature_name (STRING)
    active_users (LONG)            - Distinct users of this feature that week
    total_events (LONG)            - All usage events for this feature that week
    events_per_active_user (DOUBLE)
    weekly_active_users (LONG)     - Distinct users across ALL features that week
    pct_of_wau (DOUBLE)            - active_users / weekly_active_users
"""

from pyspark.sql.types import DateType, DoubleType, LongType, StringType, StructField, StructType

WEEKLY_ENGAGEMENT_COLUMNS = [
    "week_start",
    "feature_name",
    "active_users",
    "total_events",
    "events_per_active_user",
    "weekly_active_users",
    "pct_of_wau",
]

WEEKLY_ENGAGEMENT_SCHEMA = StructType(
    [
        StructField("week_start", DateType(), True),
        StructField("feature_name", StringType(), True),
        StructField("active_users", LongType(), True),
        StructField("total_events", LongType(), True),
        StructField("events_per_active_user", DoubleType(), True),
        StructField("weekly_active_users", LongType(), True),
        StructField("pct_of_wau", DoubleType(), True),
    ]
)
