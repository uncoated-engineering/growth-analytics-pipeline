"""
Silver feature usage facts output columns:
    user_id (INT)
    feature_id (INT)
    first_used_date (DATE)
    last_used_date (DATE)
    total_usage_count (INT)
    avg_daily_usage (DOUBLE)
    as_of_date (DATE) - Snapshot date
"""

FEATURE_USAGE_FACTS_COLUMNS = [
    "user_id",
    "feature_id",
    "first_used_date",
    "last_used_date",
    "total_usage_count",
    "avg_daily_usage",
    "as_of_date",
]
