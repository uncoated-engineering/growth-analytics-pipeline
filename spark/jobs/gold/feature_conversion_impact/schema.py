"""
Gold feature conversion impact output columns:
    feature_name (STRING)
    cohort (STRING)             - 'used_before_conversion', 'available_not_used',
                                  or 'not_available'
    total_users (LONG)
    converted_users (LONG)
    conversion_rate (DOUBLE)
    avg_days_to_convert (DOUBLE)
    avg_mrr (DOUBLE)
"""

FEATURE_CONVERSION_IMPACT_COLUMNS = [
    "feature_name",
    "cohort",
    "total_users",
    "converted_users",
    "conversion_rate",
    "avg_days_to_convert",
    "avg_mrr",
]
