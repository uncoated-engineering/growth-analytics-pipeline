"""
Gold MRR waterfall output columns (one row per calendar month):
    month (DATE)                   - First day of the month
    starting_mrr (LONG)            - MRR at the start of the month
    new_business_mrr (LONG)        - MRR from subscriptions started this month
    expansion_mrr (LONG)           - MRR gained from upgrades / seat expansion
    contraction_mrr (LONG)         - MRR lost to downgrades / seat contraction
    churned_mrr (LONG)             - MRR lost to cancellations
    net_new_mrr (LONG)             - new + expansion - contraction - churned
    ending_mrr (LONG)              - starting_mrr + net_new_mrr
    new_customers (LONG)           - Subscriptions started this month
    churned_customers (LONG)       - Subscriptions cancelled this month
    net_revenue_retention (DOUBLE) - (starting + expansion - contraction - churned)
                                     / starting; NULL for the first month
"""

from pyspark.sql.types import DateType, DoubleType, LongType, StructField, StructType

MRR_WATERFALL_COLUMNS = [
    "month",
    "starting_mrr",
    "new_business_mrr",
    "expansion_mrr",
    "contraction_mrr",
    "churned_mrr",
    "net_new_mrr",
    "ending_mrr",
    "new_customers",
    "churned_customers",
    "net_revenue_retention",
]

MRR_WATERFALL_SCHEMA = StructType(
    [
        StructField("month", DateType(), True),
        StructField("starting_mrr", LongType(), True),
        StructField("new_business_mrr", LongType(), True),
        StructField("expansion_mrr", LongType(), True),
        StructField("contraction_mrr", LongType(), True),
        StructField("churned_mrr", LongType(), True),
        StructField("net_new_mrr", LongType(), True),
        StructField("ending_mrr", LongType(), True),
        StructField("new_customers", LongType(), True),
        StructField("churned_customers", LongType(), True),
        StructField("net_revenue_retention", DoubleType(), True),
    ]
)
