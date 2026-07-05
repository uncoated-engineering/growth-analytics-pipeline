from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    greatest,
    lit,
    to_date,
    trunc,
    when,
)
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import DoubleType, LongType
from pyspark.sql.window import Window

from spark.jobs.gold.mrr_waterfall.schema import MRR_WATERFALL_COLUMNS


def calculate_mrr_waterfall(
    spark: SparkSession, bronze_path: str, silver_path: str, gold_path: str
) -> int:
    """
    Build the monthly MRR waterfall from subscription lifecycle events.

    Movement classification per event:
        subscription_started   -> new_business_mrr (+mrr)
        mrr increase           -> expansion_mrr (+delta): upgrades, seat expansion
        mrr decrease           -> contraction_mrr (+|delta|): downgrades, contraction
        subscription_cancelled -> churned_mrr (+previous_mrr)

    starting/ending MRR are derived as a running total of net movements, so
    the waterfall is internally consistent by construction:
        ending_mrr = starting_mrr + net_new_mrr

    Net revenue retention is computed against the month's starting MRR and
    excludes new business (the standard SaaS NRR definition).

    Args:
        spark: SparkSession
        bronze_path: Base path for bronze Delta tables
        silver_path: Base path for silver Delta tables (unused; kept for
            signature consistency across gold jobs)
        gold_path: Base path for gold Delta tables

    Returns:
        Number of rows in the resulting gold table
    """
    output_path = f"{gold_path}/gold_mrr_waterfall"
    print(f"Calculating MRR waterfall at {output_path}")

    events = spark.read.format("delta").load(f"{bronze_path}/subscription_events")

    events = events.withColumn("month", trunc(to_date(col("event_date")), "month"))

    is_start = col("event_type") == "subscription_started"
    is_cancel = col("event_type") == "subscription_cancelled"
    # Signed MRR delta for plan/seat changes (events that keep the subscription alive)
    change_delta = col("mrr") - coalesce(col("previous_mrr"), lit(0))

    classified = events.select(
        "month",
        when(is_start, col("mrr")).otherwise(0).alias("new_business_mrr"),
        when(~is_start & ~is_cancel, greatest(change_delta, lit(0)))
        .otherwise(0)
        .alias("expansion_mrr"),
        when(~is_start & ~is_cancel, greatest(-change_delta, lit(0)))
        .otherwise(0)
        .alias("contraction_mrr"),
        when(is_cancel, coalesce(col("previous_mrr"), lit(0))).otherwise(0).alias("churned_mrr"),
        when(is_start, 1).otherwise(0).alias("new_customers"),
        when(is_cancel, 1).otherwise(0).alias("churned_customers"),
    )

    monthly = classified.groupBy("month").agg(
        spark_sum("new_business_mrr").cast(LongType()).alias("new_business_mrr"),
        spark_sum("expansion_mrr").cast(LongType()).alias("expansion_mrr"),
        spark_sum("contraction_mrr").cast(LongType()).alias("contraction_mrr"),
        spark_sum("churned_mrr").cast(LongType()).alias("churned_mrr"),
        spark_sum("new_customers").cast(LongType()).alias("new_customers"),
        spark_sum("churned_customers").cast(LongType()).alias("churned_customers"),
    )

    monthly = monthly.withColumn(
        "net_new_mrr",
        (
            col("new_business_mrr")
            + col("expansion_mrr")
            - col("contraction_mrr")
            - col("churned_mrr")
        ).cast(LongType()),
    )

    # Running total -> ending MRR; starting MRR is the prior month's ending
    running_window = Window.orderBy("month").rowsBetween(Window.unboundedPreceding, 0)
    monthly = monthly.withColumn(
        "ending_mrr", spark_sum("net_new_mrr").over(running_window).cast(LongType())
    )
    monthly = monthly.withColumn(
        "starting_mrr", (col("ending_mrr") - col("net_new_mrr")).cast(LongType())
    )

    monthly = monthly.withColumn(
        "net_revenue_retention",
        when(
            col("starting_mrr") > 0,
            (
                (
                    col("starting_mrr")
                    + col("expansion_mrr")
                    - col("contraction_mrr")
                    - col("churned_mrr")
                ).cast(DoubleType())
                / col("starting_mrr")
            ),
        ),
    )

    result_df = monthly.select(*MRR_WATERFALL_COLUMNS).orderBy("month")

    result_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        output_path
    )

    row_count = result_df.count()
    print(f"  MRR waterfall complete: {row_count} months")
    return row_count
