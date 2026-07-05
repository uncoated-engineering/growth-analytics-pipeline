from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, date_trunc, to_date
from pyspark.sql.types import DoubleType, LongType

from spark.jobs.gold.weekly_engagement.schema import WEEKLY_ENGAGEMENT_COLUMNS


def calculate_weekly_engagement(
    spark: SparkSession, bronze_path: str, silver_path: str, gold_path: str
) -> int:
    """
    Aggregate feature usage events into weekly engagement metrics.

    Produces per-feature weekly active users alongside the overall WAU for
    the same week, so each feature's reach can be read as a share of the
    active base (pct_of_wau) — the standard feature adoption view.

    Args:
        spark: SparkSession
        bronze_path: Base path for bronze Delta tables
        silver_path: Base path for silver Delta tables (unused; kept for
            signature consistency across gold jobs)
        gold_path: Base path for gold Delta tables

    Returns:
        Number of rows in the resulting gold table
    """
    output_path = f"{gold_path}/gold_weekly_engagement"
    print(f"Calculating weekly engagement at {output_path}")

    events = spark.read.format("delta").load(f"{bronze_path}/feature_usage_events")

    events = events.withColumn("week_start", to_date(date_trunc("week", col("event_timestamp"))))

    per_feature = events.groupBy("week_start", "feature_name").agg(
        countDistinct("user_id").cast(LongType()).alias("active_users"),
        count("*").cast(LongType()).alias("total_events"),
    )
    per_feature = per_feature.withColumn(
        "events_per_active_user",
        (col("total_events") / col("active_users")).cast(DoubleType()),
    )

    overall = events.groupBy("week_start").agg(
        countDistinct("user_id").cast(LongType()).alias("weekly_active_users")
    )

    result_df = (
        per_feature.join(overall, on="week_start", how="left")
        .withColumn(
            "pct_of_wau",
            (col("active_users") / col("weekly_active_users")).cast(DoubleType()),
        )
        .select(*WEEKLY_ENGAGEMENT_COLUMNS)
        .orderBy("week_start", "feature_name")
    )

    result_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        output_path
    )

    row_count = result_df.count()
    print(f"  Weekly engagement complete: {row_count} week-feature rows")
    return row_count
