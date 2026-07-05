from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, lit, row_number, to_date, when
from pyspark.sql.window import Window

from spark.jobs.silver.user_dim.schema import USER_DIM_COLUMNS


def maintain_user_dim(spark: SparkSession, bronze_path: str, silver_path: str) -> int:
    """
    Create/update the user dimension table from bronze sources.

    Combines three bronze tables:
      - user_signups:          identity and firmographics
      - marketing_attribution: first-touch channel/campaign ('unattributed'
                               for the ~5% of signups without a record)
      - subscription_events:   current commercial state, derived from each
                               user's latest lifecycle event

    Plan resolution: never converted -> 'free'; latest event is a
    cancellation -> 'churned'; otherwise the plan on the latest event.
    current_mrr is 0 unless the user holds an active subscription.

    Args:
        spark: SparkSession
        bronze_path: Base path for bronze Delta tables
        silver_path: Base path for silver Delta tables

    Returns:
        Number of rows in the resulting silver table
    """
    output_path = f"{silver_path}/silver_user_dim"
    print(f"Maintaining user dimension at {output_path}")

    signups = spark.read.format("delta").load(f"{bronze_path}/user_signups")
    attribution = spark.read.format("delta").load(f"{bronze_path}/marketing_attribution")
    subscription_events = spark.read.format("delta").load(f"{bronze_path}/subscription_events")

    # Latest subscription event per user determines the current commercial state
    event_window = Window.partitionBy("user_id").orderBy(
        col("event_date").desc(), col("event_id").desc()
    )
    latest_state = (
        subscription_events.withColumn("rn", row_number().over(event_window))
        .filter(col("rn") == 1)
        .select(
            col("user_id").alias("sub_user_id"),
            when(col("event_type") == "subscription_cancelled", lit("churned"))
            .otherwise(col("plan"))
            .alias("current_plan"),
            when(col("event_type") == "subscription_cancelled", lit(0))
            .otherwise(col("mrr"))
            .alias("current_mrr"),
        )
    )

    first_touch = attribution.select(
        col("user_id").alias("attr_user_id"),
        col("channel").alias("acquisition_channel"),
        col("campaign").alias("acquisition_campaign"),
    ).dropDuplicates(["attr_user_id"])

    user_dim = (
        signups.select("user_id", "signup_date", "company_size", "industry")
        .dropDuplicates(["user_id"])
        .join(first_touch, col("user_id") == col("attr_user_id"), "left")
        .join(latest_state, col("user_id") == col("sub_user_id"), "left")
        .withColumn(
            "acquisition_channel", coalesce(col("acquisition_channel"), lit("unattributed"))
        )
        .withColumn(
            "acquisition_campaign", coalesce(col("acquisition_campaign"), lit("unattributed"))
        )
        .withColumn("current_plan", coalesce(col("current_plan"), lit("free")))
        .withColumn("current_mrr", coalesce(col("current_mrr"), lit(0)))
        .withColumn("signup_date", to_date(col("signup_date")))
        .select(*USER_DIM_COLUMNS)
    )

    user_dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        output_path
    )

    row_count = user_dim.count()
    print(f"  User dimension complete: {row_count} users")
    return row_count
