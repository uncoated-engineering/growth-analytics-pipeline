from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, lead, lit, to_date
from pyspark.sql.types import DateType
from pyspark.sql.window import Window

from spark.jobs.silver.subscription_periods.schema import (
    END_OF_TIME,
    SUBSCRIPTION_PERIODS_COLUMNS,
)


def create_subscription_periods(spark: SparkSession, bronze_path: str, silver_path: str) -> int:
    """
    Periodize bronze subscription events into validity intervals.

    Each lifecycle event (started, upgraded, downgraded, expanded, contracted)
    opens a period that lasts until the user's next event. Cancellation events
    close the previous period without opening a new one, so a churned customer
    has no active period.

    The result answers point-in-time questions ("what was this customer's MRR
    on June 1st?") with a simple BETWEEN filter — the same query pattern as the
    SCD Type 2 feature_states table.

    Args:
        spark: SparkSession
        bronze_path: Base path for bronze Delta tables
        silver_path: Base path for silver Delta tables

    Returns:
        Number of rows in the resulting silver table
    """
    output_path = f"{silver_path}/silver_subscription_periods"
    print(f"Creating subscription periods at {output_path}")

    events = spark.read.format("delta").load(f"{bronze_path}/subscription_events")

    events = events.withColumn("event_date", to_date(col("event_date")))

    # Order every user's events; the next event's date closes the current period.
    # lead() must run BEFORE dropping cancellations so a cancellation still
    # closes the period it terminates.
    user_window = Window.partitionBy("user_id").orderBy(col("event_date"), col("event_id"))
    periods = events.withColumn("next_event_date", lead("event_date").over(user_window))

    periods = (
        periods.filter(col("event_type") != "subscription_cancelled")
        .withColumn(
            "period_end",
            coalesce(col("next_event_date"), lit(END_OF_TIME).cast(DateType())),
        )
        .withColumn("is_active", col("next_event_date").isNull())
        .withColumnRenamed("event_date", "period_start")
        .withColumnRenamed("event_type", "change_type")
        .select(*SUBSCRIPTION_PERIODS_COLUMNS)
    )

    periods.write.format("delta").mode("overwrite").save(output_path)

    row_count = periods.count()
    print(f"  Subscription periods complete: {row_count} periods")
    return row_count
