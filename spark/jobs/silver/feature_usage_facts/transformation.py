from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, current_timestamp, datediff
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.types import DateType, DoubleType


def create_feature_usage_facts(spark: SparkSession, bronze_path: str, silver_path: str) -> int:
    """
    Aggregate bronze feature usage events into a silver fact table.

    Creates per-user, per-feature usage summaries that enable time-point queries
    like "What did usage look like on 2024-06-01?"

    Schema:
        user_id (INT)
        feature_id (INT)
        first_used_date (DATE)      - Important for "used before conversion" analysis
        last_used_date (DATE)
        total_usage_count (INT)
        avg_daily_usage (DOUBLE)
        as_of_date (DATE)           - Snapshot date

    Args:
        spark: SparkSession
        bronze_path: Base path for bronze Delta tables
        silver_path: Base path for silver Delta tables

    Returns:
        Number of rows in the resulting silver table
    """
    output_path = f"{silver_path}/silver_feature_usage_facts"
    print(f"Creating feature usage facts at {output_path}")

    # Read bronze feature usage events
    events = spark.read.format("delta").load(f"{bronze_path}/feature_usage_events")

    # Aggregate per user + feature
    usage_facts = events.groupBy("user_id", "feature_id").agg(
        spark_min("event_date").alias("first_used_date"),
        spark_max("event_date").alias("last_used_date"),
        count("*").alias("total_usage_count"),
    )

    # Calculate avg_daily_usage: total_usage_count / (last_used_date - first_used_date + 1)
    usage_facts = usage_facts.withColumn(
        "date_span",
        datediff(col("last_used_date"), col("first_used_date")) + 1,
    )
    usage_facts = usage_facts.withColumn(
        "avg_daily_usage",
        (col("total_usage_count") / col("date_span")).cast(DoubleType()),
    ).drop("date_span")

    # Add snapshot date (as_of_date) = current date
    usage_facts = usage_facts.withColumn("as_of_date", current_timestamp().cast(DateType()))

    # Select final column order
    usage_facts = usage_facts.select(
        "user_id",
        "feature_id",
        "first_used_date",
        "last_used_date",
        "total_usage_count",
        "avg_daily_usage",
        "as_of_date",
    )

    # Write/overwrite the fact table
    usage_facts.write.format("delta").mode("overwrite").save(output_path)

    row_count = usage_facts.count()
    print(f"  Feature usage facts complete: {row_count} user-feature combinations")
    return row_count
