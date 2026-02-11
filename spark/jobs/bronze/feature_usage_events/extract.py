from pyspark.sql.functions import (
    col,
    current_timestamp,
    monotonically_increasing_id,
    to_timestamp,
)

from spark.jobs.bronze.feature_usage_events.schema import FEATURE_USAGE_EVENTS_SCHEMA


def ingest_feature_usage_events(spark, input_path, output_path):
    """
    Read feature_usage_events JSONL (partitioned by hour) -> Delta

    Schema: event_id, user_id, feature_id, event_type, event_timestamp, ingestion_timestamp

    This is the largest table - partition by date for efficient querying.

    Args:
        spark: SparkSession
        input_path: Path to feature_usage_events.jsonl
        output_path: Path to bronze Delta table
    """
    print(f"Ingesting feature_usage_events from {input_path} to {output_path}")

    # Read JSONL file
    df = spark.read.schema(FEATURE_USAGE_EVENTS_SCHEMA).json(input_path)

    # Convert timestamp string to timestamp type and rename to event_timestamp
    df = df.withColumn(
        "event_timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
    ).drop("timestamp")

    # Extract date for partitioning
    df = df.withColumn("event_date", col("event_timestamp").cast("date"))

    # Add ingestion timestamp
    df = df.withColumn("ingestion_timestamp", current_timestamp())

    # Generate event_id (using monotonically_increasing_id for uniqueness)
    df = df.withColumn("event_id", monotonically_increasing_id())

    # Reorder columns
    df = df.select(
        "event_id",
        "user_id",
        "feature_id",
        "feature_name",
        "event_type",
        "event_timestamp",
        "event_date",
        "ingestion_timestamp",
    )

    # Write to Delta in append mode, partitioned by event_date
    df.write.format("delta").mode("append").partitionBy("event_date").save(output_path)

    row_count = df.count()
    print(f"Ingested {row_count} feature usage events to {output_path}")

    return row_count
