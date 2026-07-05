from pyspark.sql.functions import current_timestamp

from spark.jobs.bronze.subscription_events.schema import SUBSCRIPTION_EVENTS_SCHEMA


def ingest_subscription_events(spark, input_path, output_path):
    """
    Read subscription_events JSONL -> Delta

    Subscription lifecycle events (started, upgraded, downgraded, expanded,
    contracted, cancelled) with the MRR before and after each change.

    Args:
        spark: SparkSession
        input_path: Path to subscription_events.jsonl
        output_path: Path to bronze Delta table
    """
    print(f"Ingesting subscription_events from {input_path} to {output_path}")

    df = spark.read.schema(SUBSCRIPTION_EVENTS_SCHEMA).json(input_path)

    df = df.withColumn("ingestion_timestamp", current_timestamp())

    df.write.format("delta").mode("append").save(output_path)

    row_count = df.count()
    print(f"Ingested {row_count} subscription events to {output_path}")

    return row_count
