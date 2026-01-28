"""
Bronze Layer Ingestion - Raw Data to Delta Lake

This module ingests raw data from JSON/JSONL files into Delta Lake bronze tables.
Each function adds an ingestion_timestamp and writes data in append mode.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_timestamp
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def ingest_feature_releases(spark, input_path, output_path):
    """
    Read feature_releases JSON → Write to Delta (append mode)

    Schema: feature_id, feature_name, release_date, version, ingestion_timestamp

    Args:
        spark: SparkSession
        input_path: Path to feature_releases.json
        output_path: Path to bronze Delta table
    """
    print(f"Ingesting feature_releases from {input_path} to {output_path}")

    # Define schema for feature releases
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("release_date", StringType(), True),
            StructField("version", StringType(), True),
        ]
    )

    # Read JSON file
    df = spark.read.schema(schema).json(input_path)

    # Rename 'id' to 'feature_id' and 'name' to 'feature_name' for clarity
    df = df.withColumnRenamed("id", "feature_id").withColumnRenamed("name", "feature_name")

    # Add ingestion timestamp
    df = df.withColumn("ingestion_timestamp", current_timestamp())

    # Write to Delta in append mode
    df.write.format("delta").mode("append").save(output_path)

    row_count = df.count()
    print(f"✓ Ingested {row_count} feature releases to {output_path}")

    return row_count


def ingest_user_signups(spark, input_path, output_path):
    """
    Read user_signups JSONL (partitioned by date) → Delta

    Schema: user_id, email, signup_date, company_size, industry, ingestion_timestamp

    Args:
        spark: SparkSession
        input_path: Path to user_signups.jsonl
        output_path: Path to bronze Delta table
    """
    print(f"Ingesting user_signups from {input_path} to {output_path}")

    # Define schema for user signups
    schema = StructType(
        [
            StructField("user_id", IntegerType(), True),
            StructField("email", StringType(), True),
            StructField("signup_date", StringType(), True),
            StructField("company_size", StringType(), True),
            StructField("industry", StringType(), True),
        ]
    )

    # Read JSONL file
    df = spark.read.schema(schema).json(input_path)

    # Add ingestion timestamp
    df = df.withColumn("ingestion_timestamp", current_timestamp())

    # Write to Delta in append mode, partitioned by signup_date
    df.write.format("delta").mode("append").partitionBy("signup_date").save(output_path)

    row_count = df.count()
    print(f"✓ Ingested {row_count} user signups to {output_path}")

    return row_count


def ingest_feature_usage_events(spark, input_path, output_path):
    """
    Read feature_usage_events JSONL (partitioned by hour) → Delta

    Schema: event_id, user_id, feature_id, event_type, event_timestamp, ingestion_timestamp

    This is the largest table - partition by date for efficient querying.

    Args:
        spark: SparkSession
        input_path: Path to feature_usage_events.jsonl
        output_path: Path to bronze Delta table
    """
    print(f"Ingesting feature_usage_events from {input_path} to {output_path}")

    # Define schema for feature usage events
    schema = StructType(
        [
            StructField("timestamp", StringType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("feature_id", IntegerType(), True),
            StructField("feature_name", StringType(), True),
            StructField("event_type", StringType(), True),
        ]
    )

    # Read JSONL file
    df = spark.read.schema(schema).json(input_path)

    # Convert timestamp string to timestamp type and rename to event_timestamp
    df = df.withColumn(
        "event_timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
    ).drop("timestamp")

    # Extract date for partitioning
    df = df.withColumn("event_date", col("event_timestamp").cast("date"))

    # Add ingestion timestamp
    df = df.withColumn("ingestion_timestamp", current_timestamp())

    # Generate event_id (using monotonically_increasing_id for uniqueness)
    from pyspark.sql.functions import monotonically_increasing_id

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
    print(f"✓ Ingested {row_count} feature usage events to {output_path}")

    return row_count


def ingest_conversions(spark, input_path, output_path):
    """
    Read conversions JSONL → Delta

    Schema: user_id, conversion_date, plan, mrr, ingestion_timestamp

    Args:
        spark: SparkSession
        input_path: Path to conversions.jsonl
        output_path: Path to bronze Delta table
    """
    print(f"Ingesting conversions from {input_path} to {output_path}")

    # Define schema for conversions
    schema = StructType(
        [
            StructField("user_id", IntegerType(), True),
            StructField("conversion_date", StringType(), True),
            StructField("plan", StringType(), True),
            StructField("mrr", IntegerType(), True),
            StructField("signup_date", StringType(), True),
            StructField("days_to_convert", IntegerType(), True),
            StructField("used_real_time_collab", BooleanType(), True),
        ]
    )

    # Read JSONL file
    df = spark.read.schema(schema).json(input_path)

    # Add ingestion timestamp
    df = df.withColumn("ingestion_timestamp", current_timestamp())

    # Write to Delta in append mode
    df.write.format("delta").mode("append").save(output_path)

    row_count = df.count()
    print(f"✓ Ingested {row_count} conversions to {output_path}")

    return row_count


def run_bronze_ingestion(spark, raw_data_path="data/raw", bronze_path="data/bronze"):
    """
    Run all bronze layer ingestion jobs.

    Args:
        spark: SparkSession
        raw_data_path: Base path for raw data files
        bronze_path: Base path for bronze Delta tables
    """
    print("=" * 80)
    print("Starting Bronze Layer Ingestion")
    print("=" * 80)

    ingestion_stats = {}

    # Ingest feature releases
    ingestion_stats["feature_releases"] = ingest_feature_releases(
        spark, f"{raw_data_path}/feature_releases.json", f"{bronze_path}/feature_releases"
    )

    # Ingest user signups
    ingestion_stats["user_signups"] = ingest_user_signups(
        spark, f"{raw_data_path}/user_signups.jsonl", f"{bronze_path}/user_signups"
    )

    # Ingest feature usage events
    ingestion_stats["feature_usage_events"] = ingest_feature_usage_events(
        spark, f"{raw_data_path}/feature_usage_events.jsonl", f"{bronze_path}/feature_usage_events"
    )

    # Ingest conversions
    ingestion_stats["conversions"] = ingest_conversions(
        spark, f"{raw_data_path}/conversions.jsonl", f"{bronze_path}/conversions"
    )

    print("=" * 80)
    print("Bronze Layer Ingestion Complete")
    print("=" * 80)
    print("\nIngestion Summary:")
    for table, count in ingestion_stats.items():
        print(f"  {table}: {count} rows")

    return ingestion_stats


if __name__ == "__main__":
    # Create Spark session with Delta Lake support
    spark = (
        SparkSession.builder.appName("Bronze Layer Ingestion")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.warehouse.dir", "spark-warehouse")
        .getOrCreate()
    )

    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Run bronze ingestion
        stats = run_bronze_ingestion(spark)

        # Validation: Query Delta tables to verify
        print("\n" + "=" * 80)
        print("Validation: Verifying Delta Tables")
        print("=" * 80)

        for table_name in [
            "feature_releases",
            "user_signups",
            "feature_usage_events",
            "conversions",
        ]:
            table_path = f"data/bronze/{table_name}"
            df = spark.read.format("delta").load(table_path)
            count = df.count()
            print(f"\n{table_name}:")
            print(f"  Row count: {count}")
            print("  Schema:")
            df.printSchema()
            print("  Sample data:")
            df.show(3, truncate=False)

    finally:
        spark.stop()
