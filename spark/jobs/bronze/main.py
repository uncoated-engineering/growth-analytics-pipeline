"""
Bronze Layer Ingestion - Raw Data to Delta Lake

This module orchestrates ingestion of raw data from JSON/JSONL files
into Delta Lake bronze tables.
"""

from pyspark.sql import SparkSession

from spark.jobs.bronze.conversions.extract import ingest_conversions
from spark.jobs.bronze.feature_releases.extract import ingest_feature_releases
from spark.jobs.bronze.feature_usage_events.extract import ingest_feature_usage_events
from spark.jobs.bronze.user_signups.extract import ingest_user_signups


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
    from delta import configure_spark_with_delta_pip

    # Create Spark session with Delta Lake support
    builder = (
        SparkSession.builder.appName("Bronze Layer Ingestion")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.warehouse.dir", "spark-warehouse")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

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
