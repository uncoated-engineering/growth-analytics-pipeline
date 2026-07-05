"""Per-table entry point for bronze subscription_events ingestion."""

from pyspark.sql import SparkSession

from spark.jobs.bronze.subscription_events.extract import ingest_subscription_events


def run(spark: SparkSession, raw_data_path: str = "data/raw", bronze_path: str = "data/bronze"):
    """Run subscription_events ingestion."""
    return ingest_subscription_events(
        spark,
        f"{raw_data_path}/subscription_events.jsonl",
        f"{bronze_path}/subscription_events",
    )


if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName("Bronze: subscription_events")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", "spark-warehouse")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        count = run(spark)
        print(f"Ingested {count} subscription events")
    finally:
        spark.stop()
