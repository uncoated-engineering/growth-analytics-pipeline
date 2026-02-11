"""Per-table entry point for bronze feature_usage_events ingestion."""

from pyspark.sql import SparkSession

from spark.jobs.bronze.feature_usage_events.extract import ingest_feature_usage_events


def run(spark: SparkSession, raw_data_path: str = "data/raw", bronze_path: str = "data/bronze"):
    """Run feature_usage_events ingestion."""
    return ingest_feature_usage_events(
        spark,
        f"{raw_data_path}/feature_usage_events.jsonl",
        f"{bronze_path}/feature_usage_events",
    )


if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName("Bronze: feature_usage_events")
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
        print(f"Ingested {count} feature usage events")
    finally:
        spark.stop()
