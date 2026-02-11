"""Per-table entry point for bronze feature_releases ingestion."""

from pyspark.sql import SparkSession

from spark.jobs.bronze.feature_releases.extract import ingest_feature_releases


def run(spark: SparkSession, raw_data_path: str = "data/raw", bronze_path: str = "data/bronze"):
    """Run feature_releases ingestion."""
    return ingest_feature_releases(
        spark,
        f"{raw_data_path}/feature_releases.json",
        f"{bronze_path}/feature_releases",
    )


if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName("Bronze: feature_releases")
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
        print(f"Ingested {count} feature releases")
    finally:
        spark.stop()
