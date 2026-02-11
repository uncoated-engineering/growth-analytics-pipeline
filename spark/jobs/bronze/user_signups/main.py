"""Per-table entry point for bronze user_signups ingestion."""

from pyspark.sql import SparkSession

from spark.jobs.bronze.user_signups.extract import ingest_user_signups


def run(spark: SparkSession, raw_data_path: str = "data/raw", bronze_path: str = "data/bronze"):
    """Run user_signups ingestion."""
    return ingest_user_signups(
        spark,
        f"{raw_data_path}/user_signups.jsonl",
        f"{bronze_path}/user_signups",
    )


if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName("Bronze: user_signups")
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
        print(f"Ingested {count} user signups")
    finally:
        spark.stop()
