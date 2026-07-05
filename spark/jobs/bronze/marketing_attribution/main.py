"""Per-table entry point for bronze marketing_attribution ingestion."""

from pyspark.sql import SparkSession

from spark.jobs.bronze.marketing_attribution.extract import ingest_marketing_attribution


def run(spark: SparkSession, raw_data_path: str = "data/raw", bronze_path: str = "data/bronze"):
    """Run marketing_attribution ingestion."""
    return ingest_marketing_attribution(
        spark,
        f"{raw_data_path}/marketing_attribution.jsonl",
        f"{bronze_path}/marketing_attribution",
    )


if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName("Bronze: marketing_attribution")
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
        print(f"Ingested {count} attribution records")
    finally:
        spark.stop()
