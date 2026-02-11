"""Per-table entry point for bronze conversions ingestion."""

from pyspark.sql import SparkSession

from spark.jobs.bronze.conversions.extract import ingest_conversions


def run(spark: SparkSession, raw_data_path: str = "data/raw", bronze_path: str = "data/bronze"):
    """Run conversions ingestion."""
    return ingest_conversions(
        spark,
        f"{raw_data_path}/conversions.jsonl",
        f"{bronze_path}/conversions",
    )


if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName("Bronze: conversions")
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
        print(f"Ingested {count} conversions")
    finally:
        spark.stop()
