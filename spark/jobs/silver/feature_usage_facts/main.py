"""Per-table entry point for silver feature_usage_facts."""

from pyspark.sql import SparkSession

from spark.jobs.silver.feature_usage_facts.transformation import create_feature_usage_facts


def run(
    spark: SparkSession,
    bronze_path: str = "data/bronze",
    silver_path: str = "data/silver",
):
    """Run feature usage facts aggregation."""
    return create_feature_usage_facts(spark, bronze_path, silver_path)


if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName("Silver: feature_usage_facts")
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
        print(f"Feature usage facts complete: {count} rows")
    finally:
        spark.stop()
