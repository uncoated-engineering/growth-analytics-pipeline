"""Per-table entry point for silver subscription_periods."""

from pyspark.sql import SparkSession

from spark.jobs.silver.subscription_periods.transformation import create_subscription_periods


def run(
    spark: SparkSession,
    bronze_path: str = "data/bronze",
    silver_path: str = "data/silver",
):
    """Run subscription periods transformation."""
    return create_subscription_periods(spark, bronze_path, silver_path)


if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName("Silver: subscription_periods")
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
        print(f"Subscription periods complete: {count} rows")
    finally:
        spark.stop()
