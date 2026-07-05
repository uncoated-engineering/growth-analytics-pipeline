"""Per-table entry point for gold weekly_engagement."""

from pyspark.sql import SparkSession

from spark.jobs.gold.weekly_engagement.aggregation import calculate_weekly_engagement


def run(
    spark: SparkSession,
    bronze_path: str = "data/bronze",
    silver_path: str = "data/silver",
    gold_path: str = "data/gold",
):
    """Run weekly engagement aggregation."""
    return calculate_weekly_engagement(spark, bronze_path, silver_path, gold_path)


if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName("Gold: weekly_engagement")
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
        print(f"Weekly engagement complete: {count} rows")
    finally:
        spark.stop()
