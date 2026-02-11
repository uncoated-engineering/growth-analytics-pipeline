"""Per-table entry point for silver user_dim."""

from pyspark.sql import SparkSession

from spark.jobs.silver.user_dim.transformation import maintain_user_dim


def run(
    spark: SparkSession,
    bronze_path: str = "data/bronze",
    silver_path: str = "data/silver",
):
    """Run user dimension transformation."""
    return maintain_user_dim(spark, bronze_path, silver_path)


if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName("Silver: user_dim")
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
        print(f"User dimension complete: {count} rows")
    finally:
        spark.stop()
