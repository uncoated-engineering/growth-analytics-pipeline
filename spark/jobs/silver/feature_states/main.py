"""Per-table entry point for silver feature_states SCD Type 2."""

from pyspark.sql import SparkSession

from spark.jobs.silver.feature_states.transformation import maintain_feature_states_scd


def run(
    spark: SparkSession,
    bronze_path: str = "data/bronze",
    silver_path: str = "data/silver",
):
    """Run feature_states SCD Type 2 transformation."""
    return maintain_feature_states_scd(spark, bronze_path, silver_path)


if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName("Silver: feature_states")
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
        print(f"Feature states SCD complete: {count} rows")
    finally:
        spark.stop()
