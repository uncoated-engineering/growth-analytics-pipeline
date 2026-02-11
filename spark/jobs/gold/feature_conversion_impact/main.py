"""Per-table entry point for gold feature_conversion_impact."""

from pyspark.sql import SparkSession

from spark.jobs.gold.feature_conversion_impact.aggregation import (
    calculate_feature_conversion_impact,
)


def run(
    spark: SparkSession,
    bronze_path: str = "data/bronze",
    silver_path: str = "data/silver",
    gold_path: str = "data/gold",
):
    """Run feature conversion impact cohort analysis."""
    return calculate_feature_conversion_impact(spark, bronze_path, silver_path, gold_path)


if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName("Gold: feature_conversion_impact")
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
        print(f"Feature conversion impact complete: {count} rows")
    finally:
        spark.stop()
