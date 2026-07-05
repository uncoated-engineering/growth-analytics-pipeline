"""Per-table entry point for gold mrr_waterfall."""

from pyspark.sql import SparkSession

from spark.jobs.gold.mrr_waterfall.aggregation import calculate_mrr_waterfall


def run(
    spark: SparkSession,
    bronze_path: str = "data/bronze",
    silver_path: str = "data/silver",
    gold_path: str = "data/gold",
):
    """Run MRR waterfall aggregation."""
    return calculate_mrr_waterfall(spark, bronze_path, silver_path, gold_path)


if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName("Gold: mrr_waterfall")
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
        print(f"MRR waterfall complete: {count} rows")
    finally:
        spark.stop()
