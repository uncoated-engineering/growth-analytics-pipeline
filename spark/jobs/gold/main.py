"""
Gold Layer - Business Analytics

This module orchestrates gold layer analytics:
- gold_feature_conversion_impact: Feature correlation with conversion rates
- gold_mrr_waterfall: Monthly MRR movements and net revenue retention
- gold_channel_performance: Acquisition funnel quality per channel
- gold_weekly_engagement: Weekly active users and feature adoption
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from spark.jobs.gold.channel_performance.aggregation import calculate_channel_performance
from spark.jobs.gold.feature_conversion_impact.aggregation import (
    calculate_feature_conversion_impact,
)
from spark.jobs.gold.mrr_waterfall.aggregation import calculate_mrr_waterfall
from spark.jobs.gold.weekly_engagement.aggregation import calculate_weekly_engagement


def run_gold_aggregation(
    spark: SparkSession,
    bronze_path: str = "data/bronze",
    silver_path: str = "data/silver",
    gold_path: str = "data/gold",
) -> dict:
    """
    Run all gold layer aggregation jobs.

    Args:
        spark: SparkSession
        bronze_path: Base path for bronze Delta tables
        silver_path: Base path for silver Delta tables
        gold_path: Base path for gold Delta tables

    Returns:
        Dictionary with row counts per gold table
    """
    print("=" * 80)
    print("Starting Gold Layer Aggregation")
    print("=" * 80)

    aggregation_stats = {}

    # Feature conversion impact analysis
    aggregation_stats["feature_conversion_impact"] = calculate_feature_conversion_impact(
        spark, bronze_path, silver_path, gold_path
    )

    # Monthly MRR waterfall + net revenue retention
    aggregation_stats["mrr_waterfall"] = calculate_mrr_waterfall(
        spark, bronze_path, silver_path, gold_path
    )

    # Acquisition channel performance
    aggregation_stats["channel_performance"] = calculate_channel_performance(
        spark, bronze_path, silver_path, gold_path
    )

    # Weekly feature engagement
    aggregation_stats["weekly_engagement"] = calculate_weekly_engagement(
        spark, bronze_path, silver_path, gold_path
    )

    print("=" * 80)
    print("Gold Layer Aggregation Complete")
    print("=" * 80)
    print("\nAggregation Summary:")
    for table, row_count in aggregation_stats.items():
        print(f"  {table}: {row_count} rows")

    return aggregation_stats


if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip

    # Create Spark session with Delta Lake support
    builder = (
        SparkSession.builder.appName("Gold Layer Aggregation")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", "spark-warehouse")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")

    try:
        stats = run_gold_aggregation(spark)

        # Validation: Query gold table to verify the "story"
        print("\n" + "=" * 80)
        print("Validation: Verifying Gold Tables")
        print("=" * 80)

        gold_path = "data/gold/gold_feature_conversion_impact"
        df = spark.read.format("delta").load(gold_path)
        row_count = df.count()
        print("\ngold_feature_conversion_impact:")
        print(f"  Row count: {row_count}")
        print("  Schema:")
        df.printSchema()
        print("  All cohort data:")
        df.show(truncate=False)

        # Show the key insight: real_time_collab conversion lift
        print("\n" + "=" * 80)
        print("Key Insight: Feature Conversion Impact")
        print("=" * 80)
        df.filter(col("feature_name") == "real_time_collab").show(truncate=False)

    finally:
        spark.stop()
