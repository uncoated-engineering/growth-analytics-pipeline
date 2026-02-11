"""
Silver Layer - SCD Type 2 Transformations

This module orchestrates silver layer transformations:
- silver_feature_states: SCD Type 2 dimension tracking feature version history
- silver_user_dim: Simple user dimension table
- silver_feature_usage_facts: Aggregated feature usage fact table
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from spark.jobs.silver.feature_states.transformation import maintain_feature_states_scd
from spark.jobs.silver.feature_usage_facts.transformation import create_feature_usage_facts
from spark.jobs.silver.user_dim.transformation import maintain_user_dim


def run_silver_transformation(
    spark: SparkSession,
    bronze_path: str = "data/bronze",
    silver_path: str = "data/silver",
) -> dict:
    """
    Run all silver layer transformation jobs.

    Args:
        spark: SparkSession
        bronze_path: Base path for bronze Delta tables
        silver_path: Base path for silver Delta tables

    Returns:
        Dictionary with row counts per silver table
    """
    print("=" * 80)
    print("Starting Silver Layer Transformation")
    print("=" * 80)

    transformation_stats = {}

    # SCD Type 2 feature states
    transformation_stats["feature_states"] = maintain_feature_states_scd(
        spark, bronze_path, silver_path
    )

    # User dimension
    transformation_stats["user_dim"] = maintain_user_dim(spark, bronze_path, silver_path)

    # Feature usage facts
    transformation_stats["feature_usage_facts"] = create_feature_usage_facts(
        spark, bronze_path, silver_path
    )

    print("=" * 80)
    print("Silver Layer Transformation Complete")
    print("=" * 80)
    print("\nTransformation Summary:")
    for table, row_count in transformation_stats.items():
        print(f"  {table}: {row_count} rows")

    return transformation_stats


if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip

    # Create Spark session with Delta Lake support
    builder = (
        SparkSession.builder.appName("Silver Layer Transformation")
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
        stats = run_silver_transformation(spark)

        # Validation: Query silver tables to verify
        print("\n" + "=" * 80)
        print("Validation: Verifying Silver Tables")
        print("=" * 80)

        for table_name in [
            "silver_feature_states",
            "silver_user_dim",
            "silver_feature_usage_facts",
        ]:
            table_path = f"data/silver/{table_name}"
            df = spark.read.format("delta").load(table_path)
            row_count = df.count()
            print(f"\n{table_name}:")
            print(f"  Row count: {row_count}")
            print("  Schema:")
            df.printSchema()
            print("  Sample data:")
            df.show(5, truncate=False)

        # SCD Type 2 validation query
        print("\n" + "=" * 80)
        print("SCD Type 2 Validation")
        print("=" * 80)
        feature_states = spark.read.format("delta").load("data/silver/silver_feature_states")
        print("\nAll current feature states:")
        feature_states.filter(col("is_current") == True).show(truncate=False)  # noqa: E712

    finally:
        spark.stop()
