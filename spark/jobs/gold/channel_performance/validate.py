"""Data quality validation for gold channel_performance."""

import argparse
import sys

from pyspark.sql import SparkSession

from spark.jobs.bronze.conversions.schema import CONVERSIONS_OUTPUT_SCHEMA
from spark.jobs.data_quality.validators import validate_delta_table
from spark.jobs.gold.channel_performance.schema import CHANNEL_PERFORMANCE_SCHEMA
from spark.jobs.silver.user_dim.schema import USER_DIM_SCHEMA


def validate_input(
    spark: SparkSession,
    bronze_path: str = "data/bronze",
    silver_path: str = "data/silver",
) -> dict:
    """Validate all upstream tables needed for gold channel_performance."""
    counts = {}
    counts["silver_user_dim"] = validate_delta_table(
        spark,
        path=f"{silver_path}/silver_user_dim",
        expected_schema=USER_DIM_SCHEMA,
        table_name="gold.channel_performance (input: silver.user_dim)",
    )
    counts["bronze_conversions"] = validate_delta_table(
        spark,
        path=f"{bronze_path}/conversions",
        expected_schema=CONVERSIONS_OUTPUT_SCHEMA,
        table_name="gold.channel_performance (input: bronze.conversions)",
    )
    return counts


def validate_output(spark: SparkSession, gold_path: str = "data/gold") -> int:
    """Validate the gold channel_performance Delta table after aggregation."""
    return validate_delta_table(
        spark,
        path=f"{gold_path}/gold_channel_performance",
        expected_schema=CHANNEL_PERFORMANCE_SCHEMA,
        table_name="gold.channel_performance (output)",
    )


if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["input", "output"], required=True)
    args = parser.parse_args()

    builder = (
        SparkSession.builder.appName("Validate gold.channel_performance")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        if args.mode == "input":
            result = validate_input(spark)
            print(f"Validation passed: {result}")
        else:
            count = validate_output(spark)
            print(f"Validation passed: {count} rows")
    except Exception as e:
        print(f"Validation failed: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()
