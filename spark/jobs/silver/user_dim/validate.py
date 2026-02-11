"""Data quality validation for silver user_dim."""

import argparse
import sys

from pyspark.sql import SparkSession

from spark.jobs.bronze.conversions.schema import CONVERSIONS_OUTPUT_SCHEMA
from spark.jobs.bronze.user_signups.schema import USER_SIGNUPS_OUTPUT_SCHEMA
from spark.jobs.data_quality.validators import validate_delta_table
from spark.jobs.silver.user_dim.schema import USER_DIM_SCHEMA


def validate_input(spark: SparkSession, bronze_path: str = "data/bronze") -> dict:
    """Validate the upstream bronze user_signups and conversions Delta tables."""
    counts = {}
    counts["user_signups"] = validate_delta_table(
        spark,
        path=f"{bronze_path}/user_signups",
        expected_schema=USER_SIGNUPS_OUTPUT_SCHEMA,
        table_name="silver.user_dim (input: bronze.user_signups)",
    )
    counts["conversions"] = validate_delta_table(
        spark,
        path=f"{bronze_path}/conversions",
        expected_schema=CONVERSIONS_OUTPUT_SCHEMA,
        table_name="silver.user_dim (input: bronze.conversions)",
    )
    return counts


def validate_output(spark: SparkSession, silver_path: str = "data/silver") -> int:
    """Validate the silver user_dim Delta table after transformation."""
    return validate_delta_table(
        spark,
        path=f"{silver_path}/silver_user_dim",
        expected_schema=USER_DIM_SCHEMA,
        table_name="silver.user_dim (output)",
    )


if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["input", "output"], required=True)
    args = parser.parse_args()

    builder = (
        SparkSession.builder.appName("Validate silver.user_dim")
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
