"""Data quality validation for bronze user_signups."""

import argparse
import sys

from pyspark.sql import SparkSession

from spark.jobs.bronze.user_signups.schema import (
    USER_SIGNUPS_OUTPUT_SCHEMA,
    USER_SIGNUPS_SCHEMA,
)
from spark.jobs.data_quality.validators import validate_delta_table, validate_raw_file


def validate_input(spark: SparkSession, raw_data_path: str = "data/raw") -> int:
    """Validate the raw user_signups.jsonl file before ingestion."""
    return validate_raw_file(
        spark,
        path=f"{raw_data_path}/user_signups.jsonl",
        expected_schema=USER_SIGNUPS_SCHEMA,
        table_name="bronze.user_signups (input)",
    )


def validate_output(spark: SparkSession, bronze_path: str = "data/bronze") -> int:
    """Validate the bronze user_signups Delta table after ingestion."""
    return validate_delta_table(
        spark,
        path=f"{bronze_path}/user_signups",
        expected_schema=USER_SIGNUPS_OUTPUT_SCHEMA,
        table_name="bronze.user_signups (output)",
    )


if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["input", "output"], required=True)
    args = parser.parse_args()

    builder = (
        SparkSession.builder.appName("Validate bronze.user_signups")
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
            count = validate_input(spark)
        else:
            count = validate_output(spark)
        print(f"Validation passed: {count} rows")
    except Exception as e:
        print(f"Validation failed: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()
