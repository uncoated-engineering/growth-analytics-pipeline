"""Data quality validation for silver subscription_periods."""

import argparse
import sys

from pyspark.sql import SparkSession

from spark.jobs.bronze.subscription_events.schema import SUBSCRIPTION_EVENTS_OUTPUT_SCHEMA
from spark.jobs.data_quality.validators import validate_delta_table
from spark.jobs.silver.subscription_periods.schema import SUBSCRIPTION_PERIODS_SCHEMA


def validate_input(spark: SparkSession, bronze_path: str = "data/bronze") -> int:
    """Validate the bronze subscription_events table before transformation."""
    return validate_delta_table(
        spark,
        path=f"{bronze_path}/subscription_events",
        expected_schema=SUBSCRIPTION_EVENTS_OUTPUT_SCHEMA,
        table_name="silver.subscription_periods (input: bronze.subscription_events)",
    )


def validate_output(spark: SparkSession, silver_path: str = "data/silver") -> int:
    """Validate the silver subscription_periods Delta table after transformation."""
    return validate_delta_table(
        spark,
        path=f"{silver_path}/silver_subscription_periods",
        expected_schema=SUBSCRIPTION_PERIODS_SCHEMA,
        table_name="silver.subscription_periods (output)",
    )


if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["input", "output"], required=True)
    args = parser.parse_args()

    builder = (
        SparkSession.builder.appName("Validate silver.subscription_periods")
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
