"""Data quality validation for silver feature_usage_facts."""

import argparse
import sys

from pyspark.sql import SparkSession

from spark.jobs.bronze.feature_usage_events.schema import FEATURE_USAGE_EVENTS_OUTPUT_SCHEMA
from spark.jobs.data_quality.validators import validate_delta_table
from spark.jobs.silver.feature_usage_facts.schema import FEATURE_USAGE_FACTS_SCHEMA


def validate_input(spark: SparkSession, bronze_path: str = "data/bronze") -> int:
    """Validate the upstream bronze feature_usage_events Delta table."""
    return validate_delta_table(
        spark,
        path=f"{bronze_path}/feature_usage_events",
        expected_schema=FEATURE_USAGE_EVENTS_OUTPUT_SCHEMA,
        table_name="silver.feature_usage_facts (input: bronze.feature_usage_events)",
    )


def validate_output(spark: SparkSession, silver_path: str = "data/silver") -> int:
    """Validate the silver feature_usage_facts Delta table after transformation."""
    return validate_delta_table(
        spark,
        path=f"{silver_path}/silver_feature_usage_facts",
        expected_schema=FEATURE_USAGE_FACTS_SCHEMA,
        table_name="silver.feature_usage_facts (output)",
    )


if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["input", "output"], required=True)
    args = parser.parse_args()

    builder = (
        SparkSession.builder.appName("Validate silver.feature_usage_facts")
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
