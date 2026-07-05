"""Data quality validation for gold mrr_waterfall."""

import argparse
import sys

from pyspark.sql import SparkSession

from spark.jobs.bronze.subscription_events.schema import SUBSCRIPTION_EVENTS_OUTPUT_SCHEMA
from spark.jobs.data_quality.validators import validate_delta_table
from spark.jobs.gold.mrr_waterfall.schema import MRR_WATERFALL_SCHEMA


def validate_input(spark: SparkSession, bronze_path: str = "data/bronze") -> int:
    """Validate the bronze subscription_events table before aggregation."""
    return validate_delta_table(
        spark,
        path=f"{bronze_path}/subscription_events",
        expected_schema=SUBSCRIPTION_EVENTS_OUTPUT_SCHEMA,
        table_name="gold.mrr_waterfall (input: bronze.subscription_events)",
    )


def validate_output(spark: SparkSession, gold_path: str = "data/gold") -> int:
    """Validate the gold mrr_waterfall Delta table after aggregation."""
    return validate_delta_table(
        spark,
        path=f"{gold_path}/gold_mrr_waterfall",
        expected_schema=MRR_WATERFALL_SCHEMA,
        table_name="gold.mrr_waterfall (output)",
    )


if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["input", "output"], required=True)
    args = parser.parse_args()

    builder = (
        SparkSession.builder.appName("Validate gold.mrr_waterfall")
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
