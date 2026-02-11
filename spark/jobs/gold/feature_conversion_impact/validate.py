"""Data quality validation for gold feature_conversion_impact."""

import argparse
import sys

from pyspark.sql import SparkSession

from spark.jobs.bronze.conversions.schema import CONVERSIONS_OUTPUT_SCHEMA
from spark.jobs.data_quality.validators import validate_delta_table
from spark.jobs.gold.feature_conversion_impact.schema import FEATURE_CONVERSION_IMPACT_SCHEMA
from spark.jobs.silver.feature_states.schema import FEATURE_STATES_SCHEMA
from spark.jobs.silver.feature_usage_facts.schema import FEATURE_USAGE_FACTS_SCHEMA
from spark.jobs.silver.user_dim.schema import USER_DIM_SCHEMA


def validate_input(
    spark: SparkSession,
    bronze_path: str = "data/bronze",
    silver_path: str = "data/silver",
) -> dict:
    """Validate all upstream tables needed for gold feature_conversion_impact."""
    counts = {}
    counts["silver_user_dim"] = validate_delta_table(
        spark,
        path=f"{silver_path}/silver_user_dim",
        expected_schema=USER_DIM_SCHEMA,
        table_name="gold.feature_conversion_impact (input: silver.user_dim)",
    )
    counts["silver_feature_states"] = validate_delta_table(
        spark,
        path=f"{silver_path}/silver_feature_states",
        expected_schema=FEATURE_STATES_SCHEMA,
        table_name="gold.feature_conversion_impact (input: silver.feature_states)",
    )
    counts["silver_feature_usage_facts"] = validate_delta_table(
        spark,
        path=f"{silver_path}/silver_feature_usage_facts",
        expected_schema=FEATURE_USAGE_FACTS_SCHEMA,
        table_name="gold.feature_conversion_impact (input: silver.feature_usage_facts)",
    )
    counts["bronze_conversions"] = validate_delta_table(
        spark,
        path=f"{bronze_path}/conversions",
        expected_schema=CONVERSIONS_OUTPUT_SCHEMA,
        table_name="gold.feature_conversion_impact (input: bronze.conversions)",
    )
    return counts


def validate_output(spark: SparkSession, gold_path: str = "data/gold") -> int:
    """Validate the gold feature_conversion_impact Delta table after aggregation."""
    return validate_delta_table(
        spark,
        path=f"{gold_path}/gold_feature_conversion_impact",
        expected_schema=FEATURE_CONVERSION_IMPACT_SCHEMA,
        table_name="gold.feature_conversion_impact (output)",
    )


if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["input", "output"], required=True)
    args = parser.parse_args()

    builder = (
        SparkSession.builder.appName("Validate gold.feature_conversion_impact")
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
