"""
Shared test helpers to set up bronze (and silver) layers for Spark job tests.
"""

import json
import os

from spark.jobs.bronze.conversions.extract import ingest_conversions
from spark.jobs.bronze.feature_releases.extract import ingest_feature_releases
from spark.jobs.bronze.feature_usage_events.extract import ingest_feature_usage_events
from spark.jobs.bronze.user_signups.extract import ingest_user_signups
from spark.jobs.silver.feature_states.transformation import maintain_feature_states_scd
from spark.jobs.silver.feature_usage_facts.transformation import create_feature_usage_facts
from spark.jobs.silver.user_dim.transformation import maintain_user_dim


def setup_bronze_feature_releases(spark, temp_dir, releases, bronze_path=None):
    """Helper: ingest feature releases into bronze Delta table."""
    input_path = os.path.join(temp_dir, "feature_releases.json")
    with open(input_path, "w") as f:
        json.dump(releases, f)
    if bronze_path is None:
        bronze_path = os.path.join(temp_dir, "bronze")
    output_path = os.path.join(bronze_path, "feature_releases")
    ingest_feature_releases(spark, input_path, output_path)
    return bronze_path


def setup_bronze_user_signups(spark, temp_dir, signups, bronze_path=None):
    """Helper: ingest user signups into bronze Delta table."""
    input_path = os.path.join(temp_dir, "user_signups.jsonl")
    with open(input_path, "w") as f:
        for record in signups:
            f.write(json.dumps(record) + "\n")
    if bronze_path is None:
        bronze_path = os.path.join(temp_dir, "bronze")
    output_path = os.path.join(bronze_path, "user_signups")
    ingest_user_signups(spark, input_path, output_path)
    return bronze_path


def setup_bronze_conversions(spark, temp_dir, conversions, bronze_path=None):
    """Helper: ingest conversions into bronze Delta table."""
    input_path = os.path.join(temp_dir, "conversions.jsonl")
    with open(input_path, "w") as f:
        for record in conversions:
            f.write(json.dumps(record) + "\n")
    if bronze_path is None:
        bronze_path = os.path.join(temp_dir, "bronze")
    output_path = os.path.join(bronze_path, "conversions")
    ingest_conversions(spark, input_path, output_path)
    return bronze_path


def setup_bronze_feature_usage_events(spark, temp_dir, events, bronze_path=None):
    """Helper: ingest feature usage events into bronze Delta table."""
    input_path = os.path.join(temp_dir, "feature_usage_events.jsonl")
    with open(input_path, "w") as f:
        for record in events:
            f.write(json.dumps(record) + "\n")
    if bronze_path is None:
        bronze_path = os.path.join(temp_dir, "bronze")
    output_path = os.path.join(bronze_path, "feature_usage_events")
    ingest_feature_usage_events(spark, input_path, output_path)
    return bronze_path


def setup_full_pipeline(spark, temp_dir, releases, signups, conversions, events):
    """Helper: set up bronze + silver layers end-to-end, return paths."""
    bronze_path = os.path.join(temp_dir, "bronze")
    silver_path = os.path.join(temp_dir, "silver")
    gold_path = os.path.join(temp_dir, "gold")

    setup_bronze_feature_releases(spark, temp_dir, releases, bronze_path)
    setup_bronze_user_signups(spark, temp_dir, signups, bronze_path)
    setup_bronze_conversions(spark, temp_dir, conversions, bronze_path)
    setup_bronze_feature_usage_events(spark, temp_dir, events, bronze_path)

    maintain_feature_states_scd(spark, bronze_path, silver_path)
    maintain_user_dim(spark, bronze_path, silver_path)
    create_feature_usage_facts(spark, bronze_path, silver_path)

    return bronze_path, silver_path, gold_path
