"""
Unit tests for gold_cohort_analysis.py

Tests cover:
- Feature conversion impact cohort assignment logic
- Conversion rate calculations
- Avg days to convert and avg MRR aggregations
- Edge cases: no conversions, no feature usage, single feature
- Full gold aggregation orchestrator
"""

import json
import os

from pyspark.sql.functions import col

from spark.jobs.bronze.conversions.extract import ingest_conversions
from spark.jobs.bronze.feature_releases.extract import ingest_feature_releases
from spark.jobs.bronze.feature_usage_events.extract import ingest_feature_usage_events
from spark.jobs.bronze.user_signups.extract import ingest_user_signups
from spark.jobs.gold.feature_conversion_impact.aggregation import (
    calculate_feature_conversion_impact,
)
from spark.jobs.gold.main import run_gold_aggregation
from spark.jobs.silver.feature_states.transformation import maintain_feature_states_scd
from spark.jobs.silver.feature_usage_facts.transformation import create_feature_usage_facts
from spark.jobs.silver.user_dim.transformation import maintain_user_dim

# --- Helper functions to set up bronze + silver layers for gold tests ---


def _setup_bronze_feature_releases(spark, temp_dir, releases, bronze_path=None):
    """Helper: ingest feature releases into bronze Delta table."""
    input_path = os.path.join(temp_dir, "feature_releases.json")
    with open(input_path, "w") as f:
        json.dump(releases, f)
    if bronze_path is None:
        bronze_path = os.path.join(temp_dir, "bronze")
    output_path = os.path.join(bronze_path, "feature_releases")
    ingest_feature_releases(spark, input_path, output_path)
    return bronze_path


def _setup_bronze_user_signups(spark, temp_dir, signups, bronze_path=None):
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


def _setup_bronze_conversions(spark, temp_dir, conversions, bronze_path=None):
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


def _setup_bronze_feature_usage_events(spark, temp_dir, events, bronze_path=None):
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


def _setup_full_pipeline(spark, temp_dir, releases, signups, conversions, events):
    """Helper: set up bronze + silver layers end-to-end, return paths."""
    bronze_path = os.path.join(temp_dir, "bronze")
    silver_path = os.path.join(temp_dir, "silver")
    gold_path = os.path.join(temp_dir, "gold")

    _setup_bronze_feature_releases(spark, temp_dir, releases, bronze_path)
    _setup_bronze_user_signups(spark, temp_dir, signups, bronze_path)
    _setup_bronze_conversions(spark, temp_dir, conversions, bronze_path)
    _setup_bronze_feature_usage_events(spark, temp_dir, events, bronze_path)

    maintain_feature_states_scd(spark, bronze_path, silver_path)
    maintain_user_dim(spark, bronze_path, silver_path)
    create_feature_usage_facts(spark, bronze_path, silver_path)

    return bronze_path, silver_path, gold_path


class TestCalculateFeatureConversionImpact:
    """Test suite for calculate_feature_conversion_impact function"""

    def test_basic_cohort_assignment(self, spark, temp_dir):
        """Test that users are assigned to the correct cohorts."""
        releases = [
            {"id": 1, "name": "real_time_collab", "release_date": "2024-01-01", "version": "v1.0"},
        ]
        signups = [
            # User 1: will use feature and convert → used_before_conversion
            {
                "user_id": 1,
                "email": "u1@test.com",
                "signup_date": "2024-01-10",
                "company_size": "Small",
                "industry": "Tech",
            },
            # User 2: won't use feature, will convert → available_not_used
            {
                "user_id": 2,
                "email": "u2@test.com",
                "signup_date": "2024-01-15",
                "company_size": "Medium",
                "industry": "Finance",
            },
        ]
        conversions = [
            {
                "user_id": 1,
                "conversion_date": "2024-02-15",
                "plan": "Pro",
                "mrr": 99,
                "signup_date": "2024-01-10",
                "days_to_convert": 36,
                "used_real_time_collab": True,
            },
            {
                "user_id": 2,
                "conversion_date": "2024-02-20",
                "plan": "Pro",
                "mrr": 99,
                "signup_date": "2024-01-15",
                "days_to_convert": 36,
                "used_real_time_collab": False,
            },
        ]
        events = [
            # User 1 uses real_time_collab BEFORE conversion
            {
                "timestamp": "2024-01-20 10:00:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "real_time_collab",
                "event_type": "click",
            },
        ]

        bronze_path, silver_path, gold_path = _setup_full_pipeline(
            spark, temp_dir, releases, signups, conversions, events
        )

        row_count = calculate_feature_conversion_impact(spark, bronze_path, silver_path, gold_path)

        assert row_count > 0

        df = spark.read.format("delta").load(f"{gold_path}/gold_feature_conversion_impact")

        # User 1 should be in 'used_before_conversion' cohort
        used_cohort = df.filter(
            (col("feature_name") == "real_time_collab")
            & (col("cohort") == "used_before_conversion")
        ).collect()
        assert len(used_cohort) == 1
        assert used_cohort[0].total_users >= 1

        # User 2 should be in 'available_not_used' cohort
        available_cohort = df.filter(
            (col("feature_name") == "real_time_collab") & (col("cohort") == "available_not_used")
        ).collect()
        assert len(available_cohort) == 1
        assert available_cohort[0].total_users >= 1

    def test_output_schema(self, spark, temp_dir):
        """Test that the gold table has the expected schema."""
        releases = [
            {"id": 1, "name": "feature_a", "release_date": "2024-01-01", "version": "v1.0"},
        ]
        signups = [
            {
                "user_id": 1,
                "email": "u1@test.com",
                "signup_date": "2024-01-10",
                "company_size": "Small",
                "industry": "Tech",
            },
        ]
        conversions = [
            {
                "user_id": 1,
                "conversion_date": "2024-02-01",
                "plan": "Pro",
                "mrr": 99,
                "signup_date": "2024-01-10",
                "days_to_convert": 22,
                "used_real_time_collab": True,
            },
        ]
        events = [
            {
                "timestamp": "2024-01-15 10:00:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "feature_a",
                "event_type": "click",
            },
        ]

        bronze_path, silver_path, gold_path = _setup_full_pipeline(
            spark, temp_dir, releases, signups, conversions, events
        )
        calculate_feature_conversion_impact(spark, bronze_path, silver_path, gold_path)

        df = spark.read.format("delta").load(f"{gold_path}/gold_feature_conversion_impact")

        expected_columns = {
            "feature_name",
            "cohort",
            "total_users",
            "converted_users",
            "conversion_rate",
            "avg_days_to_convert",
            "avg_mrr",
        }
        assert set(df.columns) == expected_columns

        assert df.schema["feature_name"].dataType.typeName() == "string"
        assert df.schema["cohort"].dataType.typeName() == "string"
        assert df.schema["total_users"].dataType.typeName() == "long"
        assert df.schema["converted_users"].dataType.typeName() == "long"
        assert df.schema["conversion_rate"].dataType.typeName() == "double"
        assert df.schema["avg_days_to_convert"].dataType.typeName() == "double"
        assert df.schema["avg_mrr"].dataType.typeName() == "double"

    def test_conversion_rate_calculation(self, spark, temp_dir):
        """Test that conversion_rate is correctly calculated as converted/total."""
        releases = [
            {"id": 1, "name": "feature_x", "release_date": "2024-01-01", "version": "v1.0"},
        ]
        signups = [
            {
                "user_id": 1,
                "email": "u1@test.com",
                "signup_date": "2024-01-10",
                "company_size": "Small",
                "industry": "Tech",
            },
            {
                "user_id": 2,
                "email": "u2@test.com",
                "signup_date": "2024-01-12",
                "company_size": "Large",
                "industry": "Finance",
            },
        ]
        # Only user 1 converts
        conversions = [
            {
                "user_id": 1,
                "conversion_date": "2024-02-01",
                "plan": "Pro",
                "mrr": 99,
                "signup_date": "2024-01-10",
                "days_to_convert": 22,
                "used_real_time_collab": False,
            },
        ]
        events = []  # No feature usage → both users in 'available_not_used'

        bronze_path, silver_path, gold_path = _setup_full_pipeline(
            spark, temp_dir, releases, signups, conversions, events
        )
        calculate_feature_conversion_impact(spark, bronze_path, silver_path, gold_path)

        df = spark.read.format("delta").load(f"{gold_path}/gold_feature_conversion_impact")
        available_cohort = df.filter(
            (col("feature_name") == "feature_x") & (col("cohort") == "available_not_used")
        ).collect()

        assert len(available_cohort) == 1
        row = available_cohort[0]
        assert row.total_users == 2
        assert row.converted_users == 1
        # conversion_rate = 1/2 = 0.5
        assert abs(row.conversion_rate - 0.5) < 0.001

    def test_avg_days_to_convert(self, spark, temp_dir):
        """Test avg_days_to_convert calculation."""
        releases = [
            {"id": 1, "name": "feature_a", "release_date": "2024-01-01", "version": "v1.0"},
        ]
        signups = [
            {
                "user_id": 1,
                "email": "u1@test.com",
                "signup_date": "2024-01-10",
                "company_size": "Small",
                "industry": "Tech",
            },
            {
                "user_id": 2,
                "email": "u2@test.com",
                "signup_date": "2024-01-10",
                "company_size": "Medium",
                "industry": "Finance",
            },
        ]
        conversions = [
            {
                "user_id": 1,
                "conversion_date": "2024-01-20",
                "plan": "Pro",
                "mrr": 99,
                "signup_date": "2024-01-10",
                "days_to_convert": 10,
                "used_real_time_collab": False,
            },
            {
                "user_id": 2,
                "conversion_date": "2024-01-30",
                "plan": "Pro",
                "mrr": 49,
                "signup_date": "2024-01-10",
                "days_to_convert": 20,
                "used_real_time_collab": False,
            },
        ]
        events = []  # No feature usage

        bronze_path, silver_path, gold_path = _setup_full_pipeline(
            spark, temp_dir, releases, signups, conversions, events
        )
        calculate_feature_conversion_impact(spark, bronze_path, silver_path, gold_path)

        df = spark.read.format("delta").load(f"{gold_path}/gold_feature_conversion_impact")
        row = df.filter(
            (col("feature_name") == "feature_a") & (col("cohort") == "available_not_used")
        ).collect()[0]

        # avg_days_to_convert = avg(10, 20) = 15.0
        assert abs(row.avg_days_to_convert - 15.0) < 0.001

    def test_avg_mrr(self, spark, temp_dir):
        """Test avg_mrr calculation for converted users."""
        releases = [
            {"id": 1, "name": "feature_a", "release_date": "2024-01-01", "version": "v1.0"},
        ]
        signups = [
            {
                "user_id": 1,
                "email": "u1@test.com",
                "signup_date": "2024-01-10",
                "company_size": "Small",
                "industry": "Tech",
            },
            {
                "user_id": 2,
                "email": "u2@test.com",
                "signup_date": "2024-01-10",
                "company_size": "Large",
                "industry": "Finance",
            },
        ]
        conversions = [
            {
                "user_id": 1,
                "conversion_date": "2024-02-01",
                "plan": "Pro",
                "mrr": 99,
                "signup_date": "2024-01-10",
                "days_to_convert": 22,
                "used_real_time_collab": False,
            },
            {
                "user_id": 2,
                "conversion_date": "2024-02-05",
                "plan": "Enterprise",
                "mrr": 299,
                "signup_date": "2024-01-10",
                "days_to_convert": 26,
                "used_real_time_collab": False,
            },
        ]
        events = []

        bronze_path, silver_path, gold_path = _setup_full_pipeline(
            spark, temp_dir, releases, signups, conversions, events
        )
        calculate_feature_conversion_impact(spark, bronze_path, silver_path, gold_path)

        df = spark.read.format("delta").load(f"{gold_path}/gold_feature_conversion_impact")
        row = df.filter(
            (col("feature_name") == "feature_a") & (col("cohort") == "available_not_used")
        ).collect()[0]

        # avg_mrr = avg(99, 299) = 199.0
        assert abs(row.avg_mrr - 199.0) < 0.001

    def test_non_converted_users_count(self, spark, temp_dir):
        """Test that non-converted users are counted in total_users but not converted_users."""
        releases = [
            {"id": 1, "name": "feature_a", "release_date": "2024-01-01", "version": "v1.0"},
        ]
        signups = [
            {
                "user_id": 1,
                "email": "u1@test.com",
                "signup_date": "2024-01-10",
                "company_size": "Small",
                "industry": "Tech",
            },
            {
                "user_id": 2,
                "email": "u2@test.com",
                "signup_date": "2024-01-12",
                "company_size": "Medium",
                "industry": "Finance",
            },
            {
                "user_id": 3,
                "email": "u3@test.com",
                "signup_date": "2024-01-14",
                "company_size": "Large",
                "industry": "Healthcare",
            },
        ]
        # Only user 1 converts
        conversions = [
            {
                "user_id": 1,
                "conversion_date": "2024-02-01",
                "plan": "Pro",
                "mrr": 99,
                "signup_date": "2024-01-10",
                "days_to_convert": 22,
                "used_real_time_collab": False,
            },
        ]
        events = []

        bronze_path, silver_path, gold_path = _setup_full_pipeline(
            spark, temp_dir, releases, signups, conversions, events
        )
        calculate_feature_conversion_impact(spark, bronze_path, silver_path, gold_path)

        df = spark.read.format("delta").load(f"{gold_path}/gold_feature_conversion_impact")
        row = df.filter(
            (col("feature_name") == "feature_a") & (col("cohort") == "available_not_used")
        ).collect()[0]

        assert row.total_users == 3
        assert row.converted_users == 1

    def test_multiple_features(self, spark, temp_dir):
        """Test cohort analysis across multiple features."""
        releases = [
            {"id": 1, "name": "real_time_collab", "release_date": "2024-01-01", "version": "v1.0"},
            {"id": 2, "name": "ai_insights", "release_date": "2024-01-01", "version": "v1.0"},
        ]
        signups = [
            {
                "user_id": 1,
                "email": "u1@test.com",
                "signup_date": "2024-01-10",
                "company_size": "Small",
                "industry": "Tech",
            },
        ]
        conversions = [
            {
                "user_id": 1,
                "conversion_date": "2024-02-01",
                "plan": "Pro",
                "mrr": 99,
                "signup_date": "2024-01-10",
                "days_to_convert": 22,
                "used_real_time_collab": True,
            },
        ]
        # User 1 uses only real_time_collab
        events = [
            {
                "timestamp": "2024-01-15 10:00:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "real_time_collab",
                "event_type": "click",
            },
        ]

        bronze_path, silver_path, gold_path = _setup_full_pipeline(
            spark, temp_dir, releases, signups, conversions, events
        )
        calculate_feature_conversion_impact(spark, bronze_path, silver_path, gold_path)

        df = spark.read.format("delta").load(f"{gold_path}/gold_feature_conversion_impact")

        # real_time_collab: user 1 should be used_before_conversion
        rtc_used = df.filter(
            (col("feature_name") == "real_time_collab")
            & (col("cohort") == "used_before_conversion")
        ).collect()
        assert len(rtc_used) == 1

        # ai_insights: user 1 should be available_not_used (available but didn't use)
        ai_available = df.filter(
            (col("feature_name") == "ai_insights") & (col("cohort") == "available_not_used")
        ).collect()
        assert len(ai_available) == 1

    def test_used_before_conversion_has_higher_signal(self, spark, temp_dir):
        """
        Test the core insight: users who use a feature before converting
        should show as a distinct cohort that can be compared.
        """
        releases = [
            {"id": 1, "name": "real_time_collab", "release_date": "2024-01-01", "version": "v1.0"},
        ]
        signups = [
            # 3 users who will use feature and convert
            {
                "user_id": 1,
                "email": "u1@test.com",
                "signup_date": "2024-01-10",
                "company_size": "Small",
                "industry": "Tech",
            },
            {
                "user_id": 2,
                "email": "u2@test.com",
                "signup_date": "2024-01-10",
                "company_size": "Medium",
                "industry": "Tech",
            },
            {
                "user_id": 3,
                "email": "u3@test.com",
                "signup_date": "2024-01-10",
                "company_size": "Large",
                "industry": "Tech",
            },
            # 3 users who won't use feature (1 converts, 2 don't)
            {
                "user_id": 4,
                "email": "u4@test.com",
                "signup_date": "2024-01-10",
                "company_size": "Small",
                "industry": "Finance",
            },
            {
                "user_id": 5,
                "email": "u5@test.com",
                "signup_date": "2024-01-10",
                "company_size": "Medium",
                "industry": "Finance",
            },
            {
                "user_id": 6,
                "email": "u6@test.com",
                "signup_date": "2024-01-10",
                "company_size": "Large",
                "industry": "Finance",
            },
        ]
        conversions = [
            # All 3 feature users convert
            {
                "user_id": 1,
                "conversion_date": "2024-02-01",
                "plan": "Pro",
                "mrr": 99,
                "signup_date": "2024-01-10",
                "days_to_convert": 22,
                "used_real_time_collab": True,
            },
            {
                "user_id": 2,
                "conversion_date": "2024-02-05",
                "plan": "Pro",
                "mrr": 99,
                "signup_date": "2024-01-10",
                "days_to_convert": 26,
                "used_real_time_collab": True,
            },
            {
                "user_id": 3,
                "conversion_date": "2024-02-10",
                "plan": "Enterprise",
                "mrr": 299,
                "signup_date": "2024-01-10",
                "days_to_convert": 31,
                "used_real_time_collab": True,
            },
            # Only 1 non-feature user converts
            {
                "user_id": 4,
                "conversion_date": "2024-03-01",
                "plan": "Pro",
                "mrr": 49,
                "signup_date": "2024-01-10",
                "days_to_convert": 50,
                "used_real_time_collab": False,
            },
        ]
        events = [
            {
                "timestamp": "2024-01-15 10:00:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "real_time_collab",
                "event_type": "click",
            },
            {
                "timestamp": "2024-01-16 10:00:00",
                "user_id": 2,
                "feature_id": 1,
                "feature_name": "real_time_collab",
                "event_type": "click",
            },
            {
                "timestamp": "2024-01-17 10:00:00",
                "user_id": 3,
                "feature_id": 1,
                "feature_name": "real_time_collab",
                "event_type": "click",
            },
        ]

        bronze_path, silver_path, gold_path = _setup_full_pipeline(
            spark, temp_dir, releases, signups, conversions, events
        )
        calculate_feature_conversion_impact(spark, bronze_path, silver_path, gold_path)

        df = spark.read.format("delta").load(f"{gold_path}/gold_feature_conversion_impact")

        used_cohort = df.filter(
            (col("feature_name") == "real_time_collab")
            & (col("cohort") == "used_before_conversion")
        ).collect()[0]

        available_cohort = df.filter(
            (col("feature_name") == "real_time_collab") & (col("cohort") == "available_not_used")
        ).collect()[0]

        # used_before_conversion: 3 users, all convert → 100% conversion
        assert used_cohort.total_users == 3
        assert used_cohort.converted_users == 3
        assert abs(used_cohort.conversion_rate - 1.0) < 0.001

        # available_not_used: 3 users, 1 converts → 33% conversion
        assert available_cohort.total_users == 3
        assert available_cohort.converted_users == 1
        assert abs(available_cohort.conversion_rate - (1.0 / 3.0)) < 0.001

        # The core insight: feature users convert at a higher rate
        assert used_cohort.conversion_rate > available_cohort.conversion_rate

    def test_overwrite_mode(self, spark, temp_dir):
        """Test that re-running overwrites previous results."""
        releases = [
            {"id": 1, "name": "feature_a", "release_date": "2024-01-01", "version": "v1.0"},
        ]
        signups = [
            {
                "user_id": 1,
                "email": "u1@test.com",
                "signup_date": "2024-01-10",
                "company_size": "Small",
                "industry": "Tech",
            },
        ]
        conversions = [
            {
                "user_id": 1,
                "conversion_date": "2024-02-01",
                "plan": "Pro",
                "mrr": 99,
                "signup_date": "2024-01-10",
                "days_to_convert": 22,
                "used_real_time_collab": False,
            },
        ]
        events = []

        bronze_path, silver_path, gold_path = _setup_full_pipeline(
            spark, temp_dir, releases, signups, conversions, events
        )

        # Run twice
        calculate_feature_conversion_impact(spark, bronze_path, silver_path, gold_path)
        count_1 = (
            spark.read.format("delta").load(f"{gold_path}/gold_feature_conversion_impact").count()
        )

        calculate_feature_conversion_impact(spark, bronze_path, silver_path, gold_path)
        count_2 = (
            spark.read.format("delta").load(f"{gold_path}/gold_feature_conversion_impact").count()
        )

        # Overwrite mode: counts should be the same, not doubled
        assert count_1 == count_2

    def test_avg_days_to_convert_only_for_converted(self, spark, temp_dir):
        """Test that avg_days_to_convert only considers converted users (nulls excluded)."""
        releases = [
            {"id": 1, "name": "feature_a", "release_date": "2024-01-01", "version": "v1.0"},
        ]
        signups = [
            {
                "user_id": 1,
                "email": "u1@test.com",
                "signup_date": "2024-01-10",
                "company_size": "Small",
                "industry": "Tech",
            },
            {
                "user_id": 2,
                "email": "u2@test.com",
                "signup_date": "2024-01-10",
                "company_size": "Medium",
                "industry": "Finance",
            },
        ]
        # Only user 1 converts after 10 days
        conversions = [
            {
                "user_id": 1,
                "conversion_date": "2024-01-20",
                "plan": "Pro",
                "mrr": 99,
                "signup_date": "2024-01-10",
                "days_to_convert": 10,
                "used_real_time_collab": False,
            },
        ]
        events = []

        bronze_path, silver_path, gold_path = _setup_full_pipeline(
            spark, temp_dir, releases, signups, conversions, events
        )
        calculate_feature_conversion_impact(spark, bronze_path, silver_path, gold_path)

        df = spark.read.format("delta").load(f"{gold_path}/gold_feature_conversion_impact")
        row = df.filter(
            (col("feature_name") == "feature_a") & (col("cohort") == "available_not_used")
        ).collect()[0]

        # avg_days_to_convert should be 10 (only the converted user counts)
        assert abs(row.avg_days_to_convert - 10.0) < 0.001


class TestRunGoldAggregation:
    """Test suite for run_gold_aggregation orchestrator function"""

    def test_run_gold_aggregation_returns_stats(self, spark, temp_dir):
        """Test that run_gold_aggregation returns correct statistics."""
        releases = [
            {"id": 1, "name": "feature_a", "release_date": "2024-01-01", "version": "v1.0"},
        ]
        signups = [
            {
                "user_id": 1,
                "email": "u1@test.com",
                "signup_date": "2024-01-10",
                "company_size": "Small",
                "industry": "Tech",
            },
        ]
        conversions = [
            {
                "user_id": 1,
                "conversion_date": "2024-02-01",
                "plan": "Pro",
                "mrr": 99,
                "signup_date": "2024-01-10",
                "days_to_convert": 22,
                "used_real_time_collab": False,
            },
        ]
        events = [
            {
                "timestamp": "2024-01-15 10:00:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "feature_a",
                "event_type": "click",
            },
        ]

        bronze_path, silver_path, gold_path = _setup_full_pipeline(
            spark, temp_dir, releases, signups, conversions, events
        )

        stats = run_gold_aggregation(spark, bronze_path, silver_path, gold_path)

        assert isinstance(stats, dict)
        assert "feature_conversion_impact" in stats
        assert stats["feature_conversion_impact"] > 0

    def test_run_gold_aggregation_creates_table(self, spark, temp_dir):
        """Test that run_gold_aggregation creates the gold Delta table."""
        releases = [
            {"id": 1, "name": "feature_a", "release_date": "2024-01-01", "version": "v1.0"},
        ]
        signups = [
            {
                "user_id": 1,
                "email": "u1@test.com",
                "signup_date": "2024-01-10",
                "company_size": "Small",
                "industry": "Tech",
            },
        ]
        conversions = [
            {
                "user_id": 1,
                "conversion_date": "2024-02-01",
                "plan": "Pro",
                "mrr": 99,
                "signup_date": "2024-01-10",
                "days_to_convert": 22,
                "used_real_time_collab": False,
            },
        ]
        events = []

        bronze_path, silver_path, gold_path = _setup_full_pipeline(
            spark, temp_dir, releases, signups, conversions, events
        )

        run_gold_aggregation(spark, bronze_path, silver_path, gold_path)

        table_path = os.path.join(gold_path, "gold_feature_conversion_impact")
        assert os.path.exists(table_path)
        df = spark.read.format("delta").load(table_path)
        assert df.count() > 0
