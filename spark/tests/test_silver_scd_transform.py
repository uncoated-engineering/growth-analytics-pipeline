"""
Unit tests for silver_scd_transform.py

Tests cover:
- SCD Type 2 feature states (initial load, change detection, new features)
- User dimension table (plan enrichment, deduplication)
- Feature usage facts (aggregation, avg_daily_usage calculation)
- Full silver transformation orchestrator
"""

import json
import os
from datetime import date

from pyspark.sql.functions import col

from spark.jobs.bronze_ingestion import (
    ingest_conversions,
    ingest_feature_releases,
    ingest_feature_usage_events,
    ingest_user_signups,
)
from spark.jobs.silver_scd_transform import (
    create_feature_usage_facts,
    maintain_feature_states_scd,
    maintain_user_dim,
    run_silver_transformation,
)


def _setup_bronze_feature_releases(spark, temp_dir, releases):
    """Helper: ingest feature releases into bronze Delta table."""
    input_path = os.path.join(temp_dir, "feature_releases.json")
    with open(input_path, "w") as f:
        json.dump(releases, f)
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


class TestMaintainFeatureStatesScd:
    """Test suite for maintain_feature_states_scd function"""

    def test_initial_load_creates_scd_records(self, spark, temp_dir):
        """Test that the first run creates initial SCD records for all features."""
        releases = [
            {"id": 1, "name": "real_time_collab", "release_date": "2024-03-01", "version": "v1.0"},
            {"id": 2, "name": "ai_insights", "release_date": "2024-05-01", "version": "v1.0"},
        ]
        bronze_path = _setup_bronze_feature_releases(spark, temp_dir, releases)
        silver_path = os.path.join(temp_dir, "silver")

        row_count = maintain_feature_states_scd(spark, bronze_path, silver_path)

        assert row_count == 2

        df = spark.read.format("delta").load(f"{silver_path}/silver_feature_states")
        assert df.count() == 2

        # All records should be current
        assert df.filter(col("is_current") == True).count() == 2  # noqa: E712

        # All records should be enabled
        assert df.filter(col("is_enabled") == True).count() == 2  # noqa: E712

        # effective_to should be 9999-12-31
        end_of_time = date(9999, 12, 31)
        rows = df.collect()
        for row in rows:
            assert row.effective_to == end_of_time

    def test_initial_load_schema(self, spark, temp_dir):
        """Test that the SCD table has the correct schema."""
        releases = [
            {"id": 1, "name": "feature_a", "release_date": "2024-01-15", "version": "v1.0"},
        ]
        bronze_path = _setup_bronze_feature_releases(spark, temp_dir, releases)
        silver_path = os.path.join(temp_dir, "silver")

        maintain_feature_states_scd(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_feature_states")
        expected_columns = {
            "feature_id",
            "feature_name",
            "version",
            "is_enabled",
            "effective_from",
            "effective_to",
            "is_current",
            "record_hash",
        }
        assert set(df.columns) == expected_columns

        # Verify data types
        assert df.schema["feature_id"].dataType.typeName() == "integer"
        assert df.schema["feature_name"].dataType.typeName() == "string"
        assert df.schema["version"].dataType.typeName() == "string"
        assert df.schema["is_enabled"].dataType.typeName() == "boolean"
        assert df.schema["effective_from"].dataType.typeName() == "date"
        assert df.schema["effective_to"].dataType.typeName() == "date"
        assert df.schema["is_current"].dataType.typeName() == "boolean"
        assert df.schema["record_hash"].dataType.typeName() == "string"

    def test_effective_from_matches_release_date(self, spark, temp_dir):
        """Test that effective_from is set to the feature's release date."""
        releases = [
            {"id": 1, "name": "real_time_collab", "release_date": "2024-03-01", "version": "v1.0"},
        ]
        bronze_path = _setup_bronze_feature_releases(spark, temp_dir, releases)
        silver_path = os.path.join(temp_dir, "silver")

        maintain_feature_states_scd(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_feature_states")
        row = df.collect()[0]
        assert row.effective_from == date(2024, 3, 1)

    def test_record_hash_generated(self, spark, temp_dir):
        """Test that record_hash is computed from feature_name + version."""
        releases = [
            {"id": 1, "name": "feature_a", "release_date": "2024-01-15", "version": "v1.0"},
        ]
        bronze_path = _setup_bronze_feature_releases(spark, temp_dir, releases)
        silver_path = os.path.join(temp_dir, "silver")

        maintain_feature_states_scd(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_feature_states")
        row = df.collect()[0]
        assert row.record_hash is not None
        assert len(row.record_hash) == 32  # MD5 hex digest length

    def test_scd_type2_version_change_closes_old_record(self, spark, temp_dir):
        """Test that updating a feature version closes the old record and creates a new one."""
        # Initial load with v1.0
        releases_v1 = [
            {"id": 1, "name": "real_time_collab", "release_date": "2024-03-01", "version": "v1.0"},
        ]
        bronze_path = _setup_bronze_feature_releases(spark, temp_dir, releases_v1)
        silver_path = os.path.join(temp_dir, "silver")
        maintain_feature_states_scd(spark, bronze_path, silver_path)

        # Now simulate a version update: re-create bronze with v2.0
        # Clear old bronze data and write new version
        import shutil

        shutil.rmtree(os.path.join(bronze_path, "feature_releases"))
        releases_v2 = [
            {"id": 1, "name": "real_time_collab", "release_date": "2024-06-01", "version": "v2.0"},
        ]
        input_path = os.path.join(temp_dir, "feature_releases_v2.json")
        with open(input_path, "w") as f:
            json.dump(releases_v2, f)
        ingest_feature_releases(spark, input_path, os.path.join(bronze_path, "feature_releases"))

        # Run SCD again
        maintain_feature_states_scd(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_feature_states")

        # Should now have 2 records for feature_id=1
        assert df.count() == 2

        # Old record should be closed (is_current=False)
        old_record = df.filter((col("feature_id") == 1) & (col("version") == "v1.0")).collect()[0]
        assert old_record.is_current is False
        assert old_record.effective_to == date(2024, 6, 1)

        # New record should be current
        new_record = df.filter((col("feature_id") == 1) & (col("version") == "v2.0")).collect()[0]
        assert new_record.is_current is True
        assert new_record.effective_from == date(2024, 6, 1)
        assert new_record.effective_to == date(9999, 12, 31)

    def test_scd_new_feature_added(self, spark, temp_dir):
        """Test adding a brand new feature after initial load."""
        # Initial load with feature 1
        releases_initial = [
            {"id": 1, "name": "real_time_collab", "release_date": "2024-03-01", "version": "v1.0"},
        ]
        bronze_path = _setup_bronze_feature_releases(spark, temp_dir, releases_initial)
        silver_path = os.path.join(temp_dir, "silver")
        maintain_feature_states_scd(spark, bronze_path, silver_path)

        # Add feature 2
        import shutil

        shutil.rmtree(os.path.join(bronze_path, "feature_releases"))
        releases_with_new = [
            {"id": 1, "name": "real_time_collab", "release_date": "2024-03-01", "version": "v1.0"},
            {"id": 2, "name": "ai_insights", "release_date": "2024-05-01", "version": "v1.0"},
        ]
        input_path = os.path.join(temp_dir, "feature_releases_new.json")
        with open(input_path, "w") as f:
            json.dump(releases_with_new, f)
        ingest_feature_releases(spark, input_path, os.path.join(bronze_path, "feature_releases"))

        maintain_feature_states_scd(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_feature_states")

        # Should have 2 records total (1 existing + 1 new)
        assert df.count() == 2

        # Both should be current
        assert df.filter(col("is_current") == True).count() == 2  # noqa: E712

        # Verify the new feature
        new_feature = df.filter(col("feature_id") == 2).collect()[0]
        assert new_feature.feature_name == "ai_insights"
        assert new_feature.effective_from == date(2024, 5, 1)
        assert new_feature.is_current is True

    def test_scd_no_change_is_idempotent(self, spark, temp_dir):
        """Test that running SCD with unchanged data doesn't create duplicate records."""
        releases = [
            {"id": 1, "name": "real_time_collab", "release_date": "2024-03-01", "version": "v1.0"},
        ]
        bronze_path = _setup_bronze_feature_releases(spark, temp_dir, releases)
        silver_path = os.path.join(temp_dir, "silver")

        # Run twice with same data
        maintain_feature_states_scd(spark, bronze_path, silver_path)
        maintain_feature_states_scd(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_feature_states")
        assert df.count() == 1
        assert df.filter(col("is_current") == True).count() == 1  # noqa: E712

    def test_time_travel_query(self, spark, temp_dir):
        """Test that SCD Type 2 structure supports time-travel queries."""
        # Load two features released at different times
        releases = [
            {"id": 1, "name": "real_time_collab", "release_date": "2024-03-01", "version": "v1.0"},
            {"id": 2, "name": "ai_insights", "release_date": "2024-05-01", "version": "v1.0"},
        ]
        bronze_path = _setup_bronze_feature_releases(spark, temp_dir, releases)
        silver_path = os.path.join(temp_dir, "silver")
        maintain_feature_states_scd(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_feature_states")

        # Query: "What features were available on 2024-04-01?"
        features_on_april = df.filter(
            (col("effective_from") <= "2024-04-01") & (col("effective_to") > "2024-04-01")
        )

        # Only real_time_collab should be available (released March 2024)
        assert features_on_april.count() == 1
        assert features_on_april.collect()[0].feature_name == "real_time_collab"

        # Query: "What features were available on 2024-06-01?"
        features_on_june = df.filter(
            (col("effective_from") <= "2024-06-01") & (col("effective_to") > "2024-06-01")
        )

        # Both features should be available
        assert features_on_june.count() == 2


class TestMaintainUserDim:
    """Test suite for maintain_user_dim function"""

    def test_user_dim_basic(self, spark, temp_dir):
        """Test basic user dimension creation."""
        signups = [
            {
                "user_id": 1,
                "email": "user1@test.com",
                "signup_date": "2024-01-15",
                "company_size": "Small",
                "industry": "Tech",
            },
            {
                "user_id": 2,
                "email": "user2@test.com",
                "signup_date": "2024-01-20",
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
                "signup_date": "2024-01-15",
                "days_to_convert": 17,
                "used_real_time_collab": True,
            },
        ]

        bronze_path = os.path.join(temp_dir, "bronze")
        _setup_bronze_user_signups(spark, temp_dir, signups, bronze_path)
        _setup_bronze_conversions(spark, temp_dir, conversions, bronze_path)
        silver_path = os.path.join(temp_dir, "silver")

        row_count = maintain_user_dim(spark, bronze_path, silver_path)

        assert row_count == 2

        df = spark.read.format("delta").load(f"{silver_path}/silver_user_dim")
        assert df.count() == 2

    def test_user_dim_schema(self, spark, temp_dir):
        """Test user dimension schema."""
        signups = [
            {
                "user_id": 1,
                "email": "user1@test.com",
                "signup_date": "2024-01-15",
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
                "signup_date": "2024-01-15",
                "days_to_convert": 17,
                "used_real_time_collab": True,
            },
        ]

        bronze_path = os.path.join(temp_dir, "bronze")
        _setup_bronze_user_signups(spark, temp_dir, signups, bronze_path)
        _setup_bronze_conversions(spark, temp_dir, conversions, bronze_path)
        silver_path = os.path.join(temp_dir, "silver")

        maintain_user_dim(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_user_dim")
        expected_columns = {
            "user_id",
            "signup_date",
            "company_size",
            "industry",
            "current_plan",
        }
        assert set(df.columns) == expected_columns

        assert df.schema["user_id"].dataType.typeName() == "integer"
        assert df.schema["signup_date"].dataType.typeName() == "date"
        assert df.schema["company_size"].dataType.typeName() == "string"
        assert df.schema["industry"].dataType.typeName() == "string"
        assert df.schema["current_plan"].dataType.typeName() == "string"

    def test_user_dim_converted_user_has_plan(self, spark, temp_dir):
        """Test that converted users get their plan from conversions."""
        signups = [
            {
                "user_id": 1,
                "email": "user1@test.com",
                "signup_date": "2024-01-15",
                "company_size": "Small",
                "industry": "Tech",
            },
        ]
        conversions = [
            {
                "user_id": 1,
                "conversion_date": "2024-02-01",
                "plan": "Enterprise",
                "mrr": 299,
                "signup_date": "2024-01-15",
                "days_to_convert": 17,
                "used_real_time_collab": True,
            },
        ]

        bronze_path = os.path.join(temp_dir, "bronze")
        _setup_bronze_user_signups(spark, temp_dir, signups, bronze_path)
        _setup_bronze_conversions(spark, temp_dir, conversions, bronze_path)
        silver_path = os.path.join(temp_dir, "silver")

        maintain_user_dim(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_user_dim")
        row = df.filter(col("user_id") == 1).collect()[0]
        assert row.current_plan == "Enterprise"

    def test_user_dim_non_converted_user_defaults_to_free(self, spark, temp_dir):
        """Test that non-converted users default to 'free' plan."""
        signups = [
            {
                "user_id": 1,
                "email": "user1@test.com",
                "signup_date": "2024-01-15",
                "company_size": "Small",
                "industry": "Tech",
            },
            {
                "user_id": 2,
                "email": "user2@test.com",
                "signup_date": "2024-01-20",
                "company_size": "Medium",
                "industry": "Finance",
            },
        ]
        conversions = [
            {
                "user_id": 1,
                "conversion_date": "2024-02-01",
                "plan": "Pro",
                "mrr": 99,
                "signup_date": "2024-01-15",
                "days_to_convert": 17,
                "used_real_time_collab": True,
            },
        ]

        bronze_path = os.path.join(temp_dir, "bronze")
        _setup_bronze_user_signups(spark, temp_dir, signups, bronze_path)
        _setup_bronze_conversions(spark, temp_dir, conversions, bronze_path)
        silver_path = os.path.join(temp_dir, "silver")

        maintain_user_dim(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_user_dim")

        # User 1 has Pro plan
        user1 = df.filter(col("user_id") == 1).collect()[0]
        assert user1.current_plan == "Pro"

        # User 2 defaults to free
        user2 = df.filter(col("user_id") == 2).collect()[0]
        assert user2.current_plan == "free"

    def test_user_dim_signup_date_is_date_type(self, spark, temp_dir):
        """Test that signup_date is converted to proper date type."""
        signups = [
            {
                "user_id": 1,
                "email": "user1@test.com",
                "signup_date": "2024-01-15",
                "company_size": "Small",
                "industry": "Tech",
            },
        ]
        conversions = []  # No conversions

        bronze_path = os.path.join(temp_dir, "bronze")
        _setup_bronze_user_signups(spark, temp_dir, signups, bronze_path)
        _setup_bronze_conversions(spark, temp_dir, conversions, bronze_path)
        silver_path = os.path.join(temp_dir, "silver")

        maintain_user_dim(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_user_dim")
        row = df.collect()[0]
        assert row.signup_date == date(2024, 1, 15)

    def test_user_dim_excludes_email(self, spark, temp_dir):
        """Test that email is not included in the dimension (PII minimization)."""
        signups = [
            {
                "user_id": 1,
                "email": "user1@test.com",
                "signup_date": "2024-01-15",
                "company_size": "Small",
                "industry": "Tech",
            },
        ]
        conversions = []

        bronze_path = os.path.join(temp_dir, "bronze")
        _setup_bronze_user_signups(spark, temp_dir, signups, bronze_path)
        _setup_bronze_conversions(spark, temp_dir, conversions, bronze_path)
        silver_path = os.path.join(temp_dir, "silver")

        maintain_user_dim(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_user_dim")
        assert "email" not in df.columns


class TestCreateFeatureUsageFacts:
    """Test suite for create_feature_usage_facts function"""

    def test_feature_usage_facts_basic(self, spark, temp_dir):
        """Test basic feature usage fact aggregation."""
        events = [
            {
                "timestamp": "2024-01-15 10:30:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "Collaboration",
                "event_type": "click",
            },
            {
                "timestamp": "2024-01-16 11:00:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "Collaboration",
                "event_type": "click",
            },
            {
                "timestamp": "2024-01-15 14:00:00",
                "user_id": 2,
                "feature_id": 2,
                "feature_name": "Analytics",
                "event_type": "view",
            },
        ]

        bronze_path = os.path.join(temp_dir, "bronze")
        _setup_bronze_feature_usage_events(spark, temp_dir, events, bronze_path)
        silver_path = os.path.join(temp_dir, "silver")

        row_count = create_feature_usage_facts(spark, bronze_path, silver_path)

        # 2 unique user-feature combinations
        assert row_count == 2

        df = spark.read.format("delta").load(f"{silver_path}/silver_feature_usage_facts")
        assert df.count() == 2

    def test_feature_usage_facts_schema(self, spark, temp_dir):
        """Test feature usage facts schema."""
        events = [
            {
                "timestamp": "2024-01-15 10:30:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "Collaboration",
                "event_type": "click",
            },
        ]

        bronze_path = os.path.join(temp_dir, "bronze")
        _setup_bronze_feature_usage_events(spark, temp_dir, events, bronze_path)
        silver_path = os.path.join(temp_dir, "silver")

        create_feature_usage_facts(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_feature_usage_facts")
        expected_columns = {
            "user_id",
            "feature_id",
            "first_used_date",
            "last_used_date",
            "total_usage_count",
            "avg_daily_usage",
            "as_of_date",
        }
        assert set(df.columns) == expected_columns

        assert df.schema["user_id"].dataType.typeName() == "integer"
        assert df.schema["feature_id"].dataType.typeName() == "integer"
        assert df.schema["first_used_date"].dataType.typeName() == "date"
        assert df.schema["last_used_date"].dataType.typeName() == "date"
        assert df.schema["total_usage_count"].dataType.typeName() == "long"
        assert df.schema["avg_daily_usage"].dataType.typeName() == "double"
        assert df.schema["as_of_date"].dataType.typeName() == "date"

    def test_feature_usage_facts_aggregation(self, spark, temp_dir):
        """Test that usage counts and dates are correctly aggregated."""
        events = [
            {
                "timestamp": "2024-01-15 10:30:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "Collaboration",
                "event_type": "click",
            },
            {
                "timestamp": "2024-01-16 11:00:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "Collaboration",
                "event_type": "click",
            },
            {
                "timestamp": "2024-01-17 09:00:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "Collaboration",
                "event_type": "usage",
            },
        ]

        bronze_path = os.path.join(temp_dir, "bronze")
        _setup_bronze_feature_usage_events(spark, temp_dir, events, bronze_path)
        silver_path = os.path.join(temp_dir, "silver")

        create_feature_usage_facts(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_feature_usage_facts")
        row = df.filter((col("user_id") == 1) & (col("feature_id") == 1)).collect()[0]

        assert row.total_usage_count == 3
        assert row.first_used_date == date(2024, 1, 15)
        assert row.last_used_date == date(2024, 1, 17)

    def test_feature_usage_facts_avg_daily_usage(self, spark, temp_dir):
        """Test avg_daily_usage calculation: total_count / (date_span + 1)."""
        events = [
            {
                "timestamp": "2024-01-15 10:00:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "Collaboration",
                "event_type": "click",
            },
            {
                "timestamp": "2024-01-15 11:00:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "Collaboration",
                "event_type": "click",
            },
            {
                "timestamp": "2024-01-16 10:00:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "Collaboration",
                "event_type": "click",
            },
            {
                "timestamp": "2024-01-17 10:00:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "Collaboration",
                "event_type": "click",
            },
        ]

        bronze_path = os.path.join(temp_dir, "bronze")
        _setup_bronze_feature_usage_events(spark, temp_dir, events, bronze_path)
        silver_path = os.path.join(temp_dir, "silver")

        create_feature_usage_facts(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_feature_usage_facts")
        row = df.collect()[0]

        # 4 events over 3 days (Jan 15-17) = 4/3 = 1.333...
        assert abs(row.avg_daily_usage - (4.0 / 3.0)) < 0.001

    def test_feature_usage_facts_single_day_usage(self, spark, temp_dir):
        """Test avg_daily_usage when all events are on the same day."""
        events = [
            {
                "timestamp": "2024-01-15 10:00:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "Collaboration",
                "event_type": "click",
            },
            {
                "timestamp": "2024-01-15 11:00:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "Collaboration",
                "event_type": "click",
            },
        ]

        bronze_path = os.path.join(temp_dir, "bronze")
        _setup_bronze_feature_usage_events(spark, temp_dir, events, bronze_path)
        silver_path = os.path.join(temp_dir, "silver")

        create_feature_usage_facts(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_feature_usage_facts")
        row = df.collect()[0]

        # 2 events on 1 day: 2/1 = 2.0
        assert row.avg_daily_usage == 2.0

    def test_feature_usage_facts_multiple_users(self, spark, temp_dir):
        """Test that facts are correctly separated by user."""
        events = [
            {
                "timestamp": "2024-01-15 10:00:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "Collaboration",
                "event_type": "click",
            },
            {
                "timestamp": "2024-01-15 10:00:00",
                "user_id": 2,
                "feature_id": 1,
                "feature_name": "Collaboration",
                "event_type": "click",
            },
            {
                "timestamp": "2024-01-16 10:00:00",
                "user_id": 2,
                "feature_id": 1,
                "feature_name": "Collaboration",
                "event_type": "click",
            },
        ]

        bronze_path = os.path.join(temp_dir, "bronze")
        _setup_bronze_feature_usage_events(spark, temp_dir, events, bronze_path)
        silver_path = os.path.join(temp_dir, "silver")

        create_feature_usage_facts(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_feature_usage_facts")

        # 2 user-feature combinations
        assert df.count() == 2

        user1 = df.filter(col("user_id") == 1).collect()[0]
        assert user1.total_usage_count == 1

        user2 = df.filter(col("user_id") == 2).collect()[0]
        assert user2.total_usage_count == 2

    def test_feature_usage_facts_has_as_of_date(self, spark, temp_dir):
        """Test that as_of_date is populated with a valid date."""
        events = [
            {
                "timestamp": "2024-01-15 10:30:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "Collaboration",
                "event_type": "click",
            },
        ]

        bronze_path = os.path.join(temp_dir, "bronze")
        _setup_bronze_feature_usage_events(spark, temp_dir, events, bronze_path)
        silver_path = os.path.join(temp_dir, "silver")

        create_feature_usage_facts(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_feature_usage_facts")
        row = df.collect()[0]
        assert row.as_of_date is not None
        assert isinstance(row.as_of_date, date)


class TestRunSilverTransformation:
    """Test suite for run_silver_transformation orchestrator function"""

    def test_run_silver_transformation_full_pipeline(self, spark, temp_dir):
        """Test the full silver transformation pipeline."""
        bronze_path = os.path.join(temp_dir, "bronze")

        # Set up all bronze tables
        releases = [
            {"id": 1, "name": "Collaboration", "release_date": "2024-03-01", "version": "v1.0"},
        ]
        _setup_bronze_feature_releases(spark, temp_dir, releases)

        signups = [
            {
                "user_id": 1,
                "email": "user1@test.com",
                "signup_date": "2024-01-15",
                "company_size": "Small",
                "industry": "Tech",
            },
        ]
        _setup_bronze_user_signups(spark, temp_dir, signups, bronze_path)

        conversions = [
            {
                "user_id": 1,
                "conversion_date": "2024-02-01",
                "plan": "Pro",
                "mrr": 99,
                "signup_date": "2024-01-15",
                "days_to_convert": 17,
                "used_real_time_collab": True,
            },
        ]
        _setup_bronze_conversions(spark, temp_dir, conversions, bronze_path)

        events = [
            {
                "timestamp": "2024-01-15 10:30:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "Collaboration",
                "event_type": "click",
            },
        ]
        _setup_bronze_feature_usage_events(spark, temp_dir, events, bronze_path)

        silver_path = os.path.join(temp_dir, "silver")

        # Run full transformation
        stats = run_silver_transformation(spark, bronze_path, silver_path)

        # Verify stats
        assert stats["feature_states"] == 1
        assert stats["user_dim"] == 1
        assert stats["feature_usage_facts"] == 1

        # Verify all silver tables exist
        for table_name in [
            "silver_feature_states",
            "silver_user_dim",
            "silver_feature_usage_facts",
        ]:
            table_path = os.path.join(silver_path, table_name)
            assert os.path.exists(table_path), f"Table {table_name} should exist"
            df = spark.read.format("delta").load(table_path)
            assert df.count() > 0, f"Table {table_name} should have data"

    def test_run_silver_transformation_returns_stats(self, spark, temp_dir):
        """Test that run_silver_transformation returns correct statistics."""
        bronze_path = os.path.join(temp_dir, "bronze")

        releases = [
            {"id": 1, "name": "Feature A", "release_date": "2024-01-15", "version": "v1.0"},
            {"id": 2, "name": "Feature B", "release_date": "2024-02-15", "version": "v1.0"},
        ]
        _setup_bronze_feature_releases(spark, temp_dir, releases)

        signups = [
            {
                "user_id": 1,
                "email": "u1@test.com",
                "signup_date": "2024-01-01",
                "company_size": "Small",
                "industry": "Tech",
            },
            {
                "user_id": 2,
                "email": "u2@test.com",
                "signup_date": "2024-01-02",
                "company_size": "Large",
                "industry": "Finance",
            },
            {
                "user_id": 3,
                "email": "u3@test.com",
                "signup_date": "2024-01-03",
                "company_size": "Medium",
                "industry": "Healthcare",
            },
        ]
        _setup_bronze_user_signups(spark, temp_dir, signups, bronze_path)

        conversions = [
            {
                "user_id": 1,
                "conversion_date": "2024-02-01",
                "plan": "Pro",
                "mrr": 99,
                "signup_date": "2024-01-01",
                "days_to_convert": 31,
                "used_real_time_collab": True,
            },
        ]
        _setup_bronze_conversions(spark, temp_dir, conversions, bronze_path)

        events = [
            {
                "timestamp": "2024-01-15 10:30:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "Feature A",
                "event_type": "click",
            },
            {
                "timestamp": "2024-01-16 11:00:00",
                "user_id": 2,
                "feature_id": 1,
                "feature_name": "Feature A",
                "event_type": "view",
            },
        ]
        _setup_bronze_feature_usage_events(spark, temp_dir, events, bronze_path)

        silver_path = os.path.join(temp_dir, "silver")
        stats = run_silver_transformation(spark, bronze_path, silver_path)

        assert isinstance(stats, dict)
        assert stats["feature_states"] == 2
        assert stats["user_dim"] == 3
        assert stats["feature_usage_facts"] == 2
