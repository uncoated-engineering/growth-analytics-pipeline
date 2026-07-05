"""
Unit tests for the silver feature states SCD transformation.

Tests cover:
- SCD Type 2 feature states (initial load, change detection, new features)
"""

import json
import os
from datetime import date

from pyspark.sql.functions import col

from spark.jobs.bronze.feature_releases.extract import ingest_feature_releases
from spark.jobs.silver.feature_states.transformation import maintain_feature_states_scd
from tests.spark.helpers import (
    setup_bronze_feature_releases,
)


class TestMaintainFeatureStatesScd:
    """Test suite for maintain_feature_states_scd function"""

    def test_initial_load_creates_scd_records(self, spark, temp_dir):
        """Test that the first run creates initial SCD records for all features."""
        releases = [
            {"id": 1, "name": "real_time_collab", "release_date": "2024-03-01", "version": "v1.0"},
            {"id": 2, "name": "ai_insights", "release_date": "2024-05-01", "version": "v1.0"},
        ]
        bronze_path = setup_bronze_feature_releases(spark, temp_dir, releases)
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
        bronze_path = setup_bronze_feature_releases(spark, temp_dir, releases)
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
        bronze_path = setup_bronze_feature_releases(spark, temp_dir, releases)
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
        bronze_path = setup_bronze_feature_releases(spark, temp_dir, releases)
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
        bronze_path = setup_bronze_feature_releases(spark, temp_dir, releases_v1)
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
        bronze_path = setup_bronze_feature_releases(spark, temp_dir, releases_initial)
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
        bronze_path = setup_bronze_feature_releases(spark, temp_dir, releases)
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
        bronze_path = setup_bronze_feature_releases(spark, temp_dir, releases)
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
