"""
Unit tests for the silver transformation orchestrator.

Tests cover:
- Full silver transformation orchestrator
"""

import os

from spark.jobs.silver.main import run_silver_transformation
from tests.spark.helpers import (
    setup_bronze_conversions,
    setup_bronze_feature_releases,
    setup_bronze_feature_usage_events,
    setup_bronze_user_signups,
)


class TestRunSilverTransformation:
    """Test suite for run_silver_transformation orchestrator function"""

    def test_run_silver_transformation_full_pipeline(self, spark, temp_dir):
        """Test the full silver transformation pipeline."""
        bronze_path = os.path.join(temp_dir, "bronze")

        # Set up all bronze tables
        releases = [
            {"id": 1, "name": "Collaboration", "release_date": "2024-03-01", "version": "v1.0"},
        ]
        setup_bronze_feature_releases(spark, temp_dir, releases)

        signups = [
            {
                "user_id": 1,
                "email": "user1@test.com",
                "signup_date": "2024-01-15",
                "company_size": "Small",
                "industry": "Tech",
            },
        ]
        setup_bronze_user_signups(spark, temp_dir, signups, bronze_path)

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
        setup_bronze_conversions(spark, temp_dir, conversions, bronze_path)

        events = [
            {
                "timestamp": "2024-01-15 10:30:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "Collaboration",
                "event_type": "click",
            },
        ]
        setup_bronze_feature_usage_events(spark, temp_dir, events, bronze_path)

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
        setup_bronze_feature_releases(spark, temp_dir, releases)

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
        setup_bronze_user_signups(spark, temp_dir, signups, bronze_path)

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
        setup_bronze_conversions(spark, temp_dir, conversions, bronze_path)

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
        setup_bronze_feature_usage_events(spark, temp_dir, events, bronze_path)

        silver_path = os.path.join(temp_dir, "silver")
        stats = run_silver_transformation(spark, bronze_path, silver_path)

        assert isinstance(stats, dict)
        assert stats["feature_states"] == 2
        assert stats["user_dim"] == 3
        assert stats["feature_usage_facts"] == 2
