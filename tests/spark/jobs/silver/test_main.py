"""
Unit tests for the silver transformation orchestrator.

Tests cover:
- Full silver transformation orchestrator
"""

import os

from spark.jobs.silver.main import run_silver_transformation
from tests.spark.helpers import (
    setup_bronze_feature_releases,
    setup_bronze_feature_usage_events,
    setup_bronze_marketing_attribution,
    setup_bronze_subscription_events,
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

        attribution = [
            {
                "user_id": 1,
                "channel": "paid_search",
                "campaign": "brand_q1",
                "first_touch_date": "2024-01-15",
            },
        ]
        setup_bronze_marketing_attribution(spark, temp_dir, attribution, bronze_path)

        subscription_events = [
            {
                "event_id": 1,
                "user_id": 1,
                "event_date": "2024-02-01",
                "event_type": "subscription_started",
                "plan": "pro",
                "mrr": 99,
                "previous_plan": None,
                "previous_mrr": None,
            },
        ]
        setup_bronze_subscription_events(spark, temp_dir, subscription_events, bronze_path)

        silver_path = os.path.join(temp_dir, "silver")

        # Run full transformation
        stats = run_silver_transformation(spark, bronze_path, silver_path)

        # Verify stats
        assert stats["feature_states"] == 1
        assert stats["user_dim"] == 1
        assert stats["feature_usage_facts"] == 1
        assert stats["subscription_periods"] == 1

        # Verify all silver tables exist
        for table_name in [
            "silver_feature_states",
            "silver_user_dim",
            "silver_feature_usage_facts",
            "silver_subscription_periods",
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

        attribution = [
            {
                "user_id": 1,
                "channel": "organic",
                "campaign": "none",
                "first_touch_date": "2024-01-01",
            },
        ]
        setup_bronze_marketing_attribution(spark, temp_dir, attribution, bronze_path)

        # started + expanded -> 2 subscription periods
        subscription_events = [
            {
                "event_id": 1,
                "user_id": 1,
                "event_date": "2024-02-01",
                "event_type": "subscription_started",
                "plan": "pro",
                "mrr": 99,
                "previous_plan": None,
                "previous_mrr": None,
            },
            {
                "event_id": 2,
                "user_id": 1,
                "event_date": "2024-03-01",
                "event_type": "seats_expanded",
                "plan": "pro",
                "mrr": 150,
                "previous_plan": "pro",
                "previous_mrr": 99,
            },
        ]
        setup_bronze_subscription_events(spark, temp_dir, subscription_events, bronze_path)

        silver_path = os.path.join(temp_dir, "silver")
        stats = run_silver_transformation(spark, bronze_path, silver_path)

        assert isinstance(stats, dict)
        assert stats["feature_states"] == 2
        assert stats["user_dim"] == 3
        assert stats["feature_usage_facts"] == 2
        assert stats["subscription_periods"] == 2
