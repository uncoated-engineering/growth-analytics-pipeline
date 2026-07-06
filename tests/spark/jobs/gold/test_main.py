"""
Unit tests for the gold aggregation orchestrator.

Tests cover:
- Full gold aggregation orchestrator
"""

import os

from spark.jobs.gold.main import run_gold_aggregation
from tests.spark.helpers import setup_full_pipeline


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

        bronze_path, silver_path, gold_path = setup_full_pipeline(
            spark, temp_dir, releases, signups, conversions, events
        )

        stats = run_gold_aggregation(spark, bronze_path, silver_path, gold_path)

        assert isinstance(stats, dict)
        assert "feature_conversion_impact" in stats
        assert "mrr_waterfall" in stats
        assert "channel_performance" in stats
        assert "weekly_engagement" in stats
        assert stats["feature_conversion_impact"] > 0
        # One derived subscription_started event -> one waterfall month
        assert stats["mrr_waterfall"] == 1
        # One signup-month x channel bucket ('unattributed')
        assert stats["channel_performance"] == 1
        # One usage event -> one week-feature row
        assert stats["weekly_engagement"] == 1

    def test_run_gold_aggregation_creates_tables(self, spark, temp_dir):
        """Test that run_gold_aggregation creates the gold Delta tables."""
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

        bronze_path, silver_path, gold_path = setup_full_pipeline(
            spark, temp_dir, releases, signups, conversions, events
        )

        run_gold_aggregation(spark, bronze_path, silver_path, gold_path)

        # Every gold table must be created (weekly engagement is empty: no events)
        for table_name in [
            "gold_feature_conversion_impact",
            "gold_mrr_waterfall",
            "gold_channel_performance",
            "gold_weekly_engagement",
        ]:
            table_path = os.path.join(gold_path, table_name)
            assert os.path.exists(table_path), f"Table {table_name} should exist"

        df = spark.read.format("delta").load(
            os.path.join(gold_path, "gold_feature_conversion_impact")
        )
        assert df.count() > 0
