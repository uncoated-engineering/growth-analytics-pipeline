"""
Unit tests for the bronze ingestion orchestrator.

Tests cover:
- Full bronze ingestion pipeline
- Returned statistics
"""

import json
import os

from spark.jobs.bronze.main import run_bronze_ingestion


class TestRunBronzeIngestion:
    """Test suite for run_bronze_ingestion orchestrator function"""

    def test_run_bronze_ingestion_full_pipeline(self, spark, temp_dir):
        """Test the full bronze ingestion pipeline"""
        # Setup directory structure
        raw_data_path = os.path.join(temp_dir, "raw")
        bronze_path = os.path.join(temp_dir, "bronze")
        os.makedirs(raw_data_path, exist_ok=True)

        # Create test data files
        feature_releases = [
            {"id": 1, "name": "Feature A", "release_date": "2024-01-15", "version": "1.0.0"},
        ]
        with open(os.path.join(raw_data_path, "feature_releases.json"), "w") as f:
            json.dump(feature_releases, f)

        user_signups = [
            {
                "user_id": 1,
                "email": "user1@test.com",
                "signup_date": "2024-01-15",
                "company_size": "Small",
                "industry": "Tech",
            },
        ]
        with open(os.path.join(raw_data_path, "user_signups.jsonl"), "w") as f:
            for record in user_signups:
                f.write(json.dumps(record) + "\n")

        feature_usage_events = [
            {
                "timestamp": "2024-01-15 10:30:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "Feature A",
                "event_type": "click",
            },
        ]
        with open(os.path.join(raw_data_path, "feature_usage_events.jsonl"), "w") as f:
            for record in feature_usage_events:
                f.write(json.dumps(record) + "\n")

        conversions = [
            {
                "user_id": 1,
                "conversion_date": "2024-01-20",
                "plan": "Pro",
                "mrr": 99,
                "signup_date": "2024-01-15",
                "days_to_convert": 5,
                "used_real_time_collab": True,
            },
        ]
        with open(os.path.join(raw_data_path, "conversions.jsonl"), "w") as f:
            for record in conversions:
                f.write(json.dumps(record) + "\n")

        marketing_attribution = [
            {
                "user_id": 1,
                "channel": "paid_search",
                "campaign": "brand_q1",
                "first_touch_date": "2024-01-10",
            },
        ]
        with open(os.path.join(raw_data_path, "marketing_attribution.jsonl"), "w") as f:
            for record in marketing_attribution:
                f.write(json.dumps(record) + "\n")

        subscription_events = [
            {
                "event_id": 1,
                "user_id": 1,
                "event_date": "2024-01-20",
                "event_type": "subscription_started",
                "plan": "pro",
                "mrr": 99,
                "previous_plan": None,
                "previous_mrr": None,
            },
        ]
        with open(os.path.join(raw_data_path, "subscription_events.jsonl"), "w") as f:
            for record in subscription_events:
                f.write(json.dumps(record) + "\n")

        # Run bronze ingestion
        stats = run_bronze_ingestion(spark, raw_data_path, bronze_path)

        # Verify stats
        assert stats["feature_releases"] == 1
        assert stats["user_signups"] == 1
        assert stats["feature_usage_events"] == 1
        assert stats["conversions"] == 1
        assert stats["marketing_attribution"] == 1
        assert stats["subscription_events"] == 1

        # Verify all Delta tables exist and have data
        for table_name in [
            "feature_releases",
            "user_signups",
            "feature_usage_events",
            "conversions",
            "marketing_attribution",
            "subscription_events",
        ]:
            table_path = os.path.join(bronze_path, table_name)
            assert os.path.exists(table_path), f"Table {table_name} should exist"
            df = spark.read.format("delta").load(table_path)
            assert df.count() > 0, f"Table {table_name} should have data"

    def test_run_bronze_ingestion_returns_stats(self, spark, temp_dir):
        """Test that run_bronze_ingestion returns correct statistics"""
        raw_data_path = os.path.join(temp_dir, "raw")
        bronze_path = os.path.join(temp_dir, "bronze")
        os.makedirs(raw_data_path, exist_ok=True)

        # Create minimal test data
        with open(os.path.join(raw_data_path, "feature_releases.json"), "w") as f:
            json.dump([], f)
        with open(os.path.join(raw_data_path, "user_signups.jsonl"), "w") as f:
            pass
        with open(os.path.join(raw_data_path, "feature_usage_events.jsonl"), "w") as f:
            pass
        with open(os.path.join(raw_data_path, "conversions.jsonl"), "w") as f:
            pass
        with open(os.path.join(raw_data_path, "marketing_attribution.jsonl"), "w") as f:
            pass
        with open(os.path.join(raw_data_path, "subscription_events.jsonl"), "w") as f:
            pass

        stats = run_bronze_ingestion(spark, raw_data_path, bronze_path)

        # Verify stats structure
        assert isinstance(stats, dict)
        assert "feature_releases" in stats
        assert "user_signups" in stats
        assert "feature_usage_events" in stats
        assert "conversions" in stats
        assert "marketing_attribution" in stats
        assert "subscription_events" in stats
