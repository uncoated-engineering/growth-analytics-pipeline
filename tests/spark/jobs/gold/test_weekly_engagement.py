"""
Unit tests for the gold weekly engagement aggregation.

Tests cover:
- Per-feature weekly active users and event counts
- Overall weekly active users (WAU) and pct_of_wau
- Week bucketing to the ISO week's Monday
- Output schema
"""

import os
from datetime import date

from pyspark.sql.functions import col

from spark.jobs.gold.weekly_engagement.aggregation import calculate_weekly_engagement
from tests.spark.helpers import setup_bronze_feature_usage_events


class TestCalculateWeeklyEngagement:
    """Test suite for calculate_weekly_engagement function"""

    def test_weekly_metrics_across_two_weeks_and_features(self, spark, temp_dir):
        """Test engagement metrics on a hand-computable 2-week, 2-feature scenario."""
        # Week 1 starts Monday 2024-01-01, week 2 starts Monday 2024-01-08
        events = [
            # Week 1, feature_a: user 1 twice, user 2 once -> 2 active, 3 events
            {
                "timestamp": "2024-01-01 09:00:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "feature_a",
                "event_type": "click",
            },
            {
                "timestamp": "2024-01-03 10:00:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "feature_a",
                "event_type": "click",
            },
            {
                "timestamp": "2024-01-05 11:00:00",
                "user_id": 2,
                "feature_id": 1,
                "feature_name": "feature_a",
                "event_type": "view",
            },
            # Week 1, feature_b: user 1 once -> 1 active, 1 event
            {
                "timestamp": "2024-01-04 12:00:00",
                "user_id": 1,
                "feature_id": 2,
                "feature_name": "feature_b",
                "event_type": "click",
            },
            # Week 2, feature_a: user 3 once -> 1 active, 1 event
            {
                "timestamp": "2024-01-10 13:00:00",
                "user_id": 3,
                "feature_id": 1,
                "feature_name": "feature_a",
                "event_type": "click",
            },
        ]

        bronze_path = setup_bronze_feature_usage_events(spark, temp_dir, events)
        silver_path = os.path.join(temp_dir, "silver")
        gold_path = os.path.join(temp_dir, "gold")

        row_count = calculate_weekly_engagement(spark, bronze_path, silver_path, gold_path)

        # (week1, feature_a), (week1, feature_b), (week2, feature_a)
        assert row_count == 3

        df = spark.read.format("delta").load(f"{gold_path}/gold_weekly_engagement")

        week1_a = df.filter(
            (col("week_start") == "2024-01-01") & (col("feature_name") == "feature_a")
        ).collect()[0]
        assert week1_a.active_users == 2
        assert week1_a.total_events == 3
        assert abs(week1_a.events_per_active_user - 1.5) < 0.001
        # WAU in week 1: users 1 and 2
        assert week1_a.weekly_active_users == 2
        assert abs(week1_a.pct_of_wau - 1.0) < 0.001

        week1_b = df.filter(
            (col("week_start") == "2024-01-01") & (col("feature_name") == "feature_b")
        ).collect()[0]
        assert week1_b.active_users == 1
        assert week1_b.total_events == 1
        assert abs(week1_b.events_per_active_user - 1.0) < 0.001
        assert week1_b.weekly_active_users == 2
        assert abs(week1_b.pct_of_wau - 0.5) < 0.001

        week2_a = df.filter(
            (col("week_start") == "2024-01-08") & (col("feature_name") == "feature_a")
        ).collect()[0]
        assert week2_a.active_users == 1
        assert week2_a.total_events == 1
        assert week2_a.weekly_active_users == 1
        assert abs(week2_a.pct_of_wau - 1.0) < 0.001

    def test_week_start_is_monday(self, spark, temp_dir):
        """Test that events are bucketed to the Monday of their ISO week."""
        events = [
            # Sunday 2024-01-14 belongs to the week starting Monday 2024-01-08
            {
                "timestamp": "2024-01-14 23:00:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "feature_a",
                "event_type": "click",
            },
            # Monday 2024-01-08 stays in its own week
            {
                "timestamp": "2024-01-08 00:30:00",
                "user_id": 2,
                "feature_id": 1,
                "feature_name": "feature_a",
                "event_type": "click",
            },
        ]

        bronze_path = setup_bronze_feature_usage_events(spark, temp_dir, events)
        silver_path = os.path.join(temp_dir, "silver")
        gold_path = os.path.join(temp_dir, "gold")

        calculate_weekly_engagement(spark, bronze_path, silver_path, gold_path)

        df = spark.read.format("delta").load(f"{gold_path}/gold_weekly_engagement")
        rows = df.collect()

        # Both events collapse into a single Monday-anchored week
        assert len(rows) == 1
        assert rows[0].week_start == date(2024, 1, 8)
        assert rows[0].week_start.weekday() == 0  # Monday
        assert rows[0].active_users == 2

    def test_output_schema(self, spark, temp_dir):
        """Test that the gold table has the expected schema."""
        events = [
            {
                "timestamp": "2024-01-01 09:00:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "feature_a",
                "event_type": "click",
            },
        ]

        bronze_path = setup_bronze_feature_usage_events(spark, temp_dir, events)
        silver_path = os.path.join(temp_dir, "silver")
        gold_path = os.path.join(temp_dir, "gold")

        calculate_weekly_engagement(spark, bronze_path, silver_path, gold_path)

        df = spark.read.format("delta").load(f"{gold_path}/gold_weekly_engagement")

        expected_columns = {
            "week_start",
            "feature_name",
            "active_users",
            "total_events",
            "events_per_active_user",
            "weekly_active_users",
            "pct_of_wau",
        }
        assert set(df.columns) == expected_columns

        assert df.schema["week_start"].dataType.typeName() == "date"
        assert df.schema["feature_name"].dataType.typeName() == "string"
        assert df.schema["active_users"].dataType.typeName() == "long"
        assert df.schema["total_events"].dataType.typeName() == "long"
        assert df.schema["events_per_active_user"].dataType.typeName() == "double"
        assert df.schema["weekly_active_users"].dataType.typeName() == "long"
        assert df.schema["pct_of_wau"].dataType.typeName() == "double"
