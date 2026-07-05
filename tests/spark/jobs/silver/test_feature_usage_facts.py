"""
Unit tests for the silver feature usage facts transformation.

Tests cover:
- Feature usage facts (aggregation, avg_daily_usage calculation)
"""

import os
from datetime import date

from pyspark.sql.functions import col

from spark.jobs.silver.feature_usage_facts.transformation import create_feature_usage_facts
from tests.spark.helpers import (
    setup_bronze_feature_usage_events,
)


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
        setup_bronze_feature_usage_events(spark, temp_dir, events, bronze_path)
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
        setup_bronze_feature_usage_events(spark, temp_dir, events, bronze_path)
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
        setup_bronze_feature_usage_events(spark, temp_dir, events, bronze_path)
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
        setup_bronze_feature_usage_events(spark, temp_dir, events, bronze_path)
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
        setup_bronze_feature_usage_events(spark, temp_dir, events, bronze_path)
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
        setup_bronze_feature_usage_events(spark, temp_dir, events, bronze_path)
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
        setup_bronze_feature_usage_events(spark, temp_dir, events, bronze_path)
        silver_path = os.path.join(temp_dir, "silver")

        create_feature_usage_facts(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_feature_usage_facts")
        row = df.collect()[0]
        assert row.as_of_date is not None
        assert isinstance(row.as_of_date, date)
