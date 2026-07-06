"""
Unit tests for the silver subscription periods transformation.

Tests cover:
- Periodization of subscription lifecycle events into validity intervals
- Open periods (9999-12-31) for the current state
- Cancellations closing periods without opening new ones
- Independence across users
- Output schema
"""

import os
from datetime import date

from pyspark.sql.functions import col

from spark.jobs.silver.subscription_periods.schema import SUBSCRIPTION_PERIODS_SCHEMA
from spark.jobs.silver.subscription_periods.transformation import create_subscription_periods
from tests.spark.helpers import setup_bronze_subscription_events


class TestCreateSubscriptionPeriods:
    """Test suite for create_subscription_periods function"""

    def test_single_started_event_creates_open_period(self, spark, temp_dir):
        """Test that a lone subscription_started event opens a single active period."""
        subscription_events = [
            {
                "event_id": 1,
                "user_id": 1,
                "event_date": "2024-01-10",
                "event_type": "subscription_started",
                "plan": "pro",
                "mrr": 100,
                "previous_plan": None,
                "previous_mrr": None,
            },
        ]

        bronze_path = setup_bronze_subscription_events(spark, temp_dir, subscription_events)
        silver_path = os.path.join(temp_dir, "silver")

        row_count = create_subscription_periods(spark, bronze_path, silver_path)

        assert row_count == 1

        df = spark.read.format("delta").load(f"{silver_path}/silver_subscription_periods")
        row = df.collect()[0]
        assert row.user_id == 1
        assert row.plan == "pro"
        assert row.mrr == 100
        assert row.period_start == date(2024, 1, 10)
        assert row.period_end == date(9999, 12, 31)
        assert row.is_active is True
        assert row.change_type == "subscription_started"

    def test_expansion_closes_previous_period(self, spark, temp_dir):
        """Test that an expansion event closes the prior period and opens a new active one."""
        subscription_events = [
            {
                "event_id": 1,
                "user_id": 1,
                "event_date": "2024-01-10",
                "event_type": "subscription_started",
                "plan": "pro",
                "mrr": 100,
                "previous_plan": None,
                "previous_mrr": None,
            },
            {
                "event_id": 2,
                "user_id": 1,
                "event_date": "2024-02-15",
                "event_type": "seats_expanded",
                "plan": "pro",
                "mrr": 150,
                "previous_plan": "pro",
                "previous_mrr": 100,
            },
        ]

        bronze_path = setup_bronze_subscription_events(spark, temp_dir, subscription_events)
        silver_path = os.path.join(temp_dir, "silver")

        row_count = create_subscription_periods(spark, bronze_path, silver_path)

        assert row_count == 2

        df = spark.read.format("delta").load(f"{silver_path}/silver_subscription_periods")

        first = df.filter(col("change_type") == "subscription_started").collect()[0]
        assert first.period_start == date(2024, 1, 10)
        assert first.period_end == date(2024, 2, 15)
        assert first.is_active is False
        assert first.mrr == 100

        second = df.filter(col("change_type") == "seats_expanded").collect()[0]
        assert second.period_start == date(2024, 2, 15)
        assert second.period_end == date(9999, 12, 31)
        assert second.is_active is True
        assert second.mrr == 150

    def test_cancellation_closes_period_without_opening_new_one(self, spark, temp_dir):
        """Test that a cancellation closes the previous period and opens no new period."""
        subscription_events = [
            {
                "event_id": 1,
                "user_id": 1,
                "event_date": "2024-01-10",
                "event_type": "subscription_started",
                "plan": "pro",
                "mrr": 100,
                "previous_plan": None,
                "previous_mrr": None,
            },
            {
                "event_id": 2,
                "user_id": 1,
                "event_date": "2024-03-05",
                "event_type": "subscription_cancelled",
                "plan": None,
                "mrr": 0,
                "previous_plan": "pro",
                "previous_mrr": 100,
            },
        ]

        bronze_path = setup_bronze_subscription_events(spark, temp_dir, subscription_events)
        silver_path = os.path.join(temp_dir, "silver")

        row_count = create_subscription_periods(spark, bronze_path, silver_path)

        # The cancellation itself must NOT open a period
        assert row_count == 1

        df = spark.read.format("delta").load(f"{silver_path}/silver_subscription_periods")
        assert df.filter(col("change_type") == "subscription_cancelled").count() == 0

        row = df.collect()[0]
        assert row.change_type == "subscription_started"
        assert row.period_start == date(2024, 1, 10)
        assert row.period_end == date(2024, 3, 5)
        assert row.is_active is False

    def test_multiple_users_periodized_independently(self, spark, temp_dir):
        """Test that periods are computed per user, without cross-user interference."""
        subscription_events = [
            {
                "event_id": 1,
                "user_id": 1,
                "event_date": "2024-01-10",
                "event_type": "subscription_started",
                "plan": "pro",
                "mrr": 100,
                "previous_plan": None,
                "previous_mrr": None,
            },
            {
                "event_id": 2,
                "user_id": 2,
                "event_date": "2024-01-20",
                "event_type": "subscription_started",
                "plan": "enterprise",
                "mrr": 300,
                "previous_plan": None,
                "previous_mrr": None,
            },
            {
                "event_id": 3,
                "user_id": 1,
                "event_date": "2024-02-15",
                "event_type": "plan_upgraded",
                "plan": "enterprise",
                "mrr": 299,
                "previous_plan": "pro",
                "previous_mrr": 100,
            },
        ]

        bronze_path = setup_bronze_subscription_events(spark, temp_dir, subscription_events)
        silver_path = os.path.join(temp_dir, "silver")

        row_count = create_subscription_periods(spark, bronze_path, silver_path)

        assert row_count == 3

        df = spark.read.format("delta").load(f"{silver_path}/silver_subscription_periods")

        # User 1: closed started period + open upgraded period
        user1 = df.filter(col("user_id") == 1).orderBy("period_start").collect()
        assert len(user1) == 2
        assert user1[0].period_end == date(2024, 2, 15)
        assert user1[0].is_active is False
        assert user1[1].period_end == date(9999, 12, 31)
        assert user1[1].is_active is True

        # User 2: untouched by user 1's events — still one open period
        user2 = df.filter(col("user_id") == 2).collect()
        assert len(user2) == 1
        assert user2[0].period_end == date(9999, 12, 31)
        assert user2[0].is_active is True
        assert user2[0].mrr == 300

    def test_output_schema(self, spark, temp_dir):
        """Test that the output matches SUBSCRIPTION_PERIODS_SCHEMA."""
        subscription_events = [
            {
                "event_id": 1,
                "user_id": 1,
                "event_date": "2024-01-10",
                "event_type": "subscription_started",
                "plan": "pro",
                "mrr": 100,
                "previous_plan": None,
                "previous_mrr": None,
            },
        ]

        bronze_path = setup_bronze_subscription_events(spark, temp_dir, subscription_events)
        silver_path = os.path.join(temp_dir, "silver")

        create_subscription_periods(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_subscription_periods")

        expected_fields = {f.name: f.dataType for f in SUBSCRIPTION_PERIODS_SCHEMA.fields}
        actual_fields = {f.name: f.dataType for f in df.schema.fields}
        assert actual_fields == expected_fields
        assert len(df.columns) == 7
