"""
Unit tests for the silver user dimension transformation.

Tests cover:
- User dimension table built from signups + attribution + subscription events
- Current plan/MRR resolution from the latest subscription event
- Attribution defaults ('unattributed') and churn handling
"""

import os
from datetime import date

from pyspark.sql.functions import col

from spark.jobs.silver.user_dim.transformation import maintain_user_dim
from tests.spark.helpers import (
    setup_bronze_marketing_attribution,
    setup_bronze_subscription_events,
    setup_bronze_user_signups,
)


def _setup_bronze(spark, temp_dir, signups, attribution, subscription_events):
    """Set up the three bronze tables maintain_user_dim reads."""
    bronze_path = os.path.join(temp_dir, "bronze")
    setup_bronze_user_signups(spark, temp_dir, signups, bronze_path)
    setup_bronze_marketing_attribution(spark, temp_dir, attribution, bronze_path)
    setup_bronze_subscription_events(spark, temp_dir, subscription_events, bronze_path)
    return bronze_path


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
        attribution = [
            {
                "user_id": 1,
                "channel": "paid_search",
                "campaign": "brand_q1",
                "first_touch_date": "2024-01-15",
            },
        ]
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

        bronze_path = _setup_bronze(spark, temp_dir, signups, attribution, subscription_events)
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
        attribution = []
        subscription_events = []

        bronze_path = _setup_bronze(spark, temp_dir, signups, attribution, subscription_events)
        silver_path = os.path.join(temp_dir, "silver")

        maintain_user_dim(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_user_dim")
        expected_columns = {
            "user_id",
            "signup_date",
            "company_size",
            "industry",
            "acquisition_channel",
            "acquisition_campaign",
            "current_plan",
            "current_mrr",
        }
        assert set(df.columns) == expected_columns

        assert df.schema["user_id"].dataType.typeName() == "integer"
        assert df.schema["signup_date"].dataType.typeName() == "date"
        assert df.schema["company_size"].dataType.typeName() == "string"
        assert df.schema["industry"].dataType.typeName() == "string"
        assert df.schema["acquisition_channel"].dataType.typeName() == "string"
        assert df.schema["acquisition_campaign"].dataType.typeName() == "string"
        assert df.schema["current_plan"].dataType.typeName() == "string"
        assert df.schema["current_mrr"].dataType.typeName() == "integer"

    def test_user_dim_converted_user_gets_plan_from_latest_event(self, spark, temp_dir):
        """Test that converted users get plan and MRR from their latest subscription event."""
        signups = [
            {
                "user_id": 1,
                "email": "user1@test.com",
                "signup_date": "2024-01-15",
                "company_size": "Small",
                "industry": "Tech",
            },
        ]
        attribution = []
        subscription_events = [
            {
                "event_id": 1,
                "user_id": 1,
                "event_date": "2024-02-01",
                "event_type": "subscription_started",
                "plan": "enterprise",
                "mrr": 299,
                "previous_plan": None,
                "previous_mrr": None,
            },
        ]

        bronze_path = _setup_bronze(spark, temp_dir, signups, attribution, subscription_events)
        silver_path = os.path.join(temp_dir, "silver")

        maintain_user_dim(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_user_dim")
        row = df.filter(col("user_id") == 1).collect()[0]
        assert row.current_plan == "enterprise"
        assert row.current_mrr == 299

    def test_user_dim_never_converted_user_defaults_to_free(self, spark, temp_dir):
        """Test that users without subscription events default to 'free' with 0 MRR."""
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
        attribution = []
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

        bronze_path = _setup_bronze(spark, temp_dir, signups, attribution, subscription_events)
        silver_path = os.path.join(temp_dir, "silver")

        maintain_user_dim(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_user_dim")

        # User 1 has a subscription
        user1 = df.filter(col("user_id") == 1).collect()[0]
        assert user1.current_plan == "pro"
        assert user1.current_mrr == 99

        # User 2 never converted
        user2 = df.filter(col("user_id") == 2).collect()[0]
        assert user2.current_plan == "free"
        assert user2.current_mrr == 0

    def test_user_dim_churned_user(self, spark, temp_dir):
        """Test that a user whose latest event is a cancellation shows as 'churned' with 0 MRR."""
        signups = [
            {
                "user_id": 1,
                "email": "user1@test.com",
                "signup_date": "2024-01-15",
                "company_size": "Small",
                "industry": "Tech",
            },
        ]
        attribution = []
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
                "event_date": "2024-04-15",
                "event_type": "subscription_cancelled",
                "plan": None,
                "mrr": 0,
                "previous_plan": "pro",
                "previous_mrr": 99,
            },
        ]

        bronze_path = _setup_bronze(spark, temp_dir, signups, attribution, subscription_events)
        silver_path = os.path.join(temp_dir, "silver")

        maintain_user_dim(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_user_dim")
        row = df.filter(col("user_id") == 1).collect()[0]
        assert row.current_plan == "churned"
        assert row.current_mrr == 0

    def test_user_dim_attributed_user_gets_channel_and_campaign(self, spark, temp_dir):
        """Test that attributed users carry their first-touch channel and campaign."""
        signups = [
            {
                "user_id": 1,
                "email": "user1@test.com",
                "signup_date": "2024-01-15",
                "company_size": "Small",
                "industry": "Tech",
            },
        ]
        attribution = [
            {
                "user_id": 1,
                "channel": "content_marketing",
                "campaign": "blog_launch",
                "first_touch_date": "2024-01-14",
            },
        ]
        subscription_events = []

        bronze_path = _setup_bronze(spark, temp_dir, signups, attribution, subscription_events)
        silver_path = os.path.join(temp_dir, "silver")

        maintain_user_dim(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_user_dim")
        row = df.filter(col("user_id") == 1).collect()[0]
        assert row.acquisition_channel == "content_marketing"
        assert row.acquisition_campaign == "blog_launch"

    def test_user_dim_untracked_user_defaults_to_unattributed(self, spark, temp_dir):
        """Test that users without an attribution record default to 'unattributed'."""
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
        attribution = [
            {
                "user_id": 1,
                "channel": "paid_search",
                "campaign": "brand_q1",
                "first_touch_date": "2024-01-15",
            },
        ]
        subscription_events = []

        bronze_path = _setup_bronze(spark, temp_dir, signups, attribution, subscription_events)
        silver_path = os.path.join(temp_dir, "silver")

        maintain_user_dim(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_user_dim")
        row = df.filter(col("user_id") == 2).collect()[0]
        assert row.acquisition_channel == "unattributed"
        assert row.acquisition_campaign == "unattributed"

    def test_user_dim_upgrade_sequence_reflects_latest_event(self, spark, temp_dir):
        """Test that an upgrade sequence resolves to the latest event's plan and MRR."""
        signups = [
            {
                "user_id": 1,
                "email": "user1@test.com",
                "signup_date": "2024-01-15",
                "company_size": "Small",
                "industry": "Tech",
            },
        ]
        attribution = []
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
                "event_date": "2024-05-10",
                "event_type": "plan_upgraded",
                "plan": "enterprise",
                "mrr": 299,
                "previous_plan": "pro",
                "previous_mrr": 99,
            },
        ]

        bronze_path = _setup_bronze(spark, temp_dir, signups, attribution, subscription_events)
        silver_path = os.path.join(temp_dir, "silver")

        maintain_user_dim(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_user_dim")
        row = df.filter(col("user_id") == 1).collect()[0]
        assert row.current_plan == "enterprise"
        assert row.current_mrr == 299

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
        attribution = []
        subscription_events = []

        bronze_path = _setup_bronze(spark, temp_dir, signups, attribution, subscription_events)
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
        attribution = []
        subscription_events = []

        bronze_path = _setup_bronze(spark, temp_dir, signups, attribution, subscription_events)
        silver_path = os.path.join(temp_dir, "silver")

        maintain_user_dim(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_user_dim")
        assert "email" not in df.columns
