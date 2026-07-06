"""
Unit tests for the gold channel performance aggregation.

Tests cover:
- Signups / conversions / conversion rate per signup-month x channel
- The 'unattributed' bucket for untracked users
- MRR and days-to-convert averages computed over converted users only
- Output schema
"""

import os
from datetime import date

from pyspark.sql.functions import col

from spark.jobs.gold.channel_performance.aggregation import calculate_channel_performance
from spark.jobs.silver.user_dim.transformation import maintain_user_dim
from tests.spark.helpers import (
    setup_bronze_conversions,
    setup_bronze_marketing_attribution,
    setup_bronze_subscription_events,
    setup_bronze_user_signups,
)


def _setup_layers(spark, temp_dir, signups, attribution, conversions):
    """Set up bronze tables + silver user dim, return (bronze, silver, gold) paths."""
    bronze_path = os.path.join(temp_dir, "bronze")
    silver_path = os.path.join(temp_dir, "silver")
    gold_path = os.path.join(temp_dir, "gold")

    setup_bronze_user_signups(spark, temp_dir, signups, bronze_path)
    setup_bronze_marketing_attribution(spark, temp_dir, attribution, bronze_path)
    setup_bronze_conversions(spark, temp_dir, conversions, bronze_path)
    # user_dim requires the subscription_events table to exist; commercial
    # state is irrelevant for channel performance, so keep it empty
    setup_bronze_subscription_events(spark, temp_dir, [], bronze_path)

    maintain_user_dim(spark, bronze_path, silver_path)

    return bronze_path, silver_path, gold_path


class TestCalculateChannelPerformance:
    """Test suite for calculate_channel_performance function"""

    def test_channel_metrics_per_month(self, spark, temp_dir):
        """Test signups, conversions, and rate per channel-month with known data."""
        signups = [
            # paid_search: 2 signups in January, 1 converts
            {
                "user_id": 1,
                "email": "u1@test.com",
                "signup_date": "2024-01-10",
                "company_size": "Small",
                "industry": "Tech",
            },
            {
                "user_id": 2,
                "email": "u2@test.com",
                "signup_date": "2024-01-15",
                "company_size": "Medium",
                "industry": "Finance",
            },
            # organic: 1 signup in February, converts
            {
                "user_id": 3,
                "email": "u3@test.com",
                "signup_date": "2024-02-05",
                "company_size": "Large",
                "industry": "Healthcare",
            },
        ]
        attribution = [
            {
                "user_id": 1,
                "channel": "paid_search",
                "campaign": "brand_q1",
                "first_touch_date": "2024-01-10",
            },
            {
                "user_id": 2,
                "channel": "paid_search",
                "campaign": "brand_q1",
                "first_touch_date": "2024-01-15",
            },
            {
                "user_id": 3,
                "channel": "organic",
                "campaign": "none",
                "first_touch_date": "2024-02-05",
            },
        ]
        conversions = [
            {
                "user_id": 1,
                "conversion_date": "2024-02-01",
                "plan": "Pro",
                "mrr": 100,
                "signup_date": "2024-01-10",
                "days_to_convert": 22,
                "used_real_time_collab": False,
            },
            {
                "user_id": 3,
                "conversion_date": "2024-02-15",
                "plan": "Pro",
                "mrr": 50,
                "signup_date": "2024-02-05",
                "days_to_convert": 10,
                "used_real_time_collab": False,
            },
        ]

        bronze_path, silver_path, gold_path = _setup_layers(
            spark, temp_dir, signups, attribution, conversions
        )

        row_count = calculate_channel_performance(spark, bronze_path, silver_path, gold_path)

        # 2 month-channel rows: (Jan, paid_search) and (Feb, organic)
        assert row_count == 2

        df = spark.read.format("delta").load(f"{gold_path}/gold_channel_performance")

        paid = df.filter(col("acquisition_channel") == "paid_search").collect()[0]
        assert paid.signup_month == date(2024, 1, 1)
        assert paid.signups == 2
        assert paid.conversions == 1
        assert abs(paid.conversion_rate - 0.5) < 0.001
        assert paid.total_new_mrr == 100

        organic = df.filter(col("acquisition_channel") == "organic").collect()[0]
        assert organic.signup_month == date(2024, 2, 1)
        assert organic.signups == 1
        assert organic.conversions == 1
        assert abs(organic.conversion_rate - 1.0) < 0.001
        assert organic.total_new_mrr == 50

    def test_untracked_users_fall_into_unattributed_bucket(self, spark, temp_dir):
        """Test that users without an attribution record aggregate under 'unattributed'."""
        signups = [
            {
                "user_id": 1,
                "email": "u1@test.com",
                "signup_date": "2024-01-10",
                "company_size": "Small",
                "industry": "Tech",
            },
            {
                "user_id": 2,
                "email": "u2@test.com",
                "signup_date": "2024-01-12",
                "company_size": "Large",
                "industry": "Finance",
            },
        ]
        # Only user 1 is tracked
        attribution = [
            {
                "user_id": 1,
                "channel": "paid_search",
                "campaign": "brand_q1",
                "first_touch_date": "2024-01-10",
            },
        ]
        conversions = [
            {
                "user_id": 2,
                "conversion_date": "2024-01-25",
                "plan": "Pro",
                "mrr": 80,
                "signup_date": "2024-01-12",
                "days_to_convert": 13,
                "used_real_time_collab": False,
            },
        ]

        bronze_path, silver_path, gold_path = _setup_layers(
            spark, temp_dir, signups, attribution, conversions
        )

        calculate_channel_performance(spark, bronze_path, silver_path, gold_path)

        df = spark.read.format("delta").load(f"{gold_path}/gold_channel_performance")

        unattributed = df.filter(col("acquisition_channel") == "unattributed").collect()
        assert len(unattributed) == 1
        row = unattributed[0]
        assert row.signup_month == date(2024, 1, 1)
        assert row.signups == 1
        assert row.conversions == 1
        assert row.total_new_mrr == 80

    def test_avg_metrics_computed_over_converted_users_only(self, spark, temp_dir):
        """Test that avg_days_to_convert and avg_new_mrr ignore non-converted users."""
        signups = [
            {
                "user_id": 1,
                "email": "u1@test.com",
                "signup_date": "2024-01-10",
                "company_size": "Small",
                "industry": "Tech",
            },
            {
                "user_id": 2,
                "email": "u2@test.com",
                "signup_date": "2024-01-11",
                "company_size": "Medium",
                "industry": "Finance",
            },
            {
                "user_id": 3,
                "email": "u3@test.com",
                "signup_date": "2024-01-12",
                "company_size": "Large",
                "industry": "Healthcare",
            },
        ]
        attribution = [
            {
                "user_id": 1,
                "channel": "content",
                "campaign": "blog",
                "first_touch_date": "2024-01-10",
            },
            {
                "user_id": 2,
                "channel": "content",
                "campaign": "blog",
                "first_touch_date": "2024-01-11",
            },
            {
                "user_id": 3,
                "channel": "content",
                "campaign": "blog",
                "first_touch_date": "2024-01-12",
            },
        ]
        # Users 1 and 2 convert; user 3 does not
        conversions = [
            {
                "user_id": 1,
                "conversion_date": "2024-01-20",
                "plan": "Pro",
                "mrr": 100,
                "signup_date": "2024-01-10",
                "days_to_convert": 10,
                "used_real_time_collab": False,
            },
            {
                "user_id": 2,
                "conversion_date": "2024-01-31",
                "plan": "Enterprise",
                "mrr": 300,
                "signup_date": "2024-01-11",
                "days_to_convert": 20,
                "used_real_time_collab": False,
            },
        ]

        bronze_path, silver_path, gold_path = _setup_layers(
            spark, temp_dir, signups, attribution, conversions
        )

        calculate_channel_performance(spark, bronze_path, silver_path, gold_path)

        df = spark.read.format("delta").load(f"{gold_path}/gold_channel_performance")
        row = df.filter(col("acquisition_channel") == "content").collect()[0]

        assert row.signups == 3
        assert row.conversions == 2
        assert abs(row.conversion_rate - 2.0 / 3.0) < 0.001
        # Averages over the 2 converted users only
        assert abs(row.avg_days_to_convert - 15.0) < 0.001
        assert abs(row.avg_new_mrr - 200.0) < 0.001
        assert row.total_new_mrr == 400

    def test_output_schema(self, spark, temp_dir):
        """Test that the gold table has the expected schema."""
        signups = [
            {
                "user_id": 1,
                "email": "u1@test.com",
                "signup_date": "2024-01-10",
                "company_size": "Small",
                "industry": "Tech",
            },
        ]
        attribution = []
        conversions = []

        bronze_path, silver_path, gold_path = _setup_layers(
            spark, temp_dir, signups, attribution, conversions
        )

        calculate_channel_performance(spark, bronze_path, silver_path, gold_path)

        df = spark.read.format("delta").load(f"{gold_path}/gold_channel_performance")

        expected_columns = {
            "signup_month",
            "acquisition_channel",
            "signups",
            "conversions",
            "conversion_rate",
            "avg_days_to_convert",
            "total_new_mrr",
            "avg_new_mrr",
        }
        assert set(df.columns) == expected_columns

        assert df.schema["signup_month"].dataType.typeName() == "date"
        assert df.schema["acquisition_channel"].dataType.typeName() == "string"
        assert df.schema["signups"].dataType.typeName() == "long"
        assert df.schema["conversions"].dataType.typeName() == "long"
        assert df.schema["conversion_rate"].dataType.typeName() == "double"
        assert df.schema["avg_days_to_convert"].dataType.typeName() == "double"
        assert df.schema["total_new_mrr"].dataType.typeName() == "long"
        assert df.schema["avg_new_mrr"].dataType.typeName() == "double"
