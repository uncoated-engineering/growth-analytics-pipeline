"""
Unit tests for the silver user dimension transformation.

Tests cover:
- User dimension table (plan enrichment, deduplication)
"""

import os
from datetime import date

from pyspark.sql.functions import col

from spark.jobs.silver.user_dim.transformation import maintain_user_dim
from tests.spark.helpers import (
    setup_bronze_conversions,
    setup_bronze_user_signups,
)


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

        bronze_path = os.path.join(temp_dir, "bronze")
        setup_bronze_user_signups(spark, temp_dir, signups, bronze_path)
        setup_bronze_conversions(spark, temp_dir, conversions, bronze_path)
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

        bronze_path = os.path.join(temp_dir, "bronze")
        setup_bronze_user_signups(spark, temp_dir, signups, bronze_path)
        setup_bronze_conversions(spark, temp_dir, conversions, bronze_path)
        silver_path = os.path.join(temp_dir, "silver")

        maintain_user_dim(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_user_dim")
        expected_columns = {
            "user_id",
            "signup_date",
            "company_size",
            "industry",
            "current_plan",
        }
        assert set(df.columns) == expected_columns

        assert df.schema["user_id"].dataType.typeName() == "integer"
        assert df.schema["signup_date"].dataType.typeName() == "date"
        assert df.schema["company_size"].dataType.typeName() == "string"
        assert df.schema["industry"].dataType.typeName() == "string"
        assert df.schema["current_plan"].dataType.typeName() == "string"

    def test_user_dim_converted_user_has_plan(self, spark, temp_dir):
        """Test that converted users get their plan from conversions."""
        signups = [
            {
                "user_id": 1,
                "email": "user1@test.com",
                "signup_date": "2024-01-15",
                "company_size": "Small",
                "industry": "Tech",
            },
        ]
        conversions = [
            {
                "user_id": 1,
                "conversion_date": "2024-02-01",
                "plan": "Enterprise",
                "mrr": 299,
                "signup_date": "2024-01-15",
                "days_to_convert": 17,
                "used_real_time_collab": True,
            },
        ]

        bronze_path = os.path.join(temp_dir, "bronze")
        setup_bronze_user_signups(spark, temp_dir, signups, bronze_path)
        setup_bronze_conversions(spark, temp_dir, conversions, bronze_path)
        silver_path = os.path.join(temp_dir, "silver")

        maintain_user_dim(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_user_dim")
        row = df.filter(col("user_id") == 1).collect()[0]
        assert row.current_plan == "Enterprise"

    def test_user_dim_non_converted_user_defaults_to_free(self, spark, temp_dir):
        """Test that non-converted users default to 'free' plan."""
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

        bronze_path = os.path.join(temp_dir, "bronze")
        setup_bronze_user_signups(spark, temp_dir, signups, bronze_path)
        setup_bronze_conversions(spark, temp_dir, conversions, bronze_path)
        silver_path = os.path.join(temp_dir, "silver")

        maintain_user_dim(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_user_dim")

        # User 1 has Pro plan
        user1 = df.filter(col("user_id") == 1).collect()[0]
        assert user1.current_plan == "Pro"

        # User 2 defaults to free
        user2 = df.filter(col("user_id") == 2).collect()[0]
        assert user2.current_plan == "free"

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
        conversions = []  # No conversions

        bronze_path = os.path.join(temp_dir, "bronze")
        setup_bronze_user_signups(spark, temp_dir, signups, bronze_path)
        setup_bronze_conversions(spark, temp_dir, conversions, bronze_path)
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
        conversions = []

        bronze_path = os.path.join(temp_dir, "bronze")
        setup_bronze_user_signups(spark, temp_dir, signups, bronze_path)
        setup_bronze_conversions(spark, temp_dir, conversions, bronze_path)
        silver_path = os.path.join(temp_dir, "silver")

        maintain_user_dim(spark, bronze_path, silver_path)

        df = spark.read.format("delta").load(f"{silver_path}/silver_user_dim")
        assert "email" not in df.columns
