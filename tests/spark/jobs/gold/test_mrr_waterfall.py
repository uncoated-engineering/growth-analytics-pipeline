"""
Unit tests for the gold MRR waterfall aggregation.

Tests cover:
- Movement classification (new business, expansion, contraction, churn)
- Running starting/ending MRR and the ending = starting + net_new identity
- Net revenue retention math and its NULL first month
- Output schema
"""

import os
from datetime import date

from pyspark.sql.functions import col

from spark.jobs.gold.mrr_waterfall.aggregation import calculate_mrr_waterfall
from tests.spark.helpers import setup_bronze_subscription_events


class TestCalculateMrrWaterfall:
    """Test suite for calculate_mrr_waterfall function"""

    def test_waterfall_basic_scenario(self, spark, temp_dir):
        """Test a hand-computable 3-month scenario covering all movement types."""
        subscription_events = [
            # January: two new customers (100 + 200)
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
                "mrr": 200,
                "previous_plan": None,
                "previous_mrr": None,
            },
            # February: user 1 expands 100 -> 150 (+50)
            {
                "event_id": 3,
                "user_id": 1,
                "event_date": "2024-02-15",
                "event_type": "seats_expanded",
                "plan": "pro",
                "mrr": 150,
                "previous_plan": "pro",
                "previous_mrr": 100,
            },
            # March: user 2 churns (-200)
            {
                "event_id": 4,
                "user_id": 2,
                "event_date": "2024-03-05",
                "event_type": "subscription_cancelled",
                "plan": None,
                "mrr": 0,
                "previous_plan": "enterprise",
                "previous_mrr": 200,
            },
        ]

        bronze_path = setup_bronze_subscription_events(spark, temp_dir, subscription_events)
        silver_path = os.path.join(temp_dir, "silver")
        gold_path = os.path.join(temp_dir, "gold")

        row_count = calculate_mrr_waterfall(spark, bronze_path, silver_path, gold_path)

        assert row_count == 3

        df = spark.read.format("delta").load(f"{gold_path}/gold_mrr_waterfall")
        rows = {row.month: row for row in df.collect()}

        # January: 300 of new business, no prior MRR
        jan = rows[date(2024, 1, 1)]
        assert jan.starting_mrr == 0
        assert jan.new_business_mrr == 300
        assert jan.expansion_mrr == 0
        assert jan.contraction_mrr == 0
        assert jan.churned_mrr == 0
        assert jan.net_new_mrr == 300
        assert jan.ending_mrr == 300
        assert jan.new_customers == 2
        assert jan.churned_customers == 0
        assert jan.net_revenue_retention is None

        # February: +50 expansion on a 300 base
        feb = rows[date(2024, 2, 1)]
        assert feb.starting_mrr == 300
        assert feb.new_business_mrr == 0
        assert feb.expansion_mrr == 50
        assert feb.contraction_mrr == 0
        assert feb.churned_mrr == 0
        assert feb.net_new_mrr == 50
        assert feb.ending_mrr == 350
        assert feb.new_customers == 0
        assert feb.churned_customers == 0
        assert abs(feb.net_revenue_retention - 350.0 / 300.0) < 0.001

        # March: -200 churn on a 350 base
        mar = rows[date(2024, 3, 1)]
        assert mar.starting_mrr == 350
        assert mar.new_business_mrr == 0
        assert mar.expansion_mrr == 0
        assert mar.contraction_mrr == 0
        assert mar.churned_mrr == 200
        assert mar.net_new_mrr == -200
        assert mar.ending_mrr == 150
        assert mar.new_customers == 0
        assert mar.churned_customers == 1
        assert abs(mar.net_revenue_retention - (350.0 - 200.0) / 350.0) < 0.001

        # Waterfall identity holds for every row
        for row in rows.values():
            assert row.ending_mrr == row.starting_mrr + row.net_new_mrr

    def test_downgrade_and_contraction_classified_as_contraction(self, spark, temp_dir):
        """Test that plan downgrades and seat contractions both count as contraction MRR."""
        subscription_events = [
            {
                "event_id": 1,
                "user_id": 1,
                "event_date": "2024-01-05",
                "event_type": "subscription_started",
                "plan": "enterprise",
                "mrr": 300,
                "previous_plan": None,
                "previous_mrr": None,
            },
            # February: downgrade 300 -> 100 (-200)
            {
                "event_id": 2,
                "user_id": 1,
                "event_date": "2024-02-10",
                "event_type": "plan_downgraded",
                "plan": "pro",
                "mrr": 100,
                "previous_plan": "enterprise",
                "previous_mrr": 300,
            },
            # February: seats contracted 100 -> 80 (-20)
            {
                "event_id": 3,
                "user_id": 1,
                "event_date": "2024-02-20",
                "event_type": "seats_contracted",
                "plan": "pro",
                "mrr": 80,
                "previous_plan": "pro",
                "previous_mrr": 100,
            },
        ]

        bronze_path = setup_bronze_subscription_events(spark, temp_dir, subscription_events)
        silver_path = os.path.join(temp_dir, "silver")
        gold_path = os.path.join(temp_dir, "gold")

        calculate_mrr_waterfall(spark, bronze_path, silver_path, gold_path)

        df = spark.read.format("delta").load(f"{gold_path}/gold_mrr_waterfall")
        feb = df.filter(col("month") == "2024-02-01").collect()[0]

        # contraction = 200 (downgrade) + 20 (seat contraction), no expansion/churn
        assert feb.contraction_mrr == 220
        assert feb.expansion_mrr == 0
        assert feb.churned_mrr == 0
        assert feb.net_new_mrr == -220
        assert feb.starting_mrr == 300
        assert feb.ending_mrr == 80
        assert abs(feb.net_revenue_retention - (300.0 - 220.0) / 300.0) < 0.001

    def test_first_month_nrr_is_null(self, spark, temp_dir):
        """Test that NRR is NULL when starting MRR is zero (the first month)."""
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
        gold_path = os.path.join(temp_dir, "gold")

        calculate_mrr_waterfall(spark, bronze_path, silver_path, gold_path)

        df = spark.read.format("delta").load(f"{gold_path}/gold_mrr_waterfall")
        row = df.collect()[0]
        assert row.starting_mrr == 0
        assert row.net_revenue_retention is None

    def test_output_schema(self, spark, temp_dir):
        """Test that the gold table has the expected schema."""
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
        gold_path = os.path.join(temp_dir, "gold")

        calculate_mrr_waterfall(spark, bronze_path, silver_path, gold_path)

        df = spark.read.format("delta").load(f"{gold_path}/gold_mrr_waterfall")

        expected_columns = {
            "month",
            "starting_mrr",
            "new_business_mrr",
            "expansion_mrr",
            "contraction_mrr",
            "churned_mrr",
            "net_new_mrr",
            "ending_mrr",
            "new_customers",
            "churned_customers",
            "net_revenue_retention",
        }
        assert set(df.columns) == expected_columns

        assert df.schema["month"].dataType.typeName() == "date"
        assert df.schema["starting_mrr"].dataType.typeName() == "long"
        assert df.schema["new_business_mrr"].dataType.typeName() == "long"
        assert df.schema["expansion_mrr"].dataType.typeName() == "long"
        assert df.schema["contraction_mrr"].dataType.typeName() == "long"
        assert df.schema["churned_mrr"].dataType.typeName() == "long"
        assert df.schema["net_new_mrr"].dataType.typeName() == "long"
        assert df.schema["ending_mrr"].dataType.typeName() == "long"
        assert df.schema["new_customers"].dataType.typeName() == "long"
        assert df.schema["churned_customers"].dataType.typeName() == "long"
        assert df.schema["net_revenue_retention"].dataType.typeName() == "double"
