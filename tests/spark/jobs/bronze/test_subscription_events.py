"""
Unit tests for the bronze subscription events ingestion.

Tests cover:
- Data ingestion from JSONL files
- Schema transformations
- Delta Lake writes
"""

import json
import os

from spark.jobs.bronze.subscription_events.extract import ingest_subscription_events


class TestIngestSubscriptionEvents:
    """Test suite for ingest_subscription_events function"""

    def test_ingest_subscription_events_success(self, spark, temp_dir):
        """Test successful ingestion of subscription events"""
        input_path = os.path.join(temp_dir, "subscription_events.jsonl")
        output_path = os.path.join(temp_dir, "bronze_subscription_events")

        test_data = [
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
            {
                "event_id": 2,
                "user_id": 1,
                "event_date": "2024-03-01",
                "event_type": "plan_upgraded",
                "plan": "enterprise",
                "mrr": 299,
                "previous_plan": "pro",
                "previous_mrr": 99,
            },
        ]

        with open(input_path, "w") as f:
            for record in test_data:
                f.write(json.dumps(record) + "\n")

        # Run ingestion
        row_count = ingest_subscription_events(spark, input_path, output_path)

        # Assertions
        assert row_count == 2

        # Verify Delta table
        df = spark.read.format("delta").load(output_path)
        assert df.count() == 2

        # Verify schema
        expected_columns = {
            "event_id",
            "user_id",
            "event_date",
            "event_type",
            "plan",
            "mrr",
            "previous_plan",
            "previous_mrr",
            "ingestion_timestamp",
        }
        actual_columns = set(df.columns)
        assert actual_columns == expected_columns

        # Verify data types
        assert df.schema["event_id"].dataType.typeName() == "integer"
        assert df.schema["user_id"].dataType.typeName() == "integer"
        assert df.schema["event_date"].dataType.typeName() == "string"
        assert df.schema["event_type"].dataType.typeName() == "string"
        assert df.schema["plan"].dataType.typeName() == "string"
        assert df.schema["mrr"].dataType.typeName() == "integer"
        assert df.schema["previous_plan"].dataType.typeName() == "string"
        assert df.schema["previous_mrr"].dataType.typeName() == "integer"
        assert df.schema["ingestion_timestamp"].dataType.typeName() == "timestamp"

        # Verify data values
        started = df.filter(df.event_type == "subscription_started").first()
        assert started.event_id == 1
        assert started.plan == "pro"
        assert started.mrr == 99
        assert started.previous_plan is None
        assert started.previous_mrr is None

        upgraded = df.filter(df.event_type == "plan_upgraded").first()
        assert upgraded.plan == "enterprise"
        assert upgraded.mrr == 299
        assert upgraded.previous_plan == "pro"
        assert upgraded.previous_mrr == 99
        assert upgraded.ingestion_timestamp is not None
