"""
Unit tests for the bronze feature usage events ingestion.

Tests cover:
- Data ingestion from JSONL files
- Schema transformations
- Partitioning
"""

import json
import os
from datetime import datetime

from spark.jobs.bronze.feature_usage_events.extract import ingest_feature_usage_events


class TestIngestFeatureUsageEvents:
    """Test suite for ingest_feature_usage_events function"""

    def test_ingest_feature_usage_events_success(self, spark, temp_dir):
        """Test successful ingestion of feature usage events"""
        input_path = os.path.join(temp_dir, "feature_usage_events.jsonl")
        output_path = os.path.join(temp_dir, "bronze_feature_usage_events")

        test_data = [
            {
                "timestamp": "2024-01-15 10:30:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "Collaboration",
                "event_type": "click",
            },
            {
                "timestamp": "2024-01-15 11:45:00",
                "user_id": 2,
                "feature_id": 2,
                "feature_name": "Analytics",
                "event_type": "view",
            },
            {
                "timestamp": "2024-01-16 09:00:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "Collaboration",
                "event_type": "usage",
            },
        ]

        with open(input_path, "w") as f:
            for record in test_data:
                f.write(json.dumps(record) + "\n")

        # Run ingestion
        row_count = ingest_feature_usage_events(spark, input_path, output_path)

        # Assertions
        assert row_count == 3

        # Verify Delta table
        df = spark.read.format("delta").load(output_path)
        assert df.count() == 3

        # Verify schema
        expected_columns = {
            "event_id",
            "user_id",
            "feature_id",
            "feature_name",
            "event_type",
            "event_timestamp",
            "event_date",
            "ingestion_timestamp",
        }
        actual_columns = set(df.columns)
        assert actual_columns == expected_columns

        # Verify timestamp conversion
        row = df.filter(df.user_id == 1).first()
        assert row.event_timestamp is not None
        assert isinstance(row.event_timestamp, datetime)

        # Verify event_date extraction
        assert row.event_date is not None

        # Verify event_id generation
        event_ids = [row.event_id for row in df.collect()]
        assert len(event_ids) == len(set(event_ids)), "Event IDs should be unique"

        # Verify partitioning
        partition_dirs = os.listdir(output_path)
        partition_dirs = [d for d in partition_dirs if d.startswith("event_date=")]
        assert len(partition_dirs) > 0, "Should have partition directories"

    def test_ingest_feature_usage_events_column_order(self, spark, temp_dir):
        """Test that columns are in the correct order"""
        input_path = os.path.join(temp_dir, "feature_usage_events.jsonl")
        output_path = os.path.join(temp_dir, "bronze_feature_usage_events")

        test_data = [
            {
                "timestamp": "2024-01-15 10:30:00",
                "user_id": 1,
                "feature_id": 1,
                "feature_name": "Collaboration",
                "event_type": "click",
            },
        ]

        with open(input_path, "w") as f:
            for record in test_data:
                f.write(json.dumps(record) + "\n")

        ingest_feature_usage_events(spark, input_path, output_path)

        df = spark.read.format("delta").load(output_path)
        expected_order = [
            "event_id",
            "user_id",
            "feature_id",
            "feature_name",
            "event_type",
            "event_timestamp",
            "event_date",
            "ingestion_timestamp",
        ]
        assert df.columns == expected_order
