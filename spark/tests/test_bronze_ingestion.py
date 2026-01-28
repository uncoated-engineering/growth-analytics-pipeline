"""
Unit tests for bronze_ingestion.py

Tests cover:
- Data ingestion from JSON/JSONL files
- Schema transformations
- Delta Lake writes
- Partitioning
- Timestamp additions
"""

import json
import os
from datetime import datetime

from spark.jobs.bronze_ingestion import (
    ingest_conversions,
    ingest_feature_releases,
    ingest_feature_usage_events,
    ingest_user_signups,
    run_bronze_ingestion,
)


class TestIngestFeatureReleases:
    """Test suite for ingest_feature_releases function"""

    def test_ingest_feature_releases_success(self, spark, temp_dir):
        """Test successful ingestion of feature releases"""
        # Prepare test data
        input_path = os.path.join(temp_dir, "feature_releases.json")
        output_path = os.path.join(temp_dir, "bronze_feature_releases")

        test_data = [
            {
                "id": 1,
                "name": "Real-time Collaboration",
                "release_date": "2024-01-15",
                "version": "1.0.0",
            },
            {
                "id": 2,
                "name": "Advanced Analytics",
                "release_date": "2024-02-20",
                "version": "1.1.0",
            },
            {"id": 3, "name": "API Integration", "release_date": "2024-03-10", "version": "1.2.0"},
        ]

        with open(input_path, "w") as f:
            json.dump(test_data, f)

        # Run ingestion
        row_count = ingest_feature_releases(spark, input_path, output_path)

        # Assertions
        assert row_count == 3, "Should ingest 3 feature releases"

        # Verify Delta table
        df = spark.read.format("delta").load(output_path)
        assert df.count() == 3

        # Verify schema
        expected_columns = {
            "feature_id",
            "feature_name",
            "release_date",
            "version",
            "ingestion_timestamp",
        }
        actual_columns = set(df.columns)
        assert (
            actual_columns == expected_columns
        ), f"Expected columns {expected_columns}, got {actual_columns}"

        # Verify data types
        assert df.schema["feature_id"].dataType.typeName() == "integer"
        assert df.schema["feature_name"].dataType.typeName() == "string"
        assert df.schema["ingestion_timestamp"].dataType.typeName() == "timestamp"

        # Verify column renaming (id -> feature_id, name -> feature_name)
        feature_ids = [row.feature_id for row in df.collect()]
        assert set(feature_ids) == {1, 2, 3}

    def test_ingest_feature_releases_empty_file(self, spark, temp_dir):
        """Test ingestion with empty JSON file"""
        input_path = os.path.join(temp_dir, "feature_releases_empty.json")
        output_path = os.path.join(temp_dir, "bronze_feature_releases_empty")

        with open(input_path, "w") as f:
            json.dump([], f)

        row_count = ingest_feature_releases(spark, input_path, output_path)

        assert row_count == 0
        df = spark.read.format("delta").load(output_path)
        assert df.count() == 0


class TestIngestUserSignups:
    """Test suite for ingest_user_signups function"""

    def test_ingest_user_signups_success(self, spark, temp_dir):
        """Test successful ingestion of user signups with partitioning"""
        input_path = os.path.join(temp_dir, "user_signups.jsonl")
        output_path = os.path.join(temp_dir, "bronze_user_signups")

        test_data = [
            {
                "user_id": 1,
                "email": "user1@example.com",
                "signup_date": "2024-01-15",
                "company_size": "Small",
                "industry": "Tech",
            },
            {
                "user_id": 2,
                "email": "user2@example.com",
                "signup_date": "2024-01-15",
                "company_size": "Medium",
                "industry": "Finance",
            },
            {
                "user_id": 3,
                "email": "user3@example.com",
                "signup_date": "2024-01-16",
                "company_size": "Large",
                "industry": "Healthcare",
            },
        ]

        with open(input_path, "w") as f:
            for record in test_data:
                f.write(json.dumps(record) + "\n")

        # Run ingestion
        row_count = ingest_user_signups(spark, input_path, output_path)

        # Assertions
        assert row_count == 3

        # Verify Delta table
        df = spark.read.format("delta").load(output_path)
        assert df.count() == 3

        # Verify schema
        expected_columns = {
            "user_id",
            "email",
            "signup_date",
            "company_size",
            "industry",
            "ingestion_timestamp",
        }
        actual_columns = set(df.columns)
        assert actual_columns == expected_columns

        # Verify partitioning by signup_date
        partition_dirs = os.listdir(output_path)
        partition_dirs = [d for d in partition_dirs if d.startswith("signup_date=")]
        assert len(partition_dirs) > 0, "Should have partition directories"
        assert "signup_date=2024-01-15" in partition_dirs
        assert "signup_date=2024-01-16" in partition_dirs

    def test_ingest_user_signups_ingestion_timestamp(self, spark, temp_dir):
        """Test that ingestion_timestamp is added correctly"""
        input_path = os.path.join(temp_dir, "user_signups.jsonl")
        output_path = os.path.join(temp_dir, "bronze_user_signups")

        test_data = [
            {
                "user_id": 1,
                "email": "user1@example.com",
                "signup_date": "2024-01-15",
                "company_size": "Small",
                "industry": "Tech",
            },
        ]

        with open(input_path, "w") as f:
            for record in test_data:
                f.write(json.dumps(record) + "\n")

        ingest_user_signups(spark, input_path, output_path)

        df = spark.read.format("delta").load(output_path)
        row = df.collect()[0]

        # Verify timestamp exists and is recent
        assert row.ingestion_timestamp is not None
        assert isinstance(row.ingestion_timestamp, datetime)


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


class TestIngestConversions:
    """Test suite for ingest_conversions function"""

    def test_ingest_conversions_success(self, spark, temp_dir):
        """Test successful ingestion of conversions"""
        input_path = os.path.join(temp_dir, "conversions.jsonl")
        output_path = os.path.join(temp_dir, "bronze_conversions")

        test_data = [
            {
                "user_id": 1,
                "conversion_date": "2024-01-20",
                "plan": "Pro",
                "mrr": 99,
                "signup_date": "2024-01-15",
                "days_to_convert": 5,
                "used_real_time_collab": True,
            },
            {
                "user_id": 2,
                "conversion_date": "2024-01-25",
                "plan": "Enterprise",
                "mrr": 299,
                "signup_date": "2024-01-10",
                "days_to_convert": 15,
                "used_real_time_collab": False,
            },
        ]

        with open(input_path, "w") as f:
            for record in test_data:
                f.write(json.dumps(record) + "\n")

        # Run ingestion
        row_count = ingest_conversions(spark, input_path, output_path)

        # Assertions
        assert row_count == 2

        # Verify Delta table
        df = spark.read.format("delta").load(output_path)
        assert df.count() == 2

        # Verify schema
        expected_columns = {
            "user_id",
            "conversion_date",
            "plan",
            "mrr",
            "signup_date",
            "days_to_convert",
            "used_real_time_collab",
            "ingestion_timestamp",
        }
        actual_columns = set(df.columns)
        assert actual_columns == expected_columns

        # Verify data types
        assert df.schema["user_id"].dataType.typeName() == "integer"
        assert df.schema["mrr"].dataType.typeName() == "integer"
        assert df.schema["used_real_time_collab"].dataType.typeName() == "boolean"

        # Verify data values
        pro_user = df.filter(df.plan == "Pro").first()
        assert pro_user.mrr == 99
        assert pro_user.used_real_time_collab is True


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

        # Run bronze ingestion
        stats = run_bronze_ingestion(spark, raw_data_path, bronze_path)

        # Verify stats
        assert stats["feature_releases"] == 1
        assert stats["user_signups"] == 1
        assert stats["feature_usage_events"] == 1
        assert stats["conversions"] == 1

        # Verify all Delta tables exist and have data
        for table_name in [
            "feature_releases",
            "user_signups",
            "feature_usage_events",
            "conversions",
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

        stats = run_bronze_ingestion(spark, raw_data_path, bronze_path)

        # Verify stats structure
        assert isinstance(stats, dict)
        assert "feature_releases" in stats
        assert "user_signups" in stats
        assert "feature_usage_events" in stats
        assert "conversions" in stats
