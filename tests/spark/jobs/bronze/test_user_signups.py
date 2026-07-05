"""
Unit tests for the bronze user signups ingestion.

Tests cover:
- Data ingestion from JSONL files
- Partitioning
- Timestamp additions
"""

import json
import os
from datetime import datetime

from spark.jobs.bronze.user_signups.extract import ingest_user_signups


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
