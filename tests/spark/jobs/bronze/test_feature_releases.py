"""
Unit tests for the bronze feature releases ingestion.

Tests cover:
- Data ingestion from JSON files
- Schema transformations
- Delta Lake writes
"""

import json
import os

from spark.jobs.bronze.feature_releases.extract import ingest_feature_releases


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
