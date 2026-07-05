"""
Unit tests for the bronze conversions ingestion.

Tests cover:
- Data ingestion from JSONL files
- Schema transformations
- Delta Lake writes
"""

import json
import os

from spark.jobs.bronze.conversions.extract import ingest_conversions


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
