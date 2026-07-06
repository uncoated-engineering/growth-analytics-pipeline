"""
Unit tests for the bronze marketing attribution ingestion.

Tests cover:
- Data ingestion from JSONL files
- Schema transformations
- Delta Lake writes
"""

import json
import os

from spark.jobs.bronze.marketing_attribution.extract import ingest_marketing_attribution


class TestIngestMarketingAttribution:
    """Test suite for ingest_marketing_attribution function"""

    def test_ingest_marketing_attribution_success(self, spark, temp_dir):
        """Test successful ingestion of marketing attribution records"""
        input_path = os.path.join(temp_dir, "marketing_attribution.jsonl")
        output_path = os.path.join(temp_dir, "bronze_marketing_attribution")

        test_data = [
            {
                "user_id": 1,
                "channel": "paid_search",
                "campaign": "brand_q1",
                "first_touch_date": "2024-01-10",
            },
            {
                "user_id": 2,
                "channel": "organic",
                "campaign": "none",
                "first_touch_date": "2024-01-12",
            },
        ]

        with open(input_path, "w") as f:
            for record in test_data:
                f.write(json.dumps(record) + "\n")

        # Run ingestion
        row_count = ingest_marketing_attribution(spark, input_path, output_path)

        # Assertions
        assert row_count == 2

        # Verify Delta table
        df = spark.read.format("delta").load(output_path)
        assert df.count() == 2

        # Verify schema
        expected_columns = {
            "user_id",
            "channel",
            "campaign",
            "first_touch_date",
            "ingestion_timestamp",
        }
        actual_columns = set(df.columns)
        assert actual_columns == expected_columns

        # Verify data types
        assert df.schema["user_id"].dataType.typeName() == "integer"
        assert df.schema["channel"].dataType.typeName() == "string"
        assert df.schema["campaign"].dataType.typeName() == "string"
        assert df.schema["first_touch_date"].dataType.typeName() == "string"
        assert df.schema["ingestion_timestamp"].dataType.typeName() == "timestamp"

        # Verify data values
        paid_search = df.filter(df.channel == "paid_search").first()
        assert paid_search.user_id == 1
        assert paid_search.campaign == "brand_q1"
        assert paid_search.first_touch_date == "2024-01-10"
        assert paid_search.ingestion_timestamp is not None
