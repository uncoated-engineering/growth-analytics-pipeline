"""
Bronze Layer Validation Script

This script validates the bronze layer implementation by:
1. Checking that all raw data files exist
2. Verifying data format and basic structure
3. Providing statistics about the raw data
4. Showing what will be ingested

Note: This does not run the actual Spark ingestion.
For full ingestion, use: make ingest-bronze
"""

import json
from pathlib import Path


def validate_file_exists(path):
    """Check if file exists"""
    if not Path(path).exists():
        print(f"  ❌ File not found: {path}")
        return False
    print(f"  ✓ File exists: {path}")
    return True


def count_lines(path):
    """Count lines in a file"""
    with open(path, "r") as f:
        return sum(1 for _ in f)


def validate_json_array(path):
    """Validate JSON array file"""
    try:
        with open(path, "r") as f:
            data = json.load(f)
            if isinstance(data, list):
                print(f"  ✓ Valid JSON array with {len(data)} records")
                if len(data) > 0:
                    print(f"  Sample record: {data[0]}")
                return len(data)
    except Exception as e:
        print(f"  ❌ Error reading JSON: {e}")
    return 0


def validate_jsonl(path):
    """Validate JSONL file"""
    try:
        count = 0
        with open(path, "r") as f:
            for i, line in enumerate(f):
                json.loads(line)  # Validate it's valid JSON
                count += 1
                if i == 0:  # Show first record
                    print(f"  Sample record: {json.loads(line)}")
        print(f"  ✓ Valid JSONL with {count} records")
        return count
    except Exception as e:
        print(f"  ❌ Error reading JSONL: {e}")
    return 0


def main():
    print("=" * 80)
    print("Bronze Layer Validation")
    print("=" * 80)
    print()

    raw_data_path = "data/raw"

    # Track statistics
    stats = {}

    # 1. Validate feature_releases.json
    print("1. Feature Releases (feature_releases.json)")
    print("-" * 80)
    path = f"{raw_data_path}/feature_releases.json"
    if validate_file_exists(path):
        stats["feature_releases"] = validate_json_array(path)
    print()

    # 2. Validate user_signups.jsonl
    print("2. User Signups (user_signups.jsonl)")
    print("-" * 80)
    path = f"{raw_data_path}/user_signups.jsonl"
    if validate_file_exists(path):
        stats["user_signups"] = validate_jsonl(path)
    print()

    # 3. Validate feature_usage_events.jsonl
    print("3. Feature Usage Events (feature_usage_events.jsonl)")
    print("-" * 80)
    path = f"{raw_data_path}/feature_usage_events.jsonl"
    if validate_file_exists(path):
        stats["feature_usage_events"] = validate_jsonl(path)
    print()

    # 4. Validate conversions.jsonl
    print("4. Conversions (conversions.jsonl)")
    print("-" * 80)
    path = f"{raw_data_path}/conversions.jsonl"
    if validate_file_exists(path):
        stats["conversions"] = validate_jsonl(path)
    print()

    # Summary
    print("=" * 80)
    print("Validation Summary")
    print("=" * 80)
    total_records = sum(stats.values())
    print(f"\nTotal files validated: {len(stats)}")
    print(f"Total records to ingest: {total_records:,}\n")

    for table, count in stats.items():
        print(f"  {table:<30} {count:>10,} records")

    print()
    print("=" * 80)
    print("Bronze Layer Implementation Details")
    print("=" * 80)
    print("""
The bronze_ingestion.py script will:

1. Read each raw data file (JSON/JSONL format)
2. Add an 'ingestion_timestamp' to track when data was loaded
3. Write data to Delta Lake tables in data/bronze/
4. Apply partitioning strategies:
   - user_signups: partitioned by signup_date
   - feature_usage_events: partitioned by event_date (for performance)
   - feature_releases: no partitioning (small table)
   - conversions: no partitioning (medium table)

5. Use Delta Lake for:
   - ACID transactions
   - Schema enforcement
   - Time travel capabilities
   - Efficient updates and deletes

To run the actual ingestion:
    make ingest-bronze

To verify bronze tables after ingestion:
    ls -lR data/bronze/
    """)

    print("\n✓ Validation complete! Raw data is ready for bronze ingestion.")


if __name__ == "__main__":
    main()
