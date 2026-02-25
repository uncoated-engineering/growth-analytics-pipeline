"""
Tests for the declarative pipeline framework.

Covers:
    - Manifest loading and validation
    - Schema registry (YAML -> StructType)
    - Quality engine (declarative rule execution)
    - Bronze engine (config-driven ingestion)
    - Lineage graph and impact analysis
    - Metrics engine (KPI evaluation)
"""

import json
import os

import pytest
import yaml
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark.jobs.declarative.manifest import (
    SchemaField,
    load_manifest,
)
from spark.jobs.declarative.schema_registry import SchemaRegistry, fields_to_struct

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

MANIFEST_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "pipeline.yaml")


@pytest.fixture(scope="session")
def manifest():
    """Load the real pipeline manifest."""
    return load_manifest(MANIFEST_PATH)


@pytest.fixture()
def mini_manifest(tmp_path):
    """Create a minimal test manifest for isolated tests."""
    config = {
        "version": "1.0",
        "name": "test-pipeline",
        "description": "Test pipeline",
        "paths": {
            "raw": str(tmp_path / "raw"),
            "bronze": str(tmp_path / "bronze"),
            "silver": str(tmp_path / "silver"),
            "gold": str(tmp_path / "gold"),
        },
        "bronze": {
            "users": {
                "description": "Test users table",
                "source": {
                    "file": "users.jsonl",
                    "format": "json",
                },
                "schema": {
                    "input": [
                        {"name": "id", "type": "integer"},
                        {"name": "name", "type": "string"},
                        {"name": "email", "type": "string"},
                    ],
                    "output": [
                        {"name": "user_id", "type": "integer"},
                        {"name": "name", "type": "string"},
                        {"name": "email", "type": "string"},
                        {"name": "ingestion_timestamp", "type": "timestamp"},
                    ],
                },
                "transformations": [
                    {"type": "rename", "columns": {"id": "user_id"}},
                    {
                        "type": "add_column",
                        "name": "ingestion_timestamp",
                        "function": "current_timestamp",
                    },
                ],
                "write": {"mode": "append"},
                "quality_rules": {
                    "input": [
                        {"rule": "not_null", "columns": ["id", "name"]},
                    ],
                    "output": [
                        {"rule": "not_null", "columns": ["user_id", "name"]},
                        {"rule": "min_rows", "value": 1},
                    ],
                },
                "freshness": {"max_age_hours": 24},
                "airflow": {"schedule": "@daily", "dataset": "delta://bronze/users"},
            },
            "events": {
                "description": "Test events table",
                "source": {
                    "file": "events.jsonl",
                    "format": "json",
                },
                "schema": {
                    "input": [
                        {"name": "event_id", "type": "integer"},
                        {"name": "user_id", "type": "integer"},
                        {"name": "action", "type": "string"},
                    ],
                    "output": [
                        {"name": "event_id", "type": "integer"},
                        {"name": "user_id", "type": "integer"},
                        {"name": "action", "type": "string"},
                        {"name": "ingestion_timestamp", "type": "timestamp"},
                    ],
                },
                "transformations": [
                    {
                        "type": "add_column",
                        "name": "ingestion_timestamp",
                        "function": "current_timestamp",
                    },
                ],
                "write": {"mode": "append"},
                "airflow": {"schedule": "@daily", "dataset": "delta://bronze/events"},
            },
        },
        "silver": {
            "user_stats": {
                "description": "User statistics",
                "transformation": "custom",
                "module": "test.module",
                "function": "transform_stats",
                "depends_on": ["bronze.users", "bronze.events"],
                "schema": {
                    "output": [
                        {"name": "user_id", "type": "integer"},
                        {"name": "event_count", "type": "long"},
                    ],
                },
                "quality_rules": {
                    "output": [
                        {"rule": "not_null", "columns": ["user_id"]},
                        {"rule": "min_rows", "value": 1},
                    ],
                },
                "airflow": {
                    "schedule": {"datasets": ["delta://bronze/users", "delta://bronze/events"]},
                    "dataset": "delta://silver/user_stats",
                },
            },
        },
        "gold": {
            "summary": {
                "description": "Summary metrics",
                "transformation": "custom",
                "module": "test.module",
                "function": "calc_summary",
                "depends_on": ["silver.user_stats"],
                "schema": {
                    "output": [
                        {"name": "feature_name", "type": "string"},
                        {"name": "cohort", "type": "string"},
                        {"name": "conversion_rate", "type": "double"},
                        {"name": "avg_mrr", "type": "double"},
                    ],
                },
                "airflow": {
                    "schedule": {"datasets": ["delta://silver/user_stats"]},
                    "dataset": "delta://gold/summary",
                },
                "metrics": [
                    {
                        "name": "test_lift",
                        "description": "Test metric",
                        "formula": (
                            "conversion_rate[used_feature]" " / conversion_rate[available_not_used]"
                        ),
                        "threshold": {"warning": 1.5, "critical": 1.0, "direction": "above"},
                    },
                ],
            },
        },
    }
    manifest_path = str(tmp_path / "pipeline.yaml")
    with open(manifest_path, "w") as f:
        yaml.dump(config, f)
    return load_manifest(manifest_path)


# ===========================================================================
# Test Manifest Loading & Validation
# ===========================================================================


class TestManifest:
    def test_load_real_manifest(self, manifest):
        assert manifest.name == "growth-analytics-pipeline"
        assert manifest.version == "1.0"

    def test_manifest_has_all_tables(self, manifest):
        assert len(manifest.get_layer("bronze")) == 4
        assert len(manifest.get_layer("silver")) == 3
        assert len(manifest.get_layer("gold")) == 1

    def test_manifest_table_access(self, manifest):
        table = manifest.get_table("bronze.feature_releases")
        assert table.name == "feature_releases"
        assert table.layer == "bronze"
        assert table.source is not None
        assert table.source.file == "feature_releases.json"

    def test_manifest_dependencies(self, manifest):
        gold = manifest.get_table("gold.feature_conversion_impact")
        assert "silver.user_dim" in gold.depends_on
        assert "silver.feature_states" in gold.depends_on
        assert "silver.feature_usage_facts" in gold.depends_on
        assert "bronze.conversions" in gold.depends_on

    def test_manifest_downstream(self, manifest):
        downstream = manifest.get_downstream("bronze.feature_releases")
        names = [t.qualified_name for t in downstream]
        assert "silver.feature_states" in names

    def test_manifest_upstream(self, manifest):
        upstream = manifest.get_upstream("silver.user_dim")
        names = [t.qualified_name for t in upstream]
        assert "bronze.user_signups" in names
        assert "bronze.conversions" in names

    def test_manifest_topological_order(self, manifest):
        order = manifest.topological_order()
        names = [t.qualified_name for t in order]
        # Bronze tables must come before silver
        for bronze in ["bronze.feature_releases", "bronze.user_signups"]:
            for silver in ["silver.feature_states", "silver.user_dim"]:
                if bronze in names and silver in names:
                    assert names.index(bronze) < names.index(silver)

    def test_manifest_validation_passes(self, manifest):
        errors = manifest.validate()
        assert errors == []

    def test_manifest_summary(self, manifest):
        summary = manifest.summary()
        assert "growth-analytics-pipeline" in summary
        assert "BRONZE" in summary
        assert "SILVER" in summary
        assert "GOLD" in summary

    def test_manifest_quality_rules(self, manifest):
        table = manifest.get_table("bronze.feature_releases")
        assert len(table.input_quality_rules) > 0
        assert len(table.output_quality_rules) > 0
        assert table.input_quality_rules[0].rule == "not_null"

    def test_manifest_freshness(self, manifest):
        table = manifest.get_table("bronze.feature_releases")
        assert table.freshness is not None
        assert table.freshness.max_age_hours == 24

    def test_manifest_metrics(self, manifest):
        table = manifest.get_table("gold.feature_conversion_impact")
        assert len(table.metrics) == 3
        assert table.metrics[0].name == "feature_adoption_lift"

    def test_manifest_transformations(self, manifest):
        table = manifest.get_table("bronze.feature_releases")
        assert len(table.transformations) == 2
        assert table.transformations[0].type == "rename"
        assert table.transformations[1].type == "add_column"

    def test_mini_manifest(self, mini_manifest):
        assert mini_manifest.name == "test-pipeline"
        assert len(mini_manifest.tables) == 4
        assert len(mini_manifest.get_layer("bronze")) == 2
        assert len(mini_manifest.get_layer("silver")) == 1
        assert len(mini_manifest.get_layer("gold")) == 1

    def test_nonexistent_table_raises(self, manifest):
        with pytest.raises(KeyError):
            manifest.get_table("bronze.nonexistent")

    def test_invalid_dependency_caught(self, tmp_path):
        config = {
            "version": "1.0",
            "name": "bad-pipeline",
            "description": "Bad",
            "paths": {"raw": "data/raw"},
            "silver": {
                "broken": {
                    "description": "Broken table",
                    "transformation": "custom",
                    "module": "test",
                    "function": "test",
                    "depends_on": ["bronze.nonexistent"],
                    "schema": {"output": [{"name": "id", "type": "integer"}]},
                },
            },
        }
        path = str(tmp_path / "bad_pipeline.yaml")
        with open(path, "w") as f:
            yaml.dump(config, f)
        with pytest.raises(ValueError, match="not defined"):
            load_manifest(path)


# ===========================================================================
# Test Schema Registry
# ===========================================================================


class TestSchemaRegistry:
    def test_fields_to_struct_basic(self):
        fields = [
            SchemaField("id", "integer"),
            SchemaField("name", "string"),
            SchemaField("active", "boolean"),
        ]
        schema = fields_to_struct(fields)
        assert len(schema.fields) == 3
        assert schema["id"].dataType == IntegerType()
        assert schema["name"].dataType == StringType()
        assert schema["active"].dataType == BooleanType()

    def test_fields_to_struct_all_types(self):
        fields = [
            SchemaField("a", "integer"),
            SchemaField("b", "long"),
            SchemaField("c", "double"),
            SchemaField("d", "float"),
            SchemaField("e", "string"),
            SchemaField("f", "boolean"),
            SchemaField("g", "date"),
            SchemaField("h", "timestamp"),
        ]
        schema = fields_to_struct(fields)
        assert schema["a"].dataType == IntegerType()
        assert schema["b"].dataType == LongType()
        assert schema["c"].dataType == DoubleType()
        assert schema["g"].dataType == DateType()
        assert schema["h"].dataType == TimestampType()

    def test_fields_to_struct_nullable(self):
        fields = [
            SchemaField("id", "integer", nullable=False),
            SchemaField("name", "string", nullable=True),
        ]
        schema = fields_to_struct(fields)
        assert schema["id"].nullable is False
        assert schema["name"].nullable is True

    def test_unknown_type_raises(self):
        fields = [SchemaField("x", "unknown_type")]
        with pytest.raises(ValueError, match="Unknown type"):
            fields_to_struct(fields)

    def test_registry_from_real_manifest(self, manifest):
        registry = SchemaRegistry(manifest)
        input_schema = registry.get_input_schema("bronze.feature_releases")
        assert len(input_schema.fields) == 4
        assert input_schema["id"].dataType == IntegerType()
        assert input_schema["name"].dataType == StringType()

    def test_registry_output_schema(self, manifest):
        registry = SchemaRegistry(manifest)
        output_schema = registry.get_output_schema("bronze.feature_releases")
        assert len(output_schema.fields) == 5
        assert output_schema["feature_id"].dataType == IntegerType()
        assert output_schema["ingestion_timestamp"].dataType == TimestampType()

    def test_registry_silver_schema(self, manifest):
        registry = SchemaRegistry(manifest)
        output_schema = registry.get_output_schema("silver.feature_states")
        assert output_schema["feature_id"].dataType == IntegerType()
        assert output_schema["is_current"].dataType == BooleanType()
        assert output_schema["effective_from"].dataType == DateType()

    def test_registry_gold_schema(self, manifest):
        registry = SchemaRegistry(manifest)
        output_schema = registry.get_output_schema("gold.feature_conversion_impact")
        assert output_schema["conversion_rate"].dataType == DoubleType()
        assert output_schema["total_users"].dataType == LongType()

    def test_registry_caching(self, manifest):
        registry = SchemaRegistry(manifest)
        schema1 = registry.get_input_schema("bronze.feature_releases")
        schema2 = registry.get_input_schema("bronze.feature_releases")
        assert schema1 is schema2  # Same object from cache

    def test_registry_compare_schemas(self, manifest):
        registry = SchemaRegistry(manifest)
        diff = registry.compare_schemas("bronze.feature_releases")
        assert "ingestion_timestamp" in diff["added"]
        assert "id" in diff["removed"]  # renamed to feature_id
        assert "name" in diff["removed"]  # renamed to feature_name
        assert "feature_id" in diff["added"]
        assert "feature_name" in diff["added"]

    def test_registry_list_tables(self, manifest):
        registry = SchemaRegistry(manifest)
        tables = registry.list_tables()
        assert "bronze.feature_releases" in tables
        assert "silver.feature_states" in tables
        assert "gold.feature_conversion_impact" in tables

    def test_registry_no_input_schema_raises(self, manifest):
        registry = SchemaRegistry(manifest)
        # Silver tables don't have input schemas defined
        with pytest.raises(ValueError, match="No input schema"):
            registry.get_input_schema("silver.feature_states")

    def test_schemas_match_existing_definitions(self, manifest):
        """Verify that declarative schemas match the existing hand-written ones."""
        registry = SchemaRegistry(manifest)

        # Compare with FEATURE_RELEASES_SCHEMA
        from spark.jobs.bronze.feature_releases.schema import FEATURE_RELEASES_SCHEMA

        declarative_input = registry.get_input_schema("bronze.feature_releases")
        for expected_field in FEATURE_RELEASES_SCHEMA.fields:
            actual_field = declarative_input[expected_field.name]
            assert actual_field.dataType == expected_field.dataType, (
                f"Type mismatch for {expected_field.name}: "
                f"expected {expected_field.dataType}, got {actual_field.dataType}"
            )


# ===========================================================================
# Test Quality Engine
# ===========================================================================


class TestQualityEngine:
    def test_not_null_passes(self, spark, manifest):
        df = spark.createDataFrame(
            [(1, "alice"), (2, "bob")],
            StructType(
                [
                    StructField("id", IntegerType()),
                    StructField("name", StringType()),
                ]
            ),
        )

        from spark.jobs.declarative.manifest import QualityRule
        from spark.jobs.declarative.quality_engine import _check_not_null

        rule = QualityRule(rule="not_null", columns=["id", "name"])
        result = _check_not_null(df, rule)
        assert result.passed is True

    def test_not_null_fails(self, spark):
        from spark.jobs.declarative.manifest import QualityRule
        from spark.jobs.declarative.quality_engine import _check_not_null

        df = spark.createDataFrame(
            [(1, "alice"), (2, None)],
            StructType(
                [
                    StructField("id", IntegerType()),
                    StructField("name", StringType()),
                ]
            ),
        )
        rule = QualityRule(rule="not_null", columns=["name"])
        result = _check_not_null(df, rule)
        assert result.passed is False
        assert "name" in result.message

    def test_unique_passes(self, spark):
        from spark.jobs.declarative.manifest import QualityRule
        from spark.jobs.declarative.quality_engine import _check_unique

        df = spark.createDataFrame(
            [(1, "alice"), (2, "bob")],
            StructType(
                [
                    StructField("id", IntegerType()),
                    StructField("name", StringType()),
                ]
            ),
        )
        rule = QualityRule(rule="unique", columns=["id"])
        result = _check_unique(df, rule)
        assert result.passed is True

    def test_unique_fails(self, spark):
        from spark.jobs.declarative.manifest import QualityRule
        from spark.jobs.declarative.quality_engine import _check_unique

        df = spark.createDataFrame(
            [(1, "alice"), (1, "bob")],
            StructType(
                [
                    StructField("id", IntegerType()),
                    StructField("name", StringType()),
                ]
            ),
        )
        rule = QualityRule(rule="unique", columns=["id"])
        result = _check_unique(df, rule)
        assert result.passed is False

    def test_min_rows_passes(self, spark):
        from spark.jobs.declarative.manifest import QualityRule
        from spark.jobs.declarative.quality_engine import _check_min_rows

        df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
        rule = QualityRule(rule="min_rows", value=2)
        result = _check_min_rows(df, rule)
        assert result.passed is True

    def test_min_rows_fails(self, spark):
        from spark.jobs.declarative.manifest import QualityRule
        from spark.jobs.declarative.quality_engine import _check_min_rows

        df = spark.createDataFrame([(1,)], ["id"])
        rule = QualityRule(rule="min_rows", value=5)
        result = _check_min_rows(df, rule)
        assert result.passed is False

    def test_range_passes(self, spark):
        from spark.jobs.declarative.manifest import QualityRule
        from spark.jobs.declarative.quality_engine import _check_range

        df = spark.createDataFrame([(50,), (99,), (10,)], ["mrr"])
        rule = QualityRule(rule="range", column="mrr", min=0, max=100)
        result = _check_range(df, rule)
        assert result.passed is True

    def test_range_fails(self, spark):
        from spark.jobs.declarative.manifest import QualityRule
        from spark.jobs.declarative.quality_engine import _check_range

        df = spark.createDataFrame([(50,), (-1,), (200,)], ["mrr"])
        rule = QualityRule(rule="range", column="mrr", min=0, max=100)
        result = _check_range(df, rule)
        assert result.passed is False

    def test_accepted_values_passes(self, spark):
        from spark.jobs.declarative.manifest import QualityRule
        from spark.jobs.declarative.quality_engine import _check_accepted_values

        df = spark.createDataFrame([("Pro",), ("Enterprise",)], ["plan"])
        rule = QualityRule(
            rule="accepted_values",
            column="plan",
            values=["Pro", "Enterprise", "Business"],
        )
        result = _check_accepted_values(df, rule)
        assert result.passed is True

    def test_accepted_values_fails(self, spark):
        from spark.jobs.declarative.manifest import QualityRule
        from spark.jobs.declarative.quality_engine import _check_accepted_values

        df = spark.createDataFrame([("Pro",), ("Invalid",)], ["plan"])
        rule = QualityRule(
            rule="accepted_values",
            column="plan",
            values=["Pro", "Enterprise"],
        )
        result = _check_accepted_values(df, rule)
        assert result.passed is False
        assert "Invalid" in result.message

    def test_custom_sql_passes(self, spark):
        from spark.jobs.declarative.manifest import QualityRule
        from spark.jobs.declarative.quality_engine import _check_custom_sql

        df = spark.createDataFrame(
            [(1, True), (2, True)],
            StructType(
                [
                    StructField("feature_id", IntegerType()),
                    StructField("is_current", BooleanType()),
                ]
            ),
        )
        rule = QualityRule(
            rule="custom_sql",
            sql="SELECT feature_id FROM {table} WHERE is_current = false",
            description="No inactive features expected",
        )
        result = _check_custom_sql(spark, df, rule, "test_table")
        assert result.passed is True

    def test_custom_sql_fails(self, spark):
        from spark.jobs.declarative.manifest import QualityRule
        from spark.jobs.declarative.quality_engine import _check_custom_sql

        df = spark.createDataFrame(
            [(1, True), (1, True)],
            StructType(
                [
                    StructField("feature_id", IntegerType()),
                    StructField("is_current", BooleanType()),
                ]
            ),
        )
        rule = QualityRule(
            rule="custom_sql",
            sql=(
                "SELECT feature_id, COUNT(*) as cnt FROM {table} "
                "WHERE is_current = true GROUP BY feature_id HAVING cnt > 1"
            ),
            description="Duplicate current records",
        )
        result = _check_custom_sql(spark, df, rule, "test_table")
        assert result.passed is False

    def test_validate_table_all_pass(self, spark, mini_manifest):
        from spark.jobs.declarative.quality_engine import QualityEngine

        engine = QualityEngine(mini_manifest)
        df = spark.createDataFrame(
            [(1, "alice", "alice@test.com")],
            StructType(
                [
                    StructField("user_id", IntegerType()),
                    StructField("name", StringType()),
                    StructField("email", StringType()),
                ]
            ),
        )
        report = engine.validate_table(spark, "bronze.users", df, phase="output")
        assert report.passed is True
        assert report.row_count == 1

    def test_validate_table_reports_failures(self, spark, mini_manifest):
        from spark.jobs.declarative.quality_engine import QualityEngine

        engine = QualityEngine(mini_manifest)
        # Empty DF will fail min_rows
        df = spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("user_id", IntegerType()),
                    StructField("name", StringType()),
                    StructField("email", StringType()),
                ]
            ),
        )
        report = engine.validate_table(spark, "bronze.users", df, phase="output")
        assert report.passed is False
        failed = [r for r in report.results if not r.passed]
        assert any(r.rule_name == "min_rows" for r in failed)


# ===========================================================================
# Test Bronze Engine
# ===========================================================================


class TestBronzeEngine:
    def test_ingest_simple_table(self, spark, tmp_path):
        """Test config-driven ingestion of a simple JSON table."""
        from spark.jobs.declarative.bronze_engine import BronzeEngine

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        bronze_dir = tmp_path / "bronze"

        # Write test data
        data = [{"id": 1, "name": "alice", "email": "alice@test.com"}]
        with open(raw_dir / "users.jsonl", "w") as f:
            for record in data:
                f.write(json.dumps(record) + "\n")

        # Create a mini manifest for this test
        config = {
            "version": "1.0",
            "name": "test",
            "description": "test",
            "paths": {
                "raw": str(raw_dir),
                "bronze": str(bronze_dir),
            },
            "bronze": {
                "users": {
                    "description": "Test users",
                    "source": {"file": "users.jsonl", "format": "json"},
                    "schema": {
                        "input": [
                            {"name": "id", "type": "integer"},
                            {"name": "name", "type": "string"},
                            {"name": "email", "type": "string"},
                        ],
                        "output": [
                            {"name": "user_id", "type": "integer"},
                            {"name": "name", "type": "string"},
                            {"name": "email", "type": "string"},
                            {"name": "ingestion_timestamp", "type": "timestamp"},
                        ],
                    },
                    "transformations": [
                        {"type": "rename", "columns": {"id": "user_id"}},
                        {
                            "type": "add_column",
                            "name": "ingestion_timestamp",
                            "function": "current_timestamp",
                        },
                    ],
                    "write": {"mode": "append"},
                },
            },
        }
        manifest_path = str(tmp_path / "pipeline.yaml")
        with open(manifest_path, "w") as f:
            yaml.dump(config, f)

        engine = BronzeEngine.from_manifest(manifest_path)
        count = engine.ingest_table(spark, "bronze.users")

        assert count == 1

        # Verify Delta table
        df = spark.read.format("delta").load(str(bronze_dir / "users"))
        assert df.count() == 1
        assert "user_id" in df.columns
        assert "ingestion_timestamp" in df.columns
        row = df.collect()[0]
        assert row.user_id == 1
        assert row.name == "alice"

    def test_ingest_with_partitioning(self, spark, tmp_path):
        """Test ingestion with partition_by."""
        from spark.jobs.declarative.bronze_engine import BronzeEngine

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        bronze_dir = tmp_path / "bronze"

        data = [
            {"user_id": 1, "signup_date": "2024-01-15", "name": "alice"},
            {"user_id": 2, "signup_date": "2024-01-16", "name": "bob"},
        ]
        with open(raw_dir / "signups.jsonl", "w") as f:
            for record in data:
                f.write(json.dumps(record) + "\n")

        config = {
            "version": "1.0",
            "name": "test",
            "description": "test",
            "paths": {"raw": str(raw_dir), "bronze": str(bronze_dir)},
            "bronze": {
                "signups": {
                    "description": "Signups",
                    "source": {"file": "signups.jsonl", "format": "json"},
                    "schema": {
                        "input": [
                            {"name": "user_id", "type": "integer"},
                            {"name": "signup_date", "type": "string"},
                            {"name": "name", "type": "string"},
                        ],
                        "output": [
                            {"name": "user_id", "type": "integer"},
                            {"name": "signup_date", "type": "string"},
                            {"name": "name", "type": "string"},
                            {"name": "ingestion_timestamp", "type": "timestamp"},
                        ],
                    },
                    "transformations": [
                        {
                            "type": "add_column",
                            "name": "ingestion_timestamp",
                            "function": "current_timestamp",
                        },
                    ],
                    "write": {"mode": "append", "partition_by": ["signup_date"]},
                },
            },
        }
        manifest_path = str(tmp_path / "pipeline.yaml")
        with open(manifest_path, "w") as f:
            yaml.dump(config, f)

        engine = BronzeEngine.from_manifest(manifest_path)
        count = engine.ingest_table(spark, "bronze.signups")
        assert count == 2

        # Verify partitioning
        df = spark.read.format("delta").load(str(bronze_dir / "signups"))
        assert df.count() == 2

        partition_dirs = [
            d for d in os.listdir(str(bronze_dir / "signups")) if d.startswith("signup_date=")
        ]
        assert len(partition_dirs) == 2

    def test_ingest_with_timestamp_conversion(self, spark, tmp_path):
        """Test ingestion with convert_timestamp transformation."""
        from spark.jobs.declarative.bronze_engine import BronzeEngine

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        bronze_dir = tmp_path / "bronze"

        data = [
            {"timestamp": "2024-01-15 10:30:00", "user_id": 1, "action": "click"},
        ]
        with open(raw_dir / "events.jsonl", "w") as f:
            for record in data:
                f.write(json.dumps(record) + "\n")

        config = {
            "version": "1.0",
            "name": "test",
            "description": "test",
            "paths": {"raw": str(raw_dir), "bronze": str(bronze_dir)},
            "bronze": {
                "events": {
                    "description": "Events",
                    "source": {"file": "events.jsonl", "format": "json"},
                    "schema": {
                        "input": [
                            {"name": "timestamp", "type": "string"},
                            {"name": "user_id", "type": "integer"},
                            {"name": "action", "type": "string"},
                        ],
                        "output": [
                            {"name": "user_id", "type": "integer"},
                            {"name": "action", "type": "string"},
                            {"name": "event_timestamp", "type": "timestamp"},
                        ],
                    },
                    "transformations": [
                        {
                            "type": "convert_timestamp",
                            "source": "timestamp",
                            "target": "event_timestamp",
                            "format": "yyyy-MM-dd HH:mm:ss",
                            "drop_source": True,
                        },
                        {"type": "select", "columns": ["user_id", "action", "event_timestamp"]},
                    ],
                    "write": {"mode": "append"},
                },
            },
        }
        manifest_path = str(tmp_path / "pipeline.yaml")
        with open(manifest_path, "w") as f:
            yaml.dump(config, f)

        engine = BronzeEngine.from_manifest(manifest_path)
        count = engine.ingest_table(spark, "bronze.events")
        assert count == 1

        df = spark.read.format("delta").load(str(bronze_dir / "events"))
        assert "event_timestamp" in df.columns
        assert "timestamp" not in df.columns  # dropped
        row = df.collect()[0]
        assert row.event_timestamp is not None

    def test_ingest_all(self, spark, tmp_path):
        """Test ingesting all bronze tables."""
        from spark.jobs.declarative.bronze_engine import BronzeEngine

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        bronze_dir = tmp_path / "bronze"

        # Write test data for two tables
        with open(raw_dir / "users.jsonl", "w") as f:
            f.write(json.dumps({"id": 1, "name": "alice"}) + "\n")
        with open(raw_dir / "events.jsonl", "w") as f:
            f.write(json.dumps({"event_id": 1, "user_id": 1}) + "\n")

        config = {
            "version": "1.0",
            "name": "test",
            "description": "test",
            "paths": {"raw": str(raw_dir), "bronze": str(bronze_dir)},
            "bronze": {
                "users": {
                    "description": "Users",
                    "source": {"file": "users.jsonl", "format": "json"},
                    "schema": {
                        "input": [
                            {"name": "id", "type": "integer"},
                            {"name": "name", "type": "string"},
                        ],
                        "output": [
                            {"name": "id", "type": "integer"},
                            {"name": "name", "type": "string"},
                        ],
                    },
                    "transformations": [],
                    "write": {"mode": "append"},
                },
                "events": {
                    "description": "Events",
                    "source": {"file": "events.jsonl", "format": "json"},
                    "schema": {
                        "input": [
                            {"name": "event_id", "type": "integer"},
                            {"name": "user_id", "type": "integer"},
                        ],
                        "output": [
                            {"name": "event_id", "type": "integer"},
                            {"name": "user_id", "type": "integer"},
                        ],
                    },
                    "transformations": [],
                    "write": {"mode": "append"},
                },
            },
        }
        manifest_path = str(tmp_path / "pipeline.yaml")
        with open(manifest_path, "w") as f:
            yaml.dump(config, f)

        engine = BronzeEngine.from_manifest(manifest_path)
        stats = engine.ingest_all(spark)

        assert stats["users"] == 1
        assert stats["events"] == 1

    def test_ingest_non_bronze_raises(self, spark, mini_manifest):
        from spark.jobs.declarative.bronze_engine import BronzeEngine

        engine = BronzeEngine(mini_manifest)
        with pytest.raises(ValueError, match="not a bronze table"):
            engine.ingest_table(spark, "silver.user_stats")


# ===========================================================================
# Test Lineage
# ===========================================================================


class TestLineage:
    def test_ascii_graph(self, manifest):
        from spark.jobs.declarative.lineage import PipelineLineage

        lineage = PipelineLineage(manifest)
        graph = lineage.ascii_graph()
        assert "BRONZE" in graph
        assert "SILVER" in graph
        assert "GOLD" in graph
        assert "feature_releases" in graph

    def test_dot_graph(self, manifest):
        from spark.jobs.declarative.lineage import PipelineLineage

        lineage = PipelineLineage(manifest)
        dot = lineage.dot_graph()
        assert "digraph pipeline" in dot
        assert "cluster_bronze" in dot
        assert "cluster_silver" in dot
        assert "cluster_gold" in dot
        assert "->" in dot  # has edges

    def test_impact_analysis(self, manifest):
        from spark.jobs.declarative.lineage import PipelineLineage

        lineage = PipelineLineage(manifest)
        report = lineage.impact_analysis("bronze.feature_releases")
        assert "Impact Analysis" in report
        assert "feature_states" in report

    def test_impact_analysis_leaf_node(self, manifest):
        from spark.jobs.declarative.lineage import PipelineLineage

        lineage = PipelineLineage(manifest)
        report = lineage.impact_analysis("gold.feature_conversion_impact")
        assert "isolated" in report.lower()

    def test_upstream_trace(self, manifest):
        from spark.jobs.declarative.lineage import PipelineLineage

        lineage = PipelineLineage(manifest)
        trace = lineage.upstream_trace("gold.feature_conversion_impact")
        assert "Upstream Trace" in trace
        assert "BRONZE" in trace
        assert "SILVER" in trace

    def test_upstream_trace_source_table(self, manifest):
        from spark.jobs.declarative.lineage import PipelineLineage

        lineage = PipelineLineage(manifest)
        trace = lineage.upstream_trace("bronze.feature_releases")
        assert "source table" in trace.lower()

    def test_pipeline_health(self, manifest):
        from spark.jobs.declarative.lineage import PipelineLineage

        lineage = PipelineLineage(manifest)
        health = lineage.pipeline_health()
        assert "Pipeline Health" in health
        assert "quality:" in health
        assert "freshness:" in health


# ===========================================================================
# Test Metrics Engine
# ===========================================================================


class TestMetricsEngine:
    def test_evaluate_metrics_basic(self, spark):
        """Test metric evaluation with a mock gold table."""
        from spark.jobs.declarative.metrics_engine import MetricsEngine

        # Create a mock gold DataFrame
        data = [
            ("real_time_collab", "used_feature", 0.80, 120.0),
            ("real_time_collab", "available_not_used", 0.20, 80.0),
            ("ai_insights", "used_feature", 0.60, 100.0),
            ("ai_insights", "available_not_used", 0.15, 70.0),
        ]
        df = spark.createDataFrame(
            data,
            ["feature_name", "cohort", "conversion_rate", "avg_mrr"],
        )

        # Use mini manifest with metric definitions
        config = {
            "version": "1.0",
            "name": "test",
            "description": "test",
            "paths": {"gold": "/tmp/test_gold"},
            "gold": {
                "metrics_test": {
                    "description": "Test",
                    "transformation": "custom",
                    "module": "test",
                    "function": "test",
                    "schema": {
                        "output": [
                            {"name": "feature_name", "type": "string"},
                            {"name": "cohort", "type": "string"},
                            {"name": "conversion_rate", "type": "double"},
                            {"name": "avg_mrr", "type": "double"},
                        ],
                    },
                    "metrics": [
                        {
                            "name": "adoption_lift",
                            "description": "Lift from feature usage",
                            "formula": (
                                "conversion_rate[used_feature]"
                                " / conversion_rate[available_not_used]"
                            ),
                            "threshold": {"warning": 1.5, "critical": 1.0, "direction": "above"},
                        },
                        {
                            "name": "revenue_lift",
                            "description": "Revenue lift",
                            "formula": "avg_mrr[used_feature] / avg_mrr[available_not_used]",
                        },
                    ],
                },
            },
        }
        import tempfile

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config, f)
            manifest_path = f.name

        engine = MetricsEngine.from_manifest(manifest_path)
        report = engine.evaluate_metrics(spark, "gold.metrics_test", df=df)

        assert len(report.results) > 0

        # Check real_time_collab adoption_lift: 0.80 / 0.20 = 4.0
        rtc_lift = [
            r
            for r in report.results
            if r.name == "adoption_lift" and r.feature_name == "real_time_collab"
        ]
        assert len(rtc_lift) == 1
        assert abs(rtc_lift[0].value - 4.0) < 0.001
        assert rtc_lift[0].status == "ok"  # 4.0 > 1.5 warning threshold

        # Check ai_insights adoption_lift: 0.60 / 0.15 = 4.0
        ai_lift = [
            r
            for r in report.results
            if r.name == "adoption_lift" and r.feature_name == "ai_insights"
        ]
        assert len(ai_lift) == 1
        assert abs(ai_lift[0].value - 4.0) < 0.001

        os.unlink(manifest_path)

    def test_metric_threshold_warning(self, spark):
        """Test that metrics below warning threshold get warning status."""
        from spark.jobs.declarative.metrics_engine import _check_threshold

        status, msg = _check_threshold(1.3, {"warning": 1.5, "critical": 1.0, "direction": "above"})
        assert status == "warning"

    def test_metric_threshold_critical(self, spark):
        """Test that metrics below critical threshold get critical status."""
        from spark.jobs.declarative.metrics_engine import _check_threshold

        status, msg = _check_threshold(0.8, {"warning": 1.5, "critical": 1.0, "direction": "above"})
        assert status == "critical"

    def test_metric_threshold_ok(self, spark):
        from spark.jobs.declarative.metrics_engine import _check_threshold

        status, msg = _check_threshold(2.0, {"warning": 1.5, "critical": 1.0, "direction": "above"})
        assert status == "ok"

    def test_metric_none_value(self, spark):
        from spark.jobs.declarative.metrics_engine import _check_threshold

        status, msg = _check_threshold(
            None, {"warning": 1.5, "critical": 1.0, "direction": "above"}
        )
        assert status == "error"

    def test_metrics_report_summary(self, spark):
        from spark.jobs.declarative.metrics_engine import MetricResult, MetricsReport

        report = MetricsReport(
            table_name="gold.test",
            results=[
                MetricResult(
                    name="lift",
                    description="Test",
                    feature_name="feature_a",
                    value=3.5,
                    status="ok",
                    message="OK",
                ),
            ],
        )
        summary = report.summary()
        assert "gold.test" in summary
        assert "feature_a" in summary
        assert "[OK]" in summary
