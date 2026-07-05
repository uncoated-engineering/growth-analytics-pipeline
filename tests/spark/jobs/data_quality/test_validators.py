"""
Tests for the data quality validation framework.

Tests validate_schema, validate_delta_table, and validate_raw_file functions
along with SchemaValidationError and DataValidationError exceptions.
"""

import json

import pytest
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from spark.jobs.data_quality.validators import (
    DataValidationError,
    SchemaValidationError,
    validate_delta_table,
    validate_raw_file,
    validate_schema,
)


@pytest.fixture()
def sample_schema():
    return StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ]
    )


@pytest.fixture()
def wrong_type_schema():
    return StructType(
        [
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
        ]
    )


@pytest.fixture()
def extra_column_schema():
    return StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
        ]
    )


class TestValidateSchema:
    """Tests for the validate_schema function."""

    def test_valid_schema_passes(self, spark, sample_schema):
        df = spark.createDataFrame([(1, "alice"), (2, "bob")], sample_schema)
        validate_schema(df, sample_schema, "test_table")

    def test_missing_column_raises(self, spark, sample_schema, extra_column_schema):
        df = spark.createDataFrame([(1, "alice")], sample_schema)
        with pytest.raises(SchemaValidationError, match="Missing columns"):
            validate_schema(df, extra_column_schema, "test_table")

    def test_wrong_type_raises(self, spark, sample_schema, wrong_type_schema):
        df = spark.createDataFrame([(1, "alice")], sample_schema)
        with pytest.raises(SchemaValidationError, match="type mismatch"):
            validate_schema(df, wrong_type_schema, "test_table")

    def test_extra_columns_in_df_accepted(self, spark, sample_schema):
        """Extra columns in the DataFrame are OK (schema is a subset check)."""
        extended_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("extra", StringType(), True),
            ]
        )
        df = spark.createDataFrame([(1, "alice", "x")], extended_schema)
        validate_schema(df, sample_schema, "test_table")


class TestValidateDeltaTable:
    """Tests for the validate_delta_table function."""

    def test_valid_delta_table(self, spark, tmp_path, sample_schema):
        path = str(tmp_path / "valid_delta")
        df = spark.createDataFrame([(1, "alice"), (2, "bob")], sample_schema)
        df.write.format("delta").save(path)

        count = validate_delta_table(spark, path, sample_schema, "test_delta")
        assert count == 2

    def test_nonexistent_path_raises(self, spark, tmp_path, sample_schema):
        path = str(tmp_path / "nonexistent")
        with pytest.raises(DataValidationError, match="not found"):
            validate_delta_table(spark, path, sample_schema, "test_delta")

    def test_empty_table_raises(self, spark, tmp_path, sample_schema):
        path = str(tmp_path / "empty_delta")
        df = spark.createDataFrame([], sample_schema)
        df.write.format("delta").save(path)

        with pytest.raises(DataValidationError, match="at least 1 rows"):
            validate_delta_table(spark, path, sample_schema, "test_delta")

    def test_wrong_schema_raises(self, spark, tmp_path, sample_schema, wrong_type_schema):
        path = str(tmp_path / "wrong_schema_delta")
        df = spark.createDataFrame([(1, "alice")], sample_schema)
        df.write.format("delta").save(path)

        with pytest.raises(SchemaValidationError, match="type mismatch"):
            validate_delta_table(spark, path, wrong_type_schema, "test_delta")

    def test_min_rows_parameter(self, spark, tmp_path, sample_schema):
        path = str(tmp_path / "min_rows_delta")
        df = spark.createDataFrame([(1, "alice")], sample_schema)
        df.write.format("delta").save(path)

        with pytest.raises(DataValidationError, match="at least 5 rows"):
            validate_delta_table(spark, path, sample_schema, "test_delta", min_rows=5)


class TestValidateRawFile:
    """Tests for the validate_raw_file function."""

    def test_valid_json_file(self, spark, tmp_path, sample_schema):
        path = str(tmp_path / "data.json")
        data = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
        with open(path, "w") as f:
            json.dump(data, f)

        count = validate_raw_file(
            spark, path, sample_schema, "test_raw", read_options={"multiLine": "true"}
        )
        assert count == 2

    def test_valid_jsonl_file(self, spark, tmp_path, sample_schema):
        path = str(tmp_path / "data.jsonl")
        with open(path, "w") as f:
            f.write('{"id": 1, "name": "alice"}\n')
            f.write('{"id": 2, "name": "bob"}\n')

        count = validate_raw_file(spark, path, sample_schema, "test_raw")
        assert count == 2

    def test_nonexistent_file_raises(self, spark, tmp_path, sample_schema):
        path = str(tmp_path / "nonexistent.json")
        # Spark JSON reader doesn't raise on missing path - it returns empty DF
        # which triggers the "empty" check
        with pytest.raises(DataValidationError):
            validate_raw_file(spark, path, sample_schema, "test_raw")

    def test_empty_file_raises(self, spark, tmp_path, sample_schema):
        path = str(tmp_path / "empty.jsonl")
        with open(path, "w"):
            pass  # empty file

        with pytest.raises(DataValidationError, match="empty"):
            validate_raw_file(spark, path, sample_schema, "test_raw")
