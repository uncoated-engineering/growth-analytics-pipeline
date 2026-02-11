"""
Data quality validation utilities for the growth analytics pipeline.

Provides schema validation for raw files, Delta tables, and DataFrames.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


class SchemaValidationError(Exception):
    """Raised when a DataFrame schema does not match the expected schema."""


class DataValidationError(Exception):
    """Raised when data fails quality checks (e.g. empty table, null keys)."""


def validate_schema(df: DataFrame, expected_schema: StructType, table_name: str) -> None:
    """
    Validate that a DataFrame's schema matches the expected StructType.

    Checks field names and data types. Nullable mismatches are ignored because
    Spark infers nullability from data.

    Args:
        df: The DataFrame to validate.
        expected_schema: The expected StructType schema.
        table_name: Human-readable table name for error messages.

    Raises:
        SchemaValidationError: If field names or types don't match.
    """
    actual_fields = {f.name: f.dataType for f in df.schema.fields}
    expected_fields = {f.name: f.dataType for f in expected_schema.fields}

    missing = set(expected_fields) - set(actual_fields)
    if missing:
        raise SchemaValidationError(
            f"[{table_name}] Missing columns: {sorted(missing)}. "
            f"Expected: {sorted(expected_fields)}. "
            f"Got: {sorted(actual_fields)}."
        )

    for name, expected_type in expected_fields.items():
        actual_type = actual_fields[name]
        if actual_type != expected_type:
            raise SchemaValidationError(
                f"[{table_name}] Column '{name}' type mismatch: "
                f"expected {expected_type}, got {actual_type}."
            )


def validate_delta_table(
    spark: SparkSession,
    path: str,
    expected_schema: StructType,
    table_name: str,
    min_rows: int = 1,
) -> int:
    """
    Validate a Delta table exists, has the expected schema, and is non-empty.

    Args:
        spark: SparkSession.
        path: Path to the Delta table.
        expected_schema: Expected StructType schema.
        table_name: Human-readable name for error messages.
        min_rows: Minimum acceptable row count (default 1).

    Returns:
        The row count of the table.

    Raises:
        DataValidationError: If the table doesn't exist or has fewer rows than min_rows.
        SchemaValidationError: If the schema doesn't match.
    """
    try:
        df = spark.read.format("delta").load(path)
    except Exception as e:
        raise DataValidationError(f"[{table_name}] Delta table not found at '{path}': {e}") from e

    validate_schema(df, expected_schema, table_name)

    row_count = df.count()
    if row_count < min_rows:
        raise DataValidationError(
            f"[{table_name}] Expected at least {min_rows} rows, got {row_count}."
        )

    return row_count


def validate_raw_file(
    spark: SparkSession,
    path: str,
    expected_schema: StructType,
    table_name: str,
    file_format: str = "json",
    read_options: dict | None = None,
) -> int:
    """
    Validate a raw file can be read with the expected schema and is non-empty.

    Args:
        spark: SparkSession.
        path: Path to the raw file.
        expected_schema: Expected StructType schema for the raw file.
        table_name: Human-readable name for error messages.
        file_format: File format (default "json").
        read_options: Additional read options (e.g. {"multiLine": "true"}).

    Returns:
        The row count of the file.

    Raises:
        DataValidationError: If the file can't be read or is empty.
        SchemaValidationError: If the schema doesn't match.
    """
    try:
        reader = spark.read.schema(expected_schema)
        if read_options:
            for k, v in read_options.items():
                reader = reader.option(k, v)
        df = reader.format(file_format).load(path)
    except Exception as e:
        raise DataValidationError(f"[{table_name}] Could not read raw file at '{path}': {e}") from e

    validate_schema(df, expected_schema, table_name)

    row_count = df.count()
    if row_count < 1:
        raise DataValidationError(f"[{table_name}] Raw file at '{path}' is empty (0 rows).")

    return row_count
