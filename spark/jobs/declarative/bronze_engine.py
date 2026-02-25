"""
Config-driven bronze ingestion engine.

Replaces the repetitive per-table extract.py files with a single engine
that reads transformation specs from pipeline.yaml. Instead of writing:

    df = spark.read.schema(SCHEMA).option("multiLine", True).json(input_path)
    df = df.withColumnRenamed("id", "feature_id")
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    df.write.format("delta").mode("append").save(output_path)

You declare in YAML:

    transformations:
      - type: rename
        columns: {id: feature_id, name: feature_name}
      - type: add_column
        name: ingestion_timestamp
        function: current_timestamp

And the engine handles execution.

Usage:
    from spark.jobs.declarative.bronze_engine import BronzeEngine

    engine = BronzeEngine.from_manifest()
    stats = engine.ingest_all(spark)
    stats = engine.ingest_table(spark, "bronze.feature_releases")
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    monotonically_increasing_id,
    to_timestamp,
)

from spark.jobs.declarative.manifest import (
    PipelineManifest,
    Transformation,
    load_manifest,
)
from spark.jobs.declarative.schema_registry import SchemaRegistry


def _apply_rename(df: DataFrame, transform: Transformation) -> DataFrame:
    """Apply column renames: {old_name: new_name}."""
    columns = transform.params.get("columns", {})
    for old_name, new_name in columns.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df


def _apply_add_column(df: DataFrame, transform: Transformation) -> DataFrame:
    """Add a computed column using a named function."""
    name = transform.params["name"]
    func = transform.params["function"]

    func_map = {
        "current_timestamp": current_timestamp(),
        "monotonically_increasing_id": monotonically_increasing_id(),
    }

    if func not in func_map:
        raise ValueError(
            f"Unknown function '{func}' for add_column. " f"Supported: {sorted(func_map.keys())}"
        )

    return df.withColumn(name, func_map[func])


def _apply_convert_timestamp(df: DataFrame, transform: Transformation) -> DataFrame:
    """Convert a string column to a timestamp with a format pattern."""
    source = transform.params["source"]
    target = transform.params["target"]
    fmt = transform.params.get("format", "yyyy-MM-dd HH:mm:ss")
    drop_source = transform.params.get("drop_source", False)

    df = df.withColumn(target, to_timestamp(col(source), fmt))
    if drop_source and source != target:
        df = df.drop(source)
    return df


def _apply_cast(df: DataFrame, transform: Transformation) -> DataFrame:
    """Cast a column to a target type, optionally creating a new column."""
    source = transform.params["source"]
    target = transform.params.get("target", source)
    to_type = transform.params["to"]

    df = df.withColumn(target, col(source).cast(to_type))
    return df


def _apply_select(df: DataFrame, transform: Transformation) -> DataFrame:
    """Select and reorder specific columns."""
    columns = transform.params["columns"]
    return df.select(*columns)


# Transformation dispatcher
_TRANSFORM_HANDLERS = {
    "rename": _apply_rename,
    "add_column": _apply_add_column,
    "convert_timestamp": _apply_convert_timestamp,
    "cast": _apply_cast,
    "select": _apply_select,
}


def apply_transformations(df: DataFrame, transformations: list[Transformation]) -> DataFrame:
    """Apply a sequence of declarative transformations to a DataFrame."""
    for transform in transformations:
        handler = _TRANSFORM_HANDLERS.get(transform.type)
        if handler is None:
            raise ValueError(
                f"Unknown transformation type '{transform.type}'. "
                f"Supported: {sorted(_TRANSFORM_HANDLERS.keys())}"
            )
        df = handler(df, transform)
    return df


class BronzeEngine:
    """Config-driven bronze layer ingestion engine."""

    def __init__(self, manifest: PipelineManifest) -> None:
        self._manifest = manifest
        self._schema_registry = SchemaRegistry(manifest)

    @classmethod
    def from_manifest(cls, path: str | None = None) -> BronzeEngine:
        return cls(load_manifest(path))

    def ingest_table(
        self,
        spark: SparkSession,
        qualified_name: str,
        raw_data_path: str | None = None,
        bronze_path: str | None = None,
    ) -> int:
        """Ingest a single bronze table using its declarative config.

        Args:
            spark: SparkSession.
            qualified_name: e.g., "bronze.feature_releases"
            raw_data_path: Override base raw path. Defaults to manifest paths.raw.
            bronze_path: Override base bronze path. Defaults to manifest paths.bronze.

        Returns:
            Number of rows ingested.
        """
        table = self._manifest.get_table(qualified_name)
        if table.layer != "bronze":
            raise ValueError(f"'{qualified_name}' is not a bronze table")

        raw_base = raw_data_path or self._manifest.paths.get("raw", "data/raw")
        bronze_base = bronze_path or self._manifest.paths.get("bronze", "data/bronze")
        input_path = f"{raw_base}/{table.source.file}"
        output_path = f"{bronze_base}/{table.name}"

        print(f"[declarative] Ingesting {table.name} from {input_path} to {output_path}")

        # Read source file with declared schema and options
        input_schema = self._schema_registry.get_input_schema(qualified_name)
        reader = spark.read.schema(input_schema)
        for key, value in (table.source.options or {}).items():
            reader = reader.option(key, value)
        df = reader.format(table.source.format).load(input_path)

        # Apply declared transformations
        df = apply_transformations(df, table.transformations)

        # Write to Delta
        writer = df.write.format("delta").mode(table.write.mode)
        if table.write.partition_by:
            writer = writer.partitionBy(*table.write.partition_by)
        writer.save(output_path)

        row_count = df.count()
        print(f"[declarative] Ingested {row_count} rows into {table.name}")
        return row_count

    def ingest_all(
        self,
        spark: SparkSession,
        raw_data_path: str | None = None,
        bronze_path: str | None = None,
    ) -> dict[str, int]:
        """Ingest all bronze tables declared in the manifest.

        Returns:
            Dict of table_name -> row_count.
        """
        stats = {}
        for table in self._manifest.get_layer("bronze"):
            count = self.ingest_table(spark, table.qualified_name, raw_data_path, bronze_path)
            stats[table.name] = count
        return stats
