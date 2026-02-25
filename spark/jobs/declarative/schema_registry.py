"""
Declarative schema registry.

Converts YAML schema definitions to PySpark StructType objects. Eliminates
the need for separate schema.py files per table - schemas are defined once
in pipeline.yaml and generated at runtime.

Usage:
    from spark.jobs.declarative.schema_registry import SchemaRegistry

    registry = SchemaRegistry.from_manifest()
    input_schema = registry.get_input_schema("bronze.feature_releases")
    output_schema = registry.get_output_schema("bronze.feature_releases")
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark.jobs.declarative.manifest import PipelineManifest, SchemaField, load_manifest

# Mapping from YAML type names to PySpark types
_TYPE_MAP: dict[str, DataType] = {
    "string": StringType(),
    "integer": IntegerType(),
    "int": IntegerType(),
    "long": LongType(),
    "bigint": LongType(),
    "double": DoubleType(),
    "float": FloatType(),
    "boolean": BooleanType(),
    "bool": BooleanType(),
    "date": DateType(),
    "timestamp": TimestampType(),
}


def _resolve_type(type_name: str) -> DataType:
    """Resolve a YAML type name to a PySpark DataType."""
    normalized = type_name.strip().lower()
    if normalized not in _TYPE_MAP:
        raise ValueError(f"Unknown type '{type_name}'. Supported types: {sorted(_TYPE_MAP.keys())}")
    return _TYPE_MAP[normalized]


def fields_to_struct(fields: list[SchemaField]) -> StructType:
    """Convert a list of SchemaField definitions to a PySpark StructType."""
    return StructType([StructField(f.name, _resolve_type(f.type), f.nullable) for f in fields])


class SchemaRegistry:
    """Registry that provides PySpark schemas from the pipeline manifest.

    Caches generated StructTypes so they are only built once.
    """

    def __init__(self, manifest: PipelineManifest) -> None:
        self._manifest = manifest
        self._cache: dict[str, StructType] = {}

    @classmethod
    def from_manifest(cls, path: str | None = None) -> SchemaRegistry:
        """Create a SchemaRegistry from a pipeline manifest file."""
        return cls(load_manifest(path))

    def get_input_schema(self, qualified_name: str) -> StructType:
        """Get the input StructType for a table.

        Args:
            qualified_name: e.g., "bronze.feature_releases"

        Returns:
            PySpark StructType for the input schema.

        Raises:
            KeyError: If the table is not found.
            ValueError: If no input schema is defined.
        """
        cache_key = f"{qualified_name}:input"
        if cache_key not in self._cache:
            table = self._manifest.get_table(qualified_name)
            if not table.input_schema:
                raise ValueError(f"No input schema defined for '{qualified_name}'")
            self._cache[cache_key] = fields_to_struct(table.input_schema)
        return self._cache[cache_key]

    def get_output_schema(self, qualified_name: str) -> StructType:
        """Get the output StructType for a table.

        Args:
            qualified_name: e.g., "bronze.feature_releases"

        Returns:
            PySpark StructType for the output schema.

        Raises:
            KeyError: If the table is not found.
            ValueError: If no output schema is defined.
        """
        cache_key = f"{qualified_name}:output"
        if cache_key not in self._cache:
            table = self._manifest.get_table(qualified_name)
            if not table.output_schema:
                raise ValueError(f"No output schema defined for '{qualified_name}'")
            self._cache[cache_key] = fields_to_struct(table.output_schema)
        return self._cache[cache_key]

    def list_tables(self) -> list[str]:
        """List all table names in the registry."""
        return list(self._manifest.tables.keys())

    def compare_schemas(self, qualified_name: str) -> dict[str, list[str]]:
        """Compare input and output schemas to find differences.

        Returns:
            Dict with 'added', 'removed', 'renamed' keys.
        """
        table = self._manifest.get_table(qualified_name)
        input_names = {f.name for f in table.input_schema}
        output_names = {f.name for f in table.output_schema}

        return {
            "added": sorted(output_names - input_names),
            "removed": sorted(input_names - output_names),
            "common": sorted(input_names & output_names),
        }
