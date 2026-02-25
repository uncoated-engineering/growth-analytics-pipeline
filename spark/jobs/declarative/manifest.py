"""
Pipeline manifest loader and validator.

Loads pipeline.yaml and provides typed access to table definitions,
schemas, quality rules, dependencies, and metrics.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class FreshnessConfig:
    max_age_hours: int
    alert_channel: str | None = None


@dataclass
class QualityRule:
    rule: str
    columns: list[str] | None = None
    column: str | None = None
    values: list[Any] | None = None
    value: Any = None
    min: float | None = None
    max: float | None = None
    sql: str | None = None
    description: str | None = None


@dataclass
class SchemaField:
    name: str
    type: str
    nullable: bool = True


@dataclass
class Transformation:
    type: str
    # Flexible kwargs for different transformation types
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class MetricDefinition:
    name: str
    description: str
    formula: str
    threshold: dict[str, Any] | None = None


@dataclass
class AirflowConfig:
    schedule: str | dict | None = None
    dataset: str | None = None


@dataclass
class SourceConfig:
    file: str | None = None
    format: str = "json"
    options: dict[str, str] = field(default_factory=dict)


@dataclass
class WriteConfig:
    mode: str = "append"
    partition_by: list[str] = field(default_factory=list)


@dataclass
class TableDefinition:
    """Unified table definition across all layers."""

    name: str
    layer: str
    description: str = ""

    # Source configuration (bronze only)
    source: SourceConfig | None = None

    # Schema
    input_schema: list[SchemaField] = field(default_factory=list)
    output_schema: list[SchemaField] = field(default_factory=list)

    # Transformations
    transformations: list[Transformation] = field(default_factory=list)
    transformation_type: str = "declarative"  # "declarative" or "custom"
    module: str | None = None
    function: str | None = None

    # Write configuration (bronze only)
    write: WriteConfig = field(default_factory=WriteConfig)

    # Dependencies
    depends_on: list[str] = field(default_factory=list)

    # Quality rules
    input_quality_rules: list[QualityRule] = field(default_factory=list)
    output_quality_rules: list[QualityRule] = field(default_factory=list)

    # Freshness SLA
    freshness: FreshnessConfig | None = None

    # Airflow config
    airflow: AirflowConfig | None = None

    # Metrics (gold only)
    metrics: list[MetricDefinition] = field(default_factory=list)

    @property
    def qualified_name(self) -> str:
        return f"{self.layer}.{self.name}"


def _parse_quality_rules(rules_list: list[dict] | None) -> list[QualityRule]:
    """Parse quality rules from YAML dicts."""
    if not rules_list:
        return []
    result = []
    for r in rules_list:
        result.append(
            QualityRule(
                rule=r["rule"],
                columns=r.get("columns"),
                column=r.get("column"),
                values=r.get("values"),
                value=r.get("value"),
                min=r.get("min"),
                max=r.get("max"),
                sql=r.get("sql"),
                description=r.get("description"),
            )
        )
    return result


def _parse_schema_fields(fields_list: list[dict] | None) -> list[SchemaField]:
    """Parse schema fields from YAML dicts."""
    if not fields_list:
        return []
    return [
        SchemaField(
            name=f["name"],
            type=f["type"],
            nullable=f.get("nullable", True),
        )
        for f in fields_list
    ]


def _parse_transformations(transforms_list: list[dict] | None) -> list[Transformation]:
    """Parse transformations from YAML dicts."""
    if not transforms_list:
        return []
    result = []
    for t in transforms_list:
        t_type = t["type"]
        params = {k: v for k, v in t.items() if k != "type"}
        result.append(Transformation(type=t_type, params=params))
    return result


def _parse_table(name: str, layer: str, config: dict) -> TableDefinition:
    """Parse a single table definition from YAML config."""
    # Source
    source = None
    if "source" in config:
        src = config["source"]
        source = SourceConfig(
            file=src.get("file"),
            format=src.get("format", "json"),
            options=src.get("options", {}),
        )

    # Schema
    schema_conf = config.get("schema", {})
    input_schema = _parse_schema_fields(schema_conf.get("input"))
    output_schema = _parse_schema_fields(schema_conf.get("output"))

    # Transformations
    transformation_type = config.get("transformation", "declarative")
    transformations = _parse_transformations(config.get("transformations"))

    # Write
    write_conf = config.get("write", {})
    write = WriteConfig(
        mode=write_conf.get("mode", "append"),
        partition_by=write_conf.get("partition_by", []),
    )

    # Quality rules
    qr = config.get("quality_rules", {})
    input_rules = _parse_quality_rules(qr.get("input"))
    output_rules = _parse_quality_rules(qr.get("output"))

    # Freshness
    freshness = None
    if "freshness" in config:
        fc = config["freshness"]
        freshness = FreshnessConfig(
            max_age_hours=fc["max_age_hours"],
            alert_channel=fc.get("alert_channel"),
        )

    # Airflow
    airflow = None
    if "airflow" in config:
        ac = config["airflow"]
        airflow = AirflowConfig(
            schedule=ac.get("schedule"),
            dataset=ac.get("dataset"),
        )

    # Metrics
    metrics = []
    for m in config.get("metrics", []):
        metrics.append(
            MetricDefinition(
                name=m["name"],
                description=m.get("description", ""),
                formula=m["formula"],
                threshold=m.get("threshold"),
            )
        )

    return TableDefinition(
        name=name,
        layer=layer,
        description=config.get("description", ""),
        source=source,
        input_schema=input_schema,
        output_schema=output_schema,
        transformations=transformations,
        transformation_type=transformation_type,
        module=config.get("module"),
        function=config.get("function"),
        write=write,
        depends_on=config.get("depends_on", []),
        input_quality_rules=input_rules,
        output_quality_rules=output_rules,
        freshness=freshness,
        airflow=airflow,
        metrics=metrics,
    )


@dataclass
class PipelineManifest:
    """Parsed pipeline manifest with all table definitions."""

    version: str
    name: str
    description: str
    paths: dict[str, str]
    tables: dict[str, TableDefinition]

    def get_table(self, qualified_name: str) -> TableDefinition:
        """Get a table by qualified name (e.g., 'bronze.feature_releases')."""
        if qualified_name not in self.tables:
            raise KeyError(f"Table '{qualified_name}' not found in manifest")
        return self.tables[qualified_name]

    def get_layer(self, layer: str) -> list[TableDefinition]:
        """Get all tables in a layer (bronze, silver, gold)."""
        return [t for t in self.tables.values() if t.layer == layer]

    def get_downstream(self, qualified_name: str) -> list[TableDefinition]:
        """Get all tables that depend on the given table."""
        return [t for t in self.tables.values() if qualified_name in t.depends_on]

    def get_upstream(self, qualified_name: str) -> list[TableDefinition]:
        """Get all tables that the given table depends on."""
        table = self.get_table(qualified_name)
        return [self.get_table(dep) for dep in table.depends_on]

    def topological_order(self) -> list[TableDefinition]:
        """Return tables in dependency order (upstream before downstream)."""
        visited: set[str] = set()
        result: list[TableDefinition] = []

        def visit(name: str) -> None:
            if name in visited:
                return
            visited.add(name)
            table = self.tables[name]
            for dep in table.depends_on:
                if dep in self.tables:
                    visit(dep)
            result.append(table)

        for name in self.tables:
            visit(name)
        return result

    def validate(self) -> list[str]:
        """Validate the manifest for consistency. Returns list of errors."""
        errors = []

        # Check all dependencies reference existing tables
        for table in self.tables.values():
            for dep in table.depends_on:
                if dep not in self.tables:
                    errors.append(
                        f"[{table.qualified_name}] depends on '{dep}' which is not defined"
                    )

        # Check for circular dependencies
        def has_cycle(name: str, path: set[str]) -> bool:
            if name in path:
                return True
            path.add(name)
            for dep in self.tables.get(name, TableDefinition(name="", layer="")).depends_on:
                if dep in self.tables and has_cycle(dep, path.copy()):
                    return True
            return False

        for name in self.tables:
            if has_cycle(name, set()):
                errors.append(f"[{name}] is part of a circular dependency")

        # Check bronze tables have source config
        for table in self.get_layer("bronze"):
            if not table.source:
                errors.append(f"[{table.qualified_name}] bronze table missing source config")

        # Check silver/gold tables with custom transformation have module+function
        for table in self.get_layer("silver") + self.get_layer("gold"):
            if table.transformation_type == "custom":
                if not table.module or not table.function:
                    errors.append(
                        f"[{table.qualified_name}] custom transformation requires "
                        f"'module' and 'function'"
                    )

        return errors

    def summary(self) -> str:
        """Generate a human-readable summary of the pipeline."""
        lines = [
            f"Pipeline: {self.name} (v{self.version})",
            f"Description: {self.description.strip()}",
            "",
        ]

        for layer in ["bronze", "silver", "gold"]:
            tables = self.get_layer(layer)
            if not tables:
                continue
            lines.append(f"{'=' * 60}")
            lines.append(f"  {layer.upper()} LAYER ({len(tables)} tables)")
            lines.append(f"{'=' * 60}")

            for table in tables:
                lines.append(f"  {table.name}")
                lines.append(f"    {table.description[:70]}...")
                if table.output_schema:
                    cols = [f.name for f in table.output_schema]
                    lines.append(f"    Columns: {', '.join(cols)}")
                if table.depends_on:
                    lines.append(f"    Depends on: {', '.join(table.depends_on)}")
                n_rules = len(table.input_quality_rules) + len(table.output_quality_rules)
                if n_rules:
                    lines.append(f"    Quality rules: {n_rules}")
                if table.metrics:
                    lines.append(f"    Metrics: {', '.join(m.name for m in table.metrics)}")
                lines.append("")

        return "\n".join(lines)


def load_manifest(path: str | None = None) -> PipelineManifest:
    """Load and parse the pipeline manifest from YAML.

    Args:
        path: Path to pipeline.yaml. If None, searches upward from CWD.

    Returns:
        Parsed PipelineManifest.
    """
    if path is None:
        # Search upward for pipeline.yaml
        search_dir = Path(os.getcwd())
        while search_dir != search_dir.parent:
            candidate = search_dir / "pipeline.yaml"
            if candidate.exists():
                path = str(candidate)
                break
            search_dir = search_dir.parent
        if path is None:
            # Also check relative to this file
            project_root = Path(__file__).resolve().parents[3]
            path = str(project_root / "pipeline.yaml")

    with open(path) as f:
        raw = yaml.safe_load(f)

    tables: dict[str, TableDefinition] = {}

    for layer in ["bronze", "silver", "gold"]:
        layer_config = raw.get(layer, {})
        for table_name, table_config in layer_config.items():
            qualified_name = f"{layer}.{table_name}"
            tables[qualified_name] = _parse_table(table_name, layer, table_config)

    manifest = PipelineManifest(
        version=raw.get("version", "1.0"),
        name=raw.get("name", ""),
        description=raw.get("description", ""),
        paths=raw.get("paths", {}),
        tables=tables,
    )

    errors = manifest.validate()
    if errors:
        error_msg = "\n".join(f"  - {e}" for e in errors)
        raise ValueError(f"Pipeline manifest validation failed:\n{error_msg}")

    return manifest
