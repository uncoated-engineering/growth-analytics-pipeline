"""Metric store and semantic query compiler.

Loads semantic/metrics.yml and compiles governed metric requests
(metric + dimensions + filters) into DuckDB SQL executed by the
LakehouseEngine. Because every metric is an aggregate expression, ratio
metrics re-aggregate correctly at any grain.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

from semantic.engine import LakehouseEngine

METRICS_FILE = Path(__file__).resolve().parent / "metrics.yml"

_ALLOWED_FILTER_OPS = ("=", "!=", ">", ">=", "<", "<=", "in", "not in", "like")


class SemanticError(Exception):
    """Raised for unknown metrics/dimensions or invalid filter specs."""


@dataclass(frozen=True)
class Metric:
    name: str
    label: str
    description: str
    table: str
    expression: str
    dimensions: list[str]
    time_dimension: str | None = None
    format: str = "number"


@dataclass(frozen=True)
class MetricQuery:
    """A compiled semantic query, ready to execute."""

    sql: str
    metrics: list[str] = field(default_factory=list)
    dimensions: list[str] = field(default_factory=list)


class MetricStore:
    """All metric definitions, indexed by name."""

    def __init__(self, metrics_file: Path | str = METRICS_FILE):
        raw = yaml.safe_load(Path(metrics_file).read_text())
        self.metrics: dict[str, Metric] = {}
        for entry in raw["metrics"]:
            metric = Metric(
                name=entry["name"],
                label=entry["label"],
                description=" ".join(entry["description"].split()),
                table=entry["table"],
                expression=" ".join(entry["expression"].split()),
                dimensions=entry.get("dimensions") or [],
                time_dimension=entry.get("time_dimension"),
                format=entry.get("format", "number"),
            )
            self.metrics[metric.name] = metric

    def get(self, name: str) -> Metric:
        if name not in self.metrics:
            raise SemanticError(
                f"Unknown metric '{name}'. Available: {', '.join(sorted(self.metrics))}"
            )
        return self.metrics[name]


def _quote_value(value) -> str:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def compile_metric_query(
    store: MetricStore,
    metric_names: list[str],
    dimensions: list[str] | None = None,
    filters: list[dict] | None = None,
    order_by: str | None = None,
    limit: int | None = None,
) -> MetricQuery:
    """Compile a governed metric request into SQL.

    All requested metrics must live on the same table. Dimensions and filter
    columns are validated against the metrics' allowed dimension lists.

    Filters are dicts: {"column": ..., "op": ..., "value": ...}; `value` is a
    list for in/not in.
    """
    if not metric_names:
        raise SemanticError("At least one metric is required.")
    metrics = [store.get(name) for name in metric_names]

    tables = {m.table for m in metrics}
    if len(tables) > 1:
        raise SemanticError(
            f"Metrics span multiple tables ({', '.join(sorted(tables))}); " "query them separately."
        )
    table = metrics[0].table

    allowed_dims = set.intersection(*(set(m.dimensions) for m in metrics))
    dimensions = dimensions or []
    for dim in dimensions:
        if dim not in allowed_dims:
            raise SemanticError(
                f"Dimension '{dim}' is not allowed for {'/'.join(metric_names)}. "
                f"Allowed: {', '.join(sorted(allowed_dims)) or '(none)'}"
            )

    where_clauses = []
    for spec in filters or []:
        column, op, value = spec.get("column"), spec.get("op", "="), spec.get("value")
        if op not in _ALLOWED_FILTER_OPS:
            raise SemanticError(f"Filter op '{op}' not allowed. Allowed: {_ALLOWED_FILTER_OPS}")
        if column not in allowed_dims:
            raise SemanticError(f"Filter column '{column}' is not an allowed dimension.")
        if op in ("in", "not in"):
            if not isinstance(value, (list, tuple)) or not value:
                raise SemanticError(f"Filter op '{op}' requires a non-empty list value.")
            rendered = f"({', '.join(_quote_value(v) for v in value)})"
        else:
            rendered = _quote_value(value)
        where_clauses.append(f"{column} {op.upper()} {rendered}")

    select_parts = list(dimensions) + [f"{m.expression} AS {m.name}" for m in metrics]
    sql = f"SELECT {', '.join(select_parts)}\nFROM {table}"
    if where_clauses:
        sql += "\nWHERE " + " AND ".join(where_clauses)
    if dimensions:
        sql += "\nGROUP BY " + ", ".join(dimensions)
    if order_by:
        valid_order_cols = set(dimensions) | {m.name for m in metrics}
        order_col = order_by.removesuffix(" desc").removesuffix(" asc").strip()
        if order_col not in valid_order_cols:
            raise SemanticError(f"order_by must be one of {sorted(valid_order_cols)}")
        sql += f"\nORDER BY {order_by}"
    elif dimensions:
        sql += "\nORDER BY " + ", ".join(dimensions)
    if limit is not None:
        sql += f"\nLIMIT {int(limit)}"

    return MetricQuery(sql=sql, metrics=metric_names, dimensions=dimensions)


class SemanticLayer:
    """Governed metric queries over the lakehouse."""

    def __init__(self, engine: LakehouseEngine | None = None, store: MetricStore | None = None):
        self.engine = engine or LakehouseEngine()
        self.store = store or MetricStore()

    def query(
        self,
        metrics: list[str] | str,
        dimensions: list[str] | None = None,
        filters: list[dict] | None = None,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> tuple[list[str], list[tuple]]:
        """Execute a governed metric query; returns (columns, rows)."""
        if isinstance(metrics, str):
            metrics = [metrics]
        compiled = compile_metric_query(self.store, metrics, dimensions, filters, order_by, limit)
        return self.engine.sql_rows(compiled.sql)

    def describe_for_llm(self) -> str:
        """Compact catalog of metrics for grounding an LLM."""
        lines = []
        for m in self.store.metrics.values():
            dims = ", ".join(m.dimensions) or "(none)"
            lines.append(f"- {m.name} [{m.format}] (dims: {dims}): {m.description}")
        return "\n".join(lines)
