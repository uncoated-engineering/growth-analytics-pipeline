"""Semantic layer: governed metrics and a query engine over the lakehouse.

- catalog: data contracts (tables, columns, lineage) loaded from contracts/
- engine: DuckDB SQL engine reading the Delta tables
- metrics: metric definitions (semantic/metrics.yml) compiled to SQL
"""

from semantic.catalog import Catalog
from semantic.engine import LakehouseEngine
from semantic.metrics import MetricStore, SemanticLayer

__all__ = ["Catalog", "LakehouseEngine", "MetricStore", "SemanticLayer"]
