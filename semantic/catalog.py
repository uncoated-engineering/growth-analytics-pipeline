"""Data contract catalog.

Loads the YAML contracts in contracts/ into typed objects. The contracts are
the single source of truth for the data dictionary, the lineage graph, and
the schema context handed to the analytics chatbot.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONTRACTS_DIR = PROJECT_ROOT / "contracts"

LAYERS = ("bronze", "silver", "gold")


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    description: str


@dataclass(frozen=True)
class TableContract:
    table: str
    layer: str
    path: str
    grain: str
    description: str
    owner: str
    upstreams: list[str]
    columns: list[Column]
    source: str | None = None
    partitioned_by: list[str] = field(default_factory=list)

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]


class Catalog:
    """All table contracts, indexed by table name."""

    def __init__(self, contracts_dir: Path | str = CONTRACTS_DIR):
        self.contracts_dir = Path(contracts_dir)
        self.tables: dict[str, TableContract] = {}
        for layer in LAYERS:
            for contract_file in sorted((self.contracts_dir / layer).glob("*.yml")):
                contract = self._load(contract_file)
                self.tables[contract.table] = contract

    @staticmethod
    def _load(path: Path) -> TableContract:
        raw = yaml.safe_load(path.read_text())
        return TableContract(
            table=raw["table"],
            layer=raw["layer"],
            path=raw["path"],
            grain=raw["grain"],
            description=" ".join(raw["description"].split()),
            owner=raw["owner"],
            upstreams=raw.get("upstreams") or [],
            columns=[Column(c["name"], c["type"], c["description"]) for c in raw["columns"]],
            source=raw.get("source"),
            partitioned_by=raw.get("partitioned_by") or [],
        )

    def by_layer(self, layer: str) -> list[TableContract]:
        return [t for t in self.tables.values() if t.layer == layer]

    def lineage_edges(self) -> list[tuple[str, str]]:
        """(upstream, downstream) pairs across all contracts."""
        edges = []
        for table in self.tables.values():
            for upstream in table.upstreams:
                edges.append((upstream, table.table))
        return edges

    def describe(self, table_name: str) -> str:
        """Human/LLM-readable one-table summary."""
        t = self.tables[table_name]
        lines = [
            f"Table: {t.table} (layer: {t.layer})",
            f"Grain: {t.grain}",
            f"Description: {t.description}",
        ]
        if t.upstreams:
            lines.append(f"Upstreams: {', '.join(t.upstreams)}")
        lines.append("Columns:")
        for c in t.columns:
            lines.append(f"  - {c.name} ({c.type}): {c.description}")
        return "\n".join(lines)
