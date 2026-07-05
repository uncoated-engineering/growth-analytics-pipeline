"""DuckDB query engine over the Delta lakehouse.

Registers every table from the contract catalog as a DuckDB view (resolving
the *current* Delta snapshot via delta-rs, so stale files from previous
overwrites are never read) and exposes a guarded, read-only SQL interface.

This is the serving layer for ad-hoc analysis and the analytics chatbot:
DuckDB gives sub-second SQL over the same files Spark writes, without a
running cluster.
"""

from __future__ import annotations

import re
from pathlib import Path

import duckdb
from deltalake import DeltaTable

from semantic.catalog import PROJECT_ROOT, Catalog


class QueryRejectedError(Exception):
    """Raised when a SQL statement fails the read-only guard."""


_FORBIDDEN_KEYWORDS = (
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "create",
    "attach",
    "copy",
    "export",
    "install",
    "load",
    "pragma",
    "set",
)


def _assert_read_only(sql: str) -> None:
    """Allow a single SELECT/WITH statement, nothing else."""
    stripped = sql.strip().rstrip(";").strip()
    if ";" in stripped:
        raise QueryRejectedError("Only a single statement is allowed.")
    first_word = stripped.split(None, 1)[0].lower() if stripped else ""
    if first_word not in ("select", "with", "describe", "show"):
        raise QueryRejectedError(f"Only read queries are allowed, got '{first_word or 'empty'}'.")
    # Crude but effective: none of these keywords appear in legitimate
    # analytical SELECTs against this schema.
    for keyword in _FORBIDDEN_KEYWORDS:
        if re.search(rf"\b{keyword}\b", stripped, flags=re.IGNORECASE):
            raise QueryRejectedError(f"Statement contains forbidden keyword '{keyword}'.")


class LakehouseEngine:
    """Read-only DuckDB session with all lakehouse tables registered as views."""

    def __init__(self, project_root: Path | str = PROJECT_ROOT, catalog: Catalog | None = None):
        self.project_root = Path(project_root)
        self.catalog = catalog or Catalog()
        self.conn = duckdb.connect(database=":memory:")
        self.available_tables: list[str] = []
        self.missing_tables: list[str] = []
        self._register_views()

    def _register_views(self) -> None:
        for contract in self.catalog.tables.values():
            table_path = self.project_root / contract.path
            if not (table_path / "_delta_log").exists():
                self.missing_tables.append(contract.table)
                continue
            files = DeltaTable(str(table_path)).file_uris()
            if not files:
                self.missing_tables.append(contract.table)
                continue
            file_list = ", ".join(f"'{f}'" for f in files)
            self.conn.execute(
                f"CREATE OR REPLACE VIEW {contract.table} AS "
                f"SELECT * FROM read_parquet([{file_list}])"
            )
            self.available_tables.append(contract.table)

    def sql(self, query: str) -> "duckdb.DuckDBPyRelation":
        """Run a guarded read-only query."""
        _assert_read_only(query)
        return self.conn.sql(query)

    def sql_rows(self, query: str, limit: int = 200) -> tuple[list[str], list[tuple]]:
        """Run a query and return (column_names, rows), capped at `limit` rows."""
        relation = self.sql(query)
        rows = relation.fetchmany(limit)
        return list(relation.columns), rows
