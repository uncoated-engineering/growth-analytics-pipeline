"""Tests for the DuckDB lakehouse engine (uses delta-rs, no Spark session)."""

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from semantic.engine import LakehouseEngine, QueryRejectedError, _assert_read_only


@pytest.fixture()
def mini_lakehouse(tmp_path):
    """A minimal project root with one contract and one Delta table."""
    contracts_dir = tmp_path / "contracts"
    for layer in ("bronze", "silver", "gold"):
        (contracts_dir / layer).mkdir(parents=True)
    (contracts_dir / "gold" / "gold_numbers.yml").write_text("""table: gold_numbers
layer: gold
path: data/gold/gold_numbers
grain: One row per id
description: Test table.
owner: tests
upstreams: []
columns:
  - name: id
    type: integer
    description: Identifier
  - name: amount
    type: integer
    description: Amount
""")
    table = pa.table({"id": [1, 2, 3], "amount": [10, 20, 30]})
    write_deltalake(str(tmp_path / "data" / "gold" / "gold_numbers"), table)
    return tmp_path, contracts_dir


class TestReadOnlyGuard:
    """Test suite for the SQL statement guard."""

    def test_select_allowed(self):
        _assert_read_only("SELECT 1")

    def test_cte_allowed(self):
        _assert_read_only("WITH x AS (SELECT 1 AS a) SELECT * FROM x")

    def test_trailing_semicolon_allowed(self):
        _assert_read_only("SELECT 1;")

    @pytest.mark.parametrize(
        "statement",
        [
            "DROP TABLE silver_user_dim",
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET x = 1",
            "DELETE FROM t",
            "CREATE TABLE t (x INT)",
            "SELECT 1; DROP TABLE t",
            "COPY t TO 'out.csv'",
            "INSTALL httpfs",
            "PRAGMA database_list",
        ],
    )
    def test_writes_and_multi_statements_rejected(self, statement):
        with pytest.raises(QueryRejectedError):
            _assert_read_only(statement)


class TestLakehouseEngine:
    """Test suite for Delta view registration and querying."""

    def test_registers_current_delta_snapshot(self, mini_lakehouse):
        root, contracts_dir = mini_lakehouse
        from semantic.catalog import Catalog

        engine = LakehouseEngine(project_root=root, catalog=Catalog(contracts_dir))
        assert engine.available_tables == ["gold_numbers"]

        columns, rows = engine.sql_rows("SELECT SUM(amount) AS total FROM gold_numbers")
        assert columns == ["total"]
        assert rows[0][0] == 60

    def test_overwritten_table_reads_only_latest_version(self, mini_lakehouse):
        """Old parquet files from a previous Delta version must not be read."""
        root, contracts_dir = mini_lakehouse
        from semantic.catalog import Catalog

        table = pa.table({"id": [9], "amount": [999]})
        write_deltalake(str(root / "data" / "gold" / "gold_numbers"), table, mode="overwrite")

        engine = LakehouseEngine(project_root=root, catalog=Catalog(contracts_dir))
        columns, rows = engine.sql_rows(
            "SELECT COUNT(*) AS n, SUM(amount) AS total FROM gold_numbers"
        )
        assert rows[0] == (1, 999)

    def test_missing_table_reported_not_fatal(self, mini_lakehouse):
        root, contracts_dir = mini_lakehouse
        (contracts_dir / "gold" / "gold_ghost.yml").write_text("""table: gold_ghost
layer: gold
path: data/gold/gold_ghost
grain: One row per nothing
description: Never materialized.
owner: tests
upstreams: []
columns:
  - name: id
    type: integer
    description: Identifier
""")
        from semantic.catalog import Catalog

        engine = LakehouseEngine(project_root=root, catalog=Catalog(contracts_dir))
        assert "gold_ghost" in engine.missing_tables
        assert "gold_numbers" in engine.available_tables

    def test_guard_enforced_via_engine(self, mini_lakehouse):
        root, contracts_dir = mini_lakehouse
        from semantic.catalog import Catalog

        engine = LakehouseEngine(project_root=root, catalog=Catalog(contracts_dir))
        with pytest.raises(QueryRejectedError):
            engine.sql("DROP TABLE gold_numbers")
