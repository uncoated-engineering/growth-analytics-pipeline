"""Command-line interface for the semantic layer.

Examples:
    uv run python -m semantic.cli metrics
    uv run python -m semantic.cli tables
    uv run python -m semantic.cli describe silver_user_dim
    uv run python -m semantic.cli query conversion_rate -d acquisition_channel
    uv run python -m semantic.cli query ending_mrr net_revenue_retention -d month
    uv run python -m semantic.cli sql "SELECT count(*) FROM silver_user_dim"
"""

from __future__ import annotations

import argparse
import sys


def _print_table(columns: list[str], rows: list[tuple]) -> None:
    widths = [len(c) for c in columns]
    rendered = [[("" if v is None else str(v)) for v in row] for row in rows]
    for row in rendered:
        widths = [max(w, len(v)) for w, v in zip(widths, row, strict=False)]
    header = "  ".join(c.ljust(w) for c, w in zip(columns, widths, strict=False))
    print(header)
    print("-" * len(header))
    for row in rendered:
        print("  ".join(v.ljust(w) for v, w in zip(row, widths, strict=False)))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="semantic", description="Lakehouse semantic layer")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("metrics", help="List governed metrics")
    sub.add_parser("tables", help="List lakehouse tables and availability")

    p_describe = sub.add_parser("describe", help="Describe a table from its contract")
    p_describe.add_argument("table")

    p_query = sub.add_parser("query", help="Run a governed metric query")
    p_query.add_argument("metric", nargs="+", help="Metric name(s), same table")
    p_query.add_argument("-d", "--dimension", action="append", default=[], dest="dimensions")
    p_query.add_argument(
        "-f",
        "--filter",
        action="append",
        default=[],
        dest="filters",
        help="column=value filter (repeatable)",
    )
    p_query.add_argument("-o", "--order-by", default=None)
    p_query.add_argument("-l", "--limit", type=int, default=None)

    p_sql = sub.add_parser("sql", help="Run a read-only SQL query")
    p_sql.add_argument("query")

    args = parser.parse_args(argv)

    # Imports deferred so `--help` stays instant
    from semantic.catalog import Catalog
    from semantic.engine import LakehouseEngine
    from semantic.metrics import MetricStore, SemanticLayer

    if args.command == "metrics":
        store = MetricStore()
        for m in store.metrics.values():
            dims = ", ".join(m.dimensions) or "(none)"
            print(f"{m.name:<26} {m.label:<32} dims: {dims}")
        return 0

    if args.command == "tables":
        engine = LakehouseEngine()
        for name, contract in engine.catalog.tables.items():
            status = "available" if name in engine.available_tables else "MISSING ON DISK"
            print(f"{name:<32} {contract.layer:<7} {status}")
        return 0

    if args.command == "describe":
        print(Catalog().describe(args.table))
        return 0

    if args.command == "query":
        filters = []
        for spec in args.filters:
            column, _, value = spec.partition("=")
            filters.append({"column": column, "op": "=", "value": value})
        layer = SemanticLayer()
        columns, rows = layer.query(
            args.metric,
            dimensions=args.dimensions,
            filters=filters,
            order_by=args.order_by,
            limit=args.limit,
        )
        _print_table(columns, rows)
        return 0

    if args.command == "sql":
        engine = LakehouseEngine()
        columns, rows = engine.sql_rows(args.query)
        _print_table(columns, rows)
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
