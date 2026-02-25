"""
CLI entry point for declarative pipeline operations.

Provides commands for:
    - Pipeline info/summary
    - Lineage graph visualization
    - Impact analysis
    - Manifest validation
    - Declarative bronze ingestion
    - Quality validation
    - Metric evaluation

Usage:
    python -m spark.jobs.declarative.cli info
    python -m spark.jobs.declarative.cli lineage
    python -m spark.jobs.declarative.cli impact bronze.feature_releases
    python -m spark.jobs.declarative.cli validate
    python -m spark.jobs.declarative.cli ingest-bronze
    python -m spark.jobs.declarative.cli quality [--layer bronze]
    python -m spark.jobs.declarative.cli metrics
    python -m spark.jobs.declarative.cli dot > pipeline.dot
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure project root is on the path
_PROJECT_ROOT = str(Path(__file__).resolve().parents[3])
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def cmd_info(args: argparse.Namespace) -> None:
    """Show pipeline summary."""
    from spark.jobs.declarative.manifest import load_manifest

    manifest = load_manifest(args.manifest)
    print(manifest.summary())


def cmd_lineage(args: argparse.Namespace) -> None:
    """Show pipeline lineage graph."""
    from spark.jobs.declarative.lineage import PipelineLineage

    lineage = PipelineLineage.from_manifest(args.manifest)
    print(lineage.ascii_graph())


def cmd_dot(args: argparse.Namespace) -> None:
    """Generate DOT graph for Graphviz."""
    from spark.jobs.declarative.lineage import PipelineLineage

    lineage = PipelineLineage.from_manifest(args.manifest)
    print(lineage.dot_graph())


def cmd_impact(args: argparse.Namespace) -> None:
    """Show impact analysis for a table."""
    from spark.jobs.declarative.lineage import PipelineLineage

    lineage = PipelineLineage.from_manifest(args.manifest)
    print(lineage.impact_analysis(args.table))


def cmd_upstream(args: argparse.Namespace) -> None:
    """Trace upstream dependencies."""
    from spark.jobs.declarative.lineage import PipelineLineage

    lineage = PipelineLineage.from_manifest(args.manifest)
    print(lineage.upstream_trace(args.table))


def cmd_health(args: argparse.Namespace) -> None:
    """Show pipeline health dashboard."""
    from spark.jobs.declarative.lineage import PipelineLineage

    lineage = PipelineLineage.from_manifest(args.manifest)
    print(lineage.pipeline_health())


def cmd_validate(args: argparse.Namespace) -> None:
    """Validate the pipeline manifest."""
    from spark.jobs.declarative.manifest import load_manifest

    try:
        manifest = load_manifest(args.manifest)
        print(f"Manifest valid: {manifest.name} v{manifest.version}")
        print(f"  Tables: {len(manifest.tables)}")
        for layer in ["bronze", "silver", "gold"]:
            count = len(manifest.get_layer(layer))
            print(f"  {layer}: {count} tables")
    except ValueError as e:
        print(f"Validation failed: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_ingest_bronze(args: argparse.Namespace) -> None:
    """Run declarative bronze ingestion."""
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    from spark.jobs.declarative.bronze_engine import BronzeEngine

    builder = (
        SparkSession.builder.appName("Declarative Bronze Ingestion")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", "spark-warehouse")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        engine = BronzeEngine.from_manifest(args.manifest)
        stats = engine.ingest_all(spark)
        print("\nDeclarative Bronze Ingestion Summary:")
        for table, count in stats.items():
            print(f"  {table}: {count} rows")
    finally:
        spark.stop()


def cmd_quality(args: argparse.Namespace) -> None:
    """Run quality validation against existing Delta tables."""
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    from spark.jobs.declarative.quality_engine import QualityEngine

    builder = (
        SparkSession.builder.appName("Declarative Quality Validation")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", "spark-warehouse")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        engine = QualityEngine.from_manifest(args.manifest)
        reports = engine.validate_all(spark, layer=args.layer)
        for report in reports:
            print(report.summary())
            print()
    finally:
        spark.stop()


def cmd_metrics(args: argparse.Namespace) -> None:
    """Evaluate gold-layer metrics."""
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    from spark.jobs.declarative.manifest import load_manifest
    from spark.jobs.declarative.metrics_engine import MetricsEngine

    builder = (
        SparkSession.builder.appName("Declarative Metrics Evaluation")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", "spark-warehouse")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        engine = MetricsEngine.from_manifest(args.manifest)
        manifest = load_manifest(args.manifest)
        for table in manifest.get_layer("gold"):
            if table.metrics:
                report = engine.evaluate_metrics(spark, table.qualified_name)
                print(report.summary())
                print()
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Declarative pipeline management CLI",
        prog="python -m spark.jobs.declarative.cli",
    )
    parser.add_argument(
        "--manifest",
        default=None,
        help="Path to pipeline.yaml (auto-detected if omitted)",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # info
    subparsers.add_parser("info", help="Show pipeline summary")

    # lineage
    subparsers.add_parser("lineage", help="Show dependency lineage graph")

    # dot
    subparsers.add_parser("dot", help="Generate DOT graph for Graphviz")

    # impact
    impact_parser = subparsers.add_parser("impact", help="Impact analysis for a table")
    impact_parser.add_argument("table", help="Qualified table name (e.g., bronze.feature_releases)")

    # upstream
    upstream_parser = subparsers.add_parser("upstream", help="Trace upstream dependencies")
    upstream_parser.add_argument("table", help="Qualified table name")

    # health
    subparsers.add_parser("health", help="Pipeline health dashboard")

    # validate
    subparsers.add_parser("validate", help="Validate the pipeline manifest")

    # ingest-bronze
    subparsers.add_parser("ingest-bronze", help="Run declarative bronze ingestion")

    # quality
    quality_parser = subparsers.add_parser("quality", help="Run quality validation")
    quality_parser.add_argument("--layer", default=None, help="Filter by layer")

    # metrics
    subparsers.add_parser("metrics", help="Evaluate gold-layer metrics")

    args = parser.parse_args()

    commands = {
        "info": cmd_info,
        "lineage": cmd_lineage,
        "dot": cmd_dot,
        "impact": cmd_impact,
        "upstream": cmd_upstream,
        "health": cmd_health,
        "validate": cmd_validate,
        "ingest-bronze": cmd_ingest_bronze,
        "quality": cmd_quality,
        "metrics": cmd_metrics,
    }

    if args.command in commands:
        commands[args.command](args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
