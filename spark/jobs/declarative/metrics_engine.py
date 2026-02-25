"""
Declarative metrics engine for gold-layer KPIs.

Evaluates metric definitions from pipeline.yaml against gold-layer
Delta tables. Supports threshold-based alerting and trend tracking.

Metric formulas reference column values by cohort:
    "conversion_rate[used_feature] / conversion_rate[available_not_used]"

Usage:
    from spark.jobs.declarative.metrics_engine import MetricsEngine

    engine = MetricsEngine.from_manifest()
    results = engine.evaluate_metrics(spark, "gold.feature_conversion_impact")
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from spark.jobs.declarative.manifest import (
    PipelineManifest,
    load_manifest,
)


@dataclass
class MetricResult:
    """Result of evaluating a single metric."""

    name: str
    description: str
    feature_name: str
    value: float | None
    status: str  # "ok", "warning", "critical", "error"
    message: str
    threshold: dict | None = None


@dataclass
class MetricsReport:
    """Report of all evaluated metrics for a table."""

    table_name: str
    results: list[MetricResult]

    @property
    def has_warnings(self) -> bool:
        return any(r.status == "warning" for r in self.results)

    @property
    def has_critical(self) -> bool:
        return any(r.status == "critical" for r in self.results)

    def summary(self) -> str:
        lines = [
            f"Metrics Report: {self.table_name}",
            "=" * 60,
        ]

        # Group by feature
        features = sorted({r.feature_name for r in self.results})
        for feature in features:
            lines.append(f"\n  Feature: {feature}")
            feature_results = [r for r in self.results if r.feature_name == feature]
            for r in feature_results:
                icon = {"ok": "[OK]", "warning": "[WARN]", "critical": "[CRIT]", "error": "[ERR]"}
                status_icon = icon.get(r.status, "[?]")
                value_str = f"{r.value:.3f}" if r.value is not None else "N/A"
                lines.append(f"    {status_icon} {r.name}: {value_str} - {r.message}")

        return "\n".join(lines)


def _parse_formula_references(formula: str) -> list[tuple[str, str]]:
    """Extract column[cohort] references from a formula.

    Returns:
        List of (column_name, cohort_name) tuples.
    """
    pattern = r"(\w+)\[(\w+)\]"
    return re.findall(pattern, formula)


def _evaluate_formula(df: DataFrame, formula: str, feature_name: str) -> float | None:
    """Evaluate a metric formula against a DataFrame for a specific feature.

    Formulas use the pattern: column[cohort] to reference values.
    Example: "conversion_rate[used_feature] / conversion_rate[available_not_used]"
    """
    references = _parse_formula_references(formula)
    values: dict[str, float] = {}

    for col_name, cohort in references:
        key = f"{col_name}[{cohort}]"
        rows = (
            df.filter((col("feature_name") == feature_name) & (col("cohort") == cohort))
            .select(col_name)
            .collect()
        )
        if not rows or rows[0][0] is None:
            return None
        values[key] = float(rows[0][0])

    # Build and evaluate the expression
    expression = formula
    for key, value in values.items():
        expression = expression.replace(key, str(value))

    try:
        return float(eval(expression))  # noqa: S307
    except (ZeroDivisionError, ValueError, TypeError):
        return None


def _check_threshold(value: float | None, threshold: dict | None) -> tuple[str, str]:
    """Check a metric value against thresholds.

    Returns:
        (status, message) tuple.
    """
    if value is None:
        return "error", "Could not compute metric (missing data)"

    if not threshold:
        return "ok", f"Value: {value:.3f}"

    direction = threshold.get("direction", "above")
    warning_threshold = threshold.get("warning")
    critical_threshold = threshold.get("critical")

    if direction == "above":
        # Value should be above thresholds
        if critical_threshold is not None and value < critical_threshold:
            return "critical", f"Value {value:.3f} below critical threshold {critical_threshold}"
        if warning_threshold is not None and value < warning_threshold:
            return "warning", f"Value {value:.3f} below warning threshold {warning_threshold}"
        return "ok", f"Value {value:.3f} above thresholds"
    else:
        # Value should be below thresholds
        if critical_threshold is not None and value > critical_threshold:
            return "critical", f"Value {value:.3f} above critical threshold {critical_threshold}"
        if warning_threshold is not None and value > warning_threshold:
            return "warning", f"Value {value:.3f} above warning threshold {warning_threshold}"
        return "ok", f"Value {value:.3f} within thresholds"


class MetricsEngine:
    """Evaluate declarative metrics from pipeline manifest."""

    def __init__(self, manifest: PipelineManifest) -> None:
        self._manifest = manifest

    @classmethod
    def from_manifest(cls, path: str | None = None) -> MetricsEngine:
        return cls(load_manifest(path))

    def evaluate_metrics(
        self,
        spark: SparkSession,
        qualified_name: str,
        df: DataFrame | None = None,
    ) -> MetricsReport:
        """Evaluate all declared metrics for a gold table.

        Args:
            spark: SparkSession.
            qualified_name: e.g., "gold.feature_conversion_impact"
            df: Optional DataFrame. If None, reads from the Delta table path.

        Returns:
            MetricsReport with all evaluated metrics.
        """
        table = self._manifest.get_table(qualified_name)

        if df is None:
            paths = self._manifest.paths
            gold_path = paths.get("gold", "data/gold")
            table_path = f"{gold_path}/gold_{table.name}"
            df = spark.read.format("delta").load(table_path)

        results: list[MetricResult] = []

        # Get all unique feature names in the data
        feature_names = [row[0] for row in df.select("feature_name").distinct().collect()]

        for metric in table.metrics:
            for feature_name in feature_names:
                value = _evaluate_formula(df, metric.formula, feature_name)
                status, message = _check_threshold(value, metric.threshold)

                results.append(
                    MetricResult(
                        name=metric.name,
                        description=metric.description,
                        feature_name=feature_name,
                        value=value,
                        status=status,
                        message=message,
                        threshold=metric.threshold,
                    )
                )

        return MetricsReport(
            table_name=qualified_name,
            results=results,
        )
