"""
Declarative data quality rules engine.

Executes quality rules defined in pipeline.yaml against PySpark DataFrames.
Replaces the boilerplate validate.py files with a single, configurable engine.

Supported rules:
    not_null         - Check that columns have no null values
    unique           - Check that column values are unique
    min_rows         - Check minimum row count
    range            - Check numeric column is within a range
    accepted_values  - Check column values are in an allowed set
    custom_sql       - Run arbitrary SQL and fail if it returns rows
    freshness        - Check data is not stale (based on timestamp column)
    referential      - Check foreign key references exist

Usage:
    from spark.jobs.declarative.quality_engine import QualityEngine

    engine = QualityEngine.from_manifest()
    results = engine.validate_table(spark, "bronze.feature_releases", df, phase="output")
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import max as spark_max

from spark.jobs.declarative.manifest import (
    PipelineManifest,
    QualityRule,
    TableDefinition,
    load_manifest,
)


@dataclass
class RuleResult:
    """Result of a single quality rule check."""

    rule_name: str
    passed: bool
    message: str
    details: dict | None = None


@dataclass
class ValidationReport:
    """Complete validation report for a table."""

    table_name: str
    phase: str  # "input" or "output"
    passed: bool
    results: list[RuleResult]
    row_count: int

    def summary(self) -> str:
        lines = [
            f"Validation: {self.table_name} ({self.phase})",
            f"  Status: {'PASS' if self.passed else 'FAIL'}",
            f"  Rows: {self.row_count}",
        ]
        for r in self.results:
            status = "PASS" if r.passed else "FAIL"
            lines.append(f"  [{status}] {r.rule_name}: {r.message}")
        return "\n".join(lines)


def _check_not_null(df: DataFrame, rule: QualityRule) -> RuleResult:
    """Check that specified columns have no null values."""
    columns = rule.columns or []
    null_counts = {}
    for col_name in columns:
        if col_name not in df.columns:
            return RuleResult(
                rule_name="not_null",
                passed=False,
                message=f"Column '{col_name}' not found in DataFrame",
            )
        null_count = df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            null_counts[col_name] = null_count

    if null_counts:
        return RuleResult(
            rule_name="not_null",
            passed=False,
            message=f"Null values found: {null_counts}",
            details=null_counts,
        )
    return RuleResult(
        rule_name="not_null",
        passed=True,
        message=f"No nulls in columns: {columns}",
    )


def _check_unique(df: DataFrame, rule: QualityRule) -> RuleResult:
    """Check that specified columns have unique values."""
    columns = rule.columns or []
    for col_name in columns:
        if col_name not in df.columns:
            return RuleResult(
                rule_name="unique",
                passed=False,
                message=f"Column '{col_name}' not found in DataFrame",
            )
    total = df.count()
    distinct = df.select(columns).distinct().count()
    if total != distinct:
        return RuleResult(
            rule_name="unique",
            passed=False,
            message=f"Duplicate values found: {total} total vs {distinct} distinct "
            f"for columns {columns}",
            details={"total": total, "distinct": distinct},
        )
    return RuleResult(
        rule_name="unique",
        passed=True,
        message=f"All values unique for columns: {columns}",
    )


def _check_min_rows(df: DataFrame, rule: QualityRule) -> RuleResult:
    """Check that DataFrame has at least N rows."""
    min_rows = rule.value or 1
    actual = df.count()
    if actual < min_rows:
        return RuleResult(
            rule_name="min_rows",
            passed=False,
            message=f"Expected at least {min_rows} rows, got {actual}",
            details={"expected": min_rows, "actual": actual},
        )
    return RuleResult(
        rule_name="min_rows",
        passed=True,
        message=f"Row count {actual} >= {min_rows}",
    )


def _check_range(df: DataFrame, rule: QualityRule) -> RuleResult:
    """Check that a numeric column falls within a range."""
    col_name = rule.column
    if not col_name or col_name not in df.columns:
        return RuleResult(
            rule_name="range",
            passed=False,
            message=f"Column '{col_name}' not found in DataFrame",
        )

    violations = df
    if rule.min is not None:
        violations = violations.filter(col(col_name) < rule.min)
    if rule.max is not None:
        violations = violations.filter(col(col_name) > rule.max)

    # Re-apply: count rows outside range
    out_of_range = df
    conditions = []
    if rule.min is not None:
        conditions.append(col(col_name) < rule.min)
    if rule.max is not None:
        conditions.append(col(col_name) > rule.max)

    if conditions:
        combined = conditions[0]
        for c in conditions[1:]:
            combined = combined | c
        violation_count = out_of_range.filter(combined).count()
    else:
        violation_count = 0

    if violation_count > 0:
        range_desc = f"[{rule.min}, {rule.max}]"
        return RuleResult(
            rule_name="range",
            passed=False,
            message=f"{violation_count} values in '{col_name}' outside range {range_desc}",
            details={"column": col_name, "violations": violation_count},
        )

    range_desc = f"[{rule.min}, {rule.max}]"
    return RuleResult(
        rule_name="range",
        passed=True,
        message=f"All values in '{col_name}' within range {range_desc}",
    )


def _check_accepted_values(df: DataFrame, rule: QualityRule) -> RuleResult:
    """Check that a column only contains values from an allowed set."""
    col_name = rule.column
    allowed = rule.values or []
    if not col_name or col_name not in df.columns:
        return RuleResult(
            rule_name="accepted_values",
            passed=False,
            message=f"Column '{col_name}' not found in DataFrame",
        )

    distinct_values = {row[0] for row in df.select(col_name).distinct().collect()}
    invalid = distinct_values - set(allowed)

    if invalid:
        return RuleResult(
            rule_name="accepted_values",
            passed=False,
            message=f"Invalid values in '{col_name}': {sorted(str(v) for v in invalid)}. "
            f"Allowed: {allowed}",
            details={"invalid_values": sorted(str(v) for v in invalid)},
        )
    return RuleResult(
        rule_name="accepted_values",
        passed=True,
        message=f"All values in '{col_name}' are valid",
    )


def _check_custom_sql(
    spark: SparkSession, df: DataFrame, rule: QualityRule, table_name: str
) -> RuleResult:
    """Run a custom SQL query and fail if it returns any rows."""
    sql = rule.sql
    if not sql:
        return RuleResult(
            rule_name="custom_sql",
            passed=False,
            message="No SQL provided for custom_sql rule",
        )

    # Register DataFrame as a temp view
    view_name = f"_dq_{table_name.replace('.', '_')}"
    df.createOrReplaceTempView(view_name)

    # Replace {table} placeholder
    resolved_sql = sql.replace("{table}", view_name)
    result = spark.sql(resolved_sql)
    violation_count = result.count()

    if violation_count > 0:
        description = rule.description or "Custom SQL check failed"
        return RuleResult(
            rule_name="custom_sql",
            passed=False,
            message=f"{description}: {violation_count} violations found",
            details={"violations": violation_count, "sql": resolved_sql},
        )

    return RuleResult(
        rule_name="custom_sql",
        passed=True,
        message=rule.description or "Custom SQL check passed",
    )


def _check_freshness(df: DataFrame, rule: QualityRule) -> RuleResult:
    """Check that the most recent data is not stale."""
    col_name = rule.column or "ingestion_timestamp"
    max_age_hours = rule.value or 24

    if col_name not in df.columns:
        return RuleResult(
            rule_name="freshness",
            passed=False,
            message=f"Freshness column '{col_name}' not found",
        )

    from pyspark.sql.functions import current_timestamp, unix_timestamp

    latest = df.select(spark_max(col(col_name)).alias("latest")).collect()[0]["latest"]
    if latest is None:
        return RuleResult(
            rule_name="freshness",
            passed=False,
            message="No data found for freshness check",
        )

    # Calculate age in hours
    ts_diff = unix_timestamp(current_timestamp()) - unix_timestamp(spark_max(col(col_name)))
    age_df = df.select((ts_diff / 3600).alias("age_hours")).collect()
    age_hours = age_df[0]["age_hours"]

    if age_hours > max_age_hours:
        return RuleResult(
            rule_name="freshness",
            passed=False,
            message=f"Data is {age_hours:.1f} hours old (max: {max_age_hours}h)",
            details={"age_hours": age_hours, "max_age_hours": max_age_hours},
        )

    return RuleResult(
        rule_name="freshness",
        passed=True,
        message=f"Data is {age_hours:.1f} hours old (max: {max_age_hours}h)",
    )


# Rule dispatcher
_RULE_HANDLERS = {
    "not_null": _check_not_null,
    "unique": _check_unique,
    "min_rows": _check_min_rows,
    "range": _check_range,
    "accepted_values": _check_accepted_values,
    "freshness": _check_freshness,
}


class QualityEngine:
    """Execute declarative data quality rules against DataFrames."""

    def __init__(self, manifest: PipelineManifest) -> None:
        self._manifest = manifest

    @classmethod
    def from_manifest(cls, path: str | None = None) -> QualityEngine:
        return cls(load_manifest(path))

    def validate_table(
        self,
        spark: SparkSession,
        qualified_name: str,
        df: DataFrame,
        phase: str = "output",
    ) -> ValidationReport:
        """Validate a DataFrame against declared quality rules.

        Args:
            spark: SparkSession (needed for custom_sql rules).
            qualified_name: e.g., "bronze.feature_releases"
            df: The DataFrame to validate.
            phase: "input" or "output" - which rules to apply.

        Returns:
            ValidationReport with all rule results.
        """
        table = self._manifest.get_table(qualified_name)
        rules = table.input_quality_rules if phase == "input" else table.output_quality_rules

        row_count = df.count()
        results: list[RuleResult] = []

        for rule in rules:
            if rule.rule == "custom_sql":
                result = _check_custom_sql(spark, df, rule, qualified_name)
            elif rule.rule in _RULE_HANDLERS:
                result = _RULE_HANDLERS[rule.rule](df, rule)
            else:
                result = RuleResult(
                    rule_name=rule.rule,
                    passed=False,
                    message=f"Unknown rule type: {rule.rule}",
                )
            results.append(result)

        all_passed = all(r.passed for r in results)
        return ValidationReport(
            table_name=qualified_name,
            phase=phase,
            passed=all_passed,
            results=results,
            row_count=row_count,
        )

    def validate_all(
        self,
        spark: SparkSession,
        layer: str | None = None,
    ) -> list[ValidationReport]:
        """Validate all tables in a layer by reading their Delta tables.

        Args:
            spark: SparkSession.
            layer: Optional layer filter ("bronze", "silver", "gold").

        Returns:
            List of ValidationReports.
        """
        tables = self._manifest.get_layer(layer) if layer else list(self._manifest.tables.values())
        reports = []

        for table in tables:
            path = self._resolve_output_path(table)
            try:
                df = spark.read.format("delta").load(path)
                report = self.validate_table(spark, table.qualified_name, df, "output")
                reports.append(report)
            except Exception as e:
                reports.append(
                    ValidationReport(
                        table_name=table.qualified_name,
                        phase="output",
                        passed=False,
                        results=[
                            RuleResult(
                                rule_name="table_exists",
                                passed=False,
                                message=f"Could not read table: {e}",
                            )
                        ],
                        row_count=0,
                    )
                )

        return reports

    def _resolve_output_path(self, table: TableDefinition) -> str:
        """Resolve the output path for a table."""
        paths = self._manifest.paths
        layer_path = paths.get(table.layer, f"data/{table.layer}")
        if table.layer == "bronze":
            return f"{layer_path}/{table.name}"
        else:
            return f"{layer_path}/{table.layer}_{table.name}"
