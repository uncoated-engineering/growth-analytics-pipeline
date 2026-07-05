"""Tests for the metric store and semantic SQL compiler (no engine needed)."""

import pytest

from semantic.metrics import MetricStore, SemanticError, compile_metric_query


@pytest.fixture(scope="module")
def store():
    return MetricStore()


class TestMetricStore:
    """Test suite for metric definitions."""

    def test_metrics_load(self, store):
        assert "ending_mrr" in store.metrics
        assert "conversion_rate" in store.metrics
        assert "net_revenue_retention" in store.metrics

    def test_unknown_metric_raises(self, store):
        with pytest.raises(SemanticError, match="Unknown metric"):
            store.get("revenue_per_unicorn")

    def test_every_metric_is_documented(self, store):
        for metric in store.metrics.values():
            assert metric.label
            assert metric.description
            assert metric.table
            assert metric.expression


class TestCompileMetricQuery:
    """Test suite for the semantic SQL compiler."""

    def test_simple_metric(self, store):
        compiled = compile_metric_query(store, ["signups"])
        assert compiled.sql == "SELECT SUM(signups) AS signups\nFROM gold_channel_performance"

    def test_metric_with_dimension_groups_and_orders(self, store):
        compiled = compile_metric_query(store, ["signups"], dimensions=["acquisition_channel"])
        assert "GROUP BY acquisition_channel" in compiled.sql
        assert "ORDER BY acquisition_channel" in compiled.sql

    def test_multiple_metrics_same_table(self, store):
        compiled = compile_metric_query(
            store, ["signups", "conversion_rate"], dimensions=["signup_month"]
        )
        assert "AS signups" in compiled.sql
        assert "AS conversion_rate" in compiled.sql

    def test_metrics_across_tables_rejected(self, store):
        with pytest.raises(SemanticError, match="multiple tables"):
            compile_metric_query(store, ["signups", "ending_mrr"])

    def test_ratio_metric_is_ratio_of_sums(self, store):
        compiled = compile_metric_query(store, ["conversion_rate"])
        assert "SUM(conversions) * 1.0 / NULLIF(SUM(signups), 0)" in compiled.sql

    def test_invalid_dimension_rejected(self, store):
        with pytest.raises(SemanticError, match="not allowed"):
            compile_metric_query(store, ["ending_mrr"], dimensions=["acquisition_channel"])

    def test_filter_rendering(self, store):
        compiled = compile_metric_query(
            store,
            ["signups"],
            filters=[{"column": "acquisition_channel", "op": "=", "value": "referral"}],
        )
        assert "WHERE acquisition_channel = 'referral'" in compiled.sql

    def test_in_filter_rendering(self, store):
        compiled = compile_metric_query(
            store,
            ["signups"],
            filters=[
                {"column": "acquisition_channel", "op": "in", "value": ["referral", "partner"]}
            ],
        )
        assert "WHERE acquisition_channel IN ('referral', 'partner')" in compiled.sql

    def test_filter_value_is_escaped(self, store):
        compiled = compile_metric_query(
            store,
            ["signups"],
            filters=[{"column": "acquisition_channel", "op": "=", "value": "x'; DROP TABLE y"}],
        )
        assert "''" in compiled.sql  # single quote doubled
        assert "DROP TABLE y" in compiled.sql  # inert inside the literal

    def test_filter_on_unknown_column_rejected(self, store):
        with pytest.raises(SemanticError, match="not an allowed dimension"):
            compile_metric_query(
                store, ["signups"], filters=[{"column": "mrr", "op": "=", "value": 1}]
            )

    def test_disallowed_operator_rejected(self, store):
        with pytest.raises(SemanticError, match="not allowed"):
            compile_metric_query(
                store,
                ["signups"],
                filters=[{"column": "acquisition_channel", "op": ";", "value": "x"}],
            )

    def test_order_by_validated(self, store):
        with pytest.raises(SemanticError, match="order_by"):
            compile_metric_query(store, ["signups"], order_by="mrr desc")

    def test_limit_applied(self, store):
        compiled = compile_metric_query(store, ["signups"], limit=5)
        assert compiled.sql.endswith("LIMIT 5")
