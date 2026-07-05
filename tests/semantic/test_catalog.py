"""Tests for the data contract catalog."""

from semantic.catalog import Catalog


class TestCatalog:
    """Test suite for contract loading and lineage."""

    def test_all_tables_loaded(self):
        """All 14 lakehouse tables have a contract."""
        catalog = Catalog()
        assert len(catalog.tables) == 14
        assert len(catalog.by_layer("bronze")) == 6
        assert len(catalog.by_layer("silver")) == 4
        assert len(catalog.by_layer("gold")) == 4

    def test_every_contract_is_complete(self):
        """Every contract has grain, description, owner, and typed columns."""
        catalog = Catalog()
        for table in catalog.tables.values():
            assert table.grain, f"{table.table} missing grain"
            assert table.description, f"{table.table} missing description"
            assert table.owner, f"{table.table} missing owner"
            assert table.columns, f"{table.table} has no columns"
            for column in table.columns:
                assert column.description, f"{table.table}.{column.name} missing description"

    def test_bronze_tables_have_sources_not_upstreams(self):
        """Bronze contracts point at raw files, not other tables."""
        catalog = Catalog()
        for table in catalog.by_layer("bronze"):
            assert table.source, f"{table.table} missing source"
            assert table.upstreams == [], f"{table.table} should have no upstreams"

    def test_silver_and_gold_upstreams_exist(self):
        """Every declared upstream is itself a cataloged table."""
        catalog = Catalog()
        for layer in ("silver", "gold"):
            for table in catalog.by_layer(layer):
                assert table.upstreams, f"{table.table} must declare upstreams"
                for upstream in table.upstreams:
                    assert (
                        upstream in catalog.tables
                    ), f"{table.table} declares unknown upstream '{upstream}'"

    def test_lineage_edges(self):
        """Known edges appear in the lineage graph."""
        edges = Catalog().lineage_edges()
        assert ("subscription_events", "silver_subscription_periods") in edges
        assert ("subscription_events", "gold_mrr_waterfall") in edges
        assert ("silver_user_dim", "gold_channel_performance") in edges
        assert ("feature_releases", "silver_feature_states") in edges

    def test_describe_is_llm_friendly(self):
        """describe() contains grain, description, and every column."""
        catalog = Catalog()
        text = catalog.describe("silver_user_dim")
        assert "Grain:" in text
        assert "acquisition_channel" in text
        assert "current_plan" in text
