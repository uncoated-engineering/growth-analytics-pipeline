"""Contract tests: YAML data contracts must match the Spark schemas.

If a job's StructType changes without the contract (and therefore the data
dictionary, lineage doc, and chatbot grounding) being updated, these tests
fail. This is what makes the contracts trustworthy documentation.
"""

import pytest
from pyspark.sql.types import StructType

from semantic.catalog import Catalog
from spark.jobs.bronze.conversions.schema import CONVERSIONS_OUTPUT_SCHEMA
from spark.jobs.bronze.feature_releases.schema import FEATURE_RELEASES_OUTPUT_SCHEMA
from spark.jobs.bronze.feature_usage_events.schema import FEATURE_USAGE_EVENTS_OUTPUT_SCHEMA
from spark.jobs.bronze.marketing_attribution.schema import MARKETING_ATTRIBUTION_OUTPUT_SCHEMA
from spark.jobs.bronze.subscription_events.schema import SUBSCRIPTION_EVENTS_OUTPUT_SCHEMA
from spark.jobs.bronze.user_signups.schema import USER_SIGNUPS_OUTPUT_SCHEMA
from spark.jobs.gold.channel_performance.schema import CHANNEL_PERFORMANCE_SCHEMA
from spark.jobs.gold.feature_conversion_impact.schema import FEATURE_CONVERSION_IMPACT_SCHEMA
from spark.jobs.gold.mrr_waterfall.schema import MRR_WATERFALL_SCHEMA
from spark.jobs.gold.weekly_engagement.schema import WEEKLY_ENGAGEMENT_SCHEMA
from spark.jobs.silver.feature_states.schema import FEATURE_STATES_SCHEMA
from spark.jobs.silver.feature_usage_facts.schema import FEATURE_USAGE_FACTS_SCHEMA
from spark.jobs.silver.subscription_periods.schema import SUBSCRIPTION_PERIODS_SCHEMA
from spark.jobs.silver.user_dim.schema import USER_DIM_SCHEMA

TABLE_SCHEMAS: dict[str, StructType] = {
    "feature_releases": FEATURE_RELEASES_OUTPUT_SCHEMA,
    "user_signups": USER_SIGNUPS_OUTPUT_SCHEMA,
    "feature_usage_events": FEATURE_USAGE_EVENTS_OUTPUT_SCHEMA,
    "conversions": CONVERSIONS_OUTPUT_SCHEMA,
    "marketing_attribution": MARKETING_ATTRIBUTION_OUTPUT_SCHEMA,
    "subscription_events": SUBSCRIPTION_EVENTS_OUTPUT_SCHEMA,
    "silver_feature_states": FEATURE_STATES_SCHEMA,
    "silver_user_dim": USER_DIM_SCHEMA,
    "silver_feature_usage_facts": FEATURE_USAGE_FACTS_SCHEMA,
    "silver_subscription_periods": SUBSCRIPTION_PERIODS_SCHEMA,
    "gold_feature_conversion_impact": FEATURE_CONVERSION_IMPACT_SCHEMA,
    "gold_mrr_waterfall": MRR_WATERFALL_SCHEMA,
    "gold_channel_performance": CHANNEL_PERFORMANCE_SCHEMA,
    "gold_weekly_engagement": WEEKLY_ENGAGEMENT_SCHEMA,
}


class TestContractsMatchSchemas:
    """Every contract mirrors the authoritative StructType of its job."""

    def test_every_table_has_a_schema_mapping(self):
        """The mapping above covers exactly the cataloged tables."""
        catalog = Catalog()
        assert set(TABLE_SCHEMAS) == set(catalog.tables)

    @pytest.mark.parametrize("table_name", sorted(TABLE_SCHEMAS))
    def test_contract_columns_match_schema(self, table_name):
        """Contract column names and order match the StructType."""
        catalog = Catalog()
        contract_cols = catalog.tables[table_name].column_names
        schema_cols = [f.name for f in TABLE_SCHEMAS[table_name].fields]
        assert contract_cols == schema_cols

    @pytest.mark.parametrize("table_name", sorted(TABLE_SCHEMAS))
    def test_contract_types_match_schema(self, table_name):
        """Contract column types match the StructType type names."""
        catalog = Catalog()
        contract_types = {c.name: c.type for c in catalog.tables[table_name].columns}
        for field in TABLE_SCHEMAS[table_name].fields:
            assert contract_types[field.name] == field.dataType.typeName(), (
                f"{table_name}.{field.name}: contract says "
                f"'{contract_types[field.name]}', schema says "
                f"'{field.dataType.typeName()}'"
            )
