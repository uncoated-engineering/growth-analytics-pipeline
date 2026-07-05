"""Tests for the analytics agent's tool layer (no Claude API calls).

The agent's grounding and tool execution are deterministic and testable
without a model: build the system prompt, dispatch tool calls, verify
errors surface as recoverable tool results rather than exceptions.
"""

import json

import pytest

from chatbot.agent import TOOLS, AnalyticsAgent, build_system_prompt


@pytest.fixture(scope="module")
def agent():
    """Agent over the real lakehouse if present, else skip."""
    dummy_client = object()  # never used: we only exercise the tool layer
    instance = AnalyticsAgent(client=dummy_client)
    if instance.engine.missing_tables:
        pytest.skip(f"lakehouse tables missing: {instance.engine.missing_tables}")
    return instance


class TestToolDefinitions:
    """Test suite for the tool schemas handed to Claude."""

    def test_two_tools_defined(self):
        assert [t["name"] for t in TOOLS] == ["query_metric", "run_sql"]

    def test_schemas_are_closed(self):
        for tool in TOOLS:
            assert tool["input_schema"]["additionalProperties"] is False
            assert tool["input_schema"]["required"]


class TestSystemPrompt:
    """Test suite for the grounding prompt."""

    def test_contains_all_tables_and_metrics(self, agent):
        prompt = build_system_prompt(agent.catalog, agent.layer)
        for table_name in agent.catalog.tables:
            assert table_name in prompt
        for metric_name in agent.layer.store.metrics:
            assert metric_name in prompt

    def test_mentions_synthetic_data(self, agent):
        assert "synthetic" in agent.system_prompt


class TestToolExecution:
    """Test suite for tool dispatch against the lakehouse."""

    def test_query_metric(self, agent):
        result, is_error = agent._execute_tool(
            "query_metric",
            {"metrics": ["conversion_rate"], "dimensions": ["acquisition_channel"]},
        )
        assert not is_error
        payload = json.loads(result)
        assert payload["columns"] == ["acquisition_channel", "conversion_rate"]
        assert len(payload["rows"]) == 8  # 7 channels + unattributed

    def test_query_metric_with_in_filter(self, agent):
        result, is_error = agent._execute_tool(
            "query_metric",
            {
                "metrics": ["signups"],
                "filters": [
                    {
                        "column": "acquisition_channel",
                        "op": "in",
                        "value": '["referral", "partner"]',
                    }
                ],
            },
        )
        assert not is_error
        payload = json.loads(result)
        assert int(payload["rows"][0][0]) > 0

    def test_run_sql(self, agent):
        result, is_error = agent._execute_tool(
            "run_sql", {"sql": "SELECT COUNT(*) AS n FROM silver_user_dim"}
        )
        assert not is_error
        assert json.loads(result)["columns"] == ["n"]

    def test_bad_metric_returns_recoverable_error(self, agent):
        result, is_error = agent._execute_tool("query_metric", {"metrics": ["revenue_per_unicorn"]})
        assert is_error
        assert "Unknown metric" in result

    def test_write_sql_rejected(self, agent):
        result, is_error = agent._execute_tool("run_sql", {"sql": "DROP TABLE silver_user_dim"})
        assert is_error
        assert "QueryRejectedError" in result

    def test_sql_error_carries_duckdb_hint(self, agent):
        result, is_error = agent._execute_tool(
            "run_sql", {"sql": "SELECT nonexistent_column FROM silver_user_dim"}
        )
        assert is_error
        assert "nonexistent_column" in result

    def test_unknown_tool(self, agent):
        result, is_error = agent._execute_tool("send_email", {})
        assert is_error
