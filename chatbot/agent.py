"""The analytics agent: Claude + tools over the semantic layer.

UI-agnostic on purpose — the Streamlit app and the CLI both drive this class.
The agent loop is manual (rather than the SDK tool runner) so every SQL
statement and result can be surfaced to the user for transparency.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, cast

import anthropic
from anthropic.types import MessageParam, ToolParam

from semantic.catalog import Catalog
from semantic.engine import LakehouseEngine, QueryRejectedError
from semantic.metrics import SemanticError, SemanticLayer

MODEL = "claude-opus-4-8"
MAX_TOOL_TURNS = 8
MAX_RESULT_ROWS = 50

TOOLS: list[ToolParam] = [
    {
        "name": "query_metric",
        "description": (
            "Run a governed metric query through the semantic layer. Prefer this over "
            "run_sql whenever the question maps to a defined metric: definitions are "
            "vetted, and ratio metrics re-aggregate correctly. Metrics in one call "
            "must live on the same table."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "metrics": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Metric names from the metric catalog (same table)",
                },
                "dimensions": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Grouping columns; must be allowed for the metrics",
                },
                "filters": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "column": {"type": "string"},
                            "op": {
                                "type": "string",
                                "enum": ["=", "!=", ">", ">=", "<", "<=", "in", "not in", "like"],
                            },
                            "value": {
                                "type": "string",
                                "description": (
                                    "Filter value. For in/not in, a JSON array encoded "
                                    'as a string, e.g. \'["referral", "partner"]\''
                                ),
                            },
                        },
                        "required": ["column", "op", "value"],
                        "additionalProperties": False,
                    },
                    "description": "Optional filters on allowed dimensions",
                },
                "order_by": {
                    "type": "string",
                    "description": "Column to order by, optionally with ' desc'",
                },
                "limit": {"type": "integer", "description": "Max rows to return"},
            },
            "required": ["metrics"],
            "additionalProperties": False,
        },
    },
    {
        "name": "run_sql",
        "description": (
            "Run a single read-only SQL SELECT against the lakehouse (DuckDB syntax). "
            "Use for detail questions the metric catalog doesn't cover: top-N lists, "
            "point-in-time subscription/feature states, custom joins. Table and column "
            "names are documented in the system prompt. Write statements are rejected."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "One SELECT/WITH statement"},
            },
            "required": ["sql"],
            "additionalProperties": False,
        },
    },
]


@dataclass
class ToolCall:
    """One executed tool call, kept for transparent display in the UI."""

    name: str
    input: dict
    result: str
    is_error: bool = False


@dataclass
class AgentReply:
    text: str
    tool_calls: list[ToolCall] = field(default_factory=list)


def _render_rows(columns: list[str], rows: list[tuple]) -> str:
    """Rows as a compact JSON payload for the model."""
    payload = {
        "columns": columns,
        "rows": [[None if v is None else str(v) for v in row] for row in rows[:MAX_RESULT_ROWS]],
    }
    if len(rows) > MAX_RESULT_ROWS:
        payload["truncated_to"] = MAX_RESULT_ROWS
    return json.dumps(payload)


def build_system_prompt(catalog: Catalog, layer: SemanticLayer) -> str:
    """Ground the model in the contracts and the metric catalog."""
    table_docs = []
    for name in catalog.tables:
        table_docs.append(catalog.describe(name))
    return f"""You are the analytics assistant for a SaaS product-led-growth company.
You answer business questions by querying the company's lakehouse. The data is
synthetic demo data covering calendar year 2024; MRR is in USD.

## How to work
- Prefer the query_metric tool: it uses governed, correctly-aggregated metric
  definitions. Fall back to run_sql (DuckDB syntax, read-only) for detail
  questions, top-N lists, or joins the metrics don't cover.
- If a query fails, read the error and correct your next call.
- Answer the business question directly, with the key numbers formatted for
  reading (e.g. $1.2M, 34.5%). Mention which table/metric the answer came from.
- State assumptions when a question is ambiguous, and say so when the data
  cannot answer the question. Never invent numbers.
- Dates: 'month' columns are month-start dates; 'week_start' is a Monday.
- In silver_subscription_periods and silver_feature_states, an end date of
  9999-12-31 means "still current".

## Metric catalog (query_metric)
{layer.describe_for_llm()}

## Tables (run_sql)
{chr(10).join(table_docs)}
"""


class AnalyticsAgent:
    """Claude-driven analytics over the lakehouse semantic layer."""

    def __init__(self, client: anthropic.Anthropic | None = None):
        self.client = client or anthropic.Anthropic()
        self.engine = LakehouseEngine()
        self.catalog = self.engine.catalog
        self.layer = SemanticLayer(engine=self.engine)
        self.system_prompt = build_system_prompt(self.catalog, self.layer)
        self.messages: list[MessageParam] = []

    # ── Tool execution ─────────────────────────────────────────────────────
    def _execute_tool(self, name: str, tool_input: dict) -> tuple[str, bool]:
        """Run one tool call; returns (result_text, is_error)."""
        try:
            if name == "query_metric":
                filters = []
                for spec in tool_input.get("filters") or []:
                    value = spec["value"]
                    if spec["op"] in ("in", "not in"):
                        value = json.loads(value)
                    filters.append({"column": spec["column"], "op": spec["op"], "value": value})
                columns, rows = self.layer.query(
                    tool_input["metrics"],
                    dimensions=tool_input.get("dimensions") or [],
                    filters=filters,
                    order_by=tool_input.get("order_by"),
                    limit=tool_input.get("limit"),
                )
                return _render_rows(columns, rows), False
            if name == "run_sql":
                columns, rows = self.engine.sql_rows(tool_input["sql"])
                return _render_rows(columns, rows), False
            return f"Unknown tool: {name}", True
        except (SemanticError, QueryRejectedError) as e:
            return f"{type(e).__name__}: {e}", True
        except Exception as e:  # duckdb errors carry useful SQL hints
            return f"{type(e).__name__}: {e}", True

    # ── Agent loop ─────────────────────────────────────────────────────────
    def ask(self, question: str) -> AgentReply:
        """Answer one user question, running tools until Claude is done."""
        self.messages.append({"role": "user", "content": question})
        executed: list[ToolCall] = []

        for _ in range(MAX_TOOL_TURNS):
            response = self.client.messages.create(
                model=MODEL,
                max_tokens=4096,
                thinking={"type": "adaptive"},
                system=[
                    {
                        "type": "text",
                        "text": self.system_prompt,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                tools=TOOLS,
                messages=self.messages,
            )  # type: ignore[union-attr]

            self.messages.append({"role": "assistant", "content": response.content})

            if response.stop_reason != "tool_use":
                text = "".join(b.text for b in response.content if b.type == "text")
                return AgentReply(text=text, tool_calls=executed)

            tool_results: list[Any] = []
            for block in response.content:
                if block.type != "tool_use":
                    continue
                result, is_error = self._execute_tool(block.name, block.input)
                executed.append(
                    ToolCall(name=block.name, input=block.input, result=result, is_error=is_error)
                )
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result,
                        "is_error": is_error,
                    }
                )
            self.messages.append(cast(MessageParam, {"role": "user", "content": tool_results}))

        return AgentReply(
            text="I hit the tool-call limit before finishing — try a narrower question.",
            tool_calls=executed,
        )

    def reset(self) -> None:
        self.messages = []
