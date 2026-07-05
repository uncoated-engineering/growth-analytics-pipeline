"""Streamlit chat UI for the analytics agent.

Run with: make chatbot   (or: uv run streamlit run chatbot/app.py)

Every tool call the agent makes (semantic metric queries and SQL) is shown
inline for transparency — a business user sees the answer, an analyst can
audit exactly how it was computed.
"""

import json
import os

import anthropic
import streamlit as st

from chatbot.agent import AnalyticsAgent

st.set_page_config(page_title="Growth Analytics Chat", page_icon="📈", layout="wide")

SUGGESTIONS = [
    "How did MRR develop over 2024, and what drove the growth?",
    "Which acquisition channel brings the best customers?",
    "Does feature adoption actually drive conversion?",
    "How many paying customers do we have, and how many have churned?",
    "What are our most and least used features?",
]


@st.cache_resource
def get_agent() -> AnalyticsAgent:
    return AnalyticsAgent()


def render_tool_call(call) -> None:
    label = "🧮 metric query" if call.name == "query_metric" else "🗄️ SQL query"
    with st.expander(f"{label} — {'❌ failed' if call.is_error else 'ok'}", expanded=False):
        if call.name == "run_sql":
            st.code(call.input.get("sql", ""), language="sql")
        else:
            st.code(json.dumps(call.input, indent=2), language="json")
        if call.is_error:
            st.error(call.result)
        else:
            try:
                payload = json.loads(call.result)
                st.dataframe(
                    [dict(zip(payload["columns"], row, strict=False)) for row in payload["rows"]],
                    use_container_width=True,
                )
            except Exception:
                st.text(call.result)


def main() -> None:
    st.title("📈 Growth Analytics Chat")
    st.caption(
        "Ask business questions in plain language. Claude answers by querying the "
        "lakehouse — through governed semantic-layer metrics when possible, guarded "
        "read-only SQL otherwise. Every query it runs is shown for auditability."
    )

    if not (os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("ANTHROPIC_AUTH_TOKEN")):
        st.warning(
            "No Anthropic credentials found. Set `ANTHROPIC_API_KEY` (or log in with "
            "`ant auth login`) and restart. You can still browse the data model in "
            "the sidebar."
        )

    try:
        agent = get_agent()
    except Exception as e:
        st.error(f"Could not initialize the lakehouse engine: {e}")
        st.info("Run `make generate-data && make pipeline` first to build the lakehouse.")
        return

    with st.sidebar:
        st.header("Data model")
        if agent.engine.missing_tables:
            st.warning(
                "Missing tables (run `make pipeline`): " + ", ".join(agent.engine.missing_tables)
            )
        st.subheader("Metrics")
        for metric in agent.layer.store.metrics.values():
            with st.expander(metric.label):
                st.write(metric.description)
                st.caption(f"table: `{metric.table}` — dims: {', '.join(metric.dimensions)}")
        st.subheader("Tables")
        for table in agent.catalog.tables.values():
            with st.expander(f"{table.table} ({table.layer})"):
                st.write(table.description)
                st.caption(f"Grain: {table.grain}")
        if st.button("Clear conversation"):
            agent.reset()
            st.session_state.history = []
            st.rerun()

    if "history" not in st.session_state:
        st.session_state.history = []

    for entry in st.session_state.history:
        with st.chat_message(entry["role"]):
            st.markdown(entry["text"])
            for call in entry.get("tool_calls", []):
                render_tool_call(call)

    if not st.session_state.history:
        st.markdown("**Try one of these:**")
        columns = st.columns(len(SUGGESTIONS))
        for column, suggestion in zip(columns, SUGGESTIONS, strict=False):
            if column.button(suggestion, use_container_width=True):
                st.session_state.pending = suggestion
                st.rerun()

    question = st.chat_input("Ask about MRR, churn, channels, feature adoption…")
    if question is None:
        question = st.session_state.pop("pending", None)

    if question:
        st.session_state.history.append({"role": "user", "text": question})
        with st.chat_message("user"):
            st.markdown(question)
        with st.chat_message("assistant"):
            with st.spinner("Querying the lakehouse…"):
                try:
                    reply = agent.ask(question)
                except anthropic.AuthenticationError:
                    st.error("Invalid or missing Anthropic API key.")
                    return
                except anthropic.APIStatusError as e:
                    st.error(f"Claude API error ({e.status_code}): {e.message}")
                    return
            st.markdown(reply.text)
            for call in reply.tool_calls:
                render_tool_call(call)
        st.session_state.history.append(
            {"role": "assistant", "text": reply.text, "tool_calls": reply.tool_calls}
        )


main()
