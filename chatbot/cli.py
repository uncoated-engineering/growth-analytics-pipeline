"""Terminal interface for the analytics agent.

uv run python -m chatbot.cli "Which channel converts best?"
uv run python -m chatbot.cli            # interactive REPL
"""

from __future__ import annotations

import sys

from chatbot.agent import AnalyticsAgent


def _ask(agent: AnalyticsAgent, question: str):
    try:
        return agent.ask(question)
    except TypeError:
        print(
            "No Anthropic credentials found. Set ANTHROPIC_API_KEY (or run "
            "`ant auth login`) and try again.",
            file=sys.stderr,
        )
        raise SystemExit(1) from None


def _print_reply(reply) -> None:
    for call in reply.tool_calls:
        status = "error" if call.is_error else "ok"
        if call.name == "run_sql":
            print(f"  [sql:{status}] {call.input.get('sql', '')}")
        else:
            print(f"  [metric:{status}] {call.input}")
    print()
    print(reply.text)


def main() -> int:
    agent = AnalyticsAgent()

    if len(sys.argv) > 1:
        question = " ".join(sys.argv[1:])
        _print_reply(_ask(agent, question))
        return 0

    print("Growth analytics chat — ask about MRR, churn, channels, features. Ctrl-D to exit.")
    while True:
        try:
            question = input("\nyou> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return 0
        if not question:
            continue
        _print_reply(_ask(agent, question))


if __name__ == "__main__":
    sys.exit(main())
