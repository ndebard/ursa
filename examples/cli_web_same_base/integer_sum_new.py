# main_cli.py
"""
CLI entrypoint that wires the engine to the Rich renderer and your agent.
Replicates the original behavior/output, just via events.
"""

import sqlite3
from pathlib import Path

# from adapters.cli_renderer import handle_event      # Rich CLI renderer
from adapters.plain_renderer import handle_event  # Plain print
from engine import run_steps
from langchain_litellm import ChatLiteLLM
from langgraph.checkpoint.sqlite import SqliteSaver

from ursa.agents import ExecutionAgent

# ---- Original "problem" prompts unchanged ----
problem = [
    """\
Create a python function that finds the sum of the first N positive integers with a for loop.
Time how long it takes to sum the first 10,000 and print the results to the console.
""",
    """\
Add a new function that computes the same value using the built-in sum function, no loops.
Compare the timing for these two methods on the first 100,000 integers, and check the results match.
""",
    """\
Add a third function that uses a static formula the compute the same value.
Compare the timing for all three methods on the first million integers, and check the results match.
""",
]


def build_executor(workspace: str) -> ExecutionAgent:
    model = ChatLiteLLM(model="openai/o3")

    db_path = Path(workspace) / "checkpoint.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    checkpointer = SqliteSaver(conn)

    return ExecutionAgent(
        llm=model,
        checkpointer=checkpointer,
        enable_metrics=False,
    )


def main() -> None:
    workspace = "example_integer_sum"
    executor = build_executor(workspace)
    run_steps(
        steps=problem,
        workspace=workspace,
        executor=executor,
        emit=handle_event,  # CLI adapter renders each event
    )


if __name__ == "__main__":
    main()
