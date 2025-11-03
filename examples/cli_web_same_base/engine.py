# cli_web_same_base/engine.py
from __future__ import annotations

from typing import Any, Callable, Iterable, Mapping, Protocol

from langchain_core.messages import HumanMessage

# ✅ Dual import: works when run as a package (preferred) and as a flat script.
try:
    from .events import (
        RunCompleted,
        RunFailed,
        RunStarted,
        RunSummaryRequested,
        StepAnnounced,
        StepResult,
    )
except Exception:
    from events import (
        RunCompleted,
        RunFailed,
        RunStarted,
        RunSummaryRequested,
        StepAnnounced,
        StepResult,
    )


class ExecAgent(Protocol):
    thread_id: str

    def invoke(self, payload: Mapping[str, Any]) -> dict: ...


def run_steps(
    steps: Iterable[str],
    workspace: str,
    executor: ExecAgent,
    emit: Callable[[object], None],
) -> None:
    steps = list(steps)
    emit(RunStarted(workspace=workspace, steps_count=len(steps)))
    try:
        for idx, step_prompt in enumerate(steps, start=1):
            emit(StepAnnounced(index=idx, prompt=step_prompt))
            result = executor.invoke({
                "messages": [HumanMessage(content=step_prompt)],
                "workspace": workspace,
            })
            final_content = result["messages"][-1].content
            emit(
                StepResult(
                    index=idx,
                    title=f"Step {idx} Final Response",
                    body=final_content,
                )
            )
        emit(RunSummaryRequested(thread_id=executor.thread_id))
        emit(RunCompleted(success=True))
    except Exception as exc:
        emit(RunFailed(error=str(exc)))
        raise
