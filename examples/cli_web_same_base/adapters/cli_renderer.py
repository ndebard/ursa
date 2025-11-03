# adapters/cli_renderer.py
"""
CLI adapter that renders events with Rich, matching your current look.
"""

from events import (
    Event,
    RunCompleted,
    RunFailed,
    RunStarted,
    RunSummaryRequested,
    StepAnnounced,
    StepResult,
)
from rich import get_console
from rich.panel import Panel

# Imported here (adapter-level) to keep the engine UI-agnostic
from ursa.observability.timing import render_session_summary

_console = get_console()  # same singleton instance Rich uses


def handle_event(event: Event) -> None:
    if isinstance(event, RunStarted):
        # No visible output originally; keep quiet (or log if you want)
        return

    if isinstance(event, StepAnnounced):
        _console.print(
            f"[bold orange3]Solving Step {event.index}:[/]\n"
            f"[orange3]{event.prompt}[/]"
        )
        return

    if isinstance(event, StepResult):
        _console.print(
            Panel(
                event.body,
                title=event.title,
                border_style="orange3",
            )
        )
        return

    if isinstance(event, RunSummaryRequested):
        # Preserve your exact summary behavior
        render_session_summary(event.thread_id)
        return

    if isinstance(event, RunCompleted):
        # No extra output needed for success
        return

    if isinstance(event, RunFailed):
        _console.print(
            Panel(str(event.error), title="Error", border_style="red")
        )
        return
