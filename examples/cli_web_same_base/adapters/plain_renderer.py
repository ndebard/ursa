from events import (
    Event,
    RunCompleted,
    RunFailed,
    RunStarted,
    RunSummaryRequested,
    StepAnnounced,
    StepResult,
)

# Toggle: call the Rich summary function or skip it.
USE_RICH_SUMMARY = False


def handle_event(event: Event) -> None:
    if isinstance(event, RunStarted):
        # optional: announce run start
        print(f"Workspace: {event.workspace} — {event.steps_count} steps")

    elif isinstance(event, StepAnnounced):
        print(f"=== Solving Step {event.index} ===")
        print(event.prompt.strip())
        print()

    elif isinstance(event, StepResult):
        print(f"[{event.title}]")
        print(event.body)
        print("-" * 80)

    elif isinstance(event, RunSummaryRequested):
        if USE_RICH_SUMMARY:
            # keeps your existing summary behavior (may render with Rich)
            from ursa.observability.timing import render_session_summary

            render_session_summary(event.thread_id)
        else:
            # pure-print placeholder
            print("(summary omitted in plain renderer)")

    elif isinstance(event, RunCompleted):
        # optional: quiet on success
        pass

    elif isinstance(event, RunFailed):
        print(f"ERROR: {event.error}")
