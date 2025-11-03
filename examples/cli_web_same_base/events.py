from dataclasses import dataclass
from typing import Union


@dataclass
class RunStarted:
    workspace: str
    steps_count: int


@dataclass
class StepAnnounced:
    index: int
    prompt: str


@dataclass
class StepResult:
    index: int
    title: str
    body: str


@dataclass
class RunSummaryRequested:
    thread_id: str


@dataclass
class RunCompleted:
    success: bool = True


@dataclass
class RunFailed:
    error: str


Event = Union[
    RunStarted,
    StepAnnounced,
    StepResult,
    RunSummaryRequested,
    RunCompleted,
    RunFailed,
]
