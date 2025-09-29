import asyncio
import time
import uuid
from datetime import datetime, timezone
from functools import wraps
from types import MethodType
from typing import Annotated, Dict, List

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.load import dumps
from langchain_core.tools import InjectedToolCallId
from langchain_litellm import ChatLiteLLM
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.prebuilt import InjectedState

_METRICS_KEY = "__metrics__"  # where timing gets stored in the state updates


try:
    from langgraph.types import Command as LGCommand
except Exception:
    LGCommand = None


def ensure_annotation_globals(obj):
    # If it's a function, make sure its globals have the names get_type_hints needs.
    g = getattr(obj, "__globals__", None)
    if isinstance(g, dict):
        g.setdefault("Annotated", Annotated)
        g.setdefault("InjectedState", InjectedState)
        g.setdefault("InjectedToolCallId", InjectedToolCallId)
    return obj


def timed_node(fn):
    is_async = asyncio.iscoroutinefunction(fn)

    def _merge_updates(into: dict, add: dict) -> dict:
        for k, v in (add or {}).items():
            if k == "__metrics__":
                continue
            if k in into and isinstance(into[k], list) and isinstance(v, list):
                into[k].extend(v)
            elif (
                k in into and isinstance(into[k], dict) and isinstance(v, dict)
            ):
                into[k].update(v)
            else:
                into[k] = v
        return into

    def _finish_with(self, ret, node_name: str):
        # Command
        if LGCommand is not None and isinstance(ret, LGCommand):
            upd = dict(getattr(ret, "update", {}) or {})
            upd.pop("__metrics__", None)
            upd = self._finish_timer(upd, ok=True, extra={"node": node_name})
            kwargs = {"update": upd}
            for attr in ("goto", "graph", "interrupt", "sleep"):
                if hasattr(ret, attr):
                    val = getattr(ret, attr)
                    if val is not None:
                        kwargs[attr] = val
            return LGCommand(**kwargs)

        # list (may contain Commands + messages)
        if isinstance(ret, list):
            msgs, merged, has_cmd = [], {}, False
            for item in ret:
                if LGCommand is not None and isinstance(item, LGCommand):
                    has_cmd = True
                    upd = dict(getattr(item, "update", {}) or {})
                    if "messages" in upd:
                        msgs.extend(upd["messages"])
                        upd = {k: v for k, v in upd.items() if k != "messages"}
                    _merge_updates(merged, upd)
                else:
                    msgs.append(item)
            if has_cmd:
                merged["messages"] = msgs
                merged.pop("__metrics__", None)
                merged = self._finish_timer(
                    merged, ok=True, extra={"node": node_name}
                )
                return LGCommand(update=merged) if LGCommand else merged
            else:
                updates = {"messages": msgs}
                updates.pop("__metrics__", None)
                return self._finish_timer(
                    updates, ok=True, extra={"node": node_name}
                )

        # dict
        if isinstance(ret, dict):
            ret.pop("__metrics__", None)
            return self._finish_timer(ret, ok=True, extra={"node": node_name})

        # anything else
        self._finish_timer({}, ok=True, extra={"node": node_name})
        return ret

    if is_async:

        @wraps(fn)
        async def wrapper(self, *args, **kwargs):
            self._start_timer()
            try:
                ret = await fn(self, *args, **kwargs)
            except Exception as e:
                self._finish_timer(
                    {}, ok=False, err=e, extra={"node": fn.__name__}
                )
                raise
            return _finish_with(self, ret, fn.__name__)

        return wrapper
    else:

        @wraps(fn)
        def wrapper(self, *args, **kwargs):
            self._start_timer()
            try:
                ret = fn(self, *args, **kwargs)
            except Exception as e:
                self._finish_timer(
                    {}, ok=False, err=e, extra={"node": fn.__name__}
                )
                raise
            return _finish_with(self, ret, fn.__name__)

        return wrapper


TOOL_TIMES = []  # list of (name, ms, ok)


def timed_tool(name: str):
    def deco(fn):
        @wraps(fn)
        def wrapper(*a, **kw):
            t0 = time.perf_counter()
            ok = True
            try:
                return fn(*a, **kw)
            except Exception:
                ok = False
                raise
            finally:
                TOOL_TIMES.append((
                    name,
                    (time.perf_counter() - t0) * 1000.0,
                    ok,
                ))

        return wrapper

    return deco


def print_tool_timings():  # quick summary printer
    from collections import defaultdict

    buckets = defaultdict(list)
    for name, ms, _ok in TOOL_TIMES:
        buckets[name].append(ms)
    rows = []
    for name, arr in buckets.items():
        rows.append((
            name,
            len(arr),
            sum(arr) / 1000.0,
            sum(arr) / len(arr),
            max(arr),
        ))
    rows.sort(key=lambda r: r[2], reverse=True)
    print("\nPer-Tool Timing")
    print(
        f"{'Tool':18} {'Count':>5} {'Total(s)':>9} {'Avg(ms)':>9} {'Max(ms)':>9}"
    )
    print("-" * 52)
    for name, cnt, tot_s, avg_ms, max_ms in rows:
        print(f"{name:18} {cnt:5d} {tot_s:9.2f} {avg_ms:9.0f} {max_ms:9.0f}")


def merge_metrics(curr: dict | None, delta: dict | None) -> dict:
    out: Dict[str, List[dict]] = {k: list(v) for k, v in (curr or {}).items()}
    for agent, runs in (delta or {}).items():
        dest = out.setdefault(agent, [])
        seen = {
            (r.get("run_id"), r.get("started_at"), r.get("duration_ms"))
            for r in dest
        }
        for r in runs or []:
            key = (r.get("run_id"), r.get("started_at"), r.get("duration_ms"))
            if key not in seen:
                dest.append(r)
                seen.add(key)
    return out


class BaseAgent:
    # llm: BaseChatModel
    # llm_with_tools: Runnable[LanguageModelInput, BaseMessage]

    def __init__(
        self,
        llm: str | BaseChatModel,
        checkpointer: BaseCheckpointSaver = None,
        **kwargs,
    ):
        match llm:
            case BaseChatModel():
                self.llm = llm

            case str():
                self.llm_provider, self.llm_model = llm.split("/")
                self.llm = ChatLiteLLM(
                    model=llm,
                    max_tokens=kwargs.pop("max_tokens", 10000),
                    max_retries=kwargs.pop("max_retries", 2),
                    **kwargs,
                )

            case _:
                raise TypeError(
                    "llm argument must be a string with the provider and model, or a BaseChatModel instance."
                )

        self.checkpointer = checkpointer
        self.thread_id = self.__class__.__name__
        self._run_id = None
        self._run_start = None  # perf counter start
        self._run_wall_start = None  # wall-clock start

    # --- auto-wrap any assigned .action so invoke() gets a run-level metric ---
    def __setattr__(self, name, value):
        if (
            name == "action"
            and hasattr(value, "invoke")
            and not getattr(value, "_ursa_wrapped_action", False)
        ):
            value = self._wrap_action_for_run_timing(value)
        super().__setattr__(name, value)

    def _wrap_action_for_run_timing(self, inner):
        parent = self

        class _ActionProxy:
            __slots__ = ("_inner",)
            _ursa_wrapped_action = True

            def __init__(self, i):
                self._inner = i

            def __getattr__(self, attr):
                return getattr(self._inner, attr)

            def invoke(self, *args, **kwargs):
                import time
                import uuid
                from datetime import datetime, timezone

                t0 = time.perf_counter()
                wall = datetime.now(timezone.utc).isoformat()
                ok = True
                res = None
                try:
                    res = self._inner.invoke(*args, **kwargs)
                    return res
                except Exception:
                    ok = False
                    raise
                finally:
                    try:
                        if isinstance(res, dict):
                            bucket = res.setdefault(
                                "__metrics__", {}
                            ).setdefault(parent.thread_id, [])
                            bucket.append({
                                "agent": parent.thread_id,
                                "run_id": uuid.uuid4().hex,
                                "ok": ok,
                                "started_at": wall,
                                "duration_ms": int(
                                    (time.perf_counter() - t0) * 1000
                                ),
                                "node": "run",
                            })
                    except Exception:
                        pass

        return _ActionProxy(inner)

    def wrap_runnable_as_node(self, runnable, node_name: str = "tools_node"):
        """
        Return a bound, timed callable suitable for graph.add_node(...).
        It delegates to a Runnable/ToolNode and works with dict/list/Command returns.
        """

        # define a method-shaped function: (self, state) -> update/Command/list
        def _node(self_, state):
            return runnable.invoke(state)

        # give it a nice name for metrics ("node" field uses fn.__name__)
        try:
            _node.__name__ = node_name
        except Exception:
            pass

        # decorate so it gets timed
        decorated = timed_node(_node)  # or `self.timed_node` if you prefer

        # **bind** to this instance so LangGraph calls it with just (state)
        return MethodType(decorated, self)

    def write_state(self, filename, state):
        json_state = dumps(state, ensure_ascii=False)
        with open(filename, "w") as f:
            f.write(json_state)

    def _start_timer(self) -> None:
        """Call at the beginning of an agent run."""
        self._run_id = uuid.uuid4().hex
        self._run_start = time.perf_counter()
        self._run_wall_start = datetime.now(timezone.utc).isoformat()

    def _build_metric(
        self,
        ok: bool = True,
        err: Exception | None = None,
        extra: dict | None = None,
    ) -> dict:
        """Build a single timing record for this run."""
        if self._run_start is None:
            # If someone forgot to start, still return a minimal metric
            duration_ms = 0
        else:
            duration_ms = int((time.perf_counter() - self._run_start) * 1000)

        metric = {
            "agent": self.thread_id,  # logical agent id
            "run_id": self._run_id,  # per-run id
            "ok": ok,  # success/failure
            "started_at": self._run_wall_start,  # ISO-8601 (UTC)
            "duration_ms": duration_ms,
        }
        if err is not None:
            metric["error_type"] = type(err).__name__
            metric["error"] = str(err)[:500]
        if extra:
            metric.update(extra)
        return metric

    def _inject_metric(self, updates: dict | None, metric: dict) -> dict:
        """Inject metric into updates under __metrics__[agent_id] and return updates."""
        updates = updates or {}
        bucket = updates.setdefault(_METRICS_KEY, {})
        runs = bucket.setdefault(self.thread_id, [])
        runs.append(metric)
        return updates

    def _finish_timer(
        self,
        updates: dict | None,
        ok: bool = True,
        err: Exception | None = None,
        extra: dict | None = None,
    ) -> dict:
        """
        Call at the end of an agent run (in finally/except as needed).
        Returns the updates with the timing metric injected.
        """
        metric = self._build_metric(ok=ok, err=err, extra=extra)
        # Clear per-run fields to avoid accidental reuse
        self._run_id = None
        self._run_start = None
        self._run_wall_start = None
        return self._inject_metric(updates, metric)

    # ---- Metrics helpers (summary + printing) ----
    def _collect_metrics(self, state: dict) -> dict:
        """Return the metrics bucket from final state: {agent_id: [runs...]}"""
        return (state or {}).get(_METRICS_KEY, {}) or {}

    @staticmethod
    def _summarize_runs(runs: list[dict]) -> dict:
        """Compute simple stats for a list of run dicts (duration_ms, ok, ...)."""
        if not runs:
            return {
                "count": 0,
                "errors": 0,
                "total_ms": 0,
                "avg_ms": 0,
                "p50_ms": 0,
                "p95_ms": 0,
                "max_ms": 0,
            }
        durs = [int(r.get("duration_ms", 0)) for r in runs]
        durs_sorted = sorted(durs)
        n = len(durs_sorted)

        def pct(p: float) -> int:
            # nearest-rank
            import math

            k = max(0, min(n - 1, int(math.ceil(p * n) - 1)))
            return durs_sorted[k]

        return {
            "count": n,
            "errors": sum(0 if r.get("ok", True) else 1 for r in runs),
            "total_ms": sum(durs),
            "avg_ms": int(sum(durs) / n),
            "p50_ms": pct(0.50),
            "p95_ms": pct(0.95),
            "max_ms": durs_sorted[-1],
        }

    def print_metrics_by_node(
        self, final_state: dict, hide_zeros: bool = False
    ) -> None:
        metrics = (
            (final_state or {}).get("__metrics__", {}).get(self.thread_id, [])
        )
        by_node = {}
        for r in metrics:
            node = r.get("node", "unknown")
            d = int(r.get("duration_ms", 0))
            if hide_zeros and d == 0:
                continue
            by_node.setdefault(node, []).append(d)

        # build summaries
        rows = []
        for node, durs in by_node.items():
            durs.sort()
            n = len(durs)
            rows.append({
                "node": node,
                "count": n,
                "total_ms": sum(durs),
                "avg_ms": int(sum(durs) / n) if n else 0,
                "max_ms": durs[-1] if n else 0,
            })
        rows.sort(key=lambda r: r["total_ms"], reverse=True)

        # pretty print (Rich if available)
        try:
            from rich.console import Console
            from rich.table import Table

            console = Console()
            t = Table(title=f"{self.thread_id} – Per-Node Timing")
            t.add_column("Node", style="bold")
            t.add_column("Count", justify="right")
            t.add_column("Total (s)", justify="right")
            t.add_column("Avg (ms)", justify="right")
            t.add_column("Max (ms)", justify="right")
            for r in rows:
                t.add_row(
                    r["node"],
                    str(r["count"]),
                    f"{r['total_ms'] / 1000:.2f}",
                    str(r["avg_ms"]),
                    str(r["max_ms"]),
                )
            console.print(t)
        except Exception:
            print(f"\n{self.thread_id} – Per-Node Timing")
            for r in rows:
                print(
                    f"- {r['node']}: count={r['count']} total={r['total_ms'] / 1000:.2f}s "
                    f"avg={r['avg_ms']}ms max={r['max_ms']}ms"
                )

    def print_metrics(
        self, final_state: dict, *, top_k_slowest: int = 5
    ) -> None:
        """
        Pretty-print per-agent metrics + top slowest runs (if any).
        Uses Rich if available, falls back to plain text.
        """
        metrics = self._collect_metrics(final_state)
        print(metrics)

        # build summaries
        summaries = {
            agent: self._summarize_runs(runs) for agent, runs in metrics.items()
        }

        # gather slowest across all agents
        all_runs = []
        for agent, runs in metrics.items():
            for r in runs:
                all_runs.append({
                    "agent": agent,
                    "duration_ms": int(r.get("duration_ms", 0)),
                    "ok": bool(r.get("ok", True)),
                    "run_id": r.get("run_id", ""),
                    "started_at": r.get("started_at", ""),
                })
        all_runs.sort(key=lambda x: x["duration_ms"], reverse=True)
        top = all_runs[:top_k_slowest] if top_k_slowest and all_runs else []

        # try rich first
        try:
            from rich.console import Console
            from rich.table import Table

            console = Console()
            tbl = Table(title="Agent Timing Summary")
            tbl.add_column("Agent", style="bold")
            tbl.add_column("Count", justify="right")
            tbl.add_column("Errors", justify="right")
            tbl.add_column("Total (s)", justify="right")
            tbl.add_column("Avg (ms)", justify="right")
            tbl.add_column("P50 (ms)", justify="right")
            tbl.add_column("P95 (ms)", justify="right")
            tbl.add_column("Max (ms)", justify="right")

            for agent, s in summaries.items():
                tbl.add_row(
                    agent,
                    str(s["count"]),
                    str(s["errors"]),
                    f"{s['total_ms'] / 1000:.2f}",
                    str(s["avg_ms"]),
                    str(s["p50_ms"]),
                    str(s["p95_ms"]),
                    str(s["max_ms"]),
                )
            console.print(tbl)

            if top:
                t2 = Table(title=f"Top {len(top)} Slowest Runs")
                t2.add_column("Agent")
                t2.add_column("Duration (ms)", justify="right")
                t2.add_column("OK", justify="center")
                t2.add_column("Run ID")
                t2.add_column("Started (UTC)")
                for r in top:
                    t2.add_row(
                        r["agent"],
                        str(r["duration_ms"]),
                        "✓" if r["ok"] else "✗",
                        r["run_id"],
                        r["started_at"],
                    )
                console.print(t2)

        except Exception:
            # plain text fallback
            print("\n== Agent Timing Summary ==")
            for agent, s in summaries.items():
                print(
                    f"- {agent}: count={s['count']} errors={s['errors']} "
                    f"total={s['total_ms'] / 1000:.2f}s avg={s['avg_ms']}ms "
                    f"p50={s['p50_ms']}ms p95={s['p95_ms']}ms max={s['max_ms']}ms"
                )
            if top:
                print(f"\n== Top {len(top)} Slowest Runs ==")
                for r in top:
                    ok = "OK" if r["ok"] else "ERR"
                    print(
                        f"- {r['agent']} {r['duration_ms']}ms {ok} run_id={r['run_id']} started={r['started_at']}"
                    )
