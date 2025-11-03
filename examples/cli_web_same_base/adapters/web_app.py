# adapters/web_app.py
from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List

from fastapi import FastAPI, Form, HTTPException, Query, Request, status
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    RedirectResponse,
    StreamingResponse,
)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Executor builder (same as CLI)
from langchain_litellm import ChatLiteLLM
from langgraph.checkpoint.sqlite import SqliteSaver
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from ursa.agents import ExecutionAgent

# Robust imports that work in package or script mode
try:
    from cli_web_same_base.engine import run_steps
    from cli_web_same_base.events import (
        RunCompleted,
        RunFailed,
        RunStarted,
        RunSummaryRequested,
        StepAnnounced,
        StepResult,
    )
except Exception:
    try:
        from ..engine import run_steps
        from ..events import (
            RunCompleted,
            RunFailed,
            RunStarted,
            RunSummaryRequested,
            StepAnnounced,
            StepResult,
        )
    except Exception:
        from engine import run_steps
        from events import (
            RunCompleted,
            RunFailed,
            RunStarted,
            RunSummaryRequested,
            StepAnnounced,
            StepResult,
        )

app = FastAPI(title="Integer Sum — Web Demo")


# --- Minimal example "problem" (same as CLI) ---
PROBLEM = [
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
        llm=model, checkpointer=checkpointer, enable_metrics=True
    )


# ---- FastAPI static/templates ----
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "web" / "templates"
STATIC_DIR = BASE_DIR / "web" / "static"

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


# ---- App state & sessions ----
@app.on_event("startup")
async def _startup():
    app.state.loop = asyncio.get_running_loop()


@app.on_event("shutdown")
async def _shutdown():
    # Stop every active watcher (and any future cleanup you want)
    for sid in list(SESSIONS.keys()):
        _stop_watcher(sid)


SESSIONS: Dict[str, Dict[str, Any]] = {}
# session model:
# {
#   "status": "starting"|"running"|"done"|"error",
#   "events": [dict, ...],         # history for reloads
#   "queue": asyncio.Queue,        # live events for SSE
#   "workspace": str,
#   "thread": threading.Thread
# }


# Filter: hide SQLite artifacts & dotfiles
def _ignore_name(name: str) -> bool:
    if name.startswith("."):
        return True
    return name.endswith((".db", ".db-wal", ".db-shm"))


class WorkspaceWatcher(FileSystemEventHandler):
    """Send FS events to the session's SSE queue; coalesce tree refreshes."""

    def __init__(self, sid: str, root: Path):
        self.sid = sid
        self.root = root
        self._last_tree_emit = 0.0

    def _enqueue(self, data: dict) -> None:
        sess = SESSIONS.get(self.sid)
        if not sess:
            return
        # push to live stream (don't bloat history with FS events)
        app.state.loop.call_soon_threadsafe(sess["queue"].put_nowait, data)

    def _rel(self, path: str) -> str:
        try:
            return str(Path(path).resolve().relative_to(self.root))
        except Exception:
            return Path(path).name

    def _maybe_tree_changed(self) -> None:
        now = time.time()
        if now - self._last_tree_emit > 0.3:  # debounce bursts
            self._enqueue({"type": "tree_changed"})
            self._last_tree_emit = now

    # Watchdog callbacks
    def on_created(self, event):
        name = Path(event.src_path).name
        if not _ignore_name(name):
            self._enqueue({
                "type": "fs_created",
                "path": self._rel(event.src_path),
            })
            self._maybe_tree_changed()

    def on_modified(self, event):
        if event.is_directory:
            return
        name = Path(event.src_path).name
        if not _ignore_name(name):
            self._enqueue({
                "type": "fs_modified",
                "path": self._rel(event.src_path),
            })

    def on_deleted(self, event):
        name = Path(event.src_path).name
        if not _ignore_name(name):
            self._enqueue({
                "type": "fs_deleted",
                "path": self._rel(event.src_path),
            })
            self._maybe_tree_changed()

    def on_moved(self, event):
        name = Path(event.dest_path).name
        if not _ignore_name(name):
            self._enqueue({
                "type": "fs_moved",
                "src": self._rel(event.src_path),
                "dest": self._rel(event.dest_path),
            })
            self._maybe_tree_changed()


@app.get("/health")
def health():
    return {"ok": True}


def _event_to_dict(evt: object) -> Dict[str, Any]:
    if isinstance(evt, RunStarted):
        return {
            "type": "run_started",
            "workspace": evt.workspace,
            "steps_count": evt.steps_count,
        }
    if isinstance(evt, StepAnnounced):
        return {
            "type": "step_announced",
            "index": evt.index,
            "prompt": evt.prompt,
        }
    if isinstance(evt, StepResult):
        return {
            "type": "step_result",
            "index": evt.index,
            "title": evt.title,
            "body": evt.body,
        }
    if isinstance(evt, RunSummaryRequested):
        return {"type": "run_summary_requested"}
    if isinstance(evt, RunCompleted):
        return {"type": "run_completed", "success": evt.success}
    if isinstance(evt, RunFailed):
        return {"type": "run_failed", "error": evt.error}
    return {"type": "unknown"}


def _safe_workspace(sid: str) -> Path:
    sess = SESSIONS.get(sid)
    if not sess:
        raise HTTPException(status_code=404, detail="Session not found")
    return Path(sess["workspace"]).resolve()


def _list_tree(root: Path) -> Dict[str, Any]:
    def walk(p: Path) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for child in sorted(
            p.iterdir(), key=lambda c: (c.is_file(), c.name.lower())
        ):
            if child.name.startswith("."):
                continue
            if child.is_dir():
                items.append({
                    "type": "dir",
                    "name": child.name,
                    "children": walk(child),
                })
            else:
                # hide the sqlite db by default
                if child.name.endswith(".db"):
                    continue
                rel = child.relative_to(root).as_posix()
                items.append({
                    "type": "file",
                    "name": child.name,
                    "path": rel,
                    "size": child.stat().st_size,
                })
        return items

    return {"root": root.name, "children": walk(root)}


def _start_background_run(sid: str, workspace: str) -> None:
    sess = SESSIONS[sid]
    sess["queue"] = asyncio.Queue()
    executor = build_executor(workspace)

    def emit(evt: object) -> None:
        data = _event_to_dict(evt)
        sess["events"].append(data)
        sess["status"] = "running"
        app.state.loop.call_soon_threadsafe(sess["queue"].put_nowait, data)

    def target() -> None:
        try:
            run_steps(PROBLEM, workspace, executor, emit=emit)
            sess["status"] = "done"
            app.state.loop.call_soon_threadsafe(
                sess["queue"].put_nowait,
                {"type": "run_completed", "success": True},
            )
        except Exception as e:
            sess["status"] = "error"
            err = {"type": "run_failed", "error": str(e)}
            sess["events"].append(err)
            app.state.loop.call_soon_threadsafe(sess["queue"].put_nowait, err)

    t = threading.Thread(target=target, daemon=True)
    t.start()
    sess["thread"] = t

    # 🔎 Start filesystem watcher for this workspace
    root = Path(workspace).resolve()
    handler = WorkspaceWatcher(sid, root)
    observer = Observer()
    observer.schedule(handler, str(root), recursive=True)
    observer.start()
    sess["fs_observer"] = observer
    sess["fs_handler"] = handler


def _stop_watcher(sid: str) -> None:
    """Stop the filesystem observer for a session and clean up."""
    sess = SESSIONS.get(sid)
    if not sess:
        return
    observer = sess.get("fs_observer")
    if observer:
        try:
            observer.stop()
            observer.join(timeout=2)  # don't block forever
        except Exception:
            pass
        finally:
            sess["fs_observer"] = None


# ---- Pages ----
@app.get("/")
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/start")
def start(request: Request, workspace: str = Form("example_integer_sum")):
    sid = uuid.uuid4().hex
    SESSIONS[sid] = {"status": "starting", "events": [], "workspace": workspace}
    _start_background_run(sid, workspace)
    return RedirectResponse(url=f"/run/{sid}", status_code=303)


@app.get("/run/{sid}")
def run_view(request: Request, sid: str):
    sess = SESSIONS.get(sid)
    if not sess:
        return RedirectResponse(url="/", status_code=303)
    return templates.TemplateResponse(
        "run.html",
        {
            "request": request,
            "sid": sid,
            "status": sess["status"],
            "workspace": sess["workspace"],
        },
    )


@app.post("/close/{sid}")
def close_session(sid: str):
    if sid not in SESSIONS:
        raise HTTPException(status_code=404, detail="Session not found")
    _stop_watcher(sid)
    # Keep session metadata if you want, or remove it:
    # SESSIONS.pop(sid, None)
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)


# ---- Streaming events (SSE) ----
@app.get("/events/{sid}")
async def events_stream(sid: str):
    sess = SESSIONS.get(sid)
    if not sess:
        raise HTTPException(status_code=404, detail="Session not found")
    queue: asyncio.Queue = sess["queue"]

    async def gen():
        # send backlog first
        for e in sess["events"]:
            yield f"data: {json.dumps(e)}\n\n"
        # then live events
        while True:
            e = await queue.get()
            yield f"data: {json.dumps(e)}\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream")


# ---- Files API ----
@app.get("/api/tree/{sid}")
def api_tree(sid: str):
    root = _safe_workspace(sid)
    if not root.exists():
        return JSONResponse({"root": root.name, "children": []})
    return JSONResponse(_list_tree(root))


@app.get("/api/file/{sid}")
def api_file(
    sid: str, p: str = Query(..., alias="path"), format: str = Query("text")
):
    root = _safe_workspace(sid)
    target = (root / p).resolve()
    try:
        target.relative_to(root)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid path")
    if not target.exists() or target.is_dir():
        raise HTTPException(status_code=404, detail="File not found")

    # text fallback first
    try:
        text = target.read_text(encoding="utf-8")
    except Exception:
        text = f"<<binary or non-UTF8 file: {target.name} ({target.stat().st_size} bytes)>>"
        return PlainTextResponse(text)

    if format == "html":
        # Server-side syntax highlighting with Pygments
        try:
            from pygments import highlight
            from pygments.formatters import HtmlFormatter
            from pygments.lexers import guess_lexer_for_filename

            try:
                lexer = guess_lexer_for_filename(target.name, text)
            except Exception:
                from pygments.lexers.special import TextLexer

                lexer = TextLexer()
            formatter = HtmlFormatter(
                noclasses=True,  # inline styles (no extra CSS file)
                linenos=False,  # or True if you want line numbers
                style="monokai",  # try "native", "github-dark", etc.
                nobackground=True,  # << don't emit a background color
            )
            html = highlight(text, lexer, formatter)
            return HTMLResponse(html)
        except Exception:
            # If Pygments fails for any reason, fall back to plain text
            return PlainTextResponse(text)

    # default: plain text
    return PlainTextResponse(text)


# Run with:
#   python -m uvicorn cli_web_same_base.adapters.web_app:app --reload
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "cli_web_same_base.adapters.web_app:app",
        host="127.0.0.1",
        port=8000,
        reload=True,
    )
