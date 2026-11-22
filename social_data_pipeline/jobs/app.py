"""FastAPI app factory: wires UI, MCP, runner together in one process."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from . import auth
from .auto_accept import AutoAcceptStore
from .config import JobsConfig, load_config
from .mcp_tools import build_mcp, build_streamable_app
from .runner import Runner
from .store import Store
from .web import build_router


log = logging.getLogger(__name__)


STATIC_DIR = Path(__file__).parent / "static"


def _open_door(cfg, store, runner, auto_accept, loop):
    """Open the control socket (`door.py`) for the scheduler's lifetime, or return None. Nothing the door does or lacks stops the scheduler: its import (a missing `control` package included) and every error of its opening are caught here and logged by the name of their kind only; a door that stays closed for a known reason has logged that reason itself."""
    try:
        from .door import open_door

        door = open_door(cfg, store, runner, auto_accept, loop)
    except Exception as error:
        log.warning("the control socket stays closed: it could not be opened (%s)", type(error).__name__)
        return None
    if door is not None:
        log.info("the control socket is open")
    return door


def _close_door(door):
    """Close the control socket opened by `_open_door`; an error is logged by the name of its kind and changes nothing else."""
    if door is None:
        return
    try:
        door.close()
    except Exception as error:
        log.warning("the control socket could not be closed cleanly (%s)", type(error).__name__)


def _build_assistant(cfg, store, runner):
    """The assistant's endpoint (`assistant.py`): (its MCP server, the gate's class, the MCP server's app), or None. As for the door, nothing about it stops the scheduler: an error while it is built is logged by the name of its kind only, and the endpoint then stays closed (a path under `/assistant/` is then one the page does not have)."""
    try:
        from .assistant import AssistantGate

        assistant_mcp = build_mcp(cfg, store, runner, assistant=True)
        return assistant_mcp, AssistantGate, build_streamable_app(assistant_mcp)
    except Exception as error:
        log.warning("the assistant's endpoint stays closed: it could not be built (%s)", type(error).__name__)
        return None


def build_app(cfg: JobsConfig | None = None) -> FastAPI:
    if cfg is None:
        cfg = load_config()

    # Fail fast if auth is enabled but no password is available — better
    # than silently serving an always-rejecting login page.
    auth.validate_startup(cfg.auth_enabled)

    store = Store(cfg.jobs_dir)
    auto_accept = AutoAcceptStore(
        state_path=cfg.jobs_dir / "auto_accept.json",
        max_limit=cfg.max_concurrent,
    )
    runner = Runner(cfg, store, auto_accept)
    mcp = build_mcp(cfg, store, runner)
    # Build the ASGI app up front: it is what creates the MCP session manager,
    # which `lifespan` below runs. Mounting happens after `app` exists.
    mcp_app = build_streamable_app(mcp)
    assistant = _build_assistant(cfg, store, runner)

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        # Start MCP session manager + background runner for the lifetime of
        # the ASGI application.
        async with mcp.session_manager.run(), (assistant[0].session_manager.run() if assistant else contextlib.nullcontext()):
            runner.start()
            door = _open_door(cfg, store, runner, auto_accept, asyncio.get_running_loop())
            try:
                yield
            finally:
                _close_door(door)
                runner.stop()

    app = FastAPI(title="SDP Jobs", lifespan=lifespan)

    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    app.mount("/mcp", mcp_app)
    if assistant is not None:
        _mcp, gate, assistant_app = assistant
        app.add_middleware(gate, store=store, mcp_app=assistant_app, jobs_dir=cfg.jobs_dir)

    app.include_router(build_router(cfg, store, runner, auto_accept))

    app.state.cfg = cfg
    app.state.store = store
    app.state.runner = runner

    return app
