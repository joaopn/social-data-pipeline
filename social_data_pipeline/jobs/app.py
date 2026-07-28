"""FastAPI app factory: wires UI, MCP, runner together in one process."""

from __future__ import annotations

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

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        # Start MCP session manager + background runner for the lifetime of
        # the ASGI application.
        async with mcp.session_manager.run():
            runner.start()
            try:
                yield
            finally:
                runner.stop()

    app = FastAPI(title="SDP Jobs", lifespan=lifespan)

    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    app.mount("/mcp", mcp_app)

    app.include_router(build_router(cfg, store, runner, auto_accept))

    app.state.cfg = cfg
    app.state.store = store
    app.state.runner = runner

    return app
