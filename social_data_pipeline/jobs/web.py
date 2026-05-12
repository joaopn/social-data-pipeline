"""Web UI routes: Pending / Running / History + approve/reject/kill actions."""

from __future__ import annotations

import logging
import time
from pathlib import Path

import os
import shutil
import threading

import sqlparse
from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from . import auth
from .auto_accept import AutoAcceptStore
from .config import JobsConfig
from .runner import Runner
from .store import Store


log = logging.getLogger(__name__)


TEMPLATES_DIR = Path(__file__).parent / "templates"


def build_router(
    cfg: JobsConfig,
    store: Store,
    runner: Runner,
    auto_accept: AutoAcceptStore,
) -> APIRouter:
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    templates.env.globals["relative_time"] = _relative_time
    templates.env.globals["absolute_time"] = _absolute_time
    templates.env.globals["duration"] = _duration
    templates.env.globals["human_bytes"] = _human_bytes
    templates.env.globals["sql_preview"] = _sql_preview
    templates.env.globals["format_sql"] = _format_sql
    templates.env.globals["job_body"] = _job_body
    templates.env.globals["job_body_lang"] = _job_body_lang
    templates.env.globals["host_path"] = _make_host_path_translator(cfg)
    templates.env.globals["auth_enabled"] = cfg.auth_enabled
    templates.env.globals["disk_stat"] = lambda: _disk_stat(cfg)

    require_auth = auth.require_auth_dep(cfg.auth_enabled)

    # Two routers: the public one serves /login, /logout, /static (mounted at
    # app level). The protected one gets require_auth applied at router level
    # so we don't have to thread it through every UI handler.
    router = APIRouter()
    protected = APIRouter(dependencies=[Depends(require_auth)])

    # --- Login / logout (always public) -----------------------------------

    @router.get("/login", response_class=HTMLResponse)
    async def login_page(request: Request, error: str | None = None):
        if not cfg.auth_enabled:
            return RedirectResponse(url="/pending", status_code=302)
        if auth.is_authenticated(request):
            return RedirectResponse(url="/pending", status_code=302)
        return templates.TemplateResponse(
            request=request,
            name="login.html",
            context={"error": error},
        )

    @router.post("/login")
    async def login_submit(request: Request, password: str = Form(default="")):
        if not cfg.auth_enabled:
            return RedirectResponse(url="/pending", status_code=303)
        if auth.check_password(password):
            resp = RedirectResponse(url="/pending", status_code=303)
            auth.set_auth_cookie(resp)
            log.info("login succeeded from %s", request.client.host if request.client else "?")
            return resp
        log.warning(
            "login failed from %s",
            request.client.host if request.client else "?",
        )
        return templates.TemplateResponse(
            request=request,
            name="login.html",
            context={"error": "Invalid password."},
            status_code=401,
        )

    @router.post("/logout")
    async def logout():
        resp = RedirectResponse(url="/login", status_code=303)
        auth.clear_auth_cookie(resp)
        return resp

    # --- Protected UI routes ----------------------------------------------

    def _counts() -> dict[str, int]:
        return {
            "pending": len(list(store.pending.glob("*.json"))),
            "approved": len(list(store.approved.glob("*.json"))),
            "running": len(list(store.running.glob("*.json"))),
        }

    @protected.get("/", response_class=HTMLResponse)
    async def root():
        return RedirectResponse(url="/pending", status_code=302)

    @protected.get("/pending", response_class=HTMLResponse)
    async def pending(request: Request):
        jobs = store.list_phase("pending")
        return templates.TemplateResponse(
            request=request,
            name="pending.html",
            context={
                "tab": "pending",
                "jobs": jobs,
                "counts": _counts(),
            },
        )

    @protected.get("/running", response_class=HTMLResponse)
    async def running(request: Request):
        return templates.TemplateResponse(
            request=request,
            name="running.html",
            context={
                "tab": "running",
                "running": store.list_phase("running"),
                "approved": store.list_phase("approved"),
                "now": time.time(),
                "counts": _counts(),
            },
        )

    @protected.get("/fragments/running", response_class=HTMLResponse)
    async def running_fragment(request: Request):
        return templates.TemplateResponse(
            request=request,
            name="_running_rows.html",
            context={
                "running": store.list_phase("running"),
                "approved": store.list_phase("approved"),
                "now": time.time(),
            },
        )

    @protected.get("/history", response_class=HTMLResponse)
    async def history(request: Request):
        jobs = store.iter_history(limit=cfg.history_retention)
        return templates.TemplateResponse(
            request=request,
            name="history.html",
            context={
                "tab": "history",
                "jobs": jobs,
                "retention": cfg.history_retention,
                "counts": _counts(),
            },
        )

    @protected.post("/actions/approve/{job_id}")
    async def approve(job_id: str):
        try:
            store.approve(job_id)
            log.info("approved %s via web UI", job_id)
        except KeyError:
            log.warning("approve: %s not pending", job_id)
        return RedirectResponse(url="/pending", status_code=303)

    @protected.post("/actions/reject/{job_id}")
    async def reject(job_id: str, reason: str = Form(default="")):
        try:
            store.reject(job_id, reason=(reason.strip() or None))
            log.info("rejected %s via web UI", job_id)
        except KeyError:
            log.warning("reject: %s not pending", job_id)
        return RedirectResponse(url="/pending", status_code=303)

    @protected.post("/actions/kill/{job_id}")
    async def kill(job_id: str):
        ok = runner.request_cancel(job_id)
        if not ok:
            log.warning("kill: %s not currently running", job_id)
        return RedirectResponse(url="/running", status_code=303)

    @protected.get("/actions/explain/{job_id}", response_class=HTMLResponse)
    async def explain(request: Request, job_id: str):
        try:
            plan = runner.explain(job_id)
            return templates.TemplateResponse(
                request=request,
                name="_explain_result.html",
                context={"plan": plan, "error": None},
            )
        except Exception as e:
            return templates.TemplateResponse(
                request=request,
                name="_explain_result.html",
                context={"plan": None, "error": f"{type(e).__name__}: {e}"},
            )

    @protected.get("/actions/download/{job_id}")
    async def download(job_id: str):
        located = store.find(job_id)
        if not located:
            raise HTTPException(status_code=404, detail="job not found")
        _phase, job = located
        if job.status != "done" or not job.result_path:
            raise HTTPException(status_code=404, detail="no result available")
        try:
            buf = _build_result_zip(Path(job.result_path))
        except FileNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except RuntimeError as e:
            raise HTTPException(status_code=404, detail=str(e))

        def _iter():
            try:
                while True:
                    chunk = buf.read(64 * 1024)
                    if not chunk:
                        break
                    yield chunk
            finally:
                buf.close()

        filename = f"{job_id}.zip"
        return StreamingResponse(
            _iter(),
            media_type="application/zip",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @protected.get("/actions/preview/{job_id}", response_class=HTMLResponse)
    async def preview(request: Request, job_id: str):
        located = store.find(job_id)
        if not located:
            return templates.TemplateResponse(
                request=request,
                name="_preview_result.html",
                context={"columns": None, "rows": None, "error": "job not found"},
            )
        _phase, job = located
        try:
            columns, rows = _read_preview(job.result_path, limit=20)
            return templates.TemplateResponse(
                request=request,
                name="_preview_result.html",
                context={"columns": columns, "rows": rows, "error": None},
            )
        except Exception as e:
            return templates.TemplateResponse(
                request=request,
                name="_preview_result.html",
                context={"columns": None, "rows": None, "error": f"{type(e).__name__}: {e}"},
            )

    # --- Auto-accept controls --------------------------------------------

    def _auto_accept_context(request: Request) -> dict:
        state = auto_accept.get_state()
        # Render rows for every configured target so the user sees a full
        # list, including ones never toggled before. Untouched targets
        # surface limit = max_limit so the slider renders at its top end
        # (and matches what set_target writes on first toggle).
        rows = []
        for t in cfg.targets.values():
            tstate = state.targets.get(t.name)
            rows.append({
                "name": t.name,
                "backend": t.backend,
                "database": t.database,
                "enabled": bool(tstate.enabled) if tstate else False,
                "limit": int(tstate.limit) if tstate else auto_accept.max_limit,
            })
        enabled_count = sum(1 for r in rows if r["enabled"])
        return {
            "request": request,
            "max_limit": auto_accept.max_limit,
            "rows": rows,
            "enabled_count": enabled_count,
            "total_targets": len(rows),
        }

    @protected.get("/fragments/auto_accept", response_class=HTMLResponse)
    async def auto_accept_fragment(request: Request):
        return templates.TemplateResponse(
            request=request,
            name="_auto_accept.html",
            context=_auto_accept_context(request),
        )

    @protected.post("/actions/auto_accept/target/{name}", response_class=HTMLResponse)
    async def auto_accept_target(request: Request, name: str):
        if name not in cfg.targets:
            raise HTTPException(status_code=404, detail=f"unknown target: {name}")
        form = await request.form()
        enabled_bool, limit = _parse_auto_accept_form(form)
        auto_accept.set_target(name, enabled=enabled_bool, limit=limit)
        log.info(
            "auto-accept target %s set: enabled=%s limit=%s",
            name, enabled_bool, limit,
        )
        return templates.TemplateResponse(
            request=request,
            name="_auto_accept.html",
            context=_auto_accept_context(request),
        )

    router.include_router(protected)
    return router


def _parse_auto_accept_form(form) -> tuple[bool | None, int | None]:
    """Parse the auto-accept POST body into ``(enabled, limit)``.

    Reads via multidict membership rather than ``Form(default=None)``:
    Pydantic v2 collapses ``enabled=`` (present with empty value) back
    to None, which is indistinguishable from "field absent". We need
    the distinction — empty value means "switch off", absent means
    "no change". A missing or non-numeric ``limit`` returns None
    (no change) rather than raising — a stale tab posting garbage
    shouldn't 500.
    """
    enabled: bool | None = None
    if "enabled" in form:
        enabled = bool(form["enabled"])  # "" → False, "on" → True
    limit: int | None = None
    if "limit" in form:
        try:
            limit = int(form["limit"])
        except (TypeError, ValueError):
            limit = None
    return enabled, limit


# ----------------------------------------------------------------------------
# Formatting helpers (registered as Jinja globals).


def _relative_time(ts: float | None) -> str:
    if not ts:
        return "—"
    delta = max(0.0, time.time() - ts)
    return _duration(delta) + " ago"


def _absolute_time(ts: float | None) -> str:
    if not ts:
        return "—"
    lt = time.localtime(ts)
    return time.strftime("%Y-%m-%d %H:%M:%S", lt)


def _duration(seconds: float | None) -> str:
    if seconds is None:
        return "—"
    seconds = max(0.0, float(seconds))
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        m, s = divmod(int(seconds), 60)
        return f"{m}m {s}s"
    if seconds < 86400:
        h, rem = divmod(int(seconds), 3600)
        m = rem // 60
        return f"{h}h {m}m"
    d, rem = divmod(int(seconds), 86400)
    h = rem // 3600
    return f"{d}d {h}h"


def _human_bytes(n: int | None) -> str:
    if n is None:
        return "—"
    n = int(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024 or unit == "TiB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n:.1f} TiB"


def _sql_preview(sql: str, width: int = 90) -> str:
    flat = " ".join(sql.split())
    if len(flat) <= width:
        return flat
    return flat[: width - 1] + "…"


def _format_sql(sql: str | None) -> str:
    if not sql:
        return ""
    try:
        return sqlparse.format(
            sql,
            reindent=True,
            keyword_case="upper",
            use_space_around_operators=True,
        )
    except Exception:
        return sql


def _job_body(job) -> str:
    """Body text shown in the expanded SQL/pipeline panel.

    For PG/SR this pretty-prints the SQL. For Mongo jobs, the stored
    payload is already JSON; return it as-is so Prism's language-json
    picks it up without sqlparse mangling JSON punctuation.
    """
    if getattr(job, "backend", None) == "mongodb":
        return job.sql or ""
    return _format_sql(job.sql)


def _job_body_lang(job) -> str:
    return "language-json" if getattr(job, "backend", None) == "mongodb" else "language-sql"


def _read_preview(result_path: str | None, limit: int = 20) -> tuple[list[str], list[list]]:
    """Return (columns, rows) from the first part of a job's result folder.

    Supports parquet (via pyarrow row-group iteration, bounded memory) and CSV
    (stdlib csv, first N data rows). Raises on missing folder / no readable
    parts / unsupported extension.
    """
    if not result_path:
        raise RuntimeError("job has no result path")
    folder = Path(result_path)
    if not folder.exists():
        raise FileNotFoundError(f"result folder missing: {folder}")

    parquet_parts = sorted(folder.glob("*.parquet"))
    csv_parts = sorted(folder.glob("*.csv"))
    ndjson_parts = sorted(folder.glob("*.ndjson"))
    if ndjson_parts:
        import json as _json

        columns: list[str] = []
        rows: list[list] = []
        with open(ndjson_parts[0]) as f:
            for i, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                if i >= limit:
                    break
                try:
                    doc = _json.loads(line)
                except _json.JSONDecodeError:
                    continue
                if not isinstance(doc, dict):
                    continue
                for k in doc.keys():
                    if k not in columns:
                        columns.append(k)
                rows.append(doc)
        # Normalize: each row aligned to columns order. Nested values
        # serialized to JSON string for display (NDJSON is lossless, but the
        # preview table is flat).
        aligned: list[list] = []
        for doc in rows:
            out = []
            for k in columns:
                v = doc.get(k)
                if isinstance(v, (dict, list)):
                    v = _json.dumps(v, default=str)
                out.append(v)
            aligned.append(out)
        return columns, aligned

    if parquet_parts:
        import pyarrow.parquet as pq

        pf = pq.ParquetFile(parquet_parts[0])
        batch = next(pf.iter_batches(batch_size=limit), None)
        if batch is None:
            return list(pf.schema_arrow.names), []
        table = batch.to_pydict()
        columns = list(table.keys())
        n = min(limit, batch.num_rows)
        rows = [[table[c][i] for c in columns] for i in range(n)]
        return columns, rows

    if csv_parts:
        import csv as _csv

        with open(csv_parts[0], newline="") as f:
            reader = _csv.reader(f)
            try:
                header = next(reader)
            except StopIteration:
                return [], []
            rows = []
            for i, row in enumerate(reader):
                if i >= limit:
                    break
                rows.append(list(row))
            return list(header), rows

    raise RuntimeError(f"no .parquet / .csv / .ndjson parts found in {folder}")


def _build_result_zip(folder: Path):
    """Zip every regular file directly under ``folder`` (non-recursive) into a
    SpooledTemporaryFile and return it positioned at offset 0.

    ZIP_STORED: parquet is already compressed and deflate would burn CPU for
    almost no win. CSV/NDJSON pay a small size cost in exchange for streaming
    being I/O-bound. Spools to disk past 10 MiB so result folders larger than
    RAM still work.

    Caller owns the returned file and must close it.
    """
    import tempfile
    import zipfile

    if not folder.exists():
        raise FileNotFoundError(f"result folder missing: {folder}")
    parts = sorted(p for p in folder.iterdir() if p.is_file())
    if not parts:
        raise RuntimeError(f"no result parts in {folder}")
    buf = tempfile.SpooledTemporaryFile(max_size=10 * 1024 * 1024)
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_STORED) as zf:
        for p in parts:
            zf.write(p, arcname=p.name)
    buf.seek(0)
    return buf


_DISK_CACHE_TTL_SECONDS = 10.0
_disk_cache: dict = {"ts": 0.0, "value": None}
_disk_cache_lock = threading.Lock()


def _disk_stat(cfg: JobsConfig) -> dict | None:
    """Return ``{"results_bytes": int, "free_bytes": int}`` for the results
    folder: bytes taken by existing job outputs and bytes free on the
    filesystem that holds them.

    Cached for ~10s so the scan cost doesn't bite the Running tab's 2s
    poll even when many job folders are present.
    """
    now = time.time()
    with _disk_cache_lock:
        if _disk_cache["value"] is not None and now - _disk_cache["ts"] < _DISK_CACHE_TTL_SECONDS:
            return _disk_cache["value"]

    try:
        usage = shutil.disk_usage(str(cfg.result_root))
    except OSError:
        return None

    results_bytes = _sum_tree_bytes(cfg.result_root)
    value = {"results_bytes": results_bytes, "free_bytes": usage.free}

    with _disk_cache_lock:
        _disk_cache["ts"] = now
        _disk_cache["value"] = value
    return value


def _sum_tree_bytes(root) -> int:
    """Sum file sizes under ``root``. Fast in the common layout
    (<job_id>/<file> two levels deep); safe-ish against concurrent writes
    via per-file try/except on stat."""
    total = 0
    try:
        it = os.scandir(root)
    except OSError:
        return 0
    with it:
        for entry in it:
            try:
                if entry.is_file(follow_symlinks=False):
                    total += entry.stat(follow_symlinks=False).st_size
                elif entry.is_dir(follow_symlinks=False):
                    total += _sum_tree_bytes(entry.path)
            except OSError:
                continue
    return total


def _make_host_path_translator(cfg: JobsConfig):
    """Return a Jinja helper that maps container paths (as stored in job
    records) to the equivalent host path the user can open directly."""
    container_root = str(cfg.result_root).rstrip("/")
    host_root = (cfg.host_result_root or container_root).rstrip("/")

    def host_path(p: str | None) -> str:
        if not p:
            return ""
        if p.startswith(container_root):
            return host_root + p[len(container_root):]
        return p

    return host_path
