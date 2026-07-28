"""MCP tools exposed by the scheduler. Built dynamically from JobsConfig so
unconfigured backends' tools don't appear in discovery."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from mcp.server.mcpserver import MCPServer
from mcp.server.transport_security import TransportSecuritySettings

from .backends import BackendError, validate_submission
from .config import JobsConfig
from .runner import Runner
from .store import Job, Store


log = logging.getLogger(__name__)


def build_mcp(cfg: JobsConfig, store: Store, runner: Runner) -> MCPServer:
    """Assemble the MCP server for this process.

    Submit tools are only registered for backends with at least one configured
    target. The target name is validated against the live config, not baked
    into the tool schema — tool docstrings enumerate the configured names so
    agents see them during discovery.

    Transport options (endpoint path, DNS-rebinding policy) are not set here:
    in the mcp 2.x SDK they are arguments to ``streamable_http_app()``, which
    ``app.py`` calls when it mounts this server. See ``build_streamable_app()``.
    """
    mcp = MCPServer(name="sdp-jobs")

    pg_targets = [t.name for t in cfg.targets_for("postgres")]
    sr_targets = [t.name for t in cfg.targets_for("starrocks")]
    mongo_targets = [t.name for t in cfg.targets_for("mongodb")]

    if pg_targets:
        _register_submit_postgres(mcp, cfg, store, pg_targets)
    if sr_targets:
        _register_submit_starrocks(mcp, cfg, store, sr_targets)
    if mongo_targets:
        _register_submit_mongo(mcp, cfg, store, mongo_targets)
        _register_list_mongo_databases(mcp, cfg, runner, mongo_targets)

    _register_status_tool(mcp, store)
    _register_cancel_tool(mcp, store)
    _register_list_targets(mcp, cfg)

    return mcp


def build_streamable_app(mcp: MCPServer):
    """Return the streamable-HTTP ASGI app for `mcp`, ready to mount at /mcp.

    streamable_http_path="/" makes the returned Starlette app serve the
    streamable-HTTP endpoint at its own root; combined with FastAPI mounting it
    at /mcp, the client-visible URL is exactly /mcp.

    transport_security disables DNS-rebinding protection so remote hosts can
    connect. The jobs scheduler binds 0.0.0.0 by design (local-network MCP
    access), and the SDK enables a localhost-only policy by default whenever
    `transport_security` is left unset, which would reject those clients with
    421 Misdirected Request.

    Calling this also creates the server's StreamableHTTPSessionManager, so it
    must run before `mcp.session_manager` is touched (the 2.x SDK builds the
    manager here rather than in the MCPServer constructor).
    """
    return mcp.streamable_http_app(
        streamable_http_path="/",
        transport_security=TransportSecuritySettings(
            enable_dns_rebinding_protection=False,
        ),
    )


# ----------------------------------------------------------------------------
# submit_postgres_query

def _register_submit_postgres(
    mcp: MCPServer, cfg: JobsConfig, store: Store, targets: list[str]
) -> None:
    targets_csv = ", ".join(repr(t) for t in targets)

    @mcp.tool(
        name="submit_postgres_query",
        description=(
            "Queue a SQL SELECT for execution against a PostgreSQL target. "
            "Submit ONLY the SELECT statement — do not wrap it in "
            "`COPY (...) TO`, do not add a trailing semicolon/FORMAT clause, "
            "do not add INSERT/UPDATE/CTE-writes. The runner takes the "
            "submitted SELECT and wraps it as "
            "`COPY (<sql>) TO '/jobs_export/<job_id>/<output_filename>' "
            "WITH (FORMAT parquet|csv)` so the PG server writes the result "
            "file directly. The result folder contains one file named "
            "`<output_filename>`. "
            "`output_filename` must be a basename ending in `.parquet` or "
            "`.csv` (no slashes or path components). The extension decides "
            "the output format. "
            "Include a short `description` (1-2 sentences) explaining what "
            "the query is for — the human approver reads it to decide. "
            "Queries must be approved by a human in the web UI before they "
            "execute.\n\n"
            f"Configured targets: {targets_csv}."
        ),
    )
    def submit_postgres_query(
        sql: str,
        target: str,
        output_filename: str,
        description: str = "",
        overwrite: bool = False,
    ) -> dict[str, Any]:
        return _submit(
            store=store,
            cfg=cfg,
            backend="postgres",
            target=target,
            sql=sql,
            output_filename=output_filename,
            description=description,
            overwrite=overwrite,
        )


# ----------------------------------------------------------------------------
# submit_starrocks_query

def _register_submit_starrocks(
    mcp: MCPServer, cfg: JobsConfig, store: Store, targets: list[str]
) -> None:
    targets_csv = ", ".join(repr(t) for t in targets)

    @mcp.tool(
        name="submit_starrocks_query",
        description=(
            "Queue a SQL SELECT for execution against a StarRocks target. "
            "Submit ONLY the SELECT statement — do not append "
            "`INTO OUTFILE`, do not add a trailing semicolon, do not add "
            "FORMAT/PROPERTIES clauses, do not submit INSERT/CREATE. "
            "The runner takes the submitted SELECT and appends "
            "`INTO OUTFILE \"file:///jobs_export/<job_id>/<stem>_\" FORMAT "
            "AS PARQUET|CSV ...` (where <stem> is output_filename without "
            "its extension). SR chunks the output, so the result folder "
            "contains `<stem>_0.<ext>`, `<stem>_1.<ext>`, … (readable as a "
            "single dataset by pyarrow / polars / DuckDB). "
            "`output_filename` must be a basename ending in `.parquet` or "
            "`.csv` (no slashes or path components). The extension decides "
            "the output format. "
            "The SR target has no default database — queries must "
            "fully-qualify table names as `<database>.<table>`. "
            "Include a short `description` (1-2 sentences) explaining what "
            "the query is for — the human approver reads it to decide. "
            "Queries must be approved by a human in the web UI before they "
            "execute.\n\n"
            f"Configured targets: {targets_csv}."
        ),
    )
    def submit_starrocks_query(
        sql: str,
        target: str,
        output_filename: str,
        description: str = "",
        overwrite: bool = False,
    ) -> dict[str, Any]:
        return _submit(
            store=store,
            cfg=cfg,
            backend="starrocks",
            target=target,
            sql=sql,
            output_filename=output_filename,
            description=description,
            overwrite=overwrite,
        )


# ----------------------------------------------------------------------------
# submit_mongo_query

def _register_submit_mongo(
    mcp: MCPServer, cfg: JobsConfig, store: Store, targets: list[str]
) -> None:
    targets_csv = ", ".join(repr(t) for t in targets)

    @mcp.tool(
        name="submit_mongo_query",
        description=(
            "Queue a MongoDB aggregation for execution against a MongoDB "
            "target. A target is a Mongo node (not a single database) — "
            "agents always pass an explicit `database` on every submission. "
            "Call `list_mongo_databases(target)` first to discover what "
            "databases are available. "
            "Submit the aggregation ONLY as the `pipeline` argument (a JSON "
            "array of stages). Do NOT wrap it in `db.<collection>.aggregate(...)`, "
            "do NOT include shell helpers like `ObjectId()` / `ISODate()` "
            "(use extended-JSON literals if needed), do NOT include a "
            "cursor option object — the runner supplies `allowDiskUse=True` "
            "and `maxTimeMS` automatically. A plain `find()` is expressed "
            "as a single `$match` stage, optionally followed by "
            "`$project` / `$sort` / `$limit`. "
            "`collection` is the collection name (not a dotted path) within "
            "the chosen database. "
            "The runner streams the cursor output into "
            "`/data/jobs/results/<job_id>/<output_filename>`. "
            "`output_filename` must be a basename ending in `.ndjson` "
            "(safe default; lossless for any pipeline) or `.csv` (requires "
            "a terminal `$project` producing flat scalars — the runner "
            "fails fast on the first nested value or new key). "
            "Include a short `description` (1-2 sentences) explaining what "
            "the query is for — the human approver reads it to decide. "
            "Queries must be approved by a human in the web UI before they "
            "execute.\n\n"
            f"Configured targets: {targets_csv}."
        ),
    )
    def submit_mongo_query(
        target: str,
        database: str,
        collection: str,
        pipeline: list[dict],
        output_filename: str,
        description: str = "",
        overwrite: bool = False,
    ) -> dict[str, Any]:
        return _submit_mongo(
            store=store,
            cfg=cfg,
            target=target,
            collection=collection,
            pipeline=pipeline,
            output_filename=output_filename,
            database=database,
            description=description,
            overwrite=overwrite,
        )


# ----------------------------------------------------------------------------
# list_mongo_databases

def _register_list_mongo_databases(
    mcp: MCPServer, cfg: JobsConfig, runner: Runner, targets: list[str]
) -> None:
    targets_csv = ", ".join(repr(t) for t in targets)

    @mcp.tool(
        name="list_mongo_databases",
        description=(
            "List the databases visible to a MongoDB target. Use this to "
            "discover which database to pass as `database=` on "
            "`submit_mongo_query`. Internal databases (admin, config, "
            "local) are filtered out. Requires admin read access.\n\n"
            f"Configured targets: {targets_csv}."
        ),
    )
    def list_mongo_databases(target: str) -> dict[str, Any]:
        tgt = cfg.targets.get(target)
        if tgt is None or tgt.backend != "mongodb":
            return {
                "error": (
                    f"{target!r} is not a configured mongodb target; "
                    f"configured: {targets}"
                )
            }
        try:
            return {"databases": runner.list_mongo_databases(target)}
        except Exception as e:
            return {"error": f"{type(e).__name__}: {e}"}


# ----------------------------------------------------------------------------
# query_status

def _register_status_tool(mcp: MCPServer, store: Store) -> None:
    @mcp.tool(
        name="query_status",
        description=(
            "Return the current status of a queued query. Status values: "
            "pending, approved, running, done, failed, rejected, cancelled. "
            "When status=done, `result_path` points at a folder containing "
            "the result file(s)."
        ),
    )
    def query_status(job_id: str) -> dict[str, Any]:
        located = store.find(job_id)
        if not located:
            return {"job_id": job_id, "status": "unknown", "error": "job not found"}
        phase, job = located
        return _job_to_status(job, phase=phase)


# ----------------------------------------------------------------------------
# query_cancel

def _register_cancel_tool(mcp: MCPServer, store: Store) -> None:
    @mcp.tool(
        name="query_cancel",
        description=(
            "Cancel a pending job (one that has not yet been approved). "
            "Already-running jobs must be killed from the web UI — the "
            "scheduler does not expose a kill-via-MCP path."
        ),
    )
    def query_cancel(job_id: str) -> dict[str, Any]:
        located = store.find(job_id)
        if not located:
            return {"job_id": job_id, "status": "unknown", "error": "job not found"}
        phase, _ = located
        if phase != "pending":
            return {
                "job_id": job_id,
                "status": phase,
                "error": (
                    f"cannot cancel via MCP — job is in phase {phase!r}. "
                    "Only 'pending' jobs can be cancelled via MCP; running "
                    "jobs must be killed from the web UI."
                ),
            }
        job = store.cancel_pending(job_id)
        return _job_to_status(job, phase="history")


# ----------------------------------------------------------------------------
# list_targets

def _register_list_targets(mcp: MCPServer, cfg: JobsConfig) -> None:
    @mcp.tool(
        name="list_targets",
        description=(
            "List the configured scheduler targets and their backends. "
            "Returns `{targets: [{name, backend}, ...]}`. Use this for "
            "discovery instead of scraping the `submit_*` tool descriptions."
        ),
    )
    def list_targets() -> dict[str, Any]:
        return {
            "targets": [
                {"name": name, "backend": tgt.backend}
                for name, tgt in sorted(cfg.targets.items())
            ]
        }


# ----------------------------------------------------------------------------
# shared helpers

def _submit(
    *,
    store: Store,
    cfg: JobsConfig,
    backend: str,
    target: str,
    sql: str,
    output_filename: str,
    overwrite: bool,
    description: str = "",
) -> dict[str, Any]:
    tgt = cfg.targets.get(target)
    if tgt is None:
        return {
            "error": (
                f"unknown target {target!r}; "
                f"configured: {sorted(cfg.targets)}"
            )
        }
    if tgt.backend != backend:
        return {
            "error": (
                f"target {target!r} is backend={tgt.backend!r}, "
                f"not {backend!r}; use the matching submit tool"
            )
        }

    job = Job(
        job_id=store.new_job_id(backend),
        target=target,
        backend=backend,
        sql=sql,
        output_filename=output_filename,
        overwrite=bool(overwrite),
        submitted_at=time.time(),
        description=(description or "").strip(),
        status="pending",
    )
    try:
        validate_submission(job)
    except BackendError as e:
        return {"error": str(e)}
    try:
        store.submit(job)
    except OSError as e:
        return {"error": f"failed to queue job: {e}"}
    log.info("submitted job %s target=%s backend=%s", job.job_id, target, backend)
    return {"job_id": job.job_id, "status": "pending"}


def _submit_mongo(
    *,
    store: Store,
    cfg: JobsConfig,
    target: str,
    collection: str,
    pipeline: list[dict],
    output_filename: str,
    overwrite: bool,
    database: str = "",
    description: str = "",
) -> dict[str, Any]:
    tgt = cfg.targets.get(target)
    if tgt is None:
        return {
            "error": (
                f"unknown target {target!r}; "
                f"configured: {sorted(cfg.targets)}"
            )
        }
    if tgt.backend != "mongodb":
        return {
            "error": (
                f"target {target!r} is backend={tgt.backend!r}, not 'mongodb'; "
                "use the matching submit tool"
            )
        }
    if not isinstance(pipeline, list):
        return {"error": "pipeline must be a JSON array of aggregation stages"}
    if not collection or not isinstance(collection, str):
        return {"error": "collection must be a non-empty string"}

    resolved_db = (database or "").strip()
    if not resolved_db:
        return {
            "error": (
                "database is required for Mongo submissions; call "
                "list_mongo_databases(target) to see what's available."
            )
        }

    # Store the aggregation as pretty-printed JSON in `sql` so the UI can
    # render it and the backend can decode it. `collection` and `database`
    # are also set on the job record for display + execution.
    payload = json.dumps(
        {
            "collection": collection,
            "database": resolved_db,
            "pipeline": pipeline,
        },
        indent=2,
        default=str,
    )

    job = Job(
        job_id=store.new_job_id("mongodb"),
        target=target,
        backend="mongodb",
        sql=payload,
        output_filename=output_filename,
        overwrite=bool(overwrite),
        submitted_at=time.time(),
        description=(description or "").strip(),
        collection=collection,
        database=resolved_db,
        status="pending",
    )
    try:
        validate_submission(job)
    except BackendError as e:
        return {"error": str(e)}
    csv_check = _csv_pipeline_warning(output_filename, pipeline)
    if csv_check is not None:
        return {"error": csv_check}
    try:
        store.submit(job)
    except OSError as e:
        return {"error": f"failed to queue job: {e}"}
    log.info(
        "submitted mongo job %s target=%s db=%s collection=%s",
        job.job_id, target, resolved_db, collection,
    )
    return {"job_id": job.job_id, "status": "pending"}


# Stages whose output *can* be a flat-scalar document for CSV. A pipeline
# whose terminal stage is one of these is allowed through the submit-time
# pre-check; the runner's flat-scalars guard remains the source of truth and
# fires later if the actual output still has nested values.
_CSV_TERMINAL_STAGES: frozenset[str] = frozenset({
    "$project",
    "$replaceRoot",
    "$replaceWith",
    "$group",
    "$bucket",
    "$bucketAuto",
    "$sortByCount",
    "$count",
    "$facet",
    "$unset",
    "$addFields",  # may flatten by overwriting nested fields
    "$set",        # alias of $addFields
})


def _csv_pipeline_warning(
    output_filename: str, pipeline: list[dict]
) -> str | None:
    """If output is .csv, require the terminal stage to be one that *can* yield
    flat scalars. This catches the most common foot-gun (a bare ``$limit`` /
    ``$match`` / ``$sort`` with no projection) before it reaches the runner.
    Returns an error string for ``{"error": ...}``, or None to accept.
    """
    if not output_filename.lower().endswith(".csv"):
        return None
    if not pipeline:
        return (
            "CSV output requires a terminal stage that flattens the document "
            "(e.g. $project). Pipeline is empty — use .ndjson, or add a "
            "$project stage."
        )
    last = pipeline[-1]
    if not isinstance(last, dict) or not last:
        return (
            "CSV output requires a terminal stage that flattens the document "
            "(e.g. $project). Last stage is malformed — use .ndjson, or add "
            "a $project stage."
        )
    last_name = next(iter(last))
    if last_name not in _CSV_TERMINAL_STAGES:
        return (
            f"CSV output requires a terminal stage that flattens the "
            f"document (e.g. $project). Got {last_name!r} — use .ndjson, or "
            "add a $project stage at the end of the pipeline."
        )
    return None


def _job_to_status(job: Job, *, phase: str) -> dict[str, Any]:
    return {
        "job_id": job.job_id,
        "status": job.status,
        "target": job.target,
        "backend": job.backend,
        "submitted_at": job.submitted_at,
        "approved_at": job.approved_at,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
        "rows": job.rows,
        "size_bytes": job.size_bytes,
        "result_path": job.result_path,
        "error": job.error,
        "reject_reason": job.reject_reason,
        "phase": phase,
    }
