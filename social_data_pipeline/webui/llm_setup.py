"""The setup of the MCP servers and of the query scheduler from the browser, and the restart that applies a setup; `install(api, ctx)` adds the routes to the application of `app.create_app`.

- `GET /setup/mcp` and `GET /setup/queue`: the questions of `python sdp.py db setup-mcp` and `python sdp.py db setup-jobs` as one form each, filled with the answers the terminal offers as its defaults (`db.llm_settings`, which carries no secret). A form with the terminal's refusal (no database, a database with authentication and no read-only user or credentials file) shows the refusal instead.
- `POST /setup/mcp` and `POST /setup/queue`: the form, whose boxes "switch on" (`on_<database>`) and "add a target" (`add_<database>`) are the web UI's own: an unticked one drops what its server or target posted, a ticked one whose server or target posted nothing is refused. The rest goes through `actions.parse_form` and is kept as a pending action of the session; nothing is asked of the broker, and nothing is written before the confirm page's Confirm (`management`).
- `GET /restart?services=<a,b>` and `POST /restart`: the services that Needs attention names, stopped and started again one after the other through one confirm: `db.stop` then `db.start` (with the database admin password of this form, or the session's) of each, after the management password has been checked; each step runs only when the one before ended with exit code 0 (`management.run_steps`).

Every route answers GET or POST only, and a session the broker has ended is sent to the login page as on the other pages.
"""

import re

from fastapi import Request
from fastapi.responses import RedirectResponse

from social_data_pipeline.webui import actions, overview
from social_data_pipeline.webui.calls import BUSY_GATE, call_broker, session_ends, why

FORM_REFUSED = "The action was not taken: a field of the form was refused. Nothing was sent to the broker."
BUSY = "The web UI is busy with other requests: try again in a moment."
MCP_DATABASES = (("postgres", "PostgreSQL", True), ("mongo", "MongoDB", True), ("starrocks", "StarRocks", False))
TARGETS = (("postgres", "PostgreSQL", "postgres_target"), ("starrocks", "StarRocks", "starrocks_target"), ("mongo", "MongoDB", "mongo_target"))
RESTART_MAX = len(actions.SERVICES)


class ShapeError(Exception):
    """An answer of `db.llm_settings` of another shape."""


def _need(condition):
    if not condition:
        raise ShapeError()


def _int_or_none(value):
    _need(value is None or (isinstance(value, int) and not isinstance(value, bool)))
    return value


def _text_or_none(value):
    _need(value is None or isinstance(value, str))
    return value


def settings_model(result):
    """The forms' values from a `db.llm_settings` answer: `databases`, `refusal` (lines, or None), `mcp` ({database: enabled, port, write_access}) and `jobs` (the scheduler's numbers and `targets`). Raises ShapeError."""
    _need(isinstance(result, dict) and isinstance(result.get("data"), dict))
    data = result["data"]
    databases = data.get("databases")
    _need(isinstance(databases, list) and all(name in actions.DATABASES for name in databases))
    mcp = data.get("mcp")
    _need(isinstance(mcp, dict) and isinstance(mcp.get("servers"), dict))
    refusal = mcp.get("refusal")
    _need(refusal is None or (isinstance(refusal, list) and all(isinstance(line, str) for line in refusal)))
    model = {"databases": databases, "refusal": [overview.text(line) for line in refusal] if refusal else None, "mcp": {}, "jobs": None}
    for name, server in mcp["servers"].items():
        _need(name in databases and isinstance(server, dict) and isinstance(server.get("enabled"), bool))
        model["mcp"][name] = {"enabled": server["enabled"], "port": _int_or_none(server.get("port")), "write_access": server.get("write_access") is True}
    jobs = data.get("jobs")
    if jobs is not None:
        _need(isinstance(jobs, dict) and isinstance(jobs.get("targets"), dict) and isinstance(jobs.get("auth_enabled"), bool) and isinstance(jobs.get("configured"), bool))
        shown = {key: _int_or_none(jobs.get(key)) for key in ("port", "max_concurrent", "history_retention")}
        shown.update({"result_root": _text_or_none(jobs.get("result_root")), "auth_enabled": jobs["auth_enabled"], "configured": jobs["configured"], "targets": {}})
        for name, target in jobs["targets"].items():
            _need(name in databases and isinstance(target, dict))
            shown["targets"][name] = {"add": target.get("add") is True, "name": _text_or_none(target.get("name")), "database": _text_or_none(target.get("database")), "timeout_hours": _int_or_none(target.get("timeout_hours"))}
        model["jobs"] = shown
    return model


def _boxes(form, prefix, names):
    """Take the web UI's own boxes `<prefix><name>` out of the form: the names ticked. Raises actions.Refused for a box that posts anything but `yes`."""
    ticked = set()
    for name in names:
        value = form.pop(prefix + name, None)
        if value is None:
            continue
        if value != actions.CHECKED:
            raise actions.Refused()
        ticked.add(name)
    return ticked


def _drop(form, top):
    for key in [key for key in form if key == top or key.startswith(top + "[")]:
        del form[key]


def mcp_fields(form):
    """The fields of `db.setup_mcp` from the MCP form. Raises actions.Refused."""
    ticked = _boxes(form, "on_", [name for name, _, _ in MCP_DATABASES])
    for name, _, _ in MCP_DATABASES:
        if name not in ticked:
            _drop(form, name)
    fields = actions.parse_form(actions.TABLE["db.setup_mcp"], form)
    if any(name not in fields for name in ticked):
        raise actions.Refused(next(name for name, _, _ in MCP_DATABASES if name in ticked and name not in fields))
    return fields


def queue_fields(form):
    """The fields of `db.setup_jobs` from the scheduler's form. Raises actions.Refused."""
    ticked = _boxes(form, "add_", [name for name, _, _ in TARGETS])
    for name, _, key in TARGETS:
        if name not in ticked:
            _drop(form, key)
    fields = actions.parse_form(actions.TABLE["db.setup_jobs"], form)
    missing = [key for name, _, key in TARGETS if name in ticked and key not in fields]
    if missing:
        raise actions.Refused(missing[0])
    return fields


def restart_services(text):
    """The services of a restart from `services=a,b` (at most one of each, in the order given), or None when that is no such list."""
    if not isinstance(text, str) or not re.fullmatch(r"[a-z-]{1,200}", text.replace(",", "")) or len(text) > 200:
        return None
    services = text.split(",")
    if not services or len(services) > RESTART_MAX or len(set(services)) != len(services) or not all(service in actions.SERVICES for service in services):
        return None
    return services


def install(api, ctx):
    """Add the routes of this module to `api`, with what `ctx` (an `app.Context`) gives."""

    def refused(session, field_label=None, back=None):
        return ctx.frame("refused.html", "databases", session, 400, title="Not taken", line=FORM_REFUSED, field=field_label, back=back)

    async def settings_page(request, template, title, back):
        session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))
        answer = await ctx.pages.run(lambda: call_broker(lambda: ctx.broker(token=session.token, timeout=ctx.limits.read_seconds), lambda made: actions.call(made, "db.llm_settings", {})))
        if answer is BUSY_GATE:
            return ctx.frame("busy.html", "databases", session, 503, title=title, line=BUSY)
        kind, value = answer
        if kind == "error" and session_ends(value):
            ctx.sessions.forget(session_id)
            return ctx.to_login(request, why(value), clear_cookie=True)
        model, failure = None, None
        if kind == "ok":
            try:
                model = settings_model(value)
            except Exception:
                failure = {"sentence": overview.SHAPE_SENTENCE, "errors": None}
        else:
            failure = overview.failure(value)
        return ctx.frame(template, "databases", session, title=title, settings=model, failure=failure, back=back, mcp_databases=MCP_DATABASES, targets=TARGETS, table=actions.TABLE)

    async def make_pending(request, name, build, back):
        session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))
        form = dict(request.scope.get("sdp.form", {}))
        try:
            fields = build(form)
        except actions.Refused as error:
            inputs = actions.TABLE[name].inputs
            return refused(session, inputs[error.field].label if error.field in inputs else None, (back, "Back to the form"))
        pending = session.work.add_pending(name, fields, ctx.clock())
        return RedirectResponse(f"/pending/{pending.id}", status_code=303)

    @api.get("/setup/mcp")
    async def mcp_page(request: Request):
        return await settings_page(request, "setup_mcp.html", "Set up the MCP servers", "/databases?tab=mcp")

    @api.post("/setup/mcp")
    async def mcp_post(request: Request):
        return await make_pending(request, "db.setup_mcp", mcp_fields, "/setup/mcp")

    @api.get("/setup/queue")
    async def queue_page(request: Request):
        return await settings_page(request, "setup_queue.html", "Set up the query scheduler", "/databases?tab=queue")

    @api.post("/setup/queue")
    async def queue_post(request: Request):
        return await make_pending(request, "db.setup_jobs", queue_fields, "/setup/queue")

    @api.get("/restart")
    async def restart_page(request: Request):
        session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))
        services = restart_services(request.query_params.get("services"))
        if services is None:
            return refused(session)
        return ctx.frame("restart.html", "overview", session, title="Restart to apply", services=services, joined=",".join(services), db_password_kept=session.work.db_password_kept)

    @api.post("/restart")
    async def restart_post(request: Request):
        session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))
        form = dict(request.scope.get("sdp.form", {}))
        services = restart_services(form.pop("services", None))
        password = form.pop("admin_password", "")
        if services is None or form:
            return refused(session)
        steps = []
        try:
            for service in services:
                steps.append(("db.stop", actions.check_fields(actions.TABLE["db.stop"], {"service": service})))
                start = {"service": service, **({"admin_password": actions.Hidden(password)} if password else {})}
                steps.append(("db.start", actions.check_fields(actions.TABLE["db.start"], start)))
        except actions.Refused:
            return refused(session, "Database admin password")
        (first, first_fields), then = steps[0], steps[1:]
        pending = session.work.add_pending(first, first_fields, ctx.clock(), step_up=True, then=then)
        return RedirectResponse(f"/pending/{pending.id}", status_code=303)
