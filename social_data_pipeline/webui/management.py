"""The Management pages besides the Overview, the way an action travels from a form to its outcome, and the output view of a run; `install(api, context)` adds their routes to the application of `app.create_app`.

Pages, each read in one worker of the pages' bound, each block failing on its own:

- `GET /sources` and `GET /sources/<name>` (a plain name, else not found): the rail of sources (`source.list`) and one source, the one named or the first with a plain name: its ingestion state (`source.status`), the finished files per stage (`source.progress`), the runs (`run.list`, for the stages that run now), the error logs (`source.error_logs`, once per profile of the source among `postgres_ingest`, `postgres_ml`, `mongo_ingest`; one too large to send is answered with the terminal command to use instead), the pipeline drawn from them (`pipeline_model`), one small form per profile the source has, and "Remove…". `?tab=` picks the tab shown (Pipeline, Configuration, Errors); every tab is rendered, the others hidden, so each read still fails in one place only.
- `GET /databases` and `GET /databases/<database>` (one of the three, else not found): `db.status` and `db.verify` per database (`?tab=`: Servers, MCP servers, Queue setup, Verify); the forms to start and stop services, to reset and remove a database, to remove the MCP servers' and the scheduler's configuration, and to keep or forget the database admin password for the session.
- `GET /runs`: `run.list`. `GET /runs/<run id>` (either form of a run id, else not found; `?offset=` where a view starts): the run's facts, a cancel form while it is alive, and the output view.
- `GET /system`: the broker's `hello`, the newest 200 lines of the audit log, the table of actions (`?tab=`: Broker and verbs, Audit log).

An action, the same for every state-changing one (the Queue's actions too, with what `queue` adds through its `Hooks`: how their pending action is made and shown, and the thread that runs them, which asks the broker whether the session lives before it asks the scheduler): `POST /pending` with the form (`action` names the table's entry) is checked against the table without asking the broker and kept as a pending action of the session (`work.SessionWork`), answered with a redirect to `GET /pending/<id>`, which shows the `sdp` command, every input (a secret as dots), what a destructive verb destroys, and for a step-up entry a field for the management password. `POST /pending/<id>/confirm` carries that password or nothing; the pending action is taken out of the session before anything is awaited, so two confirms at once start one call. The call runs in a thread of its own, not tied to the request, under a bound of its own (`Limits.action_workers`; a cancel under `Limits.cancel_workers`, so that it gets through while long actions run; "busy" when full), through a client WITHOUT a time limit, and writes its outcome; the confirm waits for it up to `Limits.action_wait_seconds`: a refusal that came at once is shown on the pending page (`step_up_failed` and `step_up_required` put the action back, `unauthorized` and `session_ended` end the session), anything else is a redirect to `GET /actions/<id>`, which shows "under way" (an htmx poll every 2 seconds) and then the outcome. A broker that cannot be reached is an outcome there, not a logout.

The output view (`GET /runs/<run id>/output?offset=<n>&view=<id>`, an htmx partial; see `output`): each answer carries the next request itself. Pieces of log files go through a bound of their own (`Limits.piece_workers`) and tails of Docker through another (`Limits.tail_workers`), apart from the pages', and are answered "busy, trying again" when theirs is full. A request for an offset below what the view has shown appends nothing and stops the view; a view that stops on an error shows its unfinished line first. An htmx poll of an outcome that is no longer kept gets a part that says so and polls no more. A request of htmx whose session has ended gets an empty answer with `HX-Redirect` to the login page. Every route here answers GET only (a HEAD is 405): a GET of a run page makes an output view's state, and a HEAD must change nothing.
"""

import logging
import math
import re
import threading
import time
from functools import partial

import anyio
from fastapi import Request
from fastapi.responses import PlainTextResponse, RedirectResponse

from social_data_pipeline.webui import actions, output, overview, pages, queue
from social_data_pipeline.webui.calls import BUSY_GATE, SESSION_ENDING_CODES, call_broker, session_ends, why
from social_data_pipeline.webui.work import ID_PATTERN, VIEW_ID_PATTERN, Outcome, View, new_id

BUSY = "The web UI is busy with other requests: try again in a moment."
ACTIONS_BUSY = "The web UI already runs as many actions as it runs at once: confirm again when one of them has ended."
STEP_UP_MISSING = "Type the management password to confirm this action."
CONFIRM_REFUSED = "The confirmation carried a field it does not take."
FORM_REFUSED = "The action was not taken: a field of the form was refused. Nothing was sent to the broker."
VIEW_CLOSED = "This output view was closed because newer ones were opened in this session: reload the page to follow the output again."
VIEW_BEHIND = "This request asked for output this view has already shown, so nothing was added: reload the page to follow the output again."
OUTCOME_GONE = "This outcome is no longer kept: the session keeps the newest 32. The action itself was not changed by that."
OUTPUT_BUSY = "The web UI is busy: trying again in a moment."
NOT_FOUND = "Not found.\n"
OFFSET_PATTERN = r"[0-9]{1,16}"
RETRY_CODES = ("step_up_failed", "step_up_required")
# The title of a pending restart and of its outcome (a db.stop with the steps that follow it).
RESTART_TITLE = "Restart to apply"


def _title(action, then):
    """The title of a pending action or an outcome: a restart's own, else the table entry's."""
    return RESTART_TITLE if action.name == "db.stop" and then else action.title
RUN_PAGE_LIST_LIMIT = 1000
AUDIT_LINES = 200
# The tabs of a source's page, of the Databases page and of the System page (`?tab=`); the first is the one shown without one.
SOURCE_TABS = ("pipeline", "configuration", "errors")
DATABASE_TABS = ("servers", "mcp", "queue", "verify")
SYSTEM_TABS = ("broker", "audit")
# The "Load into databases" lanes of a source's pipeline: each database and its profiles, and which view of the ingestion state (`pages.INGESTION_NAMES`) holds each profile's state files, with the start of their labels when the view holds two profiles.
LANES = (("StarRocks", (("sr_ingest", "StarRocks", ""), ("sr_ml", "StarRocks classifiers", ""))), ("MongoDB", (("mongo_ingest", "MongoDB", "ingest_"),)), ("PostgreSQL", (("postgres_ingest", "PostgreSQL", "ingest_"), ("postgres_ml", "PostgreSQL", "ml_"))))
# The Management page each action belongs to.
ACTIVE = {"run.start": "sources", "source.remove": "sources", "run.cancel": "runs", "source.add": "sources", "source.configure": "sources", "source.add_classifiers": "sources"}

log = logging.getLogger("social_data_pipeline.webui")


def _not_found():
    return PlainTextResponse(NOT_FOUND, status_code=404)


def _offset(value, default=0):
    """An offset from a query: the default when absent, None when it is not 1 to 16 digits of at most 2**53."""
    if value is None:
        return default
    if not re.fullmatch(OFFSET_PATTERN, value) or int(value) > actions.LOG_OFFSET_MAX:
        return None
    return int(value)


def _is_htmx(request):
    return request.headers.get("hx-request") == "true"


def output_href(run_id, offset, view_id):
    return f"/runs/{run_id}/output?offset={offset}&view={view_id}"


def pending_model(pending, now, seconds):
    """What the pending page shows of a pending action."""
    action = actions.TABLE[pending.action]
    fields = pending.fields or {}
    return {
        "id": pending.id,
        "title": _title(action, pending.then),
        "verb": action.name,
        "command": action.command(fields) if action.command else None,
        "no_command": action.form if action.command is None else None,
        "inputs": actions.shown_inputs(action, fields),
        "destroys": action.destroys(fields) if action.destroys else None,
        "step_up": pending.step_up,
        "terminal_asks": action.terminal_asks,
        "note": action.note,
        "also": action.also(fields) if action.also else None,
        "warns": action.warns(fields) if action.warns else None,
        "then": _then_model(pending),
        "writes": action.writes(fields) if action.writes else None,
        # The statements a page worked out when it kept the action (a tuple of lines; the index build's), shown as they are.
        "preview": pending.shown if isinstance(pending.shown, tuple) else None,
        "changes": None,
        # A setup's change keeps its own lines and the list of what changes, made with the values it started from (`setup_db`).
        **({key: pending.shown[key] for key in ("writes", "changes") if key in pending.shown} if isinstance(pending.shown, dict) and pending.action == "db.setup" else {}),
        "minutes_left": max(1, math.ceil((seconds - (now - pending.created)) / 60)),
        "queue": False,
    }


def _then_model(pending):
    """What the pending page says of the steps that follow (`Pending.then`), in order: each one's command and what else it does, or None."""
    if not pending.then:
        return None
    steps = []
    for name, fields in pending.then:
        action = actions.TABLE[name]
        steps.append({"title": action.title, "command": action.command(fields), "also": action.also(fields) if action.also else None})
    return steps


def outcome_model(outcome):
    """What the outcome page shows of an outcome."""
    action = actions.TABLE[outcome.action]
    return {
        "id": outcome.id,
        "title": _title(action, outcome.then),
        "command": outcome.command,
        "running": not outcome.done,
        "kind": outcome.kind,
        "exit_code": outcome.exit_code,
        "output": outcome.output,
        "errors": outcome.errors,
        "run_href": "/runs/" + outcome.run_id if outcome.run_id and actions.is_run_id(outcome.run_id) else None,
        "run_id": outcome.run_id,
        "run_label": outcome.run_label,
        "sentence": outcome.sentence,
        "field": outcome.field,
        "op": action.op,
        "gone": None,
        "queue": action.door is not None,
        "verb": action.name,
        "door": outcome.door,
        "back": outcome.back,
        "then": [dict(step, run_href="/runs/" + step["run_id"] if step["run_id"] and actions.is_run_id(step["run_id"]) else None) for step in outcome.then] if outcome.then else None,
    }


def _first_ending(answers):
    return next((value for kind, value in answers.values() if kind == "error" and session_ends(value)), None)


def install(api, ctx, queue_hooks):
    """Add the routes of this module to `api`, with what `ctx` (an `app.Context`) gives; the Queue's actions go through `queue_hooks` (`queue.Hooks`)."""
    limits = ctx.limits

    def is_queue(name):
        return actions.TABLE[name].door is not None

    def active_of(name):
        return queue_hooks.active(name) if is_queue(name) else ACTIVE.get(name, "databases")

    def asker(session):
        def ask(name, fields=None):
            return call_broker(lambda: ctx.broker(token=session.token, timeout=limits.read_seconds), lambda made: actions.call(made, name, fields or {}))

        return ask

    def plan_of(*requests):
        """A plan that asks (key, name, fields) one after the other and stops once the session has ended."""

        def plan(ask):
            answers = {}
            for key, name, fields in requests:
                answers[key] = ask(name, fields)
                if answers[key][0] == "error" and session_ends(answers[key][1]):
                    break
            return answers

        return plan

    async def page(request, active, title, template, plan, build):
        session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))
        answers = await ctx.pages.run(lambda: plan(asker(session)))
        if answers is BUSY_GATE:
            return ctx.frame("busy.html", active, session, 503, title=title, line=BUSY)
        ending = _first_ending(answers)
        if ending is not None:
            ctx.sessions.forget(session_id)
            return ctx.to_login(request, why(ending), clear_cookie=True)
        return ctx.frame(template, active, session, title=title, **build(session, answers))

    # ── sources ──

    def source_plan(chosen):
        """The reads of a Sources page: the list (the rail), then for the source `chosen(list answer)` names (None: none) its state, finished files, the runs and its error logs, stopping once the session has ended."""

        def plan(ask):
            answers = {"list": ask("source.list", {})}
            if answers["list"][0] == "error" and session_ends(answers["list"][1]):
                return answers
            name = chosen(answers["list"])
            if name is None:
                return answers
            answers["name"] = ("ok", name)
            for key, verb, fields in (("status", "source.status", {"name": name}), ("progress", "source.progress", {"name": name}), ("runs", "run.list", {})):
                answers[key] = ask(verb, fields)
                if answers[key][0] == "error" and session_ends(answers[key][1]):
                    return answers
            for profile in _error_log_profiles(answers["status"]):
                answers["errors:" + profile] = ask("source.error_logs", {"name": name, "profile": profile})
                if answers["errors:" + profile][0] == "error" and session_ends(answers["errors:" + profile][1]):
                    break
            return answers

        return plan

    def source_build(request):
        def build(session, answers):
            listed = pages.build(answers, {"list": overview.sources_block})["list"]
            tab = request.query_params.get("tab")
            model = {"block": listed, "source_name": None, "tab": tab if tab in SOURCE_TABS else SOURCE_TABS[0], "tabs": SOURCE_TABS}
            if "name" not in answers:
                model["rail"] = overview.grid(listed, {"ok": False})
                return model
            name = answers["name"][1]
            makers = {"status": pages.source_status_block, "progress": pages.source_progress_block, "runs": pages.runs_block}
            profiles = [key.split(":", 1)[1] for key in answers if key.startswith("errors:")]
            makers.update({"errors:" + profile: partial(pages.error_logs_block, profile=profile) for profile in profiles})
            blocks = pages.build(answers, makers)
            status, runs = blocks["status"], blocks["runs"]
            alive = {"ok": runs["ok"], "rows": [row for row in runs["rows"] if row["alive"]] if runs["ok"] else []}
            forms = None
            if status["ok"] and status["found"]:
                forms = [{"profile": profile, "inputs": actions.run_form_inputs(profile)} for profile in status["run_profiles"]]
            error_logs = [{"profile": profile, "block": blocks["errors:" + profile], "command": f"python sdp.py source error-logs {name} --profile={profile}"} for profile in profiles]
            failed = sum(len(state_file["failed"]) for entry in error_logs if entry["block"]["ok"] for state_file in entry["block"]["files"])
            model.update({
                "source_name": name, "source_status": status, "progress": blocks["progress"], "error_logs": error_logs, "failed_files": failed, "run_forms": forms,
                "db_password_kept": session.work.db_password_kept, "database_profiles": actions.DATABASE_PROFILES,
                "rail": overview.grid(listed, alive), "pipeline": pipeline_model(name, status, answers.get("progress"), alive),
            })
            return model

        return build

    @api.get("/sources")
    async def sources_page(request: Request):
        def first_plain(answer):
            try:
                rows = overview.sources_block(answer[1])["rows"] if answer[0] == "ok" else []
            except Exception:
                return None
            return next((row["name"] for row in rows if row["href"]), None)

        return await page(request, "sources", "Sources", "source.html", source_plan(first_plain), source_build(request))

    @api.get("/sources/{name}")
    async def source_page(request: Request, name: str):
        if not actions.is_plain_name(name):
            return _not_found()
        return await page(request, "sources", f"Source {name}", "source.html", source_plan(lambda answer: name), source_build(request))

    # ── databases ──

    def databases_build(only, request):
        def build(session, answers):
            blocks = pages.build(answers, {"status": pages.databases_status_block, "verify": pages.databases_verify_block})
            status, verify = blocks["status"], blocks["verify"]
            sections = []
            if status["ok"]:
                for row in status["rows"]:
                    if only is not None and row["db"] != only:
                        continue
                    result = verify["results"].get(row["db"]) if verify["ok"] and row["db"] else None
                    sections.append({"row": row, "verify": result})
            tab = request.query_params.get("tab")
            return {"db_status": status, "db_verify": verify, "sections": sections, "only": only, "services": actions.SERVICES, "databases": actions.DATABASES, "db_password_kept": session.work.db_password_kept, "tab": tab if tab in DATABASE_TABS else DATABASE_TABS[0]}

        return build

    @api.get("/databases")
    async def databases_page(request: Request):
        return await page(request, "databases", "Databases", "databases.html", plan_of(("status", "db.status", {}), ("verify", "db.verify", {})), databases_build(None, request))

    @api.get("/databases/{name}")
    async def database_page(request: Request, name: str):
        if name not in actions.DATABASES:
            return _not_found()
        return await page(request, "databases", f"Database {name}", "databases.html", plan_of(("status", "db.status", {}), ("verify", "db.verify", {})), databases_build(name, request))

    # ── runs ──

    @api.get("/runs")
    async def runs_page(request: Request):
        def build(session, answers):
            return {"block": pages.build(answers, {"runs": pages.runs_block})["runs"]}

        return await page(request, "runs", "Runs", "runs.html", plan_of(("runs", "run.list", {})), build)

    @api.get("/runs/{run_id}")
    async def run_page(request: Request, run_id: str):
        if not actions.is_run_id(run_id):
            return _not_found()
        offset = _offset(request.query_params.get("offset"))
        if offset is None:
            return _not_found()

        def build(session, answers):
            block = pages.build(answers, {"runs": partial(pages.run_facts_block, run_id=run_id)})["runs"]
            view_id = session.work.add_view(View(run_id=run_id, offset=offset))
            return {"run_id": run_id, "block": block, "view_id": view_id, "first_href": output_href(run_id, offset, view_id), "offset": offset}

        return await page(request, "runs", f"Run {run_id}", "run.html", plan_of(("runs", "run.list", {"limit": RUN_PAGE_LIST_LIMIT})), build)

    @api.get("/runs/{run_id}/output")
    async def run_output(request: Request, run_id: str):
        if not actions.is_run_id(run_id):
            return _not_found()
        offset_text = request.query_params.get("offset")
        offset = _offset(offset_text, None) if offset_text is not None else None
        view_id = request.query_params.get("view")
        if offset is None or not isinstance(view_id, str) or not re.fullmatch(VIEW_ID_PATTERN, view_id):
            return _not_found()
        session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))
        view = session.work.view(view_id, run_id)
        if view is None:
            return ctx.render("output.html", text="", preview="", end=VIEW_CLOSED, continue_href=None, errors=None, next_href=None, trigger=None)
        if view.mode != "tail" and offset < view.offset:
            # A request older than what the view shows (a stale tab, one made by hand): it would append again what is shown.
            return ctx.render("output.html", text="", preview="", end=VIEW_BEHIND, continue_href=None, errors=None, next_href=None, trigger=None)
        tail = run_id.startswith("live-") or view.mode == "tail"
        ask = asker(session)
        answer = await (ctx.tails if tail else ctx.pieces).run(lambda: ask("run.logs", {"run_id": run_id, "offset": offset, "limit": limits.log_piece_bytes}))
        same = output_href(run_id, offset, view_id)
        if answer is BUSY_GATE:
            return ctx.render("output.html", text="", preview=OUTPUT_BUSY, end=None, continue_href=None, errors=None, next_href=same, trigger=output.BUSY_TRIGGER)
        kind, value = answer
        if kind == "error":
            if session_ends(value):
                ctx.sessions.forget(session_id)
                return ctx.to_login(request, why(value), clear_cookie=True)
            if getattr(value, "code", None) == "busy":
                return ctx.render("output.html", text="", preview=OUTPUT_BUSY, end=None, continue_href=None, errors=None, next_href=same, trigger=output.BUSY_TRIGGER)
            shown = overview.failure(value)
            return ctx.render("output.html", text=output.flush(view), preview="", end=shown["sentence"], continue_href=None, errors=shown["errors"], next_href=None, trigger=None)
        try:
            data = value["data"] if isinstance(value, dict) else None
            step = output.advance(view, offset, data, limit=limits.log_piece_bytes, held_max=limits.view_held_bytes, shown_max=limits.view_shown_bytes)
        except output.ShapeError:
            return ctx.render("output.html", text=output.flush(view), preview="", end=pages.SHAPE_SENTENCE, continue_href=None, errors=None, next_href=None, trigger=None)
        headers = {"HX-Retarget": f"#view-{view_id}", "HX-Reswap": "innerHTML"} if step.replace else None
        return ctx.render(
            "output.html", headers=headers,
            text=step.text, preview=step.preview, end=step.end, errors=None,
            continue_href=f"/runs/{run_id}?offset={step.continue_offset}" if step.continue_offset is not None else None,
            next_href=output_href(run_id, step.offset, view_id) if step.trigger else None, trigger=step.trigger,
        )

    # ── system ──

    @api.get("/system")
    async def system_page(request: Request):
        def build(session, answers):
            blocks = pages.build(answers, {"hello": pages.hello_block, "audit": pages.audit_block})
            table = [{"name": action.name, "cls": action.cls, "title": action.title, "form": action.form} for action in actions.TABLE.values()]
            tab = request.query_params.get("tab")
            return {"hello": blocks["hello"], "audit": blocks["audit"], "table": table, "tab": tab if tab in SYSTEM_TABS else SYSTEM_TABS[0]}

        return await page(request, "system", "System", "system.html", plan_of(("hello", "hello", {}), ("audit", "audit.list", {"limit": AUDIT_LINES})), build)

    # ── actions ──

    async def pending_page(session, pending, status=200, line=None, refusal=None):
        """The pending page; a Queue action's is rendered in a worker (its statement can be a megabyte), from the model made once when it was kept."""
        if is_queue(pending.action):
            model = queue_hooks.pending_model(pending, ctx.clock(), limits.pending_seconds)
            return await queue_hooks.render_pending(lambda: ctx.frame("pending.html", active_of(pending.action), session, status, title=model["title"], pending=model, line=line, refusal=refusal))
        model = pending_model(pending, ctx.clock(), limits.pending_seconds)
        return ctx.frame("pending.html", active_of(pending.action), session, status, title=model["title"], pending=model, line=line, refusal=refusal)

    @api.post("/pending")
    async def make_pending(request: Request):
        session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))
        form = dict(request.scope.get("sdp.form", {}))
        name = form.pop("action", None)
        action = actions.TABLE.get(name) if isinstance(name, str) else None
        if action is None or not action.state_changing or name == "session.check_password":
            return ctx.frame("refused.html", "overview", session, 400, title="Not taken", line=FORM_REFUSED, field=None)
        if action.door is not None:
            return await queue_hooks.make_pending(request, session, name, form)
        try:
            fields = actions.parse_form(action, form)
        except actions.Refused as refused:
            label = action.inputs[refused.field].label if refused.field in action.inputs else None
            return ctx.frame("refused.html", ACTIVE.get(name, "databases"), session, 400, title="Not taken", line=FORM_REFUSED, field=label)
        pending = session.work.add_pending(name, fields, ctx.clock())
        return RedirectResponse(f"/pending/{pending.id}", status_code=303)

    @api.get("/pending/{pending_id}")
    async def show_pending(request: Request, pending_id: str):
        if not re.fullmatch(ID_PATTERN, pending_id):
            return _not_found()
        session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))
        pending = session.work.pending(pending_id, ctx.clock())
        if pending is None:
            return _not_found()
        if is_queue(pending.action):
            # A session the broker ended does not see a Queue action's page (the job's record) any more.
            stopped = await queue_hooks.guard(request, session_id, session)
            if stopped is not None:
                return stopped
        return await pending_page(session, pending)

    def places_of(name):
        """The bound an action runs under: a cancel has its own, so that it gets through while long actions hold the others (as at the broker, where it takes no lock)."""
        return ctx.cancel_places if name == "run.cancel" else ctx.action_places

    def take_place(name):
        if is_queue(name):
            return queue_hooks.places.take()
        return places_of(name).acquire(blocking=False)

    def give_place(name):
        if is_queue(name):
            queue_hooks.places.give_back()
        else:
            places_of(name).release()

    def start_action(session, name, fields, step_up, outcome, then=None):
        """Run the call in a thread of its own, through a client without a time limit; the thread writes the outcome and gives the place back. With `then` (the steps of `Pending.then`) the call and its steps run as one chain (`run_chain`)."""
        token, work = session.token, session.work
        places = places_of(name)
        if then:
            outcome.expect_then([(step, actions.TABLE[step].command(step_fields)) for step, step_fields in then])
        # The chain's own copy of the steps: each one's fields are let go once its request was sent; the pending action keeps `then` only until the confirm has answered (or keeps it whole when it is put back for a retry).
        chain = [[name, fields], *([step, step_fields] for step, step_fields in then or ())]

        def run():
            try:
                def ask(call):
                    return call_broker(lambda: ctx.broker(token=token, timeout=None), call)

                if then:
                    run_chain(ask, outcome, chain, step_up, limits.outcome_text_bytes)
                    return
                kind, value = ask(lambda made: actions.call(made, name, fields, step_up))
                outcome.finish(kind, value, limits.outcome_text_bytes)
                if kind == "ok" and name == "session.set_db_password":
                    work.db_password_kept = True
                elif kind == "ok" and name == "session.clear_db_password":
                    work.db_password_kept = False
            finally:
                chain.clear()
                if outcome.then is not None and not outcome.done:
                    outcome.end_steps()
                places.release()

        threading.Thread(target=run, name="sdp-webui-action", daemon=True).start()

    async def queue_busy(request, session_id, session, pending, line):
        """A confirmation of a Queue action that could not start: after the session's check at the broker, only the line and the way back to the pending page, never the job's record."""
        stopped = await queue_hooks.guard(request, session_id, session)
        if stopped is not None:
            return stopped
        return ctx.frame("refused.html", active_of(pending.action), session, 503, title="Busy", line=line, field=None, back=(f"/pending/{pending.id}", "Back to the pending action"))

    @api.post("/pending/{pending_id}/confirm")
    async def confirm(request: Request, pending_id: str):
        if not re.fullmatch(ID_PATTERN, pending_id):
            return _not_found()
        session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))
        work = session.work
        form = dict(request.scope.get("sdp.form", {}))
        password = form.pop("management_password", None)
        pending = work.pending(pending_id, ctx.clock())
        if pending is None:
            return _not_found()
        step_up = pending.step_up
        refused_line = CONFIRM_REFUSED if form or (not step_up and password is not None) else STEP_UP_MISSING if step_up and not password else None
        if refused_line is not None:
            if is_queue(pending.action):
                # The page shown again holds the job's record: not for a session the broker has ended.
                stopped = await queue_hooks.guard(request, session_id, session)
                if stopped is not None:
                    return stopped
            return await pending_page(session, pending, 400, refused_line)
        # Taken out in one step before anything is awaited: a second confirm finds nothing.
        taken = work.take_pending(pending_id, ctx.clock())
        if taken is None:
            return _not_found()
        queue_action = is_queue(taken.action)
        if not take_place(taken.action):
            work.put_back(taken, ctx.clock())
            if queue_action:
                return await queue_busy(request, session_id, session, taken, queue.QUEUE_BUSY)
            return await pending_page(session, taken, 503, ACTIONS_BUSY)
        command = actions.TABLE[taken.action].command(taken.fields) if actions.TABLE[taken.action].command else None
        outcome = Outcome(new_id(), taken.action, command, ctx.clock())
        if taken.action == "db.setup" and isinstance(taken.shown, dict):
            outcome.back = taken.shown.get("back")
        work.add_outcome(outcome)
        try:
            if queue_action:
                queue_hooks.start(session, taken, password if step_up else None, outcome)
            else:
                start_action(session, taken.action, taken.fields, password if step_up else None, outcome, then=taken.then)
        except Exception as error:
            give_place(taken.action)
            work.remove_outcome(outcome.id)
            work.put_back(taken, ctx.clock())
            log.error("an action could not be started (%s)", type(error).__name__)
            if queue_action:
                return await queue_busy(request, session_id, session, taken, ACTIONS_BUSY)
            return await pending_page(session, taken, 503, ACTIONS_BUSY)
        password = None
        deadline = time.monotonic() + limits.action_wait_seconds
        while not outcome.done and time.monotonic() < deadline:
            await anyio.sleep(0.02)
        if outcome.done and outcome.kind == "refusal":
            # Only the broker's codes end the session or put the action back; the scheduler's are outcomes.
            if outcome.code in SESSION_ENDING_CODES and not outcome.from_door:
                ctx.sessions.forget(session_id)
                return ctx.to_login(request, "ended", clear_cookie=True)
            if outcome.code in RETRY_CODES and not outcome.from_door:
                work.remove_outcome(outcome.id)
                work.put_back(taken, ctx.clock())
                return await pending_page(session, taken, 403, outcome.sentence)
            if queue_action and outcome.at_session_check:
                # The broker did not confirm the session (unreachable, internal, stopping...): its sentence and the way back, never the job's record; the action is kept for a retry.
                work.remove_outcome(outcome.id)
                work.put_back(taken, ctx.clock())
                work.broker_checked_at = None
                return ctx.frame("refused.html", active_of(taken.action), session, 503, title="Not done", line=outcome.sentence, field=None, back=(f"/pending/{taken.id}", "Back to the pending action"))
            response = await pending_page(session, taken, 200, None, outcome_model(outcome))
            taken.fields = None
            taken.shown = None
            taken.then = None
            return response
        taken.fields = None
        taken.shown = None
        taken.then = None
        return RedirectResponse(f"/actions/{outcome.id}", status_code=303)

    @api.get("/actions/{outcome_id}")
    async def show_outcome(request: Request, outcome_id: str):
        if not re.fullmatch(ID_PATTERN, outcome_id):
            return _not_found()
        session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))
        outcome = session.work.outcome(outcome_id)
        if outcome is None and _is_htmx(request):
            # The poll of an outcome page whose outcome was evicted: htmx swaps no 404, so the part says so and asks no more.
            return ctx.render("_outcome.html", outcome={"id": outcome_id, "gone": OUTCOME_GONE, "running": False})
        if outcome is None:
            return _not_found()
        if outcome.done and outcome.kind == "refusal" and outcome.code in SESSION_ENDING_CODES and not outcome.from_door:
            ctx.sessions.forget(session_id)
            return ctx.to_login(request, "ended", clear_cookie=True)
        model = outcome_model(outcome)
        if _is_htmx(request):
            return ctx.render("_outcome.html", outcome=model)
        return ctx.frame("outcome.html", active_of(outcome.action), session, title=model["title"], outcome=model)


def run_chain(ask, outcome, chain, password, text_max_bytes):
    """Run an action and the steps that follow it (`chain`: [action, fields] lists, the action first) through `ask(call)` (`calls.call_broker` with a client), writing `outcome`: the action's answer with `finish`, each step's with `finish_step`. With a management password, when a step-up step follows an action that is not step-up itself (a restart: db.stop, then db.start), the password is checked first (`session.check_password`) so that nothing runs with a wrong one; when the action is step-up, its own call checks it before it does anything. A step-up step gets the password, the others none. Each step runs only when the one before ended with exit code 0 (or started its run); a step's fields are let go once its request was sent. Ends with `end_steps`, which says each step not run was not started."""
    try:
        if password is not None and actions.TABLE[chain[0][0]].cls != "step_up":
            kind, value = ask(lambda made: actions.call(made, "session.check_password", {}, password))
            if kind != "ok":
                outcome.finish(kind, value, text_max_bytes)
                return
        for index, step in enumerate(chain):
            name, fields = step
            given = password if actions.TABLE[name].cls == "step_up" else None
            kind, value = ask(lambda made, name=name, fields=fields, given=given: actions.call(made, name, fields, given))
            step[1] = fields = None
            if index == 0:
                outcome.finish(kind, value, text_max_bytes)
                carry_on = outcome.succeeded
            else:
                carry_on = outcome.finish_step(index - 1, kind, value, text_max_bytes)
            if not carry_on:
                return
    finally:
        outcome.end_steps()


def _number(value):
    """A count of the ingestion state as the page shows it (text), or None when it is no plain count."""
    return int(value) if isinstance(value, str) and value.isdigit() and len(value) <= 12 else None


def pipeline_model(name, status, progress_answer, alive):
    """The Pipeline tab of a source: `prepare` (the dumps folder, then the nodes of parse, lingua and ml) and `lanes` (per database, the nodes of its profiles). A node: `profile`, `state` (`run` while a run of it on this source is alive, `fail` when its state files name failed files, `setup` when the source has the profile, `none`), `dir`, `line` (what the reads say of it, or None), `title`, `href` (its run form on this page) and `run_href` (the page of its alive run). Only the reads' own figures are shown: the finished files of `source.progress` for parse, lingua and ml, the loaded and failed files of the state files for the loads."""
    found = status["ok"] and status["found"]
    run_profiles = status["run_profiles"] if found else []
    paths = dict(status["paths"]) if found else {}
    running = {row["profile"]: row for row in alive["rows"] if row["source"] == name}
    try:
        counts = overview.progress_cells(progress_answer[1]) if progress_answer and progress_answer[0] == "ok" else {}
    except Exception:
        counts = {}

    def node(profile, directory, line, title, failed=False):
        run = running.get(profile)
        state = "run" if run else "fail" if failed else "setup" if profile in run_profiles else "none"
        return {"profile": profile, "state": state, "dir": directory, "line": line, "title": title, "href": f"/sources/{name}#run-{profile}" if profile in run_profiles else None, "run_href": run["href"] if run else None}

    prepare = []
    for profile, directory in (("parse", paths.get("parsed")), ("lingua", paths["output"].rstrip("/") + "/lingua" if paths.get("output") else None), ("ml", paths.get("output"))):
        figure = counts.get(profile)
        prepare.append(node(profile, directory, figure["text"] + " finished" if figure else None, figure["title"] if figure else None))
    views = {view["name"]: view for view in status["ingestion"]} if found else {}
    lanes = []
    for database, profiles in LANES:
        nodes = []
        for profile, view_name, prefix in profiles:
            view = views.get(view_name)
            entries = [entry for entry in view["entries"] if entry["label"].startswith(prefix)] if view else []
            loaded = [_number(entry["count"]) for entry in entries]
            failed = [_number(entry["failed_count"]) for entry in entries]
            line = None
            if entries and None not in loaded and None not in failed:
                line = f"{sum(loaded)} files loaded" + (f", {sum(failed)} failed" if sum(failed) else "")
            elif entries:
                line = "state files of another shape"
            nodes.append(node(profile, None, line, ", ".join(entry["label"] for entry in entries) or None, failed=bool(entries) and None not in failed and sum(failed) > 0))
        lanes.append({"database": database, "nodes": nodes, "legacy": database == "PostgreSQL"})
    return {"dumps": paths.get("dumps"), "prepare": prepare, "lanes": lanes}


def _error_log_profiles(status_answer):
    """The profiles of a source whose error logs are read, from its `source.status` answer: none when that read failed or the source was not found."""
    kind, value = status_answer
    if kind != "ok":
        return []
    try:
        block = pages.source_status_block(value)
    except Exception:
        return []
    if not block["found"]:
        return []
    return [profile for profile in actions.ERROR_LOG_PROFILES if profile in block["run_profiles"]]
