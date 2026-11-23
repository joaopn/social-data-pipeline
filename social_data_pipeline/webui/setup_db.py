"""The "Set up a database" wizard of the Databases page (`sdp db setup`: the first time, a database added, or "Change…" of the databases set up); `install(api, ctx)` adds its routes, before the Management pages' (`/databases/setup` would otherwise be read as a database's name).

A change (`?change=1`, mode `change`) is the terminal's `db setup` run over the setup: it reads `db.settings` (the current answers, never a password) instead of `db.status`, fills the steps in with them (`setup_db_table.prefill`), keeps a path in the answers only when it differs from the current one (so one left as it is is written exactly as it is), offers to keep the tablespaces, the extra StarRocks disks, the PGTune output and the read-only password, refuses authentication switched off on its step (the broker refuses it too), and gives the pending action the confirm page's lines and the list of what changes (old, new) made with the read, and the outcome page's way to restart.

Steps: the databases (and, the first time, the data base path and the export path), one step per database chosen, authentication and users (left out when a database is added to a setup whose authentication is on: the new one gets it as the terminal gives it), then Review. Each step posts its own fields only (always under the guard's 64), checked by the table's kinds (`actions.parse_form` on an entry made of that step's inputs); the answers are kept in the web session (`Draft`, one per session, in this process's memory, dropped with the session) until Review makes the pending action of `db.setup` from them, after the table's whole check (`actions.check_fields`), and drops the passwords from the draft. Nothing is asked of the broker but the `db.status` read that starts the wizard; nothing is written before the confirm page's Confirm (the generic `/pending/<id>` flow, with the management password).

A draft lives as a pending action does (`Limits.pending_seconds`, counted from the last step posted); an expired one is gone, and a step posted after that is answered with a page that says so. A secret field left empty on a step keeps the password typed there before; the forms never draw a secret.

- `GET /databases/setup` (`?new=1` starts again, `?db=<database>` picks that database): reads `db.status`, then shows the first step. `GET /databases/setup/<step>` shows a step of the draft; `POST /databases/setup/<step>` saves it and goes on (`go`: `next`, `back`, or `stay` to draw one more empty row); `POST /databases/setup/confirm` makes the pending action; `POST /databases/setup/cancel` drops the draft.
- `GET /databases/setup/start?db=<database>`: after a setup, "Start it now": the `db.start` form with its admin password field (no `db`: every configured service).
"""

import threading
import weakref

from fastapi import Request
from fastapi.responses import PlainTextResponse, RedirectResponse

from social_data_pipeline.webui import actions, pages
from social_data_pipeline.webui import setup_db_table as table
from social_data_pipeline.webui.calls import BUSY_GATE, call_broker, session_ends, why

BUSY = "The web UI is busy with other requests: try again in a moment."
EXPIRED = "This setup was left for longer than a pending action is kept, so its answers are no longer kept: start again."
STEP_REFUSED = "A field of this step was refused; nothing was kept of this step."
DATABASES = actions.DATABASES
NAMES = table.DATABASE_NAMES
DATABASE_STEPS = {
    "postgres": ("pgdata_path", "db_name", "pg_port", "tablespaces", "filesystem", "pgtune", "pg_mem_limit", "keep_tablespaces", "keep_pgtune"),
    "mongo": ("mongo_data_path", "mongo_port", "mongo_cache_size_gb", "mongo_mem_limit", "mongo_validate"),
    "starrocks": ("starrocks_data_path", "starrocks_port", "starrocks_fe_http_port", "sr_fe_jvm_heap", "starrocks_mem_limit", "sr_be_mem_limit", "sr_alter_tablet_workers", "sr_compression", "starrocks_storage_paths", "keep_storage_paths"),
}
STEP_INPUTS = {"databases": ("databases", "add", "data_path", "export_path"), **DATABASE_STEPS, "auth": table.AUTH_INPUTS}
SECRETS = ("new_password", "new_password_confirm", "ro_password", "ro_password_confirm")
CHANGE_TITLE = "Change the database setup"
STEP_TITLES = {"databases": "Databases", "postgres": "PostgreSQL", "mongo": "MongoDB", "starrocks": "StarRocks", "auth": "Authentication", "review": "Review"}
# What the terminal takes on Enter for the answers that depend on this machine: the hints under their empty fields.
MACHINE_HINTS = {
    "pg_mem_limit": "Empty: the terminal's suggestion, 60% of this machine's memory (0 when unknown). 0 is unlimited.",
    "mongo_mem_limit": "Empty: the terminal's suggestion, twice the cache and at least 2. 0 is unlimited.",
    "sr_fe_jvm_heap": "Empty: the terminal's suggestion, an eighth of this machine's memory, from 2 to 8 (4 when unknown).",
    "starrocks_mem_limit": "Empty: the terminal's suggestion, 60% of this machine's memory and at least the FE heap + 4 (0, unlimited, when unknown). 0 is unlimited.",
    "sr_be_mem_limit": "Empty: the terminal's suggestion, the container limit less the FE heap less 2 (at least 2); without a limit half of this machine's memory, or 8.",
    "sr_alter_tablet_workers": "Empty: the terminal's suggestion, half of this machine's cores, from 4 to 10.",
}

_drafts = weakref.WeakKeyDictionary()
_lock = threading.Lock()


class Draft:
    """The answers of one wizard: `mode` (`first`, or `add` when a database is set up already), `configured` (the databases set up, from `db.status`), `auth_on` (authentication on for them), `fields` (the answers so far, secrets in `actions.Hidden`), `touched` (when a step was last posted, on the application's clock)."""

    __slots__ = ("mode", "configured", "auth_on", "fields", "touched", "error", "rows", "current")

    def __init__(self, mode, configured, auth_on, now, current=None):
        self.mode = mode
        # A change's `db.settings` read (`settings_block`), else None.
        self.current = current
        self.configured = configured
        self.auth_on = auth_on
        self.fields = {}
        self.touched = now
        self.error = None
        # The empty rows each list of a step draws (one, more after "Add a row").
        self.rows = {}

    def __repr__(self):
        return f"<setup draft {self.mode}>"


def steps(draft):
    """The steps of a draft, in order."""
    chosen = table.chosen(draft.fields)
    ordered = ["databases", *chosen]
    if not (draft.mode == "add" and draft.auth_on):
        ordered.append("auth")
    return ordered + ["review"]


def current_for(draft):
    """What the check takes as the setup as it is now (`setup_db_table.first_refusal`'s `current`)."""
    if draft.mode == "change":
        return table.current_of(draft.current)
    return {**table.NOTHING_SET_UP, "configured": list(draft.configured), "auth": draft.auth_on}


def settings_block(result):
    """The `db.settings` read, its shape checked (raises ValueError): `configured`, `values` (a question's name to text or a whole number), `auth`, `ro_username`, `ro_password_set`, `tablespaces`, `storage_paths`, `pgtune`, `pgtune_set`. A read that says the configuration cannot be read raises too."""
    data = result.get("data") if isinstance(result, dict) else None
    if not isinstance(data, dict) or data.get("not_readable") is not None:
        raise ValueError("not readable")
    inputs = actions.TABLE["db.setup"].inputs

    def text(value, max_bytes=4096):
        if not isinstance(value, str) or len(value.encode("utf-8", "surrogatepass")) > max_bytes:
            raise ValueError("not text")
        return value

    configured = [name for name in DATABASES if name in data.get("configured", [])]
    values = {}
    for name, value in dict(data.get("values", {})).items():
        if name not in inputs or name in SECRETS:
            raise ValueError("not a question")
        if inputs[name].kind in ("int", "port"):
            if not isinstance(value, int) or isinstance(value, bool):
                raise ValueError("not a number")
            values[name] = value
        else:
            values[name] = text(value)
    flags = {key: data.get(key) for key in ("auth", "ro_password_set", "pgtune_set")}
    if not all(isinstance(value, bool) for value in flags.values()):
        raise ValueError("not a flag")
    ro_username = data.get("ro_username")
    return {
        "configured": configured, "values": values, **flags, "ro_username": None if ro_username is None else text(ro_username, 63),
        "tablespaces": [{"name": text(item["name"], 63), "path": text(item["path"])} for item in data.get("tablespaces", [])],
        "storage_paths": [text(path) for path in data.get("storage_paths", [])], "pgtune": text(data.get("pgtune", ""), table.PGTUNE_MAX_BYTES),
    }


def draft_of(work, now, seconds):
    """The session's draft, or None when there is none or it has expired (an expired one is dropped)."""
    with _lock:
        draft = _drafts.get(work)
        if draft is not None and now - draft.touched >= seconds:
            del _drafts[work]
            draft.fields = {}
            return None
        return draft


def new_draft(work, draft):
    with _lock:
        _drafts[work] = draft


def drop_draft(work):
    with _lock:
        draft = _drafts.pop(work, None)
    if draft is not None:
        draft.fields = {}


def step_action(step):
    """An entry of the step's inputs alone, without the cross-field check: what `parse_form` checks a step's post against."""
    whole = actions.TABLE["db.setup"]
    return actions.Action("db.setup", "step_up", whole.title, {name: whole.inputs[name] for name in STEP_INPUTS[step]})


def as_written(path):
    """A path as the action writes it (`actions.as_written_path`, the host's `setup.utils.written_path`): a relative one gets its `./`, which the form's normalization takes off."""
    return actions.as_written_path(path)


def _paths_as_written(fields):
    for name in ("data_path", "export_path", "pgdata_path", "mongo_data_path", "starrocks_data_path"):
        if name in fields:
            fields[name] = as_written(fields[name])
    if "tablespaces" in fields:
        fields["tablespaces"] = [{**item, "path": as_written(item["path"])} for item in fields["tablespaces"]]
    if "starrocks_storage_paths" in fields:
        fields["starrocks_storage_paths"] = [as_written(path) for path in fields["starrocks_storage_paths"]]


def save_step(draft, step, form):
    """Keep a step's posted fields in the draft. Raises actions.Refused. A secret left empty keeps the one kept before; the databases step drops the answers of the databases no longer chosen; a relative path is kept with its `./`, as it is written."""
    parsed = actions.parse_form(step_action(step), form)
    _paths_as_written(parsed)
    if draft.mode == "change":
        current = draft.current["values"]
        for name in table.PATH_FIELDS:
            if name in parsed and table.same_path(parsed[name], current.get(name)):
                del parsed[name]
    kept = {name: draft.fields[name] for name in SECRETS if name in draft.fields and name in STEP_INPUTS[step] and name not in parsed}
    for name in STEP_INPUTS[step]:
        draft.fields.pop(name, None)
    draft.fields.update(parsed)
    if step == "auth" and draft.fields.get("auth") is True:
        draft.fields.update({name: value for name, value in kept.items() if name not in draft.fields})
    if step == "databases":
        chosen = table.chosen(draft.fields)
        for database, names in DATABASE_STEPS.items():
            if database not in chosen:
                for name in names:
                    draft.fields.pop(name, None)
    _drop_unreachable(draft)


def _drop_unreachable(draft):
    """Drop the answers a later choice made meaningless (a read-only password with authentication off), so that the draft never holds an answer the whole check refuses for its mere presence."""
    fields = draft.fields
    if fields.get("auth") is not True:
        for name in table.AUTH_INPUTS[1:]:
            fields.pop(name, None)
    if fields.get("ro_user") is not True:
        for name in ("ro_username", "ro_password_mode", "ro_password", "ro_password_confirm"):
            fields.pop(name, None)
    if fields.get("ro_password_mode") != "typed":
        fields.pop("ro_password", None)
        fields.pop("ro_password_confirm", None)


def step_refusal(draft, step):
    """The label of this step's input the whole check refuses for the draft as it is, or None (a refusal of a later step's input waits for that step)."""
    refused = table.first_refusal(draft.fields, current_for(draft))
    if refused is None or refused not in STEP_INPUTS.get(step, ()):
        return None
    return actions.TABLE["db.setup"].inputs[refused].label


def step_model(draft, step):
    """What a step's template shows: the stepper, the inputs with their values (the draft's, else the terminal's defaults that do not depend on the machine), the hints."""
    inputs = actions.TABLE["db.setup"].inputs
    ordered = steps(draft)
    values = {}
    for name in STEP_INPUTS.get(step, ()):
        if name in SECRETS:
            continue
        if name in draft.fields:
            values[name] = draft.fields[name]
        elif draft.mode == "change" and name in draft.current["values"]:
            values[name] = draft.current["values"][name]
        elif name in table.DEFAULTS and not (draft.mode == "add" and name == "data_path"):
            values[name] = table.DEFAULTS[name]
    base = draft.fields.get("data_path", "./data")
    if draft.mode in ("first", "change") and step in DATABASE_STEPS and step not in draft.configured:
        key = DATABASE_STEPS[step][0]
        values.setdefault(key, f"{base}/database/{step}")
    if step == "databases" and draft.mode == "first":
        values.setdefault("export_path", f"{base}/export")
    position = ordered.index(step) if step in ordered else 0
    return {
        "step": step, "title": STEP_TITLES[step], "steps": [{"key": key, "title": STEP_TITLES[key], "state": "done" if index < position else "on" if index == position else ""} for index, key in enumerate(ordered)],
        "number": position + 1, "inputs": inputs, "values": values, "mode": draft.mode, "configured": draft.configured, "auth_on": draft.auth_on,
        "free": [name for name in DATABASES if name not in draft.configured], "chosen": table.chosen(draft.fields), "names": NAMES,
        "hints": MACHINE_HINTS, "rows": draft.rows, "kept_secrets": [name for name in SECRETS if name in draft.fields],
        "next": ordered[position + 1] if position + 1 < len(ordered) else None,
        "next_title": (STEP_TITLES[ordered[position + 1]] if ordered[position + 1] in DATABASES else STEP_TITLES[ordered[position + 1]].lower()) if position + 1 < len(ordered) else None,
        "previous": ordered[position - 1] if position > 0 else None,
        "error": draft.error, "command": table.command(draft.fields) if step == "review" else None,
        "shown": actions.shown_inputs(actions.TABLE["db.setup"], draft.fields) if step == "review" else None,
        "writes": table.writes(draft.fields, draft.current) if step == "review" else None,
        "changes": table.changes(draft.fields, draft.current) if step == "review" and draft.mode == "change" else None,
        "read": draft.current, "heading": CHANGE_TITLE if draft.mode == "change" else "Set up a database",
        "auth_off": table.AUTH_OFF_SENTENCE,
    }


def install(api, ctx):
    """Add the wizard's routes to `api`."""
    limits = ctx.limits

    def not_found():
        return PlainTextResponse("Not found.\n", status_code=404)

    def expired(session):
        return ctx.frame("refused.html", "databases", session, 410, title="Set up a database", line=EXPIRED, field=None, back=("/databases/setup?new=1", "Start again"))

    def show(session, draft, step, status=200):
        model = step_model(draft, step)
        draft.error = None
        return ctx.frame("setup_db.html", "databases", session, status, title=model["heading"], wizard=model)

    @api.get("/databases/setup")
    async def start(request: Request):
        session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))
        change = request.query_params.get("change") == "1"
        verb = "db.settings" if change else "db.status"
        answer = await ctx.pages.run(lambda: call_broker(lambda: ctx.broker(token=session.token, timeout=limits.read_seconds), lambda made: actions.call(made, verb, {})))
        if answer is BUSY_GATE:
            return ctx.frame("busy.html", "databases", session, 503, title="Set up a database", line=BUSY)
        kind, value = answer
        if kind == "error" and session_ends(value):
            ctx.sessions.forget(session_id)
            return ctx.to_login(request, why(value), clear_cookie=True)
        block = pages.build({"status": answer}, {"status": settings_block if change else pages.databases_status_block})["status"]
        if not block["ok"]:
            return ctx.frame("refused.html", "databases", session, 503, title=CHANGE_TITLE if change else "Set up a database", line=block["sentence"], field=None, back=("/databases", "Back to Databases"))
        configured = block["configured"]
        if change:
            if not configured:
                return RedirectResponse("/databases/setup?new=1", status_code=303)
            read = {key: value for key, value in block.items() if key != "ok"}
            now = ctx.clock()
            # A change always starts again from the setup as it is now.
            draft = Draft("change", configured, read["auth"], now, current=read)
            draft.fields = table.prefill(read)
            new_draft(session.work, draft)
            picked = request.query_params.get("db")
            return show(session, draft, picked if picked in configured else "databases")
        auth_on = any(row["admin_user"] for row in block["rows"])
        mode = "add" if configured else "first"
        now = ctx.clock()
        draft = draft_of(session.work, now, limits.pending_seconds)
        if draft is None or request.query_params.get("new") == "1" or draft.mode != mode or draft.configured != configured:
            draft = Draft(mode, configured, auth_on, now)
            new_draft(session.work, draft)
        picked = request.query_params.get("db")
        if picked in DATABASES and picked not in configured:
            draft.fields.pop("add", None)
            if mode == "add":
                draft.fields["add"] = picked
            else:
                draft.fields["databases"] = sorted(set(draft.fields.get("databases", [])) | {picked}, key=DATABASES.index)
        draft.touched = now
        return show(session, draft, "databases")

    @api.get("/databases/setup/start")
    async def start_now(request: Request):
        session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))
        picked = request.query_params.get("db")
        if picked is not None and picked not in DATABASES:
            return not_found()
        return ctx.frame("setup_db_start.html", "databases", session, title="Start it now", service=picked, database_name=NAMES.get(picked), db_password_kept=session.work.db_password_kept)

    @api.get("/databases/setup/{step}")
    async def show_step(request: Request, step: str):
        session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))
        if step not in STEP_TITLES:
            return not_found()
        draft = draft_of(session.work, ctx.clock(), limits.pending_seconds)
        if draft is None:
            return expired(session)
        if step not in steps(draft):
            return RedirectResponse("/databases/setup/databases", status_code=303)
        return show(session, draft, step)

    @api.post("/databases/setup/confirm")
    async def confirm(request: Request):
        session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))
        now = ctx.clock()
        draft = draft_of(session.work, now, limits.pending_seconds)
        if draft is None:
            return expired(session)
        try:
            fields = actions.check_fields(actions.TABLE["db.setup"], dict(draft.fields))
            refused = table.first_refusal(fields, current_for(draft))
            if refused is not None:
                raise actions.Refused(refused)
        except actions.Refused as refused:
            draft.error = actions.TABLE["db.setup"].inputs[refused.field].label if refused.field in actions.TABLE["db.setup"].inputs else "the answers"
            return show(session, draft, "review", 400)
        shown = None
        if draft.mode == "change":
            # Made with the read the form started from: the old values the confirm page names (the old data folders among them).
            shown = {"writes": table.writes(fields, draft.current), "changes": table.changes(fields, draft.current), "back": ("/databases", "Databases")}
        pending = session.work.add_pending("db.setup", fields, now, shown=shown)
        # The pending action holds the passwords now; the draft keeps the other answers for a new attempt.
        for name in SECRETS:
            draft.fields.pop(name, None)
        draft.touched = now
        return RedirectResponse(f"/pending/{pending.id}", status_code=303)

    @api.post("/databases/setup/cancel")
    async def cancel(request: Request):
        session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))
        drop_draft(session.work)
        return RedirectResponse("/databases", status_code=303)

    @api.post("/databases/setup/{step}")
    async def post_step(request: Request, step: str):
        session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))
        if step not in STEP_INPUTS:
            return not_found()
        now = ctx.clock()
        draft = draft_of(session.work, now, limits.pending_seconds)
        if draft is None:
            return expired(session)
        if step not in steps(draft):
            return RedirectResponse("/databases/setup/databases", status_code=303)
        form = dict(request.scope.get("sdp.form", {}))
        go = form.pop("go", "next")
        if go not in ("next", "back", "stay"):
            return not_found()
        if step == "databases":
            # The first time one box per database (`pick_<database>`), turned into the list `databases[<i>]` here, since boxes left empty would leave gaps in the list's numbering; else one database to add.
            picked = [name for name in DATABASES if form.pop(f"pick_{name}", None) == actions.CHECKED or (draft.mode == "change" and name in draft.configured)]
            if draft.mode in ("first", "change"):
                form.pop("add", None)
                form.update({f"databases[{index}]": name for index, name in enumerate(picked)})
        try:
            save_step(draft, step, form)
        except actions.Refused as refused:
            inputs = actions.TABLE["db.setup"].inputs
            draft.error = f"{STEP_REFUSED} The field: {inputs[refused.field].label}." if refused.field in inputs else STEP_REFUSED
            draft.touched = now
            return show(session, draft, step, 400)
        draft.touched = now
        if go == "stay":
            for name in STEP_INPUTS[step]:
                if actions.TABLE["db.setup"].inputs[name].kind == "list" and name != "databases":
                    draft.rows[name] = min(draft.rows.get(name, 1) + 1, 4)
            return RedirectResponse(f"/databases/setup/{step}", status_code=303)
        if go == "next":
            if step == "databases" and not table.chosen(draft.fields):
                draft.error = "Choose a database to set up."
                return show(session, draft, step, 400)
            label = step_refusal(draft, step)
            if label is not None:
                # The passwords of a refused step are typed again, both copies: a copy kept would make the next try refused or half-checked.
                for name in SECRETS:
                    draft.fields.pop(name, None)
                draft.error = table.AUTH_OFF_SENTENCE if label == actions.TABLE["db.setup"].inputs["auth"].label else f"This answer cannot be taken as it is. The field: {label}."
                if draft.mode == "change" and label == actions.TABLE["db.setup"].inputs["auth"].label:
                    # Authentication left on as it is: the step is drawn again with the read-only user as it is set up, not with what unticking it dropped.
                    draft.fields.update({name: value for name, value in table.prefill(draft.current).items() if name in table.AUTH_INPUTS})
                return show(session, draft, step, 400)
        ordered = steps(draft)
        position = ordered.index(step)
        target = ordered[position + 1] if go == "next" else ordered[max(position - 1, 0)]
        return RedirectResponse(f"/databases/setup/{target}", status_code=303)
