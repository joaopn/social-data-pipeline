"""The Queue window: the query scheduler's queue, one to one with the scheduler's own page (Pending, Approved, Running, History, EXPLAIN, preview, download, auto-accept), through the scheduler's control socket (`scheduler.Scheduler`). Approving a query makes the scheduler run it with the database's admin login.

Pages, all GET (a HEAD is 405), each asking the socket in one worker of a bound of its own (`Limits.door_workers`, four places; "busy" when full), never on the event loop, and rendered in a worker too (a statement can be 1 MiB):

- `GET /queue?tab=pending|approved|running|history&page=N`: three tabs, as the mock-up draws them, in the frame's tab bar (`QUEUE_PAGES`): Pending and Running with their numbers (`counts`; Running counts the approved and the running jobs), and History. Each list holds 50 jobs (`jobs` with `limit` and `offset`; a "more" link while the socket says `more`): when, the job, who submitted it, target, backend, output file, description and the actions its phase allows, with the statement as cut by the socket in a row of its own under the job (opened by "SQL", no script), and in the history the outcome, rows, size, error or rejection reason. The Running tab shows two lists, each with its own pages: "Approved, waiting to start" above "Running"; `tab=approved` is the Running tab with the approved list at page N, `tab=running` the same with the running list at page N. A job's id becomes a link to its page, and its row gets its forms (approve, reject, EXPLAIN, kill: the same `POST /pending` as on its page), only when it has the form of a job id. The Pending tab also shows the auto-accept panel (`auto_accept`, asked in the same worker after the same check of the broker; its failure is said in the panel alone). The running list asks for itself again every 5 seconds (`GET /queue/running-rows?page=N`, a part of the page, with the session's form token for its forms).
- `GET /queue/<job id>` (the id checked first, else not found): what the socket's `job` gives that the scheduler's page shows, the statement as stored with every invisible or control character shown as an escape (`statements.visible`), the hints (`statements.hints`), who approved, rejected or asked to kill; the buttons the job's phase allows (pending: approve, reject with a reason, EXPLAIN; running: kill); for a finished job a button for its preview (loaded at once when the address says `?preview=1`, as a History row's "Preview" link does) and one download link per part of its result (`results`). A job whose record is too large to send has no buttons: the page says to use the scheduler's own page.
- `GET /queue/<job id>/preview`: a part of the page from the socket's `preview`: a table of text cells, the parts' names and sizes, "truncated" when the socket says so; its refusals as sentences.
- `GET /queue/<job id>/download/<part>`: the part streamed from the web UI's read-only mount of the results folder (`results`), after the socket said the job is in the history as done; at most two at a time ("busy" beyond); anything else not found.
- `GET /queue/auto-accept`: the targets with their auto-accept settings (`auto_accept`) and a form for each, and that the assistant's queries are never auto-accepted; the Pending tab's panel links here ("Change...").

Wherever a job is shown (the rows of every tab, its page, the confirm pages of its actions), a job whose record's `submitted_by` is exactly `"assistant"` is labelled as submitted by the chat's assistant (`assistant_label`); any other value of the field is shown as nothing. The approval of such a job also says that the assistant wrote it and to read it as one from a stranger, its EXPLAIN that an EXPLAIN of it runs before any approval (and that it may be the assistant's, when that could not be read).

The actions (`queue.approve`, `queue.reject`, `queue.kill`, `queue.explain`, `queue.auto_accept`) are entries of the web UI's table and take the Management window's way (`management`): `POST /pending` keeps a pending action (an approval fetches the job from the socket first and keeps its whole record and its digest; a reject, kill or EXPLAIN reads it too, after the broker's check (a session the broker ended goes to the login, nothing read), and keeps only whether the assistant submitted it, or that this could not be read, which its page then says; a reason is checked; a change of auto-accept is compared with the target's settings, and switching it on or raising its limit makes the confirmation need the management password), `GET /pending/<id>` shows it, `POST /pending/<id>/confirm` takes it out and starts a thread, which first asks the broker whether the session still lives (`audit.list` with `limit` 1) and, for a step-up, checks the management password (`session.check_password`), and only then asks the socket, with `by` set to `web-<the broker's number of the session>`; `GET /actions/<id>` shows the outcome. A session the broker ended goes to the login and nothing reaches the socket; a broker that cannot be reached refuses the action. Standard library, FastAPI and this package only.
"""

import json
import logging
import math
import os
import re
import threading
import time

import anyio
from fastapi import Request
from fastapi.responses import PlainTextResponse, RedirectResponse

from social_data_pipeline.webui import actions, results, statements, words
from social_data_pipeline.webui.calls import BUSY_GATE, SESSION_ENDING_CODES, call_broker, why
from social_data_pipeline.webui.scheduler import SchedulerUnavailable

log = logging.getLogger("social_data_pipeline.webui")

PAGE_SIZE = 50
TABS = actions.PHASES
TAB_TITLES = {"pending": "Pending", "approved": "Running", "running": "Running", "history": "History"}
QUEUE_PAGES = (
    ("queue:pending", "Pending", "/queue?tab=pending"),
    ("queue:running", "Running", "/queue?tab=running"),
    ("queue:history", "History", "/queue?tab=history"),
)
# The tab of the frame under which each phase's list is shown, and the lists of each tab: the approved jobs wait under Running.
TAB_OF = {"pending": "pending", "approved": "running", "running": "running", "history": "history"}
LISTS_OF = {"pending": ("pending",), "running": ("approved", "running"), "history": ("history",)}
LIST_TITLES = {"pending": "Pending approval", "approved": "Approved, waiting to start", "running": "Running", "history": "History"}
HEAD_NOTES = {
    "pending": "oldest first \u00b7 an approved query runs with the database's admin login, so read it first",
    "running": "oldest first \u00b7 the running list refreshes every 5 seconds",
    "history": "newest first",
}
# The dot of a job's pill by its phase, and in the history by its status (any other status: a grey dot).
PHASE_PILLS = {"pending": ("warn", "waiting for you"), "approved": ("", "waiting to start"), "running": ("run", "running")}
STATUS_DOTS = {"done": "ok", "failed": "bad", "rejected": "bad", "cancelled": "bad"}
PAGE_PATTERN = r"[1-9][0-9]{0,3}"
PAGE_MAX = actions.JOBS_OFFSET_MAX // PAGE_SIZE + 1
PART_PATTERN = r"[0-9]{1,4}"
RUNNING_TRIGGER = "load delay:5s"
NOT_FOUND = "Not found.\n"
BUSY = "The web UI is busy with other requests to the scheduler: try again in a moment."
DOWNLOADS_BUSY = "The web UI already sends two downloads: try again when one has ended."
QUEUE_BUSY = "The web UI already asks the scheduler as much as it does at once: confirm again in a moment."
SHAPE = "The scheduler's answer did not have the expected shape."

# The sentences of the pages.
ADMIN_SENTENCE = "Once approved, the scheduler runs this query on its database with the database's admin login, not with the read-only login of the MCP servers."
NO_CHECK_SENTENCE = "This page does not check the statement: it shows it as the scheduler holds it, and an approval runs exactly that."
ESCAPED_SENTENCE = "Characters that would not show as themselves (direction changes, invisible, control or private characters) are shown as escapes such as \\u202e:"
OVERWRITE_SENTENCE = "It overwrites an existing result of that name."
HINTS_LABEL = "Hints, not a check"
HINTS_NOTE = "A few words worth a look before an approval, found in the statement. An empty list proves nothing: a statement can do harm without any of them, and they can be written so that they are not found."
NO_HINTS = "None of the words of the list was found."
EXPLAIN_SENTENCE = "EXPLAIN is sent to the database with the database's admin login, before anyone approved the query, as the scheduler's own EXPLAIN button does. A statement written for it can execute on the database: this page does not check the statement."
KILL_SENTENCE = "The scheduler asks the database to stop the query. A query may still finish by itself before it is stopped."
REJECT_SENTENCE = "The job is moved to the history as rejected, with the reason; it does not run."
TOO_LARGE_SENTENCE = words.DOOR_CODE_SENTENCES["too_large"]
TOO_HEAVY = "This job's record and its page are larger than the web UI keeps for a pending action (4 MiB), so nothing was kept: use the scheduler's own page for it."
NO_NUMBER = "The broker's login reply named no session number, so this session cannot act on the Queue: log out and in again."
NO_RESULTS = "Downloads are not available: the scheduler's results folder was not mounted when the web UI was started (see python sdp.py webui status)."
NO_PARTS = "No part of the result was found in the results folder."
REASON_REFUSED = "The reason was refused: at most 500 characters, without a line break or a control character. Nothing was sent to the scheduler."
NOT_PENDING_NOW = "The job is not pending, so it cannot be approved. It is now:"
UNKNOWN_TARGET = "The target is not one of the scheduler's targets. Nothing was sent to the scheduler."
NOTHING_TO_CHANGE = "The settings chosen are the target's settings already: nothing to change."
LIMIT_REFUSED = "The limit is a whole number from 0 to {max_limit}, the scheduler's largest limit."
AUTO_ON_SENTENCE = "Every query submitted to the target {target} is then approved by the scheduler itself and runs with the database's admin login without anyone looking at it, up to {limit} of the target's queries approved or running at a time."
STEP_UP_NOTE = "Switching auto-accept on, or raising its limit, needs the management password again; switching it off or lowering the limit does not."
# A job of the chat's assistant: its record's `submitted_by` is exactly this mark (`jobs.store.ASSISTANT`); any other value, which only a hand-edited record can have, is shown as nothing.
ASSISTANT_MARK = "assistant"
ASSISTANT_LABEL = "Submitted by the chat's assistant"
# The row's "Submitted by" cell of such a job: the mock-up's tag and a note the record supports (it says no more than that the chat's assistant submitted it).
ASSISTANT_TAG = "assistant"
ASSISTANT_ROW_NOTE = "from a chat"
ASSISTANT_SENTENCE = "The chat's assistant wrote this query, and text in the data it read can steer what it writes: read the statement as carefully as one from a stranger."
ASSISTANT_UNKNOWN = "Whether the chat's assistant submitted this query could not be read."
EXPLAIN_ASSISTANT_SENTENCE = "The chat's assistant wrote this query, and text in the data it read can steer what it writes: an EXPLAIN of it runs before anyone approved it."
EXPLAIN_MAYBE_SENTENCE = "This query may have been written by the chat's assistant, and text in the data it read can steer what it writes: an EXPLAIN of it runs before anyone approved it."
NEVER_AUTO_SENTENCE = "Queries submitted by the chat's assistant are never auto-accepted, whatever the target's setting: each one waits until a person approves it."
PHASE_WORDS = {"pending": "pending", "approved": "approved and waiting to run", "running": "running", "history": "in the history", "gone": "gone"}


def _not_found():
    return PlainTextResponse(NOT_FOUND, status_code=404)


def by_of(session):
    """The label that names a session to the scheduler, `web-<number>`, or None when the login gave no usable number."""
    number = getattr(session, "number", None)
    if isinstance(number, int) and not isinstance(number, bool) and 0 <= number < 10 ** 12:
        return f"web-{number}"
    return None


# ── what is shown of the scheduler's answers ────────────────────────────────


def shown(value):
    """A value of the scheduler as runs of text and escapes for the `vis` macro: a text with its invisible characters made visible, None as `-`, a number or a boolean as its text, anything else as its JSON."""
    if isinstance(value, str):
        return statements.visible(value)[0]
    if value is None:
        return [(False, "-")]
    if isinstance(value, bool):
        return [(False, "yes" if value else "no")]
    if isinstance(value, (int, float)):
        return [(False, str(value))]
    try:
        return statements.visible(json.dumps(value, ensure_ascii=False))[0]
    except (TypeError, ValueError):
        return [(False, "-")]


# The largest whole number shown as a number; beyond it only its first digits (a hand-edited record can hold any integer, which a float cannot take).
NUMBER_DIGITS_SHOWN = 30
TIME_BOUND = 10 ** 11


def _whole(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _digits(value):
    """A whole number as text, its digits cut after NUMBER_DIGITS_SHOWN."""
    text = str(value)
    return text if len(text) <= NUMBER_DIGITS_SHOWN else text[:NUMBER_DIGITS_SHOWN] + "... (" + str(len(text)) + " digits)"


def when(value):
    """A time of the scheduler (seconds since the epoch) as UTC text; `-` for no value or one that is not a number, "not a time" for a number no clock reaches. Total over every JSON value."""
    if value is None or isinstance(value, bool) or not isinstance(value, (int, float)):
        return "-"
    if _whole(value) and not -TIME_BOUND < value < TIME_BOUND:
        return "not a time"
    if isinstance(value, float) and not (math.isfinite(value) and -TIME_BOUND < value < TIME_BOUND):
        return "not a time"
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(value))
    except (OverflowError, ValueError, OSError):
        return "not a time"


SIZE_UNITS = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")


def size_text(value):
    """A size in bytes as text, computed in whole numbers (no float of the value is made); `-` for anything but a whole number from 0 on. Total over every JSON value."""
    if not _whole(value) or value < 0:
        return "-"
    if value < 1024:
        return f"{value} B"
    for power, unit in enumerate(SIZE_UNITS[1:], start=1):
        if value < 1024 ** (power + 1) or unit == SIZE_UNITS[-1]:
            tenths = value * 10 // 1024 ** power
            return f"{_digits(tenths // 10)}.{tenths % 10} {unit}"
    return "-"


def rows_text(value):
    """A count as text with thousands separators, its digits cut; `-` for anything but a whole number. Total over every JSON value."""
    if not _whole(value):
        return "-"
    return _digits(value) if len(str(abs(value))) > NUMBER_DIGITS_SHOWN else f"{value:,}"


def overwrite_of(value):
    """(whether the job overwrites, the value as text when it is not a boolean): the backends read the record's `overwrite` by its truth, so any true value overwrites."""
    return bool(value), None if isinstance(value, bool) or not value else shown(value)


def statement_counts(text):
    """The line on top of a statement's box: its lines and characters."""
    if not text:
        return "0 lines, 0 characters"
    lines = text.count("\n") + (0 if text.endswith("\n") else 1)
    return f"{lines} line{'s' if lines != 1 else ''}, {len(text)} character{'s' if len(text) != 1 else ''}"


def job_href(job_id):
    return "/queue/" + job_id if actions.is_job_id(job_id) else None


def assistant_label(record):
    """ASSISTANT_LABEL for a record (or a list's short view) whose `submitted_by` is exactly the assistant's mark, else None: the field's value is never shown."""
    value = record.get("submitted_by")
    return ASSISTANT_LABEL if isinstance(value, str) and value == ASSISTANT_MARK else None


def row_model(job):
    """One job of a list (the socket's short view) as a row; raises ValueError for one that is not an object. `job_id` is the raw id only when it has the form of a job id (else None, and the row has no link and no form)."""
    if not isinstance(job, dict):
        raise ValueError("not a job")
    sql = job.get("sql")
    sql_length = job.get("sql_length")
    status = job.get("status")
    return {
        "id": shown(job.get("job_id")),
        "job_id": job.get("job_id") if actions.is_job_id(job.get("job_id")) else None,
        "href": job_href(job.get("job_id")),
        "assistant": ASSISTANT_TAG if assistant_label(job) else None,
        "assistant_note": ASSISTANT_ROW_NOTE if assistant_label(job) else None,
        "mongo": job.get("backend") == "mongodb",
        "done": status == "done",
        "dot": STATUS_DOTS.get(status, "") if isinstance(status, str) else "",
        "target": shown(job.get("target")),
        "backend": shown(job.get("backend")),
        "status": shown(job.get("status")),
        "submitted": when(job.get("submitted_at")),
        "approved": when(job.get("approved_at")),
        "started": when(job.get("started_at")),
        "finished": when(job.get("finished_at")),
        "rows": rows_text(job.get("rows")),
        "size": size_text(job.get("size_bytes")),
        "output": shown(job.get("output_filename")),
        "overwrite": overwrite_of(job.get("overwrite"))[0],
        "overwrite_raw": overwrite_of(job.get("overwrite"))[1],
        "sql": shown(sql),
        "sql_cut": isinstance(sql, str) and _whole(sql_length) and sql_length > len(sql),
        "sql_length": rows_text(sql_length),
        "description": shown(job.get("description")) if job.get("description") else None,
        "error": shown(job.get("error")) if job.get("error") else None,
        "reject_reason": shown(job.get("reject_reason")) if job.get("reject_reason") else None,
    }


STATEMENT_NOT_TEXT = "The statement is not text: its JSON is shown."
HINTS_NOT_COMPUTED = "Hints were not computed: the statement is not text."


def statement_model(record):
    """The statement of a whole record with what goes with it: its runs, the count of characters made visible, its lines and characters, the hints (None when the statement is not text: its JSON is shown instead, and no hints are computed)."""
    sql = record.get("sql")
    backend = record.get("backend")
    if isinstance(sql, str):
        text, not_text = sql, False
        found = statements.hints(sql, backend if isinstance(backend, str) else "")
    else:
        try:
            text = json.dumps(sql, ensure_ascii=False)
        except (TypeError, ValueError):
            text = "-"
        not_text, found = True, None
    parts, count = statements.visible(text)
    return {
        "parts": parts, "escaped": count, "hints": found, "not_text": not_text, "counts": statement_counts(text), "anchor": "statement",
        "escaped_sentence": ESCAPED_SENTENCE, "hints_label": HINTS_LABEL, "hints_note": HINTS_NOTE, "no_hints": NO_HINTS,
        "not_text_sentence": STATEMENT_NOT_TEXT, "hints_not_computed": HINTS_NOT_COMPUTED,
    }


def acted_by(record):
    labels = record.get("acted_by")
    if not isinstance(labels, dict):
        return []
    names = {"approved": "Approved by", "rejected": "Rejected by", "killed": "Kill asked by"}
    return [(names[key], shown(labels[key])) for key in ("approved", "rejected", "killed") if key in labels]


def job_model(phase, record):
    """What the job page shows of a whole record."""
    backend = record.get("backend")
    model = {
        "job_id": record.get("job_id"),
        "phase": phase,
        "phase_text": PHASE_WORDS.get(phase, "-"),
        # The dot of the page's pill: by the phase, in the history by the status.
        "dot": (STATUS_DOTS.get(record.get("status"), "") if isinstance(record.get("status"), str) else "") if phase == "history" else PHASE_PILLS.get(phase, ("", ""))[0],
        "assistant": assistant_label(record),
        "target": shown(record.get("target")),
        "backend": shown(backend),
        "mongo": backend == "mongodb",
        "collection": shown(record.get("collection")),
        "database": shown(record.get("database")),
        "status": shown(record.get("status")),
        "output": shown(record.get("output_filename")),
        "overwrite": overwrite_of(record.get("overwrite"))[0],
        "overwrite_raw": overwrite_of(record.get("overwrite"))[1],
        "submitted": when(record.get("submitted_at")),
        "approved": when(record.get("approved_at")),
        "started": when(record.get("started_at")),
        "finished": when(record.get("finished_at")),
        "rows": rows_text(record.get("rows")),
        "size": size_text(record.get("size_bytes")),
        "description": shown(record.get("description")) if record.get("description") else None,
        "error": shown(record.get("error")) if record.get("error") else None,
        "reject_reason": shown(record.get("reject_reason")) if record.get("reject_reason") else None,
        "acted_by": acted_by(record),
        "done": phase == "history" and record.get("status") == "done",
    }
    model.update(statement_model(record))
    return model


def _job_reply(value, job_id):
    """(phase, record, digest) from a `job` reply for `job_id`; raises ValueError for another shape or another job."""
    if not isinstance(value, dict):
        raise ValueError("not a reply")
    phase, record, digest = value.get("phase"), value.get("job"), value.get("digest")
    if phase not in TABS or not isinstance(record, dict) or record.get("job_id") != job_id or not isinstance(digest, str) or not re.fullmatch(actions.DIGEST_PATTERN, digest):
        raise ValueError("not a job")
    return phase, record, digest


def _failure(error):
    return {"sentence": words.door_sentence(error), "code": getattr(error, "code", None)}


def _auto_accept_reply(value):
    """(max_limit, rows) from an `auto_accept` reply; raises ValueError."""
    if not isinstance(value, dict):
        raise ValueError("not a reply")
    max_limit, targets = value.get("max_limit"), value.get("targets")
    if not isinstance(max_limit, int) or isinstance(max_limit, bool) or not isinstance(targets, list):
        raise ValueError("not a reply")
    rows = []
    for target in targets:
        if not isinstance(target, dict) or not isinstance(target.get("enabled"), bool) or not isinstance(target.get("limit"), int) or isinstance(target.get("limit"), bool):
            raise ValueError("not a target")
        name = target.get("name")
        rows.append({"name": name, "changeable": _is_target(name), "shown": shown(name), "backend": shown(target.get("backend")), "database": shown(target.get("database")), "enabled": target["enabled"], "limit": target["limit"], "limit_text": rows_text(target["limit"])})
    return max_limit, rows


def _is_target(name):
    return isinstance(name, str) and 1 <= len(name) <= actions.TARGET_MAX_CHARS and not actions.has_control_character(name)


def escalates(enabled, limit, current_enabled, current_limit):
    """Whether a change of auto-accept needs the management password: it switches the target on from off, or raises its limit."""
    return (enabled is True and not current_enabled) or (limit is not None and limit > current_limit)


# ── the routes ──────────────────────────────────────────────────────────────


class Hooks:
    """What `management` asks of this module for an action of the Queue: making its pending action, its page's model, rendering that page off the event loop, the session's check at the broker before a pending page is shown, the thread that runs a confirmed one, the page of the window it belongs to, and the bound it runs under."""

    def __init__(self, make_pending, pending_model, render_pending, guard, start, active, places):
        self.make_pending = make_pending
        self.pending_model = pending_model
        self.render_pending = render_pending
        self.guard = guard
        self.start = start
        self.active = active
        self.places = places


# How long the broker's answer that a session still lives is believed for the Queue's reads, on the application's clock. A download asks every time.
SESSION_CHECK_SECONDS = 5


class _BrokerRefused:
    """What a worker returns when the broker did not confirm the session: the error."""

    def __init__(self, error):
        self.error = error


def broker_session_error(ctx, session, cached=True, timeout=None):
    """None when the broker still knows the session (asked with `audit.list` and a limit of 1, through the table, with the reads' time limit or `timeout`; its yes believed for SESSION_CHECK_SECONDS unless `cached` is false), else the broker's error. Runs in a worker. The Queue's and the Chat's routes ask it before every page, part, download and change, the top bar's indicators (`status`) before they ask anything."""
    work = session.work
    now = ctx.clock()
    checked = getattr(work, "broker_checked_at", None)
    if cached and checked is not None and 0 <= now - checked < SESSION_CHECK_SECONDS:
        return None
    kind, value = call_broker(lambda: ctx.broker(token=session.token, timeout=ctx.limits.read_seconds if timeout is None else timeout), lambda made: actions.call(made, "audit.list", {"limit": 1}))
    if kind == "error":
        work.broker_checked_at = None
        return value
    work.broker_checked_at = now
    return None


def broker_checked(ctx, session, work, cached=True):
    """`work(session)` once the broker confirmed the session; a `_BrokerRefused` without calling it otherwise."""
    error = broker_session_error(ctx, session, cached)
    if error is not None:
        return _BrokerRefused(error)
    return work(session)


def is_refused(value):
    return isinstance(value, _BrokerRefused)


def ended_session(ctx, request, session_id, error):
    """For a session the broker no longer knows (`unauthorized`, `session_ended`): the session forgotten and the way to the login (`HX-Redirect` for htmx). None for any other error, which the caller shows."""
    if getattr(error, "code", None) in SESSION_ENDING_CODES:
        ctx.sessions.forget(session_id)
        return ctx.to_login(request, why(error), clear_cookie=True)
    return None


def install(api, ctx):
    """Add the Queue's routes to `api`; returns the `Hooks` that `management` uses for its actions."""
    limits = ctx.limits
    scheduler = ctx.scheduler

    def ask(name, fields=None):
        """One call of the socket, in the calling worker: ("ok", result) or ("error", error)."""
        return call_broker(lambda: scheduler, lambda made: made.call(name, fields or {}))

    def session_error(session, cached=True):
        return broker_session_error(ctx, session, cached)

    def checked(session, work, cached=True):
        return broker_checked(ctx, session, work, cached)

    def refusal_of(request, session_id, error):
        return ended_session(ctx, request, session_id, error)

    def broker_failure(error):
        return {"sentence": words.sentence(error), "code": getattr(error, "code", None)}

    async def guard(request, session_id, session):
        """The session's check before a pending page of the Queue (or a confirmation's answer) is shown: None to go on, or the answer (the login, or a page with the broker's sentence). It asks the broker through the pages' bound, not the scheduler's, which a busy confirmation finds full."""
        made = await ctx.pages.run(lambda: session_error(session))
        if made is BUSY_GATE:
            return ctx.frame("busy.html", "queue:job", session, 503, title="Busy", line=BUSY)
        if made is None:
            return None
        ending = refusal_of(request, session_id, made)
        if ending is not None:
            return ending
        return ctx.frame("refused.html", "queue:job", session, 503, title="Not shown", line=words.sentence(made), field=None, back=("/queue", "Back to the Queue"))

    def unavailable(answer):
        """Whether an answer says that the socket could not be asked at all (no settings, the folder's check, no socket, no reply)."""
        kind, value = answer
        return kind == "error" and (isinstance(value, SchedulerUnavailable) or getattr(value, "code", None) == "unreachable")

    async def show(request, active, title, template, work):
        """A page of the window: `work(session)` runs in a worker of the socket's bound and returns the template's context (and a status); the page is rendered in a worker too."""
        session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))
        made = await ctx.door.run(lambda: checked(session, work))
        if made is BUSY_GATE:
            return ctx.frame("busy.html", active, session, 503, title=title, line=BUSY)
        if isinstance(made, _BrokerRefused):
            ending = refusal_of(request, session_id, made.error)
            if ending is not None:
                return ending
            return ctx.frame("busy.html", active, session, 503, title=title, line=words.sentence(made.error))
        status = made.pop("_status", 200)
        return await anyio.to_thread.run_sync(lambda: ctx.frame(template, active, session, status, title=title, **made))

    # ── the tabs ──

    def tab_base(tab, page, failure=None):
        """The model of `/queue?tab=<tab>`: its head, the numbers of the frame's tabs, its lists, and on the Pending tab the auto-accept panel."""
        shown_tab = TAB_OF[tab]
        return {
            "tab": shown_tab, "page": page, "failure": failure, "head_title": LIST_TITLES[shown_tab], "head_note": HEAD_NOTES[shown_tab],
            "counts": None, "counts_failure": None, "subbar_counts": {}, "lists": [], "auto_accept": None,
        }

    def list_base(phase, page, failure=None):
        """The model of one list (`_queue_rows.html`): its jobs or why there are none, its pager, and for the running list its poll."""
        return {
            "phase": phase, "title": LIST_TITLES[phase], "page": page, "failure": failure, "jobs": None, "more_href": None, "back_href": None,
            "poll_href": f"/queue/running-rows?page={page}" if phase == "running" else None, "poll_trigger": RUNNING_TRIGGER,
            "downloads": scheduler.results is not None, "titled": phase in ("approved", "running"),
        }

    def list_model(phase, page, listed):
        model = list_base(phase, page)
        kind, value = listed
        if kind == "error":
            model["failure"] = _failure(value)
            return model
        try:
            if not isinstance(value, dict) or value.get("phase") != phase or not isinstance(value.get("jobs"), list) or not isinstance(value.get("more"), bool):
                raise ValueError("not a list")
            model["jobs"] = [row_model(job) for job in value["jobs"]]
        except ValueError:
            model["failure"] = {"sentence": SHAPE, "code": "shape"}
            return model
        if value["more"] and page < PAGE_MAX:
            model["more_href"] = f"/queue?tab={phase}&page={page + 1}"
        if page > 1:
            model["back_href"] = f"/queue?tab={phase}&page={page - 1}"
        return model

    def ask_list(phase, page):
        return list_model(phase, page, ask("queue.jobs", {"phase": phase, "limit": PAGE_SIZE, "offset": (page - 1) * PAGE_SIZE}))

    def tab_work(tab, page):
        """The worker of a tab: the numbers, on the Pending tab the auto-accept panel, then each list of the tab (the list `tab` names at `page`, any other at its first page)."""
        def work(session):
            counts = ask("queue.counts")
            if unavailable(counts):
                return tab_base(tab, page, _failure(counts[1]))
            model = tab_base(tab, page)
            kind, value = counts
            if kind == "ok" and isinstance(value, dict) and all(isinstance(value.get(key), int) and not isinstance(value.get(key), bool) for key in ("pending", "approved", "running")):
                model["counts"] = {key: value[key] for key in ("pending", "approved", "running")}
                model["subbar_counts"] = {"queue:pending": value["pending"], "queue:running": value["approved"] + value["running"]}
            else:
                model["counts_failure"] = _failure(value)["sentence"] if kind == "error" else SHAPE
            if model["tab"] == "pending":
                model["auto_accept"] = auto_accept_model()
            model["lists"] = [ask_list(phase, page if phase == tab else 1) for phase in LISTS_OF[model["tab"]]]
            return model

        return work

    def _page_of(request):
        text = request.query_params.get("page")
        if text is None:
            return 1
        if not re.fullmatch(PAGE_PATTERN, text) or int(text) > PAGE_MAX:
            return None
        return int(text)

    @api.get("/queue")
    async def queue_page(request: Request):
        tab = request.query_params.get("tab", "pending")
        page = _page_of(request)
        if tab not in TABS or page is None:
            return _not_found()
        return await show(request, f"queue:{TAB_OF[tab]}", f"Queue: {TAB_TITLES[tab]}", "queue.html", tab_work(tab, page))

    @api.get("/queue/running-rows")
    async def running_rows(request: Request):
        page = _page_of(request)
        if page is None:
            return _not_found()
        session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))

        def work(_session):
            listed = ask("queue.jobs", {"phase": "running", "limit": PAGE_SIZE, "offset": (page - 1) * PAGE_SIZE})
            return list_model("running", page, listed)

        made = await ctx.door.run(lambda: checked(session, work))
        if made is BUSY_GATE:
            made = list_base("running", page, {"sentence": BUSY, "code": "busy"})
        elif isinstance(made, _BrokerRefused):
            ending = refusal_of(request, session_id, made.error)
            if ending is not None:
                return ending
            # The broker's sentence; the part asks again, as for a busy answer.
            made = list_base("running", page, broker_failure(made.error))
        # The rows carry forms (kill): the part needs the session's form token.
        return await anyio.to_thread.run_sync(lambda: ctx.render("_queue_rows.html", rows=made, form_token=session.form_token))

    # ── auto-accept ──

    def auto_accept_model():
        """The targets' auto-accept settings, for the auto-accept page and the Pending tab's panel, in the calling worker: a failure is said in the model, never raised."""
        base = {"failure": None, "rows": None, "max_limit": None, "max_limit_text": None, "step_up_note": STEP_UP_NOTE, "never_auto_note": NEVER_AUTO_SENTENCE}
        kind, value = ask("queue.auto_accept_state")
        if kind == "error":
            return {**base, "failure": _failure(value)}
        try:
            max_limit, rows = _auto_accept_reply(value)
        except ValueError:
            return {**base, "failure": {"sentence": SHAPE, "code": "shape"}}
        return {**base, "rows": rows, "max_limit": max_limit, "max_limit_text": rows_text(max_limit)}

    def auto_accept_work(session):
        return auto_accept_model()

    @api.get("/queue/auto-accept")
    async def auto_accept_page(request: Request):
        return await show(request, "queue:auto-accept", "Queue: Auto-accept", "queue_auto_accept.html", auto_accept_work)

    # ── one job ──

    def job_work(job_id):
        def work(session):
            kind, value = ask("queue.job", {"job_id": job_id})
            base = {"job_id": job_id, "failure": None, "too_large": False, "job": None, "parts": None, "parts_line": None, "explain_sentence": EXPLAIN_SENTENCE, "kill_sentence": KILL_SENTENCE, "hints_label": HINTS_LABEL, "hints_note": HINTS_NOTE, "no_hints": NO_HINTS, "escaped_sentence": ESCAPED_SENTENCE, "reason_max": actions.REASON_MAX}
            if kind == "error":
                code = getattr(value, "code", None)
                base["failure"] = _failure(value)
                base["too_large"] = code == "too_large"
                if code == "not_found":
                    base["_status"] = 404
                return base
            try:
                phase, record, _digest = _job_reply(value, job_id)
            except ValueError:
                base["failure"] = {"sentence": SHAPE, "code": "shape"}
                return base
            base["job"] = job_model(phase, record)
            if base["job"]["done"]:
                if scheduler.results is None:
                    base["parts_line"] = NO_RESULTS
                else:
                    try:
                        found = results.list_parts(scheduler.results, job_id, record.get("output_filename"))
                    except results.NoParts:
                        found = []
                    base["parts"] = [{"number": number, "name": shown(name), "size": size_text(size), "href": f"/queue/{job_id}/download/{number}"} for number, (name, size) in enumerate(found)]
                    if not found:
                        base["parts_line"] = NO_PARTS
            return base

        return work

    @api.get("/queue/{job_id}")
    async def job_page(request: Request, job_id: str):
        if not actions.is_job_id(job_id):
            return _not_found()
        work = job_work(job_id)
        # `?preview=1` (a History row's "Preview"): the page loads the preview at once.
        preview_now = request.query_params.get("preview") == "1"
        return await show(request, "queue:job", f"Job {job_id}", "queue_job.html", lambda session: {**work(session), "preview_now": preview_now})

    @api.get("/queue/{job_id}/preview")
    async def preview_part(request: Request, job_id: str):
        if not actions.is_job_id(job_id):
            return _not_found()
        session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))

        def work():
            kind, value = ask("queue.preview", {"job_id": job_id})
            if kind == "error":
                return {"failure": _failure(value), "preview": None}
            try:
                return {"failure": None, "preview": _preview_model(value)}
            except ValueError:
                return {"failure": {"sentence": SHAPE, "code": "shape"}, "preview": None}

        made = await ctx.door.run(lambda: checked(session, lambda _session: work()))
        if made is BUSY_GATE:
            made = {"failure": {"sentence": BUSY, "code": "busy"}, "preview": None}
        elif isinstance(made, _BrokerRefused):
            ending = refusal_of(request, session_id, made.error)
            if ending is not None:
                return ending
            made = {"failure": broker_failure(made.error), "preview": None}
        return await anyio.to_thread.run_sync(lambda: ctx.render("_queue_preview.html", **made))

    @api.get("/queue/{job_id}/download/{part}")
    async def download(request: Request, job_id: str, part: str):
        if not actions.is_job_id(job_id) or not re.fullmatch(PART_PATTERN, part):
            return _not_found()
        session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))
        if scheduler.results is None:
            return _not_found()
        if not ctx.downloads.acquire(blocking=False):
            return PlainTextResponse(DOWNLOADS_BUSY + "\n", status_code=503)
        # What the worker opened, so that it is closed here when the answer never takes it over (the request cancelled while the worker ran).
        held = []
        handed_over = False
        try:
            def work(_session):
                kind, value = ask("queue.job", {"job_id": job_id})
                if kind == "error":
                    return None
                try:
                    phase, record, _digest = _job_reply(value, job_id)
                except ValueError:
                    return None
                if phase != "history" or record.get("status") != "done":
                    return None
                try:
                    held.append(results.open_part(scheduler.results, job_id, record.get("output_filename"), int(part)))
                except results.NoParts:
                    return None
                return held[0]

            # A download always asks the broker first: no belief of an earlier answer.
            opened = await ctx.door.run(lambda: checked(session, work, cached=False))
            if opened is BUSY_GATE:
                return PlainTextResponse(BUSY + "\n", status_code=503)
            if isinstance(opened, _BrokerRefused):
                ending = refusal_of(request, session_id, opened.error)
                if ending is not None:
                    return ending
                return PlainTextResponse(words.sentence(opened.error) + "\n", status_code=503)
            if opened is None:
                return _not_found()
            fd, size, name = opened
            stream = results.PartStream(fd, size, name, ctx.downloads.release)
            handed_over = True
            return stream
        finally:
            if not handed_over:
                for fd, _size, _name in held:
                    try:
                        os.close(fd)
                    except OSError:
                        pass
                ctx.downloads.release()

    # ── the actions ──

    def refused(session, line, status=400, back=("/queue", "Back to the Queue")):
        return ctx.frame("refused.html", "queue:pending", session, status, title="Not taken", line=line, field=None, back=back)

    async def make_pending(request, session, name, form):
        """`POST /pending` for an entry of the Queue: checked, and kept as a pending action (an approval and a change of auto-accept ask the socket first, in a worker of its bound)."""
        by = by_of(session)
        if by is None:
            return refused(session, NO_NUMBER)
        now = ctx.clock
        if name in ("queue.approve", "queue.kill", "queue.explain"):
            job_id = form.get("job_id")
            if set(form) != {"job_id"} or not actions.is_job_id(job_id):
                return refused(session, words_refused())
            back = ("/queue/" + job_id, "Back to the job")
            if name != "queue.approve":
                fields = {"job_id": job_id, "by": by}
                mark, answer = await assistant_mark(request, session, job_id, back)
                if answer is not None:
                    return answer
                kept = {"assistant": mark}
                pending = session.work.add_pending(name, fields, now(), shown={"model": build_pending_model(name, fields, kept)})
                return RedirectResponse(f"/pending/{pending.id}", status_code=303)
            answer = await ctx.door.run(lambda: checked(session, lambda _session: ask("queue.job", {"job_id": job_id})))
            if answer is BUSY_GATE:
                return refused(session, BUSY, 503, back)
            if isinstance(answer, _BrokerRefused):
                return refusal_of(request, request.scope.get("sdp.session_id"), answer.error) or refused(session, words.sentence(answer.error), 503, back)
            kind, value = answer
            if kind == "error":
                return refused(session, words.door_sentence(value), 503 if getattr(value, "code", None) == "unreachable" else 400, back)
            try:
                phase, record, digest = _job_reply(value, job_id)
            except ValueError:
                return refused(session, SHAPE, 400, back)
            if phase != "pending":
                return refused(session, f"{NOT_PENDING_NOW} {PHASE_WORDS[phase]}.", 409, back)
            fields = {"job_id": job_id, "digest": digest, "by": by}
            kept = {"record": record}
            # The page's model is made once, here, in a worker: a statement of a megabyte is cut into its runs once, not at every view.
            model = await anyio.to_thread.run_sync(lambda: build_pending_model(name, fields, kept))
            weight = len(json.dumps(record, ensure_ascii=False).encode("utf-8", "replace")) + model_weight(model)
            pending = session.work.add_pending(name, fields, now(), shown={**kept, "model": model}, weight=weight)
            if pending is None:
                return refused(session, TOO_HEAVY, 413, back)
            return RedirectResponse(f"/pending/{pending.id}", status_code=303)
        if name == "queue.reject":
            job_id, reason = form.get("job_id"), form.get("reason", "")
            if not set(form) <= {"job_id", "reason"} or not actions.is_job_id(job_id):
                return refused(session, words_refused())
            if not actions.is_reason(reason):
                return refused(session, REASON_REFUSED, 400, ("/queue/" + job_id, "Back to the job"))
            fields = {"job_id": job_id, "reason": reason, "by": by}
            mark, answer = await assistant_mark(request, session, job_id, ("/queue/" + job_id, "Back to the job"))
            if answer is not None:
                return answer
            kept = {"assistant": mark}
            pending = session.work.add_pending(name, fields, now(), shown={"model": build_pending_model(name, fields, kept)})
            return RedirectResponse(f"/pending/{pending.id}", status_code=303)
        if name == "queue.auto_accept":
            return await make_auto_accept(request, session, form, by)
        return refused(session, words_refused())

    async def assistant_mark(request, session, job_id, back):
        """(mark, answer) for a reject, kill or EXPLAIN page: whether the job's record carries the assistant's mark (True or False; None when it could not be read: the web UI's bound busy, the socket's refusal or failure, another shape), read from the socket only after the broker confirmed the session; `answer` is the page to give instead when the broker did not (the login for an ended session, else its sentence), and nothing is kept then. Nothing of the record but the mark is kept."""
        answer = await ctx.door.run(lambda: checked(session, lambda _session: ask("queue.job", {"job_id": job_id})))
        if isinstance(answer, _BrokerRefused):
            return None, refusal_of(request, request.scope.get("sdp.session_id"), answer.error) or refused(session, words.sentence(answer.error), 503, back)
        if answer is BUSY_GATE or answer[0] != "ok":
            return None, None
        try:
            _phase, record, _digest = _job_reply(answer[1], job_id)
        except ValueError:
            return None, None
        return assistant_label(record) is not None, None

    def words_refused():
        return "The action was not taken: a field of the form was refused. Nothing was sent to the scheduler."

    async def make_auto_accept(request, session, form, by):
        back = ("/queue/auto-accept", "Back to auto-accept")
        target, enabled_text, limit_text = form.get("target"), form.get("enabled"), form.get("limit")
        if set(form) != {"target", "enabled", "limit"} or enabled_text not in ("on", "off") or not isinstance(limit_text, str) or not re.fullmatch(r"[0-9]{1,%d}" % actions.LIMIT_DIGITS, limit_text):
            return refused(session, words_refused(), 400, back)
        if not _is_target(target):
            return refused(session, UNKNOWN_TARGET, 400, back)
        answer = await ctx.door.run(lambda: checked(session, lambda _session: ask("queue.auto_accept_state")))
        if answer is BUSY_GATE:
            return refused(session, BUSY, 503, back)
        if isinstance(answer, _BrokerRefused):
            return refusal_of(request, request.scope.get("sdp.session_id"), answer.error) or refused(session, words.sentence(answer.error), 503, back)
        kind, value = answer
        if kind == "error":
            return refused(session, words.door_sentence(value), 503 if getattr(value, "code", None) == "unreachable" else 400, back)
        try:
            max_limit, rows = _auto_accept_reply(value)
        except ValueError:
            return refused(session, SHAPE, 400, back)
        row = next((row for row in rows if row["name"] == target), None)
        if row is None:
            return refused(session, UNKNOWN_TARGET, 400, back)
        wanted_enabled, wanted_limit = enabled_text == "on", int(limit_text)
        if not 0 <= wanted_limit <= max_limit:
            return refused(session, LIMIT_REFUSED.format(max_limit=rows_text(max_limit)), 400, back)
        enabled = wanted_enabled if wanted_enabled != row["enabled"] else None
        limit = wanted_limit if wanted_limit != row["limit"] else None
        if enabled is None and limit is None:
            return refused(session, NOTHING_TO_CHANGE, 400, back)
        step_up = escalates(enabled, limit, row["enabled"], row["limit"])
        fields = {"target": target, "enabled": enabled, "limit": limit, "by": by}
        kept = {"current": (row["enabled"], row["limit"]), "backend": row["backend"], "database": row["database"], "step_up": step_up}
        pending = session.work.add_pending("queue.auto_accept", fields, ctx.clock(), step_up=step_up, shown={**kept, "model": build_pending_model("queue.auto_accept", fields, kept)})
        return RedirectResponse(f"/pending/{pending.id}", status_code=303)

    def pending_model(pending, now, seconds):
        """What the pending page shows of an action of the Queue: the model made once when the action was kept (`build_pending_model`), with its id and the minutes it is still kept."""
        kept = pending.shown or {}
        model = kept.get("model") or build_pending_model(pending.action, pending.fields or {}, kept)
        return {**model, "id": pending.id, "step_up": pending.step_up, "minutes_left": max(1, math.ceil((seconds - (now - pending.created)) / 60))}

    async def render_pending(frame):
        """A pending page of the Queue rendered in a worker (`frame()` renders it): its statement can be a megabyte of runs."""
        return await anyio.to_thread.run_sync(frame)

    def active(name):
        return "queue:auto-accept" if name == "queue.auto_accept" else "queue:pending"

    def start(session, taken, password, outcome):
        """Run a confirmed action of the Queue in a thread of its own, holding a place of the socket's bound (taken by the caller): the session's check at the broker, the management password's when it is a step-up, the settings' second look for auto-accept, then the socket."""
        token, work = session.token, session.work
        name, fields = taken.action, dict(taken.fields)
        kept_current = (taken.shown or {}).get("current")
        job_id = fields.get("job_id")
        outcome.back = ("/queue/" + job_id, "Back to the job") if actions.is_job_id(job_id) else ("/queue/auto-accept", "Back to auto-accept") if name == "queue.auto_accept" else ("/queue", "Back to the Queue")

        def broker_call(entry, entry_fields, step_up=None):
            return call_broker(lambda: ctx.broker(token=token, timeout=limits.read_seconds), lambda made: actions.call(made, entry, entry_fields, step_up))

        def run():
            try:
                kind, value = broker_call("audit.list", {"limit": 1})
                if kind == "error":
                    # Marked, so that the confirmation shows no record to a session the broker did not confirm; no earlier yes outlives it.
                    work.broker_checked_at = None
                    outcome.at_session_check = True
                    outcome.finish("error", value, limits.outcome_text_bytes)
                    return
                if password is not None:
                    kind, value = broker_call("session.check_password", {}, password)
                    if kind == "error":
                        outcome.finish("error", value, limits.outcome_text_bytes)
                        return
                if name == "queue.auto_accept":
                    refusal = _still_allowed(fields, kept_current)
                    if refusal is not None:
                        # Not the change: nothing was sent, whatever the second look met.
                        outcome.finish("error", refusal, limits.outcome_text_bytes, from_door=True)
                        return
                kind, value = ask(name, fields)
                # The one call whose lost reply may mean that the change was made.
                outcome.finish(kind, value, limits.outcome_text_bytes, from_door=True, sent=True)
            finally:
                ctx.door.give_back()

        threading.Thread(target=run, name="sdp-webui-queue-action", daemon=True).start()

    def _still_allowed(fields, kept_current):
        """None when the change of auto-accept may still be sent: the target's settings are read again and must be those the pending page showed as "Now", else its "After" line would not be what is set. This also covers a change that became a step-up meanwhile: whether the management password is asked was decided against those same settings. Else the error to show."""
        kind, value = ask("queue.auto_accept_state")
        if kind == "error":
            return _OwnRefusal("settings_not_read")
        try:
            _max_limit, rows = _auto_accept_reply(value)
        except ValueError:
            return _OwnRefusal("bad_reply")
        row = next((row for row in rows if row["name"] == fields["target"]), None)
        if row is None:
            return _OwnRefusal("unknown_target")
        if kept_current is None or (row["enabled"], row["limit"]) != tuple(kept_current):
            return _OwnRefusal("settings_changed")
        return None

    return Hooks(make_pending=make_pending, pending_model=pending_model, render_pending=render_pending, guard=guard, start=start, active=active, places=ctx.door)


def build_pending_model(name, fields, kept):
    """What the pending page of an action of the Queue shows, made once when the action is kept: for an approval the job's model from the whole record (the statement cut into its runs, its hints), the sentences of each action. `kept` is what the action keeps besides its fields (the record; for auto-accept the target's settings then)."""
    action = actions.TABLE[name]
    job_id = fields.get("job_id")
    model = {
        "queue": True, "title": action.title, "verb": action.name, "kind": name.split(".", 1)[1],
        "command": None, "no_command": None, "inputs": [], "destroys": None, "terminal_asks": "", "note": "", "also": None,
        "job_id": job_id, "job_href": job_href(job_id), "sentences": [], "job": None, "reason": None, "auto_accept": None,
        "assistant": ASSISTANT_LABEL if kept.get("assistant") is True else None, "assistant_sentence": None,
        # Kept for a reject, kill or EXPLAIN: True, False, or None when the job could not be read.
        "assistant_unknown": ASSISTANT_UNKNOWN if "assistant" in kept and kept["assistant"] is None else None,
        "hints_label": HINTS_LABEL, "hints_note": HINTS_NOTE, "no_hints": NO_HINTS, "escaped_sentence": ESCAPED_SENTENCE, "overwrite_sentence": OVERWRITE_SENTENCE,
        "leave_href": job_href(job_id) or "/queue",
    }
    if name == "queue.approve" and isinstance(kept.get("record"), dict):
        model["job"] = job_model("pending", kept["record"])
        model["assistant"] = model["job"]["assistant"]
        model["assistant_sentence"] = ASSISTANT_SENTENCE if model["assistant"] else None
        model["sentences"] = [ADMIN_SENTENCE, NO_CHECK_SENTENCE]
    elif name == "queue.reject":
        model["reason"] = shown(fields.get("reason") or "(none)")
        model["sentences"] = [REJECT_SENTENCE]
    elif name == "queue.kill":
        model["sentences"] = [KILL_SENTENCE]
    elif name == "queue.explain":
        model["sentences"] = [EXPLAIN_SENTENCE]
        if model["assistant"]:
            model["sentences"].insert(0, EXPLAIN_ASSISTANT_SENTENCE)
        elif model["assistant_unknown"]:
            model["sentences"].insert(0, EXPLAIN_MAYBE_SENTENCE)
    elif name == "queue.auto_accept":
        current_enabled, current_limit = kept.get("current", (False, 0))
        enabled = fields.get("enabled")
        limit = fields.get("limit")
        after_enabled = current_enabled if enabled is None else enabled
        after_limit = current_limit if limit is None else limit
        model["leave_href"] = "/queue/auto-accept"
        model["auto_accept"] = {
            "target": shown(fields.get("target")), "backend": kept.get("backend"), "database": kept.get("database"),
            "before": ("on" if current_enabled else "off", rows_text(current_limit)), "after": ("on" if after_enabled else "off", rows_text(after_limit)),
        }
        if after_enabled and (enabled is True or (limit is not None and limit > current_limit)):
            model["sentences"].append(AUTO_ON_SENTENCE.format(target=statements.shown_text(fields.get("target", "")), limit=rows_text(after_limit)))
        if kept.get("step_up"):
            model["sentences"].append(STEP_UP_NOTE)
    return model


# What one run of a text costs in memory besides its characters: the tuple, the flag and the string object (measured about 120 bytes on CPython 3.13 for short runs).
RUN_BYTES = 128


def model_weight(value):
    """About the bytes a pending page's model holds, walked whole: every run of escaped text anywhere in it (the statement, the description, the error, the reason, the target, the output file's name...) at RUN_BYTES plus its length, every other text at its length, nested lists, tuples and dicts walked. Counted, so that the session's cap on pending actions is what they hold."""
    if isinstance(value, str):
        return len(value)
    if isinstance(value, tuple) and len(value) == 2 and isinstance(value[0], bool) and isinstance(value[1], str):
        return RUN_BYTES + len(value[1])
    if isinstance(value, dict):
        return sum(model_weight(key) + model_weight(item) for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return sum(model_weight(item) for item in value)
    return 0


class _OwnRefusal(Exception):
    """A refusal the web UI makes itself for an action of the Queue, worded like the socket's."""

    def __init__(self, code):
        super().__init__(code)
        self.code = code
        self.details = {}


def _preview_model(value):
    """What the preview part shows of a `preview` reply; raises ValueError."""
    if not isinstance(value, dict):
        raise ValueError("not a reply")
    columns, rows, parts = value.get("columns"), value.get("rows"), value.get("parts")
    if not isinstance(columns, list) or not isinstance(rows, list) or not isinstance(parts, list):
        raise ValueError("not a preview")
    for row in rows:
        if not isinstance(row, list):
            raise ValueError("not a row")
    for part in parts:
        if not isinstance(part, dict):
            raise ValueError("not a part")
    return {
        "format": shown(value.get("format")),
        "columns": [statements.shown_text(cell) if isinstance(cell, str) else "-" for cell in columns],
        "rows": [[statements.shown_text(cell) if isinstance(cell, str) else "-" for cell in row] for row in rows],
        "parts": [{"name": shown(part.get("name")), "size": size_text(part.get("size"))} for part in parts],
        "parts_total": rows_text(value.get("parts_total")),
        "read_bytes": size_text(value.get("read_bytes")),
        "truncated": value.get("truncated") is True,
    }
