"""The Chat window: the operator asks questions about the data and an assistant in the chat's sandbox answers, through the chat's runner (`assistant.Assistant`).

Every route first asks the broker whether the session still lives (`queue.broker_checked`: `audit.list` with a limit of 1, its yes believed 5 seconds for a page or a part, never for a change, a download or an image), in a worker of the window's bound (`Limits.chat_workers`, four places; "busy" when full), and the runner only then, in the same worker; pages are rendered in a worker too. Everything the runner sends is hostile: each answer is checked whole by `assistant`, each event by `event_model` (an event of another type or shape is one line saying so, nothing of it shown), every text goes through `statements.visible` and the template's escaping, the assistant's text through `prose` (data, never markup), a file's name becomes a link only when it follows the runner's rule, and nothing of an answer reaches a header or a script.

Both pages are the mock-up's three columns (`chat.html`): on the left the chats, newest first (title, a dot while a turn runs), a button for a new chat and the data in reach (the databases the chat can read and whether the assistant can submit to the query queue, from `hello`); in the middle the open chat; on the right the sandbox (`hello`) and the open chat's files. A failure of `hello` is shown in the side columns only.

- `GET /chat`: the three columns with no chat open (`chat.list`, `hello`). `POST /chat/new` makes one (`chat.new`) and leads to it.
- `GET /chat/<id>` (an id of the runner's form, else not found): the chats (the list `chat.list` gave when the chat was looked up: the runner is not asked twice), the first 200 events rendered here, a box to send a question, a stop button while a turn runs, the chat's files (`chat.files`) with their download links, the sandbox (`hello`), the way to delete it. `GET /chat/<id>?from=N` starts the view at the event N. `GET /chat/<id>/events?offset=N&from=F` is the htmx part that appends the events from N on and carries the next request: every 2 seconds while a turn runs, at once after an answer that brought events, none once the turn has ended and nothing more came (`HX-Redirect` to the login for a session that ended); a view appends at most 2,000 events, then offers to continue from there. A page of events is at most 512 KiB of text (`assistant`) and its model at most `Limits.chat_units_max` units (runs of text, blocks and items): an event beyond them is a line with a link to a view that starts at it, the first event of a page is always shown (cut when it alone is too long), and the model is built and rendered in the worker that asked.
- `POST /chat/<id>/send` (`text`, 1 to 32,000 characters once its line ends are `\\n`; a refused text is shown again in the box with its sentence), `POST /chat/<id>/stop`, `GET /chat/<id>/delete` (a page that changes nothing) and `POST /chat/<id>/delete`; each change leads back with a redirect to a GET.
- `GET /chat/<id>/table/<name>` (a name of the runner's rule ending in `.csv`): the first 256 KiB of the file (one `chat.file` piece, the broker asked first and never believed from before) as a table in the frame: decoded as UTF-8 with errors replaced, parsed by `csv` (strict), at most 500 rows under the header row and 50 columns, the cells within the page's units (`Limits.chat_units_max`), every cell through `statements.visible` and the template's escaping; a file that is not CSV (a NUL, a quote that does not close where the file was read whole, a field over `csv`'s limit) is one sentence.
- `GET /chat/<id>/queries`: the right column's "This chat's queries in the queue", a part the page asks once it has loaded and every 60 seconds after (`queries_model`), on a bound of its own (`Limits.chat_query_workers`; a full bound answers 204 and the part stays as it is). The broker is asked first (`queue.broker_session_error`, a short time limit: an ended session gets `HX-Redirect`, a broker that does not answer is the part's one line and nothing else is asked); then the chat's own events (at most 2,000, read from the start, the runner's pages) give the job ids written in the texts of its tool calls and their results, newest first, at most 20; then the scheduler's `queue.job` for each, with a short time limit and a total one. Only a job whose record says it was submitted by the assistant is listed; an unknown id or another submitter's job is left out without a word, a failure of one job is its row.
- `GET /chat/<id>/file/<name>`: the file streamed from `chat.file` pieces as an attachment (`application/octet-stream`, the name reduced as `results.download_name` does, `Content-Security-Policy: sandbox` besides the guard's headers), its length the size the first piece gave, every later piece checked against it (a file that changes size ends the answer without sending more than announced), at most 1 GiB, two at a time, every piece but the last a whole piece, within 30 minutes. `GET /chat/<id>/image/<name>`: the whole file (at most 8 MiB, read within 30 seconds) served inline only when its first bytes are a PNG or JPEG signature, with the type of that signature, never of the name; anything else is not found. The web UI opens no file for either: the bytes come only through the socket.

Every route answers GET or POST only (a HEAD is 405); every POST passes the guard's origin and form-token rule. Standard library, anyio, FastAPI, Starlette and this package only.
"""

import collections
import csv
import io
import logging
import os
import re
import threading
import urllib.parse

import anyio
from fastapi import Request
from fastapi.responses import PlainTextResponse, RedirectResponse
from starlette.responses import Response

from social_data_pipeline.control import client
from social_data_pipeline.webui import actions, assistant, prose, queue, results, statements, words
from social_data_pipeline.webui.calls import BUSY_GATE, Gate, call_broker
from social_data_pipeline.webui.scheduler import SchedulerUnavailable

log = logging.getLogger("social_data_pipeline.webui")

CHAT_PAGES = (("chat:list", "Chats", "/chat"),)
# Whether the assistant can submit to the query queue, as `hello` says (the queue's line under "Data in reach").
QUEUE_ON = "Queue · long queries, after approval"
QUEUE_OFF = "Queue · not attached"
# The names the page gives the databases `hello` lists; any other name is shown as it is.
DATABASE_NAMES = {"postgres": "PostgreSQL", "mongo": "MongoDB", "starrocks": "StarRocks"}
# A tool name of the form `mcp__<server>__<tool>` (a tool of an MCP server, as Claude Code names it), shown as "<server> · <tool>".
MCP_TOOL = re.compile(r"mcp__([A-Za-z0-9.-][A-Za-z0-9_.-]*?)__([A-Za-z0-9_.-]+)")
# The glyph of a file of the chat by its name's ending (the mock-up's), any other file the last one.
FILE_GLYPHS = (((".png", ".jpg", ".jpeg", ".svg"), "▨"), ((".csv", ".parquet", ".json", ".jsonl", ".tsv", ".xlsx"), "▦"))
OTHER_GLYPH = "◧"
EVENTS_PAGE = 200
POLL = "load delay:2s"
NOW = "load"
RETRY = "load delay:5s"
OFFSET_PATTERN = r"[0-9]{1,7}"
EVENT_TEXT_MAX = 64 * 1024
TOOL_NAME = re.compile(r"[A-Za-z0-9_.-]{1,80}")
IMAGE_SUFFIXES = (".png", ".jpg", ".jpeg")
SIGNATURES = ((b"\x89PNG\r\n\x1a\n", "image/png"), (b"\xff\xd8\xff", "image/jpeg"))
SANDBOX_POLICY = "sandbox"
NOT_FOUND = "Not found.\n"
BUSY = "The web UI is busy with other requests to the chat: try again in a moment."
DOWNLOADS_BUSY = "The web UI already sends two of the chat's files: try again when one has ended."
TOO_LARGE = "The file is larger than 1 GiB, the most the web UI sends: copy it from the chat's work folder on this machine (.sdp/chat/work)."
TEXT_REFUSED = "The question was not sent: it must be 1 to 32,000 characters. It is kept in the box."
FORM_REFUSED = "The request was not taken: a field of the form was refused. Nothing was sent to the chat."
NOT_UNDERSTOOD_EVENT = "An event this page does not understand (nothing of it is shown)."
TOO_LONG_EVENT = "This event is too long to show together with the ones above (a page of events shows at most 100,000 pieces of text and blocks)."
FOLLOWING_EVENT = "One more event follows here: continue from the event above to see it."
SHOWN_CUT = "This event is cut here: it is too long to show whole."
# The field holding an event's text, which is cut when the event alone is too long to show.
TEXT_FIELDS = {"user": "text", "text": "text", "tool": "input", "tool_result": "text", "error": "message"}
# The most events one view of a chat appends; then it offers to continue from there in a fresh view.
VIEW_EVENTS_MAX = 2000
COST_MAX = 10 ** 9
# The plot card's buttons (`file_buttons`): the label of an image's own download by its ending, and the files of the chat's list with the same stem as the image whose ending (lower-cased) is one of these.
IMAGE_LABELS = {".png": "PNG", ".jpg": "JPEG", ".jpeg": "JPEG"}
DATA_ENDINGS = ((".csv", "Data (CSV)"), (".tsv", "Data (TSV)"), (".parquet", "Data (Parquet)"), (".json", "Data (JSON)"))
CODE_ENDING = ".py"
TABLE_ENDING = ".csv"
# The table view: the bytes read (one piece), the rows under the header row and the columns shown.
TABLE_BYTES = 256 * 1024
TABLE_ROWS_MAX = 500
TABLE_COLUMNS_MAX = 50
TABLE_NOT_CSV = "This file cannot be shown as a table: it is not CSV text. Download it instead."
TABLE_EMPTY = "The file is empty."
# The right column's queries: a job id of the scheduler's form inside a text, not part of a longer word.
JOB_ID_IN_TEXT = re.compile(rf"(?<![A-Za-z0-9_])(?:{actions.JOB_ID_PATTERN})(?![A-Za-z0-9_])")
QUERIES_MAX = 20
QUERY_EVENTS_MAX = 2000
# The time limit of each call of the part, and of the whole part (looked at before each call).
QUERY_SECONDS = 5
QUERIES_SECONDS = 30
QUERIES_POLL = "every 60s"
QUERIES_NONE = "No query of this chat is in the queue."
QUERIES_NO_QUEUE = "The query queue is not set up for the web UI."
QUERIES_DETACHED = "The query queue is not attached to the chat's sandbox."
QUERIES_SEARCHED = "Only the first 2,000 events of this chat were searched for queries."
QUERIES_LATE = "Not every query was looked up in time."
QUERY_NOT_READ = "could not be read"
# A job's dot and words by its phase, and in the history by its record's status.
QUERY_PHASES = {"pending": ("warn", "waiting for approval"), "approved": ("run", "approved, waiting to start"), "running": ("run", "running")}
QUERY_ENDS = {"done": ("ok", "finished"), "failed": ("bad", "failed"), "rejected": ("bad", "rejected"), "cancelled": ("bad", "cancelled")}
QUERY_ENDED = ("", "ended")
EVENT_FIELDS = {
    "user": {"text"}, "text": {"text"}, "tool": {"name", "input"}, "tool_result": {"text", "is_error"},
    "file": {"name", "size"}, "end": {"is_error", "cost_usd", "duration_ms", "turns"}, "error": {"message"},
}


def _not_found():
    return PlainTextResponse(NOT_FOUND, status_code=404)


def _text(value, most=EVENT_TEXT_MAX):
    if not assistant.is_text(value, most):
        raise ValueError("not text")
    return value


def _optional_whole(value):
    if value is not None and not assistant.is_whole(value):
        raise ValueError("not a count")
    return value


def file_href(chat_id, name, route="file"):
    """The address of a file of a chat whose name follows the runner's rule (checked by the caller), the name quoted."""
    return f"/chat/{chat_id}/{route}/{urllib.parse.quote(name, safe='')}"


def stems_of(names):
    """{stem: {lower-cased ending: name}} of the names of a chat's files as `chat.files` listed them (each already of the runner's rule), the first name in sorted order kept for each ending: what the plot card's buttons are made from."""
    index = {}
    for name in sorted(names):
        stem, ending = os.path.splitext(name)
        index.setdefault(stem, {}).setdefault(ending.lower(), name)
    return index


def file_buttons(chat_id, name, stems):
    """The buttons of a file's card, [{"label", "href"}], for a name of the runner's rule. An image: its own download ("PNG"), then the files of `stems` (the chat's list, `stems_of`; None when it could not be read) with the same stem: the data ("Data (CSV)", by its ending), the code ("Code", `.py`) and "Table view" of the `.csv`. Any other file: "Download", and "Table view" when it is a `.csv` itself. No other name becomes a link."""
    stem, ending = os.path.splitext(name)
    ending = ending.lower()
    if ending not in IMAGE_LABELS:
        buttons = [{"label": "Download", "href": file_href(chat_id, name)}]
        if ending == TABLE_ENDING:
            buttons.append({"label": "Table view", "href": file_href(chat_id, name, "table")})
        return buttons
    same = (stems or {}).get(stem, {})
    buttons = [{"label": IMAGE_LABELS[ending], "href": file_href(chat_id, name)}]
    buttons += [{"label": label, "href": file_href(chat_id, same[found])} for found, label in DATA_ENDINGS if found in same]
    if CODE_ENDING in same:
        buttons.append({"label": "Code", "href": file_href(chat_id, same[CODE_ENDING])})
    if TABLE_ENDING in same:
        buttons.append({"label": "Table view", "href": file_href(chat_id, same[TABLE_ENDING], "table")})
    return buttons


def event_model(event, chat_id, stems=None):
    """What the page shows of one event of the runner: a dict with `kind` (the event's type, or `unknown` for any event of another type or shape, of which nothing is shown). `stems` (`stems_of` the chat's files, or None) gives a file's card its buttons."""
    try:
        kind = event.get("type") if isinstance(event, dict) else None
        if not isinstance(kind, str) or kind not in EVENT_FIELDS or set(event) != EVENT_FIELDS[kind] | {"n", "t", "type"}:
            raise ValueError("not an event")
        if not assistant.is_whole(event["n"], 0, assistant.OFFSET_MAX) or not (event["t"] is None or assistant.is_time(event["t"])):
            raise ValueError("not an event")
        if kind == "user":
            return {"kind": kind, "parts": statements.visible(_text(event["text"]))[0]}
        if kind == "text":
            found, was_cut = prose.blocks(_text(event["text"]))
            return {"kind": kind, "blocks": found, "cut": was_cut}
        if kind == "tool":
            name = _text(event["name"], 200)
            shown = name if TOOL_NAME.fullmatch(name) else None
            server = MCP_TOOL.fullmatch(shown) if shown else None
            return {"kind": kind, "name": shown, "server": server.group(1) if server else None, "tool": server.group(2) if server else None, "input": statements.visible(_text(event["input"]))[0]}
        if kind == "tool_result":
            if not isinstance(event["is_error"], bool):
                raise ValueError("not a flag")
            return {"kind": kind, "parts": statements.visible(_text(event["text"]))[0], "is_error": event["is_error"]}
        if kind == "file":
            name = _text(event["name"], 1024)
            if not assistant.is_whole(event["size"]):
                raise ValueError("not a size")
            usable = assistant.is_file_name(name)
            return {"kind": kind, "name": statements.visible(name)[0], "size": queue.size_text(event["size"]), "href": file_href(chat_id, name) if usable else None, "image": file_href(chat_id, name, "image") if usable and name.lower().endswith(IMAGE_SUFFIXES) else None, "buttons": file_buttons(chat_id, name, stems) if usable else []}
        if kind == "end":
            cost, duration, turns = event["cost_usd"], _optional_whole(event["duration_ms"]), _optional_whole(event["turns"])
            if not isinstance(event["is_error"], bool) or (cost is not None and (isinstance(cost, bool) or not isinstance(cost, (int, float)))):
                raise ValueError("not an end")
            shown_cost = f"{cost:.4f}" if assistant.is_number(cost, 0, COST_MAX) else None
            return {"kind": kind, "is_error": event["is_error"], "seconds": f"{duration / 1000:.1f}" if duration is not None and duration < 10 ** 12 else None, "cost": shown_cost, "turns": turns if turns is not None and turns < 10 ** 6 else None}
        return {"kind": kind, "parts": statements.visible(_text(event["message"]))[0]}
    except Exception:
        # Any failure in modelling one event (a number no float holds included) is that event's line, never an error of the page.
        return {"kind": "unknown", "line": NOT_UNDERSTOOD_EVENT}


def count_units(value):
    """The units of one page's budget a model holds: every run of `statements.visible` (a pair of a flag and a text), every block, item and other object or list, walked without recursion."""
    count, todo = 0, [value]
    while todo:
        item = todo.pop()
        if isinstance(item, tuple) and len(item) == 2 and isinstance(item[0], bool) and isinstance(item[1], str):
            count += 1
        elif isinstance(item, dict):
            count += 1
            todo.extend(item.values())
        elif isinstance(item, (list, tuple)):
            count += 1
            todo.extend(item)
    return count


def fitted(event, chat_id, units_max, stems=None):
    """(the model of one event and its units) within `units_max`: an event whose model alone is larger is modelled again from the first half of its text, and again, until it fits, and is marked as cut, so that every event can be shown in a page of its own."""
    model = event_model(event, chat_id, stems)
    units = count_units(model)
    field = TEXT_FIELDS.get(model["kind"])
    if units <= units_max or field is None:
        return model, units
    length = len(event[field])
    while units > units_max and length:
        length //= 2
        model = event_model({**event, field: event[field][:length]}, chat_id, stems)
        units = count_units(model)
    model["shown_cut"] = SHOWN_CUT
    return model, units


def events_model(events, chat_id, units_max, offset, stems=None):
    """(the models of one page of events from `offset` on, the units they hold), at most `units_max` (`count_units`). The first event of a page is always shown, cut when it alone is too long (`fitted`); once the next event would pass the budget, it is the line TOO_LONG_EVENT with a link to a view that starts at it (`/chat/<id>?from=<its offset>`, made from `offset` and its place on the page only), and every later event of the page the line FOLLOWING_EVENT."""
    models, used, full = [], 0, False
    for index, event in enumerate(events):
        if not full:
            model, units = fitted(event, chat_id, units_max, stems) if not models else (event_model(event, chat_id, stems), None)
            units = count_units(model) if units is None else units
            if not models or used + units <= units_max:
                models.append(model)
                used += units
                continue
            full = True
            models.append({"kind": "skipped", "line": TOO_LONG_EVENT, "href": f"/chat/{chat_id}?from={offset + index}"})
            continue
        models.append({"kind": "skipped", "line": FOLLOWING_EVENT, "href": None})
    return models, used


def _ask(function):
    """One call of the runner, in the calling worker: ("ok", result) or ("error", the error). An exception of any other kind is an answer this page does not understand."""
    try:
        return "ok", function()
    except client.BrokerError as error:
        return "error", error
    except Exception as error:
        log.error("a call of the chat's runner failed (%s)", type(error).__name__)
        return "error", assistant.NotUnderstood()


def _status_of(error):
    code = getattr(error, "code", None)
    if code == "not_found":
        return 404
    return 503 if code in ("unreachable", "not_understood", "unavailable", "internal") else 409


def glyph_of(name):
    """The glyph shown before a file of the chat, by its name's ending."""
    lowered = name.lower()
    return next((glyph for endings, glyph in FILE_GLYPHS if lowered.endswith(endings)), OTHER_GLYPH)


def chat_rows(chats, current=None):
    """The chat list of the left column from the entries of `chat.list`: the address, the title's runs, whether a turn runs and whether it is the open chat `current`."""
    return [{"href": f"/chat/{chat['id']}", "title": statements.visible(chat["title"])[0], "running": chat["running"], "on": chat["id"] == current} for chat in chats]


def hello_model(hello):
    """What the side columns show of an answer of `hello`: the back end, the names of the databases the chat reads, whether the queue is attached and its line."""
    return {"backend": hello["backend"], "databases": [DATABASE_NAMES.get(name, name) for name in hello["databases"]], "queue": hello["queue"], "queue_line": QUEUE_ON if hello["queue"] else QUEUE_OFF}


def files_count(count, more):
    """The header's count of a chat's files."""
    return f"{count}{'+' if more else ''} file{'' if count == 1 and not more else 's'}"


def events_href(chat_id, offset, start):
    return f"/chat/{chat_id}/events?offset={offset}&from={start}"


def next_of(chat_id, start, offset, events, running):
    """`{"trigger", "next_href", "continue_href"}` after a page of events up to `offset` of a view that started at `start`: the next request (every 2 seconds while a turn runs, at once after events came), none once the turn has ended and nothing more came, and none but a link to continue in a fresh view once the view has shown VIEW_EVENTS_MAX events."""
    if offset - start >= VIEW_EVENTS_MAX:
        return {"trigger": None, "next_href": None, "continue_href": f"/chat/{chat_id}?from={offset}"}
    trigger = POLL if running else NOW if events else None
    return {"trigger": trigger, "next_href": events_href(chat_id, offset, start) if trigger else None, "continue_href": None}


def whole_in_bounds(text, high=assistant.OFFSET_MAX):
    """The number a query value writes (ASCII digits only), or None."""
    if not isinstance(text, str) or not re.fullmatch(OFFSET_PATTERN, text) or int(text) > high:
        return None
    return int(text)


def table_model(data, size, end, units_max):
    """What the table view shows of the first bytes `data` of a file of `size` bytes (`end`: they are all of it): {"header", "body": rows of cells, each the runs of `statements.visible`; "notes": what was left out; "line": the sentence instead of a table, or None}. The bytes are cut after their last line break when the file goes on, decoded as UTF-8 with errors replaced and parsed by `csv` (strict); a NUL, a parse error (but at the cut, after rows were read) or a field over `csv`'s limit is TABLE_NOT_CSV. At most TABLE_ROWS_MAX rows under the header row, TABLE_COLUMNS_MAX columns, and the rows whose cells fit in `units_max` (`count_units`; the first row always)."""
    model = {"header": [], "body": [], "notes": [], "line": None}
    cut = not end or len(data) < size
    if cut:
        last = data.rfind(b"\n")
        data = data[:last + 1] if last >= 0 else data
    text = data.decode("utf-8", errors="replace")
    if "\x00" in text:
        model["line"] = TABLE_NOT_CSV
        return model
    rows, used, more, wide = [], 0, False, False
    try:
        for row in csv.reader(io.StringIO(text, newline=""), strict=True):
            if len(rows) > TABLE_ROWS_MAX:
                more = True
                break
            wide = wide or len(row) > TABLE_COLUMNS_MAX
            cells = [statements.visible(cell)[0] for cell in row[:TABLE_COLUMNS_MAX]]
            units = count_units(cells)
            if rows and used + units > units_max:
                more = True
                break
            rows.append(cells)
            used += units
    except csv.Error:
        if not (cut and rows):
            model["line"] = TABLE_NOT_CSV
            return model
    if not rows:
        model["line"] = TABLE_EMPTY
        return model
    model["header"], model["body"] = rows[0], rows[1:]
    if cut:
        model["notes"].append(f"The file is {queue.size_text(size)}: only its first {queue.size_text(len(data))} are read here.")
    if more:
        model["notes"].append(f"The first {len(rows) - 1:,} rows under the header are shown.")
    if wide:
        model["notes"].append(f"The first {TABLE_COLUMNS_MAX} columns are shown: some rows have more.")
    return model


def job_ids_in(events, kept=None):
    """The job ids of the scheduler's form written in the texts of the tool calls (`input`) and their results (`text`) among `events` (raw, unchecked), as an ordered dict whose last key is the newest, at most QUERIES_MAX kept (`kept`, when given, is added to)."""
    kept = collections.OrderedDict() if kept is None else kept
    for event in events:
        if not isinstance(event, dict):
            continue
        kind = event.get("type")
        text = event.get("input") if kind == "tool" else event.get("text") if kind == "tool_result" else None
        if not isinstance(text, str):
            continue
        for match in JOB_ID_IN_TEXT.finditer(text):
            job_id = match.group(0)
            kept.pop(job_id, None)
            kept[job_id] = None
            if len(kept) > QUERIES_MAX:
                kept.popitem(last=False)
    return kept


def query_row(job_id, answer):
    """One row of the right column's queries from the answer of `queue.job` for `job_id` (`call_broker`'s pair), or None for a job to leave out: unknown to the scheduler, or not submitted by the assistant. A failure is a row that says so."""
    kind, value = answer
    row = {"id": job_id, "href": queue.job_href(job_id), "dot": "", "state": QUERY_NOT_READ}
    if kind == "error":
        return None if getattr(value, "code", None) == "not_found" else row
    try:
        phase, record, _digest = queue._job_reply(value, job_id)
    except ValueError:
        return row
    if not queue.assistant_label(record):
        return None
    status = record.get("status")
    row["dot"], row["state"] = QUERY_PHASES[phase] if phase in QUERY_PHASES else QUERY_ENDS.get(status, QUERY_ENDED) if isinstance(status, str) else QUERY_ENDED
    return row


def queries_model(runner, scheduler, chat_id, clock):
    """The right column's queries of the chat `chat_id`, asked in the calling worker once the broker has confirmed the session: {"line": one sentence instead of rows, or None; "rows"; "notes"}. The scheduler not given to the web UI is a line before anything is asked; then the chat's events from the start (at most QUERY_EVENTS_MAX, pages of the runner) give the job ids (`job_ids_in`), newest first, and the scheduler's `queue.job` (QUERY_SECONDS each) the rows (`query_row`). A scheduler that cannot be asked at all is the line; the total time QUERIES_SECONDS is looked at before each call."""
    deadline = clock() + QUERIES_SECONDS
    model = {"line": None, "rows": [], "notes": []}
    if not scheduler.available:
        model["line"] = QUERIES_NO_QUEUE
        return model
    kept, offset = collections.OrderedDict(), 0
    while True:
        asked = min(EVENTS_PAGE, QUERY_EVENTS_MAX - offset)
        kind, value = _ask(lambda: runner.events(chat_id, offset, asked))
        if kind == "error":
            model["line"] = words.chat_sentence(value)
            return model
        events, offset, _running = value
        job_ids_in(events, kept)
        if len(events) < asked:
            break
        if offset >= QUERY_EVENTS_MAX:
            model["notes"].append(QUERIES_SEARCHED)
            break
        if clock() > deadline:
            model["notes"].append(QUERIES_LATE)
            break
    if not kept:
        model["line"] = QUERIES_NONE
        return model
    for job_id in reversed(kept):
        if clock() > deadline:
            model["notes"].append(QUERIES_LATE)
            break
        answer = call_broker(lambda: scheduler, lambda made, job_id=job_id: made.call("queue.job", {"job_id": job_id}, timeout=QUERY_SECONDS))
        if answer[0] == "error" and (isinstance(answer[1], SchedulerUnavailable) or getattr(answer[1], "code", None) == "unreachable"):
            model["line"] = QUERIES_NO_QUEUE if getattr(answer[1], "reason", None) == "no_settings" else f"The scheduler did not answer: {words.door_sentence(answer[1])}"
            model["rows"] = []
            return model
        row = query_row(job_id, answer)
        if row is not None:
            model["rows"].append(row)
    if not model["rows"] and not model["notes"]:
        model["line"] = QUERIES_NONE
    return model


class ChatFileStream(Response):
    """The answer that streams a chat's file from the runner's pieces: an attachment of type `application/octet-stream` with a reduced name, `Content-Length` the size the first piece gave. `fetch(offset)` (run in a worker) returns the next piece as `(data, size, end)` or raises; a piece whose size is not the one announced, a piece shorter than a whole piece before the end, the time limit passed on `clock` (`deadline`), or any error ends the answer at once, never past what was announced. `release()` is called when the answer ends, however it ends."""

    media_type = "application/octet-stream"

    def __init__(self, first, size, name, fetch, release, clock, deadline):
        super().__init__(content=None, status_code=200, headers={"content-disposition": f'attachment; filename="{results.download_name(name)}"', "content-length": str(size), "content-security-policy": SANDBOX_POLICY}, media_type=self.media_type)
        self.first, self.size, self.fetch, self.release, self.clock, self.deadline = first[:size], size, fetch, release, clock, deadline

    async def __call__(self, scope, receive, send):
        try:
            async with anyio.create_task_group() as group:

                async def watch():
                    while True:
                        if (await receive())["type"] == "http.disconnect":
                            group.cancel_scope.cancel()
                            return

                group.start_soon(watch)
                await send({"type": "http.response.start", "status": self.status_code, "headers": self.raw_headers})
                sent, data = 0, self.first
                while True:
                    data = data[:self.size - sent]
                    sent += len(data)
                    await send({"type": "http.response.body", "body": data, "more_body": sent < self.size})
                    if sent >= self.size:
                        break
                    if self.clock() > self.deadline:
                        raise OSError("the download took longer than its limit")
                    data, size, end = await anyio.to_thread.run_sync(self.fetch, sent)
                    if size != self.size or not data or (len(data) < assistant.PIECE_BYTES and sent + len(data) < self.size):
                        raise OSError("the file changed while it was sent, or a piece was short")
                group.cancel_scope.cancel()
        finally:
            self.release()


def install(api, ctx):
    """Add the Chat window's routes to `api`."""
    runner = ctx.assistant
    scheduler = ctx.scheduler
    gate = ctx.chat_gate
    limits = ctx.limits
    downloads = threading.BoundedSemaphore(limits.chat_download_workers)
    queries = Gate(limits.chat_query_workers)

    def frame(template, session, status=200, **context):
        return ctx.frame(template, "chat:list", session, status, **context)

    def refused(session, line, status, back):
        return frame("refused.html", session, status, title="Not done", line=line, field=None, back=back)

    async def checked_run(request, work, cached=True, page=None):
        """(session, the result of `work(session)` in a worker of the window's bound after the broker's yes, made into the answer by `page(session, result)` in that same worker when given, so that a page's model is built and rendered within the bound) or (None, the answer to give instead: the login, a busy or a refused page)."""
        session_id, session = ctx.current(request)
        if session is None:
            return None, ctx.to_login(request, clear_cookie=bool(session_id))

        def run():
            made = queue.broker_checked(ctx, session, work, cached)
            return made if page is None or queue.is_refused(made) else page(session, made)

        made = await gate.run(run)
        if made is BUSY_GATE:
            return None, frame("busy.html", session, 503, title="Chat", line=BUSY)
        if queue.is_refused(made):
            ending = queue.ended_session(ctx, request, session_id, made.error)
            return None, ending or frame("busy.html", session, 503, title="Chat", line=words.sentence(made.error))
        return session, made

    def shown(template, title, status=None):
        """The `page` of `checked_run` that renders `template` from a model (its `_status`, unless `status` is given)."""
        def page(session, model):
            code = model.pop("_status", 200)
            return frame(template, session, code if status is None else status, title=title, **model)

        return page

    # ── the chats ──

    def page_model(chat_id=None, **values):
        """Every field `chat.html` reads, for no chat open (`chat_id` None) or for one."""
        model = {"chat_id": chat_id, "failure": None, "hello": None, "hello_failure": None, "chats": None, "start": 0, "draft": "", "line": None, "text_max": assistant.TEXT_MAX, "events": [], "events_failure": None, "trigger": None, "next_href": None, "continue_href": None, "running": False, "files": None, "files_failure": None, "files_count": None, "left_out": 0, "more": False, "chat_title": None, "queries_detached": QUERIES_DETACHED}
        model.update(values)
        return model

    def ask_hello(model):
        """Ask `hello` and put its answer, or the sentence of its failure, in `model` (the side columns only); returns the error or None."""
        kind, hello = _ask(runner.hello)
        if kind == "ok":
            model["hello"] = hello_model(hello)
            return None
        model["hello_failure"] = words.chat_sentence(hello)
        return hello

    def list_work(session):
        model = page_model()
        error = ask_hello(model)
        if error is not None and getattr(error, "code", None) == "unreachable":
            # The runner cannot be reached: the list is not asked for (the same wait again), the page says why in its middle.
            model["failure"] = model["hello_failure"]
            return model
        kind, chats = _ask(runner.chats)
        if kind == "error":
            model["failure"] = words.chat_sentence(chats)
            return model
        model["chats"] = chat_rows(chats)
        return model

    @api.get("/chat")
    async def chats_page(request: Request):
        return (await checked_run(request, list_work, page=shown("chat.html", "Chat")))[1]

    @api.post("/chat/new")
    async def new_chat(request: Request):
        if request.scope.get("sdp.form"):
            return _form_refused(request)
        session, made = await checked_run(request, lambda _session: _ask(runner.new), cached=False)
        if session is None:
            return made
        kind, value = made
        if kind == "error":
            return refused(session, words.chat_sentence(value), _status_of(value), ("/chat", "Back to the chats"))
        return RedirectResponse(f"/chat/{value}", status_code=303)

    def _form_refused(request):
        _session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request)
        return refused(session, FORM_REFUSED, 400, ("/chat", "Back to the chats"))

    # ── one chat ──

    def find(chat_id):
        """("ok", the chat's entry of `chat.list`, the list) or ("error", the error; `not_found` when it is not listed, the list or None when it could not be read)."""
        kind, chats = _ask(runner.chats)
        if kind == "error":
            return kind, chats, None
        found = next((chat for chat in chats if chat["id"] == chat_id), None)
        return ("ok", found, chats) if found is not None else ("error", client.BrokerError("not_found", "not listed"), chats)

    def conversation_work(chat_id, draft="", line=None, start=0):
        def work(_session):
            model = page_model(chat_id, start=start, draft=draft, line=line)
            kind, chat, chats = find(chat_id)
            if chats is not None:
                model["chats"] = chat_rows(chats, chat_id)
            if kind == "error":
                model.update(failure=words.chat_sentence(chat), _status=_status_of(chat))
                if getattr(chat, "code", None) == "unreachable":
                    # The runner cannot be reached: `hello` is not asked (the same wait again).
                    model["hello_failure"] = model["failure"]
                else:
                    ask_hello(model)
                return model
            model["chat_title"] = statements.visible(chat["title"])[0] if chat["title"] else None
            # The files first: the plot cards' buttons are made from their names.
            stems = None
            kind, value = _ask(lambda: runner.files(chat_id))
            if kind == "error":
                model["files_failure"] = words.chat_sentence(value)
            else:
                files, model["left_out"], model["more"] = value
                stems = stems_of(entry["name"] for entry in files)
                model["files"] = [{"name": statements.visible(entry["name"])[0], "size": queue.size_text(entry["size"]), "href": file_href(chat_id, entry["name"]), "table": file_href(chat_id, entry["name"], "table") if entry["name"].lower().endswith(TABLE_ENDING) else None, "glyph": glyph_of(entry["name"])} for entry in files]
                model["files_count"] = files_count(len(files), model["more"])
            kind, value = _ask(lambda: runner.events(chat_id, start, EVENTS_PAGE))
            if kind == "error":
                model["events_failure"] = words.chat_sentence(value)
                model["running"] = chat["running"]
            else:
                events, offset, running = value
                model.update(events=events_model(events, chat_id, limits.chat_units_max, start, stems)[0], running=running, **next_of(chat_id, start, offset, events, running))
            ask_hello(model)
            return model

        return work

    async def conversation(request, chat_id, draft="", line=None, status=None, cached=True, start=0):
        return (await checked_run(request, conversation_work(chat_id, draft, line, start), cached, page=shown("chat.html", "Chat", status)))[1]

    @api.get("/chat/{chat_id}")
    async def chat_page(request: Request, chat_id: str):
        start_text = request.query_params.get("from")
        start = 0 if start_text is None else whole_in_bounds(start_text)
        if not assistant.is_chat_id(chat_id) or start is None:
            return _not_found()
        return await conversation(request, chat_id, start=start)

    @api.get("/chat/{chat_id}/events")
    async def events_part(request: Request, chat_id: str):
        offset = whole_in_bounds(request.query_params.get("offset"))
        start = whole_in_bounds(request.query_params.get("from", "0"))
        if not assistant.is_chat_id(chat_id) or offset is None or start is None or start > offset:
            return _not_found()
        session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))
        part = {"events": [], "line": None, "trigger": None, "next_href": None, "continue_href": None, "running": None, "chat_id": chat_id, "form_token": session.form_token}
        left = VIEW_EVENTS_MAX - (offset - start)

        def work(_session):
            """The part, modelled and rendered in the worker that asked: the events of a page and their runs are bounded there."""
            if left <= 0:
                part.update(next_of(chat_id, start, offset, [], False))
                return ctx.render("_chat_events.html", **part)
            kind, value = _ask(lambda: runner.events(chat_id, offset, min(EVENTS_PAGE, left)))
            if kind == "error":
                part["line"] = words.chat_sentence(value)
            else:
                events, next_offset, running = value
                stems = None
                if any(isinstance(event, dict) and event.get("type") == "file" for event in events):
                    # A file came: the chat's list is asked once, for its card's buttons.
                    listed = _ask(lambda: runner.files(chat_id))
                    stems = stems_of(entry["name"] for entry in listed[1][0]) if listed[0] == "ok" else None
                part.update(events=events_model(events, chat_id, limits.chat_units_max, offset, stems)[0], running=running, **next_of(chat_id, start, next_offset, events, running))
            return ctx.render("_chat_events.html", **part)

        made = await gate.run(lambda: queue.broker_checked(ctx, session, work))
        if made is BUSY_GATE or queue.is_refused(made):
            ending = queue.ended_session(ctx, request, session_id, made.error) if made is not BUSY_GATE else None
            if ending is not None:
                return ending
            part.update(line=BUSY if made is BUSY_GATE else words.sentence(made.error), trigger=RETRY, next_href=events_href(chat_id, offset, start))
            return ctx.render("_chat_events.html", **part)
        return made

    @api.post("/chat/{chat_id}/send")
    async def send(request: Request, chat_id: str):
        form = request.scope.get("sdp.form", {})
        if not assistant.is_chat_id(chat_id):
            return _not_found()
        if set(form) != {"text"}:
            return _form_refused(request)
        text = form["text"].replace("\r\n", "\n").replace("\r", "\n")
        if not 1 <= len(text) <= assistant.TEXT_MAX:
            return await conversation(request, chat_id, text, TEXT_REFUSED, 400, cached=False)
        session, made = await checked_run(request, lambda _session: _ask(lambda: runner.send(chat_id, text)), cached=False)
        if session is None:
            return made
        kind, value = made
        if kind == "error":
            return await conversation(request, chat_id, text, words.chat_sentence(value), _status_of(value))
        return RedirectResponse(f"/chat/{chat_id}", status_code=303)

    async def change(request, chat_id, function, done):
        if not assistant.is_chat_id(chat_id):
            return _not_found()
        if request.scope.get("sdp.form"):
            return _form_refused(request)
        session, made = await checked_run(request, lambda _session: _ask(function), cached=False)
        if session is None:
            return made
        kind, value = made
        if kind == "error":
            return refused(session, words.chat_sentence(value), _status_of(value), (f"/chat/{chat_id}", "Back to the chat"))
        return RedirectResponse(done, status_code=303)

    @api.post("/chat/{chat_id}/stop")
    async def stop(request: Request, chat_id: str):
        return await change(request, chat_id, lambda: runner.stop(chat_id), f"/chat/{chat_id}")

    @api.post("/chat/{chat_id}/delete")
    async def delete(request: Request, chat_id: str):
        return await change(request, chat_id, lambda: runner.delete(chat_id), "/chat")

    @api.get("/chat/{chat_id}/delete")
    async def delete_page(request: Request, chat_id: str):
        if not assistant.is_chat_id(chat_id):
            return _not_found()

        def work(_session):
            kind, chat, _chats = find(chat_id)
            if kind == "error":
                return {"chat_id": chat_id, "failure": words.chat_sentence(chat), "chat_title": None, "running": False, "_status": _status_of(chat)}
            return {"chat_id": chat_id, "failure": None, "chat_title": statements.visible(chat["title"])[0] if chat["title"] else None, "running": chat["running"]}

        return (await checked_run(request, work, page=shown("chat_delete.html", "Delete this chat")))[1]

    @api.get("/chat/{chat_id}/queries")
    async def queries_part(request: Request, chat_id: str):
        """The right column's queries (`queries_model`), on a bound of its own: a full one answers 204 and the part stays as it is."""
        if not assistant.is_chat_id(chat_id):
            return _not_found()
        session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))

        def work():
            error = queue.broker_session_error(ctx, session, timeout=QUERY_SECONDS)
            if error is not None:
                return error
            return ctx.render("_chat_queries.html", chat_id=chat_id, queries=queries_model(runner, scheduler, chat_id, ctx.clock), poll=QUERIES_POLL)

        made = await queries.run(work)
        if made is BUSY_GATE:
            return Response(status_code=204)
        if isinstance(made, client.BrokerError):
            ending = queue.ended_session(ctx, request, session_id, made)
            return ending or ctx.render("_chat_queries.html", chat_id=chat_id, queries={"line": words.sentence(made), "rows": [], "notes": []}, poll=QUERIES_POLL)
        return made

    # ── files ──

    def _name_ok(chat_id, name):
        return assistant.is_chat_id(chat_id) and assistant.is_file_name(name)

    @api.get("/chat/{chat_id}/table/{name}")
    async def table_page(request: Request, chat_id: str, name: str):
        if not _name_ok(chat_id, name) or not name.lower().endswith(TABLE_ENDING):
            return _not_found()

        def work(_session):
            model = {"chat_id": chat_id, "file_name": statements.visible(name)[0], "href": file_href(chat_id, name), "size": None, "failure": None, "header": [], "body": [], "notes": [], "line": None}
            kind, value = _ask(lambda: runner.piece(chat_id, name, 0, TABLE_BYTES))
            if kind == "error":
                code = getattr(value, "code", None)
                model.update(failure=words.chat_sentence(value), _status=404 if code in ("no_such_file", "not_found") else _status_of(value))
                return model
            data, size, end = value
            model["size"] = queue.size_text(size)
            model.update(table_model(data, size, end, limits.chat_units_max))
            return model

        # Like a download, the broker is asked every time: a file's content is never shown on an earlier answer.
        return (await checked_run(request, work, cached=False, page=shown("chat_table.html", "Table view")))[1]

    @api.get("/chat/{chat_id}/file/{name}")
    async def download(request: Request, chat_id: str, name: str):
        if not _name_ok(chat_id, name):
            return _not_found()
        session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))
        if not downloads.acquire(blocking=False):
            return PlainTextResponse(DOWNLOADS_BUSY + "\n", status_code=503)
        handed_over = False
        try:
            # A download always asks the broker first: no belief of an earlier answer. Its calls are bounded by the downloads' own places.
            made = await anyio.to_thread.run_sync(lambda: queue.broker_checked(ctx, session, lambda _session: _ask(lambda: runner.piece(chat_id, name, 0)), cached=False))
            if queue.is_refused(made):
                ending = queue.ended_session(ctx, request, session_id, made.error)
                return ending or PlainTextResponse(words.sentence(made.error) + "\n", status_code=503)
            kind, value = made
            if kind == "error":
                return _not_found() if getattr(value, "code", None) in ("no_such_file", "not_found") else PlainTextResponse(words.chat_sentence(value) + "\n", status_code=_status_of(value))
            data, size, _end = value
            if size > limits.chat_download_bytes:
                return PlainTextResponse(TOO_LARGE + "\n", status_code=413)
            if len(data) < assistant.PIECE_BYTES and len(data) < size:
                return PlainTextResponse(words.CHAT_CODE_SENTENCES["not_understood"] + "\n", status_code=503)

            def fetch(offset):
                return runner.piece(chat_id, name, offset)

            stream = ChatFileStream(data, size, name, fetch, downloads.release, ctx.clock, ctx.clock() + limits.chat_download_seconds)
            handed_over = True
            return stream
        finally:
            if not handed_over:
                downloads.release()

    @api.get("/chat/{chat_id}/image/{name}")
    async def image(request: Request, chat_id: str, name: str):
        if not _name_ok(chat_id, name):
            return _not_found()
        session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))

        def whole(_session):
            """(the image's type, its bytes), or None: a file that is not a PNG or JPEG by its first bytes, larger than the limit, that changed while it was read, that came in a piece shorter than a whole piece before its end, or that took longer than its time limit."""
            deadline = ctx.clock() + limits.chat_image_seconds
            kind, value = _ask(lambda: runner.piece(chat_id, name, 0))
            if kind == "error":
                return None
            data, size, end = value
            media = next((media for signature, media in SIGNATURES if data.startswith(signature)), None)
            if media is None or size > limits.chat_image_bytes:
                return None
            parts, held = [data], len(data)
            while held < size and not end:
                if len(parts[-1]) < assistant.PIECE_BYTES or ctx.clock() > deadline:
                    return None
                kind, value = _ask(lambda: runner.piece(chat_id, name, held))
                if kind == "error" or value[1] != size or not value[0]:
                    return None
                parts.append(value[0])
                held += len(value[0])
                end = value[2]
            return (media, b"".join(parts)) if held == size else None

        made = await ctx.chat_images.run(lambda: queue.broker_checked(ctx, session, whole, cached=False), wait_seconds=limits.chat_image_wait_seconds)
        if made is BUSY_GATE:
            return PlainTextResponse(BUSY + "\n", status_code=503)
        if queue.is_refused(made):
            ending = queue.ended_session(ctx, request, session_id, made.error)
            return ending or PlainTextResponse(words.sentence(made.error) + "\n", status_code=503)
        if made is None:
            return _not_found()
        return Response(content=made[1], media_type=made[0], headers={"content-security-policy": SANDBOX_POLICY})
