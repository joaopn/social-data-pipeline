"""The data of the Overview page, made from the answers of three read verbs: `db.status`, `source.list` and `run.list`; and its live figures (`read_live`, for `GET /overview/live`): the number of queries that wait for approval and the finished files of each source's stages.

Each block is made on its own: a failed read, or an answer of a shape this module does not expect, becomes that block's sentence and leaves the other blocks as they are. Every text that came from the broker is passed through `cleaning.clean_log_text` (with line breaks made spaces) before it is shown, and is escaped by the template; a name becomes a link only when it is a plain name (`PLAIN_NAME`, the registry's pattern), and then its URL-encoded form is used.

The page draws "Needs attention" (the notices, a failed read, Docker that cannot be asked), "Right now" (the runs alive, the state of the databases, the MCP servers and the scheduler) and the grid of sources and pipeline profiles (`grid`): one row per source, one cell per profile of `GRID_PROFILES`, each "running" when a run of that profile on that source is alive, "set up" when the source has the profile, "not set up" otherwise. Nothing else is said of a cell: no state the reads do not give is drawn.

The live figures are asked after the page has loaded, so that a slow scheduler or a source with many files never slows the page: `read_live` runs in a worker of a bound of its own, asks the broker first whether it still knows the session (as `status` does), then the scheduler's `queue.counts` and, for at most `LIVE_SOURCES_MAX` sources and `LIVE_TOTAL_SECONDS` in all, `source.progress` per source, each call with the time limit `LIVE_SECONDS`, each failure confined to its own line or row. `source.progress` counts the finished files of the parse and lingua stages and of each classifier folder (the `ml` profile); it gives no total, so a cell shows the finished count alone, never a fraction or a bar.

Two notices are made from `db.status`. Databases without authentication: authentication is set once for every database of a setup, and the status names the databases with an admin user; a database counts as without authentication when authentication is off, or when the status names no admin user for it. Ports open beyond this machine: `docker-compose.yml` publishes the ports of the databases, the MCP servers and the scheduler without an address, which Docker opens on every address of the machine, so the notice lists the services that run now with their ports. Standard library and this package only.
"""

import re
import time
import urllib.parse

from social_data_pipeline.webui import words
from social_data_pipeline.webui.calls import SESSION_ENDING_CODES, call_broker
from social_data_pipeline.webui.cleaning import clean_log_text

READ_VERBS = ("db.status", "source.list", "run.list")
# A literal copy of `control.types.NAME_PATTERN` (the web UI does not import the registry; a test compares the two).
PLAIN_NAME = r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}"
ALIVE_STATES = ("starting", "running", "detached", "unknown")
# The port StarRocks's backend publishes, written in the compose file without a variable.
STARROCKS_BE_HTTP_PORT = "8040"
ERRORS_TEXT_MAX_CHARS = 4000
SHAPE_SENTENCE = "The broker's answer did not have the expected shape."
DATABASE_TITLES = {"postgres": "PostgreSQL", "mongo": "MongoDB", "starrocks": "StarRocks"}
# The grid's columns: every profile of `actions.VALID_PROFILES` (a test compares them), in the mock-up's order: the stages that prepare the files, then each database's.
GRID_PROFILES = ("parse", "lingua", "ml", "sr_ingest", "sr_ml", "mongo_ingest", "postgres_ingest", "postgres_ml")
# The live figures: the time limit of each call, the most sources asked, the most time all the calls of one request may take.
LIVE_SECONDS = 5
LIVE_SOURCES_MAX = 50
LIVE_TOTAL_SECONDS = 20
LIVE_TIME_OUT = "Not asked: the time for the figures ran out."


class ShapeError(Exception):
    """An answer that does not have the shape this module reads."""


def text(value):
    """A value as one line of text fit to show: control and direction characters and escape sequences removed, line breaks made spaces."""
    return clean_log_text(str(value)).replace("\n", " ")


def link(prefix, name):
    """`prefix` and the URL-encoded name when `name` is a plain name, else None."""
    if isinstance(name, str) and re.fullmatch(PLAIN_NAME, name):
        return prefix + urllib.parse.quote(name, safe="")
    return None


def _need(condition):
    if not condition:
        raise ShapeError()


def _string(value):
    _need(isinstance(value, str))
    return value


def _port(value):
    _need(isinstance(value, (str, int)) and not isinstance(value, bool))
    return text(value)


def _data(result):
    _need(isinstance(result, dict) and isinstance(result.get("data"), dict))
    return result["data"]


def failure(error):
    """A block's failure from a broker error object (code, message and details as the client gives them): the sentence and, for `failed`, the end of the command's error output, cleaned."""
    code = getattr(error, "code", None)
    shown = {"sentence": words.sentence(error), "errors": None}
    details = getattr(error, "details", None) or {}
    if code == "failed" and isinstance(details.get("errors"), str) and details["errors"]:
        shown["errors"] = clean_log_text(details["errors"][-ERRORS_TEXT_MAX_CHARS:])
    return shown


def databases_block(result):
    """`configured`, `rows` (`name`, `href`, `running`, `port`), `unauthenticated` (names) and `open_ports` (`service`, `ports`) from a `db.status` answer. Raises ShapeError."""
    data = _data(result)
    configured = data.get("configured")
    _need(isinstance(configured, list) and all(isinstance(name, str) for name in configured))
    block = {"configured": bool(configured), "rows": [], "unauthenticated": [], "open_ports": [], "mcp_running": 0, "jobs_running": None}
    if not configured:
        return block
    databases = data.get("databases")
    _need(isinstance(databases, list))
    auth = data.get("auth")
    _need(isinstance(auth, dict) and isinstance(auth.get("enabled"), bool) and isinstance(auth.get("rw_users"), list))
    with_admin = set()
    for user in auth["rw_users"]:
        _need(isinstance(user, dict))
        with_admin.add(_string(user.get("db")))
    for entry in databases:
        _need(isinstance(entry, dict) and isinstance(entry.get("running"), bool))
        name = _string(entry.get("db"))
        ports = [_port(entry.get("port"))]
        if name == "starrocks":
            ports += [_port(entry.get("fe_http_port")), STARROCKS_BE_HTTP_PORT]
        block["rows"].append({"name": text(name), "title": DATABASE_TITLES.get(name, text(name)), "href": link("/databases/", name), "running": entry["running"], "port": ports[0]})
        if not auth["enabled"] or name not in with_admin:
            block["unauthenticated"].append(text(name))
        if entry["running"]:
            block["open_ports"].append({"service": text(name), "ports": ports})
    for entry in data.get("mcp") or []:
        _need(isinstance(entry, dict) and isinstance(entry.get("running"), bool))
        if entry["running"]:
            block["mcp_running"] += 1
            block["open_ports"].append({"service": text(f"{_string(entry.get('db'))}-mcp"), "ports": [_port(entry.get("port"))]})
    jobs = data.get("jobs")
    if jobs is not None:
        _need(isinstance(jobs, dict) and isinstance(jobs.get("running"), bool))
        block["jobs_running"] = jobs["running"]
        if jobs["running"]:
            block["open_ports"].append({"service": "jobs", "ports": [_port(jobs.get("port"))]})
    return block


def sources_block(result):
    """`rows` (`name`, `href`, `profiles`) from a `source.list` answer. Raises ShapeError."""
    data = _data(result)
    sources = data.get("sources")
    _need(isinstance(sources, list))
    rows = []
    for entry in sources:
        _need(isinstance(entry, dict) and isinstance(entry.get("profiles"), list))
        name = _string(entry.get("name"))
        rows.append({"name": text(name), "href": link("/sources/", name), "profiles": [text(_string(profile)) for profile in entry["profiles"]]})
    return {"rows": rows}


def runs_block(result):
    """`docker_available` and `rows` (`id`, `href`: the run's page when the id has a run id's form, `kind`, `profile`, `label`: what a task shows in place of a profile, None for any other run, `source`, `source_href`, `state`, `started`) of the runs alive now, from a `run.list` answer. Raises ShapeError."""
    from social_data_pipeline.webui.actions import is_run_id  # here, not at the top: `actions` imports this module
    data = _data(result)
    runs = data.get("runs")
    _need(isinstance(runs, list) and isinstance(data.get("docker_available"), bool))
    rows = []
    for run in runs:
        _need(isinstance(run, dict))
        kind, state = run.get("kind"), run.get("state")
        if kind != "terminal" and state not in ALIVE_STATES:
            continue
        source = run.get("source")
        rows.append({
            "id": text(run.get("id") or "-"),
            "href": "/runs/" + run["id"] if is_run_id(run.get("id")) else None,
            "kind": text(kind or "-"),
            "profile": text(run.get("profile") or "-"),
            "label": text(run.get("label") or "-") if kind == "task" else None,
            "source": text(source) if source else "-",
            "source_href": link("/sources/", source),
            "state": text(state or "-"),
            "started": text(run.get("started") or "-"),
        })
    return {"docker_available": data["docker_available"], "rows": rows}


_BLOCKS = {"db.status": databases_block, "source.list": sources_block, "run.list": runs_block}


def build(answers):
    """The page's blocks from `answers`, a dict of verb to `("ok", result)` or `("error", error)`. Each block is `{"ok": True, ...data}` or `{"ok": False, "sentence", "errors"}`."""
    blocks = {}
    for verb, make in _BLOCKS.items():
        outcome, value = answers[verb]
        if outcome == "ok":
            try:
                blocks[verb] = {"ok": True, **make(value)}
                continue
            # Any exception, not only ShapeError: text from an answer can still raise on the way (the text of a very long number, for one), and one block must never fail the page.
            except Exception:
                blocks[verb] = {"ok": False, "sentence": SHAPE_SENTENCE, "errors": None}
                continue
        blocks[verb] = {"ok": False, **failure(value)}
    return blocks


def cell_key(name):
    """The part of an element id that names a source in the grid: its name in hexadecimal, so that any plain name (dots included, which a selector would read as a class) makes a valid id, and two names never one."""
    return name.encode().hex()


def grid(sources, runs):
    """The grid's rows from the blocks of `source.list` and `run.list`: per source `name`, `href`, `key` (None for a name that is not plain: its cells are not links and get no live figures), `set_up` (how many of the grid's profiles it has) and `cells` (`profile`, `state`: `run`, `setup` or `none`, `href`, `id` of the count the live figures fill)."""
    running = {(row["source"], row["profile"]) for row in runs["rows"]} if runs["ok"] else set()
    rows = []
    for row in sources["rows"] if sources["ok"] else []:
        key = cell_key(row["name"]) if row["href"] else None
        cells = []
        for profile in GRID_PROFILES:
            set_up = profile in row["profiles"]
            state = "run" if (row["name"], profile) in running else "setup" if set_up else "none"
            href = (row["href"] + ("#run-" + profile if set_up else "")) if row["href"] else None
            cells.append({"profile": profile, "state": state, "href": href, "id": f"c-{key}-{profile}" if key and state != "none" else None})
        rows.append({"name": row["name"], "href": row["href"], "key": key, "set_up": sum(profile in row["profiles"] for profile in GRID_PROFILES), "cells": cells})
    return rows


def _count(value):
    _need(isinstance(value, int) and not isinstance(value, bool) and value >= 0)
    return value


def _stage_figure(stage):
    """(finished, being written, readable) of one stage of a `source.progress` answer, summed over its data types."""
    finished = written = 0
    readable = not stage.get("not_readable")
    for item in stage.get("data_types") or []:
        _need(isinstance(item, dict))
        if item.get("not_readable"):
            readable = False
            continue
        if item.get("exists") is True:
            finished += _count(item.get("finished"))
            written += _count(item.get("being_written"))
    return finished, written, readable


def progress_cells(result):
    """The live figures of one source from its `source.progress` answer: {profile: {"text", "title"}} for `parse`, `lingua` and `ml` (each classifier folder's count, in name order), the finished files summed over the data types. Nothing for a profile the answer counts no folder of. Raises ShapeError."""
    data = _data(result)
    sources = data.get("sources")
    _need(isinstance(sources, list) and len(sources) == 1 and isinstance(sources[0], dict))
    entry = sources[0]
    if entry.get("not_readable") or entry.get("found") is not True:
        return {}
    stages = entry.get("stages")
    _need(isinstance(stages, list))
    cells, classifiers = {}, []
    for stage in stages:
        _need(isinstance(stage, dict))
        finished, written, readable = _stage_figure(stage)
        name, kind = text(_string(stage.get("stage"))), stage.get("kind")
        shown = str(finished) if readable else "?"
        title = f"{finished} finished files" + (f", {written} being written" if written else "") + ("" if readable else ", and a folder that could not be read")
        if kind in ("parse", "lingua"):
            cells[kind] = {"text": shown, "title": title}
        elif kind == "classifier":
            classifiers.append((name, shown, title))
    if classifiers:
        cells["ml"] = {"text": " · ".join(shown for _, shown, _ in classifiers), "title": "; ".join(f"{name}: {title}" for name, _, title in classifiers)}
    return cells


class LiveSessionEnded:
    """What `read_live` returns when the broker has ended the session: the broker's error."""

    def __init__(self, error):
        self.error = error


def read_live(session_error, scheduler, ask, clock=time.monotonic):
    """The live figures of the Overview: {"broker": None or the sentence of a broker that did not confirm the session (then nothing else is asked), "queue": None (not set up, or nothing asked) or {"pending"} or {"failed"}, "sources": None or the sentence of a failed `source.list`, "rows": per source asked {"key", "cells", "failed"}, "cut": None or (asked, listed)}, or `LiveSessionEnded`. `session_error()` is the broker's check of the session, `scheduler` the way to the scheduler, `ask(verb, fields)` a read of the broker with the live time limit."""
    from social_data_pipeline.webui.scheduler import SchedulerUnavailable  # here, not at the top: `scheduler` imports `actions`, which imports this module

    model = {"broker": None, "queue": None, "sources": None, "rows": [], "cut": None}
    error = session_error()
    if error is not None:
        if getattr(error, "code", None) in SESSION_ENDING_CODES:
            return LiveSessionEnded(error)
        model["broker"] = f"The broker did not answer: {words.sentence(error)}"
        return model

    kind, value = call_broker(lambda: scheduler, lambda made: made.call("queue.counts", {}, timeout=LIVE_SECONDS))
    if kind == "error" and isinstance(value, SchedulerUnavailable) and value.reason == "no_settings":
        pass
    elif kind == "error":
        model["queue"] = {"failed": f"The scheduler did not answer: {words.door_sentence(value)}"}
    elif not (isinstance(value, dict) and isinstance(value.get("pending"), int) and not isinstance(value["pending"], bool) and value["pending"] >= 0):
        model["queue"] = {"failed": "The scheduler's answer did not have the expected shape."}
    else:
        model["queue"] = {"pending": value["pending"]}

    kind, value = ask("source.list", {})
    if kind == "error":
        if getattr(value, "code", None) in SESSION_ENDING_CODES:
            return LiveSessionEnded(value)
        model["sources"] = words.sentence(value)
        return model
    try:
        listed = [row for row in sources_block(value)["rows"] if row["href"]]
    except Exception:
        model["sources"] = SHAPE_SENTENCE
        return model
    if len(listed) > LIVE_SOURCES_MAX:
        model["cut"] = (LIVE_SOURCES_MAX, len(listed))
    deadline = clock() + LIVE_TOTAL_SECONDS
    for row in listed[:LIVE_SOURCES_MAX]:
        entry = {"key": cell_key(row["name"]), "cells": {}, "failed": None}
        model["rows"].append(entry)
        if clock() >= deadline:
            entry["failed"] = LIVE_TIME_OUT
            continue
        kind, value = ask("source.progress", {"name": row["name"]})
        if kind == "error":
            if getattr(value, "code", None) in SESSION_ENDING_CODES:
                return LiveSessionEnded(value)
            entry["failed"] = words.sentence(value)
            continue
        try:
            cells = progress_cells(value)
        except Exception:
            entry["failed"] = SHAPE_SENTENCE
            continue
        entry["cells"] = {profile: cell for profile, cell in cells.items() if profile in row["profiles"]}
    return model
