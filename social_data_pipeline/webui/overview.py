"""The data of the Overview page, made from the answers of three read verbs: `db.status`, `source.list` and `run.list`; and its live figures (`read_live`, for `GET /overview/live`): the number of queries that wait for approval, the finished files of each source's stages, its loads and its size on disk.

Each block is made on its own: a failed read, or an answer of a shape this module does not expect, becomes that block's sentence and leaves the other blocks as they are. Every text that came from the broker is passed through `cleaning.clean_log_text` (with line breaks made spaces) before it is shown, and is escaped by the template; a name becomes a link only when it is a plain name (`PLAIN_NAME`, the registry's pattern), and then its URL-encoded form is used.

The page draws "Needs attention" (the notices, a failed read, Docker that cannot be asked), "Right now" (the runs alive, the state of the databases, the MCP servers and the scheduler) and the grid of sources and pipeline profiles (`grid`): one row per source, one cell per profile of `GRID_PROFILES`, each "running" when a run of that profile on that source is alive, "configured" when the source has the profile, "–" otherwise. What a configured cell has produced comes with the live figures (`live_cell`): no state the reads do not give is drawn.

The live figures are asked after the page has loaded, so that a slow scheduler or a source with many files never slows the page: `read_live` runs in a worker of a bound of its own, asks the broker first whether it still knows the session (as `status` does), then the scheduler's `queue.counts`, `source.list`, `run.list` and, for at most `LIVE_SOURCES_MAX` sources and `LIVE_TOTAL_SECONDS` in all, `source.progress`, `source.status` and `source.size` per source, each call with the time limit `LIVE_SECONDS`, each failure confined to its own line or row. `source.progress` counts the finished files of the parse and lingua stages and of each classifier folder (the `ml` profile); it gives no total, so a cell shows the finished count alone, never a fraction or a bar. `source.status` gives the files each database profile loaded and failed, as the source's page shows them in its lanes (`pages.load_figure`). `source.size` gives the bytes on disk of the source's dumps, extracted, parsed and output folders, shown beside its name in the grid (`size_figure`, `bytes_text`: decimal units as `sdp source size` writes them, "≥ " before a total of which a folder could not be read whole). A stage's unfinished files (`.temp`) are "being written" only while a run that may write them is alive (`writing_of`); otherwise they were left by a run that ended.

Two notices are made from `db.status`. Databases without authentication: authentication is set once for every database of a setup, and the status names the databases with an admin user; a database counts as without authentication when authentication is off, or when the status names no admin user for it. Ports open beyond this machine: `docker-compose.yml` publishes the ports of the databases, the MCP servers and the scheduler without an address, which Docker opens on every address of the machine, so the notice lists the services that run now with their ports. Standard library and this package only.
"""

import re
import time
import urllib.parse

from social_data_pipeline.webui import words
from social_data_pipeline.webui.calls import SESSION_ENDING_CODES, call_broker
from social_data_pipeline.webui.cleaning import clean_log_text

READ_VERBS = ("db.status", "source.list", "run.list", "db.restart_needed")
# A literal copy of `actions.SERVICES` (this module is imported by `actions`; a test compares the two): the services a restart names, in the order they are restarted.
RESTART_SERVICES = ("postgres", "mongo", "starrocks", "postgres-mcp", "mongo-mcp", "starrocks-mcp", "jobs")
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
# The profile whose run writes each kind of stage of a `source.progress` answer.
STAGE_PROFILES = {"parse": "parse", "lingua": "lingua", "classifier": "ml"}
# The grid's profiles whose figures are files in a folder (`source.progress`); the others are loads into a database (`source.status`).
FILE_PROFILES = ("parse", "lingua", "ml")
# A source's size on disk (`source.size`): the units above bytes, each 1000 times the one before (a literal copy of the CLI's, `sdp source size`), and the size from which the text no longer counts (a million million PB: no disk holds it, and the digits of a larger whole number could raise when written).
SIZE_UNITS = ("kB", "MB", "GB", "TB", "PB")
SIZE_SHOWN_MAX = 10 ** 27
SIZE_TITLE = "On disk: the source's dumps, extracted, parsed and output folders"
SIZE_AT_LEAST = "at least: a folder of the source could not be read whole"


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


def restart_block(result):
    """`services` (`service`, `files`: the settings files newer than its start) and `restart` (the services a restart stops and starts again, in that order: a database's MCP server is left out when its database is named, since `db stop` and `db start` of a database take its MCP server along) from a `db.restart_needed` answer. Raises ShapeError."""
    data = _data(result)
    _need(isinstance(data.get("docker_available"), bool) and isinstance(data.get("services"), list))
    rows = []
    for entry in data["services"]:
        _need(isinstance(entry, dict) and entry.get("service") in RESTART_SERVICES and isinstance(entry.get("files"), list))
        files = []
        for item in entry["files"]:
            _need(isinstance(item, dict))
            files.append(text(_string(item.get("file"))))
        rows.append({"service": entry["service"], "files": files})
    named = {row["service"] for row in rows}
    restart = [service for service in RESTART_SERVICES if service in named and not (service.endswith("-mcp") and service[:-len("-mcp")] in named)]
    return {"services": rows, "restart": restart}


_BLOCKS = {"db.status": databases_block, "source.list": sources_block, "run.list": runs_block, "db.restart_needed": restart_block}


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
    """The grid's rows from the blocks of `source.list` and `run.list`: per source `name`, `href`, `key` (None for a name that is not plain: its cells are not links and get no live figures), `set_up` (how many of the grid's profiles it has) and `cells` (`profile`, `state`: `run`, `setup` or `none`, `parts` and `title`: what the cell says until the live figures come, `href`, `id` of the cell the live figures replace)."""
    running = {(row["source"], row["profile"]) for row in runs["rows"]} if runs["ok"] else set()
    rows = []
    for row in sources["rows"] if sources["ok"] else []:
        key = cell_key(row["name"]) if row["href"] else None
        cells = []
        for profile in GRID_PROFILES:
            set_up = profile in row["profiles"]
            state = "run" if (row["name"], profile) in running else "setup" if set_up else "none"
            parts = {"run": [("▶", "running")], "setup": [("○", "configured")], "none": [("–", "")]}[state]
            title = f"{profile} on {row['name']}: " + {"run": "running", "setup": "configured", "none": "not configured"}[state]
            href = (row["href"] + ("#run-" + profile if set_up else "")) if row["href"] else None
            cells.append({"profile": profile, "state": state, "parts": parts, "note": None, "title": title, "href": href, "id": f"c-{key}-{profile}" if key and state != "none" else None})
        rows.append({"name": row["name"], "href": row["href"], "key": key, "set_up": sum(profile in row["profiles"] for profile in GRID_PROFILES), "cells": cells})
    return rows


def unfinished_words(count, writing):
    """What the page says of a stage's `count` unfinished (`.temp`) files: "being written" only when `writing` is True (a run that may write them is alive); left by a run that ended when it is False; and that it cannot be told when None."""
    files = "file" if count == 1 else "files"
    if writing is True:
        return f"{count} being written"
    if writing is False:
        return f"{count} unfinished {files}, left by a run that ended"
    return f"{count} unfinished {files}: whether a run still writes them cannot be told"


def writing_of(runs, source):
    """A function of a profile (`parse`, `lingua`, `ml`) that says whether a run may be writing that stage's files of the source `source` now: True while a pipeline run of that profile on the source, or a task of the source, is alive; None when that cannot be told (the runs were not read; Docker cannot be asked, so runs started in a terminal are not listed; a terminal run of that profile whose source Docker does not hand over); False otherwise. `runs` is a runs block (`ok`, `docker_available`, `rows`) whose rows are the runs alive, with `kind`, `profile` and `source` as text (`-` for none)."""

    def writing(profile):
        if not runs.get("ok"):
            return None
        unknown = not runs.get("docker_available")
        for row in runs["rows"]:
            if row["kind"] == "terminal":
                if row["profile"] == profile and row["source"] == source:
                    return True
                if row["profile"] == profile and row["source"] == "-":
                    unknown = True
            elif row["source"] == source and (row["kind"] == "task" or row["profile"] == profile):
                return True
        return None if unknown else False

    return writing


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


def progress_cells(result, writing):
    """The figures of one source's file stages from its `source.progress` answer: {profile: {"text", "finished", "readable", "title", "unfinished"}} for `parse`, `lingua` and `ml` (each classifier folder's count, in name order), the finished files summed over the data types: `text` the count as shown ("?" for a folder that could not be read; for `ml` one count per classifier folder), `finished` the sum, `unfinished` None or {"text", "writing"} for the stage's `.temp` files, worded by `writing(profile)` (`writing_of`). Nothing for a profile the answer counts no folder of. Raises ShapeError."""
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
        if kind in ("parse", "lingua"):
            cells[kind] = _figure(shown, finished, written, readable, writing(kind))
        elif kind == "classifier":
            classifiers.append((name, shown, finished, written, readable))
    if classifiers:
        written = sum(item[3] for item in classifiers)
        writes = writing("ml")
        cells["ml"] = {
            "text": " · ".join(shown for _, shown, _, _, _ in classifiers),
            "finished": sum(item[2] for item in classifiers),
            "readable": all(item[4] for item in classifiers),
            "title": "; ".join(f"{name}: " + _figure(shown, finished, each, readable, writes)["title"] for name, shown, finished, each, readable in classifiers),
            "unfinished": {"text": unfinished_words(written, writes), "writing": writes} if written else None,
        }
    return cells


def _figure(shown, finished, written, readable, writes):
    unfinished = {"text": unfinished_words(written, writes), "writing": writes} if written else None
    title = f"{finished} finished files" + (", " + unfinished["text"] if unfinished else "") + ("" if readable else ", and a folder that could not be read")
    return {"text": shown, "finished": finished, "readable": readable, "title": title, "unfinished": unfinished}


def live_cell(profile, running, figure, load):
    """What a configured cell of the grid says once the live figures are read: `state` (`run`, `done`, `fail`, `setup`), `parts` (pairs of an icon and a text), `note` (None or {"text", "warn"}: the stage's unfinished files) and `text` (all of it as one line). `running`: a run of the profile on the source is alive. For a file profile `figure` is its `progress_cells` entry (None: the answer counts no folder of it); for a database profile `load` is its `pages.load_figure` (None: no state file)."""
    parts, state, note = [], "setup", None
    if profile in FILE_PROFILES:
        if figure is not None:
            if not figure["readable"]:
                parts, state = [("?", f"{figure['text']} files, a folder could not be read")], "fail"
            elif figure["finished"]:
                parts, state = [("✓", f"{figure['text']} files")], "done"
            if figure["unfinished"]:
                note = {"text": figure["unfinished"]["text"], "warn": figure["unfinished"]["writing"] is not True}
    elif load is not None:
        if load["loaded"] is None:
            parts, state = [("?", "state files of another shape")], "fail"
        else:
            if load["loaded"]:
                parts, state = [("✓", f"{load['loaded']} loaded")], "done"
            if load["failed"]:
                parts, state = parts + [("✕", f"{load['failed']} failed")], "fail"
    if running:
        parts, state = [("▶", "running")] + parts, "run"
    elif not parts:
        parts = [("○", "configured · nothing yet")]
    shown = " · ".join(f"{icon} {words_}" for icon, words_ in parts) + (f" · {note['text']}" if note else "")
    return {"state": state, "parts": parts, "note": note, "text": shown}


def bytes_text(size):
    """A number of bytes as `sdp source size` writes it: `999 B`, then one decimal place in powers of 1000 (`1.0 kB`, `12.3 MB`, up to PB), rounded half up, in whole numbers only. Total over every int: from `SIZE_SHOWN_MAX` on, the text is "more than" that many PB."""
    if size < 1000:
        return f"{size} B"
    if size >= SIZE_SHOWN_MAX:
        return f"more than {SIZE_SHOWN_MAX // 1000 ** len(SIZE_UNITS)} PB"
    scale = 1
    for unit in SIZE_UNITS:
        scale *= 1000
        tenths = (size * 10 + scale // 2) // scale
        if tenths < 10000 or unit == SIZE_UNITS[-1]:
            return f"{tenths // 10}.{tenths % 10} {unit}"


def size_view(total, complete):
    """What the page shows of a source's size: {"text", "title"}; a total that is not complete (a folder could not be read whole) is "≥ " the bytes counted."""
    if complete:
        return {"text": bytes_text(total), "title": SIZE_TITLE + (f": {total} bytes" if total < SIZE_SHOWN_MAX else "")}
    return {"text": "≥ " + bytes_text(total), "title": SIZE_AT_LEAST}


def size_figure(result):
    """The size of one source from its `source.size` answer (`size_view`), or None when the source has no readable configuration (`found` false, or `not_readable` set). Reads only `found`, `total_bytes` (a whole number from 0 on, not a boolean) and `complete` (a boolean); raises ShapeError for an answer of another shape."""
    data = _data(result)
    sources = data.get("sources")
    _need(isinstance(sources, list) and len(sources) == 1 and isinstance(sources[0], dict))
    entry = sources[0]
    _need(isinstance(entry.get("found"), bool))
    # A config that was found but cannot be used (not a mapping, `paths` not a mapping) measures nothing: no size, rather than "≥ 0 B".
    if not entry["found"] or entry.get("not_readable"):
        return None
    total, complete = entry.get("total_bytes"), entry.get("complete")
    _need(isinstance(total, int) and not isinstance(total, bool) and total >= 0 and isinstance(complete, bool))
    return size_view(total, complete)


class LiveSessionEnded:
    """What `read_live` returns when the broker has ended the session: the broker's error."""

    def __init__(self, error):
        self.error = error


def read_live(session_error, scheduler, ask, clock=time.monotonic):
    """The live figures of the Overview: {"broker": None or the sentence of a broker that did not confirm the session (then nothing else is asked), "queue": None (not set up, or nothing asked) or {"pending"} or {"failed"}, "sources": None or the sentence of a failed `source.list`, "runs": None or the sentence of a failed `run.list` (then no cell says running, and unfinished files are not called "being written"), "rows": per source asked {"key", "cells": {profile: the cell (`live_cell`) with its `href`, `title` and `id`}, "size": None or its size on disk (`size_figure`), "failed": None or the line of its reads that failed}, "cut": None or (asked, listed)}, or `LiveSessionEnded`. `session_error()` is the broker's check of the session, `scheduler` the way to the scheduler, `ask(verb, fields)` a read of the broker with the live time limit. Per source it asks `source.progress`, `source.status` and `source.size`, in that order, each only while the time of all the calls lasts."""
    from social_data_pipeline.webui import pages  # here, not at the top: `pages` imports this module
    from social_data_pipeline.webui.scheduler import SchedulerUnavailable  # here, not at the top: `scheduler` imports `actions`, which imports this module

    model = {"broker": None, "queue": None, "sources": None, "runs": None, "rows": [], "cut": None}
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

    runs = {"ok": False}
    if listed:
        kind, value = ask("run.list", {})
        if kind == "error" and getattr(value, "code", None) in SESSION_ENDING_CODES:
            return LiveSessionEnded(value)
        runs = _runs_of(kind, value)
        if not runs["ok"]:
            model["runs"] = runs["sentence"]
    running = {(row["source"], row["profile"]) for row in runs["rows"]} if runs["ok"] else set()

    for row in listed[:LIVE_SOURCES_MAX]:
        entry = {"key": cell_key(row["name"]), "cells": {}, "failed": None, "size": None}
        model["rows"].append(entry)
        failed, figures, loads = [], None, None
        for verb, label in (("source.progress", "Finished files"), ("source.status", "Loads"), ("source.size", "Size")):
            if clock() >= deadline:
                failed.append(f"{label} not read: {LIVE_TIME_OUT}")
                continue
            kind, value = ask(verb, {"name": row["name"]})
            if kind == "error":
                if getattr(value, "code", None) in SESSION_ENDING_CODES:
                    return LiveSessionEnded(value)
                failed.append(f"{label} not read: {words.sentence(value)}")
                continue
            try:
                if verb == "source.progress":
                    figures = progress_cells(value, writing_of(runs, row["name"]))
                elif verb == "source.size":
                    entry["size"] = size_figure(value)
                    if entry["size"] is None:
                        failed.append(f"{label} not read: the source has no readable configuration.")
                else:
                    status = pages.source_status_block(value)
                    loads = status if status["found"] else None
                    if loads is None:
                        failed.append(f"{label} not read: the source has no readable configuration.")
            except Exception:
                failed.append(f"{label} not read: {SHAPE_SENTENCE}")
        entry["failed"] = " · ".join(failed) or None
        for profile in GRID_PROFILES:
            if profile not in row["profiles"]:
                continue
            is_file = profile in FILE_PROFILES
            if (figures if is_file else loads) is None:
                continue
            cell = live_cell(profile, (row["name"], profile) in running, figures.get(profile) if is_file else None, None if is_file else pages.load_figure(loads, profile))
            cell.update({"profile": profile, "href": row["href"] + "#run-" + profile, "id": f"c-{entry['key']}-{profile}", "title": f"{profile} on {row['name']}: {cell['text']}" + (f" ({figures[profile]['title']})" if is_file and profile in figures else "")})
            entry["cells"][profile] = cell
    return model


def _runs_of(kind, value):
    """The runs block of a `run.list` read (`runs_block`), or {"ok": False, "sentence"} when it failed or had another shape."""
    if kind == "error":
        return {"ok": False, "sentence": words.sentence(value)}
    try:
        return {"ok": True, **runs_block(value)}
    except Exception:
        return {"ok": False, "sentence": SHAPE_SENTENCE}
