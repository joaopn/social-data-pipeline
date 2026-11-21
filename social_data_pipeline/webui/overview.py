"""The data of the Overview page, made from the answers of three read verbs: `db.status`, `source.list` and `run.list`.

Each block is made on its own: a failed read, or an answer of a shape this module does not expect, becomes that block's sentence and leaves the other blocks as they are. Every text that came from the broker is passed through `cleaning.clean_log_text` (with line breaks made spaces) before it is shown, and is escaped by the template; a name becomes a link only when it is a plain name (`PLAIN_NAME`, the registry's pattern), and then its URL-encoded form is used.

Two notices are made from `db.status`. Databases without authentication: authentication is set once for every database of a setup, and the status names the databases with an admin user; a database counts as without authentication when authentication is off, or when the status names no admin user for it. Ports open beyond this machine: `docker-compose.yml` publishes the ports of the databases, the MCP servers and the scheduler without an address, which Docker opens on every address of the machine, so the notice lists the services that run now with their ports. Standard library and this package only.
"""

import re
import urllib.parse

from social_data_pipeline.webui import words
from social_data_pipeline.webui.cleaning import clean_log_text

READ_VERBS = ("db.status", "source.list", "run.list")
# A literal copy of `control.types.NAME_PATTERN` (the web UI does not import the registry; a test compares the two).
PLAIN_NAME = r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}"
ALIVE_STATES = ("starting", "running", "detached", "unknown")
# The port StarRocks's backend publishes, written in the compose file without a variable.
STARROCKS_BE_HTTP_PORT = "8040"
ERRORS_TEXT_MAX_CHARS = 4000
SHAPE_SENTENCE = "The broker's answer did not have the expected shape."


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
    block = {"configured": bool(configured), "rows": [], "unauthenticated": [], "open_ports": []}
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
        block["rows"].append({"name": text(name), "href": link("/databases/", name), "running": entry["running"], "port": ports[0]})
        if not auth["enabled"] or name not in with_admin:
            block["unauthenticated"].append(text(name))
        if entry["running"]:
            block["open_ports"].append({"service": text(name), "ports": ports})
    for entry in data.get("mcp") or []:
        _need(isinstance(entry, dict) and isinstance(entry.get("running"), bool))
        if entry["running"]:
            block["open_ports"].append({"service": text(f"{_string(entry.get('db'))}-mcp"), "ports": [_port(entry.get("port"))]})
    jobs = data.get("jobs")
    if jobs is not None:
        _need(isinstance(jobs, dict) and isinstance(jobs.get("running"), bool))
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
    """`docker_available` and `rows` (`id`, `kind`, `profile`, `source`, `source_href`, `state`, `started`) of the runs alive now, from a `run.list` answer. Raises ShapeError."""
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
            "kind": text(kind or "-"),
            "profile": text(run.get("profile") or "-"),
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
