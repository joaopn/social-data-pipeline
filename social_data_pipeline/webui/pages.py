"""The data of the Management pages besides the Overview, made from the answers of the read verbs: a source's ingestion state, finished files and error logs, the databases' status and checks, the runs, one run's facts, the broker's greeting and the audit log.

As in `overview`, each block is made on its own from one answer: a failed read, or an answer of a shape this module does not read, becomes that block's sentence and leaves the other blocks of the page. Every text that came from the broker is cleaned (`cleaning.clean_log_text`, one line for a value, whole lines for an error text) and escaped by the template; a value that is not text (a number, a list, an object from a state file) is shown as its JSON, cut to `VALUE_MAX_CHARS`; a name becomes a link only when it is a plain name. Standard library and this package only.
"""

import json

from social_data_pipeline.webui import actions
from social_data_pipeline.webui.cleaning import clean_log_text
from social_data_pipeline.webui.overview import ALIVE_STATES, SHAPE_SENTENCE, ShapeError, failure, link, text

VALUE_MAX_CHARS = 300
ERROR_TEXT_MAX_CHARS = 4000
DATABASE_NAMES = {"postgres": "PostgreSQL", "mongo": "MongoDB", "starrocks": "StarRocks"}
INGESTION_NAMES = {"postgres": "PostgreSQL", "mongo": "MongoDB", "sr_ingest": "StarRocks", "sr_ml": "StarRocks classifiers"}


def _need(condition):
    if not condition:
        raise ShapeError()


def _string(value):
    _need(isinstance(value, str))
    return value


def _list(value):
    _need(isinstance(value, list))
    return value


def _dict(value):
    _need(isinstance(value, dict))
    return value


def _data(result):
    _need(isinstance(result, dict) and "data" in result)
    return result["data"]


def value_text(value):
    """Any JSON value as one line of text fit to show: a text cleaned, None as `-`, anything else as its compact JSON; at most VALUE_MAX_CHARS characters."""
    if value is None:
        return "-"
    if isinstance(value, str):
        shown = text(value)
    else:
        try:
            shown = text(json.dumps(value, ensure_ascii=False, separators=(",", ":")))
        except (TypeError, ValueError):
            shown = "-"
    return shown if len(shown) <= VALUE_MAX_CHARS else shown[:VALUE_MAX_CHARS] + "..."


def lines_text(value, max_chars=ERROR_TEXT_MAX_CHARS):
    """A text of several lines fit to show: cleaned, its end kept when it is longer than `max_chars`. A value that is not text is shown as `value_text` shows it."""
    if not isinstance(value, str):
        return value_text(value)
    cleaned = clean_log_text(value)
    return cleaned if len(cleaned) <= max_chars else "..." + cleaned[-max_chars:]


def _one_source(result):
    data = _dict(_data(result))
    sources = _list(data.get("sources"))
    _need(len(sources) == 1)
    entry = _dict(sources[0])
    _need(isinstance(entry.get("found"), bool))
    return entry, [text(_string(warning)) for warning in _list(entry.get("warnings", []))]


def source_status_block(result):
    """`found`, `warnings`, `platform`, `data_types`, `profiles` (as shown), `run_profiles` (the source's profiles that can be run, in the registry's order), `paths` and `ingestion` (per database view: `name`, `has_state_files`, `entries`) from a `source.status` answer for one source. Raises ShapeError."""
    entry, warnings = _one_source(result)
    block = {"found": entry["found"], "warnings": warnings}
    if not entry["found"]:
        return block
    profiles = [_string(profile) for profile in _list(entry.get("profiles"))]
    block["profiles"] = [text(profile) for profile in profiles]
    block["run_profiles"] = [profile for profile in actions.VALID_PROFILES if profile in profiles]
    block["platform"] = value_text(entry.get("platform"))
    block["data_types"] = [value_text(item) for item in _list(entry.get("data_types", []))]
    block["paths"] = [(text(_string(key)), value_text(value)) for key, value in _dict(entry.get("paths", {})).items()]
    block["ingestion"] = []
    for view in _list(entry.get("ingestion", [])):
        _dict(view)
        _need(isinstance(view.get("has_state_files"), bool))
        db_type = _string(view.get("db_type"))
        rows = []
        for state in _list(view.get("entries", [])):
            _dict(state)
            rows.append({key: value_text(state.get(key)) for key in ("label", "count", "latest", "in_progress", "failed_count", "last_updated")})
        block["ingestion"].append({"name": INGESTION_NAMES.get(db_type, text(db_type)), "has_state_files": view["has_state_files"], "entries": rows})
    return block


def source_progress_block(result):
    """`found`, `warnings`, `not_readable`, `stages` (each with `stage`, `path`, `not_readable` and `data_types`: `data_type`, `state` as text, `newest`) and `output` from a `source.progress` answer for one source. Raises ShapeError."""
    entry, warnings = _one_source(result)
    block = {"found": entry["found"], "warnings": warnings, "not_readable": value_text(entry["not_readable"]) if entry.get("not_readable") else None, "stages": [], "output": None}
    for stage in _list(entry.get("stages", [])):
        _dict(stage)
        items = []
        for item in _list(stage.get("data_types", [])):
            _dict(item)
            if item.get("not_readable"):
                state = "not readable (" + value_text(item["not_readable"]) + ")"
            elif item.get("exists") is not True:
                state = "no folder yet"
            else:
                state = value_text(item.get("finished")) + " finished"
                if item.get("being_written"):
                    state += ", " + value_text(item.get("being_written")) + " being written"
            newest = item.get("newest")
            newest_text = None
            if isinstance(newest, dict):
                newest_text = value_text(newest.get("name")) + " (" + value_text(newest.get("modified")) + ")"
            items.append({"data_type": value_text(item.get("data_type")), "state": state, "newest": newest_text})
        block["stages"].append({"stage": value_text(stage.get("stage")), "path": value_text(stage.get("path")), "not_readable": value_text(stage["not_readable"]) if stage.get("not_readable") else None, "data_types": items})
    output = entry.get("output")
    if isinstance(output, dict):
        block["output"] = {"path": value_text(output.get("path")), "not_readable": value_text(output["not_readable"]) if output.get("not_readable") else None}
    return block


def error_logs_block(result, profile):
    """`found`, `warnings`, `files` (`label`, `failed`: `filename`, `timestamp`, `error`) and `sections` (`log_file`, `header`, `output`) of one profile from a `source.error_logs` answer for one source and that profile. Raises ShapeError."""
    entry, warnings = _one_source(result)
    block = {"found": entry["found"], "warnings": warnings, "files": [], "sections": []}
    if not entry["found"]:
        return block
    for checked in _list(entry.get("profiles", [])):
        _dict(checked)
        if checked.get("profile") != profile:
            continue
        for state_file in _list(checked.get("files", [])):
            _dict(state_file)
            failed = [{"filename": value_text(item.get("filename")), "timestamp": value_text(item.get("timestamp")), "error": lines_text(item.get("error"))} for item in (_dict(item) for item in _list(state_file.get("failed", [])))]
            block["files"].append({"label": value_text(state_file.get("label")), "failed": failed})
        for section in _list(checked.get("mongoimport_sections", [])):
            _dict(section)
            header = "\n".join(text(line) if isinstance(line, str) else value_text(line) for line in _list(section.get("header", [])))
            output = "\n".join(text(line) if isinstance(line, str) else value_text(line) for line in _list(section.get("output", [])))
            block["sections"].append({"log_file": value_text(section.get("log_file")), "header": lines_text(header), "output": lines_text(output)})
    return block


def databases_status_block(result):
    """`configured` and `rows` (per database: `name`, `db`, `title`, `running`, `ports`, `path`, `admin_user`), `mcp` (`name`, `port`, `running`, `read_only`) and `jobs` (`port`, `running`, or None) from a `db.status` answer. Raises ShapeError."""
    data = _dict(_data(result))
    configured = [_string(name) for name in _list(data.get("configured"))]
    block = {"configured": [db for db in actions.DATABASES if db in configured], "rows": [], "mcp": [], "jobs": None}
    if not configured:
        return block
    auth = _dict(data.get("auth"))
    _need(isinstance(auth.get("enabled"), bool))
    admins = {}
    for user in _list(auth.get("rw_users", [])):
        _dict(user)
        admins[_string(user.get("db"))] = value_text(user.get("user"))
    for entry in _list(data.get("databases")):
        _dict(entry)
        _need(isinstance(entry.get("running"), bool))
        name = _string(entry.get("db"))
        ports = [value_text(entry.get("port"))]
        if name == "starrocks":
            ports.append(value_text(entry.get("fe_http_port")))
        block["rows"].append({
            "db": name if name in actions.DATABASES else None,
            "name": text(name),
            "title": DATABASE_NAMES.get(name, text(name)),
            "running": entry["running"],
            "ports": ports,
            "path": value_text(entry.get("path")),
            "admin_user": admins.get(name) if auth["enabled"] else None,
        })
    for entry in _list(data.get("mcp") or []):
        _dict(entry)
        _need(isinstance(entry.get("running"), bool))
        block["mcp"].append({"name": text(_string(entry.get("db"))) + "-mcp", "port": value_text(entry.get("port")), "running": entry["running"], "read_only": entry.get("read_only") is True})
    jobs = data.get("jobs")
    if jobs is not None:
        _dict(jobs)
        _need(isinstance(jobs.get("running"), bool))
        block["jobs"] = {"port": value_text(jobs.get("port")), "running": jobs["running"]}
    return block


def databases_verify_block(result):
    """`clean` (no drift anywhere), `message`, `warnings` and `results` (by name: `ok`, `findings` with `category`, `message`, `fix`) from a `db.verify` answer. Raises ShapeError."""
    data = _dict(_data(result))
    report = _dict(data.get("report"))
    _need(isinstance(report.get("ok"), bool))
    results = {}
    for name, outcome in _dict(report.get("results", {})).items():
        _string(name)
        _dict(outcome)
        _need(isinstance(outcome.get("ok"), bool))
        findings = []
        for finding in _list(outcome.get("findings", [])):
            _dict(finding)
            findings.append({key: value_text(finding.get(key)) for key in ("category", "message", "fix")})
        results[name] = {"name": text(name), "ok": outcome["ok"], "findings": findings}
    return {"clean": report["ok"], "message": value_text(report["message"]) if "message" in report else None, "warnings": [value_text(warning) for warning in _list(data.get("warnings", []))], "results": results}


def run_row(run):
    """One run of a `run.list` answer as a row: every value as text, the id a link when it has the form of a run id, the source a link when it is a plain name. Raises ShapeError."""
    _dict(run)
    run_id, source, state, kind = run.get("id"), run.get("source"), run.get("state"), run.get("kind")
    return {
        "id": value_text(run_id),
        "raw_id": run_id if actions.is_run_id(run_id) else None,
        "href": "/runs/" + run_id if actions.is_run_id(run_id) else None,
        "kind": value_text(kind),
        "profile": value_text(run.get("profile")),
        "source": value_text(source),
        "source_href": link("/sources/", source),
        "state": value_text(state),
        "started": value_text(run.get("started")),
        "ended": value_text(run.get("ended")),
        "exit_code": value_text(run.get("exit_code")),
        "started_by": value_text(run.get("started_by")),
        "flags": [(text(key), value_text(value)) for key, value in run["flags"].items() if isinstance(key, str)] if isinstance(run.get("flags"), dict) else [],
        "alive": kind == "terminal" or state in ALIVE_STATES,
    }


def runs_block(result):
    """`docker_available` and `rows` (every run, as `run_row` makes it) from a `run.list` answer. Raises ShapeError."""
    data = _dict(_data(result))
    _need(isinstance(data.get("docker_available"), bool))
    return {"docker_available": data["docker_available"], "rows": [run_row(run) for run in _list(data.get("runs"))]}


def run_facts_block(result, run_id):
    """`docker_available` and `run` (the row of the run of this id, or None when the list does not hold it) from a `run.list` answer. Raises ShapeError."""
    data = _dict(_data(result))
    _need(isinstance(data.get("docker_available"), bool))
    found = next((run for run in _list(data.get("runs")) if isinstance(run, dict) and run.get("id") == run_id), None)
    return {"docker_available": data["docker_available"], "run": run_row(found) if found is not None else None}


def hello_block(result):
    """`protocol` and `password_set` (as text) from the broker's `hello`. Raises ShapeError."""
    _dict(result)
    password_set = result.get("password_set")
    return {"protocol": value_text(result.get("protocol")), "password_set": {True: "yes", False: "no", None: "cannot be told (the password file cannot be used)"}.get(password_set, value_text(password_set))}


def audit_block(result):
    """`rows`, newest first, from an `audit.list` answer: `time`, `session`, `event`, `verb`, `outcome` (a call's outcome, a refusal's code, the reason a session ended), every value text; a line the log could not read and the mark of older lines left out are rows of their own. Raises ShapeError."""
    lines = _list(_data(result))
    rows = []
    for line in reversed(lines):
        _dict(line)
        if line.get("unreadable") is True:
            rows.append({"time": "-", "session": "-", "event": "(a line the log could not read)", "verb": "-", "outcome": "-"})
            continue
        if line.get("truncated") is True:
            rows.append({"time": "-", "session": "-", "event": "(older lines left out: too large to send)", "verb": "-", "outcome": "-"})
            continue
        outcome = value_text(next((line[key] for key in ("outcome", "code", "reason") if key in line), None))
        if line.get("exit_code") is not None:
            outcome += ", exit code " + value_text(line["exit_code"])
        rows.append({"time": value_text(line.get("t")), "session": value_text(line.get("session")), "event": value_text(line.get("event")), "verb": value_text(line.get("verb")), "outcome": outcome})
    return {"rows": rows}


def build(answers, makers):
    """Blocks from answers: `answers` maps a key to `("ok", result)` or `("error", error)`, `makers` maps the same key to the function that makes its block from the result. Each block is `{"ok": True, ...}` or `{"ok": False, "sentence", "errors", "code"}`; a key without an answer is left out."""
    blocks = {}
    for key, make in makers.items():
        if key not in answers:
            continue
        outcome, value = answers[key]
        if outcome == "ok":
            try:
                blocks[key] = {"ok": True, **make(value)}
            # Any exception, as in `overview.build`: one block must never fail the page.
            except Exception:
                blocks[key] = {"ok": False, "sentence": SHAPE_SENTENCE, "errors": None, "code": "shape"}
            continue
        blocks[key] = {"ok": False, "code": getattr(value, "code", None), **failure(value)}
    return blocks

