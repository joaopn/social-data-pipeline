"""Tests for the Management pages (`webui.management`, `webui.pages`): what they show of the broker's answers, and the properties that hold over all of them.

The contracts pinned here, each test naming the bug class it catches:

- h. Markup, quotes and a `javascript:` string in every kind of text the broker hands over (a source name, a profile, state-file values of several JSON types, a progress file name, an error-log section, a verify message, a run's source and flags, a run's output, an audit value, a command's output and errors) are served escaped, in element and attribute positions; no form value or option is made from them; no name that is not plain becomes a link.
- i. No GET or HEAD of any route asks the broker for anything but a read verb or `hello`.
- j. Each read of each page failing on its own leaves the page and its other blocks.
- k. The scans of `test_webui_pages.py` (no inline script, style or event attribute) over every new page and partial; the headers over the new routes are in `test_webui_guard.py`.
- And what each page shows: the sources, a source's state, finished files and error logs (one too large: the terminal command instead), the databases with their checks, the runs, one run's facts and its cancel form while it is alive, the broker's greeting, the audit view and the table of actions.

Every special character in this file is written as an escape.
"""

from __future__ import annotations

import html
import re

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette.testclient")

from test_webui_pages import inline_code
from webui_helpers import (
    PASSWORD,
    RUN_ID,
    a_run,
    choose,
    confirm,
    db_status,
    db_verify,
    error_logs,
    form_token,
    log_in,
    pending_path,
    run_list,
    source_list,
    source_progress,
    source_status,
)

from social_data_pipeline.control import client as broker_client
from social_data_pipeline.control import verbs
from social_data_pipeline.webui import actions, overview, words

HOSTILE = "<script>alert('x')</script>\"&"
SCRIPT = "javascript:alert(1)"
RAW_MARKS = ("<script>", "alert('x')", "</script>\"")
ESCAPED_MARK = "&lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt;"
HX = {"hx-request": "true"}


@pytest.fixture
def app(make_app):
    return make_app(action_wait_seconds=0.5)


@pytest.fixture
def client(app, make_client):
    made = make_client(app)
    assert log_in(made).status_code == 303
    return made


def raw_hostile(page):
    return [mark for mark in RAW_MARKS if mark in page]


def attributes(page):
    """(name, value) of every attribute of every tag of a page."""
    return [(name, html.unescape(value)) for tag in re.findall(r"<[a-zA-Z][^>]*>", page) for name, value in re.findall(r'\s([a-zA-Z][\w:-]*)="([^"]*)"', tag)]


def hostile_answers(broker):
    """Answers of every read verb with the hostile text in each place the pages show one."""
    broker.answers["source.list"] = source_list({"name": HOSTILE, "profiles": [HOSTILE]}, {"name": SCRIPT, "profiles": []}, {"name": "demo", "profiles": ["parse"]})
    broker.answers["source.status"] = lambda fields: source_status(
        fields["name"], profiles=[HOSTILE, "parse", "postgres_ingest"], platform=HOSTILE, data_types=[HOSTILE, 3, None], paths={HOSTILE: HOSTILE, "parsed": {"nested": HOSTILE}}, warnings=[HOSTILE],
        ingestion=[{"db_type": HOSTILE, "has_state_files": True, "entries": [{"label": HOSTILE, "count": 2, "latest": {"k": HOSTILE}, "in_progress": [HOSTILE, 1, None, True], "failed_count": 1.5, "last_updated": HOSTILE}]}],
    )
    item = {"data_type": HOSTILE, "exists": True, "not_readable": None, "finished": HOSTILE, "being_written": 2, "newest": {"name": HOSTILE, "modified": HOSTILE}}
    broker.answers["source.progress"] = lambda fields: source_progress(fields["name"], warnings=[HOSTILE], stages=[{"stage": HOSTILE, "kind": "parse", "path": HOSTILE, "not_readable": None, "data_types": [item, {**item, "not_readable": HOSTILE}]}])
    broker.answers["source.error_logs"] = lambda fields: error_logs(fields["profile"], name=fields["name"], files=[{"label": HOSTILE, "failed": [{"filename": HOSTILE, "error": HOSTILE + "\x1b[31m\nsecond line", "timestamp": HOSTILE}]}], sections=[{"log_file": HOSTILE, "header": [HOSTILE], "output": [HOSTILE, 5]}])
    broker.answers["db.status"] = db_status(
        configured=["postgres", HOSTILE],
        databases=[{"db": "postgres", "path": HOSTILE, "port": HOSTILE, "running": True}, {"db": HOSTILE, "path": HOSTILE, "port": 1, "running": False}],
        auth={"enabled": True, "rw_users": [{"db": "postgres", "user": HOSTILE}]},
        mcp=[{"db": HOSTILE, "port": HOSTILE, "running": True}],
    )
    broker.answers["db.verify"] = db_verify(results={"postgres": {"ok": False, "findings": [{"category": HOSTILE, "message": HOSTILE, "fix": HOSTILE}]}, HOSTILE: {"ok": False, "findings": [{"category": HOSTILE, "message": HOSTILE, "fix": HOSTILE}]}}, ok=False, warnings=[HOSTILE])
    broker.answers["run.list"] = run_list(
        a_run(id=HOSTILE, source=SCRIPT, profile=HOSTILE, state=HOSTILE, flags={HOSTILE: HOSTILE}),
        a_run(source=HOSTILE, flags={"filter": HOSTILE, "n": [HOSTILE]}, started_by=HOSTILE, started=HOSTILE, exit_code=HOSTILE),
        a_run(id="live-0123456789ab", kind="terminal", source="..", state="running"),
    )
    broker.answers["run.logs"] = lambda fields: {"data": {"id": fields["run_id"], "kind": "web", "state": "finished", "text": HOSTILE + "\n" if fields["offset"] == 0 else "", "next_offset": len(HOSTILE) + 1, "complete": True}}
    broker.answers["audit.list"] = {"data": [{"n": 1, "t": HOSTILE, "session": HOSTILE, "event": "call_refused", "verb": HOSTILE, "code": HOSTILE, "field": None}, {"n": 2, "t": "2026-11-21T10:00:00Z", "session": 3, "event": HOSTILE, "verb": "db.stop", "fields": {}, "outcome": HOSTILE, "exit_code": HOSTILE}, {"unreadable": True}]}
    broker.answers["db.stop"] = {"exit_code": 1, "output": HOSTILE + "\n", "errors": HOSTILE + "\x1b[1m\n"}


def test_hostile_text_is_escaped_everywhere_and_never_an_attribute(client, broker):
    """Bug class: cross-site scripting through anything the broker hands over (names, state files, logs, Docker's output, the audit log, a command's output), in a text position or in an attribute (a form's value, an option, a link)."""
    hostile_answers(broker)
    token = form_token(client)
    outcome_page = client.get(confirm(client, pending_path(choose(client, "db.stop", token)), token).headers["location"]).text
    page_of_run = client.get(f"/runs/{RUN_ID}").text
    view = re.search(r'hx-get="([^"]+)" hx-trigger="load"', page_of_run).group(1)
    pages = {route: client.get(route).text for route in ("/sources", "/sources/demo", "/databases", "/databases/postgres", "/runs", "/system")}
    pages[f"/runs/{RUN_ID}"] = page_of_run
    pages["output"] = client.get(html.unescape(view), headers=HX).text
    pages["outcome"] = outcome_page
    allowed_values = {token, "", *actions.TABLE, *actions.VALID_PROFILES, *actions.SERVICES, *actions.DATABASES, "demo", "yes", RUN_ID, "live-0123456789ab"}
    for route, page in pages.items():
        assert raw_hostile(page) == [], route
        assert ESCAPED_MARK in page or "&lt;script&gt;" in page, route
        for name, value in attributes(page):
            assert "<" not in value and SCRIPT not in value, (route, name)
            if name == "value":
                assert value in allowed_values, (route, value)
            if name in ("href", "hx-get", "action"):
                assert value.startswith("/") and not value.startswith("//"), (route, value)
        assert [value for name, value in attributes(page) if name == "href" and value.startswith(("/sources/", "/runs/r", "/runs/l"))] == [value for name, value in attributes(page) if name == "href" and re.fullmatch(r"/sources/demo(\?tab=[a-z]+|#run-[a-z_]+)?|/runs/(r[0-9]{8}T[0-9]{6}-[0-9a-f]{6}|live-[0-9a-f]{12})", value)], route
    assert pages["/sources"].count(ESCAPED_MARK) >= 2
    assert pages["/sources/demo"].count("&lt;script&gt;") >= 15
    assert pages["/system"].count(ESCAPED_MARK) >= 5
    assert pages["output"].count(ESCAPED_MARK) == 1 and pages["outcome"].count(ESCAPED_MARK) == 2
    assert 'href="/sources/demo"' in pages["/sources"] and '<a href="/runs/live-0123456789ab">' in pages["/runs"]
    forms = re.findall(r'<input type="hidden" name="db" value="([^"]*)">', pages["/databases"])
    assert forms == ["postgres", "postgres"]


@pytest.mark.parametrize("name", ["a/b", ".hidden", "-dash", "x" * 65, "caf\u00e9", "with space", SCRIPT, HOSTILE])
def test_a_source_name_that_is_not_plain_is_not_a_link_nor_a_page(client, broker, name):
    """Bug class: a link or a page built from any name (a path, a scheme or markup of the name's author's choosing), or a page asking the broker for a name it would refuse."""
    broker.answers["source.list"] = source_list({"name": name, "profiles": []})
    broker.answers["run.list"] = run_list(a_run(source=name))
    for route in ("/sources", "/runs"):
        page = client.get(route).text
        assert [value for key, value in attributes(page) if key == "href" and value.startswith("/sources/")] == []
    broker.requests.clear()
    assert client.get("/sources/" + name.replace("/", "%2F").replace(" ", "%20")).status_code == 404
    assert broker.requests == []


# ── i. nothing state-changing on any GET ────────────────────────────────────


def state_of(app, client):
    """Everything the web UI keeps for a client's session, and the number of sessions."""
    work = app.sessions.get(client.cookies.get("__Host-sdp_session")).work
    return (len(app.sessions), sorted(work._pending), work.outcome_ids(), [(key, view.offset, view.held, view.shown, view.mode) for key, view in work._views.items()], work.db_password_kept)


def test_no_get_or_head_asks_the_broker_for_anything_but_reads_and_no_head_changes_anything(client, broker, app):
    """Bug class: a GET or a HEAD (a reload, a prefetch, a link opened by another site through the browser) that runs or confirms an action; a HEAD (which a browser or a proxy may send on its own) that makes or advances an output view, takes a pending action, or makes anything else."""
    token = form_token(client)
    pending = pending_path(choose(client, "db.reset", token, db="postgres"))
    outcome = confirm(client, pending_path(choose(client, "db.stop", token)), token).headers["location"]
    view = html.unescape(re.search(r'hx-get="([^"]+)" hx-trigger="load"', client.get(f"/runs/{RUN_ID}").text).group(1))
    live_view = html.unescape(re.search(r'hx-get="([^"]+)" hx-trigger="load"', client.get("/runs/live-0123456789ab").text).group(1))
    broker.calls.clear()
    broker.requests.clear()
    routes = [view, live_view, "/", "/sources", "/sources/demo", "/databases", "/databases/postgres", "/databases/mongo", "/runs", f"/runs/{RUN_ID}", f"/runs/{RUN_ID}?offset=5", "/runs/live-0123456789ab", "/system", pending, outcome, "/login", "/healthz", "/static/app.css"]
    before = state_of(app, client)
    for route in routes:
        for headers in ({}, HX):
            assert client.head(route, headers=headers).status_code in (200, 303, 405), route
    assert state_of(app, client) == before
    assert {call[2] for call in broker.calls if call[0] == "call"} <= set(overview.READ_VERBS)
    for route in routes:
        for headers in ({}, HX):
            assert client.get(route, headers=headers).status_code in (200, 303), route
    read_verbs = {name for name, verb in verbs.VERBS.items() if verb.cls == "read"}
    assert broker.calls and {call[0] for call in broker.calls} <= {"call", "hello"}
    assert {call[2] for call in broker.calls if call[0] == "call"} <= read_verbs
    assert {"db.status", "db.verify", "source.list", "source.status", "source.progress", "run.list", "run.logs", "audit.list"} <= {call[2] for call in broker.calls if call[0] == "call"}
    assert client.get(pending).status_code == 200
    assert [client.head(route).status_code for route in (f"/runs/{RUN_ID}", view, pending, outcome, "/sources", "/system")] == [405] * 6


# ── j. each read fails on its own block ─────────────────────────────────────


def failing(verb, broker, when=None):
    """Make `verb` fail with `timeout` (only for the fields `when` accepts, when given)."""
    kept = broker.answers[verb]

    def answer(fields):
        if when is None or when(fields):
            raise broker_client.BrokerError("timeout", "x")
        return kept(fields) if callable(kept) else kept

    broker.answers[verb] = answer


TIMEOUT = words.CODE_SENTENCES["timeout"]


@pytest.mark.parametrize("verb, when, kept, gone", [
    ("source.status", None, ["3 finished", "Finished files per stage"], ["Platform"]),
    ("source.progress", None, ["Platform", "No errors.", "Run postgres_ingest"], ["3 finished"]),
    ("source.error_logs", lambda fields: fields["profile"] == "postgres_ingest", ["Platform", "3 finished", re.compile(r'<h3 class="mono">mongo_ingest</h3>\s*<div class="cli mt">[^<]*</div>\s*<p class="muted">No errors.</p>'), "Run parse"], []),
])
def test_each_read_of_the_source_page_fails_on_its_own(client, broker, verb, when, kept, gone):
    """Bug class: one failed read of a source (a damaged state file, a time-out) failing the whole page, or taking the forms with it when it is not the read they need."""
    broker.answers["source.status"] = lambda fields: source_status(fields["name"], profiles=["parse", "postgres_ingest", "mongo_ingest"])
    failing(verb, broker, when)
    response = client.get("/sources/demo")
    assert response.status_code == 200
    assert response.text.count(TIMEOUT) == 1
    for text in kept:
        assert (text.search(response.text) if isinstance(text, re.Pattern) else text in response.text), text
    for text in gone:
        assert text not in response.text, text
    if verb == "source.status":
        assert "profiles are not known" in response.text and "Remove this source" in response.text


@pytest.mark.parametrize("verb, kept", [("db.status", ['<span class="ok">ok   </span> postgres', '<span class="ok">ok   </span> mongo']), ("db.verify", ["PostgreSQL", "MongoDB", "Reset postgres", "The checks could not be read"])])
def test_each_read_of_the_databases_page_fails_on_its_own(client, broker, verb, kept):
    """Bug class: the checks lost because the status failed, or the status lost because the checks (which probe the containers) failed."""
    failing(verb, broker)
    response = client.get("/databases")
    assert response.status_code == 200 and response.text.count(TIMEOUT) == 1
    for text in kept:
        assert text in response.text, text


@pytest.mark.parametrize("verb, kept", [("hello", ["made-up-audit-verb"]), ("audit.list", ["Protocol"])])
def test_each_read_of_the_system_page_fails_on_its_own(client, broker, verb, kept):
    """Bug class: an audit log that cannot be read hiding the broker's greeting, or the reverse; the table of actions lost with either."""
    broker.answers["audit.list"] = {"data": [{"n": 1, "t": "2026-11-21T10:00:00Z", "session": 1, "event": "call_accepted", "verb": "made-up-audit-verb", "fields": {}, "db_password_from": "none"}]}
    failing(verb, broker)
    response = client.get("/system")
    assert response.status_code == 200 and response.text.count(TIMEOUT) == 1
    assert "<code>db.reset</code></td><td>step_up</td>" in response.text
    for text in kept:
        assert text in response.text, text


@pytest.mark.parametrize("route", ["/sources", "/runs", f"/runs/{RUN_ID}"])
def test_a_page_of_one_read_says_why_and_keeps_its_frame(client, broker, route):
    """Bug class: a page whose only read failed answered with an error status or a blank page instead of the sentence; the run page losing its output view with the list."""
    failing("source.list" if route == "/sources" else "run.list", broker)
    response = client.get(route)
    assert response.status_code == 200 and TIMEOUT in response.text and 'aria-label="Management"' in response.text
    if route.startswith("/runs/"):
        assert 'class="output-view"' in response.text


# ── k. the scans over the new pages and partials ────────────────────────────


def test_no_new_page_or_partial_holds_inline_code(client, broker, monkeypatch):
    """Bug class: inline code in a page or a part of a page that only this version renders (a pending page, an outcome, an output piece, a refusal, a busy answer), which the policy blocks."""
    hostile_answers(broker)
    token = form_token(client)
    pending = pending_path(choose(client, "db.reset", token, db="postgres"))
    rendered = [client.get(pending).text, confirm(client, pending, token).text, confirm(client, pending, token, "made-up-wrong-pw").text, choose(client, "run.start", token, profile="x").text]
    outcome = confirm(client, pending, token, PASSWORD).headers["location"]
    rendered += [client.get(outcome).text, client.get(outcome, headers=HX).text]
    for route in ("/sources", "/sources/demo", "/databases", "/databases/mongo", "/runs", f"/runs/{RUN_ID}", "/system"):
        rendered.append(client.get(route).text)
    view = html.unescape(re.search(r'hx-get="([^"]+)" hx-trigger="load"', rendered[-2]).group(1))
    rendered.append(client.get(view, headers=HX).text)
    from social_data_pipeline.webui import calls

    view = html.unescape(re.search(r'hx-get="([^"]+)" hx-trigger="load"', client.get(f"/runs/{RUN_ID}").text).group(1))

    async def full(self, function, wait_seconds=0):
        return calls.BUSY_GATE

    monkeypatch.setattr(calls.Gate, "run", full)
    busy = [client.get("/sources"), client.get(view, headers=HX)]
    assert busy[0].status_code == 503 and "busy: trying again" in busy[1].text
    rendered += [response.text for response in busy]
    for page in rendered:
        assert inline_code(page) == []


# ── what the pages show ─────────────────────────────────────────────────────


def test_the_source_page_shows_state_progress_and_error_logs(client, broker):
    """Bug class: a source page that leaves out a stage, a failed file or a mongoimport section, or that reads the error logs of profiles the source does not have."""
    broker.answers["source.status"] = lambda fields: source_status(fields["name"], profiles=["parse", "mongo_ingest"], ingestion=[{"db_type": "mongo", "has_state_files": True, "entries": [{"label": "ingest_submissions", "count": 12, "latest": "RS_2024-02", "in_progress": None, "failed_count": 1, "last_updated": "2026-11-20T09:00:00Z"}]}])
    broker.answers["source.error_logs"] = lambda fields: error_logs(fields["profile"], files=[{"label": "submissions", "failed": [{"filename": "RS_2024-03", "error": "made-up import error", "timestamp": "2026-11-20"}]}], sections=[{"log_file": "mongoimport_1.log", "header": ["File: RS_2024-03"], "output": ["made-up mongoimport line"]}])
    page = client.get("/sources/demo").text
    for text in ("MongoDB ingestion", "ingest_submissions", "RS_2024-02", "3 finished, 1 being written", "RS_2024-01.parquet", "RS_2024-03", "made-up import error", "made-up mongoimport line", "Run parse", "Run mongo_ingest"):
        assert text in page, text
    assert [request["fields"].get("profile") for request in broker.requests if request["verb"] == "source.error_logs"] == ["mongo_ingest"]


def test_an_error_log_too_large_to_send_names_the_terminal_command(client, broker):
    """Bug class: an error log the broker refused as too large shown as an unexplained failure, with no way to read it."""
    broker.answers["source.status"] = lambda fields: source_status(fields["name"], profiles=["postgres_ingest"])
    broker.answers["source.error_logs"] = broker_client.BrokerError("too_large", "x")
    page = client.get("/sources/demo").text
    assert "<code>python sdp.py source error-logs demo --profile=postgres_ingest</code>" in page


def test_the_run_page_shows_the_facts_and_a_cancel_form_only_while_alive(client, broker):
    """Bug class: a cancel offered for a run that has ended (refused by the broker) or missing for one that is alive, or another run's facts shown."""
    broker.answers["run.list"] = run_list(a_run(state="running", flags={"filter": "RS_*"}), a_run(id="r20261121T101500-ffffff", state="finished", exit_code=0))
    page = client.get(f"/runs/{RUN_ID}").text
    assert 'name="action" value="run.cancel"' in page and f'name="run_id" value="{RUN_ID}"' in page and "filter=RS_*" in page
    assert [request["fields"] for request in broker.requests if request["verb"] == "run.list"][-1] == {"limit": 1000}
    page = client.get("/runs/r20261121T101500-ffffff").text
    assert "run.cancel" not in page and "<dd>finished</dd>" in page
    page = client.get("/runs/r20261121T101500-eeeeee").text
    assert "does not hold this run" in page and 'class="output-view"' in page


def test_the_runs_and_system_pages_list_what_the_broker_says(client, broker):
    """Bug class: runs of one kind left out (terminal runs, ended ones), the audit view in the wrong order, a refused call's verb or outcome left out, or the table of actions not the one the pages use."""
    broker.answers["run.list"] = run_list(a_run(), a_run(id="live-0123456789ab", kind="terminal", started_by=None), a_run(id="r20261121T091500-aaaaaa", state="cancelled", ended="2026-11-21T09:20:00Z", exit_code=130))
    page = client.get("/runs").text
    assert page.index(RUN_ID) < page.index("live-0123456789ab") < page.index("r20261121T091500-aaaaaa") and "130" in page and "2026-11-21T09:20:00Z" in page
    broker.answers["audit.list"] = {"data": [
        {"n": 1, "t": "2026-11-21T10:00:00Z", "session": 1, "event": "login"},
        {"n": 2, "t": "2026-11-21T10:01:00Z", "session": 1, "event": "call_refused", "verb": "db.reset", "code": "run_active", "field": None},
        {"n": 3, "t": "2026-11-21T10:02:00Z", "session": 1, "event": "call_ended", "verb": "db.stop", "fields": {}, "db_password_from": "none", "outcome": "exited", "exit_code": 0},
    ]}
    page = client.get("/system").text
    assert page.index("call_ended") < page.index("call_refused") < page.index("<td>login</td>")
    assert "<td>db.reset</td><td>run_active</td>" in page and "exited, exit code 0" in page
    assert [request["fields"] for request in broker.requests if request["verb"] == "audit.list"] == [{"limit": 200}]
    for action in actions.TABLE.values():
        assert f"<td><code>{action.name}</code></td><td>{action.cls}</td>" in page


def test_the_navigation_links_every_management_page(client):
    """Bug class: a Management page that cannot be reached from the frame, or one still marked as coming later."""
    page = client.get("/").text
    for address in ("/", "/sources", "/databases", "/runs", "/system"):
        assert f'href="{address}"' in page
    assert "next update" not in page and "next update" not in client.get("/runs").text


def test_a_started_run_links_to_its_page(client, broker, app):
    """Bug class: the outcome of `run.start` not leading to the run, or linking an id the broker sent that has no run id's form."""
    token = form_token(client)
    response = confirm(client, pending_path(choose(client, "run.start", token, profile="parse", source="demo")), token)
    assert f'<a href="/runs/{RUN_ID}">' in client.get(response.headers["location"]).text
    broker.answers["run.start"] = {"run_id": "../../x"}
    response = confirm(client, pending_path(choose(client, "run.start", token, profile="lingua", source="demo")), token)
    page = response.text if response.status_code == 200 else client.get(response.headers["location"]).text
    assert "../../x" not in page and "did not have the expected shape" in page
