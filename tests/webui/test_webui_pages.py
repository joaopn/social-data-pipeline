"""Tests for the pages of the web UI: the templates, what the Overview shows, and the bound on broker calls.

The contracts pinned here, each test naming the bug class it catches:

- f. No template and no rendered page holds an inline script, an inline style, a `style=` or an `on...=` attribute, and nothing is marked safe.
- g. Markup and quotes in a source name, a database name, a run's source, a profile and an error text are served escaped; a name that is not a plain name is not a link; a plain name's link is its URL-encoded form.
- h. Each of the three reads failing alone leaves the other two blocks; the notices for a database without authentication and for ports open beyond the machine; the pages of this version call nothing but read verbs.
- i. A slow broker call stalls neither the login page nor the health check, and the bound on workers is kept: a call beyond it is answered "busy" at once and reaches no broker.
"""

from __future__ import annotations

import re
import threading
import time
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette.testclient")

from markupsafe import escape
from webui_helpers import ORIGIN, a_run, db_status, form_token, log_in, run_list, session_cookie, source_list, with_cookie

from social_data_pipeline.control import client as broker_client
from social_data_pipeline.control import verbs
from social_data_pipeline.webui import app as webapp
from social_data_pipeline.webui import overview, words

PACKAGE = Path(webapp.__file__).resolve().parent
HOSTILE = "<script>alert('x')</script>\"&"
INLINE_SCRIPT = re.compile(r"<script(?![^>]*\bsrc=)[^>]*>", re.IGNORECASE)
INLINE_STYLE = re.compile(r"<style\b", re.IGNORECASE)
STYLE_ATTRIBUTE = re.compile(r"<[^>]*\sstyle\s*=", re.IGNORECASE)
EVENT_ATTRIBUTE = re.compile(r"<[^>]*\son[a-z]+\s*=", re.IGNORECASE)


def inline_code(text):
    """What of the forbidden forms `text` holds."""
    return [pattern.pattern for pattern in (INLINE_SCRIPT, INLINE_STYLE, STYLE_ATTRIBUTE, EVENT_ATTRIBUTE) if pattern.search(text)]


@pytest.fixture
def app(make_app):
    return make_app()


@pytest.fixture
def client(app, make_client):
    made = make_client(app)
    assert log_in(made).status_code == 303
    return made


def blocks_of(page):
    """The text of each section of the Overview, by its title."""
    found = {}
    for title, body in re.findall(r'<h2 id="[a-z]+-title">([^<]+)</h2>(.*?)</section>', page, re.DOTALL):
        found[title] = body
    return found


# ── f. no inline code ───────────────────────────────────────────────────────


def test_no_template_holds_inline_code_or_marks_anything_safe():
    """Bug class: an inline script, style or event handler in a template, which the policy `default-src 'self'` blocks (a broken page) or which tempts a loosening of the policy; a value marked safe, which turns hostile text into markup."""
    templates = sorted((PACKAGE / "templates").glob("*.html"))
    assert len(templates) >= 6
    for path in templates:
        text = path.read_text()
        assert inline_code(text) == [], path.name
        assert "|safe" not in text.replace(" ", "") and "Markup" not in text and "autoescape false" not in text, path.name
    for path in PACKAGE.glob("*.py"):
        text = path.read_text()
        assert "Markup(" not in text and "autoescape=False" not in text and "|safe" not in text, path.name


def test_the_checks_find_what_they_look_for():
    """Bug class: patterns that never match, which would make the scans above pass whatever the templates hold."""
    for sample in ("<script>x</script>", "<style>a{}</style>", '<p style="x">', '<a href="/" onclick="x">', "<BODY ONLOAD=x>"):
        assert inline_code(sample), sample
    assert inline_code('<script src="/static/app.js" defer></script>') == []


def test_no_rendered_page_holds_inline_code(client, broker, make_app, make_client):
    """Bug class: inline code that only appears in a page as rendered (a block, a notice, an error output)."""
    broker.answers["db.status"] = db_status(auth={"enabled": False, "rw_users": []}, mcp=[{"db": "postgres", "port": 8000, "running": True}])
    broker.answers["run.list"] = run_list(a_run(), docker_available=False)
    pages = [client.get(route).text for route in ("/", "/sources", "/sources/demo", "/databases", "/databases/postgres", "/runs", "/system")]
    broker.answers["source.list"] = broker_client.BrokerError("failed", "x", details={"errors": HOSTILE})
    pages.append(client.get("/").text)
    stranger = make_client(make_app())
    pages += [stranger.get("/login").text, stranger.get("/login?why=ended").text, log_in(stranger, "made-up-wrong-pw").text]
    for page in pages:
        assert page.startswith("<!doctype html>")
        assert inline_code(page) == []


# ── g. rendering ────────────────────────────────────────────────────────────


def test_hostile_text_is_served_escaped(client, broker):
    """Bug class: cross-site scripting through data: a source name, a database name, a run's source, a profile or a command's error output written into the page as markup."""
    broker.answers["db.status"] = db_status(configured=[HOSTILE], databases=[{"db": HOSTILE, "port": HOSTILE, "running": True}], auth={"enabled": True, "rw_users": []})
    broker.answers["source.list"] = source_list({"name": HOSTILE, "profiles": [HOSTILE]})
    broker.answers["run.list"] = broker_client.BrokerError("failed", "x", details={"errors": HOSTILE + "\x1b[31m"})
    page = client.get("/").text
    assert HOSTILE not in page and "<script>" not in page
    assert page.count(str(escape(HOSTILE))) >= 5

    broker.answers["run.list"] = run_list(a_run(source=HOSTILE, profile=HOSTILE, id=HOSTILE, state="running"))
    page = client.get("/").text
    assert HOSTILE not in page and "<script>" not in page
    assert page.count(str(escape(HOSTILE))) >= 3


@pytest.mark.parametrize("name", ["a/b", "../etc", ".hidden", "with space", "q?x=1", "h#frag", "caf\u00e9", "-dash", "x" * 65, "a%2Fb", "javascript:alert(1)"])
def test_a_name_that_is_not_plain_is_not_a_link(client, broker, name):
    """Bug class: a link built from any name, which makes a path, a query, a fragment or a scheme of the name's author's choosing."""
    broker.answers["source.list"] = source_list({"name": name, "profiles": []})
    broker.answers["run.list"] = run_list(a_run(source=name))
    page = client.get("/").text
    hrefs = re.findall(r'href="([^"]*)"', page)
    assert [href for href in hrefs if href.startswith("/sources/")] == []
    assert str(escape(overview.text(name))) in page


def test_a_plain_name_is_a_link_of_its_url_encoded_form(client, broker):
    """Bug class: a plain name linked without encoding (the pattern is the only thing standing between a name and a path), or linked to the wrong page."""
    names = ["demo", "Reddit_2024.v2-x", "0"]
    broker.answers["source.list"] = source_list(*({"name": name, "profiles": []} for name in names))
    page = client.get("/").text
    hrefs = re.findall(r'href="(/sources/[^"]*)"', page)
    assert hrefs == [f"/sources/{name}" for name in names]
    assert overview.link("/sources/", "Reddit_2024.v2-x") == "/sources/Reddit_2024.v2-x"
    assert [client.get(href).status_code for href in hrefs] == [200, 200, 200]
    assert client.get("/sources/.hidden").status_code == 404


# ── h. the Overview ─────────────────────────────────────────────────────────


@pytest.mark.parametrize("failing", overview.READ_VERBS)
def test_each_read_failing_alone_leaves_the_other_blocks(client, broker, failing):
    """Bug class: one failed read (Docker down for `run.list`, a damaged source for `source.list`) failing the whole page."""
    broker.answers["run.list"] = run_list(a_run())
    broker.answers[failing] = broker_client.BrokerError("timeout", "x")
    response = client.get("/")
    assert response.status_code == 200
    blocks = blocks_of(response.text)
    titles = {"db.status": "Databases", "source.list": "Sources", "run.list": "Runs alive now"}
    for verb, title in titles.items():
        if verb == failing:
            assert words.CODE_SENTENCES["timeout"] in blocks[title]
        else:
            assert words.CODE_SENTENCES["timeout"] not in blocks[title]
            assert "<table>" in blocks[title]


def test_an_answer_of_another_shape_fails_its_block_only(client, broker):
    """Bug class: an unexpected answer (a list for a dict, a huge number) raising and failing the page."""
    broker.answers["db.status"] = {"data": {"configured": ["postgres"], "databases": "not a list"}}
    broker.answers["source.list"] = {"data": {"sources": [{"name": "demo", "profiles": "parse"}]}}
    broker.answers["run.list"] = run_list(a_run(id=10 ** 5000))
    response = client.get("/")
    assert response.status_code == 200
    assert response.text.count(str(escape(overview.SHAPE_SENTENCE))) == 3


def test_a_database_without_authentication_gets_a_standing_notice(client, broker):
    """Bug class: a database that anyone on the network can read and change shown like any other."""
    broker.answers["db.status"] = db_status(auth={"enabled": False, "rw_users": []})
    page = client.get("/").text
    assert "The database postgres runs without authentication" in page and "The database mongo runs without authentication" in page
    broker.answers["db.status"] = db_status()
    page = client.get("/").text
    assert "without authentication" not in page
    broker.answers["db.status"] = db_status(auth={"enabled": True, "rw_users": [{"db": "postgres", "user": "postgres"}]})
    page = client.get("/").text
    assert "The database mongo runs without authentication" in page and "The database postgres runs" not in page


def test_ports_open_beyond_the_machine_get_one_notice(client, broker):
    """Bug class: services reachable from every network the machine is on, shown without a word; or one notice per service that buries the rest of the page."""
    broker.answers["db.status"] = db_status(
        configured=["postgres", "starrocks"],
        databases=[{"db": "postgres", "port": "5432", "running": True}, {"db": "starrocks", "port": "9030", "fe_http_port": "8030", "running": True}],
        mcp=[{"db": "postgres", "port": 8000, "running": True}, {"db": "mongo", "port": 3000, "running": False}],
        jobs={"port": 8050, "running": True},
        auth={"enabled": True, "rw_users": [{"db": "postgres"}, {"db": "starrocks"}]},
    )
    page = client.get("/").text
    notices = re.findall(r"These services publish their ports.*?</p>", page, re.DOTALL)
    assert len(notices) == 1
    assert "postgres (5432), starrocks (9030, 8030, 8040), postgres-mcp (8000), jobs (8050)." in " ".join(notices[0].split())
    broker.answers["db.status"] = db_status(databases=[{"db": "postgres", "port": "5432", "running": False}], configured=["postgres"])
    assert "These services publish their ports" not in client.get("/").text


def test_the_runs_block_lists_only_runs_alive_now(client, broker):
    """Bug class: finished runs listed as if alive, or a terminal run left out."""
    broker.answers["run.list"] = run_list(a_run(id="r20261121T101500-aaaaaa", state="finished"), a_run(id="r20261121T101500-bbbbbb", state="detached"), a_run(id="live-0123456789ab", kind="terminal", state="running"), a_run(id="r20261121T101500-cccccc", state="cancelled"))
    runs = blocks_of(client.get("/").text)["Runs alive now"]
    assert "bbbbbb" in runs and "live-0123456789ab" in runs
    assert "aaaaaa" not in runs and "cccccc" not in runs


def test_the_pages_call_nothing_but_read_verbs(client, broker, make_app, make_client):
    """Bug class: a page or a GET calling a verb that changes something (the full sweep of every GET route is `test_webui_management.py`, property i)."""
    for route in ("/", "/sources", "/sources/demo", "/databases", "/databases/postgres", "/runs", "/system", "/login", "/healthz"):
        client.get(route)
        client.head(route)
    client.post("/logout", data={"form_token": form_token(client)}, headers={"origin": ORIGIN})
    called = broker.verbs_called()
    assert called and set(called) <= {name for name, verb in verbs.VERBS.items() if verb.cls == "read"}
    assert set(overview.READ_VERBS) <= set(called)
    assert all(call[3] == {} for call in broker.calls if call[0] == "call" and call[2] in overview.READ_VERBS)


# ── i. a slow broker ────────────────────────────────────────────────────────


def test_a_slow_broker_stalls_neither_the_login_page_nor_the_health_check(make_app, make_client, broker):
    """Bug class: broker calls run on the event loop, or in a pool shared with everything else, so one slow call (a read takes up to 60 seconds) freezes the login page and the health check; and no bound on workers, so slow calls pile up threads."""
    from starlette.testclient import TestClient

    app = make_app(page_workers=1)
    cookie = session_cookie(log_in(make_client(app)))
    broker.calls.clear()
    broker.entered = 0
    broker.gate = threading.Event()
    results = []
    with TestClient(app, base_url="https://localhost:8443", client=("172.30.0.1", 50000), follow_redirects=False) as shared:
        slow = threading.Thread(target=lambda: results.append(shared.get("/", headers=with_cookie(cookie)).status_code))
        slow.start()
        deadline = time.monotonic() + 10
        while broker.entered < 1 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert broker.entered == 1
        started = time.monotonic()
        assert shared.get("/login").status_code == 200
        assert shared.get("/login", headers=with_cookie(cookie)).status_code == 303
        assert shared.get("/healthz").text == "ok"
        busy = shared.get("/", headers=with_cookie(cookie))
        elapsed = time.monotonic() - started
        assert busy.status_code == 503 and webapp.BUSY in busy.text
        assert elapsed < 2
        assert broker.entered == 1
        broker.gate.set()
        slow.join(10)
    assert results == [200]
    assert broker.verbs_called() == list(overview.READ_VERBS)
