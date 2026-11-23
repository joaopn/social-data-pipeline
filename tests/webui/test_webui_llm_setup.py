"""Tests for the setup of the MCP servers and of the query scheduler from the browser, and for the restart that applies it (`webui.llm_setup`, the table's `db.setup_mcp` and `db.setup_jobs`, `management.run_steps`, the Overview's restart line).

The contracts pinned here, each test naming the bug class it catches:

- The forms are filled with the answers `db.llm_settings` gives, and what a browser posts from an unchanged form is those answers; the web UI's own boxes (a server switched on, a target added) decide what is kept, and a box ticked for a server or a target without its values is refused.
- Nothing is asked of the broker but reads before the confirm, and the confirm sends exactly the fields kept, with the management password.
- The confirm page's sentences, pinned whole; the write-access warning only with write access.
- The scheduler page's login: while it is on, the form keeps it on.
- The restart: the line of Needs attention names the services, a database's MCP server is left out when its database is named, the steps are sent in order after the management password was checked, a step that fails stops the rest, and a wrong management password stops everything.

Every special character in this file is written as an escape.
"""

from __future__ import annotations

import html
import re

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette.testclient")

from test_webui_pages import inline_code
from webui_helpers import DONE, ORIGIN, PASSWORD, confirm, form_token, llm_settings, log_in, pending_path, restart_needed, wait_until

from social_data_pipeline.control import client as broker_client
from social_data_pipeline.webui import actions, llm_setup


@pytest.fixture
def app(make_app):
    return make_app(action_wait_seconds=0.5)


@pytest.fixture
def client(app, make_client):
    made = make_client(app)
    assert log_in(made).status_code == 303
    return made


def drawn_form(page, action):
    """What a browser posts from the form of `page` that posts to `action`, unchanged: every hidden, text and number input with its value, every ticked box."""
    form = re.search(rf'<form method="post" action="{action}"[^>]*>(.*?)</form>', page, re.DOTALL).group(1)
    posted = {}
    for tag in re.findall(r"<input[^>]*>", form):
        name = re.search(r'name="([^"]+)"', tag).group(1)
        kind = re.search(r'type="([^"]+)"', tag).group(1)
        value = html.unescape(re.search(r'value="([^"]*)"', tag).group(1)) if 'value="' in tag else ""
        if kind == "checkbox" and " checked" not in tag:
            continue
        if kind == "password":
            continue
        posted[html.unescape(name)] = value
    return posted


def post(client, path, data):
    return client.post(path, data=data, headers={"origin": ORIGIN})


def kept(app, client, response):
    """The fields of the pending action a post was answered with."""
    pending_id = pending_path(response).rsplit("/", 1)[1]
    work = app.sessions.get(client.cookies.get("__Host-sdp_session")).work
    return work._pending[pending_id]


def changing_requests(broker):
    return [request["verb"] for request in broker.requests if request["verb"] not in ("db.status", "source.list", "run.list", "db.restart_needed", "db.llm_settings", "audit.list")]


# ── the forms ───────────────────────────────────────────────────────────────


def test_an_unchanged_mcp_form_posts_the_current_answers_and_nothing_is_sent_before_the_confirm(app, client, broker):
    """Bug class: a form that, posted unchanged, changes the setup (a server that is on switched off, a port of the compose file instead of the one written, write access switched on); a choice that reaches the broker before the operator confirmed it; a confirm that sends other fields than the ones shown."""
    broker.answers["db.llm_settings"] = llm_settings()
    page = client.get("/setup/mcp").text
    data = drawn_form(page, "/setup/mcp")
    assert data["on_postgres"] == "yes" and "on_mongo" not in data and data["postgres[port]"] == "8000" and "postgres[write_access]" not in data
    response = post(client, "/setup/mcp", data)
    pending = kept(app, client, response)
    assert (pending.action, pending.fields, pending.step_up) == ("db.setup_mcp", {"postgres": {"port": 8000}}, True)
    assert changing_requests(broker) == []

    broker.answers["db.setup_mcp"] = dict(DONE)
    token = form_token(client)
    outcome = confirm(client, pending_path(response), token, PASSWORD)
    assert outcome.status_code == 303
    wait_until(lambda: changing_requests(broker))
    assert [(request["verb"], request["fields"], request["step_up"]) for request in broker.requests if request["verb"] == "db.setup_mcp"] == [("db.setup_mcp", {"postgres": {"port": 8000}}, True)]


@pytest.mark.parametrize("change, field", [
    ({"on_mongo": "yes", "mongo[port]": ""}, "MongoDB MCP server"),
    ({"on_postgres": "no"}, None),
    ({"postgres[port]": "80"}, "PostgreSQL MCP server"),
    ({"postgres[extra]": "1"}, "PostgreSQL MCP server"),
])
def test_the_mcp_forms_boxes_and_values_are_checked(app, client, broker, change, field):
    """Bug class: a server switched on without its port quietly left off, a box that posts anything but `yes` read as ticked, a port the broker refuses kept as a pending action."""
    broker.answers["db.llm_settings"] = llm_settings()
    data = {**drawn_form(client.get("/setup/mcp").text, "/setup/mcp"), **change}
    response = post(client, "/setup/mcp", data)
    assert response.status_code == 400
    assert (f"The field: {field}." in response.text) == (field is not None)
    assert app.sessions.get(client.cookies.get("__Host-sdp_session")).work.pending_count() == 0


def test_a_server_switched_off_drops_what_it_posted(app, client, broker):
    """Bug class: the port of a server switched off sent to the broker, which would then switch it on."""
    broker.answers["db.llm_settings"] = llm_settings()
    data = drawn_form(client.get("/setup/mcp").text, "/setup/mcp")
    del data["on_postgres"]
    data.update({"on_mongo": "yes", "mongo[port]": "3100", "mongo[write_access]": "yes"})
    assert kept(app, client, post(client, "/setup/mcp", data)).fields == {"mongo": {"port": 3100, "write_access": True}}


def test_a_refusal_of_the_terminal_is_shown_instead_of_the_form(client, broker):
    """Bug class: a form offered where the terminal command refuses (a database with authentication and no read-only credentials file), which the operator fills in only to be refused after the confirm."""
    answer = llm_settings()
    answer["data"]["mcp"]["refusal"] = ["  Error: RO credential file(s) missing for enabled databases:", "    - mongo: /x/.ro_credentials"]
    broker.answers["db.llm_settings"] = answer
    page = client.get("/setup/mcp").text
    assert "RO credential file(s) missing" in page and 'action="/setup/mcp"' not in page


def test_an_unchanged_queue_form_posts_the_current_answers(app, client, broker):
    """Bug class: a scheduler form that, posted unchanged, changes the setup: a target dropped, a timeout or a name not the one written, the results folder changed."""
    broker.answers["db.llm_settings"] = llm_settings()
    data = drawn_form(client.get("/setup/queue").text, "/setup/queue")
    pending = kept(app, client, post(client, "/setup/queue", data))
    assert pending.fields == {
        "port": 8050, "result_root": "data/jobs/results", "max_concurrent": 20, "history_retention": 500,
        "postgres_target": {"name": "pg_main", "database": "datasets", "timeout_hours": 0}, "mongo_target": {"name": "mongo_main", "timeout_hours": 0},
    }
    del data["add_mongo"]
    assert "mongo_target" not in kept(app, client, post(client, "/setup/queue", data)).fields


def test_the_login_stays_on_while_it_is_on(app, client, broker):
    """Bug class: a form posted unchanged while the scheduler page's login is on that switches it off (the broker would refuse it, and the operator would not know why), or a box offered that only the terminal may untick."""
    broker.answers["db.llm_settings"] = llm_settings(auth_enabled=True)
    page = client.get("/setup/queue").text
    assert '<input type="hidden" name="auth_enabled" value="yes">' in page and 'type="checkbox" name="auth_enabled"' not in page
    assert "Switching it off is done in a terminal: <code>python sdp.py db setup-jobs</code>" in page
    assert kept(app, client, post(client, "/setup/queue", drawn_form(page, "/setup/queue"))).fields["auth_enabled"] is True


def test_the_databases_page_links_both_forms_and_the_pages_have_no_inline_code(client, broker):
    """Bug class: a form nobody reaches from the page the mock-up puts its Change… on; inline script or style, which the content policy blocks."""
    page = client.get("/databases").text
    assert '<a class="btn sm" href="/setup/mcp">Change…</a>' in page and '<a class="btn sm" href="/setup/queue">Change…</a>' in page
    for route in ("/setup/mcp", "/setup/queue", "/restart?services=postgres"):
        assert inline_code(client.get(route).text) == [], route


# ── the confirm page ────────────────────────────────────────────────────────


MCP_SENTENCE = (
    "Writes config/db/mcp.yaml with the MCP servers switched on here (PostgreSQL, StarRocks), after copying the file it replaces to config/db/mcp.yaml.bak; a server switched off is left out of it, and db start no longer starts it. "
    "Sets in .env: POSTGRES_MCP_PORT and POSTGRES_MCP_ACCESS_MODE; STARROCKS_MCP_PORT; and POSTGRES_MCP_USER, STARROCKS_MCP_USER (the read-only user) for each of these databases that has authentication on with a read-only user. The .env keys of a server switched off stay as they are. "
    "Refused when a database with authentication has no read-only user or no read-only credentials file, and when a port is one another service of this deployment uses. "
    "Nothing is restarted: a running service keeps its old settings until it is restarted. Needs attention on the Overview names the services that must restart, with a Restart button."
)
WRITE_WARNING = "Write access for the PostgreSQL MCP server: it takes statements that change data. Any agent that reaches port 8100 can change data in PostgreSQL, unless PostgreSQL has authentication on with a read-only user: the server then logs in as that user, who cannot write."
JOBS_SENTENCE = (
    "Writes config/jobs/config.local.yaml with the targets on PostgreSQL, MongoDB and the numbers above, after copying the file it replaces to config/jobs/config.local.yaml.bak; sets JOBS_PORT and JOBS_RESULT_ROOT (./results) in .env; makes the results folder and data/jobs when they are missing. "
    "It rewrites docker-compose.override.yml as YAML (its comments are not kept, its other mounts are): the /jobs_export mount of PostgreSQL there becomes ./results:/jobs_export:rw; StarRocks, without a target here, keeps the /jobs_export mount it has there, if any. "
    "The mounts that change at each service's next start: the scheduler's /data/jobs/results and StarRocks's /jobs_export (docker-compose.yml mounts JOBS_RESULT_ROOT at both), the /jobs_export mounts of the override, and the web UI's results folder at its next python sdp.py webui start. "
    "The scheduler's own page asks for the database admin password; switching that off again is done in a terminal with python sdp.py db setup-jobs. "
    "Refused when a port is one another service of this deployment uses, and when the results folder is data/jobs or holds it (the scheduler would keep its control socket closed). "
    "Nothing is restarted: a running service keeps its old settings until it is restarted. Needs attention on the Overview names the services that must restart, with a Restart button."
)


def test_the_confirm_pages_say_what_the_commands_write(client, broker):
    """Bug class: a confirm page that says less than the action does (a .env key, the .bak copy, the compose override's rewrite and the mounts that change, a stale /jobs_export mount that stays, the login that only a terminal switches off, that nothing restarts), or that warns of write access when there is none."""
    broker.answers["db.llm_settings"] = llm_settings()
    data = {"form_token": form_token(client), "on_postgres": "yes", "postgres[port]": "8100", "on_starrocks": "yes", "starrocks[port]": "9000"}
    page = html.unescape(client.get(pending_path(post(client, "/setup/mcp", data))).text)
    assert MCP_SENTENCE in page and "Write access for" not in page
    assert "<code>python sdp.py db setup-mcp</code>" in page and 'name="management_password"' in page
    page = html.unescape(client.get(pending_path(post(client, "/setup/mcp", {**data, "postgres[write_access]": "yes"}))).text)
    assert f"<strong>Warning</strong>: {WRITE_WARNING}" in page

    queue = {"form_token": data["form_token"], "port": "8050", "result_root": "results", "max_concurrent": "2", "history_retention": "10", "add_postgres": "yes", "postgres_target[name]": "pg", "postgres_target[database]": "d", "postgres_target[timeout_hours]": "1", "add_mongo": "yes", "mongo_target[name]": "mg", "mongo_target[timeout_hours]": "0", "auth_enabled": "yes"}
    page = html.unescape(client.get(pending_path(post(client, "/setup/queue", queue))).text)
    assert JOBS_SENTENCE in page and "<code>python sdp.py db setup-jobs</code>" in page


# ── the restart ─────────────────────────────────────────────────────────────


def test_needs_attention_names_the_services_and_restarts_each_database_once(client, broker):
    """Bug class: settings written and nothing said, so the running services keep the old ones; or a restart that stops a database's MCP server on its own right after the database's own stop took it along."""
    broker.answers["db.restart_needed"] = restart_needed(
        {"service": "postgres", "started": "x", "files": [{"file": "docker-compose.override.yml", "modified": "y"}]},
        {"service": "postgres-mcp", "started": "x", "files": [{"file": ".env", "modified": "y"}]},
        {"service": "jobs", "started": "x", "files": [{"file": "config/jobs/config.local.yaml", "modified": "y"}]},
    )
    page = " ".join(client.get("/").text.split())
    assert "<b>Restart to apply:</b> the settings of postgres (docker-compose.override.yml), postgres-mcp (.env), jobs (config/jobs/config.local.yaml) changed after it started. Nothing restarts by itself." in page
    assert 'href="/restart?services=postgres,jobs"' in page
    broker.answers["db.restart_needed"] = restart_needed()
    assert "Restart to apply" not in client.get("/").text


def _restart(client, services="postgres,jobs", admin=""):
    token = form_token(client)
    page = client.get(f"/restart?services={services}").text
    assert 'name="admin_password"' in page
    response = post(client, "/restart", {"form_token": token, "services": services, "admin_password": admin})
    return token, pending_path(response)


def test_the_restart_stops_and_starts_each_service_after_the_management_password_was_checked(app, client, broker):
    """Bug class: a restart that stops a service before the management password was checked (a wrong one would leave it stopped), sends the steps in another order, or loses the database admin password `db start` needs."""
    token, path = _restart(client, admin="made-up-admin-value")
    page = html.unescape(client.get(path).text)
    assert "<code>python sdp.py db stop postgres</code>" in page and "<code>python sdp.py db start postgres</code>" in page and "<code>python sdp.py db stop jobs</code>" in page
    assert 'name="management_password"' in page and "Restart to apply</h2>" in page
    broker.requests.clear()
    confirm(client, path, token, PASSWORD)
    wait_until(lambda: len(changing_requests(broker)) == 5)
    sent = [(request["verb"], request["fields"].get("service"), request["step_up"]) for request in broker.requests if request["verb"] in ("session.check_password", "db.stop", "db.start")]
    assert sent == [("session.check_password", None, True), ("db.stop", "postgres", None), ("db.start", "postgres", True), ("db.stop", "jobs", None), ("db.start", "jobs", True)]
    assert all(request["fields"].get("admin_password") == "made-up-admin-value" for request in broker.requests if request["verb"] == "db.start")


def test_a_failed_step_stops_the_rest_and_a_wrong_password_stops_everything(app, client, broker):
    """Bug class: a restart that goes on after a stop failed (starting what did not stop, or stopping the next service), or that stops services when the management password was wrong."""
    token, path = _restart(client)
    broker.requests.clear()
    response = confirm(client, path, token, "made-up-wrong-pw")
    assert response.status_code == 403
    assert changing_requests(broker) == ["session.check_password"]

    broker.requests.clear()
    broker.answers["db.stop"] = {"exit_code": 1, "output": "", "errors": "made-up failure\n"}
    confirm(client, path, token, PASSWORD)
    wait_until(lambda: "db.stop" in changing_requests(broker))
    assert changing_requests(broker) == ["session.check_password", "db.stop"]


@pytest.mark.parametrize("services", ["", "postgres,postgres", "postgres,../x", "nginx", ",".join(["postgres"] * 9)])
def test_a_restart_of_anything_but_the_services_is_refused(client, services):
    """Bug class: a restart page or a pending restart made from a list the web UI does not know (a service twice, a name of another kind), which would send the broker a verb with fields of the author's choosing."""
    token = form_token(client)
    assert client.get(f"/restart?services={services}").status_code == 400
    assert post(client, "/restart", {"form_token": token, "services": services}).status_code == 400
    assert llm_setup.restart_services(services) is None


def test_the_settings_answer_of_another_shape_is_said(client, broker):
    """Bug class: a form drawn from an answer of another shape (a port that is a text, a database the web UI does not know), which would post values nobody chose."""
    answer = llm_settings()
    answer["data"]["mcp"]["servers"]["postgres"]["port"] = "8000"
    broker.answers["db.llm_settings"] = answer
    page = client.get("/setup/mcp").text
    assert "did not have the expected shape" in page and 'action="/setup/mcp"' not in page
    broker.answers["db.llm_settings"] = broker_client.BrokerError("timeout", "x")
    assert 'action="/setup/queue"' not in client.get("/setup/queue").text


def test_the_restart_services_are_the_tables():
    """Bug class: the Overview's copy of the services drifting from the table's, so a restart line names a service the restart then refuses."""
    from social_data_pipeline.webui import overview
    assert overview.RESTART_SERVICES == actions.SERVICES
