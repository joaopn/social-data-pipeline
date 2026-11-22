"""Tests for the web UI's table of actions (`webui.actions`) and for the way an action travels: chosen in a form, kept as a pending action, confirmed, run in a thread of its own, shown as an outcome.

The contracts pinned here, each test naming the bug class it catches:

- a and b (the table against the registry, the command shown against the CLI's parser) are in `test_webui_table.py`, which needs no FastAPI.
- c. Per action: the forms' choices and inputs; a refused input or an unknown field reaches no broker and makes no pending action; the pending page shows the command, the inputs, secrets as dots and what is destroyed; the exact request the broker gets after the confirmation, and nothing before it.
- d. Pending actions: two confirms at once start one call; an expired one and another session's are not found; at most sixteen; a GET changes nothing; a pending secret in no page, no repr, no log and no exception.
- e. Step-up: without and with a wrong management password the action is kept and the page says so; the right one runs it; `session_ended` ends the session; the management password is stored nowhere.
- f. Outcomes: an early refusal on the pending page, a slow call under way and then its result, exit codes, every refusal code worded, a long call not cut by the read time limit, an unreachable broker as an outcome, another session's outcome not found, the text bounds, the bound of 32, the bound on running actions.

Every special character in this file is written as an escape.
"""

from __future__ import annotations

import gc
import html
import re
import threading
import time

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette.testclient")

from webui_helpers import (
    DONE,
    PASSWORD,
    RUN_ID,
    TOKEN,
    choose,
    confirm,
    db_status,
    form_token,
    log_in,
    pending_path,
    source_status,
    state_changing_requests,
    wait_until,
)

from social_data_pipeline.control import client as broker_client
from social_data_pipeline.webui import actions, words, work

SECRET = "made-up-db-admin-pw-77e1"


@pytest.fixture
def app(make_app):
    return make_app(action_wait_seconds=0.5)


@pytest.fixture
def client(app, make_client):
    made = make_client(app)
    assert log_in(made).status_code == 303
    return made


@pytest.fixture
def token(client):
    return form_token(client)


def the_session(app, client):
    """The web UI's session of a client (the guard holds the session table)."""
    cookie = client.cookies.get("__Host-sdp_session")
    session = app.sessions.get(cookie)
    assert session is not None
    return session


def main_text(page):
    """The text of the page's main part, markup removed and entities read."""
    found = re.search(r'<main class="main">(.*)</main>', page, re.DOTALL)
    return html.unescape(re.sub(r"<[^>]+>", " ", found.group(1) if found else page))


# ── c. per action ───────────────────────────────────────────────────────────


def forms_of(page):
    """Each form of a page that posts to /pending: {field name: value} of its inputs (hidden ones with their value, others with the empty text), selects with their options."""
    found = []
    for body in re.findall(r'<form method="post" action="/pending" class="action-form[^"]*">(.*?)</form>', page, re.DOTALL):
        fields = {}
        for tag in re.findall(r"<input [^>]*>", body):
            name = re.search(r'name="([^"]*)"', tag).group(1)
            value = re.search(r'value="([^"]*)"', tag)
            fields[name] = html.unescape(value.group(1)) if value else ""
        for name, options in re.findall(r'<select name="([^"]*)">(.*?)</select>', body, re.DOTALL):
            fields[name] = [html.unescape(value) for value in re.findall(r'<option value="([^"]*)"', options)]
        found.append(fields)
    return found


def test_the_databases_page_offers_the_registrys_choices(client, broker):
    """Bug class: a start or stop form offering a service the broker refuses (or missing one), a reset or removal form for a database that is not configured, or one without its database."""
    broker.answers["db.status"] = db_status()
    forms = forms_of(client.get("/databases").text)
    by_action = {}
    for fields in forms:
        by_action.setdefault(fields["action"], []).append(fields)
    # One start and one stop form with every service to choose from; the cards' buttons carry one service each, always one the registry takes.
    choosing = {name: [fields for fields in by_action[name] if isinstance(fields["service"], list)] for name in ("db.start", "db.stop")}
    assert [fields["service"] for fields in choosing["db.start"]] == [["", *actions.SERVICES]]
    assert [fields["service"] for fields in choosing["db.stop"]] == [["", *actions.SERVICES]]
    assert set(choosing["db.start"][0]) == {"form_token", "action", "service", "admin_password"}
    assert [fields["service"] for fields in by_action["db.start"] if not isinstance(fields["service"], list)] == ["postgres", "mongo"]
    assert [fields["service"] for fields in by_action["db.stop"] if not isinstance(fields["service"], list)] == ["postgres", "mongo"]
    assert [fields["db"] for fields in by_action["db.reset"]] == ["postgres", "mongo"]
    assert [fields["db"] for fields in by_action["db.unsetup"]] == ["postgres", "mongo"]
    assert set(by_action) == {"db.start", "db.stop", "db.reset", "db.unsetup", "db.unsetup_mcp", "db.unsetup_jobs", "session.set_db_password", "session.clear_db_password"}
    assert set(by_action["session.set_db_password"][0]) == {"form_token", "action", "password"}


EXPECTED_PROFILE_INPUTS = {
    "parse": {"build", "filter", "skip_lingua_files"},
    "lingua": {"build", "filter", "classifier"},
    "ml": {"build", "filter", "classifier"},
    "postgres_ingest": {"build", "filter", "admin_password"},
    "postgres_ml": {"build", "filter", "admin_password"},
    "mongo_ingest": {"build", "filter", "admin_password"},
    "sr_ingest": {"build", "admin_password"},
    "sr_ml": {"build", "classifier", "admin_password"},
}


def test_each_profile_has_one_form_with_exactly_its_inputs(client, broker):
    """Bug class: a form offering an input the profile refuses (a filter for StarRocks, which is then dropped without a word; a classifier for parse), missing one it takes, or a form for a profile the source does not have."""
    broker.answers["source.status"] = lambda fields: source_status(fields["name"], profiles=list(reversed(actions.VALID_PROFILES)) + ["made_up_profile"])
    forms = [fields for fields in forms_of(client.get("/sources/demo").text) if fields["action"] == "run.start"]
    assert [fields["profile"] for fields in forms] == list(actions.VALID_PROFILES)
    for fields in forms:
        assert fields["source"] == "demo"
        assert set(fields) - {"form_token", "action", "profile", "source"} == EXPECTED_PROFILE_INPUTS[fields["profile"]], fields["profile"]
    broker.answers["source.status"] = lambda fields: source_status(fields["name"], profiles=["lingua"])
    assert [fields["profile"] for fields in forms_of(client.get("/sources/demo").text) if fields["action"] == "run.start"] == ["lingua"]


REFUSED_FORMS = [
    ("run.start", {"profile": "nope", "source": "demo"}),
    ("run.start", {"profile": "parse"}),
    ("run.start", {"profile": "parse", "source": "../etc"}),
    ("run.start", {"profile": "parse", "source": ".."}),
    ("run.start", {"profile": "parse", "source": "demo", "filter": "..."}),
    ("run.start", {"profile": "parse", "source": "demo", "filter": "a/b"}),
    ("run.start", {"profile": "parse", "source": "demo", "filter": "x" * 129}),
    ("run.start", {"profile": "ml", "source": "demo", "classifier": "-x"}),
    ("run.start", {"profile": "ml", "source": "demo", "classifier": "a b"}),
    ("run.start", {"profile": "sr_ml", "source": "demo", "filter": "RS_*"}),
    ("run.start", {"profile": "ml", "source": "demo", "skip_lingua_files": "yes"}),
    ("run.start", {"profile": "parse", "source": "demo", "classifier": "x"}),
    ("run.start", {"profile": "parse", "source": "demo", "build": "true"}),
    ("run.start", {"profile": "parse", "source": "demo", "admin_password": "a\nb"}),
    ("run.start", {"profile": "parse", "source": "demo", "admin_password": "a\u2028b"}),
    ("run.start", {"profile": "parse", "source": "demo", "admin_password": "x" * 1025}),
    ("run.start", {"profile": "parse", "source": "demo", "made_up": "1"}),
    ("db.stop", {"service": "all"}),
    ("db.reset", {}),
    ("db.reset", {"db": "postgres", "confirm": "yes"}),
    ("run.cancel", {"run_id": "r1"}),
    ("source.remove", {"name": "a/b"}),
    ("session.set_db_password", {}),
    ("source.download", {"name": "demo"}),
    ("db.status", {}),
    ("hello", {}),
    ("made.up", {}),
]


@pytest.mark.parametrize("name, fields", REFUSED_FORMS)
def test_a_refused_input_or_an_unknown_field_reaches_no_broker(client, broker, app, token, name, fields):
    """Bug class: a value the table refuses, an unknown field, a broken cross-field rule or an action outside the table kept as a pending action (and so confirmable), or sent to the broker; or the value echoed into the page."""
    broker.requests.clear()
    response = choose(client, name, token, **fields)
    assert response.status_code == 400
    assert "Nothing was sent to the broker" in response.text
    assert the_session(app, client).work.pending_count() == 0
    assert broker.requests == []
    for value in fields.values():
        if len(value) > 5:
            assert value not in response.text


def test_the_pending_page_shows_the_command_the_inputs_and_what_is_destroyed(client, broker, token):
    """Bug class: a confirmation that does not say what will run (the command), with what, or what it destroys; a secret shown on it."""
    page = client.get(pending_path(choose(client, "run.start", token, profile="ml", source="demo", classifier="toxic", build="yes", admin_password=SECRET))).text
    shown = main_text(page)
    assert "python sdp.py run ml --source=demo --build --classifier=toxic" in shown
    assert "Only this classifier toxic" in " ".join(shown.split())
    assert actions.SECRET_DOTS in shown and SECRET not in page
    assert 'name="management_password"' not in page
    page = client.get(pending_path(choose(client, "db.reset", token, db="mongo"))).text
    shown = " ".join(main_text(page).split())
    assert "python sdp.py db reset --db=mongo" in shown
    assert "mongo : Stops MongoDB and its MCP server, then deletes all of its data" in shown
    assert '<input type="password" name="management_password" autocomplete="off" required>' in page
    assert "A terminal would also ask for the secrets the command above does not show." in shown
    page = client.get(pending_path(choose(client, "source.remove", token, name="demo"))).text
    assert "Removes the source&#39;s configuration folder" in page and "python sdp.py source remove demo" in page


RESET_SENTENCES = {
    "postgres": "Stops PostgreSQL and its MCP server, then deletes all of its data: the server's folder (pgdata) and the ingestion state (state_tracking) in its data folder, and the contents of every PostgreSQL tablespace folder, which can sit on other disks. Its configuration, its .env entries, the MCP settings and the per-source settings stay. " + "Before it deletes anything, it gives your user account ownership of everything in the folder that holds its data folder, recursively (by default data/database/, which also holds the other databases' data folders), and of the folder that holds each tablespace folder. Other databases keep their data and settings, but one that is running at that moment with its files in that folder may fail.",
    "mongo": "Stops MongoDB and its MCP server, then deletes all of its data: the server's folder (db), the ingestion state (state_tracking) and its logs (logs) in its data folder. Its configuration, its .env entries, the MCP settings and the per-source settings stay. " + "Before it deletes anything, it gives your user account ownership of everything in the folder that holds its data folder, recursively (by default data/database/, which also holds the other databases' data folders). Other databases keep their data and settings, but one that is running at that moment with its files in that folder may fail.",
    "starrocks": "Stops StarRocks and its MCP server, then deletes all of its data: the server's folders (fe, be) and the ingestion state (state_tracking) in its data folder, and the contents of every StarRocks storage path, which can sit on other disks. Its configuration, its .env entries, the MCP settings and the per-source settings stay. " + "Before it deletes anything, it gives your user account ownership of everything in the folder that holds its data folder, recursively (by default data/database/, which also holds the other databases' data folders), and of the folder that holds each storage path. Other databases keep their data and settings, but one that is running at that moment with its files in that folder may fail.",
}


@pytest.mark.parametrize("db", sorted(RESET_SENTENCES))
def test_the_reset_sentence_is_pinned_whole(client, token, db):
    """Bug class: a reset sentence that drifts from what the CLI does, a clause lost (that the other databases' files change owner when they share the parent folder, so that a running one may fail)."""
    assert actions.TABLE["db.reset"].destroys({"db": db}) == (RESET_SENTENCES[db], db)
    shown = " ".join(main_text(client.get(pending_path(choose(client, "db.reset", token, db=db))).text).split())
    assert RESET_SENTENCES[db] in shown


@pytest.mark.parametrize("db, says", [
    ("postgres", ["Stops PostgreSQL and its MCP server", "pgdata", "state_tracking", "every PostgreSQL tablespace folder, which can sit on other disks"]),
    ("mongo", ["Stops MongoDB and its MCP server", "(db)", "state_tracking", "its logs"]),
    ("starrocks", ["Stops StarRocks and its MCP server", "(fe, be)", "state_tracking", "every StarRocks storage path, which can sit on other disks"]),
])
def test_the_reset_says_all_it_destroys_and_that_it_stops_the_database(client, token, db, says):
    """Bug class: a reset confirmed on a sentence that leaves out what it deletes outside the data folder (tablespaces and storage paths on other disks) or that it stops the database and its MCP server first."""
    shown = " ".join(main_text(client.get(pending_path(choose(client, "db.reset", token, db=db))).text).split())
    for text in says:
        assert text in shown, text
    assert "The broker refuses it while a pipeline run is alive" in shown


def test_removing_the_mcp_configuration_says_it_stops_their_databases(client, token):
    """Bug class: the removal of the MCP servers' configuration confirmed as if it stopped only the MCP servers, while the CLI stops each database that has one together with it."""
    shown = " ".join(main_text(client.get(pending_path(choose(client, "db.unsetup_mcp", token))).text).split())
    assert "Stops each database that has an MCP server configured, together with that server" in shown
    assert "The broker refuses it while a pipeline run is alive" in shown


UNSETUP_SENTENCES = {
    "postgres": "Stops PostgreSQL and its MCP server and removes their containers, then deletes: the whole data folder of PostgreSQL (PGDATA_PATH), its read-only user's credentials file (.ro_credentials) included; every PostgreSQL tablespace folder itself, which can sit on other disks; its configuration files config/db/postgres.yaml, config/postgres/postgresql.local.conf and config/postgres/pg_hba.local.conf, each with its .bak copy; the per-source settings files postgres.yaml and postgres_ml.yaml in every folder under config/sources/ (which a reset keeps); its keys in .env, those of its MCP server included; its block of docker-compose.override.yml; and its part of config/db/mcp.yaml (each of these two files is removed when nothing else is left in it). The scheduler's targets that point at PostgreSQL are left as they are and fail until the scheduler is set up again. Before it deletes anything, it gives your user account ownership of everything in the folder that holds its data folder, recursively (by default data/database/, which also holds the other databases' data folders), and of the folder that holds each tablespace folder. Other databases keep their data and settings, but one that is running at that moment with its files in that folder may fail.",
    "mongo": "Stops MongoDB and its MCP server and removes their containers, then deletes: the whole data folder of MongoDB (MONGO_DATA_PATH), its read-only user's credentials file (.ro_credentials) included; its configuration files config/db/mongo.yaml, each with its .bak copy; the per-source settings files mongo.yaml in every folder under config/sources/ (which a reset keeps); its keys in .env, those of its MCP server included; its block of docker-compose.override.yml; and its part of config/db/mcp.yaml (each of these two files is removed when nothing else is left in it). The scheduler's targets that point at MongoDB are left as they are and fail until the scheduler is set up again. Before it deletes anything, it gives your user account ownership of everything in the folder that holds its data folder, recursively (by default data/database/, which also holds the other databases' data folders). Other databases keep their data and settings, but one that is running at that moment with its files in that folder may fail.",
    "starrocks": "Stops StarRocks and its MCP server and removes their containers, then deletes: the whole data folder of StarRocks (STARROCKS_DATA_PATH), its read-only user's credentials file (.ro_credentials) included; every StarRocks storage-path folder itself, which can sit on other disks; its configuration files config/db/starrocks.yaml, config/starrocks/fe.local.conf and config/starrocks/be.local.conf, each with its .bak copy; the per-source settings files starrocks.yaml and sr_ml.yaml in every folder under config/sources/ (which a reset keeps); its keys in .env, those of its MCP server included; its block of docker-compose.override.yml; and its part of config/db/mcp.yaml (each of these two files is removed when nothing else is left in it). The scheduler's targets that point at StarRocks are left as they are and fail until the scheduler is set up again. Before it deletes anything, it gives your user account ownership of everything in the folder that holds its data folder, recursively (by default data/database/, which also holds the other databases' data folders), and of the folder that holds each storage path. Other databases keep their data and settings, but one that is running at that moment with its files in that folder may fail.",
}


@pytest.mark.parametrize("db", sorted(UNSETUP_SENTENCES))
def test_removing_a_database_says_all_it_deletes(client, token, db):
    """Bug class: the removal of a database confirmed on a sentence that leaves out what it deletes beyond its data (the tablespace and storage-path folders on other disks, the read-only user's credentials, the per-source settings a reset would keep, its MCP server's settings and its compose block), or that says the other databases are not affected while their files change owner."""
    assert actions.TABLE["db.unsetup"].destroys({"db": db}) == (UNSETUP_SENTENCES[db], db)
    shown = " ".join(main_text(client.get(pending_path(choose(client, "db.unsetup", token, db=db))).text).split())
    assert html.unescape(UNSETUP_SENTENCES[db]) in shown


def test_removing_the_scheduler_says_it_removes_its_export_mounts(client, token):
    """Bug class: the removal of the scheduler's configuration confirmed on a sentence that says less or other than it does: PostgreSQL loses its export mount at its next start, StarRocks keeps one (the compose file's own, at the default folder), the job store and the results stay."""
    sentence = "Stops the query scheduler and removes its configuration: config/jobs/config.local.yaml and its .env entries (JOBS_PORT, JOBS_RESULT_ROOT). It also removes the scheduler's export mount (/jobs_export) from docker-compose.override.yml, so PostgreSQL no longer has one from its next start; StarRocks keeps one, because docker-compose.yml mounts ${JOBS_RESULT_ROOT:-./data/jobs/results} at /jobs_export, which without JOBS_RESULT_ROOT in .env is ./data/jobs/results from its next start. The job store (./data/jobs) and the results on disk are kept."
    assert actions.TABLE["db.unsetup_jobs"].destroys({}) == (sentence, "the scheduler's configuration")
    shown = " ".join(main_text(client.get(pending_path(choose(client, "db.unsetup_jobs", token))).text).split())
    assert sentence in shown


ALSO = {
    ("db.stop", ""): "It stops every configured database, the MCP server of each that has one configured and the scheduler when it is configured, and removes their containers.",
    ("db.stop", "postgres"): "It also stops the MCP server of PostgreSQL when one is configured, and removes the containers it stops.",
    ("db.stop", "mongo"): "It also stops the MCP server of MongoDB when one is configured, and removes the containers it stops.",
    ("db.stop", "starrocks"): "It also stops the MCP server of StarRocks when one is configured, and removes the containers it stops.",
    ("db.stop", "postgres-mcp"): None,
    ("db.stop", "mongo-mcp"): None,
    ("db.stop", "starrocks-mcp"): None,
    ("db.stop", "jobs"): None,
    ("db.start", ""): "It starts every configured database, then the MCP server of each that has one configured, and rebuilds and starts the scheduler when one of its targets is among those databases. Before the start it rewrites the per-source data mounts of PostgreSQL and StarRocks in docker-compose.override.yml when a source is configured.",
    ("db.start", "postgres"): "It also starts the MCP server of PostgreSQL when one is configured, and rebuilds and starts the scheduler when one of its targets is PostgreSQL. Before the start, when a source is configured, it rewrites docker-compose.override.yml: it writes the per-source data mounts of PostgreSQL and removes those of StarRocks, which StarRocks then lacks until it is next started with `db start` of StarRocks or of every service.",
    ("db.start", "mongo"): "It also starts the MCP server of MongoDB when one is configured, and rebuilds and starts the scheduler when one of its targets is MongoDB.",
    ("db.start", "starrocks"): "It also starts the MCP server of StarRocks when one is configured, and rebuilds and starts the scheduler when one of its targets is StarRocks. Before the start, when a source is configured, it rewrites docker-compose.override.yml: it writes the per-source data mounts of StarRocks and removes those of PostgreSQL, which PostgreSQL then lacks until it is next started with `db start` of PostgreSQL or of every service.",
    ("db.start", "postgres-mcp"): "It also starts PostgreSQL itself, with its MCP server.",
    ("db.start", "mongo-mcp"): "It also starts MongoDB itself, with its MCP server.",
    ("db.start", "starrocks-mcp"): "It also starts StarRocks itself, with its MCP server.",
    ("db.start", "jobs"): "It rebuilds the scheduler's image before it starts the scheduler.",
}


def test_every_service_value_is_covered():
    """Bug class: a service value added to the registry without a line saying what else it acts on (or saying nothing on purpose)."""
    assert {service for verb, service in ALSO} == {"", *actions.SERVICES} and {verb for verb, service in ALSO} == {"db.stop", "db.start"}


@pytest.mark.parametrize("name, service", sorted(ALSO))
def test_the_pending_page_says_what_else_a_service_choice_acts_on(client, token, name, service):
    """Bug class: a stop of one database confirmed without saying it also stops its MCP server, a start of one without saying it also starts its MCP server and rebuilds the scheduler (and, for PostgreSQL and StarRocks, drops the other one's per-source mounts), a start of an MCP server without saying it starts its database, a start of the scheduler without saying it rebuilds its image; or such a line for a choice that touches nothing else."""
    fields = {} if service == "" else {"service": service}
    assert actions.TABLE[name].also(fields) == ALSO[(name, service)]
    page = client.get(pending_path(choose(client, name, token, service=service))).text
    lines = re.findall(r'<p class="notice">(It [^<]*)</p>', page)
    assert lines == ([html.escape(ALSO[(name, service)], quote=True).replace("&#x27;", "&#39;")] if ALSO[(name, service)] else [])


@pytest.mark.parametrize("name", ["db.stop", "db.start"])
def test_a_start_or_stop_without_a_service_says_every_configured_service(client, token, name):
    """Bug class: a start or stop of every service confirmed on a page that shows an empty input list and a command without a service, so that "all" is never said."""
    shown = " ".join(main_text(client.get(pending_path(choose(client, name, token, service=""))).text).split())
    assert "Service every configured service" in shown
    assert ("The broker refuses it while a pipeline run is alive" in shown) == (name == "db.stop")
    shown = " ".join(main_text(client.get(pending_path(choose(client, name, token, service="mongo"))).text).split())
    assert "Service mongo" in shown and "every configured service" not in shown


CONFIRMED = [
    ("run.start", {"profile": "parse", "source": "demo", "filter": "-RS_*", "build": "yes", "skip_lingua_files": "yes"}, None, {"profile": "parse", "source": "demo", "filter": "-RS_*", "build": True, "skip_lingua_files": True}),
    ("run.start", {"profile": "mongo_ingest", "source": "demo", "admin_password": SECRET, "filter": ""}, None, {"profile": "mongo_ingest", "source": "demo", "admin_password": SECRET}),
    ("db.stop", {"service": ""}, None, {}),
    ("db.stop", {"service": "mongo"}, None, {"service": "mongo"}),
    ("run.cancel", {"run_id": RUN_ID}, None, {"run_id": RUN_ID}),
    ("db.start", {"service": "postgres", "admin_password": ""}, PASSWORD, {"service": "postgres"}),
    ("db.start", {"admin_password": SECRET}, PASSWORD, {"admin_password": SECRET}),
    ("db.reset", {"db": "postgres"}, PASSWORD, {"db": "postgres"}),
    ("db.unsetup", {"db": "starrocks"}, PASSWORD, {"db": "starrocks"}),
    ("db.unsetup_mcp", {}, PASSWORD, {}),
    ("db.unsetup_jobs", {}, PASSWORD, {}),
    ("source.remove", {"name": "demo"}, PASSWORD, {"name": "demo"}),
    ("session.set_db_password", {"password": SECRET}, PASSWORD, {"password": SECRET}),
    ("session.clear_db_password", {}, None, {}),
]


@pytest.mark.parametrize("name, form, password, sent", CONFIRMED)
def test_the_broker_gets_exactly_the_request_after_the_confirmation_and_nothing_before(client, broker, token, name, form, password, sent):
    """Bug class: an action sent when it is chosen or when its page is opened (before the operator confirmed), sent with other fields than chosen (an empty field sent as an empty text, a box sent as text), without the management password a step-up verb needs, or with it to a verb that does not take it."""
    broker.requests.clear()
    path = pending_path(choose(client, name, token, **form))
    assert client.get(path).status_code == 200
    assert broker.requests == []
    response = confirm(client, path, token, password)
    assert response.status_code == 303 and response.headers["location"].startswith("/actions/")
    assert broker.requests == [{"verb": name, "fields": sent, "step_up": True if password else None}]
    assert client.get(path).status_code == 404


# ── d. pending actions ──────────────────────────────────────────────────────


def test_two_confirms_at_once_start_one_call(make_app, make_client, broker):
    """Bug class: a pending action taken out only after something was awaited, so that two confirms (a double click, two tabs) both find it and the action runs twice."""
    from starlette.testclient import TestClient

    app = make_app(action_wait_seconds=0.05)
    first = make_client(app)
    log_in(first)
    token = form_token(first)
    path = pending_path(choose(first, "db.stop", token, service="postgres"))
    broker.gate = threading.Event()
    broker.gate_verbs = {"db.stop"}
    results = []
    start = threading.Barrier(2)
    with TestClient(app, base_url="https://localhost:8443", headers={"host": "localhost:8443"}, client=("172.30.0.1", 50000), follow_redirects=False, cookies={"__Host-sdp_session": first.cookies.get("__Host-sdp_session")}) as shared:

        def send():
            start.wait(5)
            results.append(confirm(shared, path, token).status_code)

        threads = [threading.Thread(target=send) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)
        broker.gate.set()
    assert sorted(results) == [303, 404]
    wait_until(lambda: len([request for request in broker.requests if request["verb"] == "db.stop"]) == 1)
    assert [request["verb"] for request in state_changing_requests(broker)] == ["db.stop"]


def test_an_expired_pending_action_and_another_sessions_are_not_found(client, broker, token, clock, app, make_client):
    """Bug class: a pending action that never expires (a page left open confirms an old choice hours later), or one another session can see or confirm by its id."""
    path = pending_path(choose(client, "db.stop", token))
    other = make_client(app, address="192.0.2.50")
    log_in(other)
    other_token = form_token(other)
    assert other.get(path).status_code == 404
    assert confirm(other, path, other_token).status_code == 404
    clock.now += 299
    assert client.get(path).status_code == 200
    clock.now += 1
    assert client.get(path).status_code == 404
    assert confirm(client, path, token).status_code == 404
    assert state_changing_requests(broker) == []


def test_a_session_keeps_at_most_sixteen_pending_actions(client, token, app, clock):
    """Bug class: pending actions without bound (a loop of choices fills the memory), or the newest dropped instead of the oldest."""
    paths = []
    for _ in range(17):
        paths.append(pending_path(choose(client, "db.stop", token)))
        clock.now += 1
    assert the_session(app, client).work.pending_count() == 16
    assert client.get(paths[0]).status_code == 404
    assert all(client.get(path).status_code == 200 for path in paths[1:])


def test_a_get_of_the_pending_page_changes_nothing(client, broker, token, clock, app):
    """Bug class: opening the pending page (a reload, a prefetch) taking the action out, running it, or keeping it alive longer."""
    path = pending_path(choose(client, "db.stop", token))
    broker.requests.clear()
    for _ in range(3):
        clock.now += 99
        assert client.get(path).status_code == 200
        assert client.head(path).status_code == 405
        assert the_session(app, client).work.pending_count() == 1
    clock.now += 3
    assert client.get(path).status_code == 404
    assert broker.requests == []


def test_a_pending_secret_is_in_no_page_no_repr_no_log_and_no_exception(client, broker, token, app, caplog):
    """Bug class: the database password of a pending action shown on its page, in the repr of the session or the action (which a log line or a traceback prints), in a log record, or in the text of an exception raised while the action is run."""
    caplog.set_level("DEBUG")
    path = pending_path(choose(client, "run.start", token, profile="postgres_ingest", source="demo", admin_password=SECRET))
    page = client.get(path).text
    session = the_session(app, client)
    pending = session.work.pending(path.rsplit("/", 1)[1], 1000.0)
    assert SECRET not in page
    assert SECRET not in repr(session) and SECRET not in repr(session.work) and SECRET not in repr(pending) and SECRET not in repr(pending.fields) and SECRET not in str(pending.fields) and SECRET not in f"{pending.fields['admin_password']}"
    try:
        raise RuntimeError(f"made-up failure with {pending!r} {pending.fields} {session}")
    except RuntimeError as error:
        assert SECRET not in str(error)

    def fail(fields):
        raise RuntimeError(f"made-up failure with {fields}")

    broker.answers["run.start"] = fail
    response = confirm(client, path, token)
    assert response.status_code == 200 and words.CODE_SENTENCES["internal"].replace("'", "&#39;") in response.text
    assert SECRET not in response.text
    assert [request["fields"].get("admin_password") for request in broker.requests if request["verb"] == "run.start"] == [SECRET]
    for record in caplog.records:
        assert SECRET not in record.getMessage() + repr(record.args)
    assert session.work.pending_count() == 0


# ── e. step-up ──────────────────────────────────────────────────────────────


def test_without_and_with_a_wrong_password_the_action_is_kept_and_the_right_one_runs_it(client, broker, token, app):
    """Bug class: a step-up action sent without the management password, a wrong password dropping the action (the operator must choose it again) or not said, or a right one not running it."""
    path = pending_path(choose(client, "db.reset", token, db="postgres"))
    broker.requests.clear()
    response = confirm(client, path, token)
    assert response.status_code == 400 and "Type the management password" in response.text
    assert broker.requests == []
    response = confirm(client, path, token, "made-up-wrong-management-pw")
    assert response.status_code == 403 and words.CODE_SENTENCES["step_up_failed"] in response.text
    assert 'name="management_password"' in response.text
    assert client.get(path).status_code == 200
    response = confirm(client, path, token, PASSWORD)
    assert response.status_code == 303
    assert [request["step_up"] for request in broker.requests] == [False, True]
    assert client.get(path).status_code == 404


def test_step_up_required_keeps_the_action_and_a_write_takes_no_password(client, broker, token):
    """Bug class: the broker's `step_up_required` dropping the action; a management password sent with a write verb (the confirmation of a write carries nothing)."""
    broker.answers["db.reset"] = broker_client.BrokerError("step_up_required", "x")
    path = pending_path(choose(client, "db.reset", token, db="mongo"))
    response = confirm(client, path, token, PASSWORD)
    assert response.status_code == 403 and words.CODE_SENTENCES["step_up_required"] in response.text
    assert client.get(path).status_code == 200
    path = pending_path(choose(client, "db.stop", token))
    broker.requests.clear()
    assert confirm(client, path, token, PASSWORD).status_code == 400
    assert confirm(client, path, token, extra_field="x").status_code == 400
    assert broker.requests == [] and client.get(path).status_code == 200


@pytest.mark.parametrize("code", ["session_ended", "unauthorized"])
def test_a_session_the_broker_ends_at_the_confirmation_leads_to_the_login(client, broker, token, code):
    """Bug class: a session that the broker ended (too many wrong management passwords, a token it no longer knows) kept alive in the web UI."""
    broker.answers["db.reset"] = broker_client.BrokerError(code, "x")
    path = pending_path(choose(client, "db.reset", token, db="mongo"))
    response = confirm(client, path, token, PASSWORD)
    assert response.status_code == 303 and response.headers["location"] == "/login?why=ended"
    assert client.get("/").status_code == 303


def strings_reachable_from(root):
    """Every text reachable from `root` through attributes, slots, mappings and sequences (a closure's cells included)."""
    seen, found, stack = set(), [], [root]
    while stack:
        item = stack.pop()
        if id(item) in seen:
            continue
        seen.add(id(item))
        if isinstance(item, str):
            found.append(item)
            continue
        if isinstance(item, (bytes, bytearray)):
            found.append(bytes(item).decode("utf-8", "replace"))
            continue
        if isinstance(item, dict):
            stack.extend(item.keys())
            stack.extend(item.values())
        elif isinstance(item, (list, tuple, set, frozenset)):
            stack.extend(item)
        if hasattr(item, "__dict__") and not isinstance(item, type):
            stack.extend(vars(item).values())
        for slot in getattr(type(item), "__slots__", ()):
            if hasattr(item, slot):
                stack.append(getattr(item, slot))
        if hasattr(item, "__closure__") and item.__closure__:
            stack.extend(cell.cell_contents for cell in item.__closure__ if cell.cell_contents is not None)
    return found


def test_the_management_password_is_stored_nowhere(make_app, make_client, broker):
    """Bug class: the management password given again kept in the session, in the outcome or in the pending action while the call runs (a taken-over process would read it from there for as long as the session lives)."""
    app = make_app(action_wait_seconds=0.05)
    client = make_client(app)
    log_in(client)
    token = form_token(client)
    path = pending_path(choose(client, "db.reset", token, db="postgres"))
    broker.gate = threading.Event()
    broker.gate_verbs = {"db.reset"}
    response = confirm(client, path, token, PASSWORD)
    assert response.status_code == 303
    session = the_session(app, client)
    outcome = session.work.outcome(response.headers["location"].rsplit("/", 1)[1])
    assert outcome is not None and not outcome.done
    reachable = strings_reachable_from(app.sessions) + strings_reachable_from(session) + strings_reachable_from(outcome)
    assert TOKEN in reachable
    assert not any(PASSWORD in text for text in reachable)
    broker.gate.set()
    wait_until(lambda: outcome.done)
    gc.collect()


# ── f. outcomes ─────────────────────────────────────────────────────────────


def test_an_early_refusal_is_shown_on_the_pending_page(client, broker, token):
    """Bug class: a refusal that came at once (a run is alive, Docker is down) shown as "under way" or lost behind a redirect."""
    broker.answers["db.stop"] = broker_client.BrokerError("run_active", "x")
    path = pending_path(choose(client, "db.stop", token, service="postgres"))
    response = confirm(client, path, token)
    assert response.status_code == 200
    assert words.CODE_SENTENCES["run_active"] in response.text and "python sdp.py db stop postgres" in response.text
    assert 'action="' + path + '/confirm"' not in response.text


def test_a_refusal_within_the_wait_is_shown_on_the_pending_page(make_app, make_client, broker):
    """Bug class: a confirm that does not wait the moment it may for the broker's answer, so that a refusal the broker gives after a fraction of a second (a lock it tried, Docker asked whether a run is alive) is shown as "under way" first."""
    app = make_app(action_wait_seconds=2)
    client = make_client(app)
    log_in(client)
    token = form_token(client)
    broker.answers["db.stop"] = broker_client.BrokerError("docker_unreachable", "x")
    broker.gate = threading.Event()
    broker.gate_verbs = {"db.stop"}
    releasing = threading.Timer(0.2, broker.gate.set)
    releasing.start()
    response = confirm(client, pending_path(choose(client, "db.stop", token)), token)
    releasing.join()
    assert response.status_code == 200 and words.CODE_SENTENCES["docker_unreachable"] in response.text


def test_a_slow_call_is_under_way_and_then_shows_its_result(make_app, make_client, broker):
    """Bug class: a confirm that blocks until a long action ends, a page that never shows the end, or a poll that keeps going after it."""
    app = make_app(action_wait_seconds=0.05)
    client = make_client(app)
    log_in(client)
    token = form_token(client)
    broker.gate = threading.Event()
    broker.gate_verbs = {"db.stop"}
    broker.answers["db.stop"] = {"exit_code": 0, "output": "Stopping postgres...\nStopped.\n", "errors": ""}
    response = confirm(client, pending_path(choose(client, "db.stop", token)), token)
    assert response.status_code == 303
    outcome_path = response.headers["location"]
    page = client.get(outcome_path).text
    assert "under way" in page and f'hx-get="{outcome_path}" hx-trigger="load delay:2s" hx-swap="outerHTML"' in page
    partial = client.get(outcome_path, headers={"hx-request": "true"})
    assert partial.status_code == 200 and not partial.text.startswith("<!doctype") and "under way" in partial.text
    broker.gate.set()
    session = the_session(app, client)
    wait_until(lambda: session.work.outcome(outcome_path.rsplit("/", 1)[1]).done)
    partial = client.get(outcome_path, headers={"hx-request": "true"}).text
    assert "hx-get" not in partial and "exit code 0" in partial and "Stopped." in partial


@pytest.mark.parametrize("exit_code", [0, 1, 3])
def test_the_exit_code_output_and_errors_are_shown(client, broker, token, exit_code):
    """Bug class: a command that failed shown as done, or its output and errors left out."""
    broker.answers["db.unsetup_jobs"] = {"exit_code": exit_code, "output": "made-up output line\n", "errors": "made-up error line\n" if exit_code else ""}
    response = confirm(client, pending_path(choose(client, "db.unsetup_jobs", token)), token, PASSWORD)
    page = client.get(response.headers["location"]).text
    assert f"exit code {exit_code}." in page and "made-up output line" in page
    assert ("notice-bad" in page) == (exit_code != 0) and ("made-up error line" in page) == (exit_code != 0)


REFUSAL_CODES = sorted(set(words.CODE_SENTENCES) - {"unauthorized", "session_ended", "step_up_failed", "step_up_required", "unreachable"})


@pytest.mark.parametrize("code", REFUSAL_CODES)
def test_every_refusal_code_is_worded(client, broker, token, code):
    """Bug class: a refusal shown as the raw code, as the fallback, or with the broker's own message."""
    broker.answers["db.stop"] = broker_client.BrokerError(code, "made-up broker message", details={"exit_code": 2, "errors": "made-up errors\n"} if code == "failed" else None)
    response = confirm(client, pending_path(choose(client, "db.stop", token)), token)
    assert response.status_code == 200
    assert html.escape(words.CODE_SENTENCES[code], quote=True).replace("&#x27;", "&#39;") in response.text
    assert "made-up broker message" not in response.text
    if code == "failed":
        assert "made-up errors" in response.text and "Exit code <code>2</code>" in response.text


def test_a_call_longer_than_the_read_time_limit_is_not_cut_and_keeps_the_session(make_app, make_client, broker):
    """Bug class: a state-changing call made through the read client, whose time limit (75 seconds) ends the wait for a call that lasts many minutes: the page says it failed while it runs on, and the session is ended as if the broker were gone."""
    app = make_app(action_wait_seconds=0.05, read_seconds=0.05)
    client = make_client(app)
    log_in(client)
    token = form_token(client)
    broker.gate = threading.Event()
    broker.gate_verbs = {"db.reset"}
    response = confirm(client, pending_path(choose(client, "db.reset", token, db="postgres")), token, PASSWORD)
    outcome_id = response.headers["location"].rsplit("/", 1)[1]
    time.sleep(0.2)
    broker.gate.set()
    outcome = the_session(app, client).work.outcome(outcome_id)
    wait_until(lambda: outcome.done)
    assert outcome.kind == "result" and outcome.exit_code == 0
    assert [timeout for kind, verb, timeout in broker.timeouts if verb == "db.reset"] == [None]
    assert all(timeout == 0.05 for kind, verb, timeout in broker.timeouts if verb in ("db.status", "source.list", "run.list"))
    assert client.get("/").status_code == 200


def test_an_unreachable_broker_is_an_outcome_not_a_logout(client, broker, token):
    """Bug class: a broker that went away during an action logging the operator out (the outcome is lost with the session)."""
    broker.answers["db.stop"] = broker_client.BrokerUnreachable(broker_client.NO_SOCKET)
    response = confirm(client, pending_path(choose(client, "db.stop", token)), token)
    assert response.status_code == 200 and "The broker is not running" in response.text
    assert client.get("/").status_code == 200


def test_another_sessions_outcome_is_not_found(client, token, app, make_client):
    """Bug class: an outcome (a command's output) readable by another session that guesses or saw its id."""
    response = confirm(client, pending_path(choose(client, "db.stop", token)), token)
    path = response.headers["location"]
    assert client.get(path).status_code == 200
    other = make_client(app, address="192.0.2.51")
    log_in(other)
    assert other.get(path).status_code == 404
    assert client.get("/actions/" + "A" * 22).status_code == 404 and client.get("/actions/short").status_code == 404


def test_an_outcome_keeps_at_most_16_kib_of_each_text(client, broker, token, app):
    """Bug class: the whole output of a command kept for each of 32 outcomes of each session (memory without bound), or its beginning kept instead of its end."""
    long_text = "".join(f"line {number:05d} \u00e9\u4e2d\n" for number in range(4000))
    broker.answers["db.stop"] = {"exit_code": 0, "output": long_text, "errors": long_text}
    response = confirm(client, pending_path(choose(client, "db.stop", token)), token)
    outcome = the_session(app, client).work.outcome(response.headers["location"].rsplit("/", 1)[1])
    for text in (outcome.output, outcome.errors):
        assert len(text.encode()) <= 16 * 1024
        assert text.endswith("line 03999 \u00e9\u4e2d\n") and text.startswith("line ")
    assert work.text_end("a" * 10 + "\n" + "b" * 5, 8) == "b" * 5


def test_at_most_32_outcomes_and_finished_ones_go_first():
    """Bug class: outcomes without bound, or a running action's outcome evicted while finished ones are kept (its page then says "not found" while it runs)."""
    kept = work.SessionWork(outcomes_max=3)
    running_old, done, running_new = (work.Outcome(work.new_id(), "db.stop", None, 0) for _ in range(3))
    done.finish("ok", dict(DONE))
    for outcome in (running_old, done, running_new):
        kept.add_outcome(outcome)
    newest = work.Outcome(work.new_id(), "db.stop", None, 0)
    kept.add_outcome(newest)
    assert kept.outcome_ids() == [running_old.id, running_new.id, newest.id]
    kept.add_outcome(work.Outcome(work.new_id(), "db.stop", None, 0))
    assert running_old.id not in kept.outcome_ids() and len(kept.outcome_ids()) == 3
    assert work.OUTCOMES_MAX == 32


def test_the_bound_on_running_actions_answers_busy(make_app, make_client, broker):
    """Bug class: no bound on the threads that run actions (each can run for minutes), or a busy answer that drops the action."""
    app = make_app(action_wait_seconds=0.05)
    client = make_client(app)
    log_in(client)
    token = form_token(client)
    broker.gate = threading.Event()
    broker.gate_verbs = {"db.stop"}
    for _ in range(2):
        assert confirm(client, pending_path(choose(client, "db.stop", token)), token).status_code == 303
    path = pending_path(choose(client, "db.stop", token))
    response = confirm(client, path, token)
    assert response.status_code == 503 and "already runs as many actions" in response.text
    assert client.get(path).status_code == 200
    assert len([call for call in broker.calls if call[0] == "call" and call[2] == "db.stop"]) == 2
    broker.gate.set()
    wait_until(lambda: confirm(client, path, token).status_code == 303)


def test_a_cancel_gets_through_while_long_actions_hold_every_place(make_app, make_client, broker):
    """Bug class: one bound for every action, which a database start and a reset (each can run for minutes) fill, so that the cancel of a run that writes into the wrong place is answered "busy"."""
    app = make_app(action_wait_seconds=0.05)
    client = make_client(app)
    log_in(client)
    token = form_token(client)
    broker.gate = threading.Event()
    broker.gate_verbs = {"db.start", "db.reset"}
    assert confirm(client, pending_path(choose(client, "db.start", token)), token, PASSWORD).status_code == 303
    assert confirm(client, pending_path(choose(client, "db.reset", token, db="mongo")), token, PASSWORD).status_code == 303
    wait_until(lambda: len([call for call in broker.calls if call[0] == "call" and call[2] in ("db.start", "db.reset")]) == 2)
    assert confirm(client, pending_path(choose(client, "db.stop", token)), token).status_code == 503
    response = confirm(client, pending_path(choose(client, "run.cancel", token, run_id=RUN_ID)), token)
    assert response.status_code == 303
    outcome = the_session(app, client).work.outcome(response.headers["location"].rsplit("/", 1)[1])
    wait_until(lambda: outcome.done)
    assert outcome.kind == "result" and [request["verb"] for request in state_changing_requests(broker)] == ["run.cancel"]
    broker.gate.set()


def test_the_poll_of_an_outcome_no_longer_kept_says_so_and_stops(make_app, make_client, broker):
    """Bug class: an outcome evicted while its page polls answered 404, which htmx does not swap: the page says "under way" for good."""
    app = make_app(action_wait_seconds=0.05, outcomes_max=1)
    client = make_client(app)
    log_in(client)
    token = form_token(client)
    broker.gate = threading.Event()
    broker.gate_verbs = {"db.stop"}
    first = confirm(client, pending_path(choose(client, "db.stop", token)), token).headers["location"]
    confirm(client, pending_path(choose(client, "db.stop", token)), token)
    part = client.get(first, headers={"hx-request": "true"})
    assert part.status_code == 200 and "no longer kept" in part.text and "hx-get" not in part.text and "under way" not in part.text
    assert client.get(first).status_code == 404
    broker.gate.set()


def test_put_back_drops_an_action_that_expired_during_the_call_and_keeps_the_bound():
    """Bug class: a pending action put back after a refused step-up although its five minutes ran out during the call (it takes a place of the bound and drops a live one, then expires), or put back beyond the bound of sixteen."""
    kept = work.SessionWork(pending_max=1, pending_seconds=300)
    pending = kept.add_pending("db.stop", {"service": "mongo"}, 0)
    assert kept.take_pending(pending.id, 299) is pending
    live = kept.add_pending("db.stop", {}, 299)
    kept.put_back(pending, 300)
    assert kept.pending(pending.id, 300) is None and pending.fields is None
    assert kept.pending(live.id, 300) is live
    kept = work.SessionWork(pending_max=2, pending_seconds=300)
    first = kept.add_pending("db.stop", {}, 0)
    second = kept.add_pending("db.stop", {}, 1)
    taken = kept.take_pending(first.id, 2)
    third = kept.add_pending("db.stop", {}, 3)
    kept.put_back(taken, 4)
    assert kept.pending_count() == 2
    assert kept.pending(second.id, 4) is None and kept.pending(third.id, 4) is third and kept.pending(first.id, 4) is first


def test_keeping_and_forgetting_the_database_password_is_said_on_the_pages(client, broker, token):
    """Bug class: a page that says a database password is kept when it is not (a run then fails on a missing password), or the reverse."""
    assert "No database admin password is kept for this session" in client.get("/databases").text
    confirm(client, pending_path(choose(client, "session.set_db_password", token, password=SECRET)), token, PASSWORD)
    assert "A database admin password is kept for this session" in client.get("/databases").text
    confirm(client, pending_path(choose(client, "session.clear_db_password", token)), token)
    assert "No database admin password is kept for this session" in client.get("/databases").text

