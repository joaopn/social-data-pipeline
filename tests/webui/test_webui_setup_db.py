"""Tests for the "Set up a database" wizard (`webui.setup_db`) and its table entry (`webui.setup_db_table`): `sdp db setup` for a database not set up yet, from the Databases page.

The contracts pinned here, each test naming the bug class it catches:

- Nothing is asked of the broker but the `db.status` that starts the wizard until the confirm, and what the confirm sends is exactly the answers of the steps (secrets as text to the broker only).
- A step's answers are kept across Back and Next; a password is never drawn back into a page; an expired wizard is gone.
- Every step's form stays under the guard's 64 fields, with the most rows a list can have.
- The confirm page's lines of what a setup writes, pinned whole for a first-time setup and for a database added.
- The way to start a database after its setup ("Start it now") and the Databases page's buttons.

Passwords are made-up values.
"""

from __future__ import annotations

import re

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette.testclient")

from test_webui_pages import inline_code
from webui_helpers import DONE, ORIGIN, PASSWORD, confirm, db_status, form_token, log_in, main_html, pending_path, wait_until

from social_data_pipeline.webui import setup_db, setup_db_table

ADMIN = "made-up-admin-pw-77d2"
NOTHING_SET_UP = {"data": {"verbose": False, "configured": []}}


@pytest.fixture
def app(make_app):
    return make_app(action_wait_seconds=0.5)


@pytest.fixture
def client(app, make_client, broker):
    broker.answers["db.setup"] = dict(DONE)
    made = make_client(app)
    assert log_in(made).status_code == 303
    return made


def post(client, step, token, **fields):
    return client.post(f"/databases/setup/{step}", data={"form_token": token, **fields}, headers={"origin": ORIGIN})


def form_names(page, action):
    """The distinct names a form posts (its `go` buttons count once, as only the pressed one is posted)."""
    form = re.search(rf'<form method="post" action="{re.escape(action)}"[^>]*>(.*?)</form>', page, re.DOTALL).group(1)
    return set(re.findall(r'name="([^"]+)"', form))


def first_time_wizard(client, broker, token):
    """Walk the first-time wizard for MongoDB and StarRocks with authentication and a generated read-only password, up to Review."""
    broker.answers["db.status"] = NOTHING_SET_UP
    assert client.get("/databases/setup?new=1").status_code == 200
    answer = post(client, "databases", token, pick_mongo="yes", pick_starrocks="yes", data_path="./data", export_path="./data/export", go="next")
    assert answer.headers["location"] == "/databases/setup/mongo"
    assert post(client, "mongo", token, mongo_port="27018", mongo_validate="tail", go="next").headers["location"] == "/databases/setup/starrocks"
    assert post(client, "starrocks", token, sr_fe_jvm_heap="6", **{"starrocks_storage_paths[0]": "/mnt/a/sr"}, go="next").headers["location"] == "/databases/setup/auth"
    answer = post(client, "auth", token, auth="yes", new_password=ADMIN, new_password_confirm=ADMIN, ro_user="yes", ro_username="reader", ro_password_mode="generate", go="next")
    assert answer.headers["location"] == "/databases/setup/review"


def test_nothing_is_asked_or_written_before_the_confirm_and_the_confirm_sends_the_answers(client, broker):
    """Bug class: a wizard that calls the broker before the operator confirmed (a step that writes), or a confirm that sends other answers than the steps took (a value lost between steps, a default invented on the web side, a password not sent), or a password shown on a page."""
    token = form_token(client)
    before = broker.verbs_called()
    first_time_wizard(client, broker, token)
    review = client.get("/databases/setup/review").text
    assert ADMIN not in review and "python sdp.py db setup" in review
    assert broker.verbs_called() == before + ["db.status"]

    pending = pending_path(post(client, "confirm", token))
    page = client.get(pending).text
    assert ADMIN not in page and 'name="management_password"' in page and "The admin password is not stored anywhere" in page
    assert broker.verbs_called() == before + ["db.status"]
    outcome = confirm(client, pending, token, PASSWORD).headers["location"]
    wait_until(lambda: "db.setup" in broker.verbs_called())

    sent = [request for request in broker.requests if request["verb"] == "db.setup"]
    assert sent[0]["fields"] == {
        "databases": ["mongo", "starrocks"], "data_path": "./data", "export_path": "./data/export", "mongo_port": 27018, "mongo_validate": "tail", "sr_fe_jvm_heap": 6, "starrocks_storage_paths": ["/mnt/a/sr"],
        "auth": True, "new_password": ADMIN, "new_password_confirm": ADMIN, "ro_user": True, "ro_username": "reader", "ro_password_mode": "generate",
    }
    assert sent[0]["step_up"] is True
    shown = client.get(outcome).text
    assert 'href="/databases/setup/start"' in shown and "Start it now" in shown


def test_back_and_next_keep_each_steps_answers_and_never_draw_a_password(client, broker):
    """Bug class: a step's answers lost when the operator goes back (the form drawn with the defaults again), the databases step dropping its choice, or a password written back into a page's markup."""
    token = form_token(client)
    first_time_wizard(client, broker, token)
    assert post(client, "review", token, go="back").status_code == 404
    mongo = client.get("/databases/setup/mongo").text
    assert 'value="27018"' in mongo and '<option value="tail" selected>' in mongo
    assert 'name="pick_mongo" value="yes" checked' in client.get("/databases/setup/databases").text
    auth = client.get("/databases/setup/auth").text
    assert ADMIN not in auth and "A password is kept from before" in auth
    # Posting the step again with the password boxes empty keeps the passwords.
    assert post(client, "auth", token, auth="yes", ro_user="yes", ro_username="reader", ro_password_mode="generate", go="back").headers["location"] == "/databases/setup/starrocks"
    assert post(client, "starrocks", token, sr_fe_jvm_heap="7", go="back").headers["location"] == "/databases/setup/mongo"
    assert 'value="7"' in client.get("/databases/setup/starrocks").text
    pending = pending_path(post(client, "confirm", token))
    assert pending.startswith("/pending/")
    # The draft gave its passwords to the pending action.
    assert "A password is kept from before" not in client.get("/databases/setup/auth").text


def test_a_step_with_an_answer_the_check_refuses_is_shown_again_and_nothing_is_kept_of_it(client, broker):
    """Bug class: a mismatched admin password or a reserved tablespace name carried to the Review (or worse, to the broker) instead of being refused on its own step."""
    token = form_token(client)
    before = broker.verbs_called()
    broker.answers["db.status"] = NOTHING_SET_UP
    client.get("/databases/setup?new=1")
    post(client, "databases", token, pick_postgres="yes", go="next")
    refused = post(client, "postgres", token, **{"tablespaces[0][name]": "pgdata", "tablespaces[0][path]": "/mnt/a"}, go="next")
    assert refused.status_code == 400 and "The field: Tablespaces." in refused.text
    post(client, "postgres", token, go="next")
    refused = post(client, "auth", token, auth="yes", new_password=ADMIN, new_password_confirm=ADMIN + "x", go="next")
    assert refused.status_code == 400 and "The field: Admin password again." in refused.text and ADMIN not in refused.text
    assert broker.verbs_called() == before + ["db.status"]


def test_a_wizard_left_too_long_is_gone(client, broker, clock):
    """Bug class: answers (and passwords) kept in memory past the life of a pending action, or a stale wizard confirmed long after its answers were given."""
    token = form_token(client)
    first_time_wizard(client, broker, token)
    clock.now += 5 * 60
    gone = client.get("/databases/setup/review")
    assert gone.status_code == 410 and setup_db.EXPIRED in gone.text
    assert post(client, "confirm", token).status_code == 410


def test_adding_a_database_asks_one_database_and_skips_the_authentication_of_a_setup_that_has_it(client, broker):
    """Bug class: the first-time questions (base path, export path, several databases) asked when a database is set up already, which would rewrite .env whole; or authentication asked again where the terminal gives the new database the setup's."""
    token = form_token(client)
    broker.answers["db.status"] = db_status()
    page = client.get("/databases/setup?new=1&db=starrocks").text
    assert 'name="add" value="starrocks" checked' in page and 'name="data_path"' not in page and 'value="postgres"' not in page
    assert post(client, "databases", token, add="starrocks", go="next").headers["location"] == "/databases/setup/starrocks"
    assert post(client, "starrocks", token, go="next").headers["location"] == "/databases/setup/review"
    review = client.get("/databases/setup/review").text
    assert "python sdp.py db setup --add=starrocks" in review
    page = client.get(pending_path(post(client, "confirm", token))).text
    assert "the new one gets it too" in page


def test_every_steps_form_stays_under_the_guards_field_limit(client, broker):
    """Bug class: a step whose form posts more than the guard's 64 fields (the guard then refuses it whole): the PostgreSQL step with every tablespace row filled is the largest."""
    token = form_token(client)
    broker.answers["db.status"] = NOTHING_SET_UP
    client.get("/databases/setup?new=1")
    post(client, "databases", token, pick_postgres="yes", pick_mongo="yes", pick_starrocks="yes", go="next")
    rows = {f"tablespaces[{index}][{key}]": f"t{index}" if key == "name" else f"/mnt/t{index}" for index in range(setup_db_table.LIST_MAX) for key in ("name", "path")}
    assert post(client, "postgres", token, **rows, go="stay").status_code == 303
    paths = {f"starrocks_storage_paths[{index}]": f"/mnt/s{index}" for index in range(setup_db_table.LIST_MAX)}
    assert post(client, "starrocks", token, **paths, go="stay").status_code == 303
    for step in ("databases", "postgres", "mongo", "starrocks", "auth"):
        page = client.get(f"/databases/setup/{step}").text
        assert len(form_names(page, f"/databases/setup/{step}")) < 64, step
        assert inline_code(page) == []
    assert len(form_names(client.get("/databases/setup/postgres").text, "/databases/setup/postgres")) > 40


FIRST_TIME = {"databases": ["postgres", "starrocks"], "export_path": "/srv/export", "tablespaces": [{"name": "fast", "path": "/mnt/fast"}], "auth": True, "new_password": ADMIN, "new_password_confirm": ADMIN, "ro_user": True, "ro_password_mode": "generate"}
FIRST_TIME_LINES = [
    "Rewrites .env: its database, export and authentication keys are written anew (DATA_PATH, the keys of each database set up here, DB_EXPORT_PATH, the *_AUTH_ENABLED keys, MONGO_ADMIN_USER and the *_RO_USER keys); every other key in it is kept as it is, except HF_HOME and HF_TOKEN, which are written as empty comment lines, so a value either had is dropped (as the terminal does). The existing .env is kept as .env.bak.",
    "Writes config/db/postgres.yaml, config/postgres/postgresql.local.conf, config/postgres/pg_hba.local.conf; config/db/starrocks.yaml, config/starrocks/fe.local.conf, config/starrocks/be.local.conf; a file that exists already is kept as a .bak copy first.",
    "Rewrites docker-compose.override.yml whole, with only the tablespace and extra-disk mounts: every other entry in it is dropped, the scheduler's /jobs_export mounts included (run `db setup-jobs` again to put them back).",
    "Makes the export folder /srv/export when it is missing.",
    "Makes the data folder of PostgreSQL (<data base path>/database/postgres) when it is missing and writes the read-only user's password into .ro_credentials there (mode 0600).",
    "Makes the data folder of StarRocks (<data base path>/database/starrocks) when it is missing and writes the read-only user's password into .ro_credentials there (mode 0600).",
    "No other folder is made or changed here, and no data is moved.",
    "The admin password is not stored anywhere: you will give it when the database starts. It was typed twice to check it.",
    "The read-only password is generated (32 characters) and kept only in .ro_credentials; it is never shown.",
    "Nothing is started: start the database afterwards (python sdp.py db start).",
]
ADDED = {"add": "starrocks", "starrocks_data_path": "/srv/sr", "starrocks_storage_paths": ["/mnt/a"]}
ADDED_LINES = [
    "Writes config/db/starrocks.yaml, config/starrocks/fe.local.conf and config/starrocks/be.local.conf; a file that exists already is kept as a .bak copy first.",
    "Sets these keys in .env and leaves every other line of it as it is: STARROCKS_DATA_PATH, STARROCKS_PORT, STARROCKS_FE_HTTP_PORT, STARROCKS_MEM_LIMIT (when a limit is set).",
    "Rewrites docker-compose.override.yml: the starrocks block becomes one mount per extra disk, so every other mount of that block is dropped (the scheduler's /jobs_export mount included, until `db setup-jobs` is run again); the other services' blocks stay.",
    "When authentication is on for the databases set up already, the new one gets it too: its authentication keys are set, and the read-only user's password is read from an existing .ro_credentials and written into .ro_credentials in its data folder (made when missing). When it is off, the new database has none.",
    "No other folder is made or changed here, and no data is moved.",
    "Nothing is started: start the database afterwards (python sdp.py db start).",
]


@pytest.mark.parametrize("fields, lines", [(FIRST_TIME, FIRST_TIME_LINES), (ADDED, ADDED_LINES)])
def test_the_confirm_pages_lines_are_pinned_whole(fields, lines):
    """Bug class: a confirm page that says less than the setup does (the .env rewrite dropping HF_TOKEN, the override rewritten whole, the read-only password files and their folders, the admin password's handling), which the operator confirms on trust. Written from `setup.db_answers` and `setup.db`; change them together."""
    assert setup_db_table.writes(fields) == lines
    assert setup_db_table.command(fields) == ("python sdp.py db setup --add=starrocks" if "add" in fields else "python sdp.py db setup")


def test_the_command_shown_is_what_the_cli_parses():
    """Bug class: a command shown that the CLI does not take (`--add` with another value form)."""
    import shlex

    import sdp

    for fields, add in ((FIRST_TIME, None), (ADDED, "starrocks")):
        parsed = sdp.build_parser().parse_args(shlex.split(setup_db_table.command(fields))[2:])
        assert (parsed.command, parsed.db_command, parsed.add) == ("db", "setup", add)


def test_the_databases_page_offers_the_setup_and_start_it_now(client, broker):
    """Bug class: the mock-up's "Set up a database…" and "Set up…" buttons missing or leading nowhere, or a database just set up offered no way to start it with its admin password."""
    page = main_html(client.get("/databases").text)
    assert 'href="/databases/setup?new=1"' in page and 'href="/databases/setup?new=1&amp;db=starrocks"' in page
    # mongo is stopped in the stand-in's status.
    assert 'href="/databases/setup/start?db=mongo"' in page
    start = client.get("/databases/setup/start?db=mongo").text
    assert 'name="action" value="db.start"' in start and 'name="service" value="mongo"' in start and 'name="admin_password"' in start
    assert client.get("/databases/setup/start?db=nope").status_code == 404
    assert broker.verbs_called().count("db.setup") == 0
