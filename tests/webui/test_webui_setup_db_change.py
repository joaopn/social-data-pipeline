"""Tests for "Change…" of the databases set up (`webui.setup_db` in mode `change`, `webui.setup_db_table` `prefill`, `changes`, `writes` with the read): `sdp db setup` run over the setup, from the Databases page.

The contracts pinned here, each test naming the bug class it catches:

- The form filled in from the `db.settings` read and posted back as it is drawn writes what the terminal writes when every question is answered with Enter: the real read and the real action run behind the stand-in broker, against a deployment the terminal set up (the harness of `tests/setup/test_db_setup_action.py`).
- Authentication switched off is refused on its step with the sentence that sends the operator to the terminal, and never reaches the broker.
- A data path changed: the confirm page says in plain words that the data is not moved and names the old folder; it lists what changes; the outcome says to restart.

Passwords are made-up values.
"""

from __future__ import annotations

import html
import importlib.util
from html.parser import HTMLParser
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette.testclient")

from webui_helpers import DONE, ORIGIN, PASSWORD, confirm, form_token, log_in, main_html, pending_path, wait_until

import sdp
from social_data_pipeline.control import types, verbs
from social_data_pipeline.webui import actions, setup_db_table

_spec = importlib.util.spec_from_file_location("db_setup_harness", Path(__file__).resolve().parents[1] / "setup" / "test_db_setup_action.py")
harness = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(harness)

ADMIN = "made-up-admin-pw-0c3e"
RO = "made-up-ro-pw-5b8d"
BASE_TYPED = {
    "db_databases": "postgres,mongo,starrocks", "db_tablespaces": "y", "db_ts_name": ["fast1"], "db_ts_path": ["/mnt/nvme1/pg"], "db_ts_more": ["n"],
    "db_filesystem": "zfs", "db_pgtune_method": "paste", "db_pgtune_paste": "max_connections = 40", "db_sr_multidisk": "y", "db_sr_storage_path": ["/mnt/a/sr"], "db_sr_more_paths": ["n"],
    "db_auth": "y", "db_password": ADMIN, "db_ro_user": "y", "db_ro_username": "reader", "db_ro_auto_password": "n", "db_ro_password": RO,
}


class _Form(HTMLParser):
    """What a browser posts of the form with this action, as drawn: its fields with their values, ticked boxes only, the selected option of each list, no password (the page draws none), no button."""

    def __init__(self, action):
        super().__init__()
        self.action, self.inside, self.fields, self.select, self.area = action, False, {}, None, None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "form":
            self.inside = attrs.get("action") == self.action
        if not self.inside or "disabled" in attrs:
            return
        name = attrs.get("name")
        if tag == "input" and name and attrs.get("type") not in ("password", "submit"):
            if attrs.get("type") in ("checkbox", "radio"):
                if "checked" in attrs:
                    self.fields[name] = attrs.get("value", "on")
            else:
                self.fields[name] = attrs.get("value", "")
        elif tag == "select":
            self.select = name
        elif tag == "option" and self.select and ("selected" in attrs or self.select not in self.fields):
            self.fields[self.select] = attrs.get("value", "")
        elif tag == "textarea":
            self.area = name
            self.fields[name] = ""

    def handle_endtag(self, tag):
        if tag == "form":
            self.inside = False
        elif tag == "select":
            self.select = None
        elif tag == "textarea":
            self.area = None

    def handle_data(self, data):
        if self.area:
            self.fields[self.area] += data.removeprefix("\n") if self.fields[self.area] == "" else data


def drawn(page, action):
    reader = _Form(action)
    reader.feed(page)
    return reader.fields


@pytest.fixture
def app(make_app):
    return make_app(action_wait_seconds=0.5)


@pytest.fixture
def client(app, make_client):
    made = make_client(app)
    assert log_in(made).status_code == 303
    return made


def _post(client, step, fields, **extra):
    return client.post(f"/databases/setup/{step}", data={**fields, **extra}, headers={"origin": ORIGIN})


def _deployments(tmp_path, monkeypatch):
    terminal, browser = tmp_path / "terminal", tmp_path / "browser"
    for root in (terminal, browser):
        harness._project(root, harness.ENV)
        harness._terminal(monkeypatch, root, BASE_TYPED)
    return terminal, browser


def _real_broker(broker, monkeypatch, root):
    """The stand-in broker answers `db.settings` and `db.setup` with the CLI's real getter and action in `root`, the request validated by the registry as the broker's child validates it."""
    harness._point(monkeypatch, root)
    broker.answers["db.settings"] = lambda fields: {"data": sdp.get_db_settings()}

    def setup(fields):
        code = sdp.do_db_setup(**types.reveal(verbs.validate_request("db.setup", fields)))
        return {**DONE, "exit_code": code}

    broker.answers["db.setup"] = setup


def test_the_form_filled_in_and_posted_as_drawn_writes_what_the_terminals_enter_writes(tmp_path, monkeypatch, client, broker):
    """Bug class: a "Change…" form filled in otherwise than the terminal's defaults over the setup, so that confirming it unchanged rewrites the setup (a path written with another form, a memory limit lost, the tablespaces, extra disks, PGTune output or read-only password not kept, the ZFS settings switched off), or one that asks the broker for anything but the read before the confirm."""
    terminal, browser = _deployments(tmp_path, monkeypatch)
    harness._terminal(monkeypatch, terminal, {"db_filesystem": "zfs", "db_password": ADMIN})
    _real_broker(broker, monkeypatch, browser)
    token = form_token(client)

    page = client.get("/databases/setup?change=1").text
    for step, following in (("databases", "postgres"), ("postgres", "mongo"), ("mongo", "starrocks"), ("starrocks", "auth"), ("auth", "review")):
        fields = drawn(page, f"/databases/setup/{step}")
        assert fields["form_token"] == token
        if step == "auth":
            assert ADMIN not in page and RO not in page
            fields.update(new_password=ADMIN, new_password_confirm=ADMIN)
        answer = _post(client, step, fields, go="next")
        assert answer.headers.get("location") == f"/databases/setup/{following}", (step, answer.text[-2000:])
        page = client.get(answer.headers["location"]).text
    assert "No answer differs from the setup as it is now" in page
    assert broker.verbs_called().count("db.settings") == 1 and "db.setup" not in broker.verbs_called()

    outcome = confirm(client, pending_path(client.post("/databases/setup/confirm", data={"form_token": token}, headers={"origin": ORIGIN})), token, PASSWORD).headers["location"]
    wait_until(lambda: "Nothing was restarted. The output's last line names the running services that must restart to apply this" in client.get(outcome).text)
    assert '<a href="/overview">Needs attention on the Overview</a> restarts them' in client.get(outcome).text
    harness._point(monkeypatch, browser)
    assert harness._tree(browser) == harness._tree(terminal)
    assert {name: path.read_bytes() for name, path in harness._credentials(browser).items()} == {name: path.read_bytes() for name, path in harness._credentials(terminal).items()}


def _change_read(**overrides):
    read = {
        "configured": ["postgres", "mongo"], "not_readable": None, "auth": True, "ro_username": "reader", "ro_password_set": True, "tablespaces": [], "storage_paths": [], "pgtune": "", "pgtune_set": False,
        "values": {"data_path": "./data", "export_path": "./data/export", "pgdata_path": "./data/database/postgres", "db_name": "datasets", "pg_port": 5432, "pg_mem_limit": 0, "filesystem": "standard",
                   "mongo_data_path": "./data/database/mongo", "mongo_port": 27017, "mongo_cache_size_gb": 2, "mongo_mem_limit": 4, "mongo_validate": "full"},
    }
    read.update(overrides)
    return read


def test_authentication_switched_off_is_refused_on_its_step_and_never_sent(client, broker):
    """Bug class (security): the browser switching authentication off for databases that have it (decided: a terminal command), by unticking its box or by leaving it out; the step must refuse it with the way to the terminal, and nothing may reach the broker."""
    broker.answers["db.settings"] = {"data": _change_read()}
    token = form_token(client)
    before = broker.verbs_called()
    page = client.get("/databases/setup?change=1&db=mongo").text
    assert 'action="/databases/setup/mongo"' in page and 'value="27017"' in page
    page = client.get("/databases/setup/auth").text
    assert 'name="auth" value="yes" checked' in page and '<option value="keep" selected>' in page and setup_db_table.AUTH_OFF_SENTENCE in page
    fields = drawn(page, "/databases/setup/auth")
    del fields["auth"]
    refused = _post(client, "auth", {**fields, "new_password": ADMIN, "new_password_confirm": ADMIN}, go="next")
    assert refused.status_code == 400 and setup_db_table.AUTH_OFF_SENTENCE in refused.text and ADMIN not in refused.text
    # Drawn again as it is set up: authentication on, the read-only user and its password kept.
    assert 'name="auth" value="yes" checked' in refused.text and '<option value="keep" selected>' in refused.text
    assert client.post("/databases/setup/confirm", data={"form_token": token}, headers={"origin": ORIGIN}).status_code == 400
    assert broker.verbs_called() == before + ["db.settings"]


CHANGED_LINES = [
    "Rewrites .env: its database, export and authentication keys are written anew (DATA_PATH, the keys of each database set up here, DB_EXPORT_PATH, the *_AUTH_ENABLED keys, MONGO_ADMIN_USER and the *_RO_USER keys); every other key in it is kept as it is, except HF_HOME and HF_TOKEN, which are written as empty comment lines, so a value either had is dropped (as the terminal does). The existing .env is kept as .env.bak.",
    "Writes config/db/postgres.yaml, config/postgres/postgresql.local.conf, config/postgres/pg_hba.local.conf; config/db/mongo.yaml; a file that exists already is kept as a .bak copy first.",
    "Makes the export folder ./data/export when it is missing.",
    "Makes the data folder of PostgreSQL (/mnt/pg) when it is missing and writes the read-only user's password into .ro_credentials there (mode 0600).",
    "Makes the data folder of MongoDB (./data/database/mongo) when it is missing and writes the read-only user's password into .ro_credentials there (mode 0600).",
    "No other folder is made or changed here, and no data is moved.",
    "The data path of PostgreSQL changes from ./data/database/postgres to /mnt/pg. Its existing data is NOT moved: PostgreSQL will start empty at the new path, and the old data stays in ./data/database/postgres.",
    "The admin password is not stored anywhere: you will give it when the database starts. It was typed twice to check it.",
    "The read-only password is kept: it is read from the .ro_credentials set up now and written again into .ro_credentials in each database's data folder; it is never shown.",
    "Nothing is restarted: a running service keeps its old settings until it is restarted. Needs attention on the Overview names the services that must restart, with a Restart button.",
]


def test_a_data_path_changed_is_said_on_the_confirm_page_with_the_old_folder(client, broker):
    """Bug class: a data path changed from the browser confirmed without the operator being told that the data is not moved (the database starts empty and the old data stays behind, decided to be said in plain words, naming the old folder), a confirm page that does not list what changes, or an outcome that does not say to restart."""
    broker.answers["db.settings"] = {"data": _change_read()}
    broker.answers["db.setup"] = dict(DONE)
    token = form_token(client)
    page = client.get("/databases/setup?change=1&db=postgres").text
    assert "its data stays in ./data/database/postgres" in page
    fields = drawn(page, "/databases/setup/postgres")
    assert fields["pgdata_path"] == "./data/database/postgres" and fields["pg_mem_limit"] == "0"
    assert _post(client, "postgres", {**fields, "pgdata_path": "/mnt/pg"}, go="next").headers["location"] == "/databases/setup/mongo"
    fields = drawn(client.get("/databases/setup/auth").text, "/databases/setup/auth")
    assert _post(client, "auth", {**fields, "new_password": ADMIN, "new_password_confirm": ADMIN}, go="next").headers["location"] == "/databases/setup/review"

    pending = pending_path(client.post("/databases/setup/confirm", data={"form_token": token}, headers={"origin": ORIGIN}))
    shown = html.unescape(main_html(client.get(pending).text))
    for line in CHANGED_LINES:
        assert line in shown, line
    assert "<td>PostgreSQL data path</td><td class=\"mono\">./data/database/postgres</td><td class=\"mono\">/mnt/pg</td>" in shown and ADMIN not in shown
    outcome = confirm(client, pending, token, PASSWORD).headers["location"]
    wait_until(lambda: "db.setup" in broker.verbs_called())
    sent = [request["fields"] for request in broker.requests if request["verb"] == "db.setup"][0]
    assert sent["pgdata_path"] == "/mnt/pg" and "mongo_data_path" not in sent and sent["ro_password_mode"] == "keep" and sent["auth"] is True
    assert 'href="/databases"' in client.get(outcome).text


def test_the_lines_and_changes_of_a_change_are_pinned_whole():
    """Bug class: a confirm page of a change that says less than it does (the sentence on the data path, the read-only password kept, the restart), or a list of changes that misses one or lists a path left as it is."""
    read = _change_read()
    fields = {**setup_db_table.prefill(read), "pgdata_path": "/mnt/pg", "mongo_port": 27018, "new_password": ADMIN, "new_password_confirm": ADMIN}
    assert setup_db_table.first_refusal(fields, setup_db_table.current_of(read)) is None
    assert setup_db_table.writes(fields, read) == CHANGED_LINES
    assert setup_db_table.changes(fields, read) == [("PostgreSQL data path", "./data/database/postgres", "/mnt/pg"), ("MongoDB port", "27017", "27018")]
    assert setup_db_table.changes({**fields, "mongo_data_path": "data/database/mongo/"}, read) == setup_db_table.changes(fields, read)
    # Without the read (a pending action not made by the form) the sentence is still there, without the old folder's name.
    assert setup_db_table.GENERIC_DATA_PATH in setup_db_table.writes(fields)
    # One text for "nothing restarts" on every setup's confirm page: a change of databases says what the MCP servers' and the scheduler's setup say.
    assert CHANGED_LINES[-1] == setup_db_table.RESTART == actions._NOTHING_RESTARTS


def test_the_databases_page_offers_change(client, broker):
    """Bug class: the database cards and the page without a way to change a database set up."""
    page = main_html(client.get("/databases").text)
    assert 'href="/databases/setup?change=1"' in page and 'href="/databases/setup?change=1&amp;db=postgres"' in page and 'href="/databases/setup?change=1&amp;db=mongo"' in page
