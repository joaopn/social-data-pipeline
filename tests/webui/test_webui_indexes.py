"""Tests for Databases → Create indexes… (`webui.indexes`): the column picker, the pending action it makes and its confirm page.

The contracts pinned here, each test naming the bug class it catches:

- The statements the confirm page says the build will run are the ones `sdp.py` `do_db_create_indexes` runs for the same request on the same tables (both against the stand-in databases of `tests/setup/test_create_indexes.py`).
- The picker offers the real columns the broker read, and a pick naming anything else is refused by the web UI before anything is kept; nothing that changes anything is sent before the confirm; the confirm page's run sentence is pinned whole; the outcome links to the run.
"""

from __future__ import annotations

import html
import importlib.util
import json
import re
import shlex
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette.testclient")

from webui_helpers import ORIGIN, PASSWORD, RUN_ID, confirm, form_token, log_in, main_html, pending_path, source_list, state_changing_requests

from social_data_pipeline.control import client as broker_client
from social_data_pipeline.webui import actions, indexes

ROOT = Path(__file__).resolve().parents[2]


def _setup_tests():
    """The module of the CLI's index tests, loaded from its file (it is in another test folder)."""
    spec = importlib.util.spec_from_file_location("create_indexes_cases", ROOT / "tests" / "setup" / "test_create_indexes.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


TARGETS = {"data": {
    "source": "demo", "database": "starrocks", "scope": "demo", "parallel_workers": None, "truncated": False, "skipped_tables": 0, "skipped_columns": 1,
    "tables": [
        {"name": "comments", "indexes": ["idx_comments_score"], "bloom_filter": ["dataset"], "collections": None, "db_name": None, "columns": [
            {"name": "dataset", "type": "varchar", "bloom": True, "bitmap": True},
            {"name": "author", "type": "varchar", "bloom": True, "bitmap": True},
            {"name": "controversiality", "type": "tinyint", "bloom": False, "bitmap": True},
            {"name": "upvote_ratio", "type": "double", "bloom": False, "bitmap": False},
        ]},
        {"name": "comments_lingua", "indexes": [], "bloom_filter": [], "collections": None, "db_name": None, "columns": [{"name": "lang", "type": "varchar", "bloom": True, "bitmap": True}]},
    ],
}}


@pytest.fixture
def app(make_app):
    return make_app(action_wait_seconds=0.5)


@pytest.fixture
def client(app, make_client, broker):
    broker.answers["source.list"] = source_list({"name": "demo", "profiles": ["parse", "sr_ingest", "mongo_ingest"]})
    broker.answers["db.index_targets"] = TARGETS
    broker.answers["db.create_indexes"] = {"run_id": RUN_ID}
    made = make_client(app)
    assert log_in(made).status_code == 303
    return made


def pick(client, token, **boxes):
    return client.post("/indexes", data={"form_token": token, "source": "demo", "database": "starrocks", **boxes}, headers={"origin": ORIGIN})


@pytest.mark.parametrize("database", ["postgres", "starrocks", "mongo"])
def test_the_statements_shown_are_the_ones_the_action_runs(tmp_path, monkeypatch, database):
    """Bug class: a confirm page that shows other statements than the build runs: another index name, the StarRocks bloom filter list without the columns set now (the property is replaced whole), a DOUBLE column shown as indexed, a TINYINT one as a bloom filter. The listing the page reads, through its shape check, gives the preview; the action runs against the same stand-in tables."""
    import sdp

    cases = _setup_tests()
    root = tmp_path.resolve() / "project"
    cases.write_deployment(root)
    cases.point_at(sdp, monkeypatch, root)
    fake = cases.FakeDatabases()
    monkeypatch.setattr(sdp.subprocess, "run", fake.run)
    request = cases.SCENARIOS[database][2]

    block = indexes.targets_block({"data": json.loads(json.dumps(sdp.get_db_index_targets(source="reddit", db=database)))})
    shown = [line for line in indexes.preview(block, {"source": "reddit", "database": database, "indexes": request}) if not line.startswith("--")]
    sdp.do_db_create_indexes(source="reddit", database=database, indexes=request)

    ran = [statement for _, statement in fake.executed]
    if database == "mongo":
        # One createIndex per collection of the data type; the page shows one line per field.
        assert len(ran) == sum(2 for _ in request[0]["columns"]) and len(shown) == len(request[0]["columns"])
        assert all(f"createIndex({{'{column}': 1}}, {{name: '{column}_1'}});" in line for line, column in zip(shown, request[0]["columns"]))
    elif database == "starrocks":
        # Tables are built in parallel: the order between tables is not fixed.
        assert sorted(shown) == sorted(ran)
    else:
        assert shown == ran


def test_the_picker_shows_the_real_columns_and_the_terminals_explanation(client, broker):
    """Bug class: a picker that offers a column the database lacks, offers a box for one StarRocks cannot index, hides the existing indexes or the bloom filter set now, or leaves out the terminal's explanation of the two index types; and a GET that asks the broker for anything but reads."""
    page = main_html(client.get("/indexes?source=demo&database=starrocks&step=columns").text)

    assert html.escape(indexes.SR_EXPLANATION, quote=False) in page
    assert re.findall(r'name="c(\d+)" value="([^"]+)"', page) == [("0", "comments:dataset"), ("1", "comments:author"), ("2", "comments:controversiality"), ("4", "comments_lingua:lang")]
    assert re.findall(r'name="b(\d+)"', page) == ["0", "1", "4"]
    assert "BITMAP only" in page and "cannot be indexed" in page and "idx_comments_score" in page
    assert "sdp db create-indexes --source <b>demo</b>" in page
    assert broker.verbs_called() == ["source.list", "db.index_targets"] and state_changing_requests(broker) == []


def test_the_first_step_offers_the_sources_databases(client):
    """Bug class: a first step that offers no way on, or does not say which databases the source loads into (the command offers only those)."""
    page = main_html(client.get("/indexes?source=demo").text)

    assert '<option value="demo" selected>demo (MongoDB, StarRocks)</option>' in page
    assert 'action="/indexes"' in page and 'name="step" value="columns"' in page


def test_a_pick_becomes_a_pending_build_that_runs_as_a_run_after_the_confirm(client, broker):
    """Bug class: a pick sent to the broker before the operator confirmed, a request other than the boxes ticked (a BITMAP box lost, a column added), a confirm page that does not show the command, the statements or what a cancel does, or an outcome that does not lead to the run."""
    token = form_token(client)
    path = pending_path(pick(client, token, c1="comments:author", b1="comments:author", c2="comments:controversiality", c0="comments:dataset", c4="comments_lingua:lang", save="yes"))
    assert state_changing_requests(broker) == []

    page = main_html(client.get(path).text)
    assert "python sdp.py db create-indexes --source=demo" in page
    assert 'ALTER TABLE `demo`.`comments_lingua` SET (&#34;bloom_filter_columns&#34; = &#34;lang&#34;)' in page
    assert "CREATE INDEX `idx_comments_author` ON `demo`.`comments` (`author`) USING BITMAP" in page
    assert "-- comments: bloom filter already covers dataset" in page
    assert (
        "It runs as a run, listed in Runs and on the Overview with its output and Cancel. It first reads the tables and their columns from the database again and refuses a table or column the database does not have. "
        "A cancel stops the command, but does not undo what the database already did: an index being built may go on inside the database. "
        "When the build has made new indexes, it adds them to the source&#39;s config/sources/demo/platform.yaml (indexes and ml_indexes for PostgreSQL, mongo_indexes for MongoDB, sr_indexes and sr_ml_indexes for StarRocks)."
    ) in page
    assert 'name="management_password"' in page

    response = confirm(client, path, token, PASSWORD)
    sent = state_changing_requests(broker)
    assert [request["verb"] for request in sent] == ["db.create_indexes"] and sent[0]["step_up"] is True
    assert sent[0]["fields"] == {"source": "demo", "database": "starrocks", "save": True, "indexes": [
        {"table": "comments", "columns": ["dataset", "author", "controversiality"], "bitmap": ["author"]},
        {"table": "comments_lingua", "columns": ["lang"]},
    ]}
    assert f'<a href="/runs/{RUN_ID}">' in client.get(response.headers["location"]).text


@pytest.mark.parametrize("boxes", [
    {"c0": "comments:body"},
    {"c0": "other_table:id"},
    {"c3": "comments:upvote_ratio"},
    {"b2": "comments:controversiality", "c2": "comments:controversiality"},
    {"b1": "comments:author"},
    {"x1": "comments:author"},
    {"c1": "comments:author:x"},
    {"c1": "comments:author", "save": "no"},
    {},
])
def test_a_pick_of_something_the_table_lacks_is_refused_before_anything_is_kept(client, broker, boxes):
    """Bug class: a forged or stale pick kept and confirmed: a column or table the database did not list, a column StarRocks cannot index, a BITMAP box for a column that takes only BITMAP or without its index box, a box that is not the form's, nothing ticked. Refused with a way back to the columns; nothing kept, nothing sent but the read."""
    token = form_token(client)
    response = pick(client, token, **boxes)

    assert response.status_code == 400
    assert 'href="/indexes?source=demo&amp;database=starrocks&amp;step=columns"' in response.text
    assert state_changing_requests(broker) == [] and broker.verbs_called()[-1] == "db.index_targets"


def test_a_read_that_fails_is_said_with_the_way_to_the_password(client, broker):
    """Bug class: a column step that breaks, or shows an empty picker, when the database cannot be asked (stopped, or authentication on and no password kept): the broker's error is shown, with where to keep the password."""
    broker.answers["db.index_targets"] = broker_client.BrokerError("failed", "the action failed", details={"exit_code": 1, "errors": "the database admin password is required"})

    page = main_html(client.get("/indexes?source=demo&database=starrocks&step=columns").text)

    assert "the database admin password is required" in page and "keep the database admin password for this session" in page
    assert 'name="c0"' not in page


def test_the_command_shown_is_what_the_cli_parses():
    """Bug class: a confirm page whose command the CLI does not take (a flag create-indexes has not, the value cut by the shell)."""
    import sdp

    command = actions.TABLE["db.create_indexes"].command({"source": "demo.v2-x"})
    parsed = sdp.build_parser().parse_args(shlex.split(command)[2:])

    assert (parsed.command, parsed.db_command, parsed.source) == ("db", "create-indexes", "demo.v2-x")


def test_the_databases_page_and_a_sources_page_lead_to_the_picker(client):
    """Bug class: the form exists but no page leads to it."""
    assert 'href="/indexes"' in client.get("/databases").text
    assert 'href="/indexes?source=demo"' in client.get("/sources/demo?tab=configuration").text
