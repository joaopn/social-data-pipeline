"""Tests for the Add source wizard (`webui.setup_source`, `templates/source_add.html`) and its pending action `source.add`.

The contracts pinned here, each test naming the bug class it catches:

- Nothing reaches the broker but reads before the confirmation, and after it exactly the fields the steps were answered with (the table's check, step-up with the management password).
- A step's answers are kept across Back and Next; the wizard lives with the session's pending actions (their cap, their five minutes, never shown or confirmed as one).
- A refused answer keeps the step; a name that is a source already is refused on its step.
- Every step posts fewer fields than the guard's 64, for the largest wizard (eight data types, every profile).
- The confirm page's sentence, pinned whole; the command it shows is what the CLI parses.
"""

from __future__ import annotations

import re
import shlex

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette.testclient")

from test_webui_pages import inline_code
from webui_helpers import DONE, ORIGIN, PASSWORD, db_status, form_token, log_in, main_html, source_list, state_changing_requests, wait_until

from social_data_pipeline.control import verbs
from social_data_pipeline.webui import actions

ALL_DATABASES = db_status(configured=["postgres", "mongo", "starrocks"])


@pytest.fixture
def app(make_app):
    return make_app(action_wait_seconds=0.5)


@pytest.fixture
def client(app, make_client, broker):
    broker.answers["source.add"] = dict(DONE)
    broker.answers["db.status"] = ALL_DATABASES
    made = make_client(app)
    assert log_in(made).status_code == 303
    return made


def post(client, path, token, **data):
    return client.post(path, data={"form_token": token, **data}, headers={"origin": ORIGIN})


def start(client, token, platform="custom"):
    response = post(client, "/add-source", token, platform=platform)
    assert response.status_code == 303, response.text
    location = response.headers["location"]
    assert re.fullmatch(r"/add-source/[A-Za-z0-9_-]{22}/files", location), location
    return location.rsplit("/", 1)[0]


def step(client, base, which, token, nav="next", **data):
    return post(client, f"{base}/{which}", token, nav=nav, **data)


CUSTOM_FILES = {"name": "forum", "data_types": "posts, replies", "dumps_path": "/srv/dumps/forum", "file_format": "parquet", "parquet_row_group_size": "1000000", "profiles[parse]": "yes", "profiles[lingua]": "yes", "profiles[sr_ingest]": "yes", "parse_workers": "", "input_format": "csv", "csv_delimiter": "tab"}
CUSTOM_FIELDS = {"glob[posts]": "posts_*.csv.zst", "compression[posts]": "", "fields[posts]": "id, body", "glob[replies]": "replies_*.bin", "compression[replies]": "xz", "fields[replies]": "id, text"}
CUSTOM_CLASSIFIERS = {"text_columns[posts]": "body", "text_columns[replies]": "", "remove_strings": "", "remove_patterns": r"https?://\S+", "gpu_fields": "", "lingua_workers": "4", "lingua_file_workers": "", "lingua_batch_size": ""}
CUSTOM_DATABASES = {"sr_prefer_lingua": "yes", "sr_buckets": "", "sr_indexes[posts]": "id", "sr_indexes[replies]": "", "primary_key": "id"}

EXPECTED_FIELDS = {
    "platform": "custom", "name": "forum", "data_types": ["posts", "replies"],
    "dumps_path": "/srv/dumps/forum", "extracted_path": "data/extracted/forum", "parsed_path": "data/parsed/forum", "output_path": "data/output/forum",
    "file_format": "parquet", "parquet_row_group_size": 1000000, "profiles": ["parse", "lingua", "sr_ingest"], "input_format": "csv", "csv_delimiter": "tab",
    "dump_globs": {"posts": "posts_*.csv.zst", "replies": "replies_*.bin"}, "compressions": {"replies": "xz"}, "fields": {"posts": ["id", "body"], "replies": ["id", "text"]},
    "text_columns": {"posts": ["body"]}, "remove_patterns": [r"https?://\S+"], "lingua_workers": 4, "lingua_low_accuracy": False,
    "sr_indexes": {"posts": ["id"]}, "primary_key": "id", "sr_prefer_lingua": True,
}


def walk_custom(client, token):
    base = start(client, token)
    for name, data, following in (("files", CUSTOM_FILES, "fields"), ("fields", CUSTOM_FIELDS, "classifiers"), ("classifiers", CUSTOM_CLASSIFIERS, "databases"), ("databases", CUSTOM_DATABASES, "review")):
        response = step(client, base, name, token, **data)
        assert response.status_code == 303, (name, main_html(response.text))
        assert response.headers["location"] == f"{base}/{following}"
    return base


def test_the_wizard_sends_nothing_until_the_confirm_and_then_exactly_its_answers(client, broker):
    """Bug class: a wizard that writes as it goes (a step that calls the broker's state-changing verb), or a confirm that sends other fields than the steps were answered with: a default invented on the web side, an answer of a skipped question, a lost one. The confirmation needs the management password (step-up), and the wizard is gone once its pending action is made."""
    token = form_token(client)
    base = walk_custom(client, token)
    review = client.get(f"{base}/review")
    assert review.status_code == 200
    page = main_html(review.text)
    assert "parse.yaml" in page and "starrocks.yaml" in page and "lingua.yaml" in page
    assert "data/parsed/forum" in page
    assert state_changing_requests(broker) == []
    assert "source.add" not in broker.verbs_called()

    response = step(client, base, "review", token, nav="confirm")
    assert response.status_code == 303
    pending = response.headers["location"]
    assert pending.startswith("/pending/")
    assert client.get(f"{base}/files").status_code == 404
    assert state_changing_requests(broker) == []

    without = post(client, pending + "/confirm", token)
    assert without.status_code == 400
    done = post(client, pending + "/confirm", token, management_password=PASSWORD)
    assert done.status_code == 303
    wait_until(lambda: state_changing_requests(broker))
    sent = state_changing_requests(broker)
    assert len(sent) == 1 and sent[0]["verb"] == "source.add" and sent[0]["step_up"] is True
    assert sent[0]["fields"] == EXPECTED_FIELDS
    # What the broker then checks: the same fields pass the registry.
    assert verbs.validate_request("source.add", sent[0]["fields"]) == EXPECTED_FIELDS


def test_a_steps_answers_are_kept_across_back_and_next(client):
    """Bug class: Back losing what a step was given (the operator types it again, or confirms a default without noticing), or Next from a later step dropping an earlier step's answer."""
    token = form_token(client)
    base = start(client, token)
    assert step(client, base, "files", token, **CUSTOM_FILES).headers["location"] == f"{base}/fields"
    back = step(client, base, "fields", token, nav="back", **{"glob[posts]": "kept_*.zst"})
    assert back.headers["location"] == f"{base}/files"
    files = main_html(client.get(f"{base}/files").text)
    assert 'value="forum"' in files and 'value="posts, replies"' in files and 'value="/srv/dumps/forum"' in files
    assert re.search(r'name="profiles\[ml\]" value="yes">', files) and re.search(r'name="profiles\[sr_ingest\]" value="yes" checked>', files)
    fields = main_html(client.get(f"{base}/fields").text)
    assert 'value="kept_*.zst"' in fields


def test_a_refused_answer_keeps_the_step_and_an_existing_name_is_refused(client, broker):
    """Bug class: a step that moves on with an answer the action will refuse (the operator learns it only after the confirm), and `source.add` for a name that is a source already, which the terminal turns into a reconfigure (F4 decision 4: Reconfigure is its own action)."""
    broker.answers["source.list"] = source_list({"name": "taken", "profiles": []})
    token = form_token(client)
    base = start(client, token)
    for data, line in (({**CUSTOM_FILES, "name": "a b"}, "plain name"), ({**CUSTOM_FILES, "name": "taken"}, "exists already"), ({**CUSTOM_FILES, "name": "reddit"}, "plain name"), ({**CUSTOM_FILES, "data_types": "a, a"}, "data types")):
        response = step(client, base, "files", token, **data)
        assert response.status_code == 400
        assert line in main_html(response.text)
    assert "Reconfigure" in main_html(step(client, base, "files", token, **{**CUSTOM_FILES, "name": "taken"}).text)


def test_the_wizard_lives_with_the_pending_actions_and_is_never_one(app, client, clock):
    """Bug class: wizards outside the session's bounds (an unbounded number kept, never expiring), or a wizard's id accepted by the pending page or its confirm, which would act on half-given answers."""
    token = form_token(client)
    base = start(client, token)
    wizard_id = base.rsplit("/", 1)[1]
    assert client.get(f"/pending/{wizard_id}").status_code == 404
    assert post(client, f"/pending/{wizard_id}/confirm", token, management_password=PASSWORD).status_code == 404
    assert client.get(f"{base}/files").status_code == 200

    clock.now += 4 * 60
    assert step(client, base, "files", token, **CUSTOM_FILES).status_code == 303
    clock.now += 4 * 60
    assert client.get(f"{base}/fields").status_code == 200, "a step taken starts the five minutes again"
    clock.now += 5 * 60
    gone = client.get(f"{base}/fields")
    assert gone.status_code == 404 and "no longer kept" in gone.text

    first = start(client, token)
    for _ in range(16):
        start(client, token)
    assert client.get(f"{first}/files").status_code == 404, "the seventeenth drops the oldest, as for pending actions"


def test_cancel_forgets_the_wizard(client):
    """Bug class: a cancelled wizard kept and still reachable by its address."""
    token = form_token(client)
    base = start(client, token)
    response = post(client, f"{base}/cancel", token, nav="next")
    assert response.status_code == 303 and response.headers["location"] == "/sources"
    assert client.get(f"{base}/files").status_code == 404


def _named_controls(page):
    """The names a form of the page posts (inputs, selects, buttons with a name), each once."""
    return set(re.findall(r'<(?:input|select|textarea|button)[^>]*\sname="([^"]+)"', page))


def test_every_step_posts_fewer_fields_than_the_guard_takes(client):
    """Bug class: a step whose form has more fields than the guard's 64 (the guard answers 400 and the step can never be taken), for the largest wizard: eight data types and every profile of three databases."""
    token = form_token(client)
    base = start(client, token)
    types = ", ".join(f"t{i}" for i in range(8))
    every = {f"profiles[{profile}]": "yes" for profile in actions.VALID_PROFILES}
    files = {**CUSTOM_FILES, "data_types": types, **every}
    assert step(client, base, "files", token, **files).status_code == 303
    fields = {f"{kind}[t{i}]": value for i in range(8) for kind, value in (("glob", f"t{i}_*.zst"), ("fields", "id"))}
    assert step(client, base, "fields", token, **fields).status_code == 303
    scope = {"ml_classifiers": "toxic_roberta"}
    step(client, base, "classifiers", token, **scope)
    for name in ("files", "fields", "classifiers", "databases", "review"):
        page = client.get(f"{base}/{name}").text
        count = len(_named_controls(main_html(page))) + 1
        assert count < 64, (name, count)
        assert inline_code(page) == []


def test_the_confirm_page_says_what_source_add_does(client):
    """Bug class: a confirm sentence that says less than the action does (B50-B67): the files per profile, the .env key, the Reddit overrides, which folders are created and the refusals. Pinned whole."""
    fields = {**EXPECTED_FIELDS, "hf_token": actions.Hidden("x" * 8), "profiles": ["parse", "lingua", "ml", "sr_ingest"]}
    sentence = actions.source_add_sentence(fields)
    assert sentence == (
        "It creates config/sources/forum/ and writes platform.yaml, parse.yaml, starrocks.yaml, lingua.yaml, ml.yaml in it. "
        "It writes the Hugging Face token into .env as HF_TOKEN, in place of the line of HF_TOKEN there (a commented one too), or as a new line. "
        "The source's folders: dumps /srv/dumps/forum; extracted data/extracted/forum; parsed data/parsed/forum; output data/output/forum. It creates none of them: the first run of a profile creates each one that is missing. "
        "The broker refuses the action, before anything is written, when a source of this name exists already (change one with Reconfigure), when a folder is a folder of another source, lies inside one or holds one, and when an answer is one the terminal would not ask for these profiles. "
        "Answers left empty take the terminal's default, computed on the host as source add computes it (the CPU cores, the GPUs, the base classifier configuration). "
        "When PostgreSQL or StarRocks is running, the output says whether it must be restarted (db stop, then db start) to see the new source's folders."
    )
    reddit = actions.source_add_sentence({"name": "reddit", "platform": "reddit", "profiles": ["parse"], "reddit_indexes": {"comments": ["author"]}, "dumps_path": "a", "extracted_path": "b", "parsed_path": "c", "output_path": "d"})
    assert "Then it applies the Reddit schema, field and index answers to platform.yaml, as python sdp.py source configure reddit does." in reddit


def test_a_reddit_wizard_is_the_source_reddit_and_shows_both_commands(client, broker):
    """Bug class: a Reddit wizard that sends another name (the platform comes from the name), or hides that its field and index answers are what `source configure reddit` writes."""
    token = form_token(client)
    base = start(client, token, platform="reddit")
    files = {"data_types": "submissions, comments", "file_format": "parquet", "profiles[parse]": "yes"}
    assert step(client, base, "files", token, **files).headers["location"] == f"{base}/fields"
    assert step(client, base, "fields", token, **{"removed[submissions]": "domain"}).headers["location"] == f"{base}/databases"
    assert step(client, base, "databases", token, **{"reddit_index[comments]": "author"}).headers["location"] == f"{base}/review"
    review = main_html(client.get(f"{base}/review").text)
    assert "source configure <b>reddit</b>" in review
    pending = step(client, base, "review", token, nav="confirm").headers["location"]
    page = main_html(client.get(pending).text)
    assert "python sdp.py source add reddit" in page
    post(client, pending + "/confirm", token, management_password=PASSWORD)
    wait_until(lambda: state_changing_requests(broker))
    sent = state_changing_requests(broker)[0]["fields"]
    assert sent["name"] == "reddit" and sent["platform"] == "reddit" and sent["reddit_removed_fields"] == {"submissions": ["domain"]} and sent["reddit_indexes"] == {"comments": ["author"]}


def test_the_command_shown_is_what_the_cli_parses():
    """Bug class: a command shown that the CLI would not run as `source add <name>` (a name taken for a flag, a Hugging Face dataset implied)."""
    import sdp

    command = actions.TABLE["source.add"].command(EXPECTED_FIELDS)
    parsed = sdp.build_parser().parse_args(shlex.split(command)[2:])
    assert (parsed.command, parsed.source_command, parsed.name, parsed.hf_dataset) == ("source", "add", "forum", None)


def test_add_source_is_offered_on_the_sources_page_and_in_the_tab_bar(client):
    """Bug class: the wizard unreachable from the pages the mock-up puts it on (the Sources rail's "+ Add source", the tab bar's primary "Add source")."""
    page = client.get("/sources").text
    assert '<a class="btn sm add-source" href="/add-source">+ Add source</a>' in page
    assert '<a class="btn sm primary" href="/add-source">Add source</a>' in page
    origin = client.get("/add-source")
    assert origin.status_code == 200 and "Reddit dumps" in origin.text and "Your own files" in origin.text


@pytest.mark.parametrize("pattern", ["a_*.tar.gz", "a_*.TGZ", "RC_*.zst", "t_*.json.gz", "t_*.csv.gz", "d_*.csv.xz", "r_*.bin", "r_*", "r_*.zstd", "x.gz.bak"])
def test_the_wizards_compression_detection_is_the_terminals(pattern):
    """Bug class: the wizard's copy of the extension rule drifting from the terminal's (`utils.detect_compression_from_glob`), so a compression is refused where the terminal would ask it, or sent where the action then refuses it after the confirm."""
    from social_data_pipeline.setup import utils
    from social_data_pipeline.webui import setup_source

    assert setup_source.compression_of(pattern) == utils.detect_compression_from_glob(pattern)
