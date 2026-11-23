"""Tests for Reconfigure… and Add classifiers… (`webui.setup_source_change`, the wizard of `webui.setup_source` in its other modes) and their pending actions `source.configure` and `source.add_classifiers`.

The contracts pinned here, each test naming the bug class it catches:

- The wizard is prefilled from the source's current answers (`source.settings`): every step posted as the page draws it sends back exactly those answers, so an unchanged reconfigure writes nothing different (the setup tests show the action writes the same files for them).
- Add classifiers is the Classifiers step alone, then Review; its token goes to the broker once and is never drawn back.
- Nothing reaches the broker but reads before the confirmation; the confirm needs the management password.
- A confirm the broker refuses because a run of the source is alive shows the run and links to it.
- The confirm pages' sentences, pinned whole; the commands parse as the CLI's.
"""

from __future__ import annotations

import html
import re
import shlex
from html.parser import HTMLParser

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette.testclient")

from webui_helpers import DONE, ORIGIN, PASSWORD, RUN_ID, db_status, form_token, log_in, main_html, source_list, source_status, state_changing_requests, wait_until

import sdp
from social_data_pipeline.control import verbs
from social_data_pipeline.control.client import BrokerError
from social_data_pipeline.webui import actions

TOKEN = "hf_madeUpGatedModelToken7"
ALL_DATABASES = db_status(configured=["postgres", "mongo", "starrocks"])
# What `source.settings` answers for a custom source with every profile (`setup.source_change.settings_read`'s shape).
FORUM_ANSWERS = {
    "data_types": ["posts", "replies"], "dumps_path": "/srv/dumps/forum", "extracted_path": "data/extracted/forum", "parsed_path": "data/parsed/forum", "output_path": "data/output/forum",
    "file_format": "parquet", "parquet_row_group_size": 500000, "parse_workers": 6, "pg_prefer_lingua": False, "pg_parallel_index_workers": 3, "db_schema": "forum_s",
    "input_format": "csv", "csv_delimiter": "tab", "dump_globs": {"posts": "posts_*.csv.zst", "replies": "replies_*.bin"}, "compressions": {"replies": "gz"},
    "fields": {"posts": ["id", "body"], "replies": ["id", "text"]}, "mongo_collection_strategy": "per_data_type", "mongo_collections": {"posts": "p_coll", "replies": "replies"},
    "mongo_db_name": "forumdb", "pg_indexes": {"posts": ["id"]}, "sr_indexes": {"replies": ["id"]}, "primary_key": "id", "sr_prefer_lingua": True, "sr_buckets": 64,
    "text_columns": {"posts": ["body"], "replies": ["text"]}, "remove_strings": ["[deleted]"], "remove_patterns": [r"https?://\S+"], "gpu_fields": ["id", "author"],
    "lingua_workers": 16, "lingua_file_workers": 2, "lingua_batch_size": 2000000, "lingua_low_accuracy": True,
    "gpu_ids": [1], "ml_file_workers": 1, "ml_tokenize_workers": 3, "ml_classifier_batch_size": 16, "ml_classifiers": ["toxic_roberta", "go_emotions"], "ml_classifier_data_types": {"toxic_roberta": ["posts"]},
}
FORUM_PROFILES = ["parse", "lingua", "ml", "postgres_ingest", "postgres_ml", "mongo_ingest", "sr_ingest", "sr_ml"]


def settings(answers=FORUM_ANSWERS, profiles=FORUM_PROFILES, name="forum", **more):
    data = {"name": name, "platform": "reddit" if name == "reddit" else "custom", "hf_dataset": None, "data_types": answers.get("data_types", ["posts"]), "profiles": list(profiles), "answers": dict(answers), "typed_sr_indexes": {}, "hf_token_set": False, "left_out": [], "not_readable": None}
    data.update(more)
    return {"data": data}


@pytest.fixture
def app(make_app):
    return make_app(action_wait_seconds=0.5)


@pytest.fixture
def client(app, make_client, broker):
    broker.answers["db.status"] = ALL_DATABASES
    broker.answers["source.list"] = source_list({"name": "forum", "profiles": FORUM_PROFILES}, {"name": "other", "profiles": ["parse"]})
    broker.answers["source.settings"] = settings()
    broker.answers["source.configure"] = dict(DONE)
    broker.answers["source.add_classifiers"] = dict(DONE)
    made = make_client(app)
    assert log_in(made).status_code == 303
    return made


def post(client, path, token, **data):
    return client.post(path, data={"form_token": token, **data}, headers={"origin": ORIGIN})


class _Form(HTMLParser):
    """The fields a browser would post from the form whose action is `action`, as the page draws them: text and number inputs with their value, ticked boxes, the selected option; never a password field left empty."""

    def __init__(self, action):
        super().__init__()
        self.action, self.inside, self.fields, self.select = action, False, {}, None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "form":
            self.inside = attrs.get("action") == self.action
        if not self.inside or "disabled" in attrs:
            return
        if tag == "input" and attrs.get("name") and attrs.get("name") != "form_token":
            kind = attrs.get("type", "text")
            if kind in ("text", "number", "hidden") or (kind == "checkbox" and "checked" in attrs) or (kind == "password" and attrs.get("value")):
                self.fields[attrs["name"]] = attrs.get("value", "")
        elif tag == "select":
            self.select = attrs.get("name")
        elif tag == "option" and self.select and ("selected" in attrs or self.select not in self.fields):
            self.fields[self.select] = attrs.get("value", "")

    def handle_endtag(self, tag):
        if tag == "form":
            self.inside = False
        if tag == "select":
            self.select = None


def as_drawn(client, base, step):
    page = client.get(f"{base}/{step}")
    assert page.status_code == 200, step
    form = _Form(f"{base}/{step}")
    form.feed(page.text)
    return form.fields


def start(client, token, word, name="forum"):
    response = post(client, f"/sources/{name}/{word}", token)
    assert response.status_code == 303, main_html(response.text)
    return response.headers["location"]


def confirm(client, broker, location, token):
    response = post(client, location, token, nav="confirm")
    assert response.status_code == 303, main_html(response.text)
    pending = response.headers["location"]
    assert pending.startswith("/pending/") and state_changing_requests(broker) == []
    return pending


def test_the_prefilled_reconfigure_posts_back_the_current_answers_unchanged(client, broker):
    """Bug class: a prefill that draws a current value otherwise than the step reads it back (a list joined another way, a box not ticked, an option not selected, a map entry under another name, a default the web side invents for a field it left empty), so that Next, Next, Confirm on an untouched source sends other answers than it has, and writes other files."""
    token = form_token(client)
    location = start(client, token, "reconfigure")
    base, first = location.rsplit("/", 1)
    assert first == "files"
    assert client.get(f"{base}/origin").status_code == 404
    for step, following in (("files", "fields"), ("fields", "classifiers"), ("classifiers", "databases"), ("databases", "review")):
        response = post(client, f"{base}/{step}", token, nav="next", **as_drawn(client, base, step))
        assert response.status_code == 303, (step, main_html(response.text))
        assert response.headers["location"] == f"{base}/{following}"
    review = main_html(client.get(f"{base}/review").text)
    assert "python sdp.py source configure" in review and "postgres_ml.yaml" in review
    assert [request["verb"] for request in broker.requests if verbs.VERBS.get(request["verb"]) and verbs.VERBS[request["verb"]].cls != "read"] == []

    pending = confirm(client, broker, f"{base}/review", token)
    page = html.unescape(main_html(client.get(pending).text))
    assert "python sdp.py source configure forum" in page and actions.source_configure_sentence({"name": "forum", "profiles": FORUM_PROFILES, **FORUM_ANSWERS}) in page
    assert post(client, pending + "/confirm", token, management_password=PASSWORD).status_code == 303
    wait_until(lambda: state_changing_requests(broker))
    sent = state_changing_requests(broker)
    assert len(sent) == 1 and sent[0]["verb"] == "source.configure" and sent[0]["step_up"] is True
    assert sent[0]["fields"] == {"name": "forum", "profiles": FORUM_PROFILES, **FORUM_ANSWERS}


def test_add_classifiers_is_the_classifiers_step_alone_and_its_token_is_sent_once_never_drawn(client, broker):
    """Bug class: Add classifiers walking the whole wizard (and sending the source's other answers, which `source.add_classifiers` refuses), a token drawn back into a page, or the step's answers lost on the way to the confirm."""
    token = form_token(client)
    location = start(client, token, "add-classifiers")
    base, first = location.rsplit("/", 1)
    assert first == "classifiers"
    page = main_html(client.get(location).text)
    assert re.findall(r'<span(?: class="[a-z]+")?(?: title="[^"]*")?>(\d [A-Za-z]+)</span>', page) == ["1 Classifiers", "2 Review"]
    assert client.get(f"{base}/files").status_code == 404
    drawn = as_drawn(client, base, "classifiers")
    response = post(client, location, token, nav="next", **{**drawn, "lingua_workers": "10", "hf_token": TOKEN})
    assert response.headers["location"] == f"{base}/review"
    assert TOKEN not in client.get(f"{base}/classifiers").text and TOKEN not in client.get(f"{base}/review").text

    pending = confirm(client, broker, f"{base}/review", token)
    shown = client.get(pending).text
    assert TOKEN not in shown and "python sdp.py source add-classifiers forum" in shown and "HF_TOKEN" in shown
    assert post(client, pending + "/confirm", token, management_password=PASSWORD).status_code == 303
    wait_until(lambda: state_changing_requests(broker))
    sent = state_changing_requests(broker)
    expected = {key: FORUM_ANSWERS[key] for key in actions.CLASSIFIER_KEYS if key in FORUM_ANSWERS}
    assert sent[0]["verb"] == "source.add_classifiers"
    assert sent[0]["fields"] == {"name": "forum", **expected, "lingua_workers": 10, "hf_token": TOKEN}


def test_add_classifiers_is_not_started_for_a_source_without_lingua_or_ml(client, broker):
    """Bug class: a wizard offered for a source whose classifier files the terminal would not write (it says so and writes nothing)."""
    broker.answers["source.settings"] = settings({"data_types": ["posts"]}, ["parse"])
    response = post(client, "/sources/forum/add-classifiers", form_token(client))
    assert response.status_code == 400 and "neither the profile lingua nor ml" in main_html(response.text)


def test_a_confirm_refused_because_a_run_of_the_source_is_alive_shows_the_run_and_links_to_it(client, broker):
    """Bug class: the broker's refusal (F4 decision 4) shown as a bare sentence, so the operator cannot tell which run stands in the way; or a run id from the reply drawn as a link without being checked."""
    broker.answers["source.configure"] = BrokerError("source_run_alive", f"a run of this source is alive ({RUN_ID}, download · forum)", None, {"run_id": RUN_ID, "run_label": "download · forum"})
    token = form_token(client)
    location = start(client, token, "reconfigure")
    base = location.rsplit("/", 1)[0]
    for step in ("files", "fields", "classifiers", "databases"):
        post(client, f"{base}/{step}", token, nav="next", **as_drawn(client, base, step))
    pending = confirm(client, broker, f"{base}/review", token)

    page = main_html(post(client, pending + "/confirm", token, management_password=PASSWORD).text)

    assert "A run of this source is alive" in page
    assert f'<a href="/runs/{RUN_ID}"><code>{RUN_ID}</code></a> (download · forum). Nothing was written.' in page


def test_the_source_page_offers_reconfigure_and_add_classifiers_only_where_they_apply(client, broker):
    """Bug class: Add classifiers offered for a source without lingua or ml, or the buttons posting somewhere the wizard does not start."""
    broker.answers["source.status"] = lambda fields: source_status(fields["name"], profiles=("parse", "lingua") if fields["name"] == "forum" else ("parse",))
    forum = main_html(client.get("/sources/forum?tab=configuration").text)
    other = main_html(client.get("/sources/other?tab=configuration").text)
    assert 'action="/sources/forum/reconfigure"' in forum and 'action="/sources/forum/add-classifiers"' in forum
    assert 'action="/sources/other/reconfigure"' in other and "add-classifiers" not in other


def test_the_confirm_sentences_say_what_the_actions_write():
    """Bug class: a confirm page that says less than the action does (the .bak copies, the generic field types written again, the Reddit order, the token in .env, the folders not moved, the refusals while a run is alive, the restart and rerun notes)."""
    fields = {"name": "forum", "profiles": ["parse", "sr_ingest"], "dumps_path": "/srv/d", "hf_token": TOKEN}
    assert actions.source_configure_sentence(fields) == (
        "It writes platform.yaml, parse.yaml, starrocks.yaml in config/sources/forum/ again, from the answers, each file that exists first copied to its .bak beside it (as the terminal does). The files of profiles left out stay as they are: nothing is removed. "
        "For a source of your own files, platform.yaml's field types are written again as the generic ones (text, and integer for a few common names), as the terminal does: types refined there by hand are lost, the .bak copy keeps them. A Hugging Face source keeps its dataset, its grouping of configs into data types, its fields and their types. "
        "It writes the Hugging Face token into .env as HF_TOKEN, in place of the line of HF_TOKEN there (a commented one too), or as a new line. "
        "The source's folders: dumps /srv/d. It creates, moves or empties none of them: files in a folder no longer named stay there, and the first run of a profile creates each one that is missing. "
        "An answer left empty is the terminal's Enter: the source's current value where the terminal offers it; the GPUs default to every GPU found, and the compression of a dump pattern whose extension names none to zst, as in the terminal. "
        "The broker refuses it, before anything is written, while a run of this source is alive (a pipeline run, a download or an index build of it, or a run of it started in a terminal that Docker reports), and when Docker cannot be asked whether one is. It also refuses a folder that is a folder of another source, lies inside one or holds one (the source's own folders are its own), and an answer the terminal would not ask for these profiles. "
        "Nothing is rerun or restarted: each profile reads the new settings at its next run, and the output ends with the commands to run the pipeline, as the terminal prints them. When PostgreSQL or StarRocks is running, the output also says whether it must be restarted (db stop, then db start) to see changed folders."
    )
    reddit = actions.source_configure_sentence({"name": "reddit", "profiles": ["parse"]})
    assert "written again from the Reddit template" in reddit and "applied last" in reddit and "field types" not in reddit and "HF_TOKEN" not in reddit
    assert actions.source_add_classifiers_sentence({"name": "forum", "hf_token": TOKEN}) == (
        "It writes lingua.yaml and ml.yaml in config/sources/forum/ again, for whichever of the profiles lingua and ml the source has, each file that exists first copied to its .bak beside it (as the terminal does). "
        "It writes the Hugging Face token into .env as HF_TOKEN, in place of the line of HF_TOKEN there (a commented one too), or as a new line. "
        "An answer left empty is the terminal's Enter: the source's current value where the terminal offers it; the GPUs default to every GPU found, as in the terminal. "
        "The broker refuses it, before anything is written, while a run of this source is alive (a pipeline run, a download or an index build of it, or a run of it started in a terminal that Docker reports), and when Docker cannot be asked whether one is. It also refuses a source with neither the profile lingua nor ml, and an answer the terminal would not ask for its profiles. "
        "Nothing is rerun: the next lingua or ml run reads the new settings, and a file a classifier has already finished is skipped, not classified again (delete its output to have it redone). The output ends with the commands to run the pipeline, as the terminal prints them."
    )


@pytest.mark.parametrize("verb, words", [("source.configure", ("source", "configure")), ("source.add_classifiers", ("source", "add-classifiers"))])
def test_the_commands_shown_are_what_the_cli_parses(verb, words):
    """Bug class: a command shown that the CLI would not run as `source configure <name>` or `source add-classifiers <name>` (the name quoted otherwise, another subcommand)."""
    for name in ("forum", "x.y_z-1"):
        line = actions.TABLE[verb].command({"name": name, "profiles": ["parse"]})
        parsed = sdp.build_parser().parse_args(shlex.split(line)[2:])
        assert (parsed.command, parsed.source_command, parsed.name) == (*words, name)
