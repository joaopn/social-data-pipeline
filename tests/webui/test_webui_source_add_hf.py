"""Tests for the Hugging Face path of the Add source wizard (`webui.setup_source`), the download that follows the add (`work.Pending.then`), and Download on a source's page.

The contracts pinned here, each test naming the bug class it catches:

- Before the confirm the wizard asks the broker only reads (the schema through `source.hf_inspect`); after it, `source.add` with exactly the grouping answered, then `source.download` with the token once the add ended with exit code 0, and the outcome links to the download's run.
- The dataset token is never drawn back into a page, and nothing of it is left in the session once the confirmed action has run; without Download now, or when the add failed, no download is asked.
- The Fields step draws the terminal's grouping within the guard's 64 fields, and its suggestion of a data type's name is the terminal's.
- A source of a Hugging Face dataset not downloaded yet offers Download, a pending action with the token kept hidden.
"""

from __future__ import annotations

import json

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette.testclient")

from test_webui_pages import inline_code
from test_webui_source_add import _named_controls, post, start, step
from webui_helpers import DONE, ORIGIN, PASSWORD, RUN_ID, db_status, form_token, log_in, main_html, source_list, source_status, state_changing_requests, wait_until

from social_data_pipeline.control import client as broker_client
from social_data_pipeline.control import verbs
from social_data_pipeline.webui import actions, setup_source

PLANTED = "hf_plantedMadeUpToken63"
DATASET = "org-name/bsky"

# The schema `source.hf_inspect` answers (as `setup.hf_checked.inspect` makes it) for the dataset of tests/setup/test_hf_checked.py.
SCHEMA = {
    "dataset": DATASET,
    "groups": [
        {"configs": [{"name": "posts-2023", "rows": 10}, {"name": "posts-2024", "rows": 20}], "fields": [{"name": "id", "sql_type": "text", "hf_type": "string"}, {"name": "likes", "sql_type": "bigint", "hf_type": "int64"}, {"name": "embedding", "sql_type": None, "hf_type": "sequence<float32>"}, {"name": "lang", "sql_type": "text", "hf_type": "string"}]},
        {"configs": [{"name": "users", "rows": 5}], "fields": [{"name": "id", "sql_type": "bigint", "hf_type": "int64"}, {"name": "followers", "sql_type": "integer", "hf_type": "int32"}, {"name": "avatar", "sql_type": None, "hf_type": "image"}]},
    ],
}
# The grouping tests/setup/test_hf_checked.py writes byte for byte like the terminal (its HF_GROUPS).
HF_GROUPS = [
    {"configs": ["posts-2023", "posts-2024"], "data_type": "posts", "fields": [{"name": "id", "sql_type": "text"}, {"name": "lang", "sql_type": "text"}, {"name": "embedding", "sql_type": "text"}], "excluded": ["likes"]},
    {"configs": ["users"], "data_type": "people", "fields": [{"name": "id", "sql_type": "bigint"}, {"name": "followers", "sql_type": "integer"}, {"name": "avatar", "sql_type": "bigint"}]},
]
HF_FILES = {"name": "bsky", "dataset": DATASET, "dataset_token": PLANTED, "file_format": "parquet", "parquet_row_group_size": "", "profiles[parse]": "yes", "profiles[mongo_ingest]": "yes", "parse_workers": "4"}
HF_FIELDS = {"configs[0]": "", "dt[0]": "", "field[0][id]": "yes", "field[0][lang]": "yes", "cast[0][embedding]": "text", "configs[1]": "", "dt[1]": "people", "field[1][id]": "yes", "field[1][followers]": "yes", "cast[1][avatar]": "bigint"}
HF_DATABASES = {"mongo_collection_strategy": "per_data_type", "mongo_db_name": "", "collection[posts]": "", "collection[people]": "users", "mongo_indexes[posts]": "", "mongo_indexes[people]": "id"}
EXPECTED_ADD = {
    "platform": "custom", "name": "bsky", "hf_dataset": DATASET, "data_types": ["posts", "people"],
    "dumps_path": "data/dumps/bsky", "extracted_path": "data/extracted/bsky", "parsed_path": "data/parsed/bsky", "output_path": "data/output/bsky",
    "file_format": "parquet", "profiles": ["parse", "mongo_ingest"], "parse_workers": 4, "hf_groups": HF_GROUPS,
    "mongo_collection_strategy": "per_data_type", "mongo_collections": {"people": "users"}, "mongo_indexes": {"people": ["id"]},
}


@pytest.fixture
def app(make_app):
    return make_app(action_wait_seconds=0.5)


@pytest.fixture
def client(app, make_client, broker):
    broker.answers["source.add"] = dict(DONE)
    broker.answers["source.download"] = {"run_id": RUN_ID}
    broker.answers["source.hf_inspect"] = {"data": SCHEMA}
    broker.answers["db.status"] = db_status(configured=["mongo"])
    made = make_client(app)
    assert log_in(made).status_code == 303
    return made


def walk(client, token, files=HF_FILES):
    base = start(client, token, platform="hf")
    for name, data, following in (("files", files, "fields"), ("fields", HF_FIELDS, "databases"), ("databases", HF_DATABASES, "review")):
        response = step(client, base, name, token, **data)
        assert response.status_code == 303, (name, main_html(response.text))
        assert response.headers["location"] == f"{base}/{following}"
    return base


def _session_text(app, client):
    """Everything the session keeps (pending actions with their follow-ups, wizards, outcomes), revealed and written out."""
    work = app.sessions.get(client.cookies.get("__Host-sdp_session")).work
    kept = [actions.revealed({"fields": pending.fields, "then": list(pending.then) if pending.then else None}) for pending in work._pending.values()]
    kept += [vars(work.outcome(key)) for key in work.outcome_ids()]
    return json.dumps(kept, default=repr)


def test_the_add_and_then_its_download_are_sent_only_after_the_confirm(app, client, broker):
    """Bug class: a wizard that writes as it goes, a grouping sent other than it was answered (configs, names, fields, types, the fields left out), the token sent with the add (it stores nothing of it), or the download not started after the add, started before the add ended, or without the token; and an outcome that does not lead to the download's run."""
    token = form_token(client)
    base = walk(client, token)
    inspected = [request for request in broker.requests if request["verb"] == "source.hf_inspect"]
    assert [request["fields"]["dataset"] for request in inspected] == [DATASET] and inspected[0]["fields"]["token"] == PLANTED
    review = client.get(f"{base}/review")
    page = main_html(review.text)
    assert "python sdp.py source add <b>bsky</b> --hf <b>org-name/bsky</b>" in page and "python sdp.py source download <b>bsky</b>" in page
    assert 'name="download" value="yes" checked' in page and PLANTED not in review.text
    assert state_changing_requests(broker) == []

    pending = step(client, base, "review", token, nav="confirm", download="yes").headers["location"]
    confirm_page = main_html(client.get(pending).text)
    assert "python sdp.py source add bsky --hf=org-name/bsky" in confirm_page and "python sdp.py source download bsky" in confirm_page
    assert "is stored nowhere: not in a file, the run&#39;s record, its log or the session" in confirm_page and PLANTED not in confirm_page
    assert state_changing_requests(broker) == []

    done = post(client, pending + "/confirm", token, management_password=PASSWORD)
    assert done.status_code == 303
    wait_until(lambda: len(state_changing_requests(broker)) == 2)
    add, download = state_changing_requests(broker)
    assert add["verb"] == "source.add" and add["step_up"] is True and add["fields"] == EXPECTED_ADD
    assert verbs.validate_request("source.add", add["fields"])["hf_groups"] == HF_GROUPS
    assert (download["verb"], download["fields"], download["step_up"]) == ("source.download", {"source": "bsky", "token": PLANTED}, None)
    outcome = main_html(client.get(done.headers["location"]).text)
    assert f'href="/runs/{RUN_ID}"' in outcome and "The download was started as a run" in outcome
    assert PLANTED not in _session_text(app, client)


def test_without_download_now_or_after_a_failed_add_no_download_is_asked(app, client, broker):
    """Bug class: a download started although Download now was unticked, or after an add that failed (a source that does not exist then, or one whose config the operator must fix first); and the token left in the session in either case."""
    token = form_token(client)
    base = walk(client, token)
    pending = step(client, base, "review", token, nav="confirm").headers["location"]
    assert "source download" not in main_html(client.get(pending).text)
    post(client, pending + "/confirm", token, management_password=PASSWORD)
    wait_until(lambda: state_changing_requests(broker))
    assert [request["verb"] for request in state_changing_requests(broker)] == ["source.add"]

    broker.answers["source.add"] = {"exit_code": 1, "output": "", "errors": "Error: refused"}
    base = walk(client, token)
    pending = step(client, base, "review", token, nav="confirm", download="yes").headers["location"]
    done = post(client, pending + "/confirm", token, management_password=PASSWORD)
    wait_until(lambda: len(state_changing_requests(broker)) == 2)
    outcome = client.get(done.headers["location"])
    wait_until(lambda: "under way" not in client.get(done.headers["location"]).text)
    outcome = main_html(client.get(done.headers["location"]).text)
    assert [request["verb"] for request in state_changing_requests(broker)] == ["source.add", "source.add"]
    assert "It was not started, because the action above did not end with exit code 0." in outcome
    assert PLANTED not in _session_text(app, client)


def test_a_refused_download_is_said_on_the_outcome(app, client, broker):
    """Bug class: a download refused by the broker beside a live run of the source (or a second download) shown as started, or not shown at all."""
    broker.answers["source.download"] = broker_client.BrokerError("source_run_active", "a pipeline run of this source is alive")
    token = form_token(client)
    base = walk(client, token)
    pending = step(client, base, "review", token, nav="confirm", download="yes").headers["location"]
    done = post(client, pending + "/confirm", token, management_password=PASSWORD)
    wait_until(lambda: len(state_changing_requests(broker)) == 2)
    wait_until(lambda: "under way" not in client.get(done.headers["location"]).text)
    outcome = main_html(client.get(done.headers["location"]).text)
    assert "It was not started: A pipeline run of this source is alive" in outcome and "Download on the source's page" in outcome


def test_the_dataset_is_read_once_and_its_errors_keep_the_files_step(client, broker):
    """Bug class: a dataset id the registry refuses sent to the broker, the Hub's refusal lost (the operator sees a bare failure, or moves on without a schema), the token drawn back into the page, or the Hub asked again at every Next."""
    token = form_token(client)
    base = start(client, token, platform="hf")
    bad = step(client, base, "files", token, **{**HF_FILES, "dataset": "org/../x"})
    assert bad.status_code == 400 and "&lt;owner&gt;/&lt;name&gt;" in main_html(bad.text)
    assert [request for request in broker.requests if request["verb"] == "source.hf_inspect"] == []

    broker.answers["source.hf_inspect"] = broker_client.BrokerError("failed", "the command failed", None, {"exit_code": 1, "errors": "Error: the Hub refused access to the dataset: a private or gated dataset needs a token that has access to it\n"})
    refused = step(client, base, "files", token, **HF_FILES)
    page = main_html(refused.text)
    assert refused.status_code == 400 and "The dataset could not be read from the Hub: The Hub refused access to the dataset" in page
    assert PLANTED not in refused.text and "kept: type a new one to replace it" in page

    broker.answers["source.hf_inspect"] = {"data": SCHEMA}
    assert step(client, base, "files", token, **{**HF_FILES, "dataset_token": ""}).status_code == 303
    assert step(client, base, "files", token, **{**HF_FILES, "dataset_token": ""}).status_code == 303
    assert len([request for request in broker.requests if request["verb"] == "source.hf_inspect"]) == 2


def test_the_fields_step_draws_the_grouping_within_the_guards_fields(client, broker):
    """Bug class: a Fields step the guard refuses (over 64 fields) for a large dataset, or one that drops a group or a field without a SQL mapping: boxes while they fit, a comma-separated list per group beyond that."""
    token = form_token(client)
    base = start(client, token, platform="hf")
    assert step(client, base, "files", token, **HF_FILES).status_code == 303
    page = main_html(client.get(f"{base}/fields").text)
    assert {"field[0][id]", "field[0][likes]", "field[0][lang]", "cast[0][embedding]", "field[1][followers]", "cast[1][avatar]", "dt[0]", "configs[1]"} <= _named_controls(page)
    assert "sequence&lt;float32&gt;: no SQL mapping" in page and "posts-2024</code> (20 rows)" in page

    big = {"dataset": "org-name/big", "groups": [{"configs": [{"name": f"c{g}", "rows": None}], "fields": [{"name": f"f{n}", "sql_type": "text", "hf_type": "string"} for n in range(40)] + [{"name": "img", "sql_type": None, "hf_type": "image"}]} for g in range(8)]}
    broker.answers["source.hf_inspect"] = {"data": big}
    base = start(client, token, platform="hf")
    assert step(client, base, "files", token, **{**HF_FILES, "dataset": "org-name/big"}).status_code == 303
    page = client.get(f"{base}/fields").text
    assert len(_named_controls(main_html(page))) + 1 < 64 and "fields[7]" in _named_controls(main_html(page)) and inline_code(page) == []
    response = step(client, base, "fields", token, **{"fields[0]": "f1, f0", "configs[0]": ""})
    assert response.status_code == 303
    assert step(client, base, "fields", token, **{"fields[0]": "nope"}).status_code == 400


@pytest.mark.parametrize("configs", [["comments-2020", "comments-2021"], ["a_x", "a_y"], ["solo"], ["x-1", "y-1"], ["v1.a", "v1.b"]])
def test_the_suggested_name_is_the_terminals(configs):
    """Bug class: the literal copy of the terminal's suggestion drifting, so an empty name on the Fields step writes another data type than Enter in the terminal."""
    from social_data_pipeline.setup import source

    assert setup_source.suggest_data_type_name(configs) == source._suggest_data_type_name(configs)


def test_a_source_not_downloaded_yet_offers_download_with_a_hidden_token(client, broker):
    """Bug class: no way to download from the browser a source added without Download now, Download offered for a source that is no Hugging Face one or is downloaded already, or the token drawn on the confirm page."""
    status = source_status("demo")
    status["data"]["sources"][0].update({"hf_dataset": DATASET, "hf_downloaded": False})
    broker.answers["source.status"] = lambda fields: status
    page = main_html(client.get("/sources/demo").text)
    assert 'name="action" value="source.download"' in page and DATASET in page
    token = form_token(client)
    pending = client.post("/pending", data={"form_token": token, "action": "source.download", "source": "demo", "token": PLANTED}, headers={"origin": ORIGIN})
    assert pending.status_code == 303
    confirm_page = client.get(pending.headers["location"]).text
    assert "python sdp.py source download demo" in confirm_page and PLANTED not in confirm_page and "Management password" not in confirm_page
    post(client, pending.headers["location"] + "/confirm", token)
    wait_until(lambda: state_changing_requests(broker))
    assert state_changing_requests(broker)[0]["fields"] == {"source": "demo", "token": PLANTED}

    status["data"]["sources"][0]["hf_downloaded"] = True
    assert 'value="source.download"' not in main_html(client.get("/sources/demo").text)
    broker.answers["source.list"] = source_list()


def test_the_confirm_sentences_say_what_the_add_and_the_download_do():
    """Bug class: a confirm sentence that says less than the action does (B50-B67): the download's run, its check of the Hub's names before any write, where and how it writes, the refusals beside pipeline runs, where the token goes; and the add's Hugging Face keys. Pinned whole, written from `setup.hf_checked.download`, `hf.download_hf_files`, `hf.organize_hf_downloads` and `control.runs`."""
    assert actions.source_download_sentence({"source": "bsky", "token": actions.Hidden("x" * 8)}) == (
        "It runs as a run of its own (Runs and the Overview show it, with its log and Cancel). It asks the Hub for the dataset's list of Parquet files and refuses the whole list, before anything is written, when a config, a split or a file name in it is not a plain name or an address is not an https address on huggingface.co. "
        "Then it downloads every file into the dumps folder of bsky as <config>/<split>/<file> (each through a .partial file renamed when it is complete, skipping a file already there at the Hub's size), with the dataset's README.md beside them when the Hub has one and none is there, and copies the files of each data type into its folder of the extracted folder. "
        "The broker refuses it while a pipeline run of this source is alive (and when Docker cannot be asked), and refuses a pipeline run of this source while it runs. "
        "The token is sent with each request of this download (to the Hub, and, as the terminal sends it, on to where the Hub redirects a file), and is stored nowhere: not in a file, the run's record, its log or the session."
    )
    assert "token" not in actions.source_download_sentence({"source": "bsky"}).split("runs. ")[-1]
    sentence = actions.source_add_sentence({**EXPECTED_ADD, "profiles": ["parse"]})
    assert sentence.startswith(
        "It creates config/sources/bsky/ and writes platform.yaml, parse.yaml in it. platform.yaml names the Hugging Face dataset org-name/bsky (hf_dataset) and maps its configs to the data types (posts from posts-2023, posts-2024; people from users); the fields chosen and their SQL types are the ones read from the Hub when the wizard read the dataset, and the fields left out of each data type are kept out of MongoDB's documents (mongo_exclude_fields). The add itself asks the Hub nothing and downloads nothing. The source's folders:"
    )
