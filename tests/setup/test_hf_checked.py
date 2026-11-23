"""Tests for the Hugging Face steps without a terminal (`setup.hf_checked`): the schema read for the browser (`sdp.get_source_hf_inspect`), the add of a Hugging Face source (`sdp.do_source_add` with `hf_dataset`), and the checked download (`sdp.do_source_download`).

The contracts pinned here, each test naming the bug class it catches:

- The schema the browser is given is the terminal's grouping of the same Hub reply, with bounds: on the bytes read, the groups, configs and fields, and names that are plain.
- The same grouping answers write the same files: `source add <name> --hf <id>` is driven with scripted answers and the action is called with the answers the wizard sends, both against one stubbed Hub reply, and the files are compared byte for byte.
- A download refuses a Hub reply that names a folder or file that is not plain (a parent reference, a separator, an address elsewhere) before it writes anything, and downloads the rest as the terminal does.

The Hub is a stand-in for `urllib.request.urlopen`, shared by the terminal's path and the action's; nothing reaches the network.
"""

from __future__ import annotations

import io
import json
import urllib.error

import pytest
from test_source_add import HW_NONE, Script, _files, _paths, _point_at, _project

import sdp
from social_data_pipeline.setup import hf, hf_checked, source, utils

DATASET = "org-name/bsky"
PLANTED = "hf_plantedMadeUpToken77"

# Two schema groups, as the datasets server answers `/info`: posts of two years, and users. `id` is a string in one group and an integer in the other (the terminal keeps the first type), `embedding` and `avatar` have no SQL mapping.
SERVER_INFO = {
    "dataset_info": {
        "posts-2023": {"features": {"id": {"dtype": "string", "_type": "Value"}, "likes": {"dtype": "int64", "_type": "Value"}, "embedding": {"feature": {"dtype": "float32", "_type": "Value"}, "_type": "Sequence"}, "lang": {"dtype": "string", "_type": "Value"}}, "splits": {"train": {"num_examples": 10}}},
        "posts-2024": {"features": {"id": {"dtype": "string", "_type": "Value"}, "likes": {"dtype": "int64", "_type": "Value"}, "embedding": {"feature": {"dtype": "float32", "_type": "Value"}, "_type": "Sequence"}, "lang": {"dtype": "string", "_type": "Value"}}, "splits": {"train": {"num_examples": 20}}},
        "users": {"features": {"id": {"dtype": "int64", "_type": "Value"}, "followers": {"dtype": "int32", "_type": "Value"}, "avatar": {"_type": "Image"}}, "splits": {"train": {"num_examples": 5}}},
    }
}

# What the wizard sends for: both post configs, the suggested name, the fields id and lang, `embedding` as text; the users config named `people`, every field, `avatar` as bigint.
HF_GROUPS = [
    {"configs": ["posts-2023", "posts-2024"], "data_type": "posts", "fields": [{"name": "id", "sql_type": "text"}, {"name": "lang", "sql_type": "text"}, {"name": "embedding", "sql_type": "text"}], "excluded": ["likes"]},
    {"configs": ["users"], "data_type": "people", "fields": [{"name": "id", "sql_type": "bigint"}, {"name": "followers", "sql_type": "integer"}, {"name": "avatar", "sql_type": "bigint"}]},
]


class Response(io.BytesIO):
    def __init__(self, data, headers=None):
        super().__init__(data)
        self.headers = headers or {"Content-Length": str(len(data))}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class Hub:
    """Stands in for `urllib.request.urlopen`: each address (without its query for the files) answers bytes, or raises; every request is recorded with its headers and method."""

    def __init__(self, answers):
        self.answers = answers
        self.requests = []

    def __call__(self, request, timeout=None):
        url = request.full_url
        self.requests.append((request.get_method(), url, dict(request.header_items())))
        answer = self.answers.get(url)
        if answer is None:
            raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)
        if isinstance(answer, BaseException):
            raise answer
        return Response(answer)


def _hub(monkeypatch, answers):
    hub = Hub(answers)
    monkeypatch.setattr("urllib.request.urlopen", hub)
    return hub


INFO_URL = f"{hf.HF_DATASETS_SERVER}/info?dataset={DATASET}"


# ── the schema read for the browser ─────────────────────────────────────────


def test_the_browser_is_given_the_terminals_grouping_of_the_same_reply(monkeypatch):
    """Bug class: the wizard offering other groups, another field order or other SQL types than `source add --hf` asks about for the same dataset, so the same choices write another config."""
    _hub(monkeypatch, {INFO_URL: json.dumps(SERVER_INFO).encode()})
    terminal = hf.extract_hf_defaults(DATASET)

    shown = sdp.get_source_hf_inspect(dataset=DATASET)

    assert shown["dataset"] == DATASET
    assert [[config["name"] for config in group["configs"]] for group in shown["groups"]] == [[config["name"] for config in group["configs"]] for group in terminal["schema_groups"]]
    assert [[(field["name"], field["sql_type"], field["hf_type"]) for field in group["fields"]] for group in shown["groups"]] == [[(field["name"], field["sql_type"], field["hf_type"]) for field in fields] for fields in terminal["fields_by_group"]]
    assert shown["groups"][0]["configs"][1] == {"name": "posts-2024", "rows": 20}
    json.dumps(shown)


@pytest.mark.parametrize("info, line", [
    ({"dataset_info": {f"c{n}": {"features": {f"f{n}": {"dtype": "string", "_type": "Value"}}} for n in range(9)}}, "more schema groups than the browser takes (8)"),
    ({"dataset_info": {"default": {"features": {f"f{n}": {"dtype": "string", "_type": "Value"} for n in range(257)}}}}, "more configs (256) or fields (256)"),
    ({"dataset_info": {"default": {"features": {"Unnamed: 0": {"dtype": "string", "_type": "Value"}}}}}, "not a plain name"),
    ({"dataset_info": {"../up": {"features": {"id": {"dtype": "string", "_type": "Value"}}}}}, "not a plain name"),
    ({"dataset_info": {}}, "has no configs"),
    ({"dataset_info": ["not", "a", "dict"]}, "expected shape"),
    ({"dataset_info": {"default": {"features": ["id"]}}}, "expected shape"),
])
def test_a_dataset_beyond_the_browsers_bounds_is_refused_with_a_line(monkeypatch, info, line):
    """Bug class: a dataset the wizard cannot draw within the guard's 64 fields or the request's size (too many groups, configs or fields), a name that would become a folder or a YAML key the terminal's prompts never produce, or a reply of another shape, reaching the page or crashing the read; each refused with a line that says what to do, holding no value of the reply."""
    _hub(monkeypatch, {INFO_URL: json.dumps(info).encode()})

    with pytest.raises(sdp.ActionError) as exc:
        sdp.get_source_hf_inspect(dataset=DATASET)

    assert line in exc.value.message and "../up" not in exc.value.message


def test_a_reply_larger_than_the_bound_is_not_read_whole(monkeypatch):
    """Bug class: a Hub reply (or something answering in its place) of any size read into the broker's child: the read stops one byte past the bound."""
    monkeypatch.setattr(hf_checked, "INFO_MAX_BYTES", 100)
    reads = []

    class Big(Response):
        def read(self, size=-1):
            reads.append(size)
            return super().read(size)

    monkeypatch.setattr("urllib.request.urlopen", lambda request, timeout=None: Big(b"{" + b" " * 10000 + b"}"))

    with pytest.raises(sdp.ActionError) as exc:
        sdp.get_source_hf_inspect(dataset=DATASET)

    assert "larger than the browser takes" in exc.value.message and reads == [101]


@pytest.mark.parametrize("error, line", [
    (urllib.error.HTTPError(INFO_URL, 404, "Not Found", {}, None), "has no such dataset, or it is private"),
    (urllib.error.HTTPError(INFO_URL, 401, "Unauthorized", {}, None), "needs a token that has access"),
    (urllib.error.HTTPError(INFO_URL, 500, "Server Error", {}, None), "HTTP 500"),
    (urllib.error.URLError("name resolution failed for huggingface.co"), "could not be reached (a network error)"),
    (TimeoutError("timed out"), "did not answer in time"),
])
def test_the_hubs_errors_are_clear_refusals(monkeypatch, error, line):
    """Bug class: a network or Hub error surfacing as a traceback type name ("the request failed (URLError)") instead of what to do, or as the terminal's "Dataset has no configs" for a dataset that only needs a token."""
    _hub(monkeypatch, {INFO_URL: error})

    with pytest.raises(sdp.ActionError) as exc:
        sdp.get_source_hf_inspect(dataset=DATASET, token=PLANTED)

    assert line in exc.value.message and PLANTED not in exc.value.message


def test_the_token_goes_to_the_hub_and_nowhere_else(monkeypatch, capsys):
    """Bug class: the dataset token printed or returned (the read's answer reaches the browser), or not sent, so a private dataset cannot be read."""
    hub = _hub(monkeypatch, {INFO_URL: json.dumps(SERVER_INFO).encode()})

    shown = sdp.get_source_hf_inspect(dataset=DATASET, token=PLANTED)

    assert hub.requests[0][2]["Authorization"] == f"Bearer {PLANTED}"
    assert PLANTED not in json.dumps(shown) and PLANTED not in "".join(capsys.readouterr())


# ── the add ─────────────────────────────────────────────────────────────────


def test_a_hugging_face_add_writes_the_terminals_files_for_the_same_grouping(tmp_path, monkeypatch):
    """Bug class: the action making other settings than `_run_hf_config_grouping` for the same choices (the data types' order, the configs per data type, the fields chosen, a type mapped otherwise, the first type not kept on a conflict, the fields left out missing from mongo_exclude_fields, the Parquet input and patterns), which writes a platform.yaml the terminal never would."""
    _hub(monkeypatch, {INFO_URL: json.dumps(SERVER_INFO).encode()})
    steps = [
        # Schema A: every config, the suggested name, the fields id and lang, embedding cast to text.
        ("Select", ""), ("Data type name", ""), ("Select", "1,3"), ("Choice", "2"),
        # Schema B: the config, named people, every field, avatar as a custom type.
        ("Select", ""), ("Data type name", "people"), ("Select", ""), ("Choice", "3"), ("SQL type for 'avatar'", "bigint"),
        ("Dumps directory", "data/dumps/bsky"), ("Extracted directory", "data/extracted/bsky"), ("Parsed directory", "data/parsed/bsky"), ("Output directory", "data/output/bsky"),
        ("Choice", "parquet"), ("row-group", ""),
        ("Select", "1,4,6"),
        ("Parse workers", "4"),
        ("Use Lingua", "n"), ("Parallel index workers", "2"),
        ("PostgreSQL schema", ""),
        ("Choice", "per_data_type"), ("collection name for 'posts'", ""), ("collection name for 'people'", "users"),
        ("MongoDB database name", ""),
        ("PostgreSQL indexes for 'posts'", "id"), ("PostgreSQL indexes for 'people'", ""),
        ("MongoDB indexes for 'posts'", ""), ("MongoDB indexes for 'people'", "id"),
        ("Primary key", "id"),
        ("Write these files", "y"),
    ]
    terminal_root = _project(tmp_path / "terminal", databases=("postgres", "mongo"))
    action_root = _project(tmp_path / "action", databases=("postgres", "mongo"))
    _point_at(monkeypatch, terminal_root, HW_NONE)
    script = Script(steps)
    monkeypatch.setattr("builtins.input", script)
    source.main(source_name="bsky", hf_dataset_id=DATASET)
    assert script.steps == []

    _point_at(monkeypatch, action_root, HW_NONE)
    monkeypatch.setattr("builtins.input", lambda *a: pytest.fail("the action asked a question"))
    hub = _hub(monkeypatch, {})
    answers = dict(
        name="bsky", platform="custom", data_types=["posts", "people"], **_paths("bsky"), file_format="parquet", profiles=["parse", "postgres_ingest", "mongo_ingest"],
        parse_workers=4, pg_prefer_lingua=False, pg_parallel_index_workers=2, mongo_collection_strategy="per_data_type", mongo_collections={"people": "users"},
        pg_indexes={"posts": ["id"]}, mongo_indexes={"people": ["id"]}, primary_key="id", hf_dataset=DATASET, hf_groups=HF_GROUPS,
    )
    assert sdp.do_source_add(**answers) == 0

    terminal, action = _files(terminal_root, "bsky"), _files(action_root, "bsky")
    assert terminal == action
    platform = utils.load_source_config("bsky")
    assert platform["hf_config_map"] == {"posts": ["posts-2023", "posts-2024"], "people": ["users"]} and platform["mongo_exclude_fields"] == {"posts": ["likes"]}
    assert platform["field_types"]["id"] == "text" and platform["field_types"]["avatar"] == "bigint"
    # The add asks the Hub nothing: the schema came with the answers.
    assert hub.requests == []


@pytest.mark.parametrize("change, line", [
    ({"dump_globs": {"posts": "x_*.zst"}}, "'dump_globs' is not asked for a Hugging Face dataset"),
    ({"hf_groups": [{**HF_GROUPS[0], "configs": []}, HF_GROUPS[1]]}, "at least one config"),
    ({"hf_groups": [HF_GROUPS[0], {**HF_GROUPS[1], "configs": ["posts-2024"]}]}, "in two groups"),
    ({"hf_groups": [{**HF_GROUPS[0], "fields": []}, HF_GROUPS[1]]}, "at least one field"),
    ({"hf_groups": [{**HF_GROUPS[0], "excluded": ["lang"]}, HF_GROUPS[1]]}, "both chosen and left out"),
    ({"data_types": ["people", "posts"]}, "names of the Hugging Face groups"),
])
def test_a_grouping_the_terminal_cannot_make_is_refused_before_anything_is_written(tmp_path, monkeypatch, change, line):
    """Bug class: a Hugging Face add with answers the terminal's grouping never produces (a custom question it does not ask, a group without configs or fields, a config in two data types, a field both kept and excluded, data types out of the groups' order) written as a config the pipeline then misreads."""
    root = _project(tmp_path / "p", databases=("postgres",))
    _point_at(monkeypatch, root, HW_NONE)
    answers = dict(name="bsky", platform="custom", data_types=["posts", "people"], **_paths("bsky"), file_format="parquet", profiles=["parse"], hf_dataset=DATASET, hf_groups=HF_GROUPS)

    with pytest.raises(sdp.ActionError) as exc:
        sdp.do_source_add(**{**answers, **change})

    assert line in exc.value.message
    assert not (root / "config" / "sources" / "bsky").exists()


# ── the download ────────────────────────────────────────────────────────────

LIST_URL = f"{hf.HF_API_BASE}/{DATASET}/parquet"
FILE_URL = f"https://huggingface.co/api/datasets/{DATASET}/parquet/posts-2023/train/0.parquet"
README_URL = f"https://huggingface.co/datasets/{DATASET}/raw/main/README.md"


def _download_project(tmp_path, monkeypatch):
    root = _project(tmp_path / "p", databases=("postgres",))
    _point_at(monkeypatch, root, HW_NONE)
    folder = root / "config" / "sources" / "bsky"
    folder.mkdir(parents=True)
    (folder / "platform.yaml").write_text(
        f"hf_dataset: {DATASET}\nhf_config_map:\n  posts: [posts-2023]\ndata_types: [posts]\npaths:\n  dumps: data/dumps/bsky\n  extracted: data/extracted/bsky\n  parsed: data/parsed/bsky\n  output: data/output/bsky\n"
    )
    return root


@pytest.mark.parametrize("listing", [
    {"posts-2023": {"train": [FILE_URL]}, "..": {"train": [FILE_URL]}},
    {"posts-2023": {"../../etc": [FILE_URL]}},
    {"posts-2023": {"train": [FILE_URL]}, "a/b": {"train": [FILE_URL]}},
    {"posts-2023": {"train": [FILE_URL]}, "README.md": {"train": [FILE_URL]}},
    {"posts-2023": {"train": ["https://huggingface.co/api/x/..%2F..%2F..%2Fescape.parquet"]}},
    {"posts-2023": {"train": ["https://huggingface.co/api/x/%2Fetc%2Fpasswd"]}},
    {"posts-2023": {"train": ["https://evil.example/api/0.parquet"]}},
    {"posts-2023": {"train": ["http://huggingface.co/api/0.parquet"]}},
    {"posts-2023": {"train": ["https://huggingface.co@evil.example/0.parquet"]}},
    {"posts-2023": {"train": [FILE_URL, FILE_URL]}},
    {"posts-2023": {"train": "not a list"}},
    ["not", "a", "dict"],
])
def test_a_download_refuses_hostile_names_from_the_hub_before_writing_anything(tmp_path, monkeypatch, capsys, listing):
    """Bug class (security: the registry's stated blocker): a Hub reply naming a config, a split or a file that `download_hf_files` would turn into a path outside the source's dumps folder (a parent reference, a separator, an escaped slash in the address), a config that collides with the README written beside them, an address that would send the token to another host or in clear text, or one file twice. Refused before any folder or file is made and before any file is asked for."""
    root = _download_project(tmp_path, monkeypatch)
    hub = _hub(monkeypatch, {LIST_URL: json.dumps(listing).encode()})

    assert sdp.do_source_download(source="bsky", token=PLANTED) == 1

    assert not (root / "data").exists() and not (tmp_path / "etc").exists() and not (tmp_path / "escape.parquet").exists()
    assert [url for _, url, _ in hub.requests] == [LIST_URL]
    out = capsys.readouterr().out
    assert "nothing was written" in out and PLANTED not in out


def test_a_download_of_a_plain_reply_mirrors_the_hub_as_the_terminal_does(tmp_path, monkeypatch, capsys):
    """Bug class: the checked download writing elsewhere or otherwise than `source download` (the dumps mirror `<config>/<split>/<file>`, the README, the copy into extracted/<data type>), a `.partial` file left behind, or the token not sent with each request to the Hub."""
    root = _download_project(tmp_path, monkeypatch)
    hub = _hub(monkeypatch, {LIST_URL: json.dumps({"posts-2023": {"train": [FILE_URL]}}).encode(), FILE_URL: b"PAR1data", README_URL: b"# card"})

    assert sdp.do_source_download(source="bsky", token=PLANTED) == 0

    dumps = root / "data" / "dumps" / "bsky"
    assert (dumps / "posts-2023" / "train" / "0.parquet").read_bytes() == b"PAR1data" and (dumps / "README.md").read_bytes() == b"# card"
    assert not list(dumps.rglob("*.partial"))
    assert (root / "data" / "extracted" / "bsky" / "posts" / "train_0.parquet").read_bytes() == b"PAR1data"
    assert all(headers.get("Authorization") == f"Bearer {PLANTED}" for _, _, headers in hub.requests)
    out = capsys.readouterr().out
    assert "Next step: python sdp.py run parse --source bsky" in out and PLANTED not in out
