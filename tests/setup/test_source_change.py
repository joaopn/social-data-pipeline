"""Tests for `sdp.do_source_configure`, `sdp.do_source_add_classifiers` and `sdp.get_source_settings`, the non-interactive `source configure`, `source add-classifiers` and the read of a source's current answers (`setup.source_change`).

The contracts pinned here, each test naming the bug class it catches:

- The same answers give the same files: a source is added the same way in two projects, the terminal's questionnaire (`source.main` answered yes at "Reconfigure?", `reddit.main`, `classifiers.main`) is driven with scripted answers in one, the action is called with the same answers in the other, and every file of the source (the `.bak` copies included) and `.env` are compared byte for byte. Answers left out are the terminal's Enter, whose default on a reconfigure is the current value.
- The read's answers, sent back unchanged, write the same files again, and the read holds no secret.
- A folder of another source is refused before anything is written, the source's own folders are not.

The projects, the fixed machines and the scripted terminal are those of `test_source_add`.
"""

from __future__ import annotations

import json

import pytest
from test_hf_checked import DATASET, HF_GROUPS
from test_source_add import HW_GPUS, HW_NONE, TOKEN, Script, _files, _paths, _point_at, _project

import sdp
from social_data_pipeline.control import verbs
from social_data_pipeline.setup import classifiers, reddit, source, utils


def _add(monkeypatch, root, hw, **answers):
    _point_at(monkeypatch, root, hw)
    monkeypatch.setattr("builtins.input", lambda *a: pytest.fail("the action asked a question"))
    assert sdp.do_source_add(**answers) == 0


def _drive(monkeypatch, root, hw, steps, run):
    """Answer the terminal's prompts of `run()` with `steps`, in the project `root`."""
    _point_at(monkeypatch, root, hw)
    script = Script(steps)
    monkeypatch.setattr("builtins.input", script)
    monkeypatch.setattr(utils.getpass, "getpass", script)
    run()
    assert script.steps == []


def _act(monkeypatch, root, hw, function, **answers):
    _point_at(monkeypatch, root, hw)
    monkeypatch.setattr("builtins.input", lambda *a: pytest.fail("the action asked a question"))
    monkeypatch.setattr(utils.getpass, "getpass", lambda *a: pytest.fail("the action asked a question"))
    assert function(**answers) == 0


def _twins(tmp_path, monkeypatch, hw, added, **project):
    """Two projects with the same source added (`added`: the answers of `do_source_add`)."""
    roots = _project(tmp_path / "terminal", **project), _project(tmp_path / "action", **project)
    for root in roots:
        _add(monkeypatch, root, hw, **added)
    return roots


FORUM = dict(
    name="forum", platform="custom", data_types=["posts", "replies"], **_paths("forum"), file_format="parquet", profiles=["parse", "lingua", "sr_ingest"],
    dump_globs={"posts": "posts_*.zst", "replies": "replies_*.bin"}, compressions={"replies": "xz"}, fields={"posts": ["id", "body"], "replies": ["id", "text"]},
    sr_indexes={"posts": ["id"]}, primary_key="id", text_columns={"posts": ["body"]}, lingua_workers=4,
)


def test_a_custom_source_reconfigured_with_other_profiles_and_folders_writes_the_terminals_files(tmp_path, monkeypatch):
    """Bug class: the reconfigure building other settings than the terminal's questionnaire for the same answers: a default taken from the fresh source where the terminal offers the current value (a pattern, a field list, the indexes, the primary key, a Mongo name, the Lingua settings), a profile's file not written or written though not chosen, the folders of the source itself taken for another source's."""
    terminal_root, action_root = _twins(tmp_path, monkeypatch, HW_NONE, FORUM)
    steps = [
        ("Reconfigure", "y"),
        ("Data types", ""),
        ("Dumps directory", "/srv/dumps/forum"), ("Extracted directory", ""), ("Parsed directory", ""), ("Output directory", "./data/out/forum"),
        ("Choice", ""), ("row-group", ""),
        # parse, lingua, mongo_ingest, sr_ingest: MongoDB added.
        ("Select", "1,2,6,7"),
        ("Parse workers", "3"),
        ("Choice", ""),
        ("glob pattern for posts", ""), ("glob pattern for replies", ""), ("Choice", "xz"),
        ("Fields for 'posts'", ""), ("Fields for 'replies'", ""),
        ("Choice", ""), ("collection name for 'posts'", ""), ("collection name for 'replies'", "r_coll"), ("MongoDB database name", ""),
        ("MongoDB indexes for 'posts'", "id"), ("MongoDB indexes for 'replies'", ""),
        ("StarRocks BITMAP indexes for 'posts'", ""), ("StarRocks BITMAP indexes for 'replies'", ""),
        ("Primary key", ""),
        ("Use Lingua", ""), ("buckets", ""),
        ("Text columns for 'posts'", ""), ("Text columns for 'replies'", "text"), ("Exact strings", ""), ("Regex patterns", ""), ("Extra fields", ""),
        ("Lingua workers", ""), ("Lingua file workers", "3"), ("Lingua batch size", ""), ("Low accuracy", ""),
        ("Write these files", "y"),
    ]
    _drive(monkeypatch, terminal_root, HW_NONE, steps, lambda: source.main(source_name="forum"))
    _act(
        monkeypatch, action_root, HW_NONE, sdp.do_source_configure,
        name="forum", profiles=["parse", "lingua", "mongo_ingest", "sr_ingest"], dumps_path="/srv/dumps/forum", output_path="data/out/forum", parse_workers=3,
        compressions={"replies": "xz"}, mongo_collections={"replies": "r_coll"}, mongo_indexes={"posts": ["id"]}, text_columns={"replies": ["text"]}, lingua_file_workers=3,
    )

    terminal, action = _files(terminal_root, "forum"), _files(action_root, "forum")
    assert "mongo.yaml" in action and "platform.yaml.bak" in action
    assert action == terminal
    assert b"dumps: /srv/dumps/forum" in action["platform.yaml"] and b"primary_key: id" in action["platform.yaml"]


def test_reddit_reconfigured_with_its_overrides_writes_the_terminals_files(tmp_path, monkeypatch):
    """Bug class: the Reddit reconfigure built like a custom one, or its schema, field and index answers applied otherwise than `source configure reddit` applies them, or lost when platform.yaml is written again from the template."""
    import yaml

    added = dict(name="reddit", platform="reddit", data_types=["submissions", "comments"], **_paths("reddit"), file_format="parquet", profiles=["parse", "sr_ingest"], sr_buckets=32, reddit_indexes={"comments": ["author"]})
    terminal_root, action_root = _twins(tmp_path, monkeypatch, HW_NONE, added, databases=("starrocks",))
    template = yaml.safe_load((terminal_root / "config/templates/reddit.yaml").read_text())
    comment_fields = template["fields"]["comments"]
    removed = [comment_fields[2]]
    kept_indices = ",".join(str(i + 1) for i, field in enumerate(comment_fields) if field not in removed)
    steps = [
        ("Reconfigure", "y"),
        ("Data types", ""),
        ("Dumps directory", ""), ("Extracted directory", ""), ("Parsed directory", "/srv/parsed/reddit"), ("Output directory", ""),
        ("Choice", ""), ("row-group", "250000"),
        # Only StarRocks is set up: parse, lingua, ml, sr_ingest, sr_ml.
        ("Select", "1,2,4"),
        ("Parse workers", ""),
        ("Use Lingua", "y"), ("buckets", ""),
        ("Lingua workers", "6"), ("Lingua file workers", ""), ("Lingua batch size", ""), ("Low accuracy", ""),
        ("Write these files", "y"),
    ]
    reddit_steps = [
        ("Database schema name", "rd"),
        ("Remove fields", "y"), ("Select", ""), ("Select", kept_indices),
        ("Customize database indexes", "y"), ("Submissions index columns", "author, subreddit"), ("Comments index columns", "link_id"),
        ("Apply these overrides", "y"),
    ]

    def terminal():
        source.main(source_name="reddit")
        script.steps[:] = reddit_steps
        reddit.main(source_name="reddit")

    _point_at(monkeypatch, terminal_root, HW_NONE)
    script = Script(steps)
    monkeypatch.setattr("builtins.input", script)
    terminal()
    assert script.steps == []
    _act(
        monkeypatch, action_root, HW_NONE, sdp.do_source_configure,
        name="reddit", profiles=["parse", "lingua", "sr_ingest"], parsed_path="/srv/parsed/reddit", parquet_row_group_size=250000, sr_prefer_lingua=True, lingua_workers=6,
        reddit_db_schema="rd", reddit_removed_fields={"comments": removed}, reddit_indexes={"submissions": ["author", "subreddit"], "comments": ["link_id"]},
    )

    terminal_files, action_files = _files(terminal_root, "reddit"), _files(action_root, "reddit")
    assert action_files == terminal_files
    written = yaml.safe_load(action_files["platform.yaml"])
    assert written["db_schema"] == "rd" and removed[0] not in written["fields"]["comments"] and written["indexes"]["comments"] == ["link_id"]


def test_a_hugging_face_source_reconfigured_keeps_its_dataset_and_grouping_as_the_terminal_does(tmp_path, monkeypatch):
    """Bug class: a Hugging Face source reconfigured like a source of your own files (asked data types, patterns or fields, its field types reset to the generic ones) or losing hf_dataset, hf_config_map or mongo_exclude_fields, after which `source download` and the loads break."""
    added = dict(name="bsky", platform="custom", data_types=["posts", "people"], **_paths("bsky"), file_format="parquet", profiles=["parse"], hf_dataset=DATASET, hf_groups=HF_GROUPS)
    terminal_root, action_root = _twins(tmp_path, monkeypatch, HW_NONE, added, databases=("starrocks",))
    steps = [
        ("Reconfigure", "y"),
        ("Dumps directory", ""), ("Extracted directory", ""), ("Parsed directory", "./data/parsed/bsky2"), ("Output directory", ""),
        ("Choice", ""), ("row-group", ""),
        ("Select", "1,4"),
        ("Parse workers", "2"),
        ("StarRocks BITMAP indexes for 'posts'", "lang"), ("StarRocks BITMAP indexes for 'people'", ""),
        ("Primary key", "id"),
        ("Use Lingua", ""), ("buckets", "16"),
        ("Write these files", "y"),
    ]
    _drive(monkeypatch, terminal_root, HW_NONE, steps, lambda: source.main(source_name="bsky"))
    answers = dict(name="bsky", profiles=["parse", "sr_ingest"], parsed_path="data/parsed/bsky2", parse_workers=2, sr_indexes={"posts": ["lang"]}, primary_key="id", sr_buckets=16)
    _act(monkeypatch, action_root, HW_NONE, sdp.do_source_configure, **answers)

    terminal, action = _files(terminal_root, "bsky"), _files(action_root, "bsky")
    assert action == terminal
    platform = utils.load_source_config("bsky")
    assert platform["hf_dataset"] == DATASET and platform["hf_config_map"] == {"posts": ["posts-2023", "posts-2024"], "people": ["users"]} and platform["mongo_exclude_fields"] == {"posts": ["likes"]}
    assert platform["field_types"]["avatar"] == "bigint"
    with pytest.raises(sdp.ActionError, match="'data_types' is not asked for a Hugging Face source"):
        sdp.do_source_configure(**answers, data_types=["posts"])


def test_add_classifiers_with_lingua_a_gpu_classifier_and_a_token_writes_the_terminals_files(tmp_path, monkeypatch):
    """Bug class: `source add-classifiers` without a terminal building other classifier settings than its questionnaire (the current text columns, strings, patterns and extra fields not offered as the defaults, the GPUs chosen by index or label otherwise, a classifier's data types lost), or the token not written to .env as HF_TOKEN as the terminal writes it."""
    available = classifiers.compute_classifier_defaults(HW_GPUS, [])["ml_classifiers"]
    added = dict(
        name="forum", platform="custom", data_types=["posts", "replies"], **_paths("forum"), file_format="parquet", profiles=["parse", "lingua", "ml", "sr_ingest"],
        dump_globs={"posts": "posts_*.zst", "replies": "replies_*.zst"}, fields={"posts": ["id", "body"], "replies": ["id", "text"]}, primary_key="id",
        text_columns={"posts": ["body"]}, remove_strings=["[deleted]"], gpu_fields=["author"], ml_classifiers=[available[0]],
    )
    terminal_root, action_root = _twins(tmp_path, monkeypatch, HW_GPUS, added, databases=("starrocks",))
    steps = [
        ("Text columns for 'posts'", ""), ("Text columns for 'replies'", "text"),
        ("Exact strings", ""), ("Regex patterns", r"https?://\S+, #\w+"), ("Extra fields", ""),
        ("Lingua workers", "10"), ("Lingua file workers", ""), ("Lingua batch size", "5000"), ("Low accuracy", "y"),
        ("Select", "2"),
        ("File workers", ""), ("Tokenize workers", "2"), ("Classifier batch size", ""),
        ("Select", f"1,{len(available)}" if len(available) > 1 else "1"),
        # The data types of each classifier chosen: the first keeps its current ones (all), the last runs on replies only.
        *[("Select", "2" if name == available[-1] and len(available) > 1 else "") for name in dict.fromkeys([available[0], available[-1]])],
        ("HuggingFace token", TOKEN),
        ("Write these files", "y"),
    ]
    _drive(monkeypatch, terminal_root, HW_GPUS, steps, lambda: classifiers.main(source_name="forum"))
    chosen = list(dict.fromkeys([available[0], available[-1]]))
    _act(
        monkeypatch, action_root, HW_GPUS, sdp.do_source_add_classifiers,
        name="forum", text_columns={"replies": ["text"]}, remove_patterns=[r"https?://\S+", r"#\w+"], lingua_workers=10, lingua_batch_size=5000, lingua_low_accuracy=True,
        gpu_ids=[1], ml_tokenize_workers=2, ml_classifiers=chosen, ml_classifier_data_types={available[-1]: ["replies"]} if len(available) > 1 else {}, hf_token=TOKEN,
    )

    terminal, action = _files(terminal_root, "forum"), _files(action_root, "forum")
    assert {"lingua.yaml", "ml.yaml", "lingua.yaml.bak", "ml.yaml.bak"} <= set(action)
    assert action == terminal
    assert b"HF_TOKEN=" + TOKEN.encode() in action[".env"] and b"- author" in action["ml.yaml"] and b"gpu_ids:\n  - 1\n" in action["ml.yaml"]


# ── the read, and an unchanged reconfigure ─────────────────────────────────


def _reconfigure_with_the_read(name):
    data = sdp.get_source_settings(name=name)
    assert verbs.validate_request("source.configure", {"name": name, "profiles": data["profiles"], **data["answers"]})
    return data, sdp.do_source_configure(name=name, profiles=data["profiles"], **data["answers"])


def _written(root, name):
    return {key: value for key, value in _files(root, name).items() if not key.endswith(".bak")}


@pytest.mark.parametrize("added, project", [
    (dict(
        name="forum", platform="custom", data_types=["posts", "replies"], **_paths("forum"), file_format="parquet", parquet_row_group_size=500000,
        profiles=["parse", "lingua", "ml", "postgres_ingest", "postgres_ml", "mongo_ingest", "sr_ingest", "sr_ml"], parse_workers=6, pg_prefer_lingua=False, table_tablespaces={"posts": "fast"},
        db_schema="forum_s", input_format="csv", csv_delimiter="tab", dump_globs={"posts": "posts_*.csv.zst", "replies": "replies_*.bin"}, compressions={"replies": "gz"},
        fields={"posts": ["id", "body"], "replies": ["id", "text"]}, mongo_collections={"posts": "p_coll"}, mongo_db_name="forumdb", pg_indexes={"posts": ["id"]}, sr_indexes={"replies": ["id"]},
        primary_key="id", sr_buckets=64, text_columns={"posts": ["body"], "replies": ["text"]}, remove_strings=["[deleted]"], gpu_fields=["author"], lingua_low_accuracy=True,
        gpu_ids=[1], ml_tokenize_workers=3, ml_classifier_data_types={"toxic_roberta": ["posts"]},
    ), {"tablespaces": True}),
    (dict(name="reddit", platform="reddit", data_types=["submissions", "comments"], **_paths("reddit"), file_format="csv", profiles=["parse", "lingua", "sr_ingest"], reddit_db_schema="rd", reddit_indexes={"comments": ["author"]}), {}),
    (dict(name="bsky", platform="custom", data_types=["posts", "people"], **_paths("bsky"), file_format="parquet", profiles=["parse", "mongo_ingest"], hf_dataset=DATASET, hf_groups=HF_GROUPS), {}),
], ids=["custom", "reddit", "hugging-face"])
def test_the_reads_answers_sent_back_write_the_same_files(tmp_path, monkeypatch, added, project):
    """Bug class: a read that leaves out or misreads a current value (a pattern's compression, the CSV delimiter, a tablespace, a classifier's data types, the Reddit overrides against the template), so that a reconfigure prefilled with it and confirmed unchanged writes something else."""
    root = _project(tmp_path / "project", **project)
    _add(monkeypatch, root, HW_GPUS, **added)
    if added["name"] == "forum":
        # A typed StarRocks entry (written by db create-indexes) is kept, though no answer holds it.
        platform = root / "config/sources/forum/platform.yaml"
        platform.write_text(platform.read_text().replace("sr_indexes:\n  replies:\n  - id\n", "sr_indexes:\n  replies:\n    bitmap:\n    - id\n    bloomfilter:\n    - text\n"))
    before = _written(root, added["name"])

    data, code = _reconfigure_with_the_read(added["name"])

    assert code == 0
    assert _written(root, added["name"]) == before
    assert data["left_out"] == []
    if added["name"] == "forum":
        assert data["typed_sr_indexes"] == {"replies": {"bitmap": ["id"], "bloomfilter": ["text"]}}
        assert data["answers"]["compressions"] == {"replies": "gz"} and data["answers"]["csv_delimiter"] == "tab"


def test_the_read_never_holds_a_secret_and_prints_nothing(tmp_path, monkeypatch, capsys):
    """Bug class: the Hugging Face token (the one secret a source's setup writes, into .env) reaching the read's answer, which the broker hands to the browser; or a read that prints, which would break the getter's JSON."""
    root = _project(tmp_path / "project")
    added = {key: value for key, value in FORUM.items() if key not in ("sr_indexes", "primary_key", "lingua_workers")}
    _add(monkeypatch, root, HW_GPUS, **dict(added, profiles=["parse", "ml"], hf_token=TOKEN))
    capsys.readouterr()

    data = sdp.get_source_settings(name="forum")

    assert TOKEN not in json.dumps(data)
    assert data["hf_token_set"] is True and "hf_token" not in data["answers"]
    assert capsys.readouterr().out == ""


# ── refusals ────────────────────────────────────────────────────────────────


def test_a_folder_of_another_source_is_refused_and_the_sources_own_are_not(tmp_path, monkeypatch):
    """Bug class: the folder check of F4 decision 1 skipped on a reconfigure (two sources sharing a folder, so one's parse or ingest reads or deletes the other's files), or applied to the source's own folders, so that no reconfigure that keeps its folders could pass."""
    root = _project(tmp_path / "project")
    _add(monkeypatch, root, HW_NONE, **FORUM)
    _add(monkeypatch, root, HW_NONE, **dict(FORUM, name="other", **_paths("other")))
    before = _files(root, "forum")

    with pytest.raises(sdp.ActionError, match="the output folder is a folder of the source 'other'"):
        sdp.do_source_configure(name="forum", profiles=FORUM["profiles"], output_path="data/output/other/inside")
    assert _files(root, "forum") == before

    # The compression of a pattern without one is asked again, its default zst as in the terminal: given, the files are as they were.
    assert sdp.do_source_configure(name="forum", profiles=FORUM["profiles"], compressions={"replies": "xz"}) == 0
    assert _written(root, "forum") == before


def test_add_classifiers_refuses_what_the_terminal_does_not_ask(tmp_path, monkeypatch):
    """Bug class: classifier answers accepted for a source whose profiles do not ask them (the terminal writes nothing for a source without lingua or ml), or Reddit's fixed text columns overridden."""
    root = _project(tmp_path / "project")
    _add(monkeypatch, root, HW_NONE, **{key: value for key, value in dict(FORUM, profiles=["parse", "sr_ingest"]).items() if key not in ("text_columns", "lingua_workers")})
    with pytest.raises(sdp.ActionError, match="neither the profile lingua nor ml"):
        sdp.do_source_add_classifiers(name="forum")
    _add(monkeypatch, root, HW_NONE, **dict(name="reddit", platform="reddit", data_types=["comments"], **_paths("reddit"), file_format="parquet", profiles=["lingua"]))
    with pytest.raises(sdp.ActionError, match="'text_columns' is not asked for a Reddit source"):
        sdp.do_source_add_classifiers(name="reddit", text_columns={"comments": ["body"]})
    with pytest.raises(sdp.ActionError, match="'gpu_ids' is not asked without the profile ml"):
        sdp.do_source_add_classifiers(name="reddit", gpu_ids=[0])
    assert not (root / "config/sources/reddit/lingua.yaml.bak").exists()
