"""Tests for `sdp.do_source_add`, the non-interactive `source add` of the web UI's wizard (`setup.source_add`).

The contracts pinned here, each test naming the bug class it catches:

- The same answers give the same files: the terminal's questionnaire (`source.main`, and `reddit.main` for the Reddit questions of `source configure reddit`) is driven with scripted answers in one project, the action is called with the same answers in another, and every file of the source and `.env` are compared byte for byte. A third case leaves every optional answer out, which must equal pressing Enter at every question that has a default.
- What the terminal would not take or not ask is refused before anything is written: a name that is already a source, a folder of another source (also through a symlink, inside one or around one), an answer to a question the chosen profiles do not ask, a list item the comma-separated prompt cannot hold.

Each test points the setup modules and `sdp` at a temp project of its own (`project`) with a fixed machine (16 cores, two GPUs or none); nothing reaches Docker or a terminal.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

import sdp
from social_data_pipeline.setup import classifiers, reddit, source, utils

REPO = Path(sdp.__file__).resolve().parent
HW_GPUS = {"cpu_cores": 16, "ram_gb": 64, "gpus": [{"index": 0, "name": "A", "vram_mb": 1000}, {"index": 1, "name": "B", "vram_mb": 2000}]}
HW_NONE = {"cpu_cores": 16, "ram_gb": 64, "gpus": []}
TOKEN = "hf_madeUpToken42"


def _project(root, databases=("postgres", "mongo", "starrocks"), tablespaces=False):
    config = root / "config"
    (config / "db").mkdir(parents=True)
    for database in databases:
        text = "port: 1\n"
        if database == "postgres" and tablespaces:
            text += "tablespaces:\n  fast: /mnt/fast\n"
        (config / "db" / f"{database}.yaml").write_text(text)
    for part in ("templates/reddit.yaml", "lingua/cpu_classifiers.yaml", "ml/gpu_classifiers.yaml"):
        (config / part).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(REPO / "config" / part, config / part)
    (root / ".env").write_text("DATA_PATH=./data\nHF_TOKEN=\n")
    return root


def _point_at(monkeypatch, root, hw):
    config = root / "config"
    for module in (utils, source, classifiers, reddit):
        monkeypatch.setattr(module, "ROOT", root)
        monkeypatch.setattr(module, "CONFIG_DIR", config)
    monkeypatch.setattr(utils, "SOURCES_DIR", config / "sources")
    monkeypatch.setattr(classifiers, "SOURCES_DIR", config / "sources")
    monkeypatch.setattr(utils, "DB_CONFIG_DIR", config / "db")
    monkeypatch.setattr(reddit, "REDDIT_TEMPLATE", config / "templates" / "reddit.yaml")
    monkeypatch.setattr(sdp, "ROOT", root)
    monkeypatch.setattr(sdp, "CONFIG_DIR", config)
    monkeypatch.setattr(sdp, "_running_services", lambda: set())
    for module in (utils, source, classifiers):
        monkeypatch.setattr(module, "detect_hardware", lambda: hw)


class Script:
    """Answers the terminal's prompts in order: each entry is (a piece of the prompt, the answer typed)."""

    def __init__(self, steps):
        self.steps = list(steps)

    def __call__(self, prompt=""):
        assert self.steps, f"no answer left for {prompt!r}"
        expected, answer = self.steps.pop(0)
        assert expected in prompt, (expected, prompt)
        return answer


def _terminal(monkeypatch, root, hw, name, steps, reddit_steps=None):
    _point_at(monkeypatch, root, hw)
    script = Script(steps)
    monkeypatch.setattr("builtins.input", script)
    monkeypatch.setattr(utils.getpass, "getpass", script)
    source.main(source_name=name)
    if reddit_steps is not None:
        script.steps = list(reddit_steps)
        reddit.main(source_name=name)
    assert script.steps == []


def _files(root, name):
    folder = root / "config" / "sources" / name
    found = {path.name: path.read_bytes() for path in sorted(folder.iterdir())}
    found[".env"] = (root / ".env").read_bytes()
    return found


def _both(tmp_path, monkeypatch, hw, name, steps, answers, reddit_steps=None, **project):
    terminal_root = _project(tmp_path / "terminal", **project)
    action_root = _project(tmp_path / "action", **project)
    _terminal(monkeypatch, terminal_root, hw, name, steps, reddit_steps)
    _point_at(monkeypatch, action_root, hw)
    monkeypatch.setattr("builtins.input", lambda *a: pytest.fail("the action asked a question"))
    assert sdp.do_source_add(**answers) == 0
    return _files(terminal_root, name), _files(action_root, name)


def _paths(name):
    return {f"{stage}_path": f"data/{stage}/{name}" for stage in ("dumps", "extracted", "parsed", "output")}


def test_a_custom_source_with_every_question_answered_writes_the_terminals_files(tmp_path, monkeypatch):
    """Bug class: the action building other settings than the questionnaire for the same answers (a key missing or added, a default taken where an answer was given, another order), which writes a config the terminal never would: every question of a custom source with all eight profiles, tablespaces, CSV input, two GPUs and a Hugging Face token."""
    available = classifiers.compute_classifier_defaults(HW_GPUS, [])["ml_classifiers"]
    steps = [
        ("Data types", "posts, replies"),
        ("Dumps directory", "./data/dumps/forum"), ("Extracted directory", "./data/extracted/forum"), ("Parsed directory", "./data/parsed/forum"), ("Output directory", "./data/output/forum"),
        ("Choice", "parquet"), ("row-group", "500000"),
        ("Select", "1,2,3,4,5,6,7,8"),
        ("Parse workers", "6"),
        ("Use Lingua", "n"), ("Parallel index workers", "3"),
        ("Choice", "fast"), ("Choice", ""),
        ("PostgreSQL schema", "forum_s"),
        ("Choice", "csv"), ("delimiter", "\\t"),
        ("glob pattern for posts", "posts_*.csv.zst"),
        ("glob pattern for replies", "replies_*.bin"), ("Choice", "xz"),
        ("Fields for 'posts'", "id, body, user.name"), ("Fields for 'replies'", "id, text"),
        ("Choice", "per_data_type"), ("collection name for 'posts'", "p_coll"), ("collection name for 'replies'", ""),
        ("MongoDB database name", "forumdb"),
        ("PostgreSQL indexes for 'posts'", "user.name"), ("PostgreSQL indexes for 'replies'", ""),
        ("MongoDB indexes for 'posts'", "id"), ("MongoDB indexes for 'replies'", ""),
        ("StarRocks BITMAP indexes for 'posts'", ""), ("StarRocks BITMAP indexes for 'replies'", "id"),
        ("Primary key", "id"),
        ("Use Lingua", "y"), ("buckets", "64"),
        ("Text columns for 'posts'", "body"), ("Text columns for 'replies'", "text"),
        ("Exact strings", "[deleted], [removed]"), ("Regex patterns", r"https?://\S+, @\w+"), ("Extra fields", "user.name"),
        ("Lingua workers", "12"), ("Lingua file workers", "2"), ("Lingua batch size", "1000"), ("Low accuracy", "y"),
        ("Select", "2"),
        ("File workers", "1"), ("Tokenize workers", "4"), ("Classifier batch size", "32"),
        ("Select", "1"), ("Select", "1"),
        ("HuggingFace token", TOKEN),
        ("Write these files", "y"),
    ]
    answers = dict(
        name="forum", platform="custom", data_types=["posts", "replies"], **_paths("forum"), file_format="parquet", parquet_row_group_size=500000,
        profiles=["parse", "lingua", "ml", "postgres_ingest", "postgres_ml", "mongo_ingest", "sr_ingest", "sr_ml"], parse_workers=6,
        pg_prefer_lingua=False, pg_parallel_index_workers=3, table_tablespaces={"posts": "fast"}, db_schema="forum_s", input_format="csv", csv_delimiter="tab",
        dump_globs={"posts": "posts_*.csv.zst", "replies": "replies_*.bin"}, compressions={"replies": "xz"}, fields={"posts": ["id", "body", "user.name"], "replies": ["id", "text"]},
        mongo_collection_strategy="per_data_type", mongo_collections={"posts": "p_coll"}, mongo_db_name="forumdb",
        pg_indexes={"posts": ["user.name"]}, mongo_indexes={"posts": ["id"]}, sr_indexes={"replies": ["id"]}, primary_key="id", sr_prefer_lingua=True, sr_buckets=64,
        text_columns={"posts": ["body"], "replies": ["text"]}, remove_strings=["[deleted]", "[removed]"], remove_patterns=[r"https?://\S+", r"@\w+"], gpu_fields=["user.name"],
        lingua_workers=12, lingua_file_workers=2, lingua_batch_size=1000, lingua_low_accuracy=True,
        gpu_ids=[1], ml_file_workers=1, ml_tokenize_workers=4, ml_classifier_batch_size=32, ml_classifiers=[available[0]], ml_classifier_data_types={available[0]: ["posts"]}, hf_token=TOKEN,
    )

    terminal, action = _both(tmp_path, monkeypatch, HW_GPUS, "forum", steps, answers, tablespaces=True)

    assert set(terminal) == {"platform.yaml", "parse.yaml", "postgres.yaml", "postgres_ml.yaml", "mongo.yaml", "starrocks.yaml", "sr_ml.yaml", "lingua.yaml", "ml.yaml", ".env"}
    assert action == terminal
    assert TOKEN.encode() in action[".env"]


def test_every_answer_left_out_is_the_terminals_enter(tmp_path, monkeypatch):
    """Bug class: a default computed differently from the terminal's (the parse and index workers from the cores, the buckets, the Lingua and GPU settings from the base configuration, the schema and the Mongo names from the source name, the regex pattern to remove), so a field left empty in the browser writes another value than Enter would."""
    steps = [
        ("Data types", "posts"),
        ("Dumps directory", "./data/dumps/plain"), ("Extracted directory", "./data/extracted/plain"), ("Parsed directory", "./data/parsed/plain"), ("Output directory", "./data/output/plain"),
        ("Choice", ""), ("row-group", ""),
        ("Select", ""),
        ("Parse workers", ""), ("Use Lingua", ""), ("Parallel index workers", ""),
        ("PostgreSQL schema", ""), ("Choice", ""),
        ("glob pattern for posts", "posts_*.json.gz"), ("Fields for 'posts'", "id, text"),
        ("Choice", ""), ("collection name for 'posts'", ""), ("MongoDB database name", ""),
        ("PostgreSQL indexes for 'posts'", ""), ("MongoDB indexes for 'posts'", ""), ("StarRocks BITMAP indexes for 'posts'", ""),
        ("Primary key", ""),
        ("Use Lingua", ""), ("buckets", ""),
        ("Text columns for 'posts'", ""), ("Exact strings", ""), ("Regex patterns", ""), ("Extra fields", ""),
        ("Lingua workers", ""), ("Lingua file workers", ""), ("Lingua batch size", ""), ("Low accuracy", ""),
        ("Write these files", ""),
    ]
    # No GPU: the terminal's default profiles are every offered one but ml.
    profiles = ["parse", "lingua", "postgres_ingest", "postgres_ml", "mongo_ingest", "sr_ingest", "sr_ml"]
    answers = dict(name="plain", platform="custom", data_types=["posts"], **_paths("plain"), file_format="parquet", profiles=profiles, dump_globs={"posts": "posts_*.json.gz"}, fields={"posts": ["id", "text"]})

    terminal, action = _both(tmp_path, monkeypatch, HW_NONE, "plain", steps, answers)

    assert action == terminal
    assert b"parse_workers: 8" in action["parse.yaml"] and b"sr_buckets: 128" in action["platform.yaml"]


def test_a_reddit_source_with_its_field_and_index_questions_writes_the_terminals_files(tmp_path, monkeypatch):
    """Bug class: the Reddit path built like a custom one (its template replaced, a key of the custom block written), or the Reddit questions of `source configure reddit` applied otherwise than that command applies them (the fields kept in another order, the other kind's list dropped instead of kept whole)."""
    import yaml

    fields = yaml.safe_load((REPO / "config/templates/reddit.yaml").read_text())["fields"]["submissions"]
    removed = [fields[1], fields[3]]
    kept_indices = ",".join(str(i + 1) for i, field in enumerate(fields) if field not in removed)
    steps = [
        ("Data types", ""),
        ("Dumps directory", "./data/dumps/reddit"), ("Extracted directory", "./data/extracted/reddit"), ("Parsed directory", "./data/parsed/reddit"), ("Output directory", "./data/output/reddit"),
        ("Choice", "csv"),
        ("Select", "1,2,4"),
        ("Parse workers", "4"),
        ("Use Lingua", "n"), ("buckets", "32"),
        ("Lingua workers", ""), ("Lingua file workers", ""), ("Lingua batch size", ""), ("Low accuracy", ""),
        ("Write these files", "y"),
    ]
    reddit_steps = [
        ("Database schema name", "rdt"),
        ("Remove fields", "y"), ("Select", kept_indices), ("Select", ""),
        ("Customize database indexes", "y"), ("Submissions index columns", "author, subreddit"), ("Comments index columns", ""),
        ("Apply these overrides", "y"),
    ]
    answers = dict(
        name="reddit", platform="reddit", data_types=["submissions", "comments"], **_paths("reddit"), file_format="csv", profiles=["parse", "lingua", "sr_ingest"],
        parse_workers=4, sr_prefer_lingua=False, sr_buckets=32, reddit_db_schema="rdt", reddit_removed_fields={"submissions": removed}, reddit_indexes={"submissions": ["author", "subreddit"]},
    )

    terminal, action = _both(tmp_path, monkeypatch, HW_NONE, "reddit", steps, answers, reddit_steps=reddit_steps, databases=("starrocks",))

    assert action == terminal
    assert b"db_schema: rdt" in action["platform.yaml"]


# ── refusals ────────────────────────────────────────────────────────────────


def _minimal(name="fresh", **more):
    return {"name": name, "platform": "custom", "data_types": ["posts"], **_paths(name), "file_format": "parquet", "profiles": ["parse"], "dump_globs": {"posts": "p_*.zst"}, "fields": {"posts": ["id"]}, **more}


@pytest.fixture
def deployment(tmp_path, monkeypatch):
    root = _project(tmp_path / "project")
    _point_at(monkeypatch, root, HW_NONE)
    other = root / "config" / "sources" / "other"
    other.mkdir(parents=True)
    (other / "platform.yaml").write_text("paths:\n  dumps: data/dumps/other\n  extracted: /srv/extracted/other\n  parsed: data/parsed/other\n  output: data/output/other\n")
    return root


def _refused(answers, match):
    with pytest.raises(sdp.ActionError, match=match) as exc:
        sdp.do_source_add(**answers)
    return exc.value


@pytest.mark.parametrize("stage, folder", [
    ("dumps", "data/dumps/other"),
    ("dumps", "./data/dumps/../dumps/other"),
    ("parsed", "data/parsed/other/inside"),
    ("output", "data/output"),
    ("extracted", "/srv/extracted/other"),
])
def test_a_folder_of_another_source_is_refused_and_nothing_is_written(deployment, stage, folder):
    """Bug class: two sources sharing a folder (F4 decision 1), so that one source's parse or ingest reads or deletes the other's files: the same folder in another spelling, one inside another source's folder, one that holds another source's folder."""
    error = _refused(_minimal(**{f"{stage}_path": folder}), f"the {stage} folder is a folder of the source 'other'")

    assert folder not in error.message
    assert not (deployment / "config" / "sources" / "fresh").exists()


def test_a_folder_reached_through_a_symlink_is_compared_by_where_it_leads(deployment):
    """Bug class: the folders compared as text, so a symlink to another source's folder passes (the paths are canonicalized with os.path.realpath where they are compared)."""
    (deployment / "data" / "dumps" / "other").mkdir(parents=True)
    (deployment / "link").symlink_to(deployment / "data" / "dumps" / "other")

    _refused(_minimal(dumps_path=str(deployment / "link")), "the dumps folder is a folder of the source 'other'")


def test_a_name_that_is_a_source_already_is_refused(deployment):
    """Bug class: `source.add` over an existing source, which the terminal only does after "Reconfigure?" (F4 decision 4: the browser's way is Reconfigure); a folder without platform.yaml counts too, as the terminal's check of the folder does."""
    (deployment / "config" / "sources" / "half").mkdir()
    for name in ("other", "half"):
        error = _refused(_minimal(name=name), "exists already")
        assert "Reconfigure" in error.message
    assert sorted(path.name for path in (deployment / "config" / "sources" / "other").iterdir()) == ["platform.yaml"]


@pytest.mark.parametrize("extra, match", [
    ({"parse_workers": 4, "profiles": ["lingua"]}, "'parse_workers' is not asked without the profile parse"),
    ({"compressions": {"posts": "gz"}}, "names it already"),
    ({"sr_buckets": 8}, "'sr_buckets' is not asked"),
    ({"reddit_db_schema": "x"}, "is not asked for a source of your own files"),
    ({"parquet_row_group_size": 5, "file_format": "csv"}, "for CSV intermediate files"),
    ({"profiles": ["parse", "ml"], "gpu_ids": []}, "at least one GPU"),
    ({"profiles": ["parse", "lingua"], "remove_patterns": ["a,b"]}, "holds a comma"),
    ({"fields": {"posts": ["id"], "nope": ["x"]}}, "a data type the source does not have"),
    ({"fields": {}}, "at least one field"),
    ({"dump_globs": {}}, "dump file pattern is required"),
])
def test_an_answer_the_terminal_would_not_take_or_ask_is_refused(deployment, extra, match):
    """Bug class: an answer accepted that the terminal never asks for these answers (and so silently drops or writes where the terminal would not), or one its prompts could never produce; nothing is written for any of them."""
    _refused(_minimal(**extra), match)

    assert not (deployment / "config" / "sources" / "fresh").exists()


def test_without_a_database_setup_nothing_is_written(tmp_path, monkeypatch):
    """Bug class: a source added before `db setup`, which the terminal refuses (it exits with the recovery command)."""
    root = tmp_path / "bare"
    (root / "config").mkdir(parents=True)
    _point_at(monkeypatch, root, HW_NONE)

    _refused(_minimal(), "Run 'python sdp.py db setup' first")
    assert not (root / "config" / "sources").exists()


# ── the folders as written: a relative one with its `./`, never a bare name ──


STAGES = ("dumps", "extracted", "parsed", "output")
# How a folder is typed (None: left empty, the terminal's Enter) and how it must be written; `{stage}` is each stage.
FOLDER_FORMS = {
    "typed with ./": ("./elsewhere/{stage}", "./elsewhere/{stage}"),
    "typed bare": ("elsewhere/{stage}", "./elsewhere/{stage}"),
    "typed above the deployment": ("../outside/{stage}", "../outside/{stage}"),
    "absolute": ("/srv/sdp/{stage}", "/srv/sdp/{stage}"),
    "left empty": (None, "./data/{stage}/plain"),
}
PLAIN_PROFILES = ["parse", "postgres_ingest", "sr_ingest"]


def _written_shape(path):
    """What Compose takes as a folder (a bind mount) and the terminal's default has: absolute, or starting with ./ or ../ (a bare `data/x` is the name of a volume to Compose)."""
    return path.startswith(("/", "./", "../"))


def _typed(form, suffix=""):
    typed, _ = FOLDER_FORMS[form]
    return {} if typed is None else {f"{stage}_path": typed.format(stage=stage) + suffix for stage in STAGES}


def _written_paths(root, name="plain"):
    import yaml

    return yaml.safe_load((root / "config" / "sources" / name / "platform.yaml").read_text())["paths"]


@pytest.mark.parametrize("form", list(FOLDER_FORMS))
def test_every_folder_a_source_action_writes_is_absolute_or_dot_relative(tmp_path, monkeypatch, form):
    """Bug class: a folder written as a bare relative name (`data/dumps/x`), which Compose reads as the name of a volume, so the first run of the source fails (`service "parse" refers to undefined volume`): the registry's `Path` takes the `./` off a typed answer, and the wizard's own default had none. Each answer goes through the registry as the broker sends it, for `source.add` and then `source.configure`; the folders the database servers' override mounts are checked too."""
    from social_data_pipeline.control import verbs
    from social_data_pipeline.setup import mount_sync

    root = _project(tmp_path / "deployment")
    _point_at(monkeypatch, root, HW_NONE)
    monkeypatch.setattr("builtins.input", lambda *a: pytest.fail("the action asked a question"))
    _, written = FOLDER_FORMS[form]
    added = verbs.validate_request("source.add", dict(name="plain", platform="custom", data_types=["posts"], file_format="parquet", profiles=PLAIN_PROFILES, dump_globs={"posts": "posts_*.json.gz"}, fields={"posts": ["id", "text"]}, **_typed(form)))
    assert sdp.do_source_add(**added) == 0
    paths = _written_paths(root)
    assert paths == {stage: written.format(stage=stage) for stage in STAGES}

    changed = verbs.validate_request("source.configure", dict(name="plain", profiles=PLAIN_PROFILES, **_typed(form, "2")))
    assert sdp.do_source_configure(**changed) == 0
    after = _written_paths(root)
    assert after == ({stage: written.format(stage=stage) + "2" for stage in STAGES} if FOLDER_FORMS[form][0] else paths)
    assert all(_written_shape(path) for path in after.values()), after

    for service in ("postgres", "starrocks"):
        for mount in mount_sync.expected_source_mounts(sdp._collect_source_info(), service, parent_paths=sdp._get_parent_paths()):
            assert _written_shape(mount.split(":", 1)[0]), (service, mount)


def _compose_source(volume, environment):
    """The source of a short-syntax volume of docker-compose.yml with `${NAME:-default}` filled in from `environment`, and whether Compose takes it as a folder: Compose binds a source that starts with `.`, `/` or `~` and takes any other as the name of a volume (measured with Docker Compose: `DUMPS_PATH=data/dumps/x docker compose --profile parse config parse` answers `refers to undefined volume data/dumps/x`)."""
    import re

    filled = re.sub(r"\$\{(\w+)(?::-([^}]*))?\}", lambda match: environment.get(match.group(1)) or (match.group(2) or ""), volume)
    source = filled.split(":", 1)[0]
    return source, source.startswith((".", "/", "~"))


def test_the_default_folders_reach_the_runs_compose_as_bind_mounts(tmp_path, monkeypatch):
    """Bug class: a source added with its folders left empty whose run gives Compose a mount source it reads as a volume's name (`python sdp.py run parse --source <name>` failing at once): `do_run_start` puts the written folders into the environment that docker-compose.yml's `${DUMPS_PATH}` and the others are filled from, and every mount of the parse service must then be a folder."""
    import os

    import yaml

    from social_data_pipeline.control import verbs

    root = _project(tmp_path / "deployment")
    _point_at(monkeypatch, root, HW_NONE)
    monkeypatch.setattr("builtins.input", lambda *a: pytest.fail("the action asked a question"))
    added = verbs.validate_request("source.add", dict(name="plain", platform="custom", data_types=["posts"], file_format="parquet", profiles=["parse"], dump_globs={"posts": "posts_*.json.gz"}, fields={"posts": ["id", "text"]}))
    assert sdp.do_source_add(**added) == 0
    for key in ("DUMPS_PATH", "EXTRACTED_PATH", "PARSED_PATH", "OUTPUT_PATH", "SOURCE", "PLATFORM"):
        monkeypatch.delenv(key, raising=False)
    seen = []

    class Done:
        returncode = 0

    def compose(*args, **kwargs):
        seen.append((args, dict(os.environ)))
        return Done()

    monkeypatch.setattr(sdp, "docker_compose", compose)
    assert sdp.do_run_start(profile="parse", source="plain") == 0
    (args, environment), = seen
    assert args[-1] == "parse"
    volumes = yaml.safe_load((REPO / "docker-compose.yml").read_text())["services"]["parse"]["volumes"]
    sources = [_compose_source(volume, environment) for volume in volumes]
    assert all(bind for _, bind in sources), sources
    assert ("./data/dumps/plain", True) in sources
    assert all((root / "data" / stage / "plain").is_dir() for stage in STAGES)
