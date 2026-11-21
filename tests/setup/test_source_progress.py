"""Tests for `source progress`: the getter that counts the files each file-based stage (parse, lingua, the classifiers) has written for a source, its printer and its terminal command.

The contracts pinned here:

- A finished file is a file directly in a data type folder of a stage whose name ends in `.csv` or `.parquet`; a file ending in `.temp` is being written; the newest file is the finished one changed last. One listing per folder, nothing deeper.
- The stages are parse, lingua, then every other sub-folder of the output folder in name order, `lingua_ingest` left out. A relative path of the config is under the project root, wherever the command is run from.
- Nothing in a damaged config, a damaged tree or a file that disappears while the command runs raises: it becomes a "not readable" fact with a constant reason, or is passed over. A data type that is not one path component is never joined to a path.
- Every name and path in the data can be printed and sent as UTF-8, whatever bytes, control characters and format characters the file system or the config holds; a data type that is not text is never turned into text.
- The getter refuses a malformed source name before it reads anything and prints nothing; the command prints the text or the JSON of the same data, returns 1 only for a named source that is not found, and with `--json` writes a refusal to standard error.

Everything lives in a temp folder the CLI is pointed at; nothing touches the real tree or Docker.
"""

from __future__ import annotations

import calendar
import json
import os
import time
import unicodedata
from types import SimpleNamespace

import pytest

import sdp
import test_status_golden as golden
from social_data_pipeline.setup import utils

NAME_NOT_VALID = "the source name is not a valid name"
CONFIG_NOT_READABLE = "the source config cannot be read"
CONFIG_NOT_A_MAPPING = "the source config is not a mapping"
PATHS_NOT_A_MAPPING = "paths in the source config is not a mapping"
DATA_TYPES_NOT_A_LIST = "data_types in the source config is not a list"
PATH_NOT_VALID = "the path in the source config is empty or not text"
DATA_TYPE_NOT_VALID = "the data type is not a valid folder name"
NOT_TEXT = "(not text)"
FOLDER_NOT_READABLE = "the folder cannot be read"

MISSING = {"exists": False, "not_readable": None, "finished": 0, "being_written": 0, "newest": None}
REFUSED = {**MISSING, "not_readable": DATA_TYPE_NOT_VALID}
UNSAFE_CATEGORIES = ("Cc", "Cf", "Zl", "Zp", "Cs")


# ── helpers ─────────────────────────────────────────────────────────────────


@pytest.fixture
def project(tmp_path, monkeypatch):
    """An empty temp project the CLI points at. `.root` is its folder."""
    root = tmp_path.resolve() / "project"
    made = golden.Project(root)
    golden.point_at(monkeypatch, root)
    return made


def utc(year, month, day, hour=0, minute=0, second=0):
    """Seconds since the epoch of a UTC time."""
    return calendar.timegm((year, month, day, hour, minute, second))


def put(path, mtime=None):
    """Create a file (and its folders) at path, a `str`, a `Path` or `bytes`; with mtime, set its modification time."""
    os.makedirs(os.path.dirname(os.fspath(path)), exist_ok=True)
    with open(path, "wb") as handle:
        handle.write(b"x")
    if mtime is not None:
        os.utime(path, (mtime, mtime))


def one_source(data):
    """The only source entry of the data."""
    assert len(data["sources"]) == 1
    return data["sources"][0]


def items(entry, stage):
    """The data type items of one stage of a source entry, by data type."""
    found = [candidate for candidate in entry["stages"] if candidate["stage"] == stage]
    assert len(found) == 1, [candidate["stage"] for candidate in entry["stages"]]
    return {item["data_type"]: item for item in found[0]["data_types"]}


def item_list(entry, stage):
    """The data type items of one stage of a source entry, in the order of the config (labels may repeat)."""
    found = [candidate for candidate in entry["stages"] if candidate["stage"] == stage]
    assert len(found) == 1, [candidate["stage"] for candidate in entry["stages"]]
    return found[0]["data_types"]


def counts(item):
    return (item["finished"], item["being_written"])


def strings(value):
    """Every string in the data, keys included."""
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for key, child in value.items():
            yield key
            yield from strings(child)
    elif isinstance(value, list):
        for child in value:
            yield from strings(child)
    else:
        assert value is None or isinstance(value, (bool, int, float)), type(value)


def unsafe_characters(data):
    """The control characters, format characters and lone surrogates left anywhere in the data, as (string, character)."""
    return [(text, ch) for text in strings(data) for ch in text if unicodedata.category(ch) in UNSAFE_CATEGORIES]


def run_command(*argv):
    """Run `source progress` through the real parser and return its exit code."""
    args = sdp.build_parser().parse_args(["source", "progress", *argv])
    return args.func(args)


# ── counting ────────────────────────────────────────────────────────────────


def test_finished_files_are_the_csv_and_parquet_files_directly_in_the_folder(project):
    """Bug class: a count that is not the number of output files a stage finished: one extension forgotten (a source parsed to CSV would show nothing), a log or another by-product counted, a folder walked deeper than the stage writes, a folder with a file-like name counted, or a link the stages follow not followed (and a dangling one counted)."""
    comments = project.root / "data" / "parsed" / "src" / "comments"
    project.source("src", "data_types: [comments, submissions]\n")
    put(comments / "RC_2024-01.parquet")
    put(comments / "RC_2024-02.csv")
    put(comments / "parsing_errors_RC_2024-01.log")
    put(comments / "notes.parquet.txt")
    put(comments / "deeper" / "RC_2024-03.parquet")
    put(comments / "folder.parquet" / "RC_2024-04.parquet")
    put(project.root / "elsewhere" / "target.parquet")
    os.symlink(project.root / "elsewhere" / "target.parquet", comments / "linked.parquet")
    os.symlink(project.root / "elsewhere" / "nothing.parquet", comments / "dangling.parquet")
    put(project.root / "data" / "parsed" / "src" / "submissions" / "RS_2024-01.csv")

    parse = items(one_source(sdp.get_source_progress(name="src")), "parse")

    assert counts(parse["comments"]) == (3, 0)
    assert counts(parse["submissions"]) == (1, 0)
    assert parse["comments"]["exists"] is True and parse["comments"]["not_readable"] is None


def test_temp_files_are_being_written_and_not_finished(project):
    """Bug class: a file a stage is still writing (`<name>.parquet.temp`, renamed on success) counted as finished, which reports work as done that a crash would undo; or not shown at all, so a running stage looks idle."""
    comments = project.root / "data" / "parsed" / "src" / "comments"
    project.source("src", "data_types: [comments]\n")
    put(comments / "RC_2024-01.parquet")
    put(comments / "RC_2024-02.parquet.temp")
    put(comments / "RC_2024-03.csv.temp")
    put(comments / "folder.temp" / "RC_2024-04.parquet.temp")

    parse = items(one_source(sdp.get_source_progress(name="src")), "parse")

    assert counts(parse["comments"]) == (1, 2)
    assert parse["comments"]["newest"]["name"] == "RC_2024-01.parquet"


def test_newest_is_the_finished_file_changed_last(project):
    """Bug class: "newest" taken from the wrong order: the last name (stages do not work in name order when a filter or a re-run is used), the last file created, or a `.temp` file. The time is carried as UTC with a Z, whatever the local zone is."""
    comments = project.root / "data" / "parsed" / "src" / "comments"
    project.source("src", "data_types: [comments]\n")
    put(comments / "RC_2024-09.parquet", utc(2024, 3, 1, 8))
    put(comments / "RC_2024-05.parquet", utc(2024, 3, 5, 10, 11, 12))
    put(comments / "RC_2024-07.parquet.temp", utc(2024, 3, 9, 9))
    put(comments / "RC_2024-01.parquet", utc(2024, 3, 3, 9))

    parse = items(one_source(sdp.get_source_progress(name="src")), "parse")

    assert parse["comments"]["newest"] == {"name": "RC_2024-05.parquet", "modified": "2024-03-05T10:11:12Z"}


class OrderedListing:
    """What `os.scandir` returns, with the entries in a fixed order."""

    def __init__(self, entries):
        self.entries = entries

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def __iter__(self):
        return iter(self.entries)


@pytest.mark.parametrize("descending", [False, True], ids=["listed-ascending", "listed-descending"])
def test_among_files_changed_at_the_same_time_the_greater_name_is_the_newest(descending, project, monkeypatch):
    """Bug class: among files changed in the same instant, a "newest" that is whichever the file system lists first or last, so that two calls on an unchanged folder (or the same folder on another file system) give different answers. The listing is forced into each order in turn; the greater name must win both times, and an older file with a still greater name must not."""
    comments = project.root / "data" / "parsed" / "src" / "comments"
    project.source("src", "data_types: [comments]\n")
    for name in ("a.parquet", "b.parquet", "c.parquet"):
        put(comments / name, utc(2024, 3, 5, 10, 11, 12))
    put(comments / "z.parquet", utc(2024, 3, 1))
    real, listed = os.scandir, []

    def ordered(path):
        with real(path) as listing:
            entries = sorted(listing, key=lambda entry: entry.name, reverse=descending)
        listed.append([entry.name for entry in entries])
        return OrderedListing(entries)

    with monkeypatch.context() as patched:
        patched.setattr(sdp.os, "scandir", ordered)
        parse = items(one_source(sdp.get_source_progress(name="src")), "parse")

    names = ["a.parquet", "b.parquet", "c.parquet", "z.parquet"]
    assert (names[::-1] if descending else names) in listed
    assert parse["comments"]["newest"] == {"name": "c.parquet", "modified": "2024-03-05T10:11:12Z"}
    assert counts(parse["comments"]) == (4, 0)


def test_extensions_are_matched_in_lower_case_at_the_end_of_the_name(project):
    """Bug class: a count that takes other files than the stages do. The stages look for `*.csv` and `*.parquet` and write `<name>.temp`, in lower case: a file ending in `.PARQUET` or `.CSV` is not one of theirs and must not be counted (nor `.TEMP` as being written). What decides is the end of the name alone: `x.temp.csv` is finished, a file named just `.csv` is finished, `x.csv.temp` is being written."""
    upper = project.root / "data" / "parsed" / "src" / "upper"
    ends = project.root / "data" / "parsed" / "src" / "ends"
    project.source("src", "data_types: [upper, ends]\n")
    for name in ("X.PARQUET", "y.CSV", "z.Parquet", "w.parquet.TEMP", "kept.parquet"):
        put(upper / name)
    for name in ("x.temp.csv", ".csv", "x.csv.temp"):
        put(ends / name)

    parse = items(one_source(sdp.get_source_progress(name="src")), "parse")

    assert counts(parse["upper"]) == (1, 0)
    assert parse["upper"]["newest"]["name"] == "kept.parquet"
    assert counts(parse["ends"]) == (2, 1)


def test_links_that_lead_nowhere_are_passed_over(project):
    """Bug class: a link that leads back to itself, to nothing, or through a file as if it were a folder, taking the command down or making its whole folder "not readable": looking at such a link fails with another error than "no such file". Neither is a file or a folder, so in a data type folder it is not counted (whatever its name ends in), under the output folder it is not a classifier, and everything next to it is still reported."""
    comments = project.root / "data" / "parsed" / "src" / "comments"
    output = project.root / "data" / "output" / "src"
    project.source("src", "data_types: [comments]\n")
    put(comments / "RC_2024-01.parquet", utc(2024, 3, 1))
    put(output / "toxic" / "comments" / "RC_2024-01_toxic.parquet")
    for folder, names in ((comments, ("loop.parquet", "loop.csv.temp")), (output, ("loop", "selfsame"))):
        for name in names:
            os.symlink(folder / name, folder / name)
    os.symlink(comments / "nothing.parquet", comments / "dangling.parquet")
    os.symlink(comments / "nothing.temp", comments / "dangling.parquet.temp")
    os.symlink(output / "nothing", output / "dangling")
    os.symlink(comments / "RC_2024-01.parquet" / "child.parquet", comments / "through_a_file.parquet")

    entry = one_source(sdp.get_source_progress(name="src"))

    assert items(entry, "parse")["comments"] == {"data_type": "comments", "exists": True, "not_readable": None, "finished": 1, "being_written": 0, "newest": {"name": "RC_2024-01.parquet", "modified": "2024-03-01T00:00:00Z"}}
    assert [stage["stage"] for stage in entry["stages"]] == ["parse", "lingua", "toxic"]
    assert entry["output"] == {"path": str(output), "exists": True, "not_readable": None}
    assert counts(items(entry, "toxic")["comments"]) == (1, 0)


# ── stages ──────────────────────────────────────────────────────────────────


def test_stages_are_parse_lingua_and_the_classifier_folders_in_name_order(project):
    """Bug class: a stage list that is not what ran: `lingua_ingest` (a second copy of the lingua files, written for ingestion) shown as a classifier, lingua shown twice, a stray file under the output folder shown as a stage, or classifiers in the order the file system lists them, which changes between calls."""
    output = project.root / "data" / "output" / "src"
    project.source("src", "data_types: [comments]\n")
    put(project.root / "data" / "parsed" / "src" / "comments" / "RC_2024-01.parquet")
    put(output / "lingua" / "comments" / "RC_2024-01_lingua.parquet")
    put(output / "lingua_ingest" / "comments" / "RC_2024-01.parquet")
    put(output / "zeta" / "comments" / "RC_2024-01_zeta.parquet")
    put(output / "zeta" / "comments" / "RC_2024-02_zeta.parquet")
    put(output / "alpha" / "comments" / "RC_2024-01_alpha.parquet")
    put(output / "midway" / "comments" / "RC_2024-01_midway.parquet.temp")
    put(output / "notes.txt")
    put(output / "stray.parquet")

    entry = one_source(sdp.get_source_progress(name="src"))

    assert [(stage["stage"], stage["kind"]) for stage in entry["stages"]] == [("parse", "parse"), ("lingua", "lingua"), ("alpha", "classifier"), ("midway", "classifier"), ("zeta", "classifier")]
    assert [counts(items(entry, stage)["comments"]) for stage in ("parse", "lingua", "alpha", "midway", "zeta")] == [(1, 0), (1, 0), (1, 0), (0, 1), (2, 0)]
    assert [stage["path"] for stage in entry["stages"]] == [str(project.root / "data" / "parsed" / "src"), *(str(output / name) for name in ("lingua", "alpha", "midway", "zeta"))]
    assert entry["output"] == {"path": str(output), "exists": True, "not_readable": None}


def test_missing_folders_are_reported_as_missing_and_parse_and_lingua_are_always_there(project):
    """Bug class: a source that has not run yet answered with an error or an empty list instead of "nothing yet": a missing folder must be `exists` false with zero counts, told apart from a folder that cannot be read, and the parse and lingua stages must be present before their folders are."""
    project.source("src", "data_types: [comments, submissions]\n")

    entry = one_source(sdp.get_source_progress(name="src"))

    assert (entry["found"], entry["not_readable"], entry["warnings"]) == (True, None, [])
    assert [(stage["stage"], stage["not_readable"]) for stage in entry["stages"]] == [("parse", None), ("lingua", None)]
    for stage in ("parse", "lingua"):
        assert items(entry, stage) == {"comments": {"data_type": "comments", **MISSING}, "submissions": {"data_type": "submissions", **MISSING}}
    assert entry["output"] == {"path": str(project.root / "data" / "output" / "src"), "exists": False, "not_readable": None}


def test_a_data_type_folder_that_is_missing_sits_next_to_one_that_is_there(project):
    """Bug class: one missing data type folder hiding the others of its stage, or an existing empty folder reported as missing (the stage created it and has finished nothing yet, which is another state)."""
    project.source("src", "data_types: [comments, submissions, empty]\n")
    put(project.root / "data" / "parsed" / "src" / "comments" / "RC_2024-01.parquet")
    (project.root / "data" / "parsed" / "src" / "empty").mkdir()

    parse = items(one_source(sdp.get_source_progress(name="src")), "parse")

    assert counts(parse["comments"]) == (1, 0)
    assert parse["submissions"] == {"data_type": "submissions", **MISSING}
    assert parse["empty"] == {"data_type": "empty", **MISSING, "exists": True}


# ── paths ───────────────────────────────────────────────────────────────────


def test_relative_paths_are_under_the_project_root_not_the_working_directory(project, tmp_path, monkeypatch):
    """Bug class: a relative path of the config (or the default, which is relative) resolved against the directory the command is run from: the installed `sdp` command runs from anywhere and would count another tree, or none. A decoy tree with other counts sits in the working directory. An absolute path is used as it is, not re-rooted."""
    elsewhere = tmp_path.resolve() / "elsewhere"
    absolute = tmp_path.resolve() / "absolute" / "out"
    project.source("configured", f"data_types: [comments]\npaths:\n  parsed: store/parsed\n  output: {absolute}\n")
    project.source("defaults", "data_types: [comments]\n")
    put(project.root / "store" / "parsed" / "comments" / "RC_2024-01.parquet")
    put(absolute / "lingua" / "comments" / "RC_2024-01_lingua.parquet")
    put(absolute / "toxic" / "comments" / "RC_2024-01_toxic.parquet")
    put(project.root / "data" / "parsed" / "defaults" / "comments" / "RC_2024-01.parquet")
    put(project.root / "data" / "output" / "defaults" / "lingua" / "comments" / "RC_2024-01_lingua.parquet")
    for decoy in (elsewhere / "store" / "parsed", elsewhere / "data" / "parsed" / "defaults", elsewhere / "data" / "output" / "defaults" / "lingua", project.root / str(absolute).lstrip("/") / "lingua"):
        for name in ("d1.parquet", "d2.parquet", "d3.parquet"):
            put(decoy / "comments" / name)
    monkeypatch.chdir(elsewhere)

    configured, defaults = sdp.get_source_progress()["sources"]

    assert [stage["path"] for stage in configured["stages"]] == [str(project.root / "store" / "parsed"), str(absolute / "lingua"), str(absolute / "toxic")]
    assert [counts(items(configured, stage)["comments"]) for stage in ("parse", "lingua", "toxic")] == [(1, 0), (1, 0), (1, 0)]
    assert [stage["path"] for stage in defaults["stages"]] == [str(project.root / "data" / "parsed" / "defaults"), str(project.root / "data" / "output" / "defaults" / "lingua")]
    assert [counts(items(defaults, stage)["comments"]) for stage in ("parse", "lingua")] == [(1, 0), (1, 0)]


# ── a damaged config ────────────────────────────────────────────────────────


def test_a_data_type_that_is_not_one_path_component_is_never_joined_to_a_path(project):
    """Bug class: a data type from the config used as a folder name unchecked, so that `../other` reports on a folder outside the stage (here a real one, with files). Such an entry must be "not readable", which is another answer than "missing", and the data types around it must still be counted."""
    parsed = project.root / "data" / "parsed" / "src"
    project.source("src", 'data_types: ["../other", "a/b", comments, "."]\n')
    for folder in ("../other", "a/b", "comments"):
        put(parsed / folder / "RC_2024-01.parquet")
        put(parsed / folder / "RC_2024-02.parquet")

    entry = one_source(sdp.get_source_progress(name="src"))
    parse = items(entry, "parse")

    assert list(parse) == ["../other", "a/b", "comments", "."]
    for name in ("../other", "a/b", "."):
        assert parse[name] == {"data_type": name, **REFUSED}
    assert parse["../other"] != {"data_type": "../other", **MISSING}
    assert counts(parse["comments"]) == (2, 0)
    assert [item["not_readable"] for item in item_list(entry, "lingua")] == [DATA_TYPE_NOT_VALID] * 2 + [None] + [DATA_TYPE_NOT_VALID]


# A number PyYAML reads from 3600 hexadecimal digits: an int of more decimal digits than `str()` converts.
HUGE_NUMBER = "0x" + "f" * 3600


def test_a_data_type_that_is_not_text_gets_a_constant_label(project, capsys):
    """Bug class: a data type of the config that is not text turned into text for the report: `str()` of a number with more digits than Python converts raises ValueError (and takes the command down), a nested list or mapping is written out whole into the data, and the text of a small value names a folder that may exist (folders named like those texts sit here, with files) and must not be counted. Every such item gets one constant label and is "not readable"; the data types next to it in the same list are still counted."""
    parsed = project.root / "data" / "parsed" / "src"
    project.source("src", f"data_types: [{HUGE_NUMBER}, [nested, list], {{a: b}}, 5, null, true, 2024-03-05, comments]\n")
    for folder in ("5", "None", "True", "2024-03-05", "['nested', 'list']", "{'a': 'b'}", "comments"):
        put(parsed / folder / "RC_2024-01.parquet")
    loaded = utils.read_source_config("src")[0]["data_types"]
    assert [type(value).__name__ for value in loaded] == ["int", "list", "dict", "int", "NoneType", "bool", "date", "str"]
    with pytest.raises(ValueError):
        str(loaded[0])

    data = sdp.get_source_progress(name="src")
    entry = one_source(data)

    for stage in ("parse", "lingua"):
        assert item_list(entry, stage)[:7] == [{"data_type": NOT_TEXT, **REFUSED}] * 7
    assert item_list(entry, "parse")[7]["data_type"] == "comments" and counts(item_list(entry, "parse")[7]) == (1, 0)
    assert len(json.dumps(data)) < 5000
    assert capsys.readouterr().out == ""
    sdp.print_source_progress(data)
    printed = capsys.readouterr().out
    assert printed.count(f"      (not text): not readable ({DATA_TYPE_NOT_VALID})\n") == 14
    assert "      comments:   1 finished, newest: RC_2024-01.parquet (" in printed


def test_a_text_data_type_that_is_no_folder_name_is_cut_and_a_folder_name_is_not(project):
    """Bug class: a damaged `data_types` entry of any length copied whole into the report (a pasted paragraph, a megabyte of text), or the cut applied to a real data type, whose name would then no longer match its folder for the reader. A text that is no folder name is carried up to its first 80 characters and `...`; a short one and a valid one of any length (at most 255 bytes) are carried whole, and the valid one is counted."""
    parsed = project.root / "data" / "parsed" / "src"
    long_valid = "v" * 200
    project.source("src", f'data_types: ["{"x" * 300}", "../{"y" * 200}", "{"z" * 79}/", "{"w" * 80}/", {long_valid}, "\\t{"c" * 100}"]\n')
    put(parsed / long_valid / "RC_2024-01.parquet")
    put(parsed / ("y" * 200) / "RC_2024-01.parquet")

    parse = item_list(one_source(sdp.get_source_progress(name="src")), "parse")

    assert [item["data_type"] for item in parse] == ["x" * 80 + "...", "../" + "y" * 77 + "...", "z" * 79 + "/", "w" * 80 + "...", long_valid, "\\t" + "c" * 79 + "..."]
    assert [item["not_readable"] for item in parse] == [DATA_TYPE_NOT_VALID] * 4 + [None, DATA_TYPE_NOT_VALID]
    assert counts(parse[4]) == (1, 0)


# id, platform.yaml as text or bytes, whether the source counts as found, the reason expected.
DAMAGED_CONFIGS = [
    pytest.param("data_types: comments\n", True, DATA_TYPES_NOT_A_LIST, id="data-types-a-string"),
    pytest.param("data_types:\n", True, DATA_TYPES_NOT_A_LIST, id="data-types-null"),
    pytest.param("platform: custom\n", True, DATA_TYPES_NOT_A_LIST, id="data-types-absent"),
    pytest.param("data_types: {comments: 1}\n", True, DATA_TYPES_NOT_A_LIST, id="data-types-a-mapping"),
    pytest.param("data_types: [comments]\npaths: [a, b]\n", True, PATHS_NOT_A_MAPPING, id="paths-a-list"),
    pytest.param("data_types: [comments]\npaths:\n", True, PATHS_NOT_A_MAPPING, id="paths-null"),
    pytest.param("- data_types\n- comments\n", True, CONFIG_NOT_A_MAPPING, id="top-level-list"),
    pytest.param("comments\n", True, CONFIG_NOT_A_MAPPING, id="top-level-text"),
    pytest.param(b"data_types: [comm\xff\xfeents]\n", False, CONFIG_NOT_READABLE, id="not-utf-8"),
    pytest.param(golden.BROKEN_YAML, False, CONFIG_NOT_READABLE, id="not-yaml"),
    pytest.param("[" * 2000, False, CONFIG_NOT_READABLE, id="nested-too-deep"),
    pytest.param("data_types: !!bool maybe\n", False, CONFIG_NOT_READABLE, id="tagged-bool-that-is-none"),
    pytest.param('data_types: !!int ""\n', False, CONFIG_NOT_READABLE, id="tagged-int-that-is-empty"),
    pytest.param("data_types: !!timestamp x\n", False, CONFIG_NOT_READABLE, id="tagged-time-that-is-none"),
]


@pytest.mark.parametrize("content, found, reason", DAMAGED_CONFIGS)
def test_a_config_that_cannot_be_used_is_a_fact_not_a_traceback(content, found, reason, project, capsys):
    """Bug class: a platform.yaml damaged by hand taking the whole command down with a traceback (in the all-sources form, for every other source too), or being half-used: `data_types: comments` read as eight one-letter data types (folders named `c`, `o`, ... exist here and hold files), a null or a list where a mapping is expected, bytes that are not UTF-8, brackets nested deeper than the YAML parser can follow, a tagged value the parser cannot build. The source is reported with one constant reason and no stages, and the source next to it is still counted."""
    path = project.root / "config" / "sources" / "damaged" / "platform.yaml"
    path.parent.mkdir(parents=True)
    path.write_bytes(content if isinstance(content, bytes) else content.encode("utf-8"))
    project.source("fine", "data_types: [comments]\n")
    put(project.root / "data" / "parsed" / "fine" / "comments" / "RC_2024-01.parquet")
    for letter in "comments":
        put(project.root / "data" / "parsed" / "damaged" / letter / "RC_2024-01.parquet")

    data = sdp.get_source_progress()
    damaged, fine = data["sources"]

    assert (damaged["name"], damaged["found"], damaged["not_readable"], damaged["stages"], damaged["output"]) == ("damaged", found, reason, [], None)
    assert len(damaged["warnings"]) == (1 if content == golden.BROKEN_YAML else 0)
    assert counts(items(fine, "parse")["comments"]) == (1, 0)
    assert json.loads(json.dumps(data, allow_nan=False)) == data
    assert capsys.readouterr().out == ""
    sdp.print_source_progress(data)
    assert f"\n  Source 'damaged': not readable ({reason})\n" in capsys.readouterr().out


def test_a_source_folder_that_cannot_be_entered_is_not_readable(project, monkeypatch, capsys):
    """Bug class: an error of the file system while the config is looked for, which the config reader does not catch (a source folder without search permission makes the look for platform.yaml itself fail), escaping as a traceback. The source is reported as not readable with the constant reason, and the command returns 1 for it by name, as for every source whose config cannot be read."""
    def refused(source_name):
        raise PermissionError(13, "Permission denied")

    project.source("src", "data_types: [comments]\n")
    monkeypatch.setattr(utils, "read_source_config", refused)

    assert sdp.get_source_progress(name="src") == {"sources": [{"name": "src", "found": False, "warnings": [], "not_readable": CONFIG_NOT_READABLE, "stages": [], "output": None}]}
    assert capsys.readouterr().out == ""
    assert run_command("src") == 1
    assert f"\n  Source 'src': not readable ({CONFIG_NOT_READABLE})\n" in capsys.readouterr().out
    assert run_command() == 0


# id, the paths block of platform.yaml, the stages expected as (stage, reason), the output fact expected ({out} is the default output folder).
DAMAGED_PATHS = [
    pytest.param('{parsed: ""}', [("parse", PATH_NOT_VALID), ("lingua", None), ("toxic", None)], {"path": "{out}", "exists": True, "not_readable": None}, id="parsed-empty"),
    pytest.param("{parsed: 5}", [("parse", PATH_NOT_VALID), ("lingua", None), ("toxic", None)], {"path": "{out}", "exists": True, "not_readable": None}, id="parsed-a-number"),
    pytest.param("{parsed: null}", [("parse", PATH_NOT_VALID), ("lingua", None), ("toxic", None)], {"path": "{out}", "exists": True, "not_readable": None}, id="parsed-null"),
    pytest.param('{output: ""}', [("parse", None), ("lingua", PATH_NOT_VALID)], {"path": None, "exists": False, "not_readable": PATH_NOT_VALID}, id="output-empty"),
    pytest.param("{output: [a]}", [("parse", None), ("lingua", PATH_NOT_VALID)], {"path": None, "exists": False, "not_readable": PATH_NOT_VALID}, id="output-a-list"),
]


@pytest.mark.parametrize("paths, stages, output", DAMAGED_PATHS)
def test_a_path_that_is_not_text_makes_its_stages_not_readable(paths, stages, output, project, capsys):
    """Bug class: a path value of the config handed to the file system as it is: an empty string resolves to the project root itself (whose sub-folders would then be reported as stages and data types; a folder named like the data type sits there, with files), a number or a list raises. Only the stages under that path are "not readable"; the other path of the same config is still read."""
    project.source("src", f"data_types: [comments]\npaths: {paths}\n")
    out = project.root / "data" / "output" / "src"
    put(project.root / "comments" / "RC_2024-01.parquet")
    put(project.root / "lingua" / "comments" / "RC_2024-01.parquet")
    put(project.root / "data" / "parsed" / "src" / "comments" / "RC_2024-01.parquet")
    put(out / "lingua" / "comments" / "RC_2024-01_lingua.parquet")
    put(out / "toxic" / "comments" / "RC_2024-01_toxic.parquet")

    data = sdp.get_source_progress(name="src")
    entry = one_source(data)

    assert (entry["found"], entry["not_readable"]) == (True, None)
    assert [(stage["stage"], stage["not_readable"]) for stage in entry["stages"]] == stages
    for stage in entry["stages"]:
        if stage["not_readable"]:
            assert (stage["path"], stage["data_types"]) == (None, [])
        else:
            assert counts(items(entry, stage["stage"])["comments"]) == (1, 0)
    assert entry["output"] == {**output, "path": output["path"] and str(out)}
    sdp.print_source_progress(data)
    assert f": not readable ({PATH_NOT_VALID})\n" in capsys.readouterr().out


def test_a_path_the_file_system_cannot_take_is_not_readable(project, capsys):
    """Bug class: a path or a data type of the config that is text but no path at all (a NUL character, a lone surrogate written as a YAML escape) raising ValueError where only file system errors are expected, in the listing of a data type folder or of the output folder. Both become "not readable", and the path is carried in a form that can be printed."""
    project.source("src", 'data_types: [comments, "sur\\uD800"]\npaths: {parsed: "nul\\0parsed", output: "nul\\0output"}\n')
    project.source("plain", 'data_types: ["sur\\uD800"]\n')
    refused = {**MISSING, "not_readable": FOLDER_NOT_READABLE}

    data = sdp.get_source_progress()
    plain, src = data["sources"]

    assert unsafe_characters(data) == []
    assert [stage["path"] for stage in src["stages"]] == [str(project.root / "nul\\x00parsed"), str(project.root / "nul\\x00output" / "lingua")]
    assert items(src, "parse") == items(src, "lingua") == {"comments": {"data_type": "comments", **refused}, "sur\\ud800": {"data_type": "sur\\ud800", **refused}}
    assert src["output"] == {"path": str(project.root / "nul\\x00output"), "exists": False, "not_readable": FOLDER_NOT_READABLE}
    assert items(plain, "parse") == {"sur\\ud800": {"data_type": "sur\\ud800", **refused}}
    assert plain["output"] == {"path": str(project.root / "data" / "output" / "plain"), "exists": False, "not_readable": None}
    sdp.print_source_progress(data)
    assert f"    classifier folders: not readable ({FOLDER_NOT_READABLE})\n" in capsys.readouterr().out


# ── a damaged or changing tree ──────────────────────────────────────────────


def test_a_file_where_a_folder_is_expected_is_not_readable(project, capsys):
    """Bug class: an error of the file system other than "no such folder" escaping as a traceback (a regular file where a data type folder, a stage folder or the output folder is expected), or being answered with "no folder yet", which says the stage has not run when the truth is that nothing can be said."""
    project.source("src", "data_types: [comments, submissions]\n")
    project.source("flat", "data_types: [comments]\n")
    put(project.root / "data" / "parsed" / "src" / "comments")
    put(project.root / "data" / "parsed" / "src" / "submissions" / "RS_2024-01.parquet")
    put(project.root / "data" / "output" / "src" / "lingua")
    put(project.root / "data" / "output" / "flat")
    refused = {**MISSING, "not_readable": FOLDER_NOT_READABLE}

    data = sdp.get_source_progress()
    flat, src = data["sources"]

    assert items(src, "parse") == {"comments": {"data_type": "comments", **refused}, "submissions": {"data_type": "submissions", **MISSING, "exists": True, "finished": 1, "newest": items(src, "parse")["submissions"]["newest"]}}
    assert items(src, "lingua") == {"comments": {"data_type": "comments", **refused}, "submissions": {"data_type": "submissions", **refused}}
    assert src["output"]["not_readable"] is None
    assert flat["output"] == {"path": str(project.root / "data" / "output" / "flat"), "exists": False, "not_readable": FOLDER_NOT_READABLE}
    assert [stage["stage"] for stage in flat["stages"]] == ["parse", "lingua"]
    assert items(flat, "lingua") == {"comments": {"data_type": "comments", **refused}}
    sdp.print_source_progress(data)
    printed = capsys.readouterr().out
    assert f"      comments:    not readable ({FOLDER_NOT_READABLE})\n" in printed
    assert f"    classifier folders: not readable ({FOLDER_NOT_READABLE})\n" in printed


def stat_stub(monkeypatch, answers):
    """Replace `os.stat` for the paths that end in a key of answers: an exception is raised, anything else returned; every other path is answered by the real function."""
    real = os.stat

    def fake(path, *args, **kwargs):
        for ending, answer in answers.items():
            if isinstance(path, str) and path.endswith(ending):
                if isinstance(answer, BaseException):
                    raise answer
                return answer
        return real(path, *args, **kwargs)

    monkeypatch.setattr(sdp.os, "stat", fake)


def test_a_file_that_vanishes_after_the_listing_is_passed_over(project, monkeypatch):
    """Bug class: the command failing, or giving up on the whole folder, because a stage renamed or deleted one file between the directory listing and the look at that file. That happens on every run that is in progress, which is exactly when the command is used. The file that is gone is not counted, the others are."""
    comments = project.root / "data" / "parsed" / "src" / "comments"
    project.source("src", "data_types: [comments]\n")
    put(comments / "RC_2024-01.parquet", utc(2024, 3, 1))
    put(comments / "RC_2024-02.parquet", utc(2024, 3, 2))
    put(comments / "gone.parquet", utc(2024, 3, 9))
    put(comments / "RC_2024-03.parquet.temp")

    with monkeypatch.context() as patched:
        stat_stub(patched, {"gone.parquet": FileNotFoundError(2, "No such file or directory")})
        parse = items(one_source(sdp.get_source_progress(name="src")), "parse")

    assert parse["comments"] == {"data_type": "comments", "exists": True, "not_readable": None, "finished": 2, "being_written": 1, "newest": {"name": "RC_2024-02.parquet", "modified": "2024-03-02T00:00:00Z"}}


def test_a_file_that_cannot_be_looked_at_makes_the_folder_not_readable(project, monkeypatch):
    """Bug class: a folder whose files can be listed but not looked at (a permission on the folder) reported with a count that silently leaves those files out, which reads as "less work done" instead of "cannot tell"; or the error escaping as a traceback."""
    comments = project.root / "data" / "parsed" / "src" / "comments"
    project.source("src", "data_types: [comments]\n")
    put(comments / "RC_2024-01.parquet")
    put(comments / "locked.parquet")

    with monkeypatch.context() as patched:
        stat_stub(patched, {"locked.parquet": PermissionError(13, "Permission denied")})
        parse = items(one_source(sdp.get_source_progress(name="src")), "parse")

    assert parse["comments"] == {"data_type": "comments", **MISSING, "not_readable": FOLDER_NOT_READABLE}


def test_a_modification_time_no_date_can_hold_is_counted_and_not_the_newest(project, monkeypatch):
    """Bug class: one file with a modification time beyond what a date can express (a damaged file system, a clock set wrong when the file was written) raising while the time is turned into text, and so hiding the whole report. The file is counted; the newest file is the last one whose time can be told."""
    comments = project.root / "data" / "parsed" / "src" / "comments"
    project.source("src", "data_types: [comments]\n")
    put(comments / "RC_2024-01.parquet", utc(2024, 3, 1))
    put(comments / "odd.parquet")
    put(comments / "older.parquet", utc(2024, 2, 1))

    with monkeypatch.context() as patched:
        stat_stub(patched, {"odd.parquet": SimpleNamespace(st_mtime_ns=10**30)})
        parse = items(one_source(sdp.get_source_progress(name="src")), "parse")

    assert counts(parse["comments"]) == (3, 0)
    assert parse["comments"]["newest"] == {"name": "RC_2024-01.parquet", "modified": "2024-03-01T00:00:00Z"}


# ── names the terminal and JSON cannot take as they are ─────────────────────


def test_every_name_in_the_data_can_be_printed_and_sent(project, capsys):
    """Bug class: a name from the file system or the config passed on raw. An escape character in a file name rewrites the operator's terminal, a line break forges a line of the report, and a byte that is not UTF-8 (which arrives as a lone surrogate) makes `print` and the UTF-8 encoding of the JSON answer raise, so one odd file would take the report of every source down. Each such character must be replaced by a visible escape, in source names, data types, classifier folders, file names, paths and warnings; the names must stay recognisable, not be dropped."""
    sources = project.root / "config" / "sources"
    project.source("od\x1bd", 'data_types: [comments, "ta\\tb", "sep\\u2028", "sur\\uD800"]\n')
    project.source("bro\nken", golden.BROKEN_YAML)
    parsed = project.root / "data" / "parsed" / "od\x1bd"
    output = project.root / "data" / "output" / "od\x1bd"
    put(parsed / "comments" / "old.parquet", utc(2024, 3, 1))
    put(parsed / "comments" / "new\x1b[31m\n.parquet", utc(2024, 3, 2))
    put(output / "cl\x1bs\x85" / "comments" / "tem\rp.parquet.temp")
    expected_stages = ["parse", "lingua", "cl\\x1bs\\x85"]
    expected_names = ["bro\\nken", "od\\x1bd"]
    expected_newest = "new\\x1b[31m\\n.parquet"
    expected_finished = 2
    # Names that are not UTF-8, written as bytes. A file system that takes only UTF-8 names refuses them; the rest of the test still runs.
    try:
        put(os.fsencode(parsed / "comments") + b"/bad\xff.parquet", utc(2024, 3, 3))
        put(os.fsencode(output) + b"/raw\xfe/comments/x.parquet")
        put(os.fsencode(sources) + b"/src\xff/platform.yaml")
    except (OSError, UnicodeError):
        pass
    else:
        (project.root / "config" / "sources" / os.fsdecode(b"src\xff") / "platform.yaml").write_text("data_types: [comments]\n")
        expected_stages.append("raw\\udcfe")
        expected_names.append("src\\udcff")
        expected_newest = "bad\\udcff.parquet"
        expected_finished = 3

    data = sdp.get_source_progress()

    assert unsafe_characters(data) == []
    json.dumps(data, ensure_ascii=False).encode("utf-8")
    assert json.loads(json.dumps(data).encode("utf-8")) == data
    assert [entry["name"] for entry in data["sources"]] == expected_names
    odd = data["sources"][1]
    assert [stage["stage"] for stage in odd["stages"]] == expected_stages
    assert list(items(odd, "parse")) == ["comments", "ta\\tb", "sep\\u2028", "sur\\ud800"]
    assert items(odd, "parse")["comments"]["newest"]["name"] == expected_newest
    assert items(odd, "parse")["comments"]["finished"] == expected_finished
    assert odd["stages"][0]["path"] == str(project.root / "data" / "parsed") + "/od\\x1bd"
    assert items(odd, "cl\\x1bs\\x85")["comments"]["being_written"] == 1
    broken = data["sources"][0]
    assert (broken["found"], broken["not_readable"]) == (False, CONFIG_NOT_READABLE)
    assert len(broken["warnings"]) == 1 and "bro\\nken/platform.yaml" in broken["warnings"][0]

    sdp.print_source_progress(data)
    printed = capsys.readouterr().out
    assert [ch for ch in printed if ch != "\n" and unicodedata.category(ch) in UNSAFE_CATEGORIES] == []
    assert f"newest: {expected_newest} (" in printed
    assert "\n  Source: od\\x1bd\n" in printed


def test_format_characters_are_escaped(project, capsys):
    """Bug class: a format character let through because it is not a control character: a right-to-left override in a file name makes the terminal show the rest of the line reversed (`report<override>gpj.parquet` reads as another name, and the text after it is reordered too), a zero-width character makes two names look alike. Each is shown as its escape, in a file name, a classifier folder and a data type; the data type is still counted in its real folder."""
    parsed = project.root / "data" / "parsed" / "src"
    output = project.root / "data" / "output" / "src"
    project.source("src", 'data_types: [comments, "zero\\u200Bwidth"]\n')
    put(parsed / "comments" / "report\u202egpj.parquet")
    put(parsed / "zero\u200bwidth" / "RC_2024-01.parquet")
    put(output / "cl\u202es" / "comments" / "x\u2066y.parquet")

    data = sdp.get_source_progress(name="src")
    entry = one_source(data)

    assert unsafe_characters(data) == []
    assert items(entry, "parse")["comments"]["newest"]["name"] == "report\\u202egpj.parquet"
    assert counts(items(entry, "parse")["zero\\u200bwidth"]) == (1, 0)
    assert [stage["stage"] for stage in entry["stages"]] == ["parse", "lingua", "cl\\u202es"]
    assert items(entry, "cl\\u202es")["comments"]["newest"]["name"] == "x\\u2066y.parquet"
    assert entry["stages"][2]["path"] == str(output) + "/cl\\u202es"
    sdp.print_source_progress(data)
    printed = capsys.readouterr().out
    assert [ch for ch in printed if ch != "\n" and unicodedata.category(ch) in UNSAFE_CATEGORIES] == []
    assert "newest: report\\u202egpj.parquet (" in printed


# ── the public getter ───────────────────────────────────────────────────────


@pytest.mark.parametrize("name", ["../x", "", "a/b", "..", "ok\n", 5])
def test_getter_refuses_a_malformed_name_before_it_reads_anything(name, project, monkeypatch, capsys):
    """Bug class: the getter, called without the terminal command in front of it, counting files of a folder outside config/sources/ (`../x` holds a platform.yaml here), or treating the empty string as "all sources"; and an error text that repeats the name, which is whatever the caller sent. It raises before it lists a source, loads a config or lists a folder, and its message is a constant."""
    reads = []

    def must_not_read(*args, **kwargs):
        # Recorded as well as raised: the getter turns an error of the config reader into "not readable", so the raise alone would not show a read.
        reads.append(args)
        raise AssertionError("something was read for a malformed name")

    project.source("src", "data_types: [comments]\n")
    project.write("config/x/platform.yaml", "data_types: [comments]\n")
    monkeypatch.setattr(utils, "list_sources", must_not_read)
    monkeypatch.setattr(utils, "read_source_config", must_not_read)
    monkeypatch.setattr(sdp.os, "scandir", must_not_read)

    with pytest.raises(sdp.ActionError) as exc:
        sdp.get_source_progress(name=name)

    assert reads == []
    assert (exc.value.message, exc.value.exit_code) == (NAME_NOT_VALID, 1)
    assert capsys.readouterr().out == ""


def test_getter_answers_an_unknown_name_with_found_false(project, capsys):
    """Bug class: a well-formed name that is not a configured source answered with an error, with the default folders of that name counted as if it were one (files sit there), or with every source (None alone means all)."""
    project.source("src", "data_types: [comments]\n")
    put(project.root / "data" / "parsed" / "nosuch" / "comments" / "RC_2024-01.parquet")
    (project.root / "config" / "sources" / "empty").mkdir()
    (project.root / "config" / "sources" / "empty" / "platform.yaml").write_text("")

    assert sdp.get_source_progress(name="nosuch") == {"sources": [{"name": "nosuch", "found": False, "warnings": [], "not_readable": None, "stages": [], "output": None}]}
    assert sdp.get_source_progress(name="empty") == {"sources": [{"name": "empty", "found": False, "warnings": [], "not_readable": None, "stages": [], "output": None}]}
    assert [entry["name"] for entry in sdp.get_source_progress()["sources"]] == ["empty", "src"]
    assert sdp.get_source_progress(name=None) == sdp.get_source_progress()
    assert capsys.readouterr().out == ""


# ── the command ─────────────────────────────────────────────────────────────


def test_command_returns_1_only_for_a_named_source_that_is_not_found(project, capsys):
    """Bug class: a script or a caller that checks the exit code told "fine" for a source that does not exist, or told "failed" by the all-sources form because one source among many is damaged."""
    project.source("src", "data_types: [comments]\n")
    project.source("broken", golden.BROKEN_YAML)

    assert run_command("nosuch") == 1
    assert capsys.readouterr().out.endswith("=\n\n  Source 'nosuch': not found\n\n")
    assert run_command("nosuch", "--json") == 1
    assert json.loads(capsys.readouterr().out)["sources"][0]["found"] is False
    assert run_command("broken") == 1
    assert f"\n  Source 'broken': not readable ({CONFIG_NOT_READABLE})\n" in capsys.readouterr().out
    assert run_command("src") == 0
    assert "\n  Source: src\n" in capsys.readouterr().out
    project.source("aslist", "- data_types\n- comments\n")
    assert run_command("aslist") == 0
    assert f"\n  Source 'aslist': not readable ({CONFIG_NOT_A_MAPPING})\n" in capsys.readouterr().out
    assert run_command() == 0
    assert run_command("--json") == 0


def test_json_output_without_sources_is_an_empty_list(project, capsys):
    """Bug class: `--json` answering a deployment without sources with the sentence the text mode prints, which no JSON parser accepts."""
    assert run_command("--json") == 0
    assert json.loads(capsys.readouterr().out) == {"sources": []}


@pytest.mark.parametrize("flags", [(), ("--json",)], ids=["text", "json"])
def test_command_prints_the_refusal_of_a_malformed_name(flags, project, capsys):
    """Bug class: the refusal of the getter reaching the operator as a traceback, in either output mode, or with an exit code that says success; an error line that repeats the name typed; and, with `--json`, the error line on standard output, where a script that parses the output would be handed a line that is not JSON (there the line goes to standard error and standard output stays empty). The text mode prints it where the other commands print theirs."""
    project.write("config/x/platform.yaml", "data_types: [comments]\n")

    assert run_command("../x", *flags) == 1

    captured = capsys.readouterr()
    if flags:
        assert (captured.out, captured.err) == ("", f"  Error: {NAME_NOT_VALID}\n")
    else:
        assert (captured.out, captured.err) == (f"\n  Error: {NAME_NOT_VALID}\n\n", "")


def test_json_output_is_the_data_of_the_getter_and_nothing_else(project, capsys):
    """Bug class: `--json` printing something a JSON parser rejects (a heading, a warning line, the text report next to it), or other data than the getter returns, so that a script and the web client would see two different reports; and a getter that prints, which would put text into the output of whoever calls it. The tree has a warning, a name with an escape character and a file being written, the things most likely to be printed on the side."""
    project.source("od\x1bd", "data_types: [comments]\n")
    project.source("broken", golden.BROKEN_YAML)
    put(project.root / "data" / "parsed" / "od\x1bd" / "comments" / "RC_2024-01.parquet", utc(2024, 3, 1))
    put(project.root / "data" / "parsed" / "od\x1bd" / "comments" / "RC_2024-02.parquet.temp")

    data = sdp.get_source_progress()
    captured = capsys.readouterr()
    assert (captured.out, captured.err) == ("", "")
    assert [len(entry["warnings"]) for entry in data["sources"]] == [1, 0]

    assert run_command("--json") == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out) == data
    assert captured.out == json.dumps(data, indent=2) + "\n"
    assert captured.err == ""

    assert run_command("broken", "--json") == 1
    assert json.loads(capsys.readouterr().out) == {"sources": [data["sources"][0]]}


# ── the text ────────────────────────────────────────────────────────────────


@pytest.fixture
def five_hours_east():
    """Pin the local time zone to UTC+5 for one test. The zone is written in the form that needs no time zone database. The zone of the process is set back from the restored environment whatever happens in between, a failing check in here included."""
    try:
        with pytest.MonkeyPatch.context() as patched:
            patched.setenv("TZ", "<+05>-5")
            time.tzset()
            assert time.timezone == -5 * 3600
            yield
    finally:
        time.tzset()


PROGRESS_TEXT = """
  Social Data Pipeline - Source Progress
  ======================================

  Source 'broken': not readable (the source config is not a mapping)

  Source: reddit
    parse:
      comments:    2 finished, 1 being written, newest: RC_2024-02.parquet (2024-03-05 15:11 local time)
      submissions: no folder yet
    lingua:
      comments:    1 finished, newest: RC_2024-01_lingua.parquet (2024-03-06 04:30 local time)
      submissions: not readable (the folder cannot be read)
    toxic_roberta:
      comments:    0 finished
      submissions: no folder yet

  Note: a file being written can also be left over from an interrupted run.

"""


def test_text_of_a_fixed_tree(project, five_hours_east, capsys):
    """Bug class: any drift of the text the operator reads: a count on the wrong line, the "being written" part shown at zero, a missing folder and an unreadable one worded alike, columns that do not line up, the closing note missing when a file is being written, or the time of the newest file shown in UTC under the words "local time" (the zone is pinned five hours east, so the hour, and for one file the day, differ from the UTC time in the data). The text is printed from the data after a JSON round trip, as a caller that received it would."""
    parsed = project.root / "data" / "parsed" / "reddit"
    output = project.root / "data" / "output" / "reddit"
    project.source("reddit", "data_types: [comments, submissions]\n")
    project.source("broken", "- comments\n")
    put(parsed / "comments" / "RC_2024-01.parquet", utc(2024, 3, 5, 9))
    put(parsed / "comments" / "RC_2024-02.parquet", utc(2024, 3, 5, 10, 11, 12))
    put(parsed / "comments" / "RC_2024-03.parquet.temp", utc(2024, 3, 5, 11))
    put(output / "lingua" / "comments" / "RC_2024-01_lingua.parquet", utc(2024, 3, 5, 23, 30))
    put(output / "lingua" / "submissions")
    (output / "toxic_roberta" / "comments").mkdir(parents=True)

    data = sdp.get_source_progress()
    reddit = data["sources"][1]

    assert items(reddit, "parse")["comments"]["newest"]["modified"] == "2024-03-05T10:11:12Z"
    assert items(reddit, "lingua")["comments"]["newest"]["modified"] == "2024-03-05T23:30:00Z"
    sdp.print_source_progress(json.loads(json.dumps(data)))
    assert capsys.readouterr().out == PROGRESS_TEXT


def test_text_without_a_file_being_written_has_no_note(project, capsys):
    """Bug class: the note about leftover files printed on every report, where it is noise, or the "being written" part printed with a zero."""
    project.source("reddit", "data_types: [comments]\n")
    put(project.root / "data" / "parsed" / "reddit" / "comments" / "RC_2024-01.parquet")

    sdp.print_source_progress(sdp.get_source_progress())

    printed = capsys.readouterr().out
    assert "      comments: 1 finished, newest: RC_2024-01.parquet (" in printed
    assert "being written" not in printed
    assert printed.endswith(" local time)\n    lingua:\n      comments: no folder yet\n\n")


def test_text_without_sources_is_the_line_of_source_status(project, capsys):
    """Bug class: a fresh project answered with an empty report or a heading and nothing under it, instead of the line that says what to do next, which is the one `source status` prints."""
    sdp.print_source_progress(sdp.get_source_progress())
    progress = capsys.readouterr().out
    sdp.print_source_status(sdp.get_source_status())

    assert progress == capsys.readouterr().out == "\n  No sources configured. Run: python sdp.py source add <name>\n\n"
    assert run_command() == 0
