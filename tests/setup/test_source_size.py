"""Tests for `source size`: the getter that measures the bytes on disk of a source's four stage folders (dumps, extracted, parsed, output), its printer and its terminal command.

The contracts pinned here:

- Each stage folder is walked to any depth; the regular files under it are counted and their sizes summed. A link inside the tree is never followed; the stage folder itself is opened as configured. A relative path of the config is under the project root, wherever the command is run from.
- A missing stage folder is 0 bytes and does not make the report incomplete; a sub-folder that cannot be listed makes its stage not readable (the bytes found so far still counted) and the total "at least". A file or folder that disappears during the walk is passed over.
- Two stages configured on one folder are counted once in the total.
- Nothing in a damaged config raises: it becomes a "not readable" fact with a constant reason.
- The getter prints nothing and returns JSON-serialisable data; the command returns 1 only for a named source that is not found, and with `--json` writes a refusal to standard error.

Everything lives in a temp folder the CLI is pointed at; nothing touches the real tree or Docker.
"""

from __future__ import annotations

import json
import os
import shutil
from types import SimpleNamespace

import pytest

import sdp
import test_status_golden as golden
from social_data_pipeline.setup import utils

NAME_NOT_VALID = "the source name is not a valid name"
CONFIG_NOT_READABLE = "the source config cannot be read"
CONFIG_NOT_A_MAPPING = "the source config is not a mapping"
PATHS_NOT_A_MAPPING = "paths in the source config is not a mapping"
PATH_NOT_VALID = "the path in the source config is empty or not text"
FOLDER_NOT_READABLE = "the folder cannot be read"


@pytest.fixture
def project(tmp_path, monkeypatch):
    """An empty temp project the CLI points at, run from another folder. `.root` is its folder."""
    root = tmp_path.resolve() / "project"
    made = golden.Project(root)
    golden.point_at(monkeypatch, root)
    elsewhere = tmp_path / "cwd"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    return made


def put(path, size):
    """Create a file of `size` bytes (and its folders)."""
    os.makedirs(os.path.dirname(os.fspath(path)), exist_ok=True)
    with open(path, "wb") as handle:
        handle.write(b"x" * size)


def one_source(data):
    assert len(data["sources"]) == 1
    return data["sources"][0]


def stages(entry):
    return {stage["stage"]: stage for stage in entry["stages"]}


def run_command(*argv):
    """Run `source size` through the real parser and return its exit code."""
    args = sdp.build_parser().parse_args(["source", "size", *argv])
    return args.func(args)


# ── measuring ───────────────────────────────────────────────────────────────


def test_bytes_and_files_are_summed_per_stage_and_in_total(project):
    """Bug class: a size that is not what the stage holds on disk: only the top level counted (dumps and extracted nest by data type), folders counted as files, the stages in another order than the pipeline's, a relative path resolved against the working directory (the command runs from elsewhere here), or a total that is not the sum."""
    data_dir = project.root / "data"
    project.source("src", "data_types: [comments]\n")
    put(data_dir / "dumps" / "src" / "comments" / "RC_2024-01.zst", 1000)
    put(data_dir / "dumps" / "src" / "submissions" / "deep" / "RS_2024-01.zst", 500)
    put(data_dir / "extracted" / "src" / "comments" / "RC_2024-01", 2000)
    put(data_dir / "parsed" / "src" / "comments" / "RC_2024-01.parquet", 300)
    put(data_dir / "output" / "src" / "lingua" / "comments" / "RC_2024-01_lingua.parquet", 40)
    (data_dir / "output" / "src" / "toxic" / "empty").mkdir(parents=True)

    entry = one_source(sdp.get_source_size(name="src"))

    assert [stage["stage"] for stage in entry["stages"]] == ["dumps", "extracted", "parsed", "output"]
    assert {name: (stage["files"], stage["bytes"], stage["exists"], stage["not_readable"]) for name, stage in stages(entry).items()} == {
        "dumps": (2, 1500, True, None), "extracted": (1, 2000, True, None), "parsed": (1, 300, True, None), "output": (1, 40, True, None),
    }
    assert stages(entry)["parsed"]["path"] == str(data_dir / "parsed" / "src")
    assert (entry["found"], entry["total_bytes"], entry["complete"]) == (True, 3840, True)


def test_a_missing_stage_folder_is_zero_bytes_and_still_complete(project):
    """Bug class: a source that has not been parsed yet reported as incomplete or not readable (the web UI would then say "at least" for a figure that is exact), or a configured path outside the default honoured for some stages only."""
    outside = project.root.parent / "elsewhere" / "dumps"
    project.source("src", f"data_types: [comments]\npaths:\n  dumps: {outside}\n")
    put(outside / "a.zst", 7)

    entry = one_source(sdp.get_source_size(name="src"))

    assert stages(entry)["dumps"] == {"stage": "dumps", "path": str(outside), "exists": True, "not_readable": None, "files": 1, "bytes": 7}
    assert stages(entry)["parsed"] == {"stage": "parsed", "path": str(project.root / "data" / "parsed" / "src"), "exists": False, "not_readable": None, "files": 0, "bytes": 0}
    assert (entry["total_bytes"], entry["complete"]) == (7, True)


def test_a_link_inside_a_stage_is_not_followed(project):
    """Bug class: a link inside a stage folder followed, so a folder or file elsewhere (another source's dumps, a whole disk) is counted as this stage's bytes, or a link loop never ends. The stage folder itself may be a link and is followed, as the stages follow it."""
    data_dir = project.root / "data"
    target = project.root.parent / "big"
    put(target / "huge.bin", 10_000)
    project.source("src", f"data_types: [comments]\npaths:\n  parsed: {project.root.parent / 'linked-parsed'}\n")
    put(data_dir / "dumps" / "src" / "a.zst", 5)
    os.symlink(target, data_dir / "dumps" / "src" / "to-folder")
    os.symlink(target / "huge.bin", data_dir / "dumps" / "src" / "to-file")
    os.symlink(data_dir / "dumps" / "src", data_dir / "dumps" / "src" / "loop")
    os.symlink(target, project.root.parent / "linked-parsed")

    entry = stages(one_source(sdp.get_source_size(name="src")))

    assert (entry["dumps"]["files"], entry["dumps"]["bytes"]) == (1, 5)
    assert (entry["parsed"]["files"], entry["parsed"]["bytes"]) == (1, 10_000)


def test_two_stages_on_one_folder_are_counted_once_in_the_total(project):
    """Bug class: a source whose extracted folder is its dumps folder (or a link to it) reported at twice its size on disk. Each stage still shows its own figure."""
    shared = project.root / "data" / "shared"
    put(shared / "a.json", 100)
    os.symlink(shared, project.root / "data" / "shared-link")
    project.source("src", f"paths:\n  dumps: ./data/shared\n  extracted: {shared}/\n  parsed: ./data/shared-link\n")

    entry = one_source(sdp.get_source_size(name="src"))

    assert [stage["bytes"] for stage in entry["stages"]] == [100, 100, 100, 0]
    assert entry["total_bytes"] == 100


def test_a_file_or_folder_vanishing_during_the_walk_is_passed_over(project, monkeypatch):
    """Bug class: a stage that renames or deletes a file while the walk runs (parse finishing a file, a cleanup) turning the whole report into an error or the stage into "not readable"."""
    dumps = project.root / "data" / "dumps" / "src"
    put(dumps / "kept.zst", 10)
    put(dumps / "gone.zst", 20)
    put(dumps / "gonedir" / "x.zst", 30)
    project.source("src", "data_types: [comments]\n")
    real_scandir = os.scandir

    class Listing:
        """A listing that deletes `gone.zst` and `gonedir` after they were listed and before they are looked at."""

        def __init__(self, path):
            self.inner = real_scandir(path)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            self.inner.close()

        def __iter__(self):
            for item in self.inner:
                if item.name == "gone.zst":
                    os.unlink(item.path)
                elif item.name == "gonedir":
                    shutil.rmtree(item.path)
                yield item

    monkeypatch.setattr(sdp.os, "scandir", Listing)

    entry = one_source(sdp.get_source_size(name="src"))

    assert stages(entry)["dumps"] == {"stage": "dumps", "path": str(dumps), "exists": True, "not_readable": None, "files": 1, "bytes": 10}
    assert entry["complete"] is True


def test_a_sub_folder_that_cannot_be_listed_keeps_the_bytes_found_and_marks_the_total(project, monkeypatch, capsys):
    """Bug class: a folder without read permission inside a stage either raising, or silently dropped so the total looks exact when it is only a lower bound."""
    dumps = project.root / "data" / "dumps" / "src"
    put(dumps / "a.zst", 10)
    put(dumps / "locked" / "b.zst", 99)
    project.source("src", "data_types: [comments]\n")
    real_scandir = os.scandir

    def scandir(path):
        if os.fspath(path).endswith("locked"):
            raise PermissionError(13, "Permission denied")
        return real_scandir(path)

    monkeypatch.setattr(sdp.os, "scandir", scandir)

    entry = one_source(sdp.get_source_size(name="src"))

    assert stages(entry)["dumps"] == {"stage": "dumps", "path": str(dumps), "exists": True, "not_readable": FOLDER_NOT_READABLE, "files": 1, "bytes": 10}
    assert (entry["total_bytes"], entry["complete"]) == (10, False)
    sdp.print_source_size(json.loads(json.dumps({"sources": [entry]})))
    printed = capsys.readouterr().out
    assert f"    dumps:      not readable ({FOLDER_NOT_READABLE}), at least 10 B in 1 file  {dumps}\n" in printed
    assert "    Total:      at least 10 B\n" in printed


# ── damaged configs ─────────────────────────────────────────────────────────


@pytest.mark.parametrize("platform, reason", [
    pytest.param("- comments\n", CONFIG_NOT_A_MAPPING, id="a-list"),
    pytest.param("paths: [a, b]\n", PATHS_NOT_A_MAPPING, id="paths-a-list"),
    pytest.param("paths: ./data\n", PATHS_NOT_A_MAPPING, id="paths-a-string"),
])
def test_a_config_of_the_wrong_shape_is_found_but_not_readable(project, platform, reason):
    """Bug class: a platform.yaml of the wrong shape raising (a string's `.get`, a list iterated as a mapping) instead of being reported with a constant reason, or its stages measured from default folders the config does not name."""
    project.source("src", platform)
    put(project.root / "data" / "dumps" / "src" / "a.zst", 5)

    assert sdp.get_source_size(name="src") == {"sources": [{"name": "src", "found": True, "warnings": [], "not_readable": reason, "stages": [], "total_bytes": 0, "complete": False}]}


def test_a_config_the_reader_cannot_read_is_not_readable(project, monkeypatch, capsys):
    """Bug class: an error the config reader lets through (a source folder that cannot be entered, a tagged YAML value it cannot build) escaping as a traceback and hiding every other source. The command returns 1 for it by name, and 0 for all sources."""
    def refused(source_name):
        raise PermissionError(13, "Permission denied")

    project.source("src", "data_types: [comments]\n")
    monkeypatch.setattr(utils, "read_source_config", refused)

    assert sdp.get_source_size(name="src") == {"sources": [{"name": "src", "found": False, "warnings": [], "not_readable": CONFIG_NOT_READABLE, "stages": [], "total_bytes": 0, "complete": False}]}
    assert capsys.readouterr().out == ""
    assert run_command("src") == 1
    assert f"\n  Source 'src': not readable ({CONFIG_NOT_READABLE})\n" in capsys.readouterr().out
    assert run_command() == 0


def test_a_tagged_value_the_parser_cannot_build_is_not_readable(project):
    """Bug class: PyYAML's KeyError/IndexError for a tag such as `!!bool maybe`, which the config reader does not catch, raising out of the getter."""
    project.source("src", "paths: !!bool maybe\n")

    assert one_source(sdp.get_source_size(name="src"))["not_readable"] == CONFIG_NOT_READABLE


def test_a_stage_path_that_is_not_text_is_that_stage_not_readable(project):
    """Bug class: a path that is a number, a list or empty joined or converted (`str()` of a huge number raises) instead of reported, or one bad stage hiding the others."""
    project.source("src", "paths:\n  dumps: 12345\n  extracted: ''\n  parsed: [a]\n")
    put(project.root / "data" / "output" / "src" / "a.csv", 3)

    entry = one_source(sdp.get_source_size(name="src"))

    assert [(stage["stage"], stage["path"], stage["not_readable"]) for stage in entry["stages"][:3]] == [("dumps", None, PATH_NOT_VALID), ("extracted", None, PATH_NOT_VALID), ("parsed", None, PATH_NOT_VALID)]
    assert (stages(entry)["output"]["bytes"], entry["total_bytes"], entry["complete"]) == (3, 3, False)


def test_a_path_with_control_characters_is_escaped(project):
    """Bug class: a path from the config carried raw into the data, so a terminal or a page shows an escape sequence or a line break the config holds."""
    project.source("src", 'paths:\n  dumps: "./data/a\\x1b[2Jb\\u202e"\n')

    path = stages(one_source(sdp.get_source_size(name="src")))["dumps"]["path"]

    assert path == str(project.root / "data") + "/a\\x1b[2Jb\\u202e"


# ── names, data and command ─────────────────────────────────────────────────


@pytest.mark.parametrize("name", ["../x", "a/b", ".", "", "ok\n", 5])
def test_getter_refuses_a_malformed_name_before_reading(project, monkeypatch, name):
    """Bug class: a name sent by a caller joined to config/sources/ and the folders of another place measured."""
    reads = []
    monkeypatch.setattr(utils, "list_sources", lambda *a: reads.append(a))
    monkeypatch.setattr(utils, "read_source_config", lambda *a: reads.append(a))

    with pytest.raises(sdp.ActionError) as exc:
        sdp.get_source_size(name=name)

    assert reads == []
    assert (exc.value.message, exc.value.exit_code) == (NAME_NOT_VALID, 1)


def test_unknown_name_is_found_false_and_all_sources_are_in_list_order(project):
    """Bug class: a well-formed name that is not configured measured from its default folders, or answered with every source."""
    project.source("b", "{}\n")
    project.source("a", "{}\n")
    put(project.root / "data" / "dumps" / "nosuch" / "x.zst", 5)

    assert sdp.get_source_size(name="nosuch") == {"sources": [{"name": "nosuch", "found": False, "warnings": [], "not_readable": None, "stages": [], "total_bytes": 0, "complete": False}]}
    assert [entry["name"] for entry in sdp.get_source_size()["sources"]] == ["a", "b"]


def test_json_round_trip_text_and_exit_codes(project, capsys):
    """Bug class: drift between the getter, the text and the JSON: data that does not survive JSON, a getter that prints, the stages on misaligned lines, "at least" on an exact total, a missing named source answered with 0, or a refusal written into the JSON stream."""
    data_dir = project.root / "data"
    project.source("src", "{}\n")
    put(data_dir / "dumps" / "src" / "a.zst", 1_234_567)
    put(data_dir / "parsed" / "src" / "b.parquet", 1)

    data = sdp.get_source_size()
    assert capsys.readouterr().out == ""
    assert json.loads(json.dumps(data)) == data

    sdp.print_source_size(json.loads(json.dumps(data)))
    assert capsys.readouterr().out == (
        "\n  Social Data Pipeline - Source Size\n  ==================================\n\n  Source: src\n"
        f"    dumps:         1.2 MB  1 file   {data_dir}/dumps/src\n"
        f"    extracted:  no folder           {data_dir}/extracted/src\n"
        f"    parsed:           1 B  1 file   {data_dir}/parsed/src\n"
        f"    output:     no folder           {data_dir}/output/src\n"
        "    Total:         1.2 MB\n\n"
    )

    assert run_command("src", "--json") == 0
    assert json.loads(capsys.readouterr().out) == {"sources": data["sources"]}
    assert run_command("nosuch") == 1
    assert "\n  Source 'nosuch': not found\n" in capsys.readouterr().out
    args = SimpleNamespace(name="../x", json=True)
    assert sdp.cmd_source_size(args) == 1
    captured = capsys.readouterr()
    assert (captured.out, captured.err) == ("", f"  Error: {NAME_NOT_VALID}\n")


def test_no_sources_prints_the_line_of_source_status(project, capsys):
    """Bug class: a fresh project answered with an empty heading instead of the line that says what to do next."""
    assert run_command() == 0
    assert capsys.readouterr().out == "\n  No sources configured. Run: python sdp.py source add <name>\n\n"


@pytest.mark.parametrize("size, text", [
    (0, "0 B"), (999, "999 B"), (1000, "1.0 kB"), (1049, "1.0 kB"), (1050, "1.1 kB"), (999_949, "999.9 kB"), (999_950, "1.0 MB"),
    (12_345_678, "12.3 MB"), (1_400_000_000, "1.4 GB"), (2 * 10**12, "2.0 TB"), (3 * 10**15, "3.0 PB"), (5 * 10**18, "5000.0 PB"),
])
def test_size_text_at_the_unit_boundaries(size, text):
    """Bug class: a size shown as `1000.0 kB` where it rounds into the next unit, binary units where decimal ones are promised, or a size past the last unit failing."""
    assert sdp._format_size(size) == text
