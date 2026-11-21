"""Tests for the `runs` commands of the CLI: the functions that return the data of `runs list` and `runs logs` and that cancel a run (`get_run_list`, `get_run_logs`, `do_run_cancel`), their printers, the terminal commands, and their three verbs in the registry.

The run store behind them has its own tests; the contracts pinned here are the CLI's:

- A run id that has neither of the two forms, and a well-formed one that names no run, is refused with a constant message that does not hold the id, before a path is built or Docker is asked; a number out of range likewise.
- Every string that comes from Docker or a record is display-safe in the data; the text of a log is the one exception and is returned as decoded.
- The getters print nothing, return JSON data, never raise because Docker is out of reach, and carry nothing of a container's environment but the source.
- `runs list` prints one aligned line per run, or "No runs.", and one note when Docker cannot be reached; `--json` prints the getter's data. `runs logs` prints the log as it is, whole or its last lines, and can follow it. The exit code is 1 for a refused or unknown id and for a refused cancel, 0 otherwise.
- The registry's `RunId` type refuses exactly what the store refuses, the socket's number ranges are the getters' ranges, and a validated request is what the action takes.

The CLI is pointed at a temp folder and Docker is replaced by the `fake_run_docker` fixture.
"""

from __future__ import annotations

import contextlib
import json
import os
import subprocess
import sys
import time
import unicodedata
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

import sdp
from social_data_pipeline import runstore
from social_data_pipeline.control import errors, types, verbs

REPO_ROOT = Path(__file__).resolve().parents[2]

ID_NOT_VALID = "the run id is not a valid run id"
NOT_FOUND = "no run with this id"
LIMIT_NOT_VALID = "the limit is not a whole number in the accepted range"
OFFSET_NOT_VALID = "the offset is not a whole number in the accepted range"
NOT_REACHABLE = "Docker cannot be reached"
NOT_A_FOLDER = "the run state folder is not a real folder (a symbolic link or a file is in its place)"
DOCKER_NOTE = "  Note: Docker cannot be reached. Runs started in a terminal are not listed, and a recorded run that has not ended and has no live supervisor shows as unknown.\n\n"

RUN_A = "r20261121T101500-0a1b2c"
RUN_B = "r20261121T101600-ffffff"
RUN_C = "r20261122T000000-000000"
CONTAINER_A = "a1" * 32
CONTAINER_B = "b2" * 32
LIVE_A = "live-" + "a1" * 6
LIVE_B = "live-" + "b2" * 6

# The separators and the next-line character are written as escapes: typed raw, an editor may turn them into spaces.
REFUSED_IDS = [
    "", ".", "..", "../" + RUN_A, RUN_A + "/..", RUN_A + "/", "/" + RUN_A, RUN_A + "\n", LIVE_A + "\n", RUN_A + "\x00", RUN_A + "\u2028", RUN_A + "\x85",
    "r20261121T101500-0A1B2C", "live-" + "A1" * 6, "R20261121T101500-0a1b2c", "r2026112T101500-0a1b2c", "r20261121T101500-0a1b2", "r20261121T101500-0a1b2c0",
    "r\u0662\u0660261121T101500-0a1b2c", "live-" + "a1" * 5 + "a", "live-" + "a1" * 6 + "a", "live-", "ZQX9marker", "ZQX9marker" * 50,
    RUN_A.encode(), [RUN_A], 20261121, None, True,
]
ACCEPTED_IDS = [RUN_A, RUN_B, RUN_C, "r00000000T000000-000000", LIVE_A, "live-0123456789ab"]

UNSAFE_CATEGORIES = ("Cc", "Cf", "Zl", "Zp", "Cs")
MARKER = "made-up-secret-marker-7f3a"

# No test of this file reaches the real Docker: the store's Docker function is replaced for every test, and a test that reaches the replacement fails.
pytestmark = pytest.mark.usefixtures("run_docker_guard")


# ── helpers ─────────────────────────────────────────────────────────────────


@pytest.fixture
def project(tmp_path, monkeypatch, fake_run_docker):
    """An empty temp deployment the CLI points at, with Docker replaced. `.root` is its folder, `.docker` the fake."""
    root = tmp_path.resolve() / "project"
    root.mkdir()
    monkeypatch.setattr(sdp, "ROOT", root)
    # The tests hold run locks on purpose and need no pause before "held" is the answer.
    monkeypatch.setattr(runstore, "_LOCK_RETRY_SECONDS", 0)
    return SimpleNamespace(root=root, docker=fake_run_docker)


def make_run(root, run_id, state="starting", *, profile="parse", source="reddit", started="2026-11-21T10:15:00Z", **changes):
    """Create a recorded run with a chosen id and start time through the store's API, and move it to a state."""
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(runstore, "new_run_id", lambda: run_id)
        patch.setattr(runstore, "_now", lambda: started)
        assert runstore.create_run(root, {"profile": profile, "source": source, "flags": {"build": False}, "started_by": "operator"}) == run_id
    if state != "starting" or changes:
        runstore.update_record(root, run_id, state=state, **changes)
    return run_id


def write_log(root, run_id, content):
    fd = runstore.open_run_log(root, run_id)
    try:
        os.write(fd, content)
    finally:
        os.close(fd)


def fixed_runs(project):
    """One run of each kind the listing shows: finished, running under a live supervisor, unreadable, and one started in a terminal. Returns the descriptor that holds the running run's lock."""
    make_run(project.root, RUN_A, "finished", ended="2026-11-21T10:20:00Z", exit_code=0)
    make_run(project.root, RUN_B, "running", profile="mongo_ingest", started="2026-11-21T10:16:00Z")
    make_run(project.root, RUN_C, "running")
    (project.root / ".sdp" / "runs" / RUN_C / "record.json").write_text("{")
    project.docker.add(CONTAINER_A, project.root, service="ml-gpu", source=None)
    return runstore.acquire_run_lock(project.root, RUN_B)


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
        assert value is None or isinstance(value, (bool, int)), type(value)


def unsafe_characters(data):
    """The control characters, format characters and lone surrogates left anywhere in the data, as (string, character)."""
    return [(text, ch) for text in strings(data) for ch in text if unicodedata.category(ch) in UNSAFE_CATEGORIES]


def run_command(*argv):
    """Run a `runs` command through the real parser and return its exit code."""
    args = sdp.build_parser().parse_args(["runs", *argv])
    return args.func(args)


PATH_FUNCTIONS = [(os, "open"), (os, "mkdir"), (os, "stat"), (os, "lstat"), (os, "listdir"), (os, "scandir"), (os, "replace"), (os, "unlink"), (os, "fspath"), (os.path, "join"), (os.path, "realpath")]


@contextlib.contextmanager
def recorded_path_calls():
    """Record every call of the `os` functions through which a path is built or used, and still make it. A stub that only raises could be swallowed by a handler, so the calls are kept in a list."""
    calls = []

    def recording(real, name):
        def call(*args, **kwargs):
            calls.append(name)
            return real(*args, **kwargs)
        return call

    with pytest.MonkeyPatch.context() as patch:
        for module, name in PATH_FUNCTIONS:
            patch.setattr(module, name, recording(getattr(module, name), name))
        yield calls


def outcome_of(call):
    """Call and return (result, exception): an assertion is then made outside any patch."""
    try:
        return call(), None
    except Exception as error:
        return None, error


# ── the data ────────────────────────────────────────────────────────────────


class TestData:
    def test_the_service_of_every_profile(self):
        """Bug class: a pipeline profile whose compose service is missing from, or misnamed in, the table the run listing is filtered by, so that its terminal runs are never listed and cannot be cancelled. The table is written out, and each service must be one the compose file defines under that profile."""
        table = sdp._run_service_profiles()

        assert table == {"parse": "parse", "lingua": "lingua", "ml-gpu": "ml", "postgres-ingest": "postgres_ingest", "postgres-ml": "postgres_ml", "mongo-ingest": "mongo_ingest", "sr-ingest": "sr_ingest", "sr-ml": "sr_ml"}
        assert sorted(table.values()) == sorted(sdp.VALID_PROFILES)
        services = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text())["services"]
        assert {service: profile in services[service]["profiles"] for service, profile in table.items()} == dict.fromkeys(table, True)

    def test_the_data_of_the_listing(self, project, capsys):
        """Bug class: a getter that prints, returns something JSON cannot carry, or hands on the store's entry with a field renamed or missing, so that a page built on the documented fields shows a blank."""
        supervisor = fixed_runs(project)

        data = sdp.get_run_list()

        assert capsys.readouterr() == ("", "")
        assert data == {"docker_available": True, "runs": [
            {"id": RUN_C, "kind": "web", "profile": None, "source": None, "flags": None, "started_by": None, "state": "unreadable", "started": None, "ended": None, "exit_code": None},
            {"id": RUN_B, "kind": "web", "profile": "mongo_ingest", "source": "reddit", "flags": {"build": False}, "started_by": "operator", "state": "running", "started": "2026-11-21T10:16:00Z", "ended": None, "exit_code": None},
            {"id": RUN_A, "kind": "web", "profile": "parse", "source": "reddit", "flags": {"build": False}, "started_by": "operator", "state": "finished", "started": "2026-11-21T10:15:00Z", "ended": "2026-11-21T10:20:00Z", "exit_code": 0},
            {"id": LIVE_A, "kind": "terminal", "profile": "ml", "source": None, "flags": None, "started_by": None, "state": "running", "started": "2026-11-21T10:30:00Z", "ended": None, "exit_code": None},
        ]}
        assert json.loads(json.dumps(data, allow_nan=False)) == data
        assert [run["id"] for run in sdp.get_run_list(limit=1)["runs"]] == [RUN_C, LIVE_A]
        runstore.release_run_lock(supervisor)

    def test_strings_from_docker_and_records_are_display_safe(self, project, capsys):
        """Bug class: text that someone else wrote reaching a terminal or a page raw: a container's SOURCE variable or a field of a record with an escape sequence that clears the screen, a direction override that reorders the line, a line break that forges a second run, or a lone surrogate that cannot be sent as UTF-8. Every string of the listing is escaped, the keys and values of the flags included, and the text still names the run."""
        project.docker.add(CONTAINER_A, project.root, source="red\x1b[2Jdit\u202e\x00\u2028x")
        make_run(project.root, RUN_A, "finished")
        record = project.root / ".sdp" / "runs" / RUN_A / "record.json"
        stored = json.loads(record.read_text())
        stored.update(profile="par\x1bse", source="src\nr20261121T000000-000000  web  parse", started_by="op\x07\ud800", flags={"fil\x1bter": "a\rb\u2029", "build": True, "n": 3, "none": None})
        record.write_text(json.dumps(stored))

        data = sdp.get_run_list()

        assert unsafe_characters(data) == []
        recorded, terminal = data["runs"]
        assert terminal["source"] == "red\\x1b[2Jdit\\u202e\\x00\\u2028x"
        assert (recorded["profile"], recorded["source"], recorded["started_by"]) == ("par\\x1bse", "src\\nr20261121T000000-000000  web  parse", "op\\x07\\ud800")
        assert recorded["flags"] == {"fil\\x1bter": "a\\rb\\u2029", "build": True, "n": 3, "none": None}
        json.dumps(data).encode("utf-8")
        sdp.print_run_list(data)
        printed = capsys.readouterr().out
        assert unsafe_characters(printed.replace("\n", "")) == [] and len(printed.splitlines()) == 8

    def test_the_text_of_a_log_is_returned_as_decoded(self, project):
        """Bug class: the log escaped like a name, which turns every progress bar and every colour of the program's output into backslash sequences in a view that renders them. The log text is the one string that is not made display-safe; everything around it is."""
        make_run(project.root, RUN_A, "finished")
        write_log(project.root, RUN_A, "50%\r100%\n\x1b[32mdone\x1b[0m caf\u00e9\n".encode() + b"\xff")
        project.docker.add(CONTAINER_A, project.root, log="50%\r100%\n\x1b[32mdone\x1b[0m\n")

        assert sdp.get_run_logs(run_id=RUN_A) == {"id": RUN_A, "kind": "web", "state": "finished", "text": "50%\r100%\n\x1b[32mdone\x1b[0m caf\u00e9\n\ufffd", "next_offset": 30, "complete": True}
        assert sdp.get_run_logs(run_id=RUN_A, offset=4, limit=5) == {"id": RUN_A, "kind": "web", "state": "finished", "text": "100%\n", "next_offset": 9, "complete": True}
        assert sdp.get_run_logs(run_id=LIVE_A) == {"id": LIVE_A, "kind": "terminal", "state": "running", "text": "50%\r100%\n\x1b[32mdone\x1b[0m\n", "next_offset": None, "complete": False}

    @pytest.mark.parametrize("run_id", REFUSED_IDS, ids=repr)
    @pytest.mark.parametrize("action", ["get_run_logs", "do_run_cancel"])
    def test_a_malformed_run_id_is_refused_before_anything_is_touched(self, project, capsys, action, run_id):
        """Bug class: an action called directly (no terminal command and no registry in front of it) joining whatever it was sent to the state folder or handing it to Docker, and an error that repeats the value, which is text the caller wrote. The action raises with the constant message before it builds a path or asks Docker; the calls are recorded, because a handler could swallow a stub that raises."""
        make_run(project.root, RUN_A, "running")
        project.docker.add(CONTAINER_A, project.root)

        with recorded_path_calls() as path_calls:
            _, error = outcome_of(lambda: getattr(sdp, action)(run_id=run_id))

        assert isinstance(error, sdp.ActionError)
        assert (error.message, error.exit_code, str(error)) == (ID_NOT_VALID, 1, ID_NOT_VALID)
        assert error.__cause__ is None and error.__suppress_context__
        assert path_calls == [] and project.docker.calls == []
        assert capsys.readouterr() == ("", "")

    @pytest.mark.parametrize("run_id", [RUN_B, "r00000000T000000-000000", LIVE_B, "live-0123456789ab"])
    @pytest.mark.parametrize("action", ["get_run_logs", "do_run_cancel"])
    def test_an_unknown_run_id_is_refused(self, project, capsys, action, run_id):
        """Bug class: a well-formed id that names no run answered with an empty log (it reads as "the run printed nothing"), with "cancelled", or by creating its folder; and a refusal that quotes the id. The control proves a known id of each kind is served."""
        make_run(project.root, RUN_A, "finished")
        project.docker.add(CONTAINER_A, project.root, log="x\n")
        assert sdp.get_run_logs(run_id=RUN_A)["text"] == "" and sdp.get_run_logs(run_id=LIVE_A)["text"] == "x\n"
        before = sorted(str(path) for path in project.root.rglob("*"))

        with pytest.raises(sdp.ActionError) as exc:
            getattr(sdp, action)(run_id=run_id)

        assert (exc.value.message, exc.value.exit_code) == (NOT_FOUND, 1)
        assert sorted(str(path) for path in project.root.rglob("*")) == before
        assert project.docker.named("stop") == [] and capsys.readouterr() == ("", "")

    @pytest.mark.parametrize("call, message", [
        (lambda: sdp.get_run_list(limit=0), LIMIT_NOT_VALID),
        (lambda: sdp.get_run_list(limit=1001), LIMIT_NOT_VALID),
        (lambda: sdp.get_run_list(limit=-1), LIMIT_NOT_VALID),
        (lambda: sdp.get_run_list(limit=True), LIMIT_NOT_VALID),
        (lambda: sdp.get_run_list(limit="5"), LIMIT_NOT_VALID),
        (lambda: sdp.get_run_list(limit=5.0), LIMIT_NOT_VALID),
        (lambda: sdp.get_run_list(limit=None), LIMIT_NOT_VALID),
        (lambda: sdp.get_run_logs(run_id=RUN_A, limit=0), LIMIT_NOT_VALID),
        (lambda: sdp.get_run_logs(run_id=RUN_A, limit=1048577), LIMIT_NOT_VALID),
        (lambda: sdp.get_run_logs(run_id=RUN_A, limit=None), LIMIT_NOT_VALID),
        (lambda: sdp.get_run_logs(run_id=RUN_A, offset=-1), OFFSET_NOT_VALID),
        (lambda: sdp.get_run_logs(run_id=RUN_A, offset=2 ** 53 + 1), OFFSET_NOT_VALID),
        (lambda: sdp.get_run_logs(run_id=RUN_A, offset="0"), OFFSET_NOT_VALID),
        (lambda: sdp.get_run_logs(run_id=RUN_A, offset=False), OFFSET_NOT_VALID),
        (lambda: sdp.get_run_logs(run_id=RUN_A, offset=None), OFFSET_NOT_VALID),
    ])
    def test_a_number_out_of_range_is_refused(self, project, call, message):
        """Bug class: a limit or an offset that is negative, huge or not a whole number reaching a read (a negative offset reads from the end of some files, a limit of a few gigabytes is allocated before it is read), or being coerced (`True` read as 1, text as a number)."""
        make_run(project.root, RUN_A, "finished")
        write_log(project.root, RUN_A, b"0123456789")

        with recorded_path_calls() as path_calls:
            _, error = outcome_of(call)

        assert isinstance(error, sdp.ActionError) and (error.message, error.exit_code) == (message, 1)
        assert path_calls == [] and project.docker.calls == []

    def test_docker_out_of_reach(self, project, capsys):
        """Bug class: `runs list` failing on a host whose Docker daemon is down, and the opposite slip, a terminal run's log or cancel answered as if there were no such run when Docker simply could not be asked."""
        make_run(project.root, RUN_A, "finished", ended="2026-11-21T10:20:00Z", exit_code=0)
        make_run(project.root, RUN_B, "running")
        project.docker.add(CONTAINER_A, project.root)
        project.docker.reachable = False

        data = sdp.get_run_list()

        assert (data["docker_available"], [(run["id"], run["state"]) for run in data["runs"]]) == (False, [(RUN_B, "unknown"), (RUN_A, "finished")])
        assert sdp.get_run_logs(run_id=RUN_A)["state"] == "finished"
        for call in (lambda: sdp.get_run_logs(run_id=LIVE_A), lambda: sdp.do_run_cancel(run_id=LIVE_A), lambda: sdp.do_run_cancel(run_id=RUN_B)):
            with pytest.raises(sdp.ActionError) as exc:
                call()
            assert (exc.value.message, exc.value.exit_code) == (NOT_REACHABLE, 1)
        assert capsys.readouterr() == ("", "")

    def test_a_linked_state_folder_is_refused(self, project, tmp_path):
        """Bug class: the store's refusal of a symbolic link in the state folder's place surfacing from the getter as a traceback, or being read as "no runs"."""
        (tmp_path / "elsewhere").mkdir()
        (project.root / ".sdp").symlink_to(tmp_path / "elsewhere", target_is_directory=True)

        for call in (lambda: sdp.get_run_list(), lambda: sdp.get_run_logs(run_id=RUN_A), lambda: sdp.do_run_cancel(run_id=RUN_A)):
            with pytest.raises(sdp.ActionError) as exc:
                call()
            assert exc.value.message == NOT_A_FOLDER

    def test_nothing_of_a_containers_environment_but_the_source(self, project, capsys):
        """Bug class: a database password from a container's environment in the data a web client receives, or on the terminal. If Docker answered the inspect call with the whole environment, the listing must still hold none of it; the listing of containers is answered with a foreign container whose labels hold the marker too."""
        project.docker.add(CONTAINER_A, project.root, log="started\n")
        project.docker.add(CONTAINER_B, f"/srv/{MARKER}", service=MARKER)
        project.docker.inspect_answer = json.dumps([{"State": {"StartedAt": "2026-11-21T10:30:00Z"}, "Config": {"Env": ["SOURCE=reddit", f"POSTGRES_PASSWORD={MARKER}", f"MONGO_ADMIN_PASSWORD={MARKER}"]}}])
        project.docker.extra_ps_lines = [f"POSTGRES_PASSWORD={MARKER}"]

        data = sdp.get_run_list()
        logs = sdp.get_run_logs(run_id=LIVE_A)
        sdp.print_run_list(data)
        assert sdp.do_run_cancel(run_id=LIVE_A) == 0

        assert [run["id"] for run in data["runs"]] == [LIVE_A]
        assert MARKER not in json.dumps([data, logs]) + "".join(capsys.readouterr())
        assert all("Env" not in " ".join(call) or call[:2] == ["inspect", "--format"] and call[2] == runstore.INSPECT_TEMPLATE for call in project.docker.calls)


# ── the commands ────────────────────────────────────────────────────────────


LIST_TEXT = """
  Social Data Pipeline - Runs
  ===========================

  ID                       KIND      PROFILE       SOURCE  STATE       STARTED               ENDED                 EXIT
  r20261122T000000-000000  web       -             -       unreadable  -                     -                     -
  r20261121T101600-ffffff  web       mongo_ingest  reddit  running     2026-11-21T10:16:00Z  -                     -
  r20261121T101500-0a1b2c  web       parse         reddit  finished    2026-11-21T10:15:00Z  2026-11-21T10:20:00Z  0
  live-a1a1a1a1a1a1        terminal  ml            -       running     2026-11-21T10:30:00Z  -                     -

"""


class TestList:
    def test_the_text(self, project, capsys):
        """Bug class: a change of what `runs list` prints that nobody decided: a column dropped or reordered, a missing value shown as `None`, columns that no longer line up when one value is long, an exit code of 0 shown as missing. The whole text for one fixed set of runs is compared."""
        supervisor = fixed_runs(project)

        assert run_command("list") == 0

        captured = capsys.readouterr()
        assert captured.out == LIST_TEXT and captured.err == ""
        runstore.release_run_lock(supervisor)

    def test_json_is_the_getters_data(self, project, capsys):
        """Bug class: `--json` printing something other than what a caller of the getter receives (the text mixed in, a field formatted for the terminal), so that a script and the web UI disagree about a run."""
        supervisor = fixed_runs(project)

        assert run_command("list", "--json") == 0
        whole = capsys.readouterr()
        assert run_command("list", "--json", "--limit", "1") == 0
        limited = capsys.readouterr()

        assert whole.err == "" and json.loads(whole.out) == sdp.get_run_list()
        assert json.loads(limited.out) == sdp.get_run_list(limit=1) and len(json.loads(limited.out)["runs"]) == 2
        runstore.release_run_lock(supervisor)

    def test_no_runs_and_docker_out_of_reach(self, project, capsys):
        """Bug class: an empty listing printed as a bare header, and a listing made without Docker that does not say so, which reads as "nothing is running" on a host where a pipeline is at work."""
        assert run_command("list") == 0
        assert capsys.readouterr().out == "\n  No runs.\n\n"

        project.docker.reachable = False
        assert run_command("list") == 0
        assert capsys.readouterr().out == "\n  No runs.\n\n" + DOCKER_NOTE

        make_run(project.root, RUN_A, "running")
        assert run_command("list") == 0
        out = capsys.readouterr().out
        assert out.endswith("  r20261121T101500-0a1b2c  web   parse    reddit  unknown  2026-11-21T10:15:00Z  -      -\n\n" + DOCKER_NOTE)
        assert run_command("list", "--json") == 0
        assert json.loads(capsys.readouterr().out)["docker_available"] is False

    def test_no_terminal_row_above_the_note(self, project, capsys):
        """Bug class: `runs list` printing the row of a terminal run and, below it, the note that terminal runs are not listed, because Docker answered the first calls of the listing and not a later one."""
        make_run(project.root, RUN_A, "running")
        project.docker.add(CONTAINER_A, project.root)
        project.docker.unreachable_from = 2

        assert run_command("list") == 0

        out = capsys.readouterr().out
        assert LIVE_A not in out and "terminal" not in out.replace(DOCKER_NOTE, "")
        assert out.endswith("  r20261121T101500-0a1b2c  web   parse    reddit  unknown  2026-11-21T10:15:00Z  -      -\n\n" + DOCKER_NOTE)

    def test_a_refused_limit(self, project, capsys):
        """Bug class: a limit out of range printed as a traceback, or, with `--json`, as a line on standard output that a reader of the JSON cannot parse."""
        assert run_command("list", "--limit", "0") == 1
        assert capsys.readouterr() == (f"\n  Error: {LIMIT_NOT_VALID}\n\n", "")

        assert run_command("list", "--json", "--limit", "1001") == 1
        assert capsys.readouterr() == ("", f"  Error: {LIMIT_NOT_VALID}\n")


class TestLogs:
    # Longer than one piece below, with characters of every length next to the cuts.
    LOG = "first line\nsecond \u00e9\u20ac\U0001f600 line\nthird line\nlast line without an end"

    def test_the_whole_log_is_printed_as_it_is(self, project, monkeypatch, capsys):
        """Bug class: `runs logs` printing only the first piece of a long log, repeating or dropping the bytes where two pieces meet, or adding line ends of its own. The log is read in pieces to its end and written out unchanged."""
        make_run(project.root, RUN_A, "finished")
        write_log(project.root, RUN_A, self.LOG.encode())
        monkeypatch.setattr(sdp, "_RUN_LOG_PIECE", 7)

        assert run_command("logs", RUN_A) == 0

        assert capsys.readouterr() == (self.LOG, "")

    def test_offset_and_limit(self, project, capsys):
        """Bug class: `--offset` and `--limit` not reaching the read, or `--limit` read as "piece size" so that the command prints the rest of the log after all."""
        make_run(project.root, RUN_A, "finished")
        write_log(project.root, RUN_A, self.LOG.encode())

        assert run_command("logs", RUN_A, "--offset", "11", "--limit", "6") == 0
        assert capsys.readouterr().out == "second"
        assert run_command("logs", RUN_A, "--offset", "33") == 0
        assert capsys.readouterr().out == "third line\nlast line without an end"
        assert run_command("logs", RUN_A, "--offset", "-1") == 1
        assert capsys.readouterr().out == f"\n  Error: {OFFSET_NOT_VALID}\n\n"
        assert run_command("logs", RUN_A, "--limit", "0") == 1
        assert capsys.readouterr().out == f"\n  Error: {LIMIT_NOT_VALID}\n\n"

    @pytest.mark.parametrize("tail, expected", [
        ("1", "last line without an end"),
        ("2", "third line\nlast line without an end"),
        ("3", "second \u00e9\u20ac\U0001f600 line\nthird line\nlast line without an end"),
        ("4", LOG),
        ("100", LOG),
        ("0", ""),
    ])
    def test_tail_of_a_log_file(self, project, monkeypatch, capsys, tail, expected):
        """Bug class: `--tail` taken from the first piece of the log instead of its end, a line that two pieces share counted as two, or the last line lost when it has no line end."""
        make_run(project.root, RUN_A, "finished")
        write_log(project.root, RUN_A, self.LOG.encode())
        monkeypatch.setattr(sdp, "_RUN_LOG_PIECE", 7)

        assert run_command("logs", RUN_A, "--tail", tail) == 0

        assert capsys.readouterr() == (expected, "")

    def test_tail_of_a_terminal_run_and_of_a_log_that_ends_in_a_newline(self, project, capsys):
        """Bug class: the empty text after a final line end counted as a line, so that `--tail 1` prints nothing; and a carriage return, which a progress bar uses to rewrite its line, counted as a line end."""
        project.docker.add(CONTAINER_A, project.root, log="one\ntwo 10%\rtwo 90%\nthree\n")

        assert run_command("logs", LIVE_A, "--tail", "2") == 0
        assert capsys.readouterr().out == "two 10%\rtwo 90%\nthree\n"
        assert run_command("logs", LIVE_A, "--tail", "1") == 0
        assert capsys.readouterr().out == "three\n"
        assert run_command("logs", LIVE_A) == 0
        assert capsys.readouterr().out == "one\ntwo 10%\rtwo 90%\nthree\n"
        assert run_command("logs", LIVE_A, "--limit", "6") == 0
        assert capsys.readouterr().out == "three\n"
        assert sdp.get_run_logs(run_id=LIVE_A, limit=10) == {"id": LIVE_A, "kind": "terminal", "state": "running", "text": "90%\nthree\n", "next_offset": None, "complete": False}
        assert run_command("logs", LIVE_A, "--tail", "-1") == 1
        assert capsys.readouterr().out == "\n  Error: --tail must not be negative\n\n"

    @pytest.mark.parametrize("run_id", ["../x", RUN_A + "\n", "ZQX9marker", "live-ZQX9marker", RUN_B, LIVE_B])
    @pytest.mark.parametrize("command", ["logs", "cancel"])
    def test_a_refused_or_unknown_id_is_exit_code_1_and_is_not_echoed(self, project, capsys, command, run_id):
        """Bug class: the command line answering a mistyped or made-up id with exit code 0 (a script goes on as if the run were cancelled), with a traceback, or with a message that repeats what was typed."""
        make_run(project.root, RUN_A, "finished")
        message = NOT_FOUND if run_id in (RUN_B, LIVE_B) else ID_NOT_VALID

        assert run_command(command, run_id) == 1

        captured = capsys.readouterr()
        assert captured == (f"\n  Error: {message}\n\n", "")
        assert project.docker.named("stop") == []

    def test_output_that_docker_does_not_hand_over_is_an_error_not_an_empty_log(self, project, capsys):
        """Bug class: `runs logs` printing nothing with exit code 0, or "no run with this id", for a run that is alive and listed but whose output Docker would not give, or while the daemon is going down."""
        project.docker.add(CONTAINER_A, project.root, log="one\n")
        project.docker.logs_fail = True

        with pytest.raises(sdp.ActionError) as exc:
            sdp.get_run_logs(run_id=LIVE_A)
        assert exc.value.message == "the run's output cannot be read"
        assert run_command("logs", LIVE_A) == 1
        assert capsys.readouterr().out == "\n  Error: the run's output cannot be read\n\n"

        project.docker.logs_fail = False
        project.docker.daemon_down_from = len(project.docker.calls) + 1
        assert run_command("logs", LIVE_A) == 1
        assert capsys.readouterr().out == f"\n  Error: {NOT_REACHABLE}\n\n"

    def test_follow_a_log_file_until_the_run_ends(self, project, monkeypatch, capsys):
        """Bug class: `--follow` stopping at the first end of the log although a live process still writes it, never stopping after the run has ended, or printing a line twice. The log is read again about once a second while the run is supervised and once more after it ended."""
        make_run(project.root, RUN_A, "running")
        write_log(project.root, RUN_A, b"one\n")
        supervisor = runstore.acquire_run_lock(project.root, RUN_A)
        pauses = []

        def supervisor_works(seconds):
            pauses.append(seconds)
            if len(pauses) == 1:
                write_log(project.root, RUN_A, b"two\n")
            elif len(pauses) == 2:
                write_log(project.root, RUN_A, b"three\n")
                runstore.update_record(project.root, RUN_A, state="finished", ended="2026-11-21T10:20:00Z", exit_code=0)
                runstore.release_run_lock(supervisor)
            else:
                raise AssertionError("the run has ended and the log is still followed")

        monkeypatch.setattr(time, "sleep", supervisor_works)

        assert run_command("logs", RUN_A, "--follow") == 0

        assert capsys.readouterr() == ("one\ntwo\nthree\n", "")
        assert pauses == [1, 1] and project.docker.calls == []

    def test_follow_a_terminal_run_hands_over_to_docker(self, project, capsys):
        """Bug class: following a terminal run by asking Docker for the last lines again and again (every line printed many times), or handing Docker a container that is not the looked-up run. Docker follows the log itself, attached to the terminal, from the last lines on."""
        project.docker.add(CONTAINER_A, project.root, log="one\n")
        ps = ["ps", "--no-trunc", "--filter", "label=com.docker.compose.oneoff=True", "--format", runstore.PS_FORMAT]

        assert run_command("logs", LIVE_A, "--follow") == 0
        assert run_command("logs", LIVE_A, "--follow", "--tail", "5") == 0

        assert capsys.readouterr() == ("", "")
        assert project.docker.calls == [
            ps, ["logs", "--tail", "500", CONTAINER_A], ps, ["logs", "--follow", "--tail", "500", CONTAINER_A],
            ps, ["logs", "--tail", "500", CONTAINER_A], ps, ["logs", "--follow", "--tail", "5", CONTAINER_A],
        ]
        assert [options["attached"] for options in project.docker.options] == [False, False, False, True] * 2

    def test_ctrl_c_ends_following(self, project, monkeypatch, capsys):
        """Bug class: Ctrl-C, the only way to stop following a run that goes on, reported as an aborted command with exit code 1; and the same key swallowed by a plain `runs logs`."""
        make_run(project.root, RUN_A, "running")
        supervisor = runstore.acquire_run_lock(project.root, RUN_A)

        def interrupted(seconds):
            raise KeyboardInterrupt

        monkeypatch.setattr(time, "sleep", interrupted)
        assert run_command("logs", RUN_A, "--follow") == 0
        assert capsys.readouterr().out == "\n"

        monkeypatch.setattr(sdp, "get_run_logs", lambda **kwargs: interrupted(0))
        with pytest.raises(KeyboardInterrupt):
            run_command("logs", RUN_A)
        runstore.release_run_lock(supervisor)


class TestCancel:
    def test_what_cancel_says_and_returns(self, project, capsys):
        """Bug class: a cancel whose exit code or line does not say what happened: 0 for a run that was refused, 1 for one that was stopped, "stopped" for a run that was only asked to stop. Each outcome has its line and its code, and a second cancel does no harm."""
        make_run(project.root, RUN_A, "finished", ended="2026-11-21T10:20:00Z", exit_code=0)
        make_run(project.root, RUN_B, "running")
        make_run(project.root, RUN_C, "running")
        supervisor = runstore.acquire_run_lock(project.root, RUN_B)
        project.docker.add(CONTAINER_A, project.root)
        project.docker.add(CONTAINER_B, project.root, run=RUN_C)

        outcomes = []
        for run_id in (LIVE_A, LIVE_A, RUN_A, RUN_B, RUN_B, RUN_C, RUN_C):
            code, error = outcome_of(lambda: run_command("cancel", run_id))
            assert error is None
            outcomes.append((code, capsys.readouterr().out))

        assert outcomes == [
            (0, f"  Run {LIVE_A}: interrupt signal sent, the container has stopped.\n"),
            (1, f"\n  Error: {NOT_FOUND}\n\n"),
            (1, f"  Run {RUN_A}: not cancelled, the run has already ended.\n"),
            (0, f"  Run {RUN_B}: cancel requested, the process that holds the run stops it.\n"),
            (0, f"  Run {RUN_B}: cancel requested, the process that holds the run stops it.\n"),
            (0, f"  Run {RUN_C}: interrupt signal sent, the container has stopped.\n"),
            (1, f"  Run {RUN_C}: not cancelled, the run has already ended.\n"),
        ]
        assert project.docker.named("stop") == [["stop", "--signal", "INT", "-t", "30", CONTAINER_A], ["stop", "--signal", "INT", "-t", "30", CONTAINER_B]]
        assert [runstore.read_record(project.root, run_id)["state"] for run_id in (RUN_A, RUN_B, RUN_C)] == ["finished", "running", "cancelled"]
        runstore.release_run_lock(supervisor)

    def test_a_stop_that_failed_is_not_reported_as_done(self, project, capsys):
        """Bug class: `runs cancel` answering "stopped", "already ended" or exit code 0 for a container Docker could not stop, and recording the run as cancelled, after which it runs on and cannot be cancelled again."""
        make_run(project.root, RUN_A, "running")
        project.docker.add(CONTAINER_A, project.root)
        project.docker.add(CONTAINER_B, project.root, run=RUN_A)
        project.docker.stop_fails = True

        assert sdp.do_run_cancel(run_id=LIVE_A) == 1
        assert sdp.do_run_cancel(run_id=RUN_A) == 1

        assert capsys.readouterr().out == f"  Run {LIVE_A}: not cancelled, Docker could not stop the run's container.\n  Run {RUN_A}: not cancelled, Docker could not stop the run's container.\n"
        assert [run["state"] for run in sdp.get_run_list()["runs"]] == ["detached", "running"]

    def test_a_stopped_run_whose_record_could_not_be_written(self, project, capsys):
        """Bug class: the store's refusal for a record with no room for its end (one this store did not write) reaching the command line as a traceback, or as "stopped" with exit code 0 although the record still says the run is running."""
        make_run(project.root, RUN_A, "running")
        record = project.root / ".sdp" / "runs" / RUN_A / "record.json"
        stored = json.loads(record.read_text())
        stored["flags"] = {"filter": "x" * (65530 - len(record.read_bytes()) - len('"filter": ""') + len('"build": false'))}
        record.write_text(json.dumps(stored, indent=2) + "\n")
        assert 65517 <= record.stat().st_size <= 65536
        project.docker.add(CONTAINER_A, project.root, run=RUN_A)

        with pytest.raises(sdp.ActionError) as exc:
            sdp.do_run_cancel(run_id=RUN_A)
        assert exc.value.message == "the run has no running container any more, but its record could not be written"
        project.docker.add(CONTAINER_A, project.root, run=RUN_A)
        assert run_command("cancel", RUN_A) == 1
        assert capsys.readouterr().out == "\n  Error: the run has no running container any more, but its record could not be written\n\n"

    def test_a_lost_or_unreadable_run_is_refused(self, project, capsys):
        """Bug class: "cancelled" reported for a run nobody can stop any more, or whose record cannot be read."""
        make_run(project.root, RUN_A, "running")
        make_run(project.root, RUN_B, "running")
        (project.root / ".sdp" / "runs" / RUN_B / "record.json").write_text("[]")

        assert sdp.do_run_cancel(run_id=RUN_A) == 1
        assert sdp.do_run_cancel(run_id=RUN_B) == 1

        assert capsys.readouterr().out == f"  Run {RUN_A}: not cancelled, the run is no longer running.\n  Run {RUN_B}: not cancelled, the run's record cannot be read.\n"


class TestGroup:
    def test_bare_runs_prints_the_groups_help_like_bare_db(self, monkeypatch, capsys):
        """Bug class: `sdp runs` without a command printing the top-level help or nothing, or ending differently from the other command groups. It prints the help of its own group and ends the way a bare `sdp db` does."""
        ends = {}
        for group in ("db", "runs"):
            monkeypatch.setattr(sys, "argv", ["sdp.py", group])
            with pytest.raises(SystemExit) as exc:
                sdp.main()
            ends[group] = (exc.value.code, capsys.readouterr())

        assert ends["runs"][0] == ends["db"][0] == 0
        assert ends["runs"][1].out.startswith("usage: sdp.py runs [-h] {list,logs,cancel} ...") and ends["runs"][1].err == ""
        assert ends["db"][1].out.startswith("usage: sdp.py db [-h]")

    def test_loading_the_cli_loads_no_run_store(self):
        """Bug class: the CLI importing the run store at load time, so that every command, the setup ones included, pays for it and a fault in it breaks them all. Run in a fresh interpreter, since this process has imported both."""
        code = "import sys; import sdp; bad = sorted(m for m in sys.modules if m.startswith('social_data_pipeline.runstore')); print(bad); sys.exit(1 if bad else 0)"

        result = subprocess.run([sys.executable, "-c", code], cwd=REPO_ROOT, capture_output=True, text=True)

        assert result.returncode == 0, result.stdout + result.stderr


# ── the registry ────────────────────────────────────────────────────────────


class TestRegistry:
    @pytest.mark.parametrize("run_id", REFUSED_IDS + ACCEPTED_IDS, ids=repr)
    def test_the_run_id_type_and_the_store_agree(self, run_id):
        """Bug class: the registry's copy of the run id pattern drifting from the store's, so that the socket accepts an id the store then refuses with another error, or, worse, the registry stops refusing a form the store's own check was loosened to accept. Both must refuse and accept the same values, and the registry's pattern is the store's two patterns joined."""
        store_accepts = outcome_of(lambda: runstore.run_id_kind(run_id))[1] is None
        registry_accepts = outcome_of(lambda: types.RunId().validate(run_id))[1] is None

        assert store_accepts == registry_accepts == (run_id in ACCEPTED_IDS)
        assert types.RUN_ID_PATTERN == f"{runstore.RUN_ID_PATTERN}|{runstore.LIVE_ID_PATTERN}"

    @pytest.mark.parametrize("verb, field, action, others", [
        ("run.list", "limit", "get_run_list", {}),
        ("run.logs", "limit", "get_run_logs", {"run_id": RUN_A}),
        ("run.logs", "offset", "get_run_logs", {"run_id": RUN_A}),
    ])
    def test_the_sockets_number_ranges_are_the_getters(self, project, verb, field, action, others):
        """Bug class: the two copies of a range drifting apart: a request the registry accepts and the getter then refuses, or a getter that takes a larger read than the socket was reviewed for."""
        make_run(project.root, RUN_A, "finished")
        kind = verbs.VERBS[verb].socket_inputs[field].type

        for value in (kind.lo, kind.hi):
            assert getattr(sdp, action)(**verbs.validate_request(verb, {**others, field: value})) is not None
        for value in (kind.lo - 1, kind.hi + 1):
            with pytest.raises(errors.InputError):
                verbs.validate_request(verb, {**others, field: value})
            with pytest.raises(sdp.ActionError):
                getattr(sdp, action)(**{**others, field: value})

    def test_a_validated_request_is_what_the_action_takes(self, project, capsys):
        """Bug class: a verb whose validated fields its action does not accept, which would only show as a TypeError when the first request arrives. Each of the three verbs is run from a request, through the registry, to its action."""
        make_run(project.root, RUN_A, "finished")
        write_log(project.root, RUN_A, b"line\n")
        project.docker.add(CONTAINER_A, project.root)

        listed = sdp.get_run_list(**verbs.validate_request("run.list", {"limit": 10}))
        logs = sdp.get_run_logs(**verbs.validate_request("run.logs", {"run_id": RUN_A, "offset": 0, "limit": 100}))
        cancelled = sdp.do_run_cancel(**verbs.validate_request("run.cancel", {"run_id": LIVE_A}))

        assert [run["id"] for run in listed["runs"]] == [RUN_A, LIVE_A]
        assert (logs["text"], cancelled) == ("line\n", 0)
        assert {name: (verbs.VERBS[name].cls, verbs.VERBS[name].cli) for name in ("run.list", "run.logs", "run.cancel")} == {"run.list": ("read", ("runs", "list")), "run.logs": ("read", ("runs", "logs")), "run.cancel": ("write", ("runs", "cancel"))}
        for refused in ({"run_id": "../x"}, {"run_id": RUN_A, "follow": True}, {"run_id": RUN_A, "tail": 5}, {}):
            with pytest.raises(errors.InputError):
                verbs.validate_request("run.logs", refused)
