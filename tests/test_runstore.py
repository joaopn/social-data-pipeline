"""Tests for the run store: the records, logs and locks of pipeline runs under `<root>/.sdp/`, the runs started in a terminal that it finds through Docker, and cancelling either kind.

The contracts pinned here:

- A run id has one of two exact forms and is checked before it reaches a path or Docker; anything else is refused with a constant message and touches nothing.
- The state folder is private (folders 0700, files 0600, whatever the umask), is created by the first write only, and is never reached through a symbolic link.
- A record is replaced in one step, and a record that is missing, damaged or of the wrong shape makes its run `unreadable`, never an exception.
- The state of a run that has not ended is computed: the recorded one while its lock is held, `detached` or `lost` by whether a container carries its label, `unknown` when Docker cannot be asked. A run that has ended never asks Docker.
- A terminal run is a container that compose started with `run` in this deployment's folder (compared after resolving links) for a pipeline service and without a run label; nothing else is listed, and nothing in Docker's answer raises.
- Docker is asked with exactly the arguments written out below, every call with a timeout, and a container's environment is never read apart from the one variable the inspect template picks out.
- A log is read in pieces by byte offset that never end inside a character and that add up to the whole log.
- Cancel stops only a container that is listed for this deployment, with the interrupt signal and the full container id; a run that has ended is refused, and a run with a live supervisor only gets its cancel file.

Docker is replaced by the `fake_run_docker` fixture; everything else is the real file system under a temp folder.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import re
import signal
import stat
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

from social_data_pipeline import runstore

REPO_ROOT = Path(__file__).resolve().parents[1]

# The messages, written out: each is a constant that holds no input value.
ID_NOT_VALID = "the run id is not a valid run id"
NOT_FOUND = "no run with this id"
ENDED = "the run has already ended"
LOST = "the run is no longer running"
RECORD_NOT_READABLE = "the run's record cannot be read"
LOG_NOT_A_FILE = "the run's log is not a regular file"
NOT_A_FOLDER = "the run state folder is not a real folder (a symbolic link or a file is in its place)"
NOT_REACHABLE = "Docker cannot be reached"
NOT_USABLE = "the run state folder cannot be used"
NOT_A_REGULAR_FILE = "a file of the run state folder is not a regular file"
STOP_FAILED = "Docker could not stop the run's container"
OUTPUT_NOT_READABLE = "the run's output cannot be read"
RECORD_NOT_WRITTEN = "the run has no running container any more, but its record could not be written"

# No test of this file reaches the real Docker: the store's Docker function is replaced for every test, and a test that reaches the replacement fails.
pytestmark = pytest.mark.usefixtures("run_docker_guard")

# What the store must ask Docker, written out so that a change of a format is a change here too.
PS_FORMAT = '[{{json .ID}},{{json (.Label "com.docker.compose.project.working_dir")}},{{json (.Label "com.docker.compose.service")}},{{json (.Label "sdp.run")}}]'
INSPECT_TEMPLATE = '{{.State.StartedAt}} {{range .Config.Env}}{{$kv := split . "="}}{{if eq (index $kv 0) "SOURCE"}}{{.}}{{end}}{{end}}'
PS_ONEOFF = ["ps", "--no-trunc", "--filter", "label=com.docker.compose.oneoff=True", "--format", PS_FORMAT]
PS_LABELLED = ["ps", "--no-trunc", "--filter", "label=com.docker.compose.oneoff=True", "--filter", "label=sdp.run", "--format", PS_FORMAT]


def ps_for(run_id):
    """What the store asks Docker for the containers of one recorded run."""
    return ["ps", "--no-trunc", "--filter", "label=com.docker.compose.oneoff=True", "--filter", f"label=sdp.run={run_id}", "--format", PS_FORMAT]


# The compose service of every pipeline profile, as the CLI passes it.
SERVICES = {"parse": "parse", "lingua": "lingua", "ml-gpu": "ml", "postgres-ingest": "postgres_ingest", "postgres-ml": "postgres_ml", "mongo-ingest": "mongo_ingest", "sr-ingest": "sr_ingest", "sr-ml": "sr_ml"}

RUN_A = "r20261121T101500-0a1b2c"
RUN_B = "r20261121T101600-ffffff"
RUN_C = "r20261122T000000-000000"
CONTAINER_A = "a1" * 32
CONTAINER_B = "b2" * 32
LIVE_A = "live-" + "a1" * 6

# The separators and the next-line character are written as escapes: typed raw, an editor may turn them into spaces.
REFUSED_IDS = [
    "", ".", "..", "../" + RUN_A, RUN_A + "/..", RUN_A + "/", "/" + RUN_A, RUN_A + "\\x", RUN_A + "\n", LIVE_A + "\n", "\n" + RUN_A, RUN_A + "\x00", RUN_A + "\u2028",
    "r20261121T101500-0A1B2C", "live-" + "A1" * 6, "R20261121T101500-0a1b2c", "r20261121t101500-0a1b2c", "LIVE-" + "a1" * 6,
    "r2026112T101500-0a1b2c", "r202611211T101500-0a1b2c", "r20261121T10150-0a1b2c", "r20261121T101500-0a1b2", "r20261121T101500-0a1b2c0", "r20261121T101500_0a1b2c",
    "r\u0662\u0660261121T101500-0a1b2c", "live-" + "a1" * 5 + "a", "live-" + "a1" * 6 + "a", "live-" + "a1" * 5 + "ag", "live-", "live", " " + LIVE_A, LIVE_A + " ",
    RUN_A.encode(), [RUN_A], (RUN_A,), 20261121, 0, None, True,
]

MARKER = "made-up-secret-marker-7f3a"


# ── helpers ─────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _no_waiting_for_locks(monkeypatch):
    """Taking a lock that is held is tried a few times with a pause; the tests hold locks on purpose and need no pause."""
    monkeypatch.setattr(runstore, "_LOCK_RETRY_SECONDS", 0)


@pytest.fixture
def root(tmp_path):
    """An empty deployment folder."""
    folder = tmp_path.resolve() / "deployment"
    folder.mkdir()
    return folder


def make_run(root, run_id, state="starting", **changes):
    """Create a recorded run with a chosen id through the store's API, and move it to a state."""
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(runstore, "new_run_id", lambda: run_id)
        created = runstore.create_run(root, {"profile": "parse", "source": "reddit", "flags": {"build": False, "filter": "RC_2024-*"}, "started_by": "operator"})
    assert created == run_id
    if state != "starting" or changes:
        runstore.update_record(root, run_id, state=state, **changes)
    return run_id


def run_dir(root, run_id):
    return root / ".sdp" / "runs" / run_id


def write_log(root, run_id, content):
    """Write a run's log the way its supervisor does: through the descriptor the store opens."""
    fd = runstore.open_run_log(root, run_id)
    try:
        os.write(fd, content)
    finally:
        os.close(fd)


def read_whole_log(root, run_id, limit):
    """Read a run's log piece by piece from the start; returns (pieces, [(offset, next_offset)])."""
    pieces, spans, offset = [], [], 0
    while True:
        data = runstore.read_run_log(root, SERVICES, run_id, offset, limit)
        assert data["complete"] is True
        if data["next_offset"] == offset:
            assert data["text"] == ""
            return pieces, spans
        pieces.append(data["text"])
        spans.append((offset, data["next_offset"]))
        offset = data["next_offset"]


def mode(path):
    return stat.S_IMODE(os.lstat(path).st_mode)


def tree(folder):
    """Every path under a folder with its kind and, for a file, its content."""
    found = []
    for path in sorted(folder.rglob("*")):
        if path.is_symlink():
            found.append((str(path.relative_to(folder)), "link", os.readlink(path)))
        elif path.is_dir():
            found.append((str(path.relative_to(folder)), "dir", None))
        else:
            found.append((str(path.relative_to(folder)), "file", path.read_bytes()))
    return found


PATH_FUNCTIONS = [(os, "open"), (os, "mkdir"), (os, "stat"), (os, "lstat"), (os, "listdir"), (os, "scandir"), (os, "replace"), (os, "unlink"), (os, "fspath"), (os.path, "join"), (os.path, "realpath")]


@contextlib.contextmanager
def recorded_path_calls():
    """Record every call of the `os` functions through which the store builds or uses a path, and still make it. A stub that only raises could be swallowed by a handler, so the calls are kept in a list."""
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


@contextlib.contextmanager
def failing_instead_of_hanging(seconds=5):
    """Fail the test when the block takes longer than a few seconds: an open that waits on a named pipe would otherwise hold the whole run."""
    def too_long(signum, frame):
        raise AssertionError("the call did not return: it waits on the named pipe")

    previous = signal.signal(signal.SIGALRM, too_long)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


def outcome_of(call):
    """Call and return (result, exception): an assertion is then made outside any patch."""
    try:
        return call(), None
    except Exception as error:
        return None, error


# Every function of the store that takes a run id, as (name, call).
def id_taking_calls(root, run_id):
    return [
        ("run_id_kind", lambda: runstore.run_id_kind(run_id)),
        ("container_name", lambda: runstore.container_name(run_id)),
        ("run_label", lambda: runstore.run_label(run_id)),
        ("read_record", lambda: runstore.read_record(root, run_id)),
        ("update_record", lambda: runstore.update_record(root, run_id, state="finished")),
        ("open_run_log", lambda: runstore.open_run_log(root, run_id)),
        ("acquire_run_lock", lambda: runstore.acquire_run_lock(root, run_id)),
        ("cancel_requested", lambda: runstore.cancel_requested(root, run_id)),
        ("read_run", lambda: runstore.read_run(root, run_id)),
        ("read_run_log", lambda: runstore.read_run_log(root, SERVICES, run_id, 0, 100)),
        ("follow_run_log", lambda: runstore.follow_run_log(root, SERVICES, run_id, 10)),
        ("cancel_run", lambda: runstore.cancel_run(root, SERVICES, run_id)),
    ]


RECORDED_ONLY = ("container_name", "run_label", "read_record", "update_record", "open_run_log", "acquire_run_lock", "cancel_requested", "read_run")


# ── 1. run ids ──────────────────────────────────────────────────────────────


class TestRunIds:
    def test_the_two_forms_are_accepted(self):
        """Bug class: a pattern so strict that it refuses the ids the store itself hands out, or one that mixes the two kinds up (a terminal run has no folder, a recorded run is not looked up in the container listing)."""
        assert runstore.run_id_kind(RUN_A) == "web"
        assert runstore.run_id_kind("r00000000T000000-000000") == "web"
        assert runstore.run_id_kind(LIVE_A) == "terminal"
        assert runstore.run_id_kind("live-0123456789ab") == "terminal"
        assert runstore.container_name(RUN_A) == "sdp-run-" + RUN_A
        assert runstore.run_label(RUN_A) == "sdp.run=" + RUN_A

    def test_a_new_id_has_the_form_and_sorts_by_time(self, monkeypatch):
        """Bug class: an id the store's own check refuses, or one whose text order is not its time order (local time, a 12-hour clock, the random part first), which would list old runs as the newest."""
        class Clock:
            moments = iter([(2026, 11, 21, 23, 59, 59), (2026, 11, 22, 0, 0, 0), (2026, 12, 1, 9, 5, 3)])

            @classmethod
            def now(cls, zone):
                assert zone is timezone.utc
                return datetime(*next(cls.moments), tzinfo=zone)

        real = runstore.new_run_id()
        assert re.fullmatch(r"r[0-9]{8}T[0-9]{6}-[0-9a-f]{6}", real) and runstore.run_id_kind(real) == "web"

        monkeypatch.setattr(runstore, "datetime", Clock)
        ids = [runstore.new_run_id() for _ in range(3)]

        assert [run_id[:17] for run_id in ids] == ["r20261121T235959-", "r20261122T000000-", "r20261201T090503-"]
        assert ids == sorted(ids)

    @pytest.mark.parametrize("run_id", REFUSED_IDS, ids=repr)
    def test_a_malformed_id_is_refused_and_touches_nothing(self, root, monkeypatch, fake_run_docker, run_id):
        """Bug class: an id from a request joined to the state folder or handed to Docker before it is checked: `..` or a separator leaving the folder, a trailing newline (what `re.match` with `$` lets through), upper-case hex or a wrong length naming something the store never made, a value that is not text. Every function that takes an id must refuse it with the constant message before it builds a path or asks Docker; the calls are recorded, not only refused, because a handler could swallow a stub that raises."""
        make_run(root, RUN_A, "running")
        fake_run_docker.add(CONTAINER_A, root)
        before = tree(root)

        for name, call in id_taking_calls(root, run_id):
            with recorded_path_calls() as path_calls:
                result, error = outcome_of(call)

            assert isinstance(error, runstore.RunIdNotValid), (name, result, error)
            assert error.message == ID_NOT_VALID and str(error) == ID_NOT_VALID
            assert path_calls == [], name
        assert fake_run_docker.calls == []
        assert tree(root) == before

    def test_a_terminal_id_is_refused_where_a_run_folder_is_needed(self, root, fake_run_docker):
        """Bug class: the id of a terminal run, which is well-formed, joined to the runs folder by a function that works on a run's folder."""
        for name, call in id_taking_calls(root, LIVE_A):
            if name not in RECORDED_ONLY:
                continue
            with recorded_path_calls() as path_calls:
                _, error = outcome_of(call)

            assert isinstance(error, runstore.RunIdNotValid), name
            assert path_calls == [], name
        assert fake_run_docker.calls == []

    def test_the_recorders_see_a_well_formed_id(self, root, fake_run_docker):
        """Bug class: the two tests above passing because the recorders record nothing. A well-formed id that names no run must reach the file system, and a terminal id Docker."""
        with recorded_path_calls() as path_calls:
            _, error = outcome_of(lambda: runstore.read_record(root, RUN_A))
        assert isinstance(error, runstore.RunNotFound) and error.message == NOT_FOUND
        assert "open" in path_calls and "join" in path_calls

        _, error = outcome_of(lambda: runstore.cancel_run(root, SERVICES, LIVE_A))
        assert isinstance(error, runstore.RunNotFound)
        assert fake_run_docker.calls == [PS_ONEOFF]


# ── 2. the state folder ─────────────────────────────────────────────────────


class TestStateFolder:
    def test_folders_and_files_are_private_whatever_the_umask(self, root, monkeypatch, fake_run_docker):
        """Bug class: a record, a log or a lock that other users of the host can read or replace, because it was created with the default mode and the operator's umask happens to be permissive. Every folder must be 0700 and every file 0600 with the umask at zero."""
        previous = os.umask(0)
        try:
            run_id = make_run(root, RUN_A, "running")
            write_log(root, run_id, b"line\n")
            supervisor = runstore.acquire_run_lock(root, run_id)
            assert runstore.cancel_run(root, SERVICES, run_id) == "requested"
            runstore.release_run_lock(supervisor)
            with runstore.store_lock(root):
                pass
        finally:
            os.umask(previous)

        modes = {str(path.relative_to(root)): mode(path) for path in sorted(root.rglob("*"))}
        assert modes == {
            ".sdp": 0o700, ".sdp/lock": 0o600, ".sdp/runs": 0o700, f".sdp/runs/{RUN_A}": 0o700,
            f".sdp/runs/{RUN_A}/cancel": 0o600, f".sdp/runs/{RUN_A}/lock": 0o600, f".sdp/runs/{RUN_A}/log": 0o600, f".sdp/runs/{RUN_A}/record.json": 0o600,
        }

    @pytest.mark.parametrize("linked", [".sdp", "runs", "run"])
    def test_a_link_in_place_of_a_folder_is_refused(self, root, tmp_path, monkeypatch, fake_run_docker, linked):
        """Bug class: the store following a symbolic link planted where its state folder, its runs folder or a run's folder should be, and so reading, writing, locking or cancelling in a folder somewhere else on the host. Each of the three is a link to a real store here; every function must refuse with the constant message, the listing must not show the linked run as usable, and the folder behind the link must stay as it was."""
        other = tmp_path.resolve() / "elsewhere"
        other.mkdir()
        make_run(other, RUN_A, "running")
        fake_run_docker.add(CONTAINER_A, other, run=RUN_A)
        if linked == ".sdp":
            (root / ".sdp").symlink_to(other / ".sdp", target_is_directory=True)
        elif linked == "runs":
            (root / ".sdp").mkdir()
            (root / ".sdp" / "runs").symlink_to(other / ".sdp" / "runs", target_is_directory=True)
        else:
            (root / ".sdp" / "runs").mkdir(parents=True)
            run_dir(root, RUN_A).symlink_to(run_dir(other, RUN_A), target_is_directory=True)
        before = tree(other)

        for name, call in id_taking_calls(root, RUN_A):
            if name in ("run_id_kind", "container_name", "run_label", "follow_run_log"):
                continue
            _, error = outcome_of(call)
            assert type(error) is runstore.StoreError and error.message == NOT_A_FOLDER, (name, error)

        if linked == "run":
            listed = runstore.list_runs(root, SERVICES, 50)["runs"]
            assert [(run["id"], run["state"], run["profile"], run["source"]) for run in listed] == [(RUN_A, "unreadable", None, None)]
        else:
            for call in (lambda: runstore.list_runs(root, SERVICES, 50), lambda: runstore.create_run(root, {"profile": "parse"})):
                _, error = outcome_of(call)
                assert type(error) is runstore.StoreError and error.message == NOT_A_FOLDER
        if linked == ".sdp":
            _, error = outcome_of(lambda: runstore.store_lock(root).__enter__())
            assert type(error) is runstore.StoreError and error.message == NOT_A_FOLDER
        assert tree(other) == before
        assert fake_run_docker.named("stop") == []

    @pytest.mark.parametrize("name", ["record.json", "lock", "cancel", "log"])
    def test_a_link_in_place_of_a_file_is_never_followed(self, root, tmp_path, monkeypatch, fake_run_docker, name):
        """Bug class: a file of a run folder opened through a symbolic link, so that a record is read from, or a lock, a cancel request or a log line written to, a file of the operator's elsewhere. With a link in the file's place the run is unreadable or the call is refused, and the file behind the link keeps its content."""
        run_id = make_run(root, RUN_A, "running")
        target = tmp_path / "target"
        target.write_bytes((run_dir(root, run_id) / "record.json").read_bytes())
        if name == "record.json":
            (run_dir(root, run_id) / name).unlink()
        (run_dir(root, run_id) / name).symlink_to(target)
        fake_run_docker.add(CONTAINER_A, root, run=run_id)
        before = target.read_bytes()

        entry = runstore.read_run(root, run_id)
        if name == "log":
            # Reading a log through a link has its own test below; this run is detached, so its output is asked from Docker.
            _, error = outcome_of(lambda: runstore.open_run_log(root, run_id))
            assert type(error) is runstore.StoreError
        else:
            cancelled, cancel_error = outcome_of(lambda: runstore.cancel_run(root, SERVICES, run_id))
            assert cancelled is None
        if name == "record.json":
            assert runstore.read_record(root, run_id) is None and entry["state"] == "unreadable"
            assert type(cancel_error) is runstore.CancelRefused
        if name == "lock":
            assert entry["state"] == "unreadable" and type(cancel_error) is runstore.CancelRefused
            _, error = outcome_of(lambda: runstore.acquire_run_lock(root, run_id))
            assert type(error) is runstore.StoreError
        if name == "cancel":
            assert entry["state"] == "detached" and type(cancel_error) is runstore.StoreError
        assert target.read_bytes() == before
        assert fake_run_docker.named("stop") == []

    @pytest.mark.parametrize("reader", [False, True], ids=["nobody-reads", "someone-reads"])
    @pytest.mark.parametrize("name", ["cancel", "log", "lock"])
    def test_a_named_pipe_in_place_of_a_written_file_is_refused(self, root, fake_run_docker, name, reader):
        """Bug class: a cancel, a supervisor opening its log or a taker of the lock held for ever by a named pipe planted under the file's name (an open for writing waits until someone reads the pipe), or, when someone does read it, writing a run's output or a cancel request into that pipe. The open does not wait and anything but a regular file is refused; the test fails instead of hanging if the open waits."""
        run_id = make_run(root, RUN_A, "running")
        supervisor = runstore.acquire_run_lock(root, run_id) if name == "cancel" else None
        os.mkfifo(run_dir(root, run_id) / name)
        other_end = os.open(run_dir(root, run_id) / name, os.O_RDONLY | os.O_NONBLOCK) if reader else None
        call = {
            "cancel": lambda: runstore.cancel_run(root, SERVICES, run_id),
            "log": lambda: runstore.open_run_log(root, run_id),
            "lock": lambda: runstore.acquire_run_lock(root, run_id),
        }[name]

        with failing_instead_of_hanging():
            result, error = outcome_of(call)
            state = runstore.read_run(root, run_id)["state"]

        assert result is None and type(error) is runstore.StoreError, (result, error)
        # Without a reader an open for writing only is refused by the system itself; the lock is opened for reading too, which a pipe always allows.
        assert error.message == (NOT_A_REGULAR_FILE if reader or name == "lock" else NOT_USABLE)
        assert state == {"cancel": "running", "log": "lost", "lock": "unreadable"}[name]
        assert runstore.read_record(root, run_id)["state"] == "running"
        for fd in (other_end, supervisor):
            if fd is not None:
                os.close(fd)

    @pytest.mark.parametrize("foreign", [".sdp", "runs"])
    def test_a_state_folder_of_another_user_is_refused(self, root, fake_run_docker, foreign):
        """Bug class: the store working in a state folder that another user owns, which is what one run of a command with `sudo` leaves behind: that user can replace every record, log and lock in it, and the operator's own writes fail half way with a permission error. The owner of the opened state folder, and of its runs folder, is compared with the user the process acts as; with another owner every function refuses with the constant message and changes nothing. The owner is changed through the answer of `fstat` for that one folder, since a test cannot give a folder away."""
        run_id = make_run(root, RUN_A, "running")
        fake_run_docker.add(CONTAINER_A, root, run=run_id)
        folder = str(root / ".sdp") if foreign == ".sdp" else str(root / ".sdp" / "runs")
        before = tree(root)
        real_fstat = os.fstat

        def fstat(fd):
            info = real_fstat(fd)
            if os.readlink(f"/proc/self/fd/{fd}") != folder:
                return info
            fields = list(info)
            fields[4] = info.st_uid + 1
            return os.stat_result(fields)

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(os, "fstat", fstat)
            calls = [call for name, call in id_taking_calls(root, run_id) if name not in ("run_id_kind", "container_name", "run_label", "follow_run_log")]
            calls += [lambda: runstore.list_runs(root, SERVICES, 50), lambda: runstore.create_run(root, {"profile": "parse"})]
            if foreign == ".sdp":
                calls.append(lambda: runstore.store_lock(root).__enter__())
            outcomes = [outcome_of(call) for call in calls]

        assert len(outcomes) >= 10
        for result, error in outcomes:
            assert result is None and type(error) is runstore.StoreError and error.message == NOT_USABLE, (result, error)
        assert tree(root) == before and fake_run_docker.named("stop") == []
        assert runstore.read_run(root, run_id)["state"] == "detached"

    def test_reading_creates_nothing(self, root, monkeypatch, fake_run_docker):
        """Bug class: a status command that leaves a state folder behind in a deployment that never recorded a run, or a lock or log file in a run folder it only looked at. Listing, reading a record, a state or a log, asking for the cancel request and a refused cancel create nothing."""
        fake_run_docker.add(CONTAINER_A, root)
        assert [run["id"] for run in runstore.list_runs(root, SERVICES, 50)["runs"]] == [LIVE_A]
        for name, call in id_taking_calls(root, RUN_A):
            if name in ("run_id_kind", "container_name", "run_label", "follow_run_log"):
                continue
            _, error = outcome_of(call)
            assert type(error) is runstore.RunNotFound, name
        assert tree(root) == []

        (root / ".sdp").mkdir()
        assert runstore.list_runs(root, SERVICES, 50)["runs"][0]["id"] == LIVE_A
        assert tree(root) == [(".sdp", "dir", None)]

        run_id = make_run(root, RUN_A, "running")
        before = tree(root)
        assert runstore.read_run(root, run_id)["state"] == "lost"
        assert runstore.list_runs(root, SERVICES, 50)["runs"][0]["state"] == "lost"
        assert runstore.read_run_log(root, SERVICES, run_id, 0, 100)["text"] == ""
        assert runstore.cancel_requested(root, run_id) is False
        _, error = outcome_of(lambda: runstore.cancel_run(root, SERVICES, run_id))
        assert type(error) is runstore.CancelRefused
        assert tree(root) == before


# ── 3. records ──────────────────────────────────────────────────────────────


class TestRecords:
    def test_round_trip(self, root, monkeypatch):
        """Bug class: a record that does not come back as it was written (a field dropped or renamed, a time in another form), or an update that loses the fields it does not name."""
        monkeypatch.setattr(runstore, "_now", lambda: "2026-11-21T10:15:00Z")
        run_id = runstore.create_run(root, {"profile": "sr_ingest", "source": "reddit", "flags": {"build": True, "filter": "RC_2024-*", "classifier": None}, "started_by": "operator"})

        created = {"id": run_id, "profile": "sr_ingest", "source": "reddit", "flags": {"build": True, "filter": "RC_2024-*", "classifier": None}, "started_by": "operator", "state": "starting", "started": "2026-11-21T10:15:00Z", "ended": None, "exit_code": None}
        assert runstore.read_record(root, run_id) == created
        assert json.loads((run_dir(root, run_id) / "record.json").read_text()) == created

        assert runstore.update_record(root, run_id, state="running") == {**created, "state": "running"}
        finished = {**created, "state": "finished", "ended": "2026-11-21T10:20:00Z", "exit_code": 3}
        assert runstore.update_record(root, run_id, state="finished", ended="2026-11-21T10:20:00Z", exit_code=3) == finished
        assert runstore.read_record(root, run_id) == finished
        assert sorted(os.listdir(run_dir(root, run_id))) == ["record.json"]

        minimal = runstore.create_run(root, {"profile": "parse"})
        assert runstore.read_record(root, minimal) == {"id": minimal, "profile": "parse", "source": None, "flags": {}, "started_by": None, "state": "starting", "started": "2026-11-21T10:15:00Z", "ended": None, "exit_code": None}

    @pytest.mark.parametrize("changes", [{"id": RUN_B}, {"password": "x"}, {"state": "done"}, {"exit_code": "0"}, {"ended": "soon"}, {"flags": {"a": {"nested": 1}}}, {"flags": {"n": 2 ** 53 + 1}}, {"flags": {"n": -(2 ** 53) - 1}}, {"profile": None}])
    def test_a_change_a_record_cannot_hold_is_refused(self, root, monkeypatch, changes):
        """Bug class: the store writing a record it would itself call unreadable at the next read (a state it does not know, a field of the wrong type, a second id), which would turn a live run into an unreadable one."""
        run_id = make_run(root, RUN_A, "running")
        before = (run_dir(root, run_id) / "record.json").read_bytes()

        with pytest.raises(ValueError):
            runstore.update_record(root, run_id, **changes)
        for fields in ({}, {"profile": ""}, {"profile": 5}, {"profile": "parse", "flags": ["build"]}, {"profile": "parse", "source": 5}):
            with pytest.raises(ValueError):
                runstore.create_run(root, fields)

        assert (run_dir(root, run_id) / "record.json").read_bytes() == before
        assert os.listdir(root / ".sdp" / "runs") == [run_id]

    def test_a_record_too_large_to_read_back_is_never_written(self, root, tmp_path):
        """Bug class: the store writing a record it then cannot read: a record is read back up to 64 KiB, so a larger one, written, would turn a run into an unreadable one; and a run folder left behind by a start that was refused for its size. The size is checked before anything is written or created; the largest record that fits is written and read back. The run has ended here, so its record may fill the whole size (the test below is about one that has not)."""
        run_id = make_run(root, RUN_A, "finished", ended="2026-11-21T10:20:00Z", exit_code=-255)
        runstore.update_record(root, run_id, flags={"filter": ""})
        room = 65536 - (run_dir(root, run_id) / "record.json").stat().st_size
        before = tree(root)

        for changes in ({"flags": {"filter": "x" * (room + 1)}}, {"source": "s" * 70000}, {"started_by": "o" * 70000}, {"flags": {str(n): "x" * 100 for n in range(700)}}):
            with pytest.raises(ValueError):
                runstore.update_record(root, run_id, **changes)
        with pytest.raises(ValueError):
            runstore.create_run(root, {"profile": "parse", "flags": {"filter": "x" * 70000}})
        assert tree(root) == before
        fresh = tmp_path / "fresh"
        fresh.mkdir()
        with pytest.raises(ValueError):
            runstore.create_run(fresh, {"profile": "p" * 70000})
        assert tree(fresh) == []

        largest = runstore.update_record(root, run_id, flags={"filter": "x" * room})
        assert (run_dir(root, run_id) / "record.json").stat().st_size == 65536
        assert runstore.read_record(root, run_id) == largest and os.listdir(run_dir(root, run_id)) == ["record.json"]

    def test_a_run_whose_record_was_accepted_can_always_be_recorded_as_ended(self, root, fake_run_docker):
        """Bug class: a record of a run that has not ended filling the size a record may have, so that the write that ends the run (a longer state, an end time, an exit code) no longer fits: a cancel would stop the container and then fail to record it, and a supervisor could never write the end of its run. A record that has not ended is measured as it will be at its longest end. The largest one accepted, at creation and at an update, is ended and is then exactly as large as a record may be; one byte more is refused before anything is written."""
        end = {"state": "cancelled", "ended": "2026-11-21T10:20:00Z", "exit_code": -255}
        record = lambda run_id: run_dir(root, run_id) / "record.json"

        # Updated while running: "cancelled" is two bytes longer than "running", an end time 18 longer than null.
        running = make_run(root, RUN_A, "running")
        runstore.update_record(root, running, flags={"filter": ""})
        room = 65536 - record(running).stat().st_size - 20
        before = record(running).read_bytes()
        with pytest.raises(ValueError):
            runstore.update_record(root, running, flags={"filter": "x" * (room + 1)})
        assert record(running).read_bytes() == before
        runstore.update_record(root, running, flags={"filter": "x" * room})
        assert record(running).stat().st_size == 65516
        fake_run_docker.add(CONTAINER_A, root, run=running)
        assert runstore.cancel_run(root, SERVICES, running) == "stopped"
        assert record(running).stat().st_size == 65536 and runstore.read_record(root, running)["state"] == "cancelled"

        # Created: "cancelled" is one byte longer than "starting".
        probe = runstore.create_run(root, {"profile": "parse", "flags": {"filter": ""}})
        room = 65536 - record(probe).stat().st_size - 19
        runs = sorted(os.listdir(root / ".sdp" / "runs"))
        with pytest.raises(ValueError):
            runstore.create_run(root, {"profile": "parse", "flags": {"filter": "x" * (room + 1)}})
        assert sorted(os.listdir(root / ".sdp" / "runs")) == runs
        largest = runstore.create_run(root, {"profile": "parse", "flags": {"filter": "x" * room}})
        assert record(largest).stat().st_size == 65517
        assert runstore.update_record(root, largest, **end)["state"] == "cancelled"
        assert record(largest).stat().st_size == 65536 and runstore.read_record(root, largest)["exit_code"] == -255

    def test_a_record_with_no_room_for_its_end_gives_a_refusal_not_a_crash(self, root, fake_run_docker):
        """Bug class: a cancel that stops the container and then lets the failure to record it out as a bare `ValueError`, which a caller that handles the store's refusals does not expect. Only a record this store did not write can be that full (it is made by hand here: readable, not ended, six bytes under the size a record may have); the cancel then stops the container, leaves the record as it is, gives up the lock and raises a refusal with its own constant message."""
        run_id = make_run(root, RUN_A, "running")
        stored = json.loads((run_dir(root, run_id) / "record.json").read_text())
        stored["flags"] = {"filter": ""}
        stored["flags"]["filter"] = "x" * (65530 - len((json.dumps(stored, indent=2) + "\n").encode()))
        (run_dir(root, run_id) / "record.json").write_text(json.dumps(stored, indent=2) + "\n")
        assert (run_dir(root, run_id) / "record.json").stat().st_size == 65530
        fake_run_docker.add(CONTAINER_A, root, run=run_id)
        assert runstore.read_run(root, run_id)["state"] == "detached"

        with pytest.raises(runstore.StoreError) as exc:
            runstore.cancel_run(root, SERVICES, run_id)

        assert type(exc.value) is runstore.StoreError and exc.value.message == "the run has no running container any more, but its record could not be written"
        assert exc.value.__cause__ is None and fake_run_docker.named("stop") == [["stop", "--signal", "INT", "-t", "30", CONTAINER_A]]
        assert runstore.read_record(root, run_id) == stored and os.listdir(run_dir(root, run_id)).count("record.json") == 1
        assert not [name for name in os.listdir(run_dir(root, run_id)) if name.endswith(".tmp")]
        held = runstore.acquire_run_lock(root, run_id)
        assert isinstance(held, int)
        runstore.release_run_lock(held)

    def test_a_record_that_cannot_be_written_when_no_container_was_left_claims_no_stop(self, root, monkeypatch, fake_run_docker):
        """Bug class: a refusal that says a container was stopped when this call stopped nothing. The container can end by itself between the look at the run's state and the lookup under the lock; the record write that then fails is reported with a message that is true in both cases, and no stop was sent."""
        run_id = make_run(root, RUN_A, "running")
        fake_run_docker.add(CONTAINER_A, root, run=run_id)

        def container_ends(labels):
            if len(fake_run_docker.named("ps")) == 2:
                fake_run_docker.containers.clear()

        def read_only(*args, **kwargs):
            raise OSError(30, "Read-only file system")

        fake_run_docker.on_ps = container_ends
        monkeypatch.setattr(os, "replace", read_only)
        with pytest.raises(runstore.StoreError) as exc:
            runstore.cancel_run(root, SERVICES, run_id)

        assert type(exc.value) is runstore.StoreError and exc.value.message == "the run has no running container any more, but its record could not be written"
        assert fake_run_docker.named("stop") == [] and len(fake_run_docker.named("ps")) == 2
        assert runstore.read_record(root, run_id)["state"] == "running"

    def test_a_record_that_cannot_be_written_after_the_stop_is_said_so(self, root, monkeypatch, fake_run_docker):
        """Bug class: a cancel that stopped the container and then could not write the record (a full disk, a file system that is read-only) answering with the general "the run state folder cannot be used", which reads as "nothing was done" while the run's container is in fact stopped. The refusal after the stop is the one that says no container of the run is left; the record stays as it was and the lock is given up."""
        run_id = make_run(root, RUN_A, "running")
        fake_run_docker.add(CONTAINER_A, root, run=run_id)
        before = (run_dir(root, run_id) / "record.json").read_bytes()

        def read_only(*args, **kwargs):
            raise OSError(30, "Read-only file system")

        monkeypatch.setattr(os, "replace", read_only)
        with pytest.raises(runstore.StoreError) as exc:
            runstore.cancel_run(root, SERVICES, run_id)

        assert type(exc.value) is runstore.StoreError and exc.value.message == RECORD_NOT_WRITTEN
        assert fake_run_docker.named("stop") == [["stop", "--signal", "INT", "-t", "30", CONTAINER_A]] and fake_run_docker.containers == []
        assert (run_dir(root, run_id) / "record.json").read_bytes() == before
        assert sorted(os.listdir(run_dir(root, run_id))) == ["cancel", "lock", "record.json"]
        monkeypatch.undo()
        held = runstore.acquire_run_lock(root, run_id)
        assert isinstance(held, int)
        runstore.release_run_lock(held)

    def test_a_whole_number_in_the_flags_is_one_every_reader_gets_back(self, root):
        """Bug class: a flag holding a number beyond 2**53 written into a record: a reader that keeps JSON numbers as floats, a browser for one, shows another number than was sent. The two ends of the range are written and read back; one beyond is refused on write (the test above) and makes a record unreadable on read."""
        run_id = make_run(root, RUN_A, "running")

        runstore.update_record(root, run_id, flags={"most": 2 ** 53, "least": -(2 ** 53), "flag": True})

        assert runstore.read_record(root, run_id)["flags"] == {"most": 9007199254740992, "least": -9007199254740992, "flag": True}
        with pytest.raises(ValueError):
            runstore.create_run(root, {"profile": "parse", "flags": {"n": 2 ** 53 + 1}})
        assert os.listdir(root / ".sdp" / "runs") == [run_id]

    def test_the_record_is_replaced_in_one_step(self, root, monkeypatch):
        """Bug class: a record rewritten in place, so that a reader at the wrong moment gets an empty or half-written file and reports a live run as unreadable. The new content must be written to a temporary file in the run's own folder (a rename across folders is not one step) and take the record's name with `os.replace`; at that moment the record still reads as the old one."""
        run_id = make_run(root, RUN_A, "running")
        seen = []
        real_replace = os.replace

        def replace(source, destination, *, src_dir_fd=None, dst_dir_fd=None):
            seen.append({
                "source": source, "destination": destination, "same_folder": src_dir_fd is not None and src_dir_fd == dst_dir_fd,
                "folder": os.readlink(f"/proc/self/fd/{src_dir_fd}"), "names": sorted(os.listdir(run_dir(root, run_id))),
                "old_record": runstore.read_record(root, run_id)["state"], "temporary_mode": mode(run_dir(root, run_id) / source),
                "temporary": json.loads((run_dir(root, run_id) / source).read_text())["state"],
            })
            return real_replace(source, destination, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)

        monkeypatch.setattr(os, "replace", replace)
        runstore.update_record(root, run_id, state="finished", ended="2026-11-21T10:20:00Z", exit_code=0)

        assert len(seen) == 1
        step = seen[0]
        assert step["destination"] == "record.json" and step["same_folder"] and step["folder"] == str(run_dir(root, run_id))
        assert step["names"] == sorted(["record.json", step["source"]]) and "/" not in step["source"]
        assert (step["old_record"], step["temporary"], step["temporary_mode"]) == ("running", "finished", 0o600)
        assert sorted(os.listdir(run_dir(root, run_id))) == ["record.json"]
        assert runstore.read_record(root, run_id)["state"] == "finished"

    @pytest.mark.parametrize("failing", ["_write_all", "replace"])
    def test_a_write_that_fails_leaves_the_old_record(self, root, monkeypatch, failing):
        """Bug class: a write that dies half way (a full disk, a killed process) leaving a truncated record or a temporary file behind. The old record must still be there, whole, and nothing else in the folder."""
        run_id = make_run(root, RUN_A, "running")
        before = (run_dir(root, run_id) / "record.json").read_bytes()

        def half_then_fail(fd, payload):
            os.write(fd, payload[: len(payload) // 2])
            raise OSError(28, "No space left on device")

        def fail(*args, **kwargs):
            raise OSError(5, "Input/output error")

        if failing == "_write_all":
            monkeypatch.setattr(runstore, "_write_all", half_then_fail)
        else:
            monkeypatch.setattr(os, "replace", fail)
        with pytest.raises(runstore.StoreError) as exc:
            runstore.update_record(root, run_id, state="finished")

        assert exc.value.message == "the run state folder cannot be used"
        assert (run_dir(root, run_id) / "record.json").read_bytes() == before
        assert os.listdir(run_dir(root, run_id)) == ["record.json"]

    GOOD = {"id": RUN_A, "profile": "parse", "source": "reddit", "flags": {}, "started_by": None, "state": "running", "started": "2026-11-21T10:15:00Z", "ended": None, "exit_code": None}
    DAMAGED = {
        "empty": b"",
        "cut-off": b'{"id": "' + RUN_A.encode() + b'", "profile": "pa',
        "a-list": b"[]",
        "a-string": b'"running"',
        "null": b"null",
        "not-utf-8": b'{"id": "\xff\xfe"}',
        "deeply-nested": b"[" * 30000,
        "a-number-too-long": b'{"exit_code": ' + b"9" * 5000 + b"}",
        "larger-than-a-record": json.dumps({**GOOD, "flags": {"x": "y" * 70000}}).encode(),
        "no-id": json.dumps({key: value for key, value in GOOD.items() if key != "id"}).encode(),
        "another-runs-id": json.dumps({**GOOD, "id": RUN_B}).encode(),
        "id-not-text": json.dumps({**GOOD, "id": 5}).encode(),
        "no-profile": json.dumps({key: value for key, value in GOOD.items() if key != "profile"}).encode(),
        "profile-a-number": json.dumps({**GOOD, "profile": 5}).encode(),
        "profile-empty": json.dumps({**GOOD, "profile": ""}).encode(),
        "source-a-list": json.dumps({**GOOD, "source": ["reddit"]}).encode(),
        "flags-missing": json.dumps({key: value for key, value in GOOD.items() if key != "flags"}).encode(),
        "flags-a-list": json.dumps({**GOOD, "flags": ["build"]}).encode(),
        "flags-nested": json.dumps({**GOOD, "flags": {"a": {"b": 1}}}).encode(),
        "flags-a-fraction": json.dumps({**GOOD, "flags": {"a": 1.5}}).encode(),
        "flags-a-number-too-large": json.dumps({**GOOD, "flags": {"a": 2 ** 53 + 1}}).encode(),
        "flags-a-number-too-small": json.dumps({**GOOD, "flags": {"a": -(2 ** 53) - 1}}).encode(),
        "started-by-a-number": json.dumps({**GOOD, "started_by": 7}).encode(),
        "state-unknown": json.dumps({**GOOD, "state": "done"}).encode(),
        "state-computed": json.dumps({**GOOD, "state": "detached"}).encode(),
        "state-a-list": json.dumps({**GOOD, "state": ["running"]}).encode(),
        "state-missing": json.dumps({key: value for key, value in GOOD.items() if key != "state"}).encode(),
        "started-missing": json.dumps({**GOOD, "started": None}).encode(),
        "started-not-a-time": json.dumps({**GOOD, "started": "yesterday"}).encode(),
        "started-with-a-newline": json.dumps({**GOOD, "started": "2026-11-21T10:15:00Z\n"}).encode(),
        "started-local-time": json.dumps({**GOOD, "started": "2026-11-21T10:15:00+01:00"}).encode(),
        "ended-a-number": json.dumps({**GOOD, "ended": 1763720100}).encode(),
        "exit-code-text": json.dumps({**GOOD, "exit_code": "0"}).encode(),
        "exit-code-a-boolean": json.dumps({**GOOD, "exit_code": True}).encode(),
        "exit-code-a-fraction": json.dumps({**GOOD, "exit_code": 1.0}).encode(),
        "exit-code-nan": b'{"id": "' + RUN_A.encode() + b'", "profile": "parse", "source": null, "flags": {}, "started_by": null, "state": "finished", "started": "2026-11-21T10:15:00Z", "ended": null, "exit_code": NaN}',
        "exit-code-huge": json.dumps({**GOOD, "exit_code": 10 ** 40}).encode(),
    }

    @pytest.mark.parametrize("content", list(DAMAGED.values()), ids=list(DAMAGED))
    def test_a_damaged_record_is_unreadable_and_never_an_exception(self, root, monkeypatch, fake_run_docker, content):
        """Bug class: one damaged record breaking the listing of every run, or a value of the wrong type from a record reaching the data (a list where the state is compared, a number where a time is printed, the id of another run, which every path would then be built from). A record that is not exactly the nine typed fields makes its run `unreadable`, with no other field carried, and nothing raises; the control proves the undamaged record of the same shape is read."""
        make_run(root, RUN_A, "running")
        make_run(root, RUN_B, "finished", ended="2026-11-21T10:20:00Z", exit_code=0)
        record = run_dir(root, RUN_A) / "record.json"
        record.write_bytes(json.dumps(self.GOOD).encode())
        fake_run_docker.add(CONTAINER_A, root, run=RUN_A)
        assert runstore.read_record(root, RUN_A) == self.GOOD and runstore.read_run(root, RUN_A)["state"] == "detached"

        record.write_bytes(content)

        unreadable = {"id": RUN_A, "kind": "web", "profile": None, "source": None, "flags": None, "started_by": None, "state": "unreadable", "started": None, "ended": None, "exit_code": None}
        assert runstore.read_record(root, RUN_A) is None
        assert runstore.read_run(root, RUN_A) == unreadable
        listed = runstore.list_runs(root, SERVICES, 50)
        assert listed["runs"][1] == unreadable and listed["runs"][0]["state"] == "finished"
        assert json.loads(json.dumps(listed, allow_nan=False)) == listed
        assert runstore.read_run_log(root, SERVICES, RUN_A, 0, 100)["state"] == "unreadable"
        with pytest.raises(runstore.CancelRefused) as exc:
            runstore.cancel_run(root, SERVICES, RUN_A)
        assert exc.value.message == RECORD_NOT_READABLE
        with pytest.raises(runstore.StoreError) as exc:
            runstore.update_record(root, RUN_A, state="finished")
        assert exc.value.message == RECORD_NOT_READABLE
        assert record.read_bytes() == content and fake_run_docker.named("stop") == []

    @pytest.mark.parametrize("in_its_place", ["nothing", "folder", "pipe"])
    def test_a_record_that_is_no_file_is_unreadable(self, root, monkeypatch, fake_run_docker, in_its_place):
        """Bug class: a run folder without a record (a start that died between the two steps), or with a folder or a named pipe under the record's name, raising or, for the pipe, holding the listing for ever on an open that waits for a writer."""
        make_run(root, RUN_A, "running")
        record = run_dir(root, RUN_A) / "record.json"
        record.unlink()
        if in_its_place == "folder":
            record.mkdir()
        elif in_its_place == "pipe":
            os.mkfifo(record)

        with failing_instead_of_hanging():
            assert runstore.read_record(root, RUN_A) is None
            assert [run["state"] for run in runstore.list_runs(root, SERVICES, 50)["runs"]] == ["unreadable"]

    def test_extra_keys_are_dropped(self, root, monkeypatch, fake_run_docker):
        """Bug class: whatever a record file holds beyond its nine fields being handed on with the run's data."""
        make_run(root, RUN_A, "finished")
        record = run_dir(root, RUN_A) / "record.json"
        record.write_text(json.dumps({**json.loads(record.read_text()), "admin_password": MARKER, "env": {"POSTGRES_PASSWORD": MARKER}}))

        assert set(runstore.read_record(root, RUN_A)) == set(runstore.RECORD_FIELDS)
        assert MARKER not in json.dumps([runstore.read_record(root, RUN_A), runstore.read_run(root, RUN_A), runstore.list_runs(root, SERVICES, 50)])


# ── 4. computed states ──────────────────────────────────────────────────────


class TestStates:
    @pytest.mark.parametrize("recorded", ["starting", "running"])
    @pytest.mark.parametrize("lock_held, container, expected", [
        (True, True, None),
        (True, False, None),
        (False, True, "detached"),
        (False, False, "lost"),
    ])
    def test_the_state_of_a_run_that_has_not_ended(self, root, monkeypatch, fake_run_docker, recorded, lock_held, container, expected):
        """Bug class: a run shown as running for ever after its supervisor died (the record is all that says so), or a healthy run shown as lost or detached. While the run's lock is held the recorded state stands and Docker is not asked; with the lock free the run is `detached` when a container carries its label and `lost` when none does. The single read and the listing must agree."""
        run_id = make_run(root, RUN_A, recorded)
        if container:
            fake_run_docker.add(CONTAINER_A, root, run=run_id)
        fake_run_docker.add(CONTAINER_B, root, run=RUN_B)
        supervisor = runstore.acquire_run_lock(root, run_id) if lock_held else None
        assert not lock_held or supervisor is not None

        single = runstore.read_run(root, run_id)
        asked_for_one = list(fake_run_docker.calls)
        listed = runstore.list_runs(root, SERVICES, 50)["runs"]

        assert single["state"] == (expected or recorded)
        assert listed == [single]
        if lock_held:
            assert asked_for_one == [] and fake_run_docker.calls == [PS_ONEOFF]
            runstore.release_run_lock(supervisor)
        else:
            assert asked_for_one == [ps_for(run_id)]
            assert fake_run_docker.calls == asked_for_one + [PS_ONEOFF, PS_LABELLED]

    @pytest.mark.parametrize("final", ["finished", "cancelled"])
    def test_a_run_that_has_ended_never_asks_docker(self, root, monkeypatch, fake_run_docker, final):
        """Bug class: every look at an old run costing a Docker call, or, worse, an ended run whose label a container still carries being shown as detached and offered for cancelling. For a final state the record is the answer: Docker is not asked about the run, by the read, the listing, the log or the cancel. The run's lock is free and a container carries its label here, which for a run that has not ended would mean `detached`."""
        run_id = make_run(root, RUN_A, final, ended="2026-11-21T10:20:00Z", exit_code=0)
        fake_run_docker.add(CONTAINER_A, root, run=run_id)

        assert runstore.read_run(root, run_id)["state"] == final
        assert runstore.read_run_log(root, SERVICES, run_id, 0, 100)["state"] == final
        with pytest.raises(runstore.CancelRefused):
            runstore.cancel_run(root, SERVICES, run_id)
        assert fake_run_docker.calls == []
        assert runstore.list_runs(root, SERVICES, 50)["runs"][0]["state"] == final
        assert fake_run_docker.calls == [PS_ONEOFF]

    def test_docker_out_of_reach_gives_unknown_and_never_raises(self, root, monkeypatch, fake_run_docker):
        """Bug class: `runs list` failing, or calling a run lost, when the Docker daemon is down or slow. The listing must come back with `docker_available` false, the runs that ended as recorded, a run with a live supervisor as recorded (its lock says so without Docker), and a run with neither as `unknown`; no terminal run is listed."""
        make_run(root, RUN_A, "finished", ended="2026-11-21T10:20:00Z", exit_code=0)
        make_run(root, RUN_B, "running")
        make_run(root, RUN_C, "running")
        supervisor = runstore.acquire_run_lock(root, RUN_B)
        fake_run_docker.add(CONTAINER_A, root)
        fake_run_docker.add(CONTAINER_B, root, run=RUN_C)

        reachable = runstore.list_runs(root, SERVICES, 50)
        assert reachable["docker_available"] is True
        assert [(run["id"], run["state"]) for run in reachable["runs"]] == [(RUN_C, "detached"), (RUN_B, "running"), (RUN_A, "finished"), (LIVE_A, "running")]

        fake_run_docker.reachable = False
        fake_run_docker.calls.clear()
        listed = runstore.list_runs(root, SERVICES, 50)

        assert listed["docker_available"] is False
        assert [(run["id"], run["state"]) for run in listed["runs"]] == [(RUN_C, "unknown"), (RUN_B, "running"), (RUN_A, "finished")]
        # One failed call is the answer for the whole listing: a daemon that is down is not asked again for every run.
        assert fake_run_docker.calls == [PS_ONEOFF]
        assert runstore.read_run(root, RUN_C)["state"] == "unknown"
        assert runstore.read_run_log(root, SERVICES, RUN_C, 0, 100)["state"] == "unknown"
        runstore.release_run_lock(supervisor)

    def test_docker_failing_after_the_container_listing(self, root, monkeypatch, fake_run_docker):
        """Bug class: Docker answering the first call and failing a later one (a timeout on one container) raising out of the listing."""
        make_run(root, RUN_A, "running")
        fake_run_docker.add(CONTAINER_A, root)
        real_call = fake_run_docker.__call__

        def fail_at_inspect(args, **kwargs):
            if args[0] == "inspect":
                fake_run_docker.calls.append(list(args))
                raise runstore.DockerUnavailable(NOT_REACHABLE)
            return real_call(args, **kwargs)

        monkeypatch.setattr(runstore, "_docker", fail_at_inspect)
        listed = runstore.list_runs(root, SERVICES, 50)

        assert listed == {"docker_available": False, "runs": [{**runstore.read_run(root, RUN_A), "state": "unknown"}]}

    def test_docker_failing_between_two_containers_lists_no_terminal_run(self, root, fake_run_docker):
        """Bug class: a listing that says "Docker cannot be reached" and still shows one of two terminal runs, because Docker answered for the first container and not for the second. A reader takes the runs shown for all there are; with Docker out of reach no terminal run is listed."""
        fake_run_docker.add(CONTAINER_A, root)
        fake_run_docker.add(CONTAINER_B, root)
        assert [run["id"] for run in runstore.list_runs(root, SERVICES, 50)["runs"]] == [LIVE_A, "live-" + "b2" * 6]
        fake_run_docker.calls.clear()
        fake_run_docker.unreachable_from = 2

        listed = runstore.list_runs(root, SERVICES, 50)

        assert listed == {"docker_available": False, "runs": []}
        assert fake_run_docker.calls == [PS_ONEOFF, ["inspect", "--format", INSPECT_TEMPLATE, CONTAINER_A], ["inspect", "--format", INSPECT_TEMPLATE, CONTAINER_B]]

    def test_docker_failing_at_the_lookup_of_recorded_runs_lists_no_terminal_run(self, root, fake_run_docker):
        """Bug class: terminal runs listed next to `docker_available: false` when Docker answered for them and then failed the later lookup of the recorded runs' containers: `runs list` would print their rows above the note that says terminal runs are not listed, and a reader of the data could not tell whether the rows are all there are. Out of reach at any call of the listing means no terminal run in it."""
        make_run(root, RUN_A, "running")
        fake_run_docker.add(CONTAINER_A, root)
        assert [(run["id"], run["state"]) for run in runstore.list_runs(root, SERVICES, 50)["runs"]] == [(RUN_A, "lost"), (LIVE_A, "running")]
        fake_run_docker.calls.clear()
        fake_run_docker.unreachable_from = 2

        listed = runstore.list_runs(root, SERVICES, 50)

        assert listed["docker_available"] is False
        assert [(run["id"], run["state"]) for run in listed["runs"]] == [(RUN_A, "unknown")]
        assert fake_run_docker.calls == [PS_ONEOFF, ["inspect", "--format", INSPECT_TEMPLATE, CONTAINER_A], PS_LABELLED]

    def test_a_labelled_container_must_be_one_compose_started_with_run(self, root, fake_run_docker):
        """Bug class: any container that carries a run's label taken for the run's container, and so stopped by a cancel: a service container someone labelled by hand, or one started in another way. Every run is started with `compose run`, so the lookup asks for that label as well, as the listing of terminal runs does; a container without it does not make a run detached."""
        run_id = make_run(root, RUN_A, "running")
        fake_run_docker.add(CONTAINER_A, root, run=run_id, oneoff=False)

        assert runstore.read_run(root, run_id)["state"] == "lost"
        assert [run["state"] for run in runstore.list_runs(root, SERVICES, 50)["runs"]] == ["lost"]
        with pytest.raises(runstore.CancelRefused) as exc:
            runstore.cancel_run(root, SERVICES, run_id)
        assert exc.value.message == LOST and fake_run_docker.named("stop") == []
        assert fake_run_docker.calls == [ps_for(run_id), PS_ONEOFF, PS_LABELLED, ps_for(run_id)]

    def test_one_listing_serves_every_recorded_run(self, root, monkeypatch, fake_run_docker):
        """Bug class: one Docker call per recorded run, which makes the listing slower with every run that was ever lost."""
        for run_id in (RUN_A, RUN_B, RUN_C):
            make_run(root, run_id, "running")
        fake_run_docker.add(CONTAINER_A, root, run=RUN_B)

        listed = runstore.list_runs(root, SERVICES, 50)["runs"]

        assert [(run["id"], run["state"]) for run in listed] == [(RUN_C, "lost"), (RUN_B, "detached"), (RUN_A, "lost")]
        assert fake_run_docker.calls == [PS_ONEOFF, PS_LABELLED]

    def test_recorded_runs_are_newest_first_and_limited(self, root, monkeypatch, fake_run_docker):
        """Bug class: the limit cutting off the newest runs instead of the oldest, the terminal runs counted against it, or a name in the runs folder that is no run id listed as a run."""
        for run_id in (RUN_B, RUN_A, RUN_C):
            make_run(root, run_id, "finished")
        (root / ".sdp" / "runs" / "notes.txt").write_text("x")
        (root / ".sdp" / "runs" / (RUN_A + ".bak")).mkdir()
        fake_run_docker.add(CONTAINER_B, root, started="2026-11-21T10:31:00Z")
        fake_run_docker.add(CONTAINER_A, root, started="2026-11-21T10:30:00Z")

        assert [run["id"] for run in runstore.list_runs(root, SERVICES, 50)["runs"]] == [RUN_C, RUN_B, RUN_A, LIVE_A, "live-" + "b2" * 6]
        assert [run["id"] for run in runstore.list_runs(root, SERVICES, 2)["runs"]] == [RUN_C, RUN_B, LIVE_A, "live-" + "b2" * 6]
        assert [run["kind"] for run in runstore.list_runs(root, SERVICES, 1)["runs"]] == ["web", "terminal", "terminal"]


# ── locks ───────────────────────────────────────────────────────────────────


class TestLocks:
    def test_the_run_lock_has_one_holder(self, root, monkeypatch, fake_run_docker):
        """Bug class: two supervisors for one run, or a supervisor locked out by someone who only read the run's state. The lock is taken by one holder at a time, a reader does not keep it, and closing the descriptor gives it up."""
        run_id = make_run(root, RUN_A, "running")

        first = runstore.acquire_run_lock(root, run_id)
        assert isinstance(first, int)
        assert runstore.acquire_run_lock(root, run_id) is None
        assert runstore.read_run(root, run_id)["state"] == "running"
        assert runstore.acquire_run_lock(root, run_id) is None
        runstore.release_run_lock(first)

        assert runstore.read_run(root, run_id)["state"] == "lost"
        second = runstore.acquire_run_lock(root, run_id)
        assert isinstance(second, int)
        runstore.release_run_lock(second)

    def test_a_reader_does_not_look_like_a_supervisor(self, root, monkeypatch, fake_run_docker):
        """Bug class: two status commands at the same moment each seeing the other's look at the lock as a live supervisor, and reporting a lost run as running. A reader takes the lock shared, so a second reader still finds it free."""
        run_id = make_run(root, RUN_A, "running")
        (run_dir(root, run_id) / "lock").touch()
        reader = os.open(run_dir(root, run_id) / "lock", os.O_RDONLY)
        fcntl.flock(reader, fcntl.LOCK_SH)

        assert runstore.read_run(root, run_id)["state"] == "lost"
        os.close(reader)

    def test_taking_the_lock_outlasts_a_reader(self, root, monkeypatch):
        """Bug class: a supervisor, or a cancel, that finds the lock taken for the instant a status command looks at it and concludes that another supervisor is alive. Taking the lock is tried again a few times before the answer is "held"."""
        run_id = make_run(root, RUN_A, "running")
        (run_dir(root, run_id) / "lock").touch()
        reader = os.open(run_dir(root, run_id) / "lock", os.O_RDONLY)
        fcntl.flock(reader, fcntl.LOCK_SH)
        pauses = []

        def pause(seconds):
            pauses.append(seconds)
            if len(pauses) == 2:
                os.close(reader)

        monkeypatch.setattr(runstore, "_LOCK_RETRY_SECONDS", 0.05)
        monkeypatch.setattr(runstore.time, "sleep", pause)
        taken = runstore.acquire_run_lock(root, run_id)

        assert isinstance(taken, int) and pauses == [0.05, 0.05]
        runstore.release_run_lock(taken)

    @pytest.mark.parametrize("failure", ["interrupt", "error"])
    def test_a_failed_try_for_the_lock_leaves_no_descriptor_open(self, root, monkeypatch, failure):
        """Bug class: a descriptor of the lock file left open when taking the lock is interrupted (Ctrl-C during the pause between two tries) or fails: a long-lived process that asks often runs out of descriptors, and a descriptor that did get the lock would hold it for as long as that process lives."""
        run_id = make_run(root, RUN_A, "running")
        holder = runstore.acquire_run_lock(root, run_id)

        def interrupted(seconds):
            raise KeyboardInterrupt

        def broken(fd, operation):
            raise OSError(5, "Input/output error")

        if failure == "interrupt":
            monkeypatch.setattr(runstore.time, "sleep", interrupted)
        else:
            monkeypatch.setattr(fcntl, "flock", broken)
        before = sorted(os.listdir("/proc/self/fd"))
        with pytest.raises(KeyboardInterrupt if failure == "interrupt" else runstore.StoreError):
            runstore.acquire_run_lock(root, run_id)

        assert sorted(os.listdir("/proc/self/fd")) == before
        monkeypatch.undo()
        runstore.release_run_lock(holder)

    def test_the_store_lock_excludes_a_second_holder(self, root):
        """Bug class: a store-wide lock that locks nothing (another file each time, a lock given up before the block ends), or one that turns the caller's own file error into a refusal of the store."""
        with runstore.store_lock(root):
            other = os.open(root / ".sdp" / "lock", os.O_RDWR)
            with pytest.raises(BlockingIOError):
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.close(other)

        with pytest.raises(FileNotFoundError), runstore.store_lock(root):
            open(root / "no-such-file")
        with runstore.store_lock(root):
            pass

    def test_cancel_requested(self, root, monkeypatch, fake_run_docker):
        """Bug class: the supervisor never learning that a cancel was asked (the request written under another name or in another run's folder)."""
        make_run(root, RUN_A, "running")
        make_run(root, RUN_B, "running")
        supervisor = runstore.acquire_run_lock(root, RUN_A)
        assert runstore.cancel_requested(root, RUN_A) is False

        assert runstore.cancel_run(root, SERVICES, RUN_A) == "requested"

        assert runstore.cancel_requested(root, RUN_A) is True and runstore.cancel_requested(root, RUN_B) is False
        runstore.release_run_lock(supervisor)


# ── 5. terminal runs ────────────────────────────────────────────────────────


class TestTerminalRuns:
    def test_a_run_started_in_a_terminal_is_listed(self, root, fake_run_docker):
        """Bug class: a terminal run missing from the listing or listed with the compose service in place of the profile (`ml-gpu` for `ml`), a start time in Docker's nanosecond form, or the text `SOURCE=` left on the source."""
        fake_run_docker.add(CONTAINER_A, root, service="ml-gpu", started="2026-11-21T10:30:00.987654321Z", source="reddit")
        fake_run_docker.add(CONTAINER_B, root, service="parse", started="2026-11-21T10:29:59Z", source="a=b c")

        listed = runstore.list_runs(root, SERVICES, 50)

        assert listed == {"docker_available": True, "runs": [
            {"id": "live-" + "b2" * 6, "kind": "terminal", "profile": "parse", "source": "a=b c", "flags": None, "started_by": None, "state": "running", "started": "2026-11-21T10:29:59Z", "ended": None, "exit_code": None},
            {"id": LIVE_A, "kind": "terminal", "profile": "ml", "source": "reddit", "flags": None, "started_by": None, "state": "running", "started": "2026-11-21T10:30:00Z", "ended": None, "exit_code": None},
        ]}
        assert fake_run_docker.calls == [PS_ONEOFF, ["inspect", "--format", INSPECT_TEMPLATE, CONTAINER_A], ["inspect", "--format", INSPECT_TEMPLATE, CONTAINER_B]]

    def test_only_this_deployments_pipeline_containers_are_runs(self, root, tmp_path, monkeypatch, fake_run_docker):
        """Bug class: a container that is no terminal run of this deployment listed as one, and so offered for cancelling: a run of another checkout on the same host, a hand-run command in a database service, a container that carries a run label (it belongs to a recorded run) or one compose did not start with `run`. None is listed, none is inspected, and none raises."""
        other = tmp_path.resolve() / "other-checkout"
        other.mkdir()
        make_run(root, RUN_A, "running")
        fake_run_docker.add("c0" * 32, other, service="parse")
        fake_run_docker.add("c1" * 32, root, service="postgres")
        fake_run_docker.add("c2" * 32, root, service="jobs")
        fake_run_docker.add("c3" * 32, root, service="parse", run=RUN_A)
        fake_run_docker.add("c4" * 32, root, service="parse", oneoff=False)
        fake_run_docker.add("c5" * 32, "", service="parse")
        fake_run_docker.add("c6" * 32, str(root) + "/config", service="parse")
        fake_run_docker.add("c7" * 32, root, service="")
        fake_run_docker.add("c8" * 32, root, service="PARSE")

        listed = runstore.list_runs(root, SERVICES, 50)["runs"]

        assert [(run["id"], run["kind"], run["state"]) for run in listed] == [(RUN_A, "web", "detached")]
        assert fake_run_docker.named("inspect") == []
        for container in ("c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"):
            with pytest.raises(runstore.RunNotFound):
                runstore.cancel_run(root, SERVICES, "live-" + container * 6)
        assert fake_run_docker.named("stop") == []

    @pytest.mark.parametrize("folder", [".", "./", "deployment", "../deployment"])
    def test_a_folder_label_that_is_not_an_absolute_path_is_not_this_deployment(self, root, monkeypatch, fake_run_docker, folder):
        """Bug class: a container whose folder label is a relative path counted as this deployment's whenever the command happens to run from a place where that path leads to the deployment folder (compose writes an absolute path; a label written by hand need not be). A relative value is not resolved at all: the container is no terminal run and not a recorded run's container."""
        monkeypatch.chdir(root if folder.startswith(".") and "deployment" not in folder else root.parent if folder == "deployment" else root)
        assert os.path.realpath(folder) == str(root)
        make_run(root, RUN_A, "running")
        fake_run_docker.add(CONTAINER_A, folder)
        fake_run_docker.add(CONTAINER_B, folder, run=RUN_A)

        listed = runstore.list_runs(root, SERVICES, 50)["runs"]

        assert [(run["id"], run["state"]) for run in listed] == [(RUN_A, "lost")]
        assert fake_run_docker.named("inspect") == []
        with pytest.raises(runstore.RunNotFound):
            runstore.cancel_run(root, SERVICES, LIVE_A)
        assert fake_run_docker.named("stop") == []

    @pytest.mark.parametrize("linked_side", ["root", "label"])
    def test_a_linked_deployment_folder_is_the_same_folder(self, root, tmp_path, fake_run_docker, linked_side):
        """Bug class: a deployment reached through a symbolic link listing no terminal run, because the folder compose recorded and the folder the CLI resolved are compared as text. Both sides are resolved first, whichever of the two holds the link."""
        link = tmp_path / "linked-deployment"
        link.symlink_to(root, target_is_directory=True)
        fake_run_docker.add(CONTAINER_A, root if linked_side == "root" else link)

        listed = runstore.list_runs(link if linked_side == "root" else root, SERVICES, 50)["runs"]

        assert [run["id"] for run in listed] == [LIVE_A]

    def test_a_daemon_that_stops_after_the_listing_is_not_read_as_runs_that_ended(self, root, fake_run_docker):
        """Bug class: every terminal run dropped from the listing, with `docker_available` still true, because the daemon stopped between the listing and the look at the first container: the client then fails each inspect the same way it fails one for a container that is gone. When an inspect fails the listing is asked once more, and a daemon that is down is found out of reach."""
        fake_run_docker.add(CONTAINER_A, root)
        fake_run_docker.add(CONTAINER_B, root)
        fake_run_docker.daemon_down_from = 1

        listed = runstore.list_runs(root, SERVICES, 50)

        assert listed == {"docker_available": False, "runs": []}
        assert fake_run_docker.calls == [PS_ONEOFF, ["inspect", "--format", INSPECT_TEMPLATE, CONTAINER_A], ["inspect", "--format", INSPECT_TEMPLATE, CONTAINER_B], PS_ONEOFF]

    def test_a_container_that_really_ended_is_left_out_and_the_others_are_listed(self, root, fake_run_docker):
        """Bug class: the opposite slip of the test above: one run that ended between the listing and the look at it making the whole listing say "Docker cannot be reached", or being listed as if it still ran. The second listing no longer holds it; the other run is listed and Docker counts as reached."""
        fake_run_docker.add(CONTAINER_A, root)
        fake_run_docker.add(CONTAINER_B, root)
        fake_run_docker.ends_at_inspect = {CONTAINER_A}

        listed = runstore.list_runs(root, SERVICES, 50)

        assert (listed["docker_available"], [run["id"] for run in listed["runs"]]) == (True, ["live-" + "b2" * 6])
        assert fake_run_docker.calls == [PS_ONEOFF, ["inspect", "--format", INSPECT_TEMPLATE, CONTAINER_A], ["inspect", "--format", INSPECT_TEMPLATE, CONTAINER_B], PS_ONEOFF]

    def test_a_container_that_is_listed_and_cannot_be_inspected_is_still_a_run(self, root, fake_run_docker):
        """Bug class: a run that is alive, and that Docker lists before and after, missing from the listing (and so out of reach of `runs cancel` for anyone who looks the id up there) because Docker would not describe its container. It is listed without a start time and a source."""
        fake_run_docker.add(CONTAINER_A, root)
        fake_run_docker.inspect_fails = True

        listed = runstore.list_runs(root, SERVICES, 50)

        assert listed == {"docker_available": True, "runs": [{"id": LIVE_A, "kind": "terminal", "profile": "parse", "source": None, "flags": None, "started_by": None, "state": "running", "started": None, "ended": None, "exit_code": None}]}
        assert runstore.cancel_run(root, SERVICES, LIVE_A) == "stopped"

    @pytest.mark.parametrize("source, expected", [("", None), (None, None), ("reddit", "reddit"), ("a\x1b[31mb\u202e", "a\x1b[31mb\u202e")])
    def test_the_source_of_a_terminal_run(self, root, fake_run_docker, source, expected):
        """Bug class: a container without a source shown with an empty text or the word `SOURCE=` as its source. A variable that is empty or not set is None; any other value comes back as it is (the CLI makes it display-safe)."""
        fake_run_docker.add(CONTAINER_A, root, source=source)

        assert [run["source"] for run in runstore.list_runs(root, SERVICES, 50)["runs"]] == [expected]

    def test_lines_that_are_not_what_the_format_prints_are_passed_over(self, root, tmp_path, fake_run_docker):
        """Bug class: a line of Docker's answer that is not one container breaking the listing or being taken for a container: a warning, a label value with a line break or control character in it, a shortened container id. A service or a folder with control characters is not a pipeline service or this folder, so its container is not a run; nothing raises and only the real container is listed."""
        fake_run_docker.add(CONTAINER_A, root)
        fake_run_docker.add("d0" * 32, root, service="parse\x1b[2J")
        fake_run_docker.add("d1" * 32, str(root) + "\x00", service="parse")
        fake_run_docker.add("d2" * 32, str(root) + "\n", service="parse")
        fake_run_docker.extra_ps_lines = [
            "WARNING: something about the daemon", "", "   ", "{}", "null", "5", "[]", "[" * 100000,
            json.dumps(["d3" * 32, str(root), "parse"]), json.dumps(["d4" * 32, str(root), "parse", "", "extra"]),
            json.dumps(["d5" * 6, str(root), "parse", ""]), json.dumps(["D6" * 32, str(root), "parse", ""]), json.dumps(["d7" * 32 + "\n", str(root), "parse", ""]),
            json.dumps([5, str(root), "parse", ""]), json.dumps(["d8" * 32, None, "parse", ""]), json.dumps(["d9" * 32, str(root), ["parse"], ""]), json.dumps(["da" * 32, str(root), "parse", None]),
            f"{'db' * 32}\t{root}\tparse\t",
        ]

        listed = runstore.list_runs(root, SERVICES, 50)

        assert listed["docker_available"] is True
        assert [run["id"] for run in listed["runs"]] == [LIVE_A]
        assert fake_run_docker.named("inspect") == [["inspect", "--format", INSPECT_TEMPLATE, CONTAINER_A]]

    def test_a_container_that_is_gone_or_answers_oddly(self, root, fake_run_docker):
        """Bug class: a container that stopped between the listing and the look at it raising or being listed, and an inspect answer that is not a time and a source being carried into the data as if it were."""
        fake_run_docker.add(CONTAINER_A, root)
        fake_run_docker.ends_at_inspect = {CONTAINER_A}
        assert runstore.list_runs(root, SERVICES, 50) == {"docker_available": True, "runs": []}
        fake_run_docker.add(CONTAINER_A, root)

        for answer, expected in [
            ("\n", (None, None)), ("", (None, None)), ("not a time SOURCE=reddit\n", (None, None)), ("2026-11-21T10:30:00Z\n", ("2026-11-21T10:30:00Z", None)),
            ("2026-11-21T10:30:00Z \n", ("2026-11-21T10:30:00Z", None)), ("2026-11-21T10:30:00.5Z SOURCE=\n", ("2026-11-21T10:30:00Z", None)),
            ("2026-11-21T10:30:00+01:00 SOURCE=reddit\n", (None, None)), ("2026-11-21T10:30:00Z OTHER=reddit\n", ("2026-11-21T10:30:00Z", None)),
        ]:
            fake_run_docker.inspect_answer = answer
            run = runstore.list_runs(root, SERVICES, 50)["runs"][0]
            assert (run["started"], run["source"]) == expected, answer


# ── 6. what Docker is asked ─────────────────────────────────────────────────


class TestDockerCalls:
    def _everything(self, root, monkeypatch, fake_run_docker):
        """Run every operation of the store that reaches Docker once."""
        make_run(root, RUN_A, "running")
        make_run(root, RUN_B, "running")
        fake_run_docker.add(CONTAINER_A, root, log="one\ntwo\n")
        fake_run_docker.add(CONTAINER_B, root, run=RUN_A, log="three\n")
        results = [
            runstore.list_runs(root, SERVICES, 50),
            runstore.read_run(root, RUN_A),
            runstore.read_run_log(root, SERVICES, LIVE_A, 0, 100),
            runstore.read_run_log(root, SERVICES, RUN_A, 0, 100),
            runstore.follow_run_log(root, SERVICES, LIVE_A, 500),
            runstore.follow_run_log(root, SERVICES, RUN_A, 0),
            runstore.cancel_run(root, SERVICES, LIVE_A),
            runstore.cancel_run(root, SERVICES, RUN_A),
        ]
        return results

    def test_the_exact_arguments_of_every_call(self, root, monkeypatch, fake_run_docker):
        """Bug class: a Docker call that could return a container's whole environment, which holds the database passwords: an inspect without a format or with one that prints `.Config.Env` or the whole object, an `exec env`, a `ps` format with every label. The complete list of calls of every operation is compared with the one written out here, so any new call or changed format fails; and every read has the 15 second limit, every stop the 45 second one, and only following a log is attached."""
        self._everything(root, monkeypatch, fake_run_docker)
        ps_run_a = ps_for(RUN_A)

        assert fake_run_docker.calls == [
            # list_runs
            PS_ONEOFF, ["inspect", "--format", INSPECT_TEMPLATE, CONTAINER_A], PS_LABELLED,
            # read_run
            ps_run_a,
            # read_run_log: a terminal run, then a detached one
            PS_ONEOFF, ["logs", "--tail", "500", CONTAINER_A],
            ps_run_a, ["logs", "--tail", "500", CONTAINER_B],
            # follow_run_log: a terminal run, then a detached one
            PS_ONEOFF, ["logs", "--follow", "--tail", "500", CONTAINER_A],
            ps_run_a, ["logs", "--follow", "--tail", "0", CONTAINER_B],
            # cancel_run: a terminal run, then a recorded one without a supervisor
            PS_ONEOFF, ["stop", "--signal", "INT", "-t", "30", CONTAINER_A],
            ps_run_a, ps_run_a, ["stop", "--signal", "INT", "-t", "30", CONTAINER_B], ps_run_a,
        ]
        for args, options in zip(fake_run_docker.calls, fake_run_docker.options):
            following = args[:2] == ["logs", "--follow"]
            assert options["attached"] is following, args
            assert options["merge_stderr"] is (args[0] == "logs" and not following), args
            assert options["timeout"] == (None if following else 45 if args[0] == "stop" else 15), args
        for args in fake_run_docker.calls:
            assert "Env" not in " ".join(args) or args == ["inspect", "--format", INSPECT_TEMPLATE, args[-1]]

    def test_the_inspect_template_prints_one_variable(self):
        """Bug class: the template itself widened. It is compared with the text measured against Docker: the start time, then, walking the environment inside Docker, the one entry whose name is exactly SOURCE."""
        assert runstore.INSPECT_TEMPLATE == INSPECT_TEMPLATE
        assert runstore.PS_FORMAT == PS_FORMAT
        assert INSPECT_TEMPLATE.count("{{.}}") == 1 and '{{if eq (index $kv 0) "SOURCE"}}{{.}}{{end}}' in INSPECT_TEMPLATE
        assert ".Labels" not in PS_FORMAT and "json ." not in PS_FORMAT.replace("json .ID", "")

    @pytest.mark.parametrize("answer", [
        json.dumps([{"State": {"StartedAt": "2026-11-21T10:30:00Z"}, "Config": {"Env": ["SOURCE=reddit", f"POSTGRES_PASSWORD={MARKER}"]}}]),
        f"2026-11-21T10:30:00Z POSTGRES_PASSWORD={MARKER}\n",
        f"2026-11-21T10:30:00Z SOURCE=reddit\nPOSTGRES_PASSWORD={MARKER}\n",
        f"2026-11-21T10:30:00Z [SOURCE=reddit POSTGRES_PASSWORD={MARKER}]\n",
        f"POSTGRES_PASSWORD={MARKER} SOURCE=reddit\n",
    ], ids=["whole-object", "another-variable", "a-second-line", "the-whole-list", "no-time-first"])
    def test_an_answer_that_holds_more_of_the_environment_reaches_nothing(self, root, monkeypatch, fake_run_docker, answer):
        """Bug class: the store trusting Docker's answer to be what the template asked for. If an inspect ever answered with more of a container's environment (another format, another Docker), the password in it must not reach the data: the answer is taken apart strictly, and what does not fit is dropped. The same holds for the other calls: a line of the container listing that is not a container of this deployment is not carried."""
        make_run(root, RUN_A, "running")
        fake_run_docker.add(CONTAINER_A, root)
        fake_run_docker.add("e0" * 32, f"/srv/{MARKER}", service=MARKER, run=MARKER)
        fake_run_docker.inspect_answer = answer
        fake_run_docker.extra_ps_lines = [f"POSTGRES_PASSWORD={MARKER}", json.dumps([CONTAINER_B, str(root), "parse", "", f"POSTGRES_PASSWORD={MARKER}"])]

        data = [runstore.list_runs(root, SERVICES, 50), runstore.read_run(root, RUN_A)]

        assert [run["id"] for run in data[0]["runs"]] == [RUN_A, LIVE_A]
        assert MARKER not in json.dumps(data)

    def test_docker_itself_is_run_with_a_time_limit_and_without_a_terminal(self, monkeypatch, run_docker_guard):
        """Bug class: a Docker call that hangs with a daemon that does not answer, waits on the terminal, or lets an error text of Docker into the data. The real function passes the time limit on, gives the command no standard input, drops standard error except for a container's log, and decodes the output without failing on bytes that are not UTF-8."""
        seen = []

        def run(command, **kwargs):
            seen.append((command, kwargs))
            return subprocess.CompletedProcess(command, 3, stdout=b"caf\xc3\xa9 \xff\n")

        monkeypatch.setattr(subprocess, "run", run)
        monkeypatch.setattr(runstore, "_docker", run_docker_guard.real)

        assert runstore._docker(["ps", "-q"], timeout=15) == (3, "caf\u00e9 \ufffd\n")
        assert runstore._docker(["logs", "x"], timeout=15, merge_stderr=True) == (3, "caf\u00e9 \ufffd\n")
        assert runstore._docker(["logs", "--follow", "x"], timeout=None, attached=True) == (3, "")
        assert seen == [
            (["docker", "ps", "-q"], {"stdin": subprocess.DEVNULL, "stdout": subprocess.PIPE, "stderr": subprocess.DEVNULL, "timeout": 15}),
            (["docker", "logs", "x"], {"stdin": subprocess.DEVNULL, "stdout": subprocess.PIPE, "stderr": subprocess.STDOUT, "timeout": 15}),
            (["docker", "logs", "--follow", "x"], {"stdin": subprocess.DEVNULL}),
        ]

    @pytest.mark.parametrize("failure", [FileNotFoundError(2, "No such file or directory: 'docker'"), PermissionError(13, "Permission denied"), subprocess.TimeoutExpired(["docker", "ps"], 15)], ids=["no-binary", "not-allowed", "timeout"])
    def test_docker_that_cannot_be_run_is_out_of_reach(self, root, monkeypatch, run_docker_guard, failure):
        """Bug class: a host without Docker, or a daemon that does not answer in time, surfacing as a traceback of `runs list`. Each is "Docker cannot be reached": a constant message, and a listing with `docker_available` false."""
        def run(command, **kwargs):
            raise failure

        monkeypatch.setattr(subprocess, "run", run)
        monkeypatch.setattr(runstore, "_docker", run_docker_guard.real)

        with pytest.raises(runstore.DockerUnavailable) as exc:
            runstore._docker(["ps"], timeout=15)
        assert exc.value.message == NOT_REACHABLE and exc.value.__cause__ is None
        assert runstore.list_runs(root, SERVICES, 50) == {"docker_available": False, "runs": []}

    def test_a_daemon_that_is_down_is_out_of_reach(self, root, monkeypatch, run_docker_guard):
        """Bug class: `docker ps` failing because the daemon is down being read as "no containers", which would show every run without a supervisor as lost instead of unknown."""
        make_run(root, RUN_A, "running")
        monkeypatch.setattr(subprocess, "run", lambda command, **kwargs: subprocess.CompletedProcess(command, 1, stdout=b"Cannot connect to the Docker daemon"))
        monkeypatch.setattr(runstore, "_docker", run_docker_guard.real)

        listed = runstore.list_runs(root, SERVICES, 50)

        assert listed["docker_available"] is False and [run["state"] for run in listed["runs"]] == ["unknown"]


# ── 7. logs ─────────────────────────────────────────────────────────────────


class TestLogs:
    def test_pieces_by_offset_and_limit(self, root, monkeypatch, fake_run_docker):
        """Bug class: an offset or a limit counted in characters or lines instead of bytes, a `next_offset` that skips or repeats a byte, or an offset past the end raising or wrapping around."""
        run_id = make_run(root, RUN_A, "finished")
        write_log(root, run_id, b"0123456789\nabcdefghij\n")

        def piece(offset, limit):
            data = runstore.read_run_log(root, SERVICES, run_id, offset, limit)
            assert (data["id"], data["kind"], data["state"], data["complete"]) == (run_id, "web", "finished", True)
            return data["text"], data["next_offset"]

        assert piece(0, 65536) == ("0123456789\nabcdefghij\n", 22)
        assert piece(0, 5) == ("01234", 5)
        assert piece(5, 8) == ("56789\nab", 13)
        assert piece(13, 100) == ("cdefghij\n", 22)
        assert piece(21, 1) == ("\n", 22)
        assert piece(22, 100) == ("", 22)
        assert piece(1000, 100) == ("", 22)
        assert piece(2 ** 53, 1048576) == ("", 22)
        assert fake_run_docker.calls == []

    # One-, two-, three- and four-byte characters, a combining mark and a carriage return.
    TEXT = "a\u00e9\u20ac\U0001f600z e\u0301\r\n\u65e5\u672c\u8a9e \U0001f9ea\U0001f9ea\n"

    @pytest.mark.parametrize("limit", [1, 2, 3, 4, 5, 6, 7, 9, 16, 65536])
    def test_a_piece_never_ends_inside_a_character(self, root, monkeypatch, limit):
        """Bug class: a multi-byte character cut by the end of a piece, which shows as two replacement characters in a page that appends the pieces, or a reader that makes no progress when a character is longer than the limit. For every limit, each piece is whole characters (it encodes back to exactly the bytes between its two offsets), is at most three bytes over the limit, and the pieces in order are the log."""
        run_id = make_run(root, RUN_A, "finished")
        content = self.TEXT.encode("utf-8")
        write_log(root, run_id, content)

        pieces, spans = read_whole_log(root, run_id, limit)

        assert "".join(pieces) == self.TEXT
        assert spans[0][0] == 0 and spans[-1][1] == len(content)
        for text, (start, end) in zip(pieces, spans):
            assert text.encode("utf-8") == content[start:end] and "\ufffd" not in text
            assert 0 < end - start <= limit + 3
            assert end - start <= limit or len(text) == 1

    @pytest.mark.parametrize("limit", [1, 2, 3, 4, 5, 8, 65536])
    def test_bytes_that_are_not_text_are_replaced_the_same_in_pieces(self, root, monkeypatch, limit):
        """Bug class: a log with bytes that are not UTF-8 (a binary dump, Latin-1 output, a character cut by the program itself) raising, or decoding differently piece by piece than as a whole, so that the page shows other text than the terminal."""
        run_id = make_run(root, RUN_A, "finished")
        content = b"ok \xff\xfe \xe2\x82 mid \xf0\x9f\x98 x \x80\x80\x80\x80 \xc0\x80 \xed\xa0\x80 \xf8\x88 caf\xe9 end\n\xe2\xf0\x9f\x98\x80 \xf0\xe2\x82\xac \xe2\x82\xc3\xa9\n\xe2\x82\xac\xf0\x9f"
        write_log(root, run_id, content)

        pieces, spans = read_whole_log(root, run_id, limit)

        assert "".join(pieces) == content.decode("utf-8", "replace")
        assert spans[-1][1] == len(content)

    def test_a_log_that_ends_inside_a_character(self, root, monkeypatch, fake_run_docker):
        """Bug class: the first bytes of a character that the supervisor has written while the rest is still in its pipe shown as a replacement character and then never corrected. While a live process writes the log those bytes wait; once the run has ended they are given as they are, so the reader reaches the end of the file."""
        run_id = make_run(root, RUN_A, "running")
        supervisor = runstore.acquire_run_lock(root, run_id)
        write_log(root, run_id, b"abc\xf0\x9f")

        waiting = runstore.read_run_log(root, SERVICES, run_id, 0, 100)
        assert (waiting["text"], waiting["next_offset"], waiting["state"]) == ("abc", 3, "running")
        assert runstore.read_run_log(root, SERVICES, run_id, 3, 100)["next_offset"] == 3

        write_log(root, run_id, b"\x98\x80!")
        assert runstore.read_run_log(root, SERVICES, run_id, 3, 100)["text"] == "\U0001f600!"

        write_log(root, run_id, b"\xe2\x82")
        runstore.update_record(root, run_id, state="finished")
        runstore.release_run_lock(supervisor)
        ended = runstore.read_run_log(root, SERVICES, run_id, 8, 100)
        assert (ended["text"], ended["next_offset"], ended["state"]) == ("\ufffd", 10, "finished")
        assert runstore.read_run_log(root, SERVICES, run_id, 8, 1)["next_offset"] == 10

    def test_a_run_without_a_log_file(self, root, monkeypatch, fake_run_docker):
        """Bug class: a run that has not written anything yet (or never did) answered with an error instead of an empty log."""
        run_id = make_run(root, RUN_A, "starting")
        supervisor = runstore.acquire_run_lock(root, run_id)

        for offset in (0, 5):
            assert runstore.read_run_log(root, SERVICES, run_id, offset, 100) == {"id": run_id, "kind": "web", "state": "starting", "text": "", "next_offset": 0, "complete": True}
        runstore.release_run_lock(supervisor)

    @pytest.mark.parametrize("in_its_place", ["link", "folder", "pipe"])
    def test_a_log_that_is_no_regular_file_is_refused(self, root, tmp_path, monkeypatch, in_its_place):
        """Bug class: any file of the host served as a run's log through a symbolic link in the log's place (a key, a file with passwords), or a read that never returns on a named pipe. The log is opened without following a link and must be a regular file; the refusal is a constant message and holds nothing of the file behind the link."""
        run_id = make_run(root, RUN_A, "finished")
        secret = tmp_path / "secret-file"
        secret.write_text(MARKER)
        log = run_dir(root, run_id) / "log"
        if in_its_place == "link":
            log.symlink_to(secret)
        elif in_its_place == "folder":
            log.mkdir()
        else:
            os.mkfifo(log)

        with failing_instead_of_hanging(), pytest.raises(runstore.StoreError) as exc:
            runstore.read_run_log(root, SERVICES, run_id, 0, 100)

        assert exc.value.message == LOG_NOT_A_FILE and MARKER not in repr(exc.value)

    def test_the_output_of_a_terminal_run_comes_from_docker(self, root, fake_run_docker):
        """Bug class: a terminal run, which has no log file, answered with an empty log; or its output asked from Docker for a container that is not a listed run of this deployment."""
        fake_run_docker.add(CONTAINER_A, root, log="parsing RC_2024-01\r 50%\r100%\ndone \x1b[32mok\x1b[0m\n")

        data = runstore.read_run_log(root, SERVICES, LIVE_A, 7, 65536)

        assert data == {"id": LIVE_A, "kind": "terminal", "state": "running", "text": "parsing RC_2024-01\r 50%\r100%\ndone \x1b[32mok\x1b[0m\n", "next_offset": None, "complete": False}
        assert fake_run_docker.calls == [PS_ONEOFF, ["logs", "--tail", "500", CONTAINER_A]]
        with pytest.raises(runstore.RunNotFound):
            runstore.read_run_log(root, SERVICES, "live-" + "b2" * 6, 0, 100)
        assert fake_run_docker.named("logs") == [["logs", "--tail", "500", CONTAINER_A]]

    # 28 bytes: the euro sign is three, the face four.
    DOCKER_LOG = "first\nsecond \u20ac\nlast \U0001f600!\n"

    @pytest.mark.parametrize("kind", ["terminal", "detached"])
    @pytest.mark.parametrize("limit, expected", [
        (1048576, DOCKER_LOG), (28, DOCKER_LOG), (27, "irst\nsecond \u20ac\nlast \U0001f600!\n"), (11, "last \U0001f600!\n"), (6, "\U0001f600!\n"),
        (5, "!\n"), (4, "!\n"), (3, "!\n"), (2, "!\n"), (1, "\n"), (12, "\nlast \U0001f600!\n"), (13, "\nlast \U0001f600!\n"), (14, "\nlast \U0001f600!\n"), (15, "\u20ac\nlast \U0001f600!\n"),
    ])
    def test_output_from_docker_is_cut_to_its_last_bytes(self, root, fake_run_docker, kind, limit, expected):
        """Bug class: an answer far larger than the caller asked for (500 lines can be megabytes, and a caller that set a limit sized its buffer by it), the beginning kept instead of the end (the newest lines are what a tail is for), or the cut made inside a character, which shows as a replacement character at the start. The text is the end of what Docker returned, at most `limit` bytes, and begins with a whole character; `complete` stays false."""
        if kind == "terminal":
            run_id = LIVE_A
            fake_run_docker.add(CONTAINER_A, root, log=self.DOCKER_LOG)
        else:
            run_id = make_run(root, RUN_A, "running")
            fake_run_docker.add(CONTAINER_A, root, run=run_id, log=self.DOCKER_LOG)

        data = runstore.read_run_log(root, SERVICES, run_id, 0, limit)

        assert (data["text"], data["next_offset"], data["complete"]) == (expected, None, False)
        assert len(data["text"].encode("utf-8")) <= limit and "\ufffd" not in data["text"]
        assert self.DOCKER_LOG.endswith(data["text"])

    def test_a_terminal_run_that_just_ended_or_docker_out_of_reach(self, root, fake_run_docker):
        """Bug class: an empty log returned for a run that is gone or cannot be asked about, which reads as "the run printed nothing"."""
        fake_run_docker.add(CONTAINER_A, root)
        fake_run_docker.ends_at_logs = {CONTAINER_A}
        with pytest.raises(runstore.RunNotFound) as exc:
            runstore.read_run_log(root, SERVICES, LIVE_A, 0, 100)
        assert exc.value.message == NOT_FOUND
        assert fake_run_docker.calls == [PS_ONEOFF, ["logs", "--tail", "500", CONTAINER_A], PS_ONEOFF]

        fake_run_docker.add(CONTAINER_A, root)
        fake_run_docker.reachable = False
        with pytest.raises(runstore.DockerUnavailable) as exc:
            runstore.read_run_log(root, SERVICES, LIVE_A, 0, 100)
        assert exc.value.message == NOT_REACHABLE

    @pytest.mark.parametrize("kind", ["terminal", "detached"])
    @pytest.mark.parametrize("failure, expected", [("daemon-stops", runstore.DockerUnavailable), ("not-handed-over", runstore.StoreError), ("container-ended", None)])
    def test_output_that_docker_does_not_hand_over(self, root, fake_run_docker, kind, failure, expected):
        """Bug class: every failure of `docker logs` read as "the container is gone": with the daemon stopping between the listing and the reading, a terminal run answered with "no run with this id" and a detached run with an empty text and no error, which reads as "the run printed nothing"; and a container that is still listed, whose output Docker would not give, answered the same way. After a failed reading the listing is asked once more, which tells the three apart: Docker out of reach, a constant "cannot be read" refusal for a container that is still listed, and for one that ended "no run with this id" (a terminal run leaves nothing behind) or the run's log file with the state `lost` (a recorded run)."""
        if kind == "terminal":
            run_id, first, again = LIVE_A, PS_ONEOFF, PS_ONEOFF
            fake_run_docker.add(CONTAINER_A, root, log="new lines\n")
        else:
            run_id = make_run(root, RUN_A, "running")
            first = again = ps_for(run_id)
            write_log(root, run_id, b"old lines\n")
            fake_run_docker.add(CONTAINER_A, root, run=run_id, log="old lines\nnew lines\n")
        assert runstore.read_run_log(root, SERVICES, run_id, 0, 100)["text"].endswith("new lines\n")
        fake_run_docker.calls.clear()
        if failure == "daemon-stops":
            fake_run_docker.daemon_down_from = 1
        elif failure == "not-handed-over":
            fake_run_docker.logs_fail = True
        else:
            fake_run_docker.ends_at_logs = {CONTAINER_A}

        data, error = outcome_of(lambda: runstore.read_run_log(root, SERVICES, run_id, 0, 100))

        assert fake_run_docker.calls == [first, ["logs", "--tail", "500", CONTAINER_A], again]
        if failure == "daemon-stops":
            assert type(error) is runstore.DockerUnavailable and error.message == NOT_REACHABLE
        elif failure == "not-handed-over":
            assert type(error) is runstore.StoreError and error.message == OUTPUT_NOT_READABLE
        elif kind == "terminal":
            assert type(error) is runstore.RunNotFound and error.message == NOT_FOUND
        else:
            assert error is None
            assert data == {"id": run_id, "kind": "web", "state": "lost", "text": "old lines\n", "next_offset": 10, "complete": True}

    def test_the_output_of_a_detached_run_comes_from_docker(self, root, monkeypatch, fake_run_docker):
        """Bug class: a detached run answered from its log file, which stopped growing when its supervisor died, so the output looks frozen while the container works on."""
        run_id = make_run(root, RUN_A, "running")
        write_log(root, run_id, b"old lines\n")
        fake_run_docker.add(CONTAINER_A, root, run=run_id, log="old lines\nnew lines\n")

        data = runstore.read_run_log(root, SERVICES, run_id, 0, 100)

        assert data == {"id": run_id, "kind": "web", "state": "detached", "text": "old lines\nnew lines\n", "next_offset": None, "complete": False}


# ── 8. cancel ───────────────────────────────────────────────────────────────


class TestCancel:
    def test_a_terminal_run_is_stopped_with_the_interrupt_signal(self, root, fake_run_docker):
        """Bug class: a cancel that sends the default stop signal (the pipeline's Python process ignores it and is killed after the wait, with no chance to clean up), that waits for ever, or that hands Docker the shortened id from the request instead of the container it looked up. Exactly one stop, with the interrupt signal, the 30 second wait and the full container id."""
        fake_run_docker.add(CONTAINER_A, root)
        fake_run_docker.add(CONTAINER_B, root)

        assert runstore.cancel_run(root, SERVICES, LIVE_A) == "stopped"

        assert fake_run_docker.calls == [PS_ONEOFF, ["stop", "--signal", "INT", "-t", "30", CONTAINER_A]]
        assert fake_run_docker.options[1] == {"timeout": 45, "merge_stderr": False, "attached": False}
        assert [container["id"] for container in fake_run_docker.containers] == [CONTAINER_B]

    def test_a_terminal_run_that_is_not_listed_is_refused(self, root, tmp_path, fake_run_docker):
        """Bug class: a well-formed terminal id used to stop a container that is no run of this deployment: one that does not exist, one of another checkout, a database server's one-off command, a recorded run's container, or two containers whose ids start alike. The id is looked up in the current listing and nothing is stopped unless exactly one listed run has it."""
        other = tmp_path.resolve() / "other-checkout"
        other.mkdir()
        fake_run_docker.add("a1" * 6 + "00" * 26, other)
        fake_run_docker.add("a1" * 6 + "11" * 26, root, service="postgres")
        fake_run_docker.add("a1" * 6 + "22" * 26, root, run=RUN_A)

        with pytest.raises(runstore.RunNotFound) as exc:
            runstore.cancel_run(root, SERVICES, LIVE_A)
        assert exc.value.message == NOT_FOUND

        fake_run_docker.add("a1" * 6 + "33" * 26, root)
        fake_run_docker.add("a1" * 6 + "44" * 26, root)
        with pytest.raises(runstore.RunNotFound):
            runstore.cancel_run(root, SERVICES, LIVE_A)
        assert fake_run_docker.named("stop") == [] and len(fake_run_docker.containers) == 5

    def test_a_terminal_run_that_ended_first(self, root, fake_run_docker):
        """Bug class: "cancelled" reported for a run whose container was gone before the signal reached it."""
        fake_run_docker.add(CONTAINER_A, root)
        fake_run_docker.ends_before_stop = True

        with pytest.raises(runstore.CancelRefused) as exc:
            runstore.cancel_run(root, SERVICES, LIVE_A)
        assert exc.value.message == ENDED
        assert fake_run_docker.calls == [PS_ONEOFF, ["stop", "--signal", "INT", "-t", "30", CONTAINER_A], PS_ONEOFF]

    def test_a_terminal_run_that_docker_could_not_stop(self, root, fake_run_docker):
        """Bug class: a stop that failed reported as "the run has already ended" while the container runs on, so the operator believes the run is over. After a failed stop the listing is asked again: a container that is still listed was not stopped, and the refusal says so."""
        fake_run_docker.add(CONTAINER_A, root)
        fake_run_docker.stop_fails = True

        with pytest.raises(runstore.CancelRefused) as exc:
            runstore.cancel_run(root, SERVICES, LIVE_A)

        assert exc.value.message == STOP_FAILED
        assert fake_run_docker.calls == [PS_ONEOFF, ["stop", "--signal", "INT", "-t", "30", CONTAINER_A], PS_ONEOFF]
        assert [container["id"] for container in fake_run_docker.containers] == [CONTAINER_A]

    def test_a_recorded_run_that_docker_could_not_stop_stays_cancellable(self, root, monkeypatch, fake_run_docker):
        """Bug class: a detached run recorded as cancelled although the stop failed. A cancelled run is final, so its container would run on with no way left to stop it through `runs`. The containers of the run are listed again after the stop; while one is left the cancel is refused and the record stays as it was, and a second cancel, once Docker can stop the container, ends the run."""
        run_id = make_run(root, RUN_A, "running")
        fake_run_docker.add(CONTAINER_A, root, run=run_id)
        fake_run_docker.stop_fails = True
        record = (run_dir(root, run_id) / "record.json").read_bytes()

        with pytest.raises(runstore.CancelRefused) as exc:
            runstore.cancel_run(root, SERVICES, run_id)

        assert exc.value.message == STOP_FAILED
        assert fake_run_docker.calls == [ps_for(run_id), ps_for(run_id), ["stop", "--signal", "INT", "-t", "30", CONTAINER_A], ps_for(run_id)]
        assert (run_dir(root, run_id) / "record.json").read_bytes() == record
        assert runstore.read_run(root, run_id)["state"] == "detached"

        fake_run_docker.stop_fails = False
        assert runstore.cancel_run(root, SERVICES, run_id) == "stopped"
        assert runstore.read_record(root, run_id)["state"] == "cancelled" and fake_run_docker.containers == []

    @pytest.mark.parametrize("state, message", [("finished", ENDED), ("cancelled", ENDED), ("lost", LOST)])
    def test_a_run_that_is_over_is_refused_and_nothing_is_written(self, root, monkeypatch, fake_run_docker, state, message):
        """Bug class: a cancel that rewrites the record of a run that has ended (a finished run turned into a cancelled one, its end time and exit code lost), leaves a cancel request for a run nobody supervises, or stops a container."""
        if state == "lost":
            run_id = make_run(root, RUN_A, "running")
        else:
            run_id = make_run(root, RUN_A, state, ended="2026-11-21T10:20:00Z", exit_code=0)
            fake_run_docker.add(CONTAINER_A, root, run=run_id)
        before = tree(root)

        with pytest.raises(runstore.CancelRefused) as exc:
            runstore.cancel_run(root, SERVICES, run_id)

        assert exc.value.message == message
        assert tree(root) == before and fake_run_docker.named("stop") == []

    def test_a_run_with_a_live_supervisor_only_gets_its_cancel_file(self, root, monkeypatch, fake_run_docker):
        """Bug class: the cancel command stopping the container, or writing the record, behind the back of the process that supervises the run, which then records a failure for a run that was cancelled, or two writers of one record. With the run's lock held, cancel writes the request and nothing else; asked again, it does the same."""
        run_id = make_run(root, RUN_A, "running")
        fake_run_docker.add(CONTAINER_A, root, run=run_id)
        supervisor = runstore.acquire_run_lock(root, run_id)
        record = (run_dir(root, run_id) / "record.json").read_bytes()

        for _ in range(2):
            assert runstore.cancel_run(root, SERVICES, run_id) == "requested"

            assert sorted(os.listdir(run_dir(root, run_id))) == ["cancel", "lock", "record.json"]
            assert (run_dir(root, run_id) / "record.json").read_bytes() == record
            assert fake_run_docker.calls == [] and len(fake_run_docker.containers) == 1
        assert runstore.acquire_run_lock(root, run_id) is None
        runstore.release_run_lock(supervisor)

    def test_a_run_without_a_supervisor_is_stopped_by_its_label(self, root, monkeypatch, fake_run_docker):
        """Bug class: a detached run that cannot be cancelled (nobody is left to act on the request), is cancelled by a name or id read from its record instead of the label computed from its id, or stays `detached` in the listing after its container was stopped. With the lock free the container that carries the run's label is stopped the same way as a terminal run and the record is written as cancelled, with the end time; a second cancel finds the run ended and stops nothing more."""
        monkeypatch.setattr(runstore, "_now", lambda: "2026-11-21T10:15:00Z")
        run_id = make_run(root, RUN_A, "running")
        monkeypatch.setattr(runstore, "_now", lambda: "2026-11-21T10:45:00Z")
        fake_run_docker.add(CONTAINER_A, root, run=run_id)
        fake_run_docker.add(CONTAINER_B, root, run=RUN_B)
        assert runstore.read_run(root, run_id)["state"] == "detached"
        fake_run_docker.calls.clear()

        assert runstore.cancel_run(root, SERVICES, run_id) == "stopped"

        ps_run = ps_for(run_id)
        assert fake_run_docker.calls == [ps_run, ps_run, ["stop", "--signal", "INT", "-t", "30", CONTAINER_A], ps_run]
        record = runstore.read_record(root, run_id)
        assert (record["state"], record["ended"], record["exit_code"], record["started"]) == ("cancelled", "2026-11-21T10:45:00Z", None, "2026-11-21T10:15:00Z")
        assert runstore.cancel_requested(root, run_id) is True
        assert [container["id"] for container in fake_run_docker.containers] == [CONTAINER_B]
        held = runstore.acquire_run_lock(root, run_id)
        assert isinstance(held, int)
        runstore.release_run_lock(held)

        fake_run_docker.calls.clear()
        with pytest.raises(runstore.CancelRefused) as exc:
            runstore.cancel_run(root, SERVICES, run_id)
        assert exc.value.message == ENDED and fake_run_docker.calls == []
        assert runstore.read_record(root, run_id) == record

    def test_the_supervisor_ending_the_run_first_wins(self, root, monkeypatch, fake_run_docker):
        """Bug class: a run that finished at the moment it was cancelled being recorded as cancelled, its exit code lost: the look at the state said "no supervisor, container alive", and by the time the lock is taken the supervisor's last act, the final record, is there. The record is read again under the lock and a final one is left alone, without a cancel request next to it."""
        run_id = make_run(root, RUN_A, "running")
        fake_run_docker.add(CONTAINER_A, root, run=run_id)

        def supervisor_finishes(label):
            fake_run_docker.on_ps = None
            runstore.update_record(root, run_id, state="finished", ended="2026-11-21T10:20:00Z", exit_code=0)

        fake_run_docker.on_ps = supervisor_finishes
        with pytest.raises(runstore.CancelRefused) as exc:
            runstore.cancel_run(root, SERVICES, run_id)

        assert exc.value.message == ENDED
        assert runstore.read_record(root, run_id)["state"] == "finished" and runstore.read_record(root, run_id)["exit_code"] == 0
        assert fake_run_docker.named("stop") == []
        # No cancel request is left behind for a run that ended by itself.
        assert runstore.cancel_requested(root, run_id) is False and "cancel" not in os.listdir(run_dir(root, run_id))

    def test_docker_out_of_reach_changes_no_record(self, root, monkeypatch, fake_run_docker):
        """Bug class: a run recorded as cancelled although its container could not be stopped because Docker did not answer; it would then run on, shown as cancelled. And a cancel request left in the folder of a run about which nothing was decided. The cancel fails with the constant message, the record stays, no cancel file is written, and the lock is given up."""
        run_id = make_run(root, RUN_A, "running")
        fake_run_docker.add(CONTAINER_A, root, run=run_id)
        fake_run_docker.reachable = False

        with pytest.raises(runstore.DockerUnavailable) as exc:
            runstore.cancel_run(root, SERVICES, run_id)

        assert exc.value.message == NOT_REACHABLE
        assert runstore.read_record(root, run_id)["state"] == "running"
        # Nothing was decided, so no cancel request was written either.
        assert runstore.cancel_requested(root, run_id) is False and "cancel" not in os.listdir(run_dir(root, run_id))
        held = runstore.acquire_run_lock(root, run_id)
        assert isinstance(held, int)
        runstore.release_run_lock(held)
        with pytest.raises(runstore.DockerUnavailable):
            runstore.cancel_run(root, SERVICES, LIVE_A)

    def test_a_container_that_ended_between_the_look_and_the_stop(self, root, monkeypatch, fake_run_docker):
        """Bug class: a detached run whose container ends by itself just before the stop left as `running` in its record for ever, although the cancel holds the lock and knows nobody else will write it."""
        run_id = make_run(root, RUN_A, "running")
        fake_run_docker.add(CONTAINER_A, root, run=run_id)
        seen = []

        def container_ends(label):
            seen.append(label)
            if len(seen) == 2:
                fake_run_docker.containers.clear()

        fake_run_docker.on_ps = container_ends
        assert runstore.cancel_run(root, SERVICES, run_id) == "recorded"

        assert runstore.read_record(root, run_id)["state"] == "cancelled" and fake_run_docker.named("stop") == []


# ── where the module sits ───────────────────────────────────────────────────


def test_the_store_imports_neither_the_cli_nor_the_registry():
    """Bug class: the store pulling in the CLI script (a cycle once the CLI uses the store, and the whole CLI in any process that only reads runs), the control package, or anything outside the standard library. Run in a fresh interpreter, since this process has imported more."""
    code = (
        "import sys; import social_data_pipeline.runstore; "
        "bad = sorted(m for m in sys.modules if m == 'sdp' or m == 'yaml' or m.startswith('social_data_pipeline.') and m != 'social_data_pipeline.runstore'); "
        "print(bad); sys.exit(1 if bad else 0)"
    )

    result = subprocess.run([sys.executable, "-c", code], cwd=REPO_ROOT, capture_output=True, text=True)

    assert result.returncode == 0, result.stdout + result.stderr
