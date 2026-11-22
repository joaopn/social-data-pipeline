"""Tests of the scheduler's control socket (`social_data_pipeline.jobs.door`) against a real unix socket in the temp folder, a real `Store` there, stand-ins for the runner and a recording auto-accept store, and a real asyncio event loop in a thread of its own that stands for the scheduler's loop. The kernel's peer credentials and the owner of the data folder are injected where a test needs "another user" or "root owns the data folder" without being root.

The groups follow the properties: opening, peers, the shape of every op, `jobs` and `job`, `approve`, `reject` and `kill`, `explain`, `preview`, `stats`, the bounds, the scheduler's start with a door that cannot be opened, the `Job` record.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import socket
import stat
import struct
import sys
import threading
import time
from unittest.mock import MagicMock

import pytest

from social_data_pipeline.control import framing
from social_data_pipeline.jobs import door as door_module
from social_data_pipeline.jobs.auto_accept import AutoAcceptStore
from social_data_pipeline.jobs.config import JobsConfig, Target
from social_data_pipeline.jobs.store import Job, Store

if isinstance(sys.modules.get("fastapi"), MagicMock):
    # The scheduler's app module imports this one too; `tests/jobs/conftest.py` stands in for the rest of FastAPI where it is not installed.
    sys.modules.setdefault("fastapi.staticfiles", MagicMock())

MARKER = "planted-marker-5f1c0e"
JOB_ID = "pg_0123abcd"
OTHER_ID = "sr_89abcdef"
LOG_NAME = "social_data_pipeline.jobs.door"

# ── stand-ins ───────────────────────────────────────────────────────────────


class RecordingStore(Store):
    """The real store, recording every call the door makes with the thread it was made on."""

    def __init__(self, jobs_dir):
        super().__init__(jobs_dir)
        self.calls = []
        self.raise_on = {}
        self.block_on = {}

    def _note(self, name):
        self.calls.append((name, threading.get_ident()))
        if name in self.block_on:
            assert self.block_on[name].wait(15)
        if name in self.raise_on:
            raise self.raise_on[name]

    def find(self, job_id):
        self._note("find")
        return super().find(job_id)

    def approve(self, job_id, *, by=None):
        self._note("approve")
        return super().approve(job_id, by=by)

    def reject(self, job_id, reason=None, *, by=None):
        self._note("reject")
        return super().reject(job_id, reason, by=by)

    def list_phase(self, phase):
        self._note("list_phase")
        return super().list_phase(phase)

    def iter_history(self, limit=None):
        self._note("iter_history")
        return super().iter_history(limit)

    def job_result_dir(self, job_id):
        self._note("job_result_dir")
        return super().job_result_dir(job_id)


class StandInRunner:
    """Records `request_cancel` and `explain`; `explain` answers `plan`, raises `explain_error` or waits on `explain_gate`."""

    def __init__(self):
        self.cancels = []
        self.explains = []
        self.was_running = True
        self.cancel_error = None
        self.plan = "Seq Scan on t"
        self.explain_error = None
        self.explain_gate = None

    def request_cancel(self, job_id, by=None):
        self.cancels.append((job_id, by, threading.get_ident()))
        if self.cancel_error is not None:
            raise self.cancel_error
        return self.was_running

    def explain(self, job_id):
        self.explains.append((job_id, threading.get_ident()))
        if self.explain_gate is not None:
            assert self.explain_gate.wait(15)
        if self.explain_error is not None:
            raise self.explain_error
        return self.plan


class RecordingAutoAccept(AutoAcceptStore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sets = []

    def set_target(self, name, *, enabled=None, limit=None):
        self.sets.append((name, enabled, limit, threading.get_ident()))
        return super().set_target(name, enabled=enabled, limit=limit)


# ── the rig ─────────────────────────────────────────────────────────────────


@pytest.fixture
def loop():
    """A real event loop running in a thread of its own: the scheduler's loop. Yields (loop, the thread's ident)."""
    running = asyncio.new_event_loop()
    errors = []
    running.set_exception_handler(lambda _loop, context: errors.append(context))
    thread = threading.Thread(target=running.run_forever, daemon=True)
    thread.start()
    yield running, thread.ident, errors
    if not running.is_closed():
        running.call_soon_threadsafe(running.stop)
        thread.join(10)
        running.close()


class Rig:
    def __init__(self, tmp_path, loop):
        self.loop, self.loop_thread, self.loop_errors = loop
        self.jobs_dir = tmp_path / "jobs"
        self.jobs_dir.mkdir()
        os.chmod(self.jobs_dir, 0o755)
        self.cfg = JobsConfig(
            port=8050,
            jobs_dir=self.jobs_dir,
            result_root=self.jobs_dir / "results",
            host_result_root=str(self.jobs_dir / "results"),
            max_concurrent=3,
            default_timeouts={"postgres": 0, "starrocks": 259200, "mongodb": 0},
            history_retention=500,
            targets={
                "warehouse": Target(name="warehouse", backend="postgres", database="datasets"),
                "olap": Target(name="olap", backend="starrocks", database=""),
            },
        )
        self.store = RecordingStore(self.jobs_dir)
        self.runner = StandInRunner()
        self.auto_accept = RecordingAutoAccept(state_path=self.jobs_dir / "auto_accept.json", max_limit=3)
        self.peer = os.getuid()
        self.door = None

    @property
    def control(self):
        return self.jobs_dir / "control"

    def open(self, **kwargs):
        kwargs.setdefault("peer_uid", lambda connection: self.peer)
        self.door = door_module.open_door(self.cfg, self.store, self.runner, self.auto_accept, self.loop, **kwargs)
        return self.door

    def connect(self, timeout=10):
        """A connection to the socket, made through the folder's descriptor so that the temp folder's path length never matters."""
        folder = os.open(self.control, os.O_RDONLY | os.O_DIRECTORY)
        try:
            connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            connection.settimeout(timeout)
            connection.connect(f"/proc/self/fd/{folder}/{door_module.SOCKET_NAME}")
        finally:
            os.close(folder)
        return connection

    def ask_bytes(self, payload, timeout=10):
        """Send raw bytes and return the reply's document, or None when the connection ends without one."""
        connection = self.connect(timeout)
        try:
            with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                connection.sendall(payload)
            return read_reply(connection)
        finally:
            connection.close()

    def ask(self, document, timeout=10):
        return self.ask_bytes(framing.encode(document, max_bytes=1 << 22), timeout)

    def result(self, document):
        reply = self.ask(document)
        assert reply is not None and reply["ok"] is True, reply
        return reply["result"]

    def code(self, document):
        reply = self.ask(document)
        assert reply is not None and reply["ok"] is False, reply
        return reply["error"]["code"]

    def submit(self, job_id=JOB_ID, **overrides):
        job = make_job(job_id, **overrides)
        self.store.submit(job)
        return job

    def finished(self, job_id=JOB_ID, status="done", result_path=None, **overrides):
        """A job in the history, put there by the store's own transitions."""
        Store.submit(self.store, make_job(job_id, **overrides))
        Store.approve(self.store, job_id)
        job = self.store.claim_approved()
        job.result_path = result_path
        if status == "done":
            self.store.complete(job)
        else:
            self.store.fail(job, error="it failed")
        return job

    def store_calls(self):
        return [name for name, _ident in self.store.calls]

    def close(self):
        if self.door is not None:
            self.door.close()


def read_reply(connection):
    data = b""
    try:
        while True:
            chunk = connection.recv(1 << 20)
            if not chunk:
                break
            data += chunk
    except (ConnectionResetError, TimeoutError):
        pass
    if not data:
        return None
    (length,) = struct.unpack(">I", data[:4])
    assert len(data) == 4 + length
    return framing.decode(data[4:])


def make_job(job_id=JOB_ID, **overrides):
    base = dict(job_id=job_id, target="warehouse", backend="postgres", sql="SELECT 1", output_filename="out.csv", overwrite=False, submitted_at=1700000000.0)
    base.update(overrides)
    return Job(**base)


@pytest.fixture
def rig(tmp_path, loop):
    made = Rig(tmp_path, loop)
    yield made
    made.close()


@pytest.fixture
def opened(rig):
    assert rig.open() is not None
    return rig


def wait_for(what, probe, seconds=10):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if probe():
            return
        time.sleep(0.01)
    raise AssertionError(f"timed out waiting for {what}")


def door_lines(caplog):
    return [record.getMessage() for record in caplog.records if record.name == LOG_NAME]


# ── a. opening ──────────────────────────────────────────────────────────────


class TestOpening:
    def test_modes_under_a_zero_umask_and_the_umask_never_touched(self, rig, monkeypatch):
        """Bug class: the folder or the socket left open to everyone under the container's umask of 0, or the door changing the process's umask (a runner thread making a job folder at that moment would get the wrong mode)."""
        previous = os.umask(0)
        try:
            umask_calls = []
            real_umask = os.umask
            monkeypatch.setattr(os, "umask", lambda mask: umask_calls.append(mask) or real_umask(mask))
            assert rig.open() is not None
            monkeypatch.setattr(os, "umask", real_umask)
            assert umask_calls == []
            assert os.umask(0) == 0
        finally:
            os.umask(previous)
        folder = os.lstat(rig.control)
        sock = os.lstat(rig.control / door_module.SOCKET_NAME)
        assert stat.S_ISDIR(folder.st_mode) and stat.S_IMODE(folder.st_mode) == 0o700
        assert stat.S_ISSOCK(sock.st_mode) and stat.S_IMODE(sock.st_mode) == 0o600
        assert (folder.st_uid, sock.st_uid) == (os.getuid(), os.getuid())
        assert rig.result({"op": "hello"}) == {"protocol": 1}

    @pytest.mark.parametrize("kind", ["link", "file", "other_owner", "group_readable", "others_readable"])
    def test_an_unusable_existing_folder_refuses_the_door_and_is_left_as_it_is(self, rig, tmp_path, caplog, kind):
        """Bug class: a control folder that someone else could reach or that points elsewhere used anyway (a socket bound through a link), or "repaired" by deleting it (a bind mount of it would then point at the old folder)."""
        caplog.set_level(logging.INFO, logger=LOG_NAME)
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir(mode=0o700)
        owner = None
        if kind == "link":
            os.symlink(elsewhere, rig.control)
            expected = door_module.CLOSED_NOT_A_FOLDER
        elif kind == "file":
            rig.control.write_text("x")
            expected = door_module.CLOSED_NOT_A_FOLDER
        else:
            rig.control.mkdir()
            os.chmod(rig.control, {"other_owner": 0o700, "group_readable": 0o750, "others_readable": 0o705}[kind])
            if kind == "other_owner":
                owner = (os.getuid() + 1, os.getgid())
                expected = door_module.CLOSED_OTHER_OWNER
            else:
                expected = door_module.CLOSED_OPEN_FOLDER
        before = os.lstat(rig.control)

        assert rig.open(owner=owner) is None

        after = os.lstat(rig.control)
        assert (after.st_ino, after.st_mode, after.st_uid) == (before.st_ino, before.st_mode, before.st_uid)
        assert os.listdir(elsewhere) == []
        if stat.S_ISDIR(after.st_mode) and not stat.S_ISLNK(after.st_mode):
            assert os.listdir(rig.control) == []
        assert door_lines(caplog) == [expected]

    def test_a_missing_folder_that_cannot_be_given_to_the_owner_refuses_the_door(self, rig, caplog):
        """Bug class: a folder made by the scheduler (root in the container) left to root and the door opened in it, so the operator can never connect, or opened for a user who does not own it."""
        caplog.set_level(logging.INFO, logger=LOG_NAME)
        assert rig.open(owner=(os.getuid() + 1, os.getgid())) is None
        assert door_lines(caplog) == [door_module.CLOSED_NOT_MADE]
        assert os.listdir(rig.control) == []

    def test_the_folder_is_never_made_anew(self, rig):
        """Bug class: the folder deleted and made again at a close or an open, so that a bind mount of it (the web UI's) keeps pointing at the old, empty one."""
        assert rig.open() is not None
        first = os.lstat(rig.control).st_ino
        rig.close()
        assert not (rig.control / door_module.SOCKET_NAME).exists()
        assert os.lstat(rig.control).st_ino == first
        assert rig.open() is not None
        assert os.lstat(rig.control).st_ino == first
        assert rig.result({"op": "hello"}) == {"protocol": 1}

    def test_a_leftover_socket_is_replaced(self, rig):
        """Bug class: a socket left by a scheduler that was killed keeping the door shut for good."""
        rig.control.mkdir(mode=0o700)
        os.chmod(rig.control, 0o700)
        folder = os.open(rig.control, os.O_RDONLY | os.O_DIRECTORY)
        stale = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        stale.bind(f"/proc/self/fd/{folder}/{door_module.SOCKET_NAME}")
        stale.close()
        os.close(folder)
        old = os.lstat(rig.control / door_module.SOCKET_NAME).st_ino

        assert rig.open() is not None

        assert os.lstat(rig.control / door_module.SOCKET_NAME).st_ino != old
        assert rig.result({"op": "hello"}) == {"protocol": 1}

    @pytest.mark.parametrize("kind", ["file", "link"])
    def test_anything_else_in_the_socket_place_refuses_the_door(self, rig, tmp_path, caplog, kind):
        """Bug class: a file or a link in the socket's place deleted (or followed) to make room, which lets whoever put it there delete or redirect at will."""
        caplog.set_level(logging.INFO, logger=LOG_NAME)
        rig.control.mkdir(mode=0o700)
        os.chmod(rig.control, 0o700)
        place = rig.control / door_module.SOCKET_NAME
        target = tmp_path / "target"
        target.write_text(MARKER)
        if kind == "file":
            place.write_text(MARKER)
        else:
            os.symlink(target, place)

        assert rig.open() is None

        assert os.path.lexists(place)
        assert stat.S_ISLNK(os.lstat(place).st_mode) == (kind == "link")
        assert target.read_text() == MARKER
        assert door_lines(caplog) == [door_module.CLOSED_LEFTOVER]

    def test_a_data_folder_of_root_refuses_the_door(self, rig, caplog):
        """Bug class: a door opened for uid 0 when the data folder belongs to root: nobody but root could use it, and the operator is told nothing."""
        caplog.set_level(logging.INFO, logger=LOG_NAME)
        assert rig.open(owner=(0, 0)) is None
        assert not rig.control.exists()
        assert door_lines(caplog) == [door_module.CLOSED_ROOT_OWNER]

    @pytest.mark.parametrize("where", ["same", "parent", "link_to_parent", "inside_control"])
    def test_a_results_folder_that_holds_the_data_folder_refuses_the_door(self, rig, tmp_path, caplog, where):
        """Bug class: the door opened inside a tree the database servers can write (the results folder), where one of them could replace the socket or the folder."""
        caplog.set_level(logging.INFO, logger=LOG_NAME)
        expected = door_module.CLOSED_RESULTS_CONTAIN
        if where == "same":
            rig.cfg.result_root = rig.jobs_dir
        elif where == "parent":
            rig.cfg.result_root = rig.jobs_dir.parent
        elif where == "link_to_parent":
            os.symlink(rig.jobs_dir.parent, tmp_path / "results-link")
            rig.cfg.result_root = tmp_path / "results-link"
        else:
            rig.cfg.result_root = rig.control / "results"
            expected = door_module.CLOSED_RESULTS_INSIDE
        assert rig.open() is None
        assert door_lines(caplog) == [expected]

    def test_the_data_folder_found_below_the_results_folder_by_its_inode(self, tmp_path):
        """Bug class: in the container the data folder and the results folder are two mounts, so a results folder on the host that contains the data folder shares no path with it: without a look below the results folder it passes."""
        results = tmp_path / "results"
        data = results / "a" / "b" / "jobs"
        data.mkdir(parents=True)
        (results / "pg_00000000" / "x").mkdir(parents=True)
        info = os.stat(data)
        assert door_module._found_below(results, (info.st_dev, info.st_ino)) is True
        hidden = os.stat(results / "pg_00000000" / "x")
        # Job folders are not entered, and nothing deeper than three levels is looked at.
        assert door_module._found_below(results, (hidden.st_dev, hidden.st_ino)) is False
        deep = results / "a" / "b" / "c" / "d"
        deep.mkdir(parents=True)
        deep_info = os.stat(deep)
        assert door_module._found_below(results, (deep_info.st_dev, deep_info.st_ino)) is False

    def test_a_results_folder_too_large_to_look_through_refuses_the_door(self, tmp_path, monkeypatch):
        """Bug class: a scan that gives up on a large tree and lets the door open, or one without a bound that holds the scheduler's start."""
        monkeypatch.setattr(door_module, "RESULTS_SCAN_ENTRIES", 5)
        for index in range(6):
            (tmp_path / f"folder{index}").mkdir()
        with pytest.raises(door_module._Closed) as closed:
            door_module._found_below(tmp_path, (0, 0))
        assert closed.value.text == door_module.CLOSED_RESULTS_UNCHECKED

    def test_many_finished_job_folders_do_not_close_the_door(self, rig):
        """Bug class: every job folder counted towards the bound of the scan below the results folder, so that a deployment with about 10,000 finished jobs gets "cannot be looked at" at every start."""
        results = rig.store.results
        for index in range(20000):
            os.close(os.open(results / f"{('pg', 'sr', 'mg', 'job')[index % 4]}_{index:08x}", os.O_WRONLY | os.O_CREAT, 0o600))
        for name in ("a", "b"):
            (results / name).mkdir()
        (results / "notes.txt").write_text("x")
        assert rig.open() is not None
        assert rig.result({"op": "hello"}) == {"protocol": 1}

    @pytest.mark.parametrize("mode, opens", [(0o757, False), (0o777, False), (0o775, True), (0o755, True)])
    def test_a_data_folder_others_may_write_refuses_the_door(self, rig, caplog, mode, opens):
        """Bug class: a door opened in a data folder that anyone may write into, where anyone can rename the control folder away and put their own in its place; or a group-writable folder (a private group is the common default) refused."""
        caplog.set_level(logging.INFO, logger=LOG_NAME)
        os.chmod(rig.jobs_dir, mode)
        try:
            opened = rig.open()
        finally:
            os.chmod(rig.jobs_dir, 0o755)
        assert (opened is not None) == opens
        if not opens:
            assert door_lines(caplog) == [door_module.CLOSED_OTHERS_WRITE]
            assert not rig.control.exists()

    def test_every_reason_has_its_own_line(self):
        """Bug class: two reasons for a closed door logged with the same words, so the operator cannot tell which to fix."""
        texts = [value for name, value in vars(door_module).items() if name.startswith("CLOSED_")]
        assert len(texts) == len(set(texts)) == 14
        assert all(text.startswith("the control socket stays closed: ") for text in texts)


# ── b. peers ────────────────────────────────────────────────────────────────


class TestPeers:
    def test_the_kernel_reader_names_this_process_uid(self):
        """Bug class: the peer's uid read from the wrong field of SO_PEERCRED (the pid or the gid)."""
        first, second = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            assert door_module._peer_uid(first) == os.getuid()
        finally:
            first.close()
            second.close()

    def test_another_uid_gets_no_reply_and_reaches_nothing(self, opened, caplog):
        """Bug class: a connection from another user answered, or let through to the store, before its uid was looked at."""
        caplog.set_level(logging.INFO, logger=LOG_NAME)
        opened.submit()
        opened.store.calls.clear()
        opened.peer = os.getuid() + 1
        for document in ({"op": "hello"}, {"op": "jobs", "phase": "pending", "limit": 5, "offset": 0}, {"op": "approve", "job_id": JOB_ID, "digest": "0" * 64, "by": "web-1"}):
            assert opened.ask(document) is None
        assert opened.store.calls == []
        assert any("another user" in line and "(1 so far)" in line for line in door_lines(caplog))
        opened.peer = os.getuid()
        assert opened.result({"op": "hello"}) == {"protocol": 1}

    def test_the_seventeenth_connection_at_once_is_closed(self, rig):
        """Bug class: no bound on the connections at a time, so a client that opens many silent connections takes a thread each without end."""
        rig.open(limits=door_module.Limits(request_seconds=20))
        held = [rig.connect() for _ in range(16)]
        try:
            wait_for("sixteen handlers", lambda: rig.door.connection_count() == 16)
            extra = rig.connect(timeout=5)
            started = time.monotonic()
            with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                extra.sendall(framing.encode({"op": "hello"}, max_bytes=1024))
            assert read_reply(extra) is None
            assert time.monotonic() - started < 4
            extra.close()
        finally:
            for connection in held:
                connection.close()
        wait_for("the handlers to end", lambda: rig.door.connection_count() == 0)
        assert rig.result({"op": "hello"}) == {"protocol": 1}


# ── c. every op: its shape ──────────────────────────────────────────────────


VALID = {
    "hello": {},
    "counts": {},
    "stats": {},
    "targets": {},
    "jobs": {"phase": "pending", "limit": 10, "offset": 0},
    "job": {"job_id": JOB_ID},
    "approve": {"job_id": JOB_ID, "digest": "0" * 64, "by": "web-1"},
    "reject": {"job_id": JOB_ID, "reason": "", "by": "web-1"},
    "kill": {"job_id": JOB_ID, "by": "web-1"},
    "explain": {"job_id": JOB_ID, "by": "web-1"},
    "preview": {"job_id": JOB_ID},
    "auto_accept": {},
    "auto_accept.set": {"target": "warehouse", "enabled": True, "limit": 2, "by": "web-1"},
}

WRONG_TYPES = [
    ("jobs", "phase", "done"), ("jobs", "phase", 1), ("jobs", "limit", 0), ("jobs", "limit", 501), ("jobs", "limit", True), ("jobs", "limit", 2.0), ("jobs", "limit", "5"), ("jobs", "offset", -1), ("jobs", "offset", 100001), ("jobs", "offset", True), ("jobs", "offset", None), ("jobs", "offset", 1.0),
    ("approve", "digest", "0" * 63), ("approve", "digest", "A" * 64), ("approve", "digest", 5), ("approve", "by", "web-"), ("approve", "by", "web-1234567890123"), ("approve", "by", "web-1\n"), ("approve", "by", "admin"), ("approve", "by", "web-\u0661"),
    ("reject", "reason", None), ("reject", "reason", 5), ("reject", "by", None),
    ("kill", "by", 7),
    ("explain", "by", ""),
    ("auto_accept.set", "target", 5), ("auto_accept.set", "enabled", "yes"), ("auto_accept.set", "enabled", 1), ("auto_accept.set", "limit", True), ("auto_accept.set", "limit", 1.5), ("auto_accept.set", "limit", "2"),
]

BAD_IDS = ["../x", "PG_0123ABCD", "pg_0123ABCD", "pg_0123abcd0", "pg_0123abcd\n", "pg_0123abc", "xx_0123abcd", "pg_0123abcd/../..", 5, None, ["pg_0123abcd"]]


class TestShapes:
    def test_every_op_answers_with_its_shape(self, opened):
        """Bug class: an op that answers with a shape the web UI does not expect, or not at all."""
        assert set(VALID) == set(door_module.OPS)
        opened.submit()
        assert opened.result({"op": "hello"}) == {"protocol": 1}
        assert opened.result({"op": "counts"}) == {"pending": 1, "approved": 0, "running": 0}
        assert opened.result({"op": "targets"}) == {"targets": [{"name": "warehouse", "backend": "postgres", "database": "datasets"}, {"name": "olap", "backend": "starrocks", "database": ""}]}
        listed = opened.result({"op": "jobs", "phase": "pending", "limit": 10, "offset": 0})
        assert set(listed) == {"phase", "jobs", "more"} and listed["jobs"][0]["job_id"] == JOB_ID
        whole = opened.result({"op": "job", "job_id": JOB_ID})
        assert set(whole) == {"phase", "job", "digest"} and whole["phase"] == "pending"
        assert opened.result({"op": "explain", "job_id": JOB_ID, "by": "web-1"}) == {"plan": "Seq Scan on t"}
        assert opened.result({"op": "kill", "job_id": JOB_ID, "by": "web-1"}) == {"was_running": True}
        panel = opened.result({"op": "auto_accept"})
        assert panel == {"max_limit": 3, "targets": [{"name": "warehouse", "backend": "postgres", "database": "datasets", "enabled": False, "limit": 3}, {"name": "olap", "backend": "starrocks", "database": "", "enabled": False, "limit": 3}]}
        changed = opened.result({"op": "auto_accept.set", "target": "olap", "enabled": True, "limit": 9, "by": "web-1"})
        assert changed["targets"][1] == {"name": "olap", "backend": "starrocks", "database": "", "enabled": True, "limit": 3}
        assert opened.code({"op": "preview", "job_id": JOB_ID}) == "not_done"
        assert opened.result({"op": "reject", "job_id": JOB_ID, "reason": "", "by": "web-1"}) == {}
        assert opened.code({"op": "approve", "job_id": JOB_ID, "digest": "0" * 64, "by": "web-1"}) == "not_pending"

    @pytest.mark.parametrize("document", [{"op": "nope"}, {"op": 5}, {"op": None}, {}, {"op": "auto_accept.get"}, {"op": "HELLO"}])
    def test_an_unknown_op_is_refused(self, opened, document):
        """Bug class: an op the door does not list reaching a method of its own by name."""
        assert opened.code(document) == "bad_request"

    @pytest.mark.parametrize("op", sorted(VALID))
    def test_an_unknown_and_a_missing_field_are_refused(self, opened, op):
        """Bug class: a field the door does not list taken (deny by default), or one that is missing filled in by a default."""
        opened.submit()
        opened.store.calls.clear()
        assert opened.code({"op": op, **VALID[op], "extra": 1}) == "bad_request"
        for name in VALID[op]:
            fields = {key: value for key, value in VALID[op].items() if key != name}
            assert opened.code({"op": op, **fields}) == "bad_request"
        assert opened.store.calls == []
        assert opened.runner.cancels == [] and opened.runner.explains == [] and opened.auto_accept.sets == []

    @pytest.mark.parametrize("op, name, value", WRONG_TYPES)
    def test_a_field_of_the_wrong_type_or_form_is_refused(self, opened, op, name, value):
        """Bug class: a field taken with a type or form the door does not check (a boolean for a number, a label with a line break, digits of another script)."""
        opened.submit()
        opened.store.calls.clear()
        assert opened.code({"op": op, **{**VALID[op], name: value}}) == "bad_request"
        assert opened.store.calls == []
        assert opened.runner.cancels == [] and opened.runner.explains == [] and opened.auto_accept.sets == []

    def test_an_unknown_target_is_refused(self, opened):
        """Bug class: auto-accept switched on for a name that is no configured target (a state file entry nobody sees)."""
        assert opened.code({"op": "auto_accept.set", **{**VALID["auto_accept.set"], "target": "ghost"}}) == "unknown_target"
        assert opened.auto_accept.sets == []

    @pytest.mark.parametrize("op", ["job", "approve", "reject", "kill", "explain", "preview"])
    @pytest.mark.parametrize("bad", BAD_IDS, ids=repr)
    def test_a_malformed_job_id_never_reaches_the_store_the_runner_or_a_path(self, opened, op, bad):
        """Bug class: a job id joined into a path or handed to the store unchecked (`../x` names a file outside the phase folder; a trailing newline passes `re.match` with `$`)."""
        opened.store.calls.clear()
        assert opened.code({"op": op, **{**VALID[op], "job_id": bad}}) == "bad_request"
        assert opened.store.calls == []
        assert opened.runner.cancels == [] and opened.runner.explains == []


# ── d. jobs and job ─────────────────────────────────────────────────────────


class TestLists:
    def test_the_short_view_cuts_four_texts_and_gives_their_lengths(self, opened):
        """Bug class: a list reply that carries whole SQL texts (a few large jobs make it too large to send), or cuts without saying how long the text was; cut by bytes instead of characters."""
        long = "\u00e9" * 2500
        opened.submit(sql=long, description="d" * 2001, error="e" * 10, reject_reason=None, target="t" * 300)
        view = opened.result({"op": "jobs", "phase": "pending", "limit": 1, "offset": 0})["jobs"][0]
        assert view["sql"] == "\u00e9" * 2000 and view["sql_length"] == 2500
        assert view["description"] == "d" * 2000 and view["description_length"] == 2001
        assert view["error"] == "e" * 10 and view["error_length"] == 10
        assert view["reject_reason"] is None and view["reject_reason_length"] == 0
        assert view["target"] == "t" * 256
        assert set(view) == {"job_id", "target", "backend", "status", "submitted_at", "approved_at", "started_at", "finished_at", "rows", "size_bytes", "output_filename", "overwrite", "sql", "sql_length", "description", "description_length", "error", "error_length", "reject_reason", "reject_reason_length"}

    def test_the_order_and_the_limit_are_the_page_s(self, opened):
        """Bug class: a queue shown in another order than the one the runner and auto-accept follow (submission order), or the history oldest first; a limit not applied, or `more` wrong."""
        ids = [f"pg_0000000{index}" for index in range(4)]
        for index, job_id in enumerate(ids):
            opened.submit(job_id)
            stamp = 1700000000 + (10 - index if index % 2 else index)
            os.utime(opened.store.pending / f"{job_id}.json", (stamp, stamp))
        page_order = [job.job_id for job in Store.list_phase(opened.store, "pending")]
        listed = opened.result({"op": "jobs", "phase": "pending", "limit": 3, "offset": 0})
        assert [job["job_id"] for job in listed["jobs"]] == page_order[:3] and listed["more"] is True
        assert page_order == ["pg_00000000", "pg_00000002", "pg_00000003", "pg_00000001"]
        everything = opened.result({"op": "jobs", "phase": "pending", "limit": 4, "offset": 0})
        assert len(everything["jobs"]) == 4 and everything["more"] is False
        for job_id in ["sr_00000001", "sr_00000002", "sr_00000003"]:
            opened.finished(job_id)
        history = opened.result({"op": "jobs", "phase": "history", "limit": 2, "offset": 0})
        assert [job["job_id"] for job in history["jobs"]] == ["sr_00000003", "sr_00000002"] and history["more"] is True
        assert opened.result({"op": "jobs", "phase": "approved", "limit": 5, "offset": 0}) == {"phase": "approved", "jobs": [], "more": False}

    def test_paging(self, opened):
        """Bug class: no way to reach jobs beyond the first page of a long queue or history, an offset applied twice or not at all, or `more` wrong at the end."""
        ids = [f"pg_0000000{index}" for index in range(5)]
        for index, job_id in enumerate(ids):
            opened.submit(job_id)
            os.utime(opened.store.pending / f"{job_id}.json", (1700000000 + index, 1700000000 + index))
        pages = [opened.result({"op": "jobs", "phase": "pending", "limit": 2, "offset": offset}) for offset in (0, 2, 4, 10)]
        assert [[job["job_id"] for job in page["jobs"]] for page in pages] == [ids[0:2], ids[2:4], ids[4:5], []]
        assert [page["more"] for page in pages] == [True, True, False, False]
        for job_id in ["sr_00000001", "sr_00000002", "sr_00000003"]:
            opened.finished(job_id)
        history = [opened.result({"op": "jobs", "phase": "history", "limit": 2, "offset": offset}) for offset in (0, 2)]
        assert [[job["job_id"] for job in page["jobs"]] for page in history] == [["sr_00000003", "sr_00000002"], ["sr_00000001"]]
        assert [page["more"] for page in history] == [True, False]
        assert opened.result({"op": "jobs", "phase": "pending", "limit": 1, "offset": 100000}) == {"phase": "pending", "jobs": [], "more": False}

    @pytest.mark.parametrize("where", ["view", "record"])
    def test_numbers_that_are_not_finite_do_not_break_a_list(self, opened, where):
        """Bug class: one hand-edited record with `NaN`, `Infinity` or a number too large for a float making the reply unwritable, so the whole tab answers `internal`."""
        opened.submit(OTHER_ID)
        text = json.dumps(make_job().to_dict()).replace('"rows": null', '"rows": NaN').replace('"size_bytes": null', '"size_bytes": Infinity').replace('"approved_at": null', '"approved_at": -Infinity').replace('"started_at": null', '"started_at": 1e400').replace('"extras": {}', '"extras": {"x": [NaN, {"y": Infinity}]}')
        assert text.count("NaN") == 2 and "1e400" in text
        (opened.store.pending / f"{JOB_ID}.json").write_text(text)
        if where == "view":
            listed = opened.result({"op": "jobs", "phase": "pending", "limit": 10, "offset": 0})
            mine = [job for job in listed["jobs"] if job["job_id"] == JOB_ID][0]
            assert (mine["rows"], mine["size_bytes"], mine["approved_at"], mine["started_at"]) == (None, None, None, None)
            assert len(listed["jobs"]) == 2
        else:
            found = opened.result({"op": "job", "job_id": JOB_ID})["job"]
            assert (found["rows"], found["size_bytes"], found["approved_at"], found["started_at"]) == (None, None, None, None)
            assert found["extras"] == {"x": [None, {"y": None}]}

    def test_job_returns_the_whole_record_and_its_phase(self, opened):
        """Bug class: the detail view missing a field the approver must see before approving (the collection, the database, overwrite) or who acted on it."""
        opened.submit(sql="SELECT " + "x" * 5000, collection="posts", database="reddit", overwrite=True, description="why")
        found = opened.result({"op": "job", "job_id": JOB_ID})
        assert found["job"] == {**make_job(sql="SELECT " + "x" * 5000, collection="posts", database="reddit", overwrite=True, description="why").to_dict(), "acted_by": {}}
        assert found["phase"] == "pending"

    def test_a_record_over_one_mebibyte_is_too_large(self, opened):
        """Bug class: an oversized record (a store file anyone with access to the folder can write) sent whole."""
        opened.submit(sql="x" * (1024 * 1024))
        assert opened.code({"op": "job", "job_id": JOB_ID}) == "too_large"
        opened.submit(OTHER_ID, sql="x" * (1024 * 1024 - 1000))
        assert opened.result({"op": "job", "job_id": OTHER_ID})["phase"] == "pending"

    def test_an_unknown_job_is_not_found(self, opened):
        """Bug class: a missing job answered as an internal error or as some other job."""
        assert opened.code({"op": "job", "job_id": OTHER_ID}) == "not_found"

    def test_the_digest_is_the_documented_canonical_form(self, opened):
        """Bug class: a digest the web UI cannot compute or check, or one that changes with the order of keys or the platform's JSON."""
        job = make_job(sql="SELECT \u00e9", collection=None, database="db")
        canonical = json.dumps({"target": "warehouse", "backend": "postgres", "sql": "SELECT \u00e9", "output_filename": "out.csv", "overwrite": False, "collection": None, "database": "db"}, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        assert canonical == '{"backend":"postgres","collection":null,"database":"db","output_filename":"out.csv","overwrite":false,"sql":"SELECT \\u00e9","target":"warehouse"}'
        assert door_module.digest(job) == "7aed5377661e362718110056bc7e8a3121470638c07f1d56a7fbe9e58add3941"
        opened.store.submit(job)
        assert opened.result({"op": "job", "job_id": JOB_ID})["digest"] == door_module.digest(job)

    @pytest.mark.parametrize("name, value", [("target", "olap"), ("backend", "starrocks"), ("sql", "SELECT 2"), ("output_filename", "other.csv"), ("overwrite", True), ("collection", "c"), ("database", "other")])
    def test_the_digest_changes_with_each_of_the_seven_fields(self, name, value):
        """Bug class: a field that decides what runs (or where, or which file it may overwrite) left out of the digest, so a change of it passes the approval's check."""
        assert door_module.digest(make_job()) != door_module.digest(make_job(**{name: value}))

    @pytest.mark.parametrize("name, value", [("description", "other"), ("submitted_at", 1.0), ("status", "approved"), ("error", "x"), ("rows", 5), ("extras", {"a": 1})])
    def test_the_digest_does_not_change_with_other_fields(self, name, value):
        """Bug class: a digest over the whole record, which every transition changes, so an approval of an unchanged query is refused."""
        assert door_module.digest(make_job()) == door_module.digest(make_job(**{name: value}))


# ── e. approve; the loop's thread ───────────────────────────────────────────


class TestApprove:
    def _digest(self, rig, job_id=JOB_ID):
        return rig.result({"op": "job", "job_id": job_id})["digest"]

    def test_the_right_digest_approves_once_and_records_who(self, opened):
        """Bug class: an approval through the door that does not reach the queue, or that leaves no trace of who gave it."""
        opened.submit()
        digest = self._digest(opened)
        assert opened.result({"op": "approve", "job_id": JOB_ID, "digest": digest, "by": "web-42"}) == {}
        record = json.loads((opened.store.approved / f"{JOB_ID}.json").read_text())
        assert record["status"] == "approved" and record["acted_by"] == {"approved": "web-42"}
        assert opened.store_calls().count("approve") == 1
        assert opened.code({"op": "approve", "job_id": JOB_ID, "digest": digest, "by": "web-42"}) == "not_pending"

    def test_a_wrong_digest_is_changed_and_approves_nothing(self, opened):
        """Bug class: the digest not compared (or compared with something the request controls), so an approval names one query and runs another."""
        opened.submit()
        assert opened.code({"op": "approve", "job_id": JOB_ID, "digest": "0" * 64, "by": "web-1"}) == "changed"
        assert "approve" not in opened.store_calls()
        assert (opened.store.pending / f"{JOB_ID}.json").exists()

    def test_a_record_rewritten_between_reading_and_approving_is_changed(self, opened):
        """Bug class: the digest checked against what the client saw earlier instead of the record as it is at the moment of the approval."""
        opened.submit()
        digest = self._digest(opened)
        path = opened.store.pending / f"{JOB_ID}.json"
        record = json.loads(path.read_text())
        record["sql"] = "DROP TABLE everything"
        path.write_text(json.dumps(record))
        assert opened.code({"op": "approve", "job_id": JOB_ID, "digest": digest, "by": "web-1"}) == "changed"
        assert "approve" not in opened.store_calls()

    @pytest.mark.parametrize("phase", ["approved", "history"])
    def test_a_job_that_is_no_longer_pending_is_not_pending_with_its_phase(self, opened, phase):
        """Bug class: a job approved twice, or a rejected one brought back; the refusal not saying where the job is."""
        opened.submit()
        digest = self._digest(opened)
        if phase == "approved":
            Store.approve(opened.store, JOB_ID)
        else:
            Store.reject(opened.store, JOB_ID, "no")
        reply = opened.ask({"op": "approve", "job_id": JOB_ID, "digest": digest, "by": "web-1"})
        assert reply["error"]["code"] == "not_pending" and reply["error"]["phase"] == phase
        assert opened.code({"op": "reject", "job_id": JOB_ID, "reason": "", "by": "web-1"}) == "not_pending"

    def test_two_approvals_at_once_approve_once(self, opened):
        """Bug class: two approvals that meet both moving the job (the store's transitions take no lock), so the second one fails half-way or the job is claimed twice."""
        opened.submit()
        digest = self._digest(opened)
        gate = threading.Event()
        replies = []

        def approve():
            gate.wait(5)
            replies.append(opened.ask({"op": "approve", "job_id": JOB_ID, "digest": digest, "by": "web-1"}))

        threads = [threading.Thread(target=approve) for _ in range(2)]
        for thread in threads:
            thread.start()
        gate.set()
        for thread in threads:
            thread.join(15)
        outcomes = sorted("ok" if reply["ok"] else reply["error"]["code"] for reply in replies)
        assert outcomes == ["not_pending", "ok"]
        assert opened.store_calls().count("approve") == 1

    def test_every_store_runner_and_auto_accept_call_runs_on_the_loop_thread(self, opened):
        """Bug class: a store or runner call made on the door's own thread, where it can meet the page's routes (which run on the loop) in the middle of a transition."""
        opened.submit()
        opened.submit(OTHER_ID)
        opened.finished("mg_00000001")
        digest = self._digest(opened)
        opened.result({"op": "approve", "job_id": JOB_ID, "digest": digest, "by": "web-1"})
        opened.result({"op": "reject", "job_id": OTHER_ID, "reason": "no", "by": "web-1"})
        opened.result({"op": "kill", "job_id": JOB_ID, "by": "web-1"})
        opened.result({"op": "auto_accept.set", "target": "olap", "enabled": True, "limit": 1, "by": "web-1"})
        for phase in ("pending", "history"):
            opened.result({"op": "jobs", "phase": phase, "limit": 5, "offset": 0})
        opened.ask({"op": "preview", "job_id": "mg_00000001"})
        names = set(opened.store_calls())
        assert {"find", "approve", "reject", "list_phase", "iter_history", "job_result_dir"} <= names
        assert {ident for _name, ident in opened.store.calls} == {opened.loop_thread}
        assert [ident for _job, _by, ident in opened.runner.cancels] == [opened.loop_thread]
        assert [entry[3] for entry in opened.auto_accept.sets] == [opened.loop_thread]

    @pytest.mark.parametrize("op", ["approve", "reject"])
    def test_a_record_that_names_another_job_is_changed(self, opened, op):
        """Bug class: a pending file whose record names another job id approved or rejected: the store builds the paths from the record's id, so the transition would write and move a file of the other job and leave this one behind."""
        record = make_job(OTHER_ID).to_dict()
        (opened.store.pending / f"{JOB_ID}.json").write_text(json.dumps(record))
        digest = opened.result({"op": "job", "job_id": JOB_ID})["digest"]
        document = {"op": "approve", "job_id": JOB_ID, "digest": digest, "by": "web-1"} if op == "approve" else {"op": "reject", "job_id": JOB_ID, "reason": "", "by": "web-1"}
        assert opened.code(document) == "changed"
        assert sorted(os.listdir(opened.store.pending)) == [f"{JOB_ID}.json"]
        assert os.listdir(opened.store.approved) == [] and Store.iter_history(opened.store) == []

    def test_one_loop_call_at_a_time_and_busy_within_the_deadline(self, rig):
        """Bug class: every connection handing its own store walk to the loop at once (sixteen queued behind a busy loop), instead of one at a time with the others answered `busy` when their own deadline runs out."""
        rig.open(limits=door_module.Limits(request_seconds=0.5, loop_seconds=5))
        release = threading.Event()
        rig.loop.call_soon_threadsafe(lambda: release.wait(15))
        first = []
        thread = threading.Thread(target=lambda: first.append(rig.ask({"op": "jobs", "phase": "pending", "limit": 5, "offset": 0}, timeout=20)))
        thread.start()
        try:
            wait_for("the first call to hold its turn", rig.door._loop_lock.locked)
            started = time.monotonic()
            second = rig.ask({"op": "jobs", "phase": "pending", "limit": 5, "offset": 0}, timeout=20)
            waited = time.monotonic() - started
        finally:
            release.set()
        thread.join(20)
        assert second["error"] == {"code": "busy", "message": door_module.MESSAGES["busy"]} and waited < 3
        assert first[0]["ok"] is True
        time.sleep(0.2)
        assert rig.store_calls().count("list_phase") == 1

    def test_a_loop_that_does_not_take_the_call_up_is_answered_timeout_and_nothing_is_done(self, rig):
        """Bug class: a call handed to a loop that is busy (or stopped) waited for without end, or answered while it may still run later."""
        rig.open(limits=door_module.Limits(loop_seconds=0.3))
        rig.submit()
        digest = rig.result({"op": "job", "job_id": JOB_ID})["digest"]
        release = threading.Event()
        rig.loop.call_soon_threadsafe(lambda: release.wait(10))
        try:
            assert rig.code({"op": "approve", "job_id": JOB_ID, "digest": digest, "by": "web-1"}) == "timeout"
        finally:
            release.set()
        time.sleep(0.3)
        assert (rig.store.pending / f"{JOB_ID}.json").exists()
        assert "approve" not in rig.store_calls()


# ── f. reject and kill ──────────────────────────────────────────────────────


class TestRejectAndKill:
    @pytest.mark.parametrize("reason", ["a\nb", "a\rb", "a\x85b", "a\u2028b", "a\u2029b", "a\x00b", "a\tb", "a\x1bb", "x" * 501])
    def test_a_reason_with_a_line_break_a_control_character_or_too_long_is_refused(self, opened, reason):
        """Bug class: a reason that splits a log line or the history file's line, or that has no bound."""
        opened.submit()
        opened.store.calls.clear()
        assert opened.code({"op": "reject", "job_id": JOB_ID, "reason": reason, "by": "web-1"}) == "bad_request"
        assert opened.store.calls == []

    @pytest.mark.parametrize("reason, stored", [("", None), ("  ", None), ("x" * 500, "x" * 500), ("not now \u00e9", "not now \u00e9")])
    def test_a_reject_records_its_reason_and_who(self, opened, reason, stored):
        """Bug class: a reject through the door that loses its reason or who gave it, or keeps an empty reason as one."""
        opened.submit()
        assert opened.result({"op": "reject", "job_id": JOB_ID, "reason": reason, "by": "web-9"}) == {}
        record = Store.iter_history(opened.store)[0]
        assert (record.status, record.reject_reason, record.acted_by) == ("rejected", stored, {"rejected": "web-9"})

    def test_kill_calls_the_runner_once_with_who(self, opened):
        """Bug class: a kill that does not reach the runner, reaches it twice, or loses who asked."""
        assert opened.result({"op": "kill", "job_id": JOB_ID, "by": "web-5"}) == {"was_running": True}
        opened.runner.was_running = False
        assert opened.result({"op": "kill", "job_id": OTHER_ID, "by": "web-6"}) == {"was_running": False}
        assert [(job_id, by) for job_id, by, _ident in opened.runner.cancels] == [(JOB_ID, "web-5"), (OTHER_ID, "web-6")]


# ── g. explain ──────────────────────────────────────────────────────────────


class TestExplain:
    def test_the_plan_is_data_and_who_asked_is_logged(self, opened, caplog):
        """Bug class: the plan taken as something else than text, or an EXPLAIN (sent on an admin connection) that leaves no line saying who asked."""
        caplog.set_level(logging.INFO, logger=LOG_NAME)
        opened.runner.plan = '{"Plan": "<b>x</b>"}\n  -> Seq Scan'
        assert opened.result({"op": "explain", "job_id": JOB_ID, "by": "web-77"}) == {"plan": '{"Plan": "<b>x</b>"}\n  -> Seq Scan'}
        assert any(JOB_ID in line and "web-77" in line and "explain" in line and "asked" in line for line in door_lines(caplog))

    def test_a_long_plan_is_cut(self, opened):
        """Bug class: a plan without a bound that makes the reply too large to send."""
        opened.runner.plan = "\u00e9" * (300 * 1024)
        plan = opened.result({"op": "explain", "job_id": JOB_ID, "by": "web-1"})["plan"]
        assert len(plan.encode("utf-8")) == door_module.PLAN_MAX_BYTES

    def test_the_database_error_is_explain_failed_with_its_text_cut(self, opened):
        """Bug class: the database's message lost (the approver needs it), or sent without a bound."""
        opened.runner.explain_error = RuntimeError("syntax error at or near FROM " + "y" * 10000)
        reply = opened.ask({"op": "explain", "job_id": JOB_ID, "by": "web-1"})
        assert reply["error"]["code"] == "explain_failed"
        assert reply["error"]["message"] == door_module.MESSAGES["explain_failed"]
        assert reply["error"]["detail"].startswith("RuntimeError: syntax error at or near FROM")
        assert len(reply["error"]["detail"]) == door_module.EXPLAIN_ERROR_CUT

    def test_an_unknown_job_is_not_found(self, opened):
        """Bug class: a missing job's KeyError shown as a database error."""
        opened.runner.explain_error = KeyError(OTHER_ID)
        assert opened.code({"op": "explain", "job_id": OTHER_ID, "by": "web-1"}) == "not_found"

    def test_the_time_limit_and_the_number_at_once(self, rig):
        """Bug class: an EXPLAIN that the database never answers holding the connection without end, or many of them each taking a thread."""
        rig.open(limits=door_module.Limits(explain_seconds=0.3, explains_max=1))
        rig.runner.explain_gate = threading.Event()
        try:
            started = time.monotonic()
            assert rig.code({"op": "explain", "job_id": JOB_ID, "by": "web-1"}) == "explain_timeout"
            assert time.monotonic() - started < 3
            # The first is still waiting on the database: it holds the only slot.
            assert rig.code({"op": "explain", "job_id": JOB_ID, "by": "web-1"}) == "busy"
        finally:
            rig.runner.explain_gate.set()
        wait_for("the slot to be given back", lambda: rig.ask({"op": "explain", "job_id": JOB_ID, "by": "web-1"})["ok"])

    def test_the_timeout_text_says_the_statement_may_still_run(self):
        """Bug class: a timed-out EXPLAIN reported as if nothing happened on the database, while the worker goes on and the statement may still be running (or executing) there."""
        assert "may still be running" in door_module.MESSAGES["explain_timeout"]

    def test_explain_runs_off_the_loop(self, opened):
        """Bug class: a blocking EXPLAIN run on the scheduler's loop, which stops the page and the MCP endpoint for as long as the database takes."""
        opened.result({"op": "explain", "job_id": JOB_ID, "by": "web-1"})
        assert opened.runner.explains[0][1] != opened.loop_thread


# ── h. preview ──────────────────────────────────────────────────────────────


def _no_marker(reply):
    assert MARKER not in json.dumps(reply)


class TestPreview:
    def _folder(self, rig, job_id=JOB_ID):
        folder = rig.store.results / job_id
        folder.mkdir(parents=True)
        return folder

    def test_a_done_csv_is_previewed_within_the_bounds(self, opened):
        """Bug class: a preview without bounds on rows, columns, cell length or bytes read (a result can be gigabytes)."""
        opened.finished()
        folder = self._folder(opened)
        header = ",".join(f"c{index}" for index in range(300))
        wide = ",".join("v" * 600 for _ in range(300))
        row = ",".join("s" for _ in range(300))
        (folder / "out.csv").write_text(header + "\n" + wide + "\n" + "\n".join([row] * 99) + "\n")
        found = opened.result({"op": "preview", "job_id": JOB_ID})
        assert found["format"] == "csv" and len(found["columns"]) == 200 and found["columns"][0] == "c0"
        assert len(found["rows"]) == 50 and all(len(cells) == 200 for cells in found["rows"])
        assert found["rows"][0][0] == "v" * 500
        assert found["parts"] == [{"name": "out.csv", "size": (folder / "out.csv").stat().st_size}]

    def test_at_most_one_mebibyte_is_read(self, opened):
        """Bug class: a whole result file read into memory for a preview of 50 rows."""
        opened.finished()
        folder = self._folder(opened)
        line = "a,b\n"
        (folder / "out.csv").write_text("x,y\n" + line * (3 * 1024 * 1024 // len(line)))
        found = opened.result({"op": "preview", "job_id": JOB_ID})
        assert found["read_bytes"] == door_module.PREVIEW_READ_MAX and found["truncated"] is True
        assert len(found["rows"]) == 50

    def test_numbered_parts_are_found_in_order(self, opened):
        """Bug class: a StarRocks result (`<stem>_<n><ext>` parts) not found, or its parts taken in the order of the folder listing (part 10 before part 2)."""
        opened.finished()
        folder = self._folder(opened)
        for number in (10, 2, 0, 1):
            (folder / f"out_{number}.csv").write_text(f"n\n{number}\n")
        (folder / "other_0.csv").write_text("not\nmine\n")
        (folder / "out_x.csv").write_text("not\nmine\n")
        found = opened.result({"op": "preview", "job_id": JOB_ID})
        assert [part["name"] for part in found["parts"]] == ["out_0.csv", "out_1.csv", "out_2.csv", "out_10.csv"]
        assert found["rows"] == [["0"]]

    def test_a_link_named_like_a_part_is_not_followed(self, opened, tmp_path):
        """Bug class: a link planted in a result folder (the database servers can write there) followed to a file the scheduler may read as root."""
        opened.finished()
        folder = self._folder(opened)
        secret = tmp_path / "secret.csv"
        secret.write_text(f"k\n{MARKER}\n")
        os.symlink(secret, folder / "out_0.csv")
        (folder / "out_1.csv").write_text("k\nfine\n")
        found = opened.result({"op": "preview", "job_id": JOB_ID})
        assert [part["name"] for part in found["parts"]] == ["out_1.csv"] and found["rows"] == [["fine"]]
        _no_marker(found)
        os.unlink(folder / "out_1.csv")
        reply = opened.ask({"op": "preview", "job_id": JOB_ID})
        assert reply["error"]["code"] == "preview_failed"
        _no_marker(reply)

    def test_a_fifo_named_like_a_part_is_skipped_without_waiting(self, opened):
        """Bug class: a named pipe in a part's place opened for reading, which waits for a writer forever."""
        opened.finished()
        folder = self._folder(opened)
        os.mkfifo(folder / "out.csv")
        (folder / "out_0.csv").write_text("k\nfine\n")
        started = time.monotonic()
        found = opened.result({"op": "preview", "job_id": JOB_ID})
        assert time.monotonic() - started < 3
        assert [part["name"] for part in found["parts"]] == ["out_0.csv"]

    def test_the_record_s_result_path_is_ignored(self, opened, tmp_path):
        """Bug class: the folder taken from the record's `result_path` (which anyone who can write the store's files controls) instead of from the checked job id."""
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()
        (elsewhere / "out.csv").write_text(f"k\n{MARKER}\n")
        opened.finished(result_path=str(elsewhere))
        reply = opened.ask({"op": "preview", "job_id": JOB_ID})
        assert reply["error"]["code"] == "preview_failed"
        _no_marker(reply)

    def test_a_folder_that_is_a_link_is_refused(self, opened, tmp_path):
        """Bug class: a job's result folder replaced by a link to another folder and read through it."""
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()
        (elsewhere / "out.csv").write_text(f"k\n{MARKER}\n")
        opened.finished()
        os.symlink(elsewhere, opened.store.results / JOB_ID)
        reply = opened.ask({"op": "preview", "job_id": JOB_ID})
        assert reply["error"]["code"] == "preview_failed"
        _no_marker(reply)

    @pytest.mark.parametrize("state", ["pending", "failed", "missing"])
    def test_only_a_done_job_is_previewed(self, opened, state):
        """Bug class: a preview of a job that has not finished (a part being written) or that failed (a partial file)."""
        if state == "pending":
            opened.submit()
        elif state == "failed":
            opened.finished(status="failed")
        if state != "missing":
            folder = self._folder(opened)
            (folder / "out.csv").write_text(f"k\n{MARKER}\n")
        reply = opened.ask({"op": "preview", "job_id": JOB_ID})
        assert reply["error"]["code"] == ("not_found" if state == "missing" else "not_done")
        _no_marker(reply)

    def test_a_huge_single_line(self, opened):
        """Bug class: a result without line breaks read or answered without a bound (a single line of a mebibyte and more)."""
        opened.finished()
        folder = self._folder(opened)
        (folder / "out.csv").write_text("a," * (2 * 1024 * 1024))
        found = opened.result({"op": "preview", "job_id": JOB_ID})
        assert len(found["columns"]) == 200 and found["rows"] == [] and found["read_bytes"] == door_module.PREVIEW_READ_MAX
        (folder / "out.csv").write_text(MARKER + "b" * (2 * 1024 * 1024))
        reply = opened.ask({"op": "preview", "job_id": JOB_ID})
        # One field longer than the CSV reader takes: refused, with nothing of the file in the refusal.
        assert reply["error"]["code"] == "preview_failed" and reply["error"]["message"] == door_module.MESSAGES["preview_failed"]
        _no_marker(reply)

    def test_the_listing_of_a_result_folder_is_bounded(self, opened, monkeypatch):
        """Bug class: a result folder with a vast number of entries (the database servers can write there) listed whole for a preview."""
        assert door_module.PREVIEW_LISTING_MAX == 10000
        monkeypatch.setattr(door_module, "PREVIEW_LISTING_MAX", 5)
        opened.finished()
        folder = self._folder(opened)
        (folder / "out.csv").write_text("k\nv\n")
        for index in range(4):
            (folder / f"other{index}").write_text("")
        assert opened.result({"op": "preview", "job_id": JOB_ID})["rows"] == [["v"]]
        (folder / "other4").write_text("")
        assert opened.code({"op": "preview", "job_id": JOB_ID}) == "preview_failed"

    def test_binary_garbage(self, opened):
        """Bug class: bytes that are not text making the reply unwritable (an internal error), or making the preview hang."""
        opened.finished()
        folder = self._folder(opened)
        random = bytes((index * 7919 + 13) % 256 for index in range(300000))
        (folder / "out.csv").write_bytes(random)
        reply = opened.ask({"op": "preview", "job_id": JOB_ID})
        assert reply is not None and (reply["ok"] or reply["error"]["code"] == "preview_failed")
        assert len(json.dumps(reply)) < 2 * 1024 * 1024

    def test_ndjson(self, opened):
        """Bug class: a MongoDB result (NDJSON) not previewed, or a line that is not an object or not JSON stopping the preview."""
        opened.finished(output_filename="out.ndjson", backend="mongodb")
        folder = self._folder(opened)
        lines = ['{"a": 1, "b": {"c": [1, 2]}}', "not json", "[1, 2]", "[" * 100000, '{"a": "x", "d": null}']
        (folder / "out.ndjson").write_text("\n".join(lines) + "\n")
        found = opened.result({"op": "preview", "job_id": JOB_ID})
        assert found["columns"] == ["a", "b", "d"]
        assert found["rows"] == [["1", '{"c": [1, 2]}', ""], ["x", "", ""]]

    def test_parquet_small_and_too_large(self, opened):
        """Bug class: a Parquet file (read from its end, so it cannot be read in a bounded piece) opened whatever its size."""
        pyarrow = pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        opened.finished(output_filename="out.parquet")
        folder = self._folder(opened)
        pq.write_table(pyarrow.table({"n": list(range(100)), "s": ["x" * 600] * 100}), folder / "out.parquet")
        found = opened.result({"op": "preview", "job_id": JOB_ID})
        assert found["format"] == "parquet" and found["columns"] == ["n", "s"] and len(found["rows"]) == 50
        assert found["rows"][1] == ["1", "x" * 500]
        with open(folder / "out.parquet", "ab") as handle:
            handle.write(b"\0" * (2 * 1024 * 1024))
        assert opened.code({"op": "preview", "job_id": JOB_ID}) == "preview_not_available"


# ── h2. stats ───────────────────────────────────────────────────────────────


def running_job(rig, job_id):
    """A job in the running phase, put there by the store's own transitions."""
    Store.submit(rig.store, make_job(job_id))
    Store.approve(rig.store, job_id)
    return rig.store.claim_approved()


class TestStats:
    def test_stats_sums_the_results_and_counts_the_running_jobs(self, opened, tmp_path):
        """Bug class: the Queue bar's figures wrong: a link counted (or followed out of the results folder), a folder below not entered, the slots not the configured cap, the running jobs not counted as `counts` counts them."""
        results = opened.store.results
        (results / JOB_ID).mkdir()
        (results / JOB_ID / "out.csv").write_bytes(b"x" * 1000)
        (results / JOB_ID / "deep").mkdir()
        (results / JOB_ID / "deep" / "out_1.csv").write_bytes(b"y" * 24)
        outside = tmp_path / "outside"
        outside.mkdir()
        (outside / "big").write_bytes(b"z" * 5000)
        (results / "link-to-file").symlink_to(outside / "big")
        (results / "link-to-folder").symlink_to(outside)
        os.mkfifo(results / "fifo")
        running_job(opened, OTHER_ID)
        opened.submit()
        found = opened.result({"op": "stats"})
        assert set(found) == {"results_bytes", "results_files", "results_cut", "free_bytes", "slots", "busy"}
        assert (found["results_bytes"], found["results_files"], found["results_cut"]) == (1024, 2, False)
        assert (found["slots"], found["busy"]) == (3, 1)
        assert isinstance(found["free_bytes"], int) and found["free_bytes"] >= 0

    def test_a_walk_at_its_bounds_is_cut_and_says_so(self, opened, monkeypatch):
        """Bug class: a results folder with many files (or a deep one) costing the scheduler an unbounded walk for a figure of the web UI, or a cut walk given as the whole size."""
        results = opened.store.results
        for number in range(10):
            (results / f"file{number}").write_bytes(b"x")
        monkeypatch.setattr(door_module, "STATS_ENTRIES_MAX", 4)
        found = opened.result({"op": "stats"})
        assert found["results_cut"] is True and found["results_files"] <= 4 and found["results_bytes"] == found["results_files"]
        monkeypatch.setattr(door_module, "STATS_ENTRIES_MAX", 20000)
        assert opened.result({"op": "stats"})["results_cut"] is False
        # Time: a clock past the deadline at the first entry.
        ticks = iter(range(0, 1000, 5))
        assert door_module.results_usage(results, clock=lambda: next(ticks)) == (0, 0, True)
        # Depth: a file below the deepest folder that is entered is not counted.
        monkeypatch.setattr(door_module, "STATS_DEPTH_MAX", 1)
        (results / "a" / "b" / "c").mkdir(parents=True)
        (results / "a" / "b" / "c" / "deep").write_bytes(b"x" * 7)
        assert door_module.results_usage(results) == (10, 10, True)

    def test_a_file_system_error_is_never_an_error_of_the_reply(self, opened, monkeypatch):
        """Bug class: a results folder or file system the scheduler cannot read (a folder of the database servers' own mode, a mount gone away) answered `internal`, which leaves the bar without the slots too, or raised in the door's thread."""
        results = opened.store.results
        (results / "readable").write_bytes(b"x" * 3)
        (results / "closed").mkdir()
        (results / "closed" / "hidden").write_bytes(b"x" * 100)
        os.chmod(results / "closed", 0)
        try:
            found = opened.result({"op": "stats"})
        finally:
            os.chmod(results / "closed", 0o755)
        if os.getuid() != 0:
            assert (found["results_bytes"], found["results_cut"]) == (3, True)

        def refuse(path):
            raise OSError(5, "made-up failure")

        monkeypatch.setattr(door_module.os, "statvfs", refuse)
        found = opened.result({"op": "stats"})
        assert found["free_bytes"] is None and found["results_bytes"] == 103 and found["slots"] == 3
        (results / "readable").unlink()
        os.unlink(results / "closed" / "hidden")
        os.rmdir(results / "closed")
        os.rmdir(results)
        found = opened.result({"op": "stats"})
        assert (found["results_bytes"], found["results_files"], found["results_cut"], found["free_bytes"]) == (None, None, False, None)
        assert found["busy"] == 0


# ── i. bounds ───────────────────────────────────────────────────────────────


class TestBounds:
    def test_an_oversized_request_is_refused_before_it_is_read(self, opened):
        """Bug class: a request of any size read into memory."""
        payload = b"x" * (64 * 1024 + 1)
        assert opened.ask_bytes(struct.pack(">I", len(payload)) + payload)["error"]["code"] == "request_too_large"
        document = json.dumps({"op": "reject", "job_id": JOB_ID, "reason": "x" * 70000, "by": "web-1"}).encode()
        assert opened.ask_bytes(struct.pack(">I", len(document)) + document)["error"]["code"] == "request_too_large"

    def test_a_reply_over_four_mebibytes_is_too_large(self, opened):
        """Bug class: a reply without a bound (500 jobs of long multibyte texts)."""
        text = "\u20ac" * 2000
        for index in range(500):
            opened.submit(f"pg_{index:08x}", sql=text, description=text, error=text, reject_reason=text)
        assert opened.code({"op": "jobs", "phase": "pending", "limit": 500, "offset": 0}) == "too_large"
        assert len(opened.result({"op": "jobs", "phase": "pending", "limit": 50, "offset": 0})["jobs"]) == 50

    def test_a_silent_connection_is_closed_after_the_deadline(self, rig):
        """Bug class: a connection that sends nothing holding its thread and its place without end."""
        rig.open(limits=door_module.Limits(request_seconds=0.3))
        connection = rig.connect(timeout=10)
        started = time.monotonic()
        assert read_reply(connection) is None
        assert time.monotonic() - started < 5
        connection.close()
        wait_for("the handler to end", lambda: rig.door.connection_count() == 0)

    def test_malformed_frames(self, opened):
        """Bug class: a frame that is not JSON, not an object or repeats a key taken, or answered with an internal error."""
        for text in [b"not json", b"[1]", b'{"op": "hello", "op": "hello"}', b"\xff\xfe"]:
            assert opened.ask_bytes(struct.pack(">I", len(text)) + text)["error"]["code"] == "bad_request"

    def test_no_traceback_and_no_exception_text_anywhere(self, opened, caplog, capsys):
        """Bug class: an unexpected exception's text (which can hold a query, a path or a password) or a traceback in a reply or the scheduler's log."""
        caplog.set_level(logging.DEBUG)
        opened.runner.cancel_error = RuntimeError(MARKER)
        opened.submit()
        digest = opened.result({"op": "job", "job_id": JOB_ID})["digest"]
        opened.store.raise_on["approve"] = ValueError(MARKER)
        replies = [opened.ask({"op": "kill", "job_id": JOB_ID, "by": "web-1"}), opened.ask({"op": "approve", "job_id": JOB_ID, "digest": digest, "by": "web-1"})]
        assert [reply["error"] for reply in replies] == [{"code": "internal", "message": door_module.MESSAGES["internal"]}] * 2
        for record in caplog.records:
            assert MARKER not in record.getMessage() and record.exc_info is None and record.exc_text is None
        assert any("RuntimeError" in line for line in door_lines(caplog)) and any("ValueError" in line for line in door_lines(caplog))
        captured = capsys.readouterr()
        assert MARKER not in captured.out + captured.err and "Traceback" not in captured.err
        assert opened.loop_errors == []

    def test_every_change_logs_its_attempt_and_its_outcome(self, opened, caplog):
        """Bug class: an approval, rejection, kill, EXPLAIN or auto-accept change that leaves no line when it times out or fails, so the log cannot say whether one was asked and what came of it."""
        caplog.set_level(logging.INFO, logger=LOG_NAME)
        opened.submit()
        opened.submit(OTHER_ID)
        digest = opened.result({"op": "job", "job_id": JOB_ID})["digest"]
        opened.result({"op": "approve", "job_id": JOB_ID, "digest": digest, "by": "web-1"})
        opened.code({"op": "approve", "job_id": OTHER_ID, "digest": "0" * 64, "by": "web-2"})
        opened.result({"op": "reject", "job_id": OTHER_ID, "reason": "no", "by": "web-3"})
        opened.result({"op": "kill", "job_id": JOB_ID, "by": "web-4"})
        opened.result({"op": "explain", "job_id": JOB_ID, "by": "web-5"})
        opened.result({"op": "auto_accept.set", "target": "olap", "enabled": True, "limit": 1, "by": "web-6"})
        lines = door_lines(caplog)
        expected = [
            (f"approve of {JOB_ID} asked by web-1", f"approve of {JOB_ID} by web-1: ok"),
            (f"approve of {OTHER_ID} asked by web-2", f"approve of {OTHER_ID} by web-2: changed"),
            (f"reject of {OTHER_ID} asked by web-3", f"reject of {OTHER_ID} by web-3: ok"),
            (f"kill of {JOB_ID} asked by web-4", f"kill of {JOB_ID} by web-4: ok, was running"),
            (f"explain of {JOB_ID} asked by web-5", f"explain of {JOB_ID} by web-5: ok"),
            ("auto_accept.set (enabled=True, limit=1) of olap asked by web-6", "auto_accept.set (enabled=True, limit=1) of olap by web-6: ok"),
        ]
        for attempt, outcome in expected:
            assert lines.index("control socket: " + attempt) < lines.index("control socket: " + outcome)

    def test_outcome_unknown_is_logged(self, rig, caplog):
        """Bug class: an approval the loop started and did not finish in time leaving no line, while it may still take effect."""
        caplog.set_level(logging.INFO, logger=LOG_NAME)
        rig.open(limits=door_module.Limits(loop_seconds=0.2))
        rig.submit()
        digest = rig.result({"op": "job", "job_id": JOB_ID})["digest"]
        gate = threading.Event()
        rig.store.block_on["find"] = gate
        try:
            assert rig.code({"op": "approve", "job_id": JOB_ID, "digest": digest, "by": "web-8"}) == "outcome_unknown"
        finally:
            gate.set()
        lines = door_lines(caplog)
        assert lines.index(f"control socket: approve of {JOB_ID} asked by web-8") < lines.index(f"control socket: approve of {JOB_ID} by web-8: outcome_unknown")
        # It took effect after all, as the answer says it may.
        wait_for("the approval", lambda: (rig.store.approved / f"{JOB_ID}.json").exists())

    def test_unavailable_is_logged(self, opened, caplog):
        """Bug class: a request refused because the scheduler's loop is gone leaving no line."""
        caplog.set_level(logging.INFO, logger=LOG_NAME)
        opened.loop.call_soon_threadsafe(opened.loop.stop)
        wait_for("the loop to stop", lambda: not opened.loop.is_running())
        opened.loop.close()
        assert opened.code({"op": "kill", "job_id": JOB_ID, "by": "web-9"}) == "unavailable"
        lines = door_lines(caplog)
        assert lines.index(f"control socket: kill of {JOB_ID} asked by web-9") < lines.index(f"control socket: kill of {JOB_ID} by web-9: unavailable")
        assert opened.runner.cancels == []

    def test_close_removes_only_its_own_socket(self, rig):
        """Bug class: a close that removes a socket another scheduler bound since, or leaves its own behind."""
        rig.open()
        place = rig.control / door_module.SOCKET_NAME
        os.unlink(place)
        place.write_text("someone else's")
        rig.close()
        assert place.read_text() == "someone else's"


# ── j. the scheduler's start ────────────────────────────────────────────────


@pytest.fixture
def app_module():
    from social_data_pipeline.jobs import app

    return app


class TestTheSchedulerStarts:
    def test_a_door_that_raises_is_logged_by_its_kind_and_returns_none(self, app_module, rig, monkeypatch, caplog):
        """Bug class: an error of the door's opening ending the scheduler's start (the page, the MCP endpoint and the runner gone with it), or its text in the log."""
        caplog.set_level(logging.INFO)

        def broken(*args, **kwargs):
            raise RuntimeError(MARKER)

        monkeypatch.setattr(door_module, "open_door", broken)
        assert app_module._open_door(rig.cfg, rig.store, rig.runner, rig.auto_accept, rig.loop) is None
        assert any("RuntimeError" in record.getMessage() for record in caplog.records)
        assert all(MARKER not in record.getMessage() for record in caplog.records)

    @pytest.mark.parametrize("missing", ["social_data_pipeline.jobs.door", "social_data_pipeline.control"])
    def test_a_door_that_cannot_be_imported_returns_none(self, app_module, rig, monkeypatch, caplog, missing):
        """Bug class: an image without the `control` package (or a door module that fails to import) stopping the scheduler."""
        caplog.set_level(logging.INFO)
        monkeypatch.delitem(sys.modules, "social_data_pipeline.jobs.door")
        monkeypatch.setitem(sys.modules, missing, None)
        assert app_module._open_door(rig.cfg, rig.store, rig.runner, rig.auto_accept, rig.loop) is None
        assert any("ImportError" in record.getMessage() or "ModuleNotFoundError" in record.getMessage() for record in caplog.records)

    def test_a_refused_door_and_a_close_that_fails(self, app_module, rig, monkeypatch):
        """Bug class: a closed door (None) or a close that raises taken for an error that stops the scheduler or its shutdown."""
        opened = app_module._open_door(rig.cfg, rig.store, rig.runner, rig.auto_accept, rig.loop)
        assert opened is not None
        app_module._close_door(opened)
        rig.cfg.result_root = rig.jobs_dir
        assert app_module._open_door(rig.cfg, rig.store, rig.runner, rig.auto_accept, rig.loop) is None
        failing = MagicMock()
        failing.close.side_effect = OSError(MARKER)
        app_module._close_door(failing)
        app_module._close_door(None)

    @pytest.mark.parametrize("how", ["opens", "refused", "raises", "import_fails"])
    def test_the_page_works_whatever_the_door_does(self, tmp_path, monkeypatch, how):
        """Bug class: the scheduler's lifespan stopped by the door: the page does not answer when the door cannot be opened or its module cannot be imported."""
        pytest.importorskip("httpx")
        fastapi = pytest.importorskip("fastapi")
        if isinstance(fastapi, MagicMock) or not hasattr(fastapi, "__file__"):
            pytest.skip("FastAPI is a stand-in here")
        from fastapi.testclient import TestClient

        from social_data_pipeline.jobs import app as app_module

        jobs_dir = tmp_path / "jobs"
        jobs_dir.mkdir()
        cfg = JobsConfig(port=8050, jobs_dir=jobs_dir, result_root=jobs_dir / "results", host_result_root=str(jobs_dir / "results"), max_concurrent=1, default_timeouts={"postgres": 0, "starrocks": 259200, "mongodb": 0}, history_retention=500, targets={"warehouse": Target(name="warehouse", backend="postgres", database="datasets")})
        if how == "refused":
            cfg.result_root = jobs_dir
        elif how == "raises":
            monkeypatch.setattr(door_module, "open_door", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError(MARKER)))
        elif how == "import_fails":
            monkeypatch.delitem(sys.modules, "social_data_pipeline.jobs.door")
            monkeypatch.setitem(sys.modules, "social_data_pipeline.jobs.door", None)
        app = app_module.build_app(cfg)
        with TestClient(app) as client:
            assert client.get("/pending").status_code == 200
            assert (jobs_dir / "control" / door_module.SOCKET_NAME).exists() == (how == "opens")
        assert not (jobs_dir / "control" / door_module.SOCKET_NAME).exists()


# ── k. the Job record ───────────────────────────────────────────────────────


class TestJobRecord:
    def test_an_old_record_loads_and_is_written_back_byte_for_byte(self, tmp_path):
        """Bug class: a new required field (old history lines raise TypeError), or a new key written into every record."""
        old = {key: value for key, value in make_job().to_dict().items()}
        assert "acted_by" not in old
        job = Job.from_dict(old)
        assert job.acted_by == {} and job.extras == {}
        assert json.dumps(job.to_dict()) == json.dumps(old)

    def test_a_record_with_labels_round_trips(self):
        """Bug class: the labels lost at a load (taken for an unknown key and moved to extras) or at a write."""
        job = make_job(acted_by={"approved": "web-1", "killed": "web-2"})
        back = Job.from_dict(json.loads(json.dumps(job.to_dict())))
        assert back == job and back.extras == {}

    def test_approve_and_reject_without_a_label_write_what_they_wrote_before(self, tmp_path):
        """Bug class: the new keyword changing what the page's approve and reject write."""
        store = Store(tmp_path)
        store.submit(make_job())
        store.approve(JOB_ID)
        assert "acted_by" not in json.loads((store.approved / f"{JOB_ID}.json").read_text())
        store.submit(make_job(OTHER_ID))
        store.reject(OTHER_ID, "no")
        assert "acted_by" not in json.loads(store.history.read_text().splitlines()[-1])
