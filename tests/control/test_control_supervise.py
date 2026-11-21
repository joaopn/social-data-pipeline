"""Tests for `control.supervise`, the process that keeps one recorded pipeline run alive and records its end.

The contracts pinned here:

- The child's output is the run's log, in order; the record goes `starting`, `running`, then `finished` with the child's exit code and an end time.
- The supervisor holds the run's lock for its whole life and does not hand it to the child; one that was not given the lock refuses and starts nothing.
- The request reaches the child on its standard input and is in no file of the deployment, no argument list and no environment.
- A cancel stops the run's labelled containers through the run store and ends the child's process group only when Docker answers that there is no container (TERM, then KILL after the grace time); the run is recorded as `cancelled` only when the child's processes have ended and a listing made after that shows no container.
- A child that ends while something it started is still running does not end the run: the containers are counted when the whole process group is gone, and a group that was seen empty is never signalled again.
- A child that ends while a container of the run lives keeps the run `running`; the container's output is appended to the log and its exit code is the run's.
- An end is recorded only when it is established: with Docker out of reach, or a container that cannot be stopped, the record stays as it is and the log says so. Before an end is written the run's containers are listed in every state: one that was created and never started is removed, and the end waits until none is listed at all.
- The supervisor's own problems are plain lines in the log, never a traceback.

The child is a small Python program written into the temp folder, started as a real process. Most tests call `supervise` in a thread of this process with Docker replaced by the fake; the ones about signals, the lock at the end of the process and what a process's argument list and environment hold run the real entry point as a process, with the same fake inside it. Linux is assumed: `/proc/<pid>/cmdline`, `/proc/<pid>/environ` and `/proc/<pid>/stat` are read.
"""

from __future__ import annotations

import hashlib
import os
import re
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from social_data_pipeline import runstore
from social_data_pipeline.control import supervise

REPO_ROOT = Path(__file__).resolve().parents[2]

PLANTED = "planted-made-up-secret-9d2e"
REQUEST = ('{"verb": "run.start", "fields": {"profile": "parse", "source": "demo", "admin_password": "' + PLANTED + '"}, "run_id": "r20261121T101500-0a1b2c"}').encode()
CONTAINER = "a1" * 32
SERVICES = {"parse": "parse"}
PS_FORMAT = '[{{json .ID}},{{json (.Label "com.docker.compose.project.working_dir")}},{{json (.Label "com.docker.compose.service")}},{{json (.Label "sdp.run")}}]'
PS_STATE_FORMAT = '[{{json .ID}},{{json (.Label "com.docker.compose.project.working_dir")}},{{json (.Label "com.docker.compose.service")}},{{json (.Label "sdp.run")}},{{json .State}}]'

REFUSED_LOCK = b"[run supervisor] refused: this process does not hold the lock of the run\n"
REFUSED_STATE = b"[run supervisor] refused: the run is not one that waits to be started\n"
CONTAINER_LIVES = b"[run supervisor] the compose process ended while the container of the run is alive: from here on the output is read from Docker\n"
NOT_ESTABLISHED = b"[run supervisor] the end of the run could not be established (Docker does not answer, or a container of the run is not stopped or does not go away): its record is left as it is\n"
CHILD_LINES = b"first line\nto standard error\nlast line\n"

# No test of this file reaches the real Docker: the store's Docker function is replaced, and a test that reaches the replacement fails.
pytestmark = [pytest.mark.usefixtures("run_docker_guard"), pytest.mark.skipif(not sys.platform.startswith("linux"), reason="reads /proc and uses flock and process groups")]

# The child. It reads its standard input to the end and leaves the length and the SHA-256 of what it read in a file (never the request itself), prints a line on each output, and then does what its mode says:
# `exit:<code>` ends at once; `dies-alone:<code>` ends at once too, but leaves a second process in its process group that runs until the file `other-go` appears (a compose process that outlives the child); `lasts` waits for the file `go` and ends with the code in it; `ignore-term` does the same and only takes note of TERM; `container` stands for a compose process attached to a container: it ends a moment after `go` appears (a compose process needs that moment once its container has stopped), and takes note of a TERM it must never get; `group` starts a second process in its process group before it waits.
STAND_IN = r'''
import hashlib, os, signal, subprocess, sys, time

mode, out = sys.argv[1], sys.argv[2]


def note(name, text=""):
    with open(os.path.join(out, name + ".tmp"), "w") as handle:
        handle.write(text)
    os.replace(os.path.join(out, name + ".tmp"), os.path.join(out, name))


def on_term(signum, frame):
    note("got-term")
    if mode == "container":
        sys.exit(143)


if mode in ("ignore-term", "container"):
    signal.signal(signal.SIGTERM, on_term)
request = sys.stdin.buffer.read()
note("stdin", f"{len(request)} {hashlib.sha256(request).hexdigest()}")
print("first line", flush=True)
print("to standard error", file=sys.stderr, flush=True)
if mode == "group":
    other = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"])
    note("other-pid", str(other.pid))
if mode.startswith("dies-alone:"):
    waiter = "import os, sys, time\nwhile not os.path.exists(sys.argv[1]):\n    time.sleep(0.01)\n"
    other = subprocess.Popen([sys.executable, "-c", waiter, os.path.join(out, "other-go")], stdin=subprocess.DEVNULL)
    note("other-pid", str(other.pid))
note("pid", str(os.getpid()))
if mode.startswith("dies-alone:"):
    print("last line", flush=True)
    os._exit(int(mode[11:]))
if mode.startswith("exit:"):
    print("last line", flush=True)
    sys.exit(int(mode[5:]))
go = os.path.join(out, "go")
while not os.path.exists(go):
    time.sleep(0.01)
if mode == "container":
    time.sleep(0.3)
print("last line", flush=True)
sys.exit(int(open(go).read() or 0))
'''

# The real entry point with the Docker fake inside the process, the stand-in as the child and short times.
DRIVER = r'''
import sys

from social_data_pipeline import runstore
from social_data_pipeline.control import supervise
from tests.conftest import FakeRunDocker

runstore._docker = FakeRunDocker(runstore.DockerUnavailable)
supervise.CHILD_ARGV = (sys.executable, sys.argv[1], sys.argv[2], sys.argv[3])
supervise.POLL_SECONDS = 0.02
supervise.GRACE_SECONDS = 5
sys.argv[1:] = sys.argv[4:]
sys.exit(supervise.main())
'''


# ── helpers ─────────────────────────────────────────────────────────────────


def wait_for(what, probe, seconds=20):
    """Call probe every few milliseconds until it returns something true; fail when the time is up."""
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        found = probe()
        if found:
            return found
        time.sleep(0.01)
    raise AssertionError(f"timed out waiting for {what}")


def ps_for(run_id):
    return ["ps", "--no-trunc", "--filter", "label=com.docker.compose.oneoff=True", "--filter", f"label=sdp.run={run_id}", "--format", PS_FORMAT]


def ps_all_for(run_id):
    """The last look before an end is written: the run's containers in every state."""
    return ["ps", "-a", "--no-trunc", "--filter", "label=com.docker.compose.oneoff=True", "--filter", f"label=sdp.run={run_id}", "--format", PS_STATE_FORMAT]


def process_is_gone(pid):
    """Whether a process has ended: it is no longer there, or only its entry is left until its parent collects it."""
    try:
        with open(f"/proc/{pid}/stat") as handle:
            return handle.read().rsplit(")", 1)[1].split()[0] == "Z"
    except (FileNotFoundError, ProcessLookupError):
        return True


def kill_group(pid):
    for target in (lambda: os.killpg(pid, signal.SIGKILL), lambda: os.kill(pid, signal.SIGKILL)):
        try:
            target()
        except (ProcessLookupError, PermissionError):
            pass


class Run:
    """One recorded run with its lock held by this test, a folder for the notes of its child, and what is needed to end everything it started."""

    def __init__(self, root, tmp_path, script, number):
        self.root = root
        self.script = script
        self.out = tmp_path / f"notes-{number}"
        self.out.mkdir()
        self.run_id, self.lock_fd = runstore.create_run_locked(root, {"profile": "parse", "source": "demo", "flags": {}, "started_by": "operator"})
        self.folder = root / ".sdp" / "runs" / self.run_id
        self.told = threading.Event()
        self.thread = None
        self.process = None
        self.code = None
        self.error = None

    def child_argv(self, mode):
        return [sys.executable, str(self.script), mode, str(self.out)]

    def start(self, mode, *, request=REQUEST, grace=5.0):
        """Call `supervise` in a thread, the way the entry point does; at its end the lock descriptor is closed, as the end of the supervisor's process closes it."""
        def body():
            try:
                self.code = supervise.supervise(self.root, self.run_id, self.lock_fd, request, child_argv=self.child_argv(mode), interrupted=self.told.is_set, poll_seconds=0.02, grace_seconds=grace)
            except BaseException as error:
                self.error = error
            finally:
                self.close_lock()

        self.thread = threading.Thread(target=body, daemon=True)
        self.thread.start()
        return self

    def start_process(self, mode, driver, *, request=REQUEST):
        """Start the real entry point as a process that inherits the lock descriptor, hand it the request and close this process's copy of the descriptor."""
        self.stderr = self.out / "supervisor-stderr"
        with open(self.stderr, "wb") as stderr:
            self.process = subprocess.Popen(
                [sys.executable, str(driver), str(self.script), mode, str(self.out), self.run_id, str(self.lock_fd)],
                cwd=self.root, env={**os.environ, "PYTHONPATH": str(REPO_ROOT), "PYTHONDONTWRITEBYTECODE": "1"}, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=stderr, start_new_session=True, pass_fds=(self.lock_fd,),
            )
        self.process.stdin.write(request)
        self.process.stdin.close()
        self.close_lock()
        return self

    def close_lock(self):
        if self.lock_fd is not None:
            os.close(self.lock_fd)
            self.lock_fd = None

    def note(self, name):
        path = self.out / name
        return path.read_text() if path.exists() else None

    def child_pid(self):
        return int(wait_for("the child to have started", lambda: self.note("pid")))

    def go(self, exit_code=0):
        (self.out / "go.tmp").write_text(str(exit_code))
        os.replace(self.out / "go.tmp", self.out / "go")

    def join(self, seconds=20):
        self.thread.join(seconds)
        assert not self.thread.is_alive(), "the supervisor did not return"
        assert self.error is None, self.error
        return self.code

    def record(self):
        return runstore.read_record(self.root, self.run_id)

    def log(self):
        path = self.folder / "log"
        return path.read_bytes() if path.exists() else b""

    def end_everything(self):
        self.told.set()
        for name in ("pid", "other-pid"):
            if self.note(name):
                kill_group(int(self.note(name)))
        if self.process is not None:
            if self.process.poll() is None:
                kill_group(self.process.pid)
            self.process.wait(timeout=30)
        if self.thread is not None:
            self.thread.join(30)
        self.close_lock()


@pytest.fixture(autouse=True)
def _no_waiting_for_locks(monkeypatch):
    """Taking a lock that is held is tried a few times with a pause; the tests probe held locks on purpose and need no pause."""
    monkeypatch.setattr(runstore, "_LOCK_RETRY_SECONDS", 0)


@pytest.fixture
def root(tmp_path):
    folder = tmp_path.resolve() / "deployment"
    folder.mkdir()
    return folder


@pytest.fixture
def new_run(root, tmp_path, fake_run_docker):
    """Make recorded runs whose lock this test holds; everything they started is ended when the test is over."""
    script = tmp_path / "stand_in_child.py"
    script.write_text(STAND_IN)
    runs = []

    def make():
        run = Run(root, tmp_path, script, len(runs))
        runs.append(run)
        return run

    yield make
    for run in runs:
        run.end_everything()


@pytest.fixture
def driver(tmp_path):
    path = tmp_path / "supervisor_driver.py"
    path.write_text(DRIVER)
    return path


def files_under(folder):
    return [path for path in sorted(folder.rglob("*")) if path.is_file()]


# ── a run that ends by itself ───────────────────────────────────────────────


class TestARunThatEndsByItself:
    def test_the_log_the_record_and_the_lock_through_the_life_of_a_run(self, new_run, root, fake_run_docker):
        """Bug class: a run that cannot be followed or trusted: output missing from the log or out of order (the child's two outputs going to different places, or buffered), a record that never says `running` or ends without its exit code and end time, a lock that is not held while the run lives (every listing then says `lost`), or the request not reaching the child."""
        run = new_run().start("lasts")
        run.child_pid()

        wait_for("the record to say running", lambda: run.record()["state"] == "running")
        assert run.record()["ended"] is None and run.record()["exit_code"] is None
        assert run.log() == b"first line\nto standard error\n"
        assert runstore.acquire_run_lock(root, run.run_id) is None
        assert runstore.read_run(root, run.run_id)["state"] == "running"
        assert run.note("stdin") == f"{len(REQUEST)} {hashlib.sha256(REQUEST).hexdigest()}"

        run.go(7)
        assert run.join() == 0

        record = run.record()
        assert (record["state"], record["exit_code"]) == ("finished", 7)
        assert re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z", record["ended"]) and record["ended"] >= record["started"]
        assert run.log() == CHILD_LINES
        # Docker was asked twice, after the child had ended: whether a container of the run is running, and whether one is listed in any other state.
        assert fake_run_docker.calls == [ps_for(run.run_id), ps_all_for(run.run_id)]
        assert sorted(os.listdir(run.folder)) == ["lock", "log", "record.json"]

    @pytest.mark.parametrize("exit_code", [0, 1, 137])
    def test_the_exit_code_of_the_child_is_the_runs(self, new_run, exit_code):
        """Bug class: every run recorded as a success, or the exit code taken from somewhere else than the child."""
        run = new_run().start(f"exit:{exit_code}")

        assert run.join() == 0
        assert (run.record()["state"], run.record()["exit_code"]) == ("finished", exit_code)

    def test_a_cancel_that_comes_after_the_end_changes_nothing(self, new_run, root, fake_run_docker):
        """Bug class: a run that did all its work recorded as `cancelled` because a cancel was asked in the moment between its end and the writing of its record; or a cancel asked later rewriting the record of a run that has ended. The cancel request is written at the moment the supervisor asks Docker whether a container is left, which is after the child ended."""
        run = new_run()
        asked = []

        def cancel_now(labels):
            if not asked:
                asked.append(runstore.cancel_run(root, SERVICES, run.run_id))

        fake_run_docker.on_ps = cancel_now
        run.start("exit:0")

        assert run.join() == 0
        assert asked == ["requested"]
        record = run.record()
        assert (record["state"], record["exit_code"]) == ("finished", 0)
        before = (run.folder / "record.json").read_bytes()
        with pytest.raises(runstore.CancelRefused) as exc:
            runstore.cancel_run(root, SERVICES, run.run_id)
        assert exc.value.message == "the run has already ended"
        assert (run.folder / "record.json").read_bytes() == before and fake_run_docker.named("stop") == []


# ── the request ─────────────────────────────────────────────────────────────


class TestTheRequestStaysInMemory:
    def test_the_request_is_in_no_file_no_argument_list_and_no_environment(self, new_run, root, driver, fake_run_docker):
        """Bug class: the admin password written where others can read it: a file of the run (a request file, a debug line in the log), the argument list of the supervisor or of the child (readable by every user of the host through `ps`), or an environment variable (inherited by everything the child starts). The supervisor runs as a real process; the child reports the length and the hash of what it read, which is the whole request."""
        run = new_run().start_process("lasts", driver)
        child = run.child_pid()

        assert run.note("stdin") == f"{len(REQUEST)} {hashlib.sha256(REQUEST).hexdigest()}"
        for pid in (run.process.pid, child):
            for name in ("cmdline", "environ"):
                content = Path(f"/proc/{pid}/{name}").read_bytes()
                assert content and PLANTED.encode() not in content and b"admin_password" not in content, (pid, name)
        # (control) the argument list that was read is the supervisor's: it ends with the run id and the number of the lock descriptor.
        arguments = Path(f"/proc/{run.process.pid}/cmdline").read_bytes().split(b"\0")
        assert arguments[-3] == run.run_id.encode() and arguments[-2].isdigit() and arguments[-1] == b""
        run.go(0)
        assert run.process.wait(timeout=20) == 0

        assert run.record()["state"] == "finished"
        files = files_under(root)
        assert [path.name for path in files] == ["lock", "log", "record.json"]
        for path in files:
            assert PLANTED.encode() not in path.read_bytes() and b"admin_password" not in path.read_bytes(), path
        assert run.stderr.read_bytes() == b""


# ── the lock ────────────────────────────────────────────────────────────────


class TestTheLock:
    def test_the_lock_is_held_to_the_end_of_the_process_and_free_afterwards(self, new_run, root, driver, fake_run_docker):
        """Bug class: a lock that outlives the supervisor (a copy of the descriptor left in another process), which makes a run that nobody supervises read as `running` for ever; or one given up before the end is recorded, which shows the run as `lost` for a moment."""
        run = new_run().start_process("lasts", driver)
        run.child_pid()
        wait_for("the record to say running", lambda: run.record()["state"] == "running")

        assert runstore.acquire_run_lock(root, run.run_id) is None
        run.go(0)
        assert run.process.wait(timeout=20) == 0

        assert run.record()["state"] == "finished"
        taken = runstore.acquire_run_lock(root, run.run_id)
        assert isinstance(taken, int)
        runstore.release_run_lock(taken)

    def test_the_child_does_not_inherit_the_lock(self, new_run, root, driver, fake_run_docker):
        """Bug class: the lock descriptor passed on to the child (and from there to `docker compose`): a supervisor that is killed would then leave a run that reads as `running` for as long as the child lives, and that no cancel can stop, since a cancel leaves the stopping to the holder of the lock. With the supervisor killed and the child alive, the lock must be free and the run `lost` (no container carries its label here)."""
        run = new_run().start_process("lasts", driver)
        child = run.child_pid()
        wait_for("the record to say running", lambda: run.record()["state"] == "running")

        run.process.kill()
        run.process.wait(timeout=20)

        assert not process_is_gone(child)
        assert runstore.read_run(root, run.run_id)["state"] == "lost"
        assert run.record()["state"] == "running"

    @pytest.mark.parametrize("case", ["another-file", "held-by-another", "held-by-nobody", "not-open"])
    def test_a_supervisor_that_does_not_hold_the_lock_refuses(self, new_run, root, tmp_path, fake_run_docker, case):
        """Bug class: a supervisor that runs without the lock: started with a descriptor of some other file, with one of the lock file that another process holds or that nobody ever locked, or with a number that is no open descriptor. Its run would read as `lost` while it runs, a cancel would record it as cancelled while it goes on, and a second supervisor could be started for it. It must say so in the log, start nothing and leave the record alone."""
        run = new_run()
        if case == "another-file":
            fd = os.open(tmp_path / "another-file", os.O_RDWR | os.O_CREAT, 0o600)
        elif case == "not-open":
            fd = os.dup(run.lock_fd)
            os.close(fd)
        else:
            fd = os.open(run.folder / "lock", os.O_RDWR)
            if case == "held-by-nobody":
                run.close_lock()
        before = (run.folder / "record.json").read_bytes()

        code = supervise.supervise(root, run.run_id, fd, REQUEST, child_argv=run.child_argv("exit:0"), poll_seconds=0.02)

        assert code == 1 and run.log() == REFUSED_LOCK
        assert (run.folder / "record.json").read_bytes() == before and run.record()["state"] == "starting"
        assert os.listdir(run.out) == [] and fake_run_docker.calls == []
        if case != "not-open":
            os.close(fd)

    @pytest.mark.parametrize("state", ["running", "finished", "cancelled"])
    def test_a_run_that_was_started_before_is_not_started_again(self, new_run, root, fake_run_docker, state):
        """Bug class: the request of a run carried out a second time, and the record of a run that has ended overwritten with `running`, when a supervisor is started for a run that is not waiting to be started."""
        run = new_run()
        runstore.update_record(root, run.run_id, state=state)
        before = (run.folder / "record.json").read_bytes()

        code = supervise.supervise(root, run.run_id, run.lock_fd, REQUEST, child_argv=run.child_argv("exit:0"), poll_seconds=0.02)

        assert code == 1 and run.log() == REFUSED_STATE
        assert (run.folder / "record.json").read_bytes() == before
        assert os.listdir(run.out) == [] and fake_run_docker.calls == []


# ── cancel ──────────────────────────────────────────────────────────────────


class TestCancel:
    def test_without_a_container_the_childs_process_group_is_ended(self, new_run, root, fake_run_docker):
        """Bug class: a cancel during an image build, or during the checks before the container starts, that ends nothing (there is no container to stop), or that ends the child only and leaves what the child started (the `docker compose` process) running. The child here has a second process in its process group: both must be gone, the run `cancelled` with the signal as the exit code, and no stop sent to Docker."""
        run = new_run().start("group")
        child = run.child_pid()
        other = int(run.note("other-pid"))
        assert not process_is_gone(other)

        assert runstore.cancel_run(root, SERVICES, run.run_id) == "requested"
        assert run.join() == 0

        record = run.record()
        assert (record["state"], record["exit_code"]) == ("cancelled", -signal.SIGTERM) and record["ended"] is not None
        wait_for("the second process of the group to be gone", lambda: process_is_gone(other), 10)
        assert process_is_gone(child)
        assert fake_run_docker.named("stop") == [] and set(map(tuple, fake_run_docker.calls)) == {tuple(ps_for(run.run_id)), tuple(ps_all_for(run.run_id))}
        # The processes were ended by a signal, so the last look was made twice.
        assert fake_run_docker.calls[-3:] == [ps_all_for(run.run_id), ps_for(run.run_id), ps_all_for(run.run_id)] and fake_run_docker.calls.count(ps_all_for(run.run_id)) == 2
        assert run.log() == b"first line\nto standard error\n"

    def test_a_container_is_stopped_through_the_store_and_the_child_is_left_to_end(self, new_run, root, fake_run_docker):
        """Bug class: a cancel that kills the `docker compose run` process, which leaves the container running (measured: neither TERM nor KILL to that process stops it); or one that stops the container and then does not wait for the compose process, losing the container's exit code. The container must be stopped by its full id with the interrupt signal, the child must get no signal, and the exit code it ends with (the container's) is the run's."""
        run = new_run()
        fake_run_docker.add(CONTAINER, root, run=run.run_id)
        fake_run_docker.on_ps = lambda labels: run.go(3) if not fake_run_docker.containers else None
        run.start("container")
        run.child_pid()
        wait_for("the record to say running", lambda: run.record()["state"] == "running")
        assert fake_run_docker.calls == []

        assert runstore.cancel_run(root, SERVICES, run.run_id) == "requested"
        assert run.join() == 0

        # The listing, the stop and the listing after it; one that finds nothing while the compose process has ended; the one made after that; and the last look, at every state.
        ps = ps_for(run.run_id)
        assert fake_run_docker.calls == [ps, ["stop", "--signal", "INT", "-t", "30", CONTAINER], ps, ps, ps, ps_all_for(run.run_id)]
        assert run.note("got-term") is None
        assert (run.record()["state"], run.record()["exit_code"]) == ("cancelled", 3)
        assert run.log() == CHILD_LINES

    def test_the_run_is_not_cancelled_while_its_container_is_listed(self, new_run, root, monkeypatch, fake_run_docker):
        """Bug class: the record written as `cancelled` after a stop that did not stop the container: the run is then final, no later cancel reaches it, and its container runs on unseen. While Docker lists the container the record must stay `running`, and the stop must be sent again."""
        monkeypatch.setattr(supervise, "DOCKER_ATTEMPTS", 10 ** 6)
        run = new_run()
        fake_run_docker.add(CONTAINER, root, run=run.run_id)
        fake_run_docker.stop_fails = True
        fake_run_docker.on_ps = lambda labels: run.go(3) if not fake_run_docker.containers else None
        run.start("container")
        run.child_pid()

        runstore.cancel_run(root, SERVICES, run.run_id)
        wait_for("the stop to be sent again", lambda: len(fake_run_docker.named("stop")) >= 3)
        assert run.record()["state"] == "running" and run.thread.is_alive()
        fake_run_docker.stop_fails = False

        assert run.join() == 0
        assert (run.record()["state"], run.record()["exit_code"]) == ("cancelled", 3)
        assert run.note("got-term") is None

    def test_a_child_that_ignores_term_is_killed_after_the_grace_time(self, new_run, root, fake_run_docker):
        """Bug class: a cancel that waits for ever on a process that does not end on TERM (a build that ignores it), with the run stuck in `running` and its supervisor never ending; or a KILL sent at once, which gives a process no chance to clean up."""
        run = new_run().start("ignore-term", grace=0.4)
        child = run.child_pid()

        started = time.monotonic()
        runstore.cancel_run(root, SERVICES, run.run_id)
        assert run.join() == 0
        elapsed = time.monotonic() - started

        assert run.note("got-term") == ""
        assert elapsed >= 0.4
        assert (run.record()["state"], run.record()["exit_code"]) == ("cancelled", -signal.SIGKILL)
        assert process_is_gone(child)

    def test_two_cancel_requests_are_one(self, new_run, root, fake_run_docker):
        """Bug class: a second cancel request (the button pressed twice, a signal next to the cancel file) breaking the first: an error in the supervisor, a second round of signals to a process group that is gone, or a record written twice with different ends."""
        run = new_run().start("lasts")
        run.child_pid()

        assert runstore.cancel_run(root, SERVICES, run.run_id) == "requested"
        assert runstore.cancel_run(root, SERVICES, run.run_id) == "requested"
        run.told.set()
        assert run.join() == 0

        assert (run.record()["state"], run.record()["exit_code"]) == ("cancelled", -signal.SIGTERM)
        assert b"supervisor" not in run.log()

    def test_a_cancel_with_docker_out_of_reach_records_no_end(self, new_run, root, monkeypatch, fake_run_docker):
        """Bug class: a run recorded as `cancelled` when Docker could not be asked whether its container is gone. A final record is never looked at again, so a container that lives on would be invisible: not listed, not cancellable, and no reason to refuse a second run. The child is still ended; the record stays as it is, the log says why, and the run store reports the run from what Docker says later."""
        monkeypatch.setattr(supervise, "DOCKER_ATTEMPTS", 3)
        run = new_run().start("lasts")
        child = run.child_pid()
        wait_for("the record to say running", lambda: run.record()["state"] == "running")
        fake_run_docker.reachable = False

        runstore.cancel_run(root, SERVICES, run.run_id)
        assert run.join() == 1

        assert process_is_gone(child)
        assert run.record()["state"] == "running" and run.record()["ended"] is None
        assert run.log() == b"first line\nto standard error\n" + NOT_ESTABLISHED
        assert runstore.read_run(root, run.run_id)["state"] == "unknown"

    def test_a_container_that_docker_does_not_stop_ends_in_no_record(self, new_run, root, monkeypatch, fake_run_docker):
        """Bug class: a supervisor that asks Docker for ever to stop a container it does not stop, or that gives up and records the run as cancelled. After a limited number of tries the child is ended and the record is left as it is: the run then reads as `detached` and can be cancelled again. The order is pinned, not a count that depends on timing: the first signal to the child's processes is TERM, and it comes after exactly as many failed stops as there are tries."""
        monkeypatch.setattr(supervise, "DOCKER_ATTEMPTS", 3)
        signals = []
        real = supervise._Processes.signal

        def signal_and_note(processes, signum):
            signals.append((signum, len(fake_run_docker.named("stop"))))
            real(processes, signum)

        monkeypatch.setattr(supervise._Processes, "signal", signal_and_note)
        run = new_run()
        fake_run_docker.add(CONTAINER, root, run=run.run_id)
        fake_run_docker.stop_fails = True
        run.start("container")
        run.child_pid()

        runstore.cancel_run(root, SERVICES, run.run_id)
        assert run.join() == 1

        assert signals[0] == (signal.SIGTERM, 3) and run.note("got-term") == ""
        assert run.record()["state"] == "running"
        assert run.log().endswith(NOT_ESTABLISHED)
        assert runstore.read_run(root, run.run_id)["state"] == "detached"


    def test_a_container_started_in_the_last_moment_is_stopped_before_the_run_is_cancelled(self, new_run, root, monkeypatch, fake_run_docker):
        """Bug class: `cancelled` recorded on a listing that was made while the compose process was still alive: it can start the container after that listing and be gone by the time its processes are looked at, and the run is then final with a container that has just started. The listing that ends a cancel must be one made after the processes were seen to have ended. Here the container appears, and the child ends, right after the first listing was answered with "none"; and the last look, on whose answer the end is written, is never made while the child is still there."""
        run = new_run().start("lasts")
        child = run.child_pid()
        real = runstore.stop_run_containers
        real_last_look = runstore.clear_run_containers
        answers = []
        child_gone_at_the_last_look = []

        def last_look(root_, run_id):
            child_gone_at_the_last_look.append(process_is_gone(child))
            return real_last_look(root_, run_id)

        def stop(root_, run_id):
            answer = real(root_, run_id)
            answers.append(answer)
            if len(answers) == 1:
                fake_run_docker.add(CONTAINER, root, run=run.run_id)
                run.go(0)
                wait_for("the child to be gone", lambda: process_is_gone(child))
            return answer

        monkeypatch.setattr(runstore, "stop_run_containers", stop)
        monkeypatch.setattr(runstore, "clear_run_containers", last_look)
        assert runstore.cancel_run(root, SERVICES, run.run_id) == "requested"
        assert run.join() == 0

        assert answers == [(0, 0), (1, 0)] and child_gone_at_the_last_look == [True]
        assert fake_run_docker.named("stop") == [["stop", "--signal", "INT", "-t", "30", CONTAINER]] and fake_run_docker.containers == []
        assert (run.record()["state"], run.record()["exit_code"]) == ("cancelled", 0)

    def test_a_listing_that_docker_does_not_answer_once_ends_no_process(self, new_run, root, monkeypatch, fake_run_docker):
        """Bug class: "Docker did not answer" taken for "there is no container": the compose process is then sent TERM while its container is alive, which cuts the run off from its log and records the signal instead of the container's exit code. One unanswered listing must end nothing; the next one finds the container, which is stopped through the store, and the compose process ends by itself."""
        run = new_run()
        fake_run_docker.add(CONTAINER, root, run=run.run_id)
        fake_run_docker.on_ps = lambda labels: run.go(3) if not fake_run_docker.containers else None
        run.start("container")
        run.child_pid()
        real = runstore.stop_run_containers
        asked = []

        def stop(root_, run_id):
            asked.append(run_id)
            if len(asked) == 1:
                raise runstore.DockerUnavailable(runstore.DOCKER_NOT_REACHABLE)
            return real(root_, run_id)

        monkeypatch.setattr(runstore, "stop_run_containers", stop)
        runstore.cancel_run(root, SERVICES, run.run_id)
        assert run.join() == 0

        assert run.note("got-term") is None and len(asked) >= 2
        assert (run.record()["state"], run.record()["exit_code"]) == ("cancelled", 3)
        assert run.log() == CHILD_LINES


# ── a container that exists and does not run ────────────────────────────────


class TestAContainerThatIsNotRunning:
    def test_at_a_cancel_one_that_never_started_is_removed_before_the_end_is_written(self, new_run, root, fake_run_docker):
        """Bug class: `cancelled` written while a container of the run exists that was created and never started (the compose process was ended during its create call): the listing of running containers does not show it, the record is final, and the container can still be started under it. It must be removed, by its full id and without force, and the end written only after a listing of every state shows none."""
        run = new_run()
        fake_run_docker.add(CONTAINER, root, run=run.run_id, state="created")
        run.start("lasts")
        run.child_pid()

        runstore.cancel_run(root, SERVICES, run.run_id)
        assert run.join() == 0

        assert fake_run_docker.named("rm") == [["rm", CONTAINER]] and fake_run_docker.named("stop") == []
        ps, ps_all = ps_for(run.run_id), ps_all_for(run.run_id)
        assert fake_run_docker.calls[-5:] == [ps_all, ["rm", CONTAINER], ps_all, ps, ps_all]
        assert fake_run_docker.containers == []
        assert (run.record()["state"], run.record()["exit_code"]) == ("cancelled", -signal.SIGTERM)

    def test_at_a_cancel_one_that_is_listed_only_after_the_processes_are_gone_is_removed(self, new_run, root, monkeypatch, fake_run_docker):
        """Bug class: `cancelled` written on one empty listing made right after the compose process was ended by a signal: measured on a real Docker, the container that process was creating can appear in the listing only afterwards. The last look is made a second time, one pause later; the container appears between the two looks here, and must be removed before the end is written."""
        run = new_run().start("lasts")
        run.child_pid()
        real = runstore.clear_run_containers
        answers = []

        def last_look(root_, run_id):
            answers.append(real(root_, run_id))
            if len(answers) == 1:
                fake_run_docker.add(CONTAINER, root, run=run.run_id, state="created")
            return answers[-1]

        monkeypatch.setattr(runstore, "clear_run_containers", last_look)
        runstore.cancel_run(root, SERVICES, run.run_id)
        assert run.join() == 0

        assert answers[0] == 0 and len(answers) >= 2
        assert fake_run_docker.named("rm") == [["rm", CONTAINER]] and fake_run_docker.containers == []
        assert run.record()["state"] == "cancelled"

    def test_a_cancel_that_sent_no_signal_needs_one_last_look(self, new_run, root, fake_run_docker):
        """Bug class: every cancel made slower by the second look, also the usual one: the container was stopped through the store and the compose process ended by itself, so nothing was cut off while it created a container."""
        run = new_run()
        fake_run_docker.add(CONTAINER, root, run=run.run_id)
        fake_run_docker.on_ps = lambda labels: run.go(3) if not fake_run_docker.containers else None
        run.start("container")
        run.child_pid()

        runstore.cancel_run(root, SERVICES, run.run_id)
        assert run.join() == 0

        assert fake_run_docker.calls.count(ps_all_for(run.run_id)) == 1 and run.note("got-term") is None
        assert run.record()["state"] == "cancelled"

    def test_at_a_cancel_one_that_starts_before_it_is_removed_is_stopped(self, new_run, root, fake_run_docker):
        """Bug class: a container that the daemon starts a moment after the compose process is gone: removed by force (its work cut off without the interrupt the pipeline handles), or left running under a record that says `cancelled` because the removal failed and nobody looked again. The removal is without force and fails; the next round finds the container running and stops it through the store; only then is the end written."""
        run = new_run()
        fake_run_docker.add(CONTAINER, root, run=run.run_id, state="created")
        fake_run_docker.starts_at_rm = {CONTAINER}
        run.start("lasts")
        run.child_pid()

        runstore.cancel_run(root, SERVICES, run.run_id)
        assert run.join() == 0

        assert fake_run_docker.named("rm") == [["rm", CONTAINER]]
        assert fake_run_docker.named("stop") == [["stop", "--signal", "INT", "-t", "30", CONTAINER]]
        calls = fake_run_docker.calls
        assert calls.index(["rm", CONTAINER]) < calls.index(["stop", "--signal", "INT", "-t", "30", CONTAINER]) and calls[-1] == ps_all_for(run.run_id)
        assert fake_run_docker.containers == [] and run.record()["state"] == "cancelled"

    def test_at_a_natural_end_one_that_never_started_is_removed(self, new_run, root, fake_run_docker):
        """Bug class: the same at the end of a run nobody cancelled: the child has ended, no container is running, and `finished` is written over a container that was created and can still be started."""
        run = new_run()
        fake_run_docker.add(CONTAINER, root, run=run.run_id, state="created")
        run.start("exit:2")

        assert run.join() == 0

        assert fake_run_docker.calls == [ps_for(run.run_id), ps_all_for(run.run_id), ["rm", CONTAINER], ps_all_for(run.run_id)]
        assert fake_run_docker.containers == []
        assert (run.record()["state"], run.record()["exit_code"]) == ("finished", 2)

    def test_at_a_natural_end_one_that_starts_before_it_is_removed_is_waited_for(self, new_run, root, fake_run_docker):
        """Bug class: a container that starts after the compose process died, recorded as a run that finished with the dead process's exit code. The removal fails, the container is found running, and the run goes on as one whose container outlived its compose process: `running`, then `finished` with the container's exit code."""
        run = new_run()
        fake_run_docker.add(CONTAINER, root, run=run.run_id, state="created")
        fake_run_docker.starts_at_rm = {CONTAINER}
        run.start("exit:1")

        wait_for("Docker to be asked to wait for the container", lambda: fake_run_docker.named("wait"))
        assert run.record()["state"] == "running" and fake_run_docker.named("rm") == [["rm", CONTAINER]]
        fake_run_docker.end(CONTAINER, 5)
        assert run.join() == 0

        assert (run.record()["state"], run.record()["exit_code"]) == ("finished", 5)
        assert fake_run_docker.calls[-1] == ps_all_for(run.run_id)

    def test_one_that_is_on_its_way_out_delays_the_end_until_it_is_gone(self, new_run, root, fake_run_docker):
        """Bug class: the end written while Docker still lists a container of the run (stopped and not yet removed by the daemon), or such a container removed by the supervisor, which removes only what never started. The end waits for a listing of every state that is empty; nothing is removed."""
        run = new_run()
        fake_run_docker.add(CONTAINER, root, run=run.run_id, state="exited")
        looks = []

        def daemon_removes_it(labels):
            looks.append(labels)
            if len(looks) == 5:
                fake_run_docker.containers.clear()

        fake_run_docker.on_ps = daemon_removes_it
        run.start("exit:0")

        assert run.join() == 0

        ps, ps_all = ps_for(run.run_id), ps_all_for(run.run_id)
        assert fake_run_docker.calls == [ps, ps_all, ps, ps_all, ps, ps_all]
        assert (run.record()["state"], run.record()["exit_code"]) == ("finished", 0)

    @pytest.mark.parametrize("ending", ["natural", "cancel"])
    def test_one_that_never_goes_away_ends_in_no_record(self, new_run, root, monkeypatch, fake_run_docker, ending):
        """Bug class: a supervisor that waits for ever for a container Docker keeps listing, or that gives up and writes the end. After the limited number of looks the record is left as it is and the log says why."""
        monkeypatch.setattr(supervise, "DOCKER_ATTEMPTS", 3)
        run = new_run()
        fake_run_docker.add(CONTAINER, root, run=run.run_id, state="dead")
        run.start("exit:0" if ending == "natural" else "lasts")
        if ending == "cancel":
            run.child_pid()
            runstore.cancel_run(root, SERVICES, run.run_id)

        assert run.join() == 1

        assert fake_run_docker.calls.count(ps_all_for(run.run_id)) == 3 and fake_run_docker.named("rm") == []
        assert run.record()["state"] == "running" and run.log().endswith(NOT_ESTABLISHED)

    @pytest.mark.parametrize("ending", ["natural", "cancel"])
    def test_a_last_look_that_docker_does_not_answer_is_not_an_empty_one(self, new_run, root, monkeypatch, fake_run_docker, ending):
        """Bug class: "Docker did not answer the last look" read as "no container is left", and the end written on it."""
        monkeypatch.setattr(supervise, "DOCKER_ATTEMPTS", 3)
        asked = []

        def unanswered(root_, run_id):
            asked.append(run_id)
            raise runstore.DockerUnavailable(runstore.DOCKER_NOT_REACHABLE)

        monkeypatch.setattr(runstore, "clear_run_containers", unanswered)
        run = new_run().start("exit:0" if ending == "natural" else "lasts")
        if ending == "cancel":
            run.child_pid()
            runstore.cancel_run(root, SERVICES, run.run_id)

        assert run.join() == 1

        assert len(asked) == 3
        assert run.record()["state"] == "running" and run.log().endswith(NOT_ESTABLISHED)


# ── what the child started ──────────────────────────────────────────────────


class TestWhatTheChildStarted:
    def test_a_child_that_dies_alone_does_not_end_the_run(self, new_run, root, fake_run_docker):
        """Bug class: a run recorded as finished when only the child has ended (killed, crashed) and the compose process it started is still in its checks or its image build: no container is listed yet, the record becomes final, and the compose process then starts the labelled container under it (not listed as alive, a cancel refused, a second run allowed). While a process of the child's group runs, the run must stay `running`, and Docker is not even asked; the container the compose process then starts is waited for, and its exit code is the run's."""
        run = new_run().start("dies-alone:1")
        child = run.child_pid()
        other = int(run.note("other-pid"))
        wait_for("the child to be gone", lambda: process_is_gone(child))
        # Time for fifteen looks of the supervisor.
        time.sleep(0.3)

        assert not process_is_gone(other)
        assert run.thread.is_alive() and run.record()["state"] == "running" and run.record()["ended"] is None
        assert runstore.read_run(root, run.run_id)["state"] == "running"
        assert fake_run_docker.calls == []

        fake_run_docker.add(CONTAINER, root, run=run.run_id)
        (run.out / "other-go").touch()
        wait_for("Docker to be asked to wait for the container", lambda: fake_run_docker.named("wait"))
        assert process_is_gone(other)
        fake_run_docker.end(CONTAINER, 5)
        assert run.join() == 0

        assert (run.record()["state"], run.record()["exit_code"]) == ("finished", 5)

    def test_a_cancel_is_honoured_while_only_what_the_child_started_is_left(self, new_run, root, fake_run_docker):
        """Bug class: a run that can no longer be cancelled once its child has died alone: the supervisor waits for the compose process and never looks at the cancel request, or looks and ends nothing because the child is gone. The process the child left behind must be ended through the process group."""
        run = new_run().start("dies-alone:1")
        child = run.child_pid()
        other = int(run.note("other-pid"))
        wait_for("the child to be gone", lambda: process_is_gone(child))

        assert runstore.cancel_run(root, SERVICES, run.run_id) == "requested"
        assert run.join() == 0

        wait_for("the process the child left behind to be gone", lambda: process_is_gone(other), 10)
        assert (run.record()["state"], run.record()["exit_code"]) == ("cancelled", 1)
        assert fake_run_docker.named("stop") == []

    def test_a_group_that_was_seen_empty_is_never_signalled_again(self):
        """Bug class: a signal sent by number to a process group that no longer exists: once the last process of the group has been collected, the number can be given to another process of the host, which a later TERM or KILL (the grace time of a cancel) would then hit. After the group was seen empty, neither a look nor a signal reaches the operating system again; before that, both do."""
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"], start_new_session=True)
        processes = supervise._Processes(child)
        sent = []
        real = os.killpg
        try:
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(os, "killpg", lambda group, signum: (sent.append((group, signum)), real(group, signum))[1])
                assert processes.alive() is True
                processes.signal(signal.SIGKILL)
                assert sent == [(child.pid, signal.SIGKILL)]
                child.wait(timeout=20)
                assert processes.alive() is False
                seen_empty = list(sent)
                processes.signal(signal.SIGTERM)
                processes.signal(signal.SIGKILL)
                assert processes.alive() is False
                assert sent == seen_empty
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=20)

    def test_the_group_is_looked_at_when_the_child_is_collected_and_not_after_a_docker_call(self, new_run, root, monkeypatch, fake_run_docker):
        """Bug class: the look at the child's process group made only after the next Docker call (a listing can take fifteen seconds, a stop thirty): the child was collected before that call, so its number was free all that time, and a new process group that got it would be taken for the child's and sent KILL when the grace time is over. The group must be seen empty at the moment the child is collected. Here the number counts as given to someone else from the first store call after the child was collected: from then on a look would see running processes. No look and no signal may come after that."""
        children = []
        real_popen = subprocess.Popen

        def popen(*args, **kwargs):
            children.append(real_popen(*args, **kwargs))
            return children[-1]

        reused = []
        looks_after = []
        signals_after = []
        real_look = supervise._group_has_a_running_process
        real_signal = supervise._Processes.signal
        real_stop = runstore.stop_run_containers

        def look(group):
            if reused:
                looks_after.append(group)
                return True
            return real_look(group)

        def send(processes, signum):
            if reused:
                signals_after.append(signum)
            else:
                real_signal(processes, signum)

        def stop(root_, run_id):
            if children[0].returncode is not None and not reused:
                reused.append(True)
            return real_stop(root_, run_id)

        monkeypatch.setattr(supervise.subprocess, "Popen", popen)
        monkeypatch.setattr(supervise, "_group_has_a_running_process", look)
        monkeypatch.setattr(supervise._Processes, "signal", send)
        monkeypatch.setattr(runstore, "stop_run_containers", stop)
        run = new_run().start("lasts", grace=0.2)
        run.child_pid()

        runstore.cancel_run(root, SERVICES, run.run_id)
        assert run.join() == 0

        assert reused == [True] and looks_after == [] and signals_after == []
        assert (run.record()["state"], run.record()["exit_code"]) == ("cancelled", -signal.SIGTERM)

    def test_a_group_that_may_not_be_signalled_is_not_taken_for_gone(self):
        """Bug class: "this user may not signal the group" read as "the group does not exist": the group is then counted as gone for good, and an end can be written while the compose process runs on. Whether a process of the group runs is read from the process table in that case."""
        living = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"], start_new_session=True)

        def not_permitted(group, signum):
            raise PermissionError(1, "Operation not permitted")

        try:
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(os, "killpg", not_permitted)
                assert supervise._group_has_a_running_process(living.pid) is True
                living.kill()
                living.wait(timeout=20)
                assert supervise._group_has_a_running_process(living.pid) is False
        finally:
            if living.poll() is None:
                living.kill()
                living.wait(timeout=20)

    def test_one_look_that_finds_nothing_is_not_the_last_word(self, monkeypatch):
        """Bug class: the group counted as gone for good on one look that missed a process (one that hands over to a successor and ends between the listing of the process table and the reading of its entry): nothing is ended at a cancel any more, and an end is written while that process runs. Only two looks in a row that find nothing count."""
        child = subprocess.Popen([sys.executable, "-c", "pass"], start_new_session=True)
        child.wait(timeout=20)
        answers = iter([False, True, False, False])
        looks = []
        monkeypatch.setattr(supervise, "_group_has_a_running_process", lambda group: (looks.append(group), next(answers))[1])
        processes = supervise._Processes(child)

        assert processes.alive() is True and processes.gone is False
        assert processes.alive() is False and processes.gone is True
        assert looks == [child.pid] * 4
        assert processes.alive() is False and len(looks) == 4

    def test_a_process_that_has_ended_and_is_not_collected_is_not_running(self):
        """Bug class: a run that stays `running` for ever because a process the child started has ended and nobody collects it (an orphan on a host whose first process does not): its entry keeps the process group in existence. Such an entry is not a running process; a living one is."""
        ended = subprocess.Popen([sys.executable, "-c", "pass"], start_new_session=True)
        living = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"], start_new_session=True)
        try:
            wait_for("the process to have ended", lambda: process_is_gone(ended.pid))
            # Not collected yet: the group still exists for the operating system.
            os.killpg(ended.pid, 0)

            assert supervise._group_has_a_running_process(ended.pid) is False
            assert supervise._group_has_a_running_process(living.pid) is True
        finally:
            living.kill()
            living.wait(timeout=20)
            ended.wait(timeout=20)
        assert supervise._group_has_a_running_process(living.pid) is False


# ── signals ─────────────────────────────────────────────────────────────────


class TestSignals:
    @pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
    def test_a_signal_to_the_supervisor_cancels_the_run(self, new_run, root, driver, fake_run_docker, signum):
        """Bug class: a supervisor that dies on TERM (the host shutting down, a service manager stopping the broker's group) and leaves its child and the container running with nobody to record their end; the signal must be a cancel request: the child's processes are ended and the run is recorded as `cancelled` before the supervisor ends."""
        run = new_run().start_process("lasts", driver)
        child = run.child_pid()
        wait_for("the record to say running", lambda: run.record()["state"] == "running")

        os.kill(run.process.pid, signum)

        assert run.process.wait(timeout=20) == 0
        assert (run.record()["state"], run.record()["exit_code"]) == ("cancelled", -signal.SIGTERM)
        wait_for("the child to be gone", lambda: process_is_gone(child), 10)
        assert run.stderr.read_bytes() == b""

    def test_hup_is_ignored(self, new_run, root, driver, fake_run_docker):
        """Bug class: the supervisor ending with the terminal or the session of the process that started it (HUP is what a closing session sends), which would end the supervision of every run the moment the broker restarts."""
        run = new_run().start_process("lasts", driver)
        run.child_pid()
        wait_for("the record to say running", lambda: run.record()["state"] == "running")

        os.kill(run.process.pid, signal.SIGHUP)
        time.sleep(0.3)

        assert run.process.poll() is None and run.record()["state"] == "running"
        assert runstore.acquire_run_lock(root, run.run_id) is None
        run.go(0)
        assert run.process.wait(timeout=20) == 0
        assert run.record()["state"] == "finished"


# ── a container that outlives the compose process ───────────────────────────


class TestAContainerThatOutlivesTheChild:
    def test_the_run_stays_running_and_ends_with_the_containers_exit_code(self, new_run, root, fake_run_docker):
        """Bug class: a run recorded as finished, with the exit code of a compose process that died, while its container is still working: the record is final, the container invisible, and a second run of the same profile and source is allowed next to it. The run must stay `running` under its lock, the container's further output must reach the log, and the exit code must be the one Docker reports for the container."""
        run = new_run()
        fake_run_docker.add(CONTAINER, root, run=run.run_id, log="from the container\n")
        before = time.time()
        run.start("exit:1")

        wait_for("Docker to be asked to wait for the container", lambda: fake_run_docker.named("wait") and fake_run_docker.named("logs"))
        wait_for("the container's output to be in the log", lambda: run.log().endswith(b"from the container\n"))
        assert run.record()["state"] == "running" and run.record()["exit_code"] is None
        assert runstore.read_run(root, run.run_id)["state"] == "running"
        assert runstore.acquire_run_lock(root, run.run_id) is None

        fake_run_docker.end(CONTAINER, 5)
        assert run.join() == 0

        assert (run.record()["state"], run.record()["exit_code"]) == ("finished", 5)
        assert run.log() == CHILD_LINES + CONTAINER_LIVES + b"from the container\n"
        assert fake_run_docker.named("wait") == [["wait", CONTAINER]]
        (follow,) = fake_run_docker.named("logs")
        assert follow[:3] == ["logs", "--follow", "--since"] and follow[4:] == [CONTAINER]
        assert before - 1 <= float(follow[3]) <= time.time()

    def test_a_cancel_is_still_honoured(self, new_run, root, fake_run_docker):
        """Bug class: a run whose compose process died that can no longer be cancelled: the supervisor sits in the wait for the container and never looks at the cancel request."""
        run = new_run()
        fake_run_docker.add(CONTAINER, root, run=run.run_id)
        run.start("exit:1")
        wait_for("Docker to be asked to wait for the container", lambda: fake_run_docker.named("wait"))

        assert runstore.cancel_run(root, SERVICES, run.run_id) == "requested"
        assert run.join() == 0

        assert fake_run_docker.named("stop") == [["stop", "--signal", "INT", "-t", "30", CONTAINER]]
        assert (run.record()["state"], run.record()["exit_code"]) == ("cancelled", None)

    def test_docker_out_of_reach_after_the_child_ended_records_no_end(self, new_run, root, monkeypatch, fake_run_docker):
        """Bug class: "Docker cannot be asked" read as "no container is left" when the child has ended: the run would be recorded as finished while its container may be alive. Docker is asked a limited number of times; then the record is left as it is and the log says why."""
        monkeypatch.setattr(supervise, "DOCKER_ATTEMPTS", 3)
        fake_run_docker.reachable = False
        run = new_run().start("exit:0")

        assert run.join() == 1

        assert fake_run_docker.calls == [ps_for(run.run_id)] * 3
        assert run.record()["state"] == "running" and run.record()["ended"] is None
        assert run.log() == CHILD_LINES + NOT_ESTABLISHED

    def test_a_wait_that_docker_refuses_is_not_an_end(self, new_run, root, monkeypatch, fake_run_docker):
        """Bug class: a failed `docker wait` read as "the container ended", with the run recorded as finished while the container is still listed; or a supervisor that asks again without limit."""
        monkeypatch.setattr(supervise, "DOCKER_ATTEMPTS", 3)
        run = new_run()
        fake_run_docker.add(CONTAINER, root, run=run.run_id)
        fake_run_docker.wait_fails = True
        fake_run_docker.logs_fail = True
        run.start("exit:1")

        assert run.join() == 1

        assert len(fake_run_docker.named("wait")) == 3
        assert run.record()["state"] == "running"
        assert run.log() == CHILD_LINES + CONTAINER_LIVES + NOT_ESTABLISHED


# ── the supervisor's own problems ───────────────────────────────────────────


class TestProblemsAreLinesInTheLog:
    def test_a_record_that_cannot_be_written(self, new_run, root, monkeypatch, capfd, fake_run_docker):
        """Bug class: a supervisor that dies with a traceback when the record cannot be written (a full disk): the traceback's frames hold the request, and nothing tells the operator what happened to the run. Each failed write is one plain line in the log, the child is still supervised to its end, and the request is in no output."""
        run = new_run()

        def broken(run_fd, record):
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(runstore, "_write_record", broken)
        run.start("exit:4")

        assert run.join() == 1
        monkeypatch.undo()

        lines = run.log().split(b"\n")
        assert sorted(lines) == sorted([b"first line", b"to standard error", b"last line", b"[run supervisor] the record of the run could not be written: it still says starting", b"[run supervisor] the run ended (finished, exit code 4), but its record could not be written", b""])
        assert lines[-2] == b"[run supervisor] the run ended (finished, exit code 4), but its record could not be written"
        assert run.record()["state"] == "starting"
        captured = capfd.readouterr()
        assert captured.out == "" and captured.err == ""
        assert b"Traceback" not in run.log() and PLANTED.encode() not in run.log()

    def test_a_record_that_already_holds_an_end_is_left_as_it_is(self, new_run, root, fake_run_docker):
        """Bug class: the supervisor writing its end over one that is already in the record (written by a cancel that believed the supervisor gone): the record every reader took as final would change. The store refuses, and the supervisor says so in the log."""
        run = new_run().start("lasts")
        run.child_pid()
        wait_for("the record to say running", lambda: run.record()["state"] == "running")
        runstore.update_record(root, run.run_id, state="cancelled", ended="2026-11-21T10:20:00Z")
        before = (run.folder / "record.json").read_bytes()

        run.go(4)
        assert run.join() == 1

        assert (run.folder / "record.json").read_bytes() == before
        assert run.log() == CHILD_LINES + b"[run supervisor] the record of the run already holds an end: it is left as it is\n"

    def test_a_child_that_cannot_be_started(self, new_run, root, tmp_path, capfd, fake_run_docker):
        """Bug class: a run that stays `starting` for ever under a supervisor that died on the error, or a traceback with the request in it, when the child's program cannot be started."""
        run = new_run()

        code = supervise.supervise(root, run.run_id, run.lock_fd, REQUEST, child_argv=[str(tmp_path / "no-such-program")], poll_seconds=0.02)

        assert code == 0
        assert run.log() == b"[run supervisor] the run could not be started (ENOENT)\n"
        record = run.record()
        assert (record["state"], record["exit_code"]) == ("finished", 1) and record["ended"] is not None
        captured = capfd.readouterr()
        assert captured.out == "" and captured.err == ""

    def test_an_unexpected_error_names_its_kind_only(self, new_run, root, monkeypatch, capfd, fake_run_docker):
        """Bug class: an error nobody foresaw written with its text or its traceback into the log, or onto the supervisor's own output: either can hold the request."""
        run = new_run()

        def broken(*args, **kwargs):
            raise RuntimeError(f"failed with {REQUEST!r}")

        monkeypatch.setattr(supervise.subprocess, "Popen", broken)

        code = supervise.supervise(root, run.run_id, run.lock_fd, REQUEST, child_argv=run.child_argv("exit:0"), poll_seconds=0.02)

        assert code == 1
        assert run.log() == b"[run supervisor] the supervisor stopped on an error (RuntimeError)\n"
        captured = capfd.readouterr()
        assert captured.out == "" and captured.err == ""

    @pytest.mark.parametrize("request_", [b"x" * 65537, REQUEST.decode(), None], ids=["too-large", "text", "none"])
    def test_a_request_that_is_no_request_starts_nothing(self, new_run, root, fake_run_docker, request_):
        """Bug class: a request larger than a request may be, or one that is not bytes, handed on to a child (a write that waits for ever on a full pipe, a TypeError half way) instead of ending the run with a line that says so."""
        run = new_run()

        code = supervise.supervise(root, run.run_id, run.lock_fd, request_, child_argv=run.child_argv("exit:0"), poll_seconds=0.02)

        assert code == 0
        assert run.log() == b"[run supervisor] the request is larger than a request may be\n"
        assert (run.record()["state"], run.record()["exit_code"]) == ("finished", 1)
        assert os.listdir(run.out) == []

    def test_a_run_that_does_not_exist(self, root, fake_run_docker):
        """Bug class: a supervisor started for an id that names no run creating a folder for it, or ending in a traceback."""
        assert supervise.supervise(root, "r20261121T101500-0a1b2c", 0, REQUEST) == 2
        assert supervise.supervise(root, "../x", 0, REQUEST) == 2
        assert list(root.iterdir()) == []


# ── the entry point ─────────────────────────────────────────────────────────


class TestEntryPoint:
    @pytest.mark.parametrize("argv", [[], ["r20261121T101500-0a1b2c"], ["r20261121T101500-0a1b2c", "x"], ["r20261121T101500-0a1b2c", "-1"], ["r20261121T101500-0a1b2c", "3", "4"], ["r20261121T101500-0a1b2c", "1" * 10]], ids=repr)
    def test_arguments_that_are_not_a_run_id_and_a_descriptor(self, capsys, argv):
        """Bug class: the entry point reading standard input, or installing its signal handlers, for a call it cannot carry out; or a traceback for a descriptor that is no number."""
        handlers = [signal.getsignal(number) for number in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)]

        assert supervise.main(argv) == 2

        assert [signal.getsignal(number) for number in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)] == handlers
        captured = capsys.readouterr()
        assert captured.out == "" and captured.err == "usage: python -m social_data_pipeline.control.supervise <run_id> <lock_fd>, with the request on standard input\n"

    def test_an_error_outside_the_supervision_names_its_kind_only(self, new_run, root, tmp_path):
        """Bug class: a traceback on the supervisor's standard error, which is the run's log: its frames hold the request. An error that is raised outside the supervision itself is one line with the kind of error, and exit code 1."""
        run = new_run()
        script = tmp_path / "broken_supervisor.py"
        script.write_text(
            "import sys\nfrom social_data_pipeline.control import supervise\n\n\ndef broken(*args, **kwargs):\n    raise RuntimeError(repr(args))\n\n\n"
            "supervise.supervise = broken\nsys.argv[1:] = sys.argv[1:3]\nsys.exit(supervise.main())\n"
        )

        result = subprocess.run([sys.executable, str(script), run.run_id, str(run.lock_fd)], cwd=root, env={**os.environ, "PYTHONPATH": str(REPO_ROOT), "PYTHONDONTWRITEBYTECODE": "1"}, input=REQUEST, capture_output=True, timeout=60, pass_fds=(run.lock_fd,))

        assert (result.returncode, result.stdout, result.stderr) == (1, b"", b"[run supervisor] the supervisor stopped on an error (RuntimeError)\n")

    def test_the_command_of_the_child_and_the_times(self):
        """Bug class: every test of this file replaces the child's command and shortens the times, so the real ones are pinned here: the child is the generic runner, started unbuffered (its output and that of the processes it starts go to the same file and must stay in order), a cancel request is looked for about twice a second, and a process gets ten seconds to end after TERM."""
        assert supervise.CHILD_ARGV == (sys.executable, "-u", "-m", "social_data_pipeline.control.child")
        assert (supervise.POLL_SECONDS, supervise.GRACE_SECONDS, supervise.DOCKER_ATTEMPTS) == (0.5, 10, 10)
