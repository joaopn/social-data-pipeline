"""Tests for `control.runs.start_run`, which creates a recorded pipeline run and starts the process that supervises it.

The contracts pinned here:

- A request the registry refuses, a starter label that is no label and a folder that is not the CLI's deployment are refused before anything exists.
- A second run of the same profile for the same source is refused while the first is alive (recorded and not ended, or started in a terminal), also when two calls come at the same moment; with Docker out of reach the run is refused, not guessed.
- From the moment the record exists, the run's lock is held: first by the starter, then by the supervisor, which inherits the descriptor; the starter keeps no copy.
- The record's flags hold the request's fields that are no secret, without the profile and the source; the secret is in no file, and in neither the argument list nor the environment of the supervisor, which gets the request on its standard input.
- When the supervisor cannot be started, the run is recorded as finished with exit code 1 and one line in its log, and the error is raised. A supervisor that starts and fails at once leaves its own error text in the run's log, which is its standard output and standard error.
- Everything `start_run` raises beyond the registry's refusals and a bad starter label is a `RunNotStarted` with a constant message and a code.

The supervisor is replaced by a small program in the temp folder that reads its standard input, keeps the inherited descriptor open and waits to be killed; Docker is replaced by the fake. Linux is assumed: `/proc` is read.
"""

from __future__ import annotations

import hashlib
import json
import os
import signal
import subprocess
import sys
import threading
import time

import pytest

import sdp
from social_data_pipeline import runstore
from social_data_pipeline.control import errors, runs

PLANTED = "planted-made-up-secret-9d2e"
CONTAINER = "a1" * 32
RUN_OLD = "r20261120T101500-0a1b2c"

RUN_ACTIVE = "a run of this profile for this source is already active"
DOCKER_NOT_REACHABLE = "Docker cannot be reached, so it cannot be told whether a run of this profile for this source is already active"
START_FAILED = "the process that supervises the run could not be started"

# No test of this file reaches the real Docker: the store's Docker function is replaced, and a test that reaches the replacement fails.
pytestmark = [pytest.mark.usefixtures("run_docker_guard"), pytest.mark.skipif(not sys.platform.startswith("linux"), reason="reads /proc and uses flock")]

# The supervisor's stand-in: it reads the request to the end, leaves its length and hash, its argument list and its process id in files named after the run, and then waits with the inherited descriptor open.
STAND_IN = r'''
import hashlib, json, os, sys, time

out, run_id, lock_fd = sys.argv[1], sys.argv[2], int(sys.argv[3])
request = sys.stdin.buffer.read()
os.fstat(lock_fd)


def note(name, text):
    with open(os.path.join(out, run_id + "." + name + ".tmp"), "w") as handle:
        handle.write(text)
    os.replace(os.path.join(out, run_id + "." + name + ".tmp"), os.path.join(out, run_id + "." + name))


note("stdin", f"{len(request)} {hashlib.sha256(request).hexdigest()}")
note("argv", json.dumps(sys.argv))
note("pid", str(os.getpid()))
time.sleep(600)
'''


# ── helpers ─────────────────────────────────────────────────────────────────


def wait_for(what, probe, seconds=20):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        found = probe()
        if found:
            return found
        time.sleep(0.01)
    raise AssertionError(f"timed out waiting for {what}")


class Deployment:
    def __init__(self, root, out, docker):
        self.root = root
        self.out = out
        self.docker = docker

    def run_ids(self):
        folder = self.root / ".sdp" / "runs"
        return sorted(os.listdir(folder)) if folder.exists() else []

    def note(self, run_id, name):
        path = self.out / f"{run_id}.{name}"
        return wait_for(f"the supervisor's note {name}", lambda: path.read_text() if path.exists() else None)

    def record(self, run_id):
        return runstore.read_record(self.root, run_id)

    def make_run(self, run_id, state, *, profile="parse", source="ok", **changes):
        """A recorded run made through the store, moved to a state."""
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(runstore, "new_run_id", lambda: run_id)
            assert runstore.create_run(self.root, {"profile": profile, "source": source, "flags": {}, "started_by": "operator"}) == run_id
        if state != "starting" or changes:
            runstore.update_record(self.root, run_id, state=state, **changes)
        return run_id


@pytest.fixture
def deployment(source_tree, tmp_path, monkeypatch, fake_run_docker):
    """A temp deployment with the source `ok` that the CLI points at, the supervisor replaced by the stand-in and Docker by the fake. Every supervisor that was started is killed and collected when the test is over."""
    out = tmp_path / "supervisor-notes"
    out.mkdir()
    script = tmp_path / "stand_in_supervisor.py"
    script.write_text(STAND_IN)
    monkeypatch.setattr(sdp, "ROOT", source_tree)
    monkeypatch.setattr(runs, "SUPERVISOR_ARGV", (sys.executable, str(script), str(out)))
    monkeypatch.setattr(runstore, "_LOCK_RETRY_SECONDS", 0)
    yield Deployment(source_tree, out, fake_run_docker)
    with runs._supervisors_lock:
        started = list(runs._supervisors)
        runs._supervisors.clear()
    for process in started:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=30)


def open_descriptors():
    return sorted(os.listdir("/proc/self/fd"))


FIELDS = {"profile": "parse", "source": "ok"}


# ── refused before anything exists ──────────────────────────────────────────


class TestRefusedBeforeAnythingExists:
    @pytest.mark.parametrize("fields, error", [
        ({"profile": "parse"}, errors.InputError),
        ({"profile": "parse", "source": "no-such-source"}, errors.InputError),
        ({"profile": "everything", "source": "ok"}, errors.InputError),
        ({"profile": "parse", "source": "ok", "data_path": "/srv"}, errors.InputError),
        ({"profile": "parse", "source": "ok", "_run_id": "r20261121T101500-0a1b2c"}, errors.InputError),
        ({"profile": "sr_ingest", "source": "ok", "filter": "RS_*"}, errors.InputError),
        ({"profile": "mongo_ingest", "source": "ok", "admin_password": PLANTED + "\nMONGO_PORT=1"}, errors.InputError),
        ([PLANTED], errors.InputError),
    ], ids=["no-source", "unknown-source", "unknown-profile", "unlisted-field", "private-parameter", "cross-field-rule", "bad-secret", "not-an-object"])
    def test_a_request_the_registry_refuses_creates_nothing(self, deployment, fields, error):
        """Bug class: a run folder, a record or a supervisor for a request that is then refused: a record of a run that never was, and the unchecked request (a field the registry does not list, a source that is not configured) already on its way to a process. The refusal is the registry's, holds no value, and comes before the state folder exists and before Docker is asked."""
        with pytest.raises(error) as exc:
            runs.start_run(deployment.root, fields, started_by="operator")

        assert PLANTED not in str(exc.value)
        assert not (deployment.root / ".sdp").exists()
        assert deployment.docker.calls == [] and os.listdir(deployment.out) == []

    @pytest.mark.parametrize("started_by", ["", "two words", "line\nbreak", "x" * 65, "-leading", None, 7, ["operator"]], ids=repr)
    def test_a_starter_label_that_is_no_label_creates_nothing(self, deployment, started_by):
        """Bug class: text that someone else wrote (a browser's user name, with a line break or a control character) stored in the record as the starter and printed by `runs list`."""
        with pytest.raises(ValueError) as exc:
            runs.start_run(deployment.root, FIELDS, started_by=started_by)

        assert str(exc.value) == "started_by must be a short label of letters, digits, '_', '.' and '-'"
        assert not (deployment.root / ".sdp").exists() and deployment.docker.calls == []

    def test_a_folder_that_is_not_the_clis_deployment_creates_nothing(self, deployment, tmp_path, monkeypatch):
        """Bug class: a run recorded in one folder while the child acts on another: the child runs the CLI module it finds from its working directory, on that module's own folder. The folder must be the CLI's, compared after resolving links."""
        other = tmp_path / "another-deployment"
        other.mkdir()
        (tmp_path / "linked").symlink_to(deployment.root, target_is_directory=True)

        with pytest.raises(errors.RunNotStarted) as exc:
            runs.start_run(other, FIELDS, started_by="operator")

        assert (exc.value.code, exc.value.message) == ("not_the_deployment", "the folder is not the deployment folder of the CLI")
        assert list(other.iterdir()) == [] and not (deployment.root / ".sdp").exists()
        run_id = runs.start_run(tmp_path / "linked", FIELDS, started_by="operator")
        assert deployment.run_ids() == [run_id]


# ── a second run ────────────────────────────────────────────────────────────


class TestASecondRun:
    @pytest.mark.parametrize("alive", ["starting", "running", "detached", "terminal", "terminal-source-not-known"])
    def test_a_run_that_is_alive_refuses_a_second_one(self, deployment, alive):
        """Bug class: two runs of the same profile on the same source at the same time, writing the same files: the first one recorded and supervised (`starting`, `running`), recorded with a living container and no supervisor (`detached`), or started with `sdp run` in a terminal. A terminal run whose source Docker did not hand over cannot be told apart and counts. The refusal has a constant message, and no run is created and no supervisor started for it."""
        held = None
        if alive in ("starting", "running"):
            deployment.make_run(RUN_OLD, alive)
            held = runstore.acquire_run_lock(deployment.root, RUN_OLD)
        elif alive == "detached":
            deployment.make_run(RUN_OLD, "running")
            deployment.docker.add(CONTAINER, deployment.root, run=RUN_OLD)
        else:
            deployment.docker.add(CONTAINER, deployment.root, service="parse", source="ok" if alive == "terminal" else None)
        before = deployment.run_ids()

        with pytest.raises(errors.RunNotStarted) as exc:
            runs.start_run(deployment.root, FIELDS, started_by="operator")

        assert (exc.value.code, exc.value.message, str(exc.value)) == ("run_active", RUN_ACTIVE, RUN_ACTIVE)
        assert deployment.run_ids() == before and os.listdir(deployment.out) == []
        assert deployment.docker.named("stop") == []
        if held is not None:
            runstore.release_run_lock(held)

    @pytest.mark.parametrize("other", ["finished", "cancelled", "lost", "another-profile", "another-source", "terminal-another-source", "terminal-another-profile", "unreadable", "none"])
    def test_a_run_that_is_over_or_is_another_one_refuses_nothing(self, deployment, other):
        """Bug class: a refusal that never ends: a run that finished, was cancelled or was lost (its supervisor and its container are gone) still counted as alive, so the profile can never be run again for that source; or a run of another profile or another source, or a record that cannot be read, standing in the way."""
        held = None
        if other in ("finished", "cancelled"):
            deployment.make_run(RUN_OLD, other, ended="2026-11-20T10:20:00Z", exit_code=0)
        elif other == "lost":
            deployment.make_run(RUN_OLD, "running")
        elif other in ("another-profile", "another-source"):
            deployment.make_run(RUN_OLD, "running", profile="lingua" if other == "another-profile" else "parse", source="ok" if other == "another-profile" else "reddit")
            held = runstore.acquire_run_lock(deployment.root, RUN_OLD)
        elif other == "terminal-another-source":
            deployment.docker.add(CONTAINER, deployment.root, service="parse", source="reddit")
        elif other == "terminal-another-profile":
            deployment.docker.add(CONTAINER, deployment.root, service="lingua", source="ok")
        elif other == "unreadable":
            deployment.make_run(RUN_OLD, "running")
            (deployment.root / ".sdp" / "runs" / RUN_OLD / "record.json").write_text("{")

        run_id = runs.start_run(deployment.root, FIELDS, started_by="operator")

        assert runstore.run_id_kind(run_id) == "web" and run_id in deployment.run_ids()
        assert deployment.record(run_id)["state"] == "starting"
        deployment.note(run_id, "pid")
        if held is not None:
            runstore.release_run_lock(held)

    def test_with_docker_out_of_reach_the_run_is_refused(self, deployment):
        """Bug class: a run started on the guess that no other run is alive, when Docker cannot be asked: a run started in a terminal is found through Docker only."""
        deployment.docker.reachable = False

        with pytest.raises(errors.RunNotStarted) as exc:
            runs.start_run(deployment.root, FIELDS, started_by="operator")

        assert (exc.value.code, exc.value.message) == ("docker_not_reachable", DOCKER_NOT_REACHABLE)
        assert deployment.run_ids() == [] and os.listdir(deployment.out) == []

    def test_every_recorded_run_is_looked_at_not_the_newest_few(self, deployment):
        """Bug class: the look for a living run made through a listing that is cut to its newest entries: a long run that started before many short ones would no longer be seen, and a second run of its profile would be started next to it."""
        deployment.make_run("r20261101T000000-000000", "running")
        held = runstore.acquire_run_lock(deployment.root, "r20261101T000000-000000")
        for number in range(60):
            deployment.make_run(f"r20261120T1015{number:02d}-0a1b2c", "finished", ended="2026-11-20T10:20:00Z", exit_code=0)

        with pytest.raises(errors.RunNotStarted) as exc:
            runs.start_run(deployment.root, FIELDS, started_by="operator")

        assert exc.value.code == "run_active"
        runstore.release_run_lock(held)

    def test_two_calls_at_the_same_moment_start_one_run(self, deployment, monkeypatch):
        """Bug class: two requests at the same moment (the button pressed twice, two browser tabs) both finding no living run and both starting one. The look and the creation happen under the lock of the whole store; the creation is slowed down here, so that without that lock both calls would have looked before either had created its run."""
        real = runstore.create_run_locked

        def slow(root, fields):
            time.sleep(0.3)
            return real(root, fields)

        monkeypatch.setattr(runstore, "create_run_locked", slow)
        barrier = threading.Barrier(2)
        outcomes = []

        def call():
            barrier.wait(timeout=20)
            try:
                outcomes.append(("started", runs.start_run(deployment.root, FIELDS, started_by="operator")))
            except errors.RunNotStarted as error:
                outcomes.append(("refused", error.code))

        threads = [threading.Thread(target=call, daemon=True) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(30)
            assert not thread.is_alive()

        assert sorted(kind for kind, _ in outcomes) == ["refused", "started"]
        assert dict(outcomes)["refused"] == "run_active"
        assert deployment.run_ids() == [dict(outcomes)["started"]]


# ── the lock, the record and the supervisor ─────────────────────────────────


class TestTheRunThatIsStarted:
    def test_the_lock_is_held_from_the_record_on_and_handed_to_the_supervisor(self, deployment, monkeypatch):
        """Bug class: a moment in which the record exists and nobody holds the run's lock, between the creation of the run and the start of its supervisor: a listing in that moment says `lost`, and a cancel in that moment takes the lock and records the run as cancelled while its supervisor is about to start it. The state is read at the moment the supervisor is started, and again when it runs; and once the supervisor is the only holder, its end frees the lock, so the starter kept no copy."""
        seen = []
        at_the_first_write = []
        real = runs._spawn_supervisor
        real_write = runstore._write_record

        def write(run_fd, record):
            if not at_the_first_write:
                at_the_first_write.append(("free", runstore._lock_is_free(run_fd)))
            real_write(run_fd, record)

        def spawn(root, run_id, lock_fd, request):
            seen.append((runstore.read_run(root, run_id)["state"], runstore.acquire_run_lock(root, run_id), runstore.holds_run_lock(root, run_id, lock_fd)))
            return real(root, run_id, lock_fd, request)

        monkeypatch.setattr(runstore, "_write_record", write)
        monkeypatch.setattr(runs, "_spawn_supervisor", spawn)
        run_id = runs.start_run(deployment.root, FIELDS, started_by="operator")

        # The lock was held when the record was first written, and still when the supervisor was started.
        assert at_the_first_write == [("free", False)]
        assert seen == [("starting", None, True)]
        pid = int(deployment.note(run_id, "pid"))
        assert runstore.read_run(deployment.root, run_id)["state"] == "starting"
        assert runstore.acquire_run_lock(deployment.root, run_id) is None
        # Only the listing for runs started in a terminal was asked of Docker: a run whose lock is held is not looked up there.
        assert all("label=sdp.run=" + run_id not in call for call in deployment.docker.calls)

        os.kill(pid, signal.SIGKILL)
        wait_for("the lock to be free once the supervisor is gone", lambda: runstore.read_run(deployment.root, run_id)["state"] == "lost")

    def test_the_starter_keeps_no_descriptor(self, deployment):
        """Bug class: a descriptor left open in the process that starts runs for every run it starts: the lock descriptor (the run would read as supervised for as long as that process lives, whatever happens to its supervisor), or an end of the pipe the request went through."""
        runs.start_run(deployment.root, {"profile": "lingua", "source": "ok"}, started_by="operator")
        before = open_descriptors()

        run_id = runs.start_run(deployment.root, FIELDS, started_by="operator")

        assert open_descriptors() == before
        deployment.note(run_id, "pid")

    def test_the_record_holds_the_fields_that_are_no_secret(self, deployment):
        """Bug class: the admin password written into the record, which `runs list --json` prints and a browser later shows; or the flags holding the profile and the source a second time, or missing a flag the run was started with."""
        fields = {"profile": "mongo_ingest", "source": "ok", "build": True, "filter": "RS_2024-*", "admin_password": PLANTED}

        run_id = runs.start_run(deployment.root, fields, started_by="session-7")

        record = deployment.record(run_id)
        assert record == {"id": run_id, "profile": "mongo_ingest", "source": "ok", "flags": {"build": True, "filter": "RS_2024-*"}, "started_by": "session-7", "state": "starting", "started": record["started"], "ended": None, "exit_code": None}
        deployment.note(run_id, "pid")
        for path in sorted((deployment.root / ".sdp").rglob("*")):
            if path.is_file():
                assert PLANTED.encode() not in path.read_bytes() and b"admin_password" not in path.read_bytes(), path

    def test_the_supervisor_gets_the_request_on_standard_input_only(self, deployment):
        """Bug class: the admin password in the supervisor's argument list (readable by every user of the host) or in its environment (inherited by the child, by `docker compose` and readable in `/proc`); or a supervisor that gets something else than the validated request, or not the run id and the lock descriptor. The stand-in reports the hash of what it read, which must be the request with the plain password and the run id."""
        fields = {"profile": "mongo_ingest", "source": "ok", "build": True, "filter": "RS_2024-*", "admin_password": PLANTED}

        run_id = runs.start_run(deployment.root, fields, started_by="operator")

        pid = int(deployment.note(run_id, "pid"))
        expected = json.dumps({"verb": "run.start", "fields": fields, "run_id": run_id}).encode()
        assert deployment.note(run_id, "stdin") == f"{len(expected)} {hashlib.sha256(expected).hexdigest()}"
        argv = json.loads(deployment.note(run_id, "argv"))
        assert argv[2] == run_id and argv[3].isdigit() and len(argv) == 4
        for name in ("cmdline", "environ"):
            with open(f"/proc/{pid}/{name}", "rb") as handle:
                content = handle.read()
            assert content and PLANTED.encode() not in content and b"admin_password" not in content, name
        with open(f"/proc/{pid}/stat") as handle:
            session = int(handle.read().rsplit(")", 1)[1].split()[3])
        # A session of its own: the end of the starter's session does not end the supervisor.
        assert session == pid and session != os.getsid(0)
        assert os.readlink(f"/proc/{pid}/cwd") == os.path.realpath(deployment.root)
        # What the supervisor itself prints goes into the run's log, opened for appending.
        log = os.path.realpath(deployment.root / ".sdp" / "runs" / run_id / "log")
        assert os.readlink(f"/proc/{pid}/fd/1") == log and os.readlink(f"/proc/{pid}/fd/2") == log
        with open(f"/proc/{pid}/fdinfo/1") as handle:
            flags = int(next(line for line in handle if line.startswith("flags:")).split()[1], 8)
        assert flags & os.O_APPEND
        assert (deployment.root / ".sdp" / "runs" / run_id / "log").read_bytes() == b""

    def test_the_real_supervisor_is_the_module_of_this_package(self):
        """Bug class: every other test of this file replaces the supervisor's command, so the real one is pinned here: the module is run with the interpreter of the starter."""
        assert runs.SUPERVISOR_ARGV == (sys.executable, "-m", "social_data_pipeline.control.supervise")


# ── a supervisor that cannot be started ─────────────────────────────────────


class TestASupervisorThatCannotBeStarted:
    @pytest.mark.parametrize("failure", ["no-program", "request-not-written"])
    def test_the_run_is_recorded_as_failed_and_the_error_is_raised(self, deployment, tmp_path, monkeypatch, failure):
        """Bug class: a run left in `starting` for ever (it would read as `lost`, and nothing says why) when its supervisor could not be started, or when the request could not be handed to it; a supervisor without a request left running; the lock, or the pipe the request was written to, kept open by the starter; or the failure swallowed, so the caller believes the run is on its way. The second case is a real broken pipe with a supervisor that is alive: it has closed its standard input before the request is written, and the request is larger than the buffer of the pipe's writer (a password of characters that JSON writes as twelve bytes each), so the write itself fails. That supervisor must be ended and collected by the starter."""
        started = []
        password = PLANTED
        if failure == "no-program":
            monkeypatch.setattr(runs, "SUPERVISOR_ARGV", (str(tmp_path / "no-such-program"),))
        else:
            password = "\U0001F600" * 1024
            closed = tmp_path / "standard-input-closed"
            deaf = "import os, sys, time\nos.close(0)\nopen(sys.argv[1], 'w').close()\ntime.sleep(600)\n"
            monkeypatch.setattr(runs, "SUPERVISOR_ARGV", (sys.executable, "-c", deaf, str(closed)))
            real = subprocess.Popen

            def popen(*args, **kwargs):
                process = real(*args, **kwargs)
                started.append(process)
                wait_for("the supervisor to have closed its standard input", closed.exists)
                return process

            monkeypatch.setattr(runs.subprocess, "Popen", popen)
        before = open_descriptors()

        try:
            with pytest.raises(errors.RunNotStarted) as exc:
                runs.start_run(deployment.root, {**FIELDS, "admin_password": password}, started_by="operator")
            ended = [process.returncode for process in started]
        finally:
            for process in started:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=30)

        assert (exc.value.code, exc.value.message) == ("start_failed", START_FAILED)
        assert exc.value.__cause__ is None and PLANTED not in repr(exc.value)
        (run_id,) = deployment.run_ids()
        record = deployment.record(run_id)
        assert (record["state"], record["exit_code"]) == ("finished", 1) and record["ended"] is not None
        assert (deployment.root / ".sdp" / "runs" / run_id / "log").read_bytes() == b"[run start] the process that supervises the run could not be started\n"
        assert open_descriptors() == before
        taken = runstore.acquire_run_lock(deployment.root, run_id)
        assert isinstance(taken, int)
        runstore.release_run_lock(taken)
        # The supervisor that was alive was killed and collected by the starter, and the pipe to it is closed.
        assert ended == ([] if failure == "no-program" else [-signal.SIGKILL])
        assert all(process.stdin.closed for process in started) and runs.reap_supervisors() == 0

    def test_a_supervisor_that_fails_at_once_leaves_its_reason_in_the_log(self, deployment, monkeypatch):
        """Bug class: a supervisor that dies before it records anything (a damaged installation, an error while it loads) leaving no trace: its output went to the null device, and the run reads as `lost` with nobody able to say why. Its standard output and standard error are the run's log; the stand-in here takes the request and fails with a made-up message."""
        failing = "import sys\nsys.stdin.buffer.read()\nprint('made-up line on standard output', flush=True)\nsys.exit('made-up failure of the supervisor')\n"
        monkeypatch.setattr(runs, "SUPERVISOR_ARGV", (sys.executable, "-c", failing))

        run_id = runs.start_run(deployment.root, {**FIELDS, "admin_password": PLANTED}, started_by="operator")

        wait_for("the supervisor to have ended", lambda: runs.reap_supervisors() == 0)
        log = (deployment.root / ".sdp" / "runs" / run_id / "log").read_bytes()
        assert log == b"made-up line on standard output\nmade-up failure of the supervisor\n"
        assert runstore.read_run(deployment.root, run_id)["state"] == "lost" and deployment.record(run_id)["state"] == "starting"


class TestEveryFailureIsTyped:
    def test_a_cli_module_that_cannot_be_loaded(self, deployment, monkeypatch):
        """Bug class: an ImportError (or whatever a damaged CLI module raises while it loads) escaping `start_run` untyped: the caller handles the registry's errors and `RunNotStarted`, and anything else is a crash of the process that serves the browser, with a traceback whose frames hold the request."""
        monkeypatch.setitem(sys.modules, "sdp", None)

        with pytest.raises(errors.RunNotStarted) as exc:
            runs.start_run(deployment.root, {**FIELDS, "admin_password": PLANTED}, started_by="operator")

        assert (exc.value.code, exc.value.message) == ("cli_not_loadable", "the CLI module cannot be loaded")
        assert exc.value.__cause__ is None and not (deployment.root / ".sdp").exists() and os.listdir(deployment.out) == []

    def test_fields_the_store_cannot_record(self, deployment, monkeypatch):
        """Bug class: a ValueError of the run store (fields a record cannot hold, a record that would be too large) escaping `start_run` untyped, its text passed on to the caller."""
        def refuse(root, fields):
            raise ValueError(f"the fields of the run are not what a record can hold: {PLANTED}")

        monkeypatch.setattr(runstore, "create_run_locked", refuse)

        with pytest.raises(errors.RunNotStarted) as exc:
            runs.start_run(deployment.root, {**FIELDS, "admin_password": PLANTED}, started_by="operator")

        assert (exc.value.code, exc.value.message) == ("record_not_possible", "the fields of the run are not what a record can hold")
        assert exc.value.__cause__ is None and PLANTED not in repr(exc.value)
        assert deployment.run_ids() == [] and os.listdir(deployment.out) == []


class TestSupervisorsAreCollected:
    def test_an_ended_supervisor_does_not_stay_in_the_process_table(self, deployment):
        """Bug class: every supervisor that ended staying as a dead entry in the process table of the long-lived process that started it, one per run, until that process ends."""
        run_id = runs.start_run(deployment.root, FIELDS, started_by="operator")
        pid = int(deployment.note(run_id, "pid"))
        assert runs.reap_supervisors() == 1

        os.kill(pid, signal.SIGKILL)

        wait_for("the supervisor to be collected", lambda: runs.reap_supervisors() == 0)
        assert not os.path.exists(f"/proc/{pid}")
