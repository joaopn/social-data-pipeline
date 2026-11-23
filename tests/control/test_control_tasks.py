"""Tests for tasks: CLI actions that are not a pipeline profile, started as recorded runs by `control.runs.start_task` and supervised by `control.supervise` (the allow-list is `control.tasks.TASKS`).

The contracts pinned here:

- Only a verb of the allow-list is started, and one task of a verb per value of its key fields is alive at a time.
- The record names the verb and the label, holds the request's fields that are no secret, and never a secret; the secret reaches the child over standard input only, through the supervisor, and is in no argument list or environment of either.
- The supervisor never asks Docker about a task: it ends when the child's process group has ended, and a cancel sends that group TERM, then KILL after the grace time, and records `cancelled` with the line that says what a cancel does not undo.
- A task whose supervisor is gone reads `lost` without Docker, and a cancel of it is refused.
- The allow-list fits the registry.

The task runs for real: the real supervisor module and the real child, each started through a small wrapper in the temp folder that adds a made-up verb `test.task` to the registry and its action to the CLI module (the child), and shortens the supervisor's grace time. Docker is the fake in this process; in the supervisor, a call of the store's Docker function is written to a file, which must stay absent. Linux is assumed: `/proc` is read.
"""

from __future__ import annotations

import json
import os
import signal
import sys
import time
from pathlib import Path
from types import MappingProxyType

import pytest

import sdp
from social_data_pipeline import runstore
from social_data_pipeline.control import errors, runs, tasks, verbs

REPO_ROOT = Path(__file__).resolve().parents[2]
PLANTED = "planted-made-up-token-5c1e"
TASK_VERB = "test.task"
END_LINE = "[run supervisor] cancelled: the action's processes were sent TERM, and KILL after the grace time. What it had done until then is not undone: files it wrote stay, and work it handed to a database (an index being built through docker compose exec) may go on inside the database.\n"

pytestmark = [pytest.mark.usefixtures("run_docker_guard"), pytest.mark.skipif(not sys.platform.startswith("linux"), reason="reads /proc and uses flock")]

# Run in this process and in the child: the made-up verb, its allow-list entry and its action. The action says what it works on (never the token, only that it has one), leaves its process id and that of a process it starts in its own group, and waits.
TEST_TASK = r'''
import os, signal, subprocess, sys, time
from types import MappingProxyType

import sdp
from social_data_pipeline.control import tasks, verbs
from social_data_pipeline.control.types import Bool, IntRange, Name, Secret

TEST_VERB = verbs.Verb(
    name="test.task", cls="write", cli=("test", "task"),
    socket_inputs={"source": verbs.Field(Name(), required=True), "seconds": verbs.Field(IntRange(0, 120)), "ignore_term": verbs.Field(Bool()), "token": verbs.Field(Secret())},
    socket_enabled=True, action="do_test_task",
)
TEST_TASK = tasks.Task("test.task", key=("source",), label="test \u00b7 {source}")


def do_test_task(*, source, seconds=0, ignore_term=False, token=None):
    notes = os.environ.get("TEST_TASK_NOTES")
    print(f"working on {source}", flush=True)
    print("with a token" if token else "without a token", flush=True)
    if ignore_term:
        # Inherited by the process started below as well.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    helper = subprocess.Popen(["sleep", "600"]) if seconds else None
    if notes:
        with open(os.path.join(notes, "pids.tmp"), "w") as handle:
            handle.write(f"{os.getpid()} {helper.pid if helper else 0}")
        os.replace(os.path.join(notes, "pids.tmp"), os.path.join(notes, "pids"))
    time.sleep(seconds)
    if helper:
        helper.kill()
        helper.wait()
    print("done", flush=True)
    return 3


def install():
    verbs.VERBS = MappingProxyType({**verbs.VERBS, "test.task": TEST_VERB})
    tasks.TASKS = MappingProxyType({"test.task": TEST_TASK})
    sdp.do_test_task = do_test_task
'''

CHILD_WRAPPER = r'''
import sys
sys.path.insert(0, sys.argv[1])
sys.path.insert(0, sys.argv[2])
import test_task_module
test_task_module.install()
from social_data_pipeline.control import child
sys.exit(child.main())
'''

SUPERVISOR_WRAPPER = r'''
import os, sys
repo, folder = sys.argv[1], sys.argv[2]
sys.path.insert(0, repo)
from social_data_pipeline import runstore
from social_data_pipeline.control import supervise


def docker(*args, **kwargs):
    with open(os.path.join(folder, "docker-called"), "a") as handle:
        handle.write(repr(args) + "\n")
    raise runstore.DockerUnavailable(runstore.DOCKER_NOT_REACHABLE)


runstore._docker = docker
supervise.CHILD_ARGV = (sys.executable, "-u", os.path.join(folder, "child_wrapper.py"), repo, folder)
supervise.GRACE_SECONDS = 1
supervise.POLL_SECONDS = 0.1
sys.exit(supervise.main(sys.argv[3:]))
'''


def wait_for(what, probe, seconds=30):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        found = probe()
        if found:
            return found
        time.sleep(0.02)
    raise AssertionError(f"timed out waiting for {what}")


def alive(pid):
    """Whether a process runs (a zombie does not)."""
    try:
        with open(f"/proc/{pid}/stat") as handle:
            return handle.read().rsplit(")", 1)[1].split()[0] != "Z"
    except FileNotFoundError:
        return False


class TaskDeployment:
    def __init__(self, root, folder, docker):
        self.root = root
        self.folder = folder
        self.docker = docker

    def start(self, fields, started_by="web-1"):
        return runs.start_task(self.root, TASK_VERB, fields, started_by=started_by)

    def entry(self, run_id):
        return runstore.read_run(self.root, run_id)

    def ended(self, run_id):
        return wait_for("the task to end", lambda: self.entry(run_id)["state"] in runstore.FINAL_STATES and runs.reap_supervisors() == 0 and self.entry(run_id))

    def log(self, run_id):
        return (self.root / ".sdp" / "runs" / run_id / "log").read_text()

    def pids(self):
        path = self.folder / "pids"
        return tuple(int(number) for number in wait_for("the action's process ids", lambda: path.exists() and path.read_text()).split())

    def docker_reached_from_a_supervisor(self):
        return (self.folder / "docker-called").exists()


@pytest.fixture
def deployment(tmp_path, monkeypatch, fake_run_docker):
    """A temp deployment the CLI points at, the made-up task installed in this process, and the supervisor's command replaced by the wrapper. Every supervisor started is killed and collected at the end, and the action's processes with it."""
    root = tmp_path.resolve() / "deployment"
    root.mkdir()
    folder = tmp_path.resolve() / "task"
    folder.mkdir()
    (folder / "test_task_module.py").write_text(TEST_TASK)
    (folder / "child_wrapper.py").write_text(CHILD_WRAPPER)
    (folder / "supervisor_wrapper.py").write_text(SUPERVISOR_WRAPPER)
    monkeypatch.syspath_prepend(str(folder))
    import test_task_module

    monkeypatch.setattr(verbs, "VERBS", MappingProxyType({**verbs.VERBS, TASK_VERB: test_task_module.TEST_VERB}))
    monkeypatch.setattr(tasks, "TASKS", MappingProxyType({TASK_VERB: test_task_module.TEST_TASK}))
    monkeypatch.setattr(sdp, "do_test_task", test_task_module.do_test_task, raising=False)
    monkeypatch.setattr(sdp, "ROOT", root)
    monkeypatch.setattr(runs, "SUPERVISOR_ARGV", (sys.executable, str(folder / "supervisor_wrapper.py"), str(REPO_ROOT), str(folder)))
    monkeypatch.setattr(runstore, "_LOCK_RETRY_SECONDS", 0)
    monkeypatch.setenv("TEST_TASK_NOTES", str(folder))
    made = TaskDeployment(root, folder, fake_run_docker)
    yield made
    with runs._supervisors_lock:
        started = list(runs._supervisors)
        runs._supervisors.clear()
    for process in started:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=30)
    if (folder / "pids").exists():
        for pid in made.pids():
            if pid and alive(pid):
                os.kill(pid, signal.SIGKILL)
    sys.modules.pop("test_task_module", None)


class TestStart:
    @pytest.mark.parametrize("verb_name", ["run.start", "source.download", "db.create_indexes", "no.such_verb", None])
    def test_a_verb_not_on_the_allow_list_creates_nothing(self, deployment, verb_name):
        """Bug class: any verb of the registry (a pipeline run, or a setup verb switched on later) started as a task, outside the checks of its own path, because it has an action. Refused before its fields are looked at, before the state folder exists and before Docker is asked."""
        with pytest.raises(errors.RunNotStarted) as exc:
            runs.start_task(deployment.root, verb_name, {"profile": "parse", "source": "ok"}, started_by="web-1")

        assert (exc.value.code, exc.value.message) == ("not_a_task", "the verb is not one that runs as a task")
        assert not (deployment.root / ".sdp").exists() and deployment.docker.calls == []

    def test_a_second_live_task_with_the_same_key_is_refused(self, deployment):
        """Bug class: two downloads into the same source folder (the button pressed twice, two browser tabs), or a refusal that is too wide (another source) or never ends (the first task was cancelled). Docker is not asked: a task's state does not depend on it."""
        first = deployment.start({"source": "reddit", "seconds": 60})
        deployment.pids()

        with pytest.raises(errors.RunNotStarted) as exc:
            deployment.start({"source": "reddit"})
        other = deployment.start({"source": "hn"})

        assert (exc.value.code, exc.value.message) == ("task_active", "a task of this verb for the same target is already active")
        assert sorted(os.listdir(deployment.root / ".sdp" / "runs")) == sorted([first, other])
        assert runstore.cancel_run(deployment.root, {}, first) == runstore.CANCEL_REQUESTED
        assert deployment.ended(first)["state"] == "cancelled"
        deployment.ended(other)
        again = deployment.start({"source": "reddit"})
        assert deployment.ended(again)["state"] == "finished"
        assert deployment.docker.calls == [] and not deployment.docker_reached_from_a_supervisor()


class TestARunningTask:
    def test_it_runs_to_its_end_and_is_recorded_without_a_secret(self, deployment):
        """Bug class: the token of a private dataset written into the record (which `runs list --json` and the browser show), into the log, or dropped on its way, so the action runs without it; a task recorded with a profile, without its verb or label, or as a pipeline run; an end written before the action returned, or with another exit code; the supervisor asking Docker about containers a task does not have."""
        run_id = deployment.start({"source": "reddit", "token": PLANTED}, started_by="web-7")

        entry = deployment.ended(run_id)

        assert entry == {
            "id": run_id, "kind": "task", "profile": None, "source": "reddit", "flags": {}, "started_by": "web-7", "task": "test.task", "label": "test \u00b7 reddit",
            "state": "finished", "started": entry["started"], "ended": entry["ended"], "exit_code": 3,
        }
        assert deployment.log(run_id) == "working on reddit\nwith a token\ndone\n"
        for path in (deployment.root / ".sdp").rglob("*"):
            if path.is_file():
                assert PLANTED.encode() not in path.read_bytes() and b"token" not in path.read_bytes().replace(b"with a token", b""), path
        assert deployment.docker.calls == [] and not deployment.docker_reached_from_a_supervisor()

    @pytest.mark.parametrize("ignore_term", [False, True], ids=["ends-on-term", "needs-kill"])
    def test_cancel_ends_the_process_group_and_records_cancelled(self, deployment, ignore_term):
        """Bug class: a cancel that ends the child and leaves what the action started (a `docker compose exec` client, a download helper) running; a cancel that waits for ever on an action that ignores TERM; `cancelled` recorded while a process of the group still runs; no word in the log that a cancel undoes nothing. Also, while the task runs, the token is in the argument list and environment of neither the supervisor nor the child."""
        run_id = deployment.start({"source": "reddit", "seconds": 120, "ignore_term": ignore_term, "token": PLANTED})
        child, helper = deployment.pids()
        with open(f"/proc/{child}/stat") as handle:
            supervisor = int(handle.read().rsplit(")", 1)[1].split()[1])
        for pid in (supervisor, child):
            for name in ("cmdline", "environ"):
                content = Path(f"/proc/{pid}/{name}").read_bytes()
                assert content and PLANTED.encode() not in content, (pid, name)
        assert deployment.entry(run_id)["state"] == "running"

        assert runstore.cancel_run(deployment.root, {}, run_id) == runstore.CANCEL_REQUESTED
        entry = deployment.ended(run_id)

        assert (entry["state"], entry["exit_code"]) == ("cancelled", -signal.SIGKILL if ignore_term else -signal.SIGTERM)
        assert not alive(child) and not alive(helper) and not alive(supervisor)
        assert deployment.log(run_id) == "working on reddit\nwith a token\n" + END_LINE
        assert deployment.docker.calls == [] and not deployment.docker_reached_from_a_supervisor()


class TestASupervisorThatIsGone:
    def test_a_task_without_its_supervisor_reads_lost_and_is_not_cancelled(self, deployment):
        """Bug class: a task whose supervisor died (killed, a crash) shown as `running` for ever, read as `unknown` when Docker is out of reach, or looked up in Docker as if it had a container; and a cancel of it that writes `cancelled` for an end nobody established."""
        run_id = deployment.start({"source": "reddit", "seconds": 120})
        child, helper = deployment.pids()
        with open(f"/proc/{child}/stat") as handle:
            supervisor = int(handle.read().rsplit(")", 1)[1].split()[1])
        deployment.docker.reachable = False

        os.kill(supervisor, signal.SIGKILL)
        wait_for("the supervisor to be collected", lambda: runs.reap_supervisors() == 0)

        assert deployment.entry(run_id)["state"] == "lost"
        assert [run["state"] for run in runstore.list_runs(deployment.root, {}, 10)["runs"]] == ["lost"]
        with pytest.raises(runstore.CancelRefused) as exc:
            runstore.cancel_run(deployment.root, {}, run_id)
        assert exc.value.message == "the run is no longer running"
        record = runstore.read_record(deployment.root, run_id)
        assert (record["state"], record["ended"]) == ("running", None)
        assert all("label=sdp.run=" + run_id not in " ".join(call) for call in deployment.docker.calls)


class TestTheRecordOfATask:
    @pytest.mark.parametrize("change, readable", [
        ({}, True),
        ({"flags": {"columns": ["id", "created_utc"], "kind": "bloom"}}, True),
        ({"profile": "parse"}, False),
        ({"label": None}, False),
        ({"label": ""}, False),
        ({"label": "x" * 201}, False),
        ({"task": "../x"}, False),
        ({"task": None}, False),
        ({"flags": {"nested": {"a": 1}}}, False),
    ], ids=["plain", "list-flag", "with-profile", "no-label", "empty-label", "long-label", "bad-verb", "no-verb", "nested-flag"])
    def test_what_a_task_record_may_hold(self, deployment, change, readable):
        """Bug class: a hand-edited or damaged record of a task read as something else (a pipeline run with a profile, a label of any length or none) and printed, or a task whose index columns (a list) could not be recorded at all. A record that is not what a task's record is reads `unreadable`."""
        fields = {"profile": None, "task": "db.create_indexes", "label": "create indexes \u00b7 starrocks", "source": None, "flags": {}, "started_by": "web-1", **change}
        if not readable:
            with pytest.raises(ValueError):
                runstore.create_run(deployment.root, fields)
            run_id = runstore.create_run(deployment.root, {**fields, **{"profile": None, "task": "db.create_indexes", "label": "ok", "flags": {}}})
            path = deployment.root / ".sdp" / "runs" / run_id / "record.json"
            path.write_text(json.dumps({**json.loads(path.read_text()), **change}))
            assert runstore.read_run(deployment.root, run_id)["state"] == "unreadable"
            return
        run_id = runstore.create_run(deployment.root, fields)
        entry = runstore.read_run(deployment.root, run_id)
        assert (entry["kind"], entry["task"], entry["label"], entry["flags"]) == ("task", "db.create_indexes", fields["label"], fields["flags"])


class TestTheAllowList:
    def test_every_task_is_a_switched_on_verb_with_plain_required_key_fields(self):
        """Bug class: a task whose verb cannot run over the socket (no action, switched off), whose key field is optional (the label would read "None" and two tasks without it would not be told apart) or a secret (written into the record as a key, and into the label); or a verb added to the allow-list unseen. Each unit that switches a task on adds its verb to the pinned set."""
        assert set(tasks.TASKS) == {"db.create_indexes"}
        for name, task in tasks.TASKS.items():
            verb = verbs.VERBS[name]
            assert task.verb == name and verb.socket_enabled and verb.action and verb.action.startswith("do_") and callable(getattr(sdp, verb.action, None))
            assert task.key and all(key in verb.socket_inputs and verb.socket_inputs[key].required and not verb.socket_inputs[key].type.is_secret for key in task.key)
            assert tasks.label_of(task, {key: "x" for key in task.key})
