"""Tests for `control.broker`, the host process that serves the broker's socket, against real unix sockets in a temporary deployment.

The contracts pinned here, each test naming the bug class it catches:

- Who may connect: another uid is closed without a reply; the folders and files have their modes whatever the umask; a stale socket is replaced only after the lock is taken, and a second broker is refused; an over-long socket path, and code or folders that someone else could change, refuse the start.
- Nothing but `hello` and `login` is answered without a valid token, for every op and every verb and five kinds of bad token. A login draws on the budget of its channel (`local` or `remote`, the default); an unknown channel is a bad request; failed logins of both are one count.
- A call: the registry's refusals with their codes (the field name comes back, never the value), the second check of the management password decided by the verb's class alone, its fifth failure ending the session.
- The session's database password: stored only after the second check, added to `run.start` and `db.start` only when the fields have none, the audit line saying where it came from, and refused for the starts that paste it into a shell line.
- The destructive verbs are refused while a pipeline run is alive or Docker cannot be asked.
- Children: the request on standard input only, an environment of the allowed names only, a getter's data, a failure, a state-changing verb's exit code and output, the bounds (too large, too slow, too many), each ending the child's whole process group.
- The write lock: one state-changing call at a time, `run.start` and `run.cancel` not held by it, held by an orphaned child after the broker is killed; a client that goes away does not end a call; an audit log that cannot be written refuses a call before anything runs.
- `run.logs` read from the bytes of the log file so that a secret is overwritten wherever the pieces cut it; results overwritten before they leave; the transport's bounds; the audit log; stopping.

The child is a stand-in program in the temp folder (it records its argument list, environment, standard input and process ids, and behaves as a file of behaviours says); the management password is checked with a fast stand-in for scrypt; the run starter and the run listing are stubs that record their calls; Docker is never reached (`run_docker_guard`). Linux only.
"""

from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import os
import shutil
import signal
import socket
import stat
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import MappingProxyType

import pytest

from social_data_pipeline import runstore
from social_data_pipeline.control import audit, broker, client, environ, framing, passwords, redact, sessions, tasks, verbs
from social_data_pipeline.control.types import Name, Secret

REPO_ROOT = Path(__file__).resolve().parents[2]
PASSWORD = "management-made-up-7f3a"
OTHER_PASSWORD = "another-made-up-pw-91c4"
DB_PASSWORD = "db-made-up-secret-5e1b"
HF_TOKEN = "hf-made-up-token-2d8c"
RUN_ID = "r20261121T101500-0a1b2c"
SHELL_CHARACTERS = ["'", '"', "$", "`", "\\"]

pytestmark = [pytest.mark.usefixtures("run_docker_guard"), pytest.mark.skipif(not sys.platform.startswith("linux"), reason="unix sockets with SO_PEERCRED, flock, pidfd")]

# The stand-in child: records what it was given, then behaves as `behaviour.json` says for its verb.
CHILD = r'''
import json, os, signal, subprocess, sys, time

out = sys.argv[1]
raw = sys.stdin.buffer.read()
request = json.loads(raw)
try:
    with open(os.path.join(out, "behaviour.json")) as handle:
        how = json.load(handle).get(request["verb"], {})
except FileNotFoundError:
    how = {}
if how.get("ignore_term"):
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
grandchild = None
if how.get("grandchild"):
    grandchild = subprocess.Popen([sys.executable, "-c", "import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)"]).pid
note = {"argv": sys.argv, "env": dict(os.environ), "stdin": raw.decode("utf-8"), "pid": os.getpid(), "pgid": os.getpgrp(), "sid": os.getsid(0), "grandchild": grandchild, "verb": request["verb"]}
temporary = os.path.join(out, "calls", f"{os.getpid()}.tmp")
with open(temporary, "w") as handle:
    json.dump(note, handle)
os.replace(temporary, os.path.join(out, "calls", f"{os.getpid()}.json"))
for name, value in how.get("set_env", {}).items():
    os.environ[name] = value
if "stdout" in how:
    sys.stdout.write(how["stdout"])
    sys.stdout.flush()
if "stdout_bytes" in how:
    left = how["stdout_bytes"]
    while left > 0:
        sys.stdout.buffer.write(b"x" * min(left, 65536))
        left -= 65536
    sys.stdout.flush()
if "stderr" in how:
    sys.stderr.write(how["stderr"])
    sys.stderr.flush()
if "stdout" not in how and "stdout_bytes" not in how:
    print(json.dumps({"verb": request["verb"], "fields": request["fields"]}), flush=True)
if how.get("wait_for"):
    deadline = time.monotonic() + how.get("wait_seconds", 30)
    while not os.path.exists(how["wait_for"]) and time.monotonic() < deadline:
        time.sleep(0.01)
if "late_stdout_bytes" in how:
    left = how["late_stdout_bytes"]
    while left > 0:
        sys.stdout.buffer.write(b"y" * min(left, 65536))
        sys.stdout.flush()
        left -= 65536
open(os.path.join(out, "calls", f"{os.getpid()}.done"), "w").close()
sys.exit(how.get("exit", 0))
'''

# A real broker process for the tests that kill it or signal it: the parameters of a test broker, from a JSON argument.
LAUNCHER = r'''
import hashlib, json, os, signal, subprocess, sys
config = json.loads(sys.argv[1])
sys.path.insert(0, config["repo"])
from social_data_pipeline.control import broker, passwords, sessions
from social_data_pipeline.setup import utils

# The sources of the temporary deployment, which the registry checks a source name against.
utils.SOURCES_DIR = utils.Path(config["root"]) / "config" / "sources"


def scrypt(password, *, salt, n, r, p, maxmem, dklen):
    return hashlib.sha256(salt + password).digest()


def starter(root, fields, *, started_by):
    from social_data_pipeline.control import runs

    supervisor = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"], start_new_session=True)
    # Kept where `start_run` keeps the supervisors it started.
    with runs._supervisors_lock:
        runs._supervisors.append(supervisor)
    with open(os.path.join(config["out"], "supervisor.pid"), "w") as handle:
        handle.write(str(supervisor.pid))
    return config["run_id"]


table = sessions.Sessions(verify=lambda found, candidate: passwords.verify(found, candidate, scrypt=scrypt))
server = broker.Broker(config["root"], sessions=table, child_argv=config["child_argv"], run_starter=starter, run_lister=lambda: {"docker_available": True, "runs": []}, limits=broker.Limits(kill_grace_seconds=0.5, drain_seconds=1, housekeeping_seconds=3600))
signal.signal(signal.SIGTERM, lambda signum, frame: server.signal_stop())
signal.signal(signal.SIGUSR1, lambda signum, frame: server.signal_stop(force=True))
server.start()
server.serve_forever()
sys.exit(0)
'''


def fake_scrypt(password, *, salt, n, r, p, maxmem, dklen):
    """A fast stand-in for scrypt: the file format stays the real one, only the derivation is cheap."""
    return hashlib.sha256(salt + password).digest()


def fast_verify(password_file, candidate):
    return passwords.verify(password_file, candidate, scrypt=fake_scrypt)


def set_password(root, password):
    passwords.set_password(os.path.join(root, ".sdp", "broker"), password, scrypt=fake_scrypt)


def wait_for(what, probe, seconds=15):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        found = probe()
        if found:
            return found
        time.sleep(0.01)
    raise AssertionError(f"timed out waiting for {what}")


def alive(pid):
    """Whether a process runs: it exists and is not a zombie."""
    try:
        with open(f"/proc/{pid}/stat", "rb") as handle:
            return handle.read().rsplit(b")", 1)[1].split()[0] != b"Z"
    except (OSError, IndexError):
        return False


def raw_exchange(path, payload, *, read=True, timeout=10):
    """Send bytes on a new connection and return what comes back until the broker closes it."""
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    connection.settimeout(timeout)
    connection.connect(path)
    try:
        if payload:
            try:
                connection.sendall(payload)
            except (BrokenPipeError, ConnectionResetError):
                # The broker closed the connection before the request was sent: closed, without a reply.
                if read:
                    return b""
                raise
        if not read:
            return connection
        received = b""
        while True:
            try:
                chunk = connection.recv(65536)
            except ConnectionResetError:
                # The broker closed the connection with what was sent still unread: closed, without a reply.
                return received
            if not chunk:
                return received
            received += chunk
    finally:
        if read:
            connection.close()


def frame(document_text):
    data = document_text.encode("utf-8")
    return struct.pack(">I", len(data)) + data


def decoded_reply(data):
    return framing.decode(data[4:])


class Rig:
    """A temporary deployment for one test, and the brokers it starts."""

    def __init__(self, root, tmp_path):
        self.root = Path(root)
        self.out = tmp_path / "child-out"
        (self.out / "calls").mkdir(parents=True)
        self.script = tmp_path / "stand_in_child.py"
        self.script.write_text(CHILD)
        self.listing = {"docker_available": True, "runs": []}
        self.lister_calls = []
        self.started_runs = []
        self.started_tasks = []
        self.starter_raises = None
        self.peer = os.geteuid()
        self.private_group = False
        self.starter_gate = None
        self.clock_offset = 0.0
        self.running = []
        self.processes = []
        self.broker = None
        self.behaviours = {}

    # ── the deployment ──────────────────────────────────────────────────────

    def lay_out(self):
        (self.root / "sdp.py").write_text("")
        os.chmod(self.root / "sdp.py", 0o644)
        (self.root / "social_data_pipeline" / "control").mkdir(parents=True)
        os.chmod(self.root / "social_data_pipeline", 0o755)
        os.chmod(self.root / "social_data_pipeline" / "control", 0o755)
        (self.root / "social_data_pipeline" / "control" / "__init__.py").write_text("")
        os.chmod(self.root / "social_data_pipeline" / "control" / "__init__.py", 0o644)
        for folder in (".sdp", ".sdp/broker"):
            (self.root / folder).mkdir(mode=0o700, exist_ok=True)
            os.chmod(self.root / folder, 0o700)
        set_password(self.root, PASSWORD)
        assert len(os.fsencode(broker.socket_path(self.root))) <= broker.SOCKET_PATH_MAX_BYTES, "the temp folder's path is too long for a socket"

    @property
    def socket_path(self):
        return broker.socket_path(self.root)

    @property
    def broker_dir(self):
        return Path(broker.broker_folder(self.root))

    # ── stubs ───────────────────────────────────────────────────────────────

    def lister(self):
        # Whether the write lock was held while the runs were looked for.
        self.lister_calls.append(_write_lock_is_held(self))
        if isinstance(self.listing, Exception):
            raise self.listing
        return self.listing

    def starter(self, root, fields, *, started_by):
        self.started_runs.append((root, dict(fields), started_by))
        if self.starter_gate is not None:
            assert self.starter_gate.wait(15), "the starter's gate was never opened"
        if self.starter_raises is not None:
            raise self.starter_raises
        return RUN_ID

    def task_starter(self, root, verb_name, fields, *, started_by):
        self.started_tasks.append((root, verb_name, dict(fields), started_by))
        return RUN_ID

    def session_clock(self):
        return time.monotonic() + self.clock_offset

    def behave(self, verb, **how):
        self.behaviours[verb] = how
        temporary = self.out / "behaviour.json.tmp"
        temporary.write_text(json.dumps(self.behaviours))
        os.replace(temporary, self.out / "behaviour.json")

    # ── brokers ─────────────────────────────────────────────────────────────

    def make(self, **limits):
        settings = {"request_seconds": 5, "send_seconds": 5, "read_seconds": 20, "kill_grace_seconds": 0.5, "drain_seconds": 1, "housekeeping_seconds": 3600, **limits}
        table = sessions.Sessions(verify=fast_verify, now=self.session_clock)
        return broker.Broker(self.root, sessions=table, child_argv=[sys.executable, str(self.script), str(self.out)], run_starter=self.starter, task_starter=self.task_starter, run_lister=self.lister, peer_uid=lambda connection: self.peer, limits=broker.Limits(**settings), private_group=lambda gid, uid: self.private_group)

    def start(self, **limits):
        server = self.make(**limits)
        server.start()
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.running.append((server, thread))
        self.broker = server
        return server

    def stop(self, force=False):
        server, thread = self.running.pop()
        server.request_stop(force=force)
        thread.join(30)
        assert not thread.is_alive(), "the broker did not stop"

    def spawn_real(self, **extra):
        """A real broker process, started with the launcher; returns the Popen."""
        launcher = self.out.parent / "launcher.py"
        launcher.write_text(LAUNCHER)
        config = {"repo": str(REPO_ROOT), "root": str(self.root), "out": str(self.out), "run_id": RUN_ID, "child_argv": [sys.executable, str(self.script), str(self.out)], **extra}
        process = subprocess.Popen([sys.executable, str(launcher), json.dumps(config)], env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        self.processes.append(process)

        def answers():
            try:
                return self.client().hello()
            except client.BrokerError:
                assert process.poll() is None, process.stderr.read().decode()
                return None

        wait_for("the broker process to answer", answers)
        return process

    def client(self, token=None, timeout=10):
        return client.Client(self.socket_path, timeout=timeout, token=token)

    def login(self, password=PASSWORD):
        session = self.client()
        session.login(password)
        return session

    # ── what happened ───────────────────────────────────────────────────────

    def calls(self, verb=None):
        found = []
        for path in sorted((self.out / "calls").glob("*.json")):
            note = json.loads(path.read_text())
            if verb is None or note["verb"] == verb:
                found.append(note)
        return found

    def done(self, pid):
        return (self.out / "calls" / f"{pid}.done").exists()

    def audit_lines(self):
        path = self.broker_dir / audit.FILE_NAME
        return [json.loads(line) for line in path.read_text().splitlines() if line]

    def events(self, name):
        return [line for line in self.audit_lines() if line["event"] == name]

    def broker_log(self):
        path = self.broker_dir / broker.LOG_NAME
        return path.read_text() if path.exists() else ""

    def tear_down(self):
        for server, thread in self.running:
            server.request_stop(force=True)
        for server, thread in self.running:
            thread.join(30)
        for process in self.processes:
            if process.poll() is None:
                process.kill()
            process.wait(10)
            if process.stderr is not None:
                process.stderr.close()
        pids = []
        for note in self.calls():
            pids += [note["pid"], note["grandchild"]]
        supervisor = self.out / "supervisor.pid"
        if supervisor.exists():
            pids.append(int(supervisor.read_text()))
        for pid in pids:
            if pid and alive(pid):
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass


@pytest.fixture
def rig(source_tree, tmp_path):
    """A deployment in the temp folder (the `source_tree` folder, with the source `ok`), laid out for a broker; every broker, child and process is ended at the end of the test."""
    made = Rig(source_tree, tmp_path)
    made.lay_out()
    yield made
    made.tear_down()


def code_of(call):
    with pytest.raises(client.BrokerError) as caught:
        call()
    return caught.value


# ── 1. who may connect, and the start ───────────────────────────────────────


class TestStart:
    def test_another_uid_is_closed_without_a_reply(self, rig):
        """Bug class: the socket answering any local user (the folder's mode as the only line, or a peer check that reads the uid and then answers anyway); the closed connection not counted."""
        rig.start()
        rig.peer = os.geteuid() + 1

        assert raw_exchange(rig.socket_path, frame('{"op":"hello"}')) == b""
        with pytest.raises(client.BrokerUnreachable):
            rig.client().hello()
        rig.peer = os.geteuid()
        assert rig.client().hello()["protocol"] == 1
        rig.stop()
        counts = rig.events("counts")
        assert sum(line["unauthenticated"] for line in counts) == 2

    def test_folders_and_files_have_their_modes_whatever_the_umask(self, rig):
        """Bug class: a socket, log or lock made with the process's umask (group- or world-accessible), so that another user reaches the socket or reads the log."""
        old = os.umask(0o002)
        try:
            rig.start()
            session = rig.login()
            session.call("db.stop", {})
        finally:
            os.umask(old)
        modes = {name: stat.S_IMODE(os.lstat(rig.root / name).st_mode) for name in (".sdp", ".sdp/broker", ".sdp/socket", ".sdp/socket/broker.sock", ".sdp/broker/audit.jsonl", ".sdp/broker/broker.log", ".sdp/broker/lock", ".sdp/broker/write.lock", ".sdp/broker/password.json")}
        assert modes == {".sdp": 0o700, ".sdp/broker": 0o700, ".sdp/socket": 0o700, ".sdp/socket/broker.sock": 0o600, ".sdp/broker/audit.jsonl": 0o600, ".sdp/broker/broker.log": 0o600, ".sdp/broker/lock": 0o600, ".sdp/broker/write.lock": 0o600, ".sdp/broker/password.json": 0o600}
        assert stat.S_ISSOCK(os.lstat(rig.socket_path).st_mode)

    def test_a_stale_socket_is_replaced_only_after_the_lock_is_taken(self, rig):
        """Bug class: a second broker that removes the socket of the one that runs before it finds out it cannot run (the running broker then serves a path that leads nowhere); a stale socket that blocks every start."""
        (rig.root / ".sdp" / "socket").mkdir(mode=0o700)
        stale = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        stale.bind(rig.socket_path)
        stale.close()
        rig.start()
        assert rig.client().hello()["protocol"] == 1
        first = os.lstat(rig.socket_path).st_ino

        second = rig.make()
        with pytest.raises(broker.StartRefused) as refused:
            second.start()

        assert refused.value.message == "a broker is already running for this deployment"
        assert os.lstat(rig.socket_path).st_ino == first
        assert rig.client().hello()["protocol"] == 1

    def test_an_over_long_socket_path_is_a_clear_error(self, source_tree, tmp_path):
        """Bug class: a deployment path too long for a socket address failing with the system's own error text, or binding a path the kernel silently cut short."""
        deep = source_tree / ("d" * 60) / ("e" * 60)
        deep.mkdir(parents=True)
        os.chmod(deep.parent, 0o700)
        os.chmod(deep, 0o700)
        made = Rig(deep, tmp_path / "deep")
        (made.root / "sdp.py").write_text("")
        os.chmod(made.root / "sdp.py", 0o644)
        (made.root / "social_data_pipeline" / "control").mkdir(parents=True)
        os.chmod(made.root / "social_data_pipeline", 0o755)
        os.chmod(made.root / "social_data_pipeline" / "control", 0o755)

        with pytest.raises(broker.StartRefused) as refused:
            made.make().start()

        assert "longer than the 108 bytes a unix socket address holds" in refused.value.message
        assert not (made.root / ".sdp" / "broker" / "lock").exists()
        assert not (made.root / ".sdp" / "socket" / "broker.sock").exists()

    @pytest.mark.parametrize("relative", [".sdp", ".sdp/broker", ".sdp/socket"])
    @pytest.mark.parametrize("private", [False, True])
    def test_a_state_folder_the_group_may_write_refuses_the_start_even_for_a_private_group(self, rig, relative, private):
        """Bug class: the broker's own folders (its password file, audit log, socket) writable by anyone but the operator, private group or not."""
        rig.private_group = private
        (rig.root / ".sdp" / "socket").mkdir(mode=0o700, exist_ok=True)
        os.chmod(rig.root / relative, 0o770)

        with pytest.raises(broker.StartRefused) as refused:
            rig.make().start()

        assert refused.value.message == f"{relative} can be written by its group: run `chmod -R go-w {relative}` in the deployment folder"
        assert not (rig.root / ".sdp" / "socket" / "broker.sock").exists()

    @pytest.mark.parametrize("relative", [".", "sdp.py", "social_data_pipeline", "social_data_pipeline/control", "social_data_pipeline/control/__init__.py"])
    def test_code_its_group_may_write_refuses_the_start_unless_the_group_is_the_users_alone(self, rig, relative):
        """Bug class: code (the deployment folder every child imports the CLI from, the CLI, a module of the package) that another user may change through a shared group, so that user runs code as the operator; or a refusal of a group that is the operator's own (the usual umask 002 with a private group)."""
        path = rig.root / relative
        os.chmod(path, stat.S_IMODE(os.lstat(path).st_mode) | 0o020)
        name = "the deployment folder" if relative == "." else relative
        command = "chmod go-w ." if relative == "." else f"chmod -R go-w {relative}"

        with pytest.raises(broker.StartRefused) as refused:
            rig.make().start()
        rig.private_group = True
        rig.start()

        assert refused.value.message == f"{name} can be written by its group, which is not a group of this user alone: run `{command}` in the deployment folder"
        assert rig.client().hello()["protocol"] == 1

    @pytest.mark.parametrize("relative", [".", "sdp.py", "social_data_pipeline", "social_data_pipeline/control", "social_data_pipeline/control/__init__.py"])
    def test_code_others_may_write_refuses_the_start_whatever_the_group(self, rig, relative):
        """Bug class: code writable by every user of the machine accepted because its group passed."""
        rig.private_group = True
        path = rig.root / relative
        os.chmod(path, stat.S_IMODE(os.lstat(path).st_mode) | 0o002)
        name = "the deployment folder" if relative == "." else relative

        with pytest.raises(broker.StartRefused) as refused:
            rig.make().start()

        assert refused.value.message.startswith(f"{name} can be written by others: run `chmod ")

    def test_every_module_and_folder_under_the_package_is_checked(self, rig):
        """Bug class: only the top folders checked, so a module file of mode 0664 (or 0666) inside a folder of mode 0755, a compiled file next to it, or a deeper folder passes."""
        deep = rig.root / "social_data_pipeline" / "setup" / "nested"
        deep.mkdir(parents=True)
        os.chmod(deep.parent, 0o755)
        os.chmod(deep, 0o755)
        cases = [deep / "module.py", deep / "__pycache__" / "module.cpython-313.pyc", deep]
        (deep / "__pycache__").mkdir()
        os.chmod(deep / "__pycache__", 0o755)
        for path in cases[:2]:
            path.write_text("")
            os.chmod(path, 0o644)
        (deep / "template.yaml").write_text("")
        os.chmod(deep / "template.yaml", 0o666)
        rig.start()
        rig.stop()

        found = []
        for path in cases:
            os.chmod(path, os.lstat(path).st_mode | 0o002)
            with pytest.raises(broker.StartRefused) as refused:
                rig.make().start()
            found.append(refused.value.message.split(" can be written")[0])
            os.chmod(path, os.lstat(path).st_mode & ~0o002)

        assert found == [str(path.relative_to(rig.root)) for path in cases]

    def test_a_link_under_the_package_refuses_the_start(self, rig, tmp_path):
        """Bug class: the walk following a link out of the package (to a module anybody may write) or passing over it."""
        target = tmp_path / "elsewhere.py"
        target.write_text("")
        os.chmod(target, 0o644)
        (rig.root / "social_data_pipeline" / "control" / "linked.py").symlink_to(target)

        with pytest.raises(broker.StartRefused) as refused:
            rig.make().start()

        assert refused.value.message == "social_data_pipeline/control/linked.py is a symbolic link"

    def test_the_lock_is_tried_again_while_someone_looks_whether_a_broker_runs(self, rig):
        """Bug class: a start refused as "already running" because `status`, `start` or `stop` held the lock shared for the instant it looks."""
        lock_path = rig.root / ".sdp" / "broker" / "lock"
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        fcntl.flock(fd, fcntl.LOCK_SH)
        threading.Timer(0.08, os.close, args=(fd,)).start()

        rig.start()

        assert rig.client().hello()["protocol"] == 1

    def test_a_held_lock_still_refuses_after_the_tries(self, rig):
        """Bug class: tries without an end, or a lock taken while a broker holds it."""
        lock_path = rig.root / ".sdp" / "broker" / "lock"
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        fcntl.flock(fd, fcntl.LOCK_SH)
        try:
            with pytest.raises(broker.StartRefused) as refused:
                rig.make().start()
        finally:
            os.close(fd)

        assert refused.value.message == "a broker is already running for this deployment"


class _Account:
    def __init__(self, name, uid, gid):
        self.pw_name, self.pw_uid, self.pw_gid = name, uid, gid


class _Group:
    def __init__(self, members):
        self.gr_mem = members


@pytest.mark.parametrize("accounts, members, gid, expected", [
    ([_Account("operator", 1000, 1000)], [], 1000, True),
    ([_Account("operator", 1000, 1000)], ["operator"], 1000, True),
    ([_Account("operator", 1000, 1000)], ["someone"], 1000, False),
    ([_Account("operator", 1000, 1000), _Account("someone", 1001, 1000)], [], 1000, False),
    ([_Account("operator", 1000, 1000)], [], 27, False),
    ([], [], 1000, False),
], ids=["own-group", "own-name-listed", "another-member", "another-primary", "not-the-primary", "unknown-user"])
def test_a_group_counts_as_private_only_when_nobody_else_is_in_it(monkeypatch, accounts, members, gid, expected):
    """Bug class: a group taken as the operator's own while another account is in it (as a member, or as its primary group), or when the lists cannot tell."""
    import grp
    import pwd

    def getpwuid(uid):
        for account in accounts:
            if account.pw_uid == uid:
                return account
        raise KeyError(uid)

    monkeypatch.setattr(pwd, "getpwuid", getpwuid)
    monkeypatch.setattr(pwd, "getpwall", lambda: list(accounts))
    monkeypatch.setattr(grp, "getgrgid", lambda wanted: _Group(members))

    assert broker._group_is_private(gid, 1000) is expected


class TestStartMore:
    def test_a_link_in_the_place_of_the_code_refuses_the_start(self, rig, tmp_path):
        """Bug class: the ownership check made through a symbolic link (on the target, which may be anybody's folder)."""
        target = tmp_path / "elsewhere-code"
        target.mkdir()
        os.chmod(target, 0o755)
        shutil.rmtree(rig.root / "social_data_pipeline" / "control")
        (rig.root / "social_data_pipeline" / "control").symlink_to(target)

        with pytest.raises(broker.StartRefused) as refused:
            rig.make().start()

        assert refused.value.message == "social_data_pipeline/control is a symbolic link"

    def test_a_folder_of_another_owner_refuses_the_start(self, rig):
        """Bug class: no owner check, so that a folder another user created (and can replace files in) is used."""
        server = rig.make()
        server.uid = os.geteuid() + 1

        with pytest.raises(broker.StartRefused) as refused:
            server.start()

        assert refused.value.message == "the deployment folder does not belong to the user the broker runs as"


# ── 2. nothing without a token ──────────────────────────────────────────────


def _requests_with(token, include_token=True):
    """One request of every op that needs a session, and one call of every verb of the registry (and one that does not exist)."""
    def with_token(document):
        return {**document, "token": token} if include_token else document
    found = [
        with_token({"op": "logout"}),
        with_token({"op": "session.set_db_password", "step_up": PASSWORD, "password": DB_PASSWORD}),
        with_token({"op": "session.clear_db_password"}),
    ]
    for name in [*sorted(verbs.VERBS), "no.such.verb"]:
        found.append(with_token({"op": "call", "verb": name, "fields": {}, "step_up": PASSWORD}))
    return found


class TestTokens:
    @pytest.mark.parametrize("kind", ["missing", "wrong", "expired", "password_changed", "logged_out"])
    def test_nothing_but_hello_is_answered_without_a_valid_token(self, rig, kind):
        """Bug class: an op or a verb answered (or a child started, or a run listed or started) before the session is checked: a validation error, a step-up prompt or a refusal that reveals something to whoever has no session; a session that outlives its eight hours, a password change or its logout."""
        rig.start()
        token = None
        if kind == "wrong":
            token = "a-made-up-token-that-names-no-session"
        elif kind in ("expired", "password_changed", "logged_out"):
            session = rig.login()
            token = session.token
            assert session.call("db.status")["data"]["verb"] == "db.status"
            if kind == "expired":
                rig.clock_offset += sessions.SESSION_SECONDS
            elif kind == "password_changed":
                set_password(rig.root, OTHER_PASSWORD)
            else:
                session.logout()
        calls_before = len(rig.calls())

        answers = []
        for document in _requests_with(token, include_token=kind != "missing"):
            answers.append(code_of(lambda: rig.client().request(document)).code)

        assert set(answers) == {"unauthorized"}
        assert len(answers) == 3 + len(verbs.VERBS) + 1
        assert len(rig.calls()) == calls_before
        assert rig.started_runs == [] and rig.lister_calls == []
        assert rig.client().hello() == {"protocol": 1, "password_set": True}
        if kind in ("expired", "password_changed"):
            assert [line["reason"] for line in rig.events("session_end")] == ["expired" if kind == "expired" else "password_changed"]


class TestLoginChannels:
    @pytest.mark.parametrize("spent, other", [("remote", "local"), ("local", "remote")])
    def test_failed_logins_of_one_channel_leave_the_other_open(self, rig, spent, other):
        """Bug class: the broker ignoring the channel of a login, so that five wrong guesses through the network lock the operator out on the operator's own machine (and the reverse)."""
        rig.start()
        for _ in range(5):
            assert code_of(lambda: rig.client().login("made-up-wrong-pw-1", channel=spent)).code == "unauthorized"

        assert code_of(lambda: rig.client().login(PASSWORD, channel=spent)).code == "throttled"
        session = rig.client()
        session.login(PASSWORD, channel=other)
        assert session.call("db.status")["data"]["verb"] == "db.status"

    def test_a_login_without_a_channel_draws_on_the_remote_budget(self, rig):
        """Bug class: a client that names no channel (an older one, or a hand-made request) drawing on the local budget."""
        rig.start()
        for _ in range(5):
            reply = decoded_reply(raw_exchange(rig.socket_path, frame(json.dumps({"op": "login", "password": "made-up-wrong-pw-1"}))))
            assert reply["error"]["code"] == "unauthorized"

        assert code_of(lambda: rig.client().login(PASSWORD, channel="remote")).code == "throttled"
        rig.client().login(PASSWORD, channel="local")

    @pytest.mark.parametrize("channel", ["LOCAL", "", None, 1, ["local"], "both"])
    def test_an_unknown_channel_is_a_bad_request_and_verifies_nothing(self, rig, channel):
        """Bug class: an unknown channel taken for one of the two (or for a third budget without a limit); a refusal that still costs a unit of a budget."""
        rig.start()
        for _ in range(3):
            reply = decoded_reply(raw_exchange(rig.socket_path, frame(json.dumps({"op": "login", "password": PASSWORD, "channel": channel}))))
            assert reply["error"]["code"] == "bad_request"

        assert rig.broker.sessions.budget("local") == rig.broker.sessions.budget("remote") == 5
        assert rig.events("login") == []

    def test_failed_logins_of_both_channels_are_counted_as_one_number(self, rig):
        """Bug class: the audit's count of failed logins split or losing the logins of one channel."""
        rig.start()
        code_of(lambda: rig.client().login("made-up-wrong-pw-1", channel="local"))
        code_of(lambda: rig.client().login("made-up-wrong-pw-2", channel="remote"))
        rig.stop()

        assert rig.events("counts")[0]["failed_logins"] == 2


# ── 3. a call ───────────────────────────────────────────────────────────────


class TestCall:
    def test_the_registrys_refusals_have_their_codes_and_never_echo_a_value(self, rig):
        """Bug class: a refusal that sends back what the client wrote (a value that can be a secret, or text made to end up on a page), or one that is not audited; an unknown field or a disabled verb reaching a child."""
        rig.start()
        session = rig.login()
        marker = "planted-value-4b2e/../x"

        unknown = code_of(lambda: session.call("db.nope"))
        disabled = code_of(lambda: session.call("db.recover_password", {}))
        extra = code_of(lambda: session.call("db.status", {"colour": True}))
        bad = code_of(lambda: session.call("source.status", {"name": marker}))

        assert (unknown.code, disabled.code, extra.code, bad.code) == ("unknown_verb", "not_available", "invalid_input", "invalid_input")
        assert (extra.field, bad.field) == ("colour", "name")
        for refused in (unknown, disabled, extra, bad):
            assert marker not in json.dumps({"message": refused.message, "details": refused.details})
        assert rig.calls() == []
        assert [(line["verb"], line["code"], line["field"]) for line in rig.events("call_refused")] == [("db.nope", "unknown_verb", None), ("db.recover_password", "not_available", None), ("db.status", "invalid_input", "colour"), ("source.status", "invalid_input", "name")]
        assert marker not in (rig.broker_dir / audit.FILE_NAME).read_text()

    def test_a_step_up_verb_needs_the_password_again_and_the_fifth_failure_ends_the_session(self, rig):
        """Bug class: a step-up verb run on the session alone, or on a wrong password; failures that never end the session (a stolen token could guess the password without the login budget); an end that is not said."""
        rig.start()
        session = rig.login()

        missing = code_of(lambda: session.call("db.unsetup_jobs"))
        wrong = [code_of(lambda: session.call("db.unsetup_jobs", step_up="wrong-password-1")).code for _ in range(4)]
        fifth = code_of(lambda: session.call("db.unsetup_jobs", step_up="wrong-password-1"))
        after = code_of(lambda: session.call("db.status"))

        assert missing.code == "step_up_required"
        assert wrong == ["step_up_failed"] * 4
        assert fifth.code == "session_ended" and fifth.message == "the password was wrong too often: the session has ended"
        assert after.code == "unauthorized"
        assert rig.calls() == []
        assert len(rig.events("step_up_failed")) == 5
        assert [line["reason"] for line in rig.events("session_end")] == ["step_up_failures"]

        again = rig.login()
        assert again.call("db.unsetup_jobs", step_up=PASSWORD)["exit_code"] == 0
        assert [note["verb"] for note in rig.calls()] == ["db.unsetup_jobs"]

    def test_the_class_alone_decides_the_second_check(self, rig):
        """Bug class: the second check decided by a field (a `db.start` of a service that looks harmless let through, or a `step_up` value on a write verb taken as a reason to check or to skip)."""
        rig.start()
        session = rig.login()

        refused = code_of(lambda: session.call("db.start", {"service": "mongo"}))
        written = session.call("db.stop", {"service": "jobs"}, step_up="wrong-password-1")

        assert refused.code == "step_up_required"
        assert written["exit_code"] == 0
        assert [note["verb"] for note in rig.calls()] == ["db.stop"]
        assert rig.events("step_up_failed") == []


# ── 4. the session's database password ──────────────────────────────────────


class TestDatabasePassword:
    def test_without_or_with_a_wrong_second_check_nothing_is_stored(self, rig):
        """Bug class: the database password kept before the management password was checked again, so a stolen token plants a password of its choosing (and pushes real ones out of the redaction list)."""
        rig.start()
        session = rig.login()

        missing = code_of(lambda: session.request({"op": "session.set_db_password", "token": session.token, "password": DB_PASSWORD}))
        wrong = code_of(lambda: session.set_db_password(DB_PASSWORD, step_up="wrong-password-1"))
        session.call("run.start", {"profile": "parse", "source": "ok"})

        assert (missing.code, wrong.code) == ("bad_request", "step_up_failed")
        assert rig.started_runs[-1][1] == {"profile": "parse", "source": "ok"}
        assert rig.events("call_accepted")[-1]["db_password_from"] == "none"
        assert all(DB_PASSWORD not in secret.reveal() for secret in rig.broker.sessions.known_secrets())

    def test_the_stored_password_is_added_only_when_the_fields_have_none(self, rig):
        """Bug class: the session's password overriding the one the request names, added to verbs that do not take it, or not said in the audit line."""
        rig.start()
        session = rig.login()
        session.set_db_password(DB_PASSWORD, step_up=PASSWORD)

        session.call("run.start", {"profile": "parse", "source": "ok"})
        session.call("run.start", {"profile": "lingua", "source": "ok", "admin_password": "from-the-fields-1"})
        session.call("db.start", {"service": "mongo"}, step_up=PASSWORD)
        session.call("db.stop", {"service": "mongo"})
        session.clear_db_password()
        session.call("run.start", {"profile": "ml", "source": "ok"})

        assert [fields.get("admin_password") for _, fields, _ in rig.started_runs] == [DB_PASSWORD, "from-the-fields-1", None]
        assert [started_by for _, _, started_by in rig.started_runs] == ["web-1"] * 3
        assert json.loads(rig.calls("db.start")[0]["stdin"])["fields"] == {"service": "mongo", "admin_password": DB_PASSWORD}
        assert json.loads(rig.calls("db.stop")[0]["stdin"])["fields"] == {"service": "mongo"}
        assert [(line["verb"], line["db_password_from"]) for line in rig.events("call_accepted")] == [("run.start", "session"), ("run.start", "fields"), ("db.start", "session"), ("db.stop", "none"), ("run.start", "none")]

    @pytest.mark.parametrize("character", SHELL_CHARACTERS)
    @pytest.mark.parametrize("service", [None, "postgres", "postgres-mcp", "mongo"])
    @pytest.mark.parametrize("source", ["fields", "session"])
    def test_a_start_that_pastes_the_password_into_a_shell_line_refuses_shell_characters(self, rig, character, service, source):
        """Bug class: a database password with a quote, `$`, a backtick or a backslash handed to the PostgreSQL start, which pastes it into a shell line (a command run in the container); or the refusal applied to a service that does not do that."""
        rig.start()
        session = rig.login()
        password = f"made-up{character}pw-77"
        fields = {} if service is None else {"service": service}
        if source == "fields":
            fields["admin_password"] = password
        else:
            session.set_db_password(password, step_up=PASSWORD)

        if service == "mongo":
            assert session.call("db.start", fields, step_up=PASSWORD)["exit_code"] == 0
            assert len(rig.calls("db.start")) == 1
            return
        refused = code_of(lambda: session.call("db.start", fields, step_up=PASSWORD))
        assert (refused.code, refused.field) == ("password_not_allowed_here", "admin_password")
        assert rig.calls() == []
        assert password not in json.dumps(refused.details) + refused.message


# ── 5. destructive verbs while a run is alive ───────────────────────────────


# `db.unsetup_mcp` among them: the CLI stops each database that has an MCP server with that server (`docker compose --profile <db> --profile <db>_mcp down`), under a run that writes into it.
DESTRUCTIVE = [("db.reset", {"db": "mongo"}, PASSWORD), ("db.unsetup", {"db": "mongo"}, PASSWORD), ("db.unsetup_mcp", {}, PASSWORD), ("db.stop", {}, None), ("source.remove", {"name": "ok"}, PASSWORD)]


def _run(kind, state):
    return {"id": RUN_ID if kind == "web" else "live-0123456789ab", "kind": kind, "profile": "parse", "source": "ok", "flags": None, "started_by": None, "state": state, "started": None, "ended": None, "exit_code": None}


class TestDestructiveVerbs:
    @pytest.mark.parametrize("verb, fields, step_up", DESTRUCTIVE, ids=[entry[0] for entry in DESTRUCTIVE])
    @pytest.mark.parametrize("run", [_run("web", "starting"), _run("web", "running"), _run("web", "detached"), _run("web", "unknown"), _run("terminal", "running")], ids=["starting", "running", "detached", "unknown", "terminal"])
    def test_refused_while_a_run_is_alive(self, rig, verb, fields, step_up, run):
        """Bug class: a database reset, removed or stopped (also by removing the MCP servers' configuration, which stops their databases) or a source removed under a pipeline run that writes into it: a run whose supervisor is gone but whose container lives, or one Docker could not be asked about, counted as over."""
        rig.start()
        rig.listing = {"docker_available": True, "runs": [_run("web", "finished"), run]}
        session = rig.login()

        refused = code_of(lambda: session.call(verb, fields, step_up=step_up))

        assert refused.code == "run_active"
        assert rig.calls() == []
        assert rig.events("call_refused")[-1]["code"] == "run_active"

    @pytest.mark.parametrize("verb, fields, step_up", DESTRUCTIVE, ids=[entry[0] for entry in DESTRUCTIVE])
    def test_allowed_when_every_run_is_over(self, rig, verb, fields, step_up):
        """Bug class: a refusal that never lifts (an ended, cancelled or lost run counted as alive)."""
        rig.start()
        rig.listing = {"docker_available": True, "runs": [_run("web", "finished"), _run("web", "cancelled"), _run("web", "lost")]}
        session = rig.login()

        assert session.call(verb, fields, step_up=step_up)["exit_code"] == 0
        assert [note["verb"] for note in rig.calls()] == [verb]

    @pytest.mark.parametrize("verb, fields, step_up", DESTRUCTIVE, ids=[entry[0] for entry in DESTRUCTIVE])
    @pytest.mark.parametrize("unreachable", ["flag", "raised"])
    def test_refused_when_docker_cannot_be_asked(self, rig, verb, fields, step_up, unreachable):
        """Bug class: "Docker did not answer" read as "no run is alive"."""
        rig.start()
        rig.listing = {"docker_available": False, "runs": []} if unreachable == "flag" else runstore.DockerUnavailable(runstore.DOCKER_NOT_REACHABLE)
        session = rig.login()

        assert code_of(lambda: session.call(verb, fields, step_up=step_up)).code == "docker_unreachable"
        assert rig.calls() == []

    def test_other_verbs_do_not_ask_for_the_runs(self, rig):
        """Bug class: a listing (a Docker call of seconds) made for every call, or a read refused because a run is alive."""
        rig.start()
        rig.listing = {"docker_available": True, "runs": [_run("web", "running")]}
        session = rig.login()

        session.call("db.status")
        session.call("db.unsetup_jobs", step_up=PASSWORD)

        assert rig.lister_calls == []


# `source configure` and `source add-classifiers` rewrite the files every run of the source reads (its platform.yaml and profile files), and an index build of it may write its platform.yaml.
SOURCE_CONFIG = [("source.configure", {"name": "ok", "profiles": ["parse"]}), ("source.add_classifiers", {"name": "ok"})]


def _task(state, source="ok"):
    return {**_run("web", state), "kind": "task", "profile": None, "task": "source.download", "label": f"download \u00b7 {source}", "source": source}


class TestSourceConfigurationVerbs:
    @pytest.mark.parametrize("verb, fields", SOURCE_CONFIG, ids=[entry[0] for entry in SOURCE_CONFIG])
    @pytest.mark.parametrize("run, label", [
        (_run("web", "running"), "parse"),
        (_run("web", "detached"), "parse"),
        (_task("running"), "download \u00b7 ok"),
        (_task("unknown"), "download \u00b7 ok"),
        (_run("terminal", "running"), "parse"),
        ({**_run("terminal", "running"), "source": None}, "parse"),
    ], ids=["pipeline-run", "detached-run", "download", "download-unknown", "terminal-run", "terminal-run-of-no-source-docker-tells"])
    def test_refused_while_a_run_of_the_source_is_alive_and_the_refusal_names_it(self, rig, verb, fields, run, label):
        """Bug class: a source's configuration rewritten under a run of it (F4 decision 4: a pipeline run, a task such as a download, a terminal run Docker reports), which then reads half-old, half-new files; or a refusal that does not say which run, so the operator cannot find it. The runs are looked for with the write lock held, so none can start between the look and the action."""
        rig.start()
        rig.listing = {"docker_available": True, "runs": [_run("web", "finished"), {**_run("web", "running"), "source": "other"}, run]}
        session = rig.login()

        refused = code_of(lambda: session.call(verb, fields, step_up=PASSWORD))

        assert refused.code == "source_run_alive"
        assert refused.details == {"run_id": run["id"], "run_label": label}
        assert run["id"] in refused.message and label in refused.message
        assert rig.calls() == []
        assert rig.lister_calls == [True]
        assert rig.events("call_refused")[-1]["code"] == "source_run_alive"

    @pytest.mark.parametrize("verb, fields", SOURCE_CONFIG, ids=[entry[0] for entry in SOURCE_CONFIG])
    def test_allowed_beside_runs_of_other_sources_and_ended_runs(self, rig, verb, fields):
        """Bug class: a refusal that never lifts, or one that counts another source's run (Reconfigure of one source blocked by every run of the deployment)."""
        rig.start()
        rig.listing = {"docker_available": True, "runs": [_run("web", "finished"), _run("web", "lost"), _task("cancelled"), {**_run("web", "running"), "source": "other"}, _task("running", "other"), {**_run("terminal", "running"), "source": "other"}]}
        session = rig.login()

        assert session.call(verb, fields, step_up=PASSWORD)["exit_code"] == 0
        assert [note["verb"] for note in rig.calls()] == [verb]

    @pytest.mark.parametrize("verb, fields", SOURCE_CONFIG, ids=[entry[0] for entry in SOURCE_CONFIG])
    @pytest.mark.parametrize("unreachable", ["flag", "raised"])
    def test_refused_when_docker_cannot_be_asked(self, rig, verb, fields, unreachable):
        """Bug class: "Docker did not answer" read as "no run of the source is alive", which lets a reconfigure through under a terminal run."""
        rig.start()
        rig.listing = {"docker_available": False, "runs": []} if unreachable == "flag" else runstore.DockerUnavailable(runstore.DOCKER_NOT_REACHABLE)
        session = rig.login()

        assert code_of(lambda: session.call(verb, fields, step_up=PASSWORD)).code == "docker_unreachable"
        assert rig.calls() == []


# ── 6. children ─────────────────────────────────────────────────────────────


class TestChildren:
    def test_the_request_reaches_the_child_on_standard_input_only(self, rig):
        """Bug class: a secret of the request in the child's argument list (readable by every user in /proc) or its environment (inherited by everything it starts, printed by a debugging command)."""
        rig.start()
        gate = rig.out / "gate"
        rig.behave("db.start", wait_for=str(gate))
        session = rig.login()
        replies = []
        caller = threading.Thread(target=lambda: replies.append(session.call("db.start", {"service": "mongo", "admin_password": DB_PASSWORD}, step_up=PASSWORD)))
        caller.start()
        note = wait_for("the child", lambda: rig.calls("db.start"))[0]

        command_line = Path(f"/proc/{note['pid']}/cmdline").read_bytes()
        environment = Path(f"/proc/{note['pid']}/environ").read_bytes()
        gate.touch()
        caller.join(15)

        assert DB_PASSWORD.encode() not in command_line and PASSWORD.encode() not in command_line
        assert DB_PASSWORD.encode() not in environment and PASSWORD.encode() not in environment
        assert json.loads(note["stdin"]) == {"verb": "db.start", "fields": {"service": "mongo", "admin_password": DB_PASSWORD}}
        assert note["argv"] == [str(rig.script), str(rig.out)]
        assert note["sid"] == note["pid"] == note["pgid"]
        assert replies[0]["exit_code"] == 0

    def test_the_childs_environment_holds_only_allowed_names(self, rig, monkeypatch):
        """Bug class: the broker's whole environment handed on (other tokens, a preloaded library, a Python path), or what one call's child set showing up in the next."""
        monkeypatch.setenv("SDP_MADE_UP_NOT_ALLOWED", "made-up-value")
        monkeypatch.setenv("PYTHONSTARTUP", "/nonexistent/made-up.py")
        monkeypatch.setenv("HF_TOKEN", HF_TOKEN)
        rig.start()
        rig.behave("db.status", set_env={"SDP_MADE_UP_SET_BY_A_CHILD": "x"})
        session = rig.login()

        session.call("db.status")
        session.call("db.status")

        first, second = rig.calls("db.status")
        for note in (first, second):
            assert all(environ.is_allowed(name) for name in note["env"]), sorted(note["env"])
            assert "SDP_MADE_UP_NOT_ALLOWED" not in note["env"] and "PYTHONSTARTUP" not in note["env"]
            assert note["env"]["HF_TOKEN"] == HF_TOKEN
        assert "SDP_MADE_UP_SET_BY_A_CHILD" not in second["env"]
        assert "SDP_MADE_UP_SET_BY_A_CHILD" not in os.environ

    def test_a_getters_data_and_a_getters_failure(self, rig):
        """Bug class: a getter's output passed on as text, or a failed getter (exit code not 0, output that is not JSON) answered as data."""
        rig.start()
        session = rig.login()
        rig.behave("source.list", stdout=json.dumps({"sources": ["ok"], "count": 1}))
        data = session.call("source.list")
        rig.behave("source.list", stdout=json.dumps({"partial": True}), stderr="Error: the source list cannot be read\n", exit=4)
        failed = code_of(lambda: session.call("source.list"))
        rig.behave("source.list", stdout="not json at all")
        not_json = code_of(lambda: session.call("source.list"))

        assert data == {"data": {"sources": ["ok"], "count": 1}}
        assert (failed.code, failed.details) == ("failed", {"exit_code": 4, "errors": "Error: the source list cannot be read\n"})
        assert (not_json.code, not_json.details["exit_code"]) == ("failed", 0)

    @pytest.mark.parametrize("exit_code", [0, 3])
    def test_a_state_changing_verb_returns_its_exit_code_and_both_streams_whatever_the_code(self, rig, exit_code):
        """Bug class: a failed action answered as "refused" or "nothing ran" (the client then retries something that half ran), or its two streams mixed or dropped."""
        rig.start()
        rig.behave("db.unsetup_jobs", stdout="  Stopping jobs\n", stderr="  Warning: something\n", exit=exit_code)
        session = rig.login()

        result = session.call("db.unsetup_jobs", step_up=PASSWORD)

        assert result == {"exit_code": exit_code, "output": "  Stopping jobs\n", "errors": "  Warning: something\n"}
        assert [(line["outcome"], line["exit_code"]) for line in rig.events("call_ended")] == [("exited", exit_code)]

    def test_the_tails_of_a_long_output_begin_with_a_whole_line(self, rig):
        """Bug class: the kept end of a long output beginning inside a line, which shows the second half of a secret or of a `$ docker compose` line whose start (and so its overwriting) was cut away."""
        rig.start(output_tail_bytes=1000)
        lines = "".join(f"line {number:04d} {DB_PASSWORD[:10]}\n" for number in range(200))
        rig.behave("db.stop", stdout=lines, stderr=lines)
        session = rig.login()

        result = session.call("db.stop", {})

        for name in ("output", "errors"):
            assert len(result[name].encode()) <= 1000
            assert result[name].startswith("line ") and result[name].endswith("line 0199 " + DB_PASSWORD[:10] + "\n")

    def test_a_getter_that_prints_more_than_a_reply_holds_is_ended_with_its_group(self, rig):
        """Bug class: the output of a getter kept in memory without a bound, or the child (and what it started) left running after the answer was given up."""
        rig.start(reply_max_bytes=200_000)
        rig.behave("db.status", stdout_bytes=300_000, grandchild=True, wait_for=str(rig.out / "never"), wait_seconds=30)
        session = rig.login()

        refused = code_of(lambda: session.call("db.status"))

        assert refused.code == "too_large"
        note = rig.calls("db.status")[0]
        # KILL is delivered asynchronously: the group is waited for, not looked at once.
        wait_for("the child and its grandchild to be gone", lambda: not alive(note["pid"]) and not alive(note["grandchild"]))
        assert not rig.done(note["pid"])

    def test_a_read_over_its_time_limit_is_ended_with_its_group(self, rig):
        """Bug class: a read child without a time limit holding one of the few read slots for ever, or ended without what it started (a process that ignores TERM included)."""
        rig.start(read_seconds=0.3)
        rig.behave("db.verify", grandchild=True, ignore_term=True, wait_for=str(rig.out / "never"), wait_seconds=30)
        session = rig.login()

        refused = code_of(lambda: session.call("db.verify"))

        assert refused.code == "timeout"
        note = rig.calls("db.verify")[0]
        wait_for("the child and its grandchild to be gone", lambda: not alive(note["pid"]) and not alive(note["grandchild"]))
        assert not rig.done(note["pid"])

    def test_more_read_children_than_allowed_is_busy(self, rig):
        """Bug class: no bound on read children, so that a client that sends many reads starts as many processes as it likes."""
        rig.start(read_children_max=2)
        gate = rig.out / "gate"
        rig.behave("db.status", wait_for=str(gate))
        replies = []
        threads = [threading.Thread(target=lambda: replies.append(rig.login().call("db.status"))) for _ in range(2)]
        for thread in threads:
            thread.start()
        wait_for("two read children", lambda: len(rig.calls("db.status")) == 2)

        busy = code_of(lambda: rig.login().call("source.list"))
        gate.touch()
        for thread in threads:
            thread.join(15)

        assert busy.code == "busy"
        assert len(replies) == 2 and rig.calls("source.list") == []
        assert rig.login().call("source.list")["data"]["verb"] == "source.list"


# ── 7. the write lock ───────────────────────────────────────────────────────


def _write_lock_is_held(rig):
    try:
        fd = os.open(rig.broker_dir / broker.WRITE_LOCK_NAME, os.O_RDONLY)
    except FileNotFoundError:
        return False
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return False
    except BlockingIOError:
        return True
    finally:
        os.close(fd)


class TestWriteLock:
    def test_one_state_changing_call_at_a_time_a_run_waits_and_a_cancel_goes_through(self, rig):
        """Bug class: two state-changing actions at once (a reset while a stop runs), a second call waiting in a queue the client cannot see, a run started while an action changes the databases, or a cancel held up behind a long action."""
        rig.start()
        gate = rig.out / "gate"
        rig.behave("db.stop", wait_for=str(gate))
        session = rig.login()
        replies = []
        first = threading.Thread(target=lambda: replies.append(session.call("db.stop", {})))
        first.start()
        note = wait_for("the first child", lambda: rig.calls("db.stop"))[0]

        busy = code_of(lambda: session.call("db.unsetup_jobs", step_up=PASSWORD))
        run = code_of(lambda: session.call("run.start", {"profile": "parse", "source": "ok"}))
        cancel = session.call("run.cancel", {"run_id": RUN_ID})
        first_still_running = not rig.done(note["pid"])
        gate.touch()
        first.join(15)

        assert first_still_running
        assert (busy.code, run.code) == ("busy", "busy")
        assert cancel["exit_code"] == 0
        assert replies[0]["exit_code"] == 0
        assert rig.calls("db.unsetup_jobs") == [] and rig.started_runs == []
        assert not _write_lock_is_held(rig)

    def test_runs_can_be_started_at_the_same_time(self, rig):
        """Bug class: the lock of a run start taken exclusive, so that one slow start (a listing of Docker) makes every other start answer busy."""
        rig.start()
        rig.starter_gate = threading.Event()
        replies = []
        first = threading.Thread(target=lambda: replies.append(rig.login().call("run.start", {"profile": "parse", "source": "ok"})))
        first.start()
        wait_for("the first start", lambda: len(rig.started_runs) == 1)

        second = threading.Thread(target=lambda: replies.append(rig.login().call("run.start", {"profile": "lingua", "source": "ok"})))
        second.start()
        both_inside = wait_for("the second start", lambda: len(rig.started_runs) == 2)
        rig.starter_gate.set()
        first.join(15)
        second.join(15)

        assert both_inside
        assert replies == [{"run_id": RUN_ID}, {"run_id": RUN_ID}]

    @pytest.mark.parametrize("verb, fields, step_up", DESTRUCTIVE, ids=[entry[0] for entry in DESTRUCTIVE])
    def test_a_destructive_verb_cannot_look_for_runs_while_one_is_being_started(self, rig, verb, fields, step_up):
        """Bug class: a database reset that looks for live runs, finds none, and runs while a run is being started (its record not written yet), so that the run writes into a database being wiped."""
        rig.start()
        rig.starter_gate = threading.Event()
        starting = threading.Thread(target=lambda: rig.login().call("run.start", {"profile": "parse", "source": "ok"}))
        starting.start()
        wait_for("the start", lambda: rig.started_runs)

        refused = code_of(lambda: rig.login().call(verb, fields, step_up=step_up))
        rig.starter_gate.set()
        starting.join(15)

        assert refused.code == "busy"
        assert rig.lister_calls == [] and rig.calls() == []

    @pytest.mark.parametrize("verb, fields, step_up", DESTRUCTIVE, ids=[entry[0] for entry in DESTRUCTIVE])
    def test_a_destructive_verb_looks_for_runs_only_while_it_holds_the_lock(self, rig, verb, fields, step_up):
        """Bug class: the look for live runs made before the write lock is held, so that a run can start between the look and the action."""
        rig.start()
        session = rig.login()

        session.call(verb, fields, step_up=step_up)

        assert rig.lister_calls == [True]

    def test_an_orphaned_child_runs_to_its_end_and_holds_the_lock_until_then(self, rig):
        """Bug class: the write lock held by the broker alone, so that a broker started again after a crash runs a second action while the first one's child still works; or the child's output in pipes, so that it dies at its next line once the broker is gone; or the output files of a crash never cleared, or cleared while their child still writes."""
        process = rig.spawn_real()
        gate = rig.out / "gate"
        rig.behave("db.stop", wait_for=str(gate), wait_seconds=30, late_stdout_bytes=300_000)
        session = rig.login()
        caller = threading.Thread(target=lambda: code_of(lambda: session.call("db.stop", {})), daemon=True)
        caller.start()
        note = wait_for("the child", lambda: rig.calls("db.stop"))[0]

        process.kill()
        process.wait(10)
        held_after_kill = _write_lock_is_held(rig)
        rig.start()
        kept_while_it_works = sorted(path.suffix for path in (rig.broker_dir / "calls").iterdir())
        rig.stop()
        gate.touch()
        wait_for("the child to end", lambda: rig.done(note["pid"]) and not alive(note["pid"]))
        caller.join(15)
        output = next(path for path in (rig.broker_dir / "calls").iterdir() if path.suffix == ".out")
        written = output.stat().st_size
        rig.start()

        assert held_after_kill
        assert kept_while_it_works == [".err", ".out"]
        assert written == len(json.dumps({"verb": "db.stop", "fields": {}})) + 1 + 300_000
        assert not _write_lock_is_held(rig)
        assert list((rig.broker_dir / "calls").iterdir()) == []

    def test_a_client_that_goes_away_does_not_end_the_call(self, rig):
        """Bug class: a state-changing child ended (half done) because the browser tab was closed, or its end never written to the audit log."""
        rig.start()
        gate = rig.out / "gate"
        rig.behave("db.stop", wait_for=str(gate))
        session = rig.login()
        document = json.dumps({"op": "call", "token": session.token, "verb": "db.stop", "fields": {}})
        connection = raw_exchange(rig.socket_path, frame(document), read=False)
        note = wait_for("the child", lambda: rig.calls("db.stop"))[0]
        connection.close()
        # Longer than any read is given, and than the broker needs to notice the closed connection.
        time.sleep(1.5)
        still_running = alive(note["pid"]) and not rig.done(note["pid"])
        gate.touch()

        ended = wait_for("the call_ended line", lambda: rig.events("call_ended"))
        assert still_running
        assert rig.done(note["pid"])
        assert [(line["verb"], line["outcome"], line["exit_code"]) for line in ended] == [("db.stop", "exited", 0)]

    def test_an_audit_log_that_cannot_be_written_refuses_before_anything_runs(self, rig):
        """Bug class: a state-changing action run while its audit lines are lost (the log's file replaced by something that cannot be written)."""
        rig.start()
        session = rig.login()
        log = rig.broker_dir / audit.FILE_NAME
        kept = rig.broker_dir / "audit.kept"
        os.rename(log, kept)
        log.mkdir()
        try:
            refused = code_of(lambda: session.call("db.stop", {}))
            read = session.call("db.status")
        finally:
            log.rmdir()
            os.rename(kept, log)

        assert refused.code == "audit_not_writable"
        assert [note["verb"] for note in rig.calls()] == ["db.status"]
        assert read["data"]["verb"] == "db.status"
        assert not _write_lock_is_held(rig)

    def test_a_call_whose_audit_line_is_too_long_is_refused_and_never_runs(self, rig):
        """Bug class: a state-changing call run unaudited because its own `call_accepted` line could not be written while the log itself works (`writable()` said yes): the setup verbs take text, and the log's ASCII form writes each non-ASCII character as an escape of six bytes, so a request well under the 64 KiB frame makes a line over the 64 KiB a line may hold. The call is refused with `audit_not_writable`, its child never starts, no source folder is made, and the log takes the next call."""
        rig.start()
        session = rig.login()
        patterns = ["\u00e9" * 256] * 64
        fields = {"name": "wide", "platform": "custom", "data_types": ["posts"], "dumps_path": "d", "extracted_path": "e", "parsed_path": "p", "output_path": "o", "file_format": "parquet", "profiles": ["parse", "lingua"], "remove_patterns": patterns}
        assert len(json.dumps({"op": "call", "token": session.token, "verb": "source.add", "fields": fields}, ensure_ascii=False).encode("utf-8")) < 60000

        refused = code_of(lambda: session.call("source.add", fields, step_up=PASSWORD))

        assert refused.code == "audit_not_writable"
        assert "nothing was done" in str(refused)
        assert rig.calls() == []
        assert not (rig.root / "config" / "sources" / "wide").exists()
        assert [line["code"] for line in rig.events("call_refused")] == ["audit_not_writable"]
        assert rig.events("call_accepted") == []
        assert not _write_lock_is_held(rig)
        session.call("db.stop", {})
        assert [note["verb"] for note in rig.calls()] == ["db.stop"]

    @pytest.mark.parametrize("verb_name, fields", [("db.stop", {}), ("run.start", {"profile": "parse", "source": "ok"})])
    def test_a_write_call_whose_audit_line_fails_never_runs(self, rig, verb_name, fields):
        """Bug class: the same for the write verbs, a run's start included: whatever makes the `call_accepted` line fail (here a log whose line is refused), the child is not started and no run is started."""
        rig.start()
        session = rig.login()
        rig.broker.audit.call_accepted = lambda *args, **kwargs: False

        refused = code_of(lambda: session.call(verb_name, fields))

        assert refused.code == "audit_not_writable"
        assert rig.calls() == [] and rig.started_runs == []
        assert not _write_lock_is_held(rig)


# ── 8. run.logs ─────────────────────────────────────────────────────────────


def _recorded_run(root, log, state="finished"):
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(runstore, "new_run_id", lambda: RUN_ID)
        runstore.create_run(root, {"profile": "parse", "source": "ok", "flags": {}, "started_by": "operator"})
    fd = runstore.open_run_log(root, RUN_ID)
    try:
        os.write(fd, log)
    finally:
        os.close(fd)
    if state != "starting":
        runstore.update_record(root, RUN_ID, state=state)


def _log_with_secrets(secret):
    """About 18 KiB of log with the secret inside pieces of 4096 bytes, across each boundary at several shifts, next to characters of several bytes, and a `$ docker compose ` line."""
    data = bytearray()
    number = 0
    places = [100, 4096 - 7, 8192 - 1, 12288 - len(secret) + 1, 16384 - len(secret) // 2]
    while len(data) < 18_000:
        if places and len(data) >= places[0] - 40:
            data += b"x" * max(0, places[0] - len(data)) + secret.encode() + b"\n"
            places.pop(0)
            continue
        data += f"line {number} \u00e9\u4e2d\U0001f600 text\n".encode()
        number += 1
        if number == 50:
            data += b"  $ docker compose --profile parse run --rm parse\n"
    return bytes(data)


def _held_back(secrets):
    """How many bytes at the end of a log that is still written are held back for these secrets: the longest form of one, less one."""
    read_offset, read_count, cut_start = redact.window(0, 1, secrets)
    return read_count - cut_start - 1


def _read_log(session, offset=0):
    """The pieces of a run's log from an offset until a piece is empty: (the pieces' data, the offset after them)."""
    pieces = []
    while True:
        data = session.call("run.logs", {"run_id": RUN_ID, "offset": offset, "limit": redact.MIN_PIECE_BYTES})["data"]
        if data["text"] == "":
            return pieces, data["next_offset"]
        pieces.append(data)
        offset = data["next_offset"]


class TestRunLogs:
    def test_a_piece_smaller_than_the_minimum_is_refused(self, rig):
        """Bug class: pieces so small that the margins read around each outweigh the piece (a client that walks a log byte by byte)."""
        rig.start()
        session = rig.login()

        refused = code_of(lambda: session.call("run.logs", {"run_id": RUN_ID, "limit": redact.MIN_PIECE_BYTES - 1}))

        assert (refused.code, refused.field) == ("invalid_input", "limit")
        assert rig.calls() == []

    @pytest.mark.parametrize("state", ["finished", "running"])
    def test_a_secret_is_overwritten_wherever_the_pieces_cut_it(self, rig, state):
        """Bug class: each piece overwritten on its own, so that a secret cut by the end of a piece shows its two halves in two replies; offsets that do not add up to the file."""
        rig.start()
        whole = _log_with_secrets(DB_PASSWORD)
        _recorded_run(rig.root, whole, state="finished" if state == "finished" else "running")
        held = runstore.acquire_run_lock(rig.root, RUN_ID) if state == "running" else None
        try:
            session = rig.login()
            session.set_db_password(DB_PASSWORD, step_up=PASSWORD)
            pieces, offset = [], 0
            # A run that has not ended is served up to the bytes held back at the end of its file; an ended one to its end.
            served_to = len(whole) - (_held_back([DB_PASSWORD]) if state == "running" else 0)
            while True:
                data = session.call("run.logs", {"run_id": RUN_ID, "offset": offset, "limit": redact.MIN_PIECE_BYTES})["data"]
                assert data["state"] == state and data["complete"] is (state == "finished") and data["kind"] == "web"
                if data["text"] == "":
                    assert data["next_offset"] == served_to
                    break
                assert data["next_offset"] == offset + len(data["text"].encode())
                pieces.append(data["text"])
                offset = data["next_offset"]
        finally:
            if held is not None:
                runstore.release_run_lock(held)

        joined = "".join(pieces)
        assert len(pieces) >= 4
        assert DB_PASSWORD[:6] not in joined and DB_PASSWORD[-6:] not in joined
        assert joined.encode() == redact.redact_bytes(whole, [DB_PASSWORD])[0][:served_to]
        assert "  $ docker compose " + "*" * len("--profile parse run --rm parse") in joined
        assert rig.calls() == []

    def test_the_password_of_an_ended_session_is_still_overwritten(self, rig):
        """Bug class: a secret dropped from the redaction list when its session ends, so the log of a run started with it shows it to the next session."""
        rig.start()
        _recorded_run(rig.root, _log_with_secrets(DB_PASSWORD))
        first = rig.login()
        first.set_db_password(DB_PASSWORD, step_up=PASSWORD)
        first.logout()
        second = rig.login()

        data = second.call("run.logs", {"run_id": RUN_ID, "offset": 4000, "limit": 4096})["data"]

        assert "*" * len(DB_PASSWORD) in data["text"] and DB_PASSWORD not in data["text"]


# ── 9. what leaves is overwritten ───────────────────────────────────────────


class TestRedaction:
    def test_a_secret_in_a_getters_data_values_and_keys(self, rig, monkeypatch):
        """Bug class: data handed out as the command produced it: a database password in a status value or key, the Hugging Face token of the broker's environment."""
        monkeypatch.setenv("HF_TOKEN", HF_TOKEN)
        rig.start()
        session = rig.login()
        session.set_db_password(DB_PASSWORD, step_up=PASSWORD)
        rig.behave("db.status", stdout=json.dumps({"user": f"x{DB_PASSWORD}y", DB_PASSWORD: [HF_TOKEN], "port": 5432}))

        data = session.call("db.status")["data"]

        stars = "*" * len(DB_PASSWORD)
        assert data == {"user": f"x{stars}y", stars: ["*" * len(HF_TOKEN)], "port": 5432}

    def test_a_secret_and_the_compose_line_in_a_state_changing_verbs_output(self, rig):
        """Bug class: the output of an action passed on as printed: the password of this request, the `$ docker compose` line (which can hold host paths and values the CLI put into it)."""
        rig.start()
        rig.behave("db.start", stdout=f"  $ docker compose --profile mongo up -d\n  started {DB_PASSWORD}\n", stderr=f"  Warning: {DB_PASSWORD}\n")
        session = rig.login()

        result = session.call("db.start", {"service": "mongo", "admin_password": DB_PASSWORD}, step_up=PASSWORD)

        stars = "*" * len(DB_PASSWORD)
        assert result["output"] == "  $ docker compose " + "*" * len("--profile mongo up -d") + f"\n  started {stars}\n"
        assert result["errors"] == f"  Warning: {stars}\n"

    def test_data_beyond_the_redaction_budget_is_too_large_and_never_sent(self, rig):
        """Bug class: data that cannot be overwritten in full sent as it is, or in part."""
        rig.start()
        marker = "unsent-marker-3f9a"
        strings = [marker + "a" * 200_000 for _ in range(25)]
        rig.behave("db.status", stdout=json.dumps(strings))
        session = rig.login()
        document = json.dumps({"op": "call", "token": session.token, "verb": "db.status", "fields": {}})

        received = raw_exchange(rig.socket_path, frame(document))

        assert decoded_reply(received)["error"]["code"] == "too_large"
        assert marker.encode() not in received


# ── 10. transport ───────────────────────────────────────────────────────────


class TestTransport:
    def test_an_oversized_request_is_refused_before_it_is_read(self, rig):
        """Bug class: a request read whole whatever its length (memory, and the time a parser and a validator spend on it)."""
        rig.start()

        received = raw_exchange(rig.socket_path, struct.pack(">I", framing.REQUEST_MAX_BYTES + 1) + b"{")

        assert decoded_reply(received)["error"]["code"] == "request_too_large"

    def test_a_reply_over_the_limit_is_too_large(self, rig):
        """Bug class: a reply larger than a client reads, sent anyway (a client that then refuses it, or memory)."""
        rig.start(reply_max_bytes=2000)
        session = rig.login()
        for _ in range(20):
            session.call("db.stop", {})

        refused = code_of(lambda: session.call("audit.list", {"limit": 50}))

        assert refused.code == "too_large"

    def test_a_connection_that_sends_nothing_is_closed_at_its_deadline(self, rig):
        """Bug class: a connection held open for ever by a client that sends nothing, or a frame given more time piece by piece (a deadline per read instead of one for the request)."""
        rig.start(request_seconds=0.3)

        # Closed by the broker, without a reply, long before the client's own limit.
        assert raw_exchange(rig.socket_path, b"", timeout=30) == b""

        # A request sent one byte every 0.1 seconds: each read is quick, the whole is not. With one deadline for the whole request the broker closes the connection long before the last byte; with one per read it would read it all and answer.
        trickled = frame('{"op":"hello","padding":"' + "p" * 200 + '"}')
        connection = raw_exchange(rig.socket_path, b"", read=False)
        sent = [0]

        def trickle():
            for index in range(len(trickled)):
                try:
                    connection.sendall(trickled[index:index + 1])
                except OSError:
                    return
                sent[0] += 1
                time.sleep(0.1)

        sender = threading.Thread(target=trickle, daemon=True)
        sender.start()
        try:
            try:
                answer = connection.recv(65536)
            except ConnectionResetError:
                answer = b""
        finally:
            connection.close()
            sender.join(60)
        assert answer == b""
        assert sent[0] < len(trickled)
        rig.stop()
        assert sum(line["unauthenticated"] for line in rig.events("counts")) == 2

    def test_a_client_that_never_reads_its_reply_is_given_up(self, rig):
        """Bug class: a handler thread held for ever by a client that does not read (one per connection, until the broker cannot answer anyone)."""
        rig.start(send_seconds=0.3)
        rig.behave("db.status", stdout=json.dumps(["y" * 3_000_000]))
        session = rig.login()
        document = json.dumps({"op": "call", "token": session.token, "verb": "db.status", "fields": {}})
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        connection.connect(rig.socket_path)
        try:
            connection.sendall(frame(document))
            wait_for("the handler to give up", lambda: "a reply could not be sent (TimeoutError)" in rig.broker_log())
        finally:
            connection.close()
        rig.stop()

    def test_more_connections_than_allowed_are_closed_at_once(self, rig):
        """Bug class: no bound on connections, so that a client opens threads until the process runs out."""
        rig.start(connections_max=2, request_seconds=60)
        held = [raw_exchange(rig.socket_path, b"", read=False) for _ in range(2)]
        try:
            wait_for("two handlers", lambda: len(rig.broker._connections) == 2)
            # Closed without a reply while the two others are still held open by their handlers: not by a deadline.
            assert raw_exchange(rig.socket_path, frame('{"op":"hello"}'), timeout=30) == b""
            assert len(rig.broker._connections) == 2
        finally:
            for connection in held:
                connection.close()
        wait_for("the handlers to end", lambda: len(rig.broker._connections) == 0)
        assert rig.client().hello()["protocol"] == 1

    def test_two_clients_at_once(self, rig):
        """Bug class: one connection at a time (a long read holding every other client)."""
        rig.start()
        gate = rig.out / "gate"
        rig.behave("db.status", wait_for=str(gate))
        session = rig.login()
        replies = []
        slow = threading.Thread(target=lambda: replies.append(session.call("db.status")))
        slow.start()
        wait_for("the first child", lambda: rig.calls("db.status"))

        assert session.call("source.list")["data"]["verb"] == "source.list"
        gate.touch()
        slow.join(15)
        assert replies[0]["data"]["verb"] == "db.status"

    def test_a_frame_that_repeats_a_key_is_refused(self, rig):
        """Bug class: two parsers disagreeing about a field (the broker reads one `op` or `token`, a proxy or a log reads the other)."""
        rig.start()
        session = rig.login()
        text = '{"op":"call","token":"%s","verb":"db.status","verb":"db.unsetup_jobs","fields":{}}' % session.token

        received = raw_exchange(rig.socket_path, frame(text))

        assert decoded_reply(received)["error"]["code"] == "bad_request"
        assert rig.calls() == []


# ── 11. the audit log ───────────────────────────────────────────────────────


class TestAudit:
    def test_what_gets_a_line_and_what_is_only_counted(self, rig):
        """Bug class: one line per failed login or tokenless request (a client fills the log), or a login, logout, session end or call that leaves no line."""
        rig.start()
        for _ in range(2):
            code_of(lambda: rig.client().login("wrong-password-1"))
        code_of(lambda: rig.client().call("db.status"))
        session = rig.login()
        session.call("db.status")
        session.call("source.list")
        code_of(lambda: session.call("db.nope"))
        session.call("db.stop", {})
        session.logout()
        other = rig.login()
        set_password(rig.root, OTHER_PASSWORD)
        code_of(lambda: other.call("db.status"))
        rig.stop()

        events = [line["event"] for line in rig.audit_lines()]
        assert events == ["broker_start", "login", "call_refused", "call_accepted", "call_ended", "logout", "login", "session_end", "counts", "broker_stop"]
        counts = rig.events("counts")[0]
        assert (counts["failed_logins"], counts["unauthenticated"], counts["reads"]) == (2, 2, {"1": 2})

    def test_no_secret_and_no_token_in_the_logs_the_replies_or_the_argument_lists(self, rig):
        """Bug class: a password or token written into the audit log, the broker's log, a reply or a child's command line on some path (a refusal, an audit line of fields, an error)."""
        rig.start()
        session = rig.login()
        token = session.token
        rig.behave("db.start", stdout="started\n")
        replies = []
        documents = [
            {"op": "session.set_db_password", "token": token, "step_up": PASSWORD, "password": DB_PASSWORD},
            {"op": "call", "token": token, "verb": "db.start", "fields": {"service": "mongo", "admin_password": DB_PASSWORD}, "step_up": PASSWORD},
            {"op": "call", "token": token, "verb": "db.start", "fields": {"admin_password": DB_PASSWORD + "'"}, "step_up": PASSWORD},
            {"op": "call", "token": token, "verb": "run.start", "fields": {"profile": "parse", "source": "ok"}},
            {"op": "call", "token": token, "verb": "run.start", "fields": {"profile": "parse", "source": "ok", "admin_password": DB_PASSWORD, "colour": DB_PASSWORD}},
            {"op": "call", "token": token, "verb": "db.unsetup_jobs", "fields": {}, "step_up": "wrong-" + PASSWORD},
            {"op": "call", "token": token, "verb": "db.status", "fields": {}},
            {"op": "logout", "token": token},
            {"op": "call", "token": token, "verb": "db.status", "fields": {}},
        ]
        for document in documents:
            replies.append(raw_exchange(rig.socket_path, frame(json.dumps(document))))
        rig.stop()

        files_text = (rig.broker_dir / audit.FILE_NAME).read_bytes() + (rig.broker_dir / broker.LOG_NAME).read_bytes()
        for planted in (PASSWORD, DB_PASSWORD, token):
            assert planted.encode() not in files_text
            assert all(planted.encode() not in reply for reply in replies)
            assert all(planted not in " ".join(note["argv"]) for note in rig.calls())
        assert [json.loads(note["stdin"])["fields"].get("admin_password") for note in rig.calls("db.start")] == [DB_PASSWORD]

    def test_audit_list_returns_the_newest_lines(self, rig):
        """Bug class: the oldest lines, or lines of the broker's own choosing, answered for "the newest"."""
        rig.start()
        session = rig.login()
        for _ in range(4):
            session.call("db.stop", {})

        lines = session.call("audit.list", {"limit": 3})["data"]

        assert lines == rig.audit_lines()[-3:]
        assert [line["event"] for line in lines] == ["call_ended", "call_accepted", "call_ended"]

    def test_the_chain_verifies_after_a_mixed_session_and_a_restart(self, rig):
        """Bug class: two writers, or a writer that does not continue the chain of the file it found, breaking the chain at every start."""
        rig.start()
        session = rig.login()
        session.call("db.stop", {})
        code_of(lambda: session.call("db.reset", {"db": "mongo"}))
        rig.stop()
        assert audit.verify(rig.broker_dir / audit.FILE_NAME).ok
        rig.start()
        again = rig.login()
        again.call("db.unsetup_jobs", step_up=PASSWORD)
        rig.stop()

        report = audit.verify(rig.broker_dir / audit.FILE_NAME)
        assert report.ok and report.breaks == 0 and report.tail_changed == 0
        assert [line["event"] for line in rig.audit_lines()].count("broker_start") == 2


# ── 12. stopping ────────────────────────────────────────────────────────────


class TestStopping:
    def test_term_waits_for_the_running_action_then_stops_cleanly(self, rig):
        """Bug class: a stop that kills a database reset half way, or ends without its last audit line, or leaves the socket behind."""
        process = rig.spawn_real()
        gate = rig.out / "gate"
        rig.behave("db.stop", wait_for=str(gate), wait_seconds=30)
        session = rig.login()
        replies = []
        caller = threading.Thread(target=lambda: replies.append(session.call("db.stop", {})))
        caller.start()
        note = wait_for("the child", lambda: rig.calls("db.stop"))[0]

        process.send_signal(signal.SIGTERM)
        time.sleep(0.3)
        still_running = process.poll() is None and alive(note["pid"])
        gate.touch()
        code = process.wait(15)
        caller.join(15)

        assert still_running
        assert code == 0
        assert replies == [{"exit_code": 0, "output": json.dumps({"verb": "db.stop", "fields": {}}) + "\n", "errors": ""}]
        assert [line["event"] for line in rig.audit_lines()][-2:] == ["call_ended", "broker_stop"]
        assert not os.path.exists(rig.socket_path)

    def test_usr1_ends_the_running_action_first(self, rig):
        """Bug class: a forced stop that waits like a normal one, or that leaves the action's process group (one that ignores TERM included) running."""
        process = rig.spawn_real()
        rig.behave("db.stop", wait_for=str(rig.out / "never"), wait_seconds=30, ignore_term=True, grandchild=True)
        session = rig.login()
        replies = []
        caller = threading.Thread(target=lambda: replies.append(session.call("db.stop", {})))
        caller.start()
        note = wait_for("the child", lambda: rig.calls("db.stop"))[0]

        process.send_signal(signal.SIGUSR1)
        code = process.wait(15)
        caller.join(15)

        assert code == 0
        wait_for("the child and its grandchild to be gone", lambda: not alive(note["pid"]) and not alive(note["grandchild"]))
        assert replies[0]["exit_code"] == -signal.SIGKILL
        assert [(line["outcome"], line["exit_code"]) for line in rig.events("call_ended")] == [("forced", -signal.SIGKILL)]
        assert rig.audit_lines()[-1]["event"] == "broker_stop"
        assert not os.path.exists(rig.socket_path)

    def test_a_recorded_runs_supervisor_survives_the_stop(self, rig):
        """Bug class: a broker that takes the pipeline runs it started down with it (a run the operator did not ask to stop)."""
        process = rig.spawn_real()
        session = rig.login()
        assert session.call("run.start", {"profile": "parse", "source": "ok"}) == {"run_id": RUN_ID}
        supervisor = int(wait_for("the supervisor", lambda: (rig.out / "supervisor.pid").exists() and (rig.out / "supervisor.pid").read_text()))

        process.send_signal(signal.SIGTERM)
        assert process.wait(15) == 0

        assert alive(supervisor)
        os.kill(supervisor, signal.SIGKILL)


# ── review round 1: the log read from its file ──────────────────────────────


class TestRunLogsHeldBack:
    def test_pieces_equal_the_whole_log_overwritten_then_cut_with_a_compose_prefix_inside_a_line(self, rig):
        """Bug class: a piece read from the file overwritten a second time as text of its own, whose first byte is taken for the start of a line, so that `$ docker compose ` in the middle of a line at a piece's start is overwritten in that piece and not in the whole log."""
        rig.start()
        head = b"x" * (redact.MIN_PIECE_BYTES - 10) + b"\nmid-line "
        whole = head + b"$ docker compose --profile parse run parse\n" + b"tail line\n" * 500
        assert whole[redact.MIN_PIECE_BYTES:redact.MIN_PIECE_BYTES + 17] == b"$ docker compose "
        _recorded_run(rig.root, whole)
        session = rig.login()

        pieces, end = _read_log(session)

        joined = "".join(piece["text"] for piece in pieces)
        assert joined.encode() == redact.redact_bytes(whole, environ.known_secrets(os.environ))[0]
        assert "mid-line $ docker compose --profile parse run parse\n" in joined
        assert end == len(whole)

    def test_a_secret_written_in_two_parts_is_never_served_half_covered(self, rig):
        """Bug class: the end of a log that is still being written served as it is, so that a secret whose first part is written and whose second is not yet shows its first part, which nothing overwrites."""
        rig.start()
        half = len(DB_PASSWORD) // 2
        _recorded_run(rig.root, b"line\n" * 100 + b"password follows: " + DB_PASSWORD[:half].encode(), state="running")
        held = runstore.acquire_run_lock(rig.root, RUN_ID)
        try:
            session = rig.login()
            session.set_db_password(DB_PASSWORD, step_up=PASSWORD)
            before, offset = _read_log(session)
            fd = runstore.open_run_log(rig.root, RUN_ID)
            os.write(fd, DB_PASSWORD[half:].encode() + b"\n" + b"more\n" * 50)
            os.close(fd)
            after, offset = _read_log(session, offset)
        finally:
            runstore.release_run_lock(held)
        runstore.update_record(rig.root, RUN_ID, state="finished")
        last, end = _read_log(session, offset)

        whole = (rig.root / ".sdp" / "runs" / RUN_ID / "log").read_bytes()
        texts = [piece["text"] for piece in before + after + last]
        assert all(DB_PASSWORD[:half] not in text for text in texts)
        assert "".join(texts).encode() == redact.redact_bytes(whole, [DB_PASSWORD])[0]
        assert end == len(whole)
        assert [piece["complete"] for piece in before + after] == [False] * len(before + after) and [piece["complete"] for piece in last] == [True] * len(last)

    @pytest.mark.parametrize("state", ["starting", "unknown", "lost"])
    def test_every_state_that_is_not_final_holds_the_end_back(self, rig, fake_run_docker, state):
        """Bug class: only `running` taken for "still written", so that a run whose supervisor is gone, or one Docker could not be asked about, has its last bytes served while its container may still write the rest of a secret."""
        whole = b"line\n" * 200
        _recorded_run(rig.root, whole, state="starting" if state == "starting" else "running")
        held = runstore.acquire_run_lock(rig.root, RUN_ID) if state == "starting" else None
        fake_run_docker.reachable = state != "unknown"
        try:
            rig.start()
            session = rig.login()
            session.set_db_password(DB_PASSWORD, step_up=PASSWORD)
            pieces, end = _read_log(session)
        finally:
            if held is not None:
                runstore.release_run_lock(held)

        assert {piece["state"] for piece in pieces} == {state}
        assert end == len(whole) - _held_back([DB_PASSWORD])
        assert not any(piece["complete"] for piece in pieces)

    def test_a_password_sent_in_the_fields_is_overwritten_in_a_later_log(self, rig):
        """Bug class: a database password that a request carried (and not a session) forgotten once the call has run, so that the log of the run started with it shows it."""
        rig.start()
        rig.login().call("run.start", {"profile": "parse", "source": "ok", "admin_password": DB_PASSWORD})
        _recorded_run(rig.root, b"x" * 100 + DB_PASSWORD.encode() + b"\n")

        text = rig.login().call("run.logs", {"run_id": RUN_ID})["data"]["text"]

        assert DB_PASSWORD not in text and "*" * len(DB_PASSWORD) in text

    def test_a_task_goes_to_the_task_starter_and_its_secret_is_overwritten_in_its_log(self, rig, monkeypatch):
        """Bug class: a verb of the task allow-list run as a plain state-changing child (no record, no Cancel, the exclusive write lock held for a whole download), handed to the starter without its token, or its token forgotten once the call has run, so that the log of the task, read later, shows it."""
        token = "made-up-hub-token-31f0c9"
        task_verb = verbs.Verb(name="test.task", cls="write", cli=("test", "task"), socket_inputs={"source": verbs.Field(Name(), required=True), "token": verbs.Field(Secret())}, socket_enabled=True, action="do_test_task")
        monkeypatch.setattr(verbs, "VERBS", MappingProxyType({**verbs.VERBS, "test.task": task_verb}))
        monkeypatch.setattr(tasks, "TASKS", MappingProxyType({"test.task": tasks.Task("test.task", key=("source",), label="test {source}")}))
        rig.start()

        assert rig.login().call("test.task", {"source": "ok", "token": token}) == {"run_id": RUN_ID}
        _recorded_run(rig.root, b"x" * 100 + token.encode() + b"\n")
        text = rig.login().call("run.logs", {"run_id": RUN_ID})["data"]["text"]

        ((root, verb_name, fields, started_by),) = rig.started_tasks
        assert (root, verb_name, fields) == (os.path.realpath(rig.root), "test.task", {"source": "ok", "token": token}) and started_by.startswith("web-")
        assert rig.started_runs == [] and not list((rig.broker_dir / "calls").iterdir())
        assert token not in text and "*" * len(token) in text


# ── review round 1: signals, start failures, the password file, accept, stopping ──


def _stand_ins_alive(rig):
    """The stand-in children of this test that are running, found by their command line."""
    found = []
    for name in os.listdir("/proc"):
        if name.isdigit():
            try:
                if str(rig.script).encode() in Path(f"/proc/{name}/cmdline").read_bytes() and alive(int(name)):
                    found.append(int(name))
            except OSError:
                pass
    return found


def _open_descriptors():
    return len(os.listdir("/proc/self/fd"))


class TestSignalsAndFailures:
    def test_a_signalled_stop_takes_no_lock_and_the_forced_end_follows(self, rig):
        """Bug class: a signal handler that takes the lock the main thread holds when the signal arrives (in the accept or the stop), so that USR1 hangs the broker for ever."""
        rig.start()
        rig.behave("db.stop", wait_for=str(rig.out / "never"), wait_seconds=30)
        session = rig.login()
        replies = []
        caller = threading.Thread(target=lambda: replies.append(session.call("db.stop", {})))
        caller.start()
        wait_for("the child", lambda: rig.calls("db.stop"))
        returned = threading.Event()
        with rig.broker._state_lock:
            threading.Thread(target=lambda: (rig.broker.signal_stop(force=True), returned.set()), daemon=True).start()
            came_back = returned.wait(5)
        server, thread = rig.running.pop()
        thread.join(30)
        caller.join(15)

        assert came_back
        assert not thread.is_alive()
        assert replies[0]["exit_code"] == -signal.SIGTERM
        assert [line["outcome"] for line in rig.events("call_ended")] == ["forced"]

    def test_usr1_while_connections_are_accepted_stops_the_broker(self, rig):
        """Bug class: a forced stop that hangs when its signal arrives while the main thread accepts connections."""
        process = rig.spawn_real()
        busy = threading.Event()

        def hammer():
            while not busy.is_set():
                try:
                    rig.client(timeout=5).hello()
                except client.BrokerError:
                    pass

        threads = [threading.Thread(target=hammer, daemon=True) for _ in range(4)]
        for thread in threads:
            thread.start()
        time.sleep(0.2)
        for _ in range(5):
            process.send_signal(signal.SIGUSR1)
        try:
            code = process.wait(30)
        finally:
            busy.set()
            for thread in threads:
                thread.join(10)

        assert code == 0
        assert rig.audit_lines()[-1]["event"] == "broker_stop"

    def test_a_thread_that_cannot_be_started_ends_the_child_and_still_writes_the_end(self, rig, monkeypatch):
        """Bug class: a failure after `call_accepted` that leaves no `call_ended`, descriptors open, and a child that nobody feeds or drains holding the write lock, so that every later state-changing call is busy."""
        rig.start()
        rig.behave("db.stop", wait_for=str(rig.out / "never"), wait_seconds=30)
        session = rig.login()
        before = _open_descriptors()
        real_start = threading.Thread.start

        def failing(thread):
            if getattr(getattr(thread, "_target", None), "__name__", "") == "_feed":
                raise RuntimeError("can't start new thread")
            return real_start(thread)

        monkeypatch.setattr(threading.Thread, "start", failing)
        refused = code_of(lambda: session.call("db.stop", {}))
        monkeypatch.setattr(threading.Thread, "start", real_start)

        assert refused.code == "internal"
        assert [(line["verb"], line["outcome"], line["exit_code"]) for line in rig.events("call_ended")] == [("db.stop", "internal", None)]
        wait_for("no stand-in child left", lambda: not _stand_ins_alive(rig))
        wait_for("the descriptors to be closed", lambda: _open_descriptors() <= before)
        assert not _write_lock_is_held(rig)
        assert list((rig.broker_dir / "calls").iterdir()) == []
        assert session.call("db.unsetup_jobs", step_up=PASSWORD)["exit_code"] == 0

    def test_a_reader_that_cannot_be_started_ends_a_child_that_already_has_its_request(self, rig, monkeypatch):
        """Bug class: a read child that got its request while the thread that reads its output could not be started, left running: it fills its pipe, nobody drains it, and the broker waits for it for ever (a read slot gone for good)."""
        rig.start(read_children_max=1)
        rig.behave("db.status", stdout="{}", wait_for=str(rig.out / "never"), wait_seconds=30)
        session = rig.login()
        before = _open_descriptors()
        real_start = threading.Thread.start

        # The reader of standard error fails, after the request was fed and standard output is being read: nothing about the pipes ends this child.
        def failing(thread):
            if getattr(getattr(thread, "_target", None), "__name__", "") == "_read" and thread._args[1] == "stderr":
                raise RuntimeError("can't start new thread")
            return real_start(thread)

        monkeypatch.setattr(threading.Thread, "start", failing)
        refused = code_of(lambda: session.call("db.status"))
        monkeypatch.setattr(threading.Thread, "start", real_start)

        assert refused.code == "internal"
        wait_for("no stand-in child left", lambda: not _stand_ins_alive(rig))
        wait_for("the descriptors to be closed", lambda: _open_descriptors() <= before)
        rig.behave("db.status")
        assert session.call("db.status")["data"]["verb"] == "db.status"

    @pytest.mark.parametrize("verb, fields, step_up", [("db.stop", {}, None), ("db.status", {}, None)])
    def test_an_eventfd_that_cannot_be_made_starts_nothing_and_holds_nothing(self, rig, monkeypatch, verb, fields, step_up):
        """Bug class: a descriptor that cannot be made (the process is out of them) before any `try`, leaving the call without its end, the lock or a read slot taken."""
        rig.start(read_children_max=1)
        session = rig.login()

        def no_eventfd(*args):
            raise OSError(errno.EMFILE, "Too many open files")

        monkeypatch.setattr(os, "eventfd", no_eventfd)
        refused = code_of(lambda: session.call(verb, fields, step_up=step_up))
        monkeypatch.undo()

        assert refused.code == "start_failed"
        assert rig.calls() == []
        assert [line["outcome"] for line in rig.events("call_ended")] == (["start_failed"] if verb == "db.stop" else [])
        assert not _write_lock_is_held(rig)
        assert session.call(verb, fields, step_up=step_up) is not None

    def test_an_unusable_password_file_is_said_and_never_taken_for_no_password(self, rig):
        """Bug class: a password file that cannot be used answered as "a password is set" (or as "none is set"), or a session that goes on working with it."""
        rig.start()
        session = rig.login()
        (rig.broker_dir / "password.json").write_text("{}")

        hello = rig.client().hello()
        login = code_of(lambda: rig.client().login(PASSWORD))
        call = code_of(lambda: session.call("db.status"))
        set_password = code_of(lambda: session.set_db_password(DB_PASSWORD, step_up=PASSWORD))
        running_status = broker.broker_status(rig.root)
        rig.stop()

        assert hello == {"protocol": 1, "password_set": None}
        assert (login.code, call.code, set_password.code) == ("password_file_not_usable",) * 3
        assert running_status["running"] is True and running_status["password_set"] is None
        assert broker.broker_status(rig.root)["password_set"] is None
        assert rig.calls() == []

    def test_no_descriptor_for_a_connection_rests_the_listener(self, rig):
        """Bug class: an accept that fails because the process has no descriptor left, retried at once in a loop at full speed (the listener stays readable while the connection waits)."""
        rig.start()
        tries = []
        real = rig.broker._take_connection
        failing_until = time.monotonic() + 0.5

        def failing():
            tries.append(1)
            if time.monotonic() < failing_until:
                raise OSError(errno.EMFILE, "Too many open files")
            return real()

        rig.broker._take_connection = failing
        answer = rig.client(timeout=30).hello()

        assert answer["protocol"] == 1
        assert 1 < len(tries) <= 20

    def test_once_a_stop_is_asked_new_requests_are_refused_and_started_ones_finish(self, rig):
        """Bug class: new logins and calls accepted while the broker is stopping (a state-changing call it would then wait for), or a call that was running cut short by the stop."""
        rig.start()
        gate = rig.out / "gate"
        rig.behave("db.stop", wait_for=str(gate))
        session = rig.login()
        replies = []
        caller = threading.Thread(target=lambda: replies.append(session.call("db.stop", {})))
        caller.start()
        wait_for("the child", lambda: rig.calls("db.stop"))
        documents = [
            {"op": "call", "token": session.token, "verb": "db.status", "fields": {}},
            {"op": "login", "password": PASSWORD},
            {"op": "session.set_db_password", "token": session.token, "step_up": PASSWORD, "password": DB_PASSWORD},
            {"op": "session.clear_db_password", "token": session.token},
            {"op": "hello"},
            {"op": "logout", "token": session.token},
        ]
        pending = [raw_exchange(rig.socket_path, b"", read=False) for _ in documents]
        wait_for("the handlers", lambda: len(rig.broker._connections) == 1 + len(documents))

        rig.broker.request_stop()
        wait_for("the listener to close", lambda: rig.broker._listener is None)
        answers = []
        for connection, document in zip(pending, documents):
            connection.sendall(frame(json.dumps(document)))
            received = b""
            while True:
                chunk = connection.recv(65536)
                if not chunk:
                    break
                received += chunk
            connection.close()
            answers.append(decoded_reply(received))
        with pytest.raises(client.BrokerUnreachable):
            rig.client().hello()
        gate.touch()
        caller.join(15)
        server, thread = rig.running.pop()
        thread.join(30)

        assert [answer["error"]["code"] if not answer["ok"] else "ok" for answer in answers] == ["stopping", "stopping", "stopping", "stopping", "ok", "ok"]
        assert answers[4]["result"]["protocol"] == 1
        assert replies[0]["exit_code"] == 0
        assert rig.calls("db.status") == []



# ── review round 2 ──────────────────────────────────────────────────────────


class TestRoundTwo:
    def test_the_group_is_asked_about_once_per_group_in_one_check(self, rig):
        """Bug class: the account list read once for every path the group may write (every folder and module of the package with a umask of 002), so that on a large directory of accounts a start takes longer than its wait."""
        groups = [os.getegid()] + [gid for gid in os.getgroups() if gid != os.getegid()][:1]
        for index in range(30):
            path = rig.root / "social_data_pipeline" / f"module_{index}.py"
            path.write_text("")
            os.chmod(path, 0o664)
            os.chown(path, -1, groups[index % len(groups)])
        os.chmod(rig.root / "social_data_pipeline", 0o775)
        asked = []

        def private_group(gid, uid):
            asked.append((gid, uid))
            return True

        broker.check_deployment(rig.root, os.geteuid(), private_group)

        assert sorted(asked) == sorted({(gid, os.geteuid()) for gid in groups})

    def test_the_sweep_removes_leftovers_only_while_the_lock_is_free(self, rig):
        """Bug class: the output files an orphaned child left (which can hold secrets before they were overwritten) kept until the next start, or deleted while a child of a crashed broker still writes them."""
        rig.start(housekeeping_seconds=0.1)
        calls = rig.broker_dir / "calls"
        fd = os.open(rig.broker_dir / broker.WRITE_LOCK_NAME, os.O_RDWR | os.O_CREAT, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            for name in ("7-0a0b0c0d.out", "7-0a0b0c0d.err"):
                (calls / name).write_text(f"left by an orphan: {DB_PASSWORD}\n")
            while_held = rig.broker._sweep_calls_folder()
            kept = sorted(path.name for path in calls.iterdir())
        finally:
            os.close(fd)

        wait_for("the housekeeping to sweep", lambda: list(calls.iterdir()) == [])
        assert while_held is None
        assert kept == ["7-0a0b0c0d.err", "7-0a0b0c0d.out"]

    def test_the_sweep_never_removes_the_files_of_a_running_cancel(self, rig):
        """Bug class: the files of a `run.cancel` child (which holds no write lock, so a sweep can run beside it) deleted while it writes them, so that its answer is lost."""
        rig.start()
        gate = rig.out / "gate"
        rig.behave("run.cancel", stdout="  Run cancelled.\n", wait_for=str(gate))
        session = rig.login()
        replies = []
        caller = threading.Thread(target=lambda: replies.append(session.call("run.cancel", {"run_id": RUN_ID})))
        caller.start()
        wait_for("the child", lambda: rig.calls("run.cancel"))
        calls = rig.broker_dir / "calls"
        (calls / "9-0f0e0d0c.out").write_text("left over\n")

        swept = rig.broker._sweep_calls_folder()
        left = sorted(path.suffix for path in calls.iterdir())
        gate.touch()
        caller.join(15)

        assert swept == 1
        assert left == [".err", ".out"]
        assert replies == [{"exit_code": 0, "output": "  Run cancelled.\n", "errors": ""}]

    def test_a_short_password_in_the_fields_is_covered_in_its_own_reply_only(self, rig):
        """Bug class: a short password sent with a request shown in that request's own reply; or remembered, so that a word of seven letters is blanked in every later reply."""
        rig.start()
        rig.behave("db.start", stdout="started with short77\n")
        rig.behave("db.status", stdout=json.dumps({"note": "short77 again"}))
        session = rig.login()

        own = session.call("db.start", {"service": "mongo", "admin_password": "short77"}, step_up=PASSWORD)
        later = session.call("db.status")

        assert own["output"] == "started with *******\n"
        assert later["data"] == {"note": "short77 again"}

    def test_the_audit_view_is_not_blanked_by_words_sent_as_passwords(self, rig):
        """Bug class: the audit view overwritten with what a token holder sent as a password (with an earlier request, or with this one as its second check), so that whoever holds a token can blank the event names or fields of the lines that record what it did."""
        rig.start()
        session = rig.login()
        session.call("run.start", {"profile": "parse", "source": "ok", "admin_password": "call_accepted"})

        lines = session.call("audit.list", {"limit": 10}, step_up="call_ended")["data"]

        assert [line["event"] for line in lines][-3:] == ["login", "call_accepted", "call_ended"]
        assert lines[-1]["verb"] == "run.start"



# ── review round 3 ──────────────────────────────────────────────────────────


class TestRoundThree:
    def test_a_serving_loop_that_fails_still_stops_the_broker(self, rig):
        """Bug class: a serving loop that ends on an error (a selector that fails, memory) leaving a broker that never stops: it keeps its lock, its socket and its housekeeping, and nothing acts on a stop signal any more."""
        server = rig.start()

        def failing():
            raise OSError(errno.EBADF, "Bad file descriptor")

        server._accept = failing
        with pytest.raises(client.BrokerUnreachable):
            rig.client(timeout=5).hello()
        _, thread = rig.running.pop()
        thread.join(15)

        assert not thread.is_alive()
        assert not os.path.exists(rig.socket_path)
        assert rig.audit_lines()[-1]["event"] == "broker_stop"
        assert broker.running_broker(rig.root) is None
        assert "the serving loop stopped on an error (OSError)" in rig.broker_log()

    def test_a_sweep_of_a_folder_without_leftovers_never_takes_the_lock(self, rig):
        """Bug class: a sweep that takes the write lock every 30 seconds whatever the folder holds, so that a call that tries the lock at that instant is answered `busy` while no other call runs."""
        server = rig.start()
        tries = []
        real = server._take_write_lock

        def recorded(**kwargs):
            tries.append(kwargs)
            return real(**kwargs)

        server._take_write_lock = recorded
        empty = server._sweep_calls_folder()
        (rig.broker_dir / "calls" / "5-01020304.out").write_text("left over\n")
        swept = server._sweep_calls_folder()

        assert (empty, swept) == (0, 1)
        assert tries == [{"shared": False, "attempts": 1}]

    @pytest.mark.parametrize("verb, fields", [("db.stop", {}), ("run.start", {"profile": "parse", "source": "ok"})])
    def test_a_call_that_meets_a_brief_holder_of_the_lock_goes_through(self, rig, verb, fields):
        """Bug class: one non-blocking try of the write lock, so that a call (a run start included) is answered `busy` because a sweep held the lock for an instant."""
        rig.start()
        session = rig.login()
        fd = os.open(rig.broker_dir / broker.WRITE_LOCK_NAME, os.O_RDWR | os.O_CREAT, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX)
        threading.Timer(0.08, os.close, args=(fd,)).start()

        result = session.call(verb, fields)

        assert result.get("exit_code", 0) == 0 and rig.events("call_refused") == []



# ── review round 4 ──────────────────────────────────────────────────────────


class TestRoundFour:
    def test_a_thread_whose_start_fails_otherwise_is_dropped_and_its_connection_closed(self, rig, monkeypatch):
        """Bug class: a connection thread whose start fails with anything but RuntimeError (memory) left in the set of connections, where the stop waits for it for ever at full speed, and its connection left open."""
        server = rig.make()
        server.start()
        pending = raw_exchange(rig.socket_path, frame('{"op":"hello"}'), read=False)
        real_start = threading.Thread.start

        def failing(thread):
            if getattr(getattr(thread, "_target", None), "__name__", "") == "_handle":
                raise MemoryError()
            return real_start(thread)

        monkeypatch.setattr(threading.Thread, "start", failing)
        with pytest.raises(MemoryError):
            server._accept()
        monkeypatch.setattr(threading.Thread, "start", real_start)

        assert server._connections == set()
        try:
            answer = pending.recv(10)
        except ConnectionResetError:
            # Closed with the request still unread: closed, without a reply.
            answer = b""
        assert answer == b""
        pending.close()
        server.request_stop()
        server._shutdown()

    def test_the_stop_drops_a_thread_that_never_started(self, rig):
        """Bug class: a stop that waits for a thread that was never started (it is never alive and never removes itself): full speed for ever, no `broker_stop`, the lock kept."""
        server = rig.start()
        with server._state_lock:
            server._connections.add(threading.Thread(target=lambda: None))
        _, thread = rig.running.pop()
        server.request_stop()
        thread.join(10)

        assert not thread.is_alive()
        assert rig.audit_lines()[-1]["event"] == "broker_stop"
        assert broker.running_broker(rig.root) is None

    @pytest.mark.parametrize("failure", ["selector", "housekeeper"])
    def test_a_setup_of_the_serving_loop_that_fails_stops_the_broker_cleanly(self, rig, monkeypatch, failure):
        """Bug class: a housekeeper that cannot start, or a selector that cannot be made (no descriptor left), before the `try` of the serving loop: no `broker_stop`, the socket left behind, and Python's traceback with its message in the broker's log."""
        server = rig.make()
        server.start()
        if failure == "selector":
            def no_selector():
                raise OSError(errno.EMFILE, "Too many open files with a made-up detail")
            monkeypatch.setattr(broker.selectors, "DefaultSelector", no_selector)
        else:
            real_start = threading.Thread.start

            def failing(thread):
                if getattr(getattr(thread, "_target", None), "__name__", "") == "_housekeeping":
                    raise RuntimeError("can't start new thread, a made-up detail")
                return real_start(thread)

            monkeypatch.setattr(threading.Thread, "start", failing)

        clean = server.serve_forever()

        assert clean is False
        assert not os.path.exists(rig.socket_path)
        assert rig.audit_lines()[-1]["event"] == "broker_stop"
        assert broker.running_broker(rig.root) is None
        log = rig.broker_log()
        assert f"the serving loop stopped on an error ({'OSError' if failure == 'selector' else 'RuntimeError'})" in log
        assert "made-up detail" not in log and "Traceback" not in log

    def test_without_its_folder_the_write_lock_is_refused_and_nothing_is_created_elsewhere(self, rig, tmp_path, monkeypatch):
        """Bug class: the write lock taken on a file of the current folder when the broker folder is gone, so that a call would run while it excludes nothing (an orphaned child still works under the real lock). Through the socket the session check already refuses such a call (the password file reads as missing); the lock refuses by itself as well."""
        server = rig.start()
        elsewhere = tmp_path / "working-folder"
        elsewhere.mkdir()
        monkeypatch.chdir(elsewhere)
        moved = rig.broker_dir.with_name("broker-moved")
        os.rename(rig.broker_dir, moved)
        try:
            with pytest.raises(broker._LockNotUsable):
                server._take_write_lock(shared=False)
            swept = server._sweep_calls_folder()
        finally:
            os.rename(moved, rig.broker_dir)

        assert swept == 0
        assert list(elsewhere.iterdir()) == []

    def test_a_write_lock_that_cannot_be_used_refuses_the_call(self, rig):
        """Bug class: a lock that cannot be taken answered as `busy` (the client retries for ever), or the call run without it."""
        server = rig.start()
        session = rig.login()

        def not_usable(**kwargs):
            raise broker._LockNotUsable()

        server._take_write_lock = not_usable
        refused = code_of(lambda: session.call("db.stop", {}))

        assert refused.code == "broker_folder_not_usable"
        assert rig.calls() == []
        assert rig.events("call_refused")[-1]["code"] == "broker_folder_not_usable"


# ── import boundaries ───────────────────────────────────────────────────────


@pytest.mark.parametrize("module", ["broker", "client"])
def test_the_broker_and_its_client_load_neither_the_cli_nor_a_child_module(module):
    """Bug class: the broker importing the CLI script at load (a cycle once the CLI's commands import the broker, and the whole CLI in the process that holds the socket), or the client pulling in the server."""
    forbidden = ("sdp", "social_data_pipeline.control.child", "social_data_pipeline.control.supervise", "social_data_pipeline.db", "social_data_pipeline.jobs")
    if module == "client":
        forbidden += ("social_data_pipeline.control.broker", "social_data_pipeline.control.sessions", "social_data_pipeline.runstore", "yaml")
    code = f"import sys; import social_data_pipeline.control.{module}; bad = sorted(m for m in sys.modules if any(m == f or m.startswith(f + '.') for f in {forbidden!r})); print(bad); sys.exit(1 if bad else 0)"

    result = subprocess.run([sys.executable, "-c", code], cwd=REPO_ROOT, env={"PATH": os.environ.get("PATH", ""), "PYTHONDONTWRITEBYTECODE": "1"}, capture_output=True, text=True)

    assert result.returncode == 0, result.stdout + result.stderr


# ── session.check_password: the management password again, and nothing else ──


class TestCheckPassword:
    def test_a_right_and_a_wrong_password_and_the_audit_lines(self, rig):
        """Bug class: a check that accepts a wrong password, changes anything on a right one, or leaves no audit line for either (a guesser with a stolen token would go unseen, and a right check could not be told from no check)."""
        rig.start()
        session = rig.login()
        before = rig.audit_lines()

        assert session.check_password(PASSWORD) == {}
        after_right = rig.audit_lines()
        wrong = code_of(lambda: session.check_password("wrong-password-1"))

        assert after_right[:len(before)] == before and [(line["event"], line["session"]) for line in after_right[len(before):]] == [("password_checked", 1)]
        assert list(after_right[-1]) == ["n", "t", "session", "event", "prev"]
        assert wrong.code == "step_up_failed"
        assert [line["session"] for line in rig.events("step_up_failed")] == [1]
        assert rig.audit_lines()[:len(after_right)] == after_right and len(rig.audit_lines()) == len(after_right) + 1
        assert session.check_password(PASSWORD) == {}
        assert len(rig.events("password_checked")) == 2

    def test_the_fifth_failure_ends_the_session(self, rig):
        """Bug class: the check counted apart from the other second checks (or not at all), so a stolen token can guess the management password without end."""
        rig.start()
        session = rig.login()
        code_of(lambda: session.call("db.unsetup_jobs", step_up="wrong-password-1"))
        codes = [code_of(lambda: session.check_password("wrong-password-1")).code for _ in range(4)]

        assert codes == ["step_up_failed"] * 3 + ["session_ended"]
        assert [line["reason"] for line in rig.events("session_end")] == ["step_up_failures"]
        assert len(rig.events("step_up_failed")) == 5
        assert code_of(lambda: session.check_password(PASSWORD)).code == "unauthorized"

    @pytest.mark.parametrize("token", [None, "a-made-up-token-that-names-no-session"])
    def test_no_valid_token_is_unauthorized_and_nothing_is_verified(self, rig, token):
        """Bug class: the password checked for whoever asks, which makes the socket an oracle for the management password without a session."""
        rig.start()
        rig.login()
        session = rig.client(token=token)
        if token is None:
            reply = decoded_reply(raw_exchange(rig.socket_path, frame(json.dumps({"op": "session.check_password", "step_up": PASSWORD}))))
            assert reply["error"]["code"] == "unauthorized"
        assert code_of(lambda: session.check_password(PASSWORD)).code == "unauthorized"
        assert rig.events("step_up_failed") == [] and rig.events("password_checked") == []

    @pytest.mark.parametrize("document", [{"op": "session.check_password"}, {"op": "session.check_password", "step_up": PASSWORD, "extra": 1}])
    def test_a_missing_or_an_unknown_field_is_refused(self, rig, document):
        """Bug class: a check without a password taken for a failed one (counted towards the end of the session), or a field the op does not list taken."""
        rig.start()
        session = rig.login()
        reply = decoded_reply(raw_exchange(rig.socket_path, frame(json.dumps({**document, "token": session.token}))))
        assert reply["error"]["code"] == "bad_request"
        assert rig.events("step_up_failed") == []

    def test_refused_while_stopping(self, rig):
        """Bug class: a check accepted once a stop was asked, unlike every other op that needs a session."""
        rig.start()
        gate = rig.out / "gate"
        rig.behave("db.stop", wait_for=str(gate))
        session = rig.login()
        caller = threading.Thread(target=lambda: session.call("db.stop", {}))
        caller.start()
        wait_for("the child", lambda: rig.calls("db.stop"))
        pending = raw_exchange(rig.socket_path, b"", read=False)
        wait_for("the handler", lambda: len(rig.broker._connections) == 2)
        rig.broker.request_stop()
        wait_for("the listener to close", lambda: rig.broker._listener is None)
        pending.sendall(frame(json.dumps({"op": "session.check_password", "token": session.token, "step_up": PASSWORD})))
        received = b""
        while True:
            chunk = pending.recv(65536)
            if not chunk:
                break
            received += chunk
        pending.close()
        gate.touch()
        caller.join(15)
        server, thread = rig.running.pop()
        thread.join(30)
        assert decoded_reply(received)["error"]["code"] == "stopping"
