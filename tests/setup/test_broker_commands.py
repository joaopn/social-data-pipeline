"""Tests for the `broker` commands of the CLI: `passwd`, `start`, `stop`, `status`, `audit`, and the bare `sdp broker`.

The contracts pinned here:

- `passwd` asks twice without echo, refuses a mismatch and a password the library refuses with one line and exit code 1, and writes the password file with the library's modes whatever the umask.
- `start` starts the broker detached and says so with its pid (exit code 0), says when one runs already (exit code 0), and prints the start error's own line with exit code 1, leaving nothing running.
- `stop` signals the process that holds the broker's lock and nothing else: a stale lock file that names another live process is "not running" (exit code 1), a lock held by a process other than the one its file names is refused; a real broker is stopped (exit code 0) and the command says once that it waits.
- `status` says running or not, with the pid and the start time, the socket and whether a password is set; exit code 0 when it runs, 1 when not.
- `audit` prints the newest lines as text or JSON, `--limit` lines of them, and `--verify` reports the chain with exit code 1 on a break or a log that cannot be read.

The CLI is pointed at a temp folder. The tests that start a real broker give it a copy of the CLI and the package, as a deployment has; the broker then runs detached and is stopped (or killed) when the test ends. Linux only.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import signal
import stat
import subprocess
import sys
import time
from pathlib import Path

import pytest

import sdp
from social_data_pipeline.control import audit, broker, passwords

REPO_ROOT = Path(__file__).resolve().parents[2]
PASSWORD = "management-made-up-7f3a"

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="flock, pidfd and unix sockets")

# A process that holds the broker's lock the way a broker does, writes its pid and the time into the lock file, and on TERM waits a moment before it ends: a stand-in for a broker that finishes an action first.
HOLDER = r'''
import fcntl, os, signal, sys, time
fd = os.open(sys.argv[1], os.O_RDWR | os.O_CREAT, 0o600)
fcntl.flock(fd, fcntl.LOCK_EX)
os.ftruncate(fd, 0)
os.write(fd, f"{os.getpid()} 2026-11-21T10:00:00Z\n".encode())
signal.signal(signal.SIGTERM, lambda signum, frame: (time.sleep(0.5), sys.exit(0)))
open(sys.argv[2], "w").close()
time.sleep(60)
'''


def fake_scrypt(password, *, salt, n, r, p, maxmem, dklen):
    return hashlib.sha256(salt + password).digest()


def wait_for(what, probe, seconds=15):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        found = probe()
        if found:
            return found
        time.sleep(0.02)
    raise AssertionError(f"timed out waiting for {what}")


def alive(pid):
    try:
        with open(f"/proc/{pid}/stat", "rb") as handle:
            return handle.read().rsplit(b")", 1)[1].split()[0] != b"Z"
    except (OSError, IndexError):
        return False


def run_command(*argv):
    args = sdp.build_parser().parse_args(["broker", *argv])
    return args.func(args)


@pytest.fixture
def root(tmp_path, monkeypatch):
    """A deployment folder the CLI points at; a broker still running at the end of the test is killed, and every process the test noted is too."""
    folder = tmp_path / "d"
    folder.mkdir(mode=0o700)
    monkeypatch.setattr(sdp, "ROOT", folder)
    started = []
    yield folder, started
    running = broker.running_broker(folder)
    if running is not None and running["pid"]:
        started.append(running["pid"])
    # Every broker of this folder, also one that never took the lock: found by the folder in its argument list.
    mark = str(folder).encode()
    for name in os.listdir("/proc"):
        if name.isdigit() and int(name) != os.getpid():
            try:
                if b"social_data_pipeline.control.broker" in Path(f"/proc/{name}/cmdline").read_bytes() and mark in Path(f"/proc/{name}/cmdline").read_bytes():
                    started.append(int(name))
            except OSError:
                pass
    for pid in started:
        if alive(pid):
            os.kill(pid, signal.SIGKILL)
            wait_for("a process to end", lambda pid=pid: not alive(pid))


def with_code(folder):
    """Copy the CLI and the package into the deployment folder, with the modes a checkout has when nobody else may write it."""
    shutil.copy(REPO_ROOT / "sdp.py", folder / "sdp.py")
    shutil.copytree(REPO_ROOT / "social_data_pipeline", folder / "social_data_pipeline", ignore=shutil.ignore_patterns("__pycache__"))
    os.chmod(folder / "sdp.py", 0o644)
    for path, folders, names in os.walk(folder / "social_data_pipeline"):
        os.chmod(path, 0o755)
        for name in names:
            os.chmod(os.path.join(path, name), 0o644)


# ── passwd ──────────────────────────────────────────────────────────────────


class TestPasswd:
    @pytest.fixture
    def answers(self, monkeypatch):
        given = []
        asked = []

        def fake_getpass(prompt):
            asked.append(prompt)
            return given.pop(0)

        monkeypatch.setattr(sdp, "getpass", fake_getpass)
        return given, asked

    def test_two_answers_that_differ_are_refused(self, root, answers, capsys):
        """Bug class: a password set from one answer of two that differ (a typo locks the operator out)."""
        folder, _ = root
        answers[0].extend(["first-made-up-password", "other-made-up-password"])

        code = run_command("passwd")

        assert code == 1
        assert capsys.readouterr().out == "\n  Error: the two passwords are not the same.\n\n"
        assert not (folder / ".sdp" / "broker" / "password.json").exists()
        assert answers[1] == ["  New management password: ", "  The same password again: "]

    def test_a_password_the_library_refuses_is_refused_with_its_line(self, root, answers, capsys):
        """Bug class: a short password written anyway, or refused with a message that is not the library's."""
        folder, _ = root
        answers[0].extend(["short", "short"])

        code = run_command("passwd")

        assert code == 1
        assert capsys.readouterr().out == "\n  Error: the password must have at least 12 characters.\n\n"
        assert not (folder / ".sdp" / "broker" / "password.json").exists()

    def test_the_password_is_written_with_the_librarys_modes(self, root, answers, capsys):
        """Bug class: the password file or its folder made with the umask (readable by the group: an offline guess of the password), or written as something else than the library's file. The real scrypt runs here: once to write, once to check."""
        folder, _ = root
        answers[0].extend([PASSWORD, PASSWORD])
        old = os.umask(0o002)
        try:
            code = run_command("passwd")
        finally:
            os.umask(old)

        assert code == 0
        assert capsys.readouterr().out == "\n  The management password is set. Sessions that logged in with another one end at their next request.\n\n"
        modes = {name: stat.S_IMODE(os.lstat(folder / name).st_mode) for name in (".sdp", ".sdp/broker", ".sdp/broker/password.json")}
        assert modes == {".sdp": 0o700, ".sdp/broker": 0o700, ".sdp/broker/password.json": 0o600}
        written = passwords.read_password_file(folder / ".sdp" / "broker")
        assert passwords.verify(written, PASSWORD) is True
        assert PASSWORD.encode() not in (folder / ".sdp" / "broker" / "password.json").read_bytes()


# ── start, status, stop with a real broker ──────────────────────────────────


class TestLifecycle:
    def test_start_status_and_stop(self, root, capsys, monkeypatch):
        """Bug class: a start that says "started" for a broker that is not there, a second broker started next to the first, a status that cannot tell, or a stop that leaves the process, its socket or its last audit line behind."""
        folder, started = root
        # The line that says the stop waits for an action is printed after a time, which a loaded machine can reach: this test is about the rest of the output.
        monkeypatch.setattr(broker, "STOP_SAY_WAIT_SECONDS", 600)
        with_code(folder)
        (folder / ".sdp").mkdir(mode=0o700)
        passwords.set_password(folder / ".sdp" / "broker", PASSWORD, scrypt=fake_scrypt)

        assert run_command("start") == 0
        first = capsys.readouterr().out
        pid = broker.running_broker(folder)["pid"]
        started.append(pid)
        assert first == f"  The broker is started (pid {pid}).\n"
        assert run_command("start") == 0
        assert capsys.readouterr().out == f"  The broker is already running (pid {pid}).\n"

        assert run_command("status") == 0
        since = broker.running_broker(folder)["started"]
        assert capsys.readouterr().out == f"  Broker: running (pid {pid}, since {since})\n  Socket: .sdp/socket/broker.sock\n  Management password: set\n"

        assert run_command("stop") == 0
        assert capsys.readouterr().out == "  The broker has stopped.\n"
        assert not alive(pid)
        assert not (folder / ".sdp" / "socket" / "broker.sock").exists()
        assert [line["event"] for line in audit.read_tail(folder / ".sdp" / "broker" / audit.FILE_NAME, 10)] == ["broker_start", "broker_stop"]

        assert run_command("status") == 1
        assert capsys.readouterr().out == "  Broker: not running\n  Socket: .sdp/socket/broker.sock\n  Management password: set\n"
        assert run_command("stop") == 1
        assert capsys.readouterr().out == "  The broker is not running.\n"

    def test_a_start_error_is_printed_with_exit_code_1(self, root, capsys):
        """Bug class: a broker that refused to start reported as started, or its reason lost in a log the operator does not read."""
        folder, _ = root
        with_code(folder)
        os.chmod(folder / "social_data_pipeline" / "control", 0o777)
        line = "social_data_pipeline/control can be written by others: run `chmod -R go-w social_data_pipeline/control` in the deployment folder"

        code = run_command("start")

        assert code == 1
        assert capsys.readouterr().out == f"  Error: {line}\n"
        assert broker.running_broker(folder) is None
        assert f"Error: {line}" in (folder / ".sdp" / "broker" / "broker.log").read_text()

    @pytest.mark.parametrize("ignores_term", [False, True])
    def test_a_broker_that_never_answers_is_ended_and_reported_after_the_wait(self, root, tmp_path, monkeypatch, ignores_term):
        """Bug class: a start that waits for ever for a broker that hangs, reports it as started, or reports the failure and leaves the hung process running (holding the lock a next start then finds), also one that ignores TERM and has to be killed."""
        folder, started = root
        (folder / ".sdp").mkdir(mode=0o700)
        ready = tmp_path / "server.ready"
        # The stand-in sets what TERM does to it, and only then says it is ready.
        disposition = "signal.signal(signal.SIGTERM, signal.SIG_IGN)" if ignores_term else "pass"
        server = [sys.executable, "-c", f"import os, signal, time\n{disposition}\nopen({str(ready)!r} + '.tmp', 'w').write(str(os.getpid()))\nos.replace({str(ready)!r} + '.tmp', {str(ready)!r})\ntime.sleep(60)\n"]
        real_end = broker._end_unanswering
        ended = []

        def end_once_ready(process, grace_seconds):
            # The ending waits until the stand-in is ready, so its signal disposition is in place whatever the machine's load; what the process was ended by is read from its exit status.
            wait_for("the stand-in to be ready", ready.exists, seconds=60)
            real_end(process, grace_seconds)
            ended.append(process.returncode)

        monkeypatch.setattr(broker, "_end_unanswering", end_once_ready)
        outcome, detail = broker.start_detached(folder, argv=server, wait_seconds=0.5, grace_seconds=0.3)
        pid = int(ready.read_text())
        started.append(pid)

        assert (outcome, detail) == ("failed", "the broker did not answer within 0.5 seconds and was ended: see .sdp/broker/broker.log")
        # Ended and collected: not even a zombie is left; ignoring TERM, by KILL after the grace time.
        assert not os.path.exists(f"/proc/{pid}")
        assert ended == [-signal.SIGKILL if ignores_term else -signal.SIGTERM]

    def test_status_with_a_password_file_that_cannot_be_used(self, root, capsys):
        """Bug class: a password file that cannot be used reported as "set" or "not set"."""
        folder, _ = root
        (folder / ".sdp" / "broker").mkdir(parents=True, mode=0o700)
        os.chmod(folder / ".sdp", 0o700)
        (folder / ".sdp" / "broker" / "password.json").write_text("{}")

        assert run_command("status") == 1
        assert capsys.readouterr().out.endswith("  Management password: cannot be told (the password file is not usable)\n")

    def test_status_without_a_broker_or_a_password(self, root, capsys):
        """Bug class: a status that fails, or says "running", in a deployment where nothing was ever started."""
        assert run_command("status") == 1
        assert capsys.readouterr().out == "  Broker: not running\n  Socket: .sdp/socket/broker.sock\n  Management password: not set\n"


# ── stop: only the holder of the lock is signalled ──────────────────────────


@pytest.fixture
def sent(monkeypatch):
    """Every signal `stop` would send, recorded instead of sent."""
    found = []
    monkeypatch.setattr(signal, "pidfd_send_signal", lambda handle, signum: found.append(signum))
    return found


def _lock_file(folder, content):
    (folder / ".sdp" / "broker").mkdir(parents=True, mode=0o700)
    os.chmod(folder / ".sdp", 0o700)
    path = folder / ".sdp" / "broker" / "lock"
    path.write_text(content)
    os.chmod(path, 0o600)
    return path


class TestStopSignalsOnlyTheHolder:
    def test_a_stale_lock_file_that_names_a_live_process_is_not_signalled(self, root, sent, capsys):
        """Bug class: a stop that trusts the pid in the file: after a crash and a reboot that pid is some other program of the operator's."""
        folder, started = root
        other = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
        started.append(other.pid)
        _lock_file(folder, f"{other.pid} 2026-11-21T10:00:00Z\n")

        code = run_command("stop")

        assert code == 1
        assert capsys.readouterr().out == "  The broker is not running.\n"
        assert sent == [] and alive(other.pid)
        other.kill()
        other.wait()

    def test_a_lock_held_by_another_process_than_the_one_named_is_not_signalled(self, root, sent, capsys):
        """Bug class: a stop that sees "the lock is held" and signals the pid in the file without asking who holds the lock."""
        folder, started = root
        other = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
        started.append(other.pid)
        path = _lock_file(folder, f"{other.pid} 2026-11-21T10:00:00Z\n")
        fd = os.open(path, os.O_RDWR)
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            code = run_command("stop")
        finally:
            os.close(fd)

        assert code == 1
        assert capsys.readouterr().out == "  Error: the lock of the broker is not held by the process its lock file names: nothing was signalled.\n"
        assert sent == [] and alive(other.pid)
        other.kill()
        other.wait()

    def test_a_broker_that_ends_before_the_signal_counts_as_stopped(self, root, tmp_path, capsys, monkeypatch):
        """Bug class: a stop that fails with the system's error when the broker ended between the checks and the signal."""
        folder, started = root
        (folder / ".sdp" / "broker").mkdir(parents=True, mode=0o700)
        os.chmod(folder / ".sdp", 0o700)
        ready = tmp_path / "ready"
        holder = subprocess.Popen([sys.executable, "-c", HOLDER, str(folder / ".sdp" / "broker" / "lock"), str(ready)])
        started.append(holder.pid)
        wait_for("the holder", ready.exists)

        def gone(handle, signum):
            raise ProcessLookupError(3, "No such process")

        monkeypatch.setattr(signal, "pidfd_send_signal", gone)
        code = run_command("stop")

        assert code == 0
        assert capsys.readouterr().out == "  The broker has stopped.\n"
        holder.kill()
        holder.wait()

    def test_the_holder_is_signalled_and_waited_for_and_the_wait_is_said_once(self, root, tmp_path):
        """Bug class: a stop that returns before the broker has ended (a start right after it is refused), that says nothing while it waits for an action, or that says it at every look."""
        folder, started = root
        (folder / ".sdp" / "broker").mkdir(parents=True, mode=0o700)
        os.chmod(folder / ".sdp", 0o700)
        ready = tmp_path / "ready"
        holder = subprocess.Popen([sys.executable, "-c", HOLDER, str(folder / ".sdp" / "broker" / "lock"), str(ready)])
        started.append(holder.pid)
        wait_for("the holder", ready.exists)
        said = []

        stopped = broker.stop_broker(folder, on_wait=lambda: said.append("waiting"), first_wait_seconds=0.1)

        assert stopped is True
        assert said == ["waiting"]
        assert holder.wait(5) == 0


# ── audit ───────────────────────────────────────────────────────────────────


@pytest.fixture
def audit_log(root):
    folder, _ = root
    (folder / ".sdp").mkdir(mode=0o700)
    times = iter(range(1_795_000_000, 1_795_000_100))
    with audit.AuditLog(folder / ".sdp" / "broker", now=lambda: next(times)) as log:
        log.broker_start()
        log.login(1)
        log.call_refused(1, "db.nope", "unknown_verb")
        log.session_end(1, "expired")
        log.broker_stop()
    return folder / ".sdp" / "broker" / audit.FILE_NAME


class TestAuditCommand:
    def test_the_newest_lines_as_text(self, audit_log, capsys):
        """Bug class: the oldest lines printed for "the newest", or a line whose fields are dropped."""
        assert run_command("audit", "--limit", "3") == 0

        assert capsys.readouterr().out == (
            "  2026-11-18T11:06:43Z  session 1  call_refused  verb=db.nope code=unknown_verb field=null\n"
            "  2026-11-18T11:06:44Z  session 1  session_end  reason=expired\n"
            "  2026-11-18T11:06:45Z  session -  broker_stop\n"
        )

    def test_json(self, audit_log, capsys):
        """Bug class: the JSON output not being the lines of the log."""
        assert run_command("audit", "--json") == 0

        assert json.loads(capsys.readouterr().out) == audit.read_tail(audit_log, 50)

    def test_a_limit_out_of_range_is_refused(self, audit_log, capsys):
        """Bug class: a limit that the reader refuses with a traceback."""
        assert run_command("audit", "--limit", "0") == 1
        assert capsys.readouterr().out == "  Error: --limit must be from 1 to 10000.\n"

    def test_verify_a_whole_and_a_damaged_log(self, audit_log, capsys):
        """Bug class: a check that passes a log with an edited line, or fails a good one."""
        assert run_command("audit", "--verify") == 0
        assert capsys.readouterr().out.endswith("  The chain is whole.\n")
        lines = audit_log.read_bytes().split(b"\n")
        lines[2] = lines[2].replace(b"unknown_verb", b"unknown_vErb")
        audit_log.write_bytes(b"\n".join(lines))

        assert run_command("audit", "--verify") == 1
        out = capsys.readouterr().out
        assert "  Breaks of the chain: 1 (the first where line 4 was expected)\n" in out
        assert out.endswith("  The chain is broken.\n")
        assert run_command("audit", "--verify", "--json") == 1
        assert json.loads(capsys.readouterr().out)["breaks"] == 1

    def test_verify_a_missing_log(self, root, capsys):
        """Bug class: "no log" read as "a whole chain"."""
        assert run_command("audit", "--verify") == 1
        assert capsys.readouterr().out == "  The audit log cannot be read.\n"


def test_bare_broker_prints_the_groups_help():
    """Bug class: `sdp broker` without a command doing nothing, or failing, where `sdp db` prints its help."""
    result = subprocess.run([sys.executable, str(REPO_ROOT / "sdp.py"), "broker"], capture_output=True, text=True, timeout=30, env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})

    assert result.returncode == 0
    assert result.stdout.startswith("usage: sdp.py broker")
    for command in ("start", "stop", "status", "passwd", "audit"):
        assert command in result.stdout
