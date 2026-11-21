"""The broker: one host process on a unix socket through which a logged-in session may run the fixed list of typed verbs of the CLI. `python -m social_data_pipeline.control.broker --root <root> --foreground` serves in this process; without `--foreground` it starts that command detached and says whether it came up, as `sdp broker start` does.

What it defends, and only that: against another user of the machine (the socket's folder is 0700 and a connection from any other uid is closed without a reply, read from the kernel with `SO_PEERCRED`), against whatever can send bytes to the socket (the web UI container, which holds only the socket folder: one frame of at most `REQUEST_MAX_BYTES`, ten seconds for all of it, nothing but `hello` and `login` answered without a valid session token, every verb validated by the registry, deny by default), and against a stolen session token (typed verbs only, and the management password again for every verb of the class `step_up`). NOT against a program that runs on the host as the operator: that already has Docker and the files.

Files: `<root>/.sdp/socket/broker.sock` (folder 0700), and `<root>/.sdp/broker/` (0700) with `password.json`, `audit.jsonl`, `broker.log`, `lock` (an `flock` held for the broker's life; it holds the pid and the start time), `write.lock` (taken for each state-changing call and inherited by its child, so that it stays held while an orphaned child still works after a broker crash; `run.start` takes it shared) and `calls/` (the standard output and standard error of each state-changing child, two files that are read and deleted when the child ends; files no running call owns are deleted at start and by the housekeeping every 30 seconds, whenever `write.lock` is free).

One connection carries one request and one reply (`framing`). A reply is `{"ok": true, "result": ...}` or `{"ok": false, "error": {"code": ..., "message": <constant text>, "field": <name, when one field is at fault>}}`; a `failed` error also has `exit_code` and `errors` (the end of the command's standard error, overwritten like everything else that leaves). No reply holds a value of the request, a password or a traceback; the token is in the reply of `login` only. Every result that leaves passes `redact.redact_data` with the database passwords the sessions hold or held, the secret variables of the broker's environment and the secrets of the request; a result that cannot be overwritten in full is answered `too_large`, never sent as it is.

Every verb other than `run.start` (started through `control.runs.start_run`), `audit.list` (read from the audit log here) and `run.logs` of a recorded run whose output is in its log file (read here, so that a secret cut by the end of a piece is still overwritten) runs in a process of its own, `control.child`, started in the deployment folder in a session of its own, with the request on standard input and an environment of the allowed names only. A read child's standard output and standard error are pipes, read by two threads, each bounded; a read verb has at most 8 such processes at a time and 60 seconds. A state-changing child writes its standard output and standard error to its two files in `calls/`, not to the broker, so that it runs to its end even when the broker crashed; the broker reads the last 256 KiB of each when it ends. A state-changing verb other than `run.start` and `run.cancel` runs one at a time under `write.lock`, without a time limit, and to its end even when the client has gone.

What is not bounded or not done, on purpose: the output files of a state-changing action have no size limit (the CLI's actions print little); a process that an action leaves behind after the action ended (outside the group the broker ends on a timeout or a forced stop) is not ended by the broker; whether a group is the operator's private one (see `check_deployment`) is best effort where accounts come from a directory service that is not enumerated.

Linux only: `SO_PEERCRED`, `flock`, process groups, `pidfd_open` and `eventfd`.
"""

import argparse
import contextlib
import ctypes
import errno
import fcntl
import json
import os
import resource
import select
import selectors
import signal
import socket
import stat
import struct
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from social_data_pipeline.control import REQUEST_MAX_BYTES, audit, environ, errors, files, framing, passwords, redact, verbs
from social_data_pipeline.control import sessions as session_table
from social_data_pipeline.control.types import SecretValue

PROTOCOL = 1
CHILD_ARGV = (sys.executable, "-u", "-m", "social_data_pipeline.control.child")
SERVER_ARGV = (sys.executable, "-m", "social_data_pipeline.control.broker")

STATE_FOLDER = ".sdp"
BROKER_FOLDER = os.path.join(STATE_FOLDER, "broker")
SOCKET_FOLDER = os.path.join(STATE_FOLDER, "socket")
SOCKET_NAME = "broker.sock"
SOCKET_PATH = os.path.join(SOCKET_FOLDER, SOCKET_NAME)
LOG_NAME = "broker.log"
LOCK_NAME = "lock"
WRITE_LOCK_NAME = "write.lock"
# A unix socket address holds 108 bytes, the terminating zero byte included.
SOCKET_PATH_MAX_BYTES = 107
CALLS_FOLDER_NAME = "calls"
LOCK_FILE_MAX_BYTES = 256
START_WAIT_SECONDS = 10
# How long a broker that did not answer at its start is given to end on TERM before it is killed.
START_GRACE_SECONDS = 5
# How often, and how far apart, the broker's lock is tried at start: someone who looks whether a broker runs holds it shared for an instant.
LOCK_ATTEMPTS = 5
LOCK_RETRY_SECONDS = 0.05
# How often, and how far apart, a call tries the write lock before it is answered `busy`: a sweep of `calls/` holds it for an instant.
WRITE_LOCK_ATTEMPTS = 5
# How long the listener rests when the process has no descriptor left for a new connection.
ACCEPT_PAUSE_SECONDS = 0.1
# How long `stop_broker` waits before it says that it waits for an action.
STOP_SAY_WAIT_SECONDS = 2.0

# The ops of a request and the keys each one takes besides `op`; `step_up` of a call is optional.
OPS = {
    "hello": frozenset(),
    "login": frozenset({"password"}),
    "logout": frozenset({"token"}),
    "session.set_db_password": frozenset({"token", "step_up", "password"}),
    "session.clear_db_password": frozenset({"token"}),
    "call": frozenset({"token", "verb", "fields"}),
}
OPTIONAL_KEYS = {"call": frozenset({"step_up"})}

# Verbs that are refused while a pipeline run is alive, and when Docker cannot be asked whether one is.
DESTRUCTIVE_VERBS = frozenset({"db.reset", "db.unsetup", "db.stop", "source.remove"})
# The state-changing verb that takes no write lock: a cancel must get through while something else runs.
UNLOCKED_VERBS = frozenset({"run.cancel"})
# The verb that takes the write lock shared: several runs can be started at once, never while a state-changing child runs, and no destructive verb looks for live runs while one is being started.
SHARED_LOCK_VERBS = frozenset({"run.start"})
# What is refused once a stop was asked; a request that is already being answered finishes.
REFUSED_WHILE_STOPPING = frozenset({"login", "call", "session.set_db_password", "session.clear_db_password"})
# The verbs to which the session's database password is added when the request has none.
DB_PASSWORD_VERBS = frozenset({"run.start", "db.start"})
DB_PASSWORD_FIELD = "admin_password"
# The services a `db.start` pastes the password into a shell line for (with no service, every configured one), and the characters such a line cannot take.
SHELL_LINE_SERVICES = (None, "postgres", "postgres-mcp")
SHELL_LINE_CHARACTERS = frozenset("'\"$`\\")
# What a recorded run is in while it counts as alive.
ALIVE_STATES = ("starting", "running", "detached", "unknown")
RUN_LOGS_DEFAULT_LIMIT = 65536
AUDIT_LIST_DEFAULT_LIMIT = 50

MESSAGES = {
    "bad_request": "the request is not one the broker takes",
    "request_too_large": "the request is larger than a request may be",
    "password_file_not_usable": "the password file is not usable",
    "unknown_verb": "the verb is not known",
    "not_available": "the verb is not available over the socket",
    "invalid_input": "a field of the request was refused",
    "step_up_required": "this verb needs the management password again",
    "run_active": "a pipeline run is alive: this verb waits until it has ended",
    "docker_unreachable": "Docker cannot be asked whether a pipeline run is alive",
    "store_not_usable": "the run state folder cannot be used",
    "password_not_allowed_here": "this start cannot take a database password that holds a quote, '$', a backtick or a backslash",
    "audit_not_writable": "the audit log cannot be written, so no state-changing call is accepted",
    "busy": "the broker is busy with another call of this kind: try again later",
    "timeout": "the command took longer than a read may take and was ended",
    "too_large": "the answer is too large to send",
    "failed": "the command failed",
    "start_failed": "the command could not be started",
    "internal": "the broker could not answer this request",
    "stopping": "the broker is stopping: it takes no new request",
    "broker_folder_not_usable": "the broker folder is gone or cannot be used, so no state-changing call can be run",
}

# The exceptions of a frame that mean the client sent nothing usable in time: the connection is closed without a reply.
_SILENT_FRAME_ERRORS = (framing.FrameTimeout, framing.FrameHeaderIncomplete, framing.FrameIncomplete, framing.FrameReadFailed)


class StartRefused(Exception):
    """The broker did not start. `message` is one line for the operator; a path in it is relative to the deployment folder."""

    def __init__(self, message):
        super().__init__(message)
        self.message = message


class StopRefused(Exception):
    """`stop_broker` sent no signal because what it found does not add up. `message` is one line for the operator."""

    def __init__(self, message):
        super().__init__(message)
        self.message = message


@dataclass(frozen=True)
class Limits:
    """The limits and times of a broker. The defaults are the ones it runs with; a test passes smaller ones."""

    request_seconds: float = 10
    send_seconds: float = 30
    connections_max: int = 64
    read_children_max: int = 8
    read_seconds: float = 60
    kill_grace_seconds: float = 10
    drain_seconds: float = 5
    housekeeping_seconds: float = 30
    output_tail_bytes: int = 256 * 1024
    reply_max_bytes: int = framing.REPLY_MAX_BYTES


# ── replies ─────────────────────────────────────────────────────────────────


def _ok(result):
    return {"ok": True, "result": result}


def _error(code, message=None, field=None, **extra):
    error = {"code": code, "message": MESSAGES.get(code, MESSAGES["internal"]) if message is None else message}
    if field is not None:
        error["field"] = field
    error.update(extra)
    return {"ok": False, "error": error}


class _LockNotUsable(Exception):
    """The write lock cannot be taken because the broker folder is gone or cannot be used."""


class _Refusal(Exception):
    """A call refused before anything ran: the reply, and what the audit line names."""

    def __init__(self, code, message=None, field=None, **extra):
        super().__init__(code)
        self.code = code
        self.reply = _error(code, message, field, **extra)
        self.field = field


# ── paths and the files of a deployment ─────────────────────────────────────


def socket_path(root):
    """The path of the broker's socket in a deployment."""
    return os.path.join(os.fspath(root), SOCKET_PATH)


def broker_folder(root):
    """The path of the broker's own folder in a deployment."""
    return os.path.join(os.fspath(root), BROKER_FOLDER)


# The folders of the broker's own state: created with mode 0700 when they are missing, and never writable by the group, private or not.
_STATE_FOLDERS = (STATE_FOLDER, BROKER_FOLDER, SOCKET_FOLDER)
# The suffixes of the files under the package that Python loads as code.
_CODE_SUFFIXES = (".py", ".pyc")


def _group_is_private(gid, uid):
    """Whether the group `gid` is the primary group of the user `uid` and nobody else is in it: no other name in its member list and no other account with it as primary group. Best effort through the system's user and group lists; False whenever that cannot be told."""
    import grp
    import pwd

    try:
        user = pwd.getpwuid(uid)
        group = grp.getgrgid(gid)
        accounts = pwd.getpwall()
    except (KeyError, OSError):
        return False
    if user.pw_gid != gid:
        return False
    if any(member != user.pw_name for member in group.gr_mem):
        return False
    return all(account.pw_gid != gid or account.pw_uid == uid for account in accounts)


def _check_path(root, relative, info, uid, *, is_folder, strict, private_group):
    """Refuse a folder or file that is a link, of the wrong kind, of another owner, writable by others, or writable by its group (with `strict` always, otherwise unless the group is the private group of `uid`)."""
    name = "the deployment folder" if relative == "." else relative
    command = "chmod go-w ." if relative == "." else f"chmod -R go-w {relative}"
    if stat.S_ISLNK(info.st_mode):
        raise StartRefused(f"{name} is a symbolic link")
    if is_folder and not stat.S_ISDIR(info.st_mode):
        raise StartRefused(f"{name} is not a folder")
    if not is_folder and not stat.S_ISREG(info.st_mode):
        raise StartRefused(f"{name} is not a regular file")
    if info.st_uid != uid:
        raise StartRefused(f"{name} does not belong to the user the broker runs as")
    if info.st_mode & 0o002:
        raise StartRefused(f"{name} can be written by others: run `{command}` in the deployment folder")
    if info.st_mode & 0o020 and (strict or not private_group(info.st_gid, uid)):
        reason = "its group" if strict else "its group, which is not a group of this user alone"
        raise StartRefused(f"{name} can be written by {reason}: run `{command}` in the deployment folder")


def _looked_at(path, relative):
    try:
        return os.lstat(path)
    except FileNotFoundError:
        raise StartRefused(f"{relative} is missing") from None
    except OSError:
        raise StartRefused(f"{relative} cannot be looked at") from None


def check_deployment(root, uid, private_group=None):
    """Refuse a start (StartRefused, with the path relative to the deployment folder and the command that fixes it) when a folder or the code the broker relies on can be changed by someone else.

    Checked, each without following a link, to be a real folder or file that belongs to `uid` and that others may not write: the deployment folder itself (every child imports the CLI from it), `sdp.py`, the folder `social_data_pipeline` and every folder and every `.py` and `.pyc` file under it (`social_data_pipeline/control` must exist), all of which the group may write only when it is the private group of `uid` (`private_group(gid, uid)`, by default `_group_is_private`); and `.sdp`, `.sdp/broker` and `.sdp/socket` (created with mode 0700 when they are missing), which the group may never write. Also refused: a socket path longer than a unix socket address holds."""
    root = os.fspath(root)
    asked = private_group or _group_is_private
    answers = {}

    def private_group(gid, uid):
        # Asked once per group and user in one check: the default answer reads the whole account list.
        if (gid, uid) not in answers:
            answers[(gid, uid)] = asked(gid, uid)
        return answers[(gid, uid)]

    _check_path(root, ".", _looked_at(root, "."), uid, is_folder=True, strict=False, private_group=private_group)
    for relative in _STATE_FOLDERS:
        path = os.path.join(root, relative)
        try:
            os.mkdir(path, files.FOLDER_MODE)
        except FileExistsError:
            pass
        except OSError:
            raise StartRefused(f"{relative} cannot be created") from None
        _check_path(root, relative, _looked_at(path, relative), uid, is_folder=True, strict=True, private_group=private_group)
    _check_path(root, "sdp.py", _looked_at(os.path.join(root, "sdp.py"), "sdp.py"), uid, is_folder=False, strict=False, private_group=private_group)
    package = os.path.join(root, "social_data_pipeline")
    for relative in ("social_data_pipeline", os.path.join("social_data_pipeline", "control")):
        _check_path(root, relative, _looked_at(os.path.join(root, relative), relative), uid, is_folder=True, strict=False, private_group=private_group)

    def unreadable(_error):
        raise StartRefused("a folder under social_data_pipeline cannot be read")

    for folder, folder_names, file_names in os.walk(package, onerror=unreadable):
        folder_names.sort()
        for name in folder_names:
            path = os.path.join(folder, name)
            _check_path(root, os.path.relpath(path, root), _looked_at(path, os.path.relpath(path, root)), uid, is_folder=True, strict=False, private_group=private_group)
        for name in sorted(file_names):
            if name.endswith(_CODE_SUFFIXES):
                path = os.path.join(folder, name)
                _check_path(root, os.path.relpath(path, root), _looked_at(path, os.path.relpath(path, root)), uid, is_folder=False, strict=False, private_group=private_group)
    if len(os.fsencode(socket_path(root))) > SOCKET_PATH_MAX_BYTES:
        raise StartRefused(f"the socket path ({SOCKET_PATH} in the deployment folder) is longer than the {SOCKET_PATH_MAX_BYTES + 1} bytes a unix socket address holds: move the deployment to a shorter path")


# The number of the system call pidfd_open: the same on every architecture of Linux (it is in the table they share since 5.3).
_SYS_PIDFD_OPEN = 434


def pidfd_open(pid):
    """A handle on a process (`os.pidfd_open`), through the system call itself where Python was built without the function. The handle is closed on exec. Raises OSError (ProcessLookupError for a process that does not exist)."""
    opener = getattr(os, "pidfd_open", None)
    if opener is not None:
        return opener(pid)
    libc = ctypes.CDLL(None, use_errno=True)
    libc.syscall.restype = ctypes.c_long
    fd = libc.syscall(ctypes.c_long(_SYS_PIDFD_OPEN), ctypes.c_int(pid), ctypes.c_uint(0))
    if fd < 0:
        number = ctypes.get_errno()
        raise OSError(number, os.strerror(number))
    return int(fd)


def _peer_uid(connection):
    """The uid of the process at the other end of a unix socket, as the kernel recorded it when the connection was made."""
    data = connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
    return struct.unpack("3i", data)[1]


def _utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _prepare_process(log):
    """What the real broker does to its own process before any thread exists: the environment cut down to the allowed names, no core dump, and no tracing by other processes of the same user."""
    environ.reduce_process_environment()
    with contextlib.suppress(ValueError, OSError):
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        # PR_SET_DUMPABLE is 4.
        if libc.prctl(4, 0, 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno(), "prctl")
    except (OSError, AttributeError):
        log("the process could not be made undumpable (prctl PR_SET_DUMPABLE): it can be traced by other processes of this user")


def _tail_text(data, trimmed):
    """Bytes kept from the end of a stream as text. When the start was cut away, the text begins after the first line break, so no line (and no secret, or `$ docker compose ` prefix) is shown cut in two."""
    data = bytes(data)
    if trimmed:
        newline = data.find(b"\n")
        data = data[newline + 1:] if newline >= 0 else b""
    return data.decode("utf-8", "replace")


# ── one child process ───────────────────────────────────────────────────────


class _Child:
    """One run of the generic child: its process (in a session and process group of its own), the thread that feeds it the request, its output (two pipes read by two threads, or two files the caller gave), and the way it is ended. Only the handler thread that runs it waits for it, signals its group and collects it; other threads ask for an end through `end_soon`, which wakes that thread."""

    def __init__(self, broker, argv, request, *, pass_fds, keep_whole_stdout, output_files=None):
        self.broker = broker
        self.argv = argv
        self.request = request
        self.pass_fds = tuple(pass_fds)
        self.keep_whole_stdout = keep_whole_stdout
        self.output_files = output_files
        self.reason = None
        self._wake = None
        self._collected = False
        self._wake_lock = threading.Lock()
        self.process = None
        self.pidfd = None
        self.stdout = bytearray()
        self.stderr = bytearray()
        self.trimmed = {"stdout": False, "stderr": False}

    def end_soon(self, reason):
        """Ask the handler thread to end the child's process group (TERM, then KILL). `reason` is kept for the reply: `too_large` or `forced`. Asked before the child runs, it is ended as soon as it has started; asked after it was collected, nothing happens."""
        with self._wake_lock:
            if self._collected:
                return
            if self.reason is None:
                self.reason = reason
            if self._wake is not None:
                with contextlib.suppress(OSError):
                    os.eventfd_write(self._wake, 1)

    def _open_wake(self):
        with self._wake_lock:
            self._wake = os.eventfd(0, os.EFD_CLOEXEC)
            if self.reason is not None:
                os.eventfd_write(self._wake, 1)
            return self._wake

    def _close_wake(self):
        with self._wake_lock:
            self._collected = True
            if self._wake is not None:
                os.close(self._wake)
                self._wake = None

    def _read(self, fd, name):
        limits = self.broker.limits
        buffer = getattr(self, name)
        whole = self.keep_whole_stdout and name == "stdout"
        try:
            while True:
                try:
                    chunk = os.read(fd, 65536)
                except OSError:
                    break
                if not chunk:
                    break
                if whole:
                    if self.reason == "too_large":
                        continue
                    buffer += chunk
                    if len(buffer) > limits.reply_max_bytes:
                        buffer.clear()
                        self.end_soon("too_large")
                else:
                    buffer += chunk
                    if len(buffer) > limits.output_tail_bytes:
                        del buffer[:len(buffer) - limits.output_tail_bytes]
                        self.trimmed[name] = True
        finally:
            os.close(fd)

    def _feed(self, fd):
        try:
            view = memoryview(self.request)
            while view:
                view = view[os.write(fd, view):]
        except OSError:
            # The child ended or closed its input.
            pass
        finally:
            os.close(fd)

    def _signal_group(self, signum):
        # The child is not collected before the group is done with, so its number names this group and no other.
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(self.process.pid, signum)

    def _wait_exit(self, seconds):
        """Whether the child itself has ended, waiting at most `seconds` (None: no limit). It is not collected here. Without a handle on the child, nothing is waited for."""
        if self.pidfd is None:
            return False
        poller = select.poll()
        poller.register(self.pidfd, select.POLLIN)
        return bool(poller.poll(None if seconds is None else max(0, int(seconds * 1000))))

    def _end_group(self):
        limits = self.broker.limits
        self._signal_group(signal.SIGTERM)
        self._wait_exit(limits.kill_grace_seconds)
        self._signal_group(signal.SIGKILL)
        self._wait_exit(limits.kill_grace_seconds)

    def run(self, seconds):
        """Start the child, feed it the request, wait until it ends (at most `seconds`; None: no limit) or is asked to end, and collect it. Returns (outcome, exit code): `exited`, `timeout`, `too_large` or `forced`. Raises OSError when the child could not be started, and whatever else fails on the way (a thread that cannot be started); in every case the child's group is ended, every descriptor is closed and the child is collected before this returns or raises."""
        limits = self.broker.limits
        # The descriptors this call still has to close: each pipe end is handed to the child or to the thread that uses it, and is then no longer this call's.
        open_here = []
        readers = []
        try:
            wake = self._open_wake()
            in_read, in_write = os.pipe()
            open_here += [in_read, in_write]
            if self.output_files is None:
                out_read, out_write = os.pipe()
                open_here += [out_read, out_write]
                err_read, err_write = os.pipe()
                open_here += [err_read, err_write]
                child_out, child_err = out_write, err_write
            else:
                child_out, child_err = self.output_files
            self.process = subprocess.Popen(list(self.argv), cwd=self.broker.root, stdin=in_read, stdout=child_out, stderr=child_err, start_new_session=True, pass_fds=self.pass_fds, env=environ.clean(os.environ), close_fds=True)
            for fd in [in_read] + ([out_write, err_write] if self.output_files is None else []):
                open_here.remove(fd)
                os.close(fd)
            self.pidfd = pidfd_open(self.process.pid)
            jobs = [(self._feed, (in_write,), in_write)]
            if self.output_files is None:
                jobs += [(self._read, (out_read, "stdout"), out_read), (self._read, (err_read, "stderr"), err_read)]
            for target, arguments, fd in jobs:
                thread = threading.Thread(target=target, args=arguments, daemon=True)
                thread.start()
                open_here.remove(fd)
                if target is not self._feed:
                    readers.append(thread)

            poller = select.poll()
            poller.register(self.pidfd, select.POLLIN)
            poller.register(wake, select.POLLIN)
            clock = self.broker.clock
            deadline = None if seconds is None else clock() + seconds
            outcome = "exited"
            while True:
                wait = None if deadline is None else max(0, int((deadline - clock()) * 1000))
                ready = {fd for fd, _ in poller.poll(wait)}
                if self.pidfd in ready:
                    break
                if wake in ready:
                    with contextlib.suppress(OSError):
                        os.eventfd_read(wake)
                    if self.reason is not None:
                        outcome = self.reason
                        self._end_group()
                        break
                if deadline is not None and clock() >= deadline:
                    outcome = "timeout"
                    self._end_group()
                    break
            # Output that is still in the pipes is read to its end. A process of the group that holds them open after the child ended is ended too.
            for thread in readers:
                thread.join(limits.drain_seconds)
            if any(thread.is_alive() for thread in readers):
                self._end_group()
                for thread in readers:
                    thread.join(limits.drain_seconds)
            if self.reason in ("too_large", "forced") and outcome == "exited":
                outcome = self.reason
        except BaseException:
            if self.process is not None:
                self._end_group()
            raise
        finally:
            for fd in open_here:
                with contextlib.suppress(OSError):
                    os.close(fd)
            if self.process is not None:
                self.process.wait()
            if self.pidfd is not None:
                os.close(self.pidfd)
            self._close_wake()
        return outcome, self.process.returncode


# ── the broker ──────────────────────────────────────────────────────────────


class Broker:
    """The server of one deployment. `start()` checks the deployment, takes the broker's lock, binds the socket and opens the audit log; `serve_forever()` answers connections until `request_stop()`, and then stops as described in the module docstring.

    The parameters are what a test needs to replace: `sessions` (a `Sessions` object, for one with a fast verification), `child_argv` (the command of the generic child), `run_starter(root, fields, started_by)` (`control.runs.start_run`), `run_lister()` (the run store's listing of every run, with the service table of the CLI), `clock` (seconds, never going back), `audit_clock` (seconds since the epoch), `limits`, `peer_uid(connection)` (the kernel's peer credentials), `uid` (the user the broker acts as and accepts) and `prepare(log)` (what is done to the process before any thread exists: `main` passes the environment cut, no core dumps and no tracing), `private_group(gid, uid)` (whether a group is the user's alone; see `check_deployment`).
    """

    def __init__(self, root, *, sessions=None, child_argv=CHILD_ARGV, run_starter=None, run_lister=None, clock=time.monotonic, audit_clock=time.time, limits=None, peer_uid=_peer_uid, uid=None, prepare=None, private_group=None):
        self.root = os.path.realpath(os.fspath(root))
        self.sessions = session_table.Sessions() if sessions is None else sessions
        self.child_argv = tuple(child_argv)
        self.run_starter = run_starter or _default_run_starter
        self.run_lister = run_lister or (lambda: _default_run_lister(self.root))
        self.clock = clock
        self.audit_clock = audit_clock
        self.limits = limits or Limits()
        self.peer_uid = peer_uid
        self.uid = os.geteuid() if uid is None else uid
        self.prepare = prepare
        self.private_group = private_group
        self.broker_dir = broker_folder(self.root)
        self.calls_dir = os.path.join(self.broker_dir, CALLS_FOLDER_NAME)
        self.socket_path = socket_path(self.root)
        self.audit = None
        self._log_fd = None
        self._lock_fd = None
        self._listener = None
        self._socket_identity = None
        self._wake_read, self._wake_write = os.pipe()
        os.set_blocking(self._wake_read, False)
        os.set_blocking(self._wake_write, False)
        self._stopping = threading.Event()
        self._force = False
        # Set by a signal handler, which may not take a lock: the serving loop (or the stop) acts on them.
        self._signalled_stop = False
        self._signalled_force = False
        self._call_counter = 0
        # The names of the calls whose output files are in use: a sweep never deletes those.
        self._live_calls = set()
        self._state_lock = threading.Lock()
        self._connections = set()
        self._children = set()
        self._read_children = threading.BoundedSemaphore(self.limits.read_children_max)
        self._seen_digest = None
        self._housekeeper = None
        self.started = None

    # ── the log of the broker ───────────────────────────────────────────────

    def log(self, text):
        """Write one line of the broker's own into `broker.log`. `text` is a constant, with at most the name of a kind of error in it."""
        if self._log_fd is None:
            return
        with contextlib.suppress(OSError):
            os.write(self._log_fd, f"{_utc_now()} {text}\n".encode("utf-8", "replace"))

    # ── start ───────────────────────────────────────────────────────────────

    def start(self):
        """Start in the order the module describes. Raises StartRefused with a line for the operator; what was taken until then is given back."""
        try:
            self._start()
        except BaseException:
            if self._socket_identity is not None:
                with contextlib.suppress(OSError):
                    os.unlink(self.socket_path)
            self._close_files()
            raise

    def _start(self):
        check_deployment(self.root, self.uid, self.private_group)
        try:
            with files.folder(self.broker_dir) as folder_fd:
                if folder_fd is None:
                    raise StartRefused(f"{BROKER_FOLDER} is missing")
                self._log_fd = files.open_file(folder_fd, LOG_NAME, os.O_WRONLY | os.O_APPEND, create=True)
                if self.prepare is not None:
                    self.prepare(self.log)
                lock_fd = files.open_file(folder_fd, LOCK_NAME, os.O_RDWR, create=True)
        except files.BrokerFileError as error:
            raise StartRefused(f"{BROKER_FOLDER}: {error.message}") from None
        try:
            taken = self._take_broker_lock(lock_fd)
        except OSError:
            os.close(lock_fd)
            raise StartRefused(f"{os.path.join(BROKER_FOLDER, LOCK_NAME)} cannot be locked") from None
        if not taken:
            os.close(lock_fd)
            raise StartRefused("a broker is already running for this deployment")
        self._lock_fd = lock_fd
        self.started = _utc_now()
        try:
            os.ftruncate(lock_fd, 0)
            os.pwrite(lock_fd, f"{os.getpid()} {self.started}\n".encode("ascii"), 0)
        except OSError:
            raise StartRefused(f"{os.path.join(BROKER_FOLDER, LOCK_NAME)} cannot be written") from None
        self._clear_calls_folder()
        self._bind()
        try:
            self.audit = audit.AuditLog(self.broker_dir, now=self.audit_clock)
        except (audit.AuditNotUsable, audit.AuditInUse):
            raise StartRefused(f"{os.path.join(BROKER_FOLDER, audit.FILE_NAME)} cannot be used") from None
        password_file, refusal = self._read_password_file()
        self._seen_digest = password_file.digest if refusal is None else None
        self.audit.broker_start()
        self.log(f"started, pid {os.getpid()}")

    @staticmethod
    def _take_broker_lock(lock_fd):
        """Take the broker's lock: True when it was taken, False when another holds it. Tried a few times within a fifth of a second, because whoever looks whether a broker runs holds it shared for an instant."""
        for attempt in range(LOCK_ATTEMPTS):
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return True
            except BlockingIOError:
                if attempt + 1 < LOCK_ATTEMPTS:
                    time.sleep(LOCK_RETRY_SECONDS)
        return False

    def _clear_calls_folder(self):
        """Create the folder of the output files of state-changing children, and delete the files a broker that ended without its stop left there, once no child of it is alive (the write lock is free)."""
        try:
            with files.folder(self.calls_dir, create=True) as calls_fd:
                try:
                    lock_fd = self._take_write_lock(shared=False)
                except _LockNotUsable:
                    raise StartRefused(f"{BROKER_FOLDER} cannot be used") from None
                if lock_fd is None:
                    # A child of a broker that is gone still works and writes its output there.
                    return
                try:
                    for name in os.listdir(calls_fd):
                        with contextlib.suppress(OSError):
                            if stat.S_ISREG(os.stat(name, dir_fd=calls_fd, follow_symlinks=False).st_mode):
                                os.unlink(name, dir_fd=calls_fd)
                finally:
                    os.close(lock_fd)
        except files.BrokerFileError as error:
            raise StartRefused(f"{os.path.join(BROKER_FOLDER, CALLS_FOLDER_NAME)}: {error.message}") from None

    def _bind(self):
        """Remove a socket left by a broker that is gone (the lock is held, so no broker runs), and bind a new one inside the 0700 folder."""
        try:
            info = os.lstat(self.socket_path)
        except FileNotFoundError:
            info = None
        except OSError:
            raise StartRefused(f"{SOCKET_PATH} cannot be looked at") from None
        if info is not None:
            if not stat.S_ISSOCK(info.st_mode):
                raise StartRefused(f"{SOCKET_PATH} is not a socket: remove it")
            try:
                os.unlink(self.socket_path)
            except OSError:
                raise StartRefused(f"{SOCKET_PATH} cannot be removed") from None
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        # No thread exists yet, so the process-wide umask can be changed for the bind.
        old_umask = os.umask(0o177)
        try:
            listener.bind(self.socket_path)
            listener.listen(128)
        except OSError:
            listener.close()
            raise StartRefused(f"{SOCKET_PATH} cannot be bound") from None
        finally:
            os.umask(old_umask)
        info = os.lstat(self.socket_path)
        self._socket_identity = (info.st_dev, info.st_ino)
        self._listener = listener

    def _close_files(self):
        if self._listener is not None:
            self._listener.close()
            self._listener = None
        if self.audit is not None:
            self.audit.close()
            self.audit = None
        for name in ("_lock_fd", "_log_fd"):
            fd = getattr(self, name)
            if fd is not None:
                setattr(self, name, None)
                with contextlib.suppress(OSError):
                    os.close(fd)

    # ── serving and stopping ────────────────────────────────────────────────

    def signal_stop(self, force=False):
        """What a signal handler calls: it only notes the stop and wakes the serving loop, which then asks for it; it takes no lock (a handler runs in the main thread, which may hold one) and starts nothing. With `force` the children that run are ended first."""
        if force:
            self._signalled_force = True
        self._signalled_stop = True
        with contextlib.suppress(OSError):
            os.write(self._wake_write, b"x")

    def request_stop(self, force=False):
        """Ask the broker to stop, from any thread but a signal handler (which calls `signal_stop`). With `force`, the process groups of the children that run are ended first (TERM, then KILL)."""
        if force:
            with self._state_lock:
                self._force = True
                children = list(self._children)
            for child in children:
                child.end_soon("forced")
        self._stopping.set()
        with contextlib.suppress(OSError):
            os.write(self._wake_write, b"x")

    def _act_on_signals(self):
        """Ask for the stop a signal handler noted. Called from the serving loop and from the stop's wait, never from a handler."""
        if self._signalled_force and not self._force:
            self.request_stop(force=True)
        elif self._signalled_stop and not self._stopping.is_set():
            self.request_stop()

    def serve_forever(self):
        """Answer connections until a stop is asked, then stop: no new connection, every handler finished (a state-changing child is waited for without a limit), `broker_stop` written, the socket removed, the files given back. Returns True after a stop that was asked, False when the serving loop ended on an error: the error is logged by the name of its kind and the broker stops the same way."""
        selector = None
        clean = True
        try:
            # Inside the `try`: a housekeeper that cannot start, or a selector that cannot be made, ends the broker like any error of the loop.
            self._housekeeper = threading.Thread(target=self._housekeeping, name="housekeeping", daemon=True)
            self._housekeeper.start()
            selector = selectors.DefaultSelector()
            selector.register(self._listener, selectors.EVENT_READ)
            selector.register(self._wake_read, selectors.EVENT_READ)
            while not self._stopping.is_set():
                for key, _ in selector.select():
                    if key.fileobj is self._listener:
                        self._accept()
                    else:
                        with contextlib.suppress(OSError):
                            os.read(self._wake_read, 4096)
                self._act_on_signals()
        except Exception as error:
            clean = False
            self.log(f"the serving loop stopped on an error ({type(error).__name__})")
        finally:
            if selector is not None:
                selector.close()
            self._shutdown()
        return clean

    def _take_connection(self):
        """The next connection of the listener."""
        connection, _ = self._listener.accept()
        return connection

    def _accept(self):
        try:
            connection = self._take_connection()
        except OSError as error:
            if error.errno in (errno.EMFILE, errno.ENFILE, errno.ENOBUFS, errno.ENOMEM):
                # The listener stays readable while the connection waits: without a rest the loop would spin.
                self._stopping.wait(ACCEPT_PAUSE_SECONDS)
            return
        with self._state_lock:
            full = len(self._connections) >= self.limits.connections_max
            if not full:
                thread = threading.Thread(target=self._handle, args=(connection,), daemon=True)
                self._connections.add(thread)
        if full:
            connection.close()
            return
        try:
            thread.start()
        except BaseException as error:
            # The thread never started: it is no connection to wait for at the stop, and its connection is closed like one over the limit.
            with self._state_lock:
                self._connections.discard(thread)
            connection.close()
            if not isinstance(error, RuntimeError):
                raise
            self.log("a connection was closed: no thread could be started for it")

    def _shutdown(self):
        # First: the housekeeping ends only on this, and the loop can come here on an error without a stop having been asked.
        self._stopping.set()
        self._listener.close()
        self._listener = None
        while True:
            with self._state_lock:
                # A thread that never started (its start failed) never ends by itself: it is dropped, not waited for.
                self._connections = {thread for thread in self._connections if thread.ident is not None}
                running = list(self._connections)
            if not running:
                break
            for thread in running:
                # A forced stop can be signalled while this waits: it is acted on between the looks.
                while thread.is_alive():
                    thread.join(0.1)
                    self._act_on_signals()
        # Joined only when it was started: a thread that never started cannot be joined.
        if self._housekeeper is not None and self._housekeeper.ident is not None:
            self._housekeeper.join()
        self.audit.broker_stop()
        with contextlib.suppress(OSError):
            info = os.lstat(self.socket_path)
            if (info.st_dev, info.st_ino) == self._socket_identity:
                os.unlink(self.socket_path)
        self.log("stopped" + (" (forced)" if self._force else ""))
        self._close_files()
        for fd in (self._wake_read, self._wake_write):
            with contextlib.suppress(OSError):
                os.close(fd)

    def _leftover_call_files(self, calls_fd):
        """The names of the files of `calls/` that belong to no call this broker runs. Listed under the state lock: a call (a `run.cancel` holds no write lock) names itself there before it makes its files."""
        with self._state_lock:
            return [name for name in os.listdir(calls_fd) if name.rsplit(".", 1)[0] not in self._live_calls]

    def _sweep_calls_folder(self):
        """Delete the files of `calls/` that belong to no call this broker runs: what an orphaned child left after it ended, which can hold secrets before they were overwritten. The folder is looked at first, and only when there is such a file is `write.lock` taken (exclusive, one try: no state-changing child of any broker may be running), so that a sweep of an empty folder never makes a call answer `busy`; then the folder is listed again and the files deleted. Returns how many were deleted, or None when the lock was held."""
        try:
            with files.folder(self.calls_dir) as calls_fd:
                if calls_fd is None or not self._leftover_call_files(calls_fd):
                    return 0
                try:
                    lock_fd = self._take_write_lock(shared=False, attempts=1)
                except _LockNotUsable:
                    return None
                if lock_fd is None:
                    return None
                deleted = 0
                try:
                    # The lock order is the write lock, then the state lock: listed again under both.
                    with self._state_lock:
                        for name in os.listdir(calls_fd):
                            if name.rsplit(".", 1)[0] in self._live_calls:
                                continue
                            with contextlib.suppress(OSError):
                                if stat.S_ISREG(os.stat(name, dir_fd=calls_fd, follow_symlinks=False).st_mode):
                                    os.unlink(name, dir_fd=calls_fd)
                                    deleted += 1
                finally:
                    os.close(lock_fd)
                return deleted
        except (files.BrokerFileError, OSError):
            return 0

    def _housekeeping(self):
        from social_data_pipeline.control import runs

        while not self._stopping.wait(self.limits.housekeeping_seconds):
            try:
                for number in self.sessions.end_expired():
                    self.audit.session_end(number, session_table.END_EXPIRED)
                self.audit.flush_counts()
                runs.reap_supervisors()
                self._sweep_calls_folder()
            except Exception as error:
                self.log(f"housekeeping failed ({type(error).__name__})")

    # ── one connection ──────────────────────────────────────────────────────

    def _handle(self, connection):
        try:
            try:
                peer = self.peer_uid(connection)
            except OSError:
                peer = None
            if peer != self.uid:
                self.audit.count_unauthenticated()
                return
            reply = self._read_and_answer(connection)
            if reply is not None:
                self._send(connection, reply)
        except Exception as error:
            self.log(f"a request failed ({type(error).__name__})")
            with contextlib.suppress(Exception):
                self._send(connection, _error("internal"))
        finally:
            with contextlib.suppress(OSError):
                connection.close()
            with self._state_lock:
                self._connections.discard(threading.current_thread())

    def _read_and_answer(self, connection):
        deadline = self.clock() + self.limits.request_seconds

        def read(count):
            left = deadline - self.clock()
            if left <= 0:
                raise TimeoutError()
            connection.settimeout(left)
            return connection.recv(count)

        try:
            request = framing.read_frame(read, max_bytes=REQUEST_MAX_BYTES, deadline=deadline, now=self.clock)
        except framing.FrameTooLarge:
            self.audit.count_unauthenticated()
            return _error("request_too_large")
        except _SILENT_FRAME_ERRORS:
            self.audit.count_unauthenticated()
            return None
        except framing.FrameError:
            self.audit.count_unauthenticated()
            return _error("bad_request")
        try:
            return self._answer(request)
        except _Refusal as refusal:
            return refusal.reply

    def _send(self, connection, reply):
        try:
            frame = framing.encode(reply, max_bytes=self.limits.reply_max_bytes)
        except framing.FrameTooLarge:
            frame = framing.encode(_error("too_large"), max_bytes=self.limits.reply_max_bytes)
        except framing.FrameError:
            self.log("a reply could not be written as JSON")
            frame = framing.encode(_error("internal"), max_bytes=self.limits.reply_max_bytes)
        try:
            connection.settimeout(self.limits.send_seconds)
            connection.sendall(frame)
        except OSError as error:
            # The client is gone, or does not read: the call itself has run as it would have.
            self.log(f"a reply could not be sent ({type(error).__name__})")

    # ── the password file and the session ───────────────────────────────────

    def _read_password_file(self):
        """(the password file, None) or (None, the refusal reply) when it cannot be used."""
        try:
            return passwords.read_password_file(self.broker_dir), None
        except passwords.PasswordFileNotUsable:
            return None, _error("password_file_not_usable")

    def _current_password_file(self):
        """Read the password file once for a request; when its digest changed since the broker last saw one, the login budget is filled again (the password was set anew). Raises _Refusal when the file cannot be used."""
        password_file, refusal = self._read_password_file()
        if refusal is not None:
            raise _Refusal("password_file_not_usable")
        with self._state_lock:
            changed = password_file.digest != self._seen_digest
            self._seen_digest = password_file.digest
        if changed:
            self.sessions.reset_budget()
        return password_file

    def _note_ended(self, ended):
        for number, reason in ended:
            self.audit.session_end(number, reason)

    def _session_refused(self, refusal):
        """The reply to a SessionRefused, with its ends written and an unauthorized request counted."""
        self._note_ended(refusal.ended)
        if refusal.code == session_table.UNAUTHORIZED:
            self.audit.count_unauthenticated()
        return _Refusal(refusal.code, refusal.message)

    def _session(self, token, password_file):
        try:
            return self.sessions.session_for(token, password_file.digest)
        except session_table.SessionRefused as refusal:
            raise self._session_refused(refusal) from None

    # ── requests ────────────────────────────────────────────────────────────

    def _answer(self, request):
        op = request.get("op")
        if not isinstance(op, str) or op not in OPS:
            self.audit.count_unauthenticated()
            return _error("bad_request")
        if op in REFUSED_WHILE_STOPPING and self._stopping.is_set():
            return _error("stopping")
        if op == "hello":
            if set(request) != {"op"}:
                return _error("bad_request")
            password_file, refusal = self._read_password_file()
            # A file that cannot be used tells nothing about whether a password is set.
            return _ok({"protocol": PROTOCOL, "password_set": None if refusal is not None else bool(password_file)})
        if op == "login":
            if set(request) != {"op", "password"}:
                self.audit.count_unauthenticated()
                return _error("bad_request")
            return self._login(request["password"])
        # Every other op needs a session before anything else of the request is looked at.
        password_file = self._current_password_file()
        token = request.get("token")
        info = self._session(token, password_file)
        keys = set(request) - {"op"}
        if not OPS[op] <= keys <= OPS[op] | OPTIONAL_KEYS.get(op, frozenset()):
            return _error("bad_request")
        if op == "logout":
            number = self.sessions.logout(token)
            if number is not None:
                self.audit.logout(number)
            return _ok({})
        if op == "session.clear_db_password":
            try:
                self.sessions.clear_db_password(token, password_file.digest)
            except session_table.SessionRefused as refusal:
                raise self._session_refused(refusal) from None
            return _ok({})
        if op == "session.set_db_password":
            return self._set_db_password(token, info, request, password_file)
        return self._call(token, info, request, password_file)

    def _login(self, candidate):
        try:
            password_file = self._current_password_file()
        except _Refusal as refusal:
            return refusal.reply
        try:
            login = self.sessions.login(candidate, lambda: password_file)
        except session_table.SessionRefused as refusal:
            if refusal.code in (session_table.UNAUTHORIZED, session_table.THROTTLED):
                self.audit.count_failed_login()
            return _error(refusal.code, refusal.message)
        self._note_ended(login.ended)
        self.audit.login(login.number)
        return _ok({"token": login.token, "session": login.number, "expires_in": session_table.SESSION_SECONDS})

    def _step_up(self, token, info, candidate, password_file, verb_name=None):
        try:
            return self.sessions.step_up(token, candidate, lambda: password_file)
        except session_table.SessionRefused as refusal:
            self._note_ended(refusal.ended)
            if refusal.code in (session_table.STEP_UP_FAILED, session_table.SESSION_ENDED):
                self.audit.step_up_failed(info.number)
                if verb_name is not None:
                    self.audit.call_refused(info.number, verb_name, refusal.code)
                raise _Refusal(refusal.code, refusal.message) from None
            if refusal.code == session_table.UNAUTHORIZED:
                self.audit.count_unauthenticated()
            raise _Refusal(refusal.code, refusal.message) from None

    def _set_db_password(self, token, info, request, password_file):
        from social_data_pipeline.control.types import Secret

        try:
            value = Secret().validate(request["password"])
        except ValueError:
            return _error("invalid_input", field="password")
        # The management password again, before the value is kept.
        self._step_up(token, info, request["step_up"], password_file)
        try:
            self.sessions.set_db_password(token, password_file.digest, value)
        except session_table.SessionRefused as refusal:
            raise self._session_refused(refusal) from None
        return _ok({})

    # ── a call ──────────────────────────────────────────────────────────────

    def _refuse(self, number, verb_name, code, field=None, message=None):
        self.audit.call_refused(number, verb_name, code, field)
        return _Refusal(code, message, field)

    def _call(self, token, info, request, password_file):
        verb_name, fields = request["verb"], request["fields"]
        number = info.number
        try:
            validated = verbs.validate_request(verb_name, fields)
        except errors.UnknownVerb:
            raise self._refuse(number, verb_name, "unknown_verb") from None
        except errors.VerbNotAvailable:
            raise self._refuse(number, verb_name, "not_available") from None
        except errors.InputError as error:
            raise self._refuse(number, verb_name, "invalid_input", error.field) from None
        verb = verbs.VERBS[verb_name]
        request_secrets = [value for value in validated.values() if isinstance(value, SecretValue)]
        if isinstance(request.get("step_up"), str):
            request_secrets.append(request["step_up"])

        # The class decides, and nothing the request carries.
        if verb.cls == "step_up":
            if "step_up" not in request:
                raise self._refuse(number, verb_name, "step_up_required")
            self._step_up(token, info, request["step_up"], password_file, verb_name)

        plain = {name: value.reveal() if isinstance(value, SecretValue) else value for name, value in validated.items()}
        db_password_from = "none"
        if verb_name in DB_PASSWORD_VERBS:
            if DB_PASSWORD_FIELD in validated:
                db_password_from = "fields"
            else:
                try:
                    kept = self.sessions.db_password(token, password_file.digest)
                except session_table.SessionRefused as refusal:
                    raise self._session_refused(refusal) from None
                if kept is not None:
                    plain[DB_PASSWORD_FIELD] = kept.reveal()
                    db_password_from = "session"
        if verb_name == "db.start" and validated.get("service") in SHELL_LINE_SERVICES:
            password = plain.get(DB_PASSWORD_FIELD)
            if isinstance(password, str) and SHELL_LINE_CHARACTERS & set(password):
                raise self._refuse(number, verb_name, "password_not_allowed_here", DB_PASSWORD_FIELD)
        if verb_name == "run.logs" and validated.get("limit", RUN_LOGS_DEFAULT_LIMIT) < redact.MIN_PIECE_BYTES:
            raise self._refuse(number, verb_name, "invalid_input", "limit")

        context = {"number": number, "verb": verb, "validated": validated, "plain": plain, "db_password_from": db_password_from, "secrets": request_secrets, "redacted": False}
        if verb.cls == "read":
            self.audit.count_read(number)
            result = self._read_verb(context)
        else:
            result = self._state_changing_verb(context)
        # A piece of a log read from its file is overwritten as a stretch of the whole log already; a second pass would take its first byte for the start of a line.
        return result if context["redacted"] else self._redacted(result, request_secrets)

    def _refuse_while_a_run_is_alive(self, number, verb_name):
        from social_data_pipeline import runstore

        try:
            listing = self.run_lister()
        except runstore.DockerUnavailable:
            raise self._refuse(number, verb_name, "docker_unreachable") from None
        except runstore.StoreError:
            raise self._refuse(number, verb_name, "store_not_usable") from None
        if not listing["docker_available"]:
            raise self._refuse(number, verb_name, "docker_unreachable")
        for run in listing["runs"]:
            if run["kind"] == runstore.KIND_TERMINAL or run["state"] in ALIVE_STATES:
                raise self._refuse(number, verb_name, "run_active")

    def _secrets(self, request_secrets):
        return [*self.sessions.known_secrets(), *environ.known_secrets(os.environ), *request_secrets]

    def _redacted(self, reply, request_secrets):
        """The reply with every known secret overwritten in its result, or in the details of its error; `too_large` when that cannot be done in full."""
        try:
            if reply["ok"]:
                return _ok(redact.redact_data(reply["result"], self._secrets(request_secrets))[0])
            return {"ok": False, "error": redact.redact_data(reply["error"], self._secrets(request_secrets))[0]}
        except (redact.DataTooLargeToRedact, redact.DataTooDeep, redact.DataNotJson):
            return _error("too_large")

    # ── read verbs ──────────────────────────────────────────────────────────

    def _read_verb(self, context):
        verb = context["verb"]
        if verb.name == "audit.list":
            limit = context["validated"].get("limit", AUDIT_LIST_DEFAULT_LIMIT)
            lines = audit.read_tail(os.path.join(self.broker_dir, audit.FILE_NAME), limit)
            # Overwritten with the sessions' and the environment's secrets only: a word a request sent as a password (remembered, or of this request) would otherwise be blanked out of the audit view by whoever holds a token.
            context["redacted"] = True
            try:
                return _ok({"data": redact.redact_data(lines, [*self.sessions.session_secrets(), *environ.known_secrets(os.environ)])[0]})
            except (redact.DataTooLargeToRedact, redact.DataTooDeep, redact.DataNotJson):
                return _error("too_large")
        if verb.name == "run.logs":
            served = self._run_log_from_file(context)
            if served is not None:
                return served
        if not self._read_children.acquire(blocking=False):
            return _error("busy")
        try:
            return self._getter_reply(*self._run_child(context, seconds=self.limits.read_seconds, keep_whole_stdout=True))
        finally:
            self._read_children.release()

    def _run_log_from_file(self, context):
        """The piece of a recorded run's log, read here from the bytes of its file and overwritten as a stretch of the whole log, so that a secret cut by the piece's end is overwritten too; None when the output is Docker's (a run started in a terminal, a detached run), which the child reads.

        While a run has not ended (any state but `finished` and `cancelled`), its log can still grow, and a secret it is writing can be half written: the last bytes of the file, as many as the longest form of a known secret less one, are held back. `next_offset` stops before them and `complete` is false; the log of an ended run is served to its end, with `complete` true."""
        from social_data_pipeline import runstore

        fields = context["validated"]
        run_id = fields["run_id"]
        if runstore.run_id_kind(run_id) != runstore.KIND_RECORDED:
            return None
        try:
            state = runstore.read_run(self.root, run_id)["state"]
        except runstore.StoreError as error:
            return _error("failed", exit_code=None, errors=error.message)
        if state == runstore.STATE_DETACHED:
            return None
        context["redacted"] = True
        offset = fields.get("offset", 0)
        limit = fields.get("limit", RUN_LOGS_DEFAULT_LIMIT)
        final = state in runstore.FINAL_STATES
        secrets = self._secrets(context["secrets"])
        try:
            raw, size = runstore.read_run_log_bytes(self.root, run_id, offset, limit + 3)
            if final:
                served_to = size
                end = runstore.log_piece_end(raw, limit, True) if raw else 0
            else:
                read_offset, read_count, cut_start = redact.window(offset, 1, secrets)
                served_to = max(0, size - (read_count - cut_start - 1))
                room = min(limit, served_to - offset)
                end = runstore.log_piece_end(raw[:room], room, False) if room > 0 else 0
            if end == 0:
                text = ""
                next_offset = (size if offset >= size else offset) if final else min(offset, served_to)
            else:
                piece, _ = redact.redact_piece(lambda at, count: runstore.read_run_log_bytes(self.root, run_id, at, count)[0], offset, end, secrets)
                text, next_offset = piece.decode("utf-8", "replace"), offset + end
        except runstore.StoreError as error:
            return _error("failed", exit_code=None, errors=error.message)
        except redact.DataTooLargeToRedact:
            return _error("too_large")
        return _ok({"data": {"id": run_id, "kind": runstore.KIND_RECORDED, "state": state, "text": text, "next_offset": next_offset, "complete": final}})

    def _getter_reply(self, outcome, exit_code, child):
        if outcome in ("timeout", "too_large"):
            return _error(outcome)
        if outcome == "exited" and exit_code == 0:
            try:
                return _ok({"data": json.loads(bytes(child.stdout))})
            except (ValueError, RecursionError):
                pass
        return _error("failed", exit_code=exit_code, errors=_tail_text(child.stderr, child.trimmed["stderr"]))

    # ── state-changing verbs ────────────────────────────────────────────────

    def _state_changing_verb(self, context):
        """A call of the class `write` or `step_up`. The write lock is taken first (exclusive; shared for `run.start`; none for `run.cancel`), and only then are the live runs looked for, for a destructive verb, so that no run can start between the look and the action. `call_accepted` is written before anything runs and `call_ended` after, also when the call ends on an error."""
        verb, number = context["verb"], context["number"]
        validated, db_password_from = context["validated"], context["db_password_from"]
        if not self.audit.writable():
            raise self._refuse(number, verb.name, "audit_not_writable")
        lock_fd = None
        if verb.name not in UNLOCKED_VERBS:
            try:
                lock_fd = self._take_write_lock(shared=verb.name in SHARED_LOCK_VERBS)
            except _LockNotUsable:
                raise self._refuse(number, verb.name, "broker_folder_not_usable") from None
            if lock_fd is None:
                raise self._refuse(number, verb.name, "busy")
        try:
            if verb.name in DESTRUCTIVE_VERBS:
                self._refuse_while_a_run_is_alive(number, verb.name)
            self.audit.call_accepted(number, verb, validated, db_password_from)
            if db_password_from == "fields":
                # Kept for redaction like a session's password: what a run prints with it is overwritten after the call.
                self.sessions.remember_secret(context["plain"][DB_PASSWORD_FIELD])
            try:
                if verb.name == "run.start":
                    reply, outcome, exit_code = self._start_run(context)
                else:
                    reply, outcome, exit_code = self._run_state_changing_child(context, lock_fd)
            except BaseException:
                self.audit.call_ended(number, verb, validated, db_password_from, "internal", None)
                raise
            self.audit.call_ended(number, verb, validated, db_password_from, outcome, exit_code)
            return reply
        finally:
            if lock_fd is not None:
                os.close(lock_fd)

    def _take_write_lock(self, *, shared, attempts=WRITE_LOCK_ATTEMPTS):
        """A new descriptor that holds the write lock (shared or exclusive), or None when it cannot be had now. Tried `attempts` times within about a fifth of a second, never waited for: a sweep of `calls/` holds it for an instant, a call holds it for as long as it runs. A new open each time: two descriptors of one open file would share the lock. Raises _LockNotUsable when the broker folder is gone or cannot be used: a lock on any other file would exclude nothing."""
        try:
            with files.folder(self.broker_dir) as folder_fd:
                if folder_fd is None:
                    raise _LockNotUsable()
                fd = files.open_file(folder_fd, WRITE_LOCK_NAME, os.O_RDWR, create=True)
        except files.BrokerFileError:
            raise _LockNotUsable() from None
        for attempt in range(attempts):
            try:
                fcntl.flock(fd, (fcntl.LOCK_SH if shared else fcntl.LOCK_EX) | fcntl.LOCK_NB)
                return fd
            except BlockingIOError:
                if attempt + 1 < attempts:
                    time.sleep(LOCK_RETRY_SECONDS)
            except OSError:
                break
        os.close(fd)
        return None

    def _start_run(self, context):
        """Start a recorded run: (reply, outcome for the audit line, exit code)."""
        try:
            run_id = self.run_starter(self.root, context["plain"], started_by=f"web-{context['number']}")
        except errors.RunNotStarted as error:
            return _error(error.code, error.message), error.code, None
        except errors.InputError as error:
            return _error("invalid_input", field=error.field), "invalid_input", None
        except (errors.ControlError, ValueError):
            return _error("start_failed"), "start_failed", None
        return _ok({"run_id": run_id}), "started", None

    def _next_call_name(self):
        """A new name for the output files of a call, noted as live until `_end_call_name`."""
        with self._state_lock:
            self._call_counter += 1
            name = f"{self._call_counter}-{os.urandom(4).hex()}"
            self._live_calls.add(name)
            return name

    def _run_state_changing_child(self, context, lock_fd):
        """Run the child of a state-changing call with its standard output and standard error in two files of `.sdp/broker/calls/`, not in pipes: a child whose broker is gone (a crash) then still writes its output and runs to its end. The files are read (the last 256 KiB of each) and deleted when the child has ended. Returns (reply, outcome, exit code)."""
        name = self._next_call_name()
        outputs = []
        try:
            with files.folder(self.calls_dir, create=True) as calls_fd:
                for suffix in (".out", ".err"):
                    outputs.append(files.open_file(calls_fd, name + suffix, os.O_WRONLY | os.O_APPEND, create=True))
        except files.BrokerFileError:
            for fd in outputs:
                os.close(fd)
            self._delete_call_files(name)
            with self._state_lock:
                self._live_calls.discard(name)
            self.log("the output files of a call could not be made")
            return _error("start_failed"), "start_failed", None
        try:
            try:
                outcome, exit_code, _ = self._run_child(context, seconds=None, keep_whole_stdout=False, pass_fds=() if lock_fd is None else (lock_fd,), output_files=tuple(outputs))
            except _Refusal as refusal:
                return refusal.reply, "start_failed", None
            finally:
                for fd in outputs:
                    os.close(fd)
            output, errors_text = (self._call_file_tail(name + suffix) for suffix in (".out", ".err"))
            return _ok({"exit_code": exit_code, "output": output, "errors": errors_text}), outcome, exit_code
        finally:
            self._delete_call_files(name)
            with self._state_lock:
                self._live_calls.discard(name)

    def _call_file_tail(self, name):
        """The last bytes of an output file of a call (at most the tail size), as text that begins with a whole line when its start was cut away."""
        limit = self.limits.output_tail_bytes
        try:
            with files.folder(self.calls_dir) as calls_fd:
                if calls_fd is None:
                    return ""
                fd = files.open_file(calls_fd, name, os.O_RDONLY)
        except files.BrokerFileError:
            return ""
        if fd is None:
            return ""
        try:
            size = os.fstat(fd).st_size
            start = max(0, size - limit)
            data = bytearray()
            while len(data) < size - start:
                chunk = os.pread(fd, size - start - len(data), start + len(data))
                if not chunk:
                    break
                data += chunk
        except OSError:
            return ""
        finally:
            os.close(fd)
        return _tail_text(data, start > 0)

    def _delete_call_files(self, name):
        with contextlib.suppress(files.BrokerFileError, OSError):
            with files.folder(self.calls_dir) as calls_fd:
                if calls_fd is None:
                    return
                for suffix in (".out", ".err"):
                    with contextlib.suppress(FileNotFoundError):
                        os.unlink(name + suffix, dir_fd=calls_fd)

    def _run_child(self, context, *, seconds, keep_whole_stdout, pass_fds=(), output_files=None):
        """Run the generic child for the call: (outcome, exit code, the child). Its output goes into pipes, or with `output_files` (two descriptors) into those files. Raises _Refusal `start_failed` when it could not be started."""
        request = json.dumps({"verb": context["verb"].name, "fields": context["plain"]}).encode("utf-8")
        child = _Child(self, self.child_argv, request, pass_fds=pass_fds, keep_whole_stdout=keep_whole_stdout, output_files=output_files)
        with self._state_lock:
            self._children.add(child)
            if self._force:
                child.end_soon("forced")
        try:
            try:
                outcome, exit_code = child.run(seconds)
            except OSError as error:
                self.log(f"a child could not be started ({errno.errorcode.get(error.errno, type(error).__name__)})")
                raise _Refusal("start_failed") from None
        finally:
            with self._state_lock:
                self._children.discard(child)
        return outcome, exit_code, child


def _default_run_starter(root, fields, *, started_by):
    from social_data_pipeline.control import runs

    return runs.start_run(root, fields, started_by=started_by)


def _default_run_lister(root):
    """Every run of the deployment, with the service table of the CLI module of the deployment folder (imported at use, as `control.runs` does)."""
    from social_data_pipeline import runstore

    import sdp

    return runstore.list_runs(root, sdp._run_service_profiles(), 2 ** 62)


# ── the broker as seen from outside: running or not, stop ───────────────────


def _lock_file_content(root):
    """(pid, start time) from the lock file, or None when there is none or it holds something else."""
    try:
        with files.folder(broker_folder(root)) as folder_fd:
            if folder_fd is None:
                return None
            raw = files.read_file(folder_fd, LOCK_NAME, LOCK_FILE_MAX_BYTES)
    except files.BrokerFileError:
        return None
    if raw is None:
        return None
    words = raw.decode("ascii", "replace").split()
    if len(words) != 2 or not words[0].isascii() or not words[0].isdigit() or len(words[0]) > 9:
        return None
    return int(words[0]), words[1]


def _lock_identity_and_held(root):
    """((device, inode) of the lock file, whether a process holds its lock), or None when there is no lock file. The lock is probed with a shared lock that is given back at once."""
    try:
        with files.folder(broker_folder(root)) as folder_fd:
            if folder_fd is None:
                return None
            fd = files.open_file(folder_fd, LOCK_NAME, os.O_RDONLY)
    except files.BrokerFileError:
        return None
    if fd is None:
        return None
    try:
        info = os.fstat(fd)
        try:
            fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
            held = False
        except BlockingIOError:
            held = True
        return (info.st_dev, info.st_ino), held
    finally:
        os.close(fd)


def _lock_holders(identity, locks_path="/proc/locks"):
    """The pids that hold an exclusive `flock` lock on the file `identity` (device, inode), read from the kernel's table of locks. Raises OSError when the table cannot be read."""
    device, inode = identity
    wanted = f"{os.major(device):02x}:{os.minor(device):02x}:{inode}"
    holders = []
    with open(locks_path, "r", encoding="ascii", errors="replace") as table:
        for line in table:
            words = line.split()
            # A waiting request is shown with "->" after the number; it holds nothing.
            if len(words) >= 6 and words[1] == "FLOCK" and words[3] == "WRITE" and words[5] == wanted and words[4].isdigit():
                holders.append(int(words[4]))
    return holders


def running_broker(root):
    """`{"pid", "started"}` of the broker that runs for a deployment, or None when none does (its lock is free). The pid is the one in the lock file."""
    found = _lock_identity_and_held(root)
    if found is None or not found[1]:
        return None
    content = _lock_file_content(root)
    return {"pid": content[0] if content else None, "started": content[1] if content else None}


def stop_broker(root, *, force=False, on_wait=None, send=None, first_wait_seconds=None):
    """Stop the broker of a deployment: True when it ran and has ended, False when none runs. The signal (TERM, or USR1 with `force`) goes through a handle on the process (`pidfd_open`), and only after the lock was found still held, by that very process (the kernel's table of locks), and the lock file still names it; otherwise StopRefused and no signal. `on_wait()` is called once when the broker has not ended after `first_wait_seconds` (STOP_SAY_WAIT_SECONDS when None: it waits for an action to finish); then it is waited for without a limit. A broker that ends between the checks and the signal counts as stopped. `send` replaces `signal.pidfd_send_signal`."""
    send = send or signal.pidfd_send_signal
    found = _lock_identity_and_held(root)
    if found is None or not found[1]:
        return False
    identity = found[0]
    content = _lock_file_content(root)
    if content is None:
        raise StopRefused("the lock of the broker is held, but its lock file names no process: nothing was signalled")
    pid = content[0]
    try:
        handle = pidfd_open(pid)
    except ProcessLookupError:
        raise StopRefused("the lock file names a process that does not exist: nothing was signalled") from None
    except OSError:
        raise StopRefused("the process the lock file names cannot be reached: nothing was signalled") from None
    try:
        try:
            holders = _lock_holders(identity)
        except OSError:
            raise StopRefused("the table of locks cannot be read, so the holder of the lock is not known: nothing was signalled") from None
        again = _lock_identity_and_held(root)
        if again is None or again != (identity, True) or _lock_file_content(root) != content or holders != [pid]:
            raise StopRefused("the lock of the broker is not held by the process its lock file names: nothing was signalled")
        try:
            send(handle, signal.SIGUSR1 if force else signal.SIGTERM)
        except ProcessLookupError:
            # It ended after the checks: the handle names it and no other process, so it is gone.
            return True
        poller = select.poll()
        poller.register(handle, select.POLLIN)
        if not poller.poll(int((STOP_SAY_WAIT_SECONDS if first_wait_seconds is None else first_wait_seconds) * 1000)):
            if on_wait is not None:
                on_wait()
            poller.poll(None)
        return True
    finally:
        os.close(handle)


def broker_status(root):
    """What `sdp broker status` shows: `running`, `pid`, `started`, `socket` (relative to the root), `password_set` (asked of the broker when it runs and answers, else read from the file; None when that cannot be told) and `answering` (whether `hello` was answered; None when it does not run)."""
    from social_data_pipeline.control import client

    running = running_broker(root)
    status = {"running": running is not None, "pid": None, "started": None, "socket": SOCKET_PATH, "password_set": None, "answering": None}
    if running is not None:
        status.update(pid=running["pid"], started=running["started"], answering=False)
        try:
            status["password_set"] = client.Client(socket_path(root), timeout=5).hello()["password_set"]
            status["answering"] = True
        except client.BrokerError:
            pass
    if status["password_set"] is None:
        try:
            status["password_set"] = bool(passwords.read_password_file(broker_folder(root)))
        except passwords.PasswordFileNotUsable:
            status["password_set"] = None
    return status


def _end_unanswering(process, grace_seconds):
    """End a broker process that did not answer in time: TERM through a handle on it, KILL when it has not ended after the grace time, and collect it."""
    try:
        handle = pidfd_open(process.pid)
    except OSError:
        handle = None
    try:
        with contextlib.suppress(ProcessLookupError):
            if handle is not None:
                signal.pidfd_send_signal(handle, signal.SIGTERM)
            else:
                process.terminate()
        try:
            process.wait(grace_seconds)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
    finally:
        if handle is not None:
            os.close(handle)


def start_detached(root, *, argv=None, wait_seconds=START_WAIT_SECONDS, grace_seconds=START_GRACE_SECONDS):
    """Start the broker of a deployment as a process of its own (a session of its own, standard input from the null device, standard output and standard error appended to `broker.log`, the environment cut down to the allowed names) and wait at most `wait_seconds` until it answers `hello` or ends. Returns ("started", pid), ("running", pid) when one runs already, or ("failed", the start error's line). A broker that has not answered by then is ended (TERM, KILL after `grace_seconds`) and collected before the failure is reported. `argv` replaces the server command (SERVER_ARGV); `--root`, `--foreground` and `--status-fd` are added to it."""
    from social_data_pipeline.control import client

    root = os.path.realpath(os.fspath(root))
    running = running_broker(root)
    if running is not None:
        return "running", running["pid"]
    try:
        with files.folder(os.path.join(root, STATE_FOLDER), create=True):
            pass
        with files.folder(broker_folder(root), create=True) as folder_fd:
            log_fd = files.open_file(folder_fd, LOG_NAME, os.O_WRONLY | os.O_APPEND, create=True)
    except files.BrokerFileError as error:
        return "failed", f"{BROKER_FOLDER}: {error.message}"
    status_read, status_write = os.pipe()
    try:
        command = [*(argv or SERVER_ARGV), "--root", root, "--foreground", "--status-fd", str(status_write)]
        process = subprocess.Popen(command, cwd=root, stdin=subprocess.DEVNULL, stdout=log_fd, stderr=log_fd, start_new_session=True, pass_fds=(status_write,), env=environ.clean(os.environ))
    except OSError:
        os.close(status_read)
        return "failed", "the broker process could not be started"
    finally:
        os.close(log_fd)
        os.close(status_write)
    try:
        deadline = time.monotonic() + wait_seconds
        while time.monotonic() < deadline:
            if process.poll() is not None:
                said = b""
                while True:
                    chunk = os.read(status_read, 4096)
                    if not chunk or len(said) > 4096:
                        break
                    said += chunk
                line = said.decode("utf-8", "replace").strip().splitlines()
                return "failed", line[0] if line else f"the broker ended while it started: see {os.path.join(BROKER_FOLDER, LOG_NAME)}"
            # Started means: this process holds the lock and its socket answers. A `hello` alone could come from another broker.
            running = running_broker(root)
            if running is not None and running["pid"] == process.pid:
                try:
                    client.Client(socket_path(root), timeout=1).hello()
                    return "started", process.pid
                except client.BrokerError:
                    pass
            time.sleep(0.05)
        _end_unanswering(process, grace_seconds)
        return "failed", f"the broker did not answer within {wait_seconds} seconds and was ended: see {os.path.join(BROKER_FOLDER, LOG_NAME)}"
    finally:
        os.close(status_read)


# ── the entry point ─────────────────────────────────────────────────────────


def main(argv=None):
    """`--root <root> [--foreground] [--status-fd N]`. With `--foreground` the broker serves in this process until TERM or INT (a stop that waits for the call that runs) or USR1 (a stop that ends it first), and the exit code is 0 after a stop that was asked, 1 when it did not start or when its serving loop ended on an error; a start error is printed on standard error and written to the status descriptor. Without it the broker is started detached, as `sdp broker start` does."""
    parser = argparse.ArgumentParser(prog="python -m social_data_pipeline.control.broker")
    parser.add_argument("--root", required=True)
    parser.add_argument("--foreground", action="store_true")
    parser.add_argument("--status-fd", type=int, default=None)
    args = parser.parse_args(argv)
    if not args.foreground:
        outcome, detail = start_detached(args.root)
        print({"started": f"The broker is started (pid {detail}).", "running": f"The broker is already running (pid {detail})."}.get(outcome, f"Error: {detail}"))
        return 1 if outcome == "failed" else 0

    broker = Broker(args.root, prepare=_prepare_process)
    signal.signal(signal.SIGTERM, lambda signum, frame: broker.signal_stop())
    signal.signal(signal.SIGINT, lambda signum, frame: broker.signal_stop())
    signal.signal(signal.SIGUSR1, lambda signum, frame: broker.signal_stop(force=True))
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    status_fd = args.status_fd
    try:
        broker.start()
    except StartRefused as refusal:
        print(f"Error: {refusal.message}", file=sys.stderr)
        if status_fd is not None:
            with contextlib.suppress(OSError):
                os.write(status_fd, f"{refusal.message}\n".encode("utf-8", "replace"))
        return 1
    finally:
        if status_fd is not None:
            with contextlib.suppress(OSError):
                os.close(status_fd)
    return 0 if broker.serve_forever() else 1


if __name__ == "__main__":
    sys.exit(main())
