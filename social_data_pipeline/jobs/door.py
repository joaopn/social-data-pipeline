"""The scheduler's control socket: a second door on the query scheduler, a unix socket that only the deployment's operator can reach, through which the web UI's Queue window can list, approve, reject, kill, EXPLAIN and preview queries.

Approving a query makes the scheduler run it with the database's admin login, so the door is narrow on purpose:

- Where: `<jobs_dir>/control/door.sock`. The folder belongs to the owner of `jobs_dir` (on the host: the operator who made `data/jobs`), mode 0700; the socket 0600. Both are set explicitly, so the process's umask (0 in the container, which the job folders rely on) is never changed and never matters. The door stays closed when `jobs_dir` belongs to root (nobody but root could use it) or others may write into it (anyone could then move the control folder away; group write is accepted), when the results folder (which the database servers can write) is `jobs_dir`, contains it (compared by resolved path, by device and inode of `jobs_dir` and each folder above it, and looked for by device and inode up to three levels below the results folder, since in the container the two are separate mounts) or lies inside the control folder, when the control folder is a link, a file, belongs to another user or is open to the group or others, and when something other than a socket is in the socket's place. The folder is never deleted or made anew: a bind mount of it would go on pointing at the old one.
- Who: a connection from any uid but the folder's owner (read from the kernel with `SO_PEERCRED`) is closed without a reply and counted in the log.
- How much: at most 16 connections at a time (the rest closed at once); one request and one reply per connection, each a `control.framing` frame; a request of at most 64 KiB, arriving within ten seconds; a reply of at most 4 MiB (else `too_large`). A reply is `{"ok": true, "result": ...}` or `{"ok": false, "error": {"code": ..., "message": <constant text>}}` (`not_pending` adds `phase`, `explain_failed` adds `detail`, the database's error text). An unexpected exception is logged by the name of its kind only and answered `internal`; no reply and no log line carries a traceback.
- On which thread: every store and runner call of the door runs on the scheduler's own event loop (the thread on which the page's routes run), handed over with `loop.call_soon_threadsafe`, one call at a time for the whole door (a request that cannot have its turn within its own ten seconds is answered `busy`), and waited for with a time limit; the store's transitions take no lock, so this is what keeps the door's changes from meeting the page's. The one exception is EXPLAIN, which blocks on the database and only reads the store (`Runner.explain`: one `Store.find`, then the backend): it runs in a thread of its own, at most four at a time, for at most 60 seconds. A request the loop has not started within its limit is withdrawn and answered `timeout` (nothing was done); one it started and did not finish is answered `outcome_unknown`. Every approve, reject, kill, EXPLAIN and auto-accept change is logged twice: the attempt (op, job id or target, who) before the hand-over, and its outcome (`ok` or the code) after it.

The requests (`op`; every listed field is required, nothing else is taken; a job id must be `(pg|sr|mg|job)_[0-9a-f]{8}` before it is used for anything; `by` is a label `web-` followed by 1 to 12 digits; a target must be a configured one):

- `hello` -> `{"protocol": 1}`
- `counts` -> `{"pending", "approved", "running"}`, the numbers on the page's tabs
- `targets` -> `{"targets": [{"name", "backend", "database"}]}`
- `jobs` `{phase, limit, offset}` (phase `pending`, `approved`, `running` or `history`; limit 1 to 500; offset 0 to 100,000, the number of jobs to skip) -> `{"phase", "jobs": [<short view>], "more"}` (`more`: jobs follow after this page), in the page's order: the three queues oldest first (submission or approval order), the history newest first. The short view cuts `sql`, `description`, `error` and `reject_reason` to 2,000 characters and gives their full lengths as `<name>_length`; other text is cut to 256 characters. A job of the chat's assistant adds `"submitted_by": "assistant"` (left out for other jobs, as in the record).
- `job` `{job_id}` -> `{"phase", "job": <the whole record>, "digest"}` (`too_large` for a record of more than 1 MiB as JSON). `digest` is the SHA-256, in hex, of the JSON of exactly `{target, backend, sql, output_filename, overwrite, collection, database}` with sorted keys, separators `,` and `:` and ASCII only.
- `approve` `{job_id, digest, by}` -> `{}`; `not_pending` (with `phase`) unless pending; `changed` when the digest of the record as it is now differs or the record names another job id; `by` is written into the record in the same write as the approval.
- `reject` `{job_id, reason, by}` -> `{}`; the reason at most 500 characters with no line break or control character (Unicode categories Cc, Zl, Zp), empty for none; `not_pending` and `changed` (another job id in the record) as above.
- `kill` `{job_id, by}` -> `{"was_running"}`. `by` is recorded as who asked to kill (`acted_by["killed"]`): the job may still finish by itself, and the label is lost when the scheduler restarts before the job is retired.
- `explain` `{job_id, by}` -> `{"plan"}` (cut to 256 KiB); `explain_failed` with the database's error text in `detail` (cut to 4,000 characters), `explain_timeout` (the statement may still be running on the database), `busy` (four already running). EXPLAIN is sent to the database on an admin connection like the page's button; nothing in the reply says whether that was safe.
- `preview` `{job_id}` -> `{"format", "columns", "rows", "parts", "parts_total", "read_bytes", "truncated"}` for a job in the history with the status `done`: its folder is built from the checked id, never from the record's `result_path`; a folder of more than 10,000 entries is refused; only names that are the job's `output_filename` or `<stem>_<digits><ext>` and are regular files (opened without following links and without waiting) are taken; at most 1 MiB of the first part is read and at most 50 rows of 200 cells of 500 characters are returned, as text. Codes: `not_done`, `preview_not_available`, `preview_failed`, each with a constant text that holds nothing of a file.
- `auto_accept` -> `{"max_limit", "targets": [{"name", "backend", "database", "enabled", "limit"}]}`, as the page's panel shows it; `auto_accept.set` `{target, enabled, limit, by}` (`enabled` true, false or null, `limit` a whole number or null; null leaves it as it is; the limit is clamped like the page's) -> the same.

Standard library, `control.framing` and the scheduler's own store only: no FastAPI, no uvicorn, no driver; `pyarrow` is imported inside the Parquet preview, as the page does. Linux only (`SO_PEERCRED`, `/proc/self/fd`).
"""

from __future__ import annotations

import concurrent.futures
import contextlib
import csv
import errno
import hashlib
import hmac
import io
import json
import logging
import math
import os
import re
import selectors
import socket
import stat
import struct
import threading
import time
import unicodedata
from dataclasses import dataclass

from social_data_pipeline.control import framing

log = logging.getLogger(__name__)

PROTOCOL = 1
FOLDER_NAME = "control"
SOCKET_NAME = "door.sock"
FOLDER_MODE = 0o700
SOCKET_MODE = 0o600

REQUEST_MAX_BYTES = 64 * 1024
REPLY_MAX_BYTES = 4 * 1024 * 1024
JOB_MAX_BYTES = 1024 * 1024
JOBS_LIMIT_MAX = 500
# A history page far back costs the loop as much as its offset: the store reads the history file whole and builds a job for every line up to offset + limit. The door's one-call-at-a-time rule keeps such requests from piling up.
JOBS_OFFSET_MAX = 100000
TEXT_CUT = 2000
OTHER_TEXT_CUT = 256
REASON_MAX = 500
PLAN_MAX_BYTES = 256 * 1024
EXPLAIN_ERROR_CUT = 4000
PREVIEW_READ_MAX = 1024 * 1024
PREVIEW_ROWS = 50
PREVIEW_COLUMNS = 200
PREVIEW_CELL = 500
PREVIEW_PARTS_MAX = 1000
# The most entries of a result folder that are listed for a preview.
PREVIEW_LISTING_MAX = 10000
# How far below the results folder, and through how many entries, the data folder is looked for.
RESULTS_SCAN_DEPTH = 3
RESULTS_SCAN_ENTRIES = 10000
# The largest uncompressed size of the first row group of a Parquet file that is previewed.
PARQUET_ROW_GROUP_MAX = 64 * 1024 * 1024

JOB_ID = re.compile(r"(pg|sr|mg|job)_[0-9a-f]{8}")
BY_LABEL = re.compile(r"web-[0-9]{1,12}")
DIGEST = re.compile(r"[0-9a-f]{64}")
PHASES = ("pending", "approved", "running", "history")
DIGEST_FIELDS = ("target", "backend", "sql", "output_filename", "overwrite", "collection", "database")
SHORT_TEXT_FIELDS = ("sql", "description", "error", "reject_reason")
SHORT_FIELDS = ("job_id", "target", "backend", "status", "submitted_at", "approved_at", "started_at", "finished_at", "rows", "size_bytes", "output_filename", "overwrite")

# The fields of each op besides `op`; all of them are required.
OPS = {
    "hello": frozenset(),
    "counts": frozenset(),
    "targets": frozenset(),
    "jobs": frozenset({"phase", "limit", "offset"}),
    "job": frozenset({"job_id"}),
    "approve": frozenset({"job_id", "digest", "by"}),
    "reject": frozenset({"job_id", "reason", "by"}),
    "kill": frozenset({"job_id", "by"}),
    "explain": frozenset({"job_id", "by"}),
    "preview": frozenset({"job_id"}),
    "auto_accept": frozenset(),
    "auto_accept.set": frozenset({"target", "enabled", "limit", "by"}),
}

MESSAGES = {
    "bad_request": "the request is not one the door takes",
    "request_too_large": "the request is larger than a request may be",
    "unknown_target": "the target is not one of the configured targets",
    "not_found": "there is no job with this id",
    "not_pending": "the job is not pending",
    "changed": "the job changed since it was read: read it again",
    "too_large": "the answer is too large to send",
    "explain_failed": "the database could not explain the query",
    "explain_timeout": "the database did not answer the EXPLAIN in time; it may still be running there",
    "busy": "the door is busy with other requests: try again later",
    "not_done": "the job has no finished result to preview",
    "preview_not_available": "this result cannot be previewed here",
    "preview_failed": "the result could not be previewed",
    "timeout": "the scheduler did not take up the request in time; nothing was done",
    "outcome_unknown": "the scheduler did not finish the request in time; it may still take effect",
    "unavailable": "the scheduler is stopping",
    "internal": "the door could not answer this request",
}

# Why the door stays closed: one constant line each, logged once by `open_door`.
CLOSED_DATA_FOLDER = "the control socket stays closed: the data folder cannot be opened"
CLOSED_ROOT_OWNER = "the control socket stays closed: the data folder belongs to root, so nobody but root could use it; give the data folder to the operator's user"
CLOSED_OTHERS_WRITE = "the control socket stays closed: others may write into the data folder, so anyone could move the control folder away; take that permission away (chmod o-w)"
CLOSED_RESULTS_UNCHECKED = "the control socket stays closed: the results folder cannot be looked at"
CLOSED_RESULTS_CONTAIN = "the control socket stays closed: the results folder is the data folder or contains it, and the database servers can write there"
CLOSED_RESULTS_INSIDE = "the control socket stays closed: the results folder lies inside the control folder, and the database servers can write there"
CLOSED_NOT_A_FOLDER = "the control socket stays closed: the control folder is not a real folder (a link or a file is in its place)"
CLOSED_OTHER_OWNER = "the control socket stays closed: the control folder belongs to another user than the data folder"
CLOSED_OPEN_FOLDER = "the control socket stays closed: the control folder is open to the group or to others"
CLOSED_NOT_MADE = "the control socket stays closed: the control folder could not be made or given to the data folder's owner"
CLOSED_FOLDER_UNUSABLE = "the control socket stays closed: the control folder cannot be used"
CLOSED_LEFTOVER = "the control socket stays closed: something other than a socket is in the place of door.sock"
CLOSED_SOCKET = "the control socket stays closed: the socket could not be made"
CLOSED_THREAD = "the control socket stays closed: its thread could not be started"

_SILENT_FRAME_ERRORS = (framing.FrameTimeout, framing.FrameHeaderIncomplete, framing.FrameIncomplete, framing.FrameReadFailed)


@dataclass(frozen=True)
class Limits:
    """The limits and times of a door. The defaults are the ones it runs with; a test passes smaller ones."""

    connections_max: int = 16
    request_seconds: float = 10
    send_seconds: float = 30
    loop_seconds: float = 30
    explain_seconds: float = 60
    explains_max: int = 4


class _Refusal(Exception):
    """A request answered with an error: the reply."""

    def __init__(self, code, **extra):
        super().__init__(code)
        self.code = code
        self.reply = _error(code, **extra)


class _Closed(Exception):
    """The door stays closed; `text` is the line logged for it."""

    def __init__(self, text):
        super().__init__(text)
        self.text = text


def _ok(result):
    return {"ok": True, "result": result}


def _error(code, **extra):
    return {"ok": False, "error": {"code": code, "message": MESSAGES[code], **extra}}


def _peer_uid(connection):
    """The uid of the process at the other end of a unix socket, as the kernel recorded it when the connection was made."""
    data = connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
    return struct.unpack("3i", data)[1]


# ── checks of the request's fields ─────────────────────────────────────────


def _has_line_break_or_control(text):
    return any(unicodedata.category(character) in ("Cc", "Zl", "Zp") for character in text)


def _job_id(request):
    value = request["job_id"]
    if not isinstance(value, str) or JOB_ID.fullmatch(value) is None:
        raise _Refusal("bad_request")
    return value


def _by(request):
    value = request["by"]
    if not isinstance(value, str) or BY_LABEL.fullmatch(value) is None:
        raise _Refusal("bad_request")
    return value


def _whole_number(value):
    return isinstance(value, int) and not isinstance(value, bool)


# ── what a reply holds ─────────────────────────────────────────────────────


def _clean_text(text):
    """Text that can be written as UTF-8: a lone surrogate (which a JSON record may hold) becomes "?"."""
    return text.encode("utf-8", "replace").decode("utf-8")


def _clean(value):
    """A copy of a JSON value whose every string can be written as UTF-8 and whose every number is finite: `NaN`, `Infinity` and a number too large for a float (which a hand-edited record may hold) become None, since a frame cannot hold them."""
    if isinstance(value, str):
        return _clean_text(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {_clean_text(str(key)): _clean(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean(item) for item in value]
    return value


def _scalar(value, cut=OTHER_TEXT_CUT):
    """A value of the short view: text cut, numbers, booleans and None as they are, anything else (a number that is not finite included) None."""
    if isinstance(value, str):
        return _clean_text(value[:cut])
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return None


def short_view(job):
    """The short view of a job, for lists."""
    view = {name: _scalar(getattr(job, name, None)) for name in SHORT_FIELDS}
    for name in SHORT_TEXT_FIELDS:
        value = getattr(job, name, None)
        if isinstance(value, str):
            view[name] = _clean_text(value[:TEXT_CUT])
            view[f"{name}_length"] = len(value)
        else:
            view[name] = None
            view[f"{name}_length"] = 0
    submitted_by = getattr(job, "submitted_by", "")
    if isinstance(submitted_by, str) and submitted_by:
        view["submitted_by"] = _clean_text(submitted_by[:OTHER_TEXT_CUT])
    return view


def digest(job):
    """The SHA-256 (hex) of the canonical JSON of the seven fields that decide what an approved job runs, where and into which file."""
    fields = {name: getattr(job, name, None) for name in DIGEST_FIELDS}
    text = json.dumps(fields, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(text.encode("ascii")).hexdigest()


def _cut_bytes(text, limit):
    data = text.encode("utf-8", "replace")
    if len(data) <= limit:
        return _clean_text(text)
    return data[:limit].decode("utf-8", "ignore")


# ── opening ────────────────────────────────────────────────────────────────


def _is_inside(path, folder):
    return path == folder or path.startswith(folder.rstrip("/") + "/")


def _check_results(jobs_dir, jobs_fd, result_root):
    """Refuse a results folder that is the data folder or contains it (by resolved path and by device and inode of the data folder and each folder above it), or lies inside the control folder."""
    try:
        results_real = os.path.realpath(result_root)
        jobs_real = os.path.realpath(jobs_dir)
    except (OSError, ValueError):
        raise _Closed(CLOSED_RESULTS_UNCHECKED) from None
    if _is_inside(jobs_real, results_real):
        raise _Closed(CLOSED_RESULTS_CONTAIN)
    if _is_inside(results_real, os.path.join(jobs_real, FOLDER_NAME)):
        raise _Closed(CLOSED_RESULTS_INSIDE)
    try:
        results_info = os.stat(result_root)
    except (OSError, ValueError):
        raise _Closed(CLOSED_RESULTS_UNCHECKED) from None
    identity = (results_info.st_dev, results_info.st_ino)
    try:
        info = os.fstat(jobs_fd)
        if (info.st_dev, info.st_ino) == identity:
            raise _Closed(CLOSED_RESULTS_CONTAIN)
        if _found_below(result_root, (info.st_dev, info.st_ino)):
            raise _Closed(CLOSED_RESULTS_CONTAIN)
        here = jobs_real
        while True:
            parent = os.path.dirname(here)
            if parent == here:
                break
            info = os.stat(parent)
            if (info.st_dev, info.st_ino) == identity:
                raise _Closed(CLOSED_RESULTS_CONTAIN)
            here = parent
    except OSError:
        raise _Closed(CLOSED_RESULTS_UNCHECKED) from None


def _found_below(root, identity):
    """Whether a folder with the device and inode `identity` lies at most RESULTS_SCAN_DEPTH levels below `root`, links not followed. In the container the data folder and the results folder are two mounts, so a results folder on the host that contains the data folder shows no common path; this finds it. Entries named like job ids (the job folders) are neither entered nor counted. Raises _Closed (cannot be checked) when the folders below hold more than RESULTS_SCAN_ENTRIES other entries."""
    level = [os.fspath(root)]
    seen = 0
    for _depth in range(RESULTS_SCAN_DEPTH):
        below = []
        for folder in level:
            try:
                entries = os.scandir(folder)
            except OSError:
                continue
            with entries:
                for entry in entries:
                    # Job folders first: they are neither entered nor counted, so a results folder with many finished jobs never reaches the bound.
                    if JOB_ID.fullmatch(entry.name):
                        continue
                    seen += 1
                    if seen > RESULTS_SCAN_ENTRIES:
                        raise _Closed(CLOSED_RESULTS_UNCHECKED)
                    try:
                        if not entry.is_dir(follow_symlinks=False):
                            continue
                        info = entry.stat(follow_symlinks=False)
                    except OSError:
                        continue
                    if (info.st_dev, info.st_ino) == identity:
                        return True
                    below.append(entry.path)
        level = below
    return False


def _checked_control_folder(jobs_fd, uid):
    """A descriptor of the control folder, after it was found a real folder of `uid` with no permission for the group or others; the look by name and the opened folder must be the same folder."""
    try:
        named = os.stat(FOLDER_NAME, dir_fd=jobs_fd, follow_symlinks=False)
    except OSError:
        raise _Closed(CLOSED_FOLDER_UNUSABLE) from None
    if not stat.S_ISDIR(named.st_mode):
        raise _Closed(CLOSED_NOT_A_FOLDER)
    if named.st_uid != uid:
        raise _Closed(CLOSED_OTHER_OWNER)
    if named.st_mode & 0o077:
        raise _Closed(CLOSED_OPEN_FOLDER)
    try:
        fd = os.open(FOLDER_NAME, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=jobs_fd)
    except OSError as error:
        if error.errno in (errno.ENOTDIR, errno.ELOOP):
            raise _Closed(CLOSED_NOT_A_FOLDER) from None
        raise _Closed(CLOSED_FOLDER_UNUSABLE) from None
    try:
        opened = os.fstat(fd)
    except OSError:
        os.close(fd)
        raise _Closed(CLOSED_FOLDER_UNUSABLE) from None
    if (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino) or opened.st_uid != uid or opened.st_mode & 0o077 or not stat.S_ISDIR(opened.st_mode):
        os.close(fd)
        raise _Closed(CLOSED_FOLDER_UNUSABLE)
    return fd


def _control_folder(jobs_fd, uid, gid):
    """The control folder, made (0700, then given to the owner) when it is missing and checked in every case."""
    try:
        os.mkdir(FOLDER_NAME, FOLDER_MODE, dir_fd=jobs_fd)
    except FileExistsError:
        pass
    except OSError:
        raise _Closed(CLOSED_NOT_MADE) from None
    else:
        try:
            made = os.open(FOLDER_NAME, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=jobs_fd)
        except OSError:
            raise _Closed(CLOSED_NOT_MADE) from None
        try:
            # The umask can only have taken bits away from 0700; this puts them back without touching the umask.
            os.fchmod(made, FOLDER_MODE)
            os.fchown(made, uid, gid)
        except OSError:
            raise _Closed(CLOSED_NOT_MADE) from None
        finally:
            os.close(made)
    return _checked_control_folder(jobs_fd, uid)


def _bind(control_fd, uid, gid):
    """The listening socket, bound inside the control folder through its descriptor, then made 0600 and given to the owner. Returns (socket, its device and inode)."""
    try:
        leftover = os.stat(SOCKET_NAME, dir_fd=control_fd, follow_symlinks=False)
    except FileNotFoundError:
        leftover = None
    except OSError:
        raise _Closed(CLOSED_FOLDER_UNUSABLE) from None
    if leftover is not None:
        if not stat.S_ISSOCK(leftover.st_mode):
            raise _Closed(CLOSED_LEFTOVER)
        try:
            os.unlink(SOCKET_NAME, dir_fd=control_fd)
        except OSError:
            raise _Closed(CLOSED_SOCKET) from None
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        # Through the folder's descriptor: the path that is bound is the folder that was checked, however long its own path is.
        listener.bind(f"/proc/self/fd/{control_fd}/{SOCKET_NAME}")
        os.chmod(SOCKET_NAME, SOCKET_MODE, dir_fd=control_fd)
        os.chown(SOCKET_NAME, uid, gid, dir_fd=control_fd, follow_symlinks=False)
        info = os.stat(SOCKET_NAME, dir_fd=control_fd, follow_symlinks=False)
        listener.listen(32)
    except OSError:
        listener.close()
        with contextlib.suppress(OSError):
            os.unlink(SOCKET_NAME, dir_fd=control_fd)
        raise _Closed(CLOSED_SOCKET) from None
    return listener, (info.st_dev, info.st_ino)


def open_door(cfg, store, runner, auto_accept, loop, *, owner=None, peer_uid=None, limits=None, clock=time.monotonic):
    """Open the door for a running scheduler: an object with `close()`, or None after one log line that says why it stays closed. `loop` is the scheduler's event loop, on which every store and runner call is made. `owner` (uid, gid) replaces the owner of `jobs_dir` and `peer_uid(connection)` the kernel's answer, for tests."""
    limits = limits or Limits()
    jobs_fd = None
    control_fd = None
    try:
        try:
            jobs_fd = os.open(os.fspath(cfg.jobs_dir), os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
            info = os.fstat(jobs_fd)
        except OSError:
            raise _Closed(CLOSED_DATA_FOLDER) from None
        uid, gid = owner if owner is not None else (info.st_uid, info.st_gid)
        if uid == 0:
            raise _Closed(CLOSED_ROOT_OWNER)
        # Group write is accepted: a private group is the common default. Write for others is not.
        if info.st_mode & 0o002:
            raise _Closed(CLOSED_OTHERS_WRITE)
        _check_results(os.fspath(cfg.jobs_dir), jobs_fd, os.fspath(cfg.result_root))
        control_fd = _control_folder(jobs_fd, uid, gid)
        listener, identity = _bind(control_fd, uid, gid)
        try:
            door = Door(cfg, store, runner, auto_accept, loop, listener=listener, identity=identity, control_fd=control_fd, uid=uid, peer_uid=peer_uid or _peer_uid, limits=limits, clock=clock)
        except OSError:
            listener.close()
            with contextlib.suppress(OSError):
                os.unlink(SOCKET_NAME, dir_fd=control_fd)
            raise _Closed(CLOSED_SOCKET) from None
        control_fd = None
        try:
            door.start()
        except _Closed:
            door.close()
            raise
        return door
    except _Closed as closed:
        log.warning(closed.text)
        return None
    finally:
        if jobs_fd is not None:
            os.close(jobs_fd)
        if control_fd is not None:
            os.close(control_fd)


# ── the door ───────────────────────────────────────────────────────────────


class Door:
    """A listening door; made by `open_door`."""

    def __init__(self, cfg, store, runner, auto_accept, loop, *, listener, identity, control_fd, uid, peer_uid, limits, clock):
        self.cfg = cfg
        self.store = store
        self.runner = runner
        self.auto_accept = auto_accept
        self.loop = loop
        self.limits = limits
        self.uid = uid
        self.clock = clock
        self._peer_uid = peer_uid
        self._listener = listener
        self._identity = identity
        self._control_fd = control_fd
        self._lock = threading.Lock()
        self._connections = set()
        self._closed = False
        self._stopping = threading.Event()
        self._explains = threading.BoundedSemaphore(limits.explains_max)
        # One call handed to the event loop at a time, door-wide; a request waits its turn within its own deadline.
        self._loop_lock = threading.Lock()
        self._request = threading.local()
        self._foreign = 0
        self._over_limit = 0
        self._wake_read, self._wake_write = os.pipe2(os.O_CLOEXEC | os.O_NONBLOCK)
        self._acceptor = threading.Thread(target=self._accept_loop, name="jobs-door", daemon=True)

    @property
    def socket_path(self):
        return os.path.join(os.fspath(self.cfg.jobs_dir), FOLDER_NAME, SOCKET_NAME)

    def start(self):
        try:
            self._acceptor.start()
        except RuntimeError:
            raise _Closed(CLOSED_THREAD) from None
        log.info("the control socket listens in %s for uid %d", os.path.join(os.fspath(self.cfg.jobs_dir), FOLDER_NAME), self.uid)

    def close(self):
        """Stop taking connections and remove the socket (only the one this door bound). Connections being answered finish by themselves; nothing here waits on the event loop, since this is called from it."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self._stopping.set()
        with contextlib.suppress(OSError):
            os.write(self._wake_write, b"x")
        if self._acceptor.ident is not None:
            self._acceptor.join(5)
        self._listener.close()
        with contextlib.suppress(OSError):
            info = os.stat(SOCKET_NAME, dir_fd=self._control_fd, follow_symlinks=False)
            if (info.st_dev, info.st_ino) == self._identity:
                os.unlink(SOCKET_NAME, dir_fd=self._control_fd)
        for fd in (self._control_fd, self._wake_read, self._wake_write):
            with contextlib.suppress(OSError):
                os.close(fd)

    def connection_count(self):
        with self._lock:
            return len(self._connections)

    # ── connections ────────────────────────────────────────────────────────

    def _accept_loop(self):
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(self._listener, selectors.EVENT_READ)
                selector.register(self._wake_read, selectors.EVENT_READ)
                while not self._stopping.is_set():
                    for key, _ in selector.select():
                        if key.fileobj is self._listener and not self._stopping.is_set():
                            self._accept()
        except Exception as error:
            log.warning("the control socket stopped taking connections (%s)", type(error).__name__)

    def _accept(self):
        try:
            connection, _ = self._listener.accept()
        except OSError as error:
            if error.errno in (errno.EMFILE, errno.ENFILE, errno.ENOBUFS, errno.ENOMEM):
                self._stopping.wait(0.1)
            return
        thread = None
        with self._lock:
            if len(self._connections) < self.limits.connections_max:
                thread = threading.Thread(target=self._handle, args=(connection,), name="jobs-door-connection", daemon=True)
                self._connections.add(thread)
            else:
                self._over_limit += 1
                over = self._over_limit
        if thread is None:
            connection.close()
            if over & (over - 1) == 0:
                log.warning("the control socket closed a connection over its limit of %d (%d so far)", self.limits.connections_max, over)
            return
        try:
            thread.start()
        except RuntimeError:
            with self._lock:
                self._connections.discard(thread)
            connection.close()
            log.warning("the control socket closed a connection: no thread could be started for it")

    def _handle(self, connection):
        try:
            try:
                peer = self._peer_uid(connection)
            except OSError:
                peer = None
            if peer != self.uid:
                with self._lock:
                    self._foreign += 1
                    foreign = self._foreign
                if foreign & (foreign - 1) == 0:
                    log.warning("the control socket closed a connection from another user without a reply (%d so far)", foreign)
                return
            reply = self._read_and_answer(connection)
            if reply is not None:
                self._send(connection, reply)
        except Exception as error:
            log.warning("a request to the control socket failed (%s)", type(error).__name__)
            with contextlib.suppress(Exception):
                self._send(connection, _error("internal"))
        finally:
            with contextlib.suppress(OSError):
                connection.close()
            with self._lock:
                self._connections.discard(threading.current_thread())

    def _read_and_answer(self, connection):
        deadline = self.clock() + self.limits.request_seconds
        self._request.deadline = deadline

        def read(count):
            left = deadline - self.clock()
            if left <= 0:
                raise TimeoutError()
            connection.settimeout(left)
            return connection.recv(count)

        try:
            request = framing.read_frame(read, max_bytes=REQUEST_MAX_BYTES, deadline=deadline, now=self.clock)
        except framing.FrameTooLarge:
            return _error("request_too_large")
        except _SILENT_FRAME_ERRORS:
            return None
        except framing.FrameError:
            return _error("bad_request")
        try:
            return self.answer(request)
        except _Refusal as refusal:
            return refusal.reply

    def _send(self, connection, reply):
        try:
            frame = framing.encode(reply, max_bytes=REPLY_MAX_BYTES)
        except framing.FrameTooLarge:
            frame = framing.encode(_error("too_large"), max_bytes=REPLY_MAX_BYTES)
        except framing.FrameError:
            log.warning("a reply of the control socket could not be written as JSON")
            frame = framing.encode(_error("internal"), max_bytes=REPLY_MAX_BYTES)
        try:
            connection.settimeout(self.limits.send_seconds)
            connection.sendall(frame)
        except OSError as error:
            log.warning("a reply of the control socket could not be sent (%s)", type(error).__name__)

    # ── the event loop ─────────────────────────────────────────────────────

    def _on_loop(self, function, *args, **kwargs):
        """Run `function` on the scheduler's event loop and return what it returns (or raise what it raises). One call is handed over at a time for the whole door: a request that cannot have its turn before its own deadline is `busy`. A call the loop has not begun within the limit is withdrawn (`timeout`); one it began and did not end within a second limit is `outcome_unknown`. The turn is given back when the request is answered, also after `outcome_unknown`, while the loop may still be at that call: the loop is one thread, so no two calls run at once, and the next request waits behind the stuck one and is withdrawn in its turn; holding the turn instead would answer every request `busy` until the stuck call ended."""
        if self._stopping.is_set():
            raise _Refusal("unavailable")
        deadline = getattr(self._request, "deadline", None)
        wait = self.limits.request_seconds if deadline is None else max(0.0, deadline - self.clock())
        if not self._loop_lock.acquire(timeout=wait):
            raise _Refusal("busy")
        try:
            return self._hand_over(function, *args, **kwargs)
        finally:
            self._loop_lock.release()

    def _hand_over(self, function, *args, **kwargs):
        future = concurrent.futures.Future()

        def run():
            if not future.set_running_or_notify_cancel():
                return
            try:
                future.set_result(function(*args, **kwargs))
            except Exception as error:
                future.set_exception(error)
            except BaseException as error:
                future.set_exception(error)
                raise

        try:
            self.loop.call_soon_threadsafe(run)
        except RuntimeError:
            raise _Refusal("unavailable") from None
        try:
            return future.result(timeout=self.limits.loop_seconds)
        except concurrent.futures.TimeoutError:
            pass
        if future.cancel():
            raise _Refusal("timeout")
        try:
            return future.result(timeout=self.limits.loop_seconds)
        except concurrent.futures.TimeoutError:
            raise _Refusal("outcome_unknown") from None

    # ── requests ───────────────────────────────────────────────────────────

    def answer(self, request):
        """The reply to one decoded request. Raises _Refusal for a refusal."""
        op = request.get("op")
        if not isinstance(op, str) or op not in OPS:
            return _error("bad_request")
        if set(request) - {"op"} != OPS[op]:
            return _error("bad_request")
        return _ok(getattr(self, "_op_" + op.replace(".", "_"))(request))

    def _op_hello(self, request):
        return {"protocol": PROTOCOL}

    def _op_counts(self, request):
        store = self.store

        def counts():
            return {
                "pending": len(list(store.pending.glob("*.json"))),
                "approved": len(list(store.approved.glob("*.json"))),
                "running": len(list(store.running.glob("*.json"))),
            }

        return self._on_loop(counts)

    def _op_targets(self, request):
        return {"targets": [{"name": t.name, "backend": t.backend, "database": t.database} for t in self.cfg.targets.values()]}

    def _op_jobs(self, request):
        phase, limit, offset = request["phase"], request["limit"], request["offset"]
        if not isinstance(phase, str) or phase not in PHASES or not _whole_number(limit) or not 1 <= limit <= JOBS_LIMIT_MAX:
            raise _Refusal("bad_request")
        if not _whole_number(offset) or not 0 <= offset <= JOBS_OFFSET_MAX:
            raise _Refusal("bad_request")
        end = offset + limit
        if phase == "history":
            jobs = self._on_loop(self.store.iter_history, limit=end + 1)
        else:
            jobs = self._on_loop(self.store.list_phase, phase)
        return {"phase": phase, "jobs": [short_view(job) for job in jobs[offset:end]], "more": len(jobs) > end}

    def _find(self, job_id):
        located = self._on_loop(self.store.find, job_id)
        if located is None:
            raise _Refusal("not_found")
        return located

    def _op_job(self, request):
        job_id = _job_id(request)
        phase, job = self._find(job_id)
        record = _clean(job.to_dict())
        # Who approved, rejected or killed it, also when nobody did through this door (the record leaves the empty field out).
        record.setdefault("acted_by", {})
        if len(json.dumps(record, ensure_ascii=False).encode("utf-8")) > JOB_MAX_BYTES:
            raise _Refusal("too_large")
        return {"phase": phase, "job": record, "digest": digest(job)}

    def _op_approve(self, request):
        job_id = _job_id(request)
        wanted = request["digest"]
        if not isinstance(wanted, str) or DIGEST.fullmatch(wanted) is None:
            raise _Refusal("bad_request")
        by = _by(request)
        store = self.store

        def approve():
            located = store.find(job_id)
            if located is None:
                raise _Refusal("not_found")
            phase, job = located
            if phase != "pending":
                raise _Refusal("not_pending", phase=phase)
            # The store builds paths from the record's own id: a record that names another job is not the one that was read.
            if job.job_id != job_id or not hmac.compare_digest(digest(job), wanted):
                raise _Refusal("changed")
            try:
                store.approve(job_id, by=by)
            except KeyError:
                raise _Refusal("not_pending", phase=_phase_now(store, job_id)) from None

        self._acting("approve", job_id, by, lambda: self._on_loop(approve))
        return {}

    def _op_reject(self, request):
        job_id = _job_id(request)
        reason = request["reason"]
        if not isinstance(reason, str) or len(reason) > REASON_MAX or _has_line_break_or_control(reason):
            raise _Refusal("bad_request")
        by = _by(request)
        store = self.store

        def reject():
            located = store.find(job_id)
            if located is None:
                raise _Refusal("not_found")
            phase, job = located
            if phase != "pending":
                raise _Refusal("not_pending", phase=phase)
            if job.job_id != job_id:
                raise _Refusal("changed")
            try:
                store.reject(job_id, reason=(reason.strip() or None), by=by)
            except KeyError:
                raise _Refusal("not_pending", phase=_phase_now(store, job_id)) from None

        self._acting("reject", job_id, by, lambda: self._on_loop(reject))
        return {}

    def _op_kill(self, request):
        job_id = _job_id(request)
        by = _by(request)
        was_running = self._acting("kill", job_id, by, lambda: self._on_loop(self.runner.request_cancel, job_id, by=by), lambda running: "ok, was running" if running else "ok, was not running")
        return {"was_running": bool(was_running)}

    def _op_explain(self, request):
        job_id = _job_id(request)
        by = _by(request)
        return self._acting("explain", job_id, by, lambda: self._explain(job_id))

    def _explain(self, job_id):
        if not self._explains.acquire(blocking=False):
            raise _Refusal("busy")
        future = concurrent.futures.Future()

        def work():
            try:
                future.set_result(self.runner.explain(job_id))
            except BaseException as error:
                future.set_exception(error)
            finally:
                self._explains.release()

        try:
            threading.Thread(target=work, name="jobs-door-explain", daemon=True).start()
        except RuntimeError:
            self._explains.release()
            raise
        try:
            plan = future.result(timeout=self.limits.explain_seconds)
        except concurrent.futures.TimeoutError:
            raise _Refusal("explain_timeout") from None
        except KeyError:
            raise _Refusal("not_found") from None
        except Exception as error:
            # The database's error text is data for the operator, as the page shows it.
            raise _Refusal("explain_failed", detail=_clean_text(f"{type(error).__name__}: {error}"[:EXPLAIN_ERROR_CUT])) from None
        return {"plan": _cut_bytes(plan if isinstance(plan, str) else str(plan), PLAN_MAX_BYTES)}

    def _op_preview(self, request):
        job_id = _job_id(request)
        store = self.store

        def look():
            located = store.find(job_id)
            if located is None:
                return None
            # The folder from the checked id, never from the record's result_path.
            return located, store.job_result_dir(job_id)

        found = self._on_loop(look)
        if found is None:
            raise _Refusal("not_found")
        (phase, job), folder = found
        if phase != "history" or job.status != "done":
            raise _Refusal("not_done")
        return preview(folder, job.output_filename)

    def _auto_accept_view(self):
        state = self.auto_accept.get_state()
        rows = []
        for t in self.cfg.targets.values():
            settings = state.targets.get(t.name)
            rows.append({
                "name": t.name,
                "backend": t.backend,
                "database": t.database,
                "enabled": bool(settings.enabled) if settings else False,
                "limit": int(settings.limit) if settings else self.auto_accept.max_limit,
            })
        return {"max_limit": self.auto_accept.max_limit, "targets": rows}

    def _op_auto_accept(self, request):
        return self._on_loop(self._auto_accept_view)

    def _op_auto_accept_set(self, request):
        target, enabled, limit = request["target"], request["enabled"], request["limit"]
        if not isinstance(target, str):
            raise _Refusal("bad_request")
        if enabled is not None and not isinstance(enabled, bool):
            raise _Refusal("bad_request")
        if limit is not None and not _whole_number(limit):
            raise _Refusal("bad_request")
        by = _by(request)
        if target not in self.cfg.targets:
            raise _Refusal("unknown_target")

        def change():
            self.auto_accept.set_target(target, enabled=enabled, limit=limit)
            return self._auto_accept_view()

        return self._acting(f"auto_accept.set (enabled={enabled}, limit={limit})", target, by, lambda: self._on_loop(change))

    def _acting(self, op, subject, by, act, ok=lambda _result: "ok"):
        """Run `act` for a request that changes something or reaches the database, with a log line for the attempt before it and one for the outcome after it: `ok`, a refusal's code (`timeout`, `outcome_unknown` and `unavailable` included) or `internal` with the name of the exception's kind. The values logged are checked ones."""
        log.info("control socket: %s of %s asked by %s", op, subject, by)
        try:
            result = act()
        except _Refusal as refusal:
            log.info("control socket: %s of %s by %s: %s", op, subject, by, refusal.code)
            raise
        except Exception as error:
            log.warning("control socket: %s of %s by %s: internal (%s)", op, subject, by, type(error).__name__)
            raise
        log.info("control socket: %s of %s by %s: %s", op, subject, by, ok(result))
        return result


def _phase_now(store, job_id):
    located = store.find(job_id)
    return located[0] if located is not None else "gone"


# ── preview ────────────────────────────────────────────────────────────────


def _part_names(names, output_filename):
    """The names of the parts of a result, in order: the output file's own name first, then `<stem>_<digits><ext>` by number."""
    stem, ext = os.path.splitext(output_filename)
    numbered = re.compile(re.escape(stem) + r"_([0-9]{1,18})(?:" + re.escape(ext) + "|" + re.escape(ext.lower()) + ")")
    exact = [name for name in names if name == output_filename]
    parts = []
    for name in names:
        if name == output_filename:
            continue
        match = numbered.fullmatch(name)
        if match is not None:
            parts.append((int(match.group(1)), name))
    return exact + [name for _number, name in sorted(parts)]


def _read_at_most(fd, limit):
    data = bytearray()
    while len(data) < limit:
        chunk = os.read(fd, limit - len(data))
        if not chunk:
            break
        data += chunk
    return bytes(data)


def _cell(value):
    if isinstance(value, str):
        text = value
    elif value is None:
        text = ""
    elif isinstance(value, (bool, int, float, dict, list)):
        try:
            text = json.dumps(value, ensure_ascii=False, default=str)
        except (TypeError, ValueError, RecursionError):
            text = ""
    else:
        text = str(value)
    return _clean_text(text[:PREVIEW_CELL])


def _row(values):
    return [_cell(value) for value in list(values)[:PREVIEW_COLUMNS]]


def _whole_lines(data, truncated):
    """The text of what was read, without a last line that the limit cut."""
    if truncated:
        end = data.rfind(b"\n")
        if end >= 0:
            data = data[: end + 1]
    return data.decode("utf-8", "replace")


def _preview_csv(data, truncated):
    reader = csv.reader(io.StringIO(_whole_lines(data, truncated), newline=""))
    header = None
    rows = []
    for record in reader:
        if header is None:
            header = _row(record)
            continue
        if len(rows) >= PREVIEW_ROWS:
            break
        rows.append(_row(record))
    return header or [], rows


def _preview_ndjson(data, truncated):
    columns = []
    documents = []
    for line in _whole_lines(data, truncated).split("\n"):
        if len(documents) >= PREVIEW_ROWS:
            break
        line = line.strip()
        if not line:
            continue
        try:
            document = json.loads(line)
        except (ValueError, RecursionError):
            continue
        if not isinstance(document, dict):
            continue
        for key in document:
            if len(columns) < PREVIEW_COLUMNS and key not in columns:
                columns.append(key)
        documents.append(document)
    rows = [[_cell(document.get(key)) for key in columns] for document in documents]
    return [_cell(key) for key in columns], rows


def _preview_parquet(data, size):
    """A Parquet file is read whole (it is opened from its end), so only one of at most the read limit is previewed, and only when its first row group is not larger than PARQUET_ROW_GROUP_MAX uncompressed."""
    if size > PREVIEW_READ_MAX or len(data) != size:
        raise _Refusal("preview_not_available")
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        raise _Refusal("preview_not_available") from None
    try:
        parquet = pq.ParquetFile(pa.BufferReader(data))
        names = list(parquet.schema_arrow.names)[:PREVIEW_COLUMNS]
        metadata = parquet.metadata
        if metadata.num_row_groups == 0:
            return [_cell(name) for name in names], []
        if metadata.row_group(0).total_byte_size > PARQUET_ROW_GROUP_MAX:
            raise _Refusal("preview_not_available")
        batch = next(parquet.iter_batches(batch_size=PREVIEW_ROWS, row_groups=[0], columns=names), None)
        if batch is None:
            return [_cell(name) for name in names], []
        table = batch.to_pydict()
        count = min(PREVIEW_ROWS, batch.num_rows)
        return [_cell(name) for name in names], [[_cell(table[name][index]) for name in names] for index in range(count)]
    except _Refusal:
        raise
    except Exception:
        raise _Refusal("preview_failed") from None


def preview(folder, output_filename):
    """The preview of a finished job's result folder (see the module's docstring). Raises _Refusal."""
    if not isinstance(output_filename, str) or not output_filename or "/" in output_filename or "\0" in output_filename:
        raise _Refusal("preview_failed")
    kind = os.path.splitext(output_filename)[1].lower()
    if kind not in (".csv", ".ndjson", ".parquet"):
        raise _Refusal("preview_not_available")
    try:
        folder_fd = os.open(os.fspath(folder), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError:
        raise _Refusal("preview_failed") from None
    first = None
    parts = []
    try:
        try:
            listed = []
            with os.scandir(folder_fd) as entries:
                for entry in entries:
                    listed.append(entry.name)
                    if len(listed) > PREVIEW_LISTING_MAX:
                        raise _Refusal("preview_failed")
            names = _part_names(listed, output_filename)
        except OSError:
            raise _Refusal("preview_failed") from None
        for name in names[:PREVIEW_PARTS_MAX]:
            try:
                fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=folder_fd)
            except OSError:
                # A link (ELOOP), a part that went away, one this process may not open: not a part.
                continue
            try:
                info = os.fstat(fd)
            except OSError:
                os.close(fd)
                continue
            if not stat.S_ISREG(info.st_mode):
                os.close(fd)
                continue
            parts.append({"name": _clean_text(name), "size": info.st_size})
            if first is None:
                first = (fd, info.st_size)
            else:
                os.close(fd)
        if first is None:
            raise _Refusal("preview_failed")
        fd, size = first
        try:
            data = _read_at_most(fd, PREVIEW_READ_MAX)
        except OSError:
            raise _Refusal("preview_failed") from None
    finally:
        if first is not None:
            with contextlib.suppress(OSError):
                os.close(first[0])
        os.close(folder_fd)
    truncated = size > len(data)
    if kind == ".parquet":
        columns, rows = _preview_parquet(data, size)
    else:
        try:
            columns, rows = (_preview_csv if kind == ".csv" else _preview_ndjson)(data, truncated)
        except (csv.Error, ValueError, RecursionError):
            raise _Refusal("preview_failed") from None
    return {
        "format": kind[1:],
        "columns": columns,
        "rows": rows,
        "parts": parts,
        "parts_total": len(names),
        "read_bytes": len(data),
        "truncated": truncated,
    }
