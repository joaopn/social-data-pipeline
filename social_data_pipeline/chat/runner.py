"""The chat's runner: a small threaded server on a unix socket inside the sandbox, which keeps the chats in the work folder and runs their turns through a back end.

- Where: `SDP_CHAT_SOCKET` (default `/run/sdp-chat/runner.sock`), mode 0600; a leftover socket in its place is removed, anything else there refuses the start.
- How much: at most 8 connections at a time (the rest closed at once); one request and one reply per connection, each a `control.framing` frame; a request of at most 64 KiB within 10 seconds; a reply of at most 1 MiB (else `too_large`). A reply is `{"ok": true, "result": ...}` or `{"ok": false, "error": {"code", "message"}}` with a constant message. An unexpected exception is logged by the name of its kind only and answered `internal`.
- The chats: one folder per chat in the work folder (`SDP_CHAT_WORK`, default `/work`): `<id>/` with `home/` (the back end's home), `files/` (the assistant's working folder), `events.jsonl` and `meta.json` (`created`, `title`, `session`, `turns`, `resume`). A chat id is `c_` and 12 hex digits, checked before it is used for anything. At most 200 chats; at most 2 turns at a time in the sandbox, one per chat. Every file of a chat is opened relative to the folder above it without following a link.
- The requests (`op`; every listed field is required, nothing else is taken): `hello` -> `{"version", "backend", "databases", "queue"}` (`queue`: whether the query queue is attached); `chat.list` -> `{"chats": [{"id", "title", "created", "running"}]}`, newest first; `chat.new` -> `{"chat"}`; `chat.send {chat, text}` -> `{"offset"}` (the index of its `user` event; refused `busy`, `too_many`, `over_cap`, `chat_full`); `chat.events {chat, offset, limit}` -> `{"events", "next", "running"}`; `chat.stop {chat}` -> `{"stopping"}`; `chat.delete {chat}` -> `{}` (refused `busy` while a turn runs); `chat.files {chat}` -> `{"files": [{"name", "size", "mtime"}], "left_out", "more"}`; `chat.file {chat, name, offset, limit}` -> `{"data" (base64), "size", "next", "end"}`.
- The events (one JSON object per line, each at most 64 KiB, the log at most 16 MiB and 1 line per 32 bytes of that, then one `error` event says the chat is full and the turn's `end` follows): `user {text}`, `text {text}`, `tool {name, input}`, `tool_result {text, is_error}`, `file {name, size}`, `end {is_error, cost_usd, duration_ms, turns}`, `error {message}`; each has `n` (its index) and `t` (seconds since the epoch). Every turn ends with exactly one `end`.

Standard library and `control.framing`/`errors` only. The runner is not a security boundary: it keeps itself useful and bounded against what the sandbox may do to its folders.
"""

import array
import base64
import contextlib
import itertools
import errno
import json
import os
import re
import selectors
import socket
import stat
import sys
import threading
import time
import uuid
from dataclasses import dataclass

from social_data_pipeline.control import framing
from social_data_pipeline.control.errors import ControlError

PROTOCOL = 1
SOCKET_DEFAULT = "/run/sdp-chat/runner.sock"
WORK_DEFAULT = "/work"
TMP_DEFAULT = "/tmp"
SOCKET_MODE = 0o600
FOLDER_MODE = 0o700
CHAT_ID = re.compile(r"c_[0-9a-f]{12}")
FILE_NAME = re.compile(r"[A-Za-z0-9._ -]{1,128}")
EVENTS_NAME = "events.jsonl"
META_NAME = "meta.json"
META_MAX_BYTES = 64 * 1024
# Room kept at the end of a chat's log for the event that says it is full and the turn's `end`.
LOG_RESERVE = 1024
# The most lines a chat's log may hold is its byte limit divided by this (an event is longer), so its index stays small.
LINE_BYTES_MIN = 32
# The most names read from a folder at a time while it is removed; how often a folder whose listing was complete may be listed again because entries came meanwhile; how many entries one removal takes at most. Beyond either it stops ("kept changing").
REMOVE_BATCH = 10000
REMOVE_PASSES = 3
REMOVE_ENTRIES_MAX = 1_000_000
# The renamed-away folders the sweep removes: a deleted chat's under the work root, an old scratch folder under the tmp root. They never match `CHAT_ID`, so nothing else lists them.
CHAT_OLD = re.compile(r"c_[0-9a-f]{12}\.old-[0-9a-f]{8}")
SCRATCH_OLD = re.compile(r"sdp-chat-c_[0-9a-f]{12}\.old-[0-9a-f]{8}")
SCRATCH = re.compile(r"sdp-chat-(c_[0-9a-f]{12})")
_DIRECTORY = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC

OPS = {
    "hello": frozenset(),
    "chat.list": frozenset(),
    "chat.new": frozenset(),
    "chat.send": frozenset({"chat", "text"}),
    "chat.events": frozenset({"chat", "offset", "limit"}),
    "chat.stop": frozenset({"chat"}),
    "chat.delete": frozenset({"chat"}),
    "chat.files": frozenset({"chat"}),
    "chat.file": frozenset({"chat", "name", "offset", "limit"}),
}

MESSAGES = {
    "bad_request": "the request is not one the runner takes",
    "request_too_large": "the request is larger than a request may be",
    "not_found": "there is no chat with this id",
    "no_such_file": "there is no such file in the chat's folder",
    "busy": "a turn of this chat is running",
    "too_many": "two turns are running in the sandbox already: try again when one has ended",
    "over_cap": "the work folder is over its soft cap: delete chats to make room",
    "chat_full": "the chat's log is full: start a new chat",
    "too_many_chats": "there are 200 chats already: delete one first",
    "too_large": "the answer is too large to send",
    "not_usable": "the chat's folder cannot be used",
    "unavailable": "the runner is stopping",
    "internal": "the runner could not answer this request",
}

STOPPED = "the turn was stopped"
LIMIT = "the turn took longer than the limit"
CHAT_FULL = "the chat is full"
NOT_STARTED = "the back end could not be started"
UNREADABLE = "this event could not be read"
_SILENT_FRAME_ERRORS = (framing.FrameTimeout, framing.FrameHeaderIncomplete, framing.FrameIncomplete, framing.FrameReadFailed)


def ended_with(code):
    return f"the back end ended with code {int(code)}"


@dataclass(frozen=True)
class Limits:
    """The runner's limits. The defaults are the ones it runs with; a test passes smaller ones."""

    connections_max: int = 8
    request_bytes: int = 64 * 1024
    request_seconds: float = 10.0
    send_seconds: float = 30.0
    reply_bytes: int = 1024 * 1024
    text_max: int = 32000
    title_max: int = 80
    chats_max: int = 200
    turns_max: int = 2
    events_limit_max: int = 200
    events_bytes: int = 512 * 1024
    event_bytes: int = 64 * 1024
    log_bytes: int = 16 * 1024 * 1024
    files_max: int = 500
    listing_max: int = 10000
    piece_bytes: int = 512 * 1024
    turn_seconds: float = 1800.0
    walk_limit: int = 1_000_000


class _Refusal(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.code = code


def _ok(result):
    return {"ok": True, "result": result}


def _error(code):
    return {"ok": False, "error": {"code": code, "message": MESSAGES[code]}}


def _log(line):
    sys.stderr.write(f"runner: {line}\n")
    sys.stderr.flush()


# ── files ─────────────────────────────────────────────────────────────────


def _open_folder(parent_fd, name, *, create=False):
    if create:
        os.mkdir(name, FOLDER_MODE, dir_fd=parent_fd)
    return os.open(name, _DIRECTORY, dir_fd=parent_fd)


def _open_regular(parent_fd, name, flags, mode=0o600):
    fd = os.open(name, flags | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, mode, dir_fd=parent_fd)
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        raise OSError(errno.EINVAL, "not a regular file")
    return fd


def _write_all(fd, data):
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view):]


def _read_at_most(fd, limit, offset=0):
    parts, size = [], 0
    while size < limit:
        chunk = os.pread(fd, min(limit - size, 1 << 20), offset + size)
        if not chunk:
            break
        parts.append(chunk)
        size += len(chunk)
    return b"".join(parts)


def _replace(parent_fd, name, data):
    temporary = f".{name}.{os.urandom(4).hex()}.tmp"
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=parent_fd)
    try:
        _write_all(fd, data)
    finally:
        os.close(fd)
    os.replace(temporary, name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)


def scan(fd, limit):
    """(at most `limit` names of the folder `fd`, whether it holds more), read through `os.scandir` with the limit applied while iterating: a huge folder is never listed whole."""
    with os.scandir(fd) as entries:
        names = [entry.name for entry in itertools.islice(entries, limit)]
        more = len(names) == limit and next(entries, None) is not None
    return names, more


def measure(folder_fd, limit):
    """(the sum of the sizes of the regular files under the folder `folder_fd`, True), or (the sum so far, False) when it holds more than `limit` entries or the tree moved under the walk. The approach of `chat_host.measure`: iterative, one descriptor at a time, each folder opened without following a link and checked to be the one looked at, the way back up `..` checked against the folder it came from."""
    fd = os.dup(folder_fd)
    total = seen = 0
    try:
        info = os.fstat(fd)
        first, more = scan(fd, limit)
        if more:
            return total, False
        seen = len(first)
        stack = [((info.st_dev, info.st_ino), first)]
        while stack:
            pending = stack[-1][1]
            if pending:
                name = pending.pop()
                try:
                    entry = os.stat(name, dir_fd=fd, follow_symlinks=False)
                except OSError:
                    continue
                if stat.S_ISREG(entry.st_mode):
                    total += entry.st_size
                if not stat.S_ISDIR(entry.st_mode):
                    continue
                try:
                    child = os.open(name, _DIRECTORY, dir_fd=fd)
                except OSError:
                    continue
                opened = os.fstat(child)
                if (opened.st_dev, opened.st_ino) != (entry.st_dev, entry.st_ino):
                    os.close(child)
                    continue
                try:
                    listing, more = scan(child, limit - seen)
                except OSError:
                    os.close(child)
                    continue
                if more:
                    os.close(child)
                    return total, False
                seen += len(listing)
                os.close(fd)
                fd = child
                stack.append(((entry.st_dev, entry.st_ino), listing))
                continue
            stack.pop()
            if not stack:
                break
            up = os.open("..", _DIRECTORY, dir_fd=fd)
            os.close(fd)
            fd = up
            if (os.fstat(fd).st_dev, os.fstat(fd).st_ino) != stack[-1][0]:
                return total, False
    finally:
        os.close(fd)
    return total, True


class KeptChanging(OSError):
    """A removal stopped at its bound: entries kept coming while it ran."""


def remove_tree(parent_fd, name, *, passes=REMOVE_PASSES, entries_max=REMOVE_ENTRIES_MAX):
    """Remove `name` of the folder `parent_fd` and everything under it without recursion and without following a link: a link or any other non-folder is unlinked and never opened; each folder is made 0700, opened without following a link and checked to be the one looked at; the way back up is `..`, checked against the folder it came from. An entry that vanishes while it is looked at is skipped (a name made again shows up in the next listing). Bounded: a folder is listed in batches; one whose listing was complete and that holds entries again, or whose removal finds it not empty, is listed again at most `passes` times, and at most `entries_max` entries are removed in all; beyond that it raises KeptChanging. FileNotFoundError means only that `name` itself was not there; any other failure, or the tree moving, raises another OSError."""
    top = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    if not stat.S_ISDIR(top.st_mode):
        os.unlink(name, dir_fd=parent_fd)
        return
    budget = [entries_max]
    for _ in range(passes + 1):
        try:
            os.chmod(name, FOLDER_MODE, dir_fd=parent_fd, follow_symlinks=False)
            fd = os.open(name, _DIRECTORY, dir_fd=parent_fd)
        except FileNotFoundError:
            return
        try:
            _empty(fd, (top.st_dev, top.st_ino), passes, budget)
        except FileNotFoundError:
            raise OSError(errno.ESTALE, "the tree moved") from None
        try:
            os.rmdir(name, dir_fd=parent_fd)
            return
        except FileNotFoundError:
            return
        except OSError as error:
            if error.errno != errno.ENOTEMPTY:
                raise
    raise KeptChanging(errno.EBUSY, "entries kept coming")


def _empty(fd, identity, passes, budget):
    """Empty the folder `fd` (whose device and inode are `identity`) for `remove_tree`, and close `fd`."""
    try:
        if (os.fstat(fd).st_dev, os.fstat(fd).st_ino) != identity:
            raise OSError(errno.ESTALE, "the folder changed")

        def frame(identity, folder_name, relists=0):
            names, more = scan(fd, REMOVE_BATCH)
            # [identity, name, names still to look at, whether the last listing was cut at the batch, times listed again after a complete listing]
            return [identity, folder_name, names, more, relists]

        stack = [frame(identity, None)]
        while stack:
            current = stack[-1]
            if not current[2]:
                names, more = scan(fd, REMOVE_BATCH)
                if names and not current[3]:
                    current[4] += 1
                    if current[4] > passes:
                        raise KeptChanging(errno.EBUSY, "entries kept coming")
                current[2], current[3] = names, more
            if current[2]:
                entry = current[2].pop()
                budget[0] -= 1
                if budget[0] < 0:
                    raise KeptChanging(errno.EBUSY, "too many entries")
                try:
                    info = os.stat(entry, dir_fd=fd, follow_symlinks=False)
                    if not stat.S_ISDIR(info.st_mode):
                        os.unlink(entry, dir_fd=fd)
                        continue
                    os.chmod(entry, FOLDER_MODE, dir_fd=fd, follow_symlinks=False)
                    child = os.open(entry, _DIRECTORY, dir_fd=fd)
                except FileNotFoundError:
                    # It went meanwhile, which is what was wanted; it does not end the removal.
                    continue
                if (os.fstat(child).st_dev, os.fstat(child).st_ino) != (info.st_dev, info.st_ino):
                    os.close(child)
                    raise OSError(errno.ESTALE, "the folder changed")
                os.close(fd)
                fd = child
                stack.append(frame((info.st_dev, info.st_ino), entry))
                continue
            done = stack.pop()
            if not stack:
                break
            up = os.open("..", _DIRECTORY, dir_fd=fd)
            os.close(fd)
            fd = up
            if (os.fstat(fd).st_dev, os.fstat(fd).st_ino) != stack[-1][0]:
                raise OSError(errno.ESTALE, "the tree moved")
            try:
                os.rmdir(done[1], dir_fd=fd)
            except FileNotFoundError:
                pass
            except OSError as error:
                if error.errno != errno.ENOTEMPTY:
                    raise
                # Entries came meanwhile: the folder is listed again, as one of its passes.
                if done[4] + 1 > passes:
                    raise KeptChanging(errno.EBUSY, "entries kept coming") from None
                try:
                    child = os.open(done[1], _DIRECTORY, dir_fd=fd)
                except FileNotFoundError:
                    continue
                if (os.fstat(child).st_dev, os.fstat(child).st_ino) != done[0]:
                    os.close(child)
                    raise OSError(errno.ESTALE, "the folder changed") from None
                os.close(fd)
                fd = child
                stack.append(frame(done[0], done[1], done[4] + 1))
    finally:
        os.close(fd)


def listing(files_fd, limits):
    """The regular files directly in the folder `files_fd` whose names are plain: ({name: stat}, how many names were left out, whether more entries were there than looked at). Links and anything else that is not a regular file are never listed."""
    found, left_out, more = {}, 0, False
    with os.scandir(files_fd) as entries:
        for count, entry in enumerate(entries):
            if count >= limits.listing_max:
                more = True
                break
            name = entry.name
            if not FILE_NAME.fullmatch(name) or name in (".", ".."):
                left_out += 1
                continue
            try:
                info = os.stat(name, dir_fd=files_fd, follow_symlinks=False)
            except OSError:
                continue
            if stat.S_ISREG(info.st_mode):
                found[name] = info
    return found, left_out, more


# ── the chats ─────────────────────────────────────────────────────────────


class _Chat:
    def __init__(self, chat_id, meta):
        self.id = chat_id
        self.created = meta.get("created", 0)
        self.title = meta.get("title", "")
        # A chat without a valid session id gets a new one, started afresh.
        self.session = meta.get("session") or str(uuid.uuid4())
        self.turns = meta.get("turns", 0)
        self.resume = meta.get("resume") is True and "session" in meta
        self.lock = threading.Lock()
        self.offsets = None
        self.size = 0
        self.full = False
        # The log's last line has no line end (a write that failed half-way, or someone else's): the next event starts on a new line.
        self.partial = False
        self.running = False
        self.process = None
        self.stop_asked = False
        self.thread = None
        # The running turn's token: events from a reader whose turn is over (its token no longer this one) are dropped.
        self.token = None

    def meta(self):
        return {"created": self.created, "title": self.title, "session": self.session, "turns": self.turns, "resume": self.resume}


def _encodable(document):
    """Whether the document can be sent as a frame: finite numbers only and text that is UTF-8 (a lone surrogate escape decodes, and cannot be written)."""
    try:
        json.dumps(document, ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (ValueError, TypeError, UnicodeEncodeError, RecursionError):
        return False
    return True


def _checked_meta(data, title_max):
    """The fields of a `meta.json` that have the right types; others are dropped (a title that cannot be sent too)."""
    try:
        doc = framing.decode(data)
    except ControlError:
        return {}
    meta = {}
    if isinstance(doc.get("created"), (int, float)) and not isinstance(doc.get("created"), bool):
        meta["created"] = doc["created"]
    if isinstance(doc.get("title"), str) and _encodable(doc["title"]):
        meta["title"] = doc["title"][:title_max]
    if isinstance(doc.get("session"), str):
        with contextlib.suppress(ValueError):
            meta["session"] = str(uuid.UUID(doc["session"]))
    if isinstance(doc.get("turns"), int) and not isinstance(doc.get("turns"), bool):
        meta["turns"] = doc["turns"]
    if doc.get("resume") is True:
        meta["resume"] = True
    return meta


class Runner:
    """The chats and their turns. `backend` is the back end object (`claude.ClaudeCode` or `openai.OpenAICompatible`), `backend_name` its name, `databases` the names of the attached databases, `cap_bytes` the work folder's soft cap, `queue` whether the query queue is attached (for `hello`)."""

    def __init__(self, work, backend, backend_name, databases, cap_bytes, *, queue=False, tmp=TMP_DEFAULT, limits=None):
        self.limits = limits or Limits()
        self.work = os.path.abspath(work)
        self.tmp = tmp
        self.backend = backend
        self.backend_name = backend_name
        self.databases = list(databases)
        self.queue = bool(queue)
        self.cap_bytes = cap_bytes
        self.work_fd = os.open(self.work, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        self._lock = threading.Lock()
        self._chats = {}
        self._running = 0
        self._stopping = False
        self._sweeper = None
        # Whether the sweep thread runs, and whether one more pass was asked for while it ran (at most one is queued).
        self._sweeping = False
        self._sweep_again = False
        # One sweep at a time: two removals of the same folder each take the other's progress for "gone" and return early.
        self._sweep_lock = threading.Lock()
        self._load()
        self._sweep_later()

    def close(self):
        """Stop every running turn and wait for them (bounded), then close the work folder. A second call does nothing (its descriptor number may belong to something else by then)."""
        with self._lock:
            if self._stopping:
                return
            self._stopping = True
            chats = list(self._chats.values())
        for chat in chats:
            self._stop(chat)
        for chat in chats:
            if chat.thread is not None:
                chat.thread.join(60)
        if self._sweeper is not None:
            self._sweeper.join(60)
        with contextlib.suppress(OSError):
            os.close(self.work_fd)

    def _load(self):
        """The chats of the work folder: at most the chat limit, the newest by their folder's time; the folder is read through `scan`."""
        found = []
        with os.scandir(self.work_fd) as entries:
            for entry in itertools.islice(entries, self.limits.listing_max):
                if CHAT_ID.fullmatch(entry.name):
                    with contextlib.suppress(OSError):
                        info = entry.stat(follow_symlinks=False)
                        if stat.S_ISDIR(info.st_mode):
                            found.append((info.st_mtime_ns, entry.name))
        for _, name in sorted(found, reverse=True)[: self.limits.chats_max]:
            try:
                chat_fd = _open_folder(self.work_fd, name)
            except OSError:
                continue
            try:
                meta = {}
                with contextlib.suppress(OSError):
                    fd = _open_regular(chat_fd, META_NAME, os.O_RDONLY)
                    try:
                        meta = _checked_meta(_read_at_most(fd, META_MAX_BYTES), self.limits.title_max)
                    finally:
                        os.close(fd)
            finally:
                os.close(chat_fd)
            self._chats[name] = _Chat(name, meta)

    @contextlib.contextmanager
    def _chat_fd(self, chat):
        try:
            fd = _open_folder(self.work_fd, chat.id)
        except OSError:
            raise _Refusal("not_usable") from None
        try:
            yield fd
        finally:
            os.close(fd)

    def _save_meta(self, chat, chat_fd):
        _replace(chat_fd, META_NAME, (json.dumps(chat.meta(), ensure_ascii=False) + "\n").encode("utf-8"))

    # ── the log ───────────────────────────────────────────────────────────

    def _index(self, chat, chat_fd):
        """Build the chat's index of line starts once (under its lock)."""
        if chat.offsets is not None:
            return
        offsets, position = array.array("Q"), 0
        chat.partial = False
        max_lines = self.limits.log_bytes // LINE_BYTES_MIN
        try:
            fd = _open_regular(chat_fd, EVENTS_NAME, os.O_RDONLY)
        except FileNotFoundError:
            fd = None
        if fd is not None:
            try:
                size = os.fstat(fd).st_size
                at_start = True
                while position < min(size, self.limits.log_bytes) and len(offsets) <= max_lines:
                    chunk = os.pread(fd, 1 << 20, position)
                    if not chunk:
                        break
                    i = 0
                    while i < len(chunk):
                        if at_start:
                            if len(offsets) > max_lines:
                                break
                            offsets.append(position + i)
                        j = chunk.find(b"\n", i)
                        at_start = j >= 0
                        if j < 0:
                            break
                        i = j + 1
                    position += len(chunk)
                position = size
                chat.partial = size > 0 and os.pread(fd, 1, size - 1) != b"\n"
            finally:
                os.close(fd)
        chat.offsets, chat.size = offsets, position
        chat.full = position > self.limits.log_bytes - LOG_RESERVE or len(offsets) > max_lines - 2

    def _encode(self, event):
        """The line of an event, at most `event_bytes` (its text cut to fit). It never raises: a code point UTF-8 cannot hold becomes `?`, and an event that is not JSON (a number that is not finite) becomes the unreadable event."""

        def dumps(document):
            return (json.dumps(document, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8", "replace")

        try:
            line = dumps(event)
        except (ValueError, TypeError, RecursionError):
            event = {"n": event["n"], "t": event["t"], "type": "error", "message": UNREADABLE}
            line = dumps(event)
        field = "text" if "text" in event else "input" if "input" in event else None
        while len(line) > self.limits.event_bytes and field is not None and event[field]:
            event[field] = event[field][: len(event[field]) * 3 // 4]
            line = dumps(event)
        return line

    def _append(self, chat, kind, fields, *, last=False, token=None, ending=None):
        """Write one event to the chat's log; returns its index, or None when it was not written. When the log has no more room (bytes or lines), the event that says the chat is full is written instead, once, and the turn is stopped; a turn's `end` (`last`) is still written after it, in the room kept for it. After a failed write the index is built again from the file. With `token`, the event is dropped unless that turn is still the chat's running one; with `ending` (the turn's `end`), that turn's token is withdrawn under the same lock, so no event of its reader can land after its `end`."""
        stop, written = None, None
        with chat.lock:
            if token is not None and chat.token is not token:
                return None
            try:
                with self._chat_fd(chat) as chat_fd:
                    self._index(chat, chat_fd)
                    max_lines = self.limits.log_bytes // LINE_BYTES_MIN
                    lines = []
                    if not chat.full:
                        line = self._encode({"n": len(chat.offsets), "t": round(time.time(), 3), "type": kind, **fields})
                        if chat.size + len(line) <= self.limits.log_bytes - LOG_RESERVE and len(chat.offsets) + 1 <= max_lines - 2:
                            lines.append(line)
                            written = len(chat.offsets)
                            last = False
                        else:
                            lines.append(self._encode({"n": len(chat.offsets), "t": round(time.time(), 3), "type": "error", "message": CHAT_FULL}))
                            chat.full, stop = True, chat.process
                    if last:
                        n = len(chat.offsets) + len(lines)
                        line = self._encode({"n": n, "t": round(time.time(), 3), "type": kind, **fields})
                        if chat.size + sum(map(len, lines)) + len(line) <= self.limits.log_bytes and n + 1 <= max_lines:
                            lines.append(line)
                            written = n
                    if lines:
                        fd = _open_regular(chat_fd, EVENTS_NAME, os.O_WRONLY | os.O_APPEND | os.O_CREAT)
                        try:
                            for line in lines:
                                lead = b"\n" if chat.partial else b""
                                _write_all(fd, lead + line)
                                chat.offsets.append(chat.size + len(lead))
                                chat.size += len(lead) + len(line)
                                chat.partial = False
                        finally:
                            os.close(fd)
            except (OSError, _Refusal):
                _log("an event could not be written to a chat's log")
                chat.offsets, chat.size, chat.full = None, 0, False
                written = None
            finally:
                if ending is not None and chat.token is ending:
                    chat.token = None
        if stop is not None:
            stop.stop("full")
        return written

    def _read_events(self, chat, offset, limit):
        with chat.lock:
            with self._chat_fd(chat) as chat_fd:
                self._index(chat, chat_fd)
            total = len(chat.offsets)
            first = min(offset, total)
            ends = list(chat.offsets[first + 1: first + limit + 1]) + [chat.size]
            starts = list(chat.offsets[first: first + limit])
            running = chat.running
        events, used = [], 0
        if not starts:
            return {"events": [], "next": first, "running": running}
        with self._chat_fd(chat) as chat_fd:
            try:
                fd = _open_regular(chat_fd, EVENTS_NAME, os.O_RDONLY)
            except OSError:
                raise _Refusal("not_usable") from None
            try:
                for i, (start, end) in enumerate(zip(starts, ends)):
                    length = end - start
                    if events and used + min(length, self.limits.event_bytes) > self.limits.events_bytes:
                        break
                    used += min(length, self.limits.event_bytes)
                    event = None
                    if 0 < length <= self.limits.event_bytes:
                        raw = _read_at_most(fd, length, start).rstrip(b"\n")
                        try:
                            event = framing.decode(raw)
                        except ControlError:
                            event = None
                    events.append(event if isinstance(event, dict) and _encodable(event) else {"n": first + i, "t": None, "type": "error", "message": UNREADABLE})
            finally:
                os.close(fd)
        return {"events": events, "next": first + len(events), "running": running}

    # ── turns ─────────────────────────────────────────────────────────────

    def _snapshot(self, chat_fd):
        try:
            files_fd = _open_folder(chat_fd, "files")
        except OSError:
            return {}
        try:
            found, _, _ = listing(files_fd, self.limits)
        finally:
            os.close(files_fd)
        return {name: (info.st_size, info.st_mtime_ns, info.st_ino) for name, info in found.items()}

    def _turn(self, chat, text, token):
        started = time.monotonic()
        process = translator = None
        try:
            with self._chat_fd(chat) as chat_fd:
                before = self._snapshot(chat_fd)
                tmp = self._fresh_scratch(chat.id)
                process, translator = self.backend.turn(chat_fd, os.path.join(self.work, chat.id, "home"), os.path.join(tmp, "home"), os.path.join(self.work, chat.id, "files"), tmp, text, chat.session, chat.resume)
                with self._lock:
                    chat.process = process
                    if chat.stop_asked:
                        process.stop("stopped")
                timer = threading.Timer(self.limits.turn_seconds, process.stop, ("limit",))
                timer.daemon = True
                timer.start()

                def on_doc(doc):
                    # Called from the reader, which can outlive the turn: it uses only the turn's token, never the turn's descriptors.
                    if chat.token is not token:
                        return
                    for kind, fields in translator.events(doc):
                        self._append(chat, kind, fields, token=token)
                    if translator.init and not chat.resume:
                        chat.resume = True
                        with contextlib.suppress(OSError, _Refusal), self._chat_fd(chat) as meta_fd:
                            self._save_meta(chat, meta_fd)

                try:
                    process.run(on_doc)
                finally:
                    timer.cancel()
                if process.dropped:
                    _log(f"a turn dropped {process.dropped} lines of the back end's output that were not JSON objects")
                after = self._snapshot(chat_fd)
                for name in sorted(after):
                    if before.get(name) != after[name]:
                        self._append(chat, "file", {"name": name, "size": after[name][0]})
        except (OSError, _Refusal):
            if process is None or not process.started:
                self._append(chat, "error", {"message": NOT_STARTED})
        except Exception as error:
            _log(f"a turn failed ({type(error).__name__})")
            if process is None or not process.started:
                self._append(chat, "error", {"message": NOT_STARTED})
        finally:
            self._finish(chat, process, translator, started, token)

    def _fresh_scratch(self, chat_id):
        """The turn's scratch folder `<tmp>/sdp-chat-<id>` (its `TMPDIR`, with `home/`, the turn's `HOME`, and `matplotlib/`), new and empty for every turn, so that nothing a turn left there (a `.bashrc`, a `matplotlibrc`) shapes the next one. Whatever is in its place (a folder, a link, a file) is renamed away (a link itself, never followed) to `sdp-chat-<id>.old-<8 hex>` and removed afterwards by `sweep` on a thread of its own, so a leftover process that keeps writing into it cannot hold the turn; the new folder is made exclusively and its two folders through its own descriptor."""
        name = f"sdp-chat-{chat_id}"
        root_fd = os.open(self.tmp, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            with contextlib.suppress(FileNotFoundError):
                os.rename(name, f"{name}.old-{os.urandom(4).hex()}", src_dir_fd=root_fd, dst_dir_fd=root_fd)
            os.mkdir(name, FOLDER_MODE, dir_fd=root_fd)
            scratch_fd = _open_folder(root_fd, name)
            try:
                for inside in ("home", "matplotlib"):
                    os.mkdir(inside, FOLDER_MODE, dir_fd=scratch_fd)
            finally:
                os.close(scratch_fd)
        finally:
            os.close(root_fd)
        self._sweep_later()
        return os.path.join(self.tmp, name)

    def _sweep_later(self):
        """Run `sweep` on a thread of its own. While one runs, one more pass is queued (at most one), so a folder renamed away after that pass listed its roots is still removed."""
        with self._lock:
            if self._stopping:
                return
            if self._sweeping:
                self._sweep_again = True
                return
            self._sweeping, self._sweep_again = True, False
            self._sweeper = threading.Thread(target=self._sweep_loop, name="sweep", daemon=True)
            try:
                self._sweeper.start()
            except RuntimeError:
                self._sweeping = False

    def _sweep_loop(self):
        """The sweeper's passes. The decision to end and the clearing of `_sweeping` are one step under the lock a request takes, so a request can never see a sweeper that has already decided to end; an exception that ends the thread clears the flag as well."""
        ended = False
        try:
            while True:
                self.sweep()
                with self._lock:
                    if not self._sweep_again or self._stopping:
                        self._sweeping = False
                        ended = True
                        return
                    self._sweep_again = False
        finally:
            if not ended:
                with self._lock:
                    self._sweeping = False

    def sweep(self):
        """Remove the renamed-away folders: deleted chats (`c_<id>.old-<8 hex>`) under the work root and old scratch folders (`sdp-chat-<id>.old-<8 hex>`) under the tmp root, each by the bounded `remove_tree`; before that, the scratch folder `sdp-chat-<id>` of a chat that is not listed (a rename at its delete failed) is renamed away too. What is left is counted in the runner's log and swept again at the next turn, the next delete and the next start. Returns how many were left. A call waits for a sweep that runs already, then makes its own pass."""
        with self._sweep_lock:
            left = self._sweep_root(self.work_fd, CHAT_OLD)
            try:
                root_fd = os.open(self.tmp, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
            except OSError:
                root_fd = None
            if root_fd is not None:
                try:
                    self._rename_stray_scratch(root_fd)
                    left += self._sweep_root(root_fd, SCRATCH_OLD)
                finally:
                    os.close(root_fd)
        if left:
            _log(f"{left} renamed-away folders could not be removed (their files kept changing); they are tried again later")
        return left

    def _rename_stray_scratch(self, root_fd):
        """Rename away each `sdp-chat-<id>` of the tmp root whose chat is not listed (so has no turn), relative to the root's descriptor and never following a link."""
        try:
            names, _ = scan(root_fd, self.limits.listing_max)
        except OSError:
            return
        for name in names:
            found = SCRATCH.fullmatch(name)
            if found is None:
                continue
            with self._lock:
                if found.group(1) in self._chats:
                    continue
                with contextlib.suppress(OSError):
                    os.rename(name, f"{name}.old-{os.urandom(4).hex()}", src_dir_fd=root_fd, dst_dir_fd=root_fd)

    def _sweep_root(self, root_fd, pattern):
        left = 0
        try:
            names, _ = scan(root_fd, self.limits.listing_max)
        except OSError:
            return 0
        for name in names:
            if pattern.fullmatch(name):
                try:
                    remove_tree(root_fd, name)
                except FileNotFoundError:
                    pass
                except OSError:
                    left += 1
        return left

    def _finish(self, chat, process, translator, started, token):
        try:
            end = translator.end if translator is not None else None
            reason = process.stopped if process is not None and process.started else "not_started"
            if reason in ("stopped", "limit"):
                self._append(chat, "error", {"message": STOPPED if reason == "stopped" else LIMIT})
            elif reason is None and end is None:
                self._append(chat, "error", {"message": ended_with(process.returncode if process.returncode is not None else -1)})
            if end is None or reason is not None:
                end = {"is_error": True, "cost_usd": end["cost_usd"] if end else None, "duration_ms": int((time.monotonic() - started) * 1000), "turns": end["turns"] if end else None}
            self._append(chat, "end", end, last=True, ending=token)
        finally:
            with chat.lock:
                if chat.token is token:
                    chat.token = None
            with self._lock:
                chat.running = False
                chat.process = None
                chat.stop_asked = False
                self._running -= 1

    def _stop(self, chat):
        with self._lock:
            if not chat.running:
                return False
            chat.stop_asked = True
            process = chat.process
        if process is not None:
            process.stop("stopped")
        return True

    # ── requests ──────────────────────────────────────────────────────────

    def answer(self, request):
        op = request.get("op")
        if not isinstance(op, str) or op not in OPS or set(request) - {"op"} != OPS[op]:
            return _error("bad_request")
        try:
            return _ok(getattr(self, "_op_" + op.replace(".", "_"))(request))
        except _Refusal as refusal:
            return _error(refusal.code)

    def _chat_of(self, request):
        value = request["chat"]
        if not isinstance(value, str) or CHAT_ID.fullmatch(value) is None:
            raise _Refusal("bad_request")
        with self._lock:
            chat = self._chats.get(value)
        if chat is None:
            raise _Refusal("not_found")
        return chat

    def _whole(self, request, key, low, high):
        value = request[key]
        if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
            raise _Refusal("bad_request")
        return value

    def _op_hello(self, request):
        return {"version": PROTOCOL, "backend": self.backend_name, "databases": list(self.databases), "queue": self.queue}

    def _op_chat_list(self, request):
        with self._lock:
            chats = sorted(self._chats.values(), key=lambda c: (c.created, c.id), reverse=True)
            return {"chats": [{"id": c.id, "title": c.title, "created": c.created, "running": c.running} for c in chats]}

    def _op_chat_new(self, request):
        with self._lock:
            if self._stopping:
                raise _Refusal("unavailable")
            if len(self._chats) >= self.limits.chats_max:
                raise _Refusal("too_many_chats")
            chat_id = "c_" + os.urandom(6).hex()
            while chat_id in self._chats:
                chat_id = "c_" + os.urandom(6).hex()
            chat = _Chat(chat_id, {"created": round(time.time(), 3), "session": str(uuid.uuid4())})
            try:
                chat_fd = _open_folder(self.work_fd, chat_id, create=True)
                try:
                    for name in ("home", "files"):
                        os.mkdir(name, FOLDER_MODE, dir_fd=chat_fd)
                    self._save_meta(chat, chat_fd)
                finally:
                    os.close(chat_fd)
            except OSError:
                raise _Refusal("not_usable") from None
            self._chats[chat_id] = chat
        return {"chat": chat_id}

    def _op_chat_send(self, request):
        chat = self._chat_of(request)
        text = request["text"]
        if not isinstance(text, str) or not 1 <= len(text) <= self.limits.text_max:
            raise _Refusal("bad_request")
        try:
            text.encode("utf-8")
        except UnicodeEncodeError:
            raise _Refusal("bad_request") from None
        with self._lock:
            if self._stopping:
                raise _Refusal("unavailable")
            if chat.running:
                raise _Refusal("busy")
            if self._running >= self.limits.turns_max:
                raise _Refusal("too_many")
            chat.running = True
            self._running += 1
        try:
            size, complete = measure(self.work_fd, self.limits.walk_limit)
            if not complete or size > self.cap_bytes:
                # Deleted chats still count until the sweep has removed them: ask for one, so a send after it is taken.
                self._sweep_later()
                raise _Refusal("over_cap")
            with chat.lock:
                with self._chat_fd(chat) as chat_fd:
                    self._index(chat, chat_fd)
                full = chat.full
            if full:
                raise _Refusal("chat_full")
            offset = self._append(chat, "user", {"text": text})
            if offset is None:
                raise _Refusal("chat_full")
            with self._chat_fd(chat) as chat_fd:
                if not chat.title:
                    chat.title = " ".join(text.split())[: self.limits.title_max]
                chat.turns += 1
                with contextlib.suppress(OSError):
                    self._save_meta(chat, chat_fd)
            token = chat.token = object()
            chat.thread = threading.Thread(target=self._turn, args=(chat, text, token), name=f"turn-{chat.id}", daemon=True)
            chat.thread.start()
        except BaseException:
            with self._lock:
                chat.running = False
                self._running -= 1
            raise
        return {"offset": offset}

    def _op_chat_events(self, request):
        chat = self._chat_of(request)
        offset = self._whole(request, "offset", 0, 1 << 40)
        limit = self._whole(request, "limit", 1, self.limits.events_limit_max)
        return self._read_events(chat, offset, limit)

    def _op_chat_stop(self, request):
        return {"stopping": self._stop(self._chat_of(request))}

    def _op_chat_delete(self, request):
        """Refused while a turn runs. The chat's work folder and its scratch folder are renamed away (`<id>.old-<8 hex>`, relative to their roots' descriptors, never following a link) and the chat leaves the list at once; `sweep` removes them in the background. When the work folder cannot be renamed, the chat stays as it is (`not_usable`)."""
        chat = self._chat_of(request)
        with self._lock:
            if chat.running:
                raise _Refusal("busy")
            with chat.lock:
                suffix = f".old-{os.urandom(4).hex()}"
                try:
                    os.rename(chat.id, chat.id + suffix, src_dir_fd=self.work_fd, dst_dir_fd=self.work_fd)
                except FileNotFoundError:
                    pass
                except OSError:
                    raise _Refusal("not_usable") from None
                try:
                    tmp_fd = os.open(self.tmp, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
                    try:
                        os.rename(f"sdp-chat-{chat.id}", f"sdp-chat-{chat.id}{suffix}", src_dir_fd=tmp_fd, dst_dir_fd=tmp_fd)
                    finally:
                        os.close(tmp_fd)
                except FileNotFoundError:
                    pass
                except OSError:
                    _log("a deleted chat's scratch folder could not be renamed away")
                del self._chats[chat.id]
        self._sweep_later()
        return {}

    @contextlib.contextmanager
    def _files_fd(self, chat):
        with self._chat_fd(chat) as chat_fd:
            try:
                files_fd = _open_folder(chat_fd, "files")
            except OSError:
                raise _Refusal("not_usable") from None
            try:
                yield files_fd
            finally:
                os.close(files_fd)

    def _op_chat_files(self, request):
        chat = self._chat_of(request)
        with self._files_fd(chat) as files_fd:
            found, left_out, more = listing(files_fd, self.limits)
        names = sorted(found)
        more = more or len(names) > self.limits.files_max
        return {"files": [{"name": n, "size": found[n].st_size, "mtime": round(found[n].st_mtime, 3)} for n in names[: self.limits.files_max]], "left_out": left_out, "more": more}

    def _op_chat_file(self, request):
        chat = self._chat_of(request)
        name = request["name"]
        if not isinstance(name, str) or not FILE_NAME.fullmatch(name) or name in (".", ".."):
            raise _Refusal("bad_request")
        offset = self._whole(request, "offset", 0, 1 << 50)
        limit = self._whole(request, "limit", 1, self.limits.piece_bytes)
        with self._files_fd(chat) as files_fd:
            try:
                fd = _open_regular(files_fd, name, os.O_RDONLY)
            except OSError:
                raise _Refusal("no_such_file") from None
            try:
                size = os.fstat(fd).st_size
                data = _read_at_most(fd, limit, offset)
            finally:
                os.close(fd)
        return {"data": base64.b64encode(data).decode("ascii"), "size": size, "next": offset + len(data), "end": offset + len(data) >= size}


# ── the socket ────────────────────────────────────────────────────────────


class StartError(Exception):
    """The runner cannot start; the text says why."""


def bind(path):
    """The listening socket at `path`, mode 0600, bound through a descriptor of its folder (so the length of the folder's path does not matter). A leftover socket there is removed; anything else refuses the start."""
    folder, name = os.path.split(os.path.abspath(path))
    try:
        folder_fd = os.open(folder, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    except OSError:
        raise StartError("the runner's socket folder cannot be opened") from None
    try:
        try:
            leftover = os.stat(name, dir_fd=folder_fd, follow_symlinks=False)
        except FileNotFoundError:
            leftover = None
        if leftover is not None and not stat.S_ISSOCK(leftover.st_mode):
            raise StartError("something other than a socket is in the place of the runner's socket")
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        old = os.umask(0o177)
        try:
            if leftover is not None:
                os.unlink(name, dir_fd=folder_fd)
            listener.bind(f"/proc/self/fd/{folder_fd}/{name}")
            os.chmod(name, SOCKET_MODE, dir_fd=folder_fd, follow_symlinks=False)
            listener.listen(32)
        except OSError:
            listener.close()
            raise StartError("the runner's socket could not be made") from None
        finally:
            os.umask(old)
    finally:
        os.close(folder_fd)
    return listener


class Server:
    """The socket's accept loop and its connections, each answered on a thread of its own."""

    def __init__(self, runner, path):
        self.runner = runner
        self.limits = runner.limits
        self.path = path
        self.listener = bind(path)
        self._identity = (os.stat(path).st_dev, os.stat(path).st_ino)
        self._lock = threading.Lock()
        self._connections = set()
        self._stopping = threading.Event()
        self._wake_read, self._wake_write = os.pipe2(os.O_CLOEXEC | os.O_NONBLOCK)
        self._acceptor = threading.Thread(target=self._accept_loop, name="runner-accept", daemon=True)

    def start(self):
        self._acceptor.start()

    def close(self):
        if self._stopping.is_set():
            return
        self._stopping.set()
        with contextlib.suppress(OSError):
            os.write(self._wake_write, b"x")
        if self._acceptor.ident is not None:
            self._acceptor.join(5)
        self.listener.close()
        with contextlib.suppress(OSError):
            info = os.stat(self.path)
            if (info.st_dev, info.st_ino) == self._identity:
                os.unlink(self.path)
        with self._lock:
            threads = list(self._connections)
        for thread in threads:
            thread.join(self.limits.request_seconds + self.limits.send_seconds)
        for fd in (self._wake_read, self._wake_write):
            with contextlib.suppress(OSError):
                os.close(fd)

    def _accept_loop(self):
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(self.listener, selectors.EVENT_READ)
                selector.register(self._wake_read, selectors.EVENT_READ)
                while not self._stopping.is_set():
                    for key, _ in selector.select():
                        if key.fileobj is self.listener and not self._stopping.is_set():
                            self._accept()
        except Exception as error:
            _log(f"the socket stopped taking connections ({type(error).__name__})")

    def _accept(self):
        try:
            connection, _ = self.listener.accept()
        except OSError:
            self._stopping.wait(0.05)
            return
        thread = None
        with self._lock:
            if len(self._connections) < self.limits.connections_max:
                thread = threading.Thread(target=self._handle, args=(connection,), name="runner-connection", daemon=True)
                self._connections.add(thread)
        if thread is None:
            connection.close()
            return
        try:
            thread.start()
        except RuntimeError:
            with self._lock:
                self._connections.discard(thread)
            connection.close()

    def _handle(self, connection):
        try:
            reply = self._read_and_answer(connection)
            if reply is not None:
                self._send(connection, reply)
        except Exception as error:
            _log(f"a request failed ({type(error).__name__})")
            with contextlib.suppress(Exception):
                self._send(connection, _error("internal"))
        finally:
            with contextlib.suppress(OSError):
                connection.close()
            with self._lock:
                self._connections.discard(threading.current_thread())

    def _read_and_answer(self, connection):
        deadline = time.monotonic() + self.limits.request_seconds

        def read(count):
            left = deadline - time.monotonic()
            if left <= 0:
                raise TimeoutError()
            connection.settimeout(left)
            return connection.recv(count)

        try:
            request = framing.read_frame(read, max_bytes=self.limits.request_bytes, deadline=deadline)
        except framing.FrameTooLarge:
            return _error("request_too_large")
        except _SILENT_FRAME_ERRORS:
            return None
        except framing.FrameError:
            return _error("bad_request")
        return self.runner.answer(request)

    def _send(self, connection, reply):
        try:
            frame = framing.encode(reply, max_bytes=self.limits.reply_bytes)
        except framing.FrameTooLarge:
            frame = framing.encode(_error("too_large"), max_bytes=self.limits.reply_bytes)
        except framing.FrameError:
            frame = framing.encode(_error("internal"), max_bytes=self.limits.reply_bytes)
        try:
            connection.settimeout(self.limits.send_seconds)
            connection.sendall(frame)
        except OSError as error:
            _log(f"a reply could not be sent ({type(error).__name__})")
