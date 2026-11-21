"""The audit log of the broker: the file `audit.jsonl` in the broker folder (mode 0600), to which one writer appends one JSON object per line.

A line is `{"n": <sequence number>, "t": <UTC time to the second>, "session": <public session number or null>, "event": <one of a closed list>, <the fields of that event>, "prev": <SHA-256, in hex, of the bytes of the line before, without its newline; 64 zeros on the first line>}`, with the keys in that order, no spaces and ASCII only, so the bytes of a line follow from its content. Each line names the hash of the one before it: a line that was removed, edited, moved or put in between shows as a break of that chain. The chain has no key, so it finds a careless change and not a careful one (whoever can write the file can write a whole new chain), and the last line of a file is covered by nothing after it.

`AuditLog` is the writer. It opens the file by the rules of the run store, holds a lock on it for as long as it lives (a second writer is refused), and before every line checks that the file still ends with what it wrote last: when it does not (lines were appended, cut off or edited, the file was replaced or removed), it first writes a `tail_changed` line; an end that is cut off inside a line gets its newline and a `repaired` line, which names in `after` the last line of the log before the cut-off lines (one or several, when a full disk cut the `repaired` line itself), so that a change of that line is still found. A line is written with one `write` call and synced. Writing a line never raises: a line that could not be written returns False and is counted, because an audit failure must not undo an action that has already run. `writable()` is for the other direction: it lets the caller refuse a state-changing call before it runs while the log cannot be written. What is counted rather than written line by line (failed logins, requests without a valid token, read calls per session, lost lines) goes into a `counts` line at most once a minute; it names at most 64 sessions and totals the rest, so that it always fits.

Secrets never reach a line: `fields_for_audit` replaces every field whose input type is a secret by a constant, by the type and not by the name, and a `SecretValue` anywhere in a value is replaced as well.

`verify` and `read_tail` read a log without a writer: both go through the file in pieces and never raise on what the file holds; `verify` holds no more than a line at a time, `read_tail` at most 8 MiB of lines. The module uses the standard library and this package only; the clock and the folder are parameters.
"""

import contextlib
import errno
import fcntl
import hashlib
import json
import os
import re
import stat
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from social_data_pipeline.control import files, framing
from social_data_pipeline.control.errors import ControlError

FILE_NAME = "audit.jsonl"
ZERO_HASH = "0" * 64
# The longest line the writer writes, in bytes without the newline. A longer line in a file is not read into memory.
LINE_MAX_BYTES = 65536
COUNTS_SECONDS = 60
SECRET_MARK = "<secret>"

EVENTS = ("broker_start", "broker_stop", "login", "logout", "session_end", "call_accepted", "call_ended", "call_refused", "step_up_failed", "counts", "tail_changed", "repaired")
SESSION_END_REASONS = ("expired", "password_changed", "step_up_failures", "superseded")
DB_PASSWORD_FROM = ("fields", "session", "none")

# The fields of each event, in the order they are written between `event` and `prev`.
EVENT_FIELDS = {
    "broker_start": (),
    "broker_stop": (),
    "login": (),
    "logout": (),
    "session_end": ("reason",),
    "call_accepted": ("verb", "fields", "db_password_from"),
    "call_ended": ("verb", "fields", "db_password_from", "outcome", "exit_code"),
    "call_refused": ("verb", "code", "field"),
    "step_up_failed": (),
    "counts": ("failed_logins", "unauthenticated", "reads", "reads_other", "lost"),
    "tail_changed": (),
    "repaired": ("after",),
}
# A `counts` line names at most this many sessions; the reads of the others are one total, so that the line always fits.
COUNTS_SESSIONS_MAX = 64
# The largest session number a read is counted for: what every reader of the JSON gets back exactly.
_SESSION_NUMBER_MAX = 2 ** 53
# The most bytes of lines `read_tail` holds at once.
TAIL_MAX_BYTES = 8 * 1024 * 1024

# What a verb name, an error code, a field name and an outcome look like. Anything else is text someone wrote, and is not written.
_NAME_PATTERN = r"[A-Za-z][A-Za-z0-9_.-]{0,63}"
_HASH_PATTERN = r"[0-9a-f]{64}"
_EXIT_CODE_RANGE = range(-255, 256)
_VALUE_DEPTH = 16
_CHUNK_BYTES = 65536
_TAIL_LIMIT_MAX = 10000


class AuditNotUsable(ControlError):
    """The audit log could not be opened: its folder or file is a link, a special file or another user's, or the file system refused."""

    code = "audit_not_usable"

    def __init__(self):
        super().__init__("the audit log cannot be used")


class AuditInUse(ControlError):
    """Another writer holds the audit log."""

    code = "audit_in_use"

    def __init__(self):
        super().__init__("the audit log is held by another writer")


# ── what a line may hold ────────────────────────────────────────────────────


def _is_whole_number(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _is_name(value):
    return isinstance(value, str) and re.fullmatch(_NAME_PATTERN, value) is not None


def _without_secrets(value, depth=0):
    """A copy of a value in which everything that is not plain JSON is replaced by the constant: a `SecretValue`, an object of any other type (a subclass of text or of a number included, which is how a wrapper of one's own would look), and anything nested too deep."""
    if value is None or type(value) in (bool, int, float, str):
        return value
    if depth < _VALUE_DEPTH:
        if isinstance(value, dict) and all(isinstance(key, str) for key in value):
            return {key: _without_secrets(item, depth + 1) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [_without_secrets(item, depth + 1) for item in value]
    return SECRET_MARK


def fields_for_audit(verb, validated_fields):
    """The fields of a validated request as an audit line may hold them, sorted by name. `verb` is the registry's verb (its `socket_inputs` give each field's type). A field whose type is a secret is replaced by the constant "<secret>": by its type, never by its name. So is a field the verb does not list, whose type is therefore not known. Every other value is kept, except that a `SecretValue` found anywhere inside one is replaced too. Raises TypeError for fields that are not a dict with text keys."""
    if not isinstance(validated_fields, dict) or not all(isinstance(name, str) for name in validated_fields):
        raise TypeError("the fields of a request are a dict with text keys")
    inputs = verb.socket_inputs
    shown = {}
    for name in sorted(validated_fields):
        spec = inputs.get(name)
        if spec is None or getattr(spec.type, "is_secret", True) is not False:
            shown[name] = SECRET_MARK
        else:
            shown[name] = _without_secrets(validated_fields[name])
    return shown


def _checked(event, session, fields):
    """The fields of an event in their written order. Raises ValueError for an event outside the closed list and for a value an event's field cannot hold."""
    if event not in EVENT_FIELDS:
        raise ValueError("not an event of the audit log")
    if session is not None and not (_is_whole_number(session) and session >= 1):
        raise ValueError("a session is named by its number")
    if set(fields) != set(EVENT_FIELDS[event]):
        raise ValueError("these are not the fields of the event")
    checks = {
        "reason": lambda value: value in SESSION_END_REASONS,
        "verb": lambda value: _is_name(value) or (value is None and event == "call_refused"),
        "fields": lambda value: isinstance(value, dict) and all(isinstance(key, str) for key in value),
        "db_password_from": lambda value: value in DB_PASSWORD_FROM,
        "outcome": _is_name,
        "exit_code": lambda value: value is None or (_is_whole_number(value) and value in _EXIT_CODE_RANGE),
        "code": _is_name,
        "field": lambda value: value is None or _is_name(value),
        "failed_logins": lambda value: _is_whole_number(value) and value >= 0,
        "unauthenticated": lambda value: _is_whole_number(value) and value >= 0,
        "reads": lambda value: isinstance(value, dict) and len(value) <= COUNTS_SESSIONS_MAX and all(isinstance(key, str) and key.isascii() and key.isdigit() and _is_whole_number(count) and count >= 0 for key, count in value.items()),
        "reads_other": lambda value: _is_whole_number(value) and value >= 0,
        "lost": lambda value: _is_whole_number(value) and value >= 0,
        "after": lambda value: isinstance(value, str) and re.fullmatch(_HASH_PATTERN, value) is not None,
    }
    for name in EVENT_FIELDS[event]:
        if not checks[name](fields[name]):
            raise ValueError("a field of the event does not hold what it may hold")
    return [(name, fields[name]) for name in EVENT_FIELDS[event]]


def _line(number, seconds, session, event, fields, prev):
    """The bytes of a line, without its newline. Raises ValueError or TypeError for content a line cannot hold (a `SecretValue` among it is one such thing) and for a line longer than the limit."""
    stamp = datetime.fromtimestamp(int(seconds), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    content = dict([("n", number), ("t", stamp), ("session", session), ("event", event), *_checked(event, session, fields), ("prev", prev)])
    line = json.dumps(content, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode("ascii")
    if len(line) > LINE_MAX_BYTES:
        raise ValueError("the line is longer than a line may be")
    return line


def _record(content):
    """The object of a line when it is one this log could hold: strict JSON in ASCII with a sequence number and the hash of a line before it. None otherwise."""
    if content is None or not content.isascii():
        return None
    try:
        record = framing.decode(content)
    except framing.FrameError:
        return None
    number, prev = record.get("n"), record.get("prev")
    if not _is_whole_number(number) or number < 1 or not isinstance(prev, str) or not re.fullmatch(_HASH_PATTERN, prev):
        return None
    return record


# ── reading a file in pieces ────────────────────────────────────────────────


def _pread(fd, count, offset):
    """`count` bytes of a file from `offset`, fewer only at its end."""
    data = bytearray()
    while len(data) < count:
        chunk = os.pread(fd, count - len(data), offset + len(data))
        if not chunk:
            break
        data += chunk
    return bytes(data)


def _lines(fd, offset=0):
    """Go through a file from `offset` and yield, for every line, (the SHA-256 of its bytes in hex, its bytes or None when there are more than a line may have, whether it ended with a newline). Only the last line can lack the newline; an empty file yields nothing."""
    hasher, kept, too_long, pending = hashlib.sha256(), bytearray(), False, False
    while True:
        chunk = os.pread(fd, _CHUNK_BYTES, offset)
        if not chunk:
            break
        offset += len(chunk)
        start = 0
        while True:
            end = chunk.find(b"\n", start)
            piece = chunk[start:] if end < 0 else chunk[start:end]
            hasher.update(piece)
            if not too_long:
                if len(kept) + len(piece) > LINE_MAX_BYTES:
                    too_long, kept = True, bytearray()
                else:
                    kept += piece
            if end < 0:
                pending = pending or bool(piece)
                break
            yield hasher.hexdigest(), None if too_long else bytes(kept), True
            hasher, kept, too_long, pending = hashlib.sha256(), bytearray(), False, False
            start = end + 1
    if pending:
        yield hasher.hexdigest(), None if too_long else bytes(kept), False


def _line_start(fd, end, piece=None):
    """Where the line begins whose bytes end before the offset `end`: after the last newline before it, or at the start of the file. `piece` is a dict in which the last piece that was read is kept, for a caller that asks for many lines close to each other."""
    piece = {} if piece is None else piece
    position = end
    while position > 0:
        start, data = piece.get("start", 0), piece.get("data", b"")
        if not start < position <= start + len(data):
            start = max(0, position - _CHUNK_BYTES)
            data = _pread(fd, position - start, start)
            if len(data) != position - start:
                raise OSError(errno.EIO, "the file became shorter while it was read")
            piece["start"], piece["data"] = start, data
        found = data.rfind(b"\n", 0, position - start)
        if found >= 0:
            return start + found + 1
        position = start
    return 0


def _end_of_file(fd):
    """What a writer needs to know of a file's end: (its size, whether it ends with a newline or is empty, the hash of its last line, the sequence number of that line or None when it is not a line of this log)."""
    size = os.fstat(fd).st_size
    if size == 0:
        return 0, True, ZERO_HASH, 0
    complete = _pread(fd, 1, size - 1) == b"\n"
    end = size - 1 if complete else size
    for digest, content, _ in _lines(fd, _line_start(fd, end)):
        record = _record(content)
        return size, complete, digest, None if record is None else record["n"]
    # The last line is empty: two newlines in a row.
    return size, complete, hashlib.sha256().hexdigest(), None


def _last_record_hash(fd, end):
    """The hash of the last line before the offset `end` that is a line of this log, going back over lines that are not; 64 zeros when there is none. The line that ends at `end` is the first one looked at. Each line is read in pieces, so a long one costs no more memory than a line may have."""
    piece = {}
    while True:
        start = _line_start(fd, end, piece)
        for digest, content, _ in _lines(fd, start):
            if _record(content) is not None:
                return digest
            break
        if start == 0:
            return ZERO_HASH
        end = start - 1


def _last_number(fd):
    """The sequence number of the last line of a file that is a line of this log; 0 when there is none."""
    number = 0
    for _, content, _ in _lines(fd):
        record = _record(content)
        if record is not None:
            number = record["n"]
    return number


# ── the writer ──────────────────────────────────────────────────────────────

# What can go wrong while a line is made and written. OverflowError: a clock far outside what a date can hold.
_WRITE_ERRORS = (OSError, ValueError, TypeError, RecursionError, OverflowError, files.BrokerFileError)


class AuditLog:
    """The one writer of the audit log in a broker folder (a path, or a descriptor of the folder). The folder and the file are created when they are missing. `now()` returns seconds since the epoch. Every method may be called from any thread. Raises AuditNotUsable when the log cannot be opened and AuditInUse when another writer holds it.

    `lost` is the number of lines that could not be written since this writer was opened (a line that was written and could not be synced counts too). The next `counts` line that is written says how many were lost since the one before it.
    """

    def __init__(self, folder, *, now=time.time):
        self._now = now
        self._lock = threading.Lock()
        self._folder_fd = None
        self._fd = None
        self.lost = 0
        self._lost_since_counts = 0
        self._failing = False
        # Lines this writer has to write before any other: `repaired`, `tail_changed`.
        self._owed = []
        # For the `repaired` line: the hash of the last line of the log before the cut-off lines it closes.
        self._repair_after = ZERO_HASH
        # The last write stopped inside a line: the cut-off end that the next look finds is this writer's own.
        self._own_fragment = False
        self._number = 0
        self._prev = ZERO_HASH
        self._size = 0
        self._tail = b""
        self._failed_logins = 0
        self._unauthenticated = 0
        self._reads = {}
        self._counts_at = now()
        try:
            with files.folder(folder, create=True) as folder_fd:
                self._folder_fd = os.dup(folder_fd)
            self._open_file()
            self._adopt(first=True)
        except (OSError, files.BrokerFileError):
            self._close()
            raise AuditNotUsable() from None
        except BaseException:
            self._close()
            raise
        with self._lock:
            self._ready()

    # ── the file ────────────────────────────────────────────────────────────

    def _open_file(self):
        """Open the log, created when it is missing, and take its lock."""
        fd = files.open_file(self._folder_fd, FILE_NAME, os.O_RDWR | os.O_APPEND, create=True)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            raise AuditInUse() from None
        except BaseException:
            os.close(fd)
            raise
        if self._fd is not None:
            os.close(self._fd)
        self._fd = fd

    def _close(self):
        for name in ("_fd", "_folder_fd"):
            fd = getattr(self, name)
            if fd is not None:
                setattr(self, name, None)
                with contextlib.suppress(OSError):
                    os.close(fd)

    def _adopt(self, first=False):
        """Take the file's end as it is: close a cut-off last line with a newline, remember the hash of the last line and what the file ends with, and note the lines this writer owes for it. A writer that has just been opened also takes its sequence number from the file."""
        size, complete, digest, number = _end_of_file(self._fd)
        if not complete:
            self._repair_after = _last_record_hash(self._fd, size)
            if os.write(self._fd, b"\n") != 1:
                raise OSError(errno.EIO, "the newline was not written")
            size += 1
        if first:
            self._number = _last_number(self._fd) if number is None else number
        elif self._own_fragment and number == self._number + 1:
            # The write that stopped short lacked only its newline: the line is whole and has its number.
            self._number = number
        self._prev = digest
        self._size = size
        self._tail = _pread(self._fd, min(size, LINE_MAX_BYTES + 1), size - min(size, LINE_MAX_BYTES + 1))
        if not complete:
            # The repair comes before anything else that is owed: the cut-off lines must be closed by the line right after them, also when what was cut was itself an owed line (a `tail_changed`).
            if "repaired" in self._owed:
                self._owed.remove("repaired")
            self._owed.insert(0, "repaired")
        # A new writer finds a last line that no writer wrote; a writer at work finds an end that is not its own.
        foreign = (number is None and complete and size > 0) if first else not self._own_fragment
        if foreign and "tail_changed" not in self._owed:
            self._owed.append("tail_changed")
        self._own_fragment = False

    def _is_as_left(self):
        """Whether the log's name still leads to the open file and the file still ends with what this writer wrote last."""
        ours = os.fstat(self._fd)
        try:
            named = os.stat(FILE_NAME, dir_fd=self._folder_fd, follow_symlinks=False)
        except FileNotFoundError:
            named = None
        if named is None or (named.st_dev, named.st_ino) != (ours.st_dev, ours.st_ino):
            # Replaced or removed: what this descriptor writes, nobody reads. The file under the name is the log from here on, and a fragment this writer left was in the old file: the end of the new one is not its own.
            self._open_file()
            self._own_fragment = False
            return False
        return ours.st_size == self._size and _pread(self._fd, len(self._tail), self._size - len(self._tail)) == self._tail

    def _ready(self):
        """Bring the writer level with the file: True when the file's end is known and every owed line is written."""
        try:
            if not self._is_as_left():
                self._adopt()
            while self._owed:
                self._write(self._owed[0], None, {"after": self._repair_after} if self._owed[0] == "repaired" else {})
                self._owed.pop(0)
                self._failing = False
            return True
        except (*_WRITE_ERRORS, AuditInUse):
            self._failing = True
            return False

    def _write(self, event, session, fields):
        """Write one line with one `write` call and sync it. Raises one of _WRITE_ERRORS when the line was not written, or was written and not synced."""
        number = self._number + 1
        line = _line(number, self._now(), session, event, fields, self._prev)
        data = line + b"\n"
        written = os.write(self._fd, data)
        if written != len(data):
            self._own_fragment = written > 0
            raise OSError(errno.EIO, "the line was not written whole")
        self._number = number
        self._prev = hashlib.sha256(line).hexdigest()
        self._size += len(data)
        self._tail = data
        os.fsync(self._fd)

    def _append(self, event, session=None, counted=True, **fields):
        """Write the line of an event; False when it could not be written. Called with the lock held."""
        written = False
        if self._fd is not None and self._ready():
            try:
                self._write(event, session, fields)
                written = True
            except _WRITE_ERRORS:
                pass
        if written:
            self._failing = False
            return True
        self._failing = True
        if counted:
            self.lost += 1
            self._lost_since_counts += 1
        return False

    def writable(self):
        """Whether a line can be written right now, as far as that can be known before writing one: the log is open, its file is the one under its name, everything owed is written, and the last line that was tried was written. After a line that could not be written this tries a `counts` line, which says how many lines were lost and shows whether writing works again. Ask before a state-changing call runs, and refuse the call on False."""
        with self._lock:
            if self._fd is None or not self._ready():
                return False
            if self._failing:
                return self._write_counts()
            return True

    def close(self):
        """Give up the file and its lock. Every line after this is a line that could not be written."""
        with self._lock:
            self._close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    # ── the events ──────────────────────────────────────────────────────────

    def broker_start(self):
        with self._lock:
            return self._append("broker_start")

    def broker_stop(self):
        """Write what was counted since the last `counts` line, then the stop."""
        with self._lock:
            if self._something_counted():
                self._write_counts()
            return self._append("broker_stop")

    def login(self, session):
        with self._lock:
            return self._append("login", session)

    def logout(self, session):
        with self._lock:
            return self._append("logout", session)

    def session_end(self, session, reason):
        """A session ended without a logout. `reason` is one of SESSION_END_REASONS."""
        with self._lock:
            return self._append("session_end", session, reason=reason)

    def step_up_failed(self, session):
        with self._lock:
            return self._append("step_up_failed", session)

    def _call(self, event, session, verb, validated_fields, db_password_from, **more):
        with self._lock:
            try:
                name, shown = verb.name, fields_for_audit(verb, validated_fields)
            except (AttributeError, TypeError):
                return self._append(event, session, verb=None, fields=None, db_password_from=db_password_from, **more)
            return self._append(event, session, verb=name, fields=shown, db_password_from=db_password_from, **more)

    def call_accepted(self, session, verb, validated_fields, db_password_from):
        """A state-changing call was accepted and is about to run. `verb` is the registry's verb and `validated_fields` what `validate_request` returned: they pass through `fields_for_audit` here. `db_password_from` says where the database password of the call came from: `fields`, `session` or `none`."""
        return self._call("call_accepted", session, verb, validated_fields, db_password_from)

    def call_ended(self, session, verb, validated_fields, db_password_from, outcome, exit_code):
        """A call that was accepted has ended. `outcome` is a short name the caller chooses, `exit_code` a whole number from -255 to 255 or None."""
        return self._call("call_ended", session, verb, validated_fields, db_password_from, outcome=outcome, exit_code=exit_code)

    def call_refused(self, session, verb_name, code, field=None):
        """A call was refused before anything ran. `verb_name` and `field` are written only when they have the form of a name: anything else is text the client wrote, and becomes null."""
        with self._lock:
            return self._append("call_refused", session, verb=verb_name if _is_name(verb_name) else None, code=code, field=field if _is_name(field) else None)

    # ── counting ────────────────────────────────────────────────────────────

    def count_failed_login(self):
        with self._lock:
            self._failed_logins += 1

    def count_unauthenticated(self):
        with self._lock:
            self._unauthenticated += 1

    def count_read(self, session_number):
        """Count one read call of a session, which gets no line of its own. The session is named by its number, a whole number from 1 to 2**53."""
        if not _is_whole_number(session_number) or not 1 <= session_number <= _SESSION_NUMBER_MAX:
            raise TypeError("a session is named by its number")
        with self._lock:
            self._reads[session_number] = self._reads.get(session_number, 0) + 1

    def _something_counted(self):
        return bool(self._failed_logins or self._unauthenticated or self._reads or self._lost_since_counts)

    def _write_counts(self):
        """Write a `counts` line and start counting again; when it cannot be written the numbers are kept for the next try. The line names the 64 sessions with the most reads (the lower number first among equals), sorted by number, and gives the reads of all others as one total, so that it always fits in a line."""
        lost = self._lost_since_counts
        ranked = sorted(self._reads.items(), key=lambda item: (-item[1], item[0]))
        named = sorted(ranked[:COUNTS_SESSIONS_MAX])
        reads = {str(number): count for number, count in named}
        reads_other = sum(count for _, count in ranked[COUNTS_SESSIONS_MAX:])
        if not self._append("counts", None, counted=False, failed_logins=self._failed_logins, unauthenticated=self._unauthenticated, reads=reads, reads_other=reads_other, lost=lost):
            return False
        self._failed_logins = 0
        self._unauthenticated = 0
        self._reads = {}
        self._lost_since_counts -= lost
        self._counts_at = self._now()
        return True

    def flush_counts(self, *, force=False):
        """Write one `counts` line when something was counted and at least 60 seconds have passed since the last one (since the writer was opened, for the first); with `force`, whenever something was counted. True when a line was written."""
        with self._lock:
            if not self._something_counted():
                return False
            if not force and self._now() - self._counts_at < COUNTS_SECONDS:
                return False
            return self._write_counts()


# ── reading a log without a writer ──────────────────────────────────────────


@dataclass(frozen=True)
class AuditReport:
    """What `verify` found. `readable`: the file could be opened and read (a missing file, a link or a special file is not readable). `lines`: complete lines read. `breaks`: places where a line does not continue the one before it. `first_break`: the sequence number that was expected at the first of them; that line, or the one before it, is not what the writer wrote. `cut_off`: the file does not end with a newline. `repaired`: runs of cut-off lines that a writer closed and marked with a `repaired` line (not counted as breaks). `tail_changed`: lines by which a writer said that the file's end was not what it had written."""

    readable: bool
    lines: int = 0
    breaks: int = 0
    first_break: int | None = None
    cut_off: bool = False
    repaired: int = 0
    tail_changed: int = 0

    @property
    def ok(self):
        return self.readable and self.breaks == 0 and not self.cut_off


def _open_for_reading(path):
    """A descriptor of a regular file opened without following a link and without waiting, or None."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except (OSError, TypeError, ValueError):
        return None
    try:
        if stat.S_ISREG(os.fstat(fd).st_mode):
            return fd
    except OSError:
        pass
    os.close(fd)
    return None


def verify(path):
    """Check the chain of an audit log and return an `AuditReport`. Every line must be a line of this log, name the hash of the line before it and carry the sequence number after it; the first line names 64 zeros and has the number 1. A line that is not one of this log is a break, and so is every line of a run of them, unless the line after the run is the `repaired` line a writer put there: it continues the last line of the run by `prev` and the last line of the log before the run by `n` and `after`, so that a change of that line is still found. Never raises on what the file holds, and reads it in pieces: a line of any length costs no more memory than the longest line a writer writes."""
    fd = _open_for_reading(path)
    if fd is None:
        return AuditReport(readable=False)
    found = {"lines": 0, "breaks": 0, "first_break": None, "cut_off": False, "repaired": 0, "tail_changed": 0}

    def note_break(expected, count=1):
        found["breaks"] += count
        if found["first_break"] is None:
            found["first_break"] = expected

    expected_prev, last_number = ZERO_HASH, 0
    # A run of lines that are not lines of this log, until the line after it shows whether a writer repaired it: the number expected at its first line, the hash of the line before it, and how many lines it has.
    run = None
    try:
        for digest, content, complete in _lines(fd):
            if not complete:
                found["cut_off"] = True
                break
            found["lines"] += 1
            record = _record(content)
            if record is None:
                if run is None:
                    run = {"expected": last_number + 1, "before": expected_prev, "count": 0}
                run["count"] += 1
                expected_prev = digest
                continue
            follows = record["prev"] == expected_prev and record["n"] == last_number + 1
            if run is not None:
                if follows and record.get("event") == "repaired" and record.get("after") == run["before"]:
                    found["repaired"] += 1
                else:
                    # A break for every line of the run; the line after it is taken as it is.
                    note_break(run["expected"], run["count"])
                run = None
            elif not follows:
                note_break(last_number + 1)
            if record.get("event") == "tail_changed":
                found["tail_changed"] += 1
            expected_prev, last_number = digest, record["n"]
        if run is not None:
            note_break(run["expected"], run["count"])
    except OSError:
        return AuditReport(readable=False, **found)
    finally:
        os.close(fd)
    return AuditReport(readable=True, **found)


def unreadable_line():
    """What `read_tail` returns in the place of a line that is not a JSON object, is longer than a line may be, or is cut off."""
    return {"unreadable": True}


def truncated_marker():
    """What `read_tail` returns first when it left out older lines that were asked for, because the newer ones already hold as many bytes as it holds at once."""
    return {"truncated": True}


def read_tail(path, limit):
    """The newest `limit` lines of an audit log as objects, newest last. Nothing is left out without a trace: a line that is not a JSON object, is longer than a line may be or has no newline is returned as `unreadable_line()`, and when the lines asked for hold more than 8 MiB, the newest whole lines that fit are returned after `truncated_marker()`. A line of any length costs no more memory than the longest line a writer writes. A file that is missing, is not a regular file or cannot be read gives an empty list. Raises ValueError for a limit that is not a whole number from 1 to 10000."""
    if not _is_whole_number(limit) or not 1 <= limit <= _TAIL_LIMIT_MAX:
        raise ValueError("the limit is a whole number from 1 to 10000")
    fd = _open_for_reading(path)
    if fd is None:
        return []
    found = []
    try:
        end = os.fstat(fd).st_size
        # The bytes after the last newline are a line without one; with nothing after it, the last line ends before that newline.
        complete = end > 0 and _pread(fd, 1, end - 1) == b"\n"
        if complete:
            end -= 1
        piece, held = {}, 0
        while len(found) < limit and (end > 0 or complete):
            start = _line_start(fd, end, piece)
            readable = complete and end - start <= LINE_MAX_BYTES
            if readable and held + end - start > TAIL_MAX_BYTES:
                found.append(truncated_marker())
                break
            line = _pread(fd, end - start, start) if readable else None
            held += 0 if line is None else len(line)
            try:
                found.append(unreadable_line() if line is None else framing.decode(line))
            except framing.FrameError:
                found.append(unreadable_line())
            if start == 0:
                break
            end, complete = start - 1, True
    except OSError:
        return []
    finally:
        os.close(fd)
    found.reverse()
    return found
