"""The pipeline runs of one deployment, as the host sees them: runs recorded in a state folder under the deployment root, and runs alive right now that were started in a terminal, found through Docker.

A recorded run has an id of the form `r<UTC date>T<UTC time>-<6 hex>` and a folder `<root>/.sdp/runs/<id>/` that holds `record.json`, `log`, `lock` and, once a cancel was asked, `cancel`. A terminal run has no folder: it is a container that `docker compose run` started from this deployment's folder for one of the pipeline services, and its id is `live-` plus the first 12 characters of the container id. Every path, the container name and the container label of a run are computed from its id, which is checked against its pattern before it reaches a path or a Docker argument; nothing of that kind is ever read from a record.

The state folder is private to the operator (folders 0700, files 0600) and is created by the first write only: reading never creates anything. No folder or file of the store is reached through a symbolic link: each folder is opened by its own name relative to its parent with O_NOFOLLOW, each file relative to its folder. The state folder and its runs folder must belong to the user who runs the command (one made by a single run as another user, root for instance, is refused), and a file that is written must be a regular file.

Every function takes the deployment root as an argument and the module keeps no state. It uses the standard library only and imports neither the CLI nor the control package. Docker is run through one function, `_docker`, always with a timeout, with two exceptions: following a log on a terminal, and the two commands that last as long as a container lives (waiting for its end, following its output into a descriptor), which the caller ends through a function that is asked about twice a second. A container's environment holds database passwords and is never read, except for the one variable `SOURCE`, which the inspect template below picks out inside Docker.

Linux is assumed: the locks are `flock` locks, which belong to an open file and are passed on to a child process together with the descriptor.
"""

import contextlib
import errno
import fcntl
import json
import os
import re
import secrets
import stat
import subprocess
import time
from datetime import datetime, timezone

# Both are checked with re.fullmatch: re.match with a trailing "$" would accept an id that ends in a newline.
RUN_ID_PATTERN = r"r[0-9]{8}T[0-9]{6}-[0-9a-f]{6}"
LIVE_ID_PATTERN = r"live-[0-9a-f]{12}"

KIND_RECORDED = "web"
KIND_TERMINAL = "terminal"
# The kind of a recorded run whose record names a task (a CLI action that is not a pipeline profile, `control.tasks`) instead of a profile. Its id has the form of any recorded run's, so `run_id_kind` answers KIND_RECORDED for it; its entry in a listing has this kind.
KIND_TASK = "task"

STATE_STARTING = "starting"
STATE_RUNNING = "running"
STATE_FINISHED = "finished"
STATE_CANCELLED = "cancelled"
# The four states a record holds, and the two of them after which a record no longer changes.
RECORDED_STATES = (STATE_STARTING, STATE_RUNNING, STATE_FINISHED, STATE_CANCELLED)
FINAL_STATES = (STATE_FINISHED, STATE_CANCELLED)
# States that are computed when a record is read and never written: the process that supervised the run is gone and its container is still there (detached) or is not (lost), Docker could not be asked (unknown), the record cannot be used (unreadable).
STATE_DETACHED = "detached"
STATE_LOST = "lost"
STATE_UNKNOWN = "unknown"
STATE_UNREADABLE = "unreadable"

RECORD_FIELDS = ("id", "profile", "source", "flags", "started_by", "state", "started", "ended", "exit_code")
# The two fields that only the record of a task has, and its entry in a listing: the verb, and the text shown where a pipeline run shows its profile. Such a record has no profile.
TASK_FIELDS = ("task", "label")
TASK_PATTERN = r"[a-z][a-z_]{0,31}(\.[a-z][a-z_]{0,31}){1,3}"
_LABEL_MAX_CHARS = 200

# What `cancel_run` did.
CANCEL_STOPPED = "stopped"
CANCEL_REQUESTED = "requested"
CANCEL_RECORDED = "recorded"

# Why something was refused. None of them holds an input value.
RUN_ID_NOT_VALID = "the run id is not a valid run id"
RUN_NOT_FOUND = "no run with this id"
RUN_ENDED = "the run has already ended"
RUN_LOST = "the run is no longer running"
RECORD_NOT_READABLE = "the run's record cannot be read"
LOG_NOT_A_FILE = "the run's log is not a regular file"
OUTPUT_NOT_READABLE = "the run's output cannot be read"
NOT_A_REGULAR_FILE = "a file of the run state folder is not a regular file"
STOP_FAILED = "Docker could not stop the run's container"
RECORD_NOT_WRITTEN = "the run has no running container any more, but its record could not be written"
STORE_NOT_A_FOLDER = "the run state folder is not a real folder (a symbolic link or a file is in its place)"
STORE_NOT_USABLE = "the run state folder cannot be used"
DOCKER_NOT_REACHABLE = "Docker cannot be reached"

DOCKER_READ_TIMEOUT = 15
DOCKER_STOP_TIMEOUT = 45
# The pipeline services run Python as the container's first process, which ignores the default stop signal and is killed when the time is up; the interrupt signal ends it at once through KeyboardInterrupt.
STOP_SIGNAL = "INT"
STOP_GRACE_SECONDS = 30
# How often a command that lasts as long as a container lives asks its caller whether to go on.
DOCKER_WATCH_SECONDS = 0.5
LOG_TAIL_LINES = 500

# One line per container: its id and three of its labels, each as a JSON string, so that no label value can forge a second line or a field.
PS_FORMAT = '[{{json .ID}},{{json (.Label "com.docker.compose.project.working_dir")}},{{json (.Label "com.docker.compose.service")}},{{json (.Label "sdp.run")}}]'
# The start time, a space, and the one environment entry whose name is SOURCE. The environment list holds database passwords: it is walked inside Docker and only that one entry is printed.
INSPECT_TEMPLATE = '{{.State.StartedAt}} {{range .Config.Env}}{{$kv := split . "="}}{{if eq (index $kv 0) "SOURCE"}}{{.}}{{end}}{{end}}'
# The same line with the container's state (`created`, `running`, `exited`, ...) as a fifth item, for the listing that includes the containers that are not running.
PS_STATE_FORMAT = '[{{json .ID}},{{json (.Label "com.docker.compose.project.working_dir")}},{{json (.Label "com.docker.compose.service")}},{{json (.Label "sdp.run")}},{{json .State}}]'
# What Docker calls a container that exists and was never started.
CONTAINER_CREATED = "created"
ONEOFF_LABEL = "com.docker.compose.oneoff=True"
RUN_LABEL_NAME = "sdp.run"

_STORE_FOLDER = ".sdp"
_RUNS_FOLDER = "runs"
_RECORD_NAME = "record.json"
_LOG_NAME = "log"
_LOCK_NAME = "lock"
_CANCEL_NAME = "cancel"
_FOLDER_MODE = 0o700
_FILE_MODE = 0o600
_RECORD_MAX_BYTES = 65536
_CONTAINER_ID_PATTERN = r"[0-9a-f]{64}"
# What `docker wait` prints for one container: its exit code on a line.
_WAIT_ANSWER_PATTERN = r"([0-9]{1,3})\n?"
_TIME_PATTERN = r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z"
_DOCKER_TIME_PATTERN = r"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(\.[0-9]+)?Z"
_EXIT_CODE_RANGE = range(-255, 256)
# The largest whole number a flag may hold: beyond it a reader of the JSON that keeps numbers as floats no longer gets the number that was written.
_FLAG_NUMBER_MAX = 2 ** 53
_PLACEHOLDER_ID = "r00000000T000000-000000"
# The end of a run in its longest form. A record of a run that has not ended is measured with it, so that the write that ends the run always fits.
_LONGEST_END = {"state": STATE_CANCELLED, "ended": "0000-00-00T00:00:00Z", "exit_code": -255}
# Someone who reads a run's state holds its lock for an instant, so taking it is tried a few times before the answer is "held".
_LOCK_ATTEMPTS = 5
_LOCK_RETRY_SECONDS = 0.05


class StoreError(Exception):
    """Something was refused. `message` is one constant line that holds no input value."""

    def __init__(self, message):
        super().__init__(message)
        self.message = message


class RunIdNotValid(StoreError):
    """The id has neither the form of a recorded run nor that of a terminal run."""


class RunNotFound(StoreError):
    """The id is well-formed and names no run of this deployment."""


class CancelRefused(StoreError):
    """The run is not in a state in which it can be cancelled."""


class RunAlreadyEnded(StoreError):
    """The end of a run was to be written into a record that already holds one."""


class DockerUnavailable(StoreError):
    """Docker could not be asked: the binary is missing, the daemon does not answer, or the call ran out of time."""


# ── ids ─────────────────────────────────────────────────────────────────────


def run_id_kind(run_id):
    """Return KIND_RECORDED or KIND_TERMINAL for a well-formed run id. Raises RunIdNotValid for anything else, a value that is not a string included."""
    if isinstance(run_id, str):
        if re.fullmatch(RUN_ID_PATTERN, run_id):
            return KIND_RECORDED
        if re.fullmatch(LIVE_ID_PATTERN, run_id):
            return KIND_TERMINAL
    raise RunIdNotValid(RUN_ID_NOT_VALID)


def _require_recorded(run_id):
    """Refuse an id that is not the id of a recorded run: a terminal run has no folder."""
    if run_id_kind(run_id) != KIND_RECORDED:
        raise RunIdNotValid(RUN_ID_NOT_VALID)


def new_run_id():
    """A new id for a recorded run: the UTC time to the second and six random hex digits, so that ids sort by time."""
    return "r" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + secrets.token_hex(3)


def container_name(run_id):
    """The name the container of a recorded run is started with."""
    _require_recorded(run_id)
    return f"sdp-run-{run_id}"


def run_label(run_id):
    """The label the container of a recorded run is started with, as `name=value`."""
    _require_recorded(run_id)
    return f"{RUN_LABEL_NAME}={run_id}"


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


# ── folders and files ───────────────────────────────────────────────────────


@contextlib.contextmanager
def _os_errors_refused():
    """Turn a file-system error inside the store into a refusal with a constant message: the error's own text names a path."""
    try:
        yield
    except OSError:
        raise StoreError(STORE_NOT_USABLE) from None


def _open_folder(name, *, dir_fd=None, create=False, owned=False):
    """Open one folder of the store by its name relative to its parent, never through a link. Returns a descriptor, or None for a folder that is missing and is not to be created. Raises StoreError when a link or a file is in its place, and with `owned` when the folder belongs to another user than the one this process acts as."""
    if create:
        try:
            os.mkdir(name, _FOLDER_MODE, dir_fd=dir_fd)
        except FileExistsError:
            pass
        except OSError:
            raise StoreError(STORE_NOT_USABLE) from None
    try:
        fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=dir_fd)
    except FileNotFoundError:
        if create:
            raise StoreError(STORE_NOT_USABLE) from None
        return None
    except OSError as error:
        # A link in the folder's place is reported as "not a directory", like a file.
        if error.errno in (errno.ENOTDIR, errno.ELOOP):
            raise StoreError(STORE_NOT_A_FOLDER) from None
        raise StoreError(STORE_NOT_USABLE) from None
    if owned:
        # Asked of the opened folder itself, so the answer is about the folder that is then used.
        try:
            ours = os.fstat(fd).st_uid == os.geteuid()
        except BaseException:
            os.close(fd)
            raise
        if not ours:
            os.close(fd)
            raise StoreError(STORE_NOT_USABLE)
    return fd


@contextlib.contextmanager
def _folder(root, names, *, create=False):
    """Yield a descriptor of the folder `<root>/<names...>`, each step opened without following a link; None when a step is missing and nothing is to be created. The first two steps, the state folder and its runs folder, must belong to the user this process acts as."""
    fd = None
    for step, name in enumerate(names):
        parent = fd
        try:
            fd = _open_folder(os.path.join(root, name) if parent is None else name, dir_fd=parent, create=create, owned=step < 2)
        finally:
            if parent is not None:
                os.close(parent)
        if fd is None:
            break
    try:
        yield fd
    finally:
        if fd is not None:
            os.close(fd)


@contextlib.contextmanager
def _run_folder(root, run_id):
    """Yield a descriptor of the folder of a recorded run. Raises RunIdNotValid before any path is built, RunNotFound when there is no such folder."""
    _require_recorded(run_id)
    with _folder(os.fspath(root), (_STORE_FOLDER, _RUNS_FOLDER, run_id)) as run_fd:
        if run_fd is None:
            raise RunNotFound(RUN_NOT_FOUND)
        yield run_fd


def _open_for_writing(name, dir_fd, flags):
    """Open a file of a store folder for writing, created with mode 0600 when it is missing, never through a link. The open does not wait (a named pipe planted under the name would otherwise hold the caller until someone reads it; on a regular file the flag changes nothing), and anything but a regular file is refused with a StoreError."""
    fd = os.open(name, flags | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, _FILE_MODE, dir_fd=dir_fd)
    try:
        regular = stat.S_ISREG(os.fstat(fd).st_mode)
    except BaseException:
        os.close(fd)
        raise
    if not regular:
        os.close(fd)
        raise StoreError(NOT_A_REGULAR_FILE)
    return fd


def _write_all(fd, payload):
    view = memoryview(payload)
    while view:
        view = view[os.write(fd, view):]


def _read_small_file(dir_fd, name, max_bytes):
    """The bytes of a regular file of a store folder, or None when it is missing, is not a regular file, is larger than max_bytes or cannot be read."""
    try:
        # Non-blocking, so that a pipe planted under the name does not hold the open.
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=dir_fd)
    except OSError:
        return None
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > max_bytes:
            return None
        chunks, total = [], 0
        while True:
            chunk = os.read(fd, 65536)
            if not chunk:
                return b"".join(chunks)
            total += len(chunk)
            if total > max_bytes:
                return None
            chunks.append(chunk)
    except OSError:
        return None
    finally:
        os.close(fd)


# ── records ─────────────────────────────────────────────────────────────────


def _is_whole_number(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _is_time(value):
    return isinstance(value, str) and re.fullmatch(_TIME_PATTERN, value) is not None


def _is_flag_scalar(item):
    if item is None or isinstance(item, (bool, str)):
        return True
    return _is_whole_number(item) and -_FLAG_NUMBER_MAX <= item <= _FLAG_NUMBER_MAX


def _is_flat_flag(item):
    return _is_flag_scalar(item) or (isinstance(item, list) and all(_is_flag_scalar(element) for element in item))


def _is_flag_value(item):
    """Whether a flag of a record holds text, a boolean, nothing, a whole number that every reader of the JSON gets back exactly, or a list of those or of flat records (for a task's index build, each table with its columns: a mapping of text to such a value or a list of them, one level only)."""
    if _is_flag_scalar(item):
        return True
    return isinstance(item, list) and all(_is_flag_scalar(element) or (isinstance(element, dict) and all(isinstance(key, str) and _is_flat_flag(value) for key, value in element.items())) for element in item)


def _is_task_record(value):
    return "task" in value or "label" in value


def _checked_record(value, run_id):
    """Return the nine fields of a record as a new dict when every one has the type it must have and the id is the folder's; None otherwise. The record of a task (one that has `task` or `label`) has the two TASK_FIELDS as well, a verb's name and a label of at most 200 characters, and no profile. Other keys are dropped."""
    if not isinstance(value, dict):
        return None
    task = _is_task_record(value)
    record = {name: value.get(name) for name in RECORD_FIELDS + (TASK_FIELDS if task else ())}
    flags = record["flags"]
    checks = (
        isinstance(record["id"], str) and record["id"] == run_id,
        record["profile"] is None if task else isinstance(record["profile"], str) and record["profile"] != "",
        not task or (isinstance(record["task"], str) and re.fullmatch(TASK_PATTERN, record["task"]) is not None),
        not task or (isinstance(record["label"], str) and 0 < len(record["label"]) <= _LABEL_MAX_CHARS),
        record["source"] is None or isinstance(record["source"], str),
        isinstance(flags, dict) and all(isinstance(key, str) and _is_flag_value(item) for key, item in flags.items()),
        record["started_by"] is None or isinstance(record["started_by"], str),
        isinstance(record["state"], str) and record["state"] in RECORDED_STATES,
        _is_time(record["started"]),
        record["ended"] is None or _is_time(record["ended"]),
        record["exit_code"] is None or (_is_whole_number(record["exit_code"]) and record["exit_code"] in _EXIT_CODE_RANGE),
    )
    if not all(checks):
        return None
    record["flags"] = dict(flags)
    return record


def _read_record(run_fd, run_id):
    """The record of a run folder, or None when the file is missing, is not a regular file, is not JSON or holds a field of the wrong type."""
    raw = _read_small_file(run_fd, _RECORD_NAME, _RECORD_MAX_BYTES)
    if raw is None:
        return None
    try:
        value = json.loads(raw)
    except (ValueError, RecursionError):
        # ValueError: not JSON, not UTF-8, a number too long to convert. RecursionError: nested hundreds of levels deep.
        return None
    return _checked_record(value, run_id)


def _record_bytes(record):
    return (json.dumps(record, indent=2) + "\n").encode("utf-8")


def _record_payload(record):
    """The bytes of a record file. Raises ValueError for a record larger than a record is read back: written, it would make its run unreadable. The record of a run that has not ended is measured as it will be once its end is written (state, end time and exit code in their longest form), so a run whose record was accepted can always be recorded as ended."""
    payload = _record_bytes(record)
    largest = payload if record["state"] in FINAL_STATES else _record_bytes({**record, **_LONGEST_END})
    if len(largest) > _RECORD_MAX_BYTES:
        raise ValueError("the record is larger than a record may be")
    return payload


def _write_record(run_fd, record):
    """Replace the record of a run folder in one step: the new content is written to a temporary file in the same folder, which then takes the record's name, so a reader sees the old record or the new one and never a part of either. Raises ValueError, before anything is written, for a record that is too large."""
    payload = _record_payload(record)
    temporary = f"{_RECORD_NAME}.{secrets.token_hex(4)}.tmp"
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, _FILE_MODE, dir_fd=run_fd)
    try:
        try:
            _write_all(fd, payload)
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(temporary, _RECORD_NAME, src_dir_fd=run_fd, dst_dir_fd=run_fd)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(temporary, dir_fd=run_fd)
        raise


def _create_run(root, fields, locked):
    """Create the folder and the record of a new run, in state `starting`: (id, descriptor that holds the run's lock or None). With `locked` the lock is taken before the record is written."""
    template = {
        "id": None, "profile": fields.get("profile"), "source": fields.get("source"), "flags": fields.get("flags", {}), "started_by": fields.get("started_by"),
        "state": STATE_STARTING, "started": _now(), "ended": None, "exit_code": None,
    }
    if _is_task_record(fields):
        template.update({name: fields.get(name) for name in TASK_FIELDS})
    if _checked_record({**template, "id": ""}, "") is None:
        raise ValueError("the fields of the run are not what a record can hold")
    # Checked with an id of the real length, before the run's folder is made.
    _record_payload({**template, "id": _PLACEHOLDER_ID})
    with _os_errors_refused(), _folder(os.fspath(root), (_STORE_FOLDER, _RUNS_FOLDER), create=True) as runs_fd:
        while True:
            run_id = new_run_id()
            try:
                os.mkdir(run_id, _FOLDER_MODE, dir_fd=runs_fd)
            except FileExistsError:
                continue
            break
        run_fd = _open_folder(run_id, dir_fd=runs_fd)
        if run_fd is None:
            raise StoreError(STORE_NOT_USABLE)
        lock_fd = None
        try:
            if locked:
                lock_fd = _take_run_lock(run_fd)
                if lock_fd is None:
                    raise StoreError(STORE_NOT_USABLE)
            _write_record(run_fd, {**template, "id": run_id})
        except BaseException:
            if lock_fd is not None:
                os.close(lock_fd)
            raise
        finally:
            os.close(run_fd)
    return run_id, lock_fd


def create_run(root, fields):
    """Create the folder and the record of a new run, in state `starting`, and return its id. `fields` holds `profile` (or, for a task, `task` and `label` and no profile), and may hold `source`, `flags` (the request's fields that are not secret: text, whole numbers up to 2**53, booleans and lists of those or of flat records, by name) and `started_by`. Raises ValueError, before anything is created, for fields a record cannot hold and for a record that would be larger than 64 KiB once the run's end is written into it."""
    return _create_run(root, fields, False)[0]


def create_run_locked(root, fields):
    """Create a new run as `create_run` does and return (id, descriptor that holds the run's exclusive lock). The lock is taken before the record is written, so there is no moment in which the record exists and nobody holds the lock: such a run would read as `lost`. The caller hands the descriptor to the process that supervises the run (it is close-on-exec, so it is passed on by name, with `pass_fds`) and closes its own copy; the lock is held until the last copy is closed."""
    return _create_run(root, fields, True)


def read_record(root, run_id):
    """The record of a recorded run as it was written, or None when it cannot be used. Raises RunNotFound when the run has no folder."""
    with _os_errors_refused(), _run_folder(root, run_id) as run_fd:
        return _read_record(run_fd, run_id)


def _update_record(run_fd, run_id, changes):
    unknown = set(changes) - set(RECORD_FIELDS[1:])
    if unknown:
        raise ValueError("a record has no such field")
    record = _read_record(run_fd, run_id)
    if record is None:
        raise StoreError(RECORD_NOT_READABLE)
    updated = _checked_record({**record, **changes}, run_id)
    if updated is None:
        raise ValueError("the changes are not what a record can hold")
    _write_record(run_fd, updated)
    return updated


def update_record(root, run_id, **changes):
    """Change fields of a run's record (any but `id`) and return the new record. The caller is the one writer of the record at that time: the process that supervises the run, or one that holds the run's lock. Raises ValueError, with the record left as it was, for a change a record cannot hold or that makes it larger than 64 KiB (for a run that has not ended: larger than that once its end is written); StoreError when the record cannot be read."""
    with _os_errors_refused(), _run_folder(root, run_id) as run_fd:
        return _update_record(run_fd, run_id, changes)


def end_run(root, run_id, state, exit_code):
    """Write the end of a run into its record: `state` is `finished` or `cancelled`, the end time is now, `exit_code` a whole number from -255 to 255 or None when it is not known. For the one writer of the record, as `update_record`, whose errors it raises. A record that already holds an end is left as it is: RunAlreadyEnded."""
    if state not in FINAL_STATES:
        raise ValueError("a run ends as finished or cancelled")
    with _os_errors_refused(), _run_folder(root, run_id) as run_fd:
        record = _read_record(run_fd, run_id)
        if record is not None and record["state"] in FINAL_STATES:
            raise RunAlreadyEnded(RUN_ENDED)
        return _update_record(run_fd, run_id, {"state": state, "ended": _now(), "exit_code": exit_code})


def open_run_log(root, run_id):
    """Open the log of a recorded run for appending, created with mode 0600 when it is missing, and return the descriptor. The caller closes it."""
    with _os_errors_refused(), _run_folder(root, run_id) as run_fd:
        return _open_for_writing(_LOG_NAME, run_fd, os.O_WRONLY | os.O_APPEND)


# ── locks and the cancel request ────────────────────────────────────────────


def _flock(fd, operation):
    """Try to take a lock without waiting; True when it was taken."""
    try:
        fcntl.flock(fd, operation | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    return True


def _take_run_lock(run_fd):
    """Take the exclusive lock of a run folder: a descriptor that holds it, or None when another process does. The descriptor is closed on every way out but the first, an error or an interrupt during the tries included."""
    fd = _open_for_writing(_LOCK_NAME, run_fd, os.O_RDWR)
    try:
        for attempt in range(_LOCK_ATTEMPTS):
            if _flock(fd, fcntl.LOCK_EX):
                return fd
            if attempt + 1 < _LOCK_ATTEMPTS:
                time.sleep(_LOCK_RETRY_SECONDS)
    except BaseException:
        os.close(fd)
        raise
    os.close(fd)
    return None


def _lock_is_free(run_fd):
    """Whether nobody holds the run's lock. A shared lock is taken for an instant and released, so two readers never see each other as the holder; a missing lock file is a free lock and is not created."""
    try:
        fd = os.open(_LOCK_NAME, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=run_fd)
    except FileNotFoundError:
        return True
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise OSError(errno.EINVAL, "the lock is not a regular file")
        return _flock(fd, fcntl.LOCK_SH)
    finally:
        os.close(fd)


def acquire_run_lock(root, run_id):
    """Take the exclusive lock of a recorded run, which the process that supervises the run holds for as long as it lives. Returns a descriptor that holds the lock until it is closed (or its process ends), or None when another process holds it. It does not wait for a holder: it only tries a few times within a quarter of a second, because a reader of the run's state holds the lock for an instant."""
    with _os_errors_refused(), _run_folder(root, run_id) as run_fd:
        return _take_run_lock(run_fd)


def holds_run_lock(root, run_id, fd):
    """Whether `fd` is an open descriptor of the lock file of a recorded run through which the run's exclusive lock is held. For a process that was handed the descriptor by the one that created the run. False for a descriptor of any other file, for one that is not open, when nobody holds the lock (it is not taken here), and when another descriptor holds it."""
    with _os_errors_refused(), _run_folder(root, run_id) as run_fd:
        try:
            ours = os.fstat(fd)
            named = os.stat(_LOCK_NAME, dir_fd=run_fd, follow_symlinks=False)
            if not stat.S_ISREG(ours.st_mode) or (ours.st_dev, ours.st_ino) != (named.st_dev, named.st_ino):
                return False
            # Someone holds the lock when a second descriptor cannot share it, and asking for it again through this descriptor changes nothing exactly when this descriptor is that holder.
            return not _lock_is_free(run_fd) and _flock(fd, fcntl.LOCK_EX)
        except (OSError, TypeError, ValueError, OverflowError):
            return False


def release_run_lock(fd):
    """Give up a lock that `acquire_run_lock` returned."""
    os.close(fd)


@contextlib.contextmanager
def store_lock(root):
    """Hold the lock of the whole store for the length of a `with` block, waiting for another holder to finish. For a decision that must look at several runs at once."""
    with _os_errors_refused(), _folder(os.fspath(root), (_STORE_FOLDER,), create=True) as store_fd:
        fd = _open_for_writing(_LOCK_NAME, store_fd, os.O_RDWR)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
        except BaseException:
            os.close(fd)
            raise
    try:
        yield
    finally:
        os.close(fd)


def cancel_requested(root, run_id):
    """Whether a cancel was asked for a recorded run."""
    with _os_errors_refused(), _run_folder(root, run_id) as run_fd:
        return _cancel_requested(run_fd)


def _cancel_requested(run_fd):
    try:
        os.stat(_CANCEL_NAME, dir_fd=run_fd, follow_symlinks=False)
    except FileNotFoundError:
        return False
    return True


def _request_cancel(run_fd):
    os.close(_open_for_writing(_CANCEL_NAME, run_fd, os.O_WRONLY))


# ── Docker ──────────────────────────────────────────────────────────────────


def _docker(args, *, timeout, merge_stderr=False, attached=False, into=None, until=None):
    """Run `docker <args>` and return (exit code, standard output as text). The one place the store runs Docker. Standard error is dropped, or with merge_stderr joined into the output (a container's log has both); `attached` gives the command this process's own output and waits for it without a time limit, for following a log on a terminal. With `until`, a function without arguments, the command is one that lasts as long as a container lives: it runs without a time limit, `until` is asked every DOCKER_WATCH_SECONDS, and as soon as it answers true the command is ended and the exit code returned is None; `into` is a descriptor that then receives the command's output instead of it being returned. Raises DockerUnavailable when the binary is missing or the call runs out of time."""
    command = ["docker", *args]
    try:
        if until is not None:
            code, output = _run_until(command, merge_stderr, into, until)
        elif attached:
            return subprocess.run(command, stdin=subprocess.DEVNULL).returncode, ""
        else:
            result = subprocess.run(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT if merge_stderr else subprocess.DEVNULL, timeout=timeout)
            code, output = result.returncode, result.stdout
    except (OSError, subprocess.SubprocessError):
        raise DockerUnavailable(DOCKER_NOT_REACHABLE) from None
    if isinstance(output, bytes):
        output = output.decode("utf-8", "replace")
    return code, output if isinstance(output, str) else ""


def _run_until(command, merge_stderr, into, until):
    """Run a command until it ends by itself or `until` answers true: (exit code, output), the exit code None when it was ended here. The process is never left behind: it is killed and waited for on every way out, an error of `until` included."""
    with subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE if into is None else into, stderr=subprocess.STDOUT if merge_stderr else subprocess.DEVNULL) as process:
        try:
            while True:
                try:
                    output, _ = process.communicate(timeout=DOCKER_WATCH_SECONDS)
                except subprocess.TimeoutExpired:
                    if until():
                        return None, b""
                    continue
                return process.returncode, output
        finally:
            if process.poll() is None:
                process.kill()


def _containers(root, labels, every_state=False):
    """The running containers that carry every one of the labels and were started by compose in this deployment's folder, as (container id, service, run label). With `every_state` the containers that are not running are listed too, and each entry has a fourth item, the state as Docker names it. The folder a container names must be an absolute path (a relative one would be read against this process's working directory) and is compared after resolving links on both sides. A line that is not what the format prints is passed over. Raises DockerUnavailable when Docker does not answer the listing."""
    filters = [argument for label in labels for argument in ("--filter", f"label={label}")]
    code, output = _docker(["ps", *(["-a"] if every_state else []), "--no-trunc", *filters, "--format", PS_STATE_FORMAT if every_state else PS_FORMAT], timeout=DOCKER_READ_TIMEOUT)
    if code != 0:
        raise DockerUnavailable(DOCKER_NOT_REACHABLE)
    here = os.path.realpath(root)
    found = []
    for line in output.split("\n"):
        try:
            fields = json.loads(line)
        except (ValueError, RecursionError):
            continue
        if not isinstance(fields, list) or len(fields) != (5 if every_state else 4) or not all(isinstance(item, str) for item in fields):
            continue
        container, working_dir, service, run = fields[:4]
        if not re.fullmatch(_CONTAINER_ID_PATTERN, container) or not os.path.isabs(working_dir):
            continue
        try:
            if os.path.realpath(working_dir) != here:
                continue
        except (OSError, ValueError):
            continue
        found.append((container, service, run, *fields[4:]))
    return found


def _terminal_containers(root, service_profiles):
    """The containers of the runs started in a terminal, as (container id, profile): compose started them with `run` in this deployment's folder for one of the pipeline services, and they carry no run label (a container with one belongs to a recorded run). Any other one-off container, such as a hand-run command in a database service, is not a run."""
    return [(container, service_profiles[service]) for container, service, run in _containers(root, (ONEOFF_LABEL,)) if not run and service in service_profiles]


def _terminal_container(root, service_profiles, run_id):
    """The container of a terminal run, looked up in the current listing, so that an id never addresses anything but a listed run of this deployment. Raises RunNotFound."""
    prefix = run_id[len("live-"):]
    matches = [container for container, _ in _terminal_containers(root, service_profiles) if container.startswith(prefix)]
    if len(matches) != 1:
        raise RunNotFound(RUN_NOT_FOUND)
    return matches[0]


def _labelled_containers(root, run_id):
    """The running containers that carry the label of a recorded run and that compose started with `run`, as it starts every run."""
    return [container for container, _, run in _containers(root, (ONEOFF_LABEL, f"{RUN_LABEL_NAME}={run_id}")) if run == run_id]


def _inspect_terminal(container):
    """The start time and the source of a container, as (ISO-8601 UTC to the second or None, text or None), read with one inspect call; None when the container is gone. The answer is taken apart strictly: the template prints one line, a time and then nothing or `SOURCE=<value>`. A first word that is no time gives (None, None), and anything else after the time, a second line included, gives no source, so an answer that held more of the environment than the one variable would not be passed on."""
    code, output = _docker(["inspect", "--format", INSPECT_TEMPLATE, container], timeout=DOCKER_READ_TIMEOUT)
    if code != 0:
        return None
    if output.endswith("\n"):
        output = output[:-1]
    started_text, _, rest = output.partition(" ")
    started = re.fullmatch(_DOCKER_TIME_PATTERN, started_text)
    if started is None:
        return None, None
    source = rest[len("SOURCE="):] if rest.startswith("SOURCE=") and "\n" not in rest else ""
    return started.group(1) + "Z", source or None


def _end_of_text(text, limit):
    """The end of text that is at most `limit` bytes of UTF-8 and begins with a whole character."""
    data = text.encode("utf-8", "replace")
    if len(data) <= limit:
        return text
    start = len(data) - limit
    while start < len(data) and data[start] & 0xC0 == 0x80:
        start += 1
    return data[start:].decode("utf-8", "replace")


def _container_log_tail(container, limit):
    """The last lines of a container's output, standard output and standard error together, cut to its last `limit` bytes; None when the container is gone."""
    code, output = _docker(["logs", "--tail", str(LOG_TAIL_LINES), container], timeout=DOCKER_READ_TIMEOUT, merge_stderr=True)
    return _end_of_text(output, limit) if code == 0 else None


def _stop(container):
    """Send the interrupt signal to a container and wait for it to stop; True when Docker stopped it."""
    code, _ = _docker(["stop", "--signal", STOP_SIGNAL, "-t", str(STOP_GRACE_SECONDS), container], timeout=DOCKER_STOP_TIMEOUT)
    return code == 0


# ── the containers of a recorded run, for the process that supervises it ─────


def count_run_containers(root, run_id):
    """How many running containers carry the label of a recorded run. Raises RunIdNotValid before Docker is asked, DockerUnavailable when it does not answer."""
    _require_recorded(run_id)
    return len(_labelled_containers(os.fspath(root), run_id))


def stop_run_containers(root, run_id):
    """Send the interrupt signal to every container that carries the label of a recorded run, and return (how many were listed, how many are still listed afterwards). What a stop answered is not taken on trust: the second number comes from a new listing, which is not made when there was nothing to stop. Raises RunIdNotValid before Docker is asked, DockerUnavailable when a listing is not answered."""
    _require_recorded(run_id)
    root = os.fspath(root)
    containers = _labelled_containers(root, run_id)
    for container in containers:
        _stop(container)
    return len(containers), len(_labelled_containers(root, run_id)) if containers else 0


def clear_run_containers(root, run_id):
    """The last look at the containers of a recorded run before its end is written. Every container that carries the run's label is listed, whatever its state; one that was created and never started is removed, with its full id and without force, so that one that has started meanwhile is not removed (it is then stopped as a running one); returns how many containers are still listed, in any state, after that. 0 is the only answer on which an end is written: a container that is created can still be started, and one that is listed as anything else has not gone yet. Raises RunIdNotValid before Docker is asked, DockerUnavailable when a listing is not answered."""
    _require_recorded(run_id)
    root = os.fspath(root)
    labels = (ONEOFF_LABEL, f"{RUN_LABEL_NAME}={run_id}")
    listed = [(container, state) for container, _, run, state in _containers(root, labels, every_state=True) if run == run_id]
    created = [container for container, state in listed if state == CONTAINER_CREATED]
    for container in created:
        _docker(["rm", container], timeout=DOCKER_READ_TIMEOUT)
    if not created:
        return len(listed)
    return len([container for container, _, run, _ in _containers(root, labels, every_state=True) if run == run_id])


def wait_run_container(root, run_id, until):
    """Wait for the container of a recorded run to end and return its exit code (0 to 255). The wait has no time limit: `until`, a function without arguments, is asked about twice a second and ends it by answering true. None when no container carries the run's label, when the wait was ended by `until`, and when Docker's answer is not an exit code (the container was gone before the wait reached it, the daemon stopped). Raises RunIdNotValid before Docker is asked, DockerUnavailable when the listing is not answered."""
    _require_recorded(run_id)
    containers = _labelled_containers(os.fspath(root), run_id)
    if not containers:
        return None
    code, output = _docker(["wait", containers[0]], timeout=None, until=until)
    answer = re.fullmatch(_WAIT_ANSWER_PATTERN, output) if code == 0 else None
    if answer is None or int(answer.group(1)) > 255:
        return None
    return int(answer.group(1))


def follow_run_output(root, run_id, fd, since, until):
    """Write what the container of a recorded run printed from the time `since` on (seconds since the epoch), standard output and standard error together, into the descriptor `fd`, and keep writing what follows until the container ends or `until`, a function without arguments that is asked about twice a second, answers true. True when Docker followed the output to the container's end. False when no container carries the run's label, when the following was ended by `until` and when Docker refused it. Raises ValueError for a time that is not a number, RunIdNotValid before Docker is asked, DockerUnavailable when the listing is not answered."""
    _require_recorded(run_id)
    if isinstance(since, bool) or not isinstance(since, (int, float)) or not 0 <= since < float("inf"):
        raise ValueError("the time is not a number of seconds")
    containers = _labelled_containers(os.fspath(root), run_id)
    if not containers:
        return False
    code, _ = _docker(["logs", "--follow", "--since", f"{since:.3f}", containers[0]], timeout=None, merge_stderr=True, into=fd, until=until)
    return code == 0


# ── runs ────────────────────────────────────────────────────────────────────


def is_task(entry):
    """Whether a record or an entry of a listing is that of a task."""
    return entry.get("task") is not None


def _unreadable_entry(run_id):
    return {"id": run_id, "kind": KIND_RECORDED, "profile": None, "source": None, "flags": None, "started_by": None, "state": STATE_UNREADABLE, "started": None, "ended": None, "exit_code": None}


def _recorded_entry(run_fd, run_id, containers_of):
    """The entry of a recorded run: its record, with the state computed for a run that has not ended. While the run's lock is held, the process that supervises it is alive and the recorded state stands. With the lock free that process is gone: the run is `detached` when a container carries its label and `lost` when none does, `unknown` when Docker cannot be asked. A task has no container of its own (what it does runs in its supervisor's child), so with the lock free it is `lost`, and Docker is not asked. `containers_of(run_id)` returns the container ids, or None for "cannot be asked"; it is not called for a run that has ended, whose lock is held, or that is a task."""
    record = _read_record(run_fd, run_id)
    if record is None:
        return _unreadable_entry(run_id)
    task = is_task(record)
    entry = {"id": run_id, "kind": KIND_TASK if task else KIND_RECORDED, **{name: record[name] for name in RECORD_FIELDS[1:] + (TASK_FIELDS if task else ())}}
    if record["state"] in FINAL_STATES:
        return entry
    try:
        if not _lock_is_free(run_fd):
            return entry
    except OSError:
        return _unreadable_entry(run_id)
    if task:
        entry["state"] = STATE_LOST
        return entry
    containers = containers_of(run_id)
    entry["state"] = STATE_UNKNOWN if containers is None else STATE_DETACHED if containers else STATE_LOST
    return entry


def _terminal_entry(container, profile, started, source):
    return {"id": "live-" + container[:12], "kind": KIND_TERMINAL, "profile": profile, "source": source, "flags": None, "started_by": None, "state": STATE_RUNNING, "started": started, "ended": None, "exit_code": None}


def _containers_or_none(root, run_id):
    try:
        return _labelled_containers(root, run_id)
    except DockerUnavailable:
        return None


def read_run(root, run_id):
    """The entry of one recorded run, as `list_runs` gives it. Raises RunNotFound when the run has no folder."""
    with _os_errors_refused(), _run_folder(root, run_id) as run_fd:
        return _recorded_entry(run_fd, run_id, lambda wanted: _containers_or_none(os.fspath(root), wanted))


def list_recorded_runs(root, limit):
    """The recorded runs alone, newest first and at most `limit` of them, as `list_runs` gives them, without asking Docker: a pipeline run that has not ended and has no live supervisor is `unknown`. A task's state never needs Docker, so for the tasks this is the whole truth. Raises StoreError only when the state folder itself is a link or cannot be opened."""
    return _recorded_runs(os.fspath(root), limit, lambda run_id: None)


def _recorded_runs(root, limit, containers_of):
    recorded = []
    with _os_errors_refused(), _folder(root, (_STORE_FOLDER, _RUNS_FOLDER)) as runs_fd:
        if runs_fd is not None:
            run_ids = sorted((name for name in os.listdir(runs_fd) if re.fullmatch(RUN_ID_PATTERN, name)), reverse=True)
            for run_id in run_ids[:limit]:
                try:
                    run_fd = _open_folder(run_id, dir_fd=runs_fd)
                except StoreError:
                    recorded.append(_unreadable_entry(run_id))
                    continue
                if run_fd is None:
                    continue
                try:
                    recorded.append(_recorded_entry(run_fd, run_id, containers_of))
                except OSError:
                    recorded.append(_unreadable_entry(run_id))
                finally:
                    os.close(run_fd)
    return recorded


def list_runs(root, service_profiles, limit):
    """The runs of the deployment: `{"docker_available": bool, "runs": [...]}`, recorded runs newest first and at most `limit` of them, then the runs started in a terminal that are alive now, oldest first. `service_profiles` maps the compose service of each pipeline profile to the profile.

    Each run is a dict with `id`, `kind` (`web` for a recorded pipeline run, `task` for a recorded task, `terminal`), `profile`, `source`, `flags`, `started_by`, `state`, `started`, `ended` and `exit_code`; a task's entry also has `task` (the verb) and `label`, and its profile is None. A terminal run is `running`, its source is the container's `SOURCE` variable, and it has no flags, no starter, no end and no exit code; one whose container Docker lists but does not describe has no start time and no source. When Docker cannot be reached nothing is raised: `docker_available` is false, no terminal run is listed, and a recorded run that has not ended and has no live supervisor is `unknown`. A run folder that cannot be used is listed as `unreadable`. Raises StoreError only when the state folder itself is a link or cannot be opened.
    """
    root = os.fspath(root)
    docker = {"available": True, "labelled": None}
    terminal = []
    try:
        unanswered = []
        for container, profile in _terminal_containers(root, service_profiles):
            facts = _inspect_terminal(container)
            if facts is None:
                unanswered.append((container, profile))
            else:
                terminal.append(_terminal_entry(container, profile, *facts))
        if unanswered:
            # An inspect that fails means "the container is gone" only while Docker answers at all: a daemon that stopped after the listing fails every inspect the same way. Asked for the listing once more, such a daemon is found out of reach; a container that ended is no longer listed; one that still is listed is a run whose start time and source could not be read.
            listed = {container for container, _ in _terminal_containers(root, service_profiles)}
            terminal.extend(_terminal_entry(container, profile, None, None) for container, profile in unanswered if container in listed)
    except DockerUnavailable:
        docker["available"] = False
    terminal.sort(key=lambda entry: (entry["started"] or "", entry["id"]))

    def containers_of(run_id):
        # One listing for all recorded runs that need it, made when the first one does.
        if docker["available"] and docker["labelled"] is None:
            try:
                docker["labelled"] = {}
                for container, _, run in _containers(root, (ONEOFF_LABEL, RUN_LABEL_NAME)):
                    docker["labelled"].setdefault(run, []).append(container)
            except DockerUnavailable:
                docker["available"] = False
        return docker["labelled"].get(run_id, []) if docker["available"] else None

    recorded = _recorded_runs(root, limit, containers_of)
    # With Docker out of reach at any point, the first call or a later one, no terminal run is listed: a part of them next to "Docker cannot be reached" would read as all of them.
    return {"docker_available": docker["available"], "runs": recorded + (terminal if docker["available"] else [])}


# ── logs ────────────────────────────────────────────────────────────────────


def _cut_character(chunk):
    """Where the UTF-8 character that the end of chunk cuts off begins, and how many bytes that character has: (index, length). (len(chunk), 0) when chunk ends between two characters, or in bytes that are no part of a character."""
    end = len(chunk)
    for back in range(1, min(3, end) + 1):
        byte = chunk[end - back]
        if byte & 0xC0 == 0x80:
            continue
        if byte >= 0xC0:
            length = 2 if byte < 0xE0 else 3 if byte < 0xF0 else 4 if byte < 0xF8 else 1
            if length > back:
                return end - back, length
        break
    return end, 0


def _piece_end(data, limit, ended):
    """How many bytes of data, which was read `limit` bytes plus three ahead, make the piece: at most `limit`, less when the piece would end inside a character. That character goes into the next piece, or, when it is all this piece could hold, whole into this one. Where the file itself ends inside a character, the bytes wait for the rest of it while the log is still written, and are given as they are once the run has ended."""
    size = min(limit, len(data))
    start, length = _cut_character(data[:size])
    if start == size:
        return size
    if len(data) - start < length:
        return len(data) if ended else start
    if start > 0:
        return start
    # The first bytes of a character are all the limit leaves room for: the piece is that character. Bytes that only look like one (a lead byte followed by the start of another character) end where the next character begins.
    return _cut_character(data[:length])[0]


def _read_log_piece(run_fd, offset, limit, ended):
    """A piece of a run's log file as (text, offset of the byte after it). A missing file is an empty log. Raises StoreError when a link or anything but a regular file is in the log's place."""
    try:
        fd = os.open(_LOG_NAME, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=run_fd)
    except FileNotFoundError:
        return "", 0
    except OSError:
        raise StoreError(LOG_NOT_A_FILE) from None
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise StoreError(LOG_NOT_A_FILE)
        if offset >= info.st_size:
            return "", info.st_size
        data = b""
        while len(data) < limit + 3:
            chunk = os.pread(fd, limit + 3 - len(data), offset + len(data))
            if not chunk:
                break
            data += chunk
    finally:
        os.close(fd)
    end = _piece_end(data, limit, ended)
    return data[:end].decode("utf-8", "replace"), offset + end


def read_run_log_bytes(root, run_id, offset, count):
    """The bytes of a recorded run's log file as they are, for a caller that must look at the bytes themselves (the broker overwrites secrets in a stretch around a piece and then cuts the piece with `log_piece_end`): (at most `count` bytes from `offset`, fewer at the file's end; the size of the file when it was read). A missing file is an empty log. The file is opened by the store's rules: never through a link, never waiting, a regular file only. Raises ValueError for an offset or count that is not a whole number of at least 0; RunIdNotValid; RunNotFound; StoreError when a link or anything but a regular file is in the log's place."""
    if not _is_whole_number(offset) or not _is_whole_number(count) or offset < 0 or count < 0:
        raise ValueError("the offset and the count are whole numbers, at least 0")
    with _os_errors_refused(), _run_folder(root, run_id) as run_fd:
        try:
            fd = os.open(_LOG_NAME, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=run_fd)
        except FileNotFoundError:
            return b"", 0
        except OSError:
            raise StoreError(LOG_NOT_A_FILE) from None
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode):
                raise StoreError(LOG_NOT_A_FILE)
            data = bytearray()
            while len(data) < count:
                chunk = os.pread(fd, count - len(data), offset + len(data))
                if not chunk:
                    break
                data += chunk
        finally:
            os.close(fd)
    return bytes(data), info.st_size


def log_piece_end(data, limit, ended):
    """How many bytes of `data` (read from a log `limit` bytes plus three ahead) make the piece that `read_run_log` would give: at most `limit`, less where the piece would end inside a character, and the bytes of a character the file ends inside only once the run has `ended`. The cut of `read_run_log`, for a caller that reads the bytes itself."""
    return _piece_end(data, limit, ended)


def read_run_log(root, service_profiles, run_id, offset, limit):
    """A piece of a run's output: `{"id", "kind", "state", "text", "next_offset", "complete"}`.

    For a recorded run the text comes from its log file: `offset` and `limit` are byte positions, the piece never ends inside a multi-byte character, bytes that are not UTF-8 become the replacement character, `next_offset` is where the next piece starts (the pieces read one after the other are the whole log) and `complete` is true. An offset past the end gives an empty text and the end as `next_offset`. For a terminal run, and for a recorded run that is `detached` (its log file stopped with the process that wrote it), the text is the last 500 lines Docker has, standard output and standard error together, and of those the last `limit` bytes, beginning with a whole character; `offset` does not apply, `next_offset` is None and `complete` is false. The text is returned as it was decoded, control characters included: the caller renders it.

    Raises RunIdNotValid; RunNotFound, also for a terminal run whose container ended before its output was read (a detached run whose container ended is answered from its log file, as `lost`); DockerUnavailable for a terminal or detached run with Docker out of reach, at the listing or at the reading; StoreError with its own constant message when Docker lists the container and does not hand over its output, and for a log that is not a regular file.
    """
    kind = run_id_kind(run_id)
    root = os.fspath(root)
    if kind == KIND_TERMINAL:
        container = _terminal_container(root, service_profiles, run_id)
        text = _container_log_tail(container, limit)
        if text is None:
            # Docker did not hand the output over. Asked for the listing once more: a daemon that stopped is found out of reach, a container that is still listed is one whose output cannot be read, and one that is not has ended.
            if any(listed == container for listed, _ in _terminal_containers(root, service_profiles)):
                raise StoreError(OUTPUT_NOT_READABLE)
            raise RunNotFound(RUN_NOT_FOUND)
        return {"id": run_id, "kind": kind, "state": STATE_RUNNING, "text": text, "next_offset": None, "complete": False}

    with _os_errors_refused(), _run_folder(root, run_id) as run_fd:
        seen = {}

        def containers_of(wanted):
            seen["containers"] = _labelled_containers(root, wanted)
            return seen["containers"]

        try:
            entry = _recorded_entry(run_fd, run_id, containers_of)
            state, kind = entry["state"], entry["kind"]
        except DockerUnavailable:
            state = STATE_UNKNOWN
        if state == STATE_DETACHED:
            text = _container_log_tail(seen["containers"][0], limit)
            if text is not None:
                return {"id": run_id, "kind": kind, "state": state, "text": text, "next_offset": None, "complete": False}
            # As for a terminal run: out of reach, not readable, or ended. A run whose container ended while nobody supervises it is lost, and what is left of its output is its log file.
            if _labelled_containers(root, run_id):
                raise StoreError(OUTPUT_NOT_READABLE)
            state = STATE_LOST
        text, next_offset = _read_log_piece(run_fd, offset, limit, ended=state not in (STATE_STARTING, STATE_RUNNING))
        return {"id": run_id, "kind": kind, "state": state, "text": text, "next_offset": next_offset, "complete": True}


def follow_run_log(root, service_profiles, run_id, tail):
    """Print the last `tail` lines of the output of a terminal run, or of a recorded run that is detached, to this process's own output and keep printing what follows, until the container stops. For a terminal. Returns Docker's exit code. Raises RunIdNotValid, RunNotFound or DockerUnavailable."""
    kind = run_id_kind(run_id)
    root = os.fspath(root)
    if kind == KIND_TERMINAL:
        container = _terminal_container(root, service_profiles, run_id)
    else:
        containers = _labelled_containers(root, run_id)
        if not containers:
            raise RunNotFound(RUN_NOT_FOUND)
        container = containers[0]
    code, _ = _docker(["logs", "--follow", "--tail", str(int(tail)), container], timeout=None, attached=True)
    return code


# ── cancel ──────────────────────────────────────────────────────────────────


def cancel_run(root, service_profiles, run_id):
    """Cancel a run and return what was done.

    A terminal run is looked up in the current listing and its container is sent the interrupt signal: CANCEL_STOPPED. For a recorded run, while another process holds the run's lock (the one that supervises the run, or another cancel that is at work), the `cancel` file is written and that process does the stopping (CANCEL_REQUESTED). With the lock free this call takes it; if the run has still not ended, the `cancel` file is written, the container that carries the run's label is sent the interrupt signal, and once no such container is listed any more the record is written as `cancelled` (CANCEL_STOPPED, or CANCEL_RECORDED when no container was left to stop). Asking again does no harm.

    Raises RunIdNotValid; RunNotFound; DockerUnavailable when Docker is needed and cannot be reached; StoreError with its own constant message when no container of the run is left (stopped by this call, or gone before it) and the record then could not be written; CancelRefused in these cases. A recorded run that is `finished`, `cancelled`, `lost` or `unreadable`, also when it ended between the look at its state and the taking of its lock: nothing is stopped, and neither the `cancel` file nor the record is written (taking the lock creates the lock file when the run never had one). A container that is still listed after the stop: for a recorded run the record stays as it was, so the run can be cancelled again. A terminal run that ended before the signal reached it.
    """
    kind = run_id_kind(run_id)
    root = os.fspath(root)
    if kind == KIND_TERMINAL:
        container = _terminal_container(root, service_profiles, run_id)
        if _stop(container):
            return CANCEL_STOPPED
        # Docker did not stop it: either the container ended by itself first, or it is still there.
        if any(listed == container for listed, _ in _terminal_containers(root, service_profiles)):
            raise CancelRefused(STOP_FAILED)
        raise CancelRefused(RUN_ENDED)

    with _os_errors_refused(), _run_folder(root, run_id) as run_fd:
        state = _recorded_entry(run_fd, run_id, lambda wanted: _containers_or_none(root, wanted))["state"]
        if state == STATE_UNREADABLE:
            raise CancelRefused(RECORD_NOT_READABLE)
        if state in FINAL_STATES:
            raise CancelRefused(RUN_ENDED)
        if state == STATE_LOST:
            raise CancelRefused(RUN_LOST)
        lock_fd = _take_run_lock(run_fd)
        if lock_fd is None:
            _request_cancel(run_fd)
            return CANCEL_REQUESTED
        try:
            # The lock is ours, so no supervisor is alive. It may have ended the run between the look at the state and now.
            record = _read_record(run_fd, run_id)
            if record is None:
                raise CancelRefused(RECORD_NOT_READABLE)
            if record["state"] in FINAL_STATES:
                raise CancelRefused(RUN_ENDED)
            if is_task(record):
                # A task's supervisor ended between the look and now without recording an end: the task is lost, as a look made now would say, and nothing is written.
                raise CancelRefused(RUN_LOST)
            containers = _labelled_containers(root, run_id)
            _request_cancel(run_fd)
            for container in containers:
                _stop(container)
            # What a stop answered is not taken on trust: the run is cancelled when no container carries its label any more. A record written before that would make the run final and out of reach of a second cancel.
            if containers and _labelled_containers(root, run_id):
                raise CancelRefused(STOP_FAILED)
            try:
                _update_record(run_fd, run_id, {"state": STATE_CANCELLED, "ended": _now()})
            except (ValueError, OSError):
                # No container of the run is left by now (stopped here, or gone before), and the caller is told so. ValueError: a record that this store wrote always has room for its end, one that did not come from it may not. OSError: the write itself failed (a full disk, a file system that is read-only).
                raise StoreError(RECORD_NOT_WRITTEN) from None
            return CANCEL_STOPPED if containers else CANCEL_RECORDED
        finally:
            os.close(lock_fd)
