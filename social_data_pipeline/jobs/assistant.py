"""The assistant's endpoint: a second door on the query scheduler for the chat's assistant, which works in a sandbox whose only network peer is the chat's gateway.

Approving a query makes the scheduler run it with the database's admin login, and text in scraped data can steer the assistant, so what it may do is narrow:

- Where: everything under `/assistant/` of the scheduler's port, in front of every other route of the page (`AssistantGate`, an ASGI middleware): `/assistant/mcp` (a second MCP server, `build_mcp(..., assistant=True)`) and `/assistant/results/<job id>[/<part>]`. Any other path is passed on unchanged.
- Who: a request must carry the header `x-sdp-assistant` whose value equals, compared in constant time, the secret in `<jobs_dir>/control/assistant.secret`: a regular file (opened without following a link and without waiting), at most 256 bytes, of 64 to 256 lowercase hex digits after stripping. `sdp chat start` writes it and gives the gateway a copy; the gateway adds the header to what it forwards on its queue listener. Without the file, with a wrong, repeated or missing header, and for anything not found, the answer is the same 404 as a path the page does not have; refusals are only counted in the log (at the first, second, fourth, eighth one ...). The file is looked at for each request and read again only when its device, inode, size or modification time changed.
- What: the queries it submits carry `submitted_by: "assistant"` (approved by a person, or by `Runner._auto_approve_eligible` like any other query when the target's auto-accept is on); `query_status` and `query_cancel` take only job ids of the scheduler's pattern and only its own jobs, and answer the same "no such job" for any other id; nothing lists other jobs. `GET /assistant/results/<job id>` lists the parts of one of its finished jobs (404 when it has none; `{"parts": [{"index", "name", "size"}]}`, the name reduced to `[A-Za-z0-9._-]`), and `GET /assistant/results/<job id>/<part>` sends one part (`application/octet-stream`, `Content-Length` from `fstat` and never more than that, no ranges, at most two at a time, else 429; at most four listings at a time, else 429). The parts are found as the door's preview finds them: the folder from the checked id under the results root (never the record's `result_path`), opened without following a link; only names that are the job's output file or `<stem>_<digits><ext>`, looked at relative to the folder without following a link (`lstat`): regular files only, links and anything else passed over by their type. Nothing but the folder is opened to list them; a download opens only the part asked for (without following a link and without waiting) and requires it to be a regular file with the device and inode it was listed with. Any other failure of the file system while the parts are found (too many open files, a part that went away) refuses the whole request with a 503, so a part's number never points at another file.
- On which thread: the store is read and changed on the scheduler's event loop, as the page's routes do (the store's transitions take no lock): the gate and the results route are coroutines on the loop, and the assistant's store-touching tools are registered as coroutine functions, which the MCP SDK runs on the loop (it runs a plain function on a worker thread, as for the tools of `/mcp`); a submission does its checks on a worker thread and only its store write on the loop (`mcp_tools._submit_for_assistant`). Only the checks of a submission, reading a folder and the parts' bytes happen on worker threads.
"""

from __future__ import annotations

import asyncio
import contextlib
import errno
import hmac
import json
import logging
import os
import re
import stat

from .door import FOLDER_NAME, JOB_ID, PREVIEW_LISTING_MAX, PREVIEW_PARTS_MAX, _part_names
from .store import ASSISTANT

log = logging.getLogger(__name__)

PREFIX = "/assistant"
MCP_PATH = "/assistant/mcp"
RESULTS_PATH = "/assistant/results/"
HEADER = b"x-sdp-assistant"
SECRET_NAME = "assistant.secret"
SECRET_MAX_BYTES = 256
SECRET = re.compile(rb"[0-9a-f]{64,256}")
DOWNLOADS_MAX = 2
LISTINGS_MAX = 4
CHUNK = 64 * 1024
PART_INDEX = re.compile(r"0|[1-9][0-9]{0,5}")
_UNSAFE = re.compile(r"[^A-Za-z0-9._-]")

# The page's own answer for a path it does not have (FastAPI's), so that a refusal looks like nothing were there.
NOT_FOUND_BODY = b'{"detail":"Not Found"}'
BUSY_BODY = b'{"detail":"Too Many Requests"}'
UNAVAILABLE_BODY = b'{"detail":"Service Unavailable"}'
NO_SUCH_JOB = {"status": "unknown", "error": "no such job"}


# ── the secret ─────────────────────────────────────────────────────────────


class SecretFile:
    """The secret in `<jobs_dir>/control/assistant.secret`, looked at for each request (opened without following a link, as the control folder before it, and without waiting) and read again when the file's identity, size or modification time changed. `current()` is the secret as bytes, or None when there is no usable file."""

    def __init__(self, jobs_dir):
        self.jobs_dir = os.fspath(jobs_dir)
        self._key = None
        self._value = None

    def current(self):
        try:
            folder = os.open(os.path.join(self.jobs_dir, FOLDER_NAME), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        except OSError:
            return None
        try:
            try:
                fd = os.open(SECRET_NAME, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=folder)
            except OSError:
                return None
            try:
                info = os.fstat(fd)
                if not stat.S_ISREG(info.st_mode):
                    return None
                key = (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)
                if key != self._key:
                    data = os.read(fd, SECRET_MAX_BYTES + 1)
                    value = data.strip()
                    self._key, self._value = key, (value if len(data) <= SECRET_MAX_BYTES and SECRET.fullmatch(value) else None)
                return self._value
            finally:
                os.close(fd)
        except OSError:
            return None
        finally:
            os.close(folder)


def header_matches(headers, secret):
    """Whether the request's `headers` (ASGI pairs of bytes) carry exactly one `x-sdp-assistant` whose value equals `secret`, compared in constant time; False without a secret."""
    values = [value for name, value in headers if name.lower() == HEADER]
    if secret is None or len(values) != 1:
        return False
    return hmac.compare_digest(values[0], secret)


# ── the assistant's own jobs ───────────────────────────────────────────────


def own_job(store, job_id):
    """(phase, job) of the job `job_id` when the id has the scheduler's pattern and the job carries the assistant's mark (its record naming the same id), else None. The store is asked only for a well-formed id."""
    if not isinstance(job_id, str) or JOB_ID.fullmatch(job_id) is None:
        return None
    located = store.find(job_id)
    if located is None:
        return None
    phase, job = located
    if job.job_id != job_id or job.submitted_by != ASSISTANT:
        return None
    return phase, job


def status(store, job_id, to_status):
    """The assistant's `query_status`: `to_status(job, phase=...)` for its own job, `NO_SUCH_JOB` for any other id (unknown, malformed or another's alike)."""
    located = own_job(store, job_id)
    if located is None:
        return dict(NO_SUCH_JOB)
    phase, job = located
    return to_status(job, phase=phase)


def cancel(store, job_id, to_status):
    """The assistant's `query_cancel`: cancels its own pending job; `NO_SUCH_JOB` for any other id."""
    located = own_job(store, job_id)
    if located is None:
        return dict(NO_SUCH_JOB)
    phase, _job = located
    if phase == "pending":
        try:
            return to_status(store.cancel_pending(job_id), phase="history")
        except KeyError:
            located = own_job(store, job_id)
            if located is None:
                return dict(NO_SUCH_JOB)
            phase = located[0]
    return {"job_id": job_id, "status": phase, "error": f"cannot cancel: the job is in phase {phase!r}; only a pending job can be cancelled"}


# ── the result parts ───────────────────────────────────────────────────────


class _NotFound(Exception):
    pass


class _Unavailable(Exception):
    """The file system failed while the parts were found: the whole request is refused (503)."""


def _open_folder(folder, output_filename):
    if not isinstance(output_filename, str) or not output_filename or "/" in output_filename or "\0" in output_filename:
        raise _NotFound()
    try:
        return os.open(os.fspath(folder), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except FileNotFoundError:
        raise _NotFound() from None
    except OSError as error:
        if error.errno in (errno.ELOOP, errno.ENOTDIR):
            raise _NotFound() from None
        raise _Unavailable() from None


def _scan(folder_fd, output_filename):
    """[(name, size, (device, inode))] of the parts in the folder `folder_fd`, in order, looked at without opening them."""
    try:
        listed = []
        with os.scandir(folder_fd) as entries:
            for entry in entries:
                listed.append(entry.name)
                if len(listed) > PREVIEW_LISTING_MAX:
                    raise _NotFound()
        parts = []
        for name in _part_names(listed, output_filename)[:PREVIEW_PARTS_MAX]:
            info = os.stat(name, dir_fd=folder_fd, follow_symlinks=False)
            if stat.S_ISREG(info.st_mode):
                parts.append((name, info.st_size, (info.st_dev, info.st_ino)))
        return parts
    except OSError:
        raise _Unavailable() from None


def list_parts(folder, output_filename):
    """[(name, size)] of the parts of a finished job's result folder (see the module's docstring); only the folder is opened. Raises _NotFound or _Unavailable."""
    folder_fd = _open_folder(folder, output_filename)
    try:
        return [(name, size) for name, size, _identity in _scan(folder_fd, output_filename)]
    finally:
        os.close(folder_fd)


def open_part(folder, output_filename, index):
    """(descriptor, size) of the part `index`: the parts are found without opening them, then only that one is opened and must be the regular file that was looked at. Raises _NotFound or _Unavailable; the caller closes the descriptor."""
    folder_fd = _open_folder(folder, output_filename)
    try:
        parts = _scan(folder_fd, output_filename)
        if index >= len(parts):
            raise _NotFound()
        name, _size, identity = parts[index]
        try:
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=folder_fd)
        except OSError:
            raise _Unavailable() from None
    finally:
        os.close(folder_fd)
    try:
        info = os.fstat(fd)
    except OSError:
        os.close(fd)
        raise _Unavailable() from None
    if not stat.S_ISREG(info.st_mode) or (info.st_dev, info.st_ino) != identity:
        os.close(fd)
        raise _Unavailable()
    return fd, info.st_size


def safe_name(name):
    return _UNSAFE.sub("_", name)


# ── the gate ───────────────────────────────────────────────────────────────


async def _answer(send, status_code, body, content_type=b"application/json"):
    await send({"type": "http.response.start", "status": status_code, "headers": [(b"content-type", content_type), (b"content-length", str(len(body)).encode("ascii"))]})
    await send({"type": "http.response.body", "body": body, "more_body": False})


async def not_found(send):
    await _answer(send, 404, NOT_FOUND_BODY)


class AssistantGate:
    """The ASGI middleware in front of the page: requests under `/assistant/` are answered here (see the module's docstring), everything else goes to `app` unchanged. `mcp_app` is the assistant's MCP server's streamable-HTTP app; `secret` a `SecretFile` (tests pass another object with `current()`)."""

    def __init__(self, app, *, store, mcp_app, jobs_dir=None, secret=None):
        self.app = app
        self.store = store
        self.mcp_app = mcp_app
        self.secret = secret if secret is not None else SecretFile(jobs_dir)
        self.downloads = 0
        self.listings = 0
        self.refused = 0

    async def __call__(self, scope, receive, send):
        path = scope.get("path", "") if scope.get("type") == "http" else ""
        if path != PREFIX and not path.startswith(PREFIX + "/"):
            return await self.app(scope, receive, send)
        if not header_matches(scope.get("headers") or [], self.secret.current()):
            self.refused += 1
            if self.refused & (self.refused - 1) == 0:
                log.warning("the assistant's endpoint refused a request without the right secret (%d so far)", self.refused)
            return await not_found(send)
        if path in (MCP_PATH, MCP_PATH + "/"):
            # The MCP app serves its endpoint at its own root, as it does for the page's `/mcp` mount.
            child = {**scope, "path": "/", "raw_path": b"/", "root_path": scope.get("root_path", "") + MCP_PATH}
            return await self.mcp_app(child, receive, send)
        if path.startswith(RESULTS_PATH) and scope.get("method") == "GET":
            return await self.results(path[len(RESULTS_PATH):], send)
        return await not_found(send)

    async def results(self, rest, send):
        pieces = rest.split("/")
        if len(pieces) > 2 or (len(pieces) == 2 and PART_INDEX.fullmatch(pieces[1]) is None):
            return await not_found(send)
        job_id = pieces[0]
        located = own_job(self.store, job_id)
        if located is None or located[0] != "history" or located[1].status != "done":
            return await not_found(send)
        folder = self.store.job_result_dir(job_id)
        output_filename = located[1].output_filename
        if len(pieces) == 1:
            if self.listings >= LISTINGS_MAX:
                return await _answer(send, 429, BUSY_BODY)
            self.listings += 1
            try:
                parts = await asyncio.to_thread(list_parts, folder, output_filename)
            except _NotFound:
                return await not_found(send)
            except _Unavailable:
                return await _answer(send, 503, UNAVAILABLE_BODY)
            finally:
                self.listings -= 1
            if not parts:
                return await not_found(send)
            body = json.dumps({"parts": [{"index": index, "name": safe_name(name), "size": size} for index, (name, size) in enumerate(parts)]}).encode("ascii")
            return await _answer(send, 200, body)
        if self.downloads >= DOWNLOADS_MAX:
            return await _answer(send, 429, BUSY_BODY)
        self.downloads += 1
        try:
            await self._send_part(folder, output_filename, int(pieces[1]), send)
        finally:
            self.downloads -= 1

    async def _send_part(self, folder, output_filename, index, send):
        try:
            fd, size = await asyncio.to_thread(open_part, folder, output_filename, index)
        except _NotFound:
            return await not_found(send)
        except _Unavailable:
            return await _answer(send, 503, UNAVAILABLE_BODY)
        try:
            await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"application/octet-stream"), (b"content-length", str(size).encode("ascii"))]})
            left = size
            while left > 0:
                try:
                    chunk = await asyncio.to_thread(os.read, fd, min(CHUNK, left))
                except OSError:
                    chunk = b""
                if not chunk:
                    # The file shrank: the answer is left incomplete, so the server closes the connection and the client sees it cut.
                    return
                left -= len(chunk)
                await send({"type": "http.response.body", "body": chunk, "more_body": left > 0})
            if size == 0:
                await send({"type": "http.response.body", "body": b"", "more_body": False})
        finally:
            with contextlib.suppress(OSError):
                os.close(fd)
