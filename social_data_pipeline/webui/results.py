"""The parts of a finished query's result, read from the scheduler's results folder that `sdp webui start` mounts read-only (`config.Settings.results`), and the answer that streams one of them to the browser.

The parts belong to the database servers' users, not to the operator, so no owner is checked; what is checked instead: the job's folder is `<results>/<job id>` (the id checked against the job id pattern) and is opened without following a link (`O_DIRECTORY | O_NOFOLLOW`); it is listed through that descriptor, at most 10,000 entries; the parts are the names that are exactly the job's output file name or `<stem>_<digits><ext>` (the pattern built with `re.escape`; an output file name with a `/` or a NUL, `.`, `..` or an empty one is refused), in the order the scheduler's preview uses (the output file's own name first, then by number); each is opened relative to the folder's descriptor without following a link and without waiting (`O_NOFOLLOW | O_NONBLOCK`) and kept only when it is a regular file (`fstat`). A part is then streamed from its descriptor, never opened again by its path, in pieces of 64 KiB, with the length `fstat` gave; the descriptor is closed when the answer ends, also when the client goes away. Standard library, anyio and Starlette only.
"""

import os
import re
import stat

import anyio
from starlette.responses import Response

from social_data_pipeline.webui import actions

LISTING_MAX = 10000
PARTS_MAX = 1000
CHUNK_BYTES = 64 * 1024
NAME_MAX_CHARS = 128
_NAME_CHARACTERS = re.compile(r"[^A-Za-z0-9._-]")


class NoParts(Exception):
    """The folder or its parts cannot be used: answered as not found."""


def part_names(names, output_filename):
    """The names of the parts among `names`, in the scheduler's preview's order: the output file's own name first, then `<stem>_<digits><ext>` by number (the extension as written, or in lower case)."""
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


def usable_output_name(output_filename):
    return isinstance(output_filename, str) and output_filename not in ("", ".", "..") and "/" not in output_filename and "\0" not in output_filename


def _open_folder(results, job_id):
    if not actions.is_job_id(job_id) or not isinstance(results, str):
        raise NoParts()
    try:
        return os.open(os.path.join(results, job_id), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError:
        raise NoParts() from None


def _listed(folder_fd):
    names = []
    try:
        with os.scandir(folder_fd) as entries:
            for entry in entries:
                names.append(entry.name)
                if len(names) > LISTING_MAX:
                    raise NoParts()
    except OSError:
        raise NoParts() from None
    return names


def _open_part(folder_fd, name):
    """(descriptor, size) of a part that is a regular file, or None."""
    try:
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=folder_fd)
    except OSError:
        return None
    try:
        info = os.fstat(fd)
    except OSError:
        os.close(fd)
        return None
    if not stat.S_ISREG(info.st_mode):
        os.close(fd)
        return None
    return fd, info.st_size


def list_parts(results, job_id, output_filename):
    """`[(name, size)]` of the parts of a job's result that are regular files, in order, at most 1,000. Raises NoParts."""
    if not usable_output_name(output_filename):
        raise NoParts()
    folder_fd = _open_folder(results, job_id)
    try:
        found = []
        for name in part_names(_listed(folder_fd), output_filename):
            opened = _open_part(folder_fd, name)
            if opened is None:
                continue
            os.close(opened[0])
            found.append((name, opened[1]))
            if len(found) >= PARTS_MAX:
                break
        return found
    finally:
        os.close(folder_fd)


def open_part(results, job_id, output_filename, number):
    """(descriptor, size, name) of the part `number` (counted from 0 among the parts that are regular files, as `list_parts` lists them). Raises NoParts."""
    if not usable_output_name(output_filename) or not isinstance(number, int) or not 0 <= number < PARTS_MAX:
        raise NoParts()
    folder_fd = _open_folder(results, job_id)
    try:
        seen = 0
        for name in part_names(_listed(folder_fd), output_filename):
            opened = _open_part(folder_fd, name)
            if opened is None:
                continue
            if seen == number:
                return opened[0], opened[1], name
            os.close(opened[0])
            seen += 1
        raise NoParts()
    finally:
        os.close(folder_fd)


def download_name(name):
    """The file name the browser is given: every character but letters, digits, `.`, `_` and `-` replaced by `_`, at most 128 characters."""
    shown = _NAME_CHARACTERS.sub("_", name)[:NAME_MAX_CHARS]
    return shown or "download"


class PartStream(Response):
    """The answer that streams an opened part: `application/octet-stream`, an attachment with a reduced file name, `Content-Length` from `fstat`, no ranges. The descriptor is closed and `release()` called when the answer ends, however it ends (sent whole, the client gone, a shorter file, an error)."""

    media_type = "application/octet-stream"

    def __init__(self, fd, size, name, release):
        super().__init__(content=None, status_code=200, headers={"content-disposition": f'attachment; filename="{download_name(name)}"', "content-length": str(size)}, media_type=self.media_type)
        self.fd = fd
        self.size = size
        self.release = release

    def _read(self, count):
        return os.read(self.fd, count)

    async def __call__(self, scope, receive, send):
        try:
            async with anyio.create_task_group() as group:

                async def watch():
                    while True:
                        message = await receive()
                        if message["type"] == "http.disconnect":
                            group.cancel_scope.cancel()
                            return

                group.start_soon(watch)
                await send({"type": "http.response.start", "status": self.status_code, "headers": self.raw_headers})
                left = self.size
                while left > 0:
                    chunk = await anyio.to_thread.run_sync(self._read, min(CHUNK_BYTES, left))
                    if not chunk:
                        # The file became shorter than its length was: the answer cannot end well; the connection is cut.
                        raise OSError("the part became shorter while it was sent")
                    left -= len(chunk)
                    await send({"type": "http.response.body", "body": chunk, "more_body": left > 0})
                if self.size == 0:
                    await send({"type": "http.response.body", "body": b"", "more_body": False})
                group.cancel_scope.cancel()
        finally:
            try:
                os.close(self.fd)
            except OSError:
                pass
            self.release()
