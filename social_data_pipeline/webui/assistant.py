"""How the web UI reaches the chat's runner (`chat/runner.py`, in the sandbox) for the Chat window, and the shape every answer must have before anything of it is used.

Everything the runner says is hostile: the code the assistant writes runs beside it under the same uid, can replace the socket and make it answer anything. So, as the Queue's way to the scheduler does (`scheduler.Scheduler.folder_problem`, reused here), before each connection the socket's folder must be a real folder of this process's user without permission for the group or others, and the socket a socket of this user without them, else a refusal (`AssistantUnavailable` with the reason) and nothing is sent; without the setting (`config.Settings.chat_socket`) the reason is `no_settings`. A call goes through `RunnerClient` (the framing and errors of `control.client`, but a reply of at most about 1 MiB, the most the runner sends, and of at most 20,000 objects and lists, counted before it is parsed) with a time limit (10 seconds, 30 for `chat.send` and `chat.delete`) and blocks: the routes make it in a worker thread. Every result is checked against the exact shape the runner's documentation gives (the keys, their types, lengths, ids by pattern, counts within the runner's bounds); any other shape raises `NotUnderstood` and nothing of it is used. A refusal of the runner is a `BrokerError` whose code alone `words.chat_sentence` looks up. The events themselves are checked one by one where they are shown (`chat.event_model`). Standard library, the broker's client (which speaks the runner's framing) and this package only.
"""

import base64
import binascii
import math
import os
import re
import socket
import time

from social_data_pipeline.control import client, framing
from social_data_pipeline.webui import scheduler

READ_SECONDS = 10
CHANGE_SECONDS = 30
CHAT_ID = re.compile(r"c_[0-9a-f]{12}")
# The runner's own rule for the names of a chat's files (`runner.FILE_NAME`), "." and ".." besides.
FILE_NAME = re.compile(r"[A-Za-z0-9._ -]{1,128}")
NAME = re.compile(r"[a-z0-9_-]{1,40}")
# The runner's bounds.
TITLE_MAX = 80
CHATS_MAX = 200
FILES_MAX = 500
EVENTS_MAX = 200
TEXT_MAX = 32000
PIECE_BYTES = 512 * 1024
DATABASES_MAX = 16
# A model's name as the runner gives it (`hello`'s `models` and `model_default`, a chat's `model`): visible ASCII, at most 200 characters (the rule of `chat_host.parse_model`); at most MODELS_MAX of them.
MODEL = re.compile(r"[\x21-\x7e]{1,200}")
MODELS_MAX = 8
# `send` without `model`: the chat keeps the model it has.
KEEP = object()
# The most lines a chat's log holds (16 MiB, one line per 32 bytes), so the largest offset there is.
OFFSET_MAX = 16 * 1024 * 1024 // 32
SIZE_MAX = 1 << 62
TIME_BOUND = 10 ** 11
# The most bytes of UTF-8 the strings of one page of events may hold together: the runner's own bound of a page (`events_bytes`).
EVENTS_BYTES = 512 * 1024
# The largest reply frame read: the runner never sends more than 1 MiB (`reply_bytes`); a margin besides. A larger one is refused after its length, unread.
REPLY_MAX_BYTES = 1024 * 1024 + 64 * 1024
# The most objects and lists a reply may hold, counted outside its strings before it is parsed: the runner's largest honest reply (200 events, 500 files, 200 chats) holds about a thousand.
CONTAINERS_MAX = 20000
# The characters that matter for counting objects and lists outside strings: one class of single bytes, so `finditer` never goes back.
_STRUCTURE = re.compile(rb'["\\\[{]')


class AssistantUnavailable(client.BrokerError):
    """The runner is not asked: `reason` is `no_settings`, one of `scheduler.FOLDER_REASONS` or `scheduler.SOCKET_REASONS`, or `no_socket`."""

    def __init__(self, reason):
        super().__init__("unreachable", "the chat's runner is not asked")
        self.reason = reason


class NotUnderstood(client.BrokerError):
    """An answer of another shape than the one expected."""

    def __init__(self):
        super().__init__("not_understood", "the answer is not of the expected shape")


def is_chat_id(value):
    return isinstance(value, str) and CHAT_ID.fullmatch(value) is not None


def is_file_name(value):
    return isinstance(value, str) and FILE_NAME.fullmatch(value) is not None and value not in (".", "..")


def is_text(value, most):
    """A string of at most `most` characters that UTF-8 can hold (no lone surrogate)."""
    if not isinstance(value, str) or len(value) > most:
        return False
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def is_model(value):
    return isinstance(value, str) and MODEL.fullmatch(value) is not None


def is_whole(value, low=0, high=SIZE_MAX):
    return isinstance(value, int) and not isinstance(value, bool) and low <= value <= high


def is_number(value, low, high):
    """A whole or decimal number (not a flag) from `low` to `high`; compared before anything else is done with it, so a huge integer or a NaN is refused without an error."""
    return isinstance(value, (int, float)) and not isinstance(value, bool) and low <= value <= high and math.isfinite(value)


def is_time(value):
    return is_number(value, -TIME_BOUND, TIME_BOUND)


def text_bytes(value, most):
    """The bytes of UTF-8 that the strings (keys included) of a JSON value hold together, walked without recursion; raises NotUnderstood once they pass `most`."""
    total, todo = 0, [value]
    while todo:
        item = todo.pop()
        if isinstance(item, str):
            total += len(item.encode("utf-8", "surrogatepass"))
        elif isinstance(item, dict):
            todo.extend(item.keys())
            todo.extend(item.values())
        elif isinstance(item, list):
            todo.extend(item)
        if total > most:
            raise NotUnderstood()
    return total


def _exactly(value, keys):
    if not isinstance(value, dict) or set(value) != set(keys):
        raise NotUnderstood()
    return value


def _require(condition):
    if not condition:
        raise NotUnderstood()


def _reason_of(error):
    """The reason of a connection that failed, as `control.client` gives it."""
    if isinstance(error, (FileNotFoundError, NotADirectoryError)):
        return client.NO_SOCKET
    if isinstance(error, ConnectionRefusedError):
        return client.REFUSED
    if isinstance(error, PermissionError):
        return client.PERMISSION_DENIED
    return client.NOT_REACHED


def containers(payload, steps=None):
    """How many objects and lists the JSON text `payload` opens outside its strings, in one pass over the bytes that matter (quotes, backslashes, `[` and `{`), with an inside-string and an escaped-position state, as `control.framing` scans a frame's depth: linear whatever the payload holds (a string that never ends, backslashes only). `steps`, when a list, gets the number of steps taken (for a test)."""
    count, inside, escaped_at, taken = 0, False, -1, 0
    for found in _STRUCTURE.finditer(payload):
        taken += 1
        position, character = found.start(), found.group()
        if inside:
            if position == escaped_at:
                continue
            if character == b"\\":
                escaped_at = position + 1
            elif character == b'"':
                inside = False
            continue
        if character == b'"':
            inside = True
        elif character != b"\\":
            count += 1
    if steps is not None:
        steps.append(taken)
    return count


class RunnerClient:
    """A client of the runner's socket for one request: what `control.client.Client` does (one framed request, one framed reply, the same time limit and the same errors: `BrokerUnreachable` with its reason, `BrokerError` with the reply's code), but a reply is read only up to REPLY_MAX_BYTES and within the time limit as a whole (on `now`, looked at before every read and once the reply is in, as `control.framing.read_frame` does) (a longer one is refused after its length, unread) and is not parsed when it holds more than CONTAINERS_MAX objects and lists: either is `BrokerUnreachable(no_reply)`, an answer the page cannot read."""

    def __init__(self, socket_path, timeout, now=time.monotonic):
        self.socket_path = socket_path
        self.timeout = timeout
        self.now = now

    def request(self, document):
        now = self.now
        deadline = now() + self.timeout
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            try:
                connection.settimeout(self.timeout)
                connection.connect(self.socket_path)
            except OSError as error:
                raise client.BrokerUnreachable(_reason_of(error)) from None
            try:
                connection.sendall(framing.encode(document, max_bytes=framing.REQUEST_MAX_BYTES))
            except (BrokenPipeError, ConnectionResetError):
                raise client.BrokerUnreachable(client.NO_REPLY) from None
            except OSError:
                raise client.BrokerUnreachable(client.NOT_REACHED) from None

            def exactly(count):
                """`count` bytes, the rule of `control.framing`'s reader: the time left is looked at before every read, and none left is the end."""
                parts, held = [], 0
                while held < count:
                    left = deadline - now()
                    if left <= 0:
                        raise TimeoutError()
                    connection.settimeout(left)
                    chunk = connection.recv(min(count - held, 1 << 16))
                    if not chunk:
                        raise OSError("the connection ended")
                    parts.append(chunk)
                    held += len(chunk)
                return b"".join(parts)

            try:
                length = int.from_bytes(exactly(framing.HEADER_BYTES), "big")
                if length > REPLY_MAX_BYTES:
                    raise OSError("the reply is larger than the runner sends")
                payload = exactly(length)
                if now() >= deadline:
                    raise TimeoutError()
                if containers(payload) > CONTAINERS_MAX:
                    raise OSError("the reply holds more objects and lists than the runner sends")
                reply = framing.decode(payload)
            except (OSError, framing.FrameError):
                raise client.BrokerUnreachable(client.NO_REPLY) from None
        finally:
            connection.close()
        if reply.get("ok") is True and "result" in reply:
            return reply["result"]
        error = reply.get("error")
        if not isinstance(error, dict):
            raise client.BrokerError("bad_reply", "the reply is not one the runner sends")
        raise client.BrokerError(error.get("code"), "the runner refused the request")


def _connect(socket_path, timeout):
    return RunnerClient(socket_path, timeout)


class Assistant:
    """The way to the chat's runner. `connect(socket_path, timeout)` makes a client; `lstat` and `uid` are the folder check's (injected in tests)."""

    folder_problem = scheduler.Scheduler.folder_problem

    def __init__(self, socket_path, *, connect=_connect, lstat=os.lstat, uid=os.getuid):
        self.socket_path = socket_path
        self._connect = connect
        self._lstat = lstat
        self._uid = uid

    def _ask(self, op, fields=None, timeout=READ_SECONDS):
        if self.socket_path is None:
            raise AssistantUnavailable("no_settings")
        problem = self.folder_problem()
        if problem is not None:
            raise AssistantUnavailable(problem)
        return self._connect(self.socket_path, timeout).request({"op": op, **(fields or {})})

    def hello(self, timeout=READ_SECONDS):
        """`{"backend", "databases", "queue", "models", "model_default"}` of `hello`, asked with the time limit `timeout`. A runner from before the field `queue`, or from before `models` and `model_default`, answers without them: those shapes are taken too and read as without the queue and without a choice of model (`models` empty, `model_default` None), so that a web UI and a sandbox image of different versions do not refuse each other; with the fields, `queue` must be a flag, `models` at most MODELS_MAX distinct names of the model rule, `model_default` None or one of them."""
        found = self._ask("hello", timeout=timeout)
        keys = ("version", "backend", "databases")
        if isinstance(found, dict) and "queue" in found:
            keys += ("queue",)
            if "models" in found or "model_default" in found:
                keys += ("models", "model_default")
        found = _exactly(found, keys)
        queue = found.get("queue", False)
        _require(isinstance(queue, bool))
        models, default = found.get("models", []), found.get("model_default")
        _require(isinstance(models, list) and len(models) <= MODELS_MAX and all(is_model(name) for name in models) and len(set(models)) == len(models))
        _require(default is None or (is_model(default) and default in models))
        databases = found["databases"]
        _require(is_whole(found["version"], 1, 1000) and isinstance(found["backend"], str) and NAME.fullmatch(found["backend"]) is not None)
        _require(isinstance(databases, list) and len(databases) <= DATABASES_MAX and all(isinstance(name, str) and NAME.fullmatch(name) for name in databases))
        return {"backend": found["backend"], "databases": list(databases), "queue": queue, "models": list(models), "model_default": default}

    def chats(self):
        """The chats of `chat.list`, newest first: `[{"id", "title", "created", "running", "model"}]` (`model` None for the back end's default; a runner from before the field answers without it, read as None)."""
        chats = _exactly(self._ask("chat.list"), ("chats",))["chats"]
        _require(isinstance(chats, list) and len(chats) <= CHATS_MAX)
        found = []
        for chat in chats:
            _exactly(chat, ("id", "title", "created", "running", "model") if isinstance(chat, dict) and "model" in chat else ("id", "title", "created", "running"))
            model = chat.get("model")
            _require(is_chat_id(chat["id"]) and is_text(chat["title"], TITLE_MAX) and is_time(chat["created"]) and isinstance(chat["running"], bool) and (model is None or is_model(model)))
            found.append({"id": chat["id"], "title": chat["title"], "created": chat["created"], "running": chat["running"], "model": model})
        _require(len({chat["id"] for chat in found}) == len(found))
        return found

    def new(self, model=None):
        """The id of a new chat; with `model` (a name of the model rule) the chat is given that model, without it the back end's default."""
        chat = _exactly(self._ask("chat.new", {"model": model} if model is not None else None), ("chat",))["chat"]
        _require(is_chat_id(chat))
        return chat

    def send(self, chat, text, model=KEEP):
        """Send a question; the index of its `user` event. With `model` (a name of the model rule, or None for the back end's default) the chat runs on that model from this turn on; without it, on the model it has."""
        fields = {"chat": chat, "text": text, **({} if model is KEEP else {"model": model})}
        offset = _exactly(self._ask("chat.send", fields, CHANGE_SECONDS), ("offset",))["offset"]
        _require(is_whole(offset, 0, OFFSET_MAX))
        return offset

    def events(self, chat, offset, limit):
        """(the events from `offset` on, at most `limit`, each still to be checked; the offset after them; whether a turn runs). The offset after them must be `offset` plus their number: one that goes back or jumps is not understood; so is a page whose strings hold more than the runner's own bound of a page (EVENTS_BYTES)."""
        found = _exactly(self._ask("chat.events", {"chat": chat, "offset": offset, "limit": limit}), ("events", "next", "running"))
        events = found["events"]
        _require(isinstance(events, list) and len(events) <= limit and isinstance(found["running"], bool))
        text_bytes(events, EVENTS_BYTES)
        _require(is_whole(found["next"], 0, OFFSET_MAX) and found["next"] == offset + len(events))
        return events, found["next"], found["running"]

    def stop(self, chat):
        """Whether a turn was asked to stop."""
        stopping = _exactly(self._ask("chat.stop", {"chat": chat}), ("stopping",))["stopping"]
        _require(isinstance(stopping, bool))
        return stopping

    def delete(self, chat):
        _exactly(self._ask("chat.delete", {"chat": chat}, CHANGE_SECONDS), ())

    def files(self, chat):
        """([{"name", "size"}] of `chat.files`, how many names were left out, whether there are more)."""
        found = _exactly(self._ask("chat.files", {"chat": chat}), ("files", "left_out", "more"))
        files = found["files"]
        _require(isinstance(files, list) and len(files) <= FILES_MAX and is_whole(found["left_out"]) and isinstance(found["more"], bool))
        for entry in files:
            _exactly(entry, ("name", "size", "mtime"))
            _require(is_file_name(entry["name"]) and is_whole(entry["size"]) and is_time(entry["mtime"]))
        return [{"name": entry["name"], "size": entry["size"]} for entry in files], found["left_out"], found["more"]

    def piece(self, chat, name, offset, limit=PIECE_BYTES):
        """(the bytes of the file `name` from `offset` on, at most `limit`; the file's size; whether that is its end)."""
        found = _exactly(self._ask("chat.file", {"chat": chat, "name": name, "offset": offset, "limit": limit}), ("data", "size", "next", "end"))
        _require(isinstance(found["data"], str) and len(found["data"]) <= (limit + 2) // 3 * 4 and is_whole(found["size"]) and isinstance(found["end"], bool))
        try:
            data = base64.b64decode(found["data"].encode("ascii"), validate=True)
        except (binascii.Error, UnicodeEncodeError):
            raise NotUnderstood() from None
        _require(len(data) <= limit and is_whole(found["next"]) and found["next"] == offset + len(data) and found["end"] == (found["next"] >= found["size"]))
        return data, found["size"], found["end"]
