"""The agent of the OpenAI-compatible back end: one process per turn, `python -P -s -m social_data_pipeline.chat.agent --home <chat's home/> --files <chat's files/>`, started by `chat.openai`.

It reads the user's text as one stream-json line on standard input and prints its progress as stream-json lines that `claude.Translator` understands (an `assistant` line with `text` and `tool_use` blocks, a `user` line with a `tool_result` block, a last `result` line without a cost). The loop: the role text of `claude.ROLE`, the chat's history and the question go to the gateway's model port (`http://<SDP_CHAT_GATEWAY>:8080/chat/completions`, no streaming, no credential: the gateway puts it in) with the tools `run_shell` and `<db>__<tool>` of each attached database's MCP server (`SDP_CHAT_DATABASES`, reached through the gateway's port for it), and with the query queue (`SDP_CHAT_QUEUE=1`) `jobs__<tool>` of the scheduler's assistant endpoint (`claude.queue_url`) and the role text's paragraph about it; each reply's tool calls are run in order and answered, a reply without tool calls is the answer; at most 40 model calls a turn. Every reply is untrusted: its size, its types, the tool names and the arguments are checked, and what cannot be used ends the turn or is answered to the model as a tool error; a refusal (any status but 200) ends the turn with a message naming the status only, never the body.

The history is kept in `<home>/openai-session.json` (0600, replaced whole): the messages of the earlier turns, tool outputs cut, cut to a character budget by dropping the oldest turns whole (the newest is always kept, cut further when it alone is over). A file of another shape is taken as empty. Within a turn the request is budgeted too (`squeezed`), and an endpoint that refuses a request with earlier turns as too large (400, 413) is asked once more without them. On SIGTERM the running shell command's process group is killed and the agent ends at once (the turn is not added to the history).
"""

import argparse
import asyncio
import contextlib
import http.client
import importlib.util
import ipaddress
import json
import os
import re
import select
import signal
import stat
import subprocess
import sys
import threading
import time

from . import claude

MODEL_PATH = "/chat/completions"
CALLS_MAX = 40
CALLS_PER_REPLY = 16
CALLS_LISTED_MAX = 256
SHELL_SECONDS = 120.0
TOOL_SECONDS = 120.0
CONNECT_SECONDS = 30.0
MODEL_SECONDS = 600.0
OUTPUT_CUT = 16000
TEXT_CUT = 32000
ARGUMENTS_MAX = 16000
HISTORY_NAME = "openai-session.json"
HISTORY_BUDGET = 400_000
TURN_BUDGET = 1_000_000
HISTORY_TOOL_CUT = 4000
HISTORY_MAX_BYTES = 16 * 1024 * 1024
REPLY_MAX_BYTES = 4 * 1024 * 1024
STDIN_MAX_BYTES = 1024 * 1024
DRAIN_SECONDS = 1.0
DRAIN_BYTES = 1024 * 1024
TOOLS_PER_SERVER = 64
DESCRIPTION_CUT = 1000
SCHEMA_MAX_BYTES = 16 * 1024
SCHEMA_DEPTH = 32
SCHEMA_CONTAINERS = 2000
TOOLS_CONTAINERS = 100_000
TOOL_NAME = re.compile(r"[A-Za-z0-9_-]{1,64}")
CALL_ID = re.compile(r"[\x21-\x7e]{1,256}")
MODEL_NAME = re.compile(r"[\x21-\x7e]{1,200}")
_BAD_KEY = re.compile("[\x00-\x1f\ud800-\udfff]")
EMPTY_SCHEMA = {"type": "object", "properties": {}}
SHELL_TOOL = "run_shell"
SHELL_SPEC = {"type": "function", "function": {"name": SHELL_TOOL, "description": f"Run a command with bash in the chat's folder (the current folder, where files you write are shown to the user). Standard output and error together are cut to {OUTPUT_CUT} characters and the exit code is given; the command is ended after {int(SHELL_SECONDS)} seconds, and whatever it leaves running in the background is ended when it returns.", "parameters": {"type": "object", "properties": {"command": {"type": "string", "description": "The bash command."}}, "required": ["command"]}}}

UNREACHABLE = "the model endpoint could not be reached"
CALLS_LIMIT = f"the turn reached the limit of {CALLS_MAX} model calls"
NOT_RUN_LIMIT = "not run: the turn reached its limit of model calls"
NO_SUCH_TOOL = "there is no tool of this name"
NOT_AN_OBJECT = "the arguments are not a JSON object"
TOO_LONG = f"the arguments are longer than {ARGUMENTS_MAX} characters"
PLACEHOLDER = "[left out to keep the conversation within its size limit]"
TURN_FAILED = "the turn failed"
NOT_FINISHED = "the tool did not finish"
NO_COMMAND = "the arguments need `command`, a string"
TOOL_FAILED = "the tool could not be called"
TOOL_TIMEOUT = f"the tool did not answer within {int(TOOL_SECONDS)} seconds"


def status_message(status):
    return f"the model endpoint answered with status {int(status)}"


def bad_reply_message(why):
    return f"the model's reply could not be used: {why}"


class ModelRefused(Exception):
    """The model call failed: `status` is the HTTP status, or None when the endpoint could not be reached."""

    def __init__(self, status):
        super().__init__(status)
        self.status = status


class BadReply(Exception):
    """The reply cannot be used; the text is one of a fixed set of reasons."""


def _log(line):
    sys.stderr.write(f"agent: {line}\n")
    sys.stderr.flush()


def clean(text):
    """`text` as text UTF-8 can hold (a lone surrogate becomes `?`)."""
    return text.encode("utf-8", "replace").decode("utf-8")


def ignored_message(count):
    return f"{count} more tool calls of this reply were ignored: at most {CALLS_PER_REPLY} are taken from one reply"


def scrub(value):
    """`value` (JSON data) with every string as text UTF-8 can hold."""
    return json.loads(clean(json.dumps(value, ensure_ascii=False)))


def cut(text, limit):
    return text if len(text) <= limit else text[:limit]


def kind_of(error):
    """The name of the kind of `error`, of its first leaf for an exception group (the `mcp` package's task groups wrap what failed)."""
    while isinstance(error, BaseExceptionGroup) and error.exceptions:
        error = error.exceptions[0]
    return type(error).__name__


def emit(doc):
    sys.stdout.write(json.dumps(doc, separators=(",", ":")) + "\n")
    sys.stdout.flush()


# ── the shell ─────────────────────────────────────────────────────────────

# The running shell command's process group, for the TERM handler; the lock makes its start and the handler's kill one step each.
_shell_lock = threading.Lock()
_shell = {"pid": None, "stopping": False}


def _on_term(signum, frame):
    if _shell_lock.acquire(timeout=5):
        _shell["stopping"] = True
    pid = _shell["pid"]
    if pid is not None:
        with contextlib.suppress(OSError):
            os.killpg(pid, signal.SIGKILL)
    os._exit(128 + signum)


def run_shell(command, cwd, env, seconds=SHELL_SECONDS, limit=OUTPUT_CUT):
    """Run `command` with `bash -c` in `cwd` in a process group of its own: (text, is_error). The output (standard output and error together) is read bounded and cut to `limit` characters; after `seconds`, or once bash has ended, the whole group is killed."""
    with _shell_lock:
        if _shell["stopping"]:
            return "not run: the turn is stopping", True
        proc = subprocess.Popen(["bash", "-c", command], cwd=cwd, env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True, close_fds=True)
        _shell["pid"] = proc.pid
    kept, more, timed_out = bytearray(), False, False
    fd = proc.stdout.fileno()
    deadline = time.monotonic() + seconds

    def take(chunk):
        nonlocal more
        room = limit * 4 - len(kept)
        kept.extend(chunk[:max(room, 0)])
        more = more or len(chunk) > room

    ended = False
    try:
        # bash's end is looked at on every pass: something it left behind may keep the output busy, or hold it open after bash has ended.
        while proc.poll() is None:
            left = deadline - time.monotonic()
            if left <= 0:
                timed_out = True
                break
            if ended:
                with contextlib.suppress(subprocess.TimeoutExpired):
                    proc.wait(min(left, 0.2))
                continue
            if select.select([fd], [], [], min(left, 0.2))[0]:
                chunk = os.read(fd, 65536)
                if chunk:
                    take(chunk)
                else:
                    ended = True
    finally:
        with contextlib.suppress(OSError):
            os.killpg(proc.pid, signal.SIGKILL)
        code = proc.wait()
        # What is left in the pipe, bounded in time and bytes: a process that left the group can keep writing into it.
        drain_end, budget = time.monotonic() + DRAIN_SECONDS, DRAIN_BYTES
        while not ended and budget > 0:
            left = drain_end - time.monotonic()
            if left <= 0 or not select.select([fd], [], [], left)[0]:
                break
            chunk = os.read(fd, 65536)
            if not chunk:
                break
            budget -= len(chunk)
            take(chunk)
        proc.stdout.close()
        with _shell_lock:
            _shell["pid"] = None
    text = kept.decode("utf-8", "replace")
    if more or len(text) > limit:
        text = text[:limit] + f"\n[the output was cut at {limit} characters]"
    text += "\n" if text and not text.endswith("\n") else ""
    if timed_out:
        return text + f"[the command was ended after {int(seconds)} seconds]", True
    return text + f"[exit code {code}]", code != 0


# ── the databases' MCP servers ────────────────────────────────────────────


def containers(value):
    """How many objects and lists `value` holds, itself included."""
    count, stack = 0, [value]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            count += 1
            stack.extend(node.values())
        elif isinstance(node, list):
            count += 1
            stack.extend(node)
    return count


def _schema_ok(schema):
    """Whether an input schema can go into a request the gateway takes: an object under 16 KiB of at most 2,000 objects and lists, at most 32 levels deep, no key holding a control character or a surrogate."""
    if not isinstance(schema, dict):
        return False
    try:
        if len(json.dumps(schema)) >= SCHEMA_MAX_BYTES:
            return False
    except (TypeError, ValueError, RecursionError):
        return False
    if containers(schema) > SCHEMA_CONTAINERS:
        return False
    stack = [(schema, 1)]
    while stack:
        node, depth = stack.pop()
        if depth > SCHEMA_DEPTH:
            return False
        if isinstance(node, dict):
            if any(_BAD_KEY.search(key) for key in node):
                return False
            stack.extend((value, depth + 1) for value in node.values())
        elif isinstance(node, list):
            stack.extend((value, depth + 1) for value in node)
    return True


def offered_tools(database, listing):
    """The tools of one database's `tools/list` (dicts in the wire format) as {offered name: (the server's name, function spec)}: at most the first 64, each named `<db>__<tool>` by the rule `[A-Za-z0-9_-]{1,64}` (others left out with a line on stderr), its description cut, its schema passed only when `_schema_ok`."""
    offered = {}
    if len(listing) > TOOLS_PER_SERVER:
        _log(f"{database}: {len(listing)} tools listed, only the first {TOOLS_PER_SERVER} are offered")
    for tool in listing[:TOOLS_PER_SERVER]:
        name = tool.get("name") if isinstance(tool, dict) else None
        full = f"{database}__{name}" if isinstance(name, str) else None
        if full is None or not TOOL_NAME.fullmatch(name) or not TOOL_NAME.fullmatch(full) or full in offered:
            _log(f"{database}: a tool whose name cannot be offered is left out")
            continue
        description = tool.get("description") if isinstance(tool.get("description"), str) else ""
        schema = tool.get("inputSchema") if _schema_ok(tool.get("inputSchema")) else EMPTY_SCHEMA
        offered[full] = (name, {"type": "function", "function": {"name": full, "description": cut(clean(description), DESCRIPTION_CUT), "parameters": schema}})
    return offered


def call_text(result, limit=OUTPUT_CUT):
    """(text, is_error) of a `tools/call` result in the wire format: its text blocks, other blocks named by their type, cut to `limit` characters."""
    if not isinstance(result, dict):
        return TOOL_FAILED, True
    parts = []
    for block in result.get("content") if isinstance(result.get("content"), list) else ():
        kind = block.get("type") if isinstance(block, dict) else None
        if kind == "text" and isinstance(block.get("text"), str):
            parts.append(block["text"])
        else:
            parts.append(f"[{kind if isinstance(kind, str) else 'content'}]")
    return cut(clean("\n".join(parts)), limit), result.get("isError") is True


class Server:
    """One database's MCP session, held open by a task of its own for the turn (the `mcp` package's transports must be entered and left in one task): `open()` connects and lists the tools, `call()` hands a call to the task, `close()` ends it."""

    def __init__(self, database, url, kind):
        self.database, self.url, self.kind = database, url, kind
        self.tools = {}
        self._requests = asyncio.Queue()
        self._ready = None
        self._task = None

    async def open(self, seconds=None):
        seconds = CONNECT_SECONDS if seconds is None else seconds
        self._ready = asyncio.get_running_loop().create_future()
        self._task = asyncio.create_task(self._hold())
        try:
            listing = await asyncio.wait_for(asyncio.shield(self._ready), seconds)
        except BaseException:
            self._task.cancel()
            with contextlib.suppress(BaseException):
                await self._task
            raise
        self.tools = offered_tools(self.database, listing)

    async def _hold(self):
        from mcp import ClientSession

        if self.kind == "sse":
            from mcp.client.sse import sse_client

            transport = sse_client(self.url, timeout=CONNECT_SECONDS, sse_read_timeout=TOOL_SECONDS * 30)
        else:
            from mcp.client.streamable_http import streamable_http_client

            transport = streamable_http_client(self.url)
        pending = None
        try:
            async with transport as streams, ClientSession(streams[0], streams[1]) as session:
                await session.initialize()
                listing = await session.list_tools()
                self._ready.set_result([tool.model_dump(by_alias=True, mode="json") for tool in listing.tools])
                while True:
                    pending = await self._requests.get()
                    if pending is None:
                        return
                    name, arguments, future = pending
                    try:
                        result = await session.call_tool(name, arguments)
                        if not future.done():
                            future.set_result(result.model_dump(by_alias=True, mode="json"))
                    except Exception as error:
                        if not future.done():
                            future.set_exception(error)
                    pending = None
        except Exception as error:
            if not self._ready.done():
                self._ready.set_exception(error)
            if pending is not None and not pending[2].done():
                pending[2].set_exception(error)

    async def call(self, name, arguments, seconds=None):
        seconds = TOOL_SECONDS if seconds is None else seconds
        if self._task is None or self._task.done():
            return TOOL_FAILED, True
        future = asyncio.get_running_loop().create_future()
        await self._requests.put((name, arguments, future))
        done, _ = await asyncio.wait({future, self._task}, timeout=seconds, return_when=asyncio.FIRST_COMPLETED)
        if future not in done:
            # A call that does not answer leaves the session in an unknown state: the database is dropped for the rest of the turn.
            self._task.cancel()
            return (TOOL_TIMEOUT if not done else TOOL_FAILED), True
        if future.exception() is not None:
            _log(f"{self.database}: a tool call failed ({kind_of(future.exception())})")
            return TOOL_FAILED, True
        return call_text(future.result())

    async def close(self, seconds=5.0):
        if self._task is None or self._task.done():
            return
        await self._requests.put(None)
        with contextlib.suppress(BaseException):
            await asyncio.wait_for(self._task, seconds)


async def open_servers(databases, gateway, queue=False):
    """The `Server` of each attached database that could be connected and listed, and with `queue` the queue's (`jobs`, after them); one that could not is left out for the turn with a line on stderr (and so is every database when the `mcp` package cannot be imported)."""
    if not databases and not queue:
        return []
    if importlib.util.find_spec("mcp") is None:
        _log("the mcp package cannot be imported: no database is offered")
        return []
    servers = []
    wanted = [(database, f"http://{gateway}:{port}{path}", kind) for database, (port, kind, path) in ((name, claude.DATABASES[name]) for name in databases)]
    if queue:
        wanted.append((claude.QUEUE_SERVER, claude.queue_url(gateway), "http"))
    for database, url, kind in wanted:
        server = Server(database, url, kind)
        try:
            await server.open()
        except Exception as error:
            _log(f"{database}: its MCP server could not be reached ({kind_of(error)}): it is left out of this turn")
            continue
        servers.append(server)
    return servers


# ── the history ───────────────────────────────────────────────────────────


def _message_ok(message, pending):
    """Whether one stored message has exactly the shape the agent writes; `pending` (the ids of the last assistant message's tool calls not answered yet) is updated."""
    if not isinstance(message, dict) or "content" not in message:
        return False
    role, content = message.get("role"), message["content"]
    if role == "user":
        return set(message) == {"role", "content"} and isinstance(content, str) and not pending
    if role == "assistant":
        if pending or not set(message) <= {"role", "content", "tool_calls"}:
            return False
        if "tool_calls" not in message:
            return isinstance(content, str)
        calls = message["tool_calls"]
        if not (content is None or isinstance(content, str)) or not isinstance(calls, list) or not 1 <= len(calls) <= CALLS_PER_REPLY:
            return False
        for call in calls:
            if not isinstance(call, dict) or set(call) != {"id", "type", "function"}:
                return False
            call_id, function = call["id"], call["function"]
            if not (isinstance(call_id, str) and CALL_ID.fullmatch(call_id) and call_id not in pending and call["type"] == "function" and isinstance(function, dict) and set(function) == {"name", "arguments"}
                    and isinstance(function["name"], str) and TOOL_NAME.fullmatch(function["name"]) and isinstance(function["arguments"], str)):
                return False
            pending.add(call_id)
        return True
    if role == "tool":
        call_id = message.get("tool_call_id")
        if set(message) != {"role", "tool_call_id", "content"} or not isinstance(content, str) or not isinstance(call_id, str) or call_id not in pending:
            return False
        pending.discard(call_id)
        return True
    return False


def repaired(messages):
    """The messages with every tool call answered, in one pass: the answers of an assistant message follow it in the order of its calls; a call without an answer gets `NOT_FINISHED`; an answer without its call, or a second answer to one call, is dropped. Every path that keeps or sends messages goes through this, so a turn that failed half-way through its tool calls, or a history file edited by hand, never leaves a conversation an endpoint refuses."""
    if not isinstance(messages, list):
        return messages
    out, block = [], {}

    def close():
        out.extend(answer if answer is not None else {"role": "tool", "tool_call_id": call_id, "content": NOT_FINISHED} for call_id, answer in block.items())
        block.clear()

    for message in messages:
        if isinstance(message, dict) and message.get("role") == "tool":
            call_id = message.get("tool_call_id")
            if isinstance(call_id, str) and block.get(call_id, False) is None:
                block[call_id] = message
            continue
        close()
        out.append(message)
        calls = message.get("tool_calls") if isinstance(message, dict) and message.get("role") == "assistant" else None
        for call in calls if isinstance(calls, list) else ():
            if isinstance(call, dict) and isinstance(call.get("id"), str):
                block.setdefault(call["id"], None)
    close()
    return out


def turns_of(messages):
    """The messages as turns (lists that each start with a user message), or None when they are not of the shape the agent writes."""
    if not isinstance(messages, list):
        return None
    turns, pending = [], set()
    for message in messages:
        if not _message_ok(message, pending):
            return None
        if message["role"] == "user":
            turns.append([])
        elif not turns:
            return None
        turns[-1].append(message)
    return turns if not pending else None


def load_history(home):
    """The earlier turns from `<home>/openai-session.json`, opened without following a link, every string cleaned; [] when there is none, and [] with a line on stderr when it cannot be read or is of another shape, whatever fails."""
    try:
        return _load_history(home)
    except Exception as error:
        _log(f"{HISTORY_NAME} could not be read ({kind_of(error)}): the history is taken as empty")
        return []


def _load_history(home):
    try:
        home_fd = os.open(home, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError:
        _log("the chat's home folder cannot be opened: the history is taken as empty")
        return []
    try:
        fd = os.open(HISTORY_NAME, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=home_fd)
    except FileNotFoundError:
        return []
    except OSError:
        fd = None
    finally:
        os.close(home_fd)
    turns = None
    if fd is not None:
        try:
            info = os.fstat(fd)
            if info.st_size <= HISTORY_MAX_BYTES and stat.S_ISREG(info.st_mode):
                data = os.read(fd, HISTORY_MAX_BYTES + 1)
                with contextlib.suppress(ValueError, RecursionError):
                    doc = json.loads(data.decode("utf-8"))
                    if isinstance(doc, dict) and set(doc) == {"version", "messages"} and doc["version"] == 1:
                        messages = scrub(doc["messages"])
                        fixed = repaired(messages)
                        turns = turns_of(fixed)
                        if turns is not None and fixed != messages:
                            _log(f"{HISTORY_NAME} held tool calls without an answer or answers without a call: they are repaired")
        finally:
            os.close(fd)
    if turns is None:
        _log(f"{HISTORY_NAME} is not of the shape the agent writes: the history is taken as empty")
        return []
    return turns


def _size(messages):
    return sum(len(json.dumps(message)) for message in messages)


def within_budget(turns, current, budget=HISTORY_BUDGET, fixed=0):
    """The latest of the earlier `turns` that fit with the turn `current` (kept whole) and `fixed` characters into `budget`: the oldest are dropped whole, so a tool call never loses its result."""
    room = budget - fixed - _size(current)
    kept = []
    for turn in reversed(turns):
        room -= _size(turn)
        if room < 0:
            break
        kept.append(turn)
    return kept[::-1]


def stored(turn):
    """A turn as it is kept: its tool outputs cut to 4,000 characters."""
    return [{**message, "content": cut(message["content"], HISTORY_TOOL_CUT)} if message["role"] == "tool" else message for message in turn]


def squeezed(turn, budget, kinds=("output", "arguments", "text")):
    """The turn within `budget` characters as far as `kinds` allow: the oldest tool outputs replaced by a placeholder first, then the oldest tool arguments by `{}`, then the oldest texts of the assistant by the placeholder; the question and the last message stay."""
    turn = [dict(message) for message in turn]
    size = _size(turn)
    for kind in kinds:
        for message in turn[1:-1]:
            if size <= budget:
                return turn
            before = _size([message])
            if kind == "output" and message["role"] == "tool":
                message["content"] = PLACEHOLDER
            elif kind == "arguments" and message.get("tool_calls"):
                message["tool_calls"] = [{**call, "function": {**call["function"], "arguments": "{}"}} for call in message["tool_calls"]]
            elif kind == "text" and message["role"] == "assistant" and message["content"]:
                message["content"] = PLACEHOLDER
            size += _size([message]) - before
    return turn


def to_keep(earlier, current, budget=HISTORY_BUDGET):
    """The turns to save: the newest (`current`, stored) always, its tool outputs cut further when it alone is over `budget` and, when that is not enough, only its question and final answer; then the newest earlier turns that fit. A line on stderr says when anything is dropped."""
    newest = squeezed(stored(repaired(current)), budget, ("output",))
    if _size(newest) > budget:
        last = newest[-1]
        answer = last["content"] if last["role"] == "assistant" and "tool_calls" not in last else ""
        newest = [newest[0], {"role": "assistant", "content": answer}]
        _log("this turn alone is over the history's budget: only its question and its answer are kept")
    kept = within_budget(earlier, newest, budget)
    if len(kept) < len(earlier):
        _log(f"{len(earlier) - len(kept)} earlier turns are dropped from the history to keep it within {budget} characters")
    return [*kept, newest]


def save_history(home, turns):
    """Write the turns to `<home>/openai-session.json` (0600) by a new file renamed over it, without following a link; it never raises: a failure is a line on stderr."""
    try:
        _save_history(home, turns)
    except Exception as error:
        _log(f"the history could not be saved ({kind_of(error)})")


def _save_history(home, turns):
    data = json.dumps({"version": 1, "messages": [m for turn in turns for m in turn]}, separators=(",", ":")).encode("ascii")
    temporary = f".{HISTORY_NAME}.{os.urandom(4).hex()}.tmp"
    try:
        home_fd = os.open(home, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError:
        _log("the chat's home folder cannot be opened: the history is not saved")
        return
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=home_fd)
        try:
            view = memoryview(data)
            while view:
                view = view[os.write(fd, view):]
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(temporary, HISTORY_NAME, src_dir_fd=home_fd, dst_dir_fd=home_fd)
    except OSError:
        _log("the history could not be saved")
        with contextlib.suppress(OSError):
            os.unlink(temporary, dir_fd=home_fd)
    finally:
        os.close(home_fd)


# ── the model ─────────────────────────────────────────────────────────────


def call_model(host, port, body, seconds=MODEL_SECONDS):
    """POST `body` to the gateway's model port: the reply's bytes (at most 4 MiB). Raises ModelRefused (status 200 only is taken; the body of any other is never read) or BadReply."""
    connection = http.client.HTTPConnection(host, port, timeout=seconds)
    try:
        connection.request("POST", MODEL_PATH, body=body, headers={"Content-Type": "application/json", "Accept": "application/json"})
        response = connection.getresponse()
        if response.status != 200:
            raise ModelRefused(response.status)
        data = response.read(REPLY_MAX_BYTES + 1)
    except (OSError, http.client.HTTPException):
        raise ModelRefused(None) from None
    finally:
        connection.close()
    if len(data) > REPLY_MAX_BYTES:
        raise BadReply(f"it is larger than {REPLY_MAX_BYTES // (1024 * 1024)} MiB")
    return data


def parse_reply(data, offered):
    """(text, calls) of a chat-completions reply, checked: `calls` are dicts {id, name, arguments, input, problem}, where `problem` is the tool error to answer instead of running it (an unknown name, arguments that are not a JSON object or too long). Raises BadReply."""
    try:
        doc = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, ValueError, RecursionError):
        raise BadReply("it is not JSON") from None
    choices = doc.get("choices") if isinstance(doc, dict) else None
    message = choices[0].get("message") if isinstance(choices, list) and choices and isinstance(choices[0], dict) else None
    if not isinstance(message, dict):
        raise BadReply("it has no message")
    content = message.get("content")
    if content is not None and not isinstance(content, str):
        raise BadReply("its content is not text")
    listed = message.get("tool_calls")
    if listed is not None and not isinstance(listed, list):
        raise BadReply("its tool calls are not a list")
    if listed and len(listed) > CALLS_LISTED_MAX:
        raise BadReply(f"it lists more than {CALLS_LISTED_MAX} tool calls")
    calls, ids = [], set()
    for entry in (listed or [])[:CALLS_PER_REPLY]:
        function = entry.get("function") if isinstance(entry, dict) else None
        if not isinstance(function, dict) or entry.get("type", "function") != "function" or not isinstance(entry.get("id"), str) or not CALL_ID.fullmatch(entry["id"]) or entry["id"] in ids:
            raise BadReply("a tool call is not of the expected shape")
        ids.add(entry["id"])
        name, arguments = function.get("name"), function.get("arguments")
        if isinstance(arguments, dict):
            arguments = json.dumps(arguments)
        if not isinstance(name, str) or not isinstance(arguments, str):
            raise BadReply("a tool call is not of the expected shape")
        # The arguments are kept as given only when they are a JSON object (normalised); anything else is kept as `{}`, and the tool error says what was wrong.
        call = {"id": entry["id"], "name": name if TOOL_NAME.fullmatch(name) else "unknown_tool", "arguments": "{}", "input": None, "problem": None}
        if name not in offered:
            call["problem"] = NO_SUCH_TOOL
        elif len(arguments) > ARGUMENTS_MAX:
            call["problem"] = TOO_LONG
        else:
            try:
                parsed = scrub(json.loads(arguments))
            except (ValueError, RecursionError):
                parsed = None
            if isinstance(parsed, dict):
                call.update(input=parsed, arguments=json.dumps(parsed, separators=(",", ":")))
            else:
                call["problem"] = NOT_AN_OBJECT
        calls.append(call)
    return cut(clean(content or ""), TEXT_CUT), calls, max(len(listed or ()) - CALLS_PER_REPLY, 0)


# ── the loop ──────────────────────────────────────────────────────────────


class Agent:
    """One turn's loop. `host`/`port` reach the model, `servers` are the open `Server`s, `env` is the shell commands' environment; `emit` prints a stream-json line."""

    def __init__(self, host, port, model, files, env, servers=(), *, queue=False, emit=emit, calls_max=CALLS_MAX, shell_seconds=SHELL_SECONDS):
        self.host, self.port, self.model, self.files, self.env = host, port, model, files, env
        self.emit, self.calls_max, self.shell_seconds = emit, calls_max, shell_seconds
        self.routes = {}
        specs, total, left_out = [SHELL_SPEC], containers(SHELL_SPEC), 0
        for server in servers:
            for full, (name, spec) in server.tools.items():
                size = containers(spec)
                if total + size > TOOLS_CONTAINERS:
                    left_out += 1
                    continue
                total += size
                self.routes[full] = (server, name)
                specs.append(spec)
        if left_out:
            _log(f"{left_out} tools of the databases are left out: their schemas together hold more than {TOOLS_CONTAINERS} objects and lists")
        self.specs = specs
        self.offered = {SHELL_TOOL, *self.routes}
        self.system = {"role": "system", "content": claude.role(queue)}

    def body(self, earlier, current):
        """The request's bytes and the earlier turns it holds: the current turn squeezed to `TURN_BUDGET`, the newest earlier turns that fit with it into the history's budget."""
        current = squeezed(repaired(current), TURN_BUDGET)
        kept = within_budget(earlier, current, fixed=_size([self.system]))
        messages = [self.system, *(m for turn in kept for m in turn), *current]
        return json.dumps({"model": self.model, "messages": messages, "tools": self.specs, "stream": False}, separators=(",", ":")).encode("ascii"), kept

    async def run_tool(self, call):
        try:
            return await self._run_tool(call)
        except Exception as error:
            _log(f"a tool call failed ({kind_of(error)})")
            return TOOL_FAILED, True

    async def _run_tool(self, call):
        if call["problem"] is not None:
            return call["problem"], True
        if call["name"] == SHELL_TOOL:
            command = call["input"].get("command")
            if set(call["input"]) != {"command"} or not isinstance(command, str) or not command:
                return NO_COMMAND, True
            return await asyncio.to_thread(run_shell, command, self.files, self.env, self.shell_seconds)
        server, name = self.routes[call["name"]]
        return await server.call(name, call["input"])

    async def turn(self, earlier, question):
        """Run one turn: (the turns to keep, with this one; the `result` line's fields)."""
        current = [{"role": "user", "content": clean(question)}]
        earlier, calls, answer = list(earlier), 0, ""
        dropped_said = False
        try:
            while True:
                if calls >= self.calls_max:
                    raise _Ended(CALLS_LIMIT)
                body, kept = self.body(earlier, current)
                if len(kept) < len(earlier) and not dropped_said:
                    dropped_said = True
                    _log(f"{len(earlier) - len(kept)} earlier turns are left out of the request to keep it within {HISTORY_BUDGET} characters")
                calls += 1
                try:
                    data = await asyncio.to_thread(call_model, self.host, self.port, body)
                except ModelRefused as refused:
                    if refused.status not in (400, 413) or not kept or calls >= self.calls_max:
                        raise
                    calls += 1
                    data = await asyncio.to_thread(call_model, self.host, self.port, self.body([], current)[0])
                    _log(f"the endpoint refused the request with {len(kept)} earlier turns (status {refused.status}) and took it without them: the history starts again from this turn")
                    earlier = []
                text, tool_calls, ignored = parse_reply(data, self.offered)
                blocks = [{"type": "text", "text": text}] if text or not tool_calls else []
                blocks += [{"type": "tool_use", "id": c["id"], "name": c["name"], "input": c["input"] if c["input"] is not None else c["arguments"]} for c in tool_calls]
                self.emit({"type": "assistant", "message": {"role": "assistant", "content": blocks}})
                if not tool_calls:
                    current.append({"role": "assistant", "content": text})
                    answer = text
                    break
                current.append({"role": "assistant", "content": text or None, "tool_calls": [{"id": c["id"], "type": "function", "function": {"name": c["name"], "arguments": c["arguments"]}} for c in tool_calls]})
                for index, call in enumerate(tool_calls):
                    if calls >= self.calls_max:
                        output, is_error = NOT_RUN_LIMIT, True
                    else:
                        output, is_error = await self.run_tool(call)
                    output = cut(clean(output), OUTPUT_CUT)
                    self.emit({"type": "user", "message": {"role": "user", "content": [{"type": "tool_result", "tool_use_id": call["id"], "content": output, "is_error": is_error}]}})
                    if ignored and index == len(tool_calls) - 1:
                        # The model is told in its last answer; the chat gets the note as a result of its own.
                        output = cut(output + "\n" + ignored_message(ignored), OUTPUT_CUT + 200)
                        self.emit({"type": "user", "message": {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "ignored", "content": ignored_message(ignored), "is_error": True}]}})
                    current.append({"role": "tool", "tool_call_id": call["id"], "content": output})
            result = {"is_error": False, "result": answer}
        except ModelRefused as refused:
            message = status_message(refused.status) if refused.status is not None else UNREACHABLE
            _log(message)
            result = {"is_error": True, "result": message, "api_error_status": refused.status if refused.status is not None else "unreachable"}
        except BadReply as bad:
            _log(bad_reply_message(bad))
            result = {"is_error": True, "result": bad_reply_message(bad)}
        except _Ended as ended:
            _log(str(ended))
            result = {"is_error": True, "result": str(ended)}
        except Exception as error:
            _log(f"{TURN_FAILED} ({kind_of(error)})")
            result = {"is_error": True, "result": TURN_FAILED}
        return to_keep(earlier, current), {**result, "num_turns": calls}


class _Ended(Exception):
    pass


# ── the process ───────────────────────────────────────────────────────────


def settings(environ):
    """(gateway, model, databases, queue) from the environment `chat.openai` gives; raises ValueError naming the variable."""
    try:
        gateway = str(ipaddress.IPv4Address(environ.get("SDP_CHAT_GATEWAY", "")))
    except ValueError:
        raise ValueError("SDP_CHAT_GATEWAY must be an IPv4 address") from None
    model = environ.get("SDP_CHAT_MODEL", "")
    if not MODEL_NAME.fullmatch(model):
        raise ValueError("SDP_CHAT_MODEL must be a model name")
    databases = [name for name in environ.get("SDP_CHAT_DATABASES", "").split(",") if name]
    if any(name not in claude.DATABASES for name in databases) or len(set(databases)) != len(databases):
        raise ValueError("SDP_CHAT_DATABASES must be a comma list of the databases")
    queue = environ.get("SDP_CHAT_QUEUE", "0")
    if queue not in ("0", "1"):
        raise ValueError("SDP_CHAT_QUEUE must be 0 or 1")
    return gateway, model, databases, queue == "1"


def question_of(data):
    """The user's text of the stream-json line on standard input, or None."""
    try:
        doc = json.loads(data.decode("utf-8").splitlines()[0])
        content = doc["message"]["content"]
    except (UnicodeDecodeError, ValueError, IndexError, KeyError, TypeError, RecursionError):
        return None
    return content if isinstance(content, str) and content else None


async def _run(gateway, model, databases, home, files, question, queue=False):
    started = time.monotonic()
    servers = await open_servers(databases, gateway, queue)
    try:
        agent = Agent(gateway, claude.MODEL_PORT, model, files, dict(os.environ), servers, queue=queue)
        turns, result = await agent.turn(load_history(home), question)
    finally:
        for server in servers:
            await server.close()
    save_history(home, turns)
    return {"type": "result", "subtype": "error" if result["is_error"] else "success", **result, "duration_ms": int((time.monotonic() - started) * 1000)}


def main(argv=None, environ=None):
    signal.signal(signal.SIGTERM, _on_term)
    parser = argparse.ArgumentParser(prog="agent")
    parser.add_argument("--home", required=True)
    parser.add_argument("--files", required=True)
    arguments = parser.parse_args(argv)
    started = time.monotonic()
    try:
        gateway, model, databases, queue = settings(os.environ if environ is None else environ)
    except ValueError as error:
        _log(f"refusing to run: {error}")
        emit({"type": "result", "subtype": "error", "is_error": True, "result": str(error), "duration_ms": 0, "num_turns": 0})
        return 2
    question = question_of(sys.stdin.buffer.read(STDIN_MAX_BYTES + 1)[:STDIN_MAX_BYTES])
    if question is None:
        _log("standard input holds no question")
        emit({"type": "result", "subtype": "error", "is_error": True, "result": "no question", "duration_ms": int((time.monotonic() - started) * 1000), "num_turns": 0})
        return 2
    emit(asyncio.run(_run(gateway, model, databases, arguments.home, arguments.files, question, queue)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
