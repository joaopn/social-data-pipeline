"""The Claude Code back end of the chat's runner: one `claude -p` process per turn, its stream-json output turned into the chat's events.

The process gets an environment built from scratch (`environment`): never the runner's own. Its credential is a fixed placeholder (`PLACEHOLDER_TOKEN`); the gateway puts the real token in. The flags (`command`) were measured with the pinned Claude Code 2.1.328 against a stand-in API, in an empty home:

- Permissions: without a permission flag, and with `--permission-mode dontAsk` alone, a Bash call is denied (`permission_denied`); `--permission-mode bypassPermissions` runs it, and so does `dontAsk` with `--allowedTools` naming the tools. The runner uses the latter: the six tools and `mcp__<server>` of each database are allowed, anything else is denied without a prompt.
- Settings: with the default setting sources a `.claude/settings.json` planted in the working folder or the home ran its hooks and a planted skill was loaded; `--setting-sources ""` loaded neither. The three plugins built into the binary loaded in every case until `--settings` disabled them (`SETTINGS`, given inline, so no file of the chat can change it; it also switches hooks off). `CLAUDE_CODE_DISABLE_AUTO_MEMORY=1` turned the auto-memory folder off (`memory_paths` null). A planted `CLAUDE.md` in either folder reached no request in any variant measured. MCP and the token worked with all of these.
- The home: with `HOME` the chat's `home/`, a `.bashrc` and a `.bash_profile` planted there ran in the next turn's Bash tool (a marker file written, a variable set); `.profile` did not. Agents, commands, output styles, skills and a `settings.json` planted in `CLAUDE_CONFIG_DIR`, in `$HOME/.claude` or in the working folder's `.claude/` were not loaded with `--setting-sources ""` (the `init` line's lists and the requests unchanged). So `HOME` is a fresh empty folder in the chat's scratch folder, made anew for every turn, while `CLAUDE_CONFIG_DIR` stays the chat's `home/`, where the session files live; measured again, nothing planted took effect and the session still resumed.
"""

import contextlib
import json
import math
import os
import select
import signal
import stat
import subprocess
import threading
import time

EXECUTABLE = "/usr/local/bin/claude"
# Not a token: the sandbox holds no credential, and the gateway replaces the `Authorization` header with its own.
PLACEHOLDER_TOKEN = "sdp-chat-placeholder-the-gateway-holds-the-real-token"
TOOLS = ("Bash", "Read", "Write", "Edit", "Glob", "Grep")
MODEL_PORT = 8080
# Each database: its gateway port, the MCP transport and the path of its MCP server.
DATABASES = {"postgres": (8001, "sse", "/sse"), "mongo": (8002, "http", "/mcp"), "starrocks": (8003, "http", "/mcp")}
PATH = "/usr/local/bin:/usr/bin:/bin"
BUILTIN_PLUGINS = ("cc-plugin-agents-md@builtin", "cc-plugin-privacy@builtin", "cc-plugin-plugin-authoring@builtin")
SETTINGS = json.dumps({"disableAllHooks": True, "enabledPlugins": {name: False for name in BUILTIN_PLUGINS}}, separators=(",", ":"))
ROLE = "You are the assistant of a research data deployment. You answer questions about the operator's databases through the MCP tools of those databases, which are read-only. When you make a plot, save it as a PNG file in the current folder and name the file in your answer. You cannot reach the internet."
MCP_NAME = "mcp.json"
STDERR_NAME = "backend.log"
STDERR_MAX_BYTES = 1024 * 1024
TOOL_CUT = 2000
STOP_GRACE_SECONDS = 5.0
# How long the reader of the output and the copier of the error output may take to end after the group was ended (a process that detached itself can hold the pipes open).
JOIN_SECONDS = 5.0
LINE_MAX_BYTES = 4 * 1024 * 1024
_POLL_SECONDS = 0.2

MODEL_NOT_REACHED = "the model could not be reached"


def mcp_config(databases, gateway):
    """The `--mcp-config` document: one server per database, reached through the gateway's port for it."""
    servers = {}
    for name in databases:
        port, kind, path = DATABASES[name]
        servers[name] = {"type": kind, "url": f"http://{gateway}:{port}{path}"}
    return {"mcpServers": servers}


def command(executable, home, session, resume, databases):
    """The argument list of one turn: `--session-id` on a chat's first turn, `--resume` afterwards."""
    allowed = ",".join([*TOOLS, *(f"mcp__{name}" for name in databases)])
    return [
        executable, "-p", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose",
        "--mcp-config", os.path.join(home, MCP_NAME), "--strict-mcp-config", "--tools", ",".join(TOOLS),
        "--permission-mode", "dontAsk", "--allowedTools", allowed,
        "--setting-sources", "", "--settings", SETTINGS,
        "--append-system-prompt", ROLE,
        "--resume" if resume else "--session-id", session,
    ]


def environment(home, config, tmp, gateway):
    """The whole environment of the process, built from scratch: `home` is the turn's fresh `HOME`, `config` the chat's `home/` (Claude Code's settings and session files)."""
    return {
        "HOME": home, "CLAUDE_CONFIG_DIR": config,
        "CLAUDE_CODE_TMPDIR": tmp, "TMPDIR": tmp, "MPLCONFIGDIR": os.path.join(tmp, "matplotlib"),
        "PATH": PATH,
        "ANTHROPIC_BASE_URL": f"http://{gateway}:{MODEL_PORT}",
        "CLAUDE_CODE_OAUTH_TOKEN": PLACEHOLDER_TOKEN,
        "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1", "DISABLE_TELEMETRY": "1", "DISABLE_AUTOUPDATER": "1",
        "DISABLE_ERROR_REPORTING": "1", "CLAUDE_CODE_DISABLE_AUTO_MEMORY": "1",
    }


def _cut(text, limit=TOOL_CUT):
    return text if len(text) <= limit else text[:limit]


def _result_text(content):
    if isinstance(content, str):
        return content
    parts = []
    for block in content if isinstance(content, list) else ():
        if isinstance(block, dict) and block.get("type") == "text" and isinstance(block.get("text"), str):
            parts.append(block["text"])
        elif isinstance(block, dict):
            parts.append(f"[{block.get('type') if isinstance(block.get('type'), str) else 'block'}]")
    return "\n".join(parts)


def _number(value, kind):
    """`value` when it is a finite number of `kind`, else None (`NaN`, `Infinity` and `1e999` parse as floats)."""
    if isinstance(value, bool) or not isinstance(value, kind):
        return None
    return value if not isinstance(value, float) or math.isfinite(value) else None


class Translator:
    """Turns the decoded stream-json lines of one turn into events (`(type, fields)` pairs). The `result` line is kept as `end` (its fields) and not returned, so that the runner writes it after the files of the turn; `init` says that the session exists."""

    def __init__(self):
        self.end = None
        self.init = False

    def events(self, doc):
        kind = doc.get("type")
        message = doc.get("message") if isinstance(doc.get("message"), dict) else {}
        content = message.get("content") if isinstance(message.get("content"), list) else []
        out = []
        if kind == "system" and doc.get("subtype") == "init":
            self.init = True
        elif kind == "assistant":
            for block in content:
                if not isinstance(block, dict):
                    continue
                if block.get("type") == "text" and isinstance(block.get("text"), str) and block["text"]:
                    out.append(("text", {"text": block["text"]}))
                elif block.get("type") == "tool_use":
                    name = block.get("name") if isinstance(block.get("name"), str) else ""
                    try:
                        shown = json.dumps(block.get("input"), ensure_ascii=False, separators=(",", ":"))
                    except (TypeError, ValueError):
                        shown = ""
                    out.append(("tool", {"name": _cut(name, 200), "input": _cut(shown)}))
        elif kind == "user":
            for block in content:
                if isinstance(block, dict) and block.get("type") == "tool_result":
                    out.append(("tool_result", {"text": _cut(_result_text(block.get("content"))), "is_error": block.get("is_error") is True}))
        elif kind == "result":
            self.end = {"is_error": doc.get("is_error") is True, "cost_usd": _number(doc.get("total_cost_usd"), (int, float)), "duration_ms": _number(doc.get("duration_ms"), int), "turns": _number(doc.get("num_turns"), int)}
            if doc.get("is_error") is True and doc.get("api_error_status") is not None:
                out.append(("error", {"message": MODEL_NOT_REACHED}))
        return out


def _chunks(fd, stop):
    """The bytes read from the pipe `fd` until its end, or until `stop` is set (looked at between waits of at most 0.2 s)."""
    while not stop.is_set():
        ready, _, _ = select.select([fd], [], [], _POLL_SECONDS)
        if not ready:
            continue
        try:
            chunk = os.read(fd, 65536)
        except OSError:
            return
        if not chunk:
            return
        yield chunk


def read_lines(fd, limit, on_line, on_dropped, stop):
    """Read the pipe `fd` to its end (or until `stop` is set, which is looked at before each line is handed on): each line of at most `limit` bytes is handed to `on_line` as bytes; a longer line is dropped whole (`on_dropped`), never held."""
    buffer = bytearray()
    dropping = False
    for chunk in _chunks(fd, stop):
        buffer += chunk
        while True:
            end = buffer.find(b"\n")
            if end < 0:
                if len(buffer) > limit:
                    if not dropping:
                        on_dropped()
                    dropping = True
                    buffer.clear()
                break
            if stop.is_set():
                return
            line = bytes(buffer[: end + 1])
            del buffer[: end + 1]
            if dropping or len(line) > limit:
                if not dropping:
                    on_dropped()
                dropping = False
                continue
            on_line(line)
    if buffer and not dropping and not stop.is_set():
        on_line(bytes(buffer))


def _copy_bounded(source_fd, fd, limit, stop):
    written = 0
    for chunk in _chunks(source_fd, stop):
        if written < limit:
            piece = chunk[: limit - written]
            with contextlib.suppress(OSError):
                os.write(fd, piece)
            written += len(piece)


def _start_owning(target, args, fds):
    """Start a thread that runs `target(*args)` and closes the descriptors `fds` (its own) when it ends, whenever that is; when it cannot be started they are closed here."""

    def run():
        try:
            target(*args)
        finally:
            for fd in fds:
                with contextlib.suppress(OSError):
                    os.close(fd)

    thread = threading.Thread(target=run, daemon=True)
    try:
        thread.start()
    except BaseException:
        for fd in fds:
            with contextlib.suppress(OSError):
                os.close(fd)
        raise
    return thread


def _open_stderr(chat_fd):
    fd = os.open(STDERR_NAME, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, 0o600, dir_fd=chat_fd)
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        raise OSError("not a regular file")
    return fd


class Process:
    """One turn's `claude` process, started in a group of its own. `run(on_doc)` blocks until it ended and its group is gone; `stop(reason)` (any thread) ends the group: TERM, then KILL after the grace time. A child of it that ignores TERM is killed too; one that left the group (`setsid`) is not followed, and its hold on the pipes is let go after `join` seconds."""

    def __init__(self, argv, env, cwd, stdin_line, chat_fd, *, line_max=LINE_MAX_BYTES, grace=STOP_GRACE_SECONDS, stderr_max=STDERR_MAX_BYTES, join=JOIN_SECONDS):
        self.argv, self.env, self.cwd, self.stdin_line = argv, env, cwd, stdin_line
        self.chat_fd, self.line_max, self.grace, self.stderr_max, self.join = chat_fd, line_max, grace, stderr_max, join
        self.dropped = 0
        self.stopped = None
        self.returncode = None
        self._lock = threading.Lock()
        self._proc = None
        self._killer = None

    def run(self, on_doc):
        stderr_fd = _open_stderr(self.chat_fd)
        try:
            self._proc = proc = subprocess.Popen(self.argv, env=self.env, cwd=self.cwd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True, close_fds=True)
        except BaseException:
            os.close(stderr_fd)
            raise
        try:
            self._supervise(proc, on_doc, stderr_fd)
        except BaseException:
            # Whatever failed after the start, the process is not left running unsupervised.
            self._end_group()
            proc.wait()
            raise
        finally:
            for pipe in (proc.stdin, proc.stdout, proc.stderr):
                with contextlib.suppress(OSError):
                    pipe.close()

    def _supervise(self, proc, on_doc, stderr_fd):
        with self._lock:
            late = self.stopped is not None
        if late:
            self._start_killer()
        stop = threading.Event()
        threads = []

        def line(raw):
            try:
                doc = json.loads(raw.decode("utf-8"))
                if not isinstance(doc, dict):
                    raise ValueError("not an object")
                on_doc(doc)
            except Exception:
                self.dropped += 1

        # Each thread reads its own duplicate of its pipe and closes it itself, so the pipes can be closed here at any time: a thread that is still running never reads a descriptor number that was given to something else since. The stop flag is set on every way out, a failed start of the second thread included.
        try:
            try:
                error_fd = os.dup(proc.stderr.fileno())
            except BaseException:
                os.close(stderr_fd)
                raise
            threads.append(_start_owning(_copy_bounded, (error_fd, stderr_fd, self.stderr_max, stop), (error_fd, stderr_fd)))
            with contextlib.suppress(OSError):
                proc.stdin.write(self.stdin_line)
            with contextlib.suppress(OSError):
                proc.stdin.close()
            output_fd = os.dup(proc.stdout.fileno())
            threads.append(_start_owning(read_lines, (output_fd, self.line_max, line, self._count_dropped, stop), (output_fd,)))
            self.returncode = proc.wait()
            with self._lock:
                killer = self._killer
            if killer is not None:
                killer.join()
            self._end_group()
            # One deadline for both: a process that left the group holding both pipes costs one wait, not two.
            deadline = time.monotonic() + self.join
            for thread in threads:
                thread.join(max(0.0, deadline - time.monotonic()))
        finally:
            stop.set()
            deadline = time.monotonic() + 2 * _POLL_SECONDS
            for thread in threads:
                thread.join(max(0.0, deadline - time.monotonic()))

    @property
    def started(self):
        return self._proc is not None

    def _count_dropped(self):
        self.dropped += 1

    def stop(self, reason):
        """Ask the turn to end; the first reason is kept. Returns False when it was already asked."""
        with self._lock:
            if self.stopped is not None:
                return False
            self.stopped = reason
            started = self._proc is not None
        if started:
            self._start_killer()
        return True

    def _start_killer(self):
        with self._lock:
            if self._killer is not None:
                return
            self._killer = threading.Thread(target=self._end_group, daemon=True)
            self._killer.start()

    def _group_alive(self):
        self._proc.poll()
        try:
            os.killpg(self._proc.pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    def _end_group(self):
        """TERM to the group, KILL after the grace time while any of it is left. Nothing is sent once the group is gone (its id could be reused)."""
        if not self._group_alive():
            return
        with contextlib.suppress(ProcessLookupError):
            os.killpg(self._proc.pid, signal.SIGTERM)
        deadline = time.monotonic() + self.grace
        while time.monotonic() < deadline:
            if not self._group_alive():
                return
            time.sleep(0.05)
        with contextlib.suppress(ProcessLookupError):
            os.killpg(self._proc.pid, signal.SIGKILL)
        deadline = time.monotonic() + self.grace
        while self._group_alive() and time.monotonic() < deadline:
            time.sleep(0.05)


class ClaudeCode:
    """The back end: `gateway` is the gateway's address, `databases` the names of the attached databases."""

    name = "claude-code"

    def __init__(self, gateway, databases, *, executable=EXECUTABLE, line_max=LINE_MAX_BYTES, grace=STOP_GRACE_SECONDS, stderr_max=STDERR_MAX_BYTES, join=JOIN_SECONDS):
        self.gateway, self.databases, self.executable = gateway, tuple(databases), executable
        self.line_max, self.grace, self.stderr_max, self.join = line_max, grace, stderr_max, join

    def turn(self, chat_fd, config, home, files, tmp, text, session, resume):
        """A `Process` for one turn (not started) and its `Translator`. `config` is the chat's `home/`, into which `mcp.json` is written again first; `home` the turn's fresh `HOME`."""
        payload = json.dumps(mcp_config(self.databases, self.gateway), indent=2).encode("utf-8")
        home_fd = os.open("home", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=chat_fd)
        try:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(MCP_NAME, dir_fd=home_fd)
            fd = os.open(MCP_NAME, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=home_fd)
            try:
                os.write(fd, payload)
            finally:
                os.close(fd)
        finally:
            os.close(home_fd)
        line = (json.dumps({"type": "user", "message": {"role": "user", "content": text}}, ensure_ascii=False) + "\n").encode("utf-8")
        argv = command(self.executable, config, session, resume, self.databases)
        process = Process(argv, environment(home, config, tmp, self.gateway), files, line, chat_fd, line_max=self.line_max, grace=self.grace, stderr_max=self.stderr_max, join=self.join)
        return process, Translator()
