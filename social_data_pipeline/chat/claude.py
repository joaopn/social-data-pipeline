"""The Claude Code back end of the chat's runner: one `claude -p` process per turn, its stream-json output turned into the chat's events.

The process gets an environment built from scratch (`environment`): never the runner's own. Its credential is a fixed placeholder (`PLACEHOLDER_TOKEN`); the gateway puts the real token in. The flags (`command`) were measured with the pinned Claude Code 2.1.328 against a stand-in API, in an empty home:

- Permissions: without a permission flag, and with `--permission-mode dontAsk` alone, a Bash call is denied (`permission_denied`); `--permission-mode bypassPermissions` runs it, and so does `dontAsk` with `--allowedTools` naming the tools. The runner uses the latter: the six tools and `mcp__<server>` of each database (and `mcp__jobs` with the query queue) are allowed, anything else is denied without a prompt.
- Settings: with the default setting sources a `.claude/settings.json` planted in the working folder or the home ran its hooks and a planted skill was loaded; `--setting-sources ""` loaded neither. The three plugins built into the binary loaded in every case until `--settings` disabled them (`SETTINGS`, given inline, so no file of the chat can change it; it also switches hooks off). `CLAUDE_CODE_DISABLE_AUTO_MEMORY=1` turned the auto-memory folder off (`memory_paths` null). A planted `CLAUDE.md` in either folder reached no request in any variant measured. MCP and the token worked with all of these.
- The home: with `HOME` the chat's `home/`, a `.bashrc` and a `.bash_profile` planted there ran in the next turn's Bash tool (a marker file written, a variable set); `.profile` did not. Agents, commands, output styles, skills and a `settings.json` planted in `CLAUDE_CONFIG_DIR`, in `$HOME/.claude` or in the working folder's `.claude/` were not loaded with `--setting-sources ""` (the `init` line's lists and the requests unchanged). So `HOME` is a fresh empty folder in the chat's scratch folder, made anew for every turn, while `CLAUDE_CONFIG_DIR` stays the chat's `home/`, where the session files live; measured again, nothing planted took effect and the session still resumed.
- The transcript: Claude Code keeps its own record of a session (every message, tool call and result, uncut, in its own format) at `<CLAUDE_CONFIG_DIR>/projects/<the working folder, every character but A-Z, a-z and 0-9 made "-">/<session id>.jsonl` (measured with 2.1.328 against a stand-in API: a first turn made it, a resumed turn appended to the same file). `transcript` names it for the runner's `chat.transcript`.
- The title (`ClaudeCode.title`, the command of `title_command`): measured with 2.1.328 against a stand-in API that recorded each request, in a folder of its own (`HOME`, `CLAUDE_CONFIG_DIR`, `TMPDIR` and the working folder all in it), with hooks, a skill, a `CLAUDE.md` and a `.mcp.json` planted in the config folder, in `$HOME/.claude` and in the working folder. One request was sent (`POST /v1/messages?beta=true`, model `claude-haiku-5-5`, keys all among those the gateway takes, its `anthropic-beta` values all among `chat_host.BETA_ALLOWED`), with no tool definitions (`tools` empty) and no MCP server (the `init` line's `tools`, `mcp_servers`, `plugins`, `skills` and `slash_commands` empty with `--disable-slash-commands`; without it the binary's own skills were listed, though none reached the request); no hook ran and no planted file reached the request. With `--system-prompt` the request was 2,176 bytes, its system prompt 291 bytes of text (481 bytes as JSON, with the billing line and one line of the binary's own; about 545 and 73 tokens at 4 bytes a token); with `--append-system-prompt` instead the default system prompt came too: 9,623 bytes, the system prompt 7,695 bytes. The binary adds a message of about 1 KB with the environment (the working folder, the platform, the date) either way. `--effort low` was taken (`output_config.effort` `low` instead of `medium`). With `--no-session-persistence` nothing was written under `projects/` (without it a session file was); the config folder got only `.claude.json` and its backup, the scratch folder a probe file, all removed with the folder. `--input-format stream-json` is refused with `--output-format json`, so the question is given as plain text on standard input.
"""

import contextlib
import json
import math
import os
import re
import select
import shutil
import signal
import stat
import subprocess
import tempfile
import threading
import time

EXECUTABLE = "/usr/local/bin/claude"
# Not a token: the sandbox holds no credential, and the gateway replaces the `Authorization` header with its own.
PLACEHOLDER_TOKEN = "sdp-chat-placeholder-the-gateway-holds-the-real-token"
TOOLS = ("Bash", "Read", "Write", "Edit", "Glob", "Grep")
MODEL_PORT = 8080
# The models a chat may be given: Claude Code's own aliases, given as `--model`; a chat without one runs on Claude Code's default.
MODELS = ("opus", "sonnet", "haiku")
# Each database: its gateway port, the MCP transport and the path of its MCP server.
DATABASES = {"postgres": (8001, "sse", "/sse"), "mongo": (8002, "http", "/mcp"), "starrocks": (8003, "http", "/mcp")}
PATH = "/usr/local/bin:/usr/bin:/bin"
BUILTIN_PLUGINS = ("cc-plugin-agents-md@builtin", "cc-plugin-privacy@builtin", "cc-plugin-plugin-authoring@builtin")
SETTINGS = json.dumps({"disableAllHooks": True, "enabledPlugins": {name: False for name in BUILTIN_PLUGINS}}, separators=(",", ":"))
# The role text (`role`) is the instructions, which say how to work and may be replaced (INSTRUCTIONS, QUEUE_INSTRUCTIONS are the built-in ones), around fixed facts of this deployment that every chat is told (DEPLOYMENT_FACTS, QUEUE_FACTS).
INSTRUCTIONS = (
    "You are the assistant of a research data deployment. You answer questions about the operator's databases through the MCP tools of those databases, which are read-only.\n\n"
    "Before you query a table or collection for the first time in a chat, look at how it is laid out, with small queries: its columns and their types, a few rows (at most 5), and its indexes and keys (on StarRocks `SHOW CREATE TABLE`, which shows its indexes, sort key and partitions; on PostgreSQL its indexes; on MongoDB the collection's indexes). Note whether it has partitions: a table without them is scanned whole unless an index or the sort key narrows the scan. Columns that name a row's source file or period (like `dataset`, the dump month, in Reddit tables) are usually indexed and are the way to count per period.\n\n"
    "Weigh two costs before you write a query. Reading: StarRocks stores each column apart, so a query reads only the columns it names, and a filter on an indexed or sort-key column reads less. Memory, which columnar storage does not reduce: COUNT(DISTINCT ...), a GROUP BY on a high-cardinality column (such as `author`, `link_id` or `id`) and joins hold one entry per distinct key or group, so their memory grows with the number of distinct keys or groups, not with the columns read. Estimate that number, from table metadata or a small sample, before you choose between one query over everything and a loop of smaller queries, for example one per value of an indexed categorical column such as `dataset`.\n\n"
    "For a query with COUNT(DISTINCT ...), a GROUP BY on a high-cardinality column or a join of large tables, on StarRocks, write at least two versions with different strategies (for example the single query and a per-`dataset` or per-batch loop) and compare them with `EXPLAIN COSTS` before you run either. Report for each its estimated CPU, memory, partitions and tablets scanned, and rows. The estimates are rough: they do not reflect bitmap-index skipping and may rest on stale statistics (check a count they show, such as the number of months, against the real one). When the choice matters, time one chunk (one value of the loop) on the read-only connection and look at it with `analyze_query`, then scale from that measurement: one measured chunk is worth more than any number of estimates. For simple counts on indexed columns, skip this; it only adds noise.\n\n"
    "In a cost comparison, label each claim as table metadata, a measurement, an optimizer estimate or an inference."
)
DEPLOYMENT_FACTS = "Make every chart with Altair (a Vega-Lite chart) unless the operator explicitly asks for another library; a request for an image or a PNG is still an Altair chart. Save it twice in the current folder, as `<name>.vl.json` (`chart.save(\"<name>.vl.json\")`, which the web UI draws as an interactive chart) and as `<name>.png` (`chart.save(\"<name>.png\")`), and name both files in your answer. Keep the files you make for the operator in the current folder (subfolders are fine: the web UI lists `.py`, `.png`, `.jpg`, `.svg`, `.json`, `.csv`, `.txt` and `.md` files up to three folders down, and folders whose names start with `.` are not looked into), and save the script that makes a chart beside it as `<name>.py`, with the same `<name>` as its `.vl.json` and `.png`, so the chart's Code button finds it. To use query results in a script, query them from the script with `sdp_data.read_query(server, query)` (it returns the rows through the same read-only connection your tools use) instead of copying values by hand; the shell has no other database access. The first argument of `sdp_data.read_query` is the database server (`starrocks`, `postgres` or `mongo`), not a database or schema name like `reddit`: name those inside the query (`reddit.comments`). Altair and vl-convert are installed: if importing them fails, say so instead of using another library. Keep the chart's data inside the chart (at most 50,000 rows; above 5,000 call `alt.data_transformers.disable_max_rows()` first; never loaded from an address or a file, no images or links) and leave its colours and background to the page. When a log axis helps (values spanning several orders of magnitude), make the y scale log (`alt.Scale(type=\"log\")`) in a single chart rather than two panels; the page adds a linear/log switch. You cannot reach the internet. Never suggest sending data or files to websites or services outside this deployment."
# The query queue (`SDP_CHAT_QUEUE=1`): the scheduler's assistant endpoint, reached through the gateway's port for it, as the MCP server of this name; its results under QUEUE_RESULTS.
QUEUE_SERVER = "jobs"
QUEUE_PORT = 8004
QUEUE_PATH = "/assistant/mcp"
QUEUE_RESULTS = "/assistant/results"
# The queue's facts, added to the role text with the queue, and its built-in instructions. The sandbox image has no curl (measured with `command -v` in the built image).
QUEUE_FACTS = (
    f"The operator's query scheduler is attached as the MCP server `{QUEUE_SERVER}`. `list_targets` names its targets. "
    "A submitted query runs once it is approved, either by the scheduler itself when its target's auto-accept is on and has room under its limit, or by the operator in the Queue window of the web UI; you cannot approve it yourself. After a submission, tell the operator the job id and that the query may already be approved by auto-accept or else waits for their approval in the Queue window, and end your turn instead of waiting for it. "
    "`query_status` tells where a query stands, and `query_cancel` withdraws one of yours while it is still pending. "
    f"When a query is done, the parts of its result are listed at http://<gateway>:{QUEUE_PORT}{QUEUE_RESULTS}/<job id> (JSON) and each part is at http://<gateway>:{QUEUE_PORT}{QUEUE_RESULTS}/<job id>/<part number>, where <gateway> is the address in the environment variable SDP_CHAT_GATEWAY: "
    "fetch them from the shell with Python's urllib (curl is not installed) and save them in the current folder. Only your own queries are visible there."
)
QUEUE_INSTRUCTIONS = "Send a query to the scheduler with its submit tools when it must read many columns of very large tables, hold many distinct keys or groups, join large tables, or when the read-only query is refused or times out; run the others on the read-only connection. Before you submit a query, compare at least two versions with `EXPLAIN COSTS` as above, among them a single-query version and a per-dataset (or per-batch) version, report the figures for each, and submit the one you chose, saying why."
# The query time limit (`SDP_CHAT_QUERY_SECONDS`, whole seconds from 1 to QUERY_SECONDS_MAX): the database stops a read-only query on MongoDB or StarRocks after it (PostgreSQL's MCP server ignores it); the waits for an answer (the agent's tool calls, `sdp_data.read_query`) are that plus QUERY_MARGIN_SECONDS. The sentences the role text gets with it.
QUERY_ENV = "SDP_CHAT_QUERY_SECONDS"
QUERY_SECONDS_MAX = 3600
QUERY_MARGIN_SECONDS = 10
QUERY_LIMIT_ROLE = "A read-only query on MongoDB or StarRocks, through your tools or `sdp_data.read_query`, is stopped by the database after {seconds} seconds."
QUERY_LIMIT_QUEUE_ROLE = "Submit a query on MongoDB or StarRocks that may take longer than {seconds} seconds to the scheduler instead of running it on the read-only connection."
MCP_NAME = "mcp.json"
STDERR_NAME = "backend.log"
STDERR_MAX_BYTES = 1024 * 1024
# Where Claude Code keeps a session's transcript, under the chat's folder: `home/` (its CLAUDE_CONFIG_DIR), then `projects/`.
TRANSCRIPT_FOLDERS = ("home", "projects")
_NOT_ALPHANUMERIC = re.compile(r"[^A-Za-z0-9]")
TOOL_CUT = 2000
STOP_GRACE_SECONDS = 5.0
# How long the reader of the output and the copier of the error output may take to end after the group was ended (a process that detached itself can hold the pipes open).
JOIN_SECONDS = 5.0
LINE_MAX_BYTES = 4 * 1024 * 1024
_POLL_SECONDS = 0.2

MODEL_NOT_REACHED = "the model could not be reached"

# A generated title (`ClaudeCode.title`, and `openai.OpenAICompatible.title` with the same instruction): the model, the instruction, the time limit, the most bytes of output read.
TITLE_MODEL = "haiku"
TITLE_INSTRUCTION = "Write a title of at most six words for a chat that starts with the question below, in the question's language. Leave out words about plotting or charts (plot, plotting, chart, graph, visualize, figure and the like): every chat here makes charts, so they waste the title's few words. Answer with the title only, no quotes."
TITLE_SECONDS = 30.0
TITLE_OUTPUT_MAX = 64 * 1024


class TitleFailed(Exception):
    """A title could not be made; the text names the kind of failure only, never what the back end said."""


def role(queue, query_seconds=None, prompt=None):
    """The role text: the instructions, then DEPLOYMENT_FACTS, and with the query queue QUEUE_FACTS, then the queue's instructions; with the query time limit `query_seconds` a sentence naming it after the facts and one after the queue's instructions (that a longer query goes to the scheduler). `prompt` is None (INSTRUCTIONS and QUEUE_INSTRUCTIONS) or the pair (instructions, queue instructions); each part is stripped, and an empty one is left out with its separator, so the text has no doubled blank lines and no whitespace at either end."""
    instructions, queue_instructions = (INSTRUCTIONS, QUEUE_INSTRUCTIONS) if prompt is None else prompt
    facts = DEPLOYMENT_FACTS if query_seconds is None else f"{DEPLOYMENT_FACTS} {QUERY_LIMIT_ROLE.format(seconds=query_seconds)}"
    parts = [instructions.strip(), facts]
    if queue:
        queue_text = "\n\n".join(part for part in (QUEUE_FACTS, queue_instructions.strip()) if part)
        parts.append(queue_text if query_seconds is None else f"{queue_text} {QUERY_LIMIT_QUEUE_ROLE.format(seconds=query_seconds)}")
    return "\n\n".join(part for part in parts if part)


def query_seconds(environ):
    """The query time limit of `SDP_CHAT_QUERY_SECONDS` in `environ`: None when it is missing or empty, else a whole number of seconds from 1 to 3600. Raises ValueError naming the variable."""
    text = environ.get(QUERY_ENV, "")
    if not text:
        return None
    if not (text.isascii() and text.isdigit() and len(text) <= 4 and 1 <= int(text) <= QUERY_SECONDS_MAX):
        raise ValueError(f"{QUERY_ENV} must be a whole number of seconds from 1 to {QUERY_SECONDS_MAX}")
    return int(text)


def queue_url(gateway):
    return f"http://{gateway}:{QUEUE_PORT}{QUEUE_PATH}"


def mcp_config(databases, gateway, queue=False):
    """The `--mcp-config` document: one server per database, reached through the gateway's port for it, and with `queue` the server `jobs` (streamable HTTP)."""
    servers = {}
    for name in databases:
        port, kind, path = DATABASES[name]
        servers[name] = {"type": kind, "url": f"http://{gateway}:{port}{path}"}
    if queue:
        servers[QUEUE_SERVER] = {"type": "http", "url": queue_url(gateway)}
    return {"mcpServers": servers}


def command(executable, home, session, resume, databases, queue=False, model=None, query_seconds=None, prompt=None):
    """The argument list of one turn: `--session-id` on a chat's first turn, `--resume` afterwards; with `queue` the server `jobs` is allowed too and the role text has the queue's facts and instructions; with `model` (one of MODELS, checked by the runner) `--model` and its name, else none (Claude Code's default); with `query_seconds` the role text names the query time limit; `prompt` is the chat's starting prompt (`role`'s pair, None: the built-in one)."""
    allowed = ",".join([*TOOLS, *(f"mcp__{name}" for name in databases), *([f"mcp__{QUEUE_SERVER}"] if queue else [])])
    return [
        executable, "-p", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose",
        "--mcp-config", os.path.join(home, MCP_NAME), "--strict-mcp-config", "--tools", ",".join(TOOLS),
        "--permission-mode", "dontAsk", "--allowedTools", allowed,
        "--setting-sources", "", "--settings", SETTINGS,
        "--append-system-prompt", role(queue, query_seconds, prompt),
        "--resume" if resume else "--session-id", session,
        *(["--model", model] if model else []),
    ]


def title_command(executable):
    """The argument list of a title run (measured, see the module's text): `haiku`, no tools, no MCP server, no setting source but `SETTINGS`, no skills or commands, no session kept, low effort, the instruction as the whole system prompt, one JSON result; the question comes on standard input."""
    return [
        executable, "-p", "--model", TITLE_MODEL, "--tools", "", "--strict-mcp-config",
        "--setting-sources", "", "--settings", SETTINGS, "--disable-slash-commands", "--no-session-persistence",
        "--effort", "low", "--system-prompt", TITLE_INSTRUCTION, "--output-format", "json",
    ]


def run_bounded(argv, env, cwd, data, seconds, limit=TITLE_OUTPUT_MAX):
    """Run `argv` in a process group of its own with `data` on standard input and return its standard output, read to its end within `seconds` and at most `limit` bytes; standard error is not kept. Past the time or the size the group is killed and TitleFailed raised."""
    deadline = time.monotonic() + seconds
    proc = subprocess.Popen(argv, env=env, cwd=cwd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, start_new_session=True, close_fds=True)
    try:
        with contextlib.suppress(OSError):
            proc.stdin.write(data)
        with contextlib.suppress(OSError):
            proc.stdin.close()
        out = bytearray()
        fd = proc.stdout.fileno()
        while True:
            left = deadline - time.monotonic()
            if left <= 0:
                raise TitleFailed("the title run took longer than its limit")
            ready, _, _ = select.select([fd], [], [], min(left, _POLL_SECONDS))
            if not ready:
                continue
            chunk = os.read(fd, 65536)
            if not chunk:
                break
            out += chunk
            if len(out) > limit:
                raise TitleFailed("the title run's output is larger than its limit")
        try:
            proc.wait(max(0.0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            raise TitleFailed("the title run took longer than its limit") from None
        return bytes(out)
    finally:
        # Not reaped yet, the group's id cannot have been given to another: what is left of it is killed.
        if proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
        proc.stdout.close()


def title_of(data):
    """(text, cost) of a title run's JSON result: the text is the `result` of a `result` that is not an error (still model output, which the runner cleans), the cost `total_cost_usd` when it is a finite number, else None. Raises TitleFailed."""
    try:
        doc = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, ValueError, RecursionError):
        raise TitleFailed("the title run's output is not JSON") from None
    if not isinstance(doc, dict) or doc.get("type") != "result" or doc.get("is_error") is not False or not isinstance(doc.get("result"), str):
        raise TitleFailed("the title run gave no title")
    return doc["result"], _number(doc.get("total_cost_usd"), (int, float))


def environment(home, config, tmp, gateway, queue=False, databases=(), query_seconds=None):
    """The whole environment of the process, built from scratch: `home` is the turn's fresh `HOME`, `config` the chat's `home/` (Claude Code's settings and session files); `SDP_CHAT_GATEWAY` and `SDP_CHAT_DATABASES` (the attached `databases`), which `sdp_data` in the shell reads and the queue's facts in the role text name, and with `query_seconds` `SDP_CHAT_QUERY_SECONDS`, which `sdp_data` bounds its waits by. `queue` changes nothing here."""
    return {
        "SDP_CHAT_GATEWAY": gateway, "SDP_CHAT_DATABASES": ",".join(databases),
        **({QUERY_ENV: str(query_seconds)} if query_seconds is not None else {}),
        "HOME": home, "CLAUDE_CONFIG_DIR": config,
        "CLAUDE_CODE_TMPDIR": tmp, "TMPDIR": tmp, "MPLCONFIGDIR": os.path.join(tmp, "matplotlib"),
        "PATH": PATH,
        "ANTHROPIC_BASE_URL": f"http://{gateway}:{MODEL_PORT}",
        "CLAUDE_CODE_OAUTH_TOKEN": PLACEHOLDER_TOKEN,
        "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1", "DISABLE_TELEMETRY": "1", "DISABLE_AUTOUPDATER": "1",
        "DISABLE_ERROR_REPORTING": "1", "CLAUDE_CODE_DISABLE_AUTO_MEMORY": "1",
    }


def project_folder(cwd):
    """The name of the folder under `projects/` in which Claude Code keeps the sessions run in the working folder `cwd`: each character other than an ASCII letter or digit made `-` (measured)."""
    return _NOT_ALPHANUMERIC.sub("-", cwd)


def user_line(text):
    """The user's text as the one stream-json line given on standard input."""
    return (json.dumps({"type": "user", "message": {"role": "user", "content": text}}, ensure_ascii=False) + "\n").encode("utf-8")


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
    """The back end: `gateway` is the gateway's address, `databases` the names of the attached databases, `queue` whether the query queue is attached, `models` the models a chat may be given (some of MODELS), `query_seconds` the query time limit (None: none was given); a chat without a model runs on Claude Code's default (`model_default` None)."""

    name = "claude-code"
    model_default = None

    def __init__(self, gateway, databases, *, queue=False, models=MODELS, query_seconds=None, executable=EXECUTABLE, line_max=LINE_MAX_BYTES, grace=STOP_GRACE_SECONDS, stderr_max=STDERR_MAX_BYTES, join=JOIN_SECONDS):
        self.gateway, self.databases, self.queue, self.executable = gateway, tuple(databases), queue, executable
        self.query_seconds = query_seconds
        self.models = tuple(models)
        self.line_max, self.grace, self.stderr_max, self.join = line_max, grace, stderr_max, join

    def title(self, text, seconds=TITLE_SECONDS):
        """A title for a chat that starts with `text`: one `claude -p` run of `title_command` with the environment of a turn (the gateway, no databases), in a new folder of its own under the system's temporary folder (its `HOME`, `CLAUDE_CONFIG_DIR`, scratch and working folders; never a chat's), removed afterwards; within `seconds`. Returns (text, cost in USD or None); raises TitleFailed or OSError."""
        folder = tempfile.mkdtemp(prefix="sdp-title-")
        try:
            home, config, tmp, cwd = (os.path.join(folder, name) for name in ("home", "config", "tmp", "cwd"))
            for path in (home, config, tmp, cwd):
                os.mkdir(path, 0o700)
            data = run_bounded(title_command(self.executable), environment(home, config, tmp, self.gateway), cwd, text.encode("utf-8"), seconds)
        finally:
            shutil.rmtree(folder, ignore_errors=True)
        return title_of(data)

    def transcript(self, files, session):
        """Where the transcript of the session `session` run in the working folder `files` is, for the runner: (the folders under the chat's folder, the folder `files` names under them, the file's name)."""
        return TRANSCRIPT_FOLDERS, project_folder(files), f"{session}.jsonl"

    def turn(self, chat_fd, config, home, files, tmp, text, session, resume, model=None, prompt=None):
        """A `Process` for one turn (not started) and its `Translator`. `config` is the chat's `home/`, into which `mcp.json` is written again first; `home` the turn's fresh `HOME`; `model` the chat's model (None: Claude Code's default); `prompt` the chat's starting prompt, the pair the runner read from its `prompt.json` (None: the built-in one)."""
        payload = json.dumps(mcp_config(self.databases, self.gateway, self.queue), indent=2).encode("utf-8")
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
        argv = command(self.executable, config, session, resume, self.databases, self.queue, model, self.query_seconds, prompt)
        process = Process(argv, environment(home, config, tmp, self.gateway, self.queue, self.databases, self.query_seconds), files, user_line(text), chat_fd, line_max=self.line_max, grace=self.grace, stderr_max=self.stderr_max, join=self.join)
        return process, Translator()
