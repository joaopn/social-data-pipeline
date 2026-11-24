"""The query queue in the chat's runner and both back ends (`SDP_CHAT_QUEUE=1`): the setting, the MCP server `jobs` in Claude Code's command line and `mcp.json` and in the agent's servers and tools (through the real gateway's `mcp` listener), the role text (its instructions and fixed facts) with and without the queue's parts, and `hello`.

Every special character in this file is written as an escape.
"""

import asyncio
import importlib.util
import json

import pytest
import standins
from standins import reply, tool_call

from social_data_pipeline.chat import __main__ as main
from social_data_pipeline.chat import agent, claude, openai
from social_data_pipeline.gateway.server import Gateway
from social_data_pipeline.gateway.table import MCP, Listener, Table, Upstream

HAS_MCP = importlib.util.find_spec("mcp") is not None
MODEL = "vendor/model-1:free"
SESSION = "5f6cec57-5f0d-4a8c-a927-8480b9f00bb1"
HOME = "/work/c_0123456789ab/home"
# The role text's parts, pinned whole: a change of one is a change of what the assistant is told. The instructions are the built-in editable ones; the facts are fixed.
INSTRUCTIONS = (
    "You are the assistant of a research data deployment. You answer questions about the operator's databases through the MCP tools of those databases, which are read-only.\n\n"
    "Before you query a table or collection for the first time in a chat, look at how it is laid out, with small queries: its columns and their types, a few rows (at most 5), and its indexes and keys (on StarRocks `SHOW CREATE TABLE`, which shows its indexes, sort key and partitions; on PostgreSQL its indexes; on MongoDB the collection's indexes). Note whether it has partitions: a table without them is scanned whole unless an index or the sort key narrows the scan. Columns that name a row's source file or period (like `dataset`, the dump month, in Reddit tables) are usually indexed and are the way to count per period.\n\n"
    "Weigh two costs before you write a query. Reading: StarRocks stores each column apart, so a query reads only the columns it names, and a filter on an indexed or sort-key column reads less. Memory, which columnar storage does not reduce: COUNT(DISTINCT ...), a GROUP BY on a high-cardinality column (such as `author`, `link_id` or `id`) and joins hold one entry per distinct key or group, so their memory grows with the number of distinct keys or groups, not with the columns read. Estimate that number, from table metadata or a small sample, before you choose between one query over everything and a loop of smaller queries, for example one per value of an indexed categorical column such as `dataset`.\n\n"
    "For a query with COUNT(DISTINCT ...), a GROUP BY on a high-cardinality column or a join of large tables, on StarRocks, write at least two versions with different strategies (for example the single query and a per-`dataset` or per-batch loop) and compare them with `EXPLAIN COSTS` before you run either. Report for each its estimated CPU, memory, partitions and tablets scanned, and rows. The estimates are rough: they do not reflect bitmap-index skipping and may rest on stale statistics (check a count they show, such as the number of months, against the real one). When the choice matters, time one chunk (one value of the loop) on the read-only connection and look at it with `analyze_query`, then scale from that measurement: one measured chunk is worth more than any number of estimates. For simple counts on indexed columns, skip this; it only adds noise.\n\n"
    "In a cost comparison, label each claim as table metadata, a measurement, an optimizer estimate or an inference."
)
QUEUE_INSTRUCTIONS = "Send a query to the scheduler with its submit tools when it must read many columns of very large tables, hold many distinct keys or groups, join large tables, or when the read-only query is refused or times out; run the others on the read-only connection. Before you submit a query, compare at least two versions with `EXPLAIN COSTS` as above, among them a single-query version and a per-dataset (or per-batch) version, report the figures for each, and submit the one you chose, saying why."
# The old role text's sentences from the charts on, unchanged.
DEPLOYMENT_FACTS = "Make every chart with Altair (a Vega-Lite chart) unless the operator explicitly asks for another library; a request for an image or a PNG is still an Altair chart. Save it twice in the current folder, as `<name>.vl.json` (`chart.save(\"<name>.vl.json\")`, which the web UI draws as an interactive chart) and as `<name>.png` (`chart.save(\"<name>.png\")`), and name both files in your answer. Keep the files you make for the operator in the current folder (subfolders are fine: the web UI lists `.py`, `.png`, `.jpg`, `.svg`, `.json`, `.csv`, `.txt` and `.md` files up to three folders down, and folders whose names start with `.` are not looked into), and save the script that makes a chart beside it as `<name>.py`, with the same `<name>` as its `.vl.json` and `.png`, so the chart's Code button finds it. To use query results in a script, query them from the script with `sdp_data.read_query(server, query)` (it returns the rows through the same read-only connection your tools use) instead of copying values by hand; the shell has no other database access. The first argument of `sdp_data.read_query` is the database server (`starrocks`, `postgres` or `mongo`), not a database or schema name like `reddit`: name those inside the query (`reddit.comments`). Altair and vl-convert are installed: if importing them fails, say so instead of using another library. Keep the chart's data inside the chart (at most 50,000 rows; above 5,000 call `alt.data_transformers.disable_max_rows()` first; never loaded from an address or a file, no images or links) and leave its colours and background to the page. When a log axis helps (values spanning several orders of magnitude), make the y scale log (`alt.Scale(type=\"log\")`) in a single chart rather than two panels; the page adds a linear/log switch. You cannot reach the internet. Never suggest sending data or files to websites or services outside this deployment."
# The old queue paragraph without its advice on where a query runs: the scheduler's mechanics only.
QUEUE_FACTS = (
    "The operator's query scheduler is attached as the MCP server `jobs`. `list_targets` names its targets. "
    "A submitted query runs once it is approved, either by the scheduler itself when its target's auto-accept is on and has room under its limit, or by the operator in the Queue window of the web UI; you cannot approve it yourself. After a submission, tell the operator the job id and that the query may already be approved by auto-accept or else waits for their approval in the Queue window, and end your turn instead of waiting for it. "
    "`query_status` tells where a query stands, and `query_cancel` withdraws one of yours while it is still pending. "
    "When a query is done, the parts of its result are listed at http://<gateway>:8004/assistant/results/<job id> (JSON) and each part is at http://<gateway>:8004/assistant/results/<job id>/<part number>, where <gateway> is the address in the environment variable SDP_CHAT_GATEWAY: "
    "fetch them from the shell with Python's urllib (curl is not installed) and save them in the current folder. Only your own queries are visible there."
)
ROLE = INSTRUCTIONS + "\n\n" + DEPLOYMENT_FACTS
QUEUE_PARAGRAPH = QUEUE_FACTS + "\n\n" + QUEUE_INSTRUCTIONS
BASE = {"SDP_CHAT_BACKEND": "claude-code", "SDP_CHAT_CAP_GIB": "10", "SDP_CHAT_GATEWAY": "10.0.0.2", "SDP_CHAT_MODELS": "opus,sonnet,haiku"}


# ── the setting ───────────────────────────────────────────────────────────


@pytest.mark.parametrize("value, queue", [(None, False), ("0", False), ("1", True)])
def test_the_queue_is_off_without_the_setting_or_at_0_and_on_at_1(value, queue):
    """Bug class: a sandbox started with the queue that offers no `jobs` server, or one started without it that offers a server nobody answers."""
    environ = {**BASE, **({} if value is None else {"SDP_CHAT_QUEUE": value})}
    assert main.settings(environ)["queue"] is queue


@pytest.mark.parametrize("value", ["", "2", "true", "on", " 1", "1 ", "01", "١"])
def test_any_other_value_of_the_setting_refuses_the_start(value):
    """Bug class: a value that is neither on nor off read as one of them."""
    with pytest.raises(main.SettingError, match="SDP_CHAT_QUEUE"):
        main.settings({**BASE, "SDP_CHAT_QUEUE": value})


@pytest.mark.parametrize("value, queue", [(None, False), ("0", False), ("1", True)])
def test_the_agent_reads_the_setting_by_the_same_rule(value, queue):
    environ = {"SDP_CHAT_GATEWAY": "10.0.0.2", "SDP_CHAT_MODEL": MODEL, **({} if value is None else {"SDP_CHAT_QUEUE": value})}
    assert agent.settings(environ) == ("10.0.0.2", MODEL, [], queue)
    with pytest.raises(ValueError, match="SDP_CHAT_QUEUE"):
        agent.settings({**environ, "SDP_CHAT_QUEUE": "yes"})


# ── the role text ─────────────────────────────────────────────────────────


def test_the_role_text_is_pinned_with_and_without_the_queue_paragraph():
    """Bug class: the assistant told of a queue it cannot reach, or not told that a query waits for auto-accept or a person (it would wait on one in its turn), or the text changed unnoticed."""
    assert claude.INSTRUCTIONS == INSTRUCTIONS and claude.DEPLOYMENT_FACTS == DEPLOYMENT_FACTS and claude.role(False) == ROLE
    assert claude.QUEUE_INSTRUCTIONS == QUEUE_INSTRUCTIONS and claude.QUEUE_FACTS == QUEUE_FACTS and claude.role(True) == ROLE + "\n\n" + QUEUE_PARAGRAPH


def test_the_texts_are_ascii_and_the_queue_facts_keep_the_mechanics_but_not_the_advice():
    """Bug class: a typographic character slipped into what the assistant is told, or the split moving a mechanic of the queue (where results are, how to follow or withdraw a query, its targets) into the editable part, or leaving the old advice on where a query runs in the fixed part."""
    assert all(text.isascii() for text in (claude.INSTRUCTIONS, claude.QUEUE_INSTRUCTIONS, claude.DEPLOYMENT_FACTS, claude.QUEUE_FACTS, claude.TITLE_INSTRUCTION))
    assert all(part in claude.QUEUE_FACTS for part in ("`list_targets`", "`query_status`", "`query_cancel`", "http://<gateway>:8004/assistant/results/<job id>", "SDP_CHAT_GATEWAY", "urllib"))
    assert "Judge a query" not in claude.QUEUE_FACTS and "submit tools" not in claude.QUEUE_FACTS and "EXPLAIN" not in claude.QUEUE_FACTS
    assert claude.DEPLOYMENT_FACTS.startswith("Make every chart with Altair") and "You are the assistant" not in claude.DEPLOYMENT_FACTS


def test_given_instructions_replace_the_built_in_ones_around_the_fixed_facts():
    """Bug class: an operator's instructions dropped, put after the facts or in the queue's place, or the fixed facts or the limit sentences lost when instructions are given."""
    prompt = ("Be brief.\n\nCount first.", "Queue big ones.")
    assert claude.role(False, prompt=prompt) == "Be brief.\n\nCount first.\n\n" + DEPLOYMENT_FACTS
    assert claude.role(False, 45, prompt) == "Be brief.\n\nCount first.\n\n" + DEPLOYMENT_FACTS + " " + LIMIT_SENTENCE
    assert claude.role(True, prompt=prompt) == "Be brief.\n\nCount first.\n\n" + DEPLOYMENT_FACTS + "\n\n" + QUEUE_FACTS + "\n\nQueue big ones."
    assert claude.role(True, 45, prompt) == "Be brief.\n\nCount first.\n\n" + DEPLOYMENT_FACTS + " " + LIMIT_SENTENCE + "\n\n" + QUEUE_FACTS + "\n\nQueue big ones. " + LIMIT_QUEUE_SENTENCE
    assert claude.role(True, 45, None) == claude.role(True, 45)


def test_without_the_queue_its_instructions_are_left_out():
    """Bug class: the assistant told to send queries to a scheduler that is not attached."""
    assert "Queue big ones" not in claude.role(False, 45, ("Be brief.", "Queue big ones."))
    assert QUEUE_INSTRUCTIONS not in claude.role(False) and QUEUE_FACTS not in claude.role(False, 45)


@pytest.mark.parametrize("empty", ["", "  ", "\n\n"])
def test_empty_instructions_leave_no_stray_separators(empty):
    """Bug class: an operator who cleared the instructions sending the model a text that starts with blank lines or holds doubled ones."""
    assert claude.role(False, prompt=(empty, empty)) == DEPLOYMENT_FACTS
    assert claude.role(True, prompt=(empty, empty)) == DEPLOYMENT_FACTS + "\n\n" + QUEUE_FACTS
    assert claude.role(True, 45, (empty, empty)) == DEPLOYMENT_FACTS + " " + LIMIT_SENTENCE + "\n\n" + QUEUE_FACTS + " " + LIMIT_QUEUE_SENTENCE
    assert claude.role(True, 45, ("  Be brief.\n", empty)) == "Be brief.\n\n" + DEPLOYMENT_FACTS + " " + LIMIT_SENTENCE + "\n\n" + QUEUE_FACTS + " " + LIMIT_QUEUE_SENTENCE
    for text in (claude.role(True, 45, (empty, "Queue.")), claude.role(True, None, ("A.", empty))):
        assert "\n\n\n" not in text and text == text.strip()


# ── Claude Code ───────────────────────────────────────────────────────────


def test_claude_codes_command_line_and_mcp_config_with_the_queue_are_pinned():
    """Bug class: the `jobs` server written into `mcp.json` but not allowed (every call denied without a prompt), or allowed without the queue."""
    argv = claude.command("/usr/local/bin/claude", HOME, SESSION, False, ["postgres"], queue=True)
    assert argv == [
        "/usr/local/bin/claude", "-p", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose",
        "--mcp-config", "/work/c_0123456789ab/home/mcp.json", "--strict-mcp-config", "--tools", "Bash,Read,Write,Edit,Glob,Grep",
        "--permission-mode", "dontAsk", "--allowedTools", "Bash,Read,Write,Edit,Glob,Grep,mcp__postgres,mcp__jobs",
        "--setting-sources", "", "--settings", '{"disableAllHooks":true,"enabledPlugins":{"cc-plugin-agents-md@builtin":false,"cc-plugin-privacy@builtin":false,"cc-plugin-plugin-authoring@builtin":false}}',
        "--append-system-prompt", ROLE + "\n\n" + QUEUE_PARAGRAPH, "--session-id", SESSION,
    ]
    without = claude.command("/usr/local/bin/claude", HOME, SESSION, False, ["postgres"])
    assert without[without.index("--allowedTools") + 1] == "Bash,Read,Write,Edit,Glob,Grep,mcp__postgres" and without[without.index("--append-system-prompt") + 1] == ROLE
    jobs = {"type": "http", "url": "http://10.1.0.2:8004/assistant/mcp"}
    assert claude.mcp_config(["postgres"], "10.1.0.2", queue=True) == {"mcpServers": {"postgres": {"type": "sse", "url": "http://10.1.0.2:8001/sse"}, "jobs": jobs}}
    assert claude.mcp_config([], "10.1.0.2", queue=True) == {"mcpServers": {"jobs": jobs}}
    assert claude.mcp_config(["postgres"], "10.1.0.2") == {"mcpServers": {"postgres": {"type": "sse", "url": "http://10.1.0.2:8001/sse"}}}


def test_claude_codes_environment_names_the_gateway_and_the_databases_with_or_without_the_queue():
    """Bug class: the role text pointing the assistant at a variable its shell does not have (the queue's results could not be fetched, `sdp_data` could reach no database)."""
    with_queue = claude.environment("/tmp/s/home", HOME, "/tmp/s", "10.1.0.2", queue=True, databases=("mongo", "starrocks"))
    assert with_queue == claude.environment("/tmp/s/home", HOME, "/tmp/s", "10.1.0.2", databases=("mongo", "starrocks"))
    assert with_queue["SDP_CHAT_GATEWAY"] == "10.1.0.2" and with_queue["SDP_CHAT_DATABASES"] == "mongo,starrocks"


def test_a_turn_with_the_queue_writes_the_server_and_allows_it_and_hello_says_so(chat_runner):
    """Bug class: the runner started with the queue but its back end, or `hello`, not told."""
    r = chat_runner(queue=True)
    assert r.call("hello")["result"] == {"version": 1, "backend": "claude-code", "databases": ["mongo"], "queue": True, "models": ["opus", "sonnet", "haiku"], "model_default": None}
    chat = r.new()
    r.call("chat.send", chat=chat, text="hi")
    r.wait(chat)
    argv = r.records()[0]["argv"]
    assert argv[argv.index("--allowedTools") + 1].endswith(",mcp__mongo,mcp__jobs") and argv[argv.index("--append-system-prompt") + 1] == claude.role(True)
    assert json.loads((r.work / chat / "home" / "mcp.json").read_text())["mcpServers"]["jobs"] == {"type": "http", "url": "http://127.0.0.1:8004/assistant/mcp"}
    assert r.records()[0]["env"]["SDP_CHAT_GATEWAY"] == "127.0.0.1"


# ── the OpenAI-compatible back end ────────────────────────────────────────


def test_the_agents_environment_carries_the_setting_only_with_the_queue():
    without = openai.environment("/tmp/s/home", "/tmp/s", "10.201.0.2", "m", ["mongo"])
    assert openai.environment("/tmp/s/home", "/tmp/s", "10.201.0.2", "m", ["mongo"], queue=True) == {**without, "SDP_CHAT_QUEUE": "1"} and "SDP_CHAT_QUEUE" not in without


@pytest.fixture
def endpoint():
    server = standins.serve(standins.Model, answer=lambda doc: server.script.pop(0), script=[])
    yield server
    standins.stop(server)


@pytest.mark.skipif(not HAS_MCP, reason="the mcp package is not installed")
def test_the_agent_offers_the_queues_tools_as_jobs_and_calls_them_through_the_gateway(endpoint, tmp_path, monkeypatch):
    """Bug class: the agent's MCP client sending to `/assistant/mcp` what the gateway's listener refuses (it forwards only its two paths), the queue's tools offered under names the loop does not route, or its role text without the paragraph."""
    mcp_server = standins.serve(standins.Mcp, tools=standins.ping_tools("jobs"), call=standins.ping_call("jobs"))
    upstream = Upstream("http", "127.0.0.1", mcp_server.server_address[1])
    log = []
    gw = Gateway(Table("127.0.0.1", (Listener(0, MCP, upstream, paths=("/mcp",)), Listener(0, MCP, upstream, paths=("/assistant/mcp", "/assistant/results/"))), ""), "", log=log.append)
    gw.start()
    try:
        monkeypatch.setitem(claude.DATABASES, "mongo", (gw.ports[0], "http", "/mcp"))
        monkeypatch.setattr(claude, "QUEUE_PORT", gw.ports[1])

        async def turn():
            servers = await agent.open_servers(["mongo"], "127.0.0.1", queue=True)
            endpoint.script = [(200, reply(calls=[tool_call("jobs__ping", {})])), (200, reply("done"))]
            lines = []
            worker = agent.Agent("127.0.0.1", endpoint.server_address[1], MODEL, str(tmp_path), {}, servers, queue=True, emit=lines.append)
            try:
                return [(s.database, s.url) for s in servers], await worker.turn([], "ask"), lines
            finally:
                for server in servers:
                    await server.close()

        attached, (_, result), lines = asyncio.run(turn())
    finally:
        gw.stop()
        standins.stop(mcp_server)
    assert attached == [("mongo", f"http://127.0.0.1:{gw.ports[0]}/mcp"), ("jobs", f"http://127.0.0.1:{gw.ports[1]}/assistant/mcp")]
    sent = endpoint.seen[0]["doc"]
    assert [t["function"]["name"] for t in sent["tools"]] == ["run_shell", "mongo__ping", "jobs__ping"]
    assert sent["messages"][0] == {"role": "system", "content": ROLE + "\n\n" + QUEUE_PARAGRAPH}
    results = [block["content"] for line in lines if line["type"] == "user" for block in line["message"]["content"]]
    assert results == ["pong-from-jobs"] and result["is_error"] is False
    assert {entry["path"] for entry in mcp_server.seen} >= {"/assistant/mcp"} and not [line for line in log if not line.endswith(("reason=-", "reason=client_gone"))]


def test_without_the_queue_the_agent_opens_no_jobs_server_and_keeps_its_role(monkeypatch):
    opened = []

    class Recorded(agent.Server):
        async def open(self, seconds=None):
            opened.append((self.database, self.url, self.kind))

    monkeypatch.setattr(agent, "Server", Recorded)
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: object())
    assert [s.database for s in asyncio.run(agent.open_servers(["mongo"], "10.0.0.2"))] == ["mongo"]
    assert [s.database for s in asyncio.run(agent.open_servers([], "10.0.0.2", queue=True))] == ["jobs"]
    assert opened == [("mongo", "http://10.0.0.2:8002/mcp", "http"), ("jobs", "http://10.0.0.2:8004/assistant/mcp", "http")]
    assert agent.Agent("10.0.0.2", 8080, MODEL, "/tmp", {}).system == {"role": "system", "content": ROLE}


# ── the query time limit ──────────────────────────────────────────────────

LIMIT_SENTENCE = "A read-only query on MongoDB or StarRocks, through your tools or `sdp_data.read_query`, is stopped by the database after 45 seconds."
LIMIT_QUEUE_SENTENCE = "Submit a query on MongoDB or StarRocks that may take longer than 45 seconds to the scheduler instead of running it on the read-only connection."


def test_the_role_text_names_the_limit_and_with_the_queue_sends_longer_queries_there():
    """Bug class: the assistant not told the limit the database enforces (it then runs a long query until it is stopped, and again), the limit claimed for PostgreSQL (whose server ignores it), or told to use a queue that is not attached."""
    assert claude.role(False, 45) == ROLE + " " + LIMIT_SENTENCE
    assert claude.role(True, 45) == ROLE + " " + LIMIT_SENTENCE + "\n\n" + QUEUE_PARAGRAPH + " " + LIMIT_QUEUE_SENTENCE
    assert "PostgreSQL" not in LIMIT_SENTENCE + LIMIT_QUEUE_SENTENCE
    assert claude.role(False) == ROLE and claude.role(True, None) == ROLE + "\n\n" + QUEUE_PARAGRAPH


def test_both_back_ends_carry_the_limit_to_the_role_text_and_the_shell():
    """Bug class: the limit given to the runner but not to the assistant's role text, or not to the shell where `sdp_data` reads it (its waits then ignore the limit)."""
    argv = claude.command("/usr/local/bin/claude", HOME, SESSION, False, ["mongo"], True, None, 45)
    assert argv[argv.index("--append-system-prompt") + 1] == claude.role(True, 45)
    assert claude.environment("/tmp/s/home", HOME, "/tmp/s", "10.1.0.2", databases=("mongo",), query_seconds=45)["SDP_CHAT_QUERY_SECONDS"] == "45"
    assert "SDP_CHAT_QUERY_SECONDS" not in claude.environment("/tmp/s/home", HOME, "/tmp/s", "10.1.0.2", databases=("mongo",))
    assert openai.environment("/tmp/s/home", "/tmp/s", "10.201.0.2", "m", ["mongo"], query_seconds=45)["SDP_CHAT_QUERY_SECONDS"] == "45"
    worker = agent.Agent("10.0.0.2", 8080, MODEL, "/tmp", {}, queue=True, query_seconds=45)
    assert worker.system == {"role": "system", "content": claude.role(True, 45)} and worker.tool_seconds == agent.TOOL_SECONDS == 120
    assert agent.Agent("10.0.0.2", 8080, MODEL, "/tmp", {}, query_seconds=300).tool_seconds == 310
    assert agent.Agent("10.0.0.2", 8080, MODEL, "/tmp", {}).tool_seconds is None


@pytest.mark.parametrize("value, seconds", [(None, None), ("", None), ("1", 1), ("90", 90), ("3600", 3600)])
def test_the_runner_reads_the_limit(value, seconds):
    """Bug class: a missing limit read as some number, or a set one dropped."""
    environ = {**BASE, **({} if value is None else {"SDP_CHAT_QUERY_SECONDS": value})}
    assert main.settings(environ)["query_seconds"] == seconds
    assert claude.query_seconds(environ) == seconds


@pytest.mark.parametrize("value", ["0", "3601", "12345", "abc", "9.5", " 9", "٣"])
def test_a_malformed_limit_refuses_the_runner_and_the_agent(value):
    """Bug class: a malformed limit taken as none, so the assistant is told nothing while the database stops its queries."""
    with pytest.raises(main.SettingError, match="SDP_CHAT_QUERY_SECONDS"):
        main.settings({**BASE, "SDP_CHAT_QUERY_SECONDS": value})
    with pytest.raises(ValueError, match="SDP_CHAT_QUERY_SECONDS"):
        claude.query_seconds({"SDP_CHAT_QUERY_SECONDS": value})
