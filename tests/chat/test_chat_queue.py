"""The query queue in the chat's runner and both back ends (`SDP_CHAT_QUEUE=1`): the setting, the MCP server `jobs` in Claude Code's command line and `mcp.json` and in the agent's servers and tools (through the real gateway's `mcp` listener), the role text with and without its paragraph, and `hello`.

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
# The role text, pinned whole: a change of it is a change of what the assistant is told.
ROLE = "You are the assistant of a research data deployment. You answer questions about the operator's databases through the MCP tools of those databases, which are read-only. When you make a plot, save it as a PNG file in the current folder and name the file in your answer. You cannot reach the internet."
QUEUE_PARAGRAPH = (
    "The operator's query scheduler is attached as the MCP server `jobs`. Send long or heavy queries to it with its submit tools rather than through the databases' own tools; `list_targets` names its targets. "
    "A person approves each submitted query in the Queue window of the web UI before it runs, and you cannot approve it: after a submission, tell the operator the job id and that the query waits for their approval in the Queue window, and end your turn instead of waiting for it. "
    "`query_status` tells where a query stands, and `query_cancel` withdraws one of yours while it is still pending. "
    "When a query is done, the parts of its result are listed at http://<gateway>:8004/assistant/results/<job id> (JSON) and each part is at http://<gateway>:8004/assistant/results/<job id>/<part number>, where <gateway> is the address in the environment variable SDP_CHAT_GATEWAY: "
    "fetch them from the shell with Python's urllib (curl is not installed) and save them in the current folder. Only your own queries are visible there."
)
BASE = {"SDP_CHAT_BACKEND": "claude-code", "SDP_CHAT_CAP_GIB": "10", "SDP_CHAT_GATEWAY": "10.0.0.2"}


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
    """Bug class: the assistant told of a queue it cannot reach, or not told that a person approves each query (it would wait on one in its turn), or the text changed unnoticed."""
    assert claude.ROLE == ROLE and claude.role(False) == ROLE
    assert claude.QUEUE_ROLE == QUEUE_PARAGRAPH and claude.role(True) == ROLE + "\n\n" + QUEUE_PARAGRAPH


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


def test_claude_codes_environment_names_the_gateway_only_with_the_queue():
    """Bug class: the role text pointing the assistant at a variable its shell does not have (the results could not be fetched)."""
    with_queue = claude.environment("/tmp/s/home", HOME, "/tmp/s", "10.1.0.2", queue=True)
    assert with_queue == {**claude.environment("/tmp/s/home", HOME, "/tmp/s", "10.1.0.2"), "SDP_CHAT_GATEWAY": "10.1.0.2"}
    assert "SDP_CHAT_GATEWAY" not in claude.environment("/tmp/s/home", HOME, "/tmp/s", "10.1.0.2")


def test_a_turn_with_the_queue_writes_the_server_and_allows_it_and_hello_says_so(chat_runner):
    """Bug class: the runner started with the queue but its back end, or `hello`, not told."""
    r = chat_runner(queue=True)
    assert r.call("hello")["result"] == {"version": 1, "backend": "claude-code", "databases": ["mongo"], "queue": True}
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
