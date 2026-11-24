"""The OpenAI-compatible back end's agent, called in-process: its loop against a stand-in endpoint (every request body checked by the gateway's own rules), `run_shell`, the history, and the MCP part through the real gateway (skipped when the `mcp` package is not installed)."""

import asyncio
import contextlib
import importlib.util
import json
import os
import socket
import stat
import time

import pytest
import standins
from standins import gone, reply, tool_call

from social_data_pipeline.chat import agent, claude
from social_data_pipeline.gateway import rules
from social_data_pipeline.gateway.server import Gateway
from social_data_pipeline.gateway.table import MCP, MODEL_OPENAI, Listener, Table, Upstream

MODEL = "vendor/model-1:free"
LISTENER = Listener(0, MODEL_OPENAI, Upstream("https", "openrouter.ai", 443, "/api/v1"), model=MODEL)
HAS_MCP = importlib.util.find_spec("mcp") is not None
SECRET = "ENDPOINT-BODY-SECRET-3f1"


@pytest.fixture
def endpoint():
    """A stand-in endpoint answering from `.script` (a list of (status, body), or a function of the request); every body it gets is checked with the gateway's `check_body` for `model-openai`."""
    refused = []

    def answer(doc):
        last = server.seen[-1]
        try:
            rules.check_body(LISTENER, last["raw"])
        except rules.Refused as refusal:
            refused.append(refusal.code)
        return server.script(doc) if callable(server.script) else server.script.pop(0)

    server = standins.serve(standins.Model, answer=answer, script=[])
    yield server
    standins.stop(server)
    assert refused == [], f"the gateway would have refused: {refused}"


def run(endpoint, tmp_path, script, earlier=(), servers=(), port=None, **options):
    endpoint.script = [(200, body) if isinstance(body, dict) else body for body in script]
    lines = []
    worker = agent.Agent("127.0.0.1", port or endpoint.server_address[1], MODEL, str(tmp_path), {"PATH": claude.PATH}, servers, emit=lines.append, **options)
    turns, result = asyncio.run(worker.turn(list(earlier), "the question"))
    return turns, result, lines


def events(lines, result):
    translator = claude.Translator()
    out = [event for line in [*lines, {"type": "result", **result, "duration_ms": 1}] for event in translator.events(json.loads(json.dumps(line)))]
    return out, translator.end


SHELL = tool_call("run_shell", {"command": "echo hi"})


@pytest.mark.parametrize("script, kinds, results", [
    ([reply("hello")], ["text"], []),
    ([reply(calls=[SHELL]), reply("done")], ["tool", "tool_result", "text"], [("hi\n[exit code 0]", False)]),
    ([reply("looking", [SHELL, tool_call("run_shell", {"command": "echo two; exit 3"}, "call_2")]), reply("done")], ["text", "tool", "tool", "tool_result", "tool_result", "text"], [("hi\n[exit code 0]", False), ("two\n[exit code 3]", True)]),
    ([reply(calls=[tool_call("nope", {})]), reply("done")], ["tool", "tool_result", "text"], [(agent.NO_SUCH_TOOL, True)]),
    ([reply(calls=[tool_call("run_shell", "{not json")]), reply("done")], ["tool", "tool_result", "text"], [(agent.NOT_AN_OBJECT, True)]),
    ([reply(calls=[tool_call("run_shell", "[1]")]), reply("done")], ["tool", "tool_result", "text"], [(agent.NOT_AN_OBJECT, True)]),
    ([reply(calls=[tool_call("run_shell", {"cmd": "x"})]), reply("done")], ["tool", "tool_result", "text"], [(agent.NO_COMMAND, True)]),
], ids=["text", "one-tool", "several-and-a-failing-one", "unknown-tool", "invalid-arguments", "not-an-object", "no-command"])
def test_the_loop_runs_each_tool_call_and_answers_it(endpoint, tmp_path, script, kinds, results):
    """Bug class: a tool call not answered (the next request refused by the endpoint), an error raised out of the turn, or progress lines the Translator does not read."""
    turns, result, lines = run(endpoint, tmp_path, script)
    found, end = events(lines, result)
    assert [kind for kind, _ in found] == kinds and end == {"is_error": False, "cost_usd": None, "duration_ms": 1, "turns": len(script)}
    assert [(fields["text"], fields["is_error"]) for kind, fields in found if kind == "tool_result"] == results
    assert result["result"] == ("done" if len(script) > 1 else "hello")
    sent = [entry["doc"] for entry in endpoint.seen]
    assert all(doc["model"] == MODEL and doc["stream"] is False and doc["messages"][0] == {"role": "system", "content": claude.role(False)} for doc in sent)
    assert [tool["function"]["name"] for tool in sent[0]["tools"]] == ["run_shell"]
    if len(sent) > 1:
        assert [m["tool_call_id"] for m in sent[1]["messages"] if m["role"] == "tool"] == [c["id"] for c in sent[1]["messages"][2]["tool_calls"]]
    assert agent.turns_of([m for turn in turns for m in turn]) == turns and turns[0][0] == {"role": "user", "content": "the question"}


def test_the_turn_ends_at_its_limit_of_model_calls(endpoint, tmp_path):
    turns, result, lines = run(endpoint, tmp_path, [reply(calls=[SHELL])] * 3, calls_max=3)
    found, end = events(lines, result)
    assert len(endpoint.seen) == 3 and result["result"] == agent.CALLS_LIMIT and end["is_error"] is True
    assert [fields["text"] for kind, fields in found if kind == "tool_result"][-1] == agent.NOT_RUN_LIMIT
    assert agent.turns_of([m for turn in turns for m in turn]) is not None


@pytest.mark.parametrize("answer, message", [
    ((429, {"error": {"message": SECRET}}), agent.status_message(429)),
    ((403, SECRET.encode()), agent.status_message(403)),
    ((200, b"not json " + SECRET.encode()), agent.bad_reply_message("it is not JSON")),
    ((200, {"choices": "x"}), agent.bad_reply_message("it has no message")),
    ((200, {"choices": [{"message": {"content": [SECRET]}}]}), agent.bad_reply_message("its content is not text")),
    ((200, {"choices": [{"message": {"content": None, "tool_calls": [{"id": "bad id", "function": {"name": "run_shell", "arguments": "{}"}}]}}]}), agent.bad_reply_message("a tool call is not of the expected shape")),
    ((200, b'{"choices": "' + b"x" * agent.REPLY_MAX_BYTES + b'"}'), agent.bad_reply_message("it is larger than 4 MiB")),
], ids=["429", "403", "not-json", "no-message", "content-not-text", "bad-call-id", "over-long"])
def test_a_refusal_or_a_reply_that_cannot_be_used_ends_the_turn_and_shows_nothing_of_the_body(endpoint, tmp_path, capfd, answer, message):
    """Bug class (security): the endpoint's body (which could echo a secret, or be huge) reaching an event or the log."""
    _, result, lines = run(endpoint, tmp_path, [answer])
    found, end = events(lines, result)
    assert result["result"] == message and end["is_error"] is True and lines == []
    assert found == ([("error", {"message": claude.MODEL_NOT_REACHED})] if answer[0] != 200 else [])
    assert SECRET not in json.dumps([lines, result]) and SECRET not in capfd.readouterr().err


def test_an_endpoint_that_cannot_be_reached_ends_the_turn(endpoint, tmp_path):
    with socket.socket() as closed:
        closed.bind(("127.0.0.1", 0))
        port = closed.getsockname()[1]
    _, result, _ = run(endpoint, tmp_path, [], port=port)
    assert result == {"is_error": True, "result": agent.UNREACHABLE, "api_error_status": "unreachable", "num_turns": 1}


# ── run_shell ─────────────────────────────────────────────────────────────




def test_run_shell_cuts_the_output_reports_the_code_and_runs_in_the_folder(tmp_path):
    text, is_error = agent.run_shell("head -c 50000 /dev/zero | tr '\\0' a; echo; pwd >&2; exit 4", str(tmp_path), {"PATH": claude.PATH})
    assert text == "a" * 16000 + "\n[the output was cut at 16000 characters]\n[exit code 4]" and is_error is True
    text, is_error = agent.run_shell("pwd; echo err >&2", str(tmp_path), {"PATH": claude.PATH})
    assert (text, is_error) == (f"{tmp_path}\nerr\n[exit code 0]", False)


@pytest.mark.parametrize("command, ending", [
    ("sleep 60 & echo $! > pid; sleep 60", ("[the command was ended after 1 seconds]", True)),
    ("(trap '' TERM; sleep 60) & echo $! > pid; exit 0", ("[exit code 0]", False)),
], ids=["time-limit", "left-in-the-background"])
def test_run_shell_ends_the_whole_process_group(tmp_path, command, ending):
    """Bug class: a command past its time limit, or what it left in the background, still running after the tool call returned."""
    started = time.monotonic()
    assert agent.run_shell(command, str(tmp_path), {"PATH": claude.PATH}, seconds=1) == ending and time.monotonic() - started < 5
    standins.wait_for(lambda: gone(int((tmp_path / "pid").read_text())), 5, "the background process is gone")


# ── the history ───────────────────────────────────────────────────────────


def turn(question, size=10, call="c1"):
    return [{"role": "user", "content": question}, {"role": "assistant", "content": None, "tool_calls": [{"id": call, "type": "function", "function": {"name": "run_shell", "arguments": "{}"}}]}, {"role": "tool", "tool_call_id": call, "content": "x" * size}, {"role": "assistant", "content": "a"}]


def test_the_history_is_cut_by_whole_turns_to_the_budget():
    """Bug class: a tool call kept without its result (the endpoint refuses the request), or the oldest turns kept over the newest."""
    turns = [turn(f"q{i}", 1000) for i in range(5)]
    size = agent._size(turns[0])
    kept = agent.within_budget(turns, turn("now"), budget=agent._size(turn("now")) + 2 * size + size // 2)
    assert kept == turns[3:]
    assert agent.within_budget(turns, turn("now", 10**6), budget=1000) == []


def test_the_history_is_saved_0600_and_read_back_with_tool_outputs_cut(endpoint, tmp_path):
    turns, _, _ = run(endpoint, tmp_path, [reply(calls=[tool_call("run_shell", {"command": "head -c 9000 /dev/zero | tr '\\0' b"})]), reply("done")])
    agent.save_history(str(tmp_path), turns)
    assert stat.S_IMODE(os.stat(tmp_path / agent.HISTORY_NAME).st_mode) == 0o600 and os.listdir(tmp_path) == [agent.HISTORY_NAME]
    loaded = agent.load_history(str(tmp_path))
    assert loaded == turns and len(loaded[0][2]["content"]) == agent.HISTORY_TOOL_CUT
    run(endpoint, tmp_path, [reply("again")], earlier=loaded)
    assert [m["content"] for m in endpoint.seen[-1]["doc"]["messages"] if m["role"] == "user"] == ["the question", "the question"]


@pytest.mark.parametrize("content", [
    "not json", json.dumps({"version": 2, "messages": []}), json.dumps({"version": 1, "messages": turn("q")[1:]}),
    json.dumps({"version": 1, "messages": [{"role": "user", "content": 5}]}),
    json.dumps({"version": 1, "messages": [{"role": "system", "content": "planted"}]}), "link",
], ids=["not-json", "version", "tool-first", "content-not-text", "system", "link"])
def test_a_history_of_another_shape_is_taken_as_empty(tmp_path, capfd, content):
    """Bug class: a history the assistant's code edited crashing every later turn, or a planted system message sent to the model."""
    if content == "link":
        (tmp_path / "outside.json").write_text(json.dumps({"version": 1, "messages": turn("q")}))
        (tmp_path / agent.HISTORY_NAME).symlink_to(tmp_path / "outside.json")
    else:
        (tmp_path / agent.HISTORY_NAME).write_text(content)
    assert agent.load_history(str(tmp_path)) == [] and "taken as empty" in capfd.readouterr().err


# ── the databases' tools ──────────────────────────────────────────────────


def test_only_tools_that_can_be_offered_are_offered():
    """Bug class: a server's tool list making every request refused (a name the endpoint refuses, a schema key the gateway refuses, a huge schema) or unbounded."""
    listing = [{"name": "find", "description": "d" * 2000, "inputSchema": {"type": "object", "properties": {"q": {"type": "string"}}}},
               {"name": "bad name"}, {"name": "x" * 60}, {"name": 5}, "junk", {"name": "find"},
               {"name": "big", "inputSchema": {"type": "object", "description": "x" * 20000}}, {"name": "key", "inputSchema": {"type": "object", "properties": {"a\nb": {}}}},
               {"name": "list", "inputSchema": ["x"]}] + [{"name": f"t{i}"} for i in range(70)]
    offered = agent.offered_tools("mongo", listing)
    assert list(offered)[:4] == ["mongo__find", "mongo__big", "mongo__key", "mongo__list"] and len(offered) == 4 + 64 - 9
    name, spec = offered["mongo__find"]
    assert name == "find" and len(spec["function"]["description"]) == 1000 and spec["function"]["parameters"] == listing[0]["inputSchema"]
    assert all(offered[n][1]["function"]["parameters"] == agent.EMPTY_SCHEMA for n in ("mongo__big", "mongo__key", "mongo__list"))
    rules.check_body(LISTENER, json.dumps({"model": MODEL, "messages": [], "tools": [spec for _, spec in offered.values()]}).encode())


@pytest.mark.parametrize("result, expected", [
    ({"content": [{"type": "text", "text": "a"}, {"type": "image", "data": "AA"}, {"type": "resource"}]}, ("a\n[image]\n[resource]", False)),
    ({"content": [{"type": "text", "text": "y" * 20000}], "isError": True}, ("y" * 16000, True)),
    ({"content": "x"}, ("", False)), (None, (agent.TOOL_FAILED, True)),
])
def test_a_tool_result_becomes_text_cut_with_other_content_named(result, expected):
    assert agent.call_text(result) == expected


@pytest.fixture
def gateway():
    """The real gateway in-process with MCP listeners for postgres (SSE) and mongo (streamable HTTP) on loopback, forwarding to one stand-in MCP server; `.log` holds its log lines."""
    def call(tool, arguments):
        if tool == "slow":
            time.sleep(2)
        return standins.ping_call("db")(tool, arguments)

    mcp_server = standins.serve(standins.Mcp, tools=standins.ping_tools("db") + [{"name": "slow", "inputSchema": {"type": "object"}}], call=call)
    upstream = Upstream("http", "127.0.0.1", mcp_server.server_address[1])
    log = []
    gw = Gateway(Table("127.0.0.1", (Listener(0, MCP, upstream, paths=("/sse", "/messages/")), Listener(0, MCP, upstream, paths=("/mcp",))), ""), "", log=log.append)
    gw.start()
    yield gw, mcp_server, log
    gw.stop()
    standins.stop(mcp_server)


@pytest.mark.skipif(not HAS_MCP, reason="the mcp package is not installed")
def test_the_databases_tools_are_offered_and_called_through_the_gateway(endpoint, tmp_path, gateway, monkeypatch, capfd):
    """Bug class: the MCP client sending what the gateway drops or refuses (a path, a header, a query), a tool call that does not answer holding the turn, a database that cannot be reached ending the turn."""
    gw, mcp_server, log = gateway
    with socket.socket() as closed:
        closed.bind(("127.0.0.1", 0))
        dead = closed.getsockname()[1]
    monkeypatch.setitem(claude.DATABASES, "postgres", (gw.ports[0], "sse", "/sse"))
    monkeypatch.setitem(claude.DATABASES, "mongo", (gw.ports[1], "http", "/mcp"))
    monkeypatch.setitem(claude.DATABASES, "starrocks", (dead, "http", "/mcp"))
    monkeypatch.setattr(agent, "TOOL_SECONDS", 0.5)

    async def turn():
        servers = await agent.open_servers(["postgres", "mongo", "starrocks"], "127.0.0.1")
        lines = []
        calls = [tool_call("mongo__ping", {}), tool_call("postgres__ping", {}, "call_2"), tool_call("mongo__slow", {}, "call_3"), tool_call("mongo__ping", {}, "call_4")]
        endpoint.script = [(200, reply(calls=calls)), (200, reply("done"))]
        worker = agent.Agent("127.0.0.1", endpoint.server_address[1], MODEL, str(tmp_path), {}, servers, emit=lines.append)
        try:
            return await worker.turn([], "ask"), lines, [s.database for s in servers]
        finally:
            for server in servers:
                await server.close()

    (_, result), lines, attached = asyncio.run(turn())
    assert attached == ["postgres", "mongo"] and "starrocks: its MCP server could not be reached" in capfd.readouterr().err
    assert [t["function"]["name"] for t in endpoint.seen[0]["doc"]["tools"]] == ["run_shell", "postgres__ping", "postgres__slow", "mongo__ping", "mongo__slow"]
    found, _ = events(lines, result)
    assert [fields["text"] for kind, fields in found if kind == "tool_result"] == ["pong-from-db", "pong-from-db", agent.TOOL_TIMEOUT, agent.TOOL_FAILED]
    assert result["is_error"] is False and not [line for line in log if not line.endswith(("reason=-", "reason=client_gone"))]
    forwarded = {name for entry in mcp_server.seen for name in entry["headers"]}
    assert forwarded <= {"host", "content-type", "accept", "content-length", "connection", "mcp-session-id", "mcp-protocol-version", "last-event-id"}
    assert {entry["path"].partition("?")[0] for entry in mcp_server.seen} == {"/sse", "/messages/", "/mcp"}


# ── review round 1 ────────────────────────────────────────────────────────


def run_process(endpoint, tmp_path, monkeypatch, script):
    """`agent._run` as `main` calls it (the history loaded and saved in `tmp_path/home`): the result line."""
    endpoint.script = script
    monkeypatch.setattr(claude, "MODEL_PORT", endpoint.server_address[1])
    (tmp_path / "home").mkdir(exist_ok=True)
    (tmp_path / "files").mkdir(exist_ok=True)
    return asyncio.run(agent._run("127.0.0.1", MODEL, [], str(tmp_path / "home"), str(tmp_path / "files"), "the question"))


CALL = {"id": "c1", "type": "function", "function": {"name": "run_shell", "arguments": "{}"}}
HOSTILE = [
    [{"role": "user", "content": "q"}, {"role": "assistant", "content": None, "tool_calls": [CALL]}, {"role": "tool", "tool_call_id": ["c1"], "content": "x"}],
    [{"role": "user", "content": "q"}, {"role": "assistant", "content": None, "tool_calls": [{**CALL, "id": ["c1"]}]}],
    [{"role": "user", "content": "q"}, {"role": "assistant", "content": None, "tool_calls": {"c1": CALL}}],
    [{"role": "user", "content": "q"}, {"role": "assistant", "content": None, "tool_calls": [[CALL]]}],
    [{"role": "user", "content": "q"}, {"role": "assistant", "content": None, "tool_calls": [{**CALL, "function": [1]}]}],
    [{"role": "user", "content": "q"}, {"role": "assistant", "content": None, "tool_calls": [{**CALL, "function": {"name": 5, "arguments": {}}}]}],
    [{"role": ["user"], "content": "q"}], [{"role": "user", "content": {"a": [1]}}], [{"role": {"x": 1}}], [[["deep"]]], None, "messages", {"a": 1},
]


@pytest.mark.parametrize("content", [json.dumps({"version": 1, "messages": m}) for m in HOSTILE] + ['{"version": 1e999, "messages": []}', '{"version": ' + "9" * 5000 + ', "messages": []}', "[" * 100000, '["version", 1]'])
def test_a_hostile_history_never_ends_the_turn_without_its_result(endpoint, tmp_path, monkeypatch, capfd, content):
    """Bug class: a planted history (a list as `tool_call_id`) raising out of the turn: no `result` line, and the file kept, so every later turn failed the same way."""
    (tmp_path / "home").mkdir()
    (tmp_path / "home" / agent.HISTORY_NAME).write_text(content)
    result = run_process(endpoint, tmp_path, monkeypatch, [(200, reply("ok"))])
    err = capfd.readouterr().err
    assert result["type"] == "result" and result["is_error"] is False and ("taken as empty" in err or "they are repaired" in err)
    assert agent.load_history(str(tmp_path / "home"))[-1][0] == {"role": "user", "content": "the question"}


def test_a_lone_surrogate_from_the_history_or_the_model_is_cleaned_before_it_is_stored_or_sent(endpoint, tmp_path, monkeypatch):
    """Bug class: `\\ud800` (valid in JSON text) making the save raise after the turn's model calls were spent, or a request the endpoint cannot decode."""
    (tmp_path / "home").mkdir()
    (tmp_path / "home" / agent.HISTORY_NAME).write_text('{"version": 1, "messages": [{"role": "user", "content": "old \\ud800"}, {"role": "assistant", "content": "a"}]}')
    calls = [tool_call("run_shell", '{"command": "echo \\ud800x"}')]
    result = run_process(endpoint, tmp_path, monkeypatch, [(200, b'{"choices": [{"message": {"content": "say \\ud800", "tool_calls": ' + json.dumps(calls).encode() + b"}}]}"), (200, b'{"choices": [{"message": {"content": "b\\udfff"}}]}')])
    assert result["is_error"] is False and result["result"] == "b?"
    turns = agent.load_history(str(tmp_path / "home"))
    assert turns[0][0]["content"] == "old ?" and turns[1][1]["content"] == "say ?" and turns[1][2]["content"] == "?x\n[exit code 0]"
    assert all("\\ud" not in entry["raw"].decode() for entry in endpoint.seen)


def test_a_reply_listing_too_many_calls_is_cut_everywhere_and_over_256_is_a_bad_reply(endpoint, tmp_path):
    """Bug class: thousands of listed calls echoed, emitted and stored (the runner's line cap, the chat's log, the gateway's structure cap of the next request)."""
    many = [tool_call("run_shell", {"command": "true"}, f"call_{i}") for i in range(20)]
    turns, result, lines = run(endpoint, tmp_path, [reply(calls=many), reply("done")])
    found, _ = events(lines, result)
    assert [kind for kind, _ in found].count("tool") == 16 and [f["text"] for k, f in found if k == "tool_result"][-1] == agent.ignored_message(4)
    assert len(turns[0][1]["tool_calls"]) == 16 and [m["role"] for m in turns[0]].count("tool") == 16 and agent.ignored_message(4) in turns[0][17]["content"]
    assert len(endpoint.seen[1]["doc"]["messages"][2]["tool_calls"]) == 16
    _, result, _ = run(endpoint, tmp_path, [reply(calls=[tool_call("run_shell", {}, f"c{i}") for i in range(257)])])
    assert result["result"] == agent.bad_reply_message("it lists more than 256 tool calls")


def test_an_empty_answer_is_stored_as_text_and_unusable_arguments_as_an_empty_object(endpoint, tmp_path):
    turns, _, _ = run(endpoint, tmp_path, [reply(calls=[tool_call("run_shell", "[1]"), tool_call("run_shell", "", "c2")]), reply(None)])
    assert [c["function"]["arguments"] for c in turns[0][1]["tool_calls"]] == ["{}", "{}"] and turns[0][-1] == {"role": "assistant", "content": ""}
    assert agent.turns_of([m for turn in turns for m in turn]) == turns


@pytest.mark.parametrize("command, ending", [("setsid sh -c 'echo $$ > pid; exec yes' & sleep 0.2; exit 0", "[exit code 0]"), ("(while true; do echo x; done) & sleep 0.5; exit 3", "[exit code 3]")], ids=["left-the-group", "chatty-child"])
def test_run_shell_returns_in_bounds_with_a_process_writing_on(tmp_path, command, ending):
    """Bug class: the drain after the kill looping forever on a process that left the group, or a chatty background child making a finished command look timed out."""
    started = time.monotonic()
    try:
        text, _ = agent.run_shell(command, str(tmp_path), {"PATH": claude.PATH}, seconds=10)
    finally:
        if (tmp_path / "pid").exists():
            with contextlib.suppress(ProcessLookupError):
                os.kill(int((tmp_path / "pid").read_text()), 9)
    assert time.monotonic() - started < 4 and text.endswith(ending) and "[the output was cut at 16000 characters]" in text


def test_the_newest_turn_is_always_kept_and_drops_are_said(capfd):
    """Bug class: a newest turn over the budget on its own wiping the whole history without a word."""
    earlier = [turn("old", 900)]
    newest = turn("now", 3000)
    kept = agent.to_keep(earlier, newest, budget=1000)
    assert kept[-1][2]["content"] == agent.PLACEHOLDER and kept[-1][0]["content"] == "now" and len(kept) == 1
    huge = [{"role": "user", "content": "q"}, {"role": "assistant", "content": "x" * 3000, "tool_calls": [CALL]}, {"role": "tool", "tool_call_id": "c1", "content": "y"}, {"role": "assistant", "content": "the answer"}]
    assert agent.to_keep([], huge, budget=1000) == [[{"role": "user", "content": "q"}, {"role": "assistant", "content": "the answer"}]]
    err = capfd.readouterr().err
    assert "1 earlier turns are dropped" in err and "only its question and its answer are kept" in err


def test_every_request_of_a_turn_of_maximal_outputs_stays_within_the_turn_budget(endpoint, tmp_path, monkeypatch):
    """Bug class: a 40-call turn of 16,000-character outputs and arguments growing the request past what the gateway and the endpoint take."""
    monkeypatch.setattr(agent, "run_shell", lambda *a: ("o" * agent.OUTPUT_CUT, False))
    calls = [tool_call("run_shell", {"command": "c" * 15900}, f"call_{i}") for i in range(16)]
    _, result, _ = run(endpoint, tmp_path, [reply(calls=calls)] * 40)
    assert result["result"] == agent.CALLS_LIMIT and len(endpoint.seen) == 40
    assert max(len(entry["raw"]) for entry in endpoint.seen) < agent.TURN_BUDGET + 20000


def test_a_refusal_as_too_large_is_tried_once_more_without_the_earlier_turns(endpoint, tmp_path, capfd):
    """Bug class: a model with a small context refusing every request of a long chat for good."""
    endpoint.script = lambda doc: (413, b"too long") if sum(m["role"] == "user" for m in doc["messages"]) > 1 else (200, reply("ok"))
    worker = agent.Agent("127.0.0.1", endpoint.server_address[1], MODEL, str(tmp_path), {}, emit=lambda line: None)
    turns, result = asyncio.run(worker.turn([turn("old")], "the question"))
    assert result["is_error"] is False and len(endpoint.seen) == 2 and len(turns) == 1 and turns[0][0]["content"] == "the question"
    assert "the history starts again from this turn" in capfd.readouterr().err
    turns, result = asyncio.run(worker.turn([], "x"))
    assert result["is_error"] is False and len(endpoint.seen) == 3


# ── review round 2 ────────────────────────────────────────────────────────


def test_a_failure_partway_through_the_calls_saves_a_history_that_loads_whole(endpoint, tmp_path, monkeypatch):
    """Bug class: a turn that failed after its tool calls were stored but before all were answered saving a history whose next load discarded the whole chat."""
    home = tmp_path / "home"
    home.mkdir()
    agent.save_history(str(home), [turn("old")])
    count = [0]

    def failing(line):
        count[0] += 1
        if count[0] == 3:
            raise BrokenPipeError()

    endpoint.script = [(200, reply(calls=[SHELL, tool_call("run_shell", {"command": "echo two"}, "call_2")]))]
    worker = agent.Agent("127.0.0.1", endpoint.server_address[1], MODEL, str(tmp_path), {"PATH": claude.PATH}, emit=failing)
    turns, result = asyncio.run(worker.turn(agent.load_history(str(home)), "the question"))
    agent.save_history(str(home), turns)
    assert result["result"] == agent.TURN_FAILED
    loaded = agent.load_history(str(home))
    assert loaded[0] == turn("old") and [m.get("content") for m in loaded[1] if m["role"] == "tool"] == ["hi\n[exit code 0]", agent.NOT_FINISHED]
    run(endpoint, tmp_path, [reply("next")], earlier=loaded)
    assert [m["role"] for m in endpoint.seen[-1]["doc"]["messages"]].count("tool") == 3


def test_the_repair_is_one_pass_and_keeps_each_reply_to_its_own_calls():
    """Bug class: a repair that searches a list per call or answer (a planted history of a million calls hangs every later turn), or that matches answers across replies (a reused id pairs with the wrong reply)."""
    call = lambda call_id: {"id": call_id, "type": "function", "function": {"name": "run_shell", "arguments": "{}"}}
    answer = lambda call_id, text: {"role": "tool", "tool_call_id": call_id, "content": text}
    first = {"role": "assistant", "content": None, "tool_calls": [call("a"), call("b")]}
    second = {"role": "assistant", "content": None, "tool_calls": [call("a")]}
    messages = [answer("a", "stray"), {"role": "user", "content": "q"}, first, answer("b", "B"), answer("a", "A"), answer("a", "again"), second, {"role": "user", "content": "next"}]
    out = agent.repaired(messages)
    assert out == [{"role": "user", "content": "q"}, first, answer("a", "A"), answer("b", "B"), second, answer("a", agent.NOT_FINISHED), {"role": "user", "content": "next"}]
    assert agent.repaired(out) == out
    compared = []

    class Id(str):
        __hash__ = str.__hash__

        def __eq__(self, other):
            compared.append(1)
            return str.__eq__(self, other)

    many = 3000
    ids = [Id(f"c{n}") for n in range(many)]
    big = [{"role": "assistant", "content": None, "tool_calls": [call(i) for i in ids]}] + [answer(Id(f"c{n}"), "x") for n in reversed(range(many))]
    assert len(agent.repaired(big)) == 1 + many
    assert len(compared) <= 8 * many, f"the repair compared ids {len(compared)} times for {many} calls: it searches instead of looking up"


def test_a_history_whose_last_turn_lacks_an_answer_is_repaired(tmp_path, capfd):
    messages = [*turn("old"), {"role": "user", "content": "q"}, {"role": "assistant", "content": None, "tool_calls": [CALL, {**CALL, "id": "c2"}]}, {"role": "tool", "tool_call_id": "c2", "content": "two"}, {"role": "tool", "tool_call_id": "stray", "content": "x"}]
    (tmp_path / agent.HISTORY_NAME).write_text(json.dumps({"version": 1, "messages": messages}))
    loaded = agent.load_history(str(tmp_path))
    assert loaded[0] == turn("old") and loaded[1][2:] == [{"role": "tool", "tool_call_id": "c1", "content": agent.NOT_FINISHED}, {"role": "tool", "tool_call_id": "c2", "content": "two"}]
    assert "they are repaired" in capfd.readouterr().err


@pytest.mark.parametrize("status", [429, 500])
def test_a_refusal_that_is_not_about_size_is_not_tried_again(endpoint, tmp_path, status):
    endpoint.script = [(status, b"no")]
    worker = agent.Agent("127.0.0.1", endpoint.server_address[1], MODEL, str(tmp_path), {}, emit=lambda line: None)
    turns, result = asyncio.run(worker.turn([turn("old")], "q"))
    assert result["api_error_status"] == status and len(endpoint.seen) == 1 and turns[0] == turn("old")


def test_a_failed_retry_keeps_the_old_history_and_the_retry_counts_as_a_call(endpoint, tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    agent.save_history(str(home), [turn("old")])
    result = run_process(endpoint, tmp_path, monkeypatch, lambda doc: (413, b"too long"))
    assert result["api_error_status"] == 413 and result["num_turns"] == 2 and len(endpoint.seen) == 2
    assert agent.load_history(str(home))[0] == turn("old")
    endpoint.script = lambda doc: (413, b"too long") if sum(m["role"] == "user" for m in doc["messages"]) > 1 else (200, reply(calls=[SHELL]))
    worker = agent.Agent("127.0.0.1", endpoint.server_address[1], MODEL, str(tmp_path), {"PATH": claude.PATH}, emit=lambda line: None, calls_max=1)
    _, result = asyncio.run(worker.turn([turn("old")], "q"))
    assert result["api_error_status"] == 413 and result["num_turns"] == 1


def test_the_tools_offered_are_bounded_by_structure(endpoint, tmp_path, capfd):
    """Bug class: wide schemas within the size cap (a list of 1,990 empty lists is 6 KB) making the request exceed the gateway's cap of containers, so every request of the chat is refused."""
    wide = {"type": "object", "properties": {}, "x": [[]] * 1990}
    listing = [{"name": f"t{i}", "inputSchema": wide} for i in range(64)] + [{"name": "over", "inputSchema": {"type": "object", "x": [[]] * 2001}}]
    servers = []
    for database in ("postgres", "mongo", "starrocks"):
        server = agent.Server(database, "http://unused", "http")
        server.tools = agent.offered_tools(database, listing[:63] + listing[-1:])
        servers.append(server)
    assert servers[0].tools["postgres__over"][1]["function"]["parameters"] == agent.EMPTY_SCHEMA
    run(endpoint, tmp_path, [reply("ok")], servers=servers)
    offered = endpoint.seen[0]["doc"]["tools"]
    assert 40 < len(offered) < 60 and "tools of the databases are left out" in capfd.readouterr().err
    assert agent.containers(offered) <= agent.TOOLS_CONTAINERS + 10
