"""The chat's runner and its Claude Code back end, with a stand-in `claude` executable (`chat_helpers.STANDIN`) and a real socket."""

import base64
import contextlib
import json
import os
import signal
import socket
import stat
import struct
import sys
import threading
import time
import uuid

import pytest
from chat_helpers import INIT, exchange, result

from social_data_pipeline.chat import __main__ as main
from social_data_pipeline.chat import claude, runner

SAY = ["out", {"type": "assistant", "message": {"content": [{"type": "text", "text": "hello there"}]}}]
SLEEP = ["sleep", 30]
CHAT_OPS = [("chat.send", {"text": "hi"}), ("chat.events", {"offset": 0, "limit": 1}), ("chat.stop", {}), ("chat.delete", {}), ("chat.files", {}), ("chat.file", {"name": "a.png", "offset": 0, "limit": 1})]


def error_of(reply):
    assert reply["ok"] is False and set(reply["error"]) == {"code", "message"}
    return reply["error"]["code"]


def gone(pid):
    """Whether the process `pid` no longer runs (gone, or a zombie waiting for its reaper)."""
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().rsplit(")", 1)[1].split()[0] == "Z"
    except FileNotFoundError:
        return True


# ── fields and ids ────────────────────────────────────────────────────────


@pytest.mark.parametrize("request_doc", [
    {"op": "nope"}, {"op": 5}, {}, {"op": "hello", "extra": 1}, {"op": "chat.send", "chat": "c_000000000000"},
    {"op": "chat.events", "chat": "c_000000000000", "offset": 0, "limit": 1, "more": 1},
])
def test_an_unknown_op_or_a_field_too_many_or_too_few_is_bad_request(chat_runner, request_doc):
    """Bug class: a request with fields the runner does not take acted on anyway."""
    r = chat_runner()
    assert error_of(r.runner.answer(request_doc)) == "bad_request"


@pytest.mark.parametrize("bad_id", ["c_00000000000", "c_0000000000000", "c_ABCDEF012345", "../c_000000000000", "c_000000000000\n", 5, None, ["c_000000000000"]])
@pytest.mark.parametrize("op, fields", CHAT_OPS, ids=[op for op, _ in CHAT_OPS])
def test_a_bad_chat_id_is_refused_before_the_file_system_is_touched(chat_runner, monkeypatch, op, fields, bad_id):
    """Bug class: a chat id used to build a path (`..`, a line break) before it was checked."""
    r = chat_runner()
    touched = []
    for name in ("_open_folder", "_open_regular", "measure", "remove_tree", "listing"):
        monkeypatch.setattr(runner, name, lambda *a, _n=name, **k: touched.append(_n))
    assert error_of(r.runner.answer({"op": op, "chat": bad_id, **fields})) == "bad_request"
    assert touched == []


@pytest.mark.parametrize("op, fields", [
    ("chat.send", {"text": ""}), ("chat.send", {"text": "x" * 32001}), ("chat.send", {"text": "\ud800"}), ("chat.send", {"text": 5}),
    ("chat.events", {"offset": -1, "limit": 1}), ("chat.events", {"offset": 0, "limit": 0}), ("chat.events", {"offset": 0, "limit": 201}), ("chat.events", {"offset": True, "limit": 1}),
    ("chat.file", {"name": "../x", "offset": 0, "limit": 1}), ("chat.file", {"name": "..", "offset": 0, "limit": 1}), ("chat.file", {"name": "a\nb", "offset": 0, "limit": 1}),
    ("chat.file", {"name": "a.png", "offset": 0, "limit": 512 * 1024 + 1}), ("chat.file", {"name": "a.png", "offset": 0.5, "limit": 1}),
])
def test_field_values_out_of_bounds_are_bad_request(chat_runner, op, fields):
    r = chat_runner()
    assert error_of(r.runner.answer({"op": op, "chat": r.new(), **fields})) == "bad_request"


def test_a_text_of_32000_characters_is_taken_and_an_unknown_chat_is_not_found(chat_runner):
    r = chat_runner()
    chat = r.new()
    assert r.call("chat.send", chat=chat, text="é" * 32000)["ok"] is True
    r.wait(chat)
    assert error_of(r.call("chat.files", chat="c_0123456789ab")) == "not_found"


# ── the socket ────────────────────────────────────────────────────────────


def test_the_socket_is_0600_a_leftover_socket_is_replaced_and_anything_else_refuses(chat_runner, tmp_path):
    r = chat_runner()
    assert stat.S_IMODE(os.stat(r.socket).st_mode) == 0o600
    leftover = tmp_path / "left.sock"
    with socket.socket(socket.AF_UNIX) as s:
        s.bind(str(leftover))
    runner.bind(str(leftover)).close()
    assert stat.S_ISSOCK(os.lstat(leftover).st_mode)
    (tmp_path / "file.sock").write_text("x")
    with pytest.raises(runner.StartError):
        runner.bind(str(tmp_path / "file.sock"))


def test_connections_over_the_limit_are_closed_and_requests_and_replies_are_bounded(chat_runner):
    """Bug class: a client holding connections open starving the web UI, or a frame larger than the limit read anyway."""
    r = chat_runner(runner.Limits(connections_max=2, request_seconds=3, reply_bytes=300))
    folder_fd = os.open(os.path.dirname(r.socket), os.O_RDONLY)
    held = []
    try:
        for _ in range(2):
            held.append(socket.socket(socket.AF_UNIX))
            held[-1].connect(f"/proc/self/fd/{folder_fd}/runner.sock")
        time.sleep(0.2)
        assert r.call("hello") is None
        for s in held:
            s.close()
        time.sleep(0.2)
        assert r.call("hello")["result"] == {"version": 1, "backend": "claude-code", "databases": ["mongo"]}
    finally:
        os.close(folder_fd)
    big = json.dumps({"op": "hello", "pad": "x" * (64 * 1024)}).encode()
    assert error_of(exchange(r.socket, None, raw=struct.pack(">I", len(big)) + big)) == "request_too_large"
    for _ in range(6):
        r.new()
    assert error_of(r.call("chat.list")) == "too_large"


def test_openai_compatible_answers_hello_and_list_and_refuses_send(chat_runner):
    r = chat_runner(backend=False)
    assert r.call("hello")["result"]["backend"] == "openai-compatible"
    chat = r.new()
    assert r.call("chat.list")["result"]["chats"][0]["id"] == chat
    assert error_of(r.call("chat.send", chat=chat, text="hi")) == "backend_not_available"


# ── a turn ────────────────────────────────────────────────────────────────


def test_a_turn_runs_the_back_end_and_its_output_becomes_events(chat_runner):
    """Bug class: an event lost or out of order, the session not resumed, the text not given as one stream-json line."""
    r = chat_runner()
    chat = r.new()
    tool = ["out", {"type": "assistant", "message": {"content": [{"type": "tool_use", "name": "Bash", "input": {"command": "x" * 3000}}]}}]
    tool_result = ["out", {"type": "user", "message": {"content": [{"type": "tool_result", "content": [{"type": "text", "text": "MARK"}, {"type": "image"}], "is_error": False}]}}]
    r.plan([INIT, tool, tool_result, ["write", ["plot.png", "png"]], SAY, ["out", {"type": "rate_limit_event"}], result()])
    assert r.call("chat.send", chat=chat, text="draw a plot")["result"] == {"offset": 0}
    events = r.wait(chat)
    assert [(e["n"], e["type"]) for e in events] == [(0, "user"), (1, "tool"), (2, "tool_result"), (3, "text"), (4, "file"), (5, "end")]
    assert all(isinstance(e["t"], float) for e in events)
    assert events[1]["name"] == "Bash" and events[1]["input"] == json.dumps({"command": "x" * 3000}, separators=(",", ":"))[:2000]
    assert events[2]["text"] == "MARK\n[image]" and events[2]["is_error"] is False
    assert events[4] == {**events[4], "name": "plot.png", "size": 3}
    assert {k: events[5][k] for k in ("is_error", "cost_usd", "duration_ms", "turns")} == {"is_error": False, "cost_usd": 0.25, "duration_ms": 1200, "turns": 2}
    first = r.records()[0]
    assert json.loads(first["stdin"]) == {"type": "user", "message": {"role": "user", "content": "draw a plot"}} and first["stdin_end"] is True
    assert first["cwd"] == str(r.work / chat / "files")
    meta = json.loads((r.work / chat / "meta.json").read_text())
    assert meta["title"] == "draw a plot" and meta["turns"] == 1 and meta["resume"] is True
    r.plan([INIT, result()])
    r.call("chat.send", chat=chat, text="again")
    r.wait(chat)
    assert r.records()[0]["argv"][-2:] == ["--session-id", meta["session"]] and r.records()[1]["argv"][-2:] == ["--resume", meta["session"]]
    assert json.loads((r.work / chat / "home" / "mcp.json").read_text()) == {"mcpServers": {"mongo": {"type": "http", "url": "http://127.0.0.1:8002/mcp"}}}


@pytest.mark.parametrize("doc, expected", [
    ({"type": "assistant", "message": {"content": [{"type": "text", "text": "a"}, {"type": "thinking", "thinking": "t"}, {"type": "text", "text": ""}]}}, [("text", {"text": "a"})]),
    ({"type": "assistant", "message": {"content": [{"type": "tool_use", "name": "mcp__mongo__find", "input": {"q": "é"}}]}}, [("tool", {"name": "mcp__mongo__find", "input": '{"q":"é"}'})]),
    ({"type": "user", "message": {"content": [{"type": "tool_result", "content": "y" * 2500, "is_error": True}]}}, [("tool_result", {"text": "y" * 2000, "is_error": True})]),
    ({"type": "result", "is_error": True, "api_error_status": 403, "total_cost_usd": 0, "duration_ms": 5, "num_turns": 1}, [("error", {"message": "the model could not be reached"})]),
    ({"type": "result", "is_error": False, "total_cost_usd": "1", "duration_ms": True}, []),
    ({"type": "rate_limit_event"}, []), ({"type": "assistant", "message": "x"}, []), ({"type": "user", "message": {"content": [5, "x"]}}, []),
])
def test_stream_json_lines_are_translated(doc, expected):
    translator = claude.Translator()
    assert translator.events(doc) == expected
    if doc["type"] == "result":
        assert translator.end == ({"is_error": True, "cost_usd": 0, "duration_ms": 5, "turns": 1} if doc["is_error"] else {"is_error": False, "cost_usd": None, "duration_ms": None, "turns": None})


def test_lines_that_are_not_json_objects_or_too_long_are_dropped_and_counted():
    """Bug class: one bad line (not UTF-8, not JSON, over the limit) ending the turn, or a long line held whole in memory."""
    lines = []
    dropped = []
    read_fd, write_fd = os.pipe()
    os.write(write_fd, b'{"a": 1}\n\xff\xfe\nnot json\n[1]\n' + b"x" * 5000 + b'\n{"b": 2}\n{"c"')
    os.close(write_fd)
    try:
        claude.read_lines(read_fd, 1024, lines.append, lambda: dropped.append(1), threading.Event())
    finally:
        os.close(read_fd)
    assert lines == [b'{"a": 1}\n', b"\xff\xfe\n", b"not json\n", b"[1]\n", b'{"b": 2}\n', b'{"c"'] and len(dropped) == 1


def test_a_lone_surrogate_or_a_number_that_is_not_finite_in_the_output_keeps_the_turn_going(chat_runner):
    """Bug class: `json.loads` accepts `\\ud800`, `NaN` and `1e999`; the line then cannot be written as UTF-8 or JSON, and the reader thread died, so the turn hung until its limit."""
    r = chat_runner()
    chat = r.new()
    surrogate = '{"type": "assistant", "message": {"content": [{"type": "text", "text": "a\\ud800b"}, {"type": "tool_use", "name": "B\\udfff", "input": {"k": "\\ud800"}}]}}\n'
    not_finite = '{"type": "result", "is_error": false, "total_cost_usd": NaN, "duration_ms": 1e999, "num_turns": Infinity}\n'
    r.plan([INIT, ["out", surrogate], ["out", not_finite]])
    r.call("chat.send", chat=chat, text="hi")
    events = r.wait(chat, 10)
    assert [e["type"] for e in events] == ["user", "text", "tool", "end"]
    assert events[1]["text"] == "a?b" and events[2]["name"] == "B?" and events[2]["input"] == '{"k":"?"}'
    assert {k: events[3][k] for k in ("is_error", "cost_usd", "duration_ms", "turns")} == {"is_error": False, "cost_usd": None, "duration_ms": None, "turns": None}


def test_a_log_line_or_a_title_that_cannot_be_sent_is_answered_as_unreadable_not_internal(chat_runner, tmp_path):
    r = chat_runner()
    chat = r.new()
    (r.work / chat / "events.jsonl").write_bytes(b'{"n": 0, "type": "text", "text": "\\ud800"}\n{"n": 1, "type": "text", "text": "ok"}\n')
    r.server.close()
    r.runner.close()
    (r.work / chat / "meta.json").write_text('{"created": 1, "title": "x\\ud800", "session": "' + str(uuid.uuid4()) + '"}')
    r = chat_runner()
    assert [e["type"] for e in r.call("chat.events", chat=chat, offset=0, limit=10)["result"]["events"]] == ["error", "text"]
    assert r.call("chat.list")["result"]["chats"] == [{"id": chat, "title": "", "created": 1, "running": False}]


def test_garbage_and_a_back_end_ending_without_a_result_give_a_constant_error(chat_runner):
    r = chat_runner(runner.Limits())
    chat = r.new()
    r.plan([INIT, ["out", "garbage\n\xff\n"], SAY, ["exit", 3]])
    r.call("chat.send", chat=chat, text="hi")
    events = r.wait(chat)
    assert [e["type"] for e in events] == ["user", "text", "error", "end"]
    assert events[2]["message"] == "the back end ended with code 3" and events[3]["is_error"] is True


def test_a_back_end_that_cannot_be_started_gives_a_constant_error(chat_runner, tmp_path):
    r = chat_runner()
    (tmp_path / "claude").chmod(0o644)
    chat = r.new()
    r.call("chat.send", chat=chat, text="hi")
    events = r.wait(chat)
    assert [e["type"] for e in events] == ["user", "error", "end"] and events[1]["message"] == "the back end could not be started"


def test_the_back_end_gets_exactly_its_environment_and_nothing_of_the_runner(chat_runner, monkeypatch):
    """Bug class (security): the runner's environment (a variable planted in it) passed on to the assistant's process."""
    monkeypatch.setenv("SDP_CHAT_LEAK_MARKER", "leaked")
    r = chat_runner()
    chat = r.new()
    r.call("chat.send", chat=chat, text="hi")
    r.wait(chat)
    config, tmp = str(r.work / chat / "home"), str(r.tmp / f"sdp-chat-{chat}")
    assert r.records()[0]["env"] == {
        "HOME": f"{tmp}/home", "CLAUDE_CONFIG_DIR": config, "CLAUDE_CODE_TMPDIR": tmp, "TMPDIR": tmp, "MPLCONFIGDIR": f"{tmp}/matplotlib",
        "PATH": "/usr/local/bin:/usr/bin:/bin", "ANTHROPIC_BASE_URL": "http://127.0.0.1:8080", "CLAUDE_CODE_OAUTH_TOKEN": claude.PLACEHOLDER_TOKEN,
        "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1", "DISABLE_TELEMETRY": "1", "DISABLE_AUTOUPDATER": "1", "DISABLE_ERROR_REPORTING": "1", "CLAUDE_CODE_DISABLE_AUTO_MEMORY": "1",
    }


def test_each_turn_gets_a_fresh_empty_home(chat_runner):
    """Bug class (measured): a `.bashrc` the assistant left in its home ran in the next turn's Bash tool."""
    r = chat_runner()
    chat = r.new()
    r.plan([INIT, ["write", ["../../../tmp/sdp-chat-" + chat + "/home/.bashrc", "touch planted"]], result()])
    r.call("chat.send", chat=chat, text="one")
    r.wait(chat)
    assert (r.tmp / f"sdp-chat-{chat}" / "home" / ".bashrc").exists()
    r.plan([INIT, result()])
    r.call("chat.send", chat=chat, text="two")
    r.wait(chat)
    assert sorted(os.listdir(r.tmp / f"sdp-chat-{chat}")) == ["home", "matplotlib"] and os.listdir(r.tmp / f"sdp-chat-{chat}" / "home") == []


@pytest.mark.parametrize("planted", ["link", "file", "folder"])
def test_whatever_is_in_the_place_of_the_scratch_folder_is_replaced_without_following_a_link(chat_runner, tmp_path, planted):
    """Bug class (security): a link planted at `/tmp/sdp-chat-<id>` (any chat's code can write `/tmp`) followed to make folders elsewhere and then blocking every later turn, or a `matplotlibrc` read by every later turn."""
    r = chat_runner()
    chat = r.new()
    marker = tmp_path / "marker"
    marker.mkdir()
    (marker / "keep.txt").write_text("keep")
    place = r.tmp / f"sdp-chat-{chat}"
    if planted == "link":
        place.symlink_to(marker)
    elif planted == "file":
        place.write_text("x")
    else:
        (place / "matplotlib").mkdir(parents=True)
        (place / "matplotlib" / "matplotlibrc").write_text("backend: planted")
    r.call("chat.send", chat=chat, text="hi")
    assert [e["type"] for e in r.wait(chat)] == ["user", "end"]
    assert os.listdir(marker) == ["keep.txt"] and not place.is_symlink()
    assert sorted(os.listdir(place)) == ["home", "matplotlib"] and os.listdir(place / "matplotlib") == []


def test_the_command_lines_of_a_first_and_a_resumed_turn_are_pinned():
    first = claude.command("/usr/local/bin/claude", "/work/c_0123456789ab/home", "5f6cec57-5f0d-4a8c-a927-8480b9f00bb1", False, ["postgres", "mongo"])
    assert first == [
        "/usr/local/bin/claude", "-p", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose",
        "--mcp-config", "/work/c_0123456789ab/home/mcp.json", "--strict-mcp-config", "--tools", "Bash,Read,Write,Edit,Glob,Grep",
        "--permission-mode", "dontAsk", "--allowedTools", "Bash,Read,Write,Edit,Glob,Grep,mcp__postgres,mcp__mongo",
        "--setting-sources", "", "--settings", '{"disableAllHooks":true,"enabledPlugins":{"cc-plugin-agents-md@builtin":false,"cc-plugin-privacy@builtin":false,"cc-plugin-plugin-authoring@builtin":false}}',
        "--append-system-prompt", claude.ROLE, "--session-id", "5f6cec57-5f0d-4a8c-a927-8480b9f00bb1",
    ]
    resumed = claude.command("/usr/local/bin/claude", "/work/c_0123456789ab/home", "5f6cec57-5f0d-4a8c-a927-8480b9f00bb1", True, ["postgres", "mongo"])
    assert resumed == first[:-2] + ["--resume", "5f6cec57-5f0d-4a8c-a927-8480b9f00bb1"]


@pytest.mark.parametrize("databases, expected", [
    ([], {}),
    (["postgres"], {"postgres": {"type": "sse", "url": "http://10.1.0.2:8001/sse"}}),
    (["mongo", "starrocks"], {"mongo": {"type": "http", "url": "http://10.1.0.2:8002/mcp"}, "starrocks": {"type": "http", "url": "http://10.1.0.2:8003/mcp"}}),
])
def test_the_mcp_config_has_one_server_per_database_through_the_gateway(databases, expected):
    assert claude.mcp_config(databases, "10.1.0.2") == {"mcpServers": expected}


@pytest.mark.parametrize("how", ["stop", "limit"])
def test_stop_and_the_time_limit_end_the_whole_process_group(chat_runner, tmp_path, how):
    """Bug class: a stopped turn leaving the assistant's processes running (a child that ignores TERM, or the back end itself)."""
    r = chat_runner(runner.Limits(turn_seconds=1 if how == "limit" else 600))
    chat = r.new()
    pid_file = tmp_path / "child.pid"
    r.plan([INIT, ["child", str(pid_file)], ["ignore_term", 0], SLEEP])
    r.call("chat.send", chat=chat, text="hi")
    deadline = time.monotonic() + 10
    while not pid_file.exists() or not pid_file.read_text():
        assert time.monotonic() < deadline
        time.sleep(0.05)
    if how == "stop":
        assert r.call("chat.stop", chat=chat)["result"] == {"stopping": True}
    events = r.wait(chat)
    assert [e["type"] for e in events][-2:] == ["error", "end"]
    assert events[-2]["message"] == ("the turn was stopped" if how == "stop" else "the turn took longer than the limit")
    deadline = time.monotonic() + 5
    while not gone(int(pid_file.read_text())):
        assert time.monotonic() < deadline, "the child that ignores TERM is still running"
        time.sleep(0.05)
    assert r.call("chat.stop", chat=chat)["result"] == {"stopping": False}


def test_busy_too_many_and_over_cap(chat_runner):
    """Bug class: two turns of one chat on one session, more turns than the sandbox has room for, or a turn started over the soft cap."""
    r = chat_runner(runner.Limits(turns_max=2), cap=4096)
    a, b, c = r.new(), r.new(), r.new()
    r.plan([INIT, SLEEP])
    assert r.call("chat.send", chat=a, text="1")["ok"] and r.call("chat.send", chat=b, text="2")["ok"]
    assert error_of(r.call("chat.send", chat=a, text="again")) == "busy"
    assert error_of(r.call("chat.send", chat=c, text="3")) == "too_many"
    assert error_of(r.call("chat.delete", chat=a)) == "busy"
    for chat in (a, b):
        r.call("chat.stop", chat=chat)
        r.wait(chat)
    (r.work / c / "files" / "big.csv").write_bytes(b"x" * 5000)
    assert error_of(r.call("chat.send", chat=c, text="3")) == "over_cap"


def test_at_most_the_chats_limit(chat_runner):
    r = chat_runner(runner.Limits(chats_max=3))
    for _ in range(3):
        r.new()
    assert error_of(r.call("chat.new")) == "too_many_chats"


# ── the log ───────────────────────────────────────────────────────────────


def test_events_are_paged_by_index_and_by_bytes(chat_runner):
    r = chat_runner(runner.Limits(events_bytes=350))
    chat = r.new()
    for i in range(10):
        r.runner._append(r.runner._chats[chat], "text", {"text": f"{i}" * 50})
    page = r.call("chat.events", chat=chat, offset=3, limit=2)["result"]
    assert [e["n"] for e in page["events"]] == [3, 4] and page["next"] == 5 and page["running"] is False
    page = r.call("chat.events", chat=chat, offset=0, limit=200)["result"]
    assert [e["n"] for e in page["events"]] == [0, 1, 2] and page["next"] == 3
    assert r.call("chat.events", chat=chat, offset=99, limit=5)["result"] == {"events": [], "next": 10, "running": False}


def test_a_full_log_ends_with_one_error_and_refuses_more(chat_runner):
    """Bug class: a chat's log growing without bound (the 16 MiB cap, here small)."""
    r = chat_runner(runner.Limits(log_bytes=4096, event_bytes=1024))
    chat = r.new()
    r.plan([INIT] + [SAY] * 60 + [SLEEP])
    r.call("chat.send", chat=chat, text="x" * 3000)
    events = r.wait(chat)
    assert events[0]["type"] == "user" and len(json.dumps(events[0], ensure_ascii=False)) < 1024
    assert events[-2] == {**events[-2], "type": "error", "message": "the chat is full"} and events[-1]["type"] == "end"
    assert [e["type"] for e in events].count("end") == 1 and os.path.getsize(r.work / chat / "events.jsonl") <= 4096
    assert error_of(r.call("chat.send", chat=chat, text="more")) == "chat_full"


def test_the_line_index_is_bounded_and_a_log_of_bare_newlines_is_full(chat_runner):
    """Bug class: a 16 MiB log of bare newlines indexed as millions of Python integers."""
    r = chat_runner(runner.Limits(log_bytes=3200))
    chat = r.new()
    (r.work / chat / "events.jsonl").write_bytes(b"\n" * 3000)
    r.call("chat.events", chat=chat, offset=0, limit=1)
    offsets = r.runner._chats[chat].offsets
    assert offsets.typecode == "Q" and len(offsets) <= 3200 // 32 + 1
    assert error_of(r.call("chat.send", chat=chat, text="hi")) == "chat_full"


def test_a_failed_write_makes_the_index_be_built_again(chat_runner, monkeypatch):
    """Bug class: a write that failed half-way leaving the cached offsets pointing past the file's real lines."""
    r = chat_runner()
    chat = r.new()
    state = r.runner._chats[chat]
    r.runner._append(state, "text", {"text": "first"})
    real = runner._write_all

    def half(fd, data):
        os.write(fd, data[: len(data) // 2])
        raise OSError("disk full")

    monkeypatch.setattr(runner, "_write_all", half)
    assert r.runner._append(state, "text", {"text": "second"}) is None and state.offsets is None
    monkeypatch.setattr(runner, "_write_all", real)
    r.runner._append(state, "text", {"text": "third"})
    events = r.call("chat.events", chat=chat, offset=0, limit=10)["result"]["events"]
    lines = (r.work / chat / "events.jsonl").read_bytes().count(b"\n")
    assert len(events) == lines == 3 and events[0]["text"] == "first" and events[1]["message"] == "this event could not be read" and events[2]["text"] == "third"


def test_an_event_line_written_by_someone_else_is_answered_as_unreadable(chat_runner):
    r = chat_runner()
    chat = r.new()
    (r.work / chat / "events.jsonl").write_bytes(b'{"n": 0, "type": "text", "text": "ok"}\n' + b"[" * 100 + b"\n")
    events = r.call("chat.events", chat=chat, offset=0, limit=10)["result"]["events"]
    assert events[0]["text"] == "ok" and events[1] == {"n": 1, "t": None, "type": "error", "message": "this event could not be read"}


# ── files ─────────────────────────────────────────────────────────────────


def test_new_and_changed_files_are_reported_and_links_and_other_files_never_listed_or_opened(chat_runner, tmp_path):
    """Bug class (security): a link in the chat's folder followed to a file outside (its content in a reply), or a FIFO opened and the runner blocked."""
    r = chat_runner()
    chat = r.new()
    files = r.work / chat / "files"
    (tmp_path / "outside").write_text("OUTSIDE-MARKER")
    (files / "same.png").write_text("same")
    (files / "changed.png").write_text("old")
    (files / "link.png").symlink_to(tmp_path / "outside")
    os.mkfifo(files / "fifo.png")
    (files / "folder").mkdir()
    (files / "naïve.png").write_text("x")
    r.plan([INIT, ["write", ["changed.png", "newer"]], ["write", ["new.png", "brand new"]], result()])
    r.call("chat.send", chat=chat, text="hi")
    assert [(e["name"], e["size"]) for e in r.wait(chat) if e["type"] == "file"] == [("changed.png", 5), ("new.png", 9)]
    listed = r.call("chat.files", chat=chat)["result"]
    assert [f["name"] for f in listed["files"]] == ["changed.png", "new.png", "same.png"] and listed["left_out"] == 1 and listed["more"] is False
    replies = [r.call("chat.file", chat=chat, name=name, offset=0, limit=1000) for name in ("link.png", "fifo.png", "folder")]
    assert [error_of(reply) for reply in replies] == ["no_such_file"] * 3
    assert "OUTSIDE-MARKER" not in json.dumps(replies) and base64.b64encode(b"OUTSIDE-MARKER").decode() not in json.dumps(replies)
    piece = r.call("chat.file", chat=chat, name="new.png", offset=6, limit=2)["result"]
    assert piece == {"data": base64.b64encode(b"ne").decode(), "size": 9, "next": 8, "end": False}


def test_delete_follows_no_link_and_removes_a_deep_tree(chat_runner, tmp_path):
    r = chat_runner()
    chat = r.new()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep.txt").write_text("keep")
    (r.work / chat / "files" / "out").symlink_to(outside)
    deep = r.work / chat / "files"
    for _ in range(300):
        deep = deep / "d"
        deep.mkdir()
    os.chmod(deep, 0)
    assert r.call("chat.delete", chat=chat)["result"] == {}
    assert not (r.work / chat).exists() and r.call("chat.list")["result"] == {"chats": []}
    assert error_of(r.call("chat.files", chat=chat)) == "not_found"
    assert r.runner.sweep() == 0 and os.listdir(r.work) == [] and (outside / "keep.txt").read_text() == "keep"


def test_at_most_the_chat_limit_is_loaded_the_newest_and_the_list_fits_a_reply(chat_runner, tmp_path):
    """Bug class: planted `c_...` folders making the runner hold any number of chats and `chat.list` answer `too_large`."""
    work = tmp_path / "work"
    work.mkdir()
    for i in range(250):
        folder = work / f"c_{i:012x}"
        folder.mkdir()
        (folder / "meta.json").write_text(json.dumps({"created": i, "title": "\U0001f600" * 300, "session": str(uuid.uuid4())}))
        os.utime(folder, ns=(i * 10**9, i * 10**9))
    r = chat_runner()
    chats = r.call("chat.list")["result"]["chats"]
    assert len(chats) == 200 and {c["id"] for c in chats} == {f"c_{i:012x}" for i in range(50, 250)} and all(len(c["title"]) == 80 for c in chats)


def test_a_chat_without_a_valid_session_gets_a_new_one(chat_runner, tmp_path):
    work = tmp_path / "work"
    (work / "c_0123456789ab" / "home").mkdir(parents=True)
    (work / "c_0123456789ab" / "files").mkdir()
    (work / "c_0123456789ab" / "meta.json").write_text('{"created": 1, "title": "t", "session": "not-a-uuid", "resume": true}')
    r = chat_runner()
    r.call("chat.send", chat="c_0123456789ab", text="hi")
    r.wait("c_0123456789ab")
    flag, session = r.records()[0]["argv"][-2:]
    assert flag == "--session-id" and str(uuid.UUID(session)) == session


def test_a_failure_after_the_start_ends_the_process_group(monkeypatch, tmp_path):
    """Bug class: an exception between `Popen` and the supervision leaving the assistant's process running unsupervised."""
    class Failing(threading.Thread):
        def start(self):
            raise RuntimeError("no thread")

    monkeypatch.setattr(claude.threading, "Thread", Failing)
    folder_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    process = claude.Process([sys.executable, "-c", "import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)"], {}, str(tmp_path), b"", folder_fd, grace=0.3)
    try:
        with pytest.raises(RuntimeError):
            process.run(lambda doc: None)
    finally:
        os.close(folder_fd)
    assert process._proc.returncode in (-signal.SIGTERM, -signal.SIGKILL) and gone(process._proc.pid)


def wait_for_threads(count, seconds=5):
    deadline = time.monotonic() + seconds
    while threading.active_count() > count:
        assert time.monotonic() < deadline, "a thread of the turn is still alive"
        time.sleep(0.02)


def test_a_reader_still_busy_after_the_turn_never_reads_a_descriptor_opened_afterwards(tmp_path):
    """Bug class: the turn's pipes closed while its reader was still running, which then read a reused descriptor number (another turn's pipe, a socket)."""
    baseline = threading.active_count()
    release, docs = threading.Event(), []

    def slow_sink(doc):
        docs.append(doc)
        release.wait(10)

    folder_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    script = "import sys; sys.stdout.write('{\"a\": 1}\\n{\"b\": 2}\\n')"
    process = claude.Process([sys.executable, "-c", script], {}, str(tmp_path), b"", folder_fd, grace=0.3, join=0.2)
    try:
        process.run(slow_sink)
        read_fd, write_fd = os.pipe()
        os.write(write_fd, b'{"poison": 1}\n')
        release.set()
        wait_for_threads(baseline)
        assert docs[0] == {"a": 1} and {"poison": 1} not in docs and os.read(read_fd, 100) == b'{"poison": 1}\n'
        os.close(read_fd)
        os.close(write_fd)
    finally:
        release.set()
        os.close(folder_fd)


def test_a_failing_second_thread_start_leaves_no_thread_alive(monkeypatch, tmp_path):
    baseline = threading.active_count()

    class Second(threading.Thread):
        starts = 0

        def start(self):
            type(self).starts += 1
            if type(self).starts == 2:
                raise RuntimeError("no thread")
            super().start()

    monkeypatch.setattr(claude.threading, "Thread", Second)
    folder_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    process = claude.Process([sys.executable, "-c", "import time; time.sleep(60)"], {}, str(tmp_path), b"", folder_fd, grace=0.3)
    try:
        with pytest.raises(RuntimeError):
            process.run(lambda doc: None)
    finally:
        os.close(folder_fd)
    wait_for_threads(baseline)
    assert Second.starts == 2 and gone(process._proc.pid)


def test_two_held_pipes_cost_one_join_period(tmp_path):
    """Bug class: a detached process holding standard output and standard error making the turn wait the join period once per pipe."""
    pid_file = tmp_path / "held.pid"
    (tmp_path / "held.py").write_text(
        "import os, subprocess, sys, time\n"
        "subprocess.Popen([sys.executable, '-c', 'import os, sys, time; os.setsid(); open(sys.argv[1], \"w\").write(str(os.getpid())); time.sleep(600)', sys.argv[1]])\n"
        "while not (os.path.exists(sys.argv[1]) and open(sys.argv[1]).read()):\n"
        "    time.sleep(0.02)\n"
    )
    folder_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    process = claude.Process([sys.executable, str(tmp_path / "held.py"), str(pid_file)], {}, str(tmp_path), b"", folder_fd, grace=0.3, join=1.0)
    try:
        started = time.monotonic()
        process.run(lambda doc: None)
        took = time.monotonic() - started
    finally:
        os.close(folder_fd)
        if pid_file.exists() and pid_file.read_text():
            os.kill(int(pid_file.read_text()), signal.SIGKILL)
    assert 1.0 <= took < 1.8


def test_a_process_that_left_the_group_and_holds_the_pipes_does_not_hold_the_turn(chat_runner, tmp_path):
    """Bug class: a child that called `setsid` keeping standard output open, so the turn (and its slot) never ended."""
    r = chat_runner()
    chat = r.new()
    pid_file = tmp_path / "detached.pid"
    r.plan([INIT, ["detached", str(pid_file)], result()])
    r.call("chat.send", chat=chat, text="hi")
    try:
        events = r.wait(chat, 10)
        assert events[-1]["type"] == "end" and events[-1]["is_error"] is False
    finally:
        os.kill(int(pid_file.read_text()), signal.SIGKILL)


def test_nothing_a_detached_flooding_process_prints_lands_after_the_end(chat_runner, tmp_path):
    """Bug class: complete lines already read handed on after the stop, so events landed after the turn's `end`, even in the next turn."""
    r = chat_runner()
    chat = r.new()
    pid_file = tmp_path / "flood.pid"
    r.plan([INIT, ["flood", str(pid_file)], ["sleep", 0.3], result()])
    r.call("chat.send", chat=chat, text="one")

    def kinds():
        return [json.loads(line)["type"] for line in (r.work / chat / "events.jsonl").read_text().splitlines()]

    try:
        r.wait(chat, 10)
        first = kinds()
        assert first[-1] == "end" and first.count("end") == 1
        time.sleep(1)
        assert kinds() == first
        r.plan([INIT, result()])
        r.call("chat.send", chat=chat, text="two")
        r.wait(chat, 10)
        time.sleep(0.5)
        assert kinds()[len(first):] == ["user", "end"]
    finally:
        with contextlib.suppress(ProcessLookupError, ValueError):
            os.kill(int(pid_file.read_text()), signal.SIGKILL)


def refilling(monkeypatch):
    """`runner.scan` that first puts a new file in the folder it lists: a writer refilling it as fast as it is emptied."""
    real, count = runner.scan, [0]

    def scan(fd, limit):
        count[0] += 1
        with contextlib.suppress(OSError):
            os.close(os.open(f"refill-{count[0]}", os.O_WRONLY | os.O_CREAT, 0o600, dir_fd=fd))
        return real(fd, limit)

    monkeypatch.setattr(runner, "scan", scan)


def test_a_removal_under_a_refilling_writer_is_bounded(monkeypatch, tmp_path):
    (tmp_path / "x" / "sub").mkdir(parents=True)
    refilling(monkeypatch)
    folder_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(runner.KeptChanging):
            runner.remove_tree(folder_fd, "x")
    finally:
        os.close(folder_fd)


def test_delete_under_a_writer_answers_at_once_and_the_sweep_removes_it_later(chat_runner):
    """Bug class: a delete removing in place under a leftover process that keeps writing: it never returned, or left a half-deleted chat."""
    r = chat_runner()
    chat = r.new()
    files_fd = os.open(r.work / chat / "files", os.O_RDONLY | os.O_DIRECTORY)
    stop = threading.Event()

    def writer():
        i = 0
        while not stop.is_set():
            i += 1
            with contextlib.suppress(OSError):
                os.close(os.open(f"w-{i}", os.O_WRONLY | os.O_CREAT, 0o600, dir_fd=files_fd))

    thread = threading.Thread(target=writer, daemon=True)
    thread.start()
    try:
        started = time.monotonic()
        assert r.call("chat.delete", chat=chat)["result"] == {}
        assert time.monotonic() - started < 1 and r.call("chat.list")["result"] == {"chats": []}
        fresh = r.new()
    finally:
        stop.set()
        thread.join(5)
        os.close(files_fd)
    assert r.runner.sweep() == 0 and os.listdir(r.work) == [fresh]


class Interleaved:
    """Stands in for `os` inside `runner`: `before(name, path, dir_fd)` runs before each call, so another actor's change lands at an exact step."""

    def __init__(self, before):
        self.before = before

    def __getattr__(self, name):
        real = getattr(os, name)
        if not callable(real) or name not in ("chmod", "rmdir"):
            return real

        def call(path, *args, **kwargs):
            self.before(name, path, kwargs.get("dir_fd"))
            return real(path, *args, **kwargs)

        return call


def test_a_subfolder_removed_between_its_look_and_its_opening_does_not_end_the_removal(monkeypatch, tmp_path):
    (tmp_path / "x" / "victim").mkdir(parents=True)
    (tmp_path / "x" / "z").mkdir()
    (tmp_path / "x" / "z" / "b.txt").write_text("b")
    (tmp_path / "x" / "a.txt").write_text("a")

    def before(name, path, dir_fd):
        if name == "chmod" and path == "victim":
            os.rmdir("victim", dir_fd=dir_fd)

    monkeypatch.setattr(runner, "os", Interleaved(before))
    folder_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        runner.remove_tree(folder_fd, "x")
    finally:
        os.close(folder_fd)
    assert not (tmp_path / "x").exists()


@pytest.mark.parametrize("refilled", ["sub", "x"])
def test_a_folder_found_not_empty_at_its_removal_is_listed_again(monkeypatch, tmp_path, refilled):
    (tmp_path / "x" / "sub").mkdir(parents=True)
    added = []

    def before(name, path, dir_fd):
        if name == "rmdir" and path == refilled and not added:
            added.append(path)
            os.close(os.open(f"{path}/late.txt", os.O_WRONLY | os.O_CREAT, 0o600, dir_fd=dir_fd))

    monkeypatch.setattr(runner, "os", Interleaved(before))
    folder_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        runner.remove_tree(folder_fd, "x")
    finally:
        os.close(folder_fd)
    assert added == [refilled] and not (tmp_path / "x").exists()


def test_renamed_away_folders_are_never_chats_and_are_swept_at_start_up(chat_runner, tmp_path):
    work, tmp = tmp_path / "work", tmp_path / "tmp"
    old_chat, old_scratch = work / "c_0123456789ab.old-0badc0de", tmp / "sdp-chat-c_0123456789ab.old-0badc0de"
    for folder in (old_chat / "files" / "deep", old_chat / "home", old_scratch / "home"):
        folder.mkdir(parents=True)
    (old_chat / "meta.json").write_text(json.dumps({"created": 1, "title": "old", "session": str(uuid.uuid4())}))
    (old_chat / "files" / "a.png").write_text("x")
    r = chat_runner()
    assert r.call("chat.list")["result"] == {"chats": []}
    assert error_of(r.call("chat.files", chat=old_chat.name)) == "bad_request"
    assert error_of(r.call("chat.file", chat=old_chat.name, name="a.png", offset=0, limit=10)) == "bad_request"
    assert r.runner.sweep() == 0 and os.listdir(work) == [] and os.listdir(tmp) == []


def test_a_scratch_folder_still_being_written_is_renamed_away_and_the_turn_gets_a_fresh_one(chat_runner):
    """Bug class: a leftover process filling the old scratch folder as fast as it is emptied, so the turn never started and the chat stayed running."""
    r = chat_runner()
    chat = r.new()
    old = r.tmp / f"sdp-chat-{chat}"
    old.mkdir()
    old_fd = os.open(old, os.O_RDONLY | os.O_DIRECTORY)
    stop = threading.Event()

    def writer():
        i = 0
        while not stop.is_set():
            i += 1
            with contextlib.suppress(OSError):
                os.close(os.open(f"w-{i}", os.O_WRONLY | os.O_CREAT, 0o600, dir_fd=old_fd))

    thread = threading.Thread(target=writer, daemon=True)
    thread.start()
    try:
        started = time.monotonic()
        path = r.runner._fresh_scratch(chat)
        assert time.monotonic() - started < 2 and sorted(os.listdir(path)) == ["home", "matplotlib"]
    finally:
        stop.set()
        thread.join(5)
        os.close(old_fd)
    # The writer has stopped; this pass waits for the one `_fresh_scratch` started in the background.
    assert r.runner.sweep() == 0
    left = sorted(os.listdir(r.tmp))
    assert left == [f"sdp-chat-{chat}"], f"left in the tmp root: {left}"


def sweep_idle(r, seconds=5):
    deadline = time.monotonic() + seconds
    while r.runner._sweeping:
        assert time.monotonic() < deadline, "the sweep did not end"
        time.sleep(0.02)


def test_a_sweep_asked_for_while_one_runs_is_queued_once(chat_runner, monkeypatch):
    """Bug class: a delete during a running sweep, whose listing was made before it, removed by no pass until some later request."""
    r = chat_runner()
    first, second = r.new(), r.new()
    r.runner.sweep()
    sweep_idle(r)
    entered, release, passes = threading.Event(), threading.Event(), []
    real_scan, real_sweep = runner.scan, r.runner.sweep

    def held_scan(fd, limit):
        names = real_scan(fd, limit)
        if fd == r.runner.work_fd and not entered.is_set():
            entered.set()
            release.wait(10)
        return names

    def counted_sweep():
        passes.append(1)
        return real_sweep()

    monkeypatch.setattr(runner, "scan", held_scan)
    monkeypatch.setattr(r.runner, "sweep", counted_sweep)
    assert r.call("chat.delete", chat=first)["result"] == {}
    assert entered.wait(5)
    assert r.call("chat.delete", chat=second)["result"] == {}
    for _ in range(10):
        r.runner._sweep_later()
    release.set()
    sweep_idle(r)
    assert len(passes) == 2 and os.listdir(r.work) == []


def test_a_send_refused_over_the_cap_asks_for_a_sweep(chat_runner, monkeypatch):
    r = chat_runner(cap=10)
    chat = r.new()
    asked = []
    monkeypatch.setattr(r.runner, "_sweep_later", lambda: asked.append(1))
    assert error_of(r.call("chat.send", chat=chat, text="hi")) == "over_cap" and asked == [1]


def test_a_sweep_asked_for_just_as_the_sweeper_decides_to_end_is_not_lost(chat_runner, monkeypatch):
    """Bug class: the sweeper decides to end, and clears its flag only later; a request landing in between sees a sweeper that is still marked running, queues itself, and is never served."""
    r = chat_runner()
    r.runner.sweep()
    sweep_idle(r)
    real_lock, real_sweep = r.runner._lock, r.runner.sweep
    state = {"armed": False, "asked": False}
    passes, second = [], threading.Event()

    class Watched:
        def __enter__(self):
            return real_lock.__enter__()

        def __exit__(self, *exc):
            out = real_lock.__exit__(*exc)
            if state["armed"] and not state["asked"] and threading.current_thread().name == "sweep":
                state["asked"] = True
                r.runner._sweep_later()
            return out

        def acquire(self, *args, **kwargs):
            return real_lock.acquire(*args, **kwargs)

        def release(self):
            return real_lock.release()

    def counted_sweep():
        passes.append(1)
        out = real_sweep()
        if len(passes) == 1:
            state["armed"] = True
        else:
            second.set()
        return out

    monkeypatch.setattr(r.runner, "sweep", counted_sweep)
    monkeypatch.setattr(r.runner, "_lock", Watched())
    r.runner._sweep_later()
    assert second.wait(5), "the request made as the sweeper ended was never served"
    monkeypatch.setattr(r.runner, "_lock", real_lock)
    sweep_idle(r)
    assert len(passes) == 2 and state["asked"]


def test_the_sweep_removes_the_scratch_folder_of_a_chat_that_is_not_listed(chat_runner):
    r = chat_runner()
    idle, busy = r.new(), r.new()
    r.plan([INIT, SLEEP])
    r.call("chat.send", chat=busy, text="hold")
    deadline = time.monotonic() + 10
    while not r.records():
        assert time.monotonic() < deadline
        time.sleep(0.02)
    stray = r.tmp / "sdp-chat-c_0123456789ab"
    (stray / "home").mkdir(parents=True)
    (stray / "home" / ".bashrc").write_text("x")
    (r.tmp / f"sdp-chat-{idle}").mkdir()
    (r.tmp / f"sdp-chat-{idle}" / "kept").write_text("x")
    (r.tmp / f"sdp-chat-{busy}" / "kept").write_text("x")
    try:
        assert r.runner.sweep() == 0
        assert sorted(os.listdir(r.tmp)) == sorted([f"sdp-chat-{idle}", f"sdp-chat-{busy}"])
        assert (r.tmp / f"sdp-chat-{idle}" / "kept").exists() and (r.tmp / f"sdp-chat-{busy}" / "kept").exists()
    finally:
        r.call("chat.stop", chat=busy)
        r.wait(busy)


def test_a_huge_folder_is_read_only_up_to_its_limit(tmp_path):
    for i in range(50):
        (tmp_path / f"f{i}").write_text("x")
    folder_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        names, more = runner.scan(folder_fd, 10)
        assert len(names) == 10 and more is True and runner.scan(folder_fd, 50)[1] is False
        assert runner.measure(folder_fd, 49) == (runner.measure(folder_fd, 49)[0], False) and runner.measure(folder_fd, 50) == (50, True)
    finally:
        os.close(folder_fd)


@pytest.mark.parametrize("environ, word", [
    ({"SDP_CHAT_BACKEND": "other", "SDP_CHAT_CAP_GIB": "10"}, "SDP_CHAT_BACKEND"),
    ({"SDP_CHAT_BACKEND": "claude-code", "SDP_CHAT_DATABASES": "mongo,mysql", "SDP_CHAT_CAP_GIB": "10", "SDP_CHAT_GATEWAY": "10.0.0.2"}, "SDP_CHAT_DATABASES"),
    ({"SDP_CHAT_BACKEND": "claude-code", "SDP_CHAT_CAP_GIB": "0", "SDP_CHAT_GATEWAY": "10.0.0.2"}, "SDP_CHAT_CAP_GIB"),
    ({"SDP_CHAT_BACKEND": "claude-code", "SDP_CHAT_CAP_GIB": "10", "SDP_CHAT_GATEWAY": "gateway"}, "SDP_CHAT_GATEWAY"),
])
def test_a_setting_that_cannot_be_used_refuses_the_start_naming_it(environ, word):
    with pytest.raises(main.SettingError, match=word):
        main.settings(environ)
