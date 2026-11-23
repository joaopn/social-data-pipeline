"""A generated title for each new chat: the runner's title job (with a scripted `title()` of the back end, `chat_helpers.ScriptedTitles`), the hostile answer made a title, the switch, and the two back ends' `title()` against stand-ins.

`test_a_real_claude_title_sends_one_small_request_without_tools` runs the pinned Claude Code against a stand-in API when `SDP_TEST_CLAUDE_BINARY` names one (skipped otherwise, as in CI).
"""

import contextlib
import json
import os
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import standins
from chat_helpers import ScriptedTitles, result
from standins import reply, wait_for

from social_data_pipeline import chat_host
from social_data_pipeline.chat import __main__ as main
from social_data_pipeline.chat import claude, openai, runner

QUESTION = "How many comments per subreddit were written in languages other than English in 2024?"
PLAIN = " ".join(QUESTION.split())[:80]


def meta_of(r, chat):
    return json.loads((r.work / chat / "meta.json").read_text())


def entry_of(r, chat):
    return next(c for c in r.call("chat.list")["result"]["chats"] if c["id"] == chat)


def titles_of(events):
    return [e for e in events if e["type"] == "title"]


def settled(r, titles, calls):
    """Wait until `calls` title calls were made and no title job runs any more."""
    wait_for(lambda: len(titles.calls) == calls and r.runner._titling == 0, what=f"{calls} title jobs ended")


def first_question(r, chat, text=QUESTION):
    assert r.call("chat.send", chat=chat, text=text)["ok"] is True
    return r.wait(chat)


# ── the job ───────────────────────────────────────────────────────────────


def test_a_title_arrives_and_replaces_the_question_title(chat_runner):
    """Bug class: the generated title not stored (lost at the sandbox's restart), not marked as the model's, its cost not in the log, or the whole question (not its first 2,000 characters) sent."""
    titles = ScriptedTitles(("answer", "Non-English share per subreddit", 0.0012))
    r = chat_runner(auto_title=True, titles=titles)
    chat = r.new()
    first_question(r, chat, QUESTION + " " + "x" * 3000)
    settled(r, titles, 1)
    assert titles.calls == [((QUESTION + " " + "x" * 3000)[:2000], 30.0)]
    assert entry_of(r, chat) == {"id": chat, "title": "Non-English share per subreddit", "created": entry_of(r, chat)["created"], "running": False, "model": None}
    meta = meta_of(r, chat)
    assert meta["title"] == "Non-English share per subreddit" and meta["title_by"] == "model"
    events = r.call("chat.events", chat=chat, offset=0, limit=200)["result"]["events"]
    assert [{k: e[k] for k in ("type", "text", "cost_usd")} for e in titles_of(events)] == [{"type": "title", "text": "Non-English share per subreddit", "cost_usd": 0.0012}]
    assert [e["type"] for e in events].count("end") == 1


def test_a_rename_before_the_title_arrives_wins(chat_runner):
    """Bug class: the generated title overwriting the title the operator set while it was being made."""
    release = threading.Event()
    titles = ScriptedTitles(("wait", release, "Generated title", 0.001))
    r = chat_runner(auto_title=True, titles=titles)
    chat = r.new()
    first_question(r, chat)
    assert r.call("chat.rename", chat=chat, title="mine")["result"] == {"title": "mine"}
    assert meta_of(r, chat)["title_by"] == "user"
    release.set()
    settled(r, titles, 1)
    assert entry_of(r, chat)["title"] == "mine"
    assert meta_of(r, chat)["title"] == "mine" and meta_of(r, chat)["title_by"] == "user"
    assert titles_of(r.call("chat.events", chat=chat, offset=0, limit=200)["result"]["events"]) == []


def test_only_the_first_question_starts_a_title(chat_runner):
    """Bug class: every question paying for a title, or a later question replacing the title."""
    titles = ScriptedTitles(("answer", "First title", None), ("answer", "Second title", None))
    r = chat_runner(auto_title=True, titles=titles)
    chat = r.new()
    first_question(r, chat)
    settled(r, titles, 1)
    first_question(r, chat, "a second question")
    time.sleep(0.3)
    assert len(titles.calls) == 1 and entry_of(r, chat)["title"] == "First title"
    renamed = r.new()
    r.call("chat.rename", chat=renamed, title="set before any question")
    first_question(r, renamed)
    time.sleep(0.3)
    assert len(titles.calls) == 1 and entry_of(r, renamed)["title"] == "set before any question"


def test_with_titles_off_no_job_starts(chat_runner):
    """Bug class: the switch ignored, so a deployment that turned titles off still pays for them."""
    titles = ScriptedTitles()
    r = chat_runner(auto_title=False, titles=titles)
    chat = r.new()
    first_question(r, chat)
    time.sleep(0.3)
    assert titles.calls == [] and entry_of(r, chat)["title"] == PLAIN and meta_of(r, chat)["title_by"] == "question"


@pytest.mark.parametrize("value, on, said", [(None, True, False), ("", True, False), ("on", True, False), ("off", False, False), ("On", False, True), ("yes", False, True), ("1", False, True)])
def test_the_switch_is_read_from_the_environment(capsys, value, on, said):
    """Bug class: a missing switch read as off (titles are on by default), or an unknown value read as on."""
    environ = {"SDP_CHAT_BACKEND": "claude-code", "SDP_CHAT_CAP_GIB": "1", "SDP_CHAT_GATEWAY": "10.0.0.2", "SDP_CHAT_MODELS": "opus,sonnet,haiku"}
    if value is not None:
        environ["SDP_CHAT_AUTO_TITLE"] = value
    assert main.settings(environ)["auto_title"] is on
    assert ("SDP_CHAT_AUTO_TITLE" in capsys.readouterr().err) is said


@pytest.mark.parametrize("step", [("raise", RuntimeError("SECRET-DETAIL")), ("raise", OSError("SECRET-DETAIL")), ("answer", 5, None), ("answer", None, None)])
def test_a_failing_back_end_leaves_the_plain_title_and_logs_its_kind_only(chat_runner, capfd, step):
    """Bug class: a failed title call shown in the conversation, crashing the runner, or its message (hostile text) copied to the log."""
    titles = ScriptedTitles(step)
    r = chat_runner(auto_title=True, titles=titles)
    chat = r.new()
    first_question(r, chat)
    settled(r, titles, 1)
    assert entry_of(r, chat)["title"] == PLAIN and meta_of(r, chat)["title_by"] == "question"
    assert titles_of(r.call("chat.events", chat=chat, offset=0, limit=200)["result"]["events"]) == []
    assert "SECRET-DETAIL" not in capfd.readouterr().err


def test_a_title_later_than_the_limit_is_dropped(chat_runner):
    """Bug class: a back end that answers after its limit still changing the title."""
    titles = ScriptedTitles(("sleep", 0.6, "Too late", 0.001))
    r = chat_runner(runner.Limits(title_seconds=0.3), auto_title=True, titles=titles)
    chat = r.new()
    first_question(r, chat)
    settled(r, titles, 1)
    assert titles.calls[0][1] == 0.3
    assert entry_of(r, chat)["title"] == PLAIN and meta_of(r, chat)["title_by"] == "question"


@pytest.mark.parametrize("answer, title", [
    ("x" * 300, "x" * 80),
    ("\n\n  **\"Comments per subreddit\"**  \nA second line", "Comments per subreddit"),
    ("Title\x00with\x1bcontrols\x7f", "Title with controls"),
    ("Title Second line", "Title"),
    ("# Heading title", "Heading title"),
    ("`code` title", "code` title"),
    ("“Typographic quotes”", "Typographic quotes"),
    ("A‮B title", "AB title"),
    ("```\nInside a fence\n```", "Inside a fence"),
    ("Line\r\nOther", "Line"),
    ("  many   inner \t spaces  ", "many inner spaces"),
    ("a\ud800b", None),
    ("", None),
    ("   \n \t\n", None),
    ('""', None),
    ("''  ``  **  ##", None),
    (None, None),
    (5, None),
    (["a title"], None),
])
def test_a_hostile_answer_is_made_one_clean_line_or_dropped(answer, title):
    """Bug class: model output stored as a title with line breaks, markup, control or direction characters, over 80 characters, or text that cannot be written back (a lone surrogate)."""
    assert runner.clean_title(answer, 80) == title


@pytest.mark.parametrize("cost, kept", [(0.0012, 0.0012), (0, 0), (None, None), (float("nan"), None), (float("inf"), None), (-1.0, None), (True, None), ("0.1", None), (10 ** 400, None), (1e9, None)])
def test_a_title_cost_is_a_finite_number_of_at_least_zero_or_null(cost, kept):
    """Bug class: a cost the web UI adds up that is not a number, not finite, negative or absurd."""
    assert runner.clean_cost(cost) == kept


@pytest.mark.parametrize("answer, cost, title, kept", [("x\n" + "y" * 300, float("nan"), "x", None), ("\x01" * 300 + "y" * 300, -2, "y" * 80, None), ("\ud800", 0.1, None, None), ('"""', 0.1, None, None)])
def test_a_hostile_answer_reaches_the_chat_cleaned_or_not_at_all(chat_runner, answer, cost, title, kept):
    """Bug class: the cleaning applied to the check but not to what is stored, or an unwritable answer breaking `meta.json` or the log."""
    titles = ScriptedTitles(("answer", answer, cost))
    r = chat_runner(auto_title=True, titles=titles)
    chat = r.new()
    first_question(r, chat)
    settled(r, titles, 1)
    events = titles_of(r.call("chat.events", chat=chat, offset=0, limit=200)["result"]["events"])
    if title is None:
        assert entry_of(r, chat)["title"] == PLAIN and meta_of(r, chat)["title_by"] == "question" and events == []
    else:
        assert entry_of(r, chat)["title"] == title and meta_of(r, chat) | {"title": title, "title_by": "model"} == meta_of(r, chat)
        assert [(e["text"], e["cost_usd"]) for e in events] == [(title, kept)]


def test_at_most_two_title_jobs_run_and_a_third_is_skipped(chat_runner):
    """Bug class: a burst of new chats starting a title process each, without bound, beside the turns."""
    release = threading.Event()
    titles = ScriptedTitles(("wait", release, "Generated title", None))
    r = chat_runner(auto_title=True, titles=titles)
    chats = [r.new() for _ in range(3)]
    for chat in chats:
        first_question(r, chat)
    wait_for(lambda: len(titles.calls) == 2, what="two title calls")
    time.sleep(0.3)
    assert len(titles.calls) == 2 and r.runner._titling == 2
    release.set()
    settled(r, titles, 2)
    assert titles.most == 2
    assert [entry_of(r, chat)["title"] for chat in chats] == ["Generated title", "Generated title", PLAIN]
    assert meta_of(r, chats[2])["title_by"] == "question"


def test_a_title_job_does_not_count_as_a_turn(chat_runner):
    """Bug class: title jobs taking the sandbox's two turn places, so that a question is refused `too_many` while titles are made."""
    release = threading.Event()
    titles = ScriptedTitles(("wait", release, "Generated title", None))
    r = chat_runner(auto_title=True, titles=titles)
    chats = [r.new() for _ in range(2)]
    for chat in chats:
        first_question(r, chat)
    assert r.runner._running == 0
    r.plan([["sleep", 30], result()])
    assert all(r.call("chat.send", chat=chat, text="next")["ok"] for chat in chats)
    for chat in chats:
        r.call("chat.stop", chat=chat)
        r.wait(chat)
    release.set()
    settled(r, titles, 2)


def test_a_chat_deleted_before_its_title_arrives_gets_nothing_written(chat_runner):
    """Bug class: the job writing a deleted chat's `meta.json` or log again (a chat folder made anew, or written in the renamed-away folder)."""
    release = threading.Event()
    titles = ScriptedTitles(("wait", release, "Generated title", 0.001))
    r = chat_runner(auto_title=True, titles=titles)
    chat = r.new()
    first_question(r, chat)
    assert r.call("chat.delete", chat=chat)["ok"] is True
    release.set()
    settled(r, titles, 1)
    assert not (r.work / chat).exists()
    # The renamed-away folder may be removed by the sweep meanwhile: a file gone while it is read holds no title either.
    for path in r.work.rglob("*"):
        with contextlib.suppress(OSError):
            assert b"Generated title" not in path.read_bytes()


def test_a_title_job_after_the_runner_stopped_writes_nothing(chat_runner):
    """Bug class: a job that ends after `close` writing through the closed work folder's descriptor (whose number may belong to something else by then)."""
    release = threading.Event()
    titles = ScriptedTitles(("wait", release, "Generated title", 0.001))
    r = chat_runner(auto_title=True, titles=titles)
    chat = r.new()
    first_question(r, chat)
    started = time.monotonic()
    r.server.close()
    r.runner.close()
    assert time.monotonic() - started < 5
    release.set()
    wait_for(lambda: r.runner._titling == 0, what="the job ended")
    assert meta_of(r, chat)["title_by"] == "question"


# ── meta.json and chat.list ───────────────────────────────────────────────


@pytest.mark.parametrize("stored, read", [(None, "question"), ("question", "question"), ("model", "model"), ("user", "user"), ("USER", "question"), (5, "question"), (["user"], "question")])
def test_title_by_is_read_from_meta_and_anything_else_is_question(stored, read):
    """Bug class: an older `meta.json` (no `title_by`) or a planted value read so that a later title is never made or a title the operator set is replaced."""
    doc = {"created": 1, "title": "t"} | ({} if stored is None else {"title_by": stored})
    assert runner._checked_meta(json.dumps(doc).encode(), 80)["title_by"] == read


def test_an_old_chat_without_title_by_loads_and_chat_list_keeps_its_shape(chat_runner, tmp_path):
    """Bug class: `title_by` leaking into `chat.list`, whose reply the web UI checks field by field."""
    folder = tmp_path / "work" / "c_0123456789ab"
    folder.mkdir(parents=True)
    (folder / "meta.json").write_text(json.dumps({"created": 1, "title": "old chat", "session": "8d9b2f43-36b5-4d8c-9b4a-2f2b6c1d8e11"}))
    r = chat_runner(auto_title=True, titles=ScriptedTitles())
    assert r.runner._chats["c_0123456789ab"].title_by == "question"
    assert r.call("chat.list")["result"] == {"chats": [{"id": "c_0123456789ab", "title": "old chat", "created": 1, "running": False, "model": None}]}


# ── the back ends' title() ────────────────────────────────────────────────


TITLE_RESULT = {"type": "result", "subtype": "success", "is_error": False, "result": "Made-up title", "total_cost_usd": 0.00042, "num_turns": 1}


def test_claude_title_runs_the_measured_command_in_a_folder_of_its_own(chat_runner, tmp_path):
    """Bug class: the title run given tools, MCP servers, a kept session, the chat's folders, the question on the command line, or an environment of the runner's own."""
    r = chat_runner()
    r.plan([["out", TITLE_RESULT]])
    backend = r.runner.backend
    assert backend.title(QUESTION, 5) == ("Made-up title", 0.00042)
    (record,) = r.records()
    assert record["argv"] == ["-p", "--model", "haiku", "--tools", "", "--strict-mcp-config", "--setting-sources", "", "--settings", claude.SETTINGS, "--disable-slash-commands", "--no-session-persistence", "--effort", "low", "--system-prompt", claude.TITLE_INSTRUCTION, "--output-format", "json"]
    assert record["stdin"] == QUESTION and record["stdin_end"] is True
    env = record["env"]
    folder = os.path.dirname(env["HOME"])
    assert env == claude.environment(env["HOME"], env["CLAUDE_CONFIG_DIR"], env["TMPDIR"], "127.0.0.1")
    assert {os.path.dirname(env[name]) for name in ("HOME", "CLAUDE_CONFIG_DIR", "TMPDIR")} == {folder} and os.path.dirname(record["cwd"]) == folder
    assert os.path.basename(folder).startswith("sdp-title-") and not str(folder).startswith(str(r.work))
    assert not os.path.exists(folder)


@pytest.mark.parametrize("steps, seconds", [
    ([["sleep", 30]], 0.5),
    ([["out", "x" * (70 * 1024)]], 5),
    ([["out", {**TITLE_RESULT, "is_error": True}]], 5),
    ([["out", {**TITLE_RESULT, "result": 5}]], 5),
    ([["out", "not json\n"]], 5),
    ([["out", [1, 2]]], 5),
    ([["exit", 3]], 5),
])
def test_claude_title_fails_on_a_timeout_or_an_answer_it_cannot_use(chat_runner, steps, seconds):
    """Bug class: a hung or flooding title process holding the job (and its place) past its limit, or an error answer taken as the title."""
    r = chat_runner()
    r.plan(steps)
    started = time.monotonic()
    with pytest.raises(claude.TitleFailed):
        r.runner.backend.title(QUESTION, seconds)
    assert time.monotonic() - started < seconds + 3


def test_claude_title_gives_a_cost_that_is_not_a_finite_number_as_none(chat_runner):
    r = chat_runner()
    r.plan([["out", '{"type": "result", "is_error": false, "result": "T", "total_cost_usd": NaN}\n']])
    assert r.runner.backend.title(QUESTION, 5) == ("T", None)


@pytest.fixture
def endpoint(monkeypatch):
    answers = []
    server = standins.serve(standins.Model, answer=lambda doc: answers.pop(0) if answers else (200, reply("Made-up title")))
    monkeypatch.setattr(claude, "MODEL_PORT", server.server_address[1])
    server.answers = answers
    yield server
    standins.stop(server)


def test_openai_title_makes_one_small_request_with_the_configured_model(endpoint):
    """Bug class: the title request sent with tools, streaming, another model (the gateway refuses it) or without a bound on its answer."""
    backend = openai.OpenAICompatible("127.0.0.1", "vendor/model-1:free", ())
    assert backend.title(QUESTION, 5) == ("Made-up title", None)
    (seen,) = endpoint.seen
    assert seen["path"] == "/chat/completions"
    assert seen["doc"] == {"model": "vendor/model-1:free", "messages": [{"role": "system", "content": claude.TITLE_INSTRUCTION}, {"role": "user", "content": QUESTION}], "max_tokens": 30, "stream": False}


@pytest.mark.parametrize("answer", [
    (500, reply("Made-up title")),
    (200, b"not json"),
    (200, {"choices": []}),
    (200, {"choices": [{"message": {"content": None}}]}),
    (200, {"choices": [{"message": "text"}]}),
    (200, b"{" + b" " * (70 * 1024) + b"}"),
])
def test_openai_title_fails_on_an_answer_it_cannot_use(endpoint, answer):
    endpoint.answers.append(answer)
    with pytest.raises(claude.TitleFailed):
        openai.OpenAICompatible("127.0.0.1", "m", ()).title(QUESTION, 5)


def test_openai_title_fails_when_the_endpoint_is_slower_than_the_limit(endpoint):
    endpoint.answer = lambda doc: (time.sleep(2), (200, reply("late")))[1]
    started = time.monotonic()
    with pytest.raises(claude.TitleFailed):
        openai.OpenAICompatible("127.0.0.1", "m", ()).title(QUESTION, 0.5)
    assert time.monotonic() - started < 1.5


# ── a real Claude Code ────────────────────────────────────────────────────


class RecordingApi(BaseHTTPRequestHandler):
    """The model's API as far as one plain answer goes, streamed or not; each request's path and body are kept in `server.seen`."""

    def log_message(self, *args):
        pass

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("content-length", 0)))
        self.server.seen.append((self.path, raw))
        body = json.loads(raw or b"{}")
        message = {"id": "msg_1", "type": "message", "role": "assistant", "model": body.get("model", "m"), "content": [], "stop_reason": None, "stop_sequence": None, "usage": {"input_tokens": 1, "output_tokens": 1}}
        if not body.get("stream"):
            payload = json.dumps({**message, "content": [{"type": "text", "text": "Made-up title"}], "stop_reason": "end_turn"}).encode()
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.end_headers()
        for name, data in [
            ("message_start", {"type": "message_start", "message": message}),
            ("content_block_start", {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}),
            ("content_block_delta", {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "Made-up title"}}),
            ("content_block_stop", {"type": "content_block_stop", "index": 0}),
            ("message_delta", {"type": "message_delta", "delta": {"stop_reason": "end_turn", "stop_sequence": None}, "usage": {"output_tokens": 2}}),
            ("message_stop", {"type": "message_stop"}),
        ]:
            self.wfile.write(f"event: {name}\ndata: {json.dumps(data)}\n\n".encode())


@pytest.mark.skipif(not os.environ.get("SDP_TEST_CLAUDE_BINARY"), reason="needs a real claude binary (SDP_TEST_CLAUDE_BINARY)")
def test_a_real_claude_title_sends_one_small_request_without_tools(monkeypatch, tmp_path):
    """Bug class: the pinned Claude Code refusing the title flags, or sending tools, MCP servers, skills or the long default system prompt with the title request (measured: one request of about 2.2 KB)."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), RecordingApi)
    server.seen = []
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    try:
        monkeypatch.setattr(claude, "MODEL_PORT", server.server_address[1])
        backend = claude.ClaudeCode("127.0.0.1", ("mongo",), queue=True, executable=os.environ["SDP_TEST_CLAUDE_BINARY"])
        text, cost = backend.title(QUESTION, 60)
        assert text == "Made-up title" and isinstance(cost, float)
        (path, raw), = [(p, r) for p, r in server.seen if "count_tokens" not in p]
        body = json.loads(raw)
        assert path.startswith("/v1/messages") and body["model"] == chat_host.CLAUDE_MODEL_IDS[claude.TITLE_MODEL]
        assert body.get("tools") in (None, []) and b"mcp" not in raw.lower() and b"dataviz" not in raw
        system = " ".join(block["text"] for block in body["system"]) if isinstance(body["system"], list) else body["system"]
        assert claude.TITLE_INSTRUCTION in system and len(system.encode()) < 1024
        assert len(raw) < 4096
        assert list(tmp_path.iterdir()) == []
    finally:
        server.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
