"""The runner's `chat.transcript`: Claude Code's own session file of one chat, read-only and in pieces, never through a link and never another chat's.

The place is the one measured with the pinned Claude Code (`claude.project_folder`); `test_a_real_turn_writes_the_transcript_where_the_runner_reads_it` checks it against a real `claude` binary when `SDP_TEST_CLAUDE_BINARY` names one (skipped otherwise, as in CI).
"""

import base64
import json
import os
import shutil
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from social_data_pipeline.chat import claude, openai, runner

LINES = b"".join(json.dumps({"type": "user", "n": index, "text": "x" * 40}).encode() + b"\n" for index in range(50))


def error_of(reply):
    assert reply["ok"] is False and set(reply["error"]) == {"code", "message"}
    return reply["error"]["code"]


def transcript_path(r, chat):
    """The transcript's path of `chat` where Claude Code writes it (measured)."""
    session = r.runner._chats[chat].session
    return r.work / chat / "home" / "projects" / claude.project_folder(str(r.work / chat / "files")) / f"{session}.jsonl"


def plant(path, data=LINES):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def read_all(r, chat, limit):
    parts, offset = [], 0
    while True:
        piece = r.call("chat.transcript", chat=chat, offset=offset, limit=limit)["result"]
        assert set(piece) == {"found", "data", "size", "next", "end"} and piece["found"] is True
        data = base64.b64decode(piece["data"])
        assert len(data) <= limit and piece["next"] == offset + len(data)
        parts.append(data)
        offset = piece["next"]
        if piece["end"]:
            return b"".join(parts), piece["size"]


def test_the_place_is_the_working_folder_with_every_other_character_a_dash():
    """Bug class: the runner looking for the transcript under another name than Claude Code writes (measured: `/` and `_` become `-`)."""
    assert claude.project_folder("/work/c_0123456789ab/files") == "-work-c-0123456789ab-files"
    assert claude.ClaudeCode("127.0.0.1", ()).transcript("/work/c_0123456789ab/files", "s-1") == (("home", "projects"), "-work-c-0123456789ab-files", "s-1.jsonl")


def test_the_transcript_is_returned_in_pieces_that_join_to_the_whole(chat_runner):
    """Bug class: a piece lost, repeated or past the end; a reply over the runner's 1 MiB frame."""
    r = chat_runner()
    chat = r.new()
    plant(transcript_path(r, chat))
    data, size = read_all(r, chat, 1000)
    assert data == LINES and size == len(LINES)
    big = plant(transcript_path(r, chat), b"y" * (runner.Limits().piece_bytes * 2 + 10))
    data, size = read_all(r, chat, runner.Limits().piece_bytes)
    assert data == big.read_bytes() and size == len(data)
    past = r.call("chat.transcript", chat=chat, offset=size + 5, limit=10)["result"]
    assert past == {"found": True, "data": "", "size": size, "next": size + 5, "end": True}


def test_a_chat_without_a_transcript_or_a_back_end_without_one_answers_none(chat_runner, tmp_path):
    """Bug class: a chat with no turn yet, or the OpenAI-compatible back end, answered as an error instead of "none"."""
    r = chat_runner()
    chat = r.new()
    assert r.call("chat.transcript", chat=chat, offset=0, limit=10) == {"ok": True, "result": {"found": False}}
    other = chat_runner(backend=openai.OpenAICompatible("127.0.0.1", "m", ()))
    chat = other.new()
    plant(transcript_path(other, chat))
    assert other.call("chat.transcript", chat=chat, offset=0, limit=10) == {"ok": True, "result": {"found": False}}


@pytest.mark.parametrize("where", ["file", "project", "projects", "home", "hard"])
def test_no_link_is_followed_and_a_hard_link_is_not_read(chat_runner, tmp_path, where):
    """Bug class (security): a link planted by the sandbox in place of the transcript or of a folder on its way, followed to a file outside the chat (its content in a reply)."""
    r = chat_runner()
    chat = r.new()
    path = transcript_path(r, chat)
    outside = tmp_path / "outside"
    plant(outside / "home" / "projects" / path.parent.name / path.name, b"OUTSIDE-MARKER\n")
    target = {"file": (path, outside / "home" / "projects" / path.parent.name / path.name), "project": (path.parent, outside / "home" / "projects" / path.parent.name), "projects": (path.parent.parent, outside / "home" / "projects"), "home": (r.work / chat / "home", outside / "home")}
    if where == "hard":
        path.parent.mkdir(parents=True)
        os.link(outside / "home" / "projects" / path.parent.name / path.name, path)
    else:
        link, to = target[where]
        if link.exists():
            shutil.rmtree(link)
        link.parent.mkdir(parents=True, exist_ok=True)
        link.symlink_to(to)
    reply = r.call("chat.transcript", chat=chat, offset=0, limit=1000)
    assert reply == {"ok": True, "result": {"found": False}}
    assert "OUTSIDE-MARKER" not in json.dumps(reply)


def test_another_chats_transcript_is_never_read(chat_runner):
    """Bug class: one chat's request answered with another chat's session file (the session id taken from anywhere but the chat's own record, or a link across chats followed)."""
    r = chat_runner()
    first, second = r.new(), r.new()
    plant(transcript_path(r, first), b"FIRST-CHAT\n")
    assert r.call("chat.transcript", chat=second, offset=0, limit=100)["result"] == {"found": False}
    # The first chat's file, put under the second chat's folder of projects by its own name: still not the second chat's session.
    plant(r.work / second / "home" / "projects" / transcript_path(r, first).parent.name / transcript_path(r, first).name, b"FIRST-CHAT\n")
    assert r.call("chat.transcript", chat=second, offset=0, limit=100)["result"] == {"found": False}
    (r.work / second / "home" / "projects" / "elsewhere").symlink_to(transcript_path(r, first).parent)
    assert r.call("chat.transcript", chat=second, offset=0, limit=100)["result"] == {"found": False}
    assert base64.b64decode(r.call("chat.transcript", chat=first, offset=0, limit=100)["result"]["data"]) == b"FIRST-CHAT\n"


def test_a_transcript_in_a_folder_of_another_name_is_found(chat_runner):
    """Bug class: a Claude Code version that names the project folder otherwise makes every export lose its transcript."""
    r = chat_runner()
    chat = r.new()
    plant(r.work / chat / "home" / "projects" / "another-naming" / transcript_path(r, chat).name)
    assert read_all(r, chat, 4096)[0] == LINES


@pytest.mark.parametrize("fields", [
    {}, {"offset": 0}, {"offset": 0, "limit": 1, "extra": 1}, {"offset": -1, "limit": 1}, {"offset": 0, "limit": 0},
    {"offset": 0, "limit": 512 * 1024 + 1}, {"offset": True, "limit": 1}, {"offset": 0.5, "limit": 1}, {"offset": 0, "limit": "1"},
])
def test_a_request_of_another_shape_is_bad_request(chat_runner, fields):
    r = chat_runner()
    assert error_of(r.runner.answer({"op": "chat.transcript", "chat": r.new(), **fields})) == "bad_request"


@pytest.mark.parametrize("bad_id", ["c_00000000000", "../c_000000000000", "c_000000000000\n", 5, None])
def test_a_bad_chat_id_is_refused_before_the_file_system_is_touched(chat_runner, monkeypatch, bad_id):
    """Bug class: a chat id used to build a path before it was checked."""
    r = chat_runner()
    touched = []
    for name in ("_open_folder", "_open_regular", "scan"):
        monkeypatch.setattr(runner, name, lambda *a, _n=name, **k: touched.append(_n))
    assert error_of(r.runner.answer({"op": "chat.transcript", "chat": bad_id, "offset": 0, "limit": 1})) == "bad_request"
    assert touched == []
    assert error_of(r.call("chat.transcript", chat="c_0123456789ab", offset=0, limit=1)) == "not_found"


# ── a real Claude Code ────────────────────────────────────────────────────


class StandInApi(BaseHTTPRequestHandler):
    """The model's API as far as one plain answer goes: every message request is answered "made-up answer" (streamed when asked)."""

    def log_message(self, *args):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("content-length", 0))) or b"{}")
        model = body.get("model", "m")
        if "count_tokens" in self.path:
            return self._json({"input_tokens": 1})
        message = {"id": "msg_1", "type": "message", "role": "assistant", "model": model, "content": [], "stop_reason": None, "stop_sequence": None, "usage": {"input_tokens": 1, "output_tokens": 1}}
        if not body.get("stream"):
            return self._json({**message, "content": [{"type": "text", "text": "made-up answer"}], "stop_reason": "end_turn"})
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.end_headers()
        for name, data in [
            ("message_start", {"type": "message_start", "message": message}),
            ("content_block_start", {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}),
            ("content_block_delta", {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "made-up answer"}}),
            ("content_block_stop", {"type": "content_block_stop", "index": 0}),
            ("message_delta", {"type": "message_delta", "delta": {"stop_reason": "end_turn", "stop_sequence": None}, "usage": {"output_tokens": 2}}),
            ("message_stop", {"type": "message_stop"}),
        ]:
            self.wfile.write(f"event: {name}\ndata: {json.dumps(data)}\n\n".encode())

    def _json(self, document):
        payload = json.dumps(document).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


@pytest.mark.skipif(not os.environ.get("SDP_TEST_CLAUDE_BINARY"), reason="needs a real claude binary (SDP_TEST_CLAUDE_BINARY)")
def test_a_real_turn_writes_the_transcript_where_the_runner_reads_it(chat_runner, monkeypatch):
    """Bug class: the measured place of the transcript no longer the one the pinned Claude Code writes (two turns: the second resumes and appends to the same file)."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), StandInApi)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        monkeypatch.setattr(claude, "MODEL_PORT", server.server_address[1])
        backend = claude.ClaudeCode("127.0.0.1", (), executable=os.environ["SDP_TEST_CLAUDE_BINARY"])
        r = chat_runner(backend=backend, databases=())
        chat = r.new()
        for question in ("first made-up question", "second made-up question"):
            assert r.call("chat.send", chat=chat, text=question)["ok"] is True
            events = r.wait(chat, seconds=120)
            assert events[-1]["type"] == "end" and events[-1]["is_error"] is False, events
            time.sleep(0.2)
        assert transcript_path(r, chat).is_file()
        data, _size = read_all(r, chat, 64 * 1024)
        lines = [json.loads(line) for line in data.splitlines()]
        asked = [line["message"]["content"] for line in lines if line.get("type") == "user" and isinstance(line.get("message"), dict)]
        assert asked == ["first made-up question", "second made-up question"]
    finally:
        server.shutdown()
