"""The export of a whole chat (`GET /chat/<id>/export`, `webui.chat_export`), through the whole application with the stand-in broker and the scripted stand-in of the runner (`webui_helpers.ScriptedRunner`), which can answer anything.

Each test names the bug class it catches: events lost or reordered across the runner's pages (which can be shorter than asked), the transcript's lines lost at the borders of its pieces, a bound that cuts the document into invalid JSON, hostile text of the runner reaching anything but JSON data, a session the broker ended still exporting, the badge on a page with no chat.

Every special character in this file is written as an escape.
"""

from __future__ import annotations

import base64
import html
import json
import re
import threading
import time

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette.testclient")

from webui_helpers import CHAT_ID, Raw, an_event, log_in, refuse

from social_data_pipeline.webui import assistant, chat_export, words

HOSTILE = "<script>alert('x')</script>\"&\u2028"
EXPORT = f"/chat/{CHAT_ID}/export"
TRANSCRIPT = b'{"type":"user","message":{"content":"first"}}\n' + b"not json {\n" + b'{"type":"assistant","n":2}\n' + b"\xff\xfe broken utf-8\n" + b'{"type":"last","no":"line end"}'
TRANSCRIPT_VALUES = [{"type": "user", "message": {"content": "first"}}, "not json {", {"type": "assistant", "n": 2}, "\ufffd\ufffd broken utf-8", {"type": "last", "no": "line end"}]


@pytest.fixture
def exporting(make_app, make_client, scripted):
    """`exporting(**limits)`: a logged-in client of an application with those limits, on the scripted runner."""

    def make(**limits):
        made = make_client(make_app(assistant=assistant.Assistant(scripted.path), **limits))
        assert log_in(made).status_code == 303
        return made

    return make


def events_of(count, text="made-up answer"):
    return [an_event("text", n, text=f"{text} {n}") for n in range(count)]


def short_pages(scripted, most):
    """The runner's pages cut at `most` events, as its byte bound cuts them: shorter than asked while more follow."""
    def answer(request):
        first = min(request["offset"], len(scripted.events))
        listed = scripted.events[first:first + min(request["limit"], most)]
        return {"events": listed, "next": first + len(listed), "running": False}

    scripted.answers["chat.events"] = answer


def exported(response):
    assert response.status_code == 200
    return json.loads(response.content)


def test_every_event_in_order_across_short_pages_and_every_line_of_the_transcript(exporting, scripted):
    """Bug class: an export that ends at the first page shorter than asked (the runner cuts pages by bytes), or loses a line split between two pieces of the transcript."""
    client = exporting()
    scripted.events = events_of(450)
    short_pages(scripted, 7)
    scripted.transcript = TRANSCRIPT
    scripted.transcript_piece = 5
    response = client.get(EXPORT)
    document = exported(response)
    assert list(document) == ["format", "exported_at", "chat", "events", "backend_transcript", "stopped"]
    assert document["format"] == 1 and re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", document["exported_at"])
    assert document["chat"] == {"id": CHAT_ID, "title": "made-up question", "model": None, "backend": "claude-code", "created": 1795000000.0}
    assert document["events"] == scripted.events
    assert document["backend_transcript"] == TRANSCRIPT_VALUES and document["stopped"] is None
    offsets = [request["offset"] for request in scripted.requests if request["op"] == "chat.transcript"]
    assert offsets == list(range(0, len(TRANSCRIPT), 5))


def test_the_download_is_a_sandboxed_json_attachment_named_by_the_chat(exporting, scripted):
    """Bug class: the export served inline as a page of the web UI's origin, or under a name the runner chose."""
    response = exporting().get(EXPORT)
    assert response.status_code == 200
    assert response.headers["content-disposition"] == f'attachment; filename="chat-{CHAT_ID}.json"'
    assert response.headers.get_list("content-security-policy")[0] == "sandbox" and response.headers["content-type"].startswith("application/json")
    assert response.headers["x-content-type-options"] == "nosniff" and "content-length" not in response.headers
    assert exported(response) | {"exported_at": None} == {"format": 1, "exported_at": None, "chat": {"id": CHAT_ID, "title": "made-up question", "model": None, "backend": "claude-code", "created": 1795000000.0}, "events": [], "backend_transcript": None, "stopped": None}


def test_the_broker_is_asked_every_time_and_head_is_refused(exporting, scripted, broker):
    """Bug class: an export let through on an earlier answer of the broker; a HEAD that reads the chat."""
    client = exporting()

    def checks():
        return len([request for request in broker.requests if request["verb"] == "audit.list"])

    before = checks()
    for _ in range(3):
        assert client.get(EXPORT).status_code == 200
    assert checks() == before + 3
    asked = len(scripted.requests)
    assert client.head(EXPORT).status_code == 405 and len(scripted.requests) == asked


@pytest.mark.parametrize("path", ["/chat/c_0123/export", "/chat/C_0123456789AB/export", "/chat/..%2Fc_0123456789ab/export"])
def test_a_malformed_id_is_not_found_and_an_unlisted_chat_too(exporting, scripted, path):
    client = exporting()
    assert client.get(path).status_code == 404 and scripted.requests == []
    scripted.chats = []
    assert client.get(EXPORT).status_code == 404 and "chat.events" not in scripted.ops()


@pytest.mark.parametrize("limits, events, sentence", [
    ({"chat_export_events": 10}, 25, chat_export.STOPPED_EVENTS.format(count=10)),
    ({"chat_export_bytes": chat_export.TAIL_RESERVE + 3000}, 400, chat_export.STOPPED_BYTES.format(size=f"{(chat_export.TAIL_RESERVE + 3000) / (1 << 20):g} MiB")),
])
def test_a_bound_stops_the_events_says_so_and_the_json_stays_valid(exporting, scripted, limits, events, sentence):
    """Bug class: an export without a bound (memory, a 16 MiB log of tiny events), or one cut into JSON no tool can read."""
    client = exporting(**limits)
    scripted.events = events_of(events)
    scripted.transcript = TRANSCRIPT
    response = client.get(EXPORT)
    document = exported(response)
    assert document["stopped"] == sentence and document["backend_transcript"] is None
    assert document["events"] == scripted.events[:len(document["events"])] and 0 < len(document["events"]) < events
    assert len(response.content) <= limits.get("chat_export_bytes", 1 << 30)
    assert "chat.transcript" not in scripted.ops()


def test_exactly_the_most_events_is_not_stopped(exporting, scripted):
    client = exporting(chat_export_events=10)
    scripted.events = events_of(10)
    document = exported(client.get(EXPORT))
    assert len(document["events"]) == 10 and document["stopped"] is None


def test_the_byte_bound_counts_the_transcript_too(exporting, scripted):
    client = exporting(chat_export_bytes=chat_export.TAIL_RESERVE + 2000)
    scripted.events = events_of(3)
    scripted.transcript = b"".join(json.dumps({"line": n, "pad": "p" * 50}).encode() + b"\n" for n in range(100))
    response = client.get(EXPORT)
    document = exported(response)
    assert document["events"] == scripted.events and 0 < len(document["backend_transcript"]) < 100
    assert document["stopped"].startswith("The export reached") and len(response.content) <= chat_export.TAIL_RESERVE + 2000


def test_the_time_limit_stops_it(exporting, scripted, clock):
    """Bug class: a runner that answers slowly forever holding a download's place without end."""
    client = exporting(chat_export_seconds=60)
    scripted.events = events_of(1000)
    short_pages(scripted, 1)
    default = scripted.answers["chat.events"]

    def slow(request):
        clock.now += 7
        return default(request)

    scripted.answers["chat.events"] = slow
    document = exported(client.get(EXPORT))
    assert document["stopped"] == chat_export.STOPPED_TIME.format(minutes=1) and 0 < len(document["events"]) < 20


def test_a_line_longer_than_its_bound_stops_the_transcript(exporting, scripted, monkeypatch):
    """Bug class: a transcript without line ends held whole in memory."""
    monkeypatch.setattr(chat_export, "LINE_MAX_BYTES", 100)
    client = exporting()
    scripted.transcript = b'{"a":1}\n' + b"z" * 500
    scripted.transcript_piece = 64
    document = exported(client.get(EXPORT))
    assert document["backend_transcript"] == [{"a": 1}] and document["stopped"] == chat_export.STOPPED_LINE


def test_a_failure_of_the_runner_keeps_what_was_read_and_says_why(exporting, scripted):
    """Bug class: a runner failing half-way leaving a truncated, invalid document, or an error page after the download began."""
    client = exporting()
    scripted.events = events_of(5)
    scripted.transcript = TRANSCRIPT
    scripted.transcript_piece = 50
    default = scripted._default
    scripted.answers["chat.transcript"] = lambda request: default("chat.transcript", request) if request["offset"] == 0 else refuse("internal")
    document = exported(client.get(EXPORT))
    assert document["events"] == scripted.events and document["backend_transcript"] == TRANSCRIPT_VALUES[:1]
    assert document["stopped"] == chat_export.TRANSCRIPT_FAILED.format(sentence=words.CHAT_CODE_SENTENCES["internal"])
    scripted.answers["chat.events"] = lambda request: {"events": [an_event("text", 0, text="x")], "next": request["offset"] + 5, "running": False}
    document = exported(client.get(EXPORT))
    assert document["events"] == [] and document["backend_transcript"] is None
    assert document["stopped"] == chat_export.EVENTS_FAILED.format(sentence=words.CHAT_CODE_SENTENCES["not_understood"])


@pytest.mark.parametrize("reply", [
    {"found": False, "extra": 1}, {"found": "no"}, {"found": True}, {"found": True, "data": "@@", "size": 2, "next": 2, "end": True},
    {"found": True, "data": base64.b64encode(b"ab").decode(), "size": 2, "next": 5, "end": True},
    {"found": True, "data": base64.b64encode(b"ab").decode(), "size": 9, "next": 2, "end": True},
    {"found": True, "data": base64.b64encode(b"ab").decode(), "size": 2, "next": 2, "end": True, "more": 1},
])
def test_a_transcript_reply_of_another_shape_is_not_used(exporting, scripted, reply):
    """Bug class: a piece of another shape (a next that jumps, an end that is not the end) used anyway."""
    client = exporting()
    scripted.answers["chat.transcript"] = reply
    document = exported(client.get(EXPORT))
    assert document["backend_transcript"] is None
    assert document["stopped"] == chat_export.TRANSCRIPT_FAILED.format(sentence=words.CHAT_CODE_SENTENCES["not_understood"])


def test_an_older_runner_without_the_op_exports_the_events_and_says_so(exporting, scripted):
    client = exporting()
    scripted.events = events_of(2)
    scripted.answers["chat.transcript"] = refuse("bad_request")
    document = exported(client.get(EXPORT))
    assert document["events"] == scripted.events and document["backend_transcript"] is None
    assert document["stopped"] == chat_export.TRANSCRIPT_FAILED.format(sentence=words.CHAT_CODE_SENTENCES["bad_request"])


def test_hostile_text_is_json_data_and_nothing_else(exporting, scripted):
    """Bug class (security): text of the runner (an event, a title, a transcript line, a lone surrogate) reaching the download as anything but JSON strings, or making the document invalid."""
    client = exporting()
    scripted.chats = [{"id": CHAT_ID, "title": HOSTILE[:80], "created": 1795000000.0, "running": False}]
    scripted.events = [an_event("text", 0, text=HOSTILE), an_event("tool", 1, name=HOSTILE, input=HOSTILE), {"n": 2, "weird": [HOSTILE, {"x": 10 ** 30}]}]
    scripted.transcript = json.dumps({"t": HOSTILE}).encode() + b"\n" + HOSTILE.encode() + b"\n"
    response = client.get(EXPORT)
    document = exported(response)
    assert document["chat"]["title"] == HOSTILE[:80] and document["events"] == scripted.events
    assert document["backend_transcript"] == [{"t": HOSTILE}, HOSTILE]
    assert b"<html" not in response.content.lower() and response.headers["content-type"].startswith("application/json")
    scripted.answers["chat.events"] = lambda request: Raw('{"ok": true, "result": {"events": [{"n": 0, "type": "text", "text": "a\\ud800b"}], "next": %d, "running": false}}' % (request["offset"] + 1)) if request["offset"] == 0 else {"events": [], "next": request["offset"], "running": False}
    document = exported(client.get(EXPORT))
    assert document["events"] == [{"n": 0, "type": "text", "text": "a\ud800b"}]


def test_an_export_holds_a_place_of_the_downloads(exporting, scripted):
    """Bug class: exports without a bound, each holding a worker and the runner's connections for minutes."""
    client = exporting(chat_download_workers=1)
    held = threading.Event()
    scripted.files["a.png"] = b"made-up"
    scripted.answers["chat.events"] = lambda request: (held.wait(10), {"events": [], "next": request["offset"], "running": False})[1]
    results = []
    worker = threading.Thread(target=lambda: results.append(client.get(EXPORT)))
    worker.start()
    try:
        deadline = time.monotonic() + 10
        while "chat.events" not in scripted.ops() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert client.get(f"/chat/{CHAT_ID}/file/a.png").status_code == 503
        assert client.get(EXPORT).text == "The web UI already sends two of the chat's files or exports: try again when one has ended.\n"
    finally:
        held.set()
        worker.join(10)
    assert results[0].status_code == 200 and client.get(f"/chat/{CHAT_ID}/file/a.png").status_code == 200


def test_the_export_link_is_in_this_chat_and_not_on_the_list_page(exporting, scripted):
    """Bug class: an Export link on the page with no chat open (it would export nothing), missing from the right column "This chat", or not after the turn totals."""
    client = exporting()
    page = client.get(f"/chat/{CHAT_ID}").text
    aside = re.search(r'<aside class="rightp"[^>]*>(.*?)</aside>', page, re.DOTALL).group(1)
    link = re.search(rf'<a class="btn sm" href="/chat/{CHAT_ID}/export" download[^>]*>Export</a>', aside)
    assert link and aside.index('id="chat-totals"') < link.start() and page.count("/export") == 1
    assert "/export" not in client.get("/chat").text
    scripted.chats = []
    assert "/export" not in html.unescape(client.get(f"/chat/{CHAT_ID}").text)


def test_an_unreachable_runner_after_the_broker_is_a_sentence_not_a_document(exporting, scripted):
    client = exporting()
    scripted.answers["chat.list"] = refuse("internal")
    response = client.get(EXPORT)
    assert response.status_code == 503 and response.text == words.CHAT_CODE_SENTENCES["internal"] + "\n"
