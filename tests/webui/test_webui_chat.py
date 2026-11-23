"""Tests for the Chat window (`webui.chat`, `webui.assistant`), through the whole application with the stand-in broker and two kinds of runner: the scripted stand-in on a real unix socket (`webui_helpers.ScriptedRunner`), which can answer anything, and where fidelity matters the REAL `chat.runner` with the stand-in `claude` of `tests/chat/chat_helpers.py`.

The contracts pinned here, each test naming the bug class it catches:

- a. Reachability: no setting, a socket folder or socket that fails its check (injected `lstat`), no socket, a socket nobody listens on: each its sentence and nothing sent; every refusal code of the runner has a sentence and is shown with it.
- b. The session: every Chat route (GET and POST) of a session the broker ended leads to the login and the runner hears nothing; an unreachable broker: no runner call; a download and an image never use the broker's earlier yes.
- c. Hostile replies: a `hello` with a `queue` that is not a flag (and the shape without the field, of a runner from before it, taken as without the queue), wrong types, missing and extra fields, ids of another shape, over-long lists and strings, a `next` that goes back, deep nesting, NaN, huge numbers, lone surrogates, unknown events: the sentence or the line, and nothing of the reply in the page.
- d. Hostile text in well-formed replies is text in element and attribute positions, invisible characters as escapes; a file name outside the runner's rule gets no link and no image.
- f. The events part: paging by offset, the poll only while a turn runs, `HX-Redirect` for an ended session, an offset that is not a number refused; at most 2,000 events per view, then a link to continue (`from`, checked).
- Bounds against a hostile runner: a reply over 1 MiB refused unread, one of more than 20,000 objects and lists unparsed, the largest honest replies passing; a page of events over 512 KiB of text refused, the units of a page's model capped (runs, blocks and items; later events a placeholder whose first links to a view starting at it, where an event too long alone is shown cut), a huge or odd number in every numeric field of every reply never an error of the page, downloads and images ending at short pieces and at their total time.
- g. New, send, stop, delete: the exact requests; refused texts keep the text; the refusals' sentences; the delete page changes nothing; the guard refuses a POST without the form token or from another origin.
- h. Files and images: the fixed headers and the reduced name; the image's type from its signature, anything else not found; a file that changes size; the size cap; two downloads at a time; no file opened by the web UI.
- k. The plot card's buttons and the table view: a button only for a file of the chat's own list with the image's stem; a CSV's cells text, its rows, columns and bytes capped, a file that is not CSV a sentence.
- m. Chart specs: the card draws a `.vl.json` of the chat's list once (in the image's card, or its own when no image has its stem) and only then; the spec route sends the checked spec as JSON with the fixed headers, a refused one its reason, reads at most 2 MiB, never on the broker's earlier yes, and only for a `.vl.json` name of the runner's rule.
- n. Files in subfolders: listed by their path with links that keep the `/`, the note of the files left out by type, every file route taking a path, and an encoded `..`, an empty part or a too deep path not found with nothing sent.
- l. The right column's queries: the broker asked before the runner and the scheduler; the ids only from this chat's tool calls and results, newest first, at most 20; only the assistant's jobs; a failure confined to its row.
- j. The scans: no inline code in any page or part of the window, the guard's headers on every answer, HEAD refused, the navigation's first entry.
- The real runner: a turn's events, its file downloaded byte for byte.

Every special character in this file is written as an escape.
"""

from __future__ import annotations

import ast
import html
import html.parser
import importlib.util
import json
import re
import sys
import threading
import time
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette.testclient")

from test_webui_pages import inline_code
from webui_helpers import CHAT_ID, GOOD_FOLDER, ORIGIN, Raw, a_job, an_event, form_token, log_in, main_html, refuse, wait_until

from social_data_pipeline.control import client as broker_client
from social_data_pipeline.webui import assistant, chat, queue, words

HOSTILE = "<script>alert('x')</script>\"&"
RAW_MARKS = ("<script>", "alert('x')", "</script>\"")
MARK = "made-up-hostile-mark-71f2"
HX = {"hx-request": "true"}
CSP = "default-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'; object-src 'none'"
PNG = b"\x89PNG\r\n\x1a\n" + b"made-up png body"
JPEG = b"\xff\xd8\xff\xe0" + b"made-up jpeg body"
NOT_UNDERSTOOD = words.CHAT_CODE_SENTENCES["not_understood"]
NO_REPLY = words.CHAT_REASON_SENTENCES[broker_client.NO_REPLY]
RUNNER = Path(__file__).resolve().parents[2] / "social_data_pipeline" / "chat" / "runner.py"


@pytest.fixture
def app(make_app, scripted):
    return make_app(assistant=assistant.Assistant(scripted.path))


@pytest.fixture
def client(app, make_client):
    made = make_client(app)
    assert log_in(made).status_code == 303
    return made


@pytest.fixture
def token(client):
    return form_token(client)


def post(client, path, token, **fields):
    return client.post(path, data={"form_token": token, **fields}, headers={"origin": ORIGIN})


def main_text(page):
    found = re.search(r"<main[^>]*>(.*)</main>", page, re.DOTALL)
    return " ".join(html.unescape(re.sub(r"<[^>]+>", " ", found.group(1) if found else page)).split())


def attributes(page):
    return [html.unescape(value) for tag in re.findall(r"<[a-zA-Z][^>]*>", page) for value in re.findall(r'\s[a-zA-Z][\w:-]*="([^"]*)"', tag)]


# ── a. reachability ─────────────────────────────────────────────────────────


def a_stat(kind, mode, uid=1000):
    import os
    import stat

    return os.stat_result(({"dir": stat.S_IFDIR, "sock": stat.S_IFSOCK, "file": stat.S_IFREG, "link": stat.S_IFLNK}[kind] | mode, 0, 0, 1, uid, 1000, 0, 0, 0, 0))


@pytest.mark.parametrize("folder, sock, reason", [
    (FileNotFoundError(), None, "folder_missing"),
    (a_stat("file", 0o600), None, "folder_not_a_folder"),
    (a_stat("link", 0o777), None, "folder_not_a_folder"),
    (a_stat("dir", 0o700, uid=1001), None, "folder_other_owner"),
    (a_stat("dir", 0o750), None, "folder_open"),
    (GOOD_FOLDER, FileNotFoundError(), broker_client.NO_SOCKET),
    (GOOD_FOLDER, a_stat("file", 0o600), "socket_not_a_socket"),
    (GOOD_FOLDER, a_stat("link", 0o777), "socket_not_a_socket"),
    (GOOD_FOLDER, a_stat("sock", 0o600, uid=1001), "socket_other_owner"),
    (GOOD_FOLDER, a_stat("sock", 0o660), "socket_open"),
])
def test_a_folder_or_socket_that_fails_its_check_is_not_used(make_app, make_client, scripted, folder, sock, reason):
    """Bug class: a socket the sandbox replaced (a link to another socket, one of another user, one in a folder others can enter) asked anyway, or every such case shown with one vague line."""

    def lstat(path):
        found = folder if not path.endswith("runner.sock") else sock
        if isinstance(found, BaseException):
            raise found
        return found

    made = make_client(make_app(assistant=assistant.Assistant(scripted.path, lstat=lstat, uid=lambda: 1000)))
    log_in(made)
    assert words.CHAT_REASON_SENTENCES[reason] in main_text(made.get("/chat").text)
    assert scripted.requests == [] and scripted.opened == 0


def test_without_the_setting_or_the_socket_the_page_says_what_to_do(make_app, make_client, chat_folder):
    """Bug class: a Chat page that breaks or says nothing useful when the chat is not set up, not started, or its socket is dead."""
    import os
    import socket

    made = make_client(make_app())
    log_in(made)
    page = main_text(made.get("/chat").text)
    assert words.CHAT_REASON_SENTENCES["no_settings"] in page and "python sdp.py chat setup" in page
    path = os.path.join(chat_folder, "runner.sock")
    made = make_client(make_app(assistant=assistant.Assistant(path)))
    log_in(made)
    assert words.CHAT_REASON_SENTENCES[broker_client.NO_SOCKET] in main_text(made.get("/chat").text)
    dead = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    dead.bind(path)
    dead.close()
    os.chmod(path, 0o600)
    assert words.CHAT_REASON_SENTENCES[broker_client.REFUSED] in main_text(made.get("/chat").text)


def runner_codes():
    tree = ast.parse(RUNNER.read_text())
    found = next(node.value for node in tree.body if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", None) == "MESSAGES")
    return [key.value for key in found.keys]


def test_every_refusal_code_of_the_runner_has_a_sentence_and_shows_it(client, token, scripted):
    """Bug class: a refusal of the runner shown as a vague fallback, or its message (text the sandbox can choose) shown in the page."""
    codes = runner_codes()
    assert len(codes) >= 13 and set(codes) <= set(words.CHAT_CODE_SENTENCES)
    for code in codes:
        scripted.answers["chat.new"] = {"ok": False, "error": {"code": code, "message": MARK}}
        page = post(client, "/chat/new", token)
        assert words.CHAT_CODE_SENTENCES[code] in main_text(page.text) and MARK not in page.text, code
    for odd in (["x"], 7, None, "made-up-code"):
        scripted.answers["chat.new"] = {"ok": False, "error": {"code": odd, "message": MARK}}
        page = post(client, "/chat/new", token)
        assert words.CHAT_FALLBACK in main_text(page.text) and MARK not in page.text


# ── b. the session ──────────────────────────────────────────────────────────

ROUTES = [
    ("GET", "/chat"), ("GET", f"/chat/{CHAT_ID}"), ("GET", f"/chat/{CHAT_ID}/events?offset=0"), ("GET", f"/chat/{CHAT_ID}/delete"),
    ("GET", f"/chat/{CHAT_ID}/file/a.png"), ("GET", f"/chat/{CHAT_ID}/image/a.png"), ("GET", f"/chat/{CHAT_ID}/table/a.csv"), ("GET", f"/chat/{CHAT_ID}/queries"),
    ("GET", f"/chat/{CHAT_ID}/spec/a.vl.json"), ("GET", f"/chat/{CHAT_ID}/export"),
    ("POST", "/chat/new"), ("POST", f"/chat/{CHAT_ID}/send"), ("POST", f"/chat/{CHAT_ID}/stop"), ("POST", f"/chat/{CHAT_ID}/delete"),
    ("POST", f"/chat/{CHAT_ID}/title"),
]


def request_route(client, token, method, path, headers=None):
    if method == "GET":
        return client.get(path, headers=headers or {})
    return post(client, path, token, **({"text": "a question"} if path.endswith("/send") else {"title": "a title"} if path.endswith("/title") else {}))


@pytest.mark.parametrize("method, path", ROUTES)
def test_a_session_the_broker_ended_reaches_nothing_of_the_chat(client, token, scripted, broker, app, clock, method, path):
    """Bug class: a browser session whose broker session has ended (the management password changed) still reading the chats or sending questions through the web UI's own session table."""
    scripted.files["a.png"] = PNG
    clock.now += queue.SESSION_CHECK_SECONDS + 1
    broker.answers["audit.list"] = broker_client.BrokerError("session_ended", "x")
    part = "/events" in path
    response = request_route(client, token, method, path, HX if part else None)
    if part:
        assert response.status_code == 204 and response.headers["hx-redirect"] == "/login?why=ended"
    else:
        assert response.status_code == 303 and response.headers["location"] == "/login?why=ended"
    assert scripted.requests == []


@pytest.mark.parametrize("method, path", ROUTES)
def test_an_unreachable_broker_stops_every_route_before_the_runner(client, token, scripted, broker, clock, method, path):
    """Bug class: the runner asked without the broker having said that the session lives, because the broker could not be asked."""
    clock.now += queue.SESSION_CHECK_SECONDS + 1
    broker.answers["audit.list"] = broker_client.BrokerUnreachable(broker_client.NO_SOCKET)
    response = request_route(client, token, method, path)
    assert words.REASON_SENTENCES[broker_client.NO_SOCKET] in html.unescape(response.text) and scripted.requests == []


def test_pages_believe_the_brokers_yes_five_seconds_and_files_never(client, scripted, broker):
    """Bug class: a download or an image let through on an earlier answer of the broker."""
    scripted.files["a.png"] = PNG

    def checks():
        return len([request for request in broker.requests if request["verb"] == "audit.list"])

    client.get("/chat")
    before = checks()
    client.get(f"/chat/{CHAT_ID}")
    assert checks() == before
    for route in ("file", "image", "file"):
        assert client.get(f"/chat/{CHAT_ID}/{route}/a.png").status_code == 200
    assert checks() == before + 3


# ── c. hostile replies ──────────────────────────────────────────────────────

GOOD_CHAT = {"id": CHAT_ID, "title": "fine", "created": 1795000000.0, "running": False}
LIST_REPLIES = [
    {"chats": MARK},
    {"chats": [{**GOOD_CHAT, "id": "c_" + MARK}]},
    {"chats": [{**GOOD_CHAT, "title": MARK * 4}]},
    {"chats": [{**GOOD_CHAT, "running": MARK}]},
    {"chats": [{key: value for key, value in GOOD_CHAT.items() if key != "running"} | {"title": MARK}]},
    {"chats": [{**GOOD_CHAT, "title": MARK, "extra": 1}]},
    {"chats": [GOOD_CHAT], "extra": MARK},
    {"chats": [{**GOOD_CHAT, "title": MARK, "created": 10 ** 400}]},
    {"chats": [{**GOOD_CHAT, "title": MARK, "created": True}]},
    {"chats": [{**GOOD_CHAT, "id": f"c_{index:012x}", "title": MARK} for index in range(201)]},
    {"chats": [GOOD_CHAT, {**GOOD_CHAT, "title": MARK}]},
    Raw('{"ok": true, "result": {"chats": [{"id": "%s", "title": "%s\\ud800", "created": 1, "running": false}]}}' % (CHAT_ID, MARK)),
]


@pytest.mark.parametrize("reply", LIST_REPLIES)
def test_a_list_of_another_shape_is_not_shown(client, scripted, reply):
    """Bug class: a list of chats of the wrong types, lengths, ids, counts or encodings shown in part, or breaking the page."""
    scripted.answers["chat.list"] = reply
    page = client.get("/chat")
    assert page.status_code == 200 and NOT_UNDERSTOOD in main_text(page.text) and MARK not in page.text


@pytest.mark.parametrize("payload", [
    '{"ok": true, "result": {"chats": [], "x": NaN, "m": "%s"}}' % MARK,
    "[" * 40 + "]" * 40,
    '{"ok": true, "result": ' + "[" * 40 + '"%s"' % MARK + "]" * 40 + "}",
    "%s not json" % MARK,
])
def test_an_answer_that_is_not_readable_json_is_not_shown(client, scripted, payload):
    """Bug class: a frame the framing refuses (a NaN, a nesting deeper than its limit, not JSON) shown in part or answered with a traceback."""
    scripted.answers["chat.list"] = Raw(payload)
    page = client.get("/chat")
    assert page.status_code == 200 and NO_REPLY in main_text(page.text) and MARK not in page.text


@pytest.mark.parametrize("hello", [
    {"version": 1, "backend": MARK.upper(), "databases": []},
    {"version": 1, "backend": "claude-code", "databases": [MARK.upper()]},
    {"version": 1, "backend": "claude-code", "databases": ["mongo"] * 17 + [MARK]},
    {"version": True, "backend": "claude-code", "databases": [], "m": MARK},
])
def test_a_hello_of_another_shape_is_not_shown(client, scripted, hello):
    """Bug class: the back end's name or the databases taken from an answer of the wrong shape."""
    scripted.answers["hello"] = hello
    page = client.get("/chat")
    assert NOT_UNDERSTOOD in main_text(page.text) and MARK not in page.text.upper()


@pytest.mark.parametrize("hello", [
    {"version": 1, "backend": "claude-code", "databases": ["mongo"], "queue": "true", "m": MARK},
    {"version": 1, "backend": "claude-code", "databases": ["mongo"], "queue": 1, "m": MARK},
    {"version": 1, "backend": "claude-code", "databases": ["mongo"], "queue": None, "m": MARK},
    {"version": 1, "backend": "claude-code", "databases": ["mongo"], "queue": [True], "m": MARK},
])
def test_a_hello_whose_queue_is_not_a_flag_is_not_understood(client, scripted, hello):
    """Bug class: a hostile runner's `queue` taken as true by its truthiness (a string, a number), so that the page says the assistant can submit to the queue."""
    scripted.answers["hello"] = hello
    text = main_text(client.get("/chat").text)
    assert NOT_UNDERSTOOD in text and chat.QUEUE_ON not in text and chat.QUEUE_OFF not in text


@pytest.mark.parametrize("hello, line", [
    ({"version": 1, "backend": "claude-code", "databases": ["mongo"], "queue": True}, "on"),
    ({"version": 1, "backend": "claude-code", "databases": ["mongo"], "queue": False}, "off"),
    # The shape of a runner from before the field: a sandbox image older than the web UI is read as without the queue.
    ({"version": 1, "backend": "claude-code", "databases": ["mongo"]}, "off"),
])
def test_the_page_says_whether_the_assistant_can_submit_to_the_queue(client, scripted, hello, line):
    """Bug class: the page saying the assistant can submit to the queue when it cannot (or the reverse), or a web UI refusing the hello of a sandbox image of the other version."""
    scripted.answers["hello"] = hello
    text = main_text(client.get("/chat").text)
    assert NOT_UNDERSTOOD not in text and "MongoDB: checking" in text
    assert (chat.QUEUE_ON in text) is (line == "on") and (chat.QUEUE_OFF in text) is (line == "off")
    assert assistant.Assistant(scripted.path).hello() == {"backend": "claude-code", "databases": ["mongo"], "queue": line == "on", "models": [], "model_default": None}


@pytest.mark.parametrize("reply", [
    lambda request: {"events": [], "next": request["offset"] - 1, "running": False, "m": MARK},
    lambda request: {"events": [], "next": request["offset"] + 1, "running": False},
    lambda request: {"events": [an_event("text", text=MARK)] * (request["limit"] + 1), "next": request["offset"] + request["limit"] + 1, "running": False},
    lambda request: {"events": MARK, "next": 0, "running": False},
    lambda request: {"events": [an_event("text", text=MARK)], "next": 1, "running": "yes"},
    lambda request: {"events": [an_event("text", text=MARK)], "next": 10 ** 30, "running": False},
])
def test_events_of_another_shape_are_not_shown(client, scripted, reply):
    """Bug class: a page of events whose offset goes back or jumps (the poll would repeat or skip events, or never end), whose count is beyond the limit asked, or whose flags are not flags, shown in part."""
    scripted.answers["chat.events"] = reply
    page = client.get(f"/chat/{CHAT_ID}")
    assert NOT_UNDERSTOOD in main_text(page.text) and MARK not in page.text and "/events" not in main_html(page.text)


EVENT_VARIANTS = [
    an_event("made_up_type", text=MARK),
    an_event("text", text=MARK, extra=1),
    {"n": 0, "type": "text", "text": MARK},
    an_event("text", n=True, text=MARK),
    an_event("text", text=[MARK]),
    an_event("tool", name="Bash", input={"command": MARK}),
    an_event("tool_result", text=MARK, is_error="no"),
    an_event("file", name=MARK, size=-1),
    an_event("end", is_error=False, cost_usd=MARK, duration_ms=1, turns=1),
    an_event("end", is_error=False, cost_usd=1.0, duration_ms=10 ** 400, turns=1),
    an_event("error", message={"text": MARK}),
    an_event("user", text=MARK * 3000),
    [MARK],
    MARK,
]


@pytest.mark.parametrize("event", EVENT_VARIANTS)
def test_an_event_of_another_shape_is_one_line(client, scripted, event):
    """Bug class: an event of an unknown type or shape (a hand-written log) shown in part, or breaking the whole conversation."""
    scripted.events = [an_event("user", text="before"), event, an_event("user", n=2, text="after")]
    page = client.get(f"/chat/{CHAT_ID}").text
    assert MARK not in page and chat.NOT_UNDERSTOOD_EVENT in main_text(page) and "before" in page and "after" in page


def test_an_event_with_a_lone_surrogate_is_one_line(client, scripted):
    """Bug class: a text that UTF-8 cannot hold (escaped in the JSON the sandbox wrote) shown, or breaking the page's encoding."""
    scripted.answers["chat.events"] = Raw('{"ok": true, "result": {"events": [{"n": 0, "t": 1, "type": "text", "text": "%s\\ud800"}], "next": 1, "running": false}}' % MARK)
    page = client.get(f"/chat/{CHAT_ID}").text
    assert MARK not in page and chat.NOT_UNDERSTOOD_EVENT in main_text(page)


def test_numbers_of_an_end_are_shown_only_when_they_are_numbers_within_bounds(client, scripted):
    """Bug class: a duration or a count the back end did not report shown, a count made into text that overflows, or a cost shown after a turn (the turn totals of the right column hold it)."""
    scripted.events = [an_event("end", is_error=False, cost_usd=0.25, duration_ms=1500, turns=2), an_event("end", n=1, is_error=True, cost_usd=1e300, duration_ms=None, turns=None)]
    text = main_text(client.get(f"/chat/{CHAT_ID}").text)
    assert "The turn ended after 1.5 s, 2 model turns." in text and "USD" not in text
    assert "The turn ended with an error." in text and "1e+300" not in text


# ── d. hostile text ─────────────────────────────────────────────────────────

TRICKY = HOSTILE + " javascript:alert(1) a\u202eb\u200bc [x](javascript:alert(1))"


def test_hostile_text_is_text_everywhere(client, scripted):
    """Bug class: text of the sandbox (a title, a question, the assistant's text, a tool's name, input and result, a file's name, an error) that becomes markup, an attribute, a link or a direction change in the page."""
    scripted.chats = [{**GOOD_CHAT, "title": TRICKY[:80]}]
    scripted.events = [
        an_event("user", text=TRICKY), an_event("text", n=1, text="**" + TRICKY + "**\n- " + TRICKY + "\n```\n" + TRICKY + "\n```"),
        an_event("tool", n=2, name=TRICKY[:200], input=TRICKY), an_event("tool", n=3, name="mcp__mongo__find", input="{}"),
        an_event("tool_result", n=4, text=TRICKY, is_error=True), an_event("file", n=5, name=TRICKY, size=3),
        an_event("file", n=6, name="../x.png", size=3), an_event("file", n=7, name="my plot.PNG", size=3), an_event("error", n=8, message=TRICKY),
    ]
    scripted.files = {"my plot.PNG": PNG}
    for page in (client.get("/chat").text, client.get(f"/chat/{CHAT_ID}").text, client.get(f"/chat/{CHAT_ID}/delete").text, client.get(f"/chat/{CHAT_ID}/events?offset=0", headers=HX).text):
        assert not any(mark in page for mark in RAW_MARKS) and "\u202e" not in page and "\u200b" not in page
        # The title's field is the one place a text of the runner is an attribute (an input's value): inert, escaped, and with its invisible characters as escapes; checked on its own below.
        without_field = re.sub(r'<input type="text" name="title" id="chat-title"[^>]*>', "", page)
        assert not [value for value in attributes(without_field) if "javascript:" in value or "<" in value or "\\u" in value]
    page = client.get(f"/chat/{CHAT_ID}").text
    field = re.search(r'<input type="text" name="title" id="chat-title"[^>]*>', page).group(0)
    shown = TRICKY[:80].replace("\u202e", "\\u202e").replace("\u200b", "\\u200b")
    assert attributes(field)[-3:-1] == [shown, shown] and 'value="&lt;script&gt;' in field
    assert '<span class="escape" title="made visible">\\u202e</span>' in page and "&lt;script&gt;" in page
    assert '<span class="nm">a tool</span>' in page and '<span class="nm">mongo · find</span>' in page
    links = re.findall(r'href="(/chat/[^"]+)"', page)
    images = re.findall(r'src="([^"]+)"', page)
    assert [link for link in links if "/file/" in link or "/image/" in link] == [f"/chat/{CHAT_ID}/file/my%20plot.PNG"] * 2
    assert [image for image in images if not image.startswith("/static/")] == [f"/chat/{CHAT_ID}/image/my%20plot.PNG"]


# ── f. the events part ──────────────────────────────────────────────────────


def nexts(page):
    """The requests of the events part a page or part carries (the left column's data in reach, asked once at load, and the right column's turn totals, a part of their own, left out)."""
    return [(html.unescape(href), trigger) for href, trigger in re.findall(r'hx-get="([^"]+)" hx-trigger="([^"]+)"', main_html(page)) if href != "/chat/reach" and not href.endswith("/totals")]


def test_the_events_part_pages_by_offset_and_stops_when_the_turn_has_ended(client, scripted):
    """Bug class: events shown twice or skipped across pages, a poll that never stops for an ended chat, or one that stops while the turn runs."""
    scripted.events = [an_event("user", n=index, text=f"question {index}") for index in range(250)]
    page = client.get(f"/chat/{CHAT_ID}").text
    assert page.count('class="msg user"') == 200 and nexts(page) == [(f"/chat/{CHAT_ID}/events?offset=200&from=0", "load")]
    part = client.get(f"/chat/{CHAT_ID}/events?offset=200", headers=HX).text
    assert part.count('class="msg user"') == 50 and "question 200" in part and "question 199" not in part
    assert nexts(part) == [(f"/chat/{CHAT_ID}/events?offset=250&from=0", "load")]
    last = client.get(f"/chat/{CHAT_ID}/events?offset=250", headers=HX).text
    assert nexts(last) == [] and "Stop the turn" not in last and 'id="chat-controls" hx-swap-oob="true"' in last
    scripted.running = True
    running = client.get(f"/chat/{CHAT_ID}/events?offset=250", headers=HX).text
    assert nexts(running) == [(f"/chat/{CHAT_ID}/events?offset=250&from=0", "load delay:2s")] and "Stop the turn" in running
    assert [request["offset"] for request in scripted.requests if request["op"] == "chat.events"] == [0, 200, 250, 250]


@pytest.mark.parametrize("query", ["", "?offset=", "?offset=x", "?offset=-1", "?offset=1.5", "?offset=99999999", "?offset=524289", "?offset=%E2%91%A0"])
def test_an_offset_that_is_not_a_whole_number_in_bounds_is_refused(client, scripted, query):
    """Bug class: an offset from the browser passed on unchecked (a negative or huge one, a digit of another script that `int` reads)."""
    assert client.get(f"/chat/{CHAT_ID}/events{query}", headers=HX).status_code == 404 and scripted.requests == []


@pytest.mark.parametrize("path", ["/chat/c_0123", "/chat/C_0123456789AB", "/chat/c_0123456789ab0", "/chat/new", "/chat/c_0123456789ab/file/%2E%2E", "/chat/c_0123456789ab/image/..%2Fb.png", "/chat/c_0123456789ab/file/a%00.png"])
def test_a_malformed_id_or_name_is_not_found_and_sends_nothing(client, scripted, path):
    """Bug class: an id or a file name of another form passed to the runner."""
    assert client.get(path).status_code == 404 and scripted.requests == []


# ── g. new, send, stop, delete ──────────────────────────────────────────────


def changes(scripted):
    return [request for request in scripted.requests if request["op"] in ("chat.new", "chat.send", "chat.stop", "chat.delete")]


def test_each_change_sends_exactly_its_request_and_leads_back(client, token, scripted):
    """Bug class: a change that sends another op or more fields than the runner takes (it refuses them), or that answers with a page a reload would send again."""
    assert post(client, "/chat/new", token).headers["location"] == f"/chat/{CHAT_ID}"
    answer = post(client, f"/chat/{CHAT_ID}/send", token, text="line one\r\nline two\rthree")
    assert answer.status_code == 303 and answer.headers["location"] == f"/chat/{CHAT_ID}"
    assert post(client, f"/chat/{CHAT_ID}/stop", token).headers["location"] == f"/chat/{CHAT_ID}"
    assert post(client, f"/chat/{CHAT_ID}/delete", token).headers["location"] == "/chat"
    assert changes(scripted) == [{"op": "chat.new"}, {"op": "chat.send", "chat": CHAT_ID, "text": "line one\nline two\nthree"}, {"op": "chat.stop", "chat": CHAT_ID}, {"op": "chat.delete", "chat": CHAT_ID}]


@pytest.mark.parametrize("text, sent", [("", False), ("x" * 32001, False), ("x" * 30000 + "\r\n" * 1500, True), ("x" * 32000, True), ("\n", True)])
def test_a_text_is_checked_and_a_refused_one_is_kept(client, token, scripted, text, sent):
    """Bug class: a question the runner refuses sent anyway, or a refused one lost from the box."""
    answer = post(client, f"/chat/{CHAT_ID}/send", token, text=text)
    assert bool(changes(scripted)) == sent
    if not sent:
        assert answer.status_code == 400 and chat.TEXT_REFUSED in main_text(answer.text)
        assert html.unescape(re.search(r"<textarea[^>]*>\n(.*?)</textarea>", answer.text, re.DOTALL).group(1)) == text.replace("\r\n", "\n")


@pytest.mark.parametrize("code", ["busy", "too_many", "over_cap", "chat_full"])
def test_a_refused_question_says_why_and_keeps_the_text(client, token, scripted, code):
    """Bug class: a refusal of the runner shown without its reason, or the question typed lost."""
    scripted.answers["chat.send"] = refuse(code)
    answer = post(client, f"/chat/{CHAT_ID}/send", token, text=HOSTILE)
    assert answer.status_code == 409 and words.CHAT_CODE_SENTENCES[code] in main_text(answer.text)
    assert html.unescape(re.search(r"<textarea[^>]*>\n(.*?)</textarea>", answer.text, re.DOTALL).group(1)) == HOSTILE


def test_the_delete_page_changes_nothing_and_says_what_goes(client, scripted):
    """Bug class: a GET that deletes, or a confirmation that does not say the files go too."""
    scripted.chats = [{**GOOD_CHAT, "title": HOSTILE}]
    page = client.get(f"/chat/{CHAT_ID}/delete")
    assert page.status_code == 200 and "every file in its folder are removed" in main_text(page.text) and HOSTILE in main_text(page.text)
    assert scripted.ops() == ["chat.list"] and client.head(f"/chat/{CHAT_ID}/delete").status_code == 405


@pytest.mark.parametrize("path", ["/chat/new", f"/chat/{CHAT_ID}/send", f"/chat/{CHAT_ID}/stop", f"/chat/{CHAT_ID}/delete", f"/chat/{CHAT_ID}/title"])
def test_the_guard_refuses_a_post_without_the_token_or_from_another_origin(client, token, scripted, path):
    """Bug class: a page of another site making the operator's browser send a question, stop, delete or rename a chat."""
    data = {"text": "x"} if path.endswith("/send") else {"title": "x"} if path.endswith("/title") else {}
    assert client.post(path, data=data, headers={"origin": ORIGIN}).status_code == 403
    assert client.post(path, data={**data, "form_token": token}, headers={"origin": "https://elsewhere.example"}).status_code == 403
    assert client.post(path, data={**data, "form_token": token, "extra": "1"}, headers={"origin": ORIGIN}).status_code == 400
    assert scripted.requests == []


# ── h. files and images ─────────────────────────────────────────────────────


def test_a_download_has_the_fixed_headers_and_the_reduced_name(client, scripted):
    """Bug class: a file of the sandbox served with a type a browser would render (HTML, SVG), inline, under its own name with quotes or a path, or without the sandbox policy."""
    data = b"<html><script>alert(1)</script>" * 40000
    scripted.files = {"my plot .png": data}
    answer = client.get(f"/chat/{CHAT_ID}/file/my%20plot%20.png")
    headers = answer.headers
    assert answer.status_code == 200 and answer.content == data and headers["content-length"] == str(len(data))
    assert headers["content-type"] == "application/octet-stream" and headers["content-disposition"] == 'attachment; filename="my_plot_.png"'
    assert headers.get_list("content-security-policy") == ["sandbox", CSP] and headers["x-content-type-options"] == "nosniff" and headers["cache-control"] == "no-store"
    assert [request["offset"] for request in scripted.requests if request["op"] == "chat.file"] == [0, 524288, 1048576]


@pytest.mark.parametrize("name, data, served", [
    ("a.png", PNG, "image/png"), ("a.jpg", JPEG, "image/jpeg"), ("a.png", JPEG, "image/jpeg"),
    ("a.png", b"<svg xmlns='http://www.w3.org/2000/svg'><script>alert(1)</script></svg>", None),
    ("a.png", b"<html>" + PNG, None), ("a.png", b"plain text", None), ("a.png", b"", None), ("b.png", PNG, None),
])
def test_an_image_is_served_only_by_its_signature(client, scripted, name, data, served):
    """Bug class: a file named like an image (an SVG, an HTML page) served inline, or a type taken from the name."""
    scripted.files = {name: data}
    answer = client.get(f"/chat/{CHAT_ID}/image/a.{name.rsplit('.', 1)[1] if name != 'b.png' else 'png'}")
    if served is None:
        assert answer.status_code == 404
        return
    assert answer.status_code == 200 and answer.content == data and answer.headers["content-type"] == served
    assert answer.headers.get_list("content-security-policy") == ["sandbox", CSP] and answer.headers["x-content-type-options"] == "nosniff" and answer.headers["cache-control"] == "no-store"


def test_an_image_larger_than_its_limit_or_changing_is_not_served(make_app, make_client, scripted):
    """Bug class: an image of any size held in memory, or one put together from pieces of a file that changed."""
    made = make_client(make_app(assistant=assistant.Assistant(scripted.path), chat_image_bytes=len(PNG) - 1))
    log_in(made)
    scripted.files = {"a.png": PNG}
    assert made.get(f"/chat/{CHAT_ID}/image/a.png").status_code == 404


def piecewise(sizes, data):
    """A `chat.file` answer giving whole pieces of `data`, its size the next of `sizes` (in whole pieces less or more a few bytes) at each piece."""
    import base64

    told = iter(sizes)

    def answer(request):
        piece = data[request["offset"]:request["offset"] + request["limit"]]
        size = next(told)
        return {"data": base64.b64encode(piece).decode(), "size": size, "next": request["offset"] + len(piece), "end": request["offset"] + len(piece) >= size}

    return answer


WHOLE = assistant.PIECE_BYTES


@pytest.mark.parametrize("sizes", [(3 * WHOLE, 3 * WHOLE, 3 * WHOLE - 5), (3 * WHOLE, 3 * WHOLE + 1, 3 * WHOLE + 1), (3 * WHOLE,) * 4])
def test_a_file_that_changes_size_ends_the_download_without_more_than_announced(client, scripted, sizes):
    """Bug class: a download that sends more than its announced length (the browser then reads the rest as another answer) or a mixture of two versions of a file without a sign."""
    scripted.answers["chat.file"] = piecewise(sizes, b"0123456789" * (WHOLE // 2))
    answer = client.get(f"/chat/{CHAT_ID}/file/a.bin")
    assert answer.headers["content-length"] == str(3 * WHOLE) and len(answer.content) <= 3 * WHOLE
    assert (len(answer.content) == 3 * WHOLE) == (sizes[1:3] == (3 * WHOLE, 3 * WHOLE))


def test_a_file_over_the_cap_is_refused(make_app, make_client, scripted):
    """Bug class: a download of any size streamed from the sandbox."""
    made = make_client(make_app(assistant=assistant.Assistant(scripted.path), chat_download_bytes=10))
    log_in(made)
    scripted.files = {"big.bin": b"x" * 11, "small.bin": b"x" * 10}
    assert made.get(f"/chat/{CHAT_ID}/file/big.bin").status_code == 413 and made.get(f"/chat/{CHAT_ID}/file/small.bin").status_code == 200


def test_at_most_two_downloads_at_a_time_and_no_file_is_opened(client, scripted):
    """Bug class: downloads without a bound (each holds a worker and the runner's connections), and a web UI that opens a file of the chat itself (it has no mount of the chat's work folder, and must not get one)."""
    held = threading.Event()
    data = b"y" * (assistant.PIECE_BYTES + 5)
    scripted.files = {"a.bin": data}
    default = scripted._default

    def slow(request):
        if request["offset"]:
            held.wait(10)
        return default("chat.file", request)

    scripted.answers["chat.file"] = slow
    opened = []

    def watch(event, arguments):
        if event == "open" and watching and isinstance(arguments[0], (str, bytes)) and not str(arguments[0]).endswith((".py", ".pyc", ".html")):
            opened.append(arguments[0])
        if event == "socket.connect" and watching:
            opened.append(("connect", str(arguments[1])))

    watching = True
    sys.addaudithook(watch)
    try:
        answers = []
        threads = [threading.Thread(target=lambda: answers.append(client.get(f"/chat/{CHAT_ID}/file/a.bin"))) for _ in range(2)]
        for thread in threads:
            thread.start()
        wait_until(lambda: len([request for request in scripted.requests if request.get("offset")]) == 2)
        third = client.get(f"/chat/{CHAT_ID}/file/a.bin")
        assert third.status_code == 503 and chat.DOWNLOADS_BUSY in third.text
        held.set()
        for thread in threads:
            thread.join(10)
    finally:
        watching = False
    assert [answer.content == data for answer in answers] == [True, True]
    assert opened and all(entry == ("connect", scripted.path) for entry in opened), opened


# ── j. the scans ────────────────────────────────────────────────────────────


def test_no_page_or_part_holds_inline_code_and_every_answer_has_the_headers(client, token, scripted, make_app, make_client):
    """Bug class: inline code in a page only the Chat window renders, or Pico's busy spinner (both blocked by the policy), an answer without the guard's headers, a route that answers HEAD."""
    scripted.events = [an_event(kind, n=index, **fields) for index, (kind, fields) in enumerate([
        ("user", {"text": HOSTILE}), ("text", {"text": "# h\n- a\n> q\n```\nc\n```\n" + HOSTILE}), ("tool", {"name": "Bash", "input": HOSTILE}),
        ("tool_result", {"text": HOSTILE, "is_error": False}), ("file", {"name": "a.png", "size": 3}), ("end", {"is_error": False, "cost_usd": 0.1, "duration_ms": 5, "turns": 1}), ("error", {"message": HOSTILE}), ("odd", {}),
    ])]
    scripted.files = {"a.png": PNG}
    scripted.running = True
    answers = [client.get(path) for method, path in ROUTES if method == "GET"]
    answers += [client.get(f"/chat/{CHAT_ID}/events?offset=0", headers=HX), post(client, f"/chat/{CHAT_ID}/send", token, text=""), post(client, "/chat/new", token, extra="1")]
    scripted.answers["chat.stop"] = refuse("not_found")
    answers.append(post(client, f"/chat/{CHAT_ID}/stop", token))
    stranger = make_client(make_app())
    log_in(stranger)
    answers.append(stranger.get("/chat"))
    for answer in answers:
        assert CSP in answer.headers.get_list("content-security-policy") and answer.headers["cache-control"] == "no-store", answer.request.url
        if answer.headers.get("content-type", "").startswith("text/html"):
            # `aria-busy` makes Pico draw its spinner from a `data:` image, which the policy blocks (found by the browser check).
            assert inline_code(answer.text) == [] and "aria-busy" not in answer.text, answer.request.url
    assert {answer.status_code for answer in answers} >= {200, 400, 404}
    for method, path in ROUTES:
        assert client.head(path).status_code == 405, path


def test_the_navigation_names_chat_first_as_a_link(client):
    """Bug class: a Chat window that cannot be reached from the frame, or still shown as not available; the windows in another order than the mock-up's (Chat, Management, Queue)."""
    page = client.get("/").text
    windows = re.search(r'<nav class="wins"[^>]*>(.*?)</nav>', page, re.DOTALL).group(1)
    assert re.findall(r'href="([^"]+)"', windows) == ["/chat", "/", "/queue"] and "not available" not in page
    assert '<a class="win on" href="/chat" aria-current="page"><span class="ico" aria-hidden="true">✦</span>Chat</a>' in client.get("/chat").text


def chat_tab(page):
    """The address of the top bar's Chat window link."""
    windows = re.search(r'<nav class="wins"[^>]*>(.*?)</nav>', page, re.DOTALL).group(1)
    return re.findall(r'href="([^"]+)"', windows)[0]


def test_the_chat_tab_reopens_the_chat_last_opened_in_this_login(client, token, scripted):
    """Bug class: the top bar's Chat tab always landing on "No chat open" after a chat was opened, or still leading to a chat that was deleted here or is gone elsewhere (a dead link); Management and Queue moved along with it."""
    assert chat_tab(client.get("/").text) == "/chat"
    assert client.get(f"/chat/{CHAT_ID}").status_code == 200
    for page in (client.get("/").text, client.get("/queue").text, client.get("/chat").text):
        assert chat_tab(page) == f"/chat/{CHAT_ID}"
        assert re.findall(r'href="([^"]+)"', re.search(r'<nav class="wins"[^>]*>(.*?)</nav>', page, re.DOTALL).group(1))[1:] == ["/", "/queue"]
    scripted.answers["chat.delete"] = refuse("internal")
    post(client, f"/chat/{CHAT_ID}/delete", token)
    assert chat_tab(client.get("/").text) == f"/chat/{CHAT_ID}"
    del scripted.answers["chat.delete"]
    assert post(client, f"/chat/{CHAT_ID}/delete", token).status_code == 303
    assert chat_tab(client.get("/").text) == "/chat"
    assert client.get(f"/chat/{CHAT_ID}").status_code == 200 and chat_tab(client.get("/").text) == f"/chat/{CHAT_ID}"
    scripted.chats = []
    assert client.get(f"/chat/{CHAT_ID}").status_code == 404
    assert chat_tab(client.get("/").text) == "/chat"


def test_the_left_column_has_no_bottom_note(client, scripted):
    """Bug class: the removed note at the bottom of the left column coming back with a template merge."""
    for page in (client.get("/chat").text, client.get(f"/chat/{CHAT_ID}").text):
        assert "This window only reads data" not in page and "side-note" not in page


# ── the real runner ─────────────────────────────────────────────────────────


def chat_helpers():
    name = "chat_helpers"
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, Path(__file__).resolve().parents[1] / "chat" / "chat_helpers.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        sys.modules[name] = module
    return sys.modules[name]


@pytest.fixture
def real_runner(tmp_path, chat_folder):
    """The real runner with the stand-in `claude` of the runner's tests, served on a socket in `chat_folder`; `plan(steps)` sets the stand-in's steps."""
    from social_data_pipeline.chat import claude, runner

    helpers = chat_helpers()
    script, plan_path = tmp_path / "claude", tmp_path / "plan.json"
    script.write_text(f"#!{sys.executable}\nPLAN = {str(plan_path)!r}\n" + helpers.STANDIN)
    script.chmod(0o755)
    (tmp_path / "work").mkdir()
    (tmp_path / "tmp").mkdir()
    back = claude.ClaudeCode("127.0.0.1", ["mongo"], executable=str(script), grace=0.5, join=0.5)
    chats = runner.Runner(str(tmp_path / "work"), back, "claude-code", ["mongo"], 1 << 30, tmp=str(tmp_path / "tmp"))
    server = runner.Server(chats, f"{chat_folder}/runner.sock")
    server.start()

    def plan(steps):
        plan_path.write_text(json.dumps({"record": str(tmp_path / "record.jsonl"), "steps": steps}))

    yield type("Real", (), {"path": f"{chat_folder}/runner.sock", "plan": staticmethod(plan), "helpers": helpers})
    server.close()
    chats.close()


def test_a_real_turn_through_the_page(make_app, make_client, real_runner):
    """Bug class: a page whose checks of the runner's answers refuse what the real runner sends (an event's fields, the paging, a file's pieces), which the scripted stand-in alone would not show."""
    helpers = real_runner.helpers
    real_runner.plan([
        helpers.INIT,
        ["out", {"type": "assistant", "message": {"content": [{"type": "tool_use", "id": "t1", "name": "Bash", "input": {"command": "ls"}}]}}],
        ["out", {"type": "user", "message": {"content": [{"type": "tool_result", "tool_use_id": "t1", "content": "made-up listing"}]}}],
        ["write", ["notes.txt", "made-up notes\n" * 50000]],
        ["out", {"type": "assistant", "message": {"content": [{"type": "text", "text": "**Done**: see `notes.txt`"}]}}],
        helpers.result(cost=0.5),
    ])
    made = make_client(make_app(assistant=assistant.Assistant(real_runner.path)))
    log_in(made)
    token = form_token(made)
    location = post(made, "/chat/new", token).headers["location"]
    chat_id = location.rsplit("/", 1)[1]
    assert post(made, location + "/send", token, text="What is there?").status_code == 303
    deadline = time.monotonic() + 20
    while "Stop the turn" in made.get(location).text:
        assert time.monotonic() < deadline
        time.sleep(0.1)
    page = made.get(location).text
    text = main_text(page)
    assert page.count('class="msg user"') == 1 and "What is there?" in text and '<span class="nm">Bash</span>' in page and "made-up listing" in text
    assert "<strong>Done</strong>: see <code>notes.txt</code>" in page and f'href="/chat/{chat_id}/file/notes.txt"' in page
    assert "The turn ended after 1.2 s, 2 model turns." in text and chat.NOT_UNDERSTOOD_EVENT not in text and NOT_UNDERSTOOD not in text
    assert nexts(page) == [(f"/chat/{chat_id}/events?offset=6&from=0", "load")]
    assert nexts(made.get(f"/chat/{chat_id}/events?offset=6", headers=HX).text) == []
    assert made.get(f"/chat/{chat_id}/file/notes.txt").content == b"made-up notes\n" * 50000
    assert "What is there?" in main_text(made.get("/chat").text)
    assert post(made, location + "/delete", token).headers["location"] == "/chat" and "No chat yet." in main_text(made.get("/chat").text)


# ── bounds against a hostile runner ─────────────────────────────────────────


def test_a_page_of_events_over_the_byte_bound_is_not_shown(client, scripted):
    """Bug class: a frame of up to 16 MiB of event text taken whole (and multiplied by its model), which can exhaust the container's memory and take every window down."""
    scripted.events = [an_event("user", n=index, text=MARK + "x" * 3000) for index in range(200)]
    page = client.get(f"/chat/{CHAT_ID}")
    assert page.status_code == 200 and NOT_UNDERSTOOD in main_text(page.text) and MARK not in page.text


@pytest.mark.parametrize("text, shown", [("a\u200b" * 16000, 3), ("# \n" * 20000, 2), ("- \n" * 20000, 4), ("```\n```\n" * 7000, 7)])
def test_the_units_of_a_page_are_capped_and_the_first_skipped_event_links_to_itself(client, scripted, text, shown):
    """Bug class: a page within the byte bound whose model is many times its size (one run per escaped character, one block per empty heading, item or fence), unbounded; or a placeholder that promises a way to the event and gives none."""
    scripted.events = [an_event("user" if text.startswith("a") else "text", n=index, text=text) for index in range(8)]
    models, units = chat.events_model(scripted.events, CHAT_ID, 100_000, 40)
    assert units <= 100_000 and [model["kind"] for model in models] == [models[0]["kind"]] * shown + ["skipped"] * (8 - shown)
    assert models[shown] == {"kind": "skipped", "line": chat.TOO_LONG_EVENT, "href": f"/chat/{CHAT_ID}?from={40 + shown}"}
    assert all(model == {"kind": "skipped", "line": chat.FOLLOWING_EVENT, "href": None} for model in models[shown + 1:])
    page = client.get(f"/chat/{CHAT_ID}").text
    assert main_text(page).count(chat.TOO_LONG_EVENT) == 1 and main_text(page).count(chat.FOLLOWING_EVENT) == 7 - shown
    assert f'<a href="/chat/{CHAT_ID}?from={shown}">Continue from this event</a>' in page


def test_an_event_too_long_alone_is_shown_cut_in_a_view_of_its_own(make_app, make_client, scripted):
    """Bug class: an event that alone passes the budget skipped in every view, so that following its link never shows it."""
    made = make_client(make_app(assistant=assistant.Assistant(scripted.path), chat_units_max=5000))
    log_in(made)
    scripted.events = [an_event("user", text="before"), an_event("user", n=1, text="a\u200b" * 16000)]
    page = made.get(f"/chat/{CHAT_ID}").text
    assert chat.TOO_LONG_EVENT in main_text(page) and f'href="/chat/{CHAT_ID}?from=1"' in page
    page = made.get(f"/chat/{CHAT_ID}?from=1").text
    text = main_text(page)
    assert page.count('class="msg user"') == 1 and chat.SHOWN_CUT in text and chat.TOO_LONG_EVENT not in text and "\\u200b" in text
    models, units = chat.events_model(scripted.events[1:], CHAT_ID, 5000, 1)
    assert units <= 5000 and models[0]["shown_cut"] == chat.SHOWN_CUT


def test_a_reply_over_the_cap_is_refused_unread_and_one_of_many_containers_unparsed(client, scripted, monkeypatch):
    """Bug class: a frame of 16 MiB (or of a million empty lists) read and parsed whole by every one of the runner's calls at once, before any check of its shape, which can exhaust the container's memory."""
    from social_data_pipeline.control import framing

    scripted.answers["chat.list"] = Raw(b'{"ok": true, "result": {"chats": [], "x": "' + b"y" * (4 << 20) + b'"}}')
    page = client.get("/chat")
    assert NO_REPLY in main_text(page.text) and scripted.sent[-1] < 2 << 20
    decoded = []
    real = framing.decode
    monkeypatch.setattr(framing, "decode", lambda payload: decoded.append(bytes(payload[:40])) or real(payload))
    scripted.answers["chat.list"] = Raw(b'{"ok": true, "result": {"chats": [' + b",".join([b"[]"] * 20001) + b"]}}")
    page = client.get("/chat")
    assert NO_REPLY in main_text(page.text) and decoded and not [payload for payload in decoded if b'"chats": [[]' in payload]


def test_the_largest_honest_replies_pass(client, scripted):
    """Bug class: caps so tight that the runner's own largest replies (a page of events at its byte bound, 500 files, 200 chats) are refused."""
    scripted.events = [an_event("user", n=index, text="{[\"" * 640) for index in range(200)]
    scripted.files = {f"file {index:03d}.txt": b"x" for index in range(500)}
    page = client.get(f"/chat/{CHAT_ID}").text
    assert page.count('class="msg user"') == 200 and page.count(f'href="/chat/{CHAT_ID}/file/file%20') == 500 and NOT_UNDERSTOOD not in main_text(page)
    scripted.chats = [{**GOOD_CHAT, "id": f"c_{index:012x}", "title": "x" * 80} for index in range(200)]
    # Counted in `<main>`: the top bar's Chat tab leads to the chat opened above.
    assert main_html(client.get("/chat").text).count('href="/chat/c_') == 200


ODD = [10 ** 400, -1, True, 1e308, "x"]


def with_odd(reply, path, value):
    """`reply` with `value` at `path` (keys and list indexes)."""
    import copy

    made = copy.deepcopy(reply)
    target = made
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    return made


NUMERIC = [
    ("hello", ("version",), "GET", "/chat"),
    ("chat.list", ("chats", 0, "created"), "GET", "/chat"),
    ("chat.events", ("next",), "GET", f"/chat/{CHAT_ID}"),
    ("chat.events", ("events", 0, "n"), "GET", f"/chat/{CHAT_ID}"),
    ("chat.events", ("events", 0, "t"), "GET", f"/chat/{CHAT_ID}"),
    ("chat.events", ("events", 1, "size"), "GET", f"/chat/{CHAT_ID}"),
    ("chat.events", ("events", 2, "cost_usd"), "GET", f"/chat/{CHAT_ID}"),
    ("chat.events", ("events", 2, "duration_ms"), "GET", f"/chat/{CHAT_ID}"),
    ("chat.events", ("events", 2, "turns"), "GET", f"/chat/{CHAT_ID}"),
    ("chat.files", ("left_out",), "GET", f"/chat/{CHAT_ID}"),
    ("chat.files", ("unlisted",), "GET", f"/chat/{CHAT_ID}"),
    ("chat.files", ("files", 0, "size"), "GET", f"/chat/{CHAT_ID}"),
    ("chat.files", ("files", 0, "mtime"), "GET", f"/chat/{CHAT_ID}"),
    ("chat.send", ("offset",), "POST", f"/chat/{CHAT_ID}/send"),
    ("chat.file", ("size",), "GET", f"/chat/{CHAT_ID}/file/a.png"),
    ("chat.file", ("next",), "GET", f"/chat/{CHAT_ID}/image/a.png"),
]


@pytest.mark.parametrize("op, path, method, route", NUMERIC)
@pytest.mark.parametrize("value", ODD)
def test_an_odd_number_in_any_numeric_field_is_never_an_error_of_the_page(client, token, scripted, op, path, method, route, value):
    """Bug class: a number taken from the runner and used before its bounds are checked (`math.isfinite(10 ** 400)` raises), which answers 500."""
    scripted.files = {"a.png": PNG}
    scripted.events = [an_event("user", text="before"), an_event("file", n=1, name="a.png", size=3), an_event("end", n=2, is_error=False, cost_usd=0.1, duration_ms=5, turns=1)]
    good = scripted._default(op, {"op": op, "chat": CHAT_ID, "offset": 0, "limit": 200, "name": "a.png", "text": "x"})
    scripted.answers[op] = with_odd(good, path, value)
    answer = request_route(client, token, method, route)
    assert answer.status_code != 500, answer.text[-300:]
    if value in (10 ** 400, 1e308) and value is not True:
        assert str(value)[:30] not in answer.text and "1e+308" not in answer.text


def test_a_huge_time_or_cost_of_an_event_is_its_line_or_not_shown(client, scripted):
    """Bug class: `t` or `cost_usd` of 10 ** 400 answered with a 500."""
    scripted.events = [an_event("user", text="a", t=10 ** 400), an_event("end", n=1, is_error=False, cost_usd=10 ** 400, duration_ms=5, turns=1)]
    page = client.get(f"/chat/{CHAT_ID}")
    text = main_text(page.text)
    assert page.status_code == 200 and chat.NOT_UNDERSTOOD_EVENT in text and "The turn ended after 0.0 s, 1 model turns." in text


def test_a_view_appends_at_most_2000_events_then_offers_to_continue(client, scripted):
    """Bug class: one view that appends events without end (a log of half a million lines held by the browser and asked for by the page)."""
    scripted.events = [an_event("user", n=index, text=f"question {index}") for index in range(2100)]
    part = client.get(f"/chat/{CHAT_ID}/events?offset=1900&from=0", headers=HX).text
    assert part.count('class="msg user"') == 100 and nexts(part) == [] and f'<a href="/chat/{CHAT_ID}?from=2000">Continue from here</a>' in part
    assert scripted.requests[-1]["limit"] == 100
    asked = len(scripted.requests)
    part = client.get(f"/chat/{CHAT_ID}/events?offset=2050&from=50", headers=HX).text
    assert "Continue from here" in part and len(scripted.requests) == asked
    page = client.get(f"/chat/{CHAT_ID}?from=2000").text
    assert "From event 2000 on." in main_text(page) and page.count('class="msg user"') == 100 and "question 2000" in page and "question 1999" not in page
    assert nexts(page) == [(f"/chat/{CHAT_ID}/events?offset=2100&from=2000", "load")]


@pytest.mark.parametrize("path", ["?from=", "?from=x", "?from=-1", "?from=524289", "?from=1.5", "/events?offset=5&from=6", "/events?offset=5&from=x"])
def test_a_from_that_is_not_a_whole_number_in_bounds_is_refused(client, scripted, path):
    """Bug class: the start of a view from the browser passed on unchecked, or one after the offset asked (the count of events shown would go negative)."""
    assert client.get(f"/chat/{CHAT_ID}{path}", headers=HX).status_code == 404 and scripted.requests == []


def pieces_of(data, sizes, clock=None, advance=0):
    """A `chat.file` answer giving pieces of `data` of the next of `sizes` bytes each, the clock moved on by `advance` at each piece."""
    import base64

    told = iter(sizes)

    def answer(request):
        if clock is not None:
            clock.now += advance
        piece = data[request["offset"]:request["offset"] + min(next(told), request["limit"])]
        return {"data": base64.b64encode(piece).decode(), "size": len(data), "next": request["offset"] + len(piece), "end": request["offset"] + len(piece) >= len(data)}

    return answer


def test_a_download_ends_at_a_short_piece_and_at_its_total_time(client, scripted, clock):
    """Bug class: a download the sandbox can stretch without end (one byte per piece, or pieces that come slowly), which holds one of the two places for ever."""
    whole = assistant.PIECE_BYTES
    scripted.answers["chat.file"] = pieces_of(b"z" * 20, [1] * 20)
    assert client.get(f"/chat/{CHAT_ID}/file/a.bin").status_code == 503
    scripted.answers["chat.file"] = pieces_of(b"z" * (whole + 10), [whole, 1, 9])
    answer = client.get(f"/chat/{CHAT_ID}/file/a.bin")
    assert answer.headers["content-length"] == str(whole + 10) and len(answer.content) == whole
    scripted.answers["chat.file"] = pieces_of(b"z" * (3 * whole), [whole] * 3, clock, 1801)
    answer = client.get(f"/chat/{CHAT_ID}/file/a.bin")
    assert len(answer.content) == 2 * whole
    scripted.answers["chat.file"] = pieces_of(b"z" * (3 * whole), [whole] * 3, clock, 1)
    assert len(client.get(f"/chat/{CHAT_ID}/file/a.bin").content) == 3 * whole


def test_an_image_ends_at_a_short_piece_and_at_its_total_time(client, scripted, clock):
    """Bug class: an image read without end from a slow or dribbling sandbox, holding a place of the images' bound."""
    data = PNG + b"z" * assistant.PIECE_BYTES
    scripted.answers["chat.file"] = pieces_of(data, [len(PNG)] * 100)
    assert client.get(f"/chat/{CHAT_ID}/image/a.png").status_code == 404
    scripted.answers["chat.file"] = pieces_of(data, [assistant.PIECE_BYTES] * 2, clock, 31)
    assert client.get(f"/chat/{CHAT_ID}/image/a.png").status_code == 404
    scripted.answers["chat.file"] = pieces_of(data, [assistant.PIECE_BYTES] * 2, clock, 1)
    assert client.get(f"/chat/{CHAT_ID}/image/a.png").content == data


@pytest.mark.parametrize("unit", [b'\\"', b'"', b"\\", b'"[', b"[", b'\\\\"['])
def test_the_count_of_containers_is_linear(unit):
    """Bug class: a count of the objects and lists of a reply whose work grows faster than the reply (a pattern that needs a closing quote, tried again from every quote), which a reply of escaped quotes makes run for hours with every window stalled."""
    measured = []
    for count in (10000, 100000, 550000):
        steps = []
        assistant.containers(unit * count, steps)
        measured.append(steps[0] / (len(unit) * count))
    assert max(measured) <= 1 and max(measured) - min(measured) < 0.01, measured


@pytest.mark.parametrize("payload, count", [
    (b'{"a": "[[{{\\"[", "b": [[], {}]}', 4), (b'{"a": "never ends [[[', 1), (b'{"a": ["x\\', 2), (b'["\\\\"[', 2), (b'[{"k": "{"}, "]"]', 2),
])
def test_brackets_inside_strings_are_not_counted(payload, count):
    """Bug class: brackets inside a text (code the assistant wrote) counted, so that an honest reply is refused; or an unterminated string or a lone backslash breaking the count."""
    assert assistant.containers(payload) == count


def test_a_reply_that_dribbles_ends_at_the_time_limit(make_app, make_client, scripted):
    """Bug class: a reply read without a total limit (each read waits only for its own bytes), so that a runner sending a byte at a time holds a worker for many minutes."""
    looks = []

    def now():
        looks.append(1)
        return len(looks) * 0.5

    made = make_client(make_app(assistant=assistant.Assistant(scripted.path, connect=lambda path, timeout: assistant.RunnerClient(path, timeout, now=now))))
    log_in(made)
    scripted.dribble = 0.001
    page = made.get("/chat")
    assert NO_REPLY in main_text(page.text) and len(looks) <= 2 * (assistant.READ_SECONDS * 2 + 2)
    scripted.dribble = 0
    looks.clear()
    text = main_text(made.get("/chat").text)
    assert "made-up question" in text and NO_REPLY not in text


# ── the three columns (the mock-up's layout) ────────────────────────────────


class Nesting(html.parser.HTMLParser):
    """The open elements above each element with an id, and the region of each `class` value seen, as the browser's parser would nest them (void elements never open)."""

    VOID = {"input", "img", "br", "meta", "link", "hr", "source", "area", "base", "col", "embed", "wbr"}

    def __init__(self):
        super().__init__()
        self.stack, self.above, self.inside = [], {}, []

    def handle_starttag(self, tag, attrs):
        found = dict(attrs)
        if "id" in found:
            self.above[found["id"]] = [entry[0] for entry in self.stack]
        if tag not in self.VOID:
            self.stack.append((tag, found.get("class", ""), found.get("action", "")))

    def handle_endtag(self, tag):
        while self.stack:
            if self.stack.pop()[0] == tag:
                break


def nesting(page):
    parser = Nesting()
    parser.feed(page)
    return parser


def columns(page):
    """The classes of the three columns of the window's `<main>`, in order."""
    return re.findall(r'<(aside|section) class="(left|mid|rightp)"', page)


def test_the_conversation_page_shows_the_chats_and_the_data_in_reach_from_one_list(client, scripted):
    """Bug class: the conversation page without the chat list (or asking the runner for it twice), the running dot or the open chat's mark on the wrong chat, the databases of `hello` missing from "Data in reach"."""
    other = "c_0123456789ac"
    scripted.chats = [{**GOOD_CHAT, "title": "the open one"}, {**GOOD_CHAT, "id": other, "title": "the busy one", "running": True}]
    scripted.answers["hello"] = {"version": 1, "backend": "claude-code", "databases": ["mongo", "starrocks"], "queue": True}
    page = client.get(f"/chat/{CHAT_ID}").text
    assert columns(page) == [("aside", "left"), ("section", "mid"), ("aside", "rightp")]
    assert f'<a class="navi on" href="/chat/{CHAT_ID}" aria-current="page"><span class="lbl">the open one</span></a>' in page
    assert re.search(rf'<a class="navi" href="/chat/{other}"><span class="lbl">the busy one</span><span class="dot run"', page)
    assert page.count('class="dot run"') == 1
    text = main_text(page)
    assert "MongoDB: checking" in text and "StarRocks: checking" in text and 'hx-get="/chat/reach" hx-trigger="load"' in page and chat.QUEUE_ON in text and "Claude Code" in text
    assert scripted.ops().count("chat.list") == 1 and scripted.ops().count("hello") == 1


@pytest.mark.parametrize("hello", [refuse("internal"), {"version": 1, "backend": MARK.upper(), "databases": []}])
def test_a_failed_hello_is_confined_to_the_side_columns(client, scripted, hello):
    """Bug class: a `hello` that fails (refused, or of another shape) taking the conversation or the chat list down with it, or the side columns claiming what the sandbox can reach without its answer."""
    scripted.answers["hello"] = hello
    scripted.events = [an_event("user", text="a question that stays")]
    page = client.get(f"/chat/{CHAT_ID}")
    text = main_text(page.text)
    assert page.status_code == 200 and "a question that stays" in text and 'action="/chat/' + CHAT_ID + '/send"' in page.text
    assert "The sandbox did not answer" in text and "Can reach" not in text and chat.QUEUE_ON not in text and MARK not in page.text.upper()
    listing = client.get("/chat")
    assert listing.status_code == 200 and "made-up question" in main_text(listing.text) and "The sandbox did not answer" in main_text(listing.text)


def test_the_chats_page_with_no_chat_open_has_the_three_columns_and_nothing_to_send(client, scripted):
    """Bug class: `/chat` drawn as another layout than the conversation, or with an answer box that posts to no chat."""
    page = client.get("/chat").text
    assert columns(page) == [("aside", "left"), ("section", "mid"), ("aside", "rightp")]
    assert "Start a new chat" in main_text(page) and "/send" not in page and 'id="chat-controls"' not in page
    assert page.count('action="/chat/new"') == 2 and re.search(r"<textarea[^>]*disabled", page)


def test_the_stop_button_is_never_inside_the_send_form(client, scripted):
    """Bug class: `#chat-controls` (the stop form) drawn inside the send form: the parser drops the inner form, and Stop then sends the question."""
    scripted.running = True
    page = client.get(f"/chat/{CHAT_ID}").text
    parsed = nesting(page)
    assert "Stop the turn" in page and "form" not in parsed.above["chat-controls"]
    assert page.count("<form") == page.count("</form>")
    sends = [match.start() for match in re.finditer(r'<form class="send"', page)]
    stops = [match.start() for match in re.finditer(r'<form class="stop"', page)]
    assert len(sends) == 1 and len(stops) == 1 and page.index("</form>", sends[0]) < stops[0]


@pytest.mark.parametrize("name, label", [
    ("mcp__starrocks__read_query", ("starrocks", "read_query")),
    ("mcp__sdp-mongo__find__deep", ("sdp-mongo", "find__deep")),
    ("mcp__jobs", (None, None)),
    ("mcp____x", (None, None)),
    ("Bash", (None, None)),
    ("mcp__a__b<", (None, None)),
])
def test_an_mcp_tool_is_labelled_by_its_server_and_tool(name, label):
    """Bug class: a tool of an MCP server shown as its raw `mcp__server__tool` name, or a label made of anything but a name that is already shown (a name that is not plain gets no label)."""
    model = chat.event_model(an_event("tool", name=name, input="{}"), CHAT_ID)
    assert (model["server"], model["tool"]) == label
    assert model["name"] == (name if re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", name) else None)


# ── k. the plot card's buttons and the table view ───────────────────────────


def hrefs(page):
    return re.findall(r'href="(/chat/[^"]+)"', page)


def test_a_plot_cards_buttons_link_only_files_of_the_chats_list_with_its_stem():
    """Bug class: a button guessed from the image's name (a CSV or a script the chat's folder does not hold, so a link to a file that is not there, or that the sandbox placed later), a near stem taken for the same one, or a link made where the chat's list could not be read."""
    stems = chat.stems_of(["plot.png", "plot.CSV", "plot.py", "plotx.csv", "plot.png.csv", "other.parquet"])
    labels = [(button["label"], button["href"].rsplit("/", 2)[1:]) for button in chat.file_buttons(CHAT_ID, "plot.png", stems)]
    assert labels == [("PNG", ["file", "plot.png"]), ("Data (CSV)", ["file", "plot.CSV"]), ("Code", ["file", "plot.py"]), ("Table view", ["table", "plot.CSV"])]
    assert [button["label"] for button in chat.file_buttons(CHAT_ID, "other.jpg", stems)] == ["JPEG", "Data (Parquet)"]
    assert [button["label"] for button in chat.file_buttons(CHAT_ID, "plot.png", None)] == ["PNG"]
    assert [button["label"] for button in chat.file_buttons(CHAT_ID, "x.csv", {})] == ["Download", "Table view"]


def test_the_page_and_the_events_part_give_the_cards_buttons_from_the_listed_files(client, scripted):
    """Bug class: the buttons made before the chat's files were read, or from a name the assistant wrote in its text (`hidden.csv` is named, never listed), or missing on a card that arrived through the events part."""
    scripted.files = {"plot.png": PNG, "plot.csv": b"a,b\n1,2\n", "notes.txt": b"x"}
    scripted.events = [an_event("text", n=0, text="see hidden.csv and plot.py"), an_event("file", n=1, name="plot.png", size=3), an_event("file", n=2, name="hidden.png", size=3)]
    page = client.get(f"/chat/{CHAT_ID}").text
    part = client.get(f"/chat/{CHAT_ID}/events?offset=1", headers=HX).text
    for found in (hrefs(page), hrefs(part)):
        assert f"/chat/{CHAT_ID}/file/plot.csv" in found and f"/chat/{CHAT_ID}/table/plot.csv" in found
        assert not [href for href in found if "hidden.csv" in href or href.endswith(".py")]
    assert scripted.ops().count("chat.files") == 2
    assert re.search(rf'<a class="tv" href="/chat/{CHAT_ID}/table/plot.csv">Table view</a>', page) and "/table/notes.txt" not in page


def table_page(client, scripted, data, name="t.csv"):
    scripted.files[name] = data
    return client.get(f"/chat/{CHAT_ID}/table/{name}")


def test_the_table_view_shows_cells_as_text_and_caps_rows_columns_and_bytes(client, scripted):
    """Bug class: a cell of the sandbox's file drawn as markup or with an invisible direction change, a table of any number of rows or columns, or a read of the whole file."""
    rows = ["h1,h2", f'"{HOSTILE.replace(chr(34), chr(34) * 2)}",a\u202eb'] + [f"{index},x" for index in range(600)]
    page = table_page(client, scripted, "\n".join(rows).encode("utf-8"))
    assert page.status_code == 200 and not any(mark in page.text for mark in RAW_MARKS)
    assert "&lt;script&gt;" in page.text and '<span class="escape" title="made visible">\\u202e</span>' in page.text
    assert page.text.count("<tr>") == 1 + chat.TABLE_ROWS_MAX and "The first 500 rows under the header are shown." in page.text
    wide = table_page(client, scripted, (",".join(f"c{index}" for index in range(60)) + "\n").encode())
    assert wide.text.count("<th>") == chat.TABLE_COLUMNS_MAX and "c49" in wide.text and "c50" not in wide.text
    big = table_page(client, scripted, b"a,b\n" + b"1,2\n" * 100000)
    assert big.status_code == 200 and "only its first" in big.text
    assert scripted.requests[-1]["limit"] == chat.TABLE_BYTES and big.text.count("<tr>") == 1 + chat.TABLE_ROWS_MAX


@pytest.mark.parametrize("data, line", [(PNG + b"\x00\x00", chat.TABLE_NOT_CSV), (b'a,"b\n1,2\n', chat.TABLE_NOT_CSV), (b"x" * 200000, chat.TABLE_NOT_CSV), (b"", chat.TABLE_EMPTY)])
def test_a_file_that_is_not_csv_is_a_sentence(client, scripted, data, line):
    """Bug class: a binary file, a quote that never closes, a field over `csv`'s limit or an empty file making the table view an error of the page."""
    page = table_page(client, scripted, data)
    assert page.status_code == 200 and line in main_text(page.text) and "<table" not in page.text


def test_the_table_view_is_only_for_a_csv_name_of_the_runners_rule(client, scripted):
    """Bug class: the table view reading any file of the sandbox, or a name outside the runner's rule sent to it."""
    for path in ("table/a.png", "table/..%2Fb.csv", "table/%2E%2E", "table/%E2%80%AEa.csv"):
        assert client.get(f"/chat/{CHAT_ID}/{path}").status_code == 404
    assert scripted.requests == []


# ── m. chart specs ──────────────────────────────────────────────────────────

ALTAIR = (Path(__file__).resolve().parents[1] / "fixtures" / "webui" / "altair_line.vl.json").read_bytes()


def spec_boxes(page):
    return re.findall(r'<div class="vl-chart" data-vl-spec="([^"]*)"></div>', page)


def test_a_cards_chart_is_the_spec_of_its_stem_in_the_chats_list_and_drawn_once():
    """Bug class: a chart guessed from the image's name (a spec the folder does not hold), the same chart drawn twice (in the image's card and in the spec's own), or a spec without an image of its stem never drawn."""
    stems = chat.stems_of(["plot.png", "plot.vl.json", "plot.json", "alone.vl.json", "plotx.vl.json"])
    assert chat.spec_href(CHAT_ID, "plot.png", stems) == f"/chat/{CHAT_ID}/spec/plot.vl.json"
    assert chat.spec_href(CHAT_ID, "plot.vl.json", stems) is None and chat.spec_href(CHAT_ID, "alone.vl.json", stems) == f"/chat/{CHAT_ID}/spec/alone.vl.json"
    assert chat.spec_href(CHAT_ID, "plotx.png", stems) == f"/chat/{CHAT_ID}/spec/plotx.vl.json" and chat.spec_href(CHAT_ID, "other.png", stems) is None
    assert chat.spec_href(CHAT_ID, "plot.png", None) is None and chat.spec_href(CHAT_ID, "plot.vl.json", None) == f"/chat/{CHAT_ID}/spec/plot.vl.json"
    assert [button["label"] for button in chat.file_buttons(CHAT_ID, "plot.png", stems)] == ["PNG", "Data (JSON)", "Spec (JSON)"]
    assert [button["label"] for button in chat.file_buttons(CHAT_ID, "plot.vl.json", stems)] == ["Spec (JSON)"]


def test_the_page_and_the_events_part_draw_the_spec_container_only_for_a_listed_spec(client, scripted):
    """Bug class: a chart container (and a request for a spec) on a card whose chat holds no spec, or a spec's address made from text the assistant wrote."""
    scripted.events = [an_event("text", n=0, text="see b.vl.json"), an_event("file", n=1, name="a.png", size=3), an_event("file", n=2, name="a.vl.json", size=3), an_event("file", n=3, name="b.png", size=3)]
    scripted.files = {"a.png": PNG, "a.vl.json": ALTAIR, "b.png": PNG}
    page = client.get(f"/chat/{CHAT_ID}").text
    part = client.get(f"/chat/{CHAT_ID}/events?offset=1", headers=HX).text
    for found in (page, part):
        assert spec_boxes(found) == [f"/chat/{CHAT_ID}/spec/a.vl.json"]
        assert f'<a class="btn sm" href="/chat/{CHAT_ID}/file/a.vl.json">Spec (JSON)</a>' in found
    del scripted.files["a.vl.json"]
    assert spec_boxes(client.get(f"/chat/{CHAT_ID}").text) == []
    assert '<script src="/static/plots.js" defer></script>' in page and '<script src="/static/vendor/vega-embed.min.js" defer></script>' in page
    assert "plots.js" not in client.get("/").text


def test_the_spec_route_sends_the_checked_spec_with_the_fixed_headers(client, scripted):
    """Bug class: the sandbox's bytes passed on as they were (not the spec the check accepted), served with a type the browser would render, or without the sandbox policy."""
    scripted.files = {"a.vl.json": ALTAIR}
    answer = client.get(f"/chat/{CHAT_ID}/spec/a.vl.json")
    assert answer.status_code == 200 and answer.headers["content-type"] == "application/json; charset=utf-8" and answer.json() == json.loads(ALTAIR)
    assert answer.headers.get_list("content-security-policy") == ["sandbox", CSP] and answer.headers["x-content-type-options"] == "nosniff" and answer.headers["cache-control"] == "no-store"
    assert answer.content.isascii() and answer.content != ALTAIR


def test_a_refused_spec_is_its_reason_and_a_large_one_is_not_read_whole(client, scripted):
    """Bug class: a spec that loads data from an address handed to the browser, or a spec file of any size read into memory before it is refused."""
    spec = json.loads(ALTAIR)
    spec["layer"][1]["data"] = {"url": f"/chat/{CHAT_ID}/file/secret.csv"}
    scripted.files = {"a.vl.json": json.dumps(spec).encode(), "big.vl.json": b" " * (3 * assistant.PIECE_BYTES + 1) + ALTAIR}
    answer = client.get(f"/chat/{CHAT_ID}/spec/a.vl.json")
    assert answer.status_code == 422 and answer.text == "it loads data from an address" and answer.headers["content-type"].startswith("text/plain")
    scripted.requests.clear()
    answer = client.get(f"/chat/{CHAT_ID}/spec/big.vl.json")
    assert answer.status_code == 200 and answer.json() == json.loads(ALTAIR)
    scripted.files["big.vl.json"] = b" " * (4 * assistant.PIECE_BYTES + 1) + ALTAIR
    scripted.requests.clear()
    answer = client.get(f"/chat/{CHAT_ID}/spec/big.vl.json")
    assert answer.status_code == 422 and answer.text == "its file is larger than 2 MiB" and scripted.ops() == ["chat.file"]


def test_the_spec_route_takes_only_a_spec_name_of_the_runners_rule_and_asks_the_broker_each_time(client, scripted, broker):
    """Bug class: the spec route reading any file of the sandbox (a CSV, a path that climbs out), or let through on an earlier answer of the broker."""
    for path in ("spec/a.json", "spec/a.png", "spec/a%2F..%2Fb.vl.json", "spec/%2E%2E", "spec/%E2%80%AEa.vl.json"):
        assert client.get(f"/chat/{CHAT_ID}/{path}").status_code == 404
    assert scripted.requests == []
    assert client.get(f"/chat/{CHAT_ID}/spec/missing.vl.json").status_code == 404
    scripted.files = {"a.vl.json": ALTAIR}
    before = len([request for request in broker.requests if request["verb"] == "audit.list"])
    for _ in range(2):
        assert client.get(f"/chat/{CHAT_ID}/spec/a.vl.json").status_code == 200
    assert len([request for request in broker.requests if request["verb"] == "audit.list"]) == before + 2


# ── n. files in subfolders ──────────────────────────────────────────────────


def test_a_file_in_a_subfolder_is_listed_by_its_path_with_its_link_and_the_unlisted_note(client, scripted):
    """Bug class: a file the assistant wrote to a subfolder (a chart's script) missing from the Files list, its link quoting the slash away, or the files the runner left out of subfolders by type not said."""
    scripted.files = {"charts/vol.png": PNG, "charts/vol.py": b"print(1)", "charts/my data.csv": b"a\n1\n"}
    page = client.get(f"/chat/{CHAT_ID}").text
    found = hrefs(page)
    assert f"/chat/{CHAT_ID}/file/charts/vol.py" in found and f"/chat/{CHAT_ID}/file/charts/my%20data.csv" in found and f"/chat/{CHAT_ID}/table/charts/my%20data.csv" in found
    assert ">charts/vol.py<" in page and "not listed" not in page
    labels = [(button["label"], button["href"]) for button in chat.file_buttons(CHAT_ID, "charts/vol.png", chat.stems_of(scripted.files))]
    assert ("Code", f"/chat/{CHAT_ID}/file/charts/vol.py") in labels
    scripted.answers["chat.files"] = {"files": [], "left_out": 0, "unlisted": 3, "more": False}
    assert "3 files in subfolders are not listed: only .py, .png, .jpg, .svg, .json, .csv, .txt and .md files are listed there." in client.get(f"/chat/{CHAT_ID}").text


def test_the_file_routes_take_a_path_in_a_subfolder(client, scripted):
    """Bug class: the download, image, spec and table routes refusing a file the Files list links in a subfolder, or the download named by its whole path."""
    scripted.files = {"out/my plot.bin": b"made-up bytes", "charts/a.png": PNG, "charts/a.vl.json": ALTAIR, "data/t.csv": b"a,b\n1,2\n"}
    answer = client.get(f"/chat/{CHAT_ID}/file/out/my%20plot.bin")
    assert answer.status_code == 200 and answer.content == b"made-up bytes" and answer.headers["content-disposition"] == 'attachment; filename="my_plot.bin"'
    assert client.get(f"/chat/{CHAT_ID}/image/charts/a.png").content == PNG
    assert client.get(f"/chat/{CHAT_ID}/spec/charts/a.vl.json").json() == json.loads(ALTAIR)
    assert client.get(f"/chat/{CHAT_ID}/table/data/t.csv").text.count("<tr>") == 2
    assert [request["name"] for request in scripted.requests if request["op"] == "chat.file"] == ["out/my plot.bin", "charts/a.png", "charts/a.vl.json", "data/t.csv"]


@pytest.mark.parametrize("path", ["file/a%2F..%2Fb.png", "file/..%2Fx", "file/%2E%2E%2Fx", "file/%2Fetc%2Fpasswd", "image/a%2F%2Fb.png", "image/a//b.png", "spec/..%2Fa.vl.json", "table/x%2F%2E%2E%2Ft.csv", "file/a/b/c/d/e.py", "file/a/"])
def test_a_path_that_climbs_out_or_breaks_the_rule_is_not_found_and_sends_nothing(client, scripted, path):
    """Bug class (security): an encoded `..` or slash, decoded before routing, taking a file route out of the chat's folder or passing an empty or too deep path to the runner."""
    assert client.get(f"/chat/{CHAT_ID}/{path}").status_code == 404 and scripted.requests == []


# ── l. the right column's queries ───────────────────────────────────────────


def jid(index):
    return f"sr_{index:08x}"


def tool_result(n, text):
    return an_event("tool_result", n=n, text=text, is_error=False)


def queries(client):
    return client.get(f"/chat/{CHAT_ID}/queries", headers=HX)


def test_the_queries_part_asks_the_broker_before_the_runner_and_the_scheduler(client, scripted, broker, door, clock):
    """Bug class: the scheduler (or the chat's events) asked for a session the broker has ended or could not confirm, as a Queue route that trusts the web UI's own session table would."""
    door.add("pending", a_job(jid(1), submitted_by="assistant"))
    scripted.events = [tool_result(0, f"queued {jid(1)}")]
    assert jid(1) in queries(client).text
    asked = [entry for entry in broker.timeline if entry[0] == "door" or entry[2] == "audit.list"]
    assert asked[0] == ("broker", "call", "audit.list") and ("door", "job") in asked
    door.requests.clear()
    scripted.requests.clear()
    clock.now += queue.SESSION_CHECK_SECONDS + 1
    broker.answers["audit.list"] = broker_client.BrokerUnreachable(broker_client.NO_SOCKET)
    assert words.REASON_SENTENCES[broker_client.NO_SOCKET] in html.unescape(queries(client).text)
    broker.answers["audit.list"] = broker_client.BrokerError("session_ended", "x")
    ended = queries(client)
    assert ended.status_code == 204 and ended.headers["hx-redirect"] == "/login?why=ended"
    assert door.requests == [] and scripted.requests == []


def test_the_queries_come_only_from_this_chats_tool_calls_newest_first_and_capped(client, scripted, door):
    """Bug class: job ids taken from the operator's question or the assistant's prose (any id typed into the chat would be looked up), from a longer word, out of order, or without bound."""
    for index in range(30):
        door.add("pending", a_job(jid(index), submitted_by="assistant"))
    scripted.events = [an_event("user", n=0, text=jid(29)), an_event("text", n=1, text=jid(28)), an_event("tool", n=2, name="mcp__jobs__query_status", input=f'{{"job_id": "{jid(0)}"}}')]
    scripted.events += [tool_result(3 + index, f"submitted {jid(index)} and x{jid(27)} {jid(26)}0") for index in range(1, 25)]
    scripted.events.append(tool_result(30, f"again {jid(3)}"))
    asked = []
    door.answers["job"] = lambda document: (asked.append(document["job_id"]), {"phase": "pending", "job": door.jobs[document["job_id"]][1], "digest": "0" * 64})[1]
    text = main_text(queries(client).text)
    assert asked == [jid(3)] + [jid(index) for index in range(24, 5, -1) if index != 3]
    assert len(asked) == chat.QUERIES_MAX and jid(29) not in text and jid(28) not in text and "waiting for approval" in text


def test_only_the_assistants_jobs_are_listed_and_a_failure_stays_in_its_row(client, scripted, door, make_app, make_client, chat_folder):
    """Bug class: a job of the operator or of a stranger listed as the chat's, an unknown id shown, one failing lookup taking the whole list down, or a scheduler not set up asked anyway."""
    door.add("running", a_job(jid(1), submitted_by="assistant"))
    door.add("history", a_job(jid(2), submitted_by="assistant", status="done"))
    door.add("pending", a_job(jid(3), submitted_by="web-1"))
    door.add("pending", a_job(jid(4), submitted_by="assistant"))
    door.too_large.add(jid(4))
    scripted.events = [tool_result(index, jid(index)) for index in range(1, 6)]
    part = queries(client).text
    rows = re.findall(r'<a class="thumb row" href="/queue/(\w+)"><span class="dot ?(\w*)"></span><span class="mono">\w+</span><span class="right muted">([^<]+) →</span></a>', part)
    assert rows == [(jid(4), "", chat.QUERY_NOT_READ), (jid(2), "ok", "finished"), (jid(1), "run", "running")]
    unset = make_client(make_app(assistant=assistant.Assistant(scripted.path), scheduler=make_scheduler_none()))
    log_in(unset)
    scripted.requests.clear()
    assert chat.QUERIES_NO_QUEUE in unset.get(f"/chat/{CHAT_ID}/queries", headers=HX).text and scripted.requests == []


def make_scheduler_none():
    from social_data_pipeline.webui import scheduler

    return scheduler.Scheduler(None, None)


def test_the_conversation_draws_the_queries_lazily_or_says_the_queue_is_not_attached(client, scripted, door):
    """Bug class: the conversation page asking the scheduler while it renders (a slow scheduler would slow every chat), or a placeholder that asks for nothing when the sandbox has no queue."""
    scripted.answers["hello"] = {"version": 1, "backend": "claude-code", "databases": [], "queue": True}
    page = client.get(f"/chat/{CHAT_ID}").text
    assert f'hx-get="/chat/{CHAT_ID}/queries" hx-trigger="load"' in page and door.requests == []
    scripted.answers["hello"] = {"version": 1, "backend": "claude-code", "databases": [], "queue": False}
    page = client.get(f"/chat/{CHAT_ID}").text
    assert chat.QUERIES_DETACHED in main_text(page) and "/queries" not in page
