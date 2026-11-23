"""Tests for the settings box of the top bar, the themes, and the Chat window's right column "This chat" (`webui.chat` `turn_totals`, `totals_rows`, `totals_model`, `GET /chat/<id>/totals`; `_settings.html`, `_chat_model.html`, `_chat_totals.html`, `chat.html`), through the whole application with the stand-in broker and the scripted runner. What the page's script does (Enter, Shift+Enter, an input method's Enter, the box, the columns) cannot run here: `tests/e2e/manual/check_chat_ui_browser.py` checks it in a real browser.

The contracts pinned here, each test naming the bug class it catches:

- the turn totals add up only what the backend reported: an end of another shape, a missing, odd or huge duration or cost (a hostile runner can write anything) is "not reported" or "not counted", never an error and never a wrong sum;
- the totals part reads the whole chat from its start (pages of the runner, a bound on the events read), asks the broker first, and is not drawn when the events could not be read;
- the markup: the gear is a button with `aria-expanded` that controls a `role="dialog"` box, hidden by the attribute and not by an inline style; the right column's two buttons control it by id; the sandbox is one line that opens on its facts; `theme.js` runs before the page is drawn.

Every special character in this file is written as an escape.
"""

from __future__ import annotations

import html
import re

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette.testclient")

from test_webui_pages import inline_code
from webui_helpers import CHAT_ID, an_event, log_in, main_html

from social_data_pipeline.control import client as broker_client
from social_data_pipeline.webui import assistant, chat, queue, words

HX = {"hx-request": "true"}
HELLO = {"version": 1, "backend": "claude-code", "databases": ["starrocks", "mongo"], "queue": True}


@pytest.fixture
def app(make_app, scripted):
    return make_app(assistant=assistant.Assistant(scripted.path))


@pytest.fixture
def client(app, make_client):
    made = make_client(app)
    assert log_in(made).status_code == 303
    return made


def end(n=0, is_error=False, cost_usd=0.5, duration_ms=1500, turns=2, **extra):
    return an_event("end", n=n, is_error=is_error, cost_usd=cost_usd, duration_ms=duration_ms, turns=turns, **extra)


def text_of(part):
    return " ".join(html.unescape(re.sub(r"<[^>]+>", " ", part)).split())


# ── the sums ──


def test_the_totals_add_up_only_what_the_backend_reported():
    """Bug class: a hostile or odd end of a turn (a NaN, a flag, a negative or a 400-digit number, a string, a missing or extra field) raising in the sum, poisoning it (NaN), or counted as a turn when it is not an end the page can read."""
    events = [
        end(0, cost_usd=0.25, duration_ms=1000),
        end(1, is_error=True, cost_usd=1, duration_ms=2500),
        # Ends the page reads whose cost or duration it cannot use: a turn, its value "not reported".
        end(2, cost_usd=None, duration_ms=None, turns=None),
        end(3, cost_usd=float("nan"), duration_ms=10 ** 13),
        end(4, cost_usd=float("inf"), duration_ms=2 ** 62),
        end(5, cost_usd=-1.0, duration_ms=None),
        end(6, cost_usd=10 ** 400, duration_ms=0),
        end(7, cost_usd=chat.COST_MAX * 2, duration_ms=500),
        # Not ends the page can read: none of them is a turn.
        end(8, is_error="yes"), end(9, cost_usd=True), end(10, cost_usd="0.5"), end(11, extra="x"), end(12, duration_ms="1"),
        end(13, duration_ms=-5), end(14, duration_ms=True), end(15, duration_ms=1.5), end(16, duration_ms=10 ** 400), end(17, turns=-1),
        {"n": 18, "t": None, "type": "end"}, an_event("end", n=19), {"type": "end"},
        # Other events and values: left alone.
        an_event("text", n=20, text="cost_usd 99"), "end", None, 7, [end(21)],
    ]
    totals = chat.turn_totals(events)
    assert totals == {"turns": 8, "errors": 1, "ms": 4000, "ms_missing": 4, "cost": 1.25, "cost_missing": 6, "unread": 13}
    assert chat.turn_totals([end(22, cost_usd=0.5, duration_ms=500)], totals) is totals and totals["turns"] == 9 and totals["cost"] == 1.75


@pytest.mark.parametrize(("ms", "text"), [(0, "0.0 s"), (8400, "8.4 s"), (59_949, "59.9 s"), (60_000, "1 min 0 s"), (72_500, "1 min 12 s"), (3_599_999, "59 min 59 s"), (3_600_000, "1 h 00 min"), (2000 * 10 ** 12, "555,555,555 h 33 min")])
def test_a_duration_is_said_in_seconds_minutes_or_hours(ms, text):
    """Bug class: a long chat's time shown as thousands of seconds, or the largest sum the bounds allow (2,000 turns of the longest duration) raising."""
    assert chat.duration_text(ms) == text


def test_the_rows_say_what_was_not_reported():
    """Bug class: a cost of 0.0000 USD shown for a backend that reports none (an OpenAI-compatible endpoint), or a partial sum shown as the whole."""
    assert chat.totals_rows(chat.turn_totals([])) == []
    rows = chat.totals_rows(chat.turn_totals([end(0, cost_usd=None, duration_ms=None)] * 2))
    assert rows == [("Turns", "2"), ("Time", "not reported"), ("Cost", "not reported by the backend")]
    rows = chat.totals_rows(chat.turn_totals([end(0, is_error=True, cost_usd=0.12, duration_ms=61_000), end(1, cost_usd=None, duration_ms=1000)]))
    assert rows == [("Turns", "2, 1 ended with an error"), ("Time", "1 min 2 s"), ("Cost", "0.1200 USD (not reported for 1 turn)")]


# ── the part ──


def totals(client):
    return client.get(f"/chat/{CHAT_ID}/totals", headers=HX)


def test_the_totals_part_adds_up_the_whole_chat_from_its_start(client, scripted):
    """Bug class: totals of the first page of events only (a long chat's later turns left out), or of the view's own start (`?from=N`), or reading a chat without bound."""
    scripted.events = [end(index, cost_usd=0.01, duration_ms=1000) if index % 10 == 9 else an_event("text", n=index, text="x") for index in range(450)]
    part = totals(client)
    assert part.status_code == 200 and inline_code(part.text) == []
    assert [(request["offset"], request["limit"]) for request in scripted.requests if request.get("op") == "chat.events"] == [(0, 200), (200, 200), (400, 200)]
    text = text_of(part.text)
    assert "Turns 45 Time 45.0 s Cost 0.4500 USD" in text and chat.TOTALS_SEARCHED not in text
    assert f'hx-trigger="{chat.TOTALS_AGAIN}"' in part.text and 'hx-trigger="load' not in part.text
    scripted.events = [end(index, cost_usd=None) for index in range(2300)]
    scripted.requests.clear()
    text = text_of(totals(client).text)
    assert "Turns 2,000" in text and "Cost not reported by the backend" in text and chat.TOTALS_SEARCHED in text
    assert sum(request["limit"] for request in scripted.requests if request.get("op") == "chat.events") == chat.TOTALS_EVENTS_MAX
    scripted.events = [an_event("user", n=0, text="q"), end(1, is_error="<b>x</b>")]
    text = text_of(totals(client).text)
    assert chat.TOTALS_NONE in text and "1 end of a turn could not be read and is not counted." in text and "<b>" not in text


def test_the_totals_part_asks_the_broker_first_and_confines_a_failure(client, scripted, broker, clock):
    """Bug class: the chat's events read for a session the broker has ended or could not confirm; a runner failure breaking the column; the route answering HEAD."""
    scripted.answers["chat.events"] = {"ok": False, "error": {"code": "not_found", "message": "<b>x</b>"}}
    part = totals(client).text
    assert words.CHAT_CODE_SENTENCES["not_found"] in html.unescape(part) and "<b>x" not in part
    del scripted.answers["chat.events"]
    scripted.requests.clear()
    clock.now += queue.SESSION_CHECK_SECONDS + 1
    broker.answers["audit.list"] = broker_client.BrokerUnreachable(broker_client.NO_SOCKET)
    assert words.REASON_SENTENCES[broker_client.NO_SOCKET] in html.unescape(totals(client).text)
    broker.answers["audit.list"] = broker_client.BrokerError("session_ended", "x")
    ended = totals(client)
    assert ended.status_code == 204 and ended.headers["hx-redirect"] == "/login?why=ended"
    assert scripted.requests == []
    assert client.head(f"/chat/{CHAT_ID}/totals").status_code == 405
    assert client.get("/chat/c_0123/totals").status_code == 404


def test_the_page_draws_the_totals_lazily_and_not_when_the_events_failed(client, scripted):
    """Bug class: the totals read while the page renders (a long chat would slow every page), or a placeholder that asks for totals the same failing read cannot give."""
    scripted.events = [end(0)]
    page = client.get(f"/chat/{CHAT_ID}").text
    assert f'hx-get="/chat/{CHAT_ID}/totals" hx-trigger="{chat.TOTALS_ASK}"' in page
    assert [request["offset"] for request in scripted.requests if request.get("op") == "chat.events"] == [0]
    scripted.answers["chat.events"] = {"ok": False, "error": {"code": "internal", "message": "x"}}
    page = client.get(f"/chat/{CHAT_ID}").text
    assert "/totals" not in page and "The turn totals are not known" in text_of(main_html(page))


# ── the markup ──


def test_the_settings_box_is_a_dialog_controlled_by_the_gear_on_every_page(client):
    """Bug class: a settings box hidden by an inline style (blocked by the policy, so always shown), a gear that is not a button or does not say whether the box is open, or a model choice other than the ones the new-chat form understands."""
    for route in ("/", "/queue", "/chat", f"/chat/{CHAT_ID}"):
        page = client.get(route).text
        assert inline_code(page) == [], route
        gear = re.search(r'<button type="button" class="btn ghost sm gear" id="settings-button" aria-expanded="false" aria-controls="settings-box" aria-haspopup="dialog" aria-label="Settings"', page)
        box = re.search(r'<div class="settings-box" id="settings-box" role="dialog" aria-label="Settings" hidden>(.*?)\n  </div>', page, re.DOTALL)
        assert gear and box and page.index("settings-button") < page.index('<span class="pill">operator</span>'), route
    selects = dict(re.findall(r'data-setting="([^"]+)">(.*?)</select>', box.group(1), re.DOTALL))
    assert re.findall(r'<option value="([^"]*)"', selects["sdp.theme"]) == ["dark", "light", "system"]
    assert re.findall(r'<option value="([^"]*)"', selects["sdp.defaultModel"]) == ["", "opus", "sonnet", "haiku"]


def test_the_theme_is_drawn_before_the_page_and_light_is_a_palette_of_the_kit(client, app, make_client):
    """Bug class: the theme set by the deferred script only (the page flashes dark, then turns light), or a light palette that leaves dark form controls."""
    login = make_client(app).get("/login").text
    for page in (login, client.get("/").text):
        head = page[:page.index("</head>")]
        assert '<script src="/static/theme.js"></script>' in head and head.index("/static/theme.js") < head.index("/static/app.css")
    served = client.get("/static/theme.js")
    assert served.status_code == 200 and "sdp.theme" in served.text and "sdp.chatPanel" in served.text
    sheet = client.get("/static/app.css").text
    light = re.search(r':root\[data-theme="light"\] \{(.*?)\}', sheet, re.DOTALL).group(1)
    assert "color-scheme: light;" in light and "--page:" in light and "--ink:" in light


def test_the_right_column_is_this_chat_with_its_controls(client, scripted):
    """Bug class: a column that cannot be hidden or shown again (the controls missing, not buttons, or pointing at another id), a sandbox card that takes the column again instead of one line, the backend shown by its internal name, or the box without its keys."""
    scripted.answers["hello"] = HELLO
    page = client.get(f"/chat/{CHAT_ID}").text
    aside = re.search(r'<aside class="rightp" id="chat-panel" aria-label="This chat">(.*?)</aside>', page, re.DOTALL).group(1)
    buttons = re.findall(r'<button type="button" class="([^"]+)" data-chat-panel="(open|close)" aria-expanded="true" aria-controls="chat-panel"', page)
    assert buttons == [("btn sm panel-handle", "open"), ("btn ghost sm right", "close")]
    assert "<h4>This chat</h4>" in aside and "<dt>Backend</dt><dd>Claude Code</dd>" in aside
    summary = re.search(r'<details class="card sandbox">\s*<summary class="row">(.*?)</summary>(.*?)</details>', aside, re.DOTALL)
    assert text_of(summary.group(1)) == "Claude Code \u00b7 sandbox up \u00b7 read-only access to 2 databases"
    assert [text_of(term) for term in re.findall(r"<dt>(.*?)</dt>", summary.group(2))] == ["Can reach", "Cannot", "Reads", "Writes to"]
    assert aside.index("chat-totals") < aside.index("details") < aside.index("<h4>Files</h4>") < aside.index("<h4>Queries in the queue</h4>")
    assert 'placeholder="Ask about the data, or ask for a plot\u2026 (Enter to send, Shift+Enter for a new line)"' in page
    scripted.answers["hello"] = {**HELLO, "backend": "openai-compatible", "databases": ["mongo"]}
    page = client.get("/chat").text
    assert '<aside class="rightp" id="chat-panel" aria-label="Sandbox">' in page and "OpenAI-compatible \u00b7 sandbox up \u00b7 read-only access to 1 database<" in page
    assert "/totals" not in page and "<dt>Backend</dt>" not in page


def test_the_right_column_names_the_chats_model_as_text_beside_the_one_picker(client, scripted):
    """Bug class: a second field named `model` in the send form (the guard refuses a repeated name, so every question would be refused), a repeated id, or the right column naming another model than the header's picker."""
    scripted.answers["hello"] = {**HELLO, "models": ["opus", "sonnet", "haiku"], "model_default": None}
    scripted.chats[0]["model"] = "sonnet"
    page = client.get(f"/chat/{CHAT_ID}").text
    aside = re.search(r'<aside class="rightp" id="chat-panel"[^>]*>(.*?)</aside>', page, re.DOTALL).group(1)
    assert "<dt>Model</dt><dd>sonnet</dd>" in aside and "<select" not in aside and 'name="model"' not in aside
    assert page.count('id="chat-model"') == 1 and page.count('form="chat-send"') == 1
    assert re.search(r'<option value="sonnet" selected>sonnet</option>', page)
    scripted.chats[0]["model"] = None
    assert "<dt>Model</dt><dd>the backend's default</dd>" in client.get(f"/chat/{CHAT_ID}").text
    scripted.answers["hello"] = {**HELLO, "backend": "openai-compatible", "models": ["vendor/m-1"], "model_default": "vendor/m-1"}
    page = client.get(f"/chat/{CHAT_ID}").text
    assert "<dt>Backend</dt><dd>OpenAI-compatible</dd>" in page and "<dt>Model</dt><dd>vendor/m-1</dd>" in page and 'id="chat-model"' not in page
