"""Tests for a chat's generated title in the Chat window (`webui.chat` `event_model`, `last_title`, `turn_totals`, `totals_rows`; `_chat_events.html`, `chat.html`): the runner's `title {text, cost_usd}` event, through the whole application with the stand-in broker and the scripted runner. What `app.js` does with the out-of-band title (the field left alone while it has focus or was edited) cannot run here: `tests/e2e/manual/check_chat_ui_browser.py` checks it in a real browser.

The contracts pinned here, each test naming the bug class it catches:

- a `title` event is checked for its exact shape (keys, a text of 1 to 80 characters UTF-8 can hold, a cost that is null or a finite number from 0 to the runner's bound, never a flag): any other is the one "not understood" line, as a malformed `end` is; a good one draws nothing in the conversation;
- its cost is added to the chat's Cost with the note "(includes the title)", it is no turn, no error and no time; a null cost changes nothing; a malformed one is counted as not read, with its own note;
- the events part that carries a good `title` event also carries, out of band, the chat list's label of the chat and the element `app.js` copies into the title's field, both escaped (invisible characters as escapes), only the last title of the part; a part without one carries neither, and the page has both targets.

Every special character in this file is written as an escape.
"""

from __future__ import annotations

import html
import html.parser
import re

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette.testclient")

from test_webui_pages import inline_code
from webui_helpers import CHAT_ID, Raw, an_event, log_in, main_html

from social_data_pipeline.webui import assistant, chat, statements

MARK = "made-up-title-mark-5c1e"
HX = {"hx-request": "true"}
HOSTILE = "<b>x</b> \"q\" & \u200b"


@pytest.fixture
def app(make_app, scripted):
    return make_app(assistant=assistant.Assistant(scripted.path))


@pytest.fixture
def client(app, make_client):
    made = make_client(app)
    assert log_in(made).status_code == 303
    return made


def title(n=1, text=MARK, cost_usd=0.0125, **extra):
    return an_event("title", n=n, text=text, cost_usd=cost_usd, **extra)


def end(n=0, cost_usd=0.5, duration_ms=1500, is_error=False):
    return an_event("end", n=n, is_error=is_error, cost_usd=cost_usd, duration_ms=duration_ms, turns=2)


def main_text(page):
    return " ".join(html.unescape(re.sub(r"<[^>]+>", " ", main_html(page))).split())


# ── the event's shape ──


@pytest.mark.parametrize("cost", [0.0125, 0, None, 1000])
def test_a_good_title_event_draws_nothing(client, scripted, cost):
    """Bug class: the generated title shown as a line of the conversation, or a well-formed title event taken for one the page does not understand."""
    assert chat.event_model(title(cost_usd=cost), CHAT_ID)["kind"] == "title"
    scripted.events = [an_event("user", n=0, text="before"), title(cost_usd=cost), an_event("user", n=2, text="after")]
    page = client.get(f"/chat/{CHAT_ID}").text
    text = main_text(page)
    assert "before" in text and "after" in text and chat.NOT_UNDERSTOOD_EVENT not in text and MARK not in page


BAD_TITLES = [
    title(extra=1),
    {key: value for key, value in title().items() if key != "cost_usd"},
    {key: value for key, value in title().items() if key != "text"},
    title(text="x" * 81),
    title(text=""),
    title(text=[MARK]),
    title(text=None),
    title(cost_usd=True),
    title(cost_usd=False),
    title(cost_usd=-0.01),
    title(cost_usd=float("nan")),
    title(cost_usd=float("inf")),
    title(cost_usd=10 ** 400),
    title(cost_usd=1000.5),
    title(cost_usd="0.01"),
    an_event("title", n=1),
]


@pytest.mark.parametrize("event", BAD_TITLES)
def test_a_title_event_of_another_shape_is_one_line(client, scripted, event):
    """Bug class: a title event of another shape (a hand-written log, a hostile runner) taken in part, raising, or shown other than as an end of another shape is."""
    assert chat.event_model(event, CHAT_ID)["kind"] == "unknown"
    assert chat.turn_totals([event, end(2)])["titles_unread"] == 1
    if event.get("cost_usd") != event.get("cost_usd") or event.get("cost_usd") == float("inf"):
        # Not JSON: the runner's reply that would carry it is refused whole (test_webui_chat), so only the model is checked here.
        return
    scripted.events = [an_event("user", n=0, text="before"), event, an_event("user", n=2, text="after")]
    page = client.get(f"/chat/{CHAT_ID}").text
    assert chat.NOT_UNDERSTOOD_EVENT in main_text(page) and "before" in page and "after" in page and MARK not in page


def test_a_title_with_a_lone_surrogate_is_one_line(client, scripted):
    """Bug class: a title UTF-8 cannot hold (escaped in the JSON the sandbox wrote) taken, or breaking the page's encoding."""
    scripted.answers["chat.events"] = Raw('{"ok": true, "result": {"events": [{"n": 0, "t": 1, "type": "title", "text": "%s\\ud800", "cost_usd": null}], "next": 1, "running": false}}' % MARK)
    page = client.get(f"/chat/{CHAT_ID}").text
    assert MARK not in page and chat.NOT_UNDERSTOOD_EVENT in main_text(page)
    part = client.get(f"/chat/{CHAT_ID}/events?offset=0", headers=HX).text
    assert MARK not in part and "chat-title-next" not in part


# ── the totals ──


def test_a_title_cost_is_added_to_the_cost_with_its_note():
    """Bug class: the title's cost left out of the chat's Cost (the maintainer's answer: it counts), counted as a turn, an error or a time, or added without saying so."""
    totals = chat.turn_totals([an_event("user", n=0, text="q"), title(n=1, cost_usd=0.0125), end(2, cost_usd=0.25, duration_ms=1000, is_error=True)])
    assert totals["turns"] == 1 and totals["errors"] == 1 and totals["ms"] == 1000 and totals["cost"] == 0.2625 and totals["titles"] == 1
    assert chat.totals_rows(totals) == [("Turns", "1, 1 ended with an error"), ("Time", "1.0 s"), ("Cost", "0.2625 USD (includes the title)")]


def test_a_title_without_a_cost_changes_nothing():
    """Bug class: an OpenAI-compatible title (no cost) shown as a cost of the title, or making the turns' cost "not reported"."""
    assert chat.totals_rows(chat.turn_totals([title(cost_usd=None), end(2, cost_usd=0.25)])) == [("Turns", "1"), ("Time", "1.5 s"), ("Cost", "0.2500 USD")]
    assert chat.totals_rows(chat.turn_totals([title(cost_usd=None), end(2, cost_usd=None)]))[2] == ("Cost", "not reported by the backend")


def test_a_title_cost_beside_turns_without_one_is_the_known_cost_with_both_notes():
    """Bug class: a known cost hidden as "not reported by the backend", or shown as the whole chat's cost when its turns reported none."""
    rows = chat.totals_rows(chat.turn_totals([title(cost_usd=0.01), end(2, cost_usd=None), end(3, cost_usd=None)]))
    assert rows[2] == ("Cost", "0.0100 USD (not reported for 2 turns) (includes the title)")


def test_a_title_before_any_end_is_no_turn():
    """Bug class: the totals drawn as one turn (or as a cost with no turn) while the first turn still runs and only the title has come."""
    totals = chat.turn_totals([an_event("user", n=0, text="q"), title()])
    assert totals["turns"] == 0 and chat.totals_rows(totals) == []


def test_the_totals_part_counts_a_malformed_title_as_not_read(client, scripted):
    """Bug class: a malformed title event raising in the sum, poisoning it, or dropped without a word."""
    scripted.events = [an_event("user", n=0, text="q"), title(n=1, cost_usd=-1), title(n=2, cost_usd=True), title(n=3, cost_usd=0.5), end(4, cost_usd=0.25)]
    text = main_text(client.get(f"/chat/{CHAT_ID}/totals", headers=HX).text)
    assert "Turns 1" in text and "Cost 0.7500 USD (includes the title)" in text
    assert "2 title events could not be read and their cost is not counted." in text and "end of a turn" not in text


# ── the events part ──


class Elements(html.parser.HTMLParser):
    """The elements of a page or part by their id: {id: (tag, attributes, the text inside)}."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.found, self.open = {}, []

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        self.open.append([tag, attributes, []])

    def handle_data(self, data):
        for entry in self.open:
            entry[2].append(data)

    def handle_endtag(self, tag):
        while self.open:
            name, attributes, text = self.open.pop()
            if attributes.get("id"):
                self.found[attributes["id"]] = (name, attributes, "".join(text))
            if name == tag:
                break


def oob(part):
    """The out-of-band elements of a part but the controls (always there once `running` is known): [(tag, id, attributes, the text inside)]."""
    parser = Elements()
    parser.feed(part)
    return [(tag, ident, attributes, text) for ident, (tag, attributes, text) in parser.found.items() if "hx-swap-oob" in attributes and ident != "chat-controls"]


def test_the_events_part_carries_the_last_title_out_of_band_escaped(client, scripted):
    """Bug class: a generated title that reaches an open page only at its next load, the runner's text drawn as markup or with its invisible characters hidden, or an earlier title of the part winning over the last."""
    scripted.events = [an_event("user", n=0, text="q"), title(n=1, text="an earlier one"), title(n=2, text=HOSTILE)]
    part = client.get(f"/chat/{CHAT_ID}/events?offset=0", headers=HX).text
    assert inline_code(part) == [] and "<b>" not in part and "an earlier one" not in part
    found = oob(part)
    assert sorted((tag, ident, attributes["hx-swap-oob"]) for tag, ident, attributes, _ in found) == [("span", f"chat-label-{CHAT_ID}", "true"), ("span", "chat-title-next", "true")]
    label = re.search(r'<span class="lbl" id="chat-label-%s" hx-swap-oob="true">(.*?)</span>(?=<span id="chat-title-next")' % CHAT_ID, part).group(1)
    assert '<span class="escape" title="made visible">' in label and "&lt;b&gt;x&lt;/b&gt;" in label
    assert html.unescape(re.sub(r"<[^>]+>", "", label)) == statements.shown_text(HOSTILE)
    field = re.search(r'<span id="chat-title-next" hidden data-title="([^"]*)" hx-swap-oob="true"></span>', part).group(1)
    assert html.unescape(field) == statements.shown_text(HOSTILE) and "\u200b" not in html.unescape(field) and "&lt;b&gt;" in field


@pytest.mark.parametrize("events", [
    [an_event("user", n=0, text="q")],
    [an_event("user", n=0, text="q"), title(n=1, text="")],
    [an_event("user", n=0, text="q"), title(n=1, cost_usd=True)],
])
def test_an_events_part_without_a_good_title_carries_neither(client, scripted, events):
    """Bug class: the label or the field rewritten by every poll (an empty title, a stale one), or by a title the page does not understand."""
    scripted.events = events
    part = client.get(f"/chat/{CHAT_ID}/events?offset=0", headers=HX).text
    assert oob(part) == [] and "chat-label-" not in part and "chat-title-next" not in part


def test_the_page_holds_both_targets_once(client, scripted):
    """Bug class: an out-of-band title with no element to replace on the page (htmx logs an error and nothing changes), or two elements of one id."""
    scripted.chats = [{"id": CHAT_ID, "title": "made-up question", "created": 1795000000.0, "running": False}, {"id": "c_fedcba987654", "title": "another", "created": 1794000000.0, "running": False}]
    page = client.get(f"/chat/{CHAT_ID}").text
    ids = re.findall(r'\bid="([^"]+)"', page)
    assert len(ids) == len(set(ids))
    assert f'<span class="lbl" id="chat-label-{CHAT_ID}">made-up question</span>' in page and '<span class="lbl" id="chat-label-c_fedcba987654">another</span>' in page
    assert re.search(r'<span id="chat-title-next" hidden></span>', page)
