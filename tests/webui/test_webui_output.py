"""Tests for the output view of a run (`webui.output` and the route `/runs/<run id>/output`), with the stand-in broker serving a log in pieces.

The contracts pinned here (property g), each test naming the bug class it catches: pieces are appended and the offsets follow them, so that what the view showed is the cleaned whole log; an escape sequence and a carriage-return line cut by a piece boundary are cleaned as one; Docker's tail replaces what is shown, also when a recorded run turns into one; the view stops when the run has ended and the offset did not move, after one empty answer of a lost or unreadable run, and after its size limit with a link that continues; the next request's trigger is in each answer (3 s, at once, 10 s); a bad run id, offset or view is not found before any broker call; the bounds of the per-view state; an htmx request after the session ended gets `HX-Redirect` and no page.

Every special character in this file is written as an escape.
"""

from __future__ import annotations

import html
import re
import threading

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette.testclient")

from webui_helpers import ORIGIN, RUN_ID, form_token, log_in, wait_until

from social_data_pipeline.control import client as broker_client
from social_data_pipeline.webui import output, words
from social_data_pipeline.webui.cleaning import clean_log_text

LIVE_ID = "live-0123456789ab"
HX = {"hx-request": "true"}


class LogServer:
    """Answers `run.logs` like the broker for a recorded run: pieces of at most `piece` bytes of `log` (ASCII), state `running` until `ended` is set (or, with `end_when_read`, once the whole log was served)."""

    def __init__(self, log, piece=7, end_when_read=True, state_when_ended="finished"):
        self.data = log.encode("ascii")
        self.piece = piece
        self.end_when_read = end_when_read
        self.ended = False
        self.state_when_ended = state_when_ended
        self.asked = []

    def __call__(self, fields):
        self.asked.append(fields)
        offset = fields["offset"]
        chunk = self.data[offset:offset + min(self.piece, fields["limit"])]
        if self.end_when_read and offset + len(chunk) >= len(self.data):
            self.ended = True
        state = self.state_when_ended if self.ended else "running"
        return {"data": {"id": fields["run_id"], "kind": "web", "state": state, "text": chunk.decode("ascii"), "next_offset": offset + len(chunk), "complete": self.ended}}


def tail_answer(text):
    return {"data": {"id": LIVE_ID, "kind": "terminal", "state": "running", "text": text, "next_offset": None, "complete": False}}


@pytest.fixture
def app(make_app):
    return make_app()


@pytest.fixture
def client(app, make_client):
    made = make_client(app)
    assert log_in(made).status_code == 303
    return made


def first_request(client, run_id, offset=None):
    page = client.get(f"/runs/{run_id}" + (f"?offset={offset}" if offset is not None else ""))
    assert page.status_code == 200
    found = re.search(r'<div class="output-next" hx-get="([^"]+)" hx-trigger="load" hx-swap="outerHTML">', page.text)
    return html.unescape(found.group(1))


def follow(client, href, steps_max=500):
    """Make the requests of a view as htmx would, from `href` on: (what the view shows at the end, the steps: each a dict of `trigger`, `end`, `retarget`, `pieces`, `href`)."""
    shown = ""
    steps = []
    while href and len(steps) < steps_max:
        response = client.get(href, headers=HX)
        assert response.status_code == 200, response.status_code
        retarget = response.headers.get("hx-retarget")
        if retarget:
            assert response.headers.get("hx-reswap") == "innerHTML"
            shown = ""
        pieces = [html.unescape(piece) for piece in re.findall(r'<div class="output-piece">(.*?)</div>', response.text, re.DOTALL)]
        shown += "".join(pieces)
        following = re.search(r'<div class="output-next" hx-get="([^"]+)" hx-trigger="([^"]+)" hx-swap="outerHTML">', response.text)
        end = re.search(r'<p class="output-end">(.*?)</p>', response.text, re.DOTALL)
        steps.append({"trigger": following.group(2) if following else None, "end": html.unescape(end.group(1)) if end else None, "retarget": retarget, "pieces": pieces, "href": href, "text": response.text})
        href = html.unescape(following.group(1)) if following else None
    return shown, steps


MIXED_LOG = "".join([
    "starting\n",
    "\x1b[1;32mgreen\x1b[0m and plain\n",
    "progress  0%\rprogress 50%\rprogress 100%\n",
    "\x1b]0;a window title\x07after the title\n",
    "tab\tand bell\x07\n",
    "\n",
    "last line without an end",
])


@pytest.mark.parametrize("piece", [1, 3, 7, 64])
def test_the_pieces_shown_are_the_cleaned_whole_log(client, broker, piece):
    """Bug class: pieces appended at the wrong offset (repeated or skipped bytes), a piece cleaned on its own so that an escape sequence or a carriage-return line cut by its end shows as litter, or the last line without a line end never shown."""
    server = LogServer(MIXED_LOG, piece=piece)
    broker.answers["run.logs"] = server
    shown, steps = follow(client, first_request(client, RUN_ID))
    assert shown == clean_log_text(MIXED_LOG)
    offsets = [fields["offset"] for fields in server.asked]
    assert offsets == sorted(offsets) and offsets[0] == 0 and offsets[-1] == len(MIXED_LOG)
    assert all(fields["limit"] == 65536 for fields in server.asked)
    for step in steps[:-1]:
        assert all(piece.endswith("\n") for piece in step["pieces"])
    assert steps[-1]["end"] == output.ENDED_LINE and steps[-1]["trigger"] is None


def test_an_escape_and_a_carriage_return_cut_by_a_boundary_are_cleaned_as_one(client, broker):
    """Bug class: the unfinished end of a piece cleaned before its rest has come: `\\x1b[3` shown as `[3`... or dropped while `1m` is shown, and a progress line shown with all its states."""
    log = "a\x1b[31mred\x1b[0m\nprogress 10%\rprogress 99%\ndone\n"
    cut_pieces = [log[at:at + 3] for at in range(0, len(log), 3)]
    assert "".join(clean_log_text(piece) for piece in cut_pieces) != clean_log_text(log)
    broker.answers["run.logs"] = LogServer(log, piece=3)
    shown, _ = follow(client, first_request(client, RUN_ID))
    assert shown == "ared\nprogress 99%\ndone\n"


def test_the_unfinished_line_is_shown_apart_until_its_end_comes(client, broker):
    """Bug class: a progress line that is not shown at all until its line ends (the view looks stuck), or shown as if it were whole and then repeated."""
    broker.answers["run.logs"] = LogServer("whole\nhalf a li", piece=64, end_when_read=False)
    response = client.get(first_request(client, RUN_ID), headers=HX).text
    assert '<div class="output-piece">whole\n</div>' in response
    assert re.search(r'hx-trigger="load delay:3s" hx-swap="outerHTML">half a li</div>', response)


def test_a_tail_replaces_what_is_shown(client, broker):
    """Bug class: Docker's tail (the last 500 lines, again at each request) appended to itself, so that the view repeats the output every 10 seconds."""
    answers = iter([tail_answer("first\n"), tail_answer("first\nsecond\n")])
    broker.answers["run.logs"] = lambda fields: next(answers)
    shown, steps = follow(client, first_request(client, LIVE_ID), steps_max=2)
    assert shown == "first\nsecond\n"
    assert [step["retarget"] for step in steps] == [steps[0]["retarget"]] * 2 and steps[0]["retarget"].startswith("#view-")
    assert [step["trigger"] for step in steps] == ["load delay:10s", "load delay:10s"]


def test_a_recorded_run_turning_into_a_tail_and_back(client, broker):
    """Bug class: a view that keeps appending when the run's supervisor went away and the answer became Docker's tail (or the reverse), mixing the two outputs."""
    answers = iter([
        {"data": {"id": RUN_ID, "kind": "web", "state": "running", "text": "one\n", "next_offset": 4, "complete": False}},
        {"data": {"id": RUN_ID, "kind": "web", "state": "detached", "text": "one\ntwo\n", "next_offset": None, "complete": False}},
        {"data": {"id": RUN_ID, "kind": "web", "state": "lost", "text": "two\n", "next_offset": 8, "complete": True}},
        {"data": {"id": RUN_ID, "kind": "web", "state": "lost", "text": "", "next_offset": 8, "complete": True}},
    ])
    broker.answers["run.logs"] = lambda fields: next(answers)
    shown, steps = follow(client, first_request(client, RUN_ID))
    assert [bool(step["retarget"]) for step in steps] == [False, True, True, False]
    assert [step["trigger"] for step in steps] == ["load delay:3s", "load delay:10s", "load", None]
    assert shown == "two\n" and steps[-1]["end"] == output.GONE_LINES["lost"]


@pytest.mark.parametrize("state", ["lost", "unreadable"])
def test_a_lost_or_unreadable_run_stops_after_one_empty_answer(client, broker, state):
    """Bug class: a view of a run that will never write again polling for ever."""
    broker.answers["run.logs"] = LogServer("a\nb\n", piece=2, state_when_ended=state)
    shown, steps = follow(client, first_request(client, RUN_ID))
    assert shown == "a\nb\n" and steps[-1]["trigger"] is None and steps[-1]["end"] == output.GONE_LINES[state]
    assert len(steps) == 3


def test_the_triggers_follow_the_answer(client, broker):
    """Bug class: a live run asked at once again and again (a request loop), the rest of an ended run asked every 3 seconds (minutes to read a long log), or a tail asked as often as a piece."""
    live = LogServer("x\n", piece=64, end_when_read=False)
    broker.answers["run.logs"] = live
    _, steps = follow(client, first_request(client, RUN_ID), steps_max=2)
    assert [step["trigger"] for step in steps] == ["load delay:3s", "load delay:3s"]
    full = LogServer("y" * 70000 + "\n", piece=65536, end_when_read=False)
    broker.answers["run.logs"] = full
    _, steps = follow(client, first_request(client, RUN_ID), steps_max=2)
    assert steps[0]["trigger"] == "load" and steps[1]["trigger"] == "load delay:3s"
    broker.answers["run.logs"] = LogServer("a\nb\nc\n", piece=2)
    _, steps = follow(client, first_request(client, RUN_ID))
    assert [step["trigger"] for step in steps] == ["load delay:3s", "load delay:3s", "load", None]


def test_the_view_stops_at_its_size_limit_and_continues_in_a_fresh_view(make_app, make_client, broker):
    """Bug class: a view that grows without bound in the browser (a log of gigabytes), or a limit that leaves no way to read on."""
    app = make_app(view_shown_bytes=20)
    client = make_client(app)
    log_in(client)
    log = "".join(f"line {number}\n" for number in range(10))
    broker.answers["run.logs"] = LogServer(log, piece=8)
    first, steps = follow(client, first_request(client, RUN_ID))
    assert steps[-1]["end"].startswith(output.LIMIT_LINE + " <a href=") and steps[-1]["trigger"] is None
    link = html.unescape(re.search(r'<a href="([^"]+)">Continue from here</a>', steps[-1]["text"]).group(1))
    offset = int(link.rsplit("=", 1)[1])
    assert link == f"/runs/{RUN_ID}?offset={offset}" and offset == len(first) and offset >= 20
    whole, views = first, 1
    while steps[-1]["end"] != output.ENDED_LINE:
        link = html.unescape(re.search(r'<a href="([^"]+)">Continue from here</a>', steps[-1]["text"]).group(1))
        offset = int(link.rsplit("=", 1)[1])
        assert offset == len(whole)
        shown, steps = follow(client, first_request(client, RUN_ID, offset))
        assert steps[0]["href"].startswith(f"/runs/{RUN_ID}/output?offset={offset}&view=")
        whole += shown
        views += 1
    assert whole == log and views >= 3


@pytest.mark.parametrize("route", [
    "/runs/r1/output?offset=0&view=0123456789abcdef",
    "/runs/R20261121T101500-0a1b2c/output?offset=0&view=0123456789abcdef",
    "/runs/live-0123456789AB/output?offset=0&view=0123456789abcdef",
    f"/runs/{RUN_ID}/output?offset=-1&view=0123456789abcdef",
    f"/runs/{RUN_ID}/output?offset=1e3&view=0123456789abcdef",
    f"/runs/{RUN_ID}/output?offset=&view=0123456789abcdef",
    f"/runs/{RUN_ID}/output?offset=99999999999999999&view=0123456789abcdef",
    f"/runs/{RUN_ID}/output?offset={2 ** 53 + 1}&view=0123456789abcdef",
    f"/runs/{RUN_ID}/output?offset=0",
    f"/runs/{RUN_ID}/output?view=0123456789abcdef",
    f"/runs/{RUN_ID}/output?offset=0&view=0123456789ABCDEF",
    f"/runs/{RUN_ID}/output?offset=0&view=..%2F",
    f"/runs/{RUN_ID}?offset=-1",
    f"/runs/{RUN_ID}?offset=x",
    "/runs/r20261121T101500-0a1b2c%0a",
])
def test_a_bad_run_id_offset_or_view_is_not_found_before_any_broker_call(client, broker, route):
    """Bug class: a run id, an offset or a view id of any shape passed on (to the broker, into a CSS selector, into a link), or a broker asked for a request the page then refuses."""
    broker.requests.clear()
    assert client.get(route, headers=HX).status_code == 404
    assert broker.requests == []


def test_the_views_of_a_session_are_bounded(make_app, make_client, broker):
    """Bug class: one state per opened run page for ever (memory without bound), or a dropped view that goes on asking."""
    app = make_app(views_max=2)
    client = make_client(app)
    log_in(client)
    hrefs = [first_request(client, RUN_ID) for _ in range(3)]
    broker.requests.clear()
    response = client.get(hrefs[0], headers=HX).text
    assert html.escape(output_closed(), quote=False) in response and "hx-get" not in response
    assert broker.requests == []
    assert client.get(hrefs[2], headers=HX).status_code == 200 and broker.requests


def output_closed():
    from social_data_pipeline.webui import management

    return management.VIEW_CLOSED


def test_the_held_line_is_bounded(make_app, make_client, broker):
    """Bug class: a line that never ends (a progress bar without line breaks) held in memory without bound and never shown."""
    app = make_app(view_held_bytes=10)
    client = make_client(app)
    log_in(client)
    broker.answers["run.logs"] = LogServer("x" * 25, piece=7, end_when_read=False)
    _, steps = follow(client, first_request(client, RUN_ID), steps_max=4)
    assert [step["pieces"] for step in steps] == [[], ["x" * 14], [], ["x" * 11]]
    session = app.sessions.get(client.cookies.get("__Host-sdp_session"))
    assert all(len(view.held) <= 10 for view in session.work._views.values())


def test_a_poll_after_the_session_ended_gets_hx_redirect_and_no_page(client, broker, app):
    """Bug class: the login page swapped into the output view (or into the outcome box) as if it were output, after a logout in another tab or a session the broker ended."""
    href = first_request(client, RUN_ID)
    token = form_token(client)
    path = client.post("/pending", data={"form_token": token, "action": "db.stop"}, headers={"origin": ORIGIN}).headers["location"]
    outcome = client.post(path + "/confirm", data={"form_token": token}, headers={"origin": ORIGIN}).headers["location"]
    broker.answers["run.logs"] = broker_client.BrokerError("unauthorized", "x")
    ended = client.get(href, headers=HX)
    assert ended.status_code == 204 and ended.headers["hx-redirect"] == "/login?why=ended" and ended.text == ""
    for route in (href, outcome, "/", "/overview", "/runs"):
        response = client.get(route, headers=HX)
        assert response.status_code == 204 and response.headers["hx-redirect"] == "/login" and response.text == ""
        plain = client.get(route)
        assert plain.status_code == 303 and plain.headers["location"] == "/login"


def test_a_full_tail_bound_and_a_busy_broker_answer_a_quiet_retry(make_app, make_client, broker):
    """Bug class: Docker's tails (each a process of the broker, up to 15 seconds) filling the pages' bound, or a busy answer that stops the view or swaps a page into it."""
    app = make_app(tail_workers=1)
    client = make_client(app)
    log_in(client)
    first, second = first_request(client, LIVE_ID), first_request(client, LIVE_ID)
    broker.answers["run.logs"] = lambda fields: tail_answer("t\n")
    broker.gate = threading.Event()
    broker.gate_verbs = {"run.logs"}
    from starlette.testclient import TestClient

    results = []
    with TestClient(app, base_url="https://localhost:8443", headers={"host": "localhost:8443"}, client=("172.30.0.1", 50000), follow_redirects=False, cookies={"__Host-sdp_session": client.cookies.get("__Host-sdp_session")}) as shared:
        held = threading.Thread(target=lambda: results.append(shared.get(first, headers=HX).status_code))
        held.start()
        wait_until(lambda: broker.entered >= 1 and any(call[0] == "call" and call[2] == "run.logs" for call in broker.calls))
        busy = shared.get(second, headers=HX)
        broker.gate.set()
        held.join(10)
    assert results == [200]
    assert busy.status_code == 200 and "busy: trying again" in busy.text
    assert f'hx-get="{html.escape(second)}" hx-trigger="load delay:10s"' in busy.text
    broker.gate = None
    broker.answers["run.logs"] = broker_client.BrokerError("busy", "x")
    response = client.get(second, headers=HX).text
    assert "busy: trying again" in response and 'hx-trigger="load delay:10s"' in response


def test_a_failed_read_stops_the_view_with_its_sentence(client, broker):
    """Bug class: a run that is not found (a terminal run that ended) polled for ever, or its refusal shown as output."""
    broker.answers["run.logs"] = broker_client.BrokerError("failed", "x", details={"errors": "no run with this id\n"})
    shown, steps = follow(client, first_request(client, LIVE_ID))
    assert shown == "" and len(steps) == 1 and steps[0]["end"] == words.CODE_SENTENCES["failed"] and "no run with this id" in steps[0]["text"]


@pytest.mark.parametrize("failure", ["refused", "shape"])
def test_a_view_that_stops_on_an_error_shows_its_unfinished_line(client, broker, failure):
    """Bug class: the unfinished last line, shown in the element that makes the next request, lost when an error stops the view (the error's answer replaces that element)."""
    broker.answers["run.logs"] = LogServer("whole\nhalf a li", piece=64, end_when_read=False)
    href = first_request(client, RUN_ID)
    response = client.get(href, headers=HX).text
    following = html.unescape(re.search(r'hx-get="([^"]+)"', response).group(1))
    broker.answers["run.logs"] = broker_client.BrokerError("failed", "x") if failure == "refused" else {"data": "not a piece"}
    stopped = client.get(following, headers=HX).text
    assert '<div class="output-piece">half a li</div>' in stopped and "hx-get" not in stopped


def test_a_request_behind_the_view_appends_nothing(client, broker):
    """Bug class: a stale or hand-made request whose offset is below what the view has shown appending that output again."""
    broker.answers["run.logs"] = LogServer("one\ntwo\n", piece=4, end_when_read=False)
    href = first_request(client, RUN_ID)
    assert "one" in client.get(href, headers=HX).text
    broker.requests.clear()
    again = client.get(href, headers=HX).text
    assert "output-piece" not in again and "hx-get" not in again and "already shown" in again
    assert broker.requests == []


def test_open_views_cannot_make_the_pages_busy(make_app, make_client, broker):
    """Bug class: the pieces of the output views read under the pages' bound, so that a few open run pages make every other page answer "busy"; or no bound on them at all."""
    from starlette.testclient import TestClient

    app = make_app(page_workers=1, piece_workers=1)
    client = make_client(app)
    log_in(client)
    first, second = first_request(client, RUN_ID), first_request(client, RUN_ID)
    broker.gate = threading.Event()
    broker.gate_verbs = {"run.logs"}
    results = []
    with TestClient(app, base_url="https://localhost:8443", headers={"host": "localhost:8443"}, client=("172.30.0.1", 50000), follow_redirects=False, cookies={"__Host-sdp_session": client.cookies.get("__Host-sdp_session")}) as shared:
        held = threading.Thread(target=lambda: results.append(shared.get(first, headers=HX).status_code))
        held.start()
        wait_until(lambda: any(call[0] == "call" and call[2] == "run.logs" for call in broker.calls))
        page = shared.get("/runs")
        busy = shared.get(second, headers=HX)
        broker.gate.set()
        held.join(10)
    assert results == [200]
    assert page.status_code == 200
    assert busy.status_code == 200 and "busy: trying again" in busy.text

