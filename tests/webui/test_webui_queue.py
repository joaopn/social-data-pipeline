"""Tests for the Queue window (`webui.queue`, `webui.scheduler`, `webui.results`), through the whole application with a stand-in broker and a stand-in scheduler's control socket (`webui_helpers.StandInDoor`).

The contracts pinned here, each test naming the bug class it catches:

- a. The socket's reachability: without the settings, with a control folder that fails its check (a link, another owner, an open mode, none: an injected `lstat`), and with a socket that is not there, refuses the connection, may not be opened or closes without a reply (real sockets), each page says why and the socket receives nothing.
- b. A session the broker ended does not act: every change of the Queue asks the broker (`audit.list`) before the socket (the order recorded); an ended session goes to the login and the socket gets no change; an unreachable broker refuses the change; no GET or HEAD of the window asks the socket for anything but a read.
- c. Approve: the pending page shows the statement with escapes (U+202E, U+2066, U+200B, U+0000, U+2028, a lone surrogate) and says so, the overwrite line, the two sentences and the hints; the socket gets exactly `{op, job_id, digest, by}` with the digest of the pending time and `by` the broker's session number; a job changed meanwhile is `changed`; `not_pending` with its phase; a job too large has no approve form and a posted approve for it keeps nothing; two confirms at once make one call.
- d. Reject (the reason's bounds and characters, refused before the socket), kill (whether it ran), EXPLAIN (its sentence, the plan and the error as text, `explain_timeout` and `outcome_unknown`).
- e. Auto-accept: switching on and raising the limit need the management password, checked before the socket (a wrong one keeps the action and sends nothing); switching off and lowering need none; an unknown target refused by the web UI; the sentence for on; a change that became a step-up meanwhile refused.
- g. The pages: tabs, counts, paging with `more`, links only for job ids, the job page per phase, hostile text in every position, the running tab's poll and its `HX-Redirect`, the preview's table and its refusals.
- h. Downloads: byte for byte with fixed headers; not done, a foreign or malformed id, a part out of range: not found and nothing opened; a link, a FIFO, a folder named like a part, an output file name with `/`, a link in place of the job's folder: never served (a marker file is in no answer); at most two at a time; the descriptor closed after the answer and after a client that went away.
- k. The scans: no inline code in any page or part of the window; the guard's headers on every answer, the download's included; HEAD of every route of the window is 405.
- l. Whose query it is: a job whose record's `submitted_by` is exactly `"assistant"` labelled on every page that shows it; any other value shown as nothing and never echoed; the approval's extra sentence only for such a job, the approval sent unchanged; the auto-accept page's sentence.

Every special character in this file is written as an escape.
"""

from __future__ import annotations

import html
import json
import os
import re
import socket
import stat
import threading

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette.testclient")

from test_webui_pages import inline_code
from webui_helpers import (
    CHANGING_OPS,
    GOOD_FOLDER,
    GOOD_SOCKET,
    JOB_ID,
    ORIGIN,
    PASSWORD,
    READ_OPS,
    a_job,
    choose,
    confirm,
    digest_of,
    door_error,
    form_token,
    good_lstat,
    log_in,
    main_html,
    pending_path,
    wait_until,
)

from social_data_pipeline.control import client as broker_client
from social_data_pipeline.webui import queue, scheduler, statements, words
from social_data_pipeline.webui import results as webui_results

HOSTILE = "<script>alert('x')</script>\"&"
SCRIPT = "javascript:alert(1)"
RAW_MARKS = ("<script>", "alert('x')", "</script>\"")
HX = {"hx-request": "true"}
CSP = "default-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'; object-src 'none'"
DONE_ID = "pg_d0d0d0d0"
MARKER = "made-up-marker-of-a-planted-file-3b7a"


@pytest.fixture
def app(make_app):
    return make_app(action_wait_seconds=0.5)


@pytest.fixture
def client(app, make_client, broker):
    broker.session_number = 37
    made = make_client(app)
    assert log_in(made).status_code == 303
    return made


@pytest.fixture
def token(client):
    return form_token(client)


def the_session(app, client):
    return app.sessions.get(client.cookies.get("__Host-sdp_session"))


def changes(door):
    return [request for request in door.requests if request["op"] in CHANGING_OPS]


def attributes(page):
    return [(name, html.unescape(value)) for tag in re.findall(r"<[a-zA-Z][^>]*>", page) for name, value in re.findall(r'\s([a-zA-Z][\w:-]*)="([^"]*)"', tag)]


def main_part(page):
    found = re.search(r"<main[^>]*>(.*)</main>", page, re.DOTALL)
    return found.group(1) if found else page


def main_text(page):
    """The text of the page's main part, tags and escapes removed."""
    found = re.search(r"<main[^>]*>(.*)</main>", page, re.DOTALL)
    return " ".join(html.unescape(re.sub(r"<[^>]+>", " ", found.group(1) if found else page)).split())


def follow(client, response):
    """The page a confirmation leads to: the pending page itself (a refusal at once) or the outcome."""
    if response.status_code == 303:
        location = response.headers["location"]
        page = client.get(location)
        while 'hx-trigger="load delay:2s"' in page.text:
            page = client.get(location)
        return page
    return response


def a_done_job(door, results_folder, job_id=DONE_ID, files=(("result.csv", b"a,b\n1,2\n"),), **fields):
    door.add("history", a_job(job_id, status="done", rows=1, size_bytes=8, **fields))
    folder = results_folder / job_id
    folder.mkdir()
    for name, data in files:
        (folder / name).write_bytes(data)
    return folder


QUEUE_GET_ROUTES = ["/queue", "/queue?tab=approved", "/queue?tab=running", "/queue?tab=history", "/queue?tab=history&page=2", "/queue/running-rows", "/queue/auto-accept", f"/queue/{JOB_ID}", f"/queue/{DONE_ID}", f"/queue/{DONE_ID}/preview", f"/queue/{DONE_ID}/download/0", "/queue/pg_00000002"]


# ── a. reachability ─────────────────────────────────────────────────────────


def pages_of(client):
    return {route: client.get(route) for route in ("/queue", "/queue?tab=history", "/queue/auto-accept", f"/queue/{JOB_ID}")}


def test_without_the_settings_every_page_says_so_and_nothing_is_sent(make_app, make_client, make_scheduler, door):
    """Bug class: a Queue window that, without the mounts, shows an empty queue (the operator believes nothing waits) or an error that does not say what to do, or that tries a socket anyway."""
    app = make_app(scheduler=make_scheduler(socket_path=None, results=None))
    made = make_client(app)
    log_in(made)
    for route, page in pages_of(made).items():
        assert words.DOOR_REASON_SENTENCES["no_settings"] in main_text(page.text), route
    assert door.requests == []
    token = form_token(made)
    response = choose(made, "queue.approve", token, job_id=JOB_ID)
    assert words.DOOR_REASON_SENTENCES["no_settings"] in main_text(response.text) and the_session(app, made).work.pending_count() == 0


def folder_stat(mode, uid=1000):
    return os.stat_result((mode, 0, 0, 1, uid, 1000, 0, 0, 0, 0))


@pytest.mark.parametrize("found, reason", [
    (folder_stat(stat.S_IFLNK | 0o777), "folder_not_a_folder"),
    (folder_stat(stat.S_IFREG | 0o600), "folder_not_a_folder"),
    (folder_stat(stat.S_IFDIR | 0o700, uid=4321), "folder_other_owner"),
    (folder_stat(stat.S_IFDIR | 0o750), "folder_open"),
    (folder_stat(stat.S_IFDIR | 0o701), "folder_open"),
    (FileNotFoundError(), "folder_missing"),
])
def test_a_control_folder_that_fails_its_check_is_not_used(make_app, make_client, make_scheduler, door, found, reason):
    """Bug class: a request sent through a control folder that someone else could have put a socket into (a link, a folder of another user, one others may enter), where the answers would come from a socket of their own."""
    asked = []

    def lstat(path):
        asked.append(path)
        if isinstance(found, BaseException):
            raise found
        return found

    app = make_app(scheduler=make_scheduler(lstat=lstat))
    made = make_client(app)
    log_in(made)
    for route, page in pages_of(made).items():
        assert words.DOOR_REASON_SENTENCES[reason] in main_text(page.text), route
    assert door.requests == [] and set(asked) == {"/run/sdp-jobs"}
    assert scheduler.Scheduler("/run/sdp-jobs/door.sock", None, lstat=good_lstat, uid=lambda: 1000).folder_problem() is None


@pytest.fixture
def short_folder():
    """A control folder with a short path (a socket's path holds at most 107 bytes), 0700, of this user."""
    import tempfile

    folder = tempfile.mkdtemp(prefix="sdpq-", dir="/tmp")
    os.chmod(folder, 0o700)
    yield folder
    for name in os.listdir(folder):
        os.unlink(os.path.join(folder, name))
    os.rmdir(folder)


def test_a_socket_that_cannot_be_reached_is_said_for_each_reason(make_app, make_client, short_folder):
    """Bug class: no socket (the scheduler not running, or an image from before the socket), a socket nobody listens on, one this user may not open, and one whose scheduler closes the connection without a word (another user) all shown with one vague line, through the real client."""
    path = os.path.join(short_folder, "door.sock")
    app = make_app(scheduler=scheduler.Scheduler(path, None))
    made = make_client(app)
    log_in(made)

    def says(reason):
        assert words.DOOR_REASON_SENTENCES[reason] in main_text(made.get("/queue").text), reason

    says(broker_client.NO_SOCKET)
    dead = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    dead.bind(path)
    dead.close()
    # The scheduler makes its socket 0600; one open to others is refused before any connection (see the socket's own check).
    os.chmod(path, 0o600)
    says(broker_client.REFUSED)
    os.unlink(path)
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(path)
    os.chmod(path, 0o600)
    listener.listen(4)
    closing = threading.Event()

    def close_each():
        listener.settimeout(0.2)
        while not closing.is_set():
            try:
                connection, _ = listener.accept()
            except OSError:
                continue
            connection.close()

    thread = threading.Thread(target=close_each, daemon=True)
    thread.start()
    try:
        says(broker_client.NO_REPLY)
        if os.geteuid() != 0:
            os.chmod(path, 0o000)
            says(broker_client.PERMISSION_DENIED)
    finally:
        closing.set()
        thread.join(5)
        listener.close()


# ── b. the session's check, and nothing changing on a GET ───────────────────


def queue_changes(client, token, door):
    """Every change of the Queue, each made pending and then confirmed: (name, the confirmation's answer)."""
    door.add("pending", a_job())
    door.add("running", a_job("pg_00000003", status="running"))
    made = []
    for name, form, password in (
        ("queue.approve", {"job_id": JOB_ID}, None),
        ("queue.explain", {"job_id": JOB_ID}, None),
        ("queue.kill", {"job_id": "pg_00000003"}, None),
        ("queue.auto_accept", {"target": "warehouse", "enabled": "on", "limit": "4"}, PASSWORD),
        ("queue.auto_accept", {"target": "warehouse", "enabled": "off", "limit": "2"}, None),
        ("queue.reject", {"job_id": JOB_ID, "reason": "not today"}, None),
    ):
        path = pending_path(choose(client, name, token, **form))
        made.append((name, follow(client, confirm(client, path, token, password))))
    return made


def test_every_change_asks_the_broker_first(client, token, door, broker):
    """Bug class: a change of the Queue sent to the scheduler on the strength of the web UI's own session alone, after the broker ended it (the password changed, the broker restarted): the scheduler would run it with the admin login for a session nobody holds any more."""
    door.auto["targets"][0]["enabled"] = False
    made = queue_changes(client, token, door)
    assert [request["op"] for request in changes(door)] == ["approve", "explain", "kill", "auto_accept.set", "auto_accept.set", "reject"]
    events = broker.timeline
    for index, event in enumerate(events):
        if event[0] == "door" and event[1] in CHANGING_OPS:
            before = [earlier for earlier in events[:index] if earlier[0] == "broker"]
            assert before and before[-1][2] in ("audit.list", "session.check_password"), (event, before[-2:])
            last_change = max((position for position, earlier in enumerate(events[:index]) if earlier[0] == "door" and earlier[1] in CHANGING_OPS), default=-1)
            assert any(earlier == ("broker", "call", "audit.list") for earlier in events[last_change + 1:index]), event
    assert [request["fields"] for request in broker.requests if request["verb"] == "audit.list"][-1] == {"limit": 1}
    assert all(page.status_code == 200 for _name, page in made)


@pytest.mark.parametrize("code", ["session_ended", "unauthorized"])
def test_a_session_the_broker_ended_goes_to_the_login_and_changes_nothing(client, token, door, broker, app, code):
    """Bug class: a confirmation of an old browser session (the password was changed in a terminal) reaching the scheduler, which runs the approved query with the admin login."""
    door.add("pending", a_job())
    path = pending_path(choose(client, "queue.approve", token, job_id=JOB_ID))
    broker.answers["audit.list"] = broker_client.BrokerError(code, "x")
    response = confirm(client, path, token)
    assert response.status_code == 303 and response.headers["location"] == "/login?why=ended"
    assert changes(door) == [] and the_session(app, client) is None


@pytest.mark.parametrize("error", [broker_client.BrokerUnreachable(broker_client.NO_SOCKET), broker_client.BrokerError("internal", "x")])
def test_an_unreachable_broker_refuses_the_change(client, token, door, broker, app, error):
    """Bug class: a change sent to the scheduler while the broker cannot say whether the session still lives, or the confirmation's answer showing the job's record (its statement) to a session the broker did not confirm."""
    door.add("pending", a_job(sql="SELECT 'made-up-record-mark-77c1'", description="made-up-record-mark-77c1"))
    path = pending_path(choose(client, "queue.approve", token, job_id=JOB_ID))
    broker.answers["audit.list"] = error
    page = follow(client, confirm(client, path, token))
    assert words.sentence(error) in main_text(page.text)
    assert "made-up-record-mark-77c1" not in page.text and f'href="{path}"' in page.text
    assert changes(door) == [] and the_session(app, client) is not None
    assert the_session(app, client).work.broker_checked_at is None
    del broker.answers["audit.list"]
    assert client.get(path).status_code == 200


def test_no_get_or_head_of_the_window_asks_the_socket_for_a_change(client, token, door, results, broker):
    """Bug class: a GET (a reload, a prefetch, a link another site opens in the browser) or a HEAD that approves, rejects, kills, EXPLAINs or changes auto-accept."""
    door.add("pending", a_job())
    a_done_job(door, results)
    pending = pending_path(choose(client, "queue.approve", token, job_id=JOB_ID))
    door.requests.clear()
    routes = [*QUEUE_GET_ROUTES, pending]
    for route in routes:
        for headers in ({}, HX):
            assert client.get(route, headers=headers).status_code in (200, 404), route
            assert client.head(route, headers=headers).status_code == 405, route
    assert door.requests and set(door.ops()) <= READ_OPS and changes(door) == []
    assert {"counts", "jobs", "job", "preview", "auto_accept"} <= set(door.ops())
    # The only broker request of the window's reads is the session's check.
    assert {request["verb"] for request in broker.requests} - {"db.status", "source.list", "run.list"} == {"audit.list"}
    assert all(request["fields"] == {"limit": 1} for request in broker.requests if request["verb"] == "audit.list")


# ── c. approve ──────────────────────────────────────────────────────────────

TRICKY_SQL = "SELECT 1\u202e;\u2066 FROM t\u200b WHERE x = '\x00'\u2028 AND y = '\ud800' -- COPY x TO PROGRAM 'y'"


def test_the_approval_page_shows_the_statement_with_escapes_and_its_warnings(client, token, door):
    """Bug class: an approval page that shows a statement other than the one that runs (a direction change turning the end of it around, an invisible character hiding a word, a line separator) or cannot show it at all (a lone surrogate), that hides the overwrite, or that leaves out that the query runs with the admin login unchecked."""
    door.add("pending", a_job(sql=TRICKY_SQL, overwrite=True, output_filename="out\u202ecsv.exe", description="the made-up description"))
    page = client.get(pending_path(choose(client, "queue.approve", token, job_id=JOB_ID))).text
    statement = re.search(r'<pre class="statement-whole">(.*?)</pre>', page, re.DOTALL).group(1)
    # The lone surrogate arrives as "?": the scheduler's socket replaces it before sending (as the stand-in's real `_clean` does).
    assert "\\ud800" not in statement and "y = &#39;?&#39;" in statement
    for escape in ("\\u202e", "\\u2066", "\\u200b", "\\u0000", "\\u2028"):
        assert f'<span class="escape" title="made visible">{escape}</span>' in statement, escape
    assert all(character not in page for character in ("\u202e", "\u2066", "\u200b", "\x00", "\u2028"))
    assert html.unescape(re.sub(r"<[^>]+>", "", statement)) == "\n" + statements.shown_text(TRICKY_SQL.replace("\ud800", "?"))
    shown = main_text(page)
    assert queue.ESCAPED_SENTENCE in shown and "5 characters" in shown
    assert queue.OVERWRITE_SENTENCE in shown and 'out<span class="escape" title="made visible">\\u202e</span>csv.exe' in page
    assert queue.ADMIN_SENTENCE in shown and queue.NO_CHECK_SENTENCE in shown
    assert queue.HINTS_LABEL in shown and "PROGRAM" in shown and "COPY" in shown and queue.HINTS_NOTE in shown
    assert "the made-up description" in shown and "warehouse" in shown and "postgres" in shown


def test_a_statement_without_hints_says_that_proves_nothing(client, token, door):
    """Bug class: an empty list of hints read as "checked, harmless"."""
    door.add("pending", a_job(sql="SELECT 1"))
    shown = main_text(client.get(pending_path(choose(client, "queue.approve", token, job_id=JOB_ID))).text)
    assert queue.NO_HINTS in shown and "An empty list proves nothing" in shown and queue.ESCAPED_SENTENCE not in shown


def test_a_mongodb_job_shows_its_collection_and_database(client, token, door):
    """Bug class: an approval of a pipeline that does not say which collection of which database it runs on."""
    door.add("pending", a_job("mg_0000000a", backend="mongodb", sql='[{"$out": "x"}]', collection="posts", database="reddit"))
    shown = main_text(client.get(pending_path(choose(client, "queue.approve", token, job_id="mg_0000000a"))).text)
    assert "Collection posts" in shown and "Database reddit" in shown and "$out" in shown and "Pipeline" in shown


def test_the_scheduler_gets_exactly_the_approval_with_the_digest_shown(client, token, door):
    """Bug class: an approval sent with a digest read at the confirmation (so it approves whatever the job became after the page was shown), without `by`, with another session's label, or with fields the socket refuses."""
    record = door.add("pending", a_job())
    shown_digest = digest_of(record)
    path = pending_path(choose(client, "queue.approve", token, job_id=JOB_ID))
    assert changes(door) == []
    page = follow(client, confirm(client, path, token))
    assert changes(door) == [{"op": "approve", "job_id": JOB_ID, "digest": shown_digest, "by": "web-37"}]
    assert "Approved." in main_text(page.text)
    assert client.get(path).status_code == 404


def test_a_job_changed_since_the_page_is_not_approved(client, token, door):
    """Bug class: an approval of a statement other than the one shown: the job was edited at the scheduler between the page and the confirmation."""
    door.add("pending", a_job())
    path = pending_path(choose(client, "queue.approve", token, job_id=JOB_ID))
    door.jobs[JOB_ID][1] = {**door.jobs[JOB_ID][1], "sql": "SELECT 2"}
    page = follow(client, confirm(client, path, token))
    assert words.DOOR_CODE_SENTENCES["changed"] in main_text(page.text)
    assert door.jobs[JOB_ID][0] == "pending"


def test_a_job_no_longer_pending_says_its_phase(client, token, door):
    """Bug class: a refusal that does not say what became of the job (another operator approved or rejected it meanwhile)."""
    door.add("pending", a_job())
    path = pending_path(choose(client, "queue.approve", token, job_id=JOB_ID))
    door.jobs[JOB_ID][0] = "approved"
    shown = main_text(follow(client, confirm(client, path, token)).text)
    assert words.DOOR_CODE_SENTENCES["not_pending"] in shown and "It is now: approved" in shown
    door.add("history", a_job("pg_00000005", status="done"))
    response = choose(client, "queue.approve", token, job_id="pg_00000005")
    assert response.status_code == 409 and "It is now: in the history" in main_text(response.text)


def test_a_job_too_large_has_no_approve_form_and_no_pending_action(client, token, door, app):
    """Bug class: an approval offered for a job the web UI cannot show whole, so the operator approves what they did not see."""
    door.add("pending", a_job())
    door.too_large.add(JOB_ID)
    page = client.get(f"/queue/{JOB_ID}").text
    assert "queue.approve" not in page and words.DOOR_CODE_SENTENCES["too_large"] in main_text(page) and "scheduler's own page" in main_text(page)
    response = choose(client, "queue.approve", token, job_id=JOB_ID)
    assert response.status_code == 400 and the_session(app, client).work.pending_count() == 0 and changes(door) == []


def test_two_confirms_at_once_approve_once(make_app, make_client, door, broker):
    """Bug class: a double click or two tabs sending the approval twice (the second one then refused, or worse, approving a job submitted again under the same id)."""
    from starlette.testclient import TestClient

    app = make_app(action_wait_seconds=0.05)
    first = make_client(app)
    log_in(first)
    token = form_token(first)
    door.add("pending", a_job())
    path = pending_path(choose(first, "queue.approve", token, job_id=JOB_ID))
    door.gate = threading.Event()
    door.gate_ops = {"approve"}
    answers = []
    start = threading.Barrier(2)
    with TestClient(app, base_url="https://localhost:8443", headers={"host": "localhost:8443"}, client=("172.30.0.1", 50000), follow_redirects=False, cookies={"__Host-sdp_session": first.cookies.get("__Host-sdp_session")}) as shared:

        def send():
            start.wait(5)
            answers.append(confirm(shared, path, token).status_code)

        threads = [threading.Thread(target=send) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)
        door.gate.set()
    assert sorted(answers) == [303, 404]
    wait_until(lambda: len(changes(door)) == 1)
    assert [request["op"] for request in changes(door)] == ["approve"]


def test_a_session_without_a_number_cannot_act(make_app, make_client, door, broker):
    """Bug class: a change sent with a made-up or empty `by` when the broker's login reply named no session (the scheduler's record would then name nobody, or the wrong session)."""
    broker.session_number = None
    app = make_app()
    made = make_client(app)
    log_in(made)
    door.add("pending", a_job())
    response = choose(made, "queue.approve", form_token(made), job_id=JOB_ID)
    assert response.status_code == 400 and queue.NO_NUMBER in main_text(response.text) and door.requests == []


# ── d. reject, kill, EXPLAIN ────────────────────────────────────────────────


@pytest.mark.parametrize("reason, taken", [("", True), ("x" * 500, True), ("caf\u00e9 is closed", True), ("x" * 501, False), ("a\nb", False), ("a\rb", False), ("a\u2028b", False), ("a\x07b", False), ("a\x85b", False)])
def test_a_reason_is_checked_before_anything_is_sent(client, token, door, app, reason, taken):
    """Bug class: a reason the scheduler refuses (too long, a line break or a control character) taken by the web UI and refused only after the confirmation, or a good one refused."""
    door.add("pending", a_job())
    response = choose(client, "queue.reject", token, job_id=JOB_ID, reason=reason)
    if not taken:
        assert response.status_code == 400 and queue.REASON_REFUSED in main_text(response.text) and the_session(app, client).work.pending_count() == 0
        assert door.requests == []
        return
    path = pending_path(response)
    # Only the job is read (whether the assistant submitted it, for the page's label).
    assert door.ops() == ["job"]
    follow(client, confirm(client, path, token))
    assert changes(door) == [{"op": "reject", "job_id": JOB_ID, "reason": reason, "by": "web-37"}]


@pytest.mark.parametrize("phase, says", [("running", "It was running"), ("approved", "It was not running")])
def test_kill_says_whether_the_job_was_running(client, token, door, phase, says):
    """Bug class: a kill reported as done for a job that was not running (nothing was stopped), or the reverse."""
    door.add(phase, a_job(status=phase))
    path = pending_path(choose(client, "queue.kill", token, job_id=JOB_ID))
    assert queue.KILL_SENTENCE in main_text(client.get(path).text)
    shown = main_text(follow(client, confirm(client, path, token)).text)
    assert says in shown and changes(door) == [{"op": "kill", "job_id": JOB_ID, "by": "web-37"}]


def test_explain_says_it_runs_with_the_admin_login_and_shows_the_plan_as_text(client, token, door):
    """Bug class: an EXPLAIN confirmed without knowing that it reaches the database with the admin login before any approval, or a plan whose text is written into the page as markup."""
    door.add("pending", a_job())
    door.plan = HOSTILE + "\nSeq Scan on t"
    path = pending_path(choose(client, "queue.explain", token, job_id=JOB_ID))
    shown = main_text(client.get(path).text)
    assert queue.EXPLAIN_SENTENCE in shown and "admin login" in shown and "can execute" in shown
    page = follow(client, confirm(client, path, token)).text
    assert "Seq Scan on t" in page and "&lt;script&gt;" in page and not [mark for mark in RAW_MARKS if mark in page]
    assert changes(door) == [{"op": "explain", "job_id": JOB_ID, "by": "web-37"}]


@pytest.mark.parametrize("error, says", [
    (door_error("explain_failed", detail="ERROR: " + HOSTILE), "ERROR: " + HOSTILE),
    (door_error("explain_timeout"), "may still be running"),
    (door_error("outcome_unknown"), "may still happen"),
    (door_error("busy"), "busy with other requests"),
])
def test_explain_refusals_are_worded_and_the_error_is_text(client, token, door, error, says):
    """Bug class: the database's error text written into the page as markup, or a time-out worded as "nothing happened" while the statement may still run on the database."""
    door.add("pending", a_job())
    door.answers["explain"] = error
    path = pending_path(choose(client, "queue.explain", token, job_id=JOB_ID))
    page = follow(client, confirm(client, path, token)).text
    assert says in main_text(page) and not [mark for mark in RAW_MARKS if mark in page]


# ── e. auto-accept ──────────────────────────────────────────────────────────


@pytest.mark.parametrize("start, form, step_up", [
    ((False, 4), {"enabled": "on", "limit": "4"}, True),
    ((True, 2), {"enabled": "on", "limit": "3"}, True),
    ((False, 2), {"enabled": "off", "limit": "4"}, True),
    ((True, 4), {"enabled": "off", "limit": "4"}, False),
    ((True, 4), {"enabled": "on", "limit": "1"}, False),
    ((False, 4), {"enabled": "off", "limit": "0"}, False),
])
def test_switching_on_or_raising_needs_the_management_password(client, token, door, broker, start, form, step_up):
    """Bug class: auto-accept switched on, or its limit raised, with a stolen cookie alone (every query to that target then runs with the admin login unseen), or a password asked for switching it off, which would keep an operator from stopping it in a hurry."""
    door.auto["targets"][0]["enabled"], door.auto["targets"][0]["limit"] = start
    path = pending_path(choose(client, "queue.auto_accept", token, target="warehouse", **form))
    page = client.get(path).text
    assert ('name="management_password"' in page) is step_up
    if step_up:
        assert confirm(client, path, token).status_code == 400
        response = confirm(client, path, token, "made-up-wrong-pw")
        assert response.status_code == 403 and words.CODE_SENTENCES["step_up_failed"] in main_text(response.text)
        assert changes(door) == [] and client.get(path).status_code == 200
        broker.timeline.clear()
        follow(client, confirm(client, path, token, PASSWORD))
        order = [event for event in broker.timeline if event[0] == "broker" or event[1] == "auto_accept.set"]
        assert [event[2] if event[0] == "broker" else event[1] for event in order] == ["audit.list", "session.check_password", "auto_accept.set"]
    else:
        assert confirm(client, path, token, PASSWORD).status_code == 400
        follow(client, confirm(client, path, token))
        assert "session.check_password" not in [request["verb"] for request in broker.requests]
    wanted_enabled, wanted_limit = form["enabled"] == "on", int(form["limit"])
    assert changes(door) == [{"op": "auto_accept.set", "target": "warehouse", "enabled": wanted_enabled if wanted_enabled != start[0] else None, "limit": wanted_limit if wanted_limit != start[1] else None, "by": "web-37"}]


def test_the_sentence_for_switching_on(client, token, door):
    """Bug class: auto-accept switched on without the page saying that every query of that target then runs with the admin login unseen."""
    path = pending_path(choose(client, "queue.auto_accept", token, target="warehouse", enabled="on", limit="3"))
    shown = main_text(client.get(path).text)
    assert queue.AUTO_ON_SENTENCE.format(target="warehouse", limit=3) in shown
    door.auto["targets"][0]["enabled"] = True
    path = pending_path(choose(client, "queue.auto_accept", token, target="warehouse", enabled="off", limit="4"))
    assert "without anyone looking" not in main_text(client.get(path).text)


@pytest.mark.parametrize("form", [{"target": "made-up-target", "enabled": "on", "limit": "1"}, {"target": "a\nb", "enabled": "on", "limit": "1"}, {"target": "warehouse", "enabled": "maybe", "limit": "1"}, {"target": "warehouse", "enabled": "on", "limit": "5"}, {"target": "warehouse", "enabled": "on", "limit": "-1"}, {"target": "warehouse", "enabled": "off", "limit": "4"}])
def test_an_unknown_target_or_a_refused_value_keeps_nothing(client, token, door, app, form):
    """Bug class: a change for a target the scheduler does not have, a limit beyond its largest, or no change at all, kept and confirmed (the operator believes something was set)."""
    response = choose(client, "queue.auto_accept", token, **form)
    assert response.status_code == 400 and the_session(app, client).work.pending_count() == 0
    assert set(door.ops()) <= {"auto_accept"} and changes(door) == []


def test_a_change_that_became_a_step_up_meanwhile_is_refused(client, token, door):
    """Bug class: a lowering of the limit (no password) confirmed after another session lowered it further, which then raises it without the management password."""
    path = pending_path(choose(client, "queue.auto_accept", token, target="warehouse", enabled="off", limit="2"))
    door.auto["targets"][0]["limit"] = 1
    shown = main_text(follow(client, confirm(client, path, token)).text)
    assert words.DOOR_CODE_SENTENCES["settings_changed"] in shown and changes(door) == []


# ── g. the pages ────────────────────────────────────────────────────────────


def test_the_tabs_count_list_and_page_through_the_jobs(client, door):
    """Bug class: a tab that shows the wrong phase, counts that are not the scheduler's, a page that loses the 51st job (no "more"), or a "more" past the last page."""
    for number in range(51):
        door.add("pending", a_job(f"pg_{number:08x}"))
    door.add("approved", a_job("pg_000000aa", status="approved"))
    page = client.get("/queue").text
    # The numbers are the frame's tab bar's, as the mock-up writes them: Running counts the approved and the running jobs.
    assert 'aria-current="page">Pending (51)</a>' in page and '<a class="tab" href="/queue?tab=running">Running (1)</a>' in page
    assert page.count('href="/queue/pg_') == 50 and 'href="/queue?tab=pending&amp;page=2">More</a>' in page
    assert door.requests[-1] == {"op": "jobs", "phase": "pending", "limit": 50, "offset": 0}
    second = client.get("/queue?tab=pending&page=2").text
    assert second.count('href="/queue/pg_') == 1 and "More</a>" not in second and 'href="/queue?tab=pending&amp;page=1"' in second
    assert door.requests[-1] == {"op": "jobs", "phase": "pending", "limit": 50, "offset": 50}
    approved = client.get("/queue?tab=approved").text
    assert approved.count('href="/queue/pg_') == 1 and "pg_000000aa" in approved
    for route in ("/queue?tab=trash", "/queue?page=0", "/queue?page=x", "/queue?page=99999", "/queue/not-a-job", "/queue/pg_0123abc", "/queue/PG_0123ABCD", "/queue/pg_0123abcd/download/x", "/queue/pg_0123abcd/download/99999"):
        assert client.get(route).status_code == 404, route


def test_only_job_ids_become_links(client, door):
    """Bug class: a link built from an id of the scheduler's record that is not a job id (a path, a scheme), which a hand-edited record can hold."""
    for job_id in ("../../etc", SCRIPT, "pg_0123abcd/x", HOSTILE):
        door.jobs[job_id] = ["pending", a_job(job_id)]
    door.add("pending", a_job())
    page = client.get("/queue").text
    # The list alone: the auto-accept panel under it links its change page.
    rows = main_part(page).split('id="auto-accept"')[0]
    links = [value for name, value in attributes(rows) if name == "href" and value.startswith("/queue/")]
    assert links == [f"/queue/{JOB_ID}"]
    # A row's forms (approve, reject, EXPLAIN) only for the id of a job's form, and only that id is posted.
    assert re.findall(r'name="(?:action|job_id)" value="([^"]*)"', rows) == ["queue.approve", JOB_ID, "queue.reject", JOB_ID, "queue.explain", JOB_ID]
    assert not [mark for mark in RAW_MARKS if mark in page]


def forms_of(page):
    return re.findall(r'name="action" value="([a-z_.]+)"', page)


@pytest.mark.parametrize("phase, status, expected", [
    ("pending", "pending", ["queue.approve", "queue.reject", "queue.explain"]),
    ("approved", "approved", []),
    ("running", "running", ["queue.kill"]),
    ("history", "failed", []),
    ("history", "rejected", []),
])
def test_the_job_page_offers_what_its_phase_allows(client, door, phase, status, expected):
    """Bug class: an approve offered for a job that runs, a kill for a pending one, or a job page that leaves out who approved or rejected it."""
    door.add(phase, a_job(status=status, acted_by={"approved": "web-9", "rejected": "web-8"}, error="made-up error" if status == "failed" else None, reject_reason="made-up reason" if status == "rejected" else None))
    page = client.get(f"/queue/{JOB_ID}").text
    assert forms_of(page) == expected
    shown = main_text(page)
    assert "Approved by web-9" in shown and "Rejected by web-8" in shown
    assert ("made-up error" in shown) is (status == "failed") and ("made-up reason" in shown) is (status == "rejected")
    assert ("Preview" in page) is False


def test_hostile_text_is_text_everywhere(client, token, door, results):
    """Bug class: cross-site scripting through anything a job's submitter controls (the statement, the description, the output file name, an error, a rejection reason) or the scheduler's configuration (a target's name) or a result's content (a preview cell, a part's name), in an element or an attribute."""
    door.add("pending", a_job(sql=HOSTILE, description=HOSTILE, target=HOSTILE, output_filename=HOSTILE))
    door.add("history", a_job("pg_00000006", status="failed", error=HOSTILE, sql=SCRIPT))
    door.add("history", a_job("pg_00000007", status="rejected", reject_reason=HOSTILE))
    a_done_job(door, results, files=(("result.csv", b"x"), ("result_1.csv", b"y")), description=HOSTILE)
    door.preview = {**door.preview, "columns": [HOSTILE, SCRIPT], "rows": [[HOSTILE, SCRIPT]], "parts": [{"name": HOSTILE, "size": 1}], "format": HOSTILE}
    door.auto["targets"].append({"name": HOSTILE, "backend": HOSTILE, "database": SCRIPT, "enabled": False, "limit": 1})
    pages = {route: client.get(route).text for route in ("/queue", "/queue?tab=history", f"/queue/{JOB_ID}", "/queue/pg_00000006", "/queue/pg_00000007", f"/queue/{DONE_ID}", f"/queue/{DONE_ID}/preview", "/queue/auto-accept")}
    pages["approve"] = client.get(pending_path(choose(client, "queue.approve", token, job_id=JOB_ID))).text
    pages["auto"] = client.get(pending_path(choose(client, "queue.auto_accept", token, target=HOSTILE, enabled="on", limit="1"))).text
    for route, page in pages.items():
        assert not [mark for mark in RAW_MARKS if mark in page], route
        assert "&lt;script&gt;" in page, route
        for name, value in attributes(page):
            if name in ("href", "hx-get", "action"):
                assert value.startswith("/") and not value.startswith("//") and SCRIPT not in value, (route, value)
    assert pages[f"/queue/{DONE_ID}/preview"].count("&lt;script&gt;") >= 4
    hidden = [value for name, value in attributes(pages["/queue/auto-accept"]) if name == "value"]
    assert HOSTILE in hidden


def test_the_running_tab_asks_for_itself_again_and_an_ended_session_is_sent_to_the_login(client, door, app, token):
    """Bug class: a running tab that does not follow the queue (the operator watches a stale list), an ended session whose poll swaps the login page into the tab, or a polled part whose Kill form has no form token (refused by the guard) or whose render fails on it."""
    door.add("running", a_job(status="running"))
    page = client.get("/queue?tab=running").text
    assert 'hx-get="/queue/running-rows?page=1" hx-trigger="load delay:5s" hx-swap="outerHTML"' in page
    part = client.get("/queue/running-rows?page=1", headers=HX)
    assert part.status_code == 200 and not part.text.startswith("<!doctype") and 'hx-trigger="load delay:5s"' in part.text and f"/queue/{JOB_ID}" in part.text
    assert re.findall(r'name="(?:form_token|action|job_id)" value="([^"]*)"', part.text) == [token, "queue.kill", JOB_ID]
    assert 'hx-trigger' not in main_html(client.get("/queue?tab=pending").text)
    app.sessions.forget(client.cookies.get("__Host-sdp_session"))
    ended = client.get("/queue/running-rows?page=1", headers=HX)
    assert ended.status_code == 204 and ended.headers["hx-redirect"] == "/login"


def test_the_running_tab_holds_the_approved_and_the_running_lists_each_with_its_pages(client, door):
    """Bug class: the approved jobs lost when the Approved tab went into Running (or its old address answering 404), the Running tab's number leaving them out, or the pager of one list moving the other list too."""
    for number in range(51):
        door.add("approved", a_job(f"pg_{number:08x}", status="approved"))
    door.add("running", a_job("pg_000000ee", status="running"))
    page = client.get("/queue?tab=running").text
    assert 'aria-current="page">Running (52)</a>' in page and "<h1>Running</h1>" in page
    assert page.index("<h2>Approved, waiting to start</h2>") < page.index("<h2>Running</h2>") < page.index("/queue/pg_000000ee")
    assert page.count('href="/queue/pg_') == 51 and 'href="/queue?tab=approved&amp;page=2">More</a>' in page
    assert [request for request in door.requests if request["op"] == "jobs"][-2:] == [{"op": "jobs", "phase": "approved", "limit": 50, "offset": 0}, {"op": "jobs", "phase": "running", "limit": 50, "offset": 0}]
    # The old address of the Approved tab is the Running tab, its list at the page asked; the running list stays at its first.
    second = client.get("/queue?tab=approved&page=2").text
    assert 'aria-current="page">Running (52)</a>' in second and "pg_00000032" in second and "/queue/pg_000000ee" in second
    assert [request for request in door.requests if request["op"] == "jobs"][-2:] == [{"op": "jobs", "phase": "approved", "limit": 50, "offset": 50}, {"op": "jobs", "phase": "running", "limit": 50, "offset": 0}]
    assert "auto_accept" not in door.ops()


def test_a_rows_approve_posts_what_the_job_page_posts_and_leads_to_the_whole_statement(client, door):
    """Bug class: a row's Approve that skips the confirm page (the statement never seen whole before the admin login runs it), or that posts fields the pending route refuses."""
    door.add("pending", a_job(sql="SELECT 1\nFROM t\nWHERE made_up_mark_5e1"))
    page = client.get("/queue").text
    form = re.search(r'<form method="post" action="/pending" class="act">(.*?)</form>', page, re.DOTALL).group(1)
    fields = dict(re.findall(r'<input type="hidden" name="([a-z_]+)" value="([^"]*)">', form))
    assert set(fields) == {"form_token", "action", "job_id"} and fields["action"] == "queue.approve" and fields["job_id"] == JOB_ID
    pending = client.get(pending_path(client.post("/pending", data=fields, headers={"origin": ORIGIN}))).text
    assert '<pre class="statement-whole">\nSELECT 1\nFROM t\nWHERE made_up_mark_5e1</pre>' in pending and changes(door) == []


def test_the_pending_tab_shows_the_auto_accept_panel_and_its_failure_stays_in_it(client, door):
    """Bug class: the panel showing a target as switched off while the scheduler auto-approves its queries, no way from it to the change form, or a failure to read the settings that takes the pending list with it."""
    door.add("pending", a_job())
    door.auto = {"max_limit": 5, "targets": [{"name": "warehouse", "backend": "postgres", "database": "datasets", "enabled": True, "limit": 2}, {"name": "olap", "backend": "starrocks", "database": "reddit", "enabled": False, "limit": 0}]}
    panel = client.get("/queue").text.split('id="auto-accept"')[1]
    text = main_text(panel)
    assert '<span class="sw on" role="img" aria-label="auto-accept on"></span>' in panel and '<span class="sw" role="img" aria-label="auto-accept off"></span>' in panel
    assert "warehouse postgres \u00b7 auto-approving 2 of 5" in text and "olap starrocks \u00b7 every query waits for you 0 of 5" in text
    assert '<meter min="0" max="5" value="2"' in panel and panel.count('href="/queue/auto-accept"') == 2 and queue.NEVER_AUTO_SENTENCE in text
    door.answers["auto_accept"] = broker_client.BrokerUnreachable(broker_client.NO_REPLY)
    response = client.get("/queue")
    rows, panel = response.text.split('id="auto-accept"')
    assert response.status_code == 200 and f'href="/queue/{JOB_ID}"' in rows and 'value="queue.approve"' in rows
    assert words.DOOR_REASON_SENTENCES[broker_client.NO_REPLY] in main_text(panel) and words.DOOR_REASON_SENTENCES[broker_client.NO_REPLY] not in main_text(rows)


def test_the_preview_shows_a_table_of_text_and_its_refusals(client, door, results):
    """Bug class: a preview that drops its parts or the "truncated" mark (the operator believes the first rows are all), a refusal shown as an empty table, or a History row's Preview that does not lead to it."""
    a_done_job(door, results)
    door.preview = {**door.preview, "truncated": True, "parts": [{"name": "result.csv", "size": 8}, {"name": "result_1.csv", "size": 2048}], "parts_total": 2}
    page = client.get(f"/queue/{DONE_ID}").text
    assert f'hx-get="/queue/{DONE_ID}/preview"' in page and 'hx-trigger="load"' not in page
    # A History row's Preview opens the job's page with its preview loaded at once.
    assert f'href="/queue/{DONE_ID}?preview=1#result"' in client.get("/queue?tab=history").text
    assert f'hx-get="/queue/{DONE_ID}/preview" hx-trigger="load"' in client.get(f"/queue/{DONE_ID}?preview=1").text
    part = client.get(f"/queue/{DONE_ID}/preview", headers=HX).text
    assert "<th scope=\"col\">a</th>" in part and "<td>1</td><td>2</td>" in part and "truncated" in part and "result_1.csv" in part and "2.0 KiB" in part
    for code in ("not_done", "preview_not_available", "preview_failed"):
        door.answers["preview"] = door_error(code)
        assert words.DOOR_CODE_SENTENCES[code] in client.get(f"/queue/{DONE_ID}/preview", headers=HX).text


# ── h. downloads ────────────────────────────────────────────────────────────


def test_a_part_is_downloaded_byte_for_byte_with_fixed_headers(client, door, results):
    """Bug class: a part served with another type (a browser renders it as a page), inline, with its name as the submitter wrote it (a header injection, a path), with a wrong length, or changed on the way."""
    data = bytes(range(256)) * 600
    a_done_job(door, results, output_filename="r\u00e9sult \"x\".csv", files=(("r\u00e9sult \"x\".csv", data), ("r\u00e9sult \"x\"_2.csv", b"second")))
    page = client.get(f"/queue/{DONE_ID}").text
    assert [value for name, value in attributes(page) if name == "href" and "/download/" in value] == [f"/queue/{DONE_ID}/download/0", f"/queue/{DONE_ID}/download/1"]
    response = client.get(f"/queue/{DONE_ID}/download/0")
    assert response.status_code == 200 and response.content == data
    assert response.headers["content-type"] == "application/octet-stream"
    assert response.headers["content-disposition"] == 'attachment; filename="r_sult__x_.csv"'
    assert response.headers["content-length"] == str(len(data))
    assert response.headers["content-security-policy"] == CSP and response.headers["cache-control"] == "no-store" and response.headers["x-content-type-options"] == "nosniff"
    assert "accept-ranges" not in response.headers
    assert client.get(f"/queue/{DONE_ID}/download/1").content == b"second"
    assert client.get(f"/queue/{DONE_ID}/download/2").status_code == 404
    assert client.get(f"/queue/{DONE_ID}/download/0", headers={"range": "bytes=0-1"}).content == data
    assert webui_results.download_name("x" * 300) == "x" * 128 and webui_results.download_name("") == "download"


def open_fds():
    return len(os.listdir("/proc/self/fd"))


@pytest.mark.parametrize("phase, status", [("pending", "pending"), ("running", "running"), ("history", "failed"), ("history", "rejected")])
def test_a_job_that_is_not_done_has_no_download_and_nothing_is_opened(client, door, results, monkeypatch, phase, status):
    """Bug class: a result file served for a job that has not finished (a part being written by the database server), failed or was rejected."""
    a_done_job(door, results)
    door.jobs[DONE_ID] = [phase, {**door.jobs[DONE_ID][1], "status": status}]
    opened = []
    monkeypatch.setattr(webui_results, "open_part", lambda *arguments: opened.append(arguments))
    assert client.get(f"/queue/{DONE_ID}/download/0").status_code == 404 and opened == []


def test_a_malformed_or_unknown_id_opens_nothing(client, door, results, monkeypatch):
    """Bug class: a download that builds a path from what the browser sent (`..`, another folder) or from a job the scheduler does not know."""
    a_done_job(door, results)
    opened = []
    monkeypatch.setattr(webui_results, "open_part", lambda *arguments: opened.append(arguments))
    for route in ("/queue/..%2F..%2Fetc/download/0", "/queue/pg_d0d0d0d/download/0", "/queue/pg_d0d0d0d0x/download/0", "/queue/pg_00000009/download/0"):
        assert client.get(route).status_code == 404, route
    assert opened == []
    assert [request["job_id"] for request in door.requests if request["op"] == "job"] == ["pg_00000009"]


def plant(folder, kind, name, marker_file):
    path = folder / name
    if kind == "link":
        path.symlink_to(marker_file)
    elif kind == "fifo":
        os.mkfifo(path)
    elif kind == "folder":
        path.mkdir()
        (path / "inside").write_text(MARKER)


@pytest.mark.parametrize("kind", ["link", "fifo", "folder"])
def test_something_planted_as_a_part_is_never_served(client, door, results, tmp_path, kind):
    """Bug class: a link named like a part (the database server's user can write the job's folder) making the web UI serve a file of the operator's choosing elsewhere; a FIFO that hangs the download; a folder served as a file."""
    marker_file = tmp_path / "secret.txt"
    marker_file.write_text(MARKER)
    folder = a_done_job(door, results, files=(("result_1.csv", b"real part"),))
    plant(folder, kind, "result.csv", marker_file)
    page = client.get(f"/queue/{DONE_ID}").text
    assert [value for name, value in attributes(page) if name == "href" and "/download/" in value] == [f"/queue/{DONE_ID}/download/0"]
    answers = [client.get(f"/queue/{DONE_ID}/download/{number}") for number in range(3)]
    assert answers[0].content == b"real part" and [answer.status_code for answer in answers[1:]] == [404, 404]
    assert all(MARKER.encode() not in answer.content for answer in answers) and MARKER not in page


def test_an_output_name_with_a_slash_and_a_linked_job_folder_are_never_served(client, door, results, tmp_path):
    """Bug class: a record whose output file name is a path (`../x`, `a/b`), or a link put in place of the job's folder, leading the download out of the job's folder."""
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (elsewhere / "result.csv").write_text(MARKER)
    door.add("history", a_job(DONE_ID, status="done"))
    (results / DONE_ID).symlink_to(elsewhere)
    assert client.get(f"/queue/{DONE_ID}/download/0").status_code == 404
    assert MARKER not in client.get(f"/queue/{DONE_ID}").text
    folder = a_done_job(door, results, "pg_e0e0e0e0", output_filename="../elsewhere/result.csv", files=())
    (folder / "x").write_text(MARKER)
    for name in ("a/b.csv", "../../elsewhere/result.csv", "..", "."):
        door.jobs["pg_e0e0e0e0"][1]["output_filename"] = name
        answer = client.get("/queue/pg_e0e0e0e0/download/0")
        assert answer.status_code == 404 and MARKER not in answer.text, name


def test_at_most_two_downloads_at_a_time(client, door, results, monkeypatch):
    """Bug class: downloads without a bound (each holds a descriptor and a worker), so that many of them stall the window."""
    a_done_job(door, results)
    held = threading.Event()
    reading = []
    real = webui_results.PartStream._read

    def slow(self, count):
        reading.append(1)
        held.wait(10)
        return real(self, count)

    monkeypatch.setattr(webui_results.PartStream, "_read", slow)
    answers = []
    threads = [threading.Thread(target=lambda: answers.append(client.get(f"/queue/{DONE_ID}/download/0").status_code)) for _ in range(2)]
    for thread in threads:
        thread.start()
    wait_until(lambda: len(reading) == 2)
    third = client.get(f"/queue/{DONE_ID}/download/0")
    assert third.status_code == 503 and queue.DOWNLOADS_BUSY in third.text and third.headers["content-security-policy"] == CSP
    held.set()
    for thread in threads:
        thread.join(10)
    assert answers == [200, 200]
    assert client.get(f"/queue/{DONE_ID}/download/0").status_code == 200


def run_stream(stream, receive, send):
    import anyio

    async def main():
        await stream({"type": "http"}, receive, send)

    try:
        anyio.run(main)
    except BaseException as error:
        return error
    return None


def test_the_descriptor_is_closed_after_the_answer_and_after_a_client_that_went_away(tmp_path):
    """Bug class: a descriptor left open by every download, or by every download the browser gave up on, until the container runs out of them."""
    import anyio

    path = tmp_path / "part"
    path.write_bytes(b"z" * (3 * webui_results.CHUNK_BYTES + 5))
    before = open_fds()
    released = []
    fd = os.open(path, os.O_RDONLY)
    sent = []

    async def receive():
        await anyio.sleep(30)

    async def send(message):
        sent.append(message)

    assert run_stream(webui_results.PartStream(fd, os.fstat(fd).st_size, "part", lambda: released.append(1)), receive, send) is None
    assert b"".join(message.get("body", b"") for message in sent) == path.read_bytes() and open_fds() == before and released == [1]

    fd = os.open(path, os.O_RDONLY)
    started = []

    async def gone_receive():
        while not started:
            await anyio.sleep(0.01)
        return {"type": "http.disconnect"}

    async def slow_send(message):
        started.append(message)
        await anyio.sleep(30)

    run_stream(webui_results.PartStream(fd, os.fstat(fd).st_size, "part", lambda: released.append(2)), gone_receive, slow_send)
    assert open_fds() == before and released == [1, 2] and len(started) == 1

    fd = os.open(path, os.O_RDONLY)

    async def broken_send(message):
        if message["type"] == "http.response.body":
            raise OSError("the client went away")

    assert run_stream(webui_results.PartStream(fd, os.fstat(fd).st_size, "part", lambda: released.append(3)), receive, broken_send) is not None
    assert open_fds() == before and released == [1, 2, 3]


# ── k. the scans ────────────────────────────────────────────────────────────


def test_no_page_or_part_of_the_window_holds_inline_code_and_every_answer_has_the_headers(client, token, door, results):
    """Bug class: inline code in a page or a part only the Queue renders (a tab, a job, a preview, a pending approval, an outcome, a refusal), which the policy blocks; an answer of the window (a download, a busy or not-found answer) without the security headers."""
    door.add("pending", a_job(sql=TRICKY_SQL))
    door.add("running", a_job("pg_00000003", status="running"))
    a_done_job(door, results)
    answers = [client.get(route) for route in QUEUE_GET_ROUTES]
    answers.append(client.get("/queue/running-rows", headers=HX))
    pending = pending_path(choose(client, "queue.approve", token, job_id=JOB_ID))
    answers.append(client.get(pending))
    answers.append(follow(client, confirm(client, pending, token)))
    answers.append(choose(client, "queue.reject", token, job_id=JOB_ID, reason="a\nb"))
    auto = pending_path(choose(client, "queue.auto_accept", token, target="warehouse", enabled="on", limit="2"))
    answers += [client.get(auto), confirm(client, auto, token, "made-up-wrong-pw")]
    answers.append(client.get("/queue/pg_00000002"))
    for answer in answers:
        headers = answer.headers
        assert headers.get_list("content-security-policy") == [CSP], answer.request.url
        assert headers.get_list("cache-control") == ["no-store"] and headers.get_list("x-content-type-options") == ["nosniff"]
        if headers.get("content-type", "").startswith("text/html"):
            assert inline_code(answer.text) == [], answer.request.url
    assert {answer.status_code for answer in answers} >= {200, 400, 403, 404}


def test_the_navigation_links_the_queue(client):
    """Bug class: a Queue window that cannot be reached from the frame, or still marked as not available."""
    page = client.get("/").text
    assert '<a class="win" href="/queue"><span class="ico" aria-hidden="true">☑</span>Queue<span id="queue-badge"></span></a>' in page and "not available yet" not in page
    queue_page = client.get("/queue").text
    assert '<nav class="subbar" aria-label="Queue">' in queue_page and 'href="/queue/auto-accept"' in queue_page
    assert '<a class="win on" href="/queue" aria-current="page"><span class="ico" aria-hidden="true">☑</span>Queue<span id="queue-badge"></span></a>' in queue_page


def test_a_post_to_the_queue_routes_is_refused(client, token):
    """Bug class: a Queue route that takes a POST and acts on it outside the pending, confirm and outcome way."""
    for route in ("/queue", f"/queue/{JOB_ID}", f"/queue/{JOB_ID}/preview", f"/queue/{JOB_ID}/download/0", "/queue/auto-accept"):
        assert client.post(route, data={"form_token": token}, headers={"origin": ORIGIN}).status_code == 405, route


# ── round 2: a session the broker ended reads nothing ───────────────────────

READ_ROUTES = ["/queue", "/queue?tab=running", "/queue/running-rows", "/queue/auto-accept", f"/queue/{JOB_ID}", f"/queue/{DONE_ID}/preview", f"/queue/{DONE_ID}/download/0", "pending"]
PARTS = ("/queue/running-rows", f"/queue/{DONE_ID}/preview")


def spy_on_files(monkeypatch):
    opened = []
    monkeypatch.setattr(webui_results, "open_part", lambda *arguments: opened.append(arguments))
    monkeypatch.setattr(webui_results, "list_parts", lambda *arguments: opened.append(arguments) or [])
    return opened


def a_read_route(client, token, door, results, route):
    door.add("pending", a_job())
    a_done_job(door, results)
    if route == "pending":
        route = pending_path(choose(client, "queue.approve", token, job_id=JOB_ID))
    return route


@pytest.mark.parametrize("route", READ_ROUTES)
@pytest.mark.parametrize("code", ["session_ended", "unauthorized"])
def test_a_session_the_broker_ended_reads_nothing_of_the_queue(client, token, door, broker, app, results, clock, monkeypatch, route, code):
    """Bug class: a browser session whose broker session has ended (the management password changed, the broker restarted) still reading the queue's statements, previews and results through the web UI's own session table."""
    route = a_read_route(client, token, door, results, route)
    clock.now += queue.SESSION_CHECK_SECONDS + 1
    door.requests.clear()
    opened = spy_on_files(monkeypatch)
    broker.answers["audit.list"] = broker_client.BrokerError(code, "x")
    response = client.get(route, headers=HX if route in PARTS else {})
    if route in PARTS:
        assert response.status_code == 204 and response.headers["hx-redirect"] == "/login?why=ended"
    else:
        assert response.status_code == 303 and response.headers["location"] == "/login?why=ended"
    assert door.requests == [] and opened == [] and the_session(app, client) is None


@pytest.mark.parametrize("route", READ_ROUTES)
def test_an_unreachable_broker_stops_every_read_of_the_queue(client, token, door, broker, app, results, clock, monkeypatch, route):
    """Bug class: the queue read without the broker having said that the session lives, because the broker could not be asked."""
    route = a_read_route(client, token, door, results, route)
    clock.now += queue.SESSION_CHECK_SECONDS + 1
    door.requests.clear()
    opened = spy_on_files(monkeypatch)
    broker.answers["audit.list"] = broker_client.BrokerUnreachable(broker_client.NO_SOCKET)
    response = client.get(route, headers=HX if route in PARTS else {})
    assert words.REASON_SENTENCES[broker_client.NO_SOCKET] in html.unescape(response.text)
    assert door.requests == [] and opened == [] and the_session(app, client) is not None


def test_the_brokers_answer_is_believed_five_seconds_and_a_download_always_asks(client, door, broker, results, clock):
    """Bug class: every poll of the running tab a broker call (or none at all after the first), or a download let through on an old answer."""
    a_done_job(door, results)

    def checks():
        return len([request for request in broker.requests if request["verb"] == "audit.list"])

    client.get("/queue")
    client.get("/queue?tab=history")
    assert checks() == 1
    clock.now += queue.SESSION_CHECK_SECONDS - 0.5
    client.get("/queue/running-rows", headers=HX)
    assert checks() == 1
    clock.now += 0.5
    client.get("/queue/running-rows", headers=HX)
    assert checks() == 2
    for _ in range(2):
        assert client.get(f"/queue/{DONE_ID}/download/0").status_code == 200
    assert checks() == 4


# ── round 2: auto-accept, the page's "After" ────────────────────────────────


@pytest.mark.parametrize("start, form, meanwhile, password", [
    ((False, 4), {"enabled": "on", "limit": "4"}, (False, 3), PASSWORD),
    ((False, 4), {"enabled": "on", "limit": "4"}, (True, 4), PASSWORD),
    ((True, 4), {"enabled": "on", "limit": "2"}, (True, 3), None),
    ((True, 4), {"enabled": "on", "limit": "2"}, (False, 4), None),
])
def test_a_target_changed_since_the_page_is_not_changed(client, token, door, start, form, meanwhile, password):
    """Bug class: a confirmation that sets something else than the page's "After" line said, because another session changed the target in between and only the fields chosen are sent."""
    door.auto["targets"][0]["enabled"], door.auto["targets"][0]["limit"] = start
    path = pending_path(choose(client, "queue.auto_accept", token, target="warehouse", **form))
    door.auto["targets"][0]["enabled"], door.auto["targets"][0]["limit"] = meanwhile
    shown = main_text(follow(client, confirm(client, path, token, password)).text)
    assert words.DOOR_CODE_SENTENCES["settings_changed"] in shown and changes(door) == []


# ── round 2: the pending page's model, made once and rendered off the loop ──


def test_a_queue_pending_page_is_made_once_and_rendered_off_the_event_loop(client, token, door, monkeypatch):
    """Bug class: a hostile statement (a megabyte of runs to escape) cut and rendered again at every view of its pending page and at a refused confirmation, on the event loop, so that it stalls every other request."""
    import asyncio

    import jinja2

    made = []
    real = queue.build_pending_model
    monkeypatch.setattr(queue, "build_pending_model", lambda *arguments: made.append(1) or real(*arguments))
    rendered = []
    real_render = jinja2.environment.Template.render

    def render(self, *arguments, **keywords):
        try:
            asyncio.get_running_loop()
            on_loop = True
        except RuntimeError:
            on_loop = False
        rendered.append((self.name, on_loop))
        return real_render(self, *arguments, **keywords)

    monkeypatch.setattr(jinja2.environment.Template, "render", render)
    door.add("pending", a_job(sql="SELECT '\u202e'" * 2000))
    path = pending_path(choose(client, "queue.approve", token, job_id=JOB_ID))
    for _ in range(3):
        assert client.get(path).status_code == 200
    door.answers["approve"] = door_error("changed")
    assert words.DOOR_CODE_SENTENCES["changed"] in main_text(follow(client, confirm(client, path, token)).text)
    assert made == [1]
    pages = [on_loop for name, on_loop in rendered if name == "pending.html"]
    assert len(pages) == 4 and not any(pages)


# ── round 2: numbers of any shape ───────────────────────────────────────────

ODD_NUMBERS = [True, "x", [1], {"a": 1}, None, -5, 10 ** 400, 1.5, -(10 ** 400), 2 ** 70]


@pytest.mark.parametrize("value", ODD_NUMBERS)
def test_the_number_helpers_are_total(value):
    """Bug class: a hand-edited record with an integer beyond a float's range (or a text, a list, a boolean where a number belongs) answering every page that shows it with a 500."""
    for helper in (queue.when, queue.size_text, queue.rows_text):
        shown_text = helper(value)
        assert isinstance(shown_text, str) and len(shown_text) < 80, (helper.__name__, value)
    assert queue.when(10 ** 400) == "not a time" and queue.size_text(10 ** 400).endswith("PiB") and "digits" in queue.size_text(10 ** 400) and "digits" in queue.rows_text(10 ** 400)


@pytest.mark.parametrize("value", ODD_NUMBERS)
def test_odd_numbers_in_every_numeric_field_are_shown(client, token, door, results, value):
    """Bug class: an odd number in one numeric field of one job failing the whole tab, the job page, the approval page or the preview."""
    numeric = {"submitted_at": value, "approved_at": value, "started_at": value, "finished_at": value, "rows": value, "size_bytes": value}
    door.add("pending", a_job(**numeric))
    door.add("history", a_job(DONE_ID, status="done", **numeric))
    (results / DONE_ID).mkdir()
    (results / DONE_ID / "result.csv").write_bytes(b"a\n")
    door.preview = {**door.preview, "parts": [{"name": "result.csv", "size": value}], "parts_total": value, "read_bytes": value}
    door.auto["targets"][0]["limit"] = value if isinstance(value, int) and not isinstance(value, bool) else 4
    for route in ("/queue", "/queue?tab=history", f"/queue/{JOB_ID}", f"/queue/{DONE_ID}", f"/queue/{DONE_ID}/preview", "/queue/auto-accept"):
        assert client.get(route).status_code == 200, route
    assert client.get(pending_path(choose(client, "queue.approve", token, job_id=JOB_ID))).status_code == 200


# ── round 2: a change without a reply, overwrite, a statement that is not text


@pytest.mark.parametrize("name, form, look", [
    ("queue.approve", {"job_id": JOB_ID}, "Open the job again to see."),
    ("queue.reject", {"job_id": JOB_ID, "reason": "no"}, "Open the job again to see."),
    ("queue.kill", {"job_id": JOB_ID}, "Open the job again to see."),
    ("queue.explain", {"job_id": JOB_ID}, "Open the job again to see."),
    ("queue.auto_accept", {"target": "warehouse", "enabled": "off", "limit": "2"}, "Open the auto-accept page again to see."),
])
def test_a_change_sent_without_a_reply_may_have_been_done(client, token, door, name, form, look):
    """Bug class: a change whose request reached the scheduler but whose reply was lost shown as refused ("refuses this user"), so the operator believes nothing happened and does it again."""
    door.add("pending", a_job())
    path = pending_path(choose(client, name, token, **form))
    op = queue.actions.TABLE[name].door
    door.answers[op] = broker_client.BrokerUnreachable(broker_client.NO_REPLY)
    shown = main_text(follow(client, confirm(client, path, token)).text)
    assert words.DOOR_NO_ANSWER in shown and look in shown
    assert words.DOOR_REASON_SENTENCES[broker_client.NO_REPLY] not in shown
    door.answers["job"] = broker_client.BrokerUnreachable(broker_client.NO_REPLY)
    assert words.DOOR_REASON_SENTENCES[broker_client.NO_REPLY] in main_text(client.get(f"/queue/{JOB_ID}").text)


@pytest.mark.parametrize("value, overwrites, raw", [(True, True, None), (False, False, None), ("yes", True, "yes"), (0, False, None), ([1], True, "[1]"), ("", False, None), ({"a": 1}, True, '{"a": 1}')])
def test_any_true_overwrite_is_said_with_its_value(client, token, door, value, overwrites, raw):
    """Bug class: a record whose `overwrite` is true but not the boolean (the backends read it by its truth and overwrite) shown as not overwriting."""
    door.add("pending", a_job(overwrite=value))
    pages = {"tab": client.get("/queue").text, "job": client.get(f"/queue/{JOB_ID}").text, "approve": client.get(pending_path(choose(client, "queue.approve", token, job_id=JOB_ID))).text}
    # The list is the socket's short view, which keeps only a scalar `overwrite` (a list or an object arrives as None).
    assert ('<span class="tag warn">overwrites</span>' in pages["tab"]) is (overwrites and not isinstance(value, (list, dict)))
    assert ("overwrites an existing result" in pages["job"]) is overwrites
    assert (queue.OVERWRITE_SENTENCE in main_text(pages["approve"])) is overwrites
    for key in ("job", "approve"):
        assert ("(overwrite: " + raw + ")" in html.unescape(main_text(pages[key]))) if raw else ("(overwrite:" not in pages[key]), key


@pytest.mark.parametrize("sql", [{"a": "COPY"}, ["SELECT", 1], 7, None])
def test_a_statement_that_is_not_text_is_shown_as_its_json_without_hints(client, token, door, sql):
    """Bug class: a statement that is not text shown as empty, with "none of the words found", which reads as a harmless empty query."""
    door.add("pending", a_job(sql=sql))
    for page in (client.get(f"/queue/{JOB_ID}").text, client.get(pending_path(choose(client, "queue.approve", token, job_id=JOB_ID))).text):
        shown = main_text(page)
        assert queue.STATEMENT_NOT_TEXT in shown and queue.HINTS_NOT_COMPUTED in shown and queue.NO_HINTS not in shown
        assert html.unescape(re.search(r'<pre class="statement(?:-whole)?">\n(.*?)</pre>', page, re.DOTALL).group(1)) == json.dumps(sql, ensure_ascii=False)


# ── round 2: the socket's own check ─────────────────────────────────────────


def a_socket_stat(mode, uid=1000):
    return os.stat_result((mode, 0, 0, 1, uid, 1000, 0, 0, 0, 0))


@pytest.mark.parametrize("found, reason", [
    (a_socket_stat(stat.S_IFREG | 0o600), "socket_not_a_socket"),
    (a_socket_stat(stat.S_IFLNK | 0o777), "socket_not_a_socket"),
    (a_socket_stat(stat.S_IFSOCK | 0o600, uid=4321), "socket_other_owner"),
    (a_socket_stat(stat.S_IFSOCK | 0o660), "socket_open"),
    (a_socket_stat(stat.S_IFSOCK | 0o606), "socket_open"),
    (FileNotFoundError(), broker_client.NO_SOCKET),
])
def test_a_socket_that_fails_its_check_is_not_used(make_app, make_client, make_scheduler, door, found, reason):
    """Bug class: a request sent to something planted at the socket's place while the folder was open (a file, a link to another socket, a socket of another user or one others may use), whose answers would be someone else's."""
    def lstat(path):
        if str(path).endswith("door.sock"):
            if isinstance(found, BaseException):
                raise found
            return found
        return GOOD_FOLDER

    app = make_app(scheduler=make_scheduler(lstat=lstat))
    made = make_client(app)
    log_in(made)
    for route, page in pages_of(made).items():
        assert words.DOOR_REASON_SENTENCES[reason] in main_text(page.text), route
    assert door.requests == [] and GOOD_SOCKET.st_mode & 0o077 == 0


# ── round 2: the statement's box ────────────────────────────────────────────


VOID = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "source", "track", "wbr"}


def css_rules():
    """(selector, body) of every rule of every stylesheet the pages load (`static/*.css`), at-rule blocks (`@media`) opened so that the rules inside count too."""
    from pathlib import Path

    sheets = sorted((Path(queue.__file__).resolve().parent / "static").glob("*.css"))
    assert {path.name for path in sheets} >= {"app.css", "chat.css", "queue.css", "manage.css"}
    rules = []
    for path in sheets:
        text = re.sub(r"/\*.*?\*/", "", path.read_text(), flags=re.DOTALL)
        text = re.sub(r"@[a-z-]+[^{;]*\{", "", text)
        rules += [(selector.strip(), body) for selector, body in re.findall(r"([^{}]+)\{([^{}]*)\}", text)]
    return rules


def chain_of(page, wanted):
    """The elements from the first `<pre>` whose class is `wanted` up to the document's root, each (tag, classes, id)."""
    from html.parser import HTMLParser

    found = []

    class Walk(HTMLParser):
        stack = []

        def handle_starttag(self, tag, attrs):
            attrs = dict(attrs)
            element = (tag, set((attrs.get("class") or "").split()), attrs.get("id"))
            if tag == "pre" and attrs.get("class") == wanted and not found:
                found.extend([element, *reversed(self.stack)])
            if tag not in VOID:
                self.stack.append(element)

        def handle_endtag(self, tag):
            while self.stack and self.stack.pop()[0] != tag:
                pass

    Walk().feed(page)
    return found


def compound_matches(compound, element):
    """Whether a compound selector may select `element` (its type, classes and id; pseudo-classes and attribute tests are left out, so a doubt counts as a match)."""
    compound = re.sub(r"\[[^\]]*\]|::?[\w-]+", "", compound)
    tag = re.match(r"[a-zA-Z][\w-]*|\*", compound)
    return (tag.group(0).lower() if tag else None) in (None, "*", element[0]) and set(re.findall(r"\.([\w-]+)", compound)) <= element[1] and set(re.findall(r"#([\w-]+)", compound)) <= {element[2]}


def may_match(selector, chain):
    """Whether one selector of a list may select `chain[0]`, whose ancestors are `chain[1:]`: its last compound matches the element, and each compound before it an ancestor further up (a child combinator taken as a descendant one, and a sibling combinator as always met, so a doubt counts as a match)."""
    tokens = re.findall(r"[>+~]|[^\s>+~]+", re.sub(r"\([^()]*\)", "", selector))
    if not tokens or not compound_matches(tokens[-1], chain[0]):
        return False
    position = 1
    for token in reversed(tokens[:-1]):
        if token in "+~":
            return True
        if token == ">":
            continue
        while position < len(chain) and not compound_matches(token, chain[position]):
            position += 1
        if position >= len(chain):
            return False
        position += 1
    return True


def test_the_approvals_statement_box_has_no_height_and_every_box_says_its_size(client, token, door):
    """Bug class: an approval whose statement sits in a box with its own scroll bar or a height of its own (the end of a long statement is never seen unless scrolled), set on the statement or on any element around it, in any of the stylesheets; or a box that does not say how much it holds."""
    door.add("pending", a_job(sql="SELECT 1\nFROM t\nWHERE x"))
    page = client.get(pending_path(choose(client, "queue.approve", token, job_id=JOB_ID))).text
    classes = re.search(r'<pre class="([^"]+)">\nSELECT', page).group(1).split()
    assert classes == ["statement-whole"]
    chain = chain_of(page, "statement-whole")
    tags = [tag for tag, _, _ in chain]
    assert tags[0] == "pre" and "main" in tags and tags[-1] == "html", tags
    # Between the statement and `<main>`, nothing may have a height or a scroll box; `<main>` is the page's own scrolling area under the top bar (no height of its own either), and so are the elements above it.
    inside = chain[:tags.index("main")]
    rules = css_rules()
    assert any(may_match(selector, chain) for rule, _ in rules for selector in rule.split(",")), "no rule found for the statement: the check would prove nothing"
    assert not may_match("details.tool pre", chain) and may_match(".card pre", chain) and may_match("pre.statement-whole:hover", chain)
    for rule, body in rules:
        for index, element in enumerate(chain):
            if any(may_match(selector, chain[index:]) for selector in rule.split(",")):
                assert "max-height" not in body, (rule, element[0])
                if index < len(inside):
                    assert not re.search(r"overflow(?!-wrap)|(?<![\w-])height\s*:|line-clamp", body), (rule, element[0])
    assert "3 lines, 23 characters" in main_text(page)
    job_page = client.get(f"/queue/{JOB_ID}").text
    assert '<pre class="statement">\nSELECT' in job_page and "3 lines, 23 characters" in main_text(job_page)
    assert queue.statement_counts("") == "0 lines, 0 characters" and queue.statement_counts("a\n") == "1 line, 2 characters"


def test_a_statement_that_starts_with_a_line_break_keeps_it(client, token, door):
    """Bug class: the line break right after `<pre>`, which a browser drops, taken from the statement, so that what is shown is not exactly what runs."""
    door.add("pending", a_job(sql="\nSELECT 1"))
    page = client.get(pending_path(choose(client, "queue.approve", token, job_id=JOB_ID))).text
    assert '<pre class="statement-whole">\n\nSELECT 1</pre>' in page
    assert '<pre class="statement">\n\nSELECT 1</pre>' in client.get(f"/queue/{JOB_ID}").text


def test_a_limit_above_the_schedulers_largest_is_refused_with_it(client, token, door, app):
    """Bug class: a limit the scheduler would cut silently to its largest taken, so that the page's "After" line is not what is set."""
    door.auto["max_limit"] = 3
    response = choose(client, "queue.auto_accept", token, target="warehouse", enabled="off", limit="4")
    assert response.status_code == 400 and "from 0 to 3, the scheduler's largest limit" in main_text(response.text)
    assert the_session(app, client).work.pending_count() == 0


def test_an_action_of_the_queue_under_way_names_the_scheduler(make_app, make_client, door):
    """Bug class: the page of a Queue action under way saying that the broker runs it."""
    app = make_app(action_wait_seconds=0.05)
    made = make_client(app)
    log_in(made)
    door.add("pending", a_job())
    door.gate = threading.Event()
    door.gate_ops = {"explain"}
    token = form_token(made)
    response = confirm(made, pending_path(choose(made, "queue.explain", token, job_id=JOB_ID)), token)
    page = made.get(response.headers["location"]).text
    door.gate.set()
    assert "The scheduler is asked" in page and "The broker runs the action" not in page


# ── round 3 ─────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("where, error", [
    ("second look", broker_client.BrokerUnreachable(broker_client.NO_REPLY)),
    ("second look", door_error("busy")),
    ("session check", broker_client.BrokerUnreachable(broker_client.NO_REPLY)),
    ("password check", broker_client.BrokerUnreachable(broker_client.NO_REPLY)),
])
def test_a_step_before_the_change_that_fails_says_nothing_was_sent(client, token, door, broker, where, error):
    """Bug class: a change that was never sent (the second look at the settings, the session's check or the password's check lost its reply) reported as "it may have been done", so the operator believes auto-accept may be on."""
    path = pending_path(choose(client, "queue.auto_accept", token, target="warehouse", enabled="on", limit="4"))
    if where == "second look":
        door.answers["auto_accept"] = error
    elif where == "session check":
        broker.answers["audit.list"] = error
    else:
        broker.answers["session.check_password"] = error
    shown = main_text(follow(client, confirm(client, path, token, PASSWORD)).text)
    assert words.DOOR_NO_ANSWER not in shown and changes(door) == []
    if where == "second look":
        assert words.DOOR_CODE_SENTENCES["settings_not_read"] in shown and "nothing was sent" in shown
    else:
        assert words.REASON_SENTENCES[broker_client.NO_REPLY] in shown


HEAVY_DESCRIPTION = "a\u200b" * 70000


@pytest.mark.parametrize("field", ["description", "error", "reject_reason", "target", "output_filename"])
def test_every_run_of_a_pending_model_is_weighed(field):
    """Bug class: the session's cap on pending actions weighing only the statement's runs, so that a description (or an error, a reason, a target, a file name) of alternating letters and invisible characters, half a million runs, passes as a small record."""
    record = a_job(**{field: HEAVY_DESCRIPTION})
    model = queue.build_pending_model("queue.approve", {"job_id": JOB_ID}, {"record": record})
    runs = len(statements.visible(HEAVY_DESCRIPTION)[0])
    assert runs >= 140000
    assert queue.model_weight(model) >= runs * queue.RUN_BYTES
    assert queue.model_weight([(True, "\\u200b"), {"k": [(False, "ab")]}, "xyz"]) == 2 * queue.RUN_BYTES + 6 + 2 + 1 + 3


def test_a_pending_approval_heavier_than_the_cap_is_not_kept(client, token, door, app):
    """Bug class: a record whose page cuts into runs that hold far more memory than 4 MiB kept anyway (the cap only dropped older actions), so that a few such submissions fill the container's memory."""
    door.add("pending", a_job(description=HEAVY_DESCRIPTION))
    response = choose(client, "queue.approve", token, job_id=JOB_ID)
    assert response.status_code == 413 and queue.TOO_HEAVY in main_text(response.text)
    assert the_session(app, client).work.pending_count() == 0
    door.add("pending", a_job("pg_00000011", description="a\u200b" * 1000))
    path = pending_path(choose(client, "queue.approve", token, job_id="pg_00000011"))
    kept = the_session(app, client).work.pending(path.rsplit("/", 1)[1], 1000.0)
    assert kept.weight >= 2000 * queue.RUN_BYTES


MARK = "made-up-record-mark-5f2a"


@pytest.mark.parametrize("path_kind", ["extra field", "empty password", "busy", "not started"])
@pytest.mark.parametrize("ended", [True, False])
def test_a_confirmation_shows_a_queue_page_again_only_to_a_live_session(make_app, make_client, door, broker, clock, monkeypatch, path_kind, ended):
    """Bug class: a confirmation refused for an extra field, an empty password or a busy bound answering with the Queue action's page (the job's record, its statement) to a session the broker has ended."""
    app = make_app(action_wait_seconds=0.2, door_workers=0 if path_kind == "busy" else 4)
    made = make_client(app)
    log_in(made)
    token = form_token(made)
    door.add("pending", a_job(sql="SELECT '" + MARK + "'", description=MARK))
    if path_kind == "extra field":
        path = pending_path(choose(made, "queue.approve", token, job_id=JOB_ID))
        sent = {"extra": "x"}
    elif path_kind == "empty password":
        path = pending_path(choose(made, "queue.auto_accept", token, target="warehouse", enabled="on", limit="4"))
        sent = {"password": ""}
    else:
        path = pending_path(choose(made, "queue.kill", token, job_id=JOB_ID))
        sent = {}
        if path_kind == "not started":
            real_start = threading.Thread.start

            def refusing_start(self):
                if self.name == "sdp-webui-queue-action":
                    raise RuntimeError("made-up: no thread")
                return real_start(self)

            monkeypatch.setattr(threading.Thread, "start", refusing_start)
    clock.now += queue.SESSION_CHECK_SECONDS + 1
    if ended:
        broker.answers["audit.list"] = broker_client.BrokerError("session_ended", "x")
    response = confirm(made, path, token, **sent)
    assert changes(door) == []
    if ended:
        assert response.status_code == 303 and response.headers["location"] == "/login?why=ended" and the_session(app, made) is None
        assert MARK not in response.text
    elif path_kind in ("busy", "not started"):
        assert response.status_code == 503 and "Confirm this action" not in response.text and f'href="{path}"' in response.text and MARK not in response.text
        assert made.get(path).status_code == 200
    else:
        assert response.status_code == 400 and "Confirm this action" in response.text
        assert (MARK in response.text) is (path_kind == "extra field")


@pytest.mark.parametrize("name", ["queue.approve", "queue.auto_accept"])
def test_only_the_changing_request_itself_may_be_worded_as_possibly_done(name):
    """Bug class: an outcome worded "it may have been done" for a lost reply of any call made for a change (a read before it), when only the lost reply of the changing request itself can mean that."""
    from social_data_pipeline.webui import work

    before = work.Outcome("o1", name, None, 0.0)
    before.finish("error", broker_client.BrokerUnreachable(broker_client.NO_REPLY), from_door=True)
    after = work.Outcome("o2", name, None, 0.0)
    after.finish("error", broker_client.BrokerUnreachable(broker_client.NO_REPLY), from_door=True, sent=True)
    assert words.DOOR_NO_ANSWER not in before.sentence and before.sentence == words.DOOR_REASON_SENTENCES[broker_client.NO_REPLY]
    assert after.sentence == words.door_no_answer_sentence(name)


# ── l. whose query it is ────────────────────────────────────────────────────

ABSENT = object()
PHASE_IDS = {"pending": JOB_ID, "approved": "pg_0000a001", "running": "pg_0000a002", "history": "pg_0000a003"}
HOSTILE_MARK = "made-up-mark-of-a-hand-edited-record-5d1e"


def pages_of_the_jobs(client, token, door, value):
    """Every page that shows a job, for jobs whose record's `submitted_by` is `value` (left out for ABSENT): the list of each tab, each job's page, and the confirm pages of approve, reject, EXPLAIN and kill."""
    fields = {} if value is ABSENT else {"submitted_by": value}
    for phase, job_id in PHASE_IDS.items():
        door.add(phase, a_job(job_id, status="failed" if phase == "history" else phase, **fields))
    pages = {f"tab {phase}": client.get(f"/queue?tab={phase}") for phase in PHASE_IDS}
    pages.update({f"job {phase}": client.get(f"/queue/{job_id}") for phase, job_id in PHASE_IDS.items()})
    for name, job_id, form in (("queue.approve", JOB_ID, {}), ("queue.reject", JOB_ID, {"reason": "no"}), ("queue.explain", JOB_ID, {}), ("queue.kill", PHASE_IDS["running"], {})):
        pages[name] = client.get(pending_path(choose(client, name, token, job_id=job_id, **form)))
    assert all(page.status_code == 200 for page in pages.values()), {key: page.status_code for key, page in pages.items()}
    return {key: page.text for key, page in pages.items()}


def test_a_query_of_the_assistant_is_labelled_on_every_page_that_shows_it(client, token, door):
    """Bug class: a page where a person approves, rejects, EXPLAINs or kills a query without seeing that the chat's assistant wrote it."""
    pages = pages_of_the_jobs(client, token, door, "assistant")
    # A row says it in its "Submitted by" column, with the mock-up's tag; every other page with the label.
    cell = f'<span class="tag write">{queue.ASSISTANT_TAG}</span> <span class="small muted">{queue.ASSISTANT_ROW_NOTE}</span>'
    for key, page in pages.items():
        if key.startswith("tab "):
            assert cell in main_part(page), key
        else:
            assert queue.ASSISTANT_LABEL in main_text(page), key
    assert main_part(pages["tab pending"]).count(cell) == 1


@pytest.mark.parametrize("value", [ABSENT, "", "Assistant", "assistant ", HOSTILE_MARK, HOSTILE, ["assistant"], {"assistant": True}, 1, True, None])
def test_any_other_mark_is_shown_as_nothing_and_never_echoed(client, token, door, value):
    """Bug class: a hand-edited record's `submitted_by` (another name, markup, a list) echoed into the page, or taken as the assistant's mark by its truthiness or a loose compare."""
    pages = pages_of_the_jobs(client, token, door, value)
    for key, page in pages.items():
        text = main_text(page)
        assert queue.ASSISTANT_LABEL not in text and queue.ASSISTANT_SENTENCE not in text and queue.EXPLAIN_ASSISTANT_SENTENCE not in text and queue.ASSISTANT_UNKNOWN not in text, key
        assert 'class="tag write"' not in page and queue.ASSISTANT_ROW_NOTE not in text, key
        assert HOSTILE_MARK not in page and not [mark for mark in RAW_MARKS if mark in page], key


@pytest.mark.parametrize("value, said", [("assistant", True), (ABSENT, False), ("other", False)])
def test_the_approval_of_an_assistants_query_warns_above_the_statement_and_sends_the_same(client, token, door, value, said):
    """Bug class: an approval of a statement the assistant wrote (text in the data it read can steer it) shown like any other, or the warning changing what is sent (the digest, the fields)."""
    record = door.add("pending", a_job(**({} if value is ABSENT else {"submitted_by": value})))
    path = pending_path(choose(client, "queue.approve", token, job_id=JOB_ID))
    page = client.get(path).text
    text = main_text(page)
    assert (queue.ASSISTANT_SENTENCE in text) is said
    if said:
        assert text.index(queue.ASSISTANT_SENTENCE) < text.index("Statement") < text.index(queue.ADMIN_SENTENCE)
    assert queue.ASSISTANT_SENTENCE not in main_text(client.get(pending_path(choose(client, "queue.reject", token, job_id=JOB_ID, reason="no"))).text)
    # An EXPLAIN runs before any approval: a marked job's page says so above the EXPLAIN sentence.
    explain = main_text(client.get(pending_path(choose(client, "queue.explain", token, job_id=JOB_ID))).text)
    assert queue.ASSISTANT_SENTENCE not in explain and (queue.EXPLAIN_ASSISTANT_SENTENCE in explain) is said
    if said:
        assert explain.index(queue.EXPLAIN_ASSISTANT_SENTENCE) < explain.index(queue.EXPLAIN_SENTENCE)
    follow(client, confirm(client, path, token))
    assert changes(door) == [{"op": "approve", "job_id": JOB_ID, "digest": digest_of(record), "by": "web-37"}]


def test_the_auto_accept_page_says_once_that_the_assistants_queries_are_never_auto_accepted(client):
    """Bug class: an operator switching auto-accept on in the belief that it also runs the assistant's queries unseen, or the reverse."""
    text = main_text(client.get("/queue/auto-accept").text)
    assert text.count(queue.NEVER_AUTO_SENTENCE) == 1


LABELLED_ACTIONS = [("queue.reject", JOB_ID, {"reason": "no"}), ("queue.kill", PHASE_IDS["running"], {}), ("queue.explain", JOB_ID, {})]


@pytest.mark.parametrize("name, job_id, form", LABELLED_ACTIONS)
def test_a_session_the_broker_ended_reads_no_job_for_the_label(client, token, door, broker, app, name, job_id, form):
    """Bug class: the label's read of a job's record (the scheduler's socket) made for a session the broker has ended, before its check."""
    door.add("pending", a_job())
    door.add("running", a_job(PHASE_IDS["running"], status="running"))
    the_session(app, client).work.broker_checked_at = None
    broker.answers["audit.list"] = broker_client.BrokerError("session_ended", "x")
    response = choose(client, name, token, job_id=job_id, **form)
    assert response.status_code == 303 and response.headers["location"] == "/login?why=ended"
    assert door.requests == [] and the_session(app, client) is None


@pytest.mark.parametrize("failure", ["busy", "unreachable", "shape"])
@pytest.mark.parametrize("name, job_id, form", LABELLED_ACTIONS)
def test_a_label_that_could_not_be_read_is_said_and_the_action_still_goes(client, token, door, name, job_id, form, failure):
    """Bug class: a job whose mark could not be read (the scheduler busy or unreachable, a reply of another shape) shown as one that is not the assistant's, or the action refused for it."""
    door.add("pending", a_job(submitted_by="assistant"))
    door.add("running", a_job(PHASE_IDS["running"], status="running", submitted_by="assistant"))
    door.answers["job"] = {"phase": "pending", "job": "not a record", "digest": "0" * 64} if failure == "shape" else door_error(failure)
    path = pending_path(choose(client, name, token, job_id=job_id, **form))
    text = main_text(client.get(path).text)
    assert queue.ASSISTANT_UNKNOWN in text and queue.ASSISTANT_LABEL not in text
    assert (queue.EXPLAIN_MAYBE_SENTENCE in text) is (name == "queue.explain") and queue.EXPLAIN_ASSISTANT_SENTENCE not in text
    if name == "queue.explain":
        assert text.index(queue.EXPLAIN_MAYBE_SENTENCE) < text.index(queue.EXPLAIN_SENTENCE)
    follow(client, confirm(client, path, token))
    assert [request["op"] for request in changes(door)] == [name.split(".", 1)[1]]


# ── the top bar's live indicators (GET /status) ─────────────────────────────


def pills(part):
    """{name: (the dot's state, the title)} of the indicators in a part of `/status`."""
    return {name: (dot, html.unescape(title)) for title, dot, name in re.findall(r'<span class="pill" title="([^"]*)"><span class="dot ?([a-z]*)"></span>(\w+)</span>', part)}


def test_the_frame_draws_the_indicators_grey_and_status_needs_a_live_session(client, app, broker, door, make_client):
    """Bug class: a page whose top bar waits for the scheduler or the sandbox (a slow service slows every page), or indicators shown to a browser without a session, or to a session the broker has ended (the login page swapped into the top bar instead of a redirect)."""
    door.add("pending", a_job())
    page = client.get("/").text
    assert '<span class="status hide-s" hx-get="/status" hx-trigger="load, every 30s" hx-swap="outerHTML">' in page and '<span id="queue-badge"></span>' in page
    assert {name: dot for name, (dot, _) in pills(page).items()} == {"broker": "", "scheduler": "", "sandbox": ""} and door.requests == []
    stranger = make_client(app)
    assert stranger.get("/status").status_code == 303 and stranger.get("/status", headers=HX).headers["hx-redirect"] == "/login"
    broker.answers["audit.list"] = broker_client.BrokerError("session_ended", "x")
    ended = client.get("/status", headers=HX)
    assert ended.status_code == 204 and ended.headers["hx-redirect"] == "/login?why=ended" and the_session(app, client) is None and door.requests == []


def test_status_fills_the_pills_and_the_queue_number_with_table_calls_only(make_app, make_client, broker, door, scripted):
    """Bug class: an indicator that says a service answers when it does not (or the other way), the number of waiting queries missing or wrong, a call outside the web UI's table, or a call without a short time limit (a dead service holding a place of the indicators' bound for minutes)."""
    from social_data_pipeline.webui import assistant, status

    app = make_app(assistant=assistant.Assistant(scripted.path))
    client = make_client(app)
    assert log_in(client).status_code == 303
    door.add("pending", a_job())
    door.add("pending", a_job("pg_0123abce"))
    before = len(broker.calls)
    part = client.get("/status", headers=HX)
    assert part.status_code == 200 and not part.text.startswith("<!doctype")
    assert {name: dot for name, (dot, _) in pills(part.text).items()} == {"broker": "ok", "scheduler": "ok", "sandbox": "ok"}
    assert 'hx-trigger="every 30s"' in part.text and "load" not in part.text
    assert re.search(r'<span id="queue-badge" class="n" hx-swap-oob="true" title="2 queries wait for approval">2</span>', part.text)
    assert [call[2] for call in broker.calls[before:]] == ["audit.list"] and broker.timeouts[-1][2] == status.STATUS_SECONDS
    assert door.ops() == ["counts"] and door.timeouts == [("counts", status.STATUS_SECONDS)] and [request["op"] for request in scripted.requests] == ["hello"]
    door.jobs.clear()
    assert '<span id="queue-badge" hx-swap-oob="true"></span>' in client.get("/status", headers=HX).text


def test_status_tells_a_service_not_set_up_from_one_that_does_not_answer(make_app, make_client, broker, door, chat_folder, clock):
    """Bug class: a queue or chat the operator never set up shown as broken, a dead scheduler or sandbox shown as fine, or the Queue's number shown to a session the broker did not confirm."""
    from social_data_pipeline.webui import assistant, scheduler

    door.add("pending", a_job())
    plain = make_client(make_app(scheduler=scheduler.Scheduler(None, None)))
    assert log_in(plain).status_code == 303
    part = plain.get("/status", headers=HX).text
    found = pills(part)
    assert found["broker"][0] == "ok" and found["scheduler"] == ("", "The query queue is not set up for the web UI.") and found["sandbox"] == ("", "No chat is set up for the web UI.")
    assert 'class="n"' not in part and door.requests == []

    door.answers["counts"] = broker_client.BrokerUnreachable("no_reply")
    dead = make_client(make_app(assistant=assistant.Assistant(os.path.join(chat_folder, "runner.sock"))))
    assert log_in(dead).status_code == 303
    part = dead.get("/status", headers=HX).text
    found = pills(part)
    assert found["broker"][0] == "ok" and found["scheduler"][0] == "bad" and found["sandbox"][0] == "bad" and 'class="n"' not in part
    assert found["scheduler"][1].startswith("The scheduler did not answer: ") and found["sandbox"][1].startswith("The sandbox did not answer: ")

    clock.now += 60
    asked = len(door.requests)
    broker.answers["audit.list"] = broker_client.BrokerUnreachable("no_reply")
    found = pills(dead.get("/status", headers=HX).text)
    assert found["broker"][0] == "bad" and found["scheduler"][0] == found["sandbox"][0] == "" and len(door.requests) == asked
