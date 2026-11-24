"""Tests for the login, the logout and the browser sessions of the web UI, with a stand-in broker and a clock moved by the test.

The contracts pinned here, each test naming the bug class it catches:

- d. A login that the broker accepts sets `__Host-sdp_session` with exactly `Path=/; Secure; HttpOnly; SameSite=Strict` (no Domain, no Max-Age) and a new id; an id the browser sent before is not kept. The password and the broker's token are in no answer's body or header and in no log record. Failures have their own words (wrong password, throttled, no password set, each reason the broker cannot be reached). The channel sent to the broker is `local` exactly when the connection arrived on the local port (8444) AND its client address is the network's gateway, and `remote` for the three other combinations; local logins share one key of the per-address limit, and every other login is counted under its own address. Five failures from one address in five minutes refuse the sixth without asking the broker; another address is not affected; IPv6 counts by /64; the window ends after five minutes; a login cancelled while it waits gives its place back. Logins on the local port have a bound of their own, which logins from elsewhere cannot fill.
- e. A broker answer `unauthorized` or `session_ended`, and a broker that cannot be reached, end the session: the id is forgotten and the browser is sent to the login page, which says why. The logout forgets the id, asks the broker to log out (under a bound of its own, also while every login place is taken) and clears the cookie. At most `sessions_max` sessions are kept.
"""

from __future__ import annotations

import re

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette.testclient")

from webui_helpers import GATEWAY, HOST, LOCAL_PORT, ORIGIN, PASSWORD, REMOTE, TOKEN, form_token, log_in, session_cookie, source_list, with_cookie

from social_data_pipeline.control import client as broker_client
from social_data_pipeline.webui import app as webapp
from social_data_pipeline.webui import words


@pytest.fixture
def app(make_app):
    return make_app()


@pytest.fixture
def client(app, make_client):
    return make_client(app)


def login_line(response):
    found = re.search(r'<p class="notice" role="alert">([^<]*)</p>', response.text)
    return found.group(1) if found else None


def follow(client, response):
    assert response.status_code == 303
    return client.get(response.headers["location"])


# ── d. login ────────────────────────────────────────────────────────────────


def test_a_login_sets_the_session_cookie_with_exactly_its_attributes(client):
    """Bug class: a session cookie readable by script, sent over plain HTTP or by other sites, scoped to a parent domain, or kept on disk past the browser's session."""
    response = log_in(client)
    assert response.status_code == 303 and response.headers["location"] == "/"
    cookies = response.headers.get_list("set-cookie")
    assert len(cookies) == 1
    value = session_cookie(response)
    assert cookies[0] == f"__Host-sdp_session={value}; Path=/; Secure; HttpOnly; SameSite=Strict"
    assert re.fullmatch(r"[A-Za-z0-9_-]{43}", value)
    assert client.get("/").status_code == 200


def test_an_id_sent_before_the_login_is_not_kept(make_client, app):
    """Bug class: session fixation: an id planted in the browser before the login (or the id of an earlier login) becomes the logged-in session."""
    browser = make_client(app)
    planted = "made-up-planted-id-0123456789abcdefghijklmnopq"
    first = session_cookie(browser.post("/login", data={"password": PASSWORD}, headers={"origin": ORIGIN, **with_cookie(planted)}))
    assert first not in (None, planted)
    second = session_cookie(browser.post("/login", data={"password": PASSWORD}, headers={"origin": ORIGIN, **with_cookie(first)}))
    assert second not in (None, first, planted)
    probe = make_client(app)
    assert probe.get("/", headers=with_cookie(second)).status_code == 200
    assert probe.get("/", headers=with_cookie(first)).status_code == 303
    assert probe.get("/", headers=with_cookie(planted)).status_code == 303


def test_the_password_and_the_token_appear_in_no_answer_and_no_log(client, broker, caplog):
    """Bug class: the management password or the broker's token leaking into a page, a header, a cookie or a log line (the token is all a thief needs to act through the broker)."""
    caplog.set_level("DEBUG")
    answers = [client.get("/login"), log_in(client, PASSWORD + "-wrong"), log_in(client)]
    answers += [client.get(route) for route in ("/", "/sources", "/login")]
    answers.append(client.post("/logout", data={"form_token": form_token(client)}, headers={"origin": ORIGIN}))
    answers.append(log_in(client))
    broker.answers["db.status"] = broker_client.BrokerError("unauthorized", "not logged in")
    answers.append(client.get("/"))
    for answer in answers:
        whole = answer.text + "".join(f"{key}: {value}\n" for key, value in answer.headers.multi_items())
        assert PASSWORD not in whole and TOKEN not in whole
    for record in caplog.records:
        text = record.getMessage() + repr(record.args)
        assert PASSWORD not in text and TOKEN not in text
    assert ("login", "remote") in broker.calls


@pytest.mark.parametrize("error, status, line", [
    (None, 403, "The password is wrong."),
    (broker_client.BrokerError("throttled", "x"), 429, words.CODE_SENTENCES["throttled"]),
    (broker_client.BrokerError("no_password_set", "x"), 503, words.CODE_SENTENCES["no_password_set"]),
    (broker_client.BrokerError("password_file_not_usable", "x"), 503, words.CODE_SENTENCES["password_file_not_usable"]),
    (broker_client.BrokerError("stopping", "x"), 503, words.CODE_SENTENCES["stopping"]),
    *[(broker_client.BrokerUnreachable(reason), 503, words.REASON_SENTENCES[reason]) for reason in broker_client.REASONS],
])
def test_a_failed_login_says_why(client, broker, error, status, line):
    """Bug class: every failure shown as "wrong password" (the operator retypes a right password while the broker is down or throttling), or a reply's text shown instead of a constant sentence."""
    broker.login_error = error
    response = log_in(client, PASSWORD if error is not None else "made-up-wrong-pw")
    assert response.status_code == status
    assert login_line(response) == line.replace("'", "&#39;")
    assert session_cookie(response) is None


@pytest.mark.parametrize("server_port, address, channel", [
    (LOCAL_PORT, GATEWAY, "local"),
    (LOCAL_PORT, REMOTE, "remote"),
    (LOCAL_PORT, "127.0.0.1", "remote"),
    (LOCAL_PORT, "172.30.0.2", "remote"),
    (8443, GATEWAY, "remote"),
    (8443, REMOTE, "remote"),
    (8443, "127.0.0.1", "remote"),
])
def test_a_login_is_local_only_with_both_signs(make_app, make_client, broker, server_port, address, channel):
    """Bug class: `local` decided by one sign alone. By the address: Docker's proxy dials from the gateway for a client of any address on the other port. By the port: a machine that routes to the container's network through the host may reach the local port directly with its own address. Either way a stranger would draw on the broker's local budget and share the operator's limit, the lock-out the two budgets exist to prevent."""
    client = make_client(make_app(), address=address, server_port=server_port)
    log_in(client)
    assert broker.calls == [("login", channel)]


def test_local_logins_share_one_limit_key_and_others_have_their_own(make_app, make_client, broker):
    """Bug class: local logins keyed together with a remote client that the proxy shows with the gateway address (a stranger's failures lock the operator out), or a connection on the local port from another address counted under the local key."""
    app = make_app()
    local = make_client(app, address=GATEWAY, server_port=LOCAL_PORT)
    for _ in range(5):
        assert log_in(local, "made-up-wrong-pw").status_code == 403
    assert log_in(local).status_code == 429
    assert log_in(make_client(app, address=GATEWAY)).status_code == 303
    assert broker.calls[-1] == ("login", "remote")
    routed = make_client(app, address="192.0.2.30", server_port=LOCAL_PORT)
    for _ in range(5):
        assert log_in(routed, "made-up-wrong-pw").status_code == 403
    assert log_in(routed).status_code == 429
    assert all(call == ("login", "remote") for call in broker.calls[-6:])


def test_logins_from_elsewhere_cannot_make_a_local_login_busy(make_app, make_client, broker):
    """Bug class: one bound for every login, which two slow logins from anywhere fill, so that the operator at the machine itself is told "busy"."""
    import threading
    import time

    from starlette.testclient import TestClient

    app = make_app()
    broker.gate = threading.Event()
    results = []
    with TestClient(app, base_url="https://localhost:8443", headers={"host": HOST}, client=(REMOTE, 50000), follow_redirects=False) as remote, TestClient(app, base_url=f"https://localhost:{LOCAL_PORT}", headers={"host": HOST}, client=(GATEWAY, 50000), follow_redirects=False) as local:
        waiting = [threading.Thread(target=lambda: results.append(log_in(remote).status_code)) for _ in range(2)]
        for thread in waiting:
            thread.start()
        deadline = time.monotonic() + 10
        while broker.entered < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert broker.entered == 2
        assert log_in(remote).status_code == 503
        local_result = []
        local_thread = threading.Thread(target=lambda: local_result.append(log_in(local).status_code))
        local_thread.start()
        while broker.entered < 3 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert broker.entered == 3
        broker.gate.set()
        local_thread.join(10)
        for thread in waiting:
            thread.join(10)
    assert local_result == [303] and results == [303, 303]
    assert sorted(call[1] for call in broker.calls) == ["local", "remote", "remote"]


def test_a_cancelled_login_gives_its_place_in_the_limit_back(make_app, broker, monkeypatch):
    """Bug class: a login whose request is cancelled while the broker is asked (the client went away) keeping its place as if it had failed, so that four wrong guesses plus one dropped connection lock out the right password for five minutes."""
    import threading

    import anyio

    from social_data_pipeline.webui import guard

    app = make_app()
    body = f"password={PASSWORD}".encode()
    real_run = webapp.Gate.run
    hang = threading.Event()
    waiting = threading.Event()

    async def run(self, function, wait_seconds=0):
        # The request waits here for the broker until the test cancels it.
        if hang.is_set():
            waiting.set()
            await anyio.sleep_forever()
        return await real_run(self, function, wait_seconds)

    monkeypatch.setattr(webapp.Gate, "run", run)

    def scope():
        return {"type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1", "method": "POST", "scheme": "https", "path": "/login", "raw_path": b"/login", "query_string": b"", "root_path": "",
                "headers": [(b"host", HOST.encode()), (b"origin", ORIGIN.encode()), (b"content-type", guard.FORM_TYPE.encode()), (b"content-length", str(len(body)).encode())],
                "client": (REMOTE, 50000), "server": ("localhost", 8443)}

    async def one_login(answer, cancel_while_waiting):
        sent = []
        delivered = []

        async def receive():
            if not delivered:
                delivered.append(True)
                return {"type": "http.request", "body": body.replace(PASSWORD.encode(), answer.encode()), "more_body": False}
            await anyio.sleep_forever()

        async def send(message):
            sent.append(message)

        async with anyio.create_task_group() as group:
            group.start_soon(app, scope(), receive, send)
            if cancel_while_waiting:
                while not waiting.is_set():
                    await anyio.sleep(0.01)
                group.cancel_scope.cancel()
        return next((message["status"] for message in sent if message["type"] == "http.response.start"), None)

    for _ in range(4):
        assert anyio.run(one_login, "made-up-wrong-pw", False) == 403
    hang.set()
    assert anyio.run(one_login, PASSWORD, True) is None
    hang.clear()
    assert anyio.run(one_login, PASSWORD, False) == 303
    assert broker.kinds() == ["login"] * 5


def test_the_sixth_failure_from_an_address_is_refused_without_asking_the_broker(make_app, make_client, broker, clock):
    """Bug class: no limit per address (one machine guesses as fast as the broker's budget allows, and keeps the budget empty for everyone of its channel), a limit that blocks every address, or one that never lifts."""
    app = make_app()
    stranger = make_client(app, address=REMOTE)
    for _ in range(5):
        assert log_in(stranger, "made-up-wrong-pw").status_code == 403
    refused = log_in(stranger)
    assert refused.status_code == 429 and login_line(refused) == webapp.LIMITED
    assert len(broker.calls) == 5
    neighbour = make_client(app, address="192.0.2.8")
    assert log_in(neighbour).status_code == 303
    assert log_in(stranger).status_code == 429
    clock.now += 300
    assert log_in(stranger).status_code == 303


def test_ipv6_addresses_of_one_64_share_the_limit(make_app, make_client, broker):
    """Bug class: an IPv6 client taking a fresh address from its /64 for every guess."""
    app = make_app()
    for number in range(5):
        assert log_in(make_client(app, address=f"2001:db8:1:2::{number + 1}"), "made-up-wrong-pw").status_code == 403
    assert log_in(make_client(app, address="2001:db8:1:2::ff")).status_code == 429
    assert log_in(make_client(app, address="2001:db8:1:3::1")).status_code == 303
    assert len(broker.calls) == 6


def test_the_address_table_is_bounded(make_app, make_client, broker, clock):
    """Bug class: one entry per address for ever (memory without bound for whoever can send from many addresses)."""
    app = make_app(addresses_max=2)
    for number in range(3):
        client = make_client(app, address=f"192.0.2.{number + 10}")
        for _ in range(5):
            log_in(client, "made-up-wrong-pw")
        clock.now += 1
    broker.calls.clear()
    assert log_in(make_client(app, address="192.0.2.10")).status_code == 303
    assert log_in(make_client(app, address="192.0.2.12")).status_code == 429
    assert broker.calls == [("login", "remote")]


def test_failures_that_are_not_a_wrong_password_do_not_count(make_app, make_client, broker):
    """Bug class: a broker that is down or throttling locking the operator's address out for five minutes."""
    app = make_app()
    client = make_client(app, address=REMOTE)
    broker.login_error = broker_client.BrokerUnreachable("no_socket")
    for _ in range(6):
        log_in(client)
    broker.login_error = None
    assert log_in(client).status_code == 303


# ── e. sessions ─────────────────────────────────────────────────────────────


@pytest.mark.parametrize("error, why", [
    (broker_client.BrokerError("unauthorized", "x"), "ended"),
    (broker_client.BrokerError("session_ended", "x"), "ended"),
    *[(broker_client.BrokerUnreachable(reason), f"unreachable_{reason}") for reason in broker_client.REASONS],
])
def test_a_session_the_broker_ends_is_forgotten_and_the_login_page_says_why(client, broker, app, make_client, error, why):
    """Bug class: a page that shows a broker error forever (or a stale session that seems logged in) after the broker restarted, the password changed or the session ran out."""
    cookie = session_cookie(log_in(client))
    probe = make_client(app)
    assert probe.get("/", headers=with_cookie(cookie)).status_code == 200
    broker.calls.clear()
    broker.answers["source.list"] = error
    response = client.get("/?fresh=1")
    assert response.status_code == 303 and response.headers["location"] == f"/login?why={why}"
    assert "__Host-sdp_session=; Path=/; Secure; HttpOnly; SameSite=Strict; Max-Age=0" in response.headers.get_list("set-cookie")
    page = client.get(response.headers["location"])
    assert login_line(page) == webapp.LOGIN_LINES[why].replace("'", "&#39;")
    assert broker.verbs_called() == ["db.status", "source.list"]
    broker.answers["source.list"] = source_list()
    assert probe.get("/", headers=with_cookie(cookie)).status_code == 303


def test_an_unknown_why_shows_nothing(client):
    """Bug class: the login page echoing a query value (text of an attacker's choosing shown as the web UI's own words)."""
    page = client.get("/login?why=%3Cb%3Emade-up%3C%2Fb%3E")
    assert login_line(page) is None and "made-up" not in page.text


def test_the_logout_forgets_the_session_logs_out_at_the_broker_and_clears_the_cookie(client, broker, app, make_client):
    """Bug class: a logout that leaves the id usable (a stolen cookie outlives the logout) or the broker's session alive."""
    cookie = session_cookie(log_in(client))
    probe = make_client(app)
    assert probe.get("/", headers=with_cookie(cookie)).status_code == 200
    response = client.post("/logout", data={"form_token": form_token(client)}, headers={"origin": ORIGIN})
    assert response.status_code == 303 and response.headers["location"] == "/login?why=logged_out"
    assert response.headers.get_list("set-cookie") == ["__Host-sdp_session=; Path=/; Secure; HttpOnly; SameSite=Strict; Max-Age=0"]
    assert broker.calls[-1] == ("logout", TOKEN)
    assert probe.get("/", headers=with_cookie(cookie)).status_code == 303


def test_the_logout_reaches_the_broker_while_every_login_place_is_taken(make_app, make_client, broker):
    """Bug class: the logout passed to the broker under the login bound, which anyone can fill: the session is forgotten here, the broker is never told, and its token stays good for up to eight hours."""
    import threading
    import time

    from starlette.testclient import TestClient

    app = make_app()
    client = make_client(app)
    log_in(client)
    token = form_token(client)
    broker.entered = 0
    broker.gate = threading.Event()
    broker.gate_kinds = {"login"}
    with TestClient(app, base_url="https://localhost:8443", headers={"host": HOST}, client=(REMOTE, 50000), follow_redirects=False) as stranger:
        waiting = [threading.Thread(target=lambda: log_in(stranger, "made-up-wrong-pw")) for _ in range(2)]
        for thread in waiting:
            thread.start()
        deadline = time.monotonic() + 10
        while broker.entered < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert broker.entered == 2
        assert log_in(stranger).status_code == 503
        response = client.post("/logout", data={"form_token": token}, headers={"origin": ORIGIN})
        broker.gate.set()
        for thread in waiting:
            thread.join(10)
    assert response.headers["location"] == "/login?why=logged_out"
    assert ("logout", TOKEN) in broker.calls


def test_a_logout_whose_bound_stays_full_says_so(make_app, make_client, broker):
    """Bug class: a logout that silently skips the broker when its own bound stays full."""
    import threading

    app = make_app(logout_workers=1, logout_wait_seconds=0.2)
    first, second = make_client(app), make_client(app, address=REMOTE)
    log_in(first)
    log_in(second)
    tokens = form_token(first), form_token(second)
    broker.gate = threading.Event()
    broker.gate_kinds = {"logout"}
    blocked = threading.Thread(target=lambda: first.post("/logout", data={"form_token": tokens[0]}, headers={"origin": ORIGIN}))
    blocked.start()
    import time
    deadline = time.monotonic() + 10
    while broker.kinds().count("logout") < 1 and time.monotonic() < deadline:
        time.sleep(0.01)
    response = second.post("/logout", data={"form_token": tokens[1]}, headers={"origin": ORIGIN})
    broker.gate.set()
    blocked.join(10)
    assert response.headers["location"] == "/login?why=logged_out_broker_busy"
    assert "__Host-sdp_session=; Path=/; Secure; HttpOnly; SameSite=Strict; Max-Age=0" in response.headers.get_list("set-cookie")
    assert broker.kinds().count("logout") == 1


def test_the_session_table_is_bounded(make_app, make_client):
    """Bug class: one session per login for ever (memory without bound for a login loop)."""
    app = make_app(sessions_max=2)
    clients = [make_client(app) for _ in range(3)]
    for client in clients:
        assert log_in(client).status_code == 303
    assert [client.get("/").status_code for client in clients] == [303, 200, 200]


def test_a_logged_in_browser_is_sent_from_the_login_page_to_the_overview(client):
    """Bug class: a login form shown to a logged-in browser (a second login with a new id each time it is opened)."""
    log_in(client)
    response = client.get("/login")
    assert response.status_code == 303 and response.headers["location"] == "/"
