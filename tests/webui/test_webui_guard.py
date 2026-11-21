"""Tests for the request checks and response headers of the web UI (`webui.guard`), through the whole application with a stand-in broker.

The contracts pinned here, each test naming the bug class it catches:

- a. `Host`: every configured value is accepted (with the port; without it only when the port is 443), any other is refused with 400 and a constant body, on every route: pages, static files, the health check and error answers.
- b. A request whose method is not GET or HEAD needs `Origin: https://<host>`, or no `Origin` and `Sec-Fetch-Site: same-origin`; anything else is 403 and reaches no broker.
- c. A logged-in session's request that changes something needs the session's form token: missing, wrong and another session's tokens are 403; the token stays the same for the session's life and passes from the form field or the header.
- j2. A body that has not arrived within ten seconds is 408 (the clock injected), also when it never comes.
- f. Every response, of every route and status, has exactly the security headers, `no-store` (static files: cacheable), `charset=utf-8` on text, and no `Strict-Transport-Security` and no `Server`.
- j. A body over 64 KiB is 413 before it is parsed (by its length, or as it streams); a JSON or multipart body is refused.
"""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette.testclient")

from webui_helpers import ORIGIN, PASSWORD, REMOTE, form_token, log_in

from social_data_pipeline.webui import app as webapp
from social_data_pipeline.webui import config, guard

CSP = "default-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'; object-src 'none'"
GET_ROUTES = [
    "/", "/login", "/healthz", "/sources", "/sources/demo", "/databases", "/databases/postgres", "/runs", "/runs/r20261121T101500-0a1b2c", "/runs/r20261121T101500-0a1b2c/output?offset=0&view=0123456789abcdef", "/system",
    "/pending/AAAAAAAAAAAAAAAAAAAAAA", "/actions/AAAAAAAAAAAAAAAAAAAAAA", "/static/app.css", "/static/app.js", "/static/vendor/htmx.min.js", "/no-such-page",
]
POST_ROUTES = ["/login", "/logout", "/", "/healthz", "/pending", "/pending/AAAAAAAAAAAAAAAAAAAAAA/confirm", "/no-such-page"]


@pytest.fixture
def app(make_app):
    return make_app()


@pytest.fixture
def client(app, make_client):
    return make_client(app)


def refused_with(response, status):
    return response.status_code == status and response.text == guard.REFUSALS[status]


def unknown_host(response):
    return response.status_code == 400 and response.text == guard.UNKNOWN_HOST


# ── a. Host ─────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("host", ["localhost:8443", "127.0.0.1:8443", "[2001:db8::5]:8443", "pipeline.example:8443"])
def test_every_configured_host_is_accepted(client, host):
    """Bug class: a configured name or address refused (an IPv6 address in brackets, a name), so the operator cannot reach the page by it."""
    assert client.get("/healthz", headers={"host": host}).status_code == 200


def test_on_port_443_the_host_may_come_without_the_port(make_app, make_client):
    """Bug class: a page on the default HTTPS port refused for the Host every browser sends there (without the port)."""
    settings = config.Settings(hosts=("localhost", "pipeline.example"), port=443, socket_path="/run/sdp/broker.sock", gateway="172.30.0.1")
    client = make_client(make_app(settings=settings), base_url="https://localhost")
    for host in ("localhost", "localhost:443", "pipeline.example", "pipeline.example:443"):
        assert client.get("/healthz", headers={"host": host}).status_code == 200, host
    assert unknown_host(client.get("/healthz", headers={"host": "localhost:8443"}))


@pytest.mark.parametrize("route", GET_ROUTES)
@pytest.mark.parametrize("host", ["evil.example:8443", "localhost", "localhost:8444", "LOCALHOST:8443", "localhost:8443.", "127.0.0.2:8443", "[::1]:8443", "pipeline.example.evil.example:8443", ""])
def test_any_other_host_is_refused_on_every_route(client, route, host):
    """Bug class: DNS rebinding: a page of another origin that points its own name at this machine reads the answers, because one route (a static file, the health check, an error page) skips the check."""
    response = client.get(route, headers={"host": host})
    assert unknown_host(response)
    assert response.headers["content-security-policy"] == CSP


def test_other_hosts_are_refused_for_every_method_and_reach_no_broker(client, broker):
    """Bug class: the Host check applied to GET only, so a rebinding page can still post a login."""
    for route in POST_ROUTES:
        response = client.post(route, data={"password": PASSWORD}, headers={"host": "evil.example:8443", "origin": "https://evil.example:8443"})
        assert unknown_host(response)
    assert broker.calls == []


# ── b. Origin ───────────────────────────────────────────────────────────────


@pytest.fixture
def logged_in(client, broker):
    assert log_in(client).status_code == 303
    broker.calls.clear()
    return client


@pytest.mark.parametrize("route", POST_ROUTES)
@pytest.mark.parametrize("origin", ["https://evil.example", "null", "http://localhost:8443", "https://localhost:8444", "https://localhost", "https://127.0.0.1:8443", "https://localhost:8443/", "HTTPS://localhost:8443"])
def test_a_foreign_origin_is_refused_on_every_non_get_route(logged_in, broker, route, origin):
    """Bug class: cross-site request forgery: a page of another origin (or a sandboxed one, `null`, or one served over plain HTTP or another port) makes the operator's browser log out, log in or post to any route."""
    response = logged_in.post(route, data={"password": PASSWORD, "form_token": form_token(logged_in)}, headers={"origin": origin})
    assert refused_with(response, 403)
    assert [call for call in broker.calls if call[0] != "call"] == []
    assert logged_in.get("/").status_code == 200


@pytest.mark.parametrize("fetch_site", [None, "cross-site", "same-site", "none", "same-origin, cross-site"])
def test_without_an_origin_only_same_origin_fetch_metadata_passes(client, broker, fetch_site):
    """Bug class: a request without `Origin` taken as same-origin, which an old browser or a stripped header would turn into a forgery."""
    headers = {} if fetch_site is None else {"sec-fetch-site": fetch_site}
    response = client.post("/login", data={"password": PASSWORD}, headers=headers)
    assert refused_with(response, 403)
    assert broker.calls == []


@pytest.mark.parametrize("route", POST_ROUTES)
def test_the_right_origin_or_same_origin_fetch_metadata_passes(logged_in, route):
    """Bug class: a check so strict that the page's own forms are refused."""
    for headers in ({"origin": ORIGIN}, {"sec-fetch-site": "same-origin"}):
        response = logged_in.post(route, data={"form_token": form_token(logged_in)} if route != "/login" else {"password": PASSWORD}, headers=headers)
        assert not refused_with(response, 403), (route, headers)
        if route == "/logout":
            assert log_in(logged_in).status_code == 303


def test_two_origin_headers_are_refused(client, broker):
    """Bug class: the first of two `Origin` headers checked and the second believed by something else."""
    response = client.post("/login", data={"password": PASSWORD}, headers=[("origin", ORIGIN), ("origin", "https://evil.example")])
    assert refused_with(response, 403)
    assert broker.calls == []


# ── c. the form token ───────────────────────────────────────────────────────


def test_a_missing_or_wrong_form_token_is_refused_and_the_session_stays(logged_in, broker):
    """Bug class: a logged-in request that changes something accepted on the strength of the cookie and the origin alone."""
    token = form_token(logged_in)
    for data in ({}, {"form_token": ""}, {"form_token": token[:-1] + ("A" if token[-1] != "A" else "B")}, {"form_token": token + "x"}):
        response = logged_in.post("/logout", data=data, headers={"origin": ORIGIN})
        assert refused_with(response, 403)
    assert broker.kinds() == ["call", "call", "call"]
    assert logged_in.get("/").status_code == 200


def test_the_form_token_is_the_sessions_own_and_stays_the_same(logged_in, app, make_client, broker):
    """Bug class: a token of another session accepted (a token is tied to one session only), or a token that changes under a page that is still open (its next form refused)."""
    token = form_token(logged_in)
    assert logged_in.post("/no-such-page", data={"form_token": token}, headers={"origin": ORIGIN}).status_code in (404, 405)
    assert form_token(logged_in) == token
    other = make_client(app, address="192.0.2.77")
    log_in(other)
    assert refused_with(other.post("/logout", data={"form_token": token}, headers={"origin": ORIGIN}), 403)
    assert logged_in.post("/logout", data={"form_token": token}, headers={"origin": ORIGIN}).status_code == 303
    assert ("logout", "made-up-broker-token-9c1f") in broker.calls


def test_the_form_token_may_come_in_the_header(logged_in):
    """Bug class: htmx requests (which send the header) refused, or the header accepted without being compared."""
    assert refused_with(logged_in.post("/logout", headers={"origin": ORIGIN, "x-sdp-form-token": "made-up-wrong"}), 403)
    assert logged_in.post("/logout", headers={"origin": ORIGIN, "x-sdp-form-token": form_token(logged_in)}).status_code == 303


def test_the_login_needs_no_form_token(client):
    """Bug class: a login refused because no session (and so no token) exists yet."""
    assert log_in(client).status_code == 303
    assert log_in(client).status_code == 303


# ── f. headers ──────────────────────────────────────────────────────────────


def every_kind_of_answer(app, client, make_client, monkeypatch):
    """Requests that between them get every status the application gives: 200, 303, 400, 403, 404, 405, 413, 415, 429, 500 and 503."""
    answers = [client.get(route) for route in GET_ROUTES]
    answers += [client.head(route) for route in ("/healthz", "/login", "/static/app.css")]
    answers.append(client.get("/healthz", headers={"host": "evil.example"}))
    answers.append(client.post("/login", data={"password": "x"}, headers={"origin": "https://evil.example"}))
    answers.append(client.post("/login", content=b"x" * 70000, headers={"origin": ORIGIN, "content-type": guard.FORM_TYPE}))
    answers.append(client.post("/login", json={"password": PASSWORD}, headers={"origin": ORIGIN}))
    answers.append(client.put("/login", data={"a": "b"}, headers={"origin": ORIGIN}))
    for _ in range(6):
        answers.append(log_in(client, "made-up-wrong-pw"))
    other = make_client(app, address=REMOTE)
    answers.append(log_in(other))
    answers += [other.get(route) for route in GET_ROUTES]
    answers.append(other.post("/logout", data={"form_token": form_token(other)}, headers={"origin": ORIGIN}))
    answers.append(log_in(other))
    monkeypatch.setattr(webapp.overview, "build", lambda answers: 1 / 0)
    answers.append(other.get("/"))
    return answers


def test_every_answer_has_exactly_the_security_headers(app, client, make_client, monkeypatch):
    """Bug class: a route, a status or a static file without the CSP (framing, injected script), without nosniff, cacheable, or with HSTS (which would pin a self-signed name in the browser for good) or a Server banner."""
    answers = every_kind_of_answer(app, client, make_client, monkeypatch)
    statuses = {answer.status_code for answer in answers}
    assert {200, 303, 400, 403, 404, 405, 413, 415, 429, 500} <= statuses
    for answer in answers:
        headers = answer.headers
        assert headers.get_list("content-security-policy") == [CSP]
        assert headers.get_list("x-content-type-options") == ["nosniff"]
        assert headers.get_list("referrer-policy") == ["same-origin"]
        static = answer.request.url.path.startswith("/static/") and answer.request.headers.get("host") == "localhost:8443"
        assert headers.get_list("cache-control") == (["max-age=3600"] if static else ["no-store"]), answer.request.url.path
        assert "strict-transport-security" not in headers and "server" not in headers
        content_type = headers.get("content-type", "")
        if content_type.startswith("text/") or "javascript" in content_type:
            assert "charset=utf-8" in content_type, (answer.request.url.path, content_type)


@pytest.mark.parametrize("media", [b"text/html", b"text/css", b"text/plain", b"application/javascript", b"application/json"])
def test_a_text_type_without_a_charset_gets_utf8(media):
    """Bug class: a text answer without a charset (the routes' answers have one from Starlette; this layer adds it to any that has not), which a browser may then read in another encoding."""
    headers = guard._content_type([(b"content-type", media), (b"server", b"made-up"), (b"cache-control", b"public")], "/x")
    assert (b"content-type", media + b"; charset=utf-8") in headers
    assert (b"server", b"made-up") not in headers and headers.count((b"cache-control", b"no-store")) == 1
    assert guard._content_type([(b"content-type", b"image/png")], "/static/x.png")[0] == (b"content-type", b"image/png")


def test_a_failing_page_is_a_constant_500(logged_in, monkeypatch, caplog):
    """Bug class: an exception's text or traceback sent to the browser, or logged with the values in its frames."""
    monkeypatch.setattr(webapp.overview, "build", lambda answers: (_ for _ in ()).throw(RuntimeError("made-up-secret-in-an-exception")))
    caplog.set_level("DEBUG")
    response = logged_in.get("/")
    assert response.status_code == 500 and "made-up-secret" not in response.text
    assert "made-up-secret" not in caplog.text


# ── j. the body ─────────────────────────────────────────────────────────────


def test_a_body_over_64_kib_is_refused_before_it_is_parsed(client, broker):
    """Bug class: a large body read and parsed (memory and time for anyone who can reach the port), refused only after the broker was asked."""
    big = b"password=" + b"x" * (64 * 1024)
    response = client.post("/login", content=big, headers={"origin": ORIGIN, "content-type": guard.FORM_TYPE})
    assert refused_with(response, 413)

    def streamed():
        for _ in range(70):
            yield b"x" * 1024

    response = client.post("/login", content=streamed(), headers={"origin": ORIGIN, "content-type": guard.FORM_TYPE})
    assert refused_with(response, 413)
    assert broker.calls == []
    exactly = b"password=" + PASSWORD.encode() + b"&pad=" + b"y" * (64 * 1024 - len(PASSWORD) - 14)
    assert len(exactly) == 64 * 1024
    assert client.post("/login", content=exactly, headers={"origin": ORIGIN, "content-type": guard.FORM_TYPE}).status_code == 303


def test_a_content_length_over_the_limit_is_refused_without_reading(client, broker):
    """Bug class: the length believed only after the body was read."""
    response = client.post("/login", content=b"password=x", headers={"origin": ORIGIN, "content-type": guard.FORM_TYPE, "content-length": str(64 * 1024 + 1)})
    assert refused_with(response, 413)
    assert broker.calls == []


@pytest.mark.parametrize("kind", ["json", "multipart", "text", "none with a body"])
def test_only_form_bodies_are_accepted(client, broker, kind):
    """Bug class: a JSON or multipart body parsed by another parser (with its own limits and quirks), or a body without a type taken as a form."""
    if kind == "json":
        response = client.post("/login", json={"password": PASSWORD}, headers={"origin": ORIGIN})
    elif kind == "multipart":
        response = client.post("/login", files={"password": ("p", PASSWORD.encode())}, headers={"origin": ORIGIN})
    elif kind == "text":
        response = client.post("/login", content=f"password={PASSWORD}".encode(), headers={"origin": ORIGIN, "content-type": "text/plain"})
    else:
        response = client.post("/login", content=f"password={PASSWORD}".encode(), headers={"origin": ORIGIN, "content-type": ""})
    assert refused_with(response, 415)
    assert broker.calls == []


@pytest.mark.parametrize("body", [b"password=a&password=b", b"%ff=1", "&".join(f"f{number}=1" for number in range(65)).encode()])
def test_a_form_with_a_field_twice_too_many_fields_or_no_utf8_is_refused(client, broker, body):
    """Bug class: two values of one field read differently by two readers, or a form of thousands of fields parsed in full."""
    response = client.post("/login", content=body, headers={"origin": ORIGIN, "content-type": guard.FORM_TYPE})
    assert refused_with(response, 400)
    assert broker.calls == []


def _guard_on(make_app_guard_settings, clock, body_seconds=10):
    from social_data_pipeline.webui.sessions import WebSessions

    reached = []

    async def app(scope, receive, send):
        reached.append(scope["path"])
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
        await send({"type": "http.response.body", "body": b"reached"})

    return guard.Guard(app, settings=make_app_guard_settings, sessions=WebSessions(clock), body_seconds=body_seconds, clock=clock), reached


def _post(layer, receive):
    import anyio

    sent = []

    async def send(message):
        sent.append(message)

    scope = {"type": "http", "method": "POST", "path": "/login", "headers": [(b"host", b"localhost:8443"), (b"origin", ORIGIN.encode()), (b"content-type", guard.FORM_TYPE.encode())], "client": ("192.0.2.7", 1), "server": ("localhost", 8443)}

    async def bounded():
        # A layer that waits for ever fails the test here instead of hanging it.
        with anyio.fail_after(5):
            await layer(scope, receive, send)

    anyio.run(bounded)
    return next(message["status"] for message in sent if message["type"] == "http.response.start")


def test_a_body_that_takes_longer_than_its_time_is_refused(settings):
    """Bug class: a client that sends its body a byte at a time holding a connection (and a slot of the server's limit) for as long as it likes."""
    from webui_helpers import Clock

    clock = Clock()
    layer, reached = _guard_on(settings, clock)
    chunks = [b"password=", b"x", b"y"]

    async def receive():
        clock.now += 4
        return {"type": "http.request", "body": chunks.pop(0), "more_body": bool(chunks)}

    assert _post(layer, receive) == 408
    assert reached == []
    clock.now += 100
    chunks[:] = [b"password=", b"x"]
    clock.now = 0

    async def quick():
        clock.now += 1
        return {"type": "http.request", "body": chunks.pop(0), "more_body": bool(chunks)}

    assert _post(layer, quick) == 200 and reached == ["/login"]


def test_a_body_that_never_comes_is_refused_in_time(settings):
    """Bug class: the time limit checked only between pieces, so a client that sends nothing after the headers waits for ever."""
    import time

    import anyio

    layer, reached = _guard_on(settings, time.monotonic, body_seconds=0.2)

    async def never():
        await anyio.sleep_forever()

    started = time.monotonic()
    assert _post(layer, never) == 408
    assert time.monotonic() - started < 2 and reached == []


def test_a_websocket_is_refused(client):
    """Bug class: a WebSocket route reachable around the origin and form-token checks."""
    from starlette.websockets import WebSocketDisconnect

    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/"):
            pass


def test_a_client_from_another_address_is_checked_the_same_way(make_app, make_client, broker):
    """Bug class: checks skipped for clients of one kind of address."""
    client = make_client(make_app(), address=REMOTE)
    assert refused_with(client.post("/login", data={"password": PASSWORD}, headers={"origin": "https://evil.example"}), 403)
    assert broker.calls == []
