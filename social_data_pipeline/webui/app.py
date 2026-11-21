"""The web UI's application: login, logout, the frame and the read-only Overview, behind the `guard` layer.

`create_app(settings, broker=...)` builds it; `from_environment()` is what the container runs (`python -m social_data_pipeline.webui.serve` builds it once and serves it on two ports). `broker(token=None)` returns a client of the broker (`control.client.Client` or a stand-in) with `login(password, channel=...)`, `logout()` and `call(verb, fields)`; the clock, the limits, the template and static folders are parameters too.

Routes, each behind the checks of `guard` (host, origin, body, form token):

- `GET /healthz`: `ok`, without a session.
- `GET /login`: the login page; with a live session, a redirect to the Overview. `?why=` picks one of the constant lines of `LOGIN_LINES`; any other value shows nothing.
- `POST /login`: the per-address limit (`limiter`: refused without asking the broker), then the broker's `login` with the channel `local` when the connection has both signs of the machine itself (it arrived on the local port, `config.LOCAL_PORT`, to which only 127.0.0.1 of the host is published, and its client address is the network's gateway, as which Docker's proxy shows a client on 127.0.0.1) and `remote` otherwise; on success a new session id in the cookie and the broker's token in memory only; the id the browser sent before is forgotten. Local logins share one key of the per-address limit; any other login is counted under its client address.
- `POST /logout`: the session forgotten, the broker's `logout` asked (under a bound of its own, waiting a few seconds for a place), the cookie cleared.
- `GET /`: the Overview, from three read verbs; a block whose read failed says why, and the others stay. A session the broker no longer knows (`unauthorized`, `session_ended`) or a broker that cannot be reached ends the session here too, and the browser is sent to the login page with a line saying why.
- `GET /sources`, `/sources/<name>`, `/databases`, `/databases/<name>`, `/runs`, `/system`: pages that say they come with the next update, for a live session.
- `/static/...`: the stylesheet, the script and, in the image, htmx and Pico CSS.

Every broker call runs in a worker thread, never on the event loop, under a bound of its kind (pages; logins on the local port, one place; other logins, two places; logouts): a page or a login that finds its bound full is answered at once with a "busy" page instead of waiting, so a slow broker can stall neither the login page nor the health check, and logins from elsewhere cannot make a local login busy. Every state-changing form is answered with a redirect to a GET, so a reload never sends a form again. The password is read from the form, given to the broker and dropped; no template, log line or answer gets it, and the broker's token never leaves this process but towards the broker.
"""

import functools
import inspect
import logging
import os
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import anyio
import jinja2
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException

from social_data_pipeline.control import client as broker_client
from social_data_pipeline.webui import config, guard, overview, words
from social_data_pipeline.webui.limiter import LOCAL_KEY, LoginLimiter
from social_data_pipeline.webui.sessions import WebSessions

PACKAGE = Path(__file__).resolve().parent
TEMPLATES_DIR = PACKAGE / "templates"
STATIC_DIR = PACKAGE / "static"
# The longest a call may take from the web UI's side: the broker gives a read 60 seconds.
BROKER_SECONDS = 75

COOKIE_ATTRIBUTES = "Path=/; Secure; HttpOnly; SameSite=Strict"
# What ends the web UI's session when the broker answers it.
SESSION_ENDING_CODES = frozenset({"unauthorized", "session_ended"})

LOGIN_LINES = {
    "ended": "Your session has ended: log in again.",
    "logged_out": "You have logged out.",
    "logged_out_broker_busy": "You have logged out of the web UI. The broker could not be told in time: its session ends by itself within eight hours, and its token was only in the web UI's memory, which has dropped it.",
    **{f"unreachable_{reason}": words.REASON_SENTENCES[reason] for reason in broker_client.REASONS},
}
WRONG_PASSWORD = "The password is wrong."
LIMITED = "There were too many failed logins from this address: wait five minutes before the next attempt."
BUSY = "The web UI is busy with other requests: try again in a moment."
NEXT_UPDATE = "This page comes with the next update of the web UI."

# The Management pages of the frame: (key, title, address); the Overview is the only one with content in this version.
MANAGEMENT_PAGES = (
    ("overview", "Overview", "/"),
    ("sources", "Sources", "/sources"),
    ("databases", "Databases", "/databases"),
    ("runs", "Runs", "/runs"),
    ("system", "System", "/system"),
)

log = logging.getLogger("social_data_pipeline.webui")


@dataclass(frozen=True)
class Limits:
    """The bounds of the application. The defaults are the ones it runs with; a test passes smaller ones."""

    body_max: int = guard.BODY_MAX_BYTES
    sessions_max: int = 256
    login_failures: int = 5
    login_window_seconds: float = 5 * 60
    addresses_max: int = 4096
    page_workers: int = 4
    local_login_workers: int = 1
    remote_login_workers: int = 2
    logout_workers: int = 2
    logout_wait_seconds: float = 5
    body_seconds: float = guard.BODY_SECONDS


class _Busy:
    """What `Gate.run` returns when its bound is full."""


BUSY_GATE = _Busy()


class Gate:
    """A bound on the worker threads that call the broker for one kind of request. `run(function, wait_seconds=0)` runs it in a worker thread and returns its result, or `BUSY_GATE` when `workers` calls of this gate already run and no place came free within `wait_seconds` (looked for every 50 ms, without blocking the event loop). The place is given back only when the thread has ended, also when the client went away."""

    def __init__(self, workers):
        self._places = threading.BoundedSemaphore(workers)

    async def run(self, function, wait_seconds=0):
        deadline = time.monotonic() + wait_seconds
        while not self._places.acquire(blocking=False):
            if time.monotonic() >= deadline:
                return BUSY_GATE
            await anyio.sleep(0.05)
        try:
            return await anyio.to_thread.run_sync(function)
        finally:
            self._places.release()


def arrived_locally(scope, gateway):
    """Whether a request is from the machine itself: it came in on the local port (`config.LOCAL_PORT`, the container port to which only 127.0.0.1 of the host is published) and its client address is the network's gateway (`gateway`). One sign alone is not enough (see `config`)."""
    server = scope.get("server")
    client = scope.get("client")
    on_local_port = bool(server) and len(server) >= 2 and server[1] == config.LOCAL_PORT
    from_gateway = bool(client) and len(client) >= 1 and client[0] == gateway
    return on_local_port and from_gateway


def _call_broker(make_client, function):
    """Run `function(client)` and return ("ok", its result) or ("error", the BrokerError). Any other exception is turned into ("error", an `internal` BrokerError) here, in the worker, so that no traceback with a password or a token in its frames leaves the thread."""
    try:
        return "ok", function(make_client())
    except broker_client.BrokerError as error:
        return "error", error
    except Exception as error:
        log.error("a broker call failed (%s)", type(error).__name__)
        return "error", broker_client.BrokerError("internal", "the web UI could not use the answer")


def _session_ends(error):
    return isinstance(error, broker_client.BrokerUnreachable) or getattr(error, "code", None) in SESSION_ENDING_CODES


def _why(error):
    """The `why` of the login page for an error that ended a session."""
    if isinstance(error, broker_client.BrokerUnreachable) and error.reason in broker_client.REASONS:
        return f"unreachable_{error.reason}"
    return "ended"


def create_app(settings, *, broker, clock=time.monotonic, limits=Limits(), templates_dir=TEMPLATES_DIR, static_dir=STATIC_DIR):
    """The web UI as an ASGI application: the FastAPI routes inside the `guard` layer."""
    sessions = WebSessions(clock, sessions_max=limits.sessions_max)
    limiter = LoginLimiter(clock, failures_max=limits.login_failures, window_seconds=limits.login_window_seconds, addresses_max=limits.addresses_max)
    pages = Gate(limits.page_workers)
    local_logins = Gate(limits.local_login_workers)
    remote_logins = Gate(limits.remote_login_workers)
    logouts = Gate(limits.logout_workers)
    templates = jinja2.Environment(loader=jinja2.FileSystemLoader(str(templates_dir)), autoescape=True, undefined=jinja2.StrictUndefined)

    api = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    api.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    @api.exception_handler(StarletteHTTPException)
    async def http_error(request, error):
        status = error.status_code if 400 <= error.status_code < 600 else 500
        return PlainTextResponse(guard.REFUSALS.get(status, "The request was refused.\n"), status_code=status)

    def render(name, status=200, **context):
        return HTMLResponse(templates.get_template(name).render(**context), status_code=status)

    def frame(name, active, session, status=200, **context):
        return render(name, status, active=active, pages=MANAGEMENT_PAGES, form_token=session.form_token, **context)

    def to_login(why=None, clear_cookie=False):
        response = RedirectResponse("/login" + (f"?why={why}" if why else ""), status_code=303)
        if clear_cookie:
            response.headers.append("set-cookie", f"{guard.COOKIE_NAME}=; {COOKIE_ATTRIBUTES}; Max-Age=0")
        return response

    def current(request):
        session_id = request.scope.get("sdp.session_id")
        session = sessions.get(session_id) if session_id else None
        return session_id, session

    def login_page(status=200, line=None):
        return render("login.html", status, line=line)

    @api.api_route("/healthz", methods=["GET", "HEAD"])
    async def healthz():
        return PlainTextResponse("ok")

    @api.api_route("/login", methods=["GET", "HEAD"])
    async def login_form(request: Request):
        _, session = current(request)
        if session is not None:
            return RedirectResponse("/", status_code=303)
        why = request.query_params.get("why")
        return login_page(line=LOGIN_LINES.get(why) if isinstance(why, str) else None)

    @api.post("/login")
    async def login(request: Request):
        form = request.scope.get("sdp.form", {})
        password = form.pop("password", "")
        local = arrived_locally(request.scope, settings.gateway)
        attempt = limiter.begin(LOCAL_KEY if local else (request.client.host if request.client else None))
        if attempt is None:
            return login_page(429, LIMITED)
        channel = "local" if local else "remote"

        def ask(session):
            session.login(password, channel=channel)
            return session.token

        # The place in the limiter is given back on every way out (a busy bound, any answer, a request cancelled while it waits), and kept as a failure only for a wrong password.
        failed = False
        try:
            outcome = await (local_logins if local else remote_logins).run(lambda: _call_broker(broker, ask))
            password = None
            if outcome is BUSY_GATE:
                return login_page(503, BUSY)
            kind, value = outcome
            if kind == "error":
                failed = value.code == "unauthorized"
                if failed:
                    return login_page(403, WRONG_PASSWORD)
                return login_page(429 if value.code == "throttled" else 503, words.sentence(value))
        finally:
            limiter.end(attempt, failed=failed)
        if not isinstance(value, str) or not value:
            return login_page(503, words.code_sentence("bad_reply"))
        old_id = request.scope.get("sdp.session_id")
        if old_id:
            sessions.forget(old_id)
        session_id, _ = sessions.create(value)
        response = RedirectResponse("/", status_code=303)
        response.headers.append("set-cookie", f"{guard.COOKIE_NAME}={session_id}; {COOKIE_ATTRIBUTES}")
        return response

    @api.post("/logout")
    async def logout(request: Request):
        session_id, _ = current(request)
        session = sessions.forget(session_id) if session_id else None
        if session is not None:
            done = await logouts.run(lambda: _call_broker(lambda: broker(token=session.token), lambda client: client.logout()), wait_seconds=limits.logout_wait_seconds)
            if done is BUSY_GATE:
                log.warning("a logout could not be passed on to the broker: its bound stayed full")
                return to_login("logged_out_broker_busy", clear_cookie=True)
        return to_login("logged_out", clear_cookie=True)

    @api.api_route("/", methods=["GET", "HEAD"])
    async def overview_page(request: Request):
        session_id, session = current(request)
        if session is None:
            return to_login(clear_cookie=bool(session_id))

        def read_all():
            # One after the other in one worker: a page takes one place of the bound. A session that has ended stops the reads.
            answers = {}
            for verb in overview.READ_VERBS:
                answers[verb] = _call_broker(lambda: broker(token=session.token), lambda made, verb=verb: made.call(verb, {}))
                if answers[verb][0] == "error" and _session_ends(answers[verb][1]):
                    break
            return answers

        answers = await pages.run(read_all)
        if answers is BUSY_GATE:
            return frame("busy.html", "overview", session, 503, title="Overview", line=BUSY)
        for outcome, value in answers.values():
            if outcome == "error" and _session_ends(value):
                sessions.forget(session_id)
                return to_login(_why(value), clear_cookie=True)
        return frame("overview.html", "overview", session, title="Overview", blocks=overview.build(answers))

    def placeholder(request, active, title):
        session_id, session = current(request)
        if session is None:
            return to_login(clear_cookie=bool(session_id))
        return frame("placeholder.html", active, session, title=title, line=NEXT_UPDATE)

    for key, title, address in MANAGEMENT_PAGES[1:]:
        def page(request: Request, key=key, title=title):
            return placeholder(request, key, title)

        api.add_api_route(address, _as_async(page), methods=["GET", "HEAD"])

    for key, title, prefix in (("sources", "Source", "/sources/"), ("databases", "Database", "/databases/")):
        def named(request: Request, name: str, key=key, title=title):
            if overview.link("/", name) is None:
                return PlainTextResponse(guard.REFUSALS.get(404, "Not found.\n"), status_code=404)
            return placeholder(request, key, f"{title} {name}")

        api.add_api_route(prefix + "{name}", _as_async(named), methods=["GET", "HEAD"])

    return guard.Guard(api, settings=settings, sessions=sessions, body_max=limits.body_max, body_seconds=limits.body_seconds, clock=clock)


def _as_async(function):
    """`function` as a coroutine function with the same signature, so that FastAPI runs it on the event loop (it calls no broker) instead of taking a worker thread for it."""

    @functools.wraps(function)
    async def wrapper(*args, **kwargs):
        return function(*args, **kwargs)

    wrapper.__signature__ = inspect.signature(function)
    return wrapper


def from_environment():
    """The application the container runs, with the settings of its environment and a client of the broker's socket. Exits with one line when a setting is missing or malformed."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)
    try:
        settings = config.load_settings(os.environ)
    except config.ConfigError as error:
        raise SystemExit(f"Error: {error.message}") from None

    def broker(token=None):
        return broker_client.Client(settings.socket_path, timeout=BROKER_SECONDS, token=token)

    return create_app(settings, broker=broker)

