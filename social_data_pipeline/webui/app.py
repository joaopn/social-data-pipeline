"""The web UI's application: login, logout, the frame, the Overview, the Management pages of `management` and the Queue window of `queue`, behind the `guard` layer.

`create_app(settings, broker=..., scheduler=...)` builds it; `from_environment()` is what the container runs (`python -m social_data_pipeline.webui.serve` builds it once and serves it on two ports). `broker(token=None, timeout=...)` returns a client of the broker (`control.client.Client` or a stand-in) with `hello()`, `login(password, channel=...)`, `logout()`, `call(verb, fields, step_up=...)`, `set_db_password(password, step_up)` and `clear_db_password()`; a read gets the time limit `Limits.read_seconds`, a confirmed action none. `scheduler` is the way to the query scheduler's control socket (`scheduler.Scheduler`; by default made from the settings' `queue_socket` and `results`). Every request to the broker but the login and the logout, and every request to the scheduler, goes through `actions.call`, which holds it to the web UI's table. The clock, the limits, the template and static folders are parameters too.

Routes, each behind the checks of `guard` (host, origin, body, form token):

- `GET /healthz`: `ok`, without a session.
- `GET /login`: the login page; with a live session, a redirect to the Overview. `?why=` picks one of the constant lines of `LOGIN_LINES`; any other value shows nothing.
- `POST /login`: the per-address limit (`limiter`: refused without asking the broker), then the broker's `login` with the channel `local` when the connection has both signs of the machine itself (it arrived on the local port, `config.LOCAL_PORT`, to which only 127.0.0.1 of the host is published, and its client address is the network's gateway, as which Docker's proxy shows a client on 127.0.0.1) and `remote` otherwise; on success a new session id in the cookie and the broker's token in memory only; the id the browser sent before is forgotten. Local logins share one key of the per-address limit; any other login is counted under its client address.
- `POST /logout`: the session forgotten, the broker's `logout` asked (under a bound of its own, waiting a few seconds for a place), the cookie cleared.
- `GET /`: the Overview, from three read verbs; a block whose read failed says why, and the others stay. A session the broker no longer knows (`unauthorized`, `session_ended`) or a broker that cannot be reached ends the session here too, and the browser is sent to the login page with a line saying why.
- The Management pages (`/sources`, `/sources/<name>`, `/databases`, `/databases/<database>`, `/runs`, `/runs/<run id>`, `/system`), the actions (`POST /pending`, `/pending/<id>`, `POST /pending/<id>/confirm`, `/actions/<id>`) and the output view of a run (`/runs/<run id>/output`): see `management`.
- The Queue window (`/queue`, `/queue/running-rows`, `/queue/auto-accept`, `/queue/<job id>`, `/queue/<job id>/preview`, `/queue/<job id>/download/<part>`), whose actions go through the same pending, confirm and outcome routes: see `queue`.
- `/static/...`: the stylesheet, the script and, in the image, htmx and Pico CSS.

Every broker call runs in a worker thread, never on the event loop, under a bound of its kind (pages; logins on the local port, one place; other logins, two places; logouts; the pieces of the output view; Docker's tails of the output view; confirmed actions and, apart, cancels, in threads of their own): a page or a login that finds its bound full is answered at once with a "busy" page instead of waiting, so a slow broker can stall neither the login page nor the health check, and logins from elsewhere cannot make a local login busy. A choice of an action that is taken, a confirmation that is not refused at once, a login and a logout are answered with a redirect to a GET, so a reload never sends them again. The password is read from the form, given to the broker and dropped; no template, log line or answer gets it, and the broker's token never leaves this process but towards the broker.
"""

import logging
import os
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import jinja2
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException

from social_data_pipeline.control import client as broker_client
from social_data_pipeline.webui import actions, config, guard, management, overview, queue, words
from social_data_pipeline.webui.calls import BUSY_GATE, Gate, call_broker, session_ends, why
from social_data_pipeline.webui.limiter import LOCAL_KEY, LoginLimiter
from social_data_pipeline.webui.scheduler import Scheduler
from social_data_pipeline.webui.sessions import WebSessions
from social_data_pipeline.webui.work import SessionWork

PACKAGE = Path(__file__).resolve().parent
TEMPLATES_DIR = PACKAGE / "templates"
STATIC_DIR = PACKAGE / "static"
# The longest a read may take from the web UI's side: the broker gives a read 60 seconds. A state-changing call has no limit (see `management`).
BROKER_SECONDS = 75

COOKIE_ATTRIBUTES = "Path=/; Secure; HttpOnly; SameSite=Strict"

LOGIN_LINES = {
    "ended": "Your session has ended: log in again.",
    "logged_out": "You have logged out.",
    "logged_out_broker_busy": "You have logged out of the web UI. The broker could not be told in time: its session ends by itself within eight hours, and its token was only in the web UI's memory, which has dropped it.",
    **{f"unreachable_{reason}": words.REASON_SENTENCES[reason] for reason in broker_client.REASONS},
}
WRONG_PASSWORD = "The password is wrong."
LIMITED = "There were too many failed logins from this address: wait five minutes before the next attempt."
BUSY = "The web UI is busy with other requests: try again in a moment."

# The Management pages of the frame: (key, title, address).
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
    # 64 sessions of at most about 1.6 MiB each of outcomes, held lines and pending actions (see `work`): about 100 MiB against the container's 512 MiB.
    sessions_max: int = 64
    login_failures: int = 5
    login_window_seconds: float = 5 * 60
    addresses_max: int = 4096
    page_workers: int = 4
    local_login_workers: int = 1
    remote_login_workers: int = 2
    logout_workers: int = 2
    logout_wait_seconds: float = 5
    body_seconds: float = guard.BODY_SECONDS
    # The time limit of a read's client; a state-changing call's client has none.
    read_seconds: float = BROKER_SECONDS
    # Confirmed actions: the threads that run them, how long the confirm waits for an answer that comes at once.
    action_workers: int = 2
    # A cancel has places of its own, so that it gets through while long actions (a database start, a reset) hold the others.
    cancel_workers: int = 2
    action_wait_seconds: float = 2
    pending_max: int = 16
    pending_seconds: float = 5 * 60
    outcomes_max: int = 32
    outcome_text_bytes: int = 16 * 1024
    # The output view: Docker's tails have a bound of their own; the pieces asked, the views per session, the text held and shown per view.
    tail_workers: int = 2
    # The pieces of the output views have a bound of their own, so that open views cannot make the pages busy.
    piece_workers: int = 4
    log_piece_bytes: int = 65536
    views_max: int = 8
    view_held_bytes: int = 64 * 1024
    view_shown_bytes: int = 4 * 1024 * 1024
    # The scheduler's control socket: the calls of the Queue window (its pages and its confirmed actions) have a bound of their own, and so do the downloads.
    door_workers: int = 4
    download_workers: int = 2


def arrived_locally(scope, gateway):
    """Whether a request is from the machine itself: it came in on the local port (`config.LOCAL_PORT`, the container port to which only 127.0.0.1 of the host is published) and its client address is the network's gateway (`gateway`). One sign alone is not enough (see `config`)."""
    server = scope.get("server")
    client = scope.get("client")
    on_local_port = bool(server) and len(server) >= 2 and server[1] == config.LOCAL_PORT
    from_gateway = bool(client) and len(client) >= 1 and client[0] == gateway
    return on_local_port and from_gateway


@dataclass(frozen=True)
class Context:
    """What the routes of `management` share with the ones here: the rendering, the sessions, the bounds, the broker's client factory, the limits and the clock."""

    render: object
    frame: object
    current: object
    to_login: object
    sessions: WebSessions
    pages: Gate
    pieces: Gate
    tails: Gate
    action_places: threading.BoundedSemaphore
    cancel_places: threading.BoundedSemaphore
    broker: object
    limits: Limits
    clock: object
    scheduler: object = None
    door: Gate = None
    downloads: threading.BoundedSemaphore = None


def create_app(settings, *, broker, scheduler=None, clock=time.monotonic, limits=Limits(), templates_dir=TEMPLATES_DIR, static_dir=STATIC_DIR):
    """The web UI as an ASGI application: the FastAPI routes inside the `guard` layer."""
    if scheduler is None:
        scheduler = Scheduler(settings.queue_socket, settings.results)
    sessions = WebSessions(clock, sessions_max=limits.sessions_max, new_work=lambda: SessionWork(pending_max=limits.pending_max, pending_seconds=limits.pending_seconds, outcomes_max=limits.outcomes_max, views_max=limits.views_max))
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

    def render(name, status=200, headers=None, **context):
        return HTMLResponse(templates.get_template(name).render(**context), status_code=status, headers=headers)

    def frame(name, active, session, status=200, **context):
        """A page in the frame: the Queue window's navigation when `active` is one of its pages (`queue:...`), the Management window's otherwise."""
        window = "queue" if active.startswith("queue:") else "management"
        return render(name, status, active=active, window=window, pages=queue.QUEUE_PAGES if window == "queue" else MANAGEMENT_PAGES, form_token=session.form_token, **context)

    def to_login(request, why=None, clear_cookie=False):
        """The way to the login page: a redirect, or for a request of htmx (`HX-Request`) an empty answer whose `HX-Redirect` makes the browser load the login page, so that the login page is never swapped into a part of a page."""
        location = "/login" + (f"?why={why}" if why else "")
        if request.headers.get("hx-request") == "true":
            response = Response(status_code=204, headers={"HX-Redirect": location})
        else:
            response = RedirectResponse(location, status_code=303)
        if clear_cookie:
            response.headers.append("set-cookie", f"{guard.COOKIE_NAME}=; {COOKIE_ATTRIBUTES}; Max-Age=0")
        return response

    def current(request):
        session_id = request.scope.get("sdp.session_id")
        session = sessions.get(session_id) if session_id else None
        return session_id, session

    def login_page(status=200, line=None):
        return render("login.html", status, line=line)

    def read_client(token=None):
        return broker(token=token, timeout=limits.read_seconds)

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
            reply = session.login(password, channel=channel)
            number = reply.get("session") if isinstance(reply, dict) else None
            return session.token, number

        # The place in the limiter is given back on every way out (a busy bound, any answer, a request cancelled while it waits), and kept as a failure only for a wrong password.
        failed = False
        try:
            outcome = await (local_logins if local else remote_logins).run(lambda: call_broker(read_client, ask))
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
        token, number = value if isinstance(value, tuple) and len(value) == 2 else (None, None)
        if not isinstance(token, str) or not token:
            return login_page(503, words.code_sentence("bad_reply"))
        # The broker's number of the session names it to the scheduler (`web-<number>`); a reply without a usable one leaves the Queue's actions refused, nothing else.
        if not isinstance(number, int) or isinstance(number, bool) or not 0 <= number < 10 ** 12:
            number = None
        old_id = request.scope.get("sdp.session_id")
        if old_id:
            sessions.forget(old_id)
        session_id, _ = sessions.create(token, number)
        response = RedirectResponse("/", status_code=303)
        response.headers.append("set-cookie", f"{guard.COOKIE_NAME}={session_id}; {COOKIE_ATTRIBUTES}")
        return response

    @api.post("/logout")
    async def logout(request: Request):
        session_id, _ = current(request)
        session = sessions.forget(session_id) if session_id else None
        if session is not None:
            done = await logouts.run(lambda: call_broker(lambda: read_client(session.token), lambda client: client.logout()), wait_seconds=limits.logout_wait_seconds)
            if done is BUSY_GATE:
                log.warning("a logout could not be passed on to the broker: its bound stayed full")
                return to_login(request, "logged_out_broker_busy", clear_cookie=True)
        return to_login(request, "logged_out", clear_cookie=True)

    @api.api_route("/", methods=["GET", "HEAD"])
    async def overview_page(request: Request):
        session_id, session = current(request)
        if session is None:
            return to_login(request, clear_cookie=bool(session_id))

        def read_all():
            # One after the other in one worker: a page takes one place of the bound. A session that has ended stops the reads.
            answers = {}
            for verb in overview.READ_VERBS:
                answers[verb] = call_broker(lambda: read_client(session.token), lambda made, verb=verb: actions.call(made, verb, {}))
                if answers[verb][0] == "error" and session_ends(answers[verb][1]):
                    break
            return answers

        answers = await pages.run(read_all)
        if answers is BUSY_GATE:
            return frame("busy.html", "overview", session, 503, title="Overview", line=BUSY)
        for outcome, value in answers.values():
            if outcome == "error" and session_ends(value):
                sessions.forget(session_id)
                return to_login(request, why(value), clear_cookie=True)
        return frame("overview.html", "overview", session, title="Overview", blocks=overview.build(answers))

    context = Context(
        render=render, frame=frame, current=current, to_login=to_login, sessions=sessions, pages=pages, pieces=Gate(limits.piece_workers), tails=Gate(limits.tail_workers),
        action_places=threading.BoundedSemaphore(limits.action_workers), cancel_places=threading.BoundedSemaphore(limits.cancel_workers), broker=broker, limits=limits, clock=clock,
        scheduler=scheduler, door=Gate(limits.door_workers), downloads=threading.BoundedSemaphore(limits.download_workers),
    )
    # The Queue's routes first: `/queue/auto-accept` and `/queue/running-rows` before `/queue/<job id>`.
    hooks = queue.install(api, context)
    management.install(api, context, hooks)

    return guard.Guard(api, settings=settings, sessions=sessions, body_max=limits.body_max, body_seconds=limits.body_seconds, clock=clock)


def from_environment():
    """The application the container runs, with the settings of its environment and a client of the broker's socket. Exits with one line when a setting is missing or malformed."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)
    try:
        settings = config.load_settings(os.environ)
    except config.ConfigError as error:
        raise SystemExit(f"Error: {error.message}") from None

    def broker(token=None, timeout=BROKER_SECONDS):
        # A read is given `Limits.read_seconds`; a state-changing call None, no time limit: it can last many minutes and must not be cut.
        return broker_client.Client(settings.socket_path, timeout=timeout, token=token)

    return create_app(settings, broker=broker, scheduler=Scheduler(settings.queue_socket, settings.results))
