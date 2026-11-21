"""The checks every request passes and the headers every response of the application gets, as one ASGI layer around the whole application: pages, static files, health check and error answers alike. Two answers come from the server itself, before this layer runs, and carry its fixed text and none of these headers: its 503 when the limit of connections is reached, and its 400 for a request it cannot parse.

In this order, each refusal a constant text answer that asks the broker nothing:

1. Only HTTP. A WebSocket is closed before it is accepted; nothing else is served.
2. `Host` must be exactly one of the configured values (`Settings.allowed_hosts`), or 400. Against DNS rebinding: a page of another origin that resolves its own name to this machine sends its own name.
3. A request whose method is not GET or HEAD: its `Origin`, when there is one, must be `https://` and that same host, or 403; without an `Origin`, `Sec-Fetch-Site` must be `same-origin`, or 403.
4. The body: a `Content-Length` over the limit (64 KiB) is refused with 413 before anything is read, and so is a body that grows past it while it is read; a body that has not arrived whole within ten seconds of the first look at it is refused with 408; it is read whole before the application sees the request. Slow request headers are not bounded here (uvicorn reads them before this layer runs): only by the server's limit on connections.
5. A request whose method is not GET or HEAD may carry only an `application/x-www-form-urlencoded` body (415 otherwise); it is parsed here, at most 64 fields and each name once (400 otherwise), into `scope["sdp.form"]`.
6. A request whose method is not GET or HEAD, other than `POST /login`, from a browser with a live session must carry that session's form token (the header `X-SDP-Form-Token` or the form field `form_token`), compared in constant time, or 403. The token is one per session and stays the same for its life; a form that changes something is answered with a redirect to a GET, so a reload never sends it again.

The session id of the cookie `__Host-sdp_session` (when exactly one is sent) goes into `scope["sdp.session_id"]`. Every response of the application gets `Content-Security-Policy`, `X-Content-Type-Options`, `Referrer-Policy` and `Cache-Control: no-store` (a static file: cacheable for an hour), `charset=utf-8` on a text type, and no `Server` and no `Strict-Transport-Security` header. An exception of the application is answered with a constant 500 when no answer has begun, and logged by the name of its kind only. One log line per request: the method, the path (control and non-ASCII characters replaced, at most 200 characters) and the status, nothing else.
"""

import logging
import time
import urllib.parse

import anyio

COOKIE_NAME = "__Host-sdp_session"
FORM_TOKEN_HEADER = b"x-sdp-form-token"
FORM_TOKEN_FIELD = "form_token"
SAFE_METHODS = frozenset({"GET", "HEAD"})
FORM_TYPE = "application/x-www-form-urlencoded"
BODY_MAX_BYTES = 64 * 1024
BODY_SECONDS = 10
FORM_FIELDS_MAX = 64
STATIC_PREFIX = "/static/"
LOG_PATH_MAX_CHARS = 200

CONTENT_SECURITY_POLICY = "default-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'; object-src 'none'"
SECURITY_HEADERS = (
    (b"content-security-policy", CONTENT_SECURITY_POLICY.encode("ascii")),
    (b"x-content-type-options", b"nosniff"),
    (b"referrer-policy", b"same-origin"),
)
NO_STORE = b"no-store"
STATIC_CACHE = b"max-age=3600"
# Headers of the application's answer that this layer sets itself or that must not leave.
_REPLACED = frozenset({b"content-security-policy", b"x-content-type-options", b"referrer-policy", b"cache-control", b"server", b"strict-transport-security"})

# The constant answers of this layer.
REFUSALS = {
    400: "Bad request.\n",
    403: "Forbidden.\n",
    408: "The request body did not arrive in time.\n",
    413: "The request is too large.\n",
    415: "Only form data is accepted.\n",
    500: "The web UI could not answer this request.\n",
}
UNKNOWN_HOST = "Bad request: this address is not one the web UI answers on.\n"

log = logging.getLogger("social_data_pipeline.webui")


def _header_values(scope, name):
    return [value for key, value in scope.get("headers", ()) if key == name]


def _one_header(scope, name):
    """The value of a header that was sent exactly once, as text; None when it was not sent; False when it was sent more than once."""
    values = _header_values(scope, name)
    if not values:
        return None
    if len(values) > 1:
        return False
    return values[0].decode("latin-1")


def _session_id(scope):
    """The value of the session cookie when exactly one was sent, else None."""
    found = []
    for header in _header_values(scope, b"cookie"):
        for part in header.decode("latin-1").split(";"):
            name, separator, value = part.strip().partition("=")
            if separator and name == COOKIE_NAME:
                found.append(value)
    return found[0] if len(found) == 1 and found[0] else None


def _log_path(path):
    return "".join(character if 0x20 < ord(character) < 0x7F else "?" for character in path[:LOG_PATH_MAX_CHARS])


def _content_type(headers, path):
    """The answer's headers with this layer's: its own security headers, the cache rule and a charset on a text type."""
    kept = []
    for key, value in headers:
        lowered = key.lower()
        if lowered in _REPLACED:
            continue
        if lowered == b"content-type":
            media = value.decode("latin-1")
            textual = media.startswith("text/") or media.split(";")[0].strip() in ("application/javascript", "application/json")
            if textual and "charset=" not in media.lower():
                value = (media + "; charset=utf-8").encode("latin-1")
        kept.append((key, value))
    kept.extend(SECURITY_HEADERS)
    kept.append((b"cache-control", STATIC_CACHE if path.startswith(STATIC_PREFIX) else NO_STORE))
    return kept


class Guard:
    """The layer around `app`. `settings` gives the allowed hosts, `sessions` the browser sessions (`form_token_matches`, `get`), `body_max` the largest body in bytes, `body_seconds` the most time the body may take to arrive, measured on `clock` (seconds, never going back)."""

    def __init__(self, app, *, settings, sessions, body_max=BODY_MAX_BYTES, body_seconds=BODY_SECONDS, clock=time.monotonic):
        self.app = app
        self.allowed_hosts = settings.allowed_hosts()
        self.sessions = sessions
        self.body_max = body_max
        self.body_seconds = body_seconds
        self.clock = clock

    async def __call__(self, scope, receive, send):
        if scope["type"] == "lifespan":
            await self.app(scope, receive, send)
            return
        if scope["type"] == "websocket":
            await send({"type": "websocket.close", "code": 1008})
            return
        if scope["type"] != "http":
            return
        answer = {"status": None}

        async def guarded_send(message):
            if message["type"] == "http.response.start":
                answer["status"] = message["status"]
                message = {**message, "headers": _content_type(message.get("headers", []), scope.get("path", ""))}
            await send(message)

        try:
            await self._checked(scope, receive, guarded_send)
        except Exception as error:
            log.error("a request failed (%s)", type(error).__name__)
            if answer["status"] is None:
                await _refuse(guarded_send, 500)
        finally:
            log.info("%s %s %s", scope.get("method", "?"), _log_path(scope.get("path", "")), answer["status"] if answer["status"] is not None else "-")

    async def _checked(self, scope, receive, send):
        host = _one_header(scope, b"host")
        if not host or host not in self.allowed_hosts:
            await _refuse(send, 400, UNKNOWN_HOST)
            return
        method = scope["method"]
        safe = method in SAFE_METHODS
        if not safe and not _same_origin(scope, host):
            await _refuse(send, 403)
            return

        length = _one_header(scope, b"content-length")
        if length is False or (length is not None and not (length.isascii() and length.isdigit())):
            await _refuse(send, 400)
            return
        if length is not None and int(length) > self.body_max:
            await _refuse(send, 413)
            return
        body = bytearray()
        deadline = self.clock() + self.body_seconds
        while True:
            left = deadline - self.clock()
            message = None
            if left > 0:
                with anyio.move_on_after(left):
                    message = await receive()
            if message is None or self.clock() > deadline:
                await _refuse(send, 408)
                return
            if message["type"] == "http.disconnect":
                return
            body += message.get("body", b"")
            if len(body) > self.body_max:
                await _refuse(send, 413)
                return
            if not message.get("more_body", False):
                break

        form = {}
        if not safe:
            content_type = _one_header(scope, b"content-type")
            if content_type is False or (content_type is None and body) or (content_type is not None and content_type.split(";")[0].strip().lower() != FORM_TYPE):
                await _refuse(send, 415)
                return
            form = _parsed_form(bytes(body))
            if form is None:
                await _refuse(send, 400)
                return

        session_id = _session_id(scope)
        if not safe and not (method == "POST" and scope["path"] == "/login") and session_id is not None and self.sessions.get(session_id) is not None:
            header_token = _one_header(scope, FORM_TOKEN_HEADER)
            presented = header_token if header_token else form.get(FORM_TOKEN_FIELD)
            if not self.sessions.form_token_matches(session_id, presented):
                await _refuse(send, 403)
                return
        form.pop(FORM_TOKEN_FIELD, None)
        scope["sdp.form"] = form
        scope["sdp.session_id"] = session_id

        replayed = False

        async def replay():
            nonlocal replayed
            if not replayed:
                replayed = True
                return {"type": "http.request", "body": bytes(body), "more_body": False}
            return await receive()

        await self.app(scope, replay, send)


def _same_origin(scope, host):
    origin = _one_header(scope, b"origin")
    if origin is False:
        return False
    if origin is not None:
        return origin == f"https://{host}"
    return _one_header(scope, b"sec-fetch-site") == "same-origin"


def _parsed_form(body):
    """The fields of a form body as a dict, or None when it is not UTF-8, has more than 64 fields or names one field twice."""
    try:
        pairs = urllib.parse.parse_qsl(body.decode("utf-8"), keep_blank_values=True, max_num_fields=FORM_FIELDS_MAX, errors="strict")
    except (UnicodeDecodeError, ValueError):
        return None
    form = {}
    for name, value in pairs:
        if name in form:
            return None
        form[name] = value
    return form


async def _refuse(send, status, text=None):
    body = (text or REFUSALS[status]).encode("utf-8")
    await send({"type": "http.response.start", "status": status, "headers": [(b"content-type", b"text/plain; charset=utf-8"), (b"content-length", str(len(body)).encode("ascii"))]})
    await send({"type": "http.response.body", "body": body})
