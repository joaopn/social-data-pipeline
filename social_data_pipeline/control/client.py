"""A small client of the broker's socket: one connection per request, one request and one reply (`framing`).

`Client(socket_path)` keeps the token of the session it logged in with. Every method returns the reply's `result`, or raises `BrokerError` with the reply's `code`, `message` and `field` (and its other details, such as the exit code and the end of standard error of a command that failed, in `details`); `BrokerUnreachable`, a `BrokerError` with the code `unreachable`, when there is no broker to ask or the connection ended without a reply; its `reason` says which (`REASONS`). The client sends nothing but what it is given and keeps nothing but the token. Standard library and this package only.
"""

import math
import socket
import time

from social_data_pipeline.control import framing


class BrokerError(Exception):
    """The broker refused a request, or could not be asked. `code` is the reply's code, `message` its constant text, `field` the field at fault or None, `details` every other key of the error."""

    def __init__(self, code, message, field=None, details=None):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.field = field
        self.details = dict(details or {})


# Why no reply came, each with its constant text.
NO_SOCKET = "no_socket"
REFUSED = "refused"
PERMISSION_DENIED = "permission_denied"
NO_REPLY = "no_reply"
NOT_REACHED = "not_reached"
REASONS = {
    NO_SOCKET: "there is no broker socket",
    REFUSED: "nobody listens on the broker socket",
    PERMISSION_DENIED: "the broker socket cannot be opened: permission denied",
    NO_REPLY: "the broker closed the connection without a reply",
    NOT_REACHED: "the broker cannot be reached",
}


class BrokerUnreachable(BrokerError):
    """No reply. `reason` is one of `REASONS`: `no_socket` (no socket file, or no folder for it), `refused` (a socket file that nobody listens on), `permission_denied` (the socket or its folder cannot be opened by this user), `no_reply` (connected, and the connection ended or ran out of time before a whole reply came, which is what the broker does to a connection from another user), `not_reached` (connecting or sending failed in another way, a time-out included). `message` is the reason's constant text."""

    def __init__(self, reason):
        super().__init__("unreachable", REASONS[reason])
        self.reason = reason


def _reason_of(error):
    """The reason for an OSError raised while connecting."""
    if isinstance(error, (FileNotFoundError, NotADirectoryError)):
        return NO_SOCKET
    if isinstance(error, ConnectionRefusedError):
        return REFUSED
    if isinstance(error, PermissionError):
        return PERMISSION_DENIED
    return NOT_REACHED


class Client:
    """A client of the broker at `socket_path`. `timeout` is the most seconds one request may take from connecting to the end of the reply (None: no limit, which a state-changing call needs, since the broker gives it none)."""

    def __init__(self, socket_path, *, timeout=None, token=None):
        self.socket_path = socket_path
        self.timeout = timeout
        self.token = token

    def request(self, document):
        """Send one request (a dict) and return the reply's `result`. Raises BrokerError for a refusal and BrokerUnreachable when there is no reply."""
        deadline = math.inf if self.timeout is None else time.monotonic() + self.timeout
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            try:
                connection.settimeout(self.timeout)
                connection.connect(self.socket_path)
            except OSError as error:
                raise BrokerUnreachable(_reason_of(error)) from None
            try:
                connection.sendall(framing.encode(document, max_bytes=framing.REQUEST_MAX_BYTES))
            except (BrokenPipeError, ConnectionResetError):
                # Connected, and closed by the other end before the request was in: no reply will come.
                raise BrokerUnreachable(NO_REPLY) from None
            except OSError:
                raise BrokerUnreachable(NOT_REACHED) from None

            def read(count):
                if self.timeout is not None:
                    connection.settimeout(max(0.001, deadline - time.monotonic()))
                return connection.recv(count)

            try:
                reply = framing.read_frame(read, max_bytes=framing.REPLY_MAX_BYTES, deadline=deadline)
            except framing.FrameError:
                raise BrokerUnreachable(NO_REPLY) from None
        finally:
            connection.close()
        if reply.get("ok") is True and "result" in reply:
            return reply["result"]
        error = reply.get("error")
        if not isinstance(error, dict):
            raise BrokerError("bad_reply", "the reply is not one the broker sends")
        details = {key: value for key, value in error.items() if key not in ("code", "message", "field")}
        raise BrokerError(error.get("code"), error.get("message"), error.get("field"), details)

    def hello(self):
        """`{"protocol", "password_set"}`: the one request answered without a session."""
        return self.request({"op": "hello"})

    def login(self, password, channel="remote"):
        """Log in with the management password and keep the session's token. `channel` is `remote` or `local`: the broker keeps a login budget for each (see `sessions`). Returns `{"token", "session", "expires_in"}`."""
        result = self.request({"op": "login", "password": password, "channel": channel})
        self.token = result["token"]
        return result

    def logout(self):
        """End the session and forget its token."""
        try:
            return self.request({"op": "logout", "token": self.token})
        finally:
            self.token = None

    def set_db_password(self, password, step_up):
        """Keep a database password in the session; `step_up` is the management password again."""
        return self.request({"op": "session.set_db_password", "token": self.token, "step_up": step_up, "password": password})

    def check_password(self, step_up):
        """Check the management password again (`step_up`) and do nothing else; `{}` when it is right. A wrong one is refused `step_up_failed`, and the fifth wrong one in the session's life ends the session (`session_ended`), as with any second check."""
        return self.request({"op": "session.check_password", "token": self.token, "step_up": step_up})

    def clear_db_password(self):
        """Forget the session's database password."""
        return self.request({"op": "session.clear_db_password", "token": self.token})

    def call(self, verb, fields=None, step_up=None):
        """Call a verb with its fields; `step_up` is the management password again, which a verb of the class `step_up` needs."""
        document = {"op": "call", "token": self.token, "verb": verb, "fields": {} if fields is None else fields}
        if step_up is not None:
            document["step_up"] = step_up
        return self.request(document)
