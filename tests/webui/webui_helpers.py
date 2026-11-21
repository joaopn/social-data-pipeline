"""Stand-ins and helpers of the web UI's tests: a broker that records every call, a clock, made-up answers of the three read verbs, and the login and form-token steps of a client. Imported by `conftest.py` and by the test modules; it imports nothing of FastAPI, Starlette or Jinja2."""

from __future__ import annotations

import re
import threading

from social_data_pipeline.control import client as broker_client

PASSWORD = "made-up-management-pw-4e7b"
TOKEN = "made-up-broker-token-9c1f"
# An address Docker's proxy can show for a client of any address: the gateway of the container's network.
GATEWAY = "172.30.0.1"
REMOTE = "192.0.2.7"
HOST = "localhost:8443"
ORIGIN = "https://localhost:8443"
BASE_URL = "https://localhost:8443"
# The ports the container listens on, as the ASGI scope's `server` shows them; the browser's Host still says the published port 8443.
LISTEN_PORT = 8443
LOCAL_PORT = 8444
HOSTS = ("localhost", "127.0.0.1", "[2001:db8::5]", "pipeline.example")


def db_status(**overrides):
    data = {
        "verbose": False,
        "configured": ["postgres", "mongo"],
        "auth": {"enabled": True, "rw_users": [{"db": "postgres", "user": "postgres"}, {"db": "mongo", "user": "admin"}], "ro_user": None, "ro_credentials": [], "mcp_users": []},
        "databases": [{"db": "postgres", "path": "./data/database/postgres", "name": "datasets", "port": "5432", "running": True, "ingestion": {}}, {"db": "mongo", "path": "./data/database/mongo", "port": "27017", "running": False, "ingestion": {}}],
        "export": None,
        "mcp": [],
        "jobs": None,
        "drift": {"warnings": [], "findings": []},
    }
    data.update(overrides)
    return {"data": data}


def source_list(*sources):
    return {"data": {"sources": list(sources) or [{"name": "demo", "profiles": ["parse", "lingua"]}]}}


def run_list(*runs, docker_available=True):
    return {"data": {"docker_available": docker_available, "runs": list(runs)}}


def a_run(**fields):
    run = {"id": "r20261121T101500-0a1b2c", "kind": "web", "profile": "parse", "source": "demo", "flags": {}, "started_by": "web-1", "state": "running", "started": "2026-11-21T10:15:00Z", "ended": None, "exit_code": None}
    run.update(fields)
    return run


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class StandInBroker:
    """Stands in for the broker's client. `calls` records every call as a tuple (`("login", channel)`, `("logout", token)`, `("call", token, verb, fields)`); the password a login carried is compared and dropped, never kept. `answers` maps a verb to its result or to an exception to raise; `login_error` is raised by `login` when set, and a login with another password than PASSWORD is `unauthorized`. With `gate` set, a call waits for it (at most `gate_seconds`); `entered` counts calls that are waiting or running."""

    def __init__(self):
        self.calls = []
        self.lock = threading.Lock()
        self.answers = {"db.status": db_status(), "source.list": source_list(), "run.list": run_list()}
        self.login_error = None
        self.gate = None
        # The kinds of call (`login`, `logout`, `call`) the gate holds back; None: every kind.
        self.gate_kinds = None
        self.gate_seconds = 10
        self.entered = 0

    def factory(self, token=None):
        return _StandInClient(self, token)

    def verbs_called(self):
        return [call[2] for call in self.calls if call[0] == "call"]

    def kinds(self):
        return [call[0] for call in self.calls]


class _StandInClient:
    def __init__(self, broker, token):
        self.broker = broker
        self.token = token

    def _record(self, entry):
        with self.broker.lock:
            self.broker.calls.append(entry)
            self.broker.entered += 1
        if self.broker.gate is not None and (self.broker.gate_kinds is None or entry[0] in self.broker.gate_kinds):
            self.broker.gate.wait(self.broker.gate_seconds)

    def login(self, password, channel="remote"):
        right = password == PASSWORD
        self._record(("login", channel))
        if self.broker.login_error is not None:
            raise self.broker.login_error
        if not right:
            raise broker_client.BrokerError("unauthorized", "not logged in, or the password is wrong")
        self.token = TOKEN
        return {"token": TOKEN, "session": 1, "expires_in": 28800}

    def logout(self):
        self._record(("logout", self.token))
        self.token = None
        return {}

    def call(self, verb, fields=None, step_up=None):
        self._record(("call", self.token, verb, dict(fields or {})))
        answer = self.broker.answers.get(verb)
        if isinstance(answer, BaseException):
            raise answer
        return answer


def log_in(client, password=PASSWORD):
    """Log in through the form; returns the response."""
    return client.post("/login", data={"password": password}, headers={"origin": ORIGIN})


def form_token(client):
    """The current form token, read from the Overview's logout form."""
    page = client.get("/")
    assert page.status_code == 200, page.status_code
    found = re.search(r'name="form_token" value="([^"]+)"', page.text)
    assert found, "no form token in the page"
    return found.group(1)


def session_cookie(response):
    """The value the response sets for the session cookie, or None."""
    for header in response.headers.get_list("set-cookie"):
        name, _, rest = header.partition("=")
        if name == "__Host-sdp_session":
            return rest.split(";", 1)[0]
    return None


def with_cookie(session_id):
    """The header that sends a session id, for a client whose own cookie jar is empty (a jar does not send a cookie set by hand for `localhost`)."""
    return {"cookie": f"__Host-sdp_session={session_id}"}
