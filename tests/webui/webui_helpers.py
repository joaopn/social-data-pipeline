"""Stand-ins and helpers of the web UI's tests: a broker that records every call, a clock, made-up answers of the read verbs, and the login and form-token steps of a client. Imported by `conftest.py` and by the test modules; it imports nothing of FastAPI, Starlette or Jinja2 (the registry, `control.verbs`, is imported to know which verbs need the management password)."""

from __future__ import annotations

import re
import threading
import time

from social_data_pipeline.control import client as broker_client
from social_data_pipeline.control import verbs

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


def source_status(name="demo", profiles=("parse", "lingua"), **entry):
    data = {"name": name, "found": True, "warnings": [], "platform": "reddit", "data_types": ["submissions"], "profiles": list(profiles), "paths": {"dumps": f"./data/dumps/{name}", "parsed": f"./data/parsed/{name}"}, "ingestion": []}
    data.update(entry)
    return {"data": {"sources": [data]}}


def source_progress(name="demo", **entry):
    item = {"data_type": "submissions", "exists": True, "not_readable": None, "finished": 3, "being_written": 1, "newest": {"name": "RS_2024-01.parquet", "modified": "2026-11-20T10:00:00Z"}}
    data = {"name": name, "found": True, "warnings": [], "not_readable": None, "stages": [{"stage": "parse", "kind": "parse", "path": f"./data/parsed/{name}", "not_readable": None, "data_types": [item]}], "output": {"path": f"./data/output/{name}", "exists": True, "not_readable": None}}
    data.update(entry)
    return {"data": {"sources": [data]}}


def error_logs(profile, files=(), sections=(), name="demo"):
    return {"data": {"name": name, "sources": [{"name": name, "found": True, "warnings": [], "profiles": [{"profile": profile, "files": list(files), "mongoimport_sections": list(sections)}]}]}}


def db_verify(results=None, ok=True, warnings=()):
    results = {"postgres": {"ok": True, "findings": []}, "mongo": {"ok": True, "findings": []}} if results is None else results
    return {"data": {"report": {"ok": ok, "exit_code": 0 if ok else 1, "results": results}, "warnings": list(warnings)}}


def a_run(**fields):
    run = {"id": "r20261121T101500-0a1b2c", "kind": "web", "profile": "parse", "source": "demo", "flags": {}, "started_by": "web-1", "state": "running", "started": "2026-11-21T10:15:00Z", "ended": None, "exit_code": None}
    run.update(fields)
    return run


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


# The answer of a state-changing verb that ran, and of `run.start`.
DONE = {"exit_code": 0, "output": "", "errors": ""}
RUN_ID = "r20261121T101500-0a1b2c"


class StandInBroker:
    """Stands in for the broker's client. `calls` records every call as a tuple (`("hello",)`, `("login", channel)`, `("logout", token)`, `("call", token, verb, fields)`, `("set_db_password", token)`, `("clear_db_password", token)`); the password a login carried is compared and dropped, never kept. `requests` records each call of a verb or a session operation as a dict (`verb`, `fields`, `step_up`: whether the management password came, and whether it was the right one: None, True or False), and `timeouts` the time limit of the client of each call. `answers` maps a verb to its result, to an exception to raise or to a function of the fields that returns or raises one; a step-up verb (by the registry's class) without the management password is `step_up_required`, with a wrong one `step_up_failed`. `login_error` is raised by `login` when set, and a login with another password than PASSWORD is `unauthorized`. With `gate` set, a call waits for it (at most `gate_seconds`); `gate_verbs` limits that to those verbs. A client with a time limit that waits longer than its limit raises `BrokerUnreachable` as the real client would; `entered` counts calls that are waiting or running."""

    def __init__(self):
        self.calls = []
        self.requests = []
        self.timeouts = []
        self.lock = threading.Lock()
        self.answers = {
            "db.status": db_status(), "db.verify": db_verify(), "source.list": source_list(), "run.list": run_list(), "hello": {"protocol": 1, "password_set": True}, "audit.list": {"data": []},
            "source.status": lambda fields: source_status(fields["name"]), "source.progress": lambda fields: source_progress(fields["name"]), "source.error_logs": lambda fields: error_logs(fields["profile"], name=fields["name"]),
            "run.logs": lambda fields: {"data": {"id": fields["run_id"], "kind": "web", "state": "finished", "text": "", "next_offset": fields.get("offset", 0), "complete": True}},
        }
        for verb in ("db.stop", "run.cancel", "db.start", "db.reset", "db.unsetup", "db.unsetup_mcp", "db.unsetup_jobs", "source.remove"):
            self.answers[verb] = dict(DONE)
        self.answers["run.start"] = {"run_id": RUN_ID}
        self.login_error = None
        self.gate = None
        # The kinds of call (`login`, `logout`, `call`, ...) the gate holds back; None: every kind.
        self.gate_kinds = None
        self.gate_verbs = None
        self.gate_seconds = 10
        self.entered = 0

    def factory(self, token=None, timeout=None):
        return _StandInClient(self, token, timeout)

    def verbs_called(self):
        return [call[2] for call in self.calls if call[0] == "call"]

    def kinds(self):
        return [call[0] for call in self.calls]


class _StandInClient:
    def __init__(self, broker, token, timeout):
        self.broker = broker
        self.token = token
        self.timeout = timeout

    def _record(self, entry, verb=None):
        with self.broker.lock:
            self.broker.calls.append(entry)
            self.broker.timeouts.append((entry[0], verb, self.timeout))
            self.broker.entered += 1
        broker = self.broker
        if broker.gate is not None and (broker.gate_kinds is None or entry[0] in broker.gate_kinds) and (broker.gate_verbs is None or verb in broker.gate_verbs):
            started = time.monotonic()
            broker.gate.wait(broker.gate_seconds)
            if self.timeout is not None and time.monotonic() - started > self.timeout:
                raise broker_client.BrokerUnreachable(broker_client.NO_REPLY)

    def _answer(self, verb, fields):
        answer = self.broker.answers.get(verb)
        if callable(answer) and not isinstance(answer, BaseException):
            answer = answer(fields)
        if isinstance(answer, BaseException):
            raise answer
        return answer

    def _request(self, verb, fields, step_up):
        """Record the request; returns whether the management password came and was right (None, True, False)."""
        right = None if step_up is None else step_up == PASSWORD
        with self.broker.lock:
            self.broker.requests.append({"verb": verb, "fields": dict(fields), "step_up": right})
        return right

    def hello(self):
        self._record(("hello",), "hello")
        return self._answer("hello", {})

    def set_db_password(self, password, step_up):
        self._record(("set_db_password", self.token), "session.set_db_password")
        right = self._request("session.set_db_password", {"password": password}, step_up)
        if not right:
            raise broker_client.BrokerError("step_up_failed", "the management password given again is wrong")
        return self._answer("session.set_db_password", {}) or {}

    def clear_db_password(self):
        self._record(("clear_db_password", self.token), "session.clear_db_password")
        self._request("session.clear_db_password", {}, None)
        return self._answer("session.clear_db_password", {}) or {}

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
        self._record(("call", self.token, verb, dict(fields or {})), verb)
        right = self._request(verb, fields or {}, step_up)
        registered = verbs.VERBS.get(verb)
        if registered is not None and registered.cls == "step_up":
            if right is None:
                raise broker_client.BrokerError("step_up_required", "this verb needs the management password again")
            if right is False:
                raise broker_client.BrokerError("step_up_failed", "the management password given again is wrong")
        return self._answer(verb, dict(fields or {}))


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


def choose(client, action, token, **fields):
    """Post the form of an action to `/pending` (what a page's form sends); returns the response."""
    return client.post("/pending", data={"form_token": token, "action": action, **fields}, headers={"origin": ORIGIN})


def pending_path(response):
    """The address of the pending page a choice was answered with."""
    assert response.status_code == 303, response.status_code
    location = response.headers["location"]
    assert re.fullmatch(r"/pending/[A-Za-z0-9_-]{22}", location), location
    return location


def confirm(client, path, token, password=None, **extra):
    """Post the confirmation of a pending action (`path` is its page), with the management password when given."""
    data = {"form_token": token, **extra}
    if password is not None:
        data["management_password"] = password
    return client.post(path + "/confirm", data=data, headers={"origin": ORIGIN})


def wait_until(condition, seconds=5):
    """Wait until `condition()` is true, looking every 10 ms; fails after `seconds` (a test that gets there is wrong in itself)."""
    deadline = time.monotonic() + seconds
    while not condition():
        assert time.monotonic() < deadline, "the condition did not come true in time"
        time.sleep(0.01)


def state_changing_requests(broker):
    """The requests the stand-in received for anything but a read."""
    return [request for request in broker.requests if verbs.VERBS.get(request["verb"]) is None or verbs.VERBS[request["verb"]].cls != "read"]
