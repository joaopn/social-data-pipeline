"""Stand-ins and helpers of the web UI's tests: a broker that records every call, a scheduler's control socket that records every request, a scripted chat's runner on a real socket that answers anything, a clock, made-up answers of the read verbs, and the login and form-token steps of a client. Imported by `conftest.py` and by the test modules; it imports nothing of FastAPI, Starlette or Jinja2 (the registry, `control.verbs`, is imported to know which verbs need the management password, and the scheduler's `jobs.door` for its digest and short view)."""

from __future__ import annotations

import base64
import copy
import os
import re
import socket
import stat
import threading
import time
from types import SimpleNamespace

from social_data_pipeline.control import client as broker_client
from social_data_pipeline.control import verbs
from social_data_pipeline.jobs import door as jobs_door

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


def source_size(name="demo", total_bytes=1_400_000_000, complete=True, found=True, **entry):
    """A `source.size` answer for one source, shaped as `get_source_size` gives it (paths included, which the web UI never shows)."""
    stages = [{"stage": stage, "path": f"./data/{stage}/{name}", "exists": True, "not_readable": None, "files": 1, "bytes": total_bytes if stage == "dumps" else 0} for stage in ("dumps", "extracted", "parsed", "output")] if found else []
    data = {"name": name, "found": found, "warnings": [], "not_readable": None, "stages": stages, "total_bytes": total_bytes if found else 0, "complete": complete if found else False}
    data.update(entry)
    return {"data": {"sources": [data]}}


def error_logs(profile, files=(), sections=(), name="demo"):
    return {"data": {"name": name, "sources": [{"name": name, "found": True, "warnings": [], "profiles": [{"profile": profile, "files": list(files), "mongoimport_sections": list(sections)}]}]}}


def restart_needed(*services, docker_available=True):
    return {"data": {"docker_available": docker_available, "services": list(services)}}


def llm_settings(**jobs):
    """A `db.llm_settings` answer for PostgreSQL and MongoDB set up, as `get_db_llm_settings` gives it."""
    data = {
        "databases": ["postgres", "mongo"],
        "mcp": {"refusal": None, "servers": {"postgres": {"enabled": True, "port": 8000, "write_access": False}, "mongo": {"enabled": False, "port": 3000, "write_access": False}}},
        "jobs": {"configured": True, "port": 8050, "result_root": "./data/jobs/results", "max_concurrent": 20, "history_retention": 500, "auth_enabled": False,
                 "targets": {"postgres": {"add": True, "name": "pg_main", "database": "datasets", "timeout_hours": 0}, "mongo": {"add": True, "name": "mongo_main", "timeout_hours": 0}}},
    }
    data["jobs"].update(jobs)
    return {"data": data}


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
            "source.status": lambda fields: source_status(fields["name"]), "source.progress": lambda fields: source_progress(fields["name"]), "source.size": lambda fields: source_size(fields["name"]), "source.error_logs": lambda fields: error_logs(fields["profile"], name=fields["name"]),
            "db.restart_needed": restart_needed(), "db.llm_settings": llm_settings(),
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
        # Every call in order, shared with the stand-in scheduler: ("broker", kind, verb) and ("door", op).
        self.timeline = []
        # The broker's number of the session a login makes (its `session`).
        self.session_number = 1

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
            self.broker.timeline.append(("broker", entry[0], verb))
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

    def check_password(self, step_up):
        self._record(("check_password", self.token), "session.check_password")
        right = self._request("session.check_password", {}, step_up)
        answer = self.broker.answers.get("session.check_password")
        if isinstance(answer, BaseException):
            raise answer
        if not right:
            raise broker_client.BrokerError("step_up_failed", "the management password given again is wrong")
        return {}

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
        return {"token": TOKEN, "session": self.broker.session_number, "expires_in": 28800}

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


# ── the scheduler's control socket ──────────────────────────────────────────

JOB_ID = "pg_0123abcd"
GOOD_FOLDER = os.stat_result((stat.S_IFDIR | 0o700, 0, 0, 2, 1000, 1000, 0, 0, 0, 0))
GOOD_SOCKET = os.stat_result((stat.S_IFSOCK | 0o600, 0, 0, 1, 1000, 1000, 0, 0, 0, 0))


def good_lstat(path):
    """What the folder check sees in a sound deployment: the control folder 0700 and its socket 0600, both of uid 1000."""
    return GOOD_SOCKET if str(path).endswith("door.sock") else GOOD_FOLDER
CHANGING_OPS = {"approve", "reject", "kill", "explain", "auto_accept.set"}
READ_OPS = {"hello", "counts", "stats", "targets", "jobs", "job", "preview", "auto_accept"}


def a_job(job_id=JOB_ID, **fields):
    """A whole record of a job, as the scheduler's `job` sends it."""
    record = {
        "job_id": job_id, "target": "warehouse", "backend": "postgres", "sql": "SELECT 1", "output_filename": "result.csv", "overwrite": False, "submitted_at": 1795000000.0,
        "description": "made-up description", "collection": None, "database": None, "status": "pending", "approved_at": None, "started_at": None, "finished_at": None,
        "rows": None, "size_bytes": None, "result_path": None, "error": None, "reject_reason": None, "backend_pid": None, "connection_id": None, "extras": {}, "acted_by": {},
    }
    record.update(fields)
    return record


def digest_of(record):
    return jobs_door.digest(SimpleNamespace(**record))


def door_error(code, **details):
    return broker_client.BrokerError(code, jobs_door.MESSAGES.get(code, code), None, details)


class StandInDoor:
    """Stands in for the scheduler's control socket. `requests` records every request document as sent (`op` and its fields), `timeouts` the time limit of each, and `timeline` (shared with the stand-in broker) the order of all calls. `jobs` maps a job id to `[phase, record]`; the requests are answered from it as the socket would (`counts`, `stats` with `stats_disk` for the results folder's figures and its running jobs as busy, `jobs` with its short view, `job` with the digest, `approve` comparing the digest with the record as it is now, `reject`, `kill`, `explain`, `preview`, `auto_accept`, `auto_accept.set`). `answers` maps an op to a reply to give instead: a result, an exception to raise or a function of the request. With `gate` set, a request of `gate_ops` waits for it. Every reply and every error's details pass through the real socket's `jobs.door._clean` (as its replies do: a lone surrogate becomes `?`), its short view is the real `short_view` and its digest the real `digest`, so that the pages see what they see in production."""

    def __init__(self, timeline=None):
        self.requests = []
        self.timeouts = []
        self.timeline = timeline if timeline is not None else []
        self.lock = threading.Lock()
        self.jobs = {}
        self.too_large = set()
        self.answers = {}
        self.auto = {"max_limit": 4, "targets": [{"name": "warehouse", "backend": "postgres", "database": "datasets", "enabled": False, "limit": 4}]}
        self.preview = {"format": "csv", "columns": ["a", "b"], "rows": [["1", "2"]], "parts": [{"name": "result.csv", "size": 8}], "parts_total": 1, "read_bytes": 8, "truncated": False}
        self.plan = "Result  (cost=0.00..0.01 rows=1 width=4)"
        # The figures of `stats` but `busy`, which counts the running jobs of `jobs`: 38 MiB used, 4.4 TiB free, 5 slots, as the mock-up shows them.
        self.stats_disk = {"results_bytes": 38 * 1024 ** 2, "results_files": 3, "results_cut": False, "free_bytes": 4_840_000_000_000, "slots": 5}
        self.gate = None
        self.gate_ops = None

    def add(self, phase, record):
        self.jobs[record["job_id"]] = [phase, record]
        return record

    def connect(self, socket_path, timeout):
        return _DoorClient(self, timeout)

    def ops(self):
        return [request["op"] for request in self.requests]


class _DoorClient:
    def __init__(self, door, timeout):
        self.door = door
        self.timeout = timeout

    def request(self, document):
        door = self.door
        with door.lock:
            door.requests.append(copy.deepcopy(document))
            door.timeouts.append((document["op"], self.timeout))
            door.timeline.append(("door", document["op"]))
        if door.gate is not None and (door.gate_ops is None or document["op"] in door.gate_ops):
            door.gate.wait(10)
        answer = door.answers.get(document["op"])
        try:
            if answer is not None:
                if callable(answer) and not isinstance(answer, BaseException):
                    answer = answer(document)
                if isinstance(answer, BaseException):
                    raise answer
                return jobs_door._clean(copy.deepcopy(answer))
            with door.lock:
                return jobs_door._clean(copy.deepcopy(self._answer(document)))
        except broker_client.BrokerError as error:
            error.details = jobs_door._clean(error.details)
            raise

    def _find(self, job_id):
        found = self.door.jobs.get(job_id)
        if found is None:
            raise door_error("not_found")
        return found

    def _answer(self, document):
        door, op = self.door, document["op"]
        if op == "hello":
            return {"protocol": 1}
        if op == "counts":
            phases = [phase for phase, _record in door.jobs.values()]
            return {name: phases.count(name) for name in ("pending", "approved", "running")}
        if op == "stats":
            return {**door.stats_disk, "busy": [phase for phase, _record in door.jobs.values()].count("running")}
        if op == "jobs":
            listed = [record for phase, record in door.jobs.values() if phase == document["phase"]]
            end = document["offset"] + document["limit"]
            return {"phase": document["phase"], "jobs": [jobs_door.short_view(SimpleNamespace(**record)) for record in listed[document["offset"]:end]], "more": len(listed) > end}
        if op == "job":
            phase, record = self._find(document["job_id"])
            if document["job_id"] in door.too_large:
                raise door_error("too_large")
            return {"phase": phase, "job": record, "digest": digest_of(record)}
        if op == "approve":
            found = self._find(document["job_id"])
            if found[0] != "pending":
                raise door_error("not_pending", phase=found[0])
            if digest_of(found[1]) != document["digest"]:
                raise door_error("changed")
            found[0] = "approved"
            found[1] = {**found[1], "status": "approved", "acted_by": {"approved": document["by"]}}
            return {}
        if op == "reject":
            found = self._find(document["job_id"])
            if found[0] != "pending":
                raise door_error("not_pending", phase=found[0])
            found[0] = "history"
            found[1] = {**found[1], "status": "rejected", "reject_reason": document["reason"] or None, "acted_by": {"rejected": document["by"]}}
            return {}
        if op == "kill":
            found = door.jobs.get(document["job_id"])
            return {"was_running": found is not None and found[0] == "running"}
        if op == "explain":
            self._find(document["job_id"])
            return {"plan": door.plan}
        if op == "preview":
            phase, record = self._find(document["job_id"])
            if phase != "history" or record.get("status") != "done":
                raise door_error("not_done")
            return door.preview
        if op == "auto_accept":
            return door.auto
        if op == "auto_accept.set":
            row = next((row for row in door.auto["targets"] if row["name"] == document["target"]), None)
            if row is None:
                raise door_error("unknown_target")
            if document["enabled"] is not None:
                row["enabled"] = document["enabled"]
            if document["limit"] is not None:
                row["limit"] = max(0, min(door.auto["max_limit"], document["limit"]))
            return door.auto
        raise door_error("bad_request")


def log_in(client, password=PASSWORD):
    """Log in through the form; returns the response."""
    return client.post("/login", data={"password": password}, headers={"origin": ORIGIN})


def main_html(page):
    """The markup of a page's `<main>` (the whole text for a part without one): the frame's own polls (the top bar's `/status`) left out."""
    found = re.search(r"<main[^>]*>(.*)</main>", page, re.DOTALL)
    return found.group(1) if found else page


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


# ── the chat's runner ───────────────────────────────────────────────────────

CHAT_ID = "c_0123456789ab"


def refuse(code):
    """A refusal of the runner, as its reply document."""
    return {"ok": False, "error": {"code": code, "message": "made-up message of the stand-in"}}


class Raw:
    """A reply sent as these bytes of JSON in a frame, whatever they hold (NaN, a nesting too deep, any shape)."""

    def __init__(self, payload):
        self.payload = payload if isinstance(payload, bytes) else payload.encode("utf-8", "surrogatepass")


class ScriptedRunner:
    """A stand-in of the chat's runner on a real unix socket (mode 0600, in `folder`), which can answer anything. `requests` records every request document in order, `opened` counts connections and `sent` the bytes of each reply sent before the other end closed. `answers` maps an op to its result (a dict), to a reply document made by `refuse`, to a `Raw` frame, or to a function of the request returning one of those; ops without one are answered as a small runner with the one chat `CHAT_ID` would (`events` from the list `events`, `chat.file` from the bytes of `files`). With `gate` set, a request of `gate_ops` waits for it."""

    def __init__(self, folder):
        from social_data_pipeline.control import framing

        self.framing = framing
        self.path = os.path.join(folder, "runner.sock")
        self.requests = []
        self.opened = 0
        # The bytes of each reply that could be sent before the other end closed.
        self.sent = []
        # Seconds between the bytes of a reply, for a runner that dribbles; 0 sends it whole.
        self.dribble = 0
        self.answers = {}
        self.events = []
        self.running = False
        self.files = {}
        # The back end's transcript of the chat (`chat.transcript`): None for none, else its bytes, sent in pieces of at most `transcript_piece`.
        self.transcript = None
        self.transcript_piece = 1 << 30
        self.chats = [{"id": CHAT_ID, "title": "made-up question", "created": 1795000000.0, "running": False}]
        self.gate = None
        self.gate_ops = None
        self.lock = threading.Lock()
        self.listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.listener.bind(self.path)
        os.chmod(self.path, 0o600)
        self.listener.listen(16)
        self.listener.settimeout(0.2)
        self.closing = threading.Event()
        self.thread = threading.Thread(target=self._accept, daemon=True)
        self.thread.start()

    def ops(self):
        return [request.get("op") for request in self.requests]

    def close(self):
        self.closing.set()
        self.thread.join(5)
        self.listener.close()

    def _accept(self):
        while not self.closing.is_set():
            try:
                connection, _ = self.listener.accept()
            except OSError:
                continue
            threading.Thread(target=self._handle, args=(connection,), daemon=True).start()

    def _handle(self, connection):
        try:
            connection.settimeout(10)
            request = self.framing.read_frame(connection.recv, max_bytes=1 << 20, deadline=time.monotonic() + 10)
            with self.lock:
                self.opened += 1
                self.requests.append(copy.deepcopy(request))
            if self.gate is not None and (self.gate_ops is None or request.get("op") in self.gate_ops):
                self.gate.wait(10)
            reply = self._reply(request)
            frame = len(reply.payload).to_bytes(4, "big") + reply.payload if isinstance(reply, Raw) else self.framing.encode(reply, max_bytes=16 << 20)
            view, sent = memoryview(frame), 0
            try:
                while sent < len(frame):
                    sent += connection.send(view[sent:sent + (1 if self.dribble else 65536)])
                    if self.dribble:
                        time.sleep(self.dribble)
            finally:
                with self.lock:
                    self.sent.append(sent)
        except Exception:
            pass
        finally:
            connection.close()

    def _reply(self, request):
        op = request.get("op")
        answer = self.answers.get(op)
        if callable(answer):
            answer = answer(request)
        if answer is None:
            answer = self._default(op, request)
        if isinstance(answer, Raw) or (isinstance(answer, dict) and answer.get("ok") is False):
            return answer
        return {"ok": True, "result": answer}

    def _default(self, op, request):
        if op == "hello":
            return {"version": 1, "backend": "claude-code", "databases": ["mongo"]}
        if op == "chat.list":
            return {"chats": self.chats}
        if op == "chat.new":
            return {"chat": CHAT_ID}
        if op == "chat.send":
            return {"offset": len(self.events)}
        if op == "chat.events":
            first = min(request["offset"], len(self.events))
            listed = self.events[first:first + request["limit"]]
            return {"events": listed, "next": first + len(listed), "running": self.running}
        if op == "chat.stop":
            return {"stopping": True}
        if op == "chat.delete":
            return {}
        if op == "chat.rename":
            title = " ".join(request["title"].split()) if isinstance(request.get("title"), str) else ""
            if not 1 <= len(title) <= 80:
                return refuse("bad_title")
            for chat in self.chats:
                if chat["id"] == request.get("chat"):
                    chat["title"] = title
                    return {"title": title}
            return refuse("not_found")
        if op == "chat.files":
            return {"files": [{"name": name, "size": len(data), "mtime": 1795000000.0} for name, data in sorted(self.files.items())], "left_out": 0, "unlisted": 0, "more": False}
        if op == "chat.file":
            data = self.files.get(request["name"])
            if data is None:
                return refuse("no_such_file")
            piece = data[request["offset"]:request["offset"] + request["limit"]]
            return {"data": base64.b64encode(piece).decode("ascii"), "size": len(data), "next": request["offset"] + len(piece), "end": request["offset"] + len(piece) >= len(data)}
        if op == "chat.transcript":
            if self.transcript is None:
                return {"found": False}
            data = self.transcript
            piece = data[request["offset"]:request["offset"] + min(request["limit"], self.transcript_piece)]
            return {"found": True, "data": base64.b64encode(piece).decode("ascii"), "size": len(data), "next": request["offset"] + len(piece), "end": request["offset"] + len(piece) >= len(data)}
        return refuse("bad_request")


def an_event(kind, n=0, **fields):
    """An event as the runner writes it."""
    return {"n": n, "t": 1795000000.5, "type": kind, **fields}
