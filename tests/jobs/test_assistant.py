"""Tests of the scheduler's assistant endpoint (`social_data_pipeline.jobs.assistant`), the `submitted_by` mark of `Job`, auto-accept of a marked job and the door's short view of a marked job.

The gate is a plain ASGI callable, driven here by hand with a real `Store` in the temp folder and stand-ins for the page and the MCP app; nothing needs FastAPI. What needs the real `mcp` package (the assistant's MCP server and its tools as the SDK runs them, two session managers in one process) is covered by `tests/e2e/manual/check_chat_queue.py`.
"""

from __future__ import annotations

import asyncio
import json
import os
import time

import pytest

from social_data_pipeline.jobs import assistant
from social_data_pipeline.jobs.auto_accept import AutoAcceptStore
from social_data_pipeline.jobs.config import JobsConfig, Target
from social_data_pipeline.jobs.door import short_view
from social_data_pipeline.jobs.mcp_tools import _job_to_status, _submit, _submit_mongo
from social_data_pipeline.jobs.runner import Runner
from social_data_pipeline.jobs.store import Job, Store

SECRET = "0123456789abcdef" * 4
WRONG = "fedcba9876543210" * 4
MARKER = "planted-assistant-marker-7c2e"
OWN = "pg_0a0a0a0a"
FOREIGN = "pg_0b0b0b0b"
UNKNOWN = "pg_0c0c0c0c"
BAD_IDS = ["../pending/x", "pg_0a0a0a0", "pg_0a0a0a0a\n", "PG_0A0A0A0A", "", 5, None]


def make_job(job_id=OWN, mark="assistant", **fields):
    values = dict(job_id=job_id, target="warehouse", backend="postgres", sql="SELECT 1", output_filename="out.csv", overwrite=False, submitted_at=time.time(), submitted_by=mark)
    values.update(fields)
    return Job(**values)


def finished(store, job_id, mark="assistant", status="done", **fields):
    """A job retired into the history with the store's own code."""
    job = make_job(job_id, mark, status="running", started_at=time.time(), **fields)
    if status == "done":
        store.complete(job)
    else:
        store.fail(job, error="made-up failure")
    return job


class RecordingStore(Store):
    def __init__(self, jobs_dir):
        super().__init__(jobs_dir)
        self.found = []

    def find(self, job_id):
        self.found.append(job_id)
        return super().find(job_id)


@pytest.fixture
def store(tmp_path):
    return RecordingStore(tmp_path)


def write_secret(tmp_path, value=SECRET):
    control = tmp_path / "control"
    control.mkdir(mode=0o700, exist_ok=True)
    (control / "assistant.secret").write_text(value + "\n")
    return control / "assistant.secret"


class Inner:
    """Stands for the page (or the MCP app): records the scopes it gets and answers 200."""

    def __init__(self, name):
        self.name = name
        self.scopes = []

    async def __call__(self, scope, receive, send):
        self.scopes.append(scope)
        if scope["type"] == "http":
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": self.name.encode(), "more_body": False})


@pytest.fixture
def gate(tmp_path, store):
    page, mcp = Inner("page"), Inner("mcp")
    made = assistant.AssistantGate(page, store=store, mcp_app=mcp, jobs_dir=tmp_path)
    made.page, made.mcp = page, mcp
    return made


def scope_of(path, headers=(), method="GET"):
    return {"type": "http", "method": method, "path": path, "raw_path": path.encode(), "root_path": "", "query_string": b"", "headers": [(name.encode(), value.encode()) for name, value in headers]}


async def _exchange(app, scope, send=None):
    messages = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def record(message):
        messages.append(message)
        if send is not None:
            await send(message)

    await app(scope, receive, record)
    return messages


def exchange(app, path, headers=(), method="GET"):
    return answer(asyncio.run(_exchange(app, scope_of(path, headers, method))))


def answer(messages):
    """(status, headers, body, complete) of the ASGI messages of one answer."""
    start = messages[0]
    body = b"".join(m.get("body", b"") for m in messages[1:])
    complete = bool(messages[1:]) and messages[-1].get("more_body") is False
    return start["status"], dict(start["headers"]), body, complete


def secret_header(value=SECRET):
    return [("x-sdp-assistant", value)]


NOT_FOUND = (404, {b"content-type": b"application/json", b"content-length": b"22"}, b'{"detail":"Not Found"}', True)


# ── the gate ────────────────────────────────────────────────────────────────


class TestGate:
    def test_paths_outside_the_prefix_and_other_scopes_go_to_the_page_unchanged(self, gate, tmp_path):
        """Bug class: the gate changing a route of the page (the `/mcp` endpoint, a tab) or the lifespan."""
        write_secret(tmp_path)
        for path in ("/mcp", "/mcp/", "/pending", "/assistantx", "/static/assistant/mcp"):
            assert exchange(gate, path)[0] == 200
        assert [s["path"] for s in gate.page.scopes] == ["/mcp", "/mcp/", "/pending", "/assistantx", "/static/assistant/mcp"]
        assert gate.page.scopes[0] == scope_of("/mcp")
        lifespan = {"type": "lifespan"}
        asyncio.run(_exchange(gate, lifespan))
        assert gate.page.scopes[-1] is lifespan and gate.mcp.scopes == []

    @pytest.mark.parametrize("case", ["no file", "no control folder", "wrong", "missing header", "repeated header", "short secret", "uppercase secret", "too large", "link", "fifo", "folder link"])
    def test_every_refusal_is_the_same_404_and_reaches_nothing(self, gate, tmp_path, case):
        """Bug class (security): a request without the right secret reaching the assistant's MCP server or a result, or a refusal that differs by why (telling the endpoint exists, or that a file is there)."""
        headers = secret_header()
        if case == "no control folder":
            pass
        elif case == "no file":
            (tmp_path / "control").mkdir(mode=0o700)
        elif case == "wrong":
            write_secret(tmp_path)
            headers = secret_header(WRONG)
        elif case == "missing header":
            write_secret(tmp_path)
            headers = []
        elif case == "repeated header":
            write_secret(tmp_path)
            headers = secret_header() + secret_header()
        elif case in ("short secret", "uppercase secret", "too large"):
            value = {"short secret": SECRET[:62], "uppercase secret": SECRET.upper(), "too large": SECRET + " " * 300}[case]
            write_secret(tmp_path, value)
            headers = secret_header(value.strip())
        elif case in ("link", "fifo"):
            real = tmp_path / "real.secret"
            real.write_text(SECRET)
            (tmp_path / "control").mkdir(mode=0o700)
            if case == "link":
                (tmp_path / "control" / "assistant.secret").symlink_to(real)
            else:
                os.mkfifo(tmp_path / "control" / "assistant.secret")
        elif case == "folder link":
            elsewhere = tmp_path / "elsewhere"
            elsewhere.mkdir()
            (elsewhere / "assistant.secret").write_text(SECRET)
            (tmp_path / "control").symlink_to(elsewhere)
        finished(gate.store, OWN)
        for path in ("/assistant/mcp", "/assistant/mcp/", "/assistant", "/assistant/", f"/assistant/results/{OWN}", f"/assistant/results/{OWN}/0", "/assistant/other"):
            for method in ("GET", "POST"):
                assert exchange(gate, path, headers, method) == NOT_FOUND
        assert gate.mcp.scopes == [] and gate.page.scopes == [] and gate.store.found == []
        assert gate.refused == 14

    def test_the_right_secret_reaches_the_mcp_server_at_its_root(self, gate, tmp_path):
        """Bug class: the assistant's MCP server reached under another path than its own root (the SDK then answers 404), or the right secret refused."""
        write_secret(tmp_path)
        for path in ("/assistant/mcp", "/assistant/mcp/"):
            assert exchange(gate, path, secret_header(), "POST")[2] == b"mcp"
        assert [(s["path"], s["root_path"]) for s in gate.mcp.scopes] == [("/", "/assistant/mcp")] * 2
        assert exchange(gate, "/assistant/mcp/x", secret_header(), "POST") == NOT_FOUND
        assert exchange(gate, "/assistant/elsewhere", secret_header()) == NOT_FOUND
        assert len(gate.mcp.scopes) == 2 and gate.refused == 0

    def test_the_secret_is_compared_in_constant_time(self, gate, tmp_path, monkeypatch):
        """Bug class: a comparison whose time tells how many leading characters of a guess are right."""
        write_secret(tmp_path)
        seen = []
        real = assistant.hmac.compare_digest
        monkeypatch.setattr(assistant.hmac, "compare_digest", lambda a, b: seen.append((a, b)) or real(a, b))
        exchange(gate, "/assistant/mcp", secret_header(WRONG), "POST")
        exchange(gate, "/assistant/mcp", secret_header(), "POST")
        assert seen == [(WRONG.encode(), SECRET.encode()), (SECRET.encode(), SECRET.encode())]

    def test_a_changed_secret_file_is_read_again(self, gate, tmp_path):
        """Bug class: a cache that keeps accepting the old secret after `sdp chat start` replaced the file."""
        path = write_secret(tmp_path)
        assert exchange(gate, "/assistant/mcp", secret_header(), "POST")[0] == 200
        path.unlink()
        write_secret(tmp_path, WRONG)
        assert exchange(gate, "/assistant/mcp", secret_header(), "POST") == NOT_FOUND
        assert exchange(gate, "/assistant/mcp", secret_header(WRONG), "POST")[0] == 200
        path.unlink()
        assert exchange(gate, "/assistant/mcp", secret_header(WRONG), "POST") == NOT_FOUND


# ── the mark ────────────────────────────────────────────────────────────────


class TestMark:
    def test_a_record_without_the_mark_is_byte_identical_to_before(self, store):
        """Bug class: every record written by the scheduler changing (an empty `submitted_by` written out) for jobs that are not the assistant's."""
        job = make_job(FOREIGN, "", description="d", acted_by={})
        store.submit(job)
        before = [name for name in Job.__dataclass_fields__ if name not in ("acted_by", "submitted_by")]
        expected = json.dumps({name: getattr(job, name) for name in before}, separators=(",", ":"))
        assert (store.pending / f"{FOREIGN}.json").read_text() == expected
        assert "submitted_by" not in job.to_dict()

    def test_a_marked_record_round_trips(self, store):
        """Bug class: the mark lost on a transition, so a marked job becomes an ordinary one (auto-accepted, not the assistant's)."""
        store.submit(make_job(OWN))
        store.approve(OWN)
        assert store.find(OWN)[1].submitted_by == "assistant"
        assert json.loads((store.approved / f"{OWN}.json").read_text())["submitted_by"] == "assistant"

    @pytest.mark.parametrize("value", ["Assistant", "web", 1, None, ["assistant"], {"by": "assistant"}, True])
    def test_a_mark_of_another_shape_goes_to_extras(self, value):
        """Bug class: a hand-edited record (the job folders are world-writable) whose mark has another shape taken as a mark, or making `from_dict` raise."""
        record = make_job(FOREIGN, "").to_dict()
        record["submitted_by"] = value
        job = Job.from_dict(record)
        assert job.submitted_by == "" and job.extras == {"submitted_by": value}


# ── auto-accept ─────────────────────────────────────────────────────────────


def runner_for(tmp_path, store):
    cfg = JobsConfig(port=8050, jobs_dir=tmp_path, result_root=tmp_path / "results", host_result_root=str(tmp_path / "results"), max_concurrent=4, default_timeouts={"postgres": 0, "starrocks": 259200, "mongodb": 0}, history_retention=500, auth_enabled=False, targets={"warehouse": Target(name="warehouse", backend="postgres", database="datasets")})
    return Runner(cfg, store, AutoAcceptStore(state_path=tmp_path / "auto_accept.json", max_limit=4))


def test_auto_accept_approves_a_marked_job_like_any_other(tmp_path, store):
    """Bug class: a query of the chat's assistant left waiting for a person although its target auto-accepts (skipped by the scheduler), approved out of its turn among unmarked jobs, beyond the target's limit, or approved while the target's auto-accept is off."""
    runner = runner_for(tmp_path, store)
    runner.auto_accept.set_target("warehouse", enabled=True, limit=2)
    store.submit(make_job(OWN))
    time.sleep(0.01)
    store.submit(make_job(FOREIGN, ""))
    time.sleep(0.01)
    store.submit(make_job(UNKNOWN))
    assert runner._auto_approve_eligible() == 2
    assert {p.stem for p in store.approved.glob("*.json")} == {OWN, FOREIGN}
    assert {p.stem for p in store.pending.glob("*.json")} == {UNKNOWN}
    assert runner._auto_approve_eligible() == 0 and store.find(UNKNOWN)[0] == "pending"
    for job_id in (OWN, FOREIGN):
        (store.approved / f"{job_id}.json").unlink()
    runner.auto_accept.set_target("warehouse", enabled=False)
    assert runner._auto_approve_eligible() == 0 and store.find(UNKNOWN)[0] == "pending"


# ── query_status and query_cancel ───────────────────────────────────────────


class TestOwnJobs:
    @pytest.mark.parametrize("call", [assistant.status, assistant.cancel])
    def test_another_s_an_unknown_and_a_malformed_id_get_one_answer(self, store, call):
        """Bug class (security): the assistant reading or cancelling a query of someone else, telling a foreign id from an unknown one, the id echoed, or a malformed id joined into a path."""
        store.submit(make_job(FOREIGN, ""))
        store.submit(make_job(OWN))
        (store.pending / "pg_0d0d0d0d.json").write_text((store.pending / f"{OWN}.json").read_text())
        answers = [call(store, job_id, _job_to_status) for job_id in [FOREIGN, UNKNOWN, "pg_0d0d0d0d", *BAD_IDS]]
        assert answers == [{"status": "unknown", "error": "no such job"}] * len(answers)
        assert store.found == [FOREIGN, UNKNOWN, "pg_0d0d0d0d"]
        assert store.find(FOREIGN)[0] == "pending"

    def test_its_own_job_is_answered_and_cancelled(self, store):
        """Bug class: the assistant unable to follow or withdraw its own query."""
        store.submit(make_job(OWN))
        assert assistant.status(store, OWN, _job_to_status)["status"] == "pending"
        cancelled = assistant.cancel(store, OWN, _job_to_status)
        assert (cancelled["status"], cancelled["phase"]) == ("cancelled", "history")
        again = assistant.cancel(store, OWN, _job_to_status)
        assert again["status"] == "history" and "only a pending job" in again["error"]
        assert assistant.status(store, OWN, _job_to_status)["status"] == "cancelled"

    def test_the_submit_helpers_mark_only_the_assistant_s_jobs(self, tmp_path, store):
        """Bug class: a job of `/mcp` carrying the mark (it would be hidden from auto-accept and shown to the assistant), or one of the assistant's flavour without it."""
        cfg = runner_for(tmp_path, store).cfg
        cfg.targets["docstore"] = Target(name="docstore", backend="mongodb", database="")
        plain = _submit(store=store, cfg=cfg, backend="postgres", target="warehouse", sql="SELECT 1", output_filename="a.csv", overwrite=False)
        marked = _submit(store=store, cfg=cfg, backend="postgres", target="warehouse", sql="SELECT 1", output_filename="a.csv", overwrite=False, submitted_by="assistant")
        mongo = _submit_mongo(store=store, cfg=cfg, target="docstore", collection="c", pipeline=[], output_filename="a.ndjson", overwrite=False, database="d", submitted_by="assistant")
        assert "submitted_by" not in (store.pending / f"{plain['job_id']}.json").read_text()
        assert store.find(marked["job_id"])[1].submitted_by == "assistant" and store.find(mongo["job_id"])[1].submitted_by == "assistant"


# ── the results ─────────────────────────────────────────────────────────────


class TestResults:
    def result(self, gate, tmp_path, job_id=OWN, files=None):
        write_secret(tmp_path)
        folder = tmp_path / "results" / job_id
        folder.mkdir(parents=True, exist_ok=True)
        for name, data in ({"out.csv": b"a,b\n1,2\n"} if files is None else files).items():
            (folder / name).write_bytes(data)
        return folder

    def test_a_finished_marked_job_s_parts_are_listed_and_sent_byte_for_byte(self, gate, tmp_path):
        """Bug class: a part sent changed, cut, with another type or without its length, or the parts out of order."""
        finished(gate.store, OWN, output_filename="out.csv")
        data = bytes(range(256)) * 700
        self.result(gate, tmp_path, files={"out_1.csv": b"second\n", "out_0.csv": data, "out_x.csv": b"not a part"})
        status, headers, body, _ = exchange(gate, f"/assistant/results/{OWN}", secret_header())
        assert status == 200 and json.loads(body) == {"parts": [{"index": 0, "name": "out_0.csv", "size": len(data)}, {"index": 1, "name": "out_1.csv", "size": 7}]}
        status, headers, body, complete = exchange(gate, f"/assistant/results/{OWN}/0", secret_header())
        assert (status, body, complete) == (200, data, True)
        assert headers == {b"content-type": b"application/octet-stream", b"content-length": str(len(data)).encode()}
        assert exchange(gate, f"/assistant/results/{OWN}/1", secret_header())[2] == b"second\n"
        assert exchange(gate, f"/assistant/results/{OWN}/0", secret_header(), "HEAD") == NOT_FOUND

    @pytest.mark.parametrize("case", ["pending", "failed", "unmarked", "unknown", "malformed", "out of range", "link", "fifo", "folder", "slash in the name", "bad index"])
    def test_every_other_request_is_the_same_404_without_the_marker(self, gate, tmp_path, case):
        """Bug class (security): the assistant fetching a result of someone else's query or of an unfinished one, a link planted in a job folder (the folders are world-writable) read through, a FIFO blocking the scheduler, or the answer telling the cases apart."""
        (tmp_path / "marker.csv").write_text(MARKER)
        job_id, path = OWN, None
        if case == "pending":
            gate.store.submit(make_job(OWN))
        elif case == "failed":
            finished(gate.store, OWN, status="failed")
        elif case == "unmarked":
            finished(gate.store, OWN, mark="")
        elif case == "slash in the name":
            finished(gate.store, OWN, output_filename="../marker.csv")
        else:
            finished(gate.store, OWN)
        folder = self.result(gate, tmp_path, files={"out.csv": b"fine\n"} if case in ("pending", "failed", "unmarked", "out of range", "bad index") else {})
        if case == "link":
            (folder / "out.csv").symlink_to(tmp_path / "marker.csv")
        elif case == "fifo":
            os.mkfifo(folder / "out.csv")
        elif case == "folder":
            (folder / "out.csv").mkdir()
        elif case == "unknown":
            job_id = UNKNOWN
        elif case == "malformed":
            path = "/assistant/results/../marker.csv"
        index = {"out of range": "1", "bad index": "01"}.get(case, "0")
        answers = [exchange(gate, path or f"/assistant/results/{job_id}/{index}", secret_header())]
        if case not in ("out of range", "bad index", "malformed"):
            answers.append(exchange(gate, path or f"/assistant/results/{job_id}", secret_header()))
        assert answers == [NOT_FOUND] * len(answers)
        assert all(MARKER.encode() not in a[2] for a in answers)

    def test_at_most_two_parts_are_sent_at_a_time(self, gate, tmp_path):
        """Bug class: the assistant tying up the scheduler's threads and disk with many downloads at once."""
        finished(gate.store, OWN)
        self.result(gate, tmp_path)

        async def run():
            release = asyncio.Event()
            started = asyncio.Event()
            begun = []

            async def hold(message):
                if message["type"] == "http.response.start":
                    begun.append(1)
                    if len(begun) == 2:
                        started.set()
                    await release.wait()

            slow = [asyncio.create_task(_exchange(gate, scope_of(f"/assistant/results/{OWN}/0", secret_header()), hold)) for _ in range(2)]
            await asyncio.wait_for(started.wait(), 10)
            third = await _exchange(gate, scope_of(f"/assistant/results/{OWN}/0", secret_header()))
            listing = await _exchange(gate, scope_of(f"/assistant/results/{OWN}", secret_header()))
            release.set()
            done = [answer(await task) for task in slow]
            after = await _exchange(gate, scope_of(f"/assistant/results/{OWN}/0", secret_header()))
            return answer(third), answer(listing), done, answer(after)

        third, listing, done, after = asyncio.run(run())
        assert third[0] == 429 and listing[0] == 200
        assert [d[2] for d in done] == [b"a,b\n1,2\n"] * 2 and after[2] == b"a,b\n1,2\n" and gate.downloads == 0

    def test_a_part_that_grows_is_sent_up_to_its_length_and_one_that_shrinks_is_cut(self, gate, tmp_path):
        """Bug class: more bytes sent than the `Content-Length` announced (the client would take them as a next answer), or a shrunken file padded or announced as complete."""
        finished(gate.store, OWN)
        folder = self.result(gate, tmp_path, files={"out.csv": b"x" * 100})

        def grow(message):
            if message["type"] == "http.response.start":
                with open(folder / "out.csv", "ab") as f:
                    f.write(b"y" * 1000)

        async def run(change):
            async def send(message):
                change(message)

            return answer(await _exchange(gate, scope_of(f"/assistant/results/{OWN}/0", secret_header()), send))

        status, headers, body, complete = asyncio.run(run(grow))
        assert headers[b"content-length"] == b"100" and body == b"x" * 100 and complete

        def shrink(message):
            if message["type"] == "http.response.start":
                os.truncate(folder / "out.csv", 10)

        status, headers, body, complete = asyncio.run(run(shrink))
        assert headers[b"content-length"] == b"1100" and len(body) == 10 and not complete


# ── the door ────────────────────────────────────────────────────────────────


def test_the_door_s_short_view_carries_the_mark_and_its_digest_does_not():
    """Bug class: the Queue window unable to tell the assistant's queries in a list, or the mark changing the digest of what runs (an approval read before the mark was added would be refused)."""
    from social_data_pipeline.jobs.door import digest

    marked, plain = make_job(OWN), make_job(OWN, "")
    assert short_view(marked)["submitted_by"] == "assistant" and "submitted_by" not in short_view(plain)
    assert digest(marked) == digest(plain)


# ── review round 1 ──────────────────────────────────────────────────────────


class FakeMCP:
    """Stands for `MCPServer` (the `mcp` package is not installed here): keeps the functions registered as tools."""

    def __init__(self, name=""):
        self.tools = {}

    def tool(self, name, description):
        def register(fn):
            self.tools[name] = fn
            return fn

        return register


def mongo_cfg(tmp_path, store):
    cfg = runner_for(tmp_path, store).cfg
    cfg.targets["olap"] = Target(name="olap", backend="starrocks", database="")
    cfg.targets["docstore"] = Target(name="docstore", backend="mongodb", database="")
    return cfg


def assistant_tools(tmp_path, store, monkeypatch):
    from social_data_pipeline.jobs import mcp_tools

    monkeypatch.setattr(mcp_tools, "MCPServer", FakeMCP)
    cfg = mongo_cfg(tmp_path, store)
    return mcp_tools, mcp_tools.build_mcp(cfg, store, runner_for(tmp_path, store), assistant=True).tools


CALLS = {
    "submit_postgres_query": {"sql": "SELECT 1", "target": "warehouse", "output_filename": "a.csv"},
    "submit_starrocks_query": {"sql": "SELECT 1", "target": "olap", "output_filename": "a.csv"},
    "submit_mongo_query": {"target": "docstore", "database": "d", "collection": "c", "pipeline": [{"$match": {}}], "output_filename": "a.ndjson"},
}


def test_the_assistants_flavour_has_no_list_mongo_databases(tmp_path, store, monkeypatch):
    """Bug class (security): the assistant reaching the database with its admin login, without anyone's approval, through `list_mongo_databases`."""
    mcp_tools, tools = assistant_tools(tmp_path, store, monkeypatch)
    assert sorted(tools) == ["list_targets", "query_cancel", "query_status", "submit_mongo_query", "submit_postgres_query", "submit_starrocks_query"]
    ordinary = mcp_tools.build_mcp(mongo_cfg(tmp_path, store), store, runner_for(tmp_path, store)).tools
    assert "list_mongo_databases" in ordinary


@pytest.mark.parametrize("name", sorted(CALLS))
def test_a_slow_check_of_a_submission_does_not_hold_the_event_loop(tmp_path, store, monkeypatch, name):
    """Bug class: a submission whose check takes minutes (a long trailing run of semicolons) freezing the page, the door and every other request; or the store written off the loop, where its transitions can meet the page's."""
    import threading

    mcp_tools, tools = assistant_tools(tmp_path, store, monkeypatch)
    entered, release, seen = threading.Event(), threading.Event(), {}
    real_validate, real_submit = mcp_tools.validate_submission, store.submit

    def slow(job):
        seen["check"] = threading.get_ident()
        entered.set()
        assert release.wait(10)
        return real_validate(job)

    def submit(job):
        seen["write"] = threading.get_ident()
        return real_submit(job)

    monkeypatch.setattr(mcp_tools, "validate_submission", slow)
    monkeypatch.setattr(store, "submit", submit)

    async def run():
        seen["loop"] = threading.get_ident()
        task = asyncio.create_task(tools[name](**CALLS[name]))
        assert await asyncio.to_thread(entered.wait, 10)
        # The loop serves a store read while the check blocks.
        served = store.find(UNKNOWN)
        seen["served while blocked"] = not release.is_set() and served is None
        release.set()
        return await task

    answer_ = asyncio.run(run())
    assert answer_["status"] == "pending" and store.find(answer_["job_id"])[1].submitted_by == "assistant"
    assert seen["served while blocked"] and seen["check"] != seen["loop"] and seen["write"] == seen["loop"]


@pytest.mark.parametrize("name, field, value", [
    ("submit_postgres_query", "sql", "S" * 1_000_001),
    ("submit_starrocks_query", "sql", "S" * 1_000_001),
    ("submit_mongo_query", "pipeline", [{"$match": {"a": "x" * 1_000_000}}]),
    ("submit_postgres_query", "description", "d" * 2001),
    ("submit_mongo_query", "description", "d" * 2001),
])
def test_the_assistants_submissions_are_capped_before_any_check(tmp_path, store, monkeypatch, name, field, value):
    """Bug class: a huge text from the assistant reaching the quadratic checks, or stored for a person to read."""
    mcp_tools, tools = assistant_tools(tmp_path, store, monkeypatch)
    checked = []
    monkeypatch.setattr(mcp_tools, "validate_submission", checked.append)
    answer_ = asyncio.run(tools[name](**{**CALLS[name], field: value}))
    assert set(answer_) == {"error"} and ("1,000,000" in answer_["error"] or "2,000" in answer_["error"]) and "not queued" in answer_["error"]
    assert checked == [] and list(store.pending.glob("*.json")) == []


def parts_folder(gate, tmp_path, count=300):
    finished(gate.store, OWN)
    write_secret(tmp_path)
    folder = tmp_path / "results" / OWN
    folder.mkdir(parents=True)
    for index in range(count):
        (folder / f"out_{index}.csv").write_bytes(f"part {index}\n".encode())
    return folder


def test_a_listing_and_a_download_of_300_parts_hold_one_part_open_at_most(gate, tmp_path, monkeypatch):
    """Bug class: every part of a result opened and held for one request (a few requests exhaust the scheduler's descriptors), or the wrong part served for an index."""
    import threading

    parts_folder(gate, tmp_path)
    opened, peak, names = set(), [0], []
    real_open, real_close = os.open, os.close
    main = threading.get_ident()

    def counting_open(path, *args, **kwargs):
        fd = real_open(path, *args, **kwargs)
        if threading.get_ident() != main:
            opened.add(fd)
            peak[0] = max(peak[0], len(opened))
            names.append(path)
        return fd

    def counting_close(fd):
        opened.discard(fd)
        return real_close(fd)

    monkeypatch.setattr(os, "open", counting_open)
    monkeypatch.setattr(os, "close", counting_close)
    status, _, body, _ = exchange(gate, f"/assistant/results/{OWN}", secret_header())
    listing = json.loads(body)["parts"]
    assert status == 200 and len(listing) == 300 and listing[150] == {"index": 150, "name": "out_150.csv", "size": 9}
    assert peak[0] == 1 and len(names) == 1
    names.clear()
    assert exchange(gate, f"/assistant/results/{OWN}/150", secret_header())[2] == b"part 150\n"
    assert peak[0] == 2 and [n for n in names if n.startswith("out_")] == ["out_150.csv"] and opened == set()


def test_a_failure_of_the_file_system_on_one_part_refuses_the_request_and_never_shifts_an_index(gate, tmp_path, monkeypatch):
    """Bug class: a part that cannot be looked at (too many open files) passed over as "not a part", so a number points at the next file."""
    import errno

    parts_folder(gate, tmp_path, count=10)
    real_stat = os.stat

    def failing(path, *args, **kwargs):
        if path == "out_5.csv" and kwargs.get("dir_fd") is not None:
            raise OSError(errno.EMFILE, "Too many open files")
        return real_stat(path, *args, **kwargs)

    monkeypatch.setattr(os, "stat", failing)
    unavailable = (503, {b"content-type": b"application/json", b"content-length": b"32"}, b'{"detail":"Service Unavailable"}', True)
    for path in (f"/assistant/results/{OWN}", f"/assistant/results/{OWN}/5", f"/assistant/results/{OWN}/7", f"/assistant/results/{OWN}/2"):
        assert exchange(gate, path, secret_header()) == unavailable
    monkeypatch.setattr(os, "stat", real_stat)
    assert exchange(gate, f"/assistant/results/{OWN}/7", secret_header())[2] == b"part 7\n"


def test_at_most_four_listings_run_at_a_time(gate, tmp_path, monkeypatch):
    """Bug class: listings without a bound tying up the scheduler's threads and descriptors."""
    import threading

    parts_folder(gate, tmp_path, count=2)
    release = threading.Event()
    real = assistant.list_parts

    def held(folder, output_filename):
        assert release.wait(10)
        return real(folder, output_filename)

    monkeypatch.setattr(assistant, "list_parts", held)

    async def run():
        listings = [asyncio.create_task(_exchange(gate, scope_of(f"/assistant/results/{OWN}", secret_header()))) for _ in range(4)]
        while gate.listings < 4:
            await asyncio.sleep(0.01)
        fifth = await _exchange(gate, scope_of(f"/assistant/results/{OWN}", secret_header()))
        download = await _exchange(gate, scope_of(f"/assistant/results/{OWN}/1", secret_header()))
        release.set()
        return answer(fifth), answer(download), [answer(await task) for task in listings]

    fifth, download, done = asyncio.run(run())
    assert fifth[0] == 429 and download[2] == b"part 1\n" and [d[0] for d in done] == [200] * 4 and gate.listings == 0


# ── review round 2 ──────────────────────────────────────────────────────────


def nested(depth):
    value = 1
    for _ in range(depth):
        value = {"a": value}
    return value


@pytest.mark.parametrize("pipeline, refusal", [
    ([{"$match": nested(40)}], "nested deeper than 32 levels"),
    ([{"$match": {"a": [[[[0] * 300_000]]]}}], "the pipeline is longer than 1,000,000 characters as JSON"),
])
def test_a_pipeline_stored_larger_than_the_cap_or_nested_too_deep_is_refused(tmp_path, store, monkeypatch, pipeline, refusal):
    """Bug class: a pipeline under the compact cap stored as indented JSON tens of times larger (the page and the door read it whole), or one nested deep enough to make a recursive walk fail."""
    _, tools = assistant_tools(tmp_path, store, monkeypatch)
    import json as json_module

    assert len(json_module.dumps(pipeline)) < 1_000_000
    answer_ = asyncio.run(tools["submit_mongo_query"](**{**CALLS["submit_mongo_query"], "pipeline": pipeline}))
    assert set(answer_) == {"error"} and refusal in answer_["error"] and "not queued" in answer_["error"]
    assert list(store.pending.glob("*.json")) == []


def test_a_missing_database_points_the_assistant_to_the_read_only_server(tmp_path, store, monkeypatch):
    """Bug class: the assistant told to call `list_mongo_databases`, which its flavour does not have, or the ordinary tool's text changed."""
    from social_data_pipeline.jobs import mcp_tools as tools_module

    _, tools = assistant_tools(tmp_path, store, monkeypatch)
    for database in ("", "   "):
        answer_ = asyncio.run(tools["submit_mongo_query"](**{**CALLS["submit_mongo_query"], "database": database}))
        assert answer_ == {"error": "database is required for Mongo submissions; find the databases through the read-only MongoDB MCP server (its own tools list them)."}
    ordinary = tools_module._submit_mongo(store=store, cfg=mongo_cfg(tmp_path, store), target="docstore", collection="c", pipeline=[], output_filename="a.ndjson", overwrite=False, database=" ")
    assert ordinary == {"error": "database is required for Mongo submissions; call list_mongo_databases(target) to see what's available."}
    assert list(store.pending.glob("*.json")) == []


def test_the_ordinary_tools_log_records_are_unchanged(tmp_path, store, caplog):
    """Bug class: a log record of the ordinary `/mcp` tools changed in its format or arguments (log handlers and searches that read `msg` and `args`)."""
    import logging

    cfg = mongo_cfg(tmp_path, store)
    with caplog.at_level(logging.INFO, logger="social_data_pipeline.jobs.mcp_tools"):
        sql = _submit(store=store, cfg=cfg, backend="postgres", target="warehouse", sql="SELECT 1", output_filename="a.csv", overwrite=False)
        mongo = _submit_mongo(store=store, cfg=cfg, target="docstore", collection="c", pipeline=[], output_filename="a.ndjson", overwrite=False, database="d")
        _submit(store=store, cfg=cfg, backend="postgres", target="warehouse", sql="SELECT 1", output_filename="a.csv", overwrite=False, submitted_by="assistant")
    records = [(r.msg, r.args) for r in caplog.records if r.name == "social_data_pipeline.jobs.mcp_tools"]
    assert records[:2] == [("submitted job %s target=%s backend=%s", (sql["job_id"], "warehouse", "postgres")), ("submitted mongo job %s target=%s db=%s collection=%s", (mongo["job_id"], "docstore", "d", "c"))]
    assert records[2][0].endswith("by the assistant")


def test_a_part_swapped_between_its_look_and_its_open_is_refused(gate, tmp_path, monkeypatch):
    """Bug class: a file moved into a part's name after the part was looked at (the job folders are world-writable) served in its place."""
    folder = parts_folder(gate, tmp_path, count=3)
    (tmp_path / "swap.bin").write_text(MARKER)
    real_open = os.open

    def swapping(path, *args, **kwargs):
        if path == "out_1.csv" and kwargs.get("dir_fd") is not None:
            os.replace(tmp_path / "swap.bin", folder / "out_1.csv")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(os, "open", swapping)
    status, _, body, _ = exchange(gate, f"/assistant/results/{OWN}/1", secret_header())
    assert status == 503 and MARKER.encode() not in body
