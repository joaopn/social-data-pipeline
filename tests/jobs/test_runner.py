"""Tests for the Runner — backend wiring, dispatch, cancel, orphan recovery.

The runner's poll loop is a simple thread that calls `_drain_once()` at fixed
intervals; we test `_drain_once()` and `_run_job()` directly rather than start
real threads. Backends are replaced with in-memory fakes that record calls.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import pytest

from social_data_pipeline.jobs.auto_accept import AutoAcceptStore
from social_data_pipeline.jobs.backends.base import (
    BackendError,
    ExecutionHandle,
    ExecutionResult,
)
from social_data_pipeline.jobs.config import JobsConfig, Target
from social_data_pipeline.jobs.runner import Runner
from social_data_pipeline.jobs.store import Job, Store


# ── fakes ───────────────────────────────────────────────────────────────────


@dataclass
class FakeResult:
    rows: int = 1
    size_bytes: int = 100
    result_path: str = ""


class FakeBackend:
    """In-memory backend that records calls and returns a configurable result.

    Behaviour can be flipped with `mode`:
      "ok"       → execute() returns FakeResult
      "raise"    → execute() raises BackendError
      "crash"    → execute() raises a non-BackendError
      "cancel"   → execute() honors cancel by raising BackendError after the
                   on_handle callback (simulates a server-side cancel)
    """

    def __init__(self, mode: str = "ok"):
        self.mode = mode
        self.executed: list[Job] = []
        self.cancelled: list[ExecutionHandle] = []
        self.explained: list[str] = []
        self.list_db_calls = 0

    def execute(self, job: Job, timeout_seconds: int, on_handle):
        on_handle(ExecutionHandle(backend_pid=1234))
        self.executed.append(job)
        if self.mode == "raise":
            raise BackendError("backend rejected")
        if self.mode == "crash":
            raise RuntimeError("kaboom")
        if self.mode == "cancel":
            raise BackendError("query cancelled")
        return ExecutionResult(rows=10, size_bytes=512, result_path="/some/path")

    def cancel(self, handle: ExecutionHandle, job: Job | None = None):
        self.cancelled.append(handle)

    def explain(self, sql: str, job: Job | None = None) -> str:
        self.explained.append(sql)
        return "PLAN OK"

    def list_databases(self):
        self.list_db_calls += 1
        return [{"name": "x", "sizeOnDisk": 0, "empty": False}]

    def validate(self, job: Job) -> None:
        pass


def _cfg(tmp_path: Path, **overrides) -> JobsConfig:
    base = dict(
        port=8050,
        jobs_dir=tmp_path,
        result_root=tmp_path / "results",
        host_result_root=str(tmp_path / "results"),
        max_concurrent=1,
        default_timeouts={"postgres": 0, "starrocks": 259200, "mongodb": 0},
        history_retention=500,
        auth_enabled=False,
        targets={
            "warehouse": Target(name="warehouse", backend="postgres", database="datasets"),
            "olap": Target(name="olap", backend="starrocks", database=""),
            "docstore": Target(name="docstore", backend="mongodb", database=""),
        },
    )
    base.update(overrides)
    return JobsConfig(**base)


def _auto_accept(tmp_path: Path, max_limit: int = 4) -> AutoAcceptStore:
    return AutoAcceptStore(state_path=tmp_path / "auto_accept.json", max_limit=max_limit)


def _runner_with_fake_backends(tmp_path):
    cfg = _cfg(tmp_path)
    store = Store(jobs_dir=tmp_path)
    runner = Runner(cfg, store, _auto_accept(tmp_path))
    fakes = {key: FakeBackend() for key in runner._backends}
    runner._backends = fakes
    return runner, store, fakes


def _job(target="warehouse", backend="postgres", **overrides) -> Job:
    base = dict(
        job_id=f"{backend[:2]}_{target}_test",
        target=target,
        backend=backend,
        sql="SELECT 1",
        output_filename="out.parquet",
        overwrite=False,
        submitted_at=time.time(),
    )
    base.update(overrides)
    return Job(**base)


# ── _build_backends + _backend_for ──────────────────────────────────────────


class TestBackendWiring:
    def test_builds_one_per_target(self, tmp_path):
        cfg = _cfg(tmp_path)
        store = Store(jobs_dir=tmp_path)
        runner = Runner(cfg, store, _auto_accept(tmp_path))
        # Three targets → three backend instances, keyed by "<backend>:<target>".
        assert set(runner._backends.keys()) == {
            "postgres:warehouse",
            "starrocks:olap",
            "mongodb:docstore",
        }

    def test_backend_for_resolves_by_target(self, tmp_path):
        runner, _, _ = _runner_with_fake_backends(tmp_path)
        b = runner._backend_for(_job(target="warehouse", backend="postgres"))
        assert b is runner._backends["postgres:warehouse"]

    def test_backend_for_unknown_raises(self, tmp_path):
        runner, _, _ = _runner_with_fake_backends(tmp_path)
        with pytest.raises(BackendError, match="no backend"):
            runner._backend_for(_job(target="ghost", backend="postgres"))


# ── _run_job (synchronous run, replaces the pool worker call) ──────────────


def _approve_and_claim(store: Store, job: Job) -> Job:
    store.submit(job)
    store.approve(job.job_id)
    claimed = store.claim_approved()
    assert claimed is not None
    return claimed


class TestRunJobOk:
    def test_records_completion_and_metadata(self, tmp_path):
        runner, store, fakes = _runner_with_fake_backends(tmp_path)
        job = _approve_and_claim(store, _job())
        cancel_flag = threading.Event()

        runner._run_job(job, cancel_flag)

        # Job moved to history with status=done and metadata from ExecutionResult.
        hist = store.iter_history()
        assert hist[0].status == "done"
        assert hist[0].rows == 10
        assert hist[0].size_bytes == 512
        # Backend was called.
        assert len(fakes["postgres:warehouse"].executed) == 1


class TestRunJobErrors:
    def test_backend_error_marks_failed(self, tmp_path):
        runner, store, fakes = _runner_with_fake_backends(tmp_path)
        fakes["postgres:warehouse"].mode = "raise"
        job = _approve_and_claim(store, _job())

        runner._run_job(job, threading.Event())

        hist = store.iter_history()
        assert hist[0].status == "failed"
        assert "rejected" in (hist[0].error or "")

    def test_unexpected_exception_marks_failed_with_type(self, tmp_path):
        runner, store, fakes = _runner_with_fake_backends(tmp_path)
        fakes["postgres:warehouse"].mode = "crash"
        job = _approve_and_claim(store, _job())

        runner._run_job(job, threading.Event())

        hist = store.iter_history()
        assert hist[0].status == "failed"
        # Uncaught exceptions are recorded as "TypeName: message".
        assert hist[0].error.startswith("RuntimeError: ")

    def test_cancel_flag_set_during_backend_error_marks_cancelled(self, tmp_path):
        runner, store, fakes = _runner_with_fake_backends(tmp_path)
        fakes["postgres:warehouse"].mode = "raise"
        job = _approve_and_claim(store, _job())
        flag = threading.Event()
        flag.set()

        runner._run_job(job, flag)

        hist = store.iter_history()
        assert hist[0].status == "cancelled"

    def test_cancel_after_completion_wins_race(self, tmp_path):
        # execute() returned normally but cancel_flag was set in flight —
        # runner must mark cancelled rather than done.
        runner, store, _ = _runner_with_fake_backends(tmp_path)
        job = _approve_and_claim(store, _job())
        flag = threading.Event()
        flag.set()

        runner._run_job(job, flag)

        hist = store.iter_history()
        assert hist[0].status == "cancelled"


class TestRunJobCleanup:
    def test_removes_result_folder_on_failure(self, tmp_path):
        runner, store, fakes = _runner_with_fake_backends(tmp_path)
        fakes["postgres:warehouse"].mode = "raise"
        job = _approve_and_claim(store, _job())

        # Pre-populate the result folder to simulate a partial write.
        result_dir = store.job_result_dir(job.job_id)
        result_dir.mkdir(parents=True, exist_ok=True)
        (result_dir / "partial.parquet").write_bytes(b"partial")

        runner._run_job(job, threading.Event())

        assert not result_dir.exists()


# ── orphan recovery ─────────────────────────────────────────────────────────


class TestOrphanRecovery:
    def test_running_jobs_are_failed_on_recover(self, tmp_path):
        runner, store, _ = _runner_with_fake_backends(tmp_path)
        job = _approve_and_claim(store, _job())

        runner._recover_orphans()

        hist = store.iter_history()
        assert hist[0].job_id == job.job_id
        assert hist[0].status == "failed"
        assert "runner restarted" in hist[0].error

    def test_recover_with_no_orphans_is_noop(self, tmp_path):
        runner, store, _ = _runner_with_fake_backends(tmp_path)
        runner._recover_orphans()
        assert store.iter_history() == []


# ── _drain_once ─────────────────────────────────────────────────────────────


class TestDrainOnce:
    def test_drains_all_approved_jobs(self, tmp_path, monkeypatch):
        """With sync execution, the active dict empties between iterations,
        so a single _drain_once() walks all approved jobs."""
        cfg = _cfg(tmp_path, max_concurrent=2)
        store = Store(jobs_dir=tmp_path)
        runner = Runner(cfg, store, _auto_accept(tmp_path))
        runner._backends = {key: FakeBackend() for key in runner._backends}

        from concurrent.futures import Future

        def submit_sync(fn, *args, **kwargs):
            fut: Future = Future()
            try:
                fut.set_result(fn(*args, **kwargs))
            except Exception as e:
                fut.set_exception(e)
            return fut

        monkeypatch.setattr(runner._pool, "submit", submit_sync)

        for i in range(3):
            j = _job(job_id=f"pg_{i:08x}")
            store.submit(j)
            store.approve(j.job_id)

        runner._drain_once()

        # All three drained → all in history, none still approved.
        assert len(store.iter_history()) == 3
        assert list(store.approved.glob("*.json")) == []

    def test_capacity_caps_active_set(self, tmp_path, monkeypatch):
        """With max_concurrent=1 and a non-completing future, the second
        approved job stays in approved/ until the first frees a slot."""
        cfg = _cfg(tmp_path, max_concurrent=1)
        store = Store(jobs_dir=tmp_path)
        runner = Runner(cfg, store, _auto_accept(tmp_path))
        runner._backends = {key: FakeBackend() for key in runner._backends}

        from concurrent.futures import Future

        # Non-completing future — _deregister never fires.
        def submit_pending(fn, *args, **kwargs):
            return Future()

        monkeypatch.setattr(runner._pool, "submit", submit_pending)

        for i in range(2):
            j = _job(job_id=f"pg_{i:08x}")
            store.submit(j)
            store.approve(j.job_id)

        runner._drain_once()

        # Only one slot, one future in flight.
        assert len(runner.active_job_ids()) == 1
        # The other job sits in running/ (claimed by claim_approved) — wait,
        # actually claim_approved already moved it to running/. So with
        # max_concurrent=1 and the active dict full, the second should
        # never be claimed.
        running = list(store.running.glob("*.json"))
        approved = list(store.approved.glob("*.json"))
        assert len(running) == 1
        assert len(approved) == 1


# ── request_cancel + active_job_ids ─────────────────────────────────────────


class TestRequestCancel:
    def test_returns_false_for_unknown_job(self, tmp_path):
        runner, _, _ = _runner_with_fake_backends(tmp_path)
        assert runner.request_cancel("ghost") is False

    def test_calls_backend_cancel_for_active_job(self, tmp_path):
        runner, store, fakes = _runner_with_fake_backends(tmp_path)
        job = _approve_and_claim(store, _job())
        # Manually register as active without actually running.
        flag = threading.Event()
        from concurrent.futures import Future
        runner._active[job.job_id] = (Future(), flag)

        ok = runner.request_cancel(job.job_id)
        assert ok is True
        assert flag.is_set()
        assert len(fakes["postgres:warehouse"].cancelled) == 1

    def test_active_job_ids_lists_registered(self, tmp_path):
        runner, _, _ = _runner_with_fake_backends(tmp_path)
        from concurrent.futures import Future
        runner._active["a"] = (Future(), threading.Event())
        runner._active["b"] = (Future(), threading.Event())
        assert set(runner.active_job_ids()) == {"a", "b"}


# ── explain dispatch ────────────────────────────────────────────────────────


class TestExplain:
    def test_dispatches_to_backend(self, tmp_path):
        runner, store, fakes = _runner_with_fake_backends(tmp_path)
        job = _job()
        store.submit(job)

        plan = runner.explain(job.job_id)

        assert plan == "PLAN OK"
        assert fakes["postgres:warehouse"].explained == [job.sql]

    def test_unknown_job_raises(self, tmp_path):
        runner, _, _ = _runner_with_fake_backends(tmp_path)
        with pytest.raises(KeyError):
            runner.explain("ghost")

    def test_starrocks_explain_kwarg_compat(self, tmp_path):
        """SR backend's explain() doesn't accept `job=` kwarg in production —
        the dispatcher catches TypeError and retries without it. Verify the
        retry path with a backend whose explain() rejects the job kwarg.
        """
        runner, store, _ = _runner_with_fake_backends(tmp_path)

        class StrictBackend(FakeBackend):
            def explain(self, sql: str) -> str:  # no job kwarg
                self.explained.append(sql)
                return "STRICT PLAN"

        runner._backends["starrocks:olap"] = StrictBackend()
        job = _job(target="olap", backend="starrocks")
        store.submit(job)

        assert runner.explain(job.job_id) == "STRICT PLAN"


# ── list_mongo_databases dispatch ───────────────────────────────────────────


class TestListMongoDatabases:
    def test_dispatches_to_mongo_target(self, tmp_path):
        runner, _, fakes = _runner_with_fake_backends(tmp_path)
        out = runner.list_mongo_databases("docstore")
        assert out == [{"name": "x", "sizeOnDisk": 0, "empty": False}]
        assert fakes["mongodb:docstore"].list_db_calls == 1

    def test_unknown_target_raises(self, tmp_path):
        runner, _, _ = _runner_with_fake_backends(tmp_path)
        with pytest.raises(KeyError):
            runner.list_mongo_databases("ghost")


# ── _auto_approve_eligible ──────────────────────────────────────────────────


class TestAutoApproveEligible:
    def test_no_target_enabled_is_noop(self, tmp_path):
        # Default state: no targets in auto_accept.json → nothing approved.
        runner, store, _ = _runner_with_fake_backends(tmp_path)
        for i in range(3):
            j = _job(job_id=f"pg_{i:08x}")
            store.submit(j)

        n = runner._auto_approve_eligible()

        assert n == 0
        assert len(list(store.pending.glob("*.json"))) == 3
        assert len(list(store.approved.glob("*.json"))) == 0

    def test_approves_up_to_limit_fifo(self, tmp_path):
        runner, store, _ = _runner_with_fake_backends(tmp_path)
        runner.auto_accept.set_target("warehouse", enabled=True, limit=2)

        # Submit three jobs in order; the first two should auto-approve.
        # mtime resolution is fine here — the loop sleeps long enough.
        ids = []
        for i in range(3):
            j = _job(job_id=f"pg_{i:08x}")
            store.submit(j)
            ids.append(j.job_id)
            time.sleep(0.01)

        n = runner._auto_approve_eligible()

        assert n == 2
        approved_ids = {p.stem for p in store.approved.glob("*.json")}
        # FIFO: oldest two are approved.
        assert approved_ids == {ids[0], ids[1]}
        # Third stays pending.
        pending_ids = {p.stem for p in store.pending.glob("*.json")}
        assert pending_ids == {ids[2]}

    def test_running_jobs_consume_slots(self, tmp_path):
        # If one job is already running for the target, only `limit - 1`
        # more are auto-approved this tick.
        runner, store, _ = _runner_with_fake_backends(tmp_path)
        runner.auto_accept.set_target("warehouse", enabled=True, limit=2)

        # Pre-populate one running job for warehouse.
        running_job = _job(job_id="pg_running1")
        store.submit(running_job)
        store.approve(running_job.job_id)
        store.claim_approved()
        # Now there's exactly one running job.

        for i in range(3):
            j = _job(job_id=f"pg_pend_{i}")
            store.submit(j)
            time.sleep(0.01)

        n = runner._auto_approve_eligible()

        assert n == 1
        # 1 running + 1 newly approved == limit
        assert len(list(store.approved.glob("*.json"))) == 1

    def test_disabled_target_is_skipped(self, tmp_path):
        runner, store, _ = _runner_with_fake_backends(tmp_path)
        runner.auto_accept.set_target("warehouse", enabled=False, limit=5)

        store.submit(_job(job_id="pg_x"))
        n = runner._auto_approve_eligible()

        assert n == 0
        assert len(list(store.pending.glob("*.json"))) == 1

    def test_per_target_isolation(self, tmp_path):
        # Auto-accept on for warehouse only; jobs for olap stay pending
        # even when warehouse has spare slots.
        runner, store, _ = _runner_with_fake_backends(tmp_path)
        runner.auto_accept.set_target("warehouse", enabled=True, limit=5)

        store.submit(_job(job_id="pg_a", target="warehouse", backend="postgres"))
        time.sleep(0.01)
        store.submit(_job(job_id="sr_b", target="olap", backend="starrocks"))

        n = runner._auto_approve_eligible()

        assert n == 1
        approved_ids = {p.stem for p in store.approved.glob("*.json")}
        assert approved_ids == {"pg_a"}
        pending_ids = {p.stem for p in store.pending.glob("*.json")}
        assert pending_ids == {"sr_b"}

    def test_race_with_manual_approve_swallows_keyerror(self, tmp_path, monkeypatch):
        # Simulate the race: auto-accept reads pending list; before it
        # calls store.approve(), a manual approve has already moved the
        # file. The KeyError must be swallowed and the loop continues
        # for the next target's slots.
        runner, store, _ = _runner_with_fake_backends(tmp_path)
        runner.auto_accept.set_target("warehouse", enabled=True, limit=2)

        store.submit(_job(job_id="pg_doomed"))
        time.sleep(0.01)
        store.submit(_job(job_id="pg_survivor"))

        original_approve = store.approve
        calls = {"n": 0}

        def flaky_approve(job_id):
            calls["n"] += 1
            if calls["n"] == 1:
                # Simulate the file already being moved by a manual click.
                raise KeyError(f"job {job_id} not in pending")
            return original_approve(job_id)

        monkeypatch.setattr(store, "approve", flaky_approve)

        n = runner._auto_approve_eligible()

        # First failed (KeyError swallowed), second succeeded.
        assert n == 1
        assert calls["n"] == 2


# ── request_cancel(by=...): who killed a job ────────────────────────────────


class BlockingBackend(FakeBackend):
    """Runs until it is cancelled: `execute` waits for `cancel`, then fails as a server-side cancel does."""

    def __init__(self):
        super().__init__()
        self.started = threading.Event()
        self.stop = threading.Event()

    def execute(self, job: Job, timeout_seconds: int, on_handle):
        on_handle(ExecutionHandle(backend_pid=1234))
        self.executed.append(job)
        self.started.set()
        assert self.stop.wait(15), "never cancelled"
        raise BackendError("query cancelled")

    def cancel(self, handle: ExecutionHandle, job: Job | None = None):
        self.cancelled.append(handle)
        self.stop.set()


class TestRequestCancelLabel:
    @pytest.mark.parametrize("by", ["web-7", None])
    def test_the_label_reaches_the_record_the_worker_retires(self, tmp_path, by):
        """Bug class: who killed a job set on a copy of the record (the one `request_cancel` reads from the store) while the worker retires its own, so the history never says who; or a kill without a label writing one."""
        runner, store, fakes = _runner_with_fake_backends(tmp_path)
        backend = BlockingBackend()
        runner._backends["postgres:warehouse"] = backend
        job = _job(job_id="pg_0000beef")
        store.submit(job)
        store.approve(job.job_id)
        try:
            runner._drain_once()
            assert backend.started.wait(10)
            assert runner.request_cancel(job.job_id, by=by) is True
            end = time.monotonic() + 10
            while not store.iter_history() and time.monotonic() < end:
                time.sleep(0.01)
        finally:
            backend.stop.set()
            runner.stop()
        retired = store.iter_history()[0]
        assert retired.status == "cancelled"
        assert retired.acted_by == ({"killed": by} if by else {})
        assert ("acted_by" in store.history.read_text()) == (by is not None)
        assert len(backend.cancelled) == 1
        assert runner.active_job_ids() == [] and runner._held == {}


WRONG_ACTED_BY = ["x", ["web-1"], None, 5, {"approved": 1}, {"other": "web-1"}, {"killed": None}]


class TestWrongLabelsNeverStopAKill:
    @pytest.mark.parametrize("wrong", WRONG_ACTED_BY, ids=repr)
    @pytest.mark.parametrize("by", ["web-7", None])
    def test_a_record_with_labels_of_another_shape_can_still_be_killed(self, tmp_path, wrong, by):
        """Bug class: a hand-placed record whose `acted_by` is not a dict of labels (a string, a list, null) making `request_cancel` raise while it adds the kill's label, before the cancel is signalled, so the job cannot be killed through the door (or, with `by=None`, the page)."""
        runner, store, _fakes = _runner_with_fake_backends(tmp_path)
        backend = BlockingBackend()
        runner._backends["postgres:warehouse"] = backend
        record = {**_job(job_id="pg_0000beef").to_dict(), "acted_by": wrong, "status": "approved"}
        (store.approved / "pg_0000beef.json").write_text(json.dumps(record))
        try:
            runner._drain_once()
            assert backend.started.wait(10)
            assert runner.request_cancel("pg_0000beef", by=by) is True
            end = time.monotonic() + 10
            while not store.iter_history() and time.monotonic() < end:
                time.sleep(0.01)
        finally:
            backend.stop.set()
            runner.stop()
        retired = store.iter_history()[0]
        assert retired.status == "cancelled" and len(backend.cancelled) == 1
        assert retired.acted_by == ({"killed": by} if by else {})
        assert retired.extras == {"acted_by": wrong}

    @pytest.mark.parametrize("by", ["web-7", None])
    def test_a_label_that_cannot_be_added_never_stops_the_kill(self, tmp_path, by):
        """Bug class: anything about the label (here a held job whose labels were replaced by something else after it was loaded) raising before the cancel is signalled."""
        runner, store, _fakes = _runner_with_fake_backends(tmp_path)
        backend = BlockingBackend()
        runner._backends["postgres:warehouse"] = backend
        store.submit(_job(job_id="pg_0000beef"))
        store.approve("pg_0000beef")
        try:
            runner._drain_once()
            assert backend.started.wait(10)
            runner._held["pg_0000beef"].acted_by = "x"
            assert runner.request_cancel("pg_0000beef", by=by) is True
            end = time.monotonic() + 10
            while not store.iter_history() and time.monotonic() < end:
                time.sleep(0.01)
        finally:
            backend.stop.set()
            runner.stop()
        assert store.iter_history()[0].status == "cancelled" and len(backend.cancelled) == 1


class TestLabelsOfAnotherShapeLoad:
    @pytest.mark.parametrize("wrong", WRONG_ACTED_BY, ids=repr)
    def test_they_are_kept_in_extras_and_not_taken_as_labels(self, wrong):
        """Bug class: `acted_by` of another shape passed through to the transitions, which add to it (`{**acted_by, ...}`) and raise."""
        job = Job.from_dict({**_job().to_dict(), "acted_by": wrong})
        assert job.acted_by == {} and job.extras == {"acted_by": wrong}
        assert Job.from_dict(job.to_dict()) == job

    def test_labels_of_the_known_shape_are_kept(self):
        """Bug class: a check so strict that the scheduler's own labels are moved away."""
        labels = {"approved": "web-1", "rejected": "web-2", "killed": "web-3"}
        job = Job.from_dict({**_job().to_dict(), "acted_by": labels})
        assert job.acted_by == labels and job.extras == {}
