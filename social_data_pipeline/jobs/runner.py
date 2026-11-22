"""Background runner: drains approved/ into running/ and executes jobs."""

from __future__ import annotations

import logging
import os
import shutil
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path

from .auto_accept import AutoAcceptStore
from .backends import (
    Backend,
    BackendError,
    ExecutionHandle,
    MongoBackend,
    PostgresBackend,
    StarrocksBackend,
)
from .config import JobsConfig
from .store import Job, Store


log = logging.getLogger(__name__)


class Runner:
    """Single background worker pool. Polls approved/ and executes jobs.

    One ``Runner`` instance per process. Thread-safe: the MCP/web handlers call
    ``request_cancel(job_id)`` from their own threads.
    """

    POLL_INTERVAL_SECONDS = 1.0

    def __init__(self, cfg: JobsConfig, store: Store, auto_accept: AutoAcceptStore):
        self.cfg = cfg
        self.store = store
        self.auto_accept = auto_accept
        self._stop = threading.Event()
        self._pool = ThreadPoolExecutor(max_workers=max(1, cfg.max_concurrent))
        self._poller: threading.Thread | None = None
        self._lock = threading.Lock()
        # job_id -> (Future, cancel_requested_flag)
        self._active: dict[str, tuple[Future, threading.Event]] = {}
        # job_id -> the Job object its worker runs, so that `request_cancel(by=...)` can label the record the worker retires
        self._held: dict[str, Job] = {}
        self._backends: dict[str, Backend] = self._build_backends()

    # ------------------------------------------------------------------

    def _build_backends(self) -> dict[str, Backend]:
        backends: dict[str, Backend] = {}
        if self.cfg.has_backend("postgres"):
            # Multiple PG databases share one adapter class; target name drives
            # the database attr so we build one backend instance per target.
            for t in self.cfg.targets_for("postgres"):
                backends[f"postgres:{t.name}"] = PostgresBackend(
                    result_root=self.cfg.result_root, database=t.database
                )
        if self.cfg.has_backend("starrocks"):
            for t in self.cfg.targets_for("starrocks"):
                backends[f"starrocks:{t.name}"] = StarrocksBackend(
                    result_root=self.cfg.result_root, database=t.database
                )
        if self.cfg.has_backend("mongodb"):
            # Mongo target = Mongo node. The database is supplied per-query
            # by the agent (via submit_mongo_query), matching how SR targets
            # expect fully-qualified references.
            for t in self.cfg.targets_for("mongodb"):
                backends[f"mongodb:{t.name}"] = MongoBackend(
                    result_root=self.cfg.result_root,
                )
        return backends

    def _backend_for(self, job: Job) -> Backend:
        key = f"{job.backend}:{job.target}"
        b = self._backends.get(key)
        if b is None:
            raise BackendError(
                f"no backend registered for target={job.target!r} backend={job.backend!r}"
            )
        return b

    # ------------------------------------------------------------------

    def start(self) -> None:
        self._recover_orphans()
        self._poller = threading.Thread(target=self._poll_loop, name="jobs-poller", daemon=True)
        self._poller.start()
        log.info("runner started (max_concurrent=%d)", self.cfg.max_concurrent)

    def stop(self, timeout: float | None = 5.0) -> None:
        self._stop.set()
        if self._poller:
            self._poller.join(timeout=timeout)
        self._pool.shutdown(wait=True, cancel_futures=True)

    # ------------------------------------------------------------------

    def _recover_orphans(self) -> None:
        orphans = self.store.orphaned_running()
        for job in orphans:
            log.warning("cleaning up orphaned running job %s", job.job_id)
            self._cleanup_result_folder(job.job_id)
            self.store.fail(job, error="runner restarted; job aborted")

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._auto_approve_eligible()
            except Exception:
                log.exception("auto-accept poll error")
            try:
                self._drain_once()
            except Exception:
                log.exception("runner poll error")
            self._stop.wait(self.POLL_INTERVAL_SECONDS)

    def _auto_approve_eligible(self) -> int:
        """Approve pending jobs for any target with available auto-accept slots.

        Returns the number of jobs approved this tick. FIFO by submitted_at
        (which is what ``list_phase("pending")`` already gives us via mtime
        sort). Per-target slots are computed from the live counts of jobs in
        ``approved/`` + ``running/`` so the cap holds even when the runner is
        busy on other targets and the approved queue has built up.
        """
        running = self.store.list_phase("running")
        approved = self.store.list_phase("approved")
        running_counts: dict[str, int] = {}
        approved_counts: dict[str, int] = {}
        for j in running:
            running_counts[j.target] = running_counts.get(j.target, 0) + 1
        for j in approved:
            approved_counts[j.target] = approved_counts.get(j.target, 0) + 1

        slots = self.auto_accept.eligible_targets(running_counts, approved_counts)
        if not slots:
            return 0

        approved_now = 0
        for job in self.store.list_phase("pending"):
            remaining = slots.get(job.target, 0)
            if remaining <= 0:
                continue
            try:
                self.store.approve(job.job_id)
            except KeyError:
                # Lost the race against a manual approve/reject — fine,
                # just move on to the next pending job.
                continue
            slots[job.target] = remaining - 1
            approved_now += 1
            log.info("auto-approved %s target=%s", job.job_id, job.target)
        return approved_now

    def _drain_once(self) -> None:
        while True:
            with self._lock:
                if len(self._active) >= self.cfg.max_concurrent:
                    return
            job = self.store.claim_approved()
            if job is None:
                return
            cancel_flag = threading.Event()
            fut = self._pool.submit(self._run_job, job, cancel_flag)
            with self._lock:
                self._active[job.job_id] = (fut, cancel_flag)
                self._held[job.job_id] = job
            fut.add_done_callback(lambda _f, jid=job.job_id: self._deregister(jid))

    def _deregister(self, job_id: str) -> None:
        with self._lock:
            self._active.pop(job_id, None)
            self._held.pop(job_id, None)

    # ------------------------------------------------------------------

    def _run_job(self, job: Job, cancel_flag: threading.Event) -> None:
        log.info("starting job %s target=%s backend=%s", job.job_id, job.target, job.backend)
        try:
            backend = self._backend_for(job)

            def on_handle(handle: ExecutionHandle) -> None:
                job.backend_pid = handle.backend_pid
                job.connection_id = handle.connection_id
                self.store.update_running(job)

            result = backend.execute(
                job=job,
                timeout_seconds=self.cfg.timeout_for(job.backend),
                on_handle=on_handle,
            )

            if cancel_flag.is_set():
                # Cancellation won the race: execute returned normally but we
                # already flagged — treat as cancelled and remove partial output.
                self._cleanup_result_folder(job.job_id)
                self.store.mark_cancelled(job)
                log.info("job %s cancelled (raced with completion)", job.job_id)
                return

            self._normalize_result_permissions(job.job_id)
            job.rows = result.rows
            job.size_bytes = result.size_bytes
            job.result_path = result.result_path
            self.store.complete(job)
            log.info("job %s done (rows=%s size=%s)", job.job_id, job.rows, job.size_bytes)
        except BackendError as e:
            if cancel_flag.is_set():
                self._cleanup_result_folder(job.job_id)
                self.store.mark_cancelled(job)
                log.info("job %s cancelled", job.job_id)
            else:
                self._cleanup_result_folder(job.job_id)
                self.store.fail(job, error=str(e))
                log.warning("job %s failed: %s", job.job_id, e)
        except Exception as e:
            log.exception("job %s crashed", job.job_id)
            self._cleanup_result_folder(job.job_id)
            self.store.fail(job, error=f"{type(e).__name__}: {e}")

    def _cleanup_result_folder(self, job_id: str) -> None:
        job_dir: Path = self.store.job_result_dir(job_id)
        if job_dir.exists():
            try:
                shutil.rmtree(job_dir, ignore_errors=True)
            except Exception:
                log.exception("failed to rmtree %s", job_dir)

    def _normalize_result_permissions(self, job_id: str) -> None:
        """Make result files readable by the host user.

        PG/SR write files as their container user with restrictive umasks
        (postgres uses 0077 → files 0600). The jobs runner, running as root
        inside its container, can chmod these after-the-fact so the user
        on the host (typically uid 1000) can read what landed.
        """
        job_dir: Path = self.store.job_result_dir(job_id)
        if not job_dir.exists():
            return
        try:
            os.chmod(job_dir, 0o755)
            for root, dirs, files in os.walk(job_dir):
                for d in dirs:
                    try:
                        os.chmod(os.path.join(root, d), 0o755)
                    except OSError:
                        pass
                for f in files:
                    try:
                        os.chmod(os.path.join(root, f), 0o644)
                    except OSError:
                        pass
        except OSError:
            log.exception("chmod normalize failed on %s", job_dir)

    # ------------------------------------------------------------------
    # Cancel API (called from web handlers)

    def request_cancel(self, job_id: str, by: str | None = None) -> bool:
        """Signal cancellation of a currently-running job.

        Opens a separate admin connection and terminates the backend query, so
        the execute() call in the worker thread raises and the runner marks
        the job cancelled. Returns True if the job was running.

        ``by`` (a label such as "web-12", from the control socket) is set as ``acted_by["killed"]`` on the job the worker holds, before the cancel is signalled, so that the record the worker retires carries it; it names who asked to kill (the job may still finish by itself). Setting it is guarded: nothing about the label can stop the kill. Without it nothing else differs.
        """
        with self._lock:
            active = self._active.get(job_id)
        if not active:
            return False
        _fut, flag = active

        located = self.store.find(job_id)
        if not located:
            return False
        _phase, job = located

        if by is not None:
            try:
                with self._lock:
                    held = self._held.get(job_id)
                    if held is not None:
                        labels = held.acted_by if isinstance(held.acted_by, dict) else {}
                        held.acted_by = {**labels, "killed": by}
            except Exception:
                log.warning("kill of %s: who asked could not be recorded", job_id)
        flag.set()
        try:
            backend = self._backend_for(job)
            handle = ExecutionHandle(backend_pid=job.backend_pid, connection_id=job.connection_id)
            backend.cancel(handle, job)
        except Exception:
            log.exception("cancel call failed for %s", job_id)
        return True

    def active_job_ids(self) -> list[str]:
        with self._lock:
            return list(self._active.keys())

    def explain(self, job_id: str) -> str:
        """Run EXPLAIN for a stored job's SQL against its target.

        Resolves the job through the store (any phase) and dispatches to the
        backend's own explain() — PG/SR accept a plain ``EXPLAIN <sql>``
        which plans but does not execute the query. Mongo uses
        ``db.command({explain: ..., verbosity: queryPlanner})`` and needs
        the job for database resolution.
        """
        located = self.store.find(job_id)
        if not located:
            raise KeyError(job_id)
        _phase, job = located
        backend = self._backend_for(job)
        # Mongo's explain accepts an optional job argument for db resolution;
        # PG/SR ignore it but accept the kwarg.
        try:
            return backend.explain(job.sql, job=job)
        except TypeError:
            return backend.explain(job.sql)

    def list_mongo_databases(self, target: str) -> list[dict]:
        """Enumerate databases on the Mongo node reached by the target."""
        b = self._backends.get(f"mongodb:{target}")
        if b is None:
            raise KeyError(f"no mongodb target named {target!r}")
        return b.list_databases()
