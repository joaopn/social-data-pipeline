"""Filesystem-backed job store.

Layout (under <jobs_dir>):
  pending/<id>.json      — submitted, awaiting approval
  approved/<id>.json     — approved, not yet picked up
  running/<id>.json      — running; has execution metadata (pid / connection_id)
  history.jsonl          — append-only log of terminal transitions
  results/               — output files (folder per job)

Every transition is an atomic rename across sibling directories on the same
filesystem. There is never more than one writer per file per phase.
"""

from __future__ import annotations

import json
import os
import secrets
import tempfile
import threading
import time
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Any, Literal


Status = Literal[
    "pending",
    "approved",
    "running",
    "done",
    "failed",
    "rejected",
    "cancelled",
]


TERMINAL_STATUSES: frozenset[str] = frozenset({"done", "failed", "rejected", "cancelled"})


@dataclass
class Job:
    job_id: str
    target: str
    backend: str
    sql: str
    output_filename: str
    overwrite: bool
    submitted_at: float
    description: str = ""
    # Mongo jobs: collection + resolved database (from submit or target default).
    # For PG/SR these stay None. Mongo jobs store the pipeline (JSON array)
    # pretty-printed in `sql`.
    collection: str | None = None
    database: str | None = None
    status: Status = "pending"
    approved_at: float | None = None
    started_at: float | None = None
    finished_at: float | None = None
    rows: int | None = None
    size_bytes: int | None = None
    result_path: str | None = None
    error: str | None = None
    reject_reason: str | None = None
    backend_pid: int | None = None
    connection_id: int | None = None
    extras: dict[str, Any] = field(default_factory=dict)
    # Who approved or rejected the job, or asked to kill it, through the scheduler's control socket: at most the keys "approved", "rejected", "killed", each a label such as "web-12". "killed" names who asked to kill: it is set on a job that may then finish by itself, and it is lost when the scheduler restarts before the job is retired. Left out of the record while empty, so a record without labels is written byte for byte as before.
    acted_by: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        if not d["acted_by"]:
            del d["acted_by"]
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Job":
        known = {f for f in cls.__dataclass_fields__}
        kwargs = {k: v for k, v in d.items() if k in known}
        extras = {k: v for k, v in d.items() if k not in known}
        # Labels of another shape (a hand-edited record) are kept as they were, but not as labels: the transitions add to them.
        if "acted_by" in kwargs and not _is_acted_by(kwargs["acted_by"]):
            extras["acted_by"] = kwargs.pop("acted_by")
        if extras:
            kwargs.setdefault("extras", {}).update(extras)
        return cls(**kwargs)


ACTED_BY_KEYS: frozenset[str] = frozenset({"approved", "rejected", "killed"})


def _is_acted_by(value: Any) -> bool:
    """Whether a record's `acted_by` has the shape the scheduler writes: a dict of at most the three known keys, each with a text value."""
    return isinstance(value, dict) and set(value) <= ACTED_BY_KEYS and all(isinstance(label, str) for label in value.values())


class Store:
    """Filesystem-backed job store. Thread-safe via file-level atomicity."""

    def __init__(self, jobs_dir: Path):
        self.root = Path(jobs_dir)
        self.pending = self.root / "pending"
        self.approved = self.root / "approved"
        self.running = self.root / "running"
        self.results = self.root / "results"
        self.history = self.root / "history.jsonl"
        self._history_lock = threading.Lock()
        for d in (self.pending, self.approved, self.running, self.results):
            d.mkdir(parents=True, exist_ok=True)
        self.history.touch(exist_ok=True)

    # ------------------------------------------------------------------
    # id + job creation

    @staticmethod
    def new_job_id(backend: str = "job") -> str:
        """Generate a job id prefixed by the backend short name.

        Backend → prefix mapping: postgres→pg, starrocks→sr, mongodb→mg.
        Unknown backends fall back to "job". Prefix makes job ids
        recognizable at a glance in the UI and in result paths.
        """
        prefix = {"postgres": "pg", "starrocks": "sr", "mongodb": "mg"}.get(backend, "job")
        return f"{prefix}_{secrets.token_hex(4)}"

    def submit(self, job: Job) -> None:
        self._write_json(self.pending / f"{job.job_id}.json", job.to_dict())

    # ------------------------------------------------------------------
    # transitions

    def approve(self, job_id: str, *, by: str | None = None) -> Job:
        job = self._require(self.pending, job_id)
        if by is not None:
            job.acted_by = {**job.acted_by, "approved": by}
        job.status = "approved"
        job.approved_at = time.time()
        self._move(self.pending, self.approved, job)
        return job

    def reject(self, job_id: str, reason: str | None = None, *, by: str | None = None) -> Job:
        job = self._require(self.pending, job_id)
        if by is not None:
            job.acted_by = {**job.acted_by, "rejected": by}
        job.status = "rejected"
        job.reject_reason = reason
        job.finished_at = time.time()
        self._retire(self.pending, job)
        return job

    def cancel_pending(self, job_id: str) -> Job:
        """Agent-initiated cancel of a pending job (pre-approval)."""
        job = self._require(self.pending, job_id)
        job.status = "cancelled"
        job.finished_at = time.time()
        self._retire(self.pending, job)
        return job

    def claim_approved(self) -> Job | None:
        """Pop the oldest approved job, move to running/. Returns None if empty."""
        files = sorted(self.approved.glob("*.json"), key=lambda p: p.stat().st_mtime)
        for p in files:
            try:
                job = Job.from_dict(json.loads(p.read_text()))
            except (FileNotFoundError, json.JSONDecodeError):
                continue
            job.status = "running"
            job.started_at = time.time()
            dst = self.running / p.name
            # Write updated body, then rename from approved → running atomically.
            self._write_json(p, job.to_dict())
            try:
                os.rename(p, dst)
            except FileNotFoundError:
                continue
            return job
        return None

    def update_running(self, job: Job) -> None:
        """Persist an update for a running job (e.g., backend_pid after start)."""
        self._write_json(self.running / f"{job.job_id}.json", job.to_dict())

    def complete(self, job: Job) -> None:
        job.status = "done"
        job.finished_at = time.time()
        self._retire(self.running, job)

    def fail(self, job: Job, error: str) -> None:
        job.status = "failed"
        job.error = error
        job.finished_at = time.time()
        self._retire(self.running, job)

    def mark_cancelled(self, job: Job, reason: str = "cancelled by user") -> None:
        job.status = "cancelled"
        job.error = reason
        job.finished_at = time.time()
        self._retire(self.running, job)

    # ------------------------------------------------------------------
    # lookup

    def find(self, job_id: str) -> tuple[str, Job] | None:
        """Locate a job across pending/approved/running + history. Returns (phase, job)."""
        for phase, d in (("pending", self.pending), ("approved", self.approved), ("running", self.running)):
            p = d / f"{job_id}.json"
            if p.exists():
                try:
                    return phase, Job.from_dict(json.loads(p.read_text()))
                except (FileNotFoundError, json.JSONDecodeError):
                    continue
        for j in self.iter_history():
            if j.job_id == job_id:
                return "history", j
        return None

    def list_phase(self, phase: Literal["pending", "approved", "running"]) -> list[Job]:
        d = {"pending": self.pending, "approved": self.approved, "running": self.running}[phase]
        out = []
        for p in sorted(d.glob("*.json"), key=lambda p: p.stat().st_mtime):
            try:
                out.append(Job.from_dict(json.loads(p.read_text())))
            except (FileNotFoundError, json.JSONDecodeError):
                continue
        return out

    def iter_history(self, limit: int | None = None) -> list[Job]:
        """Most recent first."""
        if not self.history.exists():
            return []
        with open(self.history, "r") as f:
            lines = f.readlines()
        out: list[Job] = []
        for line in reversed(lines):
            line = line.strip()
            if not line:
                continue
            try:
                out.append(Job.from_dict(json.loads(line)))
            except json.JSONDecodeError:
                continue
            if limit is not None and len(out) >= limit:
                break
        return out

    def orphaned_running(self) -> list[Job]:
        """Jobs in running/ with no live process — restart cleanup target."""
        return self.list_phase("running")

    # ------------------------------------------------------------------
    # result folders

    def job_result_dir(self, job_id: str) -> Path:
        return self.results / job_id

    # ------------------------------------------------------------------
    # internals

    def _require(self, phase_dir: Path, job_id: str) -> Job:
        p = phase_dir / f"{job_id}.json"
        if not p.exists():
            raise KeyError(f"job {job_id} not in {phase_dir.name}")
        return Job.from_dict(json.loads(p.read_text()))

    def _move(self, src_dir: Path, dst_dir: Path, job: Job) -> None:
        src = src_dir / f"{job.job_id}.json"
        dst = dst_dir / f"{job.job_id}.json"
        self._write_json(src, job.to_dict())
        os.rename(src, dst)

    def _retire(self, src_dir: Path, job: Job) -> None:
        src = src_dir / f"{job.job_id}.json"
        self._append_history(job)
        if src.exists():
            try:
                os.unlink(src)
            except FileNotFoundError:
                pass

    def _append_history(self, job: Job) -> None:
        line = json.dumps(job.to_dict(), separators=(",", ":")) + "\n"
        with self._history_lock, open(self.history, "a") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())

    def _write_json(self, path: Path, data: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(
            prefix=path.name + ".", suffix=".tmp", dir=str(path.parent)
        )
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(data, f, separators=(",", ":"))
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
        except Exception:
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass
            raise
