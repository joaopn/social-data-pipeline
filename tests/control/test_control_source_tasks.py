"""Tests for the task `source.download` beside pipeline runs (`control.runs`, `control.tasks.SOURCE_FILE_TASKS`).

The contracts pinned here, each test naming the bug class it catches:

- A pipeline run of a source is refused while a download of that source is alive, and a download while a pipeline run of that source is alive (a terminal run whose source Docker did not hand over counts; with Docker out of reach the download is refused).
- A download started as a task records its source and its label and no secret: the token reaches the supervisor's standard input only.

The run store's listing is a stand-in; the record of a started task is the real store's, in a temp deployment.
"""

from __future__ import annotations

import json
import sys

import pytest

import sdp
from social_data_pipeline import runstore
from social_data_pipeline.control import errors, runs
from social_data_pipeline.setup import utils

PLANTED = "hf_plantedMadeUpToken91"

pytestmark = [pytest.mark.usefixtures("run_docker_guard"), pytest.mark.skipif(not sys.platform.startswith("linux"), reason="uses flock")]


def _listing(monkeypatch, runs_listed, docker=True):
    monkeypatch.setattr(runstore, "list_runs", lambda root, service_profiles, limit: {"docker_available": docker, "runs": runs_listed})


def _task(source, state="running", task="source.download"):
    return {"kind": runstore.KIND_TASK, "task": task, "profile": None, "source": source, "state": state}


def _run(source, profile="parse", kind=runstore.KIND_RECORDED, state="running"):
    return {"kind": kind, "profile": profile, "source": source, "state": state}


@pytest.mark.parametrize("listed, refused", [
    ([_task("bsky")], True),
    ([_task("bsky", state="detached")], True),
    ([_task("bsky", state="finished")], False),
    ([_task("bsky", state="lost")], False),
    ([_task("other")], False),
    ([_task("bsky", task="db.create_indexes")], False),
])
def test_a_pipeline_run_is_refused_while_a_download_of_its_source_is_alive(tmp_path, monkeypatch, listed, refused):
    """Bug class: `parse` (or any profile) reading a source's dumps while a download is still writing them, which parses half a dataset and records it as done; and a refusal that is too wide (another source, an ended download, a task that writes no source files)."""
    _listing(monkeypatch, listed)

    if not refused:
        runs._refuse_a_second_run(tmp_path, {}, "parse", "bsky")
        return
    with pytest.raises(errors.RunNotStarted) as exc:
        runs._refuse_a_second_run(tmp_path, {}, "parse", "bsky")
    assert exc.value.code == "source_task_active" and "download of this source" in exc.value.message


@pytest.mark.parametrize("listed, docker, code", [
    ([_run("bsky")], True, "source_run_active"),
    ([_run("bsky", profile="mongo_ingest", state="unknown")], True, "source_run_active"),
    ([_run(None, kind=runstore.KIND_TERMINAL)], True, "source_run_active"),
    ([_run("bsky", kind=runstore.KIND_TERMINAL)], True, "source_run_active"),
    ([_run("bsky", state="finished"), _run("other"), _run("other", kind=runstore.KIND_TERMINAL), _task("bsky")], True, None),
    ([], False, "docker_not_reachable"),
])
def test_a_download_is_refused_while_a_pipeline_run_of_its_source_is_alive(tmp_path, monkeypatch, listed, docker, code):
    """Bug class: a download rewriting the dumps and extracted files a run of the same source reads (also one started in a terminal, whose source Docker may not tell, or when Docker cannot be asked), and a refusal for runs of other sources or runs that ended. A second download is the task's own key refusal, not this one."""
    _listing(monkeypatch, listed, docker)

    if code is None:
        runs._refuse_beside_a_run_of_the_source(tmp_path, {}, "bsky")
        return
    with pytest.raises(errors.RunNotStarted) as exc:
        runs._refuse_beside_a_run_of_the_source(tmp_path, {}, "bsky")
    assert exc.value.code == code


def test_a_started_download_records_its_source_and_no_token(tmp_path, monkeypatch):
    """Bug class: the dataset token written into the run's record (its flags, its label) or anywhere under the run state folder, where `runs list` and the browser read it; or a download recorded without its source, so neither refusal above can see it."""
    root = tmp_path.resolve()
    sources = root / "config" / "sources"
    (sources / "bsky").mkdir(parents=True)
    (sources / "bsky" / "platform.yaml").write_text("hf_dataset: org/bsky\n")
    monkeypatch.setattr(utils, "SOURCES_DIR", sources)
    monkeypatch.setattr(sdp, "ROOT", root)
    monkeypatch.setattr(sdp, "_run_service_profiles", lambda: {})
    _listing(monkeypatch, [])
    sent = []
    monkeypatch.setattr(runs, "_spawn_supervisor", lambda root, run_id, lock_fd, request: sent.append(request))

    run_id = runs.start_task(root, "source.download", {"source": "bsky", "token": PLANTED}, started_by="web-7")

    entry = runstore.read_run(root, run_id)
    assert (entry["kind"], entry["task"], entry["label"], entry["source"], entry["flags"]) == (runstore.KIND_TASK, "source.download", "download · bsky", "bsky", {})
    stored = b"".join(path.read_bytes() for path in (root / ".sdp").rglob("*") if path.is_file())
    assert PLANTED.encode() not in stored
    assert json.loads(sent[0]) == {"verb": "source.download", "fields": {"source": "bsky", "token": PLANTED}}
