"""Tests for the `db start` post-launch ps probe.

`cmd_db_start` runs `docker compose up -d --wait` for MCP services and
then probes `docker compose ps --all --format json`. Any service in the
`Exited` state is reported with an actionable error block, and the
overall exit code becomes non-zero. This catches the failure shape
where `--wait` returns 0 on a container that crashed during startup.

The integration with `cmd_db_start` is a thin call to the helper, so
the tests focus on the helper itself: parsing both newline-delimited
and JSON-array `ps` output, picking out exited services, and the
human-facing message format.
"""

from __future__ import annotations

import json
import subprocess

from sdp import _exited_services_after_up, _print_exited_services


def _fake_run(stdout="", stderr="", returncode=0):
    """Build a CompletedProcess result for monkeypatching subprocess.run."""
    return subprocess.CompletedProcess(
        args=[], returncode=returncode, stdout=stdout, stderr=stderr,
    )


def test_no_exited_when_all_running(monkeypatch):
    """All services running → helper returns empty list."""
    out = "\n".join([
        json.dumps({"Service": "postgres", "State": "running"}),
        json.dumps({"Service": "postgres-mcp", "State": "running"}),
    ])
    monkeypatch.setattr(
        subprocess, "run",
        lambda *a, **kw: _fake_run(stdout=out),
    )

    exited = _exited_services_after_up(
        ["--profile", "postgres", "--profile", "postgres_mcp"],
        {"postgres", "postgres-mcp"},
    )

    assert exited == []


def test_one_exited_service_returned(monkeypatch):
    """A single Exited service shows up in the helper's return."""
    out = "\n".join([
        json.dumps({"Service": "postgres", "State": "running"}),
        json.dumps({"Service": "postgres-mcp", "State": "exited", "ExitCode": 1}),
    ])
    monkeypatch.setattr(
        subprocess, "run",
        lambda *a, **kw: _fake_run(stdout=out),
    )

    exited = _exited_services_after_up(
        ["--profile", "postgres", "--profile", "postgres_mcp"],
        {"postgres", "postgres-mcp"},
    )

    assert exited == [("postgres-mcp", 1)]


def test_out_of_profile_exited_service_ignored(monkeypatch):
    """A stale Exited container from a profile we did NOT start is ignored.

    Regression: `ps --all` lists every project container regardless of the
    active profiles. A `jobs` container left Exited (255) by an earlier
    crash must not be reported as a failure of a `mongo`+`starrocks` start,
    and (in cmd_db_start) must not abort the run before jobs itself starts.
    """
    out = "\n".join([
        json.dumps({"Service": "jobs", "State": "exited", "ExitCode": 255}),
        json.dumps({"Service": "mongo", "State": "running"}),
        json.dumps({"Service": "mongo-mcp", "State": "running"}),
        json.dumps({"Service": "starrocks", "State": "running"}),
        json.dumps({"Service": "starrocks-mcp", "State": "running"}),
    ])
    monkeypatch.setattr(
        subprocess, "run",
        lambda *a, **kw: _fake_run(stdout=out),
    )

    exited = _exited_services_after_up(
        ["--profile", "mongo", "--profile", "starrocks",
         "--profile", "mongo_mcp", "--profile", "starrocks_mcp"],
        {"mongo", "starrocks", "mongo-mcp", "starrocks-mcp"},
    )

    assert exited == []


def test_multiple_exited_services(monkeypatch):
    """Multiple Exited services are all listed in order."""
    out = "\n".join([
        json.dumps({"Service": "postgres", "State": "running"}),
        json.dumps({"Service": "mongo-mcp", "State": "exited", "ExitCode": 137}),
        json.dumps({"Service": "starrocks-mcp", "State": "exited", "ExitCode": 1}),
    ])
    monkeypatch.setattr(
        subprocess, "run",
        lambda *a, **kw: _fake_run(stdout=out),
    )

    exited = _exited_services_after_up(
        ["--profile", "starrocks_mcp", "--profile", "mongo_mcp"],
        {"mongo-mcp", "starrocks-mcp"},
    )

    assert exited == [("mongo-mcp", 137), ("starrocks-mcp", 1)]


def test_handles_json_array_format(monkeypatch):
    """Older `docker compose ps` emits a single JSON array; helper handles both."""
    out = json.dumps([
        {"Service": "postgres", "State": "running"},
        {"Service": "postgres-mcp", "State": "exited", "ExitCode": 2},
    ])
    monkeypatch.setattr(
        subprocess, "run",
        lambda *a, **kw: _fake_run(stdout=out),
    )

    exited = _exited_services_after_up(
        ["--profile", "postgres_mcp"], {"postgres", "postgres-mcp"},
    )

    assert exited == [("postgres-mcp", 2)]


def test_empty_stdout_returns_empty_list(monkeypatch):
    """`compose ps` returning nothing (no services) is treated as no exits."""
    monkeypatch.setattr(
        subprocess, "run",
        lambda *a, **kw: _fake_run(stdout=""),
    )

    assert _exited_services_after_up(["--profile", "postgres_mcp"], {"postgres-mcp"}) == []


def test_malformed_json_lines_skipped(monkeypatch):
    """Garbage lines in `compose ps` output don't crash the helper.

    The probe should be defensive: a single bad line (e.g., docker logs
    leak, deprecation warning) shouldn't cause us to miss real Exited
    services on adjacent lines or to blow up the whole `db start`.
    """
    out = "\n".join([
        "WARNING: foo",
        json.dumps({"Service": "postgres-mcp", "State": "exited", "ExitCode": 1}),
        "another garbage line",
    ])
    monkeypatch.setattr(
        subprocess, "run",
        lambda *a, **kw: _fake_run(stdout=out),
    )

    exited = _exited_services_after_up(
        ["--profile", "postgres_mcp"], {"postgres", "postgres-mcp"},
    )

    assert exited == [("postgres-mcp", 1)]


def test_ps_failure_returns_empty_list(monkeypatch):
    """If the probe's own `compose ps` fails, the helper returns no exits.

    Pins the design choice: a transient ps failure must not mask a
    successful `up -d --wait`. The caller's --wait result speaks if ps
    can't run.
    """
    monkeypatch.setattr(
        subprocess, "run",
        lambda *a, **kw: _fake_run(returncode=1),
    )

    assert _exited_services_after_up(["--profile", "postgres"], {"postgres"}) == []


def test_falls_back_to_name_when_service_missing(monkeypatch):
    """Older docker output may use `Name` instead of `Service`; helper accepts both.

    A `Name`-only row can't be scoped to `expected_services`, so it falls
    through to inclusive detection rather than being dropped — better to
    over-report on old docker than to miss a real crash.
    """
    out = json.dumps({"Name": "sdp-postgres-mcp-1", "State": "exited", "ExitCode": 1})
    monkeypatch.setattr(
        subprocess, "run",
        lambda *a, **kw: _fake_run(stdout=out),
    )

    exited = _exited_services_after_up(["--profile", "postgres_mcp"], {"postgres-mcp"})

    assert exited == [("sdp-postgres-mcp-1", 1)]


def test_print_exited_services_message_format(capsys):
    """Renders `[ERROR] '<svc>' exited (code=N). See: docker compose logs <svc>`."""
    _print_exited_services([("postgres-mcp", 1), ("mongo-mcp", None)])

    out = capsys.readouterr().out
    assert "[ERROR]" in out
    assert "'postgres-mcp' exited (code=1)" in out
    assert "'mongo-mcp' exited" in out  # No code suffix when ExitCode is None.
    assert "(code=" not in out.split("'mongo-mcp'")[1].split("\n", 1)[0]
    assert "docker compose logs postgres-mcp" in out
    assert "docker compose logs mongo-mcp" in out
