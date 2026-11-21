"""Regression tests for `sdp db status` / `sdp source status` display logic.

These pin nine fixes from a live-deployment audit:

- B1: `source status <name>` must not fall back to a source-blind glob and
  leak other sources' state when the named source has no state file yet.
- B2: `db status` defaults to a short summary; `--verbose` opts into the
  per-source ingestion breakdown.
- I3: `RO user:` line iterates per configured DB instead of collapsing to the
  first cred file found.
- I4: `MCP user:` line iterates per configured DB instead of `or`-chaining
  across env vars.
- I5: ingestion section header uses the canonical display name table
  (StarRocks, StarRocks ML, MongoDB) instead of `db_type.title()` which
  produces "Sr_Ingest" and "Mongodb".
- I6: `db status --verbose` includes the StarRocks ingestion section
  (previously absent).
- I7 + I8: `Read-only:` line uses `yes` / `no` (not the Python bool repr)
  on all three MCP backends with consistent parenthetical hints.
- I9: jobs `Targets:` line displays backends via the same lookup table.

Bug class: "code collapses N things to the first" — a class of silent-drift
bugs where multi-DB deployments look fine on the screen but the operator
has no way to see per-DB differences.
"""

from __future__ import annotations

import json
import types
from pathlib import Path

import pytest

import sdp


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _args(**fields):
    """Build a fake argparse.Namespace for cmd_db_status calls."""
    ns = types.SimpleNamespace(**fields)
    return ns


def _write_state(state_dir: Path, name: str, processed: list[str], **extra):
    """Drop a JSON state file matching the pipeline's on-disk format."""
    state_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "processed": processed,
        "failed": [],
        "in_progress": None,
        "last_updated": "",
    }
    payload.update(extra)
    (state_dir / f"{name}.json").write_text(json.dumps(payload))


def _patch_db_status_world(
    monkeypatch,
    *,
    env: dict,
    configured: list[str],
    mcp_config: dict | None = None,
    jobs_configured: bool = False,
    jobs_config: dict | None = None,
    running_services: list[str] | None = None,
):
    """Patch the live IO surfaces `cmd_db_status` reaches into.

    Each surface is mocked separately so the tests stay legible without a
    god-fixture; tests opt in to only the surfaces they need.
    """
    monkeypatch.setattr(sdp, "load_env", lambda: dict(env))
    monkeypatch.setattr(sdp, "_get_configured_db_services", lambda: list(configured))
    monkeypatch.setattr(sdp, "_load_mcp_config", lambda: dict(mcp_config or {}))
    monkeypatch.setattr(sdp, "_is_jobs_configured", lambda: jobs_configured)
    monkeypatch.setattr(sdp, "_load_jobs_config", lambda: dict(jobs_config or {}))

    # docker compose ps — return empty so no services show as running
    # unless the test sets running_services.
    svc_set = set(running_services or [])
    lines = "\n".join(json.dumps({"Service": s, "State": "running"}) for s in svc_set)

    class _Result:
        def __init__(self, stdout):
            self.returncode = 0
            self.stdout = stdout

    def fake_run(*args, **kwargs):
        return _Result(lines)

    monkeypatch.setattr(sdp.subprocess, "run", fake_run)


# ---------------------------------------------------------------------------
# B1 — source status state-file leak
# ---------------------------------------------------------------------------


class TestSourceStatusLeak:
    def test_does_not_leak_other_sources(self, tmp_path, capsys):
        """`reddit` source must not display `twitter` state files just because
        its own state dir is empty for the requested db_type."""
        state_dir = tmp_path / "state_tracking"
        _write_state(state_dir, "twitter_postgres_ingest_tweets", ["TW_2024"])
        _write_state(state_dir, "twitter_postgres_ingest_users", ["TW_USR"])

        sdp._print_source_ingestion_state(state_dir, "reddit", "postgres")

        out = capsys.readouterr().out
        assert "twitter" not in out, f"foreign-source state leaked: {out!r}"
        assert "TW_2024" not in out
        assert "no data yet for source 'reddit'" in out

    def test_finds_own_source_state(self, tmp_path, capsys):
        state_dir = tmp_path / "state_tracking"
        _write_state(state_dir, "reddit_postgres_ingest_comments", ["RC_2024"])
        _write_state(state_dir, "twitter_postgres_ingest_tweets", ["TW_2024"])

        sdp._print_source_ingestion_state(state_dir, "reddit", "postgres")

        out = capsys.readouterr().out
        assert "RC_2024" in out
        assert "TW_2024" not in out
        assert "twitter" not in out

    def test_missing_state_dir_prints_no_data_line(self, tmp_path, capsys):
        # state_dir doesn't exist at all
        sdp._print_source_ingestion_state(tmp_path / "missing", "reddit", "mongo")

        out = capsys.readouterr().out
        # Per I5: section header uses display name, not `.title()`.
        assert "MongoDB ingestion:" in out
        assert "no data yet for source 'reddit'" in out


# ---------------------------------------------------------------------------
# I5 — ingestion label uses _DB_DISPLAY
# ---------------------------------------------------------------------------


class TestIngestionLabels:
    @pytest.mark.parametrize("db_type,expected", [
        ("sr_ingest", "StarRocks ingestion:"),
        ("sr_ml",     "StarRocks ML ingestion:"),
        ("mongo",     "MongoDB ingestion:"),
        ("postgres",  "PostgreSQL ingestion:"),
    ])
    def test_section_header_uses_display_name(self, tmp_path, capsys, db_type, expected):
        state_dir = tmp_path / "state_tracking"
        _write_state(state_dir, f"reddit_{db_type}_dt", ["RC"])

        sdp._print_source_ingestion_state(state_dir, "reddit", db_type)

        out = capsys.readouterr().out
        assert expected in out
        # Negative assertion: the .title() outputs that this fix replaced.
        for bad in ("Sr_Ingest", "Sr_Ml", "Mongo ingestion:"):
            assert bad not in out, f"old .title() output leaked: {bad}"


# ---------------------------------------------------------------------------
# B2 / I6 — db status default vs --verbose; SR ingestion section included
# ---------------------------------------------------------------------------


class TestDbStatusVerbose:
    @pytest.fixture
    def workspace(self, tmp_path, monkeypatch):
        """Tmp data paths with state-tracking dirs containing one entry each
        so the teaser line / verbose breakdown both have something to show."""
        pg = tmp_path / "pg"
        mongo = tmp_path / "mongo"
        sr = tmp_path / "sr"
        _write_state(pg / "state_tracking", "reddit_postgres_ingest_comments", ["RC"])
        _write_state(mongo / "state_tracking", "reddit_mongo_ingest_comments", ["RC"])
        _write_state(sr / "state_tracking", "reddit_sr_ingest_comments", ["RC"])

        env = {
            "PGDATA_PATH": str(pg),
            "MONGO_DATA_PATH": str(mongo),
            "STARROCKS_DATA_PATH": str(sr),
        }
        _patch_db_status_world(
            monkeypatch,
            env=env,
            configured=["postgres", "mongo", "starrocks"],
        )
        return tmp_path

    def test_default_omits_ingestion_breakdown(self, workspace, capsys):
        sdp.cmd_db_status(_args(verbose=False))
        out = capsys.readouterr().out
        assert "Ingestion status:" not in out
        # B2 contract: surface the toggle hint when there *is* hidden data.
        assert out.count("run with --verbose") >= 1

    def test_verbose_shows_breakdown_for_all_configured_dbs(self, workspace, capsys):
        sdp.cmd_db_status(_args(verbose=True))
        out = capsys.readouterr().out
        # Three blocks (one per DB) and the teaser line is gone.
        assert out.count("Ingestion status:") == 3
        assert "run with --verbose" not in out

    def test_verbose_includes_starrocks_section(self, workspace, capsys):
        """Regression for I6: SR was missing from cmd_db_status entirely."""
        sdp.cmd_db_status(_args(verbose=True))
        out = capsys.readouterr().out
        # SR rendering goes through _get_ingestion_state and _print_ingestion with db_type="sr"
        # → table label is "<source>.<data_type>".
        assert "reddit.comments" in out

    def test_no_teaser_when_state_dir_empty(self, tmp_path, monkeypatch, capsys):
        """The teaser only prints when there's data hidden behind --verbose."""
        env = {
            "PGDATA_PATH": str(tmp_path / "pg_empty"),
        }
        _patch_db_status_world(
            monkeypatch,
            env=env,
            configured=["postgres"],
        )
        sdp.cmd_db_status(_args(verbose=False))
        out = capsys.readouterr().out
        assert "run with --verbose" not in out


# ---------------------------------------------------------------------------
# I3 — RO credentials per-DB
# ---------------------------------------------------------------------------


class TestRoCredentialsPerDb:
    def test_lists_each_configured_db_with_cred_file(self, tmp_path, monkeypatch, capsys):
        """Mongo + StarRocks both have .ro_credentials; both must surface."""
        mongo = tmp_path / "mongo"
        sr = tmp_path / "sr"
        (mongo / "state_tracking").mkdir(parents=True)
        (sr / "state_tracking").mkdir(parents=True)
        (mongo / ".ro_credentials").write_text("mongo-pw\n")
        (sr / ".ro_credentials").write_text("sr-pw\n")

        env = {
            "MONGO_AUTH_ENABLED": "true",
            "STARROCKS_AUTH_ENABLED": "true",
            "MONGO_RO_USER": "readonly",
            "STARROCKS_RO_USER": "readonly",
            "MONGO_DATA_PATH": str(mongo),
            "STARROCKS_DATA_PATH": str(sr),
        }
        _patch_db_status_world(
            monkeypatch,
            env=env,
            configured=["mongo", "starrocks"],
        )

        sdp.cmd_db_status(_args(verbose=False))
        out = capsys.readouterr().out

        assert str(mongo / ".ro_credentials") in out
        assert str(sr / ".ro_credentials") in out
        # Indented multi-DB form, keyed by display name.
        assert "MongoDB:" in out
        assert "StarRocks:" in out

    def test_single_db_one_liner(self, tmp_path, monkeypatch, capsys):
        pg = tmp_path / "pg"
        (pg / "state_tracking").mkdir(parents=True)
        (pg / ".ro_credentials").write_text("pg-pw\n")

        env = {
            "POSTGRES_AUTH_ENABLED": "true",
            "POSTGRES_RO_USER": "readonly",
            "PGDATA_PATH": str(pg),
        }
        _patch_db_status_world(
            monkeypatch,
            env=env,
            configured=["postgres"],
        )

        sdp.cmd_db_status(_args(verbose=False))
        out = capsys.readouterr().out

        # Inline form: `RO user:       readonly (password in <path>)`.
        assert f"RO user:       readonly (password in {pg / '.ro_credentials'})" in out

    def test_auth_disabled_db_excluded(self, tmp_path, monkeypatch, capsys):
        """A DB present in `configured` but with auth disabled must not get a
        line, even if a stale .ro_credentials file happens to exist.

        Asserted against the `RO user:` line specifically — the drift
        subsection (sourced from the verify module reading real
        config/db/*.yaml) can independently surface a cred path for unrelated
        reasons; that's outside the scope of this fix.
        """
        pg = tmp_path / "pg"
        mongo = tmp_path / "mongo"
        (pg / "state_tracking").mkdir(parents=True)
        (mongo / "state_tracking").mkdir(parents=True)
        (pg / ".ro_credentials").write_text("pg-pw\n")
        (mongo / ".ro_credentials").write_text("stale\n")

        env = {
            "POSTGRES_AUTH_ENABLED": "true",
            # MONGO_AUTH_ENABLED deliberately absent → mongo cred must not surface
            # in the RO user iteration even though the file is on disk.
            "POSTGRES_RO_USER": "readonly",
            "MONGO_DATA_PATH": str(mongo),
            "PGDATA_PATH": str(pg),
        }
        _patch_db_status_world(
            monkeypatch,
            env=env,
            configured=["postgres", "mongo"],
        )

        sdp.cmd_db_status(_args(verbose=False))
        out = capsys.readouterr().out

        # Scope the assertion to the `RO user:` line(s) the I3 fix owns.
        ro_lines = [
            line for line in out.splitlines()
            if "RO user:" in line or line.lstrip().startswith(("MongoDB:", "PostgreSQL:", "StarRocks:"))
        ]
        ro_block = "\n".join(ro_lines)
        # PG's cred path is correctly listed (single-DB inline form).
        assert str(pg / ".ro_credentials") in ro_block
        # Mongo's cred path must NOT appear in the RO user block.
        assert str(mongo / ".ro_credentials") not in ro_block


# ---------------------------------------------------------------------------
# I4 — MCP user per-DB
# ---------------------------------------------------------------------------


class TestMcpUserPerDb:
    """MCP user iteration lives inside `if auth_enabled:` (matching the
    pre-fix location). Tests enable auth on at least one DB to trigger it."""

    def test_multi_db_lists_per_backend(self, tmp_path, monkeypatch, capsys):
        env = {
            "POSTGRES_AUTH_ENABLED": "true",
            "MONGO_AUTH_ENABLED": "true",
            "STARROCKS_AUTH_ENABLED": "true",
            "POSTGRES_MCP_USER": "pg_reader",
            "MONGO_MCP_USER":    "mongo_reader",
            "STARROCKS_MCP_USER": "sr_reader",
        }
        _patch_db_status_world(
            monkeypatch,
            env=env,
            configured=["postgres", "mongo", "starrocks"],
        )

        sdp.cmd_db_status(_args(verbose=False))
        out = capsys.readouterr().out

        assert "pg_reader" in out
        assert "mongo_reader" in out
        assert "sr_reader" in out
        # Header "MCP user:" with no inline value when multiple DBs.
        assert "MCP user:\n" in out
        # Display names used to label each backend line. Names must appear in
        # the MCP-user indented block specifically.
        mcp_header_idx = out.index("MCP user:\n")
        mcp_block = out[mcp_header_idx:mcp_header_idx + 200]
        assert "PostgreSQL:" in mcp_block
        assert "MongoDB:" in mcp_block
        assert "StarRocks:" in mcp_block

    def test_single_db_one_liner(self, tmp_path, monkeypatch, capsys):
        env = {
            "POSTGRES_AUTH_ENABLED": "true",
            "POSTGRES_MCP_USER": "pg_reader",
        }
        _patch_db_status_world(
            monkeypatch,
            env=env,
            configured=["postgres"],
        )

        sdp.cmd_db_status(_args(verbose=False))
        out = capsys.readouterr().out

        assert "MCP user:      pg_reader" in out

    def test_no_mcp_user_omits_line(self, tmp_path, monkeypatch, capsys):
        """Auth enabled but no MCP_USER env var → no MCP user line at all."""
        env = {
            "POSTGRES_AUTH_ENABLED": "true",
        }
        _patch_db_status_world(
            monkeypatch,
            env=env,
            configured=["postgres"],
        )
        sdp.cmd_db_status(_args(verbose=False))
        out = capsys.readouterr().out
        assert "MCP user" not in out


# ---------------------------------------------------------------------------
# I7 + I8 — MCP Read-only line consistent across backends
# ---------------------------------------------------------------------------


class TestMcpReadOnlyDisplay:
    def test_all_three_backends_show_read_only_line(self, tmp_path, monkeypatch, capsys):
        env = {
            "STARROCKS_AUTH_ENABLED": "true",
            "STARROCKS_RO_USER": "readonly",
        }
        mcp_config = {
            "postgres":  {"enabled": True, "port": 8000, "access_mode": "restricted"},
            "mongo":     {"enabled": True, "port": 3000, "read_only": True},
            "starrocks": {"enabled": True, "port": 9000},
        }
        _patch_db_status_world(
            monkeypatch,
            env=env,
            configured=["postgres", "mongo", "starrocks"],
            mcp_config=mcp_config,
        )

        sdp.cmd_db_status(_args(verbose=False))
        out = capsys.readouterr().out

        # No bool repr leak.
        assert "Read-only: True" not in out
        assert "Read-only: False" not in out
        # PG: restricted access, parenthetical hint.
        assert "Read-only: yes (access-mode restricted)" in out
        # Mongo: filtered tool list, parenthetical hint.
        assert "Read-only: yes (filtered tool list)" in out
        # SR: database-level only, parenthetical hint.
        assert "Read-only: yes (database-level via sdp_readonly role)" in out
        # Three Read-only lines total.
        assert out.count("Read-only:") == 3

    def test_pg_unrestricted_renders_no(self, tmp_path, monkeypatch, capsys):
        mcp_config = {
            "postgres": {"enabled": True, "port": 8000, "access_mode": "unrestricted"},
        }
        _patch_db_status_world(
            monkeypatch,
            env={},
            configured=["postgres"],
            mcp_config=mcp_config,
        )
        sdp.cmd_db_status(_args(verbose=False))
        out = capsys.readouterr().out
        assert "Read-only: no (access-mode unrestricted)" in out

    def test_mongo_read_only_off_renders_no(self, tmp_path, monkeypatch, capsys):
        mcp_config = {
            "mongo": {"enabled": True, "port": 3000, "read_only": False},
        }
        _patch_db_status_world(
            monkeypatch,
            env={},
            configured=["mongo"],
            mcp_config=mcp_config,
        )
        sdp.cmd_db_status(_args(verbose=False))
        out = capsys.readouterr().out
        assert "Read-only: no" in out
        # No filtered-tool-list hint when not read-only.
        assert "Read-only: no (filtered tool list)" not in out


# ---------------------------------------------------------------------------
# I9 — jobs Targets uses display names
# ---------------------------------------------------------------------------


class TestJobsTargetsCasing:
    def test_targets_display_names(self, tmp_path, monkeypatch, capsys):
        jobs_cfg = {
            "port": 8050,
            "result_root": "./data/jobs/results",
            "max_concurrent": 1,
            "targets": {
                "sr_main":    {"backend": "starrocks"},
                "mongo_main": {"backend": "mongodb"},
                "pg_main":    {"backend": "postgres"},
            },
        }
        _patch_db_status_world(
            monkeypatch,
            env={},
            configured=["postgres", "mongo", "starrocks"],
            jobs_configured=True,
            jobs_config=jobs_cfg,
        )
        sdp.cmd_db_status(_args(verbose=False))
        out = capsys.readouterr().out

        # Targets line uses display-cased names.
        assert "sr_main (StarRocks)" in out
        assert "mongo_main (MongoDB)" in out
        assert "pg_main (PostgreSQL)" in out
        # Negative: no lowercase backend strings on the Targets line.
        targets_line = next(
            (line for line in out.splitlines() if line.lstrip().startswith("Targets:")),
            "",
        )
        assert "(starrocks)" not in targets_line
        assert "(mongodb)" not in targets_line
        assert "(postgres)" not in targets_line
