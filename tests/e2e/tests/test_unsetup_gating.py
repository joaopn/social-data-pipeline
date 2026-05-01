"""E2E: `db unsetup` cleans per-source DB overrides; `run` gates on configs.

Two coupled UX guarantees:
  1. After `db unsetup`, no DB config remains anywhere — including the
     per-source override files (e.g. `config/sources/<name>/postgres.yaml`).
     Non-DB overrides (parse.yaml, lingua.yaml, ml.yaml, platform.yaml)
     stay put.
  2. `sdp run <profile>` for a DB-bound profile must fail fast with a
     clear error if either the DB config or the per-source override is
     missing — instead of silently falling back to base pipeline.yaml
     defaults.

The unit tests cover the lookup-table shape; this E2E test exercises the
real CLI paths so we catch wiring regressions (e.g. someone adds a
profile without listing it in `_PROFILE_DB`).
"""

from tests.e2e.helpers.sdp import SDPSession, run_sdp, WORKSPACE


PG_DB_SETUP = {
    "db_data_path": "",
    "db_databases": "1",         # postgres only
    "db_pgdata_path": "",
    "db_name": "",
    "db_pg_port": "",
    "db_tablespaces": "",
    "db_filesystem": "1",
    "db_pgtune_method": "3",     # skip
    "db_pg_mem_limit": "0",
    "db_auth": "",
    "db_write_files": "",
}

PG_SOURCE_ADD = {
    "src_data_types": "",
    "src_dumps_path": "",
    "src_extracted_path": "",
    "src_parsed_path": "",
    "src_output_path": "",
    "src_file_format": "1",
    "src_parquet_rg_size": "",
    "src_profiles": "1,4",            # parse + postgres_ingest
    "src_parse_workers": "2",
    "src_pg_prefer_lingua": "n",
    "src_pg_index_workers": "2",
    "src_write_files": "",
}


def test_unsetup_wipes_source_overrides_and_run_gates(workspace):
    """Full unsetup → per-source DB overrides removed; run is gated."""
    # 1. db setup + source add — write configs without ever starting PG so
    #    no data dir exists and unsetup won't prompt for data deletion.
    rc, output = SDPSession(PG_DB_SETUP).run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"
    rc, output = SDPSession(PG_SOURCE_ADD).run_interactive("source add reddit")
    assert rc == 0, f"source add failed:\n{output}"

    src_dir = WORKSPACE / "config" / "sources" / "reddit"
    assert (WORKSPACE / "config" / "db" / "postgres.yaml").exists()
    assert (src_dir / "postgres.yaml").exists()
    assert (src_dir / "parse.yaml").exists()
    assert (src_dir / "platform.yaml").exists()

    # 2. db unsetup — `db setup` pre-creates the data dir, so the full
    #    unsetup will prompt to delete it. Decline (we only care about
    #    config-file cleanup here, not data-dir deletion).
    result = run_sdp("db unsetup", input_text="n\n")
    assert result.returncode == 0, (
        f"db unsetup failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert not (WORKSPACE / "config" / "db" / "postgres.yaml").exists(), \
        "db/postgres.yaml should be gone after unsetup"
    assert not (src_dir / "postgres.yaml").exists(), \
        "sources/reddit/postgres.yaml should be gone after unsetup"
    # postgres_ml.yaml was never created (profile not selected) — defensive.
    assert not (src_dir / "postgres_ml.yaml").exists()
    # Non-DB overrides preserved.
    assert (src_dir / "parse.yaml").exists(), \
        "sources/reddit/parse.yaml must survive unsetup"
    assert (src_dir / "platform.yaml").exists(), \
        "sources/reddit/platform.yaml must survive unsetup"

    # 3. `sdp run postgres_ingest` after unsetup must fail fast on the
    #    missing DB config — no docker-compose attempt, clear error.
    result = run_sdp("run postgres_ingest --source reddit")
    assert result.returncode == 1, (
        f"expected exit 1, got {result.returncode}\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    combined = result.stdout + result.stderr
    assert "no postgres database is configured" in combined, (
        f"missing expected DB-gating error in output:\n{combined}"
    )

    # 4. Re-add postgres at the DB level only — no `source add`.
    #    The DB-config gate now passes, but the source-override gate must
    #    catch the missing per-source postgres.yaml.
    rc, output = SDPSession(PG_DB_SETUP).run_interactive("db setup")
    assert rc == 0, f"second db setup failed:\n{output}"
    assert (WORKSPACE / "config" / "db" / "postgres.yaml").exists()
    assert not (src_dir / "postgres.yaml").exists()  # still gone

    result = run_sdp("run postgres_ingest --source reddit")
    assert result.returncode == 1, (
        f"expected exit 1, got {result.returncode}\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    combined = result.stdout + result.stderr
    assert "has no 'postgres_ingest' configuration" in combined, (
        f"missing expected source-gating error in output:\n{combined}"
    )
