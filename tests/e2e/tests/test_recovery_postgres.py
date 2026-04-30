"""E2E: postgres_ingest recovers from an interrupted prior fast-load.

Bug class: orphaned no-PK table.
  Fast-load creates the table without PK, blind-COPYs all files, then runs
  dedup + ALTER TABLE ADD PRIMARY KEY. If the run crashes between blind
  COPY and ADD PRIMARY KEY (process kill, OOM, container down, network
  blip), the next run sees `tables_existed_before == True` and routes to
  the ON CONFLICT path, which fails with:

    InvalidColumnReference: there is no unique or exclusion constraint
    matching the ON CONFLICT specification

  The orchestrator now probes PK presence (not just table existence) and
  re-routes orphaned no-PK tables to fast-load, which finalizes the PK.

This test simulates the half-finalized state by ingesting once, dropping
the PK, wiping state-tracking, and re-running ingest — the second run
must succeed and re-add the PK.
"""

from tests.e2e.helpers.sdp import SDPSession, run_sdp, wait_for_healthy
from tests.e2e.helpers.fixtures import place_reddit_fixtures
from tests.e2e.helpers.db import (
    pg_connect,
    pg_drop_pk,
    pg_row_count,
    pg_table_has_pk,
)


PG_DB_SETUP = {
    "db_data_path": "",
    "db_databases": "1",
    "db_pgdata_path": "",
    "db_export_path": "",
    "db_name": "",
    "db_pg_port": "",
    "db_tablespaces": "",
    "db_filesystem": "1",
    "db_pgtune_method": "3",
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
    "src_file_format": "1",            # parquet
    "src_parquet_rg_size": "",
    "src_profiles": "1,4",             # parse + postgres_ingest
    "src_parse_workers": "2",
    "src_pg_prefer_lingua": "n",
    "src_pg_index_workers": "2",
    "src_write_files": "",
}


def test_postgres_ingest_recovers_from_orphaned_no_pk_table(workspace):
    """Re-run ingest after PK was lost mid-fast-load — must finish, must re-add PK."""
    rc, output = SDPSession(PG_DB_SETUP).run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"
    rc, output = SDPSession(PG_SOURCE_ADD).run_interactive("source add reddit")
    assert rc == 0, f"source add failed:\n{output}"

    place_reddit_fixtures("reddit", data_types=["comments"])

    result = run_sdp("db start postgres")
    assert result.returncode == 0, f"db start failed:\n{result.stderr}"
    wait_for_healthy("postgres")

    try:
        result = run_sdp("run parse --source reddit --build")
        assert result.returncode == 0, f"parse failed:\n{result.stderr}"

        result = run_sdp("run postgres_ingest --source reddit --build")
        assert result.returncode == 0, f"first ingest failed:\n{result.stderr}"

        # Sanity: first run finalized normally.
        conn = pg_connect()
        try:
            assert pg_table_has_pk(conn, "reddit", "comments"), \
                "first run should have left a PK on reddit.comments"
            first = pg_row_count(conn, "reddit", "comments")
            assert first == 10, f"first run: expected 10 rows, got {first}"
        finally:
            conn.close()

        # Simulate an interrupted prior fast-load: PK missing, data present.
        conn = pg_connect()
        try:
            pg_drop_pk(conn, "reddit", "comments")
            assert not pg_table_has_pk(conn, "reddit", "comments"), \
                "PK drop did not take effect"
        finally:
            conn.close()

        # Wipe state so the orchestrator re-detects fixture files as work.
        # Pre-fix behavior: tables_existed_before == True → standard ON CONFLICT
        # path → InvalidColumnReference. Post-fix: table_has_pk == False →
        # fast-load path → dedup + ADD PRIMARY KEY.
        state_dir = workspace / "data" / "database" / "postgres" / "state_tracking"
        for state_file in state_dir.glob("reddit_postgres_ingest_*.json"):
            state_file.unlink()

        result = run_sdp("run postgres_ingest --source reddit")
        assert result.returncode == 0, (
            f"recovery ingest failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )

        conn = pg_connect()
        try:
            assert pg_table_has_pk(conn, "reddit", "comments"), (
                "recovery should have re-added the PK on reddit.comments. "
                f"Recovery output:\n{result.stdout}"
            )
            after = pg_row_count(conn, "reddit", "comments")
            assert after == 10, (
                f"recovery row count drift: expected 10 (deduped), got {after}. "
                f"Recovery output:\n{result.stdout}"
            )
        finally:
            conn.close()
    finally:
        run_sdp("db stop postgres")
