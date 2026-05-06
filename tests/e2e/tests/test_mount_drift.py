"""E2E: source-add ↔ DB-server mount lifecycle.

Two contracts are pinned here:

1. **In-parent source add doesn't need restart.** With the dual-mount
   design (docker-compose.yml binds ``${PARSED_PATH}`` / ``${OUTPUT_PATH}``
   on the postgres + starrocks server blocks), a source whose paths fall
   under those parents is visible to a running DB the moment its
   directories appear on disk. ``source add`` on a default-path source
   does not warn, and ``run postgres_ingest`` succeeds without a
   restart.
2. **Drift detection still backstops the multi-disk case.** Sources
   whose host paths live outside ``${PARSED_PATH}`` / ``${OUTPUT_PATH}``
   need explicit per-source override entries; that path is exercised by
   the unit tests in ``tests/setup/test_mount_sync.py`` (live ``docker
   inspect`` mocking + filtering helpers). Pinning the multi-disk path
   in E2E would require staging an out-of-parent directory inside the
   sysbox workspace and is not worth the complexity for what unit tests
   already cover deterministically.

This test exercises the postgres path. The starrocks path is structurally
identical (same dual-mount pattern in docker-compose.yml) and is covered
indirectly through the existing ``test_sr_flow.py`` E2E.
"""

from tests.e2e.helpers.sdp import SDPSession, run_sdp, wait_for_healthy
from tests.e2e.helpers.fixtures import place_reddit_fixtures
from tests.e2e.helpers.db import pg_connect, pg_table_exists, pg_row_count


PG_DB_SETUP = {
    "db_data_path": "",
    "db_databases": "1",         # postgres only
    "db_pgdata_path": "",
    "db_export_path": "",
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
    "src_parsed_path": "",        # default → ./data/parsed/reddit (in-parent)
    "src_output_path": "",        # default → ./data/output/reddit (in-parent)
    "src_file_format": "1",       # parquet
    "src_parquet_rg_size": "",
    "src_profiles": "1,4",        # parse + postgres_ingest
    "src_parse_workers": "2",
    "src_pg_prefer_lingua": "n",
    "src_pg_index_workers": "2",
    "src_write_files": "",
}


def test_in_parent_source_add_does_not_need_restart(workspace):
    """db start (no sources) → source add (default paths) → run ingest, no restart.

    Default-path source's parsed/output dirs live under ``${PARSED_PATH}`` /
    ``${OUTPUT_PATH}``, which the postgres compose block already binds. The
    running container can see the new source's files immediately; the source-
    add warning stays silent and the ingest validator green-lights the run.
    """
    # 1. db setup, no sources yet.
    rc, output = SDPSession(PG_DB_SETUP).run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"

    # 2. Start postgres BEFORE adding any source.
    result = run_sdp("db start postgres")
    assert result.returncode == 0, (
        f"db start failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    wait_for_healthy("postgres")

    # 3. Add the source AFTER PG is running. With default paths (in-parent),
    #    no override regen is required, and source add MUST NOT warn.
    rc, output = SDPSession(PG_SOURCE_ADD).run_interactive("source add reddit")
    assert rc == 0, f"source add failed:\n{output}"
    assert "[WARN] Mount drift" not in output, (
        f"source add unexpectedly warned for in-parent source:\n{output}"
    )

    # 4. db verify reports clean — no mount-related findings.
    result = run_sdp("db verify")
    assert result.returncode == 0, (
        f"db verify reported drift after in-parent source add:\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )

    # 5. Place fixtures and run parse → ingest, all in one go (no db restart).
    place_reddit_fixtures("reddit", data_types=["comments"])

    result = run_sdp("run parse --source reddit --build")
    assert result.returncode == 0, f"run parse failed:\n{result.stderr}"

    result = run_sdp("run postgres_ingest --source reddit --build")
    assert result.returncode == 0, (
        f"postgres_ingest failed without an intervening db restart "
        f"(this is the parent-mount fix's whole point):\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )

    # 6. Sanity-check the data landed via the live (un-restarted) container.
    conn = pg_connect()
    try:
        assert pg_table_exists(conn, "reddit", "comments")
        count = pg_row_count(conn, "reddit", "comments")
        assert count == 10, f"Expected 10 rows, got {count}"
    finally:
        conn.close()

    run_sdp("db stop postgres")
