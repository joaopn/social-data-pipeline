"""E2E: StarRocks full flow — parse → sr_ingest → verify.

Full flow:
  sdp db setup       → starrocks, no auth
  sdp source add reddit → parse + sr_ingest
  [compress fixtures → .zst]
  sdp db start starrocks
  sdp run parse
  sdp run sr_ingest
  → verify: database exists, rows correct, no dupes, BITMAP indexes created
  sdp db stop starrocks

Mirror of test_postgres_flow.py for the StarRocks backend. Parquet only —
the CSV path through `FILES()` is exercised at unit level in
tests/db/test_starrocks_ingest.py.
"""

from tests.e2e.helpers.sdp import SDPSession, run_sdp, wait_for_healthy
from tests.e2e.helpers.fixtures import place_reddit_fixtures
from tests.e2e.helpers.db import (
    sr_connect,
    sr_table_exists,
    sr_row_count,
    sr_query_scalar,
    sr_index_columns,
    sr_show_create_table,
)


# StarRocks-only db setup (databases option 3 = starrocks). No auth.
DB_SETUP_ANSWERS = {
    "db_data_path": "",
    "db_databases": "3",          # starrocks
    "db_sr_data_path": "",
    "db_export_path": "",
    "db_sr_port": "",
    "db_sr_fe_http_port": "",
    "db_sr_fe_heap": "",
    "db_sr_mem_limit": "0",       # unlimited (E2E machines vary)
    "db_sr_be_mem": "",
    "db_sr_alter_workers": "",
    "db_sr_multidisk": "",        # no
    "db_auth": "",                # no
    "db_write_files": "",
}

# parse + sr_ingest (positions 1 and 4 in [parse, lingua, ml, sr_ingest]).
SOURCE_ADD_ANSWERS = {
    "src_data_types": "",         # accept default [submissions, comments]
    "src_dumps_path": "",
    "src_extracted_path": "",
    "src_parsed_path": "",
    "src_output_path": "",
    "src_file_format": "1",       # parquet
    "src_parquet_rg_size": "",
    "src_profiles": "1,4",        # parse + sr_ingest
    "src_parse_workers": "2",
    "src_sr_prefer_lingua": "",   # default = "lingua" in profiles → false here
    "src_sr_buckets": "",         # accept default
    "src_write_files": "",
}

# StarRocks FE+BE startup is slower than PG; allow more headroom.
SR_HEALTH_TIMEOUT = 180


def test_starrocks_full_flow(workspace):
    """Parse Reddit dumps, ingest into StarRocks, verify data."""
    # 1. Database setup
    session = SDPSession(DB_SETUP_ANSWERS)
    rc, output = session.run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"

    # 2. Source add
    session = SDPSession(SOURCE_ADD_ANSWERS)
    rc, output = session.run_interactive("source add reddit")
    assert rc == 0, f"source add failed:\n{output}"

    # 3. Place test data
    place_reddit_fixtures("reddit", data_types=["comments"])

    # 4. Start StarRocks
    result = run_sdp("db start starrocks")
    assert result.returncode == 0, f"db start failed:\n{result.stderr}"
    wait_for_healthy("starrocks", timeout=SR_HEALTH_TIMEOUT)

    # 5. Run parse
    result = run_sdp("run parse --source reddit --build")
    assert result.returncode == 0, f"run parse failed:\n{result.stderr}"

    parsed_path = workspace / "data" / "parsed" / "reddit" / "comments" / "RC_2024-01.parquet"
    assert parsed_path.exists(), f"Parsed parquet not found. Parse output:\n{result.stdout}"

    # 6. Run sr_ingest
    result = run_sdp("run sr_ingest --source reddit --build")
    assert result.returncode == 0, f"run sr_ingest failed:\n{result.stderr}"
    sr_ingest_output = result.stdout + result.stderr

    # 7. Verify StarRocks data
    conn = sr_connect()
    try:
        # Database = SOURCE = "reddit"
        assert sr_table_exists(conn, "reddit", "comments"), (
            f"Table reddit.comments not found. sr_ingest output:\n{sr_ingest_output}"
        )

        # Row count matches fixture (10 comments)
        count = sr_row_count(conn, "reddit", "comments")
        assert count == 10, f"Expected 10 rows, got {count}"

        # No duplicate IDs (PK table should auto-dedup, but assert anyway)
        dupes = sr_query_scalar(
            conn, "SELECT COUNT(*) - COUNT(DISTINCT id) FROM `reddit`.`comments`"
        )
        assert dupes == 0, f"Found {dupes} duplicate IDs"

        # BITMAP indexes — reddit template's `indexes.comments` is the fallback
        # source when sr_indexes is absent. Expect: dataset, author, subreddit, link_id.
        idx_cols = sr_index_columns(conn, "reddit", "comments")
        expected = {"dataset", "author", "subreddit", "link_id"}
        missing = expected - idx_cols
        assert not missing, f"Missing BITMAP indexes: {missing} (have: {idx_cols})"

        # Table compression: `db setup` defaults to ZSTD and writes it to
        # config/db/starrocks.yaml; sr_ingest must carry it into CREATE TABLE.
        # Only the DDL can prove this — a missing config key or an image
        # predating the feature both silently fall back to StarRocks' LZ4,
        # which no unit test can observe.
        ddl = sr_show_create_table(conn, "reddit", "comments")
        assert ddl is not None, "SHOW CREATE TABLE returned no rows"
        assert '"compression" = "ZSTD"' in ddl, (
            f"base table should be created with ZSTD compression. Got:\n{ddl}"
        )
    finally:
        conn.close()

    # 8. Idempotency — re-run sr_ingest. State should mark the file processed;
    # PK upsert handles dedup at the DB layer either way. Folded here to avoid
    # paying SR's ~7 min cold-boot a second time in test_idempotency.py.
    result = run_sdp("run sr_ingest --source reddit")
    assert result.returncode == 0, f"sr_ingest re-run failed:\n{result.stderr}"

    conn = sr_connect()
    try:
        rows_after = sr_row_count(conn, "reddit", "comments")
        assert rows_after == count, (
            f"row count drift on re-run: was {count}, now {rows_after}"
        )
    finally:
        conn.close()

    # 9. Stop StarRocks
    run_sdp("db stop starrocks")
