"""E2E: db reset --db <name> full loop.

Setup → ingest → snapshot → reset → assert preservation → start → re-ingest →
assert identical row counts.

The reset contract is:
- Wipe DB data (server-owned subdirs + state_tracking + mongo's logs).
- Preserve config/db/<db>.yaml, config/sources/*/<db>.yaml, .env,
  docker-compose.override.yml, config/db/mcp.yaml, and .ro_credentials.
- After reset, `sdp db start <db>` brings the DB back up with the
  preserved RO credentials (the entrypoint re-creates the RO user from
  the unchanged file), ready for re-ingest.

Unit tests pin the path-level + argv-level contracts. This e2e validates
the actual chown + rmtree fires against a real container and that
re-ingest produces the same data as the first ingest.

Two DBs covered:
- MongoDB: this was the bug class that motivated the feature (the
  recover-password flow corrupted a directoryPerDB layout). The reset
  loop must restore the dataset cleanly.
- PostgreSQL: covers the COPY ingest path and the post-ingest index
  creation, which is more state to lose / recover than mongo.

StarRocks is intentionally skipped — `test_unsetup_symmetry.py` already
e2e-exercises its chown + rmtree shape (storage_paths cleanup), and the
unit tests in test_db_reset.py pin the fe/be wipe contract.
"""

from tests.e2e.helpers.sdp import SDPSession, run_sdp, wait_for_healthy, WORKSPACE
from tests.e2e.helpers.fixtures import place_reddit_fixtures, place_reddit_extracted
from tests.e2e.helpers.db import (
    pg_connect, pg_row_count, pg_table_exists,
    mongo_connect, mongo_doc_count,
)


# ---------------------------------------------------------------------------
# MongoDB reset round trip.
# ---------------------------------------------------------------------------


MONGO_DB_SETUP = {
    "db_data_path": "",
    "db_databases": "2",
    "db_mongo_data_path": "",
    "db_mongo_port": "",
    "db_mongo_cache": "1",
    "db_mongo_mem_limit": "0",
    "db_mongo_validate": "",
    "db_auth": "",
    "db_write_files": "",
}

MONGO_SOURCE_ADD = {
    "src_data_types": "",
    "src_dumps_path": "",
    "src_extracted_path": "",
    "src_parsed_path": "",
    "src_output_path": "",
    "src_file_format": "1",
    "src_parquet_rg_size": "",
    "src_profiles": "6",            # mongo_ingest
    "src_write_files": "",
}


def test_mongo_reset_round_trip(workspace):
    """Mongo: ingest → reset → re-ingest yields identical doc counts."""
    # 1. Setup + source add + ingest.
    rc, output = SDPSession(MONGO_DB_SETUP).run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"
    rc, output = SDPSession(MONGO_SOURCE_ADD).run_interactive("source add reddit")
    assert rc == 0, f"source add failed:\n{output}"

    place_reddit_extracted("reddit", data_types=["comments"])

    result = run_sdp("db start mongo")
    assert result.returncode == 0, f"db start failed:\n{result.stderr}"
    wait_for_healthy("mongo")

    result = run_sdp("run mongo_ingest --source reddit --build")
    assert result.returncode == 0, f"run mongo_ingest failed:\n{result.stderr}"

    # 2. Snapshot config + .ro_credentials + row counts.
    config_dir = WORKSPACE / "config"
    snapshots = {
        ".env": (WORKSPACE / ".env").read_bytes(),
        "config/db/mongo.yaml": (config_dir / "db" / "mongo.yaml").read_bytes(),
        "config/sources/reddit/mongo.yaml": (
            config_dir / "sources" / "reddit" / "mongo.yaml"
        ).read_bytes(),
    }
    # .ro_credentials only exists when auth is on. This test runs auth-off,
    # so we skip the cred comparison — auth-on coverage lives in
    # test_auth_mongo.py + the unit tests' byte-comparison assertions.

    client = mongo_connect()
    try:
        pre_count = mongo_doc_count(client, "reddit_comments", "2024-01")
        assert pre_count > 0, "Initial ingest produced no documents"
    finally:
        client.close()

    # 3. Reset. Single confirmation (single "y\n").
    result = run_sdp("db reset --db mongo", input_text="y\n")
    assert result.returncode == 0, (
        f"db reset --db mongo failed:\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )

    # 4. Config is byte-identical post-reset.
    assert (WORKSPACE / ".env").read_bytes() == snapshots[".env"], (
        "Reset must not modify .env"
    )
    assert (config_dir / "db" / "mongo.yaml").read_bytes() == \
        snapshots["config/db/mongo.yaml"], "Reset must not modify config/db/mongo.yaml"
    assert (config_dir / "sources" / "reddit" / "mongo.yaml").read_bytes() == \
        snapshots["config/sources/reddit/mongo.yaml"], (
            "Reset must not modify per-source override"
        )

    # 5. The mongo db/ subdir is empty (reset recreates it as a fresh dir).
    mongo_data_subdir = WORKSPACE / "data" / "database" / "mongo" / "db"
    if mongo_data_subdir.exists():
        assert list(mongo_data_subdir.iterdir()) == [], (
            f"Mongo db/ should be empty post-reset, found: "
            f"{list(mongo_data_subdir.iterdir())}"
        )

    # 6. Start mongo again — must come up cleanly with no manual setup.
    result = run_sdp("db start mongo")
    assert result.returncode == 0, (
        f"db start mongo after reset failed:\n{result.stderr}"
    )
    wait_for_healthy("mongo")

    # 7. Re-ingest.
    result = run_sdp("run mongo_ingest --source reddit --build")
    assert result.returncode == 0, (
        f"re-run mongo_ingest after reset failed:\n{result.stderr}"
    )

    # 8. Row counts match — proves state-tracking was reset AND the re-ingest
    # behaved identically to the first one.
    client = mongo_connect()
    try:
        post_count = mongo_doc_count(client, "reddit_comments", "2024-01")
        assert post_count == pre_count, (
            f"Doc count mismatch after reset+re-ingest: "
            f"before={pre_count}, after={post_count}"
        )
    finally:
        client.close()

    run_sdp("db stop mongo")


# ---------------------------------------------------------------------------
# PostgreSQL reset round trip.
# ---------------------------------------------------------------------------


PG_DB_SETUP = {
    "db_data_path": "",
    "db_databases": "1",
    "db_pgdata_path": "",
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
    "src_file_format": "1",
    "src_parquet_rg_size": "",
    "src_profiles": "1,4",          # parse + postgres_ingest
    "src_parse_workers": "2",
    "src_pg_prefer_lingua": "n",
    "src_pg_index_workers": "2",
    "src_write_files": "",
}


def test_postgres_reset_round_trip(workspace):
    """Postgres: parse → ingest → reset → re-ingest yields identical row counts."""
    rc, output = SDPSession(PG_DB_SETUP).run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"
    rc, output = SDPSession(PG_SOURCE_ADD).run_interactive("source add reddit")
    assert rc == 0, f"source add failed:\n{output}"

    place_reddit_fixtures("reddit", data_types=["comments"])

    result = run_sdp("db start postgres")
    assert result.returncode == 0, f"db start postgres failed:\n{result.stderr}"
    wait_for_healthy("postgres")

    result = run_sdp("run parse --source reddit --build")
    assert result.returncode == 0, f"run parse failed:\n{result.stderr}"

    result = run_sdp("run postgres_ingest --source reddit --build")
    assert result.returncode == 0, f"run postgres_ingest failed:\n{result.stderr}"

    # Snapshot.
    config_dir = WORKSPACE / "config"
    snapshots = {
        ".env": (WORKSPACE / ".env").read_bytes(),
        "config/db/postgres.yaml": (config_dir / "db" / "postgres.yaml").read_bytes(),
        "config/sources/reddit/postgres.yaml": (
            config_dir / "sources" / "reddit" / "postgres.yaml"
        ).read_bytes(),
    }
    override_path = WORKSPACE / "docker-compose.override.yml"
    if override_path.exists():
        snapshots["docker-compose.override.yml"] = override_path.read_bytes()

    conn = pg_connect()
    try:
        assert pg_table_exists(conn, "reddit", "comments")
        pre_count = pg_row_count(conn, "reddit", "comments")
        assert pre_count > 0, "Initial ingest produced no rows"
    finally:
        conn.close()

    # Reset.
    result = run_sdp("db reset --db postgres", input_text="y\n")
    assert result.returncode == 0, (
        f"db reset --db postgres failed:\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )

    # Every snapshotted config is byte-identical.
    for rel, data in snapshots.items():
        assert (WORKSPACE / rel).read_bytes() == data, (
            f"Reset modified {rel} — it must be preserved"
        )

    # pgdata is empty.
    pgdata_subdir = WORKSPACE / "data" / "database" / "postgres" / "pgdata"
    if pgdata_subdir.exists():
        assert list(pgdata_subdir.iterdir()) == [], (
            f"pgdata/ should be empty post-reset, found: "
            f"{list(pgdata_subdir.iterdir())}"
        )

    # Start + re-ingest.
    result = run_sdp("db start postgres")
    assert result.returncode == 0, (
        f"db start postgres after reset failed:\n{result.stderr}"
    )
    wait_for_healthy("postgres")

    result = run_sdp("run postgres_ingest --source reddit --build")
    assert result.returncode == 0, (
        f"re-run postgres_ingest after reset failed:\n{result.stderr}"
    )

    conn = pg_connect()
    try:
        post_count = pg_row_count(conn, "reddit", "comments")
        assert post_count == pre_count, (
            f"Row count mismatch after reset+re-ingest: "
            f"before={pre_count}, after={post_count}"
        )
    finally:
        conn.close()

    run_sdp("db stop postgres")


# ---------------------------------------------------------------------------
# Aborted reset.
# ---------------------------------------------------------------------------


def test_reset_aborted_keeps_data(workspace):
    """Declining the confirmation prompt is a true no-op.

    Captures the "user typed n by accident" case: nothing stops, nothing
    is deleted, the DB is still queryable on the next connection.
    """
    rc, output = SDPSession(MONGO_DB_SETUP).run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"
    rc, output = SDPSession(MONGO_SOURCE_ADD).run_interactive("source add reddit")
    assert rc == 0, f"source add failed:\n{output}"

    place_reddit_extracted("reddit", data_types=["comments"])

    result = run_sdp("db start mongo")
    assert result.returncode == 0, f"db start mongo failed:\n{result.stderr}"
    wait_for_healthy("mongo")

    result = run_sdp("run mongo_ingest --source reddit --build")
    assert result.returncode == 0, f"run mongo_ingest failed:\n{result.stderr}"

    client = mongo_connect()
    try:
        pre_count = mongo_doc_count(client, "reddit_comments", "2024-01")
    finally:
        client.close()

    # Decline the prompt.
    result = run_sdp("db reset --db mongo", input_text="n\n")
    assert result.returncode == 0
    assert "Aborted" in (result.stdout + result.stderr)

    # Mongo is still running and the data is still there. We don't
    # `db start` again here — declining must not have stopped it.
    client = mongo_connect()
    try:
        assert mongo_doc_count(client, "reddit_comments", "2024-01") == pre_count
    finally:
        client.close()

    run_sdp("db stop mongo")
