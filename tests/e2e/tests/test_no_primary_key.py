"""E2E: custom platform with no primary_key — PG append-only + SR Duplicate Key.

Bug class:
  Setup time — the PK prompt previously hard-defaulted to "id" or the first
  configured field, producing a runtime guard error or a wrong-key constraint
  for HF datasets that lack a natural unique column. Now the prompt accepts
  blank and the setup writes an "absent primary_key" config.

  Runtime — both PG and SR need to honour the missing PK without crashing.
  PG: fast-load path skips ADD PRIMARY KEY when pk_column is None.
  SR: get_create_table_query emits a Duplicate Key table with DISTRIBUTED
      BY RANDOM when pk_column is None; before the fix, the table-creation
      gate at sr_ingest.py was `if pk_column and not table_exists(...)`
      which silently skipped CREATE TABLE and let the subsequent INSERT
      fail with "table does not exist".

The PG sub-test additionally guards the F3 fix: generate_platform_yaml now
emits `mandatory_fields: [dataset]` for custom platforms so the column count
in CREATE TABLE matches what the custom parser writes (parser prepends a
`dataset` column unconditionally; without the mandatory_fields entry, PG
table has N columns while Parquet has N+1, and pg_parquet COPY explodes).
"""

import shutil
from pathlib import Path

import yaml

from tests.e2e.helpers.sdp import SDPSession, run_sdp, wait_for_healthy
from tests.e2e.helpers.db import (
    pg_connect,
    pg_table_exists,
    pg_row_count,
    pg_table_has_pk,
    sr_connect,
    sr_table_exists,
    sr_row_count,
    sr_show_create_table,
)


SR_HEALTH_TIMEOUT = 180

# The first --build of a profile inside a freshly-started sysbox does a cold
# pip install of the full requirements (polars + pyarrow + lingua-language-
# detector + psycopg). On a constrained network that can exceed the default
# 600s subprocess timeout. In a full E2E run the image is cached by earlier
# tests; in a `-k test_no_primary_key` isolation run it is not, and the first
# parse call has to do the full build itself. Pad the budget for that call
# (subsequent ones use the cached image and stay well under).
BUILD_TIMEOUT = 1800

# The dump glob `events*.ndjson.zst` (set in *_SOURCE_ADD answers) derives a
# parser json regex of `^events.*\.ndjson$` via setup.utils.derive_file_patterns
# (`.ndjson` does NOT end in `.json` for the strip check, so the suffix
# survives). The shared `place_custom_fixtures` helper writes the fixture as
# `events` with no extension — that name matches the hand-built unit-test
# platform config but NOT the source-add-derived regex, which is why
# test_parse_custom.py is currently deferred. Place inline with the matching
# `.ndjson` suffix to keep this test independent of that issue.
def _place_events_fixture(workspace: Path, source: str) -> None:
    src = workspace / "tests" / "fixtures" / "custom" / "events.ndjson"
    dst_dir = workspace / "data" / "extracted" / source / "events"
    dst_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst_dir / "events.ndjson")


# ── PostgreSQL ────────────────────────────────────────────────────────────────

PG_DB_SETUP = {
    "db_data_path": "",
    "db_databases": "1",          # postgres
    "db_pgdata_path": "",
    "db_export_path": "",
    "db_name": "",
    "db_pg_port": "",
    "db_tablespaces": "",
    "db_filesystem": "1",
    "db_pgtune_method": "3",      # skip
    "db_pg_mem_limit": "0",       # unlimited
    "db_auth": "",
    "db_write_files": "",
}

PG_SOURCE_ADD = {
    "src_data_types": "events",
    "src_dumps_path": "",
    "src_extracted_path": "",
    "src_parsed_path": "",
    "src_output_path": "",
    "src_file_format": "1",       # parquet
    "src_parquet_rg_size": "",
    "src_profiles": "1,4",        # parse + postgres_ingest (PG-only: [parse, lingua, ml, pg_ingest, pg_ml])
    "src_parse_workers": "2",
    "src_pg_prefer_lingua": "n",
    "src_pg_index_workers": "2",
    "src_db_schema": "",
    "src_input_format": "1",      # ndjson
    "src_dump_glob_events": "events*.ndjson.zst",
    # Top-level, non-reserved field names only. Dot-notation (e.g. user.name)
    # would expose an unrelated pre-existing bug (F1/F2 — custom parser
    # doesn't resolve to leaf, and PG DDL doesn't quote column names). Out
    # of scope here; sidestep with plain identifiers.
    "src_fields_events": "id, score, content",
    "src_pg_indexes_events": "",
    "src_primary_key": "",        # ← THE BIT UNDER TEST: blank = no source PK
    "src_write_files": "",
}


def test_custom_no_pk_postgres(workspace):
    """Custom platform with blank PK ingests into PG with no PRIMARY KEY constraint."""
    rc, output = SDPSession(PG_DB_SETUP).run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"

    rc, output = SDPSession(PG_SOURCE_ADD).run_interactive("source add mydata")
    assert rc == 0, f"source add failed:\n{output}"

    # Config-side assertions — pin the generator's behavior so a future
    # setup refactor can't silently regress the no-PK gating or the F3
    # mandatory_fields fix.
    platform_yaml = yaml.safe_load(
        (workspace / "config" / "sources" / "mydata" / "platform.yaml").read_text()
    )
    assert "primary_key" not in platform_yaml, (
        f"platform.yaml should omit primary_key when blank, got: {platform_yaml.get('primary_key')!r}"
    )
    # F3 regression guard: dataset column must be in mandatory_fields so PG
    # DDL column count matches what the parser writes.
    assert platform_yaml.get("mandatory_fields") == ["dataset"], (
        f"platform.yaml must declare dataset as a mandatory_field for custom platforms; "
        f"got: {platform_yaml.get('mandatory_fields')!r}"
    )

    pg_yaml = yaml.safe_load(
        (workspace / "config" / "sources" / "mydata" / "postgres.yaml").read_text()
    )
    assert pg_yaml["pipeline"]["processing"].get("check_duplicates") is False, (
        f"postgres.yaml must set check_duplicates: false when no PK; got: {pg_yaml}"
    )

    _place_events_fixture(workspace, "mydata")

    result = run_sdp("db start postgres")
    assert result.returncode == 0, f"db start failed:\n{result.stderr}"
    wait_for_healthy("postgres")

    try:
        result = run_sdp("run parse --source mydata --build", timeout=BUILD_TIMEOUT)
        assert result.returncode == 0, f"parse failed:\n{result.stderr}"
        # Pin parse-found-files: if the source-add-derived regex stops
        # matching the placed fixture, parse silently succeeds with 0 files
        # and the downstream ingest succeeds with no table created — a much
        # less readable failure mode than the assertion below.
        parsed_files = list((workspace / "data" / "parsed" / "mydata" / "events").glob("*.parquet"))
        assert parsed_files, (
            f"parse produced no parquet files. parse stdout:\n{result.stdout}"
        )

        result = run_sdp("run postgres_ingest --source mydata --build", timeout=BUILD_TIMEOUT)
        assert result.returncode == 0, (
            f"postgres_ingest failed:\n{result.stderr}\nstdout:\n{result.stdout}"
        )

        conn = pg_connect()
        try:
            assert pg_table_exists(conn, "mydata", "events"), (
                "mydata.events table not created"
            )
            assert pg_row_count(conn, "mydata", "events") == 5
            # The core assertion: no PRIMARY KEY constraint on the table
            # when primary_key was left blank at source-add time.
            assert not pg_table_has_pk(conn, "mydata", "events"), (
                "mydata.events should have NO PRIMARY KEY constraint "
                "(no-PK source); fast-load skipped ADD PRIMARY KEY"
            )
        finally:
            conn.close()
    finally:
        run_sdp("db stop postgres")


# ── StarRocks ─────────────────────────────────────────────────────────────────

SR_DB_SETUP = {
    "db_data_path": "",
    "db_databases": "3",          # starrocks
    "db_sr_data_path": "",
    "db_export_path": "",
    "db_sr_port": "",
    "db_sr_fe_http_port": "",
    "db_sr_fe_heap": "",
    "db_sr_mem_limit": "0",
    "db_sr_be_mem": "",
    "db_sr_alter_workers": "",
    "db_sr_multidisk": "",
    "db_auth": "",
    "db_write_files": "",
}

SR_SOURCE_ADD = {
    "src_data_types": "events",
    "src_dumps_path": "",
    "src_extracted_path": "",
    "src_parsed_path": "",
    "src_output_path": "",
    "src_file_format": "1",
    "src_parquet_rg_size": "",
    "src_profiles": "1,4",        # parse + sr_ingest (SR-only: [parse, lingua, ml, sr_ingest, sr_ml])
    "src_parse_workers": "2",
    "src_sr_prefer_lingua": "n",
    "src_sr_buckets": "",
    "src_db_schema": "",
    "src_input_format": "1",
    "src_dump_glob_events": "events*.ndjson.zst",
    # Top-level, non-reserved field names only. Dot-notation (e.g. user.name)
    # exposes a pre-existing latent bug: setup writes the raw "user.name"
    # into platform.yaml and the parser emits a column literally named
    # "user.name". SR is forgiving about unmatched parquet columns, but the
    # symmetric PG path explodes on the unquoted "user" reserved word in the
    # generated DDL. Out of B2 scope; sidestep with plain identifiers.
    "src_fields_events": "id, score, content",
    "src_sr_indexes_events": "",
    "src_primary_key": "",        # ← THE BIT UNDER TEST
    "src_write_files": "",
}


def test_custom_no_pk_starrocks(workspace):
    """Custom platform with blank PK ingests into SR with Duplicate Key model."""
    rc, output = SDPSession(SR_DB_SETUP).run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"

    rc, output = SDPSession(SR_SOURCE_ADD).run_interactive("source add mydata")
    assert rc == 0, f"source add failed:\n{output}"

    sr_yaml = yaml.safe_load(
        (workspace / "config" / "sources" / "mydata" / "starrocks.yaml").read_text()
    )
    assert sr_yaml["pipeline"]["processing"].get("check_duplicates") is False, (
        f"starrocks.yaml must set check_duplicates: false when no PK; got: {sr_yaml}"
    )

    _place_events_fixture(workspace, "mydata")

    result = run_sdp("db start starrocks")
    assert result.returncode == 0, f"db start failed:\n{result.stderr}"
    wait_for_healthy("starrocks", timeout=SR_HEALTH_TIMEOUT)

    try:
        result = run_sdp("run parse --source mydata --build", timeout=BUILD_TIMEOUT)
        assert result.returncode == 0, f"parse failed:\n{result.stderr}"
        # Pin parse-found-files: if the source-add-derived regex stops matching
        # the placed fixture, parse silently succeeds with 0 files and the
        # downstream ingest succeeds with no table created — a much less
        # readable failure mode than the assertion below.
        parsed_files = list((workspace / "data" / "parsed" / "mydata" / "events").glob("*.parquet"))
        assert parsed_files, (
            f"parse produced no parquet files. parse stdout:\n{result.stdout}"
        )

        result = run_sdp("run sr_ingest --source mydata --build", timeout=BUILD_TIMEOUT)
        assert result.returncode == 0, (
            f"sr_ingest failed:\n{result.stderr}\nstdout:\n{result.stdout}"
        )

        conn = sr_connect()
        try:
            assert sr_table_exists(conn, "mydata", "events"), (
                "mydata.events table not created in StarRocks"
            )
            assert sr_row_count(conn, "mydata", "events") == 5

            # Core SR assertion: Duplicate Key model with RANDOM distribution.
            # PRIMARY KEY clause MUST be absent; UNIQUE KEY / AGGREGATE KEY
            # would also be wrong — only the bare default model is acceptable.
            ddl = sr_show_create_table(conn, "mydata", "events")
            assert ddl is not None, "SHOW CREATE TABLE returned no rows"
            assert "PRIMARY KEY" not in ddl, (
                f"SR DDL should not declare PRIMARY KEY for no-PK source. Got:\n{ddl}"
            )
            assert "UNIQUE KEY" not in ddl, (
                f"SR DDL should not declare UNIQUE KEY for no-PK source. Got:\n{ddl}"
            )
            assert "DISTRIBUTED BY RANDOM" in ddl, (
                f"SR DDL should use DISTRIBUTED BY RANDOM for no-PK source. Got:\n{ddl}"
            )
            assert "enable_persistent_index" not in ddl, (
                "enable_persistent_index is a PK-table property and should not "
                f"appear on a Duplicate Key table. Got:\n{ddl}"
            )
        finally:
            conn.close()
    finally:
        run_sdp("db stop starrocks")
