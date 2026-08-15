"""E2E: StarRocks ML flow — parse → lingua → sr_ingest → sr_ml → verify.

Bug class:
  - sr_ml has zero E2E coverage prior to this test.
  - The orchestrator's classifier-table creation, schema inference, and
    PK-upsert idempotency are all silent if broken.
  - resolve_classifier_runs + detect_classifier_csvs path-name munging is
    fragile (suffix injection into the platform regex) and only runs end-to-end.
  - sr_ml index creation is fire-and-poll against a live BE: a wrong table key
    or a lost alter job leaves the table unindexed with a clean exit code.

Runs with `prefer_lingua=false` so the per-classifier table path is
actually exercised: lingua writes a second `/data/output/lingua_ingest/`
output that sr_ml ingests into `comments_lingua`. With prefer_lingua=true
the lingua data goes into the base table via sr_ingest and sr_ml has
nothing to do for the lingua classifier — that's a no-op flow we'd cover
only if GPU classifiers were available.

Flow:
  sdp db setup       → starrocks, no auth
  sdp source add reddit → parse + lingua + sr_ingest + sr_ml,
                          src_sr_prefer_lingua=n
  [compress fixtures → .zst]
  sdp db start starrocks
  sdp run parse
  sdp run lingua            (writes both lingua/ and lingua_ingest/)
  sdp run sr_ingest         (reads /data/parsed → reddit.comments)
  [write sr_ml_indexes: {comments_lingua: [lang]} into platform.yaml]
  sdp run sr_ml             (reads /data/output/lingua_ingest → reddit.comments_lingua)
  → verify both tables exist with rows, and the BITMAP index on comments_lingua
  re-run sdp run sr_ml
  → verify row counts unchanged (PK upsert idempotency)
  sdp db stop starrocks
"""

import yaml

from tests.e2e.helpers.sdp import SDPSession, run_sdp, wait_for_healthy
from tests.e2e.helpers.fixtures import place_reddit_fixtures
from tests.e2e.helpers.db import (
    sr_connect,
    sr_index_columns,
    sr_table_exists,
    sr_row_count,
    sr_show_create_table,
)


# StarRocks-only db setup, no auth.
DB_SETUP_ANSWERS = {
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

# parse + lingua + sr_ingest + sr_ml.
# all_profiles for SR-only = [parse, lingua, ml, sr_ingest, sr_ml] → 1,2,4,5.
SOURCE_ADD_ANSWERS = {
    "src_data_types": "",
    "src_dumps_path": "",
    "src_extracted_path": "",
    "src_parsed_path": "",
    "src_output_path": "",
    "src_file_format": "1",
    "src_parquet_rg_size": "",
    "src_profiles": "1,2,4,5",
    "src_parse_workers": "2",
    # lingua prompts (lingua selected)
    "cl_lingua_workers": "",
    "cl_lingua_file_workers": "",
    "cl_lingua_batch_size": "",
    "cl_lingua_low_accuracy": "",
    # SR settings — force prefer_lingua=false so sr_ml ingests the lingua
    # classifier output as its own table. With true, lingua data lands in
    # the base table via sr_ingest and sr_ml's lingua run is suppressed.
    "src_sr_prefer_lingua": "n",
    "src_sr_buckets": "",
    "src_write_files": "",
}

SR_HEALTH_TIMEOUT = 180


def test_starrocks_ml_full_flow(workspace):
    """Parse → lingua → sr_ingest → sr_ml; verify both tables and idempotency."""
    rc, output = SDPSession(DB_SETUP_ANSWERS).run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"

    rc, output = SDPSession(SOURCE_ADD_ANSWERS).run_interactive("source add reddit")
    assert rc == 0, f"source add failed:\n{output}"

    place_reddit_fixtures("reddit", data_types=["comments"])

    result = run_sdp("db start starrocks")
    assert result.returncode == 0, f"db start failed:\n{result.stderr}"
    wait_for_healthy("starrocks", timeout=SR_HEALTH_TIMEOUT)

    try:
        # parse → /data/parsed/reddit/comments/RC_2024-01.parquet
        result = run_sdp("run parse --source reddit --build")
        assert result.returncode == 0, f"run parse failed:\n{result.stderr}"

        # lingua → /data/output/reddit/lingua/comments/RC_2024-01_lingua.parquet
        # AND /data/output/reddit/lingua_ingest/comments/RC_2024-01_lingua.parquet
        # (the second only when prefer_lingua=false — it's the file sr_ml reads).
        result = run_sdp("run lingua --source reddit --build")
        assert result.returncode == 0, f"run lingua failed:\n{result.stderr}"

        lingua_path = (
            workspace / "data" / "output" / "reddit"
            / "lingua" / "comments" / "RC_2024-01_lingua.parquet"
        )
        assert lingua_path.exists(), (
            f"Lingua output not found at {lingua_path}. lingua stdout:\n{result.stdout}"
        )
        ingest_path = (
            workspace / "data" / "output" / "reddit"
            / "lingua_ingest" / "comments" / "RC_2024-01_lingua.parquet"
        )
        assert ingest_path.exists(), (
            f"lingua_ingest output not found at {ingest_path}. "
            f"prefer_lingua=false should have triggered the dual-write. "
            f"lingua stdout:\n{result.stdout}"
        )

        # sr_ingest → reddit.comments (prefer_lingua=false: ingests
        # /data/parsed; base table has no lang columns).
        result = run_sdp("run sr_ingest --source reddit --build")
        assert result.returncode == 0, f"run sr_ingest failed:\n{result.stderr}"

        # Configure a BITMAP index on the classifier table before sr_ml runs.
        # `sr_ml_indexes` is keyed by classifier table name and has no fallback
        # to sr_indexes, so this is the only way to reach sr_ml's index phase.
        # First E2E to mutate platform.yaml mid-run: there is no prompt for these
        # keys (the tables don't exist at `source add` time), so a hand edit is
        # exactly what a user would do.
        platform_path = workspace / "config" / "sources" / "reddit" / "platform.yaml"
        platform_config = yaml.safe_load(platform_path.read_text())
        platform_config["sr_ml_indexes"] = {"comments_lingua": ["lang"]}
        platform_path.write_text(yaml.safe_dump(platform_config, sort_keys=False))

        # sr_ml → reddit.comments_lingua (per-classifier table; suffix "_lingua").
        result = run_sdp("run sr_ml --source reddit --build")
        assert result.returncode == 0, f"run sr_ml failed:\n{result.stderr}"
        sr_ml_output = result.stdout + result.stderr

        # Verify both tables.
        conn = sr_connect()
        try:
            assert sr_table_exists(conn, "reddit", "comments"), "reddit.comments missing"
            base_rows = sr_row_count(conn, "reddit", "comments")
            assert base_rows == 10, f"reddit.comments: expected 10 rows, got {base_rows}"

            assert sr_table_exists(conn, "reddit", "comments_lingua"), (
                f"reddit.comments_lingua missing. sr_ml output:\n{sr_ml_output}"
            )
            ml_rows = sr_row_count(conn, "reddit", "comments_lingua")
            assert ml_rows >= 1, (
                f"reddit.comments_lingua: expected rows, got {ml_rows}. "
                f"sr_ml output:\n{sr_ml_output}"
            )

            # Classifier tables take the same global codec as base tables.
            # Asserted separately because sr_ml builds its DDL through
            # get_classifier_create_table_query — a distinct code path from
            # sr_ingest, and a separate image that can go stale on its own.
            ddl = sr_show_create_table(conn, "reddit", "comments_lingua")
            assert ddl is not None, "SHOW CREATE TABLE returned no rows"
            assert '"compression" = "ZSTD"' in ddl, (
                f"classifier table should be created with ZSTD compression. Got:\n{ddl}"
            )

            # BITMAP index from sr_ml_indexes. sr_ml polls the alter job to
            # FINISHED before returning, so this is visible immediately.
            idx_cols = sr_index_columns(conn, "reddit", "comments_lingua")
            assert "lang" in {c.lower() for c in idx_cols}, (
                f"expected a BITMAP index on lang from sr_ml_indexes, have: {idx_cols}. "
                f"sr_ml output:\n{sr_ml_output}"
            )
        finally:
            conn.close()

        # Idempotency: re-running sr_ml should not change row counts.
        # State tracking should mark the file as already processed; even if
        # the state file doesn't filter, PK upsert handles the dedup.
        result = run_sdp("run sr_ml --source reddit")
        assert result.returncode == 0, f"sr_ml re-run failed:\n{result.stderr}"

        conn = sr_connect()
        try:
            ml_rows_after = sr_row_count(conn, "reddit", "comments_lingua")
            assert ml_rows_after == ml_rows, (
                f"sr_ml re-run changed row count: was {ml_rows}, now {ml_rows_after}"
            )
        finally:
            conn.close()
    finally:
        run_sdp("db stop starrocks")
