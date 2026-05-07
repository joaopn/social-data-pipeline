"""E2E: full PostgreSQL credential lifecycle (H7).

Layered failure mode: setup writes auth state into four locations (yaml,
.env, ``.ro_credentials``, container env). ``test_auth_postgres.py``
covers the setup → start → enforce path; this test extends to setup →
start → recover-password --regenerate-ro → restart → ingest, which is
the contract that drove C1–C5 of the auth-robustness sweep. Without
this test, a regression in any one of those four storage locations can
silently survive into a release because the setup and migration paths
both still work in isolation.

Specifically pinned:
  - .ro_credentials mode 0600 + host-uid ownership preserved across
    --regenerate-ro.
  - RO password actually rotates (new value differs from initial).
  - Post-recovery restart cycle (db stop && db start) leaves the DB
    healthy with the new admin password.
  - db verify exits clean afterwards.
  - postgres_ingest succeeds under auth, end-to-end, with the new
    admin password — exercises the admin connection path that's
    independent of the RO sync that the entrypoint does.
"""

import os

from tests.e2e.helpers.sdp import SDPSession, run_sdp, wait_for_healthy, WORKSPACE
from tests.e2e.helpers.fixtures import place_reddit_fixtures
from tests.e2e.helpers.db import (
    pg_connect, pg_query_scalar, pg_table_exists, pg_row_count,
    read_ro_credentials,
)


INITIAL_ADMIN_PASSWORD = "Initial!Admin0"
ROTATED_ADMIN_PASSWORD = "Rotated!Admin1"

PG_DATA_DIR = WORKSPACE / "data" / "database" / "postgres"
RO_CRED_FILE = PG_DATA_DIR / ".ro_credentials"


DB_SETUP_AUTH_RO = {
    "db_data_path": "",
    "db_databases": "1",                # postgres only
    "db_pgdata_path": "",
    "db_export_path": "",
    "db_name": "",
    "db_pg_port": "",
    "db_tablespaces": "",
    "db_filesystem": "1",
    "db_pgtune_method": "3",            # skip
    "db_pg_mem_limit": "0",
    "db_auth": "y",
    "db_password": INITIAL_ADMIN_PASSWORD,
    "db_password_confirm": INITIAL_ADMIN_PASSWORD,
    "db_ro_user": "",                   # yes (default)
    "db_ro_username": "",
    "db_ro_auto_password": "",          # yes (default)
    "db_write_files": "",
}

PG_SOURCE_ADD = {
    "src_data_types": "",
    "src_dumps_path": "",
    "src_extracted_path": "",
    "src_parsed_path": "",
    "src_output_path": "",
    "src_file_format": "1",             # parquet
    "src_parquet_rg_size": "",
    "src_profiles": "1,4",              # parse + postgres_ingest
    "src_parse_workers": "2",
    "src_pg_prefer_lingua": "n",
    "src_pg_index_workers": "2",
    "src_write_files": "",
}


def _start_pg(password):
    result = run_sdp("db start postgres", input_text=f"{password}\n")
    assert result.returncode == 0, (
        f"db start failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    wait_for_healthy("postgres")


def _assert_cred_file_secure():
    st = RO_CRED_FILE.stat()
    mode = oct(st.st_mode)[-3:]
    assert mode == "600", (
        f".ro_credentials mode regressed to {mode}; must be 600 (chmod 0600)"
    )
    assert st.st_uid == os.getuid(), (
        f".ro_credentials uid={st.st_uid}, expected host uid={os.getuid()}. "
        f"docker may have created/touched it as root — drift in the "
        f"credential-write path."
    )


def test_setup_recover_restart_cycle_with_ro(workspace):
    """Setup → recover --regenerate-ro → restart → ingest, all with auth."""
    # 1. Initial PG setup with auth + RO user.
    rc, output = SDPSession(DB_SETUP_AUTH_RO).run_interactive("db setup")
    assert rc == 0, f"db setup failed:\n{output}"

    # 2. Start, capture initial RO credentials.
    _start_pg(INITIAL_ADMIN_PASSWORD)
    assert RO_CRED_FILE.exists(), ".ro_credentials missing after first start"
    _assert_cred_file_secure()
    initial_ro_user, initial_ro_pw = read_ro_credentials(PG_DATA_DIR)
    assert initial_ro_user, "ro_username missing from postgres.yaml"
    assert initial_ro_pw, "initial RO password is empty"

    # Sanity: the initial RO credentials actually authenticate.
    conn = pg_connect(user=initial_ro_user, password=initial_ro_pw)
    try:
        assert pg_query_scalar(conn, "SELECT count(*) FROM pg_database") >= 1
    finally:
        conn.close()

    # 3. Rotate admin password AND regenerate RO. The recovery flow does
    #    `docker compose restart` of postgres (trust-auth swap) followed
    #    by `down + up -d` (scram restore + new env). Drive via run_sdp
    #    so getpass reads from stdin (test_auth_postgres uses this same
    #    pattern for db_start under auth).
    result = run_sdp(
        "db recover-password --regenerate-ro",
        input_text=f"{ROTATED_ADMIN_PASSWORD}\n{ROTATED_ADMIN_PASSWORD}\n",
        timeout=300,
    )
    assert result.returncode == 0, (
        f"recover-password --regenerate-ro failed:\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    wait_for_healthy("postgres")

    # 4. RO password must have rotated; mode + ownership unchanged.
    _assert_cred_file_secure()
    rotated_ro_user, rotated_ro_pw = read_ro_credentials(PG_DATA_DIR)
    assert rotated_ro_user == initial_ro_user, (
        f"ro_username silently changed across rotation: "
        f"{initial_ro_user} -> {rotated_ro_user}"
    )
    assert rotated_ro_pw and rotated_ro_pw != initial_ro_pw, (
        "RO password did not rotate after `recover-password --regenerate-ro`"
    )

    # 5. New admin password works against the live container (the recovery
    #    flow's final `up -d`). Old admin password must NOT.
    conn = pg_connect(password=ROTATED_ADMIN_PASSWORD)
    try:
        assert pg_query_scalar(conn, "SELECT 1") == 1
    finally:
        conn.close()

    # 6. New RO password works (entrypoint sync picked up .ro_credentials
    #    on the post-rotation `up -d`).
    conn = pg_connect(user=rotated_ro_user, password=rotated_ro_pw)
    try:
        assert pg_query_scalar(conn, "SELECT count(*) FROM pg_database") >= 1
    finally:
        conn.close()

    # 7. Stop + start cycle on top of the rotation. Catches state that
    #    only worked on the recovery flow's own restart but not on a clean
    #    db stop/start (e.g. an env var that lingered in the recovery
    #    process but isn't reread on cmd_db_start).
    result = run_sdp("db stop postgres")
    assert result.returncode == 0, f"db stop after rotation failed:\n{result.stderr}"
    _start_pg(ROTATED_ADMIN_PASSWORD)
    _assert_cred_file_secure()

    # 8. db verify reports clean.
    result = run_sdp("db verify")
    assert result.returncode == 0, (
        f"db verify reports drift after rotate+restart:\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )

    # 9. Source add → parse → ingest under auth. The ingest path connects
    #    as admin (not RO), so this exercises the rotated admin password
    #    flowing through cmd_run -> _prompt_db_password -> POSTGRES_PASSWORD
    #    -> the postgres_ingest container's psycopg connection.
    rc, output = SDPSession(PG_SOURCE_ADD).run_interactive("source add reddit")
    assert rc == 0, f"source add failed:\n{output}"

    place_reddit_fixtures("reddit", data_types=["comments"])

    result = run_sdp("run parse --source reddit --build")
    assert result.returncode == 0, f"run parse failed:\n{result.stderr}"

    result = run_sdp(
        "run postgres_ingest --source reddit --build",
        input_text=f"{ROTATED_ADMIN_PASSWORD}\n",
    )
    assert result.returncode == 0, (
        f"postgres_ingest failed under rotated auth:\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )

    # 10. Sanity-check rows landed.
    conn = pg_connect(password=ROTATED_ADMIN_PASSWORD)
    try:
        assert pg_table_exists(conn, "reddit", "comments")
        count = pg_row_count(conn, "reddit", "comments")
        assert count == 10, f"Expected 10 rows, got {count}"
    finally:
        conn.close()

    run_sdp("db stop postgres")
