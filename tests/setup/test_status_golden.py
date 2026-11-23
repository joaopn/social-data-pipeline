"""Golden tests for the terminal text of the five read commands: `db status`, `db verify`, `source list`, `source status` and `source error-logs`.

Each scenario builds a fixture deployment in code under a temp folder, points the CLI at it, runs one `cmd_*` function and compares everything it printed, byte for byte, with a file under `golden/`. The exit code is asserted too. The goldens pin today's text as it is, known mistakes included (the read-only user taken from the first database that has one, the StarRocks ML line for a source without that profile, a source name such as `../x` read as a folder outside `config/sources/`): a change to any of them must show up as a golden diff, never slip through a refactor.

Two parts of the text depend on where and with what the test runs, and are replaced before the comparison: the temp project folder becomes `<root>`, and the message PyYAML gives for the unreadable `platform.yaml` of the fixtures becomes `<yaml error>` (its wording belongs to the installed PyYAML version; the message is taken from parsing the same broken text, so no pattern has to guess it).

Nothing touches the real tree, Docker or a terminal: `sdp` and `social_data_pipeline.setup.utils` point at the temp project, `subprocess.run` answers only `docker compose ps` (one JSON object per line), and the container probes of `db verify` are replaced. Credential files and passwords are made-up values.

To write the goldens again after an intended change of the text, run this module with `SDP_REGENERATE_GOLDEN=1` and read the diff of `golden/` before committing it:

    SDP_REGENERATE_GOLDEN=1 pytest --override-ini="pythonpath=." tests/setup/test_status_golden.py
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import pytest
import yaml

import sdp
from social_data_pipeline.setup import utils

GOLDEN_DIR = Path(__file__).parent / "golden"
REGENERATE_VARIABLE = "SDP_REGENERATE_GOLDEN"
ROOT_PLACEHOLDER = "<root>"
YAML_ERROR_PLACEHOLDER = "<yaml error>"

# Invalid YAML makes a platform.yaml unreadable whoever runs the tests; a permission bit would not stop root.
BROKEN_YAML = "data_types: [posts\n"

PROFILE_FILES = {
    "parse": "parse.yaml",
    "lingua": "lingua.yaml",
    "ml": "ml.yaml",
    "postgres_ingest": "postgres.yaml",
    "postgres_ml": "postgres_ml.yaml",
    "mongo_ingest": "mongo.yaml",
    "sr_ingest": "starrocks.yaml",
    "sr_ml": "sr_ml.yaml",
}
ALL_PROFILES = tuple(PROFILE_FILES)

COMMANDS = {
    "db status": sdp.cmd_db_status,
    "db verify": sdp.cmd_db_verify,
    "source list": sdp.cmd_source_list,
    "source status": sdp.cmd_source_status,
    "source error-logs": sdp.cmd_source_error_logs,
}


# ── fixture deployments ─────────────────────────────────────────────────────


class Project:
    """A fixture deployment, written file by file under a temp folder. Paths are relative to the project root."""

    def __init__(self, root):
        self.root = root
        (root / "config").mkdir(parents=True)

    def write(self, relative, text, mode=None):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        if mode is not None:
            os.chmod(path, mode)
        return path

    def env(self, *lines):
        """Write .env; `{root}` in a line stands for the project folder."""
        self.write(".env", "".join(line.format(root=self.root) + "\n" for line in lines))

    def database(self, name, text="auth: false\n"):
        self.write(f"config/db/{name}.yaml", text)

    def source(self, name, platform, profiles=()):
        self.write(f"config/sources/{name}/platform.yaml", platform)
        for profile in profiles:
            self.write(f"config/sources/{name}/{PROFILE_FILES[profile]}", "{}\n")

    def state(self, data_dir, stem, content):
        """Write a state file under the data folder of a database: a dict as JSON, a string as it is (for unreadable files)."""
        text = content if isinstance(content, str) else json.dumps(content)
        self.write(f"{data_dir}/state_tracking/{stem}.json", text)

    def credentials(self, data_dir, mode=0o600, text="made-up-read-only-value\n"):
        """Write the read-only credentials file of a database with an explicit mode: drift findings print the mode."""
        self.write(f"{data_dir}/.ro_credentials", text, mode=mode)


def _state(processed=(), failed=(), in_progress=None, last_updated=None):
    return {"processed": list(processed), "failed": list(failed), "in_progress": in_progress, "last_updated": last_updated}


def _failure(filename, error, timestamp="2024-03-05T10:11:12.123456"):
    return {"filename": filename, "error": error, "timestamp": timestamp}


PG = "data/database/postgres"
MONGO = "storage/mongo"
SR = "elsewhere/starrocks"
# The three ways a data path is written in .env: left out (the default applies), relative, absolute.
DATA_PATH_LINES = ("MONGO_DATA_PATH=./storage/mongo", "STARROCKS_DATA_PATH={root}/elsewhere/starrocks")


def build_empty(p):
    """Nothing configured."""


def build_postgres_plain(p):
    """PostgreSQL alone, no .env at all: every value shown is a default."""
    p.database("postgres")


def build_full(p):
    """Three databases with authentication, a read-only user and MCP users, all MCP servers, jobs with targets and a queue, an export path, and state files of several sources."""
    p.env(
        "POSTGRES_AUTH_ENABLED=true", "MONGO_AUTH_ENABLED=true", "STARROCKS_AUTH_ENABLED=true",
        "POSTGRES_USER=pgadmin", "MONGO_ADMIN_USER=mongoadmin",
        "POSTGRES_RO_USER=pg_reader", "MONGO_RO_USER=mongo_reader", "STARROCKS_RO_USER=sr_reader",
        "POSTGRES_MCP_USER=pg_mcp", "MONGO_MCP_USER=mongo_mcp", "STARROCKS_MCP_USER=sr_mcp",
        "PGDATA_PATH=./data/database/postgres", *DATA_PATH_LINES,
        "DB_NAME=research", "POSTGRES_PORT=5433", "MONGO_PORT=27018", "STARROCKS_PORT=9031", "STARROCKS_FE_HTTP_PORT=8031",
        "DB_EXPORT_PATH=/srv/export",
    )
    for name, data_dir in (("postgres", PG), ("mongo", MONGO), ("starrocks", SR)):
        p.database(name, "auth: true\nro_username: reader\n")
        p.credentials(data_dir)
    p.write("config/db/mcp.yaml", "postgres:\n  enabled: true\n  port: 8001\n  access_mode: restricted\nmongo:\n  enabled: true\n  port: 3001\nstarrocks:\n  enabled: true\n  port: 9001\n")
    p.write("config/jobs/config.yaml", "port: 8050\nmax_concurrent: 1\nauth: false\n")
    p.write("config/jobs/config.local.yaml", "port: 8055\nresult_root: /srv/jobs/results\nmax_concurrent: 3\nauth: true\ntargets:\n  pg_main:\n    backend: postgres\n  mongo_main:\n    backend: mongodb\n  sr_main:\n    backend: starrocks\n")
    for folder, count in (("pending", 2), ("approved", 1)):
        for index in range(count):
            p.write(f"data/jobs/{folder}/job{index}.json", "{}\n")
    p.write("config/postgres_ml/services.yaml", "classifiers:\n  toxic_roberta:\n    suffix: _toxicity\n  lingua: {}\n")

    p.source("reddit", "platform: reddit\ndata_types: [submissions, comments]\ndb_schema: reddit_data\npaths:\n  dumps: /mnt/dumps/reddit\n  parsed: ./data/parsed/reddit\n", ALL_PROFILES)
    p.source("twitter", "data_types: [tweets]\nmongo_db_name: tw_archive\n", ("parse", "postgres_ingest", "mongo_ingest"))
    p.source("forum", "platform: custom/forum\ndata_types: [posts]\nmongo_db_name_template: '{platform}__{data_type}'\n", ("parse", "mongo_ingest"))

    p.state(PG, "reddit_postgres_ingest_submissions", _state(["RS_2024-01", "RS_2024-02"], last_updated="2024-03-05T10:11:12.123456"))
    p.state(PG, "reddit_postgres_ingest_comments", _state([], [_failure("RC_2024-01", "COPY failed")], in_progress="RC_2024-02"))
    # Entries of two classifiers, one without a classifier prefix, and a failed entry that is a bare string.
    p.state(PG, "reddit_postgres_ml_comments", _state(
        ["toxic_roberta/RC_2024-01", "lingua/RC_2024-01", "toxic_roberta/RC_2024-02", "RC_legacy"],
        [_failure("toxic_roberta/RC_2024-03", "boom"), _failure("RC_orphan", "boom"), "lingua/RC_2024-02"],
        last_updated="2024-03-06T08:00:00",
    ))
    p.state(PG, "twitter_postgres_ingest_tweets", _state(["TW_2024"], last_updated="2024-03-07T09:30:00"))
    # A name without the usual label: the whole stem becomes both the source and the data type.
    p.state(PG, "adhoc", _state(["one"]))
    p.state(MONGO, "reddit_mongo_ingest_comments", _state(["RC_2024-01"], last_updated="2024-03-05T10:11:12"))
    p.state(MONGO, "twitter_mongo_ingest_tweets", _state(["TW_2024"]))
    p.state(MONGO, "forum_mongo_ingest_posts", _state(["P_1", "P_2"], [_failure("P_3", "boom")]))
    p.state(SR, "reddit_sr_ingest_comments", _state(["RC_2024-01"], in_progress="RC_2024-02", last_updated="2024-03-05T10:11:12"))
    p.state(SR, "reddit_sr_ml_comments", _state(["toxic_roberta/RC_2024-01"], last_updated="2024-03-06"))


def build_auth_mongo_only(p):
    """Three databases, authentication on for MongoDB alone, and a read-only user name left behind in the PostgreSQL key."""
    p.env("MONGO_AUTH_ENABLED=true", "POSTGRES_RO_USER=stale_pg_reader", "MONGO_RO_USER=mongo_reader", "STARROCKS_MCP_USER=sr_mcp", *DATA_PATH_LINES)
    p.database("postgres")
    p.database("mongo", "auth: true\nro_username: mongo_reader\n")
    p.database("starrocks")
    p.credentials(MONGO)
    # A credentials file of a database without authentication: it must not be listed.
    p.credentials(PG)


def build_postgres_auth_no_credentials(p):
    """PostgreSQL with authentication and a read-only user whose credentials file is missing: drift."""
    p.env("POSTGRES_AUTH_ENABLED=true", "POSTGRES_RO_USER=reader", "PGDATA_PATH=./data/database/postgres")
    p.database("postgres", "auth: true\nro_username: reader\n")


def build_mcp_open(p):
    """Three databases without authentication and every MCP server in its not-read-only form."""
    p.env(*DATA_PATH_LINES)
    for name in ("postgres", "mongo", "starrocks"):
        p.database(name)
    p.write("config/db/mcp.yaml", "postgres:\n  enabled: true\n  access_mode: unrestricted\nmongo:\n  enabled: true\n  port: 3005\n  read_only: false\nstarrocks:\n  enabled: true\n")


def build_jobs_bare(p):
    """StarRocks with authentication and no read-only user, a disabled MCP entry, and jobs configured without targets or queue folders."""
    p.env("STARROCKS_AUTH_ENABLED=true", "STARROCKS_DATA_PATH={root}/elsewhere/starrocks")
    p.database("starrocks", "auth: true\n")
    p.write("config/db/mcp.yaml", "starrocks:\n  enabled: false\n")
    p.write("config/jobs/config.local.yaml", "auth: false\n")


def build_state_unreadable(p):
    """State folders: every file unreadable (PostgreSQL), readable and unreadable mixed (MongoDB), an empty folder (StarRocks)."""
    p.env(*DATA_PATH_LINES)
    for name in ("postgres", "mongo", "starrocks"):
        p.database(name)
    p.state(PG, "reddit_postgres_ingest_comments", "{not json")
    p.state(PG, "reddit_postgres_ingest_submissions", "")
    p.state(MONGO, "reddit_mongo_ingest_comments", "{not json")
    p.state(MONGO, "reddit_mongo_ingest_submissions", _state(["RS_2024-01"]))
    (p.root / SR / "state_tracking").mkdir(parents=True)


def build_unreadable_platform(p):
    """Two databases and a source whose platform.yaml cannot be read, with state files in both."""
    p.env("MONGO_DATA_PATH=./storage/mongo")
    p.database("postgres")
    p.database("mongo")
    p.source("broken", BROKEN_YAML, ("postgres_ingest", "mongo_ingest"))
    p.source("fine", "data_types: [posts]\n", ("postgres_ingest",))
    p.state(PG, "broken_postgres_ingest_posts", _state(["P_1"]))
    p.state(PG, "broken_postgres_ingest_users", _state(["U_1"]))
    p.state(PG, "fine_postgres_ingest_posts", _state(["P_1"]))
    p.state(MONGO, "broken_mongo_ingest_posts", _state(["P_1"]))


def build_odd_yaml_types(p):
    """Values in YAML files that are not strings: a date as a MongoDB database name, a number as a schema, numbers and a date in the jobs and MCP config."""
    p.env("MONGO_DATA_PATH=./storage/mongo")
    p.database("postgres")
    p.database("mongo")
    p.write("config/db/mcp.yaml", "postgres:\n  enabled: true\n  port: '8000'\n  access_mode: 5\nmongo:\n  enabled: true\n  port: 3000.5\n  read_only: 0\n")
    p.write("config/jobs/config.local.yaml", "port: null\nresult_root: 2024-01-01\nmax_concurrent: [1, 2]\ntargets:\n  7:\n    backend: postgres\n  2024-02-02:\n    backend: mongodb\n  third: {}\n")
    p.source("numbers", "data_types: [posts]\ndb_schema: 7\nmongo_db_name: 2024-01-01\n", ("postgres_ingest", "mongo_ingest"))
    p.state(PG, "numbers_postgres_ingest_posts", _state(["P_1"]))
    p.state(MONGO, "numbers_mongo_ingest_posts", _state(["P_1"]))


def build_verify_coherent(p):
    """PostgreSQL and MongoDB without authentication and a source whose paths lie under the default parents: nothing to report."""
    p.env("MONGO_DATA_PATH=./storage/mongo")
    p.database("postgres")
    p.database("mongo")
    p.source("reddit", "data_types: [comments]\npaths:\n  parsed: ./data/parsed/reddit\n  output: ./data/output/reddit\n", ("parse", "postgres_ingest", "mongo_ingest"))


def build_verify_unreadable_platform(p):
    """The coherent deployment plus a source whose platform.yaml cannot be read."""
    build_verify_coherent(p)
    p.source("broken", BROKEN_YAML, ("postgres_ingest",))


def build_drift(p):
    """Drift of every kind: authentication flags that disagree, a missing credentials file, one with a loose mode, source mounts missing from and left over in the override file, an MCP server without credentials, a jobs target on an unknown backend and one on a database that is not configured."""
    p.env("MONGO_AUTH_ENABLED=true", "MONGO_RO_USER=mongo_reader", "PGDATA_PATH=./data/database/postgres", *DATA_PATH_LINES)
    p.database("postgres", "auth: true\nro_username: reader\n")
    p.database("mongo", "auth: true\nro_username: mongo_reader\n")
    p.credentials(MONGO, mode=0o644)
    p.write("config/db/mcp.yaml", "postgres:\n  enabled: true\nstarrocks:\n  enabled: true\n")
    p.write("config/jobs/config.local.yaml", "auth: false\ntargets:\n  old:\n    backend: sqlite\n  sr_main:\n    backend: starrocks\n  pg_main:\n    backend: postgres\n")
    p.write("docker-compose.override.yml", "services:\n  postgres:\n    volumes:\n      - /mnt/gone/parsed/removed:/data/parsed/removed:ro\n")
    p.source("reddit", "data_types: [comments]\npaths:\n  parsed: /mnt/ssd/parsed/reddit\n  output: ./data/output/reddit\n", ("parse", "postgres_ingest"))


def build_sources(p):
    """Sources of every shape for `source list` and `source status`: all profiles with state files, StarRocks ingestion only, MongoDB without state, no profile at all, a date as the platform, an empty and an unreadable platform.yaml, and a folder that is not a source."""
    p.env(*DATA_PATH_LINES)
    p.source("reddit", "platform: reddit\ndata_types: [submissions, comments]\npaths:\n  dumps: /mnt/dumps/reddit\n  parsed: ./data/parsed/reddit\n", ALL_PROFILES)
    p.source("twitter", "data_types: [tweets]\n", ("parse", "sr_ingest"))
    p.source("forum", "platform: custom/forum\ndata_types: [posts]\n", ("parse", "lingua", "mongo_ingest"))
    p.source("bare", "data_types: []\n")
    p.source("dated", "platform: 2024-01-01\ndata_types: [posts]\npaths:\n  output: 7\n", ("parse",))
    p.source("blank", "", ("postgres_ingest",))
    p.source("broken", BROKEN_YAML, ("postgres_ingest",))
    (p.root / "config" / "sources" / "not_a_source").mkdir()

    p.state(PG, "reddit_postgres_ingest_submissions", _state(["RS_2024-01", "RS_2024-02"], last_updated="2024-03-05T10:11:12.123456"))
    p.state(PG, "reddit_postgres_ingest_comments", _state([], [_failure("RC_2024-01", "COPY failed")], in_progress="RC_2024-02"))
    p.state(PG, "reddit_postgres_ml_comments", _state(["toxic_roberta/RC_2024-01"], last_updated="2024-03-06T08:00:00"))
    p.state(PG, "reddit_postgres_ingest_broken", "{not json")
    # Another source's file in the same folder: it must not show under reddit.
    p.state(PG, "twitter_postgres_ingest_tweets", _state(["TW_2024"]))
    p.state(MONGO, "reddit_mongo_ingest_comments", _state(["RC_2024-01"], [_failure("RC_2024-02", "boom"), _failure("RC_2024-03", "boom")]))
    p.state(SR, "reddit_sr_ingest_comments", _state(["RC_2024-01"], last_updated="2024-03-05"))
    p.state(SR, "twitter_sr_ingest_tweets", "{not json")


MONGOIMPORT_LOG_FIRST = """
============================================================
Timestamp: 2024-03-05T10:00:00.000001
Command: mongoimport --uri=mongodb://***@mongo:27017 --file /data/extracted/reddit/comments/RC_2024-01.ndjson
File: /data/extracted/reddit/comments/RC_2024-01.ndjson
============================================================
2024-03-05T10:00:01.000+0000	connected to: mongodb://mongo:27017
2024-03-05T10:00:09.000+0000	1000 document(s) imported successfully. 0 document(s) failed to import.

============================================================
Timestamp: 2024-03-05T10:01:00.000001
Command: mongoimport --uri=mongodb://***@mongo:27017 --file /data/extracted/reddit/comments/RC_2024-02.ndjson
File: /data/extracted/reddit/comments/RC_2024-02.ndjson
============================================================
2024-03-05T10:01:01.000+0000	connected to: mongodb://mongo:27017

2024-03-05T10:01:02.000+0000	Failed: error processing document #7: invalid character 'x' looking for beginning of value
2024-03-05T10:01:02.000+0000	0 document(s) imported successfully. 1 document(s) failed to import.

============================================================
Timestamp: 2024-03-05T10:02:00.000001
Command: mongoimport --uri=mongodb://***@mongo:27017 --file /tmp/RC_2024-03.ndjson
File: /tmp/RC_2024-03.ndjson
Converted from: /data/extracted/reddit/comments/RC_2024-03.parquet (parquet)
============================================================
2024-03-05T10:02:01.000+0000	Failed: error connecting to host
"""

MONGOIMPORT_LOG_SECOND = """
============================================================
Timestamp: 2024-03-06T11:00:00.000001
Command: mongoimport --uri=mongodb://***@mongo:27017 --file /data/extracted/reddit/comments/RC_2024-02.ndjson
File: /data/extracted/reddit/comments/RC_2024-02.ndjson
============================================================
"""

SEE_LOG = "mongoimport failed for /data/extracted/reddit/comments/{name} -> reddit_comments.comments. See log: /data/database/logs/mongoimport_reddit_comments_comments.log"


def build_errors(p):
    """Failed entries under every ingestion profile, mongoimport logs, a source without state files of its own (it shows no failures, none of another source's either) and one that cannot be read."""
    p.env(*DATA_PATH_LINES)
    p.source("reddit", "data_types: [submissions, comments]\n", ("postgres_ingest", "postgres_ml", "mongo_ingest", "sr_ingest", "sr_ml"))
    p.source("fresh", "data_types: [posts]\n", ("postgres_ingest",))
    p.source("twitter", "data_types: [tweets]\n", ("parse",))
    p.source("broken", BROKEN_YAML, ("postgres_ingest",))

    p.state(PG, "reddit_postgres_ingest_comments", _state(["RC_2024-01"], [_failure("RC_2024-02", "COPY failed: line 7\nDETAIL: unterminated quoted field"), {}]))
    p.state(PG, "reddit_postgres_ingest_submissions", _state(["RS_2024-01"]))
    p.state(PG, "reddit_postgres_ingest_broken", "{not json")
    p.state(PG, "reddit_postgres_ml_comments", _state([], [_failure("toxic_roberta/RC_2024-03", "missing column", timestamp="")]))
    p.state(MONGO, "reddit_mongo_ingest_comments", _state([], [_failure("RC_2024-02.ndjson", SEE_LOG.format(name="RC_2024-02.ndjson")), _failure("RC_2024-03.parquet", SEE_LOG.format(name="RC_2024-03.parquet"))]))
    p.state(SR, "reddit_sr_ingest_comments", _state([], [_failure("RC_2024-02", "load failed")]))
    p.state(SR, "reddit_sr_ml_comments", _state([], [_failure("toxic_roberta/RC_2024-02", "load failed")]))
    # Two log files hold a section for the failed NDJSON file; the Parquet file's section names the converted temp file, so it is not found.
    p.write(f"{MONGO}/logs/mongoimport_reddit_comments.log", MONGOIMPORT_LOG_FIRST)
    p.write(f"{MONGO}/logs/mongoimport_reddit_retry.log", MONGOIMPORT_LOG_SECOND)
    p.write(f"{MONGO}/logs/mongoimport_reddit_empty.log", "")


def build_errors_clean(p):
    """Sources whose state files hold no failed entry."""
    p.env(*DATA_PATH_LINES)
    p.source("reddit", "data_types: [comments]\n", ("postgres_ingest", "mongo_ingest", "sr_ingest"))
    p.source("twitter", "data_types: [tweets]\n", ("mongo_ingest",))
    p.state(PG, "reddit_postgres_ingest_comments", _state(["RC_2024-01"]))
    p.state(MONGO, "reddit_mongo_ingest_comments", _state(["RC_2024-01"]))


# The source name `../x` is joined to `config/sources/` as it is, so it addresses the folder `config/x`. These two profiles give that folder something to report.
OUTSIDE_PROFILES = ("postgres_ingest", "mongo_ingest")


def outside_folder(p):
    """A folder next to `config/sources/` that looks like a source: `config/x`, which the name `../x` reaches."""
    p.write("config/x/platform.yaml", "platform: outside\ndata_types: [posts]\n")
    for profile in OUTSIDE_PROFILES:
        p.write(f"config/x/{PROFILE_FILES[profile]}", "{}\n")


def build_sources_outside_folder(p):
    """The sources of `build_sources` plus the folder `config/x`."""
    build_sources(p)
    outside_folder(p)


def build_errors_outside_folder(p):
    """The failed entries of `build_errors` plus the folder `config/x`."""
    build_errors(p)
    outside_folder(p)


# ── scenarios ───────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Scenario:
    """One run of a command on a fixture deployment. `name` is also the golden file's name. `running` are the compose services reported as running; `containers` is what the container probe of `db verify` returns per database (absent: not running)."""

    name: str
    build: Callable
    command: str
    args: dict = field(default_factory=dict)
    exit_code: int = 0
    running: tuple = ()
    containers: dict = field(default_factory=dict)


def _status(name, build, verbose=False, running=()):
    return Scenario(name, build, "db status", {"verbose": verbose}, running=running)


def _verify(name, build, db=None, as_json=False, exit_code=0, containers=None):
    return Scenario(name, build, "db verify", {"db": db, "json": as_json}, exit_code=exit_code, running=tuple(containers or ()), containers=containers or {})


def _source_status(name, build, source, exit_code=0):
    return Scenario(name, build, "source status", {"name": source}, exit_code=exit_code)


def _error_logs(name, build, source, profile=None, exit_code=0):
    return Scenario(name, build, "source error-logs", {"name": source, "profile": profile}, exit_code=exit_code)


FULL_RUNNING = ("postgres", "starrocks", "mongo-mcp", "jobs")
HEALTHY = {"running": True, "healthy": True, "env_auth": False}
# PostgreSQL was started with authentication, which .env no longer has; MongoDB is unhealthy.
DRIFT_CONTAINERS = {"postgres": {"running": True, "healthy": True, "env_auth": True}, "mongo": {"running": True, "healthy": False, "env_auth": True}}

SCENARIOS = [
    _status("db_status_no_database", build_empty),
    _status("db_status_no_database_verbose", build_empty, verbose=True),
    _status("db_status_postgres_plain", build_postgres_plain),
    _status("db_status_postgres_plain_verbose", build_postgres_plain, verbose=True, running=("postgres",)),
    _status("db_status_full", build_full, running=FULL_RUNNING),
    _status("db_status_full_verbose", build_full, verbose=True, running=FULL_RUNNING),
    _status("db_status_auth_mongo_only", build_auth_mongo_only, running=("mongo",)),
    _status("db_status_postgres_auth_no_credentials", build_postgres_auth_no_credentials),
    _status("db_status_mcp_not_read_only", build_mcp_open, running=("postgres-mcp", "starrocks-mcp")),
    _status("db_status_jobs_without_targets", build_jobs_bare),
    _status("db_status_state_unreadable", build_state_unreadable),
    _status("db_status_state_unreadable_verbose", build_state_unreadable, verbose=True),
    _status("db_status_unreadable_platform", build_unreadable_platform),
    _status("db_status_unreadable_platform_verbose", build_unreadable_platform, verbose=True),
    _status("db_status_odd_yaml_types_verbose", build_odd_yaml_types, verbose=True, running=("jobs",)),
    _status("db_status_drift", build_drift),

    _verify("db_verify_no_database", build_empty),
    _verify("db_verify_no_database_json", build_empty, as_json=True),
    _verify("db_verify_coherent", build_verify_coherent, containers={"postgres": HEALTHY}),
    _verify("db_verify_coherent_json", build_verify_coherent, as_json=True, containers={"postgres": HEALTHY}),
    _verify("db_verify_drift", build_drift, exit_code=1, containers=DRIFT_CONTAINERS),
    _verify("db_verify_drift_json", build_drift, as_json=True, exit_code=1, containers=DRIFT_CONTAINERS),
    _verify("db_verify_drift_db_mongo", build_drift, db="mongo", exit_code=1, containers=DRIFT_CONTAINERS),
    _verify("db_verify_drift_db_postgres_json", build_drift, db="postgres", as_json=True, exit_code=1, containers=DRIFT_CONTAINERS),
    _verify("db_verify_coherent_db_mongo", build_verify_coherent, db="mongo"),
    _verify("db_verify_unreadable_platform", build_verify_unreadable_platform),
    _verify("db_verify_unreadable_platform_json", build_verify_unreadable_platform, as_json=True),

    Scenario("source_list_none", build_empty, "source list"),
    Scenario("source_list_several", build_sources, "source list"),

    _source_status("source_status_no_sources", build_empty, None),
    _source_status("source_status_all", build_sources, None),
    _source_status("source_status_empty_name_lists_all", build_sources, ""),
    _source_status("source_status_named", build_sources, "reddit"),
    _source_status("source_status_sr_ingest_only", build_sources, "twitter"),
    _source_status("source_status_no_state_files", build_sources, "forum"),
    _source_status("source_status_unreadable_platform", build_sources, "broken"),
    _source_status("source_status_unknown_name", build_sources, "nosuch"),
    _source_status("source_status_folder_without_platform", build_sources, "not_a_source"),
    # A name that is not one path component is not refused: it is joined to `config/sources/`. Without a `config/x/platform.yaml` the command says "not found"; with one it reports on a folder outside `config/sources/`. That is a known bug, filed separately, and pinned here as the behaviour of today.
    _source_status("source_status_parent_name_not_found", build_sources, "../x"),
    _source_status("source_status_parent_name_reads_outside_sources", build_sources_outside_folder, "../x"),

    _error_logs("error_logs_no_sources", build_empty, None),
    _error_logs("error_logs_none_for_named", build_errors_clean, "reddit"),
    _error_logs("error_logs_none_for_all", build_errors_clean, None),
    _error_logs("error_logs_all", build_errors, None),
    _error_logs("error_logs_empty_name_lists_all", build_errors, ""),
    _error_logs("error_logs_named", build_errors, "reddit"),
    _error_logs("error_logs_profile_postgres_ingest", build_errors, "reddit", profile="postgres_ingest"),
    _error_logs("error_logs_profile_mongo_ingest", build_errors, "reddit", profile="mongo_ingest"),
    _error_logs("error_logs_profile_not_configured", build_errors, "fresh", profile="mongo_ingest"),
    _error_logs("error_logs_source_without_state_files", build_errors, "fresh"),
    _error_logs("error_logs_unreadable_platform", build_errors, "broken"),
    _error_logs("error_logs_unknown_name", build_errors, "nosuch"),
    # The same known bug as in `source status`: `../x` is looked up as `config/x`, and when that folder has a `platform.yaml` the failures of other sources are shown under it.
    _error_logs("error_logs_parent_name_not_found", build_errors, "../x"),
    _error_logs("error_logs_parent_name_reads_outside_sources", build_errors_outside_folder, "../x"),
]


# ── running a scenario ──────────────────────────────────────────────────────


def point_at(monkeypatch, root):
    """Point the CLI and the setup helpers at the project in root."""
    config = root / "config"
    monkeypatch.setattr(sdp, "ROOT", root)
    monkeypatch.setattr(sdp, "CONFIG_DIR", config)
    monkeypatch.setattr(utils, "ROOT", root)
    monkeypatch.setattr(utils, "CONFIG_DIR", config)
    monkeypatch.setattr(utils, "SOURCES_DIR", config / "sources")


def compose_ps_output(services):
    """What `docker compose ps --format json` prints: one JSON object per line."""
    return "".join(json.dumps({"Service": service, "State": "running"}) + "\n" for service in services)


def stub_docker(monkeypatch, scenario):
    """Answer the one subprocess `db status` runs, refuse every other one, and replace the container probes of `db verify`."""
    def run(argv, **kwargs):
        assert list(argv[:3]) == ["docker", "compose", "ps"] and "--format" in argv, f"unexpected subprocess: {argv}"
        return subprocess.CompletedProcess(argv, 0, stdout=compose_ps_output(scenario.running), stderr="")

    monkeypatch.setattr(sdp.subprocess, "run", run)
    monkeypatch.setattr(sdp, "_running_services", lambda: set(scenario.running))
    monkeypatch.setattr(sdp, "_probe_container_state", lambda service: scenario.containers.get(service))


def deploy(scenario, tmp_path, monkeypatch):
    """Build the scenario's deployment under tmp_path, point the CLI at it and stub Docker; returns the project folder."""
    root = tmp_path.resolve() / "project"
    scenario.build(Project(root))
    point_at(monkeypatch, root)
    stub_docker(monkeypatch, scenario)
    return root


def broken_yaml_message():
    """The message the installed PyYAML gives for BROKEN_YAML, as it appears in the warning about an unreadable platform.yaml."""
    try:
        yaml.safe_load(BROKEN_YAML)
    except yaml.YAMLError as exc:
        return str(exc)
    raise AssertionError("BROKEN_YAML must not parse")


def normalise(text, root):
    """Replace the two parts of the text that depend on the run: the temp project folder and the PyYAML message."""
    return text.replace(str(root), ROOT_PLACEHOLDER).replace(broken_yaml_message(), YAML_ERROR_PLACEHOLDER)


def golden_path(name):
    return GOLDEN_DIR / f"{name}.txt"


def check_golden(name, text):
    """Compare text with the golden file byte for byte, or write the file when the regenerate variable is set."""
    path = golden_path(name)
    if os.environ.get(REGENERATE_VARIABLE) == "1":
        path.parent.mkdir(exist_ok=True)
        path.write_bytes(text.encode("utf-8"))
        return
    assert path.exists(), f"no golden file for {name}: run with {REGENERATE_VARIABLE}=1 and review the new file"
    assert text == path.read_bytes().decode("utf-8")


SCENARIO_PARAMS = [pytest.param(scenario, id=scenario.name) for scenario in SCENARIOS]


# ── tests ───────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("scenario", SCENARIO_PARAMS)
def test_command_prints_the_golden_text(scenario, tmp_path, monkeypatch, capsys):
    """Bug class: a change to a read command that alters what the terminal shows without anyone deciding it: a line lost, reordered, reworded or re-indented, a warning printed once more or once less or at another point, a default or a label changed, or another exit code. The whole text is compared, so the position of every line counts."""
    root = deploy(scenario, tmp_path, monkeypatch)

    exit_code = COMMANDS[scenario.command](argparse.Namespace(**scenario.args))

    captured = capsys.readouterr()
    assert captured.err == ""
    assert exit_code == scenario.exit_code
    check_golden(scenario.name, normalise(captured.out, root))


def test_every_golden_file_belongs_to_a_scenario():
    """Bug class: a golden file nothing compares against any more (its scenario renamed or removed), which then reads as pinned behaviour while pinning nothing; and two scenarios writing the same file."""
    names = [scenario.name for scenario in SCENARIOS]

    assert len(names) == len(set(names))
    assert sorted(path.name for path in GOLDEN_DIR.iterdir()) == sorted(f"{name}.txt" for name in names)


def test_goldens_hold_no_run_dependent_text():
    """Bug class: a golden that passes only on the machine that wrote it, because the temp folder or the PyYAML message was not replaced; and a placeholder that no golden uses, which would mean the scenarios meant to print a host path or the warning no longer do."""
    texts = {path.name: path.read_bytes().decode("utf-8") for path in GOLDEN_DIR.iterdir()}

    assert [name for name, text in texts.items() if "/pytest-" in text or broken_yaml_message() in text] == []
    assert any(ROOT_PLACEHOLDER in text for text in texts.values())
    assert any(YAML_ERROR_PLACEHOLDER in text for text in texts.values())
