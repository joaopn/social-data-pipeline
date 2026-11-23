#!/usr/bin/env python3
"""Social Data Pipeline CLI.

Unified entrypoint for database management, source configuration, and pipeline execution.

Usage:
    sdp.py db setup                          Configure databases (PostgreSQL, MongoDB)
    sdp.py db setup --add <db>               Add a single database to existing setup
    sdp.py db settings [--json]              Show the current answers of db setup (never a password)
    sdp.py db setup-mcp                      Configure MCP servers for databases
    sdp.py db setup-jobs                     Configure the query scheduler
    sdp.py db setup-llm                      Bundle: setup-mcp then setup-jobs
    sdp.py db restart-needed                 List the running services whose settings changed after they started
    sdp.py db llm-settings                   Show the current answers of setup-mcp and setup-jobs
    sdp.py db start [service]                Start services (postgres|mongo|starrocks|postgres-mcp|mongo-mcp|starrocks-mcp|all)
    sdp.py db stop [service]                 Stop services (postgres|mongo|starrocks|postgres-mcp|mongo-mcp|starrocks-mcp|all)
    sdp.py db status                         Show database config and health
    sdp.py db unsetup                        Remove database config (and optionally data)
    sdp.py db reset --db <name>              Wipe one database's data (keeps config)
    sdp.py db unsetup-mcp                    Remove MCP configuration
    sdp.py db recover-password               Reset database admin password
    sdp.py db create-indexes [--source <name>] Interactively create database indexes
    sdp.py db index-targets --source <name> --db <db>  List the tables, columns and indexes create-indexes offers

    sdp.py source add <name> [--hf ID]        Add a new source (--hf for HF datasets)
    sdp.py source download <name>            Download HF dataset files
    sdp.py source hf-inspect <org/dataset>   Show an HF dataset's schema groups
    sdp.py source configure <name>           Reconfigure existing source (platform-specific)
    sdp.py source add-classifiers <name>     Add ML classifiers for a source
    sdp.py source settings <name> [--json]   Show the current answers of source configure (never a secret)
    sdp.py source remove <name>              Remove source config
    sdp.py source list                       List configured sources
    sdp.py source status [name]              Show source processing/ingestion status
    sdp.py source progress [name] [--json]   Show how many files each processing stage has finished
    sdp.py source size [name] [--json]       Show how much disk space each source's stage folders take
    sdp.py source error-logs [name]          Show database ingestion error logs

    sdp.py run <profile> [--source <name>]   Run pipeline for a source (--build to rebuild, --filter to select files)
    sdp.py runs list [--limit N] [--json]    List the pipeline runs that are alive now, and the recorded ones
    sdp.py runs logs <run_id> [--follow]     Print the output of a run (--tail for its last lines)
    sdp.py runs cancel <run_id>              Cancel a run by sending its container the interrupt signal

    sdp.py broker start [--foreground]       Start the broker, the host process that serves the web UI's socket
    sdp.py broker stop [--force]             Stop the broker (--force ends the action that runs instead of waiting)
    sdp.py broker status                     Say whether the broker runs, and since when
    sdp.py broker passwd                     Set the broker's management password
    sdp.py broker audit [--limit N] [--verify] [--json]  Print the newest lines of the broker's audit log, or check its chain

    sdp.py webui setup                       Set up the web UI: its port, addresses, names and certificate; build its image
    sdp.py webui start                       Start the web UI's container (the broker must run for it to log in)
    sdp.py webui stop                        Stop and remove the web UI's container
    sdp.py webui rebuild                     Rebuild the web UI's image from the saved settings and restart it (no questions)
    sdp.py webui status                      Say whether the web UI runs, where it answers and its certificate's fingerprint
    sdp.py webui unsetup                     Remove the web UI's container, network, image and settings

    sdp.py chat setup                        Set up the Chat window: its back end, the model's credential (asked without echo), the work folder's cap; build its two images
    sdp.py chat start                        Start the chat's gateway and sandbox and attach the databases whose MCP server is on a read-only user
    sdp.py chat stop                         Stop and remove the chat's containers and networks, and detach the databases
    sdp.py chat rebuild                      Rebuild the chat's images from the saved settings and restart it (no questions)
    sdp.py chat status                       Say whether the chat runs, which databases it can read now and the size of its work folder
    sdp.py chat settings                     Show the chat's settings for the whole deployment (the read-only query limit)
    sdp.py chat query-limit [<seconds>]      Show or set the read-only query limit of the chat's MongoDB and StarRocks MCP servers
    sdp.py chat auto-title [on|off]          Show or set whether a new chat gets a short title made by the chat's model (on by default)
    sdp.py chat unsetup                      Remove the chat's images, settings and credential (and, if asked, all chats)

    sdp.py setup                             Legacy: full interactive setup (core + classifiers + platform)
"""

import argparse
import errno
import json
import math
import os
import re
import secrets
import shutil
import stat
import subprocess
import sys
import unicodedata
from datetime import datetime, timezone
from getpass import getpass
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CONFIG_DIR = ROOT / "config"

VALID_PROFILES = ["parse", "lingua", "ml", "postgres_ingest", "postgres_ml", "mongo_ingest", "sr_ingest", "sr_ml"]

# Map profile names to docker compose service names (only where they differ)
PROFILE_SERVICE_MAP = {
    "ml": "ml-gpu",
    "postgres_ingest": "postgres-ingest",
    "postgres_ml": "postgres-ml",
    "mongo_ingest": "mongo-ingest",
    "sr_ingest": "sr-ingest",
    "sr_ml": "sr-ml",
}

# Per-source override files that belong to each DB. Used by `db unsetup` to
# clean up source dirs and by `cmd_run` for source-override gating.
_DB_TO_SOURCE_FILES = {
    "postgres":  ["postgres.yaml", "postgres_ml.yaml"],
    "mongo":     ["mongo.yaml"],
    "starrocks": ["starrocks.yaml", "sr_ml.yaml"],
}

# Profile → required DB (or None for profiles with no DB dependency).
_PROFILE_DB = {
    "parse":           None,
    "lingua":          None,
    "ml":              None,
    "postgres_ingest": "postgres",
    "postgres_ml":     "postgres",
    "mongo_ingest":    "mongo",
    "sr_ingest":       "starrocks",
    "sr_ml":           "starrocks",
}

# Profile → expected source override filename (sans .yaml). Note the deliberate
# name collisions (postgres_ingest writes postgres.yaml, sr_ingest writes
# starrocks.yaml) — see social_data_pipeline/setup/source.py.
_PROFILE_SOURCE_FILE = {
    "parse":           "parse",
    "lingua":          "lingua",
    "ml":              "ml",
    "postgres_ingest": "postgres",
    "postgres_ml":     "postgres_ml",
    "mongo_ingest":    "mongo",
    "sr_ingest":       "starrocks",
    "sr_ml":           "sr_ml",
}


# ============================================================================
# Helpers
# ============================================================================

def load_env():
    """Load .env file into a dict (does not modify os.environ)."""
    env_path = ROOT / ".env"
    if not env_path.exists():
        return {}
    env = {}
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, value = line.partition("=")
            env[key.strip()] = value.strip()
    return env


def docker_compose(*args):
    """Run docker compose with the given arguments.

    Ensures the password env vars exist (empty when auth is disabled) so
    Compose doesn't emit `WARN[0000] The "X" variable is not set` for
    `${POSTGRES_PASSWORD:-}` style references in the compose file.
    """
    for var in ("POSTGRES_PASSWORD", "MONGO_ADMIN_PASSWORD", "STARROCKS_ROOT_PASSWORD"):
        os.environ.setdefault(var, "")
    cmd = ["docker", "compose"] + list(args)
    print(f"  $ {' '.join(cmd)}")
    return subprocess.run(cmd, cwd=ROOT)


def _delete_source_db_overrides(db_names):
    """Delete per-source DB override files for the given DBs.

    For each `config/sources/<name>/` directory, unlink the override files
    associated with the named DB(s) (e.g. postgres.yaml, postgres_ml.yaml).
    Prints each path removed under a single header. Returns the count.
    """
    sources_dir = CONFIG_DIR / "sources"
    if not sources_dir.is_dir():
        return 0
    targets = []
    for db in db_names:
        targets.extend(_DB_TO_SOURCE_FILES.get(db, []))
    if not targets:
        return 0
    removed_paths = []
    for src_dir in sorted(sources_dir.iterdir()):
        if not src_dir.is_dir():
            continue
        for fname in targets:
            f = src_dir / fname
            if f.exists():
                f.unlink()
                removed_paths.append(f.relative_to(ROOT))
    if removed_paths:
        print("  Removed source overrides:")
        for p in removed_paths:
            print(f"    {p}")
    return len(removed_paths)


def _get_configured_db_services():
    """Determine which database services are configured."""
    db_dir = CONFIG_DIR / "db"
    services = []
    if (db_dir / "postgres.yaml").exists():
        services.append("postgres")
    if (db_dir / "mongo.yaml").exists():
        services.append("mongo")
    if (db_dir / "starrocks.yaml").exists():
        services.append("starrocks")
    return services


def _get_configured_mcp_services():
    """Determine which MCP services are configured."""
    mcp_path = CONFIG_DIR / "db" / "mcp.yaml"
    if not mcp_path.exists():
        return []
    try:
        import yaml
        config = yaml.safe_load(mcp_path.read_text()) or {}
    except Exception:
        return []
    services = []
    if config.get("postgres", {}).get("enabled"):
        services.append("postgres_mcp")
    if config.get("mongo", {}).get("enabled"):
        services.append("mongo_mcp")
    if config.get("starrocks", {}).get("enabled"):
        services.append("starrocks_mcp")
    return services


def _load_mcp_config():
    """Load MCP config from config/db/mcp.yaml. Returns dict or empty."""
    mcp_path = CONFIG_DIR / "db" / "mcp.yaml"
    if not mcp_path.exists():
        return {}
    try:
        import yaml
        return yaml.safe_load(mcp_path.read_text()) or {}
    except Exception:
        return {}


def _is_auth_enabled():
    """Check if database authentication is enabled via .env."""
    env = load_env()
    return (env.get("POSTGRES_AUTH_ENABLED") == "true"
            or env.get("MONGO_AUTH_ENABLED") == "true"
            or env.get("STARROCKS_AUTH_ENABLED") == "true")


def _is_jobs_configured():
    """Check if the query scheduler (jobs profile) is configured.

    Keyed on the existence of the user's local config. The default
    config.yaml is a committed template and its presence alone doesn't
    indicate that the user has run `sdp db setup-jobs`.
    """
    return (CONFIG_DIR / "jobs" / "config.local.yaml").exists()


def _load_jobs_config():
    """Return the merged (default + local) jobs config, or empty dict."""
    import yaml
    from social_data_pipeline.core.config import deep_merge
    default_path = CONFIG_DIR / "jobs" / "config.yaml"
    local_path = CONFIG_DIR / "jobs" / "config.local.yaml"
    merged: dict = {}
    for path in (default_path, local_path):
        if not path.exists():
            continue
        try:
            data = yaml.safe_load(path.read_text()) or {}
        except Exception:
            continue
        merged = deep_merge(merged, data)
    return merged


def _is_jobs_auth_enabled() -> bool:
    """True when the jobs scheduler has `auth: true` in its config."""
    return bool(_load_jobs_config().get("auth", False))


def _needs_admin_password() -> bool:
    """True when a DB admin password must be prompted before starting any
    service: either a DB has auth on, or the jobs UI has auth on (which
    reuses the same admin password from the process env)."""
    return _is_auth_enabled() or _is_jobs_auth_enabled()


def _load_db_yaml(name):
    """Load a config/db/<name>.yaml file. Returns dict or empty."""
    path = CONFIG_DIR / "db" / f"{name}.yaml"
    if not path.exists():
        return {}
    try:
        import yaml
        return yaml.safe_load(path.read_text()) or {}
    except Exception:
        return {}


def _prompt_db_password(label="Database admin password", tag=None):
    """Prompt for the database admin password. Returns the password string."""
    prefix = ""
    if tag and os.environ.get('SDP_TAGGED_MODE'):
        prefix = f"[{tag}] "
    pw = getpass(f"  {prefix}{label}: ")
    if not pw:
        print("  Error: Password cannot be empty.")
        sys.exit(1)
    return pw


def _set_auth_env(password):
    """Set authentication env vars for docker compose subprocess."""
    os.environ["POSTGRES_PASSWORD"] = password
    os.environ["MONGO_ADMIN_PASSWORD"] = password
    os.environ["STARROCKS_ROOT_PASSWORD"] = password


class ActionError(Exception):
    """An action function (`do_*`) was called without an input it needs on the path it took, and stopped before changing anything; or a function that returns data (`get_*`) was called with a source name that is not one path component, or with a database or profile it does not know, and stopped before reading anything. `message` is one line that holds no input value; `exit_code` is the code a command returns for it."""

    def __init__(self, message, exit_code=1):
        super().__init__(message)
        self.message = message
        self.exit_code = exit_code


_ADMIN_PASSWORD_REQUIRED = "the database admin password is required"


def already_confirmed(prompt):
    """Confirmation callback for a caller that authenticated the operator and took the confirmation another way: every question is answered yes. An action proceeds only on exactly True, so a callback that returns anything else (the typed answer, None) is a no."""
    return True


def _confirm_on_terminal(prompt):
    """Confirmation callback of the terminal commands: ask the y/N question and return whether the answer is yes."""
    return input(prompt).strip().lower() in ("y", "yes")


def _require_plain_password(admin_password):
    """Refuse an admin password that is not a plain string. A caller holding a wrapped secret must unwrap it first: passed on as it is, it would be ignored where no password is needed and fail late where one is."""
    if admin_password is not None and not isinstance(admin_password, str):
        raise TypeError("admin_password must be a plain string")


def _resolve_admin_password(admin_password, _ask_password):
    """Return the admin password for an action that needs one: the password passed in, else the answer of the terminal callback. Raises ActionError when there is neither, so an action never waits on a terminal it does not have."""
    if admin_password:
        return admin_password
    if _ask_password is not None:
        return _ask_password()
    raise ActionError(_ADMIN_PASSWORD_REQUIRED)


_DB_DATA_PATH_ENV = {
    "postgres": "PGDATA_PATH",
    "mongo": "MONGO_DATA_PATH",
    "starrocks": "STARROCKS_DATA_PATH",
}

# Jobs scheduler backend names → the db name `_get_configured_db_services`
# uses. Mongo is the odd one out: jobs config calls it "mongodb" but the
# DB service is "mongo".
_DB_TO_JOBS_BACKEND = {
    "postgres": "postgres",
    "mongo": "mongodb",
    "starrocks": "starrocks",
}

# Canonical display names used by `db status` / `source status` output.
# Keyed by the various names a backend goes by in the codebase: the
# `configured_dbs` short name, the `db_type` arg passed to ingestion-state
# helpers (which uses the profile name for the SR variants), and the jobs
# config `backend` string. Single source of truth — using `.title()` would
# yield "Sr_Ingest" / "Mongodb" which is wrong.
_DB_DISPLAY = {
    "postgres":  "PostgreSQL",
    "mongo":     "MongoDB",
    "mongodb":   "MongoDB",
    "starrocks": "StarRocks",
    "sr_ingest": "StarRocks",
    "sr_ml":     "StarRocks ML",
}


def _read_sr_storage_paths():
    """Read StarRocks extra-disk paths from config/db/starrocks.yaml.

    Returns a dict ``{storage_0: host_path, storage_1: ...}`` mirroring the
    container-side mount layout used by `db setup`. Empty dict when the SR
    yaml is missing, malformed, or has no `storage_paths` list — callers
    treat that as "no extra disks to clean up," not a failure.
    """
    sr_db_config = CONFIG_DIR / "db" / "starrocks.yaml"
    if not sr_db_config.exists():
        return {}
    try:
        import yaml
        cfg = yaml.safe_load(sr_db_config.read_text()) or {}
    except Exception:
        return {}
    paths = cfg.get("storage_paths")
    if not isinstance(paths, list):
        return {}
    return {
        f"storage_{i}": p
        for i, p in enumerate(paths)
        if isinstance(p, str) and p
    }


def _orphaned_jobs_targets_for(db_name):
    """Names of jobs scheduler targets pointing at the DB being removed.

    Used by `_unsetup_single_db` to warn before tearing down a DB whose
    targets are still wired into the jobs config — those routes will fail
    on the next jobs run otherwise.
    """
    if not _is_jobs_configured():
        return []
    target_backend = _DB_TO_JOBS_BACKEND.get(db_name)
    if target_backend is None:
        return []
    cfg = _load_jobs_config()
    return [
        tname
        for tname, spec in (cfg.get("targets") or {}).items()
        if (spec or {}).get("backend") == target_backend
    ]


def _regenerate_ro_credentials_for(target_dbs, password=None):
    """Generate (or use given) RO password and write `.ro_credentials` to each
    target DB's data path atomically (mode 0600). Returns the password.

    Raises:
        ConfigurationError: when a target DB's data path env var is missing
            or empty in `.env`, or when the atomic write fails. No silent
            fallback to a default path — drift between `.env` and the actual
            data location must surface here, not write `.ro_credentials`
            into the wrong directory.
    """
    from social_data_pipeline.core.config import ConfigurationError
    from social_data_pipeline.setup.db import _write_ro_password_for

    env = load_env()
    new_pw = password or secrets.token_urlsafe(24)
    for db in target_dbs:
        env_key = _DB_DATA_PATH_ENV[db]
        data_path = env.get(env_key)
        if not data_path:
            raise ConfigurationError(
                f"Missing {env_key} in .env — cannot write .ro_credentials for {db}. "
                f"Re-run 'sdp db setup --add {db}' to repair the .env."
            )
        cred_path = _write_ro_password_for(db, data_path, new_pw)
        print(f"  Written: {cred_path} (chmod 600)")
    return new_pw


# DB container image used by `_chown_db_data_for_host`. Each DB writes its
# data dir as a non-host uid (postgres / mongodb / starrocks inside the
# container), so we re-chown via a container with the matching image before
# any host-side `shutil.rmtree`.
_DB_CHOWN_IMAGE = {
    "postgres": "postgres:18",
    "mongo": "mongo:8",
    "starrocks": "starrocks/allin1-ubuntu",
}


def _chown_db_data_for_host(db_name, data_path, extra_paths):
    """Chown a DB's data + extra-storage tree to the host uid/gid via a throwaway container.

    Required before host-side ``shutil.rmtree`` because the DB container
    writes data files as a non-host uid. Mounts and chowns:
      - the parent of ``data_path`` recursively (covers the data dir itself
        plus host-side siblings like ``.ro_credentials`` and ``state_tracking/``)
      - each ``extra_paths`` entry recursively (its contents)
      - each ``extra_paths`` entry's parent non-recursively, so the entry
        directory itself can be unlinked / its contents removed without
        recursing into sibling dirs that may share the same disk

    Args:
        db_name: ``"postgres"`` | ``"mongo"`` | ``"starrocks"`` — selects the
            container image.
        data_path: resolved ``Path`` to the DB data dir (e.g. ``PGDATA_PATH``).
        extra_paths: ``{name: host_path}`` for PG tablespaces or SR
            ``storage_paths``. May be empty.

    Returns:
        ``[(name, resolved_path), ...]`` for every ``extra_paths`` entry that
        existed on disk, in iteration order. Callers use this to drive a
        post-chown rmtree / wipe pass without having to re-resolve paths.
    """
    chown_image = _DB_CHOWN_IMAGE[db_name]
    parent = data_path.resolve().parent
    volume_args = ["-v", f"{parent}:/dbparent"]
    recursive = ["/dbparent"]
    non_recursive = []
    extra_mounts = []
    for name, host_path in extra_paths.items():
        ts_dir = Path(host_path)
        if not ts_dir.is_absolute():
            ts_dir = ROOT / ts_dir
        if not ts_dir.exists():
            continue
        ts_resolved = ts_dir.resolve()
        volume_args += ["-v", f"{ts_resolved}:/tablespace/{name}"]
        recursive.append(f"/tablespace/{name}")
        ts_parent_mount = f"/tsparent_{name}"
        volume_args += ["-v", f"{ts_resolved.parent}:{ts_parent_mount}"]
        non_recursive.append(ts_parent_mount)
        extra_mounts.append((name, ts_resolved))
    uid_gid = f"{os.getuid()}:{os.getgid()}"
    chown_cmd = f"chown -R {uid_gid} {' '.join(recursive)}"
    if non_recursive:
        chown_cmd += f" && chown {uid_gid} {' '.join(non_recursive)}"
    subprocess.run(
        ["docker", "run", "--rm"] + volume_args +
        [chown_image, "sh", "-c", chown_cmd],
        cwd=ROOT,
    )
    return extra_mounts


# ============================================================================
# sdp db setup
# ============================================================================

def cmd_db_setup(args):
    """Run interactive database configuration."""
    from social_data_pipeline.setup.db import main as db_main
    add = getattr(args, "add", None)
    if add:
        from social_data_pipeline.setup.db import add_database
        add_database(add)
    else:
        db_main()
    return 0


def do_db_setup(*, add=None, databases=None, data_path=None, export_path=None,
                pgdata_path=None, db_name=None, pg_port=None, tablespaces=None, filesystem=None, pgtune=None, pg_mem_limit=None,
                mongo_data_path=None, mongo_port=None, mongo_cache_size_gb=None, mongo_mem_limit=None, mongo_validate=None,
                starrocks_data_path=None, starrocks_port=None, starrocks_fe_http_port=None, sr_fe_jvm_heap=None, starrocks_mem_limit=None,
                sr_be_mem_limit=None, sr_alter_tablet_workers=None, sr_compression=None, starrocks_storage_paths=None,
                keep_tablespaces=None, keep_storage_paths=None, keep_pgtune=None, auth=None, new_password=None, new_password_confirm=None, ro_user=None, ro_username=None,
                ro_password_mode=None, ro_password=None, ro_password_confirm=None):
    """`db setup` with its answers given all at once: without `add` its questionnaire, the first time or over the databases set up already (their current values are then the defaults, and `keep_*` and the read-only password's `keep` are its "Keep existing ...?" answers), with `add` one database not set up yet added as `db setup --add` adds it. An answer left out is the terminal's Enter. Writes exactly the files and .env keys the terminal writes for the same answers (`setup.db_answers`); a data path changed moves no data. Refused, before anything is written: answers that do not belong together or that the setup as it is makes wrong (`control.dbsetup.check` with the current setup: a database set up left out, authentication switched off, nothing to keep, an add of a database set up), a port another service of the deployment uses. The admin password is checked against its confirmation and dropped, as the terminal drops it. Asks nothing; returns the exit code."""
    from social_data_pipeline.control import dbsetup
    from social_data_pipeline.core.config import ConfigurationError
    from social_data_pipeline.setup import db as db_setup
    from social_data_pipeline.setup import db_answers
    from social_data_pipeline.setup.utils import written_path

    given = {key: value for key, value in {
        "add": add, "databases": databases, "data_path": data_path, "export_path": export_path,
        "pgdata_path": pgdata_path, "db_name": db_name, "pg_port": pg_port, "tablespaces": tablespaces, "filesystem": filesystem, "pgtune": pgtune, "pg_mem_limit": pg_mem_limit,
        "mongo_data_path": mongo_data_path, "mongo_port": mongo_port, "mongo_cache_size_gb": mongo_cache_size_gb, "mongo_mem_limit": mongo_mem_limit, "mongo_validate": mongo_validate,
        "starrocks_data_path": starrocks_data_path, "starrocks_port": starrocks_port, "starrocks_fe_http_port": starrocks_fe_http_port, "sr_fe_jvm_heap": sr_fe_jvm_heap,
        "starrocks_mem_limit": starrocks_mem_limit, "sr_be_mem_limit": sr_be_mem_limit, "sr_alter_tablet_workers": sr_alter_tablet_workers, "sr_compression": sr_compression,
        "starrocks_storage_paths": starrocks_storage_paths, "keep_tablespaces": keep_tablespaces, "keep_storage_paths": keep_storage_paths, "keep_pgtune": keep_pgtune, "auth": auth, "new_password": new_password, "new_password_confirm": new_password_confirm, "ro_user": ro_user,
        "ro_username": ro_username, "ro_password_mode": ro_password_mode, "ro_password": ro_password, "ro_password_confirm": ro_password_confirm,
    }.items() if value is not None}
    for secret in ("new_password", "new_password_confirm", "ro_password", "ro_password_confirm"):
        if secret in given and not isinstance(given[secret], str):
            raise TypeError(f"{secret} must be a plain string")
    configured = _get_configured_db_services()
    existing = db_setup._load_existing_db_config()
    try:
        dbsetup.check(given, db_answers.current_of(existing, configured))
    except dbsetup.Refused as refused:
        raise ActionError(f"db setup: {refused.field} {refused}.") from None
    for key in dbsetup.PATH_FIELDS:
        if key in given:
            given[key] = written_path(given[key])
    if "tablespaces" in given:
        given["tablespaces"] = [{"name": item["name"], "path": written_path(item["path"])} for item in given["tablespaces"]]
    if "starrocks_storage_paths" in given:
        given["starrocks_storage_paths"] = [written_path(path) for path in given["starrocks_storage_paths"]]

    chosen = dbsetup.chosen(given)
    changing = add is None and bool(configured)

    env = load_env()
    ram, cores = db_answers.machine()
    try:
        if add is None:
            settings = db_answers.first_time_settings(given, ram, cores, existing)
        else:
            settings = db_answers.add_settings(add, given, ram, cores, existing, env)
    except db_answers.SetupRefused as refused:
        raise ActionError(refused.message) from None

    # The ports the answers come to (defaults included) against each other and against the deployment's other services; a database being set up does not count its own old ports.
    used = _ports_in_use(leave_out=set(chosen))
    ports = {field: settings[field] for field in dbsetup.PORT_FIELDS if field in settings}
    for field, port in ports.items():
        if port in used:
            raise ActionError(f"db setup: {field} {port} is used by {_SERVICE_TITLES[used[port]]} already.")
        if list(ports.values()).count(port) > 1:
            raise ActionError(f"db setup: {field} {port} is given to two services of this setup.")

    try:
        if add is None:
            files = db_answers.first_time_files(settings)
            if changing:
                print("  Changing the setup of: " + ", ".join(settings["databases"]), flush=True)
            else:
                print("  Setting up: " + ", ".join(settings["databases"]), flush=True)
            db_answers.write_first_time(settings, files)
        else:
            files, env_updates, volumes = db_answers.added_files(add, settings)
            print(f"  Adding: {add}", flush=True)
            db_answers.write_added(add, settings, files, env_updates, volumes)
    except db_answers.SetupRefused as refused:
        raise ActionError(refused.message) from None
    except ConfigurationError as error:
        # `_write_ro_password_for` could write no `.ro_credentials`: the other files are written by then.
        raise ActionError(f"The read-only password file could not be written: {error}") from None

    print("\n  Done! The database configuration has been written.", flush=True)
    for name in configured if changing else ():
        key = db_setup.DB_DATA_PATH_KEYS[name]
        if existing.get(key) and settings.get(key) != existing[key]:
            print(f"  The data path of {name} is now {settings[key]}: no data was moved, so {name} starts empty there; its old data stays in {existing[key]}.", flush=True)
    if settings.get("auth_enabled"):
        print("  The admin password was not stored anywhere: give it when the database starts (python sdp.py db start).", flush=True)
        if settings.get("ro_username"):
            print(f"  Read-only user: {settings['ro_username']} (its password is in .ro_credentials in the database's data folder)", flush=True)
    if changing:
        # As after the MCP servers' and the scheduler's setup: which running services must restart to use what was written (`db restart-needed`, Needs attention on the Overview). Nothing is restarted.
        _print_restart_after_setup()
    else:
        print("  Next: python sdp.py db start" + (f" {chosen[0]}" if len(chosen) == 1 else ""), flush=True)
    return 0


# ============================================================================
# sdp db settings
# ============================================================================

def cmd_db_settings(args):
    """Show the current answers of `db setup` for the databases set up."""
    data = get_db_settings()
    if getattr(args, "json", False):
        print(json.dumps(data, indent=2))
    else:
        print_db_settings(data)
    return 1 if data["not_readable"] else 0


def get_db_settings():
    """The current value of each question of `db setup` for the databases set up, which `db setup` would offer as its defaults (`setup.db_answers.settings_read`): `configured`, `not_readable`, `values`, `auth`, `ro_username`, `ro_password_set`, `tablespaces`, `storage_paths`, `pgtune`, `pgtune_set`. Never a password: whether the read-only one can be kept is a flag. Prints nothing; never raises on a damaged file."""
    from social_data_pipeline.setup import db_answers
    return db_answers.settings_read()


def print_db_settings(data):
    """Print the text of `db settings` from the data of `get_db_settings`."""
    print()
    if data["not_readable"]:
        print(f"  Error: {data['not_readable']}.")
        print()
        return
    if not data["configured"]:
        print("  No database is set up. Set one up with: python sdp.py db setup")
        print()
        return
    print("  The answers 'db setup' takes as its defaults (Enter keeps each):")
    print()
    for name, value in data["values"].items():
        print(f"    {name:26s} {_display_safe(str(value))}")
    for item in data["tablespaces"]:
        print(f"    {'tablespace':26s} {_display_safe(item['name'])}: {_display_safe(item['path'])}")
    for path in data["storage_paths"]:
        print(f"    {'extra storage path':26s} {_display_safe(path)}")
    if "postgres" in data["configured"]:
        print(f"    {'PGTune output':26s} {'set' if data['pgtune_set'] else 'none'}")
    print(f"    {'authentication':26s} {'on' if data['auth'] else 'off'}")
    if data["ro_username"]:
        print(f"    {'read-only user':26s} {_display_safe(data['ro_username'])} (password {'set' if data['ro_password_set'] else 'not found'})")
    print()


# ============================================================================
# sdp db setup-mcp / unsetup-mcp
# ============================================================================

def cmd_db_setup_mcp(args):
    """Configure MCP servers for databases."""
    from social_data_pipeline.setup.mcp import main as mcp_main
    mcp_main()
    return 0


def cmd_db_setup_jobs(args):
    """Configure the query scheduler (jobs profile)."""
    from social_data_pipeline.setup.jobs import main as jobs_main
    jobs_main()
    return 0


def cmd_db_setup_llm(args):
    """Configure both MCP servers and the jobs scheduler in one go."""
    from social_data_pipeline.setup.mcp import main as mcp_main
    from social_data_pipeline.setup.jobs import main as jobs_main
    mcp_main()
    jobs_main()
    return 0


# ============================================================================
# sdp db setup-mcp / setup-jobs without their questions, for the broker
# ============================================================================

# The host ports of each service, as docker-compose.yml publishes them: the `.env` key of each and its default there (`${KEY:-default}`); StarRocks's backend HTTP port has no key (None).
_SERVICE_PORT_KEYS = (
    ("postgres", (("POSTGRES_PORT", "5432"),)),
    ("mongo", (("MONGO_PORT", "27017"),)),
    ("starrocks", (("STARROCKS_PORT", "9030"), ("STARROCKS_FE_HTTP_PORT", "8030"), (None, "8040"))),
    ("postgres-mcp", (("POSTGRES_MCP_PORT", "8000"),)),
    ("mongo-mcp", (("MONGO_MCP_PORT", "3000"),)),
    ("starrocks-mcp", (("STARROCKS_MCP_PORT", "9000"),)),
    ("jobs", (("JOBS_PORT", "8050"),)),
)
_SERVICE_TITLES = {
    "postgres": "PostgreSQL", "mongo": "MongoDB", "starrocks": "StarRocks",
    "postgres-mcp": "the PostgreSQL MCP server", "mongo-mcp": "the MongoDB MCP server", "starrocks-mcp": "the StarRocks MCP server",
    "jobs": "the query scheduler", "webui": "the web UI",
}


def _sdp_service_ports():
    """{service: set of host ports} of the deployment's services, the one reading of the ports in use for every setup from the browser: each service this deployment has set up, read as `db start` and the compose file read them (each database with a config/db/<db>.yaml, each MCP server enabled in config/db/mcp.yaml and the scheduler when it is configured), with their ports from .env or the compose file's defaults (StarRocks's backend 8040 included); every other service whose port key is set in .env, with the port it names there (a service not set up, or set up in part, still claims it); and the web UI (`webui`) when it is set up, with the port of its settings. A value that is no whole number is left out: it cannot be the same port as a whole number."""
    env = load_env()
    configured = set(_get_configured_db_services())
    configured |= {profile.replace("_", "-") for profile in _get_configured_mcp_services()}
    if _is_jobs_configured():
        configured.add("jobs")
    ports = {}
    for service, keys in _SERVICE_PORT_KEYS:
        found = set()
        for key, default in keys:
            if service in configured:
                text = (env.get(key) or default) if key else default
            else:
                text = (env.get(key) or "") if key else ""
            if isinstance(text, str) and text.isdigit() and len(text) <= 5:
                found.add(int(text))
        if service in configured or found:
            ports[service] = found
    try:
        saved = _webui_host().read_config(str(ROOT))
    except Exception:
        saved = None
    if saved is not None:
        ports["webui"] = {saved[0].port}
    return ports


def _ports_in_use(leave_out=()):
    """{port: service} of every port in use (`_sdp_service_ports`), the services in `leave_out` (those being set up, which keep or change their own ports) not counted; a port two services name is given to the first of them."""
    taken = {}
    for service, found in _sdp_service_ports().items():
        if service in leave_out:
            continue
        for port in sorted(found):
            taken.setdefault(port, service)
    return taken


def _refuse_ports_in_use(chosen):
    """Refuse a port chosen for a service (`chosen`: {service: [ports]}, the services being set up, a service switched off with none) that another service of the deployment uses, or that two of the chosen ones share. The services being set up are compared by their new ports only, so a service keeps its own current port. Raises ActionError naming both services."""
    taken = _ports_in_use(leave_out=set(chosen))
    for service, found in chosen.items():
        for port in found:
            if port in taken:
                raise ActionError(f"the port chosen for {_SERVICE_TITLES[service]} is already used by {_SERVICE_TITLES[taken[port]]}: choose another one")
            taken[port] = service


def _refuse_unconfigured(answers, databases):
    for name, answer in answers:
        if answer is not None and name not in databases:
            raise ActionError(f"{_DB_DISPLAY[name]} is not configured: run `python sdp.py db setup` first")


def do_db_setup_mcp(*, postgres=None, mongo=None, starrocks=None):
    """`db setup-mcp` with its answers given: a server given (`{"port", "write_access"}`; StarRocks has no write access question) is switched on, one left out is switched off, as the questionnaire's "Enable ...?" answered no. Writes config/db/mcp.yaml and the .env keys exactly as the terminal does for the same answers, with the terminal's refusals (no database, a database with authentication and no read-only user, a missing read-only credentials file) and its "Nothing to write" when every server is off. Refuses besides, before anything is written, a server of a database that is not configured and a port another service of the deployment uses (the terminal checks neither). Restarts nothing. Asks nothing; returns the exit code."""
    from social_data_pipeline.setup import mcp as mcp_setup
    from social_data_pipeline.setup.utils import load_db_setup, update_env_file, write_files

    db_setup = load_db_setup()
    if not db_setup or not db_setup.get("databases"):
        print("  Error: No databases configured. Run first: python sdp.py db setup")
        return 1
    refusal = _mcp_setup_refusal(mcp_setup, db_setup)
    if refusal is not None:
        for line in refusal:
            print(line)
        return 1
    databases = db_setup["databases"]
    _refuse_unconfigured((("postgres", postgres), ("mongo", mongo), ("starrocks", starrocks)), databases)

    # What the questionnaire returns for these answers (setup/mcp.py run_questionnaire).
    settings = {}
    if "postgres" in databases:
        settings["postgres_mcp_enabled"] = postgres is not None
        if postgres is not None:
            settings["postgres_mcp_port"] = postgres["port"]
            settings["postgres_mcp_access_mode"] = "unrestricted" if postgres.get("write_access") else "restricted"
    if "mongo" in databases:
        settings["mongo_mcp_enabled"] = mongo is not None
        if mongo is not None:
            settings["mongo_mcp_port"] = mongo["port"]
            settings["mongo_mcp_read_only"] = not mongo.get("write_access", False)
    if "starrocks" in databases:
        settings["starrocks_mcp_enabled"] = starrocks is not None
        if starrocks is not None:
            settings["starrocks_mcp_port"] = starrocks["port"]
    for db in ("postgres", "mongo", "starrocks"):
        settings[f"{db}_auth"] = db_setup.get(f"{db}_auth", False)
        settings[f"{db}_ro_username"] = db_setup.get(f"{db}_ro_username")

    _refuse_ports_in_use({f"{db}-mcp": [answer["port"]] if answer is not None else [] for db, answer in (("postgres", postgres), ("mongo", mongo), ("starrocks", starrocks))})

    if not settings.get("postgres_mcp_enabled") and not settings.get("mongo_mcp_enabled") and not settings.get("starrocks_mcp_enabled"):
        print("\n  No MCP servers enabled. Nothing to write.\n")
        return 0

    # The write step of setup/mcp.py main, line for line.
    print()
    write_files([(mcp_setup.CONFIG_DIR / "db" / "mcp.yaml", mcp_setup.generate_mcp_yaml(settings))])
    env_updates = {}
    if settings.get("postgres_mcp_enabled"):
        env_updates["POSTGRES_MCP_PORT"] = str(settings["postgres_mcp_port"])
        env_updates["POSTGRES_MCP_ACCESS_MODE"] = settings["postgres_mcp_access_mode"]
        if settings.get("postgres_auth") and settings.get("postgres_ro_username"):
            env_updates["POSTGRES_MCP_USER"] = settings["postgres_ro_username"]
    if settings.get("mongo_mcp_enabled"):
        env_updates["MONGO_MCP_PORT"] = str(settings["mongo_mcp_port"])
        env_updates["MONGO_MCP_READ_ONLY"] = str(settings["mongo_mcp_read_only"]).lower()
        if settings.get("mongo_auth") and settings.get("mongo_ro_username"):
            env_updates["MONGO_MCP_USER"] = settings["mongo_ro_username"]
    if settings.get("starrocks_mcp_enabled"):
        env_updates["STARROCKS_MCP_PORT"] = str(settings["starrocks_mcp_port"])
        if settings.get("starrocks_auth") and settings.get("starrocks_ro_username"):
            env_updates["STARROCKS_MCP_USER"] = settings["starrocks_ro_username"]
    update_env_file(env_updates)
    print("  Updated:   .env")
    print("\n  Done! MCP servers have been configured.")
    _print_restart_after_setup()
    return 0


def _mcp_setup_refusal(mcp_setup, db_setup):
    """The lines `db setup-mcp` prints before it refuses for a database with authentication (setup/mcp.py main): no read-only user, or a read-only credentials file missing; None when it goes on."""
    if not (db_setup.get("postgres_auth") or db_setup.get("mongo_auth") or db_setup.get("starrocks_auth")):
        return None
    missing_users = [title for db, title in (("postgres", "PostgreSQL"), ("mongo", "MongoDB"), ("starrocks", "StarRocks")) if db_setup.get(f"{db}_auth") and not db_setup.get(f"{db}_ro_username")]
    if missing_users:
        return [
            f"  Error: No read-only user configured for: {', '.join(missing_users)}",
            "  MCP servers require a read-only database user.",
            "  Re-run: python sdp.py db setup  (enable authentication with a read-only user)",
        ]
    missing_files = mcp_setup._missing_ro_cred_files(db_setup, mcp_setup._load_env_vars())
    if missing_files:
        return [
            "  Error: RO credential file(s) missing for enabled databases:",
            *[f"    - {db}: {path}" for db, path in missing_files],
            "",
            "  MCP servers read these files at startup. Regenerate them with:",
            "    python sdp.py db recover-password --regenerate-ro",
        ]
    return None


_JOBS_AUTH_OFF_REFUSED = "the scheduler page's login is on, and it is switched off only in a terminal: run `python sdp.py db setup-jobs` there"
_RESULT_ROOT_REFUSED = "the results folder must not start with '-', end or start with white space, or hold ':', '$', '#', a quote or a backslash (it goes into .env and into a mount of docker-compose.override.yml)"
_RESULT_ROOT_BAD = (":", "$", "#", '"', "'", "`", "\\")
_RESULT_ROOT_HOLDS_JOBS = "the results folder must not be data/jobs, a folder that holds it, or inside data/jobs/control: the scheduler keeps its control socket closed then (the database servers can write into the results folder), and the Queue window does not work"


def _result_root_text(path):
    """The results folder as the files take it: refused when it starts with '-', has white space at an end or holds a character that breaks .env or a compose mount; a relative one written with its leading `./` (a compose mount source without it is the name of a volume, not a folder). Raises ActionError."""
    from social_data_pipeline.setup.utils import written_path

    if path.startswith("-") or path != path.strip() or any(character in path for character in _RESULT_ROOT_BAD):
        raise ActionError(_RESULT_ROOT_REFUSED)
    return written_path(path)


def do_db_setup_jobs(*, port, result_root, max_concurrent, history_retention, postgres_target=None, starrocks_target=None, mongo_target=None, auth_enabled=False):
    """`db setup-jobs` with its answers given: a target given (`{"name", "database", "timeout_hours"}` for PostgreSQL, `{"name", "timeout_hours"}` for StarRocks and MongoDB) is one the operator added, one left out is "Add a ... target?" answered no. Writes config/jobs/config.local.yaml, JOBS_PORT and JOBS_RESULT_ROOT in .env, docker-compose.override.yml and StarRocks's fe.local.conf exactly as the terminal does for the same answers (setup/jobs.py main), and makes the results folder and data/jobs, with the terminal's refusals (no database, no target). Refuses besides, before anything is written: a target of a database that is not configured, the scheduler page's login switched off while it is on (that stays the terminal's), a results folder that would break .env or a mount, and a port another service of the deployment uses. Restarts nothing. Asks nothing; returns the exit code."""
    from social_data_pipeline.setup import jobs as jobs_setup
    from social_data_pipeline.setup.utils import load_db_setup, update_env_file, write_files

    db_setup = load_db_setup()
    if not db_setup or not db_setup.get("databases"):
        print("  Error: No databases configured. Run first: python sdp.py db setup")
        return 1
    eligible = set(db_setup.get("databases", [])) & {"postgres", "starrocks", "mongo"}
    if not eligible:
        print("  Error: jobs scheduler requires postgres, starrocks, or mongo configured.")
        print("  Run `python sdp.py db setup` first.\n")
        return 1
    _refuse_unconfigured((("postgres", postgres_target), ("starrocks", starrocks_target), ("mongo", mongo_target)), eligible)
    existing = jobs_setup._load_existing_jobs_config()
    if isinstance(existing, dict) and bool(existing.get("auth", False)) and not auth_enabled:
        raise ActionError(_JOBS_AUTH_OFF_REFUSED)
    root_text = _result_root_text(result_root)
    # The scheduler's own rule for its control socket (jobs/door.py), by resolved path: the broker refuses what would keep it closed.
    results_real = os.path.realpath(root_text if root_text.startswith("/") else jobs_setup.ROOT / root_text)
    jobs_real = os.path.realpath(jobs_setup.ROOT / "data" / "jobs")
    if os.path.commonpath([results_real, jobs_real]) == results_real or os.path.commonpath([results_real, os.path.join(jobs_real, "control")]) == os.path.join(jobs_real, "control"):
        raise ActionError(_RESULT_ROOT_HOLDS_JOBS)

    # What the questionnaire returns for these answers (setup/jobs.py run_questionnaire), in its order.
    targets = {}
    if postgres_target is not None:
        targets[postgres_target["name"]] = {"backend": "postgres", "database": postgres_target["database"]}
    if starrocks_target is not None:
        targets[starrocks_target["name"]] = {"backend": "starrocks", "database": ""}
    if mongo_target is not None:
        targets[mongo_target["name"]] = {"backend": "mongodb", "database": ""}
    if not targets:
        print("\n  Error: at least one target must be configured.\n")
        return 1
    backends_seen = sorted({target["backend"] for target in targets.values()})
    timeouts = {}
    for backend, target in (("postgres", postgres_target), ("starrocks", starrocks_target), ("mongodb", mongo_target)):
        if backend in backends_seen:
            timeouts[backend] = target["timeout_hours"] * 3600
    settings = {
        "port": port, "result_root": root_text, "max_concurrent": max_concurrent, "history_retention": history_retention,
        "default_timeouts": timeouts, "auth_enabled": bool(auth_enabled), "targets": targets, "_backends": backends_seen,
    }

    _refuse_ports_in_use({"jobs": [port]})

    if auth_enabled and not (db_setup.get("postgres_auth") or db_setup.get("mongo_auth") or db_setup.get("starrocks_auth")):
        print()
        print("    Warning: no database has authentication enabled. The")
        print("    jobs container will refuse to start with UI auth on")
        print("    unless `sdp db setup` enables auth on at least one DB,")
        print("    so the admin password can flow into the container at")
        print("    `sdp db start` time.")

    # The write step of setup/jobs.py main, line for line.
    files_to_write = [(jobs_setup.CONFIG_DIR / "jobs" / "config.local.yaml", jobs_setup.generate_jobs_yaml(settings))]
    override_content, _ = jobs_setup.compute_override_update(settings)
    sr_fe_applicable = "starrocks" in settings["_backends"] and (jobs_setup.CONFIG_DIR / "starrocks" / "fe.local.conf").exists()
    print()
    write_files(files_to_write)
    update_env_file({"JOBS_PORT": str(settings["port"]), "JOBS_RESULT_ROOT": settings["result_root"]})
    print("  Updated:   .env")
    (jobs_setup.ROOT / "docker-compose.override.yml").write_text(override_content)
    print("  Updated:   docker-compose.override.yml")
    if sr_fe_applicable and jobs_setup.enable_starrocks_outfile_local():
        print("  Updated:   config/starrocks/fe.local.conf")
    folder = Path(root_text) if root_text.startswith("/") else jobs_setup.ROOT / root_text
    folder.mkdir(parents=True, exist_ok=True)
    print(f"  Ensured:   {root_text}/")
    (jobs_setup.ROOT / "data" / "jobs").mkdir(parents=True, exist_ok=True)
    print()
    print("  Done! Jobs scheduler has been configured.")
    _print_restart_after_setup()
    return 0


# ============================================================================
# sdp db restart-needed / llm-settings
# ============================================================================

# The files each service reads its settings from: .env (which docker-compose.yml interpolates into every service), the compose override (PostgreSQL's and StarRocks's setup and data mounts), each database's config/db/<db>.yaml (which `db start` reads: its data path, the setup's choices), the servers' own local configuration files, the MCP settings and the scheduler's. They are the files `db setup` writes, the first time and over databases set up already.
_RESTART_SETTINGS = (
    ("postgres", (".env", "docker-compose.override.yml", "config/db/postgres.yaml", "config/postgres/postgresql.local.conf", "config/postgres/pg_hba.local.conf")),
    ("mongo", (".env", "config/db/mongo.yaml")),
    ("starrocks", (".env", "docker-compose.override.yml", "config/db/starrocks.yaml", "config/starrocks/fe.local.conf", "config/starrocks/be.local.conf")),
    ("postgres-mcp", (".env", "config/db/mcp.yaml")),
    ("mongo-mcp", (".env", "config/db/mcp.yaml")),
    ("starrocks-mcp", (".env", "config/db/mcp.yaml")),
    ("jobs", (".env", "config/jobs/config.local.yaml")),
)
_CONTAINER_ID = r"[0-9a-f]{12,64}"
# One line of the `docker inspect` format below: the compose service and the start time, which Docker writes in UTC with up to nine digits of the second.
_STARTED_LINE = r"([a-z0-9][a-z0-9_.-]{0,63}) ([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(?:\.([0-9]{1,9}))?Z"
_STARTED_FORMAT = '{{index .Config.Labels "com.docker.compose.service"}} {{.State.StartedAt}}'


def _running_started_at():
    """{compose service: when its oldest running container started, in nanoseconds since the epoch} of this deployment, or None when Docker cannot be asked. Only the label and the start time are asked of `docker inspect` (never its whole answer, which holds the containers' environment); a line of another shape is left out."""
    try:
        listed = subprocess.run(["docker", "compose", "ps", "-q", "--filter", "status=running"], capture_output=True, text=True, cwd=ROOT)
        if listed.returncode != 0:
            return None
        ids = [line.strip() for line in listed.stdout.splitlines() if re.fullmatch(_CONTAINER_ID, line.strip())]
        if not ids:
            return {}
        inspected = subprocess.run(["docker", "inspect", "--format", _STARTED_FORMAT, *ids], capture_output=True, text=True, cwd=ROOT)
    except OSError:
        return None
    if inspected.returncode != 0:
        return None
    started = {}
    for line in inspected.stdout.splitlines():
        match = re.fullmatch(_STARTED_LINE, line.strip())
        if match is None:
            continue
        try:
            seconds = int(datetime.strptime(match.group(2), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp())
        except (ValueError, OverflowError):
            continue
        when = seconds * 1_000_000_000 + int((match.group(3) or "0").ljust(9, "0"))
        started[match.group(1)] = min(started.get(match.group(1), when), when)
    return started


def _restart_needed():
    """What `db restart-needed` reports: `docker_available`, and `services`, one entry per running service (in the order of `db start`'s choices) one of whose settings files (_RESTART_SETTINGS) was modified after its container started: `service`, `started` and `files` (each `file`, relative to the deployment, and `modified`), times as ISO-8601 UTC seconds. A file is compared as a whole: a change to any line of .env counts for every service."""
    started = _running_started_at()
    if started is None:
        return {"docker_available": False, "services": []}
    services = []
    for service, files in _RESTART_SETTINGS:
        when = started.get(service)
        if when is None:
            continue
        newer = []
        for relative in files:
            try:
                modified = (ROOT / relative).stat().st_mtime_ns
            except OSError:
                continue
            if modified > when:
                newer.append({"file": relative, "modified": _iso_utc_seconds(modified)})
        if newer:
            services.append({"service": service, "started": _iso_utc_seconds(when), "files": newer})
    return {"docker_available": True, "services": services}


def _print_restart_after_setup():
    """The end of a setup action: which running services must restart to use what it wrote. Nothing is restarted."""
    data = _restart_needed()
    print()
    if not data["docker_available"]:
        print("  Nothing was restarted. Docker cannot be asked which running services must restart to apply this.")
    elif data["services"]:
        print(f"  Nothing was restarted. Restart to apply: {', '.join(entry['service'] for entry in data['services'])}.")
    else:
        print("  Nothing was restarted, and no running service must restart: the services set up here start with these settings.")


def cmd_db_restart_needed(args):
    print_db_restart_needed(get_db_restart_needed())
    return 0


def get_db_restart_needed():
    """Which running services' settings files are newer than their containers (see `_restart_needed`). Prints nothing; JSON-serialisable."""
    return _restart_needed()


def print_db_restart_needed(data):
    print()
    if not data["docker_available"]:
        print("  Docker cannot be asked: which services must restart is not known.\n")
        return
    if not data["services"]:
        print("  No running service has settings newer than its start.\n")
        return
    print("  Restart to apply: these services run with settings older than their files.")
    for entry in data["services"]:
        files = ", ".join(f"{item['file']} ({item['modified']})" for item in entry["files"])
        print(f"    {entry['service']:14s} started {entry['started']}; newer: {files}")
    print()
    print("  Restart each with: python sdp.py db stop <service> && python sdp.py db start <service>")
    print()


def _whole(value, lo, hi):
    """A value of a settings file as a whole number from lo to hi, or None."""
    return value if isinstance(value, int) and not isinstance(value, bool) and lo <= value <= hi else None


def _plain_text(value, max_chars=4096):
    """A value of a settings file as text fit to send (no control or format character), or None."""
    if not isinstance(value, str) or len(value) > max_chars or any(unicodedata.category(character) in _DISPLAY_UNSAFE_CATEGORIES for character in value):
        return None
    return value


def cmd_db_llm_settings(args):
    print_db_llm_settings(get_db_llm_settings())
    return 0


def get_db_llm_settings():
    """The answers `db setup-mcp` and `db setup-jobs` would offer as their defaults now, and whether `db setup-mcp` would refuse. Prints nothing; JSON-serialisable; never raises on a damaged settings file (its values are then left out, None).

    `databases` (configured); `mcp`: `refusal` (the lines the terminal prints when it refuses, else None) and `servers` ({db: `enabled`, `port`, `write_access`} for each configured database; StarRocks without write access); `jobs`: `configured`, `port`, `result_root`, `max_concurrent`, `history_retention`, `auth_enabled` and `targets` ({db: `add`, `name`, `timeout_hours` and for PostgreSQL `database`} per configured database). No password, no user name, no credentials file's content."""
    from social_data_pipeline.setup import jobs as jobs_setup
    from social_data_pipeline.setup import mcp as mcp_setup
    from social_data_pipeline.setup.utils import load_db_setup

    try:
        db_setup = load_db_setup() or {}
    except Exception:
        db_setup = {}
    databases = [db for db in ("postgres", "mongo", "starrocks") if db in (db_setup.get("databases") or [])]
    data = {"databases": databases, "mcp": {"refusal": None, "servers": {}}, "jobs": None}
    if not databases:
        data["mcp"]["refusal"] = ["  Error: No databases configured. Run first: python sdp.py db setup"]
        return data
    try:
        data["mcp"]["refusal"] = _mcp_setup_refusal(mcp_setup, db_setup)
    except Exception:
        data["mcp"]["refusal"] = ["  Error: the read-only credentials could not be checked."]

    try:
        existing = mcp_setup._load_existing_mcp_config()
    except Exception:
        existing = {}
    defaults = {"postgres": 8000, "mongo": 3000, "starrocks": 9000}
    for db in databases:
        server = {
            "enabled": bool(existing.get(f"{db}_mcp_enabled", True)),
            "port": _whole(existing.get(f"{db}_mcp_port", defaults[db]), 1, 65535),
        }
        if db == "postgres":
            server["write_access"] = existing.get("postgres_mcp_access_mode") == "unrestricted"
        elif db == "mongo":
            server["write_access"] = existing.get("mongo_mcp_read_only") is False
        data["mcp"]["servers"][db] = server

    data["jobs"] = _jobs_defaults(jobs_setup, databases)
    return data


def _jobs_defaults(jobs_setup, databases):
    """The defaults of `db setup-jobs` (setup/jobs.py run_questionnaire) for the configured databases; a value of the local config of another shape is None."""
    try:
        existing = jobs_setup._load_existing_jobs_config()
    except Exception:
        existing = {}
    if not isinstance(existing, dict):
        existing = {}
    jobs = {
        "configured": _is_jobs_configured(),
        "port": _whole(existing.get("port", 8050), 1, 65535),
        "result_root": _plain_text(existing.get("result_root") or jobs_setup.DEFAULT_RESULT_ROOT),
        "max_concurrent": _whole(existing.get("max_concurrent", 20), 0, 10 ** 9),
        "history_retention": _whole(existing.get("history_retention", 500), 0, 10 ** 9),
        "auth_enabled": bool(existing.get("auth", False)),
        "targets": {},
    }
    targets = existing.get("targets") if isinstance(existing.get("targets"), dict) else {}
    pg_db_default = load_env().get("DB_NAME") or "datasets"
    for db, backend, fallback_name in (("postgres", "postgres", "pg_main"), ("starrocks", "starrocks", "sr_main"), ("mongo", "mongodb", "mongo_main")):
        if db not in databases:
            continue
        of_backend = [name for name, spec in targets.items() if isinstance(spec, dict) and spec.get("backend") == backend]
        name = of_backend[0] if of_backend else fallback_name
        try:
            if db == "starrocks":
                hours = (jobs_setup._existing_timeout_seconds(existing, "starrocks", 72 * 3600) // 3600) or 72
                hours = max(1, min(hours, 72))
            else:
                hours = jobs_setup._existing_timeout_seconds(existing, backend, 0) // 3600
        except Exception:
            hours = None
        entry = {"add": True, "name": _plain_text(name, 256), "timeout_hours": _whole(hours, 0, 10 ** 9)}
        if db == "postgres":
            spec = targets.get(name) if isinstance(targets.get(name), dict) else {}
            entry["database"] = _plain_text(spec.get("database") or pg_db_default, 256)
        jobs["targets"][db] = entry
    return jobs


def print_db_llm_settings(data):
    print()
    if not data["databases"]:
        print("  No databases configured. Run first: python sdp.py db setup\n")
        return
    print("  MCP servers (the defaults of python sdp.py db setup-mcp):")
    if data["mcp"]["refusal"]:
        print("    setup-mcp refuses now:")
        for line in data["mcp"]["refusal"]:
            print(f"  {line}")
    for db, server in data["mcp"]["servers"].items():
        write = f", write access {'yes' if server['write_access'] else 'no'}" if "write_access" in server else ""
        print(f"    {db:10s} {'on' if server['enabled'] else 'off'}, port {server['port']}{write}")
    jobs = data["jobs"]
    print()
    print(f"  Query scheduler (the defaults of python sdp.py db setup-jobs; {'configured' if jobs['configured'] else 'not configured yet'}):")
    print(f"    Port {jobs['port']}, results folder {jobs['result_root']}, {jobs['max_concurrent']} queries at once, {jobs['history_retention']} jobs of history, login {'on' if jobs['auth_enabled'] else 'off'}")
    for db, target in jobs["targets"].items():
        database = f", database {target['database']}" if "database" in target else ""
        print(f"    Target {target['name']} on {db}{database}, timeout {target['timeout_hours']} hours")
    print()


def cmd_db_unsetup_jobs(args):
    """Remove jobs scheduler configuration and stop its container."""
    return do_db_unsetup_jobs()


def do_db_unsetup_jobs():
    """Remove jobs scheduler configuration and stop its container. Asks nothing; returns the exit code."""
    jobs_local = CONFIG_DIR / "jobs" / "config.local.yaml"
    if not jobs_local.exists():
        print("\n  No jobs configuration found.\n")
        return 0

    print("  Stopping jobs container...")
    docker_compose("--profile", "jobs", "down")

    jobs_local.unlink()
    print("  Removed:   config/jobs/config.local.yaml")

    # Remove JOBS_* env vars from .env
    env_path = ROOT / ".env"
    if env_path.exists():
        jobs_keys = {"JOBS_PORT", "JOBS_RESULT_ROOT"}
        lines = env_path.read_text().splitlines()
        new_lines = []
        for line in lines:
            stripped = line.lstrip("# ").strip()
            key = stripped.split("=", 1)[0] if "=" in stripped else stripped
            if key not in jobs_keys:
                new_lines.append(line)
        env_path.write_text("\n".join(new_lines) + "\n")
        print("  Updated:   .env")

    # Strip /jobs_export mounts from docker-compose.override.yml
    override_path = ROOT / "docker-compose.override.yml"
    if override_path.exists():
        try:
            import yaml
            data = yaml.safe_load(override_path.read_text()) or {}
        except yaml.YAMLError:
            data = {}
        changed = False
        services = (data.get("services") or {})
        for svc in ("postgres", "starrocks"):
            block = services.get(svc) or {}
            volumes = block.get("volumes") or []
            kept = [v for v in volumes if ":/jobs_export" not in str(v)]
            if len(kept) != len(volumes):
                changed = True
                if kept:
                    block["volumes"] = kept
                else:
                    block.pop("volumes", None)
                if not block:
                    services.pop(svc, None)
                else:
                    services[svc] = block
        if changed:
            data["services"] = services
            header = (
                "# Auto-generated by sdp — volume mounts for database servers.\n"
                "# Setup mounts (tablespaces, SR storage) + per-source data mounts from sdp db start.\n"
                "\n"
            )
            body = yaml.dump(data, default_flow_style=False, sort_keys=False)
            override_path.write_text(header + body)
            print("  Updated:   docker-compose.override.yml (removed /jobs_export mounts)")

    print("\n  Jobs configuration removed. Databases are not affected.\n")
    return 0


def cmd_db_unsetup_mcp(args):
    """Remove MCP configuration and stop MCP containers."""
    return do_db_unsetup_mcp()


def do_db_unsetup_mcp():
    """Remove MCP configuration and stop MCP containers. Asks nothing; returns the exit code."""
    mcp_path = CONFIG_DIR / "db" / "mcp.yaml"
    if not mcp_path.exists():
        print("\n  No MCP configuration found.\n")
        return 0

    # Stop MCP containers
    mcp_services = _get_configured_mcp_services()
    if mcp_services:
        configured = _get_configured_db_services()
        compose_args = []
        for svc in configured:
            mcp_profile = f"{svc}_mcp"
            if mcp_profile in mcp_services:
                compose_args += ["--profile", svc, "--profile", mcp_profile]
        if compose_args:
            print("  Stopping MCP containers...")
            docker_compose(*compose_args, "down")

    # Remove config file
    mcp_path.unlink()
    print("  Removed:   config/db/mcp.yaml")

    # Remove MCP env vars from .env
    env_path = ROOT / ".env"
    if env_path.exists():
        mcp_keys = {"POSTGRES_MCP_PORT", "POSTGRES_MCP_ACCESS_MODE",
                     "MONGO_MCP_PORT", "MONGO_MCP_READ_ONLY",
                     "STARROCKS_MCP_PORT",
                     "POSTGRES_MCP_USER", "MONGO_MCP_USER", "STARROCKS_MCP_USER"}
        lines = env_path.read_text().splitlines()
        new_lines = []
        for line in lines:
            stripped = line.lstrip("# ").strip()
            key = stripped.split("=", 1)[0] if "=" in stripped else stripped
            if key not in mcp_keys:
                new_lines.append(line)
        env_path.write_text("\n".join(new_lines) + "\n")
        print("  Updated:   .env")

    print("\n  MCP configuration removed. Databases are not affected.")
    # The locally built / pulled MCP images stay on disk after `compose down`
    # — no automatic prune (would remove unrelated images on a shared host).
    # Surface the manual cleanup line so the user knows the option exists.
    print("  Note: MCP images remain on disk. Run 'docker image prune' to reclaim.\n")
    return 0


# ============================================================================
# sdp db start / stop
# ============================================================================

def _collect_source_info(warnings=None):
    """Snapshot all configured sources for mount-coherence helpers.

    Returns a list of dicts ``{name, profiles, paths}`` — the input shape
    consumed by ``setup.mount_sync.expected_source_mounts`` and
    ``compute_mount_drift``.

    The warning about a platform.yaml that cannot be read is printed, as before, unless `warnings` is given: then it is appended to that list and nothing is printed, for a caller that returns data.
    """
    from social_data_pipeline.setup.utils import (
        list_sources, load_source_config, read_source_config, get_source_profiles,
    )
    out = []
    for source in list_sources():
        if warnings is None:
            cfg = load_source_config(source) or {}
        else:
            cfg, warning = read_source_config(source)
            if warning is not None:
                warnings.append(warning)
            cfg = cfg or {}
        out.append({
            "name": source,
            "profiles": list(get_source_profiles(source)),
            "paths": cfg.get("paths", {}) or {},
        })
    return out


def _read_override_yaml():
    """Parse docker-compose.override.yml; return {} if missing or unreadable."""
    import yaml
    override_path = ROOT / "docker-compose.override.yml"
    if not override_path.exists():
        return {}
    try:
        return yaml.safe_load(override_path.read_text()) or {}
    except yaml.YAMLError:
        return {}


def _get_parent_paths(env=None):
    """Return PARSED_PATH / OUTPUT_PATH in the same form .env carries them.

    The postgres + starrocks server blocks in docker-compose.yml bind
    ``${PARSED_PATH:-./data/parsed}`` and ``${OUTPUT_PATH:-./data/output}``
    unconditionally — so any source whose ``paths.parsed`` /
    ``paths.output`` falls under those parents is already visible to a
    running DB without a per-source override entry. The mount-sync
    containment helpers (``is_path_under`` etc.) compare strings after a
    cosmetic ``./`` / trailing-slash normalization, so we deliberately do
    NOT resolve to absolute here: both sides of the compare end up in
    the same "raw .env-style" shape, and containment falls out cleanly
    whether the user kept the relative defaults or pinned absolute paths.
    """
    from social_data_pipeline.setup.mount_sync import (
        PARSED_PATH_COMPOSE_DEFAULT, OUTPUT_PATH_COMPOSE_DEFAULT,
    )
    if env is None:
        env = load_env()
    return {
        "parsed": env.get("PARSED_PATH") or PARSED_PATH_COMPOSE_DEFAULT,
        "output": env.get("OUTPUT_PATH") or OUTPUT_PATH_COMPOSE_DEFAULT,
    }


def _running_services():
    """Set of compose service names currently in `running` state."""
    result = subprocess.run(
        ["docker", "compose", "ps", "--format", "json", "--filter", "status=running"],
        capture_output=True, text=True, cwd=ROOT,
    )
    services = set()
    if result.returncode != 0 or not result.stdout.strip():
        return services
    out = result.stdout.strip()
    rows = []
    if out.startswith("["):
        try:
            rows = json.loads(out)
        except json.JSONDecodeError:
            return services
    else:
        for line in out.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    for row in rows:
        svc = row.get("Service")
        if svc:
            services.add(svc)
    return services


def _warn_mount_drift_after_source_change():
    """Warn the operator after `source add`/`remove` if a running PG/SR
    container's mount set is now stale.

    Compares the configured sources (post-change) to the override file. The
    file is the closest cheap proxy for the running container's mounts —
    `cmd_db_start` regenerates it just before `up -d`, so override =
    container after each start. If override is now out of sync with the
    sources, the running container is too. The fix is the existing two-step
    `db stop && db start` of the affected service; emit it verbatim so the
    user can copy-paste.
    """
    from social_data_pipeline.setup.mount_sync import compute_mount_drift

    running = _running_services()
    relevant = running & {"postgres", "starrocks"}
    if not relevant:
        return

    override = _read_override_yaml()
    sources_info = _collect_source_info()
    parent_paths = _get_parent_paths()
    drift = compute_mount_drift(
        override, sources_info,
        services=tuple(sorted(relevant)),
        parent_paths=parent_paths,
    )
    if not drift:
        return

    print("\n  [WARN] Mount drift: running DB container does not match the configured sources.")
    for svc in sorted(drift):
        d = drift[svc]
        if d["missing"]:
            print(f"    {svc}: missing source mount(s)")
            for m in d["missing"]:
                print(f"      - {m}")
        if d["extra"]:
            print(f"    {svc}: stale source mount(s)")
            for m in d["extra"]:
                print(f"      - {m}")
        print(f"    Fix: python sdp.py db stop {svc} && python sdp.py db start {svc}")
    print()


def _container_mounts(service):
    """Return the live container's Mounts list for `service`, or None.

    Uses `docker compose ps -q <service>` to resolve the container ID, then
    `docker inspect` to read its mount set. Returns None when the service
    isn't running or the inspection fails — the caller treats that as
    "nothing to validate" rather than an error (the user may legitimately
    invoke `sdp run` before `db start`; compose will start the container
    fresh with the current override).
    """
    ps = subprocess.run(
        ["docker", "compose", "ps", "-q", service],
        capture_output=True, text=True, cwd=ROOT,
    )
    if ps.returncode != 0:
        return None
    cid = ps.stdout.strip().splitlines()[0].strip() if ps.stdout.strip() else ""
    if not cid:
        return None
    inspect = subprocess.run(
        ["docker", "inspect", cid],
        capture_output=True, text=True, cwd=ROOT,
    )
    if inspect.returncode != 0 or not inspect.stdout.strip():
        return None
    try:
        data = json.loads(inspect.stdout)
    except json.JSONDecodeError:
        return None
    if not data:
        return None
    return data[0].get("Mounts") or []


def _validate_run_mounts(profile, source_name, source_paths):
    """Fail-fast guard for `cmd_run`: PG/SR container must already mount the
    source's parsed/output paths before we launch the ingest orchestrator.

    Returns 0 on coherence (or when the target service isn't running, in
    which case `docker compose run` will start it fresh with the current
    override). Returns 1 with an actionable hint when the live container
    is missing one or more expected mounts. The error names the exact
    `db stop && db start` line that re-generates the override and restarts
    with the new mount set.
    """
    from social_data_pipeline.setup.mount_sync import (
        PROFILE_TO_SERVICE, runtime_mount_drift,
    )

    service = PROFILE_TO_SERVICE.get(profile)
    if service is None:
        return 0
    if service not in _running_services():
        return 0

    actual = _container_mounts(service)
    if actual is None:
        return 0

    # Resolve both sides through os.path.realpath so the comparison in
    # ``runtime_mount_drift`` (string-based) doesn't fail on symlinked
    # workspaces — e.g. ``/home/user/repo`` symlinked to ``/data/.../repo``,
    # where ROOT canonicalizes to one form and ``docker inspect``'s
    # ``Source`` reports the other. Without this canonicalization, the
    # parent-mount ancestor check produces a non-matching offset and the
    # validator falsely flags drift.
    resolved_paths = {}
    for key, value in (source_paths or {}).items():
        if not value:
            continue
        p = Path(value)
        if not p.is_absolute():
            p = ROOT / p
        resolved_paths[key] = os.path.realpath(p)

    canonical_actual = []
    for m in actual:
        entry = dict(m)
        src = m.get("Source")
        if src:
            entry["Source"] = os.path.realpath(src)
        canonical_actual.append(entry)

    missing = runtime_mount_drift(canonical_actual, source_name, resolved_paths)
    if not missing:
        return 0

    print(
        f"  Error: running '{service}' container is missing mount(s) for source "
        f"'{source_name}':"
    )
    for dst in missing:
        print(f"    - {dst}")
    print(
        "  The container was started before this source's paths were registered.\n"
        f"  Fix: python sdp.py db stop {service} && python sdp.py db start {service}"
    )
    return 1


def _resolve_server_data_mounts(services):
    """Add per-source data volume mounts for database server containers.

    Database servers (PostgreSQL, StarRocks) need to see parsed/output files for
    all sources.  The ingest containers mount source-specific paths (e.g.
    /mnt/data/parsed/reddit → /data/parsed), and the server-side path
    translators (_pg_server_path, _sr_server_path) add the source subdirectory.

    This function reads all source configs, builds per-source volume mounts
    (e.g. /mnt/data/parsed/reddit:/data/parsed/reddit:ro), and merges them
    into docker-compose.override.yml alongside any existing tablespace/storage mounts.
    """
    from social_data_pipeline.setup.mount_sync import expected_source_mounts

    sources_info = _collect_source_info()
    if not sources_info:
        return

    # Build the per-source mount set for every service we're starting. Shared
    # helper so source-add/remove drift detection and override regeneration
    # stay in lock-step. Pass parent_paths so sources whose paths fall under
    # ${PARSED_PATH}/${OUTPUT_PATH} (the default-disk case) are skipped here —
    # the docker-compose.yml parent mount on the server already exposes them,
    # and writing redundant per-source entries would force a needless restart
    # whenever the user adds an in-parent source.
    parent_paths = _get_parent_paths()
    service_mounts = {
        svc: sorted(expected_source_mounts(sources_info, svc, parent_paths=parent_paths))
        for svc in services
    }

    # Out-of-parent (multi-disk) sources mount their host path at
    # `/data/<parsed|output>/<source>` — a sub-path of the read-only parent
    # mount the postgres/starrocks compose blocks already bind. Docker has
    # to create the mountpoint inside the rootfs before applying the bind,
    # but the parent mount is `:ro`, so `mkdirat` fails with
    # "read-only file system" and the container refuses to start.
    # Pre-creating an empty placeholder dir at the parent's host path
    # gives docker an existing mountpoint to bind over. The placeholder
    # is shadowed by the per-source bind inside the container, so this
    # doesn't change what data the DB sees.
    for svc_mounts in service_mounts.values():
        for mount in svc_mounts:
            parts = mount.split(":")
            if len(parts) < 2:
                continue
            container_path = parts[1]
            if container_path.startswith("/data/parsed/"):
                parent_key, sub_name = "parsed", container_path[len("/data/parsed/"):]
            elif container_path.startswith("/data/output/"):
                parent_key, sub_name = "output", container_path[len("/data/output/"):]
            else:
                continue
            parent_host = parent_paths.get(parent_key)
            if not parent_host or not sub_name:
                continue
            placeholder = Path(parent_host)
            if not placeholder.is_absolute():
                placeholder = ROOT / placeholder
            (placeholder / sub_name).mkdir(parents=True, exist_ok=True)

    # Read existing override, preserve setup-generated mounts (tablespaces, SR storage, jobs export)
    override_path = ROOT / "docker-compose.override.yml"
    override_data = _read_override_yaml()
    preserved = {"postgres": [], "starrocks": []}
    for svc_name in preserved:
        svc = (override_data.get("services") or {}).get(svc_name) or {}
        for vol in svc.get("volumes") or []:
            # Keep tablespace, SR storage, and jobs export mounts.
            # Per-source data mounts are regenerated from scratch below.
            v = str(vol)
            if (
                "/data/tablespace/" in v
                or "/data/deploy/starrocks/" in v
                or ":/jobs_export" in v
            ):
                preserved[svc_name].append(vol)

    # Always write override if there's anything to write (preserved entries or
    # out-of-parent per-source mounts). Even when service_mounts is empty for
    # an in-parent-only source set, we still want to drop any redundant per-
    # source entries left over from a previous regen — so the rewrite proceeds
    # with the filtered (possibly-empty) data block.
    if not any(service_mounts.values()) and not any(preserved.values()):
        # Nothing to keep, nothing to add — strip a stale override file
        # entirely so docker compose doesn't read leftover mounts.
        if override_path.exists():
            override_path.unlink()
        return

    # Build override content
    content = (
        "# Auto-generated by sdp — volume mounts for database servers.\n"
        "# Setup mounts (tablespaces, SR storage, jobs export) + per-source\n"
        "# data mounts for sources whose paths fall outside ${PARSED_PATH} /\n"
        "# ${OUTPUT_PATH}; in-parent sources are covered by the parent mount\n"
        "# in docker-compose.yml.\n"
        "\n"
        "services:\n"
    )

    # Emit every service that has either preserved mounts or freshly-regenerated
    # data mounts. Iterating only `services` would drop the other service's
    # preserved entries when the user runs `sdp db start <single-service>`.
    for svc in sorted(set(preserved) | set(service_mounts)):
        svc_preserved = [f"      - {v}" for v in preserved.get(svc, [])]
        svc_data = [f"      - {m}" for m in service_mounts.get(svc, [])]
        all_volumes = svc_preserved + svc_data
        if not all_volumes:
            continue
        content += f"  {svc}:\n    volumes:\n" + "\n".join(all_volumes) + "\n"

    override_path.write_text(content)


def _exited_services_after_up(profile_args, expected_services):
    """Return list of service names in `Exited` state after a `compose up -d`.

    Belt-and-suspenders for the failure shape where `up -d --wait`
    returns 0 on a container that immediately crashed. `--wait` should
    already catch it, but docker compose has been racy historically —
    this ps probe runs immediately after and catches what `--wait` missed.

    Only services in `expected_services` (the ones this `up` started) are
    considered. `ps --all` lists every container in the project regardless
    of the active profiles, so without this filter a pre-existing stopped
    container from another profile — e.g. a `jobs` container left Exited by
    an earlier crash — would be misreported as a failure of this start.

    Returns a list of (service_name, exit_code) tuples; empty list when
    everything is running. Internal `docker compose ps` failure is treated
    as "no exited services found" (we don't want a transient ps failure
    to mask a successful `up`).
    """
    args = list(profile_args) + ["ps", "--all", "--format", "json"]
    result = subprocess.run(
        ["docker", "compose", *args],
        cwd=ROOT, capture_output=True, text=True,
    )
    if result.returncode != 0:
        return []

    out = result.stdout.strip()
    rows = []
    if not out:
        return []
    if out.startswith("["):
        try:
            rows = json.loads(out)
        except json.JSONDecodeError:
            return []
    else:
        for line in out.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    exited = []
    for row in rows:
        svc = row.get("Service")
        # Scope to services this `up` started. A row with an explicit Service
        # that isn't ours — e.g. a stale `jobs` container left Exited by an
        # earlier crash and surfaced by `ps --all` — is ignored. A row with
        # only a container Name (older docker, no Service field) can't be
        # scoped, so it falls through to inclusive detection rather than being
        # silently dropped and masking a real crash.
        if svc is not None and svc not in expected_services:
            continue
        state = (row.get("State") or "").lower()
        if state != "exited":
            continue
        name = svc or row.get("Name") or "<unknown>"
        exited.append((name, row.get("ExitCode")))
    return exited


def _print_exited_services(exited):
    """Render the "[ERROR] '<svc>' exited (code=N). See: docker compose logs <svc>"
    block. Used by both the bundled and standalone MCP startup paths."""
    for name, code in exited:
        code_part = f" (code={code})" if code is not None else ""
        print(
            f"  [ERROR] Service '{name}' exited{code_part}. "
            f"See: docker compose logs {name}"
        )


def cmd_db_start(args):
    """Start configured database server(s)."""
    try:
        return do_db_start(service=args.service, _ask_password=lambda: _prompt_db_password(tag="sdp_db_password"))
    except ActionError as e:
        print(f"\n  Error: {e.message}\n")
        return e.exit_code


def do_db_start(*, service=None, admin_password=None, _ask_password=None):
    """Start configured database server(s). Asks nothing itself; returns the exit code.

    The admin password is taken from `admin_password`, else from the `_ask_password` callback at the point where the terminal asks today, and only on a path that needs it: a password passed for a deployment without authentication is ignored.
    """
    _require_plain_password(admin_password)
    # Without a password and without a terminal to ask on, stop before anything runs: the body would otherwise probe the containers on the MCP path before it reaches the question it cannot ask.
    if not admin_password and _ask_password is None:
        if service and service.endswith("-mcp"):
            password_needed = _is_auth_enabled()
        else:
            password_needed = _needs_admin_password()
        if password_needed:
            raise ActionError(_ADMIN_PASSWORD_REQUIRED)

    configured = _get_configured_db_services()
    if not configured:
        print("  No databases configured. Run: python sdp.py db setup")
        return 1

    mcp_services = _get_configured_mcp_services()

    # MCP-only target (e.g. "postgres-mcp" → profile "postgres_mcp")
    if service and service.endswith("-mcp"):
        mcp_profile = service.replace("-", "_")
        parent_db = service.rsplit("-", 1)[0]
        if mcp_profile not in mcp_services:
            print(f"  Error: '{service}' is not configured. Run: python sdp.py db setup-mcp")
            return 1
        # Verify parent DB is running
        ps_result = docker_compose("ps", "--format", "{{.Names}}", "--filter",
                                   "status=running")
        if ps_result.returncode != 0:
            return ps_result.returncode
        # Prompt for admin password if auth is enabled (needed for init container)
        if _is_auth_enabled():
            password = _resolve_admin_password(admin_password, _ask_password)
            _set_auth_env(password)
        # Start only the MCP profile (init container runs first via depends_on).
        # `--wait` blocks until services are running and (where defined) healthy;
        # the post-launch ps probe surfaces any race-condition exits that
        # `--wait` may have missed.
        profile_flags = ["--profile", parent_db, "--profile", mcp_profile]
        mcp_args = profile_flags + ["up", "-d", "--wait"]
        result = docker_compose(*mcp_args)
        # MCP profile "x_mcp" maps to compose service "x-mcp".
        expected = {parent_db, mcp_profile.replace("_", "-")}
        exited = _exited_services_after_up(profile_flags, expected)
        if exited:
            _print_exited_services(exited)
            return 1
        return result.returncode

    # jobs scheduler target
    if service == "jobs":
        if not _is_jobs_configured():
            print("  Error: 'jobs' is not configured. Run: python sdp.py db setup-jobs")
            return 1
        if _needs_admin_password():
            password = _resolve_admin_password(admin_password, _ask_password)
            _set_auth_env(password)
        # Scope the jobs profile and rebuild its image (jobs contains SDP
        # package code, so it drifts on every code change).
        compose_args = ["--profile", "jobs", "up", "-d", "--build", "jobs"]
        result = docker_compose(*compose_args)
        return result.returncode

    # Database target (with auto-bundled MCP)
    if service:
        if service not in configured:
            print(f"  Warning: '{service}' is not configured, starting anyway.")
        targets = [service]
    else:
        targets = configured

    # Prompt for admin password if either DB auth or jobs UI auth is
    # enabled. DB containers use it for their admin role; jobs UI uses it
    # to authenticate web UI logins. Single prompt covers both.
    password = None
    if _needs_admin_password():
        password = _resolve_admin_password(admin_password, _ask_password)
        _set_auth_env(password)

    # Determine which MCP profiles to start
    mcp_targets = []
    for svc in targets:
        mcp_profile = f"{svc}_mcp"
        if mcp_profile in mcp_services:
            mcp_targets.append(mcp_profile)

    # Resolve per-source data mounts for database servers that read files directly
    mount_services = [s for s in ("postgres", "starrocks") if s in targets]
    if mount_services:
        # Pre-create the parent dirs the postgres/starrocks server blocks bind
        # unconditionally (${PARSED_PATH} / ${OUTPUT_PATH}). Docker creates
        # missing bind-mount source dirs as root:root, which would block the
        # user from later 'sdp run' mkdir-ing per-source subdirs under them.
        for host_path in _get_parent_paths().values():
            p = Path(host_path)
            if not p.is_absolute():
                p = ROOT / p
            p.mkdir(parents=True, exist_ok=True)
        _resolve_server_data_mounts(mount_services)

    # Pre-create each target DB's data directory as the host user, so Docker
    # doesn't auto-create the bind-mount source as root:root on first start.
    # A root-owned data dir makes host-side `.ro_credentials` writes fall back
    # to a root-writing container, which is the source of the credential-
    # ownership drift that `sdp db verify` flags. Mongo's real mount source is
    # the `db` subdir (compose mounts `${MONGO_DATA_PATH}/db`), so create that
    # too. postgres/starrocks were already spared this because their file
    # mounts above are pre-created; mongo was the gap.
    db_env = load_env()
    for svc in targets:
        env_key = _DB_DATA_PATH_ENV.get(svc)
        data_path = db_env.get(env_key) if env_key else None
        if not data_path:
            continue
        p = Path(data_path)
        if not p.is_absolute():
            p = ROOT / p
        p.mkdir(parents=True, exist_ok=True)
        if svc == "mongo":
            (p / "db").mkdir(parents=True, exist_ok=True)

    # Start databases first (without MCP), wait for healthchecks
    db_args = []
    for svc in targets:
        db_args += ["--profile", svc]
    db_args += ["up", "-d", "--wait"]
    result = docker_compose(*db_args)
    if result.returncode != 0:
        return result.returncode

    # Start MCP servers (init containers handle user creation via depends_on).
    # `--wait` blocks until MCPs are running; the post-launch ps probe
    # surfaces any race-condition exits that `--wait` may have missed
    # (the `up -d` returns 0 on a crashed container failure shape).
    if mcp_targets:
        profile_flags = []
        for svc in targets:
            profile_flags += ["--profile", svc]
        for mp in mcp_targets:
            profile_flags += ["--profile", mp]
        mcp_args = profile_flags + ["up", "-d", "--wait"]
        result = docker_compose(*mcp_args)
        # MCP profiles "x_mcp" map to compose services "x-mcp".
        expected = set(targets) | {mp.replace("_", "-") for mp in mcp_targets}
        exited = _exited_services_after_up(profile_flags, expected)
        if exited:
            _print_exited_services(exited)
            return 1
        if result.returncode != 0:
            return result.returncode

    # Start jobs scheduler (single container: UI + MCP + runner) when configured.
    # Rebuilds the image on every start — jobs contains SDP package code, so
    # the image drifts every time code changes. Docker layer cache makes the
    # no-change rebuild cheap.
    if _is_jobs_configured():
        jobs_backends = _jobs_backend_profiles()
        attached = [b for b in jobs_backends if b in targets]
        if attached:
            jobs_args = ["--profile", "jobs", "up", "-d", "--build", "jobs"]
            result = docker_compose(*jobs_args)

    return result.returncode


def _jobs_backend_profiles() -> list[str]:
    """Which DB profiles the jobs container depends on, based on its targets.

    Maps the scheduler's backend names to docker-compose profile names:
    `mongodb` backend → `mongo` profile (the DB service in compose).
    """
    cfg = _load_jobs_config()
    backends = set()
    for spec in (cfg.get("targets") or {}).values():
        backend = (spec or {}).get("backend")
        if backend in ("postgres", "starrocks", "mongodb"):
            backends.add(backend)
    order = {"postgres": "postgres", "starrocks": "starrocks", "mongodb": "mongo"}
    return [order[b] for b in ("postgres", "starrocks", "mongodb") if b in backends]


def cmd_db_stop(args):
    """Stop configured database server(s)."""
    return do_db_stop(service=args.service)


def do_db_stop(*, service=None):
    """Stop configured database server(s). Asks nothing; returns the exit code."""
    configured = _get_configured_db_services()
    if not configured:
        print("  No databases configured. Run: python sdp.py db setup")
        return 1

    mcp_services = _get_configured_mcp_services()

    # MCP-only target (e.g. "postgres-mcp" → profile "postgres_mcp")
    if service and service.endswith("-mcp"):
        mcp_profile = service.replace("-", "_")
        parent_db = service.rsplit("-", 1)[0]
        if mcp_profile not in mcp_services:
            print(f"  Error: '{service}' is not configured.")
            return 1
        profiles = ["--profile", parent_db, "--profile", mcp_profile]
        result = docker_compose(*profiles, "stop", service)
        if result.returncode != 0:
            return result.returncode
        result = docker_compose(*profiles, "rm", "-f", service)
        return result.returncode

    # jobs scheduler target
    if service == "jobs":
        profiles = ["--profile", "jobs"]
        result = docker_compose(*profiles, "stop", "jobs")
        if result.returncode != 0:
            return result.returncode
        result = docker_compose(*profiles, "rm", "-f", "jobs")
        return result.returncode

    # Database target (stops DB + its MCP)
    if service:
        targets = [service]
    else:
        targets = configured

    compose_args = []
    for svc in targets:
        compose_args += ["--profile", svc]
        mcp_profile = f"{svc}_mcp"
        if mcp_profile in mcp_services:
            compose_args += ["--profile", mcp_profile]
    if not service and _is_jobs_configured():
        compose_args += ["--profile", "jobs"]
    compose_args += ["down"]

    result = docker_compose(*compose_args)
    return result.returncode


# ============================================================================
# sdp db status
# ============================================================================

def cmd_db_status(args):
    """Show database configuration and health."""
    print_db_status(get_db_status(verbose=getattr(args, "verbose", False)))
    return 0


def _state_float(text):
    """Return the float a number in a state file stands for, or, when it is not finite, the word the terminal prints for it as a single value (`nan`, `inf`, `-inf`)."""
    value = float(text)
    return value if math.isfinite(value) else str(value)


def _parse_state_json(text):
    """Parse the text of a state file. The JSON parser accepts NaN, Infinity and -Infinity, and reads a number too large for a float (`1e999`) as an infinity; JSON itself has no such numbers, so as floats they could not be returned as data. They become the word the terminal prints for such a value on its own. Two shapes of a damaged file print differently than before because of it: a non-finite number inside a list or object that is printed whole now shows in quotes, and a failed entry whose file name is such a number can now match a log section named `nan` or `inf`."""
    return json.loads(text, parse_constant=_state_float, parse_float=_state_float)


def _hf_downloaded(dumps):
    """Whether a Hugging Face source's dumps folder holds a downloaded Parquet file where `source download` puts them (<dumps>/<config>/<split>/*.parquet), looking at no more than 256 folders of each level. False for a folder that is missing or cannot be read."""
    if not isinstance(dumps, str) or not dumps:
        return False
    folder = Path(dumps) if Path(dumps).is_absolute() else ROOT / dumps
    try:
        for config in list(os.scandir(folder))[:256]:
            if not config.is_dir(follow_symlinks=False):
                continue
            for split in list(os.scandir(config.path))[:256]:
                if split.is_dir(follow_symlinks=False) and any(item.name.endswith(".parquet") and item.is_file(follow_symlinks=False) for item in os.scandir(split.path)):
                    return True
    except OSError:
        return False
    return False


def _json_fact(value):
    """Return a value read from a YAML file in a form JSON can hold: a string, a boolean, a number or None as it is, anything else (a date, a list) as its `str()`, which is the text the terminal prints for it."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    return str(value)


def get_db_status(*, verbose=False):
    """Gather what `db status` shows. Prints nothing and returns JSON-serialisable data for `print_db_status`: `verbose` and `configured` always; with a database configured also `auth`, `databases` (one entry per database, each with its `ingestion`), `export`, `mcp`, `jobs` and `drift`. The per-source ingestion detail is read only with `verbose`; without it each `ingestion` says only whether state files exist."""
    env = load_env()
    configured = _get_configured_db_services()

    data = {"verbose": bool(verbose), "configured": list(configured)}

    if not configured:
        return data

    # Authentication status
    auth_enabled = (env.get("POSTGRES_AUTH_ENABLED") == "true"
                    or env.get("MONGO_AUTH_ENABLED") == "true"
                    or env.get("STARROCKS_AUTH_ENABLED") == "true")
    data["auth"] = {"enabled": auth_enabled, "rw_users": [], "ro_user": None, "ro_credentials": [], "mcp_users": []}
    if auth_enabled:
        pg_user = env.get("POSTGRES_USER", "postgres") if "postgres" in configured else None
        mongo_user = env.get("MONGO_ADMIN_USER") if "mongo" in configured else None
        sr_auth = "starrocks" in configured and env.get("STARROCKS_AUTH_ENABLED") == "true"
        if pg_user:
            data["auth"]["rw_users"].append({"db": "postgres", "user": pg_user})
        if mongo_user:
            data["auth"]["rw_users"].append({"db": "mongo", "user": mongo_user})
        if sr_auth:
            data["auth"]["rw_users"].append({"db": "starrocks", "user": "root"})
        ro_user = env.get("POSTGRES_RO_USER") or env.get("MONGO_RO_USER") or env.get("STARROCKS_RO_USER")
        if ro_user:
            data["auth"]["ro_user"] = ro_user
            # Iterate every configured DB whose auth is enabled and report its
            # `.ro_credentials` path independently. Collapsing to the first
            # match (the old behavior) silently hid per-DB drift — e.g. Mongo
            # and StarRocks both having creds but only Mongo's path surfacing.
            db_path_env = [
                ("postgres",  "POSTGRES_AUTH_ENABLED",  "PGDATA_PATH",         "./data/database/postgres"),
                ("mongo",     "MONGO_AUTH_ENABLED",     "MONGO_DATA_PATH",     "./data/database/mongo"),
                ("starrocks", "STARROCKS_AUTH_ENABLED", "STARROCKS_DATA_PATH", "./data/database/starrocks"),
            ]
            for db_name, auth_key, path_key, default in db_path_env:
                if db_name not in configured:
                    continue
                if env.get(auth_key) != "true":
                    continue
                dp = Path(env.get(path_key, default))
                if not dp.is_absolute():
                    dp = ROOT / dp
                cred_file = dp / ".ro_credentials"
                if cred_file.exists():
                    data["auth"]["ro_credentials"].append({"db": db_name, "path": str(cred_file)})

        # MCP user per configured DB — same `or`-chain anti-pattern as RO above
        # collapsed every DB to whichever envvar resolved first. Iterate.
        for db_name, mcp_env in (
            ("postgres",  "POSTGRES_MCP_USER"),
            ("mongo",     "MONGO_MCP_USER"),
            ("starrocks", "STARROCKS_MCP_USER"),
        ):
            if db_name not in configured:
                continue
            user = env.get(mcp_env)
            if user:
                data["auth"]["mcp_users"].append({"db": db_name, "user": user})

    # Check which services are running
    running_services = set()
    result = subprocess.run(
        ["docker", "compose", "ps", "--format", "json", "--filter", "status=running"],
        capture_output=True, text=True, cwd=ROOT,
    )
    if result.returncode == 0 and result.stdout.strip():
        for line in result.stdout.strip().splitlines():
            try:
                container = json.loads(line)
                svc = container.get("Service")
                if svc:
                    running_services.add(svc)
            except json.JSONDecodeError:
                pass

    data["databases"] = []

    # PostgreSQL
    if "postgres" in configured:
        pgdata_path = env.get("PGDATA_PATH", "./data/database/postgres")
        pgdata = Path(pgdata_path)
        if not pgdata.is_absolute():
            pgdata = ROOT / pgdata

        state_dir = pgdata / "state_tracking"
        data["databases"].append({
            "db": "postgres",
            "path": pgdata_path,
            "name": env.get('DB_NAME', 'datasets'),
            "port": env.get('POSTGRES_PORT', '5432'),
            "running": 'postgres' in running_services,
            "ingestion": _get_ingestion(state_dir, "_postgres_", "postgres", verbose),
        })

    # MongoDB
    if "mongo" in configured:
        mongo_data_path = env.get("MONGO_DATA_PATH", "./data/database/mongo")
        mongo_data = Path(mongo_data_path)
        if not mongo_data.is_absolute():
            mongo_data = ROOT / mongo_data

        state_dir = mongo_data / "state_tracking"
        data["databases"].append({
            "db": "mongo",
            "path": mongo_data_path,
            "port": env.get('MONGO_PORT', '27017'),
            "running": 'mongo' in running_services,
            "ingestion": _get_ingestion(state_dir, "_mongo_", "mongo", verbose),
        })

    # StarRocks
    if "starrocks" in configured:
        sr_data_path = env.get("STARROCKS_DATA_PATH", "./data/database/starrocks")
        sr_data = Path(sr_data_path)
        if not sr_data.is_absolute():
            sr_data = ROOT / sr_data

        state_dir = sr_data / "state_tracking"
        data["databases"].append({
            "db": "starrocks",
            "path": sr_data_path,
            "port": env.get('STARROCKS_PORT', '9030'),
            "fe_http_port": env.get('STARROCKS_FE_HTTP_PORT', '8030'),
            "running": 'starrocks' in running_services,
            "ingestion": _get_ingestion(state_dir, "_sr_", "sr", verbose),
        })

    # Export path
    export_path = env.get("DB_EXPORT_PATH")
    data["export"] = {"host_path": export_path} if export_path else None

    # MCP servers
    data["mcp"] = []
    mcp_config = _load_mcp_config()
    if mcp_config:
        pg_mcp = mcp_config.get("postgres", {})
        if pg_mcp.get("enabled"):
            port = pg_mcp.get("port", 8000)
            access = pg_mcp.get("access_mode", "restricted")
            data["mcp"].append({
                "db": "postgres",
                "port": _json_fact(port),
                "read_only": access == "restricted",
                "access_mode": _json_fact(access),
                "running": "postgres-mcp" in running_services,
            })

        mongo_mcp = mcp_config.get("mongo", {})
        if mongo_mcp.get("enabled"):
            port = mongo_mcp.get("port", 3000)
            read_only = mongo_mcp.get("read_only", True)
            data["mcp"].append({
                "db": "mongo",
                "port": _json_fact(port),
                "read_only": bool(read_only),
                "running": "mongo-mcp" in running_services,
            })

        sr_mcp = mcp_config.get("starrocks", {})
        if sr_mcp.get("enabled"):
            port = sr_mcp.get("port", 9000)
            sr_ro_active = (
                env.get("STARROCKS_AUTH_ENABLED") == "true"
                and bool(env.get("STARROCKS_RO_USER"))
            )
            data["mcp"].append({
                "db": "starrocks",
                "port": _json_fact(port),
                "read_only": sr_ro_active,
                "running": "starrocks-mcp" in running_services,
            })

    # Jobs scheduler
    data["jobs"] = None
    if _is_jobs_configured():
        jobs_cfg = _load_jobs_config()
        port = jobs_cfg.get("port", 8050)
        targets = jobs_cfg.get("targets") or {}

        # Queue depth summary from the filesystem store
        jobs_dir = ROOT / "data" / "jobs"
        pending_n = len(list((jobs_dir / "pending").glob("*.json"))) if (jobs_dir / "pending").exists() else 0
        approved_n = len(list((jobs_dir / "approved").glob("*.json"))) if (jobs_dir / "approved").exists() else 0
        running_n = len(list((jobs_dir / "running").glob("*.json"))) if (jobs_dir / "running").exists() else 0
        data["jobs"] = {
            "port": _json_fact(port),
            "result_root": _json_fact(jobs_cfg.get('result_root', './data/jobs/results')),
            "max_concurrent": _json_fact(jobs_cfg.get('max_concurrent', 1)),
            "running": "jobs" in running_services,
            "targets": [
                {"name": _json_fact(n), "backend": _json_fact(spec.get('backend'))}
                for n, spec in targets.items()
            ],
            "queue": {"pending": pending_n, "approved": approved_n, "running": running_n},
        }

    # Drift section (local-files only — running-container probes are reserved
    # for `sdp db verify`, which has the matching exit-code contract).
    # The warnings about an unreadable platform.yaml come from loading the source configs for the mount check, so in the text they follow every section above: they are kept with the findings for the printer to put them there.
    warnings = []
    drift_findings = _drift_findings(probe_containers=False, warnings=warnings)
    data["drift"] = {
        "warnings": warnings,
        "findings": {name: [f.to_dict() for f in findings] for name, findings in drift_findings.items()},
    }

    return data


def print_db_status(data):
    """Print the text of `db status` from the data of `get_db_status`."""
    print()
    print("  Social Data Pipeline - Database Status")
    print("  =====================================")

    if not data["configured"]:
        print("\n  No databases configured. Run: python sdp.py db setup\n")
        return

    # Authentication status
    auth = data["auth"]
    print(f"\n  Authentication: {'enabled' if auth['enabled'] else 'disabled'}")
    if auth["enabled"]:
        if auth["rw_users"]:
            rw_parts = [f"{entry['db']}: {entry['user']}" for entry in auth["rw_users"]]
            print(f"    RW user:       {', '.join(rw_parts)}")
        ro_user = auth["ro_user"]
        if ro_user:
            ro_cred_paths = [(_DB_DISPLAY.get(entry["db"], entry["db"]), entry["path"]) for entry in auth["ro_credentials"]]  # [(display_name, cred_path), ...]
            if not ro_cred_paths:
                print(f"    RO user:       {ro_user} (no password)")
            elif len(ro_cred_paths) == 1:
                _, cred_path = ro_cred_paths[0]
                print(f"    RO user:       {ro_user} (password in {cred_path})")
            else:
                print(f"    RO user:       {ro_user}")
                width = max(len(name) for name, _ in ro_cred_paths)
                for name, cred_path in ro_cred_paths:
                    print(f"      {(name + ':'):<{width + 1}}  {cred_path}")

        mcp_users = [(_DB_DISPLAY.get(entry["db"], entry["db"]), entry["user"]) for entry in auth["mcp_users"]]
        if len(mcp_users) == 1:
            print(f"    MCP user:      {mcp_users[0][1]}")
        elif len(mcp_users) > 1:
            print("    MCP user:")
            width = max(len(name) for name, _ in mcp_users)
            for name, user in mcp_users:
                print(f"      {(name + ':'):<{width + 1}}  {user}")

    for database in data["databases"]:
        # PostgreSQL
        if database["db"] == "postgres":
            print("\n  PostgreSQL:")
            print(f"    Path:      {database['path']}")
            print(f"    Name:      {database['name']}")
            print(f"    Port:      {database['port']}")
            print(f"    Running:   {'yes' if database['running'] else 'no'}")

        # MongoDB
        if database["db"] == "mongo":
            print("\n  MongoDB:")
            print(f"    Path:      {database['path']}")
            print(f"    Port:      {database['port']}")
            print(f"    Running:   {'yes' if database['running'] else 'no'}")

        # StarRocks
        if database["db"] == "starrocks":
            print("\n  StarRocks:")
            print(f"    Path:      {database['path']}")
            print(f"    Port:      {database['port']}")
            print(f"    FE HTTP:   {database['fe_http_port']}")
            print(f"    Running:   {'yes' if database['running'] else 'no'}")

        _print_ingestion(database["ingestion"], data["verbose"])

    # Export path
    if data["export"]:
        print("\n  Export:")
        print(f"    Host path:      {data['export']['host_path']}")
        print("    Container path: /export")

    # MCP servers
    for server in data["mcp"]:
        port = server["port"]
        running = "yes" if server["running"] else "no"
        if server["db"] == "postgres":
            print("\n  PostgreSQL MCP:")
            print(f"    Port:      {port}")
            if server["read_only"]:
                print("    Read-only: yes (access-mode restricted)")
            else:
                print(f"    Read-only: no (access-mode {server['access_mode']})")
            print(f"    Endpoint:  http://<server_ip>:{port}/sse (type: sse)")
            print(f"    Running:   {running}")

        if server["db"] == "mongo":
            print("\n  MongoDB MCP:")
            print(f"    Port:      {port}")
            if server["read_only"]:
                print("    Read-only: yes (filtered tool list)")
            else:
                print("    Read-only: no")
            print(f"    Endpoint:  http://<server_ip>:{port}/mcp (type: http)")
            print(f"    Running:   {running}")

        if server["db"] == "starrocks":
            print("\n  StarRocks MCP:")
            print(f"    Port:      {port}")
            # StarRocks has no application-level read-only switch — the
            # parenthetical hint exists so operators don't go hunting for one.
            if server["read_only"]:
                print("    Read-only: yes (database-level via sdp_readonly role)")
            else:
                print("    Read-only: no (auth disabled — RO role not enforced)")
            print(f"    Endpoint:  http://<server_ip>:{port}/mcp (type: http)")
            print(f"    Running:   {running}")

    # Jobs scheduler
    jobs = data["jobs"]
    if jobs:
        port = jobs["port"]
        running = "yes" if jobs["running"] else "no"
        print("\n  Jobs scheduler:")
        print(f"    Port:         {port}")
        print(f"    Result root:  {jobs['result_root']}")
        print(f"    Max concur.:  {jobs['max_concurrent']}")
        print(f"    Web UI:       http://<server_ip>:{port}/")
        print(f"    MCP:          http://<server_ip>:{port}/mcp (type: http)")
        print(f"    Running:      {running}")
        if jobs["targets"]:
            names = ", ".join(
                f"{target['name']} ({_DB_DISPLAY.get(target['backend'], target['backend'])})"
                for target in jobs["targets"]
            )
            print(f"    Targets:      {names}")

        queue = jobs["queue"]
        print(f"    Queue:        pending={queue['pending']} approved={queue['approved']} running={queue['running']}")

    # Drift section
    for warning in data["drift"]["warnings"]:
        print(warning)
    drift_findings = data["drift"]["findings"]
    if drift_findings and not _is_drift_clean(drift_findings):
        print()
        _print_drift_findings(drift_findings, header="Drift")
        print("  Run 'sdp db verify' for a per-DB exit-code-aware report.")

    print()


# ============================================================================
# sdp db verify
# ============================================================================

def _resolve_cred_state(db, env):
    """Build the cred_file_state dict the verify module expects.

    Handles the legitimate "auth not configured / data path not set" cases
    by returning ``{exists: False, path: ""}`` — the verify rule decides
    whether that's drift based on yaml-auth + env-auth flags. No hardcoded
    default path: a missing env var produces ``path=""`` and the auth check
    surfaces it as drift only when the DB actually claims auth is on.
    """
    state = {
        "path": "",
        "exists": False,
        "mode": None,
        "host_owned": None,
        "readable": False,
    }
    env_key = _DB_DATA_PATH_ENV.get(db)
    if not env_key:
        return state
    raw = (env or {}).get(env_key, "")
    if not raw:
        return state
    dp = Path(raw)
    if not dp.is_absolute():
        dp = ROOT / dp
    cred_path = dp / ".ro_credentials"
    state["path"] = str(cred_path)
    if not cred_path.exists():
        return state
    state["exists"] = True
    try:
        st = cred_path.stat()
        state["mode"] = stat.S_IMODE(st.st_mode)
        state["host_owned"] = st.st_uid == os.getuid()
    except OSError:
        state["mode"] = None
        state["host_owned"] = None
    try:
        cred_path.read_text()
        state["readable"] = True
    except OSError:
        state["readable"] = False
    return state


def _probe_container_state(service):
    """Best-effort probe of a running DB container for `db verify`.

    Returns ``None`` when the container is not running (no probe to do)
    or when the docker calls fail (verify treats absence as "no signal,"
    not as drift). Otherwise returns a dict with ``running`` (always True),
    ``healthy``, and ``env_auth`` (the ``<DB>_AUTH_ENABLED`` value the
    container itself was started with).
    """
    if service not in _running_services():
        return None
    state = {"running": True, "healthy": None, "env_auth": None}

    ps = subprocess.run(
        ["docker", "compose", "ps", "-q", service],
        capture_output=True, text=True, cwd=ROOT,
    )
    cid = ""
    if ps.returncode == 0 and ps.stdout.strip():
        cid = ps.stdout.strip().splitlines()[0].strip()
    if not cid:
        return state

    inspect = subprocess.run(
        ["docker", "inspect", cid],
        capture_output=True, text=True, cwd=ROOT,
    )
    if inspect.returncode != 0 or not inspect.stdout.strip():
        return state
    try:
        data = json.loads(inspect.stdout)
    except json.JSONDecodeError:
        return state
    if not data:
        return state

    info = data[0]
    health = ((info.get("State") or {}).get("Health") or {}).get("Status")
    if health:
        state["healthy"] = health == "healthy"

    env_key = f"{service.upper()}_AUTH_ENABLED"
    cfg_env = (info.get("Config") or {}).get("Env") or []
    for entry in cfg_env:
        if isinstance(entry, str) and entry.startswith(env_key + "="):
            state["env_auth"] = entry.split("=", 1)[1] == "true"
            break
    return state


def _build_verify_context(*, env, probe_containers, warnings=None):
    """Assemble the ctx dict that ``setup.verify.compute_drift`` consumes. With `warnings` (a list) the warnings about source configs that cannot be read are appended to it and nothing is printed."""
    configured = _get_configured_db_services()
    db_yamls = {db: _load_db_yaml(db) for db in configured}
    cred_states = {db: _resolve_cred_state(db, env) for db in configured}
    sources_info = _collect_source_info(warnings=warnings)
    override_data = _read_override_yaml()
    mcp_config = _load_mcp_config()
    jobs_config = _load_jobs_config() if _is_jobs_configured() else {}
    container_states = {}
    if probe_containers:
        for db in configured:
            container_states[db] = _probe_container_state(db)
    return {
        "env": env,
        "configured_dbs": configured,
        "db_yamls": db_yamls,
        "cred_file_states": cred_states,
        "sources_info": sources_info,
        "override_data": override_data,
        "mcp_config": mcp_config,
        "jobs_config": jobs_config,
        "container_states": container_states,
        # Threaded into the mount-coherence sub-check so in-parent sources
        # don't show up as drift just because they lack per-source override
        # entries (the docker-compose.yml parent mount covers them).
        "parent_paths": _get_parent_paths(env),
    }


def _drift_findings(*, probe_containers, warnings=None):
    """Wrapper that loads .env + builds the ctx + runs compute_drift."""
    from social_data_pipeline.setup.verify import compute_drift
    env = load_env()
    ctx = _build_verify_context(env=env, probe_containers=probe_containers, warnings=warnings)
    return compute_drift(ctx)


def _is_drift_clean(findings_by_name):
    return all(not findings for findings in findings_by_name.values())


def _print_drift_findings(findings_by_name, header="Drift"):
    """Render findings in `db status` / `db verify` text mode.

    `findings_by_name` maps a name to its findings, each a dict with `category`, `message` and `fix` (the `to_dict()` of a compute_drift finding). Names are db names (`postgres`, `mongo`, `starrocks`) plus optional cross-cutting `mcp` / `jobs` keys; rendering preserves insertion order.
    """
    print(f"  {header}:")
    any_drift = False
    for name in findings_by_name:
        findings = findings_by_name[name]
        if not findings:
            continue
        any_drift = True
        n = len(findings)
        print(f"    {name}: DRIFT ({n} issue{'s' if n != 1 else ''})")
        for f in findings:
            print(f"      [{f['category']}] {f['message']}")
            print(f"        Fix: {f['fix']}")
    if not any_drift:
        print("    All databases coherent.")


def cmd_db_verify(args):
    """Operator preflight: exit non-zero on any drift across config / env /
    creds / containers / mounts / mcp / jobs.

    Two output modes:

    - default human text — same finding format as ``db status``'s drift
      section, but exhaustive and exit-code-bound.
    - ``--json`` — machine-readable for CI / scripts; structure is stable
      across releases.

    ``--db <name>`` filters to a single database (cross-cutting MCP / jobs
    sections are still emitted because their findings can name that DB).
    """
    data = get_db_verify(db=getattr(args, "db", None))
    if getattr(args, "json", False):
        print_db_verify_json(data)
    else:
        print_db_verify(data)
    return data["report"]["exit_code"]


# The databases `db verify --db` can be limited to: the parser's choices.
_VERIFY_DATABASES = ("postgres", "mongo", "starrocks")


def get_db_verify(*, db=None):
    """Gather what `db verify` reports. Prints nothing and returns `{"report": ..., "warnings": [...]}`: `report` is the object `--json` prints (`ok`, `exit_code`, `results`; with no database configured also `message`), `warnings` the lines about an unreadable platform.yaml that the terminal shows before it. Raises ActionError for a `db` that is not one of the three databases."""
    from social_data_pipeline.setup.verify import compute_drift

    # A name that matches no group would drop every database from the report and leave it "ok".
    if db is not None and db not in _VERIFY_DATABASES:
        raise ActionError("unknown database")

    env = load_env()
    configured = _get_configured_db_services()
    if not configured:
        msg = "No databases configured. Run: python sdp.py db setup"
        return {"report": {"ok": True, "exit_code": 0, "message": msg, "results": {}}, "warnings": []}

    warnings = []
    ctx = _build_verify_context(env=env, probe_containers=True, warnings=warnings)
    findings_by_name = compute_drift(ctx)

    db_filter = db
    if db_filter:
        # Keep the filtered DB plus cross-cutting checks (mcp / jobs); drop
        # other DBs. Cross-cutting findings can mention the filtered DB,
        # so silencing them would hide drift the user would want to see.
        findings_by_name = {
            name: findings for name, findings in findings_by_name.items()
            if name == db_filter or name in ("mcp", "jobs")
        }

    clean = _is_drift_clean(findings_by_name)
    exit_code = 0 if clean else 1

    payload = {
        "ok": clean,
        "exit_code": exit_code,
        "results": {
            name: {
                "ok": not findings,
                "findings": [f.to_dict() for f in findings],
            }
            for name, findings in findings_by_name.items()
        },
    }
    return {"report": payload, "warnings": warnings}


def print_db_verify_json(data):
    """Print the `--json` form of `db verify` from the data of `get_db_verify`."""
    for warning in data["warnings"]:
        print(warning)
    report = data["report"]
    # The report for "no database configured" is the one with a message, and the only one printed on a single line.
    if "message" in report:
        print(json.dumps(report))
    else:
        print(json.dumps(report, indent=2))


def print_db_verify(data):
    """Print the text form of `db verify` from the data of `get_db_verify`."""
    for warning in data["warnings"]:
        print(warning)
    report = data["report"]
    if "message" in report:
        print(f"\n  {report['message']}\n")
        return

    findings_by_name = {name: result["findings"] for name, result in report["results"].items()}
    print()
    print("  Social Data Pipeline - Database Verify")
    print("  =====================================")
    print()
    _print_drift_findings(findings_by_name, header="Findings")
    print()
    if report["ok"]:
        print("  OK")
    else:
        n_drift = sum(1 for v in findings_by_name.values() if v)
        n_total = len(findings_by_name)
        print(f"  Exit: 1 (drift in {n_drift} of {n_total} check group(s))")
    print()


def _load_classifier_suffixes():
    """Load classifier name → table suffix mapping from postgres_ml services config."""
    import yaml
    services_path = CONFIG_DIR / "postgres_ml" / "services.yaml"
    if not services_path.exists():
        return {}
    try:
        cfg = yaml.safe_load(services_path.read_text()) or {}
        return {
            name: cls.get("suffix", f"_{name}")
            for name, cls in cfg.get("classifiers", {}).items()
        }
    except (OSError, yaml.YAMLError):
        return {}


def _get_ingestion(state_dir, label_split, db_type, verbose):
    """Gather the ingestion state of one database for `db status`.

    Verbose mode dispatches to `_get_ingestion_state` for the full per-source list. Default mode reads no state file and returns only `has_state_files`, which is all the one-line teaser needs: it is shown when state files exist, and nothing when the state dir is empty or absent — no point teasing about data that isn't there.
    """
    if verbose:
        return _get_ingestion_state(state_dir, label_split, db_type)
    return {"has_state_files": state_dir.exists() and any(state_dir.glob("*.json"))}


def _print_ingestion(ingestion, verbose):
    """Print the ingestion breakdown of one database (verbose) or the one-line teaser, from the data of `_get_ingestion`."""
    if not verbose:
        if ingestion["has_state_files"]:
            print("\n    Ingestion: run with --verbose for per-source breakdown.")
        return

    if not ingestion["has_state_files"]:
        print("\n    No ingestion data yet.")
        return

    # The source configs are loaded while the state files are read, so a warning about one comes before the header.
    for warning in ingestion["warnings"]:
        print(warning)

    # Print grouped by source
    print("\n    Ingestion status:")
    for group in ingestion["sources"]:
        print(f"      platform: {group['source']}")
        for entry in group["entries"]:
            if entry["kind"] == "classifier":
                tag = f"classifier: {entry['classifier']}"
            elif entry["kind"] == "main":
                tag = "main data"
            elif entry["kind"] == "ml_classifiers":
                tag = "ml classifiers"
            else:
                tag = ""
            line = f"        - {entry['table']}: {entry['count']} datasets"
            if tag:
                line += f" ({tag})"
            if entry["in_progress"]:
                line += f" [in progress: {entry['in_progress']}]"
            if entry["failed_count"]:
                line += f" [{entry['failed_count']} failed]"
            print(line)
            details = []
            # A dataset whose name is "-" is not shown as the latest: "-" used to stand for "none".
            if entry["count"] and entry["latest"] != "-":
                details.append(f"latest: {entry['latest']}")
            if entry["last_updated"]:
                details.append(f"updated: {entry['last_updated'][:10]}")
            if details:
                print(f"            {', '.join(details)}")


def _state_file_facts(profile, data_type):
    """Return the `profile` and `data_type` fields of an ingestion entry, as parsed from the name of its state file (`<source>_<profile>_<data_type>`): the pipeline profile and the data type, or None for both when the name does not have that shape (the profile part is not an ingestion profile, or no data type follows it). The composed `table` and `label` of an entry are text for the terminal; these two are for a reader of the data."""
    if profile in INGESTION_PROFILES and data_type:
        return {"profile": profile, "data_type": data_type}
    return {"profile": None, "data_type": None}


def _get_ingestion_state(state_dir, label_split, db_type):
    """Read ingestion state from JSON files in a state directory. Returns `has_state_files`, the `warnings` about source configs that could not be read (in the order met) and `sources`: one group per source, sorted by name, each with its `entries` in file order. An entry has the `table` the terminal shows and, apart from it, the `profile` and `data_type` its file name says."""
    from social_data_pipeline.setup.utils import read_source_config

    ingestion = {"has_state_files": False, "warnings": [], "sources": []}

    if not state_dir.exists():
        return ingestion

    state_files = sorted(state_dir.glob("*.json"))
    if not state_files:
        return ingestion

    ingestion["has_state_files"] = True

    # Cache platform configs per source
    platform_cache = {}
    classifier_suffixes = None  # lazy-loaded

    # Collect entries grouped by source
    source_entries = {}  # {source: [entry, ...]}

    for sf in state_files:
        try:
            sdata = _parse_state_json(sf.read_text())
        except (json.JSONDecodeError, OSError):
            continue

        name = sf.stem
        processed = sdata.get("processed", [])
        failed = sdata.get("failed", [])
        in_progress = sdata.get("in_progress")
        last_updated = sdata.get("last_updated", "")

        parts = name.split(label_split)
        if len(parts) == 2:
            source = parts[0]
            profile_and_dt = parts[1]  # e.g., "ingest_comments" or "ml_comments"
            profile, _, data_type = profile_and_dt.partition("_")
        else:
            source = name
            profile = ""
            data_type = name
        # label_split is "_postgres_", "_mongo_" or "_sr_": with the part after it, it names the pipeline profile.
        named = _state_file_facts(f"{label_split[1:]}{profile}", data_type)

        # Load platform config for this source
        if source not in platform_cache:
            config, warning = read_source_config(source)
            if warning is not None:
                ingestion["warnings"].append(warning)
            platform_cache[source] = config or {}
        pconfig = platform_cache[source]

        if source not in source_entries:
            source_entries[source] = []

        if profile == "ml" and db_type == "postgres":
            # Group ML entries by classifier from processed entry prefixes
            if classifier_suffixes is None:
                classifier_suffixes = _load_classifier_suffixes()

            # Group processed entries by classifier (prefix before '/')
            by_classifier = {}
            for entry in processed:
                if "/" in entry:
                    cls_name, dataset = entry.split("/", 1)
                else:
                    cls_name, dataset = "unknown", entry
                by_classifier.setdefault(cls_name, []).append(dataset)

            # Group failed entries by classifier too
            failed_by_classifier = {}
            for f in failed:
                fname = f.get("filename", "") if isinstance(f, dict) else str(f)
                if "/" in fname:
                    cls_name = fname.split("/", 1)[0]
                else:
                    cls_name = "unknown"
                failed_by_classifier.setdefault(cls_name, []).append(f)

            schema = pconfig.get("db_schema", source)
            for cls_name, datasets in sorted(by_classifier.items()):
                suffix = classifier_suffixes.get(cls_name, f"_{cls_name}")
                table = f"{schema}.{data_type}{suffix}"
                count = len(datasets)
                latest = datasets[-1] if datasets else None
                cls_failed = failed_by_classifier.get(cls_name, [])
                source_entries[source].append({
                    "table": table,
                    "kind": "classifier",
                    "classifier": cls_name,
                    **named,
                    "count": count,
                    "latest": latest,
                    "in_progress": in_progress,
                    "failed_count": len(cls_failed),
                    "last_updated": last_updated,
                })
        else:
            # Main data (ingest profile, or SR ml — no per-classifier grouping).
            if db_type == "postgres":
                schema = pconfig.get("db_schema", source)
                table = f"{schema}.{data_type}"
            elif db_type == "sr":
                # StarRocks is database-per-source, like MongoDB.
                table = f"{source}.{data_type}"
            else:
                if "mongo_db_name" in pconfig:
                    table = _json_fact(pconfig["mongo_db_name"])
                else:
                    template = pconfig.get("mongo_db_name_template", "{platform}_{data_type}")
                    platform = pconfig.get("platform", source).replace("/", "_")
                    table = template.format(platform=platform, data_type=data_type)

            count = len(processed)
            latest = processed[-1] if processed else None
            if db_type == "postgres":
                kind = "main"
            elif db_type == "sr" and profile == "ml":
                kind = "ml_classifiers"
            else:
                kind = None
            source_entries[source].append({
                "table": table,
                "kind": kind,
                "classifier": None,
                **named,
                "count": count,
                "latest": latest,
                "in_progress": in_progress,
                "failed_count": len(failed) if failed else 0,
                "last_updated": last_updated,
            })

    ingestion["sources"] = [
        {"source": source, "entries": entries}
        for source, entries in sorted(source_entries.items())
    ]
    return ingestion


# ============================================================================
# sdp db unsetup
# ============================================================================

# Config files generated by db setup
DB_GENERATED_FILES = [
    "config/db/postgres.yaml",
    "config/db/mongo.yaml",
    "config/db/starrocks.yaml",
    "config/db/mcp.yaml",
    "config/postgres/postgresql.local.conf",
    "config/postgres/pg_hba.local.conf",
    "config/starrocks/fe.local.conf",
    "config/starrocks/be.local.conf",
    "docker-compose.override.yml",
    ".env",
]


def _unsetup_single_db(db_name, env, confirm=None):
    """Remove a single database entirely: containers, config, data, and shared-file entries.

    Containers, config, and data are always removed together — this is not a reconfigure path.
    Use 'sdp db setup --add <db>' to reconfigure a DB in place without losing data.

    `confirm` is called with each of the two confirmation questions and returns True to proceed; without it the removal stops with ActionError before anything is changed.
    """
    import yaml

    if db_name not in _get_configured_db_services():
        print(f"\n  No {db_name} configuration found.\n")
        return 0

    db_label = {"postgres": "PostgreSQL", "mongo": "MongoDB", "starrocks": "StarRocks"}[db_name]

    config_files_map = {
        "postgres": [
            "config/db/postgres.yaml",
            "config/postgres/postgresql.local.conf",
            "config/postgres/pg_hba.local.conf",
        ],
        "mongo": [
            "config/db/mongo.yaml",
        ],
        "starrocks": [
            "config/db/starrocks.yaml",
            "config/starrocks/fe.local.conf",
            "config/starrocks/be.local.conf",
        ],
    }
    data_env_var = {
        "postgres": "PGDATA_PATH",
        "mongo": "MONGO_DATA_PATH",
        "starrocks": "STARROCKS_DATA_PATH",
    }[db_name]
    env_keys_map = {
        "postgres": {"PGDATA_PATH", "DB_NAME", "POSTGRES_PORT", "POSTGRES_MEM_LIMIT",
                     "POSTGRES_AUTH_ENABLED", "POSTGRES_RO_USER",
                     "POSTGRES_MCP_PORT", "POSTGRES_MCP_ACCESS_MODE", "POSTGRES_MCP_USER"},
        "mongo": {"MONGO_DATA_PATH", "MONGO_PORT", "MONGO_CACHE_SIZE_GB", "MONGO_MEM_LIMIT",
                  "MONGO_AUTH_ENABLED", "MONGO_ADMIN_USER", "MONGO_RO_USER",
                  "MONGO_MCP_PORT", "MONGO_MCP_READ_ONLY", "MONGO_MCP_USER"},
        "starrocks": {"STARROCKS_DATA_PATH", "STARROCKS_PORT", "STARROCKS_FE_HTTP_PORT",
                      "STARROCKS_MEM_LIMIT", "STARROCKS_AUTH_ENABLED", "STARROCKS_RO_USER",
                      "STARROCKS_MCP_PORT", "STARROCKS_MCP_USER"},
    }
    env_keys = env_keys_map[db_name]

    print()
    header_text = f"Social Data Pipeline - Remove {db_label}"
    print(f"  {header_text}")
    print(f"  {'=' * len(header_text)}")
    print()

    # Read tablespaces (PG only) before configs are removed.
    tablespace_paths = {}
    if db_name == "postgres":
        for cfg_name in ("pipeline.yaml", "user.yaml"):
            cfg_file = CONFIG_DIR / "postgres" / cfg_name
            if cfg_file.exists():
                try:
                    cfg = yaml.safe_load(cfg_file.read_text()) or {}
                    ts = cfg.get("tablespaces") or cfg.get("pipeline", {}).get("tablespaces")
                    if ts and isinstance(ts, dict):
                        tablespace_paths.update(ts)
                except Exception:
                    pass
        pg_db_config = CONFIG_DIR / "db" / "postgres.yaml"
        if pg_db_config.exists():
            try:
                cfg = yaml.safe_load(pg_db_config.read_text()) or {}
                ts = cfg.get("tablespaces")
                if ts and isinstance(ts, dict):
                    tablespace_paths.update(ts)
            except Exception:
                pass

    # Read SR storage_paths (extra disks, parity with PG tablespaces) before
    # configs are removed. The list is positional in the yaml, so we name
    # them storage_0, storage_1, ... — matches the container path layout
    # written to docker-compose.override.yml at setup time.
    sr_storage_paths = _read_sr_storage_paths() if db_name == "starrocks" else {}

    # Read jobs targets that point at the DB being removed so we can
    # surface them: the operator needs to know the jobs scheduler will
    # start failing routes after this unsetup lands.
    orphaned_jobs_targets = _orphaned_jobs_targets_for(db_name)

    # --- Inventory ---
    print(f"  This will remove {db_label} completely:")
    print()

    existing_configs = [rel for rel in config_files_map[db_name] if (ROOT / rel).exists()]
    existing_baks = [rel + ".bak" for rel in config_files_map[db_name] if (ROOT / (rel + ".bak")).exists()]
    if existing_configs or existing_baks:
        print("  Config files:")
        for rel in existing_configs + existing_baks:
            print(f"    {rel}")
        print()

    data_path_str = env.get(data_env_var, "")
    data_path = None
    if data_path_str:
        dp = Path(data_path_str)
        if not dp.is_absolute():
            dp = ROOT / dp
        if dp.exists():
            data_path = dp
            print(f"  Data directory: {data_path_str}")
            if tablespace_paths:
                print("  Tablespace directories:")
                for ts_name, ts_path in tablespace_paths.items():
                    print(f"    {ts_name}: {ts_path}")
            if sr_storage_paths:
                print("  Extra storage directories:")
                for sp_name, sp_path in sr_storage_paths.items():
                    print(f"    {sp_name}: {sp_path}")
            print()

    if orphaned_jobs_targets:
        print(
            f"  [WARN] Jobs scheduler has target(s) pointing at {db_label}: "
            f"{', '.join(orphaned_jobs_targets)}"
        )
        print("         These will fail at next run. After this unsetup,")
        print("         run 'sdp db unsetup-jobs' or 'sdp db setup-jobs' to fix.")
        print()

    # --- Double confirmation ---
    if confirm is None:
        raise ActionError("confirmation required")
    confirm1 = confirm(
        f"  Delete {db_label} entirely? Config, containers, and data will be removed. "
        f"This CANNOT be undone [y/N]: "
    )
    if confirm1 is not True:
        print("\n  Aborted — nothing was changed.\n")
        return 0
    confirm2 = confirm(
        f"  Are you SURE? All {db_label} data will be permanently lost [y/N]: "
    )
    if confirm2 is not True:
        print("\n  Aborted — nothing was changed.\n")
        return 0
    print()

    # --- Stop containers ---
    print(f"  Stopping {db_label} containers...")
    docker_compose("--profile", db_name, "--profile", f"{db_name}_mcp",
                   "down", "--timeout", "30")
    print()

    # --- Delete data directory (+ tablespaces for PG / extra storage for SR) ---
    if data_path and data_path.exists():
        print("  Fixing directory permissions...")
        if db_name == "postgres":
            extra_paths = tablespace_paths
        elif db_name == "starrocks":
            extra_paths = sr_storage_paths
        else:
            extra_paths = {}
        ts_mounts = _chown_db_data_for_host(db_name, data_path, extra_paths)
        print(f"  Removing {data_path}...")
        shutil.rmtree(data_path)
        extra_label = "tablespace" if db_name == "postgres" else "extra storage"
        for ts_name, ts_dir in ts_mounts:
            if ts_dir.exists():
                print(f"  Removing {extra_label} '{ts_name}' at {ts_dir}...")
                shutil.rmtree(ts_dir)
        print(f"  {db_label} data removed.")
        print()

    # --- Remove .ro_credentials if still present ---
    if data_path_str:
        dp = Path(data_path_str)
        if not dp.is_absolute():
            dp = ROOT / dp
        cred_file = dp / ".ro_credentials"
        if cred_file.exists():
            try:
                cred_file.unlink()
                print(f"  Removed {cred_file}")
            except PermissionError:
                subprocess.run(
                    ["docker", "run", "--rm",
                     "-v", f"{cred_file.resolve().parent}:/data",
                     "alpine", "rm", "-f", "/data/.ro_credentials"],
                    cwd=ROOT, capture_output=True,
                )

    # --- Remove config files + .bak ---
    for rel in existing_configs + existing_baks:
        p = ROOT / rel
        if p.exists():
            p.unlink()
    if existing_configs or existing_baks:
        print(f"  Removed {len(existing_configs) + len(existing_baks)} config file(s).")

    # --- Remove per-source DB overrides ---
    _delete_source_db_overrides([db_name])

    # --- Strip DB keys from .env ---
    env_path = ROOT / ".env"
    if env_path.exists():
        lines = env_path.read_text().splitlines()
        new_lines = []
        for line in lines:
            stripped = line.lstrip("# ").strip()
            key = stripped.split("=", 1)[0] if "=" in stripped else stripped
            if key not in env_keys:
                new_lines.append(line)
        env_path.write_text("\n".join(new_lines) + "\n")
        print("  Updated:   .env")

    # --- Strip DB services block from docker-compose.override.yml ---
    override_path = ROOT / "docker-compose.override.yml"
    if override_path.exists():
        try:
            data = yaml.safe_load(override_path.read_text()) or {}
        except yaml.YAMLError:
            data = {}
        services = data.get("services") or {}
        if db_name in services:
            services.pop(db_name)
            if services:
                data["services"] = services
                header = (
                    "# Auto-generated by sdp — volume mounts for database servers.\n"
                    "# Setup mounts (tablespaces, SR storage) + per-source data mounts from sdp db start.\n"
                    "\n"
                )
                body = yaml.dump(data, default_flow_style=False, sort_keys=False)
                override_path.write_text(header + body)
                print("  Updated:   docker-compose.override.yml")
            else:
                override_path.unlink()
                print("  Removed:   docker-compose.override.yml")

    # --- Strip DB subtree from config/db/mcp.yaml ---
    mcp_path = CONFIG_DIR / "db" / "mcp.yaml"
    if mcp_path.exists():
        try:
            mcp_cfg = yaml.safe_load(mcp_path.read_text()) or {}
        except yaml.YAMLError:
            mcp_cfg = {}
        if db_name in mcp_cfg:
            mcp_cfg.pop(db_name)
            if mcp_cfg:
                mcp_path.write_text(yaml.dump(mcp_cfg, default_flow_style=False, sort_keys=False))
                print("  Updated:   config/db/mcp.yaml")
            else:
                mcp_path.unlink()
                print("  Removed:   config/db/mcp.yaml")

    print()
    print(f"  {db_label} removed. Other databases are not affected.\n")
    return 0


def do_db_unsetup(*, db, _confirm=None):
    """Remove one database entirely: containers, config, data, and shared-file entries. Asks nothing itself: `_confirm` answers the two confirmation questions. Returns the exit code."""
    env = load_env()
    return _unsetup_single_db(db, env, confirm=_confirm)


def cmd_db_unsetup(args):
    """Remove database configuration and optionally database data."""
    if getattr(args, "db", None):
        try:
            return do_db_unsetup(db=args.db, _confirm=_confirm_on_terminal)
        except ActionError as e:
            print(f"\n  Error: {e.message}\n")
            return e.exit_code

    env = load_env()

    # Capture which DBs are configured BEFORE we delete their config files,
    # so we know which per-source overrides to clean up at the end.
    configured_dbs = _get_configured_db_services()

    print()
    print("  Social Data Pipeline - Database Unsetup")
    print("  ======================================")
    print()

    # --- Stop and remove all DB + MCP containers ---
    # Must happen first so containers don't hold stale volume mounts
    # after config/data is removed. Includes MCP profiles.
    all_profiles = []
    for p in ("postgres", "postgres_mcp", "mongo", "mongo_mcp", "starrocks", "starrocks_mcp"):
        all_profiles += ["--profile", p]
    print("  Stopping all database containers...")
    docker_compose(*all_profiles, "down", "--timeout", "30")
    print()

    # --- Find config files to remove ---
    removed = []
    for rel in DB_GENERATED_FILES:
        path = ROOT / rel
        if path.exists():
            removed.append(rel)

    # Also find .bak files
    backups = []
    for rel in DB_GENERATED_FILES:
        bak = ROOT / (rel + ".bak")
        if bak.exists():
            backups.append(rel + ".bak")

    if removed or backups:
        print("  Database config files to remove:")
        for rel in removed + backups:
            print(f"    {rel}")
        print()
    else:
        print("  No database config files found.\n")

    # --- PostgreSQL data removal (double confirmation) ---
    pgdata_path = env.get("PGDATA_PATH", "")
    db_removed = False

    tablespace_paths = {}
    try:
        import yaml
        for cfg_name in ("pipeline.yaml", "user.yaml"):
            cfg_file = CONFIG_DIR / "postgres" / cfg_name
            if cfg_file.exists():
                cfg = yaml.safe_load(cfg_file.read_text()) or {}
                ts = cfg.get("tablespaces") or cfg.get("pipeline", {}).get("tablespaces")
                if ts and isinstance(ts, dict):
                    tablespace_paths.update(ts)
        # Also check config/db/postgres.yaml
        pg_db_config = CONFIG_DIR / "db" / "postgres.yaml"
        if pg_db_config.exists():
            cfg = yaml.safe_load(pg_db_config.read_text()) or {}
            ts = cfg.get("tablespaces")
            if ts and isinstance(ts, dict):
                tablespace_paths.update(ts)
    except Exception:
        pass
    # SR storage_paths via the shared helper (parity with PG tablespaces).
    sr_storage_paths = _read_sr_storage_paths()

    if pgdata_path:
        pgdata = Path(pgdata_path)
        if not pgdata.is_absolute():
            pgdata = ROOT / pgdata
        if pgdata.exists():
            print(f"  Database directory: {pgdata_path}")
            if tablespace_paths:
                print("  Tablespace directories:")
                for ts_name, ts_path in tablespace_paths.items():
                    print(f"    {ts_name}: {ts_path}")
            print()
            confirm1 = input("  Delete the PostgreSQL database? This CANNOT be undone [y/N]: ").strip().lower()
            if confirm1 in ("y", "yes"):
                confirm2 = input("  Are you SURE? All database data will be permanently lost [y/N]: ").strip().lower()
                if confirm2 in ("y", "yes"):
                    print()
                    print("  Fixing directory permissions...")
                    db_parent = pgdata.resolve().parent
                    volume_args = ["-v", f"{db_parent}:/dbparent"]
                    recursive = ["/dbparent"]
                    # Tablespace parents need non-recursive chown so we can unlink
                    # the tablespace dir itself — can't recurse, they may contain
                    # sibling DB dirs we must not touch.
                    non_recursive = []
                    ts_mounts = []
                    for ts_name, ts_path in tablespace_paths.items():
                        ts_dir = Path(ts_path)
                        if not ts_dir.is_absolute():
                            ts_dir = ROOT / ts_dir
                        if ts_dir.exists():
                            volume_args += ["-v", f"{ts_dir.resolve()}:/tablespace/{ts_name}"]
                            recursive.append(f"/tablespace/{ts_name}")
                            ts_parent_mount = f"/tsparent_{ts_name}"
                            volume_args += ["-v", f"{ts_dir.resolve().parent}:{ts_parent_mount}"]
                            non_recursive.append(ts_parent_mount)
                            ts_mounts.append((ts_name, ts_dir))
                    uid_gid = f"{os.getuid()}:{os.getgid()}"
                    chown_cmd = f"chown -R {uid_gid} {' '.join(recursive)}"
                    if non_recursive:
                        chown_cmd += f" && chown {uid_gid} {' '.join(non_recursive)}"
                    subprocess.run(
                        ["docker", "run", "--rm"] + volume_args +
                        ["postgres:18", "sh", "-c", chown_cmd],
                        cwd=ROOT,
                    )
                    print(f"  Removing {pgdata}...")
                    shutil.rmtree(pgdata)
                    for ts_name, ts_dir in ts_mounts:
                        if ts_dir.exists():
                            print(f"  Removing tablespace '{ts_name}' at {ts_dir}...")
                            shutil.rmtree(ts_dir)
                    db_removed = True
                    print("  Database removed.")
                else:
                    print("  Database removal skipped.")
            else:
                print("  Database removal skipped.")
            print()

    # --- MongoDB data removal ---
    mongo_data_path = env.get("MONGO_DATA_PATH", "")
    mongo_removed = False

    if mongo_data_path:
        mongo_data = Path(mongo_data_path)
        if not mongo_data.is_absolute():
            mongo_data = ROOT / mongo_data
        if mongo_data.exists():
            print(f"  MongoDB data directory: {mongo_data_path}")
            print()
            confirm1 = input("  Delete the MongoDB data? This CANNOT be undone [y/N]: ").strip().lower()
            if confirm1 in ("y", "yes"):
                confirm2 = input("  Are you SURE? All MongoDB data will be permanently lost [y/N]: ").strip().lower()
                if confirm2 in ("y", "yes"):
                    print()
                    print("  Fixing directory permissions...")
                    mongo_parent = mongo_data.resolve().parent
                    uid_gid = f"{os.getuid()}:{os.getgid()}"
                    subprocess.run(
                        ["docker", "run", "--rm",
                         "-v", f"{mongo_parent}:/dbparent",
                         "mongo:8", "chown", "-R", uid_gid, "/dbparent"],
                        cwd=ROOT,
                    )
                    print(f"  Removing {mongo_data}...")
                    shutil.rmtree(mongo_data)
                    mongo_removed = True
                    print("  MongoDB data removed.")
                else:
                    print("  MongoDB data removal skipped.")
            else:
                print("  MongoDB data removal skipped.")
            print()

    # --- StarRocks data removal ---
    sr_data_path = env.get("STARROCKS_DATA_PATH", "")
    sr_removed = False

    if sr_data_path:
        sr_data = Path(sr_data_path)
        if not sr_data.is_absolute():
            sr_data = ROOT / sr_data
        if sr_data.exists():
            print(f"  StarRocks data directory: {sr_data_path}")
            if sr_storage_paths:
                print("  Extra storage directories:")
                for sp_name, sp_path in sr_storage_paths.items():
                    print(f"    {sp_name}: {sp_path}")
            print()
            confirm1 = input("  Delete the StarRocks data? This CANNOT be undone [y/N]: ").strip().lower()
            if confirm1 in ("y", "yes"):
                confirm2 = input("  Are you SURE? All StarRocks data will be permanently lost [y/N]: ").strip().lower()
                if confirm2 in ("y", "yes"):
                    print()
                    print("  Fixing directory permissions...")
                    sr_parent = sr_data.resolve().parent
                    volume_args = ["-v", f"{sr_parent}:/dbparent"]
                    recursive = ["/dbparent"]
                    # Extra-storage parents need non-recursive chown so we can
                    # unlink the storage dir itself — can't recurse, they may
                    # contain sibling data we must not touch (parity with PG
                    # tablespace handling above).
                    non_recursive = []
                    sp_mounts = []
                    for sp_name, sp_path in sr_storage_paths.items():
                        sp_dir = Path(sp_path)
                        if not sp_dir.is_absolute():
                            sp_dir = ROOT / sp_dir
                        if sp_dir.exists():
                            volume_args += ["-v", f"{sp_dir.resolve()}:/storage/{sp_name}"]
                            recursive.append(f"/storage/{sp_name}")
                            sp_parent_mount = f"/spparent_{sp_name}"
                            volume_args += ["-v", f"{sp_dir.resolve().parent}:{sp_parent_mount}"]
                            non_recursive.append(sp_parent_mount)
                            sp_mounts.append((sp_name, sp_dir))
                    uid_gid = f"{os.getuid()}:{os.getgid()}"
                    chown_cmd = f"chown -R {uid_gid} {' '.join(recursive)}"
                    if non_recursive:
                        chown_cmd += f" && chown {uid_gid} {' '.join(non_recursive)}"
                    subprocess.run(
                        ["docker", "run", "--rm"] + volume_args +
                        ["starrocks/allin1-ubuntu", "sh", "-c", chown_cmd],
                        cwd=ROOT,
                    )
                    print(f"  Removing {sr_data}...")
                    shutil.rmtree(sr_data)
                    for sp_name, sp_dir in sp_mounts:
                        if sp_dir.exists():
                            print(f"  Removing extra storage '{sp_name}' at {sp_dir}...")
                            shutil.rmtree(sp_dir)
                    sr_removed = True
                    print("  StarRocks data removed.")
                else:
                    print("  StarRocks data removal skipped.")
            else:
                print("  StarRocks data removal skipped.")
            print()

    # --- Remove RO credentials from data directories ---
    for data_path_str in (pgdata_path, mongo_data_path, sr_data_path):
        if not data_path_str:
            continue
        dp = Path(data_path_str)
        if not dp.is_absolute():
            dp = ROOT / dp
        cred_file = dp / ".ro_credentials"
        if cred_file.exists():
            try:
                cred_file.unlink()
                print(f"  Removed {cred_file}")
            except PermissionError:
                # File owned by container user (root) — fix via docker
                subprocess.run(
                    ["docker", "run", "--rm",
                     "-v", f"{cred_file.resolve().parent}:/data",
                     "alpine", "rm", "-f", "/data/.ro_credentials"],
                    cwd=ROOT, capture_output=True,
                )
                if not cred_file.exists():
                    print(f"  Removed {cred_file}")
                else:
                    print(f"  [!] Could not remove {cred_file} — run: sudo rm {cred_file}")

    # --- Remove config files ---
    if removed or backups:
        for rel in removed + backups:
            (ROOT / rel).unlink()
        print(f"  Removed {len(removed) + len(backups)} config file(s).")
    else:
        print("  Nothing to remove.")

    # --- Remove per-source DB overrides for the DBs we just unsetup ---
    if configured_dbs:
        _delete_source_db_overrides(configured_dbs)

    # --- Print remaining data paths ---
    data_paths = {}
    if not db_removed and pgdata_path:
        pgdata = Path(pgdata_path)
        if not pgdata.is_absolute():
            pgdata = ROOT / pgdata
        if pgdata.exists():
            data_paths["PostgreSQL data"] = pgdata_path
            for ts_name, ts_path in tablespace_paths.items():
                data_paths[f"Tablespace '{ts_name}'"] = ts_path
    if not mongo_removed and mongo_data_path:
        mongo_data = Path(mongo_data_path)
        if not mongo_data.is_absolute():
            mongo_data = ROOT / mongo_data
        if mongo_data.exists():
            data_paths["MongoDB data"] = mongo_data_path
    if not sr_removed and sr_data_path:
        sr_data = Path(sr_data_path)
        if not sr_data.is_absolute():
            sr_data = ROOT / sr_data
        if sr_data.exists():
            data_paths["StarRocks data"] = sr_data_path
            for sp_name, sp_path in sr_storage_paths.items():
                data_paths[f"StarRocks {sp_name}"] = sp_path

    if data_paths:
        print()
        print("  Data files were NOT removed. Delete manually if needed:")
        for label, path in data_paths.items():
            print(f"    {label}: {path}")

    print()
    print("  Database unsetup complete.\n")
    return 0


# ============================================================================
# sdp db reset
# ============================================================================

# Server-owned subdirectories under <DB>_DATA_PATH that `db reset` wipes.
# Everything else inside the data dir (.ro_credentials, .bak files, etc.) is
# preserved. Each entry is recreated as an empty directory after wipe so the
# DB starts cleanly on the next `db start`.
_DB_RESET_SUBDIRS = {
    "postgres": ["pgdata", "state_tracking"],
    "mongo": ["db", "state_tracking", "logs"],
    "starrocks": ["fe", "be", "state_tracking"],
}

# Compose service names that read/write the DB. `db reset` refuses to run
# while any of these are running — otherwise the orchestrator container
# keeps writing into state_tracking/ as we delete it.
_DB_INGEST_SERVICES = {
    "postgres": {"postgres-ingest", "postgres-ml"},
    "mongo": {"mongo-ingest"},
    "starrocks": {"sr-ingest", "sr-ml"},
}


def _read_pg_tablespace_paths():
    """Read PG tablespace host paths from pipeline/user/db yaml.

    Same union the unsetup path computes (see `_unsetup_single_db`): merge
    `tablespaces:` from `config/postgres/pipeline.yaml`,
    `config/postgres/user.yaml`, and `config/db/postgres.yaml`. Returns
    ``{ts_name: host_path}``; empty when no tablespaces are configured.
    Defensive against missing/malformed yaml — reset must still be able to
    wipe pgdata/ even if a tablespace yaml is broken.
    """
    import yaml
    tablespace_paths = {}
    for cfg_name in ("pipeline.yaml", "user.yaml"):
        cfg_file = CONFIG_DIR / "postgres" / cfg_name
        if cfg_file.exists():
            try:
                cfg = yaml.safe_load(cfg_file.read_text()) or {}
                ts = cfg.get("tablespaces") or cfg.get("pipeline", {}).get("tablespaces")
                if ts and isinstance(ts, dict):
                    tablespace_paths.update(ts)
            except Exception:
                pass
    pg_db_config = CONFIG_DIR / "db" / "postgres.yaml"
    if pg_db_config.exists():
        try:
            cfg = yaml.safe_load(pg_db_config.read_text()) or {}
            ts = cfg.get("tablespaces")
            if ts and isinstance(ts, dict):
                tablespace_paths.update(ts)
        except Exception:
            pass
    return tablespace_paths


def cmd_db_reset(args):
    """Wipe a database's data; preserve all config."""
    try:
        return do_db_reset(db=args.db, _confirm=_confirm_on_terminal)
    except ActionError as e:
        print(f"\n  Error: {e.message}\n")
        return e.exit_code


def do_db_reset(*, db, _confirm=None):
    """Wipe a database's data; preserve all config.

    Deletes only the DB server's own subdirectories under ``<DB>_DATA_PATH``
    (``pgdata/``, ``db/``, ``fe/``+``be/``), plus host-side ``state_tracking/``
    and mongo's ``logs/``, plus the contents of any PG tablespace or SR
    ``storage_paths`` directories. Everything else is preserved:
    ``config/db/<db>.yaml``, ``config/sources/*/<db>.yaml``, ``.env``,
    ``docker-compose.override.yml``, ``config/db/mcp.yaml``, and
    ``.ro_credentials`` (the entrypoint re-creates the RO user from that file
    on the next start).

    Use when the data is corrupt or you want to re-ingest from scratch.
    Use ``db unsetup --db <name>`` instead to remove the DB entirely (config
    included).

    Asks nothing itself: `_confirm` is called with the confirmation question and returns True to proceed; without it the reset stops with ActionError before anything is changed. Returns the exit code.
    """
    db_name = db
    env = load_env()

    if db_name not in _get_configured_db_services():
        print(f"\n  No {db_name} configuration found.\n")
        return 0

    db_label = {"postgres": "PostgreSQL", "mongo": "MongoDB", "starrocks": "StarRocks"}[db_name]
    data_env_var = _DB_DATA_PATH_ENV[db_name]
    server_subdirs = _DB_RESET_SUBDIRS[db_name]

    # --- Resolve data path ---
    data_path_str = env.get(data_env_var, "")
    if not data_path_str:
        print(f"\n  No {data_env_var} in .env — nothing to reset.\n")
        return 0
    data_path = Path(data_path_str)
    if not data_path.is_absolute():
        data_path = ROOT / data_path
    if not data_path.exists():
        print(f"\n  {data_path} does not exist — nothing to reset.\n")
        return 0

    # --- Read extras (PG tablespaces / SR storage_paths) ---
    if db_name == "postgres":
        extra_paths = _read_pg_tablespace_paths()
        extra_label = "Tablespace"
    elif db_name == "starrocks":
        extra_paths = _read_sr_storage_paths()
        extra_label = "Extra storage"
    else:
        extra_paths = {}
        extra_label = ""

    # --- In-flight ingest guard ---
    # Without this, an `sdp run <ingest>` container keeps writing into
    # state_tracking/ while we're deleting it, producing corrupt state files
    # the next ingest would then read.
    in_flight = _running_services() & _DB_INGEST_SERVICES[db_name]
    if in_flight:
        print(
            f"\n  [ERR] Ingest container(s) running against {db_label}: "
            f"{', '.join(sorted(in_flight))}"
        )
        print("        Stop them before resetting (mid-write corruption otherwise).\n")
        return 1

    orphaned_jobs_targets = _orphaned_jobs_targets_for(db_name)

    # --- Inventory ---
    print()
    header_text = f"Social Data Pipeline - Reset {db_label}"
    print(f"  {header_text}")
    print(f"  {'=' * len(header_text)}")
    print()
    print(f"  This wipes {db_label} data and keeps your configuration.")
    print()
    print("  Will be deleted:")
    any_listed = False
    for sub in server_subdirs:
        p = data_path / sub
        if p.exists():
            print(f"    {p}")
            any_listed = True
    for name, host_path in extra_paths.items():
        p = Path(host_path)
        if not p.is_absolute():
            p = ROOT / p
        if p.exists():
            print(f"    {extra_label} '{name}' contents: {p}")
            any_listed = True
    if not any_listed:
        print("    (nothing to delete — data dirs are already empty)")
    print()
    print("  Will be preserved:")
    print(f"    config/db/{db_name}.yaml, config/db/mcp.yaml")
    print(f"    config/sources/*/{db_name}.yaml")
    print("    .env, docker-compose.override.yml")
    cred_file = data_path / ".ro_credentials"
    if cred_file.exists():
        print(f"    {cred_file} (RO user re-created on next start)")
    print()
    if orphaned_jobs_targets:
        print(
            f"  [WARN] Jobs scheduler has target(s) pointing at {db_label}: "
            f"{', '.join(orphaned_jobs_targets)}"
        )
        print("         They will fail until you run 'sdp db start' again.")
        print()

    if _confirm is None:
        raise ActionError("confirmation required")
    confirm = _confirm(
        f"  Reset {db_label} data? This CANNOT be undone [y/N]: "
    )
    if confirm is not True:
        print("\n  Aborted — nothing was changed.\n")
        return 0
    print()

    # --- Stop containers (DB + MCP profile only) ---
    print(f"  Stopping {db_label} containers...")
    docker_compose("--profile", db_name, "--profile", f"{db_name}_mcp",
                   "down", "--timeout", "30")
    print()

    # --- Chown + wipe ---
    print("  Fixing directory permissions...")
    ts_mounts = _chown_db_data_for_host(db_name, data_path, extra_paths)

    for sub in server_subdirs:
        p = data_path / sub
        if p.exists():
            print(f"  Removing {p}...")
            shutil.rmtree(p)
            p.mkdir()

    # PG tablespaces and SR storage_paths: wipe CONTENTS, preserve the dir
    # itself so the docker-compose mount still resolves on next `db start`.
    for name, p in ts_mounts:
        if not p.exists():
            continue
        print(f"  Wiping {extra_label.lower()} '{name}' at {p}...")
        for child in p.iterdir():
            if child.is_dir() and not child.is_symlink():
                shutil.rmtree(child)
            else:
                child.unlink()

    print()
    print(f"  {db_label} data reset complete.")
    print(f"  Next: sdp db start {db_name}")
    print(f"        sdp run {db_name}_ingest --source <name>")
    print()
    return 0


# ============================================================================
# sdp db recover-password
# ============================================================================

def cmd_db_recover_password(args):
    """Reset database admin password by temporarily using trust auth.

    With `--regenerate-ro`, also writes a fresh `.ro_credentials` per DB
    before each branch's final restart. The DB entrypoint syncs the RO
    user from that file on startup, so the new RO password takes effect
    on the same restart cycle that re-applies admin auth.
    """
    env = load_env()

    if not _needs_admin_password():
        print("\n  Authentication is not enabled. Nothing to recover.\n")
        return 0

    regenerate_ro = bool(getattr(args, "regenerate_ro", False))

    print()
    print("  Social Data Pipeline - Password Recovery")
    print("  ========================================")
    print()
    print("  This will temporarily restart PostgreSQL with trust auth,")
    print("  set a new admin password, and restore scram-sha-256 auth.")
    if regenerate_ro:
        print("  --regenerate-ro: also rewriting .ro_credentials with a fresh password.")
    print()

    _tag = lambda t: f"[{t}] " if os.environ.get('SDP_TAGGED_MODE') else ""
    new_password = getpass(f"  {_tag('sdp_recover_password')}New admin password: ")
    if not new_password:
        print("  Error: Password cannot be empty.")
        return 1
    confirm = getpass(f"  {_tag('sdp_recover_password_confirm')}Confirm new password: ")
    if new_password != confirm:
        print("  Error: Passwords do not match.")
        return 1

    # Set auth env up front. The PG entrypoint's auth-init block runs
    # ALTER USER postgres WITH PASSWORD '${POSTGRES_PASSWORD}' on every
    # start under AUTH_ENABLED=true; without this, intermediate restarts
    # would silently set the admin password to the empty string before
    # the explicit psql ALTER below could correct it.
    _set_auth_env(new_password)

    new_ro_password = secrets.token_urlsafe(24) if regenerate_ro else None

    configured = _get_configured_db_services()

    # --- PostgreSQL recovery ---
    if "postgres" in configured:
        pg_config = _load_db_yaml("postgres")
        if pg_config.get("auth"):
            print("\n  Recovering PostgreSQL password...")

            pg_hba_local = CONFIG_DIR / "postgres" / "pg_hba.local.conf"
            pg_hba_backup = CONFIG_DIR / "postgres" / "pg_hba.local.conf.recovery_bak"

            if not pg_hba_local.exists():
                print("  Error: pg_hba.local.conf not found. Is auth configured?")
                return 1

            # Backup current pg_hba and replace with trust-only version
            shutil.copy2(pg_hba_local, pg_hba_backup)
            trust_hba = (
                "# TEMPORARY — trust auth for password recovery\n"
                "local   all   all   trust\n"
                "host    all   all   0.0.0.0/0   trust\n"
            )
            pg_hba_local.write_text(trust_hba)

            # Restart postgres with trust auth
            print("  Restarting PostgreSQL with trust auth...")
            restart_result = docker_compose("--profile", "postgres", "restart")
            if restart_result.returncode != 0:
                shutil.copy2(pg_hba_backup, pg_hba_local)
                pg_hba_backup.unlink(missing_ok=True)
                print("  Error: trust-auth restart failed; pg_hba restored.")
                return 1

            # `docker compose restart` returns when the container is in
            # state 'running' (entrypoint executing), not when PG is
            # accepting connections. Poll pg_isready until it succeeds —
            # without this, the next psql ALTER hits "no response" mid-
            # restart, swallows the failure path, and recover-password
            # exits in a half-applied state.
            import time
            print("  Waiting for PostgreSQL to accept connections...")
            port = env.get("POSTGRES_PORT", "5432")
            db_name = env.get("DB_NAME", "datasets")
            deadline = time.time() + 60
            ready = False
            while time.time() < deadline:
                probe = subprocess.run(
                    ["docker", "compose", "exec", "-T", "postgres",
                     "pg_isready", "-U", "postgres", "-d", db_name, "-p", port],
                    cwd=ROOT, capture_output=True,
                )
                if probe.returncode == 0:
                    ready = True
                    break
                time.sleep(2)
            if not ready:
                shutil.copy2(pg_hba_backup, pg_hba_local)
                pg_hba_backup.unlink(missing_ok=True)
                print("  Error: PostgreSQL did not accept connections within 60s.")
                return 1

            # Set new password (PGPASSWORD env avoids shell interpolation of password).
            escaped_pw = new_password.replace("'", "''")
            alter_cmd = f"ALTER USER postgres WITH PASSWORD '{escaped_pw}'"
            result = subprocess.run(
                ["docker", "compose", "exec", "-T", "postgres",
                 "psql", "-U", "postgres", "-p", port, "-c", alter_cmd],
                cwd=ROOT, capture_output=True, text=True,
            )
            if result.returncode != 0:
                # Fail loud — the previous "restart on error" branch hung
                # indefinitely under compose because the trust restart had
                # already left the container in a state that didn't tolerate
                # a second restart cleanly. Restore pg_hba and return.
                shutil.copy2(pg_hba_backup, pg_hba_local)
                pg_hba_backup.unlink(missing_ok=True)
                print(
                    "  Error: Failed to set new password.\n"
                    f"  psql stderr: {result.stderr.strip()}\n"
                    "  pg_hba restored. Run 'sdp db stop postgres' and 'sdp db start postgres' to return to scram-sha-256."
                )
                return 1

            # Restore original pg_hba
            shutil.copy2(pg_hba_backup, pg_hba_local)
            pg_hba_backup.unlink(missing_ok=True)

            # Rewrite .ro_credentials so the entrypoint's RO sync block
            # picks up the new password on the final restart.
            if new_ro_password and pg_config.get("ro_username"):
                _regenerate_ro_credentials_for(["postgres"], password=new_ro_password)

            # Down + up -d (was 'restart'; mongo/sr already use this pattern)
            # so the new POSTGRES_PASSWORD env var is applied via container
            # recreation rather than an in-place restart that may not pick
            # up env changes consistently.
            print("  Restoring scram-sha-256 auth and restarting...")
            docker_compose("--profile", "postgres", "down", "--timeout", "30")
            docker_compose("--profile", "postgres", "up", "-d")
            print("  PostgreSQL password updated successfully.")

    # --- MongoDB recovery ---
    if "mongo" in configured:
        mongo_config = _load_db_yaml("mongo")
        if mongo_config.get("auth"):
            print("\n  Recovering MongoDB password...")
            mongo_admin_user = env.get("MONGO_ADMIN_USER", "admin")

            # Stop mongo
            print("  Stopping MongoDB...")
            docker_compose("--profile", "mongo", "down", "--timeout", "30")

            # Start mongo without auth temporarily
            mongo_data_path = env.get("MONGO_DATA_PATH", "./data/database/mongo")
            mongo_cache = env.get("MONGO_CACHE_SIZE_GB", "2")

            print("  Starting MongoDB without auth for recovery...")
            # The recovery mongod MUST see the same config file the real
            # mongo container does — historically we passed an ad-hoc flag
            # list ("--directoryperdb", "--bind_ip 0.0.0.0", default
            # compressors) and re-discovered every drift the hard way:
            # mismatched directoryPerDB refused to open the dataset;
            # missing journalCompressor wrote snappy journal entries into
            # a zstd-configured dataset and corrupted it on next start.
            # Mount config/mongo/mongod.conf at the same in-container path
            # the compose service uses (/etc/mongo/config/mongod.conf,
            # docker-compose.yml line 220) and invoke mongod with --config
            # exactly like config/mongo/entrypoint-wrapper.sh line 14 does.
            # The only CLI override is --wiredTigerCacheSizeGB, mirroring
            # the real wrapper — it's the one knob that lives in .env, not
            # in mongod.conf.
            # No --rm — we retain the container so `docker logs` can
            # surface mongod errors if it fails to start. We `docker rm -f`
            # on every exit path.
            mongod_conf_host = ROOT / "config" / "mongo" / "mongod.conf"
            if not mongod_conf_host.exists():
                print(f"  Error: {mongod_conf_host} not found. Is mongo configured?")
                return 1
            subprocess.run(["docker", "rm", "-f", "sdp-mongo-recovery"],
                           capture_output=True)
            result = subprocess.run(
                ["docker", "run", "-d",
                 "--name", "sdp-mongo-recovery",
                 "-v", f"{mongo_data_path}/db:/data/db",
                 "-v", f"{mongod_conf_host}:/etc/mongo/config/mongod.conf:ro",
                 "mongo:8",
                 "mongod",
                 "--config", "/etc/mongo/config/mongod.conf",
                 "--wiredTigerCacheSizeGB", mongo_cache],
                cwd=ROOT, capture_output=True, text=True,
            )
            if result.returncode != 0:
                print(f"  Error: Failed to start recovery container: {result.stderr}")
                return 1

            # Wait for mongod to be ready. If the container exits before it
            # responds (e.g. storage-layout mismatch), `docker exec` keeps
            # returning "no such container" and the loop would silently fall
            # through to the password update with the same error. Detect a
            # dead container early and surface its actual mongod logs.
            import time
            ready = False
            for _ in range(15):
                check = subprocess.run(
                    ["docker", "exec", "sdp-mongo-recovery",
                     "mongosh", "--quiet", "--eval", "db.adminCommand('ping')"],
                    capture_output=True, text=True,
                )
                if check.returncode == 0:
                    ready = True
                    break
                alive = subprocess.run(
                    ["docker", "ps", "-q", "-f", "name=^sdp-mongo-recovery$"],
                    capture_output=True, text=True,
                )
                if alive.stdout.strip() == "":
                    break
                time.sleep(2)
            if not ready:
                logs = subprocess.run(
                    ["docker", "logs", "--tail", "40", "sdp-mongo-recovery"],
                    capture_output=True, text=True,
                )
                print("  Error: recovery mongod failed to come up.")
                tail = (logs.stderr or logs.stdout).strip()
                if tail:
                    print("  --- mongod logs (tail) ---")
                    for ln in tail.splitlines():
                        print(f"  {ln}")
                subprocess.run(["docker", "rm", "-f", "sdp-mongo-recovery"],
                               capture_output=True)
                return 1

            # Update password (escape single quotes for JS string literal)
            escaped_user = mongo_admin_user.replace("'", "\\'")
            escaped_pw = new_password.replace("\\", "\\\\").replace("'", "\\'")
            update_script = (
                "db = db.getSiblingDB('admin');"
                f"db.changeUserPassword('{escaped_user}', '{escaped_pw}');"
                "print('Password updated');"
            )
            result = subprocess.run(
                ["docker", "exec", "sdp-mongo-recovery",
                 "mongosh", "--quiet", "--eval", update_script],
                cwd=ROOT,
            )

            # Stop and remove recovery container (no --rm at create time).
            subprocess.run(["docker", "rm", "-f", "sdp-mongo-recovery"],
                           capture_output=True)

            if result.returncode != 0:
                print("  Error: Failed to update MongoDB password.")
                return 1

            print("  MongoDB password updated successfully.")

            # Rewrite .ro_credentials so the entrypoint's RO sync block
            # picks up the new password on the restart below.
            if new_ro_password and mongo_config.get("ro_username"):
                _regenerate_ro_credentials_for(["mongo"], password=new_ro_password)

            # Restart mongo with auth
            print("  Restarting MongoDB with auth...")
            os.environ["MONGO_ADMIN_PASSWORD"] = new_password
            docker_compose("--profile", "mongo", "up", "-d")

    # --- StarRocks recovery ---
    if "starrocks" in configured:
        sr_config = _load_db_yaml("starrocks")
        if sr_config.get("auth"):
            print("\n  Recovering StarRocks password...")
            print("  This will temporarily restart StarRocks with auth checks disabled,")
            print("  set a new root password, and restore normal auth.")
            print()

            # Stop StarRocks
            print("  Stopping StarRocks...")
            docker_compose("--profile", "starrocks", "down", "--timeout", "120")

            # Backup fe.local.conf and add enable_auth_check = false
            fe_conf_path = CONFIG_DIR / "starrocks" / "fe.local.conf"
            fe_conf_backup = CONFIG_DIR / "starrocks" / "fe.local.conf.recovery_bak"

            if not fe_conf_path.exists():
                print("  Error: fe.local.conf not found. Is StarRocks configured?")
                return 1

            shutil.copy2(fe_conf_path, fe_conf_backup)
            fe_content = fe_conf_path.read_text()
            fe_content = fe_content.rstrip("\n") + "\n\n# TEMPORARY — auth bypass for password recovery\nenable_auth_check = false\n"
            fe_conf_path.write_text(fe_content)

            # Start StarRocks with auth disabled
            print("  Starting StarRocks with auth checks disabled...")
            docker_compose("--profile", "starrocks", "up", "-d", "--wait")

            # Set new password
            sr_port = env.get("STARROCKS_PORT", "9030")
            escaped_pw = new_password.replace("'", "''")
            alter_cmd = f"ALTER USER root IDENTIFIED BY '{escaped_pw}'"
            result = subprocess.run(
                ["docker", "compose", "exec", "starrocks",
                 "mysql", "-h", "127.0.0.1", "-P", sr_port,
                 "-u", "root", "--skip-password", "-e", alter_cmd],
                cwd=ROOT,
            )
            if result.returncode != 0:
                print("  Error: Failed to set new StarRocks password.")
                # Restore backup
                shutil.copy2(fe_conf_backup, fe_conf_path)
                fe_conf_backup.unlink(missing_ok=True)
                docker_compose("--profile", "starrocks", "down", "--timeout", "120")
                return 1

            # Rewrite .ro_credentials so the inline RO sync below (and the
            # entrypoint's own sync block on the final restart) both pick up
            # the new password.
            if new_ro_password and sr_config.get("ro_username"):
                _regenerate_ro_credentials_for(["starrocks"], password=new_ro_password)

            # Sync RO user if credentials exist. Username is authoritative
            # in config/db/starrocks.yaml; .ro_credentials holds only the password.
            sr_data_path = env.get("STARROCKS_DATA_PATH", "./data/database/starrocks")
            ro_creds = Path(sr_data_path) / ".ro_credentials"
            if not ro_creds.is_absolute():
                ro_creds = ROOT / ro_creds
            ro_user = sr_config.get("ro_username")
            if ro_creds.exists() and ro_user:
                ro_pass = ro_creds.read_text().strip()
                ro_sql = (
                    f"CREATE USER IF NOT EXISTS '{ro_user}' IDENTIFIED BY '{ro_pass}';"
                    f"ALTER USER '{ro_user}' IDENTIFIED BY '{ro_pass}';"
                    "CREATE ROLE IF NOT EXISTS 'sdp_readonly';"
                    "GRANT SELECT ON ALL TABLES IN ALL DATABASES TO ROLE 'sdp_readonly';"
                    f"GRANT 'sdp_readonly' TO '{ro_user}';"
                    f"SET DEFAULT ROLE 'sdp_readonly' TO '{ro_user}';"
                )
                subprocess.run(
                    ["docker", "compose", "exec", "starrocks",
                     "mysql", "-h", "127.0.0.1", "-P", sr_port,
                     "-u", "root", "--skip-password", "-e", ro_sql],
                    cwd=ROOT,
                )

            # Restore original fe.local.conf
            shutil.copy2(fe_conf_backup, fe_conf_path)
            fe_conf_backup.unlink(missing_ok=True)

            # Restart with normal auth
            print("  Restoring auth checks and restarting...")
            docker_compose("--profile", "starrocks", "down", "--timeout", "120")
            os.environ["STARROCKS_ROOT_PASSWORD"] = new_password
            docker_compose("--profile", "starrocks", "up", "-d")
            print("  StarRocks password updated successfully.")

    print("\n  Password recovery complete.")
    print("  IMPORTANT: Remember your new password — it is not stored anywhere.\n")
    return 0


# ============================================================================
# sdp db create-indexes
# ============================================================================

def _format_duration(seconds):
    """Format seconds into a human-readable duration string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    secs = seconds % 60
    return f"{minutes}m {secs:.0f}s"


def _psql_query(port, db_name, query, password=None):
    """Run a psql query via docker compose exec and return rows as lists of strings."""
    env = dict(os.environ)
    if password:
        env["PGPASSWORD"] = password
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres",
         "psql", "-U", "postgres", "-p", str(port), "-d", db_name,
         "-t", "-A", "-F", "\t", "-c", query],
        cwd=ROOT, capture_output=True, text=True, env=env,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip())
    lines = [line for line in result.stdout.strip().splitlines() if line.strip()]
    return [line.split("\t") for line in lines]


def _psql_exec(port, db_name, statement, password=None):
    """Execute a psql statement via docker compose exec. Returns (success, stderr)."""
    env = dict(os.environ)
    if password:
        env["PGPASSWORD"] = password
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres",
         "psql", "-U", "postgres", "-p", str(port), "-d", db_name,
         "-c", statement],
        cwd=ROOT, capture_output=True, text=True, env=env,
    )
    return result.returncode == 0, result.stderr.strip()


def _mongosh_eval(port, script, password=None):
    """Run a mongosh script via docker compose exec and return stdout."""
    cmd = ["docker", "compose", "exec", "-T", "mongo", "mongosh", "--quiet"]
    env_vars = load_env()
    if password:
        mongo_user = env_vars.get("MONGO_ADMIN_USER", "admin")
        cmd += ["-u", mongo_user, "-p", password, "--authenticationDatabase", "admin"]
    cmd += ["--eval", script]
    result = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip())
    return result.stdout.strip()


def _sr_query(port, database, query, password=None):
    """Run a MySQL query against StarRocks via docker compose exec and return rows."""
    cmd = ["docker", "compose", "exec", "-T", "starrocks",
           "mysql", "-h", "127.0.0.1", "-P", str(port), "-u", "root",
           "--skip-column-names", "--batch"]
    if password:
        cmd += [f"-p{password}"]
    if database:
        cmd += ["-D", database]
    cmd += ["-e", query]
    result = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip())
    lines = [line for line in result.stdout.strip().splitlines() if line.strip()]
    return [line.split("\t") for line in lines]


def _sr_exec(port, database, statement, password=None):
    """Execute a MySQL statement against StarRocks. Returns (success, stderr)."""
    cmd = ["docker", "compose", "exec", "-T", "starrocks",
           "mysql", "-h", "127.0.0.1", "-P", str(port), "-u", "root",
           "--batch"]
    if password:
        cmd += [f"-p{password}"]
    if database:
        cmd += ["-D", database]
    cmd += ["-e", statement]
    result = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    return result.returncode == 0, result.stderr.strip()


_SR_ALTER_TERMINAL = {"FINISHED", "CANCELLED"}


def _sr_alter_column_rows(port, database, password):
    """Return SHOW ALTER TABLE COLUMN rows (list of string lists); empty on error."""
    try:
        return _sr_query(port, database,
                         f"SHOW ALTER TABLE COLUMN FROM `{database}`", password)
    except RuntimeError:
        return []


def _sr_wait_for_active_alter(port, database, table, password, poll_interval=10.0,
                              line_prefix="", print_fn=None):
    """Block until no non-terminal alter-column job exists for `table`.

    StarRocks rejects CREATE INDEX while another schema change on the same
    table is running, so we drain any in-flight job first. `line_prefix` is
    prepended to progress lines; `print_fn` overrides `print` (e.g. for a
    thread-safe wrapper when multiple tables are polled concurrently).
    """
    import time
    emit = print_fn or (lambda m: print(m, flush=True))
    last_progress = None
    while True:
        rows = _sr_alter_column_rows(port, database, password)
        active = [r for r in rows
                  if len(r) >= 12 and r[1] == table and r[9] not in _SR_ALTER_TERMINAL]
        if not active:
            return
        row = max(active, key=lambda r: int(r[0]))
        progress = row[11]
        if progress != last_progress:
            emit(f"    {line_prefix}...waiting on alter job {row[0]}: state={row[9]} progress={progress}")
            last_progress = progress
        time.sleep(poll_interval)


def _sr_poll_alter_job(port, database, job_id, password, poll_interval=10.0,
                       line_prefix="", print_fn=None):
    """Poll one alter JobId until it reaches FINISHED or CANCELLED. Returns (state, msg)."""
    import time
    emit = print_fn or (lambda m: print(m, flush=True))
    target = str(job_id)
    last_progress = None
    while True:
        rows = _sr_alter_column_rows(port, database, password)
        row = next((r for r in rows if r and r[0] == target), None)
        if row is None:
            return "FINISHED", ""
        state = row[9] if len(row) > 9 else ""
        msg = row[10] if len(row) > 10 else ""
        progress = row[11] if len(row) > 11 else ""
        if state in _SR_ALTER_TERMINAL:
            return state, msg
        if progress != last_progress:
            emit(f"    {line_prefix}alter job {job_id}: state={state} progress={progress}")
            last_progress = progress
        time.sleep(poll_interval)


def _sr_read_bloom_columns(port, database, table, password):
    """Current bloom_filter_columns for a table, parsed from SHOW CREATE TABLE.

    Raises on an unreadable result: the ALTER replaces the whole list, so a
    false "nothing set" would silently drop every existing bloom column.
    """
    rows = _sr_query(port, database, f"SHOW CREATE TABLE `{database}`.`{table}`", password)
    if not rows or len(rows[0]) < 2:
        raise RuntimeError(f"could not read SHOW CREATE TABLE for {table}")
    return _sr_bloom_columns_of(rows[0][1])


def _sr_bloom_columns_of(create_table):
    """The bloom_filter_columns property of a SHOW CREATE TABLE text, as a set (empty when it has none)."""
    import re

    match = re.search(r'"bloom_filter_columns"\s*=\s*"([^"]*)"', create_table, re.IGNORECASE)
    if not match:
        return set()
    return {c.strip() for c in match.group(1).split(",") if c.strip()}


def _sr_bloom_statement(database, table, columns):
    """The ALTER that sets a table's bloom filter columns to `columns` (the whole list: it replaces the property)."""
    return (f'ALTER TABLE `{database}`.`{table}` '
            f'SET ("bloom_filter_columns" = "{",".join(columns)}")')


def _sr_bitmap_statement(database, table, field):
    """(index name, the CREATE INDEX statement) of a BITMAP index on one column."""
    index_name = f"idx_{table}_{field}"
    return index_name, f"CREATE INDEX `{index_name}` ON `{database}`.`{table}` (`{field}`) USING BITMAP"


def _sr_classify_pairs(pairs, column_types, types_for_column):
    """Split (table, field) pairs by what their column type allows: (unindexable, forced_bitmap, choosable). FLOAT/DOUBLE support neither mechanism, a type without bloom filter support is BITMAP only; the rest default to a bloom filter and may be chosen as BITMAP."""
    supported = {(t, f): types_for_column(column_types.get((t, f), ''))
                 for t, f in pairs}

    unindexable = [p for p in pairs if not any(supported[p].values())]
    forced_bitmap = [p for p in pairs
                     if p not in unindexable and not supported[p]['bloomfilter']]
    choosable = [p for p in pairs
                 if p not in unindexable and p not in forced_bitmap]
    return unindexable, forced_bitmap, choosable


def _sr_execute_index_plan(port, database, plan, password):
    """Build a StarRocks index plan {table: {'bitmap': [...], 'bloomfilter': [...]}}: parallel across tables, serial within each table, printing progress. Returns {table: {'bitmap': [...], 'bloomfilter': [...]}} of what was built."""
    import time
    import threading
    from concurrent.futures import ThreadPoolExecutor, as_completed

    print_lock = threading.Lock()

    def emit(msg):
        with print_lock:
            print(msg, flush=True)

    def _read_bloom_columns(table):
        return _sr_read_bloom_columns(port, database, table, password)

    def _build_bloom_for_table(table, fields, prefix):
        """Add bloom filter columns in one ALTER. Returns the columns added."""
        existing = _read_bloom_columns(table)
        new_columns = [f for f in fields if f not in existing]
        if not new_columns:
            emit(f"    {prefix}bloom filter already covers {', '.join(fields)}")
            return []

        target = sorted(existing | set(new_columns))
        emit(f"  {prefix}Setting bloom filter columns: {', '.join(target)}")
        t_start = time.time()

        _sr_wait_for_active_alter(port, database, table, password,
                                  line_prefix=prefix, print_fn=emit)

        statement = _sr_bloom_statement(database, table, target)
        success, stderr = _sr_exec(port, database, statement, password)
        if not success:
            emit(f"    {prefix}failed to submit bloom filter change: {stderr}")
            return []

        # Progress polling is best-effort: it is unverified which SHOW ALTER
        # view reports a bloom filter change, so an absent job means "no
        # information", never "done". The property re-read below decides.
        rows = _sr_alter_column_rows(port, database, password)
        job_ids = [int(r[0]) for r in rows if len(r) >= 12 and r[1] == table]
        if job_ids:
            _sr_poll_alter_job(port, database, max(job_ids), password,
                               line_prefix=prefix, print_fn=emit)
        else:
            _sr_wait_for_active_alter(port, database, table, password,
                                      line_prefix=prefix, print_fn=emit)

        actual = _read_bloom_columns(table)
        missing = set(target) - actual
        if missing:
            emit(f"    {prefix}bloom filter did NOT take effect for {sorted(missing)}; "
                 f"table now has {sorted(actual)}")
            return []
        emit(f"    {prefix}bloom filter set on {', '.join(new_columns)} "
             f"({_format_duration(time.time() - t_start)})")
        return new_columns

    def _build_for_table(table, spec):
        prefix = f"[{table}] "
        table_created = {"bitmap": [], "bloomfilter": []}

        if spec.get("bloomfilter"):
            try:
                table_created["bloomfilter"] = _build_bloom_for_table(
                    table, spec["bloomfilter"], prefix)
            except Exception as e:
                emit(f"    {prefix}bloom filter step failed: {e}")

        for field in spec.get("bitmap", []):
            index_name, statement = _sr_bitmap_statement(database, table, field)
            emit(f"  {prefix}Building {index_name}")
            t_start = time.time()

            _sr_wait_for_active_alter(port, database, table, password,
                                      line_prefix=prefix, print_fn=emit)

            success, stderr = _sr_exec(port, database, statement, password)
            if not success:
                emit(f"    {prefix}failed to submit: {stderr}")
                continue

            rows = _sr_alter_column_rows(port, database, password)
            job_ids = [int(r[0]) for r in rows if len(r) >= 12 and r[1] == table]
            if not job_ids:
                emit(f"    {prefix}submitted but no alter job visible; skipping wait")
                table_created["bitmap"].append(field)
                continue
            job_id = max(job_ids)

            state, msg = _sr_poll_alter_job(port, database, job_id, password,
                                            line_prefix=prefix, print_fn=emit)
            duration = time.time() - t_start
            if state == "FINISHED":
                emit(f"    {prefix}built {index_name} ({_format_duration(duration)})")
                table_created["bitmap"].append(field)
            else:
                emit(f"    {prefix}alter job {job_id} ended in state {state}: {msg}")
        return table_created

    total_indexes = sum(len(v["bitmap"]) + len(v["bloomfilter"]) for v in plan.values())
    n_bitmap = sum(len(v["bitmap"]) for v in plan.values())
    print()
    print(f"  Building {total_indexes} index(es) across {len(plan)} table(s) "
          f"({n_bitmap} bitmap, {total_indexes - n_bitmap} bloom filter) — "
          f"tables run in parallel, indexes within a table serially.")
    print()

    created = {}
    with ThreadPoolExecutor(max_workers=len(plan)) as pool:
        futures = {pool.submit(_build_for_table, t, spec): t for t, spec in plan.items()}
        for fut in as_completed(futures):
            table = futures[fut]
            try:
                result = fut.result()
            except Exception as e:
                emit(f"  [{table}] worker failed: {e}")
                continue
            if result["bitmap"] or result["bloomfilter"]:
                created[table] = result

    return created


def _interactive_sr_indexes(source, platform_config, password):
    """Interactive StarRocks index creation (bloom filter by default, BITMAP on request).

    Returns {table: {'bitmap': [...], 'bloomfilter': [...]}} for config persistence.
    """
    from social_data_pipeline.setup.utils import ask_multi_select, ask_list, section_header

    section_header("StarRocks Index Creation")

    sr_yaml = _load_db_yaml("starrocks")
    port = int(sr_yaml.get("port", 9030))
    database = source  # database-per-source

    try:
        rows = _sr_query(port, database,
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{database}' AND table_type = 'BASE TABLE' "
            "ORDER BY table_name",
            password)
        tables = [r[0] for r in rows]
    except Exception as e:
        print(f"\n  Could not connect to StarRocks: {e}")
        print("  Is it running? (sdp db start starrocks)\n")
        return {}

    if not tables:
        print(f"\n  No tables found in database '{database}'.\n")
        return {}

    print(f"  Database: {database}")
    selected_tables = ask_multi_select("Select tables to create indexes on", tables)

    # --- Collect phase: gather field lists per table interactively ---
    plan = {}  # table -> list of fields
    column_types = {}  # (table, field) -> lowercase StarRocks type
    for table in selected_tables:
        print(f"\n  --- {table} ---")

        try:
            rows = _sr_query(port, database,
                f"SELECT column_name, data_type FROM information_schema.columns "
                f"WHERE table_schema = '{database}' AND table_name = '{table}' "
                f"ORDER BY ordinal_position",
                password)
            columns = [r[0] for r in rows]
            for r in rows:
                if len(r) >= 2:
                    column_types[(table, r[0])] = r[1].strip().lower()
            if columns:
                print(f"  Columns: {', '.join(columns)}")
        except Exception:
            pass

        try:
            rows = _sr_query(port, database,
                f"SHOW INDEXES FROM `{database}`.`{table}`",
                password)
            existing = [r[2] for r in rows] if rows else []
            if existing:
                print(f"  Existing indexes: {', '.join(existing)}")
            else:
                print("  Existing indexes: (none)")
        except Exception:
            print("  Existing indexes: (could not query)")

        fields = ask_list("New indexes (comma-separated field names, empty to skip)", default=[])
        if fields:
            plan[table] = fields

    if not plan:
        return {}

    # --- Index type phase: one question for every field collected above ---
    # Everything defaults to a bloom filter; the user opts specific columns into
    # BITMAP. Column types decide what is even possible: FLOAT/DOUBLE support
    # neither mechanism (dropped), TINYINT/BOOLEAN/DECIMAL support only bitmap.
    # From core.config, not db.starrocks.ingest: that module imports
    # mysql-connector at module level, which is a container-only dependency.
    from social_data_pipeline.core.config import sr_index_types_for_column

    pairs = [(t, f) for t in sorted(plan) for f in plan[t]]
    unindexable, forced_bitmap, choosable = _sr_classify_pairs(pairs, column_types, sr_index_types_for_column)

    section_header("Index Type")
    print("  Indexes default to BLOOM FILTER: small on disk, accelerates = and IN.")
    print("  Choose BITMAP for columns you filter with ranges (>, <, >=, <=) or")
    print("  IS NULL. BITMAP is adaptive — StarRocks skips it at query time when")
    print("  it cannot filter out ~999/1000 of rows, so it is wasted on very")
    print("  low-cardinality columns. Neither type accelerates LIKE.")
    if unindexable:
        print()
        for t, f in unindexable:
            print(f"  Skipping {t}.{f}: StarRocks cannot index "
                  f"{column_types.get((t, f), '?').upper()} columns with either mechanism.")
    if forced_bitmap:
        print()
        for t, f in forced_bitmap:
            print(f"  {t}.{f} is {column_types.get((t, f), '?').upper()} — "
                  f"not bloom-filterable, using BITMAP.")
    print()

    for t, f in unindexable:
        plan[t].remove(f)
    pairs = [p for p in pairs if p not in unindexable]
    if not pairs:
        print("  Nothing left to index.\n")
        return {}

    bitmap_pairs = set(forced_bitmap)
    if choosable:
        labels = [f"{t}.{f}" for t, f in choosable]
        selected = ask_multi_select(
            "Select BITMAP indexes (press Enter for all bloom filter)",
            labels, defaults=[], tag="sdp_idx_sr_bitmap")
        chosen = set(selected)
        bitmap_pairs |= {(t, f) for t, f in choosable if f"{t}.{f}" in chosen}

    # table -> {'bitmap': [...], 'bloomfilter': [...]}
    typed_plan = {}
    for t, f in pairs:
        spec = typed_plan.setdefault(t, {"bitmap": [], "bloomfilter": []})
        spec["bitmap" if (t, f) in bitmap_pairs else "bloomfilter"].append(f)
    plan = typed_plan

    return _sr_execute_index_plan(port, database, plan, password)


def _pg_parallel_index_workers(source):
    """`processing.parallel_index_workers` of the source's postgres.yaml, 8 when it has none or cannot be read."""
    import yaml

    # Load parallel_index_workers from source postgres config if available
    source_pg_path = CONFIG_DIR / "sources" / source / "postgres.yaml"
    parallel_workers = 8
    if source_pg_path.exists():
        try:
            source_pg = yaml.safe_load(source_pg_path.read_text()) or {}
            parallel_workers = source_pg.get("processing", {}).get("parallel_index_workers", 8)
        except Exception:
            pass
    return parallel_workers


def _pg_index_statement(schema, table, field, parallel_workers):
    """(index name, the psql statement) that creates one PostgreSQL index, with the session settings for a parallel build."""
    index_name = f"idx_{table}_{field}"
    # Set session params for parallel index builds, then create index
    statement = (
        f"SET maintenance_work_mem = '2GB'; "
        f"SET max_parallel_maintenance_workers = {parallel_workers}; "
        f"CREATE INDEX IF NOT EXISTS {index_name} ON {schema}.{table} ({field});"
    )
    return index_name, statement


def _pg_create_table_indexes(port, db_name, schema, table, fields, parallel_workers, password):
    """Create one PostgreSQL index per field of a table, one after the other, printing a line for each. Returns the fields whose index was created."""
    import time

    table_created = []
    for field in fields:
        index_name, statement = _pg_index_statement(schema, table, field, parallel_workers)
        print(f"  Creating index: {index_name} ...", end=" ", flush=True)
        t_start = time.time()

        success, stderr = _psql_exec(port, db_name, statement, password)
        duration = time.time() - t_start

        if success:
            print(f"done ({_format_duration(duration)})")
            table_created.append(field)
        else:
            print(f"failed: {stderr}")
    return table_created


def _interactive_pg_indexes(source, platform_config, password):
    """Interactive PostgreSQL index creation. Returns {table: [new_fields]} for config persistence."""
    from social_data_pipeline.setup.utils import ask_multi_select, ask_list, section_header

    section_header("PostgreSQL Index Creation")

    db_yaml = _load_db_yaml("postgres")
    port = int(db_yaml.get("port", 5432))
    db_name = db_yaml.get("name", "datasets")
    schema = platform_config.get("db_schema", source)

    parallel_workers = _pg_parallel_index_workers(source)

    try:
        rows = _psql_query(port, db_name,
            f"SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE' ORDER BY table_name",
            password)
        tables = [r[0] for r in rows]
    except Exception as e:
        print(f"\n  Could not connect to PostgreSQL: {e}")
        print("  Is it running? (sdp db start)\n")
        return {}

    if not tables:
        print(f"\n  No tables found in schema '{schema}'.\n")
        return {}

    print(f"  Schema: {schema}")
    selected_tables = ask_multi_select("Select tables to create indexes on", tables)

    created = {}  # {table: [fields]}

    for table in selected_tables:
        print(f"\n  --- {table} ---")

        # Get columns
        try:
            rows = _psql_query(port, db_name,
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_schema = '{schema}' AND table_name = '{table}' "
                f"ORDER BY ordinal_position", password)
            columns = [r[0] for r in rows]
            if columns:
                print(f"  Columns: {', '.join(columns)}")
        except Exception:
            pass

        # Get existing indexes
        try:
            rows = _psql_query(port, db_name,
                f"SELECT indexname FROM pg_indexes "
                f"WHERE schemaname = '{schema}' AND tablename = '{table}' "
                f"ORDER BY indexname", password)
            existing = [r[0] for r in rows]
            if existing:
                print(f"  Existing indexes: {', '.join(existing)}")
            else:
                print("  Existing indexes: (none)")
        except Exception:
            print("  Existing indexes: (could not query)")

        fields = ask_list("New indexes (comma-separated field names, empty to skip)", default=[])
        if not fields:
            continue

        table_created = _pg_create_table_indexes(port, db_name, schema, table, fields, parallel_workers, password)

        if table_created:
            created[table] = table_created

    return created


def _mongo_index_db_name(source, platform_config, dt):
    """The MongoDB database of a data type of a source, by the same rule as the orchestrator."""
    # Determine platform string for db name resolution
    if source == "reddit":
        platform = "reddit"
    else:
        platform = f"custom/{source}"

    # Resolve db names per data_type using the same logic as the orchestrator
    if 'mongo_db_name' in platform_config:
        return platform_config['mongo_db_name']
    template = platform_config.get('mongo_db_name_template', '{platform}_{data_type}')
    safe_platform = platform.replace('/', '_')
    return template.format(platform=safe_platform, data_type=dt)


def _mongo_index_collections(db_name, dt, password):
    """The collections of a data type in its MongoDB database, sorted: from _sdp_metadata when it has entries, else every collection whose name does not start with '_'. Raises when mongosh fails."""
    import json

    # Query _sdp_metadata for collections belonging to this data_type
    script = (
        f"use('{db_name}'); "
        f"let meta = db.getCollection('_sdp_metadata'); "
        f"let count = meta.estimatedDocumentCount(); "
        f"if (count > 0) {{ "
        f"  let docs = meta.distinct('collection', {{data_type: '{dt}'}}); "
        f"  print(JSON.stringify(docs)); "
        f"}} else {{ "
        f"  let colls = db.getCollectionNames().filter(n => !n.startsWith('_')); "
        f"  print(JSON.stringify(colls)); "
        f"}}"
    )
    output = _mongosh_eval(0, script, password)  # port unused, goes through docker exec
    collections = json.loads(output) if output else []
    collections.sort()
    return collections


def _mongo_index_script(db_name, coll, field):
    """The mongosh script that creates the ascending index `<field>_1` on one collection."""
    return (
        f"use('{db_name}'); "
        f"db.getCollection('{coll}').createIndex("
        f"{{'{field}': 1}}, {{name: '{field}_1'}});"
    )


def _mongo_create_data_type_indexes(db_name, collections, fields, password):
    """Create the index `<field>_1` of each field on every collection of a data type, printing a line per field. Returns the fields, each counted as created whatever its collections answered (the terminal's rule)."""
    import time

    n_colls = len(collections)
    dt_created = []
    for field in fields:
        print(f"  Creating index {field}_1 on {n_colls} collections...")
        t_start = time.time()
        success = 0
        for coll in collections:
            try:
                _mongosh_eval(0, _mongo_index_script(db_name, coll, field), password)
                success += 1
            except Exception as e:
                print(f"    Warning: Failed on {coll}: {e}")
        duration = time.time() - t_start
        print(f"  Done: {field}_1 ({success}/{n_colls} collections, {_format_duration(duration)})")
        dt_created.append(field)
    return dt_created


def _interactive_mongo_indexes(source, platform_config, password):
    """Interactive MongoDB index creation. Returns {data_type: [new_fields]} for config persistence."""
    from social_data_pipeline.setup.utils import ask_multi_select, ask_list, section_header
    import json

    section_header("MongoDB Index Creation")

    data_types = platform_config.get("data_types", [])
    if not data_types:
        print("\n  No data types configured.\n")
        return {}

    def get_db_name(dt):
        return _mongo_index_db_name(source, platform_config, dt)

    # Discover collections per data_type via mongosh
    all_dt_collections = {}  # {data_type: {db_name, collections}}

    for dt in data_types:
        db_name = get_db_name(dt)
        try:
            collections = _mongo_index_collections(db_name, dt, password)
        except Exception as e:
            print(f"\n  Could not connect to MongoDB: {e}")
            print("  Is it running? (sdp db start)\n")
            return {}

        if collections:
            all_dt_collections[dt] = {"db_name": db_name, "collections": collections}

    if not all_dt_collections:
        print("\n  No collections found in MongoDB.\n")
        return {}

    # Display available collections
    print()
    for dt, info in all_dt_collections.items():
        print(f"  {info['db_name']}:")
        for coll in info['collections'][:5]:
            print(f"    - {coll}")
        remaining = len(info['collections']) - 5
        if remaining > 0:
            print(f"    ... and {remaining} more")

    # Ask which data types to index
    dt_options = [f"{dt} ({len(info['collections'])} collections)" for dt, info in all_dt_collections.items()]
    dt_keys = list(all_dt_collections.keys())
    selected_labels = ask_multi_select("Which data types to index?", dt_options)
    selected_dts = [dt_keys[dt_options.index(label)] for label in selected_labels]

    created = {}  # {data_type: [fields]}

    for dt in selected_dts:
        info = all_dt_collections[dt]
        db_name = info['db_name']
        collections = info['collections']
        n_colls = len(collections)

        print(f"\n  --- {dt} ({n_colls} collections) ---")

        # Show existing indexes from first collection
        if collections:
            try:
                script = (
                    f"use('{db_name}'); "
                    f"let idxs = db.getCollection('{collections[0]}').getIndexes(); "
                    f"let names = idxs.map(i => i.name).filter(n => n !== '_id_'); "
                    f"print(JSON.stringify(names));"
                )
                output = _mongosh_eval(0, script, password)
                existing = json.loads(output) if output else []
                if existing:
                    print(f"  Existing indexes (on {collections[0]}): {', '.join(existing)}")
                else:
                    print("  Existing indexes: (none)")
            except Exception:
                print("  Existing indexes: (could not query)")

        fields = ask_list("New indexes (comma-separated field names, empty to skip)", default=[])
        if not fields:
            continue

        dt_created = _mongo_create_data_type_indexes(db_name, collections, fields, password)

        if dt_created:
            created[dt] = dt_created

    return created


def _persist_indexes_to_config(source, pg_created, mongo_created, sr_created=None):
    """Merge newly created indexes into platform.yaml."""
    import yaml

    platform_path = CONFIG_DIR / "sources" / source / "platform.yaml"
    config = yaml.safe_load(platform_path.read_text()) or {}

    # Base tables are named after data types; anything else is a classifier
    # table (data type + classifier suffix) and belongs in the ML key, which is
    # what postgres_ml / sr_ml read.
    data_types = set(config.get("data_types", []) or [])

    def _merge(dest_key, table, fields):
        """Merge a flat field list (PostgreSQL / MongoDB: one index mechanism)."""
        existing = config.setdefault(dest_key, {}).setdefault(table, [])
        for f in fields:
            if f not in existing:
                existing.append(f)

    def _merge_sr(dest_key, table, spec):
        """Merge a StarRocks spec, accepting either a flat list or per-type dict.

        A stored plain list means bitmap, so adding bloom filter columns to it
        promotes the entry to the per-type shape with the old list preserved as
        bitmap. Without the promotion this raises AttributeError *after* a
        multi-hour build, losing the config write.
        """
        if isinstance(spec, list):
            spec = {"bitmap": list(spec), "bloomfilter": []}

        stored = config.setdefault(dest_key, {}).get(table)
        if stored is None:
            stored = []
        if isinstance(stored, list):
            stored = {"bitmap": list(stored), "bloomfilter": []}
        else:
            stored = {t: list(stored.get(t) or []) for t in ("bitmap", "bloomfilter")}

        for index_type in ("bitmap", "bloomfilter"):
            for f in spec.get(index_type) or []:
                if f not in stored[index_type]:
                    stored[index_type].append(f)

        # Keep the legacy flat shape when nothing needs the per-type form, so
        # existing configs are not churned into a new shape for no reason.
        if stored["bloomfilter"]:
            config[dest_key][table] = stored
        else:
            config[dest_key][table] = stored["bitmap"]

    if pg_created:
        for table, fields in pg_created.items():
            _merge("indexes" if table in data_types else "ml_indexes", table, fields)

    if mongo_created:
        # Mongo has no ML ingestion profile; keys are data types already.
        for dt, fields in mongo_created.items():
            _merge("mongo_indexes", dt, fields)

    if sr_created:
        for table, spec in sr_created.items():
            _merge_sr("sr_indexes" if table in data_types else "sr_ml_indexes", table, spec)

    platform_path.write_text(yaml.safe_dump(config, default_flow_style=False, sort_keys=False))
    print(f"  Updated: {platform_path}")


def _index_databases_of(source_profiles):
    """The databases a source has profiles for, in the order postgres, mongo, starrocks."""
    source_dbs = []
    if any(p in source_profiles for p in ("postgres_ingest", "postgres_ml")):
        source_dbs.append("postgres")
    if "mongo_ingest" in source_profiles:
        source_dbs.append("mongo")
    if any(p in source_profiles for p in ("sr_ingest", "sr_ml")):
        source_dbs.append("starrocks")
    return source_dbs


def cmd_db_create_indexes(args):
    """Interactively create database indexes for a source."""
    from social_data_pipeline.setup.utils import resolve_source, load_source_config, ask_choice, ask_bool

    source = resolve_source(args.source)
    platform_config = load_source_config(source)
    if not platform_config:
        print(f"\n  Error: Could not load platform config for '{source}'.\n")
        return 1

    configured = _get_configured_db_services()
    if not configured:
        print("\n  No databases configured. Run 'sdp db setup' first.\n")
        return 1

    # Filter to DBs that the source actually uses (has profile config for)
    from social_data_pipeline.setup.utils import get_source_profiles
    source_profiles = get_source_profiles(source)
    source_dbs = _index_databases_of(source_profiles)
    available = [db for db in configured if db in source_dbs]

    if not available:
        print(f"\n  Source '{source}' has no database profiles configured.\n")
        return 1

    # Determine which DB(s) to target
    db_labels = {"postgres": "PostgreSQL", "mongo": "MongoDB", "starrocks": "StarRocks"}
    if len(available) == 1:
        targets = available
        print(f"\n  Database: {db_labels.get(available[0], available[0])}")
    else:
        choices = [db_labels[db] for db in available] + ["All"]
        choice = ask_choice("Which database?", choices, default="All", tag="sdp_idx_database")
        if choice == "All":
            targets = available
        else:
            # Map label back to key
            label_to_key = {v: k for k, v in db_labels.items()}
            targets = [label_to_key[choice]]

    # Prompt for password if needed
    env = load_env()
    password = None
    needs_pg_auth = "postgres" in targets and env.get("POSTGRES_AUTH_ENABLED") == "true"
    needs_mongo_auth = "mongo" in targets and env.get("MONGO_AUTH_ENABLED") == "true"
    needs_sr_auth = "starrocks" in targets and env.get("STARROCKS_AUTH_ENABLED") == "true"
    if needs_pg_auth or needs_mongo_auth or needs_sr_auth:
        password = _prompt_db_password(tag="sdp_db_password")

    pg_created = {}
    mongo_created = {}
    sr_created = {}

    if "postgres" in targets:
        pg_created = _interactive_pg_indexes(source, platform_config, password)

    if "mongo" in targets:
        mongo_created = _interactive_mongo_indexes(source, platform_config, password)

    if "starrocks" in targets:
        sr_created = _interactive_sr_indexes(source, platform_config, password)

    # Summary
    def _count(created):
        """Count index fields, whether the value is a flat list or a per-type dict."""
        n = 0
        for v in created.values():
            n += sum(len(x) for x in v.values()) if isinstance(v, dict) else len(v)
        return n

    total = _count(pg_created) + _count(mongo_created) + _count(sr_created)
    if total == 0:
        print("\n  No new indexes created.\n")
        return 0

    print(f"\n  Created {total} new index(es).")

    if ask_bool("Save new indexes to platform.yaml?", default=False, tag="sdp_idx_save"):
        _persist_indexes_to_config(source, pg_created, mongo_created, sr_created)

    print()
    return 0


# ============================================================================
# sdp db index-targets, and create-indexes without a terminal
# ============================================================================

# How many tables (or MongoDB data types) and columns per table are read and returned at most.
INDEX_TARGETS_TABLES_MAX = 50
INDEX_TARGETS_COLUMNS_MAX = 200
# How many of the tables and columns left out of the targets are named (`skipped_names`).
INDEX_SKIPPED_NAMES_MAX = 20
# The longest text kept of a column type or of an existing index's name.
_INDEX_TEXT_MAX_CHARS = 128
# A table, column or schema name the web UI can pick: the registry's plain-name pattern, written out here because this module does not import the control package.
_INDEX_NAME_PATTERN = r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}"
_INDEX_DATABASE_LABELS = {"postgres": "PostgreSQL", "mongo": "MongoDB", "starrocks": "StarRocks"}
_INDEX_AUTH_FLAGS = {"postgres": "POSTGRES_AUTH_ENABLED", "mongo": "MONGO_AUTH_ENABLED", "starrocks": "STARROCKS_AUTH_ENABLED"}

_INDEX_UNKNOWN_DATABASE = "unknown database"
_INDEX_CONFIG_NOT_READABLE = "the source's platform.yaml cannot be read"
_INDEX_DATABASE_NOT_CONFIGURED = "the database is not configured: run python sdp.py db setup"
_INDEX_DATABASE_NOT_USED = "the source has no profile for this database"
_INDEX_SCHEMA_NOT_VALID = "the source's db_schema is not a plain name"
_INDEX_PORT_NOT_VALID = "the database's port in config/db is not a number"
_INDEX_REQUEST_NOT_VALID = "the index request is not a list of tables with their columns"
_INDEX_REQUEST_EMPTY = "the index request names no table"
_INDEX_TABLE_TWICE = "the index request names a table twice"
_INDEX_NO_TABLE = "a table of the index request is not in the database"
_INDEX_NO_COLUMN = "a column of the index request is not a column of its table"
_INDEX_BITMAP_NOT_HERE = "a bitmap choice is accepted only with starrocks"
_INDEX_BITMAP_NOT_A_COLUMN = "a bitmap column is not one of the columns to index"


class _IndexReadError(Exception):
    """The database could not be asked for its tables: `message` is the line the terminal prints for it."""

    def __init__(self, message):
        super().__init__(message)
        self.message = message


def _is_index_name(value):
    import re

    return isinstance(value, str) and re.fullmatch(_INDEX_NAME_PATTERN, value) is not None and value.strip(".") != ""


def _index_text(value):
    """A text read from the database, display-safe and at most _INDEX_TEXT_MAX_CHARS characters."""
    text = _display_safe(str(value))
    return text if len(text) <= _INDEX_TEXT_MAX_CHARS else text[:_INDEX_TEXT_MAX_CHARS] + "..."


def _index_context(source, db, admin_password, _ask_password):
    """(platform config, password) for an index read or build of `db` for `source`, checked as `db create-indexes` checks it: the source's config readable, the database configured and used by the source. The password is the admin password when the database has authentication on (`_resolve_admin_password`), else None. Raises ActionError."""
    from social_data_pipeline.setup.utils import get_source_profiles, read_source_config

    if not _is_one_path_component(source):
        raise ActionError(_SOURCE_NAME_NOT_VALID)
    if db not in _INDEX_DATABASE_LABELS:
        raise ActionError(_INDEX_UNKNOWN_DATABASE)
    try:
        config, warning = read_source_config(source)
    except Exception:
        raise ActionError(_INDEX_CONFIG_NOT_READABLE) from None
    if warning is not None or not isinstance(config, dict):
        raise ActionError(_INDEX_CONFIG_NOT_READABLE)
    if db not in _get_configured_db_services():
        raise ActionError(_INDEX_DATABASE_NOT_CONFIGURED)
    if db not in _index_databases_of(get_source_profiles(source)):
        raise ActionError(_INDEX_DATABASE_NOT_USED)
    password = None
    if load_env().get(_INDEX_AUTH_FLAGS[db]) == "true":
        password = _resolve_admin_password(admin_password, _ask_password)
    return config, password


def _index_port(name, default):
    try:
        return int(_load_db_yaml(name).get("port", default))
    except (TypeError, ValueError, OverflowError):
        raise ActionError(_INDEX_PORT_NOT_VALID) from None


def _index_rows_by_table(rows, width):
    """Rows of a query whose first value is a table name, grouped by it (rows shorter than `width` dropped)."""
    grouped = {}
    for row in rows:
        if len(row) >= width:
            grouped.setdefault(row[0], []).append(row)
    return grouped


def _index_skipped(skipped, names, name):
    """Count one table or column left out of the targets in `skipped`, and name it in `names` (display-safe, at most INDEX_SKIPPED_NAMES_MAX of them)."""
    skipped[0] += 1
    if names is not None and len(names) < INDEX_SKIPPED_NAMES_MAX:
        names.append(_index_text(name))


def _index_table_entry(name, columns, skipped, names=None):
    """One table of the targets: its pickable columns (plain names, at most INDEX_TARGETS_COLUMNS_MAX), counting the others in `skipped` and naming them (`<table>.<column>`) in `names`."""
    picked = []
    for column_name, column_type in columns:
        if not _is_index_name(column_name) or len(picked) >= INDEX_TARGETS_COLUMNS_MAX or any(c["name"] == column_name for c in picked):
            _index_skipped(skipped, names, f"{name}.{column_name}")
            continue
        picked.append({"name": column_name, "type": column_type})
    return {"name": name, "columns": picked, "indexes": None, "bloom_filter": None, "collections": None, "db_name": None}


def _pickable_tables(names, skipped, skipped_names=None):
    """The plain names among `names`, at most INDEX_TARGETS_TABLES_MAX; (names, truncated). The others are counted in `skipped` and named in `skipped_names`."""
    tables, truncated = [], False
    for name in names:
        if not _is_index_name(name):
            _index_skipped(skipped, skipped_names, name)
        elif len(tables) >= INDEX_TARGETS_TABLES_MAX:
            truncated = True
        else:
            tables.append(name)
    return tables, truncated


def _read_index_targets(source, platform_config, db, password):
    """What `db create-indexes` offers for `db`: the tables (for MongoDB the data types) with their columns and existing indexes. Column types are kept as read (lowercased, for StarRocks's choice of index type); a MongoDB data type keeps its database and collections in `db_name` and `collection_names`. Raises _IndexReadError when the database cannot be asked for its tables, ActionError for a config this cannot use."""
    skipped_tables, skipped_columns, skipped_names = [0], [0], []
    result = {"scope": None, "tables": [], "truncated": False, "parallel_workers": None}

    if db == "postgres":
        db_yaml = _load_db_yaml("postgres")
        port = _index_port("postgres", 5432)
        db_name = db_yaml.get("name", "datasets")
        schema = platform_config.get("db_schema", source)
        if not _is_index_name(schema):
            raise ActionError(_INDEX_SCHEMA_NOT_VALID)
        result["scope"] = schema
        result["parallel_workers"] = _pg_parallel_index_workers(source)
        try:
            rows = _psql_query(port, db_name,
                f"SELECT table_name FROM information_schema.tables "
                f"WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE' ORDER BY table_name",
                password)
        except Exception as e:
            raise _IndexReadError(f"Could not connect to PostgreSQL: {e}") from None
        tables, result["truncated"] = _pickable_tables([r[0] for r in rows], skipped_tables, skipped_names)
        try:
            columns = _index_rows_by_table(_psql_query(port, db_name,
                f"SELECT table_name, column_name, data_type FROM information_schema.columns "
                f"WHERE table_schema = '{schema}' ORDER BY table_name, ordinal_position", password), 3)
        except Exception:
            columns = {}
        try:
            indexes = _index_rows_by_table(_psql_query(port, db_name,
                f"SELECT tablename, indexname FROM pg_indexes "
                f"WHERE schemaname = '{schema}' ORDER BY tablename, indexname", password), 2)
        except Exception:
            indexes = None
        for table in tables:
            entry = _index_table_entry(table, [(r[1], r[2].strip().lower()) for r in columns.get(table, [])], skipped_columns, skipped_names)
            entry["indexes"] = None if indexes is None else [_index_text(r[1]) for r in indexes.get(table, [])]
            result["tables"].append(entry)

    elif db == "starrocks":
        port = _index_port("starrocks", 9030)
        database = source  # database-per-source
        if not _is_index_name(database):
            raise ActionError(_SOURCE_NAME_NOT_VALID)
        result["scope"] = database
        try:
            rows = _sr_query(port, database,
                "SELECT table_name FROM information_schema.tables "
                f"WHERE table_schema = '{database}' AND table_type = 'BASE TABLE' "
                "ORDER BY table_name",
                password)
        except Exception as e:
            raise _IndexReadError(f"Could not connect to StarRocks: {e}") from None
        tables, result["truncated"] = _pickable_tables([r[0] for r in rows], skipped_tables, skipped_names)
        try:
            columns = _index_rows_by_table(_sr_query(port, database,
                f"SELECT table_name, column_name, data_type FROM information_schema.columns "
                f"WHERE table_schema = '{database}' ORDER BY table_name, ordinal_position", password), 3)
        except Exception:
            columns = {}
        indexes = blooms = None
        if tables:
            # One mysql call for all tables of each kind: a call through `docker compose exec` takes a moment, and a read has a time limit.
            try:
                indexes = _index_rows_by_table(_sr_query(port, database, "; ".join(f"SHOW INDEXES FROM `{database}`.`{t}`" for t in tables), password), 3)
            except Exception:
                indexes = None
            try:
                blooms = {r[0]: _sr_bloom_columns_of(r[1].replace("\\n", "\n")) for r in _sr_query(port, database, "; ".join(f"SHOW CREATE TABLE `{database}`.`{t}`" for t in tables), password) if len(r) >= 2}
            except Exception:
                blooms = None
        for table in tables:
            entry = _index_table_entry(table, [(r[1], r[2].strip().lower()) for r in columns.get(table, [])], skipped_columns, skipped_names)
            entry["indexes"] = None if indexes is None else [_index_text(r[2]) for r in indexes.get(table, [])]
            entry["bloom_filter"] = None if blooms is None or table not in blooms else sorted(blooms[table])
            result["tables"].append(entry)

    else:
        import json

        data_types = platform_config.get("data_types", [])
        if not isinstance(data_types, list):
            data_types = []
        names, result["truncated"] = _pickable_tables([dt for dt in data_types if isinstance(dt, str)], skipped_tables, skipped_names)
        mandatory = platform_config.get("mandatory_fields", [])
        configured = platform_config.get("fields", {})
        for dt in names:
            try:
                db_name = _mongo_index_db_name(source, platform_config, dt)
            except Exception:
                db_name = None
            if not _is_index_name(db_name):
                _index_skipped(skipped_tables, skipped_names, dt)
                continue
            try:
                collections = _mongo_index_collections(db_name, dt, password)
            except Exception as e:
                raise _IndexReadError(f"Could not connect to MongoDB: {e}") from None
            collections = [c for c in collections if isinstance(c, str)]
            if not collections:
                continue
            # Documents have no fixed columns: the fields offered are the source's configured fields of the data type and the top-level keys of one document of its first collection.
            fields = [f for f in (mandatory if isinstance(mandatory, list) else []) if isinstance(f, str)]
            dt_fields = configured.get(dt, []) if isinstance(configured, dict) else []
            fields += [f for f in (dt_fields if isinstance(dt_fields, list) else []) if isinstance(f, str)]
            existing = None
            first = collections[0]
            if _is_index_name(first):
                try:
                    output = _mongosh_eval(0, (
                        f"use('{db_name}'); "
                        f"let c = db.getCollection('{first}'); "
                        f"let names = c.getIndexes().map(i => i.name).filter(n => n !== '_id_'); "
                        f"let doc = c.findOne(); "
                        f"print(JSON.stringify({{indexes: names, keys: doc ? Object.keys(doc) : []}}));"
                    ), password)
                    sample = json.loads(output) if output else {}
                    existing = [_index_text(n) for n in sample.get("indexes", []) if isinstance(n, str)]
                    fields += [k for k in sample.get("keys", []) if isinstance(k, str)]
                except Exception:
                    existing = None
            entry = _index_table_entry(dt, [(f, "") for f in fields], skipped_columns, skipped_names)
            entry["indexes"] = existing
            entry["collections"] = len(collections)
            entry["db_name"] = db_name
            entry["collection_names"] = collections
            result["tables"].append(entry)

    result["skipped_tables"] = skipped_tables[0]
    result["skipped_columns"] = skipped_columns[0]
    result["skipped_names"] = skipped_names
    return result


def get_db_index_targets(*, source, db, admin_password=None, _ask_password=None):
    """What `db create-indexes --source <source>` offers for one database, for a page that picks the columns. Prints nothing and returns `source`, `database`, `scope` (the PostgreSQL schema or the StarRocks database; None for MongoDB), `parallel_workers` (PostgreSQL's parallel index workers, else None), `truncated` (more than INDEX_TARGETS_TABLES_MAX tables), `skipped_tables` and `skipped_columns` (those whose name is not a plain name, or past the bounds), `skipped_names` (the first INDEX_SKIPPED_NAMES_MAX of them by name, a column as `<table>.<column>`), and `tables`: for each table (for MongoDB each data type) its `name`, `columns` (each with `name`, `type` as the database names it, `bloom` and `bitmap`: for StarRocks whether the type takes a bloom filter or a BITMAP index, else None), `indexes` (the existing indexes' names; None when they could not be read), `bloom_filter` (StarRocks: the bloom filter columns set now), `collections` and `db_name` (MongoDB: the number of collections of the data type and its database). A MongoDB data type's columns are its configured fields and the top-level keys of one document of its first collection. Every text read from the database is display-safe; no password or config value other than these is returned. Raises ActionError for a malformed source name, an unknown database, a source config that cannot be read, a database not configured or not used by the source, a missing admin password where authentication is on, and when the database cannot be asked (not running)."""
    from social_data_pipeline.core.config import sr_index_types_for_column

    platform_config, password = _index_context(source, db, admin_password, _ask_password)
    try:
        targets = _read_index_targets(source, platform_config, db, password)
    except _IndexReadError as e:
        raise ActionError(_index_text(e.message.splitlines()[0] if e.message.splitlines() else e.message)) from None
    tables = []
    for table in targets["tables"]:
        columns = []
        for column in table["columns"]:
            types = sr_index_types_for_column(column["type"]) if db == "starrocks" else None
            columns.append({"name": column["name"], "type": _index_text(column["type"]), "bloom": types["bloomfilter"] if types else None, "bitmap": types["bitmap"] if types else None})
        tables.append({"name": table["name"], "columns": columns, "indexes": table["indexes"], "bloom_filter": table["bloom_filter"], "collections": table["collections"], "db_name": table["db_name"]})
    return {
        "source": source, "database": db, "scope": targets["scope"], "parallel_workers": targets["parallel_workers"] if isinstance(targets["parallel_workers"], int) and not isinstance(targets["parallel_workers"], bool) else None,
        "truncated": targets["truncated"], "skipped_tables": targets["skipped_tables"], "skipped_columns": targets["skipped_columns"], "skipped_names": targets["skipped_names"], "tables": tables,
    }


def print_db_index_targets(data):
    """Print the data of `get_db_index_targets`."""
    label = _INDEX_DATABASE_LABELS.get(data["database"], data["database"])
    scope = f" ({'schema' if data['database'] == 'postgres' else 'database'} {data['scope']})" if data["scope"] else ""
    print(f"\n  {label}{scope}, source {data['source']}")
    if not data["tables"]:
        print("\n  No tables found.\n")
        return
    for table in data["tables"]:
        extra = f" ({table['collections']} collections in {table['db_name']})" if table["collections"] is not None else ""
        print(f"\n  --- {table['name']}{extra} ---")
        names = [c["name"] + (f" ({c['type']})" if c["type"] else "") for c in table["columns"]]
        print(f"  Columns: {', '.join(names) if names else '(none)'}")
        if table["indexes"] is None:
            print("  Existing indexes: (could not query)")
        else:
            print(f"  Existing indexes: {', '.join(table['indexes']) if table['indexes'] else '(none)'}")
        if table["bloom_filter"]:
            print(f"  Bloom filter columns: {', '.join(table['bloom_filter'])}")
    if data["truncated"] or data["skipped_tables"] or data["skipped_columns"]:
        print(f"\n  Not shown: {data['skipped_tables']} table(s) and {data['skipped_columns']} column(s) whose names cannot be picked"
              + (f"; only the first {INDEX_TARGETS_TABLES_MAX} tables are listed" if data["truncated"] else "") + ".")
    print()


def cmd_db_index_targets(args):
    """List what `db create-indexes` offers for one database of a source."""
    try:
        data = get_db_index_targets(source=args.source, db=args.db, _ask_password=lambda: _prompt_db_password(tag="sdp_db_password"))
    except ActionError as e:
        print(f"\n  Error: {e.message}\n")
        return e.exit_code
    print_db_index_targets(data)
    return 0


def _checked_index_request(db, indexes):
    """The index request as [(table, columns, bitmap columns)], its shape checked as the registry checks it (the action can be called without the registry). Raises ActionError."""
    if not isinstance(indexes, list):
        raise ActionError(_INDEX_REQUEST_NOT_VALID)
    if not indexes:
        raise ActionError(_INDEX_REQUEST_EMPTY)
    checked, seen = [], set()
    for item in indexes:
        if not isinstance(item, dict) or not set(item) <= {"table", "columns", "bitmap"}:
            raise ActionError(_INDEX_REQUEST_NOT_VALID)
        table, columns, bitmap = item.get("table"), item.get("columns"), item.get("bitmap", [])
        if not _is_index_name(table) or not isinstance(columns, list) or not columns or not all(_is_index_name(c) for c in columns) or len(set(columns)) != len(columns):
            raise ActionError(_INDEX_REQUEST_NOT_VALID)
        if not isinstance(bitmap, list) or not all(_is_index_name(c) for c in bitmap):
            raise ActionError(_INDEX_REQUEST_NOT_VALID)
        if bitmap and db != "starrocks":
            raise ActionError(_INDEX_BITMAP_NOT_HERE)
        if not set(bitmap) <= set(columns):
            raise ActionError(_INDEX_BITMAP_NOT_A_COLUMN)
        if table in seen:
            raise ActionError(_INDEX_TABLE_TWICE)
        seen.add(table)
        checked.append((table, list(columns), list(bitmap)))
    return checked


def do_db_create_indexes(*, source, database, indexes, save=None, admin_password=None, _ask_password=None):
    """Create indexes as `db create-indexes --source <source>` does for the same answers, without a terminal: `database` is the one database chosen, `indexes` the tables picked, each `{"table", "columns", "bitmap"}` (StarRocks only: the columns answered as BITMAP; the others get a bloom filter, as there), `save` the answer to "Save new indexes to platform.yaml?" (absent: no). The tables and columns are read from the database again first, and a table or column that is not there now is refused before anything is built. Then the same statements run through `docker compose exec`, in the same order, with the same progress lines, and the same summary. Returns 0, as the command does whether or not each index was built (its lines say). Raises ActionError for what `get_db_index_targets` refuses, a request of the wrong shape, a bitmap choice outside StarRocks, a table named twice, a table or column not in the database, and a database that cannot be asked."""
    from social_data_pipeline.core.config import sr_index_types_for_column

    platform_config, password = _index_context(source, database, admin_password, _ask_password)
    requests = _checked_index_request(database, indexes)
    label = _INDEX_DATABASE_LABELS[database]
    print(f"\n  {label}: reading the tables of source {source} again", flush=True)
    try:
        targets = _read_index_targets(source, platform_config, database, password)
    except _IndexReadError as e:
        print(f"\n  {e.message}", flush=True)
        print(f"  Is it running? (sdp db start{' starrocks' if database == 'starrocks' else ''})\n", flush=True)
        raise ActionError(f"{label} could not be asked for its tables") from None
    by_name = {table["name"]: table for table in targets["tables"]}
    for table, columns, _bitmap in requests:
        if table not in by_name:
            print(f"  {table}: not a table of this database now", flush=True)
            raise ActionError(_INDEX_NO_TABLE)
        known = {column["name"] for column in by_name[table]["columns"]}
        missing = [column for column in columns if column not in known]
        if missing:
            print(f"  {table}: {', '.join(missing)} not among its columns", flush=True)
            raise ActionError(_INDEX_NO_COLUMN)

    pg_created, mongo_created, sr_created = {}, {}, {}
    if database == "postgres":
        db_yaml = _load_db_yaml("postgres")
        port, db_name = _index_port("postgres", 5432), db_yaml.get("name", "datasets")
        print(f"  Schema: {targets['scope']}", flush=True)
        for table, columns, _bitmap in requests:
            print(f"\n  --- {table} ---", flush=True)
            created = _pg_create_table_indexes(port, db_name, targets["scope"], table, columns, targets["parallel_workers"], password)
            if created:
                pg_created[table] = created
    elif database == "mongo":
        for table, columns, _bitmap in requests:
            info = by_name[table]
            print(f"\n  --- {table} ({info['collections']} collections) ---", flush=True)
            created = _mongo_create_data_type_indexes(info["db_name"], info["collection_names"], columns, password)
            if created:
                mongo_created[table] = created
    else:
        port = _index_port("starrocks", 9030)
        print(f"  Database: {targets['scope']}", flush=True)
        plan = {table: list(columns) for table, columns, _bitmap in requests}
        bitmap_of = {table: set(bitmap) for table, _columns, bitmap in requests}
        column_types = {(table, column["name"]): column["type"] for table in plan for column in by_name[table]["columns"]}
        pairs = [(t, f) for t in sorted(plan) for f in plan[t]]
        unindexable, forced_bitmap, choosable = _sr_classify_pairs(pairs, column_types, sr_index_types_for_column)
        for t, f in unindexable:
            print(f"  Skipping {t}.{f}: StarRocks cannot index "
                  f"{column_types.get((t, f), '?').upper()} columns with either mechanism.", flush=True)
        for t, f in forced_bitmap:
            print(f"  {t}.{f} is {column_types.get((t, f), '?').upper()} — "
                  f"not bloom-filterable, using BITMAP.", flush=True)
        for t, f in unindexable:
            plan[t].remove(f)
        pairs = [p for p in pairs if p not in unindexable]
        if pairs:
            bitmap_pairs = set(forced_bitmap) | {(t, f) for t, f in choosable if f in bitmap_of[t]}
            typed_plan = {}
            for t, f in pairs:
                spec = typed_plan.setdefault(t, {"bitmap": [], "bloomfilter": []})
                spec["bitmap" if (t, f) in bitmap_pairs else "bloomfilter"].append(f)
            sr_created = _sr_execute_index_plan(port, targets["scope"], typed_plan, password)
        else:
            print("  Nothing left to index.\n", flush=True)

    total = 0
    for created in (pg_created, mongo_created, sr_created):
        for value in created.values():
            total += sum(len(x) for x in value.values()) if isinstance(value, dict) else len(value)
    if total == 0:
        print("\n  No new indexes created.\n", flush=True)
        return 0
    print(f"\n  Created {total} new index(es).", flush=True)
    if save is True:
        _persist_indexes_to_config(source, pg_created, mongo_created, sr_created)
    print(flush=True)
    return 0


# ============================================================================
# sdp source add
# ============================================================================

def cmd_source_add(args):
    """Add a new source (interactive setup)."""
    from social_data_pipeline.setup.source import main as source_main
    source_main(source_name=args.name, hf_dataset_id=getattr(args, 'hf_dataset', None))
    _warn_mount_drift_after_source_change()
    return 0


def do_source_add(
    *, name, platform, data_types, file_format, profiles, dumps_path=None, extracted_path=None, parsed_path=None, output_path=None,
    parquet_row_group_size=None, parse_workers=None, pg_prefer_lingua=None, pg_parallel_index_workers=None, table_tablespaces=None,
    db_schema=None, input_format=None, csv_delimiter=None, dump_globs=None, compressions=None, fields=None,
    mongo_collection_strategy=None, mongo_collections=None, mongo_db_name=None, pg_indexes=None, mongo_indexes=None, sr_indexes=None,
    primary_key=None, sr_prefer_lingua=None, sr_buckets=None, reddit_db_schema=None, reddit_removed_fields=None, reddit_indexes=None,
    text_columns=None, remove_strings=None, remove_patterns=None, gpu_fields=None,
    lingua_workers=None, lingua_file_workers=None, lingua_batch_size=None, lingua_low_accuracy=None,
    gpu_ids=None, ml_file_workers=None, ml_tokenize_workers=None, ml_classifier_batch_size=None, ml_classifiers=None, ml_classifier_data_types=None, hf_token=None, hf_dataset=None, hf_groups=None,
):
    """Add a new source for Reddit dumps, a Hugging Face dataset (`hf_dataset` with its grouping `hf_groups`, the schema as `get_source_hf_inspect` read it) or your own files without a terminal: each parameter is the answer to one question of `source add` (and, for a Reddit source, of `source configure reddit`), None being the question answered with Enter (a folder left out is the terminal's default, `<DATA_PATH>/<stage>/<name>` with .env's DATA_PATH). Writes the files the terminal writes for the same answers (`setup.source_add`), a relative folder with its leading `./` (`setup.utils.written_path`). Raises ActionError before anything is written for an answer the terminal would not take or not ask, a name that is already a source, and a folder that belongs to another source. Returns the exit code."""
    from social_data_pipeline.setup.source_add import Refused, add_source

    answers = {key: value for key, value in dict(
        name=name, platform=platform, data_types=data_types, dumps_path=dumps_path, extracted_path=extracted_path, parsed_path=parsed_path, output_path=output_path,
        file_format=file_format, profiles=profiles, parquet_row_group_size=parquet_row_group_size, parse_workers=parse_workers, pg_prefer_lingua=pg_prefer_lingua,
        pg_parallel_index_workers=pg_parallel_index_workers, table_tablespaces=table_tablespaces, db_schema=db_schema, input_format=input_format, csv_delimiter=csv_delimiter,
        dump_globs=dump_globs, compressions=compressions, fields=fields, mongo_collection_strategy=mongo_collection_strategy, mongo_collections=mongo_collections,
        mongo_db_name=mongo_db_name, pg_indexes=pg_indexes, mongo_indexes=mongo_indexes, sr_indexes=sr_indexes, primary_key=primary_key, sr_prefer_lingua=sr_prefer_lingua,
        sr_buckets=sr_buckets, reddit_db_schema=reddit_db_schema, reddit_removed_fields=reddit_removed_fields, reddit_indexes=reddit_indexes, text_columns=text_columns,
        remove_strings=remove_strings, remove_patterns=remove_patterns, gpu_fields=gpu_fields, lingua_workers=lingua_workers, lingua_file_workers=lingua_file_workers,
        lingua_batch_size=lingua_batch_size, lingua_low_accuracy=lingua_low_accuracy, gpu_ids=gpu_ids, ml_file_workers=ml_file_workers, ml_tokenize_workers=ml_tokenize_workers,
        ml_classifier_batch_size=ml_classifier_batch_size, ml_classifiers=ml_classifiers, ml_classifier_data_types=ml_classifier_data_types, hf_token=hf_token,
        hf_dataset=hf_dataset, hf_groups=hf_groups,
    ).items() if value is not None}
    try:
        code = add_source(answers)
    except Refused as refused:
        raise ActionError(refused.message) from None
    _warn_mount_drift_after_source_change()
    return code


# ============================================================================
# sdp source hf-inspect
# ============================================================================

def get_source_hf_inspect(*, dataset, token=None):
    """Read a Hugging Face dataset's schema from the Hub and group it as `source add --hf` groups it: `{"dataset", "groups"}`, each group with its `configs` (`name`, `rows`) and `fields` (`name`, `sql_type` or None for a type without a mapping, `hf_type`), in the terminal's order (`setup.hf_checked.inspect`). Prints nothing. The token is the one given, else HF_TOKEN of the environment, as `source add --hf` takes it; it is used for this request and kept nowhere. Raises ActionError for a dataset the Hub does not give (not found, no access, no network) or one the browser does not take (names that are not plain, more than 8 groups or 256 configs or fields in a group)."""
    import os
    from social_data_pipeline.setup.hf_checked import Refused, inspect

    try:
        return inspect(dataset, token or os.environ.get("HF_TOKEN"))
    except Refused as refused:
        raise ActionError(refused.message) from None


def cmd_source_hf_inspect(args):
    """Print a Hugging Face dataset's schema groups, as `source add --hf` will offer them."""
    try:
        data = get_source_hf_inspect(dataset=args.dataset, token=getattr(args, "token", None))
    except ActionError as error:
        print(f"\n  Error: {error.message}\n")
        return 1
    print(f"\n  Dataset: {data['dataset']}")
    for number, group in enumerate(data["groups"]):
        print(f"\n  Schema {chr(65 + number)} ({len(group['configs'])} config(s)):")
        for config in group["configs"]:
            rows = f"  ({config['rows']:,} rows)" if config["rows"] else ""
            print(f"    - {config['name']}{rows}")
        print("    Fields:")
        for field in group["fields"]:
            mapped = field["sql_type"] or "no SQL mapping: skip it or choose a type"
            print(f"      {field['name']}: {field['hf_type']} -> {mapped}")
    print()
    return 0


# ============================================================================
# sdp source download
# ============================================================================

def do_source_download(*, source, token=None):
    """Download a Hugging Face source's Parquet files without a terminal, for every data type: what `source download <name>` does (the 1-to-1 mirror into the dumps folder through `.partial` files, then the copies into the extracted folder by data type), with every folder and file name built from the Hub's reply checked before anything is written (`setup.hf_checked.download`). Prints its progress. The token is used for this download and kept nowhere. Returns the exit code."""
    from social_data_pipeline.setup.hf_checked import download

    return download(source, token)


def cmd_source_download(args):
    """Download HF dataset files for a source."""
    import os
    from social_data_pipeline.setup.utils import load_source_config
    from social_data_pipeline.setup.hf import (
        fetch_parquet_urls, download_hf_files, organize_hf_downloads, HFAPIError,
    )

    source_name = args.name
    source_config = load_source_config(source_name)
    if source_config is None:
        print(f"\n  Error: Source '{source_name}' not found in config/sources/\n")
        return 1

    hf_dataset = source_config.get('hf_dataset')
    if not hf_dataset:
        print(f"\n  Error: Source '{source_name}' has no hf_dataset configured.")
        print(f"  Use 'sdp source add {source_name} --hf <dataset_id>' to set up an HF source.\n")
        return 1

    token = getattr(args, 'token', None) or os.environ.get('HF_TOKEN')
    dumps_dir = source_config.get('paths', {}).get('dumps')
    extracted_dir = source_config.get('paths', {}).get('extracted')
    if not dumps_dir or not extracted_dir:
        print(f"\n  Error: No dumps/extracted path configured for source '{source_name}'.\n")
        return 1

    config_map = source_config.get('hf_config_map', {})
    if not config_map:
        print(f"\n  Error: Source '{source_name}' has no hf_config_map in platform.yaml.")
        print(f"  Re-run 'sdp source add {source_name} --hf {hf_dataset}' to regenerate it.\n")
        return 1

    # Filter by --data-type if specified
    if args.data_type:
        if args.data_type not in config_map:
            print(f"\n  Error: data type '{args.data_type}' not found. "
                  f"Available: {', '.join(config_map.keys())}\n")
            return 1
        config_map = {args.data_type: config_map[args.data_type]}

    print(f"\n  Source:  {source_name}")
    print(f"  Dataset: {hf_dataset}")
    print(f"  Dumps:   {dumps_dir}")
    print(f"  Extract: {extracted_dir}")
    print(f"  Data types: {', '.join(config_map.keys())}")

    try:
        # Phase 1: Download 1-to-1 mirror to dumps/
        print("\n  --- Downloading from HF Hub ---\n")
        parquet_urls = fetch_parquet_urls(hf_dataset, token=token)

        # Fail loud if the saved hf_config_map references configs that no
        # longer exist upstream (e.g. dataset owner renamed or removed a
        # config). Without this check organize_hf_downloads would silently
        # skip the missing configs and report "0 files organized".
        live_configs = set(parquet_urls.keys())
        saved_configs = {c for configs in config_map.values() for c in configs}
        missing = sorted(saved_configs - live_configs)
        if missing:
            print("\n  Error: saved hf_config_map references configs that no longer exist on HF.")
            print(f"  Missing configs: {', '.join(missing)}")
            print(f"  Available on HF: {', '.join(sorted(live_configs))}")
            print(f"  Re-run 'sdp source add {source_name} --hf {hf_dataset}' to regenerate the mapping.\n")
            return 1

        download_hf_files(parquet_urls, dumps_dir, dataset_id=hf_dataset, token=token)

        # Phase 2: Organize into extracted/<data_type>/ using config_map
        print("\n  --- Organizing into data types ---\n")
        organize_hf_downloads(dumps_dir, extracted_dir, config_map)

    except HFAPIError as e:
        print(f"\n  Error: {e}\n")
        return 1

    print(f"\n  Next step: python sdp.py run parse --source {source_name}\n")
    return 0


# ============================================================================
# sdp source configure
# ============================================================================

def cmd_source_configure(args):
    """Reconfigure an existing source (platform-specific customization)."""
    from social_data_pipeline.setup.utils import load_source_config

    source_name = args.name
    source_config = load_source_config(source_name)
    if source_config is None:
        print(f"\n  Error: Source '{source_name}' not found in config/sources/\n")
        return 1

    platform = source_config.get("platform", "")
    if platform == "reddit" or source_name == "reddit":
        from social_data_pipeline.setup.reddit import main as reddit_main
        from social_data_pipeline.setup.source import main as source_main
        from social_data_pipeline.setup.utils import (
            get_source_profiles, load_db_setup, ask_bool,
        )
        reddit_main(source_name=source_name)
        # After reddit platform config, offer to add any missing profile configs.
        # Use source add (with new-profiles-only defaults) so postgres/mongo/ml
        # can be added without touching existing working configurations.
        db_setup = load_db_setup()
        if db_setup:
            existing = get_source_profiles(source_name)
            databases = db_setup.get("databases", [])
            available = ["parse", "lingua", "ml"]
            if "postgres" in databases:
                available += ["postgres_ingest", "postgres_ml"]
            if "mongo" in databases:
                available += ["mongo_ingest"]
            if "starrocks" in databases:
                available += ["sr_ingest", "sr_ml"]
            missing = [p for p in available if p not in existing]
            if missing:
                print(f"\n  Profiles not yet configured: {', '.join(missing)}")
                if ask_bool("Add missing profile configurations now?", True, tag="sdp_add_missing_profiles"):
                    source_main(source_name=source_name)
    else:
        # For custom platforms, re-run source setup (also updates .env paths)
        from social_data_pipeline.setup.source import main as source_main
        source_main(source_name=source_name)

    return 0


def do_source_configure(
    *, name, profiles, data_types=None, dumps_path=None, extracted_path=None, parsed_path=None, output_path=None, file_format=None,
    parquet_row_group_size=None, parse_workers=None, pg_prefer_lingua=None, pg_parallel_index_workers=None, table_tablespaces=None,
    db_schema=None, input_format=None, csv_delimiter=None, dump_globs=None, compressions=None, fields=None,
    mongo_collection_strategy=None, mongo_collections=None, mongo_db_name=None, pg_indexes=None, mongo_indexes=None, sr_indexes=None,
    primary_key=None, sr_prefer_lingua=None, sr_buckets=None, reddit_db_schema=None, reddit_removed_fields=None, reddit_indexes=None,
    text_columns=None, remove_strings=None, remove_patterns=None, gpu_fields=None,
    lingua_workers=None, lingua_file_workers=None, lingua_batch_size=None, lingua_low_accuracy=None,
    gpu_ids=None, ml_file_workers=None, ml_tokenize_workers=None, ml_classifier_batch_size=None, ml_classifiers=None, ml_classifier_data_types=None, hf_token=None,
):
    """Reconfigure a source without a terminal: each parameter is the answer to one question of `source configure <name>`, None being the question answered with Enter, whose default is the source's current value where the terminal offers it (`get_source_settings` reads them). Writes the files the terminal writes for the same answers (`setup.source_change.configure_source`; for the source reddit the Reddit answers are applied after the files, see there). Raises ActionError before anything is written for an answer the terminal would not take or not ask and a folder that belongs to another source. Returns the exit code."""
    from social_data_pipeline.setup.source_add import Refused
    from social_data_pipeline.setup.source_change import configure_source

    answers = {key: value for key, value in dict(
        name=name, profiles=profiles, data_types=data_types, dumps_path=dumps_path, extracted_path=extracted_path, parsed_path=parsed_path, output_path=output_path,
        file_format=file_format, parquet_row_group_size=parquet_row_group_size, parse_workers=parse_workers, pg_prefer_lingua=pg_prefer_lingua,
        pg_parallel_index_workers=pg_parallel_index_workers, table_tablespaces=table_tablespaces, db_schema=db_schema, input_format=input_format, csv_delimiter=csv_delimiter,
        dump_globs=dump_globs, compressions=compressions, fields=fields, mongo_collection_strategy=mongo_collection_strategy, mongo_collections=mongo_collections,
        mongo_db_name=mongo_db_name, pg_indexes=pg_indexes, mongo_indexes=mongo_indexes, sr_indexes=sr_indexes, primary_key=primary_key, sr_prefer_lingua=sr_prefer_lingua,
        sr_buckets=sr_buckets, reddit_db_schema=reddit_db_schema, reddit_removed_fields=reddit_removed_fields, reddit_indexes=reddit_indexes, text_columns=text_columns,
        remove_strings=remove_strings, remove_patterns=remove_patterns, gpu_fields=gpu_fields, lingua_workers=lingua_workers, lingua_file_workers=lingua_file_workers,
        lingua_batch_size=lingua_batch_size, lingua_low_accuracy=lingua_low_accuracy, gpu_ids=gpu_ids, ml_file_workers=ml_file_workers, ml_tokenize_workers=ml_tokenize_workers,
        ml_classifier_batch_size=ml_classifier_batch_size, ml_classifiers=ml_classifiers, ml_classifier_data_types=ml_classifier_data_types, hf_token=hf_token,
    ).items() if value is not None}
    try:
        code = configure_source(answers)
    except Refused as refused:
        raise ActionError(refused.message) from None
    _warn_mount_drift_after_source_change()
    return code


# ============================================================================
# sdp source settings
# ============================================================================

def cmd_source_settings(args):
    """Show the current answers of `source configure <name>` (never a secret)."""
    if not args.name or args.name in (".", "..") or "/" in args.name or "\\" in args.name or not (CONFIG_DIR / "sources" / args.name / "platform.yaml").is_file():
        print(f"\n  Error: Source '{_display_safe(args.name)}' not found in config/sources/\n")
        return 1
    data = get_source_settings(name=args.name)
    if getattr(args, "json", False):
        print(json.dumps(data, indent=2))
        return 1 if data["not_readable"] else 0
    print()
    if data["not_readable"]:
        print(f"  Error: {_display_safe(data['not_readable'])}\n")
        return 1
    print(f"  The answers 'source configure {_display_safe(args.name)}' takes as its defaults (Enter keeps each):")
    print()
    print(f"    {'profiles':26s} {_display_safe(', '.join(data['profiles']) or 'none')}")
    for key, value in data["answers"].items():
        print(f"    {key:26s} {_display_safe(json.dumps(value))}")
    for dt, spec in data["typed_sr_indexes"].items():
        print(f"    {'sr_indexes (typed)':26s} {_display_safe(dt)}: {_display_safe(json.dumps(spec))}")
    print(f"    {'HF_TOKEN in .env':26s} {'set' if data['hf_token_set'] else 'not set'}")
    for line in data["left_out"]:
        print(f"    Note: {_display_safe(line)}")
    print()
    return 0


def get_source_settings(*, name):
    """The current answers of `source configure <name>` for the source's current profiles, which a reconfigure with them writes again unchanged (`setup.source_change.settings_read`): `name`, `platform`, `hf_dataset`, `data_types`, `profiles`, `answers`, `typed_sr_indexes`, `hf_token_set`, `left_out`, `not_readable`. Never a secret: whether .env has a Hugging Face token is a flag. Prints nothing."""
    from social_data_pipeline.setup.source_change import settings_read

    return settings_read(name)


# ============================================================================
# sdp source add-classifiers
# ============================================================================

def cmd_source_add_classifiers(args):
    """Add ML classifiers for a source."""
    from social_data_pipeline.setup.classifiers import main as classifiers_main
    classifiers_main(source_name=args.name)
    return 0


def do_source_add_classifiers(
    *, name, text_columns=None, remove_strings=None, remove_patterns=None, gpu_fields=None,
    lingua_workers=None, lingua_file_workers=None, lingua_batch_size=None, lingua_low_accuracy=None,
    gpu_ids=None, ml_file_workers=None, ml_tokenize_workers=None, ml_classifier_batch_size=None, ml_classifiers=None, ml_classifier_data_types=None, hf_token=None,
):
    """Configure a source's classifiers without a terminal: each parameter is the answer to one question of `source add-classifiers <name>`, None being the question answered with Enter (the source's current value where the terminal offers it). Writes lingua.yaml and ml.yaml for the source's classifier profiles, and HF_TOKEN in .env when a token is given, as the terminal does (`setup.source_change.add_classifiers`). Raises ActionError before anything is written for an answer the terminal would not take or not ask, and for a source with neither lingua nor ml. Returns the exit code."""
    from social_data_pipeline.setup.source_add import Refused
    from social_data_pipeline.setup.source_change import add_classifiers

    answers = {key: value for key, value in dict(
        name=name, text_columns=text_columns, remove_strings=remove_strings, remove_patterns=remove_patterns, gpu_fields=gpu_fields,
        lingua_workers=lingua_workers, lingua_file_workers=lingua_file_workers, lingua_batch_size=lingua_batch_size, lingua_low_accuracy=lingua_low_accuracy,
        gpu_ids=gpu_ids, ml_file_workers=ml_file_workers, ml_tokenize_workers=ml_tokenize_workers, ml_classifier_batch_size=ml_classifier_batch_size,
        ml_classifiers=ml_classifiers, ml_classifier_data_types=ml_classifier_data_types, hf_token=hf_token,
    ).items() if value is not None}
    try:
        return add_classifiers(answers)
    except Refused as refused:
        raise ActionError(refused.message) from None


# ============================================================================
# sdp source remove
# ============================================================================

def cmd_source_remove(args):
    """Remove source configuration."""
    try:
        return do_source_remove(name=args.name, _confirm=_confirm_on_terminal)
    except ActionError as e:
        print(f"\n  Error: {e.message}\n")
        return e.exit_code


def do_source_remove(*, name, _confirm=None):
    """Remove source configuration. Asks nothing itself: `_confirm` is called with the confirmation question and returns True to proceed; without it the removal stops with ActionError before anything is changed. Returns the exit code."""
    source_dir = CONFIG_DIR / "sources" / name
    if not source_dir.exists():
        print(f"\n  Error: Source '{name}' not found in config/sources/\n")
        return 1

    print(f"\n  Source: {name}")
    print(f"  Directory: config/sources/{name}/")

    # List files
    files = sorted(source_dir.glob("*"))
    if files:
        print("  Files:")
        for f in files:
            print(f"    {f.name}")
    print()

    if _confirm is None:
        raise ActionError("confirmation required")
    confirm = _confirm(f"  Remove source '{name}' configuration? [y/N]: ")
    if confirm is not True:
        print("  Aborted.\n")
        return 0

    shutil.rmtree(source_dir)
    print(f"\n  Removed config/sources/{name}/")
    print("  Note: Data files (dumps, csv, output) were NOT removed.\n")
    _warn_mount_drift_after_source_change()
    return 0


# ============================================================================
# sdp source list
# ============================================================================

def cmd_source_list(args):
    """List configured sources."""
    print_source_list(get_source_list())
    return 0


def get_source_list():
    """Gather the configured sources. Prints nothing and returns `{"sources": [{"name", "profiles"}, ...]}` for `print_source_list`."""
    from social_data_pipeline.setup.utils import list_sources, get_source_profiles

    sources = list_sources()
    data = {"sources": []}
    for source in sources:
        profiles = get_source_profiles(source)
        data["sources"].append({"name": source, "profiles": list(profiles)})
    return data


def print_source_list(data):
    """Print the text of `source list` from the data of `get_source_list`."""
    if not data["sources"]:
        print("\n  No sources configured. Run: python sdp.py source add <name>\n")
        return

    print("\n  Configured sources:\n")
    for entry in data["sources"]:
        profiles = entry["profiles"]
        profiles_str = ", ".join(profiles) if profiles else "none"
        print(f"    {entry['name']}")
        print(f"      profiles: {profiles_str}")
    print()


# ============================================================================
# sdp source status
# ============================================================================

# Holds no input value: the name is whatever the caller sent.
_SOURCE_NAME_NOT_VALID = "the source name is not a valid name"


def _is_one_path_component(name):
    """Return True when name is exactly one path component, so that a folder joined from it stays inside config/sources/."""
    # The control package has the same rule for requests. It is written out here because this module does not import that package.
    if not isinstance(name, str) or not name:
        return False
    if "/" in name or "\\" in name or name in (".", ".."):
        return False
    # A longer name cannot be a file name on common filesystems, and stat() on it raises OSError instead of answering "no such folder".
    if len(name.encode("utf-8", "surrogatepass")) > 255:
        return False
    return not any(unicodedata.category(ch) in ("Cc", "Zl", "Zp") for ch in name)


def cmd_source_status(args):
    """Show source processing/ingestion status."""
    source_name = args.name

    print_source_status(_get_source_status(source_name))
    return 0


def get_source_status(*, name=None):
    """Gather what `source status` shows, for the named source or for all. Prints nothing and returns `{"sources": [...]}` for `print_source_status`: one entry per source with `name`, `found` and `warnings` (the lines about a platform.yaml that cannot be read); a source that was found also has `platform`, `data_types`, `profiles`, `paths` and `ingestion` (one entry per database view the terminal shows), and one whose platform.yaml names a Hugging Face dataset `hf_dataset` and `hf_downloaded` (whether its dumps folder holds a downloaded Parquet file; the terminal shows neither). Raises ActionError for a name that is not one path component, the empty string included (only None means all sources); a well-formed name that does not exist is an entry with `found` false."""
    if name is not None and not _is_one_path_component(name):
        raise ActionError(_SOURCE_NAME_NOT_VALID)
    return _get_source_status(name)


def _get_source_status(source_name):
    """The body of `get_source_status` without the check of the name. The terminal command calls it with the name as it was typed, which is joined to config/sources/ unchecked, as it always was."""
    from social_data_pipeline.setup.utils import list_sources, read_source_config, get_source_profiles

    if source_name:
        sources = [source_name]
    else:
        sources = list_sources()

    data = {"sources": []}

    if not sources:
        return data

    env = load_env()

    for source in sources:
        config, warning = read_source_config(source)
        entry = {"name": source, "found": config is not None, "warnings": [warning] if warning is not None else []}
        data["sources"].append(entry)
        if config is None:
            continue

        platform = config.get("platform", source)
        data_types = config.get("data_types", [])
        profiles = get_source_profiles(source)

        entry["platform"] = _json_fact(platform)
        entry["data_types"] = [_json_fact(data_type) for data_type in data_types]
        entry["profiles"] = list(profiles)
        paths = config.get("paths", {})
        entry["paths"] = {
            "dumps": _json_fact(paths.get('dumps', f'./data/dumps/{source}')),
            "extracted": _json_fact(paths.get('extracted', f'./data/extracted/{source}')),
            "parsed": _json_fact(paths.get('parsed', f'./data/parsed/{source}')),
            "output": _json_fact(paths.get('output', f'./data/output/{source}')),
        }
        if config.get("hf_dataset"):
            entry["hf_dataset"] = _json_fact(config["hf_dataset"])
            entry["hf_downloaded"] = _hf_downloaded(entry["paths"]["dumps"])

        # Show ingestion state for this source
        entry["ingestion"] = []
        has_postgres = any(p.startswith("postgres") for p in profiles)
        if has_postgres:
            pgdata_path = env.get("PGDATA_PATH", "./data/database/postgres")
            pgdata = Path(pgdata_path)
            if not pgdata.is_absolute():
                pgdata = ROOT / pgdata
            state_dir = pgdata / "state_tracking"
            entry["ingestion"].append(_get_source_ingestion_state(state_dir, source, "postgres"))

        has_mongo = "mongo_ingest" in profiles
        if has_mongo:
            mongo_data_path = env.get("MONGO_DATA_PATH", "./data/database/mongo")
            mongo_data = Path(mongo_data_path)
            if not mongo_data.is_absolute():
                mongo_data = ROOT / mongo_data
            state_dir = mongo_data / "state_tracking"
            entry["ingestion"].append(_get_source_ingestion_state(state_dir, source, "mongo"))

        has_starrocks = any(p.startswith("sr_") for p in profiles)
        if has_starrocks:
            sr_data_path = env.get("STARROCKS_DATA_PATH", "./data/database/starrocks")
            sr_data = Path(sr_data_path)
            if not sr_data.is_absolute():
                sr_data = ROOT / sr_data
            state_dir = sr_data / "state_tracking"
            entry["ingestion"].append(_get_source_ingestion_state(state_dir, source, "sr_ingest"))
            entry["ingestion"].append(_get_source_ingestion_state(state_dir, source, "sr_ml"))

    return data


def print_source_status(data):
    """Print the text of `source status` from the data of `get_source_status`."""
    if not data["sources"]:
        print("\n  No sources configured. Run: python sdp.py source add <name>\n")
        return

    print()
    print("  Social Data Pipeline - Source Status")
    print("  ===================================")

    for entry in data["sources"]:
        source = entry["name"]
        for warning in entry["warnings"]:
            print(warning)
        if not entry["found"]:
            print(f"\n  Source '{source}': not found")
            continue

        profiles = entry["profiles"]

        print(f"\n  Source: {source}")
        print(f"    Platform:    {entry['platform']}")
        print(f"    Data types:  {', '.join(entry['data_types'])}")
        print(f"    Profiles:    {', '.join(profiles) if profiles else 'none'}")
        paths = entry["paths"]
        print("    Paths:")
        print(f"      Dumps:     {paths['dumps']}")
        print(f"      Extracted: {paths['extracted']}")
        print(f"      Parsed:    {paths['parsed']}")
        print(f"      Output:    {paths['output']}")

        # Show ingestion state for this source
        for ingestion in entry["ingestion"]:
            _print_source_ingestion(source, ingestion)

    print()


def _print_source_ingestion_state(state_dir, source, db_type):
    """Print ingestion state for a specific source from state files."""
    _print_source_ingestion(source, _get_source_ingestion_state(state_dir, source, db_type))


def _get_source_ingestion_state(state_dir, source, db_type):
    """Read ingestion state for a specific source from state files. Returns `db_type`, `has_state_files` (whether a state file of this source exists, readable or not) and `entries`, one per readable file, each with the `label` the terminal shows and, apart from it, the `profile` and `data_type` its file name says."""
    ingestion = {"db_type": db_type, "has_state_files": False, "entries": []}

    prefix = f"{source}_{db_type}_"
    if state_dir.exists():
        state_files = sorted(f for f in state_dir.glob("*.json") if f.stem.startswith(prefix))
    else:
        state_files = []

    if not state_files:
        return ingestion

    ingestion["has_state_files"] = True
    for sf in state_files:
        try:
            sdata = _parse_state_json(sf.read_text())
        except (json.JSONDecodeError, OSError):
            continue

        name = sf.stem
        processed = sdata.get("processed", [])
        failed = sdata.get("failed", [])
        in_progress = sdata.get("in_progress")
        last_updated = sdata.get("last_updated", "")

        # Extract data type from state filename
        parts = name.split(f"_{db_type}_")
        label = parts[1] if len(parts) == 2 else name

        if len(parts) != 2:
            named = _state_file_facts(None, None)
        elif db_type in INGESTION_PROFILES:
            # "sr_ingest" and "sr_ml" are profiles themselves: the label is the data type.
            named = _state_file_facts(db_type, label)
        else:
            # "postgres" and "mongo" are databases: the label still starts with "ingest_" or "ml_".
            token, _, data_type = label.partition("_")
            named = _state_file_facts(f"{db_type}_{token}", data_type)

        count = len(processed)
        latest = processed[-1] if processed else None

        ingestion["entries"].append({
            "label": label,
            **named,
            "count": count,
            "latest": latest,
            "in_progress": in_progress,
            "failed_count": len(failed) if failed else 0,
            "last_updated": last_updated,
        })

    return ingestion


def _print_source_ingestion(source, ingestion):
    """Print the ingestion state of one source in one database view, from the data of `_get_source_ingestion_state`."""
    db_type = ingestion["db_type"]
    display = _DB_DISPLAY.get(db_type, db_type.title())

    if not ingestion["has_state_files"]:
        print(f"\n    {display} ingestion: no data yet for source '{source}'.")
        return

    print(f"\n    {display} ingestion:")
    for entry in ingestion["entries"]:
        line = f"      {entry['label']}: {entry['count']} datasets"
        # A dataset whose name is "-" is not shown as the latest: "-" used to stand for "none".
        if entry["count"] and entry["latest"] != "-":
            line += f" (latest: {entry['latest']})"
        if entry["in_progress"]:
            line += f" [in progress: {entry['in_progress']}]"
        if entry["failed_count"]:
            line += f" [{entry['failed_count']} failed]"
        print(line)

        if entry["last_updated"]:
            print(f"        last updated: {entry['last_updated']}")


# ============================================================================
# sdp source progress
# ============================================================================

# Why a part of the progress report could not be read. None of them holds an input value.
_PROGRESS_CONFIG_NOT_READABLE = "the source config cannot be read"
_PROGRESS_CONFIG_NOT_A_MAPPING = "the source config is not a mapping"
_PROGRESS_PATHS_NOT_A_MAPPING = "paths in the source config is not a mapping"
_PROGRESS_DATA_TYPES_NOT_A_LIST = "data_types in the source config is not a list"
_PROGRESS_PATH_NOT_VALID = "the path in the source config is empty or not text"
_PROGRESS_DATA_TYPE_NOT_VALID = "the data type is not a valid folder name"
_PROGRESS_FOLDER_NOT_READABLE = "the folder cannot be read"

_PROGRESS_FINISHED_SUFFIXES = (".csv", ".parquet")
_PROGRESS_WRITING_SUFFIX = ".temp"
# Sub-folders of a source's output folder that are not a classifier: lingua is reported as its own stage, lingua_ingest holds a second copy of the lingua files for ingestion.
_PROGRESS_NOT_A_CLASSIFIER = ("lingua", "lingua_ingest")
# What a data type of the config that is not text is called in the progress data, and how many characters of a text that is no folder name are carried.
_PROGRESS_DATA_TYPE_NOT_TEXT = "(not text)"
_PROGRESS_DATA_TYPE_SHOWN = 80
_DISPLAY_UNSAFE_CATEGORIES = ("Cc", "Cf", "Zl", "Zp", "Cs")


def _display_safe(text):
    """Return text with every control character (categories Cc, Zl, Zp), every format character (category Cf: a direction override reorders the line a terminal shows) and every lone surrogate (how an undecodable byte of a file name arrives) replaced by its backslash escape, so that the result can be printed on a terminal and encoded as UTF-8."""
    if not any(unicodedata.category(ch) in _DISPLAY_UNSAFE_CATEGORIES for ch in text):
        return text
    return "".join(ch.encode("unicode_escape").decode("ascii") if unicodedata.category(ch) in _DISPLAY_UNSAFE_CATEGORIES else ch for ch in text)


def cmd_source_progress(args):
    """Show how many files each processing stage has finished."""
    try:
        data = get_source_progress(name=args.name)
    except ActionError as e:
        if getattr(args, "json", False):
            # Standard output stays empty, so a reader of the JSON is not handed a line that is not JSON.
            print(f"  Error: {e.message}", file=sys.stderr)
        else:
            print(f"\n  Error: {e.message}\n")
        return e.exit_code

    if getattr(args, "json", False):
        print(json.dumps(data, indent=2))
    else:
        print_source_progress(data)
    if args.name is not None and not data["sources"][0]["found"]:
        return 1
    return 0


def get_source_progress(*, name=None):
    """Count the files each file-based stage has written, for the named source or for all. Prints nothing and returns `{"sources": [...]}` for `print_source_progress`: one entry per source with `name`, `found`, `warnings` (the line about a platform.yaml that cannot be read), `not_readable` (None, or why the config cannot be used), `stages` and `output` (the `path` of the output folder, whether it `exists`, and `not_readable`; None without a usable config); `stages` holds parse, then lingua, then one entry per classifier folder in name order, each with `stage`, `kind` (`parse`, `lingua` or `classifier`), `path`, `not_readable` and `data_types`, one item per data type of the config with `data_type` (`(not text)` for a value that is not text, the first 80 characters and `...` for a text longer than that which is no folder name), `exists` (the folder was listed), `not_readable`, `finished` (files ending in .csv or .parquet), `being_written` (files ending in .temp) and `newest` (`name` and `modified`, an ISO-8601 UTC time, of the finished file changed last; None without one). Every `not_readable` is None or a constant reason, and every name and path is display-safe. Raises ActionError for a name that is not one path component, the empty string included (only None means all sources); a well-formed name without a readable config is an entry with `found` false."""
    if name is not None and not _is_one_path_component(name):
        raise ActionError(_SOURCE_NAME_NOT_VALID)

    from social_data_pipeline.setup.utils import list_sources

    sources = [name] if name is not None else list_sources()
    return {"sources": [_get_source_progress_entry(source) for source in sources]}


def _get_source_progress_entry(source):
    """The entry of one source in the data of `get_source_progress`."""
    from social_data_pipeline.setup.utils import read_source_config

    entry = {"name": _display_safe(source), "found": False, "warnings": [], "not_readable": None, "stages": [], "output": None}
    try:
        config, warning = read_source_config(source)
    except Exception:
        # The reader lets more through than can be listed: a source folder that cannot be entered raises OSError where the reader looks for the file, a platform.yaml that is not UTF-8 text raises UnicodeDecodeError, one nested hundreds of levels deep raises RecursionError in the YAML parser, and a tagged value the parser cannot build (`!!bool maybe`, `!!int ""`, `!!timestamp x`) raises KeyError, IndexError or AttributeError. Whatever it is, this source's config cannot be read and the next source is still reported.
        entry["not_readable"] = _PROGRESS_CONFIG_NOT_READABLE
        return entry
    if warning is not None:
        entry["warnings"].append(_display_safe(warning))
        entry["not_readable"] = _PROGRESS_CONFIG_NOT_READABLE
    if config is None:
        return entry

    entry["found"] = True
    if not isinstance(config, dict):
        entry["not_readable"] = _PROGRESS_CONFIG_NOT_A_MAPPING
        return entry
    paths = config.get("paths", {})
    if not isinstance(paths, dict):
        entry["not_readable"] = _PROGRESS_PATHS_NOT_A_MAPPING
        return entry
    # A string is not taken as a list of one-letter data types, and a missing key is not taken as "no data types".
    data_types = config.get("data_types")
    if not isinstance(data_types, list):
        entry["not_readable"] = _PROGRESS_DATA_TYPES_NOT_A_LIST
        return entry

    parsed = _resolve_progress_folder(paths.get("parsed", f"./data/parsed/{source}"))
    output = _resolve_progress_folder(paths.get("output", f"./data/output/{source}"))
    entry["stages"].append(_get_stage_progress("parse", "parse", parsed, data_types))
    entry["stages"].append(_get_stage_progress("lingua", "lingua", None if output is None else os.path.join(output, "lingua"), data_types))
    if output is None:
        entry["output"] = {"path": None, "exists": False, "not_readable": _PROGRESS_PATH_NOT_VALID}
        return entry

    classifiers, exists, not_readable = _list_classifier_folders(output)
    entry["output"] = {"path": _display_safe(output), "exists": exists, "not_readable": not_readable}
    for classifier in classifiers:
        entry["stages"].append(_get_stage_progress(classifier, "classifier", os.path.join(output, classifier), data_types))
    return entry


def _resolve_progress_folder(value):
    """Return a stage path of a source config as an absolute path (a relative one is under the project root, not the working directory), or None when the value is not a non-empty string."""
    if not isinstance(value, str) or not value:
        return None
    path = Path(value)
    if not path.is_absolute():
        path = ROOT / path
    return str(path)


def _list_classifier_folders(output):
    """List the classifier folders of a source's output folder with one directory listing: every direct sub-folder that is not lingua or lingua_ingest, in name order. Returns (names, whether the output folder exists, None or why it cannot be listed)."""
    try:
        with os.scandir(output) as listing:
            entries = list(listing)
    except FileNotFoundError:
        return [], False, None
    except (OSError, ValueError):
        # ValueError: a path that cannot be handed to the file system at all (a NUL character, a character its encoding does not have).
        return [], False, _PROGRESS_FOLDER_NOT_READABLE

    names = []
    for entry in entries:
        if entry.name in _PROGRESS_NOT_A_CLASSIFIER:
            continue
        try:
            if entry.is_dir():
                names.append(entry.name)
        except OSError:
            continue
    return sorted(names), True, None


def _get_stage_progress(stage, kind, folder, data_types):
    """One stage entry in the data of `get_source_progress`. `folder` is the stage folder as an absolute path, or None when the config holds no usable path for it; each data type is counted in its own sub-folder."""
    entry = {"stage": _display_safe(stage), "kind": kind, "path": None, "not_readable": None, "data_types": []}
    if folder is None:
        entry["not_readable"] = _PROGRESS_PATH_NOT_VALID
        return entry

    entry["path"] = _display_safe(folder)
    for data_type in data_types:
        if _is_one_path_component(data_type):
            counts = _count_stage_files(os.path.join(folder, data_type))
        else:
            # Never joined to a path: it could name a folder outside the stage.
            counts = {"exists": False, "not_readable": _PROGRESS_DATA_TYPE_NOT_VALID, "finished": 0, "being_written": 0, "newest": None}
        entry["data_types"].append({"data_type": _data_type_label(data_type), **counts})
    return entry


def _data_type_label(data_type):
    """The text the progress data carries for one data type of a source config. A value that is not text is never converted to text (a number can be too long to convert, a list or a mapping would be written out whole): it gets a constant label. A text that is no folder name is cut after its first characters; a folder name is at most 255 bytes and is carried whole."""
    if not isinstance(data_type, str):
        return _PROGRESS_DATA_TYPE_NOT_TEXT
    if len(data_type) > _PROGRESS_DATA_TYPE_SHOWN and not _is_one_path_component(data_type):
        return _display_safe(data_type[:_PROGRESS_DATA_TYPE_SHOWN]) + "..."
    return _display_safe(data_type)


def _count_stage_files(folder):
    """Count the files directly in one data type folder of a stage, with one directory listing and no recursion. Returns `exists`, `not_readable`, `finished`, `being_written` and `newest` as `get_source_progress` describes them; a folder that cannot be listed has `exists` false and its counts at zero."""
    counts = {"exists": False, "not_readable": None, "finished": 0, "being_written": 0, "newest": None}
    try:
        with os.scandir(folder) as listing:
            entries = list(listing)
    except FileNotFoundError:
        return counts
    except (OSError, ValueError):
        # ValueError: a path that cannot be handed to the file system at all (a NUL character, a character its encoding does not have).
        counts["not_readable"] = _PROGRESS_FOLDER_NOT_READABLE
        return counts

    finished, being_written, modified = 0, 0, []
    for entry in entries:
        is_finished = entry.name.endswith(_PROGRESS_FINISHED_SUFFIXES)
        if not is_finished and not entry.name.endswith(_PROGRESS_WRITING_SUFFIX):
            continue
        try:
            # Follows links, as the stages do when they look for their files; a folder with such a name is not a file.
            if not entry.is_file():
                continue
            if is_finished:
                modified.append((os.stat(entry.path).st_mtime_ns, entry.name))
        except FileNotFoundError:
            # A stage renames and deletes files while this runs: a file gone since the listing is not counted.
            continue
        except OSError as error:
            # A link that leads back to itself, or through a file as if it were a folder, is not a file, as a link that leads nowhere is not.
            if error.errno in (errno.ELOOP, errno.ENOTDIR):
                continue
            counts["not_readable"] = _PROGRESS_FOLDER_NOT_READABLE
            return counts
        if is_finished:
            finished += 1
        else:
            being_written += 1

    counts.update(exists=True, finished=finished, being_written=being_written)
    # The newest file is the one changed last, the greater name among files changed at the same time. A file whose time no date can hold is counted and passed over here.
    for mtime_ns, name in sorted(modified, reverse=True):
        when = _iso_utc_seconds(mtime_ns)
        if when is not None:
            counts["newest"] = {"name": _display_safe(name), "modified": when}
            break
    return counts


def _iso_utc_seconds(mtime_ns):
    """Return a modification time in nanoseconds as an ISO-8601 UTC string with seconds precision that ends in Z, or None when no date can hold it."""
    try:
        return datetime.fromtimestamp(mtime_ns // 1_000_000_000, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    except (OverflowError, OSError, ValueError):
        return None


def _local_minute(iso_utc):
    """Return an ISO-8601 UTC time of the progress data as `YYYY-MM-DD HH:MM` in the local time zone, or None when it cannot be converted."""
    try:
        return datetime.fromisoformat(iso_utc[:-1] + "+00:00").astimezone().strftime("%Y-%m-%d %H:%M")
    except (OverflowError, OSError, ValueError):
        return None


def _progress_text(item):
    """The text `source progress` shows for one data type of one stage."""
    if item["not_readable"]:
        return f"not readable ({item['not_readable']})"
    if not item["exists"]:
        return "no folder yet"
    text = f"{item['finished']} finished"
    if item["being_written"] > 0:
        text += f", {item['being_written']} being written"
    newest = item["newest"]
    if newest:
        when = _local_minute(newest["modified"])
        text += f", newest: {newest['name']} ({when} local time)" if when else f", newest: {newest['name']} ({newest['modified']})"
    return text


def print_source_progress(data):
    """Print the text of `source progress` from the data of `get_source_progress`."""
    if not data["sources"]:
        print("\n  No sources configured. Run: python sdp.py source add <name>\n")
        return

    print()
    print("  Social Data Pipeline - Source Progress")
    print("  ======================================")

    being_written = False
    for entry in data["sources"]:
        source = entry["name"]
        for warning in entry["warnings"]:
            print(warning)
        if entry["not_readable"]:
            print(f"\n  Source '{source}': not readable ({entry['not_readable']})")
            continue
        if not entry["found"]:
            print(f"\n  Source '{source}': not found")
            continue

        print(f"\n  Source: {source}")
        width = max((len(item["data_type"]) for stage in entry["stages"] for item in stage["data_types"]), default=0) + 1
        for stage in entry["stages"]:
            if stage["not_readable"]:
                print(f"    {stage['stage']}: not readable ({stage['not_readable']})")
                continue
            print(f"    {stage['stage']}:")
            if not stage["data_types"]:
                print("      no data types configured")
            for item in stage["data_types"]:
                print(f"      {(item['data_type'] + ':').ljust(width)} {_progress_text(item)}")
                being_written = being_written or (not item["not_readable"] and item["being_written"] > 0)
        if entry["output"]["not_readable"]:
            print(f"    classifier folders: not readable ({entry['output']['not_readable']})")

    print()
    if being_written:
        print("  Note: a file being written can also be left over from an interrupted run.\n")


# ============================================================================
# sdp source size
# ============================================================================

# The stage folders of a source whose size is reported, in pipeline order; each is `paths[<stage>]` of the source config, by default ./data/<stage>/<source>.
_SIZE_STAGES = ("dumps", "extracted", "parsed", "output")
# The units above bytes, each 1000 times the one before.
_SIZE_UNITS = ("kB", "MB", "GB", "TB", "PB")


def cmd_source_size(args):
    """Show how much disk space each source's dumps, extracted, parsed and output folders take."""
    try:
        data = get_source_size(name=args.name)
    except ActionError as e:
        if getattr(args, "json", False):
            # Standard output stays empty, so a reader of the JSON is not handed a line that is not JSON.
            print(f"  Error: {e.message}", file=sys.stderr)
        else:
            print(f"\n  Error: {e.message}\n")
        return e.exit_code

    if getattr(args, "json", False):
        print(json.dumps(data, indent=2))
    else:
        print_source_size(data)
    if args.name is not None and not data["sources"][0]["found"]:
        return 1
    return 0


def get_source_size(*, name=None):
    """Measure the bytes on disk of each stage folder (dumps, extracted, parsed, output), for the named source or for all. Prints nothing and returns `{"sources": [...]}` for `print_source_size`: one entry per source with `name`, `found`, `warnings` (the line about a platform.yaml that cannot be read), `not_readable` (None, or why the config cannot be used), `stages`, `total_bytes` and `complete`. `stages` is empty without a usable config, else holds dumps, extracted, parsed and output in that order, each with `stage`, `path` (None when the config holds no usable path for it), `exists` (the folder was listed), `not_readable`, `files` (the regular files found under it) and `bytes` (their sizes summed). A folder is walked to any depth without following a link inside it; a file or folder that disappears while it is walked is passed over, and a sub-folder that cannot be listed makes its stage not readable with the bytes found so far still counted. `total_bytes` sums the stages, a folder two stages resolve to counted once; `complete` is True when the config was usable and no stage is not readable (a missing folder is 0 bytes, not incomplete). Every `not_readable` is None or a constant reason, and every name and path is display-safe. Raises ActionError for a name that is not one path component, the empty string included (only None means all sources); a well-formed name without a readable config is an entry with `found` false."""
    if name is not None and not _is_one_path_component(name):
        raise ActionError(_SOURCE_NAME_NOT_VALID)

    from social_data_pipeline.setup.utils import list_sources

    sources = [name] if name is not None else list_sources()
    return {"sources": [_source_size_entry(source) for source in sources]}


def _source_size_entry(source):
    """The entry of one source in the data of `get_source_size`."""
    from social_data_pipeline.setup.utils import read_source_config

    entry = {"name": _display_safe(source), "found": False, "warnings": [], "not_readable": None, "stages": [], "total_bytes": 0, "complete": False}
    try:
        config, warning = read_source_config(source)
    except Exception:
        # As in `source progress`: the reader lets OSError, UnicodeDecodeError, RecursionError and the YAML parser's KeyError, IndexError or AttributeError for a tagged value through. This source's config cannot be read and the next source is still reported.
        entry["not_readable"] = _PROGRESS_CONFIG_NOT_READABLE
        return entry
    if warning is not None:
        entry["warnings"].append(_display_safe(warning))
        entry["not_readable"] = _PROGRESS_CONFIG_NOT_READABLE
    if config is None:
        return entry

    entry["found"] = True
    if not isinstance(config, dict):
        entry["not_readable"] = _PROGRESS_CONFIG_NOT_A_MAPPING
        return entry
    paths = config.get("paths", {})
    if not isinstance(paths, dict):
        entry["not_readable"] = _PROGRESS_PATHS_NOT_A_MAPPING
        return entry

    counted = set()
    for stage in _SIZE_STAGES:
        folder = _resolve_progress_folder(paths.get(stage, f"./data/{stage}/{source}"))
        measured = _measure_stage_folder(folder)
        entry["stages"].append({"stage": stage, "path": None if folder is None else _display_safe(folder), **measured})
        if not measured["exists"]:
            continue
        # Two stages configured on one folder (or on a link to it) are one set of bytes on disk.
        try:
            real = os.path.realpath(folder)
        except (OSError, ValueError):
            real = folder
        if real not in counted:
            counted.add(real)
            entry["total_bytes"] += measured["bytes"]
    entry["complete"] = all(stage["not_readable"] is None for stage in entry["stages"])
    return entry


def _measure_stage_folder(folder):
    """Walk one stage folder, without recursion and without following a link inside it, and return `exists`, `not_readable`, `files` and `bytes` as `get_source_size` describes them. The folder itself is opened as the configured path names it (a link to it is followed); `folder` None is a path the config does not hold in a usable form."""
    measured = {"exists": False, "not_readable": None, "files": 0, "bytes": 0}
    if folder is None:
        measured["not_readable"] = _PROGRESS_PATH_NOT_VALID
        return measured

    pending = [folder]
    top = True
    while pending:
        current = pending.pop()
        try:
            with os.scandir(current) as listing:
                measured["exists"] = measured["exists"] or top
                for item in listing:
                    try:
                        if item.is_dir(follow_symlinks=False):
                            pending.append(item.path)
                            continue
                        info = item.stat(follow_symlinks=False)
                    except FileNotFoundError:
                        # A stage renames and deletes files while this runs: one gone since the listing is not counted.
                        continue
                    except OSError:
                        measured["not_readable"] = _PROGRESS_FOLDER_NOT_READABLE
                        continue
                    if stat.S_ISREG(info.st_mode):
                        measured["files"] += 1
                        measured["bytes"] += info.st_size
        except FileNotFoundError:
            # The stage folder not made yet, or a sub-folder removed since its parent was listed.
            continue
        except (OSError, ValueError):
            # ValueError: a path that cannot be handed to the file system at all (a NUL character, a character its encoding does not have).
            measured["not_readable"] = _PROGRESS_FOLDER_NOT_READABLE
        finally:
            top = False
    return measured


def _format_size(size):
    """A number of bytes as the text `source size` shows: `999 B`, then one decimal place in powers of 1000 (`1.0 kB`, `12.3 MB`, up to PB), rounded half up."""
    if size < 1000:
        return f"{size} B"
    scale = 1
    for unit in _SIZE_UNITS:
        scale *= 1000
        tenths = (size * 10 + scale // 2) // scale
        if tenths < 10000 or unit == _SIZE_UNITS[-1]:
            return f"{tenths // 10}.{tenths % 10} {unit}"


def _files_text(count):
    return "1 file" if count == 1 else f"{count} files"


def print_source_size(data):
    """Print the text of `source size` from the data of `get_source_size`."""
    if not data["sources"]:
        print("\n  No sources configured. Run: python sdp.py source add <name>\n")
        return

    print()
    print("  Social Data Pipeline - Source Size")
    print("  ==================================")

    # Two spaces after the longest stage name and its colon; the sizes are right-aligned after that.
    width = max(len(stage) for stage in _SIZE_STAGES) + 3
    for entry in data["sources"]:
        source = entry["name"]
        for warning in entry["warnings"]:
            print(warning)
        if entry["not_readable"]:
            print(f"\n  Source '{source}': not readable ({entry['not_readable']})")
            continue
        if not entry["found"]:
            print(f"\n  Source '{source}': not found")
            continue

        print(f"\n  Source: {source}")
        files_width = max((len(_files_text(stage["files"])) for stage in entry["stages"]), default=0)
        for stage in entry["stages"]:
            label = (stage["stage"] + ":").ljust(width)
            if stage["path"] is None:
                print(f"    {label}not readable ({stage['not_readable']})")
                continue
            if stage["not_readable"]:
                found = f", at least {_format_size(stage['bytes'])} in {_files_text(stage['files'])}" if stage["exists"] else ""
                print(f"    {label}not readable ({stage['not_readable']}){found}  {stage['path']}")
            elif not stage["exists"]:
                print(f"    {label}{'no folder':>9}  {'':{files_width}}  {stage['path']}")
            else:
                print(f"    {label}{_format_size(stage['bytes']):>9}  {_files_text(stage['files']):<{files_width}}  {stage['path']}")
        total = _format_size(entry["total_bytes"])
        print(f"    {'Total:'.ljust(width)}{total:>9}" if entry["complete"] else f"    {'Total:'.ljust(width)}at least {total}")
    print()


# ============================================================================
# sdp source error-logs
# ============================================================================

INGESTION_PROFILES = ["postgres_ingest", "postgres_ml", "mongo_ingest", "sr_ingest", "sr_ml"]


def cmd_source_error_logs(args):
    """Show database ingestion error logs for sources."""
    source_name = args.name
    profile_filter = args.profile

    print_source_error_logs(_get_source_error_logs(source_name, profile_filter))
    return 0


def get_source_error_logs(*, name=None, profile=None):
    """Gather what `source error-logs` shows, for the named source or for all, for one ingestion profile or for every one. Prints nothing and returns `{"name": ..., "sources": [...]}` for `print_source_error_logs`: `name` is the source asked for (None for all); each source has `name`, `found` and `warnings`, and when found `profiles`, one entry per ingestion profile checked, with the state `files` that hold failed entries and, for MongoDB, the `mongoimport_sections` of the logs for those files. Raises ActionError for a name that is not one path component, the empty string included (only None means all sources), and for a profile that is not an ingestion profile; a well-formed name that does not exist is an entry with `found` false."""
    # A profile that does not exist would be checked nowhere and answered with "no errors".
    if profile is not None and profile not in INGESTION_PROFILES:
        raise ActionError("unknown profile")

    if name is not None and not _is_one_path_component(name):
        raise ActionError(_SOURCE_NAME_NOT_VALID)
    return _get_source_error_logs(name, profile)


def _get_source_error_logs(source_name, profile_filter):
    """The body of `get_source_error_logs` without the checks of the name and the profile. The terminal command calls it with both as they were typed, as it always did."""
    from social_data_pipeline.setup.utils import list_sources, read_source_config, get_source_profiles

    if source_name:
        sources = [source_name]
    else:
        sources = list_sources()

    data = {"name": source_name or None, "sources": []}

    if not sources:
        return data

    env = load_env()

    for source in sources:
        config, warning = read_source_config(source)
        entry = {"name": source, "found": config is not None, "warnings": [warning] if warning is not None else []}
        data["sources"].append(entry)
        if config is None:
            continue

        profiles = get_source_profiles(source)
        entry["profiles"] = []

        # Determine which ingestion profiles to check
        check_profiles = [profile_filter] if profile_filter else INGESTION_PROFILES

        for profile in check_profiles:
            if profile not in profiles:
                continue

            # Resolve state directory
            if profile.startswith("postgres"):
                pgdata_path = env.get("PGDATA_PATH", "./data/database/postgres")
                pgdata = Path(pgdata_path)
                if not pgdata.is_absolute():
                    pgdata = ROOT / pgdata
                state_dir = pgdata / "state_tracking"
            elif profile.startswith("sr_"):
                sr_data_path = env.get("STARROCKS_DATA_PATH", "./data/database/starrocks")
                sr_data = Path(sr_data_path)
                if not sr_data.is_absolute():
                    sr_data = ROOT / sr_data
                state_dir = sr_data / "state_tracking"
            else:
                mongo_data_path = env.get("MONGO_DATA_PATH", "./data/database/mongo")
                mongo_data = Path(mongo_data_path)
                if not mongo_data.is_absolute():
                    mongo_data = ROOT / mongo_data
                state_dir = mongo_data / "state_tracking"

            # Failed entries from state files
            failed_files, files = _get_failed_entries(state_dir, source, profile)
            checked = {"profile": profile, "files": files, "mongoimport_sections": []}
            entry["profiles"].append(checked)
            if files:
                # For mongo_ingest, also show relevant mongoimport log sections
                if profile == "mongo_ingest" and failed_files:
                    log_dir = state_dir.parent / "logs"
                    if log_dir.exists():
                        checked["mongoimport_sections"] = _get_mongoimport_log_sections(log_dir, failed_files)

    return data


def print_source_error_logs(data):
    """Print the text of `source error-logs` from the data of `get_source_error_logs`."""
    if not data["sources"]:
        print("\n  No sources configured. Run: python sdp.py source add <name>\n")
        return

    source_name = data["name"]
    print()
    print("  Social Data Pipeline - Error Logs")
    print("  ==================================")

    any_errors = False

    for entry in data["sources"]:
        source = entry["name"]
        for warning in entry["warnings"]:
            print(warning)
        if not entry["found"]:
            print(f"\n  Source '{source}': not found")
            continue

        source_has_errors = False

        for checked in entry["profiles"]:
            profile = checked["profile"]

            for state_file in checked["files"]:
                # The source's header comes once, before its first failed entries.
                if not source_has_errors:
                    print(f"\n  Source: {source}")
                    source_has_errors = True
                any_errors = True

                failed = state_file["failed"]
                print(f"\n    {profile} errors ({state_file['label']}): {len(failed)} failed")
                for failure in failed:
                    timestamp = failure["timestamp"]
                    ts_display = f" [{timestamp}]" if timestamp else ""
                    print(f"\n      {failure['filename']}{ts_display}")
                    for line in failure["error"].split('\n'):
                        print(f"        {line}")

            sections = checked["mongoimport_sections"]
            if sections:
                # One header for all sections; it names the log file of the first, also when later sections come from another file.
                print(f"\n    Mongoimport logs for failed files ({sections[0]['log_file']}):")
            for section in sections:
                # Print header block
                print()
                for hl in section["header"]:
                    print(f"      {hl}")

                # Print output
                if section["output"]:
                    for ol in section["output"]:
                        print(f"      {ol}")
                else:
                    print("      (no mongoimport output found)")

        if not source_has_errors and source_name:
            print(f"\n  Source: {source}")
            print("    No errors found.")

    if not any_errors and not source_name:
        print("\n  No errors found for any source.")

    print()


def _get_failed_entries(state_dir, source, profile):
    """Read failed entries from state files. Returns (failed_filenames, files): the file names of all failed entries, and one dict per state file that holds any, with its `label` and its `failed` entries (`filename`, `error`, `timestamp`)."""
    if not state_dir.exists():
        return [], []

    prefix = f"{source}_{profile}_"
    # Only the named source's own state files: a source without one has no failed entries (B3).
    state_files = sorted(f for f in state_dir.glob("*.json") if f.stem.startswith(prefix))

    if not state_files:
        return [], []

    all_failed = []
    files = []

    for sf in state_files:
        try:
            sdata = _parse_state_json(sf.read_text())
        except (json.JSONDecodeError, OSError):
            continue

        failed = sdata.get("failed", [])
        if not failed:
            continue

        # Extract data type from filename: {source}_{profile}_{data_type}
        parts = sf.stem.split(f"_{profile}_")
        label = parts[1] if len(parts) == 2 else sf.stem

        entries = []
        for entry in failed:
            filename = entry.get("filename", "unknown")
            error = entry.get("error", "no error message")
            timestamp = entry.get("timestamp", "")
            # Strip container log path from error message (we show logs inline)
            display_error = error.split(". See log:")[0] if ". See log:" in error else error
            entries.append({"filename": filename, "error": display_error, "timestamp": timestamp})
            all_failed.append(filename)
        files.append({"label": label, "failed": entries})

    return all_failed, files


def _get_mongoimport_log_sections(log_dir, failed_filenames):
    """Read the mongoimport log sections matching failed filenames. Returns one dict per section, in the order found: the `log_file` it is in, its `header` block and its `output` lines."""
    import re
    separator = re.compile(r'^={10,}$')
    failed_set = set(failed_filenames)
    log_files = sorted(log_dir.glob("mongoimport_*.log"))
    sections = []
    if not log_files:
        return sections

    for lf in log_files:
        try:
            lines = lf.read_text().splitlines()
        except OSError:
            continue

        if not lines:
            continue

        # Find all separator line indices
        sep_indices = [i for i, line in enumerate(lines) if separator.match(line)]

        # Find File: lines and their enclosing header blocks
        for idx, line in enumerate(lines):
            if not line.startswith("File:"):
                continue

            filepath = line.split(":", 1)[1].strip()
            file_id = Path(filepath).name
            if file_id not in failed_set:
                continue

            # Find the header block boundaries (opening and closing ==== lines)
            # The File: line is inside a header block between two ==== lines
            opening_sep = None
            closing_sep = None
            for si in sep_indices:
                if si < idx:
                    opening_sep = si
                elif si > idx and closing_sep is None:
                    closing_sep = si

            if opening_sep is None:
                continue

            # The output block is between the previous ==== and this header's opening ====
            # Find the ==== line before opening_sep (end of previous header)
            prev_sep = None
            for si in sep_indices:
                if si < opening_sep:
                    prev_sep = si
            output_start = (prev_sep + 1) if prev_sep is not None else 0
            output_end = opening_sep

            # Also grab any output after the closing ==== (in case flush order is normal)
            post_start = (closing_sep + 1) if closing_sep is not None else len(lines)
            next_sep = None
            for si in sep_indices:
                if si > (closing_sep if closing_sep is not None else len(lines)):
                    next_sep = si
                    break
            post_end = next_sep if next_sep is not None else len(lines)

            # Collect output lines (before header + after header)
            output_before = lines[output_start:output_end]
            output_after = lines[post_start:post_end]

            # Filter out blank-only blocks
            output_lines = [l for l in output_before if l.strip()]
            if output_after:
                output_lines += [l for l in output_after if l.strip()]

            header_block = lines[opening_sep:((closing_sep + 1) if closing_sep is not None else len(lines))]
            sections.append({"log_file": str(lf), "header": header_block, "output": output_lines})

    return sections


# ============================================================================
# sdp run
# ============================================================================

def cmd_run(args):
    """Run a pipeline profile for a source."""
    try:
        return do_run_start(
            profile=args.profile,
            source=args.source,
            build=args.build,
            filter=args.filter,
            skip_lingua_files=getattr(args, "skip_lingua_files", False),
            classifier=getattr(args, "classifier", None),
            _ask_password=lambda: _prompt_db_password(tag="sdp_db_password"),
        )
    except ActionError as e:
        print(f"\n  Error: {e.message}\n")
        return e.exit_code


def do_run_start(*, profile, source, build=False, filter=None, skip_lingua_files=False, classifier=None, admin_password=None, _ask_password=None, _run_id=None):
    """Run a pipeline profile for a source. Asks nothing itself; returns the exit code.

    `source` may be None, which selects the only configured source (a terminal convenience). The admin password is taken from `admin_password`, else from the `_ask_password` callback at the point where the terminal asks today, and only for a profile whose database has authentication on: a password passed otherwise is ignored.
    """
    from social_data_pipeline.setup.utils import resolve_source_name, load_source_config

    _profile_auth = {
        "postgres_ingest": "POSTGRES_AUTH_ENABLED",
        "postgres_ml": "POSTGRES_AUTH_ENABLED",
        "mongo_ingest": "MONGO_AUTH_ENABLED",
        "sr_ingest": "STARROCKS_AUTH_ENABLED",
        "sr_ml": "STARROCKS_AUTH_ENABLED",
    }
    # `_run_id` is the id of the recorded run this call belongs to, given by the process that supervises that run and never by a terminal: the container is then started with the name and the label by which the run store finds it. The store builds both and refuses anything that is not the id of a recorded run, here, before anything is read or changed.
    _run_options = []
    if _run_id is not None:
        runstore = _run_store()
        try:
            _run_options = ["--name", runstore.container_name(_run_id), "-l", runstore.run_label(_run_id)]
        except runstore.StoreError as e:
            raise ActionError(e.message) from None
    _require_plain_password(admin_password)
    # Without a password and without a terminal to ask on, stop before anything runs: the body would otherwise probe the running containers' mounts before it reaches the question it cannot ask.
    if not admin_password and _ask_password is None:
        auth_key = _profile_auth.get(profile)
        if auth_key and load_env().get(auth_key) == "true":
            raise ActionError(_ADMIN_PASSWORD_REQUIRED)

    if profile not in VALID_PROFILES:
        print(f"  Error: Unknown profile '{profile}'.")
        print(f"  Valid profiles: {', '.join(VALID_PROFILES)}")
        return 1

    if skip_lingua_files and profile != "parse":
        print(f"  Error: --skip-lingua-files only applies to the 'parse' profile (got '{profile}').")
        return 1

    if classifier and profile not in ("ml", "lingua", "sr_ml"):
        print(f"  Error: --classifier only applies to the 'ml', 'lingua' and 'sr_ml' profiles (got '{profile}').")
        return 1

    # Resolve source (auto-selects if only one exists)
    try:
        source = resolve_source_name(source)
    except ValueError as e:
        print(e)
        return 1
    source_config = load_source_config(source)

    # Determine PLATFORM from source config
    if source == "reddit":
        platform = "reddit"
    else:
        platform = f"custom/{source}"

    # --- Gate on configured DB / source overrides ---
    # Without these checks, `sdp run postgres_ingest` after `db unsetup` would
    # silently fall through to the global pipeline.yaml defaults; for sources
    # missing the per-profile override file, the run would proceed with no
    # source-specific config at all.
    required_db = _PROFILE_DB.get(profile)
    if required_db is not None:
        db_config = CONFIG_DIR / "db" / f"{required_db}.yaml"
        if not db_config.exists():
            print(f"  Error: profile '{profile}' requires {required_db}, "
                  f"but no {required_db} database is configured.")
            print(f"  Run 'sdp db setup' (or 'sdp db setup --add {required_db}') first.")
            return 1

    source_file = _PROFILE_SOURCE_FILE.get(profile)
    if source_file is not None:
        source_override = CONFIG_DIR / "sources" / source / f"{source_file}.yaml"
        if not source_override.exists():
            print(f"  Error: source '{source}' has no '{profile}' configuration.")
            print(f"  Expected: config/sources/{source}/{source_file}.yaml")
            print(f"  Run 'sdp source configure {source}' to add it, "
                  f"or 'sdp source add {source}' to reconfigure profiles.")
            return 1

    # Server-side mount validation: PG/SR ingest profiles read parsed/output
    # files via bind mounts on the DB server. If `source add` happened after
    # `db start`, the running container's mount set is stale and pg_parquet /
    # FILES() will die with an opaque "no such file" error. Probe live
    # container mounts and fail-fast with the recovery line instead.
    rc = _validate_run_mounts(profile, source, (source_config or {}).get("paths", {}))
    if rc != 0:
        return rc

    # Prompt for admin password if auth enabled for the target database
    auth_key = _profile_auth.get(profile)
    if auth_key and load_env().get(auth_key) == "true":
        password = _resolve_admin_password(admin_password, _ask_password)
        _set_auth_env(password)

    # Build per-source environment (read paths from source config, with defaults)
    paths = source_config.get("paths", {}) if source_config else {}
    env_overrides = {
        "PLATFORM": platform,
        "SOURCE": source,
        "DUMPS_PATH": paths.get("dumps", f"./data/dumps/{source}"),
        "PARSED_PATH": paths.get("parsed", f"./data/parsed/{source}"),
        "EXTRACTED_PATH": paths.get("extracted", f"./data/extracted/{source}"),
        "OUTPUT_PATH": paths.get("output", f"./data/output/{source}"),
    }

    # Pre-create data directories so Docker doesn't create them as root:root
    for key in ("DUMPS_PATH", "PARSED_PATH", "EXTRACTED_PATH", "OUTPUT_PATH"):
        data_dir = Path(env_overrides[key])
        if not data_dir.is_absolute():
            data_dir = ROOT / data_dir
        data_dir.mkdir(parents=True, exist_ok=True)

    # Pass file filter if provided
    if filter:
        env_overrides["FILE_FILTER"] = filter

    if skip_lingua_files:
        env_overrides["SKIP_LINGUA_FILES"] = "1"

    if classifier:
        env_overrides["CLASSIFIER"] = classifier

    # Set env vars for docker compose
    for key, value in env_overrides.items():
        os.environ[key] = value

    service = PROFILE_SERVICE_MAP.get(profile, profile)
    compose_args = ["--profile", profile, "run", "--rm"]
    if build:
        compose_args.append("--build")
    compose_args.extend(_run_options)
    compose_args.append(service)

    print(f"  Source: {source} (platform: {platform})")
    if filter:
        print(f"  Filter: {filter}")
    if skip_lingua_files:
        print("  Skip-lingua-files: enabled")
    if classifier:
        print(f"  Classifier: {classifier} (single)")
    result = docker_compose(*compose_args)
    return result.returncode


# ============================================================================
# sdp runs
# ============================================================================

# Why a runs command refused a number. Neither holds an input value.
_RUN_LIMIT_NOT_VALID = "the limit is not a whole number in the accepted range"
_RUN_OFFSET_NOT_VALID = "the offset is not a whole number in the accepted range"
_RUN_LIST_LIMIT_MAX = 1000
_RUN_LOG_LIMIT_MAX = 1048576
_RUN_LOG_OFFSET_MAX = 2 ** 53
# How many bytes of a log file the logs command reads at a time.
_RUN_LOG_PIECE = 65536
# The states in which a live process supervises a run, so its log file can still grow.
_RUN_STATES_SUPERVISED = ("starting", "running")
_RUN_LIST_COLUMNS = (("ID", "id"), ("KIND", "kind"), ("PROFILE", "profile"), ("SOURCE", "source"), ("STATE", "state"), ("STARTED", "started"), ("ENDED", "ended"), ("EXIT", "exit_code"))
_RUN_DOCKER_NOTE = "  Note: Docker cannot be reached. Runs started in a terminal are not listed, and a recorded run that has not ended and has no live supervisor shows as unknown."
# What `runs cancel` says it did, by the outcome the run store reports.
_RUN_CANCEL_TEXT = {
    "stopped": "interrupt signal sent, the container has stopped",
    "requested": "cancel requested, the process that holds the run stops it",
    "recorded": "recorded as cancelled, no container was left to stop",
}


def _run_store():
    """The module that keeps the runs. Imported at first use, so that loading the CLI loads nothing more than before."""
    from social_data_pipeline import runstore

    return runstore


def _run_service_profiles():
    """The compose service of each pipeline profile, mapped to its profile. A profile without an entry in PROFILE_SERVICE_MAP has a service of its own name."""
    return {PROFILE_SERVICE_MAP.get(profile, profile): profile for profile in VALID_PROFILES}


def _require_run_id(run_id):
    """Refuse a run id that has neither the form of a recorded run nor that of a terminal run, before it is used for a path or handed to Docker. The message is a constant."""
    runstore = _run_store()
    try:
        runstore.run_id_kind(run_id)
    except runstore.StoreError as e:
        raise ActionError(e.message) from None


def _require_whole_number(value, lowest, highest, message):
    """Refuse a value that is not a whole number from lowest to highest (a boolean is not a number)."""
    if isinstance(value, bool) or not isinstance(value, int) or not lowest <= value <= highest:
        raise ActionError(message)


def _run_display_safe(value):
    """A value of the run data with every string in it made display-safe, the keys of a mapping included."""
    if isinstance(value, str):
        return _display_safe(value)
    if isinstance(value, dict):
        return {_display_safe(key): _run_display_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_run_display_safe(item) for item in value]
    return value


def get_run_list(*, limit=50):
    """List the pipeline runs of this deployment. Prints nothing and returns `{"docker_available": bool, "runs": [...]}` for `print_run_list`: the recorded runs, newest first and at most `limit` (1 to 1000) of them, then the runs started with `sdp run` in a terminal that are alive now, oldest first. Each run has `id`, `kind` (`web` for a recorded pipeline run, `task` for a recorded task, a CLI action that the web UI started as a run, `terminal`), `profile` (None for a task), `source`, `flags` (the non-secret fields of the request that started a recorded run; None for a terminal run), `started_by`, `state`, `started` and `ended` (ISO-8601 UTC times ending in Z, or None) and `exit_code`; a task also has `task` (its verb) and `label` (what is shown in place of a profile). The state of a recorded run is `starting`, `running`, `finished` or `cancelled` as recorded; `detached` when the process that supervised it is gone and its container is alive, `lost` when both are gone, `unknown` when Docker cannot be asked, `unreadable` when its record cannot be used (every other field of it is then None). A terminal run is `running`; its source is the container's `SOURCE` variable, the only part of a container's environment that is read. When Docker cannot be reached nothing is raised: `docker_available` is false and no terminal run is listed. Every string that comes from Docker or a record is display-safe. Raises ActionError for a limit out of range, and when the run state folder is a link or cannot be opened."""
    _require_whole_number(limit, 1, _RUN_LIST_LIMIT_MAX, _RUN_LIMIT_NOT_VALID)
    runstore = _run_store()
    try:
        data = runstore.list_runs(ROOT, _run_service_profiles(), limit)
    except runstore.StoreError as e:
        raise ActionError(e.message) from None
    return {"docker_available": data["docker_available"], "runs": [_run_display_safe(run) for run in data["runs"]]}


def get_run_logs(*, run_id, offset=0, limit=65536):
    """Read a piece of the output of one run. Prints nothing and returns `id`, `kind`, `state`, `text`, `next_offset` and `complete`. For a recorded run the text is read from its log file: `offset` (0 to 2**53) and `limit` (1 to 1048576) are byte positions, the piece never ends inside a multi-byte character, bytes that are not UTF-8 become the replacement character, `next_offset` is the offset of the next piece (an offset at or past the end gives an empty text) and `complete` is true. For a terminal run, and for a recorded run that is `detached`, the text is the last 500 lines Docker has, standard output and standard error together, cut to their last `limit` bytes (the end is kept and the text begins with a whole character): `offset` does not apply, `next_offset` is None and `complete` is false. `text` is the one string that is NOT made display-safe: it is returned as decoded, with whatever control and format characters the program wrote, and the caller renders it. Raises ActionError with a constant message for a run id that has neither of the two forms (before any path or Docker use), for one that names no run, for a number out of range, and when the log or Docker cannot be read (Docker out of reach, or a container that is listed and whose output Docker does not hand over)."""
    _require_run_id(run_id)
    _require_whole_number(offset, 0, _RUN_LOG_OFFSET_MAX, _RUN_OFFSET_NOT_VALID)
    _require_whole_number(limit, 1, _RUN_LOG_LIMIT_MAX, _RUN_LIMIT_NOT_VALID)
    runstore = _run_store()
    try:
        data = runstore.read_run_log(ROOT, _run_service_profiles(), run_id, offset, limit)
    except runstore.StoreError as e:
        raise ActionError(e.message) from None
    return {"id": data["id"], "kind": data["kind"], "state": data["state"], "text": data["text"], "next_offset": data["next_offset"], "complete": data["complete"]}


def do_run_cancel(*, run_id):
    """Cancel one run. Asks nothing; prints one line that says what was done and returns the exit code: 0 when the run's container was sent the interrupt signal (a terminal run, or a recorded run without a live supervisor, which is then recorded as cancelled), when such a recorded run had no container left and was recorded as cancelled without a signal, or when the cancel was left for the process that holds the run's lock (the one that supervises the run, or another cancel that is at work); 1 when the run is refused because it has ended, is lost or has no usable record, in which case nothing is stopped and neither a cancel request nor the record is written, and 1 when Docker could not stop the container, in which case the record stays as it was and the run can be cancelled again. A terminal run is looked up again in the current listing, so nothing but a listed run of this deployment is ever stopped. Raises ActionError with a constant message for a run id that has neither of the two forms (before any path or Docker use), for one that names no run, and when Docker is needed and cannot be reached. Two of these can come AFTER something was done to a recorded run without a live supervisor: when no container of the run is left (this call stopped it, or it had ended by itself) and the record then could not be written, and when Docker stops answering after the cancel request file was written or after the stop was sent; in both the cancel request file exists and the record still says the run is alive."""
    _require_run_id(run_id)
    runstore = _run_store()
    try:
        outcome = runstore.cancel_run(ROOT, _run_service_profiles(), run_id)
    except runstore.CancelRefused as e:
        print(f"  Run {run_id}: not cancelled, {e.message}.")
        return 1
    except runstore.StoreError as e:
        raise ActionError(e.message) from None
    print(f"  Run {run_id}: {_RUN_CANCEL_TEXT[outcome]}.")
    return 0


def _run_list_cell(run, key):
    """One cell of `runs list`: the value, `-` when there is none. A task has no profile and shows its label in that column."""
    value = run.get("label") if key == "profile" and run["kind"] == "task" else run[key]
    return "-" if value is None else str(value)


def print_run_list(data):
    """Print the text of `runs list` from the data of `get_run_list`: one line per run, the columns aligned, a missing value as `-`."""
    if not data["runs"]:
        print("\n  No runs.\n")
    else:
        rows = [[title for title, _ in _RUN_LIST_COLUMNS]]
        rows += [[_run_list_cell(run, key) for _, key in _RUN_LIST_COLUMNS] for run in data["runs"]]
        widths = [max(len(row[index]) for row in rows) for index in range(len(_RUN_LIST_COLUMNS))]
        print()
        print("  Social Data Pipeline - Runs")
        print("  ===========================")
        print()
        for row in rows:
            print("  " + "  ".join(cell.ljust(width) for cell, width in zip(row, widths)).rstrip())
        print()
    if not data["docker_available"]:
        print(_RUN_DOCKER_NOTE + "\n")


def _last_lines(text, count):
    """The last `count` lines of text, a last line without a line end included. Only the newline ends a line: a carriage return is how a progress bar rewrites its line."""
    if count <= 0:
        return ""
    lines = text.split("\n")
    ends_in_newline = lines[-1] == ""
    if ends_in_newline:
        lines.pop()
    return "\n".join(lines[-count:]) + ("\n" if ends_in_newline and lines else "")


def print_run_logs(data, *, tail=None):
    """Write the text of the data of `get_run_logs` to standard output as it is, the way `docker logs` does; with `tail`, only its last lines. Nothing is added, so pieces written one after the other are the log."""
    sys.stdout.write(data["text"] if tail is None else _last_lines(data["text"], tail))


def _show_run_logs(*, run_id, offset, limit, tail, follow):
    """The body of `runs logs`. A log file is read piece by piece from `offset` to its end (one piece of at most `limit` bytes when a limit is given); with `tail` only the last lines of that are printed; with `follow` the file is then read again about once a second for as long as a live process supervises the run. Output that Docker holds (a terminal run, a detached run) is its last 500 lines, cut to their last `limit` bytes (65536 when no limit is given), and with `follow` Docker keeps printing until the container stops."""
    import time

    runstore = _run_store()
    data = get_run_logs(run_id=run_id, offset=offset, limit=_RUN_LOG_PIECE if limit is None else limit)
    from_file = data["complete"]
    kept = None if tail is None else ""
    while data["complete"]:
        if kept is None:
            print_run_logs(data)
        else:
            # The last lines of everything read so far; a line cut by the end of a piece is completed by the next one.
            kept = _last_lines(kept + data["text"], tail)
        at_end = data["text"] == ""
        if at_end or (limit is not None and not follow):
            if kept is not None:
                sys.stdout.write(kept)
                kept = None
            sys.stdout.flush()
            if not follow or (at_end and data["state"] not in _RUN_STATES_SUPERVISED):
                return
            if at_end:
                time.sleep(1)
        data = get_run_logs(run_id=run_id, offset=data["next_offset"], limit=_RUN_LOG_PIECE)

    # The output is with Docker: the run was started in a terminal, or the process that wrote its log file is gone.
    if kept:
        sys.stdout.write(kept)
    if not follow:
        if not from_file:
            print_run_logs(data, tail=tail)
        return
    sys.stdout.flush()
    lines = 0 if from_file else runstore.LOG_TAIL_LINES if tail is None else min(tail, runstore.LOG_TAIL_LINES)
    try:
        runstore.follow_run_log(ROOT, _run_service_profiles(), run_id, lines)
    except runstore.RunNotFound:
        # The container stopped between the two calls: there is nothing left to follow.
        return
    except runstore.StoreError as e:
        raise ActionError(e.message) from None


def cmd_runs_list(args):
    """List the pipeline runs."""
    try:
        data = get_run_list(limit=args.limit)
    except ActionError as e:
        if getattr(args, "json", False):
            # Standard output stays empty, so a reader of the JSON is not handed a line that is not JSON.
            print(f"  Error: {e.message}", file=sys.stderr)
        else:
            print(f"\n  Error: {e.message}\n")
        return e.exit_code

    if getattr(args, "json", False):
        print(json.dumps(data, indent=2))
    else:
        print_run_list(data)
    return 0


def cmd_runs_logs(args):
    """Print the output of a run."""
    if args.tail is not None and args.tail < 0:
        print("\n  Error: --tail must not be negative\n")
        return 1
    try:
        _show_run_logs(run_id=args.run_id, offset=args.offset, limit=args.limit, tail=args.tail, follow=args.follow)
    except ActionError as e:
        print(f"\n  Error: {e.message}\n")
        return e.exit_code
    except KeyboardInterrupt:
        if not args.follow:
            raise
        # Ctrl-C is how following ends.
        print()
    return 0


def cmd_runs_cancel(args):
    """Cancel a run."""
    try:
        return do_run_cancel(run_id=args.run_id)
    except ActionError as e:
        print(f"\n  Error: {e.message}\n")
        return e.exit_code


# ============================================================================
# sdp broker
# ============================================================================

# What `broker status` says about the management password, by what the broker or the file answered.
_BROKER_PASSWORD_TEXT = {True: "set", False: "not set", None: "cannot be told (the password file is not usable)"}


def _broker_module():
    """The broker's module. Imported at first use, so that loading the CLI loads nothing more than before."""
    from social_data_pipeline.control import broker

    return broker


def cmd_broker_passwd(args):
    """Set the broker's management password: asked twice, never echoed. Running sessions end at their next request."""
    from social_data_pipeline.control import files, passwords

    broker = _broker_module()
    first = getpass("  New management password: ")
    second = getpass("  The same password again: ")
    if first != second:
        print("\n  Error: the two passwords are not the same.\n")
        return 1
    try:
        with files.folder(ROOT / broker.STATE_FOLDER, create=True):
            pass
        passwords.set_password(broker.broker_folder(ROOT), first)
    except (passwords.PasswordRefused, passwords.PasswordFileNotUsable, passwords.PasswordNotDerivable, files.BrokerFileError) as e:
        print(f"\n  Error: {e.message}.\n")
        return 1
    print("\n  The management password is set. Sessions that logged in with another one end at their next request.\n")
    return 0


def cmd_broker_start(args):
    """Start the broker: detached (the default), or in this process with --foreground."""
    broker = _broker_module()
    if args.foreground:
        return broker.main(["--root", str(ROOT), "--foreground"])
    outcome, detail = broker.start_detached(ROOT)
    if outcome == "started":
        print(f"  The broker is started (pid {detail}).")
        return 0
    if outcome == "running":
        print(f"  The broker is already running (pid {detail}).")
        return 0
    print(f"  Error: {detail}")
    return 1


def cmd_broker_stop(args):
    """Stop the broker. It lets a state-changing action that runs finish first; with --force it ends that action."""
    broker = _broker_module()

    def say_waiting():
        if args.force:
            print("  Waiting for the broker: it ends the action that runs, then stops.", flush=True)
        else:
            print("  Waiting for the broker: it lets the action that runs finish, then stops.", flush=True)

    try:
        stopped = broker.stop_broker(ROOT, force=args.force, on_wait=say_waiting)
    except broker.StopRefused as e:
        print(f"  Error: {e.message}.")
        return 1
    if not stopped:
        print("  The broker is not running.")
        return 1
    print("  The broker has stopped.")
    return 0


def cmd_broker_status(args):
    """Say whether the broker runs. Exit code 0 when it runs, 1 when it does not."""
    status = _broker_module().broker_status(ROOT)
    if status["running"]:
        print(f"  Broker: running (pid {status['pid']}, since {status['started']})")
        if not status["answering"]:
            print("  It does not answer on its socket.")
    else:
        print("  Broker: not running")
    print(f"  Socket: {status['socket']}")
    print(f"  Management password: {_BROKER_PASSWORD_TEXT[status['password_set']]}")
    return 0 if status["running"] else 1


def _broker_audit_value(value):
    """A value of an audit line as it is printed: plain text as it is when it is printable ASCII, anything else as JSON in ASCII."""
    if isinstance(value, str) and value.isascii() and value.isprintable():
        return value
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"))


def _broker_audit_text(line):
    """One audit line as one line of text: time, session, event, and the event's fields as name=value."""
    if line.get("unreadable") is True and len(line) == 1:
        return "  (a line that cannot be read)"
    if line.get("truncated") is True and len(line) == 1:
        return "  (older lines left out: the newer ones hold more than is read at once)"
    session = line.get("session")
    details = " ".join(f"{_broker_audit_value(key)}={_broker_audit_value(value)}" for key, value in line.items() if key not in ("n", "t", "session", "event", "prev"))
    return f"  {_broker_audit_value(line.get('t'))}  session {'-' if session is None else _broker_audit_value(session)}  {_broker_audit_value(line.get('event'))}  {details}".rstrip()


def cmd_broker_audit(args):
    """Print the newest lines of the broker's audit log, or with --verify check its chain (exit code 1 on a break or a file that cannot be read)."""
    from social_data_pipeline.control import audit

    path = os.path.join(_broker_module().broker_folder(ROOT), audit.FILE_NAME)
    if args.verify:
        report = audit.verify(path)
        found = {"readable": report.readable, "lines": report.lines, "breaks": report.breaks, "first_break": report.first_break, "cut_off": report.cut_off, "repaired": report.repaired, "tail_changed": report.tail_changed}
        if args.json:
            print(json.dumps(found, indent=2))
        elif not report.readable:
            print("  The audit log cannot be read.")
        else:
            print(f"  Lines: {report.lines}")
            print(f"  Breaks of the chain: {report.breaks}" + ("" if report.first_break is None else f" (the first where line {report.first_break} was expected)"))
            print(f"  Cut-off end: {'yes' if report.cut_off else 'no'}")
            print(f"  Repaired runs of cut-off lines: {report.repaired}")
            print(f"  Lines that say the end of the log was changed: {report.tail_changed}")
            print("  The chain is whole." if report.breaks == 0 else "  The chain is broken.")
        return 0 if report.readable and report.breaks == 0 else 1
    if not 1 <= args.limit <= 10000:
        print("  Error: --limit must be from 1 to 10000.")
        return 1
    lines = audit.read_tail(path, args.limit)
    if args.json:
        print(json.dumps(lines, indent=2))
    elif not lines:
        print("  The audit log is empty, or cannot be read.")
    else:
        for line in lines:
            print(_broker_audit_text(line))
    return 0


# ============================================================================
# sdp webui
# ============================================================================


def _webui_host():
    """The web UI's host-side module. Imported at first use, so that loading the CLI loads nothing more than before."""
    from social_data_pipeline import webui_host

    return webui_host


def cmd_webui_setup(args):
    """Set up the web UI: ask its port, listen addresses, names and certificate, build its image and make or copy the certificate."""
    return _webui_host().cmd_setup(ROOT)


def cmd_webui_start(args):
    """Start the web UI's container. Exit code 0 when it was started or already runs."""
    return _webui_host().cmd_start(ROOT)


def cmd_webui_stop(args):
    """Stop and remove the web UI's container. Exit code 0 when it ran, 1 when it did not."""
    return _webui_host().cmd_stop(ROOT)


def cmd_webui_rebuild(args):
    """Rebuild the web UI's image from the saved settings, then stop it (when it runs) and start it. A failed build stops nothing."""
    return _webui_host().cmd_rebuild(ROOT)


def cmd_webui_status(args):
    """Say whether the web UI is set up and runs, where it answers and its certificate. Exit code 0 when it runs, 1 when not."""
    return _webui_host().cmd_status(ROOT)


def cmd_webui_unsetup(args):
    """Remove the web UI's container, network, image and settings, after a confirmation."""
    from social_data_pipeline.setup.utils import ask_bool

    return _webui_host().cmd_unsetup(ROOT, lambda: ask_bool("Remove the web UI's container, network, image and settings (.sdp/webui)?", default=False, tag="webui_unsetup_confirm"))


# ============================================================================
# sdp chat
# ============================================================================


def _chat_host():
    """The chat's host-side module. Imported at first use, so that loading the CLI loads nothing more than before."""
    from social_data_pipeline import chat_host

    return chat_host


def cmd_chat_setup(args):
    """Set up the Chat window: ask its back end, the model's credential and the work folder's cap, and build its two images."""
    return _chat_host().cmd_setup(ROOT)


def cmd_chat_start(args):
    """Start the chat: its two networks, the gateway, the databases attached, the sandbox."""
    return _chat_host().cmd_start(ROOT)


def cmd_chat_stop(args):
    """Stop and remove the chat's containers and networks, and detach the databases."""
    return _chat_host().cmd_stop(ROOT)


def cmd_chat_rebuild(args):
    """Rebuild the chat's two images from the saved settings, then stop the chat (when it runs) and start it. A failed build stops nothing."""
    return _chat_host().cmd_rebuild(ROOT)


def cmd_chat_status(args):
    """Say whether the chat runs, which databases it can read now and the size of its work folder."""
    return _chat_host().cmd_status(ROOT)


def cmd_chat_settings(args):
    """Show the chat's settings for the whole deployment that the web UI can change: the read-only query limit."""
    data = get_chat_settings()
    print_chat_settings(data)
    return 1 if data["query_limit"]["problem"] else 0


def get_chat_settings():
    """The chat's settings for the whole deployment: `query_limit`, the query time limit of the chat's MongoDB and StarRocks MCP servers (`chat_host.query_setting`: `seconds`, `default`, `default_seconds`, `max_seconds`, `problem`), and `auto_title`, the switch of generated chat titles (`chat_host.auto_title_setting`: `on`, `default`, `problem`), which `chat settings` does not print (`chat auto-title` does). Only those two values of .env are read. Prints nothing; never raises."""
    host = _chat_host()
    return {"query_limit": host.query_setting(str(ROOT)), "auto_title": host.auto_title_setting(str(ROOT))}


def print_chat_settings(data):
    """Print the text of `chat settings` (and of `chat query-limit` without a value) from the data of `get_chat_settings`."""
    limit = data["query_limit"]
    print()
    if limit["problem"]:
        print(f"  Error: {limit['problem']}.")
    else:
        source = f"the default: {_chat_host().QUERY_KEY} is not set in .env" if limit["default"] else f"{_chat_host().QUERY_KEY} in .env"
        print(f"  Read-only query limit for chats: {limit['seconds']} s ({source})")
    print("  MongoDB and StarRocks stop a chat's query after this many seconds. PostgreSQL ignores it (its MCP server keeps its own limit: 30 s in restricted mode, the default).")
    print(f"  Change it: python sdp.py chat query-limit <seconds> (a whole number from 1 to {limit['max_seconds']}; {limit['default_seconds']} when it is not set)")
    print()


def cmd_chat_query_limit(args):
    """Without a value, show the chat's read-only query limit; with one, set it (`do_chat_query_limit`)."""
    if args.seconds is None:
        return cmd_chat_settings(args)
    host = _chat_host()
    seconds = host.parse_query_seconds(args.seconds)
    if seconds is None:
        print(f"  Error: the limit must be a whole number of seconds from 1 to {host.QUERY_SECONDS_MAX}. Nothing was changed.")
        return 1
    try:
        return do_chat_query_limit(seconds=seconds)
    except ActionError as error:
        print(f"  Error: {error.message}.")
        return error.exit_code


def do_chat_query_limit(*, seconds):
    """`chat query-limit <seconds>` with its value given: write CHAT_QUERY_TIMEOUT_SECONDS to .env (`update_env_file`, every other line kept), then, when the chat runs, start its copies of the MongoDB and StarRocks MCP servers that run now again with the new limit and attach them again (`chat_host.apply_query_limit`); the gateway and the sandbox are not stopped. The value stays written when a copy cannot be started again. Asks nothing; returns the exit code. Raises ActionError, before anything is written, for a limit that is not a whole number of seconds from 1 to 3600."""
    host = _chat_host()
    if type(seconds) is not int or not 1 <= seconds <= host.QUERY_SECONDS_MAX:
        raise ActionError(f"the limit must be a whole number of seconds from 1 to {host.QUERY_SECONDS_MAX}")
    from social_data_pipeline.setup.utils import update_env_file

    try:
        update_env_file({host.QUERY_KEY: str(seconds)})
    except OSError as error:
        print(f"  Error: .env could not be written ({error.strerror or type(error).__name__}). Check the file and run the command again.")
        return 1
    print(f"  Updated:   .env ({host.QUERY_KEY}={seconds})")
    return host.apply_query_limit(str(ROOT), seconds)


def cmd_chat_auto_title(args):
    """Without a value, show whether a new chat gets a title made by the chat's model; with `on` or `off` (the parser refuses anything else), set it (`do_chat_auto_title`)."""
    if args.value is None:
        setting = _chat_host().auto_title_setting(str(ROOT))
        key = _chat_host().AUTO_TITLE_KEY
        print()
        if setting["problem"]:
            print(f"  Error: {setting['problem']}.")
        else:
            source = f"the default: {key} is not set in .env" if setting["default"] else f"{key} in .env"
            print(f"  Generated chat titles: {'on' if setting['on'] else 'off'} ({source})")
        print("  A new chat gets a short title made from its first question by the chat's model, a few seconds after it is sent; a title you set is never replaced; its cost counts in the chat's Cost.")
        print("  Change it: python sdp.py chat auto-title on|off (on when it is not set); a change takes effect at the chat's next start.")
        print()
        return 1 if setting["problem"] else 0
    try:
        return do_chat_auto_title(value=args.value)
    except ActionError as error:
        print(f"  Error: {error.message}.")
        return error.exit_code


def do_chat_auto_title(*, value):
    """`chat auto-title on|off` with its value given: write CHAT_AUTO_TITLE to .env (`update_env_file`, every other line kept). Restarts nothing: the sandbox is given the setting at the chat's next start (`chat_host.sandbox_arguments`). Asks nothing; returns the exit code. Raises ActionError, before anything is written, for a value that is not exactly `on` or `off`."""
    host = _chat_host()
    if not isinstance(value, str) or value not in host.AUTO_TITLE_VALUES:
        raise ActionError("the setting must be on or off")
    from social_data_pipeline.setup.utils import update_env_file

    try:
        update_env_file({host.AUTO_TITLE_KEY: value})
    except OSError as error:
        print(f"  Error: .env could not be written ({error.strerror or type(error).__name__}). Check the file and run the command again.")
        return 1
    print(f"  Updated:   .env ({host.AUTO_TITLE_KEY}={value})")
    print("  It takes effect at the chat's next start: python sdp.py chat stop, then python sdp.py chat start. Nothing was restarted.")
    return 0


def cmd_chat_unsetup(args):
    """Remove the chat's images, settings and credential after a confirmation; all chats and their files only after a second one."""
    from social_data_pipeline.setup.utils import ask_bool

    return _chat_host().cmd_unsetup(
        ROOT,
        lambda: ask_bool("Remove the chat's images, settings and credential?", default=False, tag="chat_unsetup_confirm"),
        lambda: ask_bool("Also delete all chats and their files (.sdp/chat/work)?", default=False, tag="chat_unsetup_work"),
    )


# ============================================================================
# Argument parser
# ============================================================================

def build_parser():
    parser = argparse.ArgumentParser(
        prog="sdp.py",
        description="Social Data Pipeline CLI",
    )
    parser.add_argument("--tag", action="store_true",
                        help="Prefix interactive prompts with [tag] identifiers for automation")
    subparsers = parser.add_subparsers(dest="command", help="Command group")

    # ---- sdp db ----
    db_parser = subparsers.add_parser("db", help="Database management")
    db_sub = db_parser.add_subparsers(dest="db_command", help="Database command")

    db_setup_p = db_sub.add_parser("setup", help="Configure databases (PostgreSQL, MongoDB)")
    db_setup_p.add_argument("--add", choices=["postgres", "mongo", "starrocks"],
                            help="Add a single database to existing setup without reconfiguring others")
    db_setup_p.set_defaults(func=cmd_db_setup)

    db_settings_p = db_sub.add_parser("settings", help="Show the current answers of db setup (never a password)")
    db_settings_p.add_argument("--json", action="store_true", help="Print the data as JSON instead of the text")
    db_settings_p.set_defaults(func=cmd_db_settings)

    db_setup_mcp_p = db_sub.add_parser("setup-mcp", help="Configure MCP servers for databases")
    db_setup_mcp_p.set_defaults(func=cmd_db_setup_mcp)

    db_setup_jobs_p = db_sub.add_parser(
        "setup-jobs",
        help="Configure the query scheduler (jobs profile)",
    )
    db_setup_jobs_p.set_defaults(func=cmd_db_setup_jobs)

    db_setup_llm_p = db_sub.add_parser(
        "setup-llm",
        help="Bundle: run setup-mcp then setup-jobs",
    )
    db_setup_llm_p.set_defaults(func=cmd_db_setup_llm)

    db_restart_needed_p = db_sub.add_parser(
        "restart-needed",
        help="List the running services whose settings files changed after they started",
    )
    db_restart_needed_p.set_defaults(func=cmd_db_restart_needed)

    db_llm_settings_p = db_sub.add_parser(
        "llm-settings",
        help="Show the current answers of setup-mcp and setup-jobs (no secrets)",
    )
    db_llm_settings_p.set_defaults(func=cmd_db_llm_settings)

    db_start_p = db_sub.add_parser("start", help="Start database services")
    db_start_p.add_argument("service", nargs="?",
                            choices=["postgres", "mongo", "starrocks",
                                     "postgres-mcp", "mongo-mcp", "starrocks-mcp",
                                     "jobs"],
                            help="Specific service (default: all configured)")
    db_start_p.set_defaults(func=cmd_db_start)

    db_stop_p = db_sub.add_parser("stop", help="Stop database services")
    db_stop_p.add_argument("service", nargs="?",
                           choices=["postgres", "mongo", "starrocks",
                                    "postgres-mcp", "mongo-mcp", "starrocks-mcp",
                                    "jobs"],
                           help="Specific service (default: all configured)")
    db_stop_p.set_defaults(func=cmd_db_stop)

    db_status_p = db_sub.add_parser("status", help="Show database status")
    db_status_p.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Include per-source ingestion breakdown for each configured DB",
    )
    db_status_p.set_defaults(func=cmd_db_status)

    db_verify_p = db_sub.add_parser(
        "verify",
        help="Check config / env / creds / containers / mounts coherence; exits 1 on drift",
    )
    db_verify_p.add_argument(
        "--db", choices=["postgres", "mongo", "starrocks"],
        help="Verify a single database (cross-cutting MCP / jobs checks still apply)",
    )
    db_verify_p.add_argument(
        "--json", action="store_true",
        help="Emit machine-readable JSON for CI / scripts",
    )
    db_verify_p.set_defaults(func=cmd_db_verify)

    db_unsetup_p = db_sub.add_parser("unsetup", help="Remove database configuration")
    db_unsetup_p.add_argument("--db", choices=["postgres", "mongo", "starrocks"],
                               help="Remove a single database (config, containers, and data) without affecting others")
    db_unsetup_p.set_defaults(func=cmd_db_unsetup)

    db_reset_p = db_sub.add_parser(
        "reset",
        help="Wipe a database's data (keeps config, env, MCP, and per-source overrides)",
    )
    db_reset_p.add_argument(
        "--db", choices=["postgres", "mongo", "starrocks"], required=True,
        help="Database to reset",
    )
    db_reset_p.set_defaults(func=cmd_db_reset)

    db_unsetup_mcp_p = db_sub.add_parser("unsetup-mcp", help="Remove MCP configuration")
    db_unsetup_mcp_p.set_defaults(func=cmd_db_unsetup_mcp)

    db_unsetup_jobs_p = db_sub.add_parser(
        "unsetup-jobs",
        help="Remove jobs scheduler configuration and stop its container",
    )
    db_unsetup_jobs_p.set_defaults(func=cmd_db_unsetup_jobs)

    db_recover_p = db_sub.add_parser("recover-password", help="Reset database admin password")
    db_recover_p.add_argument(
        "--regenerate-ro", dest="regenerate_ro", action="store_true",
        help="Also rewrite .ro_credentials with a fresh password (orthogonal "
             "to admin recovery; combinable). Use this when .ro_credentials "
             "is missing, lost, or known-leaked.",
    )
    db_recover_p.set_defaults(func=cmd_db_recover_password)

    db_indexes_p = db_sub.add_parser("create-indexes", help="Interactively create database indexes")
    db_indexes_p.add_argument("--source", "-s", dest="source",
                               help="Source name (auto-selects if only one configured)")
    db_indexes_p.set_defaults(func=cmd_db_create_indexes)

    db_targets_p = db_sub.add_parser("index-targets", help="List the tables, columns and existing indexes create-indexes offers for one database")
    db_targets_p.add_argument("--source", "-s", dest="source", required=True, help="Source name")
    db_targets_p.add_argument("--db", dest="db", required=True, choices=["postgres", "mongo", "starrocks"], help="Database")
    db_targets_p.set_defaults(func=cmd_db_index_targets)

    # ---- sdp source ----
    source_parser = subparsers.add_parser("source", help="Source management")
    source_sub = source_parser.add_subparsers(dest="source_command", help="Source command")

    src_add_p = source_sub.add_parser("add", help="Add a new source")
    src_add_p.add_argument("name", help="Source name (e.g. reddit, twitter_academic)")
    src_add_p.add_argument("--hf", dest="hf_dataset", default=None,
                           help="Hugging Face dataset ID (e.g. user/dataset-name)")
    src_add_p.set_defaults(func=cmd_source_add)

    src_hf_inspect_p = source_sub.add_parser("hf-inspect", help="Show a Hugging Face dataset's schema groups, as source add --hf offers them")
    src_hf_inspect_p.add_argument("dataset", help="Hugging Face dataset ID (e.g. user/dataset-name)")
    src_hf_inspect_p.add_argument("--token", help="HF token for private datasets (fallback: HF_TOKEN env var)")
    src_hf_inspect_p.set_defaults(func=cmd_source_hf_inspect)

    src_download_p = source_sub.add_parser("download", help="Download HF dataset files")
    src_download_p.add_argument("name", help="Source name")
    src_download_p.add_argument("--token", help="HF token for private datasets (fallback: HF_TOKEN env var)")
    src_download_p.add_argument("--data-type", help="Download only this data type")
    src_download_p.set_defaults(func=cmd_source_download)

    src_configure_p = source_sub.add_parser("configure", help="Reconfigure existing source")
    src_configure_p.add_argument("name", help="Source name")
    src_configure_p.set_defaults(func=cmd_source_configure)

    src_classifiers_p = source_sub.add_parser("add-classifiers", help="Add ML classifiers for a source")
    src_classifiers_p.add_argument("name", help="Source name")
    src_classifiers_p.set_defaults(func=cmd_source_add_classifiers)

    src_settings_p = source_sub.add_parser("settings", help="Show the current answers of source configure (never a secret)")
    src_settings_p.add_argument("name", help="Source name")
    src_settings_p.add_argument("--json", action="store_true", help="Print the data as JSON instead of the text")
    src_settings_p.set_defaults(func=cmd_source_settings)

    src_remove_p = source_sub.add_parser("remove", help="Remove source configuration")
    src_remove_p.add_argument("name", help="Source name")
    src_remove_p.set_defaults(func=cmd_source_remove)

    src_list_p = source_sub.add_parser("list", help="List configured sources")
    src_list_p.set_defaults(func=cmd_source_list)

    src_status_p = source_sub.add_parser("status", help="Show source status")
    src_status_p.add_argument("name", nargs="?", help="Source name (default: all sources)")
    src_status_p.set_defaults(func=cmd_source_status)

    src_progress_p = source_sub.add_parser("progress", help="Show how many files each processing stage has finished")
    src_progress_p.add_argument("name", nargs="?", help="Source name (default: all sources)")
    src_progress_p.add_argument("--json", action="store_true", help="Print the data as JSON instead of the text")
    src_progress_p.set_defaults(func=cmd_source_progress)

    src_size_p = source_sub.add_parser("size", help="Show how much disk space each source's dumps, extracted, parsed and output folders take")
    src_size_p.add_argument("name", nargs="?", help="Source name (default: all sources)")
    src_size_p.add_argument("--json", action="store_true", help="Print the data as JSON instead of the text")
    src_size_p.set_defaults(func=cmd_source_size)

    src_errors_p = source_sub.add_parser("error-logs", help="Show database ingestion error logs")
    src_errors_p.add_argument("name", nargs="?", help="Source name (default: all sources)")
    src_errors_p.add_argument("--profile", "-p",
        choices=["postgres_ingest", "postgres_ml", "mongo_ingest"],
        help="Filter by ingestion profile")
    src_errors_p.set_defaults(func=cmd_source_error_logs)

    # ---- sdp run ----
    run_parser = subparsers.add_parser("run", help="Run a pipeline profile")
    run_parser.add_argument("profile", choices=VALID_PROFILES, help="Pipeline profile to run")
    run_parser.add_argument("--source", "-s", dest="source",
                            help="Source name (auto-selects if only one configured)")
    run_parser.add_argument("--build", action="store_true", help="Rebuild Docker image")
    run_parser.add_argument("--filter", "-f", dest="filter",
                            help="Filter files by pattern (fnmatch glob on file ID, e.g. '*2024*', 'RS_2024-*')")
    run_parser.add_argument("--skip-lingua-files", action="store_true",
                            dest="skip_lingua_files",
                            help="Parse only: also skip files that already have a lingua output "
                                 "(<OUTPUT_PATH>/lingua/<data_type>/<id>_lingua.{csv,parquet}). "
                                 "Lets you delete old extracted/parsed files after lingua has consumed them.")
    run_parser.add_argument("--classifier", "-c", dest="classifier",
                            help="ml/lingua/sr_ml only: run a single classifier from the configured list "
                                 "(e.g. 'toxic_roberta') instead of all of them.")
    run_parser.set_defaults(func=cmd_run)

    # ---- sdp runs ----
    runs_parser = subparsers.add_parser("runs", help="Pipeline runs: list them, print their output, cancel one")
    runs_sub = runs_parser.add_subparsers(dest="runs_command", help="Runs command")

    runs_list_p = runs_sub.add_parser("list", help="List the pipeline runs that are alive now, and the recorded ones")
    runs_list_p.add_argument("--limit", type=int, default=50, metavar="N", help="List at most N recorded runs, newest first (1 to 1000, default: 50)")
    runs_list_p.add_argument("--json", action="store_true", help="Print the data as JSON instead of the text")
    runs_list_p.set_defaults(func=cmd_runs_list)

    runs_logs_p = runs_sub.add_parser("logs", help="Print the output of a run")
    runs_logs_p.add_argument("run_id", help="Run id, as `runs list` shows it")
    runs_logs_p.add_argument("--offset", type=int, default=0, metavar="BYTES", help="Recorded run: start at this byte of the log (default: 0)")
    runs_logs_p.add_argument("--limit", type=int, default=None, metavar="BYTES", help="Print at most this many bytes: of a recorded run's log from the offset on (default: to its end), of the output Docker holds its end (default: 65536)")
    runs_logs_p.add_argument("--tail", type=int, default=None, metavar="LINES", help="Print only the last LINES lines (the output of a run started in a terminal is at most its last 500 lines)")
    runs_logs_p.add_argument("--follow", action="store_true", help="Keep printing new output until the run ends (Ctrl-C to stop)")
    runs_logs_p.set_defaults(func=cmd_runs_logs)

    runs_cancel_p = runs_sub.add_parser("cancel", help="Cancel a run by sending its container the interrupt signal")
    runs_cancel_p.add_argument("run_id", help="Run id, as `runs list` shows it")
    runs_cancel_p.set_defaults(func=cmd_runs_cancel)

    # ---- sdp broker ----
    broker_parser = subparsers.add_parser("broker", help="The broker: the host process that serves the web UI's socket")
    broker_sub = broker_parser.add_subparsers(dest="broker_command", help="Broker command")

    broker_start_p = broker_sub.add_parser("start", help="Start the broker")
    broker_start_p.add_argument("--foreground", action="store_true", help="Run the broker in this process until Ctrl-C, instead of in the background")
    broker_start_p.set_defaults(func=cmd_broker_start)

    broker_stop_p = broker_sub.add_parser("stop", help="Stop the broker; it lets the action that runs finish first")
    broker_stop_p.add_argument("--force", action="store_true", help="End the action that runs instead of waiting for it")
    broker_stop_p.set_defaults(func=cmd_broker_stop)

    broker_status_p = broker_sub.add_parser("status", help="Say whether the broker runs (exit code 0 when it does, 1 when not)")
    broker_status_p.set_defaults(func=cmd_broker_status)

    broker_passwd_p = broker_sub.add_parser("passwd", help="Set the management password, with which a session logs in")
    broker_passwd_p.set_defaults(func=cmd_broker_passwd)

    broker_audit_p = broker_sub.add_parser("audit", help="Print the newest lines of the audit log, or check its chain")
    broker_audit_p.add_argument("--limit", type=int, default=50, metavar="N", help="Print the newest N lines (1 to 10000, default: 50)")
    broker_audit_p.add_argument("--verify", action="store_true", help="Check the chain of the whole log instead (exit code 1 on a break or a log that cannot be read)")
    broker_audit_p.add_argument("--json", action="store_true", help="Print the data as JSON instead of the text")
    broker_audit_p.set_defaults(func=cmd_broker_audit)

    # ---- sdp webui ----
    webui_parser = subparsers.add_parser("webui", help="The web UI: a page in the browser that works through the broker")
    webui_sub = webui_parser.add_subparsers(dest="webui_command", help="Web UI command")

    webui_setup_p = webui_sub.add_parser("setup", help="Set up the web UI: port, listen addresses, names, certificate; build its image")
    webui_setup_p.set_defaults(func=cmd_webui_setup)

    webui_start_p = webui_sub.add_parser("start", help="Start the web UI's container")
    webui_start_p.set_defaults(func=cmd_webui_start)

    webui_stop_p = webui_sub.add_parser("stop", help="Stop and remove the web UI's container (exit code 1 when it does not run)")
    webui_stop_p.set_defaults(func=cmd_webui_stop)

    webui_rebuild_p = webui_sub.add_parser("rebuild", help="Rebuild the web UI's image from the saved settings and restart it (no questions)")
    webui_rebuild_p.set_defaults(func=cmd_webui_rebuild)

    webui_status_p = webui_sub.add_parser("status", help="Say whether the web UI runs, where it answers and its certificate (exit code 0 when it runs, 1 when not)")
    webui_status_p.set_defaults(func=cmd_webui_status)

    webui_unsetup_p = webui_sub.add_parser("unsetup", help="Remove the web UI's container, network, image and settings")
    webui_unsetup_p.set_defaults(func=cmd_webui_unsetup)

    # ---- sdp chat ----
    chat_parser = subparsers.add_parser("chat", help="The Chat window: an AI assistant in a sandbox whose only network peer is a gateway that holds the model's credential")
    chat_sub = chat_parser.add_subparsers(dest="chat_command", help="Chat command")

    chat_setup_p = chat_sub.add_parser("setup", help="Set up the chat: back end, the model's credential (asked without echo), the work folder's cap; build its two images")
    chat_setup_p.set_defaults(func=cmd_chat_setup)

    chat_start_p = chat_sub.add_parser("start", help="Start the chat's gateway and sandbox and attach the databases whose MCP server is on a read-only user")
    chat_start_p.set_defaults(func=cmd_chat_start)

    chat_stop_p = chat_sub.add_parser("stop", help="Stop and remove the chat's containers and networks, and detach the databases (exit code 1 when it does not run)")
    chat_stop_p.set_defaults(func=cmd_chat_stop)

    chat_rebuild_p = chat_sub.add_parser("rebuild", help="Rebuild the chat's images from the saved settings and restart it (no questions)")
    chat_rebuild_p.set_defaults(func=cmd_chat_rebuild)

    chat_status_p = chat_sub.add_parser("status", help="Say whether the chat runs, which databases it can read now and the size of its work folder (exit code 0 when it runs, 1 when not)")
    chat_status_p.set_defaults(func=cmd_chat_status)

    chat_settings_p = chat_sub.add_parser("settings", help="Show the chat's settings for the whole deployment: the read-only query limit of its MongoDB and StarRocks MCP servers")
    chat_settings_p.set_defaults(func=cmd_chat_settings)

    chat_query_limit_p = chat_sub.add_parser("query-limit", help="Show or set the read-only query limit of the chat's MongoDB and StarRocks MCP servers (whole seconds, 1 to 3600; 90 when not set); setting it restarts those two servers when the chat runs, not the chat itself")
    chat_query_limit_p.add_argument("seconds", nargs="?", default=None, help="The new limit in seconds; without it, the limit in force is shown")
    chat_query_limit_p.set_defaults(func=cmd_chat_query_limit)

    chat_auto_title_p = chat_sub.add_parser("auto-title", help="Show or set whether a new chat gets a short title made from its first question by the chat's model (on when not set); a change takes effect at the chat's next start, nothing is restarted")
    chat_auto_title_p.add_argument("value", nargs="?", default=None, choices=("on", "off"), help="on or off; without it, the setting in force is shown")
    chat_auto_title_p.set_defaults(func=cmd_chat_auto_title)

    chat_unsetup_p = chat_sub.add_parser("unsetup", help="Remove the chat's images, settings and credential (and, if asked, all chats)")
    chat_unsetup_p.set_defaults(func=cmd_chat_unsetup)

    return parser


# ============================================================================
# Main
# ============================================================================

def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.tag:
        os.environ['SDP_TAGGED_MODE'] = '1'

    if not args.command:
        parser.print_help()
        return 0

    # Handle subcommand groups that need their own help
    if args.command == "db" and not getattr(args, "db_command", None):
        parser.parse_args(["db", "-h"])
        return 0
    if args.command == "source" and not getattr(args, "source_command", None):
        parser.parse_args(["source", "-h"])
        return 0
    if args.command == "runs" and not getattr(args, "runs_command", None):
        parser.parse_args(["runs", "-h"])
        return 0
    if args.command == "broker" and not getattr(args, "broker_command", None):
        parser.parse_args(["broker", "-h"])
        return 0
    if args.command == "webui" and not getattr(args, "webui_command", None):
        parser.parse_args(["webui", "-h"])
        return 0
    if args.command == "chat" and not getattr(args, "chat_command", None):
        parser.parse_args(["chat", "-h"])
        return 0

    if hasattr(args, "func"):
        return args.func(args)

    parser.print_help()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main() or 0)
    except KeyboardInterrupt:
        print("\n\n  Aborted.\n")
        sys.exit(1)
