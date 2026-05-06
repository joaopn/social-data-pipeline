#!/usr/bin/env python3
"""Social Data Pipeline CLI.

Unified entrypoint for database management, source configuration, and pipeline execution.

Usage:
    sdp.py db setup                          Configure databases (PostgreSQL, MongoDB)
    sdp.py db setup --add <db>               Add a single database to existing setup
    sdp.py db setup-mcp                      Configure MCP servers for databases
    sdp.py db start [service]                Start services (postgres|mongo|starrocks|postgres-mcp|mongo-mcp|starrocks-mcp|all)
    sdp.py db stop [service]                 Stop services (postgres|mongo|starrocks|postgres-mcp|mongo-mcp|starrocks-mcp|all)
    sdp.py db status                         Show database config and health
    sdp.py db unsetup                        Remove database config (and optionally data)
    sdp.py db unsetup-mcp                    Remove MCP configuration
    sdp.py db recover-password               Reset database admin password
    sdp.py db create-indexes [--source <name>] Interactively create database indexes

    sdp.py source add <name> [--hf ID]        Add a new source (--hf for HF datasets)
    sdp.py source download <name>            Download HF dataset files
    sdp.py source configure <name>           Reconfigure existing source (platform-specific)
    sdp.py source add-classifiers <name>     Add ML classifiers for a source
    sdp.py source remove <name>              Remove source config
    sdp.py source list                       List configured sources
    sdp.py source status [name]              Show source processing/ingestion status
    sdp.py source error-logs [name]          Show database ingestion error logs

    sdp.py run <profile> [--source <name>]   Run pipeline for a source (--build to rebuild, --filter to select files)

    sdp.py setup                             Legacy: full interactive setup (core + classifiers + platform)
"""

import argparse
import json
import os
import secrets
import shutil
import stat
import subprocess
import sys
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


def cmd_db_unsetup_jobs(args):
    """Remove jobs scheduler configuration and stop its container."""
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

def _collect_source_info():
    """Snapshot all configured sources for mount-coherence helpers.

    Returns a list of dicts ``{name, profiles, paths}`` — the input shape
    consumed by ``setup.mount_sync.expected_source_mounts`` and
    ``compute_mount_drift``.
    """
    from social_data_pipeline.setup.utils import (
        list_sources, load_source_config, get_source_profiles,
    )
    out = []
    for source in list_sources():
        cfg = load_source_config(source) or {}
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


def _exited_services_after_up(profile_args):
    """Return list of service names in `Exited` state after a `compose up -d`.

    Belt-and-suspenders for the J2 failure shape ("up -d returns 0 on a
    crashed container"). `up -d --wait` should already catch it, but
    docker compose has been racy historically — this ps probe runs
    immediately after and catches anything `--wait` missed.

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
        state = (row.get("State") or "").lower()
        if state != "exited":
            continue
        name = row.get("Service") or row.get("Name") or "<unknown>"
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
    configured = _get_configured_db_services()
    if not configured:
        print("  No databases configured. Run: python sdp.py db setup")
        return 1

    service = args.service
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
            password = _prompt_db_password(tag="sdp_db_password")
            _set_auth_env(password)
        # Start only the MCP profile (init container runs first via depends_on).
        # `--wait` blocks until services are running and (where defined) healthy;
        # the post-launch ps probe surfaces any race-condition exits that
        # `--wait` may have missed.
        profile_flags = ["--profile", parent_db, "--profile", mcp_profile]
        mcp_args = profile_flags + ["up", "-d", "--wait"]
        result = docker_compose(*mcp_args)
        exited = _exited_services_after_up(profile_flags)
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
            password = _prompt_db_password(tag="sdp_db_password")
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
        password = _prompt_db_password(tag="sdp_db_password")
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

    # Start databases first (without MCP), wait for healthchecks
    db_args = []
    for svc in targets:
        db_args += ["--profile", svc]
    db_args += ["up", "-d", "--wait"]
    result = docker_compose(*db_args)
    if result.returncode != 0:
        return result.returncode

    # Start MCP servers (init containers handle user creation via depends_on).
    # `--wait` blocks until MCPs are running; the post-launch ps probe surfaces
    # any race-condition exits that `--wait` may have missed (J2 failure
    # shape: `up -d` returns 0 on a crashed container).
    if mcp_targets:
        profile_flags = []
        for svc in targets:
            profile_flags += ["--profile", svc]
        for mp in mcp_targets:
            profile_flags += ["--profile", mp]
        mcp_args = profile_flags + ["up", "-d", "--wait"]
        result = docker_compose(*mcp_args)
        exited = _exited_services_after_up(profile_flags)
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
    configured = _get_configured_db_services()
    if not configured:
        print("  No databases configured. Run: python sdp.py db setup")
        return 1

    service = args.service
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
    env = load_env()
    configured = _get_configured_db_services()

    print()
    print("  Social Data Pipeline - Database Status")
    print("  =====================================")

    if not configured:
        print("\n  No databases configured. Run: python sdp.py db setup\n")
        return 0

    # Authentication status
    auth_enabled = (env.get("POSTGRES_AUTH_ENABLED") == "true"
                    or env.get("MONGO_AUTH_ENABLED") == "true"
                    or env.get("STARROCKS_AUTH_ENABLED") == "true")
    print(f"\n  Authentication: {'enabled' if auth_enabled else 'disabled'}")
    if auth_enabled:
        pg_user = env.get("POSTGRES_USER", "postgres") if "postgres" in configured else None
        mongo_user = env.get("MONGO_ADMIN_USER") if "mongo" in configured else None
        sr_auth = "starrocks" in configured and env.get("STARROCKS_AUTH_ENABLED") == "true"
        if pg_user or mongo_user or sr_auth:
            rw_parts = []
            if pg_user:
                rw_parts.append(f"postgres: {pg_user}")
            if mongo_user:
                rw_parts.append(f"mongo: {mongo_user}")
            if sr_auth:
                rw_parts.append("starrocks: root")
            print(f"    RW user:       {', '.join(rw_parts)}")
        ro_user = env.get("POSTGRES_RO_USER") or env.get("MONGO_RO_USER") or env.get("STARROCKS_RO_USER")
        if ro_user:
            # Check if RO credentials file exists in any data path
            ro_cred_path = None
            for path_key, default in [("PGDATA_PATH", "./data/database/postgres"),
                                      ("MONGO_DATA_PATH", "./data/database/mongo"),
                                      ("STARROCKS_DATA_PATH", "./data/database/starrocks")]:
                dp = Path(env.get(path_key, default))
                if not dp.is_absolute():
                    dp = ROOT / dp
                cred_file = dp / ".ro_credentials"
                if cred_file.exists():
                    ro_cred_path = cred_file
                    break
            if ro_cred_path:
                print(f"    RO user:       {ro_user} (password in {ro_cred_path})")
            else:
                print(f"    RO user:       {ro_user} (no password)")
        mcp_user = env.get("POSTGRES_MCP_USER") or env.get("MONGO_MCP_USER") or env.get("STARROCKS_MCP_USER")
        if mcp_user:
            print(f"    MCP user:      {mcp_user}")

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

    # PostgreSQL
    if "postgres" in configured:
        pgdata_path = env.get("PGDATA_PATH", "./data/database/postgres")
        pgdata = Path(pgdata_path)
        if not pgdata.is_absolute():
            pgdata = ROOT / pgdata

        print("\n  PostgreSQL:")
        print(f"    Path:      {pgdata_path}")
        print(f"    Name:      {env.get('DB_NAME', 'datasets')}")
        print(f"    Port:      {env.get('POSTGRES_PORT', '5432')}")
        print(f"    Running:   {'yes' if 'postgres' in running_services else 'no'}")

        state_dir = pgdata / "state_tracking"
        _print_ingestion_state(state_dir, "_postgres_", "postgres")

    # MongoDB
    if "mongo" in configured:
        mongo_data_path = env.get("MONGO_DATA_PATH", "./data/database/mongo")
        mongo_data = Path(mongo_data_path)
        if not mongo_data.is_absolute():
            mongo_data = ROOT / mongo_data

        print("\n  MongoDB:")
        print(f"    Path:      {mongo_data_path}")
        print(f"    Port:      {env.get('MONGO_PORT', '27017')}")
        print(f"    Running:   {'yes' if 'mongo' in running_services else 'no'}")

        state_dir = mongo_data / "state_tracking"
        _print_ingestion_state(state_dir, "_mongo_", "mongo")

    # StarRocks
    if "starrocks" in configured:
        sr_data_path = env.get("STARROCKS_DATA_PATH", "./data/database/starrocks")
        sr_data = Path(sr_data_path)
        if not sr_data.is_absolute():
            sr_data = ROOT / sr_data

        print("\n  StarRocks:")
        print(f"    Path:      {sr_data_path}")
        print(f"    Port:      {env.get('STARROCKS_PORT', '9030')}")
        print(f"    FE HTTP:   {env.get('STARROCKS_FE_HTTP_PORT', '8030')}")
        print(f"    Running:   {'yes' if 'starrocks' in running_services else 'no'}")

    # Export path
    export_path = env.get("DB_EXPORT_PATH")
    if export_path:
        print("\n  Export:")
        print(f"    Host path:      {export_path}")
        print("    Container path: /export")

    # MCP servers
    mcp_config = _load_mcp_config()
    if mcp_config:
        pg_mcp = mcp_config.get("postgres", {})
        if pg_mcp.get("enabled"):
            port = pg_mcp.get("port", 8000)
            access = pg_mcp.get("access_mode", "restricted")
            running = "yes" if "postgres-mcp" in running_services else "no"
            print("\n  PostgreSQL MCP:")
            print(f"    Port:      {port}")
            print(f"    Access:    {access}")
            print(f"    Endpoint:  http://<server_ip>:{port}/sse (type: sse)")
            print(f"    Running:   {running}")

        mongo_mcp = mcp_config.get("mongo", {})
        if mongo_mcp.get("enabled"):
            port = mongo_mcp.get("port", 3000)
            read_only = mongo_mcp.get("read_only", True)
            running = "yes" if "mongo-mcp" in running_services else "no"
            print("\n  MongoDB MCP:")
            print(f"    Port:      {port}")
            print(f"    Read-only: {read_only}")
            print(f"    Endpoint:  http://<server_ip>:{port}/mcp (type: http)")
            print(f"    Running:   {running}")

        sr_mcp = mcp_config.get("starrocks", {})
        if sr_mcp.get("enabled"):
            port = sr_mcp.get("port", 9000)
            running = "yes" if "starrocks-mcp" in running_services else "no"
            print("\n  StarRocks MCP:")
            print(f"    Port:      {port}")
            print(f"    Endpoint:  http://<server_ip>:{port}/mcp (type: http)")
            print(f"    Running:   {running}")

    # Jobs scheduler
    if _is_jobs_configured():
        jobs_cfg = _load_jobs_config()
        port = jobs_cfg.get("port", 8050)
        running = "yes" if "jobs" in running_services else "no"
        targets = jobs_cfg.get("targets") or {}
        print("\n  Jobs scheduler:")
        print(f"    Port:         {port}")
        print(f"    Result root:  {jobs_cfg.get('result_root', './data/jobs/results')}")
        print(f"    Max concur.:  {jobs_cfg.get('max_concurrent', 1)}")
        print(f"    Web UI:       http://<server_ip>:{port}/")
        print(f"    MCP:          http://<server_ip>:{port}/mcp (type: http)")
        print(f"    Running:      {running}")
        if targets:
            names = ", ".join(
                f"{n} ({spec.get('backend')})" for n, spec in targets.items()
            )
            print(f"    Targets:      {names}")

        # Queue depth summary from the filesystem store
        jobs_dir = ROOT / "data" / "jobs"
        pending_n = len(list((jobs_dir / "pending").glob("*.json"))) if (jobs_dir / "pending").exists() else 0
        approved_n = len(list((jobs_dir / "approved").glob("*.json"))) if (jobs_dir / "approved").exists() else 0
        running_n = len(list((jobs_dir / "running").glob("*.json"))) if (jobs_dir / "running").exists() else 0
        print(f"    Queue:        pending={pending_n} approved={approved_n} running={running_n}")

    # Drift section (local-files only — running-container probes are reserved
    # for `sdp db verify`, which has the matching exit-code contract).
    drift_findings = _drift_findings(probe_containers=False)
    if drift_findings and not _is_drift_clean(drift_findings):
        print()
        _print_drift_findings(drift_findings, header="Drift")
        print("  Run 'sdp db verify' for a per-DB exit-code-aware report.")

    print()
    return 0


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


def _build_verify_context(*, env, probe_containers):
    """Assemble the ctx dict that ``setup.verify.compute_drift`` consumes."""
    configured = _get_configured_db_services()
    db_yamls = {db: _load_db_yaml(db) for db in configured}
    cred_states = {db: _resolve_cred_state(db, env) for db in configured}
    sources_info = _collect_source_info()
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


def _drift_findings(*, probe_containers):
    """Wrapper that loads .env + builds the ctx + runs compute_drift."""
    from social_data_pipeline.setup.verify import compute_drift
    env = load_env()
    ctx = _build_verify_context(env=env, probe_containers=probe_containers)
    return compute_drift(ctx)


def _is_drift_clean(findings_by_name):
    return all(not findings for findings in findings_by_name.values())


def _print_drift_findings(findings_by_name, header="Drift"):
    """Render findings in `db status` / `db verify` text mode.

    `findings_by_name` is the dict from compute_drift. Names are db names
    (`postgres`, `mongo`, `starrocks`) plus optional cross-cutting
    `mcp` / `jobs` keys; rendering preserves insertion order.
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
            print(f"      [{f.category}] {f.message}")
            print(f"        Fix: {f.fix}")
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
    from social_data_pipeline.setup.verify import compute_drift

    env = load_env()
    configured = _get_configured_db_services()
    if not configured:
        msg = "No databases configured. Run: python sdp.py db setup"
        if getattr(args, "json", False):
            print(json.dumps({"ok": True, "exit_code": 0, "message": msg, "results": {}}))
        else:
            print(f"\n  {msg}\n")
        return 0

    ctx = _build_verify_context(env=env, probe_containers=True)
    findings_by_name = compute_drift(ctx)

    db_filter = getattr(args, "db", None)
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

    if getattr(args, "json", False):
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
        print(json.dumps(payload, indent=2))
        return exit_code

    print()
    print("  Social Data Pipeline - Database Verify")
    print("  =====================================")
    print()
    _print_drift_findings(findings_by_name, header="Findings")
    print()
    if clean:
        print("  OK")
    else:
        n_drift = sum(1 for v in findings_by_name.values() if v)
        n_total = len(findings_by_name)
        print(f"  Exit: 1 (drift in {n_drift} of {n_total} check group(s))")
    print()
    return exit_code


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


def _print_ingestion_state(state_dir, label_split, db_type):
    """Print ingestion state from JSON files in a state directory."""
    from social_data_pipeline.setup.utils import load_source_config

    if not state_dir.exists():
        print("\n    No ingestion data yet.")
        return

    state_files = sorted(state_dir.glob("*.json"))
    if not state_files:
        print("\n    No ingestion data yet.")
        return

    # Cache platform configs per source
    platform_cache = {}
    classifier_suffixes = None  # lazy-loaded

    # Collect entries grouped by source
    source_entries = {}  # {source: [(label, count, latest, in_progress, failed, last_updated), ...]}

    for sf in state_files:
        try:
            sdata = json.loads(sf.read_text())
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

        # Load platform config for this source
        if source not in platform_cache:
            platform_cache[source] = load_source_config(source) or {}
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
                latest = datasets[-1] if datasets else "-"
                cls_failed = failed_by_classifier.get(cls_name, [])
                tag = f"classifier: {cls_name}"
                source_entries[source].append(
                    (tag, table, count, latest, in_progress, cls_failed, last_updated)
                )
        else:
            # Main data (ingest profile)
            if db_type == "postgres":
                schema = pconfig.get("db_schema", source)
                table = f"{schema}.{data_type}"
            else:
                if "mongo_db_name" in pconfig:
                    table = pconfig["mongo_db_name"]
                else:
                    template = pconfig.get("mongo_db_name_template", "{platform}_{data_type}")
                    platform = pconfig.get("platform", source).replace("/", "_")
                    table = template.format(platform=platform, data_type=data_type)

            count = len(processed)
            latest = processed[-1] if processed else "-"
            tag = "main data" if db_type == "postgres" else ""
            source_entries[source].append(
                (tag, table, count, latest, in_progress, failed, last_updated)
            )

    # Print grouped by source
    print("\n    Ingestion status:")
    for source, entries in sorted(source_entries.items()):
        print(f"      platform: {source}")
        for tag, table, count, latest, in_progress, failed, last_updated in entries:
            line = f"        - {table}: {count} datasets"
            if tag:
                line += f" ({tag})"
            if in_progress:
                line += f" [in progress: {in_progress}]"
            if failed:
                line += f" [{len(failed)} failed]"
            print(line)
            details = []
            if latest != "-":
                details.append(f"latest: {latest}")
            if last_updated:
                details.append(f"updated: {last_updated[:10]}")
            if details:
                print(f"            {', '.join(details)}")


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


def _unsetup_single_db(db_name, env):
    """Remove a single database entirely: containers, config, data, and shared-file entries.

    Containers, config, and data are always removed together — this is not a reconfigure path.
    Use 'sdp db setup --add <db>' to reconfigure a DB in place without losing data.
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
    chown_image = {
        "postgres": "postgres:18",
        "mongo": "mongo:8",
        "starrocks": "starrocks/allin1-ubuntu",
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

    # Read jobs targets that point at the DB being removed (C4 — surface the
    # orphaned target so the operator knows the jobs scheduler will start
    # failing routes after this command lands).
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
    confirm1 = input(
        f"  Delete {db_label} entirely? Config, containers, and data will be removed. "
        f"This CANNOT be undone [y/N]: "
    ).strip().lower()
    if confirm1 not in ("y", "yes"):
        print("\n  Aborted — nothing was changed.\n")
        return 0
    confirm2 = input(
        f"  Are you SURE? All {db_label} data will be permanently lost [y/N]: "
    ).strip().lower()
    if confirm2 not in ("y", "yes"):
        print("\n  Aborted — nothing was changed.\n")
        return 0
    print()

    # --- Stop containers ---
    print(f"  Stopping {db_label} containers...")
    docker_compose("--profile", db_name, "--profile", f"{db_name}_mcp",
                   "down", "--timeout", "30")
    print()

    # --- Delete data directory (+ tablespaces for PG) ---
    if data_path and data_path.exists():
        print("  Fixing directory permissions...")
        parent = data_path.resolve().parent
        volume_args = ["-v", f"{parent}:/dbparent"]
        # Recursive: make everything user-owned so shutil.rmtree can clean contents.
        recursive = ["/dbparent"]
        # Non-recursive: make tablespace PARENTS user-owned so we can unlink the
        # tablespace directory itself. Can't be recursive because a parent may
        # contain sibling DB dirs (e.g. a starrocks dir) we must not touch.
        non_recursive = []
        ts_mounts = []  # for the rmtree pass (PG tablespaces and SR extra storage)
        if db_name == "postgres":
            for ts_name, ts_path in tablespace_paths.items():
                ts_dir = Path(ts_path)
                if not ts_dir.is_absolute():
                    ts_dir = ROOT / ts_dir
                if ts_dir.exists():
                    volume_args += ["-v", f"{ts_dir.resolve()}:/tablespace/{ts_name}"]
                    recursive.append(f"/tablespace/{ts_name}")
                    ts_parent = ts_dir.resolve().parent
                    ts_parent_mount = f"/tsparent_{ts_name}"
                    volume_args += ["-v", f"{ts_parent}:{ts_parent_mount}"]
                    non_recursive.append(ts_parent_mount)
                    ts_mounts.append((ts_name, ts_dir))
        if db_name == "starrocks":
            # SR extra disks: same chown / rmtree shape as PG tablespaces;
            # mount under /tablespace/ to reuse the existing chown command.
            for sp_name, sp_path in sr_storage_paths.items():
                sp_dir = Path(sp_path)
                if not sp_dir.is_absolute():
                    sp_dir = ROOT / sp_dir
                if sp_dir.exists():
                    volume_args += ["-v", f"{sp_dir.resolve()}:/tablespace/{sp_name}"]
                    recursive.append(f"/tablespace/{sp_name}")
                    sp_parent = sp_dir.resolve().parent
                    sp_parent_mount = f"/tsparent_{sp_name}"
                    volume_args += ["-v", f"{sp_parent}:{sp_parent_mount}"]
                    non_recursive.append(sp_parent_mount)
                    ts_mounts.append((sp_name, sp_dir))
        uid_gid = f"{os.getuid()}:{os.getgid()}"
        chown_cmd = f"chown -R {uid_gid} {' '.join(recursive)}"
        if non_recursive:
            chown_cmd += f" && chown {uid_gid} {' '.join(non_recursive)}"
        subprocess.run(
            ["docker", "run", "--rm"] + volume_args +
            [chown_image, "sh", "-c", chown_cmd],
            cwd=ROOT,
        )
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


def cmd_db_unsetup(args):
    """Remove database configuration and optionally database data."""
    env = load_env()

    if getattr(args, "db", None):
        return _unsetup_single_db(args.db, env)

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
            docker_compose("--profile", "postgres", "restart")

            # Wait for healthy
            print("  Waiting for PostgreSQL to be ready...")
            port = env.get("POSTGRES_PORT", "5432")
            db_name = env.get("DB_NAME", "datasets")
            subprocess.run(
                ["docker", "compose", "exec", "postgres",
                 "pg_isready", "-U", "postgres", "-d", db_name, "-p", port, "--timeout=30"],
                cwd=ROOT,
            )

            # Set new password (use PGPASSWORD env to avoid shell interpolation of password)
            escaped_pw = new_password.replace("'", "''")
            alter_cmd = f"ALTER USER postgres WITH PASSWORD '{escaped_pw}'"
            result = subprocess.run(
                ["docker", "compose", "exec", "postgres",
                 "psql", "-U", "postgres", "-p", port, "-c", alter_cmd],
                cwd=ROOT,
            )
            if result.returncode != 0:
                print("  Error: Failed to set new password.")
                # Restore backup
                shutil.copy2(pg_hba_backup, pg_hba_local)
                pg_hba_backup.unlink(missing_ok=True)
                docker_compose("--profile", "postgres", "restart")
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
            result = subprocess.run(
                ["docker", "run", "--rm", "-d",
                 "--name", "sdp-mongo-recovery",
                 "-v", f"{mongo_data_path}/db:/data/db",
                 "mongo:8",
                 "mongod", "--bind_ip", "0.0.0.0",
                 "--wiredTigerCacheSizeGB", mongo_cache],
                cwd=ROOT, capture_output=True, text=True,
            )
            if result.returncode != 0:
                print(f"  Error: Failed to start recovery container: {result.stderr}")
                return 1

            # Wait for mongod to be ready
            import time
            for _ in range(15):
                check = subprocess.run(
                    ["docker", "exec", "sdp-mongo-recovery",
                     "mongosh", "--quiet", "--eval", "db.adminCommand('ping')"],
                    capture_output=True, text=True,
                )
                if check.returncode == 0:
                    break
                time.sleep(2)

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

            # Stop recovery container
            subprocess.run(["docker", "stop", "sdp-mongo-recovery"],
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


def _interactive_sr_indexes(source, platform_config, password):
    """Interactive StarRocks BITMAP index creation. Returns {table: [new_fields]} for config persistence."""
    from social_data_pipeline.setup.utils import ask_multi_select, ask_list, section_header
    import time

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
    for table in selected_tables:
        print(f"\n  --- {table} ---")

        try:
            rows = _sr_query(port, database,
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_schema = '{database}' AND table_name = '{table}' "
                f"ORDER BY ordinal_position",
                password)
            columns = [r[0] for r in rows]
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

        fields = ask_list("New BITMAP indexes (comma-separated field names, empty to skip)", default=[])
        if fields:
            plan[table] = fields

    if not plan:
        return {}

    # --- Execute phase: parallel across tables; serial within each table ---
    import threading
    from concurrent.futures import ThreadPoolExecutor, as_completed

    print_lock = threading.Lock()

    def emit(msg):
        with print_lock:
            print(msg, flush=True)

    def _build_for_table(table, fields):
        prefix = f"[{table}] "
        table_created = []
        for field in fields:
            index_name = f"idx_{table}_{field}"
            emit(f"  {prefix}Building {index_name}")
            t_start = time.time()

            _sr_wait_for_active_alter(port, database, table, password,
                                      line_prefix=prefix, print_fn=emit)

            statement = f"CREATE INDEX `{index_name}` ON `{database}`.`{table}` (`{field}`) USING BITMAP"
            success, stderr = _sr_exec(port, database, statement, password)
            if not success:
                emit(f"    {prefix}failed to submit: {stderr}")
                continue

            rows = _sr_alter_column_rows(port, database, password)
            job_ids = [int(r[0]) for r in rows if r and r[1] == table]
            if not job_ids:
                emit(f"    {prefix}submitted but no alter job visible; skipping wait")
                table_created.append(field)
                continue
            job_id = max(job_ids)

            state, msg = _sr_poll_alter_job(port, database, job_id, password,
                                            line_prefix=prefix, print_fn=emit)
            duration = time.time() - t_start
            if state == "FINISHED":
                emit(f"    {prefix}built {index_name} ({_format_duration(duration)})")
                table_created.append(field)
            else:
                emit(f"    {prefix}alter job {job_id} ended in state {state}: {msg}")
        return table_created

    total_indexes = sum(len(v) for v in plan.values())
    print()
    print(f"  Building {total_indexes} index(es) across {len(plan)} table(s) — "
          f"tables run in parallel, fields within a table serially.")
    print()

    created = {}
    with ThreadPoolExecutor(max_workers=len(plan)) as pool:
        futures = {pool.submit(_build_for_table, t, f): t for t, f in plan.items()}
        for fut in as_completed(futures):
            table = futures[fut]
            try:
                result = fut.result()
            except Exception as e:
                emit(f"  [{table}] worker failed: {e}")
                continue
            if result:
                created[table] = result

    return created


def _interactive_pg_indexes(source, platform_config, password):
    """Interactive PostgreSQL index creation. Returns {table: [new_fields]} for config persistence."""
    from social_data_pipeline.setup.utils import ask_multi_select, ask_list, section_header
    import time
    import yaml

    section_header("PostgreSQL Index Creation")

    db_yaml = _load_db_yaml("postgres")
    port = int(db_yaml.get("port", 5432))
    db_name = db_yaml.get("name", "datasets")
    schema = platform_config.get("db_schema", source)

    # Load parallel_index_workers from source postgres config if available
    source_pg_path = CONFIG_DIR / "sources" / source / "postgres.yaml"
    parallel_workers = 8
    if source_pg_path.exists():
        try:
            source_pg = yaml.safe_load(source_pg_path.read_text()) or {}
            parallel_workers = source_pg.get("processing", {}).get("parallel_index_workers", 8)
        except Exception:
            pass

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

        table_created = []
        for field in fields:
            index_name = f"idx_{table}_{field}"
            print(f"  Creating index: {index_name} ...", end=" ", flush=True)
            t_start = time.time()

            # Set session params for parallel index builds, then create index
            statement = (
                f"SET maintenance_work_mem = '2GB'; "
                f"SET max_parallel_maintenance_workers = {parallel_workers}; "
                f"CREATE INDEX IF NOT EXISTS {index_name} ON {schema}.{table} ({field});"
            )
            success, stderr = _psql_exec(port, db_name, statement, password)
            duration = time.time() - t_start

            if success:
                print(f"done ({_format_duration(duration)})")
                table_created.append(field)
            else:
                print(f"failed: {stderr}")

        if table_created:
            created[table] = table_created

    return created


def _interactive_mongo_indexes(source, platform_config, password):
    """Interactive MongoDB index creation. Returns {data_type: [new_fields]} for config persistence."""
    from social_data_pipeline.setup.utils import ask_multi_select, ask_list, section_header
    import json
    import time

    section_header("MongoDB Index Creation")

    data_types = platform_config.get("data_types", [])
    if not data_types:
        print("\n  No data types configured.\n")
        return {}

    # Determine platform string for db name resolution
    if source == "reddit":
        platform = "reddit"
    else:
        platform = f"custom/{source}"

    # Resolve db names per data_type using the same logic as the orchestrator
    def get_db_name(dt):
        if 'mongo_db_name' in platform_config:
            return platform_config['mongo_db_name']
        template = platform_config.get('mongo_db_name_template', '{platform}_{data_type}')
        safe_platform = platform.replace('/', '_')
        return template.format(platform=safe_platform, data_type=dt)

    # Discover collections per data_type via mongosh
    all_dt_collections = {}  # {data_type: {db_name, collections}}

    for dt in data_types:
        db_name = get_db_name(dt)
        try:
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

        dt_created = []
        for field in fields:
            print(f"  Creating index {field}_1 on {n_colls} collections...")
            t_start = time.time()
            success = 0
            for coll in collections:
                try:
                    script = (
                        f"use('{db_name}'); "
                        f"db.getCollection('{coll}').createIndex("
                        f"{{'{field}': 1}}, {{name: '{field}_1'}});"
                    )
                    _mongosh_eval(0, script, password)
                    success += 1
                except Exception as e:
                    print(f"    Warning: Failed on {coll}: {e}")
            duration = time.time() - t_start
            print(f"  Done: {field}_1 ({success}/{n_colls} collections, {_format_duration(duration)})")
            dt_created.append(field)

        if dt_created:
            created[dt] = dt_created

    return created


def _persist_indexes_to_config(source, pg_created, mongo_created, sr_created=None):
    """Merge newly created indexes into platform.yaml."""
    import yaml

    platform_path = CONFIG_DIR / "sources" / source / "platform.yaml"
    config = yaml.safe_load(platform_path.read_text()) or {}

    if pg_created:
        indexes = config.setdefault("indexes", {})
        for table, fields in pg_created.items():
            existing = indexes.setdefault(table, [])
            for f in fields:
                if f not in existing:
                    existing.append(f)

    if mongo_created:
        mongo_indexes = config.setdefault("mongo_indexes", {})
        for dt, fields in mongo_created.items():
            existing = mongo_indexes.setdefault(dt, [])
            for f in fields:
                if f not in existing:
                    existing.append(f)

    if sr_created:
        sr_indexes = config.setdefault("sr_indexes", {})
        for table, fields in sr_created.items():
            existing = sr_indexes.setdefault(table, [])
            for f in fields:
                if f not in existing:
                    existing.append(f)

    platform_path.write_text(yaml.safe_dump(config, default_flow_style=False, sort_keys=False))
    print(f"  Updated: {platform_path}")


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
    source_dbs = []
    if any(p in source_profiles for p in ("postgres_ingest", "postgres_ml")):
        source_dbs.append("postgres")
    if "mongo_ingest" in source_profiles:
        source_dbs.append("mongo")
    if any(p in source_profiles for p in ("sr_ingest", "sr_ml")):
        source_dbs.append("starrocks")
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
    total = (sum(len(v) for v in pg_created.values())
             + sum(len(v) for v in mongo_created.values())
             + sum(len(v) for v in sr_created.values()))
    if total == 0:
        print("\n  No new indexes created.\n")
        return 0

    print(f"\n  Created {total} new index(es).")

    if ask_bool("Save new indexes to platform.yaml?", default=False, tag="sdp_idx_save"):
        _persist_indexes_to_config(source, pg_created, mongo_created, sr_created)

    print()
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


# ============================================================================
# sdp source download
# ============================================================================

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
        # Fallback: each data_type maps to a config with the same name
        data_types = source_config.get('data_types', [])
        config_map = {dt: [dt] for dt in data_types}

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
                available += ["sr_ingest"]
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


# ============================================================================
# sdp source add-classifiers
# ============================================================================

def cmd_source_add_classifiers(args):
    """Add ML classifiers for a source."""
    from social_data_pipeline.setup.classifiers import main as classifiers_main
    classifiers_main(source_name=args.name)
    return 0


# ============================================================================
# sdp source remove
# ============================================================================

def cmd_source_remove(args):
    """Remove source configuration."""
    source_dir = CONFIG_DIR / "sources" / args.name
    if not source_dir.exists():
        print(f"\n  Error: Source '{args.name}' not found in config/sources/\n")
        return 1

    print(f"\n  Source: {args.name}")
    print(f"  Directory: config/sources/{args.name}/")

    # List files
    files = sorted(source_dir.glob("*"))
    if files:
        print("  Files:")
        for f in files:
            print(f"    {f.name}")
    print()

    confirm = input(f"  Remove source '{args.name}' configuration? [y/N]: ").strip().lower()
    if confirm not in ("y", "yes"):
        print("  Aborted.\n")
        return 0

    shutil.rmtree(source_dir)
    print(f"\n  Removed config/sources/{args.name}/")
    print("  Note: Data files (dumps, csv, output) were NOT removed.\n")
    _warn_mount_drift_after_source_change()
    return 0


# ============================================================================
# sdp source list
# ============================================================================

def cmd_source_list(args):
    """List configured sources."""
    from social_data_pipeline.setup.utils import list_sources, get_source_profiles

    sources = list_sources()
    if not sources:
        print("\n  No sources configured. Run: python sdp.py source add <name>\n")
        return 0

    print("\n  Configured sources:\n")
    for source in sources:
        profiles = get_source_profiles(source)
        profiles_str = ", ".join(profiles) if profiles else "none"
        print(f"    {source}")
        print(f"      profiles: {profiles_str}")
    print()
    return 0


# ============================================================================
# sdp source status
# ============================================================================

def cmd_source_status(args):
    """Show source processing/ingestion status."""
    from social_data_pipeline.setup.utils import list_sources, load_source_config, get_source_profiles

    source_name = args.name

    if source_name:
        sources = [source_name]
    else:
        sources = list_sources()

    if not sources:
        print("\n  No sources configured. Run: python sdp.py source add <name>\n")
        return 0

    env = load_env()
    print()
    print("  Social Data Pipeline - Source Status")
    print("  ===================================")

    for source in sources:
        config = load_source_config(source)
        if config is None:
            print(f"\n  Source '{source}': not found")
            continue

        platform = config.get("platform", source)
        data_types = config.get("data_types", [])
        profiles = get_source_profiles(source)

        print(f"\n  Source: {source}")
        print(f"    Platform:    {platform}")
        print(f"    Data types:  {', '.join(data_types)}")
        print(f"    Profiles:    {', '.join(profiles) if profiles else 'none'}")
        paths = config.get("paths", {})
        print("    Paths:")
        print(f"      Dumps:     {paths.get('dumps', f'./data/dumps/{source}')}")
        print(f"      Extracted: {paths.get('extracted', f'./data/extracted/{source}')}")
        print(f"      Parsed:    {paths.get('parsed', f'./data/parsed/{source}')}")
        print(f"      Output:    {paths.get('output', f'./data/output/{source}')}")

        # Show ingestion state for this source
        has_postgres = any(p.startswith("postgres") for p in profiles)
        if has_postgres:
            pgdata_path = env.get("PGDATA_PATH", "./data/database/postgres")
            pgdata = Path(pgdata_path)
            if not pgdata.is_absolute():
                pgdata = ROOT / pgdata
            state_dir = pgdata / "state_tracking"
            _print_source_ingestion_state(state_dir, source, "postgres")

        has_mongo = "mongo_ingest" in profiles
        if has_mongo:
            mongo_data_path = env.get("MONGO_DATA_PATH", "./data/database/mongo")
            mongo_data = Path(mongo_data_path)
            if not mongo_data.is_absolute():
                mongo_data = ROOT / mongo_data
            state_dir = mongo_data / "state_tracking"
            _print_source_ingestion_state(state_dir, source, "mongo")

        has_starrocks = any(p.startswith("sr_") for p in profiles)
        if has_starrocks:
            sr_data_path = env.get("STARROCKS_DATA_PATH", "./data/database/starrocks")
            sr_data = Path(sr_data_path)
            if not sr_data.is_absolute():
                sr_data = ROOT / sr_data
            state_dir = sr_data / "state_tracking"
            _print_source_ingestion_state(state_dir, source, "sr_ingest")
            _print_source_ingestion_state(state_dir, source, "sr_ml")

    print()
    return 0


def _print_source_ingestion_state(state_dir, source, db_type):
    """Print ingestion state for a specific source from state files."""
    if not state_dir.exists():
        return

    prefix = f"{source}_{db_type}_"
    state_files = sorted(f for f in state_dir.glob("*.json") if f.stem.startswith(prefix))

    # Also check legacy naming (PLATFORM prefix)
    if not state_files:
        state_files = sorted(state_dir.glob(f"*_{db_type}_*.json"))

    if not state_files:
        return

    print(f"\n    {db_type.title()} ingestion:")
    for sf in state_files:
        try:
            sdata = json.loads(sf.read_text())
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

        count = len(processed)
        latest = processed[-1] if processed else "-"

        line = f"      {label}: {count} datasets"
        if latest != "-":
            line += f" (latest: {latest})"
        if in_progress:
            line += f" [in progress: {in_progress}]"
        if failed:
            line += f" [{len(failed)} failed]"
        print(line)

        if last_updated:
            print(f"        last updated: {last_updated}")


# ============================================================================
# sdp source error-logs
# ============================================================================

INGESTION_PROFILES = ["postgres_ingest", "postgres_ml", "mongo_ingest", "sr_ingest", "sr_ml"]


def cmd_source_error_logs(args):
    """Show database ingestion error logs for sources."""
    from social_data_pipeline.setup.utils import list_sources, load_source_config, get_source_profiles

    source_name = args.name
    profile_filter = args.profile

    if source_name:
        sources = [source_name]
    else:
        sources = list_sources()

    if not sources:
        print("\n  No sources configured. Run: python sdp.py source add <name>\n")
        return 0

    env = load_env()
    print()
    print("  Social Data Pipeline - Error Logs")
    print("  ==================================")

    any_errors = False

    for source in sources:
        config = load_source_config(source)
        if config is None:
            print(f"\n  Source '{source}': not found")
            continue

        profiles = get_source_profiles(source)
        source_has_errors = False

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

            # Print failed entries from state files
            failed_files, found = _print_failed_entries(state_dir, source, profile, source_has_errors)
            if found:
                if not source_has_errors:
                    source_has_errors = True
                any_errors = True

                # For mongo_ingest, also show relevant mongoimport log sections
                if profile == "mongo_ingest" and failed_files:
                    log_dir = state_dir.parent / "logs"
                    if log_dir.exists():
                        _print_mongoimport_log_sections(log_dir, failed_files)

        if not source_has_errors and source_name:
            print(f"\n  Source: {source}")
            print("    No errors found.")

    if not any_errors and not source_name:
        print("\n  No errors found for any source.")

    print()
    return 0


def _print_failed_entries(state_dir, source, profile, header_printed):
    """Print failed entries from state files. Returns (failed_filenames, found_any)."""
    if not state_dir.exists():
        return [], False

    prefix = f"{source}_{profile}_"
    state_files = sorted(f for f in state_dir.glob("*.json") if f.stem.startswith(prefix))

    # Legacy fallback: match by profile substring
    if not state_files:
        state_files = sorted(state_dir.glob(f"*_{profile}_*.json"))

    if not state_files:
        return [], False

    all_failed = []
    found_any = False

    for sf in state_files:
        try:
            sdata = json.loads(sf.read_text())
        except (json.JSONDecodeError, OSError):
            continue

        failed = sdata.get("failed", [])
        if not failed:
            continue

        if not found_any and not header_printed:
            print(f"\n  Source: {source}")
        found_any = True

        # Extract data type from filename: {source}_{profile}_{data_type}
        parts = sf.stem.split(f"_{profile}_")
        label = parts[1] if len(parts) == 2 else sf.stem

        print(f"\n    {profile} errors ({label}): {len(failed)} failed")
        for entry in failed:
            filename = entry.get("filename", "unknown")
            error = entry.get("error", "no error message")
            timestamp = entry.get("timestamp", "")
            ts_display = f" [{timestamp}]" if timestamp else ""
            print(f"\n      {filename}{ts_display}")
            # Strip container log path from error message (we show logs inline)
            display_error = error.split(". See log:")[0] if ". See log:" in error else error
            for line in display_error.split('\n'):
                print(f"        {line}")
            all_failed.append(filename)

    return all_failed, found_any


def _print_mongoimport_log_sections(log_dir, failed_filenames):
    """Print mongoimport log sections matching failed filenames."""
    import re
    separator = re.compile(r'^={10,}$')
    failed_set = set(failed_filenames)
    log_files = sorted(log_dir.glob("mongoimport_*.log"))
    if not log_files:
        return

    printed_header = False

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

            if not printed_header:
                print(f"\n    Mongoimport logs for failed files ({lf}):")
                printed_header = True

            # Print header block
            header_block = lines[opening_sep:((closing_sep + 1) if closing_sep is not None else len(lines))]
            print()
            for hl in header_block:
                print(f"      {hl}")

            # Print output
            if output_lines:
                for ol in output_lines:
                    print(f"      {ol}")
            else:
                print("      (no mongoimport output found)")


# ============================================================================
# sdp run
# ============================================================================

def cmd_run(args):
    """Run a pipeline profile for a source."""
    from social_data_pipeline.setup.utils import resolve_source, load_source_config

    profile = args.profile
    if profile not in VALID_PROFILES:
        print(f"  Error: Unknown profile '{profile}'.")
        print(f"  Valid profiles: {', '.join(VALID_PROFILES)}")
        return 1

    if getattr(args, "skip_lingua_files", False) and profile != "parse":
        print(f"  Error: --skip-lingua-files only applies to the 'parse' profile (got '{profile}').")
        return 1

    # Resolve source (auto-selects if only one exists)
    source = resolve_source(args.source)
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
    _profile_auth = {
        "postgres_ingest": "POSTGRES_AUTH_ENABLED",
        "postgres_ml": "POSTGRES_AUTH_ENABLED",
        "mongo_ingest": "MONGO_AUTH_ENABLED",
        "sr_ingest": "STARROCKS_AUTH_ENABLED",
        "sr_ml": "STARROCKS_AUTH_ENABLED",
    }
    auth_key = _profile_auth.get(profile)
    if auth_key and load_env().get(auth_key) == "true":
        password = _prompt_db_password(tag="sdp_db_password")
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
    if args.filter:
        env_overrides["FILE_FILTER"] = args.filter

    if getattr(args, "skip_lingua_files", False):
        env_overrides["SKIP_LINGUA_FILES"] = "1"

    # Set env vars for docker compose
    for key, value in env_overrides.items():
        os.environ[key] = value

    service = PROFILE_SERVICE_MAP.get(profile, profile)
    compose_args = ["--profile", profile, "run", "--rm"]
    if args.build:
        compose_args.append("--build")
    compose_args.append(service)

    print(f"  Source: {source} (platform: {platform})")
    if args.filter:
        print(f"  Filter: {args.filter}")
    if getattr(args, "skip_lingua_files", False):
        print("  Skip-lingua-files: enabled")
    result = docker_compose(*compose_args)
    return result.returncode


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

    db_setup_mcp_p = db_sub.add_parser("setup-mcp", help="Configure MCP servers for databases")
    db_setup_mcp_p.set_defaults(func=cmd_db_setup_mcp)

    db_setup_jobs_p = db_sub.add_parser(
        "setup-jobs",
        help="Configure the query scheduler (jobs profile)",
    )
    db_setup_jobs_p.set_defaults(func=cmd_db_setup_jobs)

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

    # ---- sdp source ----
    source_parser = subparsers.add_parser("source", help="Source management")
    source_sub = source_parser.add_subparsers(dest="source_command", help="Source command")

    src_add_p = source_sub.add_parser("add", help="Add a new source")
    src_add_p.add_argument("name", help="Source name (e.g. reddit, twitter_academic)")
    src_add_p.add_argument("--hf", dest="hf_dataset", default=None,
                           help="Hugging Face dataset ID (e.g. user/dataset-name)")
    src_add_p.set_defaults(func=cmd_source_add)

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

    src_remove_p = source_sub.add_parser("remove", help="Remove source configuration")
    src_remove_p.add_argument("name", help="Source name")
    src_remove_p.set_defaults(func=cmd_source_remove)

    src_list_p = source_sub.add_parser("list", help="List configured sources")
    src_list_p.set_defaults(func=cmd_source_list)

    src_status_p = source_sub.add_parser("status", help="Show source status")
    src_status_p.add_argument("name", nargs="?", help="Source name (default: all sources)")
    src_status_p.set_defaults(func=cmd_source_status)

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
    run_parser.set_defaults(func=cmd_run)

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
