"""Database configuration for Social Data Pipeline.

Configures PostgreSQL, MongoDB, and StarRocks settings (port, tablespaces,
PGTune, cache, FE/BE memory).
Generates .env, config/db/*.yaml, postgresql.local.conf, fe.local.conf, be.local.conf,
docker-compose.override.yml.

This is a global, one-time configuration independent of any source.
"""

import os
import secrets
import subprocess
import sys
from getpass import getpass
from pathlib import Path

try:
    import yaml
except ImportError:
    print("Error: PyYAML is required. Install with: pip install pyyaml")
    sys.exit(1)

from social_data_pipeline.core.config import ConfigurationError
from social_data_pipeline.setup.utils import (
    ROOT, CONFIG_DIR,
    detect_hardware, load_env,
    ask, ask_int, ask_bool, ask_choice, ask_multi_select, ask_multi_line,
    section_header, write_files,
)


# Map of db name → settings key that holds the data path on the host.
# Used by both the per-DB write loop and the MCP credential precheck so
# the symbolic db_name is the single point of truth, not three sprinkled
# `if "pgdata_path" in settings` chains.
DB_DATA_PATH_KEYS = {
    "postgres": "pgdata_path",
    "mongo": "mongo_data_path",
    "starrocks": "starrocks_data_path",
}


def ask_password(label: str, tag=None) -> str:
    """Prompt for a password with confirmation. Uses getpass for hidden input."""
    from social_data_pipeline.setup.utils import _tag_prefix
    prefix = _tag_prefix(tag)
    while True:
        pw1 = getpass(f"  {prefix}{label}: ")
        if not pw1:
            print("    Password cannot be empty.")
            continue
        confirm_tag = f"{tag}_confirm" if tag else None
        confirm_prefix = _tag_prefix(confirm_tag)
        pw2 = getpass(f"  {confirm_prefix}Confirm {label.lower()}: ")
        if pw1 != pw2:
            print("    Passwords do not match. Try again.")
            continue
        return pw1


def generate_password() -> str:
    """Generate a random 32-character URL-safe password."""
    return secrets.token_urlsafe(24)


# ============================================================================
# Load existing configuration
# ============================================================================

def _extract_existing_pgtune():
    """Extract pgtune content from existing postgresql.local.conf, if any."""
    local_conf = CONFIG_DIR / "postgres" / "postgresql.local.conf"
    if not local_conf.exists():
        return ""
    try:
        content = local_conf.read_text()
    except OSError:
        return ""
    marker = "# PASTE PGTUNE OUTPUT BELOW THIS LINE"
    if marker not in content:
        return ""
    idx = content.index(marker)
    after_marker = content[content.index("\n", idx) + 1:]
    return after_marker.strip()


def _load_existing_db_config():
    """Load existing database configuration for use as defaults on re-run."""
    existing = {}
    env = load_env()

    # Load scalar values from .env
    if env.get("DATA_PATH"):
        existing["data_path"] = env["DATA_PATH"]
    if env.get("PGDATA_PATH"):
        existing["pgdata_path"] = env["PGDATA_PATH"]
    if env.get("DB_NAME"):
        existing["db_name"] = env["DB_NAME"]
    if env.get("MONGO_DATA_PATH"):
        existing["mongo_data_path"] = env["MONGO_DATA_PATH"]
    if env.get("DB_EXPORT_PATH"):
        existing["export_path"] = env["DB_EXPORT_PATH"]
    if env.get("STARROCKS_DATA_PATH"):
        existing["starrocks_data_path"] = env["STARROCKS_DATA_PATH"]
    for env_key, setting_key in [
        ("POSTGRES_PORT", "pg_port"),
        ("MONGO_PORT", "mongo_port"),
        ("MONGO_CACHE_SIZE_GB", "mongo_cache_size_gb"),
        ("STARROCKS_PORT", "starrocks_port"),
        ("STARROCKS_FE_HTTP_PORT", "starrocks_fe_http_port"),
    ]:
        if env.get(env_key):
            try:
                existing[setting_key] = int(env[env_key])
            except (ValueError, TypeError):
                pass
    for env_key, setting_key in [
        ("POSTGRES_MEM_LIMIT", "pg_mem_limit"),
        ("MONGO_MEM_LIMIT", "mongo_mem_limit"),
        ("STARROCKS_MEM_LIMIT", "starrocks_mem_limit"),
    ]:
        if env.get(env_key):
            try:
                existing[setting_key] = int(env[env_key].rstrip("g"))
            except (ValueError, TypeError):
                pass

    # Load from YAML configs (fill gaps not covered by .env)
    pg_yaml = CONFIG_DIR / "db" / "postgres.yaml"
    if pg_yaml.exists():
        try:
            pg = yaml.safe_load(pg_yaml.read_text()) or {}
            if pg.get("port") is not None:
                existing.setdefault("pg_port", pg["port"])
            if pg.get("name"):
                existing.setdefault("db_name", pg["name"])
            if pg.get("tablespaces"):
                existing["tablespaces"] = pg["tablespaces"]
            if pg.get("auth"):
                existing["auth_enabled"] = True
            if pg.get("ro_username"):
                existing["ro_username"] = pg["ro_username"]
        except (OSError, yaml.YAMLError):
            pass

    mongo_yaml = CONFIG_DIR / "db" / "mongo.yaml"
    if mongo_yaml.exists():
        try:
            mg = yaml.safe_load(mongo_yaml.read_text()) or {}
            if mg.get("port") is not None:
                existing.setdefault("mongo_port", mg["port"])
            if mg.get("cache_size_gb") is not None:
                existing.setdefault("mongo_cache_size_gb", mg["cache_size_gb"])
            if mg.get("validate_before_import"):
                existing["mongo_validate"] = mg["validate_before_import"]
            if mg.get("auth"):
                existing["auth_enabled"] = True
            if mg.get("ro_username"):
                existing.setdefault("ro_username", mg["ro_username"])
        except (OSError, yaml.YAMLError):
            pass

    sr_yaml = CONFIG_DIR / "db" / "starrocks.yaml"
    if sr_yaml.exists():
        try:
            sr = yaml.safe_load(sr_yaml.read_text()) or {}
            if sr.get("port") is not None:
                existing.setdefault("starrocks_port", sr["port"])
            if sr.get("fe_http_port") is not None:
                existing.setdefault("starrocks_fe_http_port", sr["fe_http_port"])
            if sr.get("fe_jvm_heap") is not None:
                existing.setdefault("sr_fe_jvm_heap", sr["fe_jvm_heap"])
            if sr.get("be_mem_limit") is not None:
                existing.setdefault("sr_be_mem_limit", sr["be_mem_limit"])
            if sr.get("alter_tablet_workers") is not None:
                existing.setdefault("sr_alter_tablet_workers", sr["alter_tablet_workers"])
            if sr.get("compression"):
                existing.setdefault("sr_compression", sr["compression"])
            if sr.get("storage_paths"):
                existing["starrocks_storage_paths"] = sr["storage_paths"]
            if sr.get("auth"):
                existing["auth_enabled"] = True
            if sr.get("ro_username"):
                existing.setdefault("ro_username", sr["ro_username"])
        except (OSError, yaml.YAMLError):
            pass

    # Determine databases from existing config files
    databases = []
    if pg_yaml.exists():
        databases.append("postgres")
    if mongo_yaml.exists():
        databases.append("mongo")
    if sr_yaml.exists():
        databases.append("starrocks")
    if databases:
        existing["databases"] = databases

    return existing


# ============================================================================
# Interactive questionnaire
# ============================================================================

def run_questionnaire(hw):
    """Run the database configuration questionnaire. Returns settings dict."""
    existing = _load_existing_db_config()
    settings = {}

    # --- Print hardware summary ---
    section_header("Hardware Detected")
    cores = hw["cpu_cores"]
    ram = hw["ram_gb"]
    print(f"  CPU cores: {cores or 'unknown'}")
    print(f"  RAM:       {ram or 'unknown'} GB")
    print()

    # ---- Data base path ----
    section_header("Data Path")
    print("  Base directory for all data (dumps, parsed, output, databases).")
    print()
    data_path = ask("Data base path", existing.get("data_path", "./data"), tag="db_data_path")
    settings["data_path"] = data_path

    # ---- Database selection ----
    section_header("Database Selection")

    all_databases = ["postgres", "mongo", "starrocks"]
    databases = ask_multi_select("Databases:", all_databases, existing.get("databases", ["postgres"]), tag="db_databases")
    settings["databases"] = databases

    has_postgres = "postgres" in databases
    has_mongo = "mongo" in databases
    has_starrocks = "starrocks" in databases

    # ---- Paths (database data dirs) ----
    section_header("Database Paths")
    if has_postgres:
        settings["pgdata_path"] = ask("PostgreSQL data path", existing.get("pgdata_path", f"{data_path}/database/postgres"), tag="db_pgdata_path")
    if has_mongo:
        settings["mongo_data_path"] = ask("MongoDB data path", existing.get("mongo_data_path", f"{data_path}/database/mongo"), tag="db_mongo_data_path")
    if has_starrocks:
        settings["starrocks_data_path"] = ask("StarRocks data path", existing.get("starrocks_data_path", f"{data_path}/database/starrocks"), tag="db_sr_data_path")

    print()
    print("  Host directory bind-mounted into database containers at /export.")
    print("  Use this path in SQL COPY, mongoexport, etc. to write results to the host.")
    print()
    settings["export_path"] = ask(
        "Export path",
        existing.get("export_path", f"{data_path}/export"),
        tag="db_export_path",
    )

    # ---- PostgreSQL ----
    if has_postgres:
        section_header("PostgreSQL Configuration")

        settings["db_name"] = ask("Database name", existing.get("db_name", "datasets"), tag="db_name")
        settings["pg_port"] = ask_int("PostgreSQL port", existing.get("pg_port", 5432), tag="db_pg_port")

        # Tablespace configuration
        if ask_bool("Use tablespaces? (spread tables across multiple disks)", bool(existing.get("tablespaces")), tag="db_tablespaces"):
            print()
            print("  Note: Check documentation for expected disk usage per data type.")
            print()
            existing_ts = existing.get("tablespaces", {})
            ts_tablespaces = {}

            if existing_ts:
                print("  Current tablespaces:")
                for name, path in existing_ts.items():
                    print(f"    {name}: {path}")
                print()
                if ask_bool("Keep existing tablespaces?", True, tag="db_ts_keep"):
                    ts_tablespaces = dict(existing_ts)

            if not ts_tablespaces:
                while True:
                    ts_name = ask("Tablespace name (e.g. nvme1)", tag="db_ts_name")
                    if not ts_name or ts_name == "pgdata":
                        print("    'pgdata' is reserved for the default PostgreSQL data directory.")
                        continue
                    ts_path = ask(f"Host path for '{ts_name}' (directory on disk)", tag="db_ts_path")
                    if ts_path:
                        ts_tablespaces[ts_name] = ts_path
                    if not ask_bool("Add another tablespace?", False, tag="db_ts_more"):
                        break

            if ts_tablespaces:
                settings["tablespaces"] = ts_tablespaces
            print()

        fs = ask_choice(
            "Filesystem for PostgreSQL data:",
            ["standard", "zfs"],
            default="standard",
            tag="db_filesystem",
        )
        settings["filesystem"] = fs

        print()
        print("  For PostgreSQL memory tuning, provide your PGTune output.")
        print("  Generate at: https://pgtune.leopard.in.ua/")
        print("    DB Version: 18 | OS: linux | DB Type: dw | Storage: ssd")
        print()
        print("  NOTE: PGTune may include 'io_method = io_uring'. If PostgreSQL fails")
        print("  to start, remove that line — io_uring is blocked on some kernels.")
        if hw["ram_gb"]:
            print(f"    Total Memory: {hw['ram_gb']} GB | CPUs: {hw['cpu_cores']}")
        print()
        existing_pgtune = _extract_existing_pgtune()
        pgtune_choices = ["paste", "file", "skip"]
        pgtune_default = "paste"
        if existing_pgtune:
            pgtune_choices = ["keep", "paste", "file", "skip"]
            pgtune_default = "keep"
        pgtune_method = ask_choice(
            "PGTune output:",
            pgtune_choices,
            default=pgtune_default,
            tag="db_pgtune_method",
        )
        if pgtune_method == "keep":
            settings["pgtune_output"] = existing_pgtune
        elif pgtune_method == "paste":
            settings["pgtune_output"] = ask_multi_line("Paste PGTune output below:", tag="db_pgtune_paste")
        elif pgtune_method == "file":
            pgtune_path = ask("Path to file with PGTune output", tag="db_pgtune_file")
            try:
                settings["pgtune_output"] = Path(pgtune_path).expanduser().read_text()
            except (OSError, ValueError) as e:
                print(f"    Warning: Could not read {pgtune_path}: {e}")
                settings["pgtune_output"] = ""
        else:
            settings["pgtune_output"] = ""

        if fs == "zfs" and hw["ram_gb"]:
            arc_max_gb = int(hw["ram_gb"] // 2)
            arc_max_bytes = arc_max_gb * 1024 ** 3
            print()
            print("  NOTE: ZFS ARC cache competes with PostgreSQL for RAM.")
            print(f"  To avoid memory pressure, limit ARC (suggested ~{arc_max_gb}GB for this system):")
            print(f"    echo {arc_max_bytes} > /sys/module/zfs/parameters/zfs_arc_max")
            print("  Persist in /etc/modprobe.d/zfs.conf:")
            print(f"    options zfs zfs_arc_max={arc_max_bytes}")

        print()
        suggested_pg_mem = int(hw["ram_gb"] * 0.6) if hw["ram_gb"] else 0
        pg_mem = ask_int("PostgreSQL container memory limit (GB, 0=unlimited)", existing.get("pg_mem_limit", suggested_pg_mem), tag="db_pg_mem_limit")
        if pg_mem > 0:
            settings["pg_mem_limit"] = pg_mem

    # ---- MongoDB ----
    if has_mongo:
        section_header("MongoDB Configuration")

        settings["mongo_port"] = ask_int("MongoDB port", existing.get("mongo_port", 27017), tag="db_mongo_port")
        settings["mongo_cache_size_gb"] = ask_int("MongoDB WiredTiger cache size (GB)", existing.get("mongo_cache_size_gb", 2), tag="db_mongo_cache")

        mongo_cache = settings.get("mongo_cache_size_gb", 2)
        suggested_mongo_mem = max(2, mongo_cache * 2)
        mongo_mem = ask_int("MongoDB container memory limit (GB, 0=unlimited)", existing.get("mongo_mem_limit", suggested_mongo_mem), tag="db_mongo_mem_limit")
        if mongo_mem > 0:
            settings["mongo_mem_limit"] = mongo_mem

        print()
        print("  Pre-import file validation prevents partial ingestion of corrupt files.")
        print("  mongoimport is not atomic — without validation, truncated or malformed")
        print("  files leave partial data permanently in the database.")
        print()
        print("    full: validates every JSON line before import (one sequential read per file)")
        print("    tail: checks only the last 8KB (catches truncation, not malformed lines)")
        print("    none: skip validation")
        settings["mongo_validate"] = ask_choice(
            "Pre-import file validation",
            ["full", "tail", "none"],
            default=existing.get("mongo_validate", "full"),
            tag="db_mongo_validate",
        )

    # ---- StarRocks ----
    if has_starrocks:
        section_header("StarRocks Configuration")

        settings["starrocks_port"] = ask_int("StarRocks MySQL protocol port", existing.get("starrocks_port", 9030), tag="db_sr_port")
        settings["starrocks_fe_http_port"] = ask_int("StarRocks FE HTTP port (admin/Stream Load)", existing.get("starrocks_fe_http_port", 8030), tag="db_sr_fe_http_port")

        print()
        print("  StarRocks runs FE (query planner) and BE (storage engine) in one container.")
        print("  Allocate memory to each component separately.")
        print()

        # FE JVM heap
        default_fe_heap = max(2, min(8, int(ram // 8))) if ram else 4
        settings["sr_fe_jvm_heap"] = ask(
            "FE JVM heap size (GB)",
            existing.get("sr_fe_jvm_heap", default_fe_heap),
            tag="db_sr_fe_heap",
        )
        fe_heap = float(settings["sr_fe_jvm_heap"])

        # Container memory limit (asked before BE so the BE default can use it)
        print()
        if ram:
            suggested_sr_mem = int(max(fe_heap + 4, ram * 0.6))
        else:
            suggested_sr_mem = 0
        sr_mem_str = ask(
            "StarRocks container memory limit (GB, 0=unlimited)",
            existing.get("starrocks_mem_limit", suggested_sr_mem),
            tag="db_sr_mem_limit",
        )
        sr_mem = float(sr_mem_str)
        if sr_mem > 0:
            settings["starrocks_mem_limit"] = sr_mem_str

        # BE memory limit (default: container_limit - fe_heap - 2GB headroom, or 50% RAM)
        if sr_mem > 0:
            default_be_mem = int(max(2, sr_mem - fe_heap - 2))
        elif ram:
            default_be_mem = int(max(2, ram * 0.5))
        else:
            default_be_mem = 8
        settings["sr_be_mem_limit"] = ask(
            "BE memory limit (GB)",
            existing.get("sr_be_mem_limit", default_be_mem),
            tag="db_sr_be_mem",
        )

        # Schema-change / alter worker pool size (BE-side). Primary knob for
        # CREATE INDEX / ALTER TABLE parallelism; each worker holds a buffer
        # for bitmap building, so higher values trade memory for speed.
        default_alter_workers = max(4, min(cores // 2, 10)) if cores else 4
        print()
        print("  The BE alter worker pool controls how many tablets are rebuilt in")
        print("  parallel during CREATE INDEX / ALTER TABLE (StarRocks default: 3).")
        print("  Higher values speed up index creation but use more memory — each")
        print("  worker buffers bitmaps while it runs, and high-cardinality columns")
        print("  can push the BE past its memory limit.")
        print()
        settings["sr_alter_tablet_workers"] = ask_int(
            "BE alter/schema-change worker count",
            existing.get("sr_alter_tablet_workers", default_alter_workers),
            tag="db_sr_alter_workers",
        )

        # Table compression codec, applied to newly created tables only.
        # ZSTD default: ~33% smaller than LZ4 on text-heavy data, faster
        # queries and lower CPU in benchmarks. Existing tables keep their codec.
        print()
        print("  Compression codec for new tables. ZSTD compresses text far")
        print("  better than LZ4 (~33% smaller) with faster scans; LZ4 is")
        print("  StarRocks' own default. Only affects tables created from now on.")
        print()
        settings["sr_compression"] = ask_choice(
            "Table compression",
            ["ZSTD", "LZ4"],
            default=existing.get("sr_compression", "ZSTD"),
            tag="db_sr_compression",
        )

        # Multi-disk storage — the primary data path above is always used as
        # storage; these are additional disks on top of it.
        print()
        print("  StarRocks' storage engine can spread tablets across multiple disks.")
        print(f"  The primary data path ({settings['starrocks_data_path']}) is always used.")
        print("  Add extra disks here to increase total capacity and IO parallelism.")
        print()
        if ask_bool("Add extra disks for StarRocks storage?", bool(existing.get("starrocks_storage_paths")), tag="db_sr_multidisk"):
            existing_paths = existing.get("starrocks_storage_paths", [])
            if existing_paths:
                print()
                print("  Current extra storage paths:")
                for p in existing_paths:
                    print(f"    {p}")
                print()
                if ask_bool("Keep existing extra storage paths?", True, tag="db_sr_keep_paths"):
                    settings["starrocks_storage_paths"] = list(existing_paths)

            if "starrocks_storage_paths" not in settings:
                storage_paths = []
                while True:
                    sp = ask("Extra host path for StarRocks storage (e.g. /mnt/nvme1/starrocks)", tag="db_sr_storage_path")
                    if sp:
                        storage_paths.append(sp)
                    if not ask_bool("Add another extra storage path?", False, tag="db_sr_more_paths"):
                        break
                if storage_paths:
                    settings["starrocks_storage_paths"] = storage_paths

    # ---- Authentication ----
    if has_postgres or has_mongo or has_starrocks:
        section_header("Authentication")
        print("  Enable database authentication to require passwords for connections.")
        print("  Recommended for multi-user or remote servers.")
        print()

        if ask_bool("Enable database authentication?", existing.get("auth_enabled", False), tag="db_auth"):
            settings["auth_enabled"] = True

            print()
            print("  Choose an admin password for database access.")
            print("  This password is NOT stored anywhere — you will be prompted when needed.")
            print()
            settings["db_password"] = ask_password("Admin password", tag="db_password")

            print()
            if ask_bool("Create a read-only user? (required for MCP servers)", True, tag="db_ro_user"):
                ro_username = ask("Read-only username", existing.get("ro_username", "readonly"), tag="db_ro_username")
                settings["ro_username"] = ro_username
                print()
                settings["ro_password"] = _resolve_ro_password(ro_username, existing)

    return settings


def _resolve_ro_password(ro_username, existing):
    """Decide the RO password to use during full `db setup` reconfigure.

    Defaults to **keeping** the existing password whenever there is one to
    keep — the previous default (always auto-generate on every re-run)
    silently rotated the password and broke any client with cached
    credentials. The keep-existing prompt is only offered when the
    username matches the prior one; a different username is a different
    DB role and needs its own password.

    Returns the chosen password string.
    """
    existing_ro_password = None
    if ro_username == existing.get("ro_username"):
        try:
            existing_ro_password = _read_existing_ro_password(existing)
        except ConfigurationError:
            existing_ro_password = None
    if existing_ro_password and ask_bool(
        "Keep existing read-only password?", True, tag="db_ro_keep_existing"
    ):
        return existing_ro_password
    if ask_bool("Auto-generate read-only password?", True, tag="db_ro_auto_password"):
        return secrets.token_urlsafe(24)
    return ask_password("Read-only password", tag="db_ro_password")


# ============================================================================
# Config generators
# ============================================================================

def generate_env(settings):
    """Generate .env file content with database and global settings.

    Preserves existing env vars not managed by db setup (e.g. MCP ports,
    HF_TOKEN) by reading the current .env and appending unmanaged keys.
    """
    lines = [
        "# ===== DATA PATH =====",
        f"DATA_PATH={settings.get('data_path', './data')}",
        "",
        "# ===== HUGGINGFACE CONFIGURATION (ml profile) =====",
        "# Set HF_HOME to specify a custom cache directory for Hugging Face models and datasets.",
        "# HF_HOME=",
        "# Set HF_TOKEN to avoid rate limits and download private models.",
        "# HF_TOKEN=",
    ]

    if "pgdata_path" in settings:
        lines += [
            "",
            "# ===== POSTGRESQL CONFIGURATION =====",
            f"PGDATA_PATH={settings['pgdata_path']}",
            f"DB_NAME={settings.get('db_name', 'datasets')}",
            f"POSTGRES_PORT={settings.get('pg_port', 5432)}",
        ]
        if settings.get("pg_mem_limit"):
            lines.append(f"POSTGRES_MEM_LIMIT={settings['pg_mem_limit']}g")

    if "mongo_data_path" in settings:
        lines += [
            "",
            "# ===== MONGODB CONFIGURATION =====",
            f"MONGO_DATA_PATH={settings['mongo_data_path']}",
            f"MONGO_PORT={settings.get('mongo_port', 27017)}",
            f"MONGO_CACHE_SIZE_GB={settings.get('mongo_cache_size_gb', 2)}",
        ]
        if settings.get("mongo_mem_limit"):
            lines.append(f"MONGO_MEM_LIMIT={settings['mongo_mem_limit']}g")

    if "starrocks_data_path" in settings:
        lines += [
            "",
            "# ===== STARROCKS CONFIGURATION =====",
            f"STARROCKS_DATA_PATH={settings['starrocks_data_path']}",
            f"STARROCKS_PORT={settings.get('starrocks_port', 9030)}",
            f"STARROCKS_FE_HTTP_PORT={settings.get('starrocks_fe_http_port', 8030)}",
        ]
        if settings.get("starrocks_mem_limit"):
            lines.append(f"STARROCKS_MEM_LIMIT={settings['starrocks_mem_limit']}g")

    if "export_path" in settings:
        lines += [
            "",
            "# ===== EXPORT =====",
            f"DB_EXPORT_PATH={settings['export_path']}",
        ]

    if settings.get("auth_enabled"):
        auth_lines = [
            "",
            "# ===== AUTHENTICATION =====",
        ]
        if "pgdata_path" in settings:
            auth_lines.append("POSTGRES_AUTH_ENABLED=true")
        if "mongo_data_path" in settings:
            auth_lines += [
                "MONGO_AUTH_ENABLED=true",
                "MONGO_ADMIN_USER=admin",
            ]
        if "starrocks_data_path" in settings:
            auth_lines.append("STARROCKS_AUTH_ENABLED=true")
        if settings.get("ro_username"):
            if "pgdata_path" in settings:
                auth_lines.append(f"POSTGRES_RO_USER={settings['ro_username']}")
            if "mongo_data_path" in settings:
                auth_lines.append(f"MONGO_RO_USER={settings['ro_username']}")
            if "starrocks_data_path" in settings:
                auth_lines.append(f"STARROCKS_RO_USER={settings['ro_username']}")
        lines += auth_lines

    # Preserve existing env vars not managed by this function
    new_content = "\n".join(lines) + "\n"
    managed_keys = set()
    for line in lines:
        stripped = line.lstrip("# ").strip()
        if "=" in stripped and not stripped.startswith("="):
            managed_keys.add(stripped.split("=", 1)[0])

    env_path = ROOT / ".env"
    if env_path.exists():
        preserved = []
        for line in env_path.read_text().splitlines():
            stripped = line.lstrip("# ").strip()
            if "=" in stripped and not stripped.startswith("="):
                key = stripped.split("=", 1)[0]
                if key not in managed_keys:
                    preserved.append(line)
        if preserved:
            new_content += "\n".join(preserved) + "\n"

    return new_content


def generate_db_postgres_yaml(settings):
    """Generate config/db/postgres.yaml content."""
    config = {
        "port": settings.get("pg_port", 5432),
        "name": settings.get("db_name", "datasets"),
    }
    if "tablespaces" in settings:
        config["tablespaces"] = settings["tablespaces"]
    if settings.get("auth_enabled"):
        config["auth"] = True
    if settings.get("ro_username"):
        config["ro_username"] = settings["ro_username"]
    return yaml.dump(config, default_flow_style=False, sort_keys=False)


def generate_db_mongo_yaml(settings):
    """Generate config/db/mongo.yaml content."""
    config = {
        "port": settings.get("mongo_port", 27017),
        "cache_size_gb": settings.get("mongo_cache_size_gb", 2),
        "validate_before_import": settings.get("mongo_validate", "full"),
    }
    if settings.get("auth_enabled"):
        config["auth"] = True
    if settings.get("ro_username"):
        config["ro_username"] = settings["ro_username"]
    return yaml.dump(config, default_flow_style=False, sort_keys=False)


def generate_db_starrocks_yaml(settings):
    """Generate config/db/starrocks.yaml content."""
    config = {
        "port": settings.get("starrocks_port", 9030),
        "fe_http_port": settings.get("starrocks_fe_http_port", 8030),
        "fe_jvm_heap": settings.get("sr_fe_jvm_heap", 4),
        "be_mem_limit": settings.get("sr_be_mem_limit", 8),
        "alter_tablet_workers": settings.get("sr_alter_tablet_workers", 3),
        "compression": settings.get("sr_compression", "ZSTD"),
    }
    if settings.get("starrocks_storage_paths"):
        config["storage_paths"] = settings["starrocks_storage_paths"]
    if settings.get("auth_enabled"):
        config["auth"] = True
    if settings.get("ro_username"):
        config["ro_username"] = settings["ro_username"]
    return yaml.dump(config, default_flow_style=False, sort_keys=False)


def _replace_conf_value(content, key, value):
    """Replace a key = value line in a .conf file (properties format)."""
    import re
    pattern = rf"^(\s*#?\s*){re.escape(key)}\s*=.*$"
    replacement = f"{key} = {value}"
    new_content, count = re.subn(pattern, replacement, content, flags=re.MULTILINE)
    if count == 0:
        new_content = content.rstrip("\n") + f"\n{replacement}\n"
    return new_content


def generate_starrocks_fe_conf(settings):
    """Generate config/starrocks/fe.local.conf from base fe.conf with tuned JVM heap."""
    base_path = CONFIG_DIR / "starrocks" / "fe.conf"
    content = base_path.read_text()

    jvm_heap = settings.get("sr_fe_jvm_heap", 4)
    content = _replace_conf_value(content, "jvm_heap_size", f"{jvm_heap}g")

    return content


def generate_starrocks_be_conf(settings):
    """Generate config/starrocks/be.local.conf from base be.conf with storage paths and memory."""
    base_path = CONFIG_DIR / "starrocks" / "be.conf"
    content = base_path.read_text()

    # Multi-disk storage paths. The primary data path (STARROCKS_DATA_PATH) is
    # always mounted at /data/deploy/starrocks/be/storage by docker-compose.yml
    # and must be the first entry in storage_root_path; extras follow as
    # storage_0..N from docker-compose.override.yml.
    storage_paths = settings.get("starrocks_storage_paths")
    if storage_paths:
        container_paths = ["/data/deploy/starrocks/be/storage"] + [
            f"/data/deploy/starrocks/be/storage_{i}" for i in range(len(storage_paths))
        ]
        content = _replace_conf_value(
            content, "storage_root_path", ";".join(container_paths)
        )

    # BE memory limit (absolute GB value)
    be_mem = settings.get("sr_be_mem_limit")
    if be_mem:
        content = _replace_conf_value(content, "mem_limit", f"{be_mem}G")

    # Alter/schema-change worker pool (BE-side; sized at init, requires restart)
    alter_workers = settings.get("sr_alter_tablet_workers")
    if alter_workers:
        content = _replace_conf_value(content, "alter_tablet_worker_count", str(alter_workers))

    return content


def generate_docker_compose_override(settings):
    """Generate docker-compose.override.yml with extra volume mounts.

    Handles PostgreSQL tablespace volumes and StarRocks multi-disk storage.
    """
    tablespaces = settings.get("tablespaces", {})
    pg_lines = []
    for ts_name, host_path in tablespaces.items():
        if ts_name != "pgdata":
            pg_lines.append(f"      - {host_path}:/data/tablespace/{ts_name}")

    sr_storage = settings.get("starrocks_storage_paths", [])
    sr_lines = []
    for i, host_path in enumerate(sr_storage):
        sr_lines.append(f"      - {host_path}:/data/deploy/starrocks/be/storage_{i}")

    if not pg_lines and not sr_lines:
        return None

    content = "# Auto-generated by sdp db setup — extra volume mounts.\n\nservices:\n"
    if pg_lines:
        content += "  postgres:\n    volumes:\n" + "\n".join(pg_lines) + "\n"
    if sr_lines:
        if pg_lines:
            content += "\n"
        content += "  starrocks:\n    volumes:\n" + "\n".join(sr_lines) + "\n"

    return content


def generate_postgresql_local_conf(settings):
    """Generate postgresql.local.conf by copying base, toggling ZFS, appending pgtune."""
    base_path = CONFIG_DIR / "postgres" / "postgresql.conf"
    try:
        base_content = base_path.read_text()
    except PermissionError:
        print(f"\n  Error: Cannot read {base_path}")
        print("  The config/ directory may be missing the execute bit (needed for traversal).")
        print("  Try: chmod 755 config/ config/*/")
        sys.exit(1)

    # Split at the pgtune marker
    pgtune_marker = "# PASTE PGTUNE OUTPUT BELOW THIS LINE"
    if pgtune_marker in base_content:
        marker_idx = base_content.index(pgtune_marker)
        marker_line_end = base_content.index("\n", marker_idx) + 1
        content = base_content[:marker_line_end]
    else:
        content = base_content

    # Toggle ZFS settings
    is_zfs = settings.get("filesystem") == "zfs"
    if is_zfs:
        new_lines = []
        in_zfs_block = False
        seen_zfs_header = False
        for line in content.splitlines():
            if "ZFS optimizations" in line:
                seen_zfs_header = True
                new_lines.append(line)
                continue
            if seen_zfs_header and not in_zfs_block and line.startswith("#=="):
                in_zfs_block = True
                new_lines.append(line)
                continue
            if in_zfs_block and line.startswith("#=="):
                in_zfs_block = False
                new_lines.append(line)
                continue
            if in_zfs_block:
                if line.startswith("# # "):
                    new_lines.append("#" + line[3:])
                elif line.startswith("# ") and "=" in line:
                    new_lines.append(line[2:])
                else:
                    new_lines.append(line)
            else:
                new_lines.append(line)
        content = "\n".join(new_lines) + "\n"

    # Append pgtune output
    pgtune = settings.get("pgtune_output", "").strip()
    if pgtune:
        content += pgtune + "\n"

    return content


def generate_pg_hba_local_conf(settings):
    """Generate pg_hba.local.conf with scram-sha-256 for auth-enabled setup."""
    lines = [
        "# Auto-generated by sdp db setup — authentication enabled",
        "# TYPE  DATABASE  USER  ADDRESS  METHOD",
        "",
        "# Local (unix socket) — trust for container-internal access",
        "local   all       all                    trust",
        "",
        "# Localhost IPv4",
        "host    all       all   127.0.0.1/32     scram-sha-256",
        "",
        "# All users — require password from Docker networks",
        "host    all       all   172.16.0.0/12    scram-sha-256",
        "host    all       all   192.168.0.0/16   scram-sha-256",
    ]

    return "\n".join(lines) + "\n"


# ============================================================================
# Summary
# ============================================================================

def print_summary(settings, files_to_write):
    """Print a summary of database settings and files to be written."""
    section_header("Database Configuration Summary")

    data_path = settings.get("data_path", "./data")
    print(f"  Data path:   {data_path}")
    if "export_path" in settings:
        print(f"  Export path: {settings['export_path']}  (-> /export in containers)")

    databases = settings["databases"]
    print(f"  Databases:   {', '.join(databases)}")
    print()

    if "postgres" in databases:
        print("  PostgreSQL:")
        print(f"    DB name:             {settings.get('db_name', 'datasets')}")
        print(f"    Port:                {settings.get('pg_port', 5432)}")
        print(f"    Data path:           {settings.get('pgdata_path', './data/database/postgres')}")
        print(f"    Filesystem:          {settings.get('filesystem', 'standard')}")
        if "tablespaces" in settings:
            print("    Tablespaces:")
            for ts_name, ts_path in settings["tablespaces"].items():
                print(f"      {ts_name}: {ts_path}")
        print(f"    PGTune:              {'provided' if settings.get('pgtune_output') else 'not provided'}")
        print()

    if "mongo" in databases:
        print("  MongoDB:")
        print(f"    Port:                {settings.get('mongo_port', 27017)}")
        print(f"    Cache size:          {settings.get('mongo_cache_size_gb', 2)} GB")
        print(f"    Data path:           {settings.get('mongo_data_path', './data/database/mongo')}")
        print()

    if "starrocks" in databases:
        print("  StarRocks:")
        print(f"    Port (MySQL):        {settings.get('starrocks_port', 9030)}")
        print(f"    FE HTTP port:        {settings.get('starrocks_fe_http_port', 8030)}")
        print(f"    FE JVM heap:         {settings.get('sr_fe_jvm_heap', 4)} GB")
        print(f"    BE memory limit:     {settings.get('sr_be_mem_limit', 8)} GB")
        print(f"    Alter workers:       {settings.get('sr_alter_tablet_workers', 3)}")
        print(f"    Compression:         {settings.get('sr_compression', 'ZSTD')}")
        print(f"    Data path:           {settings.get('starrocks_data_path', './data/database/starrocks')}")
        if settings.get("starrocks_storage_paths"):
            print("    Extra storage paths:")
            for sp in settings["starrocks_storage_paths"]:
                print(f"      {sp}")
        if settings.get("starrocks_mem_limit"):
            print(f"    Container limit:     {settings['starrocks_mem_limit']} GB")
        print()

    if settings.get("auth_enabled"):
        print("  Authentication:  enabled")
        if settings.get("ro_username"):
            print(f"    RO user:         {settings['ro_username']} (auto-generated password)")
        print()

    print("  Files to write:")
    for path, _ in files_to_write:
        rel = path.relative_to(ROOT)
        exists = path.exists()
        status = " (exists, will backup)" if exists else ""
        print(f"    {rel}{status}")
    print()


# ============================================================================
# Credential writing
# ============================================================================

def _write_ro_password_for(db_name: str, data_path, password: str) -> str:
    """Atomically write the RO password for a single database to its data volume.

    The file (`.ro_credentials`) is single-line `{password}\\n`, mode 0600,
    host-owned. Atomicity: writes a sibling `.ro_credentials.tmp`, chmods it,
    then renames over `.ro_credentials`. On any failure mid-write the `.tmp`
    is unlinked and the original file (if any) is left intact.

    Args:
        db_name: "postgres" | "mongo" | "starrocks" — used only in error text.
        data_path: host directory that holds (or will hold) `.ro_credentials`.
        password: RO user password (no leading/trailing whitespace).

    Returns:
        Absolute path of the written `.ro_credentials` (string).

    Raises:
        ConfigurationError: when the file cannot be written even via the
            docker-shell PermissionError fallback. Always cleans up the
            temp file before raising.
    """
    data_path = Path(data_path)
    cred_file = data_path / ".ro_credentials"
    tmp_file = data_path / ".ro_credentials.tmp"
    payload = password + "\n"

    data_path.mkdir(parents=True, exist_ok=True)

    def _atomic_host_write():
        # Best-effort cleanup of any stale .tmp from a prior crashed write.
        if tmp_file.exists():
            try:
                tmp_file.unlink()
            except OSError:
                pass
        tmp_file.write_text(payload)
        os.chmod(tmp_file, 0o600)
        try:
            os.replace(tmp_file, cred_file)
        except OSError:
            # Rename failed — clean up so we don't leave a half-written file.
            try:
                tmp_file.unlink()
            except OSError:
                pass
            raise
        # Defensive post-rename mode assertion; chmod again if filesystems
        # silently strip mode bits on rename (rare, but cheap to enforce).
        actual_mode = os.stat(cred_file).st_mode & 0o777
        if actual_mode != 0o600:
            os.chmod(cred_file, 0o600)

    try:
        _atomic_host_write()
    except PermissionError:
        # Migration fallback for legacy installs where the data dir is owned
        # by the in-container UID (pre-Commit-1 chown). Best-effort and not
        # atomic, but only fires when the host can't write directly. After
        # this lands, fresh installs always use the atomic host path.
        abs_parent = cred_file.resolve().parent
        try:
            subprocess.run(
                ["docker", "run", "--rm", "-i",
                 "-v", f"{abs_parent}:/data",
                 "alpine", "sh", "-c",
                 "cat > /data/.ro_credentials && chmod 600 /data/.ro_credentials"],
                input=payload.encode(),
                check=True, capture_output=True,
            )
        except (OSError, subprocess.CalledProcessError) as e:
            raise ConfigurationError(
                f"Failed to write {cred_file} for {db_name} (host write blocked, "
                f"docker fallback also failed): {e}"
            ) from e

    return str(cred_file)


def _write_ro_credentials(settings):
    """Write the RO password to every configured database's data volume.

    Per-DB symmetry: this is just a loop over `DB_DATA_PATH_KEYS` that calls
    `_write_ro_password_for` for each database whose data path is in
    `settings`. Both the full `setup_databases` flow and the `--add` flow
    use this so neither silently skips a DB.
    """
    ro_password = settings["ro_password"]
    written = []
    for db_name, path_key in DB_DATA_PATH_KEYS.items():
        if path_key in settings:
            written.append(_write_ro_password_for(
                db_name, settings[path_key], ro_password
            ))
    return written


# ============================================================================
# Add a single database (--add)
# ============================================================================

def _read_existing_ro_password(existing):
    """Read the RO password from an existing database's .ro_credentials file.

    The file is the password store only — username is authoritative in
    config/db/<db>.yaml. Migrates legacy `username:password` files in-place
    by rewriting them in the new password-only format on first read.

    Returns the password string, or None when no readable file is present
    (including the auth-disabled case where a missing file is normal).

    Raises:
        ConfigurationError: when `existing["auth_enabled"]` is True and a
            cred file exists but cannot be read (`OSError` on read), or
            cannot be parsed (empty / missing post-migration password).
            This replaces the silent-`None` behavior on host-side failure
            so setup-time drift is loud, not deferred to the next DB start.
    """
    auth_on = bool(existing.get("auth_enabled"))
    last_unreadable: tuple[Path, OSError] | None = None

    for path_key in ("pgdata_path", "mongo_data_path", "starrocks_data_path"):
        data_path = existing.get(path_key)
        if not data_path:
            continue
        cred_file = Path(data_path) / ".ro_credentials"
        if not cred_file.exists():
            continue
        try:
            content = cred_file.read_text().strip()
        except OSError as e:
            last_unreadable = (cred_file, e)
            continue
        if not content:
            if auth_on:
                raise ConfigurationError(
                    f"{cred_file} exists but is empty. Run "
                    "'sdp db recover-password --regenerate-ro' to rewrite it."
                )
            continue
        if ":" in content:
            # Legacy format: username:password — convert in-place.
            _, _, password = content.partition(":")
            if password:
                try:
                    cred_file.write_text(password + "\n")
                    os.chmod(cred_file, 0o600)
                except OSError:
                    pass
                return password
            if auth_on:
                raise ConfigurationError(
                    f"{cred_file} is in legacy username:password format but "
                    "the password segment is empty. Run "
                    "'sdp db recover-password --regenerate-ro' to rewrite it."
                )
            continue
        return content

    if auth_on and last_unreadable is not None:
        cred_file, err = last_unreadable
        raise ConfigurationError(
            f"Cannot read {cred_file} ({err}). The file should be host-owned "
            "and mode 0600 — if it was chowned to a container UID by an old "
            "install, run 'sudo chown $(id -u):$(id -g) <file>' or "
            "'sdp db recover-password --regenerate-ro'."
        )
    return None


def _update_override_volumes(service_name, volume_lines):
    """Add volume mounts for a service to docker-compose.override.yml.

    Preserves existing services in the override file.
    """
    override_path = ROOT / "docker-compose.override.yml"

    services = {}
    if override_path.exists():
        try:
            data = yaml.safe_load(override_path.read_text()) or {}
            services = data.get("services", {})
        except yaml.YAMLError:
            pass

    services[service_name] = {"volumes": volume_lines}

    header = "# Auto-generated by sdp db setup — extra volume mounts.\n\n"
    override_path.write_text(
        header + yaml.dump({"services": services}, default_flow_style=False, sort_keys=False)
    )


def add_database(db_name):
    """Add a single database to existing setup without reconfiguring others."""
    from social_data_pipeline.setup.utils import update_env_file

    existing = _load_existing_db_config()
    env = load_env()

    # --- Validate preconditions ---
    if not env.get("DATA_PATH"):
        print("\n  Error: No existing setup found. Run 'python sdp.py db setup' first.\n")
        sys.exit(1)

    data_path = env["DATA_PATH"]
    hw = detect_hardware()
    ram = hw["ram_gb"]
    cores = hw["cpu_cores"]
    settings = {"data_path": data_path, "databases": [db_name]}

    # --- Header ---
    db_label = {"postgres": "PostgreSQL", "mongo": "MongoDB", "starrocks": "StarRocks"}[db_name]
    config_file = CONFIG_DIR / "db" / f"{db_name}.yaml"
    is_reconfigure = config_file.exists()
    action = "Reconfigure" if is_reconfigure else "Add"
    print()
    print(f"  Social Data Pipeline - {action} {db_label}")
    print(f"  {'=' * (3 + len(action) + len(db_label) + 25)}")
    print()
    if is_reconfigure:
        print(f"  Reconfiguring {db_label} without touching other databases.")
    else:
        print(f"  Adding {db_label} to existing database setup.")
    print("  Press Enter to accept defaults shown in [brackets].")
    print()

    # --- Hardware summary ---
    section_header("Hardware Detected")
    print(f"  CPU cores: {hw['cpu_cores'] or 'unknown'}")
    print(f"  RAM:       {ram or 'unknown'} GB")
    print()

    # --- Database-specific questions ---
    if db_name == "postgres":
        section_header("Database Path")
        settings["pgdata_path"] = ask("PostgreSQL data path", existing.get("pgdata_path", f"{data_path}/database/postgres"), tag="db_pgdata_path")

        section_header("PostgreSQL Configuration")

        settings["db_name"] = ask("Database name", existing.get("db_name", "datasets"), tag="db_name")
        settings["pg_port"] = ask_int("PostgreSQL port", existing.get("pg_port", 5432), tag="db_pg_port")

        # Tablespace configuration
        if ask_bool("Use tablespaces? (spread tables across multiple disks)", bool(existing.get("tablespaces")), tag="db_tablespaces"):
            print()
            print("  Note: Check documentation for expected disk usage per data type.")
            print()
            existing_ts = existing.get("tablespaces", {})
            ts_tablespaces = {}

            if existing_ts:
                print("  Current tablespaces:")
                for name, path in existing_ts.items():
                    print(f"    {name}: {path}")
                print()
                if ask_bool("Keep existing tablespaces?", True, tag="db_ts_keep"):
                    ts_tablespaces = dict(existing_ts)

            if not ts_tablespaces:
                while True:
                    ts_name = ask("Tablespace name (e.g. nvme1)", tag="db_ts_name")
                    if not ts_name or ts_name == "pgdata":
                        print("    'pgdata' is reserved for the default PostgreSQL data directory.")
                        continue
                    ts_path = ask(f"Host path for '{ts_name}' (directory on disk)", tag="db_ts_path")
                    if ts_path:
                        ts_tablespaces[ts_name] = ts_path
                    if not ask_bool("Add another tablespace?", False, tag="db_ts_more"):
                        break

            if ts_tablespaces:
                settings["tablespaces"] = ts_tablespaces
            print()

        fs = ask_choice(
            "Filesystem for PostgreSQL data:",
            ["standard", "zfs"],
            default="standard",
            tag="db_filesystem",
        )
        settings["filesystem"] = fs

        print()
        print("  For PostgreSQL memory tuning, provide your PGTune output.")
        print("  Generate at: https://pgtune.leopard.in.ua/")
        print("    DB Version: 18 | OS: linux | DB Type: dw | Storage: ssd")
        print()
        print("  NOTE: PGTune may include 'io_method = io_uring'. If PostgreSQL fails")
        print("  to start, remove that line — io_uring is blocked on some kernels.")
        if ram:
            print(f"    Total Memory: {ram} GB | CPUs: {hw['cpu_cores']}")
        print()
        existing_pgtune = _extract_existing_pgtune()
        pgtune_choices = ["paste", "file", "skip"]
        pgtune_default = "paste"
        if existing_pgtune:
            pgtune_choices = ["keep", "paste", "file", "skip"]
            pgtune_default = "keep"
        pgtune_method = ask_choice(
            "PGTune output:",
            pgtune_choices,
            default=pgtune_default,
            tag="db_pgtune_method",
        )
        if pgtune_method == "keep":
            settings["pgtune_output"] = existing_pgtune
        elif pgtune_method == "paste":
            settings["pgtune_output"] = ask_multi_line("Paste PGTune output below:", tag="db_pgtune_paste")
        elif pgtune_method == "file":
            pgtune_path = ask("Path to file with PGTune output", tag="db_pgtune_file")
            try:
                settings["pgtune_output"] = Path(pgtune_path).expanduser().read_text()
            except (OSError, ValueError) as e:
                print(f"    Warning: Could not read {pgtune_path}: {e}")
                settings["pgtune_output"] = ""
        else:
            settings["pgtune_output"] = ""

        if fs == "zfs" and ram:
            arc_max_gb = int(ram // 2)
            arc_max_bytes = arc_max_gb * 1024 ** 3
            print()
            print("  NOTE: ZFS ARC cache competes with PostgreSQL for RAM.")
            print(f"  To avoid memory pressure, limit ARC (suggested ~{arc_max_gb}GB for this system):")
            print(f"    echo {arc_max_bytes} > /sys/module/zfs/parameters/zfs_arc_max")
            print("  Persist in /etc/modprobe.d/zfs.conf:")
            print(f"    options zfs zfs_arc_max={arc_max_bytes}")

        print()
        suggested_pg_mem = existing.get("pg_mem_limit", int(ram * 0.6) if ram else 0)
        pg_mem = ask_int("PostgreSQL container memory limit (GB, 0=unlimited)", suggested_pg_mem, tag="db_pg_mem_limit")
        if pg_mem > 0:
            settings["pg_mem_limit"] = pg_mem

    elif db_name == "mongo":
        section_header("Database Path")
        settings["mongo_data_path"] = ask("MongoDB data path", existing.get("mongo_data_path", f"{data_path}/database/mongo"), tag="db_mongo_data_path")

        section_header("MongoDB Configuration")

        settings["mongo_port"] = ask_int("MongoDB port", existing.get("mongo_port", 27017), tag="db_mongo_port")
        settings["mongo_cache_size_gb"] = ask_int("MongoDB WiredTiger cache size (GB)", existing.get("mongo_cache_size_gb", 2), tag="db_mongo_cache")

        mongo_cache = settings.get("mongo_cache_size_gb", 2)
        suggested_mongo_mem = existing.get("mongo_mem_limit", max(2, mongo_cache * 2))
        mongo_mem = ask_int("MongoDB container memory limit (GB, 0=unlimited)", suggested_mongo_mem, tag="db_mongo_mem_limit")
        if mongo_mem > 0:
            settings["mongo_mem_limit"] = mongo_mem

        print()
        print("  Pre-import file validation prevents partial ingestion of corrupt files.")
        print("  mongoimport is not atomic — without validation, truncated or malformed")
        print("  files leave partial data permanently in the database.")
        print()
        print("    full: validates every JSON line before import (one sequential read per file)")
        print("    tail: checks only the last 8KB (catches truncation, not malformed lines)")
        print("    none: skip validation")
        settings["mongo_validate"] = ask_choice(
            "Pre-import file validation",
            ["full", "tail", "none"],
            default=existing.get("mongo_validate", "full"),
            tag="db_mongo_validate",
        )

    elif db_name == "starrocks":
        section_header("Database Path")
        settings["starrocks_data_path"] = ask("StarRocks data path", existing.get("starrocks_data_path", f"{data_path}/database/starrocks"), tag="db_sr_data_path")

        section_header("StarRocks Configuration")

        settings["starrocks_port"] = ask_int("StarRocks MySQL protocol port", existing.get("starrocks_port", 9030), tag="db_sr_port")
        settings["starrocks_fe_http_port"] = ask_int("StarRocks FE HTTP port (admin/Stream Load)", existing.get("starrocks_fe_http_port", 8030), tag="db_sr_fe_http_port")

        print()
        print("  StarRocks runs FE (query planner) and BE (storage engine) in one container.")
        print("  Allocate memory to each component separately.")
        print()

        default_fe_heap = existing.get("sr_fe_jvm_heap", max(2, min(8, int(ram // 8))) if ram else 4)
        settings["sr_fe_jvm_heap"] = ask(
            "FE JVM heap size (GB)",
            default_fe_heap,
            tag="db_sr_fe_heap",
        )
        fe_heap = float(settings["sr_fe_jvm_heap"])

        print()
        if ram:
            suggested_sr_mem = int(max(fe_heap + 4, ram * 0.6))
        else:
            suggested_sr_mem = 0
        sr_mem_str = ask(
            "StarRocks container memory limit (GB, 0=unlimited)",
            existing.get("starrocks_mem_limit", suggested_sr_mem),
            tag="db_sr_mem_limit",
        )
        sr_mem = float(sr_mem_str)
        if sr_mem > 0:
            settings["starrocks_mem_limit"] = sr_mem_str

        if sr_mem > 0:
            default_be_mem = int(max(2, sr_mem - fe_heap - 2))
        elif ram:
            default_be_mem = int(max(2, ram * 0.5))
        else:
            default_be_mem = 8
        settings["sr_be_mem_limit"] = ask(
            "BE memory limit (GB)",
            existing.get("sr_be_mem_limit", default_be_mem),
            tag="db_sr_be_mem",
        )

        default_alter_workers = max(4, min(cores // 2, 10)) if cores else 4
        print()
        print("  The BE alter worker pool controls how many tablets are rebuilt in")
        print("  parallel during CREATE INDEX / ALTER TABLE (StarRocks default: 3).")
        print("  Higher values speed up index creation but use more memory — each")
        print("  worker buffers bitmaps while it runs, and high-cardinality columns")
        print("  can push the BE past its memory limit.")
        print()
        settings["sr_alter_tablet_workers"] = ask_int(
            "BE alter/schema-change worker count",
            existing.get("sr_alter_tablet_workers", default_alter_workers),
            tag="db_sr_alter_workers",
        )

        # Table compression codec, applied to newly created tables only.
        print()
        print("  Compression codec for new tables. ZSTD compresses text far")
        print("  better than LZ4 (~33% smaller) with faster scans; LZ4 is")
        print("  StarRocks' own default. Only affects tables created from now on.")
        print()
        settings["sr_compression"] = ask_choice(
            "Table compression",
            ["ZSTD", "LZ4"],
            default=existing.get("sr_compression", "ZSTD"),
            tag="db_sr_compression",
        )

        existing_sr_paths = existing.get("starrocks_storage_paths", [])
        print()
        print("  StarRocks' storage engine can spread tablets across multiple disks.")
        print(f"  The primary data path ({settings['starrocks_data_path']}) is always used.")
        print("  Add extra disks here to increase total capacity and IO parallelism.")
        print()
        if ask_bool("Add extra disks for StarRocks storage?", bool(existing_sr_paths), tag="db_sr_multidisk"):
            if existing_sr_paths:
                print()
                print("  Current extra storage paths:")
                for p in existing_sr_paths:
                    print(f"    {p}")
                print()
                if ask_bool("Keep existing extra storage paths?", True, tag="db_sr_keep_paths"):
                    settings["starrocks_storage_paths"] = list(existing_sr_paths)

            if "starrocks_storage_paths" not in settings:
                storage_paths = []
                while True:
                    sp = ask("Extra host path for StarRocks storage (e.g. /mnt/nvme1/starrocks)", tag="db_sr_storage_path")
                    if sp:
                        storage_paths.append(sp)
                    if not ask_bool("Add another extra storage path?", False, tag="db_sr_more_paths"):
                        break
                if storage_paths:
                    settings["starrocks_storage_paths"] = storage_paths

    # --- Authentication ---
    auth_already_enabled = existing.get("auth_enabled", False)

    section_header("Authentication")
    if auth_already_enabled:
        settings["auth_enabled"] = True
        ro_user = existing.get("ro_username")
        ro_pass = _read_existing_ro_password(existing)
        if ro_user:
            settings["ro_username"] = ro_user
        if ro_pass:
            settings["ro_password"] = ro_pass

        print("  Authentication is enabled on existing databases.")
        print(f"  The same settings will be applied to {db_label}:")
        print("    - auth: enabled (admin password prompted at 'sdp db start')")
        if ro_user:
            print(f"    - read-only user: {ro_user}")
        print()
    else:
        print("  Enable database authentication to require passwords for connections.")
        print("  Recommended for multi-user or remote servers.")
        print()

        if ask_bool("Enable database authentication?", False, tag="db_auth"):
            settings["auth_enabled"] = True

            print()
            print("  Choose an admin password for database access.")
            print("  This password is NOT stored anywhere — you will be prompted when needed.")
            print()
            settings["db_password"] = ask_password("Admin password", tag="db_password")

            print()
            if ask_bool("Create a read-only user? (required for MCP servers)", True, tag="db_ro_user"):
                ro_username = ask("Read-only username", "readonly", tag="db_ro_username")
                settings["ro_username"] = ro_username
                print()
                if ask_bool("Auto-generate read-only password?", True, tag="db_ro_auto_password"):
                    settings["ro_password"] = secrets.token_urlsafe(24)
                else:
                    settings["ro_password"] = ask_password("Read-only password", tag="db_ro_password")

    # --- Build file list and env updates ---
    files_to_write = []
    env_updates = {}

    if db_name == "postgres":
        files_to_write.append((
            CONFIG_DIR / "db" / "postgres.yaml",
            generate_db_postgres_yaml(settings),
        ))
        files_to_write.append((
            CONFIG_DIR / "postgres" / "postgresql.local.conf",
            generate_postgresql_local_conf(settings),
        ))
        if settings.get("auth_enabled"):
            files_to_write.append((
                CONFIG_DIR / "postgres" / "pg_hba.local.conf",
                generate_pg_hba_local_conf(settings),
            ))
        env_updates["PGDATA_PATH"] = settings["pgdata_path"]
        env_updates["DB_NAME"] = settings.get("db_name", "datasets")
        env_updates["POSTGRES_PORT"] = str(settings.get("pg_port", 5432))
        if settings.get("pg_mem_limit"):
            env_updates["POSTGRES_MEM_LIMIT"] = f"{settings['pg_mem_limit']}g"
        if settings.get("auth_enabled"):
            env_updates["POSTGRES_AUTH_ENABLED"] = "true"
            if settings.get("ro_username"):
                env_updates["POSTGRES_RO_USER"] = settings["ro_username"]

    elif db_name == "mongo":
        files_to_write.append((
            CONFIG_DIR / "db" / "mongo.yaml",
            generate_db_mongo_yaml(settings),
        ))
        env_updates["MONGO_DATA_PATH"] = settings["mongo_data_path"]
        env_updates["MONGO_PORT"] = str(settings.get("mongo_port", 27017))
        env_updates["MONGO_CACHE_SIZE_GB"] = str(settings.get("mongo_cache_size_gb", 2))
        if settings.get("mongo_mem_limit"):
            env_updates["MONGO_MEM_LIMIT"] = f"{settings['mongo_mem_limit']}g"
        if settings.get("auth_enabled"):
            env_updates["MONGO_AUTH_ENABLED"] = "true"
            env_updates["MONGO_ADMIN_USER"] = "admin"
            if settings.get("ro_username"):
                env_updates["MONGO_RO_USER"] = settings["ro_username"]

    elif db_name == "starrocks":
        files_to_write.append((
            CONFIG_DIR / "db" / "starrocks.yaml",
            generate_db_starrocks_yaml(settings),
        ))
        files_to_write.append((
            CONFIG_DIR / "starrocks" / "fe.local.conf",
            generate_starrocks_fe_conf(settings),
        ))
        files_to_write.append((
            CONFIG_DIR / "starrocks" / "be.local.conf",
            generate_starrocks_be_conf(settings),
        ))
        env_updates["STARROCKS_DATA_PATH"] = settings["starrocks_data_path"]
        env_updates["STARROCKS_PORT"] = str(settings.get("starrocks_port", 9030))
        env_updates["STARROCKS_FE_HTTP_PORT"] = str(settings.get("starrocks_fe_http_port", 8030))
        if settings.get("starrocks_mem_limit"):
            env_updates["STARROCKS_MEM_LIMIT"] = f"{settings['starrocks_mem_limit']}g"
        if settings.get("auth_enabled"):
            env_updates["STARROCKS_AUTH_ENABLED"] = "true"
            if settings.get("ro_username"):
                env_updates["STARROCKS_RO_USER"] = settings["ro_username"]

    # --- Summary and confirm ---
    print_summary(settings, files_to_write)
    print("  .env updates:")
    for k, v in env_updates.items():
        print(f"    {k}={v}")
    print()

    if not ask_bool("Apply these changes?", True, tag="db_add_confirm"):
        print("\n  Aborted. No changes made.\n")
        sys.exit(0)

    # --- Write files ---
    print()
    write_files(files_to_write)

    # Update .env (merge, not rewrite)
    update_env_file(env_updates)
    print("  Updated: .env")

    # Handle docker-compose.override.yml for tablespace / SR storage volumes
    if db_name == "postgres" and settings.get("tablespaces"):
        volumes = [
            f"{host_path}:/data/tablespace/{ts_name}"
            for ts_name, host_path in settings["tablespaces"].items()
            if ts_name != "pgdata"
        ]
        if volumes:
            _update_override_volumes("postgres", volumes)
            print("  Updated: docker-compose.override.yml")

    if db_name == "starrocks" and settings.get("starrocks_storage_paths"):
        volumes = [
            f"{host_path}:/data/deploy/starrocks/be/storage_{i}"
            for i, host_path in enumerate(settings["starrocks_storage_paths"])
        ]
        if volumes:
            _update_override_volumes("starrocks", volumes)
            print("  Updated: docker-compose.override.yml")

    # Write RO credentials to new database data volume
    if settings.get("auth_enabled") and settings.get("ro_username") and settings.get("ro_password"):
        cred_files = _write_ro_credentials(settings)
        for cf in cred_files:
            print(f"  Written: {cf} (chmod 600)")

    # --- Done ---
    done_verb = "reconfigured" if is_reconfigure else "added to"
    print(f"\n  Done! {db_label} has been {done_verb} the database configuration.")

    if settings.get("auth_enabled"):
        rw_names = {"postgres": "postgres", "mongo": "admin", "starrocks": "root"}
        print(f"\n  {db_label} admin user: {rw_names[db_name]}")
        if settings.get("ro_username"):
            print(f"  Read-only user: {settings['ro_username']}")
        print("\n  IMPORTANT: Remember your admin password — it is not stored anywhere.")
        print("  If lost, recover with: python sdp.py db recover-password")

    print("\n  Next steps:")
    print("    python sdp.py db status           # Check database status and info")
    print("    python sdp.py db setup-llm        # Configure MCP + jobs scheduler for agentic AI access (optional)")
    print("    python sdp.py source add <name>   # Add a data source")
    print()


# ============================================================================
# Main
# ============================================================================

def main():
    print()
    print("  Social Data Pipeline - Database Configuration")
    print("  =============================================")
    print()
    print("  Configure database infrastructure (PostgreSQL, MongoDB, StarRocks).")
    print("  Press Enter to accept defaults shown in [brackets].")
    print()

    hw = detect_hardware()
    settings = run_questionnaire(hw)

    # Build file list
    files_to_write = []

    # .env
    files_to_write.append((ROOT / ".env", generate_env(settings)))

    # config/db/postgres.yaml
    if "postgres" in settings["databases"]:
        files_to_write.append((
            CONFIG_DIR / "db" / "postgres.yaml",
            generate_db_postgres_yaml(settings),
        ))
        # postgresql.local.conf
        files_to_write.append((
            CONFIG_DIR / "postgres" / "postgresql.local.conf",
            generate_postgresql_local_conf(settings),
        ))

        # pg_hba.local.conf (auth-enabled only)
        if settings.get("auth_enabled"):
            files_to_write.append((
                CONFIG_DIR / "postgres" / "pg_hba.local.conf",
                generate_pg_hba_local_conf(settings),
            ))

    # config/db/mongo.yaml
    if "mongo" in settings["databases"]:
        files_to_write.append((
            CONFIG_DIR / "db" / "mongo.yaml",
            generate_db_mongo_yaml(settings),
        ))

    # config/db/starrocks.yaml + conf files
    if "starrocks" in settings["databases"]:
        files_to_write.append((
            CONFIG_DIR / "db" / "starrocks.yaml",
            generate_db_starrocks_yaml(settings),
        ))
        files_to_write.append((
            CONFIG_DIR / "starrocks" / "fe.local.conf",
            generate_starrocks_fe_conf(settings),
        ))
        files_to_write.append((
            CONFIG_DIR / "starrocks" / "be.local.conf",
            generate_starrocks_be_conf(settings),
        ))

    # docker-compose.override.yml (tablespace volumes / SR multi-disk)
    if "tablespaces" in settings or settings.get("starrocks_storage_paths"):
        override_content = generate_docker_compose_override(settings)
        if override_content:
            files_to_write.append((
                ROOT / "docker-compose.override.yml",
                override_content,
            ))

    # Summary and confirm
    print_summary(settings, files_to_write)

    if not ask_bool("Write these files?", True, tag="db_write_files"):
        print("\n  Aborted. No files written.\n")
        sys.exit(0)

    print()
    write_files(files_to_write)

    # Create export directory on host (avoids Docker creating it as root)
    if "export_path" in settings:
        qo_path = Path(settings["export_path"])
        if not qo_path.is_absolute():
            qo_path = ROOT / qo_path
        qo_path.mkdir(parents=True, exist_ok=True)
        print(f"  Created:   {qo_path}")

    # Write RO user credentials to database data volumes
    if settings.get("auth_enabled") and settings.get("ro_username"):
        cred_files = _write_ro_credentials(settings)
        for cf in cred_files:
            print(f"  Written:   {cf} (chmod 600)")

    print("\n  Done! Database configuration has been generated.")

    if settings.get("auth_enabled"):
        rw_names = {"postgres": "postgres", "mongo": "admin", "starrocks": "root"}
        print("\n  Database users (admin / read-write):")
        for db in settings["databases"]:
            if db in rw_names:
                print(f"    {db:12s} {rw_names[db]}")
        if settings.get("ro_username"):
            print(f"\n  Read-only user for all databases: {settings['ro_username']} (auto-generated password)")
        print("\n  The admin password is the same for all databases.")
        print("\n  IMPORTANT: Remember your admin password — it is not stored anywhere.")
        print("  If lost, recover with: python sdp.py db recover-password")

    print("\n  Next steps:")
    print("    python sdp.py db status           # Check database status and info")
    print("    python sdp.py db setup-llm        # Configure MCP + jobs scheduler for agentic AI access (optional)")
    print("    python sdp.py source add <name>   # Add a data source")
    print()

    return settings
