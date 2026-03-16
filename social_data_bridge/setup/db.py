"""Database configuration for Social Data Bridge.

Configures PostgreSQL and MongoDB settings (port, tablespaces, PGTune, cache).
Generates .env, config/db/*.yaml, postgresql.local.conf, docker-compose.override.yml.

This is a global, one-time configuration independent of any source.
"""

import secrets
import sys
from getpass import getpass
from pathlib import Path

try:
    import yaml
except ImportError:
    print("Error: PyYAML is required. Install with: pip install pyyaml")
    sys.exit(1)

from social_data_bridge.setup.utils import (
    ROOT, CONFIG_DIR,
    detect_hardware,
    ask, ask_int, ask_bool, ask_choice, ask_multi_select, ask_multi_line,
    section_header, write_files,
)


def ask_password(label: str) -> str:
    """Prompt for a password with confirmation. Uses getpass for hidden input."""
    while True:
        pw1 = getpass(f"  {label}: ")
        if not pw1:
            print("    Password cannot be empty.")
            continue
        pw2 = getpass(f"  Confirm {label.lower()}: ")
        if pw1 != pw2:
            print("    Passwords do not match. Try again.")
            continue
        return pw1


def generate_password() -> str:
    """Generate a random 32-character URL-safe password."""
    return secrets.token_urlsafe(24)


# ============================================================================
# Interactive questionnaire
# ============================================================================

def run_questionnaire(hw):
    """Run the database configuration questionnaire. Returns settings dict."""
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
    data_path = ask("Data base path", "./data")
    settings["data_path"] = data_path

    # ---- Database selection ----
    section_header("Database Selection")

    all_databases = ["postgres", "mongo"]
    databases = ask_multi_select("Databases:", all_databases, ["postgres"])
    settings["databases"] = databases

    has_postgres = "postgres" in databases
    has_mongo = "mongo" in databases

    # ---- Paths (database data dirs) ----
    section_header("Database Paths")
    if has_postgres:
        settings["pgdata_path"] = ask("PostgreSQL data path", f"{data_path}/database/postgres")
    if has_mongo:
        settings["mongo_data_path"] = ask("MongoDB data path", f"{data_path}/database/mongo")

    # ---- PostgreSQL ----
    if has_postgres:
        section_header("PostgreSQL Configuration")

        settings["db_name"] = ask("Database name", "datasets")
        settings["pg_port"] = ask_int("PostgreSQL port", 5432)

        # Tablespace configuration
        if ask_bool("Use tablespaces? (spread tables across multiple disks)", False):
            print()
            print("  Note: Check documentation for expected disk usage per data type.")
            print()
            ts_tablespaces = {}
            while True:
                ts_name = ask("Tablespace name (e.g. nvme1)")
                if not ts_name or ts_name == "pgdata":
                    print("    'pgdata' is reserved for the default PostgreSQL data directory.")
                    continue
                ts_path = ask(f"Host path for '{ts_name}' (directory on disk)")
                if ts_path:
                    ts_tablespaces[ts_name] = ts_path
                if not ask_bool("Add another tablespace?", False):
                    break

            if ts_tablespaces:
                settings["tablespaces"] = ts_tablespaces
            print()

        fs = ask_choice(
            "Filesystem for PostgreSQL data:",
            ["standard", "zfs"],
            default="standard",
        )
        settings["filesystem"] = fs

        print()
        print("  For PostgreSQL memory tuning, provide your PGTune output.")
        print("  Generate at: https://pgtune.leopard.in.ua/")
        print(f"    DB Version: 18 | OS: linux | DB Type: dw | Storage: ssd")
        if hw["ram_gb"]:
            print(f"    Total Memory: {hw['ram_gb']} GB | CPUs: {hw['cpu_cores']}")
        print()
        pgtune_method = ask_choice(
            "PGTune output:",
            ["paste", "file", "skip"],
            default="paste",
        )
        if pgtune_method == "paste":
            settings["pgtune_output"] = ask_multi_line("Paste PGTune output below:")
        elif pgtune_method == "file":
            pgtune_path = ask("Path to file with PGTune output")
            try:
                settings["pgtune_output"] = Path(pgtune_path).expanduser().read_text()
            except (OSError, ValueError) as e:
                print(f"    Warning: Could not read {pgtune_path}: {e}")
                settings["pgtune_output"] = ""
        else:
            settings["pgtune_output"] = ""

    # ---- MongoDB ----
    if has_mongo:
        section_header("MongoDB Configuration")

        settings["mongo_port"] = ask_int("MongoDB port", 27017)
        settings["mongo_cache_size_gb"] = ask_int("MongoDB WiredTiger cache size (GB)", 2)

    # ---- Authentication ----
    if has_postgres or has_mongo:
        section_header("Authentication")
        print("  Enable database authentication to require passwords for connections.")
        print("  Recommended for multi-user or remote servers.")
        print()

        if ask_bool("Enable database authentication?", False):
            settings["auth_enabled"] = True

            print()
            print("  Choose an admin password for database access.")
            print("  This password is NOT stored anywhere — you will be prompted when needed.")
            print()
            settings["db_password"] = ask_password("Admin password")

            print()
            if ask_bool("Create a read-only user without password? (for pgAdmin, Compass, etc.)", True):
                ro_username = ask("Read-only username", "readonly")
                while ro_username == "readonly_mcp":
                    print("    'readonly_mcp' is reserved for MCP servers. Choose another name.")
                    ro_username = ask("Read-only username", "readonly")
                settings["ro_username"] = ro_username

    return settings


# ============================================================================
# Config generators
# ============================================================================

def generate_env(settings):
    """Generate .env file content with database and global settings."""
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

    if "mongo_data_path" in settings:
        lines += [
            "",
            "# ===== MONGODB CONFIGURATION =====",
            f"MONGO_DATA_PATH={settings['mongo_data_path']}",
            f"MONGO_PORT={settings.get('mongo_port', 27017)}",
            f"MONGO_CACHE_SIZE_GB={settings.get('mongo_cache_size_gb', 2)}",
        ]

    if settings.get("auth_enabled"):
        lines += [
            "",
            "# ===== AUTHENTICATION =====",
            "POSTGRES_AUTH_ENABLED=true",
            "MONGO_AUTH_ENABLED=true",
            "MONGO_ADMIN_USER=admin",
        ]
        if settings.get("ro_username"):
            lines += [
                f"POSTGRES_RO_USER={settings['ro_username']}",
                f"MONGO_RO_USER={settings['ro_username']}",
            ]

    return "\n".join(lines) + "\n"


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
    }
    if settings.get("auth_enabled"):
        config["auth"] = True
    if settings.get("ro_username"):
        config["ro_username"] = settings["ro_username"]
    return yaml.dump(config, default_flow_style=False, sort_keys=False)


def generate_docker_compose_override(settings):
    """Generate docker-compose.override.yml with tablespace volume mounts."""
    tablespaces = settings.get("tablespaces", {})

    volume_lines = []
    for ts_name, host_path in tablespaces.items():
        if ts_name != "pgdata":
            volume_lines.append(f"      - {host_path}:/data/tablespace/{ts_name}")

    if not volume_lines:
        return None

    volumes_str = "\n".join(volume_lines)
    return (
        "# Auto-generated by sdb db setup — tablespace volume mounts.\n"
        "# Each volume maps a host directory to a container path used by CREATE TABLESPACE.\n"
        "\n"
        "services:\n"
        "  postgres:\n"
        "    volumes:\n"
        f"{volumes_str}\n"
    )


def generate_postgresql_local_conf(settings):
    """Generate postgresql.local.conf by copying base, toggling ZFS, appending pgtune."""
    base_path = CONFIG_DIR / "postgres" / "postgresql.conf"
    try:
        base_content = base_path.read_text()
    except PermissionError:
        print(f"\n  Error: Cannot read {base_path}")
        print(f"  The config/ directory may be missing the execute bit (needed for traversal).")
        print(f"  Try: chmod 755 config/ config/*/")
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
    ro_username = settings.get("ro_username")

    lines = [
        "# Auto-generated by sdb db setup — authentication enabled",
        "# TYPE  DATABASE  USER  ADDRESS  METHOD",
        "",
        "# Local (unix socket) — trust for container-internal access",
        "local   all       all                    trust",
        "",
        "# Localhost IPv4",
        "host    all       all   127.0.0.1/32     scram-sha-256",
    ]

    if ro_username:
        lines += [
            "",
            f"# Read-only user — trust (no password) from Docker networks",
            f"host    all       {ro_username}   172.16.0.0/12    trust",
            f"host    all       {ro_username}   192.168.0.0/16   trust",
        ]

    lines += [
        "",
        "# All other users — require password from Docker networks",
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

    databases = settings["databases"]
    print(f"  Databases:   {', '.join(databases)}")
    print()

    if "postgres" in databases:
        print(f"  PostgreSQL:")
        print(f"    DB name:             {settings.get('db_name', 'datasets')}")
        print(f"    Port:                {settings.get('pg_port', 5432)}")
        print(f"    Data path:           {settings.get('pgdata_path', './data/database/postgres')}")
        print(f"    Filesystem:          {settings.get('filesystem', 'standard')}")
        if "tablespaces" in settings:
            print(f"    Tablespaces:")
            for ts_name, ts_path in settings["tablespaces"].items():
                print(f"      {ts_name}: {ts_path}")
        print(f"    PGTune:              {'provided' if settings.get('pgtune_output') else 'not provided'}")
        print()

    if "mongo" in databases:
        print(f"  MongoDB:")
        print(f"    Port:                {settings.get('mongo_port', 27017)}")
        print(f"    Cache size:          {settings.get('mongo_cache_size_gb', 2)} GB")
        print(f"    Data path:           {settings.get('mongo_data_path', './data/database/mongo')}")
        print()

    if settings.get("auth_enabled"):
        print(f"  Authentication:  enabled")
        if settings.get("ro_username"):
            print(f"    RO user:         {settings['ro_username']} (no password)")
        print()

    print("  Files to write:")
    for path, _ in files_to_write:
        rel = path.relative_to(ROOT)
        exists = path.exists()
        status = " (exists, will backup)" if exists else ""
        print(f"    {rel}{status}")
    print()


# ============================================================================
# Main
# ============================================================================

def main():
    print()
    print("  Social Data Bridge - Database Configuration")
    print("  =============================================")
    print()
    print("  Configure database infrastructure (PostgreSQL, MongoDB).")
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

    # docker-compose.override.yml (tablespace volumes)
    if "tablespaces" in settings:
        override_content = generate_docker_compose_override(settings)
        if override_content:
            files_to_write.append((
                ROOT / "docker-compose.override.yml",
                override_content,
            ))

    # Summary and confirm
    print_summary(settings, files_to_write)

    if not ask_bool("Write these files?", True):
        print("\n  Aborted. No files written.\n")
        sys.exit(0)

    print()
    write_files(files_to_write)

    print(f"\n  Done! Database configuration has been generated.")

    if settings.get("auth_enabled"):
        print(f"\n  IMPORTANT: Remember your admin password — it is not stored anywhere.")
        print(f"  If lost, recover with: python sdb.py db recover-password")

    print(f"\n  Next step:")
    print(f"    python sdb.py source add <name>   # Add a data source")
    print()

    return settings
