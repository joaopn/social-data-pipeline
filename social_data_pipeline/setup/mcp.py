"""MCP server configuration for Social Data Pipeline.

Configures MCP (Model Context Protocol) servers for PostgreSQL and MongoDB.
Generates config/db/mcp.yaml and updates .env with MCP port/access settings.

Requires databases to be configured first via `sdp db setup`.
"""

import os
import secrets
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    print("Error: PyYAML is required. Install with: pip install pyyaml")
    sys.exit(1)

from social_data_pipeline.setup.utils import (
    ROOT, CONFIG_DIR,
    ask_int, ask_bool,
    section_header, write_files, update_env_file, load_db_setup,
)


# ============================================================================
# Interactive questionnaire
# ============================================================================

def run_questionnaire(db_setup):
    """Run the MCP configuration questionnaire. Returns settings dict."""
    settings = {}
    databases = db_setup["databases"]

    section_header("MCP Server Selection")

    print(f"  Configured databases: {', '.join(databases)}")
    print()

    # ---- PostgreSQL MCP ----
    if "postgres" in databases:
        if ask_bool("Enable PostgreSQL MCP server?", True):
            settings["postgres_mcp_enabled"] = True
            settings["postgres_mcp_port"] = ask_int("PostgreSQL MCP SSE port", 8000)
            write_access = ask_bool("Allow write access? (default: read-only)", False)
            settings["postgres_mcp_access_mode"] = "unrestricted" if write_access else "restricted"
        else:
            settings["postgres_mcp_enabled"] = False

    # ---- MongoDB MCP ----
    if "mongo" in databases:
        if ask_bool("Enable MongoDB MCP server?", True):
            settings["mongo_mcp_enabled"] = True
            settings["mongo_mcp_port"] = ask_int("MongoDB MCP SSE port", 3000)
            write_access = ask_bool("Allow write access? (default: read-only)", False)
            settings["mongo_mcp_read_only"] = not write_access
        else:
            settings["mongo_mcp_enabled"] = False

    # Track auth status from db_setup for credential generation
    settings["postgres_auth"] = db_setup.get("postgres_auth", False)
    settings["mongo_auth"] = db_setup.get("mongo_auth", False)

    return settings


# ============================================================================
# Config generators
# ============================================================================

def generate_mcp_yaml(settings):
    """Generate config/db/mcp.yaml content."""
    config = {}

    if settings.get("postgres_mcp_enabled"):
        config["postgres"] = {
            "enabled": True,
            "port": settings["postgres_mcp_port"],
            "access_mode": settings["postgres_mcp_access_mode"],
        }
        if settings.get("postgres_auth"):
            config["postgres"]["mcp_user"] = "readonly_mcp"

    if settings.get("mongo_mcp_enabled"):
        config["mongo"] = {
            "enabled": True,
            "port": settings["mongo_mcp_port"],
            "read_only": settings["mongo_mcp_read_only"],
        }
        if settings.get("mongo_auth"):
            config["mongo"]["mcp_user"] = "readonly_mcp"

    return yaml.dump(config, default_flow_style=False, sort_keys=False)


def write_mcp_credentials(settings):
    """Write MCP readonly credentials to database data volumes.

    Creates .mcp_credentials files (chmod 600) in the database data paths.
    Format: username:password (single line).
    """
    mcp_password = secrets.token_urlsafe(24)

    # Read data paths from .env
    env_path = ROOT / ".env"
    env_vars = {}
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                env_vars[key.strip()] = value.strip()

    credentials = f"readonly_mcp:{mcp_password}"
    written = []

    def _write_cred_file(data_path: Path, label: str):
        cred_file = data_path / ".mcp_credentials"
        cred_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            cred_file.write_text(credentials + "\n")
            os.chmod(cred_file, 0o600)
        except PermissionError:
            # Directory owned by container user (root) — write via docker
            import subprocess
            abs_parent = cred_file.resolve().parent
            subprocess.run(
                ["docker", "run", "--rm", "-i",
                 "-v", f"{abs_parent}:/data",
                 "alpine", "sh", "-c",
                 "cat > /data/.mcp_credentials && chmod 600 /data/.mcp_credentials"],
                input=(credentials + "\n").encode(),
                check=True, capture_output=True,
            )
        written.append(str(cred_file))

    if settings.get("postgres_mcp_enabled") and settings.get("postgres_auth"):
        pgdata_path = Path(env_vars.get("PGDATA_PATH", "./data/database/postgres"))
        _write_cred_file(pgdata_path, "PostgreSQL")

    if settings.get("mongo_mcp_enabled") and settings.get("mongo_auth"):
        mongo_data_path = Path(env_vars.get("MONGO_DATA_PATH", "./data/database/mongo"))
        _write_cred_file(mongo_data_path, "MongoDB")

    return mcp_password, written


# ============================================================================
# Summary
# ============================================================================

def print_summary(settings, files_to_write):
    """Print a summary of MCP settings and files to be written."""
    section_header("MCP Configuration Summary")

    if settings.get("postgres_mcp_enabled"):
        print(f"  PostgreSQL MCP:")
        print(f"    Port:        {settings['postgres_mcp_port']}")
        print(f"    Access:      {settings['postgres_mcp_access_mode']}")
        print(f"    Endpoint:    http://localhost:{settings['postgres_mcp_port']}/sse")
        print()

    if settings.get("mongo_mcp_enabled"):
        print(f"  MongoDB MCP:")
        print(f"    Port:        {settings['mongo_mcp_port']}")
        print(f"    Read-only:   {settings['mongo_mcp_read_only']}")
        print(f"    Endpoint:    http://localhost:{settings['mongo_mcp_port']}/mcp")
        print()

    if not settings.get("postgres_mcp_enabled") and not settings.get("mongo_mcp_enabled"):
        print("  No MCP servers enabled.")
        print()
        return

    print("  Files to write:")
    for path, _ in files_to_write:
        rel = path.relative_to(ROOT)
        exists = path.exists()
        status = " (exists, will backup)" if exists else ""
        print(f"    {rel}{status}")
    print(f"    .env (update)")
    print()


# ============================================================================
# Main
# ============================================================================

def main():
    print()
    print("  Social Data Pipeline - MCP Server Configuration")
    print("  ===============================================")
    print()
    print("  Configure MCP servers for AI tool access to databases.")
    print("  Press Enter to accept defaults shown in [brackets].")
    print()

    # Check that databases are configured
    db_setup = load_db_setup()
    if not db_setup or not db_setup.get("databases"):
        print("  Error: No databases configured. Run first: python sdp.py db setup")
        sys.exit(1)

    settings = run_questionnaire(db_setup)

    # Check if anything was enabled
    if not settings.get("postgres_mcp_enabled") and not settings.get("mongo_mcp_enabled"):
        print("\n  No MCP servers enabled. Nothing to write.\n")
        sys.exit(0)

    # Build file list
    files_to_write = [(CONFIG_DIR / "db" / "mcp.yaml", generate_mcp_yaml(settings))]

    # Summary and confirm
    print_summary(settings, files_to_write)

    if not ask_bool("Write these files?", True):
        print("\n  Aborted. No files written.\n")
        sys.exit(0)

    print()
    write_files(files_to_write)

    # Write MCP credentials if any database has auth enabled
    has_auth = settings.get("postgres_auth") or settings.get("mongo_auth")
    if has_auth:
        mcp_password, cred_files = write_mcp_credentials(settings)
        for cf in cred_files:
            print(f"  Written:   {cf} (chmod 600)")

    # Update .env with MCP settings
    env_updates = {}
    if settings.get("postgres_mcp_enabled"):
        env_updates["POSTGRES_MCP_PORT"] = str(settings["postgres_mcp_port"])
        env_updates["POSTGRES_MCP_ACCESS_MODE"] = settings["postgres_mcp_access_mode"]
        if settings.get("postgres_auth"):
            env_updates["POSTGRES_MCP_USER"] = "readonly_mcp"
    if settings.get("mongo_mcp_enabled"):
        env_updates["MONGO_MCP_PORT"] = str(settings["mongo_mcp_port"])
        env_updates["MONGO_MCP_READ_ONLY"] = str(settings["mongo_mcp_read_only"]).lower()
        if settings.get("mongo_auth"):
            env_updates["MONGO_MCP_USER"] = "readonly_mcp"

    if env_updates:
        update_env_file(env_updates)
        print(f"  Updated:   .env")

    print(f"\n  Done! MCP servers have been configured.")
    print(f"\n  Start databases with MCP servers:")
    print(f"    python sdp.py db start")

    # Print client configuration instructions
    print()
    section_header("MCP Client Configuration")
    print("  Add the following to your MCP client config (e.g. VS Code mcp.json,")
    print("  Cursor, Claude Desktop). Replace <host> with your server address.")
    print("  Exact config format varies between clients.")
    print()

    if settings.get("postgres_mcp_enabled"):
        pg_port = settings["postgres_mcp_port"]
        print(f"  PostgreSQL MCP (SSE):")
        print(f"    URL: http://<host>:{pg_port}/sse")
        print()

    if settings.get("mongo_mcp_enabled"):
        mongo_port = settings["mongo_mcp_port"]
        print(f"  MongoDB MCP (Streamable HTTP):")
        print(f"    URL: http://<host>:{mongo_port}/mcp")
        print()

    print("  Example VS Code mcp.json:")
    print()
    servers = {}
    if settings.get("postgres_mcp_enabled"):
        pg_port = settings["postgres_mcp_port"]
        servers["postgres"] = {
            "url": f"http://<host>:{pg_port}/sse",
            "type": "sse",
        }
    if settings.get("mongo_mcp_enabled"):
        mongo_port = settings["mongo_mcp_port"]
        servers["mongodb"] = {
            "url": f"http://<host>:{mongo_port}/mcp",
            "type": "http",
        }

    import json
    example = json.dumps({"servers": servers}, indent=4)
    for line in example.splitlines():
        print(f"    {line}")
    print()

    return settings
