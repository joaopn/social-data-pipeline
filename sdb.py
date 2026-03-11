#!/usr/bin/env python3
"""Social Data Bridge CLI.

Unified entrypoint for configuration, database management, and pipeline execution.

Usage:
    sdb.py setup               Interactive configuration (core → classifiers → platform)
    sdb.py setup-reddit        Configure Reddit platform (fields, indexes, schema)
    sdb.py add-classifiers     Configure classifiers (Lingua, GPU transformers)
    sdb.py status              Show configuration and ingestion status
    sdb.py start               Start the PostgreSQL database
    sdb.py stop                Stop the PostgreSQL database
    sdb.py run <profile>       Run a pipeline profile (parse, ml_cpu, ml, postgres_ingest, postgres_ml)
                               Use --build to rebuild the image before running
    sdb.py unsetup             Remove all configuration (and optionally database)
"""

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


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


def load_state():
    """Load setup_state.yaml. Returns dict or None."""
    state_path = ROOT / "config" / "setup_state.yaml"
    if not state_path.exists():
        return None
    try:
        import yaml
        return yaml.safe_load(state_path.read_text())
    except Exception:
        return None


def docker_compose(*args):
    """Run docker compose with the given arguments."""
    cmd = ["docker", "compose"] + list(args)
    print(f"  $ {' '.join(cmd)}")
    return subprocess.run(cmd, cwd=ROOT)


# ============================================================================
# sdb setup
# ============================================================================

def cmd_setup():
    """Run interactive configuration: core → classifiers → platform."""
    from social_data_bridge.setup.core import main as core_main
    from social_data_bridge.setup.utils import ask_bool

    # Step 1: Core setup (always required)
    print("  Step 1: Core configuration\n")
    core_main()

    state = load_state()
    if not state:
        print("\n  Error: Core setup did not create state file.\n")
        return 1

    platform = state.get("platform")
    profiles = state.get("profiles", [])
    has_classifiers = "ml_cpu" in profiles or "ml" in profiles

    # Step 2: Classifier configuration (optional)
    if has_classifiers:
        print("\n  Step 2: Classifier configuration (optional)\n")
        try:
            if ask_bool("Customize classifier settings?", False):
                from social_data_bridge.setup.classifiers import main as classifiers_main
                classifiers_main()
            else:
                print("  Skipped. Using default classifier configuration.\n")
        except KeyboardInterrupt:
            print("\n")
            return 1

    # Step 3: Platform-specific configuration
    if platform == "reddit":
        step_n = 3 if has_classifiers else 2
        print(f"\n  Step {step_n}: Reddit platform configuration\n")
        from social_data_bridge.setup.reddit import main as reddit_main
        reddit_main()

    return 0


def cmd_setup_reddit():
    """Run Reddit platform configuration."""
    from social_data_bridge.setup.reddit import main
    main()
    return 0


def cmd_add_classifiers():
    """Run classifier configuration."""
    from social_data_bridge.setup.classifiers import main
    main()
    return 0


# ============================================================================
# sdb status
# ============================================================================

def cmd_status():
    """Show configuration and ingestion status."""
    state = load_state()
    env = load_env()

    # Configuration status
    print()
    print("  Social Data Bridge - Status")
    print("  ============================")

    if not state:
        print("\n  Not configured. Run: python sdb.py setup\n")
        return

    platform = state.get("platform", "unknown")
    data_types = state.get("data_types", [])
    profiles = state.get("profiles", [])

    print(f"\n  Platform:    {platform}")
    print(f"  Data types:  {', '.join(data_types)}")
    print(f"  Profiles:    {', '.join(profiles)}")

    # Database status
    has_postgres = any(p.startswith("postgres") for p in profiles)
    if has_postgres:
        pgdata_path = env.get("PGDATA_PATH", "./data/database")
        # Resolve relative to ROOT
        pgdata = Path(pgdata_path)
        if not pgdata.is_absolute():
            pgdata = ROOT / pgdata

        print(f"\n  Database:")
        print(f"    Path:      {pgdata_path}")
        print(f"    Name:      {env.get('DB_NAME', 'datasets')}")
        print(f"    Port:      {env.get('POSTGRES_PORT', '5432')}")

        # Check if postgres is running
        result = subprocess.run(
            ["docker", "compose", "ps", "--format", "json", "--filter", "status=running"],
            capture_output=True, text=True, cwd=ROOT,
        )
        pg_running = False
        if result.returncode == 0 and result.stdout.strip():
            for line in result.stdout.strip().splitlines():
                try:
                    container = json.loads(line)
                    if container.get("Service") == "postgres":
                        pg_running = True
                        break
                except json.JSONDecodeError:
                    pass
        print(f"    Running:   {'yes' if pg_running else 'no'}")

        # Ingestion state
        state_dir = pgdata / "state_tracking"
        if state_dir.exists():
            state_files = sorted(state_dir.glob("*.json"))
            if state_files:
                print(f"\n  Ingestion status:")
                for sf in state_files:
                    try:
                        sdata = json.loads(sf.read_text())
                    except (json.JSONDecodeError, OSError):
                        continue

                    # Parse filename: {platform}_postgres_{profile}_{data_type}.json
                    name = sf.stem
                    processed = sdata.get("processed", [])
                    failed = sdata.get("failed", [])
                    in_progress = sdata.get("in_progress")
                    last_updated = sdata.get("last_updated", "")

                    # Derive a friendly label
                    parts = name.split("_postgres_")
                    if len(parts) == 2:
                        label = parts[1].replace("_", " / ", 1)
                    else:
                        label = name

                    count = len(processed)
                    latest = processed[-1] if processed else "-"

                    line = f"    {label}: {count} datasets"
                    if latest != "-":
                        line += f" (latest: {latest})"
                    if in_progress:
                        line += f" [in progress: {in_progress}]"
                    if failed:
                        line += f" [{len(failed)} failed]"
                    print(line)

                    if last_updated:
                        print(f"      last updated: {last_updated}")
            else:
                print(f"\n  No ingestion data yet.")
        else:
            print(f"\n  No ingestion data yet.")

    print()


# ============================================================================
# sdb start / stop
# ============================================================================

def cmd_start():
    """Start the PostgreSQL database."""
    result = docker_compose("--profile", "postgres", "up", "-d")
    return result.returncode


def cmd_stop():
    """Stop the PostgreSQL database."""
    result = docker_compose("--profile", "postgres", "down")
    return result.returncode


# ============================================================================
# sdb unsetup
# ============================================================================

# All config files that setup may generate (relative to ROOT)
GENERATED_CONFIG_FILES = [
    "config/setup_state.yaml",
    "config/parse/user.yaml",
    "config/ml_cpu/user.yaml",
    "config/ml/user.yaml",
    "config/postgres/user.yaml",
    "config/postgres_ml/user.yaml",
    "config/postgres/postgresql.local.conf",
    "config/platforms/reddit/user.yaml",
    "config/platforms/generic/user.yaml",
    "docker-compose.override.yml",
    ".env",
]


def cmd_unsetup():
    """Remove all setup-generated configuration and optionally the database."""
    import shutil

    env = load_env()

    print()
    print("  Social Data Bridge - Unsetup")
    print("  =============================")
    print()

    # --- Phase 1: Remove generated config files ---
    removed = []
    for rel in GENERATED_CONFIG_FILES:
        path = ROOT / rel
        if path.exists():
            removed.append(rel)

    # Also find .bak files created by write_files()
    backups = []
    for rel in GENERATED_CONFIG_FILES:
        bak = ROOT / (rel + ".bak")
        if bak.exists():
            backups.append(rel + ".bak")

    if removed or backups:
        print("  Generated config files to remove:")
        for rel in removed:
            print(f"    {rel}")
        for rel in backups:
            print(f"    {rel}")
        print()
    else:
        print("  No generated config files found.\n")

    # --- Phase 2: Database removal (double confirmation) ---
    pgdata_path = env.get("PGDATA_PATH", "")
    db_removed = False
    if pgdata_path:
        pgdata = Path(pgdata_path)
        if not pgdata.is_absolute():
            pgdata = ROOT / pgdata
        if pgdata.exists():
            print(f"  Database directory: {pgdata_path}")
            print()
            confirm1 = input("  Delete the PostgreSQL database? This CANNOT be undone [y/N]: ").strip().lower()
            if confirm1 in ("y", "yes"):
                confirm2 = input("  Are you SURE? All database data will be permanently lost [y/N]: ").strip().lower()
                if confirm2 in ("y", "yes"):
                    # Stop postgres first
                    print()
                    print("  Stopping PostgreSQL...")
                    docker_compose("--profile", "postgres", "down")
                    print(f"  Removing {pgdata}...")
                    shutil.rmtree(pgdata)
                    db_removed = True
                    print("  Database removed.")
                else:
                    print("  Database removal skipped.")
            else:
                print("  Database removal skipped.")
            print()

    # --- Phase 3: Actually remove config files ---
    if removed or backups:
        for rel in removed:
            (ROOT / rel).unlink()
        for rel in backups:
            (ROOT / rel).unlink()
        print(f"  Removed {len(removed) + len(backups)} config file(s).")
    else:
        print("  Nothing to remove.")

    # --- Phase 4: Print data file locations for manual deletion ---
    data_paths = {}
    for key, label in [
        ("DUMPS_PATH", "Dumps (.zst files)"),
        ("EXTRACTED_PATH", "Extracted JSON"),
        ("CSV_PATH", "Parsed CSV"),
        ("OUTPUT_PATH", "ML classifier output"),
    ]:
        val = env.get(key)
        if val:
            data_paths[label] = val
    if not db_removed and pgdata_path:
        data_paths["PostgreSQL data"] = pgdata_path

    if data_paths:
        print()
        print("  Data files were NOT removed. Delete manually if needed:")
        for label, path in data_paths.items():
            print(f"    {label}: {path}")

    print()
    print("  Unsetup complete.\n")
    return 0


# ============================================================================
# sdb run
# ============================================================================

VALID_PROFILES = ["parse", "ml_cpu", "ml", "postgres_ingest", "postgres_ml"]


def cmd_run(profile, build=False):
    """Run a pipeline profile."""
    if profile not in VALID_PROFILES:
        print(f"  Error: Unknown profile '{profile}'.")
        print(f"  Valid profiles: {', '.join(VALID_PROFILES)}")
        return 1

    args = ["--profile", profile, "up"]
    if build:
        args.append("--build")
    result = docker_compose(*args)
    return result.returncode


# ============================================================================
# Main
# ============================================================================

def usage():
    print()
    print("  Social Data Bridge CLI")
    print()
    print("  Usage:")
    print("    sdb.py setup               Interactive configuration")
    print("    sdb.py setup-reddit        Configure Reddit platform")
    print("    sdb.py add-classifiers     Configure classifiers")
    print("    sdb.py status              Show configuration and ingestion status")
    print("    sdb.py start               Start the PostgreSQL database")
    print("    sdb.py stop                Stop the PostgreSQL database")
    print("    sdb.py run <profile>       Run a pipeline profile (--build to rebuild image)")
    print("    sdb.py unsetup             Remove all configuration (and optionally database)")
    print()
    print(f"  Profiles: {', '.join(VALID_PROFILES)}")
    print()


def main():
    args = sys.argv[1:]

    if not args or args[0] in ("-h", "--help", "help"):
        usage()
        return 0

    command = args[0]

    if command == "setup":
        return cmd_setup()
    elif command == "setup-reddit":
        return cmd_setup_reddit()
    elif command == "add-classifiers":
        return cmd_add_classifiers()
    elif command == "status":
        cmd_status()
        return 0
    elif command == "start":
        return cmd_start()
    elif command == "stop":
        return cmd_stop()
    elif command == "unsetup":
        return cmd_unsetup()
    elif command == "run":
        if len(args) < 2:
            print(f"  Usage: sdb.py run <profile> [--build]")
            print(f"  Profiles: {', '.join(VALID_PROFILES)}")
            return 1
        build = "--build" in args[2:]
        return cmd_run(args[1], build=build)
    else:
        print(f"  Unknown command: {command}")
        usage()
        return 1


if __name__ == "__main__":
    try:
        sys.exit(main() or 0)
    except KeyboardInterrupt:
        print("\n\n  Aborted.\n")
        sys.exit(1)
