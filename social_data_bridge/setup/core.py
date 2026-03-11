"""Core infrastructure configuration for Social Data Bridge.

Configures platform, paths, parse workers, and PostgreSQL settings.
Generates .env, user.yaml files, and postgresql.local.conf.
"""

import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    print("Error: PyYAML is required. Install with: pip install pyyaml")
    sys.exit(1)

from social_data_bridge.setup.utils import (
    ROOT, CONFIG_DIR,
    detect_hardware,
    ask, ask_int, ask_bool, ask_choice, ask_multi_select, ask_list, ask_multi_line,
    section_header, save_setup_state, write_files, print_pipeline_commands,
)


# ============================================================================
# Default heuristics
# ============================================================================

def compute_defaults(hw, profiles):
    """Compute suggested defaults from hardware and user choices."""
    cores = hw["cpu_cores"] or 4

    d = {}

    # Parse
    d["parse_workers"] = min(cores, 8)

    # Postgres
    d["pg_parallel_index_workers"] = min(max(1, cores // 2), 8)
    d["pg_prefer_lingua"] = "ml_cpu" in profiles

    return d


# ============================================================================
# Interactive questionnaire
# ============================================================================

def run_questionnaire(hw):
    """Run the core interactive questionnaire. Returns settings dict."""
    settings = {}

    # --- Print hardware summary ---
    section_header("Hardware Detected")
    cores = hw["cpu_cores"]
    ram = hw["ram_gb"]
    gpus = hw["gpus"]
    print(f"  CPU cores: {cores or 'unknown'}")
    print(f"  RAM:       {ram or 'unknown'} GB")
    if gpus:
        for g in gpus:
            print(f"  GPU {g['index']}:    {g['name']} ({g['vram_mb']} MB)")
    else:
        print(f"  GPUs:      none detected")
    print()

    # ---- Section 1: General ----
    section_header("Section 1: General")

    platform = ask_choice(
        "Platform:",
        ["reddit", "generic"],
        default="reddit",
    )
    settings["platform"] = platform

    if platform == "reddit":
        data_types = ask_list("Data types", ["submissions", "comments"])
    else:
        data_types = ask_list("Data types (comma-separated)")
        if not data_types:
            print("    Error: At least one data type is required for generic platform.")
            sys.exit(1)
    settings["data_types"] = data_types

    all_profiles = ["parse", "ml_cpu", "ml", "postgres_ingest", "postgres_ml"]
    default_profiles = all_profiles[:] if gpus else [p for p in all_profiles if p != "ml"]
    profiles = ask_multi_select("Profiles to configure:", all_profiles, default_profiles)
    settings["profiles"] = profiles

    # ---- Section 2: Paths ----
    section_header("Section 2: Paths")

    settings["dumps_path"] = ask("Dumps path (.zst files)", "./data/dumps")
    settings["extracted_path"] = ask("Extracted path (decompressed JSON)", "./data/extracted")
    settings["csv_path"] = ask("CSV path (parsed output)", "./data/csv")
    settings["output_path"] = ask("Output path (ML classifier output)", "./data/output")
    if any(p.startswith("postgres") for p in profiles):
        settings["pgdata_path"] = ask("PostgreSQL data path", "./data/database")

    # ---- Compute defaults ----
    defaults = compute_defaults(hw, profiles)

    # ---- Section 3: Parse ----
    if "parse" in profiles:
        section_header("Section 3: Parse")
        settings["parse_workers"] = ask_int("Parse workers", defaults["parse_workers"])

    # ---- Section 4: PostgreSQL ----
    has_postgres = any(p.startswith("postgres") for p in profiles)
    if has_postgres:
        section_header("Section 4: PostgreSQL")

        settings["db_name"] = ask("Database name", "datasets")
        settings["pg_port"] = ask_int("PostgreSQL port", 5432)

        if "postgres_ingest" in profiles:
            settings["pg_prefer_lingua"] = ask_bool("Prefer Lingua CSVs (includes lang columns in base tables)?", defaults["pg_prefer_lingua"])
            settings["pg_parallel_index_workers"] = ask_int("Parallel index workers", defaults["pg_parallel_index_workers"])

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
                # Assign data types to tablespaces
                ts_choices = ["pgdata"] + list(ts_tablespaces.keys())
                print()
                print("  Assign each data type to a tablespace:")
                print(f"    Available: {', '.join(ts_choices)}")
                ts_assignments = {}
                for dt in settings["data_types"]:
                    ts = ask_choice(
                        f"  {dt} tablespace:",
                        ts_choices,
                        default="pgdata",
                    )
                    ts_assignments[dt] = ts
                settings["table_tablespaces"] = ts_assignments
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

    # ---- Section 5: Generic Platform ----
    if platform == "generic":
        section_header("Section 5: Generic Platform")
        settings["db_schema"] = ask("Database schema name")
        settings["generic_file_patterns"] = {}
        for dt in data_types:
            print(f"\n  File patterns for '{dt}':")
            zst = ask(f"  .zst regex pattern for {dt}")
            json_pat = ask(f"  JSON regex pattern for {dt}")
            csv_pat = ask(f"  CSV regex pattern for {dt}")
            prefix = ask(f"  Filename prefix for {dt}")
            settings["generic_file_patterns"][dt] = {
                "zst": zst, "json": json_pat, "csv": csv_pat, "prefix": prefix,
            }

    return settings


# ============================================================================
# Config generators
# ============================================================================

def generate_env(settings):
    """Generate .env file content."""
    lines = [
        "# ===== PATH CONFIGURATION =====",
        "# Where your .zst dump files are located",
        f"DUMPS_PATH={settings['dumps_path']}",
        "",
        "# Storage for decompressed JSON files",
        f"EXTRACTED_PATH={settings['extracted_path']}",
        "",
        "# Storage for parsed CSV files",
        f"CSV_PATH={settings['csv_path']}",
        "",
        "# Storage for classifier output files",
        f"OUTPUT_PATH={settings['output_path']}",
        "",
    ]

    if "pgdata_path" in settings:
        lines += [
            "# PostgreSQL data location (postgres profiles only)",
            f"PGDATA_PATH={settings['pgdata_path']}",
            "",
        ]

    lines += [
        "# ===== HUGGINGFACE CONFIGURATION (ml profile) =====",
        "# Set HF_HOME to specify a custom cache directory for Hugging Face models and datasets. ",
        "# HF_HOME=",
        "# Set HF_TOKEN to avoid rate limits and download private models. You can get a token from https://huggingface.co/settings/tokens",
        "# HF_TOKEN=",
        "",
        "# ===== DATABASE CONFIGURATION (postgres profiles) =====",
        f"DB_NAME={settings.get('db_name', 'datasets')}",
        f"POSTGRES_PORT={settings.get('pg_port', 5432)}",
        "# Note: DB_SCHEMA is set per-platform in config/platforms/<platform>/platform.yaml",
    ]

    return "\n".join(lines) + "\n"


def generate_parse_user_yaml(settings):
    """Generate config/parse/user.yaml content."""
    config = {
        "pipeline": {
            "processing": {
                "data_types": settings["data_types"],
                "parallel_mode": True,
                "parse_workers": settings["parse_workers"],
            }
        }
    }
    return yaml.dump(config, default_flow_style=False, sort_keys=False)


def generate_postgres_user_yaml(settings):
    """Generate config/postgres/user.yaml content."""
    processing = {
        "data_types": settings["data_types"],
    }
    if "pg_prefer_lingua" in settings:
        processing["prefer_lingua"] = settings["pg_prefer_lingua"]
    if "pg_parallel_index_workers" in settings:
        processing["parallel_index_workers"] = settings["pg_parallel_index_workers"]

    config = {
        "pipeline": {
            "processing": processing,
        }
    }

    if "tablespaces" in settings:
        config["pipeline"]["tablespaces"] = settings["tablespaces"]
    if "table_tablespaces" in settings:
        config["pipeline"]["table_tablespaces"] = settings["table_tablespaces"]

    return yaml.dump(config, default_flow_style=False, sort_keys=False)


def generate_postgres_ml_user_yaml(settings):
    """Generate config/postgres_ml/user.yaml content."""
    config = {
        "pipeline": {
            "processing": {
                "data_types": settings["data_types"],
            }
        }
    }
    return yaml.dump(config, default_flow_style=False, sort_keys=False)


def generate_docker_compose_override(settings):
    """Generate docker-compose.override.yml with tablespace volume mounts."""
    tablespaces = settings.get("tablespaces", {})

    # Only include custom tablespaces (not pgdata)
    volume_lines = []
    for ts_name, host_path in tablespaces.items():
        if ts_name != "pgdata":
            volume_lines.append(f"      - {host_path}:/data/tablespace/{ts_name}")

    if not volume_lines:
        return None

    volumes_str = "\n".join(volume_lines)
    return (
        "# Auto-generated by sdb setup — tablespace volume mounts.\n"
        "# Each volume maps a host directory to a container path used by CREATE TABLESPACE.\n"
        "# Tablespace assignments are in config/postgres/user.yaml.\n"
        "\n"
        "services:\n"
        "  postgres:\n"
        "    volumes:\n"
        f"{volumes_str}\n"
    )


def generate_generic_platform_user_yaml(settings):
    """Generate config/platforms/generic/user.yaml content."""
    config = {
        "platform": {
            "db_schema": settings["db_schema"],
            "data_types": settings["data_types"],
            "file_patterns": settings["generic_file_patterns"],
        }
    }
    return yaml.dump(config, default_flow_style=False, sort_keys=False)


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


# ============================================================================
# Summary
# ============================================================================

def print_summary(settings, files_to_write):
    """Print a summary of all settings and files to be written."""
    section_header("Configuration Summary")

    profiles = settings["profiles"]

    print(f"  Platform:    {settings['platform']}")
    print(f"  Data types:  {', '.join(settings['data_types'])}")
    print(f"  Profiles:    {', '.join(profiles)}")
    print()

    print("  Paths:")
    print(f"    Dumps:     {settings['dumps_path']}")
    print(f"    Extracted: {settings['extracted_path']}")
    print(f"    CSV:       {settings['csv_path']}")
    print(f"    Output:    {settings['output_path']}")
    if "pgdata_path" in settings:
        print(f"    PG data:   {settings['pgdata_path']}")
    print()

    if "parse" in profiles:
        print(f"  Parse:")
        print(f"    Workers:   {settings['parse_workers']}")
        print()

    if any(p.startswith("postgres") for p in profiles):
        print(f"  PostgreSQL:")
        print(f"    DB name:             {settings.get('db_name', 'datasets')}")
        print(f"    Port:                {settings.get('pg_port', 5432)}")
        if "postgres_ingest" in profiles:
            print(f"    Prefer lingua:       {settings.get('pg_prefer_lingua', False)}")
            print(f"    Index workers:       {settings.get('pg_parallel_index_workers', 4)}")
        print(f"    Filesystem:          {settings.get('filesystem', 'standard')}")
        if "tablespaces" in settings:
            print(f"    Tablespaces:")
            for ts_name, ts_path in settings["tablespaces"].items():
                print(f"      {ts_name}: {ts_path}")
            if "table_tablespaces" in settings:
                print(f"    Table assignments:")
                for dt, ts in settings["table_tablespaces"].items():
                    print(f"      {dt} -> {ts}")
        print(f"    PGTune:              {'provided' if settings.get('pgtune_output') else 'not provided'}")
        print()

    if settings["platform"] == "generic":
        print(f"  Generic Platform:")
        print(f"    Schema:              {settings.get('db_schema')}")
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
    print("  Social Data Bridge - Configuration Helper")
    print("  ==========================================")
    print()
    print("  This script configures core infrastructure: paths, parse,")
    print("  and PostgreSQL settings. Press Enter to accept defaults.")
    print()

    # Detect hardware
    hw = detect_hardware()

    # Run questionnaire
    settings = run_questionnaire(hw)
    profiles = settings["profiles"]

    # Build file list
    files_to_write = []

    # .env
    files_to_write.append((ROOT / ".env", generate_env(settings)))

    # Profile user.yaml files
    if "parse" in profiles:
        files_to_write.append((
            CONFIG_DIR / "parse" / "user.yaml",
            generate_parse_user_yaml(settings),
        ))

    if "postgres_ingest" in profiles:
        files_to_write.append((
            CONFIG_DIR / "postgres" / "user.yaml",
            generate_postgres_user_yaml(settings),
        ))

    if "postgres_ml" in profiles:
        files_to_write.append((
            CONFIG_DIR / "postgres_ml" / "user.yaml",
            generate_postgres_ml_user_yaml(settings),
        ))

    # postgresql.local.conf
    if any(p.startswith("postgres") for p in profiles):
        files_to_write.append((
            CONFIG_DIR / "postgres" / "postgresql.local.conf",
            generate_postgresql_local_conf(settings),
        ))

    # docker-compose.override.yml (tablespace volumes)
    if "tablespaces" in settings:
        override_content = generate_docker_compose_override(settings)
        if override_content:
            files_to_write.append((
                ROOT / "docker-compose.override.yml",
                override_content,
            ))

    # Platform user.yaml (generic only; Reddit is handled by setup/reddit.py)
    if settings["platform"] == "generic":
        files_to_write.append((
            CONFIG_DIR / "platforms" / "generic" / "user.yaml",
            generate_generic_platform_user_yaml(settings),
        ))

    # Summary and confirm
    print_summary(settings, files_to_write)

    if not ask_bool("Write these files?", True):
        print("\n  Aborted. No files written.\n")
        sys.exit(0)

    print()
    write_files(files_to_write)

    # Save state for downstream scripts
    state = {
        "platform": settings["platform"],
        "data_types": settings["data_types"],
        "profiles": profiles,
    }
    save_setup_state(state)
    print(f"  Wrote:     config/setup_state.yaml")

    print(f"\n  Done! Core configuration files have been generated.")

    # Print next steps
    has_classifiers = "ml_cpu" in profiles or "ml" in profiles
    if settings["platform"] == "reddit":
        print("\n  Next steps:")
        if has_classifiers:
            print("    python sdb.py add-classifiers  # (optional) Customize classifier settings")
        print("    python sdb.py setup-reddit     # Configure Reddit fields and indexes")
        print()
    elif has_classifiers:
        print("\n  Next step:")
        print("    python sdb.py add-classifiers  # Configure classifier settings")
        print()
    else:
        print_pipeline_commands(profiles)
