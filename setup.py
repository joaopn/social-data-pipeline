#!/usr/bin/env python3
"""Interactive configuration helper for Social Data Bridge.

Detects hardware, asks focused questions with smart defaults,
and generates all user config files (.env, user.yaml, postgresql.local.conf).
"""

import getpass
import os
import shutil
import subprocess
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    print("Error: PyYAML is required. Install with: pip install pyyaml")
    sys.exit(1)

ROOT = Path(__file__).resolve().parent
CONFIG_DIR = ROOT / "config"

# ============================================================================
# Hardware detection
# ============================================================================

def detect_cpu_cores():
    """Return number of CPU cores, or None."""
    return os.cpu_count()


def detect_ram_gb():
    """Return total RAM in GB from /proc/meminfo, or None."""
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    kb = int(line.split()[1])
                    return round(kb / 1024 / 1024, 1)
    except (OSError, ValueError):
        pass
    return None


def detect_gpus():
    """Return list of dicts {index, name, vram_mb} via nvidia-smi, or empty list."""
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=index,name,memory.total",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            return []
        gpus = []
        for line in result.stdout.strip().splitlines():
            parts = [p.strip() for p in line.split(",")]
            if len(parts) >= 3:
                gpus.append({
                    "index": int(parts[0]),
                    "name": parts[1],
                    "vram_mb": int(float(parts[2])),
                })
        return gpus
    except (FileNotFoundError, subprocess.TimeoutExpired, ValueError):
        return []


def detect_hardware():
    """Return dict with all detected hardware info."""
    hw = {
        "cpu_cores": detect_cpu_cores(),
        "ram_gb": detect_ram_gb(),
        "gpus": detect_gpus(),
    }
    return hw


# ============================================================================
# Input helpers
# ============================================================================

def ask(prompt, default=None):
    """Prompt with a default in brackets. Enter accepts default."""
    if default is not None:
        text = f"  {prompt} [{default}]: "
    else:
        text = f"  {prompt}: "
    value = input(text).strip()
    if not value and default is not None:
        return str(default)
    return value


def ask_int(prompt, default=None):
    """Prompt for an integer with default."""
    while True:
        value = ask(prompt, default)
        try:
            return int(value)
        except ValueError:
            print(f"    Please enter a valid integer.")


def ask_bool(prompt, default=True):
    """Prompt for yes/no with default."""
    default_str = "Y/n" if default else "y/N"
    text = f"  {prompt} [{default_str}]: "
    value = input(text).strip().lower()
    if not value:
        return default
    return value in ("y", "yes", "true", "1")


def ask_choice(prompt, options, default=None):
    """Numbered list selection. Returns selected value."""
    print(f"  {prompt}")
    for i, opt in enumerate(options, 1):
        marker = " (default)" if opt == default else ""
        print(f"    {i}) {opt}{marker}")
    while True:
        value = input(f"  Choice [{options.index(default) + 1 if default else ''}]: ").strip()
        if not value and default:
            return default
        try:
            idx = int(value)
            if 1 <= idx <= len(options):
                return options[idx - 1]
        except ValueError:
            if value in options:
                return value
        print(f"    Please enter 1-{len(options)}.")


def ask_multi_select(prompt, options, defaults=None):
    """Numbered list with multi-select. Returns list of selected values."""
    if defaults is None:
        defaults = options[:]
    print(f"  {prompt}")
    for i, opt in enumerate(options, 1):
        marker = " *" if opt in defaults else ""
        print(f"    {i}) {opt}{marker}")
    default_indices = ",".join(str(options.index(d) + 1) for d in defaults)
    print(f"  (* = default)")
    value = input(f"  Select (comma-separated) [{default_indices}]: ").strip()
    if not value:
        return defaults[:]
    selected = []
    for part in value.split(","):
        part = part.strip()
        try:
            idx = int(part)
            if 1 <= idx <= len(options):
                selected.append(options[idx - 1])
        except ValueError:
            if part in options:
                selected.append(part)
    return selected if selected else defaults[:]


def ask_list(prompt, default=None):
    """Prompt for a comma-separated list. Returns list of strings."""
    default_str = ", ".join(default) if default else ""
    value = ask(prompt, default_str)
    return [item.strip() for item in value.split(",") if item.strip()]


def ask_multi_line(prompt):
    """Multi-line input via stdin. Uses readline to handle pasted content.

    Terminals send pasted text line-by-line to stdin, so we use a brief
    timeout to detect when pasting has stopped. User finishes by pressing
    Enter on an empty line.
    """
    import select
    print(f"  {prompt}")
    print(f"  (paste content, then press Enter on an empty line to finish)")
    lines = []
    while True:
        try:
            line = input()
        except EOFError:
            break
        if not line.strip() and lines:
            break
        if line.strip():
            lines.append(line)
        # After reading a line, drain any buffered lines from paste
        while select.select([sys.stdin], [], [], 0.05)[0]:
            try:
                line = input()
            except EOFError:
                break
            if not line.strip() and lines:
                break
            if line.strip():
                lines.append(line)
    return "\n".join(lines)


def section_header(title):
    """Print a section header."""
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}\n")


# ============================================================================
# Default heuristics
# ============================================================================

def compute_defaults(hw, profiles, data_types):
    """Compute suggested defaults from hardware and user choices."""
    cores = hw["cpu_cores"] or 4
    ram = hw["ram_gb"] or 8
    gpus = hw["gpus"]
    min_vram = min((g["vram_mb"] for g in gpus), default=0)

    d = {}

    # Parse
    d["parse_workers"] = min(cores, 8)

    # Lingua
    d["lingua_workers"] = cores
    d["lingua_file_workers"] = max(1, cores // 8)
    d["lingua_batch_size"] = 2_000_000 if ram >= 16 else 500_000
    d["lingua_low_accuracy"] = False

    # GPU ML
    d["gpu_ids"] = [g["index"] for g in gpus] if gpus else [0]
    d["ml_file_workers"] = max(1, len(gpus))
    d["ml_tokenize_workers"] = min(max(1, cores // 2), 8)
    if min_vram >= 12000:
        d["ml_classifier_batch_size"] = 64
    elif min_vram >= 8000:
        d["ml_classifier_batch_size"] = 32
    else:
        d["ml_classifier_batch_size"] = 16
    d["ml_classifiers"] = ["toxic_roberta", "go_emotions"]

    # Postgres
    d["pg_parallel_index_workers"] = min(max(1, cores // 2), 8)
    d["pg_prefer_lingua"] = "ml_cpu" in profiles

    return d


# ============================================================================
# Interactive questionnaire
# ============================================================================

def run_questionnaire(hw):
    """Run the full interactive questionnaire. Returns settings dict."""
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
    defaults = compute_defaults(hw, profiles, data_types)

    # ---- Section 3: Parse ----
    if "parse" in profiles:
        section_header("Section 3: Parse")
        settings["parse_workers"] = ask_int("Parse workers", defaults["parse_workers"])

    # ---- Section 4: Language Detection ----
    if "ml_cpu" in profiles:
        section_header("Section 4: Language Detection (Lingua)")
        settings["lingua_workers"] = ask_int("Lingua workers (total Rayon threads)", defaults["lingua_workers"])
        settings["lingua_file_workers"] = ask_int("Lingua file workers (concurrent files)", defaults["lingua_file_workers"])
        settings["lingua_batch_size"] = ask_int("Lingua batch size (rows per batch)", defaults["lingua_batch_size"])
        settings["lingua_low_accuracy"] = ask_bool("Low accuracy mode (faster)?", defaults["lingua_low_accuracy"])

    # ---- Section 5: GPU Classifiers ----
    if "ml" in profiles:
        section_header("Section 5: GPU Classifiers")

        if gpus:
            gpu_indices = [g["index"] for g in gpus]
            gpu_labels = [f"{g['index']}: {g['name']} ({g['vram_mb']} MB)" for g in gpus]
            selected_gpus = ask_multi_select(
                "GPUs to use:",
                gpu_labels,
                gpu_labels,  # default: all
            )
            settings["gpu_ids"] = [gpu_indices[gpu_labels.index(g)] for g in selected_gpus]
        else:
            gpu_ids_str = ask("GPU IDs (comma-separated)", "0")
            settings["gpu_ids"] = [int(x.strip()) for x in gpu_ids_str.split(",")]

        # Recompute file_workers default based on selected GPUs
        defaults["ml_file_workers"] = max(1, len(settings["gpu_ids"]))

        settings["ml_file_workers"] = ask_int("File workers", defaults["ml_file_workers"])
        settings["ml_tokenize_workers"] = ask_int("Tokenize workers", defaults["ml_tokenize_workers"])
        settings["ml_classifier_batch_size"] = ask_int("Classifier batch size", defaults["ml_classifier_batch_size"])

        available_classifiers = ["toxic_roberta", "go_emotions"]
        settings["ml_classifiers"] = ask_multi_select(
            "Classifiers to run:",
            available_classifiers,
            defaults["ml_classifiers"],
        )

        settings["hf_token"] = getpass.getpass("  HuggingFace token (optional, press Enter to skip): ").strip()

    # ---- Section 6: PostgreSQL ----
    has_postgres = any(p.startswith("postgres") for p in profiles)
    if has_postgres:
        section_header("Section 6: PostgreSQL")

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

    # ---- Section 7: Platform-specific ----
    if platform == "reddit":
        section_header("Section 7: Reddit Platform")

        settings["db_schema"] = ask("Database schema name", "reddit")

        # Field list selection
        all_sub_fields = [
            "created_utc", "id10", "score", "upvote_ratio", "num_comments",
            "num_crossposts", "total_awards_received", "subreddit",
            "subreddit_subscribers", "stickied", "gilded", "distinguished",
            "locked", "quarantine", "over_18", "is_deleted", "removal_type",
            "author", "author_flair_text", "author_created_utc",
            "link_flair_text", "domain", "url", "title", "selftext",
        ]
        all_com_fields = [
            "created_utc", "link_id", "parent_id", "score",
            "controversiality", "total_awards_received", "subreddit",
            "stickied", "gilded", "distinguished", "is_deleted",
            "removal_type", "author", "author_flair_text",
            "author_created_utc", "is_submitter", "body",
        ]

        print("  The default field list is defined in config/platforms/reddit/field_list.yaml.")
        print("  You can remove fields here. Adding new fields also requires field_types.yaml.")
        customize_fields = ask_bool("Remove fields from the default list?", False)
        if customize_fields:
            if "submissions" in data_types:
                print("\n  Submissions fields (deselect to exclude):")
                settings["reddit_sub_fields"] = ask_multi_select(
                    "Submissions fields:", all_sub_fields, all_sub_fields,
                )
            if "comments" in data_types:
                print("\n  Comments fields (deselect to exclude):")
                settings["reddit_com_fields"] = ask_multi_select(
                    "Comments fields:", all_com_fields, all_com_fields,
                )

        # Index selection
        default_sub_indexes = ["dataset", "author", "subreddit", "domain", "created_utc"]
        default_com_indexes = ["dataset", "author", "subreddit", "link_id", "created_utc"]

        customize_indexes = ask_bool("Customize database indexes?", False)
        if customize_indexes:
            if "submissions" in data_types:
                settings["reddit_sub_indexes"] = ask_list(
                    "Submissions index columns", default_sub_indexes,
                )
            if "comments" in data_types:
                settings["reddit_com_indexes"] = ask_list(
                    "Comments index columns", default_com_indexes,
                )

    elif platform == "generic":
        section_header("Section 7: Generic Platform")
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
    ]

    hf_token = settings.get("hf_token", "")
    if hf_token:
        lines.append(f"HF_TOKEN={hf_token}")
    else:
        lines += [
            "# Set HF_TOKEN to avoid rate limits and download private models. You can get a token from https://huggingface.co/settings/tokens",
            "# HF_TOKEN=",
        ]

    lines += [
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


def generate_ml_cpu_user_yaml(settings):
    """Generate config/ml_cpu/user.yaml content."""
    config = {
        "pipeline": {
            "processing": {
                "data_types": settings["data_types"],
            }
        },
        "cpu_classifiers": {
            "lingua": {
                "low_accuracy": settings["lingua_low_accuracy"],
                "workers": settings["lingua_workers"],
                "file_workers": settings["lingua_file_workers"],
                "batch_size": settings["lingua_batch_size"],
            }
        }
    }
    return yaml.dump(config, default_flow_style=False, sort_keys=False)


def generate_ml_user_yaml(settings):
    """Generate config/ml/user.yaml content."""
    config = {
        "pipeline": {
            "processing": {
                "data_types": settings["data_types"],
            },
            "gpu_classifiers": settings["ml_classifiers"],
        },
        "gpu_classifiers": {
            "gpu_ids": settings["gpu_ids"],
            "file_workers": settings["ml_file_workers"],
            "tokenize_workers": settings["ml_tokenize_workers"],
            "classifier_batch_size": settings["ml_classifier_batch_size"],
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
        "# Auto-generated by setup.py — tablespace volume mounts.\n"
        "# Each volume maps a host directory to a container path used by CREATE TABLESPACE.\n"
        "# Tablespace assignments are in config/postgres/user.yaml.\n"
        "\n"
        "services:\n"
        "  postgres:\n"
        "    volumes:\n"
        f"{volumes_str}\n"
    )


def generate_reddit_platform_user_yaml(settings):
    """Generate config/platforms/reddit/user.yaml content."""
    config = {}

    # Platform overrides (schema, indexes)
    platform = {}
    if settings.get("db_schema", "reddit") != "reddit":
        platform["db_schema"] = settings["db_schema"]

    # Indexes (only if customized)
    indexes = {}
    if "reddit_sub_indexes" in settings:
        indexes["submissions"] = settings["reddit_sub_indexes"]
    if "reddit_com_indexes" in settings:
        indexes["comments"] = settings["reddit_com_indexes"]
    if indexes:
        platform["indexes"] = indexes

    if platform:
        config["platform"] = platform

    # Field list (only if customized)
    field_list = {}
    if "reddit_sub_fields" in settings:
        field_list["submissions"] = settings["reddit_sub_fields"]
    if "reddit_com_fields" in settings:
        field_list["comments"] = settings["reddit_com_fields"]
    if field_list:
        config["field_list"] = field_list

    if not config:
        return None  # No overrides needed
    return yaml.dump(config, default_flow_style=False, sort_keys=False)


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
        # Keep everything up to and including the marker line
        marker_idx = base_content.index(pgtune_marker)
        marker_line_end = base_content.index("\n", marker_idx) + 1
        content = base_content[:marker_line_end]
    else:
        content = base_content

    # Toggle ZFS settings
    is_zfs = settings.get("filesystem") == "zfs"
    if is_zfs:
        # Uncomment ZFS block: the block after "ZFS optimizations" header
        # Format: "# shared_buffers = 16GB" → "shared_buffers = 16GB"
        # Keep "# # comment" lines as "# comment" (explanation comments)
        new_lines = []
        in_zfs_block = False
        seen_zfs_header = False
        zfs_separator_count = 0
        for line in content.splitlines():
            if "ZFS optimizations" in line:
                seen_zfs_header = True
                new_lines.append(line)
                continue
            if seen_zfs_header and not in_zfs_block and line.startswith("#=="):
                # This is the closing #=== of the header — block starts after this
                in_zfs_block = True
                new_lines.append(line)
                continue
            if in_zfs_block and line.startswith("#=="):
                # End of ZFS block (next section marker)
                in_zfs_block = False
                new_lines.append(line)
                continue
            if in_zfs_block:
                if line.startswith("# # "):
                    # Comment line → keep as single-level comment
                    new_lines.append("#" + line[3:])
                elif line.startswith("# ") and "=" in line:
                    # Setting line → uncomment
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
# Summary and file writing
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

    if "ml_cpu" in profiles:
        print(f"  Lingua:")
        print(f"    Workers:        {settings['lingua_workers']}")
        print(f"    File workers:   {settings['lingua_file_workers']}")
        print(f"    Batch size:     {settings['lingua_batch_size']:,}")
        print(f"    Low accuracy:   {settings['lingua_low_accuracy']}")
        print()

    if "ml" in profiles:
        print(f"  GPU Classifiers:")
        print(f"    GPU IDs:             {settings['gpu_ids']}")
        print(f"    File workers:        {settings['ml_file_workers']}")
        print(f"    Tokenize workers:    {settings['ml_tokenize_workers']}")
        print(f"    Classifier batch:    {settings['ml_classifier_batch_size']}")
        print(f"    Classifiers:         {', '.join(settings['ml_classifiers'])}")
        print(f"    HF token:            {'set' if settings.get('hf_token') else 'not set'}")
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

    if settings["platform"] == "reddit":
        print(f"  Reddit Platform:")
        print(f"    Schema:              {settings.get('db_schema', 'reddit')}")
        has_custom_fields = "reddit_sub_fields" in settings or "reddit_com_fields" in settings
        has_custom_indexes = "reddit_sub_indexes" in settings or "reddit_com_indexes" in settings
        print(f"    Field lists:         {'customized' if has_custom_fields else 'default'}")
        print(f"    Indexes:             {'customized' if has_custom_indexes else 'default'}")
        print()

    print("  Files to write:")
    for path, _ in files_to_write:
        rel = path.relative_to(ROOT)
        exists = path.exists()
        status = " (exists, will backup)" if exists else ""
        print(f"    {rel}{status}")
    print()


def write_files(files_to_write):
    """Write all config files, backing up existing ones."""
    for path, content in files_to_write:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            backup = path.with_suffix(path.suffix + ".bak")
            shutil.copy2(path, backup)
            print(f"  Backed up: {path.relative_to(ROOT)} -> {backup.name}")
        path.write_text(content)
        print(f"  Wrote:     {path.relative_to(ROOT)}")


# ============================================================================
# Main
# ============================================================================

def main():
    print()
    print("  Social Data Bridge - Configuration Helper")
    print("  ==========================================")
    print()
    print("  This script will detect your hardware, ask configuration")
    print("  questions with smart defaults, and generate config files.")
    print("  Press Enter to accept defaults shown in [brackets].")
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

    if "ml_cpu" in profiles:
        files_to_write.append((
            CONFIG_DIR / "ml_cpu" / "user.yaml",
            generate_ml_cpu_user_yaml(settings),
        ))

    if "ml" in profiles:
        files_to_write.append((
            CONFIG_DIR / "ml" / "user.yaml",
            generate_ml_user_yaml(settings),
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

    # Platform user.yaml
    if settings["platform"] == "reddit":
        reddit_yaml = generate_reddit_platform_user_yaml(settings)
        if reddit_yaml is not None:
            files_to_write.append((
                CONFIG_DIR / "platforms" / "reddit" / "user.yaml",
                reddit_yaml,
            ))
    elif settings["platform"] == "generic":
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
    print(f"\n  Done! Configuration files have been generated.")

    # Print commands to run the selected profiles in correct order
    profile_order = ["parse", "ml_cpu", "ml", "postgres", "postgres_ingest", "postgres_ml"]
    selected = [p for p in profile_order if p in profiles]

    # postgres server must run alongside ingestion profiles
    needs_postgres = any(p in profiles for p in ("postgres_ingest", "postgres_ml"))
    if needs_postgres and "postgres" not in selected:
        # Insert postgres before the first ingestion profile
        for i, p in enumerate(selected):
            if p in ("postgres_ingest", "postgres_ml"):
                selected.insert(i, "postgres")
                break

    print("  To run your pipeline, execute these commands in order:\n")
    for p in selected:
        if p == "postgres":
            print(f"    docker compose --profile postgres up -d")
        else:
            print(f"    docker compose --profile {p} up")
    print()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n  Aborted.\n")
        sys.exit(1)
