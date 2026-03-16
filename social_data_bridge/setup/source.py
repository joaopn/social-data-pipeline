"""Source configuration for Social Data Bridge.

Configures a data source: platform type, data types, file patterns, fields,
profile settings. Generates config/sources/<name>/ directory with per-profile
override files.

Each source is independent and self-contained.
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
    ask, ask_int, ask_bool, ask_choice, ask_list, ask_multi_select,
    section_header, write_files, list_sources, load_db_setup, load_env,
    print_pipeline_commands, update_env_file,
    detect_compression_from_glob, derive_file_patterns,
)
from social_data_bridge.setup.classifiers import (
    run_questionnaire as run_classifier_questionnaire,
    generate_lingua_user_yaml,
    generate_ml_user_yaml,
)


# ============================================================================
# Default heuristics
# ============================================================================

def compute_defaults(hw, profiles):
    """Compute suggested defaults from hardware and user choices."""
    cores = hw["cpu_cores"] or 4
    d = {}
    d["parse_workers"] = min(cores, 8)
    d["pg_parallel_index_workers"] = min(max(1, cores // 2), 8)
    d["pg_prefer_lingua"] = "lingua" in profiles
    return d


# ============================================================================
# Interactive questionnaire
# ============================================================================

def run_questionnaire(hw, source_name, db_setup):
    """Run the source configuration questionnaire. Returns settings dict."""
    settings = {}
    settings["source_name"] = source_name

    # Determine platform type from source name
    if source_name == "reddit":
        platform = "reddit"
    else:
        platform = f"custom/{source_name}"
    settings["platform"] = platform

    # ---- Data types ----
    section_header("Data Types")
    if platform == "reddit":
        data_types = ask_list("Data types", ["submissions", "comments"])
    else:
        data_types = ask_list("Data types (comma-separated)")
        if not data_types:
            print("    Error: At least one data type is required.")
            sys.exit(1)
    settings["data_types"] = data_types

    # ---- Data paths ----
    section_header("Data Paths")
    data_path = load_env().get("DATA_PATH", "./data")
    settings["dumps_path"] = ask("Dumps directory", f"{data_path}/dumps/{source_name}")
    settings["extracted_path"] = ask("Extracted directory", f"{data_path}/extracted/{source_name}")
    settings["parsed_path"] = ask("Parsed directory", f"{data_path}/parsed/{source_name}")
    settings["output_path"] = ask("Output directory", f"{data_path}/output/{source_name}")

    # ---- File format ----
    section_header("File Format")
    print("  Parquet files are smaller, enforce schema, and preserve text")
    print("  without escaping. CSV is the legacy format.")
    settings["file_format"] = ask_choice(
        "Intermediate file format", ["parquet", "csv"], default="parquet",
    )

    if settings["file_format"] == "parquet":
        rg_size = ask_int("Parquet row-group size (larger = better compression, more RAM)", 1_000_000)
        if rg_size != 1_000_000:
            settings["parquet_row_group_size"] = rg_size

    # ---- Profiles ----
    section_header("Profiles")

    databases = db_setup.get("databases", ["postgres"])
    gpus = hw["gpus"]

    all_profiles = ["parse", "lingua", "ml"]
    if "postgres" in databases:
        all_profiles += ["postgres_ingest", "postgres_ml"]
    if "mongo" in databases:
        all_profiles += ["mongo_ingest"]

    default_profiles = all_profiles[:] if gpus else [p for p in all_profiles if p != "ml"]
    profiles = ask_multi_select("Profiles to configure:", all_profiles, default_profiles)
    settings["profiles"] = profiles

    # ---- Compute defaults ----
    defaults = compute_defaults(hw, profiles)

    # ---- Parse settings ----
    if "parse" in profiles:
        section_header("Parse Settings")
        settings["parse_workers"] = ask_int("Parse workers", defaults["parse_workers"])

    # ---- PostgreSQL settings (per-source) ----
    has_postgres = any(p.startswith("postgres") for p in profiles)
    has_mongo = "mongo_ingest" in profiles
    if has_postgres:
        section_header("PostgreSQL Settings (for this source)")

        if "postgres_ingest" in profiles:
            settings["pg_prefer_lingua"] = ask_bool(
                "Use Lingua files (includes lang columns in base tables)?",
                defaults["pg_prefer_lingua"],
            )
            settings["pg_parallel_index_workers"] = ask_int(
                "Parallel index workers", defaults["pg_parallel_index_workers"],
            )

        # Tablespace assignments (if tablespaces were configured in db setup)
        db_tablespaces = db_setup.get("tablespaces", {})
        if db_tablespaces:
            ts_choices = ["pgdata"] + list(db_tablespaces.keys())
            print()
            print("  Assign each data type to a tablespace:")
            print(f"    Available: {', '.join(ts_choices)}")
            ts_assignments = {}
            for dt in data_types:
                ts = ask_choice(
                    f"  {dt} tablespace:",
                    ts_choices,
                    default="pgdata",
                )
                ts_assignments[dt] = ts
            settings["table_tablespaces"] = ts_assignments
            settings["tablespaces"] = db_tablespaces

    # ---- Custom platform details ----
    if platform.startswith("custom/"):
        section_header("Custom Platform Configuration")

        if has_postgres:
            settings["db_schema"] = ask("PostgreSQL schema name", source_name)

        # ---- Input format ----
        section_header("Input Format")
        print("  NDJSON: one JSON object per line (default for most data dumps)")
        print("  CSV: comma/tab/pipe-separated values with headers")
        settings["input_format"] = ask_choice(
            "Raw input file format", ["ndjson", "csv"], default="ndjson",
        )
        if settings["input_format"] == "csv":
            delimiter = ask("CSV delimiter character (comma=, tab=\\t pipe=|)", ",")
            if delimiter == "\\t":
                delimiter = "\t"
            settings["input_csv_delimiter"] = delimiter

        settings["custom_file_patterns"] = {}
        for dt in data_types:
            print(f"\n  File patterns for '{dt}':")
            print("    Enter a glob pattern for your compressed dump files.")
            print("    Examples: tweets_*.json.gz, RC_*.zst, data_*.csv.xz")
            print()
            dump_glob = ask(f"    Dump file glob pattern for {dt}")
            if not dump_glob:
                print("    Error: A glob pattern is required.")
                sys.exit(1)

            # Auto-detect compression from glob extension
            compression = detect_compression_from_glob(dump_glob)
            if compression is None:
                print(f"\n    Could not auto-detect compression from '{dump_glob}'.")
                compression = ask_choice(
                    "    Select compression format:",
                    ["zst", "gz", "xz", "tar.gz"],
                    default="zst",
                )

            input_format = settings.get("input_format", "ndjson")
            patterns = derive_file_patterns(dump_glob, compression, input_format=input_format)
            settings["custom_file_patterns"][dt] = patterns
            print(f"    Detected: compression={compression}, prefix={patterns['prefix']}")

        print()
        print("    Note: For uncompressed data, place files directly in")
        print("    data/extracted/<source>/<data_type>/ — the pipeline will")
        print("    pick them up automatically with no dump files needed.")

        if has_mongo:
            print()
            settings["mongo_collection_strategy"] = ask_choice(
                "MongoDB collection strategy:",
                ["per_data_type", "per_file"],
                default="per_data_type",
            )
            if settings["mongo_collection_strategy"] == "per_data_type":
                settings["mongo_collections"] = {}
                for dt in data_types:
                    coll = ask(f"  MongoDB collection name for '{dt}'", dt)
                    settings["mongo_collections"][dt] = coll
            else:
                print("    (per_file: one collection per input file, named from filename)")

            settings["mongo_db_name"] = ask("MongoDB database name", source_name)

        # ---- Index configuration ----
        if has_postgres or has_mongo:
            section_header("Database Indexes")
            print("  Indexes speed up queries on specific columns.")
            print("  Enter column names (comma-separated) to index per data type.")
            print("  Leave empty for no indexes.")

            if has_postgres:
                print()
                settings["custom_indexes"] = {}
                for dt in data_types:
                    idx = ask_list(f"  PostgreSQL indexes for '{dt}'")
                    if idx:
                        settings["custom_indexes"][dt] = idx

            if has_mongo:
                print()
                settings["custom_mongo_indexes"] = {}
                for dt in data_types:
                    idx = ask_list(f"  MongoDB indexes for '{dt}'")
                    if idx:
                        settings["custom_mongo_indexes"][dt] = idx

    return settings


# ============================================================================
# Config generators
# ============================================================================

def generate_platform_yaml(settings):
    """Generate config/sources/<name>/platform.yaml for custom platforms."""
    config = {
        "file_format": settings.get("file_format", "parquet"),
    }
    if settings.get("input_format", "ndjson") != "ndjson":
        config["input_format"] = settings["input_format"]
    if settings.get("input_csv_delimiter", ",") != ",":
        config["input_csv_delimiter"] = settings["input_csv_delimiter"]
    if "parquet_row_group_size" in settings:
        config["parquet_row_group_size"] = settings["parquet_row_group_size"]
    config.update({
        "db_schema": settings.get("db_schema", settings["source_name"]),
        "data_types": settings["data_types"],
        "paths": {
            "dumps": settings["dumps_path"],
            "extracted": settings["extracted_path"],
            "parsed": settings["parsed_path"],
            "output": settings["output_path"],
        },
        "file_patterns": settings.get("custom_file_patterns", {}),
    })

    if "mongo_collection_strategy" in settings:
        config["mongo_collection_strategy"] = settings["mongo_collection_strategy"]
        config["mongo_db_name"] = settings.get("mongo_db_name", settings["source_name"])
        if "mongo_collections" in settings:
            config["mongo_collections"] = settings["mongo_collections"]

    if settings.get("custom_mongo_indexes"):
        config["mongo_indexes"] = settings["custom_mongo_indexes"]

    config.update({
        "indexes": settings.get("custom_indexes", {}),
        "field_types": {
            # Common field types — add or modify as needed
            "id": "text",
            "created_at": "integer",
            "timestamp": "integer",
            "text": "text",
            "content": "text",
            "author": "text",
            "title": "text",
            "url": "text",
            "score": "integer",
            "count": "integer",
            # Lingua language detection fields (added by lingua profile)
            "lang": ["varchar", 2],
            "lang_prob": "float",
            "lang2": ["varchar", 2],
            "lang2_prob": "float",
            "lang_chars": "integer",
        },
        "fields": {dt: [] for dt in settings["data_types"]},
    })
    return yaml.dump(config, default_flow_style=False, sort_keys=False)


def generate_reddit_platform_yaml(settings):
    """Generate config/sources/reddit/platform.yaml by copying base config + adding paths."""
    base_path = CONFIG_DIR / "templates" / "reddit.yaml"
    try:
        base_config = yaml.safe_load(base_path.read_text())
    except (OSError, ValueError) as e:
        print(f"  Error: Could not read {base_path}: {e}")
        sys.exit(1)

    base_config["file_format"] = settings.get("file_format", "parquet")
    if "parquet_row_group_size" in settings:
        base_config["parquet_row_group_size"] = settings["parquet_row_group_size"]
    base_config["paths"] = {
        "dumps": settings["dumps_path"],
        "extracted": settings["extracted_path"],
        "parsed": settings["parsed_path"],
        "output": settings["output_path"],
    }
    return yaml.dump(base_config, default_flow_style=False, sort_keys=False)


def generate_parse_yaml(settings):
    """Generate config/sources/<name>/parse.yaml."""
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


def generate_postgres_yaml(settings):
    """Generate config/sources/<name>/postgres.yaml."""
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


def generate_postgres_ml_yaml(settings):
    """Generate config/sources/<name>/postgres_ml.yaml."""
    config = {
        "pipeline": {
            "processing": {
                "data_types": settings["data_types"],
            }
        }
    }
    return yaml.dump(config, default_flow_style=False, sort_keys=False)


def generate_mongo_yaml(settings):
    """Generate config/sources/<name>/mongo.yaml."""
    config = {
        "pipeline": {
            "processing": {
                "data_types": settings["data_types"],
            }
        }
    }
    return yaml.dump(config, default_flow_style=False, sort_keys=False)


# ============================================================================
# Summary
# ============================================================================

def print_summary(settings, files_to_write):
    """Print a summary of source settings and files to be written."""
    section_header("Source Configuration Summary")

    profiles = settings["profiles"]
    source_name = settings["source_name"]

    print(f"  Source:      {source_name}")
    print(f"  Platform:    {settings['platform']}")
    input_fmt = settings.get('input_format', 'ndjson')
    if input_fmt != 'ndjson':
        print(f"  Input fmt:   {input_fmt}")
        if settings.get('input_csv_delimiter', ',') != ',':
            print(f"  Delimiter:   {repr(settings['input_csv_delimiter'])}")
    print(f"  File format: {settings.get('file_format', 'parquet')}")
    if "parquet_row_group_size" in settings:
        print(f"  Row-group:   {settings['parquet_row_group_size']:,} rows")
    print(f"  Data types:  {', '.join(settings['data_types'])}")
    print(f"  Profiles:    {', '.join(profiles)}")
    print()

    if "parse" in profiles:
        print(f"  Parse:")
        print(f"    Workers:   {settings.get('parse_workers', 4)}")
        print()

    if any(p.startswith("postgres") for p in profiles):
        print(f"  PostgreSQL:")
        if "postgres_ingest" in profiles:
            print(f"    Prefer lingua:       {settings.get('pg_prefer_lingua', False)}")
            print(f"    Index workers:       {settings.get('pg_parallel_index_workers', 4)}")
        if "table_tablespaces" in settings:
            print(f"    Table assignments:")
            for dt, ts in settings["table_tablespaces"].items():
                print(f"      {dt} -> {ts}")
        print()

    if settings["platform"].startswith("custom/"):
        print(f"  Custom Platform:")
        print(f"    Schema:              {settings.get('db_schema', source_name)}")
        if settings.get("custom_indexes"):
            print(f"    PostgreSQL indexes:")
            for dt, idx in settings["custom_indexes"].items():
                print(f"      {dt}: {', '.join(idx)}")
        if settings.get("custom_mongo_indexes"):
            print(f"    MongoDB indexes:")
            for dt, idx in settings["custom_mongo_indexes"].items():
                print(f"      {dt}: {', '.join(idx)}")
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

def main(source_name=None):
    """Add or configure a source.

    Args:
        source_name: Source name. If None, prompts user.
    """
    print()
    print("  Social Data Bridge - Source Configuration")
    print("  ==========================================")
    print()

    # Check that db setup has been done
    db_setup = load_db_setup()
    if db_setup is None:
        print("  No database configuration found. Run 'python sdb.py db setup' first.\n")
        sys.exit(1)

    if source_name is None:
        source_name = ask("Source name (e.g. reddit, twitter_academic)")
        if not source_name:
            print("    Error: A source name is required.")
            sys.exit(1)

    # Check if source already exists
    source_dir = CONFIG_DIR / "sources" / source_name
    if source_dir.exists():
        if not ask_bool(f"Source '{source_name}' already exists. Reconfigure?", False):
            print("\n  Aborted.\n")
            sys.exit(0)

    print(f"\n  Configuring source: {source_name}")
    print(f"  Press Enter to accept defaults shown in [brackets].")
    print()

    hw = detect_hardware()
    settings = run_questionnaire(hw, source_name, db_setup)
    profiles = settings["profiles"]

    # Run classifier configuration inline if ml profiles are selected
    has_classifiers = "lingua" in profiles or "ml" in profiles
    if has_classifiers:
        classifier_state = {
            "platform": settings["platform"],
            "data_types": settings["data_types"],
            "profiles": profiles,
            "source": source_name,
        }
        classifier_settings = run_classifier_questionnaire(hw, classifier_state)
        settings["classifier_settings"] = classifier_settings

    # Build file list
    source_config_dir = CONFIG_DIR / "sources" / source_name
    files_to_write = []

    # Platform config
    if settings["platform"] == "reddit":
        files_to_write.append((
            source_config_dir / "platform.yaml",
            generate_reddit_platform_yaml(settings),
        ))
    elif settings["platform"].startswith("custom/"):
        files_to_write.append((
            source_config_dir / "platform.yaml",
            generate_platform_yaml(settings),
        ))

    # Profile override files
    if "parse" in profiles:
        files_to_write.append((
            source_config_dir / "parse.yaml",
            generate_parse_yaml(settings),
        ))

    if "postgres_ingest" in profiles:
        files_to_write.append((
            source_config_dir / "postgres.yaml",
            generate_postgres_yaml(settings),
        ))

    if "postgres_ml" in profiles:
        files_to_write.append((
            source_config_dir / "postgres_ml.yaml",
            generate_postgres_ml_yaml(settings),
        ))

    if "mongo_ingest" in profiles:
        files_to_write.append((
            source_config_dir / "mongo.yaml",
            generate_mongo_yaml(settings),
        ))

    # Classifier config files (from inline questionnaire)
    if "lingua" in profiles and "classifier_settings" in settings:
        files_to_write.append((
            source_config_dir / "lingua.yaml",
            generate_lingua_user_yaml(settings["classifier_settings"]),
        ))
    if "ml" in profiles and "classifier_settings" in settings:
        files_to_write.append((
            source_config_dir / "ml.yaml",
            generate_ml_user_yaml(settings["classifier_settings"]),
        ))

    # Summary and confirm
    print_summary(settings, files_to_write)

    if not ask_bool("Write these files?", True):
        print("\n  Aborted. No files written.\n")
        sys.exit(0)

    print()
    write_files(files_to_write)

    # Update .env with HF_TOKEN if provided during classifier setup
    cs = settings.get("classifier_settings")
    if cs and cs.get("hf_token"):
        update_env_file({"HF_TOKEN": cs["hf_token"]})
        print(f"  Updated:   .env (HF_TOKEN)")

    print(f"\n  Done! Source '{source_name}' has been configured.")

    # Print next steps
    if settings["platform"] == "reddit":
        print("\n  Next step:")
        print(f"    python sdb.py source configure {source_name}  # Customize Reddit fields/indexes")
        print()
    else:
        print()
        print_pipeline_commands(profiles, source_name)

    return settings
