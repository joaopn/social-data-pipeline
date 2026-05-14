"""Source configuration for Social Data Pipeline.

Configures a data source: platform type, data types, file patterns, fields,
profile settings. Generates config/sources/<name>/ directory with per-profile
override files.

Each source is independent and self-contained.
"""

import sys

try:
    import yaml
except ImportError:
    print("Error: PyYAML is required. Install with: pip install pyyaml")
    sys.exit(1)

from social_data_pipeline.setup.utils import (
    ROOT, CONFIG_DIR,
    detect_hardware,
    ask, ask_int, ask_bool, ask_choice, ask_list, ask_multi_select,
    section_header, write_files, load_db_setup, load_env,
    load_source_config,
    print_pipeline_commands, update_env_file,
    detect_compression_from_glob, derive_file_patterns,
    get_source_profiles,
)
from social_data_pipeline.setup.classifiers import (
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
    d["sr_prefer_lingua"] = "lingua" in profiles
    return d


# ============================================================================
# Interactive questionnaire
# ============================================================================

def _run_hf_config_grouping(hf_defaults):
    """Interactive HF config grouping: assign configs to SDP data_types.

    Shows schema groups with their configs, lets user name each data_type
    and select which configs to include.

    Returns:
        (data_types, hf_config_map, selected_fields, selected_field_types, excluded_fields)
    """
    schema_groups = hf_defaults["schema_groups"]
    fields_by_group = hf_defaults["fields_by_group"]
    field_types_by_group = hf_defaults["field_types_by_group"]

    section_header("HF Dataset Config Grouping")
    print("  HF datasets organize data into 'configs' (splits by year, topic, etc.)")
    print("  Group configs that should become the same SDP data type.\n")

    data_types = []
    hf_config_map = {}
    all_selected_fields = {}
    all_field_types = {}
    all_excluded_fields = {}

    for gi, group in enumerate(schema_groups):
        configs = group["configs"]
        field_names = group["field_names"]
        fields_info = fields_by_group[gi]
        group_field_types = field_types_by_group[gi]

        # Show schema summary
        preview = ", ".join(field_names[:6])
        if len(field_names) > 6:
            preview += ", ..."
        print(f"  Schema {chr(65 + gi)} ({len(configs)} config(s)): {preview}")

        # Show configs with row counts
        config_names = []
        for cfg in configs:
            rows = cfg.get("num_rows")
            row_str = f"  ({rows:,} rows)" if rows else ""
            print(f"    - {cfg['name']}{row_str}")
            config_names.append(cfg["name"])

        # Let user select configs
        selected_configs = ask_multi_select(
            "Include configs:", config_names, config_names,
            tag=f"hf_include_configs_{gi}",
        )
        if not selected_configs:
            print("    Skipping this schema group (no configs selected).")
            print()
            continue

        # Suggest data_type name from common prefix or first config
        suggested_name = _suggest_data_type_name(selected_configs)
        dt_name = ask("  Data type name for this group", suggested_name, tag=f"hf_data_type_name_{gi}")
        if not dt_name:
            dt_name = suggested_name

        data_types.append(dt_name)
        hf_config_map[dt_name] = selected_configs

        # Field selection for this data_type
        print(f"\n  Fields for '{dt_name}':")
        mappable_fields = [f for f in fields_info if f["mappable"]]
        unmappable_fields = [f for f in fields_info if not f["mappable"]]

        # Select from mappable fields
        mappable_names = [f["name"] for f in mappable_fields]
        selected = ask_multi_select(
            "Select fields:", mappable_names, mappable_names,
            tag=f"hf_select_fields_{gi}",
        )
        selected_set = set(selected)

        # Handle unmappable fields (ask per field)
        for f in unmappable_fields:
            print(f"\n    Field '{f['name']}' has type '{f['hf_type']}' (no direct SQL mapping)")
            choice = ask_choice(
                f"    Action for '{f['name']}':",
                ["skip", "cast to text", "enter custom type"],
                default="skip",
                tag=f"hf_unmappable_{f['name']}",
            )
            if choice == "cast to text":
                selected.append(f["name"])
                selected_set.add(f["name"])
                group_field_types[f["name"]] = "text"
            elif choice == "enter custom type":
                custom_type = ask(f"    SQL type for '{f['name']}'", tag=f"hf_custom_type_{f['name']}")
                if custom_type:
                    selected.append(f["name"])
                    selected_set.add(f["name"])
                    group_field_types[f["name"]] = custom_type

        all_selected_fields[dt_name] = selected

        # Track excluded fields (all group fields minus selected)
        all_group_names = [f["name"] for f in fields_info]
        excluded = [n for n in all_group_names if n not in selected_set]
        if excluded:
            all_excluded_fields[dt_name] = excluded

        # Merge field types (only for selected fields)
        for fname, ftype in group_field_types.items():
            if fname in selected_set:
                if fname in all_field_types and all_field_types[fname] != ftype:
                    print(f"    Warning: type conflict for '{fname}': "
                          f"{all_field_types[fname]} vs {ftype} (keeping first)")
                else:
                    all_field_types[fname] = ftype

        print()

    if not data_types:
        print("    Error: At least one data type is required.")
        sys.exit(1)

    return data_types, hf_config_map, all_selected_fields, all_field_types, all_excluded_fields


def _suggest_data_type_name(config_names):
    """Suggest a data_type name from a list of HF config names.

    Tries to find a common prefix (e.g., "comments-2020", "comments-2021" → "comments").
    Falls back to first config name.
    """
    if len(config_names) == 1:
        return config_names[0]

    # Try common prefix up to a separator
    first = config_names[0]
    for sep in ("-", "_", "."):
        prefix = first.split(sep)[0]
        if prefix and all(c.startswith(prefix) for c in config_names):
            return prefix

    return config_names[0]


def _load_existing_source_config(source_name):
    """Load existing source configuration for use as defaults on re-run."""
    existing = {}
    source_dir = CONFIG_DIR / "sources" / source_name

    # Load platform.yaml
    pc = load_source_config(source_name) or {}
    if pc:
        if pc.get("data_types"):
            existing["data_types"] = pc["data_types"]
        paths = pc.get("paths", {})
        for key in ("dumps", "extracted", "parsed", "output"):
            if paths.get(key):
                existing[f"{key}_path"] = paths[key]
        if pc.get("file_format"):
            existing["file_format"] = pc["file_format"]
        if pc.get("parquet_row_group_size"):
            existing["parquet_row_group_size"] = pc["parquet_row_group_size"]
        if pc.get("db_schema"):
            existing["db_schema"] = pc["db_schema"]
        if pc.get("input_format"):
            existing["input_format"] = pc["input_format"]
        if pc.get("input_csv_delimiter"):
            existing["input_csv_delimiter"] = pc["input_csv_delimiter"]
        if pc.get("mongo_collection_strategy"):
            existing["mongo_collection_strategy"] = pc["mongo_collection_strategy"]
        if pc.get("mongo_db_name"):
            existing["mongo_db_name"] = pc["mongo_db_name"]
        if pc.get("mongo_collections"):
            existing["mongo_collections"] = pc["mongo_collections"]
        if pc.get("indexes"):
            existing["custom_indexes"] = pc["indexes"]
        if pc.get("mongo_indexes"):
            existing["custom_mongo_indexes"] = pc["mongo_indexes"]
        if pc.get("sr_indexes"):
            existing["custom_sr_indexes"] = pc["sr_indexes"]
        if pc.get("file_patterns"):
            existing["file_patterns"] = pc["file_patterns"]
        if pc.get("fields"):
            existing["custom_fields"] = pc["fields"]
        if pc.get("field_types"):
            existing["custom_field_types"] = pc["field_types"]
        if pc.get("primary_key"):
            existing["primary_key"] = pc["primary_key"]
        if pc.get("sr_buckets") is not None:
            existing["sr_buckets"] = pc["sr_buckets"]
        if pc.get("hf_dataset"):
            existing["hf_dataset"] = pc["hf_dataset"]
        if pc.get("hf_config_map"):
            existing["hf_config_map"] = pc["hf_config_map"]
        if pc.get("mongo_exclude_fields"):
            existing["mongo_exclude_fields"] = pc["mongo_exclude_fields"]

    # Load parse.yaml
    parse_path = source_dir / "parse.yaml"
    if parse_path.exists():
        try:
            prc = yaml.safe_load(parse_path.read_text()) or {}
            proc = prc.get("pipeline", {}).get("processing", {})
            if proc.get("parse_workers") is not None:
                existing["parse_workers"] = proc["parse_workers"]
        except (OSError, yaml.YAMLError):
            pass

    # Load postgres.yaml
    pg_path = source_dir / "postgres.yaml"
    if pg_path.exists():
        try:
            pgc = yaml.safe_load(pg_path.read_text()) or {}
            proc = pgc.get("pipeline", {}).get("processing", {})
            if proc.get("prefer_lingua") is not None:
                existing["pg_prefer_lingua"] = proc["prefer_lingua"]
            if proc.get("parallel_index_workers") is not None:
                existing["pg_parallel_index_workers"] = proc["parallel_index_workers"]
            tts = pgc.get("pipeline", {}).get("table_tablespaces")
            if tts:
                existing["table_tablespaces"] = tts
        except (OSError, yaml.YAMLError):
            pass

    # Load starrocks.yaml
    sr_path = source_dir / "starrocks.yaml"
    if sr_path.exists():
        try:
            src = yaml.safe_load(sr_path.read_text()) or {}
            proc = src.get("pipeline", {}).get("processing", {})
            if proc.get("prefer_lingua") is not None:
                existing["sr_prefer_lingua"] = proc["prefer_lingua"]
        except (OSError, yaml.YAMLError):
            pass

    return existing


def run_questionnaire(hw, source_name, db_setup, hf_defaults=None):
    """Run the source configuration questionnaire. Returns settings dict."""
    existing = _load_existing_source_config(source_name)
    settings = {}
    settings["source_name"] = source_name

    # Determine platform type from source name
    if source_name == "reddit":
        platform = "reddit"
    else:
        platform = f"custom/{source_name}"
    settings["platform"] = platform

    is_hf = hf_defaults is not None or bool(existing.get("hf_dataset"))

    # ---- HF config grouping (replaces data types question) ----
    if is_hf:
        if hf_defaults is not None:
            # Fresh `--hf` add: run the interactive grouping.
            data_types, hf_config_map, hf_fields, hf_field_types, hf_excluded = (
                _run_hf_config_grouping(hf_defaults)
            )
            settings["data_types"] = data_types
            settings["hf_config_map"] = hf_config_map
            settings["custom_fields"] = hf_fields
            settings["custom_field_types"] = hf_field_types
            if hf_excluded:
                settings["mongo_exclude_fields"] = hf_excluded
        else:
            # Reconfigure of an HF source — reuse the saved grouping/fields/types.
            data_types = existing.get("data_types", [])
            settings["data_types"] = data_types
            settings["hf_config_map"] = existing.get("hf_config_map", {})
            settings["custom_fields"] = existing.get("custom_fields", {})
            settings["custom_field_types"] = existing.get("custom_field_types", {})
            if existing.get("mongo_exclude_fields"):
                settings["mongo_exclude_fields"] = existing["mongo_exclude_fields"]
            if existing.get("hf_dataset"):
                settings["hf_dataset"] = existing["hf_dataset"]

    # ---- Data types (non-HF) ----
    elif platform == "reddit":
        section_header("Data Types")
        data_types = ask_list("Data types", existing.get("data_types", ["submissions", "comments"]), tag="src_data_types")
        settings["data_types"] = data_types
    else:
        section_header("Data Types")
        data_types = ask_list("Data types (comma-separated)", existing.get("data_types"), tag="src_data_types")
        if not data_types:
            print("    Error: At least one data type is required.")
            sys.exit(1)
        settings["data_types"] = data_types

    # ---- Data paths ----
    section_header("Data Paths")
    data_path = load_env().get("DATA_PATH", "./data")
    settings["dumps_path"] = ask("Dumps directory", existing.get("dumps_path", f"{data_path}/dumps/{source_name}"), tag="src_dumps_path")
    settings["extracted_path"] = ask("Extracted directory", existing.get("extracted_path", f"{data_path}/extracted/{source_name}"), tag="src_extracted_path")
    settings["parsed_path"] = ask("Parsed directory", existing.get("parsed_path", f"{data_path}/parsed/{source_name}"), tag="src_parsed_path")
    settings["output_path"] = ask("Output directory", existing.get("output_path", f"{data_path}/output/{source_name}"), tag="src_output_path")

    # ---- File format ----
    section_header("File Format")
    print("  Parquet files are smaller, enforce schema, and preserve text")
    print("  without escaping. CSV is the legacy format.")
    settings["file_format"] = ask_choice(
        "Intermediate file format", ["parquet", "csv"],
        default=existing.get("file_format", "parquet"),
        tag="src_file_format",
    )

    if settings["file_format"] == "parquet":
        rg_size = ask_int("Parquet row-group size (larger = better compression, more RAM)", existing.get("parquet_row_group_size", 1_000_000), tag="src_parquet_rg_size")
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
    if "starrocks" in databases:
        all_profiles += ["sr_ingest", "sr_ml"]

    # When reconfiguring an existing source, only pre-check profiles that don't
    # already have a config file — avoids overwriting working configurations.
    existing_profiles = get_source_profiles(source_name)
    if existing_profiles:
        new_profiles = [p for p in all_profiles if p not in existing_profiles]
        # Remove ml if no GPU, but keep all existing regardless
        default_profiles = new_profiles if gpus else [p for p in new_profiles if p != "ml"]
        if new_profiles:
            print(f"  Already configured: {', '.join(existing_profiles)}")
            print(f"  New profiles available: {', '.join(new_profiles)}")
        else:
            print(f"  All profiles already configured: {', '.join(existing_profiles)}")
    else:
        default_profiles = all_profiles[:] if gpus else [p for p in all_profiles if p != "ml"]
    profiles = ask_multi_select("Profiles to configure:", all_profiles, default_profiles, tag="src_profiles")
    settings["profiles"] = profiles

    # ---- Compute defaults ----
    defaults = compute_defaults(hw, profiles)

    # ---- Parse settings ----
    if "parse" in profiles:
        section_header("Parse Settings")
        settings["parse_workers"] = ask_int("Parse workers", existing.get("parse_workers", defaults["parse_workers"]), tag="src_parse_workers")

    # ---- PostgreSQL settings (per-source) ----
    has_postgres = any(p.startswith("postgres") for p in profiles)
    has_mongo = "mongo_ingest" in profiles
    has_starrocks = "sr_ingest" in profiles
    if has_postgres:
        section_header("PostgreSQL Settings (for this source)")

        if "postgres_ingest" in profiles:
            settings["pg_prefer_lingua"] = ask_bool(
                "Use Lingua files (includes lang columns in base tables)?",
                existing.get("pg_prefer_lingua", defaults["pg_prefer_lingua"]),
                tag="src_pg_prefer_lingua",
            )
            settings["pg_parallel_index_workers"] = ask_int(
                "Parallel index workers",
                existing.get("pg_parallel_index_workers", defaults["pg_parallel_index_workers"]),
                tag="src_pg_index_workers",
            )

        # Tablespace assignments (if tablespaces were configured in db setup)
        db_tablespaces = db_setup.get("tablespaces", {})
        if db_tablespaces:
            ts_choices = ["pgdata"] + list(db_tablespaces.keys())
            print()
            print("  Assign each data type to a tablespace:")
            print(f"    Available: {', '.join(ts_choices)}")
            existing_ts = existing.get("table_tablespaces", {})
            ts_assignments = {}
            for dt in data_types:
                ts = ask_choice(
                    f"  {dt} tablespace:",
                    ts_choices,
                    default=existing_ts.get(dt, "pgdata"),
                    tag=f"src_tablespace_{dt}",
                )
                ts_assignments[dt] = ts
            settings["table_tablespaces"] = ts_assignments
            settings["tablespaces"] = db_tablespaces

    # ---- Custom platform details ----
    if platform.startswith("custom/"):
        section_header("Custom Platform Configuration")

        if has_postgres:
            settings["db_schema"] = ask("PostgreSQL schema name", existing.get("db_schema", source_name), tag="src_db_schema")

        if is_hf:
            # HF sources: input_format is parquet, file patterns auto-generated
            settings["input_format"] = "parquet"
            settings["custom_file_patterns"] = {}
            for dt in data_types:
                settings["custom_file_patterns"][dt] = {
                    "parquet": r"^.+\.parquet$",
                    "csv": r"^.+\.csv$",
                }
            # Fields and field_types already set from HF config grouping step
        else:
            # ---- Input format ----
            section_header("Input Format")
            print("  NDJSON: one JSON object per line (default for most data dumps)")
            print("  CSV: comma/tab/pipe-separated values with headers")
            settings["input_format"] = ask_choice(
                "Raw input file format", ["ndjson", "csv"],
                default=existing.get("input_format", "ndjson"),
                tag="src_input_format",
            )
            if settings["input_format"] == "csv":
                delimiter = ask("CSV delimiter character (comma=, tab=\\t pipe=|)", existing.get("input_csv_delimiter", ","), tag="src_csv_delimiter")
                if delimiter == "\\t":
                    delimiter = "\t"
                settings["input_csv_delimiter"] = delimiter

            settings["custom_file_patterns"] = {}
            for dt in data_types:
                print(f"\n  File patterns for '{dt}':")
                print("    Enter a glob pattern for your compressed dump files.")
                print("    Examples: tweets_*.json.gz, RC_*.zst, data_*.csv.xz")
                print()
                existing_glob = (existing.get("file_patterns", {}).get(dt, {}) or {}).get("dump_glob")
                dump_glob = ask(f"    Dump file glob pattern for {dt}", existing_glob, tag=f"src_dump_glob_{dt}")
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
                        tag=f"src_compression_{dt}",
                    )

                input_format = settings.get("input_format", "ndjson")
                patterns = derive_file_patterns(dump_glob, compression, input_format=input_format)
                settings["custom_file_patterns"][dt] = patterns
                print(f"    Detected: compression={compression}, prefix={patterns['prefix']}")

            print()
            print("    Note: For uncompressed data, place files directly in")
            print("    data/extracted/<source>/<data_type>/ — the pipeline will")
            print("    pick them up automatically with no dump files needed.")

            # ---- Fields per data type ----
            section_header("Fields")
            print("  Field names to extract from each input record. Use dot")
            print("  notation for nested fields (e.g., user.name).")
            print("  Field types default to TEXT; refine in platform.yaml after setup.")
            existing_fields = existing.get("custom_fields") or {}
            settings["custom_fields"] = {}
            for dt in data_types:
                while True:
                    fields = ask_list(
                        f"  Fields for '{dt}'",
                        existing_fields.get(dt),
                        tag=f"src_fields_{dt}",
                    )
                    if fields:
                        break
                    print(f"    Error: at least one field is required for '{dt}'.")
                settings["custom_fields"][dt] = fields

        if has_mongo:
            print()
            settings["mongo_collection_strategy"] = ask_choice(
                "MongoDB collection strategy:",
                ["per_data_type", "per_file"],
                default=existing.get("mongo_collection_strategy", "per_data_type"),
                tag="src_mongo_strategy",
            )
            if settings["mongo_collection_strategy"] == "per_data_type":
                settings["mongo_collections"] = {}
                for dt in data_types:
                    existing_coll = existing.get("mongo_collections", {}).get(dt, dt)
                    coll = ask(f"  MongoDB collection name for '{dt}'", existing_coll, tag=f"src_mongo_collection_{dt}")
                    settings["mongo_collections"][dt] = coll
            else:
                print("    (per_file: one collection per input file, named from filename)")

            settings["mongo_db_name"] = ask("MongoDB database name", existing.get("mongo_db_name", source_name), tag="src_mongo_db_name")

        # ---- Index configuration ----
        if has_postgres or has_mongo or has_starrocks:
            section_header("Database Indexes")
            print("  Indexes speed up queries on specific columns.")
            print("  Enter column names (comma-separated) to index per data type.")
            print("  Leave empty for no indexes.")

            if has_postgres:
                print()
                existing_pg_idx = existing.get("custom_indexes", {})
                settings["custom_indexes"] = {}
                for dt in data_types:
                    idx = ask_list(f"  PostgreSQL indexes for '{dt}'", existing_pg_idx.get(dt), tag=f"src_pg_indexes_{dt}")
                    if idx:
                        settings["custom_indexes"][dt] = idx

            if has_mongo:
                print()
                existing_mongo_idx = existing.get("custom_mongo_indexes", {})
                settings["custom_mongo_indexes"] = {}
                for dt in data_types:
                    idx = ask_list(f"  MongoDB indexes for '{dt}'", existing_mongo_idx.get(dt), tag=f"src_mongo_indexes_{dt}")
                    if idx:
                        settings["custom_mongo_indexes"][dt] = idx

            if has_starrocks:
                print()
                existing_sr_idx = existing.get("custom_sr_indexes", {})
                settings["custom_sr_indexes"] = {}
                for dt in data_types:
                    idx = ask_list(f"  StarRocks BITMAP indexes for '{dt}'", existing_sr_idx.get(dt), tag=f"src_sr_indexes_{dt}")
                    if idx:
                        settings["custom_sr_indexes"][dt] = idx

        # Primary key (custom platforms only — Reddit has `id` hardcoded in its template).
        # Needed whenever a DB profile performs PK-based upsert/dedup.
        if has_postgres or has_starrocks:
            section_header("Primary Key")
            print("  Column used as the table's primary key. Required for")
            print("  duplicate detection and upsert behavior.")
            print()
            # Default: "id" if present anywhere in configured fields, else first field
            configured_fields = settings.get("custom_fields") or {}
            all_field_names = sorted({f for flist in configured_fields.values() for f in (flist or [])})
            if existing.get("primary_key"):
                default_pk = existing["primary_key"]
            elif "id" in all_field_names:
                default_pk = "id"
            elif all_field_names:
                default_pk = all_field_names[0]
            else:
                default_pk = "id"
            settings["primary_key"] = ask("Primary key column", default_pk, tag="src_primary_key")

    # ---- StarRocks settings (per-source) ----
    if has_starrocks:
        section_header("StarRocks Settings (for this source)")
        settings["sr_prefer_lingua"] = ask_bool(
            "Use Lingua files (includes lang columns in base tables)?",
            existing.get("sr_prefer_lingua", defaults["sr_prefer_lingua"]),
            tag="src_sr_prefer_lingua",
        )

        print()
        cores = hw["cpu_cores"] or 4
        default_buckets = cores * 8
        print("  Splits each StarRocks table into tablets — the physical storage")
        print("  units that govern compaction granularity, disk distribution,")
        print("  and scan scheduling on the BE. Compute parallelism is set")
        print("  separately by the BE's pipeline_dop (bucket count does not cap")
        print("  it, thanks to morsel-driven sub-tablet parallelism). Target is")
        print(f"  roughly 1–10 GB per tablet; default = {cores} × 8 = {default_buckets}")
        print("  covers datasets in the 100 GB – 10 TB range. Raise for larger")
        print("  datasets, drop for very small sources (<10 GB).")
        print()
        settings["sr_buckets"] = ask_int(
            "Number of buckets per table",
            existing.get("sr_buckets", default_buckets),
            tag="src_sr_buckets",
        )

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

    # HF-specific keys
    if "hf_dataset" in settings:
        config["hf_dataset"] = settings["hf_dataset"]
    if "hf_config_map" in settings:
        config["hf_config_map"] = settings["hf_config_map"]
    if "mongo_exclude_fields" in settings:
        config["mongo_exclude_fields"] = settings["mongo_exclude_fields"]

    config["indexes"] = settings.get("custom_indexes", {})

    if settings.get("custom_sr_indexes"):
        config["sr_indexes"] = settings["custom_sr_indexes"]

    if settings.get("primary_key"):
        config["primary_key"] = settings["primary_key"]

    if settings.get("sr_buckets") is not None:
        config["sr_buckets"] = settings["sr_buckets"]

    # Field types: use HF-derived types if available, otherwise generic defaults
    if settings.get("custom_field_types"):
        field_types = dict(settings["custom_field_types"])
        # Always include lingua fields for compatibility
        field_types.update({
            "lang": ["varchar", 2],
            "lang_prob": "float",
            "lang2": ["varchar", 2],
            "lang2_prob": "float",
            "lang_chars": "integer",
        })
    else:
        field_types = {
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
        }
    config["field_types"] = field_types

    # Fields are populated either by the HF metadata flow or by the per-data-type
    # prompt in the non-HF custom flow. Either way, an empty list here means setup
    # was bypassed and downstream profiles will fail; surface that immediately.
    if not settings.get("custom_fields"):
        raise ValueError(
            "No fields configured. Run `sdp source add` interactively or set "
            "settings['custom_fields'] before calling generate_platform_yaml()."
        )
    config["fields"] = settings["custom_fields"]

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
    if settings.get("sr_buckets") is not None:
        base_config["sr_buckets"] = settings["sr_buckets"]
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


def generate_sr_ml_yaml(settings):
    """Generate config/sources/<name>/sr_ml.yaml."""
    config = {
        "pipeline": {
            "processing": {
                "data_types": settings["data_types"],
            }
        }
    }
    return yaml.dump(config, default_flow_style=False, sort_keys=False)


def generate_starrocks_yaml(settings):
    """Generate config/sources/<name>/starrocks.yaml."""
    processing = {
        "data_types": settings["data_types"],
    }
    if "sr_prefer_lingua" in settings:
        processing["prefer_lingua"] = settings["sr_prefer_lingua"]
    config = {
        "pipeline": {
            "processing": processing,
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
    if settings.get("hf_dataset"):
        print(f"  HF dataset:  {settings['hf_dataset']}")
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
        print("  Parse:")
        print(f"    Workers:   {settings.get('parse_workers', 4)}")
        print()

    if any(p.startswith("postgres") for p in profiles):
        print("  PostgreSQL:")
        if "postgres_ingest" in profiles:
            print(f"    Prefer lingua:       {settings.get('pg_prefer_lingua', False)}")
            print(f"    Index workers:       {settings.get('pg_parallel_index_workers', 4)}")
        if "table_tablespaces" in settings:
            print("    Table assignments:")
            for dt, ts in settings["table_tablespaces"].items():
                print(f"      {dt} -> {ts}")
        print()

    if "sr_ingest" in profiles:
        print("  StarRocks:")
        print(f"    Prefer lingua:       {settings.get('sr_prefer_lingua', False)}")
        if settings.get("sr_buckets") is not None:
            print(f"    Buckets per table:   {settings['sr_buckets']}")
        print()

    if settings["platform"].startswith("custom/"):
        print("  Custom Platform:")
        print(f"    Schema:              {settings.get('db_schema', source_name)}")
        if settings.get("custom_indexes"):
            print("    PostgreSQL indexes:")
            for dt, idx in settings["custom_indexes"].items():
                print(f"      {dt}: {', '.join(idx)}")
        if settings.get("custom_mongo_indexes"):
            print("    MongoDB indexes:")
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

def main(source_name=None, hf_dataset_id=None):
    """Add or configure a source.

    Args:
        source_name: Source name. If None, prompts user.
        hf_dataset_id: Optional HF dataset ID (e.g. "user/dataset-name").
            When provided, fetches metadata to pre-populate setup defaults.
    """
    print()
    print("  Social Data Pipeline - Source Configuration")
    print("  ==========================================")
    print()

    # Check that db setup has been done
    db_setup = load_db_setup()
    if db_setup is None:
        print("  No database configuration found. Run 'python sdp.py db setup' first.\n")
        sys.exit(1)

    if source_name is None:
        source_name = ask("Source name (e.g. reddit, twitter_academic)", tag="src_name")
        if not source_name:
            print("    Error: A source name is required.")
            sys.exit(1)

    # Check if source already exists
    source_dir = CONFIG_DIR / "sources" / source_name
    if source_dir.exists():
        if not ask_bool(f"Source '{source_name}' already exists. Reconfigure?", False, tag="src_reconfigure"):
            print("\n  Aborted.\n")
            sys.exit(0)

    # Fetch HF metadata if dataset ID provided
    hf_defaults = None
    if hf_dataset_id:
        from social_data_pipeline.setup.hf import extract_hf_defaults, HFAPIError
        print(f"  Fetching metadata for HF dataset: {hf_dataset_id}")
        try:
            import os
            token = os.environ.get("HF_TOKEN")
            hf_defaults = extract_hf_defaults(hf_dataset_id, token=token)
            n_configs = len(hf_defaults["all_configs"])
            n_groups = len(hf_defaults["schema_groups"])
            print(f"  Found {n_configs} configs in {n_groups} schema group(s).\n")
        except HFAPIError as e:
            print(f"  Error: {e}\n")
            sys.exit(1)

    print(f"\n  Configuring source: {source_name}")
    print("  Press Enter to accept defaults shown in [brackets].")
    print()

    hw = detect_hardware()
    settings = run_questionnaire(hw, source_name, db_setup, hf_defaults=hf_defaults)
    if hf_dataset_id:
        settings["hf_dataset"] = hf_dataset_id
    profiles = settings["profiles"]

    # Run classifier configuration inline if ml profiles are selected
    has_classifiers = "lingua" in profiles or "ml" in profiles
    if has_classifiers:
        classifier_state = {
            "platform": settings["platform"],
            "data_types": settings["data_types"],
            "profiles": profiles,
            "source": source_name,
            "primary_key": settings.get("primary_key"),
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

    if "sr_ingest" in profiles:
        files_to_write.append((
            source_config_dir / "starrocks.yaml",
            generate_starrocks_yaml(settings),
        ))

    if "sr_ml" in profiles:
        files_to_write.append((
            source_config_dir / "sr_ml.yaml",
            generate_sr_ml_yaml(settings),
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

    if not ask_bool("Write these files?", True, tag="src_write_files"):
        print("\n  Aborted. No files written.\n")
        sys.exit(0)

    print()
    write_files(files_to_write)

    # Update .env with HF_TOKEN if provided during classifier setup
    cs = settings.get("classifier_settings")
    if cs and cs.get("hf_token"):
        update_env_file({"HF_TOKEN": cs["hf_token"]})
        print("  Updated:   .env (HF_TOKEN)")

    print(f"\n  Done! Source '{source_name}' has been configured.")

    # Print next steps
    if settings.get("hf_dataset"):
        print("\n  Next steps:")
        print(f"    python sdp.py source download {source_name}   # Download HF parquet files")
        print(f"    python sdp.py run parse --source {source_name}  # Select/clean columns")
        print()
        print_pipeline_commands(
            [p for p in profiles if p != "parse"], source_name,
        )
    elif settings["platform"] == "reddit":
        print("\n  Next step:")
        print(f"    python sdp.py source configure {source_name}  # Customize Reddit fields/indexes")
        print()
    else:
        print()
        print_pipeline_commands(profiles, source_name)

    return settings
