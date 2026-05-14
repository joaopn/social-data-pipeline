"""Classifier configuration for Social Data Pipeline.

Configures language detection (Lingua) and GPU transformer classifiers.
Generates config/lingua/user.yaml, config/ml/user.yaml, and updates .env.
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
    ask, ask_int, ask_bool, ask_list, ask_multi_select, ask_password,
    section_header, require_source_state, update_env_file, write_files,
    print_pipeline_commands, SOURCES_DIR,
)


# ============================================================================
# Reddit defaults for classifier configs
# ============================================================================

REDDIT_TEXT_COLUMNS = {
    "submissions": ["title", "selftext"],
    "comments": ["body"],
}

REDDIT_REMOVE_STRINGS = ["[deleted]", "[removed]", "[unavailable]"]

REDDIT_REMOVE_PATTERNS = [
    r"https?://\S+",
    r"/?r/\w+",
    r"/?u/\w+",
]

REDDIT_GPU_FIELDS = ["author", "subreddit"]


# ============================================================================
# Default heuristics
# ============================================================================

def compute_classifier_defaults(hw, profiles):
    """Compute suggested defaults for classifier settings."""
    cores = hw["cpu_cores"] or 4
    gpus = hw["gpus"]

    d = {}

    # Lingua — read batch_size from base config
    base_lingua = {}
    try:
        lingua_config_path = CONFIG_DIR / "lingua" / "cpu_classifiers.yaml"
        if lingua_config_path.exists():
            base_lingua_raw = yaml.safe_load(lingua_config_path.read_text()) or {}
            base_lingua = base_lingua_raw.get("lingua", {})
    except Exception:
        pass

    d["lingua_workers"] = cores
    d["lingua_file_workers"] = max(1, cores // 8)
    d["lingua_batch_size"] = base_lingua.get("batch_size", 2_000_000)
    d["lingua_low_accuracy"] = False

    # GPU ML — read defaults from base config (gpu_classifiers.yaml)
    base_ml = {}
    try:
        base_config_path = CONFIG_DIR / "ml" / "gpu_classifiers.yaml"
        if base_config_path.exists():
            base_ml = yaml.safe_load(base_config_path.read_text()) or {}
    except Exception:
        pass

    d["gpu_ids"] = [g["index"] for g in gpus] if gpus else [0]
    d["ml_file_workers"] = max(1, len(gpus))
    d["ml_tokenize_workers"] = min(max(1, cores // 2), 8)
    d["ml_classifier_batch_size"] = base_ml.get("classifier_batch_size", 16)

    # Collect classifier names from base config (top-level keys with dict values that have 'type')
    base_classifiers = [
        k for k, v in base_ml.items()
        if isinstance(v, dict) and "type" in v
    ]
    d["ml_classifiers"] = base_classifiers or ["toxic_roberta", "go_emotions"]

    return d


# ============================================================================
# Load existing configuration
# ============================================================================

def _load_existing_classifier_config(source_name):
    """Load existing classifier configuration for use as defaults on re-run."""
    existing = {}
    source_dir = SOURCES_DIR / source_name

    # Load lingua.yaml
    lingua_path = source_dir / "lingua.yaml"
    if lingua_path.exists():
        try:
            lc = yaml.safe_load(lingua_path.read_text()) or {}
            lingua = lc.get("cpu_classifiers", {}).get("lingua", {})
            if lingua.get("workers") is not None:
                existing["lingua_workers"] = lingua["workers"]
            if lingua.get("file_workers") is not None:
                existing["lingua_file_workers"] = lingua["file_workers"]
            if lingua.get("batch_size") is not None:
                existing["lingua_batch_size"] = lingua["batch_size"]
            if lingua.get("low_accuracy") is not None:
                existing["lingua_low_accuracy"] = lingua["low_accuracy"]
            tc = lc.get("cpu_classifiers", {}).get("text_columns")
            if tc:
                existing["text_columns"] = tc
            rs = lc.get("cpu_classifiers", {}).get("remove_strings")
            if rs is not None:
                existing["remove_strings"] = rs
            rp = lc.get("cpu_classifiers", {}).get("remove_patterns")
            if rp is not None:
                existing["remove_patterns"] = rp
        except (OSError, yaml.YAMLError):
            pass

    # Load ml.yaml
    ml_path = source_dir / "ml.yaml"
    if ml_path.exists():
        try:
            mc = yaml.safe_load(ml_path.read_text()) or {}
            gc = mc.get("gpu_classifiers", {})
            if gc.get("gpu_ids") is not None:
                existing["gpu_ids"] = gc["gpu_ids"]
            if gc.get("file_workers") is not None:
                existing["ml_file_workers"] = gc["file_workers"]
            if gc.get("tokenize_workers") is not None:
                existing["ml_tokenize_workers"] = gc["tokenize_workers"]
            if gc.get("classifier_batch_size") is not None:
                existing["ml_classifier_batch_size"] = gc["classifier_batch_size"]
            if gc.get("fields") is not None:
                existing["gpu_fields"] = gc["fields"]
            tc = gc.get("text_columns")
            if tc and "text_columns" not in existing:
                existing["text_columns"] = tc
            gpu_cls = mc.get("pipeline", {}).get("gpu_classifiers")
            if gpu_cls:
                # Normalize to [{name, data_types}]; data_types=None means "all"
                normalized = []
                for e in gpu_cls:
                    if isinstance(e, str):
                        normalized.append({"name": e, "data_types": None})
                    elif isinstance(e, dict) and "name" in e:
                        normalized.append({
                            "name": e["name"],
                            "data_types": e.get("data_types"),
                        })
                existing["ml_classifiers"] = normalized
        except (OSError, yaml.YAMLError):
            pass

    return existing


# ============================================================================
# Platform-aware text_columns
# ============================================================================

def ask_text_columns(platform, data_types, existing=None, primary_key=None):
    """Get text_columns configuration based on platform.

    For Reddit, uses known defaults. For other platforms, asks the user.
    Returns (text_columns, remove_strings, remove_patterns, fields) tuple.
    """
    if platform == "reddit":
        # Filter to only the data_types the user selected
        text_columns = {dt: REDDIT_TEXT_COLUMNS[dt] for dt in data_types if dt in REDDIT_TEXT_COLUMNS}
        return text_columns, REDDIT_REMOVE_STRINGS, REDDIT_REMOVE_PATTERNS, REDDIT_GPU_FIELDS

    if existing is None:
        existing = {}

    # Non-Reddit: ask the user
    section_header("Text Column Configuration")
    print("  Classifiers need to know which columns contain text to process.")
    print("  For each data type, specify the column names that contain text.")
    print()

    existing_tc = existing.get("text_columns", {})
    text_columns = {}
    for dt in data_types:
        cols = ask_list(f"Text columns for '{dt}' (comma-separated)", existing_tc.get(dt), tag=f"cl_text_columns_{dt}")
        if cols:
            text_columns[dt] = cols
        else:
            print(f"    Warning: No text columns for '{dt}'. Classifiers may skip this type.")

    print()
    remove_strings = ask_list(
        "Exact strings to remove before classification (comma-separated, or Enter for none)",
        existing.get("remove_strings"),
        tag="cl_remove_strings",
    )
    existing_rp = existing.get("remove_patterns", [r"https?://\S+"])
    remove_patterns_str = ask(
        "Regex patterns to remove (comma-separated)",
        ", ".join(existing_rp) if existing_rp else r"https?://\S+",
        tag="cl_remove_patterns",
    )
    remove_patterns = [p.strip() for p in remove_patterns_str.split(",") if p.strip()]

    pk_hint = f" ({primary_key} is included automatically)" if primary_key else ""
    existing_extras = [f for f in (existing.get("gpu_fields") or []) if f != primary_key]
    extras = ask_list(
        f"Extra fields to keep in GPU classifier output{pk_hint}",
        existing_extras or None,
        tag="cl_extra_fields",
    )
    # Auto-prepend the PK so the classifier output retains the join column on
    # custom platforms (where platform.mandatory_fields is empty, see transformer.py).
    fields = ([primary_key] if primary_key else []) + [f for f in extras if f != primary_key]

    return text_columns, remove_strings, remove_patterns, fields


# ============================================================================
# Interactive questionnaire
# ============================================================================

def run_questionnaire(hw, state):
    """Run the classifier questionnaire. Returns settings dict."""
    settings = {}
    profiles = state["profiles"]
    platform = state["platform"]
    data_types = state["data_types"]
    source_name = state.get("source")
    existing = _load_existing_classifier_config(source_name) if source_name else {}

    settings["platform"] = platform
    settings["data_types"] = data_types
    settings["profiles"] = profiles

    # --- Print hardware summary ---
    section_header("Hardware Detected")
    gpus = hw["gpus"]
    print(f"  CPU cores: {hw['cpu_cores'] or 'unknown'}")
    print(f"  RAM:       {hw['ram_gb'] or 'unknown'} GB")
    if gpus:
        for g in gpus:
            print(f"  GPU {g['index']}:    {g['name']} ({g['vram_mb']} MB)")
    else:
        print("  GPUs:      none detected")
    print()

    # Compute defaults
    defaults = compute_classifier_defaults(hw, profiles)

    # Get platform-aware text columns
    text_columns, remove_strings, remove_patterns, gpu_fields = ask_text_columns(
        platform, data_types, existing=existing, primary_key=state.get("primary_key")
    )
    settings["text_columns"] = text_columns
    settings["remove_strings"] = remove_strings
    settings["remove_patterns"] = remove_patterns
    settings["gpu_fields"] = gpu_fields

    # ---- Lingua ----
    if "lingua" in profiles:
        section_header("Language Detection (Lingua)")
        settings["lingua_workers"] = ask_int("Lingua workers (total Rayon threads)", existing.get("lingua_workers", defaults["lingua_workers"]), tag="cl_lingua_workers")
        settings["lingua_file_workers"] = ask_int("Lingua file workers (concurrent files)", existing.get("lingua_file_workers", defaults["lingua_file_workers"]), tag="cl_lingua_file_workers")
        settings["lingua_batch_size"] = ask_int("Lingua batch size (rows per batch)", existing.get("lingua_batch_size", defaults["lingua_batch_size"]), tag="cl_lingua_batch_size")
        settings["lingua_low_accuracy"] = ask_bool("Low accuracy mode (faster)?", existing.get("lingua_low_accuracy", defaults["lingua_low_accuracy"]), tag="cl_lingua_low_accuracy")

    # ---- GPU Classifiers ----
    if "ml" in profiles:
        section_header("GPU Classifiers")

        if gpus:
            gpu_indices = [g["index"] for g in gpus]
            gpu_labels = [f"{g['index']}: {g['name']} ({g['vram_mb']} MB)" for g in gpus]
            selected_gpus = ask_multi_select(
                "GPUs to use:",
                gpu_labels,
                gpu_labels,  # default: all
                tag="cl_gpu_select",
            )
            settings["gpu_ids"] = [gpu_indices[gpu_labels.index(g)] for g in selected_gpus]
        else:
            gpu_ids_str = ask("GPU IDs (comma-separated)", "0", tag="cl_gpu_ids")
            settings["gpu_ids"] = [int(x.strip()) for x in gpu_ids_str.split(",")]

        # Recompute file_workers default based on selected GPUs
        defaults["ml_file_workers"] = max(1, len(settings["gpu_ids"]))

        settings["ml_file_workers"] = ask_int("File workers", existing.get("ml_file_workers", defaults["ml_file_workers"]), tag="cl_gpu_file_workers")
        settings["ml_tokenize_workers"] = ask_int("Tokenize workers", existing.get("ml_tokenize_workers", defaults["ml_tokenize_workers"]), tag="cl_gpu_tokenize_workers")
        settings["ml_classifier_batch_size"] = ask_int("Classifier batch size", existing.get("ml_classifier_batch_size", defaults["ml_classifier_batch_size"]), tag="cl_gpu_batch_size")

        available_classifiers = defaults["ml_classifiers"]
        existing_classifiers = existing.get("ml_classifiers", [])
        existing_names = [c["name"] for c in existing_classifiers] if existing_classifiers else available_classifiers
        existing_scope = {c["name"]: c.get("data_types") for c in existing_classifiers} if existing_classifiers else {}

        selected_names = ask_multi_select(
            "Classifiers to run:",
            available_classifiers,
            existing_names,
            tag="cl_gpu_classifiers",
        )

        # Force per-classifier data_types choice (skip when only one data_type is configured).
        selected_entries = []
        for name in selected_names:
            if len(data_types) <= 1:
                selected_entries.append({"name": name, "data_types": None})
                continue
            prev = existing_scope.get(name)
            default_scope = prev if prev else data_types
            chosen = ask_multi_select(
                f"Data types for '{name}':",
                data_types,
                default_scope,
                tag=f"cl_gpu_classifier_data_types__{name}",
            )
            # If the user picks the full set, store as None (= "all") to keep the YAML terse.
            scope = None if set(chosen) == set(data_types) else chosen
            selected_entries.append({"name": name, "data_types": scope})

        settings["ml_classifiers"] = selected_entries

        settings["hf_token"] = ask_password("HuggingFace token (optional, press Enter to skip): ", tag="cl_hf_token")

    return settings


# ============================================================================
# Config generators
# ============================================================================

def generate_lingua_user_yaml(settings):
    """Generate config/lingua/user.yaml content."""
    config = {
        "pipeline": {
            "processing": {
                "data_types": settings["data_types"],
            }
        },
        "cpu_classifiers": {
            "text_columns": settings["text_columns"],
            "lingua": {
                "low_accuracy": settings["lingua_low_accuracy"],
                "workers": settings["lingua_workers"],
                "file_workers": settings["lingua_file_workers"],
                "batch_size": settings["lingua_batch_size"],
            }
        }
    }

    # Add remove_strings and remove_patterns for non-Reddit platforms
    if settings["platform"] != "reddit":
        if settings["remove_strings"]:
            config["cpu_classifiers"]["remove_strings"] = settings["remove_strings"]
        if settings["remove_patterns"]:
            config["cpu_classifiers"]["remove_patterns"] = settings["remove_patterns"]

    return yaml.dump(config, default_flow_style=False, sort_keys=False)


def generate_ml_user_yaml(settings):
    """Generate config/ml/user.yaml content."""
    gpu_classifiers_config = {
        "gpu_ids": settings["gpu_ids"],
        "file_workers": settings["ml_file_workers"],
        "tokenize_workers": settings["ml_tokenize_workers"],
        "classifier_batch_size": settings["ml_classifier_batch_size"],
        "text_columns": settings["text_columns"],
    }

    # Add platform-specific overrides for non-Reddit
    if settings["platform"] != "reddit":
        if settings["remove_strings"]:
            gpu_classifiers_config["remove_strings"] = settings["remove_strings"]
        if settings["remove_patterns"]:
            gpu_classifiers_config["remove_patterns"] = settings["remove_patterns"]
        gpu_classifiers_config["fields"] = settings["gpu_fields"]

    # Emit list of classifiers: bare string when running on all data_types,
    # dict {name, data_types} when scoped to a subset.
    classifier_entries = []
    for c in settings["ml_classifiers"]:
        if c.get("data_types") is None:
            classifier_entries.append(c["name"])
        else:
            classifier_entries.append({"name": c["name"], "data_types": c["data_types"]})

    config = {
        "pipeline": {
            "processing": {
                "data_types": settings["data_types"],
            },
            "gpu_classifiers": classifier_entries,
        },
        "gpu_classifiers": gpu_classifiers_config,
    }
    return yaml.dump(config, default_flow_style=False, sort_keys=False)


# ============================================================================
# Summary
# ============================================================================

def print_summary(settings, files_to_write):
    """Print a summary of classifier settings."""
    section_header("Classifier Configuration Summary")

    profiles = settings["profiles"]

    print(f"  Platform:    {settings['platform']}")
    print(f"  Data types:  {', '.join(settings['data_types'])}")
    print()

    print("  Text columns:")
    for dt, cols in settings.get("text_columns", {}).items():
        print(f"    {dt}: {', '.join(cols)}")
    print()

    if "lingua" in profiles:
        print("  Lingua:")
        print(f"    Workers:        {settings['lingua_workers']}")
        print(f"    File workers:   {settings['lingua_file_workers']}")
        print(f"    Batch size:     {settings['lingua_batch_size']:,}")
        print(f"    Low accuracy:   {settings['lingua_low_accuracy']}")
        print()

    if "ml" in profiles:
        print("  GPU Classifiers:")
        print(f"    GPU IDs:             {settings['gpu_ids']}")
        print(f"    File workers:        {settings['ml_file_workers']}")
        print(f"    Tokenize workers:    {settings['ml_tokenize_workers']}")
        print(f"    Classifier batch:    {settings['ml_classifier_batch_size']}")
        print("    Classifiers:")
        for c in settings["ml_classifiers"]:
            scope = "all" if c.get("data_types") is None else ", ".join(c["data_types"])
            print(f"      - {c['name']} (data_types: {scope})")
        print(f"    HF token:            {'set' if settings.get('hf_token') else 'not set'}")
        print()

    print("  Files to write:")
    for path, _ in files_to_write:
        rel = path.relative_to(ROOT)
        exists = path.exists()
        status = " (exists, will backup)" if exists else ""
        print(f"    {rel}{status}")
    if settings.get("hf_token"):
        print("    .env (update HF_TOKEN)")
    print()


# ============================================================================
# Main
# ============================================================================

def main(source_name):
    """Configure classifiers for a source.

    Args:
        source_name: Source name (required).
    """
    print()
    print("  Social Data Pipeline - Classifier Configuration")
    print("  ===============================================")
    print()
    print("  Configure language detection (Lingua) and GPU classifiers.")
    print("  Press Enter to accept defaults shown in [brackets].")
    print()

    state = require_source_state(source_name)
    profiles = state["profiles"]

    has_lingua = "lingua" in profiles
    has_ml = "ml" in profiles

    if not has_lingua and not has_ml:
        print("  No classifier profiles (lingua, ml) were selected during setup.")
        print("  Re-run source setup and select lingua and/or ml profiles.\n")
        sys.exit(0)

    # Detect hardware
    hw = detect_hardware()

    # Run questionnaire
    settings = run_questionnaire(hw, state)

    # Build file list
    files_to_write = []
    source_dir = CONFIG_DIR / "sources" / source_name

    if has_lingua:
        files_to_write.append((
            source_dir / "lingua.yaml",
            generate_lingua_user_yaml(settings),
        ))
    if has_ml:
        files_to_write.append((
            source_dir / "ml.yaml",
            generate_ml_user_yaml(settings),
        ))

    # Summary and confirm
    print_summary(settings, files_to_write)

    if not ask_bool("Write these files?", True, tag="cl_write_files"):
        print("\n  Aborted. No files written.\n")
        sys.exit(0)

    print()
    write_files(files_to_write)

    # Update .env with HF_TOKEN
    if settings.get("hf_token"):
        update_env_file({"HF_TOKEN": settings["hf_token"]})
        print("  Updated:   .env (HF_TOKEN)")

    print("\n  Done! Classifier configuration has been generated.")

    platform = state["platform"]
    if platform == "reddit":
        print("\n  Next step:")
        print(f"    python sdp.py source configure {source_name}  # Customize Reddit fields/indexes")
        print()
    else:
        print_pipeline_commands(profiles, source_name)
