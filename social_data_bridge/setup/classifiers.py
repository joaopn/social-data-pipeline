"""Classifier configuration for Social Data Bridge.

Configures language detection (Lingua) and GPU transformer classifiers.
Generates config/lingua/user.yaml, config/ml/user.yaml, and updates .env.
"""

import sys

try:
    import yaml
except ImportError:
    print("Error: PyYAML is required. Install with: pip install pyyaml")
    sys.exit(1)

from social_data_bridge.setup.utils import (
    ROOT, CONFIG_DIR,
    detect_hardware,
    ask, ask_int, ask_bool, ask_list, ask_multi_select, ask_password,
    section_header, require_source_state, update_env_file, write_files,
    print_pipeline_commands,
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
    ram = hw["ram_gb"] or 8
    gpus = hw["gpus"]
    min_vram = min((g["vram_mb"] for g in gpus), default=0)

    d = {}

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

    return d


# ============================================================================
# Platform-aware text_columns
# ============================================================================

def ask_text_columns(platform, data_types):
    """Get text_columns configuration based on platform.

    For Reddit, uses known defaults. For other platforms, asks the user.
    Returns (text_columns, remove_strings, remove_patterns, fields) tuple.
    """
    if platform == "reddit":
        # Filter to only the data_types the user selected
        text_columns = {dt: REDDIT_TEXT_COLUMNS[dt] for dt in data_types if dt in REDDIT_TEXT_COLUMNS}
        return text_columns, REDDIT_REMOVE_STRINGS, REDDIT_REMOVE_PATTERNS, REDDIT_GPU_FIELDS

    # Non-Reddit: ask the user
    section_header("Text Column Configuration")
    print("  Classifiers need to know which columns contain text to process.")
    print("  For each data type, specify the column names that contain text.")
    print()

    text_columns = {}
    for dt in data_types:
        cols = ask_list(f"Text columns for '{dt}' (comma-separated)")
        if cols:
            text_columns[dt] = cols
        else:
            print(f"    Warning: No text columns for '{dt}'. Classifiers may skip this type.")

    print()
    remove_strings = ask_list(
        "Exact strings to remove before classification (comma-separated, or Enter for none)",
    )
    remove_patterns_str = ask(
        "Regex patterns to remove (comma-separated)",
        r"https?://\S+",
    )
    remove_patterns = [p.strip() for p in remove_patterns_str.split(",") if p.strip()]

    fields = ask_list(
        "Extra fields to keep in GPU classifier output (besides id, dataset, retrieved_utc)",
    )

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
        print(f"  GPUs:      none detected")
    print()

    # Compute defaults
    defaults = compute_classifier_defaults(hw, profiles)

    # Get platform-aware text columns
    text_columns, remove_strings, remove_patterns, gpu_fields = ask_text_columns(platform, data_types)
    settings["text_columns"] = text_columns
    settings["remove_strings"] = remove_strings
    settings["remove_patterns"] = remove_patterns
    settings["gpu_fields"] = gpu_fields

    # ---- Lingua ----
    if "lingua" in profiles:
        section_header("Language Detection (Lingua)")
        settings["lingua_workers"] = ask_int("Lingua workers (total Rayon threads)", defaults["lingua_workers"])
        settings["lingua_file_workers"] = ask_int("Lingua file workers (concurrent files)", defaults["lingua_file_workers"])
        settings["lingua_batch_size"] = ask_int("Lingua batch size (rows per batch)", defaults["lingua_batch_size"])
        settings["lingua_low_accuracy"] = ask_bool("Low accuracy mode (faster)?", defaults["lingua_low_accuracy"])

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

        settings["hf_token"] = ask_password("HuggingFace token (optional, press Enter to skip): ")

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

    config = {
        "pipeline": {
            "processing": {
                "data_types": settings["data_types"],
            },
            "gpu_classifiers": settings["ml_classifiers"],
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

    print(f"  Text columns:")
    for dt, cols in settings.get("text_columns", {}).items():
        print(f"    {dt}: {', '.join(cols)}")
    print()

    if "lingua" in profiles:
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

    print("  Files to write:")
    for path, _ in files_to_write:
        rel = path.relative_to(ROOT)
        exists = path.exists()
        status = " (exists, will backup)" if exists else ""
        print(f"    {rel}{status}")
    if settings.get("hf_token"):
        print(f"    .env (update HF_TOKEN)")
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
    print("  Social Data Bridge - Classifier Configuration")
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

    if not ask_bool("Write these files?", True):
        print("\n  Aborted. No files written.\n")
        sys.exit(0)

    print()
    write_files(files_to_write)

    # Update .env with HF_TOKEN
    if settings.get("hf_token"):
        update_env_file({"HF_TOKEN": settings["hf_token"]})
        print(f"  Updated:   .env (HF_TOKEN)")

    print(f"\n  Done! Classifier configuration has been generated.")

    platform = state["platform"]
    if platform == "reddit":
        print("\n  Next step:")
        print(f"    python sdb.py source configure {source_name}  # Customize Reddit fields/indexes")
        print()
    else:
        print_pipeline_commands(profiles, source_name)
