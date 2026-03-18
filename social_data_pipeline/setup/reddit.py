"""Reddit platform configuration for Social Data Pipeline.

Configures Reddit-specific settings: database schema, field lists, and indexes.
Generates overrides in config/sources/reddit/platform.yaml.
"""

import sys

try:
    import yaml
except ImportError:
    print("Error: PyYAML is required. Install with: pip install pyyaml")
    sys.exit(1)

from social_data_pipeline.setup.utils import (
    ROOT, CONFIG_DIR,
    ask, ask_bool, ask_list, ask_multi_select,
    section_header, write_files, print_pipeline_commands,
    get_source_profiles,
)


# ============================================================================
# Reddit config loading
# ============================================================================

REDDIT_TEMPLATE = CONFIG_DIR / "templates" / "reddit.yaml"


def load_reddit_platform_config():
    """Load platform config from config/templates/reddit.yaml."""
    path = REDDIT_TEMPLATE
    try:
        data = yaml.safe_load(path.read_text())
        fields = data.get("fields", {})
        indexes = data.get("indexes", {})
        return {
            "db_schema": data.get("db_schema", "reddit"),
            "submission_fields": fields.get("submissions", []),
            "comment_fields": fields.get("comments", []),
            "submission_indexes": indexes.get("submissions", []),
            "comment_indexes": indexes.get("comments", []),
        }
    except (OSError, yaml.YAMLError) as e:
        print(f"  Error: Could not read {path}: {e}")
        sys.exit(1)


# ============================================================================
# Interactive questionnaire
# ============================================================================

def run_questionnaire():
    """Run the Reddit platform questionnaire. Returns settings dict."""
    settings = {}

    # Load current config from platform.yaml
    platform_config = load_reddit_platform_config()

    section_header("Reddit Platform Configuration")

    settings["db_schema"] = ask("Database schema name", platform_config["db_schema"])

    # Field list selection
    all_sub_fields = platform_config["submission_fields"]
    all_com_fields = platform_config["comment_fields"]

    print("  The default field list is defined in config/templates/reddit.yaml.")
    print("  You can remove fields here. Adding new fields also requires updating field_types.")
    customize_fields = ask_bool("Remove fields from the default list?", False)
    if customize_fields:
        print("\n  Submissions fields (deselect to exclude):")
        settings["reddit_sub_fields"] = ask_multi_select(
            "Submissions fields:", all_sub_fields, all_sub_fields,
        )
        print("\n  Comments fields (deselect to exclude):")
        settings["reddit_com_fields"] = ask_multi_select(
            "Comments fields:", all_com_fields, all_com_fields,
        )

    # Index selection
    default_sub_indexes = platform_config["submission_indexes"]
    default_com_indexes = platform_config["comment_indexes"]
    customize_indexes = ask_bool("Customize database indexes?", False)
    if customize_indexes:
        settings["reddit_sub_indexes"] = ask_list(
            "Submissions index columns", default_sub_indexes,
        )
        settings["reddit_com_indexes"] = ask_list(
            "Comments index columns", default_com_indexes,
        )

    return settings, platform_config


# ============================================================================
# Config generator
# ============================================================================

def generate_reddit_platform_user_yaml(settings, base_config):
    """Generate Reddit platform override YAML content.

    Overrides are deep-merged into config/sources/<name>/platform.yaml.
    """
    config = {}

    # Schema override
    if settings.get("db_schema") != base_config["db_schema"]:
        config["db_schema"] = settings["db_schema"]

    # Indexes (only if customized)
    indexes = {}
    if "reddit_sub_indexes" in settings:
        indexes["submissions"] = settings["reddit_sub_indexes"]
    if "reddit_com_indexes" in settings:
        indexes["comments"] = settings["reddit_com_indexes"]
    if indexes:
        config["indexes"] = indexes

    # Fields (only if customized)
    fields = {}
    if "reddit_sub_fields" in settings:
        fields["submissions"] = settings["reddit_sub_fields"]
    if "reddit_com_fields" in settings:
        fields["comments"] = settings["reddit_com_fields"]
    if fields:
        config["fields"] = fields

    if not config:
        return None  # No overrides needed
    return yaml.dump(config, default_flow_style=False, sort_keys=False)


# ============================================================================
# Summary
# ============================================================================

def print_summary(settings, files_to_write):
    """Print a summary of Reddit platform settings."""
    section_header("Reddit Configuration Summary")

    print(f"  Schema:              {settings.get('db_schema', 'reddit')}")
    has_custom_fields = "reddit_sub_fields" in settings or "reddit_com_fields" in settings
    has_custom_indexes = "reddit_sub_indexes" in settings or "reddit_com_indexes" in settings
    print(f"  Field lists:         {'customized' if has_custom_fields else 'default'}")
    print(f"  Indexes:             {'customized' if has_custom_indexes else 'default'}")
    print()

    if files_to_write:
        print("  Files to write:")
        for path, _ in files_to_write:
            rel = path.relative_to(ROOT)
            exists = path.exists()
            status = " (exists, will backup)" if exists else ""
            print(f"    {rel}{status}")
    else:
        print("  No overrides needed (all defaults).")
    print()


# ============================================================================
# Main
# ============================================================================

def apply_overrides_to_platform_yaml(source_name, override_yaml):
    """Apply Reddit overrides directly into config/sources/<name>/platform.yaml.

    Instead of writing a separate user.yaml, deep-merges overrides into the
    existing platform.yaml in the source directory.
    """
    source_platform_path = CONFIG_DIR / "sources" / source_name / "platform.yaml"
    if not source_platform_path.exists():
        return

    base = yaml.safe_load(source_platform_path.read_text()) or {}
    overrides = yaml.safe_load(override_yaml)

    # Simple deep merge
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            base[key].update(value)
        else:
            base[key] = value

    source_platform_path.write_text(yaml.dump(base, default_flow_style=False, sort_keys=False))


def main(source_name):
    """Configure Reddit-specific fields, indexes, and schema.

    Args:
        source_name: Source name (required, e.g. 'reddit').
    """
    print()
    print("  Social Data Pipeline - Reddit Platform Configuration")
    print("  ===================================================")
    print()
    print("  Configure Reddit-specific fields, indexes, and schema.")
    print("  Press Enter to accept defaults shown in [brackets].")
    print()

    settings, platform_config = run_questionnaire()

    reddit_yaml = generate_reddit_platform_user_yaml(settings, platform_config)

    if reddit_yaml is None:
        print("  No configuration changes needed. Using defaults.\n")
        profiles = get_source_profiles(source_name)
        print_pipeline_commands(profiles, source_name)
        return

    source_platform_path = CONFIG_DIR / "sources" / source_name / "platform.yaml"
    if not source_platform_path.exists():
        print(f"  Error: Source platform config not found: {source_platform_path}\n")
        sys.exit(1)

    print_summary(settings, [(source_platform_path, reddit_yaml)])
    if not ask_bool("Apply these overrides?", True):
        print("\n  Aborted.\n")
        sys.exit(0)

    print()
    apply_overrides_to_platform_yaml(source_name, reddit_yaml)
    print(f"  Updated:   config/sources/{source_name}/platform.yaml")

    print(f"\n  Done! Reddit platform configuration has been updated.")
    profiles = get_source_profiles(source_name)
    print_pipeline_commands(profiles, source_name)
