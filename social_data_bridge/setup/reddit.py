"""Reddit platform configuration for Social Data Bridge.

Configures Reddit-specific settings: database schema, field lists, and indexes.
Generates config/platforms/reddit/user.yaml.
"""

import sys

try:
    import yaml
except ImportError:
    print("Error: PyYAML is required. Install with: pip install pyyaml")
    sys.exit(1)

from social_data_bridge.setup.utils import (
    ROOT, CONFIG_DIR,
    ask, ask_bool, ask_list, ask_multi_select,
    section_header, load_setup_state, write_files, print_pipeline_commands,
)


# ============================================================================
# Reddit config loading
# ============================================================================

REDDIT_CONFIG_DIR = CONFIG_DIR / "platforms" / "reddit"


def load_reddit_field_list():
    """Load field lists from config/platforms/reddit/field_list.yaml."""
    path = REDDIT_CONFIG_DIR / "field_list.yaml"
    try:
        data = yaml.safe_load(path.read_text())
        return data.get("submissions", []), data.get("comments", [])
    except (OSError, yaml.YAMLError) as e:
        print(f"  Error: Could not read {path}: {e}")
        sys.exit(1)


def load_reddit_platform_config():
    """Load platform config from config/platforms/reddit/platform.yaml."""
    path = REDDIT_CONFIG_DIR / "platform.yaml"
    try:
        data = yaml.safe_load(path.read_text())
        indexes = data.get("indexes", {})
        return {
            "db_schema": data.get("db_schema", "reddit"),
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

    # Load current config from files
    all_sub_fields, all_com_fields = load_reddit_field_list()
    platform_config = load_reddit_platform_config()

    section_header("Reddit Platform Configuration")

    settings["db_schema"] = ask("Database schema name", platform_config["db_schema"])

    # Field list selection
    print("  The default field list is defined in config/platforms/reddit/field_list.yaml.")
    print("  You can remove fields here. Adding new fields also requires field_types.yaml.")
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
    """Generate config/platforms/reddit/user.yaml content."""
    config = {}

    # Platform overrides (schema, indexes)
    platform = {}
    if settings.get("db_schema") != base_config["db_schema"]:
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

def main():
    print()
    print("  Social Data Bridge - Reddit Platform Configuration")
    print("  ===================================================")
    print()
    print("  Configure Reddit-specific fields, indexes, and schema.")
    print("  Press Enter to accept defaults shown in [brackets].")
    print()

    settings, platform_config = run_questionnaire()

    # Build file list
    files_to_write = []
    reddit_yaml = generate_reddit_platform_user_yaml(settings, platform_config)
    if reddit_yaml is not None:
        files_to_write.append((
            CONFIG_DIR / "platforms" / "reddit" / "user.yaml",
            reddit_yaml,
        ))

    # Summary and confirm
    print_summary(settings, files_to_write)

    if not files_to_write:
        print("  No configuration changes needed. Using defaults.\n")
        return

    if not ask_bool("Write these files?", True):
        print("\n  Aborted. No files written.\n")
        sys.exit(0)

    print()
    write_files(files_to_write)
    print(f"\n  Done! Reddit platform configuration has been generated.")

    # setup_reddit is always the last step for Reddit — print pipeline commands
    state = load_setup_state()
    if state:
        print_pipeline_commands(state.get("profiles", []))
    else:
        print()
