"""Shared utilities for setup modules.

Provides hardware detection, interactive input helpers, state management,
and file I/O used across all configuration modules.
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

ROOT = Path(__file__).resolve().parent.parent.parent
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


def ask_password(prompt):
    """Prompt for a password (hidden input)."""
    return getpass.getpass(f"  {prompt}").strip()


def section_header(title):
    """Print a section header."""
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}\n")


def require_source_state(source_name):
    """Load source state from config/sources/<name>/. Exits if not found.

    Args:
        source_name: Source name (required).

    Returns:
        Dict with platform, data_types, profiles, source keys.
    """
    source_config = load_source_config(source_name)
    if source_config is None:
        print(f"\n  Source '{source_name}' not found. Run 'python sdb.py source add {source_name}' first.\n")
        sys.exit(1)

    profiles = get_source_profiles(source_name)
    platform = source_config.get("platform")
    if not platform:
        platform = "reddit" if source_name == "reddit" else f"custom/{source_name}"

    return {
        "platform": platform,
        "data_types": source_config.get("data_types", []),
        "profiles": profiles,
        "source": source_name,
    }


# ============================================================================
# Source management
# ============================================================================

SOURCES_DIR = CONFIG_DIR / "sources"
DB_CONFIG_DIR = CONFIG_DIR / "db"


def list_sources():
    """List all configured sources (directories in config/sources/)."""
    if not SOURCES_DIR.exists():
        return []
    return sorted([
        d.name for d in SOURCES_DIR.iterdir()
        if d.is_dir() and (d / "platform.yaml").exists()
    ])


def load_source_config(source_name):
    """Load platform.yaml for a source. Returns dict or None."""
    source_path = SOURCES_DIR / source_name / "platform.yaml"
    if not source_path.exists():
        return None
    try:
        return yaml.safe_load(source_path.read_text())
    except (OSError, yaml.YAMLError) as e:
        print(f"  Warning: Could not read {source_path}: {e}")
        return None


def get_source_profiles(source_name):
    """Determine which profiles a source has configured.

    Checks which profile override files exist in the source directory.
    """
    source_dir = SOURCES_DIR / source_name
    if not source_dir.exists():
        return []

    # Map override filenames to profile names
    file_to_profile = {
        "parse.yaml": "parse",
        "ml_cpu.yaml": "ml_cpu",
        "ml.yaml": "ml",
        "postgres.yaml": "postgres_ingest",
        "postgres_ml.yaml": "postgres_ml",
        "mongo.yaml": "mongo_ingest",
    }

    profiles = []
    for filename, profile in file_to_profile.items():
        if (source_dir / filename).exists():
            profiles.append(profile)

    return profiles


def load_db_setup():
    """Load database setup info from config/db/.

    Returns dict with 'databases' and optional 'tablespaces', or None.
    """
    if not DB_CONFIG_DIR.exists():
        return None

    result = {"databases": []}

    postgres_path = DB_CONFIG_DIR / "postgres.yaml"
    if postgres_path.exists():
        try:
            pg_config = yaml.safe_load(postgres_path.read_text()) or {}
            result["databases"].append("postgres")
            if "tablespaces" in pg_config:
                result["tablespaces"] = pg_config["tablespaces"]
            if pg_config.get("auth"):
                result["postgres_auth"] = True
        except (OSError, yaml.YAMLError):
            pass

    mongo_path = DB_CONFIG_DIR / "mongo.yaml"
    if mongo_path.exists():
        try:
            mongo_config = yaml.safe_load(mongo_path.read_text()) or {}
            result["databases"].append("mongo")
            if mongo_config.get("auth"):
                result["mongo_auth"] = True
        except (OSError, yaml.YAMLError):
            pass

    if not result["databases"]:
        return None

    return result


def resolve_source(source_name=None):
    """Resolve a source name, auto-selecting if only one exists.

    Args:
        source_name: Explicit source name, or None to auto-select.

    Returns:
        Source name string.

    Raises:
        SystemExit if no sources or ambiguous.
    """
    if source_name:
        source_dir = SOURCES_DIR / source_name
        if not source_dir.exists() or not (source_dir / "platform.yaml").exists():
            print(f"\n  Error: Source '{source_name}' not found in config/sources/\n")
            sys.exit(1)
        return source_name

    sources = list_sources()
    if not sources:
        print("\n  No sources configured. Run 'python sdb.py source add <name>' first.\n")
        sys.exit(1)
    if len(sources) == 1:
        return sources[0]

    print(f"\n  Multiple sources configured: {', '.join(sources)}")
    print(f"  Please specify --source <name>\n")
    sys.exit(1)


# ============================================================================
# .env file helpers
# ============================================================================

def update_env_file(updates):
    """Read existing .env, merge key=value updates, write back.

    For each key in updates:
    - If the key exists (commented or not), replace that line
    - If the key doesn't exist, append it
    """
    env_path = ROOT / ".env"
    if not env_path.exists():
        # Create a minimal .env with just these values
        lines = [f"{k}={v}" for k, v in updates.items()]
        env_path.write_text("\n".join(lines) + "\n")
        return

    existing_lines = env_path.read_text().splitlines()
    new_lines = []
    keys_found = set()

    for line in existing_lines:
        matched = False
        for key, value in updates.items():
            # Match "KEY=...", "# KEY=...", or "# KEY=" patterns
            stripped = line.lstrip("# ").strip()
            if stripped.startswith(f"{key}=") or stripped == f"{key}":
                if key not in keys_found:
                    if value:
                        new_lines.append(f"{key}={value}")
                    else:
                        new_lines.append(f"# {key}=")
                    keys_found.add(key)
                matched = True
                break
        if not matched:
            new_lines.append(line)

    # Append any keys not found in existing file
    for key, value in updates.items():
        if key not in keys_found:
            if value:
                new_lines.append(f"{key}={value}")
            else:
                new_lines.append(f"# {key}=")

    env_path.write_text("\n".join(new_lines) + "\n")


# ============================================================================
# File I/O
# ============================================================================

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
# Pipeline commands
# ============================================================================

def print_pipeline_commands(profiles, source_name=None):
    """Print sdb.py commands to run the selected profiles in order."""
    profile_order = ["parse", "ml_cpu", "ml", "postgres_ingest", "postgres_ml", "mongo_ingest"]
    selected = [p for p in profile_order if p in profiles]

    # Database servers must run before their ingestion profiles
    db_profiles = {"postgres_ingest", "postgres_ml", "mongo_ingest"}
    needs_start = any(p in profiles for p in db_profiles)
    if needs_start:
        # Insert 'sdb start' before the first db ingestion profile
        for i, p in enumerate(selected):
            if p in db_profiles:
                selected.insert(i, "_start")
                break

    source_flag = f" --source {source_name}" if source_name else ""

    print("\n  To run your pipeline, execute these commands in order:\n")
    for p in selected:
        if p == "_start":
            print(f"    python sdb.py db start")
        else:
            print(f"    python sdb.py run {p}{source_flag}")
    print()


# ============================================================================
# File pattern helpers
# ============================================================================

def glob_to_regex(glob_pattern):
    """Convert a glob pattern to a regex pattern.

    Uses fnmatch.translate() and wraps the result in ^...$.
    Adds a capture group around the wildcard portion for file ID extraction.
    """
    import fnmatch
    regex = fnmatch.translate(glob_pattern)
    # fnmatch.translate wraps in (?s:...) with \\Z — normalize to ^...$
    # Replace .* (from *) with a capture group (.*)
    regex = regex.replace(r'(?s:', '').rstrip(r')\Z')
    regex = '^' + regex + '$'
    return regex


def detect_compression_from_glob(glob_pattern):
    """Auto-detect compression format from a glob pattern's extension.

    Returns:
        Compression type string ('zst', 'gz', 'xz', 'tar.gz') or None.
    """
    pattern = glob_pattern.lower()
    if pattern.endswith('.tar.gz') or pattern.endswith('.tgz'):
        return 'tar.gz'
    if pattern.endswith('.zst'):
        return 'zst'
    if pattern.endswith('.json.gz') or pattern.endswith('.gz'):
        return 'gz'
    if pattern.endswith('.xz'):
        return 'xz'
    return None


def derive_file_patterns(dump_glob, compression):
    """Derive dump regex, json regex, csv regex, and prefix from a dump glob.

    Args:
        dump_glob: Shell glob pattern for dump files (e.g., 'tweets_*.json.gz')
        compression: Detected compression format string

    Returns:
        Dict with keys: dump, json, csv, prefix, compression, dump_glob
    """
    import fnmatch

    # Convert glob to regex for dump matching
    dump_regex = fnmatch.translate(dump_glob)
    # fnmatch.translate produces (?s:...)\\Z — strip wrapper for a clean regex
    if dump_regex.startswith('(?s:') and dump_regex.endswith(')\\Z'):
        dump_regex = dump_regex[4:-3]
    dump_regex = '^' + dump_regex + '$'

    # Derive the stem pattern by stripping compression extension from the glob
    stem_glob = dump_glob
    lower = dump_glob.lower()
    if lower.endswith('.tar.gz'):
        stem_glob = dump_glob[:-7]
    elif lower.endswith('.tgz'):
        stem_glob = dump_glob[:-4]
    elif lower.endswith('.json.gz'):
        stem_glob = dump_glob[:-8]  # Remove .json.gz entirely
    elif lower.endswith('.zst'):
        stem_glob = dump_glob[:-4]
    elif lower.endswith('.gz'):
        stem_glob = dump_glob[:-3]
    elif lower.endswith('.xz'):
        stem_glob = dump_glob[:-3]

    # If the stem still has .json, strip it (extracted files have no extension)
    if stem_glob.lower().endswith('.json'):
        stem_glob = stem_glob[:-5]

    # Build json regex from stem
    json_regex = fnmatch.translate(stem_glob)
    if json_regex.startswith('(?s:') and json_regex.endswith(')\\Z'):
        json_regex = json_regex[4:-3]
    json_regex = '^' + json_regex + '$'

    # Build csv regex (stem + .csv)
    csv_glob = stem_glob + '.csv'
    csv_regex = fnmatch.translate(csv_glob)
    if csv_regex.startswith('(?s:') and csv_regex.endswith(')\\Z'):
        csv_regex = csv_regex[4:-3]
    csv_regex = '^' + csv_regex + '$'

    # Derive prefix (everything before the first wildcard)
    prefix = dump_glob.split('*')[0] if '*' in dump_glob else dump_glob

    return {
        'dump': dump_regex,
        'dump_glob': dump_glob,
        'json': json_regex,
        'csv': csv_regex,
        'prefix': prefix,
        'compression': compression,
    }
