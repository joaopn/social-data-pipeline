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
STATE_FILE = CONFIG_DIR / "setup_state.yaml"


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


# ============================================================================
# State management
# ============================================================================

def load_setup_state():
    """Load setup state from config/setup_state.yaml. Returns dict or None."""
    if not STATE_FILE.exists():
        return None
    try:
        return yaml.safe_load(STATE_FILE.read_text())
    except (OSError, yaml.YAMLError) as e:
        print(f"  Warning: Could not read {STATE_FILE}: {e}")
        return None


def save_setup_state(state):
    """Write setup state to config/setup_state.yaml."""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(yaml.dump(state, default_flow_style=False, sort_keys=False))


def require_setup_state():
    """Load setup state, exit with message if missing."""
    state = load_setup_state()
    if state is None:
        print("\n  No setup state found. Run 'python sdb.py setup' first.\n")
        sys.exit(1)
    return state


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

def print_pipeline_commands(profiles):
    """Print sdb.py commands to run the selected profiles in order."""
    profile_order = ["parse", "ml_cpu", "ml", "postgres", "postgres_ingest", "postgres_ml"]
    selected = [p for p in profile_order if p in profiles]

    # postgres server must run alongside ingestion profiles
    needs_postgres = any(p in profiles for p in ("postgres_ingest", "postgres_ml"))
    if needs_postgres and "postgres" not in selected:
        for i, p in enumerate(selected):
            if p in ("postgres_ingest", "postgres_ml"):
                selected.insert(i, "postgres")
                break

    print("\n  To run your pipeline, execute these commands in order:\n")
    for p in selected:
        if p == "postgres":
            print(f"    python sdb.py start")
        else:
            print(f"    python sdb.py run {p}")
    print()
