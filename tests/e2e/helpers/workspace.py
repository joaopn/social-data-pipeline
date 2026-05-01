"""Workspace management for E2E tests.

Creates a fresh copy of the repo at /workspace for each test, so that
generated configs, .env, and data directories don't leak between tests.
"""

import shutil
import subprocess
from pathlib import Path

REPO = Path("/repo")
WORKSPACE = Path("/workspace")


def create_workspace():
    """Create /workspace from /repo, isolated from host runtime state.

    Uses git as the source of truth: copies only files that `git add .`
    would stage — i.e. tracked files plus untracked-but-not-ignored.
    This honors `.gitignore` exactly (including negations like
    `!tests/fixtures/**/*.csv`) and means new gitignore rules are
    respected automatically, without a parallel allowlist to maintain.

    Crucially, this excludes anything the developer's local `sdp`
    installation has generated: `config/db/*.yaml`, `config/sources/`,
    `config/jobs/config.local.yaml`, `*.local.conf`, `.env`,
    `docker-compose.override.yml`, `data/`, `*.bak` — all of which are
    in `.gitignore`. Without this, prior versions of the runner pulled
    in host state silently (e.g. host's `config/jobs/config.local.yaml`
    with `auth: true` making every `sdp db start` EOFError on pytest's
    stdin while waiting for a password).
    """
    cleanup_workspace()
    WORKSPACE.mkdir(exist_ok=True)

    # `git ls-files -co --exclude-standard` = tracked + untracked-not-ignored.
    # Run from REPO with safe.directory=* because /repo is bind-mounted from
    # the host as a different UID inside sysbox; without this, git aborts with
    # "detected dubious ownership". Path is read-only so this is purely
    # informational, not a security concern. Surface stderr on failure since
    # subprocess.run swallows it by default.
    file_list_proc = subprocess.run(
        ["git", "-c", "safe.directory=*", "ls-files", "-co", "--exclude-standard"],
        cwd=str(REPO),
        capture_output=True,
        text=True,
    )
    if file_list_proc.returncode != 0:
        raise RuntimeError(
            f"git ls-files failed (exit {file_list_proc.returncode}):\n"
            f"stdout:\n{file_list_proc.stdout}\n"
            f"stderr:\n{file_list_proc.stderr}"
        )
    paths_file = WORKSPACE.parent / "_e2e_workspace_files.txt"
    paths_file.write_text(file_list_proc.stdout)
    try:
        subprocess.run(
            [
                "rsync", "-a",
                "--files-from", str(paths_file),
                f"{REPO}/",
                f"{WORKSPACE}/",
            ],
            check=True,
            capture_output=True,
        )
    finally:
        paths_file.unlink(missing_ok=True)

    # Create empty runtime directories the pipeline expects (all gitignored
    # so they don't come over via the file list).
    for subdir in ["dumps", "extracted", "parsed", "output",
                   "database/postgres", "database/mongo"]:
        (WORKSPACE / "data" / subdir).mkdir(parents=True, exist_ok=True)
    (WORKSPACE / "config" / "db").mkdir(parents=True, exist_ok=True)
    (WORKSPACE / "config" / "sources").mkdir(parents=True, exist_ok=True)


def cleanup_workspace():
    """Remove /workspace contents."""
    if WORKSPACE.exists():
        shutil.rmtree(WORKSPACE, ignore_errors=True)


def teardown_compose():
    """Stop all docker compose services and remove volumes."""
    subprocess.run(
        ["docker", "compose", "down", "--volumes", "--remove-orphans", "--timeout", "10"],
        cwd=WORKSPACE,
        capture_output=True,
        timeout=120,
    )
