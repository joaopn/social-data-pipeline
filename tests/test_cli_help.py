"""Smoke test: every sdp.py subcommand's --help exits 0.

Bug class: argparse regressions and import errors. Cheap to add, runs in
sub-second total. Catches:
  - subcommand removed but still wired in build_parser
  - new module imported at top level that fails to import
  - --help arg dropped from a parser
  - subparser nesting broken (e.g. `sdp.py db` no longer routes)

Doesn't run docker, doesn't touch state.
"""

import subprocess
import sys
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parent.parent
SDP_PY = REPO / "sdp.py"


SUBCOMMANDS = [
    # Top-level
    ["--help"],
    # db group
    ["db", "--help"],
    ["db", "setup", "--help"],
    ["db", "setup-mcp", "--help"],
    ["db", "setup-jobs", "--help"],
    ["db", "start", "--help"],
    ["db", "stop", "--help"],
    ["db", "status", "--help"],
    ["db", "unsetup", "--help"],
    ["db", "reset", "--help"],
    ["db", "unsetup-mcp", "--help"],
    ["db", "unsetup-jobs", "--help"],
    ["db", "recover-password", "--help"],
    ["db", "create-indexes", "--help"],
    ["db", "restart-needed", "--help"],
    ["db", "llm-settings", "--help"],
    # source group
    ["source", "--help"],
    ["source", "add", "--help"],
    ["source", "download", "--help"],
    ["source", "configure", "--help"],
    ["source", "add-classifiers", "--help"],
    ["source", "remove", "--help"],
    ["source", "list", "--help"],
    ["source", "status", "--help"],
    ["source", "progress", "--help"],
    ["source", "error-logs", "--help"],
    # run
    ["run", "--help"],
    # runs group
    ["runs", "--help"],
    ["runs", "list", "--help"],
    ["runs", "logs", "--help"],
    ["runs", "cancel", "--help"],
    # broker group
    ["broker", "--help"],
    ["broker", "start", "--help"],
    ["broker", "stop", "--help"],
    ["broker", "status", "--help"],
    ["broker", "passwd", "--help"],
    ["broker", "audit", "--help"],
    # webui group
    ["webui", "--help"],
    ["webui", "setup", "--help"],
    ["webui", "start", "--help"],
    ["webui", "stop", "--help"],
    ["webui", "status", "--help"],
    ["webui", "unsetup", "--help"],
]


@pytest.mark.parametrize("argv", SUBCOMMANDS, ids=lambda a: " ".join(a[:-1]) or "(top)")
def test_subcommand_help(argv):
    """`python sdp.py <subcmd> --help` exits 0 and prints something."""
    result = subprocess.run(
        [sys.executable, str(SDP_PY), *argv],
        capture_output=True,
        text=True,
        cwd=str(REPO),
        timeout=15,
    )
    assert result.returncode == 0, (
        f"`sdp.py {' '.join(argv)}` failed (rc={result.returncode}):\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )
    # Help output should be non-trivial — argparse always emits at least
    # `usage:` for any --help invocation.
    assert "usage" in result.stdout.lower(), (
        f"`sdp.py {' '.join(argv)}` rc=0 but no usage line in stdout:\n{result.stdout}"
    )
