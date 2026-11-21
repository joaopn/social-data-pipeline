"""Tests for `control.environ`, the short list of environment variables the broker keeps, and for where the broker's core modules sit among the others.

The contracts pinned here:

- Only the listed names and the `LC_` names survive, with their values; nothing is added.
- `reduce_process_environment` removes the rest from this process's own environment and returns names, never values.
- Loading any of the broker's core modules loads neither the CLI nor the run store, and loading the CLI loads none of them.

The process environment is a stand-in for the length of each test.
"""

from __future__ import annotations

import ast
import os
import subprocess
import sys
from pathlib import Path

import pytest

from social_data_pipeline.control import environ

REPO_ROOT = Path(__file__).resolve().parents[2]
MARKER = "made-up-token-value-e07c"

KEPT = {
    "PATH": "/usr/local/bin:/usr/bin", "HOME": "/home/operator", "USER": "operator", "LOGNAME": "operator", "LANG": "en_US.UTF-8", "LANGUAGE": "en", "TMPDIR": "/var/tmp", "XDG_RUNTIME_DIR": "/run/user/1000",
    "DOCKER_HOST": "unix:///run/user/1000/docker.sock", "DOCKER_CONTEXT": "rootless", "DOCKER_CONFIG": "/home/operator/.docker", "DOCKER_TLS_VERIFY": "1", "DOCKER_CERT_PATH": "/home/operator/.docker/certs", "SSH_AUTH_SOCK": "/run/user/1000/ssh-agent.sock",
    "LC_ALL": "C.UTF-8", "LC_CTYPE": "en_US.UTF-8", "LC_MESSAGES": "C", "LC_": "", "LC_ANYTHING_AT_ALL": "x", "TZ": "UTC",
    "COMPOSE_PROJECT_NAME": "deployment", "COMPOSE_FILE": "docker-compose.yml", "HTTP_PROXY": "http://proxy:3128", "HTTPS_PROXY": "http://proxy:3128", "NO_PROXY": "localhost",
    "http_proxy": "http://proxy:3128", "https_proxy": "http://proxy:3128", "no_proxy": "localhost", "HF_TOKEN": MARKER, "HF_HOME": "/home/operator/.cache/huggingface",
}
REMOVED = {
    "POSTGRES_PASSWORD": MARKER, "MONGO_ADMIN_PASSWORD": MARKER, "AWS_SECRET_ACCESS_KEY": MARKER, "GITHUB_TOKEN": MARKER, "LD_PRELOAD": "/tmp/evil.so", "LD_LIBRARY_PATH": "/tmp", "PYTHONPATH": "/tmp", "PYTHONSTARTUP": "/tmp/x.py",
    "ALL_PROXY": "socks5://proxy", "all_proxy": "socks5://proxy", "Http_Proxy": "x", "compose_file": "/tmp/other.yml", "COMPOSE_PROFILES": "all", "COMPOSE_ENV_FILES": "/tmp/.env", "HF_ENDPOINT": "http://elsewhere", "HF_HUB_OFFLINE": "1", "tz": "x",
    "DOCKER_HOST_EXTRA": "x", "DOCKER_BUILDKIT": "0", "BASH_ENV": "/tmp/x.sh", "IFS": " ", "SHELL": "/bin/bash", "TERM": "xterm",
    "path": "lower-case is another variable", "Path": "x", "PATH_EXTRA": "x", "XPATH": "x", "LC": "no underscore", "lc_all": "C", "XLC_ALL": "C", "LANG_": "x", "SOURCE": "reddit", "": "an empty name",
}


class TestClean:
    def test_only_the_allowed_names_survive_with_their_values(self):
        """Bug class: a broker that hands its children the shell it was started from: tokens and database passwords of that shell reach every container command, and a preloaded library, a Python path or a compose file variable changes what the CLI runs."""
        given = {**REMOVED, **KEPT}
        before = dict(given)

        cleaned = environ.clean(given)

        assert cleaned == KEPT
        # The one secret that is kept is the token the CLI needs; none of the removed ones is left.
        assert [name for name, value in cleaned.items() if value == MARKER] == ["HF_TOKEN"]
        # A new dict, and the one that was passed in is as it was.
        assert cleaned is not given and given == before

    def test_nothing_is_added(self):
        """Bug class: defaults put in for variables that were not set (a PATH, a HOME), which changes what the CLI finds compared with the terminal."""
        assert environ.clean({}) == {}
        assert environ.clean({"HOME": "/home/operator", "OTHER": "x"}) == {"HOME": "/home/operator"}

    def test_the_list_is_the_documented_one(self):
        """Bug class: a name added to the list in passing. Every name on it is something every child of the broker inherits."""
        assert environ.ALLOWED == (
            "PATH", "HOME", "USER", "LOGNAME", "LANG", "LANGUAGE", "TZ", "TMPDIR", "XDG_RUNTIME_DIR",
            "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH", "SSH_AUTH_SOCK",
            "COMPOSE_PROJECT_NAME", "COMPOSE_FILE",
            "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy",
            "HF_TOKEN", "HF_HOME",
        )
        assert environ.SECRET_NAMES == ("HF_TOKEN",) and set(environ.SECRET_NAMES) <= set(environ.ALLOWED)
        assert environ.ALLOWED_PREFIX == "LC_"
        assert [name for name in list(KEPT) + list(REMOVED) if environ.is_allowed(name)] == list(KEPT)
        assert not environ.is_allowed(None) and not environ.is_allowed(b"PATH")


class TestKnownSecrets:
    def test_the_secret_values_of_the_kept_variables_are_listed_for_redaction(self):
        """Bug class: a token the broker hands on to every child (the CLI reads it) and never overwrites in what the children print, so that a log of a download shows it in clear."""
        assert environ.known_secrets({**REMOVED, **KEPT}) == [MARKER]
        assert environ.known_secrets(KEPT) == [MARKER]

    @pytest.mark.parametrize("given", [{}, {"HF_TOKEN": ""}, {"HF_HOME": MARKER, "PATH": MARKER}, {"POSTGRES_PASSWORD": MARKER}])
    def test_nothing_else_is_listed(self, given):
        """Bug class: every value of the environment taken for a secret (paths and the user name would then be starred out of every log), or an empty value, which would match everywhere."""
        assert environ.known_secrets(given) == []


class TestReduce:
    def test_the_rest_is_removed_from_the_process_and_only_names_come_back(self, monkeypatch):
        """Bug class: the environment cleaned in a copy while the process keeps the original (every child started without an explicit environment still inherits it), or the removed values returned and logged."""
        stand_in = {name: value for name, value in {**REMOVED, **KEPT}.items() if name}
        monkeypatch.setattr(os, "environ", stand_in)

        removed = environ.reduce_process_environment()

        assert os.environ is stand_in and stand_in == KEPT
        assert removed == sorted(name for name in REMOVED if name)
        assert MARKER not in repr(removed) and all(isinstance(name, str) for name in removed)
        assert environ.reduce_process_environment() == []

    def test_a_child_of_a_reduced_process_sees_only_what_was_kept(self):
        """Bug class: the names removed from Python's view of the environment and not from the process itself, which is what a child inherits. Run in a process of its own, whose real environment is reduced."""
        code = (
            "import os, subprocess, sys\n"
            "from social_data_pipeline.control import environ\n"
            "removed = environ.reduce_process_environment()\n"
            "child = subprocess.run([sys.executable, '-c', 'import os; print(sorted(os.environ))'], capture_output=True, text=True)\n"
            "print(removed); print(child.stdout.strip())\n"
        )
        given = {**{name: value for name, value in REMOVED.items() if name}, **KEPT, "PATH": os.environ.get("PATH", ""), "PYTHONPATH": str(REPO_ROOT), "PYTHONDONTWRITEBYTECODE": "1"}

        result = subprocess.run([sys.executable, "-c", code], cwd=REPO_ROOT, env=given, capture_output=True, text=True)

        assert result.returncode == 0, result.stderr
        removed, seen_by_child = result.stdout.strip().split("\n")
        assert removed == repr(sorted([*(name for name in REMOVED if name), "PYTHONDONTWRITEBYTECODE"]))
        # The interpreter of the child may set a locale variable of its own; nothing that was removed is back.
        assert set(ast.literal_eval(seen_by_child)) - set(KEPT) == set()
        assert MARKER not in result.stdout


CORE_MODULES = ("framing", "passwords", "sessions", "audit", "redact", "environ", "files")


class TestImports:
    @pytest.mark.parametrize("module", CORE_MODULES)
    def test_a_core_module_loads_neither_the_cli_nor_the_run_store(self, module):
        """Bug class: the broker's core pulling in the CLI script (an import cycle once the CLI's commands start the broker, and the whole CLI in a process that should hold a socket and little else) or the run store and, through anything else, modules that only exist inside the containers; and a module that changes the process's environment as a side effect of being loaded. Run in a fresh interpreter, since this process has loaded them all."""
        forbidden = ("sdp", "social_data_pipeline.runstore", "social_data_pipeline.control.child", "social_data_pipeline.control.supervise", "social_data_pipeline.control.runs", "social_data_pipeline.db", "social_data_pipeline.jobs", "social_data_pipeline.orchestrators", "social_data_pipeline.classifiers", "social_data_pipeline.core", "social_data_pipeline.platforms")
        code = (
            f"import sys; before = dict(__import__('os').environ); import social_data_pipeline.control.{module}; "
            f"bad = sorted(m for m in sys.modules if any(m == f or m.startswith(f + '.') for f in {forbidden!r})); "
            "print(bad); sys.exit(1 if bad or dict(__import__('os').environ) != before else 0)"
        )

        # With an environment of its own: this process has loaded the module already, and what loading it changed would be inherited as "before".
        result = subprocess.run([sys.executable, "-c", code], cwd=REPO_ROOT, env={"PATH": os.environ.get("PATH", ""), "PYTHONDONTWRITEBYTECODE": "1"}, capture_output=True, text=True)

        assert result.returncode == 0, result.stdout + result.stderr

    def test_the_cli_loads_none_of_the_core_modules(self):
        """Bug class: the CLI growing a dependency on the broker's modules, so that a problem in them breaks every `sdp` command in the terminal."""
        forbidden = tuple(f"social_data_pipeline.control.{module}" for module in CORE_MODULES)
        code = (
            "import sys; import sdp; "
            f"bad = sorted(m for m in sys.modules if m in {forbidden!r}); "
            "print(bad); sys.exit(1 if bad else 0)"
        )

        result = subprocess.run([sys.executable, "-c", code], cwd=REPO_ROOT, capture_output=True, text=True)

        assert result.returncode == 0, result.stdout + result.stderr

    @pytest.mark.parametrize("module", ["framing", "redact", "environ", "files"])
    def test_the_modules_that_need_nothing_load_nothing_outside_the_standard_library(self, module):
        """Bug class: the frame reader or the redaction, which run on every request, starting to depend on PyYAML or on the setup helpers through a convenient import."""
        code = (
            f"import sys; import social_data_pipeline.control.{module}; "
            "bad = sorted(m for m in sys.modules if m == 'yaml' or m.startswith('social_data_pipeline.setup') or m in ('social_data_pipeline.control.names', 'social_data_pipeline.control.types', 'social_data_pipeline.control.verbs')); "
            "print(bad); sys.exit(1 if bad else 0)"
        )

        result = subprocess.run([sys.executable, "-c", code], cwd=REPO_ROOT, capture_output=True, text=True)

        assert result.returncode == 0, result.stdout + result.stderr
