"""Tests for the action functions of the CLI (`do_*`): the state-changing commands, callable without a terminal.

Each `cmd_*` function of a state-changing command is a thin wrapper around a `do_*` function that never prompts and never exits. The contracts pinned here:

- No action, and no CLI helper an action reaches, asks a question or ends the process. The same holds for the `get_*` functions, which return the data of the read commands.
- A confirmation fails closed: without a confirm callback an action stops before it changes anything; the terminal wrapper still asks today's questions, with today's texts.
- The admin password comes from the caller or from the wrapper's terminal callback, at the point where the terminal asks today. Without either, an action that needs it stops before it runs anything; a password that is not needed never reaches the environment compose reads.
- `resolve_source` prints what it always printed and exits 1; `resolve_source_name` raises instead, so `do_run_start` can return 1.

Every test points `sdp` and `social_data_pipeline.setup.utils` at a temp project and replaces `sdp.docker_compose`, `subprocess.run`, `input` and `sdp.getpass`, so nothing touches the real tree, Docker or a terminal. Passwords are made-up values.
"""

from __future__ import annotations

import ast
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

import sdp
from social_data_pipeline.setup import utils

PASSWORD_VARS = ("POSTGRES_PASSWORD", "MONGO_ADMIN_PASSWORD", "STARROCKS_ROOT_PASSWORD")
# What `do_run_start` writes into the process environment for compose.
RUN_VARS = ("PLATFORM", "SOURCE", "DUMPS_PATH", "PARSED_PATH", "EXTRACTED_PATH", "OUTPUT_PATH", "FILE_FILTER", "SKIP_LINGUA_FILES", "CLASSIFIER")

# Today's prompt texts, written out: an external harness matches them.
RESET_PROMPT = "  Reset PostgreSQL data? This CANNOT be undone [y/N]: "
UNSETUP_PROMPT_1 = "  Delete PostgreSQL entirely? Config, containers, and data will be removed. This CANNOT be undone [y/N]: "
UNSETUP_PROMPT_2 = "  Are you SURE? All PostgreSQL data will be permanently lost [y/N]: "
REMOVE_PROMPT = "  Remove source 'ok' configuration? [y/N]: "
PASSWORD_PROMPT = "  [sdp_db_password] Database admin password: "

DB_ABORT = "\n  Aborted — nothing was changed.\n\n"
PASSWORD_REQUIRED = "the database admin password is required"


# ── helpers ─────────────────────────────────────────────────────────────────


class _Recorder:
    """Stands in for `sdp.docker_compose`, `subprocess.run` and the two prompt functions; keeps what was called, in order."""

    def __init__(self):
        self.events = []

    def compose(self, *args):
        self.events.append(("compose", args))
        return subprocess.CompletedProcess(args=args, returncode=0)

    def run(self, *args, **kwargs):
        argv = args[0] if args else ()
        self.events.append(("run", argv))
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    def kinds(self):
        return [kind for kind, _ in self.events]


def _must_not_prompt(*args, **kwargs):
    raise AssertionError("an action must not prompt")


def _seed(root, env_flags=None, jobs_auth=False):
    """Write a project with PostgreSQL, its MCP server, the jobs scheduler (one target on PostgreSQL) and the source `ok`."""
    config = root / "config"
    (config / "db").mkdir(parents=True)
    (config / "db" / "postgres.yaml").write_text("port: 5432\n")
    (config / "db" / "mcp.yaml").write_text("postgres:\n  enabled: true\n")
    (config / "jobs").mkdir()
    (config / "jobs" / "config.local.yaml").write_text(f"auth: {'true' if jobs_auth else 'false'}\ntargets:\n  pg:\n    backend: postgres\n")
    source = config / "sources" / "ok"
    source.mkdir(parents=True)
    (source / "platform.yaml").write_text("data_types: [posts]\n")
    (source / "parse.yaml").write_text("{}\n")
    (source / "postgres.yaml").write_text("{}\n")
    data = root / "data" / "database" / "postgres"
    (data / "pgdata").mkdir(parents=True)
    (data / "pgdata" / "PG_VERSION").write_text("18\n")
    (data / "state_tracking").mkdir()
    (data / "state_tracking" / "ok_postgres.json").write_text("{}\n")
    (data / ".ro_credentials").write_text("made-up-read-only-value\n")
    lines = ["PGDATA_PATH=./data/database/postgres", "POSTGRES_PORT=5432", "UNRELATED=1"]
    lines += [f"{key}={value}" for key, value in (env_flags or {}).items()]
    (root / ".env").write_text("\n".join(lines) + "\n")


def _point_at(monkeypatch, root):
    """Point the CLI and the setup helpers at the project in root."""
    config = root / "config"
    monkeypatch.setattr(sdp, "ROOT", root)
    monkeypatch.setattr(sdp, "CONFIG_DIR", config)
    monkeypatch.setattr(utils, "ROOT", root)
    monkeypatch.setattr(utils, "CONFIG_DIR", config)
    monkeypatch.setattr(utils, "SOURCES_DIR", config / "sources")


def _snapshot(root):
    """Every path under root, with file contents, to prove what was or was not touched."""
    return sorted(
        (str(p.relative_to(root)), p.read_text() if p.is_file() else None)
        for p in root.rglob("*")
    )


def _args(**kwargs):
    return SimpleNamespace(**kwargs)


def _password_vars():
    return {name: os.environ.get(name) for name in PASSWORD_VARS}


@pytest.fixture(autouse=True)
def _environ_is_restored():
    """Fail a test that leaves the process environment changed: the actions write passwords and run settings into it."""
    before = {key: value for key, value in os.environ.items() if key != "PYTEST_CURRENT_TEST"}
    yield
    assert {key: value for key, value in os.environ.items() if key != "PYTEST_CURRENT_TEST"} == before


@pytest.fixture
def stubs(monkeypatch):
    """Replace everything that would reach Docker or a terminal; returns the recorder. Also registers the variables an action writes, so monkeypatch removes them after the test."""
    recorder = _Recorder()
    monkeypatch.setattr(sdp, "docker_compose", recorder.compose)
    monkeypatch.setattr(sdp.subprocess, "run", recorder.run)
    monkeypatch.setattr("builtins.input", _must_not_prompt)
    monkeypatch.setattr(sdp, "getpass", _must_not_prompt)
    for name in PASSWORD_VARS + RUN_VARS + ("SDP_TAGGED_MODE",):
        # setenv first: delenv alone records nothing for a variable that is not set, and what the action writes would then outlive the test.
        monkeypatch.setenv(name, "")
        monkeypatch.delenv(name)
    return recorder


@pytest.fixture
def project(tmp_path, monkeypatch, stubs):
    """A seeded temp project (no authentication) the CLI points at. `.root` is its folder, `.rec` the recorder."""
    root = tmp_path.resolve() / "project"
    _seed(root)
    _point_at(monkeypatch, root)
    return SimpleNamespace(root=root, rec=stubs)


# ── no prompt, no exit ──────────────────────────────────────────────────────


# Names an action, or a helper it reaches, must not use: each one asks on a terminal or ends the process. `resolve_source` does both.
# The walk does not follow functions imported from the package, so the ones that prompt or exit are named here: the setup entry points under the local names the CLI imports them by, and the setup helper require_source_state, which the CLI does not import today and is listed as a guard.
FORBIDDEN_NAMES = {
    "input", "getpass", "_prompt_db_password", "resolve_source", "require_source_state", "exit", "_exit", "quit", "SystemExit",
    "db_main", "add_database", "mcp_main", "jobs_main", "source_main", "reddit_main", "classifiers_main",
}

ACTION_NAMES = [
    "do_chat_auto_title", "do_chat_prompt", "do_chat_query_limit", "do_db_create_indexes", "do_db_reset", "do_db_setup", "do_db_setup_jobs", "do_db_setup_mcp", "do_db_start", "do_db_stop", "do_db_unsetup", "do_db_unsetup_jobs", "do_db_unsetup_mcp", "do_run_cancel", "do_run_start", "do_source_add", "do_source_add_classifiers", "do_source_configure", "do_source_download", "do_source_remove",
    "get_chat_settings", "get_db_index_targets", "get_db_llm_settings", "get_db_restart_needed", "get_db_settings", "get_db_status", "get_db_verify", "get_run_list", "get_run_logs", "get_source_error_logs", "get_source_hf_inspect", "get_source_list", "get_source_progress", "get_source_settings", "get_source_size", "get_source_status",
]

# One helper each walk must have reached, so a walk that follows nothing cannot pass.
KNOWN_DEEP_HELPER = {
    # The chat's host module does the work (`chat_host.apply_query_limit`, `query_setting`), which prompts nothing and never exits.
    "do_chat_query_limit": "_chat_host",
    "do_chat_auto_title": "_chat_host",
    "do_chat_prompt": "_chat_host",
    "get_chat_settings": "_chat_host",
    "do_db_create_indexes": "_sr_execute_index_plan",
    "do_db_reset": "_chown_db_data_for_host",
    "do_db_setup": "_get_configured_db_services",
    "do_db_start": "_resolve_server_data_mounts",
    "do_db_stop": "docker_compose",
    "do_db_unsetup": "_unsetup_single_db",
    "do_db_unsetup_jobs": "docker_compose",
    "do_db_unsetup_mcp": "_get_configured_mcp_services",
    "do_run_cancel": "_run_service_profiles",
    "do_run_start": "_container_mounts",
    "do_source_remove": "_running_services",
    "do_source_add": "_warn_mount_drift_after_source_change",
    "do_source_configure": "_warn_mount_drift_after_source_change",
    # The reconfigure's classifier part and the read live in `setup.source_change`, which prompts nothing and never exits (tests/setup/test_source_change.py runs them).
    "do_source_add_classifiers": "do_source_add_classifiers",
    "get_source_settings": "get_source_settings",
    "get_db_index_targets": "_mongo_index_collections",
    # Calls only functions imported from the package, as get_source_list does.
    "get_db_settings": "get_db_settings",
    "get_db_status": "_load_classifier_suffixes",
    "get_db_verify": "_probe_container_state",
    "get_run_list": "_run_display_safe",
    "get_run_logs": "_require_run_id",
    "get_source_error_logs": "_get_mongoimport_log_sections",
    # Calls only functions imported from the package, so the walk reaches nothing beyond the getter itself.
    "get_source_list": "get_source_list",
    # The Hugging Face steps live in `setup.hf_checked`, which prompts nothing and never exits (tests/setup/test_hf_checked.py runs them).
    "do_source_download": "do_source_download",
    "get_source_hf_inspect": "get_source_hf_inspect",
    "get_source_progress": "_count_stage_files",
    "get_source_size": "_measure_stage_folder",
    "get_source_status": "_get_source_ingestion_state",
    "do_db_setup_mcp": "_sdp_service_ports",
    "do_db_setup_jobs": "_running_started_at",
    "get_db_restart_needed": "_running_started_at",
    "get_db_llm_settings": "_jobs_defaults",
}


def _cli_functions():
    """Name to syntax tree of every function defined at module level in the CLI."""
    tree = ast.parse(Path(sdp.__file__).read_text())
    return {node.name: node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}


def _reachable(start, functions):
    """The functions reached from start: every module-level function a reached function names, called or passed on."""
    seen, todo = set(), [start]
    while todo:
        name = todo.pop()
        if name in seen:
            continue
        seen.add(name)
        for node in ast.walk(functions[name]):
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load) and node.id in functions:
                todo.append(node.id)
    return seen


def _is_forbidden(name):
    # The private callback parameter `_ask_password` starts with an underscore and is not one of the setup module's `ask*` prompt helpers.
    return name in FORBIDDEN_NAMES or name.startswith("ask")


def _forbidden_uses(names, functions):
    """Where the given functions use a forbidden name: as a plain name (`input(...)`, `raise SystemExit`) or as the last part of a dotted one (`sys.exit(...)`, `getpass.getpass(...)`)."""
    found = []
    for name in sorted(names):
        for node in ast.walk(functions[name]):
            used = node.id if isinstance(node, ast.Name) else node.attr if isinstance(node, ast.Attribute) else None
            if used is not None and _is_forbidden(used):
                found.append(f"{name} (line {node.lineno}) uses {used}")
    return found


class TestActionsNeverPromptOrExit:
    def test_the_walk_starts_at_every_action(self):
        """Bug class: an action or a getter added to the CLI that the walk below never visits."""
        assert sorted(name for name in _cli_functions() if name.startswith(("do_", "get_"))) == ACTION_NAMES

    @pytest.mark.parametrize("action", ACTION_NAMES)
    def test_no_action_reaches_a_prompt_or_an_exit(self, action):
        """Bug class: an action that, on some path, waits for a terminal the calling process does not have (`input`, `getpass`, a setup `ask*` helper) or ends that process (`sys.exit`), directly or through a function of the CLI module it calls (functions imported from the package are not followed; the ones the CLI imports that prompt or exit are in FORBIDDEN_NAMES). Checked on the syntax tree because most such paths need a failing container or a rare config to run."""
        functions = _cli_functions()
        reached = _reachable(action, functions)

        assert KNOWN_DEEP_HELPER[action] in reached
        assert _forbidden_uses(reached, functions) == []

    def test_the_walk_sees_the_terminal_helpers(self):
        """Bug class: a walk that passes because it cannot recognise a prompt or an exit. The terminal's own helpers must be reported, by their plain and their dotted names."""
        functions = _cli_functions()

        assert any("uses getpass" in use for use in _forbidden_uses({"_prompt_db_password"}, functions))
        assert any("uses exit" in use for use in _forbidden_uses({"_prompt_db_password"}, functions))
        assert any("uses input" in use for use in _forbidden_uses({"_confirm_on_terminal"}, functions))
        assert any("uses resolve_source" in use for use in _forbidden_uses({"cmd_db_create_indexes"}, functions))
        assert "_confirm_on_terminal" in _reachable("cmd_db_reset", functions)


# ── confirmation ────────────────────────────────────────────────────────────


# Per action: its inputs, its terminal command, the questions the command asks (in order) and what it prints when the answer is no.
CONFIRMED = {
    "do_db_reset": {"kwargs": {"db": "postgres"}, "wrapper": "cmd_db_reset", "prompts": [RESET_PROMPT], "abort": DB_ABORT},
    "do_db_unsetup": {"kwargs": {"db": "postgres"}, "wrapper": "cmd_db_unsetup", "prompts": [UNSETUP_PROMPT_1, UNSETUP_PROMPT_2], "abort": DB_ABORT},
    "do_source_remove": {"kwargs": {"name": "ok"}, "wrapper": "cmd_source_remove", "prompts": [REMOVE_PROMPT], "abort": "  Aborted.\n\n"},
}

# Every way to say no: at the first question, and for a command with two questions also at the second.
REFUSALS = [
    pytest.param(action, ["y"] * yes_first + ["n"], id=f"{action}-no-at-question-{yes_first + 1}")
    for action, case in sorted(CONFIRMED.items())
    for yes_first in range(len(case["prompts"]))
]


# Each question of each confirming action: (action, how many earlier questions are answered yes).
ODD_REPLY_AT = [
    pytest.param(action, yes_first, id=f"{action}-question-{yes_first + 1}")
    for action, case in sorted(CONFIRMED.items())
    for yes_first in range(len(case["prompts"]))
]


@pytest.fixture
def no_running_services(monkeypatch):
    """`db reset` and `source remove` look at the running containers (a read-only `docker compose ps`) before or after the question, in the terminal too. Stubbed so every recorded subprocess call is an effect."""
    monkeypatch.setattr(sdp, "_running_services", lambda: set())


@pytest.mark.usefixtures("no_running_services")
class TestConfirmationFailsClosed:
    @pytest.mark.parametrize("action", sorted(CONFIRMED))
    def test_without_a_confirm_callback_nothing_is_changed(self, project, capsys, action):
        """Bug class: a destructive action whose confirmation defaults to yes, so a caller that forgets the callback wipes a database or a source nobody confirmed. Without a callback the action must stop at the question, before any container is stopped or any file removed."""
        before = _snapshot(project.root)

        with pytest.raises(sdp.ActionError) as exc:
            getattr(sdp, action)(**CONFIRMED[action]["kwargs"])

        assert (exc.value.message, exc.value.exit_code) == ("confirmation required", 1)
        assert project.rec.events == []
        assert _snapshot(project.root) == before

    @pytest.mark.parametrize("action, answers", REFUSALS)
    def test_a_callback_that_says_no_changes_nothing(self, project, capsys, action, answers):
        """Bug class: an action that ignores what its confirm callback returns, or that asks only the first of two questions. A no at any question leaves everything in place and returns the abort code the command has today (0)."""
        case = CONFIRMED[action]
        before = _snapshot(project.root)
        asked = []
        replies = [answer == "y" for answer in answers]

        rc = getattr(sdp, action)(**case["kwargs"], _confirm=lambda prompt: (asked.append(prompt), replies.pop(0))[1])

        assert rc == 0
        assert asked == case["prompts"][:len(answers)]
        assert capsys.readouterr().out.endswith(case["abort"])
        assert project.rec.events == []
        assert _snapshot(project.root) == before

    @pytest.mark.parametrize("action, yes_first", ODD_REPLY_AT)
    @pytest.mark.parametrize("reply", ["y", "n", 1, None])
    def test_only_exactly_true_confirms(self, project, capsys, action, yes_first, reply):
        """Bug class: a confirm callback of the wrong shape (one that returns the typed answer, a count or nothing) being read as a yes because it is truthy, so a destructive action proceeds on "n". Tried at each question in turn, the earlier ones answered with a real True, so the second question of a two-question command is covered too."""
        case = CONFIRMED[action]
        before = _snapshot(project.root)
        asked = []
        replies = [True] * yes_first + [reply]

        rc = getattr(sdp, action)(**case["kwargs"], _confirm=lambda prompt: (asked.append(prompt), replies.pop(0))[1])

        assert rc == 0
        assert asked == case["prompts"][:yes_first + 1]
        assert project.rec.events == []
        assert _snapshot(project.root) == before

    @pytest.mark.parametrize("action", sorted(CONFIRMED))
    def test_already_confirmed_acts_like_yes_in_the_terminal(self, tmp_path, monkeypatch, stubs, capsys, action):
        """Bug class: the action and its terminal command drifting apart: a step that runs only behind the terminal's yes, or only without it. Run once with `already_confirmed` and once through the command answered "y", on two identical projects: exit code, files left, containers and subprocesses called and text printed must be the same, and something must have been removed."""
        case = CONFIRMED[action]
        outcomes = []
        for label in ("action", "terminal"):
            root = tmp_path.resolve() / label
            _seed(root)
            _point_at(monkeypatch, root)
            before = _snapshot(root)
            del stubs.events[:]
            if label == "action":
                rc = getattr(sdp, action)(**case["kwargs"], _confirm=sdp.already_confirmed)
            else:
                monkeypatch.setattr("builtins.input", lambda prompt="": "y")
                rc = getattr(sdp, case["wrapper"])(_args(**case["kwargs"]))
            after = _snapshot(root)
            assert after != before
            outcomes.append((rc, after, repr(stubs.events).replace(str(root), "<root>"), capsys.readouterr().out.replace(str(root), "<root>")))

        assert outcomes[0] == outcomes[1]
        assert outcomes[0][0] == 0


@pytest.mark.usefixtures("no_running_services")
class TestTerminalStillAsks:
    @pytest.mark.parametrize("action, answers", REFUSALS)
    def test_command_answered_no_changes_nothing(self, project, monkeypatch, capsys, action, answers):
        """Bug class: a refactored command that no longer asks (its callback dropped, or replaced by one that says yes), asks with another text than the one an external harness matches, or acts before the last question. Answering no at any question leaves the folder and the containers alone and returns 0, as today."""
        case = CONFIRMED[action]
        before = _snapshot(project.root)
        asked = []
        replies = list(answers)
        monkeypatch.setattr("builtins.input", lambda prompt="": (asked.append(prompt), replies.pop(0))[1])

        rc = getattr(sdp, case["wrapper"])(_args(**case["kwargs"]))

        assert rc == 0
        assert asked == case["prompts"][:len(answers)]
        assert capsys.readouterr().out.endswith(case["abort"])
        assert project.rec.events == []
        assert _snapshot(project.root) == before

    @pytest.mark.parametrize("answer, proceeds", [("y", True), ("yes", True), (" YES ", True), ("Y", True), ("", False), ("n", False), ("yep", False), ("ja", False)])
    def test_only_y_and_yes_count_as_yes(self, monkeypatch, answer, proceeds):
        """Bug class: the shared terminal callback accepting more than today's answers (an empty answer, any text starting with y), which would turn Enter into a yes on a destructive question."""
        monkeypatch.setattr("builtins.input", lambda prompt="": answer)

        assert sdp._confirm_on_terminal("  Question [y/N]: ") is proceeds

    @pytest.mark.parametrize("wrapper, action, args", [
        ("cmd_run", "do_run_start", {"profile": "parse", "source": "ok", "build": False, "filter": None}),
        ("cmd_db_start", "do_db_start", {"service": None}),
        ("cmd_db_reset", "do_db_reset", {"db": "postgres"}),
        ("cmd_db_unsetup", "do_db_unsetup", {"db": "postgres"}),
        ("cmd_source_remove", "do_source_remove", {"name": "ok"}),
    ])
    def test_command_reports_an_action_error_and_returns_its_code(self, monkeypatch, capsys, wrapper, action, args):
        """Bug class: a command that lets an ActionError escape as a traceback, or flattens its exit code to 1."""
        def refuse(**kwargs):
            raise sdp.ActionError("made-up reason", exit_code=3)

        monkeypatch.setattr(sdp, action, refuse)

        assert getattr(sdp, wrapper)(_args(**args)) == 3
        assert capsys.readouterr().out == "\n  Error: made-up reason\n\n"


# ── admin password ──────────────────────────────────────────────────────────


def _start(service):
    return SimpleNamespace(action=lambda **kw: sdp.do_db_start(service=service, **kw), command=lambda: sdp.cmd_db_start(_args(service=service)))


def _run(profile):
    command_args = {"profile": profile, "source": "ok", "build": False, "filter": None, "skip_lingua_files": False, "classifier": None}
    return SimpleNamespace(action=lambda **kw: sdp.do_run_start(profile=profile, source="ok", **kw), command=lambda: sdp.cmd_run(_args(**command_args)))


POSTGRES_AUTH = {"POSTGRES_AUTH_ENABLED": "true"}
OTHER_DB_AUTH = {"MONGO_AUTH_ENABLED": "true"}

# id, call, .env flags, jobs UI auth, what the terminal runs before it asks for the password (None: this path never asks).
# The four paths decide differently: `<db>-mcp` asks when any database has authentication on, `jobs` and a database also when only the jobs UI has, `run` only when the profile's own database has.
PASSWORD_CASES = [
    ("start-mcp/db-auth", _start("postgres-mcp"), POSTGRES_AUTH, False, ["compose"]),
    ("start-jobs/db-auth", _start("jobs"), POSTGRES_AUTH, False, []),
    ("start-db/db-auth", _start("postgres"), POSTGRES_AUTH, False, []),
    ("start-all/db-auth", _start(None), POSTGRES_AUTH, False, []),
    ("run-ingest/db-auth", _run("postgres_ingest"), POSTGRES_AUTH, False, ["run"]),
    ("run-parse/db-auth", _run("parse"), POSTGRES_AUTH, False, None),
    ("start-mcp/other-db-auth", _start("postgres-mcp"), OTHER_DB_AUTH, False, ["compose"]),
    ("run-ingest/other-db-auth", _run("postgres_ingest"), OTHER_DB_AUTH, False, None),
    ("start-mcp/jobs-auth", _start("postgres-mcp"), {}, True, None),
    ("start-jobs/jobs-auth", _start("jobs"), {}, True, []),
    ("start-db/jobs-auth", _start("postgres"), {}, True, []),
    ("run-ingest/jobs-auth", _run("postgres_ingest"), {}, True, None),
    ("start-mcp/no-auth", _start("postgres-mcp"), {}, False, None),
    ("start-jobs/no-auth", _start("jobs"), {}, False, None),
    ("start-db/no-auth", _start("postgres"), {}, False, None),
    ("run-ingest/no-auth", _run("postgres_ingest"), {}, False, None),
]
PASSWORD_PARAMS = [pytest.param(*case[1:], id=case[0]) for case in PASSWORD_CASES]


@pytest.fixture
def deployment(tmp_path, monkeypatch, stubs):
    """Build a temp project with the given authentication settings; returns (root, recorder, the passwords `_set_auth_env` was called with)."""
    def build(env_flags, jobs_auth):
        root = tmp_path.resolve() / "project"
        _seed(root, env_flags, jobs_auth)
        _point_at(monkeypatch, root)
        handed_over = []
        real = sdp._set_auth_env
        monkeypatch.setattr(sdp, "_set_auth_env", lambda password: (handed_over.append(password), real(password))[1])
        return root, stubs, handed_over

    return build


class TestAdminPassword:
    @pytest.mark.parametrize("missing", [{}, {"admin_password": ""}], ids=["absent", "empty"])
    @pytest.mark.parametrize("call, env_flags, jobs_auth, before_prompt", PASSWORD_PARAMS)
    def test_without_a_password_and_a_terminal(self, deployment, capsys, call, env_flags, jobs_auth, before_prompt, missing):
        """Bug class: an action that starts work and then cannot ask (it probed the containers, created folders, or hung on a prompt before finding out), or one that demands a password a path does not need and so breaks every deployment without authentication. Where the terminal would ask, the action stops before any compose call, subprocess or folder; elsewhere it runs. An empty password counts as none: the terminal refuses one too."""
        root, rec, handed_over = deployment(env_flags, jobs_auth)
        before = _snapshot(root)

        if before_prompt is None:
            assert call.action(**missing) == 0
            assert "compose" in rec.kinds()
        else:
            with pytest.raises(sdp.ActionError) as exc:
                call.action(**missing)
            assert (exc.value.message, exc.value.exit_code) == (PASSWORD_REQUIRED, 1)
            assert rec.events == []
            assert _snapshot(root) == before
        assert handed_over == []
        assert _password_vars() == dict.fromkeys(PASSWORD_VARS)

    @pytest.mark.parametrize("call, env_flags, jobs_auth, before_prompt", PASSWORD_PARAMS)
    def test_with_a_password_passed_in(self, deployment, capsys, call, env_flags, jobs_auth, before_prompt):
        """Bug class: a password passed by the caller that is not used (the action prompts anyway, which the stubs turn into a failure) or is used where it is not needed: with authentication off it would change what compose injects into the containers, so it must never reach the environment."""
        root, rec, handed_over = deployment(env_flags, jobs_auth)

        assert call.action(admin_password="made-up-pw") == 0

        assert "compose" in rec.kinds()
        if before_prompt is None:
            assert handed_over == []
            assert _password_vars() == dict.fromkeys(PASSWORD_VARS)
            assert "made-up-pw" not in os.environ.values()
        else:
            assert handed_over == ["made-up-pw"]
            assert _password_vars() == dict.fromkeys(PASSWORD_VARS, "made-up-pw")

    @pytest.mark.parametrize("call", [_start("postgres"), _start("jobs"), _start("postgres-mcp"), _run("postgres_ingest")])
    @pytest.mark.parametrize("auth", [True, False])
    def test_a_password_that_is_not_a_plain_string_is_refused(self, deployment, call, auth):
        """Bug class: a wrapped secret handed over without unwrapping, which would be silently ignored where no password is needed and reach os.environ as a non-string where one is. Refused before anything runs, whether or not the path needs a password."""
        class Wrapped:
            pass

        root, rec, handed_over = deployment(POSTGRES_AUTH if auth else {}, False)

        with pytest.raises(TypeError, match="plain string"):
            call.action(admin_password=Wrapped())

        assert rec.events == []
        assert handed_over == []

    def test_a_passed_password_wins_over_the_callback(self, deployment, capsys):
        """Bug class: a caller that passes a password and still gets the callback run, which on a terminal would be a second, unexpected prompt."""
        root, rec, handed_over = deployment(POSTGRES_AUTH, False)

        assert sdp.do_db_start(service="jobs", admin_password="made-up-pw", _ask_password=_must_not_prompt) == 0
        assert handed_over == ["made-up-pw"]

    @pytest.mark.parametrize("call, env_flags, jobs_auth, before_prompt", PASSWORD_PARAMS)
    def test_the_terminal_asks_at_the_same_point(self, deployment, monkeypatch, capsys, call, env_flags, jobs_auth, before_prompt):
        """Bug class: a refactored command that asks for the password earlier or later than before (the container probe on the MCP path and the mount probe of `run` come first today), asks where it did not, stops asking, or loses the prompt tag an external harness matches."""
        root, rec, handed_over = deployment(env_flags, jobs_auth)
        monkeypatch.setenv("SDP_TAGGED_MODE", "1")
        monkeypatch.setattr(sdp, "getpass", lambda prompt="": (rec.events.append(("prompt", prompt)), "made-up-typed")[1])

        assert call.command() == 0

        prompts = [detail for kind, detail in rec.events if kind == "prompt"]
        if before_prompt is None:
            assert prompts == []
            assert handed_over == []
        else:
            assert prompts == [PASSWORD_PROMPT]
            assert rec.kinds()[:rec.kinds().index("prompt")] == before_prompt
            assert handed_over == ["made-up-typed"]
            assert _password_vars() == dict.fromkeys(PASSWORD_VARS, "made-up-typed")


# ── source resolution ───────────────────────────────────────────────────────


NOT_FOUND = "\n  Error: Source '{name}' not found in config/sources/\n"
NO_SOURCES = "\n  No sources configured. Run 'python sdp.py source add <name>' first.\n"
SEVERAL_SOURCES = "\n  Multiple sources configured: ok, second\n  Please specify --source <name>\n"

# id, extra source folders with a platform.yaml, remove `ok` first, the name asked for, the text shown.
UNRESOLVED = [
    pytest.param([], False, "nosuch", NOT_FOUND.format(name="nosuch"), id="named-missing"),
    pytest.param([], False, "bare", NOT_FOUND.format(name="bare"), id="named-without-platform-yaml"),
    pytest.param([], True, None, NO_SOURCES, id="none-configured"),
    pytest.param(["second"], False, None, SEVERAL_SOURCES, id="several-configured"),
]


@pytest.fixture
def sources(project):
    """The project's config/sources/ arranged for one case: `bare` has no platform.yaml, config/x/ is what `../x` addresses."""
    def arrange(extra, without_ok):
        sources_dir = project.root / "config" / "sources"
        (sources_dir / "bare").mkdir()
        (project.root / "config" / "x").mkdir()
        (project.root / "config" / "x" / "platform.yaml").write_text("data_types: [posts]\n")
        for name in extra:
            (sources_dir / name).mkdir()
            (sources_dir / name / "platform.yaml").write_text("data_types: [posts]\n")
        if without_ok:
            (sources_dir / "ok" / "platform.yaml").unlink()

    return arrange


class TestResolveSource:
    @pytest.mark.parametrize("extra, without_ok, name, text", UNRESOLVED)
    def test_resolve_source_prints_the_same_text_and_exits_1(self, sources, capsys, extra, without_ok, name, text):
        """Bug class: the terminal's output or exit code changing when the lookup moved into a function that raises: a lost blank line, the two lines of the several-sources message joined wrongly, or an exit code other than 1."""
        sources(extra, without_ok)

        with pytest.raises(SystemExit) as exc:
            utils.resolve_source(name)

        assert exc.value.code == 1
        captured = capsys.readouterr()
        assert captured.out == text + "\n"
        assert captured.err == ""

    @pytest.mark.parametrize("extra, without_ok, name, text", UNRESOLVED)
    def test_resolve_source_name_raises_and_prints_nothing(self, sources, capsys, extra, without_ok, name, text):
        """Bug class: the lookup an action uses still ending the process or printing on its own, which would kill the calling process or print the message twice."""
        sources(extra, without_ok)

        with pytest.raises(ValueError) as exc:
            utils.resolve_source_name(name)

        assert str(exc.value) == text
        assert capsys.readouterr().out == ""

    @pytest.mark.parametrize("name", ["ok", None])
    def test_both_return_the_name_of_a_configured_source(self, sources, capsys, name):
        """Bug class: a lookup that refuses the legitimate cases: a named source that exists, and no name when exactly one source is configured."""
        sources([], False)

        assert utils.resolve_source_name(name) == "ok"
        assert utils.resolve_source(name) == "ok"
        assert capsys.readouterr().out == ""

    def test_a_name_is_joined_to_the_sources_folder_unchecked(self, sources, capsys):
        """Bug class: the lookup gaining a rule for names when it moved into a function that raises, so a name the terminal accepts today is refused. The name is joined to config/sources/ as given: one with `..` whose target holds a platform.yaml resolves, in both forms. A caller that must not accept such a name checks it before calling."""
        sources([], False)

        assert utils.resolve_source_name("../x") == "../x"
        assert utils.resolve_source("../x") == "../x"
        assert capsys.readouterr().out == ""

    @pytest.mark.parametrize("extra, without_ok, name, text", UNRESOLVED)
    def test_run_prints_the_text_and_returns_1(self, sources, project, capsys, extra, without_ok, name, text):
        """Bug class: `run` for a source that cannot be resolved ending the process (the old lookup called `sys.exit`) or surfacing as an ActionError with another text, instead of printing today's message and returning 1, from the action and from the command alike; and anything being started for it."""
        sources(extra, without_ok)

        assert sdp.do_run_start(profile="parse", source=name) == 1
        assert capsys.readouterr().out == text + "\n"

        assert sdp.cmd_run(_args(profile="parse", source=name, build=False, filter=None)) == 1
        assert capsys.readouterr().out == text + "\n"
        assert project.rec.events == []


# ── the recorded run a call belongs to ──────────────────────────────────────


RUN_ID = "r20261121T101500-0a1b2c"

# The compose call of `run` as it was before the action learned about recorded runs, written out for several flag combinations.
BASE_COMPOSE_CALLS = [
    ({"profile": "parse"}, ("--profile", "parse", "run", "--rm", "parse")),
    ({"profile": "parse", "build": True}, ("--profile", "parse", "run", "--rm", "--build", "parse")),
    ({"profile": "parse", "filter": "RS_2024-*", "skip_lingua_files": True}, ("--profile", "parse", "run", "--rm", "parse")),
    ({"profile": "postgres_ingest"}, ("--profile", "postgres_ingest", "run", "--rm", "postgres-ingest")),
    ({"profile": "postgres_ingest", "build": True, "filter": "RS_2024-*"}, ("--profile", "postgres_ingest", "run", "--rm", "--build", "postgres-ingest")),
]


def _compose_calls(recorder):
    return [args for kind, args in recorder.events if kind == "compose"]


class TestRunId:
    @pytest.mark.parametrize("flags, expected", BASE_COMPOSE_CALLS)
    def test_without_a_run_id_the_compose_call_is_what_it_was(self, project, flags, expected):
        """Bug class: the terminal's `run` changed by the parameter that exists for recorded runs: a container name or a label on a run nobody records (it would then be listed as a recorded run without a record, and a second `sdp run` of the same profile would be refused by the daemon over the name). Without a run id, with `None` passed, and through the command, the compose arguments are the literal lists of before."""
        assert sdp.do_run_start(source="ok", **flags) == 0
        assert sdp.do_run_start(source="ok", _run_id=None, **flags) == 0
        command_args = {"build": False, "filter": None, "skip_lingua_files": False, "classifier": None, **flags}
        assert sdp.cmd_run(_args(source="ok", **command_args)) == 0

        assert _compose_calls(project.rec) == [expected, expected, expected]

    @pytest.mark.parametrize("flags, expected", [
        ({"profile": "parse"}, ("--profile", "parse", "run", "--rm", "--name", "sdp-run-" + RUN_ID, "-l", "sdp.run=" + RUN_ID, "parse")),
        ({"profile": "postgres_ingest", "build": True, "filter": "RS_2024-*"}, ("--profile", "postgres_ingest", "run", "--rm", "--build", "--name", "sdp-run-" + RUN_ID, "-l", "sdp.run=" + RUN_ID, "postgres-ingest")),
    ])
    def test_with_a_run_id_the_container_gets_its_name_and_label(self, project, flags, expected):
        """Bug class: a recorded run whose container the run store cannot find (no label, another label name, the name without the label), so the run cannot be cancelled and reads as lost while it runs; or the two options placed after the service name, where compose takes them as the command to run in the container."""
        assert sdp.do_run_start(source="ok", _run_id=RUN_ID, **flags) == 0

        assert _compose_calls(project.rec) == [expected]

    @pytest.mark.parametrize("run_id", [
        "", "live-0a1b2c3d4e5f", RUN_ID + "\n", RUN_ID + " --privileged", "../" + RUN_ID, RUN_ID.upper(), "r20261121T101500-0a1b2", "--volume=/:/host", 20261121, ["r20261121T101500-0a1b2c"], True,
    ], ids=repr)
    def test_a_malformed_run_id_is_refused_before_anything_happens(self, project, monkeypatch, capsys, run_id):
        """Bug class: a run id that is not the id of a recorded run reaching the compose command line as a container name or a label (an option of `docker compose run` in the worst case), or being refused only after the action has read the configuration, probed the running containers, created the data folders or written the run's settings into the environment. The functions that would do those things record their calls, and the lists must be empty."""
        reached = []
        for name in ("load_env", "_validate_run_mounts", "_set_auth_env"):
            monkeypatch.setattr(sdp, name, lambda *args, _name=name, **kwargs: reached.append(_name))
        before = _snapshot(project.root)

        with pytest.raises(sdp.ActionError) as exc:
            sdp.do_run_start(profile="postgres_ingest", source="ok", admin_password="made-up-password", _run_id=run_id)

        assert exc.value.message == "the run id is not a valid run id" and exc.value.exit_code == 1
        assert reached == [] and project.rec.events == []
        assert _snapshot(project.root) == before
        assert {name: os.environ.get(name) for name in RUN_VARS + PASSWORD_VARS} == dict.fromkeys(RUN_VARS + PASSWORD_VARS)
        assert capsys.readouterr().out == ""
