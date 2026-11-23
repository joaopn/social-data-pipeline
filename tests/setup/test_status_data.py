"""Tests for the functions that return the data of the read commands (`get_db_status`, `get_db_verify`, `get_source_list`, `get_source_status`, `get_source_error_logs`, and for what all getters share also `get_source_progress`) and the printers that turn that data into the terminal text.

A caller without a terminal gets the data as JSON; the terminal prints the same data. The contracts pinned here, on the scenarios and golden files of `test_status_golden.py`:

- A getter prints nothing and returns data that survives a JSON round trip unchanged.
- Printing that data, after the round trip and with the deployment out of reach, gives the golden text: the data carries every fact, warning and position the text needs, and a printer reads nothing itself.
- No secret reaches the data or the text: not a value from `.env`, not the content of a credentials file, not a container's environment, not a key of a config file the text does not show, not the part of an error or a log the text leaves out.
- A getter refuses a source name that is not one path component (the empty string included: only None means all sources), a database and a profile it does not know, with an error that holds no input value, before it reads anything. The terminal commands do not check the name: they call the unchecked function behind the getter with the name as typed, so the terminal takes every name it always took.
- Without `verbose`, `get_db_status` reads no per-source ingestion detail.
- A state file that holds NaN or an infinity still gives JSON data, and the text it always gave.
- An ingestion entry carries the profile and the data type its state file's name says, apart from the composed text the terminal shows.

Nothing touches the real tree, Docker or a terminal; the made-up secret is a marker string.
"""

from __future__ import annotations

import argparse
import ast
import json
import subprocess
from pathlib import Path

import pytest

import sdp
import test_status_golden as golden
from test_create_indexes import FakeDatabases
from social_data_pipeline.control import verbs
from social_data_pipeline.setup import utils

SCENARIOS_BY_NAME = {scenario.name: scenario for scenario in golden.SCENARIOS}

GETTER_NAMES = ["get_db_index_targets", "get_db_llm_settings", "get_db_restart_needed", "get_db_settings", "get_db_status", "get_db_verify", "get_run_list", "get_run_logs", "get_source_error_logs", "get_source_list", "get_source_progress", "get_source_status"]

NAME_NOT_VALID = "the source name is not a valid name"


# ── helpers ─────────────────────────────────────────────────────────────────


def refused_by_the_getter(scenario):
    """The scenarios whose source name is not one path component, the empty string among them: the terminal command takes the name as typed, the getter raises."""
    name = scenario.args.get("name")
    return scenario.command in ("source status", "source error-logs") and name is not None and (name == "" or "/" in name)


def getter_call(scenario):
    """The getter, its keyword arguments and the printer that stand for the scenario's command line. For a name the getter refuses, the unchecked function behind it stands in, which is what the terminal command calls."""
    args = scenario.args
    if scenario.command == "db status":
        return sdp.get_db_status, {"verbose": args["verbose"]}, sdp.print_db_status
    if scenario.command == "db verify":
        return sdp.get_db_verify, {"db": args["db"]}, sdp.print_db_verify_json if args["json"] else sdp.print_db_verify
    if scenario.command == "source list":
        return sdp.get_source_list, {}, sdp.print_source_list
    if scenario.command == "source status":
        if refused_by_the_getter(scenario):
            return sdp._get_source_status, {"source_name": args["name"]}, sdp.print_source_status
        return sdp.get_source_status, {"name": args["name"]}, sdp.print_source_status
    if refused_by_the_getter(scenario):
        return sdp._get_source_error_logs, {"source_name": args["name"], "profile_filter": args["profile"]}, sdp.print_source_error_logs
    return sdp.get_source_error_logs, {"name": args["name"], "profile": args["profile"]}, sdp.print_source_error_logs


def take_deployment_away(monkeypatch, tmp_path):
    """Point the CLI at a folder that does not exist and refuse every subprocess, so a printer that reads anything but its data fails or prints another text."""
    def run(argv, **kwargs):
        raise AssertionError(f"a printer must not run a subprocess: {argv}")

    golden.point_at(monkeypatch, tmp_path / "gone")
    monkeypatch.setattr(sdp.subprocess, "run", run)
    monkeypatch.setattr(sdp, "_running_services", lambda: run(["_running_services"]))
    monkeypatch.setattr(sdp, "_probe_container_state", lambda service: run(["_probe_container_state"]))


DATA_SCENARIOS = [pytest.param(scenario, id=scenario.name) for scenario in golden.SCENARIOS]


# ── data and text ───────────────────────────────────────────────────────────


@pytest.mark.parametrize("scenario", DATA_SCENARIOS)
def test_getter_returns_json_data_that_prints_the_golden_text(scenario, tmp_path, monkeypatch, capsys):
    """Bug class: a getter that still prints (the text would reach a caller's output, or appear twice in the terminal), returns something JSON cannot carry unchanged (a Path, a set, a tuple, a date, a non-string key), or leaves out a fact, a warning or its position so that the text can no longer be rebuilt from the data; and a printer that reads the deployment again instead of the data, or formats a line differently from the terminal's text. The text is printed from the data after a JSON round trip, with the deployment out of reach, and compared with the golden of the command."""
    root = golden.deploy(scenario, tmp_path, monkeypatch)
    getter, kwargs, printer = getter_call(scenario)

    data = getter(**kwargs)

    captured = capsys.readouterr()
    assert (captured.out, captured.err) == ("", "")
    carried = json.loads(json.dumps(data, allow_nan=False))
    assert carried == data
    if scenario.command == "db verify":
        assert data["report"]["exit_code"] == scenario.exit_code

    take_deployment_away(monkeypatch, tmp_path)
    printer(carried)

    captured = capsys.readouterr()
    assert captured.err == ""
    assert golden.normalise(captured.out, root) == golden.golden_path(scenario.name).read_bytes().decode("utf-8")


# Where the warning about an unreadable platform.yaml sits in the data, per scenario: (path into the data, number of warnings there). Every other warning list of the scenario must be empty.
WARNING_POSITIONS = {
    "db_status_unreadable_platform": [(("drift", "warnings"), 1)],
    "db_status_unreadable_platform_verbose": [(("databases", 0, "ingestion", "warnings"), 1), (("databases", 1, "ingestion", "warnings"), 1), (("drift", "warnings"), 1)],
    "db_verify_unreadable_platform": [(("warnings",), 1)],
    "db_verify_unreadable_platform_json": [(("warnings",), 1)],
    "source_status_all": [(("sources", 2, "warnings"), 1)],
    "source_status_unreadable_platform": [(("sources", 0, "warnings"), 1)],
    "error_logs_all": [(("sources", 0, "warnings"), 1)],
    "error_logs_unreadable_platform": [(("sources", 0, "warnings"), 1)],
}


def _warning_lists(value, path=()):
    """Every list stored under a key named `warnings` anywhere in the data, as (path, list)."""
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "warnings":
                yield path + (key,), child
            else:
                yield from _warning_lists(child, path + (key,))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from _warning_lists(child, path + (index,))


@pytest.mark.parametrize("name", sorted(WARNING_POSITIONS))
def test_warnings_sit_where_the_terminal_prints_them(name, tmp_path, monkeypatch, capsys):
    """Bug class: the warning about an unreadable platform.yaml collected once per command instead of once per place that loads the file, or attached to another place, so the terminal prints it fewer times or at another point than before (in each database's ingestion block with --verbose, at the drift step, before the verify report, at the source in the source commands)."""
    scenario = SCENARIOS_BY_NAME[name]
    golden.deploy(scenario, tmp_path, monkeypatch)
    getter, kwargs, _ = getter_call(scenario)

    data = getter(**kwargs)

    found = {path: len(warnings) for path, warnings in _warning_lists(data) if warnings}
    assert found == dict(WARNING_POSITIONS[name])
    assert all(warning.startswith("  Warning: Could not read ") for _, warnings in _warning_lists(data) for warning in warnings)


def test_without_verbose_no_ingestion_detail_is_read(tmp_path, monkeypatch, capsys):
    """Bug class: `get_db_status` reading every state file and source config for a caller that asked for the short form, which makes the cheap status call as slow as the verbose one on a deployment with many state files. Without `verbose` each database's ingestion holds only whether state files exist."""
    def must_not_read(*args, **kwargs):
        raise AssertionError("ingestion detail was read without verbose")

    scenario = SCENARIOS_BY_NAME["db_status_full"]
    golden.deploy(scenario, tmp_path, monkeypatch)
    monkeypatch.setattr(sdp, "_get_ingestion_state", must_not_read)
    monkeypatch.setattr(sdp, "_load_classifier_suffixes", must_not_read)

    data = sdp.get_db_status()

    assert data["verbose"] is False
    assert [database["ingestion"] for database in data["databases"]] == [{"has_state_files": True}] * 3


# ── no printing, by the syntax tree ─────────────────────────────────────────


def _cli_functions():
    """Name to syntax tree of every function defined at module level in the CLI."""
    tree = ast.parse(Path(sdp.__file__).read_text())
    return {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)}


def _reachable(start, functions):
    """The functions reached from start: every module-level function a reached function names."""
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


def _uses(names, functions, forbidden):
    return sorted(
        f"{name} uses {node.id}"
        for name in names
        for node in ast.walk(functions[name])
        if isinstance(node, ast.Name) and node.id in forbidden
    )


@pytest.mark.parametrize("getter", GETTER_NAMES)
def test_no_getter_reaches_a_print(getter):
    """Bug class: a getter that prints on a path no scenario runs (a rare config, a failing probe), directly or through a function of the CLI module it calls, or that calls one of the printers or the compose wrapper, which prints its command line. Checked on the syntax tree; functions imported from the package are not followed, so the one that prints the warning about an unreadable source config is named: a getter may reach it only through the helper that takes a list for the warnings."""
    functions = _cli_functions()
    reached = _reachable(getter, functions)
    printers = {name for name in functions if name.startswith(("print_", "_print_"))}

    assert _uses(reached, functions, {"print", "docker_compose"} | printers) == []
    assert _uses(reached - {"_collect_source_info"}, functions, {"load_source_config"}) == []


def test_the_print_walk_sees_a_print():
    """Bug class: a walk that passes because it recognises nothing. The printers and a terminal command must be reported."""
    functions = _cli_functions()

    assert _uses({"print_db_status"}, functions, {"print"}) != []
    assert "print_source_list" in _reachable("cmd_source_list", functions)
    assert _uses({"cmd_source_download"}, functions, {"load_source_config"}) != []


# ── secrets ─────────────────────────────────────────────────────────────────


MARKER = "made-up-secret-marker-7f3a"
SECRET_ENV_LINES = (
    f"POSTGRES_PASSWORD={MARKER}", f"MONGO_ADMIN_PASSWORD={MARKER}", f"STARROCKS_ROOT_PASSWORD={MARKER}",
    f"MONGO_INITDB_ROOT_PASSWORD={MARKER}", f"MYSQL_PWD={MARKER}", f"JOBS_PASSWORD={MARKER}", f"HF_TOKEN={MARKER}",
)


# The matching section comes first: the terminal shows the lines before a section's header as that section's output, so a marker in an earlier section's output would be printed, as it always was.
SECRET_MONGOIMPORT_LOG = f"""
============================================================
Timestamp: 2024-03-05T10:01:00.000001
Command: mongoimport --file /data/extracted/reddit/comments/RC_2024-02.ndjson
File: /data/extracted/reddit/comments/RC_2024-02.ndjson
============================================================
2024-03-05T10:01:02.000+0000	Failed: error processing document #7

============================================================
Timestamp: 2024-03-05T10:02:00.000001
Command: mongoimport --uri=mongodb://admin:{MARKER}@mongo:27017 --file /data/extracted/reddit/comments/RC_2024-01.ndjson
File: /data/extracted/reddit/comments/RC_2024-01.ndjson
============================================================
2024-03-05T10:02:01.000+0000	connected with {MARKER}
"""

# The YAML files listed here, which the getters load, get a key the text never shows. The classifier suffix file of postgres_ml is loaded too and is not among them.
SECRET_CONFIG_FILES = (
    "config/db/postgres.yaml", "config/db/mongo.yaml", "config/db/starrocks.yaml", "config/db/mcp.yaml",
    "config/jobs/config.local.yaml", "config/sources/reddit/platform.yaml", "docker-compose.override.yml",
)


def build_with_secrets(p):
    """The full deployment, with the marker as every password-like value in .env, as the content of every credentials file, (through the stub below) in every container's environment, under an unused key of the config files in SECRET_CONFIG_FILES, after ". See log:" in a failed entry's error, and in a mongoimport log section of a file that did not fail."""
    golden.build_full(p)
    # The full deployment has a failed entry that is a bare string, which `db status` counts and `source error-logs` cannot read; here every getter must run.
    p.state(golden.PG, "reddit_postgres_ml_comments", {"processed": ["toxic_roberta/RC_2024-01"], "failed": [{"filename": "toxic_roberta/RC_2024-03", "error": "boom"}]})
    with open(p.root / ".env", "a") as env_file:
        env_file.write("".join(line + "\n" for line in SECRET_ENV_LINES))
    for data_dir in (golden.PG, golden.MONGO, golden.SR):
        p.credentials(data_dir, text=MARKER + "\n")
    p.write("docker-compose.override.yml", "services: {}\n")
    for relative in SECRET_CONFIG_FILES:
        with open(p.root / relative, "a") as config_file:
            config_file.write(f"unused_secret: {MARKER}\n")
    p.state(golden.MONGO, "reddit_mongo_ingest_comments", {"processed": ["RC_2024-01"], "failed": [{"filename": "RC_2024-02.ndjson", "error": "x. See log: " + MARKER, "timestamp": "2024-03-05T10:11:12"}]})
    p.write(f"{golden.MONGO}/logs/mongoimport_reddit_comments.log", SECRET_MONGOIMPORT_LOG)


# The container of a run started in a terminal, as the stub below lists it, and the id `runs list` gives that run.
TERMINAL_RUN_CONTAINER = "ab" * 32
TERMINAL_RUN_ID = "live-" + "ab" * 6


# The two getters that list containers and read their output themselves. Every other getter reaches Docker through the container probe only.
RUN_GETTERS = ("get_run_list", "get_run_logs")


def stub_docker_with_secrets(monkeypatch, calls, root=None, running=None):
    """Answer the three subprocesses the container probe runs; `docker inspect` returns an environment that holds the marker. Any other subprocess fails the test, with one exception: while `running`, a list the test keeps the name of the getter it is calling in, names one of the run getters, `docker ps` lists one container that compose started with `run` for the parse service in the folder root, and `docker logs` returns two lines. For every other getter those two calls are unexpected, as they were before the run getters existed. The inspect answer stays the whole environment for the run getters too, which is what a format that printed more than the one variable would hand over."""
    inspect = [{
        "State": {"Health": {"Status": "healthy"}},
        "Config": {"Env": [f"POSTGRES_PASSWORD={MARKER}", f"MONGO_INITDB_ROOT_PASSWORD={MARKER}", f"STARROCKS_ROOT_PASSWORD={MARKER}", "POSTGRES_AUTH_ENABLED=true", "MONGO_AUTH_ENABLED=true", "STARROCKS_AUTH_ENABLED=true"]},
    }]

    def run(argv, **kwargs):
        calls.append(list(argv[:3]))
        if list(argv[:2]) == ["docker", "inspect"]:
            return subprocess.CompletedProcess(argv, 0, stdout=json.dumps(inspect), stderr="")
        for_a_run_getter = running is not None and running and running[0] in RUN_GETTERS
        if running is not None and running and running[0] == "get_db_index_targets" and list(argv[:3]) == ["docker", "compose", "exec"]:
            # The index listing asks the database itself, through `docker compose exec`; the stand-in databases answer.
            return FakeDatabases().run(argv, **kwargs)
        if for_a_run_getter and list(argv[:2]) == ["docker", "ps"]:
            return subprocess.CompletedProcess(argv, 0, stdout=json.dumps([TERMINAL_RUN_CONTAINER, str(root), "parse", ""]) + "\n", stderr="")
        if for_a_run_getter and list(argv[:2]) == ["docker", "logs"]:
            return subprocess.CompletedProcess(argv, 0, stdout="parsing RC_2024-01\ndone\n", stderr="")
        assert list(argv[:3]) == ["docker", "compose", "ps"], f"unexpected subprocess: {argv}"
        if "-q" in argv:
            return subprocess.CompletedProcess(argv, 0, stdout="0123456789ab\n", stderr="")
        return subprocess.CompletedProcess(argv, 0, stdout=golden.compose_ps_output(("postgres", "mongo", "starrocks")), stderr="")

    monkeypatch.setattr(sdp.subprocess, "run", run)


# Every getter, in every form that reads something else.
SECRET_CALLS = [
    ("get_db_settings", {}, ["print_db_settings"]),
    ("get_db_status", {}, ["print_db_status"]),
    ("get_db_status", {"verbose": True}, ["print_db_status"]),
    ("get_db_index_targets", {"source": "reddit", "db": "postgres", "admin_password": "made-up-admin-value"}, ["print_db_index_targets"]),
    ("get_db_index_targets", {"source": "reddit", "db": "mongo", "admin_password": "made-up-admin-value"}, ["print_db_index_targets"]),
    ("get_db_index_targets", {"source": "reddit", "db": "starrocks", "admin_password": "made-up-admin-value"}, ["print_db_index_targets"]),
    ("get_db_verify", {}, ["print_db_verify", "print_db_verify_json"]),
    ("get_db_verify", {"db": "postgres"}, ["print_db_verify", "print_db_verify_json"]),
    ("get_source_list", {}, ["print_source_list"]),
    ("get_source_status", {}, ["print_source_status"]),
    ("get_source_status", {"name": "reddit"}, ["print_source_status"]),
    ("get_run_list", {}, ["print_run_list"]),
    ("get_run_logs", {"run_id": TERMINAL_RUN_ID}, ["print_run_logs"]),
    ("get_source_progress", {}, ["print_source_progress"]),
    ("get_source_progress", {"name": "reddit"}, ["print_source_progress"]),
    ("get_source_error_logs", {}, ["print_source_error_logs"]),
    ("get_source_error_logs", {"name": "reddit", "profile": "postgres_ingest"}, ["print_source_error_logs"]),
    ("get_source_error_logs", {"name": "reddit", "profile": "mongo_ingest"}, ["print_source_error_logs"]),
    ("get_db_restart_needed", {}, ["print_db_restart_needed"]),
    ("get_db_llm_settings", {}, ["print_db_llm_settings"]),
]


def test_no_secret_reaches_the_data_or_the_text(tmp_path, monkeypatch, capsys):
    """Bug class: a getter that hands a caller more than the terminal shows: the dict loaded from `.env` (database passwords, the Hugging Face token), the verify context built from it, a whole config dict (a database, MCP, jobs or source config, the compose override) where the text shows a few of its keys, the content of a credentials file, a container's environment from `docker inspect`, the part of a failed entry's error after ". See log:", or log lines of a file that did not fail. The data goes to a web client as JSON, so one such value in it is a leak. The marker is planted in every one of these places; the checks before the loop prove the code under test had it in hand."""
    root = tmp_path.resolve() / "project"
    build_with_secrets(golden.Project(root))
    golden.point_at(monkeypatch, root)
    calls = []
    running = [None]
    stub_docker_with_secrets(monkeypatch, calls, root, running)

    assert MARKER in sdp.load_env().values()
    assert MARKER in json.dumps(sdp._build_verify_context(env=sdp.load_env(), probe_containers=False))
    for loaded in (sdp._load_db_yaml("postgres"), sdp._load_db_yaml("mongo"), sdp._load_db_yaml("starrocks"), sdp._load_mcp_config(), sdp._load_jobs_config(), sdp._read_override_yaml(), utils.read_source_config("reddit")[0]):
        assert loaded["unused_secret"] == MARKER
    mongo = sdp.get_source_error_logs(name="reddit", profile="mongo_ingest")["sources"][0]["profiles"][0]
    assert [failure["error"] for failure in mongo["files"][0]["failed"]] == ["x"]
    assert [section["header"][3] for section in mongo["mongoimport_sections"]] == ["File: /data/extracted/reddit/comments/RC_2024-02.ndjson"]
    assert sdp._probe_container_state("postgres") == {"running": True, "healthy": True, "env_auth": True}
    assert ["docker", "inspect", "0123456789ab"] in calls
    # The run listing asks Docker about its container and gets the whole environment back; the run is listed all the same, without a start time or a source.
    running[0] = "get_run_list"
    assert [(run["id"], run["profile"], run["started"], run["source"]) for run in sdp.get_run_list()["runs"]] == [(TERMINAL_RUN_ID, "parse", None, None)]
    assert ["docker", "inspect", "--format"] in calls
    running[0] = None

    assert sorted({name for name, _, _ in SECRET_CALLS}) == GETTER_NAMES
    for getter, kwargs, printers in SECRET_CALLS:
        running[0] = getter
        data = getattr(sdp, getter)(**kwargs)
        running[0] = None
        assert MARKER not in json.dumps(data), (getter, kwargs)
        for printer in printers:
            getattr(sdp, printer)(data)
            captured = capsys.readouterr()
            assert captured.out != ""
            assert MARKER not in captured.out + captured.err, (getter, kwargs, printer)


# ── source names ────────────────────────────────────────────────────────────


# The line and paragraph separators and the next-line character are written as escapes: typed raw, an editor may turn them into spaces. The string of é is 128 characters and 256 bytes.
MALFORMED_NAMES = ["../x", "a/b", "/srv/x", ".", "..", "a\\b", "ok\n", "a\u2028b", "a\u2029b", "a\x85b", "x" * 300, "\u00e9" * 128, 5, "", 0]


@pytest.mark.parametrize("getter", ["get_source_status", "get_source_error_logs", "get_source_progress"])
@pytest.mark.parametrize("name", MALFORMED_NAMES)
def test_getter_refuses_a_malformed_source_name(getter, name, tmp_path, monkeypatch, capsys):
    """Bug class: a getter called directly (no terminal command in front of it) reporting on a folder outside config/sources/, because the name check was left to the unchecked function the terminal command calls; and an error text that repeats the name, which is whatever the caller sent. The getter raises before it lists or loads anything, and its message is a constant."""
    reads = []

    def must_not_read(*args, **kwargs):
        # Recorded as well as raised: a getter that turns an error of the config reader into a fact would hide the raise.
        reads.append(args)
        raise AssertionError("a source was read for a malformed name")

    golden.deploy(SCENARIOS_BY_NAME["source_status_all"], tmp_path, monkeypatch)
    (tmp_path.resolve() / "project" / "config" / "x").mkdir()
    (tmp_path.resolve() / "project" / "config" / "x" / "platform.yaml").write_text("data_types: [posts]\n")
    monkeypatch.setattr(utils, "list_sources", must_not_read)
    monkeypatch.setattr(utils, "read_source_config", must_not_read)
    monkeypatch.setattr(sdp, "load_env", must_not_read)

    with pytest.raises(sdp.ActionError) as exc:
        getattr(sdp, getter)(name=name)

    assert reads == []
    assert (exc.value.message, exc.value.exit_code) == (NAME_NOT_VALID, 1)
    assert capsys.readouterr().out == ""


# Names the getters refuse and the terminal takes; the last one is an absolute path under the temp folder.
TYPED_NAMES = ["../nosuch", "a/b", ".", "..", "a\\b", "ok\n", "{tmp}/elsewhere"]


@pytest.mark.parametrize("command, getter", [("source status", "get_source_status"), ("source error-logs", "get_source_error_logs")])
@pytest.mark.parametrize("typed", TYPED_NAMES)
def test_command_takes_a_malformed_name_as_typed(command, getter, typed, tmp_path, monkeypatch, capsys):
    """Bug class: a terminal command routed through the getter that checks the name, which would make the terminal refuse names it always took and so change the command line (the golden files pin `../x` alone). The command never calls that getter; a name that reaches no platform.yaml is answered with "not found" and exit code 0, as it was before the getters existed."""
    def must_not_be_called(**kwargs):
        raise AssertionError("the terminal command called the getter that checks the name")

    golden.deploy(SCENARIOS_BY_NAME["source_status_all"], tmp_path, monkeypatch)
    monkeypatch.setattr(sdp, getter, must_not_be_called)
    name = typed.format(tmp=tmp_path.resolve())

    assert golden.COMMANDS[command](argparse.Namespace(name=name, profile=None)) == 0
    assert capsys.readouterr().out.endswith(f"=\n\n  Source '{name}': not found\n\n")


# ── unknown database and profile ────────────────────────────────────────────


# getter, keyword arguments, the message expected.
UNKNOWN_VALUES = [
    pytest.param("get_db_verify", {"db": "postgress"}, "unknown database", id="db-misspelt"),
    pytest.param("get_db_verify", {"db": "mcp"}, "unknown database", id="db-check-group-name"),
    pytest.param("get_db_verify", {"db": ""}, "unknown database", id="db-empty"),
    pytest.param("get_db_verify", {"db": ["postgres"]}, "unknown database", id="db-list"),
    pytest.param("get_source_error_logs", {"profile": "nonsense"}, "unknown profile", id="profile-nonsense"),
    pytest.param("get_source_error_logs", {"name": "reddit", "profile": "parse"}, "unknown profile", id="profile-without-ingestion"),
    pytest.param("get_source_error_logs", {"profile": ""}, "unknown profile", id="profile-empty"),
    pytest.param("get_source_error_logs", {"name": "../x", "profile": "nonsense"}, "unknown profile", id="profile-and-name"),
]


@pytest.mark.parametrize("getter, kwargs, message", UNKNOWN_VALUES)
def test_getter_refuses_an_unknown_database_or_profile(getter, kwargs, message, tmp_path, monkeypatch, capsys):
    """Bug class: a getter that takes a value it does not know as a filter that matches nothing and reports the empty result as good news: `db verify` limited to a misspelt database drops every database group and can answer "ok" over real drift, and `error-logs` for a profile that does not exist answers "no errors". The getter raises before it reads anything, with a message that does not repeat the value."""
    def must_not_read(*args, **kwargs):
        raise AssertionError("something was read for a value the getter does not know")

    golden.deploy(SCENARIOS_BY_NAME["db_verify_drift"], tmp_path, monkeypatch)
    for module, name in ((sdp, "load_env"), (sdp, "_get_configured_db_services"), (sdp, "_build_verify_context"), (utils, "list_sources"), (utils, "read_source_config")):
        monkeypatch.setattr(module, name, must_not_read)

    with pytest.raises(sdp.ActionError) as exc:
        getattr(sdp, getter)(**kwargs)

    assert (exc.value.message, exc.value.exit_code) == (message, 1)
    assert capsys.readouterr().out == ""


def test_getter_accepts_every_database_and_profile_it_knows(tmp_path, monkeypatch, capsys):
    """Bug class: the check above refusing a legitimate value: a database or a profile the command line or the socket accepts (the registry's lists are compared too, so a value added there cannot be refused here unseen), or `None`, which means all. Each value must also do its job: the report holds only that database's group and the two cross-cutting ones, the error log only that profile."""
    assert set(verbs.DATABASES) == set(sdp._VERIFY_DATABASES)
    assert set(verbs.ERROR_LOG_PROFILES) <= set(sdp.INGESTION_PROFILES)

    golden.deploy(SCENARIOS_BY_NAME["db_verify_drift"], tmp_path, monkeypatch)
    assert set(sdp.get_db_verify()["report"]["results"]) == {"postgres", "mongo", "mcp", "jobs"}
    for db in sdp._VERIFY_DATABASES:
        assert set(sdp.get_db_verify(db=db)["report"]["results"]) == ({db} & {"postgres", "mongo"}) | {"mcp", "jobs"}

    golden.deploy(SCENARIOS_BY_NAME["error_logs_named"], tmp_path / "errors", monkeypatch)
    for profile in sdp.INGESTION_PROFILES:
        checked = sdp.get_source_error_logs(name="reddit", profile=profile)["sources"][0]["profiles"]
        assert [(entry["profile"], bool(entry["files"])) for entry in checked] == [(profile, True)]
    assert [entry["profile"] for entry in sdp.get_source_error_logs(name="reddit")["sources"][0]["profiles"]] == sdp.INGESTION_PROFILES


# ── NaN and infinities in a state file ──────────────────────────────────────


def build_state_with_json_constants(p):
    """PostgreSQL and a source whose state files hold the three constants JSON does not have but the parser accepts, and a number too large for a float: as a dataset, as the file in progress, as the update time, and as the file name and time of a failed entry. MongoDB is not a configured database here, so `db status` does not read its state file, where the update time is not text."""
    p.database("postgres")
    p.source("reddit", "data_types: [comments]\n", ("postgres_ingest", "mongo_ingest"))
    p.state(golden.PG, "reddit_postgres_ingest_comments", '{"processed": ["RC_2024-01", NaN], "failed": [{"filename": -Infinity, "error": "boom", "timestamp": NaN}], "in_progress": Infinity, "last_updated": "2024-03-05"}')
    p.state("data/database/mongo", "reddit_mongo_ingest_comments", '{"processed": [Infinity, -1e999], "failed": [], "in_progress": -Infinity, "last_updated": NaN}')


# What the commands printed for that deployment before they had getters (taken from that code), per getter.
CONSTANTS_TEXT = {
    "get_db_status": "\n  Social Data Pipeline - Database Status\n  =====================================\n\n  Authentication: disabled\n\n  PostgreSQL:\n    Path:      ./data/database/postgres\n    Name:      datasets\n    Port:      5432\n    Running:   no\n\n    Ingestion status:\n      platform: reddit\n        - reddit.comments: 2 datasets (main data) [in progress: inf] [1 failed]\n            latest: nan, updated: 2024-03-05\n\n",
    "get_source_status": "\n  Social Data Pipeline - Source Status\n  ===================================\n\n  Source: reddit\n    Platform:    reddit\n    Data types:  comments\n    Profiles:    postgres_ingest, mongo_ingest\n    Paths:\n      Dumps:     ./data/dumps/reddit\n      Extracted: ./data/extracted/reddit\n      Parsed:    ./data/parsed/reddit\n      Output:    ./data/output/reddit\n\n    PostgreSQL ingestion:\n      ingest_comments: 2 datasets (latest: nan) [in progress: inf] [1 failed]\n        last updated: 2024-03-05\n\n    MongoDB ingestion:\n      ingest_comments: 2 datasets (latest: -inf) [in progress: -inf]\n        last updated: nan\n\n",
    "get_source_error_logs": "\n  Social Data Pipeline - Error Logs\n  ==================================\n\n  Source: reddit\n\n    postgres_ingest errors (comments): 1 failed\n\n      -inf [nan]\n        boom\n\n",
}
CONSTANTS_CALLS = [
    pytest.param("get_db_status", {"verbose": True}, "print_db_status", id="db-status"),
    pytest.param("get_source_status", {"name": "reddit"}, "print_source_status", id="source-status"),
    pytest.param("get_source_error_logs", {"name": "reddit"}, "print_source_error_logs", id="error-logs"),
]


@pytest.mark.parametrize("getter, kwargs, printer", CONSTANTS_CALLS)
def test_state_file_constants_stay_json_and_print_as_before(getter, kwargs, printer, tmp_path, monkeypatch, capsys):
    """Bug class: a value copied from a state file into the data that JSON cannot carry. Python's parser reads `NaN`, `Infinity`, `-Infinity` and a number beyond the float range as non-finite floats; written out again they are bare words a browser's JSON parser rejects, so one damaged state file would break the whole answer. Each of the three places that read a state file must turn them into the text the terminal shows (`nan`, `inf`, `-inf`), and where such a value is printed on its own the text must stay what it was (inside a list or object that is printed whole it now shows in quotes)."""
    scenario = golden.Scenario("constants", build_state_with_json_constants, "db status")
    golden.deploy(scenario, tmp_path, monkeypatch)

    data = getattr(sdp, getter)(**kwargs)

    assert json.loads(json.dumps(data, allow_nan=False)) == data
    getattr(sdp, printer)(data)
    assert capsys.readouterr().out == CONSTANTS_TEXT[getter]


# ── facts in a state file's name ────────────────────────────────────────────


def build_state_names(p):
    """State files whose names have the usual shape (`<source>_<profile>_<data_type>`), for each ingestion profile, and names that do not: no label at all, the label twice, no data type, a profile that does not exist."""
    p.database("postgres")
    p.database("starrocks")
    p.source("reddit", "data_types: [comments]\n", golden.ALL_PROFILES)
    one = {"processed": ["RC_2024-01"], "failed": []}
    p.state(golden.PG, "reddit_postgres_ingest_comments", one)
    p.state(golden.PG, "reddit_postgres_ml_comments", {"processed": ["toxic/RC_2024-01", "lingua/RC_2024-01"], "failed": []})
    p.state(golden.PG, "adhoc", one)
    p.state(golden.PG, "reddit_postgres_ingest_a_postgres_ingest_b", one)
    p.state(golden.PG, "reddit_postgres_ingest", one)
    p.state(golden.PG, "reddit_postgres_other_comments", one)
    p.state("data/database/starrocks", "reddit_sr_ingest_comments", one)
    p.state("data/database/starrocks", "reddit_sr_ml_comments", one)


# (source group, composed table, profile, data type) of every entry of `db status --verbose`, per database.
STATE_NAME_FACTS_BY_DATABASE = {
    "postgres": [
        ("adhoc", "adhoc.adhoc", None, None),
        ("reddit", "reddit.", None, None),
        ("reddit", "reddit.comments", "postgres_ingest", "comments"),
        ("reddit", "reddit.comments_lingua", "postgres_ml", "comments"),
        ("reddit", "reddit.comments_toxic", "postgres_ml", "comments"),
        ("reddit", "reddit.comments", None, None),
        ("reddit_postgres_ingest_a_postgres_ingest_b", "reddit_postgres_ingest_a_postgres_ingest_b.reddit_postgres_ingest_a_postgres_ingest_b", None, None),
    ],
    "starrocks": [
        ("reddit", "reddit.comments", "sr_ingest", "comments"),
        ("reddit", "reddit.comments", "sr_ml", "comments"),
    ],
}

# (composed label, profile, data type) of every entry of `source status reddit`, per database view.
STATE_NAME_FACTS_BY_VIEW = {
    "postgres": [
        ("ingest", None, None),
        ("reddit_postgres_ingest_a_postgres_ingest_b", None, None),
        ("ingest_comments", "postgres_ingest", "comments"),
        ("ml_comments", "postgres_ml", "comments"),
        ("other_comments", None, None),
    ],
    "mongo": [],
    "sr_ingest": [("comments", "sr_ingest", "comments")],
    "sr_ml": [("comments", "sr_ml", "comments")],
}


def test_ingestion_entries_carry_the_profile_and_data_type_of_their_state_file(tmp_path, monkeypatch, capsys):
    """Bug class: a reader of the data having to take the composed text apart again (`ingest_comments`, `reddit_data.comments_toxicity`) to learn which profile and data type an entry is about, and getting it wrong where the text is ambiguous: two profiles share one table name in StarRocks, a schema or a suffix comes from config. Both getters give `profile` (an ingestion profile) and `data_type` next to the text, the same pair for the same file, and None for both when the file name does not have the usual shape, never a guess."""
    scenario = golden.Scenario("state-names", build_state_names, "db status")
    golden.deploy(scenario, tmp_path, monkeypatch)

    by_database = {
        database["db"]: [(group["source"], entry["table"], entry["profile"], entry["data_type"]) for group in database["ingestion"]["sources"] for entry in group["entries"]]
        for database in sdp.get_db_status(verbose=True)["databases"]
    }
    by_view = {
        view["db_type"]: [(entry["label"], entry["profile"], entry["data_type"]) for entry in view["entries"]]
        for view in sdp.get_source_status(name="reddit")["sources"][0]["ingestion"]
    }

    assert by_database == STATE_NAME_FACTS_BY_DATABASE
    assert by_view == STATE_NAME_FACTS_BY_VIEW
    assert {profile for facts in list(by_database.values()) + list(by_view.values()) for *_, profile, _ in facts} <= set(sdp.INGESTION_PROFILES) | {None}


# ── a source's error log is its own (B3) ────────────────────────────────────


def build_errors_of_one_source(p):
    """Two sources with MongoDB ingestion configured: `reddit` has a state file with failed entries and a mongoimport log that holds a section for one of them; `twitter` has no state file of its own."""
    p.env(*golden.DATA_PATH_LINES)
    p.source("reddit", "data_types: [comments]\n", ("mongo_ingest",))
    p.source("twitter", "data_types: [tweets]\n", ("mongo_ingest",))
    p.state(golden.MONGO, "reddit_mongo_ingest_comments", golden._state([], [golden._failure("RC_2024-02.ndjson", golden.SEE_LOG.format(name="RC_2024-02.ndjson"))]))
    p.write(f"{golden.MONGO}/logs/mongoimport_reddit_comments.log", golden.MONGOIMPORT_LOG_FIRST)


def test_a_source_without_state_files_shows_no_errors_of_another_source(tmp_path, monkeypatch, capsys):
    """Bug class: B3, a reader scoped to one source that widens to every source's state files when the named one has none (the legacy glob `*_<profile>_*.json`): the reddit source's Errors tab and `source error-logs reddit` showed other sources' failed files and their mongoimport logs. A source without a state file of its own has no failed entries and no log sections; the other source's report is untouched."""
    scenario = golden.Scenario("errors-of-one-source", build_errors_of_one_source, "source error-logs")
    golden.deploy(scenario, tmp_path, monkeypatch)
    searched = []
    real_sections = sdp._get_mongoimport_log_sections
    monkeypatch.setattr(sdp, "_get_mongoimport_log_sections", lambda log_dir, failed: searched.append(list(failed)) or real_sections(log_dir, failed))

    twitter = sdp.get_source_error_logs(name="twitter")

    assert twitter["sources"][0]["profiles"] == [{"profile": "mongo_ingest", "files": [], "mongoimport_sections": []}]
    assert searched == []
    sdp.print_source_error_logs(twitter)
    text = capsys.readouterr().out
    assert text.endswith("\n  Source: twitter\n    No errors found.\n\n")
    assert "RC_2024-02" not in text and "reddit" not in text

    reddit = sdp.get_source_error_logs(name="reddit")["sources"][0]["profiles"]

    assert [(checked["profile"], [entry["filename"] for state in checked["files"] for entry in state["failed"]]) for checked in reddit] == [("mongo_ingest", ["RC_2024-02.ndjson"])]
    assert searched == [["RC_2024-02.ndjson"]]
    assert [Path(section["log_file"]).name for section in reddit[0]["mongoimport_sections"]] == ["mongoimport_reddit_comments.log"]


# ── the source config reader ────────────────────────────────────────────────


# id, content of platform.yaml (None: no file), the config expected.
SOURCE_CONFIGS = [
    pytest.param("data_types: [posts]\n", {"data_types": ["posts"]}, id="readable"),
    pytest.param("", None, id="empty"),
    pytest.param(None, None, id="missing"),
    pytest.param(golden.BROKEN_YAML, None, id="unreadable"),
]


@pytest.mark.parametrize("content, config", SOURCE_CONFIGS)
def test_read_source_config_returns_what_load_source_config_prints(content, config, tmp_path, monkeypatch, capsys):
    """Bug class: the printing and the non-printing form of the source config reader drifting apart: the non-printing one printing after all, the printing one losing its warning or printing it twice, or the two disagreeing on the config. The warning is one line that names the file, only for a file that exists and cannot be read."""
    root = tmp_path.resolve()
    golden.point_at(monkeypatch, root)
    (root / "config" / "sources" / "src").mkdir(parents=True)
    path = root / "config" / "sources" / "src" / "platform.yaml"
    if content is not None:
        path.write_text(content)

    read_config, warning = utils.read_source_config("src")
    assert capsys.readouterr().out == ""
    loaded = utils.load_source_config("src")
    printed = capsys.readouterr().out

    assert read_config == loaded == config
    if content == golden.BROKEN_YAML:
        assert warning == f"  Warning: Could not read {path}: {golden.broken_yaml_message()}"
        assert printed == warning + "\n"
    else:
        assert warning is None
        assert printed == ""
