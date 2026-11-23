"""Tests for `do_db_setup_mcp` and `do_db_setup_jobs` (the questions of `db setup-mcp` and `db setup-jobs` answered by typed inputs), the typed inputs of their verbs, and the two read commands beside them: `db restart-needed` and `db llm-settings`.

The contracts pinned here, each test naming the bug class it catches:

- For the same answers, an action writes exactly what the terminal's questionnaire writes: every file of the project compared byte for byte, the `.bak` copies included. The terminal is driven through its own `ask*` helpers, answered by their tags.
- The terminal's refusals are the action's, with nothing written; the refusals the browser path adds (a server or a target of a database that is not set up, a port another service uses, the scheduler page's login switched off, a results folder that breaks `.env` or a mount) also write nothing, and a service keeps its own port.
- The typed inputs refuse what the terminal would never write.
- `db restart-needed` names a running service exactly when one of its settings files is newer than its container, and reads nothing of `docker inspect` but the label and the start time.
- `db llm-settings` offers the defaults the questionnaires offer.

Every test points the CLI and the setup modules at a temp project and replaces `subprocess.run`, so nothing touches the real tree or Docker. Passwords are made-up values.
"""

from __future__ import annotations

import os
import subprocess

import pytest

import sdp
from social_data_pipeline.control import errors, verbs
from social_data_pipeline.setup import jobs as jobs_setup
from social_data_pipeline.setup import mcp as mcp_setup
from social_data_pipeline.setup import utils


# ── helpers ─────────────────────────────────────────────────────────────────


def _point_at(monkeypatch, root):
    config = root / "config"
    for module in (sdp, utils, mcp_setup, jobs_setup):
        monkeypatch.setattr(module, "ROOT", root)
        monkeypatch.setattr(module, "CONFIG_DIR", config)
    monkeypatch.setattr(utils, "DB_CONFIG_DIR", config / "db")
    monkeypatch.setattr(utils, "SOURCES_DIR", config / "sources")


def _no_docker(monkeypatch, calls=None):
    """Docker cannot be asked: every subprocess fails (and is recorded)."""
    def run(argv, **kwargs):
        if calls is not None:
            calls.append(list(argv))
        return subprocess.CompletedProcess(argv, 1, stdout="", stderr="")

    monkeypatch.setattr(sdp.subprocess, "run", run)


def _seed(root, *, auth=False, databases=("postgres", "mongo", "starrocks")):
    """A project with the databases set up; with auth, each has authentication, the read-only user `reader` and its credentials file."""
    (root / "config" / "db").mkdir(parents=True)
    env = ["# made-up deployment", "PGDATA_PATH=./data/database/postgres", "MONGO_DATA_PATH=./data/database/mongo", "STARROCKS_DATA_PATH=./data/database/starrocks", "POSTGRES_PORT=5432", "DB_NAME=reddit", "# POSTGRES_MCP_PORT=", "UNRELATED=1"]
    for db in databases:
        text = "port: 1\n" + ("auth: true\nro_username: reader\n" if auth else "")
        (root / "config" / "db" / f"{db}.yaml").write_text(text)
        if auth:
            folder = root / "data" / "database" / db
            folder.mkdir(parents=True)
            (folder / ".ro_credentials").write_text("made-up-read-only-value\n")
    (root / ".env").write_text("\n".join(env) + "\n")
    (root / "config" / "starrocks").mkdir()
    (root / "config" / "starrocks" / "fe.local.conf").write_text("# made-up\nquery_timeout = 300\n# enable_outfile_to_local = false\n")
    (root / "docker-compose.override.yml").write_text("services:\n  starrocks:\n    volumes:\n    - /mnt/disk1:/data/deploy/starrocks/be/storage1\n    - ./old:/jobs_export:rw\n  postgres:\n    volumes:\n    - /mnt/ts:/data/tablespace/ts1\n")


def _snapshot(root):
    return sorted((str(path.relative_to(root)), path.read_bytes() if path.is_file() else None) for path in root.rglob("*"))


def _answering(monkeypatch, module, answers):
    """Answer the questionnaire's `ask*` helpers of `module` by their tags; a question without an answer takes its default, as Enter does."""
    asked = []

    def answer(tag, default):
        asked.append(tag)
        return answers.get(tag, default)

    monkeypatch.setattr(module, "ask_bool", lambda prompt, default=True, tag=None: answer(tag, default))
    if hasattr(module, "ask_int"):
        monkeypatch.setattr(module, "ask_int", lambda prompt, default=None, tag=None: int(answer(tag, default)))
    if hasattr(module, "ask"):
        monkeypatch.setattr(module, "ask", lambda prompt, default=None, tag=None: str(answer(tag, default)))
    return asked


def _two_projects(tmp_path, **seed):
    first, second = tmp_path / "terminal", tmp_path / "action"
    _seed(first, **seed)
    _seed(second, **seed)
    return first, second


def _without_roots(snapshot, root):
    """A snapshot whose file contents name the project folder written as <root>, so that two projects compare."""
    return [(name, content.replace(str(root).encode(), b"<root>") if content else content) for name, content in snapshot]


# ── the same files as the terminal ──────────────────────────────────────────


MCP_CASES = [
    pytest.param(
        {"mcp_pg_enable": True, "mcp_pg_port": 8100, "mcp_pg_write_access": True, "mcp_mongo_enable": True, "mcp_mongo_port": 3100, "mcp_mongo_write_access": False, "mcp_sr_enable": True, "mcp_sr_port": 9100},
        {"postgres": {"port": 8100, "write_access": True}, "mongo": {"port": 3100}, "starrocks": {"port": 9100}},
        False, id="all-on"),
    pytest.param(
        {"mcp_pg_enable": False, "mcp_mongo_enable": True, "mcp_mongo_port": 3000, "mcp_mongo_write_access": True, "mcp_sr_enable": False},
        {"mongo": {"port": 3000, "write_access": True}},
        False, id="mongo-only-writable"),
    pytest.param(
        {"mcp_pg_enable": True, "mcp_pg_port": 8000, "mcp_pg_write_access": False, "mcp_mongo_enable": False, "mcp_sr_enable": True, "mcp_sr_port": 9000},
        {"postgres": {"port": 8000, "write_access": False}, "starrocks": {"port": 9000}},
        True, id="with-auth-and-read-only-users"),
]


@pytest.mark.parametrize("answers, fields, auth", MCP_CASES)
def test_the_mcp_action_writes_what_the_terminal_writes(tmp_path, monkeypatch, capsys, answers, fields, auth):
    """Bug class: an action that writes other bytes than the terminal for the same answers: another key order in config/db/mcp.yaml, a missing or extra .env key (the read-only user's), a value spelled otherwise (`False` for `false`), no .bak copy of the file it replaces."""
    _no_docker(monkeypatch)
    terminal, action = _two_projects(tmp_path, auth=auth)
    for root in (terminal, action):
        (root / "config" / "db" / "mcp.yaml").write_text("postgres:\n  enabled: false\n")

    _point_at(monkeypatch, terminal)
    _answering(monkeypatch, mcp_setup, {**answers, "mcp_write_files": True})
    mcp_setup.main()
    _point_at(monkeypatch, action)
    assert sdp.do_db_setup_mcp(**fields) == 0

    assert _without_roots(_snapshot(action), action) == _without_roots(_snapshot(terminal), terminal)
    assert (action / "config" / "db" / "mcp.yaml.bak").exists()


JOBS_CASES = [
    pytest.param(
        {"jobs_port": 8060, "jobs_result_root": "<abs>", "jobs_max_concurrent": 4, "jobs_history_retention": 300,
         "jobs_pg_target_enable": True, "jobs_pg_target_name": "pg_a", "jobs_pg_target_db": "reddit", "jobs_sr_target_enable": True, "jobs_sr_target_name": "sr_a", "jobs_mg_target_enable": True, "jobs_mg_target_name": "mg_a",
         "jobs_pg_timeout_hours": 2, "jobs_sr_timeout_hours": 72, "jobs_mg_timeout_hours": 0, "jobs_ui_auth_enable": False},
        {"port": 8060, "result_root": "<abs>", "max_concurrent": 4, "history_retention": 300,
         "postgres_target": {"name": "pg_a", "database": "reddit", "timeout_hours": 2}, "starrocks_target": {"name": "sr_a", "timeout_hours": 72}, "mongo_target": {"name": "mg_a", "timeout_hours": 0}},
        id="three-targets"),
    pytest.param(
        {"jobs_port": 8050, "jobs_result_root": "./data/jobs/results", "jobs_max_concurrent": 20, "jobs_history_retention": 500,
         "jobs_pg_target_enable": False, "jobs_sr_target_enable": True, "jobs_sr_target_name": "sr_main", "jobs_mg_target_enable": False, "jobs_sr_timeout_hours": 5, "jobs_ui_auth_enable": True},
        {"port": 8050, "result_root": "data/jobs/results", "max_concurrent": 20, "history_retention": 500, "starrocks_target": {"name": "sr_main", "timeout_hours": 5}, "auth_enabled": True},
        id="starrocks-only-login-on"),
    pytest.param(
        {"jobs_port": 8050, "jobs_result_root": "./results", "jobs_max_concurrent": 1, "jobs_history_retention": 1,
         "jobs_pg_target_enable": True, "jobs_pg_target_name": "pg_main", "jobs_pg_target_db": "datasets", "jobs_sr_target_enable": False, "jobs_mg_target_enable": False, "jobs_pg_timeout_hours": 0, "jobs_ui_auth_enable": False},
        {"port": 8050, "result_root": "./results", "max_concurrent": 1, "history_retention": 1, "postgres_target": {"name": "pg_main", "database": "datasets", "timeout_hours": 0}},
        id="postgres-only"),
]


@pytest.mark.parametrize("answers, fields", JOBS_CASES)
def test_the_jobs_action_writes_what_the_terminal_writes(tmp_path, monkeypatch, capsys, answers, fields):
    """Bug class: an action that writes other bytes than the terminal for the same answers: the targets or timeouts in another order, a timeout in hours where seconds belong, a /jobs_export mount left from before or added to MongoDB, a compose override that loses another mount, fe.local.conf left without enable_outfile_to_local, a relative results folder written without its `./` (which compose reads as a volume name), the results folder or data/jobs not made."""
    _no_docker(monkeypatch)
    outside = str(tmp_path / "results-outside")
    answers = {key: outside if value == "<abs>" else value for key, value in answers.items()}
    fields = {key: outside if value == "<abs>" else value for key, value in fields.items()}
    terminal, action = _two_projects(tmp_path)
    for root in (terminal, action):
        (root / "config" / "jobs").mkdir(parents=True)
        (root / "config" / "jobs" / "config.local.yaml").write_text("port: 8050\ntargets:\n  old:\n    backend: postgres\n    database: x\n")

    _point_at(monkeypatch, terminal)
    _answering(monkeypatch, jobs_setup, {**answers, "jobs_write_files": True})
    jobs_setup.main()
    _point_at(monkeypatch, action)
    assert sdp.do_db_setup_jobs(**fields) == 0

    assert _snapshot(action) == _snapshot(terminal)
    assert (action / "config" / "jobs" / "config.local.yaml.bak").exists()


# ── refusals ────────────────────────────────────────────────────────────────


def test_the_terminals_refusals_are_the_actions_and_nothing_is_written(tmp_path, monkeypatch, capsys):
    """Bug class: an action that writes where the terminal refuses: with no database set up, with a database whose read-only credentials file is missing (the MCP server would fail its login at start), or with no target for the scheduler; or that refuses with another text than the terminal's recovery hint."""
    _no_docker(monkeypatch)
    root = tmp_path / "p"
    root.mkdir()
    _point_at(monkeypatch, root)
    before = _snapshot(root)
    assert sdp.do_db_setup_mcp(postgres={"port": 8000}) == 1
    assert sdp.do_db_setup_jobs(port=8050, result_root="./r", max_concurrent=1, history_retention=1) == 1
    assert capsys.readouterr().out.count("  Error: No databases configured. Run first: python sdp.py db setup\n") == 2
    assert _snapshot(root) == before

    _seed(root, auth=True)
    (root / "data" / "database" / "mongo" / ".ro_credentials").unlink()
    before = _snapshot(root)
    assert sdp.do_db_setup_mcp(postgres={"port": 8000}) == 1
    out = capsys.readouterr().out
    assert "  Error: RO credential file(s) missing for enabled databases:\n    - mongo: " in out
    assert "  MCP servers read these files at startup. Regenerate them with:\n    python sdp.py db recover-password --regenerate-ro\n" in out
    assert sdp.do_db_setup_jobs(port=8050, result_root="./r", max_concurrent=1, history_retention=1) == 1
    assert "  Error: at least one target must be configured." in capsys.readouterr().out
    assert _snapshot(root) == before


@pytest.mark.parametrize("call, message", [
    (lambda: sdp.do_db_setup_mcp(mongo={"port": 3000}), "MongoDB is not configured: run `python sdp.py db setup` first"),
    (lambda: sdp.do_db_setup_jobs(port=8050, result_root="./r", max_concurrent=1, history_retention=1, mongo_target={"name": "m", "timeout_hours": 0}), "MongoDB is not configured: run `python sdp.py db setup` first"),
    (lambda: sdp.do_db_setup_mcp(postgres={"port": 5432}), "the port chosen for the PostgreSQL MCP server is already used by PostgreSQL: choose another one"),
    (lambda: sdp.do_db_setup_mcp(postgres={"port": 8030}), "the port chosen for the PostgreSQL MCP server is already used by StarRocks: choose another one"),
    (lambda: sdp.do_db_setup_mcp(postgres={"port": 8040}), "the port chosen for the PostgreSQL MCP server is already used by StarRocks: choose another one"),
    (lambda: sdp.do_db_setup_mcp(postgres={"port": 9100}, starrocks={"port": 9100}), "the port chosen for the StarRocks MCP server is already used by the PostgreSQL MCP server: choose another one"),
    (lambda: sdp.do_db_setup_jobs(port=8000, result_root="./r", max_concurrent=1, history_retention=1, postgres_target={"name": "p", "database": "d", "timeout_hours": 0}, auth_enabled=True), "the port chosen for the query scheduler is already used by the PostgreSQL MCP server: choose another one"),
    (lambda: sdp.do_db_setup_jobs(port=8443, result_root="./r", max_concurrent=1, history_retention=1, postgres_target={"name": "p", "database": "d", "timeout_hours": 0}, auth_enabled=True), "the port chosen for the query scheduler is already used by the web UI: choose another one"),
    (lambda: sdp.do_db_setup_jobs(port=8050, result_root="./r", max_concurrent=1, history_retention=1, postgres_target={"name": "p", "database": "d", "timeout_hours": 0}), sdp._JOBS_AUTH_OFF_REFUSED),
    *[(lambda bad=bad: sdp.do_db_setup_jobs(port=8050, result_root=bad, max_concurrent=1, history_retention=1, postgres_target={"name": "p", "database": "d", "timeout_hours": 0}, auth_enabled=True), sdp._RESULT_ROOT_REFUSED) for bad in ("-x", "a:b", "$HOME/r", "r#1", " r", "r'", "a\\b")],
    *[(lambda bad=bad: sdp.do_db_setup_jobs(port=8050, result_root=bad, max_concurrent=1, history_retention=1, postgres_target={"name": "p", "database": "d", "timeout_hours": 0}, auth_enabled=True), sdp._RESULT_ROOT_HOLDS_JOBS) for bad in (".", "data", "data/jobs", "data/jobs/control/x", "/")],
])
def test_the_browsers_refusals_write_nothing(tmp_path, monkeypatch, capsys, call, message):
    """Bug class: a setup from the browser that the terminal path would have let through to a broken deployment: an MCP server or a target of a database that is not set up (it fails at the next start), a port that another service already publishes (database, StarRocks's fixed backend port, an MCP server chosen in the same request, the web UI's own), the scheduler page's login switched off from the browser, a results folder that breaks .env or the compose mount, or one that holds the scheduler's data folder (its control socket then stays closed, and the deployment folder or the whole disk would be mounted writable into the databases). Each is refused before anything is written."""
    _no_docker(monkeypatch)
    root = tmp_path / "p"
    _seed(root, databases=("postgres", "starrocks"))
    (root / "config" / "db" / "mcp.yaml").write_text("postgres:\n  enabled: true\n  port: 8000\n")
    (root / "config" / "jobs").mkdir()
    (root / "config" / "jobs" / "config.local.yaml").write_text("auth: true\ntargets:\n  p:\n    backend: postgres\n    database: d\n")
    _point_at(monkeypatch, root)
    monkeypatch.setattr(sdp, "_webui_host", lambda: type("W", (), {"read_config": staticmethod(lambda root: (type("C", (), {"port": 8443})(), {}))}))
    before = _snapshot(root)
    with pytest.raises(sdp.ActionError) as refused:
        call()
    assert refused.value.message == message
    assert _snapshot(root) == before


def test_a_service_keeps_its_own_port_and_a_switched_off_server_frees_its_port(tmp_path, monkeypatch, capsys):
    """Bug class: a port check that refuses the port a service already has (every re-run of the setup would be refused), or that counts a server switched off in the same request."""
    _no_docker(monkeypatch)
    root = tmp_path / "p"
    _seed(root, databases=("postgres", "mongo"))
    (root / "config" / "db" / "mcp.yaml").write_text("postgres:\n  enabled: true\n  port: 8000\nmongo:\n  enabled: true\n  port: 3000\n")
    (root / ".env").write_text("POSTGRES_MCP_PORT=8000\nMONGO_MCP_PORT=3000\n")
    _point_at(monkeypatch, root)
    assert sdp.do_db_setup_mcp(postgres={"port": 8000}, mongo={"port": 3000}) == 0
    assert sdp.do_db_setup_mcp(postgres={"port": 3000}) == 0
    (root / "config" / "jobs").mkdir()
    (root / "config" / "jobs" / "config.local.yaml").write_text("port: 8050\n")
    (root / ".env").write_text("POSTGRES_MCP_PORT=3000\nJOBS_PORT=8050\n")
    assert sdp.do_db_setup_jobs(port=8050, result_root="./r", max_concurrent=1, history_retention=1, postgres_target={"name": "p", "database": "d", "timeout_hours": 0}) == 0


# ── the typed inputs ────────────────────────────────────────────────────────


JOBS_MINIMAL = {"port": 8050, "result_root": "./r", "max_concurrent": 1, "history_retention": 1}


@pytest.mark.parametrize("verb, fields, field", [
    ("db.setup_mcp", {"postgres": {"port": 80}}, "postgres"),
    ("db.setup_mcp", {"postgres": {"port": "8000"}}, "postgres"),
    ("db.setup_mcp", {"postgres": {"write_access": True}}, "postgres"),
    ("db.setup_mcp", {"starrocks": {"port": 9000, "write_access": True}}, "starrocks"),
    ("db.setup_jobs", {**JOBS_MINIMAL, "port": 70000}, "port"),
    ("db.setup_jobs", {**JOBS_MINIMAL, "result_root": "r\nJOBS_PORT=1"}, "result_root"),
    ("db.setup_jobs", {**JOBS_MINIMAL, "max_concurrent": 0}, "max_concurrent"),
    ("db.setup_jobs", {**JOBS_MINIMAL, "starrocks_target": {"name": "s", "timeout_hours": 73}}, "starrocks_target"),
    ("db.setup_jobs", {**JOBS_MINIMAL, "starrocks_target": {"name": "s", "timeout_hours": 0}}, "starrocks_target"),
    ("db.setup_jobs", {**JOBS_MINIMAL, "postgres_target": {"name": "p", "timeout_hours": 1}}, "postgres_target"),
    ("db.setup_jobs", {**JOBS_MINIMAL, "mongo_target": {"name": "../x", "timeout_hours": 1}}, "mongo_target"),
    ("db.setup_jobs", {**JOBS_MINIMAL, "mongo_target": {"name": "m", "timeout_hours": 1, "database": "d"}}, "mongo_target"),
    ("db.setup_jobs", {**JOBS_MINIMAL, "postgres_target": {"name": "same", "database": "d", "timeout_hours": 0}, "mongo_target": {"name": "same", "timeout_hours": 0}}, "mongo_target"),
])
def test_the_inputs_refuse_what_the_terminal_would_never_write(verb, fields, field):
    """Bug class: a typed input that lets through a value no answer to the terminal's questions produces, or one that breaks a file: a port below 1024 or above 65535, a number as text, a server without its port, a write access StarRocks's MCP server has no question for, a line break in the results folder (a second .env line), StarRocks's timeout outside its 1 to 72 hours, a PostgreSQL target without its database, a target name that is a path, two targets of one name (one would vanish from the map)."""
    with pytest.raises(errors.InputError) as refused:
        verbs.validate_request(verb, fields)
    assert refused.value.field == field


# ── db restart-needed ───────────────────────────────────────────────────────


def _docker_listing(monkeypatch, lines, calls):
    def run(argv, **kwargs):
        calls.append(list(argv))
        if list(argv[:3]) == ["docker", "compose", "ps"]:
            return subprocess.CompletedProcess(argv, 0, stdout="0123456789ab\nfedcba987654\n", stderr="")
        if list(argv[:2]) == ["docker", "inspect"]:
            return subprocess.CompletedProcess(argv, 0, stdout="".join(line + "\n" for line in lines), stderr="")
        raise AssertionError(f"unexpected subprocess: {argv}")

    monkeypatch.setattr(sdp.subprocess, "run", run)


def test_restart_needed_names_the_services_whose_files_are_newer(tmp_path, monkeypatch):
    """Bug class: a restart notice that names a service whose settings did not change after it started, misses one whose did (the scheduler after its local config was written, PostgreSQL after the compose override), names a service that does not run, or reads a start time in another time zone."""
    root = tmp_path / "p"
    _seed(root)
    (root / "config" / "jobs").mkdir()
    (root / "config" / "jobs" / "config.local.yaml").write_text("port: 8050\n")
    started = 1_800_000_000
    for relative, when in ((".env", started - 100), ("docker-compose.override.yml", started + 5), ("config/jobs/config.local.yaml", started + 1), ("config/starrocks/fe.local.conf", started - 100)):
        os.utime(root / relative, (when, when))
    _point_at(monkeypatch, root)
    calls = []
    _docker_listing(monkeypatch, ["postgres 2027-01-15T08:00:00.123456789Z", "jobs 2027-01-15T08:00:00Z", "mongo 2027-01-15T08:00:00Z"], calls)

    data = sdp.get_db_restart_needed()

    assert data == {"docker_available": True, "services": [
        {"service": "postgres", "started": "2027-01-15T08:00:00Z", "files": [{"file": "docker-compose.override.yml", "modified": "2027-01-15T08:00:05Z"}]},
        {"service": "jobs", "started": "2027-01-15T08:00:00Z", "files": [{"file": "config/jobs/config.local.yaml", "modified": "2027-01-15T08:00:01Z"}]},
    ]}
    assert calls[1][:3] == ["docker", "inspect", "--format"] and calls[1][3] == sdp._STARTED_FORMAT


def test_restart_needed_takes_nothing_but_its_own_lines_from_docker(tmp_path, monkeypatch):
    """Bug class: a reader of `docker inspect` that passes on what it did not ask for (a whole answer holds the containers' environment, the admin password in it), or that fails on it; and Docker that cannot be asked shown as "nothing to restart"."""
    root = tmp_path / "p"
    _seed(root)
    _point_at(monkeypatch, root)
    _docker_listing(monkeypatch, ['[{"Config": {"Env": ["POSTGRES_PASSWORD=made-up-admin-value"]}}]', "postgres 2027-01-15T08:00:00Z extra"], [])
    assert sdp.get_db_restart_needed() == {"docker_available": True, "services": []}
    _no_docker(monkeypatch)
    assert sdp.get_db_restart_needed() == {"docker_available": False, "services": []}


# ── db llm-settings ─────────────────────────────────────────────────────────


def test_llm_settings_offers_the_questionnaires_defaults(tmp_path, monkeypatch, capsys):
    """Bug class: a browser form filled with other values than the terminal offers as defaults (a port of the compose file instead of the one written, a timeout in seconds, StarRocks's timeout outside 1 to 72, a database name not taken from the target or DB_NAME), so that confirming an unchanged form changes the setup."""
    root = tmp_path / "p"
    _seed(root)
    (root / "config" / "db" / "mcp.yaml").write_text("postgres:\n  enabled: true\n  port: 8100\n  access_mode: unrestricted\nmongo:\n  enabled: false\n  port: 3100\n  read_only: true\n")
    (root / "config" / "jobs").mkdir()
    (root / "config" / "jobs" / "config.local.yaml").write_text("port: 8060\nresult_root: /srv/r\nmax_concurrent: 3\nauth: true\ndefault_timeouts:\n  postgres: 7200\n  starrocks: 999999\ntargets:\n  warehouse:\n    backend: postgres\n    database: big\n  sr1:\n    backend: starrocks\n")
    _point_at(monkeypatch, root)

    data = sdp.get_db_llm_settings()
    mcp_asked = {}
    monkeypatch.setattr(mcp_setup, "ask_bool", lambda prompt, default=True, tag=None: mcp_asked.setdefault(tag, default))
    monkeypatch.setattr(mcp_setup, "ask_int", lambda prompt, default=None, tag=None: mcp_asked.setdefault(tag, default))
    mcp_setup.run_questionnaire(utils.load_db_setup())
    jobs_defaults = {}
    monkeypatch.setattr(jobs_setup, "ask", lambda prompt, default=None, tag=None: str(jobs_defaults.setdefault(tag, default)))
    monkeypatch.setattr(jobs_setup, "ask_int", lambda prompt, default=None, tag=None: int(jobs_defaults.setdefault(tag, default)))
    monkeypatch.setattr(jobs_setup, "ask_bool", lambda prompt, default=True, tag=None: jobs_defaults.setdefault(tag, default))
    jobs_setup.run_questionnaire(utils.load_db_setup())
    capsys.readouterr()

    servers = data["mcp"]["servers"]
    assert (servers["postgres"]["enabled"], servers["postgres"]["port"], servers["postgres"]["write_access"]) == (mcp_asked["mcp_pg_enable"], mcp_asked["mcp_pg_port"], mcp_asked["mcp_pg_write_access"])
    assert (servers["mongo"]["enabled"], servers["mongo"]["port"]) == (mcp_asked["mcp_mongo_enable"], 3100)
    assert (servers["starrocks"]["enabled"], servers["starrocks"]["port"]) == (mcp_asked["mcp_sr_enable"], 9000)
    jobs = data["jobs"]
    targets = jobs["targets"]
    assert (jobs["port"], jobs["result_root"], jobs["max_concurrent"], jobs["history_retention"], jobs["auth_enabled"]) == (
        jobs_defaults["jobs_port"], jobs_defaults["jobs_result_root"], jobs_defaults["jobs_max_concurrent"], jobs_defaults["jobs_history_retention"], jobs_defaults["jobs_ui_auth_enable"])
    assert (targets["postgres"]["name"], targets["postgres"]["database"], targets["postgres"]["timeout_hours"]) == (jobs_defaults["jobs_pg_target_name"], jobs_defaults["jobs_pg_target_db"], jobs_defaults["jobs_pg_timeout_hours"])
    assert (targets["starrocks"]["name"], targets["starrocks"]["timeout_hours"]) == (jobs_defaults["jobs_sr_target_name"], jobs_defaults["jobs_sr_timeout_hours"])
    assert (targets["mongo"]["name"], targets["mongo"]["timeout_hours"]) == (jobs_defaults["jobs_mg_target_name"], jobs_defaults["jobs_mg_timeout_hours"])
    assert (targets["postgres"]["database"], targets["starrocks"]["timeout_hours"], jobs["port"]) == ("big", 72, 8060)
