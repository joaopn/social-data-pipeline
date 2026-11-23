"""Tests for `do_db_setup`, the action of the verb `db.setup` (`sdp db setup` with its answers given all at once, `setup/db_answers.py`).

The contract pinned here: for the same answers, the action writes exactly the files the terminal's questionnaire writes (`db.main` the first time, `db.add_database` for one database added), byte for byte, the `.bak` copies and the made folders included; the read-only password files are compared by their shape (one line, mode 0600) or with `cmp`-like equality, never printed. Every refusal comes before anything is written.

Each test builds two throwaway deployments with the repository's base `.conf` files: the terminal's questionnaire runs in one with its prompts answered by tag (the real `ask*` helpers, fed through `input`), the action in the other with the request validated by the registry as the broker's child validates it. Passwords are made-up values.
"""

from __future__ import annotations

import builtins
import os
import shutil
import stat
from pathlib import Path

import pytest

import sdp
from social_data_pipeline.control import types, verbs
from social_data_pipeline.setup import db, db_answers, utils

REPO = Path(__file__).resolve().parents[2]
ADMIN = "made-up-admin-pw-5d2a"
RO = "made-up-ro-pw-91c0"
RAM, CORES = 31.3, 12


def _project(root, env=None):
    for part in ("postgres/postgresql.conf", "starrocks/fe.conf", "starrocks/be.conf"):
        target = root / "config" / part
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(REPO / "config" / part, target)
    (root / "config" / "db").mkdir(parents=True, exist_ok=True)
    if env is not None:
        (root / ".env").write_text(env)


def _point(monkeypatch, root):
    for module in (sdp, utils, db):
        monkeypatch.setattr(module, "ROOT", root)
        monkeypatch.setattr(module, "CONFIG_DIR", root / "config")
    monkeypatch.chdir(root)
    monkeypatch.setattr(utils, "detect_ram_gb", lambda: RAM)
    monkeypatch.setattr(utils, "detect_cpu_cores", lambda: CORES)
    monkeypatch.setattr(db, "detect_hardware", lambda: {"cpu_cores": CORES, "ram_gb": RAM, "gpus": []})


def _terminal(monkeypatch, root, typed, add=None):
    """Run the questionnaire in `root` with `typed` answers by tag (a text, or a list of texts for a question asked in a loop; a missing tag is Enter)."""
    _point(monkeypatch, root)
    answers = {tag: list(value) if isinstance(value, list) else value for tag, value in typed.items()}
    current = {"tag": None}

    def answer(tag):
        value = answers.get(tag, "")
        return value.pop(0) if isinstance(value, list) else value

    monkeypatch.setattr(builtins, "input", lambda prompt="": answer(current["tag"]))
    for name in ("ask", "ask_int", "ask_bool", "ask_choice", "ask_multi_select"):
        real = getattr(utils, name)

        def tagged(*args, _real=real, tag=None, **kwargs):
            current["tag"] = tag
            return _real(*args, tag=tag, **kwargs)

        monkeypatch.setattr(db, name, tagged)
    # The paste ends at the first blank line in the terminal; these answers have none.
    monkeypatch.setattr(db, "ask_multi_line", lambda prompt, tag=None: "\n".join(line for line in answer(tag).split("\n") if line.strip()))
    monkeypatch.setattr(db, "ask_password", lambda label, tag=None: answer(tag))
    if add:
        db.add_database(add)
    else:
        db.main()


def _action(monkeypatch, root, fields):
    """Run the action in `root` with `fields` validated by the registry, the secrets revealed (what the broker's child does)."""
    _point(monkeypatch, root)
    validated = verbs.validate_request("db.setup", fields)
    return sdp.do_db_setup(**types.reveal(validated))


def _tree(root):
    """Every file under root but the read-only password files: path to bytes; and every folder."""
    files, folders = {}, set()
    for path in sorted(root.rglob("*")):
        relative = str(path.relative_to(root))
        if path.is_dir():
            folders.add(relative)
        elif path.name != ".ro_credentials":
            files[relative] = path.read_bytes()
    return files, folders


def _credentials(root):
    return {str(path.relative_to(root)): path for path in root.rglob(".ro_credentials")}


def _shape(path):
    """The shape of a read-only password file: its mode, its line count, its length."""
    data = path.read_bytes()
    return stat.S_IMODE(path.stat().st_mode), data.count(b"\n"), len(data)


ENV = "UNRELATED=keep\nJOBS_PORT=8050\n"

FULL_TYPED = {
    "db_data_path": "./data", "db_databases": "postgres,mongo,starrocks", "db_mongo_data_path": "./elsewhere/mongo",
    "db_name": "corpus", "db_pg_port": "5433", "db_tablespaces": "y", "db_ts_name": ["fast1", "slow2"], "db_ts_path": ["/mnt/nvme1/pg", "./disks/slow"], "db_ts_more": ["y", "n"],
    "db_filesystem": "zfs", "db_pgtune_method": "paste", "db_pgtune_paste": "max_connections = 40\n  shared_buffers = 8GB",
    "db_mongo_cache": "4", "db_mongo_mem_limit": "0", "db_mongo_validate": "tail",
    "db_sr_port": "9031", "db_sr_fe_heap": "6", "db_sr_alter_workers": "8", "db_sr_compression": "LZ4", "db_sr_multidisk": "y", "db_sr_storage_path": ["/mnt/a/sr", "/mnt/b/sr"], "db_sr_more_paths": ["y", "n"],
    "db_auth": "y", "db_password": ADMIN, "db_ro_user": "y", "db_ro_username": "reader", "db_ro_auto_password": "n", "db_ro_password": RO,
}
FULL_FIELDS = {
    "data_path": "./data", "databases": ["postgres", "mongo", "starrocks"], "mongo_data_path": "./elsewhere/mongo",
    "db_name": "corpus", "pg_port": 5433, "tablespaces": [{"name": "fast1", "path": "/mnt/nvme1/pg"}, {"name": "slow2", "path": "./disks/slow"}],
    "filesystem": "zfs", "pgtune": "max_connections = 40\n\n  shared_buffers = 8GB\n",
    "mongo_cache_size_gb": 4, "mongo_mem_limit": 0, "mongo_validate": "tail",
    "starrocks_port": 9031, "sr_fe_jvm_heap": 6, "sr_alter_tablet_workers": 8, "sr_compression": "LZ4", "starrocks_storage_paths": ["/mnt/a/sr", "/mnt/b/sr"],
    "auth": True, "new_password": ADMIN, "new_password_confirm": ADMIN, "ro_user": True, "ro_username": "reader", "ro_password_mode": "typed", "ro_password": RO, "ro_password_confirm": RO,
}


def test_first_time_answers_write_what_the_terminal_writes(tmp_path, monkeypatch, capsys):
    """Bug class: the browser's setup writing other files or keys than the terminal for the same answers (a default computed otherwise, a heap written as a number where the terminal writes text, a relative path written without its `./`, a key left out of .env, an override line in another order), or a password in the output. Every question is answered, some typed and some by Enter."""
    terminal, browser = tmp_path / "terminal", tmp_path / "browser"
    _project(terminal, ENV)
    _project(browser, ENV)
    _terminal(monkeypatch, terminal, FULL_TYPED)
    capsys.readouterr()
    assert _action(monkeypatch, browser, FULL_FIELDS) == 0
    out = capsys.readouterr()

    assert _tree(browser) == _tree(terminal)
    assert ADMIN not in out.out + out.err and RO not in out.out + out.err
    written, expected = _credentials(browser), _credentials(terminal)
    assert sorted(written) == sorted(expected) == ["data/database/postgres/.ro_credentials", "data/database/starrocks/.ro_credentials", "elsewhere/mongo/.ro_credentials"]
    for name, path in written.items():
        assert _shape(path) == _shape(expected[name]) == (0o600, 1, len(RO) + 1)
        assert path.read_bytes() == expected[name].read_bytes()


def test_first_time_with_every_default_writes_what_the_terminal_writes(tmp_path, monkeypatch):
    """Bug class: an input left out taking another value than the terminal's Enter (the machine-dependent memory sizes, the alter workers, the paths derived from the data base path)."""
    terminal, browser = tmp_path / "terminal", tmp_path / "browser"
    _project(terminal, ENV)
    _project(browser, ENV)
    _terminal(monkeypatch, terminal, {"db_databases": "starrocks,mongo"})
    assert _action(monkeypatch, browser, {"databases": ["mongo", "starrocks"]}) == 0

    assert _tree(browser) == _tree(terminal)
    assert _credentials(browser) == {}


def test_a_generated_read_only_password_has_the_terminals_shape(tmp_path, monkeypatch, capsys):
    """Bug class: a generated password of another form than the terminal's `token_urlsafe(24)` (32 characters, one line, 0600), or one written into the output."""
    terminal, browser = tmp_path / "terminal", tmp_path / "browser"
    _project(terminal)
    _project(browser)
    _terminal(monkeypatch, terminal, {"db_databases": "mongo", "db_auth": "y", "db_password": ADMIN, "db_ro_user": "y"})
    capsys.readouterr()
    assert _action(monkeypatch, browser, {"databases": ["mongo"], "auth": True, "new_password": ADMIN, "new_password_confirm": ADMIN, "ro_user": True, "ro_password_mode": "generate"}) == 0
    out = capsys.readouterr().out

    assert _tree(browser) == _tree(terminal)
    made = _credentials(browser)["data/database/mongo/.ro_credentials"]
    assert _shape(made) == _shape(_credentials(terminal)["data/database/mongo/.ro_credentials"]) == (0o600, 1, 33)
    assert made.read_text().strip() not in out


ADD_ENV = "DATA_PATH=./data\nMONGO_DATA_PATH=./data/database/mongo\nMONGO_PORT=27017\nMONGO_CACHE_SIZE_GB=2\nMONGO_AUTH_ENABLED=true\nMONGO_ADMIN_USER=admin\nMONGO_RO_USER=reader\nUNRELATED=keep\n"


def _mongo_set_up(root, auth=True):
    (root / "config" / "db" / "mongo.yaml").write_text("port: 27017\ncache_size_gb: 2\nvalidate_before_import: full\n" + ("auth: true\nro_username: reader\n" if auth else ""))
    if auth:
        folder = root / "data" / "database" / "mongo"
        folder.mkdir(parents=True)
        (folder / ".ro_credentials").write_text(RO + "\n")
        os.chmod(folder / ".ro_credentials", 0o600)


def test_adding_a_database_to_a_setup_with_authentication_writes_what_the_terminal_writes(tmp_path, monkeypatch):
    """Bug class: a database added from the browser without the setup's authentication (the terminal gives it the same read-only user and password), its .env keys rewritten instead of merged, or another database's files touched."""
    terminal, browser = tmp_path / "terminal", tmp_path / "browser"
    for root in (terminal, browser):
        _project(root, ADD_ENV)
        _mongo_set_up(root)
    _terminal(monkeypatch, terminal, {"db_sr_multidisk": "y", "db_sr_storage_path": ["./disks/sr1"], "db_sr_more_paths": ["n"]}, add="starrocks")
    assert _action(monkeypatch, browser, {"add": "starrocks", "starrocks_storage_paths": ["disks/sr1"]}) == 0

    assert _tree(browser) == _tree(terminal)
    assert (browser / "data/database/starrocks/.ro_credentials").read_bytes() == (browser / "data/database/mongo/.ro_credentials").read_bytes()
    assert _shape(browser / "data/database/starrocks/.ro_credentials")[0] == 0o600


def test_adding_a_database_with_authentication_switched_on_writes_what_the_terminal_writes(tmp_path, monkeypatch):
    """Bug class: the add path's own authentication questions (only asked while it is off) answered otherwise than the terminal: the read-only user's default name, the keys of PostgreSQL, the tablespaces' override block."""
    terminal, browser = tmp_path / "terminal", tmp_path / "browser"
    env = "DATA_PATH=./data\nMONGO_DATA_PATH=./data/database/mongo\nMONGO_PORT=27017\n"
    for root in (terminal, browser):
        _project(root, env)
        _mongo_set_up(root, auth=False)
    typed = {"db_tablespaces": "y", "db_ts_name": ["fast1"], "db_ts_path": ["/mnt/fast"], "db_ts_more": ["n"], "db_auth": "y", "db_password": ADMIN, "db_ro_user": "y", "db_ro_auto_password": "n", "db_ro_password": RO}
    _terminal(monkeypatch, terminal, typed, add="postgres")
    fields = {"add": "postgres", "tablespaces": [{"name": "fast1", "path": "/mnt/fast"}], "auth": True, "new_password": ADMIN, "new_password_confirm": ADMIN, "ro_user": True, "ro_password_mode": "typed", "ro_password": RO, "ro_password_confirm": RO}
    assert _action(monkeypatch, browser, fields) == 0

    assert _tree(browser) == _tree(terminal)
    assert (browser / "data/database/postgres/.ro_credentials").read_bytes() == (terminal / "data/database/postgres/.ro_credentials").read_bytes()


def _refused(monkeypatch, root, fields):
    before = _tree(root)
    with pytest.raises(sdp.ActionError) as refused:
        _action(monkeypatch, root, fields)
    assert _tree(root) == before
    return refused.value.message


def test_a_port_another_service_uses_is_refused_before_anything_is_written(tmp_path, monkeypatch):
    """Bug class: a database set up on the port of the scheduler, an MCP server or the web UI (or two of its own services on one port), which only fails at its start; the terminal does not check, the browser refuses (decided). The default port counts too."""
    _project(tmp_path, "JOBS_PORT=9030\nMONGO_MCP_PORT=3001\n")
    assert "used by the query scheduler" in _refused(monkeypatch, tmp_path, {"databases": ["starrocks"]})
    assert "used by the MongoDB MCP server" in _refused(monkeypatch, tmp_path, {"databases": ["mongo"], "mongo_port": 3001})
    assert "two services" in _refused(monkeypatch, tmp_path, {"databases": ["postgres", "mongo"], "pg_port": 27017})


def test_the_ports_in_use_are_read_from_env_and_the_web_ui(monkeypatch):
    """Bug class: a port of another SDP service missed by the setups' one reading of the ports in use (an MCP server or the scheduler named in .env only, the web UI, a configured database's compose default, StarRocks's backend 8040), a value that is no port counted, or a database's own old port counted against itself when it is the one set up."""
    monkeypatch.setattr(sdp, "load_env", lambda: {"POSTGRES_PORT": "5432", "MONGO_MCP_PORT": "3000", "JOBS_PORT": "8050", "STARROCKS_PORT": "x", "UNRELATED_PORT": "1"})
    monkeypatch.setattr(sdp, "_get_configured_db_services", lambda: ["postgres", "starrocks"])
    monkeypatch.setattr(sdp, "_get_configured_mcp_services", lambda: [])
    monkeypatch.setattr(sdp, "_is_jobs_configured", lambda: False)
    monkeypatch.setattr(sdp, "_webui_host", lambda: type("W", (), {"read_config": staticmethod(lambda root: (type("C", (), {"port": 8443})(), {}))}))

    assert sdp._ports_in_use(leave_out={"postgres"}) == {3000: "mongo-mcp", 8050: "jobs", 8443: "webui", 8030: "starrocks", 8040: "starrocks"}
    assert sdp._ports_in_use()[5432] == "postgres"


def test_only_a_database_not_set_up_yet_is_set_up(tmp_path, monkeypatch):
    """Bug class: an add over a database that is set up (a change goes through `db setup` without add), or a run over the setup that leaves a database set up out of it (`test_db_reconfigure_action.py` has the change itself)."""
    _project(tmp_path, ADD_ENV)
    _mongo_set_up(tmp_path)
    assert "set up already" in _refused(monkeypatch, tmp_path, {"databases": ["starrocks"]})
    assert "set up already" in _refused(monkeypatch, tmp_path, {"add": "mongo"})
    assert "leave the authentication answers out" in _refused(monkeypatch, tmp_path, {"add": "postgres", "auth": True, "new_password": ADMIN, "new_password_confirm": ADMIN})


def test_an_add_without_a_setup_is_refused(tmp_path, monkeypatch):
    """Bug class: `--add` run where nothing is set up, which the terminal stops with "No existing setup found"."""
    _project(tmp_path)
    assert "No existing setup found" in _refused(monkeypatch, tmp_path, {"add": "postgres"})


def test_the_pasted_pgtune_output_keeps_what_the_terminals_paste_keeps():
    """Bug class: blank or white-space lines of a pasted PGTune output written into postgresql.local.conf (the terminal's paste drops them), or the indentation of a line lost."""
    assert db_answers.pgtune_lines("# DB Version: 18\n\n  \t\nmax_connections = 40\n  work_mem = 4MB\n") == "# DB Version: 18\nmax_connections = 40\n  work_mem = 4MB"
    assert db_answers.pgtune_lines(None) == ""
