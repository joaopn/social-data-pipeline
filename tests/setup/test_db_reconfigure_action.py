"""Tests for `do_db_setup` over databases set up already (`sdp db setup` run again, its "Change…" in the web UI) and for its read, `get_db_settings` (`sdp db settings`).

The contract pinned here, as for the first time (`test_db_setup_action.py`, whose harness this module uses): for the same answers the action writes exactly what the terminal's questionnaire writes when it runs over the setup, with the current values as its defaults and its "Keep existing ...?" questions answered, byte for byte. Each test first sets up both throwaway deployments with the terminal's questionnaire and the same answers, then changes one with the terminal and the other with the action. The read-only password files are compared with each other, never printed. Authentication switched off is refused before anything is written (decided: the browser can only switch it on). The read returns the current answers and never a password.

Passwords are made-up values.
"""

from __future__ import annotations

import json

import pytest
from test_db_setup_action import ADMIN, ENV, RO, _action, _credentials, _project, _shape, _terminal, _tree

import sdp

NEW_ADMIN = "made-up-admin-pw-e4a1"

# The first setup, made with the terminal in both deployments: every database, tablespaces, ZFS, a PGTune output, extra StarRocks disks, authentication with a typed read-only password.
BASE_TYPED = {
    "db_databases": "postgres,mongo,starrocks", "db_tablespaces": "y", "db_ts_name": ["fast1"], "db_ts_path": ["/mnt/nvme1/pg"], "db_ts_more": ["n"],
    "db_filesystem": "zfs", "db_pgtune_method": "paste", "db_pgtune_paste": "max_connections = 40\nshared_buffers = 8GB",
    "db_sr_multidisk": "y", "db_sr_storage_path": ["/mnt/a/sr"], "db_sr_more_paths": ["n"],
    "db_auth": "y", "db_password": ADMIN, "db_ro_user": "y", "db_ro_username": "reader", "db_ro_auto_password": "n", "db_ro_password": RO,
}
# The same without authentication.
BASE_OPEN = {"db_databases": "mongo,starrocks"}


def _both(tmp_path, monkeypatch, base):
    terminal, browser = tmp_path / "terminal", tmp_path / "browser"
    for root in (terminal, browser):
        _project(root, ENV)
        _terminal(monkeypatch, root, base)
    assert _tree(terminal) == _tree(browser)
    return terminal, browser


def _same(terminal, browser):
    assert _tree(browser) == _tree(terminal)
    written, expected = _credentials(browser), _credentials(terminal)
    assert sorted(written) == sorted(expected)
    for name, path in written.items():
        assert _shape(path) == _shape(expected[name])
        assert path.read_bytes() == expected[name].read_bytes()


AUTH_AGAIN = {"auth": True, "new_password": NEW_ADMIN, "new_password_confirm": NEW_ADMIN}


def test_a_port_and_memory_changed_with_everything_else_kept_writes_what_the_terminal_writes(tmp_path, monkeypatch, capsys):
    """Bug class: a change from the browser that rewrites more than the terminal's run over the setup for the same answers: a kept value taken from elsewhere than the current one, the tablespaces, the extra disks or the PGTune output lost or rewritten in another form, a new read-only password where the terminal keeps the old one (which breaks every client that has it), or a password in the output."""
    terminal, browser = _both(tmp_path, monkeypatch, BASE_TYPED)
    # The terminal over the setup: Enter on every question but the two changed, the filesystem (whose default is always "standard") and the admin password (asked again).
    _terminal(monkeypatch, terminal, {"db_mongo_port": "27018", "db_sr_mem_limit": "20", "db_pg_mem_limit": "12", "db_filesystem": "zfs", "db_password": NEW_ADMIN})
    capsys.readouterr()
    fields = {"databases": ["postgres", "mongo", "starrocks"], "mongo_port": 27018, "starrocks_mem_limit": 20, "pg_mem_limit": 12, "filesystem": "zfs",
              "keep_tablespaces": True, "keep_pgtune": True, "keep_storage_paths": True, **AUTH_AGAIN, "ro_user": True, "ro_password_mode": "keep"}
    assert _action(monkeypatch, browser, fields) == 0
    out = capsys.readouterr()

    _same(terminal, browser)
    assert (browser / "data/database/mongo/.ro_credentials").read_bytes() == (RO + "\n").encode()
    assert RO not in out.out + out.err and NEW_ADMIN not in out.out + out.err
    assert "restarted" in out.out


def test_a_change_with_the_keep_answers_left_out_is_the_terminals_enter(tmp_path, monkeypatch):
    """Bug class: an answer left out taken otherwise than the terminal's Enter over a setup: the read-only password (Enter keeps it), the user name, the ports and sizes (the current ones). The yes/no questions left out are no, as for the first time, so the tablespaces, extra disks and PGTune output are given up, as the terminal gives them up when told no."""
    terminal, browser = _both(tmp_path, monkeypatch, BASE_TYPED)
    _terminal(monkeypatch, terminal, {"db_tablespaces": "n", "db_pgtune_method": "skip", "db_sr_multidisk": "n", "db_password": NEW_ADMIN})
    assert _action(monkeypatch, browser, {"databases": ["postgres", "mongo", "starrocks"], **AUTH_AGAIN, "ro_user": True}) == 0
    _same(terminal, browser)


def test_a_data_path_changed_writes_what_the_terminal_writes_and_moves_nothing(tmp_path, monkeypatch, capsys):
    """Bug class: a data path changed from the browser that is written otherwise than by the terminal, that moves or deletes the old data (the decision is that it is not moved), or a read-only password that does not reach the new folder. The new tablespaces replace the old ones, as the terminal's "Keep existing tablespaces?" no does."""
    terminal, browser = _both(tmp_path, monkeypatch, BASE_TYPED)
    for root in (terminal, browser):
        (root / "data/database/postgres/pgdata").mkdir(parents=True)
        (root / "data/database/postgres/pgdata/PG_VERSION").write_text("18\n")
    typed = {"db_pgdata_path": "./elsewhere/pg-new", "db_ts_keep": "n", "db_ts_name": ["slow2"], "db_ts_path": ["./disks/slow"], "db_ts_more": ["n"], "db_filesystem": "zfs", "db_password": NEW_ADMIN}
    _terminal(monkeypatch, terminal, typed)
    capsys.readouterr()
    fields = {"databases": ["postgres", "mongo", "starrocks"], "pgdata_path": "elsewhere/pg-new", "tablespaces": [{"name": "slow2", "path": "disks/slow"}], "filesystem": "zfs",
              "keep_pgtune": True, "keep_storage_paths": True, **AUTH_AGAIN, "ro_user": True, "ro_password_mode": "keep"}
    assert _action(monkeypatch, browser, fields) == 0
    out = capsys.readouterr().out

    _same(terminal, browser)
    assert (browser / "data/database/postgres/pgdata/PG_VERSION").read_text() == "18\n"
    assert "no data was moved, so postgres starts empty there; its old data stays in ./data/database/postgres" in out


def test_authentication_switched_on_writes_what_the_terminal_writes(tmp_path, monkeypatch):
    """Bug class: authentication switched on from the browser over databases set up without it, written otherwise than by the terminal: the auth keys of .env, the read-only user's name and files, pg_hba.local.conf."""
    terminal, browser = _both(tmp_path, monkeypatch, BASE_OPEN)
    _terminal(monkeypatch, terminal, {"db_auth": "y", "db_password": NEW_ADMIN, "db_ro_user": "y", "db_ro_auto_password": "n", "db_ro_password": RO})
    fields = {"databases": ["mongo", "starrocks"], **AUTH_AGAIN, "ro_user": True, "ro_password_mode": "typed", "ro_password": RO, "ro_password_confirm": RO}
    assert _action(monkeypatch, browser, fields) == 0
    _same(terminal, browser)
    assert _shape(browser / "data/database/starrocks/.ro_credentials") == (0o600, 1, len(RO) + 1)


def _refused(monkeypatch, root, fields):
    before = _tree(root), {name: path.read_bytes() for name, path in _credentials(root).items()}
    with pytest.raises(sdp.ActionError) as refused:
        _action(monkeypatch, root, fields)
    assert (_tree(root), {name: path.read_bytes() for name, path in _credentials(root).items()}) == before
    return refused.value.message


def test_authentication_switched_off_is_refused_before_anything_is_written(tmp_path, monkeypatch):
    """Bug class (security): a change from the browser that switches authentication off for databases that have it, which would open them to every client without a password; decided: switching it off is done in a terminal. Left out is off too. Nothing may be written before the refusal."""
    _project(tmp_path, ENV)
    _terminal(monkeypatch, tmp_path, BASE_TYPED)
    for fields in ({"databases": ["postgres", "mongo", "starrocks"], "auth": False}, {"databases": ["postgres", "mongo", "starrocks"], "mongo_port": 27018}):
        message = _refused(monkeypatch, tmp_path, fields)
        assert message.startswith("db setup: auth cannot be switched off here") and "python sdp.py db setup" in message


def test_a_change_that_leaves_out_a_database_or_keeps_nothing_is_refused(tmp_path, monkeypatch):
    """Bug class: a run over the setup that leaves a database out (the terminal would then keep its old keys and files beside the new ones), or a keep answer with nothing to keep (the terminal does not offer it), or the read-only password kept for another user name."""
    _project(tmp_path, ENV)
    _terminal(monkeypatch, tmp_path, BASE_OPEN)
    assert "must hold every database set up already (mongo)" in _refused(monkeypatch, tmp_path, {"databases": ["starrocks"]})
    assert "keep_storage_paths there are no storage paths" in _refused(monkeypatch, tmp_path, {"databases": ["mongo", "starrocks"], "keep_storage_paths": True})
    assert "ro_password_mode keep needs" in _refused(monkeypatch, tmp_path, {"databases": ["mongo", "starrocks"], **AUTH_AGAIN, "ro_user": True, "ro_password_mode": "keep"})


def test_the_read_gives_the_current_answers_and_never_a_password(tmp_path, monkeypatch, capsys):
    """Bug class: the form of a change filled in with other values than the terminal's defaults over the setup (a memory limit not written read as missing, a size the terminal wrote as text, the filesystem), or a password in the read (the read-only one is a flag only, and the admin one is stored nowhere)."""
    _project(tmp_path, ENV)
    _terminal(monkeypatch, tmp_path, BASE_TYPED)
    capsys.readouterr()
    data = sdp.get_db_settings()
    assert capsys.readouterr().out == ""
    assert data["configured"] == ["postgres", "mongo", "starrocks"] and data["not_readable"] is None
    assert data["values"] == {
        "data_path": "./data", "export_path": "./data/export", "pgdata_path": "./data/database/postgres", "db_name": "datasets", "pg_port": 5432, "pg_mem_limit": 18, "filesystem": "zfs",
        "mongo_data_path": "./data/database/mongo", "mongo_port": 27017, "mongo_cache_size_gb": 2, "mongo_mem_limit": 4, "mongo_validate": "full",
        "starrocks_data_path": "./data/database/starrocks", "starrocks_port": 9030, "starrocks_fe_http_port": 8030, "sr_fe_jvm_heap": 3, "starrocks_mem_limit": 18, "sr_be_mem_limit": 13, "sr_alter_tablet_workers": 6, "sr_compression": "ZSTD",
    }
    assert (data["tablespaces"], data["storage_paths"], data["pgtune"], data["pgtune_set"]) == ([{"name": "fast1", "path": "/mnt/nvme1/pg"}], ["/mnt/a/sr"], "max_connections = 40\nshared_buffers = 8GB", True)
    assert (data["auth"], data["ro_username"], data["ro_password_set"]) == (True, "reader", True)
    assert RO not in json.dumps(data) and ADMIN not in json.dumps(data)
    sdp.print_db_settings(data)
    assert RO not in capsys.readouterr().out
    # A damaged config is a constant reason, never a raise.
    (tmp_path / "config/db/mongo.yaml").write_text("- a list\n")
    assert sdp.get_db_settings()["not_readable"] == "the database configuration (.env, config/db/*.yaml) cannot be read"
