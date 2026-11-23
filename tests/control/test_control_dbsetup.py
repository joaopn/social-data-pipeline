"""Tests for the answers of `db.setup` at the registry (`control.dbsetup.check` as the verb's cross-field check, the types of `verbs.DB_SETUP_INPUTS`), and for the web UI's copy of the check (`webui.setup_db_table.first_refusal`), which must refuse the same answers at the same input: the image holds no registry, so the page checks with its own copy, and a drift would let the operator confirm what the broker refuses (or refuse what it takes).

Each case names the input that must be refused; the reason never holds the value. Passwords are made-up values.
"""

from __future__ import annotations

import pytest

from social_data_pipeline.control import dbsetup, errors, verbs
from social_data_pipeline.webui import setup_db_table

PW = "made-up-pw-31b7"
AUTH = {"auth": True, "new_password": PW, "new_password_confirm": PW}
RO_TYPED = {**AUTH, "ro_user": True, "ro_password_mode": "typed", "ro_password": PW, "ro_password_confirm": PW}

GOOD = [
    {"databases": ["starrocks"]},
    {"databases": ["postgres", "mongo", "starrocks"], "data_path": "/srv/data", "pg_port": 5433, "mongo_port": 27018, "starrocks_port": 9031, "starrocks_fe_http_port": 8031},
    {"add": "postgres", "tablespaces": [{"name": "fast_1", "path": "/mnt/nvme1/pg"}], "pgtune": "max_connections = 40\n\nwork_mem = 4MB\n"},
    {"databases": ["mongo"], **AUTH},
    {"databases": ["mongo"], **AUTH, "ro_user": True, "ro_username": "reader", "ro_password_mode": "generate"},
    {"databases": ["mongo"], **RO_TYPED},
    {"databases": ["starrocks"], "starrocks_storage_paths": ["/mnt/a", "/mnt/x y/sr"]},
]

REFUSED = [
    # Mode: the first-time inputs or `add`, not both, not neither.
    ({}, "databases"),
    ({"databases": []}, "databases"),
    ({"add": "mongo", "databases": ["mongo"]}, "databases"),
    ({"add": "mongo", "data_path": "/srv"}, "data_path"),
    ({"add": "mongo", "export_path": "/srv/x"}, "export_path"),
    # A database's inputs without that database.
    ({"databases": ["mongo"], "pg_port": 5433}, "pg_port"),
    ({"add": "postgres", "sr_compression": "LZ4"}, "sr_compression"),
    # Paths the generated files cannot hold as they are.
    ({"databases": ["mongo"], "mongo_data_path": "/srv/$HOME/mongo"}, "mongo_data_path"),
    ({"databases": ["mongo"], "data_path": "/srv/a:b"}, "data_path"),
    ({"databases": ["mongo"], "export_path": "/srv/x #y"}, "export_path"),
    ({"databases": ["mongo"], "export_path": " /srv/x"}, "export_path"),
    ({"databases": ["starrocks"], "starrocks_storage_paths": ["/mnt/a'b"]}, "starrocks_storage_paths"),
    # Tablespaces: an unquoted SQL name, not the default one, not twice.
    ({"databases": ["postgres"], "tablespaces": [{"name": "pgdata", "path": "/mnt/a"}]}, "tablespaces"),
    ({"databases": ["postgres"], "tablespaces": [{"name": "Fast-1", "path": "/mnt/a"}]}, "tablespaces"),
    ({"databases": ["postgres"], "tablespaces": [{"name": "a", "path": "/mnt/a"}, {"name": "a", "path": "/mnt/b"}]}, "tablespaces"),
    ({"databases": ["postgres"], "tablespaces": [{"name": "a", "path": "/mnt/a:b"}]}, "tablespaces"),
    # Two services of the setup on one port.
    ({"databases": ["postgres", "mongo"], "pg_port": 6000, "mongo_port": 6000}, "pg_port"),
    ({"databases": ["starrocks"], "starrocks_port": 9030, "starrocks_fe_http_port": 9030}, "starrocks_port"),
    # Authentication: the passwords only with it on, required then, typed twice alike.
    ({"databases": ["mongo"], "new_password": PW}, "new_password"),
    ({"databases": ["mongo"], "auth": False, "ro_user": True}, "ro_user"),
    ({"databases": ["mongo"], "auth": True}, "new_password"),
    ({"databases": ["mongo"], "auth": True, "new_password": PW}, "new_password_confirm"),
    ({"databases": ["mongo"], **AUTH, "new_password_confirm": PW + "x"}, "new_password_confirm"),
    # The read-only user: its inputs only with it, an unquoted SQL name, a typed password without quotes or white space, typed twice alike.
    ({"databases": ["mongo"], **AUTH, "ro_username": "reader"}, "ro_username"),
    ({"databases": ["mongo"], **AUTH, "ro_user": True, "ro_username": "Reader-1"}, "ro_username"),
    ({"databases": ["mongo"], **AUTH, "ro_user": True, "ro_password_mode": "generate", "ro_password": PW}, "ro_password"),
    ({"databases": ["mongo"], **AUTH, "ro_user": True, "ro_password_mode": "typed"}, "ro_password"),
    ({"databases": ["mongo"], **RO_TYPED, "ro_password": "it's", "ro_password_confirm": "it's"}, "ro_password"),
    ({"databases": ["mongo"], **RO_TYPED, "ro_password": "a b", "ro_password_confirm": "a b"}, "ro_password"),
    ({"databases": ["mongo"], **RO_TYPED, "ro_password_confirm": PW + "x"}, "ro_password_confirm"),
]


@pytest.mark.parametrize("fields", GOOD)
def test_answers_that_belong_together_pass(fields):
    """Bug class: a rule that refuses an honest setup (a path with a space, a pasted PGTune output with blank lines, a generated read-only password), so the browser cannot do what the terminal does."""
    validated = verbs.validate_request("db.setup", fields)
    assert set(validated) == set(fields)
    assert setup_db_table.first_refusal(fields) is None


@pytest.mark.parametrize("fields, field", REFUSED)
def test_answers_the_files_cannot_hold_are_refused_at_their_input(fields, field):
    """Bug class: an answer written into .env, YAML, a mount line or unquoted SQL that breaks it (a `$`, a `:`, a quote, a tablespace named `Fast-1`), a password typed twice differently that becomes the database's, or answers of a database not set up by the request; the refusal names the input and never the value."""
    with pytest.raises(errors.InputError) as refused:
        verbs.validate_request("db.setup", fields)
    assert refused.value.field == field
    assert PW not in str(refused.value)


@pytest.mark.parametrize("fields, field", REFUSED)
def test_the_web_copy_refuses_the_same_answers_at_the_same_input(fields, field):
    """Bug class: the page's copy of the check drifting from the registry's, so the operator confirms answers the broker refuses, or cannot give ones it takes (the GOOD cases above are the other half)."""
    assert setup_db_table.first_refusal(fields) == field


@pytest.mark.parametrize("value", ["/srv/x\ny", "/srv/ x", ""])
def test_a_path_with_a_line_break_or_empty_is_refused_by_its_type(value):
    """Bug class: a path that writes a second line into .env."""
    with pytest.raises(errors.InputError) as refused:
        verbs.validate_request("db.setup", {"databases": ["mongo"], "mongo_data_path": value})
    assert refused.value.field == "mongo_data_path"


@pytest.mark.parametrize("field, value", [("pg_port", 80), ("pg_port", 70000), ("pg_mem_limit", -1), ("pg_mem_limit", 4097), ("pgtune", "a\rb"), ("pgtune", "x" * 16385), ("filesystem", "btrfs"), ("tablespaces", [{"name": "a"}])])
def test_values_out_of_the_terminals_range_are_refused_by_their_type(field, value):
    """Bug class: a port below 1024 or above 65535, a negative or absurd memory size, a carriage return in PGTune's output (the conf would hold it), a choice the terminal does not offer, a tablespace without its folder."""
    with pytest.raises(errors.InputError) as refused:
        verbs.validate_request("db.setup", {"databases": ["postgres"], field: value})
    assert refused.value.field == field


# ── over databases set up already: the keep answers and the rules on the setup as it is ──

REFUSED_ANYWAY = [
    # A keep answer together with the answer it keeps instead of.
    ({"databases": ["postgres"], "keep_tablespaces": True, "tablespaces": [{"name": "a", "path": "/mnt/a"}]}, "keep_tablespaces"),
    ({"databases": ["starrocks"], "keep_storage_paths": True, "starrocks_storage_paths": ["/mnt/a"]}, "keep_storage_paths"),
    ({"databases": ["postgres"], "keep_pgtune": True, "pgtune": "work_mem = 4MB"}, "keep_pgtune"),
    ({"databases": ["mongo"], "keep_tablespaces": True}, "keep_tablespaces"),
    # `--add` never keeps a read-only password.
    ({"add": "mongo", **AUTH, "ro_user": True, "ro_password_mode": "keep"}, "ro_password_mode"),
]

SET_UP = {"configured": ["postgres", "mongo"], "auth": True, "ro_username": "reader", "ro_password_set": True, "tablespaces": True, "storage_paths": False, "pgtune": True}
CHANGE = {"databases": ["postgres", "mongo"], **AUTH}

GOOD_NOW = [
    ({**CHANGE, "keep_tablespaces": True, "keep_pgtune": True, "ro_user": True, "ro_password_mode": "keep"}, SET_UP),
    ({**CHANGE, "databases": ["postgres", "mongo", "starrocks"], "pgdata_path": "/srv/pg"}, SET_UP),
    ({"databases": ["mongo"], **AUTH}, {**SET_UP, "configured": ["mongo"], "auth": False}),
    ({"add": "starrocks"}, SET_UP),
]
REFUSED_NOW = [
    # Authentication switched off, or left out, over databases that have it (decided: a terminal command).
    ({"databases": ["postgres", "mongo"], "auth": False}, SET_UP, "auth"),
    ({"databases": ["postgres", "mongo"]}, SET_UP, "auth"),
    # A database set up left out; an add of one set up.
    ({**CHANGE, "databases": ["postgres"]}, SET_UP, "databases"),
    ({"add": "mongo"}, SET_UP, "add"),
    # Nothing to keep.
    ({**CHANGE, "databases": ["postgres", "mongo", "starrocks"], "keep_storage_paths": True}, SET_UP, "keep_storage_paths"),
    ({**CHANGE, "keep_pgtune": True}, {**SET_UP, "pgtune": False}, "keep_pgtune"),
    # The read-only password kept for another name, or where none can be read.
    ({**CHANGE, "ro_user": True, "ro_username": "other", "ro_password_mode": "keep"}, SET_UP, "ro_password_mode"),
    ({**CHANGE, "ro_user": True, "ro_password_mode": "keep"}, {**SET_UP, "ro_password_set": False}, "ro_password_mode"),
]


@pytest.mark.parametrize("fields, field", REFUSED_ANYWAY)
def test_keep_answers_that_contradict_are_refused_by_the_registry_and_the_web_copy(fields, field):
    """Bug class: a keep answer sent with what it keeps instead of (which one wins would be a guess), or the read-only password kept by `--add`, which the terminal never offers."""
    with pytest.raises(errors.InputError) as refused:
        verbs.validate_request("db.setup", fields)
    assert refused.value.field == field
    assert setup_db_table.first_refusal(fields) == field


@pytest.mark.parametrize("fields, current", GOOD_NOW)
def test_a_change_that_fits_the_setup_passes_both_checks(fields, current):
    """Bug class: a rule on the setup as it is that refuses an honest change (keeping what is there, a data path changed, a database added in the same run)."""
    dbsetup.check(fields, current)
    assert setup_db_table.first_refusal(fields, current) is None


@pytest.mark.parametrize("fields, current, field", REFUSED_NOW)
def test_a_change_the_setup_makes_wrong_is_refused_at_the_same_input_by_both(fields, current, field):
    """Bug class: the action's and the page's rules on the setup as it is drifting apart (the page confirming what the action refuses), authentication switched off from the browser, a keep with nothing to keep, a database set up dropped from the run."""
    with pytest.raises(dbsetup.Refused) as refused:
        dbsetup.check(fields, current)
    assert refused.value.field == field and PW not in str(refused.value)
    assert setup_db_table.first_refusal(fields, current) == field
