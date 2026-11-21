"""Tests for the web UI's table of actions (`webui.actions`), which need no FastAPI: the table against the registry, and the command a pending page shows against the CLI's parser.

The contracts pinned here, each test naming the bug class it catches:

- a. The table against the registry (`control.verbs`): every verb with the registry's class, enabled on the socket, inputs a subset of the registry's socket inputs with the same kinds and choices, the patterns and choice lists literal copies; the two places where the web side is stricter on purpose; `call` refusing anything outside the table before anything is sent. (The `broker` fixture of `conftest.py` checks every call of every route test against the table.)
- b. The command shown, against the CLI's parser: split with `shlex.split` and parsed with `sdp.build_parser()`, it gives the subcommand of the registry and exactly the inputs; no secret in it.
"""

from __future__ import annotations

import shlex

import pytest
from webui_helpers import PASSWORD, RUN_ID

from social_data_pipeline.control import broker as broker_server
from social_data_pipeline.control import types, verbs
from social_data_pipeline.webui import actions

SECRET = "made-up-db-admin-pw-77e1"
KIND_OF_TYPE = {types.Enum: "enum", types.Bool: "bool", types.Name: "name", types.ConfiguredSource: "source", types.FileFilter: "filter", types.Secret: "secret", types.RunId: "run_id", types.IntRange: "int"}


# ── a. the table against the registry ───────────────────────────────────────


def test_every_entry_is_a_verb_of_the_registry_with_its_class_and_inputs():
    """Bug class: a table that drifts from the registry: a verb the broker does not know or does not take over the socket, a class that asks for no management password where the broker wants one (or the reverse), an input the broker refuses, a choice the broker refuses or one it takes that the page does not offer."""
    verb_entries = [action for action in actions.TABLE.values() if not action.op]
    assert {action.name for action in verb_entries} == {
        "db.status", "db.verify", "source.list", "source.status", "source.progress", "source.error_logs", "run.list", "run.logs", "audit.list",
        "run.start", "db.stop", "run.cancel", "db.start", "db.reset", "db.unsetup", "db.unsetup_mcp", "db.unsetup_jobs", "source.remove",
    }
    for action in verb_entries:
        verb = verbs.VERBS[action.name]
        assert verb.cls == action.cls and verb.socket_enabled, action.name
        assert set(action.inputs) <= set(verb.socket_inputs), action.name
        for name, spec in action.inputs.items():
            registered = verb.socket_inputs[name]
            assert KIND_OF_TYPE[type(registered.type)] == spec.kind, (action.name, name)
            if spec.kind == "enum":
                assert spec.choices == registered.type.choices, (action.name, name)
            if spec.kind == "int":
                assert registered.type.lo <= spec.lo <= spec.hi <= registered.type.hi, (action.name, name)
            if registered.required:
                assert spec.required, (action.name, name)
        if action.state_changing:
            assert action.command is not None, action.name


def test_the_operations_that_are_not_verbs_are_the_brokers():
    """Bug class: an operation the broker does not have, or the database password kept without the management password."""
    ops = {action.name: action for action in actions.TABLE.values() if action.op}
    assert set(ops) == {"hello", "session.set_db_password", "session.clear_db_password"}
    assert set(ops) <= set(broker_server.OPS)
    assert ops["session.set_db_password"].cls == "step_up" and ops["session.clear_db_password"].cls == "write" and ops["hello"].cls == "read"


def test_the_two_places_where_the_web_side_is_stricter_on_purpose():
    """Bug class: a source verb sent without a source (the CLI then picks one, or reports on all of them), or a piece of a log asked smaller than the broker takes. These are the only two places where the table is narrower than the registry's inputs."""
    for name in actions.SOURCE_VERBS:
        field_name = "source" if name == "run.start" else "name"
        assert actions.TABLE[name].inputs[field_name].required
    assert not verbs.VERBS["source.status"].socket_inputs["name"].required and not verbs.VERBS["source.progress"].socket_inputs["name"].required
    assert actions.TABLE["run.logs"].inputs["limit"].lo == 4096 > verbs.VERBS["run.logs"].socket_inputs["limit"].type.lo
    narrower = []
    for action in (action for action in actions.TABLE.values() if not action.op):
        for name, spec in action.inputs.items():
            registered = verbs.VERBS[action.name].socket_inputs[name]
            if spec.required and not registered.required:
                narrower.append((action.name, name))
            if spec.kind == "int" and (spec.lo, spec.hi) != (registered.type.lo, registered.type.hi):
                narrower.append((action.name, name))
    assert sorted(narrower) == [("run.logs", "limit"), ("source.error_logs", "name"), ("source.error_logs", "profile"), ("source.progress", "name"), ("source.status", "name")]


def test_the_patterns_and_choices_are_the_registrys():
    """Bug class: a literal copy of a pattern or a list that drifted: names the broker refuses offered as links or accepted in forms, or names it takes refused."""
    assert actions.FILE_FILTER_PATTERN == types.FILE_FILTER_PATTERN
    assert actions.RUN_ID_PATTERN == types.RUN_ID_PATTERN
    assert (actions.VALID_PROFILES, actions.DATABASES, actions.SERVICES, actions.ERROR_LOG_PROFILES) == (verbs.VALID_PROFILES, verbs.DATABASES, verbs.SERVICES, verbs.ERROR_LOG_PROFILES)
    assert actions.RUN_LOGS_MIN_LIMIT == broker_server.redact.MIN_PIECE_BYTES


class RecordingClient:
    def __init__(self):
        self.sent = []

    def call(self, verb, fields=None, step_up=None):
        self.sent.append(("call", verb, fields, step_up))
        return {}

    def hello(self):
        self.sent.append(("hello",))
        return {}

    def set_db_password(self, password, step_up):
        self.sent.append(("set_db_password",))
        return {}

    def clear_db_password(self):
        self.sent.append(("clear_db_password",))
        return {}


@pytest.mark.parametrize("name, fields, step_up", [
    ("source.download", {"name": "demo"}, None),
    ("db.setup", {}, PASSWORD),
    ("made.up", {}, None),
    (None, {}, None),
    ("source.status", {}, None),
    ("source.status", {"name": "demo", "extra": 1}, None),
    ("source.status", {"name": "../etc"}, None),
    ("run.logs", {"run_id": RUN_ID, "limit": 4095}, None),
    ("run.logs", {"run_id": "r1"}, None),
    ("run.start", {"profile": "sr_ml", "source": "demo", "filter": "x"}, None),
    ("run.start", {"profile": "parse", "source": "demo", "admin_password": SECRET}, None),
    ("db.reset", {"db": "postgres"}, None),
    ("db.stop", {}, PASSWORD),
    ("session.set_db_password", {"password": actions.Hidden(SECRET)}, None),
])
def test_call_sends_nothing_outside_the_table(name, fields, step_up):
    """Bug class: a page that can make the broker receive a verb outside the table, a field the table does not list, a value it refuses (a secret not wrapped, a source that is no plain name), or the management password with an entry that does not take it."""
    made = RecordingClient()
    with pytest.raises(actions.NotInTable):
        actions.call(made, name, fields, step_up)
    assert made.sent == []


def test_call_sends_what_the_table_allows():
    """Bug class: a check so strict that the pages' own requests are refused, or a secret sent still wrapped."""
    made = RecordingClient()
    actions.call(made, "hello")
    actions.call(made, "run.logs", {"run_id": RUN_ID, "offset": 0, "limit": 65536})
    actions.call(made, "db.start", {"service": "postgres", "admin_password": actions.Hidden(SECRET)}, PASSWORD)
    actions.call(made, "session.clear_db_password")
    assert made.sent == [("hello",), ("call", "run.logs", {"run_id": RUN_ID, "offset": 0, "limit": 65536}, None), ("call", "db.start", {"service": "postgres", "admin_password": SECRET}, PASSWORD), ("clear_db_password",)]


# ── b. the command shown, against the CLI ───────────────────────────────────


COMMAND_CASES = [
    ("run.start", {"profile": "parse", "source": "demo.v2", "build": True, "filter": "-RS_2024-*", "skip_lingua_files": True}),
    ("run.start", {"profile": "ml", "source": "Reddit_2024.v2-x", "classifier": "toxic.roberta-v1", "admin_password": actions.Hidden(SECRET)}),
    ("run.start", {"profile": "lingua", "source": "0", "filter": "[!a]*?.x", "classifier": "lead.v-1"}),
    ("run.start", {"profile": "sr_ml", "source": "x", "build": True, "admin_password": actions.Hidden(SECRET)}),
    ("db.stop", {}),
    ("db.stop", {"service": "postgres-mcp"}),
    ("run.cancel", {"run_id": RUN_ID}),
    ("run.cancel", {"run_id": "live-0123456789ab"}),
    ("db.start", {}),
    ("db.start", {"service": "jobs", "admin_password": actions.Hidden(SECRET)}),
    ("db.reset", {"db": "starrocks"}),
    ("db.unsetup", {"db": "mongo"}),
    ("db.unsetup_mcp", {}),
    ("db.unsetup_jobs", {}),
    ("source.remove", {"name": "demo.v2"}),
]


@pytest.mark.parametrize("name, fields", COMMAND_CASES)
def test_the_command_shown_is_what_the_cli_parses_into_the_inputs(name, fields):
    """Bug class: a command shown that is not the one the action runs: another subcommand, a flag the CLI does not have, a value taken for a flag (a filter or a classifier that starts with a dash), a value cut by the shell, a boolean shown when it is not set; or a secret in the command."""
    import sdp

    actions.check_fields(actions.TABLE[name], fields)
    command = actions.TABLE[name].command(fields)
    assert SECRET not in command
    words_of = shlex.split(command)
    assert words_of[:2] == ["python", "sdp.py"]
    try:
        parsed = sdp.build_parser().parse_args(words_of[2:])
    except SystemExit:
        pytest.fail(f"the CLI's parser refused {command!r}")
    cli = verbs.VERBS[name].cli
    assert parsed.command == cli[0]
    if len(cli) > 1:
        assert getattr(parsed, f"{cli[0]}_command") == cli[1]
    for input_name, spec in actions.TABLE[name].inputs.items():
        if spec.secret:
            continue
        expected = fields.get(input_name, False if spec.kind == "bool" else None)
        assert getattr(parsed, input_name) == expected, (command, input_name)


def test_the_verbs_said_to_be_refused_while_a_run_lives_are_the_brokers():
    """Bug class: a pending page that does not say the broker will refuse the action while a run is alive (the operator confirms and gets `run_active`), or says it of a verb the broker runs anyway."""
    assert {action.name for action in actions.TABLE.values() if action.note} == broker_server.DESTRUCTIVE_VERBS
    assert "db.unsetup_mcp" in broker_server.DESTRUCTIVE_VERBS


# How the docs name each entry that needs the management password again.
STEP_UP_IN_THE_DOCS = {
    "db.start": "a database start",
    "db.reset": "a database reset",
    "db.unsetup": "removing a database",
    "db.unsetup_mcp": "the MCP servers' configuration",
    "db.unsetup_jobs": "the scheduler's configuration",
    "source.remove": "removing a source",
    "session.set_db_password": "keeping the database admin password for the session",
}


def test_the_docs_name_every_action_that_needs_the_management_password_and_no_other():
    """Bug class: docs that tell the operator a run start needs the management password and a reset does not (or any other wrong set), so that the operator is surprised by the question or by its absence."""
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    assert set(STEP_UP_IN_THE_DOCS) == {action.name for action in actions.TABLE.values() if action.cls == "step_up"}
    phrase = "a database start, a database reset, removing a database, the MCP servers' configuration or the scheduler's configuration, removing a source, and keeping the database admin password for the session"
    assert all(named in phrase for named in STEP_UP_IN_THE_DOCS.values())
    for path in ("README.md", "docs/guides/webui.md"):
        text = (root / path).read_text()
        assert phrase in text, path
        assert "starts or removes something" not in text, path
