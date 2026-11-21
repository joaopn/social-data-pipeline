"""Tests for `control.child`, the process that runs the action of one verb: `python -m social_data_pipeline.control.child` with one JSON request on standard input.

The contracts pinned here:

- The action is called with exactly the validated fields as keyword arguments: a secret as plain text, `_confirm` only where the action has that parameter, `_run_id` only for `run.start` and only when the request carries one.
- The exit code is the integer a `do_*` action returns when it is one a process can exit with (0 to 255), 0 after the result of a `get_*` action was printed as one JSON document, the code of an `ActionError`, 2 for a request that is refused before anything is called, 1 for anything else, a result outside 0 to 255 included.
- A request that is refused calls nothing; the stand-ins record their calls, and the list must be empty.
- Nothing the child prints holds a value of the request: a made-up secret is planted in every request, and is in neither output.

`main` is called in this process with a prepared standard input, and every action of the CLI is replaced by a stand-in with the same signature that records its call. One test runs the real entry point as a process against the real CLI.
"""

from __future__ import annotations

import inspect
import io
import json
import os
import subprocess
import sys
from pathlib import Path
from types import MappingProxyType

import pytest

import sdp
from social_data_pipeline.control import child, verbs

REPO_ROOT = Path(__file__).resolve().parents[2]
# The processes started here load the package from the checkout and must leave no compiled files in it.
NO_BYTECODE = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}

# A value planted in every request, as the admin password where the verb takes one and elsewhere as the value that is refused.
PLANTED = "planted-made-up-secret-9d2e"
RUN_ID = "r20261121T101500-0a1b2c"

# No test of this file reaches the real Docker: the store's Docker function is replaced, and a test that reaches the replacement fails.
pytestmark = pytest.mark.usefixtures("run_docker_guard")


# ── helpers ─────────────────────────────────────────────────────────────────


class _Actions:
    """Stand-ins for every action of the CLI. Each has the signature of the function it replaces, refuses what that function's signature refuses, records the keyword arguments it got and returns `result` (or raises it)."""

    def __init__(self, monkeypatch):
        self.calls = []
        self.result = 0
        for name, real in list(vars(sdp).items()):
            if name.startswith(("do_", "get_")) and inspect.isfunction(real):
                monkeypatch.setattr(sdp, name, self._stand_in(name, inspect.signature(real)))

    def _stand_in(self, name, signature):
        def stand_in(**arguments):
            signature.bind(**arguments)
            self.calls.append((name, arguments))
            if isinstance(self.result, BaseException):
                raise self.result
            return self.result

        stand_in.__signature__ = signature
        return stand_in


@pytest.fixture
def actions(monkeypatch, source_tree):
    return _Actions(monkeypatch)


def run_child(request, capsys):
    """Call `main` with the request on its standard input; returns (exit code, standard output, standard error)."""
    raw = request if isinstance(request, bytes) else json.dumps(request).encode("utf-8")
    code = child.main(io.BytesIO(raw))
    captured = capsys.readouterr()
    return code, captured.out, captured.err


# ── what the action is called with ──────────────────────────────────────────


class TestArguments:
    def test_a_run_gets_its_fields_the_plain_password_and_the_run_id(self, actions, capsys):
        """Bug class: the action called with something else than the request: the wrapped secret instead of its text (`do_run_start` refuses that with a TypeError, and a wrapper that reached compose's environment would be the text `<secret>`), a field dropped or renamed, the run id left out (the container then has no label and the run reads as lost), or a callback invented for a parameter the action does not have."""
        request = {"verb": "run.start", "fields": {"profile": "mongo_ingest", "source": "ok", "build": True, "filter": "RS_2024-*", "admin_password": PLANTED}, "run_id": RUN_ID}

        code, out, err = run_child(request, capsys)

        assert (code, out, err) == (0, "", "")
        assert actions.calls == [("do_run_start", {"profile": "mongo_ingest", "source": "ok", "build": True, "filter": "RS_2024-*", "admin_password": PLANTED, "_run_id": RUN_ID})]
        assert type(actions.calls[0][1]["admin_password"]) is str

    def test_a_run_without_a_run_id_gets_none(self, actions, capsys):
        """Bug class: a run id invented for a request that carries none, which would label the container of a run that has no record."""
        code, _, _ = run_child({"verb": "run.start", "fields": {"profile": "parse", "source": "ok"}}, capsys)

        assert code == 0 and actions.calls == [("do_run_start", {"profile": "parse", "source": "ok"})]

    @pytest.mark.parametrize("verb_name, fields, expected", [
        ("db.reset", {"db": "postgres"}, ("do_db_reset", {"db": "postgres", "_confirm": sdp.already_confirmed})),
        ("db.unsetup", {"db": "mongo"}, ("do_db_unsetup", {"db": "mongo", "_confirm": sdp.already_confirmed})),
        ("source.remove", {"name": "ok"}, ("do_source_remove", {"name": "ok", "_confirm": sdp.already_confirmed})),
        ("db.start", {"service": "postgres", "admin_password": PLANTED}, ("do_db_start", {"service": "postgres", "admin_password": PLANTED})),
        ("db.start", {}, ("do_db_start", {})),
        ("db.stop", {"service": "jobs"}, ("do_db_stop", {"service": "jobs"})),
        ("run.cancel", {"run_id": RUN_ID}, ("do_run_cancel", {"run_id": RUN_ID})),
        ("db.unsetup_mcp", {}, ("do_db_unsetup_mcp", {})),
    ])
    def test_the_confirmation_is_passed_exactly_where_the_action_asks_one(self, actions, capsys, verb_name, fields, expected):
        """Bug class: a destructive action called without its confirmation callback (it then fails closed and nothing a browser asks for ever runs), with another callback than the one that answers yes, or a `_confirm` handed to an action that has no such parameter (a TypeError for every request of that verb). The password callback is never passed: there is no terminal to ask on."""
        code, _, _ = run_child({"verb": verb_name, "fields": fields}, capsys)

        assert code == 0 and actions.calls == [expected]

    def test_every_action_can_be_called_with_what_the_child_builds(self, actions, capsys):
        """Bug class: a verb whose action the child cannot call at all: the walk over the registry builds the smallest valid request of every verb that has an action and requires one recorded call each, so a new action with a parameter the child does not know how to fill fails here."""
        smallest = {"run.start": {"profile": "parse", "source": "ok"}, "run.logs": {"run_id": RUN_ID}, "run.cancel": {"run_id": RUN_ID}, "db.reset": {"db": "postgres"}, "db.unsetup": {"db": "postgres"}, "source.remove": {"name": "ok"}}
        actions.result = {}
        with_action = sorted(name for name, verb in verbs.VERBS.items() if verb.action is not None)

        for name in with_action:
            actions.result = {} if verbs.VERBS[name].action.startswith("get_") else 0
            code, _, err = run_child({"verb": name, "fields": smallest.get(name, {})}, capsys)
            assert (code, err) == (0, ""), name

        assert [name for name, _ in actions.calls] == [verbs.VERBS[name].action for name in with_action]


# ── what comes back ─────────────────────────────────────────────────────────


class TestOutcomes:
    @pytest.mark.parametrize("result", [0, 1, 3, 137, 255])
    def test_the_result_of_a_state_changing_action_is_the_exit_code(self, actions, capsys, result):
        """Bug class: every run reported as a success (or as a failure) whatever the action returned: the supervisor records this exit code as the run's."""
        actions.result = result

        code, out, err = run_child({"verb": "db.stop", "fields": {}}, capsys)

        assert (code, out, err) == (result, "", "")

    @pytest.mark.parametrize("result", [None, True, "0", 1.0, [0]], ids=repr)
    def test_a_result_that_is_no_exit_code_is_a_failure(self, actions, capsys, result):
        """Bug class: an action that returned nothing (a missing `return`) read as exit code 0."""
        actions.result = result

        code, out, err = run_child({"verb": "db.stop", "fields": {}}, capsys)

        assert (code, out, err) == (1, "", "Error: db.stop: the action returned no exit code.\n")

    @pytest.mark.parametrize("result", [256, 512, 1000, -1, -9, -15, 2 ** 40])
    def test_a_number_no_process_can_exit_with_is_a_failure(self, actions, capsys, result):
        """Bug class: a result outside 0 to 255 handed to the operating system, which keeps its lowest eight bits: 256 and 512 would read as exit code 0, a success, and the -9 of a `docker compose` that was killed as 247. Each is exit code 1 with one line that names the number."""
        actions.result = result

        code, out, err = run_child({"verb": "run.start", "fields": {"profile": "parse", "source": "ok"}}, capsys)

        assert (code, out, err) == (1, "", f"Error: run.start: the action returned {result}, which is no exit code.\n")

    @pytest.mark.parametrize("exit_code", [256, -1, None, True, "2"], ids=repr)
    def test_an_action_error_with_a_code_no_process_can_exit_with_is_a_failure(self, actions, capsys, exit_code):
        """Bug class: the exit code of an `ActionError` passed on unchecked: 256 would read as a success, `None` as exit code 0, and text would be printed by the interpreter as an error message."""
        actions.result = sdp.ActionError("the database admin password is required", exit_code=exit_code)

        code, out, err = run_child({"verb": "db.start", "fields": {}}, capsys)

        assert (code, out, err) == (1, "", "Error: the database admin password is required\n")

    def test_the_data_of_a_getter_is_one_json_document(self, actions, capsys):
        """Bug class: the data of a read verb printed in a form its reader cannot parse (the `repr` of a dict, two documents, a banner line before it), or the getter's result taken for an exit code."""
        actions.result = {"sources": [{"name": "ok", "data_types": ["posts"]}], "note": "caf\u00e9 \u2028", "count": 3}

        code, out, err = run_child({"verb": "source.list", "fields": {}}, capsys)

        assert (code, err) == (0, "")
        assert out.endswith("\n") and out.count("\n") == 1
        assert json.loads(out) == actions.result

    def test_data_that_is_no_json_is_a_failure_and_prints_no_part_of_it(self, actions, capsys):
        """Bug class: half a document on standard output, or a traceback with the data in it, when a getter returns something JSON cannot hold."""
        actions.result = {"when": object(), "note": PLANTED}

        code, out, err = run_child({"verb": "source.list", "fields": {}}, capsys)

        assert (code, out, err) == (1, "", "Error: the request failed (TypeError).\n")

    @pytest.mark.parametrize("exit_code", [1, 7])
    def test_an_action_error_prints_its_message_and_returns_its_code(self, actions, capsys, exit_code):
        """Bug class: the reason an action stopped (the admin password is required, the run id names no run) lost, replaced by a traceback, or its exit code flattened."""
        actions.result = sdp.ActionError("the database admin password is required", exit_code=exit_code)

        code, out, err = run_child({"verb": "run.start", "fields": {"profile": "mongo_ingest", "source": "ok"}}, capsys)

        assert (code, out, err) == (exit_code, "", "Error: the database admin password is required\n")

    def test_any_other_error_names_its_kind_only(self, actions, capsys):
        """Bug class: an unexpected error of an action printed with its text or as a traceback: the text can hold a value of the request (here the password), and the traceback's frames do."""
        actions.result = RuntimeError(f"could not connect with password {PLANTED}")

        code, out, err = run_child({"verb": "db.start", "fields": {"admin_password": PLANTED}}, capsys)

        assert (code, out, err) == (1, "", "Error: the request failed (RuntimeError).\n")
        assert len(actions.calls) == 1


# Requests that must be refused before anything is called, each with the one line it prints. The planted value is somewhere in every one of them.
REFUSED = [
    ("not-json", b'{"verb": "db.stop", "fields": {"service": "' + PLANTED.encode() + b'"', "Error: the request is not valid JSON.\n"),
    ("not-utf8", b'{"verb": "db.stop", "fields": {"service": "\xff' + PLANTED.encode() + b'"}}', "Error: the request is not valid JSON.\n"),
    ("empty", b"", "Error: the request is not valid JSON.\n"),
    ("nested-deep", b"[" * 20000 + PLANTED.encode(), "Error: the request is not valid JSON.\n"),
    ("too-large", json.dumps({"verb": "db.start", "fields": {"admin_password": PLANTED + "x" * 70000}}).encode(), "Error: the request is larger than a request may be.\n"),
    ("a-list", ["db.stop", {"service": PLANTED}], "Error: the request must be an object with 'verb', 'fields' and nothing else but 'run_id'.\n"),
    ("no-fields", {"verb": "db.stop", "service": PLANTED}, "Error: the request must be an object with 'verb', 'fields' and nothing else but 'run_id'.\n"),
    ("no-verb", {"fields": {"admin_password": PLANTED}}, "Error: the request must be an object with 'verb', 'fields' and nothing else but 'run_id'.\n"),
    ("another-key", {"verb": "db.start", "fields": {"admin_password": PLANTED}, "_confirm": True}, "Error: the request must be an object with 'verb', 'fields' and nothing else but 'run_id'.\n"),
    ("verb-not-text", {"verb": [PLANTED], "fields": {}}, "Error: the request must be an object with 'verb', 'fields' and nothing else but 'run_id'.\n"),
    ("fields-not-an-object", {"verb": "db.stop", "fields": [PLANTED]}, "Error: the request must be an object with 'verb', 'fields' and nothing else but 'run_id'.\n"),
    ("fields-null", {"verb": "db.stop", "fields": None, "run_id": PLANTED}, "Error: the request must be an object with 'verb', 'fields' and nothing else but 'run_id'.\n"),
    ("unknown-verb", {"verb": "db.explode", "fields": {"admin_password": PLANTED}}, "Error: Unknown verb 'db.explode'.\n"),
    ("unknown-verb-with-the-value", {"verb": PLANTED + "\nx", "fields": {}}, "Error: Unknown verb (its name is not a well-formed verb name).\n"),
    ("not-over-the-socket", {"verb": "source.add", "fields": {"name": PLANTED}}, "Error: source.add is not available over the socket. Setup becomes available when its questions have typed inputs.\n"),
    ("unknown-field", {"verb": "db.start", "fields": {"admin_password": PLANTED, "data_path": "/srv"}}, "Error: db.start: field 'data_path' is not accepted.\n"),
    ("private-parameter-as-a-field", {"verb": "db.reset", "fields": {"db": "postgres", "_confirm": PLANTED}}, "Error: db.reset: a field with a malformed name is not accepted.\n"),
    ("run-id-as-a-field", {"verb": "run.start", "fields": {"profile": "parse", "source": "ok", "_run_id": RUN_ID, "admin_password": PLANTED}}, "Error: run.start: a field with a malformed name is not accepted.\n"),
    ("bad-value", {"verb": "db.stop", "fields": {"service": PLANTED}}, "Error: db.stop: field 'service' must be one of: postgres, mongo, starrocks, postgres-mcp, mongo-mcp, starrocks-mcp, jobs.\n"),
    ("bad-secret", {"verb": "db.start", "fields": {"admin_password": PLANTED + "\nPOSTGRES_PORT=1"}}, "Error: db.start: field 'admin_password' must not contain a line break or control character.\n"),
    ("missing-field", {"verb": "run.start", "fields": {"profile": "parse", "admin_password": PLANTED}}, "Error: run.start: field 'source' is required.\n"),
    ("run-id-on-another-verb", {"verb": "db.start", "fields": {"admin_password": PLANTED}, "run_id": RUN_ID}, "Error: a run id is accepted only with run.start.\n"),
    ("run-id-on-a-read-verb", {"verb": "run.logs", "fields": {"run_id": RUN_ID}, "run_id": RUN_ID}, "Error: a run id is accepted only with run.start.\n"),
]
REFUSED += [
    (f"run-id-{index}", {"verb": "run.start", "fields": {"profile": "parse", "source": "ok", "admin_password": PLANTED}, "run_id": run_id}, "Error: the run id is not the id of a recorded run.\n")
    for index, run_id in enumerate(["live-0a1b2c3d4e5f", RUN_ID + "\n", RUN_ID + " --privileged", "../" + RUN_ID, RUN_ID.upper(), "", PLANTED, None, 20261121, [RUN_ID], True])
]


class TestRefusals:
    @pytest.mark.parametrize("request_, line", [(request, line) for _, request, line in REFUSED], ids=[name for name, _, _ in REFUSED])
    def test_a_refused_request_calls_nothing_and_says_one_line(self, actions, capsys, request_, line):
        """Bug class: a request acted on before it was checked (a field the registry does not list reaching an action, a private parameter set by the client, a run id that is no run id reaching the compose command line), a refusal that exits like an action that ran and failed, or a refusal that quotes what the client wrote. Every refusal is exit code 2 with exactly one constant-shaped line on standard error, nothing on standard output, and no call of any action."""
        code, out, err = run_child(request_, capsys)

        assert (code, out, err) == (2, "", line)
        assert actions.calls == []

    def test_a_verb_without_an_action_is_refused(self, actions, monkeypatch, capsys):
        """Bug class: a verb that is accepted over the socket but has no action ending in an AttributeError, or calling a function of the CLI picked some other way."""
        without = verbs.Verb(name="db.stop", cls="write", cli=("db", "stop"), socket_inputs=dict(verbs.VERBS["db.stop"].socket_inputs), socket_enabled=True)
        monkeypatch.setattr(verbs, "VERBS", MappingProxyType({**verbs.VERBS, "db.stop": without}))

        code, out, err = run_child({"verb": "db.stop", "fields": {"service": "jobs"}}, capsys)

        assert (code, out, err) == (2, "", "Error: db.stop has no action that runs without a terminal.\n")
        assert actions.calls == []

    def test_a_field_the_action_does_not_take_is_an_error_not_dropped(self, actions, monkeypatch, capsys):
        """Bug class: a validated field silently dropped because the action has no parameter for it: the run would then do something else than the request says (a filter ignored, a database not named), with exit code 0."""
        reached = []

        def narrower(*, admin_password=None):
            reached.append(admin_password)
            return 0

        monkeypatch.setattr(sdp, "do_db_start", narrower)

        code, out, err = run_child({"verb": "db.start", "fields": {"service": "postgres", "admin_password": PLANTED}}, capsys)

        assert (code, out, err) == (2, "", "Error: db.start: the action does not take the field 'service'.\n")
        assert reached == [] and actions.calls == []

    def test_an_action_without_a_run_id_parameter_is_not_called_with_a_run_id(self, actions, monkeypatch, capsys):
        """Bug class: the run id dropped when the action cannot take it, so the run starts without its label and its supervisor never finds the container."""
        reached = []

        def without(*, profile, source, build=False, filter=None, skip_lingua_files=False, classifier=None, admin_password=None):
            reached.append(profile)
            return 0

        monkeypatch.setattr(sdp, "do_run_start", without)

        code, out, err = run_child({"verb": "run.start", "fields": {"profile": "parse", "source": "ok"}, "run_id": RUN_ID}, capsys)

        assert (code, out, err) == (2, "", "Error: run.start: the action does not take a run id.\n")
        assert reached == []


class TestNothingOfTheRequestIsPrinted:
    def test_the_planted_value_is_in_no_output_of_any_outcome(self, actions, capsys):
        """Bug class: the request, or a part of it, in what the child prints: a debug line, an error that quotes the value it refused, a traceback. The child's output is the run's log file, which every later reader of the log sees. Every outcome is produced with the planted value in the request, and both outputs are searched for it."""
        outputs = []
        for _, request, _ in REFUSED:
            outputs.append(run_child(request, capsys))
        for result in (0, 3, None, sdp.ActionError("the database admin password is required"), RuntimeError(PLANTED), KeyError(PLANTED)):
            actions.result = result
            outputs.append(run_child({"verb": "run.start", "fields": {"profile": "mongo_ingest", "source": "ok", "admin_password": PLANTED}, "run_id": RUN_ID}, capsys))
            outputs.append(run_child({"verb": "db.start", "fields": {"admin_password": PLANTED}}, capsys))
        actions.result = {"sources": []}
        outputs.append(run_child({"verb": "source.list", "fields": {}}, capsys))

        assert len(outputs) == len(REFUSED) + 13
        for code, out, err in outputs:
            assert PLANTED not in out and PLANTED not in err, (code, out, err)
            assert "Traceback" not in err and err.count("\n") <= 1
        # The planted value did reach the actions that were called, so the search above looked for something that was there.
        assert sum(1 for _, arguments in actions.calls if arguments.get("admin_password") == PLANTED) == 12


# ── the real entry point ────────────────────────────────────────────────────


class TestEntryPoint:
    def test_a_read_verb_through_the_real_process(self):
        """Bug class: the module not runnable with `-m` (work done at import, a missing `__main__` guard), or the CLI not importable from the deployment folder in a fresh process, which no test that calls `main` in this process would see. A read verb is run against the real CLI of this checkout: one JSON document, exit code 0, nothing on standard error."""
        result = subprocess.run([sys.executable, "-m", "social_data_pipeline.control.child"], cwd=REPO_ROOT, env=NO_BYTECODE, input=json.dumps({"verb": "source.list", "fields": {}}).encode(), capture_output=True, timeout=60)

        assert result.returncode == 0, result.stderr
        assert result.stderr == b""
        assert set(json.loads(result.stdout)) == {"sources"}

    def test_a_refusal_through_the_real_process(self):
        """Bug class: the exit code of `main` not passed on to the process (a `main()` call without `sys.exit`), so every refusal reads as a success to the process that started the child."""
        result = subprocess.run([sys.executable, "-m", "social_data_pipeline.control.child"], cwd=REPO_ROOT, env=NO_BYTECODE, input=json.dumps({"verb": "db.stop", "fields": {"service": PLANTED}}).encode(), capture_output=True, timeout=60)

        assert result.returncode == 2 and result.stdout == b""
        assert result.stderr == b"Error: db.stop: field 'service' must be one of: postgres, mongo, starrocks, postgres-mcp, mongo-mcp, starrocks-mcp, jobs.\n"

    def test_loading_the_module_does_nothing(self):
        """Bug class: work at import time: a process that only imports the module (a test, the supervisor reading a constant) would read standard input or call the CLI. The process also reports that it writes no compiled files: the processes of this file load the package from the checkout, which they must leave as it is."""
        code = "import sys; import social_data_pipeline.control.child; print(sorted(m for m in sys.modules if m == 'sdp'), sys.dont_write_bytecode); sys.exit(0)"

        result = subprocess.run([sys.executable, "-c", code], cwd=REPO_ROOT, env=NO_BYTECODE, input=b'{"verb": "db.stop", "fields": {}}', capture_output=True, timeout=60)

        assert (result.returncode, result.stdout, result.stderr) == (0, b"[] True\n", b"")
