"""Tests for the verb registry and `validate_request`, the boundary a request passes before anything acts on it.

The contracts pinned here:

- Every leaf subcommand of the CLI parser is one verb or is listed as terminal-only, and each verb records exactly the destinations its subcommand has, so a new subcommand or flag cannot appear without a decision here.
- What the socket accepts is a subset of what the CLI takes or asks, typed with the closed set in `control.types`, and anything not listed is refused (deny by default).
- No string input lets a line break, a path or a parent reference through, and no error or repr shows a field's value.

The registry is reached through the module (`verbs.<name>`) and the CLI as `import sdp`; sources live in the temp tree of the `source_tree` fixture.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import pytest

import sdp
from social_data_pipeline.control import errors, types, verbs

REPO_ROOT = Path(__file__).resolve().parents[2]

# The type classes a socket input may use, written out here on purpose: adding a type to the package does not widen this set.
ALLOWED_TYPE_NAMES = {"Bool", "IntRange", "Enum", "Name", "FileFilter", "ConfiguredSource", "RunId", "Secret", "Path", "Port", "Text", "ListOf", "MapOf", "Record"}
# The wide types (a host path, any port, free text), and every (verb, field) that uses one, also nested in a container. Each entry is a decision of the maintainer (any path and any port with the management password); a setup unit adds its own here when it lists the input.
WIDE_TYPE_NAMES = {"Path", "Port", "Text"}
WIDE_INPUTS = {
    # `db setup-mcp` and `db setup-jobs`: the servers' ports and the scheduler's port and results folder.
    ("db.setup_mcp", "postgres"), ("db.setup_mcp", "mongo"), ("db.setup_mcp", "starrocks"), ("db.setup_jobs", "port"), ("db.setup_jobs", "result_root"),
    # A source's four folders may be any path (F4 decision 1); the action refuses one of another source.
    ("source.add", "dumps_path"), ("source.add", "extracted_path"), ("source.add", "parsed_path"), ("source.add", "output_path"),
    # The classifiers' exact strings and regular expressions to remove are free text in the terminal too (one line each).
    ("source.add", "remove_strings"), ("source.add", "remove_patterns"),
}

# The security classification of every verb: (class, accepted over the socket). Changing one is a decision, so it is made in two places.
CLASSIFICATION = {
    "db.status": ("read", True),
    "source.list": ("read", True),
    "db.verify": ("read", True),
    "source.status": ("read", True),
    "source.progress": ("read", True),
    "source.error_logs": ("read", True),
    "run.list": ("read", True),
    "run.logs": ("read", True),
    "audit.list": ("read", True),
    "run.start": ("write", True),
    "run.cancel": ("write", True),
    "db.stop": ("write", True),
    "source.download": ("write", False),
    "db.start": ("step_up", True),
    "db.reset": ("step_up", True),
    "db.unsetup": ("step_up", True),
    "db.unsetup_mcp": ("step_up", True),
    "db.unsetup_jobs": ("step_up", True),
    "source.remove": ("step_up", True),
    "db.setup": ("step_up", False),
    "db.setup_mcp": ("step_up", True),
    "db.setup_jobs": ("step_up", True),
    "source.add": ("step_up", True),
    "source.configure": ("step_up", False),
    "source.add_classifiers": ("step_up", False),
    "db.create_indexes": ("step_up", False),
    "db.recover_password": ("step_up", False),
    "db.restart_needed": ("read", True),
    "db.llm_settings": ("read", True),
}

ALL_VERBS = sorted(verbs.VERBS)
ENABLED_VERBS = [name for name in ALL_VERBS if verbs.VERBS[name].socket_enabled]
DISABLED_VERBS = [name for name in ALL_VERBS if not verbs.VERBS[name].socket_enabled]

# Other fields a request needs so that a given field is admissible (a cross-field rule would refuse it otherwise).
COMPANIONS = {("run.start", "classifier"): {"profile": "ml"}}

LINE_BREAK_VALUES = ["a\nB=c", "a\rB=c", "a\u2028B=c", "a\x85B=c", "ok\n"]
PATH_VALUES = ["/etc/passwd", "..", "../x", "a/b"]


# ── helpers ─────────────────────────────────────────────────────────────────


def _leaf_parsers(parser, path=()):
    """Yield (subcommand path, parser) for every parser that has no sub-commands of its own."""
    selectors = [action for action in parser._actions if isinstance(action, argparse._SubParsersAction)]
    if not selectors:
        yield path, parser
        return
    for selector in selectors:
        for name, sub_parser in selector.choices.items():
            yield from _leaf_parsers(sub_parser, path + (name,))


def _selector_dests(parser):
    """The destinations of the sub-command selectors (`command`, `db_command`, ...), at every level."""
    dests = set()
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            dests.add(action.dest)
            for sub_parser in action.choices.values():
                dests |= _selector_dests(sub_parser)
    return dests


@pytest.fixture(scope="module")
def cli():
    """The CLI parser's leaves by subcommand path, and the destinations that are not inputs."""
    parser = sdp.build_parser()
    leaves = dict(_leaf_parsers(parser))
    not_inputs = {"help", "func"} | _selector_dests(parser) | verbs.PRESENTATION_FLAGS
    return leaves, not_inputs


def _input_dests(leaf, not_inputs):
    return ({action.dest for action in leaf._actions} | set(leaf._defaults)) - not_inputs


def _valid_value(kind):
    """A value the type accepts ("ok" is a source in the `source_tree` fixture)."""
    samples = {
        types.Bool: True,
        types.Name: "toxic_roberta",
        types.FileFilter: "RS_2024-*",
        types.ConfiguredSource: "ok",
        types.RunId: "r20261121T101500-0a1b2c",
        types.Secret: "correct horse",
        types.Path: "data/x",
        types.Port: 8080,
        types.Text: "x",
    }
    if isinstance(kind, types.Enum):
        return kind.choices[0]
    if isinstance(kind, types.IntRange):
        return kind.lo
    if isinstance(kind, types.ListOf):
        return []
    if isinstance(kind, types.MapOf):
        return {}
    if isinstance(kind, types.Record):
        return {key: _valid_value(spec.type) for key, spec in kind.fields.items() if spec.required}
    return samples[type(kind)]


def _minimal(verb):
    """The smallest request the verb accepts: its required fields only."""
    return {name: _valid_value(spec.type) for name, spec in verb.socket_inputs.items() if spec.required}


def _request_with(verb, field, value):
    """A request that is valid apart from `field`, which carries `value`."""
    request = _minimal(verb)
    request.update(COMPANIONS.get((verb.name, field), {}))
    request[field] = value
    return request


def _inputs(only_strings=False, only_plain_strings=False):
    """(verb name, field name) for the socket inputs of every enabled verb."""
    pairs = []
    for name in ENABLED_VERBS:
        for field, spec in verbs.VERBS[name].socket_inputs.items():
            if only_strings and not spec.type.is_string:
                continue
            # A path is a string that is meant to address the file system: the path sweep is for the other strings.
            if only_plain_strings and not (spec.type.is_string and not spec.type.is_secret and not isinstance(spec.type, types.Path)):
                continue
            pairs.append((name, field))
    return pairs


# ── the registry against the CLI parser ─────────────────────────────────────


class TestRegistryCoversTheCli:
    def test_every_leaf_subcommand_is_one_verb_or_terminal_only(self, cli):
        """Bug class: a subcommand added to the CLI with no decision about the socket, or a verb pointing at a subcommand that was renamed or removed."""
        leaves, _ = cli
        by_cli = {}
        for verb in verbs.VERBS.values():
            by_cli.setdefault(verb.cli, []).append(verb.name)

        assert {path: names for path, names in by_cli.items() if len(names) != 1} == {}
        assert set(by_cli) & set(verbs.TERMINAL_ONLY) == set()
        assert set(by_cli) | set(verbs.TERMINAL_ONLY) == set(leaves)
        assert all(reason for reason in verbs.TERMINAL_ONLY.values())

    def test_classification_of_every_verb(self):
        """Bug class: a verb quietly reclassified (step-up to write, not available to available) or added without one. The registry must match the table in this file entry by entry."""
        assert {name: (verb.cls, verb.socket_enabled) for name, verb in verbs.VERBS.items()} == CLASSIFICATION
        assert all(verb.name == name for name, verb in verbs.VERBS.items())
        assert {verb.cls for verb in verbs.VERBS.values()} <= set(verbs.VERB_CLASSES)

    @pytest.mark.parametrize("name", ALL_VERBS)
    def test_cli_inputs_are_exactly_the_parsers_destinations(self, cli, name):
        """Bug class: a flag added to (or removed from) a subcommand without the registry noticing, which hides that someone must decide whether the socket may send it."""
        leaves, not_inputs = cli
        verb = verbs.VERBS[name]

        assert verb.cli_inputs == _input_dests(leaves[verb.cli], not_inputs)

    def test_presentation_flags_are_real_destinations(self, cli):
        """Bug class: the exclusion list going stale, so a name on it silently hides a future input of the same name."""
        leaves, _ = cli
        parser = sdp.build_parser()
        everywhere = {action.dest for action in parser._actions}
        for leaf in leaves.values():
            everywhere |= {action.dest for action in leaf._actions}

        assert verbs.PRESENTATION_FLAGS <= everywhere


class TestSocketInputsAreNarrowerThanTheCli:
    @pytest.mark.parametrize("name", ALL_VERBS)
    def test_socket_inputs_are_a_subset_of_what_the_cli_takes_or_asks(self, name):
        """Bug class: the socket accepting a field the command has no use for, which is either dead or an input someone invented past the CLI."""
        verb = verbs.VERBS[name]

        assert set(verb.socket_inputs) <= verb.cli_inputs | verb.prompted_inputs
        assert verb.cli_inputs & verb.prompted_inputs == set()

    @pytest.mark.parametrize("name", DISABLED_VERBS)
    def test_a_verb_that_is_not_available_has_no_inputs_and_says_why(self, name):
        """Bug class: inputs listed for a verb before the work that makes them safe is done, so flipping one flag later exposes them unreviewed; and a disabled verb with no stated reason, which invites that flip."""
        verb = verbs.VERBS[name]

        assert dict(verb.socket_inputs) == {}
        assert verb.check is None
        assert verb.note

    def test_enum_inputs_mirror_the_parsers_choices(self, cli):
        """Bug class: the literal choices in the registry drifting from the parser's, so the socket refuses a service the CLI knows or accepts one it dropped; and a destination with parser choices typed as anything looser than an Enum."""
        leaves, _ = cli
        compared = set()
        for name, verb in verbs.VERBS.items():
            actions = {action.dest: action for action in leaves[verb.cli]._actions}
            for field, spec in verb.socket_inputs.items():
                parser_choices = actions[field].choices if field in actions else None
                # An answer to a prompt (not a parser destination) has no parser choices to mirror.
                if isinstance(spec.type, types.Enum) and field in verb.cli_inputs:
                    assert parser_choices is not None, f"{name}.{field} is an Enum with nothing in the parser to mirror"
                    assert spec.type.choices == tuple(parser_choices), f"{name}.{field}"
                    compared.add((name, field))
                else:
                    assert parser_choices is None, f"{name}.{field} has parser choices but is not an Enum"

        assert {("db.verify", "db"), ("db.start", "service"), ("db.stop", "service"), ("run.start", "profile")} <= compared
        assert verbs.VERBS["run.start"].socket_inputs["profile"].type.choices == tuple(sdp.VALID_PROFILES)
        assert verbs.VALID_PROFILES == tuple(sdp.VALID_PROFILES)

    def test_a_path_a_port_or_free_text_only_where_listed(self):
        """Bug class: a type that accepts a host path, a port or arbitrary text reaching a verb nobody decided on, which turns an ordinary verb into root on the host. The set of type classes is closed and compared with the list in this file, and every input that uses a wide type, at any depth inside a container, is listed in WIDE_INPUTS."""
        def inner(kind):
            yield kind
            if isinstance(kind, types.ListOf):
                yield from inner(kind.item_type)
            elif isinstance(kind, types.MapOf):
                yield from inner(kind.key_type)
                yield from inner(kind.value_type)
            elif isinstance(kind, types.Record):
                for spec in kind.fields.values():
                    yield from inner(spec.type)

        used = {type(kind) for verb in verbs.VERBS.values() for spec in verb.socket_inputs.values() for kind in inner(spec.type)}
        wide = {(verb.name, name) for verb in verbs.VERBS.values() for name, spec in verb.socket_inputs.items() if any(type(kind).__name__ in WIDE_TYPE_NAMES for kind in inner(spec.type))}
        defined = {
            name for name, obj in vars(types).items()
            if isinstance(obj, type) and obj.__module__ == types.__name__ and hasattr(obj, "validate")
        }

        assert {kind.__name__ for kind in types.INPUT_TYPES} == ALLOWED_TYPE_NAMES
        assert defined == ALLOWED_TYPE_NAMES
        assert {kind.__name__ for kind in types.WIDE_TYPES} == WIDE_TYPE_NAMES
        assert used <= set(types.INPUT_TYPES)
        assert wide == WIDE_INPUTS
        # Decided with the maintainer: a path or a port from the browser needs the management password again.
        assert all(verbs.VERBS[name].cls == "step_up" for name, _ in WIDE_INPUTS)
        for name in ALLOWED_TYPE_NAMES - WIDE_TYPE_NAMES:
            assert not any(word in name.lower() for word in ("path", "dir", "port", "host", "url", "text", "string", "raw", "any"))

    def test_a_field_refuses_a_type_from_outside_the_set(self):
        """Bug class: an ad-hoc validator (a subclass or a look-alike object with a `validate` method) passed as a field type, bypassing the closed set."""
        class Anything(types.Name):
            def validate(self, value):
                return value

        for outsider in (Anything(), str, types.Name, object()):
            with pytest.raises(TypeError):
                verbs.Field(outsider)


# ── validate_request ────────────────────────────────────────────────────────


class TestDenyByDefault:
    @pytest.mark.parametrize("name", ENABLED_VERBS)
    def test_unlisted_fields_are_refused(self, source_tree, name):
        """Bug class: the boundary passing through whatever the client sends, so a new CLI flag (`data_dir`) or an input the CLI takes or asks but the socket does not list becomes reachable without review."""
        verb = verbs.VERBS[name]
        base = _minimal(verb)
        assert set(verbs.validate_request(name, dict(base))) == set(base)

        unlisted = sorted((verb.cli_inputs | verb.prompted_inputs) - set(verb.socket_inputs))
        for extra in ["data_dir", *unlisted]:
            for value in (True, "x", None):
                with pytest.raises(errors.InputError) as exc:
                    verbs.validate_request(name, {**base, extra: value})
                assert exc.value.field == extra
                assert exc.value.code == "bad_input"

    @pytest.mark.parametrize("name", DISABLED_VERBS)
    def test_a_verb_that_is_not_available_is_refused_whatever_it_carries(self, name):
        """Bug class: the availability switch checked after (or instead of) the fields, so a disabled verb runs when sent with no fields or answers differently depending on them."""
        verb = verbs.VERBS[name]
        for fields in ({}, {"data_dir": "/"}, {field: "x" for field in verb.cli_inputs | verb.prompted_inputs}, None):
            with pytest.raises(errors.VerbNotAvailable) as exc:
                verbs.validate_request(name, fields)
            assert exc.value.code == "not_available"

    def test_source_add_takes_no_hugging_face_dataset_yet(self):
        """Bug class: a Hugging Face source reachable from the browser before its folder names from the Hub are checked (F4b-2): `hf_dataset` is a CLI input the socket does not list."""
        request = {"name": "x", "platform": "custom", "data_types": ["posts"], "dumps_path": "d", "extracted_path": "e", "parsed_path": "p", "output_path": "o", "file_format": "parquet", "profiles": ["parse"]}
        assert verbs.validate_request("source.add", request)["name"] == "x"
        with pytest.raises(errors.InputError) as exc:
            verbs.validate_request("source.add", {**request, "hf_dataset": "user/data"})
        assert exc.value.field == "hf_dataset"

    @pytest.mark.parametrize("name, platform, refused", [("reddit", "reddit", False), ("x", "custom", False), ("reddit", "custom", True), ("x", "reddit", True)])
    def test_source_add_platform_follows_the_name(self, name, platform, refused):
        """Bug class: a request whose platform and name disagree, which the terminal cannot make: only the source named reddit is the Reddit platform (`source.run_questionnaire`), so a custom source named reddit would be parsed with the Reddit parser."""
        request = {"name": name, "platform": platform, "data_types": ["posts"], "dumps_path": "d", "extracted_path": "e", "parsed_path": "p", "output_path": "o", "file_format": "parquet", "profiles": ["parse"]}
        if not refused:
            assert verbs.validate_request("source.add", request)["platform"] == platform
            return
        with pytest.raises(errors.InputError) as exc:
            verbs.validate_request("source.add", request)
        assert exc.value.field == "platform"


class TestBoundary:
    @pytest.mark.parametrize("name, field", _inputs())
    def test_each_socket_input_accepts_a_valid_value(self, source_tree, name, field):
        """Bug class: a registry entry no honest request can satisfy (a type or cross-field rule that refuses the legitimate value). It is also the control for the refusals below: the same request with a good value passes."""
        verb = verbs.VERBS[name]
        value = _valid_value(verb.socket_inputs[field].type)

        result = verbs.validate_request(name, _request_with(verb, field, value))

        got = result[field]
        assert (got.reveal() if verb.socket_inputs[field].type.is_secret else got) == value

    @pytest.mark.parametrize("name, field", _inputs(only_strings=True))
    @pytest.mark.parametrize("value", LINE_BREAK_VALUES)
    def test_string_inputs_refuse_line_breaks(self, source_tree, name, field, value):
        """Bug class: a line break inside a value that later lands in .env or a log, where it writes a second setting or a forged line: `\\n`, `\\r`, the Unicode separators U+2028 and U+0085, and a trailing newline (what `re.match` with `$` lets through)."""
        verb = verbs.VERBS[name]

        with pytest.raises(errors.InputError) as exc:
            verbs.validate_request(name, _request_with(verb, field, value))

        assert exc.value.field == field
        assert value not in str(exc.value)

    @pytest.mark.parametrize("name, field", _inputs(only_plain_strings=True))
    @pytest.mark.parametrize("value", PATH_VALUES)
    def test_plain_string_inputs_refuse_paths(self, source_tree, name, field, value):
        """Bug class: a request addressing the host file system through an ordinary field: an absolute path, a parent reference or a nested path where a name, a choice or a file-id pattern is expected. (`../x` exists in the fixture tree and holds a platform.yaml.)"""
        verb = verbs.VERBS[name]

        with pytest.raises(errors.InputError) as exc:
            verbs.validate_request(name, _request_with(verb, field, value))

        assert exc.value.field == field
        assert value not in str(exc.value)

    def test_the_sweeps_cover_the_inputs_they_claim(self):
        """Bug class: a sweep that silently shrinks to nothing (a flag renamed, a verb disabled), leaving the boundary tests green and empty."""
        assert len(_inputs(only_strings=True)) >= 15
        assert ("run.start", "admin_password") in _inputs(only_strings=True)
        assert ("run.start", "admin_password") not in _inputs(only_plain_strings=True)
        assert {("run.start", "source"), ("run.start", "filter"), ("run.start", "classifier"), ("source.remove", "name")} <= set(_inputs(only_plain_strings=True))


class TestRequests:
    @pytest.mark.parametrize("name", ["db.nope", "", "status", "DB.STATUS", "db.status\n", "db status", None, 5, ["db.status"]])
    def test_unknown_verb(self, name):
        """Bug class: a name outside the registry reaching a dispatcher (a near miss, a trailing newline, a non-string that breaks the lookup instead of being refused)."""
        with pytest.raises(errors.UnknownVerb) as exc:
            verbs.validate_request(name, {})
        assert exc.value.code == "unknown_verb"

    @pytest.mark.parametrize("fields", [None, [], "verbose", 5, True, [("verbose", True)], {1: True}, {None: True}])
    def test_fields_must_be_an_object_with_string_keys(self, fields):
        """Bug class: a request body of the wrong shape (a list, a string, null, non-string keys) crashing the boundary or being read as "no fields"."""
        with pytest.raises(errors.InputError) as exc:
            verbs.validate_request("db.status", fields)
        assert exc.value.field is None

    @pytest.mark.parametrize("name, fields, missing", [
        ("run.start", {"profile": "parse"}, "source"),
        ("run.start", {"source": "ok"}, "profile"),
        ("db.reset", {}, "db"),
        ("db.unsetup", {}, "db"),
        ("source.remove", {}, "name"),
    ])
    def test_missing_required_field(self, source_tree, name, fields, missing):
        """Bug class: the socket falling back to the CLI's widest form when a field is left out: `run` picking a source by itself, `db unsetup` removing every database."""
        with pytest.raises(errors.InputError, match="is required") as exc:
            verbs.validate_request(name, fields)
        assert exc.value.field == missing

    def test_only_removal_accepts_a_source_folder_without_config(self, source_tree):
        """Bug class: the two source rules swapped between verbs: a half-written source (a folder with no platform.yaml) that can no longer be removed, or a run or status request accepted for a folder that has no config. A symlinked folder is refused for removal as well."""
        assert verbs.validate_request("source.remove", {"name": "bare"}) == {"name": "bare"}

        for name, fields, field in [
            ("source.status", {"name": "bare"}, "name"),
            ("source.error_logs", {"name": "bare"}, "name"),
            ("run.start", {"profile": "parse", "source": "bare"}, "source"),
            ("source.remove", {"name": "linked"}, "name"),
        ]:
            with pytest.raises(errors.InputError) as exc:
                verbs.validate_request(name, fields)
            assert exc.value.field == field

    def test_absent_optional_fields_stay_absent(self, source_tree):
        """Bug class: the boundary inventing defaults, so the action cannot tell "not sent" from "sent"."""
        assert verbs.validate_request("db.status", {}) == {}
        assert verbs.validate_request("db.start", {}) == {}
        assert verbs.validate_request("run.start", {"profile": "parse", "source": "ok"}) == {"profile": "parse", "source": "ok"}

    @pytest.mark.parametrize("name, field", _inputs())
    def test_none_is_a_refused_value_not_an_absent_field(self, source_tree, name, field):
        """Bug class: `null` treated as "not sent", which lets a client blank a field the action then reads as its widest default."""
        verb = verbs.VERBS[name]

        with pytest.raises(errors.InputError) as exc:
            verbs.validate_request(name, _request_with(verb, field, None))
        assert exc.value.field == field

    @pytest.mark.parametrize("extra, refused_field", [
        ({"profile": "parse", "filter": "RS_2024-*"}, None),
        ({"profile": "mongo_ingest", "filter": "RS_2024-*"}, None),
        ({"profile": "sr_ingest", "filter": "RS_2024-*"}, "filter"),
        ({"profile": "sr_ml", "filter": "RS_2024-*"}, "filter"),
        ({"profile": "parse", "skip_lingua_files": True}, None),
        ({"profile": "lingua", "skip_lingua_files": False}, None),
        ({"profile": "lingua", "skip_lingua_files": True}, "skip_lingua_files"),
        ({"profile": "sr_ml", "skip_lingua_files": True}, "skip_lingua_files"),
        ({"profile": "ml", "classifier": "toxic_roberta"}, None),
        ({"profile": "lingua", "classifier": "toxic_roberta"}, None),
        ({"profile": "sr_ml", "classifier": "toxic_roberta"}, None),
        ({"profile": "parse", "classifier": "toxic_roberta"}, "classifier"),
        ({"profile": "postgres_ml", "classifier": "toxic_roberta"}, "classifier"),
        ({"profile": "sr_ingest", "classifier": "toxic_roberta"}, "classifier"),
    ])
    def test_run_start_cross_field_rules(self, source_tree, extra, refused_field):
        """Bug class: a field accepted with a profile that ignores it, so the run does something other than what the request says (a filter dropped by the StarRocks profiles, a classifier or skip flag on a profile that has none); and the opposite slip, a rule that refuses the combination it exists for."""
        request = {"source": "ok", **extra}

        if refused_field is None:
            assert verbs.validate_request("run.start", request) == request
        else:
            with pytest.raises(errors.InputError) as exc:
                verbs.validate_request("run.start", request)
            assert exc.value.field == refused_field

    def test_result_is_a_new_dict_and_the_request_is_untouched(self, source_tree):
        """Bug class: validating in place, so the caller's raw request (with the bare secret) and the validated one are the same object."""
        request = {"profile": "mongo_ingest", "source": "ok", "admin_password": "hunter2-ZQX"}
        before = dict(request)

        result = verbs.validate_request("run.start", request)

        assert result is not request
        assert request == before


class TestSecretsNeverShow:
    def test_validated_request_hides_the_secret(self, source_tree, capsys):
        """Bug class: a password in a log line or traceback because the validated request was printed or formatted; and a boundary that prints what it handles."""
        result = verbs.validate_request("run.start", {"profile": "mongo_ingest", "source": "ok", "admin_password": "hunter2-ZQX"})

        assert "hunter2-ZQX" not in repr(result)
        assert "hunter2-ZQX" not in str(result)
        assert "admin_password" in repr(result)
        assert isinstance(result["admin_password"], types.SecretValue)
        assert result["admin_password"].reveal() == "hunter2-ZQX"
        captured = capsys.readouterr()
        assert captured.out == "" and captured.err == ""

    @pytest.mark.parametrize("name", ["run.start", "db.start"])
    @pytest.mark.parametrize("secret", ["hunter2-ZQX\nPOSTGRES_PORT=1", "hunter2-ZQX" * 200, ["hunter2-ZQX"]])
    def test_error_for_a_bad_secret_does_not_contain_it(self, source_tree, capsys, name, secret):
        """Bug class: the refusal of a malformed secret quoting it, which writes the password (or most of it) into the error shown to the client and kept in logs."""
        verb = verbs.VERBS[name]

        with pytest.raises(errors.InputError) as exc:
            verbs.validate_request(name, _request_with(verb, "admin_password", secret))

        error = exc.value
        for shown in (str(error), repr(error), error.message, repr(error.args)):
            assert "hunter2" not in shown
        assert error.__cause__ is None and error.__suppress_context__
        assert error.field == "admin_password"
        captured = capsys.readouterr()
        assert captured.out == "" and captured.err == ""

    @pytest.mark.parametrize("build", [
        lambda: verbs.validate_request("ZQX9marker\nx", {}),
        lambda: verbs.validate_request("ZQX9marker" * 10, {}),
        lambda: verbs.validate_request("db.status", {"ZQX9marker\nx": True}),
        lambda: verbs.validate_request("db.status", {"ZQX9marker" * 10: True}),
        lambda: verbs.validate_request("db.status", {"verbose": "ZQX9marker"}),
        lambda: verbs.validate_request("db.verify", {"db": "ZQX9marker"}),
        lambda: verbs.validate_request("source.status", {"name": "ZQX9marker"}),
        lambda: verbs.validate_request("db.status", "ZQX9marker"),
    ])
    def test_errors_do_not_echo_what_the_client_wrote(self, source_tree, build):
        """Bug class: log and page injection through an error message: a verb name, a field name or a value written by the client, with a line break or at any length, quoted back in the refusal."""
        with pytest.raises(errors.ControlError) as exc:
            build()

        assert "ZQX9marker" not in str(exc.value)
        assert "\n" not in str(exc.value)


# ── where the package sits ──────────────────────────────────────────────────


class TestImports:
    @pytest.mark.parametrize("statement, forbidden", [
        ("import social_data_pipeline.control.verbs", ("sdp", "social_data_pipeline.runstore", "social_data_pipeline.control.child", "social_data_pipeline.control.supervise", "social_data_pipeline.control.runs", "social_data_pipeline.db", "social_data_pipeline.jobs", "social_data_pipeline.orchestrators", "social_data_pipeline.classifiers", "social_data_pipeline.core", "social_data_pipeline.platforms")),
        ("import sdp", ("social_data_pipeline.control", "social_data_pipeline.runstore")),
        ("import social_data_pipeline.control.child", ("sdp", "social_data_pipeline.runstore", "social_data_pipeline.control.supervise", "social_data_pipeline.control.runs", "social_data_pipeline.db")),
        ("import social_data_pipeline.control.supervise", ("sdp", "yaml", "social_data_pipeline.setup", "social_data_pipeline.control.verbs", "social_data_pipeline.control.types", "social_data_pipeline.control.child", "social_data_pipeline.control.runs", "social_data_pipeline.db")),
        ("import social_data_pipeline.control.runs", ("sdp", "social_data_pipeline.control.child", "social_data_pipeline.control.supervise", "social_data_pipeline.db")),
    ], ids=["registry", "cli", "child", "supervise", "runs"])
    def test_import_boundaries(self, statement, forbidden):
        """Bug class: an import cycle between the CLI script and the registry, or the host-side registry pulling in container-only modules (database drivers) that are not installed on the host. Run in a fresh interpreter, since this process has already imported both. The same for the three modules that act on a request: the CLI loads none of them (and not the run store), the registry loads neither the CLI nor the run store, each of the three loads the CLI only when it calls it, and the supervisor, which lives as long as a run, loads the run store and nothing of the registry."""
        code = (
            f"import sys; {statement}; "
            f"bad = sorted(m for m in sys.modules if any(m == f or m.startswith(f + '.') for f in {forbidden!r})); "
            "print(bad); sys.exit(1 if bad else 0)"
        )

        result = subprocess.run([sys.executable, "-c", code], cwd=REPO_ROOT, capture_output=True, text=True)

        assert result.returncode == 0, result.stdout + result.stderr
