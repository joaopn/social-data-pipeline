"""The verb registry and `validate_request`, the one function a request passes before anything acts on it.

A verb names one subcommand of the host CLI. `socket_inputs` is the deny-by-default list of typed fields the socket accepts for it, narrower than what the CLI takes (`cli_inputs`) and asks (`prompted_inputs`): a field that is not listed is refused, so a new CLI flag stays unreachable until someone lists it here with a type from `control.types`. A field may be a container (`ListOf`, `MapOf`, `Record`): it validates what it holds itself, refuses unknown keys at every level and wraps nested secrets, and its errors name the place inside it ("field 'targets' item 2 key 'name' is required").

The CLI is a top-level script module and is never imported here (that would be an import cycle for the CLI and would pull the whole CLI into any process that only validates requests). The choices it shares with the registry are written out as literals below.
"""

import re
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Callable, Mapping

from social_data_pipeline.control.errors import InputError, UnknownVerb, VerbNotAvailable
# `Field` lives in `types` (a Record's keys are Fields too) and is used here as `verbs.Field`.
from social_data_pipeline.control.types import (
    Bool,
    ConfiguredSource,
    Enum,
    Field,
    FileFilter,
    IntRange,
    Name,
    RunId,
    Secret,
    SecretValue,
)

VERB_CLASSES = ("read", "write", "step_up")

# CLI destinations that are about terminal output. They are never verb inputs.
PRESENTATION_FLAGS = frozenset({"json", "tag"})

# Subcommand paths that have no verb, each with the reason.
TERMINAL_ONLY = MappingProxyType({
    ("db", "setup-llm"): "A bundle of two other commands (db setup-mcp, then db setup-jobs); each has its own verb.",
    ("broker", "start"): "Starts the broker itself, which serves the socket; a request arrives through the broker.",
    ("broker", "stop"): "Stops the broker itself, which serves the socket; a request arrives through the broker.",
    ("broker", "status"): "Says whether the broker runs, which only someone outside the broker needs to ask.",
    ("broker", "passwd"): "Sets the management password, which a session must never be able to change.",
    ("webui", "setup"): "Sets up the web UI itself (its addresses, names and certificate), which a session of that web UI must not be able to change.",
    ("webui", "start"): "Starts the web UI's container, which a session can only exist in once it runs.",
    ("webui", "stop"): "Stops the web UI's container; a session would stop the page it is using.",
    ("webui", "status"): "Says whether the web UI's container runs, which only someone outside the web UI needs to ask.",
    ("webui", "unsetup"): "Removes the web UI itself, which a session of that web UI must not be able to do.",
    ("chat", "setup"): "Asks for the model's secret at the terminal, without echo; a session must never be able to set or see it.",
    ("chat", "start"): "Starts the chat's gateway, which holds the model's secret, and its sandbox; the web UI's Chat page is to work with a chat started at the terminal.",
    ("chat", "stop"): "Stops the chat's containers and detaches the databases from it, which only the operator at the terminal decides.",
    ("chat", "status"): "Says whether the chat's containers run and which databases they can read, which the operator asks at the terminal.",
    ("chat", "unsetup"): "Deletes the model's secret and the chat's settings (and, if asked, all chats), which a session must not be able to do.",
})

# Literal copies of the CLI parser's choices.
VALID_PROFILES = ("parse", "lingua", "ml", "postgres_ingest", "postgres_ml", "mongo_ingest", "sr_ingest", "sr_ml")
DATABASES = ("postgres", "mongo", "starrocks")
SERVICES = ("postgres", "mongo", "starrocks", "postgres-mcp", "mongo-mcp", "starrocks-mcp", "jobs")
ERROR_LOG_PROFILES = ("postgres_ingest", "postgres_ml", "mongo_ingest")


class FieldRefused(ValueError):
    """Raised by a verb's cross-field check: names the field at fault and gives a reason that holds no field value."""

    def __init__(self, field_name, reason):
        super().__init__(reason)
        self.field = field_name


@dataclass(frozen=True)
class Verb:
    """One named action. `cli` is the subcommand path, `cli_inputs` its argparse destinations, `prompted_inputs` what the command asks interactively today, `socket_inputs` the fields accepted over the socket, `action` the name of the CLI module's function that runs the verb without a terminal (None while there is none)."""

    name: str
    cls: str
    cli: tuple
    cli_inputs: frozenset = frozenset()
    prompted_inputs: frozenset = frozenset()
    socket_inputs: Mapping = field(default_factory=dict)
    # Off unless a verb says otherwise, so a verb added without thought is not reachable.
    socket_enabled: bool = False
    check: Callable | None = None
    note: str = ""
    # A name, not the function: the CLI module is never imported here. Its keyword parameters are the socket inputs.
    action: str | None = None

    def __post_init__(self):
        object.__setattr__(self, "cli", tuple(self.cli))
        object.__setattr__(self, "cli_inputs", frozenset(self.cli_inputs))
        object.__setattr__(self, "prompted_inputs", frozenset(self.prompted_inputs))
        object.__setattr__(self, "socket_inputs", MappingProxyType(dict(self.socket_inputs)))


def _check_run_start(fields):
    """Cross-field rules of `run`: the two the CLI enforces itself, and the file filter on the StarRocks profiles."""
    profile = fields["profile"]
    # The StarRocks services are not handed the file filter, so one sent with them would be dropped without a word.
    if "filter" in fields and profile in ("sr_ingest", "sr_ml"):
        raise FieldRefused("filter", "is not accepted with the profiles sr_ingest and sr_ml")
    if fields.get("skip_lingua_files") is True and profile != "parse":
        raise FieldRefused("skip_lingua_files", "can be true only with the profile parse")
    if "classifier" in fields and profile not in ("ml", "lingua", "sr_ml"):
        raise FieldRefused("classifier", "is accepted only with the profiles ml, lingua and sr_ml")


_SETUP_NOTE = "Setup becomes available when its questions have typed inputs."


def _registry(*verbs):
    by_name = {}
    for verb in verbs:
        if verb.name in by_name:
            raise ValueError(f"verb {verb.name} is defined twice")
        by_name[verb.name] = verb
    return MappingProxyType(by_name)


VERBS = _registry(
    # ---- read ----
    Verb(
        name="db.status", cls="read", cli=("db", "status"),
        cli_inputs={"verbose"},
        socket_inputs={"verbose": Field(Bool())},
        socket_enabled=True,
        action="get_db_status",
    ),
    Verb(
        name="source.list", cls="read", cli=("source", "list"),
        socket_enabled=True,
        action="get_source_list",
    ),
    Verb(
        name="db.verify", cls="read", cli=("db", "verify"),
        cli_inputs={"db"},
        socket_inputs={"db": Field(Enum(DATABASES))},
        socket_enabled=True,
        action="get_db_verify",
    ),
    Verb(
        name="source.status", cls="read", cli=("source", "status"),
        cli_inputs={"name"},
        socket_inputs={"name": Field(ConfiguredSource())},
        socket_enabled=True,
        action="get_source_status",
    ),
    Verb(
        name="source.progress", cls="read", cli=("source", "progress"),
        cli_inputs={"name"},
        socket_inputs={"name": Field(ConfiguredSource())},
        socket_enabled=True,
        action="get_source_progress",
    ),
    Verb(
        name="source.error_logs", cls="read", cli=("source", "error-logs"),
        cli_inputs={"name", "profile"},
        socket_inputs={"name": Field(ConfiguredSource()), "profile": Field(Enum(ERROR_LOG_PROFILES))},
        socket_enabled=True,
        action="get_source_error_logs",
    ),
    Verb(
        name="run.list", cls="read", cli=("runs", "list"),
        cli_inputs={"limit"},
        socket_inputs={"limit": Field(IntRange(1, 1000))},
        socket_enabled=True,
        action="get_run_list",
    ),
    Verb(
        name="run.logs", cls="read", cli=("runs", "logs"),
        # tail and follow are how the terminal prints a log; a client reads pieces by offset.
        cli_inputs={"run_id", "offset", "limit", "tail", "follow"},
        socket_inputs={"run_id": Field(RunId(), required=True), "offset": Field(IntRange(0, 2**53)), "limit": Field(IntRange(1, 1048576))},
        socket_enabled=True,
        action="get_run_logs",
    ),
    Verb(
        name="audit.list", cls="read", cli=("broker", "audit"),
        # verify checks the whole file, which the terminal does; a client reads the newest lines.
        cli_inputs={"limit", "verify"},
        socket_inputs={"limit": Field(IntRange(1, 1000))},
        socket_enabled=True,
        note="Answered by the broker itself from its audit log: there is no action in the CLI.",
    ),
    # ---- write ----
    Verb(
        name="run.start", cls="write", cli=("run",),
        cli_inputs={"profile", "source", "build", "filter", "skip_lingua_files", "classifier"},
        prompted_inputs={"admin_password"},
        socket_inputs={
            "profile": Field(Enum(VALID_PROFILES), required=True),
            # Required: the CLI picks the only configured source when none is given, the socket never picks one.
            "source": Field(ConfiguredSource(), required=True),
            "build": Field(Bool()),
            "filter": Field(FileFilter()),
            "skip_lingua_files": Field(Bool()),
            "classifier": Field(Name()),
            "admin_password": Field(Secret()),
        },
        socket_enabled=True,
        check=_check_run_start,
        action="do_run_start",
    ),
    Verb(
        name="db.stop", cls="write", cli=("db", "stop"),
        cli_inputs={"service"},
        socket_inputs={"service": Field(Enum(SERVICES))},
        socket_enabled=True,
        action="do_db_stop",
    ),
    Verb(
        name="run.cancel", cls="write", cli=("runs", "cancel"),
        cli_inputs={"run_id"},
        socket_inputs={"run_id": Field(RunId(), required=True)},
        socket_enabled=True,
        action="do_run_cancel",
    ),
    Verb(
        name="source.download", cls="write", cli=("source", "download"),
        cli_inputs={"name", "token", "data_type"},
        note="Not available until the folder names built from the Hub's reply are checked.",
    ),
    # ---- step-up ----
    Verb(
        name="db.start", cls="step_up", cli=("db", "start"),
        cli_inputs={"service"},
        prompted_inputs={"admin_password"},
        socket_inputs={"service": Field(Enum(SERVICES)), "admin_password": Field(Secret())},
        socket_enabled=True,
        note="Step-up until the PostgreSQL start checks the admin password instead of setting it; then write.",
        action="do_db_start",
    ),
    Verb(
        name="db.reset", cls="step_up", cli=("db", "reset"),
        cli_inputs={"db"},
        prompted_inputs={"confirmation"},
        socket_inputs={"db": Field(Enum(DATABASES), required=True)},
        socket_enabled=True,
        action="do_db_reset",
    ),
    Verb(
        name="db.unsetup", cls="step_up", cli=("db", "unsetup"),
        cli_inputs={"db"},
        prompted_inputs={"confirmation"},
        # Required: without a database the CLI removes all of them and asks a data choice for each, which stays terminal-only.
        socket_inputs={"db": Field(Enum(DATABASES), required=True)},
        socket_enabled=True,
        action="do_db_unsetup",
    ),
    Verb(
        name="db.unsetup_mcp", cls="step_up", cli=("db", "unsetup-mcp"),
        socket_enabled=True,
        action="do_db_unsetup_mcp",
    ),
    Verb(
        name="db.unsetup_jobs", cls="step_up", cli=("db", "unsetup-jobs"),
        socket_enabled=True,
        action="do_db_unsetup_jobs",
    ),
    Verb(
        name="source.remove", cls="step_up", cli=("source", "remove"),
        cli_inputs={"name"},
        prompted_inputs={"confirmation"},
        socket_inputs={"name": Field(ConfiguredSource(require_config=False), required=True)},
        socket_enabled=True,
        action="do_source_remove",
    ),
    Verb(
        name="db.setup", cls="step_up", cli=("db", "setup"),
        cli_inputs={"add"},
        prompted_inputs={"questionnaire", "new_password"},
        note=_SETUP_NOTE,
    ),
    Verb(
        name="db.setup_mcp", cls="step_up", cli=("db", "setup-mcp"),
        prompted_inputs={"questionnaire"},
        note=_SETUP_NOTE,
    ),
    Verb(
        name="db.setup_jobs", cls="step_up", cli=("db", "setup-jobs"),
        prompted_inputs={"questionnaire"},
        note=_SETUP_NOTE,
    ),
    Verb(
        name="source.add", cls="step_up", cli=("source", "add"),
        cli_inputs={"name", "hf_dataset"},
        prompted_inputs={"questionnaire"},
        note=_SETUP_NOTE,
    ),
    Verb(
        name="source.configure", cls="step_up", cli=("source", "configure"),
        cli_inputs={"name"},
        prompted_inputs={"questionnaire"},
        note=_SETUP_NOTE,
    ),
    Verb(
        name="source.add_classifiers", cls="step_up", cli=("source", "add-classifiers"),
        cli_inputs={"name"},
        prompted_inputs={"questionnaire"},
        note=_SETUP_NOTE,
    ),
    Verb(
        name="db.create_indexes", cls="step_up", cli=("db", "create-indexes"),
        cli_inputs={"source"},
        prompted_inputs={"questionnaire", "admin_password"},
        note="Not available until index fields are validated against the database.",
    ),
    Verb(
        name="db.recover_password", cls="step_up", cli=("db", "recover-password"),
        cli_inputs={"regenerate_ro"},
        prompted_inputs={"new_password"},
        note="Terminal-only until recovery stops opening PostgreSQL to the network.",
    ),
)


# A name is quoted in an error only when it has this shape: an unknown verb or field name is text the client wrote.
_QUOTABLE_NAME = r"[A-Za-z][A-Za-z0-9_.-]{0,63}"


def _quotable(name):
    """Return name when it is safe to quote in an error message, else None."""
    if isinstance(name, str) and re.fullmatch(_QUOTABLE_NAME, name):
        return name
    return None


def validate_request(verb_name, fields):
    """Return the validated fields of a request for verb_name, as a new dict with secrets wrapped in SecretValue.

    `fields` is the object decoded from the request's JSON. Raises UnknownVerb, VerbNotAvailable or InputError; nothing is logged or printed, and no error message holds a field's value. A field that is not in the verb's socket_inputs is refused. An optional field that is absent stays absent from the result: no default is invented, and None is a refused value, not an absent field.
    """
    verb = VERBS.get(verb_name) if isinstance(verb_name, str) else None
    if verb is None:
        quoted = _quotable(verb_name)
        raise UnknownVerb(f"Unknown verb '{quoted}'." if quoted else "Unknown verb (its name is not a well-formed verb name).")
    if not verb.socket_enabled:
        reason = f" {verb.note}" if verb.note else ""
        raise VerbNotAvailable(f"{verb.name} is not available over the socket.{reason}")

    if not isinstance(fields, dict) or not all(isinstance(key, str) for key in fields):
        raise InputError(f"{verb.name}: the fields must be an object with string keys.")

    # Deny by default, before anything else is looked at.
    for name in fields:
        if name not in verb.socket_inputs:
            quoted = _quotable(name)
            label = f"field '{quoted}'" if quoted else "a field with a malformed name"
            raise InputError(f"{verb.name}: {label} is not accepted.", field=quoted)

    validated = {}
    for name, spec in verb.socket_inputs.items():
        if name not in fields:
            if spec.required:
                raise InputError(f"{verb.name}: field '{name}' is required.", field=name)
            continue
        try:
            value = spec.type.validate(fields[name])
        except ValueError as exc:
            raise InputError(f"{verb.name}: field '{name}' {exc}.", field=name) from None
        validated[name] = SecretValue(value) if spec.type.is_secret else value

    if verb.check is not None:
        try:
            verb.check(validated)
        except FieldRefused as exc:
            raise InputError(f"{verb.name}: field '{exc.field}' {exc}.", field=exc.field) from None
        except ValueError as exc:
            raise InputError(f"{verb.name}: {exc}.", field=None) from None

    return validated
