"""The verb registry and `validate_request`, the one function a request passes before anything acts on it.

A verb names one subcommand of the host CLI. `socket_inputs` is the deny-by-default list of typed fields the socket accepts for it, narrower than what the CLI takes (`cli_inputs`) and asks (`prompted_inputs`): a field that is not listed is refused, so a new CLI flag stays unreachable until someone lists it here with a type from `control.types`. A field may be a container (`ListOf`, `MapOf`, `Record`): it validates what it holds itself, refuses unknown keys at every level and wraps nested secrets, and its errors name the place inside it ("field 'targets' item 2 key 'name' is required").

The CLI is a top-level script module and is never imported here (that would be an import cycle for the CLI and would pull the whole CLI into any process that only validates requests). The choices it shares with the registry are written out as literals below.
"""

import re
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Callable, Mapping

from social_data_pipeline.control import dbsetup
from social_data_pipeline.control.errors import InputError, UnknownVerb, VerbNotAvailable
# `Field` lives in `types` (a Record's keys are Fields too) and is used here as `verbs.Field`.
from social_data_pipeline.control.types import (
    Bool,
    ConfiguredSource,
    Enum,
    Field,
    FileFilter,
    HfDatasetId,
    IntRange,
    ListOf,
    MapOf,
    Name,
    Path,
    Port,
    Record,
    RunId,
    Secret,
    SecretValue,
    Text,
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
    ("webui", "rebuild"): "Rebuilds and restarts the web UI's container; a session would stop the page it is using.",
    ("webui", "status"): "Says whether the web UI's container runs, which only someone outside the web UI needs to ask.",
    ("webui", "unsetup"): "Removes the web UI itself, which a session of that web UI must not be able to do.",
    ("chat", "setup"): "Asks for the model's secret at the terminal, without echo; a session must never be able to set or see it.",
    ("chat", "start"): "Starts the chat's gateway, which holds the model's secret, and its sandbox; the web UI's Chat page is to work with a chat started at the terminal.",
    ("chat", "stop"): "Stops the chat's containers and detaches the databases from it, which only the operator at the terminal decides.",
    ("chat", "rebuild"): "Rebuilds and restarts the chat's gateway, which holds the model's secret, and its sandbox, which only the operator at the terminal decides.",
    ("chat", "status"): "Says whether the chat's containers run and which databases they can read, which the operator asks at the terminal.",
    ("chat", "unsetup"): "Deletes the model's secret and the chat's settings (and, if asked, all chats), which a session must not be able to do.",
})

# Literal copies of the CLI parser's choices.
VALID_PROFILES = ("parse", "lingua", "ml", "postgres_ingest", "postgres_ml", "mongo_ingest", "sr_ingest", "sr_ml")
DATABASES = ("postgres", "mongo", "starrocks")
SERVICES = ("postgres", "mongo", "starrocks", "postgres-mcp", "mongo-mcp", "starrocks-mcp", "jobs")
ERROR_LOG_PROFILES = ("postgres_ingest", "postgres_ml", "mongo_ingest")
# The largest query time limit of the chat's MCP servers, in seconds (`chat_host.QUERY_SECONDS_MAX`; a test compares them).
CHAT_QUERY_SECONDS_MAX = 3600


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


def _check_setup_jobs(fields):
    """The scheduler's targets are one map by name: two targets of one name would leave one of them out of config/jobs/config.local.yaml without a word."""
    seen = set()
    for key in ("postgres_target", "starrocks_target", "mongo_target"):
        if key in fields:
            if fields[key]["name"] in seen:
                raise FieldRefused(key, "has the name of another target")
            seen.add(fields[key]["name"])


# The inputs of `db setup-mcp` and `db setup-jobs`, one per question the terminal asks: a server or a target that is present is one the operator switched on (the terminal asks nothing else of one switched off). The bounds of the numbers the terminal takes without one are the web UI's choice: at most 1000 queries at once, 100000 jobs of history, a year (8760 hours) of timeout; StarRocks's 1 to 72 hours are the terminal's own.
_MCP_SERVER = Record({"port": Field(Port(), required=True), "write_access": Field(Bool())})
_MCP_STARROCKS = Record({"port": Field(Port(), required=True)})
_HOURS = IntRange(0, 8760)
_JOBS_INPUTS = {
    "port": Field(Port(), required=True),
    "result_root": Field(Path(), required=True),
    "max_concurrent": Field(IntRange(1, 1000), required=True),
    "history_retention": Field(IntRange(1, 100000), required=True),
    "postgres_target": Field(Record({"name": Field(Name(), required=True), "database": Field(Name(), required=True), "timeout_hours": Field(_HOURS, required=True)})),
    "starrocks_target": Field(Record({"name": Field(Name(), required=True), "timeout_hours": Field(IntRange(1, 72), required=True)})),
    "mongo_target": Field(Record({"name": Field(Name(), required=True), "timeout_hours": Field(_HOURS, required=True)})),
    "auth_enabled": Field(Bool()),
}


def _check_source_add(fields):
    """The one cross-field rule of `source add` that needs no file: the platform comes from the name (only the source named `reddit` is the Reddit platform). Every other rule (what is asked for the profiles chosen, the folders of other sources) is the action's, which reads the deployment."""
    if (fields["platform"] == "reddit") != (fields["name"] == "reddit"):
        raise FieldRefused("platform", "must be reddit exactly for the source named reddit")
    # A Hugging Face dataset comes with its grouping, and its data types are the groups' names, in their order (the terminal asks the names in the grouping).
    if ("hf_dataset" in fields) != ("hf_groups" in fields):
        raise FieldRefused("hf_groups", "goes with hf_dataset, and hf_dataset with it")
    if "hf_groups" in fields:
        if fields["platform"] != "custom":
            raise FieldRefused("hf_dataset", "is taken for a source of the platform custom only")
        if [group["data_type"] for group in fields["hf_groups"]] != fields["data_types"]:
            raise FieldRefused("data_types", "must be the names of the Hugging Face groups, in their order")


# The questions of `source add` for Reddit dumps, a Hugging Face dataset and your own files (and the Reddit questions of `source configure reddit`), each one typed input of `source.add`; an optional one left out is the question answered with Enter (the terminal's default, computed by the action on the host: a folder left out is `<DATA_PATH>/<stage>/<name>` with .env's DATA_PATH, as the terminal offers it). Sizes: at most 32 data types, 256 fields per data type, 64 index columns and 64 text columns per data type.
_DATA_TYPE = Name()
# The SQL types a Hugging Face field can be given: those `setup.hf.map_hf_type_to_sql` maps the Hub's types to, which are also the ones the database loaders know (any other is loaded as text).
HF_SQL_TYPES = ("text", "integer", "bigint", "float", "boolean")
_NAMES = ListOf(Name(), 64, unique=True)
_REDDIT_KIND = Enum(("submissions", "comments"))
_WORKERS = IntRange(1, 1024)
SOURCE_ADD_QUESTIONS = {
    "platform": Field(Enum(("custom", "reddit")), required=True),
    "data_types": Field(ListOf(_DATA_TYPE, 32, unique=True), required=True),
    "dumps_path": Field(Path()),
    "extracted_path": Field(Path()),
    "parsed_path": Field(Path()),
    "output_path": Field(Path()),
    "file_format": Field(Enum(("parquet", "csv")), required=True),
    "parquet_row_group_size": Field(IntRange(1, 1_000_000_000)),
    "profiles": Field(ListOf(Enum(VALID_PROFILES), len(VALID_PROFILES), unique=True), required=True),
    "parse_workers": Field(_WORKERS),
    "pg_prefer_lingua": Field(Bool()),
    "pg_parallel_index_workers": Field(IntRange(0, 1024)),
    "table_tablespaces": Field(MapOf(_DATA_TYPE, Name(), 32)),
    "db_schema": Field(Name()),
    "input_format": Field(Enum(("ndjson", "csv"))),
    "csv_delimiter": Field(Enum(("comma", "tab", "pipe", "semicolon"))),
    "dump_globs": Field(MapOf(_DATA_TYPE, FileFilter(), 32)),
    "compressions": Field(MapOf(_DATA_TYPE, Enum(("zst", "gz", "xz", "tar.gz")), 32)),
    "fields": Field(MapOf(_DATA_TYPE, ListOf(Name(), 256, unique=True), 32)),
    "mongo_collection_strategy": Field(Enum(("per_data_type", "per_file"))),
    "mongo_collections": Field(MapOf(_DATA_TYPE, Name(), 32)),
    "mongo_db_name": Field(Name()),
    "pg_indexes": Field(MapOf(_DATA_TYPE, _NAMES, 32)),
    "mongo_indexes": Field(MapOf(_DATA_TYPE, _NAMES, 32)),
    "sr_indexes": Field(MapOf(_DATA_TYPE, _NAMES, 32)),
    "primary_key": Field(Name()),
    "sr_prefer_lingua": Field(Bool()),
    "sr_buckets": Field(IntRange(1, 65536)),
    "reddit_db_schema": Field(Name()),
    "reddit_removed_fields": Field(MapOf(_REDDIT_KIND, _NAMES, 2)),
    "reddit_indexes": Field(MapOf(_REDDIT_KIND, _NAMES, 2)),
    "text_columns": Field(MapOf(_DATA_TYPE, _NAMES, 32)),
    "remove_strings": Field(ListOf(Text(256, multiline=False), 64)),
    "remove_patterns": Field(ListOf(Text(512, multiline=False), 64)),
    "gpu_fields": Field(_NAMES),
    "lingua_workers": Field(_WORKERS),
    "lingua_file_workers": Field(_WORKERS),
    "lingua_batch_size": Field(IntRange(1, 100_000_000)),
    "lingua_low_accuracy": Field(Bool()),
    "gpu_ids": Field(ListOf(IntRange(0, 255), 64, unique=True)),
    "ml_file_workers": Field(_WORKERS),
    "ml_tokenize_workers": Field(IntRange(0, 1024)),
    "ml_classifier_batch_size": Field(IntRange(1, 65536)),
    "ml_classifiers": Field(_NAMES),
    "ml_classifier_data_types": Field(MapOf(Name(), ListOf(_DATA_TYPE, 32, unique=True), 64)),
    "hf_token": Field(Secret()),
    # A Hugging Face dataset's grouping (`source._run_hf_config_grouping`), one entry per schema group of the Hub's reply as `source.hf_inspect` read it: the configs included, the data type's name, the fields chosen with their SQL type (the Hub's type mapped, or for a type without a mapping the type chosen in its place), and the group's other fields (left out of the data type, and of MongoDB's documents). The schema travels in the request: the add asks the Hub nothing, so a private dataset's token is not one of its inputs.
    "hf_groups": Field(ListOf(Record({
        "configs": Field(ListOf(Name(), 256, unique=True), required=True),
        "data_type": Field(_DATA_TYPE, required=True),
        "fields": Field(ListOf(Record({"name": Field(Name(), required=True), "sql_type": Field(Enum(HF_SQL_TYPES), required=True)}), 256), required=True),
        "excluded": Field(ListOf(Name(), 256, unique=True)),
    }), 8)),
}


# The questions of `source configure <name>` (`source.main` answered yes at "Reconfigure?"): those of `source add` but the origin (it follows from the name) and a Hugging Face dataset's grouping (a Hugging Face source keeps its own). Each is optional, its default the source's current value, but the profiles: Enter there keeps only the profiles not configured yet, which the browser never means, so they are always given.
SOURCE_CONFIGURE_QUESTIONS = {key: Field(spec.type, required=key == "profiles") for key, spec in SOURCE_ADD_QUESTIONS.items() if key not in ("platform", "hf_groups")}
# The questions of `source add-classifiers <name>` (`classifiers.run_questionnaire`): the text columns and what is removed before classification (not asked for Reddit), the Lingua settings, the GPUs and GPU classifiers, and the Hugging Face token the terminal writes to .env as HF_TOKEN.
SOURCE_CLASSIFIER_QUESTIONS = {key: SOURCE_ADD_QUESTIONS[key] for key in (
    "text_columns", "remove_strings", "remove_patterns", "gpu_fields", "lingua_workers", "lingua_file_workers", "lingua_batch_size", "lingua_low_accuracy",
    "gpu_ids", "ml_file_workers", "ml_tokenize_workers", "ml_classifier_batch_size", "ml_classifiers", "ml_classifier_data_types", "hf_token",
)}


def _check_db_setup(fields):
    """Cross-field rules of `db setup` (`control.dbsetup.check`): the first-time inputs or `add`, each database's inputs with its database only, the passwords typed twice alike, the names and paths the generated files can hold."""
    try:
        dbsetup.check(fields)
    except dbsetup.Refused as exc:
        raise FieldRefused(exc.field, str(exc)) from None


# The answers of `db setup` (`setup/db.py`), one input per question; an input left out is the terminal's Enter (its default), except the PGTune output (none) and the yes/no questions (no), the keep questions included. Memory sizes in GB; 0 is "unlimited" where the terminal says so. Over databases set up already the current values are the defaults, and `keep_*` and the read-only password's `keep` are the terminal's "Keep existing ...?" questions.
_GB = 4096
DB_SETUP_INPUTS = {
    "add": Field(Enum(DATABASES)),
    "databases": Field(ListOf(Enum(DATABASES), 3, unique=True)),
    "data_path": Field(Path()),
    "export_path": Field(Path()),
    "pgdata_path": Field(Path()),
    "db_name": Field(Name()),
    "pg_port": Field(Port()),
    "tablespaces": Field(ListOf(Record({"name": Field(Name(), required=True), "path": Field(Path(), required=True)}), 16)),
    "filesystem": Field(Enum(("standard", "zfs"))),
    "pgtune": Field(Text(16384)),
    "pg_mem_limit": Field(IntRange(0, _GB)),
    "mongo_data_path": Field(Path()),
    "mongo_port": Field(Port()),
    "mongo_cache_size_gb": Field(IntRange(1, _GB)),
    "mongo_mem_limit": Field(IntRange(0, _GB)),
    "mongo_validate": Field(Enum(("full", "tail", "none"))),
    "starrocks_data_path": Field(Path()),
    "starrocks_port": Field(Port()),
    "starrocks_fe_http_port": Field(Port()),
    "sr_fe_jvm_heap": Field(IntRange(1, _GB)),
    "starrocks_mem_limit": Field(IntRange(0, _GB)),
    "sr_be_mem_limit": Field(IntRange(1, _GB)),
    "sr_alter_tablet_workers": Field(IntRange(1, 1024)),
    "sr_compression": Field(Enum(("ZSTD", "LZ4"))),
    "starrocks_storage_paths": Field(ListOf(Path(), 16, unique=True)),
    "keep_tablespaces": Field(Bool()),
    "keep_storage_paths": Field(Bool()),
    "keep_pgtune": Field(Bool()),
    "auth": Field(Bool()),
    "new_password": Field(Secret()),
    "new_password_confirm": Field(Secret()),
    "ro_user": Field(Bool()),
    "ro_username": Field(Name()),
    "ro_password_mode": Field(Enum(("generate", "typed", "keep"))),
    "ro_password": Field(Secret()),
    "ro_password_confirm": Field(Secret()),
}


def _check_create_indexes(fields):
    """Cross-field rules of `db create-indexes`: each table named once, BITMAP choices only with StarRocks and only among the table's columns. Whether a table or column exists is the action's check, and so is an empty list (refused there): only the database knows."""
    indexes = fields["indexes"]
    tables = [item["table"] for item in indexes]
    if len(set(tables)) != len(tables):
        raise FieldRefused("indexes", "names a table twice")
    for item in indexes:
        if item.get("bitmap") and fields["database"] != "starrocks":
            raise FieldRefused("indexes", "takes bitmap columns only with the database starrocks")
        if not set(item.get("bitmap", ())) <= set(item["columns"]):
            raise FieldRefused("indexes", "takes as bitmap columns only columns it indexes")


# The tables and columns of one index build, as `db create-indexes` asks them: per table its columns (index fields), and for StarRocks which of them get a BITMAP index (the others a bloom filter).
INDEX_TABLES_MAX = 50
INDEX_COLUMNS_MAX = 64
_INDEX_REQUEST = ListOf(Record({
    "table": Field(Name(), required=True),
    "columns": Field(ListOf(Name(), max_items=INDEX_COLUMNS_MAX, unique=True), required=True),
    "bitmap": Field(ListOf(Name(), max_items=INDEX_COLUMNS_MAX, unique=True)),
}), max_items=INDEX_TABLES_MAX)


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
        name="source.size", cls="read", cli=("source", "size"),
        cli_inputs={"name"},
        socket_inputs={"name": Field(ConfiguredSource())},
        socket_enabled=True,
        action="get_source_size",
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
        name="db.restart_needed", cls="read", cli=("db", "restart-needed"),
        socket_enabled=True,
        action="get_db_restart_needed",
    ),
    Verb(
        name="db.llm_settings", cls="read", cli=("db", "llm-settings"),
        socket_enabled=True,
        action="get_db_llm_settings",
    ),
    Verb(
        name="audit.list", cls="read", cli=("broker", "audit"),
        # verify checks the whole file, which the terminal does; a client reads the newest lines.
        cli_inputs={"limit", "verify"},
        socket_inputs={"limit": Field(IntRange(1, 1000))},
        socket_enabled=True,
        note="Answered by the broker itself from its audit log: there is no action in the CLI.",
    ),
    Verb(
        name="source.hf_inspect", cls="read", cli=("source", "hf-inspect"),
        cli_inputs={"dataset", "token"},
        socket_inputs={"dataset": Field(HfDatasetId(), required=True), "token": Field(Secret())},
        socket_enabled=True,
        action="get_source_hf_inspect",
    ),
    Verb(
        name="db.settings", cls="read", cli=("db", "settings"),
        socket_enabled=True,
        action="get_db_settings",
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
        # `source` is the CLI's `name`: a task's source is the field of that name in the run store. `--data-type` is not taken: the browser downloads every data type.
        socket_inputs={"source": Field(ConfiguredSource(), required=True), "token": Field(Secret())},
        socket_enabled=True,
        note="Started as a task (a recorded run); every folder and file name built from the Hub's reply is checked before anything is written.",
        action="do_source_download",
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
        # Each question of the questionnaire is one input; the admin password typed twice is `new_password` and its confirmation.
        prompted_inputs=set(DB_SETUP_INPUTS) - {"add"},
        # Without `add` the questionnaire, the first time or over the databases set up already (their values are its defaults); with `add` one database not set up yet. Authentication switched off stays a terminal command (the action refuses it).
        socket_inputs=DB_SETUP_INPUTS,
        socket_enabled=True,
        check=_check_db_setup,
        action="do_db_setup",
    ),
    Verb(
        name="db.setup_mcp", cls="step_up", cli=("db", "setup-mcp"),
        prompted_inputs={"postgres", "mongo", "starrocks"},
        socket_inputs={"postgres": Field(_MCP_SERVER), "mongo": Field(_MCP_SERVER), "starrocks": Field(_MCP_STARROCKS)},
        socket_enabled=True,
        action="do_db_setup_mcp",
    ),
    Verb(
        name="db.setup_jobs", cls="step_up", cli=("db", "setup-jobs"),
        prompted_inputs=set(_JOBS_INPUTS),
        socket_inputs=_JOBS_INPUTS,
        socket_enabled=True,
        check=_check_setup_jobs,
        action="do_db_setup_jobs",
    ),
    Verb(
        name="source.add", cls="step_up", cli=("source", "add"),
        cli_inputs={"name", "hf_dataset"},
        prompted_inputs=set(SOURCE_ADD_QUESTIONS),
        socket_inputs={"name": Field(Name(), required=True), "hf_dataset": Field(HfDatasetId()), **SOURCE_ADD_QUESTIONS},
        socket_enabled=True,
        check=_check_source_add,
        note="Reddit dumps, a Hugging Face dataset (with its grouping) and your own files.",
        action="do_source_add",
    ),
    Verb(
        name="source.configure", cls="step_up", cli=("source", "configure"),
        cli_inputs={"name"},
        prompted_inputs=set(SOURCE_CONFIGURE_QUESTIONS),
        socket_inputs={"name": Field(ConfiguredSource(), required=True), **SOURCE_CONFIGURE_QUESTIONS},
        socket_enabled=True,
        note="Refused by the broker while a run of the source is alive (a pipeline run, a task such as a download, or a terminal run Docker reports), and when Docker cannot be asked.",
        action="do_source_configure",
    ),
    Verb(
        name="source.add_classifiers", cls="step_up", cli=("source", "add-classifiers"),
        cli_inputs={"name"},
        prompted_inputs=set(SOURCE_CLASSIFIER_QUESTIONS),
        socket_inputs={"name": Field(ConfiguredSource(), required=True), **SOURCE_CLASSIFIER_QUESTIONS},
        socket_enabled=True,
        note="Refused by the broker while a run of the source is alive, as source.configure is.",
        action="do_source_add_classifiers",
    ),
    Verb(
        name="source.settings", cls="read", cli=("source", "settings"),
        cli_inputs={"name"},
        socket_inputs={"name": Field(ConfiguredSource(), required=True)},
        socket_enabled=True,
        action="get_source_settings",
    ),
    Verb(
        name="db.create_indexes", cls="step_up", cli=("db", "create-indexes"),
        cli_inputs={"source"},
        # What the command asks: which database, the tables and their fields, BITMAP or bloom filter (StarRocks), whether to save them to platform.yaml, and the admin password.
        prompted_inputs={"database", "indexes", "save", "admin_password"},
        socket_inputs={
            "source": Field(ConfiguredSource(), required=True),
            "database": Field(Enum(DATABASES), required=True),
            "indexes": Field(_INDEX_REQUEST, required=True),
            "save": Field(Bool()),
            "admin_password": Field(Secret()),
        },
        socket_enabled=True,
        check=_check_create_indexes,
        note="Runs as a recorded run (a task): the action reads the tables and columns from the database again and refuses any it does not have.",
        action="do_db_create_indexes",
    ),
    Verb(
        name="db.index_targets", cls="read", cli=("db", "index-targets"),
        cli_inputs={"source", "db"},
        prompted_inputs={"admin_password"},
        socket_inputs={"source": Field(ConfiguredSource(), required=True), "db": Field(Enum(DATABASES), required=True), "admin_password": Field(Secret())},
        socket_enabled=True,
        action="get_db_index_targets",
    ),
    Verb(
        name="chat.settings", cls="read", cli=("chat", "settings"),
        socket_enabled=True,
        action="get_chat_settings",
    ),
    Verb(
        name="chat.query_limit", cls="step_up", cli=("chat", "query-limit"),
        # Without `seconds` the command only shows the limit, which `chat.settings` reads: the socket always sets one.
        cli_inputs={"seconds"},
        socket_inputs={"seconds": Field(IntRange(1, CHAT_QUERY_SECONDS_MAX), required=True)},
        socket_enabled=True,
        note="Writes CHAT_QUERY_TIMEOUT_SECONDS to .env and, when the chat runs, starts its MongoDB and StarRocks MCP servers again with it; the gateway and the sandbox go on.",
        action="do_chat_query_limit",
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
