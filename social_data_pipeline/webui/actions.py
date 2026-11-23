"""The web UI's own table of what its pages may ask the broker for, and `call`, the one function through which every request to the broker goes.

The image holds no registry (`control.verbs`), so the table is written out here: for each verb the pages use, its class (`read`, `write`, `step_up`, the registry's), its inputs with their kinds and closed choices, the builder of the `sdp` command shown to the operator, the line about what a terminal would ask, and for a destructive verb one sentence saying what it destroys. A test compares it with the registry: every verb exists there with the same class and is enabled on the socket, and its inputs are a subset of the registry's socket inputs with the same choices. The web side is stricter than the registry in two places, on purpose: a source name is always required for the source verbs, and `run.logs` pieces are at least 4096 bytes. Besides the verbs the table has four operations of the broker that are not verbs: `hello`, `session.set_db_password` (needs the management password), `session.clear_db_password` and `session.check_password` (the management password checked again, and nothing else).

A second kind of entry is the query scheduler's control socket (`jobs/door.py`), whose entries are named `queue.*` and carry the name of the socket's operation in `door`: the reads `queue.counts`, `queue.stats`, `queue.jobs`, `queue.job`, `queue.preview` and `queue.auto_accept_state`, and the changes `queue.approve`, `queue.reject`, `queue.kill`, `queue.explain` and `queue.auto_accept` (`auto_accept.set`). The socket takes every listed field of an operation and nothing else, so every input of a `queue.*` entry is required (`opt_bool` and `opt_int` take None, which the socket reads as "leave it as it is"). A test compares them with the socket's own list of operations and fields.

`parse_form` turns what a form posted into the fields of an action, checked by the table (closed choices, the plain-name pattern for source and classifier names, the file-filter pattern, the cross-field rules of `run.start`, unknown fields refused); `check_fields` checks fields a page made itself, and `call` checks again before it sends anything, so that no page can make the broker receive a verb, a field or a value outside this table. A secret is held in `Hidden`, which no `repr`, `str` or format shows, at any depth. Standard library and this package only.

The setup verbs' kinds mirror the registry's wide types and containers (`control.types`): `path` (`Path`), `port` (`Port`), `text` (`Text`), `list` (`ListOf`), `map` (`MapOf`) and `record` (`Record`), with the same bounds. A form posts a nested value under bracketed names, the one scheme of this module (`parse_form`, `form_name`):

- a list's item i (0, 1, ... in order, no gap, no leading zero, below its `max_items`): `<name>[<i>]`; a row is dropped when every one of its texts is empty or when the box `<name>[-remove][<i>]` is ticked, and the rows left keep their order;
- a map's value for a key (a plain name or a choice, which never holds a bracket): `<name>[<key>]`;
- a record's key (a lower-case identifier): `<name>[<key>]`;
- these nest: the field `name` of row 2 of the list `targets` is `targets[2][name]`, the second field of the data type `comments` is `fields[comments][1]`.

A text left empty is an absent value, a record or a row with nothing in it is absent too (a required one is then refused), a ticked box posts `yes`, an `int` or a `port` is plain decimal digits, and a `text` has its `\r\n` turned into `\n` (browsers send a textarea's lines so). Anything else, a name posted twice included (the guard refuses that already, with more than 64 fields), is refused with the name of the top-level input.
"""

import posixpath
import re
import shlex
import unicodedata
from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Callable

from social_data_pipeline.webui.overview import PLAIN_NAME

# Literal copies of the registry's patterns and choices (`control.types`, `control.verbs`): a test compares them.
FILE_FILTER_PATTERN = r"[A-Za-z0-9_.*?\[\]!-]{1,128}"
HF_DATASET_PART = r"[A-Za-z0-9](?:[A-Za-z0-9_.-]{0,94}[A-Za-z0-9_])?"
HF_DATASET_PATTERN = HF_DATASET_PART + "/" + HF_DATASET_PART
HF_SQL_TYPES = ("text", "integer", "bigint", "float", "boolean")
RUN_ID_PATTERN = r"r[0-9]{8}T[0-9]{6}-[0-9a-f]{6}|live-[0-9a-f]{12}"
VALID_PROFILES = ("parse", "lingua", "ml", "postgres_ingest", "postgres_ml", "mongo_ingest", "sr_ingest", "sr_ml")
DATABASES = ("postgres", "mongo", "starrocks")
SERVICES = ("postgres", "mongo", "starrocks", "postgres-mcp", "mongo-mcp", "starrocks-mcp", "jobs")
ERROR_LOG_PROFILES = ("postgres_ingest", "postgres_ml", "mongo_ingest")
SECRET_MAX_CHARS = 1024
# The smallest piece of a log the pages ask for; the registry allows 1, the broker refuses less than 4096.
RUN_LOGS_MIN_LIMIT = 4096
RUN_LOGS_MAX_LIMIT = 1048576
LOG_OFFSET_MAX = 2 ** 53
# The largest read-only query limit for chats, in seconds (`control.verbs.CHAT_QUERY_SECONDS_MAX`).
CHAT_QUERY_SECONDS_MAX = 3600
# The two values of the switch of generated chat titles (`control.verbs.AUTO_TITLE_CHOICES`).
AUTO_TITLE_CHOICES = ("on", "off")

# Literal copies of the scheduler's control socket's rules (`jobs/door.py`: `JOB_ID`, `DIGEST`, `BY_LABEL`, `PHASES`, `JOBS_LIMIT_MAX`, `JOBS_OFFSET_MAX`, `REASON_MAX`): a test compares them.
JOB_ID_PATTERN = r"(pg|sr|mg|job)_[0-9a-f]{8}"
DIGEST_PATTERN = r"[0-9a-f]{64}"
BY_PATTERN = r"web-[0-9]{1,12}"
PHASES = ("pending", "approved", "running", "history")
JOBS_LIMIT_MAX = 500
JOBS_OFFSET_MAX = 100000
REASON_MAX = 500
TARGET_MAX_CHARS = 256
# The digits an auto-accept limit of a form may have: only a bound of the form, since the real largest limit is the scheduler's (its `auto_accept` reply names it, `max_limit`, and a larger one is refused before anything is kept).
LIMIT_DIGITS = 6

# The profiles whose run uses a database, and so may need its admin password.
DATABASE_PROFILES = ("postgres_ingest", "postgres_ml", "mongo_ingest", "sr_ingest", "sr_ml")
# The inputs of `run.start` a form of each profile has, besides the profile and the source.
NO_FILTER_PROFILES = ("sr_ingest", "sr_ml")
CLASSIFIER_PROFILES = ("ml", "lingua", "sr_ml")

PROGRAM = "python sdp.py"
# The value a checked box posts.
CHECKED = "yes"
SECRET_DOTS = "\u2022" * 8

CLASSES = ("read", "write", "step_up")

# Literal copies of the registry's bounds of the wide types and containers (`control.types`): a test compares them.
PATH_MAX_BYTES = 4096
TEXT_MAX_BYTES = 65536
PORT_MIN = 1024
PORT_MAX = 65535
CONTAINER_ITEMS_MAX = 1024
RECORD_KEY_PATTERN = r"[a-z][a-z0-9_]{0,63}"
# The kinds that hold other inputs, and the key kinds a map may have.
CONTAINER_KINDS = ("list", "map", "record")
MAP_KEY_KINDS = ("name", "enum")
# The kinds whose value a form posts as one text (the others are containers, or a box: `bool`).
TEXT_KINDS = ("enum", "name", "source", "filter", "secret", "run_id", "hf_dataset", "path", "text")
# A form name: a top-level input and its bracketed segments (at most five: a container's depth is at most four, and a list's remove box adds one).
FORM_NAME = re.compile(r"([a-z][a-z0-9_]{0,63})((?:\[[^\[\]]{1,64}\]){1,5})")
FORM_SEGMENT = re.compile(r"\[([^\[\]]{1,64})\]")
INDEX_PATTERN = r"0|[1-9][0-9]{0,3}"
REMOVE_SEGMENT = "-remove"
# Plain decimal digits, for an `int` or a `port` typed in a form (a sign only for an `int` whose bounds go below zero).
FORM_INT_PATTERN = r"-?(?:0|[1-9][0-9]{0,17})"


class Refused(ValueError):
    """A field of a form or of a page's request was refused. `field` is the name of the field when it is one of the action's inputs, else None; the message holds no value."""

    def __init__(self, field=None):
        super().__init__("a field was refused")
        self.field = field


class NotInTable(Exception):
    """A page asked for something the table does not allow. A bug of the web UI: nothing is sent."""


class Hidden:
    """A secret of a pending action. No `repr`, `str` or format shows it; `reveal()` returns it."""

    __slots__ = ("_value",)

    def __init__(self, value):
        self._value = value

    def reveal(self):
        return self._value

    def __repr__(self):
        return "<secret>"

    __str__ = __repr__

    def __format__(self, spec):
        return "<secret>"


@dataclass(frozen=True)
class Input:
    """One input of an action: its kind (`enum`, `bool`, `name`, `source`, `filter`, `secret`, `run_id`, `int`; for the setup verbs `path`, `port`, `text`, `list`, `map` and `record`; for the scheduler's entries also `job_id`, `digest`, `by`, `reason`, `target`, `opt_bool` and `opt_int`), whether a request must carry it, the choices of an `enum`, the bounds of an `int`, the label a page shows, and what the input left out means when that is worth saying.

    The setup kinds take: `path` max_bytes (UTF-8 bytes, default 4096); `port` nothing (1024 to 65535); `text` max_bytes and multiline; `list` item (an Input), max_items and unique; `map` key (an Input of kind `name` or `enum`), item (the values' Input) and max_items; `record` fields (a dict of a lower-case identifier to an Input, each with its own `required`). A malformed one raises when the table is built.
    """

    kind: str
    label: str
    required: bool = False
    choices: tuple = ()
    lo: int = 0
    hi: int = 0
    # What an optional input left out means, shown as its value on the pending page (None: the row is left out).
    absent: str | None = None
    max_bytes: int = 0
    multiline: bool = False
    item: "Input | None" = None
    key: "Input | None" = None
    max_items: int = 0
    unique: bool = False
    fields: object = None

    def __post_init__(self):
        if self.kind == "path" and self.max_bytes == 0:
            object.__setattr__(self, "max_bytes", PATH_MAX_BYTES)
        if self.kind in ("path", "text") and not 1 <= self.max_bytes <= (PATH_MAX_BYTES if self.kind == "path" else TEXT_MAX_BYTES):
            raise ValueError("a path or a text needs max_bytes within the registry's bound")
        if self.kind in ("list", "map"):
            if not isinstance(self.item, Input) or not 1 <= self.max_items <= CONTAINER_ITEMS_MAX:
                raise ValueError("a list or a map needs an item Input and 1 to 1024 items")
        if self.kind == "list" and self.unique and (self.item.kind in CONTAINER_KINDS or self.item.secret):
            raise ValueError("a unique list needs items that are not containers or secrets")
        if self.kind == "map" and (not isinstance(self.key, Input) or self.key.kind not in MAP_KEY_KINDS):
            raise ValueError("a map needs a key Input of kind name or enum")
        if self.kind == "record":
            if not isinstance(self.fields, dict) or not self.fields or not all(isinstance(name, str) and re.fullmatch(RECORD_KEY_PATTERN, name) and isinstance(spec, Input) for name, spec in self.fields.items()):
                raise ValueError("a record needs a dict of lower-case identifiers to Inputs")
            object.__setattr__(self, "fields", MappingProxyType(dict(self.fields)))

    @property
    def secret(self):
        return self.kind == "secret"


@dataclass(frozen=True)
class Action:
    """One entry of the table. `command(fields)` is the `sdp` command shown, or None when there is none; `destroys(fields)` is (the sentence, the name of what is destroyed) for a destructive verb, else None; `form` is the command's form as the System page lists it; `note` is a line the pending page adds; `also(fields)` is what else the chosen fields act on (a service's MCP server, the scheduler), or None; `op` is true for an operation of the broker that is not a verb of the registry; `door` is the name of the operation of the scheduler's control socket for a `queue.*` entry, else None."""

    name: str
    cls: str
    title: str
    inputs: object = MappingProxyType({})
    form: str = ""
    command: Callable | None = None
    destroys: Callable | None = None
    terminal_asks: str = ""
    check: Callable | None = None
    op: bool = False
    # One more line for the pending page: the broker's refusal while a pipeline run is alive, for the verbs it refuses then.
    note: str = ""
    # What else the action acts on for the chosen fields, `also(fields)`: a line for the pending page, or None.
    also: Callable | None = None
    door: str | None = None
    # Warnings for the pending page, `warns(fields)`: a list of sentences (the MCP servers' write access), or None.
    warns: Callable | None = None
    # What a setup writes for the chosen fields, `writes(fields)`: the confirm page's list of lines (files, .env keys, folders, passwords), or None.
    writes: Callable | None = None

    def __post_init__(self):
        object.__setattr__(self, "inputs", MappingProxyType(dict(self.inputs)))

    @property
    def state_changing(self):
        return self.cls != "read"


def _q(value):
    return shlex.quote(value)


def _command(*parts):
    return " ".join((PROGRAM, *parts))


def _run_start_command(fields):
    parts = ["run", _q(fields["profile"]), "--source=" + _q(fields["source"])]
    if fields.get("build") is True:
        parts.append("--build")
    if "filter" in fields:
        parts.append("--filter=" + _q(fields["filter"]))
    if fields.get("skip_lingua_files") is True:
        parts.append("--skip-lingua-files")
    if "classifier" in fields:
        parts.append("--classifier=" + _q(fields["classifier"]))
    return _command(*parts)


def _service_command(word):
    def build(fields):
        return _command("db", word, *([_q(fields["service"])] if "service" in fields else []))

    return build


def _check_setup_jobs(fields):
    """The registry's rule of `db.setup_jobs`: no two targets of one name."""
    seen = set()
    for key in ("postgres_target", "starrocks_target", "mongo_target"):
        if key in fields:
            if fields[key]["name"] in seen:
                raise Refused(key)
            seen.add(fields[key]["name"])


def _check_run_start(fields):
    """The cross-field rules of `run.start`, the registry's: no file filter with the StarRocks profiles, `skip_lingua_files` only with `parse`, a classifier only with `ml`, `lingua` and `sr_ml`."""
    profile = fields["profile"]
    if "filter" in fields and profile in NO_FILTER_PROFILES:
        raise Refused("filter")
    if fields.get("skip_lingua_files") is True and profile != "parse":
        raise Refused("skip_lingua_files")
    if "classifier" in fields and profile not in CLASSIFIER_PROFILES:
        raise Refused("classifier")


_SOURCE = Input("source", "Source", required=True)
_ASKS_PASSWORD = "In a terminal this command asks for the database admin password where it needs one and none is kept; the command shown holds no password."
_ASKS_CONFIRMATION = "In a terminal this command asks for a confirmation first; confirming here, with the management password, takes its place."
# The broker's destructive verbs (`control.broker.DESTRUCTIVE_VERBS`).
_REFUSED_WHILE_A_RUN_LIVES = "The broker refuses it while a pipeline run is alive, and when Docker cannot be asked whether one is."
_ALL_SERVICES = "every configured service"
_DATABASE_NAMES = {"postgres": "PostgreSQL", "mongo": "MongoDB", "starrocks": "StarRocks"}
# What `db reset` deletes of each database (`sdp.py` `do_db_reset`: the server's folders and the ingestion state in its data folder, MongoDB's logs, PostgreSQL's tablespaces, StarRocks's storage paths), after it stopped the database and its MCP server.
_RESET_DELETES = {
    "postgres": "the server's folder (pgdata) and the ingestion state (state_tracking) in its data folder, and the contents of every PostgreSQL tablespace folder, which can sit on other disks",
    "mongo": "the server's folder (db), the ingestion state (state_tracking) and its logs (logs) in its data folder",
    "starrocks": "the server's folders (fe, be) and the ingestion state (state_tracking) in its data folder, and the contents of every StarRocks storage path, which can sit on other disks",
}


# Both `db reset` and `db unsetup` first hand the parent folder of the data folder, recursively, to the host user, and the parent folder of each tablespace folder or storage path itself (`sdp.py` `_chown_db_data_for_host`). In the default layout the first holds the other databases' data folders and nothing else; with another data path it can hold anything.
_OWNERSHIP_START = "Before it deletes anything, it gives your user account ownership of everything in the folder that holds its data folder, recursively (by default data/database/, which also holds the other databases' data folders)"
_OWNERSHIP_EXTRA = {"postgres": ", and of the folder that holds each tablespace folder", "mongo": "", "starrocks": ", and of the folder that holds each storage path"}
_OWNERSHIP_END = ". Other databases keep their data and settings, but one that is running at that moment with its files in that folder may fail."


def _ownership_clause(database):
    return _OWNERSHIP_START + _OWNERSHIP_EXTRA[database] + _OWNERSHIP_END


def _reset_sentence(fields):
    name = _DATABASE_NAMES[fields["db"]]
    return (f"Stops {name} and its MCP server, then deletes all of its data: {_RESET_DELETES[fields['db']]}. Its configuration, its .env entries, the MCP settings and the per-source settings stay. {_ownership_clause(fields['db'])}", fields["db"])


# What `db unsetup --db` removes of each database besides its whole data folder (`sdp.py` `_unsetup_single_db`): the extra folders, the configuration files (each with its `.bak` copy) and the per-source settings files (`_DB_TO_SOURCE_FILES`).
_UNSETUP_REMOVES = {
    "postgres": ("PGDATA_PATH", "every PostgreSQL tablespace folder itself, which can sit on other disks; ", "config/db/postgres.yaml, config/postgres/postgresql.local.conf and config/postgres/pg_hba.local.conf", "postgres.yaml and postgres_ml.yaml"),
    "mongo": ("MONGO_DATA_PATH", "", "config/db/mongo.yaml", "mongo.yaml"),
    "starrocks": ("STARROCKS_DATA_PATH", "every StarRocks storage-path folder itself, which can sit on other disks; ", "config/db/starrocks.yaml, config/starrocks/fe.local.conf and config/starrocks/be.local.conf", "starrocks.yaml and sr_ml.yaml"),
}


def _unsetup_sentence(fields):
    name = _DATABASE_NAMES[fields["db"]]
    data_setting, extra, configuration, per_source = _UNSETUP_REMOVES[fields["db"]]
    return (
        f"Stops {name} and its MCP server and removes their containers, then deletes: the whole data folder of {name} ({data_setting}), its read-only user's credentials file (.ro_credentials) included; {extra}"
        f"its configuration files {configuration}, each with its .bak copy; the per-source settings files {per_source} in every folder under config/sources/ (which a reset keeps); "
        f"its keys in .env, those of its MCP server included; its block of docker-compose.override.yml; and its part of config/db/mcp.yaml (each of these two files is removed when nothing else is left in it). "
        f"The scheduler's targets that point at {name} are left as they are and fail until the scheduler is set up again. {_ownership_clause(fields['db'])}",
        fields["db"],
    )


# What else `db stop` and `db start` act on for each service chosen (`sdp.py` `do_db_stop`, `do_db_start`); None: nothing else. The web UI cannot see which MCP servers or which scheduler targets are configured, so a line says "when".
def _stop_also(fields):
    service = fields.get("service")
    if service is None:
        return "It stops every configured database, the MCP server of each that has one configured and the scheduler when it is configured, and removes their containers."
    if service in DATABASES:
        return f"It also stops the MCP server of {_DATABASE_NAMES[service]} when one is configured, and removes the containers it stops."
    return None


_MOUNTS_REWRITTEN = "Before the start it rewrites the per-source data mounts of PostgreSQL and StarRocks in docker-compose.override.yml when a source is configured."


def _start_also(fields):
    service = fields.get("service")
    if service is None:
        return "It starts every configured database, then the MCP server of each that has one configured, and rebuilds and starts the scheduler when one of its targets is among those databases. " + _MOUNTS_REWRITTEN
    if service in DATABASES:
        name = _DATABASE_NAMES[service]
        line = f"It also starts the MCP server of {name} when one is configured, and rebuilds and starts the scheduler when one of its targets is {name}."
        if service in ("postgres", "starrocks"):
            # `_resolve_server_data_mounts` builds the per-source mounts of the services it is given only, and keeps of the other one its setup mounts alone.
            other = _DATABASE_NAMES["starrocks" if service == "postgres" else "postgres"]
            line += f" Before the start, when a source is configured, it rewrites docker-compose.override.yml: it writes the per-source data mounts of {name} and removes those of {other}, which {other} then lacks until it is next started with `db start` of {other} or of every service."
        return line
    if service.endswith("-mcp"):
        return f"It also starts {_DATABASE_NAMES[service[:-len('-mcp')]]} itself, with its MCP server."
    # jobs: `up -d --build jobs`.
    return "It rebuilds the scheduler's image before it starts the scheduler."


# ── db setup-mcp and db setup-jobs (`sdp.py` `do_db_setup_mcp`, `do_db_setup_jobs`) ──

_ASKS_QUESTIONS = "In a terminal this command asks these questions one by one, with the current values as defaults, then whether to write the files; the answers and the confirmation here take their place."
_NOTHING_RESTARTS = "Nothing is restarted: a running service keeps its old settings until it is restarted. Needs attention on the Overview names the services that must restart, with a Restart button."
_MCP_KEYS = {"postgres": "POSTGRES_MCP_PORT and POSTGRES_MCP_ACCESS_MODE", "mongo": "MONGO_MCP_PORT and MONGO_MCP_READ_ONLY", "starrocks": "STARROCKS_MCP_PORT"}
_MCP_ORDER = ("postgres", "mongo", "starrocks")


def _setup_mcp_sentence(fields):
    """What `do_db_setup_mcp` writes for these fields."""
    on = [name for name in _MCP_ORDER if name in fields]
    if not on:
        return "Every MCP server is off here, so the command writes nothing and says so. To remove the MCP servers' configuration, use Remove… on the MCP servers tab."
    names = ", ".join(_DATABASE_NAMES[name] for name in on)
    keys = "; ".join(_MCP_KEYS[name] for name in on)
    users = ", ".join(f"{name.upper()}_MCP_USER" for name in on)
    return (
        f"Writes config/db/mcp.yaml with the MCP servers switched on here ({names}), after copying the file it replaces to config/db/mcp.yaml.bak; a server switched off is left out of it, and db start no longer starts it. "
        f"Sets in .env: {keys}; and {users} (the read-only user) for each of these databases that has authentication on with a read-only user. The .env keys of a server switched off stay as they are. "
        "Refused when a database with authentication has no read-only user or no read-only credentials file, and when a port is one another service of this deployment uses. " + _NOTHING_RESTARTS
    )


def _setup_mcp_warns(fields):
    """The write-access warning of each server switched on with write access."""
    warns = []
    for name in ("postgres", "mongo"):
        if fields.get(name, {}).get("write_access") is True:
            warns.append(f"Write access for the {_DATABASE_NAMES[name]} MCP server: it takes statements that change data. Any agent that reaches port {fields[name]['port']} can change data in {_DATABASE_NAMES[name]}, unless {_DATABASE_NAMES[name]} has authentication on with a read-only user: the server then logs in as that user, who cannot write.")
    return warns or None


def shown_result_root(path):
    """The results folder as `do_db_setup_jobs` writes it: a relative one with its leading `./` (`as_written_path`)."""
    return as_written_path(path)


_TARGETS = (("postgres_target", "postgres", "PostgreSQL"), ("starrocks_target", "starrocks", "StarRocks"), ("mongo_target", "mongo", "MongoDB"))


def _setup_jobs_sentence(fields):
    """What `do_db_setup_jobs` writes for these fields, the mounts that change included."""
    root = shown_result_root(fields["result_root"])
    targets = [title for key, _, title in _TARGETS if key in fields]
    mounted = [title for key, name, title in _TARGETS if key in fields and name in ("postgres", "starrocks")]
    kept = [title for key, name, title in _TARGETS if key not in fields and name in ("postgres", "starrocks")]
    override = f"the /jobs_export mount of {' and '.join(mounted)} there becomes {root}:/jobs_export:rw" if mounted else "no /jobs_export mount there is changed"
    if kept:
        override += f"; {' and '.join(kept)}, without a target here, keeps the /jobs_export mount it has there, if any"
    sentence = (
        f"Writes config/jobs/config.local.yaml with the targets on {', '.join(targets) or 'no database'} and the numbers above, after copying the file it replaces to config/jobs/config.local.yaml.bak; sets JOBS_PORT and JOBS_RESULT_ROOT ({root}) in .env; makes the results folder and data/jobs when they are missing. "
        f"It rewrites docker-compose.override.yml as YAML (its comments are not kept, its other mounts are): {override}. "
        f"The mounts that change at each service's next start: the scheduler's /data/jobs/results and StarRocks's /jobs_export (docker-compose.yml mounts JOBS_RESULT_ROOT at both), the /jobs_export mounts of the override, and the web UI's results folder at its next python sdp.py webui start."
    )
    if "starrocks_target" in fields:
        sentence += " With a StarRocks target it also sets enable_outfile_to_local = true in config/starrocks/fe.local.conf when that file exists."
    if fields.get("auth_enabled") is True:
        sentence += " The scheduler's own page asks for the database admin password; switching that off again is done in a terminal with python sdp.py db setup-jobs."
    return sentence + " Refused when a port is one another service of this deployment uses, and when the results folder is data/jobs or holds it (the scheduler would keep its control socket closed). " + _NOTHING_RESTARTS


# Write access left out is the box left unticked: the server is read-only (`do_db_setup_mcp`: PostgreSQL's access mode restricted, MongoDB's read-only on).
_MCP_SERVER_FIELDS = {"port": Input("port", "Port", required=True), "write_access": Input("bool", "Write access", absent="no (read-only)")}
_HOURS_INPUT = Input("int", "Default timeout (hours, 0 = no limit)", required=True, lo=0, hi=8760)


# The inputs of `source.add`, the registry's (`control.verbs.SOURCE_ADD_QUESTIONS`) as the table's kinds.
_ADD_NAME = Input("name", "Name")
_ADD_NAMES = Input("list", "Names", item=_ADD_NAME, max_items=64, unique=True)
_REDDIT_KINDS = Input("enum", "Kind", choices=("submissions", "comments"))
_ADD_WORKERS = Input("int", "Workers", lo=1, hi=1024)


# A folder of `source.add` left out: the action computes the terminal's default on the host (`setup.source_add.build`), from .env's DATA_PATH, which the web UI cannot read.
DEFAULT_FOLDER_WORDS = "the same default the terminal offers, DATA_PATH/{stage}/<name> (DATA_PATH as .env sets it, ./data when it sets none)"
SOURCE_STAGES = ("dumps", "extracted", "parsed", "output")


def as_written_path(path):
    """A path as the host's actions write it (`setup.utils.written_path`, which this container does not have; a test compares the two): a relative one gets a leading `./` unless it is `.` or `..` or starts with `./` or `../`, so a page shows what will be written."""
    if not isinstance(path, str) or not path or path.startswith("/") or path in (".", "..") or path.startswith(("./", "../")):
        return path
    return "./" + path


def _per_type(label, item, max_items=32):
    return Input("map", label, key=Input("name", "Data type"), item=item, max_items=max_items)


SOURCE_ADD_INPUTS = {
    "name": Input("name", "Source name", required=True),
    "platform": Input("enum", "Origin", required=True, choices=("custom", "reddit")),
    "data_types": Input("list", "Data types", required=True, item=Input("name", "Data type"), max_items=32, unique=True),
    "dumps_path": Input("path", "Dumps folder", absent=DEFAULT_FOLDER_WORDS.format(stage="dumps")),
    "extracted_path": Input("path", "Extracted folder", absent=DEFAULT_FOLDER_WORDS.format(stage="extracted")),
    "parsed_path": Input("path", "Parsed folder", absent=DEFAULT_FOLDER_WORDS.format(stage="parsed")),
    "output_path": Input("path", "Output folder", absent=DEFAULT_FOLDER_WORDS.format(stage="output")),
    "file_format": Input("enum", "Intermediate file format", required=True, choices=("parquet", "csv")),
    "parquet_row_group_size": Input("int", "Parquet row-group size", lo=1, hi=1_000_000_000),
    "profiles": Input("list", "Profiles", required=True, item=Input("enum", "Profile", choices=VALID_PROFILES), max_items=len(VALID_PROFILES), unique=True),
    "parse_workers": Input("int", "Parse workers", lo=1, hi=1024),
    "pg_prefer_lingua": Input("bool", "PostgreSQL: use the Lingua files"),
    "pg_parallel_index_workers": Input("int", "PostgreSQL: parallel index workers", lo=0, hi=1024),
    "table_tablespaces": _per_type("PostgreSQL tablespaces", Input("name", "Tablespace")),
    "db_schema": Input("name", "PostgreSQL schema"),
    "input_format": Input("enum", "Raw input format", choices=("ndjson", "csv")),
    "csv_delimiter": Input("enum", "CSV delimiter", choices=("comma", "tab", "pipe", "semicolon")),
    "dump_globs": _per_type("Dump file patterns", Input("filter", "Pattern")),
    "compressions": _per_type("Compressions", Input("enum", "Compression", choices=("zst", "gz", "xz", "tar.gz"))),
    "fields": _per_type("Fields", Input("list", "Fields", item=_ADD_NAME, max_items=256, unique=True)),
    "mongo_collection_strategy": Input("enum", "MongoDB collection strategy", choices=("per_data_type", "per_file")),
    "mongo_collections": _per_type("MongoDB collections", _ADD_NAME),
    "mongo_db_name": Input("name", "MongoDB database name"),
    "pg_indexes": _per_type("PostgreSQL indexes", _ADD_NAMES),
    "mongo_indexes": _per_type("MongoDB indexes", _ADD_NAMES),
    "sr_indexes": _per_type("StarRocks bitmap indexes", _ADD_NAMES),
    "primary_key": Input("name", "Primary key"),
    "sr_prefer_lingua": Input("bool", "StarRocks: use the Lingua files"),
    "sr_buckets": Input("int", "StarRocks buckets per table", lo=1, hi=65536),
    "reddit_db_schema": Input("name", "Reddit schema"),
    "reddit_removed_fields": Input("map", "Reddit fields removed", key=_REDDIT_KINDS, item=_ADD_NAMES, max_items=2),
    "reddit_indexes": Input("map", "Reddit index columns", key=_REDDIT_KINDS, item=_ADD_NAMES, max_items=2),
    "text_columns": _per_type("Text columns", _ADD_NAMES),
    "remove_strings": Input("list", "Exact strings removed", item=Input("text", "String", max_bytes=256), max_items=64),
    "remove_patterns": Input("list", "Patterns removed", item=Input("text", "Pattern", max_bytes=512), max_items=64),
    "gpu_fields": Input("list", "Extra fields in the GPU output", item=_ADD_NAME, max_items=64, unique=True),
    "lingua_workers": Input("int", "Lingua workers", lo=1, hi=1024),
    "lingua_file_workers": Input("int", "Lingua file workers", lo=1, hi=1024),
    "lingua_batch_size": Input("int", "Lingua batch size", lo=1, hi=100_000_000),
    "lingua_low_accuracy": Input("bool", "Lingua low accuracy mode"),
    "gpu_ids": Input("list", "GPUs", item=Input("int", "GPU", lo=0, hi=255), max_items=64, unique=True),
    "ml_file_workers": Input("int", "GPU file workers", lo=1, hi=1024),
    "ml_tokenize_workers": Input("int", "Tokenize workers", lo=0, hi=1024),
    "ml_classifier_batch_size": Input("int", "Classifier batch size", lo=1, hi=65536),
    "ml_classifiers": Input("list", "Classifiers", item=_ADD_NAME, max_items=64, unique=True),
    "ml_classifier_data_types": Input("map", "Data types per classifier", key=_ADD_NAME, item=Input("list", "Data types", item=Input("name", "Data type"), max_items=32, unique=True), max_items=64),
    "hf_token": Input("secret", "Hugging Face token"),
    "hf_dataset": Input("hf_dataset", "Hugging Face dataset"),
    "hf_groups": Input("list", "Hugging Face groups", max_items=8, item=Input("record", "Group", fields={
        "configs": Input("list", "Configs", required=True, item=_ADD_NAME, max_items=256, unique=True),
        "data_type": Input("name", "Data type", required=True),
        "fields": Input("list", "Fields", required=True, max_items=256, item=Input("record", "Field", fields={"name": Input("name", "Name", required=True), "sql_type": Input("enum", "SQL type", required=True, choices=HF_SQL_TYPES)})),
        "excluded": Input("list", "Left out", item=_ADD_NAME, max_items=256, unique=True),
    })),
}

# The files `source add` writes in config/sources/<name>/ for each profile (`setup.source.main`), after platform.yaml.
SOURCE_FILES = (("parse", "parse.yaml"), ("postgres_ingest", "postgres.yaml"), ("postgres_ml", "postgres_ml.yaml"), ("mongo_ingest", "mongo.yaml"), ("sr_ingest", "starrocks.yaml"), ("sr_ml", "sr_ml.yaml"), ("lingua", "lingua.yaml"), ("ml", "ml.yaml"))


def source_files(profiles):
    """The files `source add` writes for these profiles, in its order."""
    return ["platform.yaml"] + [filename for profile, filename in SOURCE_FILES if profile in profiles]


def source_add_sentence(fields):
    """What `source add` does with these fields (`sdp.do_source_add`, `setup.source_add.add_source`), clause by clause."""
    name, profiles = fields["name"], fields["profiles"]
    parts = [f"It creates config/sources/{name}/ and writes {', '.join(source_files(profiles))} in it."]
    if "hf_dataset" in fields:
        groups = "; ".join(f"{group['data_type']} from {', '.join(group['configs'])}" for group in fields["hf_groups"])
        parts.append(f"platform.yaml names the Hugging Face dataset {fields['hf_dataset']} (hf_dataset) and maps its configs to the data types ({groups}); the fields chosen and their SQL types are the ones read from the Hub when the wizard read the dataset, and the fields left out of each data type are kept out of MongoDB's documents (mongo_exclude_fields). The add itself asks the Hub nothing and downloads nothing.")
    if "hf_token" in fields:
        parts.append("It writes the Hugging Face token into .env as HF_TOKEN, in place of the line of HF_TOKEN there (a commented one too), or as a new line.")
    if fields["platform"] == "reddit" and any(key in fields for key in ("reddit_db_schema", "reddit_removed_fields", "reddit_indexes")):
        parts.append("Then it applies the Reddit schema, field and index answers to platform.yaml, as python sdp.py source configure reddit does.")
    folders = "; ".join(f"{stage} {as_written_path(fields[stage + '_path'])}" if stage + "_path" in fields else f"{stage} DATA_PATH/{stage}/{name}" for stage in SOURCE_STAGES)
    parts.append(f"The source's folders: {folders}. A folder left empty is the terminal's default, DATA_PATH/<stage>/<name> with DATA_PATH as .env sets it (./data when it sets none), and a relative folder is written with its leading ./. It creates none of them: the first run of a profile creates each one that is missing.")
    parts.append("The broker refuses the action, before anything is written, when a source of this name exists already (change one with Reconfigure), when a folder is a folder of another source, lies inside one or holds one, and when an answer is one the terminal would not ask for these profiles.")
    parts.append("Answers left empty take the terminal's default, computed on the host as source add computes it (the CPU cores, the GPUs, the base classifier configuration).")
    parts.append("When PostgreSQL or StarRocks is running, the output says whether it must be restarted (db stop, then db start) to see the new source's folders.")
    return " ".join(parts)


# The inputs of `source.configure` and `source.add_classifiers`, the registry's (`control.verbs.SOURCE_CONFIGURE_QUESTIONS`, `SOURCE_CLASSIFIER_QUESTIONS`): those of `source.add` but the origin and a Hugging Face dataset with its grouping, every one optional (its default the source's current value) but the profiles.
SOURCE_CONFIGURE_INPUTS = {"name": _SOURCE, **{key: replace(spec, required=key == "profiles", absent=None if key.endswith("_path") else spec.absent) for key, spec in SOURCE_ADD_INPUTS.items() if key not in ("name", "platform", "hf_dataset", "hf_groups")}}
CLASSIFIER_KEYS = ("text_columns", "remove_strings", "remove_patterns", "gpu_fields", "lingua_workers", "lingua_file_workers", "lingua_batch_size", "lingua_low_accuracy", "gpu_ids", "ml_file_workers", "ml_tokenize_workers", "ml_classifier_batch_size", "ml_classifiers", "ml_classifier_data_types", "hf_token")
SOURCE_CLASSIFIER_INPUTS = {"name": _SOURCE, **{key: SOURCE_ADD_INPUTS[key] for key in CLASSIFIER_KEYS}}
_TOKEN_CLAUSE = "It writes the Hugging Face token into .env as HF_TOKEN, in place of the line of HF_TOKEN there (a commented one too), or as a new line."
_RUN_REFUSAL = "The broker refuses it, before anything is written, while a run of this source is alive (a pipeline run, a download or an index build of it, or a run of it started in a terminal that Docker reports), and when Docker cannot be asked whether one is."


def source_configure_sentence(fields):
    """What `source configure <name>` does with these fields (`sdp.do_source_configure`, `setup.source_change.configure_source`), clause by clause."""
    name, profiles = fields["name"], fields["profiles"]
    parts = [f"It writes {', '.join(source_files(profiles))} in config/sources/{name}/ again, from the answers, each file that exists first copied to its .bak beside it (as the terminal does). The files of profiles left out stay as they are: nothing is removed."]
    if name == "reddit":
        parts.append("platform.yaml is written again from the Reddit template (config/templates/reddit.yaml) with the folders, the file format, the row-group size and the buckets, as python sdp.py source add reddit answered yes at Reconfigure? writes it; then the Reddit schema, field and index answers are applied to it, as python sdp.py source configure reddit applies them. A Reddit answer left empty is the template's, so a change made before is kept only as an answer here gives it. (In a terminal, source configure reddit asks the Reddit questions first and, when it adds profiles, writes platform.yaml from the template afterwards, which drops them; here they are applied last.)")
    else:
        parts.append("For a source of your own files, platform.yaml's field types are written again as the generic ones (text, and integer for a few common names), as the terminal does: types refined there by hand are lost, the .bak copy keeps them. A Hugging Face source keeps its dataset, its grouping of configs into data types, its fields and their types.")
    if "hf_token" in fields:
        parts.append(_TOKEN_CLAUSE)
    folders = [f"{stage} {as_written_path(fields[stage + '_path'])}" for stage in SOURCE_STAGES if stage + "_path" in fields]
    if folders:
        parts.append(f"The source's folders: {'; '.join(folders)}. It creates, moves or empties none of them: files in a folder no longer named stay there, and the first run of a profile creates each one that is missing.")
    parts.append("An answer left empty is the terminal's Enter: the source's current value where the terminal offers it; the GPUs default to every GPU found, and the compression of a dump pattern whose extension names none to zst, as in the terminal.")
    parts.append(_RUN_REFUSAL + " It also refuses a folder that is a folder of another source, lies inside one or holds one (the source's own folders are its own), and an answer the terminal would not ask for these profiles.")
    parts.append("Nothing is rerun or restarted: each profile reads the new settings at its next run, and the output ends with the commands to run the pipeline, as the terminal prints them. When PostgreSQL or StarRocks is running, the output also says whether it must be restarted (db stop, then db start) to see changed folders.")
    return " ".join(parts)


def source_add_classifiers_sentence(fields):
    """What `source add-classifiers <name>` does with these fields (`sdp.do_source_add_classifiers`, `setup.source_change.add_classifiers`), clause by clause."""
    name = fields["name"]
    parts = [f"It writes lingua.yaml and ml.yaml in config/sources/{name}/ again, for whichever of the profiles lingua and ml the source has, each file that exists first copied to its .bak beside it (as the terminal does)."]
    if "hf_token" in fields:
        parts.append(_TOKEN_CLAUSE)
    parts.append("An answer left empty is the terminal's Enter: the source's current value where the terminal offers it; the GPUs default to every GPU found, as in the terminal.")
    parts.append(_RUN_REFUSAL + " It also refuses a source with neither the profile lingua nor ml, and an answer the terminal would not ask for its profiles.")
    parts.append("Nothing is rerun: the next lingua or ml run reads the new settings, and a file a classifier has already finished is skipped, not classified again (delete its output to have it redone). The output ends with the commands to run the pipeline, as the terminal prints them.")
    return " ".join(parts)


def _db_setup_entry():
    """The entry of `db.setup`, kept in its own module (`setup_db_table`), which imports nothing of this one."""
    from social_data_pipeline.webui import setup_db_table

    return setup_db_table.entry(Input, Action, Refused)


# `db create-indexes` (`control.verbs` `_INDEX_REQUEST`): at most 50 tables, each with at most 64 columns, for StarRocks some of them as BITMAP.
INDEX_TABLES_MAX = 50
INDEX_COLUMNS_MAX = 64
_INDEX_REQUEST = Input("list", "Indexes", required=True, max_items=INDEX_TABLES_MAX, item=Input("record", "Table", fields={
    "table": Input("name", "Table", required=True),
    "columns": Input("list", "Columns", required=True, max_items=INDEX_COLUMNS_MAX, unique=True, item=Input("name", "Column")),
    "bitmap": Input("list", "BITMAP", max_items=INDEX_COLUMNS_MAX, unique=True, item=Input("name", "Column")),
}))
# What the pending page says of an index build (`sdp.py` `do_db_create_indexes`, run by the broker as a task: `control.runs.start_task`, `control.supervise`).
INDEXES_RUN_SENTENCE = "It runs as a run, listed in Runs and on the Overview with its output and Cancel. It first reads the tables and their columns from the database again and refuses a table or column the database does not have. A cancel stops the command, but does not undo what the database already did: an index being built may go on inside the database."
INDEXES_SAVE_SENTENCE = "When the build has made new indexes, it adds them to the source's config/sources/<name>/platform.yaml (indexes and ml_indexes for PostgreSQL, mongo_indexes for MongoDB, sr_indexes and sr_ml_indexes for StarRocks)."


def _check_create_indexes(fields):
    """The cross-field rules of `db.create_indexes`, the registry's: each table once, BITMAP columns only with StarRocks and only among the table's columns."""
    tables = [item["table"] for item in fields["indexes"]]
    if len(set(tables)) != len(tables):
        raise Refused("indexes")
    for item in fields["indexes"]:
        if item.get("bitmap") and fields["database"] != "starrocks":
            raise Refused("indexes")
        if not set(item.get("bitmap", ())) <= set(item["columns"]):
            raise Refused("indexes")


def _indexes_also(fields):
    return INDEXES_RUN_SENTENCE + (" " + INDEXES_SAVE_SENTENCE.replace("<name>", fields["source"]) if fields.get("save") is True else "")


# The entries whose result is the id of a recorded run (`run_id`), to which the outcome links: a pipeline run, and the tasks of the broker (`control.tasks.TASKS`: an index build, a download).
RUN_STARTERS = ("run.start", "db.create_indexes", "source.download")


def source_download_sentence(fields):
    """What `source download` does for a source (`sdp.do_source_download`, `setup.hf_checked.download`), clause by clause."""
    name = fields["source"]
    parts = [
        "It runs as a run of its own (Runs and the Overview show it, with its log and Cancel).",
        "It asks the Hub for the dataset's list of Parquet files and refuses the whole list, before anything is written, when a config, a split or a file name in it is not a plain name or an address is not an https address on huggingface.co.",
        f"Then it downloads every file into the dumps folder of {name} as <config>/<split>/<file> (each through a .partial file renamed when it is complete, skipping a file already there at the Hub's size), with the dataset's README.md beside them when the Hub has one and none is there, and copies the files of each data type into its folder of the extracted folder.",
        "The broker refuses it while a pipeline run of this source is alive (and when Docker cannot be asked), and refuses a pipeline run of this source while it runs.",
    ]
    if "token" in fields:
        parts.append("The token is sent with each request of this download (to the Hub, and, as the terminal sends it, on to where the Hub redirects a file), and is stored nowhere: not in a file, the run's record, its log or the session.")
    return " ".join(parts)


# What `chat query-limit <seconds>` does besides writing the value (`sdp.py` `do_chat_query_limit`, `chat_host.apply_query_limit`).
QUERY_LIMIT_SENTENCE = (
    "It writes CHAT_QUERY_TIMEOUT_SECONDS to .env, one value for the whole deployment. When the chat runs, it starts the chat's own MongoDB and StarRocks MCP servers that run now again with the new limit and attaches them again, so the databases hold queries to it within seconds; a query of those two servers that is running at that moment is cut off. "
    "The gateway and the sandbox are not restarted: running chats go on, and keep their own client-side waits until the chat is stopped and started again (python sdp.py chat stop, then python sdp.py chat start). PostgreSQL ignores the limit (its MCP server keeps its own)."
)

# What `chat auto-title on|off` does (`sdp.py` `do_chat_auto_title`: the setup's `update_env_file` for the one key, no restart; the sandbox is given the value at `chat_host._start` only), and what the setting does in the sandbox's runner.
AUTO_TITLE_SENTENCE = (
    "It writes CHAT_AUTO_TITLE to .env, one value for the whole deployment, and keeps every other line of the file. "
    "It restarts nothing: the chat's sandbox reads the setting when it starts, so the change takes effect at the chat's next start (python sdp.py chat stop, then python sdp.py chat start), and a running chat keeps the setting it started with. "
    "While it is on, a new chat gets a short title made from its first question by the chat's model a few seconds after it is sent (a title you set is never replaced, and its cost counts in the chat's Cost); while it is off, a new chat keeps the start of its first question as its title."
)


def _table(*actions):
    by_name = {}
    for action in actions:
        if action.name in by_name or action.cls not in CLASSES:
            raise ValueError("an action is defined twice or has no class")
        by_name[action.name] = action
    return MappingProxyType(by_name)


TABLE = _table(
    # ── read ──
    Action("hello", "read", "Ask the broker whether it runs", form="(no command: the broker's greeting)", op=True),
    Action("db.status", "read", "Database status", form="python sdp.py db status"),
    Action("db.verify", "read", "Check the database setup", form="python sdp.py db verify"),
    Action("source.list", "read", "List the sources", form="python sdp.py source list"),
    Action("source.status", "read", "Ingestion state of a source", {"name": _SOURCE}, form="python sdp.py source status <name>"),
    Action("source.progress", "read", "Finished files of a source", {"name": _SOURCE}, form="python sdp.py source progress <name>"),
    Action("source.size", "read", "Disk size of a source", {"name": _SOURCE}, form="python sdp.py source size <name>"),
    Action("source.error_logs", "read", "Error logs of a source", {"name": _SOURCE, "profile": Input("enum", "Profile", required=True, choices=ERROR_LOG_PROFILES)}, form="python sdp.py source error-logs <name> --profile=<profile>"),
    Action("run.list", "read", "List the runs", {"limit": Input("int", "Limit", lo=1, hi=1000)}, form="python sdp.py runs list --limit=<n>"),
    Action("run.logs", "read", "Read a run's output", {"run_id": Input("run_id", "Run", required=True), "offset": Input("int", "Offset", lo=0, hi=LOG_OFFSET_MAX), "limit": Input("int", "Limit", lo=RUN_LOGS_MIN_LIMIT, hi=RUN_LOGS_MAX_LIMIT)}, form="python sdp.py runs logs <run> --offset=<bytes> --limit=<bytes>"),
    Action("source.hf_inspect", "read", "Read a Hugging Face dataset's schema", {"dataset": Input("hf_dataset", "Dataset", required=True), "token": Input("secret", "Hugging Face token")}, form="python sdp.py source hf-inspect <owner>/<dataset>"),
    Action("audit.list", "read", "The newest lines of the audit log", {"limit": Input("int", "Limit", lo=1, hi=1000)}, form="python sdp.py broker audit --limit=<n>"),
    Action("db.restart_needed", "read", "The running services whose settings changed after they started", form="python sdp.py db restart-needed"),
    Action("db.llm_settings", "read", "The current answers of the MCP servers' and the scheduler's setup", form="python sdp.py db llm-settings"),
    Action("db.index_targets", "read", "The tables, columns and indexes of a source in one database", {"source": _SOURCE, "db": Input("enum", "Database", required=True, choices=DATABASES)}, form="python sdp.py db index-targets --source=<name> --db=<database>"),
    Action("db.settings", "read", "The current answers of the database setup", form="python sdp.py db settings"),
    Action("source.settings", "read", "The current answers of a source's setup", {"name": _SOURCE}, form="python sdp.py source settings <name>"),
    Action("chat.settings", "read", "The chat's settings for the whole deployment (the read-only query limit, generated chat titles)", form="python sdp.py chat settings"),
    # ── write ──
    Action(
        "run.start", "write", "Start a pipeline run",
        {
            "profile": Input("enum", "Profile", required=True, choices=VALID_PROFILES),
            "source": _SOURCE,
            "build": Input("bool", "Rebuild the image first"),
            "filter": Input("filter", "Only the files whose id matches"),
            "skip_lingua_files": Input("bool", "Also skip files that already have a lingua output"),
            "classifier": Input("name", "Only this classifier"),
            "admin_password": Input("secret", "Database admin password"),
        },
        form="python sdp.py run <profile> --source=<name> [--build] [--filter=<pattern>] [--skip-lingua-files] [--classifier=<name>]",
        command=_run_start_command, terminal_asks=_ASKS_PASSWORD, check=_check_run_start,
    ),
    Action("db.stop", "write", "Stop database services", {"service": Input("enum", "Service", choices=SERVICES, absent=_ALL_SERVICES)}, form="python sdp.py db stop [<service>]", command=_service_command("stop"), note=_REFUSED_WHILE_A_RUN_LIVES, also=_stop_also),
    Action("run.cancel", "write", "Cancel a run", {"run_id": Input("run_id", "Run", required=True)}, form="python sdp.py runs cancel <run>", command=lambda fields: _command("runs", "cancel", _q(fields["run_id"]))),
    Action(
        "source.download", "write", "Download a Hugging Face dataset", {"source": _SOURCE, "token": Input("secret", "Hugging Face token (private datasets only)")},
        form="python sdp.py source download <name>", command=lambda fields: _command("source", "download", _q(fields["source"])),
        also=source_download_sentence,
    ),
    Action("session.clear_db_password", "write", "Forget the database admin password of this session", form="(no command: the terminal keeps no password)", op=True),
    Action(
        "chat.auto_title", "write", "Switch generated chat titles on or off",
        {"value": Input("enum", "Generated chat titles", required=True, choices=AUTO_TITLE_CHOICES)},
        form="python sdp.py chat auto-title on|off", command=lambda fields: _command("chat", "auto-title", _q(fields["value"])),
        also=lambda fields: AUTO_TITLE_SENTENCE,
    ),
    # ── step-up ──
    Action(
        "db.start", "step_up", "Start database services",
        {"service": Input("enum", "Service", choices=SERVICES, absent=_ALL_SERVICES), "admin_password": Input("secret", "Database admin password")},
        form="python sdp.py db start [<service>]", command=_service_command("start"), terminal_asks=_ASKS_PASSWORD, also=_start_also,
    ),
    Action(
        "db.reset", "step_up", "Reset a database", {"db": Input("enum", "Database", required=True, choices=DATABASES)},
        form="python sdp.py db reset --db=<database>", command=lambda fields: _command("db", "reset", "--db=" + _q(fields["db"])),
        destroys=_reset_sentence, terminal_asks=_ASKS_CONFIRMATION, note=_REFUSED_WHILE_A_RUN_LIVES,
    ),
    Action(
        "db.unsetup", "step_up", "Remove a database", {"db": Input("enum", "Database", required=True, choices=DATABASES)},
        form="python sdp.py db unsetup --db=<database>", command=lambda fields: _command("db", "unsetup", "--db=" + _q(fields["db"])),
        destroys=_unsetup_sentence,
        terminal_asks=_ASKS_CONFIRMATION, note=_REFUSED_WHILE_A_RUN_LIVES,
    ),
    Action(
        "db.unsetup_mcp", "step_up", "Remove the MCP servers' configuration",
        form="python sdp.py db unsetup-mcp", command=lambda fields: _command("db", "unsetup-mcp"),
        destroys=lambda fields: ("Stops each database that has an MCP server configured, together with that server (the CLI stops the two as one), and removes the MCP servers' configuration: config/db/mcp.yaml and their .env entries.", "the MCP servers' configuration, and the databases they serve stopped"),
        note=_REFUSED_WHILE_A_RUN_LIVES,
    ),
    Action(
        "db.unsetup_jobs", "step_up", "Remove the scheduler's configuration",
        form="python sdp.py db unsetup-jobs", command=lambda fields: _command("db", "unsetup-jobs"),
        destroys=lambda fields: ("Stops the query scheduler and removes its configuration: config/jobs/config.local.yaml and its .env entries (JOBS_PORT, JOBS_RESULT_ROOT). It also removes the scheduler's export mount (/jobs_export) from docker-compose.override.yml, so PostgreSQL no longer has one from its next start; StarRocks keeps one, because docker-compose.yml mounts ${JOBS_RESULT_ROOT:-./data/jobs/results} at /jobs_export, which without JOBS_RESULT_ROOT in .env is ./data/jobs/results from its next start. The job store (./data/jobs) and the results on disk are kept.", "the scheduler's configuration"),
    ),
    Action(
        "source.remove", "step_up", "Remove a source", {"name": _SOURCE},
        form="python sdp.py source remove <name>", command=lambda fields: _command("source", "remove", _q(fields["name"])),
        destroys=lambda fields: ("Removes the source's configuration folder config/sources/<name>/. Its data files (dumps, extracted and parsed files, output) are not removed.", fields["name"]),
        terminal_asks=_ASKS_CONFIRMATION, note=_REFUSED_WHILE_A_RUN_LIVES,
    ),
    Action(
        "session.set_db_password", "step_up", "Keep the database admin password for this session", {"password": Input("secret", "Database admin password", required=True)},
        form="(no command: a terminal asks for the password each time a command needs it)", op=True,
    ),
    Action(
        "db.setup_mcp", "step_up", "Set up the MCP servers",
        {"postgres": Input("record", "PostgreSQL MCP server", fields=_MCP_SERVER_FIELDS, absent="off"), "mongo": Input("record", "MongoDB MCP server", fields=_MCP_SERVER_FIELDS, absent="off"), "starrocks": Input("record", "StarRocks MCP server", fields={"port": Input("port", "Port", required=True)}, absent="off")},
        form="python sdp.py db setup-mcp", command=lambda fields: _command("db", "setup-mcp"),
        terminal_asks=_ASKS_QUESTIONS, also=_setup_mcp_sentence, warns=_setup_mcp_warns,
    ),
    Action(
        "db.setup_jobs", "step_up", "Set up the query scheduler",
        {
            "port": Input("port", "Port of the page and of /mcp", required=True),
            "result_root": Input("path", "Results folder", required=True),
            "max_concurrent": Input("int", "Queries at once", required=True, lo=1, hi=1000),
            "history_retention": Input("int", "Jobs kept in the history", required=True, lo=1, hi=100000),
            "postgres_target": Input("record", "PostgreSQL target", fields={"name": Input("name", "Target name", required=True), "database": Input("name", "Database", required=True), "timeout_hours": _HOURS_INPUT}, absent="none"),
            "starrocks_target": Input("record", "StarRocks target", fields={"name": Input("name", "Target name", required=True), "timeout_hours": Input("int", "Default timeout (hours, 1 to 72)", required=True, lo=1, hi=72)}, absent="none"),
            "mongo_target": Input("record", "MongoDB target", fields={"name": Input("name", "Target name", required=True), "timeout_hours": _HOURS_INPUT}, absent="none"),
            "auth_enabled": Input("bool", "The scheduler page asks for the database admin password", absent="no"),
        },
        form="python sdp.py db setup-jobs", command=lambda fields: _command("db", "setup-jobs"),
        terminal_asks=_ASKS_QUESTIONS, also=_setup_jobs_sentence, check=_check_setup_jobs,
    ),
    Action(
        "db.create_indexes", "step_up", "Create indexes",
        {"source": _SOURCE, "database": Input("enum", "Database", required=True, choices=DATABASES), "indexes": _INDEX_REQUEST, "save": Input("bool", "Save the new indexes to platform.yaml"), "admin_password": Input("secret", "Database admin password")},
        form="python sdp.py db create-indexes --source=<name>", command=lambda fields: _command("db", "create-indexes", "--source=" + _q(fields["source"])),
        terminal_asks="In a terminal this command asks which database, the tables, the fields of each, BITMAP or bloom filter for StarRocks, whether to save them, and the database admin password where it needs one; the command shown holds none of these answers.",
        check=_check_create_indexes, also=_indexes_also,
    ),
    Action(
        "chat.query_limit", "step_up", "Change the read-only query limit for chats",
        {"seconds": Input("int", "Read-only query limit for chats (seconds)", required=True, lo=1, hi=CHAT_QUERY_SECONDS_MAX)},
        form="python sdp.py chat query-limit <seconds>", command=lambda fields: _command("chat", "query-limit", str(fields["seconds"])),
        also=lambda fields: QUERY_LIMIT_SENTENCE,
    ),
    Action("session.check_password", "step_up", "Check the management password again", form="(no command: asked before the Queue's auto-accept is switched on or its limit raised)", op=True),
    # ── the scheduler's control socket ──
    Action("queue.counts", "read", "The numbers of the Queue's tabs", form="(the scheduler's page: the tabs)", door="counts"),
    Action("queue.stats", "read", "The figures of the Queue's tab bar: the results folder's size and free space, the slots and how many are busy", form="(no command: the scheduler's configuration and its results folder)", door="stats"),
    Action("queue.jobs", "read", "One page of a tab of the Queue", {"phase": Input("enum", "Tab", required=True, choices=PHASES), "limit": Input("int", "Limit", required=True, lo=1, hi=JOBS_LIMIT_MAX), "offset": Input("int", "Offset", required=True, lo=0, hi=JOBS_OFFSET_MAX)}, form="(the scheduler's page: a tab)", door="jobs"),
    Action("queue.job", "read", "One job of the Queue", {"job_id": Input("job_id", "Job", required=True)}, form="(the scheduler's page: a job's row)", door="job"),
    Action("queue.preview", "read", "The preview of a finished job's result", {"job_id": Input("job_id", "Job", required=True)}, form="(the scheduler's page: Preview)", door="preview"),
    Action("queue.auto_accept_state", "read", "The auto-accept settings of the scheduler's targets", form="(the scheduler's page: the auto-accept panel)", door="auto_accept"),
    Action("queue.approve", "write", "Approve a query", {"job_id": Input("job_id", "Job", required=True), "digest": Input("digest", "Digest", required=True), "by": Input("by", "Asked by", required=True)}, form="(the scheduler's page: Approve)", door="approve"),
    Action("queue.reject", "write", "Reject a query", {"job_id": Input("job_id", "Job", required=True), "reason": Input("reason", "Reason", required=True), "by": Input("by", "Asked by", required=True)}, form="(the scheduler's page: Reject)", door="reject"),
    Action("queue.kill", "write", "Kill a running query", {"job_id": Input("job_id", "Job", required=True), "by": Input("by", "Asked by", required=True)}, form="(the scheduler's page: Kill)", door="kill"),
    Action("queue.explain", "write", "EXPLAIN a query on its database", {"job_id": Input("job_id", "Job", required=True), "by": Input("by", "Asked by", required=True)}, form="(the scheduler's page: EXPLAIN)", door="explain"),
    Action(
        "queue.auto_accept", "write", "Change a target's auto-accept",
        {"target": Input("target", "Target", required=True), "enabled": Input("opt_bool", "Auto-accept", required=True), "limit": Input("opt_int", "Limit", required=True, lo=0, hi=10 ** LIMIT_DIGITS - 1), "by": Input("by", "Asked by", required=True)},
        form="(the scheduler's page: the auto-accept panel; switching it on or raising the limit needs the management password again)", door="auto_accept.set",
    ),
    # ── setup (step-up) ──
    Action(
        "source.add", "step_up", "Add a source", SOURCE_ADD_INPUTS,
        form="python sdp.py source add <name> [--hf <owner>/<dataset>] (the questions are the wizard's steps)", command=lambda fields: _command("source", "add", _q(fields["name"]), *(("--hf=" + _q(fields["hf_dataset"]),) if "hf_dataset" in fields else ())),
        also=lambda fields: source_add_sentence(fields),
    ),
    _db_setup_entry(),
    Action(
        "source.configure", "step_up", "Reconfigure a source", SOURCE_CONFIGURE_INPUTS,
        form="python sdp.py source configure <name> (the questions are the wizard's steps)", command=lambda fields: _command("source", "configure", _q(fields["name"])),
        also=source_configure_sentence,
    ),
    Action(
        "source.add_classifiers", "step_up", "Add classifiers to a source", SOURCE_CLASSIFIER_INPUTS,
        form="python sdp.py source add-classifiers <name> (the questions are the wizard's Classifiers step)", command=lambda fields: _command("source", "add-classifiers", _q(fields["name"])),
        also=source_add_classifiers_sentence,
    ),
)

# The entries of the scheduler's control socket, by name.
QUEUE_ENTRIES = tuple(name for name, action in TABLE.items() if action.door is not None)

# The source verbs, for which a source name is required (the first of the web side's two stricter places).
SOURCE_VERBS = ("source.status", "source.progress", "source.size", "source.error_logs", "run.start", "source.remove")


def run_form_inputs(profile):
    """The inputs of `run.start` the form of one profile offers besides the profile and the source: build always; the file filter except for `sr_ingest` and `sr_ml`; skipping the files with a lingua output for `parse`; one classifier for `ml`, `lingua` and `sr_ml`; the database admin password for the profiles that use a database."""
    inputs = ["build"]
    if profile not in NO_FILTER_PROFILES:
        inputs.append("filter")
    if profile == "parse":
        inputs.append("skip_lingua_files")
    if profile in CLASSIFIER_PROFILES:
        inputs.append("classifier")
    if profile in DATABASE_PROFILES:
        inputs.append("admin_password")
    return inputs


def has_control_character(value):
    """Whether a text holds a control character or a line or paragraph separator (Unicode categories Cc, Zl, Zp), as the registry refuses."""
    return any(unicodedata.category(character) in ("Cc", "Zl", "Zp") for character in value)


def is_plain_name(value):
    """Whether `value` is a plain name: the registry's pattern, and not only dots."""
    return isinstance(value, str) and re.fullmatch(PLAIN_NAME, value) is not None and value.strip(".") != ""


def is_hf_dataset(value):
    """Whether `value` is a Hugging Face dataset id the registry takes (`<owner>/<name>`, no `--` or `..`)."""
    return isinstance(value, str) and re.fullmatch(HF_DATASET_PATTERN, value) is not None and "--" not in value and ".." not in value


def is_run_id(value):
    return isinstance(value, str) and re.fullmatch(RUN_ID_PATTERN, value) is not None


def is_job_id(value):
    """Whether `value` has the form of a job id of the scheduler."""
    return isinstance(value, str) and re.fullmatch(JOB_ID_PATTERN, value) is not None


def is_reason(value):
    """Whether `value` is a reason the scheduler takes for a rejection: at most 500 characters, no line break or control character."""
    return isinstance(value, str) and len(value) <= REASON_MAX and not has_control_character(value)


def _is_whole(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _utf8_length(value):
    """The UTF-8 length of a text, or None for a lone surrogate (which the registry refuses)."""
    try:
        return len(value.encode("utf-8"))
    except UnicodeEncodeError:
        return None


def _is_text(spec, value):
    """The registry's `Text`: a tab, and with multiline `\\n`, are the only control characters allowed."""
    allowed = ("\t", "\n") if spec.multiline else ("\t",)
    if any(character not in allowed and unicodedata.category(character) in ("Cc", "Zl", "Zp") for character in value):
        return False
    length = _utf8_length(value)
    return length is not None and length <= spec.max_bytes


def _container_is_valid(spec, value):
    kind = spec.kind
    if kind == "list":
        if not isinstance(value, list) or len(value) > spec.max_items or not all(_value_is_valid(spec.item, item) for item in value):
            return False
        return not spec.unique or len(set(value)) == len(value)
    if kind == "map":
        if not isinstance(value, dict) or len(value) > spec.max_items:
            return False
        return all(_value_is_valid(spec.key, key) for key in value) and all(_value_is_valid(spec.item, item) for item in value.values())
    # record
    if not isinstance(value, dict) or not all(key in spec.fields for key in value):
        return False
    if any(sub.required and key not in value for key, sub in spec.fields.items()):
        return False
    return all(_value_is_valid(spec.fields[key], item) for key, item in value.items())


def _value_is_valid(spec, value):
    kind = spec.kind
    if kind in CONTAINER_KINDS:
        return _container_is_valid(spec, value)
    if kind == "port":
        return _is_whole(value) and PORT_MIN <= value <= PORT_MAX
    if kind == "bool":
        return isinstance(value, bool)
    if kind == "int":
        return isinstance(value, int) and not isinstance(value, bool) and spec.lo <= value <= spec.hi
    if kind == "opt_int":
        return value is None or (isinstance(value, int) and not isinstance(value, bool) and spec.lo <= value <= spec.hi)
    if kind == "opt_bool":
        return value is None or isinstance(value, bool)
    if kind == "secret":
        if not isinstance(value, Hidden):
            return False
        secret = value.reveal()
        return isinstance(secret, str) and 1 <= len(secret) <= SECRET_MAX_CHARS and not has_control_character(secret)
    if not isinstance(value, str):
        return False
    if kind == "enum":
        return value in spec.choices
    if kind in ("name", "source"):
        return is_plain_name(value)
    if kind == "filter":
        return re.fullmatch(FILE_FILTER_PATTERN, value) is not None and value.strip(".") != ""
    if kind == "run_id":
        return is_run_id(value)
    if kind == "hf_dataset":
        return is_hf_dataset(value)
    if kind == "job_id":
        return is_job_id(value)
    if kind == "digest":
        return re.fullmatch(DIGEST_PATTERN, value) is not None
    if kind == "by":
        return re.fullmatch(BY_PATTERN, value) is not None
    if kind == "reason":
        return is_reason(value)
    if kind == "target":
        return 1 <= len(value) <= TARGET_MAX_CHARS and not has_control_character(value)
    if kind == "path":
        length = _utf8_length(value)
        return length is not None and 1 <= length <= spec.max_bytes and not has_control_character(value)
    if kind == "text":
        return _is_text(spec, value)
    return False


def check_fields(action, fields):
    """Check fields for an action: only its inputs, each of its kind, the required ones present, and its cross-field rules. Raises Refused."""
    if not isinstance(fields, dict):
        raise Refused()
    for name, value in fields.items():
        spec = action.inputs.get(name)
        if spec is None:
            raise Refused()
        if not _value_is_valid(spec, value):
            raise Refused(name)
    for name, spec in action.inputs.items():
        if spec.required and name not in fields:
            raise Refused(name)
    if action.check is not None:
        action.check(fields)
    return fields


def form_name(name, *segments):
    """The form name of a value nested in the input `name`: `form_name("targets", 2, "name")` is `targets[2][name]` (see the module's docstring)."""
    return name + "".join(f"[{segment}]" for segment in segments)


# What `_from_form` returns for a value the form left empty.
_ABSENT = object()


def _leaf_from_form(spec, text):
    """One posted text as the value of a scalar input, or _ABSENT. Raises ValueError for a text that cannot be one."""
    if not isinstance(text, str):
        raise ValueError("a branch where a value was expected")
    if spec.kind == "bool":
        if text != CHECKED:
            raise ValueError("a box posts yes or nothing")
        return True
    if text == "":
        return _ABSENT
    if spec.kind in ("int", "port"):
        if re.fullmatch(FORM_INT_PATTERN, text) is None or (text.startswith("-") and spec.kind == "port"):
            raise ValueError("not a whole number")
        return int(text)
    if spec.kind == "secret":
        return Hidden(text)
    if spec.kind == "path":
        return posixpath.normpath(text) if not has_control_character(text) else text
    if spec.kind == "text":
        return text.replace("\r\n", "\n").replace("\r", "\n")
    if spec.kind in TEXT_KINDS:
        return text
    raise ValueError("a kind a form cannot post")


def _from_form(spec, node):
    """The value of the input `spec` from its part of the posted names (a text, or a dict of segment to part), or _ABSENT. Raises ValueError for a shape the scheme does not have."""
    if spec.kind not in CONTAINER_KINDS:
        return _leaf_from_form(spec, node)
    if not isinstance(node, dict):
        raise ValueError("a value where a branch was expected")
    if spec.kind == "list":
        removed = node.get(REMOVE_SEGMENT, {})
        if not isinstance(removed, dict) or not all(re.fullmatch(INDEX_PATTERN, index) and text == CHECKED for index, text in removed.items()):
            raise ValueError("a remove box that is not one")
        indexes = [segment for segment in node if segment != REMOVE_SEGMENT]
        if not all(re.fullmatch(INDEX_PATTERN, index) for index in indexes):
            raise ValueError("a row that is not numbered")
        numbers = sorted(int(index) for index in indexes)
        if numbers != list(range(len(numbers))) or len(numbers) > spec.max_items or any(int(index) >= len(numbers) for index in removed):
            raise ValueError("rows missing or too many")
        items = []
        for number in numbers:
            value = _from_form(spec.item, node[str(number)])
            if value is not _ABSENT and str(number) not in removed:
                items.append(value)
        return items if items else _ABSENT
    if spec.kind == "map":
        if len(node) > spec.max_items:
            raise ValueError("too many entries")
        entries = {}
        for key, part in node.items():
            if not _value_is_valid(spec.key, key):
                raise ValueError("a key the map does not take")
            value = _from_form(spec.item, part)
            if value is not _ABSENT:
                entries[key] = value
        return entries if entries else _ABSENT
    # record
    values = {}
    for key, part in node.items():
        if key not in spec.fields:
            raise ValueError("a key the record does not have")
        value = _from_form(spec.fields[key], part)
        if value is not _ABSENT:
            values[key] = value
    return values if values else _ABSENT


def _posted_tree(action, form):
    """The posted names grouped by input: a text for a plain name, a dict of segment to part for a bracketed one. Raises Refused for a name the scheme does not have."""
    tree = {}
    for name, text in form.items():
        if not isinstance(name, str) or not isinstance(text, str):
            raise Refused()
        if "[" not in name:
            if name not in action.inputs or name in tree:
                raise Refused()
            tree[name] = text
            continue
        match = FORM_NAME.fullmatch(name)
        if match is None or match.group(1) not in action.inputs or action.inputs[match.group(1)].kind not in CONTAINER_KINDS:
            raise Refused()
        top = match.group(1)
        segments = FORM_SEGMENT.findall(match.group(2))
        node = tree.setdefault(top, {})
        if not isinstance(node, dict):
            raise Refused(top)
        for segment in segments[:-1]:
            node = node.setdefault(segment, {})
            if not isinstance(node, dict):
                raise Refused(top)
        if segments[-1] in node:
            # A value and a branch at one place.
            raise Refused(top)
        node[segments[-1]] = text
    return tree


def parse_form(action, form):
    """The fields of a state-changing action from the fields a form posted (every value text, nested values under the bracketed names of the module's docstring): an empty text is an absent field, a checked box is `yes`, a secret is wrapped in `Hidden`, an `int` or a `port` is decimal digits, a `path` is normalized, a `text` has its line ends made `\\n`; then `check_fields`. Raises Refused for a field the action does not take, a value it refuses, a name or a shape outside the scheme, a missing required one and a broken cross-field rule; nothing is asked of the broker. The scheduler's entries are not made from a form here (see `queue`)."""
    if not action.state_changing or action.door is not None:
        raise Refused()
    fields = {}
    for name, node in _posted_tree(action, form).items():
        try:
            value = _from_form(action.inputs[name], node)
        except ValueError:
            raise Refused(name) from None
        if value is not _ABSENT:
            fields[name] = value
    return check_fields(action, fields)


def shown_inputs(action, fields):
    """(label, text) for each input an action carries, in the table's order: a secret as dots, a checked box as `yes`; an input left out whose absence means something (no service: every configured one) with that meaning."""
    shown = []
    for name, spec in action.inputs.items():
        if name not in fields:
            if spec.absent is not None:
                shown.append((spec.label, spec.absent))
            continue
        shown.append((spec.label, shown_value(spec, fields[name])))
    return shown


def shown_value(spec, value):
    """The text of one value on the pending page: a secret as dots, a box as yes or no, a list as its items joined by `, ` (`none` when empty), a map or a record as `key: value` joined by `; `, at any depth (a record's field left out shown with what its absence means, when that is worth saying)."""
    if spec.secret:
        return SECRET_DOTS
    if spec.kind == "bool":
        return "yes" if value else "no"
    if spec.kind == "list":
        return ", ".join(shown_value(spec.item, item) for item in value) if value else "none"
    if spec.kind == "map":
        return "; ".join(f"{key}: {shown_value(spec.item, item)}" for key, item in value.items()) if value else "none"
    if spec.kind == "record":
        # In the record's order; a field left out whose absence means something (an unticked write-access box: read-only) with that meaning.
        return "; ".join(f"{field.label}: {shown_value(field, value[key])}" if key in value else f"{field.label}: {field.absent}" for key, field in spec.fields.items() if key in value or field.absent is not None)
    return str(value)


def revealed(value):
    """A copy of a value with every `Hidden` in it, at any depth, replaced by its text: what is sent."""
    if isinstance(value, Hidden):
        return value.reveal()
    if isinstance(value, list):
        return [revealed(item) for item in value]
    if isinstance(value, dict):
        return {key: revealed(item) for key, item in value.items()}
    return value


def call(client, name, fields=None, step_up=None):
    """Ask the broker through `client` for the entry `name` of the table, with `fields` (secrets in `Hidden`) and, for an entry of the class `step_up`, the management password again; for a `queue.*` entry, ask the scheduler's control socket through `client` (a client of that socket) for its operation with exactly `fields`. The one way the pages ask the broker or the scheduler anything: a name outside the table, fields the table refuses, or a management password given to an entry that does not take one (or none to one that does) raise NotInTable before anything is sent."""
    action = TABLE.get(name) if isinstance(name, str) else None
    if action is None:
        raise NotInTable("the page asked for something the table does not hold")
    fields = {} if fields is None else fields
    try:
        check_fields(action, fields)
    except Refused:
        raise NotInTable("the page asked with fields the table refuses") from None
    if (action.cls == "step_up") != isinstance(step_up, str):
        raise NotInTable("the management password goes with the step-up entries only")
    plain = revealed(fields)
    if action.door is not None:
        return client.request({"op": action.door, **plain})
    if name == "hello":
        return client.hello()
    if name == "session.set_db_password":
        return client.set_db_password(plain["password"], step_up)
    if name == "session.clear_db_password":
        return client.clear_db_password()
    if name == "session.check_password":
        return client.check_password(step_up)
    if step_up is None:
        return client.call(name, plain)
    return client.call(name, plain, step_up=step_up)
