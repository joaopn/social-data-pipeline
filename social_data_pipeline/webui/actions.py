"""The web UI's own table of what its pages may ask the broker for, and `call`, the one function through which every request to the broker goes.

The image holds no registry (`control.verbs`), so the table is written out here: for each verb the pages use, its class (`read`, `write`, `step_up`, the registry's), its inputs with their kinds and closed choices, the builder of the `sdp` command shown to the operator, the line about what a terminal would ask, and for a destructive verb one sentence saying what it destroys. A test compares it with the registry: every verb exists there with the same class and is enabled on the socket, and its inputs are a subset of the registry's socket inputs with the same choices. The web side is stricter than the registry in two places, on purpose: a source name is always required for the source verbs, and `run.logs` pieces are at least 4096 bytes. Besides the verbs the table has four operations of the broker that are not verbs: `hello`, `session.set_db_password` (needs the management password), `session.clear_db_password` and `session.check_password` (the management password checked again, and nothing else).

A second kind of entry is the query scheduler's control socket (`jobs/door.py`), whose entries are named `queue.*` and carry the name of the socket's operation in `door`: the reads `queue.counts`, `queue.jobs`, `queue.job`, `queue.preview` and `queue.auto_accept_state`, and the changes `queue.approve`, `queue.reject`, `queue.kill`, `queue.explain` and `queue.auto_accept` (`auto_accept.set`). The socket takes every listed field of an operation and nothing else, so every input of a `queue.*` entry is required (`opt_bool` and `opt_int` take None, which the socket reads as "leave it as it is"). A test compares them with the socket's own list of operations and fields.

`parse_form` turns what a form posted into the fields of an action, checked by the table (closed choices, the plain-name pattern for source and classifier names, the file-filter pattern, the cross-field rules of `run.start`, unknown fields refused); `check_fields` checks fields a page made itself, and `call` checks again before it sends anything, so that no page can make the broker receive a verb, a field or a value outside this table. A secret is held in `Hidden`, which no `repr`, `str` or format shows. Standard library and this package only.
"""

import re
import shlex
import unicodedata
from dataclasses import dataclass
from types import MappingProxyType
from typing import Callable

from social_data_pipeline.webui.overview import PLAIN_NAME

# Literal copies of the registry's patterns and choices (`control.types`, `control.verbs`): a test compares them.
FILE_FILTER_PATTERN = r"[A-Za-z0-9_.*?\[\]!-]{1,128}"
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
    """One input of an action: its kind (`enum`, `bool`, `name`, `source`, `filter`, `secret`, `run_id`, `int`; for the scheduler's entries also `job_id`, `digest`, `by`, `reason`, `target`, `opt_bool` and `opt_int`), whether a request must carry it, the choices of an `enum`, the bounds of an `int`, the label a page shows, and what the input left out means when that is worth saying."""

    kind: str
    label: str
    required: bool = False
    choices: tuple = ()
    lo: int = 0
    hi: int = 0
    # What an optional input left out means, shown as its value on the pending page (None: the row is left out).
    absent: str | None = None

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
    Action("source.error_logs", "read", "Error logs of a source", {"name": _SOURCE, "profile": Input("enum", "Profile", required=True, choices=ERROR_LOG_PROFILES)}, form="python sdp.py source error-logs <name> --profile=<profile>"),
    Action("run.list", "read", "List the runs", {"limit": Input("int", "Limit", lo=1, hi=1000)}, form="python sdp.py runs list --limit=<n>"),
    Action("run.logs", "read", "Read a run's output", {"run_id": Input("run_id", "Run", required=True), "offset": Input("int", "Offset", lo=0, hi=LOG_OFFSET_MAX), "limit": Input("int", "Limit", lo=RUN_LOGS_MIN_LIMIT, hi=RUN_LOGS_MAX_LIMIT)}, form="python sdp.py runs logs <run> --offset=<bytes> --limit=<bytes>"),
    Action("audit.list", "read", "The newest lines of the audit log", {"limit": Input("int", "Limit", lo=1, hi=1000)}, form="python sdp.py broker audit --limit=<n>"),
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
    Action("session.clear_db_password", "write", "Forget the database admin password of this session", form="(no command: the terminal keeps no password)", op=True),
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
    Action("session.check_password", "step_up", "Check the management password again", form="(no command: asked before the Queue's auto-accept is switched on or its limit raised)", op=True),
    # ── the scheduler's control socket ──
    Action("queue.counts", "read", "The numbers of the Queue's tabs", form="(the scheduler's page: the tabs)", door="counts"),
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
)

# The entries of the scheduler's control socket, by name.
QUEUE_ENTRIES = tuple(name for name, action in TABLE.items() if action.door is not None)

# The source verbs, for which a source name is required (the first of the web side's two stricter places).
SOURCE_VERBS = ("source.status", "source.progress", "source.error_logs", "run.start", "source.remove")


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


def is_run_id(value):
    return isinstance(value, str) and re.fullmatch(RUN_ID_PATTERN, value) is not None


def is_job_id(value):
    """Whether `value` has the form of a job id of the scheduler."""
    return isinstance(value, str) and re.fullmatch(JOB_ID_PATTERN, value) is not None


def is_reason(value):
    """Whether `value` is a reason the scheduler takes for a rejection: at most 500 characters, no line break or control character."""
    return isinstance(value, str) and len(value) <= REASON_MAX and not has_control_character(value)


def _value_is_valid(spec, value):
    kind = spec.kind
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


def parse_form(action, form):
    """The fields of a state-changing action from the fields a form posted (every value text): an empty text is an absent field, a checked box is `yes`, a secret is wrapped in `Hidden`; then `check_fields`. Raises Refused for a field the action does not take, a value it refuses, a missing required one and a broken cross-field rule; nothing is asked of the broker. The scheduler's entries are not made from a form here (see `queue`)."""
    if not action.state_changing or action.door is not None:
        raise Refused()
    fields = {}
    for name, text in form.items():
        spec = action.inputs.get(name)
        if spec is None or not isinstance(text, str):
            raise Refused()
        if spec.kind == "bool":
            if text != CHECKED:
                raise Refused(name)
            fields[name] = True
        elif text == "":
            continue
        elif spec.kind == "secret":
            fields[name] = Hidden(text)
        elif spec.kind == "int":
            raise Refused(name)
        else:
            fields[name] = text
    return check_fields(action, fields)


def shown_inputs(action, fields):
    """(label, text) for each input an action carries, in the table's order: a secret as dots, a checked box as `yes`; an input left out whose absence means something (no service: every configured one) with that meaning."""
    shown = []
    for name, spec in action.inputs.items():
        if name not in fields:
            if spec.absent is not None:
                shown.append((spec.label, spec.absent))
            continue
        value = fields[name]
        if spec.secret:
            shown.append((spec.label, SECRET_DOTS))
        elif spec.kind == "bool":
            shown.append((spec.label, "yes" if value else "no"))
        else:
            shown.append((spec.label, str(value)))
    return shown


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
    plain = {key: value.reveal() if isinstance(value, Hidden) else value for key, value in fields.items()}
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
