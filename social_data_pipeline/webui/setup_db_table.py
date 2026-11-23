"""The web UI's table entry of `db.setup` (`sdp db setup`, a database not set up yet): its inputs, the cross-field check, the command shown and the confirm page's lines of what it writes.

`entry(Input, Action, Refused)` builds the entry; `actions` calls it while it builds its table, so this module imports nothing of `actions` (the classes are handed in). The inputs mirror the registry's (`control.verbs.DB_SETUP_INPUTS`) and the check mirrors `control.dbsetup.check` rule by rule (a test runs both on the same answers): the image holds neither. Standard library only.
"""

import re
import shlex

DATABASES = ("postgres", "mongo", "starrocks")
DATABASE_NAMES = {"postgres": "PostgreSQL", "mongo": "MongoDB", "starrocks": "StarRocks"}
PER_DATABASE = {
    "postgres": ("pgdata_path", "db_name", "pg_port", "tablespaces", "filesystem", "pgtune", "pg_mem_limit"),
    "mongo": ("mongo_data_path", "mongo_port", "mongo_cache_size_gb", "mongo_mem_limit", "mongo_validate"),
    "starrocks": ("starrocks_data_path", "starrocks_port", "starrocks_fe_http_port", "sr_fe_jvm_heap", "starrocks_mem_limit", "sr_be_mem_limit", "sr_alter_tablet_workers", "sr_compression", "starrocks_storage_paths"),
}
FIRST_TIME_ONLY = ("databases", "data_path", "export_path")
AUTH_INPUTS = ("auth", "new_password", "new_password_confirm", "ro_user", "ro_username", "ro_password_mode", "ro_password", "ro_password_confirm")
PORT_FIELDS = ("pg_port", "mongo_port", "starrocks_port", "starrocks_fe_http_port")
PATH_FIELDS = ("data_path", "export_path", "pgdata_path", "mongo_data_path", "starrocks_data_path")
SQL_NAME = r"[a-z_][a-z0-9_]{0,62}"
PATH_FORBIDDEN = frozenset("$:#\"'\\`")
RO_PASSWORD_FORBIDDEN = frozenset("'\"\\`")
GB_MAX = 4096
PGTUNE_MAX_BYTES = 16384
LIST_MAX = 16
# The terminal's defaults that do not depend on the machine, as the form fills them in.
DEFAULTS = {
    "data_path": "./data", "db_name": "datasets", "pg_port": 5432, "filesystem": "standard",
    "mongo_port": 27017, "mongo_cache_size_gb": 2, "mongo_validate": "full",
    "starrocks_port": 9030, "starrocks_fe_http_port": 8030, "sr_compression": "ZSTD",
    "ro_username": "readonly", "ro_password_mode": "generate",
}
ASKS = "In a terminal this command asks each question in turn, the admin password twice; the command shown holds no answer. Here the answers were given in the steps before, and the passwords are shown as dots."


def chosen(fields):
    """The databases the answers set up, in the CLI's order."""
    if "add" in fields:
        return [fields["add"]]
    return [name for name in DATABASES if name in fields.get("databases", [])]


def _plain(value):
    return value.reveal() if hasattr(value, "reveal") else value


def _path_ok(value):
    return value == value.strip() and not any(character in PATH_FORBIDDEN for character in value)


def ro_password_ok(value):
    return not any(character.isspace() or character in RO_PASSWORD_FORBIDDEN for character in value)


def first_refusal(fields):
    """The name of the first input `control.dbsetup.check` refuses for these answers, or None: the same rules in the same order."""
    add = "add" in fields
    if add == ("databases" in fields):
        return "databases"
    if add:
        for name in FIRST_TIME_ONLY:
            if name in fields:
                return name
    elif not fields["databases"]:
        return "databases"
    databases = chosen(fields)
    for database, names in PER_DATABASE.items():
        if database not in databases:
            for name in names:
                if name in fields:
                    return name
    for name in PATH_FIELDS:
        if name in fields and not _path_ok(fields[name]):
            return name
    if not all(_path_ok(path) for path in fields.get("starrocks_storage_paths", [])):
        return "starrocks_storage_paths"
    seen = set()
    for tablespace in fields.get("tablespaces", []):
        if re.fullmatch(SQL_NAME, tablespace["name"]) is None or tablespace["name"] == "pgdata" or tablespace["name"] in seen or not _path_ok(tablespace["path"]):
            return "tablespaces"
        seen.add(tablespace["name"])
    ports = [fields[name] for name in PORT_FIELDS if name in fields]
    for name in PORT_FIELDS:
        if name in fields and ports.count(fields[name]) != 1:
            return name
    auth = fields.get("auth") is True
    for name in ("new_password", "new_password_confirm", "ro_user"):
        if not auth and name in fields:
            return name
    if auth:
        for name in ("new_password", "new_password_confirm"):
            if name not in fields:
                return name
        if _plain(fields["new_password"]) != _plain(fields["new_password_confirm"]):
            return "new_password_confirm"
    ro_user = auth and fields.get("ro_user") is True
    for name in ("ro_username", "ro_password_mode"):
        if not ro_user and name in fields:
            return name
    if "ro_username" in fields and re.fullmatch(SQL_NAME, fields["ro_username"]) is None:
        return "ro_username"
    typed = ro_user and fields.get("ro_password_mode") == "typed"
    for name in ("ro_password", "ro_password_confirm"):
        if (name in fields) != typed:
            return name
    if typed:
        if not ro_password_ok(_plain(fields["ro_password"])):
            return "ro_password"
        if _plain(fields["ro_password"]) != _plain(fields["ro_password_confirm"]):
            return "ro_password_confirm"
    return None


def command(fields):
    """The `sdp` command the answers stand for: `db setup`, or `db setup --add=<db>`."""
    if "add" in fields:
        return "python sdp.py db setup --add=" + shlex.quote(fields["add"])
    return "python sdp.py db setup"


def _or_default(fields, name, words):
    return f"{fields[name]}" if name in fields else words


def writes(fields):
    """The confirm page's lines: every file and .env key written, every folder made, and what happens to the passwords. Written from `setup.db_answers` and the functions of `setup.db` it calls, clause by clause."""
    databases = chosen(fields)
    add = "add" in fields
    auth = fields.get("auth") is True
    ro_user = auth and fields.get("ro_user") is True
    lines = []
    if add:
        name = fields["add"]
        files = {"postgres": "config/db/postgres.yaml and config/postgres/postgresql.local.conf" + (" and config/postgres/pg_hba.local.conf" if auth else ""), "mongo": "config/db/mongo.yaml", "starrocks": "config/db/starrocks.yaml, config/starrocks/fe.local.conf and config/starrocks/be.local.conf"}[name]
        lines.append(f"Writes {files}; a file that exists already is kept as a .bak copy first.")
        keys = {"postgres": "PGDATA_PATH, DB_NAME, POSTGRES_PORT, POSTGRES_MEM_LIMIT (when a limit is set)" + (", POSTGRES_AUTH_ENABLED, POSTGRES_RO_USER (with a read-only user)" if auth else ""),
                "mongo": "MONGO_DATA_PATH, MONGO_PORT, MONGO_CACHE_SIZE_GB, MONGO_MEM_LIMIT (when a limit is set)" + (", MONGO_AUTH_ENABLED, MONGO_ADMIN_USER, MONGO_RO_USER (with a read-only user)" if auth else ""),
                "starrocks": "STARROCKS_DATA_PATH, STARROCKS_PORT, STARROCKS_FE_HTTP_PORT, STARROCKS_MEM_LIMIT (when a limit is set)" + (", STARROCKS_AUTH_ENABLED, STARROCKS_RO_USER (with a read-only user)" if auth else "")}[name]
        lines.append(f"Sets these keys in .env and leaves every other line of it as it is: {keys}.")
        if (name == "postgres" and fields.get("tablespaces")) or (name == "starrocks" and fields.get("starrocks_storage_paths")):
            what = "tablespace" if name == "postgres" else "extra disk"
            lines.append(f"Rewrites docker-compose.override.yml: the {name} block becomes one mount per {what}, so every other mount of that block is dropped (the scheduler's /jobs_export mount included, until `db setup-jobs` is run again); the other services' blocks stay.")
        if "auth" not in fields:
            lines.append("When authentication is on for the databases set up already, the new one gets it too: its authentication keys are set" + (", config/postgres/pg_hba.local.conf is written" if name == "postgres" else "") + ", and the read-only user's password is read from an existing .ro_credentials and written into .ro_credentials in its data folder (made when missing). When it is off, the new database has none.")
    else:
        lines.append("Rewrites .env: its database, export and authentication keys are written anew (DATA_PATH, the keys of each database set up here, DB_EXPORT_PATH" + (", the *_AUTH_ENABLED keys, MONGO_ADMIN_USER and the *_RO_USER keys" if auth else "") + "); every other key in it is kept as it is, except HF_HOME and HF_TOKEN, which are written as empty comment lines, so a value either had is dropped (as the terminal does). The existing .env is kept as .env.bak.")
        files = []
        if "postgres" in databases:
            files.append("config/db/postgres.yaml, config/postgres/postgresql.local.conf" + (", config/postgres/pg_hba.local.conf" if auth else ""))
        if "mongo" in databases:
            files.append("config/db/mongo.yaml")
        if "starrocks" in databases:
            files.append("config/db/starrocks.yaml, config/starrocks/fe.local.conf, config/starrocks/be.local.conf")
        lines.append("Writes " + "; ".join(files) + "; a file that exists already is kept as a .bak copy first.")
        if fields.get("tablespaces") or fields.get("starrocks_storage_paths"):
            lines.append("Rewrites docker-compose.override.yml whole, with only the tablespace and extra-disk mounts: every other entry in it is dropped, the scheduler's /jobs_export mounts included (run `db setup-jobs` again to put them back).")
        lines.append("Makes the export folder " + _or_default(fields, "export_path", "<data base path>/export") + " when it is missing.")
    for name in databases:
        key = {"postgres": "pgdata_path", "mongo": "mongo_data_path", "starrocks": "starrocks_data_path"}[name]
        folder = _or_default(fields, key, f"<data base path>/database/{name}")
        if ro_user:
            lines.append(f"Makes the data folder of {DATABASE_NAMES[name]} ({folder}) when it is missing and writes the read-only user's password into .ro_credentials there (mode 0600).")
    lines.append("No other folder is made or changed here, and no data is moved.")
    if auth:
        lines.append("The admin password is not stored anywhere: you will give it when the database starts. It was typed twice to check it.")
        if ro_user:
            lines.append("The read-only password is " + ("the one you typed" if fields.get("ro_password_mode") == "typed" else "generated (32 characters)") + " and kept only in .ro_credentials; it is never shown.")
    elif not add or "auth" in fields:
        lines.append("Authentication stays off: the database takes connections without a password.")
    lines.append("Nothing is started: start the database afterwards (python sdp.py db start).")
    return lines


def entry(Input, Action, Refused):
    """The table's entry of `db.setup`, made with the table's own classes."""
    path = Input("path", "Path")

    def check(fields):
        refused = first_refusal(fields)
        if refused is not None:
            raise Refused(refused)

    inputs = {
        "add": Input("enum", "Add to the setup", choices=DATABASES),
        "databases": Input("list", "Databases", item=Input("enum", "Database", choices=DATABASES), max_items=3, unique=True),
        "data_path": Input("path", "Data base path"),
        "export_path": Input("path", "Export path"),
        "pgdata_path": Input("path", "PostgreSQL data path"),
        "db_name": Input("name", "Database name"),
        "pg_port": Input("port", "PostgreSQL port"),
        "tablespaces": Input("list", "Tablespaces", item=Input("record", "Tablespace", fields={"name": Input("name", "Name", required=True), "path": Input("path", "Host path", required=True)}), max_items=LIST_MAX),
        "filesystem": Input("enum", "Filesystem", choices=("standard", "zfs")),
        "pgtune": Input("text", "PGTune output", max_bytes=PGTUNE_MAX_BYTES, multiline=True),
        "pg_mem_limit": Input("int", "PostgreSQL memory limit (GB, 0 = unlimited)", lo=0, hi=GB_MAX),
        "mongo_data_path": Input("path", "MongoDB data path"),
        "mongo_port": Input("port", "MongoDB port"),
        "mongo_cache_size_gb": Input("int", "WiredTiger cache (GB)", lo=1, hi=GB_MAX),
        "mongo_mem_limit": Input("int", "MongoDB memory limit (GB, 0 = unlimited)", lo=0, hi=GB_MAX),
        "mongo_validate": Input("enum", "Pre-import file validation", choices=("full", "tail", "none")),
        "starrocks_data_path": Input("path", "StarRocks data path"),
        "starrocks_port": Input("port", "StarRocks MySQL protocol port"),
        "starrocks_fe_http_port": Input("port", "StarRocks FE HTTP port"),
        "sr_fe_jvm_heap": Input("int", "FE JVM heap (GB)", lo=1, hi=GB_MAX),
        "starrocks_mem_limit": Input("int", "StarRocks memory limit (GB, 0 = unlimited)", lo=0, hi=GB_MAX),
        "sr_be_mem_limit": Input("int", "BE memory limit (GB)", lo=1, hi=GB_MAX),
        "sr_alter_tablet_workers": Input("int", "BE alter worker count", lo=1, hi=1024),
        "sr_compression": Input("enum", "Table compression", choices=("ZSTD", "LZ4")),
        "starrocks_storage_paths": Input("list", "Extra storage paths", item=path, max_items=LIST_MAX, unique=True),
        "auth": Input("bool", "Authentication"),
        "new_password": Input("secret", "Admin password"),
        "new_password_confirm": Input("secret", "Admin password again"),
        "ro_user": Input("bool", "Read-only user"),
        "ro_username": Input("name", "Read-only user name"),
        "ro_password_mode": Input("enum", "Read-only password", choices=("generate", "typed")),
        "ro_password": Input("secret", "Read-only password (typed)"),
        "ro_password_confirm": Input("secret", "Read-only password again"),
    }
    return Action(
        "db.setup", "step_up", "Set up a database", inputs,
        form="python sdp.py db setup [--add=<database>]", command=command, check=check, terminal_asks=ASKS, writes=writes,
    )
