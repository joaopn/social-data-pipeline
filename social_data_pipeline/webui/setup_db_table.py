"""The web UI's table entry of `db.setup` (`sdp db setup`: the first time, a database added, or a change of the databases set up): its inputs, the cross-field check, the command shown, the confirm page's lines of what it writes, and for a change the form filled in from the `db.settings` read and the list of what changes.

`entry(Input, Action, Refused)` builds the entry; `actions` calls it while it builds its table, so this module imports nothing of `actions` (the classes are handed in). The inputs mirror the registry's (`control.verbs.DB_SETUP_INPUTS`) and the check mirrors `control.dbsetup.check` rule by rule, its rules on the current setup included (a test runs both on the same answers): the image holds neither. Standard library only.
"""

import os
import re
import shlex

DATABASES = ("postgres", "mongo", "starrocks")
DATABASE_NAMES = {"postgres": "PostgreSQL", "mongo": "MongoDB", "starrocks": "StarRocks"}
PER_DATABASE = {
    "postgres": ("pgdata_path", "db_name", "pg_port", "tablespaces", "filesystem", "pgtune", "pg_mem_limit", "keep_tablespaces", "keep_pgtune"),
    "mongo": ("mongo_data_path", "mongo_port", "mongo_cache_size_gb", "mongo_mem_limit", "mongo_validate"),
    "starrocks": ("starrocks_data_path", "starrocks_port", "starrocks_fe_http_port", "sr_fe_jvm_heap", "starrocks_mem_limit", "sr_be_mem_limit", "sr_alter_tablet_workers", "sr_compression", "starrocks_storage_paths", "keep_storage_paths"),
}
KEEPS = (("keep_tablespaces", "tablespaces", "tablespaces"), ("keep_storage_paths", "starrocks_storage_paths", "storage_paths"), ("keep_pgtune", "pgtune", "pgtune"))
DATA_PATH_KEYS = {"postgres": "pgdata_path", "mongo": "mongo_data_path", "starrocks": "starrocks_data_path"}
# What `first_refusal` takes as `current` when nothing is set up (the first time).
NOTHING_SET_UP = {"configured": [], "auth": False, "ro_username": None, "ro_password_set": False, "tablespaces": False, "storage_paths": False, "pgtune": False}
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
# The label of each input, on the forms, the pages and in the list of what a change changes.
LABELS = {
    "add": "Add to the setup",
    "databases": "Databases",
    "data_path": "Data base path",
    "export_path": "Export path",
    "pgdata_path": "PostgreSQL data path",
    "db_name": "Database name",
    "pg_port": "PostgreSQL port",
    "tablespaces": "Tablespaces",
    "filesystem": "Filesystem",
    "pgtune": "PGTune output",
    "pg_mem_limit": "PostgreSQL memory limit (GB, 0 = unlimited)",
    "mongo_data_path": "MongoDB data path",
    "mongo_port": "MongoDB port",
    "mongo_cache_size_gb": "WiredTiger cache (GB)",
    "mongo_mem_limit": "MongoDB memory limit (GB, 0 = unlimited)",
    "mongo_validate": "Pre-import file validation",
    "starrocks_data_path": "StarRocks data path",
    "starrocks_port": "StarRocks MySQL protocol port",
    "starrocks_fe_http_port": "StarRocks FE HTTP port",
    "sr_fe_jvm_heap": "FE JVM heap (GB)",
    "starrocks_mem_limit": "StarRocks memory limit (GB, 0 = unlimited)",
    "sr_be_mem_limit": "BE memory limit (GB)",
    "sr_alter_tablet_workers": "BE alter worker count",
    "sr_compression": "Table compression",
    "starrocks_storage_paths": "Extra storage paths",
    "auth": "Authentication",
    "new_password": "Admin password",
    "new_password_confirm": "Admin password again",
    "ro_user": "Read-only user",
    "ro_username": "Read-only user name",
    "ro_password_mode": "Read-only password",
    "ro_password": "Read-only password (typed)",
    "ro_password_confirm": "Read-only password again",
    "keep_tablespaces": "Keep the current tablespaces",
    "keep_storage_paths": "Keep the current extra storage paths",
    "keep_pgtune": "Keep the current PGTune output",
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


def first_refusal(fields, current=None):
    """The name of the first input `control.dbsetup.check` refuses for these answers (with `current`, the setup as it is now), or None: the same rules in the same order."""
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
    if add and fields.get("ro_password_mode") == "keep":
        return "ro_password_mode"
    for keep, kept, _ in KEEPS:
        if fields.get(keep) is True and kept in fields:
            return keep
    if current is not None:
        return _current_refusal(fields, current, add, databases)
    return None


def _current_refusal(fields, current, add, databases):
    configured = set(current["configured"])
    if add:
        if fields["add"] in configured:
            return "add"
    else:
        if any(name in configured and name not in databases for name in DATABASES):
            return "databases"
        if current["auth"] and fields.get("auth") is not True:
            return "auth"
    for keep, _, what in KEEPS:
        if fields.get(keep) is True and not current[what]:
            return keep
    if fields.get("ro_password_mode") == "keep":
        name = fields.get("ro_username") or current["ro_username"] or "readonly"
        if not (current["ro_password_set"] and name == current["ro_username"]):
            return "ro_password_mode"
    return None


# ── a change of the databases set up: the form filled in, what changes ─────


def current_of(read):
    """The `current` of `first_refusal` from a `db.settings` read."""
    return {
        "configured": list(read["configured"]), "auth": bool(read["auth"]), "ro_username": read["ro_username"], "ro_password_set": bool(read["ro_password_set"]),
        "tablespaces": bool(read["tablespaces"]), "storage_paths": bool(read["storage_paths"]), "pgtune": bool(read["pgtune_set"]),
    }


def prefill(read):
    """The answers a change starts from: the current value of each question but the paths (a path is kept in the answers only when it is changed, so that one left as it is is written exactly as it is), the keep answers where there is something to keep, authentication and the read-only user as they are."""
    fields = {"databases": [name for name in DATABASES if name in read["configured"]]}
    fields.update({name: value for name, value in read["values"].items() if name not in PATH_FIELDS})
    if read["tablespaces"]:
        fields["keep_tablespaces"] = True
    if read["storage_paths"]:
        fields["keep_storage_paths"] = True
    if read["pgtune_set"]:
        fields["keep_pgtune"] = True
    if read["auth"]:
        fields["auth"] = True
        if read["ro_username"]:
            fields.update({"ro_user": True, "ro_username": read["ro_username"], "ro_password_mode": "keep" if read["ro_password_set"] else "generate"})
    return fields


def same_path(given, current):
    """Whether a path answer names the current path as it is written (`./x`, `x` and `x/` alike), so that it is left out and written as it is."""
    return isinstance(current, str) and os.path.normpath(given) == os.path.normpath(current)


def _listed(items):
    return ", ".join(items) if items else "none"


def changes(fields, read):
    """What a change of the databases set up changes, as (what, now, after) for the confirm page; a path left out of the answers is left as it is."""
    values = read["values"]
    labels = LABELS
    rows = []
    for name in DATABASES:
        if name in chosen(fields) and name not in read["configured"]:
            rows.append((DATABASE_NAMES[name], "not set up", "set up now"))
    names = ["data_path", "export_path"] + [name for database in chosen(fields) if database in read["configured"] for name in PER_DATABASE[database]]
    for name in names:
        if name in ("tablespaces", "starrocks_storage_paths", "pgtune") or name.startswith("keep_") or name not in fields:
            continue
        old = values.get(name)
        if name in PATH_FIELDS and same_path(fields[name], old):
            continue
        if old != fields[name]:
            rows.append((labels[name], "not known" if old is None else str(old), str(fields[name])))
    if "postgres" in read["configured"] and "postgres" in chosen(fields):
        old = _listed([f"{item['name']}: {item['path']}" for item in read["tablespaces"]])
        new = old if fields.get("keep_tablespaces") is True else _listed([f"{item['name']}: {item['path']}" for item in fields.get("tablespaces", [])])
        if new != old:
            rows.append((labels["tablespaces"], old, new))
        old = "set" if read["pgtune_set"] else "none"
        new = old if fields.get("keep_pgtune") is True else "a new output" if fields.get("pgtune") else "none"
        if new != old:
            rows.append((labels["pgtune"], old, new))
    if "starrocks" in read["configured"] and "starrocks" in chosen(fields):
        old = _listed(read["storage_paths"])
        new = old if fields.get("keep_storage_paths") is True else _listed(fields.get("starrocks_storage_paths", []))
        if new != old:
            rows.append((labels["starrocks_storage_paths"], old, new))
    auth = fields.get("auth") is True
    if auth != bool(read["auth"]):
        rows.append((labels["auth"], "on" if read["auth"] else "off", "on" if auth else "off"))
    ro_user = (fields.get("ro_username") or read["ro_username"] or "readonly") if auth and fields.get("ro_user") is True else None
    if ro_user != read["ro_username"]:
        rows.append((labels["ro_username"], read["ro_username"] or "none", ro_user or "none"))
    mode = fields.get("ro_password_mode")
    if ro_user and mode != "keep":
        rows.append((labels["ro_password_mode"], "set" if read["ro_password_set"] else "none", "a new typed one" if mode == "typed" else "a new generated one"))
    return rows


def command(fields):
    """The `sdp` command the answers stand for: `db setup`, or `db setup --add=<db>`."""
    if "add" in fields:
        return "python sdp.py db setup --add=" + shlex.quote(fields["add"])
    return "python sdp.py db setup"


def _or_default(fields, name, words, read=None):
    """A path as the setup writes it: the answer, else (in a change) the current one, else the words for the terminal's default."""
    if name in fields:
        return f"{fields[name]}"
    if read is not None and name in read["values"]:
        return f"{read['values'][name]}"
    return words


def data_path_sentence(name, old, new):
    """The sentence of a database's data path changed (decided: said in plain words on the confirm page)."""
    label = DATABASE_NAMES[name]
    return f"The data path of {label} changes from {old} to {new}. Its existing data is NOT moved: {label} will start empty at the new path, and the old data stays in {old}."


AUTH_OFF_SENTENCE = "Authentication is on for the databases set up, and it cannot be switched off here: switching it off is done in a terminal, with python sdp.py db setup."
GENERIC_DATA_PATH = "When a database is set up already and its data path changes here, its existing data is NOT moved: it will start empty at the new path, and the old data stays in the old folder."
RESTART = "Nothing is started or restarted: a database that runs keeps its old settings until it is stopped and started again (Stop, then Start on its card, or python sdp.py db stop <database> and python sdp.py db start <database>)."


def writes(fields, read=None):
    """The confirm page's lines: every file and .env key written, every folder made, what happens to the passwords, and for a change of the databases set up (`read`, the `db.settings` read the form started from) what changes in their data folders. Written from `setup.db_answers` and the functions of `setup.db` it calls, clause by clause."""
    databases = chosen(fields)
    add = "add" in fields
    change = read is not None and not add and bool(read["configured"])
    auth = fields.get("auth") is True
    ro_user = auth and fields.get("ro_user") is True
    tablespaces = bool(fields.get("tablespaces")) or fields.get("keep_tablespaces") is True
    storage = bool(fields.get("starrocks_storage_paths")) or fields.get("keep_storage_paths") is True
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
        if tablespaces or storage:
            lines.append("Rewrites docker-compose.override.yml whole, with only the tablespace and extra-disk mounts: every other entry in it is dropped, the scheduler's /jobs_export mounts included (run `db setup-jobs` again to put them back).")
        elif change and (read["tablespaces"] or read["storage_paths"]):
            lines.append("Leaves docker-compose.override.yml as it is: the mounts of the tablespaces and extra disks given up here stay in it (as the terminal leaves them), and the database no longer uses them.")
        lines.append("Makes the export folder " + _or_default(fields, "export_path", "<data base path>/export", read if change else None) + " when it is missing.")
    for name in databases:
        key = DATA_PATH_KEYS[name]
        folder = _or_default(fields, key, f"<data base path>/database/{name}", read if change else None)
        if ro_user:
            lines.append(f"Makes the data folder of {DATABASE_NAMES[name]} ({folder}) when it is missing and writes the read-only user's password into .ro_credentials there (mode 0600).")
    lines.append("No other folder is made or changed here, and no data is moved.")
    if change:
        values = read["values"]
        if "data_path" in fields and "data_path" in values and not same_path(fields["data_path"], values["data_path"]):
            lines.append(f"The data base path (DATA_PATH) changes from {values['data_path']} to {fields['data_path']}: nothing under {values['data_path']} is moved; `source add` suggests a new source's folders under the new path from now on, and the sources set up already keep theirs.")
        if "export_path" in fields and "export_path" in values and not same_path(fields["export_path"], values["export_path"]):
            lines.append(f"The export folder changes from {values['export_path']} to {fields['export_path']}: the databases mount the new one at /export from their next start, and what is in the old one stays there.")
        for name in databases:
            key = DATA_PATH_KEYS[name]
            if name in read["configured"] and key in fields and key in values and not same_path(fields[key], values[key]):
                lines.append(data_path_sentence(name, values[key], fields[key]))
    elif not add and any(DATA_PATH_KEYS[name] in fields for name in databases):
        lines.append(GENERIC_DATA_PATH)
    if auth:
        lines.append("The admin password is not stored anywhere: you will give it when the database starts. It was typed twice to check it.")
        if ro_user:
            mode = fields.get("ro_password_mode")
            if mode == "keep":
                lines.append("The read-only password is kept: it is read from the .ro_credentials set up now and written again into .ro_credentials in each database's data folder; it is never shown.")
            else:
                lines.append("The read-only password is " + ("the one you typed" if mode == "typed" else "generated (32 characters)") + " and kept only in .ro_credentials; it is never shown.")
    elif not add or "auth" in fields:
        lines.append("Authentication stays off: the database takes connections without a password.")
    lines.append(RESTART if change else "Nothing is started: start the database afterwards (python sdp.py db start).")
    return lines


def entry(Input, Action, Refused):
    """The table's entry of `db.setup`, made with the table's own classes."""
    path = Input("path", "Path")

    def check(fields):
        refused = first_refusal(fields)
        if refused is not None:
            raise Refused(refused)

    inputs = {
        "add": Input("enum", LABELS["add"], choices=DATABASES),
        "databases": Input("list", LABELS["databases"], item=Input("enum", "Database", choices=DATABASES), max_items=3, unique=True),
        "data_path": Input("path", LABELS["data_path"]),
        "export_path": Input("path", LABELS["export_path"]),
        "pgdata_path": Input("path", LABELS["pgdata_path"]),
        "db_name": Input("name", LABELS["db_name"]),
        "pg_port": Input("port", LABELS["pg_port"]),
        "tablespaces": Input("list", LABELS["tablespaces"], item=Input("record", "Tablespace", fields={"name": Input("name", "Name", required=True), "path": Input("path", "Host path", required=True)}), max_items=LIST_MAX),
        "filesystem": Input("enum", LABELS["filesystem"], choices=("standard", "zfs")),
        "pgtune": Input("text", LABELS["pgtune"], max_bytes=PGTUNE_MAX_BYTES, multiline=True),
        "pg_mem_limit": Input("int", LABELS["pg_mem_limit"], lo=0, hi=GB_MAX),
        "mongo_data_path": Input("path", LABELS["mongo_data_path"]),
        "mongo_port": Input("port", LABELS["mongo_port"]),
        "mongo_cache_size_gb": Input("int", LABELS["mongo_cache_size_gb"], lo=1, hi=GB_MAX),
        "mongo_mem_limit": Input("int", LABELS["mongo_mem_limit"], lo=0, hi=GB_MAX),
        "mongo_validate": Input("enum", LABELS["mongo_validate"], choices=("full", "tail", "none")),
        "starrocks_data_path": Input("path", LABELS["starrocks_data_path"]),
        "starrocks_port": Input("port", LABELS["starrocks_port"]),
        "starrocks_fe_http_port": Input("port", LABELS["starrocks_fe_http_port"]),
        "sr_fe_jvm_heap": Input("int", LABELS["sr_fe_jvm_heap"], lo=1, hi=GB_MAX),
        "starrocks_mem_limit": Input("int", LABELS["starrocks_mem_limit"], lo=0, hi=GB_MAX),
        "sr_be_mem_limit": Input("int", LABELS["sr_be_mem_limit"], lo=1, hi=GB_MAX),
        "sr_alter_tablet_workers": Input("int", LABELS["sr_alter_tablet_workers"], lo=1, hi=1024),
        "sr_compression": Input("enum", LABELS["sr_compression"], choices=("ZSTD", "LZ4")),
        "starrocks_storage_paths": Input("list", LABELS["starrocks_storage_paths"], item=path, max_items=LIST_MAX, unique=True),
        "keep_tablespaces": Input("bool", LABELS["keep_tablespaces"]),
        "keep_storage_paths": Input("bool", LABELS["keep_storage_paths"]),
        "keep_pgtune": Input("bool", LABELS["keep_pgtune"]),
        "auth": Input("bool", LABELS["auth"]),
        "new_password": Input("secret", LABELS["new_password"]),
        "new_password_confirm": Input("secret", LABELS["new_password_confirm"]),
        "ro_user": Input("bool", LABELS["ro_user"]),
        "ro_username": Input("name", LABELS["ro_username"]),
        "ro_password_mode": Input("enum", LABELS["ro_password_mode"], choices=("generate", "typed", "keep")),
        "ro_password": Input("secret", LABELS["ro_password"]),
        "ro_password_confirm": Input("secret", LABELS["ro_password_confirm"]),
    }
    return Action(
        "db.setup", "step_up", "Set up a database", inputs,
        form="python sdp.py db setup [--add=<database>]", command=command, check=check, terminal_asks=ASKS, writes=writes,
    )
