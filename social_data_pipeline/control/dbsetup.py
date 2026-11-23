"""The answers of `sdp db setup` given all at once (the verb `db.setup`): which inputs belong together, and what a value may hold so that the files it is written to stay readable.

`check(fields)` takes the validated fields of a request (a secret as anything with `reveal()`) and raises `Refused` naming the field at fault, with a reason that holds no value. The registry runs it as the verb's cross-field check (`verbs._check_db_setup`), and the CLI's action `do_db_setup` runs it again before it reads or writes anything.

Two modes, as the terminal has them: without `add` the first-time questionnaire (`db setup`: a data base path, an export path, one or more databases), with `add` one database added to an existing setup (`db setup --add <db>`: no base path, no export path, the setup's authentication when it is on). Standard library only.
"""

import re

DATABASES = ("postgres", "mongo", "starrocks")

# The inputs that belong to each database: refused when that database is not set up by the request.
PER_DATABASE = {
    "postgres": ("pgdata_path", "db_name", "pg_port", "tablespaces", "filesystem", "pgtune", "pg_mem_limit"),
    "mongo": ("mongo_data_path", "mongo_port", "mongo_cache_size_gb", "mongo_mem_limit", "mongo_validate"),
    "starrocks": ("starrocks_data_path", "starrocks_port", "starrocks_fe_http_port", "sr_fe_jvm_heap", "starrocks_mem_limit", "sr_be_mem_limit", "sr_alter_tablet_workers", "sr_compression", "starrocks_storage_paths"),
}
# The inputs of the first-time questionnaire only (`--add` takes the base path from .env and asks no export path).
FIRST_TIME_ONLY = ("databases", "data_path", "export_path")
PORT_FIELDS = ("pg_port", "mongo_port", "starrocks_port", "starrocks_fe_http_port")
PATH_FIELDS = ("data_path", "export_path", "pgdata_path", "mongo_data_path", "starrocks_data_path")

# A name PostgreSQL takes unquoted and keeps as it is: the tablespace name and the read-only user's name are written into SQL without quotes (`CREATE TABLESPACE <name>`, `CREATE ROLE <user>` in the entrypoints), and an unquoted name is folded to lower case.
SQL_NAME = r"[a-z_][a-z0-9_]{0,62}"
# The default tablespace of PostgreSQL, which the terminal refuses as a name.
RESERVED_TABLESPACE = "pgdata"
# Characters a path may not hold: each is written as it is into .env (where `$` is interpolated by Compose and ` #` starts a comment), into a mount line `host:container` of docker-compose.override.yml (`:`), and into YAML (quotes, `#`, `\`).
PATH_FORBIDDEN = frozenset("$:#\"'\\`")
# Characters a typed read-only password may not hold: the entrypoints put it between single quotes in SQL (`PASSWORD '<password>'`), and `.ro_credentials` is read back stripped of white space.
RO_PASSWORD_FORBIDDEN = frozenset("'\"\\`")

class Refused(ValueError):
    """An answer refused: `field` names the input at fault; the message holds no value."""

    def __init__(self, field, reason):
        super().__init__(reason)
        self.field = field


def chosen(fields):
    """The databases a request sets up, in the order the CLI handles them."""
    if "add" in fields:
        return [fields["add"]]
    return [name for name in DATABASES if name in fields.get("databases", [])]


def _plain(value):
    return value.reveal() if hasattr(value, "reveal") else value


def path_reason(value):
    """Why a path cannot be written as it is, or None."""
    if value != value.strip():
        return "must not begin or end with white space"
    if any(character in PATH_FORBIDDEN for character in value):
        return "must not hold any of $ : # \" ' \\ `"
    return None


def ro_password_reason(value):
    """Why a typed read-only password cannot be used, or None."""
    if any(character.isspace() for character in value):
        return "must not hold white space"
    if any(character in RO_PASSWORD_FORBIDDEN for character in value):
        return "must not hold a quote, a backslash or a backtick"
    return None


def _need(condition, field, reason):
    if not condition:
        raise Refused(field, reason)


def check(fields):
    """Refuse answers that do not belong together or that the files cannot hold. Raises Refused; returns None."""
    add = "add" in fields
    _need(add != ("databases" in fields), "databases", "is required without add, and refused with it")
    if add:
        for name in FIRST_TIME_ONLY:
            _need(name not in fields, name, "is asked only when no database is set up yet (without add)")
    else:
        _need(len(fields["databases"]) > 0, "databases", "must name at least one database")
    databases = chosen(fields)
    for database, names in PER_DATABASE.items():
        if database not in databases:
            for name in names:
                _need(name not in fields, name, f"belongs to {database}, which this request does not set up")

    for name in PATH_FIELDS:
        if name in fields:
            reason = path_reason(fields[name])
            _need(reason is None, name, reason)
    for index, path in enumerate(fields.get("starrocks_storage_paths", []), start=1):
        reason = path_reason(path)
        _need(reason is None, "starrocks_storage_paths", f"item {index} {reason}" if reason else "")
    seen = set()
    for index, tablespace in enumerate(fields.get("tablespaces", []), start=1):
        _need(re.fullmatch(SQL_NAME, tablespace["name"]) is not None, "tablespaces", f"item {index} needs a name of lower-case letters, digits and '_' that does not start with a digit")
        _need(tablespace["name"] != RESERVED_TABLESPACE, "tablespaces", f"item {index}: 'pgdata' is reserved for the default PostgreSQL data directory")
        _need(tablespace["name"] not in seen, "tablespaces", f"item {index} repeats a name")
        seen.add(tablespace["name"])
        reason = path_reason(tablespace["path"])
        _need(reason is None, "tablespaces", f"item {index} path {reason}" if reason else "")

    ports = [fields[name] for name in PORT_FIELDS if name in fields]
    for name in PORT_FIELDS:
        if name in fields:
            _need(ports.count(fields[name]) == 1, name, "is given to two services of this setup")

    auth = fields.get("auth") is True
    for name in ("new_password", "new_password_confirm", "ro_user"):
        _need(auth or name not in fields, name, "is taken only with authentication on")
    if auth:
        _need("new_password" in fields, "new_password", "is required with authentication on")
        _need("new_password_confirm" in fields, "new_password_confirm", "is required with authentication on")
        _need(_plain(fields["new_password"]) == _plain(fields["new_password_confirm"]), "new_password_confirm", "does not match the admin password")
    ro_user = auth and fields.get("ro_user") is True
    for name in ("ro_username", "ro_password_mode"):
        _need(ro_user or name not in fields, name, "is taken only with a read-only user")
    if "ro_username" in fields:
        _need(re.fullmatch(SQL_NAME, fields["ro_username"]) is not None, "ro_username", "must be lower-case letters, digits and '_', not starting with a digit")
    typed = ro_user and fields.get("ro_password_mode") == "typed"
    for name in ("ro_password", "ro_password_confirm"):
        _need(typed or name not in fields, name, "is taken only when the read-only password is typed")
        _need(not typed or name in fields, name, "is required when the read-only password is typed")
    if typed:
        reason = ro_password_reason(_plain(fields["ro_password"]))
        _need(reason is None, "ro_password", reason)
        _need(_plain(fields["ro_password"]) == _plain(fields["ro_password_confirm"]), "ro_password_confirm", "does not match the read-only password")
