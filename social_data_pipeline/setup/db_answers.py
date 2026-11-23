"""`sdp db setup` with its answers given all at once: the settings the questionnaire would build from them, and the files it would write, without a terminal.

Used by the CLI's action `do_db_setup` (the verb `db.setup`, the web UI's "Set up a database"). Two paths, as the terminal has them:

- First time (`first_time_settings`, `write_first_time`): the answers of `db.main` (`run_questionnaire`), then its write step: `.env` rewritten by `generate_env` (keys it does not manage are kept), `config/db/<db>.yaml` per database, `config/postgres/postgresql.local.conf` (and `pg_hba.local.conf` with authentication) for PostgreSQL, `config/starrocks/fe.local.conf` and `be.local.conf` for StarRocks, `docker-compose.override.yml` rewritten whole when there are tablespaces or extra StarRocks disks, each existing file kept as `.bak`; the export folder made; `.ro_credentials` written into each database's data folder with a read-only user.
- One database added (`add_settings`, `write_added`): the answers of `db.add_database`, then its write step: that database's files, its `.env` keys merged by `update_env_file`, its block of `docker-compose.override.yml` replaced by `_update_override_volumes`, its `.ro_credentials`. When authentication is on already, the new database gets it with the same read-only user and password, as the terminal does.

An answer left out (None) is what pressing Enter takes in the terminal, computed the same way (from the existing settings and this machine's memory and cores); the PGTune output left out is none (the terminal's "skip"). The values are the terminal's types: `ask` returns text, so the StarRocks heap and memory sizes are kept as text, as the terminal writes them. The functions never prompt, never exit and never print a secret; a refusal is `SetupRefused` with a message that holds no answer. The generators and writers are the questionnaire's own (`setup.db`, `setup.utils`), so the terminal path is not touched.
"""

import secrets
from pathlib import Path

from social_data_pipeline.core.config import ConfigurationError
from social_data_pipeline.setup import db, utils

DATABASES = ("postgres", "mongo", "starrocks")


class SetupRefused(Exception):
    """The answers cannot be written: `message` says why and holds no answer."""

    def __init__(self, message):
        super().__init__(message)
        self.message = message


def machine():
    """(RAM in GB or None, CPU cores or None), as `detect_hardware` gives them (without asking for GPUs)."""
    return utils.detect_ram_gb(), utils.detect_cpu_cores()


def _text(answer, default):
    """What `ask` returns: the answer typed, else the default as text."""
    return str(answer) if answer is not None else str(default)


def _int(answer, default):
    """What `ask_int` returns: the answer, else the default read as a whole number."""
    return answer if answer is not None else int(str(default))


def pgtune_lines(text):
    """The PGTune output as the terminal's paste keeps it: the lines that are not blank, joined by line breaks."""
    return "\n".join(line for line in (text or "").split("\n") if line.strip())


def _database_settings(name, answers, existing, settings):
    """Add one database's answers to `settings`, with the terminal's defaults for the ones left out (the expressions of `run_questionnaire` and `add_database`, which agree)."""
    data_path = settings["data_path"]
    if name == "postgres":
        settings["pgdata_path"] = _text(answers.get("pgdata_path"), existing.get("pgdata_path", f"{data_path}/database/postgres"))
    elif name == "mongo":
        settings["mongo_data_path"] = _text(answers.get("mongo_data_path"), existing.get("mongo_data_path", f"{data_path}/database/mongo"))
    else:
        settings["starrocks_data_path"] = _text(answers.get("starrocks_data_path"), existing.get("starrocks_data_path", f"{data_path}/database/starrocks"))


def _postgres(answers, ram, existing, settings):
    settings["db_name"] = _text(answers.get("db_name"), existing.get("db_name", "datasets"))
    settings["pg_port"] = _int(answers.get("pg_port"), existing.get("pg_port", 5432))
    tablespaces = {item["name"]: item["path"] for item in answers.get("tablespaces") or []}
    if tablespaces:
        settings["tablespaces"] = tablespaces
    settings["filesystem"] = answers.get("filesystem") or "standard"
    settings["pgtune_output"] = pgtune_lines(answers.get("pgtune"))
    pg_mem = _int(answers.get("pg_mem_limit"), existing.get("pg_mem_limit", int(ram * 0.6) if ram else 0))
    if pg_mem > 0:
        settings["pg_mem_limit"] = pg_mem


def _mongo(answers, existing, settings):
    settings["mongo_port"] = _int(answers.get("mongo_port"), existing.get("mongo_port", 27017))
    settings["mongo_cache_size_gb"] = _int(answers.get("mongo_cache_size_gb"), existing.get("mongo_cache_size_gb", 2))
    mongo_mem = _int(answers.get("mongo_mem_limit"), existing.get("mongo_mem_limit", max(2, settings["mongo_cache_size_gb"] * 2)))
    if mongo_mem > 0:
        settings["mongo_mem_limit"] = mongo_mem
    settings["mongo_validate"] = answers.get("mongo_validate") or existing.get("mongo_validate", "full")


def _starrocks(answers, ram, cores, existing, settings):
    settings["starrocks_port"] = _int(answers.get("starrocks_port"), existing.get("starrocks_port", 9030))
    settings["starrocks_fe_http_port"] = _int(answers.get("starrocks_fe_http_port"), existing.get("starrocks_fe_http_port", 8030))
    settings["sr_fe_jvm_heap"] = _text(answers.get("sr_fe_jvm_heap"), existing.get("sr_fe_jvm_heap", max(2, min(8, int(ram // 8))) if ram else 4))
    fe_heap = float(settings["sr_fe_jvm_heap"])
    sr_mem_text = _text(answers.get("starrocks_mem_limit"), existing.get("starrocks_mem_limit", int(max(fe_heap + 4, ram * 0.6)) if ram else 0))
    sr_mem = float(sr_mem_text)
    if sr_mem > 0:
        settings["starrocks_mem_limit"] = sr_mem_text
    if sr_mem > 0:
        default_be = int(max(2, sr_mem - fe_heap - 2))
    elif ram:
        default_be = int(max(2, ram * 0.5))
    else:
        default_be = 8
    settings["sr_be_mem_limit"] = _text(answers.get("sr_be_mem_limit"), existing.get("sr_be_mem_limit", default_be))
    settings["sr_alter_tablet_workers"] = _int(answers.get("sr_alter_tablet_workers"), existing.get("sr_alter_tablet_workers", max(4, min(cores // 2, 10)) if cores else 4))
    settings["sr_compression"] = answers.get("sr_compression") or existing.get("sr_compression", "ZSTD")
    if answers.get("starrocks_storage_paths"):
        settings["starrocks_storage_paths"] = list(answers["starrocks_storage_paths"])


def _questions_of(name, answers, ram, cores, existing, settings):
    """One database's own questions (after the data paths and the export path)."""
    if name == "postgres":
        _postgres(answers, ram, existing, settings)
    elif name == "mongo":
        _mongo(answers, existing, settings)
    else:
        _starrocks(answers, ram, cores, existing, settings)


def _ro_password(answers):
    """The read-only password of the answers: typed, or made as the terminal's "Auto-generate read-only password?" makes it."""
    if answers.get("ro_password_mode") == "typed":
        return answers["ro_password"]
    return secrets.token_urlsafe(24)


def _ask_auth(answers, default_ro_username, settings):
    """The authentication questions, answered: on or off, the read-only user and its password. The admin password is checked by the caller and kept nowhere, as the terminal keeps it nowhere."""
    if answers.get("auth") is not True:
        return
    settings["auth_enabled"] = True
    if answers.get("ro_user") is True:
        settings["ro_username"] = answers.get("ro_username") or default_ro_username
        settings["ro_password"] = _ro_password(answers)


def first_time_settings(answers, ram, cores, existing):
    """The settings `run_questionnaire` builds for these answers (`answers["databases"]` in the CLI's order)."""
    settings = {"data_path": _text(answers.get("data_path"), existing.get("data_path", "./data"))}
    settings["databases"] = [name for name in DATABASES if name in answers["databases"]]
    data_path = settings["data_path"]
    for name in settings["databases"]:
        _database_settings(name, answers, existing, settings)
    settings["export_path"] = _text(answers.get("export_path"), existing.get("export_path", f"{data_path}/export"))
    for name in settings["databases"]:
        _questions_of(name, answers, ram, cores, existing, settings)
    # No existing read-only user to keep: the first-time path sets up databases that have none.
    _ask_auth(answers, existing.get("ro_username", "readonly"), settings)
    return settings


def add_settings(name, answers, ram, cores, existing, env):
    """The settings `add_database(name)` builds for these answers. With authentication on already, the new database takes it as the terminal gives it (the existing read-only user and its password, read from a `.ro_credentials`), and authentication answers are refused."""
    if not env.get("DATA_PATH"):
        raise SetupRefused("No existing setup found: set up the first database without add.")
    settings = {"data_path": env["DATA_PATH"], "databases": [name]}
    _database_settings(name, answers, existing, settings)
    _questions_of(name, answers, ram, cores, existing, settings)
    if existing.get("auth_enabled", False):
        if any(key in answers for key in ("auth", "new_password", "new_password_confirm", "ro_user", "ro_username", "ro_password_mode", "ro_password", "ro_password_confirm")):
            raise SetupRefused("Authentication is on for the databases set up already, and the new one gets the same settings: leave the authentication answers out.")
        settings["auth_enabled"] = True
        try:
            ro_password = db._read_existing_ro_password(existing)
        except ConfigurationError as error:
            raise SetupRefused(f"The existing read-only password cannot be read: {error}") from None
        if existing.get("ro_username"):
            settings["ro_username"] = existing["ro_username"]
        if ro_password:
            settings["ro_password"] = ro_password
    else:
        _ask_auth(answers, "readonly", settings)
    return settings


def _postgres_conf(settings):
    """`generate_postgresql_local_conf`, which ends the process when it cannot read the base file: here a refusal."""
    try:
        return db.generate_postgresql_local_conf(settings)
    except SystemExit:
        raise SetupRefused("config/postgres/postgresql.conf cannot be read; nothing was written.") from None


def first_time_files(settings):
    """The files of `db.main`'s write step, in its order: [(path, content)]."""
    files = [(db.ROOT / ".env", db.generate_env(settings))]
    if "postgres" in settings["databases"]:
        files.append((db.CONFIG_DIR / "db" / "postgres.yaml", db.generate_db_postgres_yaml(settings)))
        files.append((db.CONFIG_DIR / "postgres" / "postgresql.local.conf", _postgres_conf(settings)))
        if settings.get("auth_enabled"):
            files.append((db.CONFIG_DIR / "postgres" / "pg_hba.local.conf", db.generate_pg_hba_local_conf(settings)))
    if "mongo" in settings["databases"]:
        files.append((db.CONFIG_DIR / "db" / "mongo.yaml", db.generate_db_mongo_yaml(settings)))
    if "starrocks" in settings["databases"]:
        files.append((db.CONFIG_DIR / "db" / "starrocks.yaml", db.generate_db_starrocks_yaml(settings)))
        files.append((db.CONFIG_DIR / "starrocks" / "fe.local.conf", db.generate_starrocks_fe_conf(settings)))
        files.append((db.CONFIG_DIR / "starrocks" / "be.local.conf", db.generate_starrocks_be_conf(settings)))
    if "tablespaces" in settings or settings.get("starrocks_storage_paths"):
        override = db.generate_docker_compose_override(settings)
        if override:
            files.append((db.ROOT / "docker-compose.override.yml", override))
    return files


def write_first_time(settings, files):
    """`db.main`'s write step: the files (each existing one kept as `.bak`), the export folder, the read-only password files. Prints what it writes, never a password."""
    utils.write_files(files)
    if "export_path" in settings:
        export = Path(settings["export_path"])
        if not export.is_absolute():
            export = db.ROOT / export
        export.mkdir(parents=True, exist_ok=True)
        print(f"  Created:   {export}", flush=True)
    if settings.get("auth_enabled") and settings.get("ro_username"):
        for written in _write_ro_credentials(settings):
            print(f"  Written:   {written} (chmod 600)", flush=True)


def _write_ro_credentials(settings):
    """`_write_ro_credentials` with each relative data folder taken from the deployment folder (the terminal takes it from the folder it runs in, which for the broker's child is the deployment folder too)."""
    written = []
    for name, key in db.DB_DATA_PATH_KEYS.items():
        if key in settings:
            folder = Path(settings[key])
            written.append(db._write_ro_password_for(name, folder if folder.is_absolute() else db.ROOT / folder, settings["ro_password"]))
    return written


def added_files(name, settings):
    """The files, the .env updates and the override volumes of `add_database(name)`'s write step."""
    files, env = [], {}
    if name == "postgres":
        files.append((db.CONFIG_DIR / "db" / "postgres.yaml", db.generate_db_postgres_yaml(settings)))
        files.append((db.CONFIG_DIR / "postgres" / "postgresql.local.conf", _postgres_conf(settings)))
        if settings.get("auth_enabled"):
            files.append((db.CONFIG_DIR / "postgres" / "pg_hba.local.conf", db.generate_pg_hba_local_conf(settings)))
        env["PGDATA_PATH"] = settings["pgdata_path"]
        env["DB_NAME"] = settings.get("db_name", "datasets")
        env["POSTGRES_PORT"] = str(settings.get("pg_port", 5432))
        if settings.get("pg_mem_limit"):
            env["POSTGRES_MEM_LIMIT"] = f"{settings['pg_mem_limit']}g"
        if settings.get("auth_enabled"):
            env["POSTGRES_AUTH_ENABLED"] = "true"
            if settings.get("ro_username"):
                env["POSTGRES_RO_USER"] = settings["ro_username"]
    elif name == "mongo":
        files.append((db.CONFIG_DIR / "db" / "mongo.yaml", db.generate_db_mongo_yaml(settings)))
        env["MONGO_DATA_PATH"] = settings["mongo_data_path"]
        env["MONGO_PORT"] = str(settings.get("mongo_port", 27017))
        env["MONGO_CACHE_SIZE_GB"] = str(settings.get("mongo_cache_size_gb", 2))
        if settings.get("mongo_mem_limit"):
            env["MONGO_MEM_LIMIT"] = f"{settings['mongo_mem_limit']}g"
        if settings.get("auth_enabled"):
            env["MONGO_AUTH_ENABLED"] = "true"
            env["MONGO_ADMIN_USER"] = "admin"
            if settings.get("ro_username"):
                env["MONGO_RO_USER"] = settings["ro_username"]
    else:
        files.append((db.CONFIG_DIR / "db" / "starrocks.yaml", db.generate_db_starrocks_yaml(settings)))
        files.append((db.CONFIG_DIR / "starrocks" / "fe.local.conf", db.generate_starrocks_fe_conf(settings)))
        files.append((db.CONFIG_DIR / "starrocks" / "be.local.conf", db.generate_starrocks_be_conf(settings)))
        env["STARROCKS_DATA_PATH"] = settings["starrocks_data_path"]
        env["STARROCKS_PORT"] = str(settings.get("starrocks_port", 9030))
        env["STARROCKS_FE_HTTP_PORT"] = str(settings.get("starrocks_fe_http_port", 8030))
        if settings.get("starrocks_mem_limit"):
            env["STARROCKS_MEM_LIMIT"] = f"{settings['starrocks_mem_limit']}g"
        if settings.get("auth_enabled"):
            env["STARROCKS_AUTH_ENABLED"] = "true"
            if settings.get("ro_username"):
                env["STARROCKS_RO_USER"] = settings["ro_username"]
    volumes = []
    if name == "postgres" and settings.get("tablespaces"):
        volumes = [f"{path}:/data/tablespace/{ts}" for ts, path in settings["tablespaces"].items() if ts != "pgdata"]
    if name == "starrocks" and settings.get("starrocks_storage_paths"):
        volumes = [f"{path}:/data/deploy/starrocks/be/storage_{i}" for i, path in enumerate(settings["starrocks_storage_paths"])]
    return files, env, volumes


def write_added(name, settings, files, env, volumes):
    """`add_database`'s write step. Prints what it writes, never a password."""
    utils.write_files(files)
    utils.update_env_file(env)
    print("  Updated: .env", flush=True)
    if volumes:
        db._update_override_volumes(name, volumes)
        print("  Updated: docker-compose.override.yml", flush=True)
    if settings.get("auth_enabled") and settings.get("ro_username") and settings.get("ro_password"):
        for written in _write_ro_credentials(settings):
            print(f"  Written: {written} (chmod 600)", flush=True)
