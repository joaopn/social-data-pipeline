"""`source add` without a terminal: the answers of the questionnaire given as values, the same files written.

The web UI's wizard reaches this through the broker (`sdp.do_source_add`). Every question that `sdp source add` asks for a Reddit source or a source of your own files (and, for a Reddit source, the field and index questions of `sdp source configure reddit`) is a key of `answers`; a key left out is the question answered with Enter, so its value is the default the terminal would offer, computed here on the host the same way (the hardware, `.env`'s DATA_PATH, the base classifier configuration). The settings are built in the questionnaire's order and shape (`source.run_questionnaire`, `classifiers.run_questionnaire`, `reddit.run_questionnaire`), and the files are made by the questionnaire's own generators and written by `utils.write_files`, so the same answers give the same bytes (a test runs both). One difference on purpose: a relative folder is written with a leading `./` (`utils.written_path`), as the terminal's defaults are written; the terminal writes a typed folder as typed, and Compose reads a bare `data/x` as the name of a volume.

Narrower than the terminal on purpose: an answer to a question the terminal would not ask for these answers is refused (a parse worker count without the parse profile, a compression for a dump pattern whose compression is known), and so is anything the terminal's prompts could never produce (a list item with a comma, a name that already is a source, a folder of another source). `Refused` carries one line that holds no answer's value.

A Hugging Face dataset (`hf_dataset`) comes with `hf_groups`, the answers of the terminal's grouping (`source._run_hf_config_grouping`) for the schema groups the Hub's reply was read into (`setup.hf_checked.inspect`): the schema travels in the answers, so the add asks the Hub nothing and takes no token. Its settings are made as that function makes them (the data types in the groups' order, the configs of each, its fields with their SQL types merged across groups with the first type kept, the fields left out as `mongo_exclude_fields`), and the custom platform's questions the terminal skips for it (input format, dump patterns, fields) are refused.
"""

import os

from social_data_pipeline.setup import classifiers, reddit, source, utils

REDDIT_DATA_TYPES = ["submissions", "comments"]
REDDIT_KINDS = {"submissions": ("reddit_sub_fields", "reddit_sub_indexes"), "comments": ("reddit_com_fields", "reddit_com_indexes")}
COMPRESSIONS = ("zst", "gz", "xz", "tar.gz")
# The CSV delimiters by name, as the terminal stores what was typed (`\t` is stored as a tab).
CSV_DELIMITERS = {"comma": ",", "tab": "\t", "pipe": "|", "semicolon": ";"}
STAGES = ("dumps", "extracted", "parsed", "output")
DB_PROFILES = {"postgres": ("postgres_ingest", "postgres_ml"), "mongo": ("mongo_ingest",), "starrocks": ("sr_ingest", "sr_ml")}
DEFAULT_ROW_GROUP = 1_000_000
DEFAULT_REMOVE_PATTERNS = [r"https?://\S+"]

# Which answers belong to which part of the questionnaire, for the "not asked" refusals.
CUSTOM_ONLY = ("db_schema", "input_format", "csv_delimiter", "dump_globs", "compressions", "fields", "mongo_collection_strategy", "mongo_collections", "mongo_db_name", "pg_indexes", "mongo_indexes", "sr_indexes", "primary_key", "text_columns", "remove_strings", "remove_patterns", "gpu_fields")
REDDIT_ONLY = ("reddit_db_schema", "reddit_removed_fields", "reddit_indexes")
LINGUA_ONLY = ("lingua_workers", "lingua_file_workers", "lingua_batch_size", "lingua_low_accuracy")
ML_ONLY = ("gpu_ids", "ml_file_workers", "ml_tokenize_workers", "ml_classifier_batch_size", "ml_classifiers", "ml_classifier_data_types", "hf_token")
CLASSIFIER_CUSTOM = ("text_columns", "remove_strings", "remove_patterns", "gpu_fields")
# The custom platform's questions the terminal does not ask for a Hugging Face dataset (`source.run_questionnaire`).
HF_NOT_ASKED = ("input_format", "csv_delimiter", "dump_globs", "compressions", "fields")
HF_FILE_PATTERNS = {"parquet": r"^.+\.parquet$", "csv": r"^.+\.csv$"}


class Refused(ValueError):
    """An answer the terminal would not take or not ask for. `message` is one line that holds no answer's value."""

    def __init__(self, message):
        super().__init__(message)
        self.message = message


def _not_asked(answers, keys, why):
    for key in keys:
        if key in answers:
            raise Refused(f"'{key}' is not asked {why}")


def _per_data_type(answers, key, data_types):
    """A map keyed by data type: refused when it names a data type the source does not have."""
    value = answers.get(key, {})
    unknown = [dt for dt in value if dt not in data_types]
    if unknown:
        raise Refused(f"'{key}' names a data type the source does not have")
    return value


def _list_items(key, items):
    """Items of a list the terminal reads as comma-separated text (`ask_list`): none holds a comma, none is empty or begins or ends with a space."""
    for item in items:
        if not item or "," in item or item != item.strip():
            raise Refused(f"an item of '{key}' holds a comma or a space at an end, which the terminal's comma-separated answer cannot")
    return list(items)


def _real(path):
    """A folder as a canonical path: relative to the deployment, symlinks resolved (`os.path.realpath`), at the boundary where the comparison is made."""
    return os.path.realpath(path if os.path.isabs(path) else os.path.join(str(utils.ROOT), path))


def _inside(inner, outer):
    return inner == outer or inner.startswith(outer.rstrip(os.sep) + os.sep)


def check_folders(name, folders):
    """Refuse a folder that belongs to another source: one that is a folder (dumps, extracted, parsed, output) of a source in `config/sources/` (those `source list` shows), lies inside one, or holds one. Compared as canonical real paths."""
    mine = {stage: _real(path) for stage, path in folders.items()}
    for other in utils.list_sources():
        if other == name:
            continue
        try:
            config, _ = utils.read_source_config(other)
        except Exception:
            # A platform.yaml that cannot be read names no folder the check could compare; `source list` shows the source all the same.
            config = None
        paths = config.get("paths") if isinstance(config, dict) else None
        if not isinstance(paths, dict):
            continue
        for other_path in paths.values():
            if not isinstance(other_path, str) or not other_path:
                continue
            theirs = _real(other_path)
            for stage, path in mine.items():
                if _inside(path, theirs) or _inside(theirs, path):
                    raise Refused(f"the {stage} folder is a folder of the source '{other}', or lies inside or around one: each source needs folders of its own")


def build(answers, hw, db_setup, reconfigure=False):
    """The questionnaire's settings for `answers` (see the module): (settings, classifier settings or None, the Reddit override settings or None). Raises Refused.

    With `reconfigure` the source exists, and its answers are those of `source configure <name>` (`source.main` answered yes at "Reconfigure?"): every default is the source's current value, read by the questionnaire's own readers (`source._load_existing_source_config`, `classifiers._load_existing_classifier_config`), where the terminal offers it; a Hugging Face source keeps its dataset, grouping, fields and types and is asked no data types. Without it nothing exists yet, and those readers find nothing."""
    name = answers["name"]
    platform = "reddit" if name == "reddit" else f"custom/{name}"
    if (answers.get("platform", "reddit" if name == "reddit" else "custom") == "reddit") != (platform == "reddit"):
        raise Refused("a Reddit source is the source named reddit, and only it")
    is_reddit = platform == "reddit"
    existing = source._load_existing_source_config(name) if reconfigure else {}
    is_hf = bool(existing.get("hf_dataset")) if reconfigure else "hf_dataset" in answers
    if is_reddit:
        _not_asked(answers, CUSTOM_ONLY + ("hf_dataset", "hf_groups"), "for a Reddit source")
    else:
        _not_asked(answers, REDDIT_ONLY, "for a source of your own files")
    if reconfigure:
        _not_asked(answers, ("hf_dataset", "hf_groups"), "when a source is reconfigured")
        if is_hf:
            _not_asked(answers, HF_NOT_ASKED + ("data_types",), "for a Hugging Face source: its data types, fields and types are its grouping's")
    elif is_hf:
        _not_asked(answers, HF_NOT_ASKED, "for a Hugging Face dataset")
        if "hf_groups" not in answers:
            raise Refused("a Hugging Face dataset needs its grouping ('hf_groups')")
    else:
        _not_asked(answers, ("hf_groups",), "without a Hugging Face dataset")

    # The questionnaire, step by step (`source.run_questionnaire`; for a new source nothing exists, so every default is the fresh one).
    settings = {"source_name": name}
    source.carry_over_unprompted(existing, settings)
    settings["platform"] = platform

    if reconfigure and is_hf:
        # The terminal reuses the saved grouping, fields and types (`run_questionnaire`, "Reconfigure of an HF source").
        data_types = list(existing.get("data_types", []))
        settings["data_types"] = data_types
        settings["hf_config_map"] = existing.get("hf_config_map", {})
        settings["custom_fields"] = existing.get("custom_fields", {})
        settings["custom_field_types"] = existing.get("custom_field_types", {})
        if existing.get("mongo_exclude_fields"):
            settings["mongo_exclude_fields"] = existing["mongo_exclude_fields"]
        if existing.get("hf_dataset"):
            settings["hf_dataset"] = existing["hf_dataset"]
    else:
        if reconfigure and "data_types" not in answers:
            # Enter at "Data types": the current ones (for Reddit, the two kinds when none are written).
            data_types = list(existing.get("data_types", REDDIT_DATA_TYPES) if is_reddit else existing.get("data_types") or [])
        else:
            data_types = list(answers["data_types"])
        if not data_types:
            raise Refused("at least one data type is required")
        _list_items("data_types", data_types)
        settings["data_types"] = data_types
    if is_hf and not reconfigure:
        _hf_grouping(answers, settings)

    # A folder left out is the terminal's Enter: the current one, else `<DATA_PATH>/<stage>/<name>`. Every one is written with the `./` of a relative path (`utils.written_path`): the registry's `Path` takes a typed `./` off, and Compose reads a bare relative mount source as the name of a volume.
    data_path = utils.load_env().get("DATA_PATH", "./data")
    for stage in STAGES:
        settings[f"{stage}_path"] = utils.written_path(answers.get(f"{stage}_path", existing.get(f"{stage}_path", f"{data_path}/{stage}/{name}")))

    settings["file_format"] = answers.get("file_format", existing.get("file_format", "parquet"))
    if settings["file_format"] == "parquet":
        rg_size = answers.get("parquet_row_group_size", existing.get("parquet_row_group_size", DEFAULT_ROW_GROUP))
        if rg_size != DEFAULT_ROW_GROUP:
            settings["parquet_row_group_size"] = rg_size
    else:
        _not_asked(answers, ("parquet_row_group_size",), "for CSV intermediate files")

    databases = db_setup.get("databases", ["postgres"])
    all_profiles = ["parse", "lingua", "ml"]
    for database in ("postgres", "mongo", "starrocks"):
        if database in databases:
            all_profiles += list(DB_PROFILES[database])
    profiles = list(answers["profiles"])
    if not profiles:
        raise Refused("at least one profile is required")
    if any(profile not in all_profiles for profile in profiles):
        raise Refused("a profile was chosen whose database is not set up (python sdp.py db setup)")
    settings["profiles"] = profiles

    defaults = source.compute_defaults(hw, profiles)

    if "parse" in profiles:
        settings["parse_workers"] = answers.get("parse_workers", existing.get("parse_workers", defaults["parse_workers"]))
    else:
        _not_asked(answers, ("parse_workers",), "without the profile parse")

    has_postgres = any(p.startswith("postgres") for p in profiles)
    has_mongo = "mongo_ingest" in profiles
    has_starrocks = "sr_ingest" in profiles
    if "postgres_ingest" in profiles:
        settings["pg_prefer_lingua"] = answers.get("pg_prefer_lingua", existing.get("pg_prefer_lingua", defaults["pg_prefer_lingua"]))
        settings["pg_parallel_index_workers"] = answers.get("pg_parallel_index_workers", existing.get("pg_parallel_index_workers", defaults["pg_parallel_index_workers"]))
    else:
        _not_asked(answers, ("pg_prefer_lingua", "pg_parallel_index_workers"), "without the profile postgres_ingest")

    db_tablespaces = db_setup.get("tablespaces", {}) if has_postgres else {}
    if db_tablespaces:
        ts_choices = ["pgdata"] + list(db_tablespaces.keys())
        chosen = _per_data_type(answers, "table_tablespaces", data_types)
        current = existing.get("table_tablespaces") or {}
        assigned = {dt: chosen.get(dt, current.get(dt, "pgdata")) for dt in data_types}
        if any(value not in ts_choices for value in assigned.values()):
            raise Refused("a tablespace was chosen (or is assigned now) that PostgreSQL's setup does not have")
        settings["table_tablespaces"] = assigned
        settings["tablespaces"] = db_tablespaces
    else:
        _not_asked(answers, ("table_tablespaces",), "without PostgreSQL tablespaces (set up by python sdp.py db setup)")

    if not is_reddit:
        _custom(answers, settings, name, data_types, has_postgres, has_mongo, has_starrocks, is_hf, existing)

    if has_starrocks:
        settings["sr_prefer_lingua"] = answers.get("sr_prefer_lingua", existing.get("sr_prefer_lingua", defaults["sr_prefer_lingua"]))
        cores = hw["cpu_cores"] or 4
        settings["sr_buckets"] = answers.get("sr_buckets", existing.get("sr_buckets", cores * 8))
    else:
        _not_asked(answers, ("sr_prefer_lingua", "sr_buckets"), "without the profile sr_ingest")

    classifier_settings = None
    if "lingua" in profiles or "ml" in profiles:
        classifier_state = {
            "platform": settings["platform"],
            "data_types": settings["data_types"],
            "profiles": profiles,
            "source": name,
            "primary_key": settings.get("primary_key"),
        }
        classifier_settings = _classifiers(answers, hw, classifier_state, classifiers._load_existing_classifier_config(name) if reconfigure else {})
    else:
        _not_asked(answers, CLASSIFIER_CUSTOM + LINGUA_ONLY + ML_ONLY, "without the profiles lingua and ml")

    reddit_settings = _reddit(answers) if is_reddit else None
    return settings, classifier_settings, reddit_settings


def _hf_grouping(answers, settings):
    """The settings `source._run_hf_config_grouping` makes, from the answers of each group (see the module)."""
    groups = answers["hf_groups"]
    if [group["data_type"] for group in groups] != settings["data_types"]:
        raise Refused("the data types must be the names of the Hugging Face groups, in their order")
    seen = set()
    config_map, fields, field_types, excluded_by_type = {}, {}, {}, {}
    for group in groups:
        data_type, configs = group["data_type"], list(group["configs"])
        if not configs:
            raise Refused("every Hugging Face group needs at least one config")
        if seen & set(configs):
            raise Refused("a Hugging Face config is in two groups")
        seen |= set(configs)
        names = [field["name"] for field in group["fields"]]
        if not names:
            raise Refused("at least one field is required for every data type")
        if len(set(names)) != len(names):
            raise Refused("a field is chosen twice in one Hugging Face group")
        excluded = list(group.get("excluded", []))
        if set(excluded) & set(names):
            raise Refused("a field is both chosen and left out in one Hugging Face group")
        config_map[data_type] = configs
        fields[data_type] = names
        if excluded:
            excluded_by_type[data_type] = excluded
        for field in group["fields"]:
            fname, ftype = field["name"], field["sql_type"]
            if fname in field_types and field_types[fname] != ftype:
                print(f"    Warning: type conflict for '{fname}': {field_types[fname]} vs {ftype} (keeping first)")
            else:
                field_types[fname] = ftype
    settings["hf_config_map"] = config_map
    settings["custom_fields"] = fields
    settings["custom_field_types"] = field_types
    if excluded_by_type:
        settings["mongo_exclude_fields"] = excluded_by_type


def _custom(answers, settings, name, data_types, has_postgres, has_mongo, has_starrocks, is_hf=False, existing=None):
    """The block of `run_questionnaire` for a custom platform; for a Hugging Face dataset its fields and types come from the grouping (`_hf_grouping`) or, on a reconfigure, from the source as it is, and its input is Parquet. `existing` holds the defaults of a reconfigure."""
    existing = existing or {}
    if has_postgres:
        settings["db_schema"] = answers.get("db_schema", existing.get("db_schema", name))
    else:
        _not_asked(answers, ("db_schema",), "without a PostgreSQL profile")

    if is_hf:
        settings["input_format"] = "parquet"
        settings["custom_file_patterns"] = {dt: dict(HF_FILE_PATTERNS) for dt in data_types}
    else:
        _custom_files(answers, settings, data_types, existing)
    _custom_databases(answers, settings, name, data_types, has_postgres, has_mongo, has_starrocks, existing)


def _custom_files(answers, settings, data_types, existing):
    """The input format, dump patterns and fields of a source of your own files. The compression of a pattern whose extension names none defaults to zst also on a reconfigure, as the terminal's question does."""
    settings["input_format"] = answers.get("input_format", existing.get("input_format", "ndjson"))
    if settings["input_format"] == "csv":
        # A delimiter answered by its name; left out, the current one (any character the terminal took), else a comma.
        settings["input_csv_delimiter"] = CSV_DELIMITERS[answers["csv_delimiter"]] if "csv_delimiter" in answers else existing.get("input_csv_delimiter", ",")
    else:
        _not_asked(answers, ("csv_delimiter",), "for NDJSON input")

    globs = _per_data_type(answers, "dump_globs", data_types)
    compressions = _per_data_type(answers, "compressions", data_types)
    current_patterns = existing.get("file_patterns") or {}
    settings["custom_file_patterns"] = {}
    for dt in data_types:
        dump_glob = globs.get(dt) or ((current_patterns.get(dt) or {}).get("dump_glob") if isinstance(current_patterns.get(dt), dict) else None)
        if not dump_glob:
            raise Refused("a dump file pattern is required for every data type")
        compression = utils.detect_compression_from_glob(dump_glob)
        if compression is None:
            compression = compressions.get(dt, "zst")
        elif dt in compressions:
            raise Refused("a compression was given for a data type whose dump pattern names it already")
        settings["custom_file_patterns"][dt] = utils.derive_file_patterns(dump_glob, compression, input_format=settings["input_format"])

    fields = _per_data_type(answers, "fields", data_types)
    current_fields = existing.get("custom_fields") or {}
    settings["custom_fields"] = {}
    for dt in data_types:
        chosen = fields.get(dt) or current_fields.get(dt)
        if not chosen:
            raise Refused("at least one field is required for every data type")
        settings["custom_fields"][dt] = _list_items("fields", chosen)


def _custom_databases(answers, settings, name, data_types, has_postgres, has_mongo, has_starrocks, existing):
    """The MongoDB, index and primary key questions of a custom platform.

    An index answer left out for a data type keeps its current indexes, as Enter does in the terminal. One difference, on purpose: StarRocks indexes written in the typed form of `db create-indexes` (`{bitmap: [...], bloomfilter: [...]}`) are kept as they are, where the terminal's Enter would take the two keys for column names."""
    if has_mongo:
        settings["mongo_collection_strategy"] = answers.get("mongo_collection_strategy", existing.get("mongo_collection_strategy", "per_data_type"))
        collections = _per_data_type(answers, "mongo_collections", data_types)
        current_collections = existing.get("mongo_collections") or {}
        if settings["mongo_collection_strategy"] == "per_data_type":
            settings["mongo_collections"] = {dt: collections.get(dt, current_collections.get(dt, dt)) for dt in data_types}
        elif collections:
            raise Refused("'mongo_collections' is not asked with the strategy per_file")
        settings["mongo_db_name"] = answers.get("mongo_db_name", existing.get("mongo_db_name", name))
    else:
        _not_asked(answers, ("mongo_collection_strategy", "mongo_collections", "mongo_db_name", "mongo_indexes"), "without the profile mongo_ingest")

    if has_postgres or has_mongo or has_starrocks:
        for key, setting, asked in (("pg_indexes", "custom_indexes", has_postgres), ("mongo_indexes", "custom_mongo_indexes", has_mongo), ("sr_indexes", "custom_sr_indexes", has_starrocks)):
            if not asked:
                _not_asked(answers, (key,), "without its database's profile")
                continue
            given = _per_data_type(answers, key, data_types)
            current = existing.get(setting) or {}
            settings[setting] = {}
            for dt in data_types:
                chosen = given.get(dt) or current.get(dt)
                if isinstance(chosen, dict) and setting == "custom_sr_indexes":
                    settings[setting][dt] = chosen
                elif chosen:
                    settings[setting][dt] = _list_items(key, chosen)
    else:
        _not_asked(answers, ("pg_indexes", "mongo_indexes", "sr_indexes"), "without a database profile")

    if has_postgres or has_starrocks:
        settings["primary_key"] = answers.get("primary_key") or existing.get("primary_key") or None
    else:
        _not_asked(answers, ("primary_key",), "without a PostgreSQL or StarRocks profile")


def _classifiers(answers, hw, state, existing=None):
    """`classifiers.run_questionnaire`: `existing` holds the source's current classifier answers (`classifiers._load_existing_classifier_config`), the defaults of a reconfigure and of `source add-classifiers`; for a new source there are none. The GPUs default to every GPU found also then, as the terminal's question does."""
    existing = existing or {}
    profiles = state["profiles"]
    platform = state["platform"]
    data_types = state["data_types"]
    settings = {"platform": platform, "data_types": data_types, "profiles": profiles}
    defaults = classifiers.compute_classifier_defaults(hw, profiles)

    if platform == "reddit":
        text_columns, remove_strings, remove_patterns, gpu_fields = classifiers.ask_text_columns(platform, data_types)
    else:
        given = _per_data_type(answers, "text_columns", data_types)
        current_columns = existing.get("text_columns") or {}
        text_columns = {}
        for dt in data_types:
            chosen = given.get(dt) or current_columns.get(dt)
            if chosen:
                text_columns[dt] = _list_items("text_columns", chosen)
        remove_strings = _list_items("remove_strings", answers["remove_strings"] if "remove_strings" in answers else existing.get("remove_strings") or [])
        remove_patterns = _list_items("remove_patterns", answers["remove_patterns"] if "remove_patterns" in answers else existing.get("remove_patterns") or DEFAULT_REMOVE_PATTERNS)
        primary_key = state.get("primary_key")
        current_extras = [field for field in (existing.get("gpu_fields") or []) if field != primary_key]
        extras = _list_items("gpu_fields", answers["gpu_fields"] if "gpu_fields" in answers else current_extras)
        gpu_fields = ([primary_key] if primary_key else []) + [f for f in extras if f != primary_key]
    settings["text_columns"] = text_columns
    settings["remove_strings"] = remove_strings
    settings["remove_patterns"] = remove_patterns
    settings["gpu_fields"] = gpu_fields

    if "lingua" in profiles:
        for key in LINGUA_ONLY:
            settings[key] = answers.get(key, existing.get(key, defaults[key]))
    else:
        _not_asked(answers, LINGUA_ONLY, "without the profile lingua")

    if "ml" in profiles:
        gpus = hw["gpus"]
        if gpus:
            detected = [g["index"] for g in gpus]
            gpu_ids = answers.get("gpu_ids", detected)
            if any(gpu not in detected for gpu in gpu_ids):
                raise Refused("a GPU was chosen that this machine does not have")
        else:
            gpu_ids = answers.get("gpu_ids", [0])
        if not gpu_ids:
            raise Refused("at least one GPU is required")
        settings["gpu_ids"] = list(gpu_ids)
        defaults["ml_file_workers"] = max(1, len(settings["gpu_ids"]))
        for key in ("ml_file_workers", "ml_tokenize_workers", "ml_classifier_batch_size"):
            settings[key] = answers.get(key, existing.get(key, defaults[key]))

        available = defaults["ml_classifiers"]
        current_classifiers = existing.get("ml_classifiers") or []
        current_scopes = {entry["name"]: entry.get("data_types") for entry in current_classifiers}
        selected = list(answers["ml_classifiers"] if "ml_classifiers" in answers else [entry["name"] for entry in current_classifiers] or available)
        if not selected:
            raise Refused("at least one classifier is required")
        if any(name not in available for name in selected):
            raise Refused("a classifier was chosen (or is chosen now) that the base configuration (config/ml/gpu_classifiers.yaml) does not define")
        scopes = answers.get("ml_classifier_data_types", {})
        if any(name not in selected for name in scopes):
            raise Refused("'ml_classifier_data_types' names a classifier that was not chosen")
        if scopes and len(data_types) <= 1:
            raise Refused("'ml_classifier_data_types' is not asked for a source with one data type")
        entries = []
        for name in selected:
            if len(data_types) <= 1:
                entries.append({"name": name, "data_types": None})
                continue
            chosen = list(scopes[name] if name in scopes else current_scopes.get(name) or data_types)
            if not chosen or any(dt not in data_types for dt in chosen):
                raise Refused("a classifier's data types must be some of the source's data types")
            entries.append({"name": name, "data_types": None if set(chosen) == set(data_types) else chosen})
        settings["ml_classifiers"] = entries

        token = answers.get("hf_token", "")
        if token != token.strip():
            raise Refused("the Hugging Face token begins or ends with a space, which the terminal strips")
        settings["hf_token"] = token
    else:
        _not_asked(answers, ML_ONLY, "without the profile ml")
    return settings


def _reddit(answers):
    """`reddit.run_questionnaire` (asked by `source configure reddit`), or None when no Reddit question was answered."""
    if not any(key in answers for key in REDDIT_ONLY):
        return None
    platform_config = reddit.load_reddit_platform_config()
    settings = {"db_schema": answers.get("reddit_db_schema", platform_config["db_schema"])}
    removed = answers.get("reddit_removed_fields")
    if removed is not None:
        for kind, (fields_key, _) in REDDIT_KINDS.items():
            every = platform_config["submission_fields" if kind == "submissions" else "comment_fields"]
            gone = removed.get(kind, [])
            if any(field not in every for field in gone):
                raise Refused(f"a field to remove from {kind} is not in its field list (config/templates/reddit.yaml)")
            kept = [field for field in every if field not in gone]
            if not kept:
                raise Refused(f"at least one field of {kind} must be kept")
            settings[fields_key] = kept
    indexes = answers.get("reddit_indexes")
    if indexes is not None:
        for kind, (_, indexes_key) in REDDIT_KINDS.items():
            default = platform_config["submission_indexes" if kind == "submissions" else "comment_indexes"]
            settings[indexes_key] = _list_items("reddit_indexes", indexes.get(kind, default))
    return settings, platform_config


def files_to_write(settings):
    """The files `source.main` writes for these settings, in its order."""
    profiles = settings["profiles"]
    folder = utils.CONFIG_DIR / "sources" / settings["source_name"]
    generate = source.generate_reddit_platform_yaml if settings["platform"] == "reddit" else source.generate_platform_yaml
    files = [(folder / "platform.yaml", generate(settings))]
    for profile, filename, generator in (
        ("parse", "parse.yaml", source.generate_parse_yaml),
        ("postgres_ingest", "postgres.yaml", source.generate_postgres_yaml),
        ("postgres_ml", "postgres_ml.yaml", source.generate_postgres_ml_yaml),
        ("mongo_ingest", "mongo.yaml", source.generate_mongo_yaml),
        ("sr_ingest", "starrocks.yaml", source.generate_starrocks_yaml),
        ("sr_ml", "sr_ml.yaml", source.generate_sr_ml_yaml),
    ):
        if profile in profiles:
            files.append((folder / filename, generator(settings)))
    if "lingua" in profiles and "classifier_settings" in settings:
        files.append((folder / "lingua.yaml", classifiers.generate_lingua_user_yaml(settings["classifier_settings"])))
    if "ml" in profiles and "classifier_settings" in settings:
        files.append((folder / "ml.yaml", classifiers.generate_ml_user_yaml(settings["classifier_settings"])))
    return files


def print_folders(folders):
    """Say for each of a source's folders whether it exists (none is created)."""
    print("\n  Data folders:")
    for stage in STAGES:
        state = "exists" if os.path.isdir(_real(folders[stage])) else "missing: the first run of a profile creates it"
        print(f"    {stage}: {folders[stage]} ({state})")


def add_source(answers, hw=None):
    """Check `answers`, then write the source's files as `source add` (and `source configure reddit` for the Reddit questions) does; print what is done. Returns the exit code; raises Refused before anything is written."""
    db_setup = utils.load_db_setup()
    if db_setup is None:
        raise Refused("No database configuration found. Run 'python sdp.py db setup' first.")
    name = answers["name"]
    if os.path.lexists(utils.CONFIG_DIR / "sources" / name):
        raise Refused("a source of this name exists already: change it with Reconfigure (python sdp.py source configure)")
    hw = utils.detect_hardware() if hw is None else hw
    try:
        settings, classifier_settings, reddit_answers = build(answers, hw, db_setup)
        if "hf_dataset" in answers:
            settings["hf_dataset"] = answers["hf_dataset"]
        if classifier_settings is not None:
            settings["classifier_settings"] = classifier_settings
        files = files_to_write(settings)
    except SystemExit:
        # The Reddit template's readers print their error and exit, as the terminal does; here nothing is written and the action ends with a refusal instead.
        raise Refused("the Reddit template (config/templates/reddit.yaml) could not be read") from None
    # The folders as they will be written, the defaults of those left out included.
    folders = {stage: settings[f"{stage}_path"] for stage in STAGES}
    check_folders(name, folders)
    reddit_yaml = None
    if reddit_answers is not None:
        reddit_yaml = reddit.generate_reddit_platform_user_yaml(*reddit_answers)

    print(f"\n  Adding source: {name}", flush=True)
    source.print_summary(settings, files)
    utils.write_files(files)
    if classifier_settings and classifier_settings.get("hf_token"):
        utils.update_env_file({"HF_TOKEN": classifier_settings["hf_token"]})
        print("  Updated:   .env (HF_TOKEN)")
    if reddit_yaml is not None:
        reddit.apply_overrides_to_platform_yaml(name, reddit_yaml)
        print(f"  Updated:   config/sources/{name}/platform.yaml (Reddit fields and indexes, as source configure does)")
    print_folders(folders)
    print(f"\n  Done! Source '{name}' has been configured.", flush=True)
    if settings.get("hf_dataset"):
        print("\n  Next steps:")
        print(f"    python sdp.py source download {name}   # Download HF parquet files")
        print(f"    python sdp.py run parse --source {name}  # Select/clean columns")
        print()
        utils.print_pipeline_commands([p for p in settings["profiles"] if p != "parse"], name)
        return 0
    utils.print_pipeline_commands(settings["profiles"], name)
    return 0
