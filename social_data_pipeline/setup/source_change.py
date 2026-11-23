"""`source configure <name>` and `source add-classifiers <name>` without a terminal, and the current answers of a source.

The web UI reaches these through the broker (`sdp.do_source_configure`, `sdp.do_source_add_classifiers`, `sdp.get_source_settings`). They share the questionnaire's settings with `source add` (`source_add.build`, `source_add._classifiers`): a key of `answers` left out is the question answered with Enter, whose default on a reconfigure is the source's current value where the terminal offers it (read by the questionnaire's own readers), and the files are made by the same generators and written by `utils.write_files`, so the same answers give the same bytes (tests drive the terminal with the same answers).

- `configure_source`: what `sdp source configure <name>` writes for a source of your own files or a Hugging Face source (`source.main`, "Reconfigure?" answered yes; a Hugging Face source keeps its dataset, grouping, fields and types). For the source `reddit` the files `sdp source add reddit` writes when it is answered yes at "Reconfigure?", then the Reddit schema, field and index answers applied to platform.yaml as `sdp source configure reddit` applies them (`reddit.main`). The terminal's `source configure reddit` asks the Reddit questions first and the others only for profiles not configured yet, and then writes platform.yaml again from the template, which drops the Reddit answers it has just applied; here the order is the one that keeps them.
- `add_classifiers`: what `sdp source add-classifiers <name>` writes (`classifiers.main`): lingua.yaml and ml.yaml for the source's classifier profiles, and HF_TOKEN in .env when a token is given.
- `settings_read`: the answers that write the source's files as they are, for the questions asked for its current profiles, never a secret.

`Refused` (from `source_add`) carries one line that holds no answer's value; nothing is written before every check has passed.
"""

import contextlib
import io

from social_data_pipeline.setup import classifiers, reddit, source, source_add, utils
from social_data_pipeline.setup.source_add import CLASSIFIER_CUSTOM, CSV_DELIMITERS, LINGUA_ONLY, ML_ONLY, REDDIT_KINDS, STAGES, Refused

CLASSIFIER_ANSWERS = CLASSIFIER_CUSTOM + LINGUA_ONLY + ML_ONLY
DB_PROFILES = source_add.DB_PROFILES


def _source_folder(name):
    folder = utils.CONFIG_DIR / "sources" / name
    if not (folder / "platform.yaml").is_file():
        raise Refused("no source of this name is configured (python sdp.py source add)")
    return folder


def _db_setup():
    db_setup = utils.load_db_setup()
    if db_setup is None:
        raise Refused("No database configuration found. Run 'python sdp.py db setup' first.")
    return db_setup


def _token(settings):
    if settings and settings.get("hf_token"):
        utils.update_env_file({"HF_TOKEN": settings["hf_token"]})
        print("  Updated:   .env (HF_TOKEN)")


def configure_source(answers, hw=None):
    """Check `answers`, then write the source's files as `source configure <name>` does (see the module); print what is done. Returns the exit code; raises Refused before anything is written, a folder of another source included (the source's own folders are its own)."""
    db_setup = _db_setup()
    name = answers["name"]
    _source_folder(name)
    hw = utils.detect_hardware() if hw is None else hw
    try:
        settings, classifier_settings, reddit_answers = source_add.build(answers, hw, db_setup, reconfigure=True)
        if classifier_settings is not None:
            settings["classifier_settings"] = classifier_settings
        files = source_add.files_to_write(settings)
        reddit_yaml = reddit.generate_reddit_platform_user_yaml(*reddit_answers) if reddit_answers is not None else None
    except SystemExit:
        # The Reddit template's readers print their error and exit, as the terminal does; here nothing is written and the action ends with a refusal instead.
        raise Refused("the Reddit template (config/templates/reddit.yaml) could not be read") from None
    folders = {stage: settings[f"{stage}_path"] for stage in STAGES}
    source_add.check_folders(name, folders)

    print(f"\n  Reconfiguring source: {name}", flush=True)
    source.print_summary(settings, files)
    utils.write_files(files)
    _token(classifier_settings)
    if reddit_yaml is not None:
        reddit.apply_overrides_to_platform_yaml(name, reddit_yaml)
        print(f"  Updated:   config/sources/{name}/platform.yaml (Reddit schema, fields and indexes, as source configure reddit does)")
    source_add.print_folders(folders)
    print(f"\n  Done! Source '{name}' has been configured.", flush=True)
    profiles = settings["profiles"]
    if settings.get("hf_dataset"):
        print("\n  Next steps:")
        print(f"    python sdp.py source download {name}   # Download HF parquet files")
        print(f"    python sdp.py run parse --source {name}  # Select/clean columns")
        print()
        profiles = [profile for profile in profiles if profile != "parse"]
    utils.print_pipeline_commands(profiles, name)
    return 0


def add_classifiers(answers, hw=None):
    """Check `answers`, then write the source's classifier files as `source add-classifiers <name>` does; print what is done. Returns the exit code; raises Refused before anything is written, also for a source with neither the profile lingua nor ml (where the terminal says so and writes nothing)."""
    name = answers["name"]
    folder = _source_folder(name)
    # The source's state as `utils.require_source_state` reads it for the terminal, without its exit.
    config, _ = utils.read_source_config(name)
    if not isinstance(config, dict):
        raise Refused("the source's platform.yaml cannot be read")
    state = {"platform": config.get("platform") or ("reddit" if name == "reddit" else f"custom/{name}"), "data_types": config.get("data_types", []), "profiles": utils.get_source_profiles(name)}
    profiles = state["profiles"]
    if "lingua" not in profiles and "ml" not in profiles:
        raise Refused("the source has neither the profile lingua nor ml: add one with Reconfigure (python sdp.py source configure)")
    if state["platform"] == "reddit":
        source_add._not_asked(answers, CLASSIFIER_CUSTOM, "for a Reddit source")
    hw = utils.detect_hardware() if hw is None else hw
    # The terminal's state names no primary key (`utils.require_source_state`): none is put in front of the extra fields.
    classifier_state = {"platform": state["platform"], "data_types": state["data_types"], "profiles": profiles, "source": name}
    settings = source_add._classifiers(answers, hw, classifier_state, classifiers._load_existing_classifier_config(name))
    files = []
    if "lingua" in profiles:
        files.append((folder / "lingua.yaml", classifiers.generate_lingua_user_yaml(settings)))
    if "ml" in profiles:
        files.append((folder / "ml.yaml", classifiers.generate_ml_user_yaml(settings)))

    print(f"\n  Classifiers of source: {name}", flush=True)
    classifiers.print_summary(settings, files)
    utils.write_files(files)
    _token(settings)
    print("\n  Done! Classifier configuration has been generated.", flush=True)
    utils.print_pipeline_commands(profiles, name)
    return 0


# ── the current answers ─────────────────────────────────────────────────────


def _asked(key, platform, is_hf, profiles, data_types, answers, tablespaces):
    """Whether `source configure` asks the question `key` for a source of this kind with these profiles (the read leaves out what it would not ask, so that its answers can be sent back as they are)."""
    custom, reddit_source = platform != "reddit", platform == "reddit"
    has_postgres = any(profile.startswith("postgres") for profile in profiles)
    has_mongo, has_starrocks = "mongo_ingest" in profiles, "sr_ingest" in profiles
    classifying = "lingua" in profiles or "ml" in profiles
    rules = {
        "data_types": not is_hf,
        "parquet_row_group_size": answers.get("file_format", "parquet") == "parquet",
        "parse_workers": "parse" in profiles,
        "pg_prefer_lingua": "postgres_ingest" in profiles, "pg_parallel_index_workers": "postgres_ingest" in profiles,
        "table_tablespaces": has_postgres and bool(tablespaces),
        "db_schema": custom and has_postgres,
        "input_format": custom and not is_hf, "dump_globs": custom and not is_hf, "compressions": custom and not is_hf, "fields": custom and not is_hf,
        "csv_delimiter": custom and not is_hf and answers.get("input_format") == "csv",
        "mongo_collection_strategy": custom and has_mongo, "mongo_db_name": custom and has_mongo,
        "mongo_collections": custom and has_mongo and answers.get("mongo_collection_strategy", "per_data_type") == "per_data_type",
        "pg_indexes": custom and has_postgres, "mongo_indexes": custom and has_mongo, "sr_indexes": custom and has_starrocks,
        "primary_key": custom and (has_postgres or has_starrocks),
        "sr_prefer_lingua": has_starrocks, "sr_buckets": has_starrocks,
        "reddit_db_schema": reddit_source, "reddit_removed_fields": reddit_source, "reddit_indexes": reddit_source,
        "text_columns": custom and classifying, "remove_strings": custom and classifying, "remove_patterns": custom and classifying, "gpu_fields": custom and classifying,
        "ml_classifier_data_types": "ml" in profiles and len(data_types) > 1,
    }
    if key in LINGUA_ONLY:
        return "lingua" in profiles
    if key in ML_ONLY:
        return "ml" in profiles and rules.get(key, True)
    return rules.get(key, True)


def _reddit_answers(config):
    """The Reddit answers that make platform.yaml as it is from the template (`reddit.load_reddit_platform_config`), and what they cannot say."""
    found, left_out = {}, []
    template = reddit.load_reddit_platform_config()
    if config.get("db_schema", template["db_schema"]) != template["db_schema"]:
        found["reddit_db_schema"] = config.get("db_schema")
    fields, indexes = config.get("fields") or {}, config.get("indexes") or {}
    removed, changed_indexes = {}, False
    for kind, _ in REDDIT_KINDS.items():
        every = template["submission_fields" if kind == "submissions" else "comment_fields"]
        current = fields.get(kind, every)
        kept_in_order = [field for field in every if field in current]
        if not isinstance(current, list) or kept_in_order != current:
            left_out.append(f"the fields of {kind} are not the template's in its order, less some: a reconfigure writes them again from the template")
        elif current != every:
            removed[kind] = [field for field in every if field not in current]
        if indexes.get(kind, template["submission_indexes" if kind == "submissions" else "comment_indexes"]) != template["submission_indexes" if kind == "submissions" else "comment_indexes"]:
            changed_indexes = True
    if removed:
        found["reddit_removed_fields"] = removed
    if changed_indexes:
        found["reddit_indexes"] = {kind: indexes.get(kind, template["submission_indexes" if kind == "submissions" else "comment_indexes"]) for kind in REDDIT_KINDS}
    return found, left_out


def settings_read(name):
    """The current answers of the source `name`: {"name", "platform" (reddit or custom), "hf_dataset" (or None), "data_types" (as they are), "profiles", "answers", "typed_sr_indexes", "hf_token_set", "left_out", "not_readable"}.

    `answers` are the answers of `source configure <name>` (the fields of `source.configure` but `name`) that make the source's files as they are, for the questions asked for its current profiles, each checked against its input type: `do_source_configure(name=name, profiles=..., **answers)` with them writes the same bytes (but for what the terminal itself writes again: a custom source's field types, and the Reddit fields or indexes `left_out` names). `typed_sr_indexes` are the StarRocks indexes in the typed form of `db create-indexes`, which no answer holds and a reconfigure keeps. `hf_token_set` says whether .env has a Hugging Face token; the token itself is never read into the result, and no other secret is in the source's files. Prints nothing. `not_readable` is the line of a platform.yaml that cannot be read (then nothing else is filled in)."""
    from social_data_pipeline.control import verbs

    result = {"name": name, "platform": "reddit" if name == "reddit" else "custom", "hf_dataset": None, "data_types": [], "profiles": [], "answers": {}, "typed_sr_indexes": {}, "hf_token_set": False, "left_out": [], "not_readable": None}
    with contextlib.redirect_stdout(io.StringIO()):
        config, warning = utils.read_source_config(name)
        if not isinstance(config, dict):
            result["not_readable"] = (warning or f"config/sources/{name}/platform.yaml cannot be read").strip()
            return result
        existing = source._load_existing_source_config(name)
        current = classifiers._load_existing_classifier_config(name)
        db_setup = utils.load_db_setup() or {}
    profiles = utils.get_source_profiles(name)
    is_hf = bool(existing.get("hf_dataset"))
    result.update(profiles=profiles, hf_dataset=existing.get("hf_dataset") if isinstance(existing.get("hf_dataset"), str) else None)
    # The data types as they are (a Hugging Face source is asked none: these are its groups' names).
    data_types = existing.get("data_types")
    result["data_types"] = [item for item in data_types if isinstance(item, str)] if isinstance(data_types, list) else []
    result["hf_token_set"] = bool(utils.load_env().get("HF_TOKEN"))

    found = {}
    for key in ("data_types", "file_format", "parquet_row_group_size", "parse_workers", "pg_prefer_lingua", "pg_parallel_index_workers", "table_tablespaces", "db_schema", "input_format", "mongo_collection_strategy", "mongo_collections", "mongo_db_name", "primary_key", "sr_prefer_lingua", "sr_buckets"):
        if key in existing:
            found[key] = existing[key]
    for stage in STAGES:
        if f"{stage}_path" in existing:
            found[f"{stage}_path"] = existing[f"{stage}_path"]
    delimiter = existing.get("input_csv_delimiter", ",")
    names = {character: word for word, character in CSV_DELIMITERS.items()}
    if delimiter in names:
        found["csv_delimiter"] = names[delimiter]
    else:
        result["left_out"].append("the CSV delimiter is a character the browser does not offer: left empty, a reconfigure keeps it")
    patterns = existing.get("file_patterns") or {}
    globs = {dt: entry["dump_glob"] for dt, entry in patterns.items() if isinstance(entry, dict) and isinstance(entry.get("dump_glob"), str)}
    found["dump_globs"] = globs
    found["compressions"] = {dt: patterns[dt]["compression"] for dt, glob in globs.items() if utils.detect_compression_from_glob(glob) is None and patterns[dt].get("compression") in source_add.COMPRESSIONS}
    found["fields"] = existing.get("custom_fields") or {}
    for key, setting in (("pg_indexes", "custom_indexes"), ("mongo_indexes", "custom_mongo_indexes"), ("sr_indexes", "custom_sr_indexes")):
        entries = existing.get(setting) or {}
        found[key] = {dt: value for dt, value in entries.items() if isinstance(value, list)}
        if key == "sr_indexes":
            result["typed_sr_indexes"] = {str(dt): {kind: [str(column) for column in value.get(kind) or [] if isinstance(column, str)] for kind in ("bitmap", "bloomfilter")} for dt, value in entries.items() if isinstance(value, dict)}
    if result["platform"] == "reddit":
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                reddit_found, reddit_left_out = _reddit_answers(config)
        except SystemExit:
            # The template's reader prints its error and exits, as the terminal does; the read says so instead.
            reddit_found, reddit_left_out = {}, ["the Reddit template (config/templates/reddit.yaml) cannot be read, so the Reddit schema, fields and indexes are not filled in"]
        found.update(reddit_found)
        result["left_out"] += reddit_left_out
    for key in ("text_columns", "remove_strings", "remove_patterns", "gpu_fields", *LINGUA_ONLY, "gpu_ids", "ml_file_workers", "ml_tokenize_workers", "ml_classifier_batch_size"):
        if key in current:
            found[key] = current[key]
    if current.get("ml_classifiers"):
        found["ml_classifiers"] = [entry["name"] for entry in current["ml_classifiers"]]
        found["ml_classifier_data_types"] = {entry["name"]: entry["data_types"] for entry in current["ml_classifiers"] if entry.get("data_types")}

    inputs = verbs.VERBS["source.configure"].socket_inputs
    data_types = found.get("data_types") or []
    for key, value in found.items():
        if key not in inputs or inputs[key].type.is_secret or not _asked(key, result["platform"], is_hf, profiles, data_types, found, db_setup.get("tablespaces")):
            continue
        if value in ({}, []):
            continue
        try:
            result["answers"][key] = inputs[key].type.validate(value)
        except ValueError:
            result["left_out"].append(f"{key}: its current value is not one the browser can send back; left empty, a reconfigure takes the terminal's default for it")
    return result
