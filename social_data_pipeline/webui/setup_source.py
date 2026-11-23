"""The Add source wizard (`sdp source add` for Reddit dumps, a Hugging Face dataset and your own files): the mock-up's six steps, Origin, Files, Fields, Classifiers, Databases and Review, each a page with Back, Next and Cancel, then the existing pending action and confirm page (`source.add`, step-up). `install(api, ctx)` adds the routes; `app.create_app` calls it.

- `GET /add-source` shows Origin (three cards; `ORIGINS` is the list, so another origin is one entry more); a card posts to `POST /add-source`, which reads the configured databases (`db.status`) and the sources (`source.list`) in a worker of the pages' bound and keeps a new wizard in the session.
- `GET /add-source/<id>/<step>` shows a step with the answers kept; `POST /add-source/<id>/<step>` keeps what the step posted (its texts as posted, a token in `actions.Hidden`) and goes to the step before (`nav=back`) or checks the step and goes to the next one needed (`nav=next`; Classifiers only with lingua or ml, Databases only with a database profile or a Reddit source). `POST /add-source/<id>/cancel` forgets the wizard. A refused answer shows the step again with the line naming it.
- Review shows what will be done and posts `nav=confirm`: the answers are made into the fields of `source.add`, checked by the table (`actions.check_fields`), kept as a pending action and the wizard is forgotten; the confirm page then asks for the management password. Nothing is written before that confirmation.

A Hugging Face dataset (`hf`): Files asks the dataset id and its token (a password field, never drawn back, kept in the wizard as `actions.Hidden`); its Next reads the schema from the Hub through the broker (`source.hf_inspect`, `read_dataset`) and keeps it in the wizard. Fields shows the terminal's grouping (per group its configs, the data type's name, the fields and a type for each field without a SQL mapping). Review has Download now, ticked by default: the pending action of `source.add` then carries `source.download` with the token as the step that follows (`work.Pending.then`), which the confirm starts when the add has ended with exit code 0, and the token is dropped with it once it was sent.

Reconfigure and Add classifiers (`sdp source configure <name>`, `sdp source add-classifiers <name>`) are the same wizard in another mode (`state["mode"]`: `configure`, `classifiers`; `setup_source_change` starts them, prefilled from the source's current answers): Reconfigure starts at Files with the Origin fixed and asks a Hugging Face source no Fields step (its grouping is kept); Add classifiers is the Classifiers step alone, then Review. Their confirm makes the pending action of `source.configure` or `source.add_classifiers`, and an empty answer there keeps the current value (the terminal's Enter on a reconfigure).

The wizard lives in the session's pending actions (`work.SessionWork`, `wizard=True`): it counts against their cap of sixteen and their weight, and expires five minutes after its last step, like a pending action. Each step posts far fewer than the guard's 64 fields: lists of names are typed comma-separated as in the terminal, and the wizard takes at most eight data types. Empty answers are the terminal's Enter: the action computes those defaults on the host.
"""

import json
import re

from fastapi import Request
from fastapi.responses import PlainTextResponse, RedirectResponse

from social_data_pipeline.webui import actions, overview, pages, words
from social_data_pipeline.webui.calls import BUSY_GATE, call_broker, session_ends, why
from social_data_pipeline.webui.work import ID_PATTERN

STEPS = (("origin", "Origin"), ("files", "Files"), ("fields", "Fields"), ("classifiers", "Classifiers"), ("databases", "Databases"), ("review", "Review"))
STEP_KEYS = tuple(key for key, _ in STEPS)
# The origins the wizard offers: (platform, title, line). `hf` is a source of the platform custom with a Hugging Face dataset.
ORIGINS = (("reddit", "Reddit dumps", "Arctic Shift monthly submissions and comments"), ("hf", "Hugging Face dataset", "downloaded for you from the Hub, as Parquet"), ("custom", "Your own files", "JSON or CSV, one record per line or row"))
HF_SQL_TYPES = actions.HF_SQL_TYPES
# How many fields a step may post (the guard's limit is 64; the form token and the button take two): the Fields step of a Hugging Face dataset draws its fields as boxes only when they fit.
STEP_FIELDS_MAX = 60
STAGES = ("dumps", "extracted", "parsed", "output")
DATA_TYPES_MAX = 8
# The most bytes the fields of the request may take (the broker's frame is 64 KiB, and the audit log's line holds them too).
REQUEST_MAX_BYTES = 32 * 1024
DB_PROFILES = {"postgres": ("postgres_ingest", "postgres_ml"), "mongo": ("mongo_ingest",), "starrocks": ("sr_ingest", "sr_ml")}
REDDIT_KINDS = ("submissions", "comments")
DEFAULT_REMOVE_PATTERN = r"https?://\S+"
GONE = "This wizard is no longer kept: a wizard is forgotten five minutes after its last step, and when the session keeps too many. Start again with Add source."
NOT_ACTIVE = "sources"
# Lines of the refusals, each naming no value.
LINES = {
    "name": "The source name must be a plain name: a letter or digit, then letters, digits, '_', '.' and '-', at most 64 characters, and not reddit (Reddit dumps are the source reddit).",
    "exists": "A source of this name exists already: change it with Reconfigure on its page, or choose another name.",
    "data_types": f"Give 1 to {DATA_TYPES_MAX} data types, comma-separated, each a plain name, none twice.",
    "profiles": "Choose at least one profile.",
    "names": "Each item must be a plain name (letters, digits, '_', '.' and '-'), comma-separated, none twice.",
    "int": "A number is out of range or not a whole number.",
    "glob": "Give a dump file pattern for every data type: letters, digits and _ . * ? [ ] ! - (no folder).",
    "fields": "Give at least one field for every data type.",
    "text": "An item holds a character the terminal cannot take (a control character).",
    "token": "The token must be one line without spaces at its ends.",
    "too_large": "The answers are too large for one request: use fewer or shorter names.",
    "compression": "Choose a compression only for a pattern whose extension does not name one (such as .zst, .gz, .xz, .tar.gz).",
    "scope": "Choose the data types of each classifier below (empty: all of them), then Next again.",
    "dataset": "Give the Hugging Face dataset as <owner>/<name>: letters, digits, '_', '.' and '-', each part at most 96 characters.",
    "inspect": "Read the dataset from the Hub first: Next on the Files step reads it.",
    "hf_configs": "Each group's configs must be some of its own, comma-separated (empty: all of them).",
    "hf_fields": "Choose at least one field for every group (a field without a SQL mapping counts once it has a type).",
    "hf_unknown_field": "Each group's fields must be some of its own fields with a SQL mapping, comma-separated (empty: all of them).",
    "hf_cast": "Choose skip or one of the SQL types for a field without a mapping.",
    "hf_too_many": "This dataset has more fields without a SQL mapping than one page of the browser can post: add it in a terminal with python sdp.py source add <name> --hf <dataset>.",
    "hf_shape": "The broker's answer about the dataset did not have the expected shape.",
}


def _q(value):
    return actions._q(value)


def suggest_data_type_name(config_names):
    """The name `source add --hf` suggests for a group of configs: a literal copy of `setup.source._suggest_data_type_name` (the web UI's image holds no setup module; a test compares them)."""
    if len(config_names) == 1:
        return config_names[0]
    first = config_names[0]
    for sep in ("-", "_", "."):
        prefix = first.split(sep)[0]
        if prefix and all(c.startswith(prefix) for c in config_names):
            return prefix
    return config_names[0]


def checked_schema(data):
    """The broker's answer of `source.hf_inspect` as the wizard keeps it, its shape checked: {"dataset", "groups": [{"configs": [{"name", "rows"}], "fields": [{"name", "sql_type", "hf_type"}]}], "boxes"} (`boxes`: whether the Fields step draws each field as a box, which it does when they fit in one post). Raises StepError ("hf_shape", or "hf_too_many" when even the comma-separated form cannot be posted)."""
    try:
        if not isinstance(data, dict) or not actions.is_hf_dataset(data.get("dataset")) or not isinstance(data.get("groups"), list) or not 1 <= len(data["groups"]) <= 8:
            raise ValueError
        groups = []
        for group in data["groups"]:
            configs = [{"name": c["name"], "rows": c.get("rows") if isinstance(c.get("rows"), int) and not isinstance(c.get("rows"), bool) and c.get("rows") >= 0 else None} for c in group["configs"]]
            fields = [{"name": f["name"], "sql_type": f["sql_type"], "hf_type": f["hf_type"]} for f in group["fields"]]
            if not 1 <= len(configs) <= 256 or len(fields) > 256 or not all(actions.is_plain_name(c["name"]) for c in configs) or len({c["name"] for c in configs}) != len(configs):
                raise ValueError
            if not all(actions.is_plain_name(f["name"]) and (f["sql_type"] is None or f["sql_type"] in HF_SQL_TYPES) and isinstance(f["hf_type"], str) and len(f["hf_type"]) <= 64 and not actions.has_control_character(f["hf_type"]) for f in fields) or len({f["name"] for f in fields}) != len(fields):
                raise ValueError
            groups.append({"configs": configs, "fields": fields})
    except (ValueError, KeyError, TypeError):
        raise StepError("files", "hf_shape") from None
    unmappable = sum(1 for g in groups for f in g["fields"] if f["sql_type"] is None)
    boxes = 2 + sum(2 + len(g["fields"]) for g in groups) <= STEP_FIELDS_MAX
    if not boxes and 2 + 3 * len(groups) + unmappable > STEP_FIELDS_MAX:
        raise StepError("files", "hf_too_many")
    return {"dataset": data["dataset"], "groups": groups, "boxes": boxes}


# A literal copy of `setup.utils.detect_compression_from_glob` (the web UI's image holds no setup module): the extensions that name a compression, in its order. A test compares them.
COMPRESSION_SUFFIXES = ((".tar.gz", "tar.gz"), (".tgz", "tar.gz"), (".zst", "zst"), (".json.gz", "gz"), (".gz", "gz"), (".xz", "xz"))


def compression_of(pattern):
    """The compression a dump file pattern's extension names, or None (then the terminal asks it)."""
    lower = pattern.lower()
    return next((compression for suffix, compression in COMPRESSION_SUFFIXES if lower.endswith(suffix)), None)


def offered_profiles(databases):
    """The profiles `source add` offers with these databases set up, in its order."""
    offered = ["parse", "lingua", "ml"]
    for database in ("postgres", "mongo", "starrocks"):
        if database in databases:
            offered += list(DB_PROFILES[database])
    return offered


class StepError(Exception):
    def __init__(self, step, key):
        super().__init__(key)
        self.step = step
        self.line = LINES[key]


def _split(text):
    """A comma-separated answer as the terminal reads it (`utils.ask_list`)."""
    return [item.strip() for item in (text or "").split(",") if item.strip()]


def _names(step, text, unique=True):
    items = _split(text)
    if not all(actions.is_plain_name(item) for item in items) or (unique and len(set(items)) != len(items)):
        raise StepError(step, "names")
    return items


def _int(step, text, spec):
    text = (text or "").strip()
    if not text:
        return None
    if not re.fullmatch(r"(?:0|[1-9][0-9]{0,10})", text) or not spec.lo <= int(text) <= spec.hi:
        raise StepError(step, "int")
    return int(text)


def _texts(step, text, max_bytes):
    items = _split(text)
    if any(actions.has_control_character(item) or len(item.encode("utf-8", "surrogatepass")) > max_bytes for item in items):
        raise StepError(step, "text")
    return items


class Answers:
    """The fields of `source.add` from a wizard's kept texts, step by step (`upto`: the last step read; the steps after it are not looked at)."""

    def __init__(self, state, upto="review"):
        self.state = state
        self.raw = state["raw"]
        self.upto = STEP_KEYS.index(upto)
        self.mode = state.get("mode", "add")
        self.fields = {"platform": "custom" if state["platform"] == "hf" else state["platform"]} if self.mode == "add" else {"name": state["name"]}

    def get(self, step, key, default=""):
        return self.raw.get(step, {}).get(key, default)

    def reached(self, step):
        return STEP_KEYS.index(step) <= self.upto

    @property
    def reddit(self):
        return self.state["platform"] == "reddit"

    @property
    def hf(self):
        return self.state["platform"] == "hf"

    @property
    def configure(self):
        return self.mode == "configure"

    @property
    def data_types(self):
        """The data types answered on Files, or (a Hugging Face source reconfigured, Add classifiers) the source's current ones."""
        return self.fields["data_types"] if "data_types" in self.fields else list(self.state.get("current", {}).get("data_types", []))

    def build(self):
        if self.mode == "classifiers":
            if self.reached("classifiers"):
                self._classifiers()
            return self.fields
        self._files()
        if self.reached("fields"):
            self._fields_step()
        if self.reached("classifiers") and self.has_classifiers:
            self._classifiers()
        if self.reached("databases") and self.has_databases:
            self._databases()
        return self.fields

    # ── files ──

    def _files(self):
        f = self.fields
        if self.configure:
            name = f["name"]
        else:
            name = "reddit" if self.reddit else self.get("files", "name").strip()
            if not self.reddit and (not actions.is_plain_name(name) or name == "reddit"):
                raise StepError("files", "name")
            if name in self.state["existing"]:
                raise StepError("files", "exists")
            f["name"] = name
        if self.hf and self.configure:
            # A Hugging Face source keeps its data types, the names of its groups (the terminal asks none on a reconfigure).
            pass
        elif self.hf:
            # The data types are the groups' names, given on the Fields step (the terminal asks them in its grouping).
            dataset = self.get("files", "dataset").strip()
            if not actions.is_hf_dataset(dataset):
                raise StepError("files", "dataset")
            f["hf_dataset"] = dataset
        else:
            data_types = _split(self.get("files", "data_types", "submissions, comments" if self.reddit else ""))
            if not 1 <= len(data_types) <= DATA_TYPES_MAX or not all(actions.is_plain_name(dt) for dt in data_types) or len(set(data_types)) != len(data_types):
                raise StepError("files", "data_types")
            f["data_types"] = data_types
        for stage in STAGES:
            path = self.get("files", f"{stage}_path").strip()
            if not path and self.configure:
                # Empty on a reconfigure: the current folder (the terminal's Enter).
                continue
            path = path or f"data/{stage}/{name}"
            spec = actions.SOURCE_ADD_INPUTS[f"{stage}_path"]
            value = actions._leaf_from_form(spec, path)
            if not actions._value_is_valid(spec, value):
                raise StepError("files", "text")
            f[f"{stage}_path"] = value
        f["file_format"] = self.get("files", "file_format", "parquet")
        if f["file_format"] not in ("parquet", "csv"):
            raise StepError("files", "int")
        if f["file_format"] == "parquet":
            size = _int("files", self.get("files", "parquet_row_group_size"), actions.SOURCE_ADD_INPUTS["parquet_row_group_size"])
            if size is not None:
                f["parquet_row_group_size"] = size
        offered = offered_profiles(self.state["databases"])
        if "files" in self.raw:
            profiles = [profile for profile in offered if self.get("files", f"profiles[{profile}]") == actions.CHECKED]
        else:
            profiles = [profile for profile in offered if profile != "ml"]
        if not profiles:
            raise StepError("files", "profiles")
        f["profiles"] = profiles
        if "parse" in profiles:
            workers = _int("files", self.get("files", "parse_workers"), actions.SOURCE_ADD_INPUTS["parse_workers"])
            if workers is not None:
                f["parse_workers"] = workers
        if not self.reddit and not self.hf:
            f["input_format"] = self.get("files", "input_format", "ndjson") if self.get("files", "input_format", "ndjson") in ("ndjson", "csv") else "ndjson"
            if f["input_format"] == "csv":
                delimiter = self.get("files", "csv_delimiter", "comma")
                f["csv_delimiter"] = delimiter if delimiter in ("comma", "tab", "pipe", "semicolon") else "comma"

    @property
    def profiles(self):
        if self.mode == "classifiers":
            return list(self.state.get("current", {}).get("profiles", []))
        return self.fields.get("profiles", [])

    @property
    def has_classifiers(self):
        return "lingua" in self.profiles or "ml" in self.profiles

    @property
    def has_databases(self):
        return self.reddit or any(profile in self.profiles for profiles in DB_PROFILES.values() for profile in profiles)

    # ── fields ──

    def _fields_step(self):
        if self.hf:
            self._hf_fields()
            return
        f, data_types = self.fields, self.data_types
        if self.reddit:
            removed = {kind: _names("fields", self.get("fields", f"removed[{kind}]")) for kind in REDDIT_KINDS}
            removed = {kind: items for kind, items in removed.items() if items}
            if removed:
                f["reddit_removed_fields"] = removed
            return
        globs, compressions, fields = {}, {}, {}
        for dt in data_types:
            pattern = self.get("fields", f"glob[{dt}]").strip()
            if not pattern or not actions._value_is_valid(actions.Input("filter", ""), pattern):
                raise StepError("fields", "glob")
            globs[dt] = pattern
            compression = self.get("fields", f"compression[{dt}]")
            if compression in ("zst", "gz", "xz", "tar.gz"):
                if compression_of(pattern) is not None:
                    # The terminal asks the compression only when the pattern's extension does not name it.
                    raise StepError("fields", "compression")
                compressions[dt] = compression
            items = _names("fields", self.get("fields", f"fields[{dt}]"))
            if not items:
                raise StepError("fields", "fields")
            fields[dt] = items
        f["dump_globs"], f["fields"] = globs, fields
        if compressions:
            f["compressions"] = compressions

    def _hf_fields(self):
        """The grouping of `source add --hf`: per group of the schema read from the Hub, its configs (empty: all, the terminal's Enter), the data type's name (empty: the terminal's suggestion), the fields with a SQL mapping (boxes, or a comma-separated list when they do not fit in one post; empty: all) and a type or skip for each field without one. Configs and fields are kept in the Hub's order."""
        f, step = self.fields, "fields"
        schema = self.state.get("hf")
        if not schema or schema["dataset"] != f["hf_dataset"]:
            raise StepError("files", "inspect")
        visited = step in self.raw
        groups, data_types = [], []
        for number, group in enumerate(schema["groups"]):
            every_config = [config["name"] for config in group["configs"]]
            chosen = _names(step, self.get(step, f"configs[{number}]"))
            if any(config not in every_config for config in chosen):
                raise StepError(step, "hf_configs")
            configs = [config for config in every_config if config in chosen] if chosen else every_config
            data_type = self.get(step, f"dt[{number}]").strip() or suggest_data_type_name(configs)
            if not actions.is_plain_name(data_type):
                raise StepError(step, "names")
            mapped = {field["name"]: field["sql_type"] for field in group["fields"] if field["sql_type"]}
            if schema["boxes"]:
                kept = [name for name in mapped if not visited or self.get(step, f"field[{number}][{name}]") == actions.CHECKED]
            else:
                typed = _names(step, self.get(step, f"fields[{number}]"))
                if any(name not in mapped for name in typed):
                    raise StepError(step, "hf_unknown_field")
                kept = [name for name in mapped if name in typed] if typed else list(mapped)
            chosen_fields = [{"name": name, "sql_type": mapped[name]} for name in kept]
            for field in group["fields"]:
                if field["sql_type"] is None:
                    cast = self.get(step, f"cast[{number}][{field['name']}]", "skip")
                    if cast != "skip" and cast not in HF_SQL_TYPES:
                        raise StepError(step, "hf_cast")
                    if cast != "skip":
                        chosen_fields.append({"name": field["name"], "sql_type": cast})
            if not chosen_fields:
                raise StepError(step, "hf_fields")
            names = {field["name"] for field in chosen_fields}
            entry = {"configs": configs, "data_type": data_type, "fields": chosen_fields}
            excluded = [field["name"] for field in group["fields"] if field["name"] not in names]
            if excluded:
                entry["excluded"] = excluded
            groups.append(entry)
            data_types.append(data_type)
        if len(set(data_types)) != len(data_types):
            raise StepError(step, "names")
        f["data_types"], f["hf_groups"] = data_types, groups

    @property
    def download(self):
        """Whether Download now is ticked: by default, until Review was posted without it."""
        return "review" not in self.raw or self.get("review", "download") == actions.CHECKED

    # ── classifiers ──

    def _classifiers(self):
        f, data_types, profiles = self.fields, self.data_types, self.profiles
        step = "classifiers"
        if not self.reddit:
            columns = {dt: _names(step, self.get(step, f"text_columns[{dt}]")) for dt in data_types}
            columns = {dt: items for dt, items in columns.items() if items}
            if columns:
                f["text_columns"] = columns
            if step in self.raw:
                f["remove_strings"] = _texts(step, self.get(step, "remove_strings"), 256)
                f["remove_patterns"] = _texts(step, self.get(step, "remove_patterns"), 512)
                # Empty is the terminal's Enter: no strings, and the default pattern.
                for key in ("remove_strings", "remove_patterns"):
                    if not f[key]:
                        del f[key]
            extras = _names(step, self.get(step, "gpu_fields"))
            if extras:
                f["gpu_fields"] = extras
        if "lingua" in profiles:
            for key in ("lingua_workers", "lingua_file_workers", "lingua_batch_size"):
                value = _int(step, self.get(step, key), actions.SOURCE_ADD_INPUTS[key])
                if value is not None:
                    f[key] = value
            if step in self.raw:
                f["lingua_low_accuracy"] = self.get(step, "lingua_low_accuracy") == actions.CHECKED
        if "ml" in profiles:
            ids = _split(self.get(step, "gpu_ids"))
            if ids:
                if not all(re.fullmatch(r"(?:0|[1-9][0-9]{0,2})", item) and int(item) <= 255 for item in ids) or len(set(ids)) != len(ids):
                    raise StepError(step, "int")
                f["gpu_ids"] = [int(item) for item in ids]
            for key in ("ml_file_workers", "ml_tokenize_workers", "ml_classifier_batch_size"):
                value = _int(step, self.get(step, key), actions.SOURCE_ADD_INPUTS[key])
                if value is not None:
                    f[key] = value
            names = _names(step, self.get(step, "ml_classifiers"))
            if names:
                f["ml_classifiers"] = names
                if len(data_types) > 1:
                    scopes = {}
                    for name in names:
                        chosen = _names(step, self.get(step, f"scope[{name}]"))
                        if any(dt not in data_types for dt in chosen):
                            raise StepError(step, "names")
                        if chosen:
                            scopes[name] = chosen
                    if scopes:
                        f["ml_classifier_data_types"] = scopes
            token = self.get(step, "hf_token", None)
            if isinstance(token, actions.Hidden):
                f["hf_token"] = token

    # ── databases ──

    def _databases(self):
        f, data_types, profiles = self.fields, self.data_types, self.profiles
        step = "databases"
        # On a reconfigure an empty name is the current one (the terminal's Enter), not the source's name.
        keep = "" if self.configure else None
        visited = step in self.raw
        has_postgres = any(p.startswith("postgres") for p in profiles)
        has_mongo = "mongo_ingest" in profiles
        has_starrocks = "sr_ingest" in profiles
        if "postgres_ingest" in profiles:
            if visited:
                f["pg_prefer_lingua"] = self.get(step, "pg_prefer_lingua") == actions.CHECKED
            value = _int(step, self.get(step, "pg_parallel_index_workers"), actions.SOURCE_ADD_INPUTS["pg_parallel_index_workers"])
            if value is not None:
                f["pg_parallel_index_workers"] = value
        if has_postgres:
            spaces = {}
            for dt in data_types:
                chosen = self.get(step, f"tablespace[{dt}]").strip()
                if chosen:
                    if not actions.is_plain_name(chosen):
                        raise StepError(step, "names")
                    spaces[dt] = chosen
            if spaces:
                f["table_tablespaces"] = spaces
        if self.reddit:
            schema = self.get(step, "reddit_db_schema").strip()
            if schema:
                if not actions.is_plain_name(schema):
                    raise StepError(step, "names")
                f["reddit_db_schema"] = schema
            indexes = {kind: _names(step, self.get(step, f"reddit_index[{kind}]")) for kind in REDDIT_KINDS}
            indexes = {kind: items for kind, items in indexes.items() if items}
            if indexes:
                f["reddit_indexes"] = indexes
        else:
            name = f["name"]
            if has_postgres:
                schema = self.get(step, "db_schema", name if keep is None else keep).strip()
                if schema:
                    if not actions.is_plain_name(schema):
                        raise StepError(step, "names")
                    f["db_schema"] = schema
            if has_mongo:
                strategy = self.get(step, "mongo_collection_strategy", "per_data_type" if keep is None else "")
                if strategy in ("per_data_type", "per_file"):
                    f["mongo_collection_strategy"] = strategy
                elif keep is None:
                    f["mongo_collection_strategy"] = "per_data_type"
                if f.get("mongo_collection_strategy", self.state.get("current", {}).get("mongo_collection_strategy", "per_data_type")) == "per_data_type":
                    collections = {}
                    for dt in data_types:
                        chosen = self.get(step, f"collection[{dt}]", dt if keep is None else keep).strip()
                        if chosen:
                            if not actions.is_plain_name(chosen):
                                raise StepError(step, "names")
                            collections[dt] = chosen
                    if collections:
                        f["mongo_collections"] = collections
                db_name = self.get(step, "mongo_db_name", name if keep is None else keep).strip()
                if db_name:
                    if not actions.is_plain_name(db_name):
                        raise StepError(step, "names")
                    f["mongo_db_name"] = db_name
            for key, asked in (("pg_indexes", has_postgres), ("mongo_indexes", has_mongo), ("sr_indexes", has_starrocks)):
                if not asked:
                    continue
                chosen = {dt: _names(step, self.get(step, f"{key}[{dt}]")) for dt in data_types}
                chosen = {dt: items for dt, items in chosen.items() if items}
                if chosen:
                    f[key] = chosen
            if has_postgres or has_starrocks:
                key = self.get(step, "primary_key").strip()
                if key:
                    if not actions.is_plain_name(key):
                        raise StepError(step, "names")
                    f["primary_key"] = key
        if has_starrocks:
            if visited:
                f["sr_prefer_lingua"] = self.get(step, "sr_prefer_lingua") == actions.CHECKED
            value = _int(step, self.get(step, "sr_buckets"), actions.SOURCE_ADD_INPUTS["sr_buckets"])
            if value is not None:
                f["sr_buckets"] = value


def needed_steps(answers):
    """The steps a wizard goes through for its answers so far: Add classifiers only Classifiers and Review; Reconfigure no Fields for a Hugging Face source (Origin is fixed, shown as done)."""
    if answers.mode == "classifiers":
        return ["classifiers", "review"]
    needed = ["origin", "files"] if answers.configure and answers.hf else ["origin", "files", "fields"]
    if answers.has_classifiers:
        needed.append("classifiers")
    if answers.has_databases:
        needed.append("databases")
    return needed + ["review"]


def try_answers(state, upto):
    """(Answers, the StepError or None): the answers read up to a step; on an error, what was read before it."""
    answers = Answers(state, upto)
    try:
        answers.build()
    except StepError as error:
        return answers, error
    return answers, None


# The pending action each mode of the wizard makes.
MODE_ACTIONS = {"add": "source.add", "configure": "source.configure", "classifiers": "source.add_classifiers"}


def final_fields(state):
    """The fields of the wizard's action (`MODE_ACTIONS`) for a wizard whose every step was answered, checked by the table. Raises StepError."""
    answers, error = try_answers(state, "review")
    if error is not None:
        raise error
    fields = answers.fields
    try:
        actions.check_fields(actions.TABLE[MODE_ACTIONS[state.get("mode", "add")]], fields)
    except actions.Refused:
        raise StepError("review", "names") from None
    if len(json.dumps(actions.revealed(fields))) > REQUEST_MAX_BYTES:
        raise StepError("review", "too_large")
    return fields


def defaults_left(fields):
    """The questions answered with Enter (left empty) whose default the action computes on the host, for these fields."""
    profiles, left = fields["profiles"], []

    def need(key, label, when=True):
        if when and key not in fields:
            left.append(label)

    need("parse_workers", "parse workers (the CPU cores, at most 8)", "parse" in profiles)
    need("pg_parallel_index_workers", "PostgreSQL index workers (half the CPU cores, 1 to 8)", "postgres_ingest" in profiles)
    need("sr_buckets", "StarRocks buckets (the CPU cores × 8)", "sr_ingest" in profiles)
    for key, label in (("lingua_workers", "Lingua workers (the CPU cores)"), ("lingua_file_workers", "Lingua file workers (an eighth of the CPU cores, at least 1)"), ("lingua_batch_size", "Lingua batch size (the base configuration's)")):
        need(key, label, "lingua" in profiles)
    for key, label in (("gpu_ids", "GPUs (every GPU found, or GPU 0 when none is)"), ("ml_file_workers", "GPU file workers (one per GPU chosen)"), ("ml_tokenize_workers", "tokenize workers (half the CPU cores, 1 to 8)"), ("ml_classifier_batch_size", "classifier batch size (the base configuration's)"), ("ml_classifiers", "classifiers (every one the base configuration defines)")):
        need(key, label, "ml" in profiles)
    return left


def cli_lines(fields_or_state, download=True, mode="add"):
    """The `sdp` commands the wizard stands for, as (words, bold value) pairs, or (words, bold value, words, bold value), for the `.cli` lines."""
    name = fields_or_state.get("name")
    if mode == "configure":
        return [("python sdp.py source configure", name)]
    if mode == "classifiers":
        return [("python sdp.py source add-classifiers", name)]
    if "hf_dataset" in fields_or_state:
        lines = [("python sdp.py source add", name or "<name>", "--hf", fields_or_state["hf_dataset"])]
        if download:
            lines.append(("python sdp.py source download", name or "<name>"))
        return lines
    lines = [("python sdp.py source add", name or "<name>")]
    if fields_or_state.get("platform") == "reddit" and any(key in fields_or_state for key in ("reddit_db_schema", "reddit_removed_fields", "reddit_indexes")):
        lines.append(("python sdp.py source configure", "reddit"))
    return lines


def rail(names):
    """The Sources rail of the wizard's pages: the sources read when the wizard began, each a link to its page when its name is plain."""
    return [{"name": name, "href": f"/sources/{name}" if actions.is_plain_name(name) else None} for name in names]


def install(api, ctx):
    """Add the wizard's routes to `api`, with what `ctx` (an `app.Context`) gives."""
    limits = ctx.limits

    def not_found():
        return PlainTextResponse("Not found.\n", status_code=404)

    def frame(session, status=200, **model):
        # The Origin page and a failed start are Add source's.
        model = {"mode": "add", "current": {}, "verb": "source.add", "source_name": None, **model}
        return ctx.frame("source_add.html", NOT_ACTIVE, session, status, title=title_of(model), steps=STEPS, origins=ORIGINS, **model)

    def stepper(current, needed, mode="add"):
        index = STEP_KEYS.index(current)
        # Add classifiers shows its two steps only, numbered on their own.
        shown = [(key, label) for key, label in STEPS if mode != "classifiers" or key in needed]
        return [{"key": key, "label": f"{number} {label}", "state": "on" if key == current else "skip" if key not in needed else "done" if STEP_KEYS.index(key) < index else ""} for number, (key, label) in enumerate(shown, start=1)]

    def show(session, wizard, step, status=200, line=None):
        state = wizard.fields
        mode = state.get("mode", "add")
        answers, error = try_answers(state, step)
        # Before the profiles are known (Files not answered yet), no step is said to be skipped.
        needed = needed_steps(answers) if "profiles" in answers.fields or mode == "classifiers" else list(STEP_KEYS)
        model = {
            "step_data_types": answers.data_types, "step_profiles": answers.profiles,
            "mode": mode, "source_name": state.get("name"), "current": state.get("current", {}), "verb": MODE_ACTIONS[mode],
            "wizard_id": wizard.id, "step": step, "platform": state["platform"], "stepper": stepper(step, needed, mode), "line": line,
            "raw": state["raw"].get(step, {}), "visited": step in state["raw"], "answers": answers.fields, "offered": offered_profiles(state["databases"]),
            "databases": state["databases"], "reddit_kinds": REDDIT_KINDS, "default_pattern": DEFAULT_REMOVE_PATTERN, "token_kept": isinstance(state["raw"].get("classifiers", {}).get("hf_token"), actions.Hidden),
            "cli": cli_lines(answers.fields, answers.download, mode), "scope_names": _split(state["raw"].get("classifiers", {}).get("ml_classifiers", "")),
            "review": None, "rail": rail(state["existing"]),
            "hf": state.get("hf"), "dataset_token_kept": isinstance(state["raw"].get("files", {}).get("dataset_token"), actions.Hidden), "download": answers.download, "sql_choices": [(name, name) for name in HF_SQL_TYPES],
        }
        if step == "review":
            try:
                fields = final_fields(state)
            except StepError as refused:
                model["line"] = model["line"] or refused.line
                model["review_error_step"] = refused.step
            else:
                action = actions.TABLE[MODE_ACTIONS[mode]]
                if mode == "classifiers":
                    files = [filename for profile, filename in (("lingua", "lingua.yaml"), ("ml", "ml.yaml")) if profile in answers.profiles]
                else:
                    files = actions.source_files(fields["profiles"])
                model["review"] = {
                    "inputs": actions.shown_inputs(action, fields), "files": files, "folders": [(stage, fields[f"{stage}_path"]) for stage in STAGES if f"{stage}_path" in fields],
                    "defaults": defaults_left(fields) if mode == "add" else [], "cli": cli_lines(fields, answers.download, mode),
                }
        elif error is not None and error.step != step and STEP_KEYS.index(error.step) < STEP_KEYS.index(step):
            model["line"] = model["line"] or error.line
        return frame(session, status, **model)

    def current(request):
        session_id, session = ctx.current(request)
        return session_id, session

    async def facts(request, session_id, session):
        """The configured databases and the sources, read in a worker of the pages' bound: (databases, names) or a response."""

        def plan():
            answers = {}
            for key, verb in (("status", "db.status"), ("list", "source.list")):
                answers[key] = call_broker(lambda: ctx.broker(token=session.token, timeout=limits.read_seconds), lambda made, verb=verb: actions.call(made, verb, {}))
                if answers[key][0] == "error" and session_ends(answers[key][1]):
                    break
            return answers

        answers = await ctx.pages.run(plan)
        if answers is BUSY_GATE:
            return ctx.frame("busy.html", NOT_ACTIVE, session, 503, title="Add a source", line="The web UI is busy with other requests: try again in a moment.")
        for kind, value in answers.values():
            if kind == "error" and session_ends(value):
                ctx.sessions.forget(session_id)
                return ctx.to_login(request, why(value), clear_cookie=True)
        blocks = pages.build(answers, {"status": pages.databases_status_block, "list": overview.sources_block})
        if not blocks["status"]["ok"] or not blocks["list"]["ok"]:
            failed = blocks["status"] if not blocks["status"]["ok"] else blocks["list"]
            return frame(session, 502, wizard_id=None, step="origin", platform=None, stepper=stepper("origin", list(STEP_KEYS)), line=None, failure=failed, raw={}, visited=False, cli=[("python sdp.py source add", "<name>")])
        databases = blocks["status"]["configured"]
        names = [row["name"] for row in blocks["list"]["rows"]]
        return databases, names

    async def read_dataset(request, session_id, session, wizard, dataset):
        """Read the dataset's schema from the Hub through the broker (`source.hf_inspect`, with the token typed on Files) when the wizard has none for this dataset or a new token was typed, in a worker of the pages' bound; keep it in the wizard (the Fields step's answers of another dataset are dropped). None when it was read or kept, else the response to send: the Files step with the line of the refusal, or the login page for an ended session."""
        state = wizard.fields
        token = state["raw"]["files"].get("dataset_token")
        kept = state.get("hf")
        if kept is not None and kept["dataset"] == dataset and state.get("hf_token_used") is token:
            return None
        fields = {"dataset": dataset, **({"token": token} if isinstance(token, actions.Hidden) else {})}
        answer = await ctx.pages.run(lambda: call_broker(lambda: ctx.broker(token=session.token, timeout=limits.read_seconds), lambda made: actions.call(made, "source.hf_inspect", fields)))
        if answer is BUSY_GATE:
            return show(session, wizard, "files", 503, "The web UI is busy with other requests: try again in a moment.")
        kind, value = answer
        if kind == "error":
            if session_ends(value):
                ctx.sessions.forget(session_id)
                return ctx.to_login(request, why(value), clear_cookie=True)
            return show(session, wizard, "files", 400, "The dataset could not be read from the Hub: " + _refusal_text(value))
        try:
            schema = checked_schema(value.get("data") if isinstance(value, dict) else None)
            if schema["dataset"] != dataset:
                raise StepError("files", "hf_shape")
        except StepError as refused:
            return show(session, wizard, "files", 400, refused.line)
        if kept is None or kept["dataset"] != dataset:
            state["raw"].pop("fields", None)
        state["hf"] = schema
        # Which token read it (the object, never its text): a new token typed reads the dataset again.
        state["hf_token_used"] = token
        session.work.wizard(wizard.id, ctx.clock(), touch=True, weight=_weight(state))
        return None

    @api.get("/add-source")
    async def origin_page(request: Request):
        session_id, session = current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))
        return frame(session, wizard_id=None, step="origin", platform=None, stepper=stepper("origin", list(STEP_KEYS)), line=None, raw={}, visited=False, cli=[("python sdp.py source add", "<name>")])

    @api.post("/add-source")
    async def start(request: Request):
        session_id, session = current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))
        form = dict(request.scope.get("sdp.form", {}))
        platform = form.get("platform")
        if set(form) != {"platform"} or platform not in {key for key, _, _ in ORIGINS}:
            return frame(session, 400, wizard_id=None, step="origin", platform=None, stepper=stepper("origin", list(STEP_KEYS)), line="Choose where the data comes from.", raw={}, visited=False, cli=[("python sdp.py source add", "<name>")])
        made = await facts(request, session_id, session)
        if not isinstance(made, tuple):
            return made
        databases, names = made
        state = {"platform": platform, "raw": {}, "databases": databases, "existing": names}
        wizard = session.work.add_pending("source.add", state, ctx.clock(), weight=_weight(state), wizard=True)
        return RedirectResponse(f"/add-source/{wizard.id}/files", status_code=303)

    def wizard_of(request, wizard_id, touch=False, weight=None):
        session_id, session = current(request)
        if session is None:
            return session_id, None, None
        return session_id, session, session.work.wizard(wizard_id, ctx.clock(), touch=touch, weight=weight)

    @api.get("/add-source/{wizard_id}/{step}")
    async def step_page(request: Request, wizard_id: str, step: str):
        if not re.fullmatch(ID_PATTERN, wizard_id) or step not in STEP_KEYS:
            return not_found()
        session_id, session, wizard = wizard_of(request, wizard_id)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))
        if wizard is None:
            return ctx.frame("refused.html", NOT_ACTIVE, session, 404, title="Not kept", line=GONE, field=None, back=("/add-source", "Add source"))
        if step not in steps_of(wizard.fields):
            return not_found()
        return show(session, wizard, step)

    @api.post("/add-source/{wizard_id}/cancel")
    async def cancel(request: Request, wizard_id: str):
        if not re.fullmatch(ID_PATTERN, wizard_id):
            return not_found()
        session_id, session = current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))
        wizard = session.work.wizard(wizard_id, ctx.clock())
        name = wizard.fields.get("name") if wizard is not None and wizard.fields.get("mode", "add") != "add" else None
        session.work.drop_wizard(wizard_id)
        return RedirectResponse(f"/sources/{name}" if name and actions.is_plain_name(name) else "/sources", status_code=303)

    @api.post("/add-source/{wizard_id}/{step}")
    async def post_step(request: Request, wizard_id: str, step: str):
        if not re.fullmatch(ID_PATTERN, wizard_id) or step not in STEP_KEYS:
            return not_found()
        session_id, session, wizard = wizard_of(request, wizard_id)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))
        if wizard is None:
            return ctx.frame("refused.html", NOT_ACTIVE, session, 404, title="Not kept", line=GONE, field=None, back=("/add-source", "Add source"))
        state = wizard.fields
        if step not in steps_of(state):
            return not_found()
        form = dict(request.scope.get("sdp.form", {}))
        nav = form.pop("nav", None)
        if nav not in ("back", "next", "confirm") or (nav == "confirm") != (step == "review"):
            return show(session, wizard, step, 400, "The step was posted without Back, Next or Confirm.")
        if step == "origin":
            platform = form.get("platform", state["platform"])
            if platform not in {key for key, _, _ in ORIGINS}:
                return show(session, wizard, step, 400, "Choose where the data comes from.")
            if platform != state["platform"]:
                state["platform"] = platform
                state["raw"] = {}
                state.pop("hf", None)
                state.pop("hf_token_used", None)
        elif step == "review":
            # Download now (a Hugging Face dataset's): ticked, or posted without it.
            state["raw"]["review"] = {"download": form["download"]} if form.get("download") == actions.CHECKED else {}
        else:
            kept = dict(form)
            if step == "files" and state["platform"] == "hf":
                previous = state["raw"].get("files", {}).get("dataset_token")
                token = kept.pop("dataset_token", "")
                if kept.pop("dataset_token_forget", None) == actions.CHECKED:
                    previous = None
                if token:
                    if token != token.strip() or actions.has_control_character(token) or len(token) > actions.SECRET_MAX_CHARS:
                        return show(session, wizard, step, 400, LINES["token"])
                    previous = actions.Hidden(token)
                if previous is not None:
                    kept["dataset_token"] = previous
            if step == "classifiers":
                previous = state["raw"].get("classifiers", {}).get("hf_token")
                token = kept.pop("hf_token", "")
                if kept.pop("hf_token_forget", None) == actions.CHECKED:
                    previous = None
                if token:
                    if token != token.strip() or actions.has_control_character(token) or len(token) > actions.SECRET_MAX_CHARS:
                        return show(session, wizard, step, 400, LINES["token"])
                    previous = actions.Hidden(token)
                if previous is not None:
                    kept["hf_token"] = previous
            old_names = _split(state["raw"].get("classifiers", {}).get("ml_classifiers", "")) if step == "classifiers" else None
            state["raw"][step] = kept
            if step == "classifiers" and nav == "next":
                answers, _ = try_answers(state, "files")
                new_names = _split(kept.get("ml_classifiers", ""))
                if "ml" in answers.profiles and new_names and new_names != old_names and len(answers.data_types) > 1:
                    session.work.wizard(wizard_id, ctx.clock(), touch=True, weight=_weight(state))
                    return show(session, wizard, step, 200, LINES["scope"])
        session.work.wizard(wizard_id, ctx.clock(), touch=True, weight=_weight(state))
        answers, error = try_answers(state, step)
        if step == "files" and nav == "next" and state["platform"] == "hf" and error is None:
            read = await read_dataset(request, session_id, session, wizard, answers.fields["hf_dataset"])
            if read is not None:
                return read
            answers, error = try_answers(state, step)
        needed = [key for key in needed_steps(answers) if key in steps_of(state)]
        if nav == "back":
            earlier = [key for key in needed if STEP_KEYS.index(key) < STEP_KEYS.index(step)]
            return RedirectResponse(f"/add-source/{wizard_id}/{earlier[-1] if earlier else needed[0]}", status_code=303)
        if nav == "next":
            if error is not None:
                if error.step == step:
                    return show(session, wizard, step, 400, error.line)
                return RedirectResponse(f"/add-source/{wizard_id}/{error.step}", status_code=303)
            later = [key for key in needed if STEP_KEYS.index(key) > STEP_KEYS.index(step)]
            return RedirectResponse(f"/add-source/{wizard_id}/{later[0]}", status_code=303)
        try:
            fields = final_fields(state)
        except StepError as refused:
            return show(session, wizard, refused.step if refused.step != "review" else "review", 400, refused.line)
        then = None
        if state.get("mode", "add") == "add" and "hf_dataset" in fields and Answers(state).download:
            # The download follows the add; the dataset token goes with it and is dropped with the pending action once it was sent.
            token = state["raw"].get("files", {}).get("dataset_token")
            download = {"source": fields["name"], **({"token": token} if isinstance(token, actions.Hidden) else {})}
            actions.check_fields(actions.TABLE["source.download"], download)
            then = [("source.download", download)]
        pending = session.work.add_pending(MODE_ACTIONS[state.get("mode", "add")], fields, ctx.clock(), then=then)
        session.work.drop_wizard(wizard_id)
        return RedirectResponse(f"/pending/{pending.id}", status_code=303)


def steps_of(state):
    """The steps a wizard of this mode may show: every one for Add source; all but Origin for Reconfigure (the origin is the source's); Classifiers and Review for Add classifiers."""
    mode = state.get("mode", "add")
    if mode == "classifiers":
        return ("classifiers", "review")
    if mode == "configure":
        return STEP_KEYS[1:]
    return STEP_KEYS


def title_of(model):
    """The page title of a wizard's step."""
    mode, name = model.get("mode", "add"), model.get("source_name")
    if mode == "configure":
        return f"Reconfigure {name}"
    if mode == "classifiers":
        return f"Add classifiers to {name}"
    return "Add a source"


def _refusal_text(error):
    """The line of a refused read: the action's own line when the broker sent one (its first line, without the `Error: ` the child prints, at most 300 characters, control characters dropped), else the broker's sentence."""
    details = getattr(error, "details", None) or {}
    text = details.get("errors") if isinstance(details, dict) else None
    if isinstance(text, str) and text.strip():
        line = next((part for part in text.splitlines() if part.strip()), "").strip()
        line = line[len("Error: "):] if line.startswith("Error: ") else line
        line = "".join(character for character in line if not actions.has_control_character(character))[:300]
        if line:
            return line[0].upper() + line[1:] + ("" if line.endswith(".") else ".")
    return words.sentence(error)


def _weight(state):
    """The bytes a wizard's answers hold, counted as the pending actions' records are."""
    return len(json.dumps(actions.revealed(state), default=str))
