"""Reconfigure… and Add classifiers… on a source's page (`sdp source configure <name>`, `sdp source add-classifiers <name>`): the Add source wizard (`setup_source`) in another mode, prefilled from the source's current answers. `install(api, ctx)` adds the two routes that start one; `app.create_app` calls it.

- `POST /sources/<name>/reconfigure` and `POST /sources/<name>/add-classifiers` (a plain name, else not found; the form carries nothing but its token) read the source's current answers (`source.settings`), the configured databases (`db.status`) and the sources (`source.list`) in a worker of the pages' bound, keep a wizard in the session with every step's texts made from those answers (`prefill`), and go to its first step: Files for a reconfigure (the origin is the source's), Classifiers for Add classifiers. From there the wizard's own routes serve it, and its confirm makes the pending action of `source.configure` or `source.add_classifiers`.
- The read's answer is checked before it is kept (`checked_settings`): its answers must pass the table's check of `source.configure`, its other parts must have their shapes. A source with neither the profile lingua nor ml gets no Add classifiers wizard (the terminal writes nothing for it): the page says to add one with Reconfigure.
"""

from fastapi import Request
from fastapi.responses import PlainTextResponse, RedirectResponse

from social_data_pipeline.webui import actions, overview, pages, setup_source
from social_data_pipeline.webui.calls import BUSY_GATE, call_broker, session_ends, why

# The address word of each mode, and the step it starts at.
MODES = {"reconfigure": ("configure", "files"), "add-classifiers": ("classifiers", "classifiers")}
LEFT_OUT_MAX = 20
LINE_MAX = 300
NO_CLASSIFIER_PROFILE = "This source has neither the profile lingua nor ml, so python sdp.py source add-classifiers would write nothing: add one with Reconfigure first."
NOT_READ = "The source's current answers could not be read, so no wizard was started: "


class NotUsable(ValueError):
    """The read's answer cannot start a wizard; `line` says why, holding no value of it."""

    def __init__(self, line):
        super().__init__(line)
        self.line = line


def _names(value, max_items=64):
    if not isinstance(value, list) or len(value) > max_items or not all(actions.is_plain_name(item) for item in value):
        raise NotUsable("The broker's answer about the source did not have the expected shape.")
    return list(value)


def checked_settings(data, name):
    """The broker's answer of `source.settings` (`setup.source_change.settings_read`) as the wizard keeps it, its shape checked: {"platform" (reddit, hf or custom), "data_types", "profiles", "answers", "typed_sr_indexes", "hf_dataset", "hf_token_set", "left_out"}. Raises NotUsable."""
    shape = NotUsable("The broker's answer about the source did not have the expected shape.")
    if not isinstance(data, dict) or data.get("name") != name:
        raise shape
    if data.get("not_readable") is not None:
        raise NotUsable("its platform.yaml cannot be read. Look at it in a terminal with python sdp.py source settings " + name + ".")
    profiles = _names(data.get("profiles"), len(actions.VALID_PROFILES))
    if not all(profile in actions.VALID_PROFILES for profile in profiles):
        raise shape
    answers = data.get("answers")
    if not isinstance(answers, dict) or "hf_token" in answers or "name" in answers or "profiles" in answers:
        raise shape
    try:
        actions.check_fields(actions.TABLE["source.configure"], {"name": name, "profiles": profiles, **answers})
    except actions.Refused:
        raise shape from None
    hf_dataset = data.get("hf_dataset")
    if hf_dataset is not None and not actions.is_hf_dataset(hf_dataset):
        raise shape
    typed = data.get("typed_sr_indexes")
    if not isinstance(typed, dict) or len(typed) > 32 or not all(actions.is_plain_name(dt) and isinstance(spec, dict) and set(spec) == {"bitmap", "bloomfilter"} for dt, spec in typed.items()):
        raise shape
    typed = {dt: {kind: _names(spec[kind]) for kind in ("bitmap", "bloomfilter")} for dt, spec in typed.items()}
    left_out = data.get("left_out")
    if not isinstance(left_out, list) or len(left_out) > LEFT_OUT_MAX or not all(isinstance(line, str) and len(line) <= LINE_MAX and not actions.has_control_character(line) for line in left_out):
        raise shape
    if not isinstance(data.get("hf_token_set"), bool) or data.get("platform") not in ("reddit", "custom"):
        raise shape
    platform = "reddit" if data["platform"] == "reddit" else "hf" if hf_dataset else "custom"
    if (platform == "reddit") != (name == "reddit"):
        raise shape
    return {
        "platform": platform, "data_types": _names(data.get("data_types"), 32), "profiles": profiles, "answers": answers, "typed_sr_indexes": typed,
        "hf_dataset": hf_dataset, "hf_token_set": data["hf_token_set"], "left_out": list(left_out),
    }


def _text(value):
    """An answer as the wizard's form holds it: a list comma-separated (as the terminal shows its default), a number as digits, a word as it is."""
    if isinstance(value, list):
        return ", ".join(str(item) for item in value)
    return str(value)


def prefill(settings, mode):
    """The wizard's kept texts of every step, made from the current answers so that the steps posted unchanged give exactly these answers back. A step with nothing to fill in is left out (its defaults are then the page's)."""
    answers, profiles, platform = settings["answers"], settings["profiles"], settings["platform"]
    raw = {}

    def put(step, key, answer):
        if answer in answers:
            raw.setdefault(step, {})[key] = _text(answers[answer])

    def put_map(step, prefix, answer, keys=None):
        for key, value in answers.get(answer, {}).items():
            if keys is None or key in keys:
                raw.setdefault(step, {})[f"{prefix}[{key}]"] = _text(value)

    def put_box(step, key, default=False):
        if answers.get(key, default) is True:
            raw.setdefault(step, {})[key] = actions.CHECKED

    if mode == "configure":
        files = raw.setdefault("files", {})
        put("files", "data_types", "data_types")
        for stage in setup_source.STAGES:
            put("files", f"{stage}_path", f"{stage}_path")
        files["file_format"] = answers.get("file_format", "parquet")
        put("files", "parquet_row_group_size", "parquet_row_group_size")
        put("files", "input_format", "input_format")
        put("files", "csv_delimiter", "csv_delimiter")
        for profile in profiles:
            files[f"profiles[{profile}]"] = actions.CHECKED
        put("files", "parse_workers", "parse_workers")
        if platform == "reddit":
            put_map("fields", "removed", "reddit_removed_fields")
        elif platform == "custom":
            put_map("fields", "glob", "dump_globs")
            put_map("fields", "compression", "compressions")
            put_map("fields", "fields", "fields")
        lingua = "lingua" in profiles
        # A box not prefilled is a "no" once the step is kept: one whose profile is new gets the terminal's first default (whether lingua is chosen).
        put_box("databases", "pg_prefer_lingua", lingua)
        put_box("databases", "sr_prefer_lingua", lingua)
        for key in ("pg_parallel_index_workers", "reddit_db_schema", "db_schema", "mongo_collection_strategy", "mongo_db_name", "primary_key", "sr_buckets"):
            put("databases", key, key)
        put_map("databases", "tablespace", "table_tablespaces")
        put_map("databases", "reddit_index", "reddit_indexes")
        put_map("databases", "collection", "mongo_collections")
        for key in ("pg_indexes", "mongo_indexes", "sr_indexes"):
            put_map("databases", key, key)
    for key in ("remove_strings", "remove_patterns", "gpu_fields", "lingua_workers", "lingua_file_workers", "lingua_batch_size", "gpu_ids", "ml_file_workers", "ml_tokenize_workers", "ml_classifier_batch_size", "ml_classifiers"):
        put("classifiers", key, key)
    put_box("classifiers", "lingua_low_accuracy")
    put_map("classifiers", "text_columns", "text_columns")
    put_map("classifiers", "scope", "ml_classifier_data_types")
    return raw


def install(api, ctx):
    """Add the two routes that start a Reconfigure or an Add classifiers wizard."""
    limits = ctx.limits

    async def start(request, name, word):
        mode, first = MODES[word]
        if not actions.is_plain_name(name):
            return PlainTextResponse("Not found.\n", status_code=404)
        session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))
        back = (f"/sources/{name}", "Back to the source")
        if dict(request.scope.get("sdp.form", {})):
            return ctx.frame("refused.html", "sources", session, 400, title="Not taken", line="The form carried a field it does not take.", field=None, back=back)

        def plan():
            answers = {}
            for key, verb, fields in (("settings", "source.settings", {"name": name}), ("status", "db.status", {}), ("list", "source.list", {})):
                answers[key] = call_broker(lambda: ctx.broker(token=session.token, timeout=limits.read_seconds), lambda made, verb=verb, fields=fields: actions.call(made, verb, fields))
                if answers[key][0] == "error":
                    break
            return answers

        answers = await ctx.pages.run(plan)
        if answers is BUSY_GATE:
            return ctx.frame("busy.html", "sources", session, 503, title="Busy", line="The web UI is busy with other requests: try again in a moment.")
        for kind, value in answers.values():
            if kind == "error":
                if session_ends(value):
                    ctx.sessions.forget(session_id)
                    return ctx.to_login(request, why(value), clear_cookie=True)
                return ctx.frame("refused.html", "sources", session, 502, title="Not read", line=NOT_READ + setup_source._refusal_text(value), field=None, back=back)
        blocks = pages.build({key: answers[key] for key in ("status", "list")}, {"status": pages.databases_status_block, "list": overview.sources_block})
        try:
            if not blocks["status"]["ok"] or not blocks["list"]["ok"]:
                raise NotUsable("The broker's answer about the databases or the sources did not have the expected shape.")
            settings = checked_settings(answers["settings"][1].get("data") if isinstance(answers["settings"][1], dict) else None, name)
        except NotUsable as refused:
            return ctx.frame("refused.html", "sources", session, 502, title="Not read", line=NOT_READ + refused.line, field=None, back=back)
        if mode == "classifiers" and "lingua" not in settings["profiles"] and "ml" not in settings["profiles"]:
            return ctx.frame("refused.html", "sources", session, 400, title="Nothing to configure", line=NO_CLASSIFIER_PROFILE, field=None, back=back)
        current = {key: settings[key] for key in ("data_types", "profiles", "typed_sr_indexes", "hf_dataset", "hf_token_set", "left_out")}
        current["mongo_collection_strategy"] = settings["answers"].get("mongo_collection_strategy", "per_data_type")
        state = {
            "mode": mode, "name": name, "platform": settings["platform"], "raw": prefill(settings, mode),
            "databases": blocks["status"]["configured"], "existing": [row["name"] for row in blocks["list"]["rows"]], "current": current,
        }
        wizard = session.work.add_pending(setup_source.MODE_ACTIONS[mode], state, ctx.clock(), weight=setup_source._weight(state), wizard=True)
        if wizard is None:
            return ctx.frame("refused.html", "sources", session, 400, title="Not kept", line="The source's answers are too large to keep in this session: use the terminal.", field=None, back=back)
        return RedirectResponse(f"/add-source/{wizard.id}/{first}", status_code=303)

    @api.post("/sources/{name}/reconfigure")
    async def start_reconfigure(request: Request, name: str):
        return await start(request, name, "reconfigure")

    @api.post("/sources/{name}/add-classifiers")
    async def start_add_classifiers(request: Request, name: str):
        return await start(request, name, "add-classifiers")
