"""Databases → Create indexes…: the web UI's form of `sdp db create-indexes --source <name>`, in two steps and a confirm page.

1. `GET /indexes` (`?source=<name>&database=<database>` preset): the source (from `source.list`), then one of the databases it loads into (by its profiles, as the command offers them, the first chosen); with several sources and none given, the source is asked first and the page comes back with its databases.
2. `GET /indexes?source=<name>&database=<database>&step=columns`: `db.index_targets` reads the source's tables (for MongoDB its data types) from the running database, and the page shows each with its existing indexes and one box per real column; for StarRocks a second box per column picks BITMAP over the bloom filter, with the terminal's explanation, a column whose type takes neither shown without a box and one that takes no bloom filter marked BITMAP only. The broker uses the database admin password kept for the session, as for `db.start`; without one, a database with authentication on answers that it needs it.
3. `POST /indexes` with the ticked boxes: the tables are read again, a box naming no column of them is refused, the request is checked against the table (`actions.check_fields`) and kept as a pending action of the session with the statements it will run worked out here (`preview`), then the usual confirm page (`GET /pending/<id>`) with the command, the statements, the run sentence and the management password. Nothing is sent to change anything before that confirm; the confirmed action starts as a recorded run (a task) and its outcome links to the run's page.

A box is named `c<n>` (an index) or `b<n>` (BITMAP), numbered in the page's order, its value `<table>:<column>`: a box not ticked posts nothing, so the form carries only what was picked and stays under the guard's 64 fields (at most `PICK_MAX` boxes).

The routes answer GET for the steps (they change nothing) and POST for the pick; FastAPI is imported in `install` only, so that the pure parts (`picker_model`, `fields_from_form`, `preview`) are tested without it.
"""

from social_data_pipeline.webui import actions, overview, pages
from social_data_pipeline.webui.calls import BUSY_GATE, call_broker, session_ends, why

DATABASE_LABELS = {"postgres": "PostgreSQL", "mongo": "MongoDB", "starrocks": "StarRocks"}
# The profiles that make `db create-indexes` offer a database (`sdp.py` `_index_databases_of`).
DATABASE_PROFILES = {"postgres": ("postgres_ingest", "postgres_ml"), "mongo": ("mongo_ingest",), "starrocks": ("sr_ingest", "sr_ml")}
# How many names of the tables and columns left out the columns step shows (the broker sends at most as many, `sdp.INDEX_SKIPPED_NAMES_MAX`).
SKIPPED_NAMES_SHOWN = 20
# The boxes one pick may tick: the guard takes at most 64 fields, and the form has the source, the database, the save box and the form token besides.
PICK_MAX = 60
# The terminal's explanation of the StarRocks index types (`sdp.py` `_interactive_sr_indexes`).
SR_EXPLANATION = (
    "Indexes default to BLOOM FILTER: small on disk, accelerates = and IN. Choose BITMAP for columns you filter with ranges (>, <, >=, <=) or IS NULL. "
    "BITMAP is adaptive — StarRocks skips it at query time when it cannot filter out ~999/1000 of rows, so it is wasted on very low-cardinality columns. Neither type accelerates LIKE."
)
BUSY = "The web UI is busy with other requests: try again in a moment."
PICK_REFUSED = "The index build was not kept: a box of the form was refused, or it named a column the database does not have now. Nothing was sent to change anything."
NOTHING_PICKED = "No column was ticked: tick the columns to index."
TOO_MANY = f"At most {PICK_MAX} boxes can be ticked in one build: build the rest in a second one."
NOT_KEPT = "The index build is too large to keep for the confirm page."


def databases_of(profiles):
    """The databases a source loads into, by its profiles, in the order of the command."""
    return [db for db in actions.DATABASES if any(profile in profiles for profile in DATABASE_PROFILES[db])]


def targets_block(result):
    """The tables of a `db.index_targets` answer, checked: `database`, `scope`, `parallel_workers`, `truncated`, `skipped_tables`, `skipped_columns`, `skipped_names` (at most SKIPPED_NAMES_SHOWN) and `skipped_more` (how many are not named), `tables` (each `name`, `indexes` or None, `bloom_filter` or None, `collections`, `db_name`, `columns`: `name`, `type`, `bloom`, `bitmap`). Raises ShapeError."""
    data = pages._dict(pages._data(result))
    database = data.get("database")
    pages._need(database in actions.DATABASES)
    tables = []
    for table in pages._list(data.get("tables")):
        pages._dict(table)
        name = pages._string(table.get("name"))
        pages._need(actions.is_plain_name(name))
        columns = []
        for column in pages._list(table.get("columns")):
            pages._dict(column)
            column_name = pages._string(column.get("name"))
            pages._need(actions.is_plain_name(column_name))
            bloom, bitmap = column.get("bloom"), column.get("bitmap")
            pages._need(bloom in (True, False, None) and bitmap in (True, False, None))
            columns.append({"name": column_name, "type": overview.text(pages._string(column.get("type", ""))), "bloom": bloom, "bitmap": bitmap})
        indexes = table.get("indexes")
        bloom_filter = table.get("bloom_filter")
        collections = table.get("collections")
        pages._need(collections is None or (isinstance(collections, int) and not isinstance(collections, bool)))
        db_name = table.get("db_name")
        pages._need(db_name is None or actions.is_plain_name(db_name))
        tables.append({
            "name": name, "columns": columns, "collections": collections, "db_name": db_name,
            "indexes": None if indexes is None else [overview.text(pages._string(item)) for item in pages._list(indexes)],
            "bloom_filter": None if bloom_filter is None else [pages._string(item) for item in pages._list(bloom_filter)],
        })
    workers = data.get("parallel_workers")
    skipped_tables, skipped_columns = data.get("skipped_tables", 0), data.get("skipped_columns", 0)
    pages._need(all(isinstance(count, int) and not isinstance(count, bool) and count >= 0 for count in (skipped_tables, skipped_columns)))
    # The names of the tables and columns left out (a column as `<table>.<column>`), read from the database: at most SKIPPED_NAMES_SHOWN, each made one line of text.
    skipped_names = [overview.text(pages._string(item)) for item in pages._list(data.get("skipped_names", []))[:SKIPPED_NAMES_SHOWN]]
    scope = data.get("scope")
    pages._need(scope is None or actions.is_plain_name(scope))
    return {
        "database": database, "scope": scope, "tables": tables,
        "parallel_workers": workers if isinstance(workers, int) and not isinstance(workers, bool) else None,
        "truncated": data.get("truncated") is True, "skipped_tables": skipped_tables, "skipped_columns": skipped_columns,
        "skipped_names": skipped_names, "skipped_more": max(0, skipped_tables + skipped_columns - len(skipped_names)),
    }


def column_state(database, column):
    """How a column can be indexed: `index` (PostgreSQL, MongoDB), `choosable` (StarRocks: bloom filter or BITMAP), `bitmap_only`, or `none` (StarRocks takes neither for its type), as the terminal decides it."""
    if database != "starrocks":
        return "index"
    if column["bloom"] is False and column["bitmap"] is False:
        return "none"
    if column["bloom"] is False:
        return "bitmap_only"
    return "choosable"


def picker_model(block, picked=None):
    """The column step's tables: each column with its state and its boxes' names and value (`<table>:<column>`), numbered in the page's order; `picked` ({(table, column): "bloom"|"bitmap"}) ticks them again."""
    picked = picked or {}
    number = 0
    tables = []
    for table in block["tables"]:
        columns = []
        for column in table["columns"]:
            state = column_state(block["database"], column)
            choice = picked.get((table["name"], column["name"]))
            columns.append({**column, "state": state, "box": f"c{number}", "bitmap_box": f"b{number}", "value": f"{table['name']}:{column['name']}", "checked": choice is not None, "bitmap_checked": choice == "bitmap"})
            number += 1
        tables.append({**table, "columns": columns})
    return tables


class PickRefused(ValueError):
    """The pick could not be made into a request: `line` says why."""

    def __init__(self, line):
        super().__init__(line)
        self.line = line


def fields_from_form(form, block, source):
    """The fields of `db.create_indexes` from a posted pick, checked against the tables read (`block`): (fields, picked). Raises PickRefused for a box that is not one of the form's, a value naming no column of the tables read, a BITMAP box without its index box or outside StarRocks, nothing ticked, or too many."""
    database = block["database"]
    columns_of = {table["name"]: {column["name"]: column for column in table["columns"]} for table in block["tables"]}
    order = [table["name"] for table in block["tables"]]
    indexes, bitmap = {}, {}
    boxes = 0
    for name, value in form.items():
        if name in ("source", "database", "save"):
            continue
        if len(name) < 2 or name[0] not in "cb" or not name[1:].isdigit() or not isinstance(value, str) or value.count(":") != 1:
            raise PickRefused(PICK_REFUSED)
        boxes += 1
        table, column = value.split(":")
        if table not in columns_of or column not in columns_of[table] or column_state(database, columns_of[table][column]) == "none":
            raise PickRefused(PICK_REFUSED)
        if name[0] == "c":
            indexes.setdefault(table, []).append(column)
        else:
            if column_state(database, columns_of[table][column]) != "choosable":
                raise PickRefused(PICK_REFUSED)
            bitmap.setdefault(table, []).append(column)
    if boxes > PICK_MAX:
        raise PickRefused(TOO_MANY)
    if not indexes:
        raise PickRefused(NOTHING_PICKED)
    if any(column not in indexes.get(table, []) for table, chosen in bitmap.items() for column in chosen):
        raise PickRefused(PICK_REFUSED)
    picked = {}
    request = []
    for table in order:
        if table not in indexes:
            continue
        # The columns in the table's order, as the page lists them.
        chosen = [column for column in columns_of[table] if column in indexes[table]]
        item = {"table": table, "columns": chosen}
        if bitmap.get(table):
            item["bitmap"] = [column for column in chosen if column in bitmap[table]]
        request.append(item)
        for column in chosen:
            picked[(table, column)] = "bitmap" if column in bitmap.get(table, []) else "bloom"
    fields = {"source": source, "database": database, "indexes": request}
    if form.get("save") == actions.CHECKED:
        fields["save"] = True
    elif "save" in form:
        raise PickRefused(PICK_REFUSED)
    try:
        actions.check_fields(actions.TABLE["db.create_indexes"], fields)
    except actions.Refused:
        raise PickRefused(PICK_REFUSED) from None
    return fields, picked


def preview(block, fields):
    """The statements the build will run for `fields` on the tables read (`block`), as `sdp.py` `do_db_create_indexes` builds them: PostgreSQL's CREATE INDEX with its session settings, one createIndex per data type and field on each of its collections for MongoDB, StarRocks's bloom filter ALTER (the columns set now kept: the property is replaced whole) and BITMAP CREATE INDEX, with a line for a column StarRocks cannot index or indexes with BITMAP only. A tuple of lines."""
    database = block["database"]
    tables = {table["name"]: table for table in block["tables"]}
    lines = []
    if database == "postgres":
        workers = block["parallel_workers"] if block["parallel_workers"] is not None else "<parallel_index_workers>"
        for item in fields["indexes"]:
            for column in item["columns"]:
                lines.append(f"SET maintenance_work_mem = '2GB'; SET max_parallel_maintenance_workers = {workers}; CREATE INDEX IF NOT EXISTS idx_{item['table']}_{column} ON {block['scope']}.{item['table']} ({column});")
    elif database == "mongo":
        for item in fields["indexes"]:
            table = tables[item["table"]]
            for column in item["columns"]:
                lines.append(f"use('{table['db_name']}'); on each of its {table['collections']} collections of {item['table']}: createIndex({{'{column}': 1}}, {{name: '{column}_1'}});")
    else:
        scope = block["scope"]
        for item in sorted(fields["indexes"], key=lambda entry: entry["table"]):
            table = tables[item["table"]]
            columns = {column["name"]: column for column in table["columns"]}
            bloom, bitmap = [], []
            for name in item["columns"]:
                state = column_state(database, columns[name])
                if state == "none":
                    lines.append(f"-- skipped {item['table']}.{name}: StarRocks cannot index {columns[name]['type'].upper()} columns with either mechanism")
                elif state == "bitmap_only" or name in item.get("bitmap", ()):
                    if state == "bitmap_only":
                        lines.append(f"-- {item['table']}.{name} is {columns[name]['type'].upper()}: not bloom-filterable, using BITMAP")
                    bitmap.append(name)
                else:
                    bloom.append(name)
            existing = set(table["bloom_filter"] or ())
            new = [name for name in bloom if name not in existing]
            if bloom and not new:
                lines.append(f"-- {item['table']}: bloom filter already covers {', '.join(bloom)}")
            elif new:
                target = sorted(existing | set(new))
                lines.append(f'ALTER TABLE `{scope}`.`{item["table"]}` SET ("bloom_filter_columns" = "{",".join(target)}")')
            for name in bitmap:
                lines.append(f"CREATE INDEX `idx_{item['table']}_{name}` ON `{scope}`.`{item['table']}` (`{name}`) USING BITMAP")
    return tuple(lines)


def install(api, ctx):
    """Add the routes of this module to `api`, with what `ctx` (an `app.Context`) gives."""
    from fastapi import Request
    from fastapi.responses import RedirectResponse

    limits = ctx.limits

    def ask(session, name, fields):
        return call_broker(lambda: ctx.broker(token=session.token, timeout=limits.read_seconds), lambda made: actions.call(made, name, fields))

    def chosen(request):
        source = request.query_params.get("source")
        database = request.query_params.get("database")
        return (source if actions.is_plain_name(source) else None), (database if database in actions.DATABASES else None)

    async def read(request, plan):
        """(session, answers) or a response: the login page for an ended session, the busy page when the pages' bound is full."""
        session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id)), None
        answers = await ctx.pages.run(lambda: plan(session))
        if answers is BUSY_GATE:
            return ctx.frame("busy.html", "databases", session, 503, title="Create indexes", line=BUSY), None
        ending = next((value for kind, value in answers.values() if kind == "error" and session_ends(value)), None)
        if ending is not None:
            ctx.sessions.forget(session_id)
            return ctx.to_login(request, why(ending), clear_cookie=True), None
        return session, answers

    @api.get("/indexes")
    async def indexes_page(request: Request):
        source, database = chosen(request)
        columns_step = request.query_params.get("step") == "columns" and source is not None and database is not None

        def plan(session):
            answers = {"list": ask(session, "source.list", {})}
            if columns_step and not (answers["list"][0] == "error" and session_ends(answers["list"][1])):
                answers["targets"] = ask(session, "db.index_targets", {"source": source, "db": database})
            return answers

        session, answers = await read(request, plan)
        if answers is None:
            return session
        blocks = pages.build(answers, {"list": overview.sources_block, "targets": targets_block})
        sources = []
        for row in blocks["list"]["rows"] if blocks["list"]["ok"] else []:
            if row["href"]:
                used = databases_of(row["profiles"])
                sources.append({"name": row["name"], "databases": used, "database_labels": ", ".join(DATABASE_LABELS[db] for db in used)})
        # The first step offers the databases of one source only, as the command does (its `available` list, in its order), the first chosen: the source given, else the only one; with several and none given, the source is asked first.
        picked = next((row for row in sources if row["name"] == source), sources[0] if len(sources) == 1 else None)
        offered = picked["databases"] if picked else []
        model = {
            "step": "columns" if columns_step else "source", "sources": sources, "sources_block": blocks["list"], "source": source, "database": database,
            "picked": picked, "offered": offered, "preselected": database if database in offered else (offered[0] if offered else None),
            "labels": DATABASE_LABELS, "db_password_kept": session.work.db_password_kept, "pick_max": PICK_MAX, "sr_explanation": SR_EXPLANATION,
            "command": actions.TABLE["db.create_indexes"].command({"source": source}) if source else None,
        }
        if columns_step:
            targets = blocks["targets"]
            model["targets"] = targets
            model["tables"] = picker_model(targets) if targets["ok"] else []
        return ctx.frame("indexes.html", "databases", session, title="Create indexes", **model)

    @api.post("/indexes")
    async def indexes_pick(request: Request):
        session_id, session = ctx.current(request)
        if session is None:
            return ctx.to_login(request, clear_cookie=bool(session_id))
        form = dict(request.scope.get("sdp.form", {}))
        source, database = form.get("source"), form.get("database")
        back = (f"/indexes?source={source}&database={database}&step=columns", "Back to the columns") if actions.is_plain_name(source) and database in actions.DATABASES else ("/indexes", "Back")
        if not actions.is_plain_name(source) or database not in actions.DATABASES:
            return ctx.frame("refused.html", "databases", session, 400, title="Not taken", line=PICK_REFUSED, field=None, back=back)

        session, answers = await read(request, lambda current: {"targets": ask(current, "db.index_targets", {"source": source, "db": database})})
        if answers is None:
            return session
        block = pages.build(answers, {"targets": targets_block})["targets"]
        if not block["ok"]:
            return ctx.frame("refused.html", "databases", session, 400, title="Not taken", line=block["sentence"], field=None, back=back)
        try:
            fields, _picked = fields_from_form(form, block, source)
        except PickRefused as refused:
            return ctx.frame("refused.html", "databases", session, 400, title="Not taken", line=refused.line, field=None, back=back)
        lines = preview(block, fields)
        pending = session.work.add_pending("db.create_indexes", fields, ctx.clock(), shown=lines, weight=sum(len(line.encode("utf-8")) for line in lines))
        if pending is None:
            return ctx.frame("refused.html", "databases", session, 400, title="Not taken", line=NOT_KEPT, field=None, back=back)
        return RedirectResponse(f"/pending/{pending.id}", status_code=303)
