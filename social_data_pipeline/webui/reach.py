"""The Chat window's "Data in reach" (`GET /chat/reach`): per database, the sources whose data the chat's assistant can query, one per line with its size on disk, e.g. "StarRocks" over "reddit (1.4 GB)" and "MongoDB" over "voat (12.3 MB)" and "4chan (≥ 2.0 TB)".

A database is in reach only when all of these hold: the chat's runner says the gateway attaches it (`hello`'s `databases`, fixed when `sdp chat start` ran), the broker's `db.status` says it runs, and its MCP server is set up, runs and is read-only (`db.status`'s `mcp` entry: PostgreSQL's access mode `restricted`, MongoDB's read-only setting, StarRocks's read-only user). A source is listed under it when that database holds data loaded from the source: the files the source's database profiles loaded there (`source.status`, read per source as the Overview's live figures read it, `pages.load_figure`), more than none. Each source listed is shown with its size on disk (`source.size`, the bytes of its dumps, extracted, parsed and output folders, `overview.size_figure`), asked once however many databases list it, and without a size when that read failed. Each database attached but out of reach is one line that says why (stopped, MCP server down, no data loaded); a database set up but not attached says that.

`read_reach` runs in a worker of its own bound, never on the event loop; the Chat window keeps its model for the login and draws it at once on later pages, asking again in the background once it is older than `chat.REACH_FRESH_SECONDS` (a whole-block failure is not kept). It asks the broker first whether it still knows the session (`queue.broker_session_error`): a session it has ended ends the web session; a broker that does not answer is the block's one line, and nothing else is asked. Then the runner's `hello`, `db.status`, `source.list` and, only when a database is in reach and only for the sources with one of its profiles, `source.status` per source, followed by `source.size` for each source that has data in a database in reach: at most `overview.LIVE_SOURCES_MAX` sources and `overview.LIVE_TOTAL_SECONDS` in all, each call with the time limit `overview.LIVE_SECONDS`. Every failure is confined to the block (a line, or a note under the databases). Names come from `source.list` and are shown only when they are plain names (`overview.link`), cleaned and escaped by the template. Standard library and this package only.
"""

import time

from social_data_pipeline.webui import words
from social_data_pipeline.webui.calls import SESSION_ENDING_CODES
from social_data_pipeline.webui.overview import DATABASE_TITLES, LIVE_SECONDS, LIVE_SOURCES_MAX, LIVE_TOTAL_SECONDS, SHAPE_SENTENCE, size_figure, sources_block, text

# The time limit of each call of the block: the Overview's live figures' own.
CALL_SECONDS = LIVE_SECONDS
# The databases in the order the block lists them, and the profiles that load each one (keys of `pages.LOAD_VIEWS`).
ORDER = ("starrocks", "mongo", "postgres")
PROFILES = {"starrocks": ("sr_ingest", "sr_ml"), "mongo": ("mongo_ingest",), "postgres": ("postgres_ingest", "postgres_ml")}
STOPPED = "stopped"
NOT_SET_UP = "not set up"
MCP_NOT_SET_UP = "no MCP server set up"
MCP_DOWN = "MCP server down"
MCP_WRITES = "MCP server not read-only"
NO_DATA = "no data loaded"
NOT_ATTACHED = "not attached to the chat (start the chat again once its MCP server is up)"
LOADS_NOT_READ = "its loads could not be read"


class ReachSessionEnded:
    """What `read_reach` returns when the broker has ended the session: the broker's error."""

    def __init__(self, error):
        self.error = error


def _ends(kind, value):
    return kind == "error" and getattr(value, "code", None) in SESSION_ENDING_CODES


def status_facts(result):
    """{database: {"running", "mcp": None or {"running", "read_only"}}} of a `db.status` answer; raises ValueError for another shape."""
    data = result.get("data") if isinstance(result, dict) else None
    if not isinstance(data, dict) or not isinstance(data.get("configured"), list):
        raise ValueError("db.status")
    facts = {}
    for entry in data.get("databases") or []:
        if not isinstance(entry, dict) or not isinstance(entry.get("db"), str) or not isinstance(entry.get("running"), bool):
            raise ValueError("db.status")
        facts[entry["db"]] = {"running": entry["running"], "mcp": None}
    for entry in data.get("mcp") or []:
        if not isinstance(entry, dict) or not isinstance(entry.get("db"), str) or not isinstance(entry.get("running"), bool) or not isinstance(entry.get("read_only"), bool):
            raise ValueError("db.status")
        if entry["db"] in facts:
            facts[entry["db"]]["mcp"] = {"running": entry["running"], "read_only": entry["read_only"]}
    return facts


def why_out(fact):
    """Why a database attached to the chat is out of reach by its `status_facts` entry (None: not set up), or None when it is in reach."""
    if fact is None:
        return NOT_SET_UP
    if not fact["running"]:
        return STOPPED
    if fact["mcp"] is None:
        return MCP_NOT_SET_UP
    if not fact["mcp"]["running"]:
        return MCP_DOWN
    if not fact["mcp"]["read_only"]:
        return MCP_WRITES
    return None


def loaded_into(status, database):
    """Whether the `source_status_block` `status` says data of the source was loaded into `database`: more than no file loaded by one of its profiles."""
    from social_data_pipeline.webui import pages  # here, not at the top: `pages` imports `overview`, which this module imports

    for profile in PROFILES[database]:
        figure = pages.load_figure(status, profile)
        if figure is not None and isinstance(figure["loaded"], int) and figure["loaded"] > 0:
            return True
    return False


def _title(name):
    return DATABASE_TITLES.get(name, text(name))


def _ordered(names):
    return sorted(names, key=lambda name: (ORDER.index(name) if name in ORDER else len(ORDER), name))


def read_reach(session_error, hello, ask, clock=time.monotonic):
    """The block's model: {"line": None or the one sentence that replaces the block (the broker, the runner or `db.status` did not answer), "rows": one per database, {"title", "ok": True, "sources": [{"name", "size": None or {"text", "title"}}]} for one in reach with data, else {"title", "ok": False, "text": why not}, "notes": [sentences under them]}, or `ReachSessionEnded`. Per source it asks `source.status` and, when the source has data in a database in reach, `source.size`, each only while the time of all the calls lasts; a size that could not be read leaves the source shown without one, with a note. `session_error()` is the broker's check of the session, `hello()` the runner's `hello` as ("ok", answer) or ("error", error), `ask(verb, fields)` a read of the broker with the live time limit."""
    from social_data_pipeline.webui import pages  # here, not at the top: `pages` imports `overview`, which this module imports

    model = {"line": None, "rows": [], "notes": []}
    error = session_error()
    if error is not None:
        if getattr(error, "code", None) in SESSION_ENDING_CODES:
            return ReachSessionEnded(error)
        model["line"] = f"Not known: the broker did not answer: {words.sentence(error)}"
        return model

    kind, value = hello()
    if kind == "error":
        model["line"] = f"Not known: the sandbox did not answer: {words.chat_sentence(value)}"
        return model
    attached = [name for name in value["databases"] if name in PROFILES]

    kind, value = ask("db.status", {})
    if _ends(kind, value):
        return ReachSessionEnded(value)
    if kind == "error":
        model["line"] = f"Not known: the databases' state could not be read: {words.sentence(value)}"
        return model
    try:
        facts = status_facts(value)
    except ValueError:
        model["line"] = f"Not known: {SHAPE_SENTENCE}"
        return model

    reasons = {name: why_out(facts.get(name)) for name in attached}
    reachable = [name for name in _ordered(attached) if reasons[name] is None]
    found = {name: [] for name in reachable}
    unread = set()
    if reachable:
        kind, value = ask("source.list", {})
        if _ends(kind, value):
            return ReachSessionEnded(value)
        listed = None
        if kind == "error":
            model["notes"].append(f"The sources could not be read: {words.sentence(value)}")
        else:
            try:
                listed = [row for row in sources_block(value)["rows"] if row["href"]]
            except Exception:
                model["notes"].append(f"The sources could not be read: {SHAPE_SENTENCE}")
        if listed is None:
            unread.update(reachable)
            listed = []
        wanted = {name for database in reachable for name in PROFILES[database]}
        listed = [row for row in listed if wanted & set(row["profiles"])]
        if len(listed) > LIVE_SOURCES_MAX:
            model["notes"].append(f"Only the first {LIVE_SOURCES_MAX} of {len(listed)} sources were read.")
        deadline = clock() + LIVE_TOTAL_SECONDS
        failed = late = 0
        sizes_failed = sizes_late = 0
        for row in listed[:LIVE_SOURCES_MAX]:
            databases = [name for name in reachable if set(PROFILES[name]) & set(row["profiles"])]
            if clock() >= deadline:
                late += 1
                unread.update(databases)
                continue
            kind, value = ask("source.status", {"name": row["name"]})
            if _ends(kind, value):
                return ReachSessionEnded(value)
            try:
                status = pages.source_status_block(value) if kind == "ok" else None
            except Exception:
                status = None
            if status is None or not status["found"]:
                failed += 1
                unread.update(databases)
                continue
            shown = {"name": text(row["name"]), "size": None}
            under = [name for name in databases if loaded_into(status, name)]
            for name in under:
                found[name].append(shown)
            if not under:
                continue
            # The size of a source listed under any database: asked once, the same entry under each.
            if clock() >= deadline:
                sizes_late += 1
                continue
            kind, value = ask("source.size", {"name": row["name"]})
            if _ends(kind, value):
                return ReachSessionEnded(value)
            try:
                shown["size"] = size_figure(value) if kind == "ok" else None
            except Exception:
                shown["size"] = None
            if shown["size"] is None:
                sizes_failed += 1
        if failed:
            model["notes"].append(f"The loads of {failed} source{'' if failed == 1 else 's'} could not be read.")
        if late:
            model["notes"].append(f"The loads of {late} source{'' if late == 1 else 's'} were not read: the time for them ran out.")
        if sizes_failed:
            model["notes"].append(f"The size{'' if sizes_failed == 1 else 's'} of {sizes_failed} source{'' if sizes_failed == 1 else 's'} could not be read.")
        if sizes_late:
            model["notes"].append(f"The size{'' if sizes_late == 1 else 's'} of {sizes_late} source{'' if sizes_late == 1 else 's'} {'was' if sizes_late == 1 else 'were'} not read: the time for {'it' if sizes_late == 1 else 'them'} ran out.")

    for name in _ordered(attached):
        if reasons[name] is not None:
            model["rows"].append({"title": _title(name), "ok": False, "text": reasons[name]})
        elif found[name]:
            model["rows"].append({"title": _title(name), "ok": True, "sources": found[name]})
        else:
            model["rows"].append({"title": _title(name), "ok": False, "text": LOADS_NOT_READ if name in unread else NO_DATA})
    for name in _ordered(set(facts) - set(attached)):
        model["rows"].append({"title": _title(name), "ok": False, "text": NOT_ATTACHED})
    return model
