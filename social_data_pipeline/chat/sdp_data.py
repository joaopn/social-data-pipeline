"""Query results for the chat assistant's scripts: `sdp_data.read_query(database, query)` returns the rows of a read-only query through the same MCP server and gateway port the assistant's own tools use.

The sandbox image installs this file as the top-level module `sdp_data` in the interpreter's own site-packages, so `python3 -I` (which ignores `PYTHONPATH`) imports it too. It imports nothing of this package: `DATABASES` mirrors `claude.DATABASES` (a test keeps them equal). The gateway's address comes from `SDP_CHAT_GATEWAY` and the attached databases from `SDP_CHAT_DATABASES`, which both back ends give the assistant's shell. No credential is involved: the gateway adds the database's own, and the server's read-only user decides what may run.

- `starrocks`: `query` is SQL, run with the server's `read_query` tool; the rows come from its structured result (column names and rows), each value as the server's JSON gives it (a DECIMAL as text, a date or time as ISO text). Name tables with their database (`db.table`).
- `postgres`: `query` is SQL, run with postgres-mcp's `execute_sql`, whose answer is Python's text of a list of dicts; it is read without running any code: literals, and a fixed set of value types (a Decimal becomes an int when it is whole, else a float; dates, times, datetimes with their time zone and timedeltas stay Python objects; a UUID becomes text). Any other value type fails the call, naming it.
- `mongo`: `query` is a dict. `{"collection": ..., "database": ..., "pipeline": [...]}` runs the `aggregate` tool; without `pipeline`, `{"collection": ..., "database": ..., "filter": {...}, "projection": {...}, "sort": {...}, "limit": n}` runs the `query` tool (a find; all keys but `collection` optional, and no other key is taken). The documents come as the server's JSON gives them (an ObjectId as hex text, a date as ISO text).

Bounds of one call: at most `max_rows` rows (a find's limit and an aggregation's appended `$limit` ask MongoDB for one more, to tell), at most `max_bytes` read from the gateway in all, and `timeout` seconds for the whole call (MongoDB is also given it as `maxTimeMS`; a SQL query the call gives up on may still run on the server until the server's own limit). Every failure raises `QueryError`, whose message names the database; nothing fails into an empty result.
"""

import ast
import asyncio
import datetime
import decimal
import functools
import ipaddress
import json
import math
import os
import threading
import uuid
import zoneinfo

# Each database: the gateway's port, the MCP transport and the path, as in `chat.claude.DATABASES`.
DATABASES = {"postgres": (8001, "sse", "/sse"), "mongo": (8002, "http", "/mcp"), "starrocks": (8003, "http", "/mcp")}
SQL_TOOLS = {"starrocks": "read_query", "postgres": "execute_sql"}
MONGO_FIND = "query"
MONGO_AGGREGATE = "aggregate"
MONGO_FIND_KEYS = ("collection", "database", "filter", "projection", "sort", "limit")
MONGO_AGGREGATE_KEYS = ("collection", "database", "pipeline")
MAX_ROWS = 1_000_000
MAX_BYTES = 128 * 1024 * 1024
TIMEOUT = 100.0
CONNECT_SECONDS = 30.0
MESSAGE_CUT = 2000

__all__ = ["QueryError", "read_query", "DATABASES", "MAX_ROWS", "MAX_BYTES", "TIMEOUT"]


class QueryError(RuntimeError):
    """A query whose rows could not be read; the message names the database and why."""


def read_query(database, query, *, as_frame=False, max_rows=MAX_ROWS, max_bytes=MAX_BYTES, timeout=TIMEOUT):
    """The rows of `query` on the attached `database` ("starrocks", "postgres" or "mongo"), read through its read-only MCP server and the gateway: a list of dicts (column name to value), or a pandas DataFrame with `as_frame=True`.

    For StarRocks and PostgreSQL `query` is SQL; for MongoDB a dict, `{"collection", "database", "pipeline"}` for an aggregation or `{"collection", "database", "filter", "projection", "sort", "limit"}` for a find. More than `max_rows` rows, more than `max_bytes` bytes read, or more than `timeout` seconds raise QueryError, as does a refused query, a database not attached to this chat, or a server that cannot be reached.
    """
    if not isinstance(database, str) or database not in DATABASES:
        raise QueryError(f"sdp_data: unknown database {database!r}: it is one of {', '.join(DATABASES)}")
    for name, value in (("max_rows", max_rows), ("max_bytes", max_bytes)):
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise QueryError(f"sdp_data: {database}: {name} must be a whole number from 1")
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0:
        raise QueryError(f"sdp_data: {database}: timeout must be a number of seconds above 0")
    url, kind = _address(database)
    tool, arguments = _request(database, query, max_rows, timeout)
    result = _run(lambda: _call(database, url, kind, tool, arguments, max_bytes, timeout))
    columns, rows = _rows(database, result)
    if len(rows) > max_rows:
        raise QueryError(f"sdp_data: {database}: the result has more than {max_rows:,} rows (max_rows): aggregate in the query, or pass a larger max_rows=")
    if as_frame:
        import pandas

        if columns is None:
            return pandas.DataFrame(rows)
        return pandas.DataFrame(rows, columns=columns)
    if columns is None:
        return rows
    return [dict(zip(columns, row)) for row in rows]


# ── where and what ────────────────────────────────────────────────────────


def _address(database):
    """(url, transport) of `database`'s MCP server at the gateway; QueryError when it is not attached to this chat or the gateway's address is not set."""
    attached = [name for name in os.environ.get("SDP_CHAT_DATABASES", "").split(",") if name]
    if database not in attached:
        raise QueryError(f"sdp_data: {database} is not attached to this chat (attached: {', '.join(attached) or 'none'})")
    gateway = os.environ.get("SDP_CHAT_GATEWAY", "")
    try:
        ipaddress.IPv4Address(gateway)
    except ValueError:
        raise QueryError("sdp_data: SDP_CHAT_GATEWAY does not hold the gateway's IPv4 address: run this in the chat's sandbox") from None
    port, kind, path = DATABASES[database]
    return f"http://{gateway}:{port}{path}", kind


def _request(database, query, max_rows, timeout):
    """(tool name, arguments) of the MCP call for `query`."""
    if database in SQL_TOOLS:
        if not isinstance(query, str) or not query.strip():
            raise QueryError(f"sdp_data: {database}: query must be SQL text")
        key = "query" if database == "starrocks" else "sql"
        return SQL_TOOLS[database], {key: query}
    if not isinstance(query, dict):
        raise QueryError(f"sdp_data: {database}: query must be a dict: {{\"collection\": ..., \"database\": ..., \"pipeline\": [...]}} for an aggregation, or {{\"collection\": ..., \"database\": ..., \"filter\": {{...}}}} for a find")
    allowed = MONGO_AGGREGATE_KEYS if "pipeline" in query else MONGO_FIND_KEYS
    unknown = sorted(str(key) for key in query if key not in allowed)
    if unknown:
        raise QueryError(f"sdp_data: {database}: the query has keys it does not take ({', '.join(unknown)}); it takes {', '.join(allowed)}")
    if not isinstance(query.get("collection"), str) or not query["collection"]:
        raise QueryError(f"sdp_data: {database}: the query needs \"collection\", the collection's name")
    if "database" in query and (not isinstance(query["database"], str) or not query["database"]):
        raise QueryError(f"sdp_data: {database}: \"database\" must be the database's name")
    arguments = {key: query[key] for key in ("collection", "database") if key in query}
    arguments["maxTimeMS"] = max(1, int(timeout * 1000))
    if "pipeline" in query:
        if not isinstance(query["pipeline"], list) or not all(isinstance(stage, dict) for stage in query["pipeline"]):
            raise QueryError(f"sdp_data: {database}: \"pipeline\" must be a list of stages (dicts)")
        arguments["pipeline"] = [*query["pipeline"], {"$limit": max_rows + 1}]
        return MONGO_AGGREGATE, arguments
    limit = query.get("limit")
    if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 1):
        raise QueryError(f"sdp_data: {database}: \"limit\" must be a whole number from 1")
    for key in ("filter", "projection", "sort"):
        if key in query:
            if not isinstance(query[key], dict):
                raise QueryError(f"sdp_data: {database}: \"{key}\" must be a dict")
            arguments[key] = query[key]
    # The server's own default limit is 100: one is always given, one above max_rows so that more is told from enough.
    arguments["limit"] = min(limit, max_rows + 1) if limit is not None else max_rows + 1
    return MONGO_FIND, arguments


# ── the call ──────────────────────────────────────────────────────────────


def _run(make):
    """The result of the coroutine `make()`, run on a loop of its own (in a thread of its own when the caller already runs a loop, as a notebook does)."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(make())
    box = {}

    def work():
        try:
            box["value"] = asyncio.run(make())
        except BaseException as error:
            box["error"] = error

    thread = threading.Thread(target=work, daemon=True)
    thread.start()
    thread.join()
    if "error" in box:
        raise box["error"]
    return box["value"]


class _Budget:
    """The bytes the call may still read; past them `cancel` ends the call and the read fails."""

    def __init__(self, limit):
        self.left = limit
        self.exceeded = False
        self.cancel = None

    def take(self, count):
        self.left -= count
        if self.left < 0:
            if not self.exceeded:
                self.exceeded = True
                if self.cancel is not None:
                    self.cancel()
            raise OSError("sdp_data: the answer is larger than max_bytes")


def _client_factory(budget):
    """An httpx client factory (the `mcp` package's) whose every response body counts against `budget`; proxies of the environment are not used."""
    import httpx2

    class Counted(httpx2.AsyncByteStream):
        def __init__(self, inner):
            self.inner = inner

        async def __aiter__(self):
            async for chunk in self.inner:
                budget.take(len(chunk))
                yield chunk

        async def aclose(self):
            await self.inner.aclose()

    class Counting(httpx2.AsyncBaseTransport):
        def __init__(self):
            self.inner = httpx2.AsyncHTTPTransport(trust_env=False)

        async def handle_async_request(self, request):
            response = await self.inner.handle_async_request(request)
            return httpx2.Response(response.status_code, headers=response.headers, stream=Counted(response.stream), extensions=response.extensions)

        async def aclose(self):
            await self.inner.aclose()

    def factory(headers=None, timeout=None, auth=None):
        return httpx2.AsyncClient(headers=headers, timeout=timeout, auth=auth, transport=Counting(), trust_env=False)

    return factory


def _leaf(error):
    """The first leaf of an exception group (the `mcp` package's task groups wrap what failed), else `error`."""
    while isinstance(error, BaseExceptionGroup) and error.exceptions:
        error = error.exceptions[0]
    return error


def _cut(text):
    text = " ".join(str(text).split())
    return text if len(text) <= MESSAGE_CUT else text[:MESSAGE_CUT] + " [cut]"


async def _call(database, url, kind, tool, arguments, max_bytes, timeout):
    """The `tools/call` result of `tool` (wire format, a dict) from the server at `url`; QueryError for a server that cannot be reached or does not offer the tool, the time or the bytes running out."""
    try:
        import anyio
        import httpx2
        from mcp import ClientSession
        from mcp.client import sse
        from mcp.client.streamable_http import streamable_http_client
    except ImportError as error:
        raise QueryError(f"sdp_data: {database}: the mcp package cannot be imported ({error})") from None
    budget = _Budget(max_bytes)
    factory = _client_factory(budget)
    timeouts = httpx2.Timeout(CONNECT_SECONDS, read=timeout)
    original = getattr(sse, "sse_within_origin", None)
    outcome = {}
    try:
        if kind == "sse":
            # The SSE transport caps one event at 1 MiB with no setting of its own; the budget bounds what is read instead.
            if original is not None:
                sse.sse_within_origin = functools.partial(original, max_event_size=None)
            transport = sse.sse_client(url, timeout=CONNECT_SECONDS, sse_read_timeout=timeout, httpx_client_factory=factory)
        else:
            transport = streamable_http_client(url, http_client=factory(timeout=timeouts), max_sse_event_size=None)
        with anyio.CancelScope() as scope:
            budget.cancel = scope.cancel
            with anyio.fail_after(timeout):
                async with transport as streams, ClientSession(streams[0], streams[1]) as session:
                    await session.initialize()
                    listing = await session.list_tools()
                    names = sorted(entry.name for entry in listing.tools)
                    if tool not in names:
                        outcome["missing"] = names
                    else:
                        result = await session.call_tool(tool, arguments)
                        outcome["result"] = result.model_dump(by_alias=True, mode="json")
    except BaseException as error:
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        outcome.setdefault("error", _leaf(error))
    finally:
        if original is not None:
            sse.sse_within_origin = original
    if budget.exceeded:
        raise QueryError(f"sdp_data: {database}: the answer is larger than {max_bytes:,} bytes (max_bytes): aggregate in the query, select fewer columns or rows, or pass a larger max_bytes=")
    if "result" in outcome:
        return outcome["result"]
    if "missing" in outcome:
        raise QueryError(f"sdp_data: {database}: its MCP server offers no tool {tool} (it offers {', '.join(outcome['missing']) or 'none'})")
    error = outcome.get("error")
    if isinstance(error, TimeoutError):
        raise QueryError(f"sdp_data: {database}: no answer within {timeout:g} seconds (timeout): aggregate in the query, or pass a larger timeout=")
    if error is None or isinstance(error, asyncio.CancelledError):
        raise QueryError(f"sdp_data: {database}: the call ended without an answer")
    if type(error).__module__.startswith(("mcp", "mcp_types")) or "McpError" in type(error).__name__:
        raise QueryError(f"sdp_data: {database}: the MCP server refused the call: {_cut(error)}")
    raise QueryError(f"sdp_data: {database}: its MCP server could not be reached through the gateway at {url} ({type(error).__name__}: {_cut(error)})")


# ── the rows ──────────────────────────────────────────────────────────────


def _text(result):
    return "\n".join(block["text"] for block in result.get("content") or () if isinstance(block, dict) and block.get("type") == "text" and isinstance(block.get("text"), str))


def _refused(database, message):
    return QueryError(f"sdp_data: {database}: the query was refused: {_cut(message) or 'no reason given'}")


def _rows(database, result):
    """(column names or None, rows) of a `tools/call` result: rows are lists when the names are given, else dicts."""
    if not isinstance(result, dict):
        raise QueryError(f"sdp_data: {database}: the MCP server's answer is not a tool result")
    if result.get("isError") is True:
        raise _refused(database, _text(result))
    if database == "starrocks":
        return _starrocks(database, result)
    if database == "postgres":
        return _postgres(database, result)
    return _mongo(database, result)


def _starrocks(database, result):
    found = result.get("structuredContent")
    if not isinstance(found, dict):
        raise QueryError(f"sdp_data: {database}: the MCP server's answer has no structured result to read the rows from")
    if found.get("success") is not True:
        raise _refused(database, found.get("error_message") or _text(result))
    columns, rows = found.get("column_names"), found.get("rows")
    if columns is None:
        raise QueryError(f"sdp_data: {database}: the statement returned no rows to read (it is not a query)")
    if not isinstance(columns, list) or not all(isinstance(name, str) for name in columns) or not isinstance(rows, list) or not all(isinstance(row, list) and len(row) == len(columns) for row in rows):
        raise QueryError(f"sdp_data: {database}: the MCP server's structured result is not column names and rows")
    _unique(database, columns)
    return columns, rows


def _unique(database, columns):
    seen = set()
    for name in columns:
        if name in seen:
            raise QueryError(f"sdp_data: {database}: the result has two columns named {name!r}: give them distinct names with AS")
        seen.add(name)


def _postgres(database, result):
    text = _text(result).strip()
    if text.startswith("Error:"):
        raise _refused(database, text[len("Error:"):].strip())
    if text == "No results":
        raise QueryError(f"sdp_data: {database}: the statement returned no rows to read (it is not a query)")
    try:
        rows = _literal(text)
    except _Unreadable as error:
        raise QueryError(f"sdp_data: {database}: the answer holds a value that cannot be read ({error})") from None
    if not isinstance(rows, list) or not all(isinstance(row, dict) and all(isinstance(key, str) for key in row) for row in rows):
        raise QueryError(f"sdp_data: {database}: the answer is not a list of rows: {_cut(text[:200])}")
    columns = list(dict.fromkeys(key for row in rows for key in row))
    return columns, [[row.get(name) for name in columns] for row in rows]


def _mongo(database, result):
    text = _text(result)
    try:
        documents = json.loads(text)
    except (ValueError, RecursionError):
        raise QueryError(f"sdp_data: {database}: the answer is not JSON: {_cut(text[:200])}") from None
    if not isinstance(documents, list) or not all(isinstance(document, dict) for document in documents):
        raise QueryError(f"sdp_data: {database}: the answer is not a list of documents: {_cut(text[:200])}")
    return None, documents


# ── PostgreSQL's text of a list of dicts, read without running code ───────


class _Unreadable(Exception):
    pass


def _decimal(text):
    value = decimal.Decimal(text)
    if value.is_finite() and value == value.to_integral_value():
        return int(value)
    return float(value)


def _datetime(*args, tzinfo=None, **kwargs):
    return datetime.datetime(*args, tzinfo=tzinfo, **kwargs)


def _time(*args, tzinfo=None, **kwargs):
    return datetime.time(*args, tzinfo=tzinfo, **kwargs)


_CALLS = {
    "Decimal": _decimal, "decimal.Decimal": _decimal,
    "datetime.datetime": _datetime, "datetime.date": datetime.date, "datetime.time": _time, "datetime.timedelta": datetime.timedelta,
    "datetime.timezone": datetime.timezone, "zoneinfo.ZoneInfo": zoneinfo.ZoneInfo,
    "UUID": lambda text: str(uuid.UUID(text)), "uuid.UUID": lambda text: str(uuid.UUID(text)),
}
_ATTRIBUTES = {"datetime.timezone.utc": datetime.timezone.utc}
_NAMES = {"nan": math.nan, "inf": math.inf}


def _dotted(node):
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if not isinstance(node, ast.Name):
        return None
    parts.append(node.id)
    return ".".join(reversed(parts))


def _literal(text):
    try:
        tree = ast.parse(text, mode="eval")
    except (SyntaxError, ValueError, MemoryError, RecursionError) as error:
        raise _Unreadable(f"not Python's text of a value: {type(error).__name__}") from None
    try:
        return _value(tree.body)
    except RecursionError:
        raise _Unreadable("nested too deeply") from None
    except TypeError as error:
        raise _Unreadable(f"a dict key that cannot be one ({error})") from None


def _value(node):
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, (ast.List, ast.Tuple)):
        return [_value(item) for item in node.elts]
    if isinstance(node, ast.Dict):
        if any(key is None for key in node.keys):
            raise _Unreadable("a dict with **")
        return {_value(key): _value(value) for key, value in zip(node.keys, node.values)}
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.USub, ast.UAdd)):
        operand = _value(node.operand)
        if isinstance(operand, bool) or not isinstance(operand, (int, float, complex)):
            raise _Unreadable("a sign before something not a number")
        return -operand if isinstance(node.op, ast.USub) else operand
    if isinstance(node, ast.Name) and node.id in _NAMES:
        return _NAMES[node.id]
    if isinstance(node, ast.Attribute) and _dotted(node) in _ATTRIBUTES:
        return _ATTRIBUTES[_dotted(node)]
    if isinstance(node, ast.Call):
        name = _dotted(node.func)
        if name not in _CALLS:
            raise _Unreadable(f"a value of the type {name or 'unknown'}")
        args = [_value(item) for item in node.args]
        kwargs = {}
        for keyword in node.keywords:
            if keyword.arg is None:
                raise _Unreadable(f"{name} with **")
            kwargs[keyword.arg] = _value(keyword.value)
        try:
            return _CALLS[name](*args, **kwargs)
        except (TypeError, ValueError, ArithmeticError, zoneinfo.ZoneInfoNotFoundError) as error:
            raise _Unreadable(f"{name}: {error}") from None
    raise _Unreadable(f"the expression {type(node).__name__}")
