"""`sdp_data`, the module the assistant's scripts query with: against a stand-in MCP server behind the real gateway (SSE for postgres, streamable HTTP for starrocks and mongo), as in the sandbox. The calls through the gateway are skipped when the `mcp` package is not installed, `as_frame` when pandas is not."""

import importlib.util
import shutil
import socket
import subprocess
import sys
import time

import pytest
import standins

from social_data_pipeline.chat import claude, sdp_data
from social_data_pipeline.gateway.server import Gateway
from social_data_pipeline.gateway.table import MCP, Listener, Table, Upstream

HAS_MCP = importlib.util.find_spec("mcp") is not None
HAS_PANDAS = importlib.util.find_spec("pandas") is not None
needs_mcp = pytest.mark.skipif(not HAS_MCP, reason="the mcp package is not installed")
TOOLS = [{"name": name, "inputSchema": {"type": "object"}} for name in ("read_query", "write_query", "execute_sql", "query", "aggregate")]


def starrocks_result(columns, rows):
    return {"content": [{"type": "text", "text": "cut text"}], "structuredContent": {"success": True, "execution_time": 0.01, "column_names": columns, "rows": rows}}


def text_result(text, error=False):
    return {"content": [{"type": "text", "text": text}], **({"isError": True} if error else {})}


@pytest.fixture
def served(monkeypatch):
    """The real gateway with a postgres (SSE) and a starrocks and mongo (streamable HTTP) listener, all forwarding to one stand-in MCP server whose answers come from `.answers[tool]` (a result, or a function of the arguments); `.calls` holds the (tool, arguments) it was called with. The environment is the sandbox's, with all three attached."""
    calls, answers = [], {}

    def call(tool, arguments):
        calls.append((tool, arguments))
        answer = answers[tool]
        return answer(arguments) if callable(answer) else answer

    server = standins.serve(standins.Mcp, tools=TOOLS, call=call)
    upstream = Upstream("http", "127.0.0.1", server.server_address[1])
    gw = Gateway(Table("127.0.0.1", (Listener(0, MCP, upstream, paths=("/sse", "/messages/")), Listener(0, MCP, upstream, paths=("/mcp",)), Listener(0, MCP, upstream, paths=("/mcp",))), ""), "", log=lambda line: None)
    gw.start()
    monkeypatch.setitem(sdp_data.DATABASES, "postgres", (gw.ports[0], "sse", "/sse"))
    monkeypatch.setitem(sdp_data.DATABASES, "starrocks", (gw.ports[1], "http", "/mcp"))
    monkeypatch.setitem(sdp_data.DATABASES, "mongo", (gw.ports[2], "http", "/mcp"))
    monkeypatch.setenv("SDP_CHAT_GATEWAY", "127.0.0.1")
    monkeypatch.setenv("SDP_CHAT_DATABASES", "postgres,mongo,starrocks")
    server.calls, server.answers = calls, answers
    yield server
    gw.stop()
    standins.stop(server)


def test_the_table_is_the_runners_and_the_module_stands_alone(tmp_path):
    """Bug class: the helper's copy of the gateway's ports drifting from the runner's (every query sent to the wrong server), or the module importing the chat package, which `python3 -I` in the sandbox cannot see."""
    assert sdp_data.DATABASES == claude.DATABASES
    shutil.copy(sdp_data.__file__, tmp_path / "sdp_data.py")
    done = subprocess.run([sys.executable, "-I", "-c", "import sys; sys.path.insert(0, sys.argv[1]); import sdp_data; print(sdp_data.read_query.__name__)", str(tmp_path)], capture_output=True, text=True, timeout=60)
    assert done.returncode == 0 and done.stdout.strip() == "read_query", done.stderr


@needs_mcp
def test_starrocks_rows_come_back_as_dicts_through_the_gateway(served):
    """Bug class: the rows read from the tool's text (cut at 10,000 characters by the server) instead of its structured result, or the SQL sent under the wrong argument."""
    served.answers["read_query"] = starrocks_result(["month", "n"], [["2024-01", 5], ["2024-02", 7]])
    assert sdp_data.read_query("starrocks", "SELECT month, n FROM t") == [{"month": "2024-01", "n": 5}, {"month": "2024-02", "n": 7}]
    assert served.calls == [("read_query", {"query": "SELECT month, n FROM t"})]


@needs_mcp
@pytest.mark.skipif(not HAS_PANDAS, reason="pandas is not installed")
def test_as_frame_gives_a_dataframe_and_keeps_the_columns_of_an_empty_result(served):
    """Bug class: an empty result losing its columns (a script's `df["n"]` failing with a KeyError instead of finding no rows)."""
    served.answers["read_query"] = starrocks_result(["month", "n"], [["2024-01", 5]])
    frame = sdp_data.read_query("starrocks", "SELECT 1", as_frame=True)
    assert list(frame.columns) == ["month", "n"] and frame["n"].tolist() == [5]
    served.answers["read_query"] = starrocks_result(["month", "n"], [])
    empty = sdp_data.read_query("starrocks", "SELECT 1", as_frame=True)
    assert list(empty.columns) == ["month", "n"] and len(empty) == 0


@needs_mcp
def test_postgres_rows_are_read_from_the_servers_text_without_running_it(served):
    """Bug class: postgres-mcp's answer (Python's text of a list of dicts) read with `eval`, or a Decimal, a datetime with its zone or a UUID failing the read; a value of another type passing silently."""
    text = "[{'n': Decimal('12'), 'share': Decimal('0.5'), 'at': datetime.datetime(2024, 1, 2, 3, 4, tzinfo=datetime.timezone.utc), 'id': UUID('12345678-1234-5678-1234-567812345678'), 'x': None, 'neg': -1}]"
    served.answers["execute_sql"] = text_result(text)
    rows = sdp_data.read_query("postgres", "SELECT ...")
    assert rows == [{"n": 12, "share": 0.5, "at": sdp_data.datetime.datetime(2024, 1, 2, 3, 4, tzinfo=sdp_data.datetime.timezone.utc), "id": "12345678-1234-5678-1234-567812345678", "x": None, "neg": -1}]
    assert served.calls == [("execute_sql", {"sql": "SELECT ..."})]
    served.answers["execute_sql"] = text_result("[{'n': __import__('os').getpid()}]")
    with pytest.raises(sdp_data.QueryError, match=r"^sdp_data: postgres: .*cannot be read"):
        sdp_data.read_query("postgres", "SELECT ...")


@needs_mcp
def test_a_postgres_result_over_one_mib_is_read_whole_over_sse(served):
    """Bug class (measured: the `mcp` package's SSE transport caps one event at 1 MiB with no setting): a large result failing, or hanging until the time limit, on PostgreSQL only."""
    served.answers["execute_sql"] = text_result(repr([{"word": "x" * 1000, "i": i} for i in range(1500)]))
    rows = sdp_data.read_query("postgres", "SELECT ...", timeout=20)
    assert len(rows) == 1500 and rows[-1]["i"] == 1499


@needs_mcp
def test_mongo_asks_for_one_row_more_than_the_bound_and_never_takes_the_servers_default_of_100(served):
    """Bug class: a find without a limit cut silently at the server's default of 100 documents; an aggregation not bounded on the server."""
    served.answers["query"] = text_result('[{"_id": "65a", "n": 1}]')
    served.answers["aggregate"] = text_result('[{"_id": "2024-01", "n": 3}]')
    assert sdp_data.read_query("mongo", {"collection": "posts", "database": "reddit", "filter": {"n": 1}}, max_rows=10, timeout=5) == [{"_id": "65a", "n": 1}]
    assert sdp_data.read_query("mongo", {"collection": "posts", "pipeline": [{"$group": {"_id": "$month", "n": {"$sum": 1}}}]}, max_rows=10, timeout=5) == [{"_id": "2024-01", "n": 3}]
    assert served.calls == [
        ("query", {"collection": "posts", "database": "reddit", "filter": {"n": 1}, "maxTimeMS": 5000, "limit": 11}),
        ("aggregate", {"collection": "posts", "maxTimeMS": 5000, "pipeline": [{"$group": {"_id": "$month", "n": {"$sum": 1}}}, {"$limit": 11}]}),
    ]


@needs_mcp
def test_the_bounds_fail_loudly_naming_the_database_and_the_bound(served):
    """Bug class: a result over a bound cut silently, read without end, or waited on without end."""
    served.answers["read_query"] = starrocks_result(["n"], [[1], [2], [3]])
    with pytest.raises(sdp_data.QueryError, match=r"^sdp_data: starrocks: the result has more than 2 rows \(max_rows\)"):
        sdp_data.read_query("starrocks", "SELECT n FROM t", max_rows=2)
    served.answers["read_query"] = starrocks_result(["s"], [["y" * 1000]] * 200)
    with pytest.raises(sdp_data.QueryError, match=r"^sdp_data: starrocks: the answer is larger than 50,000 bytes \(max_bytes\)"):
        sdp_data.read_query("starrocks", "SELECT s FROM t", max_bytes=50_000, timeout=20)

    def slow(arguments):
        time.sleep(3)
        return starrocks_result(["n"], [[1]])

    served.answers["read_query"] = slow
    started = time.monotonic()
    with pytest.raises(sdp_data.QueryError, match=r"^sdp_data: starrocks: no answer within 0.5 seconds \(timeout\)"):
        sdp_data.read_query("starrocks", "SELECT sleep(3)", timeout=0.5)
    assert time.monotonic() - started < 2.5


@needs_mcp
@pytest.mark.parametrize("database, tool, answer, reason", [
    ("starrocks", "read_query", {"content": [{"type": "text", "text": "Error: denied"}], "structuredContent": {"success": False, "error_message": "Access denied; you need the SELECT privilege"}}, "Access denied; you need the SELECT privilege"),
    ("starrocks", "read_query", text_result("Unknown table x", error=True), "Unknown table x"),
    ("postgres", "execute_sql", text_result("Error: permission denied for table posts"), "permission denied for table posts"),
    ("mongo", "query", text_result("Failed to query collection posts: not authorized", error=True), "not authorized"),
])
def test_a_refusal_of_the_server_is_raised_with_the_database_and_its_reason(served, database, tool, answer, reason):
    """Bug class: an MCP error (a refused or failed query) read as an empty result, or raised without saying which database refused and why."""
    served.answers[tool] = answer
    query = {"collection": "posts"} if database == "mongo" else "SELECT 1"
    with pytest.raises(sdp_data.QueryError, match=rf"^sdp_data: {database}: the query was refused: .*{reason}"):
        sdp_data.read_query(database, query, timeout=10)


def test_a_database_not_attached_or_unknown_is_refused_before_any_connection(monkeypatch):
    """Bug class: a script reaching a database the chat was not given, or a typo failing as a connection error."""
    monkeypatch.setenv("SDP_CHAT_GATEWAY", "127.0.0.1")
    monkeypatch.setenv("SDP_CHAT_DATABASES", "mongo")
    with socket.socket() as listening:
        listening.bind(("127.0.0.1", 0))
        listening.listen()
        listening.settimeout(0.2)
        monkeypatch.setitem(sdp_data.DATABASES, "starrocks", (listening.getsockname()[1], "http", "/mcp"))
        with pytest.raises(sdp_data.QueryError, match=r"^sdp_data: starrocks is not attached to this chat \(attached: mongo\)$"):
            sdp_data.read_query("starrocks", "SELECT 1")
        with pytest.raises(TimeoutError):
            listening.accept()
    with pytest.raises(sdp_data.QueryError, match=r"^sdp_data: 'mysql' is not a database server: "):
        sdp_data.read_query("mysql", "SELECT 1")


def test_a_database_name_in_place_of_the_server_says_the_first_argument_is_the_server():
    """Bug class: the assistant passing the StarRocks database (`reddit`) as the first argument and being told only that it is unknown, with nothing saying where the database name goes."""
    with pytest.raises(sdp_data.QueryError) as raised:
        sdp_data.read_query("reddit", "SELECT 1 FROM comments")
    assert str(raised.value) == "sdp_data: 'reddit' is not a database server: give starrocks, postgres or mongo, and name databases and tables inside the query (for example reddit.comments)"


@needs_mcp
def test_the_server_is_taken_by_position_or_by_the_keyword_server(served):
    """Bug class: the rename breaking the positional call scripts already make, or the keyword the role text names (`server=`) not being accepted; the old keyword `database=` is not taken."""
    served.answers["read_query"] = starrocks_result(["n"], [[1]])
    assert sdp_data.read_query("starrocks", "SELECT 1 AS n FROM reddit.comments") == [{"n": 1}]
    assert sdp_data.read_query(server="starrocks", query="SELECT 1 AS n FROM reddit.comments") == [{"n": 1}]
    assert served.calls == [("read_query", {"query": "SELECT 1 AS n FROM reddit.comments"})] * 2
    with pytest.raises(TypeError):
        sdp_data.read_query(database="starrocks", query="SELECT 1")


@pytest.mark.parametrize("query, reason", [
    ("db.posts.find()", "must be a dict"),
    ({"collection": "posts", "pipeline": [], "filter": {}}, "does not take \\(filter\\)"),
    ({"filter": {}}, "needs \"collection\""),
    ({"collection": "posts", "limit": 0}, "\"limit\" must be"),
])
def test_a_mongo_query_of_another_shape_is_refused_saying_the_shape(monkeypatch, query, reason):
    """Bug class: a query the helper cannot send as asked (a key dropped silently, a shell string) reaching the server as something else."""
    monkeypatch.setenv("SDP_CHAT_GATEWAY", "127.0.0.1")
    monkeypatch.setenv("SDP_CHAT_DATABASES", "mongo")
    with pytest.raises(sdp_data.QueryError, match=f"^sdp_data: mongo: .*{reason}"):
        sdp_data.read_query("mongo", query)
