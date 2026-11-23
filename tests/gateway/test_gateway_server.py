"""The gateway end to end against stand-in upstreams on loopback, with raw bytes for every hostile case.

Bug classes: the credential leaking or the client's own reaching the provider; a request outside the table reaching the upstream at all; request smuggling through framing the gateway and the upstream read differently; client headers passed through; a model body using server-side features; buffered or redirected responses; limits not holding; request data in the log.
"""

import socket
import threading

import pytest

from social_data_pipeline.gateway import rules
from social_data_pipeline.gateway.table import MCP, MODEL_ANTHROPIC, MODEL_OPENAI, Limits

from gateway_helpers import CRED, WAIT, exchange, parse, request, wait_for

OPENAI_BODY = b'{"model":"m-1","messages":[]}'


def headers_of(rec):
    return [n for n, _ in rec.headers]


# a. the credential


@pytest.mark.parametrize("kind,target,body", [(MODEL_ANTHROPIC, "/v1/messages", b'{"model":"x"}'), (MODEL_OPENAI, "/chat/completions", OPENAI_BODY)])
def test_gateway_sets_its_credential_and_drops_the_clients(gw, kind, target, body):
    g = gw(kind)
    raw = request(target, headers=[("Authorization", "Bearer client-key"), ("x-api-key", "client-key")], body=body)
    assert parse(exchange(g.port, raw))[0] == 200
    (rec,) = g.up.records
    assert [v for n, v in rec.headers if n == "authorization"] == [f"Bearer {CRED}"]
    assert b"client-key" not in rec.raw and "x-api-key" not in headers_of(rec)


def test_mcp_gets_no_credential_and_not_the_clients(gw):
    g = gw(MCP)
    exchange(g.port, request("/mcp", headers=[("Authorization", "Bearer client-key")], body=b"{}"))
    (rec,) = g.up.records
    assert "authorization" not in headers_of(rec) and CRED.encode() not in rec.raw


# b. the table: nothing outside it reaches the upstream

REFUSED = [
    (MODEL_ANTHROPIC, b"POST /v1/files HTTP/1.1"),
    (MODEL_ANTHROPIC, b"POST /v1/messages/batches HTTP/1.1"),
    (MODEL_ANTHROPIC, b"GET /v1/models HTTP/1.1"),
    (MODEL_ANTHROPIC, b"POST /v1/messages/../files HTTP/1.1"),
    (MODEL_ANTHROPIC, b"POST /v1/messages/./count_tokens HTTP/1.1"),
    (MODEL_ANTHROPIC, b"POST /v1/%2e%2e/v1/messages HTTP/1.1"),
    (MODEL_ANTHROPIC, b"POST /v1/messages%2fcount_tokens HTTP/1.1"),
    (MODEL_ANTHROPIC, b"POST /v1/messages%5ccount_tokens HTTP/1.1"),
    (MODEL_ANTHROPIC, b"POST /v1/messages%00 HTTP/1.1"),
    (MODEL_ANTHROPIC, b"POST /v1/%252e%252e/files HTTP/1.1"),
    (MODEL_ANTHROPIC, b"POST //v1/messages HTTP/1.1"),
    (MODEL_ANTHROPIC, b"POST /v1//messages HTTP/1.1"),
    (MODEL_ANTHROPIC, b"POST /v1/messages/ HTTP/1.1"),
    (MODEL_ANTHROPIC, b"POST /v1/messages;x=1 HTTP/1.1"),
    (MODEL_ANTHROPIC, b"POST http://evil/v1/messages HTTP/1.1"),
    (MODEL_ANTHROPIC, b"POST * HTTP/1.1"),
    (MODEL_ANTHROPIC, b"CONNECT api.anthropic.com:443 HTTP/1.1"),
    (MODEL_ANTHROPIC, b"CONNECT /v1/messages HTTP/1.1"),
    (MODEL_ANTHROPIC, b"GET /v1/messages HTTP/1.1"),
    (MODEL_ANTHROPIC, b"PUT /v1/messages HTTP/1.1"),
    (MODEL_ANTHROPIC, b"POST /v1/messages?beta=false HTTP/1.1"),
    (MODEL_ANTHROPIC, b"POST /v1/messages?beta=true&x=1 HTTP/1.1"),
    (MODEL_ANTHROPIC, b"POST /v1/messages? HTTP/1.1"),
    (MODEL_ANTHROPIC, b"POST /v1/messages/count_tokens?x=1 HTTP/1.1"),
    (MODEL_ANTHROPIC, b"POST /v1/messages HTTP/1.0"),
    (MODEL_ANTHROPIC, b"POST /v1/messages  HTTP/1.1"),
    (MODEL_ANTHROPIC, "POST /v1/méssages HTTP/1.1".encode()),
    (MODEL_OPENAI, b"POST /api/v1/chat/completions HTTP/1.1"),
    (MODEL_OPENAI, b"POST /chat/completions?stream=1 HTTP/1.1"),
    (MODEL_OPENAI, b"GET /chat/completions HTTP/1.1"),
    (MODEL_OPENAI, b"POST /models HTTP/1.1"),
    (MCP, b"POST /mcpx HTTP/1.1"),
    (MCP, b"POST /mcp/ HTTP/1.1"),
    (MCP, b"POST /mcp/x HTTP/1.1"),
    (MCP, b"POST /messages HTTP/1.1"),
    (MCP, b"POST /messages/../admin HTTP/1.1"),
    (MCP, b"POST /messages/%41 HTTP/1.1"),
    (MCP, b"PUT /mcp HTTP/1.1"),
    (MCP, b"PATCH /mcp HTTP/1.1"),
    (MCP, b"OPTIONS /mcp HTTP/1.1"),
    (MCP, b"POST /messages/?session_id=a%20b HTTP/1.1"),
    (MCP, b"POST /messages/?x=<y> HTTP/1.1"),
    (MCP, b"POST /messages/?" + b"a" * 257 + b" HTTP/1.1"),
]


@pytest.mark.parametrize("kind,line", REFUSED)
def test_requests_outside_the_table_never_reach_the_upstream(gw, kind, line):
    g = gw(kind)
    status, headers, body = parse(exchange(g.port, line + b"\r\nHost: gw\r\nContent-Length: 2\r\n\r\n{}"))
    assert status in (400, 403, 404) and body.startswith(b'{"error": "')
    assert g.up.records == []


@pytest.mark.parametrize("kind,target,upstream_line", [
    (MODEL_ANTHROPIC, "/v1/messages", "POST /v1/messages HTTP/1.1"),
    (MODEL_ANTHROPIC, "/v1/messages?beta=true", "POST /v1/messages?beta=true HTTP/1.1"),
    (MODEL_ANTHROPIC, "/v1/messages/count_tokens?beta=true", "POST /v1/messages/count_tokens?beta=true HTTP/1.1"),
    (MODEL_OPENAI, "/chat/completions", "POST /api/v1/chat/completions HTTP/1.1"),
    (MCP, "/mcp", "POST /mcp HTTP/1.1"),
    (MCP, "/messages/?session_id=ab-1.2", "POST /messages/?session_id=ab-1.2 HTTP/1.1"),
])
def test_requests_in_the_table_go_to_the_tables_target(gw, kind, target, upstream_line):
    g = gw(kind)
    assert parse(exchange(g.port, request(target, body=BODY if kind == MODEL_ANTHROPIC else OPENAI_BODY)))[0] == 200
    assert [r.line for r in g.up.records] == [upstream_line]


# c. smuggling

BODY = b'{"model":"x"}'
CL = b"Content-Length: %d\r\n" % len(BODY)
SMUGGLING = [
    b"POST /v1/messages HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\nd\r\n" + BODY + b"\r\n0\r\n\r\n",
    b"POST /v1/messages HTTP/1.1\r\n" + CL + b"transfer-encoding: chunked\r\n\r\n" + BODY,
    b"POST /v1/messages HTTP/1.1\r\n" + CL + b"TRANSFER-ENCODING: identity\r\n\r\n" + BODY,
    b"POST /v1/messages HTTP/1.1\r\n" + CL + b"Transfer-Encoding : chunked\r\n\r\n" + BODY,
    b"POST /v1/messages HTTP/1.1\r\n" + CL + CL + b"\r\n" + BODY,
    b"POST /v1/messages HTTP/1.1\r\nContent-Length: +13\r\n\r\n" + BODY,
    b"POST /v1/messages HTTP/1.1\r\nContent-Length: -1\r\n\r\n" + BODY,
    b"POST /v1/messages HTTP/1.1\r\nContent-Length: 0x0d\r\n\r\n" + BODY,
    b"POST /v1/messages HTTP/1.1\r\nContent-Length: 13, 13\r\n\r\n" + BODY,
    b"POST /v1/messages HTTP/1.1\r\nContent-Length: 99\r\n\r\n" + BODY,
    b"POST /v1/messages HTTP/1.1\r\n" + CL + b"Expect: 100-continue\r\n\r\n" + BODY,
    b"POST /v1/messages HTTP/1.1\r\n" + CL + b"X-A: b\r\n c\r\n\r\n" + BODY,
    b"POST /v1/messages HTTP/1.1\n" + CL.replace(b"\r\n", b"\n") + b"\n" + BODY,
    b"POST /v1/messages HTTP/1.1\r\n" + CL + b"X-A: b\rTransfer-Encoding: chunked\r\n\r\n" + BODY,
    b"POST /v1/messages HTTP/1.1\r\n" + CL + b"X-A: " + b"a" * 20000 + b"\r\n\r\n" + BODY,
    b"POST /v1/messages HTTP/1.1\r\n" + CL + b"X-A: a\r\n" * 64 + b"\r\n" + BODY,
    b"POST /v1/messages HTTP/1.1\r\n" + CL + b"Host: a\r\nHost: b\r\n\r\n" + BODY,
]


@pytest.mark.parametrize("raw", SMUGGLING)
def test_ambiguous_framing_is_refused_before_the_upstream(gw, raw):
    g = gw()
    assert parse(exchange(g.port, raw))[0] in (400, 431)
    assert g.up.records == []


def test_a_longer_body_is_cut_and_a_second_request_never_read(gw):
    g = gw()
    second = request(body=b'{"model":"second"}')
    data = exchange(g.port, request(body=BODY, length=False)[:-len(BODY)].replace(b"\r\n\r\n", b"\r\n" + CL + b"\r\n") + BODY + second)
    assert data.count(b"HTTP/1.1 ") == 1 and parse(data)[0] == 200
    (rec,) = g.up.records
    assert rec.body == BODY and ("content-length", str(len(BODY))) in rec.headers


# d. headers


def test_only_allow_listed_headers_reach_the_upstream(gw):
    g = gw()
    sent = [("Host", "evil"), ("X-Forwarded-For", "1.2.3.4"), ("Cookie", "a=b"), ("anthropic-dangerous-direct-browser-access", "true"), ("x-stainless-os", "Linux"), ("Accept-Encoding", "gzip"), ("Content-Type", "application/json"), ("Accept", "text/event-stream"), ("anthropic-version", "2023-06-01"), ("anthropic-beta", "b1, evil-beta,b2"), ("anthropic-beta", "b1,other"), ("Connection", "keep-alive")]
    exchange(g.port, request(headers=sent).replace(b"Host: gw\r\n", b""))
    (rec,) = g.up.records
    assert dict(rec.headers) == {"host": f"127.0.0.1:{g.up.port}", "authorization": f"Bearer {CRED}", "content-type": "application/json", "accept": "text/event-stream", "anthropic-version": "2023-06-01", "anthropic-beta": "b1,b2", "content-length": "28", "connection": "close"}
    assert len(rec.headers) == 8


@pytest.mark.parametrize("kind,headers,expected", [
    (MODEL_ANTHROPIC, [("anthropic-beta", "evil")], None),
    (MODEL_ANTHROPIC, [("Accept", "a" * 1025)], None),
    (MCP, [("Mcp-Session-Id", "s-1"), ("MCP-Protocol-Version", "2025-06-18"), ("Last-Event-ID", "7"), ("anthropic-version", "x")], {"mcp-session-id": "s-1", "mcp-protocol-version": "2025-06-18", "last-event-id": "7"}),
])
def test_header_filters(gw, kind, headers, expected):
    g = gw(kind)
    exchange(g.port, request("/mcp" if kind == MCP else "/v1/messages", headers=headers))
    got = {n: v for n, v in g.up.records[0].headers if n not in ("host", "authorization", "content-length", "connection")}
    assert got == (expected or {})


# e. bodies


@pytest.mark.parametrize("kind,body", [
    (MODEL_ANTHROPIC, b'{"model":"x","mcp_servers":[]}'),
    (MODEL_ANTHROPIC, b'{"model":"x","mcp_servers":null}'),
    (MODEL_ANTHROPIC, b'{"model":"x","tools":[{"type":"web_search_20250305","name":"w"}]}'),
    (MODEL_ANTHROPIC, b'{"model":"x","tools":[{"name":"a"},{"type":"code_execution_20250522"}]}'),
    (MODEL_ANTHROPIC, b'{"model":"x","tools":[{"type":7}]}'),
    (MODEL_ANTHROPIC, b'{"model":"x","tools":{"type":"custom"}}'),
    (MODEL_ANTHROPIC, b'{"model":"x","tools":["custom"]}'),
    (MODEL_ANTHROPIC, b'{"model":"x","tools":[],"tools":[{"type":"web_search_20250305"}]}'),
    (MODEL_ANTHROPIC, b'{"model":"x","model":"y"}'),
    (MODEL_ANTHROPIC, b'{"mcp_servers":1,"mcp_servers":2}'),
    (MODEL_ANTHROPIC, b"[" * 100000 + b"]" * 100000),
    (MODEL_ANTHROPIC, b'{"a":' * 65 + b"1" + b"}" * 65),
    (MODEL_ANTHROPIC, b'{"model":"\xff"}'),
    (MODEL_ANTHROPIC, b"[]"),
    (MODEL_ANTHROPIC, b'"x"'),
    (MODEL_ANTHROPIC, b'{"model":'),
    (MODEL_ANTHROPIC, b""),
    (MODEL_OPENAI, b'{"model":"m-2"}'),
    (MODEL_OPENAI, b'{"model":"m-1:online"}'),
    (MODEL_OPENAI, b'{"messages":[]}'),
    (MODEL_OPENAI, b'{"model":"m-1","model":"m-2"}'),
    (MODEL_OPENAI, b'{"model":"m-1","plugins":[]}'),
    (MODEL_OPENAI, b'{"model":"m-1","web_search_options":{}}'),
    (MODEL_OPENAI, b'{"model":"m-1","tools":[{"type":"web_search"}]}'),
    (MODEL_OPENAI, b'{"model":"m-1","tools":[{"name":"f"}]}'),
    (MODEL_ANTHROPIC, b'{"model":"x","container":"c"}'),
    (MODEL_ANTHROPIC, b'{"model":"x","metadata":[' + b"{}," * 1_400_000 + b'{}]}'),
    (MODEL_ANTHROPIC, b'{"model":"x","tools":[{"type":"web_search_20250305","type":"custom"}]}'),
    (MODEL_ANTHROPIC, b'{"model":"x","messages":[{"role":"user","content":[{"a":1,"a":2}]}]}'),
    (MODEL_ANTHROPIC, b'{"model":"x","metadata":{"a\\u0001":1}}'),
    (MODEL_ANTHROPIC, b'{"model":"x","metadata":{"\\ud800":1}}'),
    (MODEL_ANTHROPIC, b'{"model":"x","messages":[{"role":"user","content":[{"type":"image","source":{"type":"url","url":"http://evil/x"}}]}]}'),
    (MODEL_ANTHROPIC, b'{"model":"x","messages":[{"role":"user","content":[{"type":"document","source":{"type":"file","file_id":"f"}}]}]}'),
    (MODEL_ANTHROPIC, b'{"model":"x","messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","url":"http://evil/x"}}]}]}'),
    (MODEL_ANTHROPIC, b'{"model":"x","messages":[{"role":"user","content":[{"type":"image","source":"http://evil/x"}]}]}'),
    (MODEL_ANTHROPIC, b'{"model":"x","messages":[{"role":"user","content":[{"type":"tool_result","content":[{"type":"image","source":{"type":"url","url":"http://evil/x"}}]}]}]}'),
    (MODEL_ANTHROPIC, b'{"model":"x","messages":[{"role":"user","content":[{"type":"document","source":{"type":"content","content":[{"type":"image","source":{"type":"url"}}]}}]}]}'),
    (MODEL_ANTHROPIC, b'{"model":"x","system":[{"type":"document","source":{"type":"url","url":"http://evil/x"}}]}'),
    (MODEL_OPENAI, b'{"model":"m-1","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"http://evil/x"}}]}]}'),
    (MODEL_OPENAI, b'{"model":"m-1","messages":[{"role":"user","content":[{"type":"image_url","image_url":"data:x"}]}]}'),
    (MODEL_OPENAI, b'{"model":"m-1","messages":[{"role":"user","content":[{"type":"file","file":{"file_data":"x"}}]}]}'),
    (MODEL_OPENAI, b'{"model":"m-1","messages":[{"role":"user","content":[{"type":"input_audio","input_audio":{}}]}]}'),
    (MODEL_OPENAI, b'{"model":"m-1","messages":[{"role":"user","content":["text"]}]}'),
    (MODEL_OPENAI, b'{"model":"m-1","models":["m-2"]}'),
    (MODEL_OPENAI, b'{"model":"m-1","provider":{"order":["x"]}}'),
    (MODEL_OPENAI, b'{"model":"m-1","preset":"p"}'),
    (MODEL_OPENAI, b'{"model":"m-1","route":"fallback"}'),
    (MODEL_OPENAI, b'{"model":"m-1","transforms":[]}'),
    (MODEL_OPENAI, b'{"model":"m-1","messages":[{"role":"user","content":{"type":"image_url","image_url":{"url":"http://evil/x"}}}]}'),
    (MODEL_OPENAI, b'{"model":"m-1","messages":[{"role":"user","content":7}]}'),
    (MODEL_ANTHROPIC, b'{"model":"x","messages":[{"role":"user","content":[{"type":"tool_result","content":{"type":"image","source":{"type":"url","url":"http://evil/x"}}}]}]}'),
    (MODEL_ANTHROPIC, b'{"model":"x","messages":[{"role":"user","content":[{"type":"x","deeper":[{"source":{"type":"url"}}]}]}]}'),
    (MODEL_ANTHROPIC, b'{"model":"x","metadata":[' + b"0," * 1_000_001 + b'0]}'),
])
def test_bodies_the_table_refuses(gw, kind, body):
    g = gw(kind)
    assert parse(exchange(g.port, request("/chat/completions" if kind == MODEL_OPENAI else "/v1/messages", body=body)))[0] in (400, 403, 413)
    assert g.up.records == []


@pytest.mark.parametrize("kind,body", [
    (MODEL_ANTHROPIC, b'{ "model" : "x",\n "tools": [{"name":"a"}, {"type":"custom","name":"b"}, {"type":"bash_20250124","name":"bash"}],\n "metadata": {"s":"\\u00e9[[[\\"{" , "n": 1.50E+3, "k\\\\": "\\\\"} }'),
    (MODEL_ANTHROPIC, b'{"metadata":' + b"[" * 63 + b"]" * 63 + b',"model":"x","system":"' + b"[" * 200 + b'"}'),
    (MODEL_ANTHROPIC, b'{"context_management":{},"max_tokens":1,"messages":[],"metadata":{},"model":"x","output_config":{},"stream":true,"system":[],"thinking":{},"tools":[]}'),
    (MODEL_ANTHROPIC, b'{"model":"x","system":[{"type":"text","text":"s"}],"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"AA=="}},{"type":"document","source":{"type":"text","data":"t"}},{"type":"tool_result","content":[{"type":"text","text":"r"}]},{"type":"document","source":{"type":"content","content":[{"type":"text","text":"c"}]}}]}]}'),
    (MODEL_ANTHROPIC, b'{"model":"x","messages":[{"role":"assistant","content":[{"type":"tool_use","id":"t","name":"q","input":{"source":"reddit"}}]}]}'),
    (MODEL_ANTHROPIC, b'{"model":"x","messages":[{"role":"assistant","content":[{"type":"tool_use","id":"t","name":"q","input":{"source":{"type":"url","url":"https://x"}}}]}]}'),
    (MODEL_OPENAI, b'{"model":"m-1","messages":[{"role":"user","content":[{"type":"text","text":"t"},{"type":"image_url","image_url":{"url":"data:image/png;base64,AA=="}}]}]}'),
    (MODEL_OPENAI, b'{"model":"m-1","tools":[{"type":"function","function":{"name":"f"}}],"stream":true}'),
])
def test_passing_bodies_reach_the_upstream_byte_for_byte(gw, kind, body):
    g = gw(kind)
    assert parse(exchange(g.port, request("/chat/completions" if kind == MODEL_OPENAI else "/v1/messages", body=body)))[0] == 200
    assert g.up.records[0].body == body


# f. responses


def test_sse_passes_event_by_event(gw):
    seen = [threading.Event(), threading.Event()]

    def respond(conn, rec):
        conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n")
        for i, name in enumerate((b"one", b"two")):
            conn.sendall(b"event: e\ndata: " + name + b"\n\n")
            assert seen[i].wait(WAIT), "the client did not get the event before the next was sent"

    g = gw(respond=respond)
    with socket.create_connection(("127.0.0.1", g.port), timeout=WAIT) as sock:
        sock.sendall(request())
        data = b""
        for i, name in enumerate((b"data: one", b"data: two")):
            while name not in data:
                data += sock.recv(65536)
            seen[i].set()
        while chunk := sock.recv(65536):
            data += chunk
    status, headers, body = parse(data)
    assert status == 200 and headers["content-type"] == "text/event-stream" and body == b"event: e\ndata: one\n\nevent: e\ndata: two\n\n"


def raw_answer(text):
    return lambda conn, rec: conn.sendall(text)


@pytest.mark.parametrize("answer,status", [
    (b"HTTP/1.1 302 Found\r\nLocation: http://evil/\r\nContent-Length: 0\r\n\r\n", 502),
    (b"HTTP/1.1 307 Temporary Redirect\r\nLocation: /v1/files\r\nContent-Length: 0\r\n\r\n", 502),
    (b"", 502),
    (b"garbage\r\n\r\n", 502),
])
def test_redirects_and_broken_upstreams_become_502(gw, answer, status):
    g = gw(respond=raw_answer(answer))
    got, headers, body = parse(exchange(g.port, request()))
    assert (got, "location" in headers, b"evil" in body) == (status, False, False)


def test_only_allow_listed_response_headers_come_back(gw):
    answer = b"HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/json\r\nSet-Cookie: s=1\r\nServer: up\r\nX-Internal: 1\r\nretry-after: 3\r\nrequest-id: r-1\r\nanthropic-ratelimit-requests-limit: 50\r\nCache-Control: no-cache\r\nMcp-Session-Id: m\r\nContent-Length: 2\r\n\r\n{}"
    g = gw(respond=raw_answer(answer))
    status, headers, body = parse(exchange(g.port, request()))
    assert status == 429 and body == b"{}"
    assert set(headers) == {"content-type", "retry-after", "request-id", "anthropic-ratelimit-requests-limit", "cache-control", "mcp-session-id", "transfer-encoding", "connection"}


def test_a_stream_cut_by_the_upstream_is_cut_for_the_client(gw):
    g = gw(respond=raw_answer(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\npartial"))
    data = exchange(g.port, request())
    assert data.startswith(b"HTTP/1.1 200 ") and b"partial" in data and not data.endswith(b"0\r\n\r\n")


def test_unreachable_and_silent_upstreams_are_502_without_a_hang(gw):
    silent = threading.Event()
    g = gw(respond=lambda conn, rec: silent.wait(WAIT), limits=Limits(model_idle_timeout=0.2))
    assert parse(exchange(g.port, request()))[0] == 502
    silent.set()
    g.up.close()
    status, _, body = parse(exchange(g.port, request()))
    assert (status, body) == (502, b'{"error": "upstream_unreachable"}')


# g. limits


def test_the_connection_after_the_cap_is_closed_at_once(gw):
    g = gw(MCP, limits=Limits(max_connections=32, listener_connections=64))
    held = [socket.create_connection(("127.0.0.1", g.port), timeout=WAIT) for _ in range(32)]
    try:
        with socket.create_connection(("127.0.0.1", g.port), timeout=WAIT) as extra:
            assert extra.recv(1) == b""
    finally:
        for sock in held:
            sock.close()
    wait_for(lambda: len(g.log) >= 32)
    assert parse(exchange(g.port, request("/mcp", body=b"{}")))[0] == 200


def test_requests_per_minute_with_a_moved_clock(gw):
    now = [1000.0]
    g = gw(clock=lambda: now[0])
    for _ in range(60):
        assert parse(exchange(g.port, request()))[0] == 200
    now[0] += 30
    status, headers, _ = parse(exchange(g.port, request()))
    assert (status, headers["retry-after"]) == (429, "30")
    now[0] += 30
    assert parse(exchange(g.port, request()))[0] == 200
    assert len(g.up.records) == 61


def test_concurrent_streams_are_capped(gw):
    release = threading.Event()

    def respond(conn, rec):
        release.wait(WAIT)
        ok = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}"
        conn.sendall(ok)

    g = gw(respond=respond)
    socks = [socket.create_connection(("127.0.0.1", g.port), timeout=WAIT) for _ in range(4)]
    try:
        for sock in socks:
            sock.sendall(request())
        for _ in range(4):
            assert g.up.arrived.acquire(timeout=WAIT)
        assert parse(exchange(g.port, request()))[0] == 429
        release.set()
        for sock in socks:
            data = b""
            while chunk := sock.recv(65536):
                data += chunk
            assert parse(data)[0] == 200
    finally:
        release.set()
        for sock in socks:
            sock.close()


def test_a_slow_head_is_closed_after_the_deadline(gw):
    g = gw(limits=Limits(head_timeout=0.2))
    assert exchange(g.port, b"POST /v1/messages HTTP/1.1\r\nHost: gw\r\n", half_close=False) == b""
    wait_for(lambda: g.log)
    assert g.up.records == [] and "reason=head_timeout" in g.log[0]


# i. the log


def test_the_log_holds_no_header_query_or_body_text(gw):
    g = gw(MCP)
    exchange(g.port, request("/messages/MARKP?MARKQ=1", headers=[("Mcp-Session-Id", "MARKH")], body=b'{"MARKB":1}'))
    exchange(g.port, request("/mcp?MARKQ=<", body=b"MARKB"))
    wait_for(lambda: len(g.log) == 2)
    first, second = sorted(g.log, key=lambda line: "status=200" not in line)
    assert "MARK" not in first + second
    assert " method=POST path=/messages/ status=200 in=11 out=2 " in first and "status=400" in second and "reason=bad_query" in second


def test_a_listener_has_its_own_connection_cap(gw):
    g = gw(MCP, limits=Limits(listener_connections=2))
    held = [socket.create_connection(("127.0.0.1", g.port), timeout=WAIT) for _ in range(2)]
    try:
        with socket.create_connection(("127.0.0.1", g.port), timeout=WAIT) as extra:
            assert extra.recv(1) == b""
    finally:
        for sock in held:
            sock.close()
    wait_for(lambda: len(g.log) >= 2)
    assert parse(exchange(g.port, request("/mcp", body=b"{}")))[0] == 200


def test_no_socket_stays_tracked_after_upstreams_that_close(gw):
    g = gw(respond=raw_answer(b""))
    for _ in range(3):
        assert parse(exchange(g.port, request()))[0] == 502
    wait_for(lambda: len(g.log) == 3)
    assert g.gateway._live_socks == set()


def test_a_client_that_stops_reading_is_cut_after_the_idle_timeout(gw):
    def respond(conn, rec):
        conn.sendall(b"HTTP/1.1 200 OK\r\n\r\n")
        try:
            while True:
                conn.sendall(b"x" * (1 << 20))
        except OSError:
            pass

    g = gw(respond=respond, limits=Limits(model_idle_timeout=0.3, body_timeout=WAIT * 6))
    with socket.socket() as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        sock.settimeout(WAIT)
        sock.connect(("127.0.0.1", g.port))
        sock.sendall(request())
        wait_for(lambda: g.log)
    assert "reason=client_gone" in g.log[0]


def test_an_accept_error_and_a_failed_thread_start_keep_the_listener(gw, monkeypatch):
    pauses = []
    g = gw(MCP)
    g.gateway._sleep = pauses.append
    real_accept, real_start, failed = socket.socket.accept, threading.Thread.start, []

    def accept(sock):
        if sock.getsockname()[1] == g.port and not pauses:
            raise OSError("EMFILE")
        return real_accept(sock)

    def start(thread):
        if getattr(thread, "_args", (None,))[0] == g.gateway._handle and not failed:
            failed.append(thread)
            raise RuntimeError("can't start new thread")
        return real_start(thread)

    monkeypatch.setattr(socket.socket, "accept", accept)
    monkeypatch.setattr(threading.Thread, "start", start)
    with socket.create_connection(("127.0.0.1", g.port), timeout=WAIT) as first:
        assert first.recv(1) == b""
    assert parse(exchange(g.port, request("/mcp", body=b"{}")))[0] == 200
    wait_for(lambda: g.log)
    assert pauses == [0.1] and len(failed) == 1 and g.gateway._live_socks == set()


@pytest.mark.parametrize("body,code", [(b"]" * (1 << 20), "body_too_many_containers"), (b"]" + b"[" * 10, "bad_json"), (b"[" + b"1," * 1_000_001 + b"1]", "body_too_many_elements")])
def test_the_shape_scan_refuses_before_any_long_loop(body, code):
    assert rules._shape(body) == code


def test_refused_bodies_count_against_the_window(gw):
    g = gw(clock=lambda: 1000.0)
    for _ in range(60):
        assert parse(exchange(g.port, request(body=b"[]")))[0] == 400
    assert parse(exchange(g.port, request()))[0] == 429
    assert g.up.records == []


def test_busy_body_checks_answer_429_and_give_the_connection_places_back(gw):
    g = gw(limits=Limits(body_timeout=0.2))
    held = [g.gateway._checks.acquire(blocking=False) for _ in range(2)]
    try:
        assert held == [True, True] and parse(exchange(g.port, request()))[0] == 429
    finally:
        for _ in held:
            g.gateway._checks.release()
    wait_for(lambda: g.log)
    assert (g.gateway._conns._value, g.gateway._listener_conns[0]._value, g.up.records) == (32, 8, [])
