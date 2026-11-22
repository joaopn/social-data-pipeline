"""Stand-ins on loopback threads for the chat's OpenAI-compatible back end: a chat-completions endpoint and an MCP server (streamable HTTP at `/mcp`, SSE at `/sse` and `/messages/`). Standard library only, so that the hand-run check can run this file in a plain Python image."""

import http.server
import json
import queue
import threading
import time
import uuid


def tool_call(name, arguments, call_id="call_1"):
    return {"id": call_id, "type": "function", "function": {"name": name, "arguments": arguments if isinstance(arguments, str) else json.dumps(arguments)}}


def reply(content=None, calls=None):
    """A chat-completions answer body."""
    message = {"role": "assistant", "content": content}
    if calls:
        message["tool_calls"] = calls
    return {"id": "chatcmpl-standin", "object": "chat.completion", "choices": [{"index": 0, "message": message, "finish_reason": "tool_calls" if calls else "stop"}]}


class _Quiet(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def body(self):
        return self.rfile.read(int(self.headers.get("Content-Length") or 0))

    def send(self, status, data=b"", kind="application/json", extra=()):
        self.send_response(status)
        if data:
            self.send_header("Content-Type", kind)
        for name, value in extra:
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


class Model(_Quiet):
    """POST `/chat/completions` (below any base path): `server.answer(doc)` gives the next (status, body); every request is recorded in `server.seen` as {path, headers, doc}."""

    def do_POST(self):
        raw = self.body()
        doc = json.loads(raw)
        with self.server.lock:
            self.server.seen.append({"path": self.path, "headers": {k.lower(): v for k, v in self.headers.items()}, "doc": doc, "raw": raw})
        status, body = self.server.answer(doc)
        self.send(status, body if isinstance(body, bytes) else json.dumps(body).encode())


class Mcp(_Quiet):
    """An MCP server: `server.tools` is the `tools/list` answer, `server.call(name, arguments)` the `tools/call` result; every request is recorded in `server.seen` as {method, path, headers, rpc}."""

    def note(self, rpc=None):
        with self.server.lock:
            self.server.seen.append({"method": self.command, "path": self.path, "headers": {k.lower(): v for k, v in self.headers.items()}, "rpc": rpc})

    def answer(self, doc):
        method = doc.get("method")
        if method == "initialize":
            result = {"protocolVersion": doc.get("params", {}).get("protocolVersion"), "capabilities": {"tools": {}}, "serverInfo": {"name": "standin", "version": "1"}}
        elif method == "tools/list":
            result = {"tools": self.server.tools}
        elif method == "tools/call":
            result = self.server.call(doc["params"]["name"], doc["params"].get("arguments") or {})
        else:
            result = {}
        return {"jsonrpc": "2.0", "id": doc["id"], "result": result}

    def do_GET(self):
        self.note()
        if self.path != "/sse":
            return self.send(405)
        session = uuid.uuid4().hex
        events = self.server.sessions[session] = queue.Queue()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        self.wfile.write(f"event: endpoint\r\ndata: /messages/?session_id={session}\r\n\r\n".encode())
        self.wfile.flush()
        while not self.server.stopping.is_set():
            try:
                doc = events.get(timeout=0.2)
            except queue.Empty:
                continue
            self.wfile.write(f"event: message\r\ndata: {json.dumps(doc)}\r\n\r\n".encode())
            self.wfile.flush()

    def do_POST(self):
        doc = json.loads(self.body() or b"{}")
        self.note(doc.get("method") if isinstance(doc, dict) else None)
        if self.path.startswith("/messages/"):
            session = self.path.partition("session_id=")[2]
            if session not in self.server.sessions:
                return self.send(404)
            if "id" in doc:
                self.server.sessions[session].put(self.answer(doc))
            return self.send(202, b"Accepted", "text/plain")
        if self.path not in ("/mcp", "/assistant/mcp"):
            return self.send(404)
        if "id" not in doc:
            return self.send(202)
        extra = [("Mcp-Session-Id", "standin-session")] if doc.get("method") == "initialize" else []
        self.send(200, json.dumps(self.answer(doc)).encode(), extra=extra)

    def do_DELETE(self):
        self.note()
        self.send(200)


def ping_tools(name):
    """The tools of a stand-in database: `ping` answers `pong-from-<name>`."""
    return [{"name": "ping", "description": "Answers pong.", "inputSchema": {"type": "object", "properties": {}}}]


def ping_call(name):
    def call(tool, arguments):
        if tool == "ping":
            return {"content": [{"type": "text", "text": f"pong-from-{name}"}]}
        return {"content": [{"type": "text", "text": "no such tool"}], "isError": True}

    return call


def serve(handler, port=0, host="127.0.0.1", **attributes):
    """A threaded server of `handler` on `host:port`, serving on a daemon thread; its attributes `seen`, `lock`, `stopping`, `sessions` and the given ones are set. `stop(server)` ends it."""
    server = http.server.ThreadingHTTPServer((host, port), handler)
    server.daemon_threads = True
    # A client that closes without reading the answer (the agent does with any status but 200) is not an error of the stand-in.
    server.handle_error = lambda request, address: None
    server.seen, server.lock, server.stopping, server.sessions = [], threading.Lock(), threading.Event(), {}
    for name, value in attributes.items():
        setattr(server, name, value)
    threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.1}, daemon=True).start()
    return server


def stop(server):
    server.stopping.set()
    server.shutdown()
    server.server_close()


def gone(pid):
    """Whether the process `pid` no longer runs (gone, or a zombie waiting for its reaper); it can vanish between the open and the read."""
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().rsplit(")", 1)[1].split()[0] == "Z"
    except (FileNotFoundError, ProcessLookupError):
        return True


def wait_for(condition, seconds=10, what="the condition"):
    deadline = time.monotonic() + seconds
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError(f"in time: {what}")
        time.sleep(0.05)
