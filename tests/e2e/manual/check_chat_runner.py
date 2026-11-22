#!/usr/bin/env python3
"""A check of the chat's runner and its Claude Code back end in the real sandbox image, through the real gateway's rules, run by hand: `python tests/e2e/manual/check_chat_runner.py` (Linux; a Docker without user-id mapping that can pull `python:3.13-slim` and reach PyPI and the vendor's download of Claude Code, and the image `python:3.13-alpine`, which the stand-in uses and which is not pulled).

It is not collected by pytest. It builds the gateway and sandbox images under throwaway names, makes the chat's two networks under throwaway names (`chat_host._make_networks`) and writes the gateway's config as `sdp chat start` does (`chat_host.gateway_config`, so the measured `anthropic-beta` values and tool types are the deployment's), with a made-up credential. The gateway's model upstream is fixed to the vendor's API, so the gateway container runs the gateway package with its test-only `test_upstream` (a loopback address) and a STAND-IN API runs in the gateway's network namespace: a small Messages-API server on loopback (a `tool_use` of `Bash` writing `plot.png` for a first message, a `tool_use` of the mongo MCP tool for a second, a reply held open for a third, a final text after a `tool_result`) and a small MCP server for mongo, both recording every request. The sandbox is created by `chat_host.sandbox_arguments`, so it runs the runner as `sdp chat start` would. A helper speaks the framed protocol on the runner's socket from this machine, through the mounted socket folder. What it shows:

1. `hello` names the back end and mongo and says the query queue is not attached. `chat.new`, then a `chat.send` whose turn ends with the events `user`, `tool` (Bash), `tool_result` (the marker the command printed), `text`, `file` (`plot.png`) and `end` without error; `chat.files` lists `plot.png` and `chat.file` returns its bytes.
2. A second `chat.send` resumes the session (the stand-in sees the first turn's message in the second turn's first request) and calls the mongo tool through the gateway's MCP port.
3. `chat.stop` on a turn whose reply is held open ends it with the stopped `error` event, and no process of the turn is left in the sandbox (`docker top`).
4. Every request the stand-in saw passed the gateway (its log: status 200 and nothing refused); the measured body keys, tool names and types and `anthropic-beta` values are printed; the stand-in saw the made-up credential of this check, never the sandbox's placeholder. `chat.delete` removes the chat's folder.

Clean-up, also when the check fails: every throwaway container, network and image is removed and the temp folder is deleted. What stays: the base images and Docker's build cache.
"""

import base64
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from social_data_pipeline import chat_host as host
from social_data_pipeline.chat import claude
from social_data_pipeline.control.client import Client
from social_data_pipeline.gateway import rules

TOKEN = "made-up-claude-token-for-the-runner-check-58c1"
ALPINE = "python:3.13-alpine"
MODEL_PORT, MCP_PORT = 9090, 3000
MARKER = "MARKER-7f3a"
# The gateway with its loopback test upstream, otherwise as `python -m social_data_pipeline.gateway` runs it.
GATEWAY = f"""import signal, threading
from social_data_pipeline.gateway.server import Gateway
from social_data_pipeline.gateway.table import Upstream, load, read_credential
table = load("/run/sdp-gateway/config.json")
gateway = Gateway(table, read_credential(table.credential_file), test_upstream=Upstream("http", "127.0.0.1", {MODEL_PORT}))
gateway.start()
for lst, port in zip(table.listeners, gateway.ports):
    print(f"gateway: {{lst.kind}} listening on port {{port}}", flush=True)
stop = threading.Event()
signal.signal(signal.SIGTERM, lambda *_: stop.set())
stop.wait()
gateway.stop()
"""
STANDIN = r'''
import http.server, json, sys, threading, time
lock = threading.Lock()
def record(entry):
    with lock, open("/standin/record.jsonl", "a") as f:
        f.write(json.dumps(entry) + "\n")
def sse(block, stop):
    events = [("message_start", {"type": "message_start", "message": {"id": "msg_standin", "type": "message", "role": "assistant", "model": "claude-standin", "content": [], "stop_reason": None, "stop_sequence": None, "usage": {"input_tokens": 10, "output_tokens": 1}}})]
    if block["type"] == "text":
        events += [("content_block_start", {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}), ("content_block_delta", {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": block["text"]}})]
    else:
        events += [("content_block_start", {"type": "content_block_start", "index": 0, "content_block": {"type": "tool_use", "id": block["id"], "name": block["name"], "input": {}}}), ("content_block_delta", {"type": "content_block_delta", "index": 0, "delta": {"type": "input_json_delta", "partial_json": json.dumps(block["input"])}})]
    events += [("content_block_stop", {"type": "content_block_stop", "index": 0}), ("message_delta", {"type": "message_delta", "delta": {"stop_reason": stop, "stop_sequence": None}, "usage": {"output_tokens": 5}}), ("message_stop", {"type": "message_stop"})]
    return "".join(f"event: {e}\ndata: {json.dumps(d)}\n\n" for e, d in events).encode()
def texts(content):
    return content if isinstance(content, str) else " ".join(b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text")
class Model(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *a):
        pass
    def do_POST(self):
        doc = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)))
        messages = doc.get("messages") or []
        users = [m for m in messages if m.get("role") == "user"]
        last = users[-1]["content"] if users else ""
        result = isinstance(last, list) and any(isinstance(b, dict) and b.get("type") == "tool_result" for b in last)
        said = texts(last)
        record({"path": self.path, "authorization": self.headers.get("Authorization"), "beta": [b.strip() for b in (self.headers.get("anthropic-beta") or "").split(",") if b.strip()], "keys": sorted(doc), "tools": sorted({(t.get("name"), t.get("type", "custom")) for t in doc.get("tools") or []}), "said": said[-200:], "all_said": " ".join(texts(m["content"]) for m in users)[-4000:], "role_in_system": "read-only" in json.dumps(doc.get("system"))})
        if "PLOT-TURN" in said:
            command = "python3 -c \"open('plot.png','wb').write(bytes([0x89,0x50,0x4e,0x47,0x0d,0x0a,0x1a,0x0a])+b'standin')\" && echo MARKER-7f3a"
            block, stop = {"type": "tool_use", "id": "toolu_1", "name": "Bash", "input": {"command": command, "description": "write a plot"}}, "tool_use"
        elif "MCP-TURN" in said:
            block, stop = {"type": "tool_use", "id": "toolu_2", "name": "mcp__mongo__ping", "input": {}}, "tool_use"
        elif "HOLD-TURN" in said:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Content-Length", "1000000")
            self.end_headers()
            self.wfile.write(sse({"type": "text", "text": ""}, "end_turn")[:200])
            self.wfile.flush()
            time.sleep(600)
            return
        else:
            block, stop = {"type": "text", "text": "Here is the plot: plot.png" if result else "plain answer"}, "end_turn"
        data = sse(block, stop)
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
class Mcp(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *a):
        pass
    def reply(self, status, doc=None):
        data = b"" if doc is None else json.dumps(doc).encode()
        self.send_response(status)
        if doc is not None:
            self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
    def do_POST(self):
        doc = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
        record({"mcp": doc.get("method"), "path": self.path})
        if "id" not in doc:
            return self.reply(202)
        method = doc.get("method")
        result = {"protocolVersion": doc.get("params", {}).get("protocolVersion", "2025-06-18"), "capabilities": {"tools": {}}, "serverInfo": {"name": "standin", "version": "1"}} if method == "initialize" else {"tools": [{"name": "ping", "description": "Answers pong.", "inputSchema": {"type": "object", "properties": {}}}]} if method == "tools/list" else {"content": [{"type": "text", "text": "pong-from-mongo"}]} if method == "tools/call" else {}
        self.reply(200, {"jsonrpc": "2.0", "id": doc["id"], "result": result})
    def do_GET(self):
        self.reply(405)
    def do_DELETE(self):
        self.reply(200)
for handler, port in ((Model, int(sys.argv[1])), (Mcp, int(sys.argv[2]))):
    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), handler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
print("stand-in ready", flush=True)
threading.Event().wait()
'''


def check(condition, what):
    if not condition:
        raise AssertionError(what)
    print(f"  ok: {what}")


def within(deadline, what):
    if time.monotonic() > deadline:
        raise AssertionError(f"in time: {what}")


class Check:
    def __init__(self):
        self.root = Path(tempfile.mkdtemp(prefix="sdp-chat-runner-")).resolve()
        os.chmod(self.root, 0o700)
        suffix = os.urandom(4).hex()
        self.images = [f"sdp-gateway-check-{suffix}", f"sdp-sandbox-check-{suffix}"]
        self.gateway, self.sandbox, self.standin = f"sdp-gateway-check-{suffix}", f"sdp-sandbox-check-{suffix}", f"sdp-standin-check-{suffix}"
        host.GATEWAY_IMAGE, host.SANDBOX_IMAGE = self.images
        host.GATEWAY_CONTAINER, host.SANDBOX_CONTAINER = self.gateway, self.sandbox
        host.NETWORK, host.UP_NETWORK = f"sdp-chat-check-{suffix}", f"sdp-chat-up-check-{suffix}"
        self.networks = []
        chat = self.root / ".sdp" / "chat"
        for folder in (self.root / ".sdp", chat, chat / "credential", chat / "work", chat / "socket", chat / "gateway", self.root / "standin"):
            folder.mkdir(mode=0o700)
        (chat / "credential" / "model.token").write_text(TOKEN)
        os.chmod(chat / "credential" / "model.token", 0o600)
        self.work, self.socket = chat / "work", chat / "socket" / "runner.sock"
        self.record = self.root / "standin" / "record.jsonl"

    def docker(self, *argv, timeout=300):
        return subprocess.run(["docker", *argv], capture_output=True, text=True, timeout=timeout, stdin=subprocess.DEVNULL)

    def step(self, *argv, timeout=300):
        result = self.docker(*argv, timeout=timeout)
        if result.returncode != 0:
            raise AssertionError(f"docker {argv[0]} failed: {(result.stderr or result.stdout).strip()[-1500:]}")
        return result.stdout

    def requests(self):
        return [json.loads(line) for line in self.record.read_text().splitlines()] if self.record.exists() else []

    def clean_up(self):
        for name in (self.sandbox, self.standin, self.gateway):
            self.docker("rm", "--force", name)
        for name in (host.UP_NETWORK, host.NETWORK):
            self.docker("network", "rm", name)
        for name in self.images:
            self.docker("image", "rm", "--force", name)
        shutil.rmtree(self.root, ignore_errors=True)
        left = [n for n in (self.sandbox, self.standin, self.gateway, host.NETWORK, host.UP_NETWORK) if self.docker("inspect", n).returncode == 0]
        return left + [n for n in self.images if self.docker("image", "inspect", n).returncode == 0]


def start(c):
    print("0. images, networks, the gateway with a stand-in upstream, the sandbox")
    for arguments in host.build_arguments(str(REPO_ROOT)):
        result = subprocess.run(arguments, capture_output=True, text=True, timeout=3600)
        check(result.returncode == 0, f"built {arguments[arguments.index('-t') + 1]}" + ("" if result.returncode == 0 else ": " + result.stderr.strip()[-1500:]))
    address = host._make_networks(str(c.root), c.networks)
    config = host.gateway_config(address, {"backend": host.CLAUDE_CODE}, [host.DATABASES[1]])
    config["listeners"][1]["upstream"] = f"http://127.0.0.1:{MCP_PORT}"
    host._write_gateway_config(str(c.root), config)
    c.step(*host.gateway_arguments(str(c.root), os.getuid(), os.getgid(), address)[1:], "python", "-c", GATEWAY)
    c.step("start", c.gateway)
    deadline = time.monotonic() + 30
    while c.docker("logs", c.gateway).stdout.count(" listening on port ") < 2:
        within(deadline, "the gateway listens")
        time.sleep(0.2)
    c.step("run", "-d", "--name", c.standin, "--network", f"container:{c.gateway}", "--user", f"{os.getuid()}:{os.getgid()}", "--mount", f"type=bind,source={c.root / 'standin'},target=/standin", ALPINE, "python", "-c", STANDIN, str(MODEL_PORT), str(MCP_PORT))
    c.step(*host.sandbox_arguments(str(c.root), os.getuid(), os.getgid(), address, host.CLAUDE_CODE, ["mongo"], 10)[1:])
    c.step("start", c.sandbox)
    deadline = time.monotonic() + 30
    while not c.socket.exists():
        within(deadline, "the runner's socket appears")
        time.sleep(0.2)
    print("  ok: the gateway listens, the stand-in and the sandbox run, the runner's socket is there")


def wait(client, chat, seconds=120):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        reply = client.request({"op": "chat.events", "chat": chat, "offset": 0, "limit": 200})
        if not reply["running"]:
            return reply["events"]
        time.sleep(0.3)
    raise AssertionError("the turn did not end in time")


def check_turns(c, client):
    print("1. hello, a new chat, a turn that writes a plot")
    hello = client.request({"op": "hello"})
    check(hello == {"version": 1, "backend": "claude-code", "databases": ["mongo"], "queue": False}, f"hello: {hello}")
    chat = client.request({"op": "chat.new"})["chat"]
    check(client.request({"op": "chat.send", "chat": chat, "text": "PLOT-TURN draw the counts"}) == {"offset": 0}, f"chat.send on {chat}")
    events = wait(client, chat)
    kinds = [e["type"] for e in events]
    print(f"    events: {kinds}")
    check(kinds == ["user", "tool", "tool_result", "text", "file", "end"], "the events are user, tool, tool_result, text, file, end")
    check(events[1]["name"] == "Bash" and events[2]["text"] == MARKER and events[2]["is_error"] is False, f"the tool is Bash and its result the marker: {events[2]['text']!r}")
    check(events[4]["name"] == "plot.png" and events[5]["is_error"] is False, f"the file event names plot.png and the end is no error: {events[5]}")
    files = client.request({"op": "chat.files", "chat": chat})
    check([f["name"] for f in files["files"]] == ["plot.png"], f"chat.files lists plot.png: {files}")
    piece = client.request({"op": "chat.file", "chat": chat, "name": "plot.png", "offset": 0, "limit": 1024})
    check(base64.b64decode(piece["data"]) == bytes([0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]) + b"standin" and piece["end"] is True, "chat.file returns its bytes")
    meta = json.loads((c.work / chat / "meta.json").read_text())
    check(meta["resume"] is True and meta["title"] == "PLOT-TURN draw the counts", f"meta.json: {meta}")

    print("2. a second turn resumes the session and uses the mongo tool")
    before = len(c.requests())
    client.request({"op": "chat.send", "chat": chat, "text": "MCP-TURN ask mongo"})
    events = wait(client, chat)[6:]
    check([e["type"] for e in events] == ["user", "tool", "tool_result", "text", "end"] and events[1]["name"] == "mcp__mongo__ping" and events[2]["text"] == "pong-from-mongo", f"the mongo tool answered through the gateway: {[(e['type'], e.get('name') or e.get('text')) for e in events]}")
    first = next(r for r in c.requests()[before:] if "path" in r and "said" in r)
    check("PLOT-TURN draw the counts" in first["all_said"], "the second turn's first request carries the first turn's message")

    print("3. stop a turn whose reply is held open")
    client.request({"op": "chat.send", "chat": chat, "text": "HOLD-TURN wait"})
    deadline = time.monotonic() + 60
    while not any("HOLD-TURN" in r.get("said", "") for r in c.requests()):
        within(deadline, "the held request reached the stand-in")
        time.sleep(0.2)
    check(client.request({"op": "chat.stop", "chat": chat}) == {"stopping": True}, "the held request reached the stand-in; chat.stop")
    events = wait(client, chat, 30)
    check([e["type"] for e in events[-2:]] == ["error", "end"] and events[-2]["message"] == "the turn was stopped", f"the turn ends with the stopped error: {events[-2:]}")
    top = c.step("top", c.sandbox, "-o", "pid,args").strip().splitlines()[1:]
    print("    | " + "\n    | ".join(top))
    check(len(top) == 2 and any("docker-init" in line for line in top) and any("social_data_pipeline.chat" in line for line in top), "no process of the turn is left in the sandbox (only its init and the runner)")
    return chat


def check_gateway(c, client, chat):
    print("4. what passed the gateway, and delete")
    seen = c.requests()
    model = [r for r in seen if "said" in r]
    logs = c.docker("logs", c.gateway)
    lines = [line for line in (logs.stdout + logs.stderr).splitlines() if " method=" in line]
    # A refusal of the gateway itself is logged with its code as the reason; a forwarded request with `reason=-` (or the client gone, for the stopped turn).
    refused = [line for line in lines if not line.endswith(("reason=-", "reason=client_gone"))]
    check(model and all(r["path"] == "/v1/messages?beta=true" for r in model), f"{len(model)} model requests, each POST /v1/messages?beta=true")
    check(not refused, f"the gateway refused nothing ({len(lines)} lines in its log)" + ("" if not refused else f": {refused[:3]}"))
    check(all(r["authorization"] == f"Bearer {TOKEN}" for r in model), "every model request reached the stand-in with the made-up credential the gateway holds")
    check(not any(claude.PLACEHOLDER_TOKEN in json.dumps(r) for r in seen), "the sandbox's placeholder reached the stand-in nowhere")
    check(all(r["role_in_system"] for r in model), "the role text is in each request's system prompt")
    keys = sorted({k for r in model for k in r["keys"]})
    tools = sorted({tuple(t) for r in model for t in r["tools"]})
    betas = sorted({b for r in model for b in r["beta"]})
    print(f"    measured body keys: {keys}")
    print(f"    measured tools (name, type): {tools}")
    print(f"    measured anthropic-beta values: {betas}")
    print(f"    MCP requests: {sorted({r['mcp'] for r in seen if 'mcp' in r})}")
    check(set(betas) <= set(host.BETA_ALLOWED) and set(keys) <= rules.ANTHROPIC_KEYS, "the measured keys and beta values are within the gateway's constants")
    check(client.request({"op": "chat.delete", "chat": chat}) == {} and not (c.work / chat).exists() and client.request({"op": "chat.list"}) == {"chats": []}, "chat.delete removes the chat's folder")


def main():
    c = Check()
    failed = None
    try:
        try:
            print(f"temp folder {c.root.name}, names *-check-{c.images[0].rsplit('-', 1)[1]}, Docker {c.docker('version', '--format', '{{.Server.Version}}').stdout.strip()}")
            start(c)
            client = Client(str(c.socket), timeout=15)
            chat = check_turns(c, client)
            check_gateway(c, client, chat)
        except Exception as error:
            failed = error
    finally:
        left = c.clean_up()
    if left:
        print(f"FAILED: left behind: {left}")
        return 1
    print("  ok: no container, network or image of the check is left, and the temp folder is deleted")
    if failed is not None:
        print(f"FAILED: {type(failed).__name__}: {failed}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
