#!/usr/bin/env python3
"""A check of the chat's assistant using the query queue, end to end against a real Docker, run by hand: `python tests/e2e/manual/check_chat_queue_use.py` (what it needs: what `check_chat_queue.py` and `check_chat_runner.py` need).

It is not collected by pytest. The scheduler is `check_chat_queue.py`'s (its throwaway deployment and compose project; the secret file made by `chat start`'s own code); the chat is `check_chat_runner.py`'s (throwaway images built by `chat_host.build_arguments`, the chat's two networks by `chat_host._make_networks`, a made-up credential), with the gateway's config written by `chat_host.gateway_config` with the queue's listener and its copy of the secret by `chat_host._write_gateway_secret`. The gateway runs as `python -m social_data_pipeline.gateway` does (secrets read from its files) but with its test-only loopback model upstream, where a STAND-IN API runs in its network namespace (the Messages API for Claude Code, chat completions for the agent; both record every request). The scheduler is attached to the chat's upper network under its alias as `chat start` attaches it, and the gateway's queue listener reaches it through a RECORDER in the scheduler's network namespace, which writes down the method, path and header names of each request as the gateway forwarded it (never a value) and passes the bytes on. The sandbox is created by `chat_host.sandbox_arguments` with the queue, without databases. For each back end (Claude Code, then the OpenAI-compatible agent, the gateway, the stand-in and the sandbox made again for it):

1. `hello` says `queue: true`; `curl` is not in the image.
2. A turn whose stand-in reply calls the submit tool (`mcp__jobs__submit_postgres_query`, `jobs__submit_postgres_query`): the chat's events show the tool call and its result, a job id; the scheduler's record on disk carries `submitted_by: "assistant"` and is pending. The role text's paragraph is in the request.
3. A turn whose reply calls `query_status` for it: pending.
4. A finished marked job (written with the store's own code inside the scheduler's container) fetched by a shell tool call (`Bash`, `run_shell`) that lists its parts and saves part 0 into the chat's folder with Python's `urllib` and the address in `SDP_CHAT_GATEWAY`: the `file` event, and the bytes equal.
5. The gateway refused nothing; the requests the MCP client and the fetch sent, as forwarded, are printed.

Clean-up, also when the check fails: every throwaway container, network and image of both is removed and both temp folders are deleted. What stays: the base images and Docker's build cache.
"""

import json
import os
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from check_chat_queue import Deployment, check_start
from check_chat_runner import ALPINE, REPO_ROOT, Check, check, wait, within

from social_data_pipeline import chat_host as host
from social_data_pipeline.control.client import Client

TARGET = "warehouse"
DONE_ID = "pg_c4c4c4c4"
MODEL = "standin/model-for-the-check:free"
MODEL_PORT, RECORDER_PORT = 9090, 8090
JOB_ID = re.compile(r"pg_[0-9a-f]{8}")
# The gateway as `python -m social_data_pipeline.gateway` runs it (each listener's secret read from its file), with its loopback test upstream for the model (for `openai-compatible` also its base path there).
GATEWAY = f"""import dataclasses, signal, sys, threading
from social_data_pipeline.gateway.server import Gateway
from social_data_pipeline.gateway.table import MODEL_OPENAI, Upstream, load, read_credential, read_secret
table = load("/run/sdp-gateway/config.json")
local = Upstream("http", "127.0.0.1", {MODEL_PORT}, "/api/v1")
table = dataclasses.replace(table, listeners=tuple(dataclasses.replace(lst, upstream=local) if lst.kind == MODEL_OPENAI else lst for lst in table.listeners))
secrets = [read_secret(lst.secret_file, "secret_file") if lst.secret_file else "" for lst in table.listeners]
gateway = Gateway(table, read_credential(table.credential_file), secrets=secrets, test_upstream=Upstream("http", "127.0.0.1", {MODEL_PORT}))
gateway.start()
for lst, port in zip(table.listeners, gateway.ports):
    print(f"gateway: {{lst.kind}} listening on port {{port}}", flush=True)
stop = threading.Event()
signal.signal(signal.SIGTERM, lambda *_: stop.set())
stop.wait()
gateway.stop()
"""
# Between the gateway and the scheduler: one request per connection (the gateway sends `Connection: close`), its head written down by name and passed on with everything after it.
RECORDER = r'''
import json, socket, sys, threading
lock = threading.Lock()
def pipe(a, b):
    try:
        while True:
            data = a.recv(65536)
            if not data:
                break
            b.sendall(data)
    except OSError:
        pass
    for s in (a, b):
        try:
            s.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
def handle(client):
    head = b""
    while b"\r\n\r\n" not in head:
        data = client.recv(65536)
        if not data:
            return client.close()
        head += data
    lines = head.split(b"\r\n\r\n", 1)[0].decode("latin-1").split("\r\n")
    method, target, _ = lines[0].split(" ")
    with lock, open("/record/forwarded.jsonl", "a") as f:
        f.write(json.dumps({"method": method, "path": target.partition("?")[0], "query": bool(target.partition("?")[2]), "headers": sorted(line.split(":", 1)[0].lower() for line in lines[1:])}) + "\n")
    up = socket.create_connection(("127.0.0.1", int(sys.argv[2])))
    up.sendall(head)
    threading.Thread(target=pipe, args=(up, client), daemon=True).start()
    pipe(client, up)
server = socket.create_server(("0.0.0.0", int(sys.argv[1])))
while True:
    threading.Thread(target=handle, args=(server.accept()[0],), daemon=True).start()
'''
STANDIN = r'''
import http.server, json, re, sys, threading
TARGET, DONE_ID = sys.argv[2], sys.argv[3]
# The questions whose tool call was given: the next request of the turn gets the text (the shape of a request after a tool's result differs between the back ends and Claude Code's turns).
served, lock = set(), threading.Lock()
FETCH = "python3 -c \"import json, os, urllib.request as u; base = 'http://' + os.environ['SDP_CHAT_GATEWAY'] + ':8004/assistant/results/" + DONE_ID + "'; parts = json.load(u.urlopen(base))['parts']; open('part0.csv', 'wb').write(u.urlopen(base + '/0').read()); print('parts', len(parts), parts[0]['name'])\""
def texts(content):
    return content if isinstance(content, str) else " ".join(b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text")
def decide(said, after_result, anthropic):
    """(kind, name, input): a tool call for the turn's marker, a text after a tool's result."""
    prefix, shell = ("mcp__jobs__", "Bash") if anthropic else ("jobs__", "run_shell")
    with lock:
        if after_result or said in served:
            return "text", None, "done"
        served.add(said)
    if "SUBMIT-TURN" in said:
        return "tool", prefix + "submit_postgres_query", {"sql": "SELECT 42 AS answer", "target": TARGET, "output_filename": "answer.csv", "description": "a query of the assistant in the check"}
    found = re.search(r"STATUS-TURN (pg_[0-9a-f]{8})", said)
    if found:
        return "tool", prefix + "query_status", {"job_id": found.group(1)}
    if "FETCH-TURN" in said:
        return "tool", shell, {"command": FETCH, **({"description": "fetch the result"} if anthropic else {})}
    return "text", None, "plain answer"
def sse(kind, name, value):
    events = [("message_start", {"type": "message_start", "message": {"id": "msg_standin", "type": "message", "role": "assistant", "model": "claude-standin", "content": [], "stop_reason": None, "stop_sequence": None, "usage": {"input_tokens": 10, "output_tokens": 1}}})]
    if kind == "text":
        events += [("content_block_start", {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}), ("content_block_delta", {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": value}})]
    else:
        events += [("content_block_start", {"type": "content_block_start", "index": 0, "content_block": {"type": "tool_use", "id": "toolu_1", "name": name, "input": {}}}), ("content_block_delta", {"type": "content_block_delta", "index": 0, "delta": {"type": "input_json_delta", "partial_json": json.dumps(value)}})]
    events += [("content_block_stop", {"type": "content_block_stop", "index": 0}), ("message_delta", {"type": "message_delta", "delta": {"stop_reason": "tool_use" if kind == "tool" else "end_turn", "stop_sequence": None}, "usage": {"output_tokens": 5}}), ("message_stop", {"type": "message_stop"})]
    return "".join(f"event: {e}\ndata: {json.dumps(d)}\n\n" for e, d in events).encode()
class Model(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *a):
        pass
    def do_POST(self):
        doc = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)))
        messages = doc.get("messages") or []
        anthropic = self.path.startswith("/v1/")
        if anthropic:
            users = [m for m in messages if m.get("role") == "user"]
            last = users[-1]["content"] if users else ""
            after = isinstance(last, list) and any(isinstance(b, dict) and b.get("type") == "tool_result" for b in last)
            said = texts(last)
        else:
            after = bool(messages) and messages[-1]["role"] == "tool"
            said = [m["content"] for m in messages if m["role"] == "user"][-1]
        kind, name, value = decide(said, after, anthropic)
        tools = sorted(t.get("name") or t.get("function", {}).get("name") for t in doc.get("tools") or [])
        with open("/standin/record.jsonl", "a") as f:
            f.write(json.dumps({"path": self.path, "said": said[-200:], "after": after, "answer": [kind, name], "last": [[m.get("role"), [b.get("type") for b in m["content"]] if isinstance(m.get("content"), list) else type(m.get("content")).__name__] for m in messages[-3:]], "tools": tools, "paragraph": "Queue window of the web UI" in json.dumps(doc.get("system") if anthropic else messages[:1])}) + "\n")
        if anthropic:
            data, kind_header = sse(kind, name, value), "text/event-stream"
        else:
            message = {"role": "assistant", "content": value if kind == "text" else None}
            if kind == "tool":
                message["tool_calls"] = [{"id": "call_1", "type": "function", "function": {"name": name, "arguments": json.dumps(value)}}]
            data, kind_header = json.dumps({"id": "chatcmpl-standin", "object": "chat.completion", "choices": [{"index": 0, "message": message, "finish_reason": "tool_calls" if kind == "tool" else "stop"}]}).encode(), "application/json"
        self.send_response(200)
        self.send_header("Content-Type", kind_header)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
server = http.server.ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), Model)
server.daemon_threads = True
print("stand-in ready", flush=True)
server.serve_forever()
'''


def lines_of(path):
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def set_up(d, c):
    print("0. the scheduler, the images, the chat's networks, the scheduler attached, the recorder")
    check_start(d)
    secret = host._queue_secret(str(d.root))
    host._write_gateway_secret(str(c.root), secret)
    check(True, "the scheduler's secret made by chat start's own code and copied into the gateway's folder")
    for arguments in host.build_arguments(str(REPO_ROOT)):
        result = c.docker(*arguments[1:], timeout=3600)
        check(result.returncode == 0, f"built {arguments[arguments.index('-t') + 1]}" + ("" if result.returncode == 0 else ": " + result.stderr.strip()[-1500:]))
    address = host._make_networks(str(c.root), c.networks)
    c.step("network", "connect", "--alias", host.QUEUE_ALIAS, host.UP_NETWORK, d.container())
    (c.root / "record").mkdir(mode=0o700)
    recorder = f"{d.name}-recorder"
    d.runs.append(recorder)
    c.step("run", "-d", "--name", recorder, "--network", f"container:{d.container()}", "--user", f"{os.getuid()}:{os.getgid()}", "--mount", f"type=bind,source={c.root / 'record'},target=/record", ALPINE, "python", "-c", RECORDER, str(RECORDER_PORT), str(d.port))
    check(True, f"the scheduler is attached to {host.UP_NETWORK} as {host.QUEUE_ALIAS}; the recorder listens on {RECORDER_PORT} in its network namespace")
    code = (
        "import time\n"
        "from social_data_pipeline.jobs.store import Job, Store\n"
        f"job = Job(job_id={DONE_ID!r}, target={TARGET!r}, backend='postgres', sql='SELECT 3', output_filename='out.csv', overwrite=False, submitted_at=time.time(), status='running', started_at=time.time(), submitted_by='assistant')\n"
        "job.result_path = '/data/jobs'\n"
        "Store('/data/jobs').complete(job)\n"
        "print('ok')\n"
    )
    check(d.in_container(code).strip() == "ok", f"a finished marked job {DONE_ID} written with the store's own code inside the scheduler's container")
    data = os.urandom(70000) + b"\nend\n"
    (d.jobs / "results" / DONE_ID).mkdir()
    (d.jobs / "results" / DONE_ID / "out.csv").write_bytes(data)
    return address, secret, data


def start_chat(c, address, backend):
    for name in (c.sandbox, c.standin, c.gateway):
        c.docker("rm", "--force", name)
    if c.socket.exists():
        c.socket.unlink()
    settings = {"backend": backend} if backend == host.CLAUDE_CODE else {"backend": backend, "upstream": "https://openrouter.ai/api/v1", "model": MODEL}
    host._write_gateway_config(str(c.root), host.gateway_config(address, settings, [], queue_port=RECORDER_PORT))
    c.step(*host.gateway_arguments(str(c.root), os.getuid(), os.getgid(), address)[1:], "python", "-c", GATEWAY)
    c.step("network", "connect", host.UP_NETWORK, c.gateway)
    c.step("start", c.gateway)
    deadline = time.monotonic() + 30
    while c.docker("logs", c.gateway).stdout.count(" listening on port ") < 2:
        within(deadline, "the gateway listens")
        time.sleep(0.2)
    c.step("run", "-d", "--name", c.standin, "--network", f"container:{c.gateway}", "--user", f"{os.getuid()}:{os.getgid()}", "--mount", f"type=bind,source={c.root / 'standin'},target=/standin", ALPINE, "python", "-c", STANDIN, str(MODEL_PORT), TARGET, DONE_ID)
    c.step(*host.sandbox_arguments(str(c.root), os.getuid(), os.getgid(), address, backend, [], 10, MODEL, queue=True)[1:])
    c.step("start", c.sandbox)
    deadline = time.monotonic() + 30
    while not c.socket.exists():
        within(deadline, "the runner's socket appears")
        time.sleep(0.2)
    check(True, f"{backend}: the gateway (model and queue listeners), the stand-in and the sandbox with the queue run")


def turn(c, client, chat, text, start):
    client.request({"op": "chat.send", "chat": chat, "text": text})
    try:
        events = wait(client, chat, 300)[start:]
    except AssertionError:
        print("    the turn did not end: the stand-in saw " + json.dumps([{k: r[k] for k in ("said", "after", "answer", "last")} for r in lines_of(c.record)[-6:]]))
        print("    the chat's events: " + json.dumps([(e["type"], e.get("name"), (e.get("text") or e.get("input") or "")[:300]) for e in client.request({"op": "chat.events", "chat": chat, "offset": start, "limit": 50})["events"]]))
        print("    the gateway's log: " + " / ".join(c.docker("logs", c.gateway).stdout.splitlines()[-6:]))
        raise
    print(f"    {text.split()[0]}: {[(e['type'], e.get('name') or (e.get('text') or '')[:90]) for e in events]}")
    return events


def check_back_end(d, c, address, data, backend):
    shell, prefix = ("Bash", "mcp__jobs__") if backend == host.CLAUDE_CODE else ("run_shell", "jobs__")
    print(f"{'1' if backend == host.CLAUDE_CODE else '2'}. the back end {backend}")
    start_chat(c, address, backend)
    for path in (c.record, c.root / "record" / "forwarded.jsonl"):
        path.unlink(missing_ok=True)
    client = Client(str(c.socket), timeout=15)
    hello = client.request({"op": "hello"})
    check(hello == {"version": 1, "backend": backend, "databases": [], "queue": True}, f"hello: {hello}")
    found = c.step("exec", c.sandbox, "sh", "-c", "command -v curl wget || echo none").strip()
    check(found == "none", f"neither curl nor wget is in the sandbox image ({found})")
    chat = client.request({"op": "chat.new"})["chat"]

    events = turn(c, client, chat, "SUBMIT-TURN run the heavy query", 0)
    check([e["type"] for e in events] == ["user", "tool", "tool_result", "text", "end"] and events[1]["name"] == prefix + "submit_postgres_query" and events[2]["is_error"] is False, "the submit tool was called and answered")
    job_id = JOB_ID.search(events[2]["text"]).group(0)
    phase, record = d.record(job_id)
    check(phase == "pending" and record.get("submitted_by") == "assistant", f"its result names {job_id}, whose record on disk is pending and carries submitted_by: assistant")
    seen = lines_of(c.record)
    check(seen and all(r["paragraph"] for r in seen) and any(prefix + "submit_postgres_query" in r["tools"] for r in seen), f"{len(seen)} model requests, each with the role text's paragraph, the queue's tools offered")

    events = turn(c, client, chat, f"STATUS-TURN {job_id} where is it", 5)
    check(events[1]["name"] == prefix + "query_status" and json.loads(events[2]["text"]).get("status") == "pending", f"query_status answers pending: {events[2]['text'][:200]}")

    events = turn(c, client, chat, "FETCH-TURN save the result", 10)
    kinds = [e["type"] for e in events]
    check(kinds == ["user", "tool", "tool_result", "text", "file", "end"] and events[1]["name"] == shell and "parts 1 out.csv" in events[2]["text"], f"the {shell} call listed the parts and saved part 0: {events[2]['text'][:200]!r}")
    saved = (c.work / chat / "files" / "part0.csv").read_bytes()
    check(events[4]["name"] == "part0.csv" and events[4]["size"] == len(data) and saved == data, f"the file event names part0.csv and its {len(saved)} bytes equal the part's")

    logs = c.docker("logs", c.gateway)
    lines = [line for line in (logs.stdout + logs.stderr).splitlines() if " method=" in line]
    refused = [line for line in lines if not line.endswith(("reason=-", "reason=client_gone"))]
    check(not refused, f"the gateway refused nothing ({len(lines)} lines in its log)" + ("" if not refused else f": {refused[:5]}"))
    forwarded = lines_of(c.root / "record" / "forwarded.jsonl")
    check(forwarded, f"{len(forwarded)} requests reached the scheduler through the gateway's queue listener")
    for method, path, query, headers in sorted({(r["method"], re.sub(r"/pg_[0-9a-f]{8}", "/<job id>", r["path"]), r["query"], tuple(r["headers"])) for r in forwarded}):
        print(f"    forwarded {method} {path}{'?<query>' if query else ''}: {', '.join(headers)}")
    client.request({"op": "chat.delete", "chat": chat})


def main():
    d, c = Deployment(), Check()
    failed = None
    try:
        try:
            print(f"compose project {d.name}, chat names *-check-{c.images[0].rsplit('-', 1)[1]}, Docker {c.docker('version', '--format', '{{.Server.Version}}').stdout.strip()}")
            address, secret, data = set_up(d, c)
            check_back_end(d, c, address, data, host.CLAUDE_CODE)
            check_back_end(d, c, address, data, host.OPENAI_COMPATIBLE)
            logs = d.compose("logs", "jobs").stdout + c.docker("logs", c.gateway).stdout + c.docker("logs", c.gateway).stderr
            check(secret not in logs and secret not in (c.root / "record" / "forwarded.jsonl").read_text(), "the secret is in no log line and in no record of the recorder")
        except Exception as error:
            failed = error
    finally:
        left = d.clean_up()
        chat_left = c.clean_up()
    containers, networks, images, folder = left
    if containers or networks or images or folder or chat_left:
        print(f"FAILED: left behind: containers {containers}, networks {networks}, images {images}, temp folder {folder}, chat {chat_left}")
        return 1
    print("  ok: no container, network or image of the check is left, and both temp folders are deleted")
    if failed is not None:
        print(f"FAILED: {type(failed).__name__}: {failed}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
