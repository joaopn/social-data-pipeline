#!/usr/bin/env python3
"""A check of the chat's OpenAI-compatible back end in the real sandbox image, through the real gateway's rules, run by hand: `python tests/e2e/manual/check_chat_openai.py` (what it needs, the throwaway names and the clean-up are those of `check_chat_runner.py`, whose helpers it uses).

It is not collected by pytest. The gateway's config is written as `sdp chat start` writes it for `openai-compatible` (`chat_host.gateway_config`, with `https://openrouter.ai/api/v1` as the address, never reached) and with the MCP servers of postgres and mongo; the gateway container runs the gateway package with its model upstream replaced by a loopback `http://` address (its test-only `test_upstream` allows that) and its MCP upstreams pointed at loopback, where a STAND-IN runs in its network namespace: `tests/chat/standins.py` (mounted read-only) as a chat-completions endpoint and as one MCP server for both databases (SSE for postgres, streamable HTTP for mongo), recording every request. The sandbox is created by `chat_host.sandbox_arguments`, so it runs the runner and its agent as `sdp chat start` would. What it shows:

1. `hello` names the back end and both databases; a question whose first reply asks for `run_shell` writing `plot.png` and whose second is text gives the events `user`, `tool`, `tool_result`, `text`, `file`, `end`.
2. A second question resumes the history (the stand-in sees the first question in its first request) and calls `mongo__ping` through the gateway; the tools offered hold `run_shell` and the tools of both databases.
3. `chat.stop` on a turn whose shell command waits leaves no process of the turn in the sandbox (`docker top`).
4. The gateway refused nothing; every model request carried the configured model and reached the stand-in with the made-up key the gateway holds, and no MCP request carried an `Authorization` header. The `mcp` package's version in the image and the MCP requests' paths and header names as the gateway forwarded them are printed. `chat.delete` removes the chat's folder.
"""

import os
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from check_chat_runner import ALPINE, REPO_ROOT, TOKEN, Check, check, wait, within

from social_data_pipeline import chat_host as host
from social_data_pipeline.control.client import Client

MODEL = "standin/model-for-the-check:free"
MODEL_PORT, MCP_PORT = 9090, 3000
MARKER = "MARKER-5c2e"
GATEWAY = f"""import dataclasses, signal, threading
from social_data_pipeline.gateway.server import Gateway
from social_data_pipeline.gateway.table import MODEL_OPENAI, Upstream, load, read_credential
table = load("/run/sdp-gateway/config.json")
local = Upstream("http", "127.0.0.1", {MODEL_PORT}, "/api/v1")
table = dataclasses.replace(table, listeners=tuple(dataclasses.replace(lst, upstream=local) if lst.kind == MODEL_OPENAI else lst for lst in table.listeners))
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
import json, sys, threading
sys.path.insert(0, "/code")
import standins
from standins import reply, tool_call
lock = threading.Lock()
def record(entry):
    with lock, open("/standin/record.jsonl", "a") as f:
        f.write(json.dumps(entry) + "\n")
def answer(doc):
    entry = model.seen[-1]
    messages = doc["messages"]
    users = [m["content"] for m in messages if m["role"] == "user"]
    record({"path": entry["path"], "authorization": entry["headers"].get("authorization"), "headers": sorted(entry["headers"]), "model": doc.get("model"), "keys": sorted(doc), "tools": [t["function"]["name"] for t in doc.get("tools", [])], "said": users[-1], "all_said": " | ".join(users), "roles": [m["role"] for m in messages]})
    if messages[-1]["role"] == "tool":
        return 200, reply("Here is the plot: plot.png" if "PLOT-TURN" in users[-1] else "mongo says: " + messages[-1]["content"])
    if "PLOT-TURN" in users[-1]:
        return 200, reply(calls=[tool_call("run_shell", {"command": "python3 -c \"open('plot.png','wb').write(bytes([0x89,0x50,0x4e,0x47,0x0d,0x0a,0x1a,0x0a])+b'standin')\" && echo MARKER-5c2e"})])
    if "MCP-TURN" in users[-1]:
        return 200, reply(calls=[tool_call("mongo__ping", {})])
    if "HOLD-TURN" in users[-1]:
        return 200, reply(calls=[tool_call("run_shell", {"command": "sleep 600 & sleep 600; wait"})])
    return 200, reply("plain answer")
class Mcp(standins.Mcp):
    def note(self, rpc=None):
        record({"mcp": rpc, "method": self.command, "path": self.path, "headers": sorted(k.lower() for k in self.headers)})
model = standins.serve(standins.Model, int(sys.argv[1]), answer=answer)
standins.serve(Mcp, int(sys.argv[2]), tools=standins.ping_tools("mongo"), call=standins.ping_call("mongo"))
print("stand-in ready", flush=True)
threading.Event().wait()
'''


def start(c):
    print("0. images, networks, the gateway with a stand-in upstream, the sandbox")
    for arguments in host.build_arguments(str(REPO_ROOT)):
        result = c.docker(*arguments[1:], timeout=3600)
        check(result.returncode == 0, f"built {arguments[arguments.index('-t') + 1]}" + ("" if result.returncode == 0 else ": " + result.stderr.strip()[-1500:]))
    address = host._make_networks(str(c.root), c.networks)
    databases = [db for db in host.DATABASES if db.name in ("postgres", "mongo")]
    config = host.gateway_config(address, {"backend": host.OPENAI_COMPATIBLE, "upstream": "https://openrouter.ai/api/v1", "model": MODEL}, databases)
    for listener in config["listeners"][1:]:
        listener["upstream"] = f"http://127.0.0.1:{MCP_PORT}"
    host._write_gateway_config(str(c.root), config)
    c.step(*host.gateway_arguments(str(c.root), os.getuid(), os.getgid(), address)[1:], "python", "-c", GATEWAY)
    c.step("start", c.gateway)
    deadline = time.monotonic() + 30
    while c.docker("logs", c.gateway).stdout.count(" listening on port ") < 3:
        within(deadline, "the gateway listens")
        time.sleep(0.2)
    c.step("run", "-d", "--name", c.standin, "--network", f"container:{c.gateway}", "--user", f"{os.getuid()}:{os.getgid()}", "--mount", f"type=bind,source={c.root / 'standin'},target=/standin",
           "--mount", f"type=bind,source={REPO_ROOT / 'tests' / 'chat'},target=/code,readonly", ALPINE, "python", "-c", STANDIN, str(MODEL_PORT), str(MCP_PORT))
    c.step(*host.sandbox_arguments(str(c.root), os.getuid(), os.getgid(), address, host.OPENAI_COMPATIBLE, [db.name for db in databases], 10, MODEL)[1:])
    c.step("start", c.sandbox)
    deadline = time.monotonic() + 30
    while not c.socket.exists():
        within(deadline, "the runner's socket appears")
        time.sleep(0.2)
    version = c.step("exec", c.sandbox, "python", "-c", "import importlib.metadata as m; print(m.version('mcp'))").strip()
    print(f"  ok: the gateway listens, the stand-in and the sandbox run; the mcp package in the image: {version}")


def check_turns(c, client):
    print("1. hello, a new chat, a turn that writes a plot")
    hello = client.request({"op": "hello"})
    check(hello == {"version": 1, "backend": "openai-compatible", "databases": ["postgres", "mongo"]}, f"hello: {hello}")
    chat = client.request({"op": "chat.new"})["chat"]
    client.request({"op": "chat.send", "chat": chat, "text": "PLOT-TURN draw the counts"})
    events = wait(client, chat)
    print(f"    events: {[e['type'] for e in events]}")
    check([e["type"] for e in events] == ["user", "tool", "tool_result", "text", "file", "end"], "the events are user, tool, tool_result, text, file, end")
    check(events[1]["name"] == "run_shell" and events[2]["text"] == f"{MARKER}\n[exit code 0]" and events[4]["name"] == "plot.png" and events[5]["is_error"] is False, f"run_shell wrote plot.png: {events[2]['text']!r}, {events[5]}")
    print("    the agent's log: " + ((c.work / chat / "backend.log").read_text().strip().replace("\n", " / ") or "(empty)"))

    print("2. a second question resumes the history and calls the mongo tool")
    before = len(c.requests())
    client.request({"op": "chat.send", "chat": chat, "text": "MCP-TURN ask mongo"})
    events = wait(client, chat)[6:]
    check([e["type"] for e in events] == ["user", "tool", "tool_result", "text", "end"] and events[1]["name"] == "mongo__ping" and events[2]["text"] == "pong-from-mongo", f"mongo__ping answered through the gateway: {[(e['type'], e.get('name') or e.get('text')) for e in events]}")
    first = next(r for r in c.requests()[before:] if "said" in r)
    check("PLOT-TURN draw the counts" in first["all_said"] and first["roles"][:5] == ["system", "user", "assistant", "tool", "assistant"], f"the second question's first request carries the first turn: {first['roles']}")
    check(first["tools"] == ["run_shell", "postgres__ping", "mongo__ping"], f"the tools offered: {first['tools']}")

    print("3. stop a turn whose shell command waits")
    client.request({"op": "chat.send", "chat": chat, "text": "HOLD-TURN wait"})
    deadline = time.monotonic() + 60
    while "sleep 600" not in c.step("top", c.sandbox, "-o", "pid,args"):
        within(deadline, "the shell command runs")
        time.sleep(0.2)
    check(client.request({"op": "chat.stop", "chat": chat}) == {"stopping": True}, "the shell command runs; chat.stop")
    events = wait(client, chat, 30)
    check([e["type"] for e in events[-2:]] == ["error", "end"] and events[-2]["message"] == "the turn was stopped", f"the turn ends with the stopped error: {events[-2:]}")
    time.sleep(0.5)
    top = c.step("top", c.sandbox, "-o", "pid,args").strip().splitlines()[1:]
    print("    | " + "\n    | ".join(top))
    check(len(top) == 2 and any("docker-init" in line for line in top) and any("social_data_pipeline.chat" in line for line in top), "no process of the turn is left in the sandbox (only its init and the runner)")
    return chat


def check_gateway(c, client, chat):
    print("4. what passed the gateway, and delete")
    seen = c.requests()
    model = [r for r in seen if "said" in r]
    mcp = [r for r in seen if "mcp" in r]
    logs = c.docker("logs", c.gateway)
    lines = [line for line in (logs.stdout + logs.stderr).splitlines() if " method=" in line]
    refused = [line for line in lines if not line.endswith(("reason=-", "reason=client_gone"))]
    check(not refused, f"the gateway refused nothing ({len(lines)} lines in its log)" + ("" if not refused else f": {refused[:3]}"))
    check(model and all(r["path"] == "/api/v1/chat/completions" and r["model"] == MODEL for r in model), f"{len(model)} model requests, each POST /api/v1/chat/completions with the model {MODEL}")
    check(all(r["authorization"] == f"Bearer {TOKEN}" for r in model), "every model request reached the stand-in with the made-up key the gateway holds")
    check(mcp and not any("authorization" in r["headers"] for r in mcp), f"{len(mcp)} MCP requests, none with an Authorization header")
    print(f"    model request body keys: {sorted({k for r in model for k in r['keys']})}; header names: {sorted({h for r in model for h in r['headers']})}")
    for method, path, headers in sorted({(r["method"], re.sub(r"=[^&]*", "=<id>", r["path"]), tuple(r["headers"])) for r in mcp}):
        print(f"    MCP {method} {path}: {', '.join(headers)}")
    print(f"    MCP methods: {sorted({r['mcp'] for r in mcp if r['mcp']})}")
    check(client.request({"op": "chat.delete", "chat": chat}) == {} and not (c.work / chat).exists(), "chat.delete removes the chat's folder")


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
