#!/usr/bin/env python3
"""A check of the query scheduler's assistant endpoint and of the gateway's queue listener against a real Docker, run by hand: `python tests/e2e/manual/check_chat_queue.py` (Linux; a Docker with the compose plugin and without user-id mapping that can pull `python:3.13-slim` and reach PyPI, unpkg.com, cdn.jsdelivr.net and the vendor's download of Claude Code, and the image `python:3.13-alpine`, which is not pulled). Standard library and this package only.

It is not collected by pytest. It builds a throwaway deployment in a temp folder as `check_door.py` does (a copy of `docker-compose.yml`, `Dockerfile.jobs`, `requirements-jobs.txt`, the package and the scheduler's committed configuration, a `config/jobs/config.local.yaml` with one PostgreSQL and one MongoDB target on a free port and no database, `data/jobs` made as the current user), under a throwaway compose project name (`sdp-queue-check-<hex>`), and builds the gateway's and the sandbox's images from `Dockerfile.gateway` and `Dockerfile.sandbox` under throwaway names. What it shows:

1. The scheduler's image builds and its `jobs` service starts; its page answers; the control folder is there.
2. `/mcp` lists its tools as before (`list_mongo_databases` among them), and a query submitted through it (plain HTTP, as before) has no `submitted_by` in its record.
3. On the scheduler's published port, `/assistant/mcp` and `/assistant/results/x` answer the page's own 404 (the same bytes as a path it does not have) before the secret file exists, then without the header and with a wrong secret; the secret file is made by `sdp chat start`'s own code (`chat_host._queue_secret`), mode 0600.
4. A gateway container on a throwaway network, with an `mcp` listener with the secret file (port 8004) and one without (port 8005) to the scheduler attached to that network under the alias `sdp-chat-jobs`. A helper container from the sandbox image (its `mcp` client), on that network as the current user, playing the sandbox: the assistant's MCP server lists its tools (without `list_mongo_databases`), a query is submitted (its record on disk carries the mark), `query_status` answers for it and "no such job" for the job of step 2 and for a made-up and a malformed id; `query_cancel` of the job of step 2 is "no such job" and the job stays pending.
5. With auto-accept switched on for the target through the control socket, the auto-accept thread approves the marked job like any other, and a new unmarked one after it.
6. Finished jobs written with the store's own code inside the container (`Store.complete`) and their files by hand: the marked one's part list and part are fetched byte for byte through the gateway; an unmarked one's part and list are 404; a link planted as a part of a marked job is not listed and its marker is in no answer. The helper sending the right secret itself as `x-sdp-assistant` through the listener without a secret file does not get in (404).
7. The scheduler's own page still answers.

Clean-up, also when the check is interrupted: the compose project is taken down, the gateway and helper containers and the network are removed, the three images are removed, what the scheduler made as root is removed by a root container, and the temp folder is deleted. What stays: the base image `python:3.13-slim` and Docker's build cache.
"""

import hashlib
import json
import os
import re
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from social_data_pipeline import chat_host as host
from social_data_pipeline.control import client, files

RUN_IMAGE = "python:3.13-alpine"
MARKER = "queue-check-marker-3b9d"
TARGET = "warehouse"
MONGO_TARGET = "docstore"
BY = "web-4343"
DONE_ID = "pg_d1d1d1d1"
PLAIN_ID = "pg_e2e2e2e2"
LINKED_ID = "pg_f3f3f3f3"
WRONG = "0f" * 32
# Run in the helper: an MCP session through the gateway (its task on standard input), one JSON line out.
SESSION = r'''
import asyncio, json, sys
spec = json.loads(sys.stdin.read())

async def main():
    from mcp import ClientSession
    from mcp.client.streamable_http import streamable_http_client
    out = {}
    async with streamable_http_client(spec["url"]) as streams, ClientSession(streams[0], streams[1]) as session:
        await session.initialize()
        out["tools"] = sorted(tool.name for tool in (await session.list_tools()).tools)

        async def call(name, arguments):
            result = await session.call_tool(name, arguments)
            return json.loads(result.content[0].text)

        submitted = await call("submit_postgres_query", {"sql": "SELECT 7 AS seven", "target": spec["target"], "output_filename": "seven.csv", "description": "a query of the assistant"})
        out["submitted"] = submitted
        out["own"] = await call("query_status", {"job_id": submitted["job_id"]})
        out["others"] = [await call("query_status", {"job_id": job_id}) for job_id in spec["others"]]
        out["cancel_other"] = await call("query_cancel", {"job_id": spec["others"][0]})
        out["targets"] = await call("list_targets", {})
    print(json.dumps(out))

asyncio.run(main())
'''
# Run in the helper: plain HTTP requests through the gateway, one JSON line out (bodies as hex).
FETCH = r'''
import http.client, json, sys
spec = json.loads(sys.stdin.read())
out = []
for port, method, path, headers in spec["requests"]:
    try:
        c = http.client.HTTPConnection(spec["gateway"], port, timeout=30)
        body = b"{}" if method == "POST" else None
        c.request(method, path, body=body, headers={**headers, **({"Content-Type": "application/json", "Accept": "application/json, text/event-stream"} if body else {})})
        r = c.getresponse()
        out.append([r.status, r.getheader("Content-Type"), r.read().hex()])
    except OSError as e:
        out.append([0, type(e).__name__, ""])
print(json.dumps(out))
'''


def check(condition, what):
    if not condition:
        raise AssertionError(what)
    print(f"  ok: {what}")


def wait_for(what, probe, seconds=120):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        try:
            found = probe()
        except Exception:
            found = None
        if found:
            return found
        time.sleep(0.5)
    raise AssertionError(f"timed out waiting for {what}")


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def docker(*argv, timeout=300, stdin=None):
    return subprocess.run(["docker", *argv], capture_output=True, text=True, timeout=timeout, input=stdin)


class Deployment:
    def __init__(self):
        self.root = Path(tempfile.mkdtemp(prefix="sdp-queue-check-")).resolve()
        os.chmod(self.root, 0o755)
        self.name = "sdp-queue-check-" + os.urandom(4).hex()
        self.image = f"{self.name}-jobs"
        self.gateway_image, self.sandbox_image = f"{self.name}-gateway-image", f"{self.name}-sandbox-image"
        self.gateway, self.network = f"{self.name}-gateway", f"{self.name}-net"
        self.port = free_port()
        self.runs = []
        for name in ("docker-compose.yml", "Dockerfile.jobs", "requirements-jobs.txt", "requirements-sandbox.txt", host.GATEWAY_DOCKERFILE, host.SANDBOX_DOCKERFILE, f"{host.GATEWAY_DOCKERFILE}.dockerignore", f"{host.SANDBOX_DOCKERFILE}.dockerignore"):
            shutil.copy(REPO_ROOT / name, self.root / name)
        shutil.copytree(REPO_ROOT / "social_data_pipeline", self.root / "social_data_pipeline", ignore=shutil.ignore_patterns("__pycache__"))
        (self.root / "config" / "jobs").mkdir(parents=True)
        for name in ("config.yaml", "entrypoint.sh"):
            shutil.copy(REPO_ROOT / "config" / "jobs" / name, self.root / "config" / "jobs" / name)
        (self.root / "config" / "jobs" / "config.local.yaml").write_text(f"port: {self.port}\nmax_concurrent: 1\nauth: false\ntargets:\n  {TARGET}:\n    backend: postgres\n    database: datasets\n  {MONGO_TARGET}:\n    backend: mongodb\n")
        self.jobs = self.root / "data" / "jobs"
        (self.jobs / "results").mkdir(parents=True)
        os.chmod(self.jobs, 0o755)
        os.chmod(self.jobs / "results", 0o755)
        self.gateway_folder = self.root / "gateway"
        self.gateway_folder.mkdir(mode=0o700)
        self.environment = {**os.environ, "COMPOSE_PROJECT_NAME": self.name, "JOBS_PORT": str(self.port)}

    def compose(self, *argv, timeout=1800):
        return subprocess.run(["docker", "compose", "--project-name", self.name, "--profile", "jobs", *argv], cwd=self.root, env=self.environment, capture_output=True, text=True, timeout=timeout)

    def door(self, document):
        return client.Client(str(self.jobs / "control" / "door.sock"), timeout=30).request(document)

    def http(self, path, headers=None, method="GET"):
        """(status, body) of a request to the scheduler's published port."""
        data = b"{}" if method == "POST" else None
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, headers={"Content-Type": "application/json", "Accept": "application/json, text/event-stream", **(headers or {})}, method=method)
        try:
            with urllib.request.urlopen(request, timeout=10) as answer:
                return answer.status, answer.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()

    def container(self):
        result = docker("ps", "-q", "--no-trunc", "--filter", f"label=com.docker.compose.project={self.name}", "--filter", "label=com.docker.compose.service=jobs", timeout=30)
        return result.stdout.strip()

    def in_container(self, code):
        result = docker("exec", self.container(), "python", "-c", code, timeout=60)
        if result.returncode != 0:
            raise AssertionError(f"docker exec failed: {result.stderr.strip().splitlines()[-1:]}")
        return result.stdout

    def record(self, job_id):
        """(phase, record) of a job, its file read as it is on the disk (inside the container: the scheduler writes its records 0600 as root)."""
        code = (
            "import json, os\n"
            f"job_id = {job_id!r}\n"
            "for phase in ('pending', 'approved', 'running'):\n"
            "    path = f'/data/jobs/{phase}/{job_id}.json'\n"
            "    if os.path.exists(path):\n"
            "        print(json.dumps([phase, json.load(open(path))])); raise SystemExit\n"
            "for line in reversed(open('/data/jobs/history.jsonl').read().splitlines()):\n"
            "    if line.strip() and json.loads(line)['job_id'] == job_id:\n"
            "        print(json.dumps(['history', json.loads(line)])); raise SystemExit\n"
            "print(json.dumps([None, None]))\n"
        )
        return tuple(json.loads(self.in_container(code)))

    def helper(self, code, spec, label):
        name = f"{self.name}-{label}"
        self.runs.append(name)
        result = docker("run", "--rm", "-i", "--pull", "never", "--name", name, "--network", self.network, "--user", f"{os.getuid()}:{os.getgid()}", "--read-only", "--tmpfs", "/tmp", "-e", "HOME=/tmp", self.sandbox_image, "python", "-c", code, stdin=json.dumps(spec), timeout=300)
        if result.returncode != 0:
            raise AssertionError(f"the helper failed: {result.stderr.strip()[-1500:]}")
        return json.loads(result.stdout.strip().splitlines()[-1])

    def leftovers(self):
        containers = docker("ps", "-a", "-q", "--filter", f"label=com.docker.compose.project={self.name}", timeout=30).stdout.split()
        for name in [self.gateway, *self.runs]:
            if docker("container", "inspect", name, timeout=30).returncode == 0:
                containers.append(name)
        networks = docker("network", "ls", "-q", "--filter", f"label=com.docker.compose.project={self.name}", timeout=30).stdout.split()
        if docker("network", "inspect", self.network, timeout=30).returncode == 0:
            networks.append(self.network)
        images = [image for image in (self.image, self.gateway_image, self.sandbox_image) if docker("image", "inspect", image, timeout=30).returncode == 0]
        return containers, networks, images, self.root.exists()

    def clean_up(self):
        for name in [self.gateway, *self.runs]:
            docker("rm", "--force", name, timeout=60)
        container = self.container()
        if container:
            docker("network", "disconnect", "--force", self.network, container, timeout=60)
        docker("network", "rm", self.network, timeout=60)
        self.compose("down", "--volumes", "--remove-orphans", timeout=300)
        for image in (self.image, self.gateway_image, self.sandbox_image):
            docker("image", "rm", "--force", image, timeout=300)
        if self.jobs.exists():
            docker("run", "--rm", "--pull", "never", "--network", "none", "-v", f"{self.root / 'data'}:/data", RUN_IMAGE, "rm", "-rf", "/data/jobs", timeout=120)
        shutil.rmtree(self.root, ignore_errors=True)
        return self.leftovers()


# ── MCP over plain HTTP on /mcp, as check_door.py ───────────────────────────


def _mcp_post(deployment, document, session=None):
    headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
    if session is not None:
        headers["mcp-session-id"] = session
    request = urllib.request.Request(f"http://127.0.0.1:{deployment.port}/mcp/", data=json.dumps(document).encode(), headers=headers, method="POST")
    with urllib.request.urlopen(request, timeout=30) as answer:
        body = answer.read().decode()
        session_id = answer.headers.get("mcp-session-id")
    messages = [json.loads(line[5:].strip()) for line in body.splitlines() if line.startswith("data:")]
    if not messages and body.strip():
        messages.append(json.loads(body))
    return session_id, messages


def tools_through_mcp(deployment):
    session, _ = _mcp_post(deployment, {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "check-queue", "version": "1"}}})
    _mcp_post(deployment, {"jsonrpc": "2.0", "method": "notifications/initialized"}, session)
    _, messages = _mcp_post(deployment, {"jsonrpc": "2.0", "id": 2, "method": "tools/list"}, session)
    return sorted(tool["name"] for tool in messages[0]["result"]["tools"])


def submit_through_mcp(deployment, sql, filename):
    session, _ = _mcp_post(deployment, {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "check-queue", "version": "1"}}})
    _mcp_post(deployment, {"jsonrpc": "2.0", "method": "notifications/initialized"}, session)
    _, messages = _mcp_post(deployment, {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "submit_postgres_query", "arguments": {"sql": sql, "target": TARGET, "output_filename": filename, "description": "a query of the check"}}}, session)
    found = re.search(r"pg_[0-9a-f]{8}", json.dumps(messages))
    if found is None:
        raise AssertionError(f"no job id in the MCP answer: {json.dumps(messages)[:300]}")
    return found.group(0)


# ── the checks ──────────────────────────────────────────────────────────────


def check_start(d):
    print("1. the scheduler")
    started = time.monotonic()
    built = d.compose("build", "jobs")
    if built.returncode != 0:
        raise AssertionError("the image did not build; the build's last lines:\n" + "\n".join(built.stderr.strip().splitlines()[-15:]))
    check(True, f"the image {d.image} built from Dockerfile.jobs in {time.monotonic() - started:.0f} s")
    up = d.compose("up", "-d", "jobs", timeout=300)
    check(up.returncode == 0, "the jobs service started" + ("" if up.returncode == 0 else ": " + up.stderr.strip()[-300:]))
    wait_for("the page", lambda: d.http("/pending")[0] == 200)
    check(True, f"the scheduler's page answers on port {d.port}")
    wait_for("the control folder", lambda: (d.jobs / "control" / "door.sock").exists(), 60)
    logs = d.compose("logs", "jobs").stdout
    check("StreamableHTTP session manager started" in logs and logs.count("StreamableHTTP session manager started") == 2, "two MCP session managers started in the one process (its log says so twice)")


def check_plain_mcp(d):
    print("2. /mcp as before")
    tools = tools_through_mcp(d)
    check(tools == ["list_mongo_databases", "list_targets", "query_cancel", "query_status", "submit_mongo_query", "submit_postgres_query"], f"/mcp lists its tools as before: {tools}")
    job_id = submit_through_mcp(d, "SELECT 1 AS one", "one.csv")
    phase, record = d.record(job_id)
    check(phase == "pending" and "submitted_by" not in record, f"a query submitted through /mcp ({job_id}) is pending and its record has no submitted_by")
    return job_id


def check_refusals(d):
    print("3. the assistant's endpoint without the secret")
    unknown = d.http("/no-such-page")
    check(unknown == (404, b'{"detail":"Not Found"}'), f"a path the page does not have: {unknown}")
    paths = [("/assistant/mcp", "POST"), ("/assistant/mcp/", "POST"), ("/assistant/results/x", "GET"), ("/assistant", "GET")]
    before = [d.http(path, {"x-sdp-assistant": WRONG}, method) for path, method in paths]
    check(before == [unknown] * len(paths), "before the secret file exists, the four paths with a made-up header answer the same 404")
    secret = host._queue_secret(str(d.root))
    path = d.jobs / "control" / host.QUEUE_SECRET_NAME
    info = os.lstat(path)
    check(stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode) == 0o600 and info.st_uid == os.getuid() and path.read_text().strip() == secret and len(secret) == 64, "the secret file is made by chat start's own code: a regular file of this user, mode 0600, 64 hex digits")
    without = [d.http(p, None, m) for p, m in paths]
    wrong = [d.http(p, {"x-sdp-assistant": WRONG}, m) for p, m in paths]
    check(without == [unknown] * len(paths) and wrong == [unknown] * len(paths), "with the file there: without the header and with a wrong secret, the same 404")
    return secret


def check_gateway(d, secret):
    print("4. through a gateway: the assistant's MCP server")
    for image, dockerfile in ((d.gateway_image, host.GATEWAY_DOCKERFILE), (d.sandbox_image, host.SANDBOX_DOCKERFILE)):
        started = time.monotonic()
        built = docker("build", "-f", str(d.root / dockerfile), "-t", image, str(d.root), timeout=3600)
        if built.returncode != 0:
            raise AssertionError(f"{dockerfile} did not build: " + "\n".join(built.stderr.strip().splitlines()[-15:]))
        check(True, f"the image {image} built from {dockerfile} in {time.monotonic() - started:.0f} s")
    made = docker("network", "create", d.network)
    check(made.returncode == 0, f"the network {d.network} is made")
    subnet = json.loads(docker("network", "inspect", "--format", "{{json .IPAM.Config}}", d.network).stdout)[0]["Subnet"]
    docker("network", "rm", d.network)
    docker("network", "create", "--subnet", subnet, d.network)
    network_gateway = json.loads(docker("network", "inspect", "--format", "{{json .IPAM.Config}}", d.network).stdout)[0].get("Gateway", "")
    address = host.gateway_address(subnet, network_gateway)
    config = {"listen": address, "listeners": [
        {"port": host.QUEUE_PORT, "kind": "mcp", "upstream": f"http://{host.QUEUE_ALIAS}:{d.port}", "paths": list(host.QUEUE_PATHS), "secret_file": f"{host.GATEWAY_MOUNT}/{host.QUEUE_SECRET_NAME}"},
        {"port": 8005, "kind": "mcp", "upstream": f"http://{host.QUEUE_ALIAS}:{d.port}", "paths": list(host.QUEUE_PATHS)},
    ]}
    with files.folder(str(d.gateway_folder)) as fd:
        files.replace_file(fd, "config.json", json.dumps(config).encode())
        files.replace_file(fd, host.QUEUE_SECRET_NAME, (secret + "\n").encode())
    run = docker("run", "-d", "--name", d.gateway, "--pull", "never", "--user", f"{os.getuid()}:{os.getgid()}", "--read-only", "--cap-drop", "ALL", "--network", d.network, "--ip", address, "--mount", f"type=bind,source={d.gateway_folder},target={host.GATEWAY_MOUNT},readonly", d.gateway_image)
    check(run.returncode == 0, "the gateway container started" + ("" if run.returncode == 0 else ": " + run.stderr.strip()))
    wait_for("the gateway's listeners", lambda: docker("logs", d.gateway).stdout.count(" listening on port ") == 2, 30)
    check(True, "the gateway listens on 8004 (with the secret file) and 8005 (without)")
    # After the gateway, which has the fixed address the scheduler would otherwise be given.
    connected = docker("network", "connect", "--alias", host.QUEUE_ALIAS, d.network, d.container())
    check(connected.returncode == 0, f"the scheduler is attached to it as {host.QUEUE_ALIAS}")
    return address


def check_session(d, address, plain_id):
    out = d.helper(SESSION, {"url": f"http://{address}:{host.QUEUE_PORT}/assistant/mcp", "target": TARGET, "others": [plain_id, "pg_00000000", "../pending/x"]}, "session")
    check(out["tools"] == ["list_targets", "query_cancel", "query_status", "submit_mongo_query", "submit_postgres_query"], f"the helper (the sandbox image's mcp client) lists the assistant's tools, without list_mongo_databases: {out['tools']}")
    marked = out["submitted"].get("job_id", "")
    phase, record = d.record(marked)
    check(re.fullmatch(r"pg_[0-9a-f]{8}", marked) and phase == "pending" and record.get("submitted_by") == "assistant", f"a query submitted through the gateway ({marked}) is pending and its record on disk carries submitted_by: assistant")
    check(out["own"]["job_id"] == marked and out["own"]["status"] == "pending", "query_status answers for it: pending")
    check(out["others"] == [{"status": "unknown", "error": "no such job"}] * 3, "query_status answers \"no such job\" for the job of step 2, a made-up id and a malformed one, without the id")
    check(out["cancel_other"] == {"status": "unknown", "error": "no such job"} and d.record(plain_id)[0] == "pending", "query_cancel of the job of step 2 is \"no such job\", and that job stays pending")
    check(out["targets"] == {"targets": [{"name": MONGO_TARGET, "backend": "mongodb"}, {"name": TARGET, "backend": "postgres"}]}, "list_targets as on /mcp")
    return marked


def check_auto_accept(d, plain_id, marked):
    print("5. auto-accept on")
    check(d.door({"op": "reject", "job_id": plain_id, "reason": "", "by": BY}) == {}, "the job of step 2 is rejected through the control socket (it would take the slot)")
    changed = d.door({"op": "auto_accept.set", "target": TARGET, "enabled": True, "limit": 2, "by": BY})
    check(changed["targets"][0]["enabled"] is True, "auto-accept is switched on for the target through the control socket")
    approved = wait_for("the auto-accept thread to approve the marked job", lambda: d.record(marked)[0] != "pending" and d.record(marked)[1], 30)
    check(approved.get("approved_at") and "acted_by" not in approved and approved.get("submitted_by") == "assistant", f"the marked job ({marked}) is approved by the auto-accept thread (no one's label in its record) and keeps its mark")
    unmarked = submit_through_mcp(d, "SELECT 2", "two.csv")
    approved = wait_for("the auto-accept thread to approve the unmarked job", lambda: d.record(unmarked)[0] != "pending" and d.record(unmarked)[1], 30)
    check(approved.get("approved_at") and "acted_by" not in approved, f"a new unmarked job ({unmarked}) is approved by the auto-accept thread (no one's label in its record)")
    d.door({"op": "auto_accept.set", "target": TARGET, "enabled": False, "limit": None, "by": BY})


def check_results(d, address, secret):
    print("6. results through the gateway")
    code = (
        "import time\n"
        "from social_data_pipeline.jobs.store import Job, Store\n"
        "store = Store('/data/jobs')\n"
        f"for job_id, mark in (({DONE_ID!r}, 'assistant'), ({PLAIN_ID!r}, ''), ({LINKED_ID!r}, 'assistant')):\n"
        f"    job = Job(job_id=job_id, target={TARGET!r}, backend='postgres', sql='SELECT 3', output_filename='out.csv', overwrite=False, submitted_at=time.time(), status='running', started_at=time.time(), submitted_by=mark)\n"
        "    job.result_path = '/data/jobs'\n"
        "    store.complete(job)\n"
        "print('ok')\n"
    )
    check(d.in_container(code).strip() == "ok", "three finished job records written with the store's own code inside the container (two marked, one not)")
    data = os.urandom(150000) + b"\nend\n"
    for job_id, parts in ((DONE_ID, {"out_0.csv": data, "out_1.csv": b"k\n2\n"}), (PLAIN_ID, {"out.csv": b"k\n1\n"}), (LINKED_ID, {"out_1.csv": b"k\nfine\n"})):
        folder = d.jobs / "results" / job_id
        folder.mkdir()
        for name, content in parts.items():
            (folder / name).write_bytes(content)
    (d.jobs / "marker.csv").write_text(f"secret\n{MARKER}\n")
    os.symlink("../../marker.csv", d.jobs / "results" / LINKED_ID / "out_0.csv")
    own = {"x-sdp-assistant": secret}
    requests = [
        [host.QUEUE_PORT, "GET", f"/assistant/results/{DONE_ID}", {}],
        [host.QUEUE_PORT, "GET", f"/assistant/results/{DONE_ID}/0", {}],
        [host.QUEUE_PORT, "GET", f"/assistant/results/{DONE_ID}/1", {}],
        [host.QUEUE_PORT, "GET", f"/assistant/results/{PLAIN_ID}", {}],
        [host.QUEUE_PORT, "GET", f"/assistant/results/{PLAIN_ID}/0", {}],
        [host.QUEUE_PORT, "GET", f"/assistant/results/{LINKED_ID}", {}],
        [host.QUEUE_PORT, "GET", f"/assistant/results/{LINKED_ID}/1", {}],
        [8005, "POST", "/assistant/mcp", own],
        [8005, "GET", f"/assistant/results/{DONE_ID}/0", own],
    ]
    out = d.helper(FETCH, {"gateway": address, "requests": requests}, "fetch")
    not_found = [404, "application/json", b'{"detail":"Not Found"}'.hex()]
    listing = json.loads(bytes.fromhex(out[0][2]))
    check(out[0][0] == 200 and listing == {"parts": [{"index": 0, "name": "out_0.csv", "size": len(data)}, {"index": 1, "name": "out_1.csv", "size": 4}]}, f"the marked job's part list: {listing}")
    fetched = bytes.fromhex(out[1][2])
    check(out[1][:2] == [200, "application/octet-stream"] and fetched == data, f"its part 0 comes byte for byte as application/octet-stream ({len(fetched)} bytes, sha256 {hashlib.sha256(fetched).hexdigest()[:16]}...)")
    check(bytes.fromhex(out[2][2]) == b"k\n2\n", "and its part 1")
    check(out[3] == not_found and out[4] == not_found, "the unmarked finished job's list and part are the page's 404")
    linked = json.loads(bytes.fromhex(out[5][2]))
    check(linked == {"parts": [{"index": 0, "name": "out_1.csv", "size": 7}]} and out[6] == not_found, "a link planted as a part is not listed (only the real part is), and there is no second part")
    check(all(MARKER.encode().hex() not in answer[2] for answer in out), "the marker the link points at is in no answer")
    check(out[7] == not_found and out[8] == not_found, "the helper sending the right secret itself through the listener without a secret file gets the 404 (the gateway drops the header)")


def check_page(d):
    print("7. the page")
    check(d.http("/pending")[0] == 200 and d.http("/history")[0] == 200, "the scheduler's own page still answers (Pending, History)")


def main():
    d = Deployment()
    failed = None
    try:
        try:
            print(f"compose project {d.name}, Docker {docker('version', '--format', '{{.Server.Version}}').stdout.strip()}")
            check_start(d)
            plain_id = check_plain_mcp(d)
            secret = check_refusals(d)
            address = check_gateway(d, secret)
            marked = check_session(d, address, plain_id)
            check_auto_accept(d, plain_id, marked)
            check_results(d, address, secret)
            check_page(d)
            logs = d.compose("logs", "jobs").stdout + docker("logs", d.gateway).stdout + docker("logs", d.gateway).stderr
            check(secret not in logs, "the secret is in no log line of the scheduler or the gateway")
        except Exception as error:
            failed = error
    finally:
        left = d.clean_up()
    containers, networks, images, folder = left
    if containers or networks or images or folder:
        print(f"FAILED: left behind: containers {containers}, networks {networks}, images {images}, temp folder {folder}")
        return 1
    print("  ok: no container, network or image of the check is left, and the temp folder is deleted (the base image python:3.13-slim and the build cache stay)")
    if failed is not None:
        print(f"FAILED: {type(failed).__name__}: {failed}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
