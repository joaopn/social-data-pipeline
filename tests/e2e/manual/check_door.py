#!/usr/bin/env python3
"""A check of the query scheduler's control socket against a real Docker, run by hand: `python tests/e2e/manual/check_door.py` (Linux; a Docker with the compose plugin and without user-id mapping that can pull `python:3.13-slim` and reach PyPI, unpkg.com and cdn.jsdelivr.net, and the image `python:3.13-alpine`, which is not pulled). Standard library and this package only.

It is not collected by pytest. It builds a throwaway deployment in a temp folder: a copy of `docker-compose.yml`, `Dockerfile.jobs`, `requirements-jobs.txt`, the package and the scheduler's committed configuration, plus a `config/jobs/config.local.yaml` with one PostgreSQL target on a free port and no database at all, and `data/jobs` (with `data/jobs/results`) made by this script as the current user, as `sdp db setup-jobs` makes it. The compose project gets a throwaway name (`sdp-door-check-<hex>`), so the image it builds is `<name>-jobs`. What it shows, from this machine as the current user:

1. The scheduler's image builds from `Dockerfile.jobs` and its `jobs` service starts (as root, with `umask 0000`); its page answers on its port.
2. `data/jobs/control` is 0700 and `door.sock` 0600, both of the current user; `hello`, `counts` and `targets` answer through the socket.
3. A query is submitted through the scheduler's `/mcp` endpoint with plain HTTP (JSON-RPC over streamable HTTP, without the MCP client library); `jobs pending` lists it; `job` returns it with its digest; `approve` with a wrong digest is `changed`; with the right one it is approved, the runner picks it up, and it ends in the history as failed (there is no database), with who approved it in its record.
4. A second query is rejected with a reason; the history holds the reason and who rejected it.
5. A finished job with a small CSV result, its record written with the store's own code inside the container (`docker exec`, `Store.complete`) and its file by hand, is previewed; a link planted as a part of a second finished job (pointing at a file with a marker) is not read and the marker is in no reply.
6. The scheduler's own page still answers on its port.
7. Containers run as another uid (4321) with the control folder mounted cannot connect (permission denied); one run as root reaches the socket and is closed without a reply (`SO_PEERCRED`); one run as the current uid gets `hello` answered.

Clean-up, also when the check is interrupted: the compose project is taken down (its containers and network), the image `<name>-jobs` is removed, the containers of step 7 are removed by name, the files the scheduler made as root are removed by a root container, and the temp folder is deleted. What stays: the base image `python:3.13-slim` and Docker's build cache.
"""

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

from social_data_pipeline.control import client

RUN_IMAGE = "python:3.13-alpine"
MARKER = "door-check-marker-61c4"
TARGET = "warehouse"
BY = "web-4242"
DONE_ID = "pg_d0d0d0d0"
LINKED_ID = "pg_11eeee11"


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


def refused(call):
    try:
        call()
    except client.BrokerError as error:
        return error
    raise AssertionError("the request was not refused")


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class Deployment:
    def __init__(self):
        self.root = Path(tempfile.mkdtemp(prefix="sdp-door-check-")).resolve()
        os.chmod(self.root, 0o755)
        self.name = "sdp-door-check-" + os.urandom(4).hex()
        self.image = f"{self.name}-jobs"
        self.port = free_port()
        self.runs = []
        for name in ("docker-compose.yml", "Dockerfile.jobs", "requirements-jobs.txt"):
            shutil.copy(REPO_ROOT / name, self.root / name)
        shutil.copytree(REPO_ROOT / "social_data_pipeline", self.root / "social_data_pipeline", ignore=shutil.ignore_patterns("__pycache__"))
        (self.root / "config" / "jobs").mkdir(parents=True)
        for name in ("config.yaml", "entrypoint.sh"):
            shutil.copy(REPO_ROOT / "config" / "jobs" / name, self.root / "config" / "jobs" / name)
        # The smallest configuration with one target; no database is started.
        (self.root / "config" / "jobs" / "config.local.yaml").write_text(f"port: {self.port}\nmax_concurrent: 1\nauth: false\ntargets:\n  {TARGET}:\n    backend: postgres\n    database: datasets\n")
        self.jobs = self.root / "data" / "jobs"
        (self.jobs / "results").mkdir(parents=True)
        os.chmod(self.jobs, 0o755)
        os.chmod(self.jobs / "results", 0o755)
        self.environment = {**os.environ, "COMPOSE_PROJECT_NAME": self.name, "JOBS_PORT": str(self.port)}

    @property
    def socket_path(self):
        return str(self.jobs / "control" / "door.sock")

    def compose(self, *argv, timeout=1800):
        return subprocess.run(["docker", "compose", "--project-name", self.name, "--profile", "jobs", *argv], cwd=self.root, env=self.environment, capture_output=True, text=True, timeout=timeout)

    def door(self):
        return client.Client(self.socket_path, timeout=30)

    def ask(self, document):
        return self.door().request(document)

    def page(self, path):
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=10) as answer:
            return answer.status

    def container(self):
        result = subprocess.run(["docker", "ps", "-q", "--filter", f"label=com.docker.compose.project={self.name}", "--filter", "label=com.docker.compose.service=jobs"], capture_output=True, text=True, timeout=30)
        return result.stdout.strip()

    def in_container(self, code):
        result = subprocess.run(["docker", "exec", self.container(), "python", "-c", code], capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            raise AssertionError(f"docker exec failed: {result.stderr.strip().splitlines()[-1:]}")
        return result.stdout

    def run_as(self, user, code, label):
        """A throwaway container run as `user` with only the control folder mounted; (exit code, its last line)."""
        name = f"{self.name}-{label}"
        self.runs.append(name)
        result = subprocess.run(["docker", "run", "--rm", "--pull", "never", "--name", name, "--user", user, "--network", "none", "-v", f"{self.jobs / 'control'}:/control", RUN_IMAGE, "python", "-c", code], capture_output=True, text=True, timeout=120)
        lines = (result.stdout + result.stderr).strip().splitlines()
        return result.returncode, lines[-1] if lines else ""

    def leftovers(self):
        containers = subprocess.run(["docker", "ps", "-a", "-q", "--filter", f"label=com.docker.compose.project={self.name}"], capture_output=True, text=True, timeout=30).stdout.split()
        for name in self.runs:
            if subprocess.run(["docker", "container", "inspect", name], capture_output=True, timeout=30).returncode == 0:
                containers.append(name)
        networks = subprocess.run(["docker", "network", "ls", "-q", "--filter", f"label=com.docker.compose.project={self.name}"], capture_output=True, text=True, timeout=30).stdout.split()
        image = subprocess.run(["docker", "image", "inspect", self.image], capture_output=True, timeout=30).returncode == 0
        return containers, networks, image, self.root.exists()

    def clean_up(self):
        self.compose("down", "--volumes", "--remove-orphans", timeout=300)
        for name in self.runs:
            subprocess.run(["docker", "rm", "--force", name], capture_output=True, timeout=60)
        subprocess.run(["docker", "image", "rm", "--force", self.image], capture_output=True, timeout=300)
        # The scheduler runs as root: what it made under data/jobs is removed by a root container first.
        if self.jobs.exists():
            subprocess.run(["docker", "run", "--rm", "--pull", "never", "--network", "none", "-v", f"{self.root / 'data'}:/data", RUN_IMAGE, "rm", "-rf", "/data/jobs"], capture_output=True, timeout=120)
        shutil.rmtree(self.root, ignore_errors=True)
        return self.leftovers()


# ── MCP over plain HTTP ─────────────────────────────────────────────────────


def _mcp_post(deployment, document, session=None):
    headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
    if session is not None:
        headers["mcp-session-id"] = session
    request = urllib.request.Request(f"http://127.0.0.1:{deployment.port}/mcp/", data=json.dumps(document).encode(), headers=headers, method="POST")
    with urllib.request.urlopen(request, timeout=30) as answer:
        body = answer.read().decode()
        session_id = answer.headers.get("mcp-session-id")
    messages = []
    for line in body.splitlines():
        if line.startswith("data:"):
            messages.append(json.loads(line[5:].strip()))
    if not messages and body.strip():
        messages.append(json.loads(body))
    return session_id, messages


def submit_through_mcp(deployment, sql, filename, description):
    session, _ = _mcp_post(deployment, {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "check-door", "version": "1"}}})
    _mcp_post(deployment, {"jsonrpc": "2.0", "method": "notifications/initialized"}, session)
    _, messages = _mcp_post(deployment, {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "submit_postgres_query", "arguments": {"sql": sql, "target": TARGET, "output_filename": filename, "description": description}}}, session)
    found = re.search(r"pg_[0-9a-f]{8}", json.dumps(messages))
    if found is None:
        raise AssertionError(f"no job id in the MCP answer: {json.dumps(messages)[:300]}")
    return found.group(0)


# ── the checks ──────────────────────────────────────────────────────────────


def check_start(deployment):
    started = time.monotonic()
    built = deployment.compose("build", "jobs")
    if built.returncode != 0:
        tail = "\n".join(built.stderr.strip().splitlines()[-15:])
        raise AssertionError(f"the image did not build; the build's last lines:\n{tail}")
    check(True, f"the image {deployment.image} built from Dockerfile.jobs in {time.monotonic() - started:.0f} s")
    up = deployment.compose("up", "-d", "jobs", timeout=300)
    check(up.returncode == 0, "the jobs service started" + ("" if up.returncode == 0 else ": " + up.stderr.strip()[-300:]))
    wait_for("the page", lambda: deployment.page("/pending") == 200)
    check(True, f"the scheduler's page answers on port {deployment.port}")
    wait_for("the socket", lambda: os.path.exists(deployment.socket_path), 60)
    # The scheduler's own process (pid 1, the entrypoint's `exec`), not a process of `docker exec`, which gets Docker's default umask.
    status = dict(line.split(":", 1) for line in deployment.in_container("print(open('/proc/1/status').read())").splitlines() if ":" in line)
    umask, user = status["Umask"].strip(), status["Uid"].split()[0]
    check((umask, user) == ("0000", "0"), f"the scheduler (pid 1) runs as uid {user} with umask {umask}")


def check_folder(deployment):
    folder = os.lstat(deployment.jobs / "control")
    sock = os.lstat(deployment.socket_path)
    check(stat.S_ISDIR(folder.st_mode) and stat.S_IMODE(folder.st_mode) == 0o700 and folder.st_uid == os.getuid(), f"data/jobs/control is a folder, mode {oct(stat.S_IMODE(folder.st_mode))}, owner uid {folder.st_uid}")
    check(stat.S_ISSOCK(sock.st_mode) and stat.S_IMODE(sock.st_mode) == 0o600 and sock.st_uid == os.getuid(), f"door.sock is a socket, mode {oct(stat.S_IMODE(sock.st_mode))}, owner uid {sock.st_uid}")
    check(deployment.ask({"op": "hello"}) == {"protocol": 1}, "hello answers {'protocol': 1}")
    check(deployment.ask({"op": "counts"}) == {"pending": 0, "approved": 0, "running": 0}, "counts answers zeros")
    targets = deployment.ask({"op": "targets"})
    check(targets == {"targets": [{"name": TARGET, "backend": "postgres", "database": "datasets"}]}, f"targets answers {targets}")


def check_approve(deployment):
    job_id = submit_through_mcp(deployment, "SELECT 1 AS one", "one.csv", "a query of the check")
    check(True, f"a query submitted through /mcp with plain HTTP: {job_id}")
    pending = deployment.ask({"op": "jobs", "phase": "pending", "limit": 10, "offset": 0})
    check([job["job_id"] for job in pending["jobs"]] == [job_id], "jobs pending lists it")
    found = deployment.ask({"op": "job", "job_id": job_id})
    check(found["phase"] == "pending" and found["job"]["sql"] == "SELECT 1 AS one" and re.fullmatch(r"[0-9a-f]{64}", found["digest"]), f"job returns it with its digest {found['digest'][:16]}...")
    wrong = refused(lambda: deployment.ask({"op": "approve", "job_id": job_id, "digest": "0" * 64, "by": BY}))
    check(wrong.code == "changed", "approve with a wrong digest is refused `changed`")
    check(deployment.ask({"op": "jobs", "phase": "pending", "limit": 10, "offset": 0})["jobs"][0]["job_id"] == job_id, "and the job is still pending")
    check(deployment.ask({"op": "approve", "job_id": job_id, "digest": found["digest"], "by": BY}) == {}, "approve with the right digest answers {}")

    def ended():
        answer = deployment.ask({"op": "job", "job_id": job_id})
        return answer if answer["phase"] == "history" else None

    done = wait_for("the runner to end the job", ended, 60)
    record = done["job"]
    check(record["status"] == "failed" and record["acted_by"] == {"approved": BY}, f"the runner ran it and it ended as {record['status']} ({(record['error'] or '')[:70]}...), approved by {record['acted_by'].get('approved')}")
    again = refused(lambda: deployment.ask({"op": "approve", "job_id": job_id, "digest": found["digest"], "by": BY}))
    check(again.code == "not_pending" and again.details.get("phase") == "history", "a second approve is `not_pending` in phase history")


def check_reject(deployment):
    job_id = submit_through_mcp(deployment, "SELECT 2", "two.csv", "a second query")
    check(deployment.ask({"op": "reject", "job_id": job_id, "reason": "not today", "by": BY}) == {}, f"reject of {job_id} with a reason answers {{}}")
    record = deployment.ask({"op": "job", "job_id": job_id})["job"]
    check((record["status"], record["reject_reason"], record["acted_by"]) == ("rejected", "not today", {"rejected": BY}), "the history holds the reason and who rejected it")


def check_preview(deployment):
    code = (
        "import time\n"
        "from social_data_pipeline.jobs.store import Job, Store\n"
        "store = Store('/data/jobs')\n"
        f"for job_id in ({DONE_ID!r}, {LINKED_ID!r}):\n"
        f"    job = Job(job_id=job_id, target={TARGET!r}, backend='postgres', sql='SELECT 3', output_filename='out.csv', overwrite=False, submitted_at=time.time(), status='running', started_at=time.time())\n"
        "    job.result_path = '/data/jobs'\n"
        "    store.complete(job)\n"
        "print('ok')\n"
    )
    check(deployment.in_container(code).strip() == "ok", "two finished job records written with the store's own code (Store.complete) inside the container")
    done_folder = deployment.jobs / "results" / DONE_ID
    done_folder.mkdir()
    (done_folder / "out.csv").write_text("name,count\nalpha,1\nbeta,2\n")
    shown = deployment.ask({"op": "preview", "job_id": DONE_ID})
    check(shown["columns"] == ["name", "count"] and shown["rows"] == [["alpha", "1"], ["beta", "2"]] and shown["parts"] == [{"name": "out.csv", "size": 26}], f"the small CSV is previewed: {shown['columns']} {shown['rows']}")
    (deployment.jobs / "marker.csv").write_text(f"secret\n{MARKER}\n")
    linked_folder = deployment.jobs / "results" / LINKED_ID
    linked_folder.mkdir()
    # Relative, so that it points at the marker inside the container too.
    os.symlink("../../marker.csv", linked_folder / "out_0.csv")
    (linked_folder / "out_1.csv").write_text("k\nfine\n")
    shown = deployment.ask({"op": "preview", "job_id": LINKED_ID})
    check([part["name"] for part in shown["parts"]] == ["out_1.csv"] and shown["rows"] == [["fine"]], "a link planted as a part is skipped; the real part is previewed")
    check(MARKER not in json.dumps(shown), "the marker the link points at is in no reply")
    os.unlink(linked_folder / "out_1.csv")
    alone = refused(lambda: deployment.ask({"op": "preview", "job_id": LINKED_ID}))
    check(alone.code == "preview_failed" and MARKER not in alone.message, "with only the link left the preview is `preview_failed`, without the marker")


def check_page_and_others(deployment):
    check(deployment.page("/pending") == 200 and deployment.page("/history") == 200, "the scheduler's own page still answers (Pending, History)")
    connect = "import socket\ns = socket.socket(socket.AF_UNIX)\ns.settimeout(10)\ntry:\n    s.connect('/control/door.sock')\nexcept OSError as error:\n    print(type(error).__name__); raise SystemExit(3)\nprint('connected')\n"
    code, last = deployment.run_as("4321:4321", connect, "other")
    check(code == 3 and last == "PermissionError", f"a container run as uid 4321 with the control folder mounted cannot connect: {last}")
    # A connection closed with the request unread is reset: that, like an empty read, is "no reply".
    talk = "import socket, struct\ns = socket.socket(socket.AF_UNIX)\ns.settimeout(10)\ns.connect('/control/door.sock')\nbody = b'{\"op\": \"hello\"}'\ndata = b''\ntry:\n    s.sendall(struct.pack('>I', len(body)) + body)\n    while True:\n        chunk = s.recv(65536)\n        if not chunk:\n            break\n        data += chunk\nexcept (BrokenPipeError, ConnectionResetError):\n    pass\nprint(repr(data[4:].decode()) if data else 'no reply')\n"
    code, last = deployment.run_as("0:0", talk, "root")
    check(code == 0 and last == "no reply", f"a container run as root reaches the socket and is closed without a reply: {last}")
    code, last = deployment.run_as(f"{os.getuid()}:{os.getgid()}", talk, "same")
    check(code == 0 and "protocol" in last, f"a container run as the current uid gets hello answered: {last}")


def main():
    deployment = Deployment()
    failed = None
    left = None
    try:
        try:
            version = subprocess.run(["docker", "version", "--format", "{{.Server.Version}}"], capture_output=True, text=True, timeout=30).stdout.strip()
            print(f"compose project {deployment.name}, Docker {version}")
            check_start(deployment)
            check_folder(deployment)
            check_approve(deployment)
            check_reject(deployment)
            check_preview(deployment)
            check_page_and_others(deployment)
        except Exception as error:
            failed = error
    finally:
        # Also on Ctrl-C and on anything else that ends the checks early.
        left = deployment.clean_up()
    containers, networks, image, folder = left
    if containers or networks or image or folder:
        print(f"FAILED: left behind: containers {containers}, networks {networks}, image {image}, temp folder {folder}")
        return 1
    print("  ok: no container, network or image of the check is left, and the temp folder is deleted (the base image python:3.13-slim and the build cache stay)")
    if failed is not None:
        print(f"FAILED: {type(failed).__name__}: {failed}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
