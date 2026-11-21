#!/usr/bin/env python3
"""A check of the broker against a real Docker, run by hand: `python tests/e2e/manual/check_broker.py` (an interpreter with PyYAML, a Docker with the compose plugin and the image `python:3.13-alpine`, which is not pulled). Linux only: it reads `/proc`.

It is not collected by pytest. It builds a throwaway deployment in a temp folder: a copy of the CLI and of the package, the smallest configuration with which `do_run_start` passes its checks for the profile `parse` on a source `demo`, and a compose file whose `parse` service runs a program that prints a line every second and exits with code 3 on KeyboardInterrupt. The broker is the real one, started with `sdp.py broker start`; every request goes through its socket with the package's client. What it shows:

1. `sdp.py broker passwd` sets the management password (asked on standard input, as `getpass` does without a terminal); `sdp.py broker start` starts the broker; `hello` answers; a wrong login is refused and the right one gives a session.
2. `db.status` and `source.list` answer with data through the socket.
3. A made-up database password is kept in the session after the second check of the management password; `run.start` starts a recorded run with it; `run.list` lists the run; `run.logs` reads its log in pieces, and neither the log on disk nor any reply holds the made-up password; `db.stop` is refused while the run is alive; `run.cancel` cancels it (exit code 3 in the record); `db.stop` is then allowed and runs with its output in files of `.sdp/broker/calls/`, which are deleted afterwards. That a child goes on after its broker was killed is not shown here: no state-changing action of this deployment lasts long enough to kill the broker in its middle (the unit tests show it with a stand-in child).
4. A second run is started; `sdp.py broker stop` stops the broker while that run lives, the run is still running afterwards and is cancelled with `sdp.py runs cancel`.
5. `sdp.py broker audit --verify` finds the chain whole; neither the audit log nor the broker's log holds the made-up password or the session's token.

Every process it starts is killed by the deployment folder's mark (its working directory, or the folder in its argument list), every container is stopped by its id, the project's network is removed with the project's own `down`, and the temp folder is deleted, also when the check is interrupted; nothing is pruned.
"""

import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from social_data_pipeline.control import client, redact

IMAGE = "python:3.13-alpine"
MANAGEMENT = "made-up-management-password-8d21"
PLANTED = "planted-made-up-db-password-6b3e"
PROGRAM = "import sys, time\ntry:\n    n = 0\n    while True:\n        print('line', n, 'x' * 300, flush=True)\n        n += 1\n        time.sleep(0.2)\nexcept KeyboardInterrupt:\n    print('interrupted', flush=True)\n    sys.exit(3)\n"


def compose_file(project):
    # Never pulled: a missing image is an error of this check, not a download.
    service = {"image": IMAGE, "pull_policy": "never", "environment": {"SOURCE": "${SOURCE:-}"}, "command": ["python", "-u", "-c", PROGRAM], "profiles": ["parse"]}
    return json.dumps({"name": project, "services": {"parse": service}}, indent=2)


def check(condition, what):
    if not condition:
        raise AssertionError(what)
    print(f"  ok: {what}")


def wait_for(what, probe, seconds=60):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        found = probe()
        if found:
            return found
        time.sleep(0.25)
    raise AssertionError(f"timed out waiting for {what}")


def refused(call):
    try:
        call()
    except client.BrokerError as error:
        return error
    raise AssertionError("the request was not refused")


class Deployment:
    def __init__(self):
        self.root = Path(tempfile.mkdtemp(prefix="sdp-broker-check-")).resolve()
        os.chmod(self.root, 0o700)
        self.name = "sdp-broker-check-" + os.urandom(4).hex()
        shutil.copy(REPO_ROOT / "sdp.py", self.root / "sdp.py")
        shutil.copytree(REPO_ROOT / "social_data_pipeline", self.root / "social_data_pipeline", ignore=shutil.ignore_patterns("__pycache__"))
        # The broker refuses code that the group or others may write; a checkout made with a umask of 002 is such code.
        os.chmod(self.root / "sdp.py", 0o644)
        for path, _, names in os.walk(self.root / "social_data_pipeline"):
            os.chmod(path, 0o755)
            for name in names:
                os.chmod(os.path.join(path, name), 0o644)
        (self.root / "docker-compose.yml").write_text(compose_file(self.name))
        source = self.root / "config" / "sources" / "demo"
        source.mkdir(parents=True)
        for name in ("platform.yaml", "parse.yaml"):
            (source / name).write_text("data_types: [posts]\n" if name == "platform.yaml" else "{}\n")
        self.env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
        self.replies = []

    def cli(self, *argv, stdin=None):
        """Run the copy of the CLI in the deployment, in a session of its own (no terminal): (exit code, standard output)."""
        result = subprocess.run([sys.executable, str(self.root / "sdp.py"), *argv], env=self.env, capture_output=True, text=True, timeout=180, input=stdin, start_new_session=True)
        return result.returncode, result.stdout

    def client(self, token=None):
        return client.Client(str(self.root / ".sdp" / "socket" / "broker.sock"), timeout=120, token=token)

    def call(self, session, verb, fields=None, step_up=None):
        """A call through the socket; its reply is kept to be searched for the planted password."""
        try:
            result = session.call(verb, fields, step_up=step_up)
        except client.BrokerError as error:
            self.replies.append(json.dumps({"code": error.code, "message": error.message, "details": error.details}))
            raise
        self.replies.append(json.dumps(result))
        return result

    def log(self, run_id):
        path = self.root / ".sdp" / "runs" / run_id / "log"
        return path.read_bytes() if path.exists() else b""

    def containers(self):
        result = subprocess.run(["docker", "ps", "-a", "--no-trunc", "-q", "--filter", f"label=com.docker.compose.project={self.name}"], capture_output=True, text=True, timeout=30)
        return result.stdout.split()

    def own_processes(self):
        """The processes of the deployment: run in its folder, or named with it in their argument list (the broker: its other files under /proc are closed to this user, its argument list is not)."""
        found = []
        mark = str(self.root).encode()
        for name in os.listdir("/proc"):
            if not name.isdigit() or int(name) == os.getpid():
                continue
            try:
                arguments = Path(f"/proc/{name}/cmdline").read_bytes()
            except OSError:
                continue
            try:
                folder = os.readlink(f"/proc/{name}/cwd")
            except OSError:
                folder = None
            if folder == str(self.root) or mark in arguments:
                found.append((int(name), arguments.split(b"\0")))
        return found

    def broker_pid(self):
        path = self.root / ".sdp" / "broker" / "lock"
        return int(path.read_text().split()[0]) if path.exists() and path.read_text().strip() else None

    def clean_up(self):
        for pid, _ in self.own_processes():
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass
        for container in self.containers():
            subprocess.run(["docker", "stop", "--signal", "KILL", container], capture_output=True, timeout=60)
            subprocess.run(["docker", "rm", "--force", container], capture_output=True, timeout=60)
        subprocess.run(["docker", "compose", "--profile", "parse", "down"], cwd=self.root, capture_output=True, timeout=120)
        time.sleep(0.5)
        left = self.containers(), [pid for pid, _ in self.own_processes()]
        shutil.rmtree(self.root, ignore_errors=True)
        return left


def check_start(deployment):
    print("1. password, start, hello, login")
    code, text = deployment.cli("broker", "passwd", stdin=f"{MANAGEMENT}\n{MANAGEMENT}\n")
    check(code == 0 and "The management password is set." in text, "`broker passwd` sets the management password")
    code, text = deployment.cli("broker", "start")
    check(code == 0 and re.fullmatch(r"  The broker is started \(pid [0-9]+\)\.\n", text) is not None, "`broker start`: " + text.strip())
    code, text = deployment.cli("broker", "status")
    check(code == 0 and "  Management password: set\n" in text, "`broker status` says it runs: " + text.splitlines()[0].strip())
    check(deployment.client().hello() == {"protocol": 1, "password_set": True}, "hello answers: protocol 1, a password is set")
    check(refused(lambda: deployment.client().login("a-wrong-made-up-password")).code == "unauthorized", "a wrong login is refused")
    session = deployment.client()
    login = session.login(MANAGEMENT)
    check(login["session"] == 1 and login["expires_in"] == 8 * 3600, "the right login gives session 1 for eight hours")
    return session


def check_reads(deployment, session):
    print("2. reads through the socket")
    status = deployment.call(session, "db.status")
    check(isinstance(status.get("data"), dict), "db.status answers with data: " + ", ".join(sorted(status["data"])[:6]))
    sources = deployment.call(session, "source.list")
    check("demo" in json.dumps(sources["data"]), "source.list lists the source demo")


def check_a_run(deployment, session):
    print("3. a run with the session's database password")
    check(refused(lambda: session.set_db_password(PLANTED, step_up="a-wrong-made-up-password")).code == "step_up_failed", "a database password with a wrong second check is refused")
    session.set_db_password(PLANTED, step_up=MANAGEMENT)
    check(True, "the database password is kept in the session after the second check")
    run_id = deployment.call(session, "run.start", {"profile": "parse", "source": "demo"})["run_id"]
    check(re.fullmatch(r"r[0-9]{8}T[0-9]{6}-[0-9a-f]{6}", run_id) is not None, f"run.start started the recorded run {run_id}")
    listed = wait_for("the run to be running", lambda: next((run for run in deployment.call(session, "run.list")["data"]["runs"] if run["id"] == run_id and run["state"] == "running"), None))
    check(listed["kind"] == "web" and listed["started_by"] == "web-1", "run.list lists it: kind web, running, started by web-1")
    wait_for("some output", lambda: len(deployment.log(run_id)) > 12000)
    pieces, offset = [], 0
    while True:
        data = deployment.call(session, "run.logs", {"run_id": run_id, "offset": offset, "limit": 4096})["data"]
        if data["text"] == "" or offset > 12000:
            break
        pieces.append(data["text"])
        offset = data["next_offset"]
    whole = deployment.log(run_id)
    check(len(pieces) >= 3 and "".join(pieces).encode() == redact.redact_bytes(whole, [PLANTED])[0][:offset], f"run.logs read the log in {len(pieces)} pieces of 4096 bytes, equal to the file's first {offset} bytes as overwritten whole")
    check(b"$ docker compose " in whole and "  $ docker compose " + "*" * 10 in "".join(pieces), "the `$ docker compose` line is in the file and overwritten in the reply")
    stop = refused(lambda: deployment.call(session, "db.stop", {}))
    check(stop.code == "run_active", "db.stop is refused while the run is alive: " + stop.code)
    cancel = deployment.call(session, "run.cancel", {"run_id": run_id})
    check(cancel["exit_code"] == 0, "run.cancel: " + cancel["output"].strip())
    entry = wait_for("the run to be cancelled", lambda: next((run for run in deployment.call(session, "run.list")["data"]["runs"] if run["id"] == run_id and run["state"] == "cancelled"), None))
    check(entry["exit_code"] == 3, "the run is cancelled with exit code 3 (the program saw the interrupt)")
    stop = deployment.call(session, "db.stop", {})
    check(stop == {"exit_code": 1, "output": "  No databases configured. Run: python sdp.py db setup\n", "errors": ""}, "db.stop, allowed once no run is alive, runs with its output in files and answers its exit code and output: " + stop["output"].strip())
    check(list((deployment.root / ".sdp" / "broker" / "calls").iterdir()) == [], "the output files of that call are deleted")
    check(PLANTED.encode() not in deployment.log(run_id), "the planted password is not in the run's log on disk")
    check(all(PLANTED not in reply for reply in deployment.replies), f"the planted password is in none of the {len(deployment.replies)} replies")


def check_stop_with_a_run_alive(deployment, session):
    print("4. broker stop while a run lives")
    run_id = deployment.call(session, "run.start", {"profile": "parse", "source": "demo"})["run_id"]
    wait_for("the second run's output", lambda: b"line 2 " in deployment.log(run_id))
    broker_pid = deployment.broker_pid()
    code, text = deployment.cli("broker", "stop")
    check(code == 0 and text == "  The broker has stopped.\n", "`broker stop`: " + text.strip())
    check(not Path(f"/proc/{broker_pid}").exists(), f"the broker process {broker_pid} is gone")
    check(not (deployment.root / ".sdp" / "socket" / "broker.sock").exists(), "the socket is removed")
    code, text = deployment.cli("runs", "list", "--json")
    entry = next(run for run in json.loads(text)["runs"] if run["id"] == run_id)
    check(entry["state"] == "running", "the run still runs after the broker stopped")
    before = len(deployment.log(run_id))
    wait_for("the run to go on writing", lambda: len(deployment.log(run_id)) > before)
    code, text = deployment.cli("runs", "cancel", run_id)
    check(code == 0, "`runs cancel` in the terminal: " + text.strip())
    wait_for("the run to be cancelled", lambda: next(run for run in json.loads(deployment.cli("runs", "list", "--json")[1])["runs"] if run["id"] == run_id)["state"] == "cancelled")
    check(True, "the run is cancelled")


def check_the_audit_log(deployment, token):
    print("5. the audit log")
    code, text = deployment.cli("broker", "audit", "--verify")
    check(code == 0 and text.endswith("  The chain is whole.\n"), "`broker audit --verify`: " + " / ".join(line.strip() for line in text.splitlines()))
    code, text = deployment.cli("broker", "audit", "--limit", "200")
    events = [line.split()[3] for line in text.splitlines()]
    check(events[0] == "broker_start" and events[-1] == "broker_stop" and "login" in events and "call_accepted" in events and "call_refused" in events, "`broker audit` prints the lines: " + " ".join(events))
    folder = deployment.root / ".sdp" / "broker"
    for name in ("audit.jsonl", "broker.log"):
        content = (folder / name).read_bytes()
        check(PLANTED.encode() not in content and MANAGEMENT.encode() not in content and token.encode() not in content, f"{name} holds neither password nor the token")
    modes = {oct(path.lstat().st_mode & 0o777) for path in (deployment.root / ".sdp").rglob("*")}
    check(modes <= {"0o700", "0o600"}, f"the state folder is private: {sorted(modes)}")


def main():
    deployment = Deployment()
    failed = None
    left = None
    try:
        try:
            print(f"compose project {deployment.name}, Docker {subprocess.run(['docker', 'version', '--format', '{{.Server.Version}}'], capture_output=True, text=True, timeout=30).stdout.strip()}")
            session = check_start(deployment)
            check_reads(deployment, session)
            check_a_run(deployment, session)
            token = session.token
            check_stop_with_a_run_alive(deployment, session)
            check_the_audit_log(deployment, token)
        except Exception as error:
            failed = error
    finally:
        # Also on Ctrl-C and on anything else that ends the checks early.
        left = deployment.clean_up()
        if left[0] or left[1]:
            print(f"FAILED: left behind: containers {left[0]}, processes {left[1]}")
    if left[0] or left[1]:
        return 1
    print("  ok: no container and no process of the deployment is left, and the temp folder is deleted")
    if failed is not None:
        print(f"FAILED: {type(failed).__name__}: {failed}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
