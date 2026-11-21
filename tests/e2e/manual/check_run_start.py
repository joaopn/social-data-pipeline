#!/usr/bin/env python3
"""A check of starting a recorded pipeline run against a real Docker, run by hand: `python tests/e2e/manual/check_run_start.py` (an interpreter with PyYAML, a Docker with the compose plugin and the image `python:3.13-alpine`, which is not pulled). Linux only: it reads `/proc`.

It is not collected by pytest. It builds a throwaway deployment in a temp folder: a copy of the CLI and of the package, the smallest configuration with which `do_run_start` passes its checks for the profiles `parse` and `mongo_ingest` on a source `demo`, and a compose file whose two services run a program that prints a line every second and exits with code 3 on KeyboardInterrupt. Every run goes through the real chain: `start_run`, the supervisor, the child, `do_run_start`, `docker compose run`. What it shows:

1. A run that was started appears in `sdp runs list` as kind `web` and `running`; `sdp runs logs` reads its log in pieces from the log file; its container carries the name and the label of the run; a second run of the same profile and source is refused while it runs.
2. A made-up password sent as `admin_password` is in no file under `.sdp/`, in no argument list of any process and not in the output of `runs list --json`. The profile `parse` needs no password, so there it is only carried by the request.
3. `sdp runs cancel` ends the run within a few seconds: state `cancelled`, exit code 3 (the program saw the interrupt), no container and no supervisor left.
4. A second run is then allowed. Killing its `docker compose run` process while the container lives keeps the run `running`, and the container's output goes on in the log; when the container is then stopped, the record ends `finished` with the container's exit code.
5. With authentication switched on for MongoDB in the throwaway deployment, a `mongo_ingest` run without a password ends at once with the line that says the password is required; with the password the container gets it (the program says so without printing it), and the password is again in no file under `.sdp/`, in no argument list and not in `runs list --json`, nor in the environment the supervisor and the child were started with. It is in the environment of the `docker compose` process and of the container: that is how the CLI hands a password to compose.
6. That run is cancelled like the first.

Every process it starts runs in the temp folder and is killed by that mark, every container is stopped by its id, the project's network is removed with the project's own `down`, and the temp folder is deleted, also when the check is interrupted; nothing is pruned.
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

from social_data_pipeline import runstore

IMAGE = "python:3.13-alpine"
PLANTED = "planted-made-up-password-4c1d"
PROGRAM = "import os, sys, time\ntry:\n    print('password received:', 'yes' if os.environ.get('MONGO_ADMIN_PASSWORD') else 'no', flush=True)\n    n = 0\n    while True:\n        print('line', n, flush=True)\n        n += 1\n        time.sleep(1)\nexcept KeyboardInterrupt:\n    print('interrupted', flush=True)\n    sys.exit(3)\n"
SERVICES = {"parse": "parse", "mongo-ingest": "mongo_ingest"}
# Run in the throwaway deployment: the request comes on standard input, never as an argument.
STARTER = "import json, os, sys\nfrom social_data_pipeline.control import errors, runs\ntry:\n    print(runs.start_run(os.getcwd(), json.load(sys.stdin), started_by='check'))\nexcept errors.RunNotStarted as error:\n    print(error.code)\n    sys.exit(3)\n"


def compose_file(project):
    # Never pulled: a missing image is an error of this check, not a download.
    service = {"image": IMAGE, "pull_policy": "never", "environment": {"SOURCE": "${SOURCE:-}", "MONGO_ADMIN_PASSWORD": "${MONGO_ADMIN_PASSWORD:-}"}, "command": ["python", "-u", "-c", PROGRAM]}
    return json.dumps({"name": project, "services": {"parse": {**service, "profiles": ["parse"]}, "mongo-ingest": {**service, "profiles": ["mongo_ingest"]}}}, indent=2)


def check(condition, what):
    if not condition:
        raise AssertionError(what)
    print(f"  ok: {what}")


def wait_for(what, probe, seconds=60):
    """Call probe about four times a second until it returns something true; fail after the time is up."""
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        found = probe()
        if found:
            return found
        time.sleep(0.25)
    raise AssertionError(f"timed out waiting for {what}")


def processes():
    """Every process this user can look at, as (pid, argument list, working directory)."""
    found = []
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            arguments = Path(f"/proc/{name}/cmdline").read_bytes().split(b"\0")
            folder = os.readlink(f"/proc/{name}/cwd")
        except OSError:
            continue
        found.append((int(name), arguments, folder))
    return found


class Deployment:
    def __init__(self):
        self.root = Path(tempfile.mkdtemp(prefix="sdp-run-start-check-")).resolve()
        self.name = "sdp-run-start-check-" + os.urandom(4).hex()
        shutil.copy(REPO_ROOT / "sdp.py", self.root / "sdp.py")
        shutil.copytree(REPO_ROOT / "social_data_pipeline", self.root / "social_data_pipeline", ignore=shutil.ignore_patterns("__pycache__"))
        (self.root / "docker-compose.yml").write_text(compose_file(self.name))
        source = self.root / "config" / "sources" / "demo"
        source.mkdir(parents=True)
        for name in ("platform.yaml", "parse.yaml", "mongo.yaml"):
            (source / name).write_text("data_types: [posts]\n" if name == "platform.yaml" else "{}\n")
        (self.root / "config" / "db").mkdir()
        (self.root / "config" / "db" / "mongo.yaml").write_text("port: 27017\n")
        (self.root / ".env").write_text("MONGO_AUTH_ENABLED=true\n")
        self.env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}

    def start(self, fields):
        """Start a run the way a host process would: (exit code, the run id or the reason it was refused)."""
        result = subprocess.run([sys.executable, "-c", STARTER], cwd=self.root, env=self.env, input=json.dumps(fields), capture_output=True, text=True, timeout=120)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        return result.returncode, result.stdout.strip()

    def cli(self, *argv):
        """Run the copy of the CLI in the deployment; returns (exit code, standard output)."""
        result = subprocess.run([sys.executable, str(self.root / "sdp.py"), *argv], env=self.env, capture_output=True, text=True, timeout=120, stdin=subprocess.DEVNULL)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        return result.returncode, result.stdout

    def listed(self, run_id):
        code, text = self.cli("runs", "list", "--json")
        check_quietly(code == 0, "`runs list --json` failed")
        return next((run for run in json.loads(text)["runs"] if run["id"] == run_id), None)

    def log(self, run_id):
        path = self.root / ".sdp" / "runs" / run_id / "log"
        return path.read_bytes() if path.exists() else b""

    def containers(self):
        """The ids of every container of this project, running or not."""
        result = subprocess.run(["docker", "ps", "-a", "--no-trunc", "-q", "--filter", f"label=com.docker.compose.project={self.name}"], capture_output=True, text=True, timeout=30)
        return result.stdout.split()

    def container_of(self, run_id):
        """(names, id) of the running containers that carry the run's label."""
        result = subprocess.run(["docker", "ps", "--no-trunc", "--filter", f"label=sdp.run={run_id}", "--format", "{{.Names}} {{.ID}}"], capture_output=True, text=True, timeout=30)
        return [tuple(line.split()) for line in result.stdout.splitlines() if line.strip()]

    def own_processes(self):
        """The processes that run in the deployment folder: supervisors, children and compose processes."""
        return [(pid, arguments) for pid, arguments, folder in processes() if folder == str(self.root) and pid != os.getpid()]

    def supervisors(self, run_id):
        return [pid for pid, arguments in self.own_processes() if b"social_data_pipeline.control.supervise" in arguments and run_id.encode() in arguments]

    def planted_in_files(self):
        return [str(path.relative_to(self.root)) for path in sorted((self.root / ".sdp").rglob("*")) if path.is_file() and PLANTED.encode() in path.read_bytes()]

    def clean_up(self):
        for pid, _ in self.own_processes():
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass
        for container in self.containers():
            subprocess.run(["docker", "stop", "--signal", "KILL", container], capture_output=True, timeout=60)
            subprocess.run(["docker", "rm", "--force", container], capture_output=True, timeout=60)
        subprocess.run(["docker", "compose", "--profile", "parse", "--profile", "mongo_ingest", "down"], cwd=self.root, capture_output=True, timeout=120)
        left = self.containers(), [pid for pid, _ in self.own_processes()]
        shutil.rmtree(self.root, ignore_errors=True)
        return left


def check_quietly(condition, what):
    if not condition:
        raise AssertionError(what)


def planted_in_argument_lists():
    return [pid for pid, arguments, _ in processes() if any(PLANTED.encode() in argument for argument in arguments)]


def check_a_run(deployment):
    print("1. a run that is started, listed, read and refused a second time")
    code, run_id = deployment.start({"profile": "parse", "source": "demo", "admin_password": PLANTED})
    check(code == 0 and re.fullmatch(runstore.RUN_ID_PATTERN, run_id) is not None, f"start_run returned the id {run_id}")
    wait_for("the run to be running", lambda: (deployment.listed(run_id) or {}).get("state") == "running")
    wait_for("the program's second line", lambda: b"line 1\n" in deployment.log(run_id))
    code, text = deployment.cli("runs", "list")
    print(text)
    check(code == 0 and re.search(rf"^  {run_id}  web   parse    demo    running  \S+Z  -      -$", text, re.M) is not None, "`runs list` prints the run as kind web, running")
    entry = deployment.listed(run_id)
    check((entry["kind"], entry["profile"], entry["source"], entry["flags"], entry["started_by"]) == ("web", "parse", "demo", {}, "check"), "`runs list --json`: kind web, profile parse, source demo, no flags, started by 'check'")

    whole = deployment.log(run_id)
    code, first = deployment.cli("runs", "logs", run_id, "--offset", "0", "--limit", "40")
    code_2, second = deployment.cli("runs", "logs", run_id, "--offset", "40", "--limit", "40")
    check(code == 0 and code_2 == 0 and (first + second).encode() == whole[:80], "`runs logs` reads the log file in pieces: " + repr(first + second))
    piece = runstore.read_run_log(deployment.root, SERVICES, run_id, 0, 40)
    check(piece["complete"] is True and piece["next_offset"] == 40 and piece["state"] == "running", "the store reads the same piece from the file (complete, next offset 40)")
    command = f"  $ docker compose --profile parse run --rm --name sdp-run-{run_id} -l sdp.run={run_id} parse\n"
    check(command.encode() in whole and b"  Source: demo (platform: custom/demo)\n" in whole and b"line 0\nline 1\n" in whole, "the log holds the CLI's lines and the program's output: " + command.strip())

    found = deployment.container_of(run_id)
    check(len(found) == 1 and found[0][0] == f"sdp-run-{run_id}", f"one container carries the label sdp.run={run_id}, and its name is {found and found[0][0]}")
    code, reason = deployment.start({"profile": "parse", "source": "demo"})
    check((code, reason) == (3, "run_active"), "a second run of the same profile and source is refused: " + reason)
    check(len(deployment.supervisors(run_id)) == 1, "one supervisor process holds the run")
    return run_id


def check_the_password(deployment, where):
    code, as_json = deployment.cli("runs", "list", "--json")
    check(deployment.planted_in_files() == [], f"the planted password is in no file under .sdp/ ({where})")
    check(planted_in_argument_lists() == [], f"the planted password is in no argument list of any process ({where})")
    check(code == 0 and PLANTED not in as_json and "admin_password" not in as_json, f"the planted password is not in `runs list --json` ({where})")


def check_cancel(deployment, run_id, title):
    print(title)
    started = time.monotonic()
    code, text = deployment.cli("runs", "cancel", run_id)
    check(code == 0 and text == f"  Run {run_id}: cancel requested, the process that holds the run stops it.\n", "`runs cancel` says: " + text.strip())
    entry = wait_for("the run to be cancelled", lambda: (lambda run: run if run["state"] == "cancelled" else None)(deployment.listed(run_id)), 20)
    check(time.monotonic() - started < 15, f"the run is cancelled {time.monotonic() - started:.1f} s after the command")
    check(entry["exit_code"] == 3 and entry["ended"] is not None, f"exit code 3 in the record: the program saw the interrupt (ended {entry['ended']})")
    print("  the end of the log: " + repr(deployment.log(run_id)[-120:].decode()))
    check(deployment.log(run_id).rstrip(b"\n").endswith(b"\ninterrupted"), "the log ends with the program's last line: 'interrupted'")
    check(deployment.container_of(run_id) == [] and deployment.containers() == [], "no container is left")
    wait_for("the supervisor to end", lambda: not deployment.supervisors(run_id), 20)
    check(True, "the supervisor has ended")


def check_a_dead_compose_process(deployment):
    print("4. a second run, whose compose process is killed while the container lives")
    code, run_id = deployment.start({"profile": "parse", "source": "demo", "build": False})
    check(code == 0, f"a second run after the first is allowed: {run_id}")
    wait_for("the program's second line", lambda: b"line 1\n" in deployment.log(run_id))
    # The Docker client starts the compose plugin as a process of its own: both are the compose process of the run, and both are killed.
    compose = [(pid, os.path.basename(arguments[0]).decode()) for pid, arguments in deployment.own_processes() if b"compose" in arguments and f"sdp-run-{run_id}".encode() in arguments]
    check(len(compose) >= 1, f"the `docker compose run` process of the run is found: {sorted(name for _, name in compose)}")
    for pid, _ in compose:
        os.kill(pid, signal.SIGKILL)
    marker = b"[run supervisor] the compose process ended while the container of the run is alive: from here on the output is read from Docker\n"
    wait_for("the supervisor to notice", lambda: marker in deployment.log(run_id))
    after = deployment.log(run_id).split(marker)[1]
    wait_for("more output of the container in the log", lambda: len(re.findall(rb"line [0-9]+\n", deployment.log(run_id).split(marker)[1])) >= len(re.findall(rb"line [0-9]+\n", after)) + 2)
    entry = deployment.listed(run_id)
    check(entry["state"] == "running" and len(deployment.container_of(run_id)) == 1 and len(deployment.supervisors(run_id)) == 1, "the compose process is dead, the container lives, and the run is still running under its supervisor")
    print("  the log around that moment: " + repr(deployment.log(run_id)[-220:].decode()))
    container = deployment.container_of(run_id)[0][1]
    subprocess.run(["docker", "stop", "--signal", "INT", "-t", "30", container], capture_output=True, timeout=60)
    entry = wait_for("the run to be finished", lambda: (lambda run: run if run["state"] == "finished" else None)(deployment.listed(run_id)), 30)
    check(entry["exit_code"] == 3, "the container was stopped by hand, and the record ends finished with the container's exit code 3")
    check(deployment.log(run_id).rstrip(b"\n").endswith(b"\ninterrupted"), "the container's last line reached the log")
    wait_for("the supervisor to end", lambda: not deployment.supervisors(run_id), 20)
    check(deployment.containers() == [], "no container is left")


def check_a_required_password(deployment):
    print("5. a profile whose database has authentication on")
    code, run_id = deployment.start({"profile": "mongo_ingest", "source": "demo"})
    check(code == 0, f"without a password the run is created: {run_id}")
    entry = wait_for("the run to end", lambda: (lambda run: run if run["state"] == "finished" else None)(deployment.listed(run_id)), 30)
    check(entry["exit_code"] == 1 and deployment.log(run_id) == b"Error: the database admin password is required\n", "and ends at once with exit code 1 and the line: " + deployment.log(run_id).decode().strip())
    check(deployment.containers() == [], "no container was started for it")

    code, run_id = deployment.start({"profile": "mongo_ingest", "source": "demo", "admin_password": PLANTED})
    check(code == 0, f"with the password the run is started: {run_id}")
    wait_for("the program's lines", lambda: b"line 1\n" in deployment.log(run_id))
    check(b"password received: yes\n" in deployment.log(run_id), "the program in the container was handed the password (it says 'password received: yes', not the password)")
    check_the_password(deployment, "a run that needs the password")
    for pid in deployment.supervisors(run_id) + [pid for pid, arguments in deployment.own_processes() if b"social_data_pipeline.control.child" in arguments]:
        check(PLANTED.encode() not in Path(f"/proc/{pid}/environ").read_bytes(), f"the planted password is not in the environment process {pid} was started with (supervisor or child)")
    return run_id


def main():
    deployment = Deployment()
    failed = None
    left = None
    try:
        try:
            print(f"compose project {deployment.name}, Docker {subprocess.run(['docker', 'version', '--format', '{{.Server.Version}}'], capture_output=True, text=True, timeout=30).stdout.strip()}")
            run_id = check_a_run(deployment)
            print("2. the planted password")
            check_the_password(deployment, "a run that does not need it")
            check_cancel(deployment, run_id, "3. cancel")
            check_a_dead_compose_process(deployment)
            run_id = check_a_required_password(deployment)
            check_cancel(deployment, run_id, "6. cancel of the run that was given the password")
            modes = {oct(path.lstat().st_mode & 0o777) for path in (deployment.root / ".sdp").rglob("*")}
            check(modes <= {"0o700", "0o600"}, f"the state folder is private: {sorted(modes)}")
        except Exception as error:
            failed = error
    finally:
        # Also on Ctrl-C and on anything else that ends the checks early: the processes are killed, the containers stopped and the network removed before the interrupt goes on.
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
