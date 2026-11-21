#!/usr/bin/env python3
"""A check of the `runs` commands against a real Docker, run by hand: `python tests/e2e/manual/check_runs.py` (an interpreter with PyYAML, a Docker with the compose plugin and the image `python:3.13-alpine`, which is not pulled).

It is not collected by pytest. It builds a throwaway compose project in a temp folder with a service named `parse` that prints a line every second and exits with code 3 on KeyboardInterrupt, and checks what only a real Docker can show:

1. A container started with `docker compose --profile parse run --rm parse` is listed by the run store as a terminal run with profile `parse` and the source of its SOURCE variable; its output is read; a made-up password planted in the container's environment appears nowhere in the data; cancelling it stops the container within a few seconds, and the program saw the interrupt signal (the compose process ends with exit code 3).
2. The same through the real entry point, with a copy of the CLI in the temp folder: `runs list`, `runs list --json`, `runs logs --tail`, `runs cancel`.
3. A container started with `--name sdp-run-<id> -l sdp.run=<id>` next to a hand-made record is listed as that recorded run in state `detached`, not as a terminal run; its output is read from Docker; cancelling it stops the container by its label and writes the record as cancelled.
4. A one-off container of a service that is no pipeline service is not listed and cannot be cancelled.

Every container it starts is stopped by its id, and the project's network is removed with the project's own `down`, also when the check is interrupted; nothing is pruned.
"""

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

import sdp
from social_data_pipeline import runstore

IMAGE = "python:3.13-alpine"
PLANTED = "planted-made-up-password-4c1d"
PROGRAM = "import sys, time\ntry:\n    n = 0\n    while True:\n        print('line', n, flush=True)\n        n += 1\n        time.sleep(1)\nexcept KeyboardInterrupt:\n    print('interrupted', flush=True)\n    sys.exit(3)\n"
SERVICES = sdp._run_service_profiles()
# The control reads the one planted variable the same way the store reads SOURCE: the environment as a whole is never printed.
PLANTED_ONLY = '{{range .Config.Env}}{{$kv := split . "="}}{{if eq (index $kv 0) "FAKE_DB_PASSWORD"}}{{.}}{{end}}{{end}}'


def compose_file(project):
    # Never pulled: a missing image is an error of this check, not a download.
    service = {"image": IMAGE, "pull_policy": "never", "environment": {"SOURCE": "${SOURCE:-}", "FAKE_DB_PASSWORD": "${FAKE_DB_PASSWORD:-}"}, "command": ["python", "-u", "-c", PROGRAM]}
    return json.dumps({"name": project, "services": {"parse": {**service, "profiles": ["parse"]}, "helper": {**service, "profiles": ["helper"]}}}, indent=2)


def check(condition, what):
    if not condition:
        raise AssertionError(what)
    print(f"  ok: {what}")


class Project:
    def __init__(self):
        self.root = Path(tempfile.mkdtemp(prefix="sdp-runs-check-")).resolve()
        self.name = "sdp-runs-check-" + os.urandom(4).hex()
        (self.root / "docker-compose.yml").write_text(compose_file(self.name))
        self.processes = []

    def start(self, service, *extra):
        """Start a one-off container the way `sdp run` does and return the compose process."""
        env = {**os.environ, "SOURCE": "demo", "FAKE_DB_PASSWORD": PLANTED}
        log = open(self.root / f"compose-{len(self.processes)}.out", "wb")
        process = subprocess.Popen(["docker", "compose", "--profile", service, "run", "--rm", *extra, service], cwd=self.root, env=env, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
        self.processes.append(process)
        return process

    def containers(self):
        """The ids of every container of this project, running or not."""
        result = subprocess.run(["docker", "ps", "-a", "--no-trunc", "-q", "--filter", f"label=com.docker.compose.project={self.name}"], capture_output=True, text=True, timeout=30)
        return result.stdout.split()

    def clean_up(self):
        for container in self.containers():
            subprocess.run(["docker", "stop", "--signal", "KILL", container], capture_output=True, timeout=60)
            subprocess.run(["docker", "rm", "--force", container], capture_output=True, timeout=60)
        for process in self.processes:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=30)
        subprocess.run(["docker", "compose", "--profile", "parse", "--profile", "helper", "down"], cwd=self.root, capture_output=True, timeout=120)
        left = self.containers()
        shutil.rmtree(self.root, ignore_errors=True)
        return left


def wait_for(what, probe, seconds=60):
    """Call probe about four times a second until it returns something true; fail after the time is up."""
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        found = probe()
        if found:
            return found
        time.sleep(0.25)
    raise AssertionError(f"timed out waiting for {what}")


def terminal_runs(root):
    return [run for run in runstore.list_runs(root, SERVICES, 50)["runs"] if run["kind"] == "terminal"]


def gone_within(project, seconds):
    start = time.monotonic()
    wait_for("the container to be removed", lambda: not project.containers(), seconds)
    return time.monotonic() - start


def cli(project, *argv):
    """Run the copy of the CLI in the project folder; returns (exit code, standard output)."""
    result = subprocess.run([sys.executable, str(project.root / "sdp.py"), *argv], capture_output=True, text=True, timeout=120, stdin=subprocess.DEVNULL)
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    return result.returncode, result.stdout


def check_terminal_run(project):
    print("1. a run started in a terminal, through the store")
    process = project.start("parse")
    run = wait_for("the terminal run to be listed", lambda: terminal_runs(project.root))[0]
    check(re.fullmatch(r"live-[0-9a-f]{12}", run["id"]) is not None, f"listed with the id {run['id']}")
    check((run["profile"], run["source"], run["state"]) == ("parse", "demo", "running"), "profile parse, source demo, state running")
    check(re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z", run["started"] or "") is not None, f"start time {run['started']}")
    log = wait_for("a line of output", lambda: (lambda data: data if "line 0" in data["text"] else None)(runstore.read_run_log(project.root, SERVICES, run["id"], 0, 65536)))
    check(log["complete"] is False and log["next_offset"] is None, "the output is read from Docker: " + repr(log["text"]))
    listing = runstore.list_runs(project.root, SERVICES, 50)
    check(PLANTED not in json.dumps([listing, log]), "the planted password is nowhere in the data")
    planted = subprocess.run(["docker", "inspect", "--format", PLANTED_ONLY, project.containers()[0]], capture_output=True, text=True, timeout=30).stdout
    check(planted.strip() == f"FAKE_DB_PASSWORD={PLANTED}", "(control) the planted password is in the container's environment")
    started = time.monotonic()
    check(runstore.cancel_run(project.root, SERVICES, run["id"]) == "stopped", f"cancel returned 'stopped' after {time.monotonic() - started:.1f} s")
    check(gone_within(project, 10) < 10, "the container is gone")
    check(process.wait(timeout=30) == 3, "the compose process ended with exit code 3: the program saw the interrupt")
    check(terminal_runs(project.root) == [], "the run is no longer listed")
    try:
        runstore.cancel_run(project.root, SERVICES, run["id"])
    except runstore.RunNotFound:
        check(True, "a second cancel is refused as an unknown run")
    else:
        raise AssertionError("a second cancel of a run that is gone was not refused")


def check_cli(project):
    print("2. the same through the real entry point")
    shutil.copy(REPO_ROOT / "sdp.py", project.root / "sdp.py")
    shutil.copytree(REPO_ROOT / "social_data_pipeline", project.root / "social_data_pipeline", ignore=shutil.ignore_patterns("__pycache__"))
    process = project.start("parse")
    run = wait_for("the terminal run to be listed", lambda: terminal_runs(project.root))[0]
    wait_for("a line of output", lambda: "line 1" in runstore.read_run_log(project.root, SERVICES, run["id"], 0, 65536)["text"])

    code, text = cli(project, "runs", "list")
    print(text)
    check(code == 0 and re.search(rf"^  {run['id']}  terminal  parse    demo    running  \S+Z  -      -$", text, re.M) is not None, "`runs list` prints the run")
    code, as_json = cli(project, "runs", "list", "--json")
    print(as_json)
    data = json.loads(as_json)
    check(code == 0 and data["docker_available"] is True and [(entry["id"], entry["kind"], entry["profile"], entry["source"]) for entry in data["runs"]] == [(run["id"], "terminal", "parse", "demo")], "`runs list --json` holds the run")
    code, tail = cli(project, "runs", "logs", run["id"], "--tail", "2")
    check(code == 0 and re.fullmatch(r"line [0-9]+\nline [0-9]+\n", tail) is not None, "`runs logs --tail 2` prints two lines: " + repr(tail))
    check(PLANTED not in text + as_json + tail, "the planted password is in none of the three outputs")
    code, refused = cli(project, "runs", "cancel", "live-000000000000")
    check(code == 1 and refused == "\n  Error: no run with this id\n\n", "`runs cancel` of an unknown id is exit code 1")
    code, said = cli(project, "runs", "cancel", run["id"])
    check(code == 0 and said == f"  Run {run['id']}: interrupt signal sent, the container has stopped.\n", "`runs cancel` says: " + said.strip())
    check(gone_within(project, 10) < 10, "the container is gone")
    check(process.wait(timeout=30) == 3, "the compose process ended with exit code 3")
    code, text = cli(project, "runs", "list")
    check(code == 0 and text == "\n  No runs.\n\n", "`runs list` prints 'No runs.'")
    check(not (project.root / ".sdp").exists(), "no state folder was created by listing, reading and cancelling")


def check_detached_run(project):
    print("3. a recorded run whose supervisor is gone")
    run_id = runstore.create_run(project.root, {"profile": "parse", "source": "demo", "flags": {"build": False}, "started_by": "check"})
    runstore.update_record(project.root, run_id, state="running")
    check(runstore.read_run(project.root, run_id)["state"] == "lost", "without a container the run is lost")
    process = project.start("parse", "--name", runstore.container_name(run_id), "-l", runstore.run_label(run_id))
    wait_for("the run to be detached", lambda: runstore.read_run(project.root, run_id)["state"] == "detached")
    listing = runstore.list_runs(project.root, SERVICES, 50)
    check([(run["id"], run["kind"], run["state"]) for run in listing["runs"]] == [(run_id, "web", "detached")], "listed once, as the recorded run in state detached, not as a terminal run")
    log = wait_for("a line of output", lambda: (lambda data: data if "line 0" in data["text"] else None)(runstore.read_run_log(project.root, SERVICES, run_id, 0, 65536)))
    check(log["state"] == "detached" and log["complete"] is False, "its output is read from Docker: " + repr(log["text"]))
    check(PLANTED not in json.dumps([listing, log]), "the planted password is nowhere in the data")
    check(runstore.cancel_run(project.root, SERVICES, run_id) == "stopped", "cancel returned 'stopped'")
    check(gone_within(project, 10) < 10, "the container is gone")
    check(process.wait(timeout=30) == 3, "the compose process ended with exit code 3")
    record = runstore.read_record(project.root, run_id)
    check(record["state"] == "cancelled" and record["ended"] is not None, f"the record is cancelled, ended {record['ended']}")
    modes = {str(path.relative_to(project.root)): oct(path.lstat().st_mode & 0o777) for path in sorted((project.root / ".sdp").rglob("*"))}
    check(set(modes.values()) <= {"0o700", "0o600"}, f"the state folder is private: {modes}")


def check_other_container(project):
    print("4. a one-off container that is no pipeline run")
    process = project.start("helper")
    container = wait_for("the helper container", project.containers)[0]
    wait_for("the helper container to run", lambda: subprocess.run(["docker", "ps", "-q", "--no-trunc", "--filter", f"id={container}"], capture_output=True, text=True, timeout=30).stdout.strip())
    check(terminal_runs(project.root) == [], "it is not listed as a run")
    try:
        runstore.cancel_run(project.root, SERVICES, "live-" + container[:12])
    except runstore.RunNotFound:
        check(True, "cancelling it by its container id is refused")
    else:
        raise AssertionError("a container that is no run was cancelled")
    check(subprocess.run(["docker", "ps", "-q", "--filter", f"id={container}"], capture_output=True, text=True, timeout=30).stdout.strip() != "", "it is still running")
    subprocess.run(["docker", "stop", "--signal", "INT", container], capture_output=True, timeout=60)
    check(process.wait(timeout=30) == 3, "(clean-up) stopped by its id")


def main():
    project = Project()
    failed = None
    left = None
    try:
        try:
            print(f"compose project {project.name}, Docker {subprocess.run(['docker', 'version', '--format', '{{.Server.Version}}'], capture_output=True, text=True, timeout=30).stdout.strip()}")
            check_terminal_run(project)
            check_cli(project)
            check_detached_run(project)
            check_other_container(project)
        except Exception as error:
            failed = error
    finally:
        # Also on Ctrl-C and on anything else that ends the checks early: the containers are stopped and the network removed before the interrupt goes on.
        left = project.clean_up()
        if left:
            print(f"FAILED: containers left behind: {left}")
    if left:
        return 1
    print("  ok: no container of the project is left")
    if failed is not None:
        print(f"FAILED: {type(failed).__name__}: {failed}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
