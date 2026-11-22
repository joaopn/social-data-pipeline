#!/usr/bin/env python3
"""A check of the web UI's Queue window against a real Docker, run by hand: `python tests/e2e/manual/check_queue.py [--browser PYTHON]` (Linux; an interpreter with PyYAML; a Docker with the compose plugin and without user-id mapping that can pull `python:3.13-slim` and reach PyPI, unpkg.com and cdn.jsdelivr.net, and the image `python:3.13-alpine`, which is not pulled). With `--browser`, PYTHON is an interpreter with Playwright and `PLAYWRIGHT_BROWSERS_PATH` must be set in the environment to the folder of its browsers.

It is not collected by pytest. It builds a throwaway deployment in a temp folder: a copy of the CLI, the package, `docker-compose.yml`, `Dockerfile.jobs`, `Dockerfile.webui`, their requirements and the scheduler's committed configuration, plus a `config/jobs/config.local.yaml` with one PostgreSQL target and no database at all, and `data/jobs` (with `data/jobs/results`) made by this script as the current user, as `sdp db setup-jobs` makes it. The scheduler runs as a compose project of a throwaway name (its image `<name>-jobs`); the web UI's image, container and network get the same throwaway name, set on the host module before it is used. What it shows:

1. The broker (`sdp.py broker passwd`, `broker start`), the scheduler (its image built, its `jobs` service started, its control socket there) and the web UI (set up and started by the host module) all run; the start prints no line about the Queue and mounts `data/jobs/control` and `data/jobs/results` read-only, with their two settings.
2. A query holding a U+202E character and a word of the hints is submitted through the scheduler's `/mcp` with plain HTTP. Over HTTPS, with the generated certificate as the only trust root: the Queue page lists it; its page shows the escape and the hint; the approval's pending page shows the statement and the two sentences; confirming approves it, the scheduler's record names `web-<n>`, and it ends in History as failed (there is no database).
3. A second query is rejected with a reason through the pages; the record holds the reason and the same `web-<n>`.
4. Auto-accept switched on with a wrong management password is refused and changes nothing; with the right one it is on; switching it off needs no password.
5. A finished job with a small CSV result, its record written with the store's own code inside the scheduler's container (`Store.complete`) and its file by this script, is previewed through the web UI and its part downloaded byte for byte. A second finished job has a link planted as a part (pointing at a file with a marker): it is neither previewed nor offered, and the marker is in no answer.
6. With `--browser PYTHON`: `check_queue_browser.py` approves a third query (its statement starts with a line break) by clicking, checks that the statement boxes of its job page and its pending page hold exactly as many characters as the statement, watches the running tab refresh itself and fails on any policy violation.
7. A fourth query is made a pending approval in a browser session, and a second browser session is opened; the management password is changed with `sdp.py broker passwd`; the confirmation of the first session leads to the login page and the scheduler's record shows no approval; the Queue page, the job page, the preview and a download of the second each lead to the login page (the preview, an htmx part, by `HX-Redirect`).
8. The scheduler's own page still answers.

Clean-up, also when the check is interrupted: the web UI's container, network and image, the compose project (its containers and network) and its image are removed, the broker and every process of the deployment are killed, the files the scheduler made as root are removed by a root container, and the temp folder is deleted. What stays: the base image `python:3.13-slim` and Docker's build cache.
"""

import argparse
import contextlib
import http.client
import io
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import check_door
from check_webui import CSP, Page, outcome_of, pending_and_confirm

from social_data_pipeline import webui_host as host
from social_data_pipeline.control import client
from social_data_pipeline.setup import utils as setup_utils

MANAGEMENT = "made-up-management-password-7c2e"
NEW_MANAGEMENT = "made-up-new-management-password-19d4"
WRONG = "a-wrong-made-up-password"
RUN_IMAGE = "python:3.13-alpine"
TARGET = check_door.TARGET
MARKER = "queue-check-marker-8e51"
DONE_ID = "pg_c0c0c0c0"
LINKED_ID = "pg_1c1c1c1c"
RISKY_SQL = "SELECT '\u202e' AS mark FROM dblink_made_up_table"
CSV = b"name,count\nalpha,1\nbeta,2\n"


def check(condition, what):
    if not condition:
        raise AssertionError(what)
    print(f"  ok: {what}")


def wait_for(what, probe, seconds=120):
    return check_door.wait_for(what, probe, seconds)


class Deployment:
    def __init__(self):
        self.root = Path(tempfile.mkdtemp(prefix="sdp-queue-check-")).resolve()
        os.chmod(self.root, 0o755)
        self.name = "sdp-queue-check-" + os.urandom(4).hex()
        self.jobs_image = f"{self.name}-jobs"
        self.port = check_door.free_port()
        self.webui_port = check_door.free_port()
        for name in ("sdp.py", "docker-compose.yml", "Dockerfile.jobs", "requirements-jobs.txt", "Dockerfile.webui", "requirements-webui.txt"):
            shutil.copy(REPO_ROOT / name, self.root / name)
        shutil.copytree(REPO_ROOT / "social_data_pipeline", self.root / "social_data_pipeline", ignore=shutil.ignore_patterns("__pycache__"))
        os.chmod(self.root / "sdp.py", 0o644)
        for path, _, names in os.walk(self.root / "social_data_pipeline"):
            os.chmod(path, 0o755)
            for name in names:
                os.chmod(os.path.join(path, name), 0o644)
        (self.root / "config" / "jobs").mkdir(parents=True)
        for name in ("config.yaml", "entrypoint.sh"):
            shutil.copy(REPO_ROOT / "config" / "jobs" / name, self.root / "config" / "jobs" / name)
        (self.root / "config" / "jobs" / "config.local.yaml").write_text(f"port: {self.port}\nmax_concurrent: 1\nauth: false\ntargets:\n  {TARGET}:\n    backend: postgres\n    database: datasets\n")
        (self.root / ".env").write_text(f"JOBS_PORT={self.port}\nJOBS_RESULT_ROOT=./data/jobs/results\n")
        self.jobs = self.root / "data" / "jobs"
        (self.jobs / "results").mkdir(parents=True)
        os.chmod(self.jobs, 0o755)
        os.chmod(self.jobs / "results", 0o755)
        self.environment = {**os.environ, "COMPOSE_PROJECT_NAME": self.name, "JOBS_PORT": str(self.port), "PYTHONDONTWRITEBYTECODE": "1"}
        # The throwaway names, and the deployment's `.env`, used by the host module from here on.
        host.IMAGE = host.CONTAINER = host.NETWORK = self.name
        setup_utils.ROOT = self.root

    @property
    def socket_path(self):
        return str(self.jobs / "control" / "door.sock")

    def cli(self, *argv, stdin=None):
        result = subprocess.run([sys.executable, str(self.root / "sdp.py"), *argv], env=self.environment, capture_output=True, text=True, timeout=180, input=stdin, start_new_session=True)
        return result.returncode, result.stdout

    def compose(self, *argv, timeout=1800):
        return subprocess.run(["docker", "compose", "--project-name", self.name, "--profile", "jobs", *argv], cwd=self.root, env=self.environment, capture_output=True, text=True, timeout=timeout)

    def docker(self, *argv, timeout=120):
        return subprocess.run(["docker", *argv], capture_output=True, text=True, timeout=timeout)

    def ask(self, document):
        return client.Client(self.socket_path, timeout=30).request(document)

    def record(self, job_id):
        return self.ask({"op": "job", "job_id": job_id})

    def scheduler_page(self, path):
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=10) as answer:
            return answer.status

    def jobs_container(self):
        return self.docker("ps", "-q", "--filter", f"label=com.docker.compose.project={self.name}", "--filter", "label=com.docker.compose.service=jobs").stdout.strip()

    def in_jobs(self, code):
        result = self.docker("exec", self.jobs_container(), "python", "-c", code, timeout=60)
        if result.returncode != 0:
            raise AssertionError(f"docker exec failed: {result.stderr.strip().splitlines()[-1:]}")
        return result.stdout

    def own_processes(self):
        found = []
        mark = str(self.root).encode()
        for name in os.listdir("/proc"):
            if not name.isdigit() or int(name) == os.getpid():
                continue
            try:
                arguments = Path(f"/proc/{name}/cmdline").read_bytes()
                folder = os.readlink(f"/proc/{name}/cwd")
            except OSError:
                continue
            if folder == str(self.root) or mark in arguments:
                found.append(int(name))
        return found

    def project_containers(self):
        return self.docker("ps", "-a", "-q", "--filter", f"label=com.docker.compose.project={self.name}").stdout.split()

    def clean_up(self):
        self.docker("rm", "--force", self.name)
        self.docker("network", "rm", self.name)
        self.compose("down", "--volumes", "--remove-orphans", timeout=300)
        for container in self.project_containers():
            self.docker("rm", "--force", container)
        self.docker("image", "rm", "--force", self.name, timeout=300)
        self.docker("image", "rm", "--force", self.jobs_image, timeout=300)
        self.cli("broker", "stop")
        for pid in self.own_processes():
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass
        time.sleep(0.5)
        # The scheduler runs as root: what it made under data/jobs is removed by a root container first.
        if self.jobs.exists():
            self.docker("run", "--rm", "--pull", "never", "--network", "none", "-v", f"{self.root / 'data'}:/data", RUN_IMAGE, "rm", "-rf", "/data/jobs", timeout=120)
        left = {
            "containers": self.docker("ps", "-a", "-q", "--filter", f"name={self.name}").stdout.split() + self.project_containers(),
            "networks": self.docker("network", "ls", "-q", "--filter", f"name={self.name}").stdout.split(),
            "images": self.docker("images", "-q", self.name).stdout.split() + self.docker("images", "-q", self.jobs_image).stdout.split(),
            "processes": self.own_processes(),
        }
        shutil.rmtree(self.root, ignore_errors=True)
        left["folder"] = [str(self.root)] if self.root.exists() else []
        return left


def check_start(deployment):
    print("1. the broker, the scheduler and the web UI")
    code, text = deployment.cli("broker", "passwd", stdin=f"{MANAGEMENT}\n{MANAGEMENT}\n")
    check(code == 0, "`broker passwd` sets the management password")
    code, text = deployment.cli("broker", "start")
    check(code == 0 and "started" in text, "`broker start`: " + text.strip())
    started = time.monotonic()
    built = deployment.compose("build", "jobs")
    if built.returncode != 0:
        raise AssertionError("the scheduler's image did not build:\n" + "\n".join(built.stderr.strip().splitlines()[-15:]))
    check(True, f"the scheduler's image {deployment.jobs_image} built in {time.monotonic() - started:.0f} s")
    up = deployment.compose("up", "-d", "jobs", timeout=300)
    check(up.returncode == 0, "the jobs service started" + ("" if up.returncode == 0 else ": " + up.stderr.strip()[-300:]))
    wait_for("the scheduler's page", lambda: deployment.scheduler_page("/pending") == 200)
    wait_for("the control socket", lambda: os.path.exists(deployment.socket_path), 60)
    check(deployment.ask({"op": "hello"}) == {"protocol": 1}, "the scheduler's control socket answers hello")
    answers = [str(deployment.webui_port), "", "", "generate"]
    started = time.monotonic()
    code = host._setup(str(deployment.root), lambda *a, **k: answers.pop(0), lambda *a, **k: answers.pop(0), os.getuid(), os.getgid())
    check(code == 0, f"the web UI's setup built its image and made the certificate in {time.monotonic() - started:.0f} s")
    shown = io.StringIO()
    with contextlib.redirect_stdout(shown):
        code = host._start(str(deployment.root), os.getuid(), os.getgid())
    text = shown.getvalue()
    check(code == 0 and "Queue:" not in text, "the web UI started with no line about the Queue")
    found = json.loads(deployment.docker("inspect", deployment.name).stdout)[0]
    mounts = sorted((mount["Source"], mount["Destination"], mount["RW"]) for mount in found["Mounts"])
    check((str(deployment.jobs / "control"), "/run/sdp-jobs", False) in mounts and (str(deployment.jobs / "results"), "/run/sdp-results", False) in mounts and len(mounts) == 4, f"the control folder and the results folder are mounted read-only: {[(mount[1], 'rw' if mount[2] else 'ro') for mount in mounts]}")
    settings = [entry.split("=", 1)[0] for entry in found["Config"]["Env"]]
    check("SDP_WEBUI_QUEUE_SOCKET" in settings and "SDP_WEBUI_RESULTS" in settings, "with their two settings")
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        with socket.socket() as attempt:
            if attempt.connect_ex(("127.0.0.1", deployment.webui_port)) == 0:
                break
        time.sleep(0.25)
    time.sleep(1)
    status = io.StringIO()
    with contextlib.redirect_stdout(status):
        host.cmd_status(str(deployment.root))
    check("the scheduler's control folder is mounted" in status.getvalue() and "the results folder is mounted" in status.getvalue(), "`webui status` says both are mounted")
    return deployment.root / ".sdp" / "webui" / "tls" / "cert.pem"


def log_in(deployment, cafile, password=MANAGEMENT):
    page = Page(deployment.webui_port, cafile)
    status, _, _, cookies = page.request("POST", "/login", headers=page.origin, form={"password": password})
    check(status == 303 and cookies, "a login through the page")
    page.cookie = cookies[0].split(";")[0].split("=", 1)[1]
    _, _, text, _ = page.request("GET", "/")
    return page, re.search(r'name="form_token" value="([^"]+)"', text).group(1)


def check_approve(deployment, page, token, seen):
    print("2. a risky query approved through the Queue")
    job_id = check_door.submit_through_mcp(deployment, RISKY_SQL, "risky.csv", "a query of the check")
    check(True, f"a query with U+202E and a word of the hints submitted through /mcp: {job_id}")
    status, _, text, _ = page.request("GET", "/queue")
    seen.append(text)
    check(status == 200 and f'href="/queue/{job_id}"' in text, "the Queue page lists it, as a link")
    status, _, text, _ = page.request("GET", f"/queue/{job_id}")
    seen.append(text)
    check('<span class="escape" title="made visible">\\u202e</span>' in text and "\u202e" not in text, "its page shows the character as an escape, not as itself")
    check("<li><code>dblink</code></li>" in text, "and the hint dblink")
    _, pending_text, status, headers, _ = pending_and_confirm(page, token, "queue.approve", {"job_id": job_id}, seen=seen)
    check("<pre class=\"statement-whole\">\nSELECT &#39;" in pending_text and "runs this query on its database with the database&#39;s admin login" in pending_text and "This page does not check the statement" in pending_text, "the approval's pending page shows the statement and the two sentences")
    check(status == 303, "the confirmation leads to the outcome")
    outcome = outcome_of(page, headers["location"], seen)
    check("Approved." in outcome, "the outcome says it was approved")

    def ended():
        answer = deployment.record(job_id)
        return answer if answer["phase"] == "history" else None

    record = wait_for("the job's end", ended, 90)["job"]
    label = record["acted_by"].get("approved", "")
    check(record["status"] == "failed" and re.fullmatch(r"web-[0-9]+", label), f"the scheduler ran it as approved by {label} and it failed ({(record['error'] or '')[:60]}...)")
    _, _, text, _ = page.request("GET", "/queue?tab=history")
    seen.append(text)
    check(f'href="/queue/{job_id}"' in text and "failed" in text, "it is in History as failed")
    return label


def check_reject(deployment, page, token, label, seen):
    print("3. a second query rejected with a reason")
    job_id = check_door.submit_through_mcp(deployment, "SELECT 2", "two.csv", "a second query")
    _, pending_text, status, headers, _ = pending_and_confirm(page, token, "queue.reject", {"job_id": job_id, "reason": "not today"}, seen=seen)
    check("not today" in pending_text and status == 303 and "Rejected" in outcome_of(page, headers["location"], seen), "rejected through its pending page")
    record = deployment.record(job_id)["job"]
    check((record["status"], record["reject_reason"], record["acted_by"]) == ("rejected", "not today", {"rejected": label}), f"the record holds the reason and {label}")


def auto_state(deployment):
    return next(row for row in deployment.ask({"op": "auto_accept"})["targets"] if row["name"] == TARGET)


def check_auto_accept(deployment, page, token, seen):
    print("4. auto-accept")
    limit = auto_state(deployment)["limit"]
    path, pending_text, status, _, text = pending_and_confirm(page, token, "queue.auto_accept", {"target": TARGET, "enabled": "on", "limit": str(limit)}, WRONG, seen=seen)
    check('name="management_password"' in pending_text and "without anyone looking at it" in pending_text, "switching it on asks for the management password and says what it means")
    check(status == 403 and "was wrong" in text and auto_state(deployment)["enabled"] is False, "a wrong management password is refused and nothing changed")
    status, headers, _, _ = page.request("POST", path + "/confirm", headers=page.origin, form={"form_token": token, "management_password": MANAGEMENT})
    outcome_of(page, headers["location"], seen)
    check(auto_state(deployment)["enabled"] is True, "with the right one auto-accept is on")
    _, pending_text, status, headers, _ = pending_and_confirm(page, token, "queue.auto_accept", {"target": TARGET, "enabled": "off", "limit": str(limit)}, seen=seen)
    check('name="management_password"' not in pending_text and status == 303, "switching it off asks for no password")
    outcome_of(page, headers["location"], seen)
    check(auto_state(deployment)["enabled"] is False, "and it is off")


def check_results(deployment, page, seen):
    print("5. preview and download")
    code = (
        "import time\n"
        "from social_data_pipeline.jobs.store import Job, Store\n"
        "store = Store('/data/jobs')\n"
        f"for job_id in ({DONE_ID!r}, {LINKED_ID!r}):\n"
        f"    job = Job(job_id=job_id, target={TARGET!r}, backend='postgres', sql='SELECT 3', output_filename='out.csv', overwrite=False, submitted_at=time.time(), status='running', started_at=time.time())\n"
        "    store.complete(job)\n"
        "print('ok')\n"
    )
    check(deployment.in_jobs(code).strip() == "ok", "two finished job records written with the store's own code inside the scheduler's container")
    folder = deployment.jobs / "results" / DONE_ID
    folder.mkdir()
    (folder / "out.csv").write_bytes(CSV)
    _, _, text, _ = page.request("GET", f"/queue/{DONE_ID}")
    seen.append(text)
    check(f'href="/queue/{DONE_ID}/download/0"' in text and f'hx-get="/queue/{DONE_ID}/preview"' in text, "the finished job's page offers its preview and one download")
    _, _, part, _ = page.request("GET", f"/queue/{DONE_ID}/preview", headers={"HX-Request": "true"})
    seen.append(part)
    check("<td>alpha</td><td>1</td>" in part and "<td>beta</td><td>2</td>" in part, "the preview shows the CSV's rows as a table")
    connection = http.client.HTTPSConnection("127.0.0.1", deployment.webui_port, context=page.context, timeout=60)
    connection.request("GET", f"/queue/{DONE_ID}/download/0", headers={"Host": f"127.0.0.1:{deployment.webui_port}", "Cookie": f"__Host-sdp_session={page.cookie}"})
    response = connection.getresponse()
    body = response.read()
    headers = {key.lower(): value for key, value in response.getheaders()}
    connection.close()
    check(response.status == 200 and body == CSV, "its part is downloaded byte for byte")
    check(headers.get("content-type") == "application/octet-stream" and headers.get("content-disposition") == 'attachment; filename="out.csv"' and headers.get("content-security-policy") == CSP, "as an attachment of type application/octet-stream, with the security headers")
    (deployment.jobs / "marker.csv").write_text(f"secret\n{MARKER}\n")
    linked = deployment.jobs / "results" / LINKED_ID
    linked.mkdir()
    os.symlink("../../marker.csv", linked / "out.csv")
    (linked / "out_1.csv").write_text("k\nfine\n")
    _, _, text, _ = page.request("GET", f"/queue/{LINKED_ID}")
    seen.append(text)
    links = re.findall(rf'href="/queue/{LINKED_ID}/download/([0-9]+)"', text)
    _, _, part, _ = page.request("GET", f"/queue/{LINKED_ID}/preview", headers={"HX-Request": "true"})
    seen.append(part)
    answers = []
    for number in range(2):
        status, _, body, _ = page.request("GET", f"/queue/{LINKED_ID}/download/{number}")
        answers.append((status, body))
    check(links == ["0"] and "out_1.csv" in text and answers[0] == (200, "k\nfine\n") and answers[1][0] == 404, "a link planted as a part is not offered: one download, the real part")
    check("<td>fine</td>" in part and all(MARKER not in item for item in (text, part, answers[0][1], answers[1][1])), "the preview shows the real part, and the marker is in no answer")


BROWSER_SQL = "\nSELECT 4\n  FROM (SELECT 1) AS t"


def check_browser(deployment, browser):
    print("6. the browser")
    job_id = check_door.submit_through_mcp(deployment, BROWSER_SQL, "four.csv", "a query for the browser")
    stored = deployment.record(job_id)["job"]["sql"]
    check(stored == BROWSER_SQL, "the scheduler holds the statement with its first line break")
    result = subprocess.run([browser, str(Path(__file__).with_name("check_queue_browser.py")), f"https://127.0.0.1:{deployment.webui_port}/", job_id, str(len(stored))], input=MANAGEMENT + "\n", capture_output=True, text=True, timeout=300)
    print(result.stdout, end="")
    check(result.returncode == 0, "the browser check passed" + ("" if result.returncode == 0 else ": " + result.stderr.strip()[-2000:]))
    check(deployment.record(job_id)["job"]["acted_by"].get("approved", "").startswith("web-"), "the scheduler's record of the job approved by clicking names a web session")


def check_password_change(deployment, cafile, seen):
    print("7. a session the broker has ended")
    page, token = log_in(deployment, cafile)
    # One more session for each read below, each logged in and reading the Queue before the change.
    readers = []
    for _ in range(4):
        reader, _ = log_in(deployment, cafile)
        status, _, _, _ = reader.request("GET", "/queue")
        readers.append(reader)
    check(all(reader.cookie for reader in readers), "four more sessions read the Queue before the change")
    job_id = check_door.submit_through_mcp(deployment, "SELECT 5", "five.csv", "a query for the old session")
    status, headers, _, _ = page.request("POST", "/pending", headers=page.origin, form={"form_token": token, "action": "queue.approve", "job_id": job_id})
    check(status == 303, "a pending approval in a browser session")
    code, _ = deployment.cli("broker", "passwd", stdin=f"{NEW_MANAGEMENT}\n{NEW_MANAGEMENT}\n")
    check(code == 0, "the management password changed with `sdp.py broker passwd`")
    status, answer_headers, text, _ = page.request("POST", headers["location"] + "/confirm", headers=page.origin, form={"form_token": token})
    check(status == 303 and answer_headers.get("location", "").startswith("/login"), f"the old session's confirmation leads to the login page ({answer_headers.get('location')})")
    time.sleep(2)
    record = deployment.record(job_id)
    check(record["phase"] == "pending" and not record["job"]["acted_by"], "the scheduler's record shows no approval: the job is still pending")
    # The broker's earlier yes for the reader is believed for 5 seconds.
    time.sleep(6)
    for reader, (path, headers, want) in zip(readers, (("/queue", {}, 303), (f"/queue/{job_id}", {}, 303), (f"/queue/{DONE_ID}/preview", {"HX-Request": "true"}, 204), (f"/queue/{DONE_ID}/download/0", {}, 303))):
        status, answer_headers, text, _ = reader.request("GET", path, headers=headers)
        where = answer_headers.get("hx-redirect") if want == 204 else answer_headers.get("location")
        check(status == want and where == "/login?why=ended" and "alpha" not in text, f"an old session's GET {path} leads to the login because the broker ended it ({status}, {where})")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--browser", metavar="PYTHON", help="an interpreter with Playwright: run check_queue_browser.py with it (needs PLAYWRIGHT_BROWSERS_PATH in the environment)")
    options = parser.parse_args()
    if options.browser and not os.environ.get("PLAYWRIGHT_BROWSERS_PATH"):
        parser.error("--browser needs PLAYWRIGHT_BROWSERS_PATH set to the folder of Playwright's browsers")
    deployment = Deployment()
    failed = None
    try:
        try:
            print(f"deployment {deployment.root.name}, names {deployment.name}, Docker {deployment.docker('version', '--format', '{{.Server.Version}}').stdout.strip()}")
            cafile = check_start(deployment)
            page, token = log_in(deployment, cafile)
            seen = []
            label = check_approve(deployment, page, token, seen)
            check_reject(deployment, page, token, label, seen)
            check_auto_accept(deployment, page, token, seen)
            check_results(deployment, page, seen)
            check(not [text for text in seen if MANAGEMENT in text or WRONG in text], f"no password in any of the {len(seen)} pages and parts seen")
            if options.browser:
                check_browser(deployment, options.browser)
            check_password_change(deployment, cafile, seen)
            print("8. the scheduler's own page")
            check(deployment.scheduler_page("/pending") == 200 and deployment.scheduler_page("/history") == 200, "the scheduler's own page still answers (Pending, History)")
        except Exception as error:
            failed = error
    finally:
        left = deployment.clean_up()
    if any(left.values()):
        print(f"FAILED: left behind: {left}")
        return 1
    print("  ok: no container, network, image or process of the check is left, and the temp folder is deleted (the base image python:3.13-slim and the build cache stay)")
    if failed is not None:
        print(f"FAILED: {type(failed).__name__}: {failed}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
